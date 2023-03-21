use super::utils::{self, Result, sync_try};
use super::sut::*;

use std::collections::{hash_map, HashMap};
use std::io::prelude::*;
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::io::BufReader;
use std::process::Stdio;
use serde::{Serialize, Deserialize};
use time::{Date, OffsetDateTime};
use tokio::process::Command;
use tokio::sync::Notify;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum BuildLogContents {
    Create { rev: Revision },
    Complete { success: bool },
    Use,
    ClearFail,
}

#[derive(Debug, Serialize, Deserialize)]
struct BuildLogEntry {
    id: u64,
    #[serde(with = "time::serde::rfc3339")]
    time: OffsetDateTime,
    #[serde(flatten)]
    contents: BuildLogContents,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildStatus {
    Pending,
    Building,
    Ok,
    Fail,
}

#[derive(Debug)]
struct Build {
    #[allow(unused)]
    id: u64,
    rev: Revision,
    last_used: Date,
    status: BuildStatus,
    status_notify: Notify,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct BuildMgrConfig {
    /// Path to the directory where build artefacts are kept
    artefact_path: PathBuf,

    /// Maximum number of build artefacts to keep
    #[serde(default = "BuildMgrConfig::default_max_artefacts")]
    _max_artefacts: u64,

    /// Path to the directory in which build temporaries are kept
    build_path: PathBuf,

    /// Path to the build script
    build_script: String,
}
impl BuildMgrConfig {
    fn default_max_artefacts() -> u64 { 100 }
}

#[derive(Debug)]
struct BuildMgrState {
    builds_by_id: HashMap<u64, Build>,
    builds_by_rev: HashMap<Revision, u64>,
    next_build: u64,
}
impl Default for BuildMgrState {
    fn default() -> Self {
        Self {
            builds_by_id: HashMap::new(),
            builds_by_rev: HashMap::new(),
            next_build: 1,
        }
    }
}
impl BuildMgrState {
    fn apply_log_entry(&mut self, log_entry: &BuildLogEntry) -> Result<()> {
        let id_entry = self.builds_by_id.entry(log_entry.id);
        let is_vacant = matches!(id_entry, hash_map::Entry::Vacant(_));
        let is_create = matches!(log_entry.contents, BuildLogContents::Create { .. });
        if is_vacant != is_create {
            return Err(format!("unexpected log entry id").into())
        }

        match &log_entry.contents {
        BuildLogContents::Create { rev } => {
            self.builds_by_rev.insert(rev.clone(), log_entry.id);
            self.next_build = std::cmp::max(self.next_build, log_entry.id.wrapping_add(1));
            id_entry.or_insert(Build {
                id: log_entry.id,
                rev: rev.clone(),
                last_used: log_entry.time.date(),
                status: BuildStatus::Pending,
                status_notify: Notify::new(),
            });
        },
        BuildLogContents::Complete { success } => {
            id_entry.and_modify(|v| {
                v.status = if *success { BuildStatus::Ok } else { BuildStatus::Fail };
                v.last_used = log_entry.time.date();
            });
        },
        BuildLogContents::Use => {
            id_entry.and_modify(|v| v.last_used = log_entry.time.date());
        },
        BuildLogContents::ClearFail => {
            id_entry.and_modify(|v| {
                if v.status == BuildStatus::Fail {
                    v.status = BuildStatus::Pending;
                }
            });
        }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct BuildMgr {
    config: BuildMgrConfig,
    sut: SoftwareUnderTest,
    state: BuildMgrState,
    build_log: File,
    building: bool,
    building_notify: Notify,
}
impl BuildMgr {
    pub fn new(config: BuildMgrConfig, sut: SoftwareUnderTest) -> Result<Self> {
        let mut state = BuildMgrState::default();

        let build_log_path = config.artefact_path.join("buildlog.json");

        // Attempt to read the build log. If there is a read error, we truncate
        // the log.
        let mut truncate = false;
        match File::open(&build_log_path) {
        Err(err) => {
            if err.kind() != std::io::ErrorKind::NotFound {
                Err(err)?
            }

            // If the build log wasn't found, it's possible that the artefact
            // path doesn't exist yet. Create it.
            fs::create_dir_all(&config.artefact_path)?;
        },
        Ok(file) => {
            let log = BufReader::new(file);
            if let Err(err) = (|| -> Result<()> {
                for line in log.lines() {
                    let line = line?;
                    let entry = serde_json::from_str(&line)?;
                    state.apply_log_entry(&entry)?;
                }
                Ok(())
            })() {
                // TODO: proper logging
                println!("Error reading build log: {}", err);
                truncate = true;
            }
        },
        }

        let build_log = sync_try(
            || Ok(OpenOptions::new()
                .append(true)
                .create(true)
                .truncate(truncate)
                .open(&build_log_path)?),
            || "opening build log for writing")?;
        Ok(Self {
            config,
            sut,
            state,
            build_log,
            building: false,
            building_notify: Notify::new(),
        })
    }

    fn commit_build_log_entry(&mut self, log_entry: BuildLogEntry) -> Result<()> {
        let result = (|| {
            let s = serde_json::to_string(&log_entry)?;
            assert!(s.find('\n').is_none());
            writeln!(&self.build_log, "{s}")?;
            self.state.apply_log_entry(&log_entry)
        })();

        if let Err(err) = &result {
            // TODO: proper logging
            println!("Error committing log entry {log_entry:?}: {err}");
        }

        result
    }

    fn get_artefact_path(&self, id: u64) -> PathBuf {
        self.config.artefact_path.join(format!("{id}"))
    }

    /// Get the path to the build artefacts of the given revision, if it exists.
    pub fn get_build(&self, rev: &Revision) -> Option<(PathBuf, BuildStatus)> {
        let id = self.state.builds_by_rev.get(rev)?;
        let build = self.state.builds_by_id.get(id).unwrap();
        Some((self.get_artefact_path(*id), build.status))
    }

    pub async fn get_or_make_build(&mut self, rev: &Revision) -> Option<PathBuf> {
        let id = self.state.builds_by_rev.get(rev).copied().or_else(|| {
            let mut id = self.state.next_build;
            while self.state.builds_by_id.contains_key(&id) {
                id += 1;
            }
            self.state.next_build = id.wrapping_add(1);

            let entry = BuildLogEntry {
                id,
                time: OffsetDateTime::now_utc(),
                contents: BuildLogContents::Create { rev: rev.clone() },
            };
            self.commit_build_log_entry(entry).ok().and(Some(id))
        })?;

        loop {
            let mut build = self.state.builds_by_id.get_mut(&id).unwrap();
            if build.status == BuildStatus::Pending {
                build.status = BuildStatus::Building;

                while self.building {
                    self.building_notify.notified().await;
                }
                self.building = true;

                let result = self.build_inner(id).await;

                self.building = false;
                self.building_notify.notify_one();

                let entry = BuildLogEntry {
                    id,
                    time: OffsetDateTime::now_utc(),
                    contents: BuildLogContents::Complete { success: result.is_ok() },
                };

                let result = self.commit_build_log_entry(entry);
                build = self.state.builds_by_id.get_mut(&id).unwrap();
                if result.is_err() {
                    build.status = BuildStatus::Fail;
                }

                build.status_notify.notify_waiters();
            } else if build.status == BuildStatus::Building {
                let notified = build.status_notify.notified();
                if build.status == BuildStatus::Building {
                    notified.await;
                }
                continue
            }

            if build.status == BuildStatus::Ok {
                break Some(self.get_artefact_path(id))
            } else {
                break None
            }
        }
    }

    async fn build_inner(&mut self, id: u64) -> Result<()> {
        // Cleanup the artefact path
        let artefact_path = self.get_artefact_path(id);
        std::mem::drop(fs::remove_dir_all(&artefact_path));
        fs::create_dir_all(&artefact_path)?;

        let result = (async {
            // Checkout sources
            let build = self.state.builds_by_id.get_mut(&id).unwrap();
            self.sut.checkout(&build.rev).await?;

            // Run the build script
            let mut build_script = self.config.build_script.split_whitespace();
            let build_cmd = build_script.next()
                .ok_or_else(|| utils::error("empty build script"))?;
            let mut cmd = Command::new(build_cmd);
            cmd.args(build_script);
            cmd.arg(&artefact_path);
            cmd.stdin(Stdio::null());
            cmd.stdout(File::create(artefact_path.join("stdout"))?);
            cmd.stderr(File::create(artefact_path.join("stderr"))?);
            cmd.current_dir(&self.config.build_path);
            cmd.kill_on_drop(true);

            let status = cmd.status().await?;

            if status.success() {
                Ok(())
            } else {
                Err(format!("build script exit status: {status}").into())
            }
        }).await;

        if let Err(err) = &result {
            // TODO: proper logging
            println!("Build {id} failure: {err}");

            // Also attempt to write the error to the artefact path.
            std::mem::drop((|| {
                let stderr = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(artefact_path.join("stderr"))?;
                writeln!(&stderr, "Rust error: {err}")?;
                Result::Ok(())
            })());
        }

        result
    }

    /// Get the path to the build artefacts for the given revision or the most
    /// recent older revision for which we have a build, if one exists.
    pub fn get_most_recent_build(&self, _rev: &Revision) -> Option<(Revision, PathBuf, Option<bool>)> {
        todo!();
    }

    /// Clear a failure notice for a given build ID.
    pub fn clear_fail(&mut self, id: u64) -> Result<()> {
        let log_entry = BuildLogEntry {
            id,
            time: OffsetDateTime::now_utc(),
            contents: BuildLogContents::ClearFail,
        };
        self.commit_build_log_entry(log_entry)
    }
}
