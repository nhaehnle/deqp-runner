use std::fmt::Write;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::prelude::*;
use super::utils::{self, Result, async_try};

type StdResult<T, E> = std::result::Result<T, E>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ModuleRevision {
    Git([u8; 20])
}
impl ModuleRevision {
    fn from_git_ascii(hex: &[u8]) -> Result<ModuleRevision> {
        if hex.len() != 40 {
            return Err("bad git hash length".into())
        }
        let mut hash = [0; 20];

        for i in 0..20 {
            let digit = std::str::from_utf8(&hex[2 * i..2 * i + 2])?;
            hash[i] = u8::from_str_radix(digit, 16)?;
        }

        Ok(Self::Git(hash))
    }

    fn to_git_string(&self) -> String {
        match self {
        Self::Git(hash) => {
            let mut s = String::with_capacity(40);
            for b in hash {
                write!(s, "{b:02x}").unwrap();
            }
            s
        },
        }
    }
}
impl Serialize for ModuleRevision {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
        where
            S: serde::Serializer {
        if serializer.is_human_readable() {
            let s: String = match self {
                Self::Git(_) => {
                    format!("git-{}", self.to_git_string())
                },
            };
            s.serialize(serializer)
        } else {
            let mut b: Vec<u8> = Vec::new();
            match self {
            Self::Git(hash) => {
                b.push(0);
                b.extend_from_slice(hash);
            },
            }
            serializer.serialize_bytes(&b)
        }
    }
}
impl<'de> Deserialize<'de> for ModuleRevision {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
        where
            D: serde::Deserializer<'de> {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            if let Some(hex) = s.strip_prefix("git-") {
                Self::from_git_ascii(hex.as_bytes())
                    .map_err(serde::de::Error::custom)
            } else {
                Err(serde::de::Error::custom("bad module revision prefix"))
            }
        } else {
            todo!()
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub struct Revision {
    top: ModuleRevision,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    submodule_overrides: Vec<(String, ModuleRevision)>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SutBranch {
    remote: String,
    branch: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct SoftwareUnderTest {
    source: std::path::PathBuf,
    submodules: Vec<std::path::PathBuf>,
    main: SutBranch,
    git_wrapper: Option<String>,
    _run_script: std::path::PathBuf,
}
impl SoftwareUnderTest {
    async fn exec_git<'a, I, A>(&self, subcommand: &str, args: I, ignore_stderr: bool)
        -> Result<Vec<u8>>
        where I: IntoIterator<Item = A>,
              A: AsRef<std::ffi::OsStr>
    {
        let git = self.git_wrapper.as_ref().map(String::as_str).unwrap_or("git");
        let mut git = git.split_whitespace();
        let mut cmd = Command::new(git.next().ok_or_else(|| utils::error("empty git-wrapper"))?);
        cmd.args(git);
        cmd.arg(subcommand);
        cmd.args(args);

        cmd.current_dir(&self.source);

        let output = cmd.output().await?;

        if !output.status.success() {
            return Err(format!("git {subcommand} failed: {}\n{}", output.status,
                               String::from_utf8_lossy(&output.stderr)).into())
        }
        if !ignore_stderr && !output.stderr.is_empty() {
            return Err(format!("unexpected stderr in git {subcommand}:\n{}",
                               String::from_utf8_lossy(&output.stderr)).into())
        }

        Ok(output.stdout)
    }

    pub async fn get_branch_revision(&self, branch: &SutBranch) -> Result<Revision> {
        let arg = format!("{}/{}", branch.remote, branch.branch);
        let result = async_try(
            async { self.exec_git("rev-parse", [&arg], false).await },
            || "calling git rev-parse").await?;
        let hex = result.trim_whitespace_start().trim_whitespace_end();
        Ok(Revision {
            top: ModuleRevision::from_git_ascii(hex)?,
            submodule_overrides: Vec::new(),
        })
    }

    pub async fn get_main_revision(&self) -> Result<Revision> {
        self.get_branch_revision(&self.main).await
    }

    pub async fn checkout(&self, rev: &Revision) -> Result<()> {
        assert!(rev.submodule_overrides.is_empty(), "not implemented");

        let hex = rev.top.to_git_string();
        self.exec_git("switch", ["-d", &hex], true).await?;
        if !self.submodules.is_empty() {
            self.exec_git("submodule", ["update"], true).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn check_module_revision() -> Result<()> {
        let sample = r#"{"top":"git-6309e9c7eeddc731815eea5fee696ac4fb098e09"}"#;
        let rev: Revision = serde_json::from_str(sample)?;
        assert!(rev.submodule_overrides.is_empty());
        assert!(matches!(rev.top, ModuleRevision::Git([99, ..])));

        assert_eq!(&serde_json::to_string(&rev)?, sample);

        Ok(())
    }
}
