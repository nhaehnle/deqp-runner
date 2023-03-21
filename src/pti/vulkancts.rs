
use std::io::{prelude::*, BufReader};
use std::fs::File;
use futures::prelude::*;

use slog::{Drain, o};

use super::*;
use super::utils::{Result, sync_try};

#[derive(Debug, Clone)]
pub struct Options {
    /// Keep temporary files for debugging.
    pub keep_temps: bool,

    /// Turn on more debug output.
    pub verbose: bool,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            keep_temps: false,
            verbose: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Path to the Vulkan CTS "deqp_vk" file.
    pub deqp_vk: std::path::PathBuf,

    /// Path to a list of CTS cases that overrides the full list.
    pub deqp_cases: Option<std::path::PathBuf>,

    pub options: Options,
}

fn parse_caselist(suite: &mut suite::Suite, path: &std::path::Path) -> Result<()> {
    let list_file = BufReader::new(File::open(path)?);
    for line in list_file.lines() {
        let line = line?;
        if let Some(test) = line.strip_prefix("TEST: ") {
            suite.put(&test)?;
        }
    }
    Ok(())
}

pub fn get_caselist(config: &Config) -> Result<suite::Suite> {
    sync_try(|| {
        let Some(deqp_vk) = config.deqp_vk.to_str() else {
            return Err("deqp_vk path not valid UTF-8?".into())
        };
        let Some(parentdir) = config.deqp_vk.parent() else {
            return Err("deqp_vk path is incomplete?".into());
        };

        let mut suite = suite::Suite::new(".".into());

        if let Some(caselist) = &config.deqp_cases {
            parse_caselist(&mut suite, caselist)?;
        } else {
            let tempdir = tempfile::tempdir()?;
            let temppath = tempdir.path().display();

            let mut cmd = std::process::Command::new(deqp_vk);
            cmd.current_dir(parentdir);
            cmd.arg("--deqp-runmode=txt-caselist");
            cmd.arg(format!("--deqp-log-filename={temppath}/TestResults.qpa"));
            cmd.arg(format!("--deqp-caselist-export-file={temppath}/${{packageName}}-cases.${{typeExtension}}"));

            let result = cmd.output()?;
            if !result.status.success() || !result.stderr.is_empty() {
                return Err(format!("command failed: {}\n{}", result.status,
                                   String::from_utf8_lossy(&result.stderr)).into());
            }

            let packages = [
                ("dEQP-VK", false),
                ("dEQP-VK-experimental", true),
            ];
            for (package, optional) in packages {
                let list_path = tempdir.path().join(format!("{package}-cases.txt"));
                if !list_path.exists() {
                    if optional {
                        continue
                    }
                    return Err(format!("missing case list for package {package}").into());
                }

                parse_caselist(&mut suite, &list_path)?;
            }

            if config.options.keep_temps {
                // Leak the temp dir on purpose so it doesn't get cleaned up.
                println!("Keeping temporaries in {}", temppath);
                std::mem::forget(tempdir);
            }

        }

        Ok(suite)
    }, || "retrieving Vulkan CTS case list")
}

pub fn run_tests(config: &Config, suite: &suite::Suite, tests: &[suite::TestRef]) -> Result<()> {
    sync_try(|| {
        let decorator = slog_term::PlainDecorator::new(std::io::stdout());
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let root = slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));

        let options = crate::RunOptions {
            args: [config.deqp_vk.to_string_lossy().into(), "--deqp-caselist-file".into()].into(),
            batch_size: 0,
            capture_dumps: true,
            timeout: std::time::Duration::from_secs(10),
            retry: true,
            max_failures: 20,
            fail_dir: Some(".".into()),
        };

        let test_names: Vec<_> = tests.iter().map(|&test_ref| suite.get_name(test_ref)).collect();
        let test_names_borrows: Vec<&str> = test_names.iter().map(String::as_str).collect();

        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
            let mut stream = crate::run_test_list(root, &test_names_borrows, &options);
            while let Some(event) = stream.next().await {
                // println!("{:?}", event)
            }
        });

        Ok(())
    }, || "running CTS tests")
}
