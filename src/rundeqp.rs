use std::collections::HashMap;
use std::ffi::OsStr;
use std::pin::Pin;
use std::process::Stdio;

use futures::prelude::*;
use genawaiter::sync::gen;
use genawaiter::yield_;
use once_cell::sync::Lazy;
use slog::{debug, error, o, trace, warn, Logger};
use time::{Duration, OffsetDateTime};
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::time::Sleep;

use super::prelude::*;
use super::{DeqpError, TestResult, TestResultType};

#[derive(Debug)]
pub enum DeqpEvent {
    /// Notification that the deqp process has been spawned successfully.
    Launch { pid: u32 },

    /// Report one test outcome. This includes crashes and timeouts.
    Test { name: String, start: OffsetDateTime, duration: Duration, result: TestResult },

    /// Notification that the deqp process has ended, with or without error.
    ///
    /// `error` is always set if one or more requested tests may not have run.
    Finished { error: Option<DeqpError>, stdout: String, stderr: String },
}

struct RunDeqpState {
    logger: Logger,
    pid: u32,
    timeout_duration: std::time::Duration,
    timeout: Pin<Box<Sleep>>,
    stdout_reader: Option<Pin<Box<io::Lines<BufReader<ChildStdout>>>>>,
    stderr_reader: Option<Pin<Box<io::Lines<BufReader<ChildStderr>>>>>,
    /// Buffer for stdout
    stdout: String,
    /// Buffer for stderr
    stderr: String,
    crash: Option<DeqpError>,
    current_test: Option<(String, OffsetDateTime)>,
    tests_done: bool,
    tests_run: bool,
    child: Option<Child>,
}

static RESULT_VARIANTS: Lazy<HashMap<&str, TestResultType>> = Lazy::new(|| {
    let mut result_variants = HashMap::new();
    result_variants.insert("Pass", TestResultType::Pass);
    result_variants.insert("CompatibilityWarning", TestResultType::CompatibilityWarning);
    result_variants.insert("QualityWarning", TestResultType::QualityWarning);
    result_variants.insert("NotSupported", TestResultType::NotSupported);
    result_variants.insert("Fail", TestResultType::Fail);
    result_variants.insert("ResourceError", TestResultType::ResourceError);
    result_variants.insert("InternalError", TestResultType::InternalError);
    result_variants.insert("Crash", TestResultType::Crash);
    result_variants.insert("Timeout", TestResultType::Timeout);
    result_variants.insert("Waiver", TestResultType::Waiver);
    result_variants
});

impl RunDeqpState {
    fn new<S: AsRef<OsStr> + std::fmt::Debug>(
        mut logger: Logger,
        timeout_duration: std::time::Duration,
        args: &[S],
        env: &[(&str, &str)],
    ) -> Result<Self, DeqpError> {
        let mut cmd = Command::new(&args[0]);
        cmd.args(&args[1..])
            .envs(env.iter().cloned())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        trace!(logger, "Run deqp"; "args" => ?args);
        let mut child = cmd.spawn().map_err(DeqpError::SpawnFailed)?;

        let pid = child.id().ok_or_else(|| {
            DeqpError::SpawnFailed(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to get child pid",
            ))
        })?;
        logger = logger.new(o!("pid" => pid));

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        Ok(Self {
            logger,
            pid,
            timeout_duration,
            timeout: Box::pin(tokio::time::sleep(timeout_duration)),
            stdout_reader: Some(Box::pin(BufReader::new(stdout).lines())),
            stderr_reader: Some(Box::pin(BufReader::new(stderr).lines())),
            stdout: String::new(),
            stderr: String::new(),
            crash: None,
            current_test: None,
            tests_done: false,
            tests_run: false,
            child: Some(child),
        })
    }

    fn handle_stdout_error(&mut self, error: DeqpError) {
        debug!(self.logger, "Failed to read stdout of process"; "error" => %error);
        self.stdout_reader = None;
        if self.crash.is_none() {
            self.crash = Some(error);
        }
    }

    fn handle_stdout_line(
        &mut self,
        l: Result<Option<String>, std::io::Error>,
    ) -> Option<DeqpEvent> {
        let l = match l {
            Ok(None) => {
                self.stdout_reader = None;
                return None;
            }
            Ok(Some(r)) => r,
            Err(e) => {
                self.handle_stdout_error(DeqpError::ReadFailed(e));
                return None;
            }
        };
        trace!(self.logger, "stdout"; "line" => &l);

        if self.tests_done {
            self.stdout.push_str(&l);
            self.stdout.push('\n');
            return None;
        }

        if let Some(report) = l.strip_prefix("  ") {
            for (s, res) in &*RESULT_VARIANTS {
                let Some(report) = report.strip_prefix(s) else { continue };
                let mut report = report.trim();
                if report.starts_with('(') && report.ends_with(')') {
                    report = &report[1..report.len() - 1];
                }
                self.timeout = Box::pin(tokio::time::sleep(self.timeout_duration));

                self.stdout.push_str(&l);
                self.stdout.push('\n');

                let Some((name, start)) = self.current_test.take() else {
                    self.handle_stdout_error(DeqpError::ParseError {
                        error: "Test result appeared outside of a test".into(),
                    });
                    return None;
                };

                let duration = OffsetDateTime::now_utc() - start;
                return Some(DeqpEvent::Test {
                    name,
                    start,
                    duration,
                    result: TestResult {
                        stdout: report.into(),
                        full_stdout: std::mem::take(&mut self.stdout),
                        stderr: String::new(),
                        variant: res.clone(),
                    },
                });
            }
        }

        if let Some(name) =
            l.strip_prefix("TEST: ")
                .or_else(
                    || l.strip_prefix("Test case '")
                        .and_then(|l| l.strip_suffix("'..")))
        {
            let now = OffsetDateTime::now_utc();
            let event = self.current_test.take().map(|(name, start)| {
                let duration = now - start;
                DeqpEvent::Test {
                    name,
                    start,
                    duration,
                    result: TestResult {
                        stdout: String::new(),
                        full_stdout: std::mem::take(&mut self.stdout),
                        stderr: String::new(),
                        variant: TestResultType::InternalError,
                    },
                }
            });

            self.stdout.clear();
            self.stdout.push_str(&l);
            self.stdout.push('\n');

            self.current_test = Some((name.into(), now));
            self.tests_run = true;

            return event
        }

        self.stdout.push_str(&l);
        self.stdout.push('\n');

        if l == "DONE!" {
            self.tests_done = true;
        }
        None
    }

    fn handle_stderr_line(&mut self, l: Result<Option<String>, std::io::Error>) {
        let l = match l {
            Ok(None) => {
                self.stderr_reader = None;
                return;
            }
            Ok(Some(r)) => r,
            Err(e) => {
                debug!(self.logger, "Failed to read stderr of process"; "error" => %e);
                self.stderr_reader = None;
                if self.crash.is_none() {
                    self.crash = Some(DeqpError::ReadFailed(e));
                }
                return;
            }
        };
        trace!(self.logger, "stderr"; "line" => &l);
        if l.contains("FATAL ERROR: ") {
            warn!(self.logger, "Deqp encountered fatal error"; "error" => &l);
            if self.crash.is_none() {
                self.crash = Some(DeqpError::DeqpFatalError);
            }
        }
        self.stderr.push_str(&l);
        self.stderr.push('\n');
    }

    async fn next(&mut self) -> DeqpEvent {
        while self.stdout_reader.is_some() || self.stderr_reader.is_some() || self.child.is_some() {
            tokio::select! {
                line = self.stdout_reader.as_mut().map(|r| r.next_line()).unwrap_or_never() => {
                    if let Some(event) = self.handle_stdout_line(line) {
                        return event
                    }
                }
                line = self.stderr_reader.as_mut().map(|r| r.next_line()).unwrap_or_never() => {
                    self.handle_stderr_line(line);
                }
                result = self.child.as_mut().map(|c| c.wait()).unwrap_or_never() => {
                    self.child = None;
                    match result {
                    Ok(status) if status.success() => (),
                    Ok(status) => {
                        // Record the crash, but continue parsing stdout and stderr. There may have been successful
                        // tests we haven't recorded yet, or a more informative fatal error on stderr.
                        self.crash = Some(DeqpError::Crash {
                            exit_status: status.code(),
                        });
                    }
                    Err(error) => {
                        if self.crash.is_none() {
                            self.crash = Some(DeqpError::WaitFailed(error));
                        }
                    }
                    }
                }
                () = &mut self.timeout => {
                    debug!(self.logger, "Detected timeout");
                    // Kill deqp
                    let logger = self.logger.clone();
                    let mut child = self.child.take().unwrap();
                    tokio::spawn(async move {
                        if let Err(e) = child.kill().await {
                            error!(logger, "Failed to kill deqp after timeout"; "error" => %e);
                        }
                    });
                    self.stdout_reader = None;
                    self.stderr_reader = None;

                    // Timeouts override other kinds of errors, so that we have
                    // a higher chance of noticing hangs.
                    self.crash = Some(DeqpError::Timeout);
                }
            }
        }

        if let Some((name, start)) = self.current_test.take() {
            let error = self.crash.take().unwrap_or(DeqpError::Crash { exit_status: None });
            if matches!(error, DeqpError::Timeout) {
                // Preserve timeout information since a timeout is like a device
                // hang which we want to consider as more fatal.
                self.crash = Some(DeqpError::Timeout);
            } else {
                self.crash = Some(DeqpError::Incomplete);
            }

            let duration = OffsetDateTime::now_utc() - start;
            return DeqpEvent::Test {
                name,
                start,
                duration,
                result: TestResult {
                    stdout: String::new(),
                    full_stdout: std::mem::take(&mut self.stdout),
                    stderr: std::mem::take(&mut self.stderr),
                    variant: match error {
                        DeqpError::Timeout => TestResultType::Timeout,
                        _ => TestResultType::Crash,
                    },
                },
            };
        }

        if self.crash.is_none() {
            if !self.tests_run {
                self.crash = Some(DeqpError::NoTestsRun);
            } else if !self.tests_done {
                self.crash = Some(DeqpError::Incomplete);
            }
        }

        DeqpEvent::Finished {
            error: self.crash.take(),
            stdout: std::mem::take(&mut self.stdout),
            stderr: std::mem::take(&mut self.stderr),
        }
    }
}

/// Start a deqp process and parse the output.
///
/// The started process gets killed on drop.
///
/// Returns the pid of the started process and a stream of events.
pub fn run_deqp<S: AsRef<OsStr> + std::fmt::Debug>(
    logger: Logger,
    timeout_duration: std::time::Duration,
    args: &[S],
    env: &[(&str, &str)],
) -> impl Stream<Item = DeqpEvent> + Send + Unpin {
    debug!(logger, "Start deqp"; "args" => ?args);

    let state_init = RunDeqpState::new(logger, timeout_duration, args, env);

    gen!({
        let mut state = match state_init {
            Ok(state) => state,
            Err(error) => {
                yield_!(DeqpEvent::Finished {
                    error: Some(error),
                    stdout: String::new(),
                    stderr: String::new(),
                });
                return;
            },
        };

        yield_!(DeqpEvent::Launch { pid: state.pid });

        loop {
            let event = state.next().await;
            let is_end = matches!(event, DeqpEvent::Finished { .. });
            yield_!(event);
            if is_end {
                return;
            }
        }
    })
}
