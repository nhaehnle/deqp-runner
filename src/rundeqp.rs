use std::collections::HashMap;
use std::ffi::OsStr;
use std::pin::Pin;
use std::process::Stdio;
use std::task::{Context, Poll};

use futures::prelude::*;
use once_cell::sync::Lazy;
use slog::{debug, error, o, trace, warn, Logger};
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::time::Sleep;

use crate::{DeqpError, TestResult, TestResultType};

#[derive(Debug)]
pub enum DeqpEvent {
    TestStart { name: String },
    TestEnd { result: TestResult },
}

pub struct RunDeqpState {
    logger: Logger,
    pub pid: u32,
    timeout_duration: std::time::Duration,
    timeout: Pin<Box<Sleep>>,
    stdout_reader: Pin<Box<io::Lines<BufReader<ChildStdout>>>>,
    stderr_reader: Pin<Box<io::Lines<BufReader<ChildStderr>>>>,
    /// Buffer for stdout
    pub stdout: String,
    /// Buffer for stderr
    pub stderr: String,
    stdout_finished: bool,
    stderr_finished: bool,
    /// Process exited
    finished: bool,
    has_timeout: bool,
    tests_done: bool,
    /// deqp reported a fatal error on stderr
    has_fatal_error: bool,
    /// Process exit status
    pub finished_result: Option<Result<(), DeqpError>>,
    child: Option<tokio::process::Child>,
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
    fn new(
        mut logger: Logger,
        timeout_duration: std::time::Duration,
        mut child: Child,
    ) -> Result<Self, DeqpError> {
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
            stdout_reader: Box::pin(BufReader::new(stdout).lines()),
            stderr_reader: Box::pin(BufReader::new(stderr).lines()),
            stdout: String::new(),
            stderr: String::new(),
            stdout_finished: false,
            stderr_finished: false,
            finished: false,
            has_timeout: false,
            tests_done: false,
            has_fatal_error: false,
            finished_result: None,
            child: Some(child),
        })
    }

    fn handle_stdout_line(
        &mut self,
        l: Result<Option<String>, std::io::Error>,
    ) -> Option<DeqpEvent> {
        let l = match l {
            Ok(None) => {
                self.stdout_finished = true;
                return None;
            }
            Ok(Some(r)) => r,
            Err(e) => {
                self.stdout_finished = true;
                debug!(self.logger, "Failed to read stdout of process"; "error" => %e);
                return None;
            }
        };
        trace!(self.logger, "stdout"; "line" => &l);
        if self.tests_done {
            if !l.is_empty() {
                self.stdout.push_str(&l);
                self.stdout.push('\n');
            }
            return None;
        }

        if let Some(l) = l.strip_prefix("  ") {
            for (s, res) in &*RESULT_VARIANTS {
                if let Some(l) = l.strip_prefix(s) {
                    let mut l = l.trim();
                    if l.starts_with('(') && l.ends_with(')') {
                        l = &l[1..l.len() - 1];
                    }
                    self.stdout.push_str(l);
                    self.timeout = Box::pin(tokio::time::sleep(self.timeout_duration));
                    return Some(DeqpEvent::TestEnd {
                        result: TestResult {
                            stdout: std::mem::take(&mut self.stdout),
                            variant: res.clone(),
                        },
                    });
                }
            }
        }

        if let Some(l) = l.strip_prefix("TEST: ") {
            return Some(DeqpEvent::TestStart {
                name: l.to_string(),
            });
        } else if let Some(l) = l.strip_prefix("Test case '") {
            if let Some(l) = l.strip_suffix("'..") {
                self.stdout.clear();
                return Some(DeqpEvent::TestStart { name: l.into() });
            } else {
                self.stdout.push_str(l);
                self.stdout.push('\n');
            }
        } else if l == "DONE!" {
            self.tests_done = true;
        } else if l.is_empty() {
        } else {
            self.stdout.push_str(&l);
            self.stdout.push('\n');
        }
        None
    }

    fn handle_stderr_line(&mut self, l: Result<Option<String>, std::io::Error>) {
        let l = match l {
            Ok(None) => {
                self.stderr_finished = true;
                return;
            }
            Ok(Some(r)) => r,
            Err(e) => {
                self.stderr_finished = true;
                debug!(self.logger, "Failed to read stderr of process"; "error" => %e);
                return;
            }
        };
        trace!(self.logger, "stderr"; "line" => &l);
        if l.contains("FATAL ERROR: ") {
            warn!(self.logger, "Deqp encountered fatal error"; "error" => &l);
            self.has_fatal_error = true;
        }
        self.stderr.push_str(&l);
        self.stderr.push('\n');
    }
}

impl Stream for RunDeqpState {
    type Item = DeqpEvent;
    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Continue reading stdout and stderr even when the process exited
        loop {
            if !self.stdout_finished {
                if let Poll::Ready(r) = self.stdout_reader.as_mut().poll_next_line(ctx) {
                    if let Some(r) = self.handle_stdout_line(r) {
                        return Poll::Ready(Some(r));
                    } else {
                        continue;
                    }
                }
            }

            if !self.stderr_finished {
                if let Poll::Ready(r) = self.stderr_reader.as_mut().poll_next_line(ctx) {
                    self.handle_stderr_line(r);
                    continue;
                }
            }

            if !self.finished {
                // Wait for process in parallel so it can make progress
                let res = if let Poll::Ready(r) =
                    Pin::new(&mut Box::pin(self.child.as_mut().unwrap().wait())).poll(ctx)
                {
                    Some(match r {
                        Ok(status) if status.success() => Ok(()),
                        Ok(status) => Err(DeqpError::Crash {
                            exit_status: status.code(),
                        }),
                        Err(e) => Err(DeqpError::WaitFailed(e)),
                    })
                } else {
                    None
                };
                if let Some(res) = res {
                    self.finished_result = Some(res);
                    self.finished = true;
                    continue;
                }

                if !self.has_timeout && self.timeout.as_mut().poll(ctx).is_ready() {
                    debug!(self.logger, "Detected timeout");
                    self.has_timeout = true;
                    self.finished_result = Some(Err(DeqpError::Timeout));
                    // Kill deqp
                    let logger = self.logger.clone();
                    let mut child = self.child.take().unwrap();
                    tokio::spawn(async move {
                        if let Err(e) = child.kill().await {
                            error!(logger, "Failed to kill deqp after timeout"; "error" => %e);
                        }
                    });
                    return Poll::Ready(None);
                }
            }

            if self.stdout_finished && self.stderr_finished && self.finished {
                if self.has_fatal_error {
                    self.finished_result = Some(Err(DeqpError::DeqpFatalError));
                }
                return Poll::Ready(None);
            }
            break Poll::Pending;
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
) -> Result<RunDeqpState, DeqpError> {
    debug!(logger, "Start deqp"; "args" => ?args);
    let mut cmd = Command::new(&args[0]);
    cmd.args(&args[1..])
        .envs(env.iter().cloned())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    trace!(logger, "Run deqp"; "args" => ?args);
    let child = cmd.spawn().map_err(DeqpError::SpawnFailed)?;
    RunDeqpState::new(logger, timeout_duration, child)
}
