//! One event-driven owner of the real TypeScript model process.
use std::{
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{Mutex, MutexGuard, OnceLock},
};

/// Process-wide step budget of one model process (test/model-driver.ts `MODEL_STEPS`).
pub const MODEL_STEPS: u64 = 4096;

/// A process initialization result retained by its original serialized owner.
pub type Shared = OnceLock<io::Result<Mutex<Driver>>>;

/// One real TypeScript process per bounded model; callers serialize commands.
/// Its original stdin owns its lifetime, including when a static owner is killed.
/// Drop closes that producer and waits for the process. Runs renew a spent
/// process between inputs rather than resetting the process-wide step budget.
pub struct Driver {
    pub(crate) child: Child,
    pub(crate) input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    pub(crate) remaining: u64,
    configuration: (PathBuf, String, String, u64),
}
impl Driver {
    /// Start the actual model with its ordinary finite exploration budget.
    pub fn configured(root: &Path, scenario: &str, fault: &str) -> io::Result<Self> {
        Self::budgeted(root, scenario, fault, MODEL_STEPS)
    }

    /// Borrow the one process owned by a long-lived fuzz or model caller.
    pub fn shared<'a>(
        shared: &'a Shared,
        root: &Path,
        scenario: &str,
        fault: &str,
    ) -> io::Result<MutexGuard<'a, Self>> {
        let initialized =
            shared.get_or_init(|| Self::configured(root, scenario, fault).map(Mutex::new));
        let owner = initialized
            .as_ref()
            .map_err(|error| io::Error::new(error.kind(), error.to_string()))?;
        owner
            .lock()
            .map_err(|error| io::Error::other(format!("serialize model commands: {error}")))
    }

    /// Start a process with an explicit budget for tests at the exact boundary.
    pub fn budgeted(root: &Path, scenario: &str, fault: &str, steps: u64) -> io::Result<Self> {
        let mut child = Command::new("bun")
            .arg(if scenario.starts_with("deadline") {
                "typescript/packages/stream/test/session-deadline-model-driver.ts"
            } else if scenario == "queue-work" {
                "typescript/packages/stream/test/session-queue-work-driver.ts"
            } else if scenario == "queue" {
                "typescript/packages/stream/test/session-queue-model-driver.ts"
            } else {
                "typescript/packages/stream/test/session-model-driver.ts"
            })
            .current_dir(root)
            .env("STREAM_SESSION_SCENARIO", scenario)
            .env("STREAM_SESSION_FAULT", fault)
            .env("STREAM_MODEL_STEPS", steps.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "start actual TypeScript model {scenario} at {}: {error}",
                        root.display()
                    ),
                )
            })?;
        match (child.stdin.take(), child.stdout.take()) {
            (Some(input), Some(output)) => Ok(Self {
                child,
                input: Some(input),
                output: BufReader::new(output),
                remaining: steps,
                configuration: (root.to_path_buf(), scenario.into(), fault.into(), steps),
            }),
            (input, _) => {
                // Closing the original producer must precede the exit wait.
                drop(input);
                child.wait()?;
                Err(io::Error::other(
                    "start actual TypeScript model: missing piped stdin or stdout",
                ))
            }
        }
    }

    /// Start a bounded run, renewing the process once less than half its budget remains.
    pub fn run(&mut self) -> io::Result<&mut Self> {
        let (root, scenario, fault, steps) = &self.configuration;
        if self.remaining < steps / 2 {
            *self = Self::budgeted(root, scenario, fault, *steps)?;
        }
        Ok(self)
    }

    /// Send one command without waiting for its result.
    pub fn send(&mut self, request: &serde_json::Value) -> io::Result<()> {
        let input = self.input.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "write model action: original stdin producer is closed",
            )
        })?;
        // ChildStdin is unbuffered; newline publishes the command without a flush.
        writeln!(input, "{request}").map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("write model action {request}: {error}"),
            )
        })
    }

    /// Read the next real envelope, or `None` after the process closes its output.
    pub fn answer(&mut self) -> io::Result<Option<serde_json::Value>> {
        let mut line = String::new();
        if self
            .output
            .read_line(&mut line)
            .map_err(|error| io::Error::new(error.kind(), format!("read model result: {error}")))?
            == 0
        {
            return Ok(None);
        }
        let value: serde_json::Value = serde_json::from_str(&line).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("decode model JSON result {line:?}: {error}"),
            )
        })?;
        self.remaining = value
            .get("remaining")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("model envelope has no unsigned remaining budget: {value}"),
                )
            })?;
        Ok(Some(value))
    }

    /// Send one command and return its result, preserving refusal and producer-loss details.
    pub fn call(&mut self, request: &serde_json::Value) -> io::Result<serde_json::Value> {
        self.send(request)?;
        let mut value = self.answer()?.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, format!("model exited before answering {request} (step budget exhausted or process crashed)")))?;
        if value.get("error").is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("model refused action {request}: {value}"),
            ));
        }
        let Some(result) = value.get_mut("result") else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("model envelope has no action result: {value}"),
            ));
        };
        Ok(result.take())
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        drop(self.input.take());
        let _ = self.child.wait();
    }
}

/// Encode a frame once for the real model protocol, without a temporary string per byte.
pub fn hex(input: &[u8]) -> io::Result<String> {
    let capacity = input.len().checked_mul(2).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "hex frame length overflows addressable memory",
        )
    })?;
    let mut output = String::new();
    output.try_reserve_exact(capacity).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("allocate hex frame of {capacity} bytes: {error}"),
        )
    })?;
    let digit = |value: u8| {
        char::from(if value < 10 {
            b'0' + value
        } else {
            b'a' + value - 10
        })
    };
    for byte in input {
        output.push(digit(byte >> 4));
        output.push(digit(byte & 15));
    }
    Ok(output)
}
