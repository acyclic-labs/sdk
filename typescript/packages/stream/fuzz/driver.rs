use std::{
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

/// Process-wide step budget of one model process (test/model-driver.ts `MODEL_STEPS`).
pub const MODEL_STEPS: u64 = 4096;

/// One real TypeScript process per bounded model; callers serialize commands. The process lives exactly as long as
/// its stdin (test/model-driver.ts), so it ends with this owner however the owner ends, including a static driver
/// that is never dropped and a test process that is killed. Dropping the driver takes that same path: it closes
/// stdin and waits, so every model run proves the process ends on its own. All of the process's work draws from one
/// budget; a long-lived owner renews the process between runs (`run`) rather than resetting that budget.
pub struct Driver {
    pub(crate) child: Child,
    pub(crate) input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    pub(crate) remaining: u64,
    configuration: (PathBuf, String, String, u64),
}
impl Driver {
    pub fn configured(root: &Path, scenario: &str, fault: &str) -> Self {
        Self::budgeted(root, scenario, fault, MODEL_STEPS)
    }
    /// A process with an explicit budget, for tests at the budget boundary.
    pub fn budgeted(root: &Path, scenario: &str, fault: &str, steps: u64) -> Self {
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
            .expect("start the actual TypeScript session model driver");
        let input = child.stdin.take().expect("driver stdin");
        let output = BufReader::new(child.stdout.take().expect("driver stdout"));
        Self {
            child,
            input: Some(input),
            output,
            remaining: steps,
            configuration: (root.to_path_buf(), scenario.into(), fault.into(), steps),
        }
    }
    /// Starts one bounded run on a long-lived driver: once the process has spent half its budget it is replaced by a
    /// fresh one, so a run never inherits another run's spending and a single run can never exceed half the budget
    /// without ending its process.
    pub fn run(&mut self) -> &mut Self {
        let (root, scenario, fault, steps) = &self.configuration;
        if self.remaining < steps / 2 {
            *self = Self::budgeted(root, scenario, fault, *steps);
        }
        self
    }
    /// Sends one command without waiting for its result.
    pub fn send(&mut self, request: serde_json::Value) {
        let input = self.input.as_mut().expect("model stdin is open");
        writeln!(input, "{request}").expect("write model action");
        input.flush().expect("flush model action");
    }
    /// The next envelope the process writes, or `None` once it has exited.
    pub fn answer(&mut self) -> Option<serde_json::Value> {
        let mut line = String::new();
        if self.output.read_line(&mut line).expect("read model result") == 0 {
            return None;
        }
        let value: serde_json::Value = serde_json::from_str(&line).expect("model JSON result");
        self.remaining = value["remaining"]
            .as_u64()
            .expect("model envelope reports its budget");
        Some(value)
    }
    pub fn call(&mut self, request: serde_json::Value) -> serde_json::Value {
        self.send(request);
        let mut value = self
            .answer()
            .expect("model process exited before answering (step budget exhausted or crashed)");
        assert!(value.get("error").is_none(), "{value}");
        value["result"].take()
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        drop(self.input.take());
        let _ = self.child.wait();
    }
}
