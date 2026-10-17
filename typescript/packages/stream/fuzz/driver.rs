use std::{
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

/// One real TypeScript process per bounded model; callers serialize commands. The process lives exactly as long as
/// its stdin (test/model-driver.ts), so it ends with this owner however the owner ends, including a static driver
/// that is never dropped and a test process that is killed. Dropping the driver takes that same path: it closes
/// stdin and waits, so every model run proves the process ends on its own.
pub struct Driver {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
}
impl Driver {
    pub fn configured(root: &Path, scenario: &str, fault: &str) -> Self {
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
        }
    }
    /// Sends one command without waiting for its result.
    pub fn send(&mut self, request: serde_json::Value) {
        let input = self.input.as_mut().expect("model stdin is open");
        writeln!(input, "{request}").expect("write model action");
        input.flush().expect("flush model action");
    }
    pub fn call(&mut self, request: serde_json::Value) -> serde_json::Value {
        self.send(request);
        let mut line = String::new();
        assert_ne!(
            self.output.read_line(&mut line).expect("read model result"),
            0,
            "model process exited before answering (step budget exhausted or crashed)"
        );
        let value: serde_json::Value = serde_json::from_str(&line).expect("model JSON result");
        assert!(value.get("error").is_none(), "{value}");
        value
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        drop(self.input.take());
        let _ = self.child.wait();
    }
}
