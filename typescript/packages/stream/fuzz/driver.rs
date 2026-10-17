use std::{
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

/// One real TypeScript process per bounded model; callers serialize commands.
pub struct Driver {
    child: Child,
    input: ChildStdin,
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
            input,
            output,
        }
    }
    pub fn call(&mut self, request: serde_json::Value) -> serde_json::Value {
        writeln!(self.input, "{request}").expect("write model action");
        self.input.flush().expect("flush model action");
        let mut line = String::new();
        assert_ne!(
            self.output.read_line(&mut line).expect("read model result"),
            0
        );
        let value: serde_json::Value = serde_json::from_str(&line).expect("model JSON result");
        assert!(value.get("error").is_none(), "{value}");
        value
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
