//! Public root-workspace Git regressions.
//!
//! These tests deliberately drive the packaged control boundary instead of a
//! fake `GitFilesystemExecutor`: root branch transitions must use the same
//! workspace authority and publication checks as the production service.

#![allow(clippy::expect_used, clippy::panic)]

#[allow(dead_code, unused_imports)]
mod support;

use serde_json::{Value, json};
use std::fs;
use std::path::PathBuf;
use support::{ACYCLIC, ServiceGuard, command, isolated_state, output_with_stdin, test_tempdir};

const HOST: &str = "claude-code";

struct Session {
    home: tempfile::TempDir,
    root: PathBuf,
    id: String,
    service: Option<ServiceGuard>,
}

impl Session {
    fn open() -> Self {
        let home = test_tempdir("root-git-regression-");
        let root = home.path().join("root");
        fs::create_dir_all(&root).expect("root");
        fs::write(root.join("shared.txt"), "base\n").expect("base file");
        let service = ServiceGuard::new(home.path());
        let session = Self {
            home,
            root,
            id: "root-git-regression".to_owned(),
            service: Some(service),
        };
        session.hook(
            "SessionStart",
            &json!({"session_id": session.id, "cwd": session.root}),
        );
        session.hook(
            "UserPromptSubmit",
            &json!({
                "session_id": session.id,
                "cwd": session.root,
                "turn_id": format!("{}:root", session.id),
            }),
        );
        session
    }

    fn run(&self, arguments: &[&str], input: &[u8]) -> (bool, String) {
        let mut process = command(ACYCLIC);
        process.args(arguments).current_dir(&self.root);
        isolated_state(&mut process, self.home.path());
        let output = output_with_stdin(&mut process, input);
        let text = if output.status.success() {
            String::from_utf8_lossy(&output.stdout).into_owned()
        } else {
            String::from_utf8_lossy(&output.stderr).into_owned()
        };
        (output.status.success(), text)
    }

    fn hook(&self, event: &str, payload: &Value) -> Value {
        let input = serde_json::to_vec(payload).expect("hook payload");
        let (ok, text) = self.run(&["__hook", HOST, event], &input);
        assert!(ok, "{event} hook failed: {text}");
        if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).expect("hook JSON")
        }
    }

    fn git_ok(&self, arguments: &[&str]) {
        let mut argv = Vec::with_capacity(arguments.len() + 1);
        argv.push("git");
        argv.extend_from_slice(arguments);
        let (ok, text) = self.run(&argv, b"");
        assert!(ok, "acyclic git {} failed: {text}", arguments.join(" "));
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let input = serde_json::to_vec(&json!({
            "session_id": self.id,
            "cwd": self.root,
        }))
        .expect("session end");
        let _ = self.run(&["__hook", HOST, "SessionEnd"], &input);
        if let Some(service) = self.service.take()
            && !std::thread::panicking()
        {
            service.drain();
        }
    }
}

#[test]
#[ignore = "requires the packaged native plugin service"]
fn root_branch_merge_publishes_the_source_workspace() {
    let session = Session::open();

    session.git_ok(&["commit", "-m", "initial"]);
    session.git_ok(&["switch", "-c", "feature"]);
    fs::write(session.root.join("feature.txt"), "feature\n").expect("feature edit");
    session.git_ok(&["commit", "-m", "feature"]);
    session.git_ok(&["switch", "main"]);
    fs::write(session.root.join("main.txt"), "main\n").expect("main edit");
    session.git_ok(&["commit", "-m", "main"]);

    let (ok, text) = session.run(&["git", "merge", "feature"], b"");
    assert!(ok, "branch merge failed: {text}");
    assert_eq!(
        fs::read_to_string(session.root.join("feature.txt")).expect("feature file"),
        "feature\n",
        "root merge must publish the source generation",
    );
    assert_eq!(
        fs::read_to_string(session.root.join("main.txt")).expect("main file"),
        "main\n",
    );
}

#[test]
#[ignore = "requires the packaged native plugin service"]
fn root_branch_rebase_publishes_the_target_workspace() {
    let session = Session::open();

    session.git_ok(&["commit", "-m", "initial"]);
    session.git_ok(&["switch", "-c", "feature"]);
    fs::write(session.root.join("feature.txt"), "feature\n").expect("feature edit");
    session.git_ok(&["commit", "-m", "feature"]);
    session.git_ok(&["switch", "main"]);
    fs::write(session.root.join("main.txt"), "main\n").expect("main edit");
    session.git_ok(&["commit", "-m", "main"]);
    session.git_ok(&["switch", "feature"]);

    let (ok, text) = session.run(&["git", "rebase", "main"], b"");
    assert!(ok, "branch rebase failed: {text}");
    assert_eq!(
        fs::read_to_string(session.root.join("feature.txt")).expect("feature file"),
        "feature\n",
    );
    assert_eq!(
        fs::read_to_string(session.root.join("main.txt")).expect("main file"),
        "main\n",
    );
}
