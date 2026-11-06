//! Native stdin failures must stop at the installed command boundary.
#[path = "support/binary.rs"]
mod binary;
#[path = "support/native_os.rs"]
mod native_os;
use acyclic_native_runtime::{
    CaptureSchedule, ProcessCommand, ProcessInput, ProcessStdio, ProcessTree,
};
use std::io::Read;

#[test]
fn native_input_failures_stop_before_workspace_effects() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let workspace = directory.path().join("workspace");
    std::fs::create_dir(&workspace)?;
    // Rust stdin treats Unix EBADF as EOF; a directory produces a real read error.
    #[cfg(unix)]
    let input = std::fs::File::open(&workspace)?;
    #[cfg(windows)]
    let input = std::fs::File::options()
        .write(true)
        .create_new(true)
        .open(directory.path().join("stdin"))?;
    let cause = (&input)
        .read(&mut [0_u8; 1])
        .expect_err("selected stdin must refuse an OS read")
        .to_string();
    let executable = binary::acyclic();
    let capture = |args: &[&str],
                   input: &std::fs::File,
                   task: Option<&std::ffi::OsStr>|
     -> Result<std::process::Output, Box<dyn std::error::Error>> {
        let mut command = ProcessCommand::new(&executable);
        command
            .args(args)
            .current_dir(&workspace)
            .env_clear()
            .env("HOME", directory.path().join("home"))
            .env("USERPROFILE", directory.path().join("home"))
            .env("CODEX_HOME", directory.path().join("codex"))
            .stdin(input.try_clone()?)
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());
        if let Some(task) = task {
            command.env("ACYCLIC_TASK", task);
        }
        #[cfg(windows)]
        if let Some(root) = std::env::var_os("SystemRoot") {
            command.env("SystemRoot", root);
        }
        let mut process = ProcessTree::spawn(&mut command)?;
        let output = process
            .wait_with_output(
                CaptureSchedule::Unbounded,
                65_536,
                None,
                || Ok(()),
                |_| Ok(ProcessInput::default()),
                None,
                None,
                None,
            )
            .map_err(|failure| format!(
                "step=capture native stdin refusal for {args:?}; cleanup_completed={}; error={}; recovery: cargo test --locked -p acyclic-plugin --test main native_input::native_input_failures_stop_before_workspace_effects -- --exact --nocapture",
                failure.cleanup_completed, failure.error
            ))?;
        assert!(
            process.is_reaped(),
            "native stdin refusal must retire its process"
        );
        Ok(output)
    };
    for args in [
        &["__workspace"][..],
        &["__install-package"][..],
        &["__hook", "codex", "Stop"][..],
    ] {
        let output = capture(args, &input, None)?;
        if args[0] == "__hook" {
            assert!(output.status.success());
            assert!(output.stderr.is_empty());
            let answer: serde_json::Value = serde_json::from_slice(&output.stdout)?;
            assert_eq!(answer.get("continue"), Some(&serde_json::json!(false)));
            assert_eq!(
                answer.get("stopReason").and_then(serde_json::Value::as_str),
                Some(cause.as_str()),
                "native stdin read error must retain its original cause"
            );
        } else {
            assert!(!output.status.success());
            assert!(output.stdout.is_empty());
            assert_eq!(
                std::str::from_utf8(&output.stderr)?.trim_end(),
                cause,
                "native stdin read error must retain its original cause"
            );
        }
        assert!(
            std::fs::read_dir(&workspace)?.next().is_none(),
            "native stdin failure must precede workspace effects"
        );
        assert!(
            !directory.path().join("home").exists() && !directory.path().join("codex").exists(),
            "native stdin failure must precede package and host settings effects"
        );
    }
    use acyclic_harness::{
        Error, OperationId, Outcome,
        agent::{AgentId, AgentPath},
        executor::TurnOutput,
        local::{
            host_files,
            host_owner::{HostAction, SessionAction, TurnClaim},
        },
    };
    let task = AgentPath::root(&AgentId::from_bytes([13; 16]));
    let action = HostAction::Session {
        parent: task.clone(),
        reference: "near-bound-completion".into(),
        next: false,
        action: SessionAction::Complete {
            claim: TurnClaim {
                agent: task,
                operation: OperationId::from_bytes([19; 16]),
                turn: 1,
            },
            outcome: Outcome::Succeeded(TurnOutput::new(
                "x".repeat(65_336),
                serde_json::Value::Null,
                1,
            )),
        },
    };
    let bytes = serde_json::to_vec(&action)?;
    assert!(
        bytes.len() > 65_536,
        "workspace envelope crosses the hook frame bound"
    );
    let Error::Command(refusal) = host_files::request(&workspace, action).unwrap_err() else {
        panic!("workspace client must retain its original refusal");
    };
    let expected = refusal.original().unwrap().to_string();
    let path = directory.path().join("completion.json");
    std::fs::write(&path, bytes)?;
    let output = capture(&["__workspace"], &std::fs::File::open(path)?, None)?;
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        std::str::from_utf8(&output.stderr)?
            .trim_end()
            .ends_with(&expected),
        "workspace envelope must reach the original workspace client: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(std::fs::read_dir(&workspace)?.next().is_none());
    let selected = native_os::string(&[0xff, 0xdb]);
    assert!(selected.to_str().is_none());
    let output = capture(&["agents"], &input, Some(&selected))?;
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        std::str::from_utf8(&output.stderr)?
            .contains("step=admit selected native task: native task identity is not UTF-8"),
        "selected native task encoding must be refused before workspace resolution: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(std::fs::read_dir(&workspace)?.next().is_none());
    Ok(())
}
