//! Native stdin failures must stop at the installed command boundary.
use super::binary;
use acyclic_native_runtime::{
    CaptureSchedule, ProcessCommand, ProcessInput, ProcessStdio, ProcessTree,
};
use std::io::Read;

#[test]
fn native_stdin_failures_stop_before_workspace_effects() -> Result<(), Box<dyn std::error::Error>> {
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
    for args in [
        &["__workspace"][..],
        &["__install-package"][..],
        &["__hook", "codex", "Stop"][..],
        &["fs", "--help"][..],
        &["fs", "-h"][..],
        &["fs", "help"][..],
    ] {
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
                "step=capture native stdin refusal for {args:?}; cleanup_completed={}; error={}; recovery: cargo test --locked -p acyclic-plugin --test main native_input::native_stdin_failures_stop_before_workspace_effects -- --exact --nocapture",
                failure.cleanup_completed, failure.error
            ))?;
        assert!(
            process.is_reaped(),
            "native stdin refusal must retire its process"
        );
        if args[0] == "fs" {
            assert!(output.status.success(), "help must not require a workspace");
            assert!(output.stderr.is_empty());
            let help = std::str::from_utf8(&output.stdout)?;
            let descriptor = acyclic_harness::tools::catalog()
                .find(|tool| tool.name == "fs")
                .ok_or("filesystem tool descriptor absent")?;
            assert!(help.contains(descriptor.description));
        } else if args[0] == "__hook" {
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
    Ok(())
}
