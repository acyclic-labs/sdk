//! Command dispatch is independent of a native host's workspace owner.
#[path = "support/binary.rs"]
mod binary;

#[test]
fn execution_dispatch_precedes_workspace_lookup() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for args in [
        vec!["run", "--help"],
        vec!["serve", "--help"],
        vec!["attach", "--help"],
    ] {
        let output = std::process::Command::new(binary::acyclic())
            .args(args)
            .env_clear()
            .current_dir(directory.path())
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    for args in [
        vec!["attach", "harness/agents/root", "-p", "new task"],
        vec!["serve", "127.0.0.1:0", "--operation", "new-operation"],
    ] {
        let output = std::process::Command::new(binary::acyclic())
            .args(args)
            .env_clear()
            .current_dir(directory.path())
            .output()?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("recovery: acyclic"));
    }
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}
