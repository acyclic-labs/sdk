//! Native process checks exercise the shared CLI and MCP binary against HTTP fakes.

#[test]
fn identity_processes() -> Result<(), Box<dyn std::error::Error>> {
    let status = std::process::Command::new("node")
        .arg("--test")
        .arg("tests/identity.test.mjs")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("ACYCLIC_TEST_BINARY", env!("CARGO_BIN_EXE_acyclic"))
        .status()?;
    assert!(status.success(), "identity process tests failed");
    Ok(())
}
