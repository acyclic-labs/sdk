//! Native process checks exercise the shared CLI and MCP binary against HTTP fakes.

#[path = "support/binary.rs"]
mod binary;

#[test]
fn identity_processes() -> Result<(), Box<dyn std::error::Error>> {
    let status = std::process::Command::new("node")
        .arg("--test")
        .arg("tests/identity.test.mjs")
        .arg("tests/identity-mcp.test.mjs")
        .arg("tests/identity-agent.test.mjs")
        .current_dir(acyclic_test_support::source_path("plugin"))
        .env("ACYCLIC_TEST_BINARY", binary::acyclic())
        .status()?;
    assert!(status.success(), "identity process tests failed");
    Ok(())
}
