//! Pins the CLI's wire JSON to Cloud's recorded Global handler tests.

#[path = "support/binary.rs"]
mod binary;

#[test]
fn cloud_identity_contract() -> Result<(), Box<dyn std::error::Error>> {
    let status = std::process::Command::new("node")
        .arg("--test")
        .arg("tests/identity-contract.test.mjs")
        .current_dir(acyclic_test_support::source_path("plugin"))
        .env("ACYCLIC_TEST_BINARY", binary::acyclic())
        .status()?;
    assert!(
        status.success(),
        "Cloud identity JSON contract tests failed"
    );
    Ok(())
}
