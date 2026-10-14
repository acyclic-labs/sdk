//! Pins the CLI's wire JSON to Cloud's recorded Global handler tests.

#[test]
fn cloud_identity_contract() -> Result<(), Box<dyn std::error::Error>> {
    let status = std::process::Command::new("node")
        .arg("--test")
        .arg("tests/identity-contract.test.mjs")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("ACYCLIC_TEST_BINARY", env!("CARGO_BIN_EXE_acyclic"))
        .status()?;
    assert!(
        status.success(),
        "Cloud identity JSON contract tests failed"
    );
    Ok(())
}
