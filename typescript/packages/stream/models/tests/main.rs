//! Real TypeScript transport, queue and latency model drivers.
#[path = "../../fuzz/driver.rs"]
mod driver;
#[path = "../latency.rs"]
mod latency;
#[path = "../session_model.rs"]
mod session_model;

#[test]
fn wasm_hosted_record_projection() -> std::io::Result<()> {
    let output = std::process::Command::new("bun")
        .args([
            "test",
            "./node_modules/@acyclic-labs/stream/test/record-origin.test.ts",
        ])
        .current_dir(acyclic_test_support::source_path(""))
        .output()?;
    assert!(
        output.status.success(),
        "hosted record projection failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn completed(result: &std::io::Result<()>) {
    assert!(result.is_ok(), "real TypeScript model failed: {result:?}");
}

fn field<'a>(value: &'a serde_json::Value, name: &str) -> std::io::Result<&'a serde_json::Value> {
    value.get(name).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("real TypeScript model result has no {name:?} field: {value}"),
        )
    })
}
