//! Real TypeScript transport, queue and latency model drivers.
#[path = "../../fuzz/driver.rs"]
mod driver;
#[path = "../latency.rs"]
mod latency;
#[path = "../session_model.rs"]
mod session_model;

fn completed(result: std::io::Result<()>) {
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
