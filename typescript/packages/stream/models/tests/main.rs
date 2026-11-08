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
