//! Replay every generated frame through the actual JavaScript session decoder.
#![no_main]
use libfuzzer_sys::fuzz_target;
use std::{io, path::Path};
#[path = "../driver.rs"]
mod driver;

fuzz_target!(|input: &[u8]| assert_eq!(check(input).map_err(|error| error.to_string()), Ok(())));

fn check(input: &[u8]) -> io::Result<()> {
    static DRIVER: driver::Shared = driver::Shared::new();
    let result = driver::Driver::shared(
        &DRIVER,
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.."),
        "decode",
        "",
    )?
    .run()?
    .call(&serde_json::json!({"action":"decode", "hex":driver::hex(input)?}))?;
    let outcome = result.get("outcome").and_then(serde_json::Value::as_str);
    let valid = result.get("valid").and_then(serde_json::Value::as_bool);
    // These branches retain feedback from the real JS validation outcomes.
    // Rust coverage instruments this bridge, not the JavaScript engine.
    match outcome {
        Some("record") => assert_eq!(
            result.get("sequence"),
            Some(&serde_json::json!(0)),
            "{result}"
        ),
        Some("invalid") => assert_eq!(valid, Some(false), "{result}"),
        _ => assert_eq!(valid, Some(true), "{result}"),
    }
    Ok(())
}
