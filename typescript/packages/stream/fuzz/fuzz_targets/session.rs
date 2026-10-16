#![no_main]
use libfuzzer_sys::fuzz_target;
use std::{
    path::Path,
    sync::{Mutex, OnceLock},
};
#[path = "../driver.rs"]
mod driver;

fuzz_target!(|input: &[u8]| {
    if input.len() > 4096 {
        return;
    }
    static DRIVER: OnceLock<Mutex<driver::Driver>> = OnceLock::new();
    let shared = DRIVER.get_or_init(|| {
        Mutex::new(driver::Driver::configured(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.."),
        , "decode", ""))
    });
    let hex: String = input.iter().map(|byte| format!("{byte:02x}")).collect();
    let result = shared
        .lock()
        .unwrap()
        .call(serde_json::json!({"action":"decode", "hex":hex}));
    // Branches expose JavaScript validation outcomes to libFuzzer's feedback;
    // LLVM coverage here instruments the Rust bridge, not the JS engine.
    match result["outcome"].as_str() {
        Some("record") => assert_eq!(result["sequence"], 0),
        Some("invalid_response") => assert!(result["valid"].as_bool().unwrap()),
        Some("pending") => assert!(result["valid"].as_bool().unwrap()),
        Some("invalid") => assert!(!result["valid"].as_bool().unwrap()),
        _ => assert!(result["valid"].as_bool().unwrap()),
    }
});
