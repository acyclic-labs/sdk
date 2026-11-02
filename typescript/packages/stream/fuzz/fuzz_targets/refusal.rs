#![no_main]
//! Exercises the shipping shared foreign-property and product error projectors.
//! Generated property inputs do not claim to be captured service responses.
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
            "refusal",
            "",
        ))
    });
    let hex: String = input.iter().map(|byte| format!("{byte:02x}")).collect();
    let result = shared
        .lock()
        .unwrap()
        .run()
        .call(serde_json::json!({"action":"refusal", "hex":hex}));
    assert_eq!(result["valid"], true);
});
