#![no_main]

// Compile the same installer validator used by the native activation command.
#[path = "../../../../plugin/src/install/certification.rs"]
mod certification;

use libfuzzer_sys::fuzz_target;
use serde_json::Value;

fuzz_target!(|input: &[u8]| {
    if input.len() > 1024 * 1024 {
        return;
    }
    if let Ok(mut value) = serde_json::from_slice::<Value>(input) {
        let digest = "0".repeat(64);
        if certification::valid(&value, &digest, "linux", "x86_64", "0.2.0") {
            let encoded = serde_json::to_vec(&value).expect("serializable JSON value");
            let decoded: Value = serde_json::from_slice(&encoded).expect("encoded JSON value");
            assert_eq!(decoded, value);
            assert!(certification::valid(
                &decoded, &digest, "linux", "x86_64", "0.2.0"
            ));
            value
                .as_object_mut()
                .expect("validated object")
                .insert("passed".into(), Value::Bool(false));
            assert!(!certification::valid(
                &value, &digest, "linux", "x86_64", "0.2.0"
            ));
        }
    }
});
