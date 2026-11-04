#![no_main]
use libfuzzer_sys::fuzz_target;

#[path = "../../rust/crates/conformance/tests/support/inference_manifest.rs"]
mod oracle;

fuzz_target!(|bytes: &[u8]| {
    static ORIGINAL: std::sync::Once = std::sync::Once::new();
    ORIGINAL.call_once(oracle::recorded);
    oracle::arbitrary(bytes);
});
