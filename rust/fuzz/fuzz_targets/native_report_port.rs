//! Exercise the public native report decoder and its single request admission.
#![no_main]

#[path = "../../crates/native-runtime/tests/report_port/oracle.rs"]
mod oracle;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    let result = oracle::check(input);
    assert!(
        result.is_ok(),
        "original report oracle setup failed: {result:?}"
    );
});
