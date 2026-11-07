//! Exercise the public native owner-envelope implementation and OS protection.
#![no_main]

#[path = "../../crates/native-runtime/tests/owner_codec/oracle.rs"]
mod native_owner_record;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let result = native_owner_record::check(bytes);
    assert!(
        result.is_ok(),
        "original owner oracle setup failed: {result:?}"
    );
});
