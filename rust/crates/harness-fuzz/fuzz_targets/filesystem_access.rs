#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    acyclic_harness::filesystem::check_access_record(bytes)
        .expect("bounded access record roundtrip");
});
