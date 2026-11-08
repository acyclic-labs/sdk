#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    acyclic_harness_core::native_settings::codec_fuzz::check(bytes).unwrap();
    acyclic_harness_core::hooks::codec_fuzz::check(bytes);
});
