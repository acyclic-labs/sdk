#![no_main]

use acyclic_harness_core::filesystem::execution_codec_fuzz;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    execution_codec_fuzz::check(bytes);
});
