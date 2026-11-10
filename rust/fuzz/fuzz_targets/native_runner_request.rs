#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    acyclic_native_runtime::fuzz_runner_request(bytes);
});
