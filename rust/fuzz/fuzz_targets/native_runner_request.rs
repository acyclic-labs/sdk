#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    let checked = acyclic_native_runtime::fuzz_runner_request(bytes);
    assert!(checked.is_ok(), "{checked:?}");
});
