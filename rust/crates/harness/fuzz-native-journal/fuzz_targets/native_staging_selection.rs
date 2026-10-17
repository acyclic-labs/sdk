#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &str| {
    if input.len() <= 4096 {
        acyclic_fs::native_mount::verify_native_staging_selection(input);
    }
});
