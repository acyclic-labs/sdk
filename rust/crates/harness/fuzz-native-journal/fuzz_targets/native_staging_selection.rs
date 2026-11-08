//! Check arbitrary staging selections against the production path oracle.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &str| acyclic_fs::native_mount::verify_native_staging_selection(input));
