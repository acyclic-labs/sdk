//! Exercise native encoder admission and accounting boundaries.
#![no_main]
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| assert!(
    acyclic_fs::kernel::encoder_fuzz::check(bytes).is_ok()
));
