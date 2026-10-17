//! Canonical byte inputs through the original accounting observer.
#![no_main]
libfuzzer_sys::fuzz_target!(|input: &[u8]| acyclic_fs::kernel::decoder_fuzz::check(input));
