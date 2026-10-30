#![no_main]
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| assert!(acyclic_objects::codec_fuzz::check(bytes)));
