#![no_main]
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_codec_fuzz::object_schedule::check(bytes).unwrap_or_else(|error| {
        panic!("real Medium schedule failed: {error}; recovery: cargo +nightly fuzz run --fuzz-dir . --features storage-schedules object_schedule")
    });
});
