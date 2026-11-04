#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("start the original blocking-pool driver")
        })
        .block_on(acyclic_harness_core::executor::check_code_mode(input));
});
