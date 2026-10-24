#![no_main]
libfuzzer_sys::fuzz_target!(|input: &[u8]| acyclic_fs::native_mount::fuzz_callback_path(input));
