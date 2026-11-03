//! Mutate real persisted operation-window records through the public store.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    acyclic_fs::operation_window::check_persisted_window(data);
});
