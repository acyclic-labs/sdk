//! Exercises the shared workspace-name decoder with arbitrary bytes.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    acyclic_fs::workspace::check_workspace_name(data);
});
