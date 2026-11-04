//! Exercise portable paths and the production operation-lease formatter.
#![no_main]

#[path = "../../tests/path_input.rs"]
mod input;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if let Some(input) = data.first_chunk::<41>() {
        acyclic_fs_lease_path_proof::check_formatter_input(input);
    }
    if data.len() <= 4096 {
        input::check(data);
    }
});
