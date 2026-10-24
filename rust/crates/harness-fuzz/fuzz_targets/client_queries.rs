#![no_main]
#[path = "../support/queries.rs"]
mod queries;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    queries::exercise(bytes);
});
