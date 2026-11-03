#![no_main]

#[path = "../native_answer_oracle.rs"]
mod oracle;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    oracle::check(bytes);
});
