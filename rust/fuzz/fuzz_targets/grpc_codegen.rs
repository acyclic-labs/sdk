//! Exercise the shipped structural observer on generated Rust and mutations.
#![no_main]

#[path = "../support/grpc_codegen.rs"]
mod oracle;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Ok(source) = std::str::from_utf8(bytes) {
        oracle::check(source);
    }
});
