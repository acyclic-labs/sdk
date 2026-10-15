#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../../src/filesystem/native_execution/journal_allocation.rs"]
mod journal_allocation;
#[path = "../../src/filesystem/native_execution/journal_encoding.rs"]
mod journal_encoding;
#[path = "../../src/filesystem/native_execution/journal_verification.rs"]
mod journal_verification;

fuzz_target!(|input: &[u8]| {
    if input.len() <= 65_536 {
        journal_verification::verify_decode(input);
    }
});
