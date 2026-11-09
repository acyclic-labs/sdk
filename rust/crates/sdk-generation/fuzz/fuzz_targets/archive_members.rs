//! Compare admitted archive inventory and bodies with the original input.
#![no_main]
use sdk_generation::historical;
#[path = "../archive_members.rs"]
mod archive_members;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    archive_members::check_input(bytes);
});
