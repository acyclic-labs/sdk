#![no_main]
#[path = "search_http_codec/input.rs"]
mod input;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| input::run(data));
