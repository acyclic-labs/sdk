#![no_main]

#[path = "../../tests/support/decoder_input.rs"]
mod input;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if data.len() <= 4096 {
        input::wire(data);
        input::journal(data);
    }
});
