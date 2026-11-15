//! Checks every Stream trailer status against the shared status contract.
#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    let checked = acyclic_stream::wire_codec::status_contract::check(input);
    assert_eq!(checked, Ok(()), "Stream trailer status contract");
});
