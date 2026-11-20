//! Checks the stream status wire contract for arbitrary encoded inputs.
#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    assert_eq!(
        acyclic_stream::wire_codec::status_contract::check(input),
        Ok(())
    );
});
