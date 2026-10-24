#![no_main]

use acyclic_stream::test_support::follow_contract as checks;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    // A non-palindromic anchor detects symmetric encoder/decoder corruption.
    let mut cursor = vec![1, 8, 1, 18, 32];
    cursor.extend(0..32);
    for fault in [
        checks::Fault::ReverseEncode,
        checks::Fault::ReverseDecode,
        checks::Fault::ReverseBoth,
    ] {
        assert!(checks::check(&cursor, fault).is_err());
    }
    checks::check(input, checks::Fault::None).expect("follow wire contract");
});
