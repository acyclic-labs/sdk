#![no_main]
#[path = "../../harness/src/store/effect_certificate.rs"]
mod codec;
#[path = "../../harness/tests/support/effect_certificate.rs"]
mod oracle;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    let actual = codec::decode(input);
    assert_eq!(
        actual.map(|(root, proof)| (root.copied(), *proof)),
        oracle::reference(input)
    );
    if let Some((root, proof)) = actual {
        assert_eq!(codec::parts(root, proof).concat(), input);
    }
});
