#![no_main]

#[path = "../../harness/src/store/effect_certificate.rs"]
mod codec;
pub use acyclic_fuzz_support as fuzz_support;
#[path = "../../harness/tests/support/effect_certificate.rs"]
mod oracle;

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    let actual = fuzz_support::FuzzInput::optional(fuzz_input, codec::decode);
    assert_eq!(
        actual.map(|(root, proof)| (root.copied(), *proof)),
        oracle::reference(input)
    );
    if let Some((root, proof)) = actual {
        assert_eq!(codec::parts(root, proof).concat(), input);
    }
});
