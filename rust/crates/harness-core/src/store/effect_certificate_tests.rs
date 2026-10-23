use super::*;
use proptest::prelude::*;

#[path = "../../../harness/tests/support/effect_certificate.rs"]
mod oracle;
use oracle::reference;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn complete_binary_cuts_round_trip(root in proptest::option::of(any::<[u8;32]>()), proof in any::<[u8;64]>()) {
        let bytes = parts(root.as_ref(), &proof).concat();
        prop_assert_eq!(bytes.len(), LENGTH);
        let (selected, signed) = decode(&bytes).unwrap();
        prop_assert_eq!(selected.copied(), root);
        prop_assert_eq!(*signed, proof);
        prop_assert_eq!(reference(&bytes), Some((root, proof)));
        prop_assert!(std::ptr::eq(signed.as_ptr(), bytes[33..].as_ptr()));
    }

    #[test]
    fn every_accepted_input_has_one_exact_encoding(bytes in prop::collection::vec(any::<u8>(), 0..=112)) {
        let actual = decode(&bytes).map(|(root, proof)| (root.copied(), *proof));
        prop_assert_eq!(actual, reference(&bytes));
        if let Some((root, proof)) = decode(&bytes) {
            prop_assert_eq!(parts(root, proof).concat(), bytes);
        }
    }
}

#[test]
fn acceptance_oracle_detects_discarded_absence_bytes() {
    let mut malformed = [0; LENGTH];
    malformed[1] = 1;
    // Broken decoder ignores the root bytes when the tag says absent.
    let broken = Some((None, [0; 64]));
    assert_ne!(broken, reference(&malformed));
    assert!(decode(&malformed).is_none());
}
