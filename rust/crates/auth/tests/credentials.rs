//! Fresh opaque credentials obey bounded admission without caching or fallback.
use acyclic_auth::credential::*;
use futures::{executor::block_on, future};
use proptest::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn credential_admission_matches_the_bounded_header_contract(value in prop_oneof![
        any::<String>(),
        (0..=MAX_CREDENTIAL_BYTES + 2, any::<bool>()).prop_map(|(length, unicode)|
            if unicode { "é".repeat(length) } else { "a".repeat(length) }),
    ]) {
        let expected = value.len() <= 12288 && value.chars().any(|c| !c.is_whitespace())
            && !value.bytes().any(|c| [0, 10, 13].contains(&c));
        prop_assert_eq!(valid_credential(&value), expected);
        let provider = CredentialProvider::new(move || future::ready(Ok(value.clone())));
        prop_assert_eq!(block_on(provider.resolve()).is_ok(), expected);
    }
    #[test]
    fn each_resolution_owns_the_current_value_without_caching(values in prop::collection::vec("[a-zA-Z0-9_-]{1,128}", 1..16)) {
        let index = Arc::new(AtomicUsize::new(0));
        let calls = index.clone();
        let expected = values.clone();
        let provider = CredentialProvider::new(move || {
            let value = values.get(calls.fetch_add(1, Ordering::Relaxed)).cloned();
            future::ready(value.ok_or(CredentialError::Invalid))
        });
        let clone = provider.clone();
        for value in &expected {
            let resolved = block_on(clone.resolve()).unwrap();
            prop_assert_eq!(resolved.as_str(), value.as_str());
        }
        prop_assert_eq!(index.load(Ordering::Relaxed), expected.len());
        prop_assert!(block_on(provider.resolve()).is_err());
        prop_assert_eq!(format!("{provider:?}"), "CredentialProvider(..)");
    }
}

#[test]
fn byte_limits_delimiters_and_provider_failure_fail_closed() {
    for (value, valid) in [
        ("a".repeat(12288), true),
        ("a".repeat(12289), false),
        ("é".repeat(6144), true),
        ("é".repeat(6145), false),
        ("a\0b".into(), false),
        ("a\rb".into(), false),
        ("a\nb".into(), false),
    ] {
        assert_eq!(valid_credential(&value), valid);
    }
    let provider = CredentialProvider::new(|| {
        future::ready(Err(CredentialError::Provider(Box::new(
            std::io::Error::other("renewal unavailable"),
        ))))
    });
    assert!(matches!(
        block_on(provider.resolve()),
        Err(CredentialError::Provider(_))
    ));
}
