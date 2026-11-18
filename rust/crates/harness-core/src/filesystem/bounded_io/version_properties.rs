//! FileRef version format v1: every issued spelling of a cut parses back to
//! that exact cut, and no other spelling of it parses.
use super::*;
use crate::resources::{GenerationRef, ProviderRef, ResourceKind, ResourceRef};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

fn ok<T>(result: Result<T>) -> std::result::Result<T, TestCaseError> {
    result.map_err(|error| TestCaseError::fail(error.to_string()))
}

fn private_cut(provider: &ProviderRef, key: Vec<u8>) -> Result<GenerationRef> {
    let resource = ResourceRef::new(
        ResourceKind::Generation,
        provider.clone(),
        key,
        Some(PRIVATE_CUT_FORMAT.into()),
        &mut None,
    )?;
    GenerationRef::from_resource(resource, &mut None)
}

proptest! {
    #[test]
    fn issued_versions_round_trip_and_every_other_spelling_is_refused(
        digest in any::<[u8; 32]>(),
        key in proptest::collection::vec(any::<u8>(), 1..256),
        prefix in "[a-z0-9-]{1,12}",
        authored in any::<bool>(),
    ) {
        let provider = ok(ProviderRef::new(
            "version-properties".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        ))?;
        let cut = if authored {
            let id = acyclic_fs::GenerationId::new(
                acyclic_fs_format::identity::Digest::from_bytes(digest),
            );
            ok(generation_ref(&provider, id, None))?
        } else {
            ok(private_cut(&provider, key))?
        };
        let version = ok(file_version(&cut, None))?;
        prop_assert_eq!(&ok(version_generation(&provider, &version, None))?, &cut);

        let parsed = |spelling: &str| version_generation(&provider, spelling, None).ok();
        let (head, body) = match version.rsplit_once(':') {
            Some((format, body)) => (format!("{format}:"), body),
            None => (String::new(), version.as_str()),
        };
        // Uppercase hex is another spelling of the same key.
        if body.bytes().any(|byte| byte.is_ascii_lowercase()) {
            let upper = format!("{head}{}", body.to_ascii_uppercase());
            prop_assert!(parsed(&upper).is_none());
        }
        // A truncated key has odd length.
        prop_assert!(parsed(&version[..version.len() - 1]).is_none());
        // An unknown format names no cut.
        if prefix != PRIVATE_CUT_FORMAT {
            let unknown = format!("{prefix}:{body}");
            prop_assert!(parsed(&unknown).is_none());
        }
        // Moving the key across formats never names this cut.
        let moved = if authored {
            format!("{PRIVATE_CUT_FORMAT}:{body}")
        } else {
            body.to_owned()
        };
        prop_assert_ne!(parsed(&moved), Some(cut));
    }
}
