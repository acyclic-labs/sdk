use super::*;
use crate::kernel::NameEncoding;
use crate::model::VolumeLimits;
use proptest::prelude::*;

// Original workspace projection, independent of the shared path renderer.
fn original_text(path: &NamespacePath) -> Result<String, String> {
    if path.is_root() {
        return Ok("/".to_owned());
    }
    let components = path
        .components()
        .iter()
        .map(logical_name_text)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    Ok(format!("/{}", components.join("/")))
}

// Frozen tags used by both original conflict keys and capture-policy digests.
fn original_tag(encoding: NameEncoding) -> u8 {
    match encoding {
        NameEncoding::Utf8 => 1,
        NameEncoding::PosixBytes => 2,
        NameEncoding::WindowsUtf16Le => 3,
    }
}

fn check(path: &NamespacePath) {
    let (text, [before, after]) = acyclic_test_support::differential(
        || original_text(path),
        || namespace_path_text(path).map_err(|error| error.to_string()),
    );
    assert!(
        after.count_total <= before.count_total,
        "{before:?} -> {after:?}"
    );
    if text.is_ok() && !path.is_root() {
        assert!(
            after.count_total < before.count_total,
            "{before:?} -> {after:?}"
        );
        assert!(
            after.bytes_total < before.bytes_total,
            "{before:?} -> {after:?}"
        );
    }
    assert_eq!(
        after.count_current, 1,
        "only the returned text or cause remains"
    );

    let directory_id = FileId::from_bytes([7; 16]);
    for name in path.components() {
        let mut expected = vec![original_tag(name.encoding())];
        expected.extend_from_slice(name.as_bytes());
        let encoded = canonical_name_key(name);
        assert_eq!(encoded, expected);
        let key = ConflictKey::Binding {
            directory_id,
            name: encoded,
        };
        assert_eq!(
            kernel_conflict(&key).unwrap(),
            MergeConflict::Binding {
                directory_id,
                name: name.clone(),
            }
        );
        for tag in [0, 4, 255] {
            let mut invalid = expected.clone();
            invalid[0] = tag;
            let key = ConflictKey::Binding {
                directory_id,
                name: invalid,
            };
            assert!(matches!(
                kernel_conflict(&key),
                Err(DrivenJoinError::StaleCandidate)
            ));
        }
    }

    #[cfg(feature = "native-watch")]
    if !path.is_root() {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-fs-capture-policy-v1\0");
        hasher.update(&(path.depth() as u64).to_le_bytes());
        for name in path.components() {
            hasher.update(&[original_tag(name.encoding())]);
            hasher.update(&(name.as_bytes().len() as u64).to_le_bytes());
            hasher.update(name.as_bytes());
        }
        assert_eq!(
            crate::CapturePolicy::excluding(vec![path.clone()])
                .unwrap()
                .fingerprint(),
            crate::foundation::Digest::from_bytes(*hasher.finalize().as_bytes())
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn shared_name_codecs_preserve_exact_paths_keys_and_policy_bytes(
        names in prop::collection::vec((0_u8..3, any::<bool>(),
            prop::collection::vec(any::<char>().prop_filter("separator-free", |c|
                !matches!(c, '\0' | '/' | '\\')), 0..16)), 0..16),
    ) {
        let components = names.into_iter().map(|(encoding, opaque, chars)| {
            let text = std::iter::once('a').chain(chars).collect::<String>();
            let (encoding, bytes) = match encoding {
                0 => (NameEncoding::Utf8, text.into_bytes()),
                1 => {
                    let mut bytes = text.into_bytes();
                    if opaque { bytes.push(0xff); }
                    (NameEncoding::PosixBytes, bytes)
                }
                _ => {
                    let mut bytes = text.encode_utf16().flat_map(u16::to_le_bytes).collect::<Vec<_>>();
                    if opaque { bytes.extend_from_slice(&0xd800_u16.to_le_bytes()); }
                    (NameEncoding::WindowsUtf16Le, bytes)
                }
            };
            LogicalName::new(encoding, bytes).unwrap()
        }).collect();
        check(&NamespacePath::new(components, VolumeLimits::default()).unwrap());
        let unicode = NamespacePath::from_text_in_profile("/é/𝄞", crate::model::FilesystemProfile::Windows,
            VolumeLimits::default()).unwrap();
        check(&unicode);
    }
}
