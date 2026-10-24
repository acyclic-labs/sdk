//! Callback path codecs without intermediate platform strings.

#[path = "../../src/native_mount/projfs/callback_path.rs"]
mod callback_path;
use proptest::prelude::*;
#[cfg(windows)]
#[path = "../support/projfs_legacy_path.rs"]
mod legacy;

fn units(path: &str) -> Vec<u16> {
    path.encode_utf16().collect()
}

fn wide_paths() -> impl Strategy<Value = Vec<u16>> {
    proptest::collection::vec(prop_oneof![4 => 0u16..128, 1 => any::<u16>()], 0..256)
}

fn encoded(parts: &[Vec<u8>]) -> Vec<u16> {
    let mut wide = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        if index != 0 {
            wide.push(92);
        }
        wide.extend(
            part.chunks_exact(2)
                .map(|pair| u16::from_le_bytes([pair[0], pair[1]])),
        );
    }
    wide
}

#[test]
fn callback_paths_keep_windows_component_rules() {
    for (input, expected) in [
        ("", Some(vec![])),
        ("a", Some(vec![units("a")])),
        ("a\\b/./c/", Some(vec![units("a"), units("b"), units("c")])),
        ("a//b\\\\c", Some(vec![units("a"), units("b"), units("c")])),
        ("1:a", Some(vec![units("1:a")])),
        ("é:a", Some(vec![units("é:a")])),
        ("a/.:", Some(vec![units("a"), units(".:")])),
        (".", None),
        (".\\a", None),
        ("a/../b", None),
        ("/a", None),
        ("\\a", None),
        ("C:a", None),
        ("z:", None),
        ("C:\\a", None),
        ("\\\\server\\share", None),
        ("\\\\?\\C:\\a", None),
        ("\\\\.\\pipe\\a", None),
    ] {
        let actual = callback_path::components(&units(input));
        let expected = expected.map(|parts| {
            parts
                .into_iter()
                .map(|part| {
                    part.into_iter()
                        .flat_map(u16::to_le_bytes)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        });
        assert_eq!(actual, expected, "{input:?}");
    }
    let opaque = vec![0xd800, 92, 0xdc00];
    assert_eq!(
        encoded(&callback_path::components(&opaque).expect("opaque names")),
        opaque
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn accepted_callback_paths_round_trip(wide in wide_paths()) {
        if let Some(parts) = callback_path::components(&wide) {
            prop_assert_eq!(parts.capacity(), parts.len());
            for part in &parts {
                prop_assert_eq!(part.capacity(), part.len());
            }
            // Canonical relative paths round-trip, including unpaired surrogates.
            prop_assert_eq!(callback_path::components(&encoded(&parts)), Some(parts));
        }
    }

    #[test]
    fn direct_metadata_paths_round_trip(parts in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..64), 0..8)) {
        if let Some(wide) = callback_path::wide_components(&parts) {
            let wide: Vec<_> = wide.collect();
            let decoded = callback_path::components(&wide);
            prop_assert_eq!(decoded.as_ref(), Some(&parts));
        }
    }

    #[cfg(windows)]
    #[test]
    fn direct_metadata_paths_match_windows_paths(wide in wide_paths()) {
        use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
        if let Some(parts) = callback_path::components(&wide)
            && let Some(direct) = callback_path::wide_components(&parts)
        {
            let mut host = std::path::PathBuf::new();
            for part in &parts {
                let wide: Vec<_> = part.chunks_exact(2).map(|p| u16::from_le_bytes([p[0], p[1]])).collect();
                host.push(std::ffi::OsString::from_wide(&wide));
            }
            prop_assert_eq!(direct.collect::<Vec<_>>(), host.as_os_str().encode_wide().collect::<Vec<_>>());
        }
    }

    #[cfg(windows)]
    #[test]
    fn callback_paths_match_windows_standard_components(wide in wide_paths()) {
        let expected = legacy::components(&wide);
        prop_assert_eq!(callback_path::components(&wide), expected);
    }
}

#[test]
fn noncanonical_metadata_parts_keep_the_platform_fallback() {
    for part in [
        vec![],
        vec![1],
        vec![46, 0],
        vec![46, 0, 46, 0],
        vec![0, 0],
        vec![47, 0],
        vec![58, 0],
        vec![92, 0],
    ] {
        assert!(callback_path::wide_components(&[part]).is_none());
    }
    assert_eq!(
        callback_path::wide_components(&[]).map(Iterator::count),
        Some(0)
    );
    let parts = vec![vec![0, 0xd8], vec![0, 0xdc]];
    assert_eq!(
        callback_path::wide_components(&parts).map(Iterator::collect::<Vec<_>>),
        Some(vec![0xd800, 92, 0xdc00])
    );
}
