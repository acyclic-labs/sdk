//! Lossless Windows path spelling and prefix equivalence.
use std::{
    borrow::Cow,
    path::{Component, Path, PathBuf},
};

// Encoding is the only platform adapter. The namespace decision itself is shared.
fn strip_units<T: Copy + PartialEq + From<u8>>(units: &[T]) -> Option<Vec<T>> {
    let tail = units.strip_prefix(&br"\\?\".map(T::from))?;
    if tail.get(1) == Some(&T::from(b':'))
        && tail.first().is_some_and(|drive| {
            (b'a'..=b'z')
                .chain(b'A'..=b'Z')
                .any(|letter| *drive == T::from(letter))
        })
    {
        Some(tail.to_vec())
    } else {
        let share = tail.strip_prefix(&br"UNC\".map(T::from))?;
        let mut normal = br"\\".map(T::from).to_vec();
        normal.extend_from_slice(share);
        Some(normal)
    }
}

/// Removes only drive and UNC verbatim prefixes; preserves every native code unit.
#[must_use]
pub fn strip_verbatim(path: &Path) -> Cow<'_, Path> {
    #[cfg(windows)]
    let normal = {
        use std::{
            ffi::OsString,
            os::windows::ffi::{OsStrExt as _, OsStringExt as _},
        };
        strip_units(&path.as_os_str().encode_wide().collect::<Vec<_>>())
            .map(|units| PathBuf::from(OsString::from_wide(&units)))
    };
    #[cfg(not(windows))]
    let normal = path.to_str().and_then(|text| {
        strip_units(text.as_bytes())
            .and_then(|units| String::from_utf8(units).ok())
            .map(PathBuf::from)
    });
    normal.map_or(Cow::Borrowed(path), Cow::Owned)
}

/// A local Windows drive with the verbatim namespace, rather than a UNC share.
#[must_use]
pub fn is_local_verbatim(path: &Path) -> bool {
    #[cfg(windows)]
    {
        matches!(path.components().next(), Some(Component::Prefix(prefix)) if matches!(prefix.kind(), std::path::Prefix::VerbatimDisk(_)))
    }
    #[cfg(not(windows))]
    {
        let _ = path;
        false
    }
}

/// Returns a borrowed suffix after matching original path components. Windows
/// drive/UNC prefixes admit equivalent verbatim spelling; leaf names stay exact.
#[must_use]
pub fn strip_native_prefix<'a>(path: &'a Path, root: &Path) -> Option<&'a Path> {
    let mut selected = path.components();
    for expected in root.components() {
        let actual = selected.next()?;
        if !equivalent_component(actual, expected) {
            return None;
        }
    }
    Some(selected.as_path())
}

fn equivalent_component(actual: Component<'_>, expected: Component<'_>) -> bool {
    #[cfg(windows)]
    if let (Component::Prefix(actual), Component::Prefix(expected)) = (actual, expected) {
        use std::path::Prefix;
        let disk = |prefix| match prefix {
            Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => Some(drive.to_ascii_uppercase()),
            _ => None,
        };
        if let (Some(actual), Some(expected)) = (disk(actual.kind()), disk(expected.kind())) {
            return actual == expected;
        }
        let share = |prefix| match prefix {
            Prefix::UNC(server, share) | Prefix::VerbatimUNC(server, share) => {
                Some((server, share))
            }
            _ => None,
        };
        if let (Some(actual), Some(expected)) = (share(actual.kind()), share(expected.kind())) {
            return actual == expected;
        }
    }
    actual == expected
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prefix_conversion_preserves_arbitrary_native_suffix_units(suffix in proptest::collection::vec(any::<u16>(), 0..100)) {
            let mut drive = br"\\?\C:\".map(u16::from).to_vec(); drive.extend_from_slice(&suffix);
            let mut expected = br"C:\".map(u16::from).to_vec(); expected.extend_from_slice(&suffix);
            prop_assert_eq!(strip_units(&drive), Some(expected));
            let mut unc = br"\\?\UNC\host\share\".map(u16::from).to_vec(); unc.extend_from_slice(&suffix);
            let mut expected = br"\\host\share\".map(u16::from).to_vec(); expected.extend_from_slice(&suffix);
            prop_assert_eq!(strip_units(&unc), Some(expected));
            let mut other = br"\\?\Volume{1}\".map(u16::from).to_vec(); other.extend_from_slice(&suffix);
            prop_assert_eq!(strip_units(&other), None);
        }
    }

    #[test]
    fn path_conversion_keeps_ordinary_paths_and_component_boundaries() {
        for (input, expected) in [
            (r"\\?\C:\a\é", r"C:\a\é"),
            (r"\\?\UNC\host\share\é", r"\\host\share\é"),
            (r"\\?\Volume{1}\a", r"\\?\Volume{1}\a"),
            ("/work/a", "/work/a"),
        ] {
            assert_eq!(strip_verbatim(Path::new(input)), Path::new(expected));
        }
        assert_eq!(
            strip_native_prefix(Path::new("/a/b/c"), Path::new("/a/b")),
            Some(Path::new("c"))
        );
        assert!(strip_native_prefix(Path::new("/a/bc/c"), Path::new("/a/b")).is_none());
    }

    #[cfg(windows)]
    #[test]
    fn windows_paths_keep_unpaired_surrogates_and_only_equivalent_prefixes_match() {
        use std::{
            ffi::OsString,
            os::windows::ffi::{OsStrExt as _, OsStringExt as _},
        };
        let mut units = br"\\?\C:\a\".map(u16::from).to_vec();
        units.extend([0xd800, u16::from(b'x'), 0xdcff]);
        let path = PathBuf::from(OsString::from_wide(&units));
        let actual = strip_verbatim(&path);
        assert_eq!(
            actual.as_os_str().encode_wide().collect::<Vec<_>>(),
            units[4..]
        );
        for (path, root, expected) in [
            (r"C:\a\b", r"\\?\C:\a", Some("b")),
            (r"\\?\C:\a\b", r"C:\a", Some("b")),
            (r"D:\a\b", r"C:\a", None),
            (r"\\?\UNC\host\share\a", r"\\host\share", Some("a")),
        ] {
            assert_eq!(
                strip_native_prefix(Path::new(path), Path::new(root)),
                expected.map(Path::new)
            );
        }
    }
}
