use acyclic_fs::{
    VolumeLimits,
    kernel::{LogicalName, NameEncoding, TreePageError},
    path::PortablePath,
};

const FOLDS: &[(&str, &str)] = &[
    ("A", "a"),
    ("Straße", "strasse"),
    ("ẞ", "ss"),
    ("ΟΣ", "οσ"),
    ("ος", "οσ"),
    ("ΟΣΑ", "οσα"),
    ("İ", "i\u{307}"),
    ("ﬃ", "ffi"),
    ("K", "k"),
    ("Ꭰꭰ", "ᎠᎠ"),
];

pub fn check(data: &[u8]) {
    check_with_lowercase_fold(data, false);
}

pub fn check_with_lowercase_fold(data: &[u8], broken: bool) {
    let limits = VolumeLimits::default();
    if let Ok(text) = std::str::from_utf8(data)
        && let Ok(path) = PortablePath::parse(text, limits)
    {
        assert_eq!(path.as_str(), text, "parsing silently normalized a path");
        assert_eq!(PortablePath::parse(path.as_str(), limits).unwrap(), path);
        assert!(path.is_within(&path));
        assert!(path.is_within(&PortablePath::parse("/", limits).unwrap()));
    }
    native_names(data, LogicalName::new);
    if let Ok(text) = std::str::from_utf8(data) {
        for component in text.split('/') {
            let Ok(name) = LogicalName::new(
                NameEncoding::Utf8,
                component.as_bytes().to_vec(),
                limits.maximum_component_bytes,
            ) else {
                continue;
            };
            let alias = LogicalName::new(
                NameEncoding::Utf8,
                component.to_ascii_uppercase().into_bytes(),
                limits.maximum_component_bytes,
            )
            .unwrap();
            let key = name.case_fold_key();
            assert_eq!(key, alias.case_fold_key());
            if let Ok(folded) = LogicalName::new(
                NameEncoding::Utf8,
                key.clone(),
                limits.maximum_component_bytes,
            ) {
                assert_eq!(folded.case_fold_key(), key);
            }
        }
    }
    // Known full-fold mappings compose without locale or surrounding context.
    // This catches lowercase-only folding, final-sigma rules and lost expansions.
    let parts: Vec<_> = data
        .iter()
        .take(16)
        .copied()
        .chain(std::iter::once(0))
        .map(|index| FOLDS[usize::from(index) % FOLDS.len()])
        .collect();
    let text: String = parts.iter().map(|&(text, _)| text).collect();
    let expected: String = parts.iter().map(|&(_, folded)| folded).collect();
    let name = LogicalName::new(
        NameEncoding::Utf8,
        text.into_bytes(),
        limits.maximum_component_bytes,
    )
    .unwrap();
    let folded_key = if broken {
        std::str::from_utf8(name.as_bytes())
            .unwrap()
            .to_lowercase()
            .into_bytes()
    } else {
        name.case_fold_key()
    };
    assert_eq!(
        folded_key,
        expected.as_bytes(),
        "full Unicode case fold was lost"
    );
    let folded = LogicalName::new(
        NameEncoding::Utf8,
        expected.into_bytes(),
        limits.maximum_component_bytes,
    )
    .unwrap();
    assert_eq!(folded.case_fold_key(), folded.as_bytes());
    let path = format!(
        "/{}",
        parts
            .iter()
            .map(|&(text, _)| text)
            .collect::<Vec<_>>()
            .join("/")
    );
    let canonical = PortablePath::parse(&path, limits).unwrap();
    assert_eq!(canonical.depth(), parts.len());
    for suffix in ["/", "//leaf", "/.", "/..", "/\0"] {
        assert!(PortablePath::parse(&format!("{path}{suffix}"), limits).is_err());
    }
    let child = PortablePath::parse(&format!("{path}/child"), limits).unwrap();
    let sibling = PortablePath::parse(&format!("{path}-sibling"), limits).unwrap();
    assert!(child.is_within(&canonical));
    assert!(!sibling.is_within(&canonical));
    assert!(!canonical.is_within(&child));
}

#[cfg(test)]
pub fn check_with_rejected_native_names(data: &[u8], rejected: NameEncoding) {
    native_names(data, |encoding, bytes, maximum| {
        let result = LogicalName::new(encoding, bytes, maximum);
        if encoding == rejected {
            Err(TreePageError::InvalidName)
        } else {
            result
        }
    });
}

fn native_names(
    data: &[u8],
    new: impl Fn(NameEncoding, Vec<u8>, u32) -> Result<LogicalName, TreePageError>,
) {
    let maximum = VolumeLimits::default().maximum_component_bytes;
    // Prefixes exclude dot components and force opaque, non-Unicode examples.
    // The empty chunk ensures even an empty fuzz input requires both encodings.
    for chunk in data.chunks(64).chain(std::iter::once(&[][..])) {
        let posix: Vec<_> = [b'n', 0xff, b'\\']
            .into_iter()
            .chain(chunk.iter().map(|&byte| match byte {
                0 | b'/' => b'x',
                byte => byte,
            }))
            .collect();
        let windows: Vec<_> = std::iter::once(u16::from(b'n'))
            .chain(chunk.chunks(2).map(|pair| {
                match u16::from_le_bytes([pair[0], pair.get(1).copied().unwrap_or(0)]) {
                    0 | 47 | 92 => u16::from(b'x'),
                    unit => unit,
                }
            }))
            .chain([0xd800, 0xd800])
            .flat_map(u16::to_le_bytes)
            .collect();
        for (encoding, bytes) in [
            (NameEncoding::PosixBytes, posix),
            (NameEncoding::WindowsUtf16Le, windows),
        ] {
            let name =
                new(encoding, bytes.clone(), maximum).expect("valid native name was rejected");
            assert_eq!(name.encoding(), encoding, "native encoding changed");
            assert_eq!(name.as_bytes(), bytes, "native byte spelling changed");
            assert_eq!(
                name.case_fold_key(),
                bytes,
                "native encoding was Unicode folded"
            );
            let length = u32::try_from(bytes.len()).unwrap();
            assert_eq!(
                new(encoding, bytes.clone(), length).expect("exact native byte bound was rejected"),
                name
            );
            assert!(new(encoding, bytes.clone(), length - 1).is_err());
            for unit in [0, u16::from(b'/'), u16::from(b'\\')] {
                if encoding == NameEncoding::PosixBytes && unit == u16::from(b'\\') {
                    continue;
                }
                let mut invalid = bytes.clone();
                if encoding == NameEncoding::WindowsUtf16Le {
                    invalid.extend_from_slice(&unit.to_le_bytes());
                } else {
                    invalid.push(u8::try_from(unit).unwrap());
                }
                assert!(
                    new(encoding, invalid, maximum).is_err(),
                    "invalid native name was accepted"
                );
            }
        }
    }
    for invalid in ["", ".", ".."] {
        assert!(
            new(
                NameEncoding::PosixBytes,
                invalid.as_bytes().to_vec(),
                maximum
            )
            .is_err()
        );
        assert!(
            new(
                NameEncoding::WindowsUtf16Le,
                invalid.encode_utf16().flat_map(u16::to_le_bytes).collect(),
                maximum,
            )
            .is_err()
        );
    }
    assert!(new(NameEncoding::WindowsUtf16Le, vec![b'n'], maximum).is_err());
}
