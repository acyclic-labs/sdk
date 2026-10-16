use acyclic_fs::{
    VolumeLimits,
    kernel::{LogicalName, NameEncoding},
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
    let limits = VolumeLimits::default();
    if let Ok(text) = std::str::from_utf8(data)
        && let Ok(path) = PortablePath::parse(text, limits)
    {
        assert_eq!(path.as_str(), text, "parsing silently normalized a path");
        assert_eq!(PortablePath::parse(path.as_str(), limits).unwrap(), path);
        assert!(path.is_within(&path));
        assert!(path.is_within(&PortablePath::parse("/", limits).unwrap()));
    }
    for chunk in data.chunks(64) {
        for encoding in [NameEncoding::PosixBytes, NameEncoding::WindowsUtf16Le] {
            if let Ok(name) =
                LogicalName::new(encoding, chunk.to_vec(), limits.maximum_component_bytes)
            {
                assert_eq!(
                    name.case_fold_key(),
                    chunk,
                    "native encoding was Unicode folded"
                );
            }
        }
        if let Ok(name) = LogicalName::new(
            NameEncoding::Utf8,
            chunk.to_vec(),
            limits.maximum_component_bytes,
        ) {
            let text = std::str::from_utf8(name.as_bytes()).unwrap();
            let alias = LogicalName::new(
                NameEncoding::Utf8,
                text.to_ascii_uppercase().into_bytes(),
                limits.maximum_component_bytes,
            )
            .unwrap();
            assert_eq!(name.case_fold_key(), alias.case_fold_key());
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
    assert_eq!(name.case_fold_key(), expected.as_bytes());
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
