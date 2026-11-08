#![no_main]

// The native settings loader selects project entries through this same module.
use acyclic_harness_core::native_settings::test_support::{
    workspace_equivalent, workspace_matches,
};

fn same(left: &str, right: &str, windows: bool) {
    assert!(workspace_equivalent(left, right, windows));
    assert!(workspace_equivalent(right, left, windows));
}

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    assert!(
        !workspace_equivalent("C:", "C:/", true),
        "workspace roots must remain distinct"
    );
    let text = String::from_utf8_lossy(bytes);
    let mut paths = text.split('\0');
    let left = paths.next().unwrap_or_default();
    let right = paths.next().unwrap_or_default();
    let third = paths.next().unwrap_or_default();
    for windows in [false, true] {
        same(left, left, windows);
        assert_eq!(
            workspace_equivalent(left, right, windows),
            workspace_equivalent(right, left, windows)
        );
        if workspace_equivalent(left, right, windows) && workspace_equivalent(right, third, windows)
        {
            same(left, third, windows);
        }
        assert!(!workspace_equivalent("", ".", windows));
    }
    assert!(workspace_matches(
        left,
        std::path::Path::new(left),
        std::path::Path::new(right)
    ));
    assert!(workspace_matches(
        right,
        std::path::Path::new(left),
        std::path::Path::new(right)
    ));
    assert!(!workspace_equivalent("C:/x", "D:/x", true));
    assert!(!workspace_equivalent("C:/ß", "C:/SS", true));
    same("C:/Σ", "C:/ς", true);
    assert!(!workspace_equivalent(
        r"\\.\server\share",
        r"\\server\share",
        true
    ));

    // Generate alias spellings from caller input, never from another normalizer.
    // These characters have one-character native case folds; ß is tested above.
    let parts: Vec<String> = text
        .split('\0')
        .map(|part| {
            part.chars()
                .filter(|character| {
                    character.is_ascii_alphabetic() || "éÉ猫Σσς".contains(*character)
                })
                .collect::<String>()
        })
        .filter(|part| !part.is_empty())
        .collect();
    let drive = char::from(b'A' + bytes.first().copied().unwrap_or_default() % 26);
    let canonical = format!("{drive}:\\root\\{}", parts.join("\\"));
    let server = parts.first().map(String::as_str).unwrap_or("server");
    let share = parts.get(1).map(String::as_str).unwrap_or("share");
    let unc = format!(r"\\{server}\{share}\{}", parts.join("\\"));
    for separator in ["/", "\\"] {
        for verbatim in [false, true] {
            for upper in [false, true] {
                let path = format!(
                    "{drive}:{separator}root{separator}{}",
                    parts.join(separator)
                );
                let path = if upper {
                    path.to_uppercase()
                } else {
                    path.to_lowercase()
                };
                let alias = format!("{}{path}{separator}.", if verbatim { r"\\?\" } else { "" });
                same(&canonical, &alias, true);
                assert!(!workspace_equivalent(
                    &canonical,
                    &format!("{alias}{separator}different"),
                    true
                ));
                assert!(!workspace_equivalent(
                    &canonical,
                    &canonical.replacen(":\\", ":", 1),
                    true
                ));
                let alias = format!(
                    "{}{server}{separator}{share}{separator}{}",
                    if verbatim { r"\\?\UNC\" } else { "//" },
                    parts.join(separator)
                );
                let alias = if upper {
                    alias.to_uppercase()
                } else {
                    alias.to_lowercase()
                };
                same(&unc, &alias, true);
                assert!(!workspace_equivalent(
                    &unc,
                    &format!("/{server}/{share}"),
                    true
                ));
                assert!(!workspace_equivalent(
                    &unc,
                    &format!("{alias}{separator}different"),
                    true
                ));
            }
        }
    }
    let posix = format!("/root/a{}", parts.join("/"));
    assert!(!workspace_equivalent(&posix, &posix.to_uppercase(), false));
    assert!(!workspace_equivalent(
        &posix,
        &posix.replace('/', "\\"),
        false
    ));
});
