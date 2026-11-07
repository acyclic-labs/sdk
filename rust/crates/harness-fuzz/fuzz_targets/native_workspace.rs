#![no_main]

// The native settings loader selects project entries through this same module.
#[path = "../../harness-core/src/native_settings/workspace.rs"]
mod workspace;

fn same(left: &str, right: &str, windows: bool) {
    assert!(workspace::equivalent(left, right, windows));
    assert!(workspace::equivalent(right, left, windows));
}

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    assert!(
        !workspace::equivalent("C:", "C:/", true),
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
            workspace::equivalent(left, right, windows),
            workspace::equivalent(right, left, windows)
        );
        if workspace::equivalent(left, right, windows)
            && workspace::equivalent(right, third, windows)
        {
            same(left, third, windows);
        }
        assert!(!workspace::equivalent("", ".", windows));
    }
    assert!(workspace::matches(
        left,
        std::path::Path::new(left),
        std::path::Path::new(right)
    ));
    assert!(workspace::matches(
        right,
        std::path::Path::new(left),
        std::path::Path::new(right)
    ));
    assert!(!workspace::equivalent("C:/x", "D:/x", true));
    assert!(!workspace::equivalent("C:/ß", "C:/SS", true));
    same("C:/Σ", "C:/ς", true);
    assert!(!workspace::equivalent(
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
                assert!(!workspace::equivalent(
                    &canonical,
                    &format!("{alias}{separator}different"),
                    true
                ));
                assert!(!workspace::equivalent(
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
                assert!(!workspace::equivalent(
                    &unc,
                    &format!("/{server}/{share}"),
                    true
                ));
                assert!(!workspace::equivalent(
                    &unc,
                    &format!("{alias}{separator}different"),
                    true
                ));
            }
        }
    }
    let posix = format!("/root/a{}", parts.join("/"));
    assert!(!workspace::equivalent(&posix, &posix.to_uppercase(), false));
    assert!(!workspace::equivalent(
        &posix,
        &posix.replace('/', "\\"),
        false
    ));
});
