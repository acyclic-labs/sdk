// Compare path components explicitly so Windows Rustdoc paths can be replayed
// on any host. Device namespaces other than verbatim drive/UNC remain distinct.
fn components(path: &str, windows: bool) -> Option<Vec<String>> {
    let mut path = if windows {
        path.replace('\\', "/")
    } else {
        path.to_owned()
    };
    if windows {
        if let Some(rest) = path.strip_prefix("//?/") {
            if rest
                .get(..4)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("UNC/"))
            {
                path = format!("//{}", &rest[4..]);
            } else if rest.as_bytes().get(1) == Some(&b':') {
                path = rest.to_owned();
            }
        }
    }
    let root = if windows && path.starts_with("//") {
        "unc"
    } else if windows && path.as_bytes().get(1) == Some(&b':') {
        if path.as_bytes().get(2) == Some(&b'/') {
            "drive-root"
        } else {
            "drive-relative"
        }
    } else if path.starts_with('/') {
        "root"
    } else {
        "relative"
    };
    let mut parts = vec![root.to_owned()];
    for part in path
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
    {
        if part == ".." {
            return None;
        }
        if windows
            && part.contains(':')
            && !(parts.len() == 1
                && part.len() == 2
                && part.as_bytes()[0].is_ascii_alphabetic()
                && matches!(root, "drive-root" | "drive-relative"))
        {
            return None;
        }
        parts.push(part.to_owned());
    }
    Some(parts)
}

pub(super) fn rebase(path: &str, old: &str, new: &str, windows: bool) -> Option<String> {
    let parts = components(path, windows)?;
    let prefix = components(old, windows)?;
    let destination = components(new, windows)?;
    if [prefix[0].as_str(), destination[0].as_str()]
        .iter()
        .any(|root| matches!(*root, "relative" | "drive-relative"))
    {
        return None;
    }
    if parts.len() < prefix.len()
        || !parts.iter().zip(&prefix).all(|(a, b)| {
            if windows {
                a.eq_ignore_ascii_case(b)
            } else {
                a == b
            }
        })
    {
        return None;
    }
    let mut result = std::path::PathBuf::from(new);
    for component in &parts[prefix.len()..] {
        result.push(component);
    }
    Some(result.to_str()?.to_owned())
}

#[cfg(test)]
proptest::proptest! {
    #[test]
    fn receipt_paths_rebase_components_across_windows_spellings(
        directory in "[a-z]{1,12}", leaf in "[a-z]{1,12}", drive in b'A'..=b'Z',
    ) {
        let drive = char::from(drive);
        for (old, variants, new) in [
            (format!("{drive}:\\old\\{directory}"), vec![
                format!("{drive}:\\old\\{directory}\\{leaf}"),
                format!(r"\\?\{drive}:\OLD\{}\{leaf}", directory.to_ascii_uppercase()),
                format!("{}:/OLD/{directory}/{leaf}", drive.to_ascii_lowercase()),
            ], format!("{drive}:\\new\\é")),
            (format!(r"\\server\share\old\{directory}"), vec![
                format!(r"\\SERVER\SHARE\OLD\{directory}\{leaf}"),
                format!(r"\\?\UNC\server\share\OLD\{directory}\{leaf}"),
                format!("//server/share/old/{directory}/{leaf}"),
            ], r"\\server\share\new\é".to_owned()),
        ] {
            let mut expected = components(&new, true).unwrap();
            expected.push(leaf.clone());
            for path in variants {
                let result = rebase(&path, &old, &new, true).unwrap();
                proptest::prop_assert_eq!(components(&result, true).unwrap(), expected.clone());
                proptest::prop_assert_eq!(rebase(&result, &new, &old, true).and_then(|p| components(&p, true)), components(&format!("{old}/{leaf}"), true));
            }
            proptest::prop_assert!(rebase(&format!("{old}-sibling/{leaf}"), &old, &new, true).is_none());
        }
        proptest::prop_assert!(rebase("/OLD/item", "/old", "/new", false).is_none());
        proptest::prop_assert!(rebase("/old/../other", "/old", "/new", false).is_none());
    }
}
