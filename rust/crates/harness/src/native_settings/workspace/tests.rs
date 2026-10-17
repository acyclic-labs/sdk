use super::*;
use proptest::prelude::*;

proptest! {
    // Round-trip property: ordinary absolute drive and UNC keys survive
    // verbatim-prefix, case and separator changes without changing identity.
    #[test]
    fn drive_keys_round_trip_alias_spellings(
        drive in b'A'..=b'Z',
        parts in prop::collection::vec("[a-zA-ZéÉ猫Σσς]{1,12}", 1..8),
        forward in any::<bool>(),
        verbatim in any::<bool>(),
        upper in any::<bool>(),
    ) {
        let canonical = format!("{}:\\{}", char::from(drive), parts.join("\\"));
        let separator = if forward { "/" } else { "\\" };
        let ordinary = format!("{}:{separator}{}", char::from(drive.to_ascii_lowercase()), parts.join(separator));
        let ordinary = if upper { ordinary.to_uppercase() } else { ordinary.to_lowercase() };
        let alias = format!("{}{ordinary}{separator}.", if verbatim { "\\\\?\\" } else { "" });
        let different = format!("{alias}{separator}different");
        prop_assert!(equivalent(&canonical, &alias, true));
        prop_assert!(equivalent(&alias, &canonical, true));
        prop_assert!(!equivalent(&canonical, &different, true));
        prop_assert!(!equivalent(&canonical, &canonical.replacen(":\\", ":", 1), true));
    }

    #[test]
    fn unc_keys_round_trip_alias_spellings(
        server in "[a-zA-Z]{1,16}",
        share in "[a-zA-Z]{1,16}",
        parts in prop::collection::vec("[a-zA-ZéÉ猫Σσς]{1,12}", 0..8),
        forward in any::<bool>(),
        verbatim in any::<bool>(),
    ) {
        let canonical = format!("\\\\{server}\\{share}\\{}", parts.join("\\"));
        let separator = if forward { "/" } else { "\\" };
        let alias = format!("{}{}{separator}{}{separator}{}",
            if verbatim { "\\\\?\\UNC\\" } else { "//" },
            server.to_uppercase(), share.to_uppercase(), parts.join(separator).to_lowercase(),
        );
        let rooted = format!("{separator}{server}{separator}{share}");
        let different = format!("{alias}{separator}different");
        prop_assert!(equivalent(&canonical, &alias, true));
        prop_assert!(equivalent(&alias, &canonical, true));
        prop_assert!(!equivalent(&canonical, &rooted, true));
        prop_assert!(!equivalent(&canonical, &different, true));
    }

    // Property: spelling equivalence is reflexive, symmetric and transitive;
    // POSIX paths retain case sensitivity and literal backslashes.
    #[test]
    fn equivalence_preserves_platform_roots_and_case(
        left in ".{0,128}", right in ".{0,128}", third in ".{0,128}", windows in any::<bool>(),
        component in "[a-z]{1,24}",
    ) {
        prop_assert!(equivalent(&left, &left, windows));
        prop_assert_eq!(equivalent(&left, &right, windows), equivalent(&right, &left, windows));
        if equivalent(&left, &right, windows) && equivalent(&right, &third, windows) {
            prop_assert!(equivalent(&left, &third, windows));
        }
        let path = format!("/root/{component}");
        prop_assert!(!equivalent(&path, &path.to_uppercase(), false));
        prop_assert!(!equivalent(&path, &path.replace('/', "\\"), false));
        prop_assert!(!equivalent("", ".", windows));
        prop_assert!(!equivalent("C:", "C:/", true));
        prop_assert!(!equivalent("C:/x", "D:/x", true));
        prop_assert!(!equivalent("C:/ß", "C:/SS", true));
        prop_assert!(equivalent("C:/Σ", "C:/ς", true));
        prop_assert!(!equivalent(r"\\.\server\share", r"\\server\share", true));
    }
}

#[cfg(windows)]
#[test]
fn relative_workspace_uses_normalized_absolute_drive_and_unc_keys() {
    for (key, canonical) in [
        ("q:/Repo/Project", r"\\?\Q:\repo\project"),
        (r"\\Server\Share\Project", r"\\?\UNC\server\share\project"),
        (r"\\?\UNC\Server\Share\Project", r"\\server\share\project"),
    ] {
        assert!(matches(key, Path::new("."), Path::new(canonical)));
    }
}
