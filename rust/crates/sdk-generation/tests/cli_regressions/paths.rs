#[path = "../../../sdk-docs/src/path_identity.rs"]
mod identity;
#[cfg(test)]
use identity::components;
pub(super) use identity::rebase;
pub(super) use identity::stable_filename;

pub(super) fn fixed_revision<'a>(head: &[u8], revision: &'a [u8]) -> Option<&'a str> {
    if head != b"ref: refs/heads/sdk-fixture\n" {
        return None;
    }
    let revision = std::str::from_utf8(revision).ok()?.trim();
    (revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then_some(revision)
}

#[cfg(test)]
proptest::proptest! {
    #[test]
    fn receipt_paths_rebase_components_across_windows_spellings(
        directory in "[a-z]{1,12}", leaf in "[a-z]{1,12}", drive in b'A'..=b'Z',
        revision in "[a-f0-9]{40}", other_head in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
    ) {
        crate::runtime::time_control();
        let head = b"ref: refs/heads/sdk-fixture\n";
        let encoded_revision = format!("{revision}\n");
        proptest::prop_assert_eq!(fixed_revision(head, encoded_revision.as_bytes()), Some(revision.as_str()));
        if other_head != head { proptest::prop_assert!(fixed_revision(&other_head, revision.as_bytes()).is_none()); }
        let malformed_revision = format!("{revision}!");
        proptest::prop_assert!(fixed_revision(head, malformed_revision.as_bytes()).is_none());
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
                proptest::prop_assert_eq!(stable_filename(&path, &old), stable_filename(&result, &new));
                proptest::prop_assert_eq!(rebase(&result, &new, &old, true).and_then(|p| components(&p, true)), components(&format!("{old}/{leaf}"), true));
            }
            let sibling = format!("{old}-sibling/{leaf}");
            proptest::prop_assert!(rebase(&sibling, &old, &new, true).is_none());
        }
        proptest::prop_assert!(rebase("/OLD/item", "/old", "/new", false).is_none());
        proptest::prop_assert!(rebase("/old/../other", "/old", "/new", false).is_none());
    }
}
