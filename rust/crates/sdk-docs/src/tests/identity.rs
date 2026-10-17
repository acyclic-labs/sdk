use super::*;
use proptest::prelude::*;

proptest! {
    // Distinct POSIX component sequences remain distinct, including literal
    // backslashes, uppercase, drive-like names and the normalization marker.
    #[test]
    fn owned_posix_filename_identity_is_injective(
        a in prop::collection::vec("[a-zA-Z0-9_\\\\:<>é]{1,8}", 1..5),
        b in prop::collection::vec("[a-zA-Z0-9_\\\\:<>é]{1,8}", 1..5),
    ) {
        prop_assume!(a != b);
        let normalize = |parts: &[String]| crate::path_identity::stable_filename(
            &format!("/checkout/{}", parts.join("/")), "/checkout");
        prop_assert_ne!(normalize(&a), normalize(&b));
        let original = format!("/checkout/{}", a.join("/"));
        let encoded = crate::path_identity::rebase(&original, "/checkout", "/relocated", false).unwrap();
        prop_assert_eq!(crate::path_identity::stable_filename(&encoded, "/relocated"), normalize(&a));
    }
    // A physical rebase preserves the real normalized digest for every Windows
    // spelling, even when the test host cannot open any of these source paths.
    #[test]
    fn windows_receipts_preserve_digest_identity_on_every_host(
        directory in "[a-z]{1,10}", leaf in "[a-z]{1,10}", drive in b'A'..=b'Z',
    ) {
        let drive = char::from(drive);
        for (old, path, new) in [
            (format!("{drive}:\\old\\{directory}"), format!(r"\\?\{drive}:\OLD\{directory}\src\{leaf}.rs"), "D:\\new\\checkout".to_owned()),
            (format!(r"\\server\share\old\{directory}"), format!(r"\\?\UNC\SERVER\SHARE\old\{directory}\src\{leaf}.rs"), r"\\server\share\new\checkout".to_owned()),
        ] {
            let rebase = crate::path_identity::rebase(&path, &old, &new, true).unwrap();
            let normalize = |path: &str, root: &str| normalized_rustdoc_digest_bytes(
                &serde_json::to_vec(&serde_json::json!({"index": {"0": {"span": {"filename": path}}}})).unwrap().into(), Path::new(root)).unwrap();
            prop_assert_eq!(normalize(&path, &old), normalize(&rebase, &new));
            prop_assert_ne!(normalize(&path, &old), normalize(&format!("{rebase}.changed"), &new));
        }
    }
}

#[test]
fn treating_posix_backslashes_as_separators_is_detected() {
    let paths = [r"/checkout/src/a\a.rs", "/checkout/src/a/a.rs"];
    // Negative control: the former separator conversion aliases these names.
    assert_eq!(paths[0].replace('\\', "/"), paths[1]);
    assert_ne!(
        crate::path_identity::stable_filename(paths[0], "/checkout"),
        crate::path_identity::stable_filename(paths[1], "/checkout")
    );
}

proptest! {
    #[test]
    fn root_marker_components_are_never_stripped_from_source_identity(depth in 1usize..5, leaf in "[a-z]{1,10}") {
        let root = Path::new("/checkout");
        let digest = |path: &str| normalized_rustdoc_digest_bytes(
            &serde_json::to_vec(&serde_json::json!({"filename": path})).unwrap().into(), root).unwrap();
        let marked = format!("/checkout/{}{leaf}", "<root>/".repeat(depth));
        prop_assert_ne!(digest(&marked), digest(&format!("/checkout/{leaf}")));
        let nested = format!("/checkout/<root>/{}", "<root>/".repeat(depth));
        prop_assert_ne!(digest(&format!("{nested}{leaf}")), digest(&marked));
    }
}
