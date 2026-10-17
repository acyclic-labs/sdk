use super::*;
use proptest::prelude::*;

fn helper_fixture(root: &Path) -> Vec<String> {
    let helper = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../compatibility");
    let mut files = vec![helper.join("Cargo.toml")];
    let mut directories = vec![helper.join("src")];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory).expect("helper sources should be readable") {
            let path = entry
                .expect("helper source entry should be readable")
                .path();
            if path.is_dir() {
                directories.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
        .into_iter()
        .map(|path| {
            let relative = format!(
                "compatibility/{}",
                path_string(path.strip_prefix(&helper).expect("helper input is local"))
            );
            write(
                root,
                &relative,
                &fs::read(path).expect("helper input should be readable"),
            );
            relative
        })
        .collect()
}

#[test]
fn every_build_helper_input_changes_the_actual_attested_identity() {
    let root = test_root("build-helper-inputs");
    let output = root.join("output");
    let inputs = helper_fixture(&root);
    let baseline = source_file_hashes(&root, &output).expect("source inventory should build");
    assert_eq!(baseline.len(), inputs.len());
    let identity = digest_map(&baseline);
    for relative in inputs {
        assert!(baseline.contains_key(&relative), "missing {relative}");
        let original = fs::read(root.join(&relative)).expect("helper input should exist");
        let mut edited = original.clone();
        edited.extend_from_slice(b"\n// attestation mutation\n");
        write(&root, &relative, &edited);
        let actual = source_file_hashes(&root, &output).expect("edited inventory should build");
        assert_ne!(identity, digest_map(&actual), "edit undetected: {relative}");
        // Negative control: the old collector omits the helper inputs entirely.
        let old = |inventory: &BTreeMap<String, String>| {
            inventory
                .iter()
                .filter(|(path, _)| !inputs_path(path))
                .map(|(path, digest)| (path.clone(), digest.clone()))
                .collect()
        };
        assert_eq!(digest_map(&old(&baseline)), digest_map(&old(&actual)));
        write(&root, &relative, &original);
    }
    fs::remove_dir_all(root).expect("source fixture should be removable");
}

fn inputs_path(path: &str) -> bool {
    path == "compatibility/Cargo.toml" || path.starts_with("compatibility/src/")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // Every nested source, including non-Rust include_bytes! inputs, contributes
    // both its path and contents to the production source identity.
    #[test]
    fn nested_helper_inputs_are_attested(
        directory in "[a-z][a-z0-9]{0,15}",
        file in "[a-z][a-z0-9]{0,15}\\.(rs|bin|toml)",
        bytes in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        let root = test_root("nested-helper-property");
        let output = root.join("output");
        let relative = format!("compatibility/src/{directory}/{file}");
        write(&root, &relative, &bytes);
        let before = source_file_hashes(&root, &output).expect("source inventory should build");
        prop_assert_eq!(before.get(&relative), Some(&sha256_bytes(&bytes)));
        let mut changed = bytes;
        changed.push(0);
        write(&root, &relative, &changed);
        let after = source_file_hashes(&root, &output).expect("edited inventory should build");
        prop_assert_ne!(digest_map(&before), digest_map(&after));
        fs::remove_dir_all(root).expect("source fixture should be removable");
    }
}
