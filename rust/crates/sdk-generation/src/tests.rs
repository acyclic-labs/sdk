#[cfg(windows)]
use std::process::Command;

#[test]
fn registry_frontier_cli_requires_paired_authority_inputs() {
    let base: Vec<String> = [
        "--root",
        "root",
        "--output",
        "output",
        "--version",
        "0.1.1",
        "--channel",
        "release",
        "--execute-profiles",
        "--historical-archives",
        "archives",
        "--historical-source",
        "registry-archives",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    // Older manifests can still be decoded; new generation must capture closure.
    assert!(
        parse_generate(base.clone())
            .unwrap_err()
            .to_string()
            .contains("requires a complete registry corpus")
    );
    let mut missing = base.clone();
    missing.extend(["--registry-frontier-repository".into(), "repository".into()]);
    assert!(
        parse_generate(missing)
            .unwrap_err()
            .to_string()
            .contains("requires both")
    );
    let mut complete = base;
    complete.extend([
        "--registry-frontier-repository".into(),
        "repository".into(),
        "--registry-frontier-revisions".into(),
        "a".repeat(40),
    ]);
    let args = parse_generate(complete).unwrap();
    assert_eq!(
        args.registry_frontier_revisions,
        BTreeSet::from(["a".repeat(40)])
    );
}
use super::*;
use sha2::{Digest, Sha256};

#[path = "source_attestation_tests.rs"]
mod source_attestation;

#[test]
fn release_version_must_match_published_sdk_version() {
    let versions = BTreeSet::from(["0.2.0".to_owned()]);
    let error = validate_release_package_versions("0.1.0", &versions)
        .expect_err("a stale docs version must not be released");
    assert!(
        error
            .to_string()
            .contains("does not match SDK package version")
    );
    validate_release_package_versions("0.2.0", &versions)
        .expect("matching SDK and docs versions should be accepted");
}

fn test_root(label: &str) -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::Builder::new()
        .prefix(&format!("sdk-generation-{label}-"))
        .tempdir()
        .expect("test root should be creatable");
    let path = directory.path().to_owned();
    (directory, path)
}

fn write(root: &Path, relative: &str, bytes: &[u8]) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().expect("fixture path has a parent"))
        .expect("fixture parent should be creatable");
    fs::write(path, bytes).expect("fixture file should be writable");
}

#[test]
fn internal_profile_build_cache_is_excluded_but_public_outputs_are_hashed() {
    let (_root_directory, root) = test_root("artifact-cache");
    let output = root.join("output");
    write(&output, "sdk-docs-data.v1.json", b"docs");
    write(&output, ".profile-build/debug/cache.bin", b"cache-v1");
    write(&output, ".sdk-docs-versions.v1.lock", b"coordination");
    write(&output, "assets/.sdk-docs-versions.v1.lock", b"published");
    let lock = fs::File::options()
        .read(true)
        .write(true)
        .open(output.join(".sdk-docs-versions.v1.lock"))
        .unwrap();
    lock.lock().unwrap();
    write(
        &output,
        "assets/.profile-build/qualified-example.txt",
        b"published",
    );
    let before = artifact_hashes(&output).expect("artifact inventory should build");
    assert!(before.contains_key("sdk-docs-data.v1.json"));
    assert!(!before.contains_key(".sdk-docs-versions.v1.lock"));
    assert_eq!(
        before["assets/.sdk-docs-versions.v1.lock"],
        sha256_bytes(b"published")
    );
    assert!(
        !before
            .keys()
            .any(|path| path.starts_with(".profile-build/"))
    );
    assert_eq!(
        before.get("assets/.profile-build/qualified-example.txt"),
        Some(&sha256_bytes(b"published")),
        "only the output-root profile scratch directory is private"
    );

    write(&output, ".profile-build/debug/cache.bin", b"cache-v2");
    let after = artifact_hashes(&output).expect("artifact inventory should rebuild");
    assert_eq!(
        before, after,
        "internal cache mutation must not alter artifacts"
    );
    drop(lock);
}

proptest::proptest! {
    #[test]
    fn mutable_catalog_files_are_excluded_only_at_the_output_root(
        parents in proptest::collection::vec("[a-z]{1,8}", 0..6),
    ) {
        let output = Path::new("output");
        let mut directory = output.to_owned();
        for parent in &parents { directory.push(parent); }
        for name in [".sdk-docs-versions.v1.lock", "sdk-docs-versions.v1.json"] {
            proptest::prop_assert_eq!(is_immutable_artifact(output, &directory.join(name)), !parents.is_empty());
        }
        proptest::prop_assert!(is_immutable_artifact(output, &directory.join("published.json")));
    }
}

#[test]
fn contract_inputs_are_source_attested_with_precise_non_rust_rules() {
    let (_root_directory, root) = test_root("source-inputs");
    let output = root.join("output");
    for (relative, bytes) in [
        ("proto/demo.proto", b"syntax = \"proto3\";".as_slice()),
        (
            "rust/crates/stream/proto/stream/v2/stream.proto",
            b"message Stream {}".as_slice(),
        ),
        (
            "rust/crates/stream/conformance/stream.json",
            b"{\"suite\":true}".as_slice(),
        ),
        (
            "compatibility/descriptors/stream-v1.bin",
            b"descriptor-v1".as_slice(),
        ),
        (
            "plugin/bin/install.js",
            b"console.log('install');".as_slice(),
        ),
        (
            "plugin/scripts/package.mjs",
            b"export default {};".as_slice(),
        ),
        ("plugin/package.json", b"{\"name\":\"plugin\"}".as_slice()),
        ("rust/crates/demo/src/ignored.json", b"{}".as_slice()),
    ] {
        write(&root, relative, bytes);
    }

    for (relative, bytes) in [
        ("docs/research/ignored.md", b"research note".as_slice()),
        (
            "rust/crates/demo/research/ignored.md",
            b"crate research note".as_slice(),
        ),
        (".cache/ignored.md", b"build cache".as_slice()),
    ] {
        write(&root, relative, bytes);
    }

    let before = source_file_hashes(&root, &output).expect("source inventory should build");
    for required in [
        "proto/demo.proto",
        "rust/crates/stream/proto/stream/v2/stream.proto",
        "rust/crates/stream/conformance/stream.json",
        "compatibility/descriptors/stream-v1.bin",
        "plugin/bin/install.js",
        "plugin/scripts/package.mjs",
        "plugin/package.json",
    ] {
        assert!(
            before.contains_key(required),
            "missing source input {required}"
        );
    }
    for excluded in [
        "rust/crates/demo/src/ignored.json",
        "docs/research/ignored.md",
        "rust/crates/demo/research/ignored.md",
        ".cache/ignored.md",
    ] {
        assert!(
            !before.contains_key(excluded),
            "unexpected source input {excluded}"
        );
    }

    for (relative, changed) in [
        (
            "proto/demo.proto",
            b"syntax = \"proto3\"; message Changed {}".as_slice(),
        ),
        (
            "compatibility/descriptors/stream-v1.bin",
            b"descriptor-v2".as_slice(),
        ),
        (
            "rust/crates/stream/conformance/stream.json",
            b"{\"suite\":false}".as_slice(),
        ),
        (
            "plugin/bin/install.js",
            b"console.log('changed install');".as_slice(),
        ),
    ] {
        let original = fs::read(root.join(relative)).expect("source fixture should exist");
        write(&root, relative, changed);
        let after = source_file_hashes(&root, &output)
            .expect("source inventory should rebuild after an input edit");
        assert_ne!(
            digest_map(&before),
            digest_map(&after),
            "editing {relative} must change the source attestation"
        );
        fs::write(root.join(relative), original).expect("source fixture should restore");
    }
}

#[test]
fn release_generation_requires_executed_profiles() {
    let error = parse_generate(vec![
        "--root".into(),
        "root".into(),
        "--output".into(),
        "output".into(),
        "--rustdoc-dir".into(),
        "rustdoc".into(),
        "--version".into(),
        "1.0.0".into(),
        "--channel".into(),
        "release".into(),
    ])
    .expect_err("release hand-supplied receipts must be rejected");
    assert!(error.to_string().contains("requires --execute-profiles"));
}

#[test]
fn missing_external_rustdoc_span_is_restored_at_rustdoc_path() {
    let (_root_directory, root) = test_root("generated-span-staging");
    let root = root.canonicalize().expect("test root should canonicalize");
    let candidate = root.join("target/candidate/wire.rs");
    write(&root, "target/candidate/wire.rs", b"pub struct Wire;\n");
    let foreign = root.join("target/producer/wire.rs");
    let receipt = root.join("receipt.json");
    fs::write(
        &receipt,
        serde_json::to_vec(&serde_json::json!({
            "span": { "filename": foreign.to_string_lossy() }
        }))
        .expect("span fixture should serialize"),
    )
    .expect("receipt should be writable");
    let sources = materialize_generated_sources(&root, std::slice::from_ref(&receipt))
        .expect("matching generated source should be staged");
    assert_eq!(sources.len(), 1);
    assert_eq!(
        sources[0].physical_path,
        foreign
            .canonicalize()
            .expect("restored span should canonicalize")
    );
    assert_eq!(fs::read(&foreign).unwrap(), fs::read(&candidate).unwrap());
    assert_eq!(sources[0].sha256, sha256_file(&candidate).unwrap());
    assert!(candidate.exists());
}

#[test]
fn equivalent_rustdoc_source_paths_are_deduplicated() {
    let (_root_directory, root) = test_root("deduplicate-generated-span");
    write(&root, "target/candidate/wire.rs", b"pub struct Wire;\n");
    let canonical = root.join("target/candidate/wire.rs");
    let equivalent = root.join("target/candidate/./wire.rs");
    let receipt = root.join("receipt.json");
    fs::write(
        &receipt,
        serde_json::to_vec(&serde_json::json!({
            "spans": [
                { "filename": canonical.to_string_lossy() },
                { "filename": equivalent.to_string_lossy() }
            ]
        }))
        .expect("span fixture should serialize"),
    )
    .expect("receipt should be writable");

    let sources = materialize_generated_sources(&root, std::slice::from_ref(&receipt))
        .expect("equivalent source paths should be accepted");
    assert_eq!(sources.len(), 1);
    assert_eq!(
        sources[0].sha256,
        sha256_file(&canonical).expect("source should hash")
    );
}

#[test]
fn existing_external_rustdoc_span_keeps_canonical_input_path() {
    let (_root_directory, root) = test_root("existing-external-generated-span");
    let directory = tempfile::tempdir().expect("external fixture directory should be writable");
    let external = directory.path().join("wire.rs");
    fs::write(&external, b"pub struct Wire;\n").expect("external source should be writable");
    let receipt = root.join("receipt.json");
    fs::write(
        &receipt,
        serde_json::to_vec(&serde_json::json!({
            "span": { "filename": external.to_string_lossy() }
        }))
        .expect("span fixture should serialize"),
    )
    .expect("receipt should be writable");

    let sources = materialize_generated_sources(&root, std::slice::from_ref(&receipt))
        .expect("existing external source should be attested");
    assert_eq!(sources.len(), 1);
    assert_eq!(
        sources[0].physical_path,
        external
            .canonicalize()
            .expect("external source should canonicalize")
    );
}

#[test]
fn ambiguous_external_rustdoc_span_is_rejected() {
    let (_root_directory, root) = test_root("ambiguous-generated-span");
    write(&root, "target/one/wire.rs", b"pub struct One;\n");
    write(&root, "rust/two/wire.rs", b"pub struct Two;\n");
    let foreign = root.join("target/producer/wire.rs");
    let receipt = root.join("receipt.json");
    fs::write(
        &receipt,
        serde_json::to_vec(&serde_json::json!({
            "span": { "filename": foreign.to_string_lossy() }
        }))
        .expect("span fixture should serialize"),
    )
    .expect("receipt should be writable");
    let error = materialize_generated_sources(&root, std::slice::from_ref(&receipt))
        .expect_err("ambiguous generated source must fail closed");
    assert!(error.to_string().contains("basename is ambiguous"));
    assert!(!foreign.exists());
}

#[test]
fn missing_owned_generated_source_is_restored_across_root_spellings() {
    let (root_directory, root) = test_root("restore-generated-span");
    let bytes = b"pub struct Restored;\n";
    write(&root, "target/one/wire.rs", bytes);
    // Keep this ordinary spelling. materialize_generated_sources canonicalizes
    // its root to a verbatim path on Windows before checking ownership.
    let missing = root.join("target/producer/wire.rs");
    let receipt = root.join("receipt.json");
    fs::write(
        &receipt,
        serde_json::to_vec(&serde_json::json!({
            "span": { "filename": missing.to_string_lossy() }
        }))
        .unwrap(),
    )
    .unwrap();
    let sources = materialize_generated_sources(&root, &[receipt]).unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(fs::read(&missing).unwrap(), bytes);
    assert_eq!(sources[0].physical_path, missing.canonicalize().unwrap());
    assert_eq!(sources[0].sha256, sha256_file(&missing).unwrap());
    root_directory.close().unwrap();
}

#[test]
fn restoration_rejects_arbitrary_paths_and_parent_traversal() {
    let (_root_directory, root) = test_root("restoration-boundary");
    let root = root.canonicalize().unwrap();
    for path in [
        std::env::temp_dir().join(".cargo/sdk-docs-unowned/wire.rs"),
        root.join("rust/wire.rs"),
        root.join("target/../unowned/wire.rs"),
    ] {
        let error = validate_restore_path(&root, &[], &path).unwrap_err();
        assert!(
            error.to_string().contains("outside owned")
                || error.to_string().contains("parent traversal")
        );
    }
    let missing = root.join("target/missing/wire.rs");
    validate_restore_path(&root, &[], &missing).unwrap();
    assert!(!missing.exists());

    #[cfg(unix)]
    {
        let (_outside_directory, outside) = test_root("restoration-outside");
        fs::create_dir_all(root.join("target")).unwrap();
        let linked = root.join("target/linked");
        for destination in [outside, root.join("absent")] {
            std::os::unix::fs::symlink(&destination, &linked).unwrap();
            let error = validate_restore_path(&root, &[], &linked.join("wire.rs")).unwrap_err();
            assert!(error.to_string().contains("reparse point or symlink"));
            assert!(!destination.join("wire.rs").exists());
            fs::remove_file(&linked).unwrap();
        }
        let file = root.join("target/file");
        fs::write(&file, b"retained").unwrap();
        let error = validate_restore_path(&root, &[], &file.join("wire.rs")).unwrap_err();
        assert_eq!(
            error.to_string(),
            io_error(fs::symlink_metadata(file.join("wire.rs")).unwrap_err()).to_string()
        );
        assert_eq!(fs::read(file).unwrap(), b"retained");
    }
}

#[test]
fn captured_external_restoration_is_owned_and_digest_bound() {
    let (_root_directory, root) = test_root("captured-restoration");
    let root = root.canonicalize().unwrap();
    let (_output_directory, output) = test_root("captured-restoration-output");
    let output = output.canonicalize().unwrap();
    let foreign = output.join(".profile-build/build/demo/out/wire.rs");
    let receipt = output.join(".rustdoc/demo.json");
    let bytes = b"pub struct Captured;\n";
    let digest = sha256_bytes(bytes);
    let relative = format!(
        "target/sdk-generation-generated-sources/retained/{}-wire.rs",
        digest.trim_start_matches("sha256:")
    );
    write(&root, &relative, bytes);
    write(
        &output,
        ".rustdoc/demo.json",
        &serde_json::to_vec(&serde_json::json!({
            "span": { "filename": foreign.to_string_lossy() }
        }))
        .unwrap(),
    );
    let aliases = serde_json::to_vec(&BTreeMap::from([(
        foreign.to_string_lossy().to_string(),
        relative.clone(),
    )]))
    .unwrap();
    write(
        &root,
        "target/sdk-generation-generated-sources/retained/aliases.json",
        &aliases,
    );
    let sources = materialize_generated_sources(&root, std::slice::from_ref(&receipt)).unwrap();
    assert_eq!(sources[0].sha256, digest);
    assert_eq!(fs::read(&foreign).unwrap(), bytes);
    fs::remove_file(&foreign).unwrap();
    fs::write(root.join(relative), b"changed capture").unwrap();
    let error = materialize_generated_sources(&root, std::slice::from_ref(&receipt)).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("captured Rustdoc source digest changed")
    );
    assert!(!foreign.exists());
}

#[test]
fn identical_profile_outputs_keep_each_physical_span_mapping() {
    let (_root_directory, root) = test_root("identical-profile-spans");
    let root = root.canonicalize().unwrap();
    let (_output_directory, output) = test_root("identical-profile-spans-output");
    let output = output.canonicalize().unwrap();
    let first = output.join(".profile-build/build/one/out/wire.rs");
    let second = output.join(".profile-build/build/two/out/wire.rs");
    write(
        &output,
        ".profile-build/build/one/out/wire.rs",
        b"pub struct Wire;\n",
    );
    write(
        &output,
        ".profile-build/build/two/out/wire.rs",
        b"pub struct Wire;\n",
    );
    let receipt = output.join(".rustdoc/demo.json");
    write(&output, ".rustdoc/demo.json", &serde_json::to_vec(&serde_json::json!({
        "spans": [{ "filename": first.to_string_lossy() }, { "filename": second.to_string_lossy() }]
    })).unwrap());
    let sources = materialize_generated_sources(&root, &[receipt]).unwrap();
    assert_eq!(sources.len(), 2);
    assert_eq!(sources[0].sha256, sources[1].sha256);
    assert_eq!(sources[0].logical_path, sources[1].logical_path);
    assert_ne!(sources[0].physical_path, sources[1].physical_path);
}

mod rustdoc_corpus {
    use super::*;

    #[test]
    fn real_rustdoc_corpus_preserves_its_producer_artifacts() {
        let root = PathBuf::from(env::var("SDK_GENERATION_CORPUS_ROOT").unwrap())
            .canonicalize()
            .unwrap();
        let rustdoc = PathBuf::from(env::var("SDK_GENERATION_CORPUS_RUSTDOC").unwrap())
            .canonicalize()
            .unwrap();
        // The real producer already projects every receipt using its admitted
        // Cargo target, binary guides and source-span attestations. Verify that
        // exact output instead of rebuilding a library-only view in the test.
        let output = rustdoc.parent().unwrap();
        let args = [
            "drift".into(),
            "--root".into(),
            root.to_str().unwrap().into(),
            "--output".into(),
            output.to_str().unwrap().into(),
            "--rustdoc-dir".into(),
            rustdoc.to_str().unwrap().into(),
        ];
        run(args.clone(), &mut Vec::new()).unwrap();

        let seal = output.join("generation-manifest.v1.sha256");
        let original = fs::read(&seal).unwrap();
        let mut changed = original.clone();
        changed[0] ^= 1;
        fs::write(&seal, changed).unwrap();
        let rejected = run(args, &mut Vec::new());
        fs::write(&seal, original).unwrap();
        assert!(
            rejected
                .unwrap_err()
                .to_string()
                .contains("generation manifest integrity check failed")
        );
    }
}

#[cfg(windows)]
#[test]
fn restoration_rejects_junction_ancestors_before_writing() {
    let (_root_directory, root) = test_root("restoration-junction");
    let (_external_directory, external) = test_root("restoration-junction-external");
    fs::create_dir_all(root.join("target")).unwrap();
    let junction = root.join("target/linked");
    assert!(Command::new("powershell.exe")
        .env("SDK_DOCS_RESTORE_JUNCTION", &junction)
        .env("SDK_DOCS_RESTORE_TARGET", &external)
        .args(["-NoProfile", "-NonInteractive", "-Command",
            "New-Item -ItemType Junction -Path $env:SDK_DOCS_RESTORE_JUNCTION -Target $env:SDK_DOCS_RESTORE_TARGET | Out-Null"])
        .status().unwrap().success());
    let error = validate_restore_path(&root, &[], &junction.join("wire.rs")).unwrap_err();
    assert!(error.to_string().contains("reparse point or symlink"));
    assert!(!external.join("wire.rs").exists());
    fs::remove_dir(junction).unwrap();
}

#[test]
fn file_hashes_keep_existing_artifact_and_tool_identities() {
    use proptest::prelude::*;
    struct ReadFailureOnce {
        file: fs::File,
        failure: Option<std::io::ErrorKind>,
    }
    impl std::io::Read for ReadFailureOnce {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            match self.failure.take() {
                Some(kind) => Err(kind.into()),
                None => std::io::Read::read(&mut self.file, buffer),
            }
        }
    }
    let (root_directory, root) = test_root("sha256-equivalence");
    let path = root.join("input.bin");
    // The full branch retains every original byte vector with fewer shrink trees.
    let full = (
        proptest::collection::vec(any::<u128>(), 0..4096),
        proptest::collection::vec(any::<u8>(), 0..16),
    )
        .prop_map(|(words, tail)| {
            words
                .into_iter()
                .flat_map(u128::to_le_bytes)
                .chain(tail)
                .collect::<Vec<_>>()
        });
    let bytes = prop_oneof![
        15 => proptest::collection::vec(any::<u8>(), 0..512),
        1 => full,
    ];
    let check = |bytes: &[u8]| {
        fs::write(&path, bytes).unwrap();
        prop_assert_eq!(sha256_file(&path).unwrap(), sha256_bytes(bytes));
        prop_assert_eq!(
            sha256_bytes(bytes),
            format!("sha256:{:x}", Sha256::digest(bytes))
        );
        Ok(())
    };
    proptest::test_runner::TestRunner::default()
        .run(&bytes, |bytes| check(&bytes))
        .unwrap();
    for length in [
        0, 1, 55, 56, 63, 64, 65, 127, 128, 129, 511, 512, 513, 16383, 16384, 16385, 65535, 65536,
        65537,
    ] {
        let bytes = (0..length)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        check(&bytes).unwrap();
    }
    let copied = root.join("copied.bin");
    let mut input = ReadFailureOnce {
        file: fs::File::open(&path).unwrap(),
        failure: Some(std::io::ErrorKind::Interrupted),
    };
    let mut output = fs::File::create(&copied).unwrap();
    let (digest, length) = copy_and_hash(&mut input, &mut output, 65537, "member-byte").unwrap();
    assert_eq!(length, 65537);
    assert_eq!(digest, sha256_file(&path).unwrap());
    assert_eq!(fs::read(&copied).unwrap(), fs::read(&path).unwrap());
    drop((input, output));
    let mut input = ReadFailureOnce {
        file: fs::File::open(&path).unwrap(),
        failure: Some(std::io::ErrorKind::PermissionDenied),
    };
    let error = copy_and_hash(&mut input, &mut std::io::sink(), 65537, "member-byte").unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    drop(input);
    let mut input = fs::File::open(&path).unwrap();
    let mut output = fs::File::create(&copied).unwrap();
    let error = copy_and_hash(&mut input, &mut output, 16384, "member-byte").unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "registry archive member-byte limit exceeded"
    );
    assert_eq!(output.metadata().unwrap().len(), 16384);
    drop((input, output));
    root_directory.close().unwrap();
}

#[test]
fn regular_sources_ignore_root_alias_spelling() {
    use proptest::prelude::*;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::create_dir_all(root.join("src")).unwrap();
    let source = root.join("src/lib.rs");
    fs::write(&source, b"pub struct Regular;\n").unwrap();
    let physical = source.canonicalize().unwrap();
    let receipt = root.join("receipt.json");
    fs::write(
        &receipt,
        serde_json::to_vec(&serde_json::json!({"span":{"filename":physical}})).unwrap(),
    )
    .unwrap();
    sdk_docs::with_rustdoc_cache(|| {
        proptest::test_runner::TestRunner::default()
            .run(
                &(any::<bool>(), prop::collection::vec(any::<bool>(), 0..8)),
                |(canonical, aliases)| {
                    let mut alias = if canonical {
                        root.canonicalize().unwrap()
                    } else {
                        root.to_owned()
                    };
                    for dot in aliases {
                        alias.push(if dot { "." } else { "src/.." });
                    }
                    prop_assert!(
                        materialize_generated_sources(&alias, std::slice::from_ref(&receipt))
                            .unwrap()
                            .is_empty()
                    );
                    Ok(())
                },
            )
            .unwrap();
    });
    // The old raw-prefix comparison misclassifies this in-root source on every host.
    assert!(!physical.starts_with(root.join("src/..")));
}
