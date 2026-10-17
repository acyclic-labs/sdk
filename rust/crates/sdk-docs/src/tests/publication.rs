use super::*;

#[test]
fn release_publication_rejects_unbound_source_before_creating_output() {
    let output = std::env::temp_dir().join(format!("sdk-docs-provenance-{}", std::process::id()));
    let _ = fs::remove_dir_all(&output);
    let mut data = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: "1.0.0".into(),
        channel: Channel::Release,
        source: SourceInfo {
            captured_source: None,
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            revision: "a".repeat(40),
            source_state: "working-tree".into(),
            source_sha256: None,
            input_sha256: "b".repeat(64),
            rustdoc_format_versions: vec![FORMAT_VERSION],
            generator: "sdk-docs/test".into(),
        },
        navigation: Navigation {
            entries: Vec::new(),
        },
        packages: PackageCatalog {
            entries: Vec::new(),
        },
        search: SearchIndex {
            entries: Vec::new(),
        },
        families: Vec::new(),
    };
    let error = write_bundle(&data, &output, false)
        .expect_err("a release cannot publish an unbound working tree");
    assert!(error.to_string().contains("requires captured-snapshot"));
    assert!(!output.exists());

    data.source.source_state = "captured-snapshot".into();
    data.source.source_sha256 = Some(format!("sha256:{}", "c".repeat(64)));
    data.source.rustdoc_format_versions.clear();
    let error = write_bundle(&data, &output, false)
        .expect_err("publication must include Rustdoc format metadata");
    assert!(error.to_string().contains("format metadata"));
    assert!(!output.exists());
}

#[test]
fn version_index_must_be_a_regular_non_reparse_file() {
    let output = std::env::temp_dir().join(format!("sdk-docs-index-kind-{}", std::process::id()));
    let _ = fs::remove_dir_all(&output);
    fs::create_dir_all(output.join("sdk-docs-versions.v1.json"))
        .expect("index directory should be creatable");
    let data = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: "1.0.0".into(),
        channel: Channel::Release,
        source: SourceInfo {
            captured_source: None,
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            revision: "a".repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
            input_sha256: "a".repeat(64),
            rustdoc_format_versions: vec![FORMAT_VERSION],
            generator: "sdk-docs/test".into(),
        },
        navigation: Navigation {
            entries: Vec::new(),
        },
        packages: PackageCatalog {
            entries: Vec::new(),
        },
        search: SearchIndex {
            entries: Vec::new(),
        },
        families: Vec::new(),
    };
    let error = write_bundle(&data, &output, false)
        .expect_err("directory at the index path must block publication");
    assert!(error
        .to_string()
        .contains("version index is not a regular file"));
    assert!(output.join("sdk-docs-versions.v1.json").is_dir());
    assert!(!output.join("releases").exists());
    fs::remove_dir_all(output).expect("test output should be removable");
}

#[test]
fn version_index_rejects_a_conflicting_release_rewrite() {
    let output = std::env::temp_dir().join(format!("sdk-docs-index-{}", std::process::id()));
    let _ = fs::remove_dir_all(&output);
    let data = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: "1.0.0".into(),
        channel: Channel::Release,
        source: SourceInfo {
            captured_source: None,
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            revision: "a".repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
            input_sha256: "a".repeat(64),
            rustdoc_format_versions: vec![60],
            generator: "sdk-docs/test".into(),
        },
        navigation: Navigation {
            entries: Vec::new(),
        },
        packages: PackageCatalog {
            entries: Vec::new(),
        },
        search: SearchIndex {
            entries: Vec::new(),
        },
        families: Vec::new(),
    };
    write_bundle(&data, &output, false).expect("initial release should write");
    let mut conflicting = data;
    conflicting.source.revision = "b".repeat(40);
    let error = write_bundle(&conflicting, &output, false).expect_err("release rewrite must fail");
    assert!(error.to_string().contains("refusing to rewrite"));
    fs::remove_dir_all(output).expect("test output should be removable");
}

#[test]
fn malformed_prior_version_index_is_rejected_without_mutation() {
    let output =
        std::env::temp_dir().join(format!("sdk-docs-malformed-index-{}", std::process::id()));
    let _ = fs::remove_dir_all(&output);
    fs::create_dir_all(&output).expect("output directory should be creatable");
    let malformed = VersionIndex {
        release_candidates: Vec::new(),
        historical_prereleases: Vec::new(),
        schema: VERSION_INDEX_SCHEMA_VERSION.into(),
        latest: None,
        releases: vec![VersionEntry {
            captured_source: None,
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            revision: "a".repeat(40),
            data_file: "release-candidates/1.0.0/sdk-docs-data.v1.json".into(),
            data_sha256: "0".repeat(64),
        }],
        preview: None,
    };
    let index_path = output.join("sdk-docs-versions.v1.json");
    fs::write(
        &index_path,
        serde_json::to_vec_pretty(&malformed).expect("malformed index should serialize"),
    )
    .expect("malformed index should write");
    let before = fs::read(&index_path).expect("malformed index should remain readable");
    let data = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: "2.0.0".into(),
        channel: Channel::Release,
        source: SourceInfo {
            captured_source: None,
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            revision: "b".repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", "b".repeat(64))),
            input_sha256: "b".repeat(64),
            rustdoc_format_versions: vec![FORMAT_VERSION],
            generator: "sdk-docs/test".into(),
        },
        navigation: Navigation {
            entries: Vec::new(),
        },
        packages: PackageCatalog {
            entries: Vec::new(),
        },
        search: SearchIndex {
            entries: Vec::new(),
        },
        families: Vec::new(),
    };
    let error = write_bundle(&data, &output, false)
        .expect_err("malformed prior index must block publication");
    assert!(error.to_string().contains("missing data file"));
    assert_eq!(
        before,
        fs::read(&index_path).expect("malformed index should be unchanged")
    );
    assert!(!output.join("releases").exists());

    let data_path = output.join("release-candidates/1.0.0/sdk-docs-data.v1.json");
    fs::create_dir_all(data_path.parent().expect("data file should have a parent"))
        .expect("prior data directory should be creatable");
    fs::write(&data_path, b"prior data").expect("prior data should be writable");
    let prior_data = fs::read(&data_path).expect("prior data should remain readable");
    let error = write_bundle(&data, &output, false)
        .expect_err("wrong prior data hash must block publication");
    assert!(error.to_string().contains("data digest does not match"));
    assert_eq!(
        before,
        fs::read(&index_path).expect("malformed index should remain unchanged")
    );
    assert_eq!(
        prior_data,
        fs::read(&data_path).expect("prior data should remain unchanged")
    );
    fs::remove_dir_all(output).expect("test output should be removable");
}

#[test]
fn version_index_rejects_invalid_docs_json_even_when_hash_matches() {
    let output =
        std::env::temp_dir().join(format!("sdk-docs-invalid-bundle-{}", std::process::id()));
    let _ = fs::remove_dir_all(&output);
    let data = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: "1.0.0".into(),
        channel: Channel::Release,
        source: SourceInfo {
            captured_source: None,
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            revision: "a".repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
            input_sha256: "a".repeat(64),
            rustdoc_format_versions: vec![FORMAT_VERSION],
            generator: "sdk-docs/test".into(),
        },
        navigation: Navigation {
            entries: Vec::new(),
        },
        packages: PackageCatalog {
            entries: Vec::new(),
        },
        search: SearchIndex {
            entries: Vec::new(),
        },
        families: Vec::new(),
    };
    write_bundle(&data, &output, false).expect("initial release should write");
    let data_path = output.join("release-candidates/1.0.0/sdk-docs-data.v1.json");
    let valid = fs::read(&data_path).expect("valid bundle should be readable");
    let schema_path = output.join("release-candidates/1.0.0/sdk-docs-data.v1.schema.json");
    let schema = fs::read(&schema_path).expect("schema sidecar should be readable");
    let index_path = output.join("sdk-docs-versions.v1.json");
    let index_before_sidecar_failure =
        fs::read(&index_path).expect("index should be readable before sidecar checks");
    fs::remove_file(&schema_path).expect("schema sidecar should be removable");
    let error = write_bundle(&data, &output, false)
        .expect_err("missing schema sidecar must block publication");
    assert!(error.to_string().contains("missing schema sidecar"));
    assert_eq!(
        valid,
        fs::read(&data_path).expect("data must remain unchanged")
    );
    assert_eq!(
        index_before_sidecar_failure,
        fs::read(&index_path).expect("index must remain unchanged")
    );
    fs::write(&schema_path, b"{}").expect("corrupt schema sidecar should be writable");
    let error = write_bundle(&data, &output, false)
        .expect_err("corrupt schema sidecar must block publication");
    assert!(error
        .to_string()
        .contains("does not match the pinned schema"));
    assert_eq!(
        valid,
        fs::read(&data_path).expect("data must remain unchanged")
    );
    assert_eq!(
        index_before_sidecar_failure,
        fs::read(&index_path).expect("index must remain unchanged")
    );
    fs::write(&schema_path, schema).expect("valid schema sidecar should be restored");
    let mut missing_packages: serde_json::Value =
        serde_json::from_slice(&valid).expect("valid bundle should be JSON");
    missing_packages
        .as_object_mut()
        .expect("bundle should be an object")
        .remove("packages");
    let missing_packages_bytes = serde_json::to_vec_pretty(&missing_packages)
        .expect("missing-package bundle should serialize");
    fs::write(&data_path, &missing_packages_bytes)
        .expect("missing-package bundle should be writable");
    let mut index: VersionIndex =
        serde_json::from_slice(&fs::read(&index_path).expect("index should exist"))
            .expect("index should parse");
    let missing_digest = sha256_hex(&missing_packages_bytes);
    for entry in &mut index.release_candidates {
        entry.data_sha256 = missing_digest.clone();
    }
    if let Some(latest) = &mut index.latest {
        latest.data_sha256 = missing_digest;
    }
    fs::write(
        &index_path,
        serde_json::to_vec_pretty(&index).expect("index should serialize"),
    )
    .expect("index should be writable");
    let before_missing = fs::read(&index_path).expect("index should remain readable");
    let error = write_bundle(&data, &output, false)
        .expect_err("bundle without packages must block publication");
    assert!(error.to_string().contains("requires packages"));
    assert_eq!(
        before_missing,
        fs::read(&index_path).expect("index should be unchanged")
    );
    fs::write(&data_path, &valid).expect("valid bundle should be restored");
    let valid_digest = sha256_hex(&valid);
    for entry in &mut index.release_candidates {
        entry.data_sha256 = valid_digest.clone();
    }
    if let Some(latest) = &mut index.latest {
        latest.data_sha256 = valid_digest;
    }
    fs::write(
        &index_path,
        serde_json::to_vec_pretty(&index).expect("index should serialize"),
    )
    .expect("index should be restored");
    let invalid = b"not DocsData";
    fs::write(&data_path, invalid).expect("invalid bundle should be writable");
    let index_path = output.join("sdk-docs-versions.v1.json");
    let mut index: VersionIndex =
        serde_json::from_slice(&fs::read(&index_path).expect("index should exist"))
            .expect("index should parse");
    let digest = sha256_hex(invalid);
    for entry in &mut index.release_candidates {
        entry.data_sha256 = digest.clone();
    }
    if let Some(latest) = &mut index.latest {
        latest.data_sha256 = digest;
    }
    fs::write(
        &index_path,
        serde_json::to_vec_pretty(&index).expect("index should serialize"),
    )
    .expect("index should be writable");
    let before = fs::read(&index_path).expect("index should remain readable");
    let error = write_bundle(&data, &output, false)
        .expect_err("hash-valid invalid JSON must block publication");
    assert!(error.to_string().contains("not valid DocsData"));
    assert_eq!(
        before,
        fs::read(&index_path).expect("index should be unchanged")
    );
    fs::remove_dir_all(output).expect("test output should be removable");
}

#[test]
fn version_index_rejects_bundle_revision_mismatch() {
    let output =
        std::env::temp_dir().join(format!("sdk-docs-revision-index-{}", std::process::id()));
    let _ = fs::remove_dir_all(&output);
    let data = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: "1.0.0".into(),
        channel: Channel::Release,
        source: SourceInfo {
            captured_source: None,
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            revision: "a".repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
            input_sha256: "a".repeat(64),
            rustdoc_format_versions: vec![FORMAT_VERSION],
            generator: "sdk-docs/test".into(),
        },
        navigation: Navigation {
            entries: Vec::new(),
        },
        packages: PackageCatalog {
            entries: Vec::new(),
        },
        search: SearchIndex {
            entries: Vec::new(),
        },
        families: Vec::new(),
    };
    write_bundle(&data, &output, false).expect("initial release should write");
    let index_path = output.join("sdk-docs-versions.v1.json");
    let mut index: VersionIndex =
        serde_json::from_slice(&fs::read(&index_path).expect("index should exist"))
            .expect("index should parse");
    for entry in &mut index.release_candidates {
        entry.revision = "b".repeat(40);
    }
    if let Some(latest) = &mut index.latest {
        latest.revision = "b".repeat(40);
    }
    fs::write(
        &index_path,
        serde_json::to_vec_pretty(&index).expect("index should serialize"),
    )
    .expect("index should be writable");
    let before = fs::read(&index_path).expect("index should remain readable");
    let error = write_bundle(&data, &output, false)
        .expect_err("bundle revision mismatch must block publication");
    assert!(error
        .to_string()
        .contains("data revision does not match the index"));
    assert_eq!(
        before,
        fs::read(&index_path).expect("index should be unchanged")
    );
    fs::remove_dir_all(output).expect("test output should be removable");
}

#[test]
fn version_index_advances_candidates_and_preview_without_rewriting_data() {
    let output = std::env::temp_dir().join(format!("sdk-docs-lifecycle-{}", std::process::id()));
    let _ = fs::remove_dir_all(&output);
    let mut data = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: "1.0.0".into(),
        channel: Channel::Release,
        source: SourceInfo {
            captured_source: None,
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            revision: "1".repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
            input_sha256: "a".repeat(64),
            rustdoc_format_versions: vec![60],
            generator: "sdk-docs/test".into(),
        },
        navigation: Navigation {
            entries: Vec::new(),
        },
        packages: PackageCatalog {
            entries: Vec::new(),
        },
        search: SearchIndex {
            entries: Vec::new(),
        },
        families: Vec::new(),
    };
    write_bundle(&data, &output, false).expect("first release should write");
    data.version = "1.1.0".into();
    data.source.revision = "2".repeat(40);
    write_bundle(&data, &output, false).expect("latest should advance to the next release");
    data.version = "0.9.0".into();
    data.source.revision = "9".repeat(40);
    write_bundle(&data, &output, false).expect("an older release should remain immutable");
    data.version = "0.10.0".into();
    data.source.revision = "8".repeat(40);
    write_bundle(&data, &output, false).expect("semantic release ordering should be accepted");
    data.version = "preview-1".into();
    data.channel = Channel::Preview;
    data.source.revision = "3".repeat(40);
    write_bundle(&data, &output, false).expect("preview should write");
    let preview_data_path = output.join("preview/preview-1/sdk-docs-data.v1.json");
    let preview_bytes = fs::read(&preview_data_path).expect("preview data should remain readable");
    let preview_index_path = output.join("sdk-docs-versions.v1.json");
    let preview_index_before_conflict =
        fs::read(&preview_index_path).expect("preview index should be readable");
    let mut conflicting_preview = data.clone();
    conflicting_preview.source.revision = "f".repeat(40);
    let error = write_bundle(&conflicting_preview, &output, false)
        .expect_err("same preview version with a different revision must be immutable");
    assert!(error.to_string().contains("refusing to rewrite"));
    assert_eq!(
        preview_bytes,
        fs::read(&preview_data_path).expect("preview data must remain unchanged")
    );
    assert_eq!(
        preview_index_before_conflict,
        fs::read(&preview_index_path).expect("preview index must remain unchanged")
    );
    data.version = "preview-2".into();
    data.source.revision = "4".repeat(40);
    write_bundle(&data, &output, false).expect("preview should advance");
    data.version = "branch_abc".into();
    data.source.revision = "5".repeat(40);
    write_bundle(&data, &output, false).expect("lowercase preview should write");
    data.version = "branch_ABC".into();
    data.source.revision = "6".repeat(40);
    write_bundle(&data, &output, false).expect("case-distinct preview should write");
    let index: VersionIndex = serde_json::from_slice(
        &fs::read(output.join("sdk-docs-versions.v1.json")).expect("index should exist"),
    )
    .expect("index should parse");
    assert_eq!(
        index
            .release_candidates
            .iter()
            .map(|entry| entry.version.as_str())
            .collect::<Vec<_>>(),
        vec!["0.9.0", "0.10.0", "1.0.0", "1.1.0"]
    );
    assert_eq!(
        index.latest.as_ref().map(|entry| entry.version.as_str()),
        None
    );
    assert_eq!(
        index.preview.as_ref().map(|entry| entry.version.as_str()),
        Some("branch_ABC")
    );
    assert!(output
        .join("preview/branch_abc/sdk-docs-data.v1.json")
        .is_file());
    assert!(output
        .join("preview/~6272616e63685f414243/sdk-docs-data.v1.json")
        .is_file());
    fs::remove_dir_all(output).expect("test output should be removable");
}

#[test]
fn version_index_latest_must_be_the_maximum_release() {
    let entry = |version: &str| VersionEntry {
        captured_source: None,
        publication_status: PublicationStatus::RegistryReleased,
        released_packages: vec![historical::ReleasedPackage {
            package: "policy-fixture".into(),
            version: version.into(),
            yanked: false,
            registry_checksum: "a".repeat(64),
            source_revision: "a".repeat(40),
            path_in_vcs: "fixture".into(),
        }],
        version: version.into(),
        channel: Channel::Release,
        revision: "a".repeat(40),
        data_file: format!("release-candidates/{version}/sdk-docs-data.v1.json"),
        data_sha256: "b".repeat(64),
    };
    let stale = VersionIndex {
        release_candidates: Vec::new(),
        historical_prereleases: Vec::new(),
        schema: VERSION_INDEX_SCHEMA_VERSION.into(),
        latest: Some(entry("1.0.0")),
        releases: vec![entry("1.0.0"), entry("2.0.0")],
        preview: None,
    };
    let error =
        validate_latest_release(&stale).expect_err("a stale latest release must be rejected");
    assert!(error.to_string().contains("maximum release 2.0.0"));

    let missing = VersionIndex {
        release_candidates: Vec::new(),
        historical_prereleases: Vec::new(),
        schema: VERSION_INDEX_SCHEMA_VERSION.into(),
        latest: None,
        releases: vec![entry("1.0.0")],
        preview: None,
    };
    let error = validate_latest_release(&missing)
        .expect_err("a non-empty release archive must have a latest release");
    assert!(error.to_string().contains("latest release is missing"));
}

#[test]
fn concurrent_publications_preserve_both_entries_and_index_hashes() {
    let output = std::env::temp_dir().join(format!("sdk-docs-concurrent-{}", std::process::id()));
    let _ = fs::remove_dir_all(&output);
    let make_data = |version: &str, revision: char| DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: version.into(),
        channel: Channel::Release,
        source: SourceInfo {
            captured_source: None,
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            revision: revision.to_string().repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", revision.to_string().repeat(64))),
            input_sha256: revision.to_string().repeat(64),
            rustdoc_format_versions: vec![FORMAT_VERSION],
            generator: "sdk-docs/test".into(),
        },
        navigation: Navigation {
            entries: Vec::new(),
        },
        packages: PackageCatalog {
            entries: Vec::new(),
        },
        search: SearchIndex {
            entries: Vec::new(),
        },
        families: Vec::new(),
    };
    let first_data = make_data("1.0.0", 'a');
    let second_data = make_data("2.0.0", 'b');
    let barrier = Arc::new(Barrier::new(2));
    let first_output = output.clone();
    let first_barrier = Arc::clone(&barrier);
    let first = thread::spawn(move || {
        first_barrier.wait();
        write_bundle(&first_data, &first_output, false)
    });
    let second_output = output.clone();
    let second_barrier = Arc::clone(&barrier);
    let second = thread::spawn(move || {
        second_barrier.wait();
        write_bundle(&second_data, &second_output, false)
    });
    first
        .join()
        .expect("first publication thread should complete")
        .expect("first concurrent publication should succeed");
    second
        .join()
        .expect("second publication thread should complete")
        .expect("second concurrent publication should succeed");

    let index: VersionIndex = serde_json::from_slice(
        &fs::read(output.join("sdk-docs-versions.v1.json")).expect("index should exist"),
    )
    .expect("index should parse");
    assert_eq!(
        index
            .release_candidates
            .iter()
            .map(|entry| entry.version.as_str())
            .collect::<Vec<_>>(),
        vec!["1.0.0", "2.0.0"]
    );
    assert_eq!(
        index.latest.as_ref().map(|entry| entry.version.as_str()),
        None
    );
    for entry in &index.release_candidates {
        let bytes = fs::read(output.join(&entry.data_file)).expect("bundle should exist");
        assert_eq!(sha256_hex(&bytes), entry.data_sha256);
    }
    fs::remove_dir_all(output).expect("test output should be removable");
}
