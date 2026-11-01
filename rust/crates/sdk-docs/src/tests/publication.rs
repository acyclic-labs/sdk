use super::*;

pub(super) fn publication_data(version: &str, revision: char) -> DocsData {
    DocsData {
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
    }
}

#[test]
fn equal_publication_preserves_files_and_still_rejects_external_corruption() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("output");
    let data = publication_data("1.0.0", 'a');
    write_bundle(&data, &output, false).unwrap();
    let paths = [
        output.join("release-candidates/1.0.0/sdk-docs-data.v1.json"),
        output.join("release-candidates/1.0.0/sdk-docs-data.v1.schema.json"),
        output.join("sdk-docs-versions.v1.schema.json"),
        output.join("sdk-docs-versions.v1.json"),
    ];
    // Keep the original files open so an unlink cannot recycle their identity.
    let originals: Vec<_> = paths.iter().map(|path| File::open(path).unwrap()).collect();
    let stamps: Vec<_> = paths
        .iter()
        .map(|path| sdk_file_identity::stamp(path).unwrap())
        .collect();
    write_bundle(&data, &output, false).unwrap();
    assert_eq!(
        stamps,
        paths
            .iter()
            .map(|path| sdk_file_identity::stamp(path).unwrap())
            .collect::<Vec<_>>(),
        "an equal publication must preserve every original file identity"
    );
    let index = fs::read(&paths[3]).unwrap();
    for (path, expected_error) in [
        (&paths[0], "data digest does not match"),
        (&paths[1], "schema sidecar does not match"),
    ] {
        let bytes = fs::read(path).unwrap();
        fs::write(path, b"externally changed").unwrap();
        let error = write_bundle(&data, &output, false).unwrap_err();
        assert!(error.to_string().contains(expected_error), "{error}");
        assert_eq!(fs::read(&paths[3]).unwrap(), index);
        fs::write(path, bytes).unwrap();
    }
    write_bundle(&data, &output, false).unwrap();
    assert_eq!(sdk_file_identity::stamp(&paths[3]).unwrap(), stamps[3]);
    drop(originals);
    directory.close().unwrap();
}

#[test]
fn release_publication_rejects_unbound_source_before_creating_output() {
    let directory = tempfile::tempdir().expect("fixture directory should be creatable");
    let output = directory.path().join("output");
    let mut data = publication_data("1.0.0", 'a');
    data.source.source_state = "working-tree".into();
    data.source.source_sha256 = None;
    data.source.input_sha256 = "b".repeat(64);
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
    let directory = tempfile::tempdir().expect("fixture directory should be creatable");
    let output = directory.path().join("output");
    fs::create_dir_all(output.join("sdk-docs-versions.v1.json"))
        .expect("index directory should be creatable");
    let data = publication_data("1.0.0", 'a');
    let error = write_bundle(&data, &output, false)
        .expect_err("directory at the index path must block publication");
    assert!(
        error
            .to_string()
            .contains("version index is not a regular file")
    );
    assert!(output.join("sdk-docs-versions.v1.json").is_dir());
    assert!(!output.join("releases").exists());
    directory.close().expect("test output should be removable");
}

#[test]
fn version_index_rejects_a_conflicting_release_rewrite() {
    let directory = tempfile::tempdir().expect("fixture directory should be creatable");
    let output = directory.path().join("output");
    let mut data = publication_data("1.0.0", 'a');
    data.source.rustdoc_format_versions = vec![60];
    write_bundle(&data, &output, false).expect("initial release should write");
    let mut conflicting = data;
    conflicting.source.revision = "b".repeat(40);
    let error = write_bundle(&conflicting, &output, false).expect_err("release rewrite must fail");
    assert!(error.to_string().contains("refusing to rewrite"));
    directory.close().expect("test output should be removable");
}

#[test]
fn malformed_prior_version_index_is_rejected_without_mutation() {
    let directory = tempfile::tempdir().expect("fixture directory should be creatable");
    let output = directory.path().join("output");
    let candidate = publication_data("1.0.0", 'a');
    write_bundle(&candidate, &output, false).unwrap();
    let index_path = output.join("sdk-docs-versions.v1.json");
    let canonical: VersionIndex = serde_json::from_slice(&fs::read(&index_path).unwrap()).unwrap();
    assert_eq!(load_version_index(&output).unwrap(), canonical);
    for misplaced_release in [true, false] {
        let mut malformed = canonical.clone();
        let entry = malformed.release_candidates[0].clone();
        if misplaced_release {
            malformed.release_candidates.clear();
            malformed.releases.push(entry);
        } else {
            malformed.latest = Some(entry);
        }
        let bytes = serde_json::to_vec_pretty(&malformed).unwrap();
        fs::write(&index_path, &bytes).unwrap();
        assert!(matches!(
            load_version_index(&output),
            Err(Error::Invalid(_))
        ));
        assert_eq!(fs::read(&index_path).unwrap(), bytes);
    }
    let malformed = VersionIndex {
        historical_prereleases: Vec::new(),
        schema: VERSION_INDEX_SCHEMA_VERSION.into(),
        latest: None,
        releases: Vec::new(),
        release_candidates: vec![VersionEntry {
            captured_source: None,
            publication_status: PublicationStatus::Candidate,
            released_packages: Vec::new(),
            version: "0.9.0".into(),
            channel: Channel::Release,
            revision: "a".repeat(40),
            data_file: "release-candidates/0.9.0/sdk-docs-data.v1.json".into(),
            data_sha256: "0".repeat(64),
        }],
        preview: None,
    };
    fs::write(
        &index_path,
        serde_json::to_vec_pretty(&malformed).expect("malformed index should serialize"),
    )
    .expect("malformed index should write");
    let before = fs::read(&index_path).expect("malformed index should remain readable");
    let data = publication_data("2.0.0", 'b');
    let error = write_bundle(&data, &output, false)
        .expect_err("malformed prior index must block publication");
    assert!(error.to_string().contains("missing data file"));
    assert_eq!(
        before,
        fs::read(&index_path).expect("malformed index should be unchanged")
    );
    assert!(!output.join("releases").exists());

    let data_path = output.join("release-candidates/0.9.0/sdk-docs-data.v1.json");
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
    directory.close().expect("test output should be removable");
}

#[test]
fn version_index_rejects_invalid_docs_json_even_when_hash_matches() {
    let directory = tempfile::tempdir().expect("fixture directory should be creatable");
    let output = directory.path().join("output");
    let data = publication_data("1.0.0", 'a');
    write_bundle(&data, &output, false).expect("initial release should write");
    let data_path = output.join("release-candidates/1.0.0/sdk-docs-data.v1.json");
    let valid = fs::read(&data_path).expect("valid bundle should be readable");
    let schema_path = output.join("release-candidates/1.0.0/sdk-docs-data.v1.schema.json");
    let schema = fs::read(&schema_path).expect("schema sidecar should be readable");
    let index_path = output.join("sdk-docs-versions.v1.json");
    let index_before_sidecar_failure =
        fs::read(&index_path).expect("index should be readable before sidecar checks");
    for (replacement, expected) in [
        (None, "missing schema sidecar"),
        (Some(b"{}".as_slice()), "does not match the pinned schema"),
    ] {
        if let Some(bytes) = replacement {
            fs::write(&schema_path, bytes).unwrap();
        } else {
            fs::remove_file(&schema_path).unwrap();
        }
        let error = write_bundle(&data, &output, false).unwrap_err();
        assert!(matches!(error, Error::Invalid(_)), "{error}");
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(fs::read(&data_path).unwrap(), valid);
        assert_eq!(fs::read(&index_path).unwrap(), index_before_sidecar_failure);
    }
    fs::write(&schema_path, schema).expect("valid schema sidecar should be restored");
    let canonical: VersionIndex = serde_json::from_slice(&index_before_sidecar_failure).unwrap();
    let document: serde_json::Value = serde_json::from_slice(&valid).unwrap();
    let next = publication_data("2.0.0", 'b');
    let mut malformed = vec![
        (b"not DocsData".to_vec(), "must be a JSON object".to_owned()),
        (b"[]".to_vec(), "must be a JSON object".to_owned()),
        (b"{}".to_vec(), "not valid DocsData".to_owned()),
    ];
    // A complete positional struct representation must not widen the JSON format.
    let positional: Vec<_> = [
        "schema",
        "schemaVersion",
        "version",
        "channel",
        "source",
        "navigation",
        "packages",
        "search",
        "families",
    ]
    .into_iter()
    .map(|key| document[key].clone())
    .collect();
    malformed.push((
        serde_json::to_vec(&positional).unwrap(),
        "must be a JSON object".into(),
    ));
    for key in ["schema", "schemaVersion", "packages", "search"] {
        let mut missing = document.clone();
        missing.as_object_mut().unwrap().remove(key);
        malformed.push((
            serde_json::to_vec(&missing).unwrap(),
            format!("missing field `{key}`"),
        ));
    }
    for key in ["schema", "schemaVersion"] {
        let mut wrong = document.clone();
        wrong[key] = "unsupported-sdk-docs-data".into();
        malformed.push((
            serde_json::to_vec(&wrong).unwrap(),
            "unsupported docs schema".into(),
        ));
    }
    // Preserve the original typed decoder's duplicate-field refusal. Decoding
    // through a generic JSON map would silently keep only the final value.
    malformed.push((
        format!(
            "{{\"schema\":\"{DATA_SCHEMA_VERSION}\",{}",
            std::str::from_utf8(&valid)
                .unwrap()
                .strip_prefix('{')
                .unwrap()
        )
        .into_bytes(),
        "duplicate field `schema`".into(),
    ));
    for (bytes, expected) in malformed {
        fs::write(&data_path, &bytes).unwrap();
        let mut index = canonical.clone();
        index.release_candidates[0].data_sha256 = sha256_hex(&bytes);
        let before = serde_json::to_vec_pretty(&index).unwrap();
        fs::write(&index_path, &before).unwrap();
        let error = write_bundle(&next, &output, false).unwrap_err();
        assert!(matches!(error, Error::Invalid(_)), "{error}");
        assert!(error.to_string().contains(&expected), "{expected}: {error}");
        assert_eq!(fs::read(&index_path).unwrap(), before);
        assert_eq!(fs::read(&data_path).unwrap(), bytes);
        assert!(!output.join("release-candidates/2.0.0").exists());
    }
    fs::write(&data_path, valid).unwrap();
    fs::write(&index_path, index_before_sidecar_failure).unwrap();
    write_bundle(&next, &output, false).unwrap();
    assert_eq!(
        load_version_index(&output)
            .unwrap()
            .release_candidates
            .len(),
        2
    );
    directory.close().expect("test output should be removable");
}

#[test]
fn version_index_rejects_bundle_revision_mismatch() {
    let directory = tempfile::tempdir().expect("fixture directory should be creatable");
    let output = directory.path().join("output");
    let data = publication_data("1.0.0", 'a');
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
    assert!(
        error
            .to_string()
            .contains("data revision does not match the index")
    );
    assert_eq!(
        before,
        fs::read(&index_path).expect("index should be unchanged")
    );
    directory.close().expect("test output should be removable");
}

#[test]
fn version_index_advances_candidates_and_preview_without_rewriting_data() {
    let directory = tempfile::tempdir().expect("fixture directory should be creatable");
    let output = directory.path().join("output");
    let mut data = publication_data("1.0.0", 'a');
    data.source.revision = "1".repeat(40);
    data.source.rustdoc_format_versions = vec![60];
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
    assert!(
        output
            .join("preview/branch_abc/sdk-docs-data.v1.json")
            .is_file()
    );
    assert!(
        output
            .join("preview/~6272616e63685f414243/sdk-docs-data.v1.json")
            .is_file()
    );
    directory.close().expect("test output should be removable");
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
    let directory = tempfile::tempdir().expect("fixture directory should be creatable");
    let output = directory.path().join("output");
    let first_data = publication_data("1.0.0", 'a');
    let second_data = publication_data("2.0.0", 'b');
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
    directory.close().expect("test output should be removable");
}
