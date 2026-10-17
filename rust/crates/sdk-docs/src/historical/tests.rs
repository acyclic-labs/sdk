use super::*;

fn fixture(root: &Path, version: &str) -> (BuildInput, Input) {
    fs::create_dir_all(root.join("cli/src")).unwrap();
    let source = root.join("cli/src/main.rs");
    fs::write(&source, "//! Source-owned CLI guide.\nfn main() {}\n").unwrap();
    let readme = root.join("cli/README.md");
    fs::write(&readme, "# CLI\n\ncargo install --locked --path cli\n").unwrap();
    let receipt = root.join("cli.json");
    let item = |id, name, docs, visibility, inner| {
        serde_json::json!({
            "id":id,"crate_id":0,"name":name,"span": {"filename":"cli/src/main.rs","begin":[1,0],"end":[2,12]},
            "visibility":visibility,"docs":docs,"links":{},"attrs":[],"deprecation":null,"stability":null,"const_stability":null,"inner":inner
        })
    };
    fs::write(&receipt, serde_json::to_vec(&serde_json::json!({
        "root":0,"crate_version":version,"includes_private":true,"format_version":60,
        "index":{"0":item(0,"cli","Source-owned CLI guide.","public",serde_json::json!({"module":{"is_crate":true,"items":[1],"is_stripped":false}})),
            "1":item(1,"hidden","PRIVATE CLI INTERNALS","default",serde_json::json!({"module":{"is_crate":false,"items":[],"is_stripped":false}}))},
        "paths":{},"external_crates":{},"target":{"triple":"x86_64-pc-windows-msvc","target_features":[]}
    })).unwrap()).unwrap();
    let input = BuildInput {
        version: version.into(),
        channel: Channel::Release,
        revision: "a".repeat(40),
        source_state: "captured-snapshot".into(),
        source_sha256: Some(format!("sha256:{}", "b".repeat(64))),
        repository_root: root.into(),
        rustdoc_files: vec![receipt.clone()],
        package_metadata: vec![PackageMetadata {
            rustdoc_source_root: None,
            rustdoc_file: receipt.clone(),
            package_name: "demo-cli".into(),
            crate_name: "cli".into(),
            version: version.into(),
        }],
        generated_sources: Vec::new(),
        mark_latest: false,
    };
    let scope = Input {
        captured_source: None,
        released_packages: vec![ReleasedPackage {
            package: "demo-cli".into(),
            version: version.into(),
            yanked: false,
            registry_checksum: "c".repeat(64),
            source_revision: input.revision.clone(),
            path_in_vcs: "cli".into(),
        }],
        binaries: vec![BinaryInput {
            rustdoc_file: receipt,
            source_file: source,
            readme: Some(readme),
        }],
    };
    (input, scope)
}

#[test]
fn binary_guides_preserve_native_source_without_exposing_private_items() {
    let root = tempfile::tempdir().unwrap();
    let (input, scope) = fixture(root.path(), "0.1.0");
    assert!(build_data(&input, &[])
        .unwrap_err()
        .to_string()
        .contains("private rustdoc"));
    let candidate = build_data(&input, &scope.binaries).unwrap();
    assert_eq!(
        candidate.source.publication_status,
        PublicationStatus::Candidate
    );
    assert!(candidate.source.released_packages.is_empty());
    assert!(candidate.families[0].items.is_empty());
    assert_eq!(candidate.families[0].guides.len(), 2);
    assert!(!serde_json::to_string(&candidate)
        .unwrap()
        .contains("PRIVATE CLI INTERNALS"));
    let data = build(&input, &scope).unwrap();
    assert!(data.families[0].items.is_empty());
    assert_eq!(data.families[0].guides.len(), 2);
    let bytes = serde_json::to_string(&data).unwrap();
    assert!(bytes.contains("cargo install --locked --path cli"));
    assert!(!bytes.contains("PRIVATE CLI INTERNALS"));
    let mut unverified = data.clone();
    let mut extra = unverified.packages.entries[0].clone();
    extra.package_name = "unreleased-extra".into();
    unverified.packages.entries.push(extra);
    assert!(validate_release_identity(&unverified)
        .unwrap_err()
        .to_string()
        .contains("no verified released archive"));
    let mut escaped = scope.clone();
    escaped.binaries[0].source_file = root.path().join("cli/README.md");
    assert!(build(&input, &escaped)
        .unwrap_err()
        .to_string()
        .contains("span differs"));
    let mut wrong_identity = scope;
    wrong_identity.released_packages[0].source_revision = "d".repeat(40);
    assert!(build(&input, &wrong_identity).is_err());
}

#[test]
fn producer_dependency_identity_is_complete_and_sorted() {
    let prefix = ".docs-producer/demo-0.1.0/";
    let producer = ProducerResolution {
        lock_path: format!("{prefix}Cargo.lock"),
        lock_sha256: format!("sha256:{}", "b".repeat(64)),
        config_path: format!("{prefix}config.toml"),
        config_sha256: format!("sha256:{}", "c".repeat(64)),
        dependency_archives: Vec::new(),
    };
    let encoded = serde_json::to_value(&producer).unwrap();
    assert_eq!(encoded["dependencyArchives"], serde_json::json!([]));
    assert_eq!(
        serde_json::from_value::<ProducerResolution>(encoded).unwrap(),
        producer
    );
    let dependency = ProducerDependencyArchive {
        package: "dep".into(),
        version: "1.0.0-rc.2".into(),
        registry_checksum: "d".repeat(64),
        publisher_vcs: Some(PublisherVcs {
            revision: "e".repeat(40),
            dirty: true,
            path_in_vcs: "crates/dep".into(),
        }),
        yanked: true,
    };
    let archive = ArchiveSource {
        package: "demo".into(),
        version: "0.1.0".into(),
        registry_checksum: "a".repeat(64),
        publisher_vcs: None,
        resolution_lock: ResolutionLock::SeparateDocsProducer {
            original_lock: OriginalLock::Published {
                sha256: format!("sha256:{}", "f".repeat(64)),
            },
            producer,
        },
    };
    let mut captured = CapturedSource::RegistryArchives {
        archives: vec![archive],
    };
    let unpatched_revision = captured.revision().unwrap();
    let mutate = |captured: &mut CapturedSource, f: fn(&mut ProducerResolution)| {
        let CapturedSource::RegistryArchives { archives } = captured else {
            unreachable!()
        };
        let ResolutionLock::SeparateDocsProducer { producer, .. } =
            &mut archives[0].resolution_lock
        else {
            unreachable!()
        };
        f(producer);
    };
    let CapturedSource::RegistryArchives { archives } = &mut captured else {
        unreachable!()
    };
    let ResolutionLock::SeparateDocsProducer { producer, .. } = &mut archives[0].resolution_lock
    else {
        unreachable!()
    };
    producer.dependency_archives.push(dependency);
    let revision = captured.revision().unwrap();
    assert_ne!(revision, unpatched_revision);
    for change in [
        (|p: &mut ProducerResolution| p.dependency_archives[0].registry_checksum = "0".repeat(64))
            as fn(&mut ProducerResolution),
        |p| p.dependency_archives[0].yanked = false,
        |p| {
            p.dependency_archives[0]
                .publisher_vcs
                .as_mut()
                .unwrap()
                .dirty = false
        },
    ] {
        let mut changed = captured.clone();
        mutate(&mut changed, change);
        assert_ne!(changed.revision().unwrap(), revision);
    }
    for change in [
        (|p: &mut ProducerResolution| p.dependency_archives[0].package = "../dep".into())
            as fn(&mut ProducerResolution),
        |p| p.dependency_archives[0].version = "not-semver".into(),
        |p| p.dependency_archives[0].registry_checksum = "missing".into(),
        |p| {
            p.dependency_archives[0]
                .publisher_vcs
                .as_mut()
                .unwrap()
                .revision = "f".repeat(64)
        },
        |p| {
            p.dependency_archives[0]
                .publisher_vcs
                .as_mut()
                .unwrap()
                .path_in_vcs = "../foreign".into()
        },
        |p| p.dependency_archives.push(p.dependency_archives[0].clone()),
        |p| {
            let mut other = p.dependency_archives[0].clone();
            other.package = "aaa".into();
            p.dependency_archives.push(other);
        },
    ] {
        let mut changed = captured.clone();
        mutate(&mut changed, change);
        assert!(changed.revision().is_err());
    }
}

#[test]
fn separate_resolution_identity_distinguishes_original_and_producer_locks() {
    let prefix = ".docs-producer/demo-cli-0.1.0/";
    let archive = ArchiveSource {
        package: "demo-cli".into(),
        version: "0.1.0".into(),
        registry_checksum: "c".repeat(64),
        publisher_vcs: None,
        resolution_lock: ResolutionLock::SeparateDocsProducer {
            original_lock: OriginalLock::Published {
                sha256: format!("sha256:{}", "a".repeat(64)),
            },
            producer: ProducerResolution {
                lock_path: format!("{prefix}Cargo.lock"),
                lock_sha256: format!("sha256:{}", "b".repeat(64)),
                config_path: format!("{prefix}config.toml"),
                dependency_archives: Vec::new(),
                config_sha256: format!("sha256:{}", "d".repeat(64)),
            },
        },
    };
    let captured = CapturedSource::RegistryArchives {
        archives: vec![archive],
    };
    let revision = captured.revision().unwrap();
    let mut changed = captured.clone();
    let CapturedSource::RegistryArchives { archives } = &mut changed else {
        unreachable!()
    };
    let ResolutionLock::SeparateDocsProducer { original_lock, .. } =
        &mut archives[0].resolution_lock
    else {
        unreachable!()
    };
    *original_lock = OriginalLock::Absent;
    assert_ne!(changed.revision().unwrap(), revision);
    let CapturedSource::RegistryArchives { archives } = &mut changed else {
        unreachable!()
    };
    let ResolutionLock::SeparateDocsProducer { producer, .. } = &mut archives[0].resolution_lock
    else {
        unreachable!()
    };
    producer.lock_path = "../Cargo.lock".into();
    assert!(changed.revision().is_err());
    let bytes = serde_json::to_vec(&captured).unwrap();
    assert_eq!(
        serde_json::from_slice::<CapturedSource>(&bytes).unwrap(),
        captured
    );
}

#[test]
fn archive_capture_keeps_dirty_publisher_vcs_separate_and_binds_catalog_identity() {
    let root = tempfile::tempdir().unwrap();
    let (mut input, mut scope) = fixture(root.path(), "0.1.0");
    let publisher = "f".repeat(40);
    let captured = CapturedSource::RegistryArchives {
        archives: vec![ArchiveSource {
            package: "demo-cli".into(),
            version: "0.1.0".into(),
            registry_checksum: "c".repeat(64),
            publisher_vcs: Some(PublisherVcs {
                revision: publisher.clone(),
                dirty: true,
                path_in_vcs: "cli".into(),
            }),
            resolution_lock: ResolutionLock::Published {
                sha256: format!("sha256:{}", "d".repeat(64)),
            },
        }],
    };
    input.revision = captured.revision().unwrap();
    input.source_state = "registry-archives".into();
    scope.released_packages[0].source_revision = publisher.clone();
    scope.captured_source = Some(captured.clone());
    let package = root.path().join("demo-cli-0.1.0");
    fs::create_dir(&package).unwrap();
    fs::rename(root.path().join("cli"), package.join("cli")).unwrap();
    scope.binaries[0].source_file = package.join("cli/src/main.rs");
    scope.binaries[0].readme = Some(package.join("cli/README.md"));
    let data = build(&input, &scope).unwrap();
    assert_ne!(data.source.revision, publisher);
    assert_eq!(data.source.captured_source.as_ref(), Some(&captured));
    let mut wrong_owner = input.clone();
    wrong_owner.package_metadata[0].package_name = "unregistered-owner".into();
    assert!(build(&wrong_owner, &scope)
        .unwrap_err()
        .to_string()
        .contains("no captured registry archive"));
    let hidden = root.path().join("hidden-package");
    fs::rename(&package, &hidden).unwrap();
    assert!(build(&input, &scope)
        .unwrap_err()
        .to_string()
        .contains("archive package root is unavailable"));
    fs::rename(hidden, &package).unwrap();
    let original = fs::read(&input.rustdoc_files[0]).unwrap();
    let mut malformed: serde_json::Value = serde_json::from_slice(&original).unwrap();
    malformed["index"]["0"]["span"]["filename"] = "missing/main.rs".into();
    fs::write(
        &input.rustdoc_files[0],
        serde_json::to_vec(&malformed).unwrap(),
    )
    .unwrap();
    assert!(build(&input, &scope)
        .unwrap_err()
        .to_string()
        .contains("no exact owner source"));
    fs::write(&input.rustdoc_files[0], original).unwrap();
    let mut false_git = data.clone();
    false_git.source.captured_source = Some(CapturedSource::Git {
        revision: publisher,
    });
    assert!(validate_source_info(&false_git.source, &false_git.channel).is_err());
    let mut false_archive = data.clone();
    false_archive.source.revision = "a".repeat(64);
    assert!(validate_source_info(&false_archive.source, &false_archive.channel).is_err());
    let output = tempfile::tempdir().unwrap();
    write_bundle(&data, output.path(), true).unwrap();
    let mut index = load_version_index(output.path()).unwrap();
    index.releases[0].captured_source = Some(CapturedSource::Git {
        revision: data.source.revision.clone(),
    });
    index.latest = Some(index.releases[0].clone());
    assert!(validate_version_index(&index, output.path())
        .unwrap_err()
        .to_string()
        .contains("captured source differs"));
}

#[test]
fn include_str_readme_keeps_original_root_guide_without_duplicate() {
    let root = tempfile::tempdir().unwrap();
    let (input, scope) = fixture(root.path(), "0.1.0");
    let readme = fs::read_to_string(scope.binaries[0].readme.as_ref().unwrap()).unwrap();
    let receipt = &input.rustdoc_files[0];
    let mut native: serde_json::Value =
        serde_json::from_slice(&fs::read(receipt).unwrap()).unwrap();
    native["index"]["0"]["docs"] = readme.clone().into();
    fs::write(receipt, serde_json::to_vec(&native).unwrap()).unwrap();
    let data = build(&input, &scope).unwrap();
    assert_eq!(data.families[0].guides.len(), 1);
    assert_eq!(data.families[0].guides[0].path, "cli/src/main.rs");
    assert_eq!(data.families[0].guides[0].markdown, readme);
}

#[test]
fn immutable_yanked_prereleases_never_replace_latest_stable_and_bind_index_metadata() {
    let root = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    for version in ["0.1.5", "1.0.0-rc.3", "0.1.1"] {
        let (input, mut scope) = fixture(root.path(), version);
        if version.contains('-') {
            scope.released_packages[0].yanked = true;
        }
        if version.contains('-') {
            assert!(build_data(&input, &[]).is_err());
        }
        let data = build(&input, &scope).unwrap();
        write_bundle(&data, output.path(), false).unwrap();
        let unadmitted = load_version_index(output.path()).unwrap();
        assert!(!unadmitted
            .releases
            .iter()
            .chain(&unadmitted.historical_prereleases)
            .any(|entry| entry.version == version));
        write_bundle(&data, output.path(), true).unwrap();
        if version.contains('-') {
            assert!(super::super::write_bundle(&data, output.path(), false).is_err());
            let mut changed = data.clone();
            changed.source.released_packages[0].yanked = false;
            assert!(write_bundle(&changed, output.path(), true).is_err());
        }
    }
    let mut index = load_version_index(output.path()).unwrap();
    assert_eq!(index.latest.as_ref().unwrap().version, "0.1.5");
    assert_eq!(index.historical_prereleases.len(), 1);
    assert!(index.historical_prereleases[0].released_packages[0].yanked);
    index.historical_prereleases[0].released_packages[0].yanked = false;
    fs::write(
        output.path().join("sdk-docs-versions.v1.json"),
        serde_json::to_vec(&index).unwrap(),
    )
    .unwrap();
    assert!(load_version_index(output.path())
        .unwrap_err()
        .to_string()
        .contains("immutable data"));
}
#[test]
fn qualified_candidates_are_explicit_and_cannot_replace_actual_registry_latest() {
    let root = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let (input, scope) = fixture(root.path(), "0.1.5");
    let released = build(&input, &scope).unwrap();
    write_bundle(&released, output.path(), true).unwrap();
    let mut candidate = released.clone();
    candidate.version = "0.2.0".into();
    candidate.source.publication_status = PublicationStatus::Candidate;
    candidate.source.released_packages.clear();
    let before = fs::read(output.path().join("sdk-docs-versions.v1.json")).unwrap();
    assert!(super::super::write_bundle(&candidate, output.path(), true)
        .unwrap_err()
        .to_string()
        .contains("candidate cannot be marked latest"));
    assert_eq!(
        before,
        fs::read(output.path().join("sdk-docs-versions.v1.json")).unwrap()
    );
    super::super::write_bundle(&candidate, output.path(), false).unwrap();
    let index = load_version_index(output.path()).unwrap();
    assert_eq!(index.latest.as_ref().unwrap().version, "0.1.5");
    assert_eq!(index.release_candidates.len(), 1);
    assert_eq!(
        index.release_candidates[0].publication_status,
        PublicationStatus::Candidate
    );
    assert_eq!(
        index.release_candidates[0].data_file,
        "release-candidates/0.2.0/sdk-docs-data.v1.json"
    );
    assert!(
        fs::read_to_string(output.path().join(&index.release_candidates[0].data_file))
            .unwrap()
            .contains("\"publicationStatus\": \"candidate\"")
    );
}
#[test]
fn yanked_stable_is_selectable_immutable_and_never_latest() {
    let root = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    for (version, yanked, latest) in [
        ("0.1.0", false, "0.1.0"),
        ("0.1.1", true, "0.1.0"),
        ("0.1.5", false, "0.1.5"),
    ] {
        let (input, mut scope) = fixture(root.path(), version);
        scope.released_packages[0].yanked = yanked;
        let data = build(&input, &scope).unwrap();
        write_bundle(&data, output.path(), true).unwrap();
        let mut index = load_version_index(output.path()).unwrap();
        assert_eq!(index.latest.as_ref().unwrap().version, latest);
        assert!(index.releases.iter().any(|entry| entry.version == version));
        if yanked {
            let mut changed = data.clone();
            changed.source.released_packages[0].yanked = false;
            assert!(write_bundle(&changed, output.path(), true).is_err());
            index.latest = index
                .releases
                .iter()
                .find(|entry| entry.version == version)
                .cloned();
            assert!(validate_version_index(&index, output.path())
                .unwrap_err()
                .to_string()
                .contains("maximum release"));
        }
    }
}

#[test]
fn relative_span_validation_rechecks_missing_and_restored_files_each_call() {
    let root = tempfile::tempdir().unwrap();
    let (input, _) = fixture(root.path(), "1.0.0");
    let krate: Crate = serde_json::from_slice(&fs::read(&input.rustdoc_files[0]).unwrap()).unwrap();
    let root = root.path().canonicalize().unwrap();
    let before = RELATIVE_SOURCE_CHECKS.get();
    validate_relative_package_spans(&root, &krate).unwrap();
    assert_eq!(
        RELATIVE_SOURCE_CHECKS.get() - before,
        1,
        "two item occurrences must perform one production source check"
    );
    let path = root.join("cli/src/main.rs");
    let bytes = fs::read(&path).unwrap();
    fs::remove_file(&path).unwrap();
    assert!(validate_relative_package_spans(&root, &krate).is_err());
    fs::write(path, bytes).unwrap();
    validate_relative_package_spans(&root, &krate).unwrap();
}
