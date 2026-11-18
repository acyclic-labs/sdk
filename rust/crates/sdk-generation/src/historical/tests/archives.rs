use super::*;

#[test]
fn retained_inventory_accepts_exact_flat_and_nested_members_only() {
    let fixture = Fixture::new();
    let directory = fixture.path().canonicalize().unwrap().join("inventory");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("archive.crate"), b"flat").unwrap();
    let mut expected = BTreeSet::from(["archive.crate".into()]);
    validate_archive_inventory(&directory, &expected).unwrap();
    validate_archive_inventory(&directory.join("."), &expected).unwrap();
    #[cfg(unix)]
    {
        let cwd = std::env::current_dir().unwrap().canonicalize().unwrap();
        let common = cwd.ancestors().find(|p| directory.starts_with(p)).unwrap();
        let relative =
            PathBuf::from("../".repeat(cwd.strip_prefix(common).unwrap().components().count()))
                .join(directory.strip_prefix(common).unwrap());
        validate_archive_inventory(&relative, &expected).unwrap();
    }
    fs::create_dir_all(directory.join("dependencies/dep/src")).unwrap();
    fs::write(directory.join("dependencies/dep/src/lib.rs"), b"nested").unwrap();
    expected.insert("dependencies/dep/src/lib.rs".into());
    validate_archive_inventory(&directory, &expected).unwrap();
    fs::create_dir(directory.join("empty-extra")).unwrap();
    assert!(validate_archive_inventory(&directory, &expected).is_err());
    fs::remove_dir(directory.join("empty-extra")).unwrap();
    fs::write(directory.join("extra"), b"unknown").unwrap();
    assert!(validate_archive_inventory(&directory, &expected).is_err());
    fs::remove_file(directory.join("extra")).unwrap();
    fs::remove_file(directory.join("dependencies/dep/src/lib.rs")).unwrap();
    assert!(validate_archive_inventory(&directory, &expected).is_err());
    #[cfg(unix)]
    {
        fs::write(directory.join("dependencies/dep/src/lib.rs"), b"nested").unwrap();
        std::os::unix::fs::symlink(directory.join("archive.crate"), directory.join("redirect"))
            .unwrap();
        expected.insert("redirect".into());
        assert!(validate_archive_inventory(&directory, &expected).is_err());
        let alias = fixture.path().join("inventory-alias");
        std::os::unix::fs::symlink(&directory, &alias).unwrap();
        assert!(validate_archive_inventory(&alias, &expected).is_err());
    }
}

#[test]
fn separate_producer_resolution_retains_original_lock_and_rejects_config_inventory_drift() {
    compiler::with(
        separate_producer_resolution_retains_original_lock_and_rejects_config_inventory_drift_world,
    );
}

fn separate_producer_resolution_retains_original_lock_and_rejects_config_inventory_drift_world() {
    let fixture = Fixture::new();
    fs::write(
        fixture.path().join("Cargo.lock"),
        "broken original published lock\n",
    )
    .unwrap();
    let original = fs::read(fixture.path().join("Cargo.lock")).unwrap();
    let directory = "historical-fixture-0.1.0";
    let prefix = format!(".docs-producer/{directory}/");
    let producer_dir = fixture.path().join(&prefix);
    fs::create_dir_all(&producer_dir).unwrap();
    let external_lock = producer_dir.join("Cargo.lock");
    let external_lock = external_lock
        .parent()
        .unwrap()
        .canonicalize()
        .unwrap()
        .join("Cargo.lock");
    let config = format!("[resolver]\nlockfile-path = \"{directory}/Cargo.lock\"\n");
    fs::write(producer_dir.join("config.toml"), &config).unwrap();
    let cargo = cargo();
    let config_path = producer_dir.join("config.toml");
    let context = sdk_docs::rustdoc_profiles::CargoExecutionContext {
        cargo_path: Some(&cargo),
        lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(
            &config_path,
        )),
    };
    let mut generate = Command::new(&cargo);
    generate
        .args(["generate-lockfile", "--offline", "--manifest-path"])
        .arg(fixture.path().join("Cargo.toml"));
    context.configure(&mut generate).unwrap();
    command_output(&mut generate).unwrap();
    assert_eq!(
        fs::read(fixture.path().join("Cargo.lock")).unwrap(),
        original
    );
    assert!(external_lock.is_file());
    let mut producer = ProducerResolution {
        lock_path: format!("{prefix}Cargo.lock"),
        lock_sha256: crate::sha256_file(&external_lock).unwrap(),
        config_path: format!("{prefix}config.toml"),
        config_sha256: crate::sha256_file(&producer_dir.join("config.toml")).unwrap(),
        dependency_archives: Vec::new(),
    };
    assert_eq!(
        verify_producer_resolution(fixture.path(), directory, &producer)
            .unwrap()
            .len(),
        2
    );
    let relocated = Fixture::new();
    let relocated_producer = relocated.path().join(&prefix);
    fs::create_dir_all(&relocated_producer).unwrap();
    fs::copy(&external_lock, relocated_producer.join("Cargo.lock")).unwrap();
    fs::write(relocated_producer.join("config.toml"), &config).unwrap();
    assert_eq!(
        verify_producer_resolution(relocated.path(), directory, &producer).unwrap(),
        verify_producer_resolution(fixture.path(), directory, &producer).unwrap()
    );
    fs::write(relocated.path().join("Cargo.lock"), &original).unwrap();
    let relocated_config = relocated_producer.join("config.toml");
    let relocated_context = sdk_docs::rustdoc_profiles::CargoExecutionContext {
        cargo_path: Some(&cargo),
        lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(
            &relocated_config,
        )),
    };
    let relocated_metadata = sdk_docs::rustdoc_profiles::load_metadata(
        relocated.path().join("Cargo.toml"),
        relocated_context,
    )
    .unwrap();
    let metadata =
        sdk_docs::rustdoc_profiles::load_metadata(fixture.path().join("Cargo.toml"), context)
            .unwrap();
    assert_eq!(
        metadata.packages[0].name,
        relocated_metadata.packages[0].name
    );
    let rustc = cargo
        .parent()
        .unwrap()
        .join(if cfg!(windows) { "rustc.exe" } else { "rustc" });
    let host = String::from_utf8(command_output(Command::new(rustc).arg("-vV")).unwrap())
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap()
        .to_owned();
    let profile = sdk_docs::rustdoc_profiles::ProfileSpec {
        package: "historical-fixture".into(),
        target: host.clone(),
        default_features: true,
        features: BTreeSet::new(),
    };
    for (root, metadata, context) in [
        (fixture.path(), &metadata, context),
        (relocated.path(), &relocated_metadata, relocated_context),
    ] {
        sdk_docs::rustdoc_profiles::execute_profile(
            root.join("Cargo.toml"),
            metadata,
            &profile,
            &BTreeSet::from([host.clone()]),
            root.join("target"),
            root.join("receipt.json"),
            context,
            &sdk_docs::rustdoc_profiles::RustdocTarget::Library,
        )
        .unwrap();
        assert_eq!(fs::read(root.join("Cargo.lock")).unwrap(), original);
    }
    assert_eq!(
        fs::read(relocated.path().join("Cargo.lock")).unwrap(),
        original
    );
    let entry = fixture.stage_archive(&"a".repeat(40), "", false);
    let imported_lock = entry.join("Cargo.lock");
    fs::write(&imported_lock, &original).unwrap();
    let (release, archive) = fixture.pack_archive(&entry);
    let mut identity = ArchiveSource {
        package: release.package.clone(),
        version: release.num.clone(),
        registry_checksum: release.checksum.clone(),
        publisher_vcs: Some(PublisherVcs {
            revision: "a".repeat(40),
            dirty: false,
            path_in_vcs: String::new(),
        }),
        resolution_lock: ResolutionLock::SeparateDocsProducer {
            original_lock: OriginalLock::Published {
                sha256: crate::sha256_bytes(&original),
            },
            producer: producer.clone(),
        },
    };
    let captured = CapturedSource::RegistryArchives {
        archives: vec![identity.clone()],
    };
    let owners = BTreeMap::from([("historical-fixture".into(), metadata.clone())]);
    let discovered = examples(
        &metadata,
        fixture.path(),
        &BTreeSet::from(["historical-fixture".into()]),
    )
    .unwrap();
    let executed = execute_examples(
        fixture.path(),
        &cargo,
        &fixture.path().join("target"),
        &discovered,
        Some(&owners),
        Some(&captured),
    )
    .unwrap();
    assert_eq!(executed.len(), 1);
    assert_eq!(
        fs::read(fixture.path().join("Cargo.lock")).unwrap(),
        original
    );
    let (_, inputs) =
        verify_imported_archive(fixture.path(), &release, &archive, &identity).unwrap();
    assert_eq!(inputs.get(&producer.lock_path), Some(&producer.lock_sha256));
    fs::write(&imported_lock, "changed original lock").unwrap();
    assert!(verify_imported_archive(fixture.path(), &release, &archive, &identity).is_err());
    fs::write(&imported_lock, &original).unwrap();
    let ResolutionLock::SeparateDocsProducer { original_lock, .. } = &mut identity.resolution_lock
    else {
        unreachable!()
    };
    *original_lock = OriginalLock::Absent;
    assert!(verify_imported_archive(fixture.path(), &release, &archive, &identity).is_err());
    fs::write(producer_dir.join("extra.json"), "{}").unwrap();
    assert!(verify_producer_resolution(fixture.path(), directory, &producer).is_err());
    fs::remove_file(producer_dir.join("extra.json")).unwrap();
    fs::write(&external_lock, "changed producer lock").unwrap();
    assert!(verify_producer_resolution(fixture.path(), directory, &producer).is_err());
    fs::write(&external_lock, &original).unwrap();
    producer.lock_sha256 = crate::sha256_file(&external_lock).unwrap();
    fs::write(
        producer_dir.join("config.toml"),
        format!("{config}[net]\noffline=true\n"),
    )
    .unwrap();
    producer.config_sha256 = crate::sha256_file(&producer_dir.join("config.toml")).unwrap();
    assert_eq!(
        verify_producer_resolution(fixture.path(), directory, &producer).unwrap_err(),
        "producer config differs from captured lock and dependency archives"
    );
    fs::write(producer_dir.join("config.toml"), &config).unwrap();
    producer.config_sha256 = crate::sha256_file(&producer_dir.join("config.toml")).unwrap();
    producer.lock_path = "../Cargo.lock".into();
    assert!(verify_producer_resolution(fixture.path(), directory, &producer).is_err());
    assert_eq!(
        fs::read(fixture.path().join("Cargo.lock")).unwrap(),
        original
    );
    producer.lock_path = format!("{prefix}Cargo.lock");
    fs::remove_file(producer_dir.join("config.toml")).unwrap();
    assert!(verify_producer_resolution(fixture.path(), directory, &producer).is_err());
}

#[test]
fn archive_requires_checksum_vcs_cargo_identity_and_intact_sources() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata();
    let revision = "a".repeat(40);
    let (release, archive) = fixture.archive(&revision, "");
    let verify = |revision: &str, release: &RegistryVersion, archive: &Path| {
        verify_archive(&metadata, fixture.path(), revision, release, archive)
    };
    let accepted = verify(&revision, &release, &archive).unwrap();
    assert!(accepted.0.yanked);
    assert_eq!(
        accepted.1.get("src/lib.rs"),
        Some(&crate::sha256_file(&fixture.path().join("src/lib.rs")).unwrap())
    );
    fs::write(
        fixture.path().join("src/lib.rs"),
        "//! Divergent checkout Rust bytes.\n",
    )
    .unwrap();
    assert!(verify(&revision, &release, &archive)
        .unwrap_err()
        .contains("source bytes differ"));
    fs::copy(
        fixture.path().join("historical-fixture-0.1.0/src/lib.rs"),
        fixture.path().join("src/lib.rs"),
    )
    .unwrap();
    let mut wrong = release.clone();
    wrong.checksum = "b".repeat(64);
    assert!(verify(&revision, &wrong, &archive)
        .unwrap_err()
        .contains("checksum differs"));
    assert!(verify(&"c".repeat(40), &release, &archive)
        .unwrap_err()
        .contains("revision differs"));
    let manifest = fixture.path().join("Cargo.toml");
    fs::write(&manifest, "different manifest").unwrap();
    assert!(verify(&revision, &release, &archive)
        .unwrap_err()
        .contains("source bytes differ"));
    let entry = fixture.path().join("historical-fixture-0.1.0");
    fs::copy(entry.join("Cargo.toml.orig"), manifest).unwrap();
    fs::remove_file(entry.join("src/lib.rs")).unwrap();
    let (release, archive) = fixture.pack_archive(&entry);
    assert!(verify(&revision, &release, &archive)
        .unwrap_err()
        .contains("missing Cargo Rust source"));
    for (path, dirty, expected) in [("", true, "dirty source"), ("../outside", false, "escapes")] {
        let (release, archive) = fixture.archive_with_dirty(&revision, path, dirty);
        let error = verify(&revision, &release, &archive).unwrap_err();
        assert!(error.contains(expected), "{error}");
    }
    let (release, archive) = fixture.archive(&revision, "examples");
    assert!(verify(&revision, &release, &archive)
        .unwrap_err()
        .contains("differs from Cargo"));
    let mut metadata = metadata;
    metadata.packages[0].version = "0.1.1".parse().unwrap();
    assert!(
        verify_archive(&metadata, fixture.path(), &revision, &release, &archive)
            .unwrap_err()
            .contains("matching historical Cargo")
    );
    let mut invalid = release;
    invalid.num = "../0.1.0".into();
    assert!(
        verify_archive(&metadata, fixture.path(), &revision, &invalid, &archive)
            .unwrap_err()
            .contains("invalid registry version")
    );
}
#[test]
fn archive_rejects_unarchived_module_in_maintained_native_source_closure() {
    let fixture = Fixture::new();
    let package = fixture.path().join("rust/crates/historical-fixture");
    fs::create_dir_all(package.join("src")).unwrap();
    fs::create_dir_all(package.join("examples")).unwrap();
    fs::copy(
        fixture.path().join("Cargo.toml"),
        package.join("Cargo.toml"),
    )
    .unwrap();
    fs::copy(
        fixture.path().join("src/lib.rs"),
        package.join("src/lib.rs"),
    )
    .unwrap();
    fs::copy(
        fixture.path().join("examples/old-example.rs"),
        package.join("examples/old-example.rs"),
    )
    .unwrap();
    fs::write(
        package.join("src/unarchived.rs"),
        "//! Unarchived native source.\n",
    )
    .unwrap();
    let metadata = sdk_docs::rustdoc_profiles::load_metadata(
        package.join("Cargo.toml"),
        sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(&cargo()),
            lock_selection: None,
        },
    )
    .unwrap();
    let revision = "a".repeat(40);
    let (release, archive) = fixture.archive(&revision, "rust/crates/historical-fixture");
    assert!(
        verify_archive(&metadata, fixture.path(), &revision, &release, &archive)
            .unwrap_err()
            .contains("missing native docs source: src/unarchived.rs")
    );
}
#[test]
fn actual_cargo_examples_keep_required_features_execute_and_reject_drift_failure() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata();
    let owners = BTreeSet::from(["historical-fixture".into()]);
    let discovered = examples(&metadata, fixture.path(), &owners).unwrap();
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].target, "old-example");
    assert_eq!(discovered[0].required_features, vec!["old-feature"]);
    let target_dir = fixture.path().join("target");
    fs::create_dir(&target_dir).unwrap();
    let mode = target_dir.join("example-mode");
    fs::write(
        fixture.path().join("examples/old-example.rs"),
        r#"fn main() {
            match std::fs::read_to_string("target/example-mode").unwrap().as_str() {
                "success" => println!("{}", historical_fixture::value()),
                "failure" => panic!("example deliberately failed"),
                "mutate" => std::fs::write("examples/old-example.rs", "fn main() {}\n").unwrap(),
                mode => panic!("unexpected example mode: {mode}"),
            }
        }"#,
    )
    .unwrap();
    let cargo = cargo();
    let run = |examples: &[_]| {
        execute_examples(fixture.path(), &cargo, &target_dir, examples, None, None)
    };
    assert!(run(&discovered)
        .unwrap_err()
        .contains("changed before execution"));

    // Cargo's output directory is outside the source closure. Changing the
    // mode exercises all outcomes without rebuilding the example.
    let discovered = examples(&metadata, fixture.path(), &owners).unwrap();
    fs::write(&mode, "success").unwrap();
    assert_eq!(
        run(&discovered).unwrap()[0].stdout_sha256,
        crate::sha256_bytes(b"7\n")
    );
    fs::write(&mode, "failure").unwrap();
    assert!(run(&discovered)
        .unwrap_err()
        .contains("example deliberately failed"));
    fs::write(&mode, "mutate").unwrap();
    assert!(run(&discovered)
        .unwrap_err()
        .contains("changed during execution"));
    assert!(examples(
        &metadata,
        fixture.path(),
        &BTreeSet::from(["modern-absent-owner".into()])
    )
    .unwrap_err()
    .contains("absent"));
}
#[test]
fn archive_checks_embedded_binary_json_proto_and_build_configuration() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata();
    let revision = "a".repeat(40);
    let names = [
        "descriptor.bin",
        "input.json",
        "schema.proto",
        "build.rs",
        ".cargo/config.toml",
    ];
    let entry = fixture.stage_archive(&revision, "", false);
    let original = b"original archived input";
    for root in [fixture.path(), entry.as_path()] {
        for name in names {
            let path = root.join(name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, original).unwrap();
        }
    }
    let (release, archive) = fixture.pack_archive(&entry);
    let (_, files) =
        verify_archive(&metadata, fixture.path(), &revision, &release, &archive).unwrap();
    for name in names {
        assert!(files.contains_key(name));
        let path = fixture.path().join(name);
        fs::write(&path, b"changed source input").unwrap();
        let error =
            verify_archive(&metadata, fixture.path(), &revision, &release, &archive).unwrap_err();
        assert!(
            error.contains("source bytes differ") && error.ends_with(name),
            "{name}: {error}"
        );
        fs::write(path, original).unwrap();
    }
}

#[test]
fn executed_examples_reject_library_build_script_readme_manifest_and_lock_mutations() {
    let fixture = Fixture::new();
    let root = fixture.path();
    let package = root.join("rust/crates/owned");
    fs::create_dir_all(package.join("src")).unwrap();
    fs::copy(root.join("src/lib.rs"), package.join("src/lib.rs")).unwrap();
    fs::write(package.join("build.rs"), "fn main() {}\n").unwrap();
    fs::write(package.join("README.md"), "# Source README\n").unwrap();
    let manifest = root.join("Cargo.toml");
    let contents = fs::read_to_string(&manifest).unwrap().replace(
        "edition='2021'",
        "edition='2021'\nbuild='rust/crates/owned/build.rs'\nreadme='rust/crates/owned/README.md'",
    );
    fs::write(
        manifest,
        format!("{contents}\n[lib]\npath='rust/crates/owned/src/lib.rs'\n"),
    )
    .unwrap();
    fs::write(
        root.join("examples/old-example.rs"),
        r#"fn main() {
            let input = std::fs::read_to_string("target/mutation-input").unwrap();
            std::fs::write(input, "mutated documentation input").unwrap();
        }"#,
    )
    .unwrap();
    let target = root.join("target");
    fs::create_dir_all(&target).unwrap();
    let selector = target.join("mutation-input");
    let metadata = fixture.metadata();
    let examples = examples(
        &metadata,
        root,
        &BTreeSet::from(["historical-fixture".into()]),
    )
    .unwrap();
    let cargo = cargo();
    for changed in [
        "rust/crates/owned/src/lib.rs",
        "rust/crates/owned/build.rs",
        "rust/crates/owned/README.md",
        "Cargo.toml",
        "Cargo.lock",
    ] {
        let input = root.join(changed);
        let original = fs::read(&input).unwrap();
        fs::write(&selector, changed).unwrap();
        let error = execute_examples(root, &cargo, &target, &examples, None, None).unwrap_err();
        assert!(
            error.contains("source closure changed"),
            "{changed}: {error}"
        );
        fs::write(input, original).unwrap();
    }
}
