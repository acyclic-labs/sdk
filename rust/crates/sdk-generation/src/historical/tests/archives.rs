use super::*;

#[test]
fn retained_inventory_accepts_exact_flat_and_nested_members_only() {
    let fixture = Fixture::new();
    let directory = fixture.0.canonicalize().unwrap().join("inventory");
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
        let alias = fixture.0.join("inventory-alias");
        std::os::unix::fs::symlink(&directory, &alias).unwrap();
        assert!(validate_archive_inventory(&alias, &expected).is_err());
    }
}

#[test]
fn separate_producer_resolution_retains_original_lock_and_rejects_config_inventory_drift() {
    let fixture = Fixture::new();
    fs::write(
        fixture.0.join("Cargo.lock"),
        "broken original published lock\n",
    )
    .unwrap();
    let original = fs::read(fixture.0.join("Cargo.lock")).unwrap();
    let directory = "historical-fixture-0.1.0";
    let prefix = format!(".docs-producer/{directory}/");
    let producer_dir = fixture.0.join(&prefix);
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
        .arg(fixture.0.join("Cargo.toml"));
    context.configure(&mut generate).unwrap();
    command_output(&mut generate).unwrap();
    assert_eq!(fs::read(fixture.0.join("Cargo.lock")).unwrap(), original);
    assert!(external_lock.is_file());
    let mut producer = ProducerResolution {
        lock_path: format!("{prefix}Cargo.lock"),
        lock_sha256: crate::sha256_file(&external_lock).unwrap(),
        config_path: format!("{prefix}config.toml"),
        config_sha256: crate::sha256_file(&producer_dir.join("config.toml")).unwrap(),
        dependency_archives: Vec::new(),
    };
    assert_eq!(
        verify_producer_resolution(&fixture.0, directory, &producer)
            .unwrap()
            .len(),
        2
    );
    let relocated = Fixture::new();
    let relocated_producer = relocated.0.join(&prefix);
    fs::create_dir_all(&relocated_producer).unwrap();
    fs::copy(&external_lock, relocated_producer.join("Cargo.lock")).unwrap();
    fs::write(relocated_producer.join("config.toml"), &config).unwrap();
    assert_eq!(
        verify_producer_resolution(&relocated.0, directory, &producer).unwrap(),
        verify_producer_resolution(&fixture.0, directory, &producer).unwrap()
    );
    fs::write(relocated.0.join("Cargo.lock"), &original).unwrap();
    let relocated_config = relocated_producer.join("config.toml");
    let relocated_context = sdk_docs::rustdoc_profiles::CargoExecutionContext {
        cargo_path: Some(&cargo),
        lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(
            &relocated_config,
        )),
    };
    let relocated_metadata = sdk_docs::rustdoc_profiles::load_metadata(
        relocated.0.join("Cargo.toml"),
        relocated_context,
    )
    .unwrap();
    let metadata =
        sdk_docs::rustdoc_profiles::load_metadata(fixture.0.join("Cargo.toml"), context).unwrap();
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
        (&fixture.0, &metadata, context),
        (&relocated.0, &relocated_metadata, relocated_context),
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
    assert_eq!(fs::read(relocated.0.join("Cargo.lock")).unwrap(), original);
    let (mut release, archive) = fixture.archive(&"a".repeat(40), "");
    let imported_lock = fixture.0.join(directory).join("Cargo.lock");
    fs::write(&imported_lock, &original).unwrap();
    command_output(
        Command::new("tar")
            .arg("-cf")
            .arg(&archive)
            .arg("-C")
            .arg(&fixture.0)
            .arg(directory),
    )
    .unwrap();
    release.checksum = crate::sha256_file(&archive)
        .unwrap()
        .trim_start_matches("sha256:")
        .into();
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
        &fixture.0,
        &BTreeSet::from(["historical-fixture".into()]),
    )
    .unwrap();
    let executed = execute_examples(
        &fixture.0,
        &cargo,
        &fixture.0.join("target"),
        &discovered,
        Some(&owners),
        Some(&captured),
    )
    .unwrap();
    assert_eq!(executed.len(), 1);
    assert_eq!(fs::read(fixture.0.join("Cargo.lock")).unwrap(), original);
    let (_, inputs) = verify_imported_archive(&fixture.0, &release, &archive, &identity).unwrap();
    assert_eq!(inputs.get(&producer.lock_path), Some(&producer.lock_sha256));
    fs::write(&imported_lock, "changed original lock").unwrap();
    assert!(verify_imported_archive(&fixture.0, &release, &archive, &identity).is_err());
    fs::write(&imported_lock, &original).unwrap();
    let ResolutionLock::SeparateDocsProducer { original_lock, .. } = &mut identity.resolution_lock
    else {
        unreachable!()
    };
    *original_lock = OriginalLock::Absent;
    assert!(verify_imported_archive(&fixture.0, &release, &archive, &identity).is_err());
    fs::write(producer_dir.join("extra.json"), "{}").unwrap();
    assert!(verify_producer_resolution(&fixture.0, directory, &producer).is_err());
    fs::remove_file(producer_dir.join("extra.json")).unwrap();
    fs::write(&external_lock, "changed producer lock").unwrap();
    assert!(verify_producer_resolution(&fixture.0, directory, &producer).is_err());
    fs::write(&external_lock, &original).unwrap();
    producer.lock_sha256 = crate::sha256_file(&external_lock).unwrap();
    fs::write(
        producer_dir.join("config.toml"),
        format!("{config}[net]\noffline=true\n"),
    )
    .unwrap();
    producer.config_sha256 = crate::sha256_file(&producer_dir.join("config.toml")).unwrap();
    assert_eq!(
        verify_producer_resolution(&fixture.0, directory, &producer).unwrap_err(),
        "producer config differs from captured lock and dependency archives"
    );
    fs::write(producer_dir.join("config.toml"), &config).unwrap();
    producer.config_sha256 = crate::sha256_file(&producer_dir.join("config.toml")).unwrap();
    producer.lock_path = "../Cargo.lock".into();
    assert!(verify_producer_resolution(&fixture.0, directory, &producer).is_err());
    assert_eq!(fs::read(fixture.0.join("Cargo.lock")).unwrap(), original);
    producer.lock_path = format!("{prefix}Cargo.lock");
    fs::remove_file(producer_dir.join("config.toml")).unwrap();
    assert!(verify_producer_resolution(&fixture.0, directory, &producer).is_err());
}

#[test]
fn archive_requires_checksum_vcs_and_exact_cargo_identity() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata();
    let revision = "a".repeat(40);
    let (release, archive) = fixture.archive(&revision, "");
    let accepted = verify_archive(&metadata, &fixture.0, &revision, &release, &archive).unwrap();
    assert!(accepted.0.yanked);
    assert_eq!(
        accepted.1.get("src/lib.rs"),
        Some(&crate::sha256_file(&fixture.0.join("src/lib.rs")).unwrap())
    );
    fs::write(
        fixture.0.join("src/lib.rs"),
        "//! Divergent checkout Rust bytes.\n",
    )
    .unwrap();
    assert!(
        verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
            .unwrap_err()
            .contains("source bytes differ")
    );
    fs::copy(
        fixture.0.join("historical-fixture-0.1.0/src/lib.rs"),
        fixture.0.join("src/lib.rs"),
    )
    .unwrap();
    let (dirty, dirty_archive) = fixture.archive_with_dirty(&revision, "", true);
    assert!(
        verify_archive(&metadata, &fixture.0, &revision, &dirty, &dirty_archive)
            .unwrap_err()
            .contains("dirty source")
    );
    let (release, archive) = fixture.archive(&revision, "");
    let mut wrong = release.clone();
    wrong.checksum = "b".repeat(64);
    assert!(
        verify_archive(&metadata, &fixture.0, &revision, &wrong, &archive)
            .unwrap_err()
            .contains("checksum differs")
    );
    assert!(
        verify_archive(&metadata, &fixture.0, &"c".repeat(40), &release, &archive)
            .unwrap_err()
            .contains("revision differs")
    );
    let (escaping, archive) = fixture.archive(&revision, "../outside");
    assert!(
        verify_archive(&metadata, &fixture.0, &revision, &escaping, &archive)
            .unwrap_err()
            .contains("escapes")
    );
    let (release, archive) = fixture.archive(&revision, "examples");
    assert!(
        verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
            .unwrap_err()
            .contains("differs from Cargo")
    );
    let mut metadata = metadata;
    metadata.packages[0].version = "0.1.1".parse().unwrap();
    assert!(
        verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
            .unwrap_err()
            .contains("matching historical Cargo")
    );
    let mut invalid = release;
    invalid.num = "../0.1.0".into();
    assert!(
        verify_archive(&metadata, &fixture.0, &revision, &invalid, &archive)
            .unwrap_err()
            .contains("invalid registry version")
    );
}
#[test]
fn archive_rejects_missing_target_source_and_changed_original_manifest() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata();
    let revision = "a".repeat(40);
    let (mut release, archive) = fixture.archive(&revision, "");
    fs::remove_file(fixture.0.join("historical-fixture-0.1.0/src/lib.rs")).unwrap();
    command_output(
        Command::new("tar")
            .arg("-cf")
            .arg(&archive)
            .arg("-C")
            .arg(&fixture.0)
            .arg("historical-fixture-0.1.0"),
    )
    .unwrap();
    release.checksum = crate::sha256_file(&archive)
        .unwrap()
        .trim_start_matches("sha256:")
        .into();
    assert!(
        verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
            .unwrap_err()
            .contains("missing Cargo Rust source")
    );
    let (release, archive) = fixture.archive(&revision, "");
    fs::write(fixture.0.join("Cargo.toml"), "different manifest").unwrap();
    assert!(
        verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
            .unwrap_err()
            .contains("source bytes differ")
    );
}
#[test]
fn archive_rejects_unarchived_module_in_maintained_native_source_closure() {
    let fixture = Fixture::new();
    let package = fixture.0.join("rust/crates/historical-fixture");
    fs::create_dir_all(package.join("src")).unwrap();
    fs::create_dir_all(package.join("examples")).unwrap();
    fs::copy(fixture.0.join("Cargo.toml"), package.join("Cargo.toml")).unwrap();
    fs::copy(fixture.0.join("src/lib.rs"), package.join("src/lib.rs")).unwrap();
    fs::copy(
        fixture.0.join("examples/old-example.rs"),
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
        verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
            .unwrap_err()
            .contains("missing native docs source: src/unarchived.rs")
    );
}
#[test]
fn actual_cargo_examples_keep_required_features_execute_and_reject_drift_failure() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata();
    let owners = BTreeSet::from(["historical-fixture".into()]);
    let discovered = examples(&metadata, &fixture.0, &owners).unwrap();
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].target, "old-example");
    assert_eq!(discovered[0].required_features, vec!["old-feature"]);
    let execution = execute_examples(
        &fixture.0,
        &cargo(),
        &fixture.0.join("target"),
        &discovered,
        None,
        None,
    )
    .unwrap();
    assert_eq!(execution[0].stdout_sha256, crate::sha256_bytes(b"7\n"));
    fs::write(
        fixture.0.join("examples/old-example.rs"),
        "fn main() { panic!(\"failure\"); }\n",
    )
    .unwrap();
    assert!(execute_examples(
        &fixture.0,
        &cargo(),
        &fixture.0.join("target"),
        &discovered,
        None,
        None
    )
    .unwrap_err()
    .contains("changed before execution"));
    let failing = examples(&metadata, &fixture.0, &owners).unwrap();
    assert!(execute_examples(
        &fixture.0,
        &cargo(),
        &fixture.0.join("target"),
        &failing,
        None,
        None
    )
    .unwrap_err()
    .contains("failed"));
    fs::write(
        fixture.0.join("examples/old-example.rs"),
        r#"fn main() { std::fs::write("examples/old-example.rs", "fn main() {}\n").unwrap(); }"#,
    )
    .unwrap();
    let mutating = examples(&metadata, &fixture.0, &owners).unwrap();
    assert!(execute_examples(
        &fixture.0,
        &cargo(),
        &fixture.0.join("target"),
        &mutating,
        None,
        None
    )
    .unwrap_err()
    .contains("changed during execution"));
    assert!(examples(
        &metadata,
        &fixture.0,
        &BTreeSet::from(["modern-absent-owner".into()])
    )
    .unwrap_err()
    .contains("absent"));
}
#[test]
fn cargo_binary_target_produces_native_receipt_without_public_api_admission() {
    use sdk_docs::rustdoc_profiles::{
        execute_profile, observe_rustdoc, ProfileSpec, RustdocTarget,
    };
    let fixture = Fixture::new();
    fs::write(
        fixture.0.join("src/main.rs"),
        "//! Source-owned historical command instructions.\nfn main() {}\n",
    )
    .unwrap();
    let metadata = fixture.metadata();
    let rustc = cargo()
        .parent()
        .unwrap()
        .join(if cfg!(windows) { "rustc.exe" } else { "rustc" });
    let output = command_output(Command::new(rustc).arg("-vV")).unwrap();
    let info = String::from_utf8(output).unwrap();
    let host = info
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap()
        .to_owned();
    let profile = ProfileSpec {
        package: "historical-fixture".into(),
        target: host.clone(),
        default_features: true,
        features: BTreeSet::new(),
    };
    let output = fixture.0.join("binary.json");
    let target_dir = fixture.0.join("native-rustdoc");
    let absent = execute_profile(
        fixture.0.join("Cargo.toml"),
        &metadata,
        &profile,
        &BTreeSet::from([host.clone()]),
        &target_dir,
        &output,
        sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(&cargo()),
            lock_selection: None,
        },
        &RustdocTarget::Binary("absent".into()),
    );
    assert!(absent
        .unwrap_err()
        .to_string()
        .contains("absent from Cargo"));
    assert!(!target_dir.exists());
    let observation = execute_profile(
        fixture.0.join("Cargo.toml"),
        &metadata,
        &profile,
        &BTreeSet::from([host]),
        &target_dir,
        &output,
        sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(&cargo()),
            lock_selection: None,
        },
        &RustdocTarget::Binary("historical-fixture".into()),
    )
    .unwrap();
    assert_eq!(observation.format_version, 60);
    assert_eq!(observation.crate_version.as_deref(), Some("0.1.0"));
    assert!(observation.includes_private);
    assert!(observe_rustdoc(&output).is_err());
    let native: serde_json::Value = serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
    assert_eq!(
        native["index"][native["root"].to_string()]["docs"],
        "Source-owned historical command instructions."
    );
}
#[test]
fn archive_checks_embedded_binary_json_proto_and_build_configuration() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata();
    let revision = "a".repeat(40);
    for name in [
        "descriptor.bin",
        "input.json",
        "schema.proto",
        "build.rs",
        ".cargo/config.toml",
    ] {
        let path = fixture.0.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"original archived input").unwrap();
        let (mut release, archive) = fixture.archive(&revision, "");
        let archived = fixture.0.join("historical-fixture-0.1.0").join(name);
        fs::create_dir_all(archived.parent().unwrap()).unwrap();
        fs::copy(&path, &archived).unwrap();
        command_output(
            Command::new("tar")
                .arg("-cf")
                .arg(&archive)
                .arg("-C")
                .arg(&fixture.0)
                .arg("historical-fixture-0.1.0"),
        )
        .unwrap();
        release.checksum = crate::sha256_file(&archive)
            .unwrap()
            .trim_start_matches("sha256:")
            .into();
        let (_, files) =
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive).unwrap();
        assert!(files.contains_key(name));
        fs::write(&path, b"changed source input").unwrap();
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("source bytes differ")
        );
        fs::copy(&archived, &path).unwrap();
    }
}

#[test]
fn executed_examples_reject_library_build_script_readme_manifest_and_lock_mutations() {
    for changed in [
        "rust/crates/owned/src/lib.rs",
        "rust/crates/owned/build.rs",
        "rust/crates/owned/README.md",
        "Cargo.toml",
        "Cargo.lock",
    ] {
        let fixture = Fixture::new();
        fs::create_dir_all(fixture.0.join("rust/crates/owned/src")).unwrap();
        fs::copy(
            fixture.0.join("src/lib.rs"),
            fixture.0.join("rust/crates/owned/src/lib.rs"),
        )
        .unwrap();
        fs::write(
            fixture.0.join("rust/crates/owned/build.rs"),
            "fn main() {}\n",
        )
        .unwrap();
        fs::write(
            fixture.0.join("rust/crates/owned/README.md"),
            "# Source README\n",
        )
        .unwrap();
        let manifest = fs::read_to_string(fixture.0.join("Cargo.toml")).unwrap().replace("edition='2021'", "edition='2021'\nbuild='rust/crates/owned/build.rs'\nreadme='rust/crates/owned/README.md'");
        fs::write(
            fixture.0.join("Cargo.toml"),
            format!("{manifest}\n[lib]\npath='rust/crates/owned/src/lib.rs'\n"),
        )
        .unwrap();
        fs::write(fixture.0.join("examples/old-example.rs"), format!("fn main() {{ std::fs::write({changed:?}, \"mutated documentation input\").unwrap(); }}\n")).unwrap();
        let metadata = fixture.metadata();
        let examples = examples(
            &metadata,
            &fixture.0,
            &BTreeSet::from(["historical-fixture".into()]),
        )
        .unwrap();
        let error = execute_examples(
            &fixture.0,
            &cargo(),
            &fixture.0.join("target"),
            &examples,
            None,
            None,
        )
        .unwrap_err();
        assert!(
            error.contains("source closure changed"),
            "{changed}: {error}"
        );
    }
}
