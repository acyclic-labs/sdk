use super::*;

#[test]
fn actual_cargo_archived_dependency_resolution_preserves_originals_and_relocates() {
    let fixture = Fixture::new();
    let root = fixture.0.canonicalize().unwrap();
    let directory = "historical-fixture-0.1.0";
    let producer_dir = root.join(format!(".docs-producer/{directory}"));
    fs::create_dir_all(producer_dir.join("registry-archives")).unwrap();
    let dependency = Fixture::new();
    fs::write(dependency.0.join("Cargo.toml"), "[package]\nname='docs-retained-dependency'\nversion='0.1.0'\nedition='2021'\n[workspace]\n[features]\nnative=[]\n").unwrap();
    fs::write(dependency.0.join("src/lib.rs"), "/// Verified archived API marker.\npub struct ArchiveMarker;\n#[cfg(feature=\"native\")] pub fn value() -> u8 { 37 }\n").unwrap();
    fs::write(
        dependency.0.join("README.md"),
        "# Actual archived dependency\n",
    )
    .unwrap();
    fs::write(
        dependency.0.join("descriptor.bin"),
        b"original binary descriptor",
    )
    .unwrap();
    let cargo = cargo();
    command_output(
        Command::new(&cargo)
            .args([
                "package",
                "--offline",
                "--allow-dirty",
                "--no-verify",
                "--manifest-path",
            ])
            .arg(dependency.0.join("Cargo.toml"))
            .arg("--target-dir")
            .arg(dependency.0.join("target")),
    )
    .unwrap();
    let archive = producer_dir.join("registry-archives/docs-retained-dependency-0.1.0.crate");
    fs::copy(
        dependency
            .0
            .join("target/package/docs-retained-dependency-0.1.0.crate"),
        &archive,
    )
    .unwrap();
    let checksum = crate::sha256_file(&archive)
        .unwrap()
        .trim_start_matches("sha256:")
        .to_owned();
    let transport = root.join("transport");
    fs::create_dir_all(&transport).unwrap();
    fs::write(transport.join("release.json"), serde_json::to_vec(&serde_json::json!({"version":{"crate":"docs-retained-dependency","num":"0.1.0","checksum":checksum,"yanked":true}})).unwrap()).unwrap();
    // Isolate HTTP transport in the child test process, following the
    // existing CLI fixtures; actual Cargo still packages and compiles sources.
    let curl_source = transport.join("fixture-curl.rs");
    fs::write(&curl_source,"fn main() { let root=std::env::current_exe().unwrap();print!(\"{}\\n200\",std::fs::read_to_string(root.parent().unwrap().join(\"release.json\")).unwrap()); }\n").unwrap();
    let rustc = cargo
        .parent()
        .unwrap()
        .join(if cfg!(windows) { "rustc.exe" } else { "rustc" });
    command_output(
        Command::new(rustc)
            .args(["--edition=2021"])
            .arg(&curl_source)
            .arg("-o")
            .arg(transport.join(if cfg!(windows) { "curl.exe" } else { "curl" })),
    )
    .unwrap();
    let path = std::env::join_paths(
        std::iter::once(transport.clone())
            .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap();
    #[cfg(unix)]
    {
        let foreign = Fixture::new();
        let foreign = foreign.0.canonicalize().unwrap();
        fs::write(foreign.join("sentinel"), b"unchanged").unwrap();
        let before = fs::read_dir(&foreign)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect::<BTreeSet<_>>();
        let linked = producer_dir.join("dependencies");
        std::os::unix::fs::symlink(&foreign, &linked).unwrap();
        let rejected = Command::new(std::env::current_exe().unwrap())
            .args([
                "historical::tests::dependency_archive_initial_import_uses_verified_registry_facts",
                "--exact",
                "--nocapture",
            ])
            .env("SDK_DOCS_DEPENDENCY_IMPORT_ROOT", &root)
            .env("PATH", &path)
            .output()
            .unwrap();
        assert!(!rejected.status.success());
        assert!(!transport.join("import.json").exists());
        assert_eq!(fs::read(foreign.join("sentinel")).unwrap(), b"unchanged");
        assert_eq!(
            fs::read_dir(&foreign)
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect::<BTreeSet<_>>(),
            before
        );
        fs::remove_file(linked).unwrap();
    }
    let imported = Command::new(std::env::current_exe().unwrap())
        .args([
            "historical::tests::dependency_archive_initial_import_uses_verified_registry_facts",
            "--exact",
            "--nocapture",
        ])
        .env("SDK_DOCS_DEPENDENCY_IMPORT_ROOT", &root)
        .env("PATH", path)
        .output()
        .unwrap();
    assert!(
        imported.status.success(),
        "{}",
        String::from_utf8_lossy(&imported.stderr)
    );
    let (dependencies, imported_files): (Vec<ProducerDependencyArchive>, BTreeMap<String, String>) =
        serde_json::from_slice(&fs::read(transport.join("import.json")).unwrap()).unwrap();
    assert_eq!(dependencies.len(), 1);
    assert_eq!(dependencies[0].registry_checksum, checksum);
    assert!(dependencies[0].yanked);
    assert!(dependencies[0].publisher_vcs.is_none());
    assert!(imported_files
        .keys()
        .any(|p| p.ends_with("/Cargo.toml.orig")));
    assert!(imported_files
        .keys()
        .any(|p| p.ends_with("/descriptor.bin")));
    let original_manifest = "[package]\nname='historical-fixture'\nversion='0.1.0'\nedition='2021'\n[workspace]\n[features]\ndefault=[]\nold-feature=['dep-alias/native']\n[dependencies]\ndep-alias={package='docs-retained-dependency',version='=0.1.0',default-features=false,optional=true}\n[[example]]\nname='old-example'\nrequired-features=['old-feature']\n";
    fs::write(root.join("Cargo.toml"), original_manifest).unwrap();
    fs::write(
        root.join("src/lib.rs"),
        "#[cfg(feature=\"dep-alias\")]\npub use dep_alias::ArchiveMarker;\n#[cfg(feature=\"dep-alias\")]\npub fn value() -> u8 { dep_alias::value() }\n",
    )
    .unwrap();
    let original_lock = b"broken original published lock\n";
    fs::write(root.join("Cargo.lock"), original_lock).unwrap();
    let config = producer_config_bytes(directory, &dependencies).unwrap();
    assert!(config.contains("historical-fixture-0.1.0/dependencies/docs-retained-dependency-0.1.0"));
    let config_path = producer_dir.join("config.toml");
    fs::write(&config_path, &config).unwrap();
    let context = sdk_docs::rustdoc_profiles::CargoExecutionContext {
        cargo_path: Some(&cargo),
        lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(
            &config_path,
        )),
    };
    let mut generate = Command::new(&cargo);
    generate
        .args(["generate-lockfile", "--offline", "--manifest-path"])
        .arg(root.join("Cargo.toml"));
    context.configure(&mut generate).unwrap();
    command_output(&mut generate).unwrap();
    let producer = ProducerResolution {
        lock_path: format!(".docs-producer/{directory}/Cargo.lock"),
        lock_sha256: crate::sha256_file(&producer_dir.join("Cargo.lock")).unwrap(),
        config_path: format!(".docs-producer/{directory}/config.toml"),
        config_sha256: crate::sha256_file(&config_path).unwrap(),
        dependency_archives: dependencies.clone(),
    };
    let files = verify_producer_resolution(&root, directory, &producer).unwrap();
    // Default Cargo metadata legitimately omits an optional renamed dependency.
    let mut default_metadata = Command::new(&cargo);
    default_metadata
        .args([
            "metadata",
            "--offline",
            "--locked",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(root.join("Cargo.toml"));
    context.configure(&mut default_metadata).unwrap();
    let default_metadata: Metadata =
        serde_json::from_slice(&command_output(&mut default_metadata).unwrap()).unwrap();
    assert_eq!(default_metadata.resolve.as_ref().unwrap().nodes.len(), 1);
    assert!(default_metadata
        .packages
        .iter()
        .all(|package| package.name.as_ref() != "docs-retained-dependency"));
    verify_producer_metadata(
        &root,
        directory,
        &root.join("Cargo.toml"),
        &cargo,
        &config_path,
        &dependencies,
    )
    .unwrap();
    assert_eq!(fs::read(root.join("Cargo.lock")).unwrap(), original_lock);
    assert_eq!(
        fs::read_to_string(root.join("Cargo.toml")).unwrap(),
        original_manifest
    );
    let mut original = Command::new(&cargo);
    original
        .args(["metadata", "--locked", "--offline", "--manifest-path"])
        .arg(root.join("Cargo.toml"));
    sdk_docs::rustdoc_profiles::CargoExecutionContext {
        cargo_path: Some(&cargo),
        lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::SourceAdjacent(
            &root.join("Cargo.lock"),
        )),
    }
    .configure(&mut original)
    .unwrap();
    assert!(command_output(&mut original).is_err());
    let metadata =
        sdk_docs::rustdoc_profiles::load_metadata(root.join("Cargo.toml"), context).unwrap();
    let examples = examples(
        &metadata,
        &root,
        &BTreeSet::from(["historical-fixture".to_owned()]),
    )
    .unwrap();
    let captured = CapturedSource::RegistryArchives {
        archives: vec![ArchiveSource {
            package: "historical-fixture".into(),
            version: "0.1.0".into(),
            registry_checksum: "a".repeat(64),
            publisher_vcs: None,
            resolution_lock: ResolutionLock::SeparateDocsProducer {
                original_lock: OriginalLock::Published {
                    sha256: crate::sha256_bytes(original_lock),
                },
                producer: producer.clone(),
            },
        }],
    };
    let owners = BTreeMap::from([("historical-fixture".into(), metadata.clone())]);
    let executions = execute_examples(
        &root,
        &cargo,
        &root.join("target"),
        &examples,
        Some(&owners),
        Some(&captured),
    )
    .unwrap();
    assert_eq!(executions[0].stdout_sha256, crate::sha256_bytes(b"37\n"));
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
        features: BTreeSet::from(["dep-alias/native".into(), "old-feature".into()]),
    };
    sdk_docs::rustdoc_profiles::execute_profile(
        root.join("Cargo.toml"),
        &metadata,
        &profile,
        &BTreeSet::from([host.clone()]),
        root.join("target"),
        root.join("target/receipt.json"),
        context,
        &sdk_docs::rustdoc_profiles::RustdocTarget::Library,
    )
    .unwrap();
    assert!(fs::read_to_string(root.join("target/receipt.json"))
        .unwrap()
        .contains("ArchiveMarker"));
    let mut invalid_alias = profile.clone();
    invalid_alias.features = BTreeSet::from(["missing-alias/native".into()]);
    assert!(sdk_docs::rustdoc_profiles::execute_profile(
        root.join("Cargo.toml"),
        &metadata,
        &invalid_alias,
        &BTreeSet::from([host.clone()]),
        root.join("target"),
        root.join("target/invalid.json"),
        context,
        &sdk_docs::rustdoc_profiles::RustdocTarget::Library
    )
    .is_err());
    let relocated = Fixture::new();
    let relocated_root = relocated.0.canonicalize().unwrap();
    for path in files.keys() {
        crate::write_immutable(
            &relocated_root.join(path),
            &fs::read(root.join(path)).unwrap(),
        )
        .unwrap();
    }
    fs::write(relocated_root.join("Cargo.toml"), original_manifest).unwrap();
    fs::copy(root.join("src/lib.rs"), relocated_root.join("src/lib.rs")).unwrap();
    fs::write(relocated_root.join("Cargo.lock"), original_lock).unwrap();
    let relocated_config = relocated_root.join(&producer.config_path);
    assert_eq!(
        verify_producer_resolution(&relocated_root, directory, &producer).unwrap(),
        files
    );
    verify_producer_metadata(
        &relocated_root,
        directory,
        &relocated_root.join("Cargo.toml"),
        &cargo,
        &relocated_config,
        &dependencies,
    )
    .unwrap();
    let relocated_context = sdk_docs::rustdoc_profiles::CargoExecutionContext {
        cargo_path: Some(&cargo),
        lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(
            &relocated_config,
        )),
    };
    let relocated_metadata = sdk_docs::rustdoc_profiles::load_metadata(
        relocated_root.join("Cargo.toml"),
        relocated_context,
    )
    .unwrap();
    sdk_docs::rustdoc_profiles::execute_profile(
        relocated_root.join("Cargo.toml"),
        &relocated_metadata,
        &profile,
        &BTreeSet::from([host]),
        relocated_root.join("target"),
        relocated_root.join("target/receipt.json"),
        relocated_context,
        &sdk_docs::rustdoc_profiles::RustdocTarget::Library,
    )
    .unwrap();
    assert!(
        fs::read_to_string(relocated_root.join("target/receipt.json"))
            .unwrap()
            .contains("ArchiveMarker")
    );
    let mut relocated_capture = captured.clone();
    let CapturedSource::RegistryArchives { archives } = &mut relocated_capture else {
        unreachable!()
    };
    let ResolutionLock::SeparateDocsProducer {
        producer: relocated_producer,
        ..
    } = &mut archives[0].resolution_lock
    else {
        unreachable!()
    };
    relocated_producer.lock_sha256 =
        crate::sha256_file(&relocated_root.join(&producer.lock_path)).unwrap();
    relocated_producer.config_sha256 = crate::sha256_file(&relocated_config).unwrap();
    assert_eq!(
        captured.revision().unwrap(),
        relocated_capture.revision().unwrap()
    );
    let wrong_template = config.replace(&format!("{directory}/dependencies/"), "dependencies/");
    fs::write(&config_path, &wrong_template).unwrap();
    let mut resealed = producer.clone();
    resealed.config_sha256 = crate::sha256_file(&config_path).unwrap();
    assert!(verify_producer_resolution(&root, directory, &resealed).is_err());
    fs::write(&config_path, &config).unwrap();
    for member in [
        "src/lib.rs",
        "README.md",
        "descriptor.bin",
        "Cargo.lock",
        "Cargo.toml.orig",
    ] {
        let path = producer_dir.join(format!(
            "dependencies/docs-retained-dependency-0.1.0/{member}"
        ));
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, b"changed imported dependency input").unwrap();
        assert!(
            verify_producer_resolution(&root, directory, &producer).is_err(),
            "{member}"
        );
        fs::write(&path, bytes).unwrap();
    }
    let bytes = fs::read(&archive).unwrap();
    fs::write(&archive, b"wrong dependency archive checksum").unwrap();
    assert!(verify_producer_resolution(&root, directory, &producer).is_err());
    fs::write(&archive, bytes).unwrap();
    let extra = producer_dir.join("dependencies/docs-retained-dependency-0.1.0/unverified.json");
    fs::write(&extra, "{}").unwrap();
    assert!(verify_producer_resolution(&root, directory, &producer).is_err());
    fs::remove_file(extra).unwrap();
    let omitted = producer_dir.join("dependencies/docs-retained-dependency-0.1.0/src/lib.rs");
    let bytes = fs::read(&omitted).unwrap();
    fs::remove_file(&omitted).unwrap();
    assert!(verify_producer_resolution(&root, directory, &producer).is_err());
    assert!(
        !omitted.exists(),
        "verification must not restore missing authoritative inputs"
    );
    fs::write(omitted, bytes).unwrap();
    let mut wrong_identity = producer.clone();
    wrong_identity.dependency_archives[0].publisher_vcs = Some(PublisherVcs {
        revision: "b".repeat(40),
        dirty: true,
        path_in_vcs: "src".into(),
    });
    assert!(verify_producer_resolution(&root, directory, &wrong_identity).is_err());
    wrong_identity.dependency_archives[0] = dependencies[0].clone();
    wrong_identity
        .dependency_archives
        .push(dependencies[0].clone());
    assert!(verify_producer_resolution(&root, directory, &wrong_identity).is_err());
    #[cfg(unix)]
    {
        let original = producer_dir.join("dependencies/docs-retained-dependency-0.1.0");
        let redirected = producer_dir.join("redirected-dependency");
        fs::rename(&original, &redirected).unwrap();
        std::os::unix::fs::symlink(&redirected, &original).unwrap();
        assert!(verify_producer_resolution(&root, directory, &producer).is_err());
        fs::remove_file(original).unwrap();
        fs::rename(
            redirected,
            producer_dir.join("dependencies/docs-retained-dependency-0.1.0"),
        )
        .unwrap();
    }
    assert_eq!(
        verify_producer_resolution(&root, directory, &producer).unwrap(),
        files
    );
    // A genuine Cargo-produced package recorded as a patch but unused by the
    // owner must fail the actual metadata selection check.
    let unused = Fixture::new();
    fs::write(
        unused.0.join("Cargo.toml"),
        "[package]\nname='docs-unused-dependency'\nversion='0.1.0'\nedition='2021'\n[workspace]\n",
    )
    .unwrap();
    command_output(
        Command::new(&cargo)
            .args([
                "package",
                "--offline",
                "--allow-dirty",
                "--no-verify",
                "--manifest-path",
            ])
            .arg(unused.0.join("Cargo.toml"))
            .arg("--target-dir")
            .arg(unused.0.join("target")),
    )
    .unwrap();
    let unused_archive = producer_dir.join("registry-archives/docs-unused-dependency-0.1.0.crate");
    fs::copy(
        unused
            .0
            .join("target/package/docs-unused-dependency-0.1.0.crate"),
        &unused_archive,
    )
    .unwrap();
    command_output(
        Command::new("tar")
            .arg("-xf")
            .arg(&unused_archive)
            .arg("-C")
            .arg(producer_dir.join("dependencies")),
    )
    .unwrap();
    let mut unused_records = dependencies.clone();
    unused_records.push(ProducerDependencyArchive {
        package: "docs-unused-dependency".into(),
        version: "0.1.0".into(),
        registry_checksum: crate::sha256_file(&unused_archive)
            .unwrap()
            .trim_start_matches("sha256:")
            .to_owned(),
        publisher_vcs: None,
        yanked: false,
    });
    fs::write(
        &config_path,
        producer_config_bytes(directory, &unused_records).unwrap(),
    )
    .unwrap();
    let mut generate = Command::new(&cargo);
    generate
        .args(["generate-lockfile", "--offline", "--manifest-path"])
        .arg(root.join("Cargo.toml"));
    context.configure(&mut generate).unwrap();
    command_output(&mut generate).unwrap();
    assert!(verify_producer_metadata(
        &root,
        directory,
        &root.join("Cargo.toml"),
        &cargo,
        &config_path,
        &unused_records
    )
    .is_err());
    // Replay must independently reject a coherently resealed unused patch record.
    let mut resealed = producer.clone();
    resealed.dependency_archives = unused_records.clone();
    resealed.config_sha256 = crate::sha256_file(&config_path).unwrap();
    resealed.lock_sha256 = crate::sha256_file(&producer_dir.join("Cargo.lock")).unwrap();
    let replay_files = verify_producer_resolution(&root, directory, &resealed).unwrap();
    let replay = Fixture::new();
    let replay_root = replay.0.canonicalize().unwrap();
    for path in replay_files.keys() {
        let destination = replay_root.join(path);
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::copy(root.join(path), destination).unwrap();
    }
    for path in [
        "Cargo.toml",
        "Cargo.lock",
        "src/lib.rs",
        "examples/old-example.rs",
    ] {
        let destination = replay_root.join(directory).join(path);
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::copy(root.join(path), destination).unwrap();
    }
    let mut replay_identity = captured.clone();
    let CapturedSource::RegistryArchives { archives } = &mut replay_identity else {
        panic!("archive fixture")
    };
    let ResolutionLock::SeparateDocsProducer { producer, .. } = &mut archives[0].resolution_lock
    else {
        panic!("producer fixture")
    };
    *producer = resealed;
    assert!(imported_metadata(&replay_root, &replay_identity, &cargo)
        .unwrap_err()
        .contains("Cargo did not select captured dependency root"));
    let foreign = Fixture::new();
    fs::write(
        foreign.0.join("Cargo.toml"),
        "[package]\nname='docs-foreign-dependency'\nversion='0.1.0'\nedition='2021'\n[workspace]\n",
    )
    .unwrap();
    let path = serde_json::to_string(foreign.0.to_str().unwrap()).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        original_manifest.replace(
            "[dependencies]\n",
            &format!(
                "[dependencies]\nforeign-extra={{package='docs-foreign-dependency',path={path}}}\n"
            ),
        ),
    )
    .unwrap();
    fs::write(&config_path, &config).unwrap();
    let mut generate = Command::new(&cargo);
    generate
        .args(["generate-lockfile", "--offline", "--manifest-path"])
        .arg(root.join("Cargo.toml"));
    context.configure(&mut generate).unwrap();
    command_output(&mut generate).unwrap();
    assert!(verify_producer_metadata(
        &root,
        directory,
        &root.join("Cargo.toml"),
        &cargo,
        &config_path,
        &dependencies
    )
    .unwrap_err()
    .contains("uncaptured path source"));
}
