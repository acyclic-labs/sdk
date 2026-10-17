use super::*;

#[test]
fn registry_published_and_missing_locks_ignore_ambient_lock_selection() {
    runtime::time_control();
    std::thread::scope(|scope| {
        for published in [true, false] {
            scope.spawn(move || {
                let (fixture, support) = registry_archive_fixture(false, published, None, true);
                let foreign = support.join("foreign/Cargo.lock");
                fs::create_dir_all(foreign.parent().unwrap()).unwrap();
                fs::write(&foreign, "invalid foreign lock must never be selected\n").unwrap();
                let archive = support.join("registry/demo-1.0.0.crate");
                let original_archive = fs::read(&archive).unwrap();
                let result = run_historical_generate_mode_with_env(
                    &fixture,
                    &support,
                    Some("registry-archives"),
                    Some(&foreign),
                );
                assert!(result.status.success(), "{}", output_message(&result));
                assert_eq!(
                    fs::read(&foreign).unwrap(),
                    b"invalid foreign lock must never be selected\n"
                );
                assert_eq!(fs::read(&archive).unwrap(), original_archive);
                let data: Value = serde_json::from_slice(
                    &fs::read(fixture.output.join("releases/1.0.0/sdk-docs-data.v1.json")).unwrap(),
                )
                .unwrap();
                assert_eq!(
                    data["source"]["capturedSource"]["archives"][0]["resolutionLock"]["kind"],
                    if published {
                        "published"
                    } else {
                        "docsProducer"
                    }
                );
                let _ = fs::remove_dir_all(fixture.root);
                let _ = fs::remove_dir_all(fixture.output);
            });
        }
    });
}

#[test]
fn archive_locks_override_foreign_automatic_cargo_configuration() {
    runtime::time_control();
    std::thread::scope(|scope| {
        for published in [true, false] {
            scope.spawn(move || {
                let (fixture, support) = registry_archive_fixture(false, published, None, true);
                // These controls differ only in automatic-config location. Keep the
                // captured source and compiler output immutable between the two runs.
                let mut discovery = Vec::new();
                let queries = commands::Queries::default();
                for cargo_home in [true, false] {
                    let directory = support.join("automatic-config");
                    let config_directory = if cargo_home {
                        directory.clone()
                    } else {
                        directory.join(".cargo")
                    };
                    fs::create_dir_all(&config_directory).unwrap();
                    let foreign = support.join("foreign-automatic/Cargo.lock");
                    fs::create_dir_all(foreign.parent().unwrap()).unwrap();
                    let foreign = foreign
                        .parent()
                        .unwrap()
                        .canonicalize()
                        .unwrap()
                        .join("Cargo.lock");
                    let foreign_bytes = b"invalid foreign automatic lock must not be selected\n";
                    fs::write(&foreign, foreign_bytes).unwrap();
                    let config = format!(
                        "[resolver]\nlockfile-path = {}\n",
                        serde_json::to_string(foreign.to_str().unwrap()).unwrap()
                    );
                    fs::write(config_directory.join("config.toml"), &config).unwrap();
                    // Negative control: maintained Cargo really discovers this automatic config.
                    let mut control = Command::new(&*runtime::cargo());
                    control.env("CARGO_HOME", fixture_cargo_home(&fixture));
                    control
                        .args([
                            "metadata",
                            "--locked",
                            "--offline",
                            "--format-version",
                            "1",
                            "--manifest-path",
                        ])
                        .arg(support.join("cargo-packaged-source/demo-1.0.0/Cargo.toml"))
                        .env_remove("CARGO_RESOLVER_LOCKFILE_PATH");
                    if cargo_home {
                        control.env("CARGO_HOME", &directory);
                    } else {
                        control.current_dir(&directory);
                    }
                    let rejected = control.output().unwrap();
                    assert!(
                        !rejected.status.success(),
                        "automatic config negative control did not select foreign lock"
                    );
                    assert!(
                        output_message(&rejected).contains("lock"),
                        "{}",
                        output_message(&rejected)
                    );
                    let archive = support.join("registry/demo-1.0.0.crate");
                    let original_archive = fs::read(&archive).unwrap();
                    let result = queries.run(|| {
                        run_historical_generate_mode_with_authority(
                            &fixture,
                            &support,
                            Some("registry-archives"),
                            None,
                            Some((&directory, cargo_home)),
                        )
                    });
                    assert!(result.status.success(), "{}", output_message(&result));
                    assert_eq!(fs::read(&foreign).unwrap(), foreign_bytes);
                    assert_eq!(fs::read(&archive).unwrap(), original_archive);
                    assert_eq!(
                        fs::read_to_string(config_directory.join("config.toml")).unwrap(),
                        config
                    );
                    let data: Value = serde_json::from_slice(
                        &fs::read(fixture.output.join("releases/1.0.0/sdk-docs-data.v1.json"))
                            .unwrap(),
                    )
                    .unwrap();
                    assert_eq!(
                        data["source"]["capturedSource"]["archives"][0]["resolutionLock"]["kind"],
                        if published {
                            "published"
                        } else {
                            "docsProducer"
                        }
                    );
                    let output = fixture.output.join("releases/1.0.0");
                    let manifest: Value = serde_json::from_slice(
                        &fs::read(output.join("generation-manifest.v1.json")).unwrap(),
                    )
                    .unwrap();
                    discovery.push(manifest["registryCorpus"]["frontier"].clone());
                    // Each case gets a new admission: its discovery context differs.
                    // Retain the exact source, receipts and compiler directory only.
                    fs::remove_file(output.join("generation-manifest.v1.json")).unwrap();
                    fs::remove_file(output.join("generation-manifest.v1.sha256")).unwrap();
                    fs::remove_file(config_directory.join("config.toml")).unwrap();
                }
                assert_eq!(discovery.len(), 2);
                let (recorded, replayed_compilers) = queries.compiler_counts();
                assert!(
                    recorded > 0,
                    "the original compiler matrix must really execute"
                );
                assert_eq!(
                    recorded, replayed_compilers,
                    "each identical policy profile must replay its real JSON"
                );
                let (executed, replayed) = queries.counts();
                assert!(
                    executed > 0 && replayed > 0,
                    "both real recording and policy replay must run"
                );
                assert_ne!(
                    discovery[0], discovery[1],
                    "both automatic-config authorities must execute"
                );
                let _ = fs::remove_dir_all(fixture.root);
                let _ = fs::remove_dir_all(fixture.output);
            });
        }
    });
}

#[test]
fn imported_initial_extras_and_profile_added_files_cannot_admit_catalog() {
    runtime::time_control();
    fn add_input_during_profile(package: &Path) {
        let manifest = fs::read_to_string(package.join("Cargo.toml")).unwrap();
        fs::write(
            package.join("Cargo.toml"),
            manifest.replace(
                "include = [",
                "build = \"build.rs\"\ninclude = [\"build.rs\", ",
            ),
        )
        .unwrap();
        fs::write(package.join("build.rs"), "fn main() { std::fs::write(std::path::Path::new(&std::env::var(\"CARGO_MANIFEST_DIR\").unwrap()).join(\"unverified-added.json\"), \"{}\").unwrap(); }\n").unwrap();
    }
    std::thread::scope(|scope| {
        for during_profile in [false, true] {
            scope.spawn(move || {
                let (fixture, support) = registry_archive_fixture_configured(
                    false,
                    true,
                    None,
                    true,
                    "demo",
                    if during_profile {
                        Some(add_input_during_profile)
                    } else {
                        None
                    },
                    None,
                );
                if !during_profile {
                    let unknown = fixture
                        .output
                        .join("releases/1.0.0/sources/.docs-producer/unknown-owner/config.toml");
                    fs::create_dir_all(unknown.parent().unwrap()).unwrap();
                    fs::write(unknown, "unknown producer input").unwrap();
                }
                let result =
                    run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
                assert!(!result.status.success());
                assert!(
                    output_message(&result).contains(if during_profile {
                        "source closure changed during qualification before manifest admission"
                    } else {
                        "initial imported source closure differs from verified archive inputs"
                    }),
                    "{}",
                    output_message(&result)
                );
                assert!(!fixture.output.join("sdk-docs-versions.v1.json").exists());
                if during_profile {
                    assert!(fixture
                        .output
                        .join("releases/1.0.0/sources/demo-1.0.0/unverified-added.json")
                        .exists());
                }
                let _ = fs::remove_dir_all(fixture.root);
                let _ = fs::remove_dir_all(fixture.output);
            });
        }
    });
}

#[test]
fn archive_feature_profiles_bind_all_generated_aliases_and_keep_required_failures_fatal() {
    runtime::time_control();
    fn setup(package: &Path) {
        let manifest = fs::read_to_string(package.join("Cargo.toml")).unwrap();
        fs::write(
            package.join("Cargo.toml"),
            manifest.replace(
                "include = [",
                "build = \"build.rs\"\ninclude = [\"build.rs\", ",
            ),
        )
        .unwrap();
        fs::write(package.join("src/lib.rs"), "#[cfg(not(feature=\"base\"))]\ncompile_error!(\"original package requires base\");\ninclude!(concat!(env!(\"OUT_DIR\"), \"/wire.rs\"));\n").unwrap();
        fs::write(package.join("build.rs"), "fn main() { let name = if std::env::var_os(\"CARGO_FEATURE_EXTRA\").is_some() { \"ExtraWire\" } else { \"DefaultWire\" }; std::fs::write(std::path::PathBuf::from(std::env::var(\"OUT_DIR\").unwrap()).join(\"wire.rs\"), format!(\"/// Actual feature-generated API.\\npub struct {name};\\n\")).unwrap(); }\n").unwrap();
    }
    fn required_no_defaults(package: &Path) {
        setup(package);
        let manifest = fs::read_to_string(package.join("Cargo.toml")).unwrap();
        fs::write(package.join("Cargo.toml"), format!("{manifest}\n[package.metadata.docs.rs]\nno-default-features=true\nfeatures=['extra']\n")).unwrap();
    }
    std::thread::scope(|scope| {
        for required_failure in [false, true] {
            scope.spawn(move || {
                let (fixture, support) = registry_archive_fixture_configured(
                    false,
                    true,
                    None,
                    true,
                    "demo",
                    Some(if required_failure {
                        required_no_defaults
                    } else {
                        setup
                    }),
                    None,
                );
                let result =
                    run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
                if required_failure {
                    assert!(!result.status.success());
                    assert!(output_message(&result).contains("original package requires base"));
                    assert!(
                        !fixture.output.join("sdk-docs-versions.v1.json").exists(),
                        "required upstream profile failure cannot admit the version"
                    );
                } else {
                    assert!(result.status.success(), "{}", output_message(&result));
                    let output = fixture.output.join("releases/1.0.0");
                    let profiles: Value = serde_json::from_slice(
                        &fs::read(output.join("sdk-docs-rustdoc-profiles.v1.json")).unwrap(),
                    )
                    .unwrap();
                    assert!(profiles
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|profile| profile["defaultFeatures"] == true));
                    let data: Value = serde_json::from_slice(
                        &fs::read(output.join("sdk-docs-data.v1.json")).unwrap(),
                    )
                    .unwrap();
                    for name in ["DefaultWire", "ExtraWire"] {
                        let item = data["families"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .flat_map(|family| family["items"].as_array().unwrap())
                            .find(|item| item["name"] == name)
                            .unwrap();
                        let path = item["source"]["path"].as_str().unwrap();
                        assert!(path.starts_with("releases/1.0.0/generated/rustdoc/"));
                        assert!(fs::read_to_string(fixture.output.join(path))
                            .unwrap()
                            .contains(name));
                    }
                    let aliases: Value = serde_json::from_slice(
                        &fs::read(output.join(
                            "sources/target/sdk-generation-generated-sources/retained/aliases.json",
                        ))
                        .unwrap(),
                    )
                    .unwrap();
                    assert!(
                        aliases
                            .as_object()
                            .unwrap()
                            .keys()
                            .filter(|path| path.ends_with("wire.rs"))
                            .count()
                            >= 2,
                        "actual Cargo profiles must use distinct OUT_DIR aliases"
                    );
                    let drift = run_historical_drift(
                        &Fixture {
                            root: fixture.root.clone(),
                            rustdoc: output.join(".rustdoc"),
                            output,
                        },
                        &support,
                    );
                    assert!(drift.status.success(), "{}", output_message(&drift));
                }
                let _ = fs::remove_dir_all(fixture.root);
                let _ = fs::remove_dir_all(fixture.output);
            });
        }
    });
}

#[test]
fn three_archive_owners_bind_identical_relative_spans_to_their_own_source() {
    runtime::time_control();
    fn owner_example(package: &Path) {
        fs::write(
            package.join("examples/historical.rs"),
            "fn main() { println!(\"archive-example-executed:{}\", env!(\"CARGO_PKG_NAME\")); }\n",
        )
        .unwrap();
    }
    // One immutable initial history prevents incidental wall-clock differences
    // from creating redundant source frontiers. Each owner still packages and
    // compiles its own source and complete four-profile historical matrix.
    let template = historical_fixture(None);
    let base_revision = git_revision(&template.0.root);
    let fixtures = std::thread::scope(|scope| {
        let template = &template;
        let jobs = ["demo-a", "demo-b", "demo-c"].map(|name| {
            scope.spawn(move || {
                registry_archive_fixture_configured(
                    false,
                    true,
                    None,
                    true,
                    name,
                    Some(owner_example),
                    Some(super::replay::copy_historical_fixture(template)),
                )
            })
        });
        jobs.map(|job| job.join().unwrap())
    });
    let (fixture, support) = &fixtures[0];
    runtime::phase("fixtures");
    let mut anchors = vec![git_revision(&fixture.root)];
    for (index, (other_fixture, other)) in fixtures.iter().enumerate().skip(1) {
        let name = ["demo-a", "demo-b", "demo-c"][index];
        fs::copy(
            other.join(format!("registry/{name}-1.0.0.crate")),
            support.join(format!("registry/{name}-1.0.0.crate")),
        )
        .unwrap();
        let revision = git_revision(&other_fixture.root);
        // Share immutable objects, then let real Git verify the ancestry below.
        super::replay::copy_git_objects(
            &other_fixture.root.join(".git/objects"),
            &fixture.root.join(".git/objects"),
        );
        git(
            &fixture.root,
            &["merge-base", "--is-ancestor", &base_revision, &revision],
        );
        anchors.push(revision);
    }
    fs::write(
        support.join("registry-frontier-revisions"),
        anchors.join(","),
    )
    .unwrap();
    write_registry_transport(support);
    runtime::phase("anchors");
    let queries = commands::Queries::default();
    let generated =
        queries.run(|| run_historical_generate_mode(fixture, support, Some("registry-archives")));
    runtime::phase("generated");
    assert!(generated.status.success(), "{}", output_message(&generated));
    let output = fixture.output.join("releases/1.0.0");
    assert_executed_profiles(
        &output,
        &output.join(".rustdoc"),
        &[("demo-a", 4), ("demo-b", 4), ("demo-c", 4)],
    );
    let executions: Value = serde_json::from_slice(
        &fs::read(output.join("sdk-docs-scenario-executions.v1.json")).unwrap(),
    )
    .unwrap();
    let executions = executions.as_array().unwrap();
    assert_eq!(executions.len(), 3);
    for (row, owner) in executions.iter().zip(["demo-a", "demo-b", "demo-c"]) {
        assert_eq!(row["example"]["package"], owner);
        assert_eq!(
            row["stdoutSha256"],
            hash_bytes(format!("archive-example-executed:{owner}\n").as_bytes())
        );
        assert_eq!(row["stderrSha256"], hash_bytes(b""));
    }
    let data: Value =
        serde_json::from_slice(&fs::read(output.join("sdk-docs-data.v1.json")).unwrap()).unwrap();
    assert_eq!(
        data["source"]["releasedPackages"].as_array().unwrap().len(),
        3
    );
    for (family, package) in data["families"]
        .as_array()
        .unwrap()
        .iter()
        .zip(["demo-a", "demo-b", "demo-c"])
    {
        let item = family["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["name"] == "ArchiveOnly")
            .unwrap();
        assert_eq!(
            item["source"]["path"],
            format!("releases/1.0.0/sources/{package}-1.0.0/src/lib.rs")
        );
        assert!(fixture
            .output
            .join(item["source"]["path"].as_str().unwrap())
            .is_file());
    }
    let drift = queries.run(|| {
        run_historical_drift(
            &Fixture {
                root: fixture.root.clone(),
                rustdoc: output.join(".rustdoc"),
                output: output.clone(),
            },
            support,
        )
    });
    assert!(drift.status.success(), "{}", output_message(&drift));
    runtime::phase("drifted");
    fs::remove_dir_all(&template.0.root).unwrap();
    fs::remove_dir_all(&template.0.output).unwrap();
    for (fixture, _) in fixtures {
        let _ = fs::remove_dir_all(fixture.root);
        let _ = fs::remove_dir_all(fixture.output);
    }
    runtime::phase("cleaned");
}

#[test]
fn registry_archive_native_source_is_not_a_publisher_git_retag() {
    runtime::time_control();
    std::thread::scope(|scope| {
        for (dirty, published_lock, publisher_vcs) in [
            (true, true, true),
            (false, false, true),
            (false, true, false),
        ] {
            scope.spawn(move || {
                let (fixture, support) =
                    registry_archive_fixture(dirty, published_lock, None, publisher_vcs);
                let publisher = git_revision(&fixture.root);
                let result =
                    run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
                assert!(result.status.success(), "{}", output_message(&result));
                let output = fixture.output.join("releases/1.0.0");
                let data: Value = serde_json::from_slice(
                    &fs::read(output.join("sdk-docs-data.v1.json")).unwrap(),
                )
                .unwrap();
                assert_eq!(data["source"]["capturedSource"]["kind"], "registryArchives");
                assert_ne!(data["source"]["revision"], publisher);
                assert_eq!(data["source"]["sourceState"], "registry-archives");
                let archive = &data["source"]["capturedSource"]["archives"][0];
                if publisher_vcs {
                    assert_eq!(archive["publisherVcs"]["revision"], publisher);
                    assert_eq!(archive["publisherVcs"]["dirty"], dirty);
                } else {
                    assert!(archive["publisherVcs"].is_null());
                    assert_eq!(data["source"]["releasedPackages"][0]["sourceRevision"], "");
                    assert_eq!(data["source"]["releasedPackages"][0]["pathInVcs"], "");
                }
                assert_eq!(
                    archive["resolutionLock"]["kind"],
                    if published_lock {
                        "published"
                    } else {
                        "docsProducer"
                    }
                );
                let item = data["families"][0]["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|item| item["name"] == "ArchiveOnly")
                    .unwrap();
                let source_path = item["source"]["path"].as_str().unwrap();
                assert!(source_path.starts_with("releases/1.0.0/sources/demo-1.0.0/"));
                assert!(fixture.output.join(source_path).exists());
                if !published_lock {
                    let lock = output.join("sources/demo-1.0.0/Cargo.lock");
                    let captured_lock = fs::read(&lock).unwrap();
                    let repeated =
                        run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
                    assert!(repeated.status.success(), "{}", output_message(&repeated));
                    assert_eq!(fs::read(&lock).unwrap(), captured_lock);
                }
                let imported = Fixture {
                    root: fixture.root.clone(),
                    rustdoc: output.join(".rustdoc"),
                    output: output.clone(),
                };
                for path in [
                    "Cargo.lock",
                    "Cargo.toml",
                    "descriptor.bin",
                    "input.json",
                    "schema.proto",
                ] {
                    let source = output.join("sources/demo-1.0.0").join(path);
                    let original = fs::read(&source).unwrap();
                    fs::write(&source, b"tampered imported source").unwrap();
                    let drift = run_historical_drift(&imported, &support);
                    assert!(!drift.status.success(), "{path} drift was accepted");
                    fs::write(&source, original).unwrap();
                }
                fs::write(output.join("sources/demo-1.0.0/stale-extra.bin"), b"stale").unwrap();
                assert!(!run_historical_drift(&imported, &support).status.success());
                let _ = fs::remove_dir_all(fixture.root);
                let _ = fs::remove_dir_all(fixture.output);
            });
        }
    });
}

#[test]
fn covered_publisher_history_keeps_expanded_anchors_and_rejects_export_drift() {
    runtime::time_control();
    let (fixture, support) = registry_archive_fixture(true, true, None, true);
    let publisher = git_revision(&fixture.root);
    git(
        &fixture.root,
        &[
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "newer declared history",
        ],
    );
    let declared = git_revision(&fixture.root);
    assert_ne!(publisher, declared);
    let generated = run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
    assert!(generated.status.success(), "{}", output_message(&generated));
    let output = fixture.output.join("releases/1.0.0");
    let manifest: Value =
        serde_json::from_slice(&fs::read(output.join("generation-manifest.v1.json")).unwrap())
            .unwrap();
    let corpus = &manifest["registryCorpus"];
    assert_eq!(corpus["declared_anchors"], json!([declared]));
    let anchors = corpus["anchors"].as_array().unwrap();
    assert_eq!(anchors.len(), 2);
    assert!(anchors.contains(&json!(publisher)));
    assert!(anchors.contains(&json!(declared)));
    let imported = Fixture {
        root: fixture.root.clone(),
        rustdoc: output.join(".rustdoc"),
        output: output.clone(),
    };
    let changed_context = run_drift(&imported);
    assert!(!changed_context.status.success());
    assert!(
        output_message(&changed_context)
            .contains("historical source frontier or discovery context changed"),
        "{}",
        output_message(&changed_context)
    );
    let drift = run_historical_drift(&imported, &support);
    assert!(drift.status.success(), "{}", output_message(&drift));
    fs::write(
        output
            .join("registry-frontier")
            .join(&publisher)
            .join("rust/crates/demo/src/lib.rs"),
        b"tampered original publisher export",
    )
    .unwrap();
    let drift = run_historical_drift(&imported, &support);
    assert!(
        !drift.status.success(),
        "changed original publisher export was admitted"
    );
    let _ = fs::remove_dir_all(fixture.root);
    let _ = fs::remove_dir_all(fixture.output);
}

#[test]
fn historical_source_help_and_flag_contract_are_explicit() {
    runtime::time_control();
    let binary = runtime::binary("sdk-generation");
    let help = Command::new(&binary).arg("help").output().unwrap();
    assert!(help.status.success());
    let direct = invoke(Command::new(&binary).arg("help"));
    assert_eq!(direct.status.code(), help.status.code());
    assert_eq!(direct.stdout, help.stdout);
    assert_eq!(direct.stderr, help.stderr);
    let text = output_message(&help);
    assert!(text.contains("--historical-archives DIR"));
    assert!(text.contains("--historical-source git|registry-archives"));
    let missing_frontier = invoke(Command::new(&binary).args([
        "generate",
        "--root",
        "missing-root",
        "--output",
        "missing-output",
        "--version",
        "1.0.0",
        "--execute-profiles",
        "--channel",
        "release",
        "--historical-source",
        "registry-archives",
        "--historical-archives",
        "missing-archives",
    ]));
    assert!(!missing_frontier.status.success());
    assert!(output_message(&missing_frontier).contains("requires a complete registry corpus"));
    for (mode, expected) in [
        ("registry-archives", "requires --historical-archives"),
        ("unverified", "unknown historical source"),
    ] {
        let rejected = invoke(Command::new(&binary).args([
            "generate",
            "--root",
            "missing",
            "--output",
            "missing",
            "--version",
            "1.0.0",
            "--execute-profiles",
            "--channel",
            "release",
            "--historical-source",
            mode,
        ]));
        assert!(!rejected.status.success());
        assert!(output_message(&rejected).contains(expected));
    }
}

#[test]
fn archive_example_mutation_cannot_admit_its_native_release_catalog() {
    runtime::time_control();
    let (fixture, support) = registry_archive_fixture(true, true, Some("input.json"), true);
    let result = run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
    assert!(!result.status.success());
    assert!(
        output_message(&result).contains("source closure changed"),
        "{}",
        output_message(&result)
    );
    assert_eq!(
        fs::read(
            fixture
                .output
                .join("releases/1.0.0/sources/demo-1.0.0/input.json")
        )
        .unwrap(),
        b"changed imported input"
    );
    assert!(!fixture.output.join("sdk-docs-versions.v1.json").exists());
    let _ = fs::remove_dir_all(fixture.root);
    let _ = fs::remove_dir_all(fixture.output);
}

#[test]
fn historical_native_pipeline_admits_only_executed_source_owned_registry_docs() {
    runtime::time_control();
    let (fixture, support) = historical_fixture(None);
    let result = run_historical_generate(&fixture, &support);
    assert!(result.status.success(), "{}", output_message(&result));
    let version = fixture.output.join("releases/1.0.0");
    assert_executed_profiles(&version, &version.join(".rustdoc"), &[("demo", 4)]);
    let data: Value =
        serde_json::from_slice(&fs::read(version.join("sdk-docs-data.v1.json")).unwrap()).unwrap();
    assert_eq!(data["source"]["publicationStatus"], "registryReleased");
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(version.join("generation-manifest.v1.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["scope"]["kind"], "historicalRelease");
    assert!(manifest["sourceFiles"]["rust/crates/demo/descriptor.bin"].is_string());
    assert!(manifest["declaredSourceToolchain"]
        .as_str()
        .unwrap()
        .contains("1.98.1"));
    let scenarios: Value =
        serde_json::from_slice(&fs::read(version.join("sdk-docs-scenarios.v1.json")).unwrap())
            .unwrap();
    assert_eq!(scenarios.as_array().unwrap().len(), 1);
    assert_eq!(scenarios[0]["target"], "historical");
    let index: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("sdk-docs-versions.v1.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(index["latest"]["version"], "1.0.0");
    assert_eq!(index["releases"].as_array().unwrap().len(), 1);
    // Resealing a manifest after omitting an ignored archive member must not
    // make the archive's authoritative input disappear from final validation.
    let omitted = "rust/crates/demo/descriptor.bin";
    manifest["scope"]["archive_source_paths"]
        .as_array_mut()
        .unwrap()
        .retain(|path| path.as_str() != Some(omitted));
    manifest["sourceFiles"]
        .as_object_mut()
        .unwrap()
        .remove(omitted);
    let files: BTreeMap<String, String> =
        serde_json::from_value(manifest["sourceFiles"].clone()).unwrap();
    manifest["sourceSha256"] = json!(source_digest(&files));
    write_manifest(&version, &manifest);
    let altered = Fixture {
        root: fixture.root.clone(),
        rustdoc: version.join(".rustdoc"),
        output: version.clone(),
    };
    let drift = run_drift(&altered);
    assert!(!drift.status.success());
    assert!(
        output_message(&drift).contains("retained registry archive closure differs from scope"),
        "{}",
        output_message(&drift)
    );
    let _ = fs::remove_dir_all(fixture.root);
    let _ = fs::remove_dir_all(fixture.output);
}

#[test]
fn successful_historical_example_mutating_archived_inputs_cannot_admit_catalog() {
    runtime::time_control();
    std::thread::scope(|scope| {
        for mutation in ["descriptor.bin", "input.json", "schema.proto"] {
            scope.spawn(move || {
                let (fixture, support) = historical_fixture(Some(mutation));
                let result = run_historical_generate(&fixture, &support);
                assert!(!result.status.success(), "{mutation} was admitted");
                assert!(
                    output_message(&result).contains(
                        "source closure changed during qualification before manifest admission"
                    ),
                    "{mutation}: {}",
                    output_message(&result)
                );
                assert_eq!(
                    fs::read(fixture.root.join("rust/crates/demo").join(mutation)).unwrap(),
                    b"changed archived input"
                );
                let executions: Value = serde_json::from_slice(
                    &fs::read(
                        fixture
                            .output
                            .join("releases/1.0.0/sdk-docs-scenario-executions.v1.json"),
                    )
                    .unwrap(),
                )
                .unwrap();
                assert_eq!(
                    executions.as_array().unwrap().len(),
                    1,
                    "example must really execute successfully before outer drift rejects"
                );
                assert!(
                    !fixture.output.join("sdk-docs-versions.v1.json").exists(),
                    "mutated historical source must remain unadmitted"
                );
                let _ = fs::remove_dir_all(fixture.root);
                let _ = fs::remove_dir_all(fixture.output);
            });
        }
    });
}

#[test]
fn excluded_published_path_owners_have_executed_api_and_drift() {
    runtime::time_control();
    let fixture = feature_profile_fixture();
    let manifest = fixture.root.join("rust/vendor/macros/Cargo.toml");
    fs::create_dir_all(manifest.parent().unwrap().join("src")).unwrap();
    let demo_manifest = fixture.root.join("rust/crates/demo/Cargo.toml");
    let original = fs::read_to_string(&demo_manifest)
        .unwrap()
        .replace("name = \"demo\"", "name = \"demo-macros\"");
    fs::write(
        &demo_manifest,
        format!("{}\n[dependencies]\ndemo-macros = {{ path = \"../../vendor/macros\" }}\ndemo-helper = {{ path = \"../../vendor/helper\" }}\n", fs::read_to_string(&demo_manifest).unwrap()),
    ).unwrap();
    fs::write(fixture.root.join("Cargo.toml"), "[workspace]\nmembers = [\"rust/crates/demo\"]\nexclude = [\"rust/vendor/macros\", \"rust/vendor/helper\", \"rust/vendor/macro-dev-helper\"]\nresolver = \"2\"\n").unwrap();
    fs::create_dir_all(fixture.root.join("rust/vendor/helper/src")).unwrap();
    fs::write(
        fixture.root.join("rust/vendor/helper/Cargo.toml"),
        "[package]\nname = \"demo-helper\"\nversion = \"1.0.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    fs::write(
        fixture.root.join("rust/vendor/helper/src/lib.rs"),
        "/// Published excluded library API.\npub struct ExcludedOwnedApi;\n",
    )
    .unwrap();
    fs::write(
        fixture.root.join("release/cargo-crates.json"),
        "[\"demo\",\"demo-macros\",\"demo-helper\"]\n",
    )
    .unwrap();
    fs::write(fixture.root.join("Cargo.lock"), "version = 3\n\n[[package]]\nname = \"demo\"\nversion = \"1.0.0\"\ndependencies = [\"demo-helper\", \"demo-macros\"]\n\n[[package]]\nname = \"demo-helper\"\nversion = \"1.0.0\"\n\n[[package]]\nname = \"demo-macros\"\nversion = \"1.0.0\"\n").unwrap();
    fs::write(
        &manifest,
        original.replace(
            "[lib]\npath = \"src/lib.rs\"",
            "[lib]\nname = \"renamed_macro_target\"\nproc-macro = true\npath = \"src/lib.rs\"",
        ),
    )
    .unwrap();
    let macro_manifest = fs::read_to_string(&manifest).unwrap();
    fs::write(&manifest, format!("{macro_manifest}\n[dev-dependencies]\nmacro-dev-helper = {{ path = \"../macro-dev-helper\" }}\n")).unwrap();
    fs::create_dir_all(fixture.root.join("rust/vendor/macro-dev-helper/src")).unwrap();
    fs::write(fixture.root.join("rust/vendor/macro-dev-helper/Cargo.toml"), "[package]\nname = \"macro-dev-helper\"\nversion = \"1.0.0\"\nedition = \"2021\"\npublish = false\n").unwrap();
    fs::write(
        fixture.root.join("rust/vendor/macro-dev-helper/src/lib.rs"),
        "pub struct DevOnly;\n",
    )
    .unwrap();
    fs::write(fixture.root.join("rust/vendor/macros/Cargo.lock"), "version = 3\n\n[[package]]\nname = \"demo-macros\"\nversion = \"1.0.0\"\ndependencies = [\"macro-dev-helper\"]\n\n[[package]]\nname = \"macro-dev-helper\"\nversion = \"1.0.0\"\n").unwrap();
    fs::write(
        fixture.root.join("rust/vendor/macros/src/lib.rs"),
        r#"//! Public procedural macro documentation.
extern crate proc_macro;
use proc_macro::TokenStream;
/// Return the supplied tokens unchanged.
#[proc_macro]
pub fn demo_macro(input: TokenStream) -> TokenStream { input }
/// Preserve an attributed item.
#[proc_macro_attribute]
pub fn demo_attribute(_args: TokenStream, input: TokenStream) -> TokenStream { input }
/// Add no derived implementation.
#[proc_macro_derive(DemoDerive)]
pub fn demo_derive(_input: TokenStream) -> TokenStream { TokenStream::new() }
"#,
    )
    .unwrap();
    git(&fixture.root, &["add", "."]);
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "procedural macro owner fixture"],
    );

    let workspace_only = sdk_docs::rustdoc_profiles::load_metadata(
        fixture.root.join("Cargo.toml"),
        sdk_docs::rustdoc_profiles::CargoExecutionContext::default(),
    )
    .unwrap();
    assert!(!workspace_only
        .packages
        .iter()
        .any(|package| matches!(package.name.as_ref(), "demo-macros" | "demo-helper")));
    let locked_bytes = fs::read(fixture.root.join("Cargo.lock")).unwrap();
    let macro_locked_bytes = fs::read(fixture.root.join("rust/vendor/macros/Cargo.lock")).unwrap();
    let queries = commands::Queries::default();
    let generated = queries.run(|| run_generate_execute_profiles(&fixture));
    assert!(generated.status.success(), "{}", output_message(&generated));
    assert!(fixture.rustdoc.join("renamed_macro_target.json").is_file());
    assert_executed_profiles(
        &fixture.output,
        &fixture.rustdoc,
        &[("demo", 5), ("demo-macros", 5), ("demo-helper", 1)],
    );
    let availability: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("sdk-docs-profile-availability.v1.json")).unwrap(),
    )
    .unwrap();
    for name in ["demo_macro", "demo_attribute", "DemoDerive"] {
        let expected = format!("renamed_macro_target::{name}");
        assert!(
            availability["entries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| {
                    entry["path"].as_str() == Some(expected.as_str())
                        && entry["profiles"]
                            .as_array()
                            .is_some_and(|profiles| !profiles.is_empty())
                }),
            "the actual procedural macro API {expected} must have executed profile coverage"
        );
    }
    assert!(availability["entries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry["path"] == "demo_helper::ExcludedOwnedApi"));
    let data: Value = serde_json::from_slice(
        &fs::read(
            fixture
                .output
                .join("preview/feature-profile-catalog/sdk-docs-data.v1.json"),
        )
        .unwrap(),
    )
    .unwrap();
    for (path, source) in [
        (
            "renamed_macro_target::demo_macro",
            "rust/vendor/macros/src/lib.rs",
        ),
        (
            "demo_helper::ExcludedOwnedApi",
            "rust/vendor/helper/src/lib.rs",
        ),
    ] {
        let item = data["families"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|family| family["items"].as_array().unwrap())
            .find(|item| item["path"] == path)
            .unwrap();
        assert_eq!(item["source"]["path"], source);
    }
    let drift = queries.run(|| run_drift_execute_profiles(&fixture));
    assert!(drift.status.success(), "{}", output_message(&drift));
    assert_eq!(
        fs::read(fixture.root.join("Cargo.lock")).unwrap(),
        locked_bytes
    );
    assert_eq!(
        fs::read(fixture.root.join("rust/vendor/macros/Cargo.lock")).unwrap(),
        macro_locked_bytes
    );
    assert!(!fixture.root.join("rust/vendor/helper/Cargo.lock").exists());
    let _ = fs::remove_dir_all(fixture.root);
    let _ = fs::remove_dir_all(fixture.output);
}
