use super::*;

#[test]
fn scenario_failure_leaves_docs_unadmitted() {
    runtime::time_control();
    let fixture = fixture(false);
    let result = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "unadmitted-scenario-failure",
        "preview",
        false,
    );
    assert!(!result.status.success(), "{}", output_message(&result));
    assert!(
        output_message(&result).contains("scenario"),
        "{}",
        output_message(&result)
    );
    assert!(fixture
        .output
        .join("preview/unadmitted-scenario-failure/sdk-docs-data.v1.json")
        .exists());
    assert!(!fixture.output.join("sdk-docs-versions.v1.json").exists());
}

#[test]
fn source_mutation_during_profiles_leaves_docs_unadmitted() {
    runtime::time_control();
    let fixture = feature_profile_fixture();
    fs::write(fixture.root.join("rust/crates/demo/build.rs"), r#"
fn main() {
    use std::io::Write;
    let path = std::path::Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("src/lib.rs");
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    writeln!(file, "// source mutated during profile execution").unwrap();
}
"#).unwrap();
    git(&fixture.root, &["add", "."]);
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "source mutation fixture"],
    );
    let result = run_generate_execute_profiles(&fixture);
    assert!(!result.status.success(), "{}", output_message(&result));
    assert!(
        output_message(&result)
            .contains("source closure changed during qualification before manifest admission"),
        "{}",
        output_message(&result)
    );
    assert!(!fixture.output.join("generation-manifest.v1.json").exists());
    assert!(!fixture.output.join("sdk-docs-versions.v1.json").exists());
}

fn assert_source_edit_invalidates_output(
    fixture: &Fixture,
    relative: &str,
    changed: &str,
) -> String {
    let source = fixture.root.join(relative);
    let original = fs::read(&source).unwrap();
    let original_hash = hash_bytes(&original);
    fs::write(&source, changed).unwrap();
    let changed_hash = hash_file(&source);
    assert_ne!(
        original_hash, changed_hash,
        "test edit did not change {relative}"
    );

    let drift = run_drift(fixture);
    assert!(!drift.status.success());
    let drift_message = output_message(&drift);
    assert!(
        drift_message.contains("source digest changed"),
        "{drift_message}"
    );
    assert!(drift_message.contains(relative), "{drift_message}");

    let suffix = relative.replace(['/', '.'], "-");
    let stale_output = fixture
        .root
        .join(format!("stale-after-source-edit-{suffix}"));
    let stale = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &stale_output,
        "source-closure-stale",
        "preview",
        true,
    );
    assert!(!stale.status.success());
    let stale_message = output_message(&stale);
    assert!(
        stale_message.contains("matching source attestation"),
        "{stale_message}"
    );
    assert!(!stale_output.join("generation-manifest.v1.json").exists());

    fs::write(&source, &original).unwrap();
    assert_eq!(hash_file(&source), original_hash);
    let drift = run_drift(fixture);
    assert!(drift.status.success(), "{}", output_message(&drift));

    original_hash
}

#[test]
fn crate_workspace_and_plugin_source_edits_invalidate_output() {
    runtime::time_control();
    let fixture = fixture(true);
    let first = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "source-closure-baseline",
        "preview",
        true,
    );
    assert!(first.status.success(), "{}", output_message(&first));

    let restored = [
        (
            "rust/crates/demo/src/lib.rs",
            "pub fn demo() -> u64 { 7 }\n// changed crate comment\n",
        ),
        (
            "rust/crates/demo/README.md",
            "# Demo crate\n\nChanged crate guide.\n",
        ),
        (
            "docs/guide.md",
            "# Fixture guide\n\nChanged workspace guide.\n",
        ),
        (
            "plugin/src/main.rs",
            "fn main() {}\n// changed plugin source\n",
        ),
    ]
    .map(|(path, changed)| {
        (
            path,
            assert_source_edit_invalidates_output(&fixture, path, changed),
        )
    });

    let refreshed_output = fixture.root.join("refreshed-after-source-restore");
    let refreshed = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &refreshed_output,
        "source-closure-restored",
        "preview",
        true,
    );
    assert!(refreshed.status.success(), "{}", output_message(&refreshed));
    let manifest: Value = serde_json::from_slice(
        &fs::read(refreshed_output.join("generation-manifest.v1.json")).unwrap(),
    )
    .unwrap();
    for (path, hash) in restored {
        assert_eq!(manifest["sourceFiles"][path], hash);
    }
}

#[test]
fn unknown_options_are_rejected_before_accessing_fixtures() {
    let directory = fixture_directory();
    let binary = runtime::binary("sdk-generation");
    for (subcommand, args) in [("generate", &["--version", "1.0.0"][..]), ("drift", &[])] {
        let result = Command::new(&binary)
            .current_dir(directory.path())
            .arg(subcommand)
            .args([
                "--root",
                "source",
                "--output",
                "output",
                "--rustdoc-dir",
                "rustdoc",
            ])
            .args(args)
            .args(["--unknown", "accepted"])
            .output()
            .unwrap();
        let message = output_message(&result);
        assert!(!result.status.success(), "{subcommand}: {message}");
        assert!(
            message.contains("unknown option `--unknown`"),
            "{subcommand}: {message}"
        );
        assert!(!directory.path().join("output").exists(), "{subcommand}");
    }
}

#[test]
fn source_state_tamper_is_rejected() {
    runtime::time_control();
    let fixture = fixture(false);
    let mut value = manifest(&fixture.root, false, "working-tree");
    write_manifest(&fixture.output, &value);
    assert!(run_drift(&fixture).status.success());

    value["sourceState"] = Value::String("captured-snapshot".to_string());
    write_manifest(&fixture.output, &value);
    let result = run_drift(&fixture);
    assert!(!result.status.success());
    let message = output_message(&result);
    assert!(message.contains("source state"), "{message}");
}

#[test]
fn versioned_release_manifest_binds_source_and_scenario_artifacts() {
    runtime::time_control();
    let fixture = fixture(false);
    let result = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "versioned-provenance",
        "preview",
        true,
    );
    assert!(result.status.success(), "{}", output_message(&result));
    let generation: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("generation-manifest.v1.json")).unwrap(),
    )
    .unwrap();
    let release_relative = generation["releaseManifest"].as_str().unwrap();
    let release_path = fixture.output.join(release_relative);
    let release_bytes = fs::read(&release_path).unwrap();
    let release: Value = serde_json::from_slice(&release_bytes).unwrap();
    assert_eq!(release["schema"], "sdk-generation-release-manifest.v1");
    assert_eq!(release["version"], "versioned-provenance");
    assert_eq!(release["revision"], generation["revision"]);
    assert_eq!(release["sourceSha256"], generation["sourceSha256"]);
    assert_eq!(release["sourceFiles"], generation["sourceFiles"]);
    let catalog = release["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|artifact| artifact["kind"] == "scenario-catalog")
        .expect("release manifest must bind the scenario catalog artifact");
    let catalog_path = fixture.output.join(catalog["path"].as_str().unwrap());
    assert_eq!(catalog["sha256"], hash_file(&catalog_path));
    assert_eq!(
        fs::read_to_string(release_path.with_extension("sha256"))
            .unwrap()
            .trim(),
        hash_bytes(&release_bytes)
    );
    assert!(run_drift(&fixture).status.success());

    let original_catalog = fs::read(&catalog_path).unwrap();
    fs::write(&catalog_path, b"tampered scenario catalog\n").unwrap();
    let drift = run_drift(&fixture);
    assert!(!drift.status.success());
    assert!(output_message(&drift).contains("generated artifact digest changed"));
    fs::write(&catalog_path, original_catalog).unwrap();
    assert!(run_drift(&fixture).status.success());
}

#[test]
fn release_generation_requires_executed_profiles() {
    let directory = fixture_directory();
    let root = directory.path();
    let output = root.join("output");
    let result = generate_command(
        root,
        &root.join("rustdoc"),
        &output,
        "1.0.0",
        "release",
        true,
    )
    .output()
    .unwrap();
    assert!(!result.status.success());
    let message = output_message(&result);
    assert!(
        message.contains(
            "release generation does not accept --rustdoc-dir; omit it to execute profiles"
        ),
        "{message}"
    );
    assert!(!output.exists());
}

fn assert_publishers_succeeded(results: &[std::process::Output; 2]) {
    for (writer, result) in results.iter().enumerate() {
        assert_eq!(
            result.status.code(),
            Some(0),
            "publisher {writer}: {}",
            output_message(result)
        );
        assert!(
            result.stderr.is_empty(),
            "publisher {writer}: {}",
            output_message(result)
        );
    }
}

#[test]
fn concurrent_preview_publications_are_idempotent() {
    runtime::time_control();
    let fixture = fixture(false);
    let spawn = || {
        configure_cli(&mut generate_command(
            &fixture.root,
            &fixture.rustdoc,
            &fixture.output,
            "concurrent-preview",
            "preview",
            true,
        ))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap()
    };
    let first = spawn();
    let second = spawn();
    assert_ne!(
        first.id(),
        second.id(),
        "publication writers must be independent processes"
    );
    let results = [
        first.wait_with_output().unwrap(),
        second.wait_with_output().unwrap(),
    ];
    assert_publishers_succeeded(&results);
    let drift = run_drift(&fixture);
    assert!(drift.status.success(), "{}", output_message(&drift));
}

#[test]
fn broken_publication_lock_cannot_be_hidden_by_another_success() {
    runtime::time_control();
    let fixture = fixture(false);
    let lock = fixture.output.join(".sdk-docs-versions.v1.lock");
    fs::create_dir_all(&lock).unwrap();
    let run = || {
        configure_cli(&mut generate_command(
            &fixture.root,
            &fixture.rustdoc,
            &fixture.output,
            "broken-lock",
            "preview",
            true,
        ))
        .output()
        .unwrap()
    };
    let failed = run();
    assert_eq!(failed.status.code(), Some(1), "{}", output_message(&failed));
    assert!(!failed.stderr.is_empty());
    assert!(!fixture.output.join("sdk-docs-versions.v1.json").exists());
    fs::remove_dir(lock).unwrap();
    let succeeded = invoke(&mut generate_command(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "broken-lock",
        "preview",
        true,
    ));
    assert_eq!(
        succeeded.status.code(),
        Some(0),
        "{}",
        output_message(&succeeded)
    );
    let outcomes = [failed, succeeded];
    assert!(
        std::panic::catch_unwind(|| assert_publishers_succeeded(&outcomes)).is_err(),
        "the publication oracle must reject a broken lock even when the other publisher succeeds"
    );
}

#[test]
fn long_feature_identities_use_portable_receipt_paths_and_pass_drift() {
    runtime::time_control();
    let fixture = feature_profile_fixture();
    let feature = "native-execution-observation-".repeat(8);
    let manifest = fixture.root.join("rust/crates/demo/Cargo.toml");
    let original = fs::read_to_string(&manifest).unwrap();
    let declared = include_str!("../fixtures/feature-profile/rust/crates/demo/long-features.toml");
    assert_eq!(declared, format!("{original}\n{feature} = []\n"));
    fs::write(&manifest, declared).unwrap();
    git(&fixture.root, &["add", "."]);
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "long feature identity fixture"],
    );
    let generated = run_generate_execute_profiles(&fixture);
    assert!(generated.status.success(), "{}", output_message(&generated));
    let receipts: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("sdk-docs-rustdoc-profiles.v1.json")).unwrap(),
    )
    .unwrap();
    let long_profiles = receipts
        .as_array()
        .unwrap()
        .iter()
        .filter(|receipt| {
            receipt["features"]
                .as_array()
                .is_some_and(|features| features.iter().any(|value| value == &feature))
        })
        .collect::<Vec<_>>();
    assert!(
        !long_profiles.is_empty(),
        "the long Cargo feature must be executed"
    );
    for receipt in long_profiles {
        assert!(receipt["profile"].as_str().unwrap().len() > 255);
        let relative = receipt["rustdocFile"]
            .as_str()
            .unwrap()
            .strip_prefix("rustdoc/")
            .unwrap();
        let path = fixture.rustdoc.join(relative);
        assert_eq!(
            path.file_name().unwrap().to_string_lossy(),
            format!(
                "profile-{}.json",
                hex::encode(Sha256::digest(
                    receipt["profile"].as_str().unwrap().as_bytes()
                ))
            )
        );
        assert!(path.file_name().unwrap().to_string_lossy().len() < 255);
        assert!(path.is_file(), "the executed receipt must be retained");
        assert!(path.with_extension("source.sha256").is_file());
    }
    let drift = run_drift_execute_profiles(&fixture);
    assert!(drift.status.success(), "{}", output_message(&drift));
}

#[test]
fn feature_profile_api_is_joined_with_catalog_and_source_provenance() {
    runtime::time_control();
    let fixture = feature_profile_fixture();
    let result = run_generate_execute_profiles(&fixture);
    assert!(
        result.status.success(),
        "feature-only profile API must be joined into the docs catalog:\n{}",
        output_message(&result)
    );

    let data_path = fixture
        .output
        .join("preview/feature-profile-catalog/sdk-docs-data.v1.json");
    let data: Value = serde_json::from_slice(&fs::read(&data_path).unwrap()).unwrap();
    assert!(data.to_string().contains("FeatureOnly"));

    let availability: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("sdk-docs-profile-availability.v1.json")).unwrap(),
    )
    .unwrap();
    let entries = availability["entries"].as_array().unwrap();
    for (path, expected) in [
        ("demo::FeatureOnly", 2),
        ("demo::base_only", 3),
        ("demo::default_only", 2),
    ] {
        let entry = entries.iter().find(|entry| entry["path"] == path).unwrap();
        assert_eq!(
            entry["profiles"].as_array().unwrap().len(),
            expected,
            "the compilation reuse key must preserve every cfg feature: {path}"
        );
    }
    let feature_entry = entries
        .iter()
        .find(|entry| entry["path"] == "demo::FeatureOnly")
        .expect("feature-only item must retain a sidecar entry");
    let feature_profile = feature_entry["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|profile| {
            profile["features"]
                .as_array()
                .is_some_and(|features| features.iter().any(|feature| feature == "extra"))
        })
        .expect("FeatureOnly must be attributed to the extra profile");

    let variant_entries = entries
        .iter()
        .filter(|entry| entry["path"] == "demo::overloaded")
        .collect::<Vec<_>>();
    let signatures = variant_entries
        .iter()
        .filter_map(|entry| entry["signature"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        signatures.len() >= 2,
        "same-path profile variants must remain signature-distinct"
    );
    assert!(variant_entries.iter().any(|entry| {
        entry["profiles"].as_array().is_some_and(|profiles| {
            profiles.iter().any(|profile| {
                profile["features"]
                    .as_array()
                    .is_some_and(|features| features.iter().any(|feature| feature == "extra"))
            })
        })
    }));

    let receipts = assert_executed_profiles(&fixture.output, &fixture.rustdoc, &[("demo", 5)]);
    let effective = receipts
        .iter()
        .map(|receipt| {
            (
                receipt["defaultFeatures"].as_bool().unwrap(),
                receipt["features"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|feature| feature == "extra"),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        effective,
        std::collections::BTreeSet::from([
            (false, false),
            (false, true),
            (true, false),
            (true, true),
        ]),
        "both API branches remain qualified with and without Cargo defaults"
    );
    let receipt = receipts
        .iter()
        .find(|receipt| receipt["profile"] == feature_profile["profile"])
        .expect("sidecar profile must have an executed receipt row");
    let receipt_path = fixture.output.join(".rustdoc").join(
        receipt["rustdocFile"]
            .as_str()
            .unwrap()
            .strip_prefix("rustdoc/")
            .unwrap(),
    );
    assert_eq!(
        receipt["rustdocSha256"],
        rustdoc_digest(&receipt_path, &fixture.root).unwrap()
    );
    assert_eq!(
        fs::read_to_string(receipt_path.with_extension("source.sha256"))
            .unwrap()
            .trim(),
        data["source"]["sourceSha256"].as_str().unwrap()
    );
    assert_eq!(data["source"]["revision"], git_revision(&fixture.root));

    let availability_hash =
        hash_file(&fixture.output.join("sdk-docs-profile-availability.v1.json"));
    let metadata = super::replay::metadata(&fixture);
    let (rebuilt, replayed) = super::replay::project(&fixture, &metadata);
    sdk_docs::write_bundle_files(&rebuilt, &fixture.output, false).unwrap();
    assert_eq!(
        hash_bytes(&serde_json::to_vec_pretty(&replayed).unwrap()),
        availability_hash
    );

    assert_eq!(
        hash_file(&fixture.output.join("sdk-docs-profile-availability.v1.json")),
        availability_hash
    );

    let source = fixture.root.join("rust/crates/demo/src/lib.rs");
    let original = fs::read(&source).unwrap();
    let mut changed = original.clone();
    changed.extend_from_slice(b"// source API closure mutation\n");
    fs::write(&source, changed).unwrap();
    let drift = run_drift_execute_profiles(&fixture);
    assert!(!drift.status.success());
    assert!(output_message(&drift).contains("rust/crates/demo/src/lib.rs"));
    fs::write(&source, original).unwrap();
    assert!(
        run_drift_execute_profiles(&fixture).status.success(),
        "restoring the exact Rust source must restore drift validity"
    );
}

#[test]
fn current_binary_only_owner_emits_guides_without_private_api_or_registry_claims() {
    runtime::time_control();
    let fixture = profile_source(
        include_str!("../fixtures/feature-profile/rust/crates/demo/binary.toml"),
        "src/main.rs",
        include_str!("../fixtures/feature-profile/rust/crates/demo/binary.rs"),
    );
    init_git(&fixture.root, "binary-only fixture");
    commands::expect_declared_profiles(1, || {
        let result = run_generate_execute_profiles(&fixture);
        assert!(result.status.success(), "{}", output_message(&result));
    });
    let receipt = fixture.rustdoc.join("demo_cli.json");
    let native = sdk_docs::rustdoc_profiles::observe_binary_rustdoc(&receipt).unwrap();
    assert_eq!(native.format_version, 60);
    assert_eq!(native.crate_version.as_deref(), Some("1.0.0"));
    assert!(native.includes_private);
    assert!(sdk_docs::rustdoc_profiles::observe_rustdoc(&receipt).is_err());
    let raw: Value = serde_json::from_slice(&fs::read(receipt).unwrap()).unwrap();
    assert_eq!(
        raw["index"][raw["root"].to_string()]["docs"],
        "Run the source-owned command guide."
    );
    let data: Value = serde_json::from_slice(
        &fs::read(
            fixture
                .output
                .join("preview/feature-profile-catalog/sdk-docs-data.v1.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(data["source"]["publicationStatus"], "candidate");
    assert!(data["source"]["releasedPackages"].is_null());
    assert!(data["families"][0]["items"].as_array().unwrap().is_empty());
    assert!(data.to_string().contains("source-owned command guide"));
    assert!(!data.to_string().contains("PRIVATE CLI INTERNALS"));
    let drift = run_drift_execute_profiles(&fixture);
    assert!(drift.status.success(), "{}", output_message(&drift));
}

#[test]
fn clean_relocated_profile_builds_share_generated_source_content_identity() {
    runtime::time_control();
    let fixture = feature_profile_fixture();
    for (relative, source) in [
        (
            "Cargo.toml",
            include_str!("../fixtures/relocation/Cargo.toml"),
        ),
        (
            "Cargo.lock",
            include_str!("../fixtures/relocation/Cargo.lock"),
        ),
        (
            "rust/crates/demo/Cargo.toml",
            include_str!("../fixtures/relocation/rust/crates/demo/Cargo.toml"),
        ),
        (
            "rust/crates/demo/build.rs",
            include_str!("../fixtures/relocation/rust/crates/demo/build.rs"),
        ),
        (
            "rust/crates/demo/src/lib.rs",
            include_str!("../fixtures/relocation/rust/crates/demo/src/lib.rs"),
        ),
        (
            "rust/crates/helper/Cargo.toml",
            include_str!("../fixtures/relocation/rust/crates/helper/Cargo.toml"),
        ),
        (
            "rust/crates/helper/src/lib.rs",
            include_str!("../fixtures/relocation/rust/crates/helper/src/lib.rs"),
        ),
    ] {
        let path = fixture.root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, source).unwrap();
    }
    fs::write(
        fixture.root.join("release/cargo-crates.json"),
        "[\"demo\", \"relocation-helper\"]\n",
    )
    .unwrap();
    git(&fixture.root, &["add", "."]);
    git(
        &fixture.root,
        &[
            "commit",
            "--quiet",
            "-m",
            "generated source relocation fixture",
        ],
    );
    let directory = fixture_directory();
    let relocated_root = directory.path().join("source");
    let relocated_output = directory.path().join("output");
    clone_source(&fixture.root, &relocated_root);
    let relocated = Fixture {
        directory,
        root: relocated_root,
        rustdoc: relocated_output.join(".rustdoc"),
        output: relocated_output,
    };
    assert!(!fixture.output.exists());
    assert!(!relocated.output.exists());
    let result = run_generate_execute_profiles(&fixture);
    assert!(result.status.success(), "{}", output_message(&result));
    let owners = [("demo", 5), ("relocation-helper", 1)];
    let original_receipts = assert_executed_profiles(&fixture.output, &fixture.rustdoc, &owners);
    let metadata = super::replay::metadata(&fixture);
    let members = metadata
        .workspace_members
        .iter()
        .map(|id| {
            metadata
                .packages
                .iter()
                .find(|package| package.id == *id)
                .unwrap()
                .name
                .to_string()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        members,
        std::collections::BTreeSet::from(["demo".to_owned(), "relocation-helper".to_owned()])
    );
    assert_eq!(
        serde_json::from_slice::<Vec<String>>(
            &fs::read(fixture.root.join("release/cargo-crates.json")).unwrap()
        )
        .unwrap(),
        vec!["demo", "relocation-helper"]
    );
    super::replay::copy_profile_output(&fixture.output, &relocated.output);
    let generated = "target/sdk-generation-generated-sources";
    super::replay::copy_tree(
        &fixture.root.join(generated),
        &relocated.root.join(generated),
    );
    super::replay::relocate_receipts(&fixture, &relocated);
    std::thread::scope(|scope| {
        for checkout in [&fixture, &relocated] {
            scope.spawn(move || {
                let drift = run_drift_execute_profiles(checkout);
                assert!(drift.status.success(), "{}", output_message(&drift));
            });
        }
    });
    let (data, availability) = super::replay::project(&relocated, &metadata);
    super::replay::assert_catalog_matches(&relocated, &data);
    sdk_docs::write_bundle_files(&data, &relocated.output, false).unwrap();
    assert_eq!(
        serde_json::to_vec_pretty(&availability).unwrap(),
        fs::read(fixture.output.join("sdk-docs-profile-availability.v1.json")).unwrap(),
    );
    let host = original_receipts[0]["target"].as_str().unwrap();
    let available = BTreeSet::from([host.to_owned()]);
    let mut expected = BTreeSet::new();
    for (package, count) in owners {
        let profiles =
            sdk_docs::rustdoc_profiles::profiles_for_package(&metadata, package, host, &available)
                .unwrap();
        assert_eq!(
            profiles.len(),
            count,
            "{package} must retain its full relocation matrix"
        );
        expected.extend(profiles.into_iter().map(|profile| profile.id().0));
    }
    let dependency_paths = |checkout: &Fixture, receipts: &[Value]| {
        let actual = receipts
            .iter()
            .map(|receipt| receipt["profile"].as_str().unwrap().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            actual, expected,
            "every planned profile must have an executed receipt"
        );
        let mut paths = Vec::new();
        for receipt in receipts {
            let path = checkout.rustdoc.join(
                receipt["rustdocFile"]
                    .as_str()
                    .unwrap()
                    .strip_prefix("rustdoc/")
                    .unwrap(),
            );
            let raw: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
            let before = paths.len();
            for record in raw["external_crates"].as_object().unwrap().values() {
                if record["name"] == "relocation_helper" {
                    let physical = record["path"].as_str().unwrap();
                    assert!(physical.contains(".profile-build"), "{physical}");
                    assert!(
                        Path::new(physical).is_file(),
                        "dependency artifact must exist: {physical}"
                    );
                    paths.push(physical.to_string());
                }
            }
            if receipt["package"] == "demo" {
                assert!(
                    paths.len() > before,
                    "every demo profile must retain its real dependency load path"
                );
            }
        }
        paths.sort();
        paths.dedup();
        assert!(
            !paths.is_empty(),
            "actual dependency load paths must be captured"
        );
        paths
    };
    let original_dependencies = dependency_paths(&fixture, &original_receipts);
    let relocated_receipts =
        assert_executed_profiles(&relocated.output, &relocated.rustdoc, &owners);
    assert_ne!(
        original_dependencies,
        dependency_paths(&relocated, &relocated_receipts),
        "raw Rustdoc must retain the relocated dependency artifacts"
    );
    for original in original_dependencies {
        let relocated = super::paths::rebase(
            &original,
            fixture.output.to_str().unwrap(),
            relocated.output.to_str().unwrap(),
            cfg!(windows),
        )
        .unwrap();
        assert_eq!(
            sdk_file_identity::stamp(Path::new(&original)).unwrap(),
            sdk_file_identity::stamp(Path::new(&relocated)).unwrap(),
            "completed compiler artifacts share their native file identity"
        );
    }
    let path = "preview/feature-profile-catalog/sdk-docs-data.v1.json";
    assert_eq!(
        fs::read(fixture.output.join(path)).unwrap(),
        fs::read(relocated.output.join(path)).unwrap(),
        "relocated profile outputs must publish identical content identities and bytes"
    );
    let data: Value =
        serde_json::from_slice(&fs::read(fixture.output.join(path)).unwrap()).unwrap();
    let mut spans = BTreeSet::new();
    fn source_paths(value: &Value, paths: &mut BTreeSet<String>) {
        if let Some(object) = value.as_object() {
            if let Some(path) = object
                .get("path")
                .and_then(Value::as_str)
                .filter(|path| path.starts_with("generated/rustdoc/"))
            {
                paths.insert(path.to_string());
            }
            for child in object.values() {
                source_paths(child, paths);
            }
        } else if let Some(array) = value.as_array() {
            for child in array {
                source_paths(child, paths);
            }
        }
    }
    source_paths(&data, &mut spans);
    assert!(
        !spans.is_empty(),
        "generated source spans must appear in the actual docs data"
    );
    for path in spans {
        for checkout in [&fixture, &relocated] {
            assert_eq!(
                fs::read(checkout.output.join(&path)).unwrap(),
                b"pub struct GeneratedWire;\n"
            );
        }
    }
    assert_eq!(
        fs::read(fixture.output.join("sdk-docs-profile-availability.v1.json")).unwrap(),
        fs::read(
            relocated
                .output
                .join("sdk-docs-profile-availability.v1.json")
        )
        .unwrap()
    );
}
