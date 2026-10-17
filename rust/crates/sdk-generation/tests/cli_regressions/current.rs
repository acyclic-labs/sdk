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

fn source_edit_baseline(include_plugin: bool) -> Fixture {
    let fixture = fixture(include_plugin);
    let first = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "source-closure-baseline",
        "preview",
        true,
    );
    assert!(
        first.status.success(),
        "{}{}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );

    fixture
}

fn assert_source_edit_invalidates_output(fixture: &Fixture, relative: &str, changed: &str) {
    let source = fixture.root.join(relative);
    let original = fs::read(&source).unwrap();
    let original_hash = hash_bytes(&original);
    fs::write(&source, changed).unwrap();
    let changed_hash = hash_file(&source);
    assert_ne!(
        original_hash, changed_hash,
        "test edit did not change {relative}"
    );

    let drift = run_drift(&fixture);
    assert!(!drift.status.success());
    let drift_message = format!(
        "{}{}",
        String::from_utf8_lossy(&drift.stdout),
        String::from_utf8_lossy(&drift.stderr)
    );
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
    let stale_message = format!(
        "{}{}",
        String::from_utf8_lossy(&stale.stdout),
        String::from_utf8_lossy(&stale.stderr)
    );
    assert!(
        stale_message.contains("matching source attestation"),
        "{stale_message}"
    );
    assert!(!stale_output.join("generation-manifest.v1.json").exists());

    fs::write(&source, &original).unwrap();
    assert_eq!(hash_file(&source), original_hash);
    let drift = run_drift(&fixture);
    assert!(drift.status.success(), "{}", output_message(&drift));

    let refreshed_output = fixture
        .root
        .join(format!("refreshed-after-source-restore-{suffix}"));
    let refreshed = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &refreshed_output,
        "source-closure-restored",
        "preview",
        true,
    );
    assert!(
        refreshed.status.success(),
        "{}{}",
        String::from_utf8_lossy(&refreshed.stdout),
        String::from_utf8_lossy(&refreshed.stderr)
    );
    let manifest: Value = serde_json::from_slice(
        &fs::read(refreshed_output.join("generation-manifest.v1.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["sourceFiles"][relative], original_hash);
}

#[test]
fn crate_rust_comment_and_markdown_guide_edits_invalidate_output() {
    runtime::time_control();
    let fixture = source_edit_baseline(false);
    assert_source_edit_invalidates_output(
        &fixture,
        "rust/crates/demo/src/lib.rs",
        "pub fn demo() -> u64 { 7 }\n// changed crate comment\n",
    );
    assert_source_edit_invalidates_output(
        &fixture,
        "rust/crates/demo/README.md",
        "# Demo crate\n\nChanged crate guide.\n",
    );
    assert_source_edit_invalidates_output(
        &fixture,
        "docs/guide.md",
        "# Fixture guide\n\nChanged workspace guide.\n",
    );
}

#[test]
fn plugin_rust_edits_invalidate_output() {
    runtime::time_control();
    let fixture = source_edit_baseline(true);
    assert_source_edit_invalidates_output(
        &fixture,
        "plugin/src/main.rs",
        "fn main() {}\n// changed plugin source\n",
    );
}

#[test]
fn unknown_generate_option_is_rejected_before_writes() {
    runtime::time_control();
    let fixture = fixture(false);
    let result = invoke(Command::new(runtime::binary("sdk-generation")).args([
        "generate",
        "--root",
        fixture.root.to_str().unwrap(),
        "--output",
        fixture.output.to_str().unwrap(),
        "--rustdoc-dir",
        fixture.rustdoc.to_str().unwrap(),
        "--version",
        "unknown-option",
        "--unknown",
        "accepted",
    ]));
    assert!(!result.status.success());
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(message.contains("unknown option `--unknown`"), "{message}");
    assert!(
        !fixture.output.exists(),
        "unknown options must fail before generation writes output"
    );
}

#[test]
fn unknown_drift_option_is_rejected_before_reads() {
    runtime::time_control();
    let fixture = fixture(false);
    let result = invoke(Command::new(runtime::binary("sdk-generation")).args([
        "drift",
        "--root",
        fixture.root.to_str().unwrap(),
        "--output",
        fixture.output.to_str().unwrap(),
        "--rustdoc-dir",
        fixture.rustdoc.to_str().unwrap(),
        "--unknown",
        "accepted",
    ]));
    assert!(!result.status.success());
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(message.contains("unknown option `--unknown`"), "{message}");
    assert!(
        !fixture.output.exists(),
        "unknown options must fail before drift reads or writes output"
    );
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
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(message.contains("source state"), "{message}");
}

#[test]
fn plugin_source_is_part_of_manifest_closure() {
    runtime::time_control();
    let fixture = fixture(true);
    let result = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "plugin-source-closure",
        "preview",
        true,
    );
    assert!(
        result.status.success(),
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let generated: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("generation-manifest.v1.json")).unwrap(),
    )
    .unwrap();
    assert!(generated["sourceFiles"]
        .get("plugin/src/main.rs")
        .and_then(Value::as_str)
        .is_some());
    let drift = run_drift(&fixture);
    assert!(drift.status.success(), "{}", output_message(&drift));
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
fn stale_source_attestation_is_rejected_before_generate() {
    runtime::time_control();
    let fixture = fixture(false);
    let first = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "stale-attestation",
        "preview",
        true,
    );
    assert!(
        first.status.success(),
        "{}{}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );

    fs::write(
        fixture.root.join("rust/crates/demo/src/lib.rs"),
        "pub fn demo() -> u64 { 8 }\n",
    )
    .unwrap();
    let second_output = fixture.root.join("stale-output");
    let second = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &second_output,
        "stale-attestation",
        "preview",
        true,
    );
    assert!(!second.status.success());
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    assert!(message.contains("matching source attestation"), "{message}");
    assert!(!second_output.join("generation-manifest.v1.json").exists());
}

#[test]
fn release_generation_requires_executed_profiles() {
    runtime::time_control();
    let fixture = fixture(false);
    git(&fixture.root, &["add", "rustdoc"]);
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "rustdoc receipt"],
    );
    let result = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "release-scenario-bypass",
        "release",
        true,
    );
    assert!(!result.status.success());
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        message.contains("release generation requires --execute-profiles"),
        "{message}"
    );
    assert!(!fixture.output.join("generation-manifest.v1.json").exists());
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
        generate_command(
            &fixture.root,
            &fixture.rustdoc,
            &fixture.output,
            "concurrent-preview",
            "preview",
            true,
        )
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
        generate_command(
            &fixture.root,
            &fixture.rustdoc,
            &fixture.output,
            "broken-lock",
            "preview",
            true,
        )
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
    fs::write(&manifest, format!("{original}\n{feature} = []\n")).unwrap();
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
    for (path, expected) in [
        ("demo::FeatureOnly", 2),
        ("demo::base_only", 3),
        ("demo::default_only", 2),
    ] {
        let entry = availability["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["path"] == path)
            .unwrap();
        assert_eq!(
            entry["profiles"].as_array().unwrap().len(),
            expected,
            "the compilation reuse key must preserve every cfg feature: {path}"
        );
    }
    let feature_entry = availability["entries"]
        .as_array()
        .unwrap()
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

    let variant_entries = availability["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["path"] == "demo::overloaded")
        .collect::<Vec<_>>();
    assert!(
        variant_entries.len() >= 2,
        "profile merge must retain same-path items with distinct signatures"
    );
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

    let receipts: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("sdk-docs-rustdoc-profiles.v1.json")).unwrap(),
    )
    .unwrap();
    assert_executed_profiles(&fixture.output, &fixture.rustdoc, &[("demo", 5)]);
    let effective = receipts
        .as_array()
        .unwrap()
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
        .as_array()
        .unwrap()
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
    let fixture = feature_profile_fixture();
    fs::write(fixture.root.join("rust/crates/demo/Cargo.toml"),
        "[package]\nname = 'demo'\nversion = '1.0.0'\nedition = '2021'\n[[bin]]\nname = 'demo-cli'\npath = 'src/main.rs'\n").unwrap();
    fs::remove_file(fixture.root.join("rust/crates/demo/src/lib.rs")).unwrap();
    fs::write(fixture.root.join("rust/crates/demo/src/main.rs"),
        "//! Run the source-owned command guide.\n/// PRIVATE CLI INTERNALS\nfn hidden() {}\nfn main() { hidden(); }\n").unwrap();
    git(&fixture.root, &["add", "."]);
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "binary-only fixture"],
    );
    let result = run_generate_execute_profiles(&fixture);
    assert!(result.status.success(), "{}", output_message(&result));
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
    fs::create_dir_all(fixture.root.join("rust/crates/helper/src")).unwrap();
    fs::write(
        fixture.root.join("rust/crates/helper/Cargo.toml"),
        "[package]\nname = \"relocation-helper\"\nversion = \"1.0.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    fs::write(
        fixture.root.join("rust/crates/helper/src/lib.rs"),
        "pub struct Witness;\n",
    )
    .unwrap();
    let manifest = fixture.root.join("rust/crates/demo/Cargo.toml");
    let mut contents = fs::read_to_string(&manifest).unwrap();
    contents.push_str("\n[dependencies]\nrelocation-helper = { path = \"../helper\" }\n");
    fs::write(manifest, contents).unwrap();
    // Cargo may infer in-root path dependencies as workspace members. Declare
    // both inventories explicitly so membership discovery cannot change coverage.
    fs::write(
        fixture.root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"rust/crates/demo\", \"rust/crates/helper\"]\nresolver = \"2\"\n",
    )
    .unwrap();
    fs::write(
        fixture.root.join("release/cargo-crates.json"),
        "[\"demo\", \"relocation-helper\"]\n",
    )
    .unwrap();
    fs::write(fixture.root.join("Cargo.lock"),
        "version = 3\n\n[[package]]\nname = \"demo\"\nversion = \"1.0.0\"\ndependencies = [\"relocation-helper\"]\n\n[[package]]\nname = \"relocation-helper\"\nversion = \"1.0.0\"\n").unwrap();
    fs::write(fixture.root.join("rust/crates/demo/build.rs"),
        "fn main() { std::fs::write(std::path::PathBuf::from(std::env::var(\"OUT_DIR\").unwrap()).join(\"wire.rs\"), \"pub struct GeneratedWire;\\n\").unwrap(); }\n").unwrap();
    fs::write(
        fixture.root.join("rust/crates/demo/src/lib.rs"),
        "include!(concat!(env!(\"OUT_DIR\"), \"/wire.rs\"));\npub fn dependency_witness() -> relocation_helper::Witness { relocation_helper::Witness }\n",
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
    let relocated_root = fixture.root.with_extension("relocated");
    let relocated_output = fixture.output.with_extension("relocated");
    assert!(Command::new(runtime::git())
        .args([
            "clone",
            "--quiet",
            fixture.root.to_str().unwrap(),
            relocated_root.to_str().unwrap()
        ])
        .status()
        .unwrap()
        .success());
    let relocated = Fixture {
        root: relocated_root,
        rustdoc: relocated_output.join(".rustdoc"),
        output: relocated_output,
    };
    assert!(!fixture.output.exists());
    assert!(!relocated.output.exists());
    let result = run_generate_execute_profiles(&fixture);
    assert!(result.status.success(), "{}", output_message(&result));
    assert_executed_profiles(
        &fixture.output,
        &fixture.rustdoc,
        &[("demo", 5), ("relocation-helper", 1)],
    );
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
    super::replay::copy_tree(&fixture.root.join("target"), &relocated.root.join("target"));
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
    let dependency_paths = |checkout: &Fixture| {
        let receipts: Value = serde_json::from_slice(
            &fs::read(checkout.output.join("sdk-docs-rustdoc-profiles.v1.json")).unwrap(),
        )
        .unwrap();
        let receipts = receipts.as_array().unwrap();
        let host = receipts[0]["target"].as_str().unwrap();
        let mut expected = sdk_docs::rustdoc_profiles::profiles_for_package(
            &metadata,
            "demo",
            host,
            &std::collections::BTreeSet::from([host.to_owned()]),
        )
        .unwrap()
        .into_iter()
        .map(|profile| profile.id().0)
        .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            expected.len(),
            5,
            "the full original relocation matrix must remain"
        );
        expected.extend(
            sdk_docs::rustdoc_profiles::profiles_for_package(
                &metadata,
                "relocation-helper",
                host,
                &std::collections::BTreeSet::from([host.to_owned()]),
            )
            .unwrap()
            .into_iter()
            .map(|profile| profile.id().0),
        );
        assert_eq!(
            expected.len(),
            6,
            "five demo profiles plus the native helper profile"
        );
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
    let original_dependencies = dependency_paths(&fixture);
    assert_ne!(
        original_dependencies,
        dependency_paths(&relocated),
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
    let mut spans = Vec::new();
    fn source_paths(value: &Value, paths: &mut Vec<String>) {
        if let Some(object) = value.as_object() {
            if let Some(path) = object
                .get("path")
                .and_then(Value::as_str)
                .filter(|path| path.starts_with("generated/rustdoc/"))
            {
                paths.push(path.to_string());
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
        assert_eq!(
            fs::read(fixture.output.join(&path)).unwrap(),
            b"pub struct GeneratedWire;\n"
        );
        assert_eq!(
            fs::read(fixture.output.join(&path)).unwrap(),
            fs::read(relocated.output.join(&path)).unwrap()
        );
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
