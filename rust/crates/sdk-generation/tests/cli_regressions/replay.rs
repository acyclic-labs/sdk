use super::*;
use sdk_docs::rustdoc_profiles::{self as profiles, ProfileSpec};

pub(super) fn assert_catalog_matches(fixture: &Fixture, data: &sdk_docs::DocsData) {
    fn equal(actual: &Value, expected: &Value, path: String) {
        match (actual, expected) {
            (Value::Object(a), Value::Object(b)) => {
                assert_eq!(a.len(), b.len(), "catalog {path} keys");
                for (key, value) in a {
                    equal(value, &b[key], format!("{path}/{key}"));
                }
            }
            (Value::Array(a), Value::Array(b)) => {
                assert_eq!(a.len(), b.len(), "catalog {path} length");
                for (index, (a, b)) in a.iter().zip(b).enumerate() {
                    equal(a, b, format!("{path}/{index}"));
                }
            }
            _ => assert_eq!(actual, expected, "catalog {path}"),
        }
    }
    let previous: Value = serde_json::from_slice(
        &fs::read(
            fixture
                .output
                .join("preview/feature-profile-catalog/sdk-docs-data.v1.json"),
        )
        .unwrap(),
    )
    .unwrap();
    equal(
        &serde_json::to_value(data).unwrap(),
        &previous,
        String::new(),
    );
}

pub(super) fn copy_tree(source: &Path, destination: &Path) {
    copy_tree_inner(source, destination, false);
}

pub(super) fn copy_profile_output(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_name() == ".profile-build" {
            // Compilers have finished, and drift/project only read these files.
            // Mutable receipts, manifests and retained sources stay private.
            copy_tree_inner(&entry.path(), &target, true);
        } else if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn copy_tree_inner(source: &Path, destination: &Path, immutable: bool) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if source.file_name() == Some(std::ffi::OsStr::new(".git"))
            && entry.file_name() == "objects"
        {
            // Git objects are immutable; refs, index and configuration stay private.
            copy_git_objects(&entry.path(), &target);
        } else if entry.file_type().unwrap().is_dir() {
            copy_tree_inner(&entry.path(), &target, immutable);
        } else if immutable {
            fs::hard_link(entry.path(), target).unwrap();
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

pub(super) fn copy_historical_fixture(template: &(Fixture, PathBuf)) -> (Fixture, PathBuf) {
    let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let root = template.0.root.with_extension(format!("owner-{id}"));
    let output = template.0.output.with_extension(format!("owner-{id}"));
    copy_tree(&template.0.root, &root);
    copy_tree(&template.0.output, &output);
    let support = output.join("test-support");
    (
        Fixture {
            root,
            rustdoc: output.join(".rustdoc"),
            output,
        },
        support,
    )
}

pub(super) fn copy_git_objects(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_git_objects(&entry.path(), &target);
        } else if target.exists() {
            assert_eq!(fs::read(entry.path()).unwrap(), fs::read(target).unwrap());
        } else {
            fs::hard_link(entry.path(), target).unwrap();
        }
    }
}

pub(super) fn relocate_receipts(original: &Fixture, relocated: &Fixture) {
    fn rebase(value: &mut Value, original: &Fixture, relocated: &Fixture) {
        match value {
            Value::String(text) => {
                for (old, new) in [
                    (&original.root, &relocated.root),
                    (&original.output, &relocated.output),
                ] {
                    if let Some(rebased) = super::paths::rebase(
                        text,
                        old.to_str().unwrap(),
                        new.to_str().unwrap(),
                        cfg!(windows),
                    ) {
                        *text = rebased;
                        break;
                    }
                }
            }
            Value::Array(items) => items
                .iter_mut()
                .for_each(|item| rebase(item, original, relocated)),
            Value::Object(items) => items
                .values_mut()
                .for_each(|item| rebase(item, original, relocated)),
            _ => {}
        }
    }
    fn receipts(directory: &Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                receipts(&path, files);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                files.push(path);
            }
        }
    }
    let mut files = Vec::new();
    receipts(&relocated.rustdoc, &mut files);
    let mut changed = BTreeMap::new();
    for file in files {
        let mut value: Value = serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
        if value.get("format_version").is_none() {
            continue;
        }
        let relative = file.strip_prefix(&relocated.output).unwrap();
        let original_file = original.output.join(relative);
        let logical = rustdoc_digest(&original_file, &original.root).unwrap();
        rebase(&mut value, original, relocated);
        fs::write(&file, serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(
            rustdoc_digest(&file, &relocated.root).unwrap(),
            logical,
            "rebasing every physical receipt must preserve its logical digest: {}",
            file.display()
        );
        changed.insert(
            relative.to_str().unwrap().replace('\\', "/"),
            (hash_file(&original_file), hash_file(&file)),
        );
    }
    // Relocation changes physical receipt bytes. Re-attest only those verified
    // rewrites; preserve all source identities and logical Rustdoc digests.
    let manifest_path = relocated.output.join("generation-manifest.v1.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    let release_path = relocated
        .output
        .join(manifest["releaseManifest"].as_str().unwrap());
    let mut release: Value = serde_json::from_slice(&fs::read(&release_path).unwrap()).unwrap();
    for row in release["artifacts"].as_array_mut().unwrap() {
        if let Some((old, new)) = changed.get(row["path"].as_str().unwrap()) {
            assert_eq!(row["sha256"].as_str().unwrap(), old);
            row["sha256"] = json!(new);
        }
    }
    let old_release = hash_file(&release_path);
    let digest_path = release_path.with_extension("sha256");
    let old_sidecar = hash_file(&digest_path);
    fs::write(&release_path, serde_json::to_vec_pretty(&release).unwrap()).unwrap();
    fs::write(&digest_path, hash_file(&release_path)).unwrap();
    for (path, old) in [(&release_path, old_release), (&digest_path, old_sidecar)] {
        changed.insert(
            path.strip_prefix(&relocated.output)
                .unwrap()
                .to_str()
                .unwrap()
                .replace('\\', "/"),
            (old, hash_file(path)),
        );
    }
    for (path, (old, new)) in changed {
        assert_eq!(manifest["artifacts"][&path].as_str().unwrap(), old);
        manifest["artifacts"][&path] = json!(new);
    }
    write_manifest(&relocated.output, &manifest);
}
// Replay the production projection from every real Cargo-produced receipt.
// Compilation is performed once; copies retain the full matrix and source bytes.
pub(super) fn metadata(fixture: &Fixture) -> cargo_metadata::Metadata {
    let mut command = cargo_metadata::MetadataCommand::new();
    command
        .cargo_path(&*runtime::cargo())
        .env("CARGO_HOME", fixture_cargo_home(fixture))
        .env_remove("CARGO_RESOLVER_LOCKFILE_PATH")
        .no_deps()
        .other_options(vec!["--locked".into()])
        .manifest_path(fixture.root.join("Cargo.toml"));
    command.exec().unwrap()
}

pub(super) fn project(
    fixture: &Fixture,
    metadata: &cargo_metadata::Metadata,
) -> (sdk_docs::DocsData, profiles::ProfileAvailability) {
    sdk_docs::with_rustdoc_cache(|| project_inner(fixture, metadata))
}

fn project_inner(
    fixture: &Fixture,
    metadata: &cargo_metadata::Metadata,
) -> (sdk_docs::DocsData, profiles::ProfileAvailability) {
    let template: sdk_docs::DocsData = serde_json::from_slice(
        &fs::read(
            fixture
                .output
                .join("preview/feature-profile-catalog/sdk-docs-data.v1.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let manifest: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("generation-manifest.v1.json")).unwrap(),
    )
    .unwrap();
    for (file, digest) in manifest["sourceFiles"].as_object().unwrap() {
        assert_eq!(
            hash_file(&fixture.root.join(file)),
            digest.as_str().unwrap()
        );
    }
    let rows: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("sdk-docs-rustdoc-profiles.v1.json")).unwrap(),
    )
    .unwrap();
    let rows = rows.as_array().unwrap();
    let mut specs = BTreeMap::new();
    let mut items = Vec::new();
    let mut prepared = Vec::new();
    for row in rows {
        assert_eq!(row["rustdocProfilesCovered"], true);
        let spec = ProfileSpec {
            package: row["package"].as_str().unwrap().to_owned(),
            target: row["target"].as_str().unwrap().to_owned(),
            default_features: row["defaultFeatures"].as_bool().unwrap(),
            features: row["features"]
                .as_array()
                .unwrap()
                .iter()
                .map(|feature| feature.as_str().unwrap().to_owned())
                .collect(),
        };
        assert_eq!(spec.id().0, row["profile"].as_str().unwrap());
        let receipt = fixture.rustdoc.join(
            row["rustdocFile"]
                .as_str()
                .unwrap()
                .strip_prefix("rustdoc/")
                .unwrap(),
        );
        assert_eq!(
            rustdoc_digest(&receipt, &fixture.root).unwrap(),
            row["rustdocSha256"]
        );
        assert_eq!(
            fs::read_to_string(receipt.with_extension("source.sha256"))
                .unwrap()
                .trim(),
            template.source.source_sha256.as_deref().unwrap()
        );
        let observation = profiles::observe_rustdoc(&receipt).unwrap();
        profiles::validate_rustdoc_version(metadata, &spec.package, &observation).unwrap();
        let owner = profiles::api_owner_for_package(metadata, &spec.package).unwrap();
        items.extend(
            profiles::extract_owned_api(&receipt, &owner, spec.id(), &observation.crate_name)
                .unwrap(),
        );
        let package = metadata
            .packages
            .iter()
            .find(|package| package.name.as_ref() == spec.package)
            .unwrap();
        let package_metadata = sdk_docs::PackageMetadata {
            rustdoc_source_root: None,
            rustdoc_file: receipt.clone(),
            package_name: spec.package.clone(),
            crate_name: observation.crate_name,
            version: package.version.to_string(),
        };
        let base = spec.default_features && spec.features.is_empty();
        assert!(specs.insert(spec.id(), spec).is_none());
        prepared.push((base, receipt, package_metadata));
    }
    let build = |selected: Vec<&(bool, PathBuf, sdk_docs::PackageMetadata)>| {
        let receipts = selected
            .iter()
            .map(|(_, receipt, _)| receipt.clone())
            .collect::<Vec<_>>();
        sdk_docs::build_data(
            &sdk_docs::BuildInput {
                version: template.version.clone(),
                channel: template.channel.clone(),
                revision: template.source.revision.clone(),
                source_state: template.source.source_state.clone(),
                source_sha256: template.source.source_sha256.clone(),
                repository_root: fixture.root.clone(),
                generated_sources: sdk_generation::materialize_generated_sources(
                    &fixture.root,
                    &receipts,
                )
                .unwrap(),
                rustdoc_files: receipts,
                package_metadata: selected
                    .iter()
                    .map(|(_, _, metadata)| metadata.clone())
                    .collect(),
                mark_latest: false,
            },
            &[],
        )
        .unwrap()
    };
    let mut data = build(prepared.iter().filter(|(base, _, _)| *base).collect());
    for variant in prepared.iter().filter(|(base, _, _)| !*base) {
        sdk_docs::merge_profile_catalog(&mut data, build(vec![variant])).unwrap();
    }
    let availability = profiles::project_into_docs(&data, metadata, items, &specs).unwrap();
    (data, availability)
}
