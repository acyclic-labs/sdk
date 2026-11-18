use super::*;
use sdk_docs::rustdoc_profiles::{self as profiles, ProfileSpec};

pub(super) fn assert_catalog_matches(fixture: &Fixture, data: &sdk_docs::DocsData) {
    let previous: Value = serde_json::from_slice(
        &fs::read(
            fixture
                .output
                .join("preview/feature-profile-catalog/sdk-docs-data.v1.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(serde_json::to_value(data).unwrap(), previous);
}

pub(super) fn copy_tree(source: &Path, destination: &Path) {
    copy_tree_inner(source, destination, false);
}

pub(super) fn copy_profile_output(source: &Path, destination: &Path) {
    copy_tree_inner(source, destination, true);
}

fn copy_tree_inner(source: &Path, destination: &Path, skip_profile_build: bool) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        if skip_profile_build && entry.file_name() == ".profile-build" {
            continue;
        }
        let target = destination.join(entry.file_name());
        if source.file_name() == Some(std::ffi::OsStr::new(".git"))
            && entry.file_name() == "objects"
        {
            // Git objects are immutable; refs, index and configuration stay private.
            copy_git_objects(&entry.path(), &target);
        } else if entry.file_type().unwrap().is_dir() {
            copy_tree_inner(&entry.path(), &target, false);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
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
        let scratch = relocated.output.join(".profile-build");
        let cargo_target = relocated.root.join("target");
        rustdoc_paths::visit(value, |_, text| {
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
                    let destination = Path::new(&rebased);
                    if (destination.starts_with(&scratch) || destination.starts_with(&cargo_target))
                        && !destination.exists()
                        && Path::new(text.as_str()).is_file()
                    {
                        fs::create_dir_all(destination.parent().unwrap()).unwrap();
                        fs::hard_link(text.as_str(), destination).unwrap();
                    }
                    *text = rebased;
                    break;
                }
            }
        });
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
    receipts(&original.rustdoc, &mut files);
    let mut changed = BTreeMap::new();
    for original_file in files {
        let original_bytes = fs::read(&original_file).unwrap();
        let mut value: Value = serde_json::from_slice(&original_bytes).unwrap();
        if value.get("format_version").is_none() {
            continue;
        }
        let relative = original_file.strip_prefix(&original.output).unwrap();
        let file = relocated.output.join(relative);
        let logical = rustdoc_digest(&original_file, &original.root).unwrap();
        rebase(&mut value, original, relocated);
        let rebased_bytes = serde_json::to_vec(&value).unwrap();
        fs::write(&file, &rebased_bytes).unwrap();
        assert_eq!(
            rustdoc_digest(&file, &relocated.root).unwrap(),
            logical,
            "rebasing every physical receipt must preserve its logical digest: {}",
            file.display()
        );
        changed.insert(
            relative.to_str().unwrap().replace('\\', "/"),
            (hash_bytes(&original_bytes), hash_bytes(&rebased_bytes)),
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
    let cargo = runtime::cargo();
    let mut command = Command::new(cargo.as_path());
    configure_cli(command.env("CARGO_HOME", fixture_cargo_home(fixture)));
    sdk_docs::with_command_environment(&command, || {
        sdk_docs::rustdoc_profiles::load_metadata(
            fixture.root.join("Cargo.toml"),
            sdk_docs::rustdoc_profiles::CargoExecutionContext {
                cargo_path: Some(cargo.as_path()),
                ..Default::default()
            },
        )
    })
    .unwrap()
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
