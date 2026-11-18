use super::*;
use sdk_docs::rustdoc_profiles::{self as profiles, ProfileSpec};

pub(super) fn copy_tree(source: &Path, destination: &Path) {
    copy_tree_inner(source, destination, false, &|_| false);
}

pub(super) fn copy_git_objects(source: &Path, destination: &Path) {
    copy_tree_inner(source, destination, true, &|_| false);
}

fn copy_tree_inner(
    source: &Path,
    destination: &Path,
    immutable: bool,
    skip: &dyn Fn(&Path) -> bool,
) {
    fs::create_dir_all(destination).unwrap();
    let git_dir = !immutable && source.file_name() == Some(std::ffi::OsStr::new(".git"));
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if skip(&path) {
            continue;
        }
        let name = entry.file_name();
        let target = destination.join(&name);
        if git_dir && name == "objects" {
            // Git objects are immutable; refs, index and configuration stay private.
            copy_git_objects(&path, &target);
        } else if entry.file_type().unwrap().is_dir() {
            copy_tree_inner(&path, &target, immutable, skip);
        } else if !immutable {
            fs::copy(path, target).unwrap();
        } else if target.exists() {
            assert_eq!(fs::read(path).unwrap(), fs::read(target).unwrap());
        } else {
            fs::hard_link(path, target).unwrap();
        }
    }
}

pub(super) fn relocate_profile_output(original: &Fixture, relocated: &Fixture) {
    let scratch = relocated.output.join(".profile-build");
    let cargo_target = relocated.root.join("target");
    let roots = [
        (&original.root, &relocated.root),
        (&original.output, &relocated.output),
    ]
    .map(|(old, new)| (old.to_str().unwrap(), new.to_str().unwrap()));
    let rebase = |value: &mut Value| {
        rustdoc_paths::visit(value, |_, text| {
            for (old, new) in roots {
                if let Some(rebased) = super::paths::rebase(text, old, new, cfg!(windows)) {
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
    };
    let manifest_path = original.output.join(GENERATION_MANIFEST);
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    let release_path = PathBuf::from(manifest["releaseManifest"].as_str().unwrap());
    let digest_path = release_path.with_extension("sha256");
    let inventory = manifest["rustdocFiles"]
        .as_object()
        .unwrap()
        .keys()
        .map(|key| original.rustdoc.join(key.strip_prefix("rustdoc/").unwrap()))
        .collect::<std::collections::BTreeSet<_>>();
    let replaced = [
        manifest_path.with_extension("sha256"),
        manifest_path,
        original.output.join(".profile-build"),
        original.output.join(&release_path),
        original.output.join(&digest_path),
    ];
    copy_tree_inner(&original.output, &relocated.output, false, &|path| {
        replaced.iter().any(|replaced| replaced.as_path() == path) || inventory.contains(path)
    });
    let mut rebased_bytes = Vec::new();
    let mut changed = sdk_docs::with_rustdoc_cache(|| {
        let mut changed = BTreeMap::new();
        for original_file in inventory {
            let original_bytes = sdk_docs::read_rustdoc(&original_file).unwrap();
            let value = sdk_docs::read_rustdoc_json(&original_file).unwrap();
            let relative = original_file.strip_prefix(&original.output).unwrap();
            let file = relocated.output.join(relative);
            let mut value = std::rc::Rc::unwrap_or_clone(value);
            rebase(&mut value);
            rebased_bytes.clear();
            serde_json::to_writer(&mut rebased_bytes, &value).unwrap();
            fs::write(&file, &rebased_bytes).unwrap();
            changed.insert(
                relative.to_str().unwrap().replace('\\', "/"),
                (hash_bytes(&original_bytes), hash_bytes(&rebased_bytes)),
            );
        }
        changed
    });
    // Relocation changes physical receipt bytes. Re-attest only those verified
    // rewrites. Drift checks both checkouts against the unchanged logical digests.
    let original_release = fs::read(original.output.join(&release_path)).unwrap();
    let mut release: Value = serde_json::from_slice(&original_release).unwrap();
    for row in release["artifacts"].as_array_mut().unwrap() {
        if let Some((old, new)) = changed.get(row["path"].as_str().unwrap()) {
            assert_eq!(row["sha256"].as_str().unwrap(), old);
            row["sha256"] = json!(new);
        }
    }
    let original_digest = hash_bytes(&original_release);
    assert_eq!(
        fs::read(original.output.join(&digest_path)).unwrap(),
        original_digest.as_bytes(),
        "the original release sidecar must attest the original manifest"
    );
    rebased_bytes.clear();
    serde_json::to_writer_pretty(&mut rebased_bytes, &release).unwrap();
    let rebased_digest = hash_bytes(&rebased_bytes);
    fs::write(relocated.output.join(&release_path), &rebased_bytes).unwrap();
    fs::write(
        relocated.output.join(&digest_path),
        rebased_digest.as_bytes(),
    )
    .unwrap();
    let sidecar_hashes = (
        hash_bytes(original_digest.as_bytes()),
        hash_bytes(rebased_digest.as_bytes()),
    );
    for (path, hashes) in [
        (release_path, (original_digest, rebased_digest)),
        (digest_path, sidecar_hashes),
    ] {
        changed.insert(path.to_str().unwrap().replace('\\', "/"), hashes);
    }
    for (path, (old, new)) in changed {
        assert_eq!(manifest["artifacts"][&path].as_str().unwrap(), old);
        manifest["artifacts"][&path] = json!(new);
    }
    rebased_bytes.clear();
    serde_json::to_writer_pretty(&mut rebased_bytes, &manifest).unwrap();
    write_manifest_bytes(&relocated.output, &rebased_bytes);
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
    rows: &[Value],
    template: &sdk_docs::DocsData,
) -> (sdk_docs::DocsData, profiles::ProfileAvailability) {
    sdk_docs::with_rustdoc_cache(|| project_inner(fixture, metadata, rows, template))
}

fn project_inner(
    fixture: &Fixture,
    metadata: &cargo_metadata::Metadata,
    rows: &[Value],
    template: &sdk_docs::DocsData,
) -> (sdk_docs::DocsData, profiles::ProfileAvailability) {
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(fixture.output.join(GENERATION_MANIFEST)).unwrap())
            .unwrap();
    // The CLI drift checks recompute the logical digests in this inventory.
    let digests = manifest["rustdocFiles"].as_object_mut().unwrap();
    let mut specs = BTreeMap::new();
    let mut items = Vec::new();
    let mut base = Vec::new();
    let mut variants = Vec::new();
    for row in rows {
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
        let id = spec.id();
        assert_eq!(id.0, row["profile"].as_str().unwrap());
        let receipt = receipt_path(&fixture.rustdoc, row);
        assert_eq!(
            digests
                .remove(row["rustdocFile"].as_str().unwrap())
                .unwrap()
                .as_str()
                .unwrap(),
            row["rustdocSha256"].as_str().unwrap()
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
            profiles::extract_owned_api(&receipt, &owner, id.clone(), &observation.crate_name)
                .unwrap(),
        );
        let package = metadata
            .packages
            .iter()
            .find(|package| package.name.as_ref() == spec.package)
            .unwrap();
        let package_metadata = sdk_docs::PackageMetadata {
            rustdoc_source_root: None,
            rustdoc_file: receipt,
            package_name: spec.package.clone(),
            crate_name: observation.crate_name,
            version: package.version.to_string(),
        };
        let group = if spec.default_features && spec.features.is_empty() {
            &mut base
        } else {
            &mut variants
        };
        group.push(package_metadata);
        assert!(specs.insert(id, spec).is_none());
    }
    assert!(
        digests.is_empty(),
        "every declared Rustdoc receipt must have a profile row"
    );
    let build = |package_metadata: Vec<sdk_docs::PackageMetadata>| {
        let receipts = package_metadata
            .iter()
            .map(|metadata| metadata.rustdoc_file.clone())
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
                package_metadata,
                mark_latest: false,
            },
            &[],
        )
        .unwrap()
    };
    let mut data = build(base);
    for variant in variants {
        sdk_docs::merge_profile_catalog(&mut data, build(vec![variant])).unwrap();
    }
    let availability = profiles::project_into_docs(&data, metadata, items, &specs).unwrap();
    (data, availability)
}
