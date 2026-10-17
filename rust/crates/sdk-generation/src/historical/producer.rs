use super::*;

/// Reconstruct the separately retained resolution without interpreting arbitrary
/// Cargo overrides. Patches are restricted to checksum-verified dependency archives.
pub(super) fn verify_producer_resolution(
    root: &Path,
    directory: &str,
    producer: &ProducerResolution,
) -> Result<BTreeMap<String, String>, String> {
    let prefix = format!(".docs-producer/{directory}/");
    if producer.lock_path != format!("{prefix}Cargo.lock")
        || producer.config_path != format!("{prefix}config.toml")
    {
        return Err("producer resolution paths differ from archive owner".into());
    }
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let directory_path = root.join(format!(".docs-producer/{directory}"));
    let (_, mut files) =
        producer_dependencies(&root, directory, Some(&producer.dependency_archives))?;
    let mut expected = BTreeSet::from(["Cargo.lock".into(), "config.toml".into()]);
    expected.extend(
        files
            .keys()
            .map(|path| path.strip_prefix(&prefix).unwrap().to_owned()),
    );
    validate_archive_inventory(&directory_path, &expected)?;
    for (logical, expected) in [
        (&producer.lock_path, &producer.lock_sha256),
        (&producer.config_path, &producer.config_sha256),
    ] {
        let path = root.join(logical);
        // Equality with the canonical root-relative path rejects linked ancestors,
        // including links that resolve to another location within the aggregate.
        if path.canonicalize().map_err(|e| e.to_string())? != path {
            return Err("producer input contains a linked or redirected path".into());
        }
        let digest = crate::sha256_file(&path).map_err(|e| e.to_string())?;
        if &digest != expected {
            return Err(format!("retained producer input differs: {logical}"));
        }
        files.insert(logical.clone(), digest);
    }
    validate_producer_config(&root, directory, &producer.dependency_archives)?;
    Ok(files)
}

pub(super) fn validate_producer_config(
    root: &Path,
    directory: &str,
    dependencies: &[ProducerDependencyArchive],
) -> Result<(), String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let config = root.join(format!(".docs-producer/{directory}/config.toml"));
    if config.canonicalize().map_err(|e| e.to_string())? != config {
        return Err("producer config contains a linked or redirected path".into());
    }
    // Config-file paths are relative to TWO levels above the config file.
    let expected = producer_config_bytes(directory, dependencies)?;
    if fs::read(config).map_err(|e| e.to_string())? != expected.as_bytes() {
        return Err("producer config differs from captured lock and dependency archives".into());
    }
    Ok(())
}

pub(super) fn producer_config_bytes(
    directory: &str,
    dependencies: &[ProducerDependencyArchive],
) -> Result<String, String> {
    if dependencies
        .windows(2)
        .any(|p| (&p[0].package, &p[0].version) >= (&p[1].package, &p[1].version))
    {
        return Err("dependency archives must be sorted and unique".into());
    }
    let mut config = format!("[resolver]\nlockfile-path = \"{directory}/Cargo.lock\"\n");
    if !dependencies.is_empty() {
        config.push_str("[patch.crates-io]\n");
    }
    for dependency in dependencies {
        validate_name(&dependency.package)?;
        cargo_metadata::semver::Version::parse(&dependency.version).map_err(|e| e.to_string())?;
        let name = format!("{}-{}", dependency.package, dependency.version);
        config.push_str(&format!(
            "\"{name}\" = {{ package = \"{}\", path = \"{directory}/dependencies/{name}\" }}\n",
            dependency.package
        ));
    }
    Ok(config)
}

pub(super) fn producer_dependencies(
    root: &Path,
    directory: &str,
    retained: Option<&[ProducerDependencyArchive]>,
) -> Result<(Vec<ProducerDependencyArchive>, BTreeMap<String, String>), String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let prefix = format!(".docs-producer/{directory}/");
    let mut dependencies = retained.unwrap_or_default().to_vec();
    if retained.is_none() && root.join(format!("{prefix}registry-archives")).exists() {
        for entry in fs::read_dir(root.join(format!("{prefix}registry-archives")))
            .map_err(|e| e.to_string())?
        {
            let path = entry.map_err(|e| e.to_string())?.path();
            let name = path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or("invalid dependency archive name")?
                .strip_suffix(".crate")
                .ok_or("unknown dependency archive input")?;
            let (package, version) = name
                .match_indices('-')
                .find_map(|(i, _)| {
                    cargo_metadata::semver::Version::parse(&name[i + 1..])
                        .ok()
                        .map(|_| (&name[..i], &name[i + 1..]))
                })
                .ok_or("invalid dependency archive version")?;
            let release = registry_version(package, version)?;
            dependencies.push(ProducerDependencyArchive {
                package: release.package,
                version: release.num,
                registry_checksum: release.checksum,
                yanked: release.yanked,
                publisher_vcs: None,
            });
        }
        dependencies.sort_by(|a, b| (&a.package, &a.version).cmp(&(&b.package, &b.version)));
    }
    producer_config_bytes(directory, &dependencies)?;
    let mut files = BTreeMap::new();
    for dependency in &mut dependencies {
        let release = RegistryVersion {
            package: dependency.package.clone(),
            num: dependency.version.clone(),
            checksum: dependency.registry_checksum.clone(),
            yanked: dependency.yanked,
        };
        let name = format!("{}-{}", release.package, release.num);
        let logical = format!("{prefix}registry-archives/{name}.crate");
        let path = root.join(&logical);
        if path.canonicalize().map_err(|e| e.to_string())? != path {
            return Err("dependency archive redirects source".into());
        }
        let members = archive_members(&release, &path)?;
        let vcs = archive_vcs(&members)?;
        if retained.is_none() {
            dependency.publisher_vcs = vcs.clone();
        }
        if vcs != dependency.publisher_vcs {
            return Err("dependency publisher VCS differs".into());
        }
        files.insert(
            logical,
            crate::sha256_file(&path).map_err(|e| e.to_string())?,
        );
        if retained.is_none() {
            // Check every destination before the first immutable import can create anything.
            for member in members.keys() {
                validate_import_path(
                    &root,
                    &root.join(format!("{prefix}dependencies/{name}/{member}")),
                )?;
            }
        }
        for (member, bytes) in members {
            let logical = format!("{prefix}dependencies/{name}/{member}");
            let path = root.join(&logical);
            if retained.is_none() {
                crate::write_immutable(&path, &bytes).map_err(|e| e.to_string())?;
            }
            if path.canonicalize().map_err(|e| e.to_string())? != path
                || fs::read(path).map_err(|e| e.to_string())? != bytes
            {
                return Err("dependency source differs or redirects".into());
            }
            files.insert(logical, crate::sha256_bytes(&bytes));
        }
    }
    Ok((dependencies, files))
}

pub(super) fn verify_producer_metadata(
    root: &Path,
    directory: &str,
    manifest: &Path,
    cargo: &Path,
    config: &Path,
    dependencies: &[ProducerDependencyArchive],
) -> Result<(), String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    if dependencies.is_empty() {
        return Ok(());
    }
    // Attest the all-features dependency graph required by the historical planner.
    // Optional captured patches need not participate in the default-feature graph.
    let mut command = sdk_docs::command(cargo);
    command
        .args([
            "metadata",
            "--locked",
            "--all-features",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(manifest);
    sdk_docs::rustdoc_profiles::CargoExecutionContext {
        cargo_path: Some(cargo),
        lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(
            config,
        )),
    }
    .configure(&mut command)
    .map_err(|e| e.to_string())?;
    let metadata: Metadata =
        serde_json::from_slice(&command_output(&mut command)?).map_err(|e| e.to_string())?;
    // Cargo may report ordinary Windows paths while the captured root is extended-prefixed.
    let package_paths = metadata
        .packages
        .iter()
        .map(|package| {
            package
                .manifest_path
                .as_std_path()
                .canonicalize()
                .map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut selected_paths =
        BTreeSet::from([manifest.canonicalize().map_err(|e| e.to_string())?]);
    for dependency in dependencies {
        let path = root.join(format!(
            ".docs-producer/{directory}/dependencies/{}-{}/Cargo.toml",
            dependency.package, dependency.version
        ));
        selected_paths.insert(path.clone());
        let package = metadata
            .packages
            .iter()
            .zip(&package_paths)
            .find(|(p, actual)| {
                **actual == path
                    && p.name.as_ref() == dependency.package
                    && p.version.to_string() == dependency.version
            })
            .map(|(package, _)| package)
            .ok_or("Cargo did not select captured dependency root")?;
        if !metadata
            .resolve
            .as_ref()
            .is_some_and(|r| r.nodes.iter().any(|n| n.id == package.id))
        {
            return Err("captured dependency is absent from actual Cargo resolution".into());
        }
    }
    if metadata
        .packages
        .iter()
        .zip(&package_paths)
        .any(|(p, path)| p.source.is_none() && !selected_paths.contains(path))
    {
        return Err("Cargo selected an uncaptured path source".into());
    }
    Ok(())
}

/// Derive the same verified owner config at every maintained Cargo boundary.
pub fn producer_config(
    root: &Path,
    captured: Option<&CapturedSource>,
    package: &str,
) -> Result<Option<std::path::PathBuf>, String> {
    let Some(CapturedSource::RegistryArchives { archives }) = captured else {
        return Ok(None);
    };
    let archive = archives
        .iter()
        .find(|archive| archive.package == package)
        .ok_or("producer Cargo owner is absent from captured archives")?;
    let ResolutionLock::SeparateDocsProducer { producer, .. } = &archive.resolution_lock else {
        return Ok(None);
    };
    verify_producer_resolution(
        root,
        &format!("{}-{}", archive.package, archive.version),
        producer,
    )?;
    Ok(Some(root.join(&producer.config_path)))
}

pub fn producer_lock_selection<'a>(
    config: Option<&'a Path>,
    lock: &'a Path,
    captured: Option<&CapturedSource>,
) -> Option<sdk_docs::rustdoc_profiles::LockSelection<'a>> {
    use sdk_docs::rustdoc_profiles::LockSelection;
    config.map(LockSelection::RetainedConfig).or_else(|| {
        matches!(captured, Some(CapturedSource::RegistryArchives { .. }))
            .then_some(LockSelection::SourceAdjacent(lock))
    })
}

pub fn imported_metadata(
    root: &Path,
    captured: &CapturedSource,
    cargo: &Path,
) -> Result<BTreeMap<String, Metadata>, String> {
    let CapturedSource::RegistryArchives { archives } = captured else {
        return Err("imported metadata requires registry archive identity".into());
    };
    let mut result = BTreeMap::new();
    for archive in archives {
        let manifest = root
            .join(format!("{}-{}", archive.package, archive.version))
            .join("Cargo.toml");
        let config = producer_config(root, Some(captured), &archive.package)?;
        let lock = manifest.with_file_name("Cargo.lock");
        let metadata = sdk_docs::rustdoc_profiles::load_metadata(
            &manifest,
            sdk_docs::rustdoc_profiles::CargoExecutionContext {
                cargo_path: Some(cargo),
                lock_selection: producer_lock_selection(config.as_deref(), &lock, Some(captured)),
            },
        )
        .map_err(|e| e.to_string())?;
        validate_archive_owner(&metadata, &manifest, &archive.package, &archive.version)?;
        if let ResolutionLock::SeparateDocsProducer { producer, .. } = &archive.resolution_lock {
            verify_producer_metadata(
                root,
                &format!("{}-{}", archive.package, archive.version),
                &manifest,
                cargo,
                config.as_deref().ok_or("producer config is absent")?,
                &producer.dependency_archives,
            )?;
        }
        result.insert(archive.package.clone(), metadata);
    }
    Ok(result)
}
