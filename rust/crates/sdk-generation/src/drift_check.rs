use super::*;

pub(super) fn drift(
    (root, output, rustdoc_dir): (PathBuf, PathBuf, PathBuf),
    stdout: &mut dyn Write,
) -> Result<(), CliError> {
    let mut root = canonical(&root)?;
    let output = canonical(&output)?;
    let rustdoc_dir = canonical(&rustdoc_dir)?;
    let manifest_path = output.join("generation-manifest.v1.json");
    let manifest_bytes = fs::read(&manifest_path).map_err(io_error)?;
    let manifest: GenerationManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| CliError(format!("invalid generation manifest: {e}")))?;
    let expected_manifest_digest =
        fs::read_to_string(output.join("generation-manifest.v1.sha256")).map_err(io_error)?;
    if expected_manifest_digest.trim() != sha256_bytes(&manifest_bytes) {
        return Err(CliError(
            "generation manifest integrity check failed".into(),
        ));
    }
    let archive_capture = if let historical::Scope::RegistryArchives {
        captured_source,
        source_root,
        ..
    } = &manifest.scope
    {
        if source_root != "sources"
            || manifest.channel != Channel::Release
            || !matches!(
                captured_source,
                sdk_docs::historical::CapturedSource::RegistryArchives { .. }
            )
        {
            return Err(CliError(
                "registry archive scope does not identify its owned captured source".into(),
            ));
        }
        root = canonical(&output.join(source_root))?;
        Some(captured_source)
    } else {
        None
    };
    if archive_capture.is_some() && manifest.registry_corpus.is_none() {
        return Err(CliError(
            "registry archive drift requires a complete registry corpus".into(),
        ));
    }
    let source_files =
        historical::scope_source_file_hashes(&root, &output, &manifest.scope).map_err(CliError)?;
    if archive_capture.is_none()
        && !manifest.declared_source_toolchain.is_empty()
        && fs::read_to_string(root.join("rust-toolchain.toml")).map_err(io_error)?
            != manifest.declared_source_toolchain
    {
        return Err(CliError("declared source toolchain changed".into()));
    }
    let digest = digest_map(&source_files);
    if source_files != manifest.source_files || digest != manifest.source_sha256 {
        let changed = manifest
            .source_files
            .keys()
            .chain(source_files.keys())
            .find(|path| manifest.source_files.get(*path) != source_files.get(*path))
            .cloned()
            .unwrap_or_else(|| "<source manifest>".into());
        return Err(CliError(format!(
            "source digest changed since generation at {changed} (expected {}, got {})",
            manifest.source_sha256, digest
        )));
    }
    if let Some(corpus) = &manifest.registry_corpus {
        if archive_capture.is_none() {
            return Err(CliError(
                "registry corpus is outside registry archive scope".into(),
            ));
        }
        historical::verify_registry_corpus(
            corpus,
            &output.join("registry-frontier"),
            &documentation_cargo(&root, true)?,
            &manifest.version,
            &output.join("registry-archives"),
        )
        .map_err(CliError)?;
    }
    let current_revision = if let Some(captured) = archive_capture {
        captured.revision().map_err(docs_error)?
    } else {
        git_revision(&root)?
    };
    if manifest.revision != current_revision {
        return Err(CliError("source revision changed since generation".into()));
    }
    if !manifest.toolchain.is_empty() {
        let current_toolchain =
            toolchain_identity(&documentation_cargo(&root, !manifest.scope.is_current())?)?;
        if manifest.toolchain != current_toolchain {
            return Err(CliError(format!(
                "pinned Cargo toolchain changed since generation (expected {}, got {})",
                manifest.toolchain, current_toolchain
            )));
        }
    }
    let expected_state = if archive_capture.is_some() {
        "registry-archives"
    } else if manifest.channel == Channel::Release {
        "captured-snapshot"
    } else {
        "working-tree"
    };
    if manifest.source_state != expected_state {
        return Err(CliError(format!(
            "generation source state `{}` does not match channel `{expected_state}`",
            manifest.source_state
        )));
    }
    if manifest.channel == Channel::Release && archive_capture.is_none() && git_dirty(&root)? {
        return Err(CliError("release checkout is no longer clean".into()));
    }
    if let historical::Scope::RegistryArchives {
        released_packages,
        archive_source_paths,
        captured_source,
        ..
    } = &manifest.scope
    {
        let sdk_docs::historical::CapturedSource::RegistryArchives { archives } = captured_source
        else {
            return Err(CliError(
                "registry archive scope has another source authority".into(),
            ));
        };
        let releases: Vec<historical::RegistryVersion> = serde_json::from_slice(
            &fs::read(output.join("sdk-docs-registry-releases.v1.json")).map_err(io_error)?,
        )
        .map_err(|e| CliError(e.to_string()))?;
        let mut identities = Vec::new();
        let mut archive_files = BTreeMap::new();
        let mut inventory = BTreeSet::new();
        for release in &releases {
            let identity = archives
                .iter()
                .find(|archive| {
                    archive.package == release.package && archive.version == release.num
                })
                .ok_or_else(|| {
                    CliError(
                        "retained registry owner is absent from captured archive identity".into(),
                    )
                })?;
            let name = format!("{}-{}.crate", release.package, release.num);
            if !inventory.insert(name.clone()) {
                return Err(CliError("duplicate retained registry archive".into()));
            }
            let (publisher, files) = historical::verify_imported_archive(
                &root,
                release,
                &output.join("registry-archives").join(name),
                identity,
            )
            .map_err(CliError)?;
            identities.push(publisher);
            archive_files.extend(files);
        }
        identities.sort_by(|a, b| a.package.cmp(&b.package));
        if identities.len() != archives.len()
            || &identities != released_packages
            || archive_files != source_files
            || archive_files.keys().cloned().collect::<Vec<_>>() != *archive_source_paths
        {
            return Err(CliError(
                "imported registry archive closure differs from captured scope".into(),
            ));
        }
        historical::validate_archive_inventory(&output.join("registry-archives"), &inventory)
            .map_err(CliError)?;
        let cargo = documentation_cargo(&root, true)?;
        let owners =
            historical::imported_metadata(&root, captured_source, &cargo).map_err(CliError)?;
        if declared_archive_toolchain(&historical::owner_index(&owners).map_err(CliError)?)?
            != manifest.declared_source_toolchain
        {
            return Err(CliError("declared archive Cargo toolchain changed".into()));
        }
    }
    if let historical::Scope::HistoricalRelease {
        released_packages,
        archive_source_paths,
    } = &manifest.scope
    {
        let cargo = documentation_cargo(&root, true)?;
        let metadata = load_metadata(&root, Some(&cargo))?;
        let releases: Vec<historical::RegistryVersion> = serde_json::from_slice(
            &fs::read(output.join("sdk-docs-registry-releases.v1.json")).map_err(io_error)?,
        )
        .map_err(|e| CliError(e.to_string()))?;
        let mut identities = Vec::new();
        let mut archive_files = BTreeMap::new();
        let mut inventory = BTreeSet::new();
        for release in &releases {
            let name = format!("{}-{}.crate", release.package, release.num);
            if !inventory.insert(name.clone()) {
                return Err(CliError("duplicate retained registry archive".into()));
            }
            let archive = output.join("registry-archives").join(name);
            let (identity, files) =
                historical::verify_archive(&metadata, &root, &manifest.revision, release, &archive)
                    .map_err(CliError)?;
            identities.push(identity);
            archive_files.extend(files);
        }
        identities.sort_by(|a, b| a.package.cmp(&b.package));
        if &identities != released_packages
            || archive_files.keys().cloned().collect::<Vec<_>>() != *archive_source_paths
        {
            return Err(CliError(
                "retained registry archive closure differs from scope".into(),
            ));
        }
        historical::validate_archive_inventory(&output.join("registry-archives"), &inventory)
            .map_err(CliError)?;
    }
    for (path, expected) in &manifest.rustdoc_files {
        let relative = path.strip_prefix("rustdoc/").unwrap_or(path);
        let actual = rustdoc_digest(&rustdoc_dir.join(relative), &root)
            .map_err(|error| CliError(error.to_string()))?;
        if &actual != expected {
            return Err(CliError(format!("Rustdoc input changed: {path}")));
        }
    }
    let actual = artifact_hashes_with_scope(&output, &manifest.scope)?;
    if actual != manifest.artifacts {
        return Err(CliError(
            "generated artifact digest changed since generation".into(),
        ));
    }
    if !manifest.release_manifest.is_empty() {
        release_manifest::validate(
            &root,
            &output,
            &output.join(&manifest.release_manifest),
            release_manifest::SourceIdentity {
                version: &manifest.version,
                channel: &manifest.channel,
                revision: &manifest.revision,
                source_state: &manifest.source_state,
                source_sha256: &manifest.source_sha256,
                source_files: &manifest.source_files,
            },
            &actual,
        )
        .map_err(|error| CliError(format!("release manifest drift: {error}")))?;
    }
    writeln!(stdout, "drift check passed for {}", output.display()).map_err(io_error)?;
    Ok(())
}
