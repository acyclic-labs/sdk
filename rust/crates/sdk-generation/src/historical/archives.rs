use super::*;

mod staged;
pub(super) use staged::StagedArchive;

/// Imports exact archives with their optional registry inventory.
pub fn archive_plan(
    version: &str,
    archive_dir: &Path,
    source_root: &Path,
    cargo: &Path,
    inventory: Option<&BTreeMap<String, RegistryOwnerVersions>>,
) -> Result<(Plan, Metadata), String> {
    validate_version(version)?;
    if let Some(inventory) = inventory {
        validate_registry_cohort(version, inventory, archive_dir)?;
    }
    let mut inputs = fs::read_dir(archive_dir)
        .map_err(|e| e.to_string())?
        .map(|entry| entry.map(|entry| entry.path()).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    inputs.sort();
    let mut plan = Plan {
        released_packages: Vec::new(),
        source_files: BTreeMap::new(),
        archives: Vec::new(),
        captured_source: None,
        owner_metadata: BTreeMap::new(),
    };
    let mut archive_sources = Vec::new();
    for archive in inputs {
        let name = archive
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("invalid archive filename")?;
        let package = name
            .strip_suffix(&format!("-{version}.crate"))
            .ok_or("archive inventory differs from requested version")?;
        if !archive.is_file() {
            return Err("archive inventory contains a non-file".into());
        }
        let release = if let Some(inventory) = inventory {
            inventory
                .get(package)
                .and_then(|owner| owner.versions.iter().find(|release| release.num == version))
                .cloned()
                .ok_or("archive owner/version is absent from verified registry corpus")?
        } else {
            registry_version(package, version)?
        };
        let members = StagedArchive::read(&release, &archive, ArchiveLimits::default())?;
        let directory = format!("{}-{}", release.package, release.num);
        if !members.files.contains_key("Cargo.toml") {
            return Err("registry archive lacks its normalized Cargo manifest".into());
        }
        let publisher_vcs = members.vcs()?;
        members.publish(source_root, &directory)?;
        for (member, digest) in &members.files {
            plan.source_files
                .insert(format!("{directory}/{member}"), digest.clone());
        }
        let manifest = source_root.join(&directory).join("Cargo.toml");
        let lock = manifest.with_file_name("Cargo.lock");
        let config_directory = source_root.join(format!(".docs-producer/{directory}"));
        let config = config_directory.join("config.toml");
        let resolution_lock = if config_directory.exists() {
            // Explicit retained config is the only opt-in. Never retry a failed
            // published resolution with an implicit replacement or source patch.
            let (dependencies, dependency_files) =
                producer_dependencies(source_root, &directory, None)?;
            validate_producer_config(source_root, &directory, &dependencies)?;
            let mut expected = BTreeSet::from(["config.toml".into()]);
            let prefix = format!(".docs-producer/{directory}/");
            expected.extend(
                dependency_files
                    .keys()
                    .map(|path| path.strip_prefix(&prefix).unwrap().to_owned()),
            );
            let producer_lock = config_directory.join("Cargo.lock");
            if producer_lock.exists() {
                expected.insert("Cargo.lock".into());
            }
            validate_archive_inventory(&config_directory, &expected)?;
            let config_digest = crate::sha256_file(&config).map_err(|e| e.to_string())?;
            let original_lock = match members.files.get("Cargo.lock") {
                Some(bytes) => OriginalLock::Published {
                    sha256: bytes.clone(),
                },
                None => OriginalLock::Absent,
            };
            if !producer_lock.exists() {
                generate_lockfile(
                    cargo,
                    &manifest,
                    sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(&config),
                )?;
            }
            let producer = ProducerResolution {
                lock_path: format!(".docs-producer/{directory}/Cargo.lock"),
                lock_sha256: crate::sha256_file(&producer_lock).map_err(|e| e.to_string())?,
                config_path: format!(".docs-producer/{directory}/config.toml"),
                config_sha256: config_digest,
                dependency_archives: dependencies,
            };
            plan.source_files.extend(verify_producer_resolution(
                source_root,
                &directory,
                &producer,
            )?);
            ResolutionLock::SeparateDocsProducer {
                original_lock,
                producer,
            }
        } else if members.files.contains_key("Cargo.lock") {
            ResolutionLock::Published {
                sha256: crate::sha256_file(&lock).map_err(|e| e.to_string())?,
            }
        } else {
            // Reuse an already captured producer resolution; the locked Cargo
            // metadata/profile commands below validate it without updating it.
            if !lock.exists() {
                generate_lockfile(
                    cargo,
                    &manifest,
                    sdk_docs::rustdoc_profiles::LockSelection::SourceAdjacent(&lock),
                )?;
            }
            let digest = crate::sha256_file(&lock).map_err(|e| e.to_string())?;
            plan.source_files
                .insert(format!("{directory}/Cargo.lock"), digest.clone());
            ResolutionLock::DocsProducer { sha256: digest }
        };
        let metadata = sdk_docs::rustdoc_profiles::load_metadata(
            &manifest,
            sdk_docs::rustdoc_profiles::CargoExecutionContext {
                cargo_path: Some(cargo),
                lock_selection: Some(
                    if matches!(
                        &resolution_lock,
                        ResolutionLock::SeparateDocsProducer { .. }
                    ) {
                        sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(&config)
                    } else {
                        sdk_docs::rustdoc_profiles::LockSelection::SourceAdjacent(&lock)
                    },
                ),
            },
        )
        .map_err(|e| e.to_string())?;
        if let ResolutionLock::SeparateDocsProducer { producer, .. } = &resolution_lock {
            verify_producer_metadata(
                source_root,
                &directory,
                &manifest,
                cargo,
                &config,
                &producer.dependency_archives,
            )?;
        }
        validate_archive_owner(&metadata, &manifest, &release.package, &release.num)?;
        let identity = ArchiveSource {
            package: release.package.clone(),
            version: release.num.clone(),
            registry_checksum: release.checksum.clone(),
            publisher_vcs,
            resolution_lock,
        };
        // Independently reread original members plus selected config/lock after
        // lock generation and metadata; command success cannot hide input drift.
        let (released, verified_files) =
            verify_staged_archive(source_root, &release, &archive, &identity, &members)?;
        plan.source_files.extend(verified_files);
        plan.released_packages.push(released);
        archive_sources.push(identity);
        if plan
            .owner_metadata
            .insert(release.package.clone(), metadata)
            .is_some()
        {
            return Err("duplicate registry archive owner".into());
        }
        plan.archives.push((archive, release));
    }
    plan.released_packages
        .sort_by(|a, b| a.package.cmp(&b.package));
    archive_sources.sort_by(|a, b| (&a.package, &a.version).cmp(&(&b.package, &b.version)));
    let captured = CapturedSource::RegistryArchives {
        archives: archive_sources,
    };
    captured.revision().map_err(|e| e.to_string())?;
    plan.captured_source = Some(captured);
    // This is an owner/target lookup index, not an invented Cargo workspace.
    // Cargo execution always receives its original per-owner metadata/manifest.
    let index = owner_index(&plan.owner_metadata)?;
    Ok((plan, index))
}

fn generate_lockfile(
    cargo: &Path,
    manifest: &Path,
    selection: sdk_docs::rustdoc_profiles::LockSelection<'_>,
) -> Result<(), String> {
    let mut command = sdk_docs::command(cargo);
    command
        .args(["generate-lockfile", "--manifest-path"])
        .arg(manifest);
    sdk_docs::rustdoc_profiles::CargoExecutionContext {
        cargo_path: Some(cargo),
        lock_selection: Some(selection),
    }
    .configure(&mut command)
    .map_err(|e| e.to_string())?;
    command_output(&mut command)?;
    Ok(())
}

/// Admission limits apply to the compressed input and the complete expanded
/// stream, including tar metadata, padding and the gzip trailer pass.
#[derive(Clone, Copy, Debug)]
pub struct ArchiveLimits {
    pub compressed_bytes: u64,
    pub expanded_bytes: u64,
    pub member_bytes: u64,
    pub members: usize,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            compressed_bytes: 64 * 1024 * 1024,
            expanded_bytes: 256 * 1024 * 1024,
            member_bytes: 32 * 1024 * 1024,
            members: 65_536,
        }
    }
}

struct ExpandedReader<R> {
    reader: R,
    remaining: u64,
}

impl<R: Read> Read for ExpandedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut extra = [0];
            return match self.reader.read(&mut extra)? {
                0 => Ok(0),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "registry archive expanded-byte limit exceeded",
                )),
            };
        }
        let length = self.remaining.min(buffer.len() as u64) as usize;
        let read = self.reader.read(&mut buffer[..length])?;
        self.remaining -= read as u64;
        Ok(read)
    }
}

/// Read the whole checksum-verified archive; reject links and escaping members
/// before writing any imported source. Normalized Cargo inputs remain intact.
pub fn archive_members(
    release: &RegistryVersion,
    archive: &Path,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    validate_name(&release.package)?;
    validate_version(&release.num)?;
    let expected = format!("sha256:{}", release.checksum);
    let limits = ArchiveLimits::default();
    let bytes = read_archive_snapshot(archive, limits)?;
    if crate::sha256_bytes(&bytes) != expected {
        return Err("released registry archive checksum differs".into());
    }
    let result =
        decode_archive_members_with_limits(&bytes, &release.package, &release.num, limits)?;
    if crate::sha256_bytes(&read_archive_snapshot(archive, limits)?) != expected {
        return Err("registry archive changed during import".into());
    }
    Ok(result)
}

fn read_archive_snapshot(archive: &Path, limits: ArchiveLimits) -> Result<Vec<u8>, String> {
    let file = fs::File::open(archive).map_err(|e| e.to_string())?;
    if file.metadata().map_err(|e| e.to_string())?.len() > limits.compressed_bytes {
        return Err("registry archive compressed-byte limit exceeded".into());
    }
    let mut bytes = Vec::new();
    file.take(limits.compressed_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > limits.compressed_bytes {
        return Err("registry archive compressed-byte limit exceeded".into());
    }
    Ok(bytes)
}

/// Reads plain tar or gzip Cargo archives without extracting to the filesystem.
pub fn decode_archive_members(
    bytes: &[u8],
    package: &str,
    version: &str,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    decode_archive_members_with_limits(bytes, package, version, ArchiveLimits::default())
}

/// Decode under an explicit caller budget; no member is published before full
/// validation. The retained map is bounded, pending staged-streaming migration.
pub fn decode_archive_members_with_limits(
    bytes: &[u8],
    package: &str,
    version: &str,
    limits: ArchiveLimits,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    if bytes.len() as u64 > limits.compressed_bytes {
        return Err("registry archive compressed-byte limit exceeded".into());
    }
    let mut result = BTreeMap::new();
    visit_archive_members(bytes, package, version, limits, |name, entry| {
        let mut body = Vec::new();
        entry.read_to_end(&mut body).map_err(|e| e.to_string())?;
        result.insert(name.to_owned(), body);
        Ok(())
    })?;
    Ok(result)
}

fn visit_archive_members<R: Read>(
    bytes: R,
    package: &str,
    version: &str,
    limits: ArchiveLimits,
    mut visit: impl FnMut(&str, &mut dyn Read) -> Result<(), String>,
) -> Result<(), String> {
    validate_name(package)?;
    validate_version(version)?;
    let mut input = BufReader::new(bytes);
    let compressed = input
        .fill_buf()
        .map_err(|e| e.to_string())?
        .starts_with(&[0x1f, 0x8b]);
    let reader: Box<dyn Read + '_> = if compressed {
        Box::new(flate2::read::MultiGzDecoder::new(input))
    } else {
        Box::new(input)
    };
    let mut reader = BufReader::new(ExpandedReader {
        reader,
        remaining: limits.expanded_bytes,
    });
    if reader.fill_buf().map_err(|e| e.to_string())?.is_empty() {
        return Err("registry archive is empty".into());
    }
    let mut archive = tar::Archive::new(reader);
    let directory = format!("{package}-{version}");
    let prefix = format!("{directory}/");
    let mut names = BTreeSet::new();
    for (index, entry) in archive.entries().map_err(|e| e.to_string())?.enumerate() {
        if index >= limits.members {
            return Err("registry archive member-count limit exceeded".into());
        }
        let mut entry = entry.map_err(|e| e.to_string())?;
        if entry.size() > limits.member_bytes {
            return Err("registry archive member-byte limit exceeded".into());
        }
        let kind = entry.header().entry_type();
        if !kind.is_file() && !kind.is_dir() {
            return Err("registry archive contains unsupported linked source members".into());
        }
        let path = entry.path_bytes();
        let path = std::str::from_utf8(&path).map_err(|e| e.to_string())?;
        if kind.is_dir() && path == directory {
            continue;
        }
        let relative = path
            .strip_prefix(&prefix)
            .ok_or("registry archive member escapes package prefix")?;
        if relative.contains(['\\', ':'])
            || Path::new(relative).components().any(|part| {
                matches!(
                    part,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err("registry archive member escapes imported source".into());
        }
        if kind.is_dir() {
            continue;
        }
        if relative.is_empty() || !names.insert(relative.to_owned()) {
            return Err("duplicate or empty registry archive member".into());
        }
        let relative = relative.to_owned();
        visit(&relative, &mut entry)?;
    }
    // Reading through the gzip trailer verifies the transport checksum too.
    std::io::copy(&mut archive.into_inner(), &mut std::io::sink()).map_err(|e| e.to_string())?;
    Ok(())
}

pub fn archive_vcs(members: &BTreeMap<String, Vec<u8>>) -> Result<Option<PublisherVcs>, String> {
    parse_archive_vcs(members.get(".cargo_vcs_info.json").map(Vec::as_slice))
}

fn parse_archive_vcs(bytes: Option<&[u8]>) -> Result<Option<PublisherVcs>, String> {
    let Some(bytes) = bytes else { return Ok(None) };
    #[derive(Deserialize)]
    struct Git {
        sha1: String,
        #[serde(default)]
        dirty: bool,
    }
    #[derive(Deserialize)]
    struct Vcs {
        git: Git,
        path_in_vcs: String,
    }
    let vcs: Vcs = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    Ok(Some(PublisherVcs {
        revision: vcs.git.sha1,
        dirty: vcs.git.dirty,
        path_in_vcs: vcs.path_in_vcs,
    }))
}

pub fn verify_imported_archive(
    root: &Path,
    release: &RegistryVersion,
    archive: &Path,
    identity: &ArchiveSource,
) -> Result<(ReleasedPackage, BTreeMap<String, String>), String> {
    let members = StagedArchive::read(release, archive, ArchiveLimits::default())?;
    verify_staged_archive(root, release, archive, identity, &members)
}

fn verify_staged_archive(
    root: &Path,
    release: &RegistryVersion,
    archive: &Path,
    identity: &ArchiveSource,
    members: &StagedArchive,
) -> Result<(ReleasedPackage, BTreeMap<String, String>), String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    if identity.package != release.package
        || identity.version != release.num
        || identity.registry_checksum != release.checksum
    {
        return Err("captured archive identity differs from retained registry facts".into());
    }
    members.check_source(release, archive)?;
    let vcs = members.vcs()?;
    if vcs != identity.publisher_vcs {
        return Err("publisher VCS facts differ from exact archive".into());
    }
    let directory = format!("{}-{}", release.package, release.num);
    let mut files = BTreeMap::new();
    for (member, digest) in &members.files {
        let path = format!("{directory}/{member}");
        let source = root.join(&path).canonicalize().map_err(|e| e.to_string())?;
        if !source.starts_with(&root) || !members.matches(member, &source)? {
            return Err(format!("imported archive source differs: {path}"));
        }
        files.insert(path, digest.clone());
    }
    let lock_path = format!("{directory}/Cargo.lock");
    let lock_digest = members.files.get("Cargo.lock").cloned();
    match &identity.resolution_lock {
        ResolutionLock::Published { sha256 } if lock_digest.as_ref() == Some(sha256) => {}
        ResolutionLock::DocsProducer { sha256 }
            if lock_digest.is_none()
                && crate::sha256_file(&root.join(&lock_path)).map_err(|e| e.to_string())?
                    == *sha256 =>
        {
            files.insert(lock_path, sha256.clone());
        }
        ResolutionLock::SeparateDocsProducer {
            original_lock,
            producer,
        } => {
            match (original_lock, lock_digest.as_ref()) {
                (OriginalLock::Published { sha256 }, Some(actual)) if sha256 == actual => {}
                (OriginalLock::Absent, None) if !root.join(&lock_path).exists() => {}
                _ => return Err("original published lock facts differ from archive".into()),
            }
            files.extend(verify_producer_resolution(&root, &directory, producer)?);
        }
        _ => {
            return Err(
                "Cargo resolution lock does not match its published or producer identity".into(),
            )
        }
    }
    Ok((
        ReleasedPackage {
            package: release.package.clone(),
            version: release.num.clone(),
            yanked: release.yanked,
            registry_checksum: release.checksum.clone(),
            source_revision: vcs
                .as_ref()
                .map(|vcs| vcs.revision.clone())
                .unwrap_or_default(),
            path_in_vcs: vcs
                .as_ref()
                .map(|vcs| vcs.path_in_vcs.clone())
                .unwrap_or_default(),
        },
        files,
    ))
}

pub(super) fn validate_archive_owner(
    metadata: &Metadata,
    manifest: &Path,
    name: &str,
    version: &str,
) -> Result<(), String> {
    let canonical = manifest.canonicalize().map_err(|e| e.to_string())?;
    let owner = metadata
        .packages
        .iter()
        .find(|package| {
            package
                .manifest_path
                .as_std_path()
                .canonicalize()
                .ok()
                .as_deref()
                == Some(&canonical)
        })
        .ok_or("normalized archive manifest is absent from Cargo metadata")?;
    if owner.name.as_ref() != name || owner.version.to_string() != version {
        return Err("normalized archive Cargo identity differs from captured source".into());
    }
    Ok(())
}

pub fn owner_index(owners: &BTreeMap<String, Metadata>) -> Result<Metadata, String> {
    let mut index = owners
        .values()
        .next()
        .cloned()
        .ok_or("no archive Cargo owners")?;
    index.packages.clear();
    index.workspace_members.clear();
    for (name, metadata) in owners {
        let owner = metadata
            .packages
            .iter()
            .find(|package| package.name.as_ref() == name)
            .ok_or("archive owner disappeared from Cargo metadata")?;
        index.packages.push(owner.clone());
        index.workspace_members.push(owner.id.clone());
    }
    Ok(index)
}

pub(super) fn is_linked(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return true;
        }
    }
    metadata.file_type().is_symlink()
}

pub(super) fn validate_import_path(root: &Path, path: &Path) -> Result<(), String> {
    if !path.starts_with(root) {
        return Err("import destination escapes owned root".into());
    }
    for ancestor in path.ancestors().take_while(|p| p.starts_with(root)) {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) => {
                if is_linked(&metadata)
                    || ancestor.canonicalize().map_err(|e| e.to_string())? != ancestor
                {
                    return Err(
                        "import destination contains a linked or redirected ancestor".into(),
                    );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}

pub fn validate_archive_inventory(
    directory: &Path,
    expected: &BTreeSet<String>,
) -> Result<(), String> {
    let metadata = fs::symlink_metadata(directory).map_err(|e| e.to_string())?;
    if is_linked(&metadata) || !metadata.is_dir() {
        return Err("retained inventory root is linked or not a directory".into());
    }
    let root = directory.canonicalize().map_err(|e| e.to_string())?;
    let mut pending = vec![root.clone()];
    let mut actual = BTreeSet::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).map_err(|e| e.to_string())? {
            let path = entry.map_err(|e| e.to_string())?.path();
            validate_import_path(&root, &path)?;
            let kind = fs::symlink_metadata(&path)
                .map_err(|e| e.to_string())?
                .file_type();
            let logical = path
                .strip_prefix(&root)
                .map_err(|e| e.to_string())?
                .to_str()
                .ok_or("invalid retained inventory path")?
                .replace('\\', "/");
            if kind.is_dir()
                && expected
                    .iter()
                    .any(|p| p.starts_with(&format!("{logical}/")))
            {
                pending.push(path);
            } else if kind.is_file() {
                actual.insert(logical);
            } else {
                return Err("retained registry archive inventory contains an unexpected directory or non-file".into());
            }
        }
    }
    if &actual != expected {
        return Err("retained registry archive inventory differs".into());
    }
    Ok(())
}
