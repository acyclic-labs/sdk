//! Historical planning uses released archives and Cargo-owned sources, not legacy docs JSON.
use cargo_metadata::{Metadata, TargetKind};
use sdk_docs::historical::{
    ArchiveSource, CapturedSource, OriginalLock, ProducerResolution, PublisherVcs, ReleasedPackage,
    ResolutionLock,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path};
use std::process::Command;

/// The current SDK registry remains the default qualification scope.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum Scope {
    #[default]
    CurrentSdk,
    HistoricalRelease {
        released_packages: Vec<ReleasedPackage>,
        archive_source_paths: Vec<String>,
    },
    RegistryArchives {
        released_packages: Vec<ReleasedPackage>,
        archive_source_paths: Vec<String>,
        captured_source: CapturedSource,
        source_root: String,
    },
}

impl Scope {
    pub fn is_current(&self) -> bool {
        matches!(self, Self::CurrentSdk)
    }
    pub fn archive_source_paths(&self) -> &[String] {
        match self {
            Self::CurrentSdk => &[],
            Self::HistoricalRelease {
                archive_source_paths,
                ..
            }
            | Self::RegistryArchives {
                archive_source_paths,
                ..
            } => archive_source_paths,
        }
    }
}

/// Verified archive inputs for one immutable historical source/version.
pub struct Plan {
    pub released_packages: Vec<ReleasedPackage>,
    pub source_files: BTreeMap<String, String>,
    pub archives: Vec<(std::path::PathBuf, RegistryVersion)>,
    pub captured_source: Option<CapturedSource>,
    pub owner_metadata: BTreeMap<String, Metadata>,
}

/// Query every publishable same-version Cargo owner; missing registry versions
/// are excluded, while errors and missing archives for actual releases fail.
pub fn plan(
    metadata: &Metadata,
    root: &Path,
    revision: &str,
    version: &str,
    archive_dir: &Path,
) -> Result<Plan, String> {
    let mut result = Plan {
        released_packages: Vec::new(),
        source_files: BTreeMap::new(),
        archives: Vec::new(),
        captured_source: None,
        owner_metadata: BTreeMap::new(),
    };
    let mut expected = BTreeSet::new();
    for package in metadata.packages.iter().filter(|package| {
        package.version.to_string() == version
            && package
                .publish
                .as_ref()
                .is_none_or(|registries| !registries.is_empty())
    }) {
        let Some(release) = registry_version_optional(package.name.as_ref(), version)? else {
            continue;
        };
        let name = format!("{}-{version}.crate", package.name);
        expected.insert(name.clone());
        let archive = archive_dir.join(name);
        let identity = verify_archive(metadata, root, revision, &release, &archive)?;
        result
            .source_files
            .extend(archive_source_files(metadata, root, &release, &archive)?);
        result.released_packages.push(identity);
        result.archives.push((archive, release));
    }
    if result.released_packages.is_empty() {
        return Err(
            "historical scope has no actual registry releases for this source/version".into(),
        );
    }
    for entry in fs::read_dir(archive_dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        if !entry.file_type().map_err(|e| e.to_string())?.is_file()
            || !expected.contains(&entry.file_name().to_string_lossy().into_owned())
        {
            return Err(
                "historical archive inventory has an unregistered source or version".into(),
            );
        }
    }
    result
        .released_packages
        .sort_by(|a, b| a.package.cmp(&b.package));
    Ok(result)
}

/// The fields read from the authoritative crates.io version endpoint.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegistryVersion {
    #[serde(rename = "crate")]
    pub package: String,
    pub num: String,
    pub checksum: String,
    pub yanked: bool,
}

/// Import actual package archives without rewriting a publisher's manifest or
/// claiming that its VCS revision identifies the bytes being compiled.
pub fn archive_plan(
    version: &str,
    archive_dir: &Path,
    source_root: &Path,
    cargo: &Path,
) -> Result<(Plan, Metadata), String> {
    validate_version(version)?;
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
        let release = registry_version(package, version)?;
        let members = archive_members(&release, &archive)?;
        let directory = format!("{}-{}", release.package, release.num);
        for (member, bytes) in &members {
            let logical = format!("{directory}/{member}");
            super::write_immutable(&source_root.join(&logical), bytes)
                .map_err(|e| e.to_string())?;
            plan.source_files
                .insert(logical, super::sha256_bytes(bytes));
        }
        let manifest = source_root.join(&directory).join("Cargo.toml");
        if !members.contains_key("Cargo.toml") {
            return Err("registry archive lacks its normalized Cargo manifest".into());
        }
        let lock = manifest.with_file_name("Cargo.lock");
        let config_directory = source_root.join(format!(".docs-producer/{directory}"));
        let config = config_directory.join("config.toml");
        let resolution_lock = if config_directory.exists() {
            // Explicit retained config is the only opt-in. Never retry a failed
            // published resolution with an implicit replacement or source patch.
            validate_producer_config(source_root, &directory)?;
            let mut expected = BTreeSet::from(["config.toml".into()]);
            let producer_lock = config_directory.join("Cargo.lock");
            if producer_lock.exists() {
                expected.insert("Cargo.lock".into());
            }
            validate_archive_inventory(&config_directory, &expected)?;
            let config_digest = super::sha256_file(&config).map_err(|e| e.to_string())?;
            let original_lock = match members.get("Cargo.lock") {
                Some(bytes) => OriginalLock::Published {
                    sha256: super::sha256_bytes(bytes),
                },
                None => OriginalLock::Absent,
            };
            if !producer_lock.exists() {
                let mut command = Command::new(cargo);
                command
                    .args(["generate-lockfile", "--manifest-path"])
                    .arg(&manifest);
                sdk_docs::rustdoc_profiles::CargoExecutionContext {
                    cargo_path: Some(cargo),
                    config_path: Some(&config),
                }
                .configure(&mut command);
                command_output(&mut command)?;
            }
            let producer = ProducerResolution {
                lock_path: format!(".docs-producer/{directory}/Cargo.lock"),
                lock_sha256: super::sha256_file(&producer_lock).map_err(|e| e.to_string())?,
                config_path: format!(".docs-producer/{directory}/config.toml"),
                config_sha256: config_digest,
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
        } else if members.contains_key("Cargo.lock") {
            ResolutionLock::Published {
                sha256: super::sha256_file(&lock).map_err(|e| e.to_string())?,
            }
        } else {
            // Reuse an already captured producer resolution; the locked Cargo
            // metadata/profile commands below validate it without updating it.
            if !lock.exists() {
                command_output(
                    Command::new(cargo)
                        .args(["generate-lockfile", "--manifest-path"])
                        .arg(&manifest),
                )?;
            }
            let digest = super::sha256_file(&lock).map_err(|e| e.to_string())?;
            plan.source_files
                .insert(format!("{directory}/Cargo.lock"), digest.clone());
            ResolutionLock::DocsProducer { sha256: digest }
        };
        let metadata = sdk_docs::rustdoc_profiles::load_metadata_with_context(
            &manifest,
            sdk_docs::rustdoc_profiles::CargoExecutionContext {
                cargo_path: Some(cargo),
                config_path: matches!(
                    &resolution_lock,
                    ResolutionLock::SeparateDocsProducer { .. }
                )
                .then_some(config.as_path()),
            },
        )
        .map_err(|e| e.to_string())?;
        validate_archive_owner(&metadata, &manifest, &release.package, &release.num)?;
        let publisher_vcs = archive_vcs(&members)?;
        let identity = ArchiveSource {
            package: release.package.clone(),
            version: release.num.clone(),
            registry_checksum: release.checksum.clone(),
            publisher_vcs: publisher_vcs.clone(),
            resolution_lock: resolution_lock.clone(),
        };
        // Independently reread original members plus selected config/lock after
        // lock generation and metadata; command success cannot hide input drift.
        let (_, verified_files) =
            verify_imported_archive(source_root, &release, &archive, &identity)?;
        plan.source_files.extend(verified_files);
        plan.released_packages.push(ReleasedPackage {
            package: release.package.clone(),
            version: release.num.clone(),
            yanked: release.yanked,
            registry_checksum: release.checksum.clone(),
            source_revision: publisher_vcs
                .as_ref()
                .map(|vcs| vcs.revision.clone())
                .unwrap_or_default(),
            path_in_vcs: publisher_vcs
                .as_ref()
                .map(|vcs| vcs.path_in_vcs.clone())
                .unwrap_or_default(),
        });
        archive_sources.push(ArchiveSource {
            package: release.package.clone(),
            version: release.num.clone(),
            registry_checksum: release.checksum.clone(),
            publisher_vcs,
            resolution_lock,
        });
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

/// Read the whole checksum-verified archive; reject links and escaping members
/// before writing any imported source. Normalized Cargo inputs remain intact.
pub fn archive_members(
    release: &RegistryVersion,
    archive: &Path,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    validate_name(&release.package)?;
    validate_version(&release.num)?;
    let expected = format!("sha256:{}", release.checksum);
    if super::sha256_file(archive).map_err(|e| e.to_string())? != expected {
        return Err("released registry archive checksum differs".into());
    }
    let types = command_output(Command::new("tar").arg("-tvf").arg(archive))?;
    if String::from_utf8(types)
        .map_err(|e| e.to_string())?
        .lines()
        .any(|line| !line.starts_with('-') && !line.starts_with('d'))
    {
        return Err("registry archive contains unsupported linked source members".into());
    }
    let listing = String::from_utf8(command_output(Command::new("tar").arg("-tf").arg(archive))?)
        .map_err(|e| e.to_string())?;
    let prefix = format!("{}-{}/", release.package, release.num);
    let mut result = BTreeMap::new();
    for entry in listing.lines() {
        let relative = entry
            .strip_prefix(&prefix)
            .ok_or("registry archive member escapes package prefix")?;
        let path = Path::new(relative);
        if path.is_absolute()
            || relative.contains(['\\', ':'])
            || path
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
        {
            return Err("registry archive member escapes imported source".into());
        }
        if entry.ends_with('/') {
            continue;
        }
        if relative.is_empty() || result.contains_key(relative) {
            return Err("duplicate or empty registry archive member".into());
        }
        result.insert(
            relative.into(),
            command_output(Command::new("tar").arg("-xOf").arg(archive).arg(entry))?,
        );
    }
    if super::sha256_file(archive).map_err(|e| e.to_string())? != expected {
        return Err("registry archive changed during import".into());
    }
    Ok(result)
}

pub fn archive_vcs(members: &BTreeMap<String, Vec<u8>>) -> Result<Option<PublisherVcs>, String> {
    let Some(bytes) = members.get(".cargo_vcs_info.json") else {
        return Ok(None);
    };
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
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    if identity.package != release.package
        || identity.version != release.num
        || identity.registry_checksum != release.checksum
    {
        return Err("captured archive identity differs from retained registry facts".into());
    }
    let members = archive_members(release, archive)?;
    let vcs = archive_vcs(&members)?;
    if vcs != identity.publisher_vcs {
        return Err("publisher VCS facts differ from exact archive".into());
    }
    let directory = format!("{}-{}", release.package, release.num);
    let mut files = BTreeMap::new();
    for (member, bytes) in &members {
        let path = format!("{directory}/{member}");
        let source = root.join(&path).canonicalize().map_err(|e| e.to_string())?;
        if !source.starts_with(&root) || fs::read(&source).map_err(|e| e.to_string())? != *bytes {
            return Err(format!("imported archive source differs: {path}"));
        }
        files.insert(path, super::sha256_bytes(bytes));
    }
    let lock_path = format!("{directory}/Cargo.lock");
    let lock_digest = members
        .get("Cargo.lock")
        .map(|bytes| super::sha256_bytes(bytes));
    match &identity.resolution_lock {
        ResolutionLock::Published { sha256 } if lock_digest.as_ref() == Some(sha256) => {}
        ResolutionLock::DocsProducer { sha256 }
            if lock_digest.is_none()
                && super::sha256_file(&root.join(&lock_path)).map_err(|e| e.to_string())?
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

/// Reconstruct the separately retained resolution without interpreting arbitrary
/// Cargo overrides. This cut supports an external lock only, never path patches.
fn verify_producer_resolution(
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
    validate_archive_inventory(
        &directory_path,
        &BTreeSet::from(["Cargo.lock".into(), "config.toml".into()]),
    )?;
    let mut files = BTreeMap::new();
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
        let digest = super::sha256_file(&path).map_err(|e| e.to_string())?;
        if &digest != expected {
            return Err(format!("retained producer input differs: {logical}"));
        }
        files.insert(logical.clone(), digest);
    }
    validate_producer_config(&root, directory)?;
    Ok(files)
}

fn validate_producer_config(root: &Path, directory: &str) -> Result<(), String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let config = root.join(format!(".docs-producer/{directory}/config.toml"));
    if config.canonicalize().map_err(|e| e.to_string())? != config {
        return Err("producer config contains a linked or redirected path".into());
    }
    // Config-file paths are relative to TWO levels above the config file.
    let expected = format!("[resolver]\nlockfile-path = \"{directory}/Cargo.lock\"\n");
    if fs::read(config).map_err(|e| e.to_string())? != expected.as_bytes() {
        return Err("producer config must select only the captured external lock".into());
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
        let metadata = sdk_docs::rustdoc_profiles::load_metadata_with_context(
            &manifest,
            sdk_docs::rustdoc_profiles::CargoExecutionContext {
                cargo_path: Some(cargo),
                config_path: config.as_deref(),
            },
        )
        .map_err(|e| e.to_string())?;
        validate_archive_owner(&metadata, &manifest, &archive.package, &archive.version)?;
        result.insert(archive.package.clone(), metadata);
    }
    Ok(result)
}

fn validate_archive_owner(
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

pub fn validate_archive_inventory(
    directory: &Path,
    expected: &BTreeSet<String>,
) -> Result<(), String> {
    let actual = fs::read_dir(directory)
        .map_err(|e| e.to_string())?
        .map(|entry| {
            let entry = entry.map_err(|e| e.to_string())?;
            if !entry.file_type().map_err(|e| e.to_string())?.is_file() {
                return Err("retained registry archive inventory contains a non-file".into());
            }
            Ok(entry.file_name().to_string_lossy().into_owned())
        })
        .collect::<Result<BTreeSet<_>, String>>()?;
    if &actual != expected {
        return Err("retained registry archive inventory differs".into());
    }
    Ok(())
}

/// Fetch an actual registry version, retaining yanked historical releases.
pub fn registry_version(package: &str, version: &str) -> Result<RegistryVersion, String> {
    validate_name(package)?;
    validate_version(version)?;
    registry_version_optional(package, version)?
        .ok_or("requested package version is absent from the registry".into())
}

fn registry_version_optional(
    package: &str,
    version: &str,
) -> Result<Option<RegistryVersion>, String> {
    validate_name(package)?;
    validate_version(version)?;
    let url = format!("https://crates.io/api/v1/crates/{package}/{version}");
    // Resolve PATH explicitly: Windows otherwise searches System32 before PATH,
    // bypassing the caller's selected registry transport executable.
    let executable = std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .map(|path| path.join(if cfg!(windows) { "curl.exe" } else { "curl" }))
                .find(|path| path.is_file())
        })
        .ok_or("registry transport curl is absent from PATH")?;
    let bytes = command_output(Command::new(executable).args([
        "--silent",
        "--show-error",
        "--location",
        "--user-agent",
        concat!(
            "acyclic-sdk-docs/",
            env!("CARGO_PKG_VERSION"),
            " (https://github.com/acyclic-labs/sdk)"
        ),
        "--max-time",
        "60",
        "--write-out",
        "\n%{http_code}",
        &url,
    ]))?;
    let split = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .ok_or("registry response lacks HTTP status")?;
    let status = &bytes[split + 1..];
    if status == b"404" {
        return Ok(None);
    }
    if status != b"200" {
        return Err(format!(
            "registry lookup failed with HTTP {}",
            String::from_utf8_lossy(status)
        ));
    }
    #[derive(Deserialize)]
    struct Response {
        version: RegistryVersion,
    }
    let response: Response = serde_json::from_slice(&bytes[..split]).map_err(|e| e.to_string())?;
    if response.version.package != package || response.version.num != version {
        return Err("registry response identity differs from requested release".into());
    }
    Ok(Some(response.version))
}

/// Verify archive bytes, VCS provenance and exact historical Cargo identity.
pub fn verify_archive(
    metadata: &Metadata,
    root: &Path,
    revision: &str,
    release: &RegistryVersion,
    archive: &Path,
) -> Result<ReleasedPackage, String> {
    validate_name(&release.package)?;
    validate_version(&release.num)?;
    if release.checksum.len() != 64
        || !release
            .checksum
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("invalid registry archive checksum".into());
    }
    let actual = super::sha256_file(archive).map_err(|e| e.to_string())?;
    if actual != format!("sha256:{}", release.checksum) {
        return Err("released registry archive checksum differs".into());
    }
    let members = archive_members(release, archive)?;
    let vcs = archive_vcs(&members)?.ok_or("clean Git binding requires publisher VCS facts")?;
    if vcs.dirty {
        return Err(
            "released archive records dirty source; clean Git provenance is not valid".into(),
        );
    }
    if vcs.revision != revision
        || revision.len() != 40
        || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("released archive VCS revision differs from historical source".into());
    }
    let path = Path::new(&vcs.path_in_vcs);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err("released archive VCS path escapes source root".into());
    }
    let package = metadata
        .packages
        .iter()
        .find(|package| {
            package.name.as_ref() == release.package && package.version.to_string() == release.num
        })
        .ok_or("released archive has no matching historical Cargo package/version")?;
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let source = root.join(path).canonicalize().map_err(|e| e.to_string())?;
    if !source.starts_with(&root)
        || package
            .manifest_path
            .parent()
            .ok_or("package manifest has no parent")?
            .as_std_path()
            .canonicalize()
            .map_err(|e| e.to_string())?
            != source
    {
        return Err("released archive VCS path differs from Cargo package source".into());
    }
    verify_archive_sources(&root, package, &source, archive, &members)?;
    if super::sha256_file(archive).map_err(|e| e.to_string())? != actual {
        return Err("released archive changed during verification".into());
    }
    Ok(ReleasedPackage {
        package: release.package.clone(),
        version: release.num.clone(),
        yanked: release.yanked,
        registry_checksum: release.checksum.clone(),
        source_revision: revision.into(),
        path_in_vcs: vcs.path_in_vcs,
    })
}

// Read entries without extracting or rewriting historical sources. Every retained
// native Rust/Markdown byte must agree with the claimed clean checkout.
fn verify_archive_sources(
    root: &Path,
    package: &cargo_metadata::Package,
    source: &Path,
    archive: &Path,
    members: &BTreeMap<String, Vec<u8>>,
) -> Result<BTreeMap<String, String>, String> {
    let mut source_inputs = BTreeMap::new();
    let mut retained = BTreeSet::new();
    let mut has_manifest = false;
    for (member, released) in members {
        let relative = member.as_str();
        let original_manifest = relative == "Cargo.toml.orig";
        // Cargo creates the normalized manifest and standalone package lock.
        // The exact archive checksum retains these packaging products; native
        // generation uses Cargo.toml.orig and the frozen workspace Cargo.lock.
        if matches!(
            relative,
            "Cargo.toml" | "Cargo.lock" | ".cargo_vcs_info.json"
        ) {
            continue;
        }
        let relative = if original_manifest {
            "Cargo.toml"
        } else {
            relative
        };
        if !retained.insert(relative.to_owned()) {
            return Err("released archive repeats an authoritative source entry".into());
        }
        let checkout = source.join(relative).canonicalize().map_err(|e| {
            format!("released archive source is absent from checkout: {relative}: {e}")
        })?;
        if !checkout.starts_with(source) {
            return Err("released archive source escapes checkout package".into());
        }
        if fs::read(checkout).map_err(|e| e.to_string())? != *released {
            return Err(format!(
                "released archive source bytes differ from checkout: {relative}"
            ));
        }
        let logical = super::path_string(
            source
                .join(relative)
                .strip_prefix(root)
                .map_err(|e| e.to_string())?,
        );
        source_inputs.insert(logical, super::sha256_bytes(released));
        has_manifest |= original_manifest;
    }
    if !has_manifest {
        return Err("released archive has no original Cargo manifest".into());
    }
    // Cargo's explicit sources and the maintained docs source closure must be
    // present too: matching a subset of the archive cannot prove a checkout.
    for target in &package.targets {
        let target_source = target
            .src_path
            .as_std_path()
            .canonicalize()
            .map_err(|e| e.to_string())?;
        let relative = target_source
            .strip_prefix(source)
            .map_err(|_| "historical Cargo target escapes package")?;
        if !retained.contains(&super::path_string(relative)) {
            return Err(format!(
                "released archive is missing Cargo Rust source: {}",
                relative.display()
            ));
        }
    }
    let mut source_files = Vec::new();
    super::collect_source_files(root, source, archive, &mut source_files)
        .map_err(|e| e.to_string())?;
    for file in source_files {
        if matches!(
            file.extension().and_then(|ext| ext.to_str()),
            Some("rs" | "md")
        ) {
            let relative =
                super::path_string(file.strip_prefix(source).map_err(|e| e.to_string())?);
            if !retained.contains(&relative) {
                return Err(format!(
                    "released archive is missing native docs source: {relative}"
                ));
            }
        }
    }
    Ok(source_inputs)
}

/// All non-Cargo-generated archive inputs, verified against the source checkout.
/// Returned paths extend the maintained generation manifest's source map.
pub fn archive_source_files(
    metadata: &Metadata,
    root: &Path,
    release: &RegistryVersion,
    archive: &Path,
) -> Result<BTreeMap<String, String>, String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let package = metadata
        .packages
        .iter()
        .find(|package| {
            package.name.as_ref() == release.package && package.version.to_string() == release.num
        })
        .ok_or("released archive has no matching Cargo identity")?;
    let source = package
        .manifest_path
        .parent()
        .ok_or("Cargo manifest has no parent")?
        .as_std_path()
        .canonicalize()
        .map_err(|e| e.to_string())?;
    verify_archive_sources(
        &root,
        package,
        &source,
        archive,
        &archive_members(release, archive)?,
    )
}

/// A source-owned example and its exact Cargo invocation inputs.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Example {
    pub package: String,
    pub version: String,
    pub target: String,
    pub source_path: String,
    pub required_features: Vec<String>,
    pub source_sha256: String,
}

pub fn examples(
    metadata: &Metadata,
    root: &Path,
    owners: &BTreeSet<String>,
) -> Result<Vec<Example>, String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    if owners.iter().any(|owner| {
        !metadata
            .packages
            .iter()
            .any(|package| package.name.as_ref() == owner)
    }) {
        return Err("historical example owner is absent from Cargo metadata".into());
    }
    let mut examples = Vec::new();
    for package in metadata
        .packages
        .iter()
        .filter(|package| owners.contains(package.name.as_ref()))
    {
        for target in package
            .targets
            .iter()
            .filter(|target| target.kind.contains(&TargetKind::Example))
        {
            let path = target
                .src_path
                .as_std_path()
                .canonicalize()
                .map_err(|e| e.to_string())?;
            let relative = path
                .strip_prefix(&root)
                .map_err(|_| "historical example source escapes source root")?;
            examples.push(Example {
                package: package.name.to_string(),
                version: package.version.to_string(),
                target: target.name.clone(),
                source_path: super::path_string(relative),
                required_features: target.required_features.clone(),
                source_sha256: super::sha256_file(&path).map_err(|e| e.to_string())?,
            });
        }
    }
    examples.sort_by(|a, b| (&a.package, &a.target).cmp(&(&b.package, &b.target)));
    Ok(examples)
}

/// Extend the maintained closure with source files verified from archive members.
pub fn source_file_hashes(
    root: &Path,
    output: &Path,
    archive_paths: &[String],
) -> Result<BTreeMap<String, String>, String> {
    let mut files = super::source_file_hashes(root, output).map_err(|e| e.to_string())?;
    for relative in archive_paths {
        let path = Path::new(relative);
        if path.is_absolute()
            || path
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
        {
            return Err("historical archive source path escapes source root".into());
        }
        let physical = root.join(path).canonicalize().map_err(|e| e.to_string())?;
        if !physical.starts_with(root.canonicalize().map_err(|e| e.to_string())?) {
            return Err("historical archive source escapes source root".into());
        }
        files.insert(
            relative.clone(),
            super::sha256_file(&physical).map_err(|e| e.to_string())?,
        );
    }
    Ok(files)
}

pub fn scope_source_file_hashes(
    root: &Path,
    output: &Path,
    scope: &Scope,
) -> Result<BTreeMap<String, String>, String> {
    if matches!(scope, Scope::RegistryArchives { .. }) {
        super::imported_source_file_hashes(root, output).map_err(|e| e.to_string())
    } else {
        source_file_hashes(root, output, scope.archive_source_paths())
    }
}

/// Run only the actual historical targets; no modern scenario sources are copied.
pub fn execute_examples(
    root: &Path,
    cargo: &Path,
    target_dir: &Path,
    examples: &[Example],
) -> Result<Vec<ExampleExecution>, String> {
    execute_examples_with_metadata(root, cargo, target_dir, examples, None)
}

pub fn execute_examples_with_metadata(
    root: &Path,
    cargo: &Path,
    target_dir: &Path,
    examples: &[Example],
    owner_metadata: Option<&BTreeMap<String, Metadata>>,
) -> Result<Vec<ExampleExecution>, String> {
    execute_examples_with_context(root, cargo, target_dir, examples, owner_metadata, None)
}

pub fn execute_examples_with_context(
    root: &Path,
    cargo: &Path,
    target_dir: &Path,
    examples: &[Example],
    owner_metadata: Option<&BTreeMap<String, Metadata>>,
    captured: Option<&CapturedSource>,
) -> Result<Vec<ExampleExecution>, String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let capture = || {
        if owner_metadata.is_some() {
            super::imported_source_file_hashes(&root, target_dir)
        } else {
            super::source_file_hashes(&root, target_dir)
        }
    };
    let source_files = capture().map_err(|e| e.to_string())?;
    let mut results = Vec::new();
    for example in examples {
        let source = root
            .join(&example.source_path)
            .canonicalize()
            .map_err(|e| e.to_string())?;
        if !source.starts_with(&root) {
            return Err("historical example source escapes source root".into());
        }
        if super::sha256_file(&root.join(&example.source_path)).map_err(|e| e.to_string())?
            != example.source_sha256
        {
            return Err("historical example source changed before execution".into());
        }
        let mut command = Command::new(cargo);
        let manifest = if let Some(owners) = owner_metadata {
            let metadata = owners
                .get(&example.package)
                .ok_or("historical example owner metadata is absent")?;
            metadata
                .packages
                .iter()
                .find(|package| package.name.as_ref() == example.package)
                .ok_or("historical example Cargo owner is absent")?
                .manifest_path
                .clone()
                .into_std_path_buf()
        } else {
            root.join("Cargo.toml")
        };
        command
            .current_dir(if owner_metadata.is_some() {
                manifest
                    .parent()
                    .ok_or("archive example manifest has no parent")?
            } else {
                &root
            })
            .args(["run", "--locked", "--quiet", "--manifest-path"])
            .arg(&manifest)
            .args(["--package", &example.package, "--example", &example.target])
            .arg("--target-dir")
            .arg(target_dir);
        let config = producer_config(&root, captured, &example.package)?;
        sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(cargo),
            config_path: config.as_deref(),
        }
        .configure(&mut command);
        if !example.required_features.is_empty() {
            command
                .arg("--features")
                .arg(example.required_features.join(","));
        }
        if let Some(directory) = cargo.parent() {
            command.env(
                "RUSTC",
                directory.join(if cfg!(windows) { "rustc.exe" } else { "rustc" }),
            );
        }
        command
            .env_remove("RUSTC_WRAPPER")
            .env_remove("RUSTC_WORKSPACE_WRAPPER");
        let output = command.output().map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(format!(
                "historical example {}::{} failed: {}",
                example.package,
                example.target,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        if super::sha256_file(&root.join(&example.source_path)).map_err(|e| e.to_string())?
            != example.source_sha256
        {
            return Err("historical example source changed during execution".into());
        }
        if capture().map_err(|e| e.to_string())? != source_files {
            return Err("historical source closure changed during example execution".into());
        }
        results.push(ExampleExecution {
            example: example.clone(),
            stdout_sha256: super::sha256_bytes(&output.stdout),
            stderr_sha256: super::sha256_bytes(&output.stderr),
        });
    }
    Ok(results)
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExampleExecution {
    pub example: Example,
    pub stdout_sha256: String,
    pub stderr_sha256: String,
}

fn validate_version(version: &str) -> Result<(), String> {
    if version.is_empty()
        || !version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+'))
    {
        return Err("invalid registry version".into());
    }
    Ok(())
}
fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err("invalid registry package name".into());
    }
    Ok(())
}
fn command_output(command: &mut Command) -> Result<Vec<u8>, String> {
    let output = command.output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    Ok(output.stdout)
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "sdk-historical-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(root.join("src")).unwrap();
            fs::create_dir_all(root.join("examples")).unwrap();
            fs::write(root.join("Cargo.toml"), "[package]\nname='historical-fixture'\nversion='0.1.0'\nedition='2021'\n[workspace]\n[features]\nold-feature=[]\n[[example]]\nname='old-example'\nrequired-features=['old-feature']\n").unwrap();
            fs::write(
                root.join("src/lib.rs"),
                "//! Historical source.\npub fn value() -> u8 { 7 }\n",
            )
            .unwrap();
            fs::write(
                root.join("examples/old-example.rs"),
                "fn main() { println!(\"{}\", historical_fixture::value()); }\n",
            )
            .unwrap();
            fs::write(root.join("Cargo.lock"), "# This file is automatically @generated by Cargo.\nversion = 4\n[[package]]\nname = \"historical-fixture\"\nversion = \"0.1.0\"\n").unwrap();
            Self(root)
        }
        fn metadata(&self) -> Metadata {
            sdk_docs::rustdoc_profiles::load_metadata_with_cargo(
                self.0.join("Cargo.toml"),
                Some(&cargo()),
            )
            .unwrap()
        }
        fn archive(&self, revision: &str, vcs_path: &str) -> (RegistryVersion, PathBuf) {
            self.archive_with_dirty(revision, vcs_path, false)
        }
        fn archive_with_dirty(
            &self,
            revision: &str,
            vcs_path: &str,
            dirty: bool,
        ) -> (RegistryVersion, PathBuf) {
            let entry = self.0.join("historical-fixture-0.1.0");
            fs::create_dir_all(&entry).unwrap();
            fs::write(
                entry.join(".cargo_vcs_info.json"),
                serde_json::to_vec(
                    &serde_json::json!({"git":{"sha1": revision,"dirty":dirty}, "path_in_vcs": vcs_path}),
                )
                .unwrap(),
            )
            .unwrap();
            fs::create_dir_all(entry.join("src")).unwrap();
            fs::create_dir_all(entry.join("examples")).unwrap();
            fs::copy(self.0.join("src/lib.rs"), entry.join("src/lib.rs")).unwrap();
            fs::copy(
                self.0.join("examples/old-example.rs"),
                entry.join("examples/old-example.rs"),
            )
            .unwrap();
            fs::copy(self.0.join("Cargo.toml"), entry.join("Cargo.toml.orig")).unwrap();
            let archive = self.0.join("release.crate");
            command_output(
                Command::new("tar")
                    .arg("-cf")
                    .arg(&archive)
                    .arg("-C")
                    .arg(&self.0)
                    .arg("historical-fixture-0.1.0"),
            )
            .unwrap();
            let checksum = super::super::sha256_file(&archive)
                .unwrap()
                .trim_start_matches("sha256:")
                .to_owned();
            (
                RegistryVersion {
                    package: "historical-fixture".into(),
                    num: "0.1.0".into(),
                    checksum,
                    yanked: true,
                },
                archive,
            )
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn cargo() -> PathBuf {
        std::env::var_os("CARGO")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("cargo"))
    }
    #[test]
    fn separate_producer_resolution_retains_original_lock_and_rejects_config_inventory_drift() {
        let fixture = Fixture::new();
        fs::write(
            fixture.0.join("Cargo.lock"),
            "broken original published lock\n",
        )
        .unwrap();
        let original = fs::read(fixture.0.join("Cargo.lock")).unwrap();
        let directory = "historical-fixture-0.1.0";
        let prefix = format!(".docs-producer/{directory}/");
        let producer_dir = fixture.0.join(&prefix);
        fs::create_dir_all(&producer_dir).unwrap();
        let external_lock = producer_dir.join("Cargo.lock");
        let external_lock = external_lock
            .parent()
            .unwrap()
            .canonicalize()
            .unwrap()
            .join("Cargo.lock");
        let config = format!("[resolver]\nlockfile-path = \"{directory}/Cargo.lock\"\n");
        fs::write(producer_dir.join("config.toml"), &config).unwrap();
        let cargo = cargo();
        let config_path = producer_dir.join("config.toml");
        let context = sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(&cargo),
            config_path: Some(&config_path),
        };
        let mut generate = Command::new(&cargo);
        generate
            .args(["generate-lockfile", "--offline", "--manifest-path"])
            .arg(fixture.0.join("Cargo.toml"));
        context.configure(&mut generate);
        command_output(&mut generate).unwrap();
        assert_eq!(fs::read(fixture.0.join("Cargo.lock")).unwrap(), original);
        assert!(external_lock.is_file());
        let mut producer = ProducerResolution {
            lock_path: format!("{prefix}Cargo.lock"),
            lock_sha256: super::super::sha256_file(&external_lock).unwrap(),
            config_path: format!("{prefix}config.toml"),
            config_sha256: super::super::sha256_file(&producer_dir.join("config.toml")).unwrap(),
        };
        assert_eq!(
            verify_producer_resolution(&fixture.0, directory, &producer)
                .unwrap()
                .len(),
            2
        );
        let relocated = Fixture::new();
        let relocated_producer = relocated.0.join(&prefix);
        fs::create_dir_all(&relocated_producer).unwrap();
        fs::copy(&external_lock, relocated_producer.join("Cargo.lock")).unwrap();
        fs::write(relocated_producer.join("config.toml"), &config).unwrap();
        assert_eq!(
            verify_producer_resolution(&relocated.0, directory, &producer).unwrap(),
            verify_producer_resolution(&fixture.0, directory, &producer).unwrap()
        );
        fs::write(relocated.0.join("Cargo.lock"), &original).unwrap();
        let relocated_config = relocated_producer.join("config.toml");
        let relocated_context = sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(&cargo),
            config_path: Some(&relocated_config),
        };
        let relocated_metadata = sdk_docs::rustdoc_profiles::load_metadata_with_context(
            relocated.0.join("Cargo.toml"),
            relocated_context,
        )
        .unwrap();
        let metadata = sdk_docs::rustdoc_profiles::load_metadata_with_context(
            fixture.0.join("Cargo.toml"),
            context,
        )
        .unwrap();
        assert_eq!(
            metadata.packages[0].name,
            relocated_metadata.packages[0].name
        );
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
            features: BTreeSet::new(),
        };
        for (root, metadata, context) in [
            (&fixture.0, &metadata, context),
            (&relocated.0, &relocated_metadata, relocated_context),
        ] {
            sdk_docs::rustdoc_profiles::execute_target_profile_with_context(
                root.join("Cargo.toml"),
                metadata,
                &profile,
                &BTreeSet::from([host.clone()]),
                root.join("target"),
                root.join("receipt.json"),
                context,
                &sdk_docs::rustdoc_profiles::RustdocTarget::Library,
            )
            .unwrap();
            assert_eq!(fs::read(root.join("Cargo.lock")).unwrap(), original);
        }
        assert_eq!(fs::read(relocated.0.join("Cargo.lock")).unwrap(), original);
        let (mut release, archive) = fixture.archive(&"a".repeat(40), "");
        let imported_lock = fixture.0.join(directory).join("Cargo.lock");
        fs::write(&imported_lock, &original).unwrap();
        command_output(
            Command::new("tar")
                .arg("-cf")
                .arg(&archive)
                .arg("-C")
                .arg(&fixture.0)
                .arg(directory),
        )
        .unwrap();
        release.checksum = super::super::sha256_file(&archive)
            .unwrap()
            .trim_start_matches("sha256:")
            .into();
        let mut identity = ArchiveSource {
            package: release.package.clone(),
            version: release.num.clone(),
            registry_checksum: release.checksum.clone(),
            publisher_vcs: Some(PublisherVcs {
                revision: "a".repeat(40),
                dirty: false,
                path_in_vcs: String::new(),
            }),
            resolution_lock: ResolutionLock::SeparateDocsProducer {
                original_lock: OriginalLock::Published {
                    sha256: super::super::sha256_bytes(&original),
                },
                producer: producer.clone(),
            },
        };
        let captured = CapturedSource::RegistryArchives {
            archives: vec![identity.clone()],
        };
        let owners = BTreeMap::from([("historical-fixture".into(), metadata.clone())]);
        let discovered = examples(
            &metadata,
            &fixture.0,
            &BTreeSet::from(["historical-fixture".into()]),
        )
        .unwrap();
        let executed = execute_examples_with_context(
            &fixture.0,
            &cargo,
            &fixture.0.join("target"),
            &discovered,
            Some(&owners),
            Some(&captured),
        )
        .unwrap();
        assert_eq!(executed.len(), 1);
        assert_eq!(fs::read(fixture.0.join("Cargo.lock")).unwrap(), original);
        let (_, inputs) =
            verify_imported_archive(&fixture.0, &release, &archive, &identity).unwrap();
        assert_eq!(inputs.get(&producer.lock_path), Some(&producer.lock_sha256));
        fs::write(&imported_lock, "changed original lock").unwrap();
        assert!(verify_imported_archive(&fixture.0, &release, &archive, &identity).is_err());
        fs::write(&imported_lock, &original).unwrap();
        let ResolutionLock::SeparateDocsProducer { original_lock, .. } =
            &mut identity.resolution_lock
        else {
            unreachable!()
        };
        *original_lock = OriginalLock::Absent;
        assert!(verify_imported_archive(&fixture.0, &release, &archive, &identity).is_err());
        fs::write(producer_dir.join("extra.json"), "{}").unwrap();
        assert!(verify_producer_resolution(&fixture.0, directory, &producer).is_err());
        fs::remove_file(producer_dir.join("extra.json")).unwrap();
        fs::write(&external_lock, "changed producer lock").unwrap();
        assert!(verify_producer_resolution(&fixture.0, directory, &producer).is_err());
        fs::write(&external_lock, &original).unwrap();
        producer.lock_sha256 = super::super::sha256_file(&external_lock).unwrap();
        fs::write(
            producer_dir.join("config.toml"),
            format!("{config}[net]\noffline=true\n"),
        )
        .unwrap();
        producer.config_sha256 =
            super::super::sha256_file(&producer_dir.join("config.toml")).unwrap();
        assert!(verify_producer_resolution(&fixture.0, directory, &producer)
            .unwrap_err()
            .contains("only the captured"));
        fs::write(producer_dir.join("config.toml"), &config).unwrap();
        producer.config_sha256 =
            super::super::sha256_file(&producer_dir.join("config.toml")).unwrap();
        producer.lock_path = "../Cargo.lock".into();
        assert!(verify_producer_resolution(&fixture.0, directory, &producer).is_err());
        assert_eq!(fs::read(fixture.0.join("Cargo.lock")).unwrap(), original);
        producer.lock_path = format!("{prefix}Cargo.lock");
        fs::remove_file(producer_dir.join("config.toml")).unwrap();
        assert!(verify_producer_resolution(&fixture.0, directory, &producer).is_err());
    }

    #[test]
    fn archive_requires_checksum_vcs_and_exact_cargo_identity() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata();
        let revision = "a".repeat(40);
        let (release, archive) = fixture.archive(&revision, "");
        let accepted =
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive).unwrap();
        assert!(accepted.yanked);
        fs::write(
            fixture.0.join("src/lib.rs"),
            "//! Divergent checkout Rust bytes.\n",
        )
        .unwrap();
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("source bytes differ")
        );
        fs::copy(
            fixture.0.join("historical-fixture-0.1.0/src/lib.rs"),
            fixture.0.join("src/lib.rs"),
        )
        .unwrap();
        let (dirty, dirty_archive) = fixture.archive_with_dirty(&revision, "", true);
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &dirty, &dirty_archive)
                .unwrap_err()
                .contains("dirty source")
        );
        let (release, archive) = fixture.archive(&revision, "");
        let mut wrong = release.clone();
        wrong.checksum = "b".repeat(64);
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &wrong, &archive)
                .unwrap_err()
                .contains("checksum differs")
        );
        assert!(
            verify_archive(&metadata, &fixture.0, &"c".repeat(40), &release, &archive)
                .unwrap_err()
                .contains("revision differs")
        );
        let (escaping, archive) = fixture.archive(&revision, "../outside");
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &escaping, &archive)
                .unwrap_err()
                .contains("escapes")
        );
        let (release, archive) = fixture.archive(&revision, "examples");
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("differs from Cargo")
        );
        let mut metadata = metadata;
        metadata.packages[0].version = "0.1.1".parse().unwrap();
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("matching historical Cargo")
        );
        let mut invalid = release;
        invalid.num = "../0.1.0".into();
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &invalid, &archive)
                .unwrap_err()
                .contains("invalid registry version")
        );
    }
    #[test]
    fn archive_rejects_missing_target_source_and_changed_original_manifest() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata();
        let revision = "a".repeat(40);
        let (mut release, archive) = fixture.archive(&revision, "");
        fs::remove_file(fixture.0.join("historical-fixture-0.1.0/src/lib.rs")).unwrap();
        command_output(
            Command::new("tar")
                .arg("-cf")
                .arg(&archive)
                .arg("-C")
                .arg(&fixture.0)
                .arg("historical-fixture-0.1.0"),
        )
        .unwrap();
        release.checksum = super::super::sha256_file(&archive)
            .unwrap()
            .trim_start_matches("sha256:")
            .into();
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("missing Cargo Rust source")
        );
        let (release, archive) = fixture.archive(&revision, "");
        fs::write(fixture.0.join("Cargo.toml"), "different manifest").unwrap();
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("source bytes differ")
        );
    }
    #[test]
    fn archive_rejects_unarchived_module_in_maintained_native_source_closure() {
        let fixture = Fixture::new();
        let package = fixture.0.join("rust/crates/historical-fixture");
        fs::create_dir_all(package.join("src")).unwrap();
        fs::create_dir_all(package.join("examples")).unwrap();
        fs::copy(fixture.0.join("Cargo.toml"), package.join("Cargo.toml")).unwrap();
        fs::copy(fixture.0.join("src/lib.rs"), package.join("src/lib.rs")).unwrap();
        fs::copy(
            fixture.0.join("examples/old-example.rs"),
            package.join("examples/old-example.rs"),
        )
        .unwrap();
        fs::write(
            package.join("src/unarchived.rs"),
            "//! Unarchived native source.\n",
        )
        .unwrap();
        let metadata = sdk_docs::rustdoc_profiles::load_metadata_with_cargo(
            package.join("Cargo.toml"),
            Some(&cargo()),
        )
        .unwrap();
        let revision = "a".repeat(40);
        let (release, archive) = fixture.archive(&revision, "rust/crates/historical-fixture");
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("missing native docs source: src/unarchived.rs")
        );
    }
    #[test]
    fn actual_cargo_examples_keep_required_features_execute_and_reject_drift_failure() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata();
        let owners = BTreeSet::from(["historical-fixture".into()]);
        let discovered = examples(&metadata, &fixture.0, &owners).unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].target, "old-example");
        assert_eq!(discovered[0].required_features, vec!["old-feature"]);
        let execution =
            execute_examples(&fixture.0, &cargo(), &fixture.0.join("target"), &discovered).unwrap();
        assert_eq!(
            execution[0].stdout_sha256,
            super::super::sha256_bytes(b"7\n")
        );
        fs::write(
            fixture.0.join("examples/old-example.rs"),
            "fn main() { panic!(\"failure\"); }\n",
        )
        .unwrap();
        assert!(
            execute_examples(&fixture.0, &cargo(), &fixture.0.join("target"), &discovered)
                .unwrap_err()
                .contains("changed before execution")
        );
        let failing = examples(&metadata, &fixture.0, &owners).unwrap();
        assert!(
            execute_examples(&fixture.0, &cargo(), &fixture.0.join("target"), &failing)
                .unwrap_err()
                .contains("failed")
        );
        fs::write(fixture.0.join("examples/old-example.rs"), r#"fn main() { std::fs::write("examples/old-example.rs", "fn main() {}\n").unwrap(); }"#).unwrap();
        let mutating = examples(&metadata, &fixture.0, &owners).unwrap();
        assert!(
            execute_examples(&fixture.0, &cargo(), &fixture.0.join("target"), &mutating)
                .unwrap_err()
                .contains("changed during execution")
        );
        assert!(examples(
            &metadata,
            &fixture.0,
            &BTreeSet::from(["modern-absent-owner".into()])
        )
        .unwrap_err()
        .contains("absent"));
    }
    #[test]
    fn cargo_binary_target_produces_native_receipt_without_public_api_admission() {
        use sdk_docs::rustdoc_profiles::{
            execute_target_profile_with_cargo, observe_rustdoc, ProfileSpec, RustdocTarget,
        };
        let fixture = Fixture::new();
        fs::write(
            fixture.0.join("src/main.rs"),
            "//! Source-owned historical command instructions.\nfn main() {}\n",
        )
        .unwrap();
        let metadata = fixture.metadata();
        let rustc =
            cargo()
                .parent()
                .unwrap()
                .join(if cfg!(windows) { "rustc.exe" } else { "rustc" });
        let output = command_output(Command::new(rustc).arg("-vV")).unwrap();
        let info = String::from_utf8(output).unwrap();
        let host = info
            .lines()
            .find_map(|line| line.strip_prefix("host: "))
            .unwrap()
            .to_owned();
        let profile = ProfileSpec {
            package: "historical-fixture".into(),
            target: host.clone(),
            default_features: true,
            features: BTreeSet::new(),
        };
        let output = fixture.0.join("binary.json");
        let target_dir = fixture.0.join("native-rustdoc");
        let absent = execute_target_profile_with_cargo(
            fixture.0.join("Cargo.toml"),
            &metadata,
            &profile,
            &BTreeSet::from([host.clone()]),
            &target_dir,
            &output,
            Some(&cargo()),
            &RustdocTarget::Binary("absent".into()),
        );
        assert!(absent
            .unwrap_err()
            .to_string()
            .contains("absent from Cargo"));
        assert!(!target_dir.exists());
        let observation = execute_target_profile_with_cargo(
            fixture.0.join("Cargo.toml"),
            &metadata,
            &profile,
            &BTreeSet::from([host]),
            &target_dir,
            &output,
            Some(&cargo()),
            &RustdocTarget::Binary("historical-fixture".into()),
        )
        .unwrap();
        assert_eq!(observation.format_version, 60);
        assert_eq!(observation.crate_version.as_deref(), Some("0.1.0"));
        assert!(observation.includes_private);
        assert!(observe_rustdoc(&output).is_err());
        let native: serde_json::Value = serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
        assert_eq!(
            native["index"][native["root"].to_string()]["docs"],
            "Source-owned historical command instructions."
        );
    }
    #[test]
    fn archive_checks_embedded_binary_json_proto_and_build_configuration() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata();
        let revision = "a".repeat(40);
        for name in [
            "descriptor.bin",
            "input.json",
            "schema.proto",
            "build.rs",
            ".cargo/config.toml",
        ] {
            let path = fixture.0.join(name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, b"original archived input").unwrap();
            let (mut release, archive) = fixture.archive(&revision, "");
            let archived = fixture.0.join("historical-fixture-0.1.0").join(name);
            fs::create_dir_all(archived.parent().unwrap()).unwrap();
            fs::copy(&path, &archived).unwrap();
            command_output(
                Command::new("tar")
                    .arg("-cf")
                    .arg(&archive)
                    .arg("-C")
                    .arg(&fixture.0)
                    .arg("historical-fixture-0.1.0"),
            )
            .unwrap();
            release.checksum = super::super::sha256_file(&archive)
                .unwrap()
                .trim_start_matches("sha256:")
                .into();
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive).unwrap();
            assert!(
                archive_source_files(&metadata, &fixture.0, &release, &archive)
                    .unwrap()
                    .contains_key(name)
            );
            fs::write(&path, b"changed source input").unwrap();
            assert!(
                verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                    .unwrap_err()
                    .contains("source bytes differ")
            );
            fs::copy(&archived, &path).unwrap();
        }
    }

    #[test]
    fn executed_examples_reject_library_build_script_readme_manifest_and_lock_mutations() {
        for changed in [
            "rust/crates/owned/src/lib.rs",
            "rust/crates/owned/build.rs",
            "rust/crates/owned/README.md",
            "Cargo.toml",
            "Cargo.lock",
        ] {
            let fixture = Fixture::new();
            fs::create_dir_all(fixture.0.join("rust/crates/owned/src")).unwrap();
            fs::copy(
                fixture.0.join("src/lib.rs"),
                fixture.0.join("rust/crates/owned/src/lib.rs"),
            )
            .unwrap();
            fs::write(
                fixture.0.join("rust/crates/owned/build.rs"),
                "fn main() {}\n",
            )
            .unwrap();
            fs::write(
                fixture.0.join("rust/crates/owned/README.md"),
                "# Source README\n",
            )
            .unwrap();
            let manifest = fs::read_to_string(fixture.0.join("Cargo.toml")).unwrap().replace("edition='2021'", "edition='2021'\nbuild='rust/crates/owned/build.rs'\nreadme='rust/crates/owned/README.md'");
            fs::write(
                fixture.0.join("Cargo.toml"),
                format!("{manifest}\n[lib]\npath='rust/crates/owned/src/lib.rs'\n"),
            )
            .unwrap();
            fs::write(fixture.0.join("examples/old-example.rs"), format!("fn main() {{ std::fs::write({changed:?}, \"mutated documentation input\").unwrap(); }}\n")).unwrap();
            let metadata = fixture.metadata();
            let examples = examples(
                &metadata,
                &fixture.0,
                &BTreeSet::from(["historical-fixture".into()]),
            )
            .unwrap();
            let error =
                execute_examples(&fixture.0, &cargo(), &fixture.0.join("target"), &examples)
                    .unwrap_err();
            assert!(
                error.contains("source closure changed"),
                "{changed}: {error}"
            );
        }
    }
}
