//! Historical planning uses released archives and Cargo-owned sources.
use cargo_metadata::{Metadata, TargetKind};
use sdk_docs::historical::{
    ArchiveSource, CapturedSource, OriginalLock, ProducerDependencyArchive, ProducerResolution,
    PublisherVcs, ReleasedPackage, ResolutionLock,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Component, Path};
use std::process::{Command, Stdio};

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

/// Cargo permits crates.io by default or when its registry name is explicit.
/// A private-only publish allowlist cannot establish a crates.io release owner.
fn crates_io_eligible(publish: Option<&[String]>) -> bool {
    publish.is_none_or(|registries| registries.iter().any(|registry| registry == "crates-io"))
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
        package.version.to_string() == version && crates_io_eligible(package.publish.as_deref())
    }) {
        let Some(release) = registry_version_optional(package.name.as_ref(), version)? else {
            continue;
        };
        let name = format!("{}-{version}.crate", package.name);
        expected.insert(name.clone());
        let archive = archive_dir.join(name);
        let (identity, files) = verify_archive(metadata, root, revision, &release, &archive)?;
        result.source_files.extend(files);
        result.released_packages.push(identity);
        result.archives.push((archive, release));
    }
    if result.released_packages.is_empty() {
        return Err(
            "historical scope has no actual registry releases for this source/version".into(),
        );
    }
    validate_archive_inventory(archive_dir, &expected)?;
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
    archive_plan_with_inventory(version, archive_dir, source_root, cargo, None)
}

pub fn archive_plan_with_inventory(
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
                    lock_selection: Some(
                        sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(&config),
                    ),
                }
                .configure(&mut command)
                .map_err(|e| e.to_string())?;
                command_output(&mut command)?;
            }
            let producer = ProducerResolution {
                lock_path: format!(".docs-producer/{directory}/Cargo.lock"),
                lock_sha256: super::sha256_file(&producer_lock).map_err(|e| e.to_string())?,
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
        } else if members.contains_key("Cargo.lock") {
            ResolutionLock::Published {
                sha256: super::sha256_file(&lock).map_err(|e| e.to_string())?,
            }
        } else {
            // Reuse an already captured producer resolution; the locked Cargo
            // metadata/profile commands below validate it without updating it.
            if !lock.exists() {
                let mut command = Command::new(cargo);
                command
                    .args(["generate-lockfile", "--manifest-path"])
                    .arg(&manifest);
                sdk_docs::rustdoc_profiles::CargoExecutionContext {
                    cargo_path: Some(cargo),
                    lock_selection: Some(
                        sdk_docs::rustdoc_profiles::LockSelection::SourceAdjacent(&lock),
                    ),
                }
                .configure(&mut command)
                .map_err(|e| e.to_string())?;
                command_output(&mut command)?;
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
/// Cargo overrides. Patches are restricted to checksum-verified dependency archives.
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
        let digest = super::sha256_file(&path).map_err(|e| e.to_string())?;
        if &digest != expected {
            return Err(format!("retained producer input differs: {logical}"));
        }
        files.insert(logical.clone(), digest);
    }
    validate_producer_config(&root, directory, &producer.dependency_archives)?;
    Ok(files)
}

fn validate_producer_config(
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

fn producer_config_bytes(
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

fn producer_dependencies(
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
            super::sha256_file(&path).map_err(|e| e.to_string())?,
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
                super::write_immutable(&path, &bytes).map_err(|e| e.to_string())?;
            }
            if path.canonicalize().map_err(|e| e.to_string())? != path
                || fs::read(path).map_err(|e| e.to_string())? != bytes
            {
                return Err("dependency source differs or redirects".into());
            }
            files.insert(logical, super::sha256_bytes(&bytes));
        }
    }
    Ok((dependencies, files))
}

fn verify_producer_metadata(
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
    let mut command = Command::new(cargo);
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
        let metadata = sdk_docs::rustdoc_profiles::load_metadata_with_context(
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

fn is_linked(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return true;
        }
    }
    metadata.file_type().is_symlink()
}

fn validate_import_path(root: &Path, path: &Path) -> Result<(), String> {
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

/// SDK candidates belong to the original release authority and Cargo workspace.
/// The declared frontier is explicit; it does not prove unseen Git history.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RegistryOwnerSource {
    pub revision: String,
    pub input_sha256: BTreeMap<String, String>,
    /// Original Git link blobs retained as opaque regular files, never followed.
    pub symlink_paths: BTreeSet<String>,
    pub discovery: CargoDiscoveryInputs,
    pub owners: BTreeSet<String>,
    pub outside_release_authority: BTreeSet<String>,
    pub uses_frontier_authority: bool,
    pub absent_authority_members: BTreeSet<String>,
}

/// Discovery transport/config inputs are producer evidence, not archive identity.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct CargoDiscoveryInputs {
    git_sha256: String,
    cargo_sha256: String,
    cargo_version: String,
    config_sha256: BTreeMap<String, Option<String>>,
    environment_sha256: BTreeMap<String, String>,
}

fn frontier_git() -> Result<std::path::PathBuf, String> {
    std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .map(|path| path.join(if cfg!(windows) { "git.exe" } else { "git" }))
                .find(|path| path.is_file())
        })
        .ok_or("source frontier Git is absent from PATH".into())
}

// One maintained Git process reads exact ls-tree IDs without export-ignore,
// textconv, filters or replacement-object substitution.
fn git_source_blobs(
    repository: &Path,
    listing: &[u8],
    consume: impl FnMut(&str, &[u8]) -> Result<(), String>,
) -> Result<(), String> {
    let mut child = Command::new(frontier_git()?)
        .arg("--no-replace-objects")
        .current_dir(repository)
        .args(["cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;
    read_git_source_blobs(&mut child, listing, consume)
}

fn read_git_source_blobs(
    child: &mut std::process::Child,
    listing: &[u8],
    mut consume: impl FnMut(&str, &[u8]) -> Result<(), String>,
) -> Result<(), String> {
    let result: Result<(), String> = (|| {
        let mut input = child.stdin.take().ok_or("Git batch stdin is absent")?;
        let mut output = BufReader::new(child.stdout.take().ok_or("Git batch stdout is absent")?);
        for entry in listing
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
        {
            let entry = std::str::from_utf8(entry).map_err(|e| e.to_string())?;
            let (metadata, path) = entry
                .split_once('\t')
                .ok_or("invalid original Git tree entry")?;
            let fields: Vec<_> = metadata.split_whitespace().collect();
            let [mode, kind, object] = fields.as_slice() else {
                return Err("invalid original Git tree metadata".into());
            };
            if !matches!(*mode, "100644" | "100755" | "120000")
                || *kind != "blob"
                || object.len() != 40
                || !object.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err("original source frontier contains a linked or non-file member".into());
            }
            writeln!(input, "{object}").map_err(|e| e.to_string())?;
            input.flush().map_err(|e| e.to_string())?;
            let mut header = String::new();
            output.read_line(&mut header).map_err(|e| e.to_string())?;
            let fields: Vec<_> = header.split_whitespace().collect();
            let [actual, kind, size] = fields.as_slice() else {
                return Err("invalid Git batch object header".into());
            };
            if actual != object || *kind != "blob" {
                return Err("Git batch object identity differs from original tree".into());
            }
            let mut bytes = vec![0; size.parse::<usize>().map_err(|e| e.to_string())?];
            output.read_exact(&mut bytes).map_err(|e| e.to_string())?;
            let mut separator = [0];
            output
                .read_exact(&mut separator)
                .map_err(|e| e.to_string())?;
            if separator != *b"\n" {
                return Err("invalid Git batch object boundary".into());
            }
            consume(path, &bytes)?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = child.kill();
    }
    let status = child.wait().map_err(|e| e.to_string())?;
    result?;
    if !status.success() {
        return Err("Git batch source reader failed".into());
    }
    Ok(())
}

fn discovery_environment_inputs(
    inputs: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
    windows: bool,
) -> Result<BTreeMap<String, String>, String> {
    // Retain digests, never raw credential-bearing environment values.
    let mut environment_sha256 = BTreeMap::new();
    for (key, value) in inputs {
        let mut key = key
            .into_string()
            .map_err(|_| "non-UTF8 discovery environment key")?;
        // Windows environment names are case-insensitive and commonly expose
        // PATH as `Path`; retain the same discovery authority on both hosts.
        if windows {
            key.make_ascii_uppercase();
        }
        if key == "PATH"
            || key.starts_with("GIT_")
            || key.starts_with("CARGO_")
            || key.starts_with("RUSTUP_")
            || key == "HOME"
            || key == "USERPROFILE"
        {
            let value = value
                .into_string()
                .map_err(|_| "non-UTF8 discovery environment value")?;
            let digest = super::sha256_bytes(value.as_bytes());
            if environment_sha256
                .insert(key, digest.clone())
                .is_some_and(|previous| previous != digest)
            {
                return Err("conflicting case aliases in Cargo discovery environment".into());
            }
        }
    }
    Ok(environment_sha256)
}

fn cargo_discovery_inputs(root: &Path, cargo: &Path) -> Result<CargoDiscoveryInputs, String> {
    let mut config_sha256 = BTreeMap::new();
    let home = std::env::var_os("CARGO_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
                .map(|home| std::path::PathBuf::from(home).join(".cargo"))
        })
        .ok_or("Cargo discovery home is absent")?;
    // Record both accepted spellings, including absence. A newly introduced
    // ancestor/home config must not silently change workspace discovery.
    for directory in root
        .ancestors()
        .map(|path| path.join(".cargo"))
        .chain([home])
    {
        for name in ["config", "config.toml"] {
            let path = directory.join(name);
            let digest = if path.try_exists().map_err(|e| e.to_string())? {
                let metadata = fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
                if !metadata.is_file() || is_linked(&metadata) {
                    return Err("Cargo discovery config is redirected or non-file".into());
                }
                Some(super::sha256_file(&path).map_err(|e| e.to_string())?)
            } else {
                None
            };
            config_sha256.insert(super::path_string(&path), digest);
        }
    }
    let environment_sha256 = discovery_environment_inputs(std::env::vars_os(), cfg!(windows))?;
    Ok(CargoDiscoveryInputs {
        git_sha256: super::sha256_file(&frontier_git()?).map_err(|e| e.to_string())?,
        cargo_sha256: super::sha256_file(cargo).map_err(|e| e.to_string())?,
        cargo_version: String::from_utf8(command_output(
            Command::new(cargo).current_dir(root).arg("-vV"),
        )?)
        .map_err(|e| e.to_string())?,
        config_sha256,
        environment_sha256,
    })
}

fn registry_owner_source_inner(
    repository: &Path,
    root: &Path,
    revision: &str,
    cargo: &Path,
    capture: bool,
    frontier_authority: Option<&BTreeSet<String>>,
) -> Result<RegistryOwnerSource, String> {
    if revision.len() != 40 || !revision.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("registry owner source requires an immutable Git revision".into());
    }
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let repository = repository.canonicalize().map_err(|e| e.to_string())?;
    let top = command_output(
        Command::new(frontier_git()?)
            .arg("--no-replace-objects")
            .current_dir(&repository)
            .args(["rev-parse", "--show-toplevel"]),
    )?;
    if Path::new(std::str::from_utf8(&top).map_err(|e| e.to_string())?.trim())
        .canonicalize()
        .map_err(|e| e.to_string())?
        != repository
    {
        return Err("registry owner discovery requires the complete source root".into());
    }
    let listing = command_output(
        Command::new(frontier_git()?)
            .arg("--no-replace-objects")
            .current_dir(&repository)
            .args(["ls-tree", "-r", "-z", revision]),
    )?;
    let mut source = RegistryOwnerSource {
        revision: revision.into(),
        input_sha256: BTreeMap::new(),
        symlink_paths: listing
            .split(|byte| *byte == 0)
            .filter_map(|entry| {
                let entry = std::str::from_utf8(entry).ok()?;
                let (metadata, path) = entry.split_once('\t')?;
                (metadata.split_whitespace().next() == Some("120000")).then(|| path.to_owned())
            })
            .collect(),
        discovery: CargoDiscoveryInputs::default(),
        owners: BTreeSet::new(),
        outside_release_authority: BTreeSet::new(),
        uses_frontier_authority: false,
        absent_authority_members: BTreeSet::new(),
    };
    // The complete tracked frontier includes configs, workspace manifests and
    // original release authority; no source-owned input is replaced by a name list.
    git_source_blobs(&repository, &listing, |path, original| {
        let input = root.join(path);
        validate_import_path(&root, &input)?;
        if capture {
            super::write_immutable(&input, original).map_err(|e| e.to_string())?;
        }
        if fs::read(&input).map_err(|e| e.to_string())?.as_slice() != original {
            return Err("registry owner input differs from declared Git source".into());
        }
        source
            .input_sha256
            .insert(path.into(), super::sha256_bytes(original));
        Ok(())
    })?;
    // Link blobs preserve the exact Git bytes and mode evidence without
    // constructing host links. They cannot stand in for a Cargo discovery input.
    for path in &source.symlink_paths {
        let path = Path::new(path);
        if path.parent().and_then(Path::file_name) == Some(std::ffi::OsStr::new(".cargo"))
            && matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some("config" | "config.toml")
            )
        {
            return Err("original Git link is a Cargo discovery config".into());
        }
    }
    source.discovery = cargo_discovery_inputs(&root, cargo)?;
    let authority_path = "release/cargo-crates.json";
    let authority = if source.input_sha256.contains_key(authority_path) {
        decode_release_authority(&fs::read(root.join(authority_path)).map_err(|e| e.to_string())?)?
    } else {
        source.uses_frontier_authority = true;
        frontier_authority
            .ok_or("frontier has no original SDK release authority")?
            .clone()
    };
    // Cargo's no-deps branch serializes validated workspace members without
    // build_resolve_graph. Reuse the maintained context, not a workspace glob parser.
    validate_archive_inventory(&root, &source.input_sha256.keys().cloned().collect())?;
    let metadata = cargo_metadata::MetadataCommand::new()
        .cargo_path(cargo)
        .current_dir(&root)
        .manifest_path(root.join("Cargo.toml"))
        .env_remove("CARGO_RESOLVER_LOCKFILE_PATH")
        .other_options(vec!["--locked".into(), "--offline".into()])
        .no_deps()
        .exec()
        .map_err(|e| e.to_string())?;
    if metadata
        .workspace_root
        .as_std_path()
        .canonicalize()
        .map_err(|e| e.to_string())?
        != root
    {
        return Err("Cargo owner inspection selected a different workspace".into());
    }
    let cargo_root = metadata.workspace_root.as_std_path();
    for package in &metadata.packages {
        for path in
            std::iter::once(package.manifest_path.as_std_path())
                .chain(
                    package
                        .targets
                        .iter()
                        .map(|target| target.src_path.as_std_path()),
                )
                .chain(package.dependencies.iter().filter_map(|dependency| {
                    dependency.path.as_ref().map(|path| path.as_std_path())
                }))
        {
            if let Ok(relative) = path.strip_prefix(cargo_root) {
                let mut normalized = std::path::PathBuf::new();
                for component in relative.components() {
                    match component {
                        std::path::Component::Normal(part) => normalized.push(part),
                        std::path::Component::ParentDir => {
                            normalized.pop();
                        }
                        std::path::Component::CurDir => {}
                        _ => {
                            return Err("Cargo discovery input has an invalid relative path".into())
                        }
                    }
                }
                if source
                    .symlink_paths
                    .iter()
                    .any(|link| normalized.starts_with(link))
                {
                    return Err(
                        "original Git link is a Cargo manifest, target, or dependency input".into(),
                    );
                }
            }
        }
    }
    let mut workspace_names = BTreeSet::new();
    for package in &metadata.packages {
        if !metadata.workspace_members.contains(&package.id) {
            continue;
        }
        let manifest = package
            .manifest_path
            .as_std_path()
            .canonicalize()
            .map_err(|e| e.to_string())?;
        validate_import_path(&root, &manifest)?;
        let relative = manifest.strip_prefix(&root).map_err(|e| e.to_string())?;
        if !source
            .input_sha256
            .contains_key(&super::path_string(relative))
        {
            return Err("Cargo member has no original Git manifest".into());
        }
        let name = package.name.to_string();
        validate_name(&name)?;
        if !workspace_names.insert(name.clone()) {
            return Err("original workspace has ambiguous package identities".into());
        }
        if crates_io_eligible(package.publish.as_deref()) {
            if authority.contains(&name) {
                source.owners.insert(name);
            } else {
                source.outside_release_authority.insert(name);
            }
        }
    }
    source.absent_authority_members = authority.difference(&workspace_names).cloned().collect();
    validate_archive_inventory(&root, &source.input_sha256.keys().cloned().collect())?;
    if cargo_discovery_inputs(&root, cargo)? != source.discovery {
        return Err("Cargo discovery tool/config/environment changed during inspection".into());
    }
    for (path, expected) in &source.input_sha256 {
        if super::sha256_file(&root.join(path)).map_err(|e| e.to_string())? != *expected {
            return Err("registry owner source changed during Cargo inspection".into());
        }
    }
    Ok(source)
}

/// Generated historical corpus provenance. This is producer evidence, kept
/// separate from CapturedSource's exact compiled registry archive identity.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RegistryCorpus {
    pub repository: String,
    pub declared_anchors: BTreeSet<String>,
    pub anchors: BTreeSet<String>,
    pub original_release_authority: BTreeSet<String>,
    pub frontier: Vec<RegistryOwnerSource>,
    pub responses: BTreeMap<String, Option<String>>,
    pub publisher_anchors: BTreeMap<String, Option<PublisherVcs>>,
}

/// The caller declares immutable history anchors, never package names. Git
/// derives original release/workspace/package-manifest changes plus anchors.
fn frontier_revisions(
    repository: &Path,
    anchors: &BTreeSet<String>,
) -> Result<BTreeSet<String>, String> {
    if anchors.is_empty()
        || anchors
            .iter()
            .any(|sha| sha.len() != 40 || !sha.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return Err("registry frontier needs immutable Git history anchors".into());
    }
    let git = frontier_git()?;
    let shallow = command_output(
        Command::new(&git)
            .arg("--no-replace-objects")
            .current_dir(repository)
            .args(["rev-parse", "--is-shallow-repository"]),
    )?;
    if std::str::from_utf8(&shallow)
        .map_err(|e| e.to_string())?
        .trim()
        != "false"
    {
        return Err("source frontier requires complete original Git ancestry".into());
    }
    let graft = command_output(Command::new(&git).current_dir(repository).args([
        "rev-parse",
        "--git-path",
        "info/grafts",
    ]))?;
    let graft = repository.join(
        std::str::from_utf8(&graft)
            .map_err(|e| e.to_string())?
            .trim(),
    );
    if graft.try_exists().map_err(|e| e.to_string())?
        || std::env::var_os("GIT_GRAFT_FILE").is_some()
    {
        return Err("source frontier does not accept substituted Git ancestry".into());
    }
    let mut command = Command::new(git);
    command.arg("--no-replace-objects");
    command
        .current_dir(repository)
        .arg("log")
        .arg("--format=%H")
        .args(anchors)
        .args([
            "--",
            "release/cargo-crates.json",
            "Cargo.toml",
            ":(glob)**/Cargo.toml",
        ]);
    let revisions = String::from_utf8(command_output(&mut command)?).map_err(|e| e.to_string())?;
    Ok(anchors
        .iter()
        .cloned()
        .chain(revisions.lines().map(str::to_owned))
        .collect())
}

fn decode_release_authority(bytes: &[u8]) -> Result<BTreeSet<String>, String> {
    let names: Vec<String> = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let authority: BTreeSet<_> = names.iter().cloned().collect();
    if authority.is_empty() || authority.len() != names.len() {
        return Err("original release authority is empty or duplicated".into());
    }
    for name in &authority {
        validate_name(name)?;
    }
    Ok(authority)
}

fn original_frontier_authority(
    repository: &Path,
    revisions: &BTreeSet<String>,
) -> Result<BTreeSet<String>, String> {
    let mut authority = BTreeSet::new();
    for revision in revisions {
        let present = command_output(
            Command::new(frontier_git()?)
                .arg("--no-replace-objects")
                .current_dir(repository)
                .args(["ls-tree", "-z", revision, "--", "release/cargo-crates.json"]),
        )?;
        if !present.is_empty() {
            let bytes = command_output(
                Command::new(frontier_git()?)
                    .arg("--no-replace-objects")
                    .current_dir(repository)
                    .args(["show", &format!("{revision}:release/cargo-crates.json")]),
            )?;
            authority.extend(decode_release_authority(&bytes)?);
        }
    }
    if authority.is_empty() {
        return Err("declared frontier has no original release authority".into());
    }
    Ok(authority)
}
pub fn capture_registry_corpus(
    repository: &Path,
    anchors: BTreeSet<String>,
    frontier_root: &Path,
    cargo: &Path,
) -> Result<RegistryCorpus, String> {
    let repository = repository.canonicalize().map_err(|e| e.to_string())?;
    // Before the first mkdir, anchor at the nearest existing nonlinked parent.
    // Use its physical spelling so Windows extended paths and /var aliases do
    // not weaken the existing no-follow importer guard.
    let mut existing = frontier_root;
    loop {
        match fs::symlink_metadata(existing) {
            Ok(metadata) => {
                if is_linked(&metadata) || !metadata.is_dir() {
                    return Err("registry frontier ancestor is linked or non-directory".into());
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                existing = existing
                    .parent()
                    .ok_or("registry frontier has no owned ancestor")?;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    let physical = existing.canonicalize().map_err(|e| e.to_string())?;
    let destination = physical.join(
        frontier_root
            .strip_prefix(existing)
            .map_err(|e| e.to_string())?,
    );
    validate_import_path(&physical, &destination)?;
    let frontier_root = destination.as_path();
    fs::create_dir_all(frontier_root).map_err(|e| e.to_string())?;
    if is_linked(&fs::symlink_metadata(frontier_root).map_err(|e| e.to_string())?) {
        return Err("registry frontier root is linked".into());
    }
    let frontier_root = frontier_root.canonicalize().map_err(|e| e.to_string())?;
    let mut frontier = Vec::new();
    let revisions = frontier_revisions(&repository, &anchors)?;
    let original_release_authority = original_frontier_authority(&repository, &revisions)?;
    for revision in revisions {
        let root = frontier_root.join(&revision);
        validate_import_path(&frontier_root, &root)?;
        fs::create_dir_all(&root).map_err(|e| e.to_string())?;
        frontier.push(registry_owner_source_inner(
            &repository,
            &root,
            &revision,
            cargo,
            true,
            Some(&original_release_authority),
        )?);
    }
    if frontier.is_empty() {
        return Err("registry source frontier is empty".into());
    }
    let mut responses = BTreeMap::new();
    for owner in frontier
        .iter()
        .flat_map(|source| &source.owners)
        .collect::<BTreeSet<_>>()
    {
        let bytes = registry_response(&format!("{owner}/versions"))?;
        let text = bytes
            .map(String::from_utf8)
            .transpose()
            .map_err(|e| e.to_string())?;
        if let Some(text) = &text {
            decode_registry_versions(owner, text.as_bytes())?;
        }
        responses.insert(owner.clone(), text);
    }
    Ok(RegistryCorpus {
        repository: super::path_string(&repository),
        declared_anchors: anchors.clone(),
        anchors,
        original_release_authority,
        frontier,
        responses,
        publisher_anchors: BTreeMap::new(),
    })
}

fn corpus_inventory(
    corpus: &RegistryCorpus,
) -> Result<BTreeMap<String, RegistryOwnerVersions>, String> {
    corpus
        .responses
        .iter()
        .map(|(owner, response)| {
            let versions = response
                .as_ref()
                .map(|text| decode_registry_versions(owner, text.as_bytes()))
                .transpose()?
                .unwrap_or_default();
            Ok((
                owner.clone(),
                RegistryOwnerVersions {
                    response_sha256: response
                        .as_ref()
                        .map(|text| super::sha256_bytes(text.as_bytes())),
                    versions,
                },
            ))
        })
        .collect()
}

fn cohort_publishers(
    inventory: &BTreeMap<String, RegistryOwnerVersions>,
    version: &str,
    archives: &Path,
) -> Result<BTreeMap<String, Option<PublisherVcs>>, String> {
    validate_registry_cohort(version, inventory, archives)?;
    inventory
        .values()
        .flat_map(|owner| &owner.versions)
        .filter(|release| release.num == version)
        .map(|release| {
            let name = format!("{}-{}.crate", release.package, release.num);
            let vcs = archive_vcs(&archive_members(release, &archives.join(&name))?)?;
            if vcs.as_ref().is_some_and(|vcs| {
                vcs.revision.len() != 40 || !vcs.revision.bytes().all(|b| b.is_ascii_hexdigit())
            }) {
                return Err("published archive has an invalid original Git anchor".into());
            }
            Ok((name, vcs))
        })
        .collect()
}

/// Publisher facts extend the declared scope; they never supply owner authority
/// or retag dirty/divergent archive bytes as clean Git inputs.
pub fn capture_registry_cohort(
    repository: &Path,
    mut anchors: BTreeSet<String>,
    frontier_root: &Path,
    cargo: &Path,
    version: &str,
    archives: &Path,
) -> Result<RegistryCorpus, String> {
    let declared_anchors = anchors.clone();
    loop {
        let mut corpus =
            capture_registry_corpus(repository, anchors.clone(), frontier_root, cargo)?;
        let publishers = cohort_publishers(&corpus_inventory(&corpus)?, version, archives)?;
        let expanded = publisher_anchor_union(&anchors, &publishers);
        if expanded != anchors {
            // Publisher commits commonly already occur in manifest history.
            // Reuse this capture only when the exact immutable revision set is
            // unchanged. Admission and drift still recheck every original export.
            let captured = corpus
                .frontier
                .iter()
                .map(|source| source.revision.clone())
                .collect();
            if frontier_revisions(repository, &expanded)? != captured {
                anchors = expanded;
                continue;
            }
            corpus.anchors = expanded;
        }
        corpus.declared_anchors = declared_anchors;
        corpus.publisher_anchors = publishers;
        return Ok(corpus);
    }
}

fn publisher_anchor_union(
    anchors: &BTreeSet<String>,
    publishers: &BTreeMap<String, Option<PublisherVcs>>,
) -> BTreeSet<String> {
    anchors
        .iter()
        .cloned()
        .chain(
            publishers
                .values()
                .flatten()
                .map(|vcs| vcs.revision.clone()),
        )
        .collect()
}

/// Call before archive_plan and again from the existing drift admission gate.
/// The original Git exports/config context are rechecked through the same
/// source discovery function, not through a second source snapshot engine.
pub fn verify_registry_corpus(
    corpus: &RegistryCorpus,
    frontier_root: &Path,
    cargo: &Path,
    version: &str,
    archives: &Path,
) -> Result<BTreeMap<String, RegistryOwnerVersions>, String> {
    if corpus.frontier.is_empty() {
        return Err("registry source frontier is empty".into());
    }
    if corpus.declared_anchors.is_empty() {
        return Err("registry corpus has no declared original history anchors".into());
    }
    let repository = Path::new(&corpus.repository);
    let expected_revisions = frontier_revisions(repository, &corpus.anchors)?;
    if original_frontier_authority(repository, &expected_revisions)?
        != corpus.original_release_authority
    {
        return Err("original frontier release authority changed".into());
    }
    let mut revisions = BTreeSet::new();
    let mut owners = BTreeSet::new();
    for source in &corpus.frontier {
        if !revisions.insert(&source.revision) {
            return Err("duplicate source frontier revision".into());
        }
        let current = registry_owner_source_inner(
            repository,
            &frontier_root.join(&source.revision),
            &source.revision,
            cargo,
            false,
            Some(&corpus.original_release_authority),
        )?;
        if current != *source {
            return Err("historical source frontier or discovery context changed".into());
        }
        owners.extend(source.owners.iter().cloned());
    }
    if revisions.into_iter().cloned().collect::<BTreeSet<_>>() != expected_revisions {
        return Err("registry corpus omits original release-authority history".into());
    }
    let expected_paths = corpus
        .frontier
        .iter()
        .flat_map(|source| {
            source
                .input_sha256
                .keys()
                .map(|path| format!("{}/{path}", source.revision))
        })
        .collect();
    validate_archive_inventory(frontier_root, &expected_paths)?;
    if owners != corpus.responses.keys().cloned().collect() {
        return Err("registry corpus does not exactly cover source-owned candidates".into());
    }
    let inventory = corpus_inventory(corpus)?;
    let publishers = cohort_publishers(&inventory, version, archives)?;
    if publishers != corpus.publisher_anchors
        || publisher_anchor_union(&corpus.declared_anchors, &publishers) != corpus.anchors
    {
        return Err("checksum-bound archive publisher frontier differs".into());
    }
    Ok(inventory)
}

/// Complete version responses retain yanked releases and prereleases. The raw
/// response digest belongs in the existing generation artifact/input attestation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegistryOwnerVersions {
    pub response_sha256: Option<String>,
    pub versions: Vec<RegistryVersion>,
}

fn decode_registry_versions(package: &str, bytes: &[u8]) -> Result<Vec<RegistryVersion>, String> {
    #[derive(Deserialize)]
    struct Response {
        versions: Vec<RegistryVersion>,
        meta: Meta,
    }
    #[derive(Deserialize)]
    struct Meta {
        total: usize,
        next_page: Option<String>,
    }
    let response: Response = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    // The maintained endpoint is unpaginated when per_page is omitted. Detect
    // truncation/schema policy changes rather than treating a first page as complete.
    if response.meta.total != response.versions.len() || response.meta.next_page.is_some() {
        return Err("registry version inventory is incomplete".into());
    }
    let mut versions = BTreeMap::new();
    for version in response.versions {
        validate_version(&version.num)?;
        if version.package != package
            || version.checksum.len() != 64
            || !version.checksum.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err("registry inventory identity or checksum differs".into());
        }
        if versions.insert(version.num.clone(), version).is_some() {
            return Err("duplicate registry version identity".into());
        }
    }
    Ok(versions.into_values().collect())
}

/// Use BEFORE archive_plan/import/Cargo execution. Archive directory contents
/// are checked against registry evidence, never used to define expected owners.
pub fn validate_registry_cohort(
    version: &str,
    inventory: &BTreeMap<String, RegistryOwnerVersions>,
    archives: &Path,
) -> Result<(), String> {
    validate_version(version)?;
    let expected: BTreeSet<_> = inventory
        .values()
        .flat_map(|o| &o.versions)
        .filter(|v| v.num == version)
        .map(|v| format!("{}-{}.crate", v.package, v.num))
        .collect();
    if expected.is_empty() {
        return Err("requested cohort has no registry releases in captured source frontier".into());
    }
    validate_archive_inventory(archives, &expected)
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
    let Some(bytes) = registry_response(&format!("{package}/{version}"))? else {
        return Ok(None);
    };
    #[derive(Deserialize)]
    struct Response {
        version: RegistryVersion,
    }
    let response: Response = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    if response.version.package != package || response.version.num != version {
        return Err("registry response identity differs from requested release".into());
    }
    Ok(Some(response.version))
}

fn registry_response(path: &str) -> Result<Option<Vec<u8>>, String> {
    let url = format!("https://crates.io/api/v1/crates/{path}");
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
    Ok(Some(bytes[..split].to_vec()))
}

/// Verify archive bytes, VCS provenance and exact historical Cargo identity.
pub fn verify_archive(
    metadata: &Metadata,
    root: &Path,
    revision: &str,
    release: &RegistryVersion,
    archive: &Path,
) -> Result<(ReleasedPackage, BTreeMap<String, String>), String> {
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
    let files = verify_archive_sources(&root, package, &source, archive, &members)?;
    if super::sha256_file(archive).map_err(|e| e.to_string())? != actual {
        return Err("released archive changed during verification".into());
    }
    Ok((
        ReleasedPackage {
            package: release.package.clone(),
            version: release.num.clone(),
            yanked: release.yanked,
            registry_checksum: release.checksum.clone(),
            source_revision: revision.into(),
            path_in_vcs: vcs.path_in_vcs,
        },
        files,
    ))
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
        let lock = manifest.with_file_name("Cargo.lock");
        sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(cargo),
            lock_selection: producer_lock_selection(config.as_deref(), &lock, captured),
        }
        .configure(&mut command)
        .map_err(|e| e.to_string())?;
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

    // Controlled protocol bytes exercise the production reader; this is not a parser model.
    fn batch_protocol_fixture(
        response: &[u8],
        hold: bool,
        consume: impl FnMut(&str, &[u8]) -> Result<(), String>,
    ) -> (Result<(), String>, std::process::ExitStatus) {
        #[cfg(not(windows))]
        let fixture = Fixture::new();
        let object = "a".repeat(40);
        let listing = format!("100644 blob {object}\toriginal.bin\0");
        #[cfg(windows)]
        let mut command = {
            let bytes = response
                .iter()
                .map(u8::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let script = format!(
                "$ErrorActionPreference = 'Stop'\nif ([Console]::ReadLine() -ne '{object}') {{ exit 42 }}\n[byte[]]$bytes = @({bytes})\n$out = [Console]::OpenStandardOutput()\n$out.Write($bytes, 0, $bytes.Length)\n$out.Flush()\n{}\nexit 0\n",
                if hold { "Start-Sleep -Seconds 10" } else { "" }
            );
            let executable = std::path::PathBuf::from(
                std::env::var_os("SystemRoot").expect("Windows system root"),
            )
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
            let mut command = Command::new(executable);
            command
                .args(["-NoLogo", "-NoProfile", "-NonInteractive", "-Command"])
                .arg(script);
            command
        };
        #[cfg(not(windows))]
        let mut command = {
            let script = fixture.0.join("batch.sh");
            // POSIX shell builtin printf writes arbitrary binary bytes without an extra process.
            let bytes = response
                .iter()
                .map(|byte| format!("\\0{byte:03o}"))
                .collect::<String>();
            fs::write(&script, format!(
                "IFS= read -r object\n[ \"$object\" = '{object}' ] || exit 42\nprintf '%b' '{bytes}'\n{}\n",
                if hold { "exec sleep 10" } else { "exit 0" }
            )).unwrap();
            let mut command = Command::new("/bin/sh");
            command.arg(script);
            command
        };
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let result = read_git_source_blobs(&mut child, listing.as_bytes(), consume);
        // Retain the actual child handle: a missing drain cannot masquerade as a returned parser error.
        let status = child
            .try_wait()
            .unwrap()
            .expect("batch child must be terminated before returning");
        (result, status)
    }

    fn fixture_blob_response(body: &[u8]) -> Vec<u8> {
        let mut response = format!("{} blob {}\n", "a".repeat(40), body.len()).into_bytes();
        response.extend_from_slice(body);
        response.push(b'\n');
        response
    }

    #[test]
    fn git_batch_reader_preserves_binary_body_and_waits_for_successful_child() {
        let body = [0, 255, b'\n', 0];
        let mut received = None;
        let (result, status) =
            batch_protocol_fixture(&fixture_blob_response(&body), false, |path, bytes| {
                assert_eq!(path, "original.bin");
                received = Some(bytes.to_vec());
                Ok(())
            });
        assert_eq!(result, Ok(()));
        assert!(status.success());
        assert_eq!(received.as_deref(), Some(body.as_slice()));
    }

    #[test]
    fn git_batch_reader_rejects_malformed_protocol_and_kills_waiting_child() {
        let cases = [
            (
                b"malformed header\n".to_vec(),
                "invalid Git batch object header",
            ),
            (
                format!("{} blob 0\n\n", "b".repeat(40)).into_bytes(),
                "object identity differs",
            ),
            (
                format!("{} tree 0\n\n", "a".repeat(40)).into_bytes(),
                "object identity differs",
            ),
            (
                format!("{} blob not-a-size\n", "a".repeat(40)).into_bytes(),
                "invalid digit",
            ),
            (
                format!("{} blob {}\n", "a".repeat(40), "9".repeat(100)).into_bytes(),
                "number too large",
            ),
            (
                {
                    let mut bytes = format!("{} blob 1\n", "a".repeat(40)).into_bytes();
                    bytes.extend_from_slice(&[0, b'!']);
                    bytes
                },
                "invalid Git batch object boundary",
            ),
        ];
        for (response, expected) in cases {
            let mut called = false;
            let (result, status) = batch_protocol_fixture(&response, true, |_, _| {
                called = true;
                Ok(())
            });
            assert!(
                result.unwrap_err().contains(expected),
                "wrong protocol failure branch"
            );
            assert!(!called, "malformed body reached consumer");
            // The fixture would exit successfully after its hold; failure proves forced termination.
            assert!(
                !status.success(),
                "reader waited for normal fixture exit instead of killing it"
            );
        }
    }

    #[test]
    fn git_batch_reader_rejects_truncated_eof_and_drains_exited_child() {
        let mut response = format!("{} blob 4\n", "a".repeat(40)).into_bytes();
        response.extend_from_slice(&[0, 255]);
        let (result, status) = batch_protocol_fixture(&response, false, |_, _| {
            panic!("truncated body reached consumer")
        });
        assert!(result.is_err());
        assert!(status.success()); // Protocol failure is distinct from the producer exit status.
    }

    #[test]
    fn git_batch_reader_propagates_callback_failure_and_terminates_child() {
        let body = [0, 255, b'\n', 0];
        let (result, status) =
            batch_protocol_fixture(&fixture_blob_response(&body), true, |_, bytes| {
                assert_eq!(bytes, body);
                Err("callback refused fixture".into())
            });
        assert_eq!(result, Err("callback refused fixture".into()));
        assert!(!status.success());
    }

    #[cfg(unix)]
    #[test]
    fn registry_frontier_rejects_redirect_before_first_export_write() {
        let fixture = Fixture::new();
        let repository = fixture.0.join("repository");
        let foreign = fixture.0.join("foreign");
        fs::create_dir(&repository).unwrap();
        fs::create_dir(&foreign).unwrap();
        fs::write(foreign.join("sentinel"), "preserve").unwrap();
        let linked = fixture.0.join("frontier");
        std::os::unix::fs::symlink(&foreign, &linked).unwrap();
        let error = capture_registry_corpus(
            &repository,
            BTreeSet::from(["a".repeat(40)]),
            &linked.join("child"),
            Path::new("absent-cargo"),
        )
        .unwrap_err();
        assert!(error.contains("linked"));
        assert_eq!(fs::read(foreign.join("sentinel")).unwrap(), b"preserve");
        assert!(!foreign.join("child").exists());
    }

    #[test]
    fn original_git_links_are_opaque_and_cannot_supply_cargo_inputs() {
        let fixture = Fixture::new();
        let repository = fixture.0.join("repository");
        fs::create_dir_all(repository.join("src")).unwrap();
        fs::create_dir_all(repository.join("release")).unwrap();
        fs::write(
            repository.join("Cargo.toml"),
            "[package]\nname='demo'\nversion='0.1.0'\nedition='2021'\n[workspace]\n",
        )
        .unwrap();
        fs::write(repository.join("src/lib.rs"), "pub fn demo() {}\n").unwrap();
        fs::write(repository.join("release/cargo-crates.json"), "[\"demo\"]").unwrap();
        let git = |args: &[&str]| {
            command_output(
                Command::new(frontier_git().unwrap())
                    .current_dir(&repository)
                    .args(args),
            )
            .unwrap()
        };
        git(&["init", "--quiet"]);
        git(&["add", "."]);
        let blob_file = fixture.0.join("link-blob");
        let link_bytes = b"../../../../foreign-outside-workspace";
        fs::write(&blob_file, link_bytes).unwrap();
        let object =
            String::from_utf8(git(&["hash-object", "-w", blob_file.to_str().unwrap()])).unwrap();
        let object = object.trim();
        let link_path = "typescript/node_modules/typescript";
        git(&[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("120000,{object},{link_path}"),
        ]);
        let commit = || {
            git(&[
                "-c",
                "commit.gpgsign=false",
                "-c",
                "user.name=Source Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "Git link fixture",
            ]);
            String::from_utf8(git(&["rev-parse", "HEAD"]))
                .unwrap()
                .trim()
                .to_owned()
        };
        let revision = commit();
        let export = fixture.0.join("export");
        fs::create_dir(&export).unwrap();
        let captured =
            registry_owner_source_inner(&repository, &export, &revision, &cargo(), true, None)
                .unwrap();
        assert_eq!(captured.symlink_paths, BTreeSet::from([link_path.into()]));
        assert_eq!(captured.owners, BTreeSet::from(["demo".into()]));
        assert_eq!(fs::read(export.join(link_path)).unwrap(), link_bytes);
        assert!(fs::symlink_metadata(export.join(link_path))
            .unwrap()
            .is_file());
        assert_eq!(
            registry_owner_source_inner(&repository, &export, &revision, &cargo(), false, None)
                .unwrap(),
            captured
        );
        fs::write(export.join(link_path), "tampered link bytes").unwrap();
        assert!(registry_owner_source_inner(
            &repository,
            &export,
            &revision,
            &cargo(),
            false,
            None
        )
        .unwrap_err()
        .contains("differs from declared Git source"));
        for (index, linked_input) in ["src/lib.rs", ".cargo/config.toml"].iter().enumerate() {
            git(&[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("120000,{object},{linked_input}"),
            ]);
            let revision = commit();
            let export = fixture.0.join(format!("rejected-{index}"));
            fs::create_dir(&export).unwrap();
            let error =
                registry_owner_source_inner(&repository, &export, &revision, &cargo(), true, None)
                    .unwrap_err();
            assert!(error.contains("original Git link is a Cargo"), "{error}");
        }
    }

    #[test]
    fn original_release_history_retains_retired_owner_and_rejects_missing_frontier() {
        let fixture = Fixture::new();
        let repository = fixture.0.join("repository");
        fs::create_dir_all(repository.join("release")).unwrap();
        fs::write(
            repository.join(".gitattributes"),
            "original.bin export-ignore\n",
        )
        .unwrap();
        fs::write(repository.join("original.bin"), [0, 255, b'\n', 0]).unwrap();
        for package in ["current-owner", "retired-owner"] {
            let root = repository.join(package);
            fs::create_dir_all(root.join("src")).unwrap();
            fs::write(
                root.join("Cargo.toml"),
                format!("[package]\nname = {package:?}\nversion = \"0.1.1\"\nedition = \"2021\"\n"),
            )
            .unwrap();
            fs::write(root.join("src/lib.rs"), "pub fn fixture() {}\n").unwrap();
        }
        fs::write(
            repository.join("Cargo.toml"),
            "[workspace]\nmembers = [\"current-owner\", \"retired-owner\"]\nresolver = \"2\"\n",
        )
        .unwrap();
        command_output(
            Command::new(frontier_git().unwrap())
                .current_dir(&repository)
                .arg("init"),
        )
        .unwrap();
        let commit = || {
            command_output(
                Command::new(frontier_git().unwrap())
                    .current_dir(&repository)
                    .args(["add", "."]),
            )
            .unwrap();
            command_output(
                Command::new(frontier_git().unwrap())
                    .current_dir(&repository)
                    .args([
                        "-c",
                        "commit.gpgsign=false",
                        "-c",
                        "user.name=Source Fixture",
                        "-c",
                        "user.email=fixture@example.invalid",
                        "commit",
                        "-m",
                        "Source fixture",
                    ]),
            )
            .unwrap();
            String::from_utf8(
                command_output(
                    Command::new(frontier_git().unwrap())
                        .current_dir(&repository)
                        .args(["rev-parse", "HEAD"]),
                )
                .unwrap(),
            )
            .unwrap()
            .trim()
            .to_owned()
        };
        let manifest = repository.join("retired-owner/Cargo.toml");
        let source = fs::read_to_string(&manifest).unwrap();
        let before_index = commit();
        fs::write(&manifest, format!("{source}publish = false\n")).unwrap();
        fs::write(
            repository.join("release/cargo-crates.json"),
            "[\"current-owner\", \"retired-owner\"]",
        )
        .unwrap();
        let original = commit();
        fs::write(&manifest, &source).unwrap();
        let public_again = commit();
        fs::write(&manifest, format!("{source}publish = false\n")).unwrap();
        let private = commit();
        fs::write(
            repository.join("Cargo.toml"),
            "[workspace]\nmembers = [\"current-owner\"]\nresolver = \"2\"\n",
        )
        .unwrap();
        fs::remove_dir_all(repository.join("retired-owner")).unwrap();
        let current = commit();
        let anchors = BTreeSet::from([current.clone()]);
        let revisions = frontier_revisions(&repository, &anchors).unwrap();
        assert_eq!(
            revisions,
            BTreeSet::from([
                before_index.clone(),
                original,
                private,
                public_again.clone(),
                current.clone()
            ])
        );
        let authority = original_frontier_authority(&repository, &revisions).unwrap();
        assert_eq!(
            authority,
            BTreeSet::from(["current-owner".into(), "retired-owner".into()])
        );
        for revision in &revisions {
            let export = fixture.0.join(revision);
            fs::create_dir(&export).unwrap();
            let captured = registry_owner_source_inner(
                &repository,
                &export,
                revision,
                &cargo(),
                true,
                Some(&authority),
            )
            .unwrap();
            assert_eq!(captured.uses_frontier_authority, *revision == before_index);
            assert_eq!(
                captured.owners.contains("retired-owner"),
                *revision == before_index || *revision == public_again
            );
            assert_eq!(
                captured.absent_authority_members.contains("retired-owner"),
                *revision == current
            );
            assert_eq!(
                fs::read(export.join("original.bin")).unwrap(),
                [0, 255, b'\n', 0]
            );
            assert_eq!(
                registry_owner_source_inner(
                    &repository,
                    &export,
                    revision,
                    &cargo(),
                    false,
                    Some(&authority),
                )
                .unwrap(),
                captured
            );
        }
        // The complete corpus source vector cannot substitute current-only
        // authority for the history computed from these immutable anchors.
        let corpus = RegistryCorpus {
            repository: super::super::path_string(&repository),
            declared_anchors: anchors.clone(),
            anchors,
            original_release_authority: BTreeSet::new(),
            frontier: vec![],
            responses: BTreeMap::new(),
            publisher_anchors: BTreeMap::new(),
        };
        assert!(verify_registry_corpus(
            &corpus,
            &fixture.0.join("frontier"),
            Path::new("absent-cargo"),
            "0.1.1",
            &fixture.0
        )
        .unwrap_err()
        .contains("empty"));
    }

    #[test]
    fn cohort_publisher_anchors_require_immutable_archive_checksum_lineage() {
        let fixture = Fixture::new();
        let publisher = "a".repeat(40);
        let (release, archive) = fixture.archive_with_dirty(&publisher, "original-owner", true);
        let archives = fixture.0.join("cohort");
        fs::create_dir(&archives).unwrap();
        let name = "historical-fixture-0.1.0.crate";
        let retained = archives.join(name);
        fs::copy(&archive, &retained).unwrap();
        let inventory = BTreeMap::from([(
            "historical-fixture".into(),
            RegistryOwnerVersions {
                response_sha256: None,
                versions: vec![release],
            },
        )]);
        let facts = cohort_publishers(&inventory, "0.1.0", &archives).unwrap();
        let vcs = facts[name].as_ref().unwrap();
        assert!(vcs.dirty);
        assert_eq!(vcs.path_in_vcs, "original-owner");
        let declared = BTreeSet::from(["b".repeat(40)]);
        assert_eq!(
            publisher_anchor_union(&declared, &facts),
            BTreeSet::from([publisher, "b".repeat(40)])
        );
        fs::write(&retained, "different published bytes").unwrap();
        assert!(cohort_publishers(&inventory, "0.1.0", &archives)
            .unwrap_err()
            .contains("checksum"));
    }

    #[test]
    fn registry_frontier_rejects_missing_or_nonimmutable_history_before_git() {
        assert!(frontier_revisions(Path::new("absent"), &BTreeSet::new())
            .unwrap_err()
            .contains("immutable Git"));
        assert!(
            frontier_revisions(Path::new("absent"), &BTreeSet::from(["HEAD".into()]))
                .unwrap_err()
                .contains("immutable Git")
        );
        let corpus = RegistryCorpus {
            repository: "absent-repository".into(),
            declared_anchors: BTreeSet::new(),
            anchors: BTreeSet::from(["a".repeat(40)]),
            original_release_authority: BTreeSet::new(),
            frontier: vec![RegistryOwnerSource {
                revision: "a".repeat(40),
                input_sha256: BTreeMap::new(),
                symlink_paths: BTreeSet::new(),
                discovery: CargoDiscoveryInputs::default(),
                owners: BTreeSet::new(),
                outside_release_authority: BTreeSet::new(),
                uses_frontier_authority: true,
                absent_authority_members: BTreeSet::new(),
            }],
            responses: BTreeMap::new(),
            publisher_anchors: BTreeMap::new(),
        };
        assert!(verify_registry_corpus(
            &corpus,
            Path::new("absent"),
            Path::new("absent-cargo"),
            "0.1.0",
            Path::new("absent")
        )
        .unwrap_err()
        .contains("declared original history"));
    }
    use super::*;

    #[test]
    fn windows_discovery_environment_rejects_conflicting_case_aliases() {
        let values = [
            ("PATH".into(), "first-toolchain".into()),
            ("Path".into(), "second-toolchain".into()),
        ];
        for inputs in [values.clone(), [values[1].clone(), values[0].clone()]] {
            let error = discovery_environment_inputs(inputs, true).unwrap_err();
            assert!(error.contains("conflicting case aliases"));
        }
    }

    #[test]
    fn windows_discovery_environment_retains_equal_path_aliases() {
        let inputs = [
            ("Path".into(), "same-toolchain".into()),
            ("PATH".into(), "same-toolchain".into()),
        ];
        let retained = discovery_environment_inputs(inputs, true).unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(
            retained["PATH"],
            super::super::sha256_bytes(b"same-toolchain")
        );
    }

    #[test]
    fn posix_discovery_environment_keeps_distinct_case_sensitive_names() {
        let inputs = [
            ("CARGO_TEST_VALUE".into(), "upper".into()),
            ("CARGO_test_value".into(), "lower".into()),
        ];
        let retained = discovery_environment_inputs(inputs, false).unwrap();
        assert_eq!(retained.len(), 2);
        assert_ne!(retained["CARGO_TEST_VALUE"], retained["CARGO_test_value"]);
    }

    #[test]
    fn crates_io_publish_eligibility_respects_the_registry_allowlist() {
        assert!(crates_io_eligible(None));
        assert!(!crates_io_eligible(Some(&[])));
        assert!(crates_io_eligible(Some(&["crates-io".into()])));
        assert!(!crates_io_eligible(Some(&["private".into()])));
        assert!(crates_io_eligible(Some(&[
            "private".into(),
            "crates-io".into()
        ])));
        assert!(!crates_io_eligible(Some(&["crates-io-mirror".into()])));
    }

    #[test]
    fn complete_registry_inventory_keeps_yanked_prereleases_and_rejects_partial_sets() {
        let response = serde_json::json!({
            "versions": [
                {"crate":"historical-fixture","num":"0.1.0","checksum":"a".repeat(64),"yanked":false},
                {"crate":"historical-fixture","num":"0.1.1","checksum":"b".repeat(64),"yanked":true},
                {"crate":"historical-fixture","num":"1.0.0-rc.3","checksum":"c".repeat(64),"yanked":true}
            ],
            "meta":{"total":3,"next_page":null}
        });
        let encode = |v: &serde_json::Value| serde_json::to_vec(v).unwrap();
        let versions = decode_registry_versions("historical-fixture", &encode(&response)).unwrap();
        assert_eq!(versions.len(), 3);
        assert_eq!(versions.iter().filter(|v| v.yanked).count(), 2);
        let mut truncated = response.clone();
        truncated["versions"].as_array_mut().unwrap().pop();
        assert!(
            decode_registry_versions("historical-fixture", &encode(&truncated))
                .unwrap_err()
                .contains("incomplete")
        );
        let mut paged = response.clone();
        paged["meta"]["next_page"] = "?seek=retained-next".into();
        assert!(decode_registry_versions("historical-fixture", &encode(&paged)).is_err());
        let mut duplicate = response.clone();
        duplicate["versions"][1] = duplicate["versions"][0].clone();
        assert!(
            decode_registry_versions("historical-fixture", &encode(&duplicate))
                .unwrap_err()
                .contains("duplicate")
        );
        let mut wrong_owner = response.clone();
        wrong_owner["versions"][0]["crate"] = "different-owner".into();
        assert!(decode_registry_versions("historical-fixture", &encode(&wrong_owner)).is_err());
        let mut checksum = response.clone();
        checksum["versions"][0]["checksum"] = "not-a-checksum".into();
        assert!(decode_registry_versions("historical-fixture", &encode(&checksum)).is_err());
    }

    #[test]
    fn complete_registry_cohort_rejects_missing_retired_owner_before_import() {
        let fixture = Fixture::new();
        let archive_dir = fixture.0.join("inventory-inputs");
        fs::create_dir(&archive_dir).unwrap();
        let mut inventory = BTreeMap::new();
        for package in ["current-owner", "retired-owner"] {
            inventory.insert(
                package.into(),
                RegistryOwnerVersions {
                    response_sha256: Some(format!("sha256:{}", "a".repeat(64))),
                    versions: vec![RegistryVersion {
                        package: package.into(),
                        num: "0.1.1".into(),
                        checksum: "b".repeat(64),
                        yanked: true,
                    }],
                },
            );
        }
        fs::write(
            archive_dir.join("current-owner-0.1.1.crate"),
            b"not imported by this gate",
        )
        .unwrap();
        assert!(validate_registry_cohort("0.1.1", &inventory, &archive_dir).is_err());
        fs::write(
            archive_dir.join("retired-owner-0.1.1.crate"),
            b"not imported by this gate",
        )
        .unwrap();
        validate_registry_cohort("0.1.1", &inventory, &archive_dir).unwrap();
        fs::write(archive_dir.join("unregistered-owner-0.1.1.crate"), b"extra").unwrap();
        assert!(validate_registry_cohort("0.1.1", &inventory, &archive_dir).is_err());
        assert!(!archive_dir.join("sources").exists());
    }

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
    fn dependency_archive_initial_import_uses_verified_registry_facts() {
        if let Some(root) = std::env::var_os("SDK_DOCS_DEPENDENCY_IMPORT_ROOT") {
            let root = PathBuf::from(root);
            let (dependencies, files) =
                producer_dependencies(&root, "historical-fixture-0.1.0", None).unwrap();
            fs::write(
                root.join("transport/import.json"),
                serde_json::to_vec(&(dependencies, files)).unwrap(),
            )
            .unwrap();
        }
    }

    #[test]
    fn actual_cargo_archived_dependency_resolution_preserves_originals_and_relocates() {
        let fixture = Fixture::new();
        let root = fixture.0.canonicalize().unwrap();
        let directory = "historical-fixture-0.1.0";
        let producer_dir = root.join(format!(".docs-producer/{directory}"));
        fs::create_dir_all(producer_dir.join("registry-archives")).unwrap();
        let dependency = Fixture::new();
        fs::write(dependency.0.join("Cargo.toml"), "[package]\nname='docs-retained-dependency'\nversion='0.1.0'\nedition='2021'\n[workspace]\n[features]\nnative=[]\n").unwrap();
        fs::write(dependency.0.join("src/lib.rs"), "/// Verified archived API marker.\npub struct ArchiveMarker;\n#[cfg(feature=\"native\")] pub fn value() -> u8 { 37 }\n").unwrap();
        fs::write(
            dependency.0.join("README.md"),
            "# Actual archived dependency\n",
        )
        .unwrap();
        fs::write(
            dependency.0.join("descriptor.bin"),
            b"original binary descriptor",
        )
        .unwrap();
        let cargo = cargo();
        command_output(
            Command::new(&cargo)
                .args([
                    "package",
                    "--offline",
                    "--allow-dirty",
                    "--no-verify",
                    "--manifest-path",
                ])
                .arg(dependency.0.join("Cargo.toml"))
                .arg("--target-dir")
                .arg(dependency.0.join("target")),
        )
        .unwrap();
        let archive = producer_dir.join("registry-archives/docs-retained-dependency-0.1.0.crate");
        fs::copy(
            dependency
                .0
                .join("target/package/docs-retained-dependency-0.1.0.crate"),
            &archive,
        )
        .unwrap();
        let checksum = super::super::sha256_file(&archive)
            .unwrap()
            .trim_start_matches("sha256:")
            .to_owned();
        let transport = root.join("transport");
        fs::create_dir_all(&transport).unwrap();
        fs::write(transport.join("release.json"), serde_json::to_vec(&serde_json::json!({"version":{"crate":"docs-retained-dependency","num":"0.1.0","checksum":checksum,"yanked":true}})).unwrap()).unwrap();
        // Isolate HTTP transport in the child test process, following the
        // existing CLI fixtures; actual Cargo still packages and compiles sources.
        let curl_source = transport.join("fixture-curl.rs");
        fs::write(&curl_source,"fn main() { let root=std::env::current_exe().unwrap();print!(\"{}\\n200\",std::fs::read_to_string(root.parent().unwrap().join(\"release.json\")).unwrap()); }\n").unwrap();
        let rustc = cargo
            .parent()
            .unwrap()
            .join(if cfg!(windows) { "rustc.exe" } else { "rustc" });
        command_output(
            Command::new(rustc)
                .args(["--edition=2021"])
                .arg(&curl_source)
                .arg("-o")
                .arg(transport.join(if cfg!(windows) { "curl.exe" } else { "curl" })),
        )
        .unwrap();
        let path = std::env::join_paths(
            std::iter::once(transport.clone())
                .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
        )
        .unwrap();
        #[cfg(unix)]
        {
            let foreign = Fixture::new();
            let foreign = foreign.0.canonicalize().unwrap();
            fs::write(foreign.join("sentinel"), b"unchanged").unwrap();
            let before = fs::read_dir(&foreign)
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect::<BTreeSet<_>>();
            let linked = producer_dir.join("dependencies");
            std::os::unix::fs::symlink(&foreign, &linked).unwrap();
            let rejected = Command::new(std::env::current_exe().unwrap())
                .args(["historical::tests::dependency_archive_initial_import_uses_verified_registry_facts", "--exact", "--nocapture"])
                .env("SDK_DOCS_DEPENDENCY_IMPORT_ROOT", &root)
                .env("PATH", &path).output().unwrap();
            assert!(!rejected.status.success());
            assert!(!transport.join("import.json").exists());
            assert_eq!(fs::read(foreign.join("sentinel")).unwrap(), b"unchanged");
            assert_eq!(
                fs::read_dir(&foreign)
                    .unwrap()
                    .map(|e| e.unwrap().file_name())
                    .collect::<BTreeSet<_>>(),
                before
            );
            fs::remove_file(linked).unwrap();
        }
        let imported = Command::new(std::env::current_exe().unwrap())
            .args([
                "historical::tests::dependency_archive_initial_import_uses_verified_registry_facts",
                "--exact",
                "--nocapture",
            ])
            .env("SDK_DOCS_DEPENDENCY_IMPORT_ROOT", &root)
            .env("PATH", path)
            .output()
            .unwrap();
        assert!(
            imported.status.success(),
            "{}",
            String::from_utf8_lossy(&imported.stderr)
        );
        let (dependencies, imported_files): (
            Vec<ProducerDependencyArchive>,
            BTreeMap<String, String>,
        ) = serde_json::from_slice(&fs::read(transport.join("import.json")).unwrap()).unwrap();
        assert_eq!(dependencies.len(), 1);
        assert_eq!(dependencies[0].registry_checksum, checksum);
        assert!(dependencies[0].yanked);
        assert!(dependencies[0].publisher_vcs.is_none());
        assert!(imported_files
            .keys()
            .any(|p| p.ends_with("/Cargo.toml.orig")));
        assert!(imported_files
            .keys()
            .any(|p| p.ends_with("/descriptor.bin")));
        let original_manifest = "[package]\nname='historical-fixture'\nversion='0.1.0'\nedition='2021'\n[workspace]\n[features]\ndefault=[]\nold-feature=['dep-alias/native']\n[dependencies]\ndep-alias={package='docs-retained-dependency',version='=0.1.0',default-features=false,optional=true}\n[[example]]\nname='old-example'\nrequired-features=['old-feature']\n";
        fs::write(root.join("Cargo.toml"), original_manifest).unwrap();
        fs::write(
            root.join("src/lib.rs"),
            "#[cfg(feature=\"dep-alias\")]\npub use dep_alias::ArchiveMarker;\n#[cfg(feature=\"dep-alias\")]\npub fn value() -> u8 { dep_alias::value() }\n",
        )
        .unwrap();
        let original_lock = b"broken original published lock\n";
        fs::write(root.join("Cargo.lock"), original_lock).unwrap();
        let config = producer_config_bytes(directory, &dependencies).unwrap();
        assert!(
            config.contains("historical-fixture-0.1.0/dependencies/docs-retained-dependency-0.1.0")
        );
        let config_path = producer_dir.join("config.toml");
        fs::write(&config_path, &config).unwrap();
        let context = sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(&cargo),
            lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(
                &config_path,
            )),
        };
        let mut generate = Command::new(&cargo);
        generate
            .args(["generate-lockfile", "--offline", "--manifest-path"])
            .arg(root.join("Cargo.toml"));
        context.configure(&mut generate).unwrap();
        command_output(&mut generate).unwrap();
        let producer = ProducerResolution {
            lock_path: format!(".docs-producer/{directory}/Cargo.lock"),
            lock_sha256: super::super::sha256_file(&producer_dir.join("Cargo.lock")).unwrap(),
            config_path: format!(".docs-producer/{directory}/config.toml"),
            config_sha256: super::super::sha256_file(&config_path).unwrap(),
            dependency_archives: dependencies.clone(),
        };
        let files = verify_producer_resolution(&root, directory, &producer).unwrap();
        // Default Cargo metadata legitimately omits an optional renamed dependency.
        let mut default_metadata = Command::new(&cargo);
        default_metadata
            .args([
                "metadata",
                "--offline",
                "--locked",
                "--format-version",
                "1",
                "--manifest-path",
            ])
            .arg(root.join("Cargo.toml"));
        context.configure(&mut default_metadata).unwrap();
        let default_metadata: Metadata =
            serde_json::from_slice(&command_output(&mut default_metadata).unwrap()).unwrap();
        assert_eq!(default_metadata.resolve.as_ref().unwrap().nodes.len(), 1);
        assert!(default_metadata
            .packages
            .iter()
            .all(|package| package.name.as_ref() != "docs-retained-dependency"));
        verify_producer_metadata(
            &root,
            directory,
            &root.join("Cargo.toml"),
            &cargo,
            &config_path,
            &dependencies,
        )
        .unwrap();
        assert_eq!(fs::read(root.join("Cargo.lock")).unwrap(), original_lock);
        assert_eq!(
            fs::read_to_string(root.join("Cargo.toml")).unwrap(),
            original_manifest
        );
        let mut original = Command::new(&cargo);
        original
            .args(["metadata", "--locked", "--offline", "--manifest-path"])
            .arg(root.join("Cargo.toml"));
        sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(&cargo),
            lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::SourceAdjacent(
                &root.join("Cargo.lock"),
            )),
        }
        .configure(&mut original)
        .unwrap();
        assert!(command_output(&mut original).is_err());
        let metadata = sdk_docs::rustdoc_profiles::load_metadata_with_context(
            root.join("Cargo.toml"),
            context,
        )
        .unwrap();
        let examples = examples(
            &metadata,
            &root,
            &BTreeSet::from(["historical-fixture".to_owned()]),
        )
        .unwrap();
        let captured = CapturedSource::RegistryArchives {
            archives: vec![ArchiveSource {
                package: "historical-fixture".into(),
                version: "0.1.0".into(),
                registry_checksum: "a".repeat(64),
                publisher_vcs: None,
                resolution_lock: ResolutionLock::SeparateDocsProducer {
                    original_lock: OriginalLock::Published {
                        sha256: super::super::sha256_bytes(original_lock),
                    },
                    producer: producer.clone(),
                },
            }],
        };
        let owners = BTreeMap::from([("historical-fixture".into(), metadata.clone())]);
        let executions = execute_examples_with_context(
            &root,
            &cargo,
            &root.join("target"),
            &examples,
            Some(&owners),
            Some(&captured),
        )
        .unwrap();
        assert_eq!(
            executions[0].stdout_sha256,
            super::super::sha256_bytes(b"37\n")
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
            features: BTreeSet::from(["dep-alias/native".into(), "old-feature".into()]),
        };
        sdk_docs::rustdoc_profiles::execute_target_profile_with_context(
            root.join("Cargo.toml"),
            &metadata,
            &profile,
            &BTreeSet::from([host.clone()]),
            root.join("target"),
            root.join("target/receipt.json"),
            context,
            &sdk_docs::rustdoc_profiles::RustdocTarget::Library,
        )
        .unwrap();
        assert!(fs::read_to_string(root.join("target/receipt.json"))
            .unwrap()
            .contains("ArchiveMarker"));
        let mut invalid_alias = profile.clone();
        invalid_alias.features = BTreeSet::from(["missing-alias/native".into()]);
        assert!(
            sdk_docs::rustdoc_profiles::execute_target_profile_with_context(
                root.join("Cargo.toml"),
                &metadata,
                &invalid_alias,
                &BTreeSet::from([host.clone()]),
                root.join("target"),
                root.join("target/invalid.json"),
                context,
                &sdk_docs::rustdoc_profiles::RustdocTarget::Library
            )
            .is_err()
        );
        let relocated = Fixture::new();
        let relocated_root = relocated.0.canonicalize().unwrap();
        for path in files.keys() {
            super::super::write_immutable(
                &relocated_root.join(path),
                &fs::read(root.join(path)).unwrap(),
            )
            .unwrap();
        }
        fs::write(relocated_root.join("Cargo.toml"), original_manifest).unwrap();
        fs::copy(root.join("src/lib.rs"), relocated_root.join("src/lib.rs")).unwrap();
        fs::write(relocated_root.join("Cargo.lock"), original_lock).unwrap();
        let relocated_config = relocated_root.join(&producer.config_path);
        assert_eq!(
            verify_producer_resolution(&relocated_root, directory, &producer).unwrap(),
            files
        );
        verify_producer_metadata(
            &relocated_root,
            directory,
            &relocated_root.join("Cargo.toml"),
            &cargo,
            &relocated_config,
            &dependencies,
        )
        .unwrap();
        let relocated_context = sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(&cargo),
            lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(
                &relocated_config,
            )),
        };
        let relocated_metadata = sdk_docs::rustdoc_profiles::load_metadata_with_context(
            relocated_root.join("Cargo.toml"),
            relocated_context,
        )
        .unwrap();
        sdk_docs::rustdoc_profiles::execute_target_profile_with_context(
            relocated_root.join("Cargo.toml"),
            &relocated_metadata,
            &profile,
            &BTreeSet::from([host]),
            relocated_root.join("target"),
            relocated_root.join("target/receipt.json"),
            relocated_context,
            &sdk_docs::rustdoc_profiles::RustdocTarget::Library,
        )
        .unwrap();
        assert!(
            fs::read_to_string(relocated_root.join("target/receipt.json"))
                .unwrap()
                .contains("ArchiveMarker")
        );
        let mut relocated_capture = captured.clone();
        let CapturedSource::RegistryArchives { archives } = &mut relocated_capture else {
            unreachable!()
        };
        let ResolutionLock::SeparateDocsProducer {
            producer: relocated_producer,
            ..
        } = &mut archives[0].resolution_lock
        else {
            unreachable!()
        };
        relocated_producer.lock_sha256 =
            super::super::sha256_file(&relocated_root.join(&producer.lock_path)).unwrap();
        relocated_producer.config_sha256 = super::super::sha256_file(&relocated_config).unwrap();
        assert_eq!(
            captured.revision().unwrap(),
            relocated_capture.revision().unwrap()
        );
        let wrong_template = config.replace(&format!("{directory}/dependencies/"), "dependencies/");
        fs::write(&config_path, &wrong_template).unwrap();
        let mut resealed = producer.clone();
        resealed.config_sha256 = super::super::sha256_file(&config_path).unwrap();
        assert!(verify_producer_resolution(&root, directory, &resealed).is_err());
        fs::write(&config_path, &config).unwrap();
        for member in [
            "src/lib.rs",
            "README.md",
            "descriptor.bin",
            "Cargo.lock",
            "Cargo.toml.orig",
        ] {
            let path = producer_dir.join(format!(
                "dependencies/docs-retained-dependency-0.1.0/{member}"
            ));
            let bytes = fs::read(&path).unwrap();
            fs::write(&path, b"changed imported dependency input").unwrap();
            assert!(
                verify_producer_resolution(&root, directory, &producer).is_err(),
                "{member}"
            );
            fs::write(&path, bytes).unwrap();
        }
        let bytes = fs::read(&archive).unwrap();
        fs::write(&archive, b"wrong dependency archive checksum").unwrap();
        assert!(verify_producer_resolution(&root, directory, &producer).is_err());
        fs::write(&archive, bytes).unwrap();
        let extra =
            producer_dir.join("dependencies/docs-retained-dependency-0.1.0/unverified.json");
        fs::write(&extra, "{}").unwrap();
        assert!(verify_producer_resolution(&root, directory, &producer).is_err());
        fs::remove_file(extra).unwrap();
        let omitted = producer_dir.join("dependencies/docs-retained-dependency-0.1.0/src/lib.rs");
        let bytes = fs::read(&omitted).unwrap();
        fs::remove_file(&omitted).unwrap();
        assert!(verify_producer_resolution(&root, directory, &producer).is_err());
        assert!(
            !omitted.exists(),
            "verification must not restore missing authoritative inputs"
        );
        fs::write(omitted, bytes).unwrap();
        let mut wrong_identity = producer.clone();
        wrong_identity.dependency_archives[0].publisher_vcs = Some(PublisherVcs {
            revision: "b".repeat(40),
            dirty: true,
            path_in_vcs: "src".into(),
        });
        assert!(verify_producer_resolution(&root, directory, &wrong_identity).is_err());
        wrong_identity.dependency_archives[0] = dependencies[0].clone();
        wrong_identity
            .dependency_archives
            .push(dependencies[0].clone());
        assert!(verify_producer_resolution(&root, directory, &wrong_identity).is_err());
        #[cfg(unix)]
        {
            let original = producer_dir.join("dependencies/docs-retained-dependency-0.1.0");
            let redirected = producer_dir.join("redirected-dependency");
            fs::rename(&original, &redirected).unwrap();
            std::os::unix::fs::symlink(&redirected, &original).unwrap();
            assert!(verify_producer_resolution(&root, directory, &producer).is_err());
            fs::remove_file(original).unwrap();
            fs::rename(
                redirected,
                producer_dir.join("dependencies/docs-retained-dependency-0.1.0"),
            )
            .unwrap();
        }
        assert_eq!(
            verify_producer_resolution(&root, directory, &producer).unwrap(),
            files
        );
        // A genuine Cargo-produced package recorded as a patch but unused by the
        // owner must fail the actual metadata selection check.
        let unused = Fixture::new();
        fs::write(unused.0.join("Cargo.toml"),"[package]\nname='docs-unused-dependency'\nversion='0.1.0'\nedition='2021'\n[workspace]\n").unwrap();
        command_output(
            Command::new(&cargo)
                .args([
                    "package",
                    "--offline",
                    "--allow-dirty",
                    "--no-verify",
                    "--manifest-path",
                ])
                .arg(unused.0.join("Cargo.toml"))
                .arg("--target-dir")
                .arg(unused.0.join("target")),
        )
        .unwrap();
        let unused_archive =
            producer_dir.join("registry-archives/docs-unused-dependency-0.1.0.crate");
        fs::copy(
            unused
                .0
                .join("target/package/docs-unused-dependency-0.1.0.crate"),
            &unused_archive,
        )
        .unwrap();
        command_output(
            Command::new("tar")
                .arg("-xf")
                .arg(&unused_archive)
                .arg("-C")
                .arg(producer_dir.join("dependencies")),
        )
        .unwrap();
        let mut unused_records = dependencies.clone();
        unused_records.push(ProducerDependencyArchive {
            package: "docs-unused-dependency".into(),
            version: "0.1.0".into(),
            registry_checksum: super::super::sha256_file(&unused_archive)
                .unwrap()
                .trim_start_matches("sha256:")
                .to_owned(),
            publisher_vcs: None,
            yanked: false,
        });
        fs::write(
            &config_path,
            producer_config_bytes(directory, &unused_records).unwrap(),
        )
        .unwrap();
        let mut generate = Command::new(&cargo);
        generate
            .args(["generate-lockfile", "--offline", "--manifest-path"])
            .arg(root.join("Cargo.toml"));
        context.configure(&mut generate).unwrap();
        command_output(&mut generate).unwrap();
        assert!(verify_producer_metadata(
            &root,
            directory,
            &root.join("Cargo.toml"),
            &cargo,
            &config_path,
            &unused_records
        )
        .is_err());
        // Replay must independently reject a coherently resealed unused patch record.
        let mut resealed = producer.clone();
        resealed.dependency_archives = unused_records.clone();
        resealed.config_sha256 = super::super::sha256_file(&config_path).unwrap();
        resealed.lock_sha256 = super::super::sha256_file(&producer_dir.join("Cargo.lock")).unwrap();
        let replay_files = verify_producer_resolution(&root, directory, &resealed).unwrap();
        let replay = Fixture::new();
        let replay_root = replay.0.canonicalize().unwrap();
        for path in replay_files.keys() {
            let destination = replay_root.join(path);
            fs::create_dir_all(destination.parent().unwrap()).unwrap();
            fs::copy(root.join(path), destination).unwrap();
        }
        for path in [
            "Cargo.toml",
            "Cargo.lock",
            "src/lib.rs",
            "examples/old-example.rs",
        ] {
            let destination = replay_root.join(directory).join(path);
            fs::create_dir_all(destination.parent().unwrap()).unwrap();
            fs::copy(root.join(path), destination).unwrap();
        }
        let mut replay_identity = captured.clone();
        let CapturedSource::RegistryArchives { archives } = &mut replay_identity else {
            panic!("archive fixture")
        };
        let ResolutionLock::SeparateDocsProducer { producer, .. } =
            &mut archives[0].resolution_lock
        else {
            panic!("producer fixture")
        };
        *producer = resealed;
        assert!(imported_metadata(&replay_root, &replay_identity, &cargo)
            .unwrap_err()
            .contains("Cargo did not select captured dependency root"));
        let foreign = Fixture::new();
        fs::write(foreign.0.join("Cargo.toml"),"[package]\nname='docs-foreign-dependency'\nversion='0.1.0'\nedition='2021'\n[workspace]\n").unwrap();
        let path = serde_json::to_string(foreign.0.to_str().unwrap()).unwrap();
        fs::write(
            root.join("Cargo.toml"),
            original_manifest.replace(
                "[dependencies]\n",
                &format!("[dependencies]\nforeign-extra={{package='docs-foreign-dependency',path={path}}}\n"),
            ),
        )
        .unwrap();
        fs::write(&config_path, &config).unwrap();
        let mut generate = Command::new(&cargo);
        generate
            .args(["generate-lockfile", "--offline", "--manifest-path"])
            .arg(root.join("Cargo.toml"));
        context.configure(&mut generate).unwrap();
        command_output(&mut generate).unwrap();
        assert!(verify_producer_metadata(
            &root,
            directory,
            &root.join("Cargo.toml"),
            &cargo,
            &config_path,
            &dependencies
        )
        .unwrap_err()
        .contains("uncaptured path source"));
    }

    #[test]
    fn retained_inventory_accepts_exact_flat_and_nested_members_only() {
        let fixture = Fixture::new();
        let directory = fixture.0.canonicalize().unwrap().join("inventory");
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("archive.crate"), b"flat").unwrap();
        let mut expected = BTreeSet::from(["archive.crate".into()]);
        validate_archive_inventory(&directory, &expected).unwrap();
        validate_archive_inventory(&directory.join("."), &expected).unwrap();
        #[cfg(unix)]
        {
            let cwd = std::env::current_dir().unwrap().canonicalize().unwrap();
            let common = cwd.ancestors().find(|p| directory.starts_with(p)).unwrap();
            let relative =
                PathBuf::from("../".repeat(cwd.strip_prefix(common).unwrap().components().count()))
                    .join(directory.strip_prefix(common).unwrap());
            validate_archive_inventory(&relative, &expected).unwrap();
        }
        fs::create_dir_all(directory.join("dependencies/dep/src")).unwrap();
        fs::write(directory.join("dependencies/dep/src/lib.rs"), b"nested").unwrap();
        expected.insert("dependencies/dep/src/lib.rs".into());
        validate_archive_inventory(&directory, &expected).unwrap();
        fs::create_dir(directory.join("empty-extra")).unwrap();
        assert!(validate_archive_inventory(&directory, &expected).is_err());
        fs::remove_dir(directory.join("empty-extra")).unwrap();
        fs::write(directory.join("extra"), b"unknown").unwrap();
        assert!(validate_archive_inventory(&directory, &expected).is_err());
        fs::remove_file(directory.join("extra")).unwrap();
        fs::remove_file(directory.join("dependencies/dep/src/lib.rs")).unwrap();
        assert!(validate_archive_inventory(&directory, &expected).is_err());
        #[cfg(unix)]
        {
            fs::write(directory.join("dependencies/dep/src/lib.rs"), b"nested").unwrap();
            std::os::unix::fs::symlink(directory.join("archive.crate"), directory.join("redirect"))
                .unwrap();
            expected.insert("redirect".into());
            assert!(validate_archive_inventory(&directory, &expected).is_err());
            let alias = fixture.0.join("inventory-alias");
            std::os::unix::fs::symlink(&directory, &alias).unwrap();
            assert!(validate_archive_inventory(&alias, &expected).is_err());
        }
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
            lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(
                &config_path,
            )),
        };
        let mut generate = Command::new(&cargo);
        generate
            .args(["generate-lockfile", "--offline", "--manifest-path"])
            .arg(fixture.0.join("Cargo.toml"));
        context.configure(&mut generate).unwrap();
        command_output(&mut generate).unwrap();
        assert_eq!(fs::read(fixture.0.join("Cargo.lock")).unwrap(), original);
        assert!(external_lock.is_file());
        let mut producer = ProducerResolution {
            lock_path: format!("{prefix}Cargo.lock"),
            lock_sha256: super::super::sha256_file(&external_lock).unwrap(),
            config_path: format!("{prefix}config.toml"),
            config_sha256: super::super::sha256_file(&producer_dir.join("config.toml")).unwrap(),
            dependency_archives: Vec::new(),
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
            lock_selection: Some(sdk_docs::rustdoc_profiles::LockSelection::RetainedConfig(
                &relocated_config,
            )),
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
        assert_eq!(
            verify_producer_resolution(&fixture.0, directory, &producer).unwrap_err(),
            "producer config differs from captured lock and dependency archives"
        );
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
        assert!(accepted.0.yanked);
        assert_eq!(
            accepted.1.get("src/lib.rs"),
            Some(&super::super::sha256_file(&fixture.0.join("src/lib.rs")).unwrap())
        );
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
            let (_, files) =
                verify_archive(&metadata, &fixture.0, &revision, &release, &archive).unwrap();
            assert!(files.contains_key(name));
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
