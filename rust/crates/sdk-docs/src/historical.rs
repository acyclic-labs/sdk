//! Historical release identities and binary-only native documentation.
use super::*;

/// Registry facts verified against an actual released source archive.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReleasedPackage {
    pub package: String,
    pub version: String,
    pub yanked: bool,
    pub registry_checksum: String,
    pub source_revision: String,
    pub path_in_vcs: String,
}

/// Publisher VCS facts embedded in the exact released archive, independent of
/// the imported source actually compiled by the documentation producer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublisherVcs {
    pub revision: String,
    pub dirty: bool,
    pub path_in_vcs: String,
}

/// An immutable registry input and the lock actually used for its Cargo run.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArchiveSource {
    pub package: String,
    pub version: String,
    pub registry_checksum: String,
    pub publisher_vcs: Option<PublisherVcs>,
    pub resolution_lock: ResolutionLock,
}

/// Published locks and newly generated documentation resolutions remain distinct.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ResolutionLock {
    Published {
        sha256: String,
    },
    DocsProducer {
        sha256: String,
    },
    SeparateDocsProducer {
        original_lock: OriginalLock,
        producer: ProducerResolution,
    },
}

/// Facts about the original archive, never the lock selected by a later producer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum OriginalLock {
    Published { sha256: String },
    Absent,
}

/// An external maintained-Cargo resolution. Paths are relative to the captured
/// aggregate; config selects the lock and verified archived dependency sources.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProducerResolution {
    pub lock_path: String,
    pub lock_sha256: String,
    pub config_path: String,
    pub config_sha256: String,
    pub dependency_archives: Vec<ProducerDependencyArchive>,
}

/// Exact released dependency source selected by an explicit docs producer patch.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProducerDependencyArchive {
    pub package: String,
    pub version: String,
    pub registry_checksum: String,
    pub publisher_vcs: Option<PublisherVcs>,
    pub yanked: bool,
}

/// Authority of the captured source; archive identities never impersonate Git.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum CapturedSource {
    Git { revision: String },
    RegistryArchives { archives: Vec<ArchiveSource> },
}

impl CapturedSource {
    pub fn revision(&self) -> Result<String, Error> {
        match self {
            Self::Git { revision } => Ok(revision.clone()),
            Self::RegistryArchives { archives } => {
                if archives.is_empty()
                    || archives.windows(2).any(|rows| {
                        (&rows[0].package, &rows[0].version) >= (&rows[1].package, &rows[1].version)
                    })
                {
                    return Err(Error::Invalid(
                        "archive source identities must be nonempty, unique and sorted".into(),
                    ));
                }
                for archive in archives {
                    if archive.package.is_empty() {
                        return Err(Error::Invalid("archive source package is empty".into()));
                    }
                    Version::parse(&archive.version).map_err(|e| Error::Invalid(e.to_string()))?;
                    validate_source_digest(&format!("sha256:{}", archive.registry_checksum))?;
                    let sha256 = match &archive.resolution_lock {
                        ResolutionLock::Published { sha256 }
                        | ResolutionLock::DocsProducer { sha256 } => sha256,
                        ResolutionLock::SeparateDocsProducer {
                            original_lock,
                            producer,
                        } => {
                            if let OriginalLock::Published { sha256 } = original_lock {
                                validate_source_digest(sha256)?;
                            }
                            validate_source_digest(&producer.config_sha256)?;
                            let prefix =
                                format!(".docs-producer/{}-{}/", archive.package, archive.version);
                            if producer.lock_path != format!("{prefix}Cargo.lock")
                                || producer.config_path != format!("{prefix}config.toml")
                            {
                                return Err(Error::Invalid(
                                    "producer resolution paths differ from owner".into(),
                                ));
                            }
                            &producer.lock_sha256
                        }
                    };
                    validate_source_digest(sha256)?;
                    if let Some(vcs) = &archive.publisher_vcs {
                        validate_publisher_vcs(vcs)?;
                    }
                    if let ResolutionLock::SeparateDocsProducer { producer, .. } =
                        &archive.resolution_lock
                    {
                        if producer.dependency_archives.windows(2).any(|p| {
                            (&p[0].package, &p[0].version) >= (&p[1].package, &p[1].version)
                        }) {
                            return Err(Error::Invalid(
                                "dependency archives must be sorted and unique".into(),
                            ));
                        }
                        for dependency in &producer.dependency_archives {
                            if dependency.package.is_empty()
                                || !dependency
                                    .package
                                    .bytes()
                                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
                            {
                                return Err(Error::Invalid("invalid dependency package".into()));
                            }
                            Version::parse(&dependency.version)
                                .map_err(|e| Error::Invalid(e.to_string()))?;
                            validate_source_digest(&format!(
                                "sha256:{}",
                                dependency.registry_checksum
                            ))?;
                            if let Some(vcs) = &dependency.publisher_vcs {
                                validate_publisher_vcs(vcs)?;
                            }
                        }
                    }
                }
                Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
            }
        }
    }
}

fn validate_publisher_vcs(vcs: &PublisherVcs) -> Result<(), Error> {
    if vcs.revision.len() != 40 || !vcs.revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::Invalid(
            "publisher VCS revision must be an original SHA1".into(),
        ));
    }
    let path = Path::new(&vcs.path_in_vcs);
    if path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(Error::Invalid(
            "publisher VCS path escapes its source".into(),
        ));
    }
    Ok(())
}

/// Cargo-derived binary target, whose private implementation is never API data.
#[derive(Clone, Debug)]
pub struct BinaryInput {
    pub rustdoc_file: PathBuf,
    pub source_file: PathBuf,
    pub readme: Option<PathBuf>,
}

/// Explicit historical scope; normal [`build_data`] remains public/stable-only.
#[derive(Clone, Debug)]
pub struct Input {
    pub released_packages: Vec<ReleasedPackage>,
    pub binaries: Vec<BinaryInput>,
    pub captured_source: Option<CapturedSource>,
}

/// Project native historical receipts with separately verified release facts.
pub fn build(input: &BuildInput, scope: &Input) -> Result<DocsData, Error> {
    if scope
        .binaries
        .iter()
        .any(|binary| !input.rustdoc_files.contains(&binary.rustdoc_file))
    {
        return Err(Error::Invalid(
            "binary input is absent from Rustdoc inputs".into(),
        ));
    }
    build_data_inner(input, Some(scope), &scope.binaries)
}

/// Cargo runs in each original normalized package, so relative native spans
/// belong to that exact captured owner rather than the aggregate imports root.
pub(super) fn package_root(
    root: &Path,
    scope: &Input,
    metadata: &PackageMetadata,
) -> Result<PathBuf, Error> {
    let Some(captured) = &scope.captured_source else {
        return Ok(root.to_path_buf());
    };
    let CapturedSource::RegistryArchives { archives } = captured else {
        return Ok(root.to_path_buf());
    };
    captured.revision()?;
    let archive = archives
        .iter()
        .find(|archive| {
            archive.package == metadata.package_name && archive.version == metadata.version
        })
        .ok_or_else(|| Error::Invalid("Rustdoc owner has no captured registry archive".into()))?;
    let owner = root.join(format!("{}-{}", archive.package, archive.version));
    reject_reparse_ancestors(&owner)?;
    let owner = owner.canonicalize().map_err(|error| {
        Error::Invalid(format!(
            "captured archive package root is unavailable: {}: {error}",
            owner.display()
        ))
    })?;
    if !owner.starts_with(root) || !owner.is_dir() {
        return Err(Error::Invalid(
            "captured archive package root escapes imports".into(),
        ));
    }
    Ok(owner)
}

#[cfg(test)]
thread_local! { static RELATIVE_SOURCE_CHECKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

pub(super) fn validate_relative_package_spans(root: &Path, krate: &Crate) -> Result<(), Error> {
    for filename in crate::relative_spans::sources(krate.index.values()) {
        #[cfg(test)]
        RELATIVE_SOURCE_CHECKS.set(RELATIVE_SOURCE_CHECKS.get() + 1);
        let source = root.join(filename);
        reject_reparse_ancestors(&source)?;
        if filename
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
            || !source
                .canonicalize()
                .is_ok_and(|path| path.starts_with(root) && path.is_file())
        {
            return Err(Error::Invalid(format!(
                "archive relative Rustdoc span has no exact owner source: {}",
                filename.display()
            )));
        }
    }
    Ok(())
}

/// Write immutable files; publish only after the producer's full drift check.
pub fn write_bundle(data: &DocsData, output: &Path, publish: bool) -> Result<(), Error> {
    let stable = Version::parse(&data.version)
        .map_err(|e| Error::Invalid(e.to_string()))?
        .pre
        .is_empty()
        && !data
            .source
            .released_packages
            .iter()
            .any(|package| package.yanked);
    write_bundle_inner(data, output, stable, publish, true)
}

pub(super) fn validate_release_identity(data: &DocsData) -> Result<(), Error> {
    if data.channel != Channel::Release
        || data.source.publication_status != PublicationStatus::RegistryReleased
        || data.source.released_packages.is_empty()
    {
        return Err(Error::Invalid(
            "historical data requires verified released package identities".into(),
        ));
    }
    let version = Version::parse(&data.version).map_err(|e| Error::Invalid(e.to_string()))?;
    if !version.build.is_empty() {
        return Err(Error::Invalid(
            "released package version cannot contain build metadata".into(),
        ));
    }
    let mut names = HashSet::new();
    for release in &data.source.released_packages {
        let source_matches = match &data.source.captured_source {
            Some(CapturedSource::RegistryArchives { archives }) => archives.iter().any(|archive| {
                archive.package == release.package
                    && archive.version == release.version
                    && archive.registry_checksum == release.registry_checksum
                    && archive
                        .publisher_vcs
                        .as_ref()
                        .map(|vcs| (&vcs.revision, &vcs.path_in_vcs))
                        == if release.source_revision.is_empty() {
                            None
                        } else {
                            Some((&release.source_revision, &release.path_in_vcs))
                        }
            }),
            _ => release.source_revision == data.source.revision,
        };
        if !names.insert(&release.package) || !source_matches || release.version != data.version {
            return Err(Error::Invalid(
                "historical package revision/version/identity differs from native data".into(),
            ));
        }
        validate_source_digest(&format!("sha256:{}", release.registry_checksum))?;
        let path = Path::new(&release.path_in_vcs);
        if release.package.is_empty()
            || path.is_absolute()
            || path
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
        {
            return Err(Error::Invalid(
                "historical package source path or name is invalid".into(),
            ));
        }
        if !data.packages.entries.iter().any(|package| {
            package.package_name == release.package && package.version == release.version
        }) {
            return Err(Error::Invalid(
                "released package has no matching native Cargo docs".into(),
            ));
        }
    }
    if data
        .packages
        .entries
        .iter()
        .any(|package| !names.contains(&package.package_name))
    {
        return Err(Error::Invalid(
            "historical docs package has no verified released archive".into(),
        ));
    }
    Ok(())
}

pub(super) fn binary_family(
    root: &Path,
    rustdoc_source_root: &Path,
    generated_sources: &HashMap<PathBuf, GeneratedSource>,
    krate: &Crate,
    binary: &BinaryInput,
) -> Result<Family, Error> {
    let item = krate
        .index
        .get(&krate.root)
        .ok_or_else(|| Error::Invalid("binary Rustdoc root is missing".into()))?;
    if !matches!(&item.inner, ItemEnum::Module(module) if module.is_crate) {
        return Err(Error::Invalid(
            "binary Rustdoc root must be a crate module".into(),
        ));
    }
    let name = item
        .name
        .clone()
        .ok_or_else(|| Error::Invalid("binary Rustdoc root name is missing".into()))?;
    reject_reparse_ancestors(&binary.source_file)?;
    let source = binary.source_file.canonicalize()?;
    let relative_source = source
        .strip_prefix(root)
        .map_err(|_| Error::Invalid("binary Cargo source escapes repository root".into()))?;
    let span = item
        .span
        .as_ref()
        .ok_or_else(|| Error::Invalid("binary Rustdoc root source span is missing".into()))?;
    let original = source_span_from_base(root, rustdoc_source_root, &HashMap::new(), span)?;
    if original.path != normalize_path(relative_source) {
        return Err(Error::Invalid(
            "binary Rustdoc span differs from Cargo target source".into(),
        ));
    }
    let projected = source_span_from_base(root, rustdoc_source_root, generated_sources, span)?;
    let mut guides = Vec::new();
    if let Some(markdown) = item.docs.as_ref().filter(|value| !value.trim().is_empty()) {
        guides.push(Guide {
            path: projected.path,
            title: titleize(&name),
            markdown: markdown.clone(),
            links: BTreeMap::new(),
        });
    }
    if let Some(readme) = &binary.readme {
        reject_reparse_ancestors(readme)?;
        let readme = readme.canonicalize()?;
        let path = readme
            .strip_prefix(root)
            .map_err(|_| Error::Invalid("binary README escapes repository root".into()))?;
        let markdown = fs::read_to_string(&readme)?;
        if !markdown.trim().is_empty()
            && !guides
                .iter()
                .any(|guide| guide.markdown.trim() == markdown.trim())
        {
            guides.push(Guide {
                path: generated_sources
                    .get(&readme)
                    .map(|source| normalize_path(&source.logical_path))
                    .unwrap_or_else(|| normalize_path(path)),
                title: guide_title(&markdown, &name),
                markdown,
                links: BTreeMap::new(),
            });
        }
    }
    if guides.is_empty() {
        return Err(Error::Invalid(
            "binary target has no source-owned comments or guides".into(),
        ));
    }
    Ok(Family {
        slug: slugify(&name),
        title: titleize(&name),
        crate_name: name,
        items: Vec::new(),
        guides,
    })
}

#[cfg(test)]
mod tests;
