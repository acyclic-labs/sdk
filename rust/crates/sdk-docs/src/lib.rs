//! Deterministic, Rust-owned data for the versioned SDK documentation site.
//!
//! The input boundary is the pinned [`rustdoc_types::Crate`] representation. The output is a
//! deliberately small public projection: private rustdoc items and compiler-only metadata never
//! cross this boundary.
use rustdoc_types::{Crate, FORMAT_VERSION, Id, Item, ItemEnum, ItemKind};
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

mod public_api;
pub mod rustdoc_profiles;

pub const DATA_SCHEMA_VERSION: &str = "sdk-docs-data.v2";
pub const LEGACY_DATA_SCHEMA_VERSION: &str = "sdk-docs-data.v1";
pub const VERSION_INDEX_SCHEMA_VERSION: &str = "sdk-docs-versions.v1";
pub const GENERATOR_VERSION: &str = "0.1.0";

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Preview,
    Release,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceInfo {
    pub revision: String,
    pub source_state: String,
    /// Digest of the trusted source manifest supplied by the generation launcher.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_sha256: Option<String>,
    pub input_sha256: String,
    pub rustdoc_format_versions: Vec<u32>,
    pub generator: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NavigationEntry {
    pub slug: String,
    pub title: String,
    pub crate_name: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Navigation {
    pub entries: Vec<NavigationEntry>,
}

/// The Rust crate identities represented by one documentation bundle.
///
/// This is derived from the typed Rustdoc families. It is deliberately a
/// projection of the Rustdoc crate graph, rather than a second package
/// manifest maintained by a website or language binding. Exact package names
/// and install instructions require the later Rust-owned package metadata
/// stage; this record does not guess them from crate spelling.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PackageEntry {
    pub package_name: String,
    pub crate_name: String,
    pub family_slug: String,
    pub version: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PackageCatalog {
    pub entries: Vec<PackageEntry>,
}

/// One deterministic search document derived from the final Rust-owned data.
/// Consumers can build an in-memory index without parsing website content.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SearchEntry {
    pub id: String,
    pub family_slug: String,
    pub title: String,
    pub path: String,
    pub kind: String,
    pub text: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SearchIndex {
    pub entries: Vec<SearchEntry>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceSpan {
    pub path: String,
    pub begin_line: usize,
    pub begin_column: usize,
    pub end_line: usize,
    pub end_column: usize,
}

/// A compiler-reported source file that was generated outside the repository
/// checkout and copied into the documentation bundle by the caller.
///
/// The physical path is attested by its exact bytes before it can satisfy a
/// Rustdoc source span. The logical path is the repository-relative path under
/// which the caller bundles those bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedSource {
    /// The exact physical file reported by Rustdoc, before canonicalization.
    pub physical_path: PathBuf,
    /// The relative path at which the caller includes the file in its source bundle.
    pub logical_path: PathBuf,
    /// A SHA-256 digest, optionally prefixed with `sha256:`.
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ApiItem {
    pub id: String,
    /// Rustdoc parent identity retained when public-api reports one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub name: String,
    pub kind: String,
    pub path: String,
    pub signature: String,
    pub docs: Option<String>,
    /// Public same-crate Rustdoc links keyed by their rendered label.
    #[serde(default)]
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub links: BTreeMap<String, Vec<String>>,
    pub source: Option<SourceSpan>,
    pub reexport: Option<String>,
    pub reexport_target: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Guide {
    pub path: String,
    pub title: String,
    pub markdown: String,
    /// Public same-crate Rustdoc links keyed by their rendered label.
    #[serde(default)]
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub links: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Family {
    pub slug: String,
    pub title: String,
    pub crate_name: String,
    pub items: Vec<ApiItem>,
    pub guides: Vec<Guide>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DocsData {
    pub schema: String,
    pub schema_version: String,
    pub version: String,
    pub channel: Channel,
    pub source: SourceInfo,
    pub navigation: Navigation,
    #[serde(default)]
    #[schemars(required)]
    pub packages: PackageCatalog,
    #[serde(default)]
    #[schemars(required)]
    pub search: SearchIndex,
    pub families: Vec<Family>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VersionEntry {
    pub version: String,
    pub channel: Channel,
    pub revision: String,
    pub data_file: String,
    pub data_sha256: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VersionIndex {
    pub schema: String,
    pub latest: Option<VersionEntry>,
    pub releases: Vec<VersionEntry>,
    pub preview: Option<VersionEntry>,
}

#[derive(Clone, Debug)]
pub struct BuildInput {
    pub version: String,
    pub channel: Channel,
    pub revision: String,
    pub source_state: String,
    pub source_sha256: Option<String>,
    pub repository_root: PathBuf,
    pub rustdoc_files: Vec<PathBuf>,
    /// Cargo metadata for each rustdoc file. Package identity and package
    /// version come from this Rust/Cargo input, never from the bundle label.
    pub package_metadata: Vec<PackageMetadata>,
    /// Generated Rust sources whose external Rustdoc spans may be projected.
    pub generated_sources: Vec<GeneratedSource>,
    pub mark_latest: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageMetadata {
    pub rustdoc_file: PathBuf,
    pub package_name: String,
    pub crate_name: String,
    pub version: String,
}

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Json(serde_json::Error),
    Invalid(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Json(e) => write!(f, "JSON error: {e}"),
            Self::Invalid(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Build a stable public data projection from one or more typed rustdoc JSON files.
pub fn build_data(input: &BuildInput) -> Result<DocsData, Error> {
    if input.version.trim().is_empty() {
        return Err(Error::Invalid("version must not be empty".into()));
    }
    if input.version != input.version.trim() {
        return Err(Error::Invalid(
            "version must not contain surrounding whitespace".into(),
        ));
    }
    if input.channel == Channel::Release {
        stable_version(&input.version)?;
    }
    if input.revision.len() < 40
        || input.revision.len() > 64
        || !input.revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(
            "revision must be a 40- to 64-character hexadecimal source identity".into(),
        ));
    }
    if input.rustdoc_files.is_empty() {
        return Err(Error::Invalid(
            "at least one rustdoc JSON file is required".into(),
        ));
    }
    if input.package_metadata.len() != input.rustdoc_files.len() {
        return Err(Error::Invalid(format!(
            "package metadata count {} does not match rustdoc input count {}",
            input.package_metadata.len(),
            input.rustdoc_files.len()
        )));
    }
    if input.mark_latest && input.channel != Channel::Release {
        return Err(Error::Invalid("only a release can be marked latest".into()));
    }
    if !matches!(
        input.source_state.as_str(),
        "captured-snapshot" | "working-tree" | "release-tag"
    ) {
        return Err(Error::Invalid(
            "source state must be captured-snapshot, working-tree, or release-tag".into(),
        ));
    }
    if let Some(source_sha256) = &input.source_sha256 {
        validate_source_digest(source_sha256)?;
    } else if input.source_state != "working-tree" {
        return Err(Error::Invalid(
            "captured snapshots and release tags require a trusted source manifest digest".into(),
        ));
    }
    let repository_root = canonical_repository_root(&input.repository_root)?;
    let generated_sources = attest_generated_sources(&repository_root, &input.generated_sources)?;
    // Callers may discover rustdoc files through different filesystem traversals.
    // Normalize the order before hashing or projecting so the same inputs always
    // produce the same identity and family order.
    let mut rustdoc_files = input.rustdoc_files.clone();
    rustdoc_files.sort_by_key(|path| normalize_path(path));
    if rustdoc_files.windows(2).any(|paths| paths[0] == paths[1]) {
        return Err(Error::Invalid(
            "rustdoc JSON input contains the same file more than once".into(),
        ));
    }
    let mut family_packages = Vec::new();
    let mut format_versions = std::collections::BTreeSet::new();
    let mut input_digest = Sha256::new();
    for path in &rustdoc_files {
        let metadata = package_metadata_for_path(&input.package_metadata, path)?;
        validate_package_metadata(metadata)?;
        input_digest.update(metadata.package_name.as_bytes());
        input_digest.update([0]);
        input_digest.update(metadata.crate_name.as_bytes());
        input_digest.update([0]);
        input_digest.update(metadata.version.as_bytes());
        input_digest.update([0]);
        let bytes = fs::read(path)?;
        if let Some(name) = path.file_name() {
            input_digest.update(name.to_string_lossy().as_bytes());
        }
        input_digest.update([0]);
        input_digest.update(&bytes);
        let krate: Crate = serde_json::from_slice(&bytes).map_err(|e| {
            Error::Invalid(format!(
                "{} is not valid typed rustdoc JSON: {e}",
                path.display()
            ))
        })?;
        if krate.format_version != FORMAT_VERSION {
            return Err(Error::Invalid(format!(
                "{} uses unsupported rustdoc format {}; this generator accepts {}",
                path.display(),
                krate.format_version,
                FORMAT_VERSION
            )));
        }
        match krate.crate_version.as_deref() {
            Some(crate_version) if crate_version != metadata.version => {
                return Err(Error::Invalid(format!(
                    "{} reports crate version {crate_version}, but Cargo metadata reports {}",
                    path.display(),
                    metadata.version
                )));
            }
            None => {
                return Err(Error::Invalid(format!(
                    "{} does not declare crate version for Cargo package {}",
                    path.display(),
                    metadata.package_name
                )));
            }
            _ => {}
        }
        if krate.includes_private {
            return Err(Error::Invalid(format!(
                "{} includes private rustdoc items; public-api requires normal public JSON",
                path.display()
            )));
        }
        format_versions.insert(krate.format_version);
        let family = build_family(
            &repository_root,
            &generated_sources,
            path,
            &bytes,
            &krate,
        )?;
        if family.crate_name != metadata.crate_name {
            return Err(Error::Invalid(format!(
                "{} has Rust crate {}, but Cargo metadata names {}",
                path.display(),
                family.crate_name,
                metadata.crate_name
            )));
        }
        family_packages.push((family, metadata.clone()));
    }
    let final_generated_sources =
        attest_generated_sources(&repository_root, &input.generated_sources)?;
    if final_generated_sources != generated_sources {
        return Err(Error::Invalid(
            "generated source changed while projecting rustdoc".into(),
        ));
    }
    family_packages.sort_by(|(left, _), (right, _)| left.slug.cmp(&right.slug));
    let families = family_packages
        .iter()
        .map(|(family, _)| family.clone())
        .collect::<Vec<_>>();
    if families.windows(2).any(|pair| pair[0].slug == pair[1].slug) {
        return Err(Error::Invalid(
            "duplicate crate family in rustdoc input".into(),
        ));
    }
    let navigation = Navigation {
        entries: navigation_entries(&families),
    };
    let packages = package_catalog(&family_packages);
    let search = search_index(&families);
    let data = DocsData {
        schema: DATA_SCHEMA_VERSION.into(),
        schema_version: DATA_SCHEMA_VERSION.into(),
        version: input.version.clone(),
        channel: input.channel.clone(),
        source: SourceInfo {
            revision: input.revision.clone(),
            source_state: input.source_state.clone(),
            source_sha256: input.source_sha256.clone(),
            input_sha256: format!("{:x}", input_digest.finalize()),
            rustdoc_format_versions: format_versions.into_iter().collect(),
            generator: format!("sdk-docs/{GENERATOR_VERSION}"),
        },
        navigation,
        packages,
        search,
        families,
    };
    validate_source_info(&data.source, &input.channel)?;
    Ok(data)
}

/// Write data, its Schemars-generated schema, and the guarded version index.
///
/// A persistent output-directory lock serializes concurrent publications
/// across index validation, bundle writes, and index replacement.
pub fn write_bundle(data: &DocsData, output_dir: &Path, mark_latest: bool) -> Result<(), Error> {
    if data.schema != DATA_SCHEMA_VERSION || data.schema_version != DATA_SCHEMA_VERSION {
        return Err(Error::Invalid(
            "cannot publish data with an unsupported docs schema".into(),
        ));
    }
    if data.channel == Channel::Release {
        stable_version(&data.version)?;
    }
    if mark_latest && data.channel != Channel::Release {
        return Err(Error::Invalid("only a release can be marked latest".into()));
    }
    validate_source_info(&data.source, &data.channel)?;
    validate_navigation(&data)?;
    reject_reparse_ancestors(output_dir)?;
    fs::create_dir_all(output_dir)?;
    let _publication_lock = lock_publication(output_dir)?;
    let index = load_version_index(output_dir)?;
    let channel_dir = match data.channel {
        Channel::Release => "releases",
        Channel::Preview => "preview",
    };
    let version_dir = output_dir
        .join(channel_dir)
        .join(safe_version(&data.version)?);
    let data_file = format!(
        "{channel_dir}/{}/sdk-docs-data.v2.json",
        safe_version(&data.version)?
    );
    let data_bytes = serde_json::to_vec_pretty(data)?;
    let digest = sha256_hex(&data_bytes);
    let data_path = output_dir.join(&data_file);
    if data_path.exists() {
        let existing = fs::read(&data_path)?;
        if sha256_hex(&existing) != digest {
            return Err(Error::Invalid(format!(
                "refusing to rewrite {} with different data",
                data_path.display()
            )));
        }
    }
    let entry = VersionEntry {
        version: data.version.clone(),
        channel: data.channel.clone(),
        revision: data.source.revision.clone(),
        data_file,
        data_sha256: digest,
    };
    let updated_index = merge_version_index(index, &entry, output_dir, mark_latest)?;
    validate_latest_release(&updated_index)?;
    reject_reparse_ancestors(&version_dir)?;
    fs::create_dir_all(&version_dir)?;
    atomic_write(&data_path, &data_bytes, true)?;
    let schema = schema_json()?;
    atomic_write(
        &version_dir.join("sdk-docs-data.v2.schema.json"),
        &serde_json::to_vec_pretty(&schema)?,
        true,
    )?;
    atomic_write(
        &output_dir.join("sdk-docs-versions.v1.schema.json"),
        &serde_json::to_vec_pretty(&schemars::schema_for!(VersionIndex))?,
        true,
    )?;
    atomic_write(
        &output_dir.join("sdk-docs-versions.v1.json"),
        &serde_json::to_vec_pretty(&updated_index)?,
        false,
    )?;
    Ok(())
}

fn load_version_index(output_dir: &Path) -> Result<VersionIndex, Error> {
    let index_path = output_dir.join("sdk-docs-versions.v1.json");
    reject_reparse_ancestors(&index_path)?;
    let index = if index_path.exists() {
        let metadata = fs::symlink_metadata(&index_path)?;
        if !metadata.is_file() {
            return Err(Error::Invalid(format!(
                "version index is not a regular file: {}",
                index_path.display()
            )));
        }
        serde_json::from_slice::<VersionIndex>(&fs::read(&index_path)?)?
    } else {
        VersionIndex {
            schema: VERSION_INDEX_SCHEMA_VERSION.into(),
            latest: None,
            releases: Vec::new(),
            preview: None,
        }
    };
    if index.schema != VERSION_INDEX_SCHEMA_VERSION {
        return Err(Error::Invalid(format!(
            "unsupported version index schema: {}",
            index.schema
        )));
    }
    validate_version_index(&index, output_dir)?;
    Ok(index)
}

fn lock_publication(output_dir: &Path) -> Result<File, Error> {
    let lock_path = output_dir.join(".sdk-docs-versions.v1.lock");
    reject_reparse_ancestors(&lock_path)?;
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    reject_reparse_ancestors(&lock_path)?;
    if !lock.metadata()?.is_file() {
        return Err(Error::Invalid(format!(
            "publication lock is not a regular file: {}",
            lock_path.display()
        )));
    }
    lock.lock()?;
    Ok(lock)
}

fn validate_version_index(index: &VersionIndex, output_dir: &Path) -> Result<(), Error> {
    let mut release_versions = HashSet::new();
    for entry in &index.releases {
        validate_version_entry(entry, &Channel::Release, output_dir)?;
        if !release_versions.insert(entry.version.as_str()) {
            return Err(Error::Invalid(format!(
                "version index contains duplicate release {}",
                entry.version
            )));
        }
    }
    if let Some(preview) = &index.preview {
        validate_version_entry(preview, &Channel::Preview, output_dir)?;
    }
    if let Some(latest) = &index.latest {
        validate_version_entry(latest, &Channel::Release, output_dir)?;
        let Some(release) = index
            .releases
            .iter()
            .find(|entry| entry.version == latest.version)
        else {
            return Err(Error::Invalid(format!(
                "version index latest release {} is absent from releases",
                latest.version
            )));
        };
        if release != latest {
            return Err(Error::Invalid(format!(
                "version index latest release {} differs from its release entry",
                latest.version
            )));
        }
    }
    validate_latest_release(index)?;
    Ok(())
}

fn validate_latest_release(index: &VersionIndex) -> Result<(), Error> {
    let Some(maximum) = index.releases.iter().max_by(|left, right| {
        match (
            release_version(&left.version),
            release_version(&right.version),
        ) {
            (Ok(left), Ok(right)) => left.cmp(&right),
            _ => left.version.cmp(&right.version),
        }
    }) else {
        return Ok(());
    };
    match index.latest.as_ref() {
        Some(latest) if latest == maximum => Ok(()),
        Some(latest) => Err(Error::Invalid(format!(
            "version index latest release {} is not the maximum release {}",
            latest.version, maximum.version
        ))),
        None => Err(Error::Invalid(format!(
            "version index latest release is missing; maximum release is {}",
            maximum.version
        ))),
    }
}

fn validate_version_entry(
    entry: &VersionEntry,
    expected_channel: &Channel,
    output_dir: &Path,
) -> Result<(), Error> {
    if &entry.channel != expected_channel {
        return Err(Error::Invalid(format!(
            "version index entry {} has the wrong channel",
            entry.version
        )));
    }
    match entry.channel {
        Channel::Release => {
            stable_version(&entry.version)?;
        }
        Channel::Preview => {
            safe_version(&entry.version)?;
        }
    }
    if entry.revision.len() < 40
        || entry.revision.len() > 64
        || !entry.revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(format!(
            "version index entry {} has an invalid source revision",
            entry.version
        )));
    }
    if entry.data_sha256.len() != 64
        || !entry
            .data_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(format!(
            "version index entry {} has an invalid data digest",
            entry.version
        )));
    }
    let channel_dir = match entry.channel {
        Channel::Release => "releases",
        Channel::Preview => "preview",
    };
    let schema = data_schema_for_file(&entry.data_file).ok_or_else(|| {
        Error::Invalid(format!(
            "version index entry {} has an unsupported data file {}",
            entry.version, entry.data_file
        ))
    })?;
    let expected_data_file = format!(
        "{channel_dir}/{}/{}.json",
        safe_version(&entry.version)?,
        schema
    );
    if entry.data_file != expected_data_file {
        return Err(Error::Invalid(format!(
            "version index entry {} has unexpected data file {}",
            entry.version, entry.data_file
        )));
    }
    let data_path = output_dir.join(&entry.data_file);
    reject_reparse_ancestors(&data_path)?;
    let metadata = fs::symlink_metadata(&data_path).map_err(|_| {
        Error::Invalid(format!(
            "version index entry {} refers to a missing data file {}",
            entry.version,
            data_path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(Error::Invalid(format!(
            "version index entry {} data path is not a regular file {}",
            entry.version,
            data_path.display()
        )));
    }
    let data_bytes = fs::read(&data_path)?;
    if sha256_hex(&data_bytes) != entry.data_sha256 {
        return Err(Error::Invalid(format!(
            "version index entry {} data digest does not match {}",
            entry.version,
            data_path.display()
        )));
    }
    if schema == DATA_SCHEMA_VERSION {
        validate_v2_document_shape(&data_bytes, &entry.version)?;
        validate_v2_schema_sidecar(output_dir, entry)?;
    }
    let data: DocsData = serde_json::from_slice(&data_bytes).map_err(|error| {
        Error::Invalid(format!(
            "version index entry {} data file is not valid DocsData: {error}",
            entry.version
        ))
    })?;
    if data.schema != schema || data.schema_version != schema {
        return Err(Error::Invalid(format!(
            "version index entry {} data file has an unsupported docs schema",
            entry.version
        )));
    }
    if data.version != entry.version {
        return Err(Error::Invalid(format!(
            "version index entry {} data version is {}",
            entry.version, data.version
        )));
    }
    if data.channel != entry.channel {
        return Err(Error::Invalid(format!(
            "version index entry {} data channel does not match the index",
            entry.version
        )));
    }
    if data.source.revision != entry.revision {
        return Err(Error::Invalid(format!(
            "version index entry {} data revision does not match the index",
            entry.version
        )));
    }
    validate_source_info(&data.source, &data.channel)?;
    validate_navigation(&data)?;
    Ok(())
}

fn validate_v2_schema_sidecar(output_dir: &Path, entry: &VersionEntry) -> Result<(), Error> {
    let data_path = output_dir.join(&entry.data_file);
    let sidecar_path = data_path
        .parent()
        .ok_or_else(|| Error::Invalid("v2 data file has no version directory".into()))?
        .join("sdk-docs-data.v2.schema.json");
    reject_reparse_ancestors(&sidecar_path)?;
    let metadata = fs::symlink_metadata(&sidecar_path).map_err(|_| {
        Error::Invalid(format!(
            "version index entry {} refers to a missing v2 schema sidecar {}",
            entry.version,
            sidecar_path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(Error::Invalid(format!(
            "version index entry {} v2 schema sidecar is not a regular file {}",
            entry.version,
            sidecar_path.display()
        )));
    }
    let expected = serde_json::to_vec_pretty(&schema_json()?)?;
    let actual = fs::read(&sidecar_path)?;
    if actual != expected {
        return Err(Error::Invalid(format!(
            "version index entry {} v2 schema sidecar does not match the pinned schema",
            entry.version
        )));
    }
    Ok(())
}

fn validate_v2_document_shape(data_bytes: &[u8], version: &str) -> Result<(), Error> {
    let value: serde_json::Value = serde_json::from_slice(data_bytes).map_err(|error| {
        Error::Invalid(format!(
            "version index entry {version} v2 data file is not valid DocsData JSON: {error}"
        ))
    })?;
    let object = value.as_object().ok_or_else(|| {
        Error::Invalid(format!(
            "version index entry {version} v2 data file must be a JSON object"
        ))
    })?;
    for key in ["schema", "schemaVersion"] {
        if object.get(key).and_then(serde_json::Value::as_str) != Some(DATA_SCHEMA_VERSION) {
            return Err(Error::Invalid(format!(
                "version index entry {version} v2 data file must set {key} to {DATA_SCHEMA_VERSION}"
            )));
        }
    }
    for key in ["packages", "search"] {
        if !object.contains_key(key) {
            return Err(Error::Invalid(format!(
                "version index entry {version} v2 data file requires {key}"
            )));
        }
    }
    Ok(())
}

fn data_schema_for_file(data_file: &str) -> Option<&'static str> {
    if data_file.ends_with("/sdk-docs-data.v2.json") {
        Some(DATA_SCHEMA_VERSION)
    } else if data_file.ends_with("/sdk-docs-data.v1.json") {
        Some(LEGACY_DATA_SCHEMA_VERSION)
    } else {
        None
    }
}

fn merge_version_index(
    mut index: VersionIndex,
    entry: &VersionEntry,
    output_dir: &Path,
    mark_latest: bool,
) -> Result<VersionIndex, Error> {
    validate_version_index(&index, output_dir)?;
    if entry.channel == Channel::Release {
        if let Some(existing) = index
            .releases
            .iter()
            .find(|item| item.version == entry.version)
        {
            if existing != entry {
                return Err(Error::Invalid(format!(
                    "refusing to rewrite release {} with a different revision or digest",
                    entry.version
                )));
            }
        } else {
            index.releases.push(entry.clone());
            index.releases.sort_by(|a, b| {
                match (release_version(&a.version), release_version(&b.version)) {
                    (Ok(a), Ok(b)) => a.cmp(&b),
                    _ => a.version.cmp(&b.version),
                }
            });
        }
        if mark_latest {
            let should_advance = match index.latest.as_ref() {
                None => true,
                Some(current) => match (
                    release_version(&entry.version),
                    release_version(&current.version),
                ) {
                    (Ok(entry), Ok(current)) => entry > current,
                    _ => false,
                },
            };
            if should_advance {
                index.latest = Some(entry.clone());
            }
        }
    } else if let Some(existing) = &index.preview {
        if existing.version == entry.version && existing != entry {
            return Err(Error::Invalid(
                "refusing to rewrite the preview version with a different revision or digest"
                    .into(),
            ));
        }
        index.preview = Some(entry.clone());
    } else {
        index.preview = Some(entry.clone());
    }
    Ok(index)
}

fn build_family(
    repository_root: &Path,
    generated_sources: &HashMap<PathBuf, GeneratedSource>,
    json_path: &Path,
    raw_json: &[u8],
    krate: &Crate,
) -> Result<Family, Error> {
    let root_item = krate.index.get(&krate.root).ok_or_else(|| {
        Error::Invalid(format!(
            "{} does not contain its declared rustdoc root item",
            json_path.display()
        ))
    })?;
    if !matches!(&root_item.inner, ItemEnum::Module(module) if module.is_crate) {
        return Err(Error::Invalid(format!(
            "{} does not declare a crate root module",
            json_path.display()
        )));
    }
    let crate_name = root_item
        .name
        .clone()
        .ok_or_else(|| Error::Invalid(format!("{} has no crate name", json_path.display())))?;
    let slug = slugify(&crate_name);
    let title = titleize(&crate_name);
    let mut public_items = public_api::extract(json_path, raw_json)?;
    deduplicate_public_items(&mut public_items);
    let public_occurrence_paths = public_occurrence_paths(&public_items, &crate_name);
    let use_occurrences = public_use_occurrences(krate, &crate_name)?;
    let mut items = Vec::new();
    for public_item in &public_items {
        let id = public_item.id;
        let path = public_item.path.clone();
        // `public-api` reports impl occurrences as public signatures too. Their
        // target may be a tuple, reference, or dyn type without a crate path;
        // retain the typed occurrence for identity/link analysis, but do not
        // subject it to the ordinary exported-item path assertion.
        if krate
            .index
            .get(&id)
            .is_some_and(|item| matches!(item.inner, ItemEnum::Impl(_)))
        {
            continue;
        }
        // public-api renders an implementation method through the receiver's
        // path. An impl owned by this crate can therefore legitimately have a
        // foreign-looking path, for example `String::from` for a local
        // `impl From<&ActorId> for String`. Keep that local implementation
        // occurrence instead of treating the receiver path as an external
        // definition.
        if path.first() != Some(&crate_name) && !is_local_impl_member(&krate, public_item) {
            return Err(Error::Invalid(format!(
                "{} public-api item {} ({}) does not resolve to the crate root: {}",
                json_path.display(),
                id.0,
                public_item.display,
                path.join("::")
            )));
        }
        let (item_id, item, target) =
            if let Some(use_id) = use_occurrences.get(&(id, path.join("::"))).copied() {
                let use_item = krate.index.get(&use_id).ok_or_else(|| {
                    Error::Invalid(format!(
                        "public use item {} is absent from rustdoc index",
                        use_id.0
                    ))
                })?;
                let target = krate.index.get(&id);
                (use_id, use_item, target)
            } else {
                let item = krate.index.get(&id).ok_or_else(|| {
                    Error::Invalid(format!(
                        "public-api item {} is absent from rustdoc index",
                        id.0
                    ))
                })?;
                (id, item, None)
            };
        if item.crate_id != 0
            || item_name(item).is_none()
            || matches!(item.inner, ItemEnum::Impl(_))
        {
            continue;
        }
        let name = item_name(item).unwrap_or_default();
        // A public `use` is represented by its own item, but Rustdoc attaches
        // inherited documentation and links to the referenced definition. Keep
        // those projections on the same effective item so aliases do not lose
        // the definition's links or accidentally expose links authored only on
        // the re-export node.
        let effective_item = target.unwrap_or(item);
        let docs = item.docs.clone().or_else(|| effective_item.docs.clone());
        let (reexport, reexport_target) = match &item.inner {
            ItemEnum::Use(use_) => (
                Some(use_.source.clone()),
                use_.id
                    .and_then(|target| krate.paths.get(&target))
                    .map(|summary| summary.path.join("::")),
            ),
            _ => (None, None),
        };
        items.push(ApiItem {
            id: format_id(item_id),
            parent_id: public_item.parent_id.map(format_id),
            name,
            kind: kind_name(item.inner.item_kind()).into(),
            path: path.join("::"),
            signature: public_item.display.clone(),
            docs,
            links: rustdoc_links(effective_item, &public_occurrence_paths),
            source: item
                .span
                .as_ref()
                .map(|span| source_span_at_root(repository_root, generated_sources, span))
                .transpose()?,
            reexport,
            reexport_target,
        });
    }
    items.sort_by(|a, b| a.path.cmp(&b.path).then(a.id.cmp(&b.id)));
    let guides = guides_from_rustdoc(
        krate,
        &crate_name,
        root_item,
        &public_items,
        &public_occurrence_paths,
        &use_occurrences,
    )?;
    Ok(Family {
        slug,
        title,
        crate_name,
        items,
        guides,
    })
}

fn navigation_entries(families: &[Family]) -> Vec<NavigationEntry> {
    let mut entries = families
        .iter()
        .map(|family| NavigationEntry {
            slug: family.slug.clone(),
            title: family.title.clone(),
            crate_name: family.crate_name.clone(),
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.slug.cmp(&right.slug));
    entries
}

fn package_catalog(families: &[(Family, PackageMetadata)]) -> PackageCatalog {
    let mut entries = families
        .iter()
        .map(|(family, metadata)| PackageEntry {
            package_name: metadata.package_name.clone(),
            crate_name: metadata.crate_name.clone(),
            family_slug: family.slug.clone(),
            version: metadata.version.clone(),
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.package_name
            .cmp(&right.package_name)
            .then(left.crate_name.cmp(&right.crate_name))
    });
    PackageCatalog { entries }
}

fn search_index(families: &[Family]) -> SearchIndex {
    let mut entries = Vec::new();
    for family in families {
        entries.push(SearchEntry {
            id: format!("family:{}", family.slug),
            family_slug: family.slug.clone(),
            title: family.title.clone(),
            path: family.slug.clone(),
            kind: "family".into(),
            text: format!("{} {}", family.title, family.crate_name),
        });
        for guide in &family.guides {
            entries.push(SearchEntry {
                id: format!("guide:{}:{}", family.slug, guide.path),
                family_slug: family.slug.clone(),
                title: guide.title.clone(),
                path: guide.path.clone(),
                kind: "guide".into(),
                text: guide.markdown.clone(),
            });
        }
        for item in &family.items {
            let docs = item.docs.as_deref().unwrap_or_default();
            entries.push(SearchEntry {
                id: format!("item:{}:{}", family.slug, item.id),
                family_slug: family.slug.clone(),
                title: item.name.clone(),
                path: item.path.clone(),
                kind: item.kind.clone(),
                text: format!("{} {} {}", item.name, item.signature, docs),
            });
        }
    }
    entries.sort_by(|left, right| left.id.cmp(&right.id));
    SearchIndex { entries }
}

fn validate_generated_catalog(data: &DocsData) -> Result<(), Error> {
    if data.packages.entries.len() != data.families.len()
        || data.packages.entries.iter().any(|entry| {
            entry.package_name.trim().is_empty()
                || entry.crate_name.trim().is_empty()
                || entry.family_slug.trim().is_empty()
                || entry.version.trim().is_empty()
                || Version::parse(&entry.version).is_err()
        })
    {
        return Err(Error::Invalid(
            "package catalog must contain one complete Cargo-backed entry per family".into(),
        ));
    }
    let mut package_names = HashSet::new();
    let mut family_slugs = HashSet::new();
    for entry in &data.packages.entries {
        if !package_names.insert(entry.package_name.as_str())
            || !family_slugs.insert(entry.family_slug.as_str())
            || !data.families.iter().any(|family| {
                family.slug == entry.family_slug && family.crate_name == entry.crate_name
            })
        {
            return Err(Error::Invalid(
                "package catalog contains duplicate or unbound Cargo identity".into(),
            ));
        }
    }
    let expected_search = search_index(&data.families);
    if data.search != expected_search {
        return Err(Error::Invalid(
            "search index must exactly match the generated Rust-owned data".into(),
        ));
    }
    Ok(())
}


fn package_metadata_for_path<'a>(
    metadata: &'a [PackageMetadata],
    path: &Path,
) -> Result<&'a PackageMetadata, Error> {
    let matches = metadata
        .iter()
        .filter(|entry| normalize_path(&entry.rustdoc_file) == normalize_path(path))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [entry] => Ok(entry),
        [] => Err(Error::Invalid(format!(
            "rustdoc input {} has no Cargo package metadata",
            path.display()
        ))),
        _ => Err(Error::Invalid(format!(
            "rustdoc input {} has duplicate Cargo package metadata",
            path.display()
        ))),
    }
}

fn validate_package_metadata(metadata: &PackageMetadata) -> Result<(), Error> {
    if metadata.package_name.trim().is_empty()
        || metadata.crate_name.trim().is_empty()
        || metadata.version.trim().is_empty()
    {
        return Err(Error::Invalid(
            "Cargo package metadata requires package name, crate name, and version".into(),
        ));
    }
    Version::parse(&metadata.version).map_err(|error| {
        Error::Invalid(format!(
            "Cargo package {} has invalid version {}: {error}",
            metadata.package_name, metadata.version
        ))
    })?;
    Ok(())
}

fn validate_navigation(data: &DocsData) -> Result<(), Error> {
    let expected = navigation_entries(&data.families);
    if data.navigation.entries != expected {
        return Err(Error::Invalid(
            "navigation entries must exactly match the sorted family entries".into(),
        ));
    }
    if data.schema == DATA_SCHEMA_VERSION {
        validate_generated_catalog(data)?;
    }
    Ok(())
}

fn public_occurrence_paths(
    public_items: &[public_api::PublicItemSignature],
    crate_name: &str,
) -> HashMap<Id, Vec<String>> {
    // Rustdoc IDs identify definitions within this blob. A public `use` alias
    // or glob contributes an occurrence path for that definition, so retain
    // those paths while keeping the link target anchored to the definition ID.
    // Only local public-api occurrences are safe to expose as stable targets.
    let mut paths = HashMap::<Id, Vec<String>>::new();
    for public_item in public_items {
        if public_item.path.first().map(String::as_str) != Some(crate_name) {
            continue;
        }
        paths
            .entry(public_item.id)
            .or_default()
            .push(public_item.path.join("::"));
    }
    for occurrences in paths.values_mut() {
        occurrences.sort();
        occurrences.dedup();
    }
    paths
}

fn is_local_impl_member(krate: &Crate, public_item: &public_api::PublicItemSignature) -> bool {
    // public-api queues both explicit impl items and inherited trait members
    // with the impl as their logical parent. Keep either only when Rustdoc
    // proves the child belongs to this local impl and, for inherited members,
    // to the local trait that supplied the default.
    let Some(item) = krate.index.get(&public_item.id) else {
        return false;
    };
    if item.crate_id != 0 {
        return false;
    }
    let Some(parent) = public_item
        .parent_id
        .and_then(|parent_id| krate.index.get(&parent_id))
    else {
        return false;
    };
    let ItemEnum::Impl(implementation) = &parent.inner else {
        return false;
    };
    if parent.crate_id != 0 {
        return false;
    }
    if implementation.items.contains(&public_item.id) {
        return true;
    }

    let Some(trait_path) = &implementation.trait_ else {
        return false;
    };
    let Some(trait_item) = krate.index.get(&trait_path.id) else {
        return false;
    };
    let Some(name) = item.name.as_deref() else {
        return false;
    };
    trait_item.crate_id == 0
        && !implementation
            .items
            .iter()
            .filter_map(|id| krate.index.get(id))
            .any(|explicit| explicit.name.as_deref() == Some(name))
        && matches!(
            &trait_item.inner,
            ItemEnum::Trait(trait_definition)
                if trait_definition.items.contains(&public_item.id)
        )
}

fn rustdoc_links(
    item: &Item,
    public_occurrence_paths: &HashMap<Id, Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
    item.links
        .iter()
        .filter_map(|(label, target)| {
            public_occurrence_paths
                .get(target)
                .filter(|paths| !paths.is_empty())
                .map(|paths| (label.clone(), paths.clone()))
        })
        .collect()
}

fn deduplicate_public_items(items: &mut Vec<public_api::PublicItemSignature>) {
    items.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then(left.path.cmp(&right.path))
            .then(left.display.cmp(&right.display))
            .then(left.parent_id.cmp(&right.parent_id))
    });
    items.dedup_by(|left, right| {
        left.id == right.id
            && left.path == right.path
            && left.display == right.display
            && left.parent_id == right.parent_id
    });
}

fn public_use_occurrences(
    krate: &Crate,
    crate_name: &str,
) -> Result<HashMap<(Id, String), Id>, Error> {
    let mut occurrences = HashMap::new();
    let mut ancestors = Vec::new();
    collect_public_use_occurrences(
        krate,
        krate.root,
        vec![crate_name.to_owned()],
        &mut occurrences,
        &mut ancestors,
        false,
    )?;
    Ok(occurrences)
}

fn collect_public_use_occurrences(
    krate: &Crate,
    id: Id,
    path: Vec<String>,
    occurrences: &mut HashMap<(Id, String), Id>,
    ancestors: &mut Vec<Id>,
    via_glob: bool,
) -> Result<(), Error> {
    // A module can be reachable through multiple public aliases. Keep the
    // current path in the cycle guard so each alias gets its own descendants,
    // while a re-export back to an ancestor terminates immediately.
    if ancestors.contains(&id) {
        return Ok(());
    }
    ancestors.push(id);
    let item = krate.index.get(&id).ok_or_else(|| {
        Error::Invalid(format!("rustdoc module item {} is absent from index", id.0))
    })?;
    let ItemEnum::Module(module) = &item.inner else {
        ancestors.pop();
        return Ok(());
    };
    for child_id in &module.items {
        let child = krate.index.get(child_id).ok_or_else(|| {
            Error::Invalid(format!(
                "rustdoc child item {} is absent from index",
                child_id.0
            ))
        })?;
        match &child.inner {
            ItemEnum::Use(use_) => {
                if let Some(target) = use_.id {
                    let mut exported = path.clone();
                    exported.push(use_.name.clone());
                    let exported_path = exported.join("::");
                    let occurrence = (target, exported_path);
                    let occurrence_is_glob = via_glob || use_.is_glob;
                    if occurrence_is_glob {
                        // Explicit re-exports win over a glob reaching the
                        // same target under the same public path.
                        occurrences.entry(occurrence).or_insert(*child_id);
                    } else {
                        occurrences.insert(occurrence, *child_id);
                    }
                    if matches!(
                        krate.index.get(&target).map(|item| &item.inner),
                        Some(ItemEnum::Module(_))
                    ) {
                        let nested_path = if use_.is_glob { path.clone() } else { exported };
                        collect_public_use_occurrences(
                            krate,
                            target,
                            nested_path,
                            occurrences,
                            ancestors,
                            occurrence_is_glob,
                        )?;
                    }
                }
            }
            ItemEnum::Module(_) => {
                if let Some(name) = item_name(child) {
                    let mut nested = path.clone();
                    nested.push(name);
                    collect_public_use_occurrences(
                        krate,
                        *child_id,
                        nested,
                        occurrences,
                        ancestors,
                        via_glob,
                    )?;
                }
            }
            _ => {}
        }
    }
    ancestors.pop();
    Ok(())
}

fn canonical_repository_root(path: &Path) -> Result<PathBuf, Error> {
    reject_reparse_ancestors(path)?;
    let root = path.canonicalize().map_err(|error| {
        Error::Invalid(format!(
            "cannot resolve rustdoc source root {}: {error}",
            path.display()
        ))
    })?;
    reject_reparse_ancestors(&root)?;
    Ok(root)
}

fn attest_generated_sources(
    repository_root: &Path,
    sources: &[GeneratedSource],
) -> Result<HashMap<PathBuf, GeneratedSource>, Error> {
    let mut attested = HashMap::new();
    let mut logical_paths = HashSet::new();
    let mut physical_paths = HashSet::new();
    for source in sources {
        let logical_path = normalize_generated_logical_path(&source.logical_path)?;
        let logical_key = if cfg!(windows) {
            logical_path.to_ascii_lowercase()
        } else {
            logical_path.clone()
        };
        if !logical_paths.insert(logical_key) {
            return Err(Error::Invalid(format!(
                "generated source logical path is duplicated: {logical_path}"
            )));
        }
        let repository_path = repository_root.join(&logical_path);
        reject_reparse_ancestors(&repository_path)?;
        match fs::symlink_metadata(&repository_path) {
            Ok(_) => {
                return Err(Error::Invalid(format!(
                    "generated source logical path collides with a repository path: {logical_path}"
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::Invalid(format!(
                    "cannot inspect generated source logical path {logical_path}: {error}"
                )));
            }
        }
        reject_reparse_ancestors(&source.physical_path)?;
        let physical_path = source.physical_path.canonicalize().map_err(|error| {
            Error::Invalid(format!(
                "cannot resolve generated source {}: {error}",
                source.physical_path.display()
            ))
        })?;
        reject_reparse_ancestors(&physical_path)?;
        let metadata = fs::symlink_metadata(&physical_path)?;
        if !metadata.is_file() {
            return Err(Error::Invalid(format!(
                "generated source is not a regular file: {}",
                physical_path.display()
            )));
        }
        let physical_key = if cfg!(windows) {
            physical_path.to_string_lossy().to_ascii_lowercase()
        } else {
            physical_path.to_string_lossy().into_owned()
        };
        if !physical_paths.insert(physical_key) {
            return Err(Error::Invalid(format!(
                "generated source physical path is duplicated: {}",
                physical_path.display()
            )));
        }
        let bytes = fs::read(&physical_path)?;
        validate_generated_sha256(&source.sha256)?;
        if !generated_sha256_matches(&source.sha256, &bytes) {
            return Err(Error::Invalid(format!(
                "generated source digest does not match {}",
                physical_path.display()
            )));
        }
        attested.insert(
            physical_path.clone(),
            GeneratedSource {
                physical_path,
                logical_path: PathBuf::from(logical_path),
                sha256: source.sha256.clone(),
            },
        );
    }
    Ok(attested)
}

fn normalize_generated_logical_path(path: &Path) -> Result<String, Error> {
    let raw = path.to_string_lossy().replace('\\', "/");
    if raw.is_empty() || raw.contains(':') {
        return Err(Error::Invalid(format!(
            "generated source logical path must be relative: {raw:?}"
        )));
    }
    let mut components = Vec::new();
    for component in Path::new(&raw).components() {
        match component {
            Component::Normal(component) => {
                let component = component.to_string_lossy().into_owned();
                if component.is_empty()
                    || !component.bytes().all(|byte| {
                        byte.is_ascii()
                            && !byte.is_ascii_control()
                            && !matches!(byte, b'<' | b'>' | b'"' | b'|' | b'?' | b'*')
                    })
                    || component.ends_with('.')
                    || component.ends_with(' ')
                    || is_windows_reserved_segment(&component)
                {
                    return Err(Error::Invalid(format!(
                        "generated source logical path contains a non-portable segment: {raw:?}"
                    )));
                }
                components.push(component)
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(Error::Invalid(format!(
                    "generated source logical path escapes its bundle root: {raw:?}"
                )));
            }
        }
    }
    if components.is_empty() {
        return Err(Error::Invalid(format!(
            "generated source logical path must not be empty: {raw:?}"
        )));
    }
    Ok(components.join("/"))
}

fn validate_generated_sha256(value: &str) -> Result<(), Error> {
    let digest = value.strip_prefix("sha256:").unwrap_or(value);
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::Invalid(format!(
            "generated source digest must be a SHA-256 value: {value:?}"
        )));
    }
    Ok(())
}

fn generated_sha256_matches(expected: &str, bytes: &[u8]) -> bool {
    expected
        .strip_prefix("sha256:")
        .unwrap_or(expected)
        .eq_ignore_ascii_case(&sha256_hex(bytes))
}

fn source_span_at_root(
    repository_root: &Path,
    generated_sources: &HashMap<PathBuf, GeneratedSource>,
    span: &rustdoc_types::Span,
) -> Result<SourceSpan, Error> {
    let source_path = if span.filename.is_absolute() {
        span.filename.clone()
    } else {
        repository_root.join(&span.filename)
    };
    reject_reparse_ancestors(&source_path)?;
    let source_path = source_path.canonicalize().map_err(|error| {
        Error::Invalid(format!(
            "cannot resolve rustdoc source span {}: {error}",
            span.filename.display()
        ))
    })?;
    let path = if let Some(generated) = generated_sources.get(&source_path) {
        normalize_path(&generated.logical_path)
    } else if let Ok(relative) = source_path.strip_prefix(&repository_root) {
        normalize_path(relative)
    } else {
        return Err(Error::Invalid(format!(
            "rustdoc source span escapes its source root without an attested generated source: {}",
            span.filename.display()
        )));
    };
    Ok(SourceSpan {
        path,
        begin_line: span.begin.0,
        begin_column: span.begin.1,
        end_line: span.end.0,
        end_column: span.end.1,
    })
}

fn item_name(item: &Item) -> Option<String> {
    item.name.clone().or_else(|| match &item.inner {
        ItemEnum::Use(use_) => Some(use_.name.clone()),
        _ => None,
    })
}

fn kind_name(kind: ItemKind) -> &'static str {
    match kind {
        ItemKind::Module => "module",
        ItemKind::ExternCrate => "extern_crate",
        ItemKind::Use => "use",
        ItemKind::Struct => "struct",
        ItemKind::StructField => "struct_field",
        ItemKind::Union => "union",
        ItemKind::Enum => "enum",
        ItemKind::Variant => "variant",
        ItemKind::Function => "function",
        ItemKind::TypeAlias => "type_alias",
        ItemKind::Constant => "constant",
        ItemKind::Static => "static",
        ItemKind::Trait => "trait",
        ItemKind::TraitAlias => "trait_alias",
        ItemKind::Impl => "impl",
        ItemKind::ExternType => "extern_type",
        ItemKind::Macro => "macro",
        ItemKind::ProcAttribute => "proc_attribute",
        ItemKind::ProcDerive => "proc_derive",
        ItemKind::Primitive => "primitive",
        ItemKind::AssocConst => "assoc_const",
        ItemKind::AssocType => "assoc_type",
        ItemKind::Keyword => "keyword",
        ItemKind::Attribute => "attribute",
    }
}

fn guides_from_rustdoc(
    krate: &Crate,
    crate_name: &str,
    root_item: &Item,
    public_items: &[public_api::PublicItemSignature],
    public_occurrence_paths: &HashMap<Id, Vec<String>>,
    use_occurrences: &HashMap<(Id, String), Id>,
) -> Result<Vec<Guide>, Error> {
    let mut guides = Vec::new();
    if let Some(markdown) = root_item
        .docs
        .clone()
        .filter(|docs| !docs.trim().is_empty())
    {
        guides.push(Guide {
            path: crate_name.to_owned(),
            title: guide_title(&markdown, crate_name),
            markdown,
            links: rustdoc_links(root_item, public_occurrence_paths),
        });
    }
    for public_item in public_items {
        if public_item.path.first().map(String::as_str) != Some(crate_name) {
            continue;
        }
        let Some(target) = krate.index.get(&public_item.id) else {
            continue;
        };
        let path = public_item.path.join("::");
        let (item, effective) = if let Some(use_id) = use_occurrences
            .get(&(public_item.id, path.clone()))
            .copied()
        {
            let use_item = krate.index.get(&use_id).ok_or_else(|| {
                Error::Invalid(format!(
                    "public use item {} is absent from rustdoc index",
                    use_id.0
                ))
            })?;
            (use_item, target)
        } else {
            (target, target)
        };
        if !matches!(&effective.inner, ItemEnum::Module(_)) {
            continue;
        }
        let Some(markdown) = item
            .docs
            .clone()
            .or_else(|| effective.docs.clone())
            .filter(|docs| !docs.trim().is_empty())
        else {
            continue;
        };
        let fallback = public_item
            .path
            .last()
            .map(String::as_str)
            .unwrap_or(crate_name);
        // Preserve links authored on a public re-export.  The definition's
        // links are only a fallback when that occurrence has no link metadata.
        let link_source = if item.links.is_empty() {
            effective
        } else {
            item
        };
        guides.push(Guide {
            path,
            title: guide_title(&markdown, fallback),
            markdown,
            links: rustdoc_links(link_source, public_occurrence_paths),
        });
    }
    guides.sort_by(|a, b| a.path.cmp(&b.path));
    guides.dedup_by(|a, b| a.path == b.path);
    Ok(guides)
}

fn guide_title(markdown: &str, fallback: &str) -> String {
    markdown
        .lines()
        .find_map(|line| line.strip_prefix("# "))
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .unwrap_or(fallback)
        .to_owned()
}
fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

fn atomic_write(path: &Path, bytes: &[u8], immutable: bool) -> Result<(), Error> {
    reject_reparse_ancestors(path)?;
    if let Ok(existing) = fs::read(path) {
        if immutable && existing != bytes {
            return Err(Error::Invalid(format!(
                "refusing to rewrite immutable file {}",
                path.display()
            )));
        }
        if immutable {
            return Ok(());
        }
    }
    let parent = path.parent().unwrap_or(path);
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    if immutable {
        file.persist_noclobber(path)
            .map_err(|error| Error::Io(error.error))?;
    } else {
        file.persist(path).map_err(|error| Error::Io(error.error))?;
    }
    Ok(())
}

fn reject_reparse_ancestors(path: &Path) -> Result<(), Error> {
    let mut current = Some(path);
    while let Some(candidate) = current {
        match fs::symlink_metadata(candidate) {
            Ok(metadata) => {
                if is_reparse_or_symlink(&metadata) {
                    return Err(Error::Invalid(format!(
                        "path contains a reparse point or symlink: {}",
                        candidate.display()
                    )));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::Invalid(format!(
                    "cannot inspect path ancestor {}: {error}",
                    candidate.display()
                )));
            }
        }
        current = candidate.parent();
    }
    Ok(())
}

fn is_reparse_or_symlink(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        return metadata.file_attributes() & 0x400 != 0;
    }
    #[cfg(not(windows))]
    {
        false
    }
}

fn stable_version(version: &str) -> Result<Version, Error> {
    let parsed = Version::parse(version).map_err(|error| {
        Error::Invalid(format!(
            "release version {version:?} is not valid semver: {error}"
        ))
    })?;
    if !parsed.pre.is_empty() {
        return Err(Error::Invalid(format!(
            "release version {version:?} must not contain a pre-release identifier"
        )));
    }
    Ok(parsed)
}

fn validate_source_digest(value: &str) -> Result<(), Error> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(Error::Invalid(
            "source manifest digest must use the sha256:<64 hex characters> form".into(),
        ));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::Invalid(
            "source manifest digest must use the sha256:<64 hex characters> form".into(),
        ));
    }
    Ok(())
}

fn validate_source_info(source: &SourceInfo, channel: &Channel) -> Result<(), Error> {
    if source.revision.len() < 40
        || source.revision.len() > 64
        || !source.revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(
            "source revision must be a 40- to 64-character hexadecimal identity".into(),
        ));
    }
    if !matches!(
        source.source_state.as_str(),
        "captured-snapshot" | "working-tree" | "release-tag"
    ) {
        return Err(Error::Invalid("unsupported source state".into()));
    }
    if matches!(channel, Channel::Release) && source.source_state == "working-tree" {
        return Err(Error::Invalid(
            "release data requires captured-snapshot or release-tag source state".into(),
        ));
    }
    if let Some(source_sha256) = &source.source_sha256 {
        validate_source_digest(source_sha256)?;
    } else if source.source_state != "working-tree" {
        return Err(Error::Invalid(
            "captured snapshots and release tags require a trusted source manifest digest".into(),
        ));
    }
    if source.input_sha256.len() != 64
        || !source
            .input_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(
            "rustdoc input digest must be 64 hexadecimal characters".into(),
        ));
    }
    if source.rustdoc_format_versions.is_empty() {
        return Err(Error::Invalid(
            "rustdoc format metadata must contain at least one version".into(),
        ));
    }
    if source
        .rustdoc_format_versions
        .iter()
        .any(|format| *format != FORMAT_VERSION)
    {
        return Err(Error::Invalid(format!(
            "unsupported rustdoc format in source metadata; expected {}",
            FORMAT_VERSION
        )));
    }
    Ok(())
}

fn release_version(version: &str) -> Result<Version, Error> {
    stable_version(version)
}
fn safe_version(version: &str) -> Result<String, Error> {
    if version.is_empty() {
        return Err(Error::Invalid(format!(
            "invalid version path segment: {version:?}"
        )));
    }

    // Keep ordinary lowercase names readable. Unsafe names use a literal
    // `~` plus lowercase UTF-8 hex so URL handling cannot decode them before
    // the static server performs its filesystem lookup. Excluding `~` from
    // the readable alphabet keeps this mapping injective.
    let encoded = if version
        .as_bytes()
        .iter()
        .all(|byte| is_portable_version_byte(*byte))
        && version != "."
        && version != ".."
        && !version.ends_with('.')
        && !version.ends_with(' ')
        && !is_windows_reserved_segment(version)
    {
        version.to_owned()
    } else {
        hex_version(version.as_bytes())
    };
    if encoded.len() > 255 {
        return Err(Error::Invalid(format!(
            "version path segment exceeds 255 bytes: {version:?}"
        )));
    }
    Ok(encoded)
}

fn is_portable_version_byte(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-' | b'_')
}

fn hex_version(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(1 + bytes.len() * 2);
    encoded.push('~');
    for byte in bytes {
        encoded.push(HEX[(*byte >> 4) as usize] as char);
        encoded.push(HEX[(*byte & 0x0f) as usize] as char);
    }
    encoded
}

fn is_windows_reserved_segment(segment: &str) -> bool {
    let stem = segment.split('.').next().unwrap_or_default();
    matches!(
        stem.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}
fn format_id(id: Id) -> String {
    id.0.to_string()
}
fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
fn slugify(value: &str) -> String {
    value.replace('_', "-").to_ascii_lowercase()
}
fn titleize(value: &str) -> String {
    value
        .split(['-', '_'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Return the schema generated directly from the output structs.
pub fn schema_json() -> Result<serde_json::Value, Error> {
    let mut schema = serde_json::to_value(schemars::schema_for!(DocsData))?;
    let object = schema
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("DocsData schema must be a JSON object".into()))?;
    let required = object
        .entry("required")
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let required = required
        .as_array_mut()
        .ok_or_else(|| Error::Invalid("DocsData schema required must be an array".into()))?;
    for field in ["packages", "search"] {
        if !required.iter().any(|value| value == field) {
            required.push(serde_json::Value::String(field.into()));
        }
    }
    let properties = object
        .get_mut("properties")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| Error::Invalid("DocsData schema properties are missing".into()))?;
    for field in ["schema", "schemaVersion"] {
        let property = properties
            .get_mut(field)
            .ok_or_else(|| Error::Invalid(format!("DocsData schema property {field} is missing")))?;
        property["const"] = serde_json::Value::String(DATA_SCHEMA_VERSION.into());
    }
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    #[test]
    fn schema_is_stable_and_identifies_the_public_contract() {
        let first = schema_json().expect("schema should serialize");
        let second = schema_json().expect("schema should serialize");
        assert_eq!(first, second);
        assert_eq!(first["title"], "DocsData");
        let required = first["required"]
            .as_array()
            .expect("DocsData JSON schema should declare required fields");
        assert!(required.iter().any(|field| field == "packages"));
        assert!(required.iter().any(|field| field == "search"));
        assert_eq!(first["properties"]["schema"]["const"], DATA_SCHEMA_VERSION);
        assert_eq!(first["properties"]["schemaVersion"]["const"], DATA_SCHEMA_VERSION);
        let mut wrong = serde_json::json!({
            "schema": "sdk-docs-data.v1",
            "schemaVersion": DATA_SCHEMA_VERSION,
            "packages": {},
            "search": {}
        });
        let error = validate_v2_document_shape(
            &serde_json::to_vec(&wrong).expect("wrong-schema fixture should serialize"),
            "1.0.0",
        )
        .expect_err("v2 schema validator must reject the wrong schema constant");
        assert!(error.to_string().contains("must set schema"));
        wrong["schema"] = DATA_SCHEMA_VERSION.into();
        wrong["schemaVersion"] = "sdk-docs-data.v1".into();
        let error = validate_v2_document_shape(
            &serde_json::to_vec(&wrong).expect("wrong-version fixture should serialize"),
            "1.0.0",
        )
        .expect_err("v2 schema validator must reject the wrong schemaVersion constant");
        assert!(error.to_string().contains("must set schemaVersion"));
    }
    #[test]
    fn slug_and_title_are_deterministic() {
        assert_eq!(slugify("sdk_stream"), "sdk-stream");
        assert_eq!(titleize("sdk-stream"), "Sdk Stream");
    }

    #[test]
    fn preview_version_paths_are_injective_and_portable() {
        assert_eq!(safe_version("preview-1").unwrap(), "preview-1");
        assert_eq!(safe_version("preview:1").unwrap(), "~707265766965773a31");
        assert_eq!(safe_version("preview_1").unwrap(), "preview_1");
        assert_eq!(safe_version("branch_ABC").unwrap(), "~6272616e63685f414243");
        assert_ne!(
            safe_version("preview:1").unwrap(),
            safe_version("preview_1").unwrap()
        );
        assert_eq!(safe_version("a%b").unwrap(), "~612562");
        assert_eq!(safe_version("CON").unwrap(), "~434f4e");
        assert_eq!(safe_version("con.txt").unwrap(), "~636f6e2e747874");
        assert_eq!(safe_version("a/b").unwrap(), "~612f62");
        assert_eq!(safe_version(&"a".repeat(255)).unwrap().len(), 255);
        assert!(
            safe_version(&"a".repeat(256))
                .unwrap_err()
                .to_string()
                .contains("exceeds 255 bytes")
        );
        assert_eq!(safe_version(&":".repeat(127)).unwrap().len(), 255);
        assert!(
            safe_version(&":".repeat(128))
                .unwrap_err()
                .to_string()
                .contains("exceeds 255 bytes")
        );
    }

    #[test]
    fn generated_source_spans_require_attestation_and_preserve_lines() {
        let suffix = std::process::id();
        let repository_root =
            std::env::temp_dir().join(format!("sdk-docs-generated-source-root-{suffix}"));
        let external_root =
            std::env::temp_dir().join(format!("sdk-docs-generated-source-external-{suffix}"));
        let _ = fs::remove_dir_all(&repository_root);
        let _ = fs::remove_dir_all(&external_root);
        fs::create_dir_all(repository_root.join("src"))
            .expect("repository root should be writable");
        fs::create_dir_all(repository_root.join("target/out"))
            .expect("generated checkout directory should be writable");
        fs::create_dir_all(&external_root).expect("external source root should be writable");

        let inside = repository_root.join("src/inside.rs");
        fs::write(&inside, b"pub struct Inside;\n").expect("checkout source should be writable");
        let external = external_root.join("wire.rs");
        let bytes = b"pub struct Wire;\n";
        fs::write(&external, bytes).expect("generated source should be writable");
        let in_tree = repository_root.join("target/out/wire.rs");
        let in_tree_bytes = b"pub struct InTreeWire;\n";
        fs::write(&in_tree, in_tree_bytes).expect("in-tree generated source should be writable");
        let digest = sha256_hex(bytes);
        let in_tree_generated = GeneratedSource {
            physical_path: in_tree.clone(),
            logical_path: PathBuf::from("generated/actors/in-tree-wire.rs"),
            sha256: sha256_hex(in_tree_bytes),
        };
        let generated = GeneratedSource {
            physical_path: external.clone(),
            logical_path: PathBuf::from("generated/actors/wire.rs"),
            sha256: digest.clone(),
        };
        let attested = attest_generated_sources(
            &repository_root,
            &[generated.clone(), in_tree_generated.clone()],
        )
        .expect("matching generated source should be attested");
        let repository_root = canonical_repository_root(&repository_root)
            .expect("repository fixture should have a canonical path");

        let external_span = rustdoc_types::Span {
            filename: external.clone(),
            begin: (7, 3),
            end: (8, 9),
        };
        let projected = source_span_at_root(&repository_root, &attested, &external_span)
            .expect("attested generated source should project");
        assert_eq!(projected.path, "generated/actors/wire.rs");
        assert_eq!(projected.begin_line, 7);
        assert_eq!(projected.begin_column, 3);
        assert_eq!(projected.end_line, 8);
        assert_eq!(projected.end_column, 9);

        let in_tree_span = rustdoc_types::Span {
            filename: in_tree.clone(),
            begin: (1, 1),
            end: (1, 24),
        };
        let in_tree_projection = source_span_at_root(&repository_root, &attested, &in_tree_span)
            .expect("attested in-tree generated source should use its logical path");
        assert_eq!(in_tree_projection.path, "generated/actors/in-tree-wire.rs");

        let checkout_span = rustdoc_types::Span {
            filename: PathBuf::from("src/inside.rs"),
            begin: (1, 1),
            end: (1, 18),
        };
        let checkout_projection = source_span_at_root(&repository_root, &attested, &checkout_span)
            .expect("checkout source should take precedence over generated mappings");
        assert_eq!(checkout_projection.path, "src/inside.rs");

        let error = source_span_at_root(&repository_root, &HashMap::new(), &external_span)
            .expect_err("unattested external source must be rejected");
        assert!(
            error
                .to_string()
                .contains("without an attested generated source")
        );

        let mut bad_digest = generated.clone();
        bad_digest.sha256 = "0".repeat(64);
        let error = attest_generated_sources(&repository_root, &[bad_digest])
            .expect_err("incorrect generated digest must be rejected");
        assert!(error.to_string().contains("digest does not match"));

        let mut escaping = generated.clone();
        escaping.logical_path = PathBuf::from("../wire.rs");
        let error = attest_generated_sources(&repository_root, &[escaping])
            .expect_err("escaping generated logical path must be rejected");
        assert!(error.to_string().contains("escapes its bundle root"));

        for logical_path in [
            "generated/actors/CON.txt",
            "generated/actors/unicode-ÃƒÆ’Ã…Â½Ãƒâ€šÃ‚Â».rs",
            "generated/actors/bad<name.rs",
            "generated/actors/bad>name.rs",
            "generated/actors/bad\"name.rs",
            "generated/actors/bad|name.rs",
            "generated/actors/bad?name.rs",
            "generated/actors/bad*name.rs",
        ] {
            let mut non_portable = generated.clone();
            non_portable.logical_path = PathBuf::from(logical_path);
            let error = attest_generated_sources(&repository_root, &[non_portable])
                .expect_err("non-portable generated logical path must be rejected");
            assert!(
                error
                    .to_string()
                    .contains("contains a non-portable segment")
            );
        }

        let collision_path = repository_root.join("generated/actors/existing.rs");
        fs::create_dir_all(
            collision_path
                .parent()
                .expect("collision parent should exist"),
        )
        .expect("collision parent should be writable");
        fs::write(&collision_path, b"pub struct Existing;\n")
            .expect("collision source should be writable");
        let mut collision = generated.clone();
        collision.logical_path = PathBuf::from("generated/actors/existing.rs");
        let error = attest_generated_sources(&repository_root, &[collision])
            .expect_err("generated logical path colliding with repository source must be rejected");
        assert!(
            error
                .to_string()
                .contains("collides with a repository path")
        );

        let mut duplicate = generated.clone();
        duplicate.logical_path = PathBuf::from("generated/actors/./wire.rs");
        let error = attest_generated_sources(&repository_root, &[generated.clone(), duplicate])
            .expect_err("normalized duplicate logical paths must be rejected");
        assert!(error.to_string().contains("logical path is duplicated"));

        fs::write(&external, b"pub struct Changed;\n").expect("generated source should be mutable");
        let error = attest_generated_sources(&repository_root, &[generated])
            .expect_err("generated source mutation must invalidate attestation");
        assert!(error.to_string().contains("digest does not match"));

        fs::remove_dir_all(&repository_root).expect("repository fixture should be removed");
        fs::remove_dir_all(&external_root).expect("external fixture should be removed");
    }

    #[cfg(windows)]
    #[test]
    fn generated_sources_reject_junction_ancestors() {
        let suffix = std::process::id();
        let repository_root = std::env::temp_dir().join(format!("sdk-docs-junction-root-{suffix}"));
        let external_root =
            std::env::temp_dir().join(format!("sdk-docs-junction-external-{suffix}"));
        let _ = fs::remove_dir_all(&repository_root);
        let _ = fs::remove_dir_all(&external_root);
        fs::create_dir_all(&repository_root).expect("repository fixture should be writable");
        fs::create_dir_all(&external_root).expect("external fixture should be writable");
        let external_file = external_root.join("wire.rs");
        let bytes = b"pub struct Wire;\n";
        fs::write(&external_file, bytes).expect("generated source should be writable");
        let junction = repository_root.join("linked");
        let status = std::process::Command::new("powershell.exe")
            .env("SDK_DOCS_JUNCTION", &junction)
            .env("SDK_DOCS_JUNCTION_TARGET", &external_root)
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "New-Item -ItemType Junction -Path $env:SDK_DOCS_JUNCTION -Target $env:SDK_DOCS_JUNCTION_TARGET -Force | Out-Null",
            ])
            .status()
            .expect("PowerShell should launch");
        if !status.success() {
            let _ = fs::remove_dir_all(&repository_root);
            let _ = fs::remove_dir_all(&external_root);
            panic!("PowerShell should create the junction");
        }
        let error = canonical_repository_root(&junction)
            .expect_err("repository root junctions must be rejected");
        assert!(error.to_string().contains("reparse point or symlink"));
        let source = GeneratedSource {
            physical_path: junction.join("wire.rs"),
            logical_path: PathBuf::from("generated/actors/junction-wire.rs"),
            sha256: sha256_hex(bytes),
        };
        let error = attest_generated_sources(&repository_root, &[source])
            .expect_err("junction ancestors must be rejected");
        assert!(error.to_string().contains("reparse point or symlink"));
        fs::remove_dir(&junction).expect("junction itself should be removable");
        fs::remove_dir_all(&repository_root).expect("repository fixture should be removed");
        fs::remove_dir_all(&external_root).expect("external fixture should be removed");
    }

    #[test]
    fn publication_rejects_navigation_that_does_not_match_sorted_families() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-navigation-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        let data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "a".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
                input_sha256: "a".repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: vec![NavigationEntry {
                    slug: "zeta".into(),
                    title: "Zeta".into(),
                    crate_name: "zeta".into(),
                }],
            },
            packages: PackageCatalog { entries: Vec::new() },
            search: SearchIndex { entries: Vec::new() },
            families: vec![
                Family {
                    slug: "zeta".into(),
                    title: "Zeta".into(),
                    crate_name: "zeta".into(),
                    items: Vec::new(),
                    guides: Vec::new(),
                },
                Family {
                    slug: "alpha".into(),
                    title: "Alpha".into(),
                    crate_name: "alpha".into(),
                    items: Vec::new(),
                    guides: Vec::new(),
                },
            ],
        };
        let error = write_bundle(&data, &output, true)
            .expect_err("publication must reject stale or incomplete navigation");
        assert!(
            error
                .to_string()
                .contains("navigation entries must exactly match")
        );
        assert!(!output.exists());
    }

    #[test]
    fn duplicate_public_occurrences_collapse_without_losing_aliases() {
        let item = public_api::PublicItemSignature {
            id: Id(1),
            parent_id: None,
            display: "pub struct Thing".into(),
            path: vec!["demo".into(), "Thing".into()],
        };
        let nested_occurrence = public_api::PublicItemSignature {
            parent_id: Some(Id(9)),
            ..item.clone()
        };
        let alias = public_api::PublicItemSignature {
            id: Id(2),
            parent_id: Some(Id(3)),
            display: "pub use demo::Thing as Alias".into(),
            path: vec!["demo".into(), "Alias".into()],
        };
        let mut items = vec![item.clone(), alias.clone(), nested_occurrence.clone(), item];
        deduplicate_public_items(&mut items);
        assert_eq!(items.len(), 3);
        assert!(items.contains(&nested_occurrence));
        assert!(items.contains(&alias));
    }

    #[test]
    fn local_impl_member_keeps_a_foreign_receiver_path() {
        let krate: Crate = serde_json::from_value(serde_json::json!({
            "root": 0,
            "crate_version": "1.0.0",
            "includes_private": false,
            "index": {
                "0": {
                    "id": 0, "crate_id": 0, "name": "demo", "span": null,
                    "visibility": "public", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"module": {"is_crate": true, "items": [7], "is_stripped": false}}
                },
                "7": {
                    "id": 7, "crate_id": 0, "name": null, "span": null,
                    "visibility": "default", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"impl": {
                        "is_unsafe": false,
                        "generics": {"params": [], "where_predicates": []},
                        "provided_trait_methods": [],
                        "trait": {"path": "From", "id": 8, "args": null},
                        "for": {"resolved_path": {"path": "String", "id": 8, "args": null}},
                        "items": [129], "is_negative": false, "is_synthetic": false,
                        "blanket_impl": null
                    }}
                },
                "129": {
                    "id": 129, "crate_id": 0, "name": "from", "span": null,
                    "visibility": "default", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"function": {
                        "sig": {"inputs": [], "output": null, "is_c_variadic": false},
                        "generics": {"params": [], "where_predicates": []},
                        "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"},
                        "has_body": true, "default_unstable": null
                    }}
                }
            },
            "paths": {},
            "external_crates": {},
            "target": {"triple": "x86_64-pc-windows-msvc", "target_features": []},
            "format_version": 60
        }))
        .expect("local implementation fixture should deserialize");
        let occurrence = public_api::PublicItemSignature {
            id: Id(129),
            parent_id: Some(Id(7)),
            display: "fn from(&ActorId) -> String".into(),
            path: ["alloc", "string", "String", "from"]
                .map(String::from)
                .to_vec(),
        };
        assert!(is_local_impl_member(&krate, &occurrence));
        let mut unparented = occurrence;
        unparented.parent_id = None;
        assert!(!is_local_impl_member(&krate, &unparented));
    }

    #[test]
    fn inherited_local_trait_associated_members_use_typed_membership() {
        let krate: Crate = serde_json::from_value(serde_json::json!({
            "root": 0,
            "crate_version": "1.0.0",
            "includes_private": false,
            "index": {
                "0": {
                    "id": 0, "crate_id": 0, "name": "demo", "span": null,
                    "visibility": "public", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"module": {"is_crate": true, "items": [8], "is_stripped": false}}
                },
                "8": {
                    "id": 8, "crate_id": 0, "name": "LocalTrait", "span": null,
                    "visibility": "public", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"trait": {
                        "is_auto": false, "is_unsafe": false, "is_dyn_compatible": true,
                        "items": [130, 131, 133],
                        "generics": {"params": [], "where_predicates": []},
                        "bounds": [], "implementations": [7]
                    }}
                },
                "7": {
                    "id": 7, "crate_id": 0, "name": null, "span": null,
                    "visibility": "default", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"impl": {
                        "is_unsafe": false,
                        "generics": {"params": [], "where_predicates": []},
                        "provided_trait_methods": [],
                        "trait": {"path": "LocalTrait", "id": 8, "args": null},
                        "for": {"resolved_path": {"path": "String", "id": 99, "args": null}},
                        "items": [132], "is_negative": false, "is_synthetic": false,
                        "blanket_impl": null
                    }}
                },
                "130": {
                    "id": 130, "crate_id": 0, "name": "VALUE", "span": null,
                    "visibility": "default", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"assoc_const": {
                        "type": {"primitive": "usize"}, "value": "0", "default_unstable": null
                    }}
                },
                "131": {
                    "id": 131, "crate_id": 0, "name": "Output", "span": null,
                    "visibility": "default", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"assoc_type": {
                        "generics": {"params": [], "where_predicates": []},
                        "bounds": [], "type": {"primitive": "usize"}, "default_unstable": null
                    }}
                },
                "132": {
                    "id": 132, "crate_id": 0, "name": "VALUE", "span": null,
                    "visibility": "default", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"assoc_const": {
                        "type": {"primitive": "usize"}, "value": "1", "default_unstable": null
                    }}
                },
                "133": {
                    "id": 133, "crate_id": 0, "name": "Other", "span": null,
                    "visibility": "default", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"assoc_const": {
                        "type": {"primitive": "usize"}, "value": "2", "default_unstable": null
                    }}
                }
            },
            "paths": {
                "99": {"crate_id": 1, "path": ["alloc", "string", "String"], "kind": "struct"}
            },
            "external_crates": {},
            "target": {"triple": "x86_64-pc-windows-msvc", "target_features": []},
            "format_version": 60
        }))
        .expect("associated-member fixture should deserialize");

        for (id, name) in [(131, "Output"), (133, "Other")] {
            let occurrence = public_api::PublicItemSignature {
                id: Id(id),
                parent_id: Some(Id(7)),
                display: format!("{name}"),
                path: vec![
                    "alloc".into(),
                    "string".into(),
                    "String".into(),
                    name.into(),
                ],
            };
            assert!(is_local_impl_member(&krate, &occurrence));
        }
        let overridden = public_api::PublicItemSignature {
            id: Id(130),
            parent_id: Some(Id(7)),
            display: "VALUE".into(),
            path: vec![
                "alloc".into(),
                "string".into(),
                "String".into(),
                "VALUE".into(),
            ],
        };
        assert!(!is_local_impl_member(&krate, &overridden));
        let explicit = public_api::PublicItemSignature {
            id: Id(132),
            parent_id: Some(Id(7)),
            display: "VALUE".into(),
            path: vec![
                "alloc".into(),
                "string".into(),
                "String".into(),
                "VALUE".into(),
            ],
        };
        assert!(is_local_impl_member(&krate, &explicit));
    }

    #[test]
    fn build_family_keeps_inherited_local_trait_member_and_source_span() {
        let root =
            std::env::temp_dir().join(format!("sdk-docs-inherited-impl-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("src")).expect("fixture source directory should be creatable");
        fs::write(
            root.join("src/lib.rs"),
            "pub trait LocalTrait { fn default(&self) {} }\n",
        )
        .expect("fixture source should be writable");
        let rustdoc_path = root.join("demo.json");
        let fixture = serde_json::json!({
            "root": 0,
            "crate_version": "1.0.0",
            "includes_private": false,
            "index": {
                "0": {
                    "id": 0, "crate_id": 0, "name": "demo", "span": null,
                    "visibility": "public", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"module": {"is_crate": true, "items": [20], "is_stripped": false}}
                },
                "20": {
                    "id": 20, "crate_id": 0, "name": "LocalTrait",
                    "span": {"filename": "src/lib.rs", "begin": [1, 1], "end": [1, 44]},
                    "visibility": "public", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"trait": {
                        "is_auto": false, "is_unsafe": false, "is_dyn_compatible": true,
                        "items": [21],
                        "generics": {"params": [], "where_predicates": []},
                        "bounds": [], "implementations": [22]
                    }}
                },
                "21": {
                    "id": 21, "crate_id": 0, "name": "default",
                    "span": {"filename": "src/lib.rs", "begin": [1, 24], "end": [1, 43]},
                    "visibility": "default", "docs": "Default behavior.", "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"function": {
                        "sig": {"inputs": [], "output": null, "is_c_variadic": false},
                        "generics": {"params": [], "where_predicates": []},
                        "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"},
                        "has_body": true, "default_unstable": null
                    }}
                },
                "22": {
                    "id": 22, "crate_id": 0, "name": null,
                    "span": {"filename": "src/lib.rs", "begin": [2, 1], "end": [2, 25]},
                    "visibility": "default", "docs": null, "links": {}, "attrs": [],
                    "deprecation": null, "stability": null, "const_stability": null,
                    "inner": {"impl": {
                        "is_unsafe": false,
                        "generics": {"params": [], "where_predicates": []},
                        "provided_trait_methods": ["default"],
                        "trait": {"path": "LocalTrait", "id": 20, "args": null},
                        "for": {"resolved_path": {"path": "String", "id": 99, "args": null}},
                        "items": [], "is_negative": false, "is_synthetic": false,
                        "blanket_impl": null
                    }}
                }
            },
            "paths": {
                "99": {"crate_id": 1, "path": ["alloc", "string", "String"], "kind": "struct"}
            },
            "external_crates": {},
            "target": {"triple": "x86_64-pc-windows-msvc", "target_features": []},
            "format_version": 60
        });
        fs::write(
            &rustdoc_path,
            serde_json::to_vec(&fixture).expect("fixture should serialize"),
        )
        .expect("fixture should be writable");
        let input = BuildInput {
            version: "1.0.0".into(),
            channel: Channel::Release,
            revision: "a".repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", "b".repeat(64))),
            repository_root: root.clone(),
            rustdoc_files: vec![rustdoc_path.clone()],
            package_metadata: vec![PackageMetadata {
                rustdoc_file: rustdoc_path.clone(),
                package_name: "demo".into(),
                crate_name: "demo".into(),
                version: "1.0.0".into(),
            }],
            generated_sources: Vec::new(),
            mark_latest: false,
        };
        let data = build_data(&input).expect("inherited local trait fixture should build");
        let family = &data.families[0];
        let inherited = family
            .items
            .iter()
            .find(|item| item.id == "21" && item.path == "alloc::string::String::default")
            .unwrap_or_else(|| {
                panic!(
                    "inherited default method should retain its foreign receiver path: {:?}",
                    family
                        .items
                        .iter()
                        .map(|item| (&item.id, &item.path, &item.signature))
                        .collect::<Vec<_>>()
                )
            });
        assert_eq!(
            inherited.source,
            Some(SourceSpan {
                path: "src/lib.rs".into(),
                begin_line: 1,
                begin_column: 24,
                end_line: 1,
                end_column: 43,
            })
        );
        assert!(
            family
                .items
                .iter()
                .any(|item| item.id == "21" && item.path == "demo::LocalTrait::default")
        );

        let mut external_trait = fixture;
        external_trait["index"]["20"]["crate_id"] = serde_json::json!(1);
        fs::write(
            &rustdoc_path,
            serde_json::to_vec(&external_trait).expect("negative fixture should serialize"),
        )
        .expect("negative fixture should be writable");
        let error = build_data(&input).expect_err("foreign inherited member must be rejected");
        let message = error.to_string();
        assert!(message.contains("does not resolve to the crate root"));
        assert!(message.contains("String::default"));
        fs::remove_dir_all(root).expect("fixture directory should be removable");
    }

    #[test]
    fn release_publication_rejects_unbound_source_before_creating_output() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-provenance-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        let mut data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "a".repeat(40),
                source_state: "working-tree".into(),
                source_sha256: None,
                input_sha256: "b".repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            packages: PackageCatalog { entries: Vec::new() },
            search: SearchIndex { entries: Vec::new() },
            families: Vec::new(),
        };
        let error = write_bundle(&data, &output, true)
            .expect_err("a release cannot publish an unbound working tree");
        assert!(error.to_string().contains("requires captured-snapshot"));
        assert!(!output.exists());

        data.source.source_state = "captured-snapshot".into();
        data.source.source_sha256 = Some(format!("sha256:{}", "c".repeat(64)));
        data.source.rustdoc_format_versions.clear();
        let error = write_bundle(&data, &output, true)
            .expect_err("publication must include Rustdoc format metadata");
        assert!(error.to_string().contains("format metadata"));
        assert!(!output.exists());
    }

    #[test]
    fn legacy_v1_release_remains_readable_when_v2_is_added() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-legacy-v1-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        let family = Family {
            slug: "legacy".into(),
            title: "Legacy".into(),
            crate_name: "legacy".into(),
            items: Vec::new(),
            guides: Vec::new(),
        };
        let old_data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "a".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "b".repeat(64))),
                input_sha256: "c".repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/0.1.0".into(),
            },
            navigation: Navigation {
                entries: vec![NavigationEntry {
                    slug: "legacy".into(),
                    title: "Legacy".into(),
                    crate_name: "legacy".into(),
                }],
            },
            packages: package_catalog(&[(
                family.clone(),
                PackageMetadata {
                    rustdoc_file: PathBuf::new(),
                    package_name: "legacy-package".into(),
                    crate_name: "legacy".into(),
                    version: "1.0.0".into(),
                },
            )]),
            search: search_index(std::slice::from_ref(&family)),
            families: vec![family],
        };
        let mut old_value = serde_json::to_value(&old_data).expect("old fixture should serialize");
        old_value["schema"] = LEGACY_DATA_SCHEMA_VERSION.into();
        old_value["schemaVersion"] = LEGACY_DATA_SCHEMA_VERSION.into();
        old_value
            .as_object_mut()
            .expect("old fixture should be an object")
            .remove("packages");
        old_value
            .as_object_mut()
            .expect("old fixture should be an object")
            .remove("search");
        let old_bytes = serde_json::to_vec_pretty(&old_value).expect("old fixture should encode");
        let old_path = output.join("releases/1.0.0/sdk-docs-data.v1.json");
        fs::create_dir_all(old_path.parent().expect("old data parent should exist"))
            .expect("old data parent should be creatable");
        fs::write(&old_path, &old_bytes).expect("old data should be writable");
        let old_entry = VersionEntry {
            version: "1.0.0".into(),
            channel: Channel::Release,
            revision: "a".repeat(40),
            data_file: "releases/1.0.0/sdk-docs-data.v1.json".into(),
            data_sha256: sha256_hex(&old_bytes),
        };
        let old_index = VersionIndex {
            schema: VERSION_INDEX_SCHEMA_VERSION.into(),
            latest: Some(old_entry.clone()),
            releases: vec![old_entry],
            preview: None,
        };
        fs::write(
            output.join("sdk-docs-versions.v1.json"),
            serde_json::to_vec_pretty(&old_index).expect("old index should encode"),
        )
        .expect("old index should be writable");

        let new_data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "2.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "d".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "e".repeat(64))),
                input_sha256: "f".repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/0.1.0".into(),
            },
            navigation: Navigation { entries: Vec::new() },
            packages: PackageCatalog { entries: Vec::new() },
            search: SearchIndex { entries: Vec::new() },
            families: Vec::new(),
        };
        let old_index_bytes = fs::read(output.join("sdk-docs-versions.v1.json"))
            .expect("old index should remain readable");
        let same_version = DocsData {
            version: "1.0.0".into(),
            source: SourceInfo {
                revision: "d".repeat(40),
                ..new_data.source.clone()
            },
            ..new_data.clone()
        };
        let error = write_bundle(&same_version, &output, true)
            .expect_err("v1 to v2 publication at one version must be immutable");
        assert!(error.to_string().contains("refusing to rewrite release 1.0.0"));
        assert_eq!(fs::read(&old_path).expect("old data should remain"), old_bytes);
        assert_eq!(
            fs::read(output.join("sdk-docs-versions.v1.json"))
                .expect("old index should remain"),
            old_index_bytes
        );
        write_bundle(&new_data, &output, true).expect("v2 publication should preserve v1 history");

        assert_eq!(fs::read(&old_path).expect("old data should remain"), old_bytes);
        let index: VersionIndex = serde_json::from_slice(
            &fs::read(output.join("sdk-docs-versions.v1.json")).expect("index should exist"),
        )
        .expect("mixed index should deserialize");
        assert_eq!(index.latest.as_ref().map(|entry| entry.version.as_str()), Some("2.0.0"));
        assert_eq!(index.releases.len(), 2);
        assert!(index
            .releases
            .iter()
            .any(|entry| entry.data_file.ends_with("sdk-docs-data.v1.json")));
        let old_roundtrip: DocsData = serde_json::from_slice(&old_bytes).expect("v1 should read");
        assert_eq!(old_roundtrip.schema, LEGACY_DATA_SCHEMA_VERSION);
        assert_eq!(old_roundtrip.version, "1.0.0");
        validate_version_index(&index, &output).expect("old and new entries should validate");
        fs::remove_dir_all(output).expect("test output should be removable");
    }

    #[test]
    fn version_index_must_be_a_regular_non_reparse_file() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-index-kind-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        fs::create_dir_all(output.join("sdk-docs-versions.v1.json"))
            .expect("index directory should be creatable");
        let data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "a".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
                input_sha256: "a".repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            packages: PackageCatalog { entries: Vec::new() },
            search: SearchIndex { entries: Vec::new() },
            families: Vec::new(),
        };
        let error = write_bundle(&data, &output, true)
            .expect_err("directory at the index path must block publication");
        assert!(
            error
                .to_string()
                .contains("version index is not a regular file")
        );
        assert!(output.join("sdk-docs-versions.v1.json").is_dir());
        assert!(!output.join("releases").exists());
        fs::remove_dir_all(output).expect("test output should be removable");
    }

    #[test]
    fn version_index_rejects_a_conflicting_release_rewrite() {
        let output = std::env::temp_dir().join(format!("sdk-docs-index-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        let data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "a".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
                input_sha256: "a".repeat(64),
                rustdoc_format_versions: vec![60],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            packages: PackageCatalog { entries: Vec::new() },
            search: SearchIndex { entries: Vec::new() },
            families: Vec::new(),
        };
        write_bundle(&data, &output, true).expect("initial release should write");
        let mut conflicting = data;
        conflicting.source.revision = "b".repeat(40);
        let error =
            write_bundle(&conflicting, &output, true).expect_err("release rewrite must fail");
        assert!(error.to_string().contains("refusing to rewrite"));
        fs::remove_dir_all(output).expect("test output should be removable");
    }

    #[test]
    fn malformed_prior_version_index_is_rejected_without_mutation() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-malformed-index-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        fs::create_dir_all(&output).expect("output directory should be creatable");
        let malformed = VersionIndex {
            schema: VERSION_INDEX_SCHEMA_VERSION.into(),
            latest: None,
            releases: vec![VersionEntry {
                version: "1.0.0".into(),
                channel: Channel::Release,
                revision: "a".repeat(40),
                data_file: "releases/1.0.0/sdk-docs-data.v2.json".into(),
                data_sha256: "0".repeat(64),
            }],
            preview: None,
        };
        let index_path = output.join("sdk-docs-versions.v1.json");
        fs::write(
            &index_path,
            serde_json::to_vec_pretty(&malformed).expect("malformed index should serialize"),
        )
        .expect("malformed index should write");
        let before = fs::read(&index_path).expect("malformed index should remain readable");
        let data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "2.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "b".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "b".repeat(64))),
                input_sha256: "b".repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            packages: PackageCatalog { entries: Vec::new() },
            search: SearchIndex { entries: Vec::new() },
            families: Vec::new(),
        };
        let error = write_bundle(&data, &output, true)
            .expect_err("malformed prior index must block publication");
        assert!(error.to_string().contains("missing data file"));
        assert_eq!(
            before,
            fs::read(&index_path).expect("malformed index should be unchanged")
        );
        assert!(!output.join("releases").exists());

        let data_path = output.join("releases/1.0.0/sdk-docs-data.v2.json");
        fs::create_dir_all(data_path.parent().expect("data file should have a parent"))
            .expect("prior data directory should be creatable");
        fs::write(&data_path, b"prior data").expect("prior data should be writable");
        let prior_data = fs::read(&data_path).expect("prior data should remain readable");
        let error = write_bundle(&data, &output, true)
            .expect_err("wrong prior data hash must block publication");
        assert!(error.to_string().contains("data digest does not match"));
        assert_eq!(
            before,
            fs::read(&index_path).expect("malformed index should remain unchanged")
        );
        assert_eq!(
            prior_data,
            fs::read(&data_path).expect("prior data should remain unchanged")
        );
        fs::remove_dir_all(output).expect("test output should be removable");
    }

    #[test]
    fn version_index_rejects_invalid_docs_json_even_when_hash_matches() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-invalid-bundle-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        let data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "a".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
                input_sha256: "a".repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            packages: PackageCatalog { entries: Vec::new() },
            search: SearchIndex { entries: Vec::new() },
            families: Vec::new(),
        };
        write_bundle(&data, &output, true).expect("initial release should write");
        let data_path = output.join("releases/1.0.0/sdk-docs-data.v2.json");
        let valid = fs::read(&data_path).expect("valid bundle should be readable");
        let schema_path = output.join("releases/1.0.0/sdk-docs-data.v2.schema.json");
        let schema = fs::read(&schema_path).expect("v2 schema sidecar should be readable");
        let index_path = output.join("sdk-docs-versions.v1.json");
        let index_before_sidecar_failure =
            fs::read(&index_path).expect("index should be readable before sidecar checks");
        fs::remove_file(&schema_path).expect("schema sidecar should be removable");
        let error = write_bundle(&data, &output, true)
            .expect_err("missing v2 schema sidecar must block publication");
        assert!(error.to_string().contains("missing v2 schema sidecar"));
        assert_eq!(valid, fs::read(&data_path).expect("data must remain unchanged"));
        assert_eq!(
            index_before_sidecar_failure,
            fs::read(&index_path).expect("index must remain unchanged")
        );
        fs::write(&schema_path, b"{}").expect("corrupt schema sidecar should be writable");
        let error = write_bundle(&data, &output, true)
            .expect_err("corrupt v2 schema sidecar must block publication");
        assert!(error.to_string().contains("does not match the pinned schema"));
        assert_eq!(valid, fs::read(&data_path).expect("data must remain unchanged"));
        assert_eq!(
            index_before_sidecar_failure,
            fs::read(&index_path).expect("index must remain unchanged")
        );
        fs::write(&schema_path, schema).expect("valid schema sidecar should be restored");
        let mut missing_packages: serde_json::Value =
            serde_json::from_slice(&valid).expect("valid bundle should be JSON");
        missing_packages
            .as_object_mut()
            .expect("v2 bundle should be an object")
            .remove("packages");
        let missing_packages_bytes = serde_json::to_vec_pretty(&missing_packages)
            .expect("missing-package bundle should serialize");
        fs::write(&data_path, &missing_packages_bytes)
            .expect("missing-package bundle should be writable");
        let mut index: VersionIndex =
            serde_json::from_slice(&fs::read(&index_path).expect("index should exist"))
                .expect("index should parse");
        let missing_digest = sha256_hex(&missing_packages_bytes);
        for entry in &mut index.releases {
            entry.data_sha256 = missing_digest.clone();
        }
        if let Some(latest) = &mut index.latest {
            latest.data_sha256 = missing_digest;
        }
        fs::write(
            &index_path,
            serde_json::to_vec_pretty(&index).expect("index should serialize"),
        )
        .expect("index should be writable");
        let before_missing = fs::read(&index_path).expect("index should remain readable");
        let error = write_bundle(&data, &output, true)
            .expect_err("v2 bundle without packages must block publication");
        assert!(error.to_string().contains("requires packages"));
        assert_eq!(before_missing, fs::read(&index_path).expect("index should be unchanged"));
        fs::write(&data_path, &valid).expect("valid bundle should be restored");
        let valid_digest = sha256_hex(&valid);
        for entry in &mut index.releases {
            entry.data_sha256 = valid_digest.clone();
        }
        if let Some(latest) = &mut index.latest {
            latest.data_sha256 = valid_digest;
        }
        fs::write(
            &index_path,
            serde_json::to_vec_pretty(&index).expect("index should serialize"),
        )
        .expect("index should be restored");
        let invalid = b"not DocsData";
        fs::write(&data_path, invalid).expect("invalid bundle should be writable");
        let index_path = output.join("sdk-docs-versions.v1.json");
        let mut index: VersionIndex =
            serde_json::from_slice(&fs::read(&index_path).expect("index should exist"))
                .expect("index should parse");
        let digest = sha256_hex(invalid);
        for entry in &mut index.releases {
            entry.data_sha256 = digest.clone();
        }
        if let Some(latest) = &mut index.latest {
            latest.data_sha256 = digest;
        }
        fs::write(
            &index_path,
            serde_json::to_vec_pretty(&index).expect("index should serialize"),
        )
        .expect("index should be writable");
        let before = fs::read(&index_path).expect("index should remain readable");
        let error = write_bundle(&data, &output, true)
            .expect_err("hash-valid invalid JSON must block publication");
        assert!(error.to_string().contains("not valid DocsData"));
        assert_eq!(
            before,
            fs::read(&index_path).expect("index should be unchanged")
        );
        fs::remove_dir_all(output).expect("test output should be removable");
    }

    #[test]
    fn version_index_rejects_bundle_revision_mismatch() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-revision-index-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        let data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "a".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
                input_sha256: "a".repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            packages: PackageCatalog { entries: Vec::new() },
            search: SearchIndex { entries: Vec::new() },
            families: Vec::new(),
        };
        write_bundle(&data, &output, true).expect("initial release should write");
        let index_path = output.join("sdk-docs-versions.v1.json");
        let mut index: VersionIndex =
            serde_json::from_slice(&fs::read(&index_path).expect("index should exist"))
                .expect("index should parse");
        for entry in &mut index.releases {
            entry.revision = "b".repeat(40);
        }
        if let Some(latest) = &mut index.latest {
            latest.revision = "b".repeat(40);
        }
        fs::write(
            &index_path,
            serde_json::to_vec_pretty(&index).expect("index should serialize"),
        )
        .expect("index should be writable");
        let before = fs::read(&index_path).expect("index should remain readable");
        let error = write_bundle(&data, &output, true)
            .expect_err("bundle revision mismatch must block publication");
        assert!(
            error
                .to_string()
                .contains("data revision does not match the index")
        );
        assert_eq!(
            before,
            fs::read(&index_path).expect("index should be unchanged")
        );
        fs::remove_dir_all(output).expect("test output should be removable");
    }

    #[test]
    fn version_index_advances_latest_and_preview_without_rewriting_releases() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-lifecycle-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        let mut data = DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: "1.0.0".into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: "1".repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", "a".repeat(64))),
                input_sha256: "a".repeat(64),
                rustdoc_format_versions: vec![60],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            packages: PackageCatalog { entries: Vec::new() },
            search: SearchIndex { entries: Vec::new() },
            families: Vec::new(),
        };
        write_bundle(&data, &output, true).expect("first release should write");
        data.version = "1.1.0".into();
        data.source.revision = "2".repeat(40);
        write_bundle(&data, &output, true).expect("latest should advance to the next release");
        data.version = "0.9.0".into();
        data.source.revision = "9".repeat(40);
        write_bundle(&data, &output, true).expect("an older release should remain immutable");
        data.version = "0.10.0".into();
        data.source.revision = "8".repeat(40);
        write_bundle(&data, &output, true).expect("semantic release ordering should be accepted");
        data.version = "preview-1".into();
        data.channel = Channel::Preview;
        data.source.revision = "3".repeat(40);
        write_bundle(&data, &output, false).expect("preview should write");
        let preview_data_path = output.join("preview/preview-1/sdk-docs-data.v2.json");
        let preview_bytes =
            fs::read(&preview_data_path).expect("preview data should remain readable");
        let preview_index_path = output.join("sdk-docs-versions.v1.json");
        let preview_index_before_conflict =
            fs::read(&preview_index_path).expect("preview index should be readable");
        let mut conflicting_preview = data.clone();
        conflicting_preview.source.revision = "f".repeat(40);
        let error = write_bundle(&conflicting_preview, &output, false)
            .expect_err("same preview version with a different revision must be immutable");
        assert!(error.to_string().contains("refusing to rewrite"));
        assert_eq!(
            preview_bytes,
            fs::read(&preview_data_path).expect("preview data must remain unchanged")
        );
        assert_eq!(
            preview_index_before_conflict,
            fs::read(&preview_index_path).expect("preview index must remain unchanged")
        );
        data.version = "preview-2".into();
        data.source.revision = "4".repeat(40);
        write_bundle(&data, &output, false).expect("preview should advance");
        data.version = "branch_abc".into();
        data.source.revision = "5".repeat(40);
        write_bundle(&data, &output, false).expect("lowercase preview should write");
        data.version = "branch_ABC".into();
        data.source.revision = "6".repeat(40);
        write_bundle(&data, &output, false).expect("case-distinct preview should write");
        let index: VersionIndex = serde_json::from_slice(
            &fs::read(output.join("sdk-docs-versions.v1.json")).expect("index should exist"),
        )
        .expect("index should parse");
        assert_eq!(
            index
                .releases
                .iter()
                .map(|entry| entry.version.as_str())
                .collect::<Vec<_>>(),
            vec!["0.9.0", "0.10.0", "1.0.0", "1.1.0"]
        );
        assert_eq!(
            index.latest.as_ref().map(|entry| entry.version.as_str()),
            Some("1.1.0")
        );
        assert_eq!(
            index.preview.as_ref().map(|entry| entry.version.as_str()),
            Some("branch_ABC")
        );
        assert!(
            output
                .join("preview/branch_abc/sdk-docs-data.v2.json")
                .is_file()
        );
        assert!(
            output
                .join("preview/~6272616e63685f414243/sdk-docs-data.v2.json")
                .is_file()
        );
        fs::remove_dir_all(output).expect("test output should be removable");
    }

    #[test]
    fn version_index_latest_must_be_the_maximum_release() {
        let entry = |version: &str| VersionEntry {
            version: version.into(),
            channel: Channel::Release,
            revision: "a".repeat(40),
            data_file: format!("releases/{version}/sdk-docs-data.v2.json"),
            data_sha256: "b".repeat(64),
        };
        let stale = VersionIndex {
            schema: VERSION_INDEX_SCHEMA_VERSION.into(),
            latest: Some(entry("1.0.0")),
            releases: vec![entry("1.0.0"), entry("2.0.0")],
            preview: None,
        };
        let error =
            validate_latest_release(&stale).expect_err("a stale latest release must be rejected");
        assert!(error.to_string().contains("maximum release 2.0.0"));

        let missing = VersionIndex {
            schema: VERSION_INDEX_SCHEMA_VERSION.into(),
            latest: None,
            releases: vec![entry("1.0.0")],
            preview: None,
        };
        let error = validate_latest_release(&missing)
            .expect_err("a non-empty release archive must have a latest release");
        assert!(error.to_string().contains("latest release is missing"));
    }

    #[test]
    fn concurrent_publications_preserve_both_entries_and_index_hashes() {
        let output =
            std::env::temp_dir().join(format!("sdk-docs-concurrent-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        let make_data = |version: &str, revision: char| DocsData {
            schema: DATA_SCHEMA_VERSION.into(),
            schema_version: DATA_SCHEMA_VERSION.into(),
            version: version.into(),
            channel: Channel::Release,
            source: SourceInfo {
                revision: revision.to_string().repeat(40),
                source_state: "captured-snapshot".into(),
                source_sha256: Some(format!("sha256:{}", revision.to_string().repeat(64))),
                input_sha256: revision.to_string().repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/test".into(),
            },
            navigation: Navigation {
                entries: Vec::new(),
            },
            packages: PackageCatalog { entries: Vec::new() },
            search: SearchIndex { entries: Vec::new() },
            families: Vec::new(),
        };
        let first_data = make_data("1.0.0", 'a');
        let second_data = make_data("2.0.0", 'b');
        let barrier = Arc::new(Barrier::new(2));
        let first_output = output.clone();
        let first_barrier = Arc::clone(&barrier);
        let first = thread::spawn(move || {
            first_barrier.wait();
            write_bundle(&first_data, &first_output, true)
        });
        let second_output = output.clone();
        let second_barrier = Arc::clone(&barrier);
        let second = thread::spawn(move || {
            second_barrier.wait();
            write_bundle(&second_data, &second_output, true)
        });
        first
            .join()
            .expect("first publication thread should complete")
            .expect("first concurrent publication should succeed");
        second
            .join()
            .expect("second publication thread should complete")
            .expect("second concurrent publication should succeed");

        let index: VersionIndex = serde_json::from_slice(
            &fs::read(output.join("sdk-docs-versions.v1.json")).expect("index should exist"),
        )
        .expect("index should parse");
        assert_eq!(
            index
                .releases
                .iter()
                .map(|entry| entry.version.as_str())
                .collect::<Vec<_>>(),
            vec!["1.0.0", "2.0.0"]
        );
        assert_eq!(
            index.latest.as_ref().map(|entry| entry.version.as_str()),
            Some("2.0.0")
        );
        for entry in &index.releases {
            let bytes = fs::read(output.join(&entry.data_file)).expect("bundle should exist");
            assert_eq!(sha256_hex(&bytes), entry.data_sha256);
        }
        fs::remove_dir_all(output).expect("test output should be removable");
    }

    #[test]
    fn typed_rustdoc_projection_respects_public_reachability_and_use_aliases() {
        let root = std::env::temp_dir().join(format!("sdk-docs-rustdoc-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("fixture directory should be creatable");
        let rustdoc_path = root.join("demo.json");
        let fixture = serde_json::json!({
            "root": 0,
            "crate_version": "1.0.0",
            "includes_private": false,
            "index": {
                "0": {"id": 0, "crate_id": 0, "name": "demo", "span": null, "visibility": "public", "docs": "# Demo\n\nRoot guide", "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"module": {"is_crate": true, "items": [2, 4, 5, 6], "is_stripped": false}}},
                "2": {"id": 2, "crate_id": 0, "name": null, "span": null, "visibility": "public", "docs": null, "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"use": {"source": "hidden::Visible", "name": "Visible", "id": 3, "is_glob": false}}},
                "3": {"id": 3, "crate_id": 0, "name": "private_function", "span": null, "visibility": "public", "docs": "hidden", "links": {"target link": 5}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"function": {"sig": {"inputs": [], "output": null, "is_c_variadic": false}, "generics": {"params": [], "where_predicates": []}, "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"}, "has_body": true, "default_unstable": null}}},
                "4": {"id": 4, "crate_id": 0, "name": "linked", "span": null, "visibility": "public", "docs": "links", "links": {"alias target": 3, "associated target": 5, "private target": 1, "external target": 99}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"function": {"sig": {"inputs": [], "output": null, "is_c_variadic": false}, "generics": {"params": [], "where_predicates": []}, "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"}, "has_body": true, "default_unstable": null}}},
                "5": {"id": 5, "crate_id": 0, "name": "associated_target", "span": null, "visibility": "public", "docs": "target", "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"function": {"sig": {"inputs": [], "output": null, "is_c_variadic": false}, "generics": {"params": [], "where_predicates": []}, "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"}, "has_body": true, "default_unstable": null}}},
                "6": {"id": 6, "crate_id": 0, "name": "nested", "span": null, "visibility": "public", "docs": "Nested guide", "links": {"associated target": 5}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"module": {"is_crate": false, "items": [7], "is_stripped": false}}},
                "7": {"id": 7, "crate_id": 0, "name": null, "span": null, "visibility": "public", "docs": null, "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"use": {"source": "crate", "name": "crate", "id": 0, "is_glob": true}}}
            },
            "paths": {"3": {"crate_id": 0, "path": ["demo", "hidden", "private_function"], "kind": "function"}, "4": {"crate_id": 0, "path": ["demo", "linked"], "kind": "function"}, "5": {"crate_id": 0, "path": ["demo", "associated_target"], "kind": "function"}},
            "external_crates": {},
            "target": {"triple": "x86_64-pc-windows-msvc", "target_features": []},
            "format_version": 60
        });
        fs::write(
            &rustdoc_path,
            serde_json::to_vec(&fixture).expect("fixture should serialize"),
        )
        .expect("fixture should write");
        let input = BuildInput {
            version: "1.0.0".into(),
            channel: Channel::Release,
            revision: "f".repeat(40),
            source_state: "captured-snapshot".into(),
            source_sha256: Some(format!("sha256:{}", "b".repeat(64))),
            repository_root: root.clone(),
            rustdoc_files: vec![rustdoc_path.clone()],
            package_metadata: vec![PackageMetadata {
                rustdoc_file: rustdoc_path.clone(),
                package_name: "demo".into(),
                crate_name: "demo".into(),
                version: "1.0.0".into(),
            }],
            generated_sources: Vec::new(),
            mark_latest: false,
        };
        let first = build_data(&input).expect("typed fixture should build");
        let second = build_data(&input).expect("typed fixture should build deterministically");
        assert_eq!(first, second);
        assert_eq!(
            first.packages.entries,
            vec![PackageEntry {
                package_name: "demo".into(),
                crate_name: "demo".into(),
                family_slug: "demo".into(),
                version: "1.0.0".into(),
            }]
        );
        let mut preview_input = input.clone();
        preview_input.version = "feature-preview".into();
        preview_input.channel = Channel::Preview;
        preview_input.source_state = "working-tree".into();
        preview_input.source_sha256 = None;
        let preview = build_data(&preview_input).expect("preview fixture should build");
        assert_eq!(preview.version, "feature-preview");
        assert_eq!(preview.packages.entries[0].version, "1.0.0");
        let mut mismatched_metadata = input.clone();
        mismatched_metadata.package_metadata[0].crate_name = "different-crate".into();
        let error = build_data(&mismatched_metadata)
            .expect_err("Cargo crate identity mismatch must fail");
        assert!(error.to_string().contains("Cargo metadata names different-crate"));
        let mut missing_metadata = input.clone();
        missing_metadata.package_metadata[0].version.clear();
        let error = build_data(&missing_metadata)
            .expect_err("missing Cargo package version must fail");
        assert!(error
            .to_string()
            .contains("requires package name, crate name, and version"));
        assert!(first
            .search
            .entries
            .iter()
            .any(|entry| entry.id == "family:demo" && entry.kind == "family"));
        assert!(first
            .search
            .entries
            .iter()
            .any(|entry| entry.id == "item:demo:4" && entry.text.contains("links")));
        let mut changed_source = input.clone();
        changed_source.source_sha256 = Some(format!("sha256:{}", "c".repeat(64)));
        let changed = build_data(&changed_source).expect("source identity should be retained");
        assert_ne!(first.source.source_sha256, changed.source.source_sha256);

        let mut alias_graph = fixture.clone();
        alias_graph["index"]["0"]["inner"]["module"]["items"] =
            serde_json::json!([2, 4, 5, 6, 8, 10, 11, 12]);
        alias_graph["index"]["8"] = serde_json::json!({
            "id": 8,
            "crate_id": 0,
            "name": "source",
            "span": null,
            "visibility": "public",
            "docs": "# Source\n\nDefinition guide",
            "links": {"target-only": 5},
            "attrs": [],
            "deprecation": null,
            "stability": null,
            "const_stability": null,
            "inner": {"module": {"is_crate": false, "items": [9, 14], "is_stripped": false}}
        });
        alias_graph["index"]["9"] = serde_json::json!({
            "id": 9,
            "crate_id": 0,
            "name": null,
            "span": null,
            "visibility": "public",
            "docs": null,
            "links": {},
            "attrs": [],
            "deprecation": null,
            "stability": null,
            "const_stability": null,
            "inner": {"use": {"source": "associated_target", "name": "associated_target", "id": 5, "is_glob": false}}
        });
        alias_graph["index"]["10"] = serde_json::json!({
            "id": 10,
            "crate_id": 0,
            "name": null,
            "span": null,
            "visibility": "public",
            "docs": "# First\n\nAlias guide",
            "links": {"alias-only": 5},
            "attrs": [],
            "deprecation": null,
            "stability": null,
            "const_stability": null,
            "inner": {"use": {"source": "source", "name": "first", "id": 8, "is_glob": false}}
        });
        alias_graph["index"]["11"] = serde_json::json!({
            "id": 11,
            "crate_id": 0,
            "name": null,
            "span": null,
            "visibility": "public",
            "docs": null,
            "links": {},
            "attrs": [],
            "deprecation": null,
            "stability": null,
            "const_stability": null,
            "inner": {"use": {"source": "source", "name": "second", "id": 8, "is_glob": false}}
        });
        alias_graph["index"]["12"] = serde_json::json!({
            "id": 12,
            "crate_id": 0,
            "name": null,
            "span": null,
            "visibility": "public",
            "docs": null,
            "links": {},
            "attrs": [],
            "deprecation": null,
            "stability": null,
            "const_stability": null,
            "inner": {"use": {"source": "source", "name": "source", "id": 8, "is_glob": true}}
        });
        alias_graph["index"]["14"] = serde_json::json!({
            "id": 14,
            "crate_id": 0,
            "name": null,
            "span": null,
            "visibility": "public",
            "docs": null,
            "links": {},
            "attrs": [],
            "deprecation": null,
            "stability": null,
            "const_stability": null,
            "inner": {"use": {"source": "source", "name": "first", "id": 8, "is_glob": false}}
        });
        let alias_graph: Crate =
            serde_json::from_value(alias_graph).expect("module alias fixture should deserialize");
        let occurrences = public_use_occurrences(&alias_graph, "demo")
            .expect("module alias occurrences should resolve");
        assert_eq!(
            occurrences.get(&(Id(5), "demo::source::associated_target".into())),
            Some(&Id(9))
        );
        assert_eq!(
            occurrences.get(&(Id(5), "demo::associated_target".into())),
            Some(&Id(9))
        );
        assert_eq!(
            occurrences.get(&(Id(5), "demo::first::associated_target".into())),
            Some(&Id(9))
        );
        assert_eq!(
            occurrences.get(&(Id(5), "demo::second::associated_target".into())),
            Some(&Id(9))
        );
        assert_eq!(
            occurrences.get(&(Id(8), "demo::first".into())),
            Some(&Id(10)),
            "an explicit re-export must win over the same path reached through a glob"
        );
        assert_eq!(
            occurrences.get(&(Id(0), "demo::nested::crate".into())),
            Some(&Id(7))
        );
        let alias_public_items = vec![
            public_api::PublicItemSignature {
                id: Id(8),
                parent_id: Some(Id(0)),
                display: "pub use source as first".into(),
                path: vec!["demo".into(), "first".into()],
            },
            public_api::PublicItemSignature {
                id: Id(8),
                parent_id: Some(Id(0)),
                display: "pub use source as second".into(),
                path: vec!["demo".into(), "second".into()],
            },
            public_api::PublicItemSignature {
                id: Id(5),
                parent_id: Some(Id(0)),
                display: "pub fn associated_target".into(),
                path: vec!["demo".into(), "associated_target".into()],
            },
        ];
        let alias_public_paths = public_occurrence_paths(&alias_public_items, "demo");
        let alias_root = alias_graph
            .index
            .get(&alias_graph.root)
            .expect("module alias fixture root should exist");
        let alias_guides = guides_from_rustdoc(
            &alias_graph,
            "demo",
            alias_root,
            &alias_public_items,
            &alias_public_paths,
            &occurrences,
        )
        .expect("module alias guide should resolve");
        let alias_guide = alias_guides
            .iter()
            .find(|guide| guide.path == "demo::first")
            .expect("the alias guide should be projected");
        assert_eq!(alias_guide.title, "First");
        assert_eq!(alias_guide.markdown, "# First\n\nAlias guide");
        assert!(alias_guide.links.contains_key("alias-only"));
        assert!(!alias_guide.links.contains_key("target-only"));
        let fallback_guide = alias_guides
            .iter()
            .find(|guide| guide.path == "demo::second")
            .expect("the target guide should be projected for an undocumented alias");
        assert!(fallback_guide.links.contains_key("target-only"));
        let missing_version_path = root.join("missing-version.json");
        let mut missing_version = fixture.clone();
        missing_version["crate_version"] = serde_json::Value::Null;
        fs::write(
            &missing_version_path,
            serde_json::to_vec(&missing_version).expect("missing-version fixture should serialize"),
        )
        .expect("missing-version fixture should write");
        let mut missing_version_input = input.clone();
        missing_version_input.rustdoc_files = vec![missing_version_path.clone()];
        missing_version_input.package_metadata[0].rustdoc_file = missing_version_path;
        let error = build_data(&missing_version_input)
            .expect_err("release rustdoc without crate version must fail");
        assert!(error.to_string().contains("does not declare crate version"));
        let family = &first.families[0];
        let alias = family
            .items
            .iter()
            .find(|item| {
                item.reexport.as_deref() == Some("hidden::Visible")
                    && item.reexport_target.as_deref() == Some("demo::hidden::private_function")
            })
            .expect("the public alias should be projected");
        assert!(alias.parent_id.is_some());
        assert_eq!(alias.docs.as_deref(), Some("hidden"));
        assert!(
            alias
                .links
                .get("target link")
                .is_some_and(|paths| paths.contains(&"demo::associated_target".to_owned()))
        );
        assert!(!alias.links.contains_key("alias-only"));
        let linked = family
            .items
            .iter()
            .find(|item| item.name == "linked")
            .expect("the link source should be projected");
        // Rustdoc resolves the alias reference to definition id 3; the
        // public occurrence map still exposes the reachable `demo::Visible`
        // use path rather than confusing the use item id with the definition.
        assert!(
            linked
                .links
                .get("alias target")
                .is_some_and(|paths| paths.contains(&"demo::Visible".to_owned()))
        );
        let associated_paths = linked
            .links
            .get("associated target")
            .expect("the associated public target should be projected");
        assert!(associated_paths.contains(&"demo::associated_target".to_owned()));
        assert!(associated_paths.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            associated_paths,
            &associated_paths
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        );
        assert!(
            !linked.links.contains_key("private target"),
            "private link unexpectedly projected: links={:?}; items={:?}",
            linked.links,
            family
                .items
                .iter()
                .map(|item| (&item.id, &item.path, &item.name))
                .collect::<Vec<_>>()
        );
        assert!(!linked.links.contains_key("external target"));
        assert_eq!(
            linked.links.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["alias target", "associated target"]
        );
        let root_guide = family
            .guides
            .iter()
            .find(|guide| guide.path == "demo")
            .expect("the crate-root guide should be projected");
        assert_eq!(root_guide.title, "Demo");
        assert_eq!(root_guide.markdown, "# Demo\n\nRoot guide");
        let guide = family
            .guides
            .iter()
            .find(|guide| guide.path == "demo::nested")
            .expect("the public module guide should be projected");
        assert!(
            guide
                .links
                .get("associated target")
                .is_some_and(|paths| paths.contains(&"demo::associated_target".to_owned()))
        );
        let linked_roundtrip: ApiItem = serde_json::from_value(
            serde_json::to_value(linked).expect("non-empty links should serialize"),
        )
        .expect("non-empty links should deserialize");
        assert_eq!(&linked_roundtrip, linked);
        let alias_json = serde_json::to_value(alias).expect("inherited links should serialize");
        let alias_links = alias_json
            .get("links")
            .and_then(serde_json::Value::as_object)
            .expect("inherited links should be present");
        assert!(alias_links.contains_key("target link"));
        assert!(!alias_links.contains_key("alias-only"));
        let alias_roundtrip: ApiItem =
            serde_json::from_value(alias_json).expect("inherited links should deserialize");
        assert_eq!(&alias_roundtrip, alias);
        let mismatched_path = root.join("mismatched.json");
        let mut mismatched = fixture.clone();
        mismatched["crate_version"] = serde_json::json!("9.9.9");
        fs::write(
            &mismatched_path,
            serde_json::to_vec(&mismatched).expect("mismatched fixture should serialize"),
        )
        .expect("mismatched fixture should write");
        let mut mismatched_input = input;
        mismatched_input.rustdoc_files = vec![mismatched_path.clone()];
        mismatched_input.package_metadata[0].rustdoc_file = mismatched_path;
        let error = build_data(&mismatched_input).expect_err("crate version mismatch must fail");
        assert!(error.to_string().contains("reports crate version 9.9.9"));
        fs::remove_dir_all(root).expect("fixture directory should be removable");
    }
}
