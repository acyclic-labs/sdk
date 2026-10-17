//! Deterministic, Rust-owned data for the versioned SDK documentation site.
//!
//! The input boundary is the pinned [`rustdoc_types::Crate`] representation. The output is a
//! deliberately small public projection: private rustdoc items and compiler-only metadata never
//! cross this boundary.
use rustdoc_types::{Crate, Id, Item, ItemEnum, ItemKind, FORMAT_VERSION};
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

mod child;
mod concrete_versions;
/// Native projections and immutable identities for historical releases.
pub mod historical;
mod path_identity;
mod public_api;
mod relative_spans;
mod render_cache;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub use child::with_command_executor;
pub use child::{command, command_environment, with_command_environment};
#[doc(hidden)]
pub use child::{command_directory, execute_metadata, CommandOutput};
pub use render_cache::{read_rustdoc, read_rustdoc_json, rustdoc_worker, with_rustdoc_cache};
/// Cargo/Rustdoc-owned profile projections.
pub mod rustdoc_profiles;
/// Rust-owned executable examples and generated consumer projections.
pub mod scenarios;

pub const DATA_SCHEMA_VERSION: &str = "sdk-docs-data.v1";
pub const VERSION_INDEX_SCHEMA_VERSION: &str = "sdk-docs-versions.v1";
pub const GENERATOR_VERSION: &str = "0.1.0";

/// Return a relocation-invariant digest for a Rustdoc receipt. Rustdoc span
/// filenames are normalized to repository-relative or basename identities
/// before hashing, while the receipt bytes used for projection remain intact.
pub fn rustdoc_digest(path: &Path, repository_root: &Path) -> Result<String, Error> {
    let bytes = read_rustdoc(path)?;
    let normalized = normalized_rustdoc_digest_bytes(&bytes, repository_root)?;
    Ok(format!("sha256:{}", sha256_hex(&normalized)))
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Preview,
    Release,
}

/// Whether a qualified bundle is a candidate or an actual registry release.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PublicationStatus {
    #[default]
    Candidate,
    RegistryReleased,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_source: Option<historical::CapturedSource>,
    #[serde(default)]
    pub publication_status: PublicationStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub released_packages: Vec<historical::ReleasedPackage>,
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
    #[schemars(required)]
    pub packages: PackageCatalog,
    #[schemars(required)]
    pub search: SearchIndex,
    pub families: Vec<Family>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VersionEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_source: Option<historical::CapturedSource>,
    #[serde(default)]
    pub publication_status: PublicationStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub released_packages: Vec<historical::ReleasedPackage>,
    pub version: String,
    pub channel: Channel,
    pub revision: String,
    pub data_file: String,
    pub data_sha256: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VersionIndex {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub release_candidates: Vec<VersionEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub historical_prereleases: Vec<VersionEntry>,
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
    /// Base of relative Rustdoc filenames for an explicitly selected owner
    /// manifest. It must remain within the captured repository/package root.
    pub rustdoc_source_root: Option<PathBuf>,
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

mod build;
pub use build::*;
mod publication;
pub use publication::*;
mod version_index;
use version_index::*;
mod projection;
use projection::*;
mod source;
use source::*;
mod guides;
use guides::*;
mod atomic_io;
use atomic_io::*;
mod versions;
pub use versions::*;

#[cfg(test)]
mod tests;
