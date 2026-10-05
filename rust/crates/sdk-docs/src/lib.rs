//! Build a deterministic, Rust-owned documentation manifest for an SDK tree.
//!
//! The source scanner is deliberately conservative. It collects Markdown and
//! Rust doc comments, records public declarations with source locations, and
//! emits diagnostics for `pub use` and conditional declarations that require
//! compiler reflection. When a matching rustdoc JSON file is available, the
//! caller can attach it to the bundle and use it as the authoritative item
//! inventory. The scanner never claims to resolve a re-export or cfg branch.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

fn git_command() -> Command {
    let mut command = Command::new("git");
    command
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR");
    command
}

/// The bundle format version. Increment when the JSON contract changes.
pub const BUNDLE_SCHEMA_VERSION: u32 = 1;

/// Errors returned while reading a repository or writing a bundle.
#[derive(Debug)]
pub enum Error {
    /// An operating-system filesystem error.
    Io(io::Error),
    /// A JSON input or output error.
    Json(serde_json::Error),
    /// The supplied repository path is not a directory.
    InvalidRepository(PathBuf),
    /// A strict bundle was requested but semantic rustdoc JSON was not
    /// available for every crate.
    Strict(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Json(error) => write!(formatter, "JSON error: {error}"),
            Self::InvalidRepository(path) => {
                write!(
                    formatter,
                    "repository root is not a directory: {}",
                    path.display()
                )
            }
            Self::Strict(message) => write!(formatter, "strict docs bundle rejected: {message}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

/// A diagnostic that prevents consumers from mistaking source scanning for
/// complete compiler reflection.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Diagnostic {
    /// `warning` or `error`.
    pub severity: String,
    /// Stable machine-readable code.
    pub code: String,
    /// Human-readable explanation.
    pub message: String,
    /// Repository-relative source path when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// One-based source line when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<usize>,
}

/// A content-addressed source file included in the bundle.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceFile {
    /// Repository-relative path using `/` separators.
    pub path: String,
    /// BLAKE3 digest of the exact UTF-8 bytes.
    pub blake3: String,
    /// UTF-8 contents. Website builders may omit this after materializing the
    /// bundle into a content-addressed store.
    pub contents: String,
}

/// The compiler-resolved target of a public re-export whose declaration lives
/// in another crate graph.  Rustdoc does not repeat the external declaration
/// in the exporting crate's index, so this stable path lets consumers resolve
/// the alias to the target family instead of treating the alias as a local
/// declaration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReexportTarget {
    /// Cargo package namespace from the first segment of the rustdoc path.
    pub package: String,
    /// Target item path after the package namespace.  A crate-root export has
    /// no path component.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Exact rustdoc `use.source` path, retained for diagnostics and links.
    pub source: String,
}

/// A public declaration discovered by rustdoc JSON or conservative source
/// scanning.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicItem {
    /// Rust declaration name as written by the source or rustdoc JSON.
    pub name: String,
    /// Compiler-resolved module path, when rustdoc provided the parent path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module_path: Option<String>,
    /// `struct`, `enum`, `trait`, `fn`, `type`, `const`, `static`, `mod`, or
    /// `macro`.
    pub kind: String,
    /// Compiler-resolved semantic signature or type shape.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<serde_json::Value>,
    /// Readable Rust declaration rendered from the compiler signature.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature_text: Option<String>,
    /// Repository-relative source location, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    /// One-based declaration line, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_line: Option<usize>,
    /// Rustdoc text immediately attached to the declaration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docs: Option<String>,
    /// `true` when this item is guarded by a `cfg` attribute.
    pub conditional: bool,
    /// `true` when the item came from a generated source path.
    pub generated: bool,
    /// External target when this item is a public re-export from another
    /// crate graph.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reexport: Option<ReexportTarget>,
}

/// Optional rustdoc JSON provenance.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustdocProvenance {
    /// Repository-relative or caller-provided JSON path.
    pub path: String,
    /// `format_version` from rustdoc JSON, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format_version: Option<u64>,
    /// BLAKE3 digest of the exact JSON bytes.
    pub blake3: String,
    /// Number of entries in the rustdoc `index` map.
    pub index_items: usize,
    /// BLAKE3 digest of the crate source inputs used for the rustdoc run.
    pub source_blake3: String,
    /// Git revision used for the rustdoc invocation.
    pub source_revision: String,
    /// Toolchain recorded by the generation receipt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub toolchain: Option<String>,
    /// Target recorded by the generation receipt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Cargo features recorded by the generation receipt.
    pub features: Vec<String>,
    /// Digest of the exact target/features/profile identity.
    pub profile_blake3: String,
    /// Receipt path and digest when the artifact was generated by this tool.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt_blake3: Option<String>,
}

/// Exact compiler inputs and artifact binding emitted beside each rustdoc JSON.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustdocReceipt {
    pub schema_version: u32,
    pub package_name: String,
    pub source_blake3: String,
    pub source_revision: String,
    pub toolchain: String,
    pub target: String,
    pub features: Vec<String>,
    pub profile_blake3: String,
    pub profile: String,
    pub rustdoc_json_blake3: String,
}

/// A compiler feature/target profile whose rustdoc output is required for
/// website and SDK coverage.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnalysisProfile {
    /// Stable profile name, such as `host-default` or `wasm-bindings`.
    pub name: String,
    /// Package-specific compiler inputs. A profile may contain native and
    /// target-only binding packages with different feature sets.
    pub packages: Vec<ProfilePackage>,
    /// Rust-owned qualification scope. Release-family profiles qualify only
    /// the named package/version; `all-pkgs` keeps the complete workspace
    /// graph requirement used by current previews.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<ProfileScope>,
}

/// Scope that binds a compiler profile to one immutable release family or to
/// the complete publishable package set.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileScope {
    /// `release-family` or `all-pkgs`.
    pub kind: String,
    /// Family encoded by the immutable release tag, such as `stream`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    /// Exact Cargo package qualified by this release profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package: Option<String>,
    /// Exact package version qualified by this release profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// One package entry in a rustdoc target/feature profile.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfilePackage {
    /// Cargo package name.
    pub package: String,
    /// Rust target triple used for this package, or `host` to resolve the
    /// pinned toolchain's native host triple at generation time.
    pub target: String,
    /// Cargo features enabled for this package. `default` is explicit when
    /// the package's manifest default features are part of the graph.
    #[serde(default)]
    pub features: Vec<String>,
    /// Whether Cargo's manifest defaults participate in the graph.
    #[serde(default = "default_features")]
    pub default_features: bool,
    /// Declared Cargo library kinds covered by this package. When omitted,
    /// generation records the crate types from the package manifest.
    #[serde(default)]
    pub crate_types: Vec<String>,
}

const fn default_features() -> bool {
    true
}

/// Result of checking one [`AnalysisProfile`] against the bundle.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileStatus {
    /// Profile identity and compiler inputs.
    pub profile: AnalysisProfile,
    /// Whether every expected package has a non-empty rustdoc public graph.
    pub complete: bool,
    /// Digest of this profile's target/features/package matrix.
    pub profile_blake3: String,
    /// Expected packages not discovered in the crate inventory.
    pub missing_packages: Vec<String>,
    /// Packages with source fallback or no resolved public items.
    pub unresolved_packages: Vec<String>,
}

/// A Markdown source that becomes a website guide or README page.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GuideSource {
    /// Repository-relative Markdown path.
    pub path: String,
    /// First level-one heading, or the package name when absent.
    pub title: String,
    /// Exact authored Markdown.
    pub contents: String,
    /// `readme` or `guide`.
    pub kind: String,
}

/// A Rust example that can become a generated website snippet.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExampleSource {
    /// Repository-relative Rust path.
    pub path: String,
    /// File stem used for navigation and snippet labels.
    pub name: String,
    /// Exact authored Rust source.
    pub contents: String,
}

/// Installation instruction derived from Cargo metadata rather than website
/// prose.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageInstruction {
    /// Package ecosystem.
    pub ecosystem: String,
    /// Package name.
    pub package: String,
    /// Exact install command.
    pub command: String,
    /// Rust-owned registry publication evidence for this package/version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registry: Option<RegistryPackageMetadata>,
}

/// Registry evidence attached to a generated package instruction.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryPackageMetadata {
    pub registry: String,
    pub version: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// Rust-owned registry publication manifest consumed by docs generation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryManifest {
    pub schema_version: u32,
    pub entries: Vec<RegistryManifestEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryManifestEntry {
    pub ecosystem: String,
    pub registry: String,
    pub package: String,
    pub version: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// Documentation and SDK inputs for one Rust crate.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrateBundle {
    /// Package name from Cargo.toml.
    pub package_name: String,
    /// Cargo crate name, if declared.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crate_name: Option<String>,
    /// Repository-relative crate directory.
    pub path: String,
    /// Whether Cargo metadata marks this package `publish = false`.
    pub publish: bool,
    /// Cargo package version, when declared by the manifest.
    pub version: Option<String>,
    /// `registry-verified`, `registry-unverified`, or `source-only`; registry
    /// status comes from the Rust-owned publication manifest.
    pub availability: String,
    /// `source-fallback` or `rustdoc-json`.
    pub analysis_mode: String,
    /// Markdown and Rust files that are source inputs for docs.
    pub sources: Vec<SourceFile>,
    /// Crate-owned Markdown guides and README pages.
    pub guides: Vec<GuideSource>,
    /// Crate-owned executable Rust examples.
    pub examples: Vec<ExampleSource>,
    /// Install commands derived from Cargo package metadata.
    pub package_instructions: Vec<PackageInstruction>,
    /// Website navigation identity derived from the crate path.
    pub navigation: String,
    /// Public declarations found by the selected analysis mode.
    pub public_items: Vec<PublicItem>,
    /// Every qualified rustdoc graph available for this package. Keeping the
    /// graphs separate prevents a host graph from being mistaken for a WASM
    /// graph when a package participates in more than one profile.
    #[serde(default)]
    pub graphs: Vec<RustdocGraph>,
    /// Rustdoc JSON provenance when supplied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rustdoc: Option<RustdocProvenance>,
    /// Diagnostics associated with this crate.
    pub diagnostics: Vec<Diagnostic>,
    /// BLAKE3 digest of the sorted source paths and per-file digests.
    pub content_blake3: String,
    /// Counts used by navigation and coverage checks.
    pub coverage: DocCoverage,
}

/// Source-owned landing metadata projected from the published Rust crate
/// inventory. The website may choose its presentation, but it does not
/// maintain a second list of SDK families or installation commands.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LandingCatalog {
    /// Version of this landing projection schema.
    pub schema_version: u32,
    /// Navigation entries in stable package order.
    pub navigation: Vec<LandingNavigationEntry>,
    /// Objective package groups used by the landing page.
    pub categories: Vec<LandingCategory>,
    /// Deduplicated install instructions from the same Rust crate inventory.
    pub package_instructions: Vec<PackageInstruction>,
}

/// One source-owned landing navigation entry.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LandingNavigationEntry {
    pub package: String,
    pub route: String,
    pub title: String,
    pub category: String,
    pub publish: bool,
}

/// A deterministic category with package identities rather than independently
/// authored website prose. `published` and `source-only` come directly from
/// Cargo's `publish` metadata.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LandingCategory {
    pub id: String,
    pub packages: Vec<String>,
}

/// Stable documentation coverage counters for one package.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocCoverage {
    pub guides: usize,
    pub examples: usize,
    pub public_items: usize,
    pub documented_items: usize,
    pub conditional_items: usize,
}

/// One source-bound semantic rustdoc graph for a package/profile pair.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustdocGraph {
    /// Profile name from the generation manifest.
    pub profile: String,
    pub target: String,
    pub features: Vec<String>,
    pub profile_blake3: String,
    pub public_items: Vec<PublicItem>,
    /// Graph-local diagnostics. A package-owned generated source gap makes
    /// this graph incomplete even when other declarations were retained.
    #[serde(default)]
    pub diagnostics: Vec<Diagnostic>,
    pub rustdoc: RustdocProvenance,
}

/// Complete website and SDK documentation input bundle.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocsBundle {
    /// Stable JSON schema version.
    pub schema_version: u32,
    /// Git revision used to build the bundle, or `unknown` outside a checkout.
    pub source_revision: String,
    /// Crates sorted by package name.
    pub crates: Vec<CrateBundle>,
    /// Repository-level diagnostics.
    pub diagnostics: Vec<Diagnostic>,
    /// Feature/target coverage statuses.
    pub profiles: Vec<ProfileStatus>,
    /// Rust-owned landing navigation, categories, and install instructions.
    pub landing: LandingCatalog,
    /// Rust-owned executable language scenarios and their qualification
    /// receipts. This remains optional so older source exports can still be
    /// scanned, while strict preview bundles can require it explicitly.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scenario_bundle: Option<serde_json::Value>,
    /// Verified release identity when an explicit release qualification
    /// manifest was supplied. Branch previews leave this absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release: Option<ReleaseQualification>,
    /// BLAKE3 digest of the canonical bundle payload excluding this field.
    pub bundle_blake3: String,
}

/// Exact Git identity accepted for a released documentation bundle.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReleaseQualification {
    pub schema: String,
    pub version: String,
    pub tag: String,
    pub revision: String,
    pub qualified: bool,
}

/// Configuration for [`build_bundle`].
#[derive(Clone, Debug)]
pub struct BuildOptions {
    /// Repository root containing `rust/crates`.
    pub repository_root: PathBuf,
    /// Optional directory or file containing compiled rustdoc JSON artifacts.
    pub rustdoc_json: Option<PathBuf>,
    /// Override the Git revision for an archive or source export.
    pub source_revision: Option<String>,
    /// Reject source-scanned crates. Website release builds should enable
    /// this after the pinned rustdoc JSON pass has completed.
    pub require_rustdoc_json: bool,
    /// Optional JSON manifest describing required feature/target profiles.
    /// `docs/rustdoc-profiles.json` is discovered automatically when present.
    pub profile_manifest: Option<PathBuf>,
    /// Installation provenance used when emitting preview instructions.
    pub source_state: String,
    /// Optional output directory produced by `acyclic-sdk-examples`.
    pub examples_bundle: Option<PathBuf>,
    /// Optional immutable source authority manifest for qualified examples.
    /// The authority is kept outside the examples bundle so a producer cannot
    /// edit source bytes and its receipt together.
    pub source_authority: Option<PathBuf>,
    /// Trusted digest of the external source authority manifest. Supplying a
    /// manifest without this configured trust-root digest is self-attesting.
    pub source_authority_sha256: Option<String>,
    /// Explicit release qualification manifest. A release bundle requires a
    /// verified Git tag, matching HEAD, and a clean worktree.
    pub release_manifest: Option<PathBuf>,
    /// Optional Rust-owned registry publication manifest.
    pub registry_manifest: Option<PathBuf>,
}

/// Inputs for the docs-only rustdoc JSON generation command.
#[derive(Clone, Debug)]
pub struct GenerateOptions {
    pub repository_root: PathBuf,
    /// Explicit immutable revision for an extracted release archive. When
    /// omitted, the exact Git revision of `repository_root` is required.
    pub source_revision: Option<String>,
    /// Repository containing the generator implementation. This is kept
    /// separate from `repository_root` so historical source checkouts can be
    /// generated while the receipt still identifies the exact generator.
    pub generator_root: Option<PathBuf>,
    pub profile_manifest: PathBuf,
    pub output_dir: PathBuf,
    pub toolchain: String,
    /// External Cargo target/cache root. Generated JSON and receipts remain
    /// in `output_dir`; compiler intermediates never become bundle inputs.
    /// The CLI also accepts `SDK_DOCS_RUSTDOC_CACHE_DIR` for CI reuse.
    pub compiler_cache_dir: Option<PathBuf>,
}

/// One generated artifact recorded in the reproducibility receipt.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GeneratedArtifact {
    pub profile: String,
    pub package_name: String,
    pub target: String,
    pub features: Vec<String>,
    pub profile_blake3: String,
    pub source_blake3: String,
    pub rustdoc_json: String,
    pub rustdoc_json_blake3: String,
    pub receipt: String,
    /// Cargo library kinds declared by the generated package.
    #[serde(default)]
    pub crate_types: Vec<String>,
    /// Rust source files included in the source-bound public coverage.
    #[serde(default)]
    pub public_source_paths: Vec<String>,
    /// Crate-owned Markdown guides and README files consumed by the website.
    #[serde(default)]
    pub guide_source_paths: Vec<String>,
    /// BLAKE3 digest of sorted guide paths and their exact bytes.
    #[serde(default)]
    pub guide_source_blake3: String,
}

/// Reproducibility receipt for one complete profile generation invocation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenerationReceipt {
    pub schema_version: u32,
    pub source_revision: String,
    pub toolchain: String,
    pub profile_manifest_blake3: String,
    pub artifacts: Vec<GeneratedArtifact>,
    /// BLAKE3 digest of the exact sdk-docs writer executable.
    #[serde(rename = "writerSHA", default)]
    pub writer_sha: String,
    /// Git revision containing the generator and source inputs.
    #[serde(rename = "generatorGitSHA", default)]
    pub generator_git_sha: String,
    /// Every source input consumed by generation, including dirty and
    /// untracked files, with the output/cache trees excluded.
    #[serde(rename = "inputClosure", default)]
    pub input_closure: Vec<GenerationInput>,
    /// Every generator source input consumed by generation, kept separate
    /// from the historical source checkout's closure.
    #[serde(rename = "generatorInputClosure", default)]
    pub generator_input_closure: Vec<GenerationInput>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenerationInput {
    pub path: String,
    pub blake3: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GenerationProvenance {
    writer_sha: String,
    source_git_sha: String,
    generator_git_sha: String,
    profile_manifest_blake3: String,
    input_closure: Vec<GenerationInput>,
    generator_input_closure: Vec<GenerationInput>,
}

/// Source-bound profile metadata for a historical checkout.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustdocProfileReceipt {
    pub schema_version: u32,
    pub source_revision: String,
    pub toolchain: String,
    pub profile_manifest_blake3: String,
    pub profiles: Vec<RustdocProfileReceiptEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustdocProfileReceiptEntry {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<ProfileScope>,
    pub packages: Vec<RustdocProfilePackageReceipt>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustdocProfilePackageReceipt {
    pub package: String,
    pub target: String,
    pub features: Vec<String>,
    pub default_features: bool,
    pub crate_types: Vec<String>,
    pub source_blake3: String,
    pub public_source_paths: Vec<String>,
    #[serde(default)]
    pub guide_source_paths: Vec<String>,
    #[serde(default)]
    pub guide_source_blake3: String,
}

#[derive(Debug, Deserialize)]
struct ProfileManifestFile {
    schema_version: u32,
    profiles: Vec<AnalysisProfile>,
}

impl BuildOptions {
    /// Build options for a repository root using conservative source scanning.
    pub fn new(repository_root: impl Into<PathBuf>) -> Self {
        Self {
            repository_root: repository_root.into(),
            rustdoc_json: None,
            source_revision: None,
            require_rustdoc_json: false,
            profile_manifest: None,
            source_state: "working-tree".to_owned(),
            examples_bundle: None,
            source_authority: None,
            source_authority_sha256: None,
            release_manifest: None,
            registry_manifest: None,
        }
    }
}

type RegistryLookup = BTreeMap<(String, String, String), RegistryPackageMetadata>;

fn load_registry_manifest(path: &Path) -> Result<RegistryLookup, Error> {
    let manifest: RegistryManifest = serde_json::from_slice(&fs::read(path)?)?;
    if manifest.schema_version != 1 {
        return Err(Error::Strict(format!(
            "unsupported registry manifest schema: {}",
            manifest.schema_version
        )));
    }
    let mut lookup = BTreeMap::new();
    for entry in manifest.entries {
        if entry.ecosystem.is_empty()
            || entry.registry.is_empty()
            || entry.package.is_empty()
            || entry.version.is_empty()
        {
            return Err(Error::Strict(
                "registry manifest entries require ecosystem, registry, package, and version"
                    .to_owned(),
            ));
        }
        if !matches!(
            entry.status.as_str(),
            "published" | "yanked" | "unavailable"
        ) {
            return Err(Error::Strict(format!(
                "invalid registry status for {}@{}: {}",
                entry.package, entry.version, entry.status
            )));
        }
        if entry.status == "published" && !entry.sha256.as_deref().is_some_and(is_sha256) {
            return Err(Error::Strict(format!(
                "published registry entry has no valid SHA-256: {}@{}",
                entry.package, entry.version
            )));
        }
        if entry.status == "unavailable" && entry.sha256.is_some() {
            return Err(Error::Strict(format!(
                "unavailable registry entry cannot carry a SHA-256: {}@{}",
                entry.package, entry.version
            )));
        }
        let package = entry.package.clone();
        let version = entry.version.clone();
        let key = (entry.ecosystem.clone(), package.clone(), version.clone());
        if lookup
            .insert(
                key,
                RegistryPackageMetadata {
                    registry: entry.registry,
                    version,
                    status: entry.status,
                    sha256: entry.sha256,
                },
            )
            .is_some()
        {
            return Err(Error::Strict(format!(
                "duplicate registry manifest entry: {}@{}",
                package, entry.version
            )));
        }
    }
    Ok(lookup)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Build a deterministic bundle from all immediate crates under
/// `repository_root/rust/crates`.
pub fn build_bundle(options: &BuildOptions) -> Result<DocsBundle, Error> {
    if !options.repository_root.is_dir() {
        return Err(Error::InvalidRepository(options.repository_root.clone()));
    }
    let crates_root = options.repository_root.join("rust/crates");
    let mut crate_dirs = fs::read_dir(&crates_root)
        .map_err(Error::Io)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir() && path.join("Cargo.toml").is_file())
        .filter(|path| !is_private_sdk_crate(path))
        .collect::<Vec<_>>();
    crate_dirs.sort();

    let mut diagnostics = Vec::new();
    let source_revision = options
        .source_revision
        .clone()
        .or_else(|| git_revision(&options.repository_root))
        .unwrap_or_else(|| {
            diagnostics.push(Diagnostic {
                severity: "warning".to_owned(),
                code: "source_revision_unavailable".to_owned(),
                message: "Git revision unavailable; bundle is source-addressed only".to_owned(),
                path: None,
                line: None,
            });
            "unknown".to_owned()
        });
    let expected_toolchain = rust_toolchain(&options.repository_root);
    let dirty_worktree = git_worktree_dirty(&options.repository_root);
    let registry_manifest = options
        .registry_manifest
        .as_deref()
        .map(load_registry_manifest)
        .transpose()?;
    let release = options
        .release_manifest
        .as_deref()
        .map(|path| {
            verify_release_qualification(
                path,
                &options.repository_root,
                &source_revision,
                dirty_worktree,
            )
        })
        .transpose()?;
    if release.is_some() && !options.require_rustdoc_json {
        return Err(Error::Strict(
            "release qualification requires --strict-rustdoc-json so source fallback cannot be published as released documentation"
                .to_owned(),
        ));
    }
    let mut crates = Vec::new();
    for crate_dir in crate_dirs {
        match scan_crate(
            &options.repository_root,
            &crate_dir,
            options.rustdoc_json.as_deref(),
            options.require_rustdoc_json,
            &source_revision,
            expected_toolchain.as_deref(),
            &options.source_state,
            dirty_worktree,
            registry_manifest.as_ref(),
        ) {
            Ok(crate_bundle) => crates.push(crate_bundle),
            Err(error) => diagnostics.push(Diagnostic {
                severity: "error".to_owned(),
                code: "crate_scan_failed".to_owned(),
                message: error.to_string(),
                path: Some(relative_path(&options.repository_root, &crate_dir)),
                line: None,
            }),
        }
    }
    crates.sort_by(|left, right| left.package_name.cmp(&right.package_name));
    let profile_manifest = options.profile_manifest.clone().or_else(|| {
        let candidate = options.repository_root.join("docs/rustdoc-profiles.json");
        candidate.is_file().then_some(candidate)
    });
    let profile_definitions = profile_manifest
        .as_deref()
        .map(load_profile_manifest)
        .transpose()?;
    let profiles = evaluate_profiles(
        profile_definitions.as_deref().unwrap_or_default(),
        &crates,
        &mut diagnostics,
        &source_revision,
        expected_toolchain.as_deref(),
    )?;
    if options.require_rustdoc_json && profiles.iter().any(|status| !status.complete) {
        return Err(Error::Strict(
            "one or more required rustdoc feature/target profiles have missing or unresolved packages".to_owned(),
        ));
    }
    if options.require_rustdoc_json {
        let required_packages = required_strict_packages(
            profile_definitions.as_deref().unwrap_or_default(),
            release.as_ref(),
            &crates,
            &options.repository_root,
        )?;
        if crates.iter().any(|crate_bundle| {
            // Cargo packages such as the historical CLI can be published as
            // binaries without a library target. They have no rustdoc graph
            // to qualify. Release-family profiles narrow this check to the
            // exact package/version that the immutable release exposes;
            // all-pkgs and unscoped profiles retain the full workspace gate.
            let crate_dir = options.repository_root.join(&crate_bundle.path);
            required_packages.contains(&crate_bundle.package_name)
                && crate_bundle.publish
                && crate_has_library_target(&crate_dir)
                && (crate_bundle.analysis_mode != "rustdoc-json"
                    || crate_bundle.public_items.is_empty())
        }) {
            return Err(Error::Strict(
                "one or more required publishable crates lack a matching compiled rustdoc JSON artifact or resolved public graph".to_owned(),
            ));
        }
    }

    let scenario_bundle = options
        .examples_bundle
        .as_deref()
        .map(|path| {
            load_scenario_bundle_with_authority(
                path,
                &options.repository_root,
                &source_revision,
                options.source_authority.as_deref(),
                options.source_authority_sha256.as_deref(),
            )
        })
        .transpose()?;

    let landing = landing_catalog(&crates);
    let payload = serde_json::json!({
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "source_revision": source_revision,
        "crates": crates,
        "diagnostics": diagnostics,
        "profiles": profiles,
        "landing": landing,
        "scenario_bundle": scenario_bundle,
        "release": release,
    });
    let bundle_blake3 = digest_bytes(canonical_json(&payload).as_bytes());
    Ok(DocsBundle {
        schema_version: BUNDLE_SCHEMA_VERSION,
        source_revision: payload["source_revision"]
            .as_str()
            .unwrap_or("unknown")
            .to_owned(),
        crates: serde_json::from_value(payload["crates"].clone())?,
        diagnostics: serde_json::from_value(payload["diagnostics"].clone())?,
        profiles: serde_json::from_value(payload["profiles"].clone())?,
        landing: serde_json::from_value(payload["landing"].clone())?,
        scenario_bundle: serde_json::from_value(payload["scenario_bundle"].clone())?,
        release: serde_json::from_value(payload["release"].clone())?,
        bundle_blake3,
    })
}

fn landing_catalog(crates: &[CrateBundle]) -> LandingCatalog {
    let navigation = crates
        .iter()
        .map(|crate_bundle| LandingNavigationEntry {
            package: crate_bundle.package_name.clone(),
            route: crate_bundle.navigation.clone(),
            title: crate_bundle
                .package_name
                .strip_prefix("acyclic-")
                .unwrap_or(&crate_bundle.package_name)
                .split('-')
                .map(|part| {
                    let mut chars = part.chars();
                    chars
                        .next()
                        .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(" "),
            category: if crate_bundle.publish {
                "published".to_owned()
            } else {
                "source-only".to_owned()
            },
            publish: crate_bundle.publish,
        })
        .collect::<Vec<_>>();
    let mut published = Vec::new();
    let mut source_only = Vec::new();
    let mut package_instructions = Vec::new();
    for crate_bundle in crates {
        if crate_bundle.publish {
            published.push(crate_bundle.package_name.clone());
        } else {
            source_only.push(crate_bundle.package_name.clone());
        }
        package_instructions.extend(crate_bundle.package_instructions.iter().cloned());
    }
    package_instructions.sort_by(|left, right| {
        left.ecosystem
            .cmp(&right.ecosystem)
            .then(left.package.cmp(&right.package))
            .then(left.command.cmp(&right.command))
    });
    package_instructions.dedup();
    LandingCatalog {
        schema_version: 1,
        navigation,
        categories: vec![
            LandingCategory {
                id: "published".to_owned(),
                packages: published,
            },
            LandingCategory {
                id: "source-only".to_owned(),
                packages: source_only,
            },
        ],
        package_instructions,
    }
}

fn crate_has_library_target(crate_dir: &Path) -> bool {
    if crate_dir.join("src/lib.rs").is_file() {
        return true;
    }
    let Ok(manifest) = fs::read_to_string(crate_dir.join("Cargo.toml")) else {
        return false;
    };
    let mut in_lib = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_lib = trimmed == "[lib]";
            continue;
        }
        if in_lib && trimmed.starts_with("path") && trimmed.split_once('=').is_some() {
            let Some((_, value)) = trimmed.split_once('=') else {
                continue;
            };
            let path = value.trim().trim_matches('"').trim_matches('\'');
            return crate_dir.join(path).is_file();
        }
    }
    false
}

fn resolve_profile_target(target: &str, toolchain: Option<&str>) -> Result<String, Error> {
    if target != "host" {
        return Ok(target.to_owned());
    }
    let mut command = Command::new("rustc");
    if let Some(toolchain) = toolchain {
        command.arg(format!("+{toolchain}"));
    }
    let output = command.args(["-vV"]).output().map_err(Error::Io)?;
    if !output.status.success() {
        return Err(Error::Strict(format!(
            "rustc -vV failed while resolving the host target: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("host:").map(str::trim))
        .filter(|host| !host.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| Error::Strict("rustc -vV did not report a host target".to_owned()))
}

/// Generate pinned, docs-only rustdoc JSON artifacts and source-binding
/// receipts for every package in every declared profile.
pub fn generate_rustdoc(options: &GenerateOptions) -> Result<GenerationReceipt, Error> {
    if !options.repository_root.is_dir() {
        return Err(Error::InvalidRepository(options.repository_root.clone()));
    }
    // Read and parse the exact profile bytes that will drive Cargo before
    // capturing provenance. The before/after guard hashes the same path, so a
    // concurrent edit between parsing and the initial capture fails closed
    // instead of pairing an old profile with a new digest.
    let (profiles, profile_manifest_bytes) =
        load_profile_manifest_snapshot(&options.profile_manifest)?;
    let generator_root = options
        .generator_root
        .as_deref()
        .unwrap_or(&options.repository_root);
    let compiler_cache_root = options
        .compiler_cache_dir
        .clone()
        .or_else(|| std::env::var_os("SDK_DOCS_RUSTDOC_CACHE_DIR").map(PathBuf::from));
    let compiler_cache_root = compiler_cache_root.unwrap_or_else(|| {
        options
            .output_dir
            .parent()
            .map(|parent| parent.join(".sdk-docs-rustdoc-cache"))
            .unwrap_or_else(|| options.output_dir.join(".cargo-target"))
    });
    // Materialize both generated trees before the guard so their canonical
    // paths can be excluded explicitly even when they live below a source
    // checkout. They are compiler/output state, never source inputs.
    fs::create_dir_all(&options.output_dir)?;
    fs::create_dir_all(&compiler_cache_root)?;
    // Capture the complete source closure before invoking Cargo. The output
    // and compiler cache trees are excluded by `capture_generation_provenance`.
    // A second capture below makes generation fail closed if any source or
    // generator input changes while the compiler is running.
    let provenance_before = capture_generation_provenance(
        generator_root,
        &options.repository_root,
        &options.output_dir,
        &compiler_cache_root,
        &options.profile_manifest,
    )?;
    if provenance_before.profile_manifest_blake3 != digest_bytes(&profile_manifest_bytes) {
        return Err(Error::Strict(
            "profile manifest changed while establishing generation provenance; rerun from an unchanged profile"
                .to_owned(),
        ));
    }
    let source_revision = options
        .source_revision
        .clone()
        .unwrap_or_else(|| provenance_before.source_git_sha.clone());
    if source_revision == "unknown" {
        return Err(Error::Strict(
            "generation source is not an exact Git checkout; pass --source-revision for a verified release archive"
                .to_owned(),
        ));
    }
    let mut artifacts = Vec::new();
    for profile in profiles {
        let profile_output = options.output_dir.join(&profile.name);
        fs::create_dir_all(&profile_output)?;
        let profile_blake3 = profile_blake3(&profile);
        let target_dir = compiler_cache_root.join(format!(
            "toolchain-{}-profile-{}",
            cache_path_component(&options.toolchain),
            profile_blake3
        ));
        for package in &profile.packages {
            let package_name = &package.package;
            let crate_dir = find_package_dir(&options.repository_root, package_name)?;
            let source_blake3 = source_content_hash(&crate_dir, &options.repository_root)?;
            let crate_types = if package.crate_types.is_empty() {
                declared_crate_types(&crate_dir)?
            } else {
                package.crate_types.clone()
            };
            let public_source_paths = public_source_paths(&crate_dir, &options.repository_root)?;
            let (guide_source_paths, guide_source_blake3) =
                guide_source_metadata(&crate_dir, &options.repository_root)?;
            let target = resolve_profile_target(&package.target, Some(&options.toolchain))?;
            let mut command = Command::new("cargo");
            command.arg(format!("+{}", options.toolchain)).args([
                "rustdoc",
                "--locked",
                "--package",
                package_name,
                "--target",
                &target,
                "--lib",
            ]);
            let features = normalized_features(&package.features);
            if !features.is_empty() {
                command.args(["--features", &features.join(",")]);
            }
            if !package.default_features {
                command.arg("--no-default-features");
            }
            command.args(["--", "-Z", "unstable-options", "--output-format", "json"]);
            command
                .current_dir(&options.repository_root)
                .env("CARGO_TARGET_DIR", &target_dir)
                .env("RUSTC_BOOTSTRAP", "1");
            let command_debug = format!("{command:?}");
            let output = command.output().map_err(|error| {
                Error::Strict(format!(
                    "rustdoc process could not start for profile {} package {} target {} using {}: {}",
                    profile.name, package_name, target, command_debug, error
                ))
            })?;
            if !output.status.success() {
                return Err(Error::Strict(format!(
                    "rustdoc generation failed for profile {} package {}: {}",
                    profile.name,
                    package_name,
                    String::from_utf8_lossy(&output.stderr).trim()
                )));
            }
            let generated_root = target_dir.join(&target).join("doc");
            let generated_json =
                find_rustdoc_json(&generated_root, package_name).ok_or_else(|| {
                    Error::Strict(format!(
                        "rustdoc generation produced no JSON for profile {} package {}",
                        profile.name, package_name
                    ))
                })?;
            let json_name = format!("{}.json", package_name.replace('-', "_"));
            let output_json = profile_output.join(json_name);
            fs::copy(&generated_json, &output_json).map_err(|error| {
                Error::Strict(format!(
                    "could not copy rustdoc JSON for profile {} package {} from {} to {}: {}",
                    profile.name,
                    package_name,
                    generated_json.display(),
                    output_json.display(),
                    error
                ))
            })?;
            let json_bytes = fs::read(&output_json).map_err(|error| {
                Error::Strict(format!(
                    "could not read copied rustdoc JSON for profile {} package {} at {}: {}",
                    profile.name,
                    package_name,
                    output_json.display(),
                    error
                ))
            })?;
            let rustdoc_json_blake3 = digest_bytes(&json_bytes);
            let receipt = RustdocReceipt {
                schema_version: 1,
                package_name: package_name.clone(),
                source_blake3: source_blake3.clone(),
                source_revision: source_revision.clone(),
                toolchain: options.toolchain.clone(),
                target: target.clone(),
                features: features.clone(),
                profile_blake3: profile_blake3.clone(),
                profile: profile.name.clone(),
                rustdoc_json_blake3: rustdoc_json_blake3.clone(),
            };
            let receipt_file = receipt_path(&output_json);
            fs::write(
                &receipt_file,
                serde_json::to_string_pretty(&receipt)? + "\n",
            )
            .map_err(|error| {
                Error::Strict(format!(
                    "could not write rustdoc receipt for profile {} package {} at {}: {}",
                    profile.name,
                    package_name,
                    receipt_file.display(),
                    error
                ))
            })?;
            artifacts.push(GeneratedArtifact {
                profile: profile.name.clone(),
                package_name: package_name.clone(),
                target,
                features,
                profile_blake3: profile_blake3.clone(),
                source_blake3,
                rustdoc_json: relative_path(&options.output_dir, &output_json),
                rustdoc_json_blake3,
                receipt: relative_path(&options.output_dir, &receipt_file),
                crate_types,
                public_source_paths,
                guide_source_paths,
                guide_source_blake3,
            });
        }
    }
    artifacts.sort_by(|left, right| {
        left.profile
            .cmp(&right.profile)
            .then(left.package_name.cmp(&right.package_name))
    });
    let provenance_after = capture_generation_provenance(
        generator_root,
        &options.repository_root,
        &options.output_dir,
        &compiler_cache_root,
        &options.profile_manifest,
    )?;
    if provenance_before != provenance_after {
        return Err(Error::Strict(
            "generation inputs changed during rustdoc generation; rerun from an unchanged source and generator checkout"
                .to_owned(),
        ));
    }
    let result = GenerationReceipt {
        schema_version: 1,
        source_revision,
        toolchain: options.toolchain.clone(),
        profile_manifest_blake3: provenance_after.profile_manifest_blake3.clone(),
        artifacts,
        writer_sha: provenance_after.writer_sha,
        generator_git_sha: provenance_after.generator_git_sha,
        input_closure: provenance_after.input_closure,
        generator_input_closure: provenance_after.generator_input_closure,
    };
    fs::write(
        options.output_dir.join("generation-receipt.json"),
        serde_json::to_string_pretty(&result)? + "\n",
    )?;
    Ok(result)
}

/// Write source-bound profile metadata for a historical checkout without
/// mutating that checkout or relying on the website to infer Cargo inputs.
pub fn write_rustdoc_profile(
    repository_root: &Path,
    profile_manifest: &Path,
    output: &Path,
    toolchain: &str,
) -> Result<RustdocProfileReceipt, Error> {
    if !repository_root.is_dir() {
        return Err(Error::InvalidRepository(repository_root.to_owned()));
    }
    let (profiles, profile_manifest_bytes) = match fs::read(profile_manifest) {
        Ok(contents) => (
            load_profile_manifest_bytes(&contents, profile_manifest)?,
            contents,
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let profiles = infer_historical_profile_manifest(repository_root)?;
            let contents = serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "profiles": &profiles,
            }))?;
            (profiles, contents)
        }
        Err(error) => return Err(error.into()),
    };
    let profile_manifest_blake3 = digest_bytes(&profile_manifest_bytes);
    let mut receipt_profiles = Vec::with_capacity(profiles.len());
    for profile in profiles {
        let mut packages = Vec::with_capacity(profile.packages.len());
        for package in profile.packages {
            let crate_dir = find_package_dir(repository_root, &package.package)?;
            let (guide_source_paths, guide_source_blake3) =
                guide_source_metadata(&crate_dir, repository_root)?;
            packages.push(RustdocProfilePackageReceipt {
                package: package.package,
                target: resolve_profile_target(&package.target, Some(toolchain))?,
                features: normalized_features(&package.features),
                default_features: package.default_features,
                crate_types: if package.crate_types.is_empty() {
                    declared_crate_types(&crate_dir)?
                } else {
                    package.crate_types
                },
                source_blake3: source_content_hash(&crate_dir, repository_root)?,
                public_source_paths: public_source_paths(&crate_dir, repository_root)?,
                guide_source_paths,
                guide_source_blake3,
            });
        }
        receipt_profiles.push(RustdocProfileReceiptEntry {
            name: profile.name,
            scope: profile.scope,
            packages,
        });
    }
    let receipt = RustdocProfileReceipt {
        schema_version: 1,
        source_revision: git_revision(repository_root).unwrap_or_else(|| "unknown".to_owned()),
        toolchain: toolchain.to_owned(),
        profile_manifest_blake3,
        profiles: receipt_profiles,
    };
    if let Some(parent) = output.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::write(output, serde_json::to_string_pretty(&receipt)? + "\n")?;
    Ok(receipt)
}

fn capture_generation_provenance(
    generator_root: &Path,
    input_root: &Path,
    output_dir: &Path,
    compiler_cache_dir: &Path,
    profile_manifest: &Path,
) -> Result<GenerationProvenance, Error> {
    let writer_path = std::env::current_exe().map_err(Error::Io)?;
    let writer_sha = digest_bytes(&fs::read(writer_path)?);
    let source_git_sha = git_revision(input_root).unwrap_or_else(|| "unknown".to_owned());
    let generator_git_sha = git_revision(generator_root).unwrap_or_else(|| "unknown".to_owned());
    let profile_manifest_blake3 = digest_bytes(&fs::read(profile_manifest)?);
    let canonical_root = input_root
        .canonicalize()
        .unwrap_or_else(|_| input_root.to_owned());
    let canonical_generator_root = generator_root
        .canonicalize()
        .unwrap_or_else(|_| generator_root.to_owned());
    let mut excluded_roots = vec![
        output_dir
            .canonicalize()
            .unwrap_or_else(|_| output_dir.to_owned()),
        compiler_cache_dir
            .canonicalize()
            .unwrap_or_else(|_| compiler_cache_dir.to_owned()),
    ];
    excluded_roots.sort();
    excluded_roots.dedup();
    let input_closure = generation_input_closure(&canonical_root, &excluded_roots)?;
    let generator_input_closure =
        generation_input_closure(&canonical_generator_root, &excluded_roots)?;
    Ok(GenerationProvenance {
        writer_sha,
        source_git_sha,
        generator_git_sha,
        profile_manifest_blake3,
        input_closure,
        generator_input_closure,
    })
}

fn load_profile_manifest_snapshot(
    path: &Path,
) -> Result<(Vec<AnalysisProfile>, Vec<u8>), Error> {
    let bytes = fs::read(path)?;
    let profiles = load_profile_manifest_bytes(&bytes, path)?;
    Ok((profiles, bytes))
}

fn generation_input_closure(
    root: &Path,
    excluded_roots: &[PathBuf],
) -> Result<Vec<GenerationInput>, Error> {
    let mut paths = Vec::new();
    collect_generation_input_paths(root, excluded_roots, &mut paths)?;
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let bytes = fs::read(&path)?;
            Ok(GenerationInput {
                path: relative_path(root, &path),
                blake3: digest_bytes(&bytes),
                bytes: bytes.len() as u64,
            })
        })
        .collect()
}

fn collect_generation_input_paths(
    directory: &Path,
    excluded_roots: &[PathBuf],
    paths: &mut Vec<PathBuf>,
) -> Result<(), Error> {
    if excluded_roots
        .iter()
        .any(|excluded| directory.starts_with(excluded))
        || directory
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                matches!(
                    name,
                    ".git" | "node_modules" | ".toolchains" | "cargo-home-private"
                )
            })
    {
        return Ok(());
    }
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            // Language qualification trees may contain links into their
            // build outputs. They are environment-owned and cannot be part
            // of a reproducible Rust source closure; retain ordinary source
            // files under `target` for compatibility, but omit only these
            // linked build inputs.
            if path
                .components()
                .any(|component| matches!(component.as_os_str().to_str(), Some("target" | "work")))
            {
                continue;
            }
            return Err(Error::Strict(format!(
                "symlink source input is not supported: {}",
                path.display()
            )));
        }
        if file_type.is_dir() {
            collect_generation_input_paths(&path, excluded_roots, paths)?;
        } else if file_type.is_file() {
            if excluded_roots
                .iter()
                .any(|excluded| path.starts_with(excluded))
            {
                continue;
            }
            paths.push(path);
        }
    }
    Ok(())
}

fn cache_path_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

/// Serialize a bundle in stable pretty-printed JSON.
pub fn to_pretty_json(bundle: &DocsBundle) -> Result<String, Error> {
    Ok(serde_json::to_string_pretty(bundle)? + "\n")
}

/// Rust identity and public route identity for one generated documentation
/// family. The package and crate names remain exact Rust metadata; the public
/// slug is the stable website route shared by current and historical package
/// names.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentationRouteIdentity {
    /// Stable public route used by the website, such as `inference`.
    #[serde(rename = "publicSlug")]
    pub public_slug: String,
    /// Exact Cargo package name that produced this bundle entry.
    #[serde(rename = "rustPackage")]
    pub rust_package: String,
    /// Exact Rust library crate name, when the manifest declares one.
    #[serde(rename = "rustCrate", skip_serializing_if = "Option::is_none")]
    pub rust_crate: Option<String>,
    /// Older package-derived slugs normalized to `public_slug` for released
    /// archives. These are provenance, not independently authored redirects.
    #[serde(rename = "historicalSlugs")]
    pub historical_slugs: Vec<String>,
}

/// Derive the public documentation route from Rust package metadata.
///
/// Historical archives used `fs` and `inference-sdk` as package-derived route
/// slugs. Their Rust identities remain intact, while the generated public
/// route is normalized to `filesystem` and `inference` respectively so the
/// website can resolve every release through one data-driven lookup.
pub fn documentation_route_identity(
    package_name: &str,
    crate_name: Option<&str>,
) -> DocumentationRouteIdentity {
    let package_slug = package_name.strip_prefix("acyclic-").unwrap_or(package_name);
    let public_slug = match package_slug {
        "fs" => "filesystem",
        "inference-sdk" => "inference",
        slug => slug,
    };
    let historical_slugs = (public_slug != package_slug)
        .then(|| package_slug.to_owned())
        .into_iter()
        .collect();
    DocumentationRouteIdentity {
        public_slug: public_slug.to_owned(),
        rust_package: package_name.to_owned(),
        rust_crate: crate_name.map(str::to_owned),
        historical_slugs,
    }
}

/// Serialize the compact, website-facing projection without moving content
/// authority into a website language. The full bundle remains the provenance
/// artifact; this projection carries its digest and source revision.
pub fn to_website_json(
    bundle: &DocsBundle,
    repository: &str,
    source_state: &str,
    channel: &str,
) -> Result<String, Error> {
    let families = bundle
        .crates
        .iter()
        .map(|crate_bundle| {
            let route = documentation_route_identity(
                &crate_bundle.package_name,
                crate_bundle.crate_name.as_deref(),
            );
            let title = crate_bundle
                .package_name
                .strip_prefix("acyclic-")
                .unwrap_or(&crate_bundle.package_name)
                .split('-')
                .map(|part| {
                    let mut characters = part.chars();
                    match characters.next() {
                        Some(first) => first.to_uppercase().collect::<String>() + characters.as_str(),
                        None => String::new(),
                    }
                })
                .collect::<Vec<_>>()
                .join(" ");
            let items = crate_bundle
                .public_items
                .iter()
                .map(|item| {
                    let mut projection = serde_json::json!({
                        "kind": item.kind,
                        "name": item.name,
                        "modulePath": item.module_path,
                        "signature": item.signature,
                        "signatureText": item.signature_text,
                        "summary": item.docs.as_deref().and_then(|docs| docs.lines().next()).filter(|summary| !summary.trim().is_empty()).unwrap_or("No declaration summary was provided."),
                        "sourcePath": item.source_path,
                        "sourceLine": item.source_line,
                        "conditional": item.conditional,
                        "generated": item.generated,
                    });
                    if let Some(reexport) = item.reexport.as_ref() {
                        projection["reexportTarget"] = reexport_target_projection(reexport, &bundle.crates);
                    }
                    projection
                })
                .collect::<Vec<_>>();
            let referenced_paths = crate_bundle
                .public_items
                .iter()
                .filter_map(|item| item.source_path.as_deref())
                .chain(crate_bundle.guides.iter().map(|guide| guide.path.as_str()))
                .chain(crate_bundle.examples.iter().map(|example| example.path.as_str()))
                .collect::<HashSet<_>>();
            let source_files = crate_bundle
                .sources
                .iter()
                .filter(|source| referenced_paths.contains(source.path.as_str()))
                .map(|source| {
                    serde_json::json!({
                        "path": source.path,
                        "blake3": source.blake3,
                        "contents": source.contents,
                    })
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "slug": route.public_slug,
                "route": route,
                "title": title,
                "crate": crate_bundle.package_name,
                "crateName": crate_bundle.crate_name,
                "navigation": crate_bundle.navigation,
                "version": crate_bundle.version,
                "availability": crate_bundle.availability,
                "analysisMode": crate_bundle.analysis_mode,
                "contentBlake3": crate_bundle.content_blake3,
                "coverage": crate_bundle.coverage,
                "sourceCoverage": {
                    "sourceFiles": crate_bundle.sources.len(),
                    "referencedSourceFiles": referenced_paths.len(),
                    "guideFiles": crate_bundle.guides.len(),
                    "exampleFiles": crate_bundle.examples.len(),
                },
                "maturity": if crate_bundle.publish { "preview" } else { "source-only" },
                "deployment": "unknown",
                "qualification": if crate_bundle.graphs.iter().any(|graph| !graph.public_items.is_empty()) { "qualified-graph" } else { "unqualified" },
                "summary": format!("Rust-owned API reference for {}.", crate_bundle.package_name),
                "guides": crate_bundle.guides,
                "examples": crate_bundle.examples,
                "packageInstructions": crate_bundle.package_instructions,
                "sourceFiles": source_files,
                "items": items,
                "graphs": crate_bundle.graphs.iter().map(|graph| serde_json::json!({
                    "profile": graph.profile,
                    "target": graph.target,
                    "features": graph.features,
                    "profileBlake3": graph.profile_blake3,
                    "publicItems": graph.public_items,
                    "rustdoc": {
                        "path": graph.rustdoc.path,
                        "formatVersion": graph.rustdoc.format_version,
                        "blake3": graph.rustdoc.blake3,
                        "sourceBlake3": graph.rustdoc.source_blake3,
                        "sourceRevision": graph.rustdoc.source_revision,
                        "toolchain": graph.rustdoc.toolchain,
                        "target": graph.rustdoc.target,
                        "features": graph.rustdoc.features,
                        "profileBlake3": graph.rustdoc.profile_blake3,
                        "receipt": graph.rustdoc.receipt,
                        "receiptBlake3": graph.rustdoc.receipt_blake3,
                    },
                })).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    let scenario_authority_sha256 = bundle
        .scenario_bundle
        .as_ref()
        .and_then(|scenario| scenario.get("source"))
        .and_then(|source| source.get("source_authority_sha256"))
        .cloned();
    let mut projection_source = serde_json::json!({
        "repository": repository,
        "revision": bundle.source_revision,
        "generator": "sdk-docs",
        "generatorVersion": env!("CARGO_PKG_VERSION"),
        "bundleBlake3": bundle.bundle_blake3,
        "sourceState": source_state,
        "channel": channel,
    });
    if let Some(release) = &bundle.release {
        let mut release_projection = serde_json::to_value(release)?;
        if let Some(release_object) = release_projection.as_object_mut() {
            // Keep the signed release receipt together with the exact bundle
            // bytes consumed by the website. The full bundle carries these
            // values in separate fields; the projection must retain both
            // identities so a released page cannot be mistaken for a branch
            // preview with the same API names.
            release_object.insert(
                "sourceRevision".to_owned(),
                serde_json::Value::String(bundle.source_revision.clone()),
            );
            release_object.insert(
                "bundleBlake3".to_owned(),
                serde_json::Value::String(bundle.bundle_blake3.clone()),
            );
        }
        projection_source["release"] = release_projection;
    }
    if let Some(authority_sha256) = scenario_authority_sha256 {
        projection_source["scenarioAuthoritySha256"] = authority_sha256;
    }
    let projection = serde_json::json!({
        "$schema": "https://acyclic.dev/schemas/sdk-reference-bundle.v1.json",
        "schemaVersion": "sdk-reference-bundle.v1",
        "source": projection_source,
        "landing": bundle.landing,
        "profiles": bundle.profiles,
        "scenarioBundle": bundle.scenario_bundle,
        "families": families,
    });
    Ok(serde_json::to_string_pretty(&projection)? + "\n")
}

fn reexport_target_projection(
    target: &ReexportTarget,
    crates: &[CrateBundle],
) -> serde_json::Value {
    let target_crate = crates
        .iter()
        .find(|crate_bundle| crate_bundle.package_name == target.package);
    let target_name = target
        .path
        .as_deref()
        .and_then(|path| path.rsplit("::").next())
        .filter(|name| !name.is_empty());
    let definition = target_crate.and_then(|crate_bundle| {
        target_name.and_then(|name| {
            crate_bundle
                .public_items
                .iter()
                .enumerate()
                .find(|(_, item)| item.name == name)
        })
    });
    let family = documentation_route_identity(&target.package, None).public_slug;
    serde_json::json!({
        "package": target.package,
        "family": family,
        "path": target.path,
        "source": target.source,
        "definition": definition.map(|(index, item)| serde_json::json!({
            "index": index,
            "kind": item.kind,
            "name": item.name,
            "modulePath": item.module_path,
            "signature": item.signature,
            "signatureText": item.signature_text,
            "summary": item.docs.as_deref().and_then(|docs| docs.lines().next()).filter(|summary| !summary.trim().is_empty()).unwrap_or("No declaration summary was provided."),
            "sourcePath": item.source_path,
            "sourceLine": item.source_line,
        })),
    })
}

/// Read the Rust-owned scenario output and bind every rendered file to the
/// source revision and source digest recorded by its generator. The generator
/// owns the language code; this layer only adds exact file bytes and a local
/// digest for downstream projections.
#[cfg(test)]
fn load_scenario_bundle(
    bundle_root: &Path,
    repository_root: &Path,
    source_revision: &str,
) -> Result<serde_json::Value, Error> {
    load_scenario_bundle_with_authority(bundle_root, repository_root, source_revision, None, None)
}

fn load_scenario_bundle_with_authority(
    bundle_root: &Path,
    repository_root: &Path,
    source_revision: &str,
    source_authority: Option<&Path>,
    source_authority_sha256: Option<&str>,
) -> Result<serde_json::Value, Error> {
    let manifest_path = bundle_root.join("sdk-examples-manifest.json");
    let mut manifest: serde_json::Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    if manifest.get("schema").and_then(serde_json::Value::as_str)
        != Some("acyclic.sdk.examples.bundle.v1")
    {
        return Err(Error::Strict(format!(
            "unsupported SDK examples bundle schema in {}",
            manifest_path.display()
        )));
    }
    let source = manifest
        .get_mut("source")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| Error::Strict("SDK examples bundle has no source object".to_owned()))?;
    let source_path = source
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::Strict("SDK examples bundle source path is missing".to_owned()))?
        .to_owned();
    if Path::new(&source_path).is_absolute()
        || Path::new(&source_path)
            .components()
            .any(|component| component == std::path::Component::ParentDir)
    {
        return Err(Error::Strict(format!(
            "SDK examples source is outside the repository: {source_path}"
        )));
    }
    let declared_revision = source
        .get("revision")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    if source_revision != "unknown" && declared_revision != source_revision {
        return Err(Error::Strict(format!(
            "SDK examples source revision mismatch: expected {source_revision}, got {declared_revision}"
        )));
    }
    let source_file = repository_root.join(&source_path);
    let source_bytes = if source_file.is_file() {
        fs::read(&source_file).map_err(|error| {
            Error::Strict(format!(
                "SDK examples source {} is unavailable: {error}",
                source_file.display()
            ))
        })?
    } else {
        let declared_source_files = source
            .get("files")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                Error::Strict(format!(
                    "SDK examples source directory {source_path} has no source files closure"
                ))
            })?;
        if declared_source_files.is_empty() {
            return Err(Error::Strict(format!(
                "SDK examples source directory {source_path} has an empty source files closure"
            )));
        }
        let repository_root_canonical = repository_root.canonicalize()?;
        let allowed_roots = cargo_source_closure_roots(repository_root, &source_path)?;
        let mut seen = HashSet::new();
        let mut closure_files = Vec::new();
        for value in declared_source_files {
            let relative = value.as_str().ok_or_else(|| {
                Error::Strict("SDK examples source closure path is not a string".to_owned())
            })?;
            let relative_path = Path::new(relative);
            if relative_path.is_absolute()
                || relative_path
                    .components()
                    .any(|component| component == std::path::Component::ParentDir)
                || !allowed_roots
                    .iter()
                    .any(|root| relative_path.starts_with(root))
                || !seen.insert(relative.to_owned())
            {
                return Err(Error::Strict(format!(
                    "SDK examples source closure contains an unsafe or duplicate path: {relative}"
                )));
            }
            let path = repository_root.join(relative_path);
            let canonical_path = path.canonicalize().map_err(|error| {
                Error::Strict(format!(
                    "SDK examples source closure file {relative} is unavailable: {error}"
                ))
            })?;
            if !canonical_path.starts_with(&repository_root_canonical) {
                return Err(Error::Strict(format!(
                    "SDK examples source closure file escapes the repository: {relative}"
                )));
            }
            let bytes = fs::read(&path).map_err(|error| {
                Error::Strict(format!(
                    "SDK examples source closure file {relative} is unavailable: {error}"
                ))
            })?;
            closure_files.push((relative.to_owned(), bytes));
        }
        let build_target = source
            .get("build_target")
            .or_else(|| source.get("target"))
            .and_then(serde_json::Value::as_str)
            .filter(|target| !target.trim().is_empty())
            .ok_or_else(|| {
                Error::Strict(
                    "SDK examples directory source closure has no producer build target".to_owned(),
                )
            })?;
        cargo_source_closure_bytes(repository_root, &source_path, &closure_files, build_target)?
    };
    let source_blake3 = digest_bytes(&source_bytes);
    let actual_source_sha256 = sha256_digest(&source_bytes);
    source.insert(
        "source_blake3".to_owned(),
        serde_json::Value::String(source_blake3.clone()),
    );
    let declared_source_hash = source
        .get("sha256")
        .and_then(serde_json::Value::as_str)
        .filter(|hash| !hash.trim().is_empty())
        .ok_or_else(|| Error::Strict("SDK examples source sha256 is missing".to_owned()))?
        .to_owned();
    if declared_source_hash != actual_source_sha256 {
        return Err(Error::Strict(format!(
            "SDK examples source sha256 mismatch: expected {actual_source_sha256}, got {declared_source_hash}"
        )));
    }
    if let Some(authority_path) = source_authority {
        let authority_sha256 = verify_source_authority(
            authority_path,
            repository_root,
            &source_path,
            &declared_revision,
            &actual_source_sha256,
            source_authority_sha256,
        )?;
        source.insert(
            "source_authority_sha256".to_owned(),
            serde_json::Value::String(authority_sha256),
        );
    }
    source.insert(
        "source_sha256".to_owned(),
        serde_json::Value::String(actual_source_sha256.clone()),
    );

    let declared_files = manifest
        .get("files")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::Strict("SDK examples bundle files are missing".to_owned()))?;
    let mut files = HashSet::new();
    for value in declared_files {
        let relative = value.as_str().ok_or_else(|| {
            Error::Strict("SDK examples declared file path is not a string".to_owned())
        })?;
        if !files.insert(relative.to_owned()) {
            return Err(Error::Strict(format!(
                "SDK examples declared file is duplicated: {relative}"
            )));
        }
    }
    for relative in &files {
        if Path::new(relative).is_absolute()
            || Path::new(relative)
                .components()
                .any(|component| component == std::path::Component::ParentDir)
        {
            return Err(Error::Strict(format!(
                "SDK examples declared file is outside its bundle: {relative}"
            )));
        }
        let path = bundle_root.join(relative);
        if !path.is_file() {
            return Err(Error::Strict(format!(
                "SDK examples declared file is unavailable: {relative}"
            )));
        }
    }
    let snippets = manifest
        .get_mut("snippets")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| Error::Strict("SDK examples bundle snippets are missing".to_owned()))?;
    for snippet in snippets {
        let object = snippet
            .as_object_mut()
            .ok_or_else(|| Error::Strict("SDK examples snippet is not an object".to_owned()))?;
        let relative = object
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Strict("SDK examples snippet path is missing".to_owned()))?;
        if Path::new(relative).is_absolute()
            || Path::new(relative)
                .components()
                .any(|component| component == std::path::Component::ParentDir)
            || !files.contains(relative)
        {
            return Err(Error::Strict(format!(
                "SDK examples snippet path is outside its declared file set: {relative}"
            )));
        }
        let bytes = fs::read(bundle_root.join(relative)).map_err(|error| {
            Error::Strict(format!(
                "SDK examples snippet {relative} is unavailable: {error}"
            ))
        })?;
        let contents = String::from_utf8(bytes.clone()).map_err(|error| {
            Error::Strict(format!(
                "SDK examples snippet {relative} is not UTF-8: {error}"
            ))
        })?;
        let declared_code_hash = object
            .get("code_sha256")
            .and_then(serde_json::Value::as_str)
            .filter(|hash| !hash.trim().is_empty())
            .ok_or_else(|| {
                Error::Strict(format!("SDK examples snippet {relative} has no code hash"))
            })?;
        let actual_code_hash = sha256_digest(&bytes);
        if declared_code_hash != actual_code_hash {
            return Err(Error::Strict(format!(
                "SDK examples code sha256 mismatch for {relative}: expected {actual_code_hash}, got {declared_code_hash}"
            )));
        }
        if let Some(snippet_source) = object
            .get("source_sha256")
            .and_then(serde_json::Value::as_str)
        {
            if snippet_source != declared_source_hash {
                return Err(Error::Strict(format!(
                    "SDK examples snippet source sha256 mismatch for {relative}: expected {declared_source_hash}, got {snippet_source}"
                )));
            }
        }
        let receipt_source = object
            .get("validation")
            .and_then(serde_json::Value::as_object)
            .and_then(|validation| validation.get("receipt"))
            .and_then(serde_json::Value::as_object)
            .and_then(|receipt| receipt.get("source_sha256"))
            .and_then(serde_json::Value::as_str);
        if receipt_source != Some(declared_source_hash.as_str()) {
            return Err(Error::Strict(format!(
                "SDK examples receipt for {relative} is not bound to source sha256"
            )));
        }
        validate_qualified_receipt(
            object,
            bundle_root,
            &files,
            relative,
            Some(declared_revision.as_str()),
            Some(source_path.as_str()),
            source_authority.is_some(),
        )?;
        object.insert("contents".to_owned(), serde_json::Value::String(contents));
        object.insert(
            "code_blake3".to_owned(),
            serde_json::Value::String(digest_bytes(&bytes)),
        );
        object.insert(
            "source_blake3".to_owned(),
            serde_json::Value::String(source_blake3.clone()),
        );
        object.insert(
            "code_sha256".to_owned(),
            serde_json::Value::String(actual_code_hash),
        );
    }
    Ok(manifest)
}

/// Return the repository-relative roots that Cargo can compile into the
/// producer's local source closure. The examples producer includes every
/// local package reached by Cargo, plus workspace Cargo inputs; requiring the
/// same independently resolved roots here prevents a forged manifest from
/// adding arbitrary repository files while still allowing dependencies such
/// as `rust/crates/actors` and `.cargo/config.toml`.
fn cargo_source_closure_roots(
    repository_root: &Path,
    source_path: &str,
) -> Result<Vec<PathBuf>, Error> {
    let source_root = repository_root.canonicalize()?;
    let manifest = source_root.join(source_path).join("Cargo.toml");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args([
            "metadata",
            "--manifest-path",
            &manifest.to_string_lossy(),
            "--locked",
            "--format-version",
            "1",
        ])
        .output()
        .map_err(Error::Io)?;
    if !output.status.success() {
        return Err(Error::Strict(format!(
            "cargo metadata failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::Strict("cargo metadata packages are missing".to_owned()))?;
    let package_by_id = packages
        .iter()
        .filter_map(|package| Some((package.get("id")?.as_str()?.to_owned(), package)))
        .collect::<BTreeMap<_, _>>();
    let nodes = metadata
        .pointer("/resolve/nodes")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::Strict("cargo metadata resolve.nodes are missing".to_owned()))?;
    let node_by_id = nodes
        .iter()
        .filter_map(|node| Some((node.get("id")?.as_str()?.to_owned(), node)))
        .collect::<BTreeMap<_, _>>();
    let root_id = metadata
        .pointer("/resolve/root")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::Strict("cargo metadata resolve.root is missing".to_owned()))?;
    let mut reachable = HashSet::new();
    let mut pending = VecDeque::from([root_id.to_owned()]);
    while let Some(id) = pending.pop_front() {
        if !reachable.insert(id.clone()) {
            continue;
        }
        let node = node_by_id
            .get(&id)
            .ok_or_else(|| Error::Strict(format!("cargo metadata node is missing for {id}")))?;
        for dependency in node
            .get("dependencies")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
        {
            pending.push_back(dependency.to_owned());
        }
    }

    let mut roots = Vec::new();
    for id in reachable {
        let package = package_by_id
            .get(&id)
            .ok_or_else(|| Error::Strict(format!("cargo metadata package is missing for {id}")))?;
        if package
            .get("source")
            .is_some_and(|source| !source.is_null())
        {
            continue;
        }
        let manifest = package
            .get("manifest_path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Strict(format!("manifest path is missing for {id}")))?;
        let package_dir = PathBuf::from(manifest)
            .canonicalize()
            .map_err(Error::Io)?
            .parent()
            .ok_or_else(|| Error::Strict(format!("manifest has no parent for {id}")))?
            .to_owned();
        let relative = package_dir
            .strip_prefix(&source_root)
            .map_err(|_| Error::Strict(format!("local package escapes repository: {manifest}")))?
            .to_owned();
        roots.push(relative);
    }

    for relative in [
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain",
        "rust-toolchain.toml",
    ] {
        if source_root.join(relative).is_file() {
            roots.push(PathBuf::from(relative));
        }
    }
    if source_root.join(".cargo").is_dir() {
        roots.push(PathBuf::from(".cargo"));
    }
    roots.sort();
    roots.dedup();
    if roots.is_empty() {
        return Err(Error::Strict(
            "cargo source closure has no local package or workspace roots".to_owned(),
        ));
    }
    Ok(roots)
}

fn cargo_source_closure_bytes(
    repository_root: &Path,
    source_path: &str,
    files: &[(String, Vec<u8>)],
    build_target: &str,
) -> Result<Vec<u8>, Error> {
    let source_root = repository_root.canonicalize()?;
    let manifest = source_root.join(source_path).join("Cargo.toml");
    let output = Command::new("cargo")
        .args([
            "metadata",
            "--manifest-path",
            &manifest.to_string_lossy(),
            "--locked",
            "--format-version",
            "1",
        ])
        .output()
        .map_err(Error::Io)?;
    if !output.status.success() {
        return Err(Error::Strict(format!(
            "cargo metadata failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let recipe =
        sdk_source_identity::normalized_build_recipe(&source_root, &metadata, Some(build_target))
            .map_err(Error::Strict)?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"cargo-build-recipe\0");
    bytes.extend_from_slice(&recipe);
    bytes.push(0);
    for (relative, contents) in files {
        bytes.extend_from_slice(relative.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(contents);
        bytes.push(0);
    }
    Ok(bytes)
}

/// Verify the source closure against an authority manifest supplied outside
/// the examples bundle. The bundle's own hashes are useful consistency checks,
/// but they cannot establish identity when the source and receipt are edited
/// together.
fn verify_source_authority(
    authority_path: &Path,
    repository_root: &Path,
    source_path: &str,
    source_revision: &str,
    actual_source_sha256: &str,
    configured_authority_sha256: Option<&str>,
) -> Result<String, Error> {
    let authority_bytes = fs::read(authority_path)?;
    let authority_digest = sha256_digest(&authority_bytes);
    let configured_authority_sha256 = configured_authority_sha256.ok_or_else(|| {
        Error::Strict(
            "SDK examples source authority requires a configured authority sha256 trust root"
                .to_owned(),
        )
    })?;
    if configured_authority_sha256 != authority_digest {
        return Err(Error::Strict(format!(
            "SDK examples source authority digest mismatch: expected {configured_authority_sha256}, got {authority_digest}"
        )));
    }
    let authority: serde_json::Value = serde_json::from_slice(&authority_bytes)?;
    let authority_schema = authority.get("schema").and_then(serde_json::Value::as_str);
    let captured_snapshot = authority_schema == Some("acyclic.sdk.docs.source-capture.v1");
    if !matches!(
        authority_schema,
        Some("acyclic.sdk.examples.source-authority.v1")
            | Some("acyclic.sdk.qualification-receipt.v1")
            | Some("acyclic.sdk.docs.source-capture.v1")
    ) {
        return Err(Error::Strict(format!(
            "unsupported SDK examples source authority schema in {}",
            authority_path.display()
        )));
    }
    let authority_source = authority
        .get("source")
        .and_then(serde_json::Value::as_object);
    let authority_revision = authority
        .get("source_revision")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            authority
                .get("origin_revision")
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            authority_source
                .and_then(|source| source.get("revision"))
                .and_then(serde_json::Value::as_str)
        })
        .ok_or_else(|| {
            Error::Strict("SDK examples source authority has no source revision".to_owned())
        })?;
    let repository_revision = git_revision(repository_root).ok_or_else(|| {
        Error::Strict(
            "SDK examples source authority requires a repository with an immutable origin revision"
                .to_owned(),
        )
    })?;
    if authority_revision != repository_revision {
        return Err(Error::Strict(format!(
            "SDK examples source authority origin revision mismatch: expected {repository_revision}, got {authority_revision}"
        )));
    }
    if authority_revision != source_revision {
        return Err(Error::Strict(format!(
            "SDK examples source authority revision mismatch: expected {authority_revision}, got {source_revision}"
        )));
    }
    let authority_path_value = authority
        .get("source_path")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            authority_source
                .and_then(|source| source.get("path"))
                .and_then(serde_json::Value::as_str)
        });
    if let Some(authority_path_value) = authority_path_value {
        if authority_path_value != source_path {
            return Err(Error::Strict(format!(
                "SDK examples source authority path mismatch: expected {authority_path_value}, got {source_path}"
            )));
        }
    }
    let authority_hash = authority
        .get("source_sha256")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            authority
                .get("closure_sha256")
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            authority_source
                .and_then(|source| source.get("sha256"))
                .and_then(serde_json::Value::as_str)
        })
        .ok_or_else(|| {
            Error::Strict("SDK examples source authority has no source sha256".to_owned())
        })?;
    if authority_hash != actual_source_sha256 {
        return Err(Error::Strict(format!(
            "SDK examples immutable source/revision closure mismatch: expected {authority_hash}, got {actual_source_sha256}"
        )));
    }

    let source_files = authority
        .get("source_files")
        .and_then(serde_json::Value::as_array)
        .or_else(|| {
            authority_source
                .and_then(|source| source.get("files"))
                .and_then(serde_json::Value::as_array)
        });
    let source_hashes = authority
        .get("source_file_hashes")
        .and_then(serde_json::Value::as_object)
        .or_else(|| {
            authority_source
                .and_then(|source| source.get("file_hashes"))
                .and_then(serde_json::Value::as_object)
        });
    let captured_files = authority.get("files").and_then(serde_json::Value::as_array);
    if let Some(source_files) = source_files.or(captured_files) {
        if source_files.is_empty() {
            return Err(Error::Strict(
                "SDK examples source authority has an empty file list".to_owned(),
            ));
        }
        let mut seen = HashSet::new();
        let mut closure = Vec::new();
        for value in source_files {
            let relative = value
                .as_str()
                .or_else(|| value.get("path").and_then(serde_json::Value::as_str))
                .ok_or_else(|| {
                    Error::Strict(
                        "SDK examples source authority file is not a path string".to_owned(),
                    )
                })?;
            let relative_path = Path::new(relative);
            if !is_portable_relative_path(relative_path) || !seen.insert(relative.to_owned()) {
                return Err(Error::Strict(format!(
                    "SDK examples source authority contains an unsafe or duplicate path: {relative}"
                )));
            }
            let bytes = fs::read(repository_root.join(relative_path)).map_err(|error| {
                Error::Strict(format!(
                    "SDK examples source authority file {relative} is unavailable: {error}"
                ))
            })?;
            if let Some(source_hashes) = source_hashes {
                let expected = source_hashes
                    .get(relative)
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        Error::Strict(format!(
                            "SDK examples source authority has no hash: {relative}"
                        ))
                    })?;
                if expected != sha256_digest(&bytes) {
                    return Err(Error::Strict(format!(
                        "SDK examples immutable source/revision closure mismatch: {relative}"
                    )));
                }
            } else if let Some(expected) = value.get("sha256").and_then(serde_json::Value::as_str) {
                if expected != sha256_digest(&bytes) {
                    return Err(Error::Strict(format!(
                        "SDK examples immutable source/revision closure mismatch: {relative}"
                    )));
                }
            }
            closure.extend_from_slice(relative.as_bytes());
            closure.push(0);
            closure.extend_from_slice(&bytes);
            closure.push(0);
        }
        let closure_hash = if source_files.len() == 1
            && source_files[0].as_str().or_else(|| {
                source_files[0]
                    .get("path")
                    .and_then(serde_json::Value::as_str)
            }) == Some(source_path)
        {
            sha256_digest(&fs::read(repository_root.join(source_path))?)
        } else {
            sha256_digest(&closure)
        };
        if closure_hash != actual_source_sha256 {
            return Err(Error::Strict(
                "SDK examples immutable source/revision closure digest mismatch".to_owned(),
            ));
        }
    }
    if !captured_snapshot {
        let paths = if let Some(source_files) = source_files {
            source_files
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .or_else(|| value.get("path").and_then(serde_json::Value::as_str))
                        .ok_or_else(|| {
                            Error::Strict(
                                "SDK examples source authority file is not a path string"
                                    .to_owned(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![source_path]
        };
        for relative in paths {
            let expected = git_blob_sha256(repository_root, authority_revision, relative)?;
            let actual = sha256_digest(&fs::read(repository_root.join(relative))?);
            if expected != actual {
                return Err(Error::Strict(format!(
                    "SDK examples source authority revision does not contain current bytes: {relative}"
                )));
            }
        }
    }
    Ok(authority_digest)
}

fn git_blob_sha256(
    repository_root: &Path,
    revision: &str,
    relative: &str,
) -> Result<String, Error> {
    let object = format!("{revision}:{relative}");
    let output = git_command()
        .arg("-C")
        .arg(repository_root)
        .arg("show")
        .arg(&object)
        .output()
        .map_err(Error::Io)?;
    if !output.status.success() {
        return Err(Error::Strict(format!(
            "SDK examples source authority cannot resolve Git blob {object}"
        )));
    }
    Ok(sha256_digest(&output.stdout))
}

fn is_portable_relative_path(path: &Path) -> bool {
    !path.is_absolute()
        && !path
            .components()
            .any(|component| component == std::path::Component::ParentDir)
        && !path.to_string_lossy().contains('\\')
        && !path.to_string_lossy().contains(':')
}

/// A qualified execution claim must be backed by bytes that this importer can
/// inspect. Hash strings alone are not evidence: a producer can write a hash
/// for a path that does not exist without running the scenario. Empty output is
/// valid when its actual bytes are bound and the receipt records assertions.
/// Pending receipts are intentionally left available for generators that have
/// rendered a snippet but do not yet have an installable artifact.
fn validate_qualified_receipt(
    snippet: &serde_json::Map<String, serde_json::Value>,
    bundle_root: &Path,
    declared_files: &HashSet<String>,
    relative_snippet: &str,
    expected_source_revision: Option<&str>,
    expected_source_path: Option<&str>,
    require_source_identity: bool,
) -> Result<(), Error> {
    let Some(validation) = snippet
        .get("validation")
        .and_then(serde_json::Value::as_object)
    else {
        return Ok(());
    };
    let Some(receipt) = validation
        .get("receipt")
        .and_then(serde_json::Value::as_object)
    else {
        return Ok(());
    };
    if receipt.get("status").and_then(serde_json::Value::as_str) != Some("qualified") {
        return Ok(());
    }
    if require_source_identity {
        let receipt_revision = receipt
            .get("source_revision")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                Error::Strict(format!(
                    "SDK examples qualified receipt for {relative_snippet} has no source revision"
                ))
            })?;
        if Some(receipt_revision) != expected_source_revision {
            return Err(Error::Strict(format!(
                "SDK examples qualified receipt for {relative_snippet} has source revision mismatch"
            )));
        }
        let receipt_path = receipt
            .get("source_path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                Error::Strict(format!(
                    "SDK examples qualified receipt for {relative_snippet} has no source path"
                ))
            })?;
        if Some(receipt_path) != expected_source_path {
            return Err(Error::Strict(format!(
                "SDK examples qualified receipt for {relative_snippet} has source path mismatch"
            )));
        }
    }

    if let Some(artifact) = receipt.get("artifact") {
        verify_bound_receipt_artifact(
            artifact,
            bundle_root,
            declared_files,
            relative_snippet,
            "artifact",
            true,
        )?;
    } else {
        // sdk-examples emits separate compile, runtime, and installed-package
        // artifacts. Keep accepting the compact `artifact` form for small
        // producers, but when the expanded form is used every stage must be
        // present and bound to bytes in this bundle.
        let stages = [
            (
                "compile_artifact_path",
                "compile_artifact_sha256",
                "compile artifact",
            ),
            (
                "runtime_artifact_path",
                "runtime_artifact_sha256",
                "runtime artifact",
            ),
            (
                "package_artifact_path",
                "package_artifact_sha256",
                "package artifact",
            ),
        ];
        let any_stage = stages.iter().any(|(path_key, hash_key, _)| {
            receipt.contains_key(*path_key) || receipt.contains_key(*hash_key)
        });
        if !any_stage {
            return Err(Error::Strict(format!(
                "SDK examples qualified receipt for {relative_snippet} has no bound artifact"
            )));
        }
        for (path_key, hash_key, label) in stages {
            let path = receipt
                .get(path_key)
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    Error::Strict(format!(
                        "SDK examples qualified receipt for {relative_snippet} has no {label} path"
                    ))
                })?;
            let hash = receipt
                .get(hash_key)
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    Error::Strict(format!(
                        "SDK examples qualified receipt for {relative_snippet} has no {label} sha256"
                    ))
                })?;
            verify_bound_receipt_path_hash(
                path,
                hash,
                bundle_root,
                declared_files,
                relative_snippet,
                label,
                true,
            )?;
        }
    }

    // Accept either named output-artifact objects or inline non-empty output
    // strings. The former is preferred because it preserves exact bytes and a
    // portable path in the bundle; the latter is useful for tiny local probes.
    // An output artifact may be empty (many successful commands are silent),
    // but it still has to be accompanied by an explicit assertion record.
    let mut bound_output = false;
    for key in ["stdout_artifact", "stderr_artifact"] {
        if let Some(value) = receipt.get(key) {
            verify_bound_receipt_artifact(
                value,
                bundle_root,
                declared_files,
                relative_snippet,
                key,
                false,
            )?;
            bound_output = true;
        }
    }
    for (path_key, hash_key, label) in [
        ("stdout_path", "stdout_sha256", "stdout"),
        ("stderr_path", "stderr_sha256", "stderr"),
    ] {
        if receipt.contains_key(path_key) || receipt.contains_key(hash_key) {
            let path = receipt
                .get(path_key)
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    Error::Strict(format!(
                        "SDK examples qualified receipt for {relative_snippet} has no {label} path"
                    ))
                })?;
            let hash = receipt
                .get(hash_key)
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    Error::Strict(format!(
                        "SDK examples qualified receipt for {relative_snippet} has no {label} sha256"
                    ))
                })?;
            verify_bound_receipt_path_hash(
                path,
                hash,
                bundle_root,
                declared_files,
                relative_snippet,
                label,
                false,
            )?;
            bound_output = true;
        }
    }
    for key in ["stdout", "stderr"] {
        if let Some(value) = receipt.get(key).and_then(serde_json::Value::as_str) {
            if !value.is_empty() {
                bound_output = true;
                if let Some(declared_hash) = receipt
                    .get(&format!("{key}_sha256"))
                    .and_then(serde_json::Value::as_str)
                {
                    let actual_hash = sha256_digest(value.as_bytes());
                    if declared_hash != actual_hash {
                        return Err(Error::Strict(format!(
                            "SDK examples qualified receipt for {relative_snippet} has {key} sha256 mismatch: expected {actual_hash}, got {declared_hash}"
                        )));
                    }
                }
            }
        }
    }
    if !bound_output {
        return Err(Error::Strict(format!(
            "SDK examples qualified receipt for {relative_snippet} has no bound output evidence"
        )));
    }
    if !has_meaningful_assertions(receipt) {
        return Err(Error::Strict(format!(
            "SDK examples qualified receipt for {relative_snippet} has no meaningful assertions"
        )));
    }
    Ok(())
}

fn has_meaningful_assertions(receipt: &serde_json::Map<String, serde_json::Value>) -> bool {
    if receipt
        .get("assertion_count")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|count| count > 0)
    {
        return true;
    }
    receipt
        .get("assertions")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|assertions| {
            !assertions.is_empty()
                && assertions.iter().all(|assertion| match assertion {
                    serde_json::Value::String(value) => !value.trim().is_empty(),
                    serde_json::Value::Object(value) => value
                        .get("status")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|status| matches!(status, "passed" | "qualified" | "ok")),
                    _ => false,
                })
        })
}

fn verify_bound_receipt_artifact(
    value: &serde_json::Value,
    bundle_root: &Path,
    declared_files: &HashSet<String>,
    relative_snippet: &str,
    label: &str,
    require_non_empty: bool,
) -> Result<(), Error> {
    let object = value.as_object().ok_or_else(|| {
        Error::Strict(format!(
            "SDK examples qualified receipt for {relative_snippet} has invalid {label}"
        ))
    })?;
    let relative = object
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            Error::Strict(format!(
                "SDK examples qualified receipt for {relative_snippet} has no {label} path"
            ))
        })?;
    let declared_hash = object
        .get("sha256")
        .and_then(serde_json::Value::as_str)
        .filter(|hash| !hash.trim().is_empty())
        .ok_or_else(|| {
            Error::Strict(format!(
                "SDK examples qualified receipt for {relative_snippet} has no {label} sha256"
            ))
        })?;
    verify_bound_receipt_path_hash(
        relative,
        declared_hash,
        bundle_root,
        declared_files,
        relative_snippet,
        label,
        require_non_empty,
    )
}

fn verify_bound_receipt_path_hash(
    relative: &str,
    declared_hash: &str,
    bundle_root: &Path,
    declared_files: &HashSet<String>,
    relative_snippet: &str,
    label: &str,
    require_non_empty: bool,
) -> Result<(), Error> {
    let path = Path::new(relative);
    if !is_portable_relative_path(path) {
        return Err(Error::Strict(format!(
            "SDK examples qualified receipt for {relative_snippet} has unsafe {label} path: {relative}"
        )));
    }
    if !declared_files.contains(relative) {
        return Err(Error::Strict(format!(
            "SDK examples qualified receipt for {relative_snippet} has undeclared {label}: {relative}"
        )));
    }
    let artifact_path = bundle_root.join(path);
    let canonical_root = fs::canonicalize(bundle_root).map_err(|error| {
        Error::Strict(format!(
            "SDK examples qualified receipt for {relative_snippet} cannot resolve bundle root: {error}"
        ))
    })?;
    let canonical_artifact = fs::canonicalize(&artifact_path).map_err(|error| {
        Error::Strict(format!(
            "SDK examples qualified receipt for {relative_snippet} cannot read {label} {relative}: {error}"
        ))
    })?;
    if !canonical_artifact.starts_with(&canonical_root) {
        return Err(Error::Strict(format!(
            "SDK examples qualified receipt for {relative_snippet} resolves {label} outside its bundle: {relative}"
        )));
    }
    let bytes = fs::read(&canonical_artifact).map_err(|error| {
        Error::Strict(format!(
            "SDK examples qualified receipt for {relative_snippet} cannot read {label} {relative}: {error}"
        ))
    })?;
    if require_non_empty && bytes.is_empty() {
        return Err(Error::Strict(format!(
            "SDK examples qualified receipt for {relative_snippet} has empty {label}"
        )));
    }
    let actual_hash = sha256_digest(&bytes);
    if declared_hash != actual_hash {
        return Err(Error::Strict(format!(
            "SDK examples qualified receipt for {relative_snippet} has {label} sha256 mismatch: expected {actual_hash}, got {declared_hash}"
        )));
    }
    Ok(())
}

fn scan_crate(
    repository_root: &Path,
    crate_dir: &Path,
    json_root: Option<&Path>,
    require_rustdoc_json: bool,
    source_revision: &str,
    expected_toolchain: Option<&str>,
    source_state: &str,
    dirty_worktree: bool,
    registry_manifest: Option<&RegistryLookup>,
) -> Result<CrateBundle, Error> {
    let manifest = fs::read_to_string(crate_dir.join("Cargo.toml"))?;
    let package_name = manifest_value(&manifest, "name").unwrap_or_else(|| {
        crate_dir
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown")
            .to_owned()
    });
    let version =
        manifest_value(&manifest, "version").or_else(|| workspace_version(repository_root));
    let crate_name =
        manifest_value(&manifest, "lib.name").or_else(|| Some(package_name.replace('-', "_")));
    let publish = !manifest
        .lines()
        .any(|line| line.trim() == "publish = false");
    let mut files = collect_source_files(crate_dir)?;
    files.sort_by(|left, right| left.cmp(right));

    let mut public_items = Vec::new();
    let mut diagnostics = Vec::new();
    let mut sources = Vec::new();
    let mut guides = Vec::new();
    let mut examples = Vec::new();
    for path in &files {
        let bytes = fs::read(path)?;
        let relative = relative_path(repository_root, path);
        let contents = String::from_utf8_lossy(&bytes).into_owned();
        let is_markdown = path.extension().and_then(|ext| ext.to_str()) == Some("md");
        sources.push(SourceFile {
            path: relative.clone(),
            blake3: digest_bytes(&bytes),
            contents: contents.clone(),
        });
        if is_markdown {
            let title = markdown_title(&contents).unwrap_or_else(|| package_name.clone());
            let kind = if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case("README.md"))
            {
                "readme"
            } else {
                "guide"
            };
            guides.push(GuideSource {
                path: relative.clone(),
                title,
                contents,
                kind: kind.to_owned(),
            });
        } else {
            scan_rust_file(&contents, &relative, &mut public_items, &mut diagnostics);
            if relative.split('/').any(|part| part == "examples") {
                let name = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or("example")
                    .to_owned();
                examples.push(ExampleSource {
                    path: relative,
                    name,
                    contents,
                });
            }
        }
    }
    let content_blake3 = digest_bytes(
        sources
            .iter()
            .fold(String::new(), |mut text, source| {
                let _ = writeln!(text, "{} {}", source.path, source.blake3);
                text
            })
            .as_bytes(),
    );

    let json_matches = json_root
        .map(|root| find_rustdoc_json_all(root, &package_name))
        .unwrap_or_default();
    // Rustdoc receipts bind the compiled graph to the Rust sources that the
    // compiler consumed.  Guide Markdown and examples are carried in the
    // bundle separately, so they must not invalidate a compiler graph.
    let rust_source_blake3 = source_content_hash(crate_dir, repository_root)?;
    let mut graphs = Vec::new();
    for json_path in &json_matches {
        let bytes = fs::read(json_path)?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        let index_items = value
            .get("index")
            .and_then(serde_json::Value::as_object)
            .map_or(0, serde_json::Map::len);
        let receipt_path = receipt_path(json_path);
        let Some(receipt) = read_receipt(
            &receipt_path,
            repository_root,
            &bytes,
            &package_name,
            &rust_source_blake3,
            source_revision,
            expected_toolchain,
            require_rustdoc_json,
            &mut diagnostics,
        )?
        else {
            continue;
        };
        let mut graph_generated_source_aliases = HashMap::new();
        let graph_diagnostic_start = diagnostics.len();
        collect_rustdoc_generated_sources(
            &value,
            repository_root,
            crate_dir,
            &package_name,
            &receipt.profile,
            &mut sources,
            &mut graph_generated_source_aliases,
            &mut diagnostics,
        )?;
        let format_version = value
            .get("format_version")
            .and_then(serde_json::Value::as_u64);
        let mut graph_diagnostics = Vec::new();
        let graph_items = rustdoc_public_items_with_sources(
            &value,
            repository_root,
            crate_dir,
            &graph_generated_source_aliases,
            true,
            &mut graph_diagnostics,
        );
        diagnostics.extend(graph_diagnostics);
        if graph_items.is_empty() {
            diagnostics.push(Diagnostic {
                severity: "warning".to_owned(),
                code: "rustdoc_json_empty_public_graph".to_owned(),
                message: "rustdoc JSON resolved no public items for this feature/target profile"
                    .to_owned(),
                path: Some(relative_path(repository_root, json_path)),
                line: None,
            });
        }
        if index_items == 0 {
            diagnostics.push(Diagnostic {
                severity: "warning".to_owned(),
                code: "rustdoc_json_empty_index".to_owned(),
                message: "rustdoc JSON was supplied but contains no index entries".to_owned(),
                path: Some(relative_path(repository_root, json_path)),
                line: None,
            });
        }
        let provenance = RustdocProvenance {
            path: relative_path(repository_root, json_path),
            format_version,
            blake3: digest_bytes(&bytes),
            index_items,
            source_blake3: rust_source_blake3.clone(),
            source_revision: receipt.source_revision.clone(),
            toolchain: Some(receipt.toolchain.clone()),
            target: Some(receipt.target.clone()),
            features: receipt.features.clone(),
            profile_blake3: receipt.profile_blake3.clone(),
            receipt: Some(relative_path(repository_root, &receipt_path)),
            receipt_blake3: fs::read(&receipt_path).ok().as_deref().map(digest_bytes),
        };
        graphs.push(RustdocGraph {
            profile: receipt.profile,
            target: receipt.target,
            features: receipt.features,
            profile_blake3: receipt.profile_blake3,
            public_items: graph_items,
            diagnostics: diagnostics[graph_diagnostic_start..]
                .iter()
                .filter(|diagnostic| diagnostic.code == "rustdoc_generated_source_missing")
                .cloned()
                .collect(),
            rustdoc: provenance,
        });
    }
    let (analysis_mode, rustdoc) = if !graphs.is_empty() {
        diagnostics.retain(|diagnostic| diagnostic.code != "unresolved_re_export");
        // A qualified compiler graph is the complete public-item authority.
        // Do not union it with the conservative source scanner: doing so
        // duplicates re-exports and can expose cfg-inactive or malformed
        // fallback names in an otherwise qualified reference.
        public_items = merge_compiler_public_items(&graphs);
        let undocumented = public_items
            .iter()
            .filter(|item| item.docs.is_none())
            .count();
        if undocumented > 0 {
            diagnostics.push(Diagnostic {
                severity: "warning".to_owned(),
                code: "rustdoc_public_items_missing_docs".to_owned(),
                message: format!(
                    "{undocumented} compiler-resolved public declarations have no Rustdoc summary"
                ),
                path: Some(relative_path(repository_root, crate_dir)),
                line: None,
            });
        }
        (
            "rustdoc-json".to_owned(),
            graphs.first().map(|graph| graph.rustdoc.clone()),
        )
    } else {
        if !json_matches.is_empty() {
            diagnostics.push(Diagnostic {
                severity: if require_rustdoc_json { "error" } else { "warning" }.to_owned(),
                code: "rustdoc_graph_unqualified".to_owned(),
                message: "Compiled rustdoc JSON was present but no graph had a valid source/profile receipt".to_owned(),
                path: Some(relative_path(repository_root, crate_dir)),
                line: None,
            });
        }
        diagnostics.push(Diagnostic {
            severity: "warning".to_owned(),
            code: if require_rustdoc_json {
                "rustdoc_json_required"
            } else {
                "rustdoc_json_unavailable"
            }
            .to_owned(),
            message: if require_rustdoc_json {
                "Strict mode requires compiled rustdoc JSON for every crate"
            } else {
                "No compiled rustdoc JSON supplied; public item inventory is conservative source scanning"
            }
            .to_owned(),
            path: Some(relative_path(repository_root, crate_dir)),
            line: None,
        });
        ("source-fallback".to_owned(), None)
    };

    public_items.sort_by(|left, right| {
        left.source_path
            .cmp(&right.source_path)
            .then(left.source_line.cmp(&right.source_line))
            .then(left.name.cmp(&right.name))
    });
    guides.sort_by(|left, right| left.path.cmp(&right.path));
    examples.sort_by(|left, right| left.path.cmp(&right.path));
    let crate_relative_path = relative_path(repository_root, crate_dir);
    guides.push(reference_guide(
        &package_name,
        &crate_relative_path,
        &public_items,
    ));
    guides.sort_by(|left, right| left.path.cmp(&right.path));
    let registry = registry_manifest
        .and_then(|manifest| {
            manifest.get(&(
                "cargo".to_owned(),
                package_name.clone(),
                version.clone().unwrap_or_default(),
            ))
        })
        .cloned();
    let registry_verified = registry
        .as_ref()
        .is_some_and(|entry| entry.status == "published");
    let package_instructions = if publish {
        vec![PackageInstruction {
            ecosystem: "cargo".to_owned(),
            package: package_name.clone(),
            command: if registry_verified {
                format!(
                    "cargo add {package_name}@={}",
                    registry
                        .as_ref()
                        .map(|entry| entry.version.as_str())
                        .unwrap_or_default()
                )
            } else if dirty_worktree
                || source_state == "working-tree"
                || source_revision == "unknown"
            {
                format!("cargo add {package_name} --path {crate_relative_path}")
            } else {
                format!(
                    "cargo add {package_name} --git https://github.com/acyclic-labs/sdk --rev {source_revision}"
                )
            },
            registry,
        }]
    } else {
        Vec::new()
    };
    let navigation = format!("crates/{package_name}");
    let coverage = DocCoverage {
        guides: guides.len(),
        examples: examples.len(),
        public_items: public_items.len(),
        documented_items: public_items
            .iter()
            .filter(|item| {
                item.docs
                    .as_deref()
                    .is_some_and(|docs| !docs.trim().is_empty())
            })
            .count(),
        conditional_items: public_items.iter().filter(|item| item.conditional).count(),
    };
    Ok(CrateBundle {
        package_name: package_name.clone(),
        crate_name,
        path: relative_path(repository_root, crate_dir),
        publish,
        version,
        availability: if publish && registry_verified {
            "registry-verified".to_owned()
        } else if publish {
            "registry-unverified".to_owned()
        } else {
            "source-only".to_owned()
        },
        analysis_mode,
        sources,
        guides,
        examples,
        package_instructions,
        navigation,
        public_items,
        graphs,
        rustdoc,
        diagnostics,
        content_blake3,
        coverage,
    })
}

fn reference_guide(package_name: &str, crate_path: &str, items: &[PublicItem]) -> GuideSource {
    let mut contents = format!("# {package_name} API reference\n\n");
    contents.push_str("This reference is generated from the source-bound rustdoc public graph. Each symbol links to its Rust source location.\n\n");
    let mut current_kind = String::new();
    for item in items {
        if item.kind != current_kind {
            current_kind = item.kind.clone();
            let _ = writeln!(contents, "## {}", title_case(&current_kind));
            contents.push('\n');
        }
        let anchor = item.name.to_ascii_lowercase().replace([' ', ':'], "-");
        let summary = item
            .docs
            .as_deref()
            .and_then(|docs| docs.lines().find(|line| !line.trim().is_empty()))
            .unwrap_or("No declaration summary was provided.");
        let location = match (&item.source_path, item.source_line) {
            (Some(path), Some(line)) => format!("[`{path}:{line}`](/{path}#L{line})"),
            (Some(path), None) => format!("[`{path}`](/{path})"),
            _ => "generated source".to_owned(),
        };
        let module = item
            .module_path
            .as_deref()
            .map(|path| format!("\nModule path: `{path}`\n"))
            .unwrap_or_default();
        let signature = item
            .signature_text
            .as_deref()
            .map(|signature| format!("\nCompiler signature: `{signature}`\n"))
            .unwrap_or_default();
        let _ = writeln!(
            contents,
            "### `{}` {{#{anchor}}}\n{}{}\n{}\n\nSource: {}\n",
            item.name,
            module,
            signature,
            summary.trim(),
            location
        );
    }
    if items.is_empty() {
        contents.push_str("No qualified public items are available for this profile.\n");
    }
    GuideSource {
        path: format!("{crate_path}/REFERENCE.md"),
        title: format!("{package_name} API reference"),
        contents,
        kind: "reference".to_owned(),
    }
}

fn title_case(value: &str) -> String {
    value
        .split('-')
        .map(|part| {
            let mut chars = part.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_uppercase().collect::<String>() + chars.as_str()
            })
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn find_package_dir(repository_root: &Path, package_name: &str) -> Result<PathBuf, Error> {
    let crates_root = repository_root.join("rust/crates");
    for entry in fs::read_dir(&crates_root)? {
        let path = entry?.path();
        if !path.is_dir() || !path.join("Cargo.toml").is_file() {
            continue;
        }
        let manifest = fs::read_to_string(path.join("Cargo.toml"))?;
        if manifest_value(&manifest, "name").as_deref() == Some(package_name) {
            return Ok(path);
        }
    }
    Err(Error::Strict(format!(
        "profile package {package_name} is absent from rust/crates"
    )))
}

fn source_content_hash(crate_dir: &Path, repository_root: &Path) -> Result<String, Error> {
    let mut paths = collect_source_files(crate_dir)?;
    paths.retain(|path| {
        path == &crate_dir.join("Cargo.toml")
            || path
                .strip_prefix(crate_dir.join("src"))
                .ok()
                .is_some_and(|relative| {
                    relative.extension().and_then(|ext| ext.to_str()) == Some("rs")
                })
    });
    paths.sort();
    let mut text = String::new();
    for path in &paths {
        let source = fs::read(&path)?;
        let _ = writeln!(
            text,
            "{} {}",
            relative_path(repository_root, &path),
            digest_bytes(&source)
        );
    }
    Ok(digest_bytes(text.as_bytes()))
}

fn declared_crate_types(crate_dir: &Path) -> Result<Vec<String>, Error> {
    let manifest = fs::read_to_string(crate_dir.join("Cargo.toml"))?;
    let mut in_lib = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_lib = trimmed == "[lib]";
        } else if in_lib && trimmed.starts_with("crate-type") {
            let Some((_, value)) = trimmed.split_once('=') else {
                continue;
            };
            let types = value
                .trim()
                .trim_start_matches('[')
                .trim_end_matches(']')
                .split(',')
                .map(|kind| kind.trim().trim_matches('"').trim_matches('\'').to_owned())
                .filter(|kind| !kind.is_empty())
                .collect::<Vec<_>>();
            if !types.is_empty() {
                return Ok(types);
            }
        }
    }
    Ok(vec!["lib".to_owned()])
}

fn public_source_paths(crate_dir: &Path, repository_root: &Path) -> Result<Vec<String>, Error> {
    let mut paths = collect_source_files(crate_dir)?;
    paths.retain(|path| {
        path.strip_prefix(crate_dir.join("src"))
            .ok()
            .is_some_and(|relative| relative.extension().and_then(|ext| ext.to_str()) == Some("rs"))
    });
    paths.sort();
    Ok(paths
        .into_iter()
        .map(|path| relative_path(repository_root, &path))
        .collect())
}

fn guide_source_metadata(
    crate_dir: &Path,
    repository_root: &Path,
) -> Result<(Vec<String>, String), Error> {
    let mut paths = collect_source_files(crate_dir)?;
    paths.retain(|path| path.extension().and_then(|ext| ext.to_str()) == Some("md"));
    paths.sort();
    let mut closure = String::new();
    let mut relative_paths = Vec::with_capacity(paths.len());
    for path in paths {
        let bytes = fs::read(&path)?;
        let relative = relative_path(repository_root, &path);
        let _ = writeln!(closure, "{} {}", relative, digest_bytes(&bytes));
        relative_paths.push(relative);
    }
    Ok((relative_paths, digest_bytes(closure.as_bytes())))
}

fn is_private_sdk_crate(path: &Path) -> bool {
    let directory_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if directory_name.starts_with("sdk-") {
        return true;
    }
    let Ok(manifest) = fs::read_to_string(path.join("Cargo.toml")) else {
        return false;
    };
    let package_name = manifest_value(&manifest, "name").unwrap_or_default();
    package_name.starts_with("sdk-") || package_name.starts_with("acyclic-sdk-")
}

fn receipt_path(json_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.receipt.json", json_path.display()))
}

fn read_receipt(
    path: &Path,
    repository_root: &Path,
    json_bytes: &[u8],
    package_name: &str,
    source_blake3: &str,
    source_revision: &str,
    expected_toolchain: Option<&str>,
    require_receipt: bool,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<Option<RustdocReceipt>, Error> {
    let relative = relative_path(repository_root, path);
    if !path.is_file() {
        let message = format!("rustdoc JSON has no source-binding receipt: {relative}");
        diagnostics.push(Diagnostic {
            severity: if require_receipt { "error" } else { "warning" }.to_owned(),
            code: "rustdoc_receipt_missing".to_owned(),
            message,
            path: Some(relative),
            line: None,
        });
        if require_receipt {
            return Err(Error::Strict(format!(
                "rustdoc source-binding receipt missing for {package_name}"
            )));
        }
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    let receipt: RustdocReceipt = serde_json::from_slice(&bytes)?;
    let expected_json_blake3 = digest_bytes(json_bytes);
    let valid = receipt.schema_version == 1
        && receipt.package_name == package_name
        && receipt.source_blake3 == source_blake3
        && receipt.source_revision == source_revision
        && expected_toolchain.is_none_or(|toolchain| receipt.toolchain == toolchain)
        && !receipt.profile.trim().is_empty()
        && !receipt.profile_blake3.trim().is_empty()
        && receipt.rustdoc_json_blake3 == expected_json_blake3;
    if !valid {
        let message = format!(
            "rustdoc receipt does not bind package {package_name} to the current source and JSON bytes (source expected {source_blake3}, got {}; revision expected {source_revision}, got {}; toolchain expected {}, got {}; JSON expected {expected_json_blake3}, got {})",
            receipt.source_blake3,
            receipt.source_revision,
            expected_toolchain.unwrap_or("any"),
            receipt.toolchain,
            receipt.rustdoc_json_blake3,
        );
        diagnostics.push(Diagnostic {
            severity: if require_receipt { "error" } else { "warning" }.to_owned(),
            code: "rustdoc_receipt_mismatch".to_owned(),
            message,
            path: Some(relative),
            line: None,
        });
        if require_receipt {
            return Err(Error::Strict(format!(
                "stale or invalid rustdoc source-binding receipt for {package_name}"
            )));
        }
        return Ok(None);
    }
    Ok(Some(receipt))
}

fn markdown_title(contents: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let title = line.trim().strip_prefix("# ")?.trim();
        (!title.is_empty()).then(|| title.to_owned())
    })
}

fn load_profile_manifest(path: &Path) -> Result<Vec<AnalysisProfile>, Error> {
    let contents = fs::read(path)?;
    load_profile_manifest_bytes(&contents, path)
}

fn load_profile_manifest_bytes(
    contents: &[u8],
    path: &Path,
) -> Result<Vec<AnalysisProfile>, Error> {
    let manifest: ProfileManifestFile = serde_json::from_slice(contents)?;
    if manifest.schema_version != 1 {
        return Err(Error::Strict(format!(
            "unsupported rustdoc profile manifest schema {} in {}",
            manifest.schema_version,
            path.display()
        )));
    }
    let mut names = HashSet::new();
    for profile in &manifest.profiles {
        if profile.name.trim().is_empty() || profile.packages.is_empty() {
            return Err(Error::Strict(format!(
                "rustdoc profile {} must have a name and at least one package",
                profile.name
            )));
        }
        if !names.insert(profile.name.clone()) {
            return Err(Error::Strict(format!(
                "duplicate rustdoc profile name {}",
                profile.name
            )));
        }
        validate_profile_scope(profile)?;
        let mut packages = HashSet::new();
        for package in &profile.packages {
            if package.package.trim().is_empty() || package.target.trim().is_empty() {
                return Err(Error::Strict(format!(
                    "rustdoc profile {} has a package without a package name or target",
                    profile.name
                )));
            }
            if !packages.insert(package.package.clone()) {
                return Err(Error::Strict(format!(
                    "rustdoc profile {} repeats package {}",
                    profile.name, package.package
                )));
            }
        }
    }
    Ok(manifest.profiles)
}

fn validate_profile_scope(profile: &AnalysisProfile) -> Result<(), Error> {
    let Some(scope) = profile.scope.as_ref() else {
        return Ok(());
    };
    match scope.kind.as_str() {
        "all-pkgs" => {
            if scope.family.is_some() || scope.package.is_some() || scope.version.is_some() {
                return Err(Error::Strict(format!(
                    "all-pkgs scope for profile {} cannot carry family, package, or version",
                    profile.name
                )));
            }
        }
        "release-family" => {
            let family = scope.family.as_deref().filter(|value| !value.trim().is_empty());
            let package = scope.package.as_deref().filter(|value| !value.trim().is_empty());
            let version = scope.version.as_deref().filter(|value| !value.trim().is_empty());
            if family.is_none() || package.is_none() || version.is_none() {
                return Err(Error::Strict(format!(
                    "release-family scope for profile {} requires family, package, and version",
                    profile.name
                )));
            }
            if !profile
                .packages
                .iter()
                .any(|entry| Some(entry.package.as_str()) == package)
            {
                return Err(Error::Strict(format!(
                    "release-family scope for profile {} must name one of its package entries",
                    profile.name
                )));
            }
        }
        other => {
            return Err(Error::Strict(format!(
                "unsupported rustdoc profile scope kind {other:?} for profile {}",
                profile.name
            )));
        }
    }
    Ok(())
}

fn required_strict_packages(
    profiles: &[AnalysisProfile],
    release: Option<&ReleaseQualification>,
    crates: &[CrateBundle],
    repository_root: &Path,
) -> Result<HashSet<String>, Error> {
    let all_packages = || {
        crates
            .iter()
            .filter(|crate_bundle| {
                crate_bundle.publish
                    && crate_has_library_target(&repository_root.join(&crate_bundle.path))
            })
            .map(|crate_bundle| crate_bundle.package_name.clone())
            .collect::<HashSet<_>>()
    };
    let Some(release) = release else {
        return Ok(all_packages());
    };
    if profiles.is_empty()
        || profiles.iter().any(|profile| {
            !profile
                .scope
                .as_ref()
                .is_some_and(|scope| scope.kind == "release-family")
        })
    {
        return Ok(all_packages());
    }

    let family = release_family_from_tag(&release.tag).ok_or_else(|| {
        Error::Strict(format!(
            "release-family profile cannot derive a family from immutable tag {}",
            release.tag
        ))
    })?;
    let mut required = HashSet::new();
    for profile in profiles {
        let scope = profile
            .scope
            .as_ref()
            .expect("release-family scope checked above");
        if scope.family.as_deref() != Some(family.as_str()) {
            return Err(Error::Strict(format!(
                "profile {} scopes family {:?}, but release tag {} is {:?}",
                profile.name,
                scope.family,
                release.tag,
                family
            )));
        }
        if scope.version.as_deref() != Some(release.version.as_str()) {
            return Err(Error::Strict(format!(
                "profile {} scopes version {:?}, but immutable release {} is {}",
                profile.name, scope.version, release.tag, release.version
            )));
        }
        let package = scope.package.as_deref().ok_or_else(|| {
            Error::Strict(format!(
                "release-family profile {} has no exact package scope",
                profile.name
            ))
        })?;
        let crate_bundle = crates
            .iter()
            .find(|crate_bundle| crate_bundle.package_name == package)
            .ok_or_else(|| {
                Error::Strict(format!(
                    "release-family profile {} names missing package {}",
                    profile.name, package
                ))
            })?;
        if crate_bundle.version.as_deref() != scope.version.as_deref() {
            return Err(Error::Strict(format!(
                "release-family profile {} package {} has source version {:?}, expected {}",
                profile.name, package, crate_bundle.version, release.version
            )));
        }
        required.insert(package.to_owned());
    }
    Ok(required)
}

fn release_family_from_tag(tag: &str) -> Option<String> {
    let direct = [
        ("filesystem-", "filesystem"),
        ("fs-", "filesystem"),
        ("stream-", "stream"),
        ("harness-", "harness"),
        ("inference-", "inference"),
        ("machines-", "machines"),
        ("objects-", "objects"),
    ]
    .into_iter()
    .find_map(|(prefix, family)| tag.starts_with(prefix).then_some(family.to_owned()));
    if direct.is_some() {
        return direct;
    }
    [
        ("publish/acyclic-fs/", "filesystem"),
        ("publish/acyclic-stream/", "stream"),
        ("publish/acyclic-harness/", "harness"),
        ("publish/acyclic-inference/", "inference"),
        ("publish/acyclic-machines/", "machines"),
        ("publish/acyclic-objects/", "objects"),
    ]
    .into_iter()
    .find_map(|(prefix, family)| tag.starts_with(prefix).then_some(family.to_owned()))
}

/// Older releases predate the checked-in multi-target profile manifest. Their
/// historical receipt still needs a source-bound profile, so infer a stable
/// host profile from the Cargo package manifests that exist in that release.
/// This intentionally records only facts present in the archived source and
/// never imports package names or feature flags from the current checkout.
fn infer_historical_profile_manifest(
    repository_root: &Path,
) -> Result<Vec<AnalysisProfile>, Error> {
    let crates_root = repository_root.join("rust/crates");
    let mut packages = Vec::new();
    for entry in fs::read_dir(&crates_root)? {
        let path = entry?.path();
        if !path.is_dir() || !path.join("Cargo.toml").is_file() {
            continue;
        }
        let manifest = fs::read_to_string(path.join("Cargo.toml"))?;
        let Some(package) = manifest_value(&manifest, "name") else {
            continue;
        };
        packages.push(ProfilePackage {
            package,
            target: "host".to_owned(),
            features: Vec::new(),
            default_features: true,
            crate_types: Vec::new(),
        });
    }
    packages.sort_by(|left, right| left.package.cmp(&right.package));
    if packages.is_empty() {
        return Err(Error::Strict(format!(
            "cannot infer a historical profile: no Cargo packages found in {}",
            crates_root.display()
        )));
    }
    Ok(vec![AnalysisProfile {
        name: "host-default-inferred".to_owned(),
        packages,
        scope: None,
    }])
}

fn evaluate_profiles(
    profiles: &[AnalysisProfile],
    crates: &[CrateBundle],
    diagnostics: &mut Vec<Diagnostic>,
    source_revision: &str,
    expected_toolchain: Option<&str>,
) -> Result<Vec<ProfileStatus>, Error> {
    let by_package = crates
        .iter()
        .map(|crate_bundle| (crate_bundle.package_name.as_str(), crate_bundle))
        .collect::<HashMap<_, _>>();
    let mut statuses = profiles
        .iter()
        .map(|profile| -> Result<ProfileStatus, Error> {
            let mut missing_packages = Vec::new();
            let mut unresolved_packages = Vec::new();
            let profile_digest = profile_blake3(profile);
            for package in &profile.packages {
                match by_package.get(package.package.as_str()) {
                    None => {
                        missing_packages.push(package.package.clone());
                        diagnostics.push(Diagnostic {
                            severity: "error".to_owned(),
                            code: "profile_package_missing".to_owned(),
                            message: format!(
                                "profile {} expects package {} but it is absent from the public crate inventory",
                                profile.name, package.package
                            ),
                            path: None,
                            line: None,
                        });
                    }
                    Some(crate_bundle) => {
                        let resolved_target = resolve_profile_target(&package.target, expected_toolchain)?;
                        let graph = crate_bundle.graphs.iter().find(|graph| {
                            graph.profile_blake3 == profile_digest
                                && graph.target == resolved_target
                                && graph.features == normalized_features(&package.features)
                                && graph.rustdoc.source_revision == source_revision
                                && expected_toolchain
                                    .is_none_or(|toolchain| graph.rustdoc.toolchain.as_deref() == Some(toolchain))
                                && !graph.public_items.is_empty()
                        });
                        let generated_source_gap = graph.is_some_and(|graph| {
                            graph
                                .diagnostics
                                .iter()
                                .any(|diagnostic| diagnostic.code == "rustdoc_generated_source_missing")
                        });
                        let public_identity_gap = graph.is_some_and(|graph| {
                            graph
                                .public_items
                                .iter()
                                .any(|item| !rustdoc_public_item_identity_complete(item))
                        });
                        if graph.is_none() || generated_source_gap || public_identity_gap {
                            unresolved_packages.push(package.package.clone());
                            diagnostics.push(Diagnostic {
                                severity: "error".to_owned(),
                                code: if generated_source_gap {
                                    "profile_generated_source_missing"
                                } else if public_identity_gap {
                                    "profile_public_identity_incomplete"
                                } else {
                                    "profile_public_graph_unresolved"
                                }
                                .to_owned(),
                                message: format!(
                                    "profile {} ({}; features: {}) has no complete source-bound public graph for package {}{}",
                                    profile.name,
                                    package.target,
                                    if package.features.is_empty() {
                                        "default".to_owned()
                                    } else {
                                        package.features.join(",")
                                    },
                                    package.package,
                                    if generated_source_gap {
                                        "; a package-owned generated source referenced by rustdoc is missing"
                                    } else if public_identity_gap {
                                        "; at least one public compiler item lacks a module path or semantic signature"
                                    } else {
                                        ""
                                    }
                                ),
                                path: Some(crate_bundle.path.clone()),
                                line: None,
                            });
                        }
                    }
                }
            }
            let complete = missing_packages.is_empty() && unresolved_packages.is_empty();
            Ok(ProfileStatus {
                profile: profile.clone(),
                complete,
                profile_blake3: profile_digest,
                missing_packages,
                unresolved_packages,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    statuses.sort_by(|left, right| left.profile.name.cmp(&right.profile.name));
    Ok(statuses)
}

fn rustdoc_public_item_identity_complete(item: &PublicItem) -> bool {
    item.module_path
        .as_deref()
        .is_some_and(|path| !path.trim().is_empty())
        && (item.signature.is_some()
            || item
                .signature_text
                .as_deref()
                .is_some_and(|signature| !signature.trim().is_empty())
            || item.reexport.is_some())
}

/// Extract the compiler-resolved public graph from rustdoc JSON. The JSON
/// format is intentionally read as `serde_json::Value`: its schema is
/// experimental, and the recorded format version must remain visible in the
/// bundle instead of being hidden behind an accidentally stale Rust type.
#[cfg(test)]
fn rustdoc_public_items(
    value: &serde_json::Value,
    repository_root: &Path,
    crate_dir: &Path,
    diagnostics: &mut Vec<Diagnostic>,
) -> Vec<PublicItem> {
    rustdoc_public_items_with_sources(
        value,
        repository_root,
        crate_dir,
        &HashMap::new(),
        false,
        diagnostics,
    )
}

fn path_identity(path: &str) -> String {
    path.replace('\\', "/").to_ascii_lowercase()
}

/// Retain generated Rust source files referenced by the compiler graph. Build
/// scripts often place protobuf output in `OUT_DIR`; the exact bytes are still
/// a valid source authority when the graph receipt points at that invocation.
/// The logical path is stable across target-directory relocations and is
/// recorded in the bundle alongside the generated bytes.
fn collect_rustdoc_generated_sources(
    value: &serde_json::Value,
    repository_root: &Path,
    crate_dir: &Path,
    package_name: &str,
    profile_name: &str,
    sources: &mut Vec<SourceFile>,
    aliases: &mut HashMap<String, String>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<(), Error> {
    let Some(index) = value.get("index").and_then(serde_json::Value::as_object) else {
        return Ok(());
    };
    let package_marker = format!("/{}-", package_name).to_ascii_lowercase();
    let mut seen_paths = HashSet::new();
    for item in index.values() {
        if item.get("crate_id").and_then(serde_json::Value::as_u64) != Some(0) {
            continue;
        }
        let Some(filename) = item
            .get("span")
            .and_then(serde_json::Value::as_object)
            .and_then(|span| span.get("filename"))
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let normalized = filename.replace('\\', "/");
        let lowered = normalized.to_ascii_lowercase();
        if !lowered.contains("/out/")
            || !lowered.ends_with(".rs")
            || !lowered.contains(&package_marker)
        {
            continue;
        }
        if !seen_paths.insert(path_identity(&normalized)) {
            continue;
        }
        let source_path = PathBuf::from(filename);
        let source_path = if source_path.is_absolute() {
            source_path
        } else {
            repository_root.join(source_path)
        };
        if !source_path.is_file() {
            diagnostics.push(Diagnostic {
                severity: "error".to_owned(),
                code: "rustdoc_generated_source_missing".to_owned(),
                message: format!(
                    "rustdoc graph references generated source that is not present: {}",
                    source_path.display()
                ),
                path: Some(normalized),
                line: None,
            });
            continue;
        }
        let bytes = fs::read(&source_path)?;
        let contents = String::from_utf8(bytes.clone()).map_err(|error| {
            Error::Strict(format!(
                "rustdoc generated source is not UTF-8: {} ({error})",
                source_path.display()
            ))
        })?;
        let file_name = source_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::Strict("generated rustdoc source has no file name".to_owned()))?;
        let profile_segment = generated_source_profile_segment(profile_name);
        let base_logical_path = format!(
            "{}/src/generated/{profile_segment}/{file_name}",
            relative_path(repository_root, crate_dir)
        );
        let source_digest = digest_bytes(normalized.as_bytes());
        let mut logical_path = base_logical_path.clone();
        if sources
            .iter()
            .any(|source| source.path == logical_path && source.blake3 != digest_bytes(&bytes))
        {
            let stem = Path::new(file_name)
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or(file_name);
            let extension = Path::new(file_name)
                .extension()
                .and_then(|value| value.to_str())
                .map(|value| format!(".{value}"))
                .unwrap_or_default();
            let directory = Path::new(&base_logical_path)
                .parent()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            logical_path = format!("{directory}/{stem}-{}{extension}", &source_digest[..12]);
            if sources
                .iter()
                .any(|source| source.path == logical_path && source.blake3 != digest_bytes(&bytes))
            {
                logical_path = format!("{directory}/{stem}-{source_digest}{extension}");
            }
        }
        if let Some(existing) = aliases.get(&path_identity(&normalized)) {
            if existing != &logical_path {
                continue;
            }
        }
        if let Some(existing) = sources.iter().find(|source| source.path == logical_path) {
            if existing.blake3 != digest_bytes(&bytes) {
                return Err(Error::Strict(format!(
                    "generated rustdoc source path collision at {logical_path}"
                )));
            }
        } else {
            sources.push(SourceFile {
                path: logical_path.clone(),
                blake3: digest_bytes(&bytes),
                contents,
            });
        }
        aliases.insert(path_identity(&normalized), logical_path);
    }
    Ok(())
}

fn generated_source_profile_segment(profile_name: &str) -> String {
    let segment = profile_name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    if segment.is_empty() {
        "profile".to_owned()
    } else {
        segment
    }
}

/// Resolve generated compiler spans to retained, content-addressed source
/// entries and omit declarations whose compiler span belongs to an external
/// dependency.  External impl items can appear while traversing a public SDK
/// type, but their source is not part of this crate's authority closure and
/// must not be relabelled as a local declaration.
fn rustdoc_public_items_with_sources(
    value: &serde_json::Value,
    repository_root: &Path,
    crate_dir: &Path,
    generated_source_aliases: &HashMap<String, String>,
    require_retained_source: bool,
    diagnostics: &mut Vec<Diagnostic>,
) -> Vec<PublicItem> {
    let Some(root_id) = value.get("root").and_then(serde_json::Value::as_u64) else {
        diagnostics.push(Diagnostic {
            severity: "error".to_owned(),
            code: "rustdoc_json_missing_root".to_owned(),
            message: "rustdoc JSON has no numeric root item".to_owned(),
            path: None,
            line: None,
        });
        return Vec::new();
    };
    let Some(index) = value.get("index").and_then(serde_json::Value::as_object) else {
        diagnostics.push(Diagnostic {
            severity: "error".to_owned(),
            code: "rustdoc_json_missing_index".to_owned(),
            message: "rustdoc JSON has no item index".to_owned(),
            path: None,
            line: None,
        });
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut pending = vec![(root_id.to_string(), false, None::<String>)];
    // Some wasm-bindgen wrapper crates expose their public adapter structs in
    // the rustdoc index without linking them from the synthetic crate module.
    // The compiler graph is still authoritative: walk those index entries
    // when the root module has no children so the profile records the actual
    // wasm API instead of an empty graph.
    let root_has_children = index
        .get(&root_id.to_string())
        .and_then(serde_json::Value::as_object)
        .and_then(|item| item.get("inner"))
        .and_then(serde_json::Value::as_object)
        .and_then(|inner| inner.get("module"))
        .and_then(serde_json::Value::as_object)
        .and_then(|module| module.get("items"))
        .and_then(serde_json::Value::as_array)
        .is_some_and(|children| !children.is_empty());
    if !root_has_children {
        pending.extend(
            index
                .keys()
                .filter(|id| id.as_str() != root_id.to_string())
                .cloned()
                .map(|id| (id, false, None)),
        );
    }
    let mut items = Vec::new();
    while let Some((id, inherited_public, parent_path)) = pending.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        let Some(item) = index.get(&id).and_then(serde_json::Value::as_object) else {
            diagnostics.push(Diagnostic {
                severity: "error".to_owned(),
                code: "rustdoc_json_missing_item".to_owned(),
                message: format!("rustdoc root references missing item {id}"),
                path: None,
                line: None,
            });
            continue;
        };
        if item.get("crate_id").and_then(serde_json::Value::as_u64) != Some(0) {
            continue;
        }
        let visibility = item.get("visibility").and_then(serde_json::Value::as_str);
        let Some(inner) = item.get("inner").and_then(serde_json::Value::as_object) else {
            continue;
        };
        let item_name = item
            .get("name")
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                inner
                    .get("use")
                    .and_then(serde_json::Value::as_object)
                    .and_then(|use_item| use_item.get("name"))
                    .and_then(serde_json::Value::as_str)
            });
        let module_path = item_name.map(|name| match parent_path.as_deref() {
            Some(parent) if !parent.is_empty() => format!("{parent}::{name}"),
            _ => name.to_owned(),
        });
        let child_path = module_path.as_deref().or(parent_path.as_deref());
        if let Some(module) = inner.get("module").and_then(serde_json::Value::as_object) {
            if let Some(children) = module.get("items").and_then(serde_json::Value::as_array) {
                enqueue_rustdoc_ids(children, &mut pending, false, child_path);
            }
        }
        if let Some(use_item) = inner.get("use").and_then(serde_json::Value::as_object) {
            if let Some(target_id) = use_item.get("id").and_then(serde_json::Value::as_u64) {
                let target = target_id.to_string();
                if index.contains_key(&target) {
                    pending.push((target, false, parent_path.clone()));
                }
            }
        }
        if let Some(struct_item) = inner.get("struct").and_then(serde_json::Value::as_object) {
            if let Some(kind) = struct_item
                .get("kind")
                .and_then(serde_json::Value::as_object)
            {
                for fields in kind.values() {
                    if let Some(fields) = fields.get("fields").and_then(serde_json::Value::as_array)
                    {
                        enqueue_rustdoc_ids(fields, &mut pending, true, child_path);
                    }
                }
            }
            if let Some(impls) = struct_item
                .get("impls")
                .and_then(serde_json::Value::as_array)
            {
                enqueue_rustdoc_ids(impls, &mut pending, false, child_path);
            }
        }
        if let Some(enum_item) = inner.get("enum").and_then(serde_json::Value::as_object) {
            if let Some(variants) = enum_item
                .get("variants")
                .and_then(serde_json::Value::as_array)
            {
                enqueue_rustdoc_ids(variants, &mut pending, true, child_path);
            }
            if let Some(impls) = enum_item.get("impls").and_then(serde_json::Value::as_array) {
                enqueue_rustdoc_ids(impls, &mut pending, false, child_path);
            }
        }
        if let Some(union_item) = inner.get("union").and_then(serde_json::Value::as_object) {
            if let Some(fields) = union_item
                .get("fields")
                .and_then(serde_json::Value::as_array)
            {
                enqueue_rustdoc_ids(fields, &mut pending, true, child_path);
            }
            if let Some(impls) = union_item
                .get("impls")
                .and_then(serde_json::Value::as_array)
            {
                enqueue_rustdoc_ids(impls, &mut pending, false, child_path);
            }
        }
        if let Some(trait_item) = inner.get("trait").and_then(serde_json::Value::as_object) {
            if let Some(trait_items) = trait_item
                .get("items")
                .and_then(serde_json::Value::as_array)
            {
                enqueue_rustdoc_ids(trait_items, &mut pending, true, child_path);
            }
        }
        if let Some(impl_item) = inner.get("impl").and_then(serde_json::Value::as_object) {
            if let Some(impl_items) = impl_item.get("items").and_then(serde_json::Value::as_array) {
                enqueue_rustdoc_ids(impl_items, &mut pending, false, child_path);
            }
        }
        if (visibility != Some("public") && !inherited_public) || id == root_id.to_string() {
            continue;
        }
        let Some((kind, _)) = inner.iter().next() else {
            continue;
        };
        // Rustdoc emits public re-export items whose top-level `name` is
        // null when the target lives in an external crate.  The compiler
        // still records the public alias in `inner.use.name`; retain that
        // alias as a graph item instead of dropping the umbrella crate's
        // complete public surface merely because the external target is not
        // repeated in this crate's index.
        let name = item_name;
        let Some(name) = name else {
            continue;
        };
        let reexport = inner
            .get("use")
            .and_then(serde_json::Value::as_object)
            .and_then(|use_item| external_reexport_target(use_item, index));
        let span = item.get("span").and_then(serde_json::Value::as_object);
        let raw_source_path = span
            .and_then(|span| span.get("filename"))
            .and_then(serde_json::Value::as_str)
            .map(|path| path.replace('\\', "/"));
        let source_path = raw_source_path
            .as_deref()
            .and_then(|path| {
                let normalized_crate = relative_path(repository_root, crate_dir);
                let normalized = normalize_relative_path(path.trim_start_matches("./"));
                if normalized.starts_with(&format!("{normalized_crate}/"))
                    || normalized == normalized_crate
                {
                    return Some(normalized.to_owned());
                }
                let root = repository_root.to_string_lossy().replace('\\', "/");
                let from_root = normalized
                    .strip_prefix(&format!("{root}/"))
                    .unwrap_or(normalized.as_str());
                (from_root.starts_with(&format!("{normalized_crate}/"))
                    || from_root == normalized_crate)
                    .then(|| from_root.to_owned())
            })
            .or_else(|| {
                raw_source_path
                    .as_deref()
                    .and_then(|path| generated_source_aliases.get(&path_identity(path)))
                    .cloned()
            });
        let source_line = span
            .and_then(|span| span.get("begin"))
            .and_then(serde_json::Value::as_array)
            .and_then(|begin| begin.first())
            .and_then(serde_json::Value::as_u64)
            .and_then(|line| usize::try_from(line).ok());
        // Rustdoc leaves documentation on the declaration targeted by an
        // in-crate `pub use` when the re-export itself has no doc comment.
        // The public API surface still needs the declaration's Rust-authored
        // explanation, so inherit it from the resolved target while keeping
        // an explicit alias comment authoritative.
        let docs = item
            .get("docs")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                inner
                    .get("use")
                    .and_then(serde_json::Value::as_object)
                    .and_then(|use_item| use_item.get("id"))
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|target_id| index.get(&target_id.to_string()))
                    .and_then(|target| target.get("docs"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            });
        let conditional = item
            .get("attrs")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|attrs| {
                attrs
                    .iter()
                    .any(|attr| attr.as_str().is_some_and(|attr| attr.contains("cfg")))
            });
        let generated = source_path
            .as_deref()
            .is_some_and(|path| path.split('/').any(|part| part == "generated"));
        if require_retained_source && source_path.is_none() {
            diagnostics.push(Diagnostic {
                severity: "warning".to_owned(),
                code: "rustdoc_external_source_skipped".to_owned(),
                message: format!(
                    "skipped compiler item {name} because its declaration source is outside the retained crate closure"
                ),
                path: raw_source_path,
                line: source_line,
            });
            continue;
        }
        items.push(PublicItem {
            name: name.to_owned(),
            module_path,
            kind: kind.replace('_', "-"),
            signature: rustdoc_signature(inner),
            signature_text: rustdoc_signature_text(name, kind, inner, index),
            source_path,
            source_line,
            docs,
            conditional,
            generated,
            reexport,
        });
    }
    items
}

fn external_reexport_target(
    use_item: &serde_json::Map<String, serde_json::Value>,
    index: &serde_json::Map<String, serde_json::Value>,
) -> Option<ReexportTarget> {
    // A target id present in this index is an in-crate declaration and is
    // already traversed above.  A missing target id is how rustdoc represents
    // a re-export whose definition belongs to another crate graph.  Keep the
    // original path so a renderer can resolve the item in that graph.
    let target_id = use_item.get("id").and_then(serde_json::Value::as_u64);
    if target_id.is_some_and(|id| index.contains_key(&id.to_string())) {
        return None;
    }
    let source = use_item
        .get("source")
        .and_then(serde_json::Value::as_str)?
        .trim();
    let mut segments = source.split("::");
    let package = segments.next()?.trim();
    if package.is_empty() {
        return None;
    }
    let path = segments.collect::<Vec<_>>().join("::");
    Some(ReexportTarget {
        package: package.replace('_', "-"),
        path: (!path.is_empty()).then_some(path),
        source: source.to_owned(),
    })
}

fn rustdoc_signature(
    inner: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    let (kind, value) = inner.iter().next()?;
    let signature = match kind.as_str() {
        // Keep the complete compiler-provided shape for declarations whose
        // value carries the semantic type/expression/signature. In rustdoc
        // JSON constants are keyed as `constant` (associated constants use
        // `assoc_const`), so treating only `const` as a signature silently
        // drops the type of every module-level constant.
        "function" | "assoc_const" | "assoc_type" | "constant" | "static" | "macro" | "variant"
        | "struct_field" => value.clone(),
        // Modules are namespaces rather than callable declarations, but the
        // strict graph validator still requires a non-null semantic payload
        // for every compiler item. Keep a small stable marker instead of
        // copying the module's child index into every projection.
        "module" => serde_json::json!({"kind": "module"}),
        "struct" | "enum" | "union" | "trait" | "type" => {
            let object = value.as_object()?;
            let mut selected = serde_json::Map::new();
            for key in ["kind", "generics", "bounds", "is_auto", "is_unsafe"] {
                if let Some(value) = object.get(key) {
                    selected.insert(key.to_owned(), value.clone());
                }
            }
            serde_json::Value::Object(selected)
        }
        "type_alias" => value.clone(),
        // Keep the compiler's alias/source identity for public `use` items.
        // Without this, an otherwise resolvable re-export is emitted with no
        // semantic payload and cannot satisfy strict graph qualification.
        "use" => value.clone(),
        _ => return None,
    };
    Some(signature)
}

fn rustdoc_signature_text(
    name: &str,
    kind: &str,
    inner: &serde_json::Map<String, serde_json::Value>,
    index: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    let value = inner.get(kind)?;
    let object = value.as_object();
    let generics = object
        .and_then(|object| object.get("generics"))
        .map(rustdoc_generics_text)
        .unwrap_or_default();
    let where_clause = object
        .and_then(|object| object.get("generics"))
        .map(rustdoc_where_clause_text)
        .unwrap_or_default();
    let text = match kind {
        "type_alias" => format!(
            "pub type {name}{generics}{where_clause} = {}",
            object
                .and_then(|object| object.get("type"))
                .map(rustdoc_type_text)
                .unwrap_or_else(|| "_".to_owned())
        ),
        "function" => {
            let sig = object?.get("sig")?.as_object()?;
            let header = object?.get("header").and_then(serde_json::Value::as_object);
            let mut prefix = String::from("pub ");
            if header
                .and_then(|header| header.get("is_const"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                prefix.push_str("const ");
            }
            if header
                .and_then(|header| header.get("is_unsafe"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                prefix.push_str("unsafe ");
            }
            if header
                .and_then(|header| header.get("is_async"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                prefix.push_str("async ");
            }
            let inputs = sig
                .get("inputs")
                .and_then(serde_json::Value::as_array)
                .map(|inputs| {
                    inputs
                        .iter()
                        .filter_map(|input| {
                            let pair = input.as_array()?;
                            Some(format!(
                                "{}: {}",
                                pair.first()?.as_str()?,
                                pair.get(1).map(rustdoc_type_text)?
                            ))
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let output = sig
                .get("output")
                .filter(|output| !output.is_null())
                .map(|output| format!(" -> {}", rustdoc_type_text(output)))
                .unwrap_or_default();
            format!("{prefix}fn {name}{generics}({inputs}){output}{where_clause}")
        }
        "struct" => format!(
            "pub struct {name}{generics}{where_clause}{}",
            object
                .and_then(|object| object.get("kind"))
                .map(|kind| rustdoc_struct_fields_text(kind, index))
                .unwrap_or_default()
        ),
        "enum" => format!(
            "pub enum {name}{generics}{where_clause}{}",
            object
                .and_then(|object| object.get("variants"))
                .map(|variants| rustdoc_variants_text(variants, index))
                .unwrap_or_default()
        ),
        "union" => format!(
            "pub union {name}{generics}{where_clause}{}",
            object
                .and_then(|object| object.get("fields"))
                .map(|fields| rustdoc_fields_text(fields, index))
                .unwrap_or_default()
        ),
        "trait" => format!(
            "pub trait {name}{generics}{}{where_clause}",
            object
                .and_then(|object| object.get("bounds"))
                .map(rustdoc_bounds_text)
                .filter(|bounds| !bounds.is_empty())
                .map(|bounds| format!(": {bounds}"))
                .unwrap_or_default()
        ),
        "constant" | "assoc_const" => format!(
            "pub const {name}: {}{where_clause}",
            object
                .and_then(|object| object.get("type"))
                .map(rustdoc_type_text)
                .unwrap_or_else(|| "_".to_owned())
        ),
        "assoc_type" => format!(
            "type {name}{}{}{}",
            object
                .and_then(|object| object.get("bounds"))
                .map(rustdoc_bounds_text)
                .filter(|bounds| !bounds.is_empty())
                .map(|bounds| format!(": {bounds}"))
                .unwrap_or_default(),
            object
                .and_then(|object| object.get("type"))
                .map(|value| format!(" = {}", rustdoc_type_text(value)))
                .unwrap_or_default(),
            where_clause
        ),
        "static" => format!(
            "pub static {name}: {}",
            object
                .and_then(|object| object.get("type"))
                .map(rustdoc_type_text)
                .unwrap_or_else(|| "_".to_owned())
        ),
        "macro" => format!("macro {name}!"),
        "module" => format!("pub mod {name}"),
        "variant" => format!(
            "{name}{}",
            object
                .and_then(|object| object.get("kind"))
                .map(|kind| rustdoc_struct_fields_text(kind, index))
                .unwrap_or_default()
        ),
        "struct_field" => format!(
            "{name}: {}",
            object
                .and_then(|object| object.get("type"))
                .map(rustdoc_type_text)
                .unwrap_or_else(|| rustdoc_type_text(value))
        ),
        "use" => format!(
            "pub use {}",
            object
                .and_then(|object| object.get("source"))
                .and_then(serde_json::Value::as_str)
                .filter(|source| !source.trim().is_empty())
                .unwrap_or(name)
        ),
        _ => format!("pub {kind} {name}{generics}"),
    };
    Some(text)
}

fn rustdoc_struct_fields_text(
    value: &serde_json::Value,
    index: &serde_json::Map<String, serde_json::Value>,
) -> String {
    let Some(kind) = value.as_object().and_then(|value| value.values().next()) else {
        return String::new();
    };
    let Some(fields) = kind.get("fields") else {
        return String::new();
    };
    rustdoc_fields_text(fields, index)
}

fn rustdoc_fields_text(
    value: &serde_json::Value,
    index: &serde_json::Map<String, serde_json::Value>,
) -> String {
    let Some(fields) = value.as_array() else {
        return String::new();
    };
    let values = fields
        .iter()
        .filter_map(|field| {
            let id = field.as_u64()?.to_string();
            let item = index.get(&id)?;
            let name = item
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("_");
            let field_type = item
                .get("inner")
                .and_then(serde_json::Value::as_object)
                .and_then(|inner| inner.get("struct_field"))
                .map(rustdoc_type_text)
                .unwrap_or_else(|| "_".to_owned());
            Some(if name == "_" {
                field_type
            } else {
                format!("{name}: {field_type}")
            })
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        String::new()
    } else {
        format!(" {{ {} }}", values.join(", "))
    }
}

fn rustdoc_variants_text(
    value: &serde_json::Value,
    index: &serde_json::Map<String, serde_json::Value>,
) -> String {
    let Some(variants) = value.as_array() else {
        return String::new();
    };
    let values = variants
        .iter()
        .filter_map(|variant| {
            let id = variant.as_u64()?.to_string();
            let item = index.get(&id)?;
            let name = item.get("name").and_then(serde_json::Value::as_str)?;
            let fields = item
                .get("inner")
                .and_then(serde_json::Value::as_object)
                .and_then(|inner| inner.get("variant"))
                .and_then(|variant| variant.get("kind"))
                .map(|kind| rustdoc_struct_fields_text(kind, index))
                .unwrap_or_default();
            Some(format!("{name}{fields}"))
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        String::new()
    } else {
        format!(" {{ {} }}", values.join(", "))
    }
}

fn rustdoc_generics_text(value: &serde_json::Value) -> String {
    let Some(params) = value.get("params").and_then(serde_json::Value::as_array) else {
        return String::new();
    };
    let values = params
        .iter()
        .filter_map(|param| {
            let object = param.as_object()?;
            let name = object.get("name")?.as_str()?;
            let kind = object.get("kind")?.as_object()?;
            if let Some(constant) = kind.get("const").and_then(serde_json::Value::as_object) {
                let ty = constant
                    .get("type")
                    .map(rustdoc_type_text)
                    .unwrap_or_else(|| "_".to_owned());
                return Some(format!("const {name}: {ty}"));
            }
            if kind.contains_key("lifetime") || name.starts_with('\'') {
                return Some(name.to_owned());
            }
            if kind
                .get("type")
                .and_then(|kind| kind.get("is_synthetic"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                return None;
            }
            let bounds = kind
                .get("type")
                .and_then(|kind| kind.get("bounds"))
                .map(rustdoc_bounds_text)
                .filter(|bounds| !bounds.is_empty())
                .map(|bounds| format!(": {bounds}"))
                .unwrap_or_default();
            let default = kind
                .get("type")
                .and_then(|kind| kind.get("default"))
                .map(rustdoc_type_text)
                .filter(|default| default != "_")
                .map(|default| format!(" = {default}"))
                .unwrap_or_default();
            Some(format!("{name}{bounds}{default}"))
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        String::new()
    } else {
        format!("<{}>", values.join(", "))
    }
}

fn rustdoc_bounds_text(value: &serde_json::Value) -> String {
    let Some(bounds) = value.as_array() else {
        return String::new();
    };
    bounds
        .iter()
        .filter_map(|bound| {
            let object = bound.as_object()?;
            if let Some(trait_bound) = object.get("trait_bound") {
                let trait_bound = trait_bound.as_object()?;
                let mut text = trait_bound
                    .get("trait")
                    .map(rustdoc_trait_text)
                    .unwrap_or_else(|| "Trait".to_owned());
                if trait_bound
                    .get("modifier")
                    .and_then(serde_json::Value::as_str)
                    == Some("maybe")
                {
                    text = format!("?{text}");
                }
                return Some(text);
            }
            object
                .get("outlives")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect::<Vec<_>>()
        .join(" + ")
}

fn rustdoc_where_clause_text(value: &serde_json::Value) -> String {
    let Some(predicates) = value
        .get("where_predicates")
        .and_then(serde_json::Value::as_array)
    else {
        return String::new();
    };
    let values = predicates
        .iter()
        .filter_map(|predicate| {
            let bound = predicate.get("bound_predicate")?.as_object()?;
            let subject = bound.get("type").map(rustdoc_type_text)?;
            let bounds = bound.get("bounds").map(rustdoc_bounds_text)?;
            (!bounds.is_empty()).then(|| format!("{subject}: {bounds}"))
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        String::new()
    } else {
        format!(" where {}", values.join(", "))
    }
}

fn rustdoc_type_text(value: &serde_json::Value) -> String {
    let Some(object) = value.as_object() else {
        return "_".to_owned();
    };
    let Some((kind, inner)) = object.iter().next() else {
        return "_".to_owned();
    };
    match kind.as_str() {
        "primitive" | "generic" => inner.as_str().unwrap_or("_").to_owned(),
        "resolved_path" => {
            let Some(inner) = inner.as_object() else {
                return "_".to_owned();
            };
            let path = inner
                .get("path")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("_");
            let args = inner
                .get("args")
                .map(rustdoc_generic_args_text)
                .unwrap_or_default();
            format!("{path}{args}")
        }
        "borrowed_ref" => {
            let Some(inner) = inner.as_object() else {
                return "_".to_owned();
            };
            let lifetime = inner
                .get("lifetime")
                .and_then(serde_json::Value::as_str)
                .map(|value| format!("{value} "))
                .unwrap_or_default();
            let mutable = inner
                .get("is_mutable")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let target = inner
                .get("type")
                .map(rustdoc_type_text)
                .unwrap_or_else(|| "_".to_owned());
            format!("&{lifetime}{}{target}", if mutable { "mut " } else { "" })
        }
        "raw_pointer" => {
            let Some(inner) = inner.as_object() else {
                return "_".to_owned();
            };
            let mutable = inner
                .get("is_mutable")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let target = inner
                .get("type")
                .map(rustdoc_type_text)
                .unwrap_or_else(|| "_".to_owned());
            format!("*{}{}", if mutable { "mut " } else { "const " }, target)
        }
        "slice" => format!(
            "[{}]",
            inner
                .get("type")
                .map(rustdoc_type_text)
                .unwrap_or_else(|| "_".to_owned())
        ),
        "array" => {
            let target = inner
                .get("type")
                .map(rustdoc_type_text)
                .unwrap_or_else(|| "_".to_owned());
            let len = inner
                .get("len")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("_");
            format!("[{target}; {len}]")
        }
        "tuple" => inner
            .as_array()
            .map(|items| {
                format!(
                    "({})",
                    items
                        .iter()
                        .map(rustdoc_type_text)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .unwrap_or_else(|| "()".to_owned()),
        "function_pointer" => "fn(...)".to_owned(),
        "impl_trait" => format!("impl {}", rustdoc_bounds_text(inner)),
        "infer" => "_".to_owned(),
        "qualified_path" => {
            let name = inner
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("_");
            let self_type = inner
                .get("self_type")
                .map(rustdoc_type_text)
                .unwrap_or_else(|| "Self".to_owned());
            let trait_name = inner
                .get("trait")
                .map(rustdoc_trait_text)
                .unwrap_or_else(|| "Trait".to_owned());
            if trait_name.is_empty() {
                format!("{self_type}::{name}")
            } else {
                format!("<{self_type} as {trait_name}>::{name}")
            }
        }
        "dyn_trait" => {
            let traits = inner
                .get("traits")
                .and_then(serde_json::Value::as_array)
                .map(|traits| {
                    traits
                        .iter()
                        .filter_map(|bound| bound.get("trait").map(rustdoc_trait_text))
                        .collect::<Vec<_>>()
                        .join(" + ")
                })
                .filter(|traits| !traits.is_empty())
                .unwrap_or_else(|| "Trait".to_owned());
            let lifetime = inner
                .get("lifetime")
                .and_then(serde_json::Value::as_str)
                .map(|lifetime| format!(" + {lifetime}"))
                .unwrap_or_default();
            format!("dyn {traits}{lifetime}")
        }
        _ => serde_json::to_string(value).unwrap_or_else(|_| "_".to_owned()),
    }
}

fn rustdoc_trait_text(value: &serde_json::Value) -> String {
    let Some(object) = value.as_object() else {
        return "Trait".to_owned();
    };
    let path = object
        .get("path")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Trait");
    let args = object
        .get("args")
        .map(rustdoc_generic_args_text)
        .unwrap_or_default();
    format!("{path}{args}")
}

fn rustdoc_generic_args_text(value: &serde_json::Value) -> String {
    let Some(angle) = value.get("angle_bracketed") else {
        return String::new();
    };
    let Some(args) = angle.get("args").and_then(serde_json::Value::as_array) else {
        return String::new();
    };
    let mut values = args
        .iter()
        .filter_map(|arg| {
            let object = arg.as_object()?;
            if let Some(value) = object.get("type") {
                return Some(rustdoc_type_text(value));
            }
            object
                .get("lifetime")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    if let Some(constraints) = angle
        .get("constraints")
        .and_then(serde_json::Value::as_array)
    {
        values.extend(constraints.iter().filter_map(|constraint| {
            let object = constraint.as_object()?;
            let name = object.get("name")?.as_str()?;
            let equality = object
                .get("binding")?
                .get("equality")?
                .get("type")
                .map(rustdoc_type_text)?;
            Some(format!("{name} = {equality}"))
        }));
    }
    if values.is_empty() {
        String::new()
    } else {
        format!("<{}>", values.join(", "))
    }
}

fn enqueue_rustdoc_ids(
    values: &[serde_json::Value],
    pending: &mut Vec<(String, bool, Option<String>)>,
    inherited_public: bool,
    module_path: Option<&str>,
) {
    pending.extend(values.iter().filter_map(|value| {
        value.as_u64().map(|id| {
            (
                id.to_string(),
                inherited_public,
                module_path.map(str::to_owned),
            )
        })
    }));
}

fn merge_compiler_public_items(graphs: &[RustdocGraph]) -> Vec<PublicItem> {
    let mut items = graphs
        .iter()
        .flat_map(|graph| graph.public_items.iter().cloned())
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        left.source_path
            .cmp(&right.source_path)
            .then(left.source_line.cmp(&right.source_line))
            .then(left.name.cmp(&right.name))
    });
    items.dedup_by(|left, right| {
        left.name == right.name
            && left.kind == right.kind
            && left.source_path == right.source_path
            && left.source_line == right.source_line
    });
    items
}

fn collect_source_files(crate_dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut result = Vec::new();
    collect_files_recursive(crate_dir, &mut result)?;
    result.retain(|path| {
        path.extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| matches!(extension, "rs" | "md"))
    });
    Ok(result)
}

fn collect_files_recursive(directory: &Path, result: &mut Vec<PathBuf>) -> Result<(), Error> {
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.file_name().and_then(|name| name.to_str()) == Some("target") {
            continue;
        }
        if path.is_dir() {
            collect_files_recursive(&path, result)?;
        } else if path.is_file() {
            result.push(path);
        }
    }
    Ok(())
}

fn scan_rust_file(
    contents: &str,
    path: &str,
    items: &mut Vec<PublicItem>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let mut docs = Vec::new();
    let mut conditional = false;
    let mut hidden = false;
    for (index, line) in contents.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if trimmed.starts_with("///") {
            docs.push(trimmed.trim_start_matches("///").trim().to_owned());
            continue;
        }
        if trimmed.starts_with("//!") {
            docs.push(trimmed.trim_start_matches("//!").trim().to_owned());
            continue;
        }
        if trimmed.starts_with("#[cfg") {
            conditional = true;
            continue;
        }
        if trimmed.starts_with("#[doc(hidden") {
            hidden = true;
            continue;
        }
        if let Some((kind, name)) = public_declaration(trimmed) {
            if !hidden {
                items.push(PublicItem {
                    name,
                    module_path: None,
                    kind,
                    signature: None,
                    signature_text: None,
                    source_path: Some(path.to_owned()),
                    source_line: Some(line_number),
                    docs: (!docs.is_empty()).then(|| docs.join("\n")),
                    conditional,
                    generated: path.split('/').any(|part| part == "generated"),
                    reexport: None,
                });
            }
            docs.clear();
            conditional = false;
            hidden = false;
            continue;
        }
        if trimmed.starts_with("pub use ") {
            diagnostics.push(Diagnostic {
                severity: "warning".to_owned(),
                code: "unresolved_re_export".to_owned(),
                message: "`pub use` export requires rustdoc JSON to resolve its public target"
                    .to_owned(),
                path: Some(path.to_owned()),
                line: Some(line_number),
            });
        }
        if !trimmed.is_empty() && !trimmed.starts_with("#") {
            docs.clear();
            conditional = false;
            hidden = false;
        }
    }
}

fn public_declaration(line: &str) -> Option<(String, String)> {
    let declaration = line.strip_prefix("pub ")?;
    let mut words = declaration.split_whitespace();
    let mut kind = words.next()?;
    if kind == "unsafe" || kind == "async" {
        kind = words.next()?;
    }
    if kind == "extern" {
        kind = words.nth(1)?;
    }
    if !matches!(
        kind,
        "struct" | "enum" | "trait" | "fn" | "type" | "const" | "static" | "mod" | "macro"
    ) {
        return None;
    }
    let name = words
        .next()?
        .trim_matches(|character: char| !character.is_ascii_alphanumeric() && character != '_');
    (!name.is_empty()).then(|| (kind.to_owned(), name.to_owned()))
}

fn manifest_value(manifest: &str, key: &str) -> Option<String> {
    let direct_key = key.rsplit('.').next().unwrap_or(key);
    manifest.lines().find_map(|line| {
        let (line_key, value) = line.split_once('=')?;
        if line_key.trim() != direct_key {
            return None;
        }
        let value = value.trim().trim_matches('"');
        (!value.is_empty()).then(|| value.to_owned())
    })
}

fn workspace_version(repository_root: &Path) -> Option<String> {
    let manifest = fs::read_to_string(repository_root.join("Cargo.toml")).ok()?;
    let mut in_workspace_package = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_workspace_package = trimmed == "[workspace.package]";
            continue;
        }
        if in_workspace_package {
            let Some((key, value)) = trimmed.split_once('=') else {
                continue;
            };
            if key.trim() == "version" {
                return Some(value.trim().trim_matches('"').to_owned());
            }
        }
    }
    None
}

fn normalized_features(features: &[String]) -> Vec<String> {
    let mut normalized = features
        .iter()
        .filter(|feature| feature.as_str() != "default")
        .cloned()
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn profile_blake3(profile: &AnalysisProfile) -> String {
    let mut packages = profile
        .packages
        .iter()
        .map(|package| {
            serde_json::json!({
                "package": package.package,
                "target": package.target,
                "features": normalized_features(&package.features),
                "defaultFeatures": package.default_features,
            })
        })
        .collect::<Vec<_>>();
    packages.sort_by_key(|package| package["package"].as_str().unwrap_or_default().to_owned());
    digest_bytes(
        canonical_json(&serde_json::json!({
            "name": profile.name,
            "packages": packages,
            "scope": profile.scope.as_ref(),
        }))
        .as_bytes(),
    )
}

fn rust_toolchain(root: &Path) -> Option<String> {
    let contents = fs::read_to_string(root.join("rust-toolchain.toml")).ok()?;
    contents.lines().find_map(|line| {
        let (key, value) = line.split_once('=')?;
        (key.trim() == "channel").then(|| value.trim().trim_matches('"').to_owned())
    })
}

fn find_rustdoc_json(root: &Path, package_name: &str) -> Option<PathBuf> {
    if root.is_file() {
        return root
            .extension()
            .and_then(|extension| (extension == "json").then(|| root.to_owned()));
    }
    let mut candidates = Vec::new();
    collect_json_recursive(root, &mut candidates).ok()?;
    candidates.sort();
    let normalized = package_name.replace('-', "_");
    candidates.into_iter().find(|path| {
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .is_some_and(|stem| stem == normalized)
    })
}

fn find_rustdoc_json_all(root: &Path, package_name: &str) -> Vec<PathBuf> {
    if root.is_file() {
        return root
            .extension()
            .and_then(|extension| (extension == "json").then(|| vec![root.to_owned()]))
            .unwrap_or_default();
    }
    let normalized = package_name.replace('-', "_");
    let mut candidates = Vec::new();
    if collect_json_recursive(root, &mut candidates).is_err() {
        return Vec::new();
    }
    candidates.sort();
    candidates
        .into_iter()
        .filter(|path| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .is_some_and(|stem| stem == normalized)
        })
        .collect()
}

fn collect_json_recursive(directory: &Path, result: &mut Vec<PathBuf>) -> Result<(), Error> {
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.file_name().and_then(|name| name.to_str()) == Some(".cargo-target") {
            continue;
        }
        if path.is_dir() {
            collect_json_recursive(&path, result)?;
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("json")
            && !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".receipt.json"))
        {
            result.push(path);
        }
    }
    Ok(())
}

fn git_revision(root: &Path) -> Option<String> {
    git_checkout_root(root)?;
    let output = git_command()
        .args(["-C", root.to_str()?, "rev-parse", "HEAD"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Return a Git checkout root only when Git's resolved top-level directory is
/// exactly the requested source root. `git -C` otherwise walks up through
/// parent directories, which would let an extracted source archive inherit an
/// unrelated parent checkout revision.
fn git_checkout_root(root: &Path) -> Option<PathBuf> {
    let requested = fs::canonicalize(root).ok()?;
    let output = git_command()
        .args(["-C", root.to_str()?, "rev-parse", "--show-toplevel"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let resolved = fs::canonicalize(String::from_utf8_lossy(&output.stdout).trim()).ok()?;
    (resolved == requested).then_some(resolved)
}

fn verify_release_qualification(
    manifest_path: &Path,
    repository_root: &Path,
    source_revision: &str,
    dirty_worktree: bool,
) -> Result<ReleaseQualification, Error> {
    let manifest: serde_json::Value = serde_json::from_slice(&fs::read(manifest_path)?)?;
    let schema = manifest
        .get("schema")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::Strict("release qualification has no schema".to_owned()))?;
    if schema != "acyclic.sdk.docs.release-qualification.v1" {
        return Err(Error::Strict(format!(
            "unsupported release qualification schema: {schema}"
        )));
    }
    let version = manifest
        .get("version")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::Strict("release qualification has no version".to_owned()))?;
    let tag = manifest
        .get("tag")
        .and_then(serde_json::Value::as_str)
        .filter(|value| {
            !value.trim().is_empty()
                && value
                    .chars()
                    .all(|character| !character.is_control() && character != '\\')
        })
        .ok_or_else(|| Error::Strict("release qualification has no valid Git tag".to_owned()))?;
    validate_release_tag_version(tag, version)?;
    let revision = manifest
        .get("revision")
        .and_then(serde_json::Value::as_str)
        .filter(|value| value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| {
            Error::Strict("release qualification has no full Git revision".to_owned())
        })?;
    if manifest
        .get("qualified")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        return Err(Error::Strict(
            "release qualification must explicitly set qualified=true".to_owned(),
        ));
    }
    if dirty_worktree {
        return Err(Error::Strict(
            "release qualification requires a clean Git worktree".to_owned(),
        ));
    }
    if source_revision != revision {
        return Err(Error::Strict(format!(
            "release qualification revision mismatch: expected {source_revision}, got {revision}"
        )));
    }
    let head = git_revision(repository_root).ok_or_else(|| {
        Error::Strict("release qualification requires a Git HEAD revision".to_owned())
    })?;
    if head != revision {
        return Err(Error::Strict(format!(
            "release qualification HEAD mismatch: expected {head}, got {revision}"
        )));
    }
    let tag_ref = format!("refs/tags/{tag}^{{commit}}");
    let output = git_command()
        .args([
            "-C",
            repository_root.to_str().unwrap_or_default(),
            "rev-parse",
            "--verify",
        ])
        .arg(&tag_ref)
        .output()
        .map_err(Error::Io)?;
    if !output.status.success() {
        return Err(Error::Strict(format!(
            "release qualification Git tag cannot be resolved: {tag}"
        )));
    }
    let tagged_revision = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if tagged_revision != revision {
        return Err(Error::Strict(format!(
            "release qualification tag {tag} resolves to {tagged_revision}, expected {revision}"
        )));
    }
    Ok(ReleaseQualification {
        schema: schema.to_owned(),
        version: version.to_owned(),
        tag: tag.to_owned(),
        revision: revision.to_owned(),
        qualified: true,
    })
}

fn validate_release_tag_version(tag: &str, version: &str) -> Result<(), Error> {
    // Repository and published family releases use an explicit, finite tag
    // namespace. Keep this allowlist closed so an arbitrary branch-like tag
    // cannot qualify a historical archive by merely ending in a version.
    let scope = [
        "acyclic-v",
        "cargo-v",
        "npm-v",
        "fs-v",
        "filesystem-v",
        "harness-v",
        "inference-v",
        "machines-v",
        "objects-v",
        "stream-v",
        "typescript-v",
    ]
    .into_iter()
    .find(|prefix| tag.starts_with(prefix))
    .map(|prefix| &tag[prefix.len()..]);
    let publish_scope = [
        "publish/acyclic-fs/",
        "publish/acyclic-inference/",
        "publish/acyclic-machines/",
        "publish/acyclic-objects/",
        "publish/acyclic-stream/",
        "publish/acyclic-harness/",
    ]
    .into_iter()
    .find_map(|prefix| tag.strip_prefix(prefix));
    let tagged_version = scope.or(publish_scope).ok_or_else(|| {
        Error::Strict(format!(
            "release qualification tag has no supported release identity scope: {tag}"
        ))
    })?;
    if !valid_release_version(version) {
        return Err(Error::Strict(format!(
            "release qualification version is not a supported release version: {version}"
        )));
    }
    if tagged_version != version {
        return Err(Error::Strict(format!(
            "release qualification tag/version mismatch: tag {tag} names {tagged_version}, manifest names {version}"
        )));
    }
    Ok(())
}

fn valid_release_version(version: &str) -> bool {
    let split = version
        .find(|character| character == '-' || character == '+')
        .unwrap_or(version.len());
    let core = &version[..split];
    let suffix = &version[split..];
    let core_valid = core.split('.').count() == 3
        && core
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()));
    let suffix_valid = suffix.is_empty()
        || (suffix.len() > 1
            && suffix[1..]
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-'));
    core_valid && suffix_valid
}

fn git_worktree_dirty(root: &Path) -> bool {
    if git_checkout_root(root).is_none() {
        return false;
    }
    git_command()
        .args([
            "-C",
            root.to_str().unwrap_or_default(),
            "status",
            "--porcelain",
        ])
        .output()
        .is_ok_and(|output| output.status.success() && !output.stdout.is_empty())
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn normalize_relative_path(path: &str) -> String {
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            component => parts.push(component),
        }
    }
    parts.join("/")
}

fn digest_bytes(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn sha256_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(7 + digest.len() * 2);
    encoded.push_str("sha256:");
    for byte in digest {
        write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn canonical_json(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry_fixture(name: &str, package: &str, version: &str, status: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("sdk-docs-registry-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("src")).expect("create registry fixture");
        fs::write(
            root.join("Cargo.toml"),
            format!("[package]\nname = \"{package}\"\nversion = \"{version}\"\n"),
        )
        .expect("write manifest");
        fs::write(
            root.join("src/lib.rs"),
            "//! Fixture crate.\npub struct Item;\n",
        )
        .expect("write source");
        let manifest = serde_json::json!({
            "schema_version": 1,
            "entries": [{
                "ecosystem": "cargo",
                "registry": "crates.io",
                "package": package,
                "version": version,
                "status": status,
                "sha256": if status == "published" {
                    Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
                } else {
                    None
                }
            }]
        });
        fs::write(
            root.join("registry.json"),
            serde_json::to_vec_pretty(&manifest).expect("serialize registry fixture"),
        )
        .expect("write registry manifest");
        root
    }

    #[test]
    fn registry_manifest_rejects_published_without_checksum() {
        let path = std::env::temp_dir().join(format!(
            "sdk-docs-registry-invalid-{}.json",
            std::process::id()
        ));
        fs::write(
            &path,
            r#"{"schema_version":1,"entries":[{"ecosystem":"cargo","registry":"crates.io","package":"demo","version":"0.1.0","status":"published"}]}"#,
        )
        .expect("write invalid registry manifest");
        let error = load_registry_manifest(&path).expect_err("missing checksum must fail");
        assert!(error.to_string().contains("no valid SHA-256"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn scan_crate_uses_verified_registry_install_instruction() {
        let root = registry_fixture("published", "demo-published", "0.1.5", "published");
        let lookup = load_registry_manifest(&root.join("registry.json")).expect("load registry");
        let bundle = scan_crate(
            &root,
            &root,
            None,
            false,
            "release-revision",
            None,
            "release",
            false,
            Some(&lookup),
        )
        .expect("scan fixture");
        assert_eq!(bundle.availability, "registry-verified");
        assert_eq!(
            bundle.package_instructions[0].command,
            "cargo add demo-published@=0.1.5"
        );
        assert_eq!(
            bundle.package_instructions[0]
                .registry
                .as_ref()
                .and_then(|entry| entry.sha256.as_deref()),
            Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scan_crate_preserves_source_install_for_unavailable_registry_package() {
        let root = registry_fixture("unavailable", "demo-unavailable", "0.1.4", "unavailable");
        let lookup = load_registry_manifest(&root.join("registry.json")).expect("load registry");
        let bundle = scan_crate(
            &root,
            &root,
            None,
            false,
            "release-revision",
            None,
            "release",
            false,
            Some(&lookup),
        )
        .expect("scan fixture");
        assert_eq!(bundle.availability, "registry-unverified");
        assert_eq!(
            bundle.package_instructions[0].command,
            "cargo add demo-unavailable --git https://github.com/acyclic-labs/sdk --rev release-revision"
        );
        assert_eq!(
            bundle.package_instructions[0]
                .registry
                .as_ref()
                .map(|entry| entry.status.as_str()),
            Some("unavailable")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn source_parser_preserves_docs_and_marks_cfg() {
        let source = "/// A documented type.\n#[cfg(feature = \"native\")]\npub struct NativeThing;\n\npub use other::Thing;\n";
        let mut items = Vec::new();
        let mut diagnostics = Vec::new();
        scan_rust_file(
            source,
            "rust/crates/demo/src/lib.rs",
            &mut items,
            &mut diagnostics,
        );
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].name, "NativeThing");
        assert!(items[0].conditional);
        assert_eq!(items[0].docs.as_deref(), Some("A documented type."));
        assert_eq!(diagnostics[0].code, "unresolved_re_export");
    }

    #[test]
    fn public_parser_handles_async_and_unsafe_items() {
        assert_eq!(
            public_declaration("pub async fn fetch()"),
            Some(("fn".to_owned(), "fetch".to_owned()))
        );
        assert_eq!(
            public_declaration("pub unsafe fn mount()"),
            Some(("fn".to_owned(), "mount".to_owned()))
        );
        assert_eq!(
            public_declaration("pub struct Workspace;"),
            Some(("struct".to_owned(), "Workspace".to_owned()))
        );
        assert!(public_declaration("fn private()").is_none());
    }

    #[test]
    fn digest_is_stable() {
        assert_eq!(digest_bytes(b"acyclic"), digest_bytes(b"acyclic"));
        assert_ne!(digest_bytes(b"acyclic"), digest_bytes(b"changed"));
    }

    #[test]
    fn rustdoc_fixture_resolves_public_reexport_and_cfg_item() {
        let value = serde_json::json!({
            "format_version": 60,
            "root": 1,
            "index": {
                "1": {
                    "crate_id": 0,
                    "name": "demo",
                    "visibility": "public",
                    "inner": {"module": {"items": [2, 3]}}
                },
                "2": {
                    "crate_id": 0,
                    "name": "Alias",
                    "visibility": "public",
                    "span": {"filename": "rust/crates/demo/src/lib.rs", "begin": [4, 1]},
                    "inner": {"use": {"id": 4, "name": "Alias", "source": "demo::Thing"}}
                },
                "3": {
                    "crate_id": 0,
                    "name": "NativeThing",
                    "visibility": "public",
                    "attrs": ["#[cfg(feature = \"native\")]"] ,
                    "span": {"filename": "rust/crates/demo/src/lib.rs", "begin": [8, 1]},
                    "inner": {"struct": {}}
                },
                "4": {
                    "crate_id": 0,
                    "name": "Thing",
                    "visibility": "private",
                    "docs": "The Rust-authored declaration explanation.",
                    "span": {"filename": "rust/crates/demo/src/lib.rs", "begin": [12, 1]},
                    "inner": {"struct": {}}
                }
            }
        });
        let mut diagnostics = Vec::new();
        let items = rustdoc_public_items(
            &value,
            Path::new("Q:/sdk"),
            Path::new("Q:/sdk/rust/crates/demo"),
            &mut diagnostics,
        );
        assert!(diagnostics.is_empty());
        assert_eq!(items.len(), 2);
        assert!(items.iter().any(|item| item.name == "Alias"
            && item.kind == "use"
            && item.docs.as_deref() == Some("The Rust-authored declaration explanation.")));
        let alias = items.iter().find(|item| item.name == "Alias").unwrap();
        assert_eq!(alias.module_path.as_deref(), Some("demo::Alias"));
        assert!(alias.reexport.is_none());
        assert!(items
            .iter()
            .any(|item| item.name == "NativeThing" && item.conditional));
        assert_eq!(
            items
                .iter()
                .find(|item| item.name == "NativeThing")
                .and_then(|item| item.module_path.as_deref()),
            Some("demo::NativeThing")
        );
    }

    #[test]
    fn rustdoc_fixture_retains_external_reexport_when_item_name_is_null() {
        let value = serde_json::json!({
            "format_version": 60,
            "root": 1,
            "index": {
                "1": {
                    "crate_id": 0,
                    "name": "sdk",
                    "visibility": "public",
                    "inner": {"module": {"items": [2]}}
                },
                "2": {
                    "crate_id": 0,
                    "name": null,
                    "visibility": "public",
                    "span": {"filename": "rust/crates/sdk/src/lib.rs", "begin": [3, 1]},
                    "inner": {
                        "use": {
                            "id": 999,
                            "name": "filesystem",
                            "source": "acyclic_fs"
                        }
                    }
                }
            }
        });
        let mut diagnostics = Vec::new();
        let items = rustdoc_public_items(
            &value,
            Path::new("Q:/sdk"),
            Path::new("Q:/sdk/rust/crates/sdk"),
            &mut diagnostics,
        );
        assert!(diagnostics.is_empty());
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].name, "filesystem");
        assert_eq!(items[0].kind, "use");
        assert_eq!(
            items[0].source_path.as_deref(),
            Some("rust/crates/sdk/src/lib.rs")
        );
        let target = items[0]
            .reexport
            .as_ref()
            .expect("external re-export target should be retained");
        assert_eq!(target.package, "acyclic-fs");
        assert_eq!(target.path, None);
        assert_eq!(target.source, "acyclic_fs");
    }

    #[test]
    fn rustdoc_fixture_resolves_external_reexport_item_path() {
        let value = serde_json::json!({
            "format_version": 60,
            "root": 1,
            "index": {
                "1": {
                    "crate_id": 0,
                    "name": "sdk",
                    "visibility": "public",
                    "inner": {"module": {"items": [2]}}
                },
                "2": {
                    "crate_id": 0,
                    "name": "Filesystem",
                    "visibility": "public",
                    "inner": {
                        "use": {
                            "name": "Filesystem",
                            "source": "acyclic_fs::Filesystem"
                        }
                    }
                }
            }
        });
        let mut diagnostics = Vec::new();
        let items = rustdoc_public_items(
            &value,
            Path::new("Q:/sdk"),
            Path::new("Q:/sdk/rust/crates/sdk"),
            &mut diagnostics,
        );
        assert!(diagnostics.is_empty());
        let target = items[0]
            .reexport
            .as_ref()
            .expect("external re-export item path should be retained");
        assert_eq!(target.package, "acyclic-fs");
        assert_eq!(target.path.as_deref(), Some("Filesystem"));
        assert_eq!(target.source, "acyclic_fs::Filesystem");
    }

    #[test]
    fn rustdoc_generated_spans_bind_retained_bytes_and_skip_external_sources() {
        let root =
            std::env::temp_dir().join(format!("sdk-docs-generated-source-{}", std::process::id()));
        let generated = root.join("target/debug/build/acyclic-stream-test-abc/out/wire.rs");
        fs::create_dir_all(generated.parent().unwrap()).unwrap();
        fs::write(&generated, "pub struct Wire;\n").unwrap();
        let crate_dir = root.join("rust/crates/stream");
        fs::create_dir_all(crate_dir.join("src")).unwrap();
        fs::write(crate_dir.join("src/lib.rs"), "pub struct Local;\n").unwrap();
        let value = serde_json::json!({
            "root": 1,
            "index": {
                "1": {"crate_id": 0, "name": "stream", "visibility": "public", "inner": {"module": {"items": [2, 3]}}},
                "2": {"crate_id": 0, "name": "Wire", "visibility": "public", "span": {"filename": generated.to_string_lossy(), "begin": [1, 1]}, "inner": {"struct": {}}},
                "3": {"crate_id": 0, "name": "External", "visibility": "public", "span": {"filename": "C:/registry/external/src/lib.rs", "begin": [1, 1]}, "inner": {"struct": {}}}
            }
        });
        let mut sources = Vec::new();
        let mut aliases = HashMap::new();
        let mut diagnostics = Vec::new();
        collect_rustdoc_generated_sources(
            &value,
            &root,
            &crate_dir,
            "acyclic-stream",
            "host-default",
            &mut sources,
            &mut aliases,
            &mut diagnostics,
        )
        .unwrap();
        assert!(diagnostics.is_empty());
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0].path,
            "rust/crates/stream/src/generated/host-default/wire.rs"
        );
        assert_eq!(sources[0].contents, "pub struct Wire;\n");
        let items = rustdoc_public_items_with_sources(
            &value,
            &root,
            &crate_dir,
            &aliases,
            true,
            &mut diagnostics,
        );
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].name, "Wire");
        assert_eq!(
            items[0].source_path.as_deref(),
            Some("rust/crates/stream/src/generated/host-default/wire.rs")
        );
        assert!(diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "rustdoc_external_source_skipped"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rustdoc_generated_source_missing_is_reported_once_per_path() {
        let root = std::env::temp_dir().join(format!(
            "sdk-docs-generated-source-missing-{}",
            std::process::id()
        ));
        let crate_dir = root.join("rust/crates/stream");
        fs::create_dir_all(crate_dir.join("src")).unwrap();
        let missing = root.join("target/debug/build/acyclic-stream-test-abc/out/wire.rs");
        let value = serde_json::json!({
            "index": {
                "1": {"crate_id": 0, "span": {"filename": missing.to_string_lossy(), "begin": [1, 1]}, "inner": {"struct": {}}},
                "2": {"crate_id": 0, "span": {"filename": missing.to_string_lossy(), "begin": [2, 1]}, "inner": {"struct": {}}}
            }
        });
        let mut sources = Vec::new();
        let mut aliases = HashMap::new();
        let mut diagnostics = Vec::new();
        collect_rustdoc_generated_sources(
            &value,
            &root,
            &crate_dir,
            "acyclic-stream",
            "host-default",
            &mut sources,
            &mut aliases,
            &mut diagnostics,
        )
        .unwrap();
        assert_eq!(
            diagnostics
                .iter()
                .filter(|diagnostic| diagnostic.code == "rustdoc_generated_source_missing")
                .count(),
            1
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rustdoc_generated_source_basename_collisions_get_stable_suffixes() {
        let root = std::env::temp_dir().join(format!(
            "sdk-docs-generated-source-collision-{}",
            std::process::id()
        ));
        let first = root.join("target/debug/build/acyclic-stream-test-abc/out/wire.rs");
        let second = root.join("target/debug/build/acyclic-stream-test-def/out/wire.rs");
        fs::create_dir_all(first.parent().unwrap()).unwrap();
        fs::create_dir_all(second.parent().unwrap()).unwrap();
        fs::write(&first, "pub struct First;\n").unwrap();
        fs::write(&second, "pub struct Second;\n").unwrap();
        let crate_dir = root.join("rust/crates/stream");
        fs::create_dir_all(crate_dir.join("src")).unwrap();
        let value = serde_json::json!({
            "root": 1,
            "index": {
                "1": {"crate_id": 0, "name": "stream", "visibility": "public", "inner": {"module": {"items": [2, 3]}}},
                "2": {"crate_id": 0, "name": "First", "visibility": "public", "span": {"filename": first.to_string_lossy(), "begin": [1, 1]}, "inner": {"struct": {}}},
                "3": {"crate_id": 0, "name": "Second", "visibility": "public", "span": {"filename": second.to_string_lossy(), "begin": [1, 1]}, "inner": {"struct": {}}}
            }
        });
        let mut sources = Vec::new();
        let mut aliases = HashMap::new();
        let mut diagnostics = Vec::new();
        collect_rustdoc_generated_sources(
            &value,
            &root,
            &crate_dir,
            "acyclic-stream",
            "host-default",
            &mut sources,
            &mut aliases,
            &mut diagnostics,
        )
        .unwrap();
        assert!(diagnostics.is_empty());
        assert_eq!(sources.len(), 2);
        assert!(sources
            .iter()
            .any(|source| source.path.ends_with("/wire.rs")));
        assert!(sources.iter().any(|source| {
            source
                .path
                .starts_with("rust/crates/stream/src/generated/host-default/wire-")
                && source.path.ends_with(".rs")
        }));
        assert!(sources
            .iter()
            .any(|source| source.contents == "pub struct First;\n"));
        assert!(sources
            .iter()
            .any(|source| source.contents == "pub struct Second;\n"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn website_projection_resolves_external_definition_across_crate_graphs() {
        let target_crate = CrateBundle {
            package_name: "acyclic-fs".to_owned(),
            crate_name: Some("acyclic_fs".to_owned()),
            path: "rust/crates/filesystem".to_owned(),
            publish: true,
            version: Some("0.1.0".to_owned()),
            availability: "registry-unverified".to_owned(),
            analysis_mode: "rustdoc-json".to_owned(),
            sources: Vec::new(),
            guides: Vec::new(),
            examples: Vec::new(),
            package_instructions: Vec::new(),
            navigation: "filesystem".to_owned(),
            public_items: vec![PublicItem {
                name: "Filesystem".to_owned(),
                module_path: Some("filesystem::Filesystem".to_owned()),
                kind: "trait".to_owned(),
                signature: Some(serde_json::json!({
                    "generics": {"params": []},
                    "bounds": []
                })),
                signature_text: Some("pub trait Filesystem".to_owned()),
                source_path: Some("rust/crates/filesystem/src/lib.rs".to_owned()),
                source_line: Some(12),
                docs: Some("Filesystem access.".to_owned()),
                conditional: false,
                generated: false,
                reexport: None,
            }],
            graphs: Vec::new(),
            rustdoc: None,
            diagnostics: Vec::new(),
            content_blake3: "content".to_owned(),
            coverage: DocCoverage {
                guides: 0,
                examples: 0,
                public_items: 1,
                documented_items: 1,
                conditional_items: 0,
            },
        };
        let projection = reexport_target_projection(
            &ReexportTarget {
                package: "acyclic-fs".to_owned(),
                path: Some("Filesystem".to_owned()),
                source: "acyclic_fs::Filesystem".to_owned(),
            },
            &[target_crate],
        );
        assert_eq!(projection["family"], "filesystem");
        assert_eq!(projection["definition"]["name"], "Filesystem");
        assert_eq!(projection["definition"]["index"], 0);
        assert_eq!(projection["definition"]["summary"], "Filesystem access.");
        assert_eq!(
            projection["definition"]["sourcePath"],
            "rust/crates/filesystem/src/lib.rs"
        );
        assert_eq!(projection["definition"]["sourceLine"], 12);
        assert_eq!(
            projection["definition"]["modulePath"],
            "filesystem::Filesystem"
        );
        assert_eq!(
            projection["definition"]["signature"]["generics"]["params"],
            serde_json::json!([])
        );
    }

    #[test]
    fn documentation_routes_normalize_historical_package_slugs() {
        let inference = documentation_route_identity("inference-sdk", Some("inference_sdk"));
        assert_eq!(inference.public_slug, "inference");
        assert_eq!(inference.rust_package, "inference-sdk");
        assert_eq!(inference.rust_crate.as_deref(), Some("inference_sdk"));
        assert_eq!(inference.historical_slugs, ["inference-sdk"]);

        let filesystem = documentation_route_identity("acyclic-fs", Some("acyclic_fs"));
        assert_eq!(filesystem.public_slug, "filesystem");
        assert_eq!(filesystem.historical_slugs, ["fs"]);

        let current = documentation_route_identity("acyclic-inference", Some("inference_sdk"));
        assert_eq!(current.public_slug, "inference");
        assert!(current.historical_slugs.is_empty());
    }

    #[test]
    fn website_projection_preserves_release_identity_and_distinguishes_preview() {
        let release = ReleaseQualification {
            schema: "acyclic.sdk.docs.release-qualification.v1".to_owned(),
            version: "0.3.0".to_owned(),
            tag: "sdk-v0.3.0".to_owned(),
            revision: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            qualified: true,
        };
        let released = DocsBundle {
            schema_version: BUNDLE_SCHEMA_VERSION,
            source_revision: release.revision.clone(),
            crates: Vec::new(),
            diagnostics: Vec::new(),
            profiles: Vec::new(),
            landing: LandingCatalog {
                schema_version: 1,
                navigation: Vec::new(),
                categories: Vec::new(),
                package_instructions: Vec::new(),
            },
            scenario_bundle: None,
            release: Some(release.clone()),
            bundle_blake3: "bundle-release-blake3".to_owned(),
        };
        let released_projection: serde_json::Value = serde_json::from_str(
            &to_website_json(
                &released,
                "https://github.com/example/sdk",
                "clean",
                "release",
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            released_projection["source"]["revision"],
            release.revision.as_str()
        );
        assert_eq!(
            released_projection["source"]["release"]["schema"],
            release.schema.as_str()
        );
        assert_eq!(
            released_projection["source"]["release"]["version"],
            release.version.as_str()
        );
        assert_eq!(
            released_projection["source"]["release"]["tag"],
            release.tag.as_str()
        );
        assert_eq!(
            released_projection["source"]["release"]["revision"],
            release.revision.as_str()
        );
        assert_eq!(
            released_projection["source"]["release"]["sourceRevision"],
            release.revision.as_str()
        );
        assert_eq!(
            released_projection["source"]["release"]["bundleBlake3"],
            "bundle-release-blake3"
        );
        assert_eq!(released_projection["landing"]["schema_version"], 1);
        assert!(released_projection["landing"]["navigation"]
            .as_array()
            .is_some_and(Vec::is_empty));

        let preview = DocsBundle {
            source_revision: "fedcba9876543210fedcba9876543210fedcba98".to_owned(),
            release: None,
            ..released
        };
        let preview_projection: serde_json::Value = serde_json::from_str(
            &to_website_json(
                &preview,
                "https://github.com/example/sdk",
                "working-tree",
                "branch-preview",
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(preview_projection["source"]["channel"], "branch-preview");
        assert_eq!(
            preview_projection["source"]["revision"],
            "fedcba9876543210fedcba9876543210fedcba98"
        );
        assert!(preview_projection["source"].get("release").is_none());
    }

    #[test]
    fn rustdoc_fixture_retains_enum_variants_fields_and_trait_methods() {
        let value = serde_json::json!({
            "format_version": 60,
            "root": 1,
            "index": {
                "1": {
                    "crate_id": 0,
                    "name": "demo",
                    "visibility": "public",
                    "inner": {"module": {"items": [2, 3, 4, 8]}}
                },
                "2": {
                    "crate_id": 0,
                    "name": "State",
                    "visibility": "public",
                    "docs": "State values.",
                    "inner": {"enum": {"variants": [5], "impls": []}}
                },
                "3": {
                    "crate_id": 0,
                    "name": "Provider",
                    "visibility": "public",
                    "inner": {"trait": {"items": [6], "implementations": []}}
                },
                "4": {
                    "crate_id": 0,
                    "name": "Request",
                    "visibility": "public",
                    "inner": {"struct": {"kind": {"plain": {"fields": [7]}}}}
                },
                "5": {
                    "crate_id": 0,
                    "name": "Ready",
                    "visibility": "default",
                    "docs": "The ready state.",
                    "inner": {"variant": {"kind": "plain"}}
                },
                "6": {
                    "crate_id": 0,
                    "name": "connect",
                    "visibility": "default",
                    "docs": "Connect the provider.",
                    "inner": {"function": {"sig": {}}}
                },
                "7": {
                    "crate_id": 0,
                    "name": "request_id",
                    "visibility": "public",
                    "docs": "The request identifier.",
                    "inner": {"struct_field": {"primitive": "u64"}}
                },
                "8": {
                    "crate_id": 0,
                    "name": "MAX_REQUESTS",
                    "visibility": "public",
                    "docs": "The maximum number of requests.",
                    "inner": {
                        "constant": {
                            "type": {"primitive": "usize"},
                            "const": "64"
                        }
                    }
                }
            }
        });
        let mut diagnostics = Vec::new();
        let items = rustdoc_public_items(
            &value,
            Path::new("Q:/sdk"),
            Path::new("Q:/sdk/rust/crates/demo"),
            &mut diagnostics,
        );
        assert!(diagnostics.is_empty());
        assert!(items
            .iter()
            .any(|item| item.name == "Ready" && item.kind == "variant"));
        assert!(items
            .iter()
            .find(|item| item.name == "Ready")
            .and_then(|item| item.signature.as_ref())
            .is_some());
        assert!(items
            .iter()
            .any(|item| item.name == "request_id" && item.kind == "struct-field"));
        assert!(items
            .iter()
            .find(|item| item.name == "request_id")
            .and_then(|item| item.signature.as_ref())
            .is_some());
        assert!(items
            .iter()
            .any(|item| item.name == "connect" && item.kind == "function"));
        let max_requests = items
            .iter()
            .find(|item| item.name == "MAX_REQUESTS")
            .expect("module-level constant should be retained");
        assert_eq!(max_requests.kind, "constant");
        assert_eq!(
            max_requests.module_path.as_deref(),
            Some("demo::MAX_REQUESTS")
        );
        assert_eq!(
            max_requests
                .signature
                .as_ref()
                .and_then(|signature| signature.get("type"))
                .and_then(|value| value.get("primitive"))
                .and_then(serde_json::Value::as_str),
            Some("usize")
        );
        assert_eq!(
            max_requests.signature_text.as_deref(),
            Some("pub const MAX_REQUESTS: usize")
        );
        let connect = items.iter().find(|item| item.name == "connect").unwrap();
        assert_eq!(
            connect.module_path.as_deref(),
            Some("demo::Provider::connect")
        );
        assert!(connect.signature.is_some());
        assert_eq!(connect.signature_text.as_deref(), Some("pub fn connect()"));
        assert_eq!(
            items
                .iter()
                .find(|item| item.name == "request_id")
                .and_then(|item| item.module_path.as_deref()),
            Some("demo::Request::request_id")
        );
    }

    #[test]
    fn rustdoc_signature_text_keeps_bounds_where_clauses_and_associated_types() {
        let function = serde_json::json!({
            "sig": {
                "inputs": [["value", {"borrowed_ref": {"lifetime": "'a", "is_mutable": false, "type": {"generic": "T"}}}]],
                "output": {"generic": "T"}
            },
            "generics": {
                "params": [{"name": "'a", "kind": {"lifetime": {}}}, {"name": "T", "kind": {"type": {"bounds": [{"trait_bound": {"trait": {"path": "Send", "args": null}, "modifier": "none"}}]}}}],
                "where_predicates": [{"bound_predicate": {"type": {"generic": "T"}, "bounds": [{"trait_bound": {"trait": {"path": "Sync", "args": null}, "modifier": "none"}}]}}]
            },
            "header": {"is_const": false, "is_unsafe": false, "is_async": true}
        });
        let mut inner = serde_json::Map::new();
        inner.insert("function".to_owned(), function);
        assert_eq!(
            rustdoc_signature_text("fetch", "function", &inner, &serde_json::Map::new()).as_deref(),
            Some("pub async fn fetch<'a, T: Send>(value: &'a T) -> T where T: Sync")
        );

        let associated = serde_json::json!({
            "generics": {"params": [], "where_predicates": []},
            "bounds": [{"trait_bound": {"trait": {"path": "Future", "args": null}, "modifier": "none"}}],
            "type": {"generic": "Output"}
        });
        let mut associated_inner = serde_json::Map::new();
        associated_inner.insert("assoc_type".to_owned(), associated);
        assert_eq!(
            rustdoc_signature_text(
                "Item",
                "assoc_type",
                &associated_inner,
                &serde_json::Map::new()
            )
            .as_deref(),
            Some("type Item: Future = Output")
        );
        let synthetic_function = serde_json::json!({
            "sig": {
                "inputs": [["value", {"impl_trait": [{"trait_bound": {"trait": {"path": "AsRef", "args": {"angle_bracketed": {"args": [{"type": {"primitive": "str"}}], "constraints": []}}}, "modifier": "none"}}]}]],
                "output": null
            },
            "generics": {"params": [{"name": "impl AsRef<str>", "kind": {"type": {"bounds": [], "is_synthetic": true}}}], "where_predicates": []},
            "header": {"is_const": false, "is_unsafe": false, "is_async": false}
        });
        let mut synthetic_inner = serde_json::Map::new();
        synthetic_inner.insert("function".to_owned(), synthetic_function);
        assert_eq!(
            rustdoc_signature_text(
                "take",
                "function",
                &synthetic_inner,
                &serde_json::Map::new()
            )
            .as_deref(),
            Some("pub fn take(value: impl AsRef<str>)")
        );
        assert_eq!(
            rustdoc_type_text(&serde_json::json!({
                "qualified_path": {
                    "name": "Output",
                    "self_type": {"generic": "Self"},
                    "trait": {"path": "", "args": null}
                }
            })),
            "Self::Output"
        );
    }

    #[test]
    fn compiler_graph_merge_deduplicates_same_declaration_across_profiles() {
        let item = PublicItem {
            name: "StreamProvider".to_owned(),
            module_path: None,
            kind: "trait".to_owned(),
            signature: None,
            signature_text: None,
            source_path: Some("rust/crates/stream/src/lib.rs".to_owned()),
            source_line: Some(526),
            docs: Some("A stream provider.".to_owned()),
            conditional: false,
            generated: false,
            reexport: None,
        };
        let mut second = item.clone();
        second.source_line = Some(527);
        let graphs = vec![
            RustdocGraph {
                profile: "host-default".to_owned(),
                target: "x86_64-pc-windows-msvc".to_owned(),
                features: Vec::new(),
                profile_blake3: "default".to_owned(),
                public_items: vec![item.clone()],
                diagnostics: Vec::new(),
                rustdoc: test_rustdoc_provenance(),
            },
            RustdocGraph {
                profile: "host-capabilities".to_owned(),
                target: "x86_64-pc-windows-msvc".to_owned(),
                features: vec!["grpc".to_owned()],
                profile_blake3: "capabilities".to_owned(),
                public_items: vec![item, second],
                diagnostics: Vec::new(),
                rustdoc: test_rustdoc_provenance(),
            },
        ];
        let merged = merge_compiler_public_items(&graphs);
        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged
                .iter()
                .filter(|item| item.name == "StreamProvider")
                .count(),
            2
        );
    }

    #[test]
    fn host_profile_target_resolves_from_rustc() {
        let host = resolve_profile_target("host", None).expect("rustc host target");
        assert!(!host.is_empty());
        assert_ne!(host, "host");
        assert_eq!(
            resolve_profile_target("wasm32-unknown-unknown", None).unwrap(),
            "wasm32-unknown-unknown"
        );
    }

    #[test]
    fn rustdoc_public_item_identity_requires_path_and_semantics() {
        let mut item = PublicItem {
            name: "Demo".to_owned(),
            module_path: None,
            kind: "struct".to_owned(),
            signature: None,
            signature_text: None,
            source_path: None,
            source_line: None,
            docs: None,
            conditional: false,
            generated: false,
            reexport: None,
        };
        assert!(!rustdoc_public_item_identity_complete(&item));

        item.module_path = Some("demo::Demo".to_owned());
        assert!(!rustdoc_public_item_identity_complete(&item));

        item.signature_text = Some("pub struct Demo".to_owned());
        assert!(rustdoc_public_item_identity_complete(&item));
    }

    #[test]
    fn profile_status_fails_closed_for_empty_graph() {
        let profile = AnalysisProfile {
            name: "wasm".to_owned(),
            packages: vec![ProfilePackage {
                package: "demo-wasm".to_owned(),
                target: "wasm32-unknown-unknown".to_owned(),
                features: vec!["wasm32".to_owned()],
                default_features: false,
                crate_types: Vec::new(),
            }],
            scope: None,
        };
        let crate_bundle = CrateBundle {
            package_name: "demo-wasm".to_owned(),
            crate_name: Some("demo_wasm".to_owned()),
            path: "rust/crates/demo-wasm".to_owned(),
            publish: false,
            version: Some("0.1.0".to_owned()),
            availability: "source-only".to_owned(),
            analysis_mode: "rustdoc-json".to_owned(),
            sources: Vec::new(),
            guides: Vec::new(),
            examples: Vec::new(),
            package_instructions: Vec::new(),
            navigation: "crates/demo-wasm".to_owned(),
            public_items: Vec::new(),
            graphs: Vec::new(),
            rustdoc: None,
            diagnostics: Vec::new(),
            content_blake3: "hash".to_owned(),
            coverage: DocCoverage {
                guides: 0,
                examples: 0,
                public_items: 0,
                documented_items: 0,
                conditional_items: 0,
            },
        };
        let mut diagnostics = Vec::new();
        let statuses = evaluate_profiles(
            &[profile],
            &[crate_bundle],
            &mut diagnostics,
            "unknown",
            None,
        )
        .expect("profile evaluation");
        assert_eq!(statuses.len(), 1);
        assert!(!statuses[0].complete);
        assert_eq!(statuses[0].unresolved_packages, vec!["demo-wasm"]);
        assert_eq!(diagnostics[0].code, "profile_public_graph_unresolved");
    }

    #[test]
    fn release_scope_requires_only_the_exact_family_package() {
        let root = std::env::temp_dir().join(format!(
            "sdk-docs-release-scope-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join("rust/crates/stream/src")).unwrap();
        fs::create_dir_all(root.join("rust/crates/objects/src")).unwrap();
        fs::write(root.join("rust/crates/stream/src/lib.rs"), b"pub struct Stream;").unwrap();
        fs::write(root.join("rust/crates/objects/src/lib.rs"), b"pub struct Object;").unwrap();
        fs::write(
            root.join("rust/crates/stream/Cargo.toml"),
            b"[package]\nname = \"acyclic-stream\"\nversion = \"1.0.0-rc.5\"\n",
        )
        .unwrap();
        fs::write(
            root.join("rust/crates/objects/Cargo.toml"),
            b"[package]\nname = \"acyclic-objects\"\nversion = \"1.0.0-rc.3\"\n",
        )
        .unwrap();
        let profile = AnalysisProfile {
            name: "host-default".to_owned(),
            packages: vec![ProfilePackage {
                package: "acyclic-stream".to_owned(),
                target: "host".to_owned(),
                features: Vec::new(),
                default_features: true,
                crate_types: Vec::new(),
            }],
            scope: Some(ProfileScope {
                kind: "release-family".to_owned(),
                family: Some("stream".to_owned()),
                package: Some("acyclic-stream".to_owned()),
                version: Some("1.0.0-rc.5".to_owned()),
            }),
        };
        let crates = vec![
            CrateBundle {
                package_name: "acyclic-stream".to_owned(),
                crate_name: Some("acyclic_stream".to_owned()),
                path: "rust/crates/stream".to_owned(),
                publish: true,
                version: Some("1.0.0-rc.5".to_owned()),
                availability: "source-only".to_owned(),
                analysis_mode: "rustdoc-json".to_owned(),
                sources: Vec::new(),
                guides: Vec::new(),
                examples: Vec::new(),
                package_instructions: Vec::new(),
                navigation: "stream".to_owned(),
                public_items: vec![PublicItem {
                    name: "Stream".to_owned(),
                    module_path: Some("stream::Stream".to_owned()),
                    kind: "struct".to_owned(),
                    signature: Some(serde_json::json!({"kind":"plain"})),
                    signature_text: Some("pub struct Stream".to_owned()),
                    source_path: Some("rust/crates/stream/src/lib.rs".to_owned()),
                    source_line: Some(1),
                    docs: None,
                    conditional: false,
                    generated: false,
                    reexport: None,
                }],
                graphs: Vec::new(),
                rustdoc: None,
                diagnostics: Vec::new(),
                content_blake3: "stream".to_owned(),
                coverage: DocCoverage {
                    guides: 0,
                    examples: 0,
                    public_items: 1,
                    documented_items: 0,
                    conditional_items: 0,
                },
            },
            CrateBundle {
                package_name: "acyclic-objects".to_owned(),
                crate_name: Some("acyclic_objects".to_owned()),
                path: "rust/crates/objects".to_owned(),
                publish: true,
                version: Some("1.0.0-rc.3".to_owned()),
                availability: "source-only".to_owned(),
                analysis_mode: "source-fallback".to_owned(),
                sources: Vec::new(),
                guides: Vec::new(),
                examples: Vec::new(),
                package_instructions: Vec::new(),
                navigation: "objects".to_owned(),
                public_items: Vec::new(),
                graphs: Vec::new(),
                rustdoc: None,
                diagnostics: Vec::new(),
                content_blake3: "objects".to_owned(),
                coverage: DocCoverage {
                    guides: 0,
                    examples: 0,
                    public_items: 0,
                    documented_items: 0,
                    conditional_items: 0,
                },
            },
        ];
        let release = ReleaseQualification {
            schema: "acyclic.sdk.docs.release-qualification.v1".to_owned(),
            version: "1.0.0-rc.5".to_owned(),
            tag: "stream-v1.0.0-rc.5".to_owned(),
            revision: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            qualified: true,
        };
        let required = required_strict_packages(&[profile], Some(&release), &crates, &root)
            .expect("release profile scope");
        assert_eq!(required, HashSet::from(["acyclic-stream".to_owned()]));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn profile_status_fails_closed_for_missing_generated_source() {
        let profile = AnalysisProfile {
            name: "wasm".to_owned(),
            packages: vec![ProfilePackage {
                package: "demo-wasm".to_owned(),
                target: "wasm32-unknown-unknown".to_owned(),
                features: vec!["wasm32".to_owned()],
                default_features: false,
                crate_types: Vec::new(),
            }],
            scope: None,
        };
        let item = PublicItem {
            name: "Demo".to_owned(),
            module_path: Some("demo_wasm".to_owned()),
            kind: "struct".to_owned(),
            signature: Some(serde_json::json!({"kind": "plain"})),
            signature_text: Some("pub struct Demo".to_owned()),
            source_path: Some("rust/crates/demo-wasm/src/lib.rs".to_owned()),
            source_line: Some(1),
            docs: Some("Demo type.".to_owned()),
            conditional: false,
            generated: false,
            reexport: None,
        };
        let crate_bundle = CrateBundle {
            package_name: "demo-wasm".to_owned(),
            crate_name: Some("demo_wasm".to_owned()),
            path: "rust/crates/demo-wasm".to_owned(),
            publish: false,
            version: Some("0.1.0".to_owned()),
            availability: "source-only".to_owned(),
            analysis_mode: "rustdoc-json".to_owned(),
            sources: Vec::new(),
            guides: Vec::new(),
            examples: Vec::new(),
            package_instructions: Vec::new(),
            navigation: "crates/demo-wasm".to_owned(),
            public_items: vec![item.clone()],
            graphs: vec![RustdocGraph {
                profile: profile.name.clone(),
                target: "wasm32-unknown-unknown".to_owned(),
                features: vec!["wasm32".to_owned()],
                profile_blake3: profile_blake3(&profile),
                public_items: vec![item],
                diagnostics: vec![Diagnostic {
                    severity: "error".to_owned(),
                    code: "rustdoc_generated_source_missing".to_owned(),
                    message: "generated source is missing".to_owned(),
                    path: Some("/out/demo-wasm/generated.rs".to_owned()),
                    line: None,
                }],
                rustdoc: RustdocProvenance {
                    source_revision: "revision".to_owned(),
                    ..test_rustdoc_provenance()
                },
            }],
            rustdoc: None,
            diagnostics: Vec::new(),
            content_blake3: "hash".to_owned(),
            coverage: DocCoverage {
                guides: 0,
                examples: 0,
                public_items: 1,
                documented_items: 1,
                conditional_items: 0,
            },
        };
        let mut diagnostics = Vec::new();
        let statuses = evaluate_profiles(
            &[profile],
            &[crate_bundle],
            &mut diagnostics,
            "revision",
            None,
        )
        .expect("profile evaluation");
        assert!(!statuses[0].complete);
        assert_eq!(statuses[0].unresolved_packages, vec!["demo-wasm"]);
        assert_eq!(diagnostics[0].code, "profile_generated_source_missing");
    }

    fn test_rustdoc_provenance() -> RustdocProvenance {
        RustdocProvenance {
            path: "graph.json".to_owned(),
            format_version: Some(60),
            blake3: "graph".to_owned(),
            index_items: 1,
            source_blake3: "source".to_owned(),
            source_revision: "revision".to_owned(),
            toolchain: Some("1.98.1".to_owned()),
            target: Some("x86_64-pc-windows-msvc".to_owned()),
            features: Vec::new(),
            profile_blake3: "profile".to_owned(),
            receipt: Some("receipt.json".to_owned()),
            receipt_blake3: Some("receipt".to_owned()),
        }
    }

    fn scenario_fixture(name: &str, source: &[u8], snippet: &[u8], code_sha256: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("sdk-docs-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("snippets")).expect("create scenario fixture");
        let source_path = root.join("source.rs");
        fs::write(&source_path, source).expect("write scenario source");
        let snippet_path = root.join("snippets/example.rs");
        fs::write(&snippet_path, snippet).expect("write scenario snippet");
        let manifest = serde_json::json!({
            "schema": "acyclic.sdk.examples.bundle.v1",
            "source": {
                "path": "source.rs",
                "revision": "scenario-test-revision",
                "sha256": sha256_digest(source),
            },
            "files": ["snippets/example.rs"],
            "snippets": [{
                "path": "snippets/example.rs",
                "code_sha256": code_sha256,
                "validation": {
                    "receipt": {"source_sha256": sha256_digest(source)}
                }
            }]
        });
        fs::write(
            root.join("sdk-examples-manifest.json"),
            serde_json::to_vec(&manifest).expect("encode scenario manifest"),
        )
        .expect("write scenario manifest");
        root
    }

    #[test]
    fn scenario_bundle_rejects_declared_source_hash_not_bound_to_bytes() {
        let root = scenario_fixture(
            "source-hash",
            b"pub struct Source;\n",
            b"fn example() {}\n",
            &sha256_digest(b"fn example() {}\n"),
        );
        let manifest_path = root.join("sdk-examples-manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("read fixture")).unwrap();
        manifest["source"]["sha256"] = serde_json::Value::String("sha256:stale".to_owned());
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        let error = load_scenario_bundle(&root, &root, "scenario-test-revision")
            .expect_err("stale source hash must fail closed");
        assert!(error.to_string().contains("source sha256 mismatch"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scenario_bundle_rejects_declared_code_hash_not_bound_to_bytes() {
        let root = scenario_fixture(
            "code-hash",
            b"pub struct Source;\n",
            b"fn example() {}\n",
            "sha256:stale",
        );
        let error = load_scenario_bundle(&root, &root, "scenario-test-revision")
            .expect_err("stale code hash must fail closed");
        assert!(error.to_string().contains("code sha256 mismatch"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scenario_bundle_rejects_qualified_receipt_without_bound_artifact() {
        let root = scenario_fixture(
            "qualified-artifact",
            b"pub struct Source;\n",
            b"fn example() {}\n",
            &sha256_digest(b"fn example() {}\n"),
        );
        let manifest_path = root.join("sdk-examples-manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("read fixture")).unwrap();
        manifest["snippets"][0]["validation"]["receipt"]["status"] =
            serde_json::Value::String("qualified".to_owned());
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        let error = load_scenario_bundle(&root, &root, "scenario-test-revision")
            .expect_err("qualified receipt without artifact must fail closed");
        assert!(error.to_string().contains("no bound artifact"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scenario_bundle_rejects_qualified_receipt_for_missing_artifact_bytes() {
        let root = scenario_fixture(
            "missing-qualified-artifact",
            b"pub struct Source;\n",
            b"fn example() {}\n",
            &sha256_digest(b"fn example() {}\n"),
        );
        let manifest_path = root.join("sdk-examples-manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("read fixture")).unwrap();
        manifest["files"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::Value::String(
                "artifacts/package.tgz".to_owned(),
            ));
        manifest["snippets"][0]["validation"]["receipt"] = serde_json::json!({
            "status": "qualified",
            "source_sha256": sha256_digest(b"pub struct Source;\n"),
            "artifact": {
                "kind": "generated-package",
                "path": "artifacts/package.tgz",
                "sha256": sha256_digest(b"fabricated artifact")
            }
        });
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        let error = load_scenario_bundle(&root, &root, "scenario-test-revision")
            .expect_err("missing qualified artifact bytes must fail closed");
        assert!(error.to_string().contains("declared file is unavailable"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scenario_bundle_accepts_hashed_empty_output_with_assertions() {
        let root = scenario_fixture(
            "silent-qualified-artifact",
            b"pub struct Source;\n",
            b"fn example() {}\n",
            &sha256_digest(b"fn example() {}\n"),
        );
        fs::create_dir_all(root.join("artifacts")).unwrap();
        fs::write(root.join("artifacts/package.tgz"), b"package bytes").unwrap();
        fs::write(root.join("artifacts/stdout.log"), []).unwrap();
        fs::write(root.join("artifacts/stderr.log"), []).unwrap();
        let manifest_path = root.join("sdk-examples-manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("read fixture")).unwrap();
        for file in [
            "artifacts/package.tgz",
            "artifacts/stdout.log",
            "artifacts/stderr.log",
        ] {
            manifest["files"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::Value::String(file.to_owned()));
        }
        manifest["snippets"][0]["validation"]["receipt"] = serde_json::json!({
            "status": "qualified",
            "source_sha256": sha256_digest(b"pub struct Source;\n"),
            "assertions": ["roundtrip response was validated"],
            "artifact": {
                "kind": "generated-package",
                "path": "artifacts/package.tgz",
                "sha256": sha256_digest(b"package bytes")
            },
            "stdout_artifact": {
                "kind": "stdout",
                "path": "artifacts/stdout.log",
                "sha256": sha256_digest(b"")
            },
            "stderr_artifact": {
                "kind": "stderr",
                "path": "artifacts/stderr.log",
                "sha256": sha256_digest(b"")
            }
        });
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        load_scenario_bundle(&root, &root, "scenario-test-revision")
            .expect("real empty output files plus assertions qualify");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scenario_bundle_accepts_expanded_compile_runtime_package_receipt() {
        let root = scenario_fixture(
            "expanded-qualified-artifact",
            b"pub struct Source;\n",
            b"fn example() {}\n",
            &sha256_digest(b"fn example() {}\n"),
        );
        fs::create_dir_all(root.join("artifacts")).unwrap();
        let artifact_bytes = [
            ("compile.bin", b"compile bytes".as_slice()),
            ("runtime.bin", b"runtime bytes".as_slice()),
            ("package.tgz", b"package bytes".as_slice()),
        ];
        for (name, bytes) in artifact_bytes {
            fs::write(root.join("artifacts").join(name), bytes).unwrap();
        }
        fs::write(root.join("artifacts/stdout.log"), []).unwrap();
        fs::write(root.join("artifacts/stderr.log"), []).unwrap();
        let manifest_path = root.join("sdk-examples-manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("read fixture")).unwrap();
        for file in [
            "artifacts/compile.bin",
            "artifacts/runtime.bin",
            "artifacts/package.tgz",
            "artifacts/stdout.log",
            "artifacts/stderr.log",
        ] {
            manifest["files"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::Value::String(file.to_owned()));
        }
        manifest["snippets"][0]["validation"]["receipt"] = serde_json::json!({
            "status": "qualified",
            "source_sha256": sha256_digest(b"pub struct Source;\n"),
            "assertion_count": 2,
            "compile_artifact_path": "artifacts/compile.bin",
            "compile_artifact_sha256": sha256_digest(b"compile bytes"),
            "runtime_artifact_path": "artifacts/runtime.bin",
            "runtime_artifact_sha256": sha256_digest(b"runtime bytes"),
            "package_artifact_path": "artifacts/package.tgz",
            "package_artifact_sha256": sha256_digest(b"package bytes"),
            "stdout_path": "artifacts/stdout.log",
            "stdout_sha256": sha256_digest(b""),
            "stderr_path": "artifacts/stderr.log",
            "stderr_sha256": sha256_digest(b"")
        });
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        load_scenario_bundle(&root, &root, "scenario-test-revision")
            .expect("expanded package and compile/runtime receipt qualifies");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scenario_bundle_binds_directory_source_closure_bytes() {
        let root = scenario_fixture(
            "source-closure",
            b"pub struct Source;\n",
            b"fn example() {}\n",
            &sha256_digest(b"fn example() {}\n"),
        );
        fs::create_dir_all(root.join("source")).unwrap();
        fs::rename(root.join("source.rs"), root.join("source/lib.rs")).unwrap();
        fs::write(
            root.join("source/Cargo.toml"),
            b"[package]\nname = \"source\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[lib]\npath = \"lib.rs\"\n",
        )
        .unwrap();
        fs::write(
            root.join("source/Cargo.lock"),
            b"version = 3\n\n[[package]]\nname = \"source\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();
        fs::write(
            root.join("Cargo.toml"),
            b"[package]\nname = \"fixture-root\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[lib]\npath = \"source/lib.rs\"\n",
        )
        .unwrap();
        fs::write(root.join("Cargo.lock"), b"version = 3\n").unwrap();
        fs::create_dir_all(root.join(".cargo")).unwrap();
        fs::write(
            root.join(".cargo/config.toml"),
            b"[build]\ntarget-dir = \"target\"\n",
        )
        .unwrap();
        let source_files = vec![
            (
                "Cargo.lock".to_owned(),
                fs::read(root.join("Cargo.lock")).unwrap(),
            ),
            (
                "Cargo.toml".to_owned(),
                fs::read(root.join("Cargo.toml")).unwrap(),
            ),
            (
                ".cargo/config.toml".to_owned(),
                fs::read(root.join(".cargo/config.toml")).unwrap(),
            ),
            (
                "source/Cargo.lock".to_owned(),
                fs::read(root.join("source/Cargo.lock")).unwrap(),
            ),
            (
                "source/Cargo.toml".to_owned(),
                fs::read(root.join("source/Cargo.toml")).unwrap(),
            ),
            (
                "source/lib.rs".to_owned(),
                fs::read(root.join("source/lib.rs")).unwrap(),
            ),
        ];
        let source_hash = sha256_digest(
            &cargo_source_closure_bytes(&root, "source", &source_files, "x86_64-pc-windows-msvc")
                .unwrap(),
        );
        let manifest_path = root.join("sdk-examples-manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("read fixture")).unwrap();
        manifest["source"]["path"] = serde_json::Value::String("source".to_owned());
        manifest["source"]["files"] = serde_json::json!([
            "Cargo.lock",
            "Cargo.toml",
            ".cargo/config.toml",
            "source/Cargo.lock",
            "source/Cargo.toml",
            "source/lib.rs"
        ]);
        manifest["source"]["target"] =
            serde_json::Value::String("x86_64-pc-windows-msvc".to_owned());
        manifest["source"]["sha256"] = serde_json::Value::String(source_hash.clone());
        manifest["snippets"][0]["source_sha256"] = serde_json::Value::String(source_hash.clone());
        manifest["snippets"][0]["validation"]["receipt"]["source_sha256"] =
            serde_json::Value::String(source_hash);
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        load_scenario_bundle(&root, &root, "scenario-test-revision")
            .expect("directory source closure bytes must bind exactly");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scenario_bundle_rejects_undeclared_outside_source_input() {
        let root = scenario_fixture(
            "source-closure-outside",
            b"pub struct Source;\n",
            b"fn example() {}\n",
            &sha256_digest(b"fn example() {}\n"),
        );
        fs::create_dir_all(root.join("source")).unwrap();
        fs::rename(root.join("source.rs"), root.join("source/lib.rs")).unwrap();
        fs::write(
            root.join("source/Cargo.toml"),
            b"[package]\nname = \"source\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[lib]\npath = \"lib.rs\"\n",
        )
        .unwrap();
        fs::write(
            root.join("source/Cargo.lock"),
            b"version = 3\n\n[[package]]\nname = \"source\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();
        fs::write(root.join("outside.txt"), b"outside").unwrap();
        let manifest_path = root.join("sdk-examples-manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("read fixture")).unwrap();
        manifest["source"]["path"] = serde_json::Value::String("source".to_owned());
        manifest["source"]["files"] =
            serde_json::json!(["source/Cargo.toml", "source/lib.rs", "outside.txt"]);
        manifest["source"]["target"] =
            serde_json::Value::String("x86_64-pc-windows-msvc".to_owned());
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        let error = load_scenario_bundle(&root, &root, "scenario-test-revision")
            .expect_err("unapproved outside source input must fail closed");
        assert!(error.to_string().contains("unsafe or duplicate path"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn release_tag_scopes_bind_the_exact_version() {
        for (tag, version) in [
            ("acyclic-v0.1.5", "0.1.5"),
            ("cargo-v0.1.5", "0.1.5"),
            ("npm-v0.1.5-rc.1", "0.1.5-rc.1"),
            ("fs-v0.2.0-rc.1", "0.2.0-rc.1"),
            ("publish/acyclic-fs/0.2.0-rc.3", "0.2.0-rc.3"),
            ("publish/acyclic-harness/0.1.0-rc.1", "0.1.0-rc.1"),
        ] {
            validate_release_tag_version(tag, version)
                .unwrap_or_else(|error| panic!("{tag} should be accepted: {error}"));
        }
        for (tag, version) in [
            ("cargo-v0.1.5", "0.1.6"),
            ("npm-v0.1.5", "0.1.5+build.2"),
            ("v0.1.5", "0.1.5"),
            ("untrusted-v0.1.5", "0.1.5"),
        ] {
            let error = validate_release_tag_version(tag, version)
                .expect_err("mismatched or unscoped release identity must fail closed");
            assert!(
                error.to_string().contains("release qualification"),
                "unexpected error for {tag}: {error}"
            );
        }
    }

    #[test]
    fn release_family_scopes_accept_published_package_tags() {
        for (tag, family) in [
            ("publish/acyclic-fs/0.2.0-rc.2", "filesystem"),
            ("publish/acyclic-stream/1.0.0-rc.5", "stream"),
            ("publish/acyclic-harness/0.1.0-rc.1", "harness"),
            ("publish/acyclic-inference/1.0.0-rc.6", "inference"),
            ("publish/acyclic-machines/1.0.0-rc.5", "machines"),
            ("publish/acyclic-objects/1.0.0-rc.3", "objects"),
        ] {
            assert_eq!(release_family_from_tag(tag).as_deref(), Some(family));
        }
    }

    #[test]
    fn release_qualification_requires_explicit_qualification() {
        let path = std::env::temp_dir().join(format!(
            "sdk-docs-release-qualification-{}-{}.json",
            std::process::id(),
            "unqualified"
        ));
        let manifest = serde_json::json!({
            "schema": "acyclic.sdk.docs.release-qualification.v1",
            "version": "0.1.5",
            "tag": "cargo-v0.1.5",
            "revision": "0000000000000000000000000000000000000000",
            "qualified": false
        });
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let error = verify_release_qualification(&path, Path::new("."), "unknown", false)
            .expect_err("unqualified release manifest must fail closed");
        assert!(error.to_string().contains("qualified=true"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn release_qualification_rejects_mismatched_tag_version() {
        let path = std::env::temp_dir().join(format!(
            "sdk-docs-release-qualification-{}-{}.json",
            std::process::id(),
            "version-mismatch"
        ));
        let manifest = serde_json::json!({
            "schema": "acyclic.sdk.docs.release-qualification.v1",
            "version": "0.1.6",
            "tag": "cargo-v0.1.5",
            "revision": "0000000000000000000000000000000000000000",
            "qualified": true
        });
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let error = verify_release_qualification(&path, Path::new("."), "unknown", false)
            .expect_err("a release manifest must bind its version to its tag scope");
        assert!(error.to_string().contains("tag/version mismatch"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn release_qualification_requires_clean_worktree() {
        let path = std::env::temp_dir().join(format!(
            "sdk-docs-release-qualification-{}-{}.json",
            std::process::id(),
            "dirty"
        ));
        let manifest = serde_json::json!({
            "schema": "acyclic.sdk.docs.release-qualification.v1",
            "version": "0.1.5",
            "tag": "cargo-v0.1.5",
            "revision": "0000000000000000000000000000000000000000",
            "qualified": true
        });
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let error = verify_release_qualification(&path, Path::new("."), "unknown", true)
            .expect_err("dirty worktree must fail release qualification");
        assert!(error.to_string().contains("clean Git worktree"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rustdoc_profile_receipt_records_targets_features_crate_types_and_sources() {
        let root = std::env::temp_dir().join(format!(
            "sdk-docs-profile-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let crate_dir = root.join("rust/crates/profile_fixture");
        fs::create_dir_all(crate_dir.join("src")).unwrap();
        fs::write(
            crate_dir.join("Cargo.toml"),
            "[package]\nname = \"profile_fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[lib]\ncrate-type = [\"cdylib\", \"rlib\"]\n",
        )
        .unwrap();
        fs::write(
            crate_dir.join("src/lib.rs"),
            "/// Fixture API\npub fn answer() -> u32 { 42 }\n",
        )
        .unwrap();
        fs::write(
            crate_dir.join("README.md"),
            "# Fixture guide\n\nA Rust-owned guide.\n",
        )
        .unwrap();
        let manifest = root.join("profiles.json");
        fs::write(
            &manifest,
            r#"{"schema_version":1,"profiles":[{"name":"synthetic","packages":[{"package":"profile_fixture","target":"host","features":["json"],"default_features":false}]}]}"#,
        )
        .unwrap();
        let output = root.join("receipt.json");

        let receipt = write_rustdoc_profile(&root, &manifest, &output, "1.98.1").unwrap();
        let package = &receipt.profiles[0].packages[0];
        assert_eq!(package.package, "profile_fixture");
        assert!(!package.target.is_empty());
        assert_eq!(package.features, vec!["json"]);
        assert!(!package.default_features);
        assert_eq!(package.crate_types, vec!["cdylib", "rlib"]);
        assert!(package
            .public_source_paths
            .iter()
            .any(|path| path.ends_with("rust/crates/profile_fixture/src/lib.rs")));
        assert!(package
            .guide_source_paths
            .iter()
            .any(|path| path.ends_with("rust/crates/profile_fixture/README.md")));
        assert!(!package.guide_source_blake3.is_empty());
        assert!(!package.source_blake3.is_empty());
        let persisted: RustdocProfileReceipt =
            serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
        assert_eq!(persisted, receipt);
        fs::remove_file(&manifest).unwrap();
        let inferred = write_rustdoc_profile(&root, &manifest, &output, "1.98.1").unwrap();
        assert_eq!(inferred.profiles[0].name, "host-default-inferred");
        assert_eq!(inferred.profiles[0].packages[0].default_features, true);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn inferred_legacy_profile_binds_exact_checkout_revision() {
        let root = std::env::temp_dir().join(format!(
            "sdk-docs-legacy-profile-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let crate_dir = root.join("rust/crates/legacy_fixture");
        fs::create_dir_all(crate_dir.join("src")).unwrap();
        fs::write(
            crate_dir.join("Cargo.toml"),
            "[package]\nname = \"legacy_fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        fs::write(crate_dir.join("src/lib.rs"), "pub fn legacy() {}\n").unwrap();
        assert!(Command::new("git")
            .args(["-C", root.to_str().unwrap(), "init", "--quiet"])
            .status()
            .unwrap()
            .success());
        assert!(Command::new("git")
            .args([
                "-C",
                root.to_str().unwrap(),
                "-c",
                "user.name=profile-test",
                "-c",
                "user.email=profile-test@example.invalid",
                "add",
                ".",
            ])
            .status()
            .unwrap()
            .success());
        assert!(Command::new("git")
            .args([
                "-C",
                root.to_str().unwrap(),
                "-c",
                "user.name=profile-test",
                "-c",
                "user.email=profile-test@example.invalid",
                "-c",
                "commit.gpgSign=false",
                "commit",
                "--quiet",
                "-m",
                "legacy profile fixture",
            ])
            .status()
            .unwrap()
            .success());
        let expected_revision = String::from_utf8(
            Command::new("git")
                .args(["-C", root.to_str().unwrap(), "rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();
        let receipt = write_rustdoc_profile(
            &root,
            &root.join("missing-profile.json"),
            &root.join("out/receipt.json"),
            "1.98.1",
        )
        .unwrap();
        assert_eq!(receipt.source_revision, expected_revision);
        assert_eq!(receipt.profiles[0].name, "host-default-inferred");
        assert_eq!(receipt.profiles[0].packages[0].package, "legacy_fixture");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn generation_provenance_separates_generator_and_source_and_detects_mutation() {
        let root = std::env::temp_dir().join(format!(
            "sdk-docs-provenance-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source_root = root.join("source");
        let generator_root = root.join("generator");
        fs::create_dir_all(&source_root).unwrap();
        fs::create_dir_all(&generator_root).unwrap();
        fs::write(source_root.join("Cargo.toml"), b"[workspace]\n").unwrap();
        fs::write(source_root.join("guide.md"), b"# Source\n").unwrap();
        fs::create_dir_all(source_root.join("target")).unwrap();
        fs::write(
            source_root.join("target/authoritative-source.txt"),
            b"This is source, not compiler output.\n",
        )
        .unwrap();
        fs::create_dir_all(source_root.join("work")).unwrap();
        fs::write(
            source_root.join("work/authoritative-source.txt"),
            b"This is also source, not compiler output.\n",
        )
        .unwrap();
        fs::create_dir_all(source_root.join(".toolchains/qualification-run" )).unwrap();
        fs::write(
            source_root.join(".toolchains/qualification-run/linked-input"),
            b"environment-owned toolchain input\n",
        )
        .unwrap();
        fs::write(generator_root.join("Cargo.toml"), b"[workspace]\n").unwrap();
        fs::write(generator_root.join("generator.md"), b"# Generator\n").unwrap();
        let profile_manifest = root.join("profiles.json");
        fs::write(&profile_manifest, b"{\"profiles\":[]}\n").unwrap();
        for repository in [&source_root, &generator_root] {
            assert!(Command::new("git")
                .args(["-C", repository.to_str().unwrap(), "init", "--quiet"])
                .status()
                .unwrap()
                .success());
            assert!(Command::new("git")
                .args([
                    "-C",
                    repository.to_str().unwrap(),
                    "-c",
                    "user.name=provenance-test",
                    "-c",
                    "user.email=provenance-test@example.invalid",
                    "add",
                    ".",
                ])
                .status()
                .unwrap()
                .success());
            assert!(Command::new("git")
                .args([
                    "-C",
                    repository.to_str().unwrap(),
                    "-c",
                    "user.name=provenance-test",
                    "-c",
                    "user.email=provenance-test@example.invalid",
                    "-c",
                    "commit.gpgSign=false",
                    "commit",
                    "--quiet",
                    "-m",
                    "fixture",
                ])
                .status()
                .unwrap()
                .success());
        }
        let output_dir = source_root.join("generated");
        let compiler_cache_dir = source_root.join("cache");
        let before = capture_generation_provenance(
            &generator_root,
            &source_root,
            &output_dir,
            &compiler_cache_dir,
            &profile_manifest,
        )
        .unwrap();
        assert_ne!(before.source_git_sha, before.generator_git_sha);
        assert!(before
            .input_closure
            .iter()
            .any(|input| input.path == "guide.md"));
        assert!(before
            .input_closure
            .iter()
            .any(|input| input.path == "target/authoritative-source.txt"));
        assert!(before
            .input_closure
            .iter()
            .any(|input| input.path == "work/authoritative-source.txt"));
        assert!(!before
            .input_closure
            .iter()
            .any(|input| input.path.starts_with(".toolchains/")));
        assert!(before
            .generator_input_closure
            .iter()
            .any(|input| input.path == "generator.md"));
        assert!(!before.input_closure.iter().any(|input| {
            input.path.starts_with("generated/") || input.path.starts_with("cache/")
        }));
        fs::write(source_root.join("guide.md"), b"# Changed source\n").unwrap();
        fs::write(&profile_manifest, b"{\"profiles\":[1]}\n").unwrap();
        fs::write(
            generator_root.join("generator.md"),
            b"# Changed generator\n",
        )
        .unwrap();
        let after = capture_generation_provenance(
            &generator_root,
            &source_root,
            &output_dir,
            &compiler_cache_dir,
            &profile_manifest,
        )
        .unwrap();
        assert_eq!(before.source_git_sha, after.source_git_sha);
        assert_ne!(before.input_closure, after.input_closure);
        assert_ne!(
            before.generator_input_closure,
            after.generator_input_closure
        );
        assert_ne!(
            before.profile_manifest_blake3,
            after.profile_manifest_blake3
        );
        assert_ne!(before, after);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn profile_snapshot_is_the_bytes_that_provenance_guards() {
        let root = std::env::temp_dir().join(format!(
            "sdk-docs-profile-snapshot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let profile = root.join("profiles.json");
        let original = b"{\"schema_version\":1,\"profiles\":[]}\n";
        fs::write(&profile, original).unwrap();
        let (_profiles, snapshot) = load_profile_manifest_snapshot(&profile).unwrap();
        assert_eq!(snapshot, original);
        let before_digest = digest_bytes(&snapshot);
        fs::write(&profile, b"{\"schema_version\":1,\"profiles\":[1]}\n").unwrap();
        let observed_digest = digest_bytes(&fs::read(&profile).unwrap());
        assert_ne!(before_digest, observed_digest);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn source_archive_does_not_inherit_parent_git_revision() {
        let root =
            std::env::temp_dir().join(format!("sdk-docs-source-archive-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        assert!(git_revision(&root).is_none());
        assert!(!git_worktree_dirty(&root));
        fs::remove_dir_all(root).unwrap();
    }
}
