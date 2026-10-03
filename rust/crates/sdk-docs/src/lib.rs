//! Build a deterministic, Rust-owned documentation manifest for an SDK tree.
//!
//! The source scanner is deliberately conservative. It collects Markdown and
//! Rust doc comments, records public declarations with source locations, and
//! emits diagnostics for `pub use` and conditional declarations that require
//! compiler reflection. When a matching rustdoc JSON file is available, the
//! caller can attach it to the bundle and use it as the authoritative item
//! inventory. The scanner never claims to resolve a re-export or cfg branch.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

/// A public declaration discovered by rustdoc JSON or conservative source
/// scanning.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicItem {
    /// Rust declaration name as written by the source or rustdoc JSON.
    pub name: String,
    /// `struct`, `enum`, `trait`, `fn`, `type`, `const`, `static`, `mod`, or
    /// `macro`.
    pub kind: String,
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
}

/// One package entry in a rustdoc target/feature profile.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfilePackage {
    /// Cargo package name.
    pub package: String,
    /// Rust target triple used for this package.
    pub target: String,
    /// Cargo features enabled for this package. `default` is explicit when
    /// the package's manifest default features are part of the graph.
    #[serde(default)]
    pub features: Vec<String>,
    /// Whether Cargo's manifest defaults participate in the graph.
    #[serde(default = "default_features")]
    pub default_features: bool,
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
    /// `registry-unverified` or `source-only`; this is never inferred from a
    /// website route or a generated binding package.
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
    /// Rust-owned executable language scenarios and their qualification
    /// receipts. This remains optional so older source exports can still be
    /// scanned, while strict preview bundles can require it explicitly.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scenario_bundle: Option<serde_json::Value>,
    /// BLAKE3 digest of the canonical bundle payload excluding this field.
    pub bundle_blake3: String,
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
}

/// Inputs for the docs-only rustdoc JSON generation command.
#[derive(Clone, Debug)]
pub struct GenerateOptions {
    pub repository_root: PathBuf,
    pub profile_manifest: PathBuf,
    pub output_dir: PathBuf,
    pub toolchain: String,
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
}

/// Reproducibility receipt for one complete profile generation invocation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenerationReceipt {
    pub schema_version: u32,
    pub source_revision: String,
    pub toolchain: String,
    pub profile_manifest_blake3: String,
    pub artifacts: Vec<GeneratedArtifact>,
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
        }
    }
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
    if options.require_rustdoc_json
        && crates.iter().any(|crate_bundle| {
            crate_bundle.publish
                && (crate_bundle.analysis_mode != "rustdoc-json"
                    || crate_bundle.public_items.is_empty())
        })
    {
        return Err(Error::Strict(
            "one or more publishable crates lack a matching compiled rustdoc JSON artifact or resolved public graph".to_owned(),
        ));
    }

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
    );
    if options.require_rustdoc_json && profiles.iter().any(|status| !status.complete) {
        return Err(Error::Strict(
            "one or more required rustdoc feature/target profiles have missing or unresolved packages".to_owned(),
        ));
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

    let payload = serde_json::json!({
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "source_revision": source_revision,
        "crates": crates,
        "diagnostics": diagnostics,
        "profiles": profiles,
        "scenario_bundle": scenario_bundle,
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
        scenario_bundle: serde_json::from_value(payload["scenario_bundle"].clone())?,
        bundle_blake3,
    })
}

/// Generate pinned, docs-only rustdoc JSON artifacts and source-binding
/// receipts for every package in every declared profile.
pub fn generate_rustdoc(options: &GenerateOptions) -> Result<GenerationReceipt, Error> {
    if !options.repository_root.is_dir() {
        return Err(Error::InvalidRepository(options.repository_root.clone()));
    }
    let profiles = load_profile_manifest(&options.profile_manifest)?;
    let profile_manifest_blake3 = digest_bytes(&fs::read(&options.profile_manifest)?);
    fs::create_dir_all(&options.output_dir)?;
    let source_revision =
        git_revision(&options.repository_root).unwrap_or_else(|| "unknown".to_owned());
    let mut artifacts = Vec::new();
    for profile in profiles {
        let profile_output = options.output_dir.join(&profile.name);
        fs::create_dir_all(&profile_output)?;
        let target_dir = options.output_dir.join(".cargo-target").join(&profile.name);
        let profile_blake3 = profile_blake3(&profile);
        for package in &profile.packages {
            let package_name = &package.package;
            let crate_dir = find_package_dir(&options.repository_root, package_name)?;
            let source_blake3 = source_content_hash(&crate_dir, &options.repository_root)?;
            let mut command = Command::new("cargo");
            command.arg(format!("+{}", options.toolchain)).args([
                "rustdoc",
                "--locked",
                "--package",
                package_name,
                "--target",
                &package.target,
                "--lib",
            ]);
            let features = package
                .features
                .iter()
                .filter(|feature| feature.as_str() != "default")
                .cloned()
                .collect::<Vec<_>>();
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
            let output = command.output().map_err(Error::Io)?;
            if !output.status.success() {
                return Err(Error::Strict(format!(
                    "rustdoc generation failed for profile {} package {}: {}",
                    profile.name,
                    package_name,
                    String::from_utf8_lossy(&output.stderr).trim()
                )));
            }
            let generated_root = target_dir.join(&package.target).join("doc");
            let generated_json =
                find_rustdoc_json(&generated_root, package_name).ok_or_else(|| {
                    Error::Strict(format!(
                        "rustdoc generation produced no JSON for profile {} package {}",
                        profile.name, package_name
                    ))
                })?;
            let json_name = format!("{}.json", package_name.replace('-', "_"));
            let output_json = profile_output.join(json_name);
            fs::copy(&generated_json, &output_json)?;
            let json_bytes = fs::read(&output_json)?;
            let rustdoc_json_blake3 = digest_bytes(&json_bytes);
            let receipt = RustdocReceipt {
                schema_version: 1,
                package_name: package_name.clone(),
                source_blake3: source_blake3.clone(),
                source_revision: source_revision.clone(),
                toolchain: options.toolchain.clone(),
                target: package.target.clone(),
                features: features.clone(),
                profile_blake3: profile_blake3.clone(),
                profile: profile.name.clone(),
                rustdoc_json_blake3: rustdoc_json_blake3.clone(),
            };
            let receipt_file = receipt_path(&output_json);
            fs::write(
                &receipt_file,
                serde_json::to_string_pretty(&receipt)? + "\n",
            )?;
            artifacts.push(GeneratedArtifact {
                profile: profile.name.clone(),
                package_name: package_name.clone(),
                target: package.target.clone(),
                features,
                profile_blake3: profile_blake3.clone(),
                source_blake3,
                rustdoc_json: relative_path(&options.output_dir, &output_json),
                rustdoc_json_blake3,
                receipt: relative_path(&options.output_dir, &receipt_file),
            });
        }
    }
    artifacts.sort_by(|left, right| {
        left.profile
            .cmp(&right.profile)
            .then(left.package_name.cmp(&right.package_name))
    });
    let result = GenerationReceipt {
        schema_version: 1,
        source_revision,
        toolchain: options.toolchain.clone(),
        profile_manifest_blake3,
        artifacts,
    };
    fs::write(
        options.output_dir.join("generation-receipt.json"),
        serde_json::to_string_pretty(&result)? + "\n",
    )?;
    Ok(result)
}

/// Serialize a bundle in stable pretty-printed JSON.
pub fn to_pretty_json(bundle: &DocsBundle) -> Result<String, Error> {
    Ok(serde_json::to_string_pretty(bundle)? + "\n")
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
                    serde_json::json!({
                        "kind": item.kind,
                        "name": item.name,
                        "summary": item.docs.as_deref().and_then(|docs| docs.lines().next()).filter(|summary| !summary.trim().is_empty()).unwrap_or("No declaration summary was provided."),
                        "sourcePath": item.source_path,
                        "sourceLine": item.source_line,
                        "conditional": item.conditional,
                        "generated": item.generated,
                    })
                })
                .collect::<Vec<_>>();
            let referenced_paths = crate_bundle
                .public_items
                .iter()
                .filter_map(|item| item.source_path.as_deref())
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
                "slug": crate_bundle.package_name.strip_prefix("acyclic-").unwrap_or(&crate_bundle.package_name),
                "title": title,
                "crate": crate_bundle.package_name,
                "crateName": crate_bundle.crate_name,
                "navigation": crate_bundle.navigation,
                "version": crate_bundle.version,
                "availability": crate_bundle.availability,
                "analysisMode": crate_bundle.analysis_mode,
                "contentBlake3": crate_bundle.content_blake3,
                "coverage": crate_bundle.coverage,
                "maturity": if crate_bundle.publish { "preview" } else { "source-only" },
                "deployment": "unknown",
                "qualification": if crate_bundle.graphs.iter().any(|graph| !graph.public_items.is_empty()) { "qualified-graph" } else { "unqualified" },
                "summary": format!("Rust-owned API reference for {}.", crate_bundle.package_name),
                "guides": crate_bundle.guides,
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
    if let Some(authority_sha256) = scenario_authority_sha256 {
        projection_source["scenarioAuthoritySha256"] = authority_sha256;
    }
    let projection = serde_json::json!({
        "$schema": "https://acyclic.dev/schemas/sdk-reference-bundle.v1.json",
        "schemaVersion": "sdk-reference-bundle.v1",
        "source": projection_source,
        "profiles": bundle.profiles,
        "scenarioBundle": bundle.scenario_bundle,
        "families": families,
    });
    Ok(serde_json::to_string_pretty(&projection)? + "\n")
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
        let source_root = Path::new(&source_path);
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
                || !relative_path.starts_with(source_root)
                || !seen.insert(relative.to_owned())
            {
                return Err(Error::Strict(format!(
                    "SDK examples source closure contains an unsafe or duplicate path: {relative}"
                )));
            }
            let path = repository_root.join(relative_path);
            let bytes = fs::read(&path).map_err(|error| {
                Error::Strict(format!(
                    "SDK examples source closure file {relative} is unavailable: {error}"
                ))
            })?;
            closure_files.push((relative.to_owned(), bytes));
        }
        cargo_source_closure_bytes(repository_root, &source_path, &closure_files)?
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

fn cargo_source_closure_bytes(
    repository_root: &Path,
    source_path: &str,
    files: &[(String, Vec<u8>)],
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
    let recipe = normalized_cargo_build_recipe(&source_root, &metadata)?;
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

fn normalized_cargo_build_recipe(
    source_root: &Path,
    metadata: &serde_json::Value,
) -> Result<Vec<u8>, Error> {
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
    let root_package = package_by_id
        .get(root_id)
        .ok_or_else(|| Error::Strict(format!("cargo metadata package is missing for {root_id}")))?;
    let root_label = format!(
        "{}@{}",
        root_package
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown"),
        root_package
            .get("version")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
    );
    let mut reachable = BTreeSet::new();
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
    let mut normalized = BTreeMap::new();
    for id in reachable {
        let package = package_by_id
            .get(&id)
            .ok_or_else(|| Error::Strict(format!("cargo metadata package is missing for {id}")))?;
        let node = node_by_id
            .get(&id)
            .ok_or_else(|| Error::Strict(format!("cargo metadata node is missing for {id}")))?;
        let manifest = package
            .get("manifest_path")
            .and_then(serde_json::Value::as_str)
            .map(PathBuf::from)
            .and_then(|path| path.strip_prefix(source_root).ok().map(Path::to_owned))
            .map(|path| path.to_string_lossy().replace('\\', "/"));
        let key = format!(
            "{}@{}",
            package
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            package
                .get("version")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
        );
        let targets = package
            .get("targets")
            .and_then(serde_json::Value::as_array)
            .map(|targets| {
                let mut targets = targets
                    .iter()
                    .map(|target| {
                        serde_json::json!({
                            "name": target.get("name"),
                            "kind": target.get("kind"),
                            "crate_types": target.get("crate_types"),
                            "required_features": target.get("required_features"),
                        })
                    })
                    .collect::<Vec<_>>();
                targets.sort_by_key(serde_json::Value::to_string);
                targets
            })
            .unwrap_or_default();
        normalized.insert(
            format!("{key}:{}", manifest.as_deref().unwrap_or("registry")),
            serde_json::json!({
                "name": package.get("name"),
                "version": package.get("version"),
                "source": if package.get("source").is_some_and(|source| !source.is_null()) { "registry" } else { "local" },
                "manifest": manifest,
                "features": node.get("features"),
                "targets": targets,
                "dependencies": package.get("dependencies"),
            }),
        );
    }
    serde_json::to_vec(&serde_json::json!({
        "schema": "acyclic.sdk.cargo-build-recipe.v1",
        "target": serde_json::Value::Null,
        "root": root_label,
        "packages": normalized,
    }))
    .map_err(Error::Json)
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
    if !matches!(
        authority_schema,
        Some("acyclic.sdk.examples.source-authority.v1")
            | Some("acyclic.sdk.qualification-receipt.v1")
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
            authority_source
                .and_then(|source| source.get("revision"))
                .and_then(serde_json::Value::as_str)
        })
        .ok_or_else(|| {
            Error::Strict("SDK examples source authority has no source revision".to_owned())
        })?;
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
    if let Some(source_files) = source_files {
        if source_files.is_empty() {
            return Err(Error::Strict(
                "SDK examples source authority has an empty file list".to_owned(),
            ));
        }
        let mut seen = HashSet::new();
        let mut closure = Vec::new();
        for value in source_files {
            let relative = value.as_str().ok_or_else(|| {
                Error::Strict("SDK examples source authority file is not a string".to_owned())
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
            }
            closure.extend_from_slice(relative.as_bytes());
            closure.push(0);
            closure.extend_from_slice(&bytes);
            closure.push(0);
        }
        let closure_hash =
            if source_files.len() == 1 && source_files[0].as_str() == Some(source_path) {
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
    Ok(authority_digest)
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
        let format_version = value
            .get("format_version")
            .and_then(serde_json::Value::as_u64);
        let mut graph_diagnostics = Vec::new();
        let graph_items =
            rustdoc_public_items(&value, repository_root, crate_dir, &mut graph_diagnostics);
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
    let package_instructions = if publish {
        vec![PackageInstruction {
            ecosystem: "cargo".to_owned(),
            package: package_name.clone(),
            command: if dirty_worktree
                || source_state == "working-tree"
                || source_revision == "unknown"
            {
                format!("cargo add {package_name} --path {crate_relative_path}")
            } else {
                format!(
                    "cargo add {package_name} --git https://github.com/acyclic-labs/sdk --rev {source_revision}"
                )
            },
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
        availability: if publish {
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
        let _ = writeln!(
            contents,
            "### `{}` {{#{anchor}}}\n\n{}\n\nSource: {}\n",
            item.name,
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
    let contents = fs::read_to_string(path)?;
    let manifest: ProfileManifestFile = serde_json::from_str(&contents)?;
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

fn evaluate_profiles(
    profiles: &[AnalysisProfile],
    crates: &[CrateBundle],
    diagnostics: &mut Vec<Diagnostic>,
    source_revision: &str,
    expected_toolchain: Option<&str>,
) -> Vec<ProfileStatus> {
    let by_package = crates
        .iter()
        .map(|crate_bundle| (crate_bundle.package_name.as_str(), crate_bundle))
        .collect::<HashMap<_, _>>();
    let mut statuses = profiles
        .iter()
        .map(|profile| {
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
                        let graph = crate_bundle.graphs.iter().find(|graph| {
                            graph.profile_blake3 == profile_digest
                                && graph.target == package.target
                                && graph.features == normalized_features(&package.features)
                                && graph.rustdoc.source_revision == source_revision
                                && expected_toolchain
                                    .is_none_or(|toolchain| graph.rustdoc.toolchain.as_deref() == Some(toolchain))
                                && !graph.public_items.is_empty()
                        });
                        if graph.is_none() {
                            unresolved_packages.push(package.package.clone());
                            diagnostics.push(Diagnostic {
                                severity: "error".to_owned(),
                                code: "profile_public_graph_unresolved".to_owned(),
                                message: format!(
                                    "profile {} ({}; features: {}) has no source-bound resolved public graph for package {}",
                                    profile.name,
                                    package.target,
                                    if package.features.is_empty() {
                                        "default".to_owned()
                                    } else {
                                        package.features.join(",")
                                    },
                                    package.package
                                ),
                                path: Some(crate_bundle.path.clone()),
                                line: None,
                            });
                        }
                    }
                }
            }
            let complete = missing_packages.is_empty() && unresolved_packages.is_empty();
            ProfileStatus {
                profile: profile.clone(),
                complete,
                profile_blake3: profile_digest,
                missing_packages,
                unresolved_packages,
            }
        })
        .collect::<Vec<_>>();
    statuses.sort_by(|left, right| left.profile.name.cmp(&right.profile.name));
    statuses
}

/// Extract the compiler-resolved public graph from rustdoc JSON. The JSON
/// format is intentionally read as `serde_json::Value`: its schema is
/// experimental, and the recorded format version must remain visible in the
/// bundle instead of being hidden behind an accidentally stale Rust type.
fn rustdoc_public_items(
    value: &serde_json::Value,
    repository_root: &Path,
    crate_dir: &Path,
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
    let mut pending = vec![(root_id.to_string(), false)];
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
                .map(|id| (id, false)),
        );
    }
    let mut items = Vec::new();
    while let Some((id, inherited_public)) = pending.pop() {
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
        if let Some(module) = inner.get("module").and_then(serde_json::Value::as_object) {
            if let Some(children) = module.get("items").and_then(serde_json::Value::as_array) {
                enqueue_rustdoc_ids(children, &mut pending, false);
            }
        }
        if let Some(use_item) = inner.get("use").and_then(serde_json::Value::as_object) {
            if let Some(target_id) = use_item.get("id").and_then(serde_json::Value::as_u64) {
                let target = target_id.to_string();
                if index.contains_key(&target) {
                    pending.push((target, false));
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
                        enqueue_rustdoc_ids(fields, &mut pending, true);
                    }
                }
            }
            if let Some(impls) = struct_item
                .get("impls")
                .and_then(serde_json::Value::as_array)
            {
                enqueue_rustdoc_ids(impls, &mut pending, false);
            }
        }
        if let Some(enum_item) = inner.get("enum").and_then(serde_json::Value::as_object) {
            if let Some(variants) = enum_item
                .get("variants")
                .and_then(serde_json::Value::as_array)
            {
                enqueue_rustdoc_ids(variants, &mut pending, true);
            }
            if let Some(impls) = enum_item.get("impls").and_then(serde_json::Value::as_array) {
                enqueue_rustdoc_ids(impls, &mut pending, false);
            }
        }
        if let Some(union_item) = inner.get("union").and_then(serde_json::Value::as_object) {
            if let Some(fields) = union_item
                .get("fields")
                .and_then(serde_json::Value::as_array)
            {
                enqueue_rustdoc_ids(fields, &mut pending, true);
            }
            if let Some(impls) = union_item
                .get("impls")
                .and_then(serde_json::Value::as_array)
            {
                enqueue_rustdoc_ids(impls, &mut pending, false);
            }
        }
        if let Some(trait_item) = inner.get("trait").and_then(serde_json::Value::as_object) {
            if let Some(trait_items) = trait_item
                .get("items")
                .and_then(serde_json::Value::as_array)
            {
                enqueue_rustdoc_ids(trait_items, &mut pending, true);
            }
        }
        if let Some(impl_item) = inner.get("impl").and_then(serde_json::Value::as_object) {
            if let Some(impl_items) = impl_item.get("items").and_then(serde_json::Value::as_array) {
                enqueue_rustdoc_ids(impl_items, &mut pending, false);
            }
        }
        if (visibility != Some("public") && !inherited_public) || id == root_id.to_string() {
            continue;
        }
        let Some((kind, _)) = inner.iter().next() else {
            continue;
        };
        let Some(name) = item.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let span = item.get("span").and_then(serde_json::Value::as_object);
        let source_path = span
            .and_then(|span| span.get("filename"))
            .and_then(serde_json::Value::as_str)
            .map(|path| path.replace('\\', "/"));
        let source_path = source_path.and_then(|path| {
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
        });
        let source_line = span
            .and_then(|span| span.get("begin"))
            .and_then(serde_json::Value::as_array)
            .and_then(|begin| begin.first())
            .and_then(serde_json::Value::as_u64)
            .and_then(|line| usize::try_from(line).ok());
        let docs = item
            .get("docs")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
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
        items.push(PublicItem {
            name: name.to_owned(),
            kind: kind.replace('_', "-"),
            source_path,
            source_line,
            docs,
            conditional,
            generated,
        });
    }
    items
}

fn enqueue_rustdoc_ids(
    values: &[serde_json::Value],
    pending: &mut Vec<(String, bool)>,
    inherited_public: bool,
) {
    pending.extend(
        values
            .iter()
            .filter_map(|value| value.as_u64().map(|id| (id.to_string(), inherited_public))),
    );
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
                    kind,
                    source_path: Some(path.to_owned()),
                    source_line: Some(line_number),
                    docs: (!docs.is_empty()).then(|| docs.join("\n")),
                    conditional,
                    generated: path.split('/').any(|part| part == "generated"),
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
    let output = Command::new("git")
        .args(["-C", root.to_str()?, "rev-parse", "HEAD"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git_worktree_dirty(root: &Path) -> bool {
    Command::new("git")
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
                    "docs": "Re-exported API.",
                    "span": {"filename": "rust/crates/demo/src/lib.rs", "begin": [4, 1]},
                    "inner": {"use": {"id": 4, "name": "Alias", "source": "private::Thing"}}
                },
                "3": {
                    "crate_id": 0,
                    "name": "NativeThing",
                    "visibility": "public",
                    "attrs": ["#[cfg(feature = \"native\")]"] ,
                    "span": {"filename": "rust/crates/demo/src/lib.rs", "begin": [8, 1]},
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
            && item.docs.as_deref() == Some("Re-exported API.")));
        assert!(items
            .iter()
            .any(|item| item.name == "NativeThing" && item.conditional));
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
                    "inner": {"module": {"items": [2, 3, 4]}}
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
            .any(|item| item.name == "request_id" && item.kind == "struct-field"));
        assert!(items
            .iter()
            .any(|item| item.name == "connect" && item.kind == "function"));
    }

    #[test]
    fn compiler_graph_merge_deduplicates_same_declaration_across_profiles() {
        let item = PublicItem {
            name: "StreamProvider".to_owned(),
            kind: "trait".to_owned(),
            source_path: Some("rust/crates/stream/src/lib.rs".to_owned()),
            source_line: Some(526),
            docs: Some("A stream provider.".to_owned()),
            conditional: false,
            generated: false,
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
                rustdoc: test_rustdoc_provenance(),
            },
            RustdocGraph {
                profile: "host-capabilities".to_owned(),
                target: "x86_64-pc-windows-msvc".to_owned(),
                features: vec!["grpc".to_owned()],
                profile_blake3: "capabilities".to_owned(),
                public_items: vec![item, second],
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
    fn profile_status_fails_closed_for_empty_graph() {
        let profile = AnalysisProfile {
            name: "wasm".to_owned(),
            packages: vec![ProfilePackage {
                package: "demo-wasm".to_owned(),
                target: "wasm32-unknown-unknown".to_owned(),
                features: vec!["wasm32".to_owned()],
                default_features: false,
            }],
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
        );
        assert_eq!(statuses.len(), 1);
        assert!(!statuses[0].complete);
        assert_eq!(statuses[0].unresolved_packages, vec!["demo-wasm"]);
        assert_eq!(diagnostics[0].code, "profile_public_graph_unresolved");
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
        let mut source_hasher = Sha256::new();
        source_hasher.update(b"source/lib.rs");
        source_hasher.update([0]);
        source_hasher.update(b"pub struct Source;\n");
        source_hasher.update([0]);
        let source_hash = format!("sha256:{:x}", source_hasher.finalize());
        let manifest_path = root.join("sdk-examples-manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("read fixture")).unwrap();
        manifest["source"]["path"] = serde_json::Value::String("source".to_owned());
        manifest["source"]["files"] = serde_json::json!(["source/lib.rs"]);
        manifest["source"]["sha256"] = serde_json::Value::String(source_hash.clone());
        manifest["snippets"][0]["source_sha256"] = serde_json::Value::String(source_hash.clone());
        manifest["snippets"][0]["validation"]["receipt"]["source_sha256"] =
            serde_json::Value::String(source_hash);
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        load_scenario_bundle(&root, &root, "scenario-test-revision")
            .expect("directory source closure bytes must bind exactly");
        fs::remove_dir_all(root).unwrap();
    }
}
