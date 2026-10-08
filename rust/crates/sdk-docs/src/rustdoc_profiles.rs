//! Cargo and Rustdoc owned availability profiles.
//!
//! This module is the small shared boundary between the generator and the
//! documentation projection. Cargo metadata supplies package features,
//! publication, and dependency ownership; Rustdoc format 60 supplies the
//! public items that were actually present for one target/profile. Binding
//! packages therefore retain their own receipt and API items while pointing at
//! the published core package that owns the shared contract.

#![allow(missing_docs)]

use cargo_metadata::{DependencyKind, Metadata, MetadataCommand, Package, TargetKind};
use rustdoc_types::{Crate as RustdocCrate, Visibility, FORMAT_VERSION};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::public_api;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileError {
    UnknownPackage(String),
    UnknownApiOwner(String),
    AmbiguousApiOwner(String),
    InvalidFeature { package: String, feature: String },
    UnsupportedTarget { target: String },
    InvalidRustdoc(String),
}

impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPackage(package) => {
                write!(f, "package `{package}` is absent from Cargo metadata")
            }
            Self::UnknownApiOwner(package) => write!(
                f,
                "private package `{package}` has no published Rust API owner"
            ),
            Self::AmbiguousApiOwner(package) => write!(
                f,
                "private package `{package}` has multiple published Rust API owners"
            ),
            Self::InvalidFeature { package, feature } => write!(
                f,
                "feature `{feature}` is absent from package `{package}` metadata"
            ),
            Self::UnsupportedTarget { target } => {
                write!(f, "target `{target}` is not installed/available")
            }
            Self::InvalidRustdoc(message) => write!(f, "invalid rustdoc receipt: {message}"),
        }
    }
}

impl std::error::Error for ProfileError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiOwnerKind {
    PublishedRoot,
    NapiBinding,
    WasmBinding,
    UniFfiBinding,
    OtherBinding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiOwner {
    /// The published package that owns the shared Rust contract.
    pub published_package: String,
    /// The package whose Rustdoc receipt contributes the actual public API.
    pub rustdoc_package: String,
    pub kind: ApiOwnerKind,
}

fn is_publishable(package: &Package) -> bool {
    package
        .publish
        .as_ref()
        .is_none_or(|registries| !registries.is_empty())
}

fn package_by_name<'a>(metadata: &'a Metadata, name: &str) -> Option<&'a Package> {
    metadata
        .packages
        .iter()
        .find(|package| package.name.as_ref() == name)
}

/// Read the exact Cargo metadata used by a profile invocation.
pub fn load_metadata(manifest: impl AsRef<Path>) -> Result<Metadata, ProfileError> {
    load_metadata_with_cargo(manifest, None)
}

/// Read metadata with an explicitly selected Cargo executable.
///
/// Release generation passes the pinned toolchain's Cargo here so feature
/// and target profiles, ownership, and version checks all use one Cargo
/// identity. The one-argument helper remains available to library callers.
pub fn load_metadata_with_cargo(
    manifest: impl AsRef<Path>,
    cargo_path: Option<&Path>,
) -> Result<Metadata, ProfileError> {
    let mut command = MetadataCommand::new();
    if let Some(cargo_path) = cargo_path {
        command.cargo_path(cargo_path);
    }
    command.other_options(vec!["--locked".to_owned()]);
    command
        .no_deps()
        .manifest_path(manifest.as_ref())
        .exec()
        .map_err(|error| ProfileError::InvalidRustdoc(format!("cargo metadata failed: {error}")))
}

/// Resolve the published Rust owner of a package from Cargo metadata.
///
/// A published package owns its own receipt. A private binding must have one
/// published non-dev dependency; a package with zero or multiple candidates
/// fails closed. The binding remains the Rustdoc source so binding-only
/// getters, opaque IDs, and strong enums are retained in the projection.
pub fn api_owner_for_package(metadata: &Metadata, package: &str) -> Result<ApiOwner, ProfileError> {
    let current = package_by_name(metadata, package)
        .ok_or_else(|| ProfileError::UnknownPackage(package.to_owned()))?;
    if is_publishable(current) {
        return Ok(ApiOwner {
            published_package: package.to_owned(),
            rustdoc_package: package.to_owned(),
            kind: ApiOwnerKind::PublishedRoot,
        });
    }

    let mut owners = current
        .dependencies
        .iter()
        .filter(|dependency| dependency.kind != DependencyKind::Development)
        .filter_map(|dependency| package_by_name(metadata, dependency.name.as_str()))
        .filter(|candidate| {
            is_publishable(candidate)
                && (candidate.name.as_ref() == package
                    || package.starts_with(&format!("{}-", candidate.name)))
        })
        .collect::<Vec<_>>();
    owners.sort_by(|left, right| left.name.cmp(&right.name));
    owners.dedup_by(|left, right| left.name == right.name);

    let owner_name = if owners.len() == 1 {
        owners[0].name.to_string()
    } else if owners.is_empty() {
        let mut fallback = current
            .dependencies
            .iter()
            .filter(|dependency| dependency.kind != DependencyKind::Development)
            .filter_map(|dependency| package_by_name(metadata, dependency.name.as_str()))
            .filter(|candidate| is_publishable(candidate))
            .collect::<Vec<_>>();
        fallback.sort_by(|left, right| left.name.cmp(&right.name));
        fallback.dedup_by(|left, right| left.name == right.name);
        match fallback.as_slice() {
            [owner] => owner.name.to_string(),
            [] => return Err(ProfileError::UnknownApiOwner(package.to_owned())),
            _ => return Err(ProfileError::AmbiguousApiOwner(package.to_owned())),
        }
    } else {
        return Err(ProfileError::AmbiguousApiOwner(package.to_owned()));
    };

    let kind = if current
        .dependencies
        .iter()
        .any(|dependency| matches!(dependency.name.as_str(), "napi" | "napi-derive"))
    {
        ApiOwnerKind::NapiBinding
    } else if current
        .dependencies
        .iter()
        .any(|dependency| dependency.name == "uniffi")
    {
        ApiOwnerKind::UniFfiBinding
    } else if current
        .dependencies
        .iter()
        .any(|dependency| matches!(dependency.name.as_str(), "wasm-bindgen" | "js-sys"))
    {
        ApiOwnerKind::WasmBinding
    } else {
        ApiOwnerKind::OtherBinding
    };
    Ok(ApiOwner {
        published_package: owner_name,
        rustdoc_package: package.to_owned(),
        kind,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileSpec {
    pub package: String,
    pub target: String,
    pub default_features: bool,
    pub features: BTreeSet<String>,
}

/// Stable package/target/default/feature identity for one Rustdoc receipt.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProfileId(pub String);

impl ProfileSpec {
    pub fn id(&self) -> ProfileId {
        let features = self
            .features
            .iter()
            .map(|feature| hex(feature.as_bytes()))
            .collect::<Vec<_>>()
            .join(",");
        ProfileId(format!(
            "p{};t{};d{};f{}",
            hex(self.package.as_bytes()),
            hex(self.target.as_bytes()),
            u8::from(self.default_features),
            features
        ))
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    result
}

/// Derive the bounded profile matrix from the package's actual Cargo feature
/// map. Every returned profile is intended to compile; a failed profile is a
/// qualification failure rather than a reason to omit an API.
pub fn profiles_for_package(
    metadata: &Metadata,
    package: &str,
    target: &str,
    available_targets: &BTreeSet<String>,
) -> Result<Vec<ProfileSpec>, ProfileError> {
    if !available_targets.contains(target) {
        return Err(ProfileError::UnsupportedTarget {
            target: target.to_owned(),
        });
    }
    let package_metadata = package_by_name(metadata, package)
        .ok_or_else(|| ProfileError::UnknownPackage(package.to_owned()))?;
    let mut profiles = vec![ProfileSpec {
        package: package.to_owned(),
        target: target.to_owned(),
        default_features: true,
        features: BTreeSet::new(),
    }];
    if !package_metadata.features.is_empty() {
        profiles.push(ProfileSpec {
            package: package.to_owned(),
            target: target.to_owned(),
            default_features: false,
            features: BTreeSet::new(),
        });
    }
    let declared = package_metadata
        .features
        .keys()
        .filter(|feature| feature.as_str() != "default")
        .cloned()
        .collect::<BTreeSet<_>>();
    for feature in &declared {
        profiles.push(ProfileSpec {
            package: package.to_owned(),
            target: target.to_owned(),
            default_features: false,
            features: feature_closure(&package_metadata.features, feature),
        });
    }
    if !declared.is_empty() {
        profiles.push(ProfileSpec {
            package: package.to_owned(),
            target: target.to_owned(),
            default_features: true,
            features: declared,
        });
    }
    profiles.sort_by_key(ProfileSpec::id);
    profiles.dedup_by_key(|profile| profile.id());
    Ok(profiles)
}

fn feature_closure(features: &BTreeMap<String, Vec<String>>, root: &str) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    let mut pending = vec![root.to_owned()];
    while let Some(feature) = pending.pop() {
        if !result.insert(feature.clone()) {
            continue;
        }
        if let Some(dependencies) = features.get(&feature) {
            for dependency in dependencies {
                let local = dependency.strip_prefix("?").unwrap_or(dependency);
                if !local.starts_with("dep:")
                    && !local.contains('/')
                    && features.contains_key(local)
                {
                    pending.push(local.to_owned());
                }
            }
        }
    }
    result
}

/// Execute one profile through Cargo and retain its exact Rustdoc JSON.
pub fn execute_profile(
    manifest: impl AsRef<Path>,
    metadata: &Metadata,
    profile: &ProfileSpec,
    available_targets: &BTreeSet<String>,
    target_dir: impl AsRef<Path>,
    output_json: impl AsRef<Path>,
) -> Result<RustdocObservation, ProfileError> {
    execute_profile_with_cargo(
        manifest,
        metadata,
        profile,
        available_targets,
        target_dir,
        output_json,
        None,
    )
}

/// Execute a profile with the caller's pinned Cargo binary.
///
/// The default [`execute_profile`] entry point remains convenient for library
/// callers. The production generator uses this variant so experimental binding
/// receipts use the same pinned Cargo as the stable Rustdoc stage.
pub fn execute_profile_with_cargo(
    manifest: impl AsRef<Path>,
    metadata: &Metadata,
    profile: &ProfileSpec,
    available_targets: &BTreeSet<String>,
    target_dir: impl AsRef<Path>,
    output_json: impl AsRef<Path>,
    cargo_path: Option<&Path>,
) -> Result<RustdocObservation, ProfileError> {
    execute_target_profile_with_cargo(
        manifest,
        metadata,
        profile,
        available_targets,
        target_dir,
        output_json,
        cargo_path,
        &RustdocTarget::Library,
    )
}

/// A Cargo-owned target; binary receipts are for source guides, never public API extraction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RustdocTarget {
    Library,
    Binary(String),
}

// Preserve the existing seven-argument Cargo boundary while adding explicit target ownership.
#[allow(clippy::too_many_arguments)]
pub fn execute_target_profile_with_cargo(
    manifest: impl AsRef<Path>,
    metadata: &Metadata,
    profile: &ProfileSpec,
    available_targets: &BTreeSet<String>,
    target_dir: impl AsRef<Path>,
    output_json: impl AsRef<Path>,
    cargo_path: Option<&Path>,
    rustdoc_target: &RustdocTarget,
) -> Result<RustdocObservation, ProfileError> {
    if !available_targets.contains(&profile.target) {
        return Err(ProfileError::UnsupportedTarget {
            target: profile.target.clone(),
        });
    }
    let package = package_by_name(metadata, &profile.package)
        .ok_or_else(|| ProfileError::UnknownPackage(profile.package.clone()))?;
    if let RustdocTarget::Binary(name) = rustdoc_target {
        if !package
            .targets
            .iter()
            .any(|target| target.name == *name && target.kind.contains(&TargetKind::Bin))
        {
            return Err(ProfileError::InvalidRustdoc(
                "binary target is absent from Cargo metadata".into(),
            ));
        }
    }
    for feature in &profile.features {
        if !package.features.contains_key(feature) {
            return Err(ProfileError::InvalidFeature {
                package: profile.package.clone(),
                feature: feature.clone(),
            });
        }
    }
    let target_dir = target_dir.as_ref();
    fs::create_dir_all(target_dir).map_err(|error| {
        ProfileError::InvalidRustdoc(format!("cannot create target directory: {error}"))
    })?;
    let cargo = cargo_path
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("CARGO").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("cargo"));
    let mut command = Command::new(cargo);
    // Cargo can inherit a different RUSTC/RUSTDOC from the host process (for
    // example when the generator itself was launched by another toolchain).
    // Pin all three producer binaries together so incremental artifacts cannot
    // cross-contaminate a profile receipt.
    if let Some(cargo_bin) = cargo_path.and_then(|path| path.parent()) {
        let rustc = cargo_bin.join(if cfg!(windows) { "rustc.exe" } else { "rustc" });
        let rustdoc = cargo_bin.join(if cfg!(windows) {
            "rustdoc.exe"
        } else {
            "rustdoc"
        });
        if rustc.is_file() {
            command.env("RUSTC", rustc);
        }
        if rustdoc.is_file() {
            command.env("RUSTDOC", rustdoc);
        }
    }
    command
        .arg("rustdoc")
        .arg("--locked")
        .arg("--manifest-path")
        .arg(manifest.as_ref())
        .arg("--package")
        .arg(&profile.package);
    match rustdoc_target {
        RustdocTarget::Library => {
            command.arg("--lib");
        }
        RustdocTarget::Binary(name) => {
            command.args(["--bin", name]);
        }
    }
    command
        .arg("--target")
        .arg(&profile.target)
        .env("CARGO_TARGET_DIR", target_dir)
        .env("RUSTC_BOOTSTRAP", "1");
    if !profile.default_features {
        command.arg("--no-default-features");
    }
    if !profile.features.is_empty() {
        command.arg("--features").arg(
            profile
                .features
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    command.args(["--", "-Z", "unstable-options", "--output-format", "json"]);
    let output = command.output().map_err(|error| {
        ProfileError::InvalidRustdoc(format!(
            "could not start Cargo for {}: {error}",
            profile.id().0
        ))
    })?;
    if !output.status.success() {
        return Err(ProfileError::InvalidRustdoc(format!(
            "Cargo profile {} failed ({}): {}",
            profile.id().0,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let library_name = package
        .targets
        .iter()
        .find(|target| {
            target.kind.iter().any(|kind| {
                matches!(
                    kind,
                    TargetKind::Lib
                        | TargetKind::RLib
                        | TargetKind::DyLib
                        | TargetKind::CDyLib
                        | TargetKind::StaticLib
                )
            })
        })
        .map(|target| target.name.as_str())
        .unwrap_or(profile.package.as_str());
    let receipt_name = match rustdoc_target {
        RustdocTarget::Library => library_name,
        RustdocTarget::Binary(name) => name,
    };
    let filename = format!("{}.json", receipt_name.replace('-', "_"));
    let generated = target_dir.join(&profile.target).join("doc").join(filename);
    if !generated.is_file() {
        return Err(ProfileError::InvalidRustdoc(format!(
            "Cargo succeeded but did not produce {}",
            generated.display()
        )));
    }
    fs::copy(&generated, output_json.as_ref()).map_err(|error| {
        ProfileError::InvalidRustdoc(format!(
            "cannot retain Rustdoc receipt {}: {error}",
            output_json.as_ref().display()
        ))
    })?;
    match rustdoc_target {
        RustdocTarget::Library => observe_rustdoc(output_json),
        RustdocTarget::Binary(_) => observe_binary_rustdoc(output_json),
    }
}

pub fn local_item_names(path: impl AsRef<Path>) -> Result<BTreeSet<String>, ProfileError> {
    let receipt = read_public_receipt(path.as_ref())?;
    Ok(receipt
        .index
        .values()
        .filter(|item| item.crate_id == 0)
        .filter_map(|item| item.name.clone())
        .collect())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedApiItem {
    /// Package whose Rustdoc receipt supplied this exact item.
    pub rustdoc_package: String,
    /// Published package owning the shared Rust contract.
    pub published_owner: String,
    /// Stable package/target/features identity of the receipt.
    pub profile: ProfileId,
    /// Signature-aware Rustdoc identity of the item.
    pub key: ProjectionKey,
    /// Crate version reported by Rustdoc, when present.
    pub rustdoc_version: Option<String>,
    /// Whether Rustdoc supplied documentation for the item.
    pub docs_present: bool,
}

/// Extract the binding package's own public API. The core owner's projection is
/// deliberately not substituted for this list.
pub fn extract_owned_api(
    path: impl AsRef<Path>,
    owner: &ApiOwner,
    profile: ProfileId,
) -> Result<Vec<OwnedApiItem>, ProfileError> {
    let expected_crate_name = owner.rustdoc_package.replace('-', "_");
    extract_owned_api_for_crate(path, owner, profile, &expected_crate_name)
}

/// Extract a receipt when the package's library target has a crate name that
/// differs from its package name (for example the `acyclic-plugin` binary).
/// The generator resolves this target name from Cargo metadata before calling
/// this adapter, so ownership remains package exact while the Rustdoc root
/// remains target exact.
pub fn extract_owned_api_for_crate(
    path: impl AsRef<Path>,
    owner: &ApiOwner,
    profile: ProfileId,
    expected_crate_name: &str,
) -> Result<Vec<OwnedApiItem>, ProfileError> {
    if profile.0.is_empty() {
        return Err(ProfileError::InvalidRustdoc(
            "binding API extraction requires a non-empty profile identity".to_owned(),
        ));
    }
    let receipt = read_public_receipt(path.as_ref())?;
    let root = receipt.index.get(&receipt.root).ok_or_else(|| {
        ProfileError::InvalidRustdoc("binding receipt root is absent from its index".to_owned())
    })?;
    if root.name.as_deref() != Some(expected_crate_name) {
        return Err(ProfileError::InvalidRustdoc(format!(
            "binding receipt belongs to {:?}, expected {expected_crate_name}",
            root.name
        )));
    }
    // Use the same format-59 compatibility adapter and public-api renderer as
    // `build_data`. Rustdoc's debug representation is a compiler-internal
    // identity and does not match the rendered SDK signature.
    let raw = fs::read(path.as_ref()).map_err(|error| {
        ProfileError::InvalidRustdoc(format!(
            "cannot read public API input {}: {error}",
            path.as_ref().display()
        ))
    })?;
    let public_items = public_api::extract(path.as_ref(), &raw).map_err(|error| {
        ProfileError::InvalidRustdoc(format!(
            "public API extraction failed for {}: {error}",
            path.as_ref().display()
        ))
    })?;
    let mut items = public_items
        .into_iter()
        .filter_map(|public_item| {
            let item = receipt.index.get(&public_item.id)?;
            if item.crate_id != 0 || item.visibility != Visibility::Public {
                return None;
            }
            let summary = receipt.paths.get(&public_item.id)?;
            Some(OwnedApiItem {
                rustdoc_package: owner.rustdoc_package.clone(),
                published_owner: owner.published_package.clone(),
                profile: profile.clone(),
                key: ProjectionKey {
                    path: public_item.path.join("::"),
                    kind: format!("{:?}", summary.kind).to_lowercase(),
                    signature: public_item.display,
                },
                rustdoc_version: receipt.crate_version.clone(),
                docs_present: item.docs.is_some(),
            })
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(items)
}

fn read_public_receipt(path: impl AsRef<Path>) -> Result<RustdocCrate, ProfileError> {
    let path = path.as_ref();
    let bytes = fs::read(path)
        .map_err(|error| ProfileError::InvalidRustdoc(format!("{}: {error}", path.display())))?;
    let receipt = serde_json::from_slice::<RustdocCrate>(&bytes)
        .map_err(|error| ProfileError::InvalidRustdoc(format!("{}: {error}", path.display())))?;
    if receipt.format_version != FORMAT_VERSION || receipt.includes_private {
        return Err(ProfileError::InvalidRustdoc(
            "public Rustdoc extraction requires format 60 with includes_private=false".to_owned(),
        ));
    }
    Ok(receipt)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RustdocObservation {
    pub format_version: u32,
    pub crate_name: String,
    pub crate_version: Option<String>,
    pub target: String,
    pub includes_private: bool,
    pub item_count: usize,
}

pub fn observe_rustdoc(path: impl AsRef<Path>) -> Result<RustdocObservation, ProfileError> {
    let receipt = read_public_receipt(path)?;
    observe_receipt(receipt)
}

/// Read a native binary receipt without exposing its private implementation as API.
pub fn observe_binary_rustdoc(path: impl AsRef<Path>) -> Result<RustdocObservation, ProfileError> {
    let receipt: RustdocCrate = serde_json::from_slice(
        &fs::read(path).map_err(|e| ProfileError::InvalidRustdoc(e.to_string()))?,
    )
    .map_err(|e| ProfileError::InvalidRustdoc(e.to_string()))?;
    if receipt.format_version != FORMAT_VERSION {
        return Err(ProfileError::InvalidRustdoc(
            "binary Rustdoc requires format 60".into(),
        ));
    }
    observe_receipt(receipt)
}

fn observe_receipt(receipt: RustdocCrate) -> Result<RustdocObservation, ProfileError> {
    let root = receipt.index.get(&receipt.root).ok_or_else(|| {
        ProfileError::InvalidRustdoc(format!("root item {} is absent from index", receipt.root.0))
    })?;
    let crate_name = root
        .name
        .clone()
        .ok_or_else(|| ProfileError::InvalidRustdoc("root module has no crate name".to_owned()))?;
    Ok(RustdocObservation {
        format_version: receipt.format_version,
        crate_name,
        crate_version: receipt.crate_version,
        target: receipt.target.triple,
        includes_private: receipt.includes_private,
        item_count: receipt.index.len(),
    })
}

/// Require a Rustdoc receipt to identify the exact Cargo package release it
/// was generated from. This check runs even when a receipt happens to expose
/// no projectable public items.
pub fn validate_rustdoc_version(
    metadata: &Metadata,
    package: &str,
    observation: &RustdocObservation,
) -> Result<(), ProfileError> {
    let package_metadata = package_by_name(metadata, package)
        .ok_or_else(|| ProfileError::UnknownPackage(package.to_owned()))?;
    let expected = package_metadata.version.to_string();
    if observation.crate_version.as_deref() != Some(expected.as_str()) {
        return Err(ProfileError::InvalidRustdoc(format!(
            "Rustdoc receipt for {package} reports {:?}, Cargo reports {expected}",
            observation.crate_version
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionKey {
    pub path: String,
    pub kind: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedItem {
    pub key: ProjectionKey,
    pub profile: ProfileId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailableItem {
    pub key: ProjectionKey,
    pub profiles: BTreeSet<ProfileId>,
}

/// Merge profile projections without collapsing overloads that have different
/// signatures.
pub fn merge_projection(
    projections: impl IntoIterator<Item = ProjectedItem>,
) -> Vec<AvailableItem> {
    let mut merged = BTreeMap::<ProjectionKey, BTreeSet<ProfileId>>::new();
    for projection in projections {
        merged
            .entry(projection.key)
            .or_default()
            .insert(projection.profile);
    }
    merged
        .into_iter()
        .map(|(key, profiles)| AvailableItem { key, profiles })
        .collect()
}

/// Stable sidecar schema for target/feature availability projected onto the
/// existing docs item IDs. The docs catalog remains authoritative for item
/// content; this sidecar only records where each exact signature was emitted.
pub const PROFILE_AVAILABILITY_SCHEMA: &str = "sdk-docs-profile-availability.v1";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProfileAvailability {
    /// Sidecar schema identity.
    pub schema: String,
    /// Deterministically ordered item availability entries.
    pub entries: Vec<ProfileAvailabilityEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProfileAvailabilityEntry {
    /// ID assigned by the existing `sdk-docs` item catalog.
    pub item_id: String,
    /// Rustdoc path retained for consumers that do not load the catalog.
    pub path: String,
    /// Rustdoc item kind.
    pub kind: String,
    /// Exact rendered Rust signature; this keeps overloads distinct.
    pub signature: String,
    /// Profiles in which this exact item was publicly emitted.
    pub profiles: Vec<ProfileAvailabilityProfile>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub struct ProfileAvailabilityProfile {
    /// Stable package/target/features identity.
    pub profile: ProfileId,
    /// Package whose Rustdoc receipt supplied the item.
    pub rustdoc_package: String,
    /// Published package owning the shared Rust contract.
    pub published_owner: String,
    /// Version reported by the Rustdoc receipt.
    pub rustdoc_version: String,
    /// Version reported by Cargo for the published owner.
    pub published_version: String,
    /// Stable API owner category.
    pub owner_kind: String,
    /// Target triple used for this receipt.
    pub target: String,
    /// Whether Cargo defaults were enabled.
    pub default_features: bool,
    /// Local Cargo features enabled for this receipt.
    pub features: Vec<String>,
    /// Rust-derived binding and platform capabilities.
    pub capabilities: Vec<String>,
}

/// Resolve owned profile items to the IDs already emitted by `sdk-docs`.
///
/// The adapter is deliberately signature-aware: two overloads with the same
/// path and kind remain separate entries. Cargo metadata supplies the exact
/// package versions, while the Rustdoc receipt supplies the observed version;
/// a mismatch fails closed before a sidecar can be written.
pub fn project_into_docs(
    data: &crate::DocsData,
    metadata: &Metadata,
    items: impl IntoIterator<Item = OwnedApiItem>,
    profiles: &BTreeMap<ProfileId, ProfileSpec>,
) -> Result<ProfileAvailability, ProfileError> {
    let mut lookup = BTreeMap::<(String, ProjectionKey), String>::new();
    // A public re-export is represented by rustdoc's `use` node in the
    // existing catalog, while the profile extractor quite correctly sees the
    // referenced definition kind (`struct`, `enum`, ...). Keep an additional
    // path/signature index so this representation detail cannot hide a
    // binding-only item. The rendered signature remains part of the key, so
    // overloads are still kept distinct.
    let mut reexport_lookup = BTreeMap::<(String, String, String), Vec<(String, String)>>::new();
    for family in &data.families {
        for item in &family.items {
            let key = ProjectionKey {
                path: item.path.clone(),
                kind: item.kind.clone(),
                signature: item.signature.clone(),
            };
            lookup.insert((family.crate_name.clone(), key.clone()), item.id.clone());
            reexport_lookup
                .entry((
                    family.crate_name.clone(),
                    item.path.clone(),
                    item.signature.clone(),
                ))
                .or_default()
                .push((item.kind.clone(), item.id.clone()));
        }
    }

    let mut entries =
        BTreeMap::<(String, ProjectionKey), BTreeSet<ProfileAvailabilityProfile>>::new();
    for item in items {
        let profile = profiles.get(&item.profile).ok_or_else(|| {
            ProfileError::InvalidRustdoc(format!(
                "profile {} has no Cargo profile specification",
                item.profile.0
            ))
        })?;
        if profile.package != item.rustdoc_package {
            return Err(ProfileError::InvalidRustdoc(format!(
                "profile {} belongs to {}, receipt belongs to {}",
                item.profile.0, profile.package, item.rustdoc_package
            )));
        }
        let owner = api_owner_for_package(metadata, &item.rustdoc_package)?;
        if owner.published_package != item.published_owner {
            return Err(ProfileError::InvalidRustdoc(format!(
                "receipt owner for {} is {}, expected {}",
                item.rustdoc_package, item.published_owner, owner.published_package
            )));
        }
        let rustdoc_package = package_by_name(metadata, &item.rustdoc_package)
            .ok_or_else(|| ProfileError::UnknownPackage(item.rustdoc_package.clone()))?;
        let published_package = package_by_name(metadata, &owner.published_package)
            .ok_or_else(|| ProfileError::UnknownPackage(owner.published_package.clone()))?;
        let rustdoc_version = item.rustdoc_version.clone().ok_or_else(|| {
            ProfileError::InvalidRustdoc(format!(
                "Rustdoc receipt for {} has no crate version",
                item.rustdoc_package
            ))
        })?;
        let expected_rustdoc_version = rustdoc_package.version.to_string();
        if rustdoc_version != expected_rustdoc_version {
            return Err(ProfileError::InvalidRustdoc(format!(
                "Rustdoc receipt for {} reports {}, Cargo reports {}",
                item.rustdoc_package, rustdoc_version, expected_rustdoc_version
            )));
        }
        let crate_name = item
            .key
            .path
            .split_once("::")
            .map_or(item.key.path.as_str(), |(crate_name, _)| crate_name)
            .to_owned();
        let item_id = lookup
            .get(&(crate_name.clone(), item.key.clone()))
            .cloned()
            .or_else(|| {
                let candidates = reexport_lookup.get(&(
                    crate_name.clone(),
                    item.key.path.clone(),
                    item.key.signature.clone(),
                ))?;
                (candidates.len() == 1).then(|| candidates[0].1.clone())
            });
        let Some(item_id) = item_id else {
            return Err(ProfileError::InvalidRustdoc(format!(
                "profile item {} is absent from the generated docs catalog",
                item.key.path
            )));
        };
        let availability = ProfileAvailabilityProfile {
            profile: item.profile,
            rustdoc_package: item.rustdoc_package,
            published_owner: item.published_owner,
            rustdoc_version,
            published_version: published_package.version.to_string(),
            owner_kind: owner_kind_name(owner.kind).into(),
            target: profile.target.clone(),
            default_features: profile.default_features,
            features: profile.features.iter().cloned().collect(),
            capabilities: profile_capabilities(owner.kind, &profile.target),
        };
        entries
            .entry((item_id, item.key))
            .or_default()
            .insert(availability);
    }

    Ok(ProfileAvailability {
        schema: PROFILE_AVAILABILITY_SCHEMA.into(),
        entries: entries
            .into_iter()
            .map(|((item_id, key), profiles)| ProfileAvailabilityEntry {
                item_id,
                path: key.path,
                kind: key.kind,
                signature: key.signature,
                profiles: profiles.into_iter().collect(),
            })
            .collect(),
    })
}

fn profile_capabilities(kind: ApiOwnerKind, target: &str) -> Vec<String> {
    let mut capabilities = BTreeSet::from([if target.starts_with("wasm32") {
        "browser"
    } else {
        "native"
    }
    .to_owned()]);
    capabilities.insert(
        match kind {
            ApiOwnerKind::PublishedRoot => "rust",
            ApiOwnerKind::NapiBinding => "napi",
            ApiOwnerKind::WasmBinding => "wasm",
            ApiOwnerKind::UniFfiBinding => "uniffi",
            ApiOwnerKind::OtherBinding => "binding",
        }
        .to_owned(),
    );
    capabilities.into_iter().collect()
}

fn owner_kind_name(kind: ApiOwnerKind) -> &'static str {
    match kind {
        ApiOwnerKind::PublishedRoot => "publishedRoot",
        ApiOwnerKind::NapiBinding => "napiBinding",
        ApiOwnerKind::WasmBinding => "wasmBinding",
        ApiOwnerKind::UniFfiBinding => "uniFfiBinding",
        ApiOwnerKind::OtherBinding => "otherBinding",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cargo_metadata::MetadataCommand;

    fn metadata() -> Metadata {
        let json = serde_json::json!({
            "packages": [{
                "name": "feature-rich", "version": "0.1.0", "id": "path+file:///fixture#feature-rich@0.1.0",
                "source": null, "dependencies": [], "targets": [],
                "features": {"default": ["json"], "json": ["codec", "dep:serde_json"], "codec": [], "wasm": ["codec", "other/feature"]},
                "manifest_path": "C:/fixture/Cargo.toml", "categories": [], "keywords": [], "readme": null,
                "repository": null, "homepage": null, "documentation": null, "edition": "2021",
                "metadata": null, "links": null, "publish": null, "default_run": null, "rust_version": null
            }],
            "workspace_members": ["path+file:///fixture#feature-rich@0.1.0"],
            "workspace_default_members": ["path+file:///fixture#feature-rich@0.1.0"],
            "resolve": null, "workspace_root": "C:/fixture", "target_directory": "C:/fixture/target",
            "build_directory": null, "metadata": null, "version": 1
        });
        serde_json::from_value(json).expect("valid Cargo metadata fixture")
    }

    #[test]
    fn unsupported_target_is_hard_error() {
        let result = profiles_for_package(
            &metadata(),
            "feature-rich",
            "wasm32-wasip1",
            &BTreeSet::new(),
        );
        assert_eq!(
            result,
            Err(ProfileError::UnsupportedTarget {
                target: "wasm32-wasip1".into()
            })
        );
    }

    #[test]
    fn feature_profiles_include_local_closure_and_all_features() {
        let profiles = profiles_for_package(
            &metadata(),
            "feature-rich",
            "host",
            &BTreeSet::from(["host".into()]),
        )
        .unwrap();
        assert!(profiles
            .iter()
            .any(|profile| profile.default_features && profile.features.is_empty()));
        assert!(profiles
            .iter()
            .any(|profile| profile.features == BTreeSet::from(["json".into(), "codec".into()])));
        assert!(profiles.iter().any(|profile| profile.default_features
            && profile.features == BTreeSet::from(["codec".into(), "json".into(), "wasm".into()])));
    }

    #[test]
    fn profile_ids_are_order_independent_and_distinguish_defaults() {
        let profile = |default_features, features| ProfileSpec {
            package: "p".into(),
            target: "t".into(),
            default_features,
            features,
        };
        let a = profile(false, BTreeSet::from(["b".into(), "a".into()])).id();
        let b = profile(false, BTreeSet::from(["a".into(), "b".into()])).id();
        let default = profile(true, BTreeSet::new()).id();
        assert_eq!(a, b);
        assert_ne!(a, default);
    }

    #[test]
    fn rustdoc_version_must_match_cargo_package_even_without_items() {
        let valid = RustdocObservation {
            format_version: FORMAT_VERSION,
            crate_name: "feature_rich".into(),
            crate_version: Some("0.1.0".into()),
            target: "host".into(),
            includes_private: false,
            item_count: 1,
        };
        validate_rustdoc_version(&metadata(), "feature-rich", &valid).unwrap();
        let mut invalid = valid.clone();
        invalid.crate_version = Some("0.9.0".into());
        assert!(validate_rustdoc_version(&metadata(), "feature-rich", &invalid).is_err());
        invalid.crate_version = None;
        assert!(validate_rustdoc_version(&metadata(), "feature-rich", &invalid).is_err());
    }

    #[test]
    fn merge_retains_overloads_and_unions_availability() {
        let key = |signature: &str| ProjectionKey {
            path: "crate::Thing::run".into(),
            kind: "function".into(),
            signature: signature.into(),
        };
        let merged = merge_projection([
            ProjectedItem {
                key: key("(u8)"),
                profile: ProfileId("one".into()),
            },
            ProjectedItem {
                key: key("(u8)"),
                profile: ProfileId("two".into()),
            },
            ProjectedItem {
                key: key("(String)"),
                profile: ProfileId("one".into()),
            },
        ]);
        assert_eq!(merged.len(), 2);
        assert!(merged
            .iter()
            .any(|item| item.key.signature == "(u8)" && item.profiles.len() == 2));
        assert!(merged
            .iter()
            .any(|item| item.key.signature == "(String)" && item.profiles.len() == 1));
    }

    #[test]
    fn metadata_command_is_the_runtime_source() {
        let command = MetadataCommand::new();
        assert!(!format!("{command:?}").is_empty());
    }

    #[test]
    fn invalid_feature_fails_before_cargo_runs() {
        let profile = ProfileSpec {
            package: "feature-rich".into(),
            target: "host".into(),
            default_features: false,
            features: BTreeSet::from(["missing".into()]),
        };
        let error = execute_profile(
            "missing-Cargo.toml",
            &metadata(),
            &profile,
            &BTreeSet::from(["host".into()]),
            "unused-target",
            "unused-output.json",
        )
        .expect_err("invalid feature must be rejected before spawning Cargo");
        assert_eq!(
            error,
            ProfileError::InvalidFeature {
                package: "feature-rich".into(),
                feature: "missing".into(),
            }
        );
    }

    #[test]
    fn private_binding_keeps_binding_receipt_and_resolves_published_owner() {
        let value = serde_json::json!({
            "packages": [
                {
                    "name": "published-root", "version": "0.1.0", "id": "path+file:///fixture#published-root@0.1.0",
                    "source": null, "dependencies": [], "targets": [], "features": {},
                    "manifest_path": "C:/fixture/Cargo.toml", "categories": [], "keywords": [], "readme": null,
                    "repository": null, "homepage": null, "documentation": null, "edition": "2021",
                    "metadata": null, "links": null, "publish": null, "default_run": null, "rust_version": null
                },
                {
                    "name": "published-root-napi", "version": "0.1.0", "id": "path+file:///fixture#published-root-napi@0.1.0",
                    "source": null,
                    "dependencies": [{"name": "published-root", "source": null, "req": "*", "kind": null, "optional": false, "uses_default_features": true, "features": [], "target": null, "rename": null, "registry": null, "path": "C:/fixture"}],
                    "targets": [], "features": {}, "manifest_path": "C:/fixture/Cargo.toml", "categories": [], "keywords": [], "readme": null,
                    "repository": null, "homepage": null, "documentation": null, "edition": "2021",
                    "metadata": null, "links": null, "publish": [], "default_run": null, "rust_version": null
                }
            ],
            "workspace_members": [], "workspace_default_members": [], "resolve": null,
            "workspace_root": "C:/fixture", "target_directory": "C:/fixture/target", "build_directory": null,
            "metadata": null, "version": 1
        });
        let metadata: Metadata =
            serde_json::from_value(value).expect("valid binding metadata fixture");
        let owner = api_owner_for_package(&metadata, "published-root-napi").unwrap();
        assert_eq!(owner.published_package, "published-root");
        assert_eq!(owner.rustdoc_package, "published-root-napi");
        assert_eq!(owner.kind, ApiOwnerKind::OtherBinding);
    }

    #[test]
    fn real_format_60_fixture_is_accepted_when_requested() {
        let Some(path) = std::env::var_os("RUSTDOC_PROFILE_FIXTURE") else {
            return;
        };
        let observation = observe_rustdoc(path).unwrap();
        assert_eq!(observation.format_version, FORMAT_VERSION);
        assert_eq!(observation.crate_name, "acyclic_actors");
        assert_eq!(observation.crate_version.as_deref(), Some("0.2.0"));
        assert!(!observation.includes_private);
        assert_eq!(observation.target, "x86_64-pc-windows-msvc");
        assert!(observation.item_count > 100);
    }

    #[test]
    fn real_binding_rustdoc_retains_binding_ownership_when_requested() {
        let Some(path) = std::env::var_os("RUSTDOC_BINDING_FIXTURE") else {
            return;
        };
        let owner = ApiOwner {
            published_package: "acyclic-fs".into(),
            rustdoc_package: "acyclic-fs-napi".into(),
            kind: ApiOwnerKind::NapiBinding,
        };
        let items =
            extract_owned_api(path, &owner, ProfileId("binding-host-default".into())).unwrap();
        assert!(!items.is_empty());
        assert!(items.iter().all(|item| {
            item.rustdoc_package == "acyclic-fs-napi" && item.published_owner == "acyclic-fs"
        }));
        assert!(items.iter().any(|item| item.docs_present));
    }

    #[test]
    fn real_binding_top_level_exports_are_documented_when_requested() {
        let Some(path) = std::env::var_os("RUSTDOC_BINDING_FIXTURE") else {
            return;
        };
        let owner = ApiOwner {
            published_package: "acyclic-fs".into(),
            rustdoc_package: "acyclic-fs-napi".into(),
            kind: ApiOwnerKind::NapiBinding,
        };
        let items =
            extract_owned_api(path, &owner, ProfileId("binding-host-default".into())).unwrap();
        let top_level = items
            .iter()
            .filter(|item| item.key.path.matches("::").count() == 1)
            .collect::<Vec<_>>();
        assert!(!top_level.is_empty());
        assert!(top_level.iter().all(|item| item.docs_present));
    }

    #[test]
    fn real_wasm_binding_rustdoc_retains_browser_owned_api_when_requested() {
        let Some(path) = std::env::var_os("RUSTDOC_WASM_BINDING_FIXTURE") else {
            return;
        };
        let owner = ApiOwner {
            published_package: "acyclic-fs".into(),
            rustdoc_package: "acyclic-fs-wasm".into(),
            kind: ApiOwnerKind::WasmBinding,
        };
        let observation = observe_rustdoc(&path).unwrap();
        assert_eq!(observation.format_version, FORMAT_VERSION);
        assert_eq!(observation.crate_name, "acyclic_fs_wasm");
        assert_eq!(observation.crate_version.as_deref(), Some("0.2.0"));
        assert_eq!(observation.target, "wasm32-unknown-unknown");
        let items =
            extract_owned_api(path, &owner, ProfileId("wasm-binding-no-defaults".into())).unwrap();
        assert!(!items.is_empty());
        assert!(items.iter().all(|item| {
            item.rustdoc_package == "acyclic-fs-wasm" && item.published_owner == "acyclic-fs"
        }));
        assert!(items.iter().any(|item| {
            item.key.path.contains("Browser") || item.key.path.contains("browser")
        }));
    }

    #[test]
    fn real_binding_projection_keeps_binding_only_types_when_requested() {
        let Some(path) = std::env::var_os("RUSTDOC_BINDING_FIXTURE") else {
            return;
        };
        let owner = ApiOwner {
            published_package: "acyclic-fs".into(),
            rustdoc_package: "acyclic-fs-napi".into(),
            kind: ApiOwnerKind::NapiBinding,
        };
        let items =
            extract_owned_api(path, &owner, ProfileId("binding-host-default".into())).unwrap();

        // These types belong to the binding crate. A projection through the
        // published core owner would lose the binding namespace entirely.
        assert!(items.iter().any(|item| {
            item.key.kind == "struct"
                && item.key.path.starts_with("acyclic_fs_napi::Native")
                && item.rustdoc_package == "acyclic-fs-napi"
                && item.published_owner == "acyclic-fs"
        }));
    }

    #[test]
    fn projection_sidecar_preserves_binding_identity_versions_and_capabilities() {
        let Some(path) = std::env::var_os("RUSTDOC_BINDING_FIXTURE") else {
            return;
        };
        let Some(metadata_path) = std::env::var_os("CARGO_METADATA_PROFILE_FIXTURE") else {
            return;
        };
        let metadata: Metadata =
            serde_json::from_slice(&fs::read(metadata_path).unwrap()).expect("Cargo metadata");
        let owner = ApiOwner {
            published_package: "acyclic-fs".into(),
            rustdoc_package: "acyclic-fs-napi".into(),
            kind: ApiOwnerKind::NapiBinding,
        };
        let profile = ProfileSpec {
            package: "acyclic-fs-napi".into(),
            target: "x86_64-pc-windows-msvc".into(),
            default_features: true,
            features: BTreeSet::new(),
        };
        let profile_id = profile.id();
        let items = extract_owned_api(path, &owner, profile_id.clone()).unwrap();
        let api_items = items
            .iter()
            .map(|item| crate::ApiItem {
                id: format!("binding:{}", item.key.path),
                parent_id: None,
                name: item.key.path.rsplit("::").next().unwrap().into(),
                kind: item.key.kind.clone(),
                path: item.key.path.clone(),
                signature: item.key.signature.clone(),
                docs: None,
                links: BTreeMap::new(),
                source: None,
                reexport: None,
                reexport_target: None,
            })
            .collect();
        let data = crate::DocsData {
            schema: crate::DATA_SCHEMA_VERSION.into(),
            schema_version: crate::DATA_SCHEMA_VERSION.into(),
            version: "0.2.0".into(),
            channel: crate::Channel::Preview,
            source: crate::SourceInfo {
                released_packages: Vec::new(),
                revision: "a".repeat(40),
                source_state: "working-tree".into(),
                source_sha256: None,
                input_sha256: "a".repeat(64),
                rustdoc_format_versions: vec![FORMAT_VERSION],
                generator: "sdk-docs/test".into(),
            },
            navigation: crate::Navigation {
                entries: Vec::new(),
            },
            packages: crate::PackageCatalog {
                entries: Vec::new(),
            },
            search: crate::SearchIndex {
                entries: Vec::new(),
            },
            families: vec![crate::Family {
                slug: "acyclic-fs-napi".into(),
                title: "acyclic-fs-napi".into(),
                crate_name: "acyclic_fs_napi".into(),
                items: api_items,
                guides: Vec::new(),
            }],
        };
        let profiles = BTreeMap::from([(profile_id, profile)]);
        let sidecar = project_into_docs(&data, &metadata, items, &profiles).unwrap();
        assert_eq!(sidecar.schema, PROFILE_AVAILABILITY_SCHEMA);
        let native_binding = sidecar.entries.iter().find(|entry| {
            entry.path.starts_with("acyclic_fs_napi::Native") && entry.kind == "struct"
        });
        let native_binding = native_binding.expect("binding-only type should be projected");
        assert!(native_binding
            .profiles
            .iter()
            .any(|profile| profile.rustdoc_package == "acyclic-fs-napi"
                && profile.published_owner == "acyclic-fs"
                && profile.rustdoc_version == "0.2.0"
                && profile.published_version == "0.2.0"
                && profile.capabilities == vec!["napi", "native"]));
    }

    #[test]
    fn real_cargo_metadata_drives_binding_owner_profiles_when_requested() {
        let Some(path) = std::env::var_os("CARGO_METADATA_PROFILE_FIXTURE") else {
            return;
        };
        let metadata: Metadata =
            serde_json::from_slice(&fs::read(path).unwrap()).expect("Cargo metadata fixture");
        let napi = api_owner_for_package(&metadata, "acyclic-fs-napi").unwrap();
        assert_eq!(napi.published_package, "acyclic-fs");
        assert_eq!(napi.rustdoc_package, "acyclic-fs-napi");
        assert_eq!(napi.kind, ApiOwnerKind::NapiBinding);
        let wasm = api_owner_for_package(&metadata, "acyclic-fs-wasm").unwrap();
        assert_eq!(wasm.published_package, "acyclic-fs");
        assert_eq!(wasm.kind, ApiOwnerKind::WasmBinding);
        let plugin = api_owner_for_package(&metadata, "acyclic-plugin").unwrap();
        assert_eq!(plugin.published_package, "acyclic-plugin");
        assert_eq!(plugin.rustdoc_package, "acyclic-plugin");
        assert_eq!(plugin.kind, ApiOwnerKind::PublishedRoot);
    }

    #[test]
    fn real_cargo_profiles_have_distinct_public_items_when_requested() {
        let Some(manifest) = std::env::var_os("RUSTDOC_PROFILE_MANIFEST") else {
            return;
        };
        let Some(metadata_path) = std::env::var_os("CARGO_METADATA_PROFILE_FIXTURE") else {
            return;
        };
        let Some(target_dir) = std::env::var_os("RUSTDOC_PROFILE_TARGET_DIR") else {
            return;
        };
        let metadata: Metadata = serde_json::from_slice(&fs::read(metadata_path).unwrap())
            .expect("Cargo metadata fixture");
        let target = "x86_64-pc-windows-msvc";
        let available_targets = BTreeSet::from([target.to_owned()]);
        let profiles =
            profiles_for_package(&metadata, "acyclic-objects", target, &available_targets).unwrap();
        let json = profiles
            .iter()
            .find(|profile| profile.features == BTreeSet::from(["json".into()]))
            .unwrap();
        let all_features = profiles
            .iter()
            .find(|profile| {
                profile.default_features
                    && profile.features.contains("http")
                    && profile.features.contains("local")
            })
            .unwrap();
        let out_dir = std::env::temp_dir().join("rust-docs-profile-receipts");
        fs::create_dir_all(&out_dir).unwrap();
        let first = out_dir.join("objects-json.json");
        let second = out_dir.join("objects-all-features.json");
        let first_observation = execute_profile(
            &manifest,
            &metadata,
            json,
            &available_targets,
            &target_dir,
            &first,
        )
        .unwrap();
        let second_observation = execute_profile(
            &manifest,
            &metadata,
            all_features,
            &available_targets,
            &target_dir,
            &second,
        )
        .unwrap();
        assert_eq!(first_observation.crate_name, "acyclic_objects");
        assert_eq!(second_observation.crate_name, "acyclic_objects");
        assert_ne!(
            local_item_names(&first).unwrap(),
            local_item_names(&second).unwrap()
        );
    }
}
