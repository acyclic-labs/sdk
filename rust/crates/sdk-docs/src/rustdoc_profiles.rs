//! Cargo and Rustdoc owned availability profiles.
//!
//! This module is the small shared boundary between the generator and the
//! documentation projection. Cargo metadata supplies package features,
//! publication, and dependency ownership; Rustdoc format 60 supplies the
//! public items that were actually present for one target/profile. Binding
//! packages therefore retain their own receipt and API items while pointing at
//! the published core package that owns the shared contract.

#![allow(missing_docs)]

use cargo_metadata::{DependencyKind, Metadata, Package, TargetKind};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

mod execution;
pub(crate) use execution::RustdocProducer;
mod receipts;
pub use execution::{
    execute_profile, load_metadata, rustdoc_host, rustdoc_platform_cfgs, rustdoc_targets,
    CargoExecutionContext, LockSelection, RustdocTarget,
};
pub use receipts::{
    extract_owned_api, local_item_names, observe_binary_rustdoc, observe_rustdoc,
    validate_rustdoc_version, OwnedApiItem, RustdocObservation,
};

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

/// Select the browser profile from Cargo declarations without enabling native
/// provider features. The caller evaluates target predicates using rustc cfg.
pub fn browser_profile_for_package(
    package: &Package,
    has_browser_target_dependencies: bool,
    available_targets: &BTreeSet<String>,
) -> Result<Option<ProfileSpec>, ProfileError> {
    if package
        .targets
        .iter()
        .any(|target| target.kind.contains(&TargetKind::ProcMacro))
    {
        return Ok(None);
    }
    let napi = package.metadata.get("napi");
    let own_binding = napi
        .and_then(|value| value.get("wasm-package"))
        .and_then(serde_json::Value::as_str)
        == Some(package.name.as_str());
    let mut features = BTreeSet::new();
    let mut default_features = false;
    if own_binding {
        let napi = napi.expect("binding policy was found");
        if let Some(value) = napi.get("wasm-features") {
            let values = value.as_array().ok_or_else(|| {
                ProfileError::InvalidRustdoc("wasm-features must be an array".into())
            })?;
            for value in values {
                let feature = value.as_str().ok_or_else(|| {
                    ProfileError::InvalidRustdoc("wasm-features must contain strings".into())
                })?;
                if !package.features.contains_key(feature) {
                    return Err(ProfileError::InvalidFeature {
                        package: package.name.to_string(),
                        feature: feature.into(),
                    });
                }
                features.extend(feature_closure(&package.features, feature));
            }
        }
        default_features = !match napi.get("wasm-no-default-features") {
            Some(value) => value.as_bool().ok_or_else(|| {
                ProfileError::InvalidRustdoc("wasm-no-default-features must be a boolean".into())
            })?,
            None => false,
        };
    } else if package.features.contains_key("wasm") {
        features = feature_closure(&package.features, "wasm");
    } else if !has_browser_target_dependencies {
        return Ok(None);
    }
    let target = "wasm32-unknown-unknown";
    if !available_targets.contains(target) {
        return Err(ProfileError::UnsupportedTarget {
            target: target.into(),
        });
    }
    Ok(Some(ProfileSpec {
        package: package.name.to_string(),
        target: target.into(),
        default_features,
        features,
    }))
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

/// Historical documentation keeps the original Cargo defaults while covering
/// declared features. An explicit upstream docs.rs feature policy is required
/// in addition to the default profile; every selected invocation must succeed.
pub fn historical_profiles_for_package(
    metadata: &Metadata,
    package: &str,
    target: &str,
    available_targets: &BTreeSet<String>,
) -> Result<Vec<ProfileSpec>, ProfileError> {
    let mut profiles = profiles_for_package(metadata, package, target, available_targets)?;
    for profile in &mut profiles {
        profile.default_features = true;
    }
    let owner = package_by_name(metadata, package)
        .ok_or_else(|| ProfileError::UnknownPackage(package.to_owned()))?;
    if let Some(policy) = owner.metadata.get("docs").and_then(|docs| docs.get("rs")) {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "kebab-case", deny_unknown_fields)]
        struct DocsRsFeatures {
            #[serde(default)]
            all_features: bool,
            #[serde(default)]
            no_default_features: bool,
            #[serde(default)]
            features: Vec<String>,
        }
        let policy: DocsRsFeatures = serde_json::from_value(policy.clone()).map_err(|error| {
            ProfileError::InvalidRustdoc(format!("invalid historical docs.rs policy: {error}"))
        })?;
        let mut features = BTreeSet::new();
        for feature in policy.features {
            if feature.contains('/') {
                // The existing Cargo invocation validates qualified feature expressions.
                features.insert(feature);
                continue;
            }
            if !owner.features.contains_key(&feature) {
                return Err(ProfileError::InvalidFeature {
                    package: package.to_owned(),
                    feature,
                });
            }
            features.extend(feature_closure(&owner.features, &feature));
        }
        if policy.all_features {
            features.extend(
                owner
                    .features
                    .keys()
                    .filter(|name| name.as_str() != "default")
                    .cloned(),
            );
        }
        profiles.push(ProfileSpec {
            package: package.to_owned(),
            target: target.to_owned(),
            default_features: !policy.no_default_features,
            features,
        });
    }
    profiles.sort_by_key(ProfileSpec::id);
    profiles.dedup_by_key(|profile| profile.id());
    Ok(profiles)
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
    /// Requested Cargo feature expressions enabled for this receipt.
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
mod tests;
