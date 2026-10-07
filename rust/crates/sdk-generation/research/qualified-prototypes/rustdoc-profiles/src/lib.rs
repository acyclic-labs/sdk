//! A small research model for Rust-owned documentation availability.
//!
//! The production generator should eventually obtain these values from Cargo
//! metadata and one Rustdoc JSON receipt per profile.  This crate deliberately
//! has no hand-maintained API or TypeScript feature catalog: Cargo is the
//! source of feature names and Rustdoc is the source of observed items.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::Path;
use std::process::Command;

use rustdoc_types::{Crate as RustdocCrate, FORMAT_VERSION, Visibility};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileError {
    UnknownPackage(String),
    UnknownApiOwner(String),
    AmbiguousApiOwner(String),
    InvalidFeature {
        package: String,
        feature: String,
    },
    UnsupportedTarget {
        target: String,
    },
    InvalidRustdoc(String),
    DuplicateProjection {
        key: ProjectionKey,
        profile: ProfileId,
    },
}

impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPackage(package) => {
                write!(f, "package `{package}` is absent from Cargo metadata")
            }
            Self::UnknownApiOwner(package) => {
                write!(
                    f,
                    "private package `{package}` has no published Rust API owner"
                )
            }
            Self::AmbiguousApiOwner(package) => {
                write!(
                    f,
                    "private package `{package}` has multiple published Rust API owners"
                )
            }
            Self::InvalidFeature { package, feature } => {
                write!(
                    f,
                    "feature `{feature}` is absent from package `{package}` metadata"
                )
            }
            Self::UnsupportedTarget { target } => {
                write!(f, "target `{target}` is not installed/available")
            }
            Self::InvalidRustdoc(message) => write!(f, "invalid rustdoc receipt: {message}"),
            Self::DuplicateProjection { key, profile } => {
                write!(
                    f,
                    "duplicate projection {:?} for profile {}",
                    key, profile.0
                )
            }
        }
    }
}

impl std::error::Error for ProfileError {}

/// The subset of `cargo metadata --format-version 1` needed to derive profiles.
#[derive(Debug, Clone, Deserialize)]
pub struct CargoMetadata {
    pub packages: Vec<CargoPackage>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CargoPackage {
    pub name: String,
    #[serde(default)]
    pub features: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub dependencies: Vec<CargoDependency>,
    #[serde(default)]
    pub targets: Vec<CargoTarget>,
    #[serde(default)]
    pub publish: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CargoDependency {
    pub name: String,
    #[serde(default)]
    pub package: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CargoTarget {
    pub kind: Vec<String>,
    #[serde(default)]
    pub crate_types: Vec<String>,
}

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
    /// The package whose Rust API contract owns the binding's public types.
    pub published_package: String,
    /// The package whose Rustdoc receipt contributes the actual API items.
    pub rustdoc_package: String,
    pub kind: ApiOwnerKind,
}

/// Resolve a publishable Rust API owner from Cargo metadata. Private FFI
/// packages are attached to their sole publishable Rust dependency; a private
/// package with zero or multiple such dependencies fails closed. This keeps
/// bindings in the same availability graph without inventing a second JSON
/// ownership manifest.
pub fn api_owner_for_package(
    metadata: &CargoMetadata,
    package: &str,
) -> Result<ApiOwner, ProfileError> {
    let current = metadata
        .packages
        .iter()
        .find(|candidate| candidate.name == package)
        .ok_or_else(|| ProfileError::UnknownPackage(package.to_owned()))?;
    let published = |candidate: &&CargoPackage| {
        candidate
            .publish
            .as_ref()
            .is_none_or(|value| !value.is_empty())
    };
    if published(&current) {
        return Ok(ApiOwner {
            published_package: package.to_owned(),
            rustdoc_package: package.to_owned(),
            kind: ApiOwnerKind::PublishedRoot,
        });
    }

    let owners = current
        .dependencies
        .iter()
        .filter(|dependency| dependency.kind() != Some("dev"))
        .filter_map(|dependency| {
            let name = dependency.package.as_deref().unwrap_or(&dependency.name);
            metadata.packages.iter().find(|candidate| {
                candidate.name == name
                    && published(candidate)
                    && current.name.starts_with(&format!("{}-", candidate.name))
            })
        })
        .collect::<Vec<_>>();
    let owner_name = if owners.len() == 1 {
        owners[0].name.clone()
    } else if owners.is_empty() {
        let fallback = current
            .dependencies
            .iter()
            .filter(|dependency| dependency.kind() != Some("dev"))
            .filter_map(|dependency| {
                let name = dependency.package.as_deref().unwrap_or(&dependency.name);
                metadata
                    .packages
                    .iter()
                    .find(|candidate| candidate.name == name && published(candidate))
            })
            .collect::<Vec<_>>();
        let [owner] = fallback.as_slice() else {
            return if fallback.is_empty() {
                Err(ProfileError::UnknownApiOwner(package.to_owned()))
            } else {
                Err(ProfileError::AmbiguousApiOwner(package.to_owned()))
            };
        };
        owner.name.clone()
    } else {
        return Err(ProfileError::AmbiguousApiOwner(package.to_owned()));
    };
    let kind = if current
        .dependencies
        .iter()
        .any(|dependency| dependency.name == "napi" || dependency.name == "napi-derive")
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
        .any(|dependency| dependency.name == "wasm-bindgen" || dependency.name == "js-sys")
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

impl CargoDependency {
    fn kind(&self) -> Option<&str> {
        self.kind.as_deref()
    }
}

/// A Cargo compilation choice.  `features` contains explicit feature names;
/// `default_features` is kept separately because it changes Cargo semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileSpec {
    pub package: String,
    pub target: String,
    pub default_features: bool,
    pub features: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProfileId(pub String);

impl ProfileSpec {
    /// Encode every byte, rather than relying on separators or case-sensitive
    /// filesystem names.  The identifier is therefore stable and injective.
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

/// Derive a bounded, deterministic profile set from the package's actual Cargo
/// feature map.  Every returned profile is intended to be compiled; a compile
/// failure is an error in the release profile, never a reason to omit an API.
pub fn profiles_for_package(
    metadata: &CargoMetadata,
    package: &str,
    target: &str,
    available_targets: &BTreeSet<String>,
) -> Result<Vec<ProfileSpec>, ProfileError> {
    if !available_targets.contains(target) {
        return Err(ProfileError::UnsupportedTarget {
            target: target.to_owned(),
        });
    }
    let package_metadata = metadata
        .packages
        .iter()
        .find(|candidate| candidate.name == package)
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
            // Cargo's `--all-features` retains the package default feature set;
            // this profile therefore exercises the maximal valid feature union.
            default_features: true,
            features: declared,
        });
    }

    profiles.sort_by_key(ProfileSpec::id);
    profiles.dedup_by_key(|profile| profile.id());
    Ok(profiles)
}

/// Execute one profile through Cargo and retain the exact Rustdoc receipt it
/// produced.  This is intentionally a process boundary: Cargo resolves
/// optional dependencies and target cfgs, while Rustdoc remains the authority
/// for which public items actually exist. Invalid features and unavailable
/// targets fail before a receipt can be published.
pub fn execute_profile(
    manifest: impl AsRef<Path>,
    metadata: &CargoMetadata,
    profile: &ProfileSpec,
    available_targets: &BTreeSet<String>,
    target_dir: impl AsRef<Path>,
    output_json: impl AsRef<Path>,
) -> Result<RustdocObservation, ProfileError> {
    if !available_targets.contains(&profile.target) {
        return Err(ProfileError::UnsupportedTarget {
            target: profile.target.clone(),
        });
    }
    let package = metadata
        .packages
        .iter()
        .find(|candidate| candidate.name == profile.package)
        .ok_or_else(|| ProfileError::UnknownPackage(profile.package.clone()))?;
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
    let mut command = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command
        .arg("rustdoc")
        .arg("--manifest-path")
        .arg(manifest.as_ref())
        .arg("--package")
        .arg(&profile.package)
        .arg("--lib")
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

    let filename = format!("{}.json", profile.package.replace('-', "_"));
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
    observe_rustdoc(output_json)
}

/// Return names of local Rustdoc items.  This deliberately uses the receipt's
/// crate ownership, rather than path prefixes or a handwritten API list.
pub fn local_item_names(path: impl AsRef<Path>) -> Result<BTreeSet<String>, ProfileError> {
    let bytes = fs::read(path.as_ref()).map_err(|error| {
        ProfileError::InvalidRustdoc(format!("{}: {error}", path.as_ref().display()))
    })?;
    let receipt: RustdocCrate = serde_json::from_slice(&bytes).map_err(|error| {
        ProfileError::InvalidRustdoc(format!("{}: {error}", path.as_ref().display()))
    })?;
    if receipt.format_version != FORMAT_VERSION || receipt.includes_private {
        return Err(ProfileError::InvalidRustdoc(
            "item-name projection requires a public format-60 receipt".to_owned(),
        ));
    }
    Ok(receipt
        .index
        .values()
        .filter(|item| item.crate_id == 0)
        .filter_map(|item| item.name.clone())
        .collect())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedApiItem {
    /// The binding package remains the receipt source, even when it points at
    /// a published core owner for shared contract types.
    pub rustdoc_package: String,
    pub published_owner: String,
    pub profile: ProfileId,
    pub key: ProjectionKey,
    pub docs_present: bool,
}

/// Extract the binding crate's own public Rust API from its real Rustdoc
/// receipt. This intentionally does not replace binding items with the core
/// crate's projection: generated getters, opaque IDs, and strong enums belong
/// to the binding package and must remain visible in its API metadata.
pub fn extract_owned_api(
    path: impl AsRef<Path>,
    owner: &ApiOwner,
    profile: ProfileId,
) -> Result<Vec<OwnedApiItem>, ProfileError> {
    if profile.0.is_empty() {
        return Err(ProfileError::InvalidRustdoc(
            "binding API extraction requires a non-empty profile identity".to_owned(),
        ));
    }
    let bytes = fs::read(path.as_ref()).map_err(|error| {
        ProfileError::InvalidRustdoc(format!("{}: {error}", path.as_ref().display()))
    })?;
    let receipt: RustdocCrate = serde_json::from_slice(&bytes).map_err(|error| {
        ProfileError::InvalidRustdoc(format!("{}: {error}", path.as_ref().display()))
    })?;
    if receipt.format_version != FORMAT_VERSION || receipt.includes_private {
        return Err(ProfileError::InvalidRustdoc(
            "binding API extraction requires a public format-60 receipt".to_owned(),
        ));
    }
    let root = receipt.index.get(&receipt.root).ok_or_else(|| {
        ProfileError::InvalidRustdoc("binding receipt root is absent from its index".to_owned())
    })?;
    let expected_crate_name = owner.rustdoc_package.replace('-', "_");
    if root.name.as_deref() != Some(expected_crate_name.as_str()) {
        return Err(ProfileError::InvalidRustdoc(format!(
            "binding receipt belongs to {:?}, expected {}",
            root.name, expected_crate_name
        )));
    }
    let mut items = receipt
        .index
        .values()
        .filter(|item| item.crate_id == 0 && item.visibility == Visibility::Public)
        .filter_map(|item| {
            let summary = receipt.paths.get(&item.id)?;
            let path = summary.path.join("::");
            let kind = format!("{:?}", summary.kind).to_lowercase();
            let signature = format!("{:?}", item.inner);
            Some(OwnedApiItem {
                rustdoc_package: owner.rustdoc_package.clone(),
                published_owner: owner.published_package.clone(),
                profile: profile.clone(),
                key: ProjectionKey {
                    path,
                    kind,
                    signature,
                },
                docs_present: item.docs.is_some(),
            })
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(items)
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
                // `dep:foo` enables an optional dependency, while `foo/bar`
                // refers to another package's feature. Neither is a local
                // Cargo feature profile choice.
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
    let path = path.as_ref();
    let bytes = fs::read(path)
        .map_err(|error| ProfileError::InvalidRustdoc(format!("{}: {error}", path.display())))?;
    let receipt: RustdocCrate = serde_json::from_slice(&bytes)
        .map_err(|error| ProfileError::InvalidRustdoc(format!("{}: {error}", path.display())))?;
    if receipt.format_version != FORMAT_VERSION {
        return Err(ProfileError::InvalidRustdoc(format!(
            "format {} is unsupported; expected {FORMAT_VERSION}",
            receipt.format_version
        )));
    }
    if receipt.includes_private {
        return Err(ProfileError::InvalidRustdoc(
            "private-item receipts cannot establish published availability".to_owned(),
        ));
    }
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

/// Merge Rustdoc projections while retaining signatures as part of identity.
/// This allows two overloaded methods to accumulate different availability
/// sets instead of collapsing into one path/name entry.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata() -> CargoMetadata {
        CargoMetadata {
            packages: vec![CargoPackage {
                name: "feature-rich".into(),
                features: BTreeMap::from([
                    ("default".into(), vec!["json".into()]),
                    ("json".into(), vec!["codec".into(), "dep:serde_json".into()]),
                    ("codec".into(), vec![]),
                    ("wasm".into(), vec!["codec".into(), "other/feature".into()]),
                ]),
                dependencies: Vec::new(),
                targets: Vec::new(),
                publish: None,
            }],
        }
    }

    #[test]
    fn unsupported_target_is_a_hard_error() {
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
    fn invalid_feature_is_a_hard_error_before_cargo() {
        let profile = ProfileSpec {
            package: "feature-rich".into(),
            target: "x86_64-pc-windows-msvc".into(),
            default_features: false,
            features: BTreeSet::from(["does-not-exist".into()]),
        };
        let error = execute_profile(
            "missing-Cargo.toml",
            &metadata(),
            &profile,
            &BTreeSet::from(["x86_64-pc-windows-msvc".into()]),
            "unused-target",
            "unused-output.json",
        )
        .unwrap_err();
        assert_eq!(
            error,
            ProfileError::InvalidFeature {
                package: "feature-rich".into(),
                feature: "does-not-exist".into()
            }
        );
    }

    #[test]
    fn binding_owner_is_derived_from_publish_and_dependency_metadata() {
        let metadata = CargoMetadata {
            packages: vec![
                CargoPackage {
                    name: "published-root".into(),
                    features: BTreeMap::new(),
                    dependencies: Vec::new(),
                    targets: Vec::new(),
                    publish: None,
                },
                CargoPackage {
                    name: "published-root-wasm".into(),
                    features: BTreeMap::new(),
                    dependencies: vec![
                        CargoDependency {
                            name: "published-root".into(),
                            package: None,
                            kind: None,
                        },
                        CargoDependency {
                            name: "wasm-bindgen".into(),
                            package: None,
                            kind: None,
                        },
                    ],
                    targets: Vec::new(),
                    publish: Some(Vec::new()),
                },
            ],
        };
        assert_eq!(
            api_owner_for_package(&metadata, "published-root-wasm").unwrap(),
            ApiOwner {
                published_package: "published-root".into(),
                rustdoc_package: "published-root-wasm".into(),
                kind: ApiOwnerKind::WasmBinding,
            }
        );
    }

    #[test]
    fn features_are_derived_with_local_closure_and_all_features() {
        let profiles = profiles_for_package(
            &metadata(),
            "feature-rich",
            "x86_64-pc-windows-msvc",
            &BTreeSet::from(["x86_64-pc-windows-msvc".into()]),
        )
        .unwrap();
        assert!(
            profiles
                .iter()
                .any(|profile| profile.default_features && profile.features.is_empty())
        );
        assert!(
            profiles
                .iter()
                .any(|profile| profile.features == BTreeSet::from(["json".into(), "codec".into()]))
        );
        assert!(profiles.iter().any(|profile| {
            profile.default_features
                && profile.features
                    == BTreeSet::from(["codec".into(), "json".into(), "wasm".into()])
        }));
    }

    #[test]
    fn profile_ids_are_order_independent_and_distinguish_defaults() {
        let mut first = BTreeSet::new();
        first.insert("b".into());
        first.insert("a".into());
        let mut second = BTreeSet::new();
        second.insert("a".into());
        second.insert("b".into());
        let a = ProfileSpec {
            package: "p".into(),
            target: "t".into(),
            default_features: false,
            features: first,
        }
        .id();
        let b = ProfileSpec {
            package: "p".into(),
            target: "t".into(),
            default_features: false,
            features: second,
        }
        .id();
        let default = ProfileSpec {
            package: "p".into(),
            target: "t".into(),
            default_features: true,
            features: BTreeSet::new(),
        }
        .id();
        assert_eq!(a, b);
        assert_ne!(a, default);
    }

    #[test]
    fn merge_retains_overloads_and_unions_availability() {
        let one = ProfileId("one".into());
        let two = ProfileId("two".into());
        let key = |signature: &str| ProjectionKey {
            path: "crate::Thing::run".into(),
            kind: "function".into(),
            signature: signature.into(),
        };
        let merged = merge_projection([
            ProjectedItem {
                key: key("(u8)"),
                profile: one.clone(),
            },
            ProjectedItem {
                key: key("(u8)"),
                profile: two.clone(),
            },
            ProjectedItem {
                key: key("(String)"),
                profile: one.clone(),
            },
        ]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].profiles.len(), 1);
        assert_eq!(merged[1].profiles.len(), 2);
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
    fn real_binding_rustdoc_items_retain_binding_ownership_when_requested() {
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
    fn real_cargo_metadata_drives_feature_profiles_when_requested() {
        let Some(path) = std::env::var_os("CARGO_METADATA_PROFILE_FIXTURE") else {
            return;
        };
        let metadata: CargoMetadata = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        let target = "x86_64-pc-windows-msvc";
        let profiles = profiles_for_package(
            &metadata,
            "acyclic-fs",
            target,
            &BTreeSet::from([target.to_owned()]),
        )
        .unwrap();
        let s3 = profiles
            .iter()
            .find(|profile| profile.features.contains("s3-http"))
            .expect("Cargo metadata feature should produce an explicit profile");
        assert!(s3.features.contains("distributed"));
        assert!(profiles.iter().any(|profile| profile.default_features));
    }

    #[test]
    fn real_binding_packages_resolve_to_published_rust_owners_when_requested() {
        let Some(path) = std::env::var_os("CARGO_METADATA_PROFILE_FIXTURE") else {
            return;
        };
        let metadata: CargoMetadata =
            serde_json::from_slice(&fs::read(Path::new(&path)).unwrap()).unwrap();
        let napi = api_owner_for_package(&metadata, "acyclic-fs-napi").unwrap();
        assert_eq!(napi.published_package, "acyclic-fs");
        assert_eq!(napi.rustdoc_package, "acyclic-fs-napi");
        assert_eq!(napi.kind, ApiOwnerKind::NapiBinding);
        let wasm = api_owner_for_package(&metadata, "acyclic-fs-wasm").unwrap();
        assert_eq!(wasm.published_package, "acyclic-fs");
        assert_eq!(wasm.kind, ApiOwnerKind::WasmBinding);
        let actors_napi = api_owner_for_package(&metadata, "acyclic-actors-napi").unwrap();
        assert_eq!(actors_napi.published_package, "acyclic-actors");
        assert_eq!(actors_napi.kind, ApiOwnerKind::NapiBinding);
        let actors_wasm = api_owner_for_package(&metadata, "acyclic-actors-wasm").unwrap();
        assert_eq!(actors_wasm.published_package, "acyclic-actors");
        assert_eq!(actors_wasm.kind, ApiOwnerKind::WasmBinding);
    }

    #[test]
    fn real_cargo_profiles_produce_distinct_rustdoc_availability_when_requested() {
        let Some(manifest) = std::env::var_os("RUSTDOC_PROFILE_MANIFEST") else {
            return;
        };
        let Some(metadata_path) = std::env::var_os("CARGO_METADATA_PROFILE_FIXTURE") else {
            return;
        };
        let Some(target_dir) = std::env::var_os("RUSTDOC_PROFILE_TARGET_DIR") else {
            return;
        };
        let metadata: CargoMetadata =
            serde_json::from_slice(&fs::read(Path::new(&metadata_path)).unwrap()).unwrap();
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
        let out_dir = std::env::temp_dir().join("rust-docs-profile-prototype-receipts");
        fs::create_dir_all(&out_dir).unwrap();
        let first = out_dir.join("objects-json.json");
        let second = out_dir.join("objects-all-features.json");
        let first_observation = execute_profile(
            Path::new(&manifest),
            &metadata,
            json,
            &available_targets,
            Path::new(&target_dir),
            &first,
        )
        .unwrap();
        let second_observation = execute_profile(
            Path::new(&manifest),
            &metadata,
            all_features,
            &available_targets,
            Path::new(&target_dir),
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
