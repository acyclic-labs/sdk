use super::{package_by_name, ApiOwner, ProfileError, ProfileId, ProjectionKey};
use crate::public_api;
use cargo_metadata::Metadata;
use rustdoc_types::{Crate as RustdocCrate, Visibility, FORMAT_VERSION};
use std::{collections::BTreeSet, path::Path};

pub fn local_item_names(path: impl AsRef<Path>) -> Result<BTreeSet<String>, ProfileError> {
    let (_, receipt) = read_public_receipt(path.as_ref())?;
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

/// Extracts the package-owned API for its actual Cargo target name.
pub fn extract_owned_api(
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
    let (raw, receipt) = read_public_receipt(path.as_ref())?;
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
    let public_items = public_api::extract(path.as_ref(), &raw, &receipt).map_err(|error| {
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
            // Rustdoc format 60 omits derive macros from its path index. Their
            // typed local item and public-api occurrence still identify the
            // public macro exactly; retain its profile availability.
            let kind = if matches!(item.inner, rustdoc_types::ItemEnum::ProcMacro(_)) {
                crate::kind_name(item.inner.item_kind()).to_owned()
            } else {
                format!("{:?}", receipt.paths.get(&public_item.id)?.kind).to_lowercase()
            };
            Some(OwnedApiItem {
                rustdoc_package: owner.rustdoc_package.clone(),
                published_owner: owner.published_package.clone(),
                profile: profile.clone(),
                key: ProjectionKey {
                    path: public_item.path.join("::"),
                    kind,
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

fn read_public_receipt(
    path: impl AsRef<Path>,
) -> Result<(std::rc::Rc<[u8]>, std::rc::Rc<RustdocCrate>), ProfileError> {
    let path = path.as_ref();
    let bytes = crate::read_rustdoc(path)
        .map_err(|error| ProfileError::InvalidRustdoc(format!("{}: {error}", path.display())))?;
    let receipt = crate::render_cache::decode(std::rc::Rc::clone(&bytes))
        .map_err(|error| ProfileError::InvalidRustdoc(format!("{}: {error}", path.display())))?;
    if receipt.format_version != FORMAT_VERSION || receipt.includes_private {
        return Err(ProfileError::InvalidRustdoc(
            "public Rustdoc extraction requires format 60 with includes_private=false".to_owned(),
        ));
    }
    Ok((bytes, receipt))
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
    let (_, receipt) = read_public_receipt(path)?;
    observe_receipt(&receipt)
}

/// Read a native binary receipt without exposing its private implementation as API.
pub fn observe_binary_rustdoc(path: impl AsRef<Path>) -> Result<RustdocObservation, ProfileError> {
    let receipt = crate::render_cache::decode(
        crate::read_rustdoc(path.as_ref())
            .map_err(|e| ProfileError::InvalidRustdoc(e.to_string()))?,
    )
    .map_err(|e| ProfileError::InvalidRustdoc(e.to_string()))?;
    if receipt.format_version != FORMAT_VERSION {
        return Err(ProfileError::InvalidRustdoc(
            "binary Rustdoc requires format 60".into(),
        ));
    }
    observe_receipt(&receipt)
}

fn observe_receipt(receipt: &RustdocCrate) -> Result<RustdocObservation, ProfileError> {
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
        crate_version: receipt.crate_version.clone(),
        target: receipt.target.triple.clone(),
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
