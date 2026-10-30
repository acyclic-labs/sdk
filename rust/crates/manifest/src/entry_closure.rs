//! Versioned selected-entry identity; Objects supplies storage and authority.
use crate::{DeploymentManifest, Entry, EntryAsset, Environment, ManifestError, select_entry};
use acyclic_stream::{OwnedBytes, SealedBytes};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// An immutable Objects identity and its exact byte admission, never authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntryObject {
    /// Canonical BLAKE3-256 identity of stored bytes.
    pub digest: [u8; 32],
    /// Exact byte length, used as the Objects read admission.
    pub length: u64,
}

impl EntryObject {
    fn sealed(bytes: &SealedBytes) -> Self {
        Self {
            digest: *bytes.digest(),
            length: bytes.bytes().len() as u64,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClosureV1 {
    schema_version: u32,
    manifest: DeploymentManifest,
    artifact: EntryObject,
    // Ordered exactly as the selected entry's asset declarations.
    assets: Vec<EntryObject>,
}

fn invalid(rule: &str) -> ManifestError {
    ManifestError::invalid("entryClosure", rule)
}

fn selected(manifest: &DeploymentManifest) -> Result<(&Environment, &Entry), ManifestError> {
    let environment = manifest
        .environments
        .first()
        .ok_or_else(|| invalid("single-selected-entry"))?;
    let entry = environment
        .entries
        .first()
        .ok_or_else(|| invalid("single-selected-entry"))?;
    Ok((environment, entry))
}

/// Validated bytes ready for the existing Objects sealed batch and closure put.
/// Payloads move without copying; each seal computes its content digest once.
#[derive(Debug)]
pub struct PreparedEntry {
    closure: SealedBytes,
    payloads: Vec<SealedBytes>,
}

impl PreparedEntry {
    /// Admit one selected entry and all its modules before any publication.
    /// Selection consumes configuration, and payload ownership is retained
    /// unchanged through SHA-256 verification and canonical content sealing.
    pub fn new(
        manifest: DeploymentManifest,
        environment: &str,
        entry: &str,
        artifact: OwnedBytes,
        files: Vec<(String, OwnedBytes)>,
    ) -> Result<Self, ManifestError> {
        let manifest = select_entry(manifest, environment, entry)?;
        let (_, entry) = selected(&manifest)?;
        entry.verify_artifact_bytes(&artifact)?;
        let mut supplied = HashMap::with_capacity(files.len());
        for (path, bytes) in files {
            if supplied.insert(path, bytes).is_some() {
                return Err(ManifestError::invalid("assets", "duplicate-asset"));
            }
        }
        let mut payloads = Vec::with_capacity(1 + entry.assets.len());
        let sealed_artifact = SealedBytes::new(artifact);
        let artifact = EntryObject::sealed(&sealed_artifact);
        payloads.push(sealed_artifact);
        let mut assets = Vec::with_capacity(entry.assets.len());
        for asset in &entry.assets {
            let bytes = supplied
                .remove(&asset.path)
                .ok_or_else(|| ManifestError::invalid("assets", "missing-asset"))?;
            asset.verify_bytes(&bytes)?;
            let sealed = SealedBytes::new(bytes);
            assets.push(EntryObject::sealed(&sealed));
            payloads.push(sealed);
        }
        if !supplied.is_empty() {
            return Err(ManifestError::invalid("assets", "undeclared-asset"));
        }
        let record = ClosureV1 {
            schema_version: 1,
            manifest,
            artifact,
            assets,
        };
        let closure = serde_json::to_vec(&record).map_err(ManifestError::json)?;
        Ok(Self {
            closure: SealedBytes::new(closure.into()),
            payloads,
        })
    }

    /// Move the descriptor and payload seals to the original Objects provider.
    #[must_use]
    pub fn into_parts(self) -> (SealedBytes, Vec<SealedBytes>) {
        (self.closure, self.payloads)
    }
}

/// Selected configuration and constant-work lookup of its declared modules.
/// The original account-scoped Objects provider remains the only authority.
#[derive(Debug)]
pub struct EntryClosure {
    record: ClosureV1,
    modules: HashMap<String, usize>,
}

impl EntryClosure {
    /// Largest canonical v1 encoding permitted by the existing manifest
    /// contract, plus its fixed-width Objects references. This introduces no
    /// additional restriction on admitted manifests or payload byte lengths.
    pub const MAX_ENCODED_BYTES: usize = {
        // Each digest byte occupies at most three decimal digits; a u64
        // occupies at most twenty. Include the original field punctuation.
        let object = br#"{"digest":[],"length":}"#.len() + 32 * 3 + 31 + 20;
        br#"{"schemaVersion":1,"manifest":,"artifact":,"assets":[]}"#.len()
            + crate::MAX_JSON_BYTES
            + object * (crate::MAX_ITEMS + 1)
            + crate::MAX_ITEMS.saturating_sub(1)
    };

    /// Admit descriptor bytes before typed JSON allocation or an Objects read.
    pub fn admit_length(length: u64) -> Result<(), ManifestError> {
        if length > Self::MAX_ENCODED_BYTES as u64 {
            return Err(invalid("encoding-size"));
        }
        Ok(())
    }

    /// Admit a stored descriptor under the originally selected environment and
    /// entry. Version, selection and reference shape are checked once here.
    pub fn decode(bytes: &[u8], environment: &str, entry: &str) -> Result<Self, ManifestError> {
        Self::admit_length(bytes.len() as u64)?;
        let mut record: ClosureV1 = serde_json::from_slice(bytes).map_err(ManifestError::json)?;
        if record.schema_version != 1 {
            return Err(invalid("unsupported-version"));
        }
        let (environment_record, _) = selected(&record.manifest)?;
        if record.manifest.environments.len() != 1 || environment_record.entries.len() != 1 {
            return Err(invalid("single-selected-entry"));
        }
        let width = |manifest: &DeploymentManifest| {
            let (env, _) = selected(manifest)?;
            Ok::<_, ManifestError>([
                env.bindings.len(),
                env.grants.len(),
                env.secrets.len(),
                env.crons.len(),
                env.assets.len(),
                env.routes.len(),
                env.worker_routes.len(),
            ])
        };
        let original_width = width(&record.manifest)?;
        record.manifest = select_entry(record.manifest, environment, entry)?;
        if width(&record.manifest)? != original_width {
            return Err(invalid("unselected-configuration"));
        }
        let (_, entry) = selected(&record.manifest)?;
        if record.assets.len() != entry.assets.len()
            || record
                .assets
                .iter()
                .zip(&entry.assets)
                .any(|(object, asset)| object.length != asset.size)
        {
            return Err(invalid("asset-references"));
        }
        let modules = entry
            .assets
            .iter()
            .enumerate()
            .map(|(index, asset)| (asset.path.clone(), index))
            .collect();
        Ok(Self { record, modules })
    }

    /// The exact selected configuration; unrelated bindings are absent.
    #[must_use]
    pub const fn manifest(&self) -> &DeploymentManifest {
        &self.record.manifest
    }

    /// Immutable entry module identity and read admission.
    #[must_use]
    pub const fn artifact(&self) -> EntryObject {
        self.record.artifact
    }

    /// Exact payload edges that the descriptor must retain through Objects.
    pub fn objects(&self) -> impl Iterator<Item = EntryObject> + '_ {
        std::iter::once(self.record.artifact).chain(self.record.assets.iter().copied())
    }

    /// Verify entry bytes read through the original Objects provider.
    pub fn verify_artifact(&self, bytes: &[u8]) -> Result<(), ManifestError> {
        selected(&self.record.manifest)?
            .1
            .verify_artifact_bytes(bytes)
    }

    /// Index one selected module; no deployment, history or sibling scan.
    pub fn module(&self, path: &str) -> Result<(&EntryAsset, EntryObject), ManifestError> {
        let index = *self
            .modules
            .get(path)
            .ok_or_else(|| ManifestError::invalid("assets", "undeclared-asset"))?;
        selected(&self.record.manifest)?
            .1
            .assets
            .get(index)
            .zip(self.record.assets.get(index))
            .map(|(asset, reference)| (asset, *reference))
            .ok_or_else(|| invalid("asset-references"))
    }
}
