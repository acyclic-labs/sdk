//! Validated publication shared by semantic request admission and the Actors host.

use crate::{
    compatibility::{CompatibilityManifest, PublicationUpload},
    live_selection::{Artifact, DeploymentId, Error, Version},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[cfg(test)]
mod tests;

fn validate_compatibility(manifest: &CompatibilityManifest) -> Result<(), Error> {
    let mut schemas = manifest.messages.iter().collect::<Vec<_>>();
    for (name, rpc) in &manifest.rpcs {
        schemas.extend([(name, &rpc.request), (name, &rpc.reply)]);
    }
    for (name, schema) in schemas {
        #[cfg(test)]
        tests::DESCRIPTOR_DECODES.with(|decodes| decodes.set(decodes.get() + 1));
        let pool = prost_reflect::DescriptorPool::decode(schema.descriptor_set.as_slice())
            .map_err(|_| Error::Invalid("schema descriptor"))?;
        if name.is_empty() || pool.get_message_by_name(&schema.message).is_none() {
            return Err(Error::Invalid("public schema root"));
        }
    }
    let mut scopes = std::collections::BTreeSet::new();
    for peer in &manifest.peers {
        peer.deployment.validate()?;
        if !scopes.insert(&peer.deployment) || peer.last.is_some_and(|last| last < peer.first) {
            return Err(Error::Invalid("peer range"));
        }
    }
    if !manifest.handoff.reads.contains(&manifest.handoff.writes)
        || manifest.handoff.writes.name.is_empty()
        || manifest.handoff.writes.version.is_empty()
        || manifest
            .handoff
            .reads
            .iter()
            .chain(&manifest.handoff.journal_reads)
            .any(|codec| codec.name.is_empty() || codec.version.is_empty())
        || manifest.handoff.durable_await && manifest.handoff.journal_reads.is_empty()
        || manifest
            .adapters
            .iter()
            .any(|adapter| adapter.from == adapter.to || adapter.code == [0; 32])
    {
        return Err(Error::Invalid("handoff or adapter declaration"));
    }
    Ok(())
}

/// Immutable upload. Its monotonically assigned number uses expected_version
/// as the conditional predecessor, independently of the selected code's age.
///
/// A value exists only after validation: [`Publication::new`] and decoding both
/// validate once and keep the identity, so every later read is O(1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Publication {
    upload: PublicationUpload,
    #[serde(skip)]
    schema: [u8; 32],
    #[serde(skip)]
    version: Version,
}

impl<'de> Deserialize<'de> for Publication {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let upload = PublicationUpload::deserialize(deserializer)?;
        Self::new(
            upload.deployment,
            upload.expected_version,
            upload.artifacts,
            upload.compatibility,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl Publication {
    /// Validate the deployment scope, every schema and retained-work declaration,
    /// and each artifact's schema, then fix the version identity. Cloud assigns
    /// it only after checking the predecessor.
    pub fn new(
        deployment: DeploymentId,
        expected_version: u64,
        artifacts: BTreeMap<String, Artifact>,
        compatibility: CompatibilityManifest,
    ) -> Result<Self, Error> {
        deployment.validate()?;
        validate_compatibility(&compatibility)?;
        let upload = PublicationUpload {
            deployment,
            expected_version,
            artifacts,
            compatibility,
        };
        let (schema, version) = upload.identity()?;
        Ok(Self {
            upload,
            schema,
            version,
        })
    }

    /// Content identity for this exact immutable publication.
    pub fn version(&self) -> &Version {
        &self.version
    }

    /// Identity of the declared RPC requests/replies and durable messages.
    pub fn schema_digest(&self) -> [u8; 32] {
        self.schema
    }

    /// Authenticated deployment scope.
    pub fn deployment(&self) -> &DeploymentId {
        &self.upload.deployment
    }

    /// Zero only before the first immutable upload.
    pub fn expected_version(&self) -> u64 {
        self.upload.expected_version
    }

    /// Namespace/service identity to immutable artifact/configuration selection.
    pub fn artifacts(&self) -> &BTreeMap<String, Artifact> {
        &self.upload.artifacts
    }

    /// Rust-owned contract submitted for Cloud's upload gate.
    pub fn compatibility(&self) -> &CompatibilityManifest {
        &self.upload.compatibility
    }
}
