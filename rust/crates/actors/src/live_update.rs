//! Public live-update contract. Cloud admits these values against the original
//! deployment, membership, retained work and Streams authorities. None of these
//! serializable observations is an execution grant.
//!
//! All documents use the same strict v1 encoding in uploads, volume files and
//! call envelopes. Epochs and version numbers are comparable only within their
//! deployment. Selection changes use the original conditional Stream commit.

use acyclic_stream::{CommitCondition, CommitId, StreamPath};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, num::NonZeroU64};

mod handler;
pub use handler::{FencedHandler, VersionedHandler};
#[cfg(test)]
mod tests;

use acyclic_actors_guest_wire::compatibility::PublicationUpload;
pub use acyclic_actors_guest_wire::compatibility::{
    Adapter, CallEnvelope, CompatibilityManifest, HandoffRequirements, PeerRange, RpcSchema,
    Schema, decode_envelope,
};
pub use acyclic_actors_guest_wire::live_selection::{
    Artifact, CallPair, ChangeKind, DeploymentId, Error, Fraction, GuardDecision, POLICY_FILE, Pin,
    RolloutPolicy, RolloutStatus, SELECTION_FILE, SELECTION_REQUEST_FILE, STATUS_FILE, Selection,
    SelectionChange, Split, VERSIONS_DIRECTORY, Version, decode, encode,
};

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
        let schema = compatibility.schema_digest()?;
        if artifacts.is_empty()
            || artifacts.iter().any(|(name, artifact)| {
                name.is_empty()
                    || artifact.code == [0; 32]
                    || artifact.configuration == [0; 32]
                    || artifact.schema != schema
            })
        {
            return Err(Error::Invalid("publication"));
        }
        let number = NonZeroU64::new(expected_version.checked_add(1).ok_or(Error::Exhausted)?)
            .ok_or(Error::Exhausted)?;
        let mut publication = Self {
            upload: PublicationUpload {
                deployment,
                expected_version,
                artifacts,
                compatibility,
            },
            schema,
            version: Version {
                number,
                digest: [0; 32],
            },
        };
        publication.version.digest = Sha256::digest(encode(&publication)?).into();
        Ok(publication)
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

/// A publication with its content identity derived once. Deriving the identity
/// decodes every schema descriptor and hashes the whole document, so hosts and
/// handlers hold this instead of re-deriving it for each Actor they serve.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedPublication {
    publication: Publication,
    version: Version,
    schema: [u8; 32],
}

impl AdmittedPublication {
    /// Validate the publication and fix its identity and contract digest.
    pub fn new(publication: Publication) -> Result<Self, Error> {
        Ok(Self {
            version: publication.version().clone(),
            schema: publication.schema_digest(),
            publication,
        })
    }

    /// The validated immutable upload.
    pub const fn publication(&self) -> &Publication {
        &self.publication
    }

    /// Content identity of [`Self::publication`].
    pub const fn version(&self) -> &Version {
        &self.version
    }

    /// Digest of the declared RPC and message schemas, shared by every artifact.
    pub const fn schema(&self) -> [u8; 32] {
        self.schema
    }
}

/// Bind a durable message without changing its application bytes or indexed
/// keys. An existing envelope cannot be replaced during forwarding.
pub fn bind_message(
    envelope: &CallEnvelope,
    mut record: acyclic_stream::AppendRecord,
) -> Result<acyclic_stream::AppendRecord, Error> {
    envelope.validate()?;
    if message_envelope(&record.headers)?.is_some() {
        return Err(Error::Invalid("message already has caller provenance"));
    }
    let mut attributes = record.headers.attributes().to_vec();
    attributes.push(acyclic_stream::Attribute {
        name: MESSAGE_HEADER.into(),
        value: encode(envelope)?.into(),
    });
    attributes.sort_by(|left, right| left.name.cmp(&right.name));
    record.headers = acyclic_stream::RecordHeaders::new(record.headers.keys().to_vec(), attributes)
        .map_err(|_| Error::Invalid("message headers"))?;
    Ok(record)
}

/// Public durable-message header containing the same v1 call envelope as RPCs.
pub const MESSAGE_HEADER: &str = "acyclic.actors.deployment";

/// Read original message provenance, retained by Streams through replay/fork.
/// Hosts authenticate the caller and require this field for deployment traffic.
pub fn message_envelope(
    headers: &acyclic_stream::RecordHeaders,
) -> Result<Option<CallEnvelope>, Error> {
    let attributes = headers.attributes();
    let Ok(index) = attributes.binary_search_by(|attribute| attribute.name.cmp(MESSAGE_HEADER))
    else {
        return Ok(None);
    };
    let attribute = attributes
        .get(index)
        .ok_or(Error::Invalid("message headers"))?;
    decode_envelope(&attribute.value).map(Some)
}

/// A reference into the existing admitted journal.
pub type JournalPosition = acyclic_actors_guest_wire::compatibility::JournalPosition<StreamPath>;

/// A committed resumable continuation at an admitted journal cut.
pub type DurableAwait = acyclic_actors_guest_wire::compatibility::DurableAwait<StreamPath>;

/// Original selection publication carried by the admitted host. It is joined to
/// the Actor's existing transaction, buying stale-instance publication exclusion
/// with one condition, zero control reads and zero extra commits per delivery.
#[derive(Clone, Debug)]
pub struct SelectionFence {
    /// Original selection stream, bound by the host rather than the guest.
    pub path: StreamPath,
    /// Original committed selection publication.
    pub commit: CommitId,
    /// Exact exclusive tail containing that publication.
    pub tail: u64,
}
impl SelectionFence {
    /// Reuse the existing conditional-commit primitive, including recovery.
    pub fn condition(&self) -> CommitCondition {
        CommitCondition::Head {
            path: self.path.clone(),
            expected: self.tail,
            commit_id: self.commit,
        }
    }
}
