//! Public live-update contract. Cloud admits these values against the original
//! deployment, membership, retained work and Streams authorities. None of these
//! serializable observations is an execution grant.
//!
//! All documents use the same strict v1 encoding in uploads, volume files and
//! call envelopes. Epochs and version numbers are comparable only within their
//! deployment. Selection changes use the original conditional Stream commit.

use acyclic_stream::{CommitCondition, CommitId, StreamPath};

mod handler;
pub use handler::{FencedHandler, VersionedHandler};

pub use acyclic_actors_guest_wire::compatibility::{
    Adapter, CallEnvelope, CompatibilityManifest, HandoffRequirements, PeerRange, RpcSchema,
    Schema, decode_envelope,
};
pub use acyclic_actors_guest_wire::live_selection::{
    Artifact, CallPair, ChangeKind, DeploymentId, Error, Fraction, GuardDecision, POLICY_FILE, Pin,
    RolloutPolicy, RolloutStatus, SELECTION_FILE, SELECTION_REQUEST_FILE, STATUS_FILE, Selection,
    SelectionChange, Split, VERSIONS_DIRECTORY, Version, decode, encode,
};
pub use acyclic_actors_guest_wire::live_update::Publication;

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
