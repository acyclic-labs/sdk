//! Public live-update contract. Cloud admits these values against the original
//! deployment, membership, retained work and Streams authorities. None of these
//! serializable observations is an execution grant.
//!
//! All documents use the same strict v1 encoding in uploads, volume files and
//! call envelopes. Epochs and version numbers are comparable only within their
//! deployment. Selection changes use the original conditional Stream commit.

use crate::guest::Checkpoint;
use acyclic_stream::{CommitCondition, CommitId, StreamPath};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, num::NonZeroU64};
use ts_rs::TS;

mod handler;
pub use handler::{FencedHandler, VersionedHandler};
#[cfg(test)]
mod tests;

use acyclic_actors_guest_wire::live_selection::decimal;
pub use acyclic_actors_guest_wire::live_selection::{
    Artifact, ChangeKind, DeploymentId, Error, Fraction, Pin, Selection, SelectionChange, Split,
    Version, decode, encode,
};

/// Canonical public schema, rather than a Cloud-maintained parallel declaration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Schema {
    /// Canonical Protobuf FileDescriptorSet including its dependency closure.
    pub descriptor_set: Vec<u8>,
    /// Fully qualified message name within that descriptor set.
    pub message: String,
}

/// Request and reply must both remain readable across every reachable live edge.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RpcSchema {
    /// Request declaration.
    pub request: Schema,
    /// Reply declaration, including unsupported-enum/unknown-field behavior.
    pub reply: Schema,
}

/// Inclusive supported range in exactly one peer deployment, never a bare epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PeerRange {
    /// Scope of the peer's immutable publication numbers.
    pub deployment: DeploymentId,
    /// First supported immutable publication.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub first: NonZeroU64,
    /// Last supported publication; None means unbounded, without a fallback.
    #[serde(with = "optional_decimal")]
    #[ts(type = "string | null")]
    pub last: Option<NonZeroU64>,
}

/// State and the original turn journal's decoder obligations at handoff.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandoffRequirements {
    /// Codec emitted by this publication.
    pub writes: crate::guest::CodecIdentity,
    /// Every state/continuation codec this publication can resume.
    pub reads: Vec<crate::guest::CodecIdentity>,
    /// Original journal formats this publication can consume without reissuing work.
    pub journal_reads: Vec<crate::guest::CodecIdentity>,
    /// Durable await support is explicit. False forbids idle-free cutover claims.
    pub durable_await: bool,
}

/// Explicit application adapter in the immutable authenticated publication.
/// Cloud validates its executable artifact and edge before accepting a rollout.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Adapter {
    /// Exact source schema digest.
    pub from: [u8; 32],
    /// Exact destination schema digest.
    pub to: [u8; 32],
    /// Application code providing the declared adapter, never a host fallback.
    pub code: [u8; 32],
}

/// Upload compatibility manifest. Live obligations include hibernated/pinned
/// Actors and unresolved inboxes, turns and receipts, not only executing heaps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompatibilityManifest {
    /// Public method name to canonical request/reply pair.
    pub rpcs: BTreeMap<String, RpcSchema>,
    /// Durable message name to canonical schema.
    pub messages: BTreeMap<String, Schema>,
    /// All declared peer deployment ranges; both call directions are checked.
    pub peers: Vec<PeerRange>,
    /// Retained state/journal obligations, also enforced before rollback.
    pub handoff: HandoffRequirements,
    /// Explicit incompatible-edge adapters supplied by the application.
    pub adapters: Vec<Adapter>,
}

impl CompatibilityManifest {
    /// Identity of the declared RPC requests/replies and durable messages.
    pub fn schema_digest(&self) -> Result<[u8; 32], Error> {
        Ok(Sha256::digest(encode(&(&self.rpcs, &self.messages))?).into())
    }

    fn validate(&self) -> Result<(), Error> {
        let mut schemas = self.messages.iter().collect::<Vec<_>>();
        for (name, rpc) in &self.rpcs {
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
        for peer in &self.peers {
            peer.deployment.validate()?;
            if !scopes.insert(&peer.deployment) || peer.last.is_some_and(|last| last < peer.first) {
                return Err(Error::Invalid("peer range"));
            }
        }
        if !self.handoff.reads.contains(&self.handoff.writes)
            || self.handoff.writes.name.is_empty()
            || self.handoff.writes.version.is_empty()
            || self
                .handoff
                .reads
                .iter()
                .chain(&self.handoff.journal_reads)
                .any(|codec| codec.name.is_empty() || codec.version.is_empty())
            || self.handoff.durable_await && self.handoff.journal_reads.is_empty()
            || self
                .adapters
                .iter()
                .any(|adapter| adapter.from == adapter.to || adapter.code == [0; 32])
        {
            return Err(Error::Invalid("handoff or adapter declaration"));
        }
        Ok(())
    }
}

/// Immutable upload. Its monotonically assigned number uses expected_version
/// as the conditional predecessor, independently of the selected code's age.
///
/// A value exists only after validation: [`Publication::new`] and decoding both
/// validate once and keep the identity, so every later read is O(1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Publication {
    /// Authenticated deployment scope.
    deployment: DeploymentId,
    /// Zero only before the first immutable upload.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    expected_version: u64,
    /// Namespace/service identity to immutable artifact/configuration selection.
    artifacts: BTreeMap<String, Artifact>,
    /// Rust-owned contract submitted for Cloud's upload gate.
    compatibility: CompatibilityManifest,
    #[serde(skip)]
    #[ts(skip)]
    schema: [u8; 32],
    #[serde(skip)]
    #[ts(skip)]
    version: Version,
}

/// The wire fields of a [`Publication`], before validation.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Upload {
    deployment: DeploymentId,
    #[serde(with = "decimal")]
    expected_version: u64,
    artifacts: BTreeMap<String, Artifact>,
    compatibility: CompatibilityManifest,
}

impl<'de> Deserialize<'de> for Publication {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let upload = Upload::deserialize(deserializer)?;
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
        compatibility.validate()?;
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
            deployment,
            expected_version,
            artifacts,
            compatibility,
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
        &self.deployment
    }

    /// Zero only before the first immutable upload.
    pub fn expected_version(&self) -> u64 {
        self.expected_version
    }

    /// Namespace/service identity to immutable artifact/configuration selection.
    pub fn artifacts(&self) -> &BTreeMap<String, Artifact> {
        &self.artifacts
    }

    /// Rust-owned contract submitted for Cloud's upload gate.
    pub fn compatibility(&self) -> &CompatibilityManifest {
        &self.compatibility
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

/// Host-bound provenance on every Actor RPC/message and retained reply. Copying
/// or supplying this envelope never authorizes code selection or effects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CallEnvelope {
    /// Caller deployment; unrelated deployments' epochs are incomparable.
    pub deployment: DeploymentId,
    /// Admitted caller membership, never a cohort chosen by the request header.
    pub pin: Pin,
    /// Original caller admission epoch, retained across queues and rollback.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub epoch: NonZeroU64,
    /// Caller immutable version set.
    pub version: Version,
    /// Original method/message contract identity.
    pub contract: [u8; 32],
}

impl CallEnvelope {
    /// Structural admission only. The receiving host also binds these values to
    /// original caller authority and the admitted live compatibility relation.
    pub fn validate(&self) -> Result<(), Error> {
        self.deployment.validate()?;
        self.pin.validate(&self.deployment)
    }

    /// Bind a durable message without changing its application bytes or indexed
    /// keys. An existing envelope cannot be replaced during forwarding.
    pub fn message(
        &self,
        mut record: acyclic_stream::AppendRecord,
    ) -> Result<acyclic_stream::AppendRecord, Error> {
        self.validate()?;
        if message_envelope(&record.headers)?.is_some() {
            return Err(Error::Invalid("message already has caller provenance"));
        }
        let mut attributes = record.headers.attributes().to_vec();
        attributes.push(acyclic_stream::Attribute {
            name: MESSAGE_HEADER.into(),
            value: encode(self)?.into(),
        });
        attributes.sort_by(|left, right| left.name.cmp(&right.name));
        record.headers =
            acyclic_stream::RecordHeaders::new(record.headers.keys().to_vec(), attributes)
                .map_err(|_| Error::Invalid("message headers"))?;
        Ok(record)
    }
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

/// Admit the original call's structural scope before retaining its provenance.
pub fn decode_envelope(bytes: &[u8]) -> Result<CallEnvelope, Error> {
    let envelope: CallEnvelope = decode(bytes)?;
    envelope.validate()?;
    Ok(envelope)
}

/// A reference into the existing journal, not a second journal or effect owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JournalPosition {
    /// Original admitted journal stream.
    #[ts(type = "string")]
    pub stream: StreamPath,
    /// Publication containing the accepted operation/continuation.
    pub commit: [u8; 32],
    /// Original operation slot, including zero.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub sequence: u64,
    /// Stable external operation identity; receipt completion keeps this identity.
    pub operation: Vec<u8>,
}

/// Only a committed resumable continuation is a durable handoff point. Hosts
/// verify the checkpoint and journal cut share the original publication before
/// changing selection. An arbitrary in-memory await cannot produce this proof.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DurableAwait {
    /// Artifact-owned resumable continuation and its codec.
    #[ts(type = "{ codec: CodecIdentity; bytes: number[] }")]
    pub continuation: Checkpoint,
    /// Pending original operations, whose producers/unknown outcomes remain owned.
    pub pending: Vec<JournalPosition>,
}

/// Bounded version-pair aggregates, never Actor-id metric labels.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CallPair {
    /// Original caller deployment/version; direction is significant.
    pub caller: Version,
    /// Actual callee deployment/version.
    pub callee: Version,
    /// Original caller scope, avoiding unrelated version-number comparisons.
    pub caller_deployment: DeploymentId,
    /// Actual callee scope.
    pub callee_deployment: DeploymentId,
    /// Admitted attempts; zero cannot establish coverage.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub attempted: u64,
    /// Original terminal successful results, excluding injected guard evidence.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub completed: u64,
    /// Stable typed failure category to count.
    #[serde(with = "decimal_map")]
    #[ts(type = "{ [kind: string]: string }")]
    pub failures: BTreeMap<String, u64>,
    /// Aggregate time awaiting the admitted selection/journal/inbox event.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub queue_micros: u64,
    /// Aggregate end-to-end time, reported separately from queue time.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub latency_micros: u64,
}

/// Declared rollout SLI policy in the deployment volume. Cloud checks every
/// reachable mixed-version direction separately from same-version traffic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RolloutPolicy {
    /// Required terminal successes per direction; zero traffic is not coverage.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub minimum_completed: NonZeroU64,
    /// Maximum typed-failure fraction among original attempts.
    pub maximum_failure_fraction: Fraction,
    /// Optional mean queue threshold; None means truly unbounded.
    #[serde(with = "optional_decimal")]
    #[ts(type = "string | null")]
    pub maximum_mean_queue_micros: Option<NonZeroU64>,
    /// Optional mean latency threshold, independently reported from queue time.
    #[serde(with = "optional_decimal")]
    #[ts(type = "string | null")]
    pub maximum_mean_latency_micros: Option<NonZeroU64>,
}

/// Guard evidence and policy outcome persisted beside wave progress.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "decision", rename_all = "camelCase", deny_unknown_fields)]
pub enum GuardDecision {
    /// At least one required direction lacks observed terminal traffic.
    AwaitingCoverage,
    /// The declared SLI policy admits this next wave.
    Advance {
        /// Digest of the admitted SLI policy.
        policy: [u8; 32],
    },
    /// Retain the original evidence; rollback advances the selection epoch.
    Rollback {
        /// Digest of the admitted SLI policy.
        policy: [u8; 32],
        /// Typed guard failure, independent of application calls.
        reason: String,
    },
    /// An unsupported/non-yielding handler prevents safe preparation of the cut.
    HandoffBlocked {
        /// Stable reason reported to the deployment owner.
        reason: String,
    },
}

/// Files plus live queries expose rollout progress. Hosts append/update the
/// original volume files with their selection transaction; followers use the
/// existing filesystem/Streams query, never a status polling endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RolloutStatus {
    /// Original currently selected cut.
    pub selection: Selection,
    /// Opaque existing placement/index page cursor; None means wave complete.
    pub next_page: Option<Vec<u8>>,
    /// Pins adopted in this wave; cohorts are the unit, not individual members.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub adopted: u64,
    /// Guard outcome and retained version-pair evidence.
    pub guard: GuardDecision,
    /// Version-pair/direction observations required by the declared policy.
    pub calls: Vec<CallPair>,
    /// Fresh epoch allocated for rollback; None means no rollback admitted.
    #[serde(with = "optional_decimal")]
    #[ts(type = "string | null")]
    pub rollback_epoch: Option<NonZeroU64>,
}

/// Deployment-volume paths, relative to the admitted deployment root.
pub const SELECTION_FILE: &str = "deployment/selection.json";
/// Immutable publication files are named by the monotonically assigned version.
pub const VERSIONS_DIRECTORY: &str = "deployment/versions";
/// Follow this file through the existing live query to observe wave/guard changes.
pub const STATUS_FILE: &str = "deployment/rollout.json";
/// Promotion/rollback controls use the admitted volume's ordinary write path.
pub const SELECTION_REQUEST_FILE: &str = "deployment/selection.request.json";
/// The SLI policy is observed through the same deployment live query.
pub const POLICY_FILE: &str = "deployment/rollout-policy.json";

mod optional_decimal {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};
    use std::{fmt::Display, str::FromStr};
    pub fn serialize<T: Display, S: Serializer>(
        value: &Option<T>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => serializer.serialize_some(&value.to_string()),
            None => serializer.serialize_none(),
        }
    }
    pub fn deserialize<'de, T: FromStr, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<T>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| D::Error::custom("invalid unsigned decimal"))
            })
            .transpose()
    }
}

mod decimal_map {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
    use std::collections::BTreeMap;
    pub fn serialize<S: Serializer>(
        value: &BTreeMap<String, u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .iter()
            .map(|(key, value)| (key, value.to_string()))
            .collect::<BTreeMap<_, _>>()
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<String, u64>, D::Error> {
        BTreeMap::<String, String>::deserialize(deserializer)?
            .into_iter()
            .map(|(key, value)| {
                value
                    .parse()
                    .map(|value| (key, value))
                    .map_err(|_| D::Error::custom("invalid unsigned decimal"))
            })
            .collect()
    }
}

/// Self-contained TypeScript JSON declarations, emitted by the existing Actors
/// producer. u64 values are decimal strings, including epochs above 2^53.
pub fn typescript() -> String {
    let config = ts_rs::Config::default();
    let mut output = String::from(
        "// Generated from acyclic_actors::live_update. Do not edit.\nimport type { CodecIdentity } from './guest.js';\n",
    );
    macro_rules! declarations {
        ($($ty:ty),+ $(,)?) => { $(
            output.push_str("export "); output.push_str(&<$ty as TS>::decl(&config)); output.push('\n');
        )+ };
    }
    declarations!(
        DeploymentId,
        Version,
        Artifact,
        Pin,
        Fraction,
        Split,
        Selection,
        ChangeKind,
        SelectionChange,
        Schema,
        RpcSchema,
        PeerRange,
        HandoffRequirements,
        Adapter,
        CompatibilityManifest,
        Publication,
        CallEnvelope,
        JournalPosition,
        DurableAwait,
        CallPair,
        GuardDecision,
        RolloutPolicy,
        RolloutStatus
    );
    output
}

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
