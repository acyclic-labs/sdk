//! Public live-update contract. Cloud admits these values against the original
//! deployment, membership, retained work and Streams authorities. None of these
//! serializable observations is an execution grant.
//!
//! All documents use the same strict v1 encoding in uploads, volume files and
//! call envelopes. Epochs and version numbers are comparable only within their
//! deployment. Selection changes use the original conditional Stream commit.

use crate::{addressing::ActorReference, guest::Checkpoint};
use acyclic_stream::{CommitCondition, CommitId, StreamPath};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, num::NonZeroU64};
use ts_rs::TS;

mod handler;
pub use handler::{FencedHandler, VersionedHandler};

/// Invalid declaration, incompatible retained work or exhausted monotonic space.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A declaration contradicts its public contract.
    #[error("invalid live-update declaration: {0}")]
    Invalid(&'static str),
    /// The original deployment epoch changed before a control was admitted.
    #[error("deployment selection changed")]
    Conflict,
    /// Integer exhaustion never wraps an epoch or immutable version identity.
    #[error("live-update monotonic identity exhausted")]
    Exhausted,
    /// Strict public v1 document encoding or decoding failed.
    #[error(transparent)]
    Encoding(#[from] serde_json::Error),
}

/// Account-scoped deployment identity, shared by all participating namespaces.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeploymentId {
    /// Canonical account bytes, encoded as in the public Actor reference.
    pub account: Vec<u8>,
    /// Admitted deployment name. This is independent of an Actor address.
    pub name: String,
}

impl DeploymentId {
    fn validate(&self) -> Result<(), Error> {
        crate::addressing::AccountId::new(&self.account).map_err(|_| Error::Invalid("account"))?;
        if self.name.is_empty() {
            return Err(Error::Invalid("deployment name"));
        }
        Ok(())
    }
}

/// Immutable monotonically allocated publication; rollback never rewrites it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Version {
    /// Monotonic only in the containing deployment. Starts at one.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub number: NonZeroU64,
    /// Digest of the complete immutable code/configuration/schema version set.
    pub digest: [u8; 32],
}

/// One immutable namespace/service entry in a cohort's selected version set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Artifact {
    /// Canonical code digest.
    pub code: [u8; 32],
    /// Includes bindings and every admitted execution configuration value.
    pub configuration: [u8; 32],
    /// Canonical public RPC/message descriptor digest.
    pub schema: [u8; 32],
}

/// Pin membership is supplied by the admitted owner and serialized with cuts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "kind",
    content = "id",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum Pin {
    /// Default: the complete Actor address is the pin key.
    Actor(Vec<u8>),
    /// Explicit cohort, possibly spanning namespaces, hosts and future children.
    Cohort(String),
}

impl Pin {
    fn validate(&self, deployment: &DeploymentId) -> Result<(), Error> {
        match self {
            Self::Cohort(id) if id.is_empty() => Err(Error::Invalid("empty cohort")),
            Self::Actor(bytes) => {
                let actor =
                    ActorReference::decode(bytes).map_err(|_| Error::Invalid("actor pin"))?;
                if actor.account().as_bytes() != deployment.account {
                    return Err(Error::Invalid("pin belongs to another account"));
                }
                Ok(())
            }
            Self::Cohort(_) => Ok(()),
        }
    }
}

/// Rational wave boundary. Zero and complete waves need no sentinel values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Fraction {
    /// Selected fraction's numerator, at most its denominator.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub numerator: u64,
    /// Unit chosen by the publisher, with no fixed percentage granularity.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub denominator: NonZeroU64,
}

/// Immutable version split. Hashing uses only the deployment and pin key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Split {
    /// Selected outside the wave.
    pub stable: Version,
    /// Selected inside the wave, including a previous publication on rollback.
    pub target: Version,
    /// Stable prefix of the full 64-bit hash space.
    pub wave: Fraction,
}

/// One authoritative logical admission cut; physical adoption is lazy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Selection {
    /// Scope for both monotonic counters and pin membership.
    pub deployment: DeploymentId,
    /// Fresh on every selection change, including rollback and wave advancement.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub epoch: NonZeroU64,
    /// All members of a cohort select the same immutable version set.
    pub split: Split,
}

impl Selection {
    /// Select without enumerating Actors or consulting a control service.
    /// The host must also retain and enforce this selection's publication fence.
    pub fn version(&self, pin: &Pin) -> Result<&Version, Error> {
        self.validate()?;
        pin.validate(&self.deployment)?;
        let mut hash = Sha256::new();
        hash.update(b"acyclic.actors.pin.v1\0");
        // Length-prefixed components prevent ambiguous composite keys.
        for component in [&self.deployment.account, self.deployment.name.as_bytes()] {
            hash.update(
                u64::try_from(component.len())
                    .map_err(|_| Error::Exhausted)?
                    .to_be_bytes(),
            );
            hash.update(component);
        }
        match pin {
            Pin::Actor(actor) => {
                hash.update([0]);
                hash.update(actor);
            }
            Pin::Cohort(id) => {
                hash.update([1]);
                hash.update(id.as_bytes());
            }
        }
        let digest = hash.finalize();
        let mut prefix = [0; 8];
        prefix.copy_from_slice(digest.first_chunk::<8>().ok_or(Error::Invalid("hash"))?);
        let rank = u128::from(u64::from_be_bytes(prefix));
        let selected = rank * u128::from(self.split.wave.denominator.get())
            < u128::from(self.split.wave.numerator) * (u128::from(u64::MAX) + 1);
        Ok(if selected {
            &self.split.target
        } else {
            &self.split.stable
        })
    }

    /// Construct a compare-and-replace promotion. Cloud revalidates compatibility
    /// against its reachable retained live set in the same admission transaction.
    pub fn promote(&self, split: Split) -> Result<SelectionChange, Error> {
        self.change(ChangeKind::Promote, split)
    }

    /// Select older immutable code with a fresh epoch. Retained data and receipts
    /// are preserved; Cloud must prove the rollback reader accepts all of them.
    pub fn rollback(&self, split: Split) -> Result<SelectionChange, Error> {
        self.change(ChangeKind::Rollback, split)
    }

    fn change(&self, kind: ChangeKind, split: Split) -> Result<SelectionChange, Error> {
        self.validate()?;
        if matches!(kind, ChangeKind::Promote) && split.target.number < split.stable.number
            || matches!(kind, ChangeKind::Rollback) && split.target.number >= split.stable.number
        {
            return Err(Error::Invalid("selection direction"));
        }
        let next = Self {
            deployment: self.deployment.clone(),
            epoch: NonZeroU64::new(self.epoch.get().checked_add(1).ok_or(Error::Exhausted)?)
                .ok_or(Error::Exhausted)?,
            split,
        };
        next.validate()?;
        Ok(SelectionChange {
            expected_epoch: self.epoch,
            kind,
            next,
        })
    }

    fn validate(&self) -> Result<(), Error> {
        self.deployment.validate()?;
        if self.split.wave.numerator > self.split.wave.denominator.get()
            || (self.split.stable.number == self.split.target.number
                && self.split.stable != self.split.target)
        {
            return Err(Error::Invalid("selection"));
        }
        Ok(())
    }
}

/// Explicit control intent, retained with the status evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum ChangeKind {
    /// Select the same or a newer immutable publication.
    Promote,
    /// Select an older immutable publication under a fresh epoch.
    Rollback,
}

/// Deployment API mutation. Cloud owns authentication, CAS and durable custody.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SelectionChange {
    /// Expected original epoch, checked at the same committed selection cut.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub expected_epoch: NonZeroU64,
    /// Rollback uses exactly the promotion handoff/fence path.
    pub kind: ChangeKind,
    /// Complete next selection; no independently mutable version pointer.
    pub next: Selection,
}

impl SelectionChange {
    /// Revalidate deserialized control intent at Cloud's original CAS cut.
    pub fn validate_against(&self, previous: &Selection) -> Result<(), Error> {
        if self.expected_epoch != previous.epoch {
            return Err(Error::Conflict);
        }
        let expected = previous.change(self.kind.clone(), self.next.split.clone())?;
        if expected != *self {
            return Err(Error::Invalid("selection change"));
        }
        Ok(())
    }
}

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
        let mut schemas = self
            .messages
            .iter()
            .map(|(name, schema)| (name, schema))
            .collect::<Vec<_>>();
        for (name, rpc) in &self.rpcs {
            schemas.extend([(name, &rpc.request), (name, &rpc.reply)]);
        }
        for (name, schema) in schemas {
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Publication {
    /// Authenticated deployment scope.
    pub deployment: DeploymentId,
    /// Zero only before the first immutable upload.
    #[serde(with = "decimal")]
    #[ts(type = "string")]
    pub expected_version: u64,
    /// Namespace/service identity to immutable artifact/configuration selection.
    pub artifacts: BTreeMap<String, Artifact>,
    /// Rust-owned contract submitted for Cloud's upload gate.
    pub compatibility: CompatibilityManifest,
}

impl Publication {
    /// Content identity for this exact immutable publication. Cloud assigns it
    /// only after checking the predecessor and validating schemas/retained work.
    pub fn version(&self) -> Result<Version, Error> {
        self.deployment.validate()?;
        self.compatibility.validate()?;
        let schema = self.compatibility.schema_digest()?;
        if self.artifacts.is_empty()
            || self.artifacts.iter().any(|(name, artifact)| {
                name.is_empty()
                    || artifact.code == [0; 32]
                    || artifact.configuration == [0; 32]
                    || artifact.schema != schema
            })
        {
            return Err(Error::Invalid("publication"));
        }
        Ok(Version {
            number: NonZeroU64::new(
                self.expected_version
                    .checked_add(1)
                    .ok_or(Error::Exhausted)?,
            )
            .ok_or(Error::Exhausted)?,
            digest: Sha256::digest(encode(self)?).into(),
        })
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document<T> {
    v: u8,
    value: T,
}

/// Canonical strict v1 JSON; shared by files, upload bytes and call envelopes.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    let mut document = serde_json::to_value(Document { v: 1, value })?;
    document.sort_all_objects();
    Ok(serde_json::to_vec(&document)?)
}

// Decimal strings preserve every u64 in JavaScript and volume JSON. They are
// rejected if noncanonical by the same document comparison, including leading 0.
mod decimal {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};
    use std::{fmt::Display, str::FromStr};
    pub fn serialize<T: Display, S: Serializer>(
        value: &T,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, T: FromStr, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|_| D::Error::custom("invalid unsigned decimal"))
    }
}

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

/// Refuse unknown versions, unknown fields and noncanonical encodings. There is
/// no migration path and no silently ignored deployment provenance.
pub fn decode<T: DeserializeOwned + Serialize>(bytes: &[u8]) -> Result<T, Error> {
    let document: Document<T> = serde_json::from_slice(bytes)?;
    if document.v != 1 || encode(&document.value)? != bytes {
        return Err(Error::Invalid("noncanonical v1 document"));
    }
    Ok(document.value)
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
