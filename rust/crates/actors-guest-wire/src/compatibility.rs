//! Wire declarations for live-update compatibility manifests.

use crate::{
    Checkpoint, CodecIdentity,
    live_selection::{
        Artifact, CallPair, ChangeKind, DeploymentId, Error, Fraction, GuardDecision, Pin,
        RolloutPolicy, RolloutStatus, Selection, SelectionChange, Split, Version, decimal, decode,
        encode, optional_decimal,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, num::NonZeroU64};
use ts_rs::TS;

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
    pub writes: CodecIdentity,
    /// Every state/continuation codec this publication can resume.
    pub reads: Vec<CodecIdentity>,
    /// Original journal formats this publication can consume without reissuing work.
    pub journal_reads: Vec<CodecIdentity>,
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
}

/// Immutable upload fields before runtime descriptor admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(rename = "Publication")]
pub struct PublicationUpload {
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

/// A reference into the existing journal, not a second journal or effect owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(concrete(Path = String))]
pub struct JournalPosition<Path> {
    /// Original admitted journal stream.
    #[ts(type = "string")]
    pub stream: Path,
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
#[ts(concrete(Path = String))]
pub struct DurableAwait<Path> {
    /// Artifact-owned resumable continuation and its codec.
    #[ts(type = "{ codec: CodecIdentity; bytes: number[] }")]
    pub continuation: Checkpoint,
    /// Pending original operations, whose producers/unknown outcomes remain owned.
    pub pending: Vec<JournalPosition<Path>>,
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
}

/// Admit the original call's structural scope before retaining its provenance.
pub fn decode_envelope(bytes: &[u8]) -> Result<CallEnvelope, Error> {
    let envelope: CallEnvelope = decode(bytes)?;
    envelope.validate()?;
    Ok(envelope)
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
        PublicationUpload,
        CallEnvelope,
        JournalPosition<String>,
        DurableAwait<String>,
        CallPair,
        GuardDecision,
        RolloutPolicy,
        RolloutStatus
    );
    output
}
