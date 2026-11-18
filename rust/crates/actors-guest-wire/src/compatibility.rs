//! Wire declarations for live-update compatibility manifests.

use crate::{
    CodecIdentity,
    live_selection::{Artifact, DeploymentId, Error, decimal, encode, optional_decimal},
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
