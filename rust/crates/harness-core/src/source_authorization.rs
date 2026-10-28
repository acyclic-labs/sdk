//! Operation-bound reads of trusted project snapshots.

use crate::conversation::VolumeRef;
use crate::core::{Authority, Scope};
use crate::resources::GenerationRef;
use crate::{OperationId, TaskId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// MAC domain for project-source reads.
pub const PROJECT_SOURCE_READ_PURPOSE: &str = "harness.project-source.read.v1";

/// Exact caller lease retained in persisted project-source authorizations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
pub struct LeaseFence {
    /// Provider lease identity.
    pub reservation_id: String,
    /// Worker or placement owning the lease.
    pub placement: String,
}

/// Signed project selection retained with the original admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct ProjectSourceAuthorization {
    /// Trusted service owner.
    pub owner: Authority,
    /// Original caller task.
    pub caller_task: TaskId,
    /// Complete original admission digest.
    pub caller_admission_digest: [u8; 32],
    /// Exact original caller lease.
    pub caller_fence: LeaseFence,
    /// Original fork operation.
    pub operation_id: OperationId,
    /// Selected source task.
    pub source_task: TaskId,
    /// Authenticated project volume.
    pub project: VolumeRef,
    /// Pinned immutable generation.
    pub generation: GenerationRef,
    /// Authenticated backend evidence.
    pub native_snapshot: Option<Value>,
    /// Common coordinator graph root.
    pub graph_root: TaskId,
    /// Finite work allowance.
    pub maximum_work: u64,
    /// Finite byte allowance.
    pub maximum_bytes: u64,
    /// Issuer's opaque descriptor attestation.
    pub policy_scope: Scope,
}

impl ProjectSourceAuthorization {}

crate::core::ownership::owned_struct!(LeaseFence {
    reservation_id,
    placement
});

crate::core::ownership::owned_struct!(ProjectSourceAuthorization {
    owner,
    caller_task,
    caller_admission_digest,
    caller_fence,
    operation_id,
    source_task,
    project,
    generation,
    native_snapshot,
    graph_root,
    maximum_work,
    maximum_bytes,
    policy_scope
});
