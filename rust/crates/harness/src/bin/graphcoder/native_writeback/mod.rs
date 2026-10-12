//! Approval of the SDK's original caller-owned native publication.
#![allow(missing_docs, reason = "private binary composition")]
mod config;
pub use config::NativePublicationConfig;
#[cfg(feature = "native-execution")]
mod apply;
#[cfg(feature = "native-execution")]
mod approval;
#[cfg(feature = "native-execution")]
mod approval_frame;
#[cfg(feature = "native-execution")]
mod open;
#[cfg(feature = "native-execution")]
mod protocol;

use crate::{native::AuthorityBinding, terminal::MAX_FRAME_BYTES};
use acyclic_fs::{GenerationId, MaterializationJournal, WorkBudget, WorkCounters};
use acyclic_harness::{
    Error, OperationId, Result, TaskId,
    conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeRef},
    core::AuthorityIssuer,
    distributed::WorkLease,
    interaction::{InteractionResolution, InteractionTicket},
};
use serde::{Deserialize, Serialize};
use std::path::{Component, PathBuf};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePublicationRequest {
    pub operation_id: OperationId,
    pub caller_lease: WorkLease,
}
#[derive(Serialize)]
pub struct NativePublicationResult {
    pub operation_id: OperationId,
    pub caller: TaskId,
    pub status: &'static str,
    pub interaction: Option<InteractionTicket>,
    pub resolution: Option<InteractionResolution>,
    pub action_digest: Option<[u8; 32]>,
    pub publication: Option<MaterializationJournal>,
    pub work: WorkCounters,
    pub failure: Option<String>,
    pub approved_frame_hex: Option<String>,
}

#[cfg(feature = "native-execution")]
use crate::native::NativeSession;
#[cfg(feature = "native-execution")]
use acyclic_fs::MaterializationPhase;
#[cfg(feature = "native-execution")]
use acyclic_harness::{
    Capabilities, InteractionId,
    core::{Action, SchemaRegistry},
    executor::ExecutionJournal,
    filesystem::{FilesystemExecutionJournal, FilesystemInteractionHost},
    interaction::{Interaction, InteractionOutcome},
    scheduler::LeaseFence,
    store::StreamAggregate,
    tool::ToolApprovalVerifier,
    wire,
    wire_api::current_protocol,
    wire_codec::encode_authority,
};
#[cfg(feature = "native-execution")]
use protocol::*;
#[cfg(feature = "native-execution")]
type Journal = FilesystemExecutionJournal<
    acyclic_stream::LocalStream,
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
>;
#[cfg(feature = "native-execution")]
type Interactions = FilesystemInteractionHost<
    acyclic_stream::LocalStream,
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
>;
