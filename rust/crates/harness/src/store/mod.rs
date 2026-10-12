//! Direct Stream persistence for durable aggregate histories.

mod admission;
mod archive;
mod configuration;
mod execution;
mod forks;
mod migrations;
mod opening;
use execution::stream_idempotency_key_with_work;
use forks::{fork_child_binding_with_work, fork_command_key_with_work};
use migrations::extension_migration_command;
pub use migrations::{ExtensionMigrationRequest, NativeExtensionMigrations};

mod checkpoints;

mod effect_heads;

mod effects;

pub(crate) mod history;

pub use checkpoints::{DEFAULT_PROJECTION_EVENTS, default_projection_read_limits};

pub(crate) mod operations;

mod guarded;

pub use history::*;

pub use effects::effect_events;

use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{
        ContentPublisher, ContentResidencyVerifier, Limits, ReferencedAttachments,
        verified_attachment_manifest, verified_content_bytes,
    },
    core::{
        Action, ApplyResult, Authority, AuthorityVerifier, Command, EffectStatus, EventPayload,
        EventReference, ExtensionDependency, Reducer, SchemaRegistry, Scope, Snapshot,
    },
    effects::{validate_result_bytes, validate_schema_bytes},
    fork::{CompositeForkVerifier, ForkPreparer, ForkReport, ForkRequest, ForkSeed},
    interaction::InteractionOutcome,
    merge::ProjectMergeVerifier,
};

use acyclic_stream::{
    CommitOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, StreamError,
    StreamPath, StreamProvider,
};

use acyclic_stream::OwnedBytes;

use serde_json::Value;

use std::{collections::BTreeMap, sync::Arc};

/// Finite recent terminal cache.
pub const DEFAULT_TERMINAL_EFFECTS: usize = 64;

pub(crate) fn guarded_boundary(
    work: &Option<&mut dyn history::HistoryIo>,
    kind: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_ref()
        .is_some_and(|work| work.original_native_work().is_some())
    {
        return Error::NativeBoundary { code: kind, reason };
    }
    let _ = work;
    match kind {
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Storage(reason.into()),
    }
}

/// Original canonical ROOT binding custody, constructed only by this store.
#[cfg(feature = "filesystem")]
pub(crate) struct VerifiedRootBinding {
    original_task: crate::TaskId,
    admission_digest: [u8; 32],
    fence: crate::scheduler::LeaseFence,
    agent: crate::AgentId,
    audience: Authority,
    bind_operation: OperationId,
    intent_digest: [u8; 32],
}

#[cfg(feature = "filesystem")]
impl VerifiedRootBinding {
    pub(crate) fn original_task(&self) -> crate::TaskId {
        self.original_task
    }
    pub(crate) fn admission_digest(&self) -> [u8; 32] {
        self.admission_digest
    }
    pub(crate) fn fence(&self) -> &crate::scheduler::LeaseFence {
        &self.fence
    }
    pub(crate) fn agent(&self) -> crate::AgentId {
        self.agent
    }
    pub(crate) fn audience(&self) -> &Authority {
        &self.audience
    }
    pub(crate) fn bind_operation(&self) -> OperationId {
        self.bind_operation
    }
    pub(crate) fn intent_digest(&self) -> [u8; 32] {
        self.intent_digest
    }
}

/// One authoritative reducer whose canonical history is stored directly in Stream.
pub struct StreamAggregate<P> {
    client: std::sync::Arc<P>,
    path: StreamPath,
    reducer: Reducer,
    content_verifier: Option<Arc<dyn ContentResidencyVerifier>>,
    fork_verifier: Option<Arc<CompositeForkVerifier>>,
    merge_verifier: Option<Arc<dyn ProjectMergeVerifier>>,
    extension_migrations: Option<Arc<NativeExtensionMigrations>>,
    limits: Limits,
    effect_history_limits: HistoryReadLimits,
}

#[cfg(test)]
mod tests;
