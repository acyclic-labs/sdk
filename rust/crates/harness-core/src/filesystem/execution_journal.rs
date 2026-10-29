//! Stream-backed execution observations with private Filesystem payloads.

use super::bounded_io::{PreparationIo, PreparationRecord};
use super::{FilesystemHost, InternalContentClass};
use crate::obs::{obs_span, traced};
use crate::{
    Error, IdempotencyKey, InteractionId, OperationId, Result,
    conversation::{
        ContentGrant, ContentResidencyVerifier, FileRef, Limits, VolumeClass, VolumeOperation,
        VolumeRef,
    },
    core::{AuthorityVerifier, Scope},
    executor::{
        EXECUTION_REPLAY_PAGE_RECORDS, ExecutionEvent, ExecutionJournal, ExecutionRecord,
        ModelEventAdmission, decode_json,
    },
    interaction::{Interaction, InteractionOutcome},
    model::{ModelContent, ModelEvent},
    projection::SelectedModelContext,
    tool::ToolApprovalVerifier,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture as BoxFuture;
#[cfg(test)]
use acyclic_stream::{AppendOutcome, IdempotencyKey as StreamKey};
use acyclic_stream::{StreamError, StreamProvider};
#[cfg(test)]
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

mod cancellation;
mod canonical;
mod checkpoint;
mod checkpoint_maps;
mod configuration;
mod content;
mod durability;
mod indexed_publication;
#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod inline_tests;
mod interactions;
mod observations;
mod persistent_index;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod publication_future_tests;
mod replay;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod replay_capacity_tests;
#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod request_segments_tests;
mod selected;
mod step_index;
mod summary;
mod summary_authorization;
mod summary_cache;
mod summary_map;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod test_support;
#[cfg(test)]
mod tests;
mod traits;
use summary::*;
use summary_cache::SummaryCache;

/// Bound on one journal payload. A journal records harness-owned
/// execution state, not admitted content, so it takes no content bound;
/// this is the largest exactly representable length.
pub(crate) const MAXIMUM_PAYLOAD_BYTES: u64 = crate::conversation::MAX_EXACT_JS_INTEGER;

/// Durable observations and private payload references for one exact agent-private volume.
pub struct FilesystemExecutionJournal<P: ?Sized, A: 'static, O: 'static> {
    cache_identity: crate::executor::ExecutionJournalCacheId,
    stream: Arc<P>,
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    input_verifier: Option<Arc<dyn ContentResidencyVerifier>>,
    /// The one model context an agent turn may dispatch, built by its turn host
    /// from the agent's own conversation stream; any other selection is refused.
    bound_selection: Option<crate::projection::SelectedModelContext>,
    /// An agent turn's stop signal, followed as the executor's cancellation.
    stop: Option<acyclic_actors::guest::CancellationToken>,
    /// A native agent's approval files; its interactions never touch the ledger.
    agent_approvals: Option<Arc<crate::approvals::AgentApprovals>>,
    execution: Option<(OperationId, OperationId)>,
    verified: tokio::sync::Mutex<SummaryCache>,
    history_read_limits: Option<crate::store::HistoryReadLimits>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

fn canonical_boundary(
    io: Option<&PreparationIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    if code == crate::wire::ErrorCode::NotFound
        && io.is_none_or(|io| io.original_native_work().is_none())
    {
        Error::NotFound(reason.into())
    } else {
        crate::executor::stock_boundary(io, code, reason)
    }
}
