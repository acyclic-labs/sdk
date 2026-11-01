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
#[cfg(feature = "test-support")]
/// On-demand checks of the original retained execution metadata codecs.
pub mod codec_fuzz {
    use super::*;

    /// Check metadata syntax without granting checkpoint signature authority.
    pub fn check(bytes: &[u8]) {
        observations::codec_fuzz::check(bytes);
        checkpoint::codec_fuzz::check(bytes);
        persistent_index::node::codec_fuzz::check(bytes);
        checkpoint_maps::codec_fuzz::check(bytes);
        step_index::codec_fuzz::check(bytes);
    }

    /// Capture the actual observation and fixed checkpoint frame writers.
    pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>> {
        let mut seeds = observations::codec_fuzz::seeds()?;
        seeds.extend(checkpoint::codec_fuzz::seeds()?);
        seeds.extend(persistent_index::node::codec_fuzz::seeds()?);
        seeds.extend(checkpoint_maps::codec_fuzz::seeds()?);
        seeds.extend(step_index::codec_fuzz::seeds()?);
        Ok(seeds)
    }
}
mod configuration;
mod content;
#[cfg(not(target_arch = "wasm32"))]
mod context_publication;
mod conversation;
mod durability;
mod indexed_publication;
#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod inline_tests;
mod interactions;
mod observations;
#[cfg(not(target_arch = "wasm32"))]
pub use context_publication::{
    ContextCheckpoint, ContextPublicationReceipt, ContextPublicationRequest,
};
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
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
mod tool_settlement;
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
    boundary: Option<Arc<dyn crate::agent::ModelBoundary>>,
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
