//! Stream-backed execution observations with harness-owned journal payloads.

use super::bounded_io::{PreparationIo, PreparationRecord};
use super::{FilesystemHost, InternalContentClass};
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
use acyclic_stream::StreamError;
#[cfg(test)]
use acyclic_stream::{AppendOutcome, IdempotencyKey as StreamKey};
#[cfg(test)]
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod activity_tests;
mod cancellation;
mod canonical;
mod checkpoint;
mod checkpoint_maps;
#[cfg(feature = "test-support")]
/// On-demand checks of the original retained execution metadata codecs.
pub mod codec_fuzz {
    use super::*;

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn check_decode_custody(
        check: impl FnOnce(&PreparationIo) -> Result<()>,
    ) -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let result = check(&PreparationIo::from_native_work(&work)?);
        assert_eq!(
            work.live_allocation_bytes(),
            baseline,
            "codec custody was retained"
        );
        result
    }

    pub(super) fn check_versioned_frame<T>(
        bytes: &[u8],
        versions: impl IntoIterator<Item = u8>,
        decode: impl Fn(&[u8]) -> super::Result<T>,
    ) -> super::Result<()> {
        use crate::wire::ErrorCode;

        let [first, second, third, version, ..] = bytes else {
            return Err(crate::Error::Invalid(
                "admitted journal frame has no header".into(),
            ));
        };
        for unsupported in versions {
            let header = [*first, *second, *third, unsupported];
            assert!(decode(&header).is_err_and(|error| error.code() == ErrorCode::Unsupported));
        }
        let header = [*first, *second, *third, *version];
        assert!(decode(&header).is_err_and(|error| error.code() != ErrorCode::Unsupported));
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
        Ok(())
    }

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
#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod claim_tests;
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
mod model_calls;
mod observations;
mod publication_authorization;
#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod request_prefix_tests;
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
mod supervision;
#[cfg(test)]
pub(super) mod test_support;
#[cfg(test)]
mod tests;
mod tool_settlement;
mod traits;
use summary::*;
use summary_cache::SummaryCache;

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionReplayObservation {
    Requested,
    Decoded,
}

/// Bound on one journal payload. A journal records harness-owned
/// execution state, not admitted content, so it takes no content bound;
/// this is the largest exactly representable length.
pub(crate) const MAXIMUM_PAYLOAD_BYTES: u64 = crate::conversation::MAX_EXACT_JS_INTEGER;

/// Durable observations and private payload references for one exact agent-private volume.
pub struct FilesystemExecutionJournal<P: ?Sized, A: 'static, O: 'static> {
    cache_identity: crate::executor::ExecutionJournalCacheId,
    stream: Arc<P>,
    traces: Option<(acyclic_stream::trace::Registry<P>, crate::agent::AgentPath)>,
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    /// Signs this journal's execution checkpoints under its current custody.
    execution_checkpoint_verifier: AuthorityVerifier,
    /// The other custody identity the same journal may have signed under.
    execution_checkpoint_alternate: Option<AuthorityVerifier>,
    scope: Scope,
    input_verifier: Option<Arc<dyn ContentResidencyVerifier>>,
    /// The one model context an agent turn may dispatch, built by its turn host
    /// from the agent's own conversation stream; any other selection is refused.
    bound_selection: Option<crate::projection::SelectedModelContext>,
    /// Original admitted request of the immediately preceding selected turn.
    preceding_request: Option<FileRef>,
    /// An agent turn's stop signal, followed as the executor's cancellation.
    stop: Option<acyclic_actors::guest::CancellationToken>,
    clock: Option<Arc<dyn acyclic_actors::local::Clock>>,
    boundary: Option<Arc<dyn crate::agent::ModelBoundary>>,
    /// A native agent's approval files; its interactions never touch the ledger.
    agent_approvals: Option<Arc<crate::approvals::AgentApprovals>>,
    execution: Option<(OperationId, OperationId)>,
    /// Owning agent/run; bound once by the shared turn host, never by a model.
    agent_events: Option<(crate::agent::AgentPath, OperationId, u64, bool)>,
    /// Host-authenticated actor state paths from the tree root through this agent.
    actor_admissions: Vec<acyclic_stream::StreamPath>,
    run_admission: Option<crate::supervision::RunAdmissionSource>,
    verified: tokio::sync::Mutex<SummaryCache>,
    history_read_limits: Option<crate::store::HistoryReadLimits>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(test)]
impl<P: ?Sized, A: 'static, O: 'static> FilesystemExecutionJournal<P, A, O> {
    pub(crate) fn execution_checkpoint_authority(&self) -> &AuthorityVerifier {
        &self.execution_checkpoint_verifier
    }
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
