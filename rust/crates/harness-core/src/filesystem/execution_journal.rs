//! Stream-backed execution observations with private Filesystem payloads.

use super::fork_preparer_io::{PreparationIo, PreparationRecord};
use super::interaction_host::interaction_boundary as canonical_boundary;
use super::{FilesystemHost, FilesystemInteractionHost, InternalContentClass};
use crate::obs::{obs_span, traced};
use crate::{
    Error, IdempotencyKey, InteractionId, OperationId, Result,
    conversation::{
        ContentGrant, ContentResidencyVerifier, FileRef, Limits, VolumeClass, VolumeOperation,
        VolumeRef,
    },
    core::{AuthorityVerifier, SchemaRegistry, Scope},
    durable_host::TaskJournalOwner,
    executor::{
        EXECUTION_REPLAY_PAGE_RECORDS, ExecutionEvent, ExecutionJournal, ExecutionRecord,
        ModelEventAdmission, decode_json,
    },
    interaction::{Interaction, InteractionOutcome, InteractionResolution, InteractionResponse},
    model::{ModelContent, ModelEvent},
    projection::{SelectedModelContext, select_model_context},
    store::StreamAggregate,
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
use std::{collections::BTreeMap, sync::Arc};

mod conversation;

mod approval_files;
mod cancellation;
mod canonical;
mod checkpoint;
mod checkpoint_maps;
mod chunks;
mod configuration;
mod content;
mod durability;
#[cfg(all(test, feature = "filesystem-local", not(target_arch = "wasm32")))]
mod durability_tests;
mod indexed_publication;
#[cfg(all(test, not(acyclic_harness_loom), not(target_arch = "wasm32")))]
mod inline_tests;
mod interactions;
mod latest_response;
mod observations;
mod persistent_index;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod publication_future_tests;
mod replay;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod replay_capacity_tests;
#[cfg(all(test, not(acyclic_harness_loom), not(target_arch = "wasm32")))]
mod request_segments_tests;
mod router;
mod selected;
mod step_index;
mod summary;
mod summary_authorization;
#[cfg(test)]
mod summary_authorization_tests;
mod summary_cache;
mod summary_map;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod test_support;
#[cfg(test)]
mod tests;
mod traits;
use selected::JournalSelectedResolver;
use summary::*;
use summary_cache::SummaryCache;

/// Durable observations and private payload references for one exact agent-private volume.
pub struct FilesystemExecutionJournal<P, A, O> {
    cache_identity: crate::executor::ExecutionJournalCacheId,
    stream: Arc<P>,
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    schemas: SchemaRegistry,
    maximum_payload_bytes: u64,
    input_verifier: Option<Arc<dyn ContentResidencyVerifier>>,
    /// The one model context an agent turn may dispatch, built by its turn host
    /// from the agent's own conversation stream; any other selection is refused.
    bound_selection: Option<crate::projection::SelectedModelContext>,
    /// An agent turn's stop signal, followed as the executor's cancellation.
    stop: Option<Arc<crate::agent::TurnStop>>,
    interactions: Option<FilesystemInteractionHost<P, A, O>>,
    owner: Option<(TaskJournalOwner<P>, OperationId)>,
    execution: Option<(OperationId, OperationId)>,
    verified: tokio::sync::Mutex<SummaryCache>,
    conversation_projection: tokio::sync::Mutex<Option<StreamAggregate<P>>>,
    history_read_limits: Option<crate::store::HistoryReadLimits>,
    task_files: Option<Arc<crate::task_volume::TaskFiles<P>>>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}
