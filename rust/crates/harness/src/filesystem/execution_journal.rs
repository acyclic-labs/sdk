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
use acyclic_stream::{AppendOutcome, IdempotencyKey as StreamKey, StreamError, StreamProvider};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

mod conversation;

mod canonical;
mod configuration;
mod content;
mod interactions;
mod observations;
mod replay;
mod selected;
mod summary;
#[cfg(test)]
mod tests;
mod traits;
use selected::JournalSelectedResolver;
use summary::*;

/// Durable, ref-only journal for one exact agent-private volume.
pub struct FilesystemExecutionJournal<P, A, O> {
    stream: Arc<P>,
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    schemas: SchemaRegistry,
    maximum_payload_bytes: u64,
    input_verifier: Option<Arc<dyn ContentResidencyVerifier>>,
    interactions: Option<FilesystemInteractionHost<P, A, O>>,
    owner: Option<(TaskJournalOwner<P>, OperationId)>,
    execution: Option<(OperationId, OperationId)>,
    verified: tokio::sync::Mutex<ExecutionSummary>,
    conversation_projection: tokio::sync::Mutex<Option<StreamAggregate<P>>>,
    history_read_limits: Option<crate::store::HistoryReadLimits>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}
