//! Stream-backed execution observations with private Filesystem payloads.

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
use super::fork_preparer_io::{PreparationIo, PreparationRecord};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::Arc,
};

mod conversation;

#[derive(Default)]
struct ExecutionSummary {
    operation: Option<OperationId>,
    tail: u64,
    retries: BTreeMap<[u8; 32], (u64, [u8; 32])>,
    contexts: BTreeMap<u32, bool>,
    models: BTreeMap<(u32, crate::executor::ModelPurpose), Option<ModelEventAdmission>>,
    tools: BTreeMap<(u32, [u8; 32]), bool>,
}

impl ExecutionSummary {

    fn require_next(&self, event: &ExecutionEvent, io: Option<&PreparationIo>) -> Result<Option<[u8; 32]>> {
        let hashed = match event {
            ExecutionEvent::ToolStarted { call_id, .. } if self.tail > 0 && !call_id.is_empty() =>
                call_id.len() as u64,
            ExecutionEvent::ToolCompleted { call_id, .. }
            | ExecutionEvent::ToolFailed { call_id, .. } => call_id.len() as u64,
            _ => 0,
        };
        let work = acyclic_fs::WorkCounters { bytes_hashed: hashed, ..Default::default() };
        if let Some(io) = io { io.preflight(work)?; }
        let mut tool_key = None;
        let valid = match event {
            ExecutionEvent::Started { request_digest } => {
                self.tail == 0 && *request_digest != [0; 32]
            }
            ExecutionEvent::ContextPrepared { step, .. } => {
                self.tail > 0
                    && !self.contexts.contains_key(step)
                    && !self
                        .models
                        .contains_key(&(*step, crate::executor::ModelPurpose::Response))
            }
            ExecutionEvent::ContextCompacted { step, .. } => {
                self.contexts.get(step) == Some(&false)
                    && matches!(
                        self.models
                            .get(&(*step, crate::executor::ModelPurpose::Summary)),
                        Some(None)
                    )
                    && !self
                        .models
                        .contains_key(&(*step, crate::executor::ModelPurpose::Response))
            }
            ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } => {
                self.tail > 0
                    && *request_digest != [0; 32]
                    && !self.models.contains_key(&(*step, *purpose))
            }
            ExecutionEvent::Model { step, purpose, .. } => self
                .models
                .get(&(*step, *purpose))
                .is_some_and(Option::is_some),
            ExecutionEvent::ToolStarted { step, call_id, .. } => {
                self.tail > 0
                    && !call_id.is_empty()
                    && {
                        let key = *blake3::hash(call_id.as_bytes()).as_bytes();
                        tool_key = Some(key);
                        !self.tools.contains_key(&(*step, key))
                    }
            }
            ExecutionEvent::ToolCompleted { step, call_id, .. }
            | ExecutionEvent::ToolFailed { step, call_id, .. } => {
                let key = *blake3::hash(call_id.as_bytes()).as_bytes();
                tool_key = Some(key);
                self.tools.get(&(*step, key)) == Some(&false)
            }
        };
        if let Some(io) = io { io.charge(work)?; }
        if valid {
            Ok(tool_key)
        } else {
            Err(canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                "execution observation lacks its dispatch or reuses a start"))
        }
    }

    fn validate_next(
        &self, event: &ExecutionEvent, model: Option<&ModelEvent>,
        limits: Limits, io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io { io.check_cancellation()?; }
        self.require_next(event, io)?;
        if let ExecutionEvent::Model { step, purpose, .. } = event {
            self.models
                .get(&(*step, *purpose))
                .and_then(Option::as_ref)
                .ok_or_else(|| canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                    "model dispatch is already settled"))?
                .validate_next(
                    model.ok_or_else(|| canonical_boundary(io, crate::wire::ErrorCode::Storage,
                        "model observation is missing"))?,
                    limits, io,
                )?;
        }
        Ok(())
    }

    fn accept(
        &mut self, record: &ExecutionRecord, model: Option<&ModelEvent>,
        limits: Limits, io: Option<&PreparationIo>,
    ) -> Result<()> {
        let key_work = acyclic_fs::WorkCounters { bytes_hashed: record.idempotency_key.len() as u64, ..Default::default() };
        if let Some(io) = io { io.preflight(key_work)?; }
        let key = *blake3::hash(record.idempotency_key.as_bytes()).as_bytes();
        if let Some(io) = io { io.charge(key_work)?; }
        if record.sequence != self.tail + 1
            || record.idempotency_key.is_empty()
            || self.retries.contains_key(&key)
        {
            return Err(canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                "execution journal sequence or retry identity is invalid"));
        }
        let tool_key = self.require_next(&record.event, io)?;
        let digest = super::task_runtime::observed::digest(&record.event, io, u64::MAX)?;
        if let Some(io) = io {
            let extra = match &record.event {
                ExecutionEvent::ContextPrepared { .. } => std::mem::size_of::<(u32, bool)>(),
                ExecutionEvent::ModelStarted { .. } => std::mem::size_of::<((u32, crate::executor::ModelPurpose), Option<ModelEventAdmission>)>(),
                ExecutionEvent::ToolStarted { .. } => std::mem::size_of::<((u32, [u8; 32]), bool)>(),
                _ => 0,
            };
            io.retain_owned((std::mem::size_of::<([u8; 32], (u64, [u8; 32]))>() + extra) as u64)?;
        }
        match &record.event {
            ExecutionEvent::ContextPrepared { step, .. } => {
                self.contexts.insert(*step, false);
            }
            ExecutionEvent::ContextCompacted { step, .. } => {
                self.contexts.insert(*step, true);
            }
            ExecutionEvent::ModelStarted { step, purpose, .. } => {
                self.models
                    .insert((*step, *purpose), Some(ModelEventAdmission::default()));
            }
            ExecutionEvent::Model { step, purpose, .. } => {
                let state = self
                    .models
                    .get_mut(&(*step, *purpose))
                    .ok_or_else(|| canonical_boundary(io, crate::wire::ErrorCode::Storage,
                        "model dispatch is missing"))?;
                let admission = state
                    .as_mut()
                    .ok_or_else(|| canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                        "model dispatch is already settled"))?;
                admission.observe(
                    model.ok_or_else(|| canonical_boundary(io, crate::wire::ErrorCode::Storage,
                        "model observation is missing"))?,
                    limits, io,
                )?;
                if admission.completed {
                    *state = None;
                }
            }
            ExecutionEvent::ToolStarted { step, .. } => {
                self.tools
                    .insert((*step, tool_key.expect("tool start has an authenticated identity")), false);
            }
            ExecutionEvent::ToolCompleted { step, .. }
            | ExecutionEvent::ToolFailed { step, .. } => {
                self.tools
                    .insert((*step, tool_key.expect("tool settlement has an authenticated identity")), true);
            }
            ExecutionEvent::Started { .. } => {}
        }
        self.retries.insert(key, (record.sequence, digest));
        self.tail = record.sequence;
        Ok(())
    }

    fn quiescent(&self, io: Option<&PreparationIo>) -> Result<bool> {
        for model in self.models.values() {
            super::fork_preparer_io::item(io, 0)?;
            if model.is_some() { return Ok(false); }
        }
        for settled in self.tools.values() {
            super::fork_preparer_io::item(io, 0)?;
            if !settled { return Ok(false); }
        }
        Ok(true)
    }
}

const OBSERVATION_VERSION: u32 = 2;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Observation {
    version: u32,
    operation_id: OperationId,
    turn_operation: OperationId,
    retry_digest: String,
    event: ExecutionEvent,
}

impl PreparationRecord for Observation {
    fn diagnostic_label_bytes() -> u64 {
        const PRIVATE_LABEL_BYTES: u64 = {
            let labels = [
                "Observation", "version", "operation_id", "turn_operation", "retry_digest", "event",
                "ExecutionEvent", "kind", "started", "context_prepared", "context_compacted",
                "model_started", "model", "tool_started", "tool_completed", "tool_failed",
                "request_digest", "step", "projection", "accounting", "compaction", "purpose",
                "request", "call_id", "invocation", "result", "reason", "ModelPurpose", "response",
                "summary", "ToolFailureKind", "executor_rejected", "invalid_output",
                "projection_rejected", "publication_rejected",
            ];
            let mut bytes = 0;
            let mut index = 0;
            while index < labels.len() {
                bytes += labels[index].len() as u64 + 4;
                index += 1;
            }
            bytes
        };
        crate::contract::preparation_serde_diagnostic_label_bytes() + PRIVATE_LABEL_BYTES
    }
}

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

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn original_native_execution_work_matches(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> bool {
        self._native_metadata.as_ref().is_some_and(|custody| work.shares_ledger(custody.work()))
    }

    /// Checks the task lease, execution and coordinator bindings.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn original_task_execution_binding_matches(
        &self, task: crate::TaskId, fence: &crate::scheduler::LeaseFence,
        turn: OperationId, operation: OperationId,
        host: &crate::durable_host::CoordinatorTaskHost<P>,
    ) -> bool {
        self.execution.as_ref().is_some_and(|identity|
            identity.0 == operation && identity.1 == turn)
            && self.owner.as_ref().is_some_and(|(owner, bound)|
                *bound == operation && owner.matches_task_binding(task, fence)
                    && owner.original_host_binding_matches(host))
    }

    /// Binds the execution to the task's exact lease and coordinator provider.
    #[allow(clippy::too_many_arguments)]
    pub fn for_task(
        owner: TaskJournalOwner<P>, operation_id: OperationId, turn_operation: OperationId,
        host: Arc<FilesystemHost<A, O>>, volume: &VolumeRef,
        verifier: &AuthorityVerifier, scope: &Scope, maximum_payload_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        owner.validate_storage(verifier, maximum_payload_bytes, &mut work)?;
        let mut journal = Self::new(owner.stream(), host, volume, verifier,
            &SchemaRegistry::new(), scope, maximum_payload_bytes, io)?;
        journal.owner = Some((owner, operation_id));
        journal.execution = Some((operation_id, turn_operation));
        Ok(journal)
    }

    /// Binds passive replay to an original turn without granting a task lease.
    pub(super) fn with_turn(mut self, operation: OperationId, turn: OperationId) -> Self {
        self.execution = Some((operation, turn));
        self
    }

    fn turn_operation(&self, operation: OperationId) -> OperationId {
        self.execution
            .as_ref()
            .map_or(operation, |identity| identity.1)
    }

    /// Admits the SDK Arc attempt while keeping metadata custody intrinsic.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        mut self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self._native_metadata.as_ref().is_some_and(|custody|
            custody.work().shares_ledger(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native journal sharing requires its original funded metadata",
            });
        }
        let io = PreparationIo::from_native_work(work)?;
        let mut body = work.retain_owned(std::mem::size_of::<Self>() as u64)
            .map_err(Error::from)?;
        self._native_metadata.as_mut().expect("original funded metadata checked")
            .absorb(&mut body)
            .map_err(|cause| Error::NativeWork { cause, custody: Some(body) })?;
        io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        Ok(Arc::new(self))
    }

    fn require_operation(
        &self, operation: OperationId, io: Option<&PreparationIo>,
    ) -> Result<()> {
        if self.execution.as_ref().is_some_and(|bound| bound.0 != operation) {
            return Err(canonical_boundary(io, crate::wire::ErrorCode::Unauthorized,
                "journal belongs to another task execution"));
        }
        Ok(())
    }

    async fn refresh_summary(
        &self, operation: OperationId, summary: &mut ExecutionSummary,
        io: Option<&PreparationIo>,
    ) -> Result<()>
    {
        if summary.operation != Some(operation) {
            *summary = ExecutionSummary {
                operation: Some(operation),
                ..ExecutionSummary::default()
            };
        }
        let limits = self
            .owner
            .as_ref()
            .map_or_else(Limits::default, |(owner, _)| owner.input_limits());
        loop {
            let before = io.map(PreparationIo::owned_bytes).transpose()?;
            let page_hold;
            let page = match io {
                #[cfg(not(target_arch = "wasm32"))]
                Some(io) if io.original_native_work().is_some() => {
                    let work = io.original_native_work().expect("matched original native work");
                    crate::NativeFutureBundle::new(work, || self.replay_page(
                        operation, summary.tail, EXECUTION_REPLAY_PAGE_RECORDS, true, Some(io)))?.await?
                }
                Some(io) => Box::pin(self.replay_page(
                    operation, summary.tail, EXECUTION_REPLAY_PAGE_RECORDS, true, Some(io))).await?,
                None => self.replay_page(
                    operation, summary.tail, EXECUTION_REPLAY_PAGE_RECORDS, true, None).await?,
            };
            page_hold = match (io, before) {
                (Some(io), Some(before)) => Some(io.guard_owned(io.owned_bytes()?.checked_sub(before)
                    .ok_or_else(|| super::fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Invalid, "execution page custody underflow"))?)?),
                _ => None,
            };
            if page.is_empty() {
                return Ok(());
            }
            for (record, model) in page {
                summary.accept(&record, model.as_ref(), limits, io)?;
            }
            drop(page_hold);
        }
    }

    /// Checks settled dispatches under their original owner and optional IO.
    pub async fn is_quiescent(&self, io: Option<&PreparationIo>) -> Result<bool> {
        let (owner, operation) = self.owner.as_ref().ok_or_else(|| canonical_boundary(
            io, crate::wire::ErrorCode::Unsupported, "quiescence requires a task-owned execution journal"))?;
        self.require_operation(*operation, io)?;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|history| history as &mut dyn crate::store::history::HistoryIo);
        owner.verify(true, &mut work).await?;
        let quiescent = {
            let mut shared = if io.is_none() { Some(self.verified.lock().await) } else { None };
            let summary_hold;
            let mut transient = ExecutionSummary::default();
            let summary = shared.as_deref_mut().unwrap_or(&mut transient);
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            self.refresh_summary(*operation, summary, io).await?;
            summary_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| io.invalid("execution summary custody underflow"))?)).transpose()?;
            let quiescent = summary.quiescent(io)?;
            drop(transient);
            drop(summary_hold);
            quiescent
        };
        owner.verify(true, &mut work).await?;
        Ok(quiescent)
    }

    /// Releases only after both configured original journals prove passive workflow state and settled dispatches.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn release_if_quiescent_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
        workflow: &super::FilesystemWorkflowJournal<P, A, O>,
    ) -> Result<bool>
    {
        let io = PreparationIo::from_native_work(work)?;
        let (owner, operation) = self.owner.as_ref().ok_or_else(|| Error::Unsupported(
            "quiescence requires a task-owned execution journal".into()))?;
        workflow.require_execution_owner(owner, &self.volume, self.verifier.audience())?;
        let result: Result<_> = async {
            let mut borrowed = io.history_io();
            owner.verify(true, &mut Some(&mut borrowed)).await?;
            let tail = {
                let before = io.owned_bytes()?;
                let summary_hold;
                let mut summary = ExecutionSummary::default();
                self.refresh_summary(*operation, &mut summary, Some(&io)).await?;
                summary_hold = io.guard_owned(io.owned_bytes()?.checked_sub(before)
                    .ok_or_else(|| Error::Invalid("execution summary custody underflow".into()))?)?;
                if !summary.quiescent(Some(&io))? { return Ok(None); }
                let tail = summary.tail;
                drop(summary);
                drop(summary_hold);
                tail
            };
            Ok(workflow.prove_release_with_io(&io).await?.map(|pending| (tail, pending)))
        }.await;
        let Some((tail, pending)) = result? else { return Ok(false); };
        owner.release_quiescent_execution_with_native_work(*operation, tail, &pending, work).await
    }

    /// Suspends only after the task-bound journal proves every dispatch settled.
    pub async fn suspend_workflow_command_if_quiescent(
        &self, revision: u64, command: OperationId, io: Option<&PreparationIo>,
    ) -> Result<()> {
        let span = obs_span!("acyclic.harness.journal.suspend", rev = revision);
        traced(span, async {
            let mut shared = if io.is_none() { Some(self.verified.lock().await) } else { None };
            let mut transient = ExecutionSummary::default();
            let summary = shared.as_deref_mut().unwrap_or(&mut transient);
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let result = async {
                let (owner, operation) = self.owner.as_ref().ok_or_else(|| {
                    #[cfg(not(target_arch = "wasm32"))]
                    if io.is_some_and(|io| io.original_native_work().is_some()) {
                        return Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                            reason: "quiescence requires a task-owned execution journal" };
                    }
                    Error::Unsupported("quiescence requires a task-owned execution journal".into())
                })?;
                self.refresh_summary(*operation, summary, io).await?;
                if !summary.quiescent(io)? {
                    return Err(Error::Indeterminate(*operation));
                }
                let mut borrowed = io.map(PreparationIo::history_io);
                owner.suspend_quiescent_execution_with_work(
                    *operation, summary.tail, revision, command,
                    &mut borrowed.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo),
                ).await
            }.await;
            let summary_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| io.invalid("execution summary custody underflow"))?)).transpose()?;
            drop(transient);
            drop(summary_hold);
            result
        }).await
    }

    async fn append_validated(
        &self, operation_id: OperationId, expected_tail: Option<u64>,
        claim_id: &str, event: &ExecutionEvent, io: Option<&PreparationIo>,
    ) -> Result<bool>
    {
        self.require_operation(operation_id, io)?;
        if claim_id.is_empty() {
            return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid,
                "execution journal compare-and-append is invalid"));
        }
        let owner = self.owner.as_ref().map(|(owner, _)| owner);
        let model = self.verify_event_refs_with_io(event, io).await?;
        let limits = owner.map_or_else(Limits::default, TaskJournalOwner::input_limits);
        self.verify_canonical_publication_with_io(operation_id, event, limits, io).await?;
        #[derive(Serialize)]
        struct BorrowedObservation<'a> {
            version: u32,
            operation_id: OperationId,
            turn_operation: OperationId,
            retry_digest: &'a str,
            event: &'a ExecutionEvent,
        }
        let hash_input_bytes = 37_u64.checked_add(claim_id.len() as u64)
            .ok_or_else(|| super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "execution retry identity length overflows"))?;
        let hash_input_hold = io.map(|io| io.reserve_decode(hash_input_bytes)).transpose()?;
        if let Some(io) = io {
            io.allocation_attempt(hash_input_bytes)?;
            let copying = acyclic_fs::WorkCounters { bytes_copied: hash_input_bytes, ..Default::default() };
            io.preflight(copying)?; io.charge(copying)?;
        }
        let mut hash_input = String::with_capacity(hash_input_bytes as usize);
        use std::fmt::Write as _;
        write!(&mut hash_input, "{operation_id}:{claim_id}")
            .map_err(|_| super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "execution retry identity formatting failed"))?;
        let hashing = acyclic_fs::WorkCounters { bytes_hashed: hash_input.len() as u64, ..Default::default() };
        if let Some(io) = io { io.preflight(hashing)?; io.charge(hashing)?; }
        let digest = blake3::hash(hash_input.as_bytes());
        drop(hash_input);
        drop(hash_input_hold);
        let retry_hold = io.map(|io| io.reserve_decode(64)).transpose()?;
        if let Some(io) = io {
            io.allocation_attempt(64)?;
            let copying = acyclic_fs::WorkCounters { bytes_copied: 64, ..Default::default() };
            io.preflight(copying)?; io.charge(copying)?;
        }
        let retry_digest = digest.to_hex().to_string();
        let observation = BorrowedObservation {
            version: OBSERVATION_VERSION,
            operation_id,
            turn_operation: self.turn_operation(operation_id),
            retry_digest: &retry_digest,
            event,
        };
        let body_hold;
        let bytes = match io {
            Some(io) => {
                let encoded = io.encode(&observation, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
                body_hold = io.guard_bytes(&encoded)?;
                encoded
            }
            None => {
                body_hold = None;
                let encoded = serde_json::to_vec(&observation).map_err(|error| Error::Invalid(error.to_string()))?;
                if encoded.len() > acyclic_stream::MAX_RECORD_BYTES {
                    return Err(Error::Invalid("execution journal observation exceeds Stream limit".into()));
                }
                encoded.into()
            }
        };
        let key_hold = io.map(|io| io.reserve_decode(32)).transpose()?;
        if let Some(io) = io {
            io.allocation_attempt(32)?;
            let copying = acyclic_fs::WorkCounters { bytes_copied: 32, ..Default::default() };
            io.preflight(copying)?; io.charge(copying)?;
        }
        let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes())).map_err(|error| {
            #[cfg(not(target_arch = "wasm32"))]
            if io.is_some() { return Error::NativeStream { code: crate::wire::ErrorCode::Invalid, source: error, custody: None }; }
            Error::Invalid(error.to_string())
        })?;
        let retry_hashing = acyclic_fs::WorkCounters { bytes_hashed: 64, ..Default::default() };
        if let Some(io) = io { io.preflight(retry_hashing)?; io.charge(retry_hashing)?; }
        let retry_key = *blake3::hash(retry_digest.as_bytes()).as_bytes();
        let event_digest = super::task_runtime::observed::digest(
            event, io, acyclic_stream::MAX_RECORD_BYTES as u64,
        )?;
        let mut shared = if io.is_none() { Some(self.verified.lock().await) } else { None };
        let mut transient = ExecutionSummary::default();
        let summary = shared.as_deref_mut().unwrap_or(&mut transient);
        loop {
            self.refresh_summary(operation_id, summary, io).await?;
            if let Some(owner) = owner {
                let mut borrowed = io.map(PreparationIo::history_io);
                owner.verify_execution_with_work(operation_id, event,
                    &mut borrowed.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo)).await?;
            }
            if let Some((sequence, previous)) = summary.retries.get(&retry_key) {
                if *previous != event_digest {
                    #[cfg(not(target_arch = "wasm32"))]
                    if io.is_some() {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                            reason: "execution journal retry identity reused" });
                    }
                    return Err(Error::Conflict("execution journal retry identity reused".into()));
                }
                return Ok(expected_tail.is_none_or(|tail| tail.checked_add(1) == Some(*sequence)));
            }
            if expected_tail.is_some_and(|tail| tail != summary.tail) { return Ok(false); }
            summary.validate_next(event, model.as_ref(), limits, io)?;
            let committed = if let Some(owner) = owner {
                let mut borrowed = io.map(PreparationIo::history_io);
                owner.append_execution_with_work(operation_id, summary.tail, &key, bytes.clone(), event,
                    &mut borrowed.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo)).await?
            } else {
                self.append_local_with_io(operation_id, summary.tail, key.clone(), bytes.clone(),
                    &retry_digest, event, io).await?
            };
            if committed {
                drop(key); drop(key_hold); drop(bytes); drop(body_hold);
                drop(retry_digest); drop(retry_hold);
                return Ok(true);
            }
        }
    }

    async fn verify_canonical_publication_with_io(
        &self, operation_id: OperationId, event: &ExecutionEvent,
        limits: Limits, io: Option<&PreparationIo>,
    ) -> Result<()>
    {
        if let Some(io) = io { io.check_cancellation()?; }
        let ExecutionEvent::ContextCompacted {
            step,
            projection,
            compaction,
            ..
        } = event
        else {
            return Ok(());
        };
        let mut history = io.map(PreparationIo::history_io);
        limits.validate_file(projection, &mut history.as_mut().map(
            |history| history as &mut dyn crate::store::history::HistoryIo))?;
        let projection = crate::executor::load_json::<
            crate::executor::ResponseProjection>(self, projection, io).await?;
        crate::context::validate_projected_context(&projection.0.context, limits, io)?;
        let Some(reference) = &projection.0.checkpoint else {
            if projection.0.canonical.is_some() {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Invalid,
                    "canonical publication has no checkpoint"));
            }
            return Ok(());
        };
        let mut history = io.map(PreparationIo::history_io);
        limits.validate_file(reference, &mut history.as_mut().map(
            |history| history as &mut dyn crate::store::history::HistoryIo))?;
        let envelope = crate::executor::load_json::<
            crate::context::CanonicalContextCheckpoint>(self, reference, io).await?;
        envelope.0.validate(limits, io)?;
        if *step != 0
            || envelope.0.operation_id != operation_id
            || projection.0.canonical.as_ref() != Some(&envelope.0.source)
            || &envelope.0.compaction != compaction
        {
            return Err(canonical_boundary(io, crate::wire::ErrorCode::Invalid,
                "canonical publication differs from its envelope"));
        }
        let source = crate::executor::load_json::<crate::context::Context>(
            self, &envelope.0.source, io).await?;
        let retained = crate::executor::load_json::<crate::context::Context>(
            self, &envelope.0.retained, io).await?;
        let proof = crate::executor::load_json::<crate::context::CompactionReference>(
            self, compaction, io).await?;
        envelope.0.validate_projection(&source.0, &retained.0, &proof.0, limits, io)?;
        self.verify_canonical_source_with_io(&envelope.0, &source.0, limits, io).await?;
        self.verify_canonical_summary_with_io(operation_id, &envelope.0, &source.0, &proof.0, limits, io).await
    }

    pub(super) fn context_history_limits(
        &self,
        limits: Limits,
    ) -> Result<crate::store::HistoryReadLimits> {
        limits.validate(&mut None)?;
        Ok(if let Some(limits) = self.history_read_limits {
            limits
        } else {
            let maximum_events = u32::try_from(limits.context_messages).map_err(|_| {
                Error::Invalid("canonical delta count exceeds portable history bound".into())
            })?;
            let maximum_bytes = (acyclic_stream::MAX_RECORD_BYTES as u64)
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_mul(u64::from(maximum_events)))
                .ok_or_else(|| Error::Invalid("canonical delta byte bound overflows".into()))?;
            crate::store::HistoryReadLimits {
                maximum_events,
                maximum_bytes,
            }
        })
    }

    async fn verify_canonical_source_with_io(
        &self, envelope: &crate::context::CanonicalContextCheckpoint,
        source: &crate::context::Context, limits: Limits, io: Option<&PreparationIo>,
    ) -> Result<()>
    {
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let result = async {
            let mut history = io.map(PreparationIo::history_io);
            limits.validate(&mut history.as_mut().map(
                |history| history as &mut dyn crate::store::history::HistoryIo))?;
            self.require_operation(envelope.operation_id, io)?;
            if self.execution.is_some() {
                let association_hold;
                let association_before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                let records = self.replay_page(envelope.operation_id, 0, 1, true, io).await?;
                association_hold = io.map(|io| io.guard_owned(
                    io.owned_bytes()?.checked_sub(association_before)
                        .ok_or_else(|| io.invalid("checkpoint association custody underflows"))?,
                )).transpose()?;
                drop(records);
                drop(association_hold);
            }
            let previous = match &envelope.selection.checkpoint {
                Some(reference) => Some(crate::executor::load_canonical_checkpoint(
                    self, reference, limits, None, io).await?),
                None => None,
            };
            let after = previous.as_ref()
                .map_or(0, |(previous, _)| previous.selection.conversation_revision);
            let verifier = self.input_verifier.as_ref().ok_or_else(|| canonical_boundary(
                io, crate::wire::ErrorCode::Unsupported, "canonical source reader is not bound"))?;
            let history_limits = if let Some(limits) = self.history_read_limits { limits } else {
                let maximum_events = u32::try_from(limits.context_messages).map_err(|_| canonical_boundary(
                    io, crate::wire::ErrorCode::Invalid,
                    "canonical delta count exceeds portable history bound"))?;
                let maximum_bytes = (acyclic_stream::MAX_RECORD_BYTES as u64).checked_mul(2)
                    .and_then(|bytes| bytes.checked_mul(u64::from(maximum_events)))
                    .ok_or_else(|| canonical_boundary(io, crate::wire::ErrorCode::Invalid,
                        "canonical delta byte bound overflows"))?;
                crate::store::HistoryReadLimits { maximum_events, maximum_bytes }
            };
            let mut borrowed = io.map(PreparationIo::history_io);
            let reader = crate::store::HistoryReader::new(&self.stream, self.verifier.audience(), &self.verifier, &mut borrowed.as_mut().map(|work| work as &mut dyn crate::store::HistoryIo))?;
            let event = match borrowed.as_mut() {
                Some(work) => crate::store::operations::find_operation_bounded_with_work(
                    &self.stream, self.verifier.audience(), &self.verifier, self.turn_operation(envelope.operation_id),
                    history_limits.maximum_bytes, true,
                    &mut Some(work as &mut dyn crate::store::history::HistoryIo)).await?.0,
                None => reader.operation_event(self.turn_operation(envelope.operation_id), 2 * acyclic_stream::MAX_RECORD_BYTES as u64, &mut None).await.map(|(event, _)| event)?,
            }.ok_or_else(|| canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                "checkpoint source selection is not the committed admission"))?;
            let crate::core::EventPayload::ModelContextSelected { selection } = &event.payload else {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                    "checkpoint operation is not a model-context selection"));
            };
            if selection != &envelope.selection {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                    "checkpoint source selection is not the committed admission"));
            }
            let authority = match borrowed.as_mut() {
                Some(work) => crate::store::history::HistoryIo::clone_authority(
                    work, self.verifier.audience())?,
                None => self.verifier.audience().clone(),
            };
            let cursor = crate::store::HistoryCursor {
                authority, after_revision: 0, through_revision: event.revision,
            };
            let loaded = reader.conversation_range(
                &cursor, after, envelope.selection.conversation_revision, history_limits,
                &mut borrowed.as_mut().map(|work| work as &mut dyn crate::store::history::HistoryIo),
            ).await?;
            let delta = project_canonical_delta_with_io(
                self, loaded, &envelope.selection, verifier.as_ref(), limits, io).await?;
            let expected = match previous {
                Some((_, retained)) => crate::context::ContextPipeline::continue_base(
                    retained, delta, limits, io)?,
                None => delta,
            };
            let matches = match io {
                Some(io) => {
                    let _actual_hold;
                    let actual = io.encode(source, limits.render_bytes, true)?;
                    _actual_hold = io.guard_bytes(&actual)?;
                    let _expected_hold;
                    let expected = io.encode(&expected, limits.render_bytes, true)?;
                    _expected_hold = io.guard_bytes(&expected)?;
                    io.check_cancellation()?;
                    actual == expected
                }
                None => source == &expected,
            };
            if !matches {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                    "checkpoint source differs from its canonical base and delta"));
            }
            Ok(())
        }.await;
        if let Some(io) = io { io.release(io.owned_bytes()? - before)?; }
        result
    }

    async fn verify_canonical_summary_with_io(
        &self, operation: OperationId,
        envelope: &crate::context::CanonicalContextCheckpoint,
        source: &crate::context::Context, proof: &crate::context::CompactionReference,
        limits: Limits, io: Option<&PreparationIo>,
    ) -> Result<()>
    {
        use crate::executor::ModelPurpose;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let result = async {
            let summary = proof.summary.as_ref().filter(|summary| summary.step == 0)
                .ok_or_else(|| canonical_boundary(io, crate::wire::ErrorCode::Invalid,
                    "canonical summary belongs to another step"))?;
            let (_, records) = crate::executor::replay_execution(
                self, operation, limits.model_events_per_step.saturating_add(2),
                |event| matches!(event,
                    ExecutionEvent::ContextPrepared { step: 0, .. }
                    | ExecutionEvent::ModelStarted { step: 0, purpose: ModelPurpose::Summary, .. }
                    | ExecutionEvent::Model { step: 0, purpose: ModelPurpose::Summary, .. }),
                io,
            ).await?;
            let mut prepared = None;
            let mut request = None;
            let mut output = String::new();
            let mut admission = ModelEventAdmission::default();
            for record in &records {
                super::fork_preparer_io::item(io, 0)?;
                match &record.event {
                    ExecutionEvent::ContextPrepared { projection, .. } => {
                        prepared = Some(crate::executor::load_json::<
                            crate::executor::ResponseProjection>(self, projection, io).await?);
                    }
                    ExecutionEvent::ModelStarted { request: reference, .. } => {
                        request = Some(crate::executor::load_model_request(
                            self, reference, limits, io).await?);
                    }
                    ExecutionEvent::Model { event, .. } => {
                        let event = crate::executor::load_json::<ModelEvent>(
                            self, event, io).await?;
                        admission.observe(&event.0, limits, io)?;
                        match &event.0 {
                            ModelEvent::Content { delta } => {
                                canonical_push_text(&mut output, delta, limits.render_bytes, io)?;
                            }
                            ModelEvent::Completed { .. } | ModelEvent::Reasoning { .. } => {}
                            ModelEvent::ToolCall { .. } => {
                                return Err(canonical_boundary(io, crate::wire::ErrorCode::Invalid,
                                    "canonical summary requested a tool"));
                            }
                        }
                    }
                    _ => return Err(canonical_boundary(io, crate::wire::ErrorCode::Storage,
                        "canonical summary selection is invalid")),
                }
            }
            if prepared.as_ref().and_then(|projection| projection.0.canonical.as_ref())
                != Some(&envelope.source) || !admission.completed || output.is_empty() {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                    "canonical summary lacks its prepared source or settled output"));
            }
            let request = request.ok_or_else(|| canonical_boundary(io,
                crate::wire::ErrorCode::Conflict, "canonical summary has no admitted request"))?;
            verify_summary_prefix_with_io(&request, source, summary.source_messages, limits, io)?;
            let mut history = io.map(PreparationIo::history_io);
            limits.validate_file(&summary.output, &mut history.as_mut().map(
                |history| history as &mut dyn crate::store::history::HistoryIo))?;
            let bytes = match io {
                #[cfg(not(target_arch = "wasm32"))]
                Some(io) if io.original_native_work().is_some() => {
                    let work = io.original_native_work().expect("matched original native work");
                    let bytes = self.load_with_native_work(&summary.output, work)?.await?;
                    bytes
                }
                Some(io) => io.receive_content(self.load_measured(
                    &summary.output, io.remaining()?, io.cancellation()).await)?,
                None => self.load(&summary.output).await?,
            };
            if let Some(io) = io { io.check_cancellation()?; }
            let matches = bytes.as_ref() == output.as_bytes();
            if !matches {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                    "canonical summary output differs from admitted observations"));
            }
            Ok(())
        }.await;
        if let Some(io) = io { io.release(io.owned_bytes()? - before)?; }
        result
    }

    async fn append_local_with_io(
        &self, operation_id: OperationId, tail: u64, key: StreamKey,
        bytes: acyclic_stream::OwnedBytes, retry_digest: &str, event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<bool>
    {
        let path_hold = io.map(|io| io.reserve_decode(
            2 * ("harness/v2/execution/".len() + 36) as u64)).transpose()?;
        if let Some(io) = io {
            io.allocation_attempt(("harness/v2/execution/".len() + 36) as u64)?;
            io.allocation_attempt(("harness/v2/execution/".len() + 36) as u64)?;
        }
        let stream = self.path(operation_id)?;
        let appended = match io {
            Some(io) => {
                let mut borrowed = io.history_io();
                crate::distributed::append(
                    (&stream, &self.stream), [bytes], 0, Some(tail), key, operation_id, &mut Some(&mut borrowed),
                ).await
            }
            None => stream.append_batch(vec![bytes], Some(tail), Some(key)).await
                .map_err(|error| match error {
                    StreamError::IdempotencyMismatch =>
                        Error::Conflict("execution journal retry identity reused".into()),
                    _ => Error::Indeterminate(operation_id),
                }),
        };
        drop(stream); drop(path_hold);
        match appended {
            Ok(AppendOutcome::Committed(receipt))
                if receipt.start == tail && Some(receipt.end) == tail.checked_add(1) => Ok(true),
            Ok(AppendOutcome::Committed(_)) => {
                #[cfg(not(target_arch = "wasm32"))]
                if io.is_some() {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                        reason: "execution journal append receipt is invalid" });
                }
                Err(Error::Storage("execution journal append receipt is invalid".into()))
            }
            Ok(AppendOutcome::TailConflict { .. }) => Ok(false),
            Err(Error::Indeterminate(_)) => {
                let records = self.replay_page(operation_id, tail, 1, true, io).await?;
                if records.iter().any(|(record, _)|
                    record.idempotency_key == retry_digest && &record.event == event) {
                    Ok(true)
                } else { Err(Error::Indeterminate(operation_id)) }
            }
            Err(error @ Error::Conflict(_)) => {
                let records = self.replay_page(operation_id, tail, 1, true, io).await?;
                if records.iter().any(|(record, _)|
                    record.idempotency_key == retry_digest && &record.event == event) {
                    Ok(true)
                } else { Err(error) }
            }
            #[cfg(not(target_arch = "wasm32"))]
            Err(error @ Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, .. })
            | Err(error @ Error::NativeStream { source: StreamError::IdempotencyMismatch, .. }) => {
                let records = self.replay_page(operation_id, tail, 1, true, io).await?;
                if records.iter().any(|(record, _)|
                    record.idempotency_key == retry_digest && &record.event == event) {
                    Ok(true)
                } else { Err(error) }
            }
            Err(error) => Err(error),
        }
    }

    /// Binds the pinned extension registry with optional original preparation IO.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: Arc<P>, host: Arc<FilesystemHost<A, O>>, volume: &VolumeRef,
        verifier: &AuthorityVerifier, schemas: &SchemaRegistry, scope: &Scope,
        maximum_payload_bytes: u64, io: Option<&PreparationIo>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let invalid = |reason: &'static str| {
            #[cfg(not(target_arch = "wasm32"))]
            if io.is_some_and(|io| io.original_native_work().is_some()) {
                return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
            }
            Error::Invalid(reason.into())
        };
        super::fork_preparer_io::item(io, 0)?;
        if volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider
            || maximum_payload_bytes == 0
        {
            return Err(invalid("execution journal requires a private volume and positive limit"));
        }
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            let maximum = io.map(|io| io.remaining().map(|budget| budget.bytes_encoded)).transpose()?.unwrap_or(u64::MAX);
            crate::executor::with_history_io(io, |work|
                ContentGrant::authorize_volume(verifier, scope, volume, operation, maximum, work))?;
        }
        if !matches!(verifier.audience().kind,
            crate::core::AggregateKind::Conversation | crate::core::AggregateKind::Task)
        {
            return Err(invalid("execution journal requires a task or conversation authority"));
        }
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        let volume = volume.copy(&mut work)?;
        let verifier = verifier.copy(&mut work)?;
        let schemas = schemas.copy(&mut work)?;
        let scope = scope.copy(&mut work)?;
        let interactions = match verifier.audience().kind {
            crate::core::AggregateKind::Conversation => Some(FilesystemInteractionHost::new(
                stream.clone(), Arc::clone(&host), verifier.audience(), &verifier,
                &schemas, &scope, &volume, maximum_payload_bytes, io,
            )?),
            crate::core::AggregateKind::Task => None,
            _ => unreachable!("execution authority validated before assembly"),
        };
        #[cfg(not(target_arch = "wasm32"))]
        let _native_metadata = io.filter(|io| io.original_native_work().is_some())
            .map(|io| io.take_native_custody(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| io.invalid("execution journal custody underflows"))?)).transpose()?;
        Ok(Self {
            stream,
            host,
            volume,
            verifier,
            scope,
            schemas,
            maximum_payload_bytes,
            input_verifier: None,
            interactions,
            owner: None,
            execution: None,
            verified: tokio::sync::Mutex::new(ExecutionSummary::default()),
            conversation_projection: tokio::sync::Mutex::new(None),
            history_read_limits: None,
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata,
        })
    }

    /// Binds tickets to the conversation owner with optional original preparation IO.
    pub fn with_interaction_owner(
        mut self, verifier: &AuthorityVerifier, schemas: &SchemaRegistry,
        scope: &Scope, private_volume: &VolumeRef, io: Option<&PreparationIo>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            let guard = self._native_metadata.as_mut().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native interaction binding requires original journal custody",
            })?;
            let mut identity = io.take_native_custody(0)?;
            guard.absorb(&mut identity).map_err(|cause| Error::NativeWork {
                cause, custody: Some(identity),
            })?;
        }
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo);
        if let Some((owner, _)) = &self.owner {
            owner.require_interaction_grant(&mut work)?;
        } else if self.verifier.audience().kind == crate::core::AggregateKind::Task {
            return Err(super::interaction_host::interaction_boundary(
                io, crate::wire::ErrorCode::Unauthorized,
                "task interaction binding requires an admitted journal owner"));
        }
        self.interactions = Some(FilesystemInteractionHost::new(
            Arc::clone(&self.stream), Arc::clone(&self.host), verifier.audience(), verifier,
            schemas, scope, private_volume, self.maximum_payload_bytes, io,
        )?);
        Ok(self)
    }

    /// Sets a separate finite work/byte allowance for cold canonical deltas.
    pub fn with_history_read_limits(
        mut self,
        limits: crate::store::HistoryReadLimits,
    ) -> Result<Self> {
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
            return Err(Error::Invalid(
                "cold history read bounds must be positive".into(),
            ));
        }
        self.history_read_limits = Some(limits);
        Ok(self)
    }

    /// Routes turn-input attachment admission through an explicitly bound provider set.
    #[must_use]
    pub fn with_input_verifier(mut self, verifier: Arc<dyn ContentResidencyVerifier>) -> Self {
        self.input_verifier = Some(verifier);
        self
    }

    fn interactions(&self) -> Result<&FilesystemInteractionHost<P, A, O>> {
        self.interactions.as_ref().ok_or_else(|| {
            Error::Unsupported("task execution has no conversation interaction owner".into())
        })
    }

    fn path(&self, operation_id: OperationId) -> Result<acyclic_stream::Stream<P>>
    {
        acyclic_stream::Stream::new(Arc::clone(&self.stream),
            crate::distributed::execution_path(operation_id, &mut None)?.as_str())
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    /// Resolves an interaction with an authenticated responder grant.
    pub async fn resolve_interaction(
        &self, id: InteractionId, response: InteractionResponse, responder: &Scope,
    ) -> Result<()> {
        let span = obs_span!(INFO, "acyclic.harness.journal.resolve_interaction");
        traced(span, async {
            let host = self.interactions()?;
            host.verify_responder_scope_with_io(id, responder, None)?;
            if let Some((owner, _)) = &self.owner {
                owner.require_interaction_grant(&mut None)?;
                owner.verify(true, &mut None).await?;
            }
            let (ticket, prior) = host.read(id, None).await?
                .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?;
            let request = host.read_request(id, None).await?
                .ok_or_else(|| Error::Storage("admitted interaction has no request".into()))?;
            request.validate_response(&response, &mut None)?;
            if let Some(previous) = &prior && previous.outcome.is_terminal() {
                let stored = match (&previous.outcome, &response) {
                    (InteractionOutcome::Approved, InteractionResponse::Approval { approved: true, .. })
                    | (InteractionOutcome::Declined, InteractionResponse::Approval { approved: false, .. }) => {
                        Some(host.read_resolution_file(id, true, None).await?
                            .ok_or_else(|| Error::Storage("admitted approval detail is missing".into()))?)
                    }
                    (InteractionOutcome::Answered { .. }, _) => {
                        Some(host.read_resolution_file(id, false, None).await?
                            .ok_or_else(|| Error::Storage("admitted answer is missing".into()))?)
                    }
                    _ => None,
                };
                let same = match stored {
                    Some(bytes) => crate::contract::json_from_slice::<InteractionResponse>(&bytes)
                        .map_err(|error| Error::Storage(error.to_string()))? == response,
                    None => false,
                };
                if let Some((owner, _)) = &self.owner { owner.verify(true, &mut None).await?; }
                return if same { Ok(()) } else { Err(Error::Conflict("interaction was already resolved".into())) };
            }
            let expected_version = prior.as_ref().map_or(Ok(1_u64), |value|
                value.expected_version.checked_add(1)
                    .ok_or_else(|| Error::Invalid("interaction version exhausted".into())))?;
            let digest = blake3::hash(format!("interaction:{}:resolve-{expected_version}",
                uuid::Uuid::from_bytes(id.into_bytes()).hyphenated()).as_bytes());
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&digest.as_bytes()[..16]);
            let operation = OperationId::from_bytes(bytes);
            if let Some((owner, _)) = &self.owner { owner.verify(true, &mut None).await?; }
            let staged = host.stage_answer(id, expected_version, operation, &response, None).await?;
            let (outcome, detail) = match response {
                InteractionResponse::Approval { approved: true, .. } => (InteractionOutcome::Approved, Some(staged)),
                InteractionResponse::Approval { approved: false, .. } => (InteractionOutcome::Declined, Some(staged)),
                _ => (InteractionOutcome::Answered { answer: Box::new(staged) }, None),
            };
            host.resolve(operation, responder, InteractionResolution {
                id: ticket.id, expected_version, outcome, detail,
            }, None).await?;
            if let Some((owner, _)) = &self.owner { owner.verify(true, &mut None).await?; }
            Ok(())
        }).await
    }

    /// Refreshes the shared authoritative conversation reducer.
    pub(super) async fn conversation_projection(
        &self,
        limits: Limits,
    ) -> Result<tokio::sync::MappedMutexGuard<'_, StreamAggregate<P>>>
    {
        limits.validate(&mut None)?;
        let verifier = self.input_verifier.as_ref().ok_or_else(|| {
            Error::Unsupported("conversation content verifier is not bound".into())
        })?;
        let mut cached = self.conversation_projection.lock().await;
        if cached.is_none() {
            *cached = Some(
                StreamAggregate::open(
                    &self.stream,
                    self.verifier.audience().clone(),
                    self.verifier.clone(),
                    self.schemas.clone(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                .await?
                .with_content_verifier(Arc::clone(verifier)),
            );
        }
        let mut aggregate = tokio::sync::MutexGuard::try_map(cached, Option::as_mut)
            .map_err(|_| Error::Storage("conversation projection is absent".into()))?;
        aggregate.set_limits(limits)?;
        let through = aggregate.tail_revision().await?;
        if through.saturating_sub(aggregate.reducer().revision())
            > u64::from(crate::store::DEFAULT_PROJECTION_EVENTS)
        {
            *aggregate = StreamAggregate::open(
                &self.stream,
                self.verifier.audience().clone(),
                self.verifier.clone(),
                self.schemas.clone(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
            .await?
            .with_content_verifier(Arc::clone(verifier));
            aggregate.set_limits(limits)?;
            return Ok(aggregate);
        }
        while !aggregate
            .refresh_through(through, EXECUTION_REPLAY_PAGE_RECORDS)
            .await?
        {}
        Ok(aggregate)
    }

    fn stage_identity_with_io(
        &self, operation_id: OperationId, idempotency_key: &str,
        io: Option<&PreparationIo>,
    ) -> Result<(IdempotencyKey, String)> {
        let digest = {
            let _input_hold;
            let input = crate::contract::preparation_format(
                format_args!("{operation_id}:{idempotency_key}"), io)?;
            _input_hold = io.map(|io| io.guard_owned(input.capacity() as u64)).transpose()?;
            if let Some(io) = io {
                let hashing = acyclic_fs::WorkCounters { bytes_hashed: input.len() as u64, ..Default::default() };
                io.preflight(hashing)?;
                io.charge(hashing)?;
            }
            blake3::hash(input.as_bytes())
        };
        let key = IdempotencyKey::new(crate::contract::preparation_format(
            format_args!("journal-{}", digest.to_hex()), io)?)?;
        let path = crate::contract::preparation_format(
            format_args!(".system/execution/{operation_id}/{}.json", digest.to_hex()), io)?;
        Ok((key, path))
    }

    async fn stage_with_io(
        &self, operation_id: OperationId, idempotency_key: &str, bytes: &[u8],
        media_type: &'static str, io: Option<&PreparationIo>,
    ) -> Result<FileRef>
    {
        if bytes.len() as u64 > self.maximum_payload_bytes {
            return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "execution journal payload exceeds limit"));
        }
        let _identity_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let (key, path) = self.stage_identity_with_io(operation_id, idempotency_key, io)?;
        _identity_hold = io.map(|io| {
            let retained = io.owned_bytes()?.checked_sub(before).ok_or_else(||
                super::fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Invalid, "staging identity ownership underflow"))?;
            io.guard_owned(retained)
        }).transpose()?;
        match io {
            Some(io) => self.host.put_internal_content_original_with_io(
                &self.volume, &self.verifier, &self.scope, &path, bytes, media_type,
                "execution.json", self.maximum_payload_bytes, &key, InternalContentClass::Execution, io,
            ).await,
            None => {
                let grant = ContentGrant::verify(
                    &self.verifier, &self.scope, &self.volume, VolumeOperation::Write, u64::MAX, &mut None)?;
                self.host.put_internal_content(
                    &self.volume, &grant, &path, bytes, media_type, "execution.json",
                    self.maximum_payload_bytes, &key, InternalContentClass::Execution,
                ).await
            }
        }
    }

    async fn load_content(&self, reference: &FileRef, io: Option<&PreparationIo>) -> Result<acyclic_stream::OwnedBytes>
    {
        if reference.volume() != &self.volume {
            return Err(canonical_boundary(io, crate::wire::ErrorCode::Unauthorized,
                "journal content belongs to another private volume"));
        }
        let maximum = io.map(|io| io.remaining().map(|budget| budget.bytes_encoded)).transpose()?.unwrap_or(u64::MAX);
        crate::executor::with_history_io(io, |work|
            ContentGrant::authorize_volume(&self.verifier, &self.scope, &self.volume,
                VolumeOperation::Read, maximum, work))?;
        if io.is_none() { reference.validate(&mut None)?; }
        let bytes = self.host.read_pinned_io(reference, self.maximum_payload_bytes, io).await?;
        if io.is_none() { self.host.retain_file_generation(reference).await?; }
        Ok(bytes)
    }

    fn journal_read_stream_error(
        io: Option<&PreparationIo>, code: crate::wire::ErrorCode, source: StreamError,
    ) -> Error {
        #[cfg(not(target_arch = "wasm32"))]
        if io.and_then(PreparationIo::original_native_work).is_some() {
            return Error::NativeStream { code, source, custody: None };
        }
        if code == crate::wire::ErrorCode::Invalid {
            Error::Invalid(source.to_string())
        } else {
            Error::Storage(source.to_string())
        }
    }

    pub(super) async fn replay_page(
        &self, operation_id: OperationId, after: u64, maximum: u32,
        retain_models: bool, io: Option<&PreparationIo>,
    ) -> Result<Vec<(ExecutionRecord, Option<ModelEvent>)>>
    {
        self.require_operation(operation_id, io)?;
        if maximum == 0 {
            return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "execution replay page bound is invalid"));
        }
        let capacity = maximum.min(EXECUTION_REPLAY_PAGE_RECORDS) as usize;
        let vector_bytes = capacity.checked_mul(std::mem::size_of::<(ExecutionRecord, Option<ModelEvent>)>())
            .ok_or_else(|| super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "execution replay vector ownership overflow"))? as u64;
        if let Some(io) = io {
            io.retain_owned(vector_bytes)?;
            io.allocation_attempt(vector_bytes)?;
        }
        let mut result: Vec<(ExecutionRecord, Option<ModelEvent>)> = Vec::with_capacity(capacity);
        let _path_hold;
        let path = crate::contract::preparation_format(
            format_args!("harness/v2/execution/{operation_id}"), io)?;
        _path_hold = io.map(|io| io.guard_owned(path.capacity() as u64)).transpose()?;
        let _stream_hold;
        _stream_hold = if let Some(io) = io {
            let copying = acyclic_fs::WorkCounters {
                bytes_copied: path.len() as u64, items_examined: path.len() as u64,
                ..Default::default()
            };
            io.preflight(copying)?;
            let hold = io.reserve_decode(path.len() as u64)?;
            io.allocation_attempt(path.len() as u64)?;
            io.charge(copying)?;
            Some(hold)
        } else { None };
        let stream = acyclic_stream::Stream::new(Arc::clone(&self.stream), &path)
            .map_err(|error| Self::journal_read_stream_error(io, crate::wire::ErrorCode::Invalid, error))?;
        let _read_path_hold;
        _read_path_hold = if let Some(io) = io {
            let call = acyclic_fs::WorkCounters {
                backend_read_operations: 1, bytes_copied: path.len() as u64, ..Default::default()
            };
            io.preflight(call)?;
            let hold = io.reserve_decode(path.len() as u64)?;
            io.allocation_attempt(path.len() as u64)?;
            io.charge(call)?;
            Some(hold)
        } else { None };
        let mut entries = match stream.read(after, capacity as u32).await {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => {
                if let Some(io) = io { io.check_cancellation()?; }
                return Ok(result);
            }
            Err(error) => return Err(Self::journal_read_stream_error(io, crate::wire::ErrorCode::Storage, error)),
        };
        loop {
            let _record_hold;
            if let Some(io) = io { io.check_cancellation()?; }
            let Some(record) = entries.try_next().await
                .map_err(|error| Self::journal_read_stream_error(io, crate::wire::ErrorCode::Storage, error))? else {
                    if let Some(io) = io { io.check_cancellation()?; }
                    break;
                };
            _record_hold = if let Some(io) = io {
                let read = acyclic_fs::WorkCounters {
                    authority_records_read: 1, authority_bytes_read: record.value.len() as u64,
                    source_bytes_read: record.value.len() as u64, items_examined: 1, ..Default::default()
                };
                io.charge(read)?;
                io.retain_owned(record.value.len() as u64)?;
                Some(io.guard_owned(record.value.len() as u64)?)
            } else { None };
            if record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Storage,
                    "execution Stream record exceeds protocol bound"));
            }
            let sequence = after.checked_add(result.len() as u64)
                .ok_or_else(|| super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "execution replay sequence overflow"))?;
            if result.len() >= capacity || record.sequence != sequence {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Storage,
                    "execution journal sequence or bound is invalid"));
            }
            let observation: Observation = match io {
                Some(io) => crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<Observation>, &record.value, crate::wire::ErrorCode::Storage)?),
                None => crate::contract::json_from_slice(&record.value)
                    .map_err(|error| Error::Storage(format!("execution journal record is invalid: {error}")))?,
            };
            if observation.version != OBSERVATION_VERSION {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Storage,
                    "execution journal observation version is unsupported"));
            }
            if observation.operation_id != operation_id {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Storage,
                    "execution journal identity is invalid"));
            }
            if observation.turn_operation != self.turn_operation(operation_id) {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                    "execution journal belongs to another original turn"));
            }
            for (previous, _) in &result {
                super::fork_preparer_io::item(io, 0)?;
                if previous.idempotency_key == observation.retry_digest {
                    return Err(canonical_boundary(io, crate::wire::ErrorCode::Storage,
                        "execution journal identity is invalid"));
                }
            }
            let before_model = io.map(PreparationIo::owned_bytes).transpose()?;
            let _model_hold;
            let model = self.verify_event_refs_with_io(&observation.event, io).await?;
            _model_hold = if retain_models {
                None
            } else {
                match (io, before_model) {
                    (Some(io), Some(before)) => Some(io.guard_owned(io.owned_bytes()?.checked_sub(before)
                        .ok_or_else(|| super::fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Invalid, "model observation custody underflow"))?)?),
                    _ => None,
                }
            };
            let model = if retain_models { model } else { drop(model); None };
            result.push((
                ExecutionRecord {
                    operation_id,
                    sequence: record.sequence.checked_add(1).ok_or_else(||
                        super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "execution replay sequence overflow"))?,
                    idempotency_key: observation.retry_digest,
                    event: observation.event,
                },
                model,
            ));

        }
        if let Some((owner, _)) = &self.owner {
            if let Some(io) = io {
                let mut work = io.history_io();
                owner.verify_model_history(
                    operation_id, result.iter().map(|(record, _)| record), &mut Some(&mut work),
                ).await?;
            } else {
                owner.verify_model_history(
                    operation_id, result.iter().map(|(record, _)| record), &mut None).await?;
            }
        }
        Ok(result)
    }

    async fn records(
        &self, operation: OperationId, after: u64, maximum: u32, io: Option<&PreparationIo>,
    ) -> Result<Vec<ExecutionRecord>> {
        let page_slots;
        let page = self.replay_page(operation, after, maximum, io.is_none(), io).await?;
        let page_bytes = page.capacity().checked_mul(std::mem::size_of::<(ExecutionRecord, Option<ModelEvent>)>())
            .ok_or_else(|| super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "execution replay vector ownership overflow"))? as u64;
        page_slots = io.map(|io| io.guard_owned(page_bytes)).transpose()?;
        let output_bytes = page.len().checked_mul(std::mem::size_of::<ExecutionRecord>())
            .ok_or_else(|| super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "execution replay vector ownership overflow"))? as u64;
        if let Some(io) = io { io.retain_owned(output_bytes)?; io.allocation_attempt(output_bytes)?; }
        let mut records = Vec::with_capacity(page.len());
        for (record, _) in page { records.push(record); }
        drop(page_slots);
        Ok(records)
    }

    async fn verify_event_refs_with_io(&self, event: &ExecutionEvent, io: Option<&PreparationIo>) -> Result<Option<ModelEvent>>
    {
        let refs = match event {
            ExecutionEvent::ContextPrepared { projection, accounting, .. } =>
                [Some(projection), accounting.as_ref(), None],
            ExecutionEvent::ContextCompacted { projection, compaction, accounting, .. } =>
                [Some(projection), Some(compaction), Some(accounting)],
            ExecutionEvent::ModelStarted { request, .. } => [Some(request), None, None],
            ExecutionEvent::Model { event, .. } => [Some(event), None, None],
            ExecutionEvent::ToolStarted { invocation, .. } => [Some(invocation), None, None],
            ExecutionEvent::ToolCompleted { result, projection, .. } => [Some(result), Some(projection), None],
            ExecutionEvent::Started { .. } | ExecutionEvent::ToolFailed { .. } => [None, None, None],
        };
        let mut model = None;
        for reference in refs.into_iter().flatten() {
            if reference.volume() != &self.volume {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Unauthorized,
                    "journal event refers to another private volume"));
            }
            let _bytes_hold;
            super::fork_preparer_io::item(io, 0)?;
            let owned_bytes = self.load_content(reference, io).await?;
            _bytes_hold = io.map(|io| io.guard_bytes(&owned_bytes)).transpose()?.flatten();
            let bytes = owned_bytes.as_ref();
            if let ExecutionEvent::ModelStarted { request_digest, .. } = event {
                let limits = self
                    .owner
                    .as_ref()
                    .map_or_else(Limits::default, |(owner, _)| owner.input_limits());
                let before = io.map(PreparationIo::owned_bytes).transpose()?;
                let _prepared_hold;
                let prepared = match io {
                    Some(io) => crate::model::PreparedModelRequest::decode(&bytes, limits, Some(io))?,
                    None => crate::model::PreparedModelRequest::decode(&bytes, limits, None)?,
                };
                _prepared_hold = match (io, before) {
                    (Some(io), Some(before)) => Some(io.guard_owned(io.owned_bytes()?.checked_sub(before)
                        .ok_or_else(|| super::fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Invalid, "prepared model request custody underflow"))?)?),
                    _ => None,
                };
                if &prepared.request_digest() != request_digest {
                    return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid,
                        "model start digest differs from its retained request"));
                }
            }
            if matches!(event, ExecutionEvent::Model { .. }) {
                if reference.descriptor().media_type() != "application/json" {
                    return Err(canonical_boundary(io, crate::wire::ErrorCode::Storage,
                        "model observation is not JSON"));
                }
                model = Some(match io {
                    Some(io) => {
                        let parsed = io.decode(std::marker::PhantomData::<ModelEvent>, &bytes, crate::wire::ErrorCode::Storage)?;
                        let _canonical_hold;
                        let canonical = io.encode(&parsed.0, self.maximum_payload_bytes, true)?;
                        _canonical_hold = io.guard_bytes(&canonical)?;
                        if canonical.as_ref() != bytes {
                            return Err(canonical_boundary(Some(io), crate::wire::ErrorCode::Storage,
                                "execution journal JSON is not canonical"));
                        }
                        crate::filesystem::retain_decoded(parsed)
                    }
                    None => decode_json::<ModelEvent>(&bytes, None)?.0,
                });
            }
        }
        Ok(model)
    }

    async fn verify_input_file_with_io(&self, reference: &FileRef, io: Option<&PreparationIo>) -> Result<()>
    {
        if reference.descriptor().byte_length() > self.maximum_payload_bytes {
            return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "turn input file exceeds journal limit"));
        }
        if let Some((owner, _)) = &self.owner
            && owner.verify_created_input_with_io(reference, io).await?
        {
            return Ok(());
        }
        if reference.volume().provider() != &self.host.provider {
            let verifier = self.input_verifier.as_ref().ok_or_else(|| canonical_boundary(
                io, crate::wire::ErrorCode::Unsupported, "turn input file belongs to another content provider"))?;
            return verifier.verify(reference, self.maximum_payload_bytes, io)?.await;
        }
        if let Some(io) = io {
            super::fork_preparer_io::item(Some(io), reference.logical_owned_bytes()?)?;
            reference.validate(&mut None)?;
            if reference.volume().class() == VolumeClass::AgentPrivate
                && reference.volume().owner() != self.volume.owner()
            {
                if crate::conversation::is_internal_path(reference.path())
                    && !crate::conversation::is_inherited_context_path(reference.path())
                {
                    return Err(canonical_boundary(Some(io), crate::wire::ErrorCode::Unauthorized,
                        "internal storage file is not public content"));
                }
                let mut work = io.history_io();
                self.verifier.verify(&self.scope, &mut Some(&mut work))?;
                if !io.contains_capability(self.scope.capabilities(), reference, "file:read:")? {
                    return Err(canonical_boundary(Some(io), crate::wire::ErrorCode::Unauthorized,
                        "exact file read is not granted"));
                }
            } else {
                crate::executor::with_history_io(Some(io), |work|
                    ContentGrant::authorize_volume(&self.verifier, &self.scope, reference.volume(),
                        VolumeOperation::Read, io.remaining()?.bytes_encoded, work))?;
            }
            let before = io.owned_bytes()?;
            let _bytes_hold;
            let bytes = self.host.read_pinned_io(reference, self.maximum_payload_bytes, Some(io)).await?;
            _bytes_hold = io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| super::fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Invalid, "input file custody underflow"))?)?;
            drop(bytes);
        } else {
            let grant = if reference.volume().class() == VolumeClass::AgentPrivate
                && reference.volume().owner() != self.volume.owner()
            {
                ContentGrant::verify_file_read(&self.verifier, &self.scope, reference)?
            } else {
                ContentGrant::verify(&self.verifier, &self.scope, reference.volume(), VolumeOperation::Read, self.maximum_payload_bytes, &mut None)?
            };
            self.host.read_content(reference, &grant, self.maximum_payload_bytes).await?;
        }
        Ok(())
    }

    async fn verify_model_content_with_io(&self, content: &ModelContent, io: Option<&PreparationIo>) -> Result<()>
    {
        let mut count = 0_usize;
        for _ in content.file_refs() {
            super::fork_preparer_io::item(io, 0)?;
            count = count.checked_add(1).ok_or_else(||
                super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "model reference count overflow"))?;
        }
        let references_hold;
        let bytes = (count as u64).checked_mul(std::mem::size_of::<(&FileRef, Option<&crate::model::NativeConfigurationBinding>)>() as u64)
            .ok_or_else(|| super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "model reference ownership overflow"))?;
        if let Some(io) = io {
            io.retain_owned(bytes)?;
            references_hold = Some(io.guard_owned(bytes)?);
            if count != 0 { io.allocation_attempt(bytes)?; }
        } else {
            references_hold = None;
        }
        let mut references = Vec::with_capacity(count);
        for (file, binding) in content.file_bindings() {
            if let Some((owner, _)) = &self.owner {
                owner.validate_input_file(file, io)?;
            }
            if file.descriptor().byte_length() > self.maximum_payload_bytes {
                return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "model content exceeds journal limit"));
            }
            references.push((file, binding));
        }
        for (_, binding) in &references {
            if let Some(binding) = binding {
                let (owner, _) = self.owner.as_ref().ok_or_else(|| canonical_boundary(
                    io, crate::wire::ErrorCode::Unsupported, "native options require an original task owner"))?;
                owner.validate_native_configuration(binding, io)?;
            }
        }
        for (_, binding) in &references {
            if let Some(binding) = binding {
                let (owner, _) = self.owner.as_ref().ok_or_else(|| canonical_boundary(
                    io, crate::wire::ErrorCode::Unsupported, "native options require an original task owner"))?;
                owner.verify_native_configuration(binding, io).await?;
            }
        }
        for (file, _) in &references {
            let mut configuration = false;
            for (_, binding) in &references {
                super::fork_preparer_io::item(io, 0)?;
                if binding.is_some_and(|binding| &binding.configuration.content == *file) {
                    configuration = true;
                    break;
                }
            }
            if !configuration { self.verify_input_file_with_io(file, io).await?; }
        }
        drop(references);
        drop(references_hold);
        Ok(())
    }
    async fn verify_tool_approval_with_io(
        &self, id: InteractionId, operation: OperationId, digest: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        self.require_operation(operation, io)?;
        let host = self.interactions.as_ref().ok_or_else(||
            super::interaction_host::interaction_boundary(io, crate::wire::ErrorCode::Unsupported,
                "task execution has no conversation interaction owner"))?;
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo);
        if let Some((owner, _)) = &self.owner {
            owner.require_interaction_grant(&mut work)?;
            owner.verify(true, &mut work).await?;
        }
        host.verify_approval_with_io(id, operation, digest, io).await?;
        if let Some((owner, _)) = &self.owner { owner.verify(true, &mut work).await?; }
        Ok(())
    }
}

impl<P, A, O> ToolApprovalVerifier for FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn verify<'a>(
        &'a self,
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.verify_tool_approval_with_io(id, operation, digest, None))
    }

    fn verify_measured<'a>(
        &'a self, id: InteractionId, operation: OperationId, digest: [u8; 32],
        budget: acyclic_fs::WorkBudget, cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let io = super::fork_preparer_io::PreparationIo::from_budget(budget, cancellation)
                .map_err(|error| acyclic_fs::OperationFailure::new(error, Default::default()))?;
            let result = self.verify_tool_approval_with_io(id, operation, digest, Some(&io)).await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(
        &'a self, id: InteractionId, operation: OperationId, digest: [u8; 32],
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.verify_tool_approval_with_io(id, operation, digest, Some(&io)).await
        })
    }
}

impl<P, A, O> ExecutionJournal for FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn canonical_authority(&self) -> Option<&crate::core::Authority> {
        Some(self.verifier.audience())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_execution(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining().map_err(Error::from)?;
        Ok(true)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn replay_with_native_work<'a>(
        &'a self, operation_id: OperationId, after: u64, maximum: u32,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<ExecutionRecord>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let records = self.records(operation_id, after, maximum, Some(&io)).await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((records, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stage_with_native_work<'a>(
        &'a self, operation_id: OperationId, idempotency_key: &'a str, bytes: &'a [u8],
        media_type: &'static str, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        work.remaining().map_err(Error::from)?;
        let (owner, _) = self.owner.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native staging requires an original dispatch owner",
        })?;
        owner.require_volume_grant_with_native_work(&self.volume, VolumeOperation::Write, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            owner.verify_with_native_work(false, work).await?;
            let file = self.stage_with_io(
                operation_id, idempotency_key, bytes, media_type, Some(&io)).await?;
            owner.verify_with_native_work(false, work).await?;
            let retained = file.logical_owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((file, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn staged_ref_with_native_work<'a>(
        &'a self, operation_id: OperationId, idempotency_key: &'a str,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        work.remaining().map_err(Error::from)?;
        let (owner, _) = self.owner.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "original staged-payload observation requires a task-owned journal",
        })?;
        owner.require_volume_grant_with_native_work(&self.volume, VolumeOperation::Read, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let file = {
                owner.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                let _identity_hold;
                let before = io.owned_bytes()?;
                let (key, path) = self.stage_identity_with_io(operation_id, idempotency_key, Some(&io))?;
                let retained = io.owned_bytes()?.checked_sub(before).ok_or_else(||
                    super::fork_preparer_io::failure(Some(&io), crate::wire::ErrorCode::Invalid, "staging identity ownership underflow"))?;
                _identity_hold = io.guard_owned(retained)?;
                let file = self.host.original_internal_content_for_operation_with_io(
                    &self.volume, &self.verifier, &self.scope, &key, &path,
                    self.maximum_payload_bytes, InternalContentClass::Execution, &io,
                ).await?.ok_or_else(|| canonical_boundary(
                    Some(&io), crate::wire::ErrorCode::NotFound,
                    "original staged payload was not published"))?;
                owner.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                file
            };
            let retained = file.logical_owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((file, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn load_with_native_work<'a>(
        &'a self, reference: &'a FileRef, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let bytes = self.load_content(reference, Some(&io)).await?;
            drop(io);
            Ok(bytes)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_input_file_with_native_work<'a>(
        &'a self, reference: &'a FileRef, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.verify_input_file_with_io(reference, Some(&io)).await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_model_content_with_native_work<'a>(
        &'a self, content: &'a ModelContent, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.verify_model_content_with_io(content, Some(&io)).await
        })
    }

    fn replay<'a>(
        &'a self,
        operation_id: OperationId,
        after: u64,
        maximum: u32,
    ) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>> {
        let span = obs_span!(
            "acyclic.harness.journal.replay",
            rev = after,
            items = crate::obs::Empty
        );
        traced(span, async move {
            let records = self.records(operation_id, after, maximum, None).await?;
            crate::obs::obs_record!("items" = records.len());
            Ok(records)
        })
    }
    fn replay_measured<'a>(
        &'a self,
        operation_id: OperationId,
        after: u64,
        maximum: u32,
        budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<ExecutionRecord>>, Error>> {
        Box::pin(async move {
            let io = PreparationIo::from_budget(budget, cancellation)
                .map_err(|cause| acyclic_fs::OperationFailure::new(cause, Default::default()))?;
            let result = async {
                let records = self.records(operation_id, after, maximum, Some(&io)).await?;
                io.remaining()?;
                Ok(records)
            }.await;
            result.map(|value| acyclic_fs::OperationReceipt { value, work: io.receipt() })
                .map_err(|cause| acyclic_fs::OperationFailure::new(cause, io.receipt()))
        })
    }

    fn append<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<()>> {
        let span = obs_span!("acyclic.harness.journal.append");
        traced(span, async move {
            self.append_validated(operation_id, None, &idempotency_key, &event, None)
                .await
                .map(|_| ())
        })
    }

    fn append_if_tail<'a>(
        &'a self,
        operation_id: OperationId,
        expected_tail: u64,
        claim_id: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<bool>> {
        let span = obs_span!(
            "acyclic.harness.journal.append_if_tail",
            rev = expected_tail
        );
        traced(span, async move {
            self.append_validated(operation_id, Some(expected_tail), &claim_id, &event, None)
                .await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn append_with_native_work<'a>(
        &'a self, operation_id: OperationId, idempotency_key: &'a str,
        event: &'a ExecutionEvent, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let result = self.append_validated(
                operation_id, None, idempotency_key, event, Some(&io)).await;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            drop(custody);
            result.map(|_| ())
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn append_if_tail_with_native_work<'a>(
        &'a self, operation_id: OperationId, expected_tail: u64, idempotency_key: &'a str,
        event: &'a ExecutionEvent, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, bool>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let result = self.append_validated(
                operation_id, Some(expected_tail), idempotency_key, event, Some(&io)).await;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            drop(custody);
            result
        })
    }

    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        bytes: Vec<u8>,
        media_type: &'static str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        let span = obs_span!("acyclic.harness.journal.stage", bytes = bytes.len());
        traced(span, async move {
            self.stage_with_io(operation_id, &idempotency_key, &bytes, media_type, None).await
        })
    }

    fn load<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
        let span = obs_span!("acyclic.harness.journal.load", bytes = crate::obs::Empty);
        traced(span, async move {
            let bytes = self.load_content(reference, None).await?;
            crate::obs::obs_record!("bytes" = bytes.len());
            Ok(bytes)
        })
    }

    fn load_measured<'a>(
        &'a self, reference: &'a FileRef, budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<acyclic_stream::OwnedBytes>, Error>> {
        Box::pin(async move {
            let io = super::PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = self.load_content(reference, Some(&io)).await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    fn verify_input_file<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.verify_input_file_with_io(reference, None))
    }

    fn verify_input_file_measured<'a>(
        &'a self, reference: &'a FileRef, budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let io = PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = self.verify_input_file_with_io(reference, Some(&io)).await;
            result.map(|value| acyclic_fs::OperationReceipt { value, work: io.receipt() })
                .map_err(|cause| acyclic_fs::OperationFailure::new(cause, io.receipt()))
        })
    }

    fn verify_model_content<'a>(&'a self, content: &'a ModelContent) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.verify_model_content_with_io(content, None))
    }

    fn verify_model_content_measured<'a>(
        &'a self, content: &'a ModelContent, budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let io = PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = self.verify_model_content_with_io(content, Some(&io)).await;
            result.map(|value| acyclic_fs::OperationReceipt { value, work: io.receipt() })
                .map_err(|cause| acyclic_fs::OperationFailure::new(cause, io.receipt()))
        })
    }

    fn verify_selected_context<'a>(
        &'a self, operation_id: OperationId, selected: &'a SelectedModelContext,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.verify_selected_context_with_io(operation_id, selected, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_selected_context_with_native_work<'a>(
        &'a self, operation_id: OperationId, selected: &'a SelectedModelContext,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.verify_selected_context_with_io(operation_id, selected, Some(&io)).await
        })
    }

    fn open_interaction<'a>(
        &'a self, id: InteractionId, interaction: Interaction,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.open_interaction_with_io(id, &interaction, None).await })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn open_interaction_with_native_work<'a>(
        &'a self, id: InteractionId, interaction: &'a Interaction,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.open_interaction_with_io(id, interaction, Some(&io)).await
        })
    }

    fn interaction_outcome<'a>(
        &'a self, id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>> {
        Box::pin(self.interaction_outcome_with_io(id, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn interaction_outcome_with_native_work<'a>(
        &'a self, id: InteractionId, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<InteractionOutcome>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let outcome = self.interaction_outcome_with_io(id, Some(&io)).await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((outcome, custody))
        })
    }
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
{
    async fn verify_selected_context_with_io(
        &self, operation_id: OperationId, selected: &SelectedModelContext, io: Option<&PreparationIo>,
    ) -> Result<()> {
        self.require_operation(operation_id, io)?;
        let turn_operation = self.turn_operation(operation_id);
        {
            let association_hold;
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let records = self.replay_page(operation_id, 0, 1, true, io).await?;
            association_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| super::interaction_host::interaction_boundary(Some(io),
                    crate::wire::ErrorCode::Invalid, "selected association custody underflows"))?)).transpose()?;
            drop(records);
            drop(association_hold);
        }
        let _query_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let verifier = self.input_verifier.as_ref().ok_or_else(|| super::interaction_host::interaction_boundary(
            io, crate::wire::ErrorCode::Unsupported, "conversation content verifier is not bound"))?;
        let limits = self.owner.as_ref().map_or_else(Limits::default, |(owner, _)| owner.input_limits());
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo);
        limits.validate(&mut work)?;
        if selected.selection.message_ids.len() > limits.context_messages {
            return Err(super::interaction_host::interaction_boundary(
                io, crate::wire::ErrorCode::Invalid, "selected history exceeds context message limit"));
        }
        let lookup_bytes = 2 * acyclic_stream::MAX_RECORD_BYTES as u64;
        let (event, _) = crate::store::operations::find_operation_bounded_with_work(
            &self.stream, self.verifier.audience(), &self.verifier, turn_operation,
            lookup_bytes, true, &mut work,
        ).await?;
        let event = event.ok_or_else(|| super::interaction_host::interaction_boundary(
            io, crate::wire::ErrorCode::Conflict, "turn has no committed model-context selection"))?;
        let crate::core::EventPayload::ModelContextSelected { selection: committed } = event.payload else {
            return Err(super::interaction_host::interaction_boundary(
                io, crate::wire::ErrorCode::Conflict, "turn operation is not a model-context selection"));
        };
        if committed != selected.selection {
            return Err(super::interaction_host::interaction_boundary(
                io, crate::wire::ErrorCode::Conflict, "model-context selection does not match the committed turn"));
        }
        let slots = committed.message_ids.len().checked_mul(
            std::mem::size_of::<crate::conversation::ConversationMessage>())
            .ok_or_else(|| super::interaction_host::interaction_boundary(
                io, crate::wire::ErrorCode::Invalid, "selected history vector size overflows"))?;
        if let Some(io) = io {
            io.retain_owned(slots as u64)?;
            io.allocation_attempt(slots as u64)?;
        }
        let mut loaded = Vec::with_capacity(committed.message_ids.len());
        for id in &committed.message_ids {
            let (message, _) = crate::store::operations::find_message_with_work(
                &self.stream, self.verifier.audience(), &self.verifier, *id,
                event.revision, lookup_bytes, &mut work,
            ).await?;
            loaded.push(message.ok_or_else(|| super::interaction_host::interaction_boundary(
                io, crate::wire::ErrorCode::Storage, "committed selected message is missing at its cutoff"))?);
        }
        let (historical, _) = crate::conversation::ConversationState::from_wire(crate::conversation::ConversationStateWire { agent: None, messages: loaded, archived_prefix: None }, acyclic_stream::MAX_RECORD_BYTES as u64, &mut work)?;
        let (messages, attachments, render_bytes) = self.owner.as_ref().map_or(
            (crate::conversation::MAX_PORTABLE_COUNT, crate::conversation::MAX_PORTABLE_COUNT,
                self.maximum_payload_bytes),
            |(owner, _)| { let limits = owner.input_limits();
                (limits.context_messages, limits.attachments, limits.render_bytes) });
        let resolver = JournalSelectedResolver {
            journal: self, inner: verifier.as_ref(),
        };
        for message in historical.messages() {
            if message.kind != crate::conversation::MessageKind::ToolCall {
                self.verify_input_file_with_io(&message.content, io).await?;
            }
            match &message.attachments {
                crate::conversation::ReferencedAttachments::Inline { items } => {
                    for item in items { self.verify_input_file_with_io(&item.file, io).await?; }
                }
                crate::conversation::ReferencedAttachments::Manifest { manifest, item_count }
                    if message.kind == crate::conversation::MessageKind::ToolCall => {
                    if *item_count as usize > attachments {
                        return Err(canonical_boundary(io, crate::wire::ErrorCode::Invalid, "selected manifest exceeds attachment limit"));
                    }
                    let (items, hold) = crate::conversation::load_manifest(&resolver, manifest, *item_count, io).await?;
                    for item in &items { self.verify_input_file_with_io(&item.file, io).await?; }
                    drop(items);
                    drop(hold);
                }
                crate::conversation::ReferencedAttachments::Manifest { .. } => {}
            }
        }
        let projected = crate::projection::select_model_context(
            &historical,
            committed.conversation_revision,
            committed,
            &resolver,
            crate::conversation::Limits { context_messages: messages, attachments: attachments, render_bytes: render_bytes, ..Default::default() },
            attachments,
            io,
        ).await?;
        _query_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| super::interaction_host::interaction_boundary(Some(io),
                crate::wire::ErrorCode::Invalid, "selected query custody underflows"))?)).transpose()?;
        if &projected != selected {
            return Err(super::interaction_host::interaction_boundary(
                io, crate::wire::ErrorCode::Conflict, "model context differs from committed conversation projection"));
        }
        drop(projected);
        drop(historical);
        Ok(())
    }

    fn authorize_selected_file_with_io(&self, file: &FileRef, io: Option<&PreparationIo>) -> Result<()> {
        if let Some((owner, _)) = &self.owner {
            owner.validate_input_file(file, io)
        } else {
            let mut borrowed = io.map(PreparationIo::history_io);
            file.validate(&mut borrowed.as_mut().map(
                |work| work as &mut dyn crate::store::history::HistoryIo))
        }
    }

    async fn open_interaction_with_io(
        &self, id: InteractionId, interaction: &Interaction, io: Option<&PreparationIo>,
    ) -> Result<()> {
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo);
        interaction.validate(&mut work)?;
        if let Some((owner, _)) = &self.owner {
            owner.require_interaction_grant(&mut work)?;
            owner.verify(false, &mut work).await?;
        }
        let host = self.interactions.as_ref().ok_or_else(|| super::interaction_host::interaction_boundary(
            io, crate::wire::ErrorCode::Unsupported, "task execution has no conversation interaction owner"))?;
        {
            let request_hold;
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let original = host.read_request(id, io).await?;
            request_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| super::interaction_host::interaction_boundary(Some(io),
                    crate::wire::ErrorCode::Invalid, "interaction request custody underflows"))?)).transpose()?;
            if let Some(original) = &original {
                if let Some((owner, _)) = &self.owner { owner.verify(false, &mut work).await?; }
                return if original == interaction { Ok(()) } else {
                    Err(super::interaction_host::interaction_boundary(
                        io, crate::wire::ErrorCode::Conflict, "interaction identity reused"))
                };
            }
            drop(original);
            drop(request_hold);
        }
        let operation = interaction_open_operation(id, io)?;
        let ticket_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let ticket = host.stage_request_with_io(id, interaction, None, io).await?;
        ticket_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| super::interaction_host::interaction_boundary(Some(io),
                crate::wire::ErrorCode::Invalid, "interaction ticket custody underflows"))?)).transpose()?;
        let publication = host.open(
            operation, None, &ticket, self.owner.as_ref().map(|(owner, _)| owner), io).await;
        drop(ticket);
        drop(ticket_hold);
        if let Some((owner, _)) = &self.owner { owner.verify(false, &mut work).await?; }
        let result = match publication {
            Ok(_) => Ok(()),
            Err(error) if matches!(error.code(),
                crate::wire::ErrorCode::Conflict | crate::wire::ErrorCode::Indeterminate) => {
                let request_hold;
                let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                let original = host.read_request(id, io).await?;
                request_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                    .ok_or_else(|| super::interaction_host::interaction_boundary(Some(io),
                        crate::wire::ErrorCode::Invalid, "interaction request custody underflows"))?)).transpose()?;
                let result = match &original {
                    Some(original) if original == interaction => Ok(()),
                    Some(_) => Err(super::interaction_host::interaction_boundary(
                        io, crate::wire::ErrorCode::Conflict, "interaction identity reused")),
                    None => Err(Error::Indeterminate(operation)),
                };
                drop(original);
                drop(request_hold);
                result
            }
            Err(error) => Err(error),
        };
        if let Some((owner, _)) = &self.owner { owner.verify(false, &mut work).await?; }
        result
    }

    async fn interaction_outcome_with_io(
        &self, id: InteractionId, io: Option<&PreparationIo>,
    ) -> Result<Option<InteractionOutcome>> {
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo);
        if let Some((owner, _)) = &self.owner {
            owner.require_interaction_grant(&mut work)?;
            owner.verify(true, &mut work).await?;
        }
        let host = self.interactions.as_ref().ok_or_else(|| super::interaction_host::interaction_boundary(
            io, crate::wire::ErrorCode::Unsupported, "task execution has no conversation interaction owner"))?;
        let outcome = host.read_outcome_with_io(id, io).await?;
        if let Some((owner, _)) = &self.owner { owner.verify(true, &mut work).await?; }
        Ok(outcome)
    }
}

struct JournalSelectedResolver<'a, P, A, O> {
    journal: &'a FilesystemExecutionJournal<P, A, O>,
    inner: &'a dyn ContentResidencyVerifier,
}

impl<P, A, O> crate::conversation::ContentResidencyVerifier for JournalSelectedResolver<'_, P, A, O>
where P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
{
    fn verify<'a>(&'a self, _file: &'a FileRef, _maximum_bytes: u64, io: Option<&'a PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        Err(canonical_boundary(io, crate::wire::ErrorCode::Unsupported, "projection-only reader"))
    }

    fn supports_read(&self, volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<bool> {
        self.inner.supports_read(volume, io)
    }

    fn read<'a>(&'a self, file: &'a FileRef, maximum_bytes: u64, io: Option<&'a PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.journal.authorize_selected_file_with_io(file, io)?;
        self.inner.read(file, maximum_bytes, io)
    }
}

fn interaction_open_operation(id: InteractionId, io: Option<&PreparationIo>) -> Result<OperationId> {
    let mut source = [0_u8; 53];
    source[..12].copy_from_slice(b"interaction:");
    let mut encoded = [0_u8; 36];
    source[12..48].copy_from_slice(uuid::Uuid::from_bytes(id.into_bytes())
        .hyphenated().encode_lower(&mut encoded).as_bytes());
    source[48..].copy_from_slice(b":open");
    let hashed = acyclic_fs::WorkCounters { bytes_hashed: source.len() as u64, ..Default::default() };
    if let Some(io) = io { io.preflight(hashed)?; }
    let digest = blake3::hash(&source);
    if let Some(io) = io { io.charge(hashed)?; }
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(OperationId::from_bytes(bytes))
}

fn canonical_push_text(
    output: &mut String, delta: &str, maximum: u64, io: Option<&PreparationIo>,
) -> Result<()> {
    let length = output.len().checked_add(delta.len()).filter(|length| *length as u64 <= maximum)
        .ok_or_else(|| canonical_boundary(io, crate::wire::ErrorCode::Invalid,
            "canonical summary output exceeds bounds"))?;
    if let Some(io) = io {
        let old = output.capacity();
        let grows = length > old;
        let copy = acyclic_fs::WorkCounters {
            bytes_copied: delta.len() as u64 + if grows { output.len() as u64 } else { 0 },
            ..Default::default()
        };
        io.preflight(copy)?;
        if grows {
            io.retain_owned((length - old) as u64)?;
            io.allocation_attempt((length - old) as u64)?;
            output.try_reserve_exact(length - output.len()).map_err(|_| canonical_boundary(
                Some(io), crate::wire::ErrorCode::Storage, "canonical summary allocation failed"))?;
            if output.capacity() > length { io.retain_owned((output.capacity() - length) as u64)?; }
        }
        output.push_str(delta);
        io.charge(copy)?;
    } else {
        output.push_str(delta);
    }
    Ok(())
}

fn verify_summary_prefix_with_io(
    request: &crate::model::PreparedModelRequest, source: &crate::context::Context,
    count: u32, limits: Limits, io: Option<&PreparationIo>,
) -> Result<()> {
    let count = count as usize;
    let expected = count.checked_add(1).ok_or_else(|| canonical_boundary(
        io, crate::wire::ErrorCode::Invalid, "summary prefix count overflows"))?;
    let messages = &request.request().messages;
    let matches = match (messages.get(..count), source.messages.get(..count)) {
        (Some(actual), Some(expected_prefix)) if messages.len() == expected => match io {
            Some(io) => {
                let _actual_hold;
                let actual = io.encode(&actual, limits.render_bytes, true)?;
                _actual_hold = io.guard_bytes(&actual)?;
                let _expected_hold;
                let expected = io.encode(&expected_prefix, limits.render_bytes, true)?;
                _expected_hold = io.guard_bytes(&expected)?;
                io.check_cancellation()?;
                actual == expected
            }
            None => actual == expected_prefix,
        },
        _ => false,
    };
    if !matches {
        return Err(canonical_boundary(io, crate::wire::ErrorCode::Conflict,
            "canonical summary request differs from its source prefix"));
    }
    Ok(())
}

async fn project_canonical_delta_with_io<P, A, O>(
    journal: &FilesystemExecutionJournal<P, A, O>,
    loaded: Vec<crate::conversation::ConversationMessage>,
    selection: &crate::conversation::ModelContextSelection,
    verifier: &dyn ContentResidencyVerifier, limits: Limits, io: Option<&PreparationIo>,
) -> Result<crate::context::Context>
where P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
{
    let mut selected_ids = selection.message_ids.iter();
    for message in &loaded {
        super::fork_preparer_io::item(io, 0)?;
        if matches!(message.kind,
            crate::conversation::MessageKind::System | crate::conversation::MessageKind::User
            | crate::conversation::MessageKind::Assistant | crate::conversation::MessageKind::ToolCall
            | crate::conversation::MessageKind::ToolResult) {
            if selected_ids.next() != Some(&message.id) {
                return Err(canonical_boundary(io, crate::wire::ErrorCode::Conflict,
                    "checkpoint skips uncovered canonical history"));
            }
        }
    }
    if selected_ids.next().is_some() {
        return Err(canonical_boundary(io, crate::wire::ErrorCode::Conflict,
            "checkpoint skips uncovered canonical history"));
    }
    if io.is_some() {
        for message in &loaded {
            super::fork_preparer_io::item(io, 0)?;
            journal.verify_input_file_with_io(&message.content, io).await?;
            match &message.attachments {
                crate::conversation::ReferencedAttachments::Inline { items } => {
                    for item in items {
                        super::fork_preparer_io::item(io, 0)?;
                        journal.verify_input_file_with_io(&item.file, io).await?;
                    }
                }
                crate::conversation::ReferencedAttachments::Manifest { manifest, .. } => {
                    journal.verify_input_file_with_io(manifest, io).await?;
                }
            }
        }
    }
    let mut borrowed = io.map(PreparationIo::history_io);
    let (history, _) = crate::conversation::ConversationState::from_wire(crate::conversation::ConversationStateWire { agent: None, messages: loaded, archived_prefix: None }, acyclic_stream::MAX_RECORD_BYTES as u64, &mut borrowed.as_mut().map(|work| work as &mut dyn crate::store::history::HistoryIo))?;
    let mut message_ids = Vec::new();
    for id in &selection.message_ids { crate::executor::stock_vec_push(&mut message_ids, *id, io)?; }
    let selection = crate::conversation::ModelContextSelection {
        conversation_revision: selection.conversation_revision, message_ids,
        checkpoint: selection.checkpoint.as_ref().map(|file| crate::executor::with_history_io(io, |work| file.copy(work))).transpose()?,
    };
    let resolver = JournalSelectedResolver { journal, inner: verifier };
    let resolver: &dyn ContentResidencyVerifier = if io.is_some() { &resolver } else { verifier };
    let selected = select_model_context(
        &history,
        selection.conversation_revision,
        selection,
        resolver,
        limits,
        limits.attachments,
        io,
    ).await?;
    let input = crate::executor::with_history_io(io, |work| crate::model::clone_content(&selected.messages.last().ok_or_else(|| canonical_boundary(io, crate::wire::ErrorCode::Invalid, "canonical input is missing"))?.content, work).map(|(copy, _)| copy))?;
    let mut selected_delta = selected;
    selected_delta.selection.checkpoint = None;
    crate::context::ContextPipeline::base_context(
        &crate::context::ContextInput {
            input, selected_context: Some(selected_delta), step: 0, prior_messages: Vec::new(),
        }, limits, io,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId,
        conversation::{FileDescriptor, VolumeOwner},
        executor::ModelPurpose,
        resources::ProviderRef,
    };

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one production journal roundtrip and isolated persisted-header adversaries"
    )]
    async fn observation_reopen_preserves_original_turn_and_rejects_header_substitution()
    -> Result<()> {
        let provider = ProviderRef::new("observation-identity".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host = Arc::new(FilesystemHost::new(
            acyclic_fs::Fs::memory(),
            provider.clone(),
        )?);
        let agent = AgentId::new();
        let volume = VolumeRef::new(
            provider,
            "observation-identity",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        host.create_volume(&volume).await?;
        let issuer = crate::core::AuthorityIssuer::new(
            "observation-identity",
            [81; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Conversation,
                id: crate::ConversationId::new().to_string(),
            },
        );
        let scope = issuer.issue("owner", &crate::Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]), Some(agent), None, &mut None).expect("scope proof");
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let open = |operation, turn| {
            FilesystemExecutionJournal::new(
                stream.clone(),
                host.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &crate::core::SchemaRegistry::new(),
                &scope,
                4096,
                None,
            )
            .map(|journal| journal.with_turn(operation, turn))
        };
        let turn = OperationId::new();
        let operation = super::super::task_runtime::execution_operation(crate::TaskId::new(), turn, None)?;
        assert_ne!(operation, turn);
        let event = ExecutionEvent::Started {
            request_digest: [9; 32],
        };
        open(operation, turn)?
            .append(operation, "start".into(), event.clone())
            .await?;
        let reopened = open(operation, turn)?;
        let records = reopened.replay(operation, 0, 1).await?;
        assert_eq!(records.len(), 1);
        assert_eq!(records.first().map(|record| &record.event), Some(&event));
        reopened
            .append(operation, "start".into(), event.clone())
            .await?;
        assert_eq!(reopened.replay(operation, 0, 2).await?.len(), 1);
        assert!(matches!(
            open(operation, OperationId::new())?
                .replay(operation, 0, 1)
                .await,
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            reopened.replay(turn, 0, 1).await,
            Err(Error::Unauthorized(_))
        ));
        let summary_operation = ModelPurpose::Summary.attempt_operation(operation, None)?;
        assert_ne!(summary_operation, operation);
        for mutation in [
            "wrong_execution",
            "summary_attempt_as_execution",
            "wrong_turn",
            "missing_turn",
            "missing_version",
            "old_version",
            "unknown_version",
            "wrong_version_type",
            "unknown_field",
        ] {
            let target = OperationId::new();
            let mut wire = serde_json::to_value(Observation {
                version: OBSERVATION_VERSION, operation_id: target, turn_operation: turn,
                retry_digest: blake3::hash(b"start").to_hex().to_string(), event: event.clone(),
            })
                .map_err(|error| Error::Invalid(error.to_string()))?;
            match mutation {
                "wrong_execution" => wire["operation_id"] = serde_json::json!(operation),
                "summary_attempt_as_execution" => {
                    wire["operation_id"] = serde_json::json!(summary_operation);
                }
                "wrong_turn" => wire["turn_operation"] = serde_json::json!(OperationId::new()),
                "missing_turn" => {
                    wire.as_object_mut().ok_or_else(|| Error::Invalid("envelope".into()))?
                        .remove("turn_operation");
                }
                "missing_version" => {
                    wire.as_object_mut().ok_or_else(|| Error::Invalid("envelope".into()))?
                        .remove("version");
                }
                "old_version" => wire["version"] = serde_json::json!(1),
                "unknown_version" => wire["version"] = serde_json::json!(3),
                "wrong_version_type" => wire["version"] = serde_json::json!("2"),
                "unknown_field" => wire["event_operation"] = serde_json::json!(operation),
                _ => unreachable!("enumerated envelope mutation"),
            }
            let journal = open(target, turn)?;
            journal.path(target)?
                .append_batch(
                    vec![Bytes::from(serde_json::to_vec(&wire)
                        .map_err(|error| Error::Invalid(error.to_string()))?)],
                    Some(0),
                    None,
                )
                .await?;
            assert!(
                matches!(
                    open(target, turn)?.replay(target, 0, 1).await,
                    Err(Error::Storage(_) | Error::Conflict(_))
                ),
                "accepted {mutation}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        clippy::indexing_slicing,
        reason = "one real four-record canonical source barrier timeline uses its fixed fixture identities"
    )]
    async fn cold_canonical_source_checks_complete_mixed_delta_without_reducer_restore()
    -> Result<()> {
        use crate::conversation::{
            ContentPublisher as _, ConversationMessage, MessageKind, ModelContextSelection,
        };
        use crate::core::{Action, AggregateKind, Authority, AuthorityIssuer, Command};
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<
            acyclic_stream::MemoryStream,
        >::default());
        let client = provider.clone();
        let fs_provider = ProviderRef::new("cold-canonical-source".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host = Arc::new(FilesystemHost::new(
            acyclic_fs::Fs::memory(),
            fs_provider.clone(),
        )?);
        let agent = AgentId::new();
        let volume = VolumeRef::new(
            fs_provider,
            "cold-source",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        host.create_volume(&volume).await?;
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: crate::ConversationId::new().to_string(),
        };
        let issuer = AuthorityIssuer::new("cold-canonical-owner", [72; 32], authority.clone());
        let scope = issuer.issue("owner", &crate::Capabilities::new([
                "conversation:bind".into(),
                "conversation:append".into(),
                "conversation:select_context".into(),
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]), Some(agent), None, &mut None).expect("scope proof");
        let content = Arc::new(super::super::FilesystemContentVerifier::new(
            host.clone(),
            &issuer.verifier(&mut None).expect("verifier copy"),
            &scope,
            4096, None,
        )?);
        let publisher = super::super::FilesystemContentPublisher::new(
            host.clone(),
            &volume,
            &issuer.verifier(&mut None).expect("verifier copy"),
            &scope,
            4096, None,
        )?;
        let mut aggregate = StreamAggregate::open(
            &client,
            authority.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
        .await?
        .with_content_verifier(content.clone());
        aggregate.set_resident_event_limit(1)?;
        aggregate
            .execute(Command {
                operation_id: OperationId::new(),
                idempotency_key: crate::IdempotencyKey::new("cold-bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            }, #[cfg(feature = "filesystem")] None, &mut None)
            .await?;
        let mut ids = Vec::new();
        let mut references = Vec::new();
        for (position, kind) in [
            MessageKind::User,
            MessageKind::Interaction,
            MessageKind::Assistant,
            MessageKind::User,
        ]
        .into_iter()
        .enumerate()
        {
            let sequence = position as u64 + 1;
            let file = publisher
                .stage(
                    OperationId::new(),
                    &format!("messages/{sequence}.txt"),
                    format!("canonical-{sequence}").as_bytes(),
                    "text/plain",
                    "message.txt",
                )
                .await?;
            let id = uuid::Uuid::new_v4();
            let reply_to = if kind == MessageKind::Assistant {
                ids.first().copied()
            } else {
                None
            };
            aggregate
                .execute(Command {
                    operation_id: OperationId::new(),
                    idempotency_key: crate::IdempotencyKey::new(format!(
                        "cold-message-{sequence}"
                    ))?,
                    expected_revision: aggregate.reducer().revision(),
                    scope: scope.clone(),
                    causal_parent: None,
                    action: Action::AppendConversationMessage {
                        message: Box::new(ConversationMessage {
                            id,
                            sequence,
                            kind,
                            content: file.clone(),
                            attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
                            reply_to,
                            tool_call_id: None,
                            extensions: Default::default(),
                        }),
                    },
                }, #[cfg(feature = "filesystem")] None, &mut None)
                .await?;
            ids.push(id);
            references.push(file);
        }
        let omitted_operation = OperationId::new();
        let omitted = ModelContextSelection {
            conversation_revision: 4,
            message_ids: vec![ids[3]],
            checkpoint: None,
        };
        let complete_operation = OperationId::new();
        let complete = ModelContextSelection {
            conversation_revision: 4,
            message_ids: vec![ids[0], ids[2], ids[3]],
            checkpoint: None,
        };
        for (operation, selection) in [
            (omitted_operation, omitted.clone()),
            (complete_operation, complete.clone()),
        ] {
            aggregate
                .execute(Command {
                    operation_id: operation,
                    idempotency_key: crate::IdempotencyKey::new(format!(
                        "cold-selection:{operation}"
                    ))?,
                    expected_revision: aggregate.reducer().revision(),
                    scope: scope.clone(),
                    causal_parent: None,
                    action: Action::SelectModelContext { selection },
                }, #[cfg(feature = "filesystem")] None, &mut None)
                .await?;
        }
        let limits = Limits {
            context_messages: 3,
            file_bytes: 4096,
            render_bytes: 4096,
            ..Limits::default()
        };
        async fn source(
            history: &crate::conversation::ConversationState,
            selection: ModelContextSelection,
            content: &dyn ContentResidencyVerifier,
            limits: Limits,
        ) -> Result<crate::context::Context> {
            let selected = select_model_context(
                history,
                selection.conversation_revision,
                selection,
                content,
                limits,
                limits.attachments,
                None,
            )
            .await?;
            let input = selected
                .messages
                .last()
                .ok_or_else(|| Error::NotFound("cold current input".into()))?
                .content
                .clone();
            crate::context::ContextPipeline::base_context(
                &crate::context::ContextInput {
                    input,
                    selected_context: Some(selected),
                    step: 0,
                    prior_messages: Vec::new(),
                },
                limits, None,
            )
        }
        let history = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::NotFound("cold history".into()))?;
        let omitted_source = source(history, omitted.clone(), content.as_ref(), limits).await?;
        let complete_source = source(history, complete.clone(), content.as_ref(), limits).await?;
        let selected = select_model_context(
            history,
            complete.conversation_revision,
            complete.clone(),
            content.as_ref(),
            limits,
            limits.attachments,
            None,
        )
        .await?;
        let placeholder = references
            .last()
            .ok_or_else(|| Error::NotFound("cold placeholder".into()))?
            .clone();
        let envelope = |operation_id, selection| crate::context::CanonicalContextCheckpoint {
            operation_id,
            selection,
            source: placeholder.clone(),
            retained: placeholder.clone(),
            compaction: placeholder.clone(),
        };
        let new_journal = || {
            FilesystemExecutionJournal::new(
                client.clone(),
                host.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &SchemaRegistry::new(),
                &scope,
                4096,
                None,
            )
            .map(|journal| journal.with_input_verifier(content.clone()))
        };
        let journal = new_journal()?.with_history_read_limits(crate::store::HistoryReadLimits {
            maximum_events: 4,
            maximum_bytes: 16_384,
        })?;
        let execution_operation = super::super::task_runtime::execution_operation(
            crate::TaskId::new(),
            complete_operation, None,
        )?;
        assert_ne!(execution_operation, complete_operation);
        let bound = new_journal()?
            .with_turn(execution_operation, complete_operation)
            .with_history_read_limits(crate::store::HistoryReadLimits {
                maximum_events: 4,
                maximum_bytes: 16_384,
            })?;
        bound
            .verify_selected_context(execution_operation, &selected)
            .await?;
        bound
            .verify_canonical_source_with_io(
                &envelope(execution_operation, complete.clone()),
                &complete_source,
                limits, None,
            )
            .await?;
        assert!(matches!(
            new_journal()?
                .with_turn(execution_operation, omitted_operation)
                .verify_selected_context(execution_operation, &selected)
                .await,
            Err(Error::Conflict(_))
        ));
        provider.forbid_writes.store(true, Ordering::SeqCst);
        assert!(
            matches!(journal.verify_canonical_source_with_io(&envelope(omitted_operation, omitted), &omitted_source, limits, None).await,
            Err(Error::Conflict(_)))
        );
        journal
            .verify_canonical_source_with_io(
                &envelope(complete_operation, complete.clone()),
                &complete_source,
                limits, None,
            )
            .await?;
        let mut altered = complete_source.clone();
        altered
            .messages
            .last_mut()
            .ok_or_else(|| Error::NotFound("cold source input".into()))?
            .content = crate::model::ModelContent::Text("altered source".into());
        assert!(
            matches!(journal.verify_canonical_source_with_io(&envelope(complete_operation, complete.clone()), &altered, limits, None).await,
            Err(Error::Conflict(_)))
        );
        let bounded = new_journal()?;
        assert!(matches!(
            bounded
                .verify_canonical_source_with_io(
                    &envelope(complete_operation, complete.clone()),
                    &complete_source,
                    limits, None
                )
                .await,
            Err(Error::Invalid(_))
        ));
        let byte_bounded =
            new_journal()?.with_history_read_limits(crate::store::HistoryReadLimits {
                maximum_events: 4,
                maximum_bytes: 1,
            })?;
        assert!(matches!(
            byte_bounded
                .verify_canonical_source_with_io(
                    &envelope(complete_operation, complete),
                    &complete_source,
                    limits, None
                )
                .await,
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

}
