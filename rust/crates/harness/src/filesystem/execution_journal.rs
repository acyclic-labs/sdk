//! Stream-backed execution observations with private Filesystem payloads.
//! Version 2 observations bind the execution namespace to its original turn.
//! Unversioned records are not replayable after this envelope cutover.

use super::{FilesystemHost, FilesystemInteractionHost, InternalContentClass};
use crate::contract::capability;
use crate::contract::next_revision;
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
        ModelEventAdmission, decode_json, validate_execution_page,
    },
    interaction::{Interaction, InteractionOutcome, InteractionResolution, InteractionResponse},
    model::{ModelContent, ModelEvent},
    projection::{SelectedModelContext, select_model_context_at_revision},
    store::StreamAggregate,
    tool::ToolApprovalVerifier,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use super::fork_preparer_io::{PreparationIo, PreparationRecord, preparation_slot_bytes};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

mod conversation;

#[derive(Default)]
struct ExecutionSummary {
    operation: Option<OperationId>,
    tail: u64,
    retries: BTreeMap<[u8; 32], (u64, [u8; 32])>,
    prepared_contexts: HashSet<u32>,
    compacted_contexts: HashSet<u32>,
    // None is a completed dispatch. Its event/call bodies are no longer needed.
    models: BTreeMap<(u32, crate::executor::ModelPurpose), Option<ModelEventAdmission>>,
    tools: BTreeMap<(u32, [u8; 32]), bool>,
}

impl ExecutionSummary {
    fn require_next(&self, event: &ExecutionEvent) -> Result<()> {
        let valid = match event {
            ExecutionEvent::Started { request_digest } => {
                self.tail == 0 && *request_digest != [0; 32]
            }
            ExecutionEvent::ContextPrepared { step, .. } => {
                self.tail > 0
                    && !self.prepared_contexts.contains(step)
                    && !self
                        .models
                        .contains_key(&(*step, crate::executor::ModelPurpose::Response))
            }
            ExecutionEvent::ContextCompacted { step, .. } => {
                self.prepared_contexts.contains(step)
                    && !self.compacted_contexts.contains(step)
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
                    && !self
                        .tools
                        .contains_key(&(*step, *blake3::hash(call_id.as_bytes()).as_bytes()))
            }
            ExecutionEvent::ToolCompleted { step, call_id, .. }
            | ExecutionEvent::ToolFailed { step, call_id, .. } => {
                self.tools
                    .get(&(*step, *blake3::hash(call_id.as_bytes()).as_bytes()))
                    == Some(&false)
            }
        };
        if valid {
            Ok(())
        } else {
            Err(Error::Conflict(
                "execution observation lacks its dispatch or reuses a start".into(),
            ))
        }
    }

    fn validate_next(
        &self,
        event: &ExecutionEvent,
        model: Option<&ModelEvent>,
        limits: Limits,
    ) -> Result<()> {
        self.require_next(event)?;
        if let ExecutionEvent::Model { step, purpose, .. } = event {
            self.models
                .get(&(*step, *purpose))
                .and_then(Option::as_ref)
                .ok_or_else(|| Error::Conflict("model dispatch is already settled".into()))?
                .validate_next(
                    model.ok_or_else(|| Error::Storage("model observation is missing".into()))?,
                    limits,
                )?;
        }
        Ok(())
    }

    fn accept(
        &mut self,
        record: &ExecutionRecord,
        model: Option<&ModelEvent>,
        limits: Limits,
    ) -> Result<()> {
        self.accept_with_io(record, model, limits, None)
    }

    fn accept_with_io(
        &mut self, record: &ExecutionRecord, model: Option<&ModelEvent>,
        limits: Limits, io: Option<&PreparationIo>,
    ) -> Result<()> {
        let call_bytes = match &record.event {
            ExecutionEvent::ToolStarted { call_id, .. }
            | ExecutionEvent::ToolCompleted { call_id, .. }
            | ExecutionEvent::ToolFailed { call_id, .. } => call_id.len() as u64,
            _ => 0,
        };
        let key_work = acyclic_fs::WorkCounters { bytes_hashed: record.idempotency_key.len() as u64, ..Default::default() };
        let call_work = acyclic_fs::WorkCounters { bytes_hashed: call_bytes, ..Default::default() };
        if let Some(io) = io { io.preflight(key_work)?; }
        let key = *blake3::hash(record.idempotency_key.as_bytes()).as_bytes();
        if let Some(io) = io { io.charge(key_work)?; }
        if record.sequence != self.tail + 1
            || record.idempotency_key.is_empty()
            || self.retries.contains_key(&key)
        {
            return Err(Error::Conflict(
                "execution journal sequence or retry identity is invalid".into(),
            ));
        }
        let first_call_work = match &record.event {
            ExecutionEvent::ToolStarted { call_id, .. } if self.tail == 0 || call_id.is_empty() => Default::default(),
            _ => call_work,
        };
        if let Some(io) = io { io.preflight(first_call_work)?; }
        let valid = self.validate_next(&record.event, model, limits);
        if let Some(io) = io { io.charge(first_call_work)?; }
        valid?;
        let tool_key = match &record.event {
            ExecutionEvent::ToolStarted { call_id, .. }
            | ExecutionEvent::ToolCompleted { call_id, .. }
            | ExecutionEvent::ToolFailed { call_id, .. } => {
                if let Some(io) = io { io.preflight(call_work)?; }
                let key = *blake3::hash(call_id.as_bytes()).as_bytes();
                if let Some(io) = io { io.charge(call_work)?; }
                Some(key)
            }
            _ => None,
        };
        let digest = match io {
            Some(io) => {
                let encoded_hold;
                let encoded = io.encode_json(&record.event, io.remaining()?.bytes_encoded, true)?;
                encoded_hold = io.guard_owned(encoded.len() as u64)?;
                let work = acyclic_fs::WorkCounters { bytes_hashed: encoded.len() as u64, ..Default::default() };
                io.preflight(work)?;
                let digest = *blake3::hash(&encoded).as_bytes();
                io.charge(work)?;
                drop(encoded);
                drop(encoded_hold);
                let extra = match &record.event {
                    ExecutionEvent::ContextPrepared { .. } | ExecutionEvent::ContextCompacted { .. } => std::mem::size_of::<u32>(),
                    ExecutionEvent::ModelStarted { .. } => std::mem::size_of::<((u32, crate::executor::ModelPurpose), Option<ModelEventAdmission>)>(),
                    ExecutionEvent::ToolStarted { .. } => std::mem::size_of::<((u32, [u8; 32]), bool)>(),
                    _ => 0,
                };
                io.claim((std::mem::size_of::<([u8; 32], (u64, [u8; 32]))>() + extra) as u64)?;
                digest
            }
            None => crate::contract::canonical_json_digest(&record.event)?,
        };
        match &record.event {
            ExecutionEvent::ContextPrepared { step, .. } => {
                self.prepared_contexts.insert(*step);
            }
            ExecutionEvent::ContextCompacted { step, .. } => {
                self.compacted_contexts.insert(*step);
            }
            ExecutionEvent::ModelStarted { step, purpose, .. } => {
                self.models
                    .insert((*step, *purpose), Some(ModelEventAdmission::default()));
            }
            ExecutionEvent::Model { step, purpose, .. } => {
                let state = self
                    .models
                    .get_mut(&(*step, *purpose))
                    .ok_or_else(|| Error::Storage("model dispatch is missing".into()))?;
                let admission = state
                    .as_mut()
                    .ok_or_else(|| Error::Conflict("model dispatch is already settled".into()))?;
                admission.observe_with_io(
                    model.ok_or_else(|| Error::Storage("model observation is missing".into()))?,
                    limits, io,
                )?;
                if admission.completed() {
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
    fn maximum_slot_bytes() -> u64 { preparation_slot_bytes::<Self>() }
}

impl Observation {
    fn encode(
        operation_id: OperationId,
        turn_operation: OperationId,
        claim_id: &str,
        event: &ExecutionEvent,
    ) -> Result<(StreamKey, String, Bytes)> {
        let digest = blake3::hash(format!("{operation_id}:{claim_id}").as_bytes());
        let retry_digest = digest.to_hex().to_string();
        let bytes = serde_json::to_vec(&Self {
            version: OBSERVATION_VERSION,
            operation_id,
            turn_operation,
            retry_digest: retry_digest.clone(),
            event: event.clone(),
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(Error::Invalid(
                "execution journal observation exceeds Stream limit".into(),
            ));
        }
        Ok((
            StreamKey::new(Bytes::copy_from_slice(digest.as_bytes()))?,
            retry_digest,
            Bytes::from(bytes),
        ))
    }
}

struct TaskExecutionIdentity {
    operation: OperationId,
    turn: OperationId,
}

/// Durable, ref-only journal for one exact agent-private volume.
pub struct FilesystemExecutionJournal<P, A, O> {
    stream: StreamClient<P>,
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    schemas: SchemaRegistry,
    maximum_payload_bytes: u64,
    input_verifier: Option<Arc<dyn ContentResidencyVerifier>>,
    interactions: Option<FilesystemInteractionHost<P, A, O>>,
    owner: Option<(TaskJournalOwner<P>, OperationId)>,
    execution: Option<TaskExecutionIdentity>,
    verified: tokio::sync::Mutex<ExecutionSummary>,
    conversation_projection: tokio::sync::Mutex<Option<StreamAggregate<P>>>,
    history_read_limits: Option<crate::store::HistoryReadLimits>,
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O> {
    /// Binds one execution to the task's exact lease and coordinator provider.
    /// The original turn identifies committed context independently of this
    /// task's execution namespace.
    /// Fresh model starts require their matching retained shared-budget claim.
    #[expect(
        clippy::too_many_arguments,
        reason = "task, execution, original turn and storage bindings are explicit"
    )]
    pub fn for_task(
        owner: TaskJournalOwner<P>,
        operation_id: OperationId,
        turn_operation: OperationId,
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        scope: Scope,
        maximum_payload_bytes: u64,
    ) -> Result<Self>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        owner.validate_storage(&verifier, maximum_payload_bytes)?;
        let mut journal = Self::new(
            owner.stream(),
            host,
            volume,
            verifier,
            scope,
            maximum_payload_bytes,
        )?;
        journal.owner = Some((owner, operation_id));
        journal.execution = Some(TaskExecutionIdentity {
            operation: operation_id,
            turn: turn_operation,
        });
        Ok(journal)
    }

    /// Binds passive replay to an original turn without granting a task lease.
    pub(super) fn with_turn(mut self, operation: OperationId, turn: OperationId) -> Self {
        self.execution = Some(TaskExecutionIdentity { operation, turn });
        self
    }

    fn turn_operation(&self, operation: OperationId) -> OperationId {
        self.execution
            .as_ref()
            .map_or(operation, |identity| identity.turn)
    }

    fn require_operation(&self, operation: OperationId) -> Result<()> {
        if self
            .execution
            .as_ref()
            .is_some_and(|bound| bound.operation != operation)
        {
            return Err(Error::Unauthorized(
                "journal belongs to another task execution".into(),
            ));
        }
        Ok(())
    }

    async fn refresh_summary(
        &self,
        operation: OperationId,
        summary: &mut ExecutionSummary,
    ) -> Result<()>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        self.refresh_summary_with_io(operation, summary, None).await
    }

    async fn refresh_summary_with_io(
        &self, operation: OperationId, summary: &mut ExecutionSummary,
        io: Option<&PreparationIo>,
    ) -> Result<()>
    where P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
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
            let page = Box::pin(self.replay_verified_with_io(
                operation, summary.tail, EXECUTION_REPLAY_PAGE_RECORDS, io)).await?;
            page_hold = match (io, before) {
                (Some(io), Some(before)) => Some(io.guard_owned(io.owned_bytes()?.checked_sub(before)
                    .ok_or_else(|| Error::Invalid("execution page custody underflow".into()))?)?),
                _ => None,
            };
            if page.is_empty() {
                return Ok(());
            }
            for (record, model) in page {
                summary.accept_with_io(&record, model.as_ref(), limits, io)?;
            }
            drop(page_hold);
        }
    }

    /// Observes the same validated dispatch summary used by passive suspension.
    /// This grants no dispatch or release authority, including after cancellation.
    pub(crate) async fn is_quiescent(&self) -> Result<bool>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        let (owner, operation) = self.owner.as_ref().ok_or_else(|| {
            Error::Unsupported("quiescence requires a task-owned execution journal".into())
        })?;
        owner.verify(true).await?;
        let quiescent = {
            let mut summary = self.verified.lock().await;
            Box::pin(self.refresh_summary(*operation, &mut summary)).await?;
            summary.quiescent(None)?
        };
        owner.verify(true).await?;
        Ok(quiescent)
    }

    /// Releases only after both configured original journals prove passive
    /// workflow state and settled dispatches. Their tails and the current owner
    /// fence are compared atomically by the canonical coordinator release.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn release_if_quiescent_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
        workflow: &super::FilesystemWorkflowJournal<P, A, O>,
    ) -> Result<bool>
    where P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
    {
        let io = PreparationIo::from_native_work(work)?;
        let (owner, operation) = self.owner.as_ref().ok_or_else(|| Error::Unsupported(
            "quiescence requires a task-owned execution journal".into()))?;
        workflow.require_execution_owner(owner, &self.volume, self.verifier.audience())?;
        let result: Result<_> = async {
            let mut borrowed = io.history_io();
            owner.verify_with_work(true, &mut Some(&mut borrowed)).await?;
            let tail = {
                let before = io.owned_bytes()?;
                let summary_hold;
                let mut summary = ExecutionSummary::default();
                self.refresh_summary_with_io(*operation, &mut summary, Some(&io)).await?;
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

    /// Releases a passive workflow command only after this task-bound journal
    /// proves every started model and tool dispatch settled. Publication compares
    /// the same journal tail and current task lease atomically.
    pub async fn suspend_workflow_command_if_quiescent(
        &self,
        revision: u64,
        command: OperationId,
    ) -> Result<()>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        let span = obs_span!("acyclic.harness.journal.suspend", rev = revision);
        traced(span, self.suspend_untraced(revision, command)).await
    }

    async fn suspend_untraced(&self, revision: u64, command: OperationId) -> Result<()>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        let (owner, operation) = self.owner.as_ref().ok_or_else(|| {
            Error::Unsupported("quiescence requires a task-owned execution journal".into())
        })?;
        let mut summary = self.verified.lock().await;
        Box::pin(self.refresh_summary(*operation, &mut summary)).await?;
        if !summary.quiescent(None)? {
            return Err(Error::Indeterminate(*operation));
        }
        owner
            .suspend_quiescent_execution(*operation, summary.tail, revision, command)
            .await
    }

    async fn append_validated(
        &self,
        operation_id: OperationId,
        expected_tail: Option<u64>,
        claim_id: &str,
        event: ExecutionEvent,
    ) -> Result<bool>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        self.require_operation(operation_id)?;
        if claim_id.is_empty() {
            return Err(Error::Invalid(
                "execution journal compare-and-append is invalid".into(),
            ));
        }
        let owner = self.owner.as_ref().map(|(owner, _)| owner);
        let model = self.verify_event_refs(&event).await?;
        let limits = owner.map_or_else(Limits::default, TaskJournalOwner::input_limits);
        self.verify_canonical_publication(operation_id, &event, limits)
            .await?;
        let (key, retry_digest, bytes) = Observation::encode(
            operation_id,
            self.turn_operation(operation_id),
            claim_id,
            &event,
        )?;
        let retry_key = *blake3::hash(retry_digest.as_bytes()).as_bytes();
        let event_digest = crate::contract::canonical_json_digest(&event)?;
        let mut summary = self.verified.lock().await;
        // Rebuild once on cold open, then read only the newly committed suffix.
        // No payload bodies are retained in this rebuildable index.
        loop {
            Box::pin(self.refresh_summary(operation_id, &mut summary)).await?;
            if let Some(owner) = owner {
                owner.verify_execution(operation_id, &event).await?;
            }
            if let Some((sequence, previous)) = summary.retries.get(&retry_key) {
                if *previous != event_digest {
                    return Err(Error::Conflict(
                        "execution journal retry identity reused".into(),
                    ));
                }
                return Ok(expected_tail.is_none_or(|tail| tail + 1 == *sequence));
            }
            if expected_tail.is_some_and(|tail| tail != summary.tail) {
                return Ok(false);
            }
            summary.validate_next(&event, model.as_ref(), limits)?;
            let committed = if let Some(owner) = owner {
                owner
                    .append_execution(operation_id, summary.tail, &key, bytes.clone(), &event)
                    .await?
            } else {
                self.append_local(
                    operation_id,
                    summary.tail,
                    key.clone(),
                    bytes.clone(),
                    &retry_digest,
                    &event,
                )
                .await?
            };
            if committed {
                let sequence = summary.tail + 1;
                summary.accept(
                    &ExecutionRecord {
                        operation_id,
                        sequence,
                        idempotency_key: retry_digest,
                        event,
                    },
                    model.as_ref(),
                    limits,
                )?;
                return Ok(true);
            }
        }
    }

    async fn verify_canonical_publication(
        &self,
        operation_id: OperationId,
        event: &ExecutionEvent,
        limits: Limits,
    ) -> Result<()>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        let ExecutionEvent::ContextCompacted {
            step,
            projection,
            compaction,
            ..
        } = event
        else {
            return Ok(());
        };
        limits.validate_file(projection)?;
        let projection =
            crate::executor::load_json::<crate::executor::ResponseProjection>(self, projection)
                .await?;
        crate::context::validate_projected_context(&projection.context, limits)?;
        let Some(reference) = &projection.checkpoint else {
            if projection.canonical.is_some() {
                return Err(Error::Invalid(
                    "canonical publication has no checkpoint".into(),
                ));
            }
            return Ok(());
        };
        limits.validate_file(reference)?;
        let envelope = crate::executor::load_json::<crate::context::CanonicalContextCheckpoint>(
            self, reference,
        )
        .await?;
        envelope.validate(limits)?;
        if *step != 0
            || envelope.operation_id != operation_id
            || projection.canonical.as_ref() != Some(&envelope.source)
            || &envelope.compaction != compaction
        {
            return Err(Error::Invalid(
                "canonical publication differs from its envelope".into(),
            ));
        }
        let source =
            crate::executor::load_json::<crate::context::Context>(self, &envelope.source).await?;
        let retained =
            crate::executor::load_json::<crate::context::Context>(self, &envelope.retained).await?;
        let proof =
            crate::executor::load_json::<crate::context::CompactionReference>(self, compaction)
                .await?;
        envelope.validate_projection(&source, &retained, &proof, limits)?;
        self.verify_canonical_source(&envelope, &source, limits)
            .await?;
        self.verify_canonical_summary(operation_id, &envelope, &source, &proof, limits)
            .await
    }

    pub(super) fn context_history_limits(
        &self,
        limits: Limits,
    ) -> Result<crate::store::HistoryReadLimits> {
        limits.validate()?;
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

    async fn verify_canonical_source(
        &self,
        envelope: &crate::context::CanonicalContextCheckpoint,
        source: &crate::context::Context,
        limits: Limits,
    ) -> Result<()>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        self.require_operation(envelope.operation_id)?;
        if self.execution.is_some() {
            self.replay_verified(envelope.operation_id, 0, 1).await?;
        }
        let previous = match &envelope.selection.checkpoint {
            Some(reference) => {
                Some(crate::executor::load_canonical_checkpoint(self, reference, limits).await?)
            }
            None => None,
        };
        let after = previous
            .as_ref()
            .map_or(0, |(previous, _)| previous.selection.conversation_revision);
        let verifier = self
            .input_verifier
            .as_ref()
            .ok_or_else(|| Error::Unsupported("canonical source reader is not bound".into()))?;
        limits.validate()?;
        let reader = crate::store::HistoryReader::new(
            &self.stream,
            self.verifier.audience(),
            self.verifier.clone(),
        )?;
        let event = reader
            .operation_event(self.turn_operation(envelope.operation_id))
            .await?
            .ok_or_else(|| {
                Error::Conflict("checkpoint source selection is not the committed admission".into())
            })?;
        let crate::core::EventPayload::ModelContextSelected { selection } = event.payload else {
            return Err(Error::Conflict(
                "checkpoint operation is not a model-context selection".into(),
            ));
        };
        if selection != envelope.selection {
            return Err(Error::Conflict(
                "checkpoint source selection is not the committed admission".into(),
            ));
        }
        let cursor = crate::store::HistoryCursor {
            authority: self.verifier.audience().clone(),
            after_revision: 0,
            through_revision: event.revision,
        };
        let history_limits = self.context_history_limits(limits)?;
        let loaded = reader
            .conversation_range(
                &cursor,
                after,
                envelope.selection.conversation_revision,
                history_limits,
            )
            .await?;
        let delta =
            project_canonical_delta(loaded, &envelope.selection, verifier.as_ref(), limits).await?;
        let expected = match previous {
            Some((_, retained)) => {
                crate::context::ContextPipeline::continue_base(retained, delta, limits)?
            }
            None => delta,
        };
        if source != &expected {
            return Err(Error::Conflict(
                "checkpoint source differs from its canonical base and delta".into(),
            ));
        }
        Ok(())
    }

    async fn verify_canonical_summary(
        &self,
        operation: OperationId,
        envelope: &crate::context::CanonicalContextCheckpoint,
        source: &crate::context::Context,
        proof: &crate::context::CompactionReference,
        limits: Limits,
    ) -> Result<()>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        use crate::executor::ModelPurpose;
        let summary = proof
            .summary
            .as_ref()
            .filter(|summary| summary.step == 0)
            .ok_or_else(|| Error::Invalid("canonical summary belongs to another step".into()))?;
        let (_, records) = crate::executor::replay_execution(
            self,
            operation,
            limits.model_events_per_step.saturating_add(2),
            |event| {
                matches!(
                    event,
                    ExecutionEvent::ContextPrepared { step: 0, .. }
                        | ExecutionEvent::ModelStarted {
                            step: 0,
                            purpose: ModelPurpose::Summary,
                            ..
                        }
                        | ExecutionEvent::Model {
                            step: 0,
                            purpose: ModelPurpose::Summary,
                            ..
                        }
                )
            },
        )
        .await?;
        let mut prepared = None;
        let mut request = None;
        let mut output = String::new();
        let mut completed = false;
        for record in records {
            match record.event {
                ExecutionEvent::ContextPrepared { projection, .. } => {
                    let projection = crate::executor::load_json::<
                        crate::executor::ResponseProjection,
                    >(self, &projection)
                    .await?;
                    prepared = projection.canonical;
                }
                ExecutionEvent::ModelStarted {
                    request: reference, ..
                } => {
                    request = Some(crate::model::PreparedModelRequest::decode(
                        &self.load(&reference).await?,
                        limits,
                    )?);
                }
                ExecutionEvent::Model { event, .. } => {
                    let event = crate::executor::load_json::<ModelEvent>(self, &event).await?;
                    match event {
                        ModelEvent::Content { delta } => {
                            if (output.len() as u64)
                                .checked_add(delta.len() as u64)
                                .is_none_or(|length| length > limits.render_bytes)
                            {
                                return Err(Error::Invalid(
                                    "canonical summary output exceeds bounds".into(),
                                ));
                            }
                            output.push_str(&delta);
                        }
                        ModelEvent::Completed { .. } => completed = true,
                        ModelEvent::Reasoning { .. } => {}
                        ModelEvent::ToolCall { .. } => {
                            return Err(Error::Invalid(
                                "canonical summary requested a tool".into(),
                            ));
                        }
                    }
                }
                _ => {
                    return Err(Error::Storage(
                        "canonical summary selection is invalid".into(),
                    ));
                }
            }
        }
        if prepared.as_ref() != Some(&envelope.source) || !completed || output.is_empty() {
            return Err(Error::Conflict(
                "canonical summary lacks its prepared source or settled output".into(),
            ));
        }
        let request = request
            .ok_or_else(|| Error::Conflict("canonical summary has no admitted request".into()))?;
        verify_summary_prefix(&request, source, summary.source_messages)?;
        limits.validate_file(&summary.output)?;
        if self.load(&summary.output).await? != output.as_bytes() {
            return Err(Error::Conflict(
                "canonical summary output differs from admitted observations".into(),
            ));
        }
        Ok(())
    }

    async fn append_local(
        &self,
        operation_id: OperationId,
        tail: u64,
        key: StreamKey,
        bytes: Bytes,
        retry_digest: &str,
        event: &ExecutionEvent,
    ) -> Result<bool>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        match self
            .path(operation_id)?
            .append_batch(vec![bytes], Some(tail), Some(key))
            .await
        {
            Ok(AppendOutcome::Committed(receipt))
                if receipt.start == tail && receipt.end == tail + 1 =>
            {
                Ok(true)
            }
            Ok(AppendOutcome::Committed(_)) => Err(Error::Storage(
                "execution journal append receipt is invalid".into(),
            )),
            Ok(AppendOutcome::TailConflict { .. }) => Ok(false),
            Err(StreamError::IdempotencyMismatch) => Err(Error::Conflict(
                "execution journal retry identity reused".into(),
            )),
            Err(_) => {
                let records = self
                    .replay(operation_id, tail, 1)
                    .await
                    .map_err(|_| Error::Indeterminate(operation_id))?;
                if records
                    .iter()
                    .any(|record| record.idempotency_key == retry_digest && &record.event == event)
                {
                    Ok(true)
                } else {
                    Err(Error::Indeterminate(operation_id))
                }
            }
        }
    }

    /// Binds an exact private volume and authenticated read/write grant.
    pub fn new(
        stream: StreamClient<P>,
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        scope: Scope,
        maximum_payload_bytes: u64,
    ) -> Result<Self>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        Self::new_with_schemas(
            stream,
            host,
            volume,
            verifier,
            SchemaRegistry::new(),
            scope,
            maximum_payload_bytes,
        )
    }

    /// Binds the exact pinned extension registry used by the conversation reducer.
    pub fn new_with_schemas(
        stream: StreamClient<P>,
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        scope: Scope,
        maximum_payload_bytes: u64,
    ) -> Result<Self>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        if volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider
            || maximum_payload_bytes == 0
        {
            return Err(Error::Invalid(
                "execution journal requires a private volume and positive limit".into(),
            ));
        }
        ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Read)?;
        ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Write)?;
        let interactions = match verifier.audience().kind {
            crate::core::AggregateKind::Conversation => Some(FilesystemInteractionHost::new(
                stream.clone(),
                Arc::clone(&host),
                verifier.audience().clone(),
                verifier.clone(),
                schemas.clone(),
                scope.clone(),
                volume.clone(),
                maximum_payload_bytes,
            )?),
            crate::core::AggregateKind::Task => None,
            _ => {
                return Err(Error::Invalid(
                    "execution journal requires a task or conversation authority".into(),
                ));
            }
        };
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
        })
    }

    /// Binds approval/question tickets to an existing conversation owner on the
    /// same Stream and Filesystem providers. This does not grant response rights.
    /// Task-bound request publication also requires retained interaction:route
    /// and atomically compares the current task lease with the conversation tail.
    pub fn with_interaction_owner(
        mut self,
        verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        scope: Scope,
        private_volume: VolumeRef,
    ) -> Result<Self>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        if let Some((owner, _)) = &self.owner {
            owner.require_interaction_grant()?;
        } else if self.verifier.audience().kind == crate::core::AggregateKind::Task {
            return Err(Error::Unauthorized(
                "task interaction binding requires an admitted journal owner".into(),
            ));
        }
        self.interactions = Some(FilesystemInteractionHost::new(
            self.stream.clone(),
            self.host.clone(),
            verifier.audience().clone(),
            verifier,
            schemas,
            scope,
            private_volume,
            self.maximum_payload_bytes,
        )?);
        Ok(self)
    }

    /// Sets a separate finite work/byte allowance for cold canonical deltas.
    /// All canonical message kinds count toward this work limit. Without an
    /// override, the admitted context-message limit also bounds delta reads.
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
    where
        P: StreamProvider,
    {
        self.stream
            .stream(crate::distributed::execution_path(operation_id)?.as_str())
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    /// Resolves through the conversation-owned ledger with an exact responder grant.
    pub async fn resolve_interaction(
        &self,
        id: InteractionId,
        response: InteractionResponse,
        responder: &Scope,
    ) -> Result<()>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        let span = obs_span!(INFO, "acyclic.harness.journal.resolve_interaction");
        traced(
            span,
            self.resolve_interaction_untraced(id, response, responder),
        )
        .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one ordered interaction resolution transaction"
    )]
    async fn resolve_interaction_untraced(
        &self,
        id: InteractionId,
        response: InteractionResponse,
        responder: &Scope,
    ) -> Result<()>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        // Reply scopes belong to the actual conversation owner, not the
        // execution's Task aggregate verifier. Keep its exact ticket grant.
        self.interactions()?.verify_responder_scope(id, responder)?;
        if let Some((owner, _)) = &self.owner {
            owner.require_interaction_grant()?;
            owner.verify(true).await?;
        }
        let Some((ticket, prior)) = self.interactions()?.read(id).await? else {
            return Err(Error::NotFound(format!("interaction {id}")));
        };
        let request = self
            .interactions()?
            .read_request(id)
            .await?
            .ok_or_else(|| Error::Storage("admitted interaction has no request".into()))?;
        request.validate_response(&response)?;
        if let Some(previous) = &prior
            && previous.outcome.is_terminal()
        {
            let same = match (&previous.outcome, &response) {
                (
                    InteractionOutcome::Approved,
                    InteractionResponse::Approval { approved: true, .. },
                )
                | (
                    InteractionOutcome::Declined,
                    InteractionResponse::Approval {
                        approved: false, ..
                    },
                ) => {
                    let bytes = self
                        .interactions()?
                        .read_decision_detail(id)
                        .await?
                        .ok_or_else(|| {
                            Error::Storage("admitted approval detail is missing".into())
                        })?;
                    let original: InteractionResponse = crate::contract::json_from_slice(&bytes)
                        .map_err(|error| Error::Storage(error.to_string()))?;
                    original == response
                }
                (InteractionOutcome::Answered { .. }, _) => {
                    let bytes = self
                        .interactions()?
                        .read_answer(id)
                        .await?
                        .ok_or_else(|| Error::Storage("admitted answer is missing".into()))?;
                    let original: InteractionResponse = crate::contract::json_from_slice(&bytes)
                        .map_err(|error| Error::Storage(error.to_string()))?;
                    original == response
                }
                _ => false,
            };
            return if same {
                Ok(())
            } else {
                Err(Error::Conflict("interaction was already resolved".into()))
            };
        }
        let expected_version = prior.map_or(Ok(1_u64), |value| {
            value
                .expected_version
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("interaction version exhausted".into()))
        })?;
        let resolution_operation =
            interaction_operation(id, &format!("resolve-{expected_version}"));
        if let Some((owner, _)) = &self.owner { owner.verify(true).await?; }
        let detail = if matches!(&response, InteractionResponse::Approval { .. }) {
            Some(
                self.interactions()?
                    .stage_answer(id, expected_version, resolution_operation, &response)
                    .await?,
            )
        } else {
            None
        };
        let outcome = match response {
            InteractionResponse::Approval { approved: true, .. } => InteractionOutcome::Approved,
            InteractionResponse::Approval {
                approved: false, ..
            } => InteractionOutcome::Declined,
            other => InteractionOutcome::Answered {
                answer: Box::new(
                    self.interactions()?
                        .stage_answer(id, expected_version, resolution_operation, &other)
                        .await?,
                ),
            },
        };
        let result = self.interactions()?
            .resolve(
                resolution_operation,
                responder.clone(),
                InteractionResolution {
                    id: ticket.id,
                    expected_version,
                    outcome,
                    detail,
                },
            )
            .await
            .map(|_| ());
        result?;
        if let Some((owner, _)) = &self.owner { owner.verify(true).await?; }
        Ok(())
    }

    /// Shares the existing authoritative reducer between turn admission and
    /// selected-context verification; refresh never rebuilds its retained prefix.
    pub(super) async fn conversation_projection(
        &self,
        limits: Limits,
    ) -> Result<tokio::sync::MappedMutexGuard<'_, StreamAggregate<P>>>
    where
        P: StreamProvider,
    {
        limits.validate()?;
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
                    self.schemas.clone(),
                )
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
                self.schemas.clone(),
            )
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

    async fn replay_verified(
        &self,
        operation_id: OperationId,
        after: u64,
        maximum: u32,
    ) -> Result<Vec<(ExecutionRecord, Option<ModelEvent>)>>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        self.replay_verified_with_io(operation_id, after, maximum, None).await
    }

    async fn replay_verified_with_io(
        &self, operation_id: OperationId, after: u64, maximum: u32, io: Option<&PreparationIo>,
    ) -> Result<Vec<(ExecutionRecord, Option<ModelEvent>)>>
    where P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
    {
        self.require_operation(operation_id)?;
        validate_execution_page(after, maximum)?;
        let capacity = maximum.min(EXECUTION_REPLAY_PAGE_RECORDS) as usize;
        if let Some(io) = io {
            let bytes = capacity.checked_mul(std::mem::size_of::<(ExecutionRecord, Option<ModelEvent>)>())
                .ok_or_else(|| Error::Invalid("execution replay vector ownership overflow".into()))? as u64;
            io.claim(bytes)?;
            io.allocation_attempt(bytes)?;
        }
        let mut result = Vec::with_capacity(capacity);
        let mut keys = HashSet::new();
        if let Some(io) = io {
            // Two explicit owned path handles, not provider-internal memory.
            let path_bytes = ("harness/v2/execution/".len() + 36) as u64;
            io.claim(path_bytes.checked_mul(2).ok_or_else(|| Error::Invalid("execution path ownership overflow".into()))?)?;
            let call = acyclic_fs::WorkCounters { backend_read_operations: 1, ..Default::default() };
            io.preflight(call)?;
            io.charge(call)?;
        }
        let mut entries = match self
            .path(operation_id)?
            .read(after, maximum.min(EXECUTION_REPLAY_PAGE_RECORDS))
            .await
        {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => return Ok(result),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        loop {
            let record_hold;
            let Some(record) = entries.try_next().await? else { break; };
            if let Some(io) = io {
                let read = acyclic_fs::WorkCounters {
                    authority_records_read: 1, authority_bytes_read: record.value.len() as u64,
                    source_bytes_read: record.value.len() as u64, items_examined: 1, ..Default::default()
                };
                io.preflight(read)?;
                io.charge(read)?;
                io.claim(record.value.len() as u64)?;
            }
            record_hold = io.map(|io| io.guard_owned(record.value.len() as u64)).transpose()?;
            if result.len() >= maximum as usize || record.sequence != after + result.len() as u64 {
                return Err(Error::Storage(
                    "execution journal sequence or bound is invalid".into(),
                ));
            }
            let observation: Observation = match io {
                Some(io) => io.decode_preparation::<Observation>(&record.value)?.into_retained(),
                None => crate::contract::json_from_slice(&record.value)
                    .map_err(|error| Error::Storage(format!("execution journal record is invalid: {error}")))?,
            };
            if observation.version != OBSERVATION_VERSION {
                return Err(Error::Storage(
                    "execution journal observation version is unsupported".into(),
                ));
            }
            if let Some(io) = io {
                io.claim(observation.retry_digest.len() as u64)?;
                io.claim(std::mem::size_of::<String>() as u64)?;
            }
            if observation.operation_id != operation_id
                || !keys.insert(observation.retry_digest.clone())
            {
                return Err(Error::Storage(
                    "execution journal identity is invalid".into(),
                ));
            }
            if observation.turn_operation != self.turn_operation(operation_id) {
                return Err(Error::Conflict(
                    "execution journal belongs to another original turn".into(),
                ));
            }
            let model = self.verify_event_refs_with_io(&observation.event, io).await?;
            result.push((
                ExecutionRecord {
                    operation_id,
                    sequence: next_revision(record.sequence)?,
                    idempotency_key: observation.retry_digest,
                    event: observation.event,
                },
                model,
            ));
            drop(record);
            drop(record_hold);
        }
        if let Some((owner, _)) = &self.owner {
            if let Some(io) = io {
                let mut work = io.history_io();
                owner.verify_model_history_with_work(
                    operation_id, result.iter().map(|(record, _)| record), &mut Some(&mut work),
                ).await?;
            } else {
                owner.verify_model_history_with_work(
                    operation_id, result.iter().map(|(record, _)| record), &mut None).await?;
            }
        }
        Ok(result)
    }

    async fn verify_event_refs(&self, event: &ExecutionEvent) -> Result<Option<ModelEvent>>
    where P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
    {
        self.verify_event_refs_with_io(event, None).await
    }

    async fn verify_event_refs_with_io(&self, event: &ExecutionEvent, io: Option<&PreparationIo>) -> Result<Option<ModelEvent>>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
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
                return Err(Error::Unauthorized(
                    "journal event refers to another private volume".into(),
                ));
            }
            let _bytes_hold;
            let bytes = if let Some(io) = io {
                super::fork_preparer_io::item(Some(io), 0)?;
                ContentGrant::verify_original_volume_with_io(
                    &self.verifier, &self.scope, &self.volume, VolumeOperation::Read, io)?;
                let bytes = self.host.read_pinned_owned_io(reference, self.maximum_payload_bytes, io).await?;
                _bytes_hold = Some(io.guard_owned(bytes.capacity() as u64)?);
                bytes
            } else {
                _bytes_hold = None;
                let grant = ContentGrant::verify(&self.verifier, &self.scope, &self.volume, VolumeOperation::Read)?;
                self.host.read_content(reference, &grant, self.maximum_payload_bytes).await?
            };
            if let ExecutionEvent::ModelStarted { request_digest, .. } = event {
                let limits = self
                    .owner
                    .as_ref()
                    .map_or_else(Limits::default, |(owner, _)| owner.input_limits());
                let prepared = match io {
                    Some(io) => crate::model::PreparedModelRequest::decode_with_io(&bytes, limits, io)?,
                    None => crate::model::PreparedModelRequest::decode(&bytes, limits)?,
                };
                if &prepared.manifest().request_digest != request_digest {
                    return Err(Error::Invalid(
                        "model start digest differs from its retained request".into(),
                    ));
                }
            }
            if matches!(event, ExecutionEvent::Model { .. }) {
                if reference.descriptor().media_type() != "application/json" {
                    return Err(Error::Storage("model observation is not JSON".into()));
                }
                model = Some(match io {
                    Some(io) => {
                        let parsed = io.decode_preparation::<ModelEvent>(&bytes)?;
                        if io.encode_json(&parsed.value, self.maximum_payload_bytes, true)? != bytes {
                            return Err(Error::Storage("execution journal JSON is not canonical".into()));
                        }
                        parsed.into_retained()
                    }
                    None => decode_json(&bytes)?,
                });
            }
        }
        Ok(model)
    }

    async fn verify_input_file_with_io(&self, reference: &FileRef, io: Option<&PreparationIo>) -> Result<()>
    where P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
    {
        if reference.descriptor().byte_length() > self.maximum_payload_bytes {
            return Err(Error::Invalid("turn input file exceeds journal limit".into()));
        }
        if let Some((owner, _)) = &self.owner
            && owner.verify_created_input_with_io(reference, io).await?
        {
            return Ok(());
        }
        if reference.volume().provider() != &self.host.provider {
            let verifier = self.input_verifier.as_ref().ok_or_else(|| Error::Unsupported(
                "turn input file belongs to another content provider".into()))?;
            return match io {
                Some(io) => {
                    let mut work = io.history_io();
                    crate::store::history::HistoryIo::verify_content(&mut work, verifier.as_ref(), reference).await
                }
                None => verifier.verify(reference).await,
            };
        }
        if let Some(io) = io {
            super::fork_preparer_io::item(Some(io), reference.logical_owned_bytes()?)?;
            reference.validate()?;
            if reference.volume().class() == VolumeClass::AgentPrivate
                && reference.volume().owner() != self.volume.owner()
            {
                if crate::conversation::is_internal_path(reference.path())
                    && !crate::conversation::is_inherited_context_path(reference.path())
                {
                    return Err(Error::Unauthorized("internal storage file is not public content".into()));
                }
                let mut work = io.history_io();
                self.verifier.verify_with_work(&self.scope, &mut Some(&mut work))?;
                if !io.contains_capability(self.scope.capabilities(), reference, "file:read:")? {
                    return Err(Error::Unauthorized("exact file read is not granted".into()));
                }
            } else {
                ContentGrant::verify_original_volume_with_io(
                    &self.verifier, &self.scope, reference.volume(), VolumeOperation::Read, io)?;
            }
            self.host.read_pinned_io(reference, self.maximum_payload_bytes, Some(io)).await?;
        } else {
            let grant = if reference.volume().class() == VolumeClass::AgentPrivate
                && reference.volume().owner() != self.volume.owner()
            {
                ContentGrant::verify_file_read(&self.verifier, &self.scope, reference)?
            } else {
                ContentGrant::verify(&self.verifier, &self.scope, reference.volume(), VolumeOperation::Read)?
            };
            self.host.read_content(reference, &grant, self.maximum_payload_bytes).await?;
        }
        Ok(())
    }

    async fn verify_model_content_with_io(&self, content: &ModelContent, io: Option<&PreparationIo>) -> Result<()>
    where P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
    {
        let mut count = 0_usize;
        content.visit_file_bindings(|_, _| {
            super::fork_preparer_io::item(io, 0)?;
            count = count.checked_add(1).ok_or_else(|| Error::Invalid("model reference count overflow".into()))?;
            Ok(())
        })?;
        let references_hold;
        let bytes = (count as u64).checked_mul(std::mem::size_of::<(&FileRef, Option<&crate::model::NativeConfigurationBinding>)>() as u64)
            .ok_or_else(|| Error::Invalid("model reference ownership overflow".into()))?;
        if let Some(io) = io {
            io.claim(bytes)?;
            references_hold = Some(io.guard_owned(bytes)?);
            if count != 0 { io.allocation_attempt(bytes)?; }
        } else {
            references_hold = None;
        }
        let mut references = Vec::with_capacity(count);
        content.visit_file_bindings(|file, binding| {
            if let Some((owner, _)) = &self.owner {
                match io {
                    Some(io) => owner.validate_input_file_with_io(file, io)?,
                    None => owner.validate_input_file(file)?,
                }
            }
            if file.descriptor().byte_length() > self.maximum_payload_bytes {
                return Err(Error::Invalid("model content exceeds journal limit".into()));
            }
            references.push((file, binding));
            Ok(())
        })?;
        // Authenticate every retained claim before reading any media or options.
        for (_, binding) in &references {
            if let Some(binding) = binding {
                let (owner, _) = self.owner.as_ref().ok_or_else(|| Error::Unsupported(
                    "native options require an original task owner".into()))?;
                match io {
                    Some(io) => { owner.validate_native_configuration_with_io(binding, io)?; }
                    None => { owner.validate_native_configuration(binding)?; }
                }
            }
        }
        for (_, binding) in &references {
            if let Some(binding) = binding {
                let (owner, _) = self.owner.as_ref().ok_or_else(|| Error::Unsupported(
                    "native options require an original task owner".into()))?;
                match io {
                    Some(io) => owner.verify_native_configuration_with_io(binding, io).await?,
                    None => owner.verify_native_configuration(binding).await?,
                }
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
        Box::pin(async move {
            self.interactions()?.verify_approval(id, operation, digest, None).await
        })
    }

    fn verify_measured<'a>(
        &'a self, id: InteractionId, operation: OperationId, digest: [u8; 32],
        budget: acyclic_fs::WorkBudget, cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let io = super::fork_preparer_io::PreparationIo::from_budget(budget, cancellation)
                .map_err(|error| acyclic_fs::OperationFailure::new(error, Default::default()))?;
            let result = async {
                if let Some((owner, _)) = &self.owner {
                    io.receive_content(owner.require_interaction_grant_measured(io.remaining()?, cancellation))?;
                }
                self.interactions()?.verify_approval(id, operation, digest, Some(&io)).await?;
                io.remaining()?;
                Ok(())
            }.await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
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
            let page = self.replay_verified(operation_id, after, maximum).await?;
            crate::obs::obs_record!("items" = page.len());
            Ok(page.into_iter().map(|(record, _)| record).collect())
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
                let page = self.replay_verified_with_io(operation_id, after, maximum, Some(&io)).await?;
                // The consuming map transfers the admitted record custody;
                // std's collection internals are not allocator statistics.
                let records = page.into_iter().map(|(record, _)| record).collect();
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
            self.append_validated(operation_id, None, &idempotency_key, event)
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
            self.append_validated(operation_id, Some(expected_tail), &claim_id, event)
                .await
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
            self.require_operation(operation_id)?;
            if bytes.len() as u64 > self.maximum_payload_bytes {
                return Err(Error::Invalid(
                    "execution journal payload exceeds limit".into(),
                ));
            }
            let digest = blake3::hash(format!("{operation_id}:{idempotency_key}").as_bytes());
            let key = IdempotencyKey::new(format!("journal-{}", digest.to_hex()))?;
            let grant = ContentGrant::verify(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Write,
            )?;
            self.host
                .put_internal_content(
                    &self.volume,
                    &grant,
                    // The returned reference pins the generation, not the
                    // current head. Reuse one active slot without changing any
                    // historical payload or operation's retry identity.
                    ".system/execution/payload.json",
                    &bytes,
                    media_type,
                    "execution.json",
                    self.maximum_payload_bytes,
                    &key,
                    InternalContentClass::Execution,
                )
                .await
        })
    }

    fn load<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        let span = obs_span!("acyclic.harness.journal.load", bytes = crate::obs::Empty);
        traced(span, async move {
            if reference.volume() != &self.volume {
                return Err(Error::Unauthorized(
                    "journal content belongs to another private volume".into(),
                ));
            }
            let grant = ContentGrant::verify(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Read,
            )?;
            let bytes = self
                .host
                .read_content(reference, &grant, self.maximum_payload_bytes)
                .await?;
            crate::obs::obs_record!("bytes" = bytes.len());
            Ok(bytes.to_vec())
        })
    }

    fn load_measured<'a>(
        &'a self, reference: &'a FileRef, budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<u8>>, Error>> {
        Box::pin(async move {
            let io = super::PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = async {
                reference.validate()?;
                if reference.volume() != &self.volume {
                    return Err(Error::Unauthorized("journal content belongs to another private volume".into()));
                }
                // The constructor verified this immutable private scope's
                // read/write grants. Borrow it; do not mint a new grant or
                // weaken the original journal volume/payload policy.
                let capability = io.capability(&self.volume, "volume:read:")?;
                if !self.scope.capabilities().contains(&capability) {
                    return Err(Error::Unauthorized("volume operation is not granted".into()));
                }
                let bytes = self.host.read_pinned_io(reference, self.maximum_payload_bytes, Some(&io)).await?;
                io.claim(bytes.len() as u64)?;
                let copied = acyclic_fs::WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() };
                io.preflight(copied)?;
                io.allocation_attempt(bytes.len() as u64)?;
                let output = bytes.to_vec();
                io.charge(copied)?;
                Ok(output)
            }.await;
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
        &'a self,
        operation_id: OperationId,
        selected: &'a SelectedModelContext,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.require_operation(operation_id)?;
            let turn_operation = self.turn_operation(operation_id);
            // A retained observation pins the original-turn association on reopen.
            self.replay_verified(operation_id, 0, 1).await?;
            let verifier = self.input_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("conversation content verifier is not bound".into())
            })?;
            let limits = self
                .owner
                .as_ref()
                .map_or_else(Limits::default, |(owner, _)| owner.input_limits());
            limits.validate()?;
            if selected.selection.message_ids.len() > limits.context_messages {
                return Err(Error::Invalid(
                    "selected history exceeds context message limit".into(),
                ));
            }
            let reader = crate::store::HistoryReader::new(
                &self.stream,
                self.verifier.audience(),
                self.verifier.clone(),
            )?;
            let event = reader
                .operation_event(turn_operation)
                .await?
                .ok_or_else(|| {
                    Error::Conflict("turn has no committed model-context selection".into())
                })?;
            let crate::core::EventPayload::ModelContextSelected {
                selection: committed,
            } = event.payload
            else {
                return Err(Error::Conflict(
                    "turn operation is not a model-context selection".into(),
                ));
            };
            if committed != selected.selection {
                return Err(Error::Conflict(
                    "model-context selection does not match the committed turn".into(),
                ));
            }
            // The exact attested selection event fixes the cutoff. Later parent
            // appends cannot change these indexed immutable records on retry.
            let cursor = crate::store::HistoryCursor {
                authority: self.verifier.audience().clone(),
                after_revision: 0,
                through_revision: event.revision,
            };
            let lookup_bytes = (acyclic_stream::MAX_RECORD_BYTES as u64)
                .checked_mul(2)
                .ok_or_else(|| Error::Invalid("selected history byte bound overflows".into()))?;
            let mut loaded = Vec::with_capacity(committed.message_ids.len());
            for id in &committed.message_ids {
                loaded.push(
                    reader
                        .conversation_message(&cursor, *id, lookup_bytes)
                        .await?
                        .ok_or_else(|| {
                            Error::Storage(
                                "committed selected message is missing at its cutoff".into(),
                            )
                        })?,
                );
            }
            let historical = crate::conversation::ConversationState::selected_view(loaded)?;
            let (messages, attachments, render_bytes) = self.owner.as_ref().map_or(
                (
                    crate::conversation::MAX_PORTABLE_COUNT,
                    crate::conversation::MAX_PORTABLE_COUNT,
                    self.maximum_payload_bytes,
                ),
                |(owner, _)| {
                    let limits = owner.input_limits();
                    (
                        limits.context_messages,
                        limits.attachments,
                        limits.render_bytes,
                    )
                },
            );
            let projected = select_model_context_at_revision(
                &historical,
                committed.clone(),
                verifier.as_ref(),
                messages,
                attachments,
                render_bytes,
                attachments,
            )
            .await?;
            if &projected != selected {
                return Err(Error::Conflict(
                    "model context differs from committed conversation projection".into(),
                ));
            }
            Ok(())
        })
    }

    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            interaction.validate()?;
            if let Some((owner, _)) = &self.owner {
                owner.require_interaction_grant()?;
                owner.verify(false).await?;
            }
            if let Some(original) = self.interactions()?.read_request(id).await? {
                if let Some((owner, _)) = &self.owner {
                    owner.verify(false).await?;
                }
                return if original == interaction {
                    Ok(())
                } else {
                    Err(Error::Conflict("interaction identity reused".into()))
                };
            }
            // Staging and publication must not inflate this journal future's inline state.
            let ticket =
                Box::pin(self.interactions()?.stage_request(id, &interaction, None)).await?;
            let owner_scope = self.interactions()?.owner_scope();
            let publication = if let Some((owner, _)) = &self.owner {
                Box::pin(self.interactions()?.open_owned(
                    interaction_operation(id, "open"),
                    owner_scope,
                    ticket,
                    owner,
                ))
                .await
            } else {
                Box::pin(self.interactions()?.open(
                    interaction_operation(id, "open"),
                    owner_scope,
                    ticket,
                ))
                .await
            };
            if let Some((owner, _)) = &self.owner {
                owner.verify(false).await?;
            }
            let result = match publication {
                Ok(_) => Ok(()),
                Err(Error::Conflict(_)) | Err(Error::Indeterminate(_)) => {
                    match self.interactions()?.read_request(id).await? {
                        Some(original) if original == interaction => Ok(()),
                        Some(_) => Err(Error::Conflict("interaction identity reused".into())),
                        None => Err(Error::Indeterminate(interaction_operation(id, "open"))),
                    }
                }
                Err(error) => Err(error),
            };
            if let Some((owner, _)) = &self.owner {
                owner.verify(false).await?;
            }
            result
        })
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>> {
        Box::pin(async move {
            if let Some((owner, _)) = &self.owner {
                owner.require_interaction_grant()?;
                owner.verify(true).await?;
            }
            let outcome = self
                .interactions()?
                .read(id)
                .await?
                .and_then(|(_, resolution)| resolution.map(|value| value.outcome));
            if let Some((owner, _)) = &self.owner {
                owner.verify(true).await?;
            }
            Ok(outcome)
        })
    }
}

fn verify_summary_prefix(
    request: &crate::model::PreparedModelRequest,
    source: &crate::context::Context,
    count: u32,
) -> Result<()> {
    let count = count as usize;
    let expected = count
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("summary prefix count overflows".into()))?;
    let messages = &request.request().messages;
    if messages.len() != expected || messages.get(..count) != source.messages.get(..count) {
        return Err(Error::Conflict(
            "canonical summary request differs from its source prefix".into(),
        ));
    }
    Ok(())
}

fn interaction_operation(id: InteractionId, phase: &str) -> OperationId {
    let digest = blake3::hash(format!("interaction:{id}:{phase}").as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

async fn project_canonical_delta(
    loaded: Vec<crate::conversation::ConversationMessage>,
    selection: &crate::conversation::ModelContextSelection,
    verifier: &dyn ContentResidencyVerifier,
    limits: Limits,
) -> Result<crate::context::Context> {
    let ids = loaded
        .iter()
        .filter(|message| {
            matches!(
                message.kind,
                crate::conversation::MessageKind::System
                    | crate::conversation::MessageKind::User
                    | crate::conversation::MessageKind::Assistant
                    | crate::conversation::MessageKind::ToolCall
                    | crate::conversation::MessageKind::ToolResult
            )
        })
        .map(|message| message.id)
        .collect::<Vec<_>>();
    if ids != selection.message_ids {
        return Err(Error::Conflict(
            "checkpoint skips uncovered canonical history".into(),
        ));
    }
    let history = crate::conversation::ConversationState::selected_view(loaded)?;
    let selected = select_model_context_at_revision(
        &history,
        selection.clone(),
        verifier,
        limits.context_messages,
        limits.attachments,
        limits.render_bytes,
        limits.attachments,
    )
    .await?;
    let input = selected
        .messages
        .last()
        .ok_or_else(|| Error::Invalid("canonical input is missing".into()))?
        .content
        .clone();
    let mut selected_delta = selected;
    selected_delta.selection.checkpoint = None;
    crate::context::ContextPipeline::base_context(
        &crate::context::ContextInput {
            input,
            selected_context: Some(selected_delta),
            step: 0,
            prior_messages: Vec::new(),
        },
        limits,
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
        let provider = ProviderRef::new("observation-identity", "filesystem", "2")?;
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
        let scope = issuer.root_for_agent(
            agent,
            "owner",
            crate::Capabilities::new([
                volume.capability(VolumeOperation::Read)?,
                volume.capability(VolumeOperation::Write)?,
            ]),
        );
        let stream = StreamClient::new(Arc::new(acyclic_stream::MemoryStream::default()));
        let open = |operation, turn| {
            FilesystemExecutionJournal::new(
                stream.clone(),
                host.clone(),
                volume.clone(),
                issuer.verifier(),
                scope.clone(),
                4096,
            )
            .map(|journal| journal.with_turn(operation, turn))
        };
        let turn = OperationId::new();
        let operation = super::super::task_runtime::execution_operation(crate::TaskId::new(), turn);
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
            Err(Error::Conflict(message)) if message.contains("another original turn")
        ));
        assert!(matches!(
            reopened.replay(turn, 0, 1).await,
            Err(Error::Unauthorized(_))
        ));
        let (_, first_retry, _) = Observation::encode(operation, turn, "first", &event)?;
        let (_, second_retry, _) = Observation::encode(operation, turn, "second", &event)?;
        assert_ne!(first_retry, second_retry);
        // Summary provider attempts have their own operation, not a new journal
        // namespace. Substituting that operation into the envelope must fail.
        let summary_operation = ModelPurpose::Summary.attempt_operation(operation);
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
            let (_, _, bytes) = Observation::encode(target, turn, "start", &event)?;
            let mut wire: serde_json::Value = serde_json::from_slice(&bytes)
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

    #[test]
    fn compaction_publication_requires_settled_summary_before_response() -> Result<()> {
        let operation = OperationId::new();
        let volume = VolumeRef::new(
            ProviderRef::new("test", "memory", "1")?,
            "journal",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::new()),
        )?;
        let file = FileRef::new(
            volume,
            "context.json",
            "1",
            FileDescriptor::from_bytes(b"null", "application/json")?,
            "context.json",
        )?;
        let compacted = ExecutionEvent::ContextCompacted {
            step: 0,
            projection: file.clone(),
            compaction: file.clone(),
            accounting: file.clone(),
        };
        let mut state = ExecutionSummary::default();
        let observations = [
            ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            ExecutionEvent::ContextPrepared {
                step: 0,
                projection: file.clone(),
                accounting: None,
            },
            ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Summary,
                request_digest: [2; 32],
                request: file.clone(),
            },
            ExecutionEvent::Model {
                step: 0,
                purpose: ModelPurpose::Summary,
                event: file.clone(),
            },
        ];
        let completed = ModelEvent::Completed {
            metadata: serde_json::Value::Null,
        };
        for (index, event) in observations.into_iter().enumerate() {
            assert!(state.require_next(&compacted).is_err());
            let model = matches!(event, ExecutionEvent::Model { .. }).then_some(&completed);
            state.accept(
                &ExecutionRecord {
                    operation_id: operation,
                    sequence: state.tail + 1,
                    idempotency_key: format!("admit:{index}"),
                    event,
                },
                model,
                Limits::default(),
            )?;
        }
        state.require_next(&compacted)?;
        state.accept(
            &ExecutionRecord {
                operation_id: operation,
                sequence: state.tail + 1,
                idempotency_key: "compacted".into(),
                event: compacted.clone(),
            },
            None,
            Limits::default(),
        )?;
        assert!(state.require_next(&compacted).is_err());
        let wrong_step = ExecutionEvent::ContextCompacted {
            step: 1,
            projection: file.clone(),
            compaction: file.clone(),
            accounting: file.clone(),
        };
        assert!(state.require_next(&wrong_step).is_err());
        let response = ExecutionEvent::ModelStarted {
            step: 0,
            purpose: ModelPurpose::Response,
            request_digest: [3; 32],
            request: file,
        };
        state.accept(
            &ExecutionRecord {
                operation_id: operation,
                sequence: state.tail + 1,
                idempotency_key: "response".into(),
                event: response,
            },
            None,
            Limits::default(),
        )?;
        // Even a fresh summary checkpoint identity cannot publish after response dispatch.
        state.compacted_contexts.clear();
        assert!(state.require_next(&compacted).is_err());
        Ok(())
    }

    #[test]
    fn summary_and_response_share_quiescence_without_aliasing_admissions() -> Result<()> {
        let operation = OperationId::new();
        let file = FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test", "filesystem", "1")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            )?,
            "request.json",
            "1",
            FileDescriptor::from_bytes(b"null", "application/json")?,
            "request.json",
        )?;
        let limits = Limits::default();
        for first_start in [ModelPurpose::Response, ModelPurpose::Summary] {
            for first_completion in [ModelPurpose::Response, ModelPurpose::Summary] {
                let mut state = ExecutionSummary::default();
                state.accept(
                    &ExecutionRecord {
                        operation_id: operation,
                        sequence: 1,
                        idempotency_key: "started".into(),
                        event: ExecutionEvent::Started {
                            request_digest: [1; 32],
                        },
                    },
                    None,
                    limits,
                )?;
                let prepared = ExecutionEvent::ContextPrepared {
                    step: 0,
                    projection: file.clone(),
                    accounting: None,
                };
                state.accept(
                    &ExecutionRecord {
                        operation_id: operation,
                        sequence: state.tail + 1,
                        idempotency_key: "context-prepared".into(),
                        event: prepared.clone(),
                    },
                    None,
                    limits,
                )?;
                assert!(state.require_next(&prepared).is_err());
                for purpose in [first_start, other(first_start)] {
                    let start = ExecutionEvent::ModelStarted {
                        step: 0,
                        purpose,
                        request_digest: [2; 32],
                        request: file.clone(),
                    };
                    state.accept(
                        &ExecutionRecord {
                            operation_id: operation,
                            sequence: state.tail + 1,
                            idempotency_key: format!("start:{purpose:?}"),
                            event: start.clone(),
                        },
                        None,
                        limits,
                    )?;
                    assert!(state.require_next(&start).is_err());
                    assert!(!state.quiescent(None)?);
                }
                for purpose in [first_completion, other(first_completion)] {
                    state.accept(
                        &ExecutionRecord {
                            operation_id: operation,
                            sequence: state.tail + 1,
                            idempotency_key: format!("complete:{purpose:?}"),
                            event: ExecutionEvent::Model {
                                step: 0,
                                purpose,
                                event: file.clone(),
                            },
                        },
                        Some(&ModelEvent::Completed {
                            metadata: serde_json::json!({}),
                        }),
                        limits,
                    )?;
                    assert_eq!(state.quiescent(None)?, purpose != first_completion);
                    assert!(
                        state
                            .require_next(&ExecutionEvent::Model {
                                step: 0,
                                purpose,
                                event: file.clone()
                            })
                            .is_err()
                    );
                }
            }
        }
        assert_eq!(
            ModelPurpose::Response.attempt_operation(operation),
            operation
        );
        assert_ne!(
            ModelPurpose::Summary.attempt_operation(operation),
            operation
        );
        assert_ne!(
            ModelPurpose::Summary.attempt_operation(operation),
            ModelPurpose::Summary.attempt_operation(OperationId::new())
        );
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
        let client = StreamClient::new(provider.clone());
        let fs_provider = ProviderRef::new("cold-canonical-source", "filesystem", "2")?;
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
        let scope = issuer.root_for_agent(
            agent,
            "owner",
            crate::Capabilities::new([
                "conversation:bind".into(),
                "conversation:append".into(),
                "conversation:select_context".into(),
                volume.capability(VolumeOperation::Read)?,
                volume.capability(VolumeOperation::Write)?,
            ]),
        );
        let content = Arc::new(super::super::FilesystemContentVerifier::new(
            host.clone(),
            issuer.verifier(),
            scope.clone(),
            4096,
        )?);
        let publisher = super::super::FilesystemContentPublisher::new(
            host.clone(),
            volume.clone(),
            &issuer.verifier(),
            &scope,
            4096,
        )?;
        let mut aggregate = StreamAggregate::open(
            &client,
            authority.clone(),
            issuer.verifier(),
            SchemaRegistry::new(),
        )
        .await?
        .with_content_verifier(content.clone())
        .with_resident_event_limit(1)?;
        aggregate
            .execute(Command {
                operation_id: OperationId::new(),
                idempotency_key: crate::IdempotencyKey::new("cold-bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            })
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
                            attachments: Vec::new().into(),
                            reply_to,
                            tool_call_id: None,
                            extensions: Default::default(),
                        }),
                    },
                })
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
                })
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
            let selected = select_model_context_at_revision(
                history,
                selection,
                content,
                limits.context_messages,
                limits.attachments,
                limits.render_bytes,
                limits.attachments,
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
                limits,
            )
        }
        let history = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::NotFound("cold history".into()))?;
        let omitted_source = source(history, omitted.clone(), content.as_ref(), limits).await?;
        let complete_source = source(history, complete.clone(), content.as_ref(), limits).await?;
        let selected = select_model_context_at_revision(
            history,
            complete.clone(),
            content.as_ref(),
            limits.context_messages,
            limits.attachments,
            limits.render_bytes,
            limits.attachments,
        )
        .await?;
        let placeholder = references
            .last()
            .ok_or_else(|| Error::NotFound("cold placeholder".into()))?
            .clone();
        // This tests the actual source barrier, not complete checkpoint publication:
        // no summary admission or compaction-envelope validity is asserted here.
        let envelope = |operation_id, selection| crate::context::CanonicalContextCheckpoint {
            operation_id,
            selection,
            source: placeholder.clone(),
            retained: placeholder.clone(),
            compaction: placeholder.clone(),
        };
        let new_journal = || {
            FilesystemExecutionJournal::new_with_schemas(
                client.clone(),
                host.clone(),
                volume.clone(),
                issuer.verifier(),
                SchemaRegistry::new(),
                scope.clone(),
                4096,
            )
            .map(|journal| journal.with_input_verifier(content.clone()))
        };
        let journal = new_journal()?.with_history_read_limits(crate::store::HistoryReadLimits {
            maximum_events: 4,
            maximum_bytes: 16_384,
        })?;
        // The committed conversation operation is the original turn, while
        // stock execution and its checkpoint retain the task-derived namespace.
        let execution_operation = super::super::task_runtime::execution_operation(
            crate::TaskId::new(),
            complete_operation,
        );
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
            .verify_canonical_source(
                &envelope(execution_operation, complete.clone()),
                &complete_source,
                limits,
            )
            .await?;
        assert!(matches!(
            new_journal()?
                .with_turn(execution_operation, omitted_operation)
                .verify_selected_context(execution_operation, &selected)
                .await,
            Err(Error::Conflict(message)) if message.contains("committed turn")
        ));
        provider.forbid_writes.store(true, Ordering::SeqCst);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(
            matches!(journal.verify_canonical_source(&envelope(omitted_operation, omitted), &omitted_source, limits).await,
            Err(Error::Conflict(message)) if message.contains("skips uncovered canonical history"))
        );
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 10);
        assert!(journal.conversation_projection.lock().await.is_none());
        provider.observation_reads.store(0, Ordering::SeqCst);
        journal
            .verify_canonical_source(
                &envelope(complete_operation, complete.clone()),
                &complete_source,
                limits,
            )
            .await?;
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 10);
        assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 1);
        assert!(journal.conversation_projection.lock().await.is_none());
        let mut altered = complete_source.clone();
        altered
            .messages
            .last_mut()
            .ok_or_else(|| Error::NotFound("cold source input".into()))?
            .content = crate::model::ModelContent::Text("altered source".into());
        assert!(
            matches!(journal.verify_canonical_source(&envelope(complete_operation, complete.clone()), &altered, limits).await,
            Err(Error::Conflict(message)) if message.contains("canonical base and delta"))
        );
        // Model projection fits three records, while the independent range work
        // bound must count the intervening Interaction record as well.
        let bounded = new_journal()?;
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            bounded
                .verify_canonical_source(
                    &envelope(complete_operation, complete.clone()),
                    &complete_source,
                    limits
                )
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
        let byte_bounded =
            new_journal()?.with_history_read_limits(crate::store::HistoryReadLimits {
                maximum_events: 4,
                maximum_bytes: 1,
            })?;
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            byte_bounded
                .verify_canonical_source(
                    &envelope(complete_operation, complete),
                    &complete_source,
                    limits
                )
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 3);
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        Ok(())
    }

    fn other(purpose: ModelPurpose) -> ModelPurpose {
        match purpose {
            ModelPurpose::Response => ModelPurpose::Summary,
            ModelPurpose::Summary => ModelPurpose::Response,
        }
    }
}
