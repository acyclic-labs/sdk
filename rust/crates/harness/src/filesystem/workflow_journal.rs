//! Ref-only Stream journal for pinned resumable workflow transitions.
//!
//! Passive finite observation borrows the original task owner and cumulative
//! preparation/native ledger. Receipts cover accepted protocol-bounded Stream
//! records and SDK data work, not opaque provider transfers/allocations or RSS.

use super::{FilesystemHost, InternalContentClass, PreparationIo};
use crate::obs::{obs_span, traced};
use crate::{
    Error, IdempotencyKey, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
    durable_host::TaskJournalOwner,
    workflow::{
        MachineCheckpoint, MachineStatus, WORKFLOW_REPLAY_PAGE_RECORDS, WorkflowAdmission,
        WorkflowCommitOutcome, WorkflowJournal, WorkflowRecord, validate_workflow_commit,
        validate_workflow_next, validate_workflow_page,
    },
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use crate::store::history::HistoryIo as _;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

struct VerifiedSummary {
    admission: WorkflowAdmission,
    revision: u64,
    checkpoint: MachineCheckpoint,
    reserved: BTreeSet<crate::OperationId>,
    keys: BTreeSet<IdempotencyKey>,
    terminal: bool,
}

impl VerifiedSummary {
    fn require_next(&self, record: &WorkflowRecord) -> Result<()> {
        validate_workflow_next(
            &self.checkpoint,
            self.terminal,
            &self.reserved,
            &self.keys,
            record,
        )
    }

    fn advance(&mut self, record: &WorkflowRecord) {
        for identity in record.operation_ids() {
            self.reserved.insert(identity);
        }
        self.keys.insert(record.idempotency_key.clone());
        self.revision = record.next.revision;
        self.checkpoint = record.next.clone();
        self.terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
    }
}

/// One workflow's atomic transition ledger; Streams never carry state or command bodies.
pub struct FilesystemWorkflowJournal<P, A, O> {
    stream: StreamClient<P>,
    host: Arc<FilesystemHost<A, O>>,
    workflow_id: String,
    path: String,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    maximum_payload_bytes: u64,
    verified: Mutex<Option<VerifiedSummary>>,
    owner: Option<Arc<TaskJournalOwner<P>>>,
}

/// A private original-owner observation, usable only with an atomic workflow
/// tail comparison. An admission or caller-supplied status cannot create it.
pub(crate) struct WorkflowReleaseWitness<'a, P> {
    owner: &'a TaskJournalOwner<P>,
    path: &'a str,
    tail: u64,
}

impl<P: StreamProvider> WorkflowReleaseWitness<'_, P> {
    pub(crate) fn task_binding(&self) -> (crate::TaskId, &crate::scheduler::LeaseFence) {
        self.owner.task_binding()
    }
    pub(crate) fn path(&self) -> &str { self.path }
    pub(crate) fn tail(&self) -> u64 { self.tail }
}

impl<P, A, O> FilesystemWorkflowJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn require_execution_owner(
        &self, execution_owner: &TaskJournalOwner<P>, volume: &VolumeRef,
        authority: &crate::core::Authority,
    ) -> Result<()> {
        let owner = self.owner.as_ref().ok_or_else(|| Error::Unsupported(
            "workflow release requires its original task owner".into()))?;
        if owner.task_binding() != execution_owner.task_binding()
            || owner.workflow_admission() != execution_owner.workflow_admission()
            || &self.volume != volume || self.verifier.audience() != authority
        {
            return Err(Error::Unauthorized("workflow and execution original owners differ".into()));
        }
        Ok(())
    }

    /// Binds one task workflow to its exact worker lease. Publications atomically
    /// compare the coordinator tail and workflow tail on the same Stream provider.
    pub fn for_task(
        owner: TaskJournalOwner<P>,
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        scope: Scope,
        maximum_payload_bytes: u64,
    ) -> Result<Self> {
        owner.validate_storage(&verifier, maximum_payload_bytes)?;
        let mut journal = Self::new(
            owner.stream(),
            host,
            &owner.workflow_admission().operation_id.to_string(),
            volume,
            verifier,
            scope,
            maximum_payload_bytes,
        )?;
        journal.owner = Some(Arc::new(owner));
        Ok(journal)
    }

    pub(crate) fn task_journal_owner(&self) -> Option<Arc<TaskJournalOwner<P>>> {
        self.owner.clone()
    }

    /// Binds a validated workflow identity and its exact private-volume grants.
    pub fn new(
        stream: StreamClient<P>,
        host: Arc<FilesystemHost<A, O>>,
        workflow_id: &str,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        scope: Scope,
        maximum_payload_bytes: u64,
    ) -> Result<Self> {
        Self::new_with_io(stream, host, workflow_id, volume, verifier, scope,
            maximum_payload_bytes, None)
    }

    pub(crate) fn new_with_io(
        stream: StreamClient<P>, host: Arc<FilesystemHost<A, O>>, workflow_id: &str,
        volume: VolumeRef, verifier: AuthorityVerifier, scope: Scope,
        maximum_payload_bytes: u64, io: Option<&PreparationIo>,
    ) -> Result<Self> {
        if workflow_id.is_empty()
            || workflow_id == "."
            || workflow_id == ".."
            || workflow_id.contains('/')
            || workflow_id.contains('\\')
            || workflow_id.chars().any(char::is_control)
            || volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider
            || maximum_payload_bytes == 0
        {
            return Err(Error::Invalid("workflow journal binding is invalid".into()));
        }
        if let Some(io) = io {
            ContentGrant::verify_original_volume_with_io(&verifier, &scope, &volume, VolumeOperation::Read, io)?;
            ContentGrant::verify_original_volume_with_io(&verifier, &scope, &volume, VolumeOperation::Write, io)?;
        } else {
            ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Read)?;
            ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Write)?;
        }
        let path_hold;
        let path = crate::contract::preparation_format(
            format_args!("harness/v2/workflows/{workflow_id}"), io)?;
        path_hold = io.map(|io| io.guard_owned(path.capacity() as u64)).transpose()?;
        let stream_hold;
        let (validated, held) = Self::observation_stream_from(&stream, &path, io)?;
        stream_hold = held;
        drop(validated);
        drop(stream_hold);
        let id_hold;
        let workflow_id = if let Some(io) = io {
            let copied = acyclic_fs::WorkCounters { bytes_copied: workflow_id.len() as u64, ..Default::default() };
            io.preflight(copied)?;
            io.claim(workflow_id.len() as u64)?;
            id_hold = Some(io.guard_owned(workflow_id.len() as u64)?);
            io.allocation_attempt(workflow_id.len() as u64)?;
            let owned = workflow_id.to_owned();
            io.charge(copied)?;
            owned
        } else { id_hold = None; workflow_id.to_owned() };
        if let Some(held) = path_hold { held.into_retained(); }
        if let Some(held) = id_hold { held.into_retained(); }
        Ok(Self {
            stream,
            host,
            workflow_id,
            path,
            volume,
            verifier,
            scope,
            maximum_payload_bytes,
            verified: Mutex::new(None),
            owner: None,
        })
    }

    async fn read_record(
        &self,
        reference: &FileRef,
        verify_payloads: bool,
        verify_machine: bool,
    ) -> Result<WorkflowRecord> {
        self.read_record_with_io(reference, verify_payloads, verify_machine, None).await
    }

    async fn read_record_with_io(
        &self, reference: &FileRef, verify_payloads: bool, verify_machine: bool,
        io: Option<&PreparationIo>,
    ) -> Result<WorkflowRecord> {
        if io.is_some() && verify_machine {
            return Err(Error::Unsupported("finite workflow replay cannot execute an unmeasured machine callback".into()));
        }
        let _prefix;
        let prefix = crate::contract::preparation_format(
            format_args!(".system/workflows/{}/", self.workflow_id), io,
        )?;
        _prefix = io.map(|io| io.guard_owned(prefix.capacity() as u64)).transpose()?;
        if reference.volume() != &self.volume
            || reference.descriptor().media_type() != "application/json"
            || !reference.path().starts_with(&prefix)
        {
            return Err(Error::Unauthorized(
                "workflow record belongs to another private volume".into(),
            ));
        }
        let _bytes;
        let bytes = self.read_bound_content(reference, io).await?;
        _bytes = io.map(|io| io.guard_owned(bytes.len() as u64)).transpose()?;
        let record: WorkflowRecord = match io {
            Some(io) => io.decode_preparation::<WorkflowRecord>(&bytes)?.into_retained(),
            None => crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Storage(format!("workflow record is invalid: {error}")))?,
        };
        record.validate_with_io(io)?;
        if let Some(owner) = &self.owner {
            if verify_machine {
                owner.validate_workflow_record(&record)?;
            } else if record.prior.machine != owner.workflow_admission().initial.machine
                || record.next.machine != owner.workflow_admission().initial.machine
            {
                return Err(Error::Unauthorized("observed workflow changed its pinned machine".into()));
            }
        }
        if verify_payloads {
            self.verify_command_payloads_with_io(&record, io).await?;
        }
        Ok(record)
    }

    async fn replay_records(
        &self,
        after: u64,
        maximum: u32,
        verify_payloads: bool,
        verify_machine: bool,
    ) -> Result<Vec<WorkflowRecord>> {
        self.replay_records_with_io(after, maximum, verify_payloads, verify_machine, None).await
    }

    async fn replay_records_with_io(
        &self, after: u64, maximum: u32, verify_payloads: bool,
        verify_machine: bool, io: Option<&PreparationIo>,
    ) -> Result<Vec<WorkflowRecord>> {
        if io.is_some() && verify_machine {
            return Err(Error::Unsupported("finite workflow replay cannot execute an unmeasured machine callback".into()));
        }
        validate_workflow_page(after, maximum)?;
        let _path;
        let (stream, held) = self.observation_stream(&self.path, io)?;
        _path = held;
        self.begin_observation_call(io)?;
        let entries = match stream
            .read(after, maximum.min(WORKFLOW_REPLAY_PAGE_RECORDS))
            .await
        {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => {
                self.check_observation_cancellation(io)?;
                return Ok(Vec::new());
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        let mut entries = entries;
        let capacity = maximum.min(WORKFLOW_REPLAY_PAGE_RECORDS) as usize;
        let vector_bytes = capacity.checked_mul(std::mem::size_of::<WorkflowRecord>())
            .ok_or_else(|| Error::Invalid("workflow replay vector ownership overflow".into()))? as u64;
        let vector_hold = if let Some(io) = io {
            io.claim(vector_bytes)?;
            let hold = io.guard_owned(vector_bytes)?;
            io.allocation_attempt(vector_bytes)?;
            Some(hold)
        } else { None };
        let mut records = Vec::with_capacity(capacity);
        loop {
            let _entry;
            let Some((entry, held)) = Self::next_observation_record(&mut entries, io).await? else { break; };
            _entry = held;
            let sequence = after + records.len() as u64;
            if records.len() >= capacity || entry.sequence != sequence {
                return Err(Error::Storage(
                    "workflow sequence or page bound is invalid".into(),
                ));
            }
            let reference_hold;
            let reference_before = io.map(PreparationIo::owned_bytes).transpose()?;
            let reference: FileRef = match io {
                Some(io) => io.decode_preparation::<FileRef>(&entry.value)?.into_retained(),
                None => crate::contract::json_from_slice(&entry.value).map_err(|error| {
                    Error::Storage(format!("workflow reference is invalid: {error}"))
                })?,
            };
            reference_hold = match (io, reference_before) {
                (Some(io), Some(before)) => Some(io.guard_owned(io.owned_bytes()?.checked_sub(before)
                    .ok_or_else(|| Error::Invalid("workflow reference custody underflow".into()))?)?),
                _ => None,
            };
            let record = self.read_record_with_io(&reference, verify_payloads, verify_machine, io).await?;
            if record.prior.revision != sequence || record.next.revision != sequence + 1 {
                return Err(Error::Storage(
                    "workflow checkpoint revision is discontinuous".into(),
                ));
            }
            records.push(record);
            drop(reference);
            drop(reference_hold);
        }
        if let Some(hold) = vector_hold { hold.into_retained(); }
        Ok(records)
    }

    /// Reads one committed transition for passive observation. Command bodies
    /// are verified individually by the task reader before readiness checks.
    pub(crate) async fn observe_transition(&self, revision: u64) -> Result<WorkflowRecord> {
        self.observe_transition_with_io(revision, None).await
    }

    pub(crate) async fn observe_transition_with_io(
        &self, revision: u64, io: Option<&PreparationIo>,
    ) -> Result<WorkflowRecord> {
        let after = revision.checked_sub(1)
            .ok_or_else(|| Error::Invalid("zero observation revision".into()))?;
        self.replay_records_with_io(after, 1, false, false, io)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Storage("observed workflow checkpoint is absent".into()))
    }

    /// Reads the exact historical transition using the original native ledger.
    /// The returned guard owns its audited logical decode allowance until the
    /// record's last use; it is not an opaque allocator-footprint measurement.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn observe_transition_with_native_work(
        &self, revision: u64, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(WorkflowRecord, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let record = self.observe_transition_with_io(revision, Some(&io)).await?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok((record, custody))
    }

    /// Reads original admission through the canonical authenticated engine,
    /// retaining its audited logical decode allowance with the returned DTO.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn admission_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<WorkflowAdmission>, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let admission = self.read_admission_with_io(Some(&io)).await?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok((admission, custody))
    }

    /// Original authenticated outbox observation; no machine callback executes.
    pub(crate) async fn observe_latest_outbox(&self) -> Result<Option<crate::workflow::MachineTransition>> {
        Ok(self.observe_latest_record().await?.map(|record| record.transition))
    }

    pub(crate) async fn observe_latest_record(&self) -> Result<Option<WorkflowRecord>> {
        self.observe_latest_record_engine(None).await
    }

    pub(crate) async fn observe_latest_record_with_io(
        &self, io: &PreparationIo,
    ) -> Result<Option<WorkflowRecord>> {
        self.observe_latest_record_engine(Some(io)).await
    }

    async fn observe_latest_record_engine(
        &self, io: Option<&PreparationIo>,
    ) -> Result<Option<WorkflowRecord>> {
        let owner = self.owner.as_ref().ok_or_else(|| {
            Error::Unsupported("task outbox observation requires its original owner".into())
        })?;
        self.verify_observation_owner(owner, io).await?;
        let Some(admission) = self.read_admission_with_io(io).await? else { return Ok(None); };
        if &admission != owner.workflow_admission() {
            return Err(Error::Unauthorized("observed outbox differs from original task admission".into()));
        }
        let _path;
        let (stream, held) = self.observation_stream(&self.path, io)?;
        _path = held;
        self.begin_observation_call(io)?;
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => {
                self.check_observation_cancellation(io)?;
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        self.check_observation_cancellation(io)?;
        if bounds.tail == 0 { return Ok(None); }
        let record = self.replay_records_with_io(bounds.tail - 1, 1, true, false, io).await?
            .into_iter().next()
            .ok_or_else(|| Error::Storage("original outbox record is absent".into()))?;
        self.verify_observation_owner(owner, io).await?;
        Ok(Some(record))
    }

    pub(crate) async fn prove_release_with_io(
        &self, io: &PreparationIo,
    ) -> Result<Option<WorkflowReleaseWitness<'_, P>>> {
        let Some(record) = self.observe_latest_record_with_io(io).await? else { return Ok(None); };
        if !matches!(record.transition.status, MachineStatus::Suspended) { return Ok(None); }
        let owner = self.owner.as_ref().ok_or_else(|| {
            Error::Unsupported("workflow release requires its original task owner".into())
        })?;
        Ok(Some(WorkflowReleaseWitness { owner, path: &self.path, tail: record.next.revision }))
    }

    async fn verify_observation_owner(
        &self, owner: &TaskJournalOwner<P>, io: Option<&PreparationIo>,
    ) -> Result<()> {
        match io {
            Some(io) => {
                let mut borrowed = io.history_io();
                owner.verify_with_work(true, &mut Some(&mut borrowed)).await
            }
            None => owner.verify(true).await,
        }
    }

    fn begin_observation_call(&self, io: Option<&PreparationIo>) -> Result<()> {
        if let Some(io) = io { io.history_io().begin_call()?; }
        Ok(())
    }

    fn check_observation_cancellation(&self, io: Option<&PreparationIo>) -> Result<()> {
        if let Some(io) = io { io.check_cancellation()?; }
        Ok(())
    }

    fn observation_stream<'a>(
        &self, path: &str, io: Option<&'a PreparationIo>,
    ) -> Result<(acyclic_stream::Stream<P>, Option<super::DecodeReservation<'a>>)> {
        Self::observation_stream_from(&self.stream, path, io)
    }

    fn observation_stream_from<'a>(
        stream: &StreamClient<P>, path: &str, io: Option<&'a PreparationIo>,
    ) -> Result<(acyclic_stream::Stream<P>, Option<super::DecodeReservation<'a>>)> {
        let held = if let Some(io) = io {
            let work = acyclic_fs::WorkCounters {
                bytes_copied: path.len() as u64, items_examined: path.len() as u64,
                ..Default::default()
            };
            io.preflight(work)?;
            let hold = io.reserve_decode(path.len() as u64)?;
            io.allocation_attempt(path.len() as u64)?;
            io.charge(work)?;
            Some(hold)
        } else { None };
        Ok((stream.stream(path)?, held))
    }

    async fn next_observation_record<'a>(
        entries: &mut acyclic_stream::RecordStream, io: Option<&'a PreparationIo>,
    ) -> Result<Option<(acyclic_stream::Record, Option<super::DecodeReservation<'a>>)>> {
        if let Some(io) = io { io.check_cancellation()?; }
        let held;
        let Some(record) = entries.try_next().await? else {
            if let Some(io) = io { io.check_cancellation()?; }
            return Ok(None);
        };
        held = if let Some(io) = io {
            io.history_io().receive_record(record.value.len() as u64)?;
            Some(io.guard_owned(record.value.len() as u64)?)
        } else { None };
        if record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(Error::Storage("workflow Stream record exceeds protocol bound".into()));
        }
        Ok(Some((record, held)))
    }

    async fn read_bound_content(&self, reference: &FileRef, io: Option<&PreparationIo>) -> Result<Bytes> {
        match io {
            Some(io) => {
                super::fork_preparer_io::item(Some(io), 0)?;
                reference.validate()?;
                if reference.volume() != &self.volume {
                    return Err(Error::Unauthorized("workflow content belongs to another private volume".into()));
                }
                ContentGrant::verify_original_volume_with_io(
                    &self.verifier, &self.scope, &self.volume, VolumeOperation::Read, io,
                )?;
                self.host.read_pinned_io(reference, self.maximum_payload_bytes, Some(io)).await
            }
            None => {
                let read = ContentGrant::verify(
                    &self.verifier, &self.scope, &self.volume, VolumeOperation::Read,
                )?;
                self.host.read_content(reference, &read, self.maximum_payload_bytes).await
            }
        }
    }
    async fn verified_history(
        &self,
        admission: &WorkflowAdmission,
        operation_id: crate::OperationId,
    ) -> Result<(u64, Option<WorkflowRecord>)> {
        // Retain identities and one checkpoint, never historic state or command bodies.
        let mut summary = VerifiedSummary {
            admission: admission.clone(),
            revision: 0,
            checkpoint: admission.initial.clone(),
            reserved: BTreeSet::new(),
            keys: BTreeSet::new(),
            terminal: false,
        };
        let mut matched = None;
        loop {
            let page = self
                .replay_records(summary.revision, WORKFLOW_REPLAY_PAGE_RECORDS, false, true)
                .await?;
            if page.is_empty() {
                break;
            }
            for record in page {
                summary.require_next(&record).map_err(|_| {
                    Error::Storage(
                        "workflow history diverges from admission or reuses an identity".into(),
                    )
                })?;
                summary.advance(&record);
                if record.operation_id == operation_id {
                    matched = Some(record);
                }
            }
        }
        let revision = summary.revision;
        if let Ok(mut cached) = self.verified.lock()
            && cached
                .as_ref()
                .is_none_or(|current| current.revision <= summary.revision)
        {
            *cached = Some(summary);
        }
        Ok((revision, matched))
    }

    fn cached_preflight(
        &self,
        admission: &WorkflowAdmission,
        record: &WorkflowRecord,
    ) -> Result<bool> {
        let cached = self
            .verified
            .lock()
            .map_err(|_| Error::Storage("workflow summary cache is poisoned".into()))?;
        let Some(summary) = cached.as_ref() else {
            return Ok(false);
        };
        if summary.admission != *admission || summary.revision != record.prior.revision {
            return Ok(false);
        }
        summary.require_next(record)?;
        Ok(true)
    }

    fn advance_cached(&self, admission: &WorkflowAdmission, record: &WorkflowRecord) {
        if let Ok(mut cached) = self.verified.lock()
            && let Some(summary) = cached.as_mut()
            && summary.admission == *admission
            && summary.revision == record.prior.revision
        {
            summary.advance(record);
        }
    }

    fn admission_path(&self) -> String {
        format!("harness/v2/workflow-admissions/{}", self.workflow_id)
    }

    async fn has_transition(&self) -> Result<bool> {
        let stream = self.stream.stream(&self.path)?;
        let entries = match stream.read(0, 1).await {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => return Ok(false),
            Err(error) => return Err(Error::Storage(error.to_string())),
        }
        .try_collect::<Vec<_>>()
        .await?;
        Ok(!entries.is_empty())
    }

    async fn read_admission(&self) -> Result<Option<WorkflowAdmission>> {
        self.read_admission_with_io(None).await
    }

    pub(crate) async fn read_admission_with_io(&self, io: Option<&PreparationIo>) -> Result<Option<WorkflowAdmission>> {
        let _path;
        let path = crate::contract::preparation_format(
            format_args!("harness/v2/workflow-admissions/{}", self.workflow_id), io,
        )?;
        _path = io.map(|io| io.guard_owned(path.capacity() as u64)).transpose()?;
        let _stream_path;
        let (stream, held) = self.observation_stream(&path, io)?;
        _stream_path = held;
        self.begin_observation_call(io)?;
        let mut entries = match stream.read(0, 2).await {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => {
                self.check_observation_cancellation(io)?;
                return Ok(None);
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        let _entry;
        let Some((entry, held)) = Self::next_observation_record(&mut entries, io).await? else {
            return Ok(None);
        };
        _entry = held;
        if Self::next_observation_record(&mut entries, io).await?.is_some() {
            return Err(Error::Storage("workflow admission stream is invalid".into()));
        }
        if entry.sequence != 0 {
            return Err(Error::Storage(
                "workflow admission sequence is invalid".into(),
            ));
        }
        let _reference_hold;
        let reference_before = io.map(PreparationIo::owned_bytes).transpose()?;
        let reference: FileRef = match io {
            Some(io) => io.decode_preparation::<FileRef>(&entry.value)?.into_retained(),
            None => crate::contract::json_from_slice(&entry.value).map_err(|error| {
                Error::Storage(format!("workflow admission reference is invalid: {error}"))
            })?,
        };
        _reference_hold = match (io, reference_before) {
            (Some(io), Some(before)) => Some(io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| Error::Invalid("workflow admission reference custody underflow".into()))?)?),
            _ => None,
        };
        let _expected_path;
        let expected_path = crate::contract::preparation_format(
            format_args!(".system/workflows/{}/admission.json", self.workflow_id), io,
        )?;
        _expected_path = io.map(|io| io.guard_owned(expected_path.capacity() as u64)).transpose()?;
        if reference.volume() != &self.volume
            || reference.path() != expected_path
            || reference.descriptor().media_type() != "application/json"
        {
            return Err(Error::Unauthorized(
                "workflow admission belongs to another volume".into(),
            ));
        }
        let _bytes;
        let bytes = self.read_bound_content(&reference, io).await?;
        _bytes = io.map(|io| io.guard_owned(bytes.len() as u64)).transpose()?;
        let admission: WorkflowAdmission = match io {
            Some(io) => io.decode_preparation::<WorkflowAdmission>(&bytes)?.into_retained(),
            None => crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Storage(format!("workflow admission is invalid: {error}")))?,
        };
        admission.validate_with_io(io)?;
        if self
            .owner
            .as_ref()
            .is_some_and(|owner| owner.workflow_admission() != &admission)
        {
            return Err(Error::Conflict(
                "workflow differs from retained task admission".into(),
            ));
        }
        Ok(Some(admission))
    }

    async fn stage_admission(&self, admission: &WorkflowAdmission) -> Result<FileRef> {
        let bytes =
            serde_json::to_vec(admission).map_err(|error| Error::Invalid(error.to_string()))?;
        if bytes.len() as u64 > self.maximum_payload_bytes {
            return Err(Error::Invalid(
                "workflow admission exceeds private-file limit".into(),
            ));
        }
        let write = ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.volume,
            VolumeOperation::Write,
        )?;
        self.host
            .put_internal_content(
                &self.volume,
                &write,
                &format!(".system/workflows/{}/admission.json", self.workflow_id),
                &bytes,
                "application/json",
                "admission.json",
                self.maximum_payload_bytes,
                &IdempotencyKey::new(format!("workflow:{}:admission", self.workflow_id))?,
                InternalContentClass::Workflow,
            )
            .await
    }

    async fn verify_command_payloads(&self, record: &WorkflowRecord) -> Result<()> {
        self.verify_command_payloads_with_io(record, None).await
    }

    async fn verify_command_payloads_with_io(
        &self, record: &WorkflowRecord, io: Option<&PreparationIo>,
    ) -> Result<()> {
        for command in &record.transition.commands {
            if command.payload.descriptor().byte_length() > self.maximum_payload_bytes {
                return Err(Error::Invalid("workflow command exceeds payload limit".into()));
            }
            if let Some(io) = io {
                let step = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
                io.preflight(step)?;
                io.charge(step)?;
            }
            if let Some(owner) = &self.owner
                && owner.verify_created_input_with_io(&command.payload, io).await?
            {
                continue;
            }
            if command.payload.volume() != &self.volume {
                return Err(Error::Unauthorized(
                    "workflow command payload belongs to another volume".into(),
                ));
            }
            let _bytes;
            let bytes = self.read_bound_content(&command.payload, io).await?;
            _bytes = io.map(|io| io.guard_owned(bytes.len() as u64)).transpose()?;
        }
        Ok(())
    }

    async fn stage(&self, record: &WorkflowRecord) -> Result<FileRef> {
        // A committed transition may expose its commands immediately on
        // recovery. Admit their immutable argument versions first.
        self.verify_command_payloads(record).await?;
        let bytes =
            serde_json::to_vec(record).map_err(|error| Error::Invalid(error.to_string()))?;
        if bytes.len() as u64 > self.maximum_payload_bytes {
            return Err(Error::Invalid(
                "workflow transition exceeds private-file limit".into(),
            ));
        }
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
                &format!(
                    ".system/workflows/{}/{:020}-{}.json",
                    self.workflow_id, record.prior.revision, record.operation_id
                ),
                &bytes,
                "application/json",
                "transition.json",
                self.maximum_payload_bytes,
                &IdempotencyKey::new(format!(
                    "workflow:{}:{}",
                    self.workflow_id, record.operation_id
                ))?,
                InternalContentClass::Workflow,
            )
            .await
    }

    async fn reconcile(&self, record: &WorkflowRecord) -> Result<Option<WorkflowCommitOutcome>> {
        let admission = self.read_admission().await?.ok_or_else(|| {
            Error::Conflict("workflow transition has no retained admission".into())
        })?;
        let (_, existing) = self
            .verified_history(&admission, record.operation_id)
            .await?;
        if let Some(existing) = existing {
            return if existing == *record {
                self.verify_command_payloads(&existing).await?;
                Ok(Some(WorkflowCommitOutcome::Replayed(existing)))
            } else {
                Err(Error::Conflict("workflow operation identity reused".into()))
            };
        }
        // Validate collisions with committed command identities as well.
        let cached = self
            .verified
            .lock()
            .map_err(|_| Error::Storage("workflow summary cache is poisoned".into()))?;
        if cached.as_ref().is_some_and(|summary| {
            record
                .operation_ids()
                .any(|id| summary.reserved.contains(&id))
        }) {
            return Err(Error::Conflict(
                "workflow command identity is already bound".into(),
            ));
        }
        Ok(None)
    }
}

impl<P, A, O> WorkflowJournal for FilesystemWorkflowJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn task_binding(&self) -> Option<(crate::TaskId, &crate::scheduler::LeaseFence)> {
        self.owner.as_ref().map(TaskJournalOwner::task_binding)
    }

    fn admit<'a>(
        &'a self,
        admission: WorkflowAdmission,
    ) -> BoxFuture<'a, Result<WorkflowAdmission>> {
        let span = obs_span!("acyclic.harness.workflow.admit");
        traced(span, async move {
            admission.validate()?;
            if let Some(owner) = &self.owner {
                if &admission != owner.workflow_admission() {
                    return Err(Error::Unauthorized(
                        "workflow differs from retained task admission".into(),
                    ));
                }
                owner.verify(false).await?;
            }
            if let Some(existing) = self.read_admission().await? {
                return if existing == admission {
                    Ok(existing)
                } else {
                    Err(Error::Conflict(
                        "workflow admission identity is already bound".into(),
                    ))
                };
            }
            if self.has_transition().await? {
                return Err(Error::Conflict(
                    "workflow transitions exist without admission".into(),
                ));
            }
            let reference = self.stage_admission(&admission).await?;
            let bytes = serde_json::to_vec(&reference)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(Error::Invalid(
                    "workflow admission reference exceeds Stream limit".into(),
                ));
            }
            let digest = blake3::hash(format!("{}:admission", self.admission_path()).as_bytes());
            let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes()))
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if let Some(owner) = &self.owner {
                if owner
                    .append_workflow(true, 0, &key, Bytes::from(bytes))
                    .await?
                {
                    return Ok(admission);
                }
                return match self.read_admission().await? {
                    Some(existing) if existing == admission => Ok(existing),
                    Some(_) => Err(Error::Conflict(
                        "workflow admission identity is already bound".into(),
                    )),
                    None => Err(Error::Conflict(
                        "workflow admission ownership changed".into(),
                    )),
                };
            }
            let stream = self.stream.stream(self.admission_path())?;
            match stream
                .append_batch(vec![Bytes::from(bytes)], Some(0), Some(key))
                .await
            {
                Ok(AppendOutcome::Committed(receipt)) if receipt.start == 0 && receipt.end == 1 => {
                    Ok(admission)
                }
                Ok(AppendOutcome::Committed(_)) => Err(Error::Storage(
                    "workflow admission receipt is invalid".into(),
                )),
                Ok(AppendOutcome::TailConflict { .. }) | Err(StreamError::IdempotencyMismatch) => {
                    match self.read_admission().await? {
                        Some(existing) if existing == admission => Ok(existing),
                        Some(_) => Err(Error::Conflict(
                            "workflow admission identity is already bound".into(),
                        )),
                        None => Err(Error::Indeterminate(admission.operation_id)),
                    }
                }
                Err(StreamError::Unavailable) => match self.read_admission().await {
                    Ok(Some(existing)) if existing == admission => Ok(existing),
                    Ok(Some(_)) => Err(Error::Conflict(
                        "workflow admission identity is already bound".into(),
                    )),
                    Ok(None) | Err(Error::Storage(_)) => {
                        Err(Error::Indeterminate(admission.operation_id))
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(Error::Storage(error.to_string())),
            }
        })
    }

    fn admission(&self) -> BoxFuture<'_, Result<Option<WorkflowAdmission>>> {
        Box::pin(self.read_admission())
    }

    fn replay(&self, after: u64, maximum: u32) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>> {
        let span = obs_span!("acyclic.harness.workflow.replay", rev = after);
        traced(span, self.replay_records(after, maximum, true, true))
    }

    fn commit<'a>(
        &'a self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: WorkflowRecord,
    ) -> BoxFuture<'a, Result<WorkflowCommitOutcome>> {
        let span = obs_span!("acyclic.harness.workflow.commit", rev = expected_revision);
        traced(span, async move {
            if let Some(owner) = &self.owner {
                owner.verify(false).await?;
            }
            let admission = self.read_admission().await?.ok_or_else(|| {
                Error::Conflict("workflow transition has no retained admission".into())
            })?;
            validate_workflow_commit(expected_revision, &idempotency_key, &record)?;
            if let Some(owner) = &self.owner {
                owner.validate_workflow_record(&record)?;
            }
            if !self.cached_preflight(&admission, &record)? {
                // Reopening or reconciling an old retry scans the authoritative
                // chain once. The normal next step uses its verified summary;
                // Stream's tail CAS fences concurrent writers after preflight.
                let (revision, existing) = self
                    .verified_history(&admission, record.operation_id)
                    .await?;
                if let Some(existing) = existing {
                    return if existing == record {
                        self.verify_command_payloads(&existing).await?;
                        Ok(WorkflowCommitOutcome::Replayed(existing))
                    } else {
                        Err(Error::Conflict(
                            "workflow operation identity is already bound".into(),
                        ))
                    };
                }
                if revision != expected_revision {
                    return Err(Error::Conflict(
                        "workflow checkpoint revision is stale".into(),
                    ));
                }
                self.cached_preflight(&admission, &record)?;
            }
            let reference = self.stage(&record).await?;
            if self.read_record(&reference, true, true).await? != record {
                return Err(Error::Conflict(
                    "staged workflow record disagrees with its transition".into(),
                ));
            }
            let bytes = serde_json::to_vec(&reference)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(Error::Invalid(
                    "workflow reference exceeds Stream limit".into(),
                ));
            }
            let digest =
                blake3::hash(format!("{}:{}", self.path, idempotency_key.as_str()).as_bytes());
            let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes()))
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if let Some(owner) = &self.owner {
                if owner
                    .append_workflow(false, expected_revision, &key, Bytes::from(bytes))
                    .await?
                {
                    self.advance_cached(&admission, &record);
                    return Ok(WorkflowCommitOutcome::Applied(record));
                }
                return match self.reconcile(&record).await? {
                    Some(previous) => Ok(previous),
                    None => Err(Error::Conflict(
                        "workflow checkpoint or ownership changed".into(),
                    )),
                };
            }
            let stream = self.stream.stream(&self.path)?;
            match stream
                .append_batch(vec![Bytes::from(bytes)], Some(expected_revision), Some(key))
                .await
            {
                Ok(AppendOutcome::Committed(receipt))
                    if receipt.start == expected_revision
                        && Some(receipt.end) == expected_revision.checked_add(1) =>
                {
                    self.advance_cached(&admission, &record);
                    Ok(WorkflowCommitOutcome::Applied(record))
                }
                Ok(AppendOutcome::Committed(_)) => {
                    Err(Error::Storage("workflow append receipt is invalid".into()))
                }
                Ok(AppendOutcome::TailConflict { .. }) => match self.reconcile(&record).await? {
                    Some(previous) => Ok(previous),
                    None => Err(Error::Conflict(
                        "workflow checkpoint revision is stale".into(),
                    )),
                },
                Err(StreamError::Unavailable) => match self.reconcile(&record).await {
                    Ok(Some(previous)) => Ok(previous),
                    Ok(None) | Err(Error::Storage(_)) => {
                        Err(Error::Indeterminate(record.operation_id))
                    }
                    Err(error) => Err(error),
                },
                Err(StreamError::IdempotencyMismatch) => {
                    Err(Error::Conflict("workflow retry identity reused".into()))
                }
                Err(error) => Err(Error::Storage(error.to_string())),
            }
        })
    }
}
