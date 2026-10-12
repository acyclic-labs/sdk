//! Ref-only Stream journal for pinned resumable workflow transitions.

use super::{FilesystemHost, InternalContentClass, PreparationIo};
use super::interaction_host::interaction_boundary as observation_boundary;
use crate::obs::{obs_span, traced};
use crate::{
    Error, IdempotencyKey, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
    durable_host::TaskJournalOwner,
    workflow::{
        MachineCheckpoint, MachineStatus, WORKFLOW_REPLAY_PAGE_RECORDS, WorkflowAdmission,
        WorkflowCommitOutcome, WorkflowJournal, WorkflowRecord,
        validate_workflow_next,
    },
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, OwnedBytes, StreamClient, StreamError, StreamProvider,
};
use futures::TryStreamExt as _;
use crate::store::history::HistoryIo as _;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

fn external_read_hold<'a>(
    bytes: &OwnedBytes, io: Option<&'a PreparationIo>,
) -> Result<Option<super::DecodeReservation<'a>>> {
    let Some(io) = io else { return Ok(None); };
    if io.uses_native_work() { return Ok(None); }
    let retained = bytes.retained_capacity().ok_or_else(|| observation_boundary(
        Some(io), crate::wire::ErrorCode::Unsupported,
        "workflow byte backing capacity is unavailable",
    ))?;
    io.guard_owned(retained as u64).map(Some)
}

struct VerifiedSummary {
    admission: WorkflowAdmission,
    checkpoint: MachineCheckpoint,
    reserved: BTreeSet<crate::OperationId>,
    keys: BTreeSet<IdempotencyKey>,
    terminal: bool,
}

impl VerifiedSummary {
    fn advance(&mut self, record: &WorkflowRecord) {
        for identity in record.operation_ids() {
            self.reserved.insert(identity);
        }
        self.keys.insert(record.idempotency_key.clone());
        self.checkpoint = record.next.clone();
        self.terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
    }
}

struct WorkflowIdentities<'a> {
    io: Option<&'a PreparationIo>,
    owned: u64,
    reserved: BTreeSet<crate::OperationId>,
    keys: BTreeSet<IdempotencyKey>,
}

impl WorkflowIdentities<'_> {
    fn advance(&mut self, record: &WorkflowRecord) -> Result<()> {
        let Some(io) = self.io else {
            self.reserved.extend(record.operation_ids());
            self.keys.insert(record.idempotency_key.clone());
            return Ok(());
        };
        for identity in record.operation_ids() {
            let bytes = std::mem::size_of::<crate::OperationId>() as u64;
            let owned = self.owned.checked_add(bytes)
                .ok_or_else(|| Error::Invalid("workflow identity ownership overflow".into()))?;
            let work = acyclic_fs::WorkCounters {
                bytes_copied: bytes, items_examined: 1, ..Default::default()
            };
            io.check_cancellation()?;
            io.preflight(work)?;
            io.retain_owned(bytes)?;
            self.owned = owned;
            io.allocation_attempt(bytes)?;
            io.charge(work)?;
            self.reserved.insert(identity);
        }
        let copied = record.idempotency_key.0.len() as u64;
        let bytes = copied.checked_add(std::mem::size_of::<IdempotencyKey>() as u64)
            .ok_or_else(|| Error::Invalid("workflow identity ownership overflow".into()))?;
        let work = acyclic_fs::WorkCounters {
            bytes_copied: copied, items_examined: copied + 1, ..Default::default()
        };
        let owned = self.owned.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("workflow identity ownership overflow".into()))?;
        io.preflight(work)?;
        io.retain_owned(bytes)?;
        self.owned = owned;
        io.allocation_attempt(bytes)?;
        io.charge(work)?;
        self.keys.insert(record.idempotency_key.clone());
        Ok(())
    }
}

impl Drop for WorkflowIdentities<'_> {
    fn drop(&mut self) {
        self.reserved.clear();
        self.keys.clear();
        if let Some(io) = self.io { io.release(self.owned).expect("workflow identities retain their original ledger"); }
    }
}

/// One workflow's atomic transition ledger; Streams never carry state or command bodies.
pub struct FilesystemWorkflowJournal<P, A, O> {
    stream: StreamClient<P>,
    host: Arc<FilesystemHost<A, O>>,
    path: String,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    maximum_payload_bytes: u64,
    verified: Mutex<Option<VerifiedSummary>>,
    owner: Option<Arc<TaskJournalOwner<P>>>,
    #[cfg(not(target_arch = "wasm32"))]
    native_metadata: Option<acyclic_fs::NativeAllocation>,
}

/// A private original-owner observation, usable only with an atomic workflow tail comparison.
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

    /// Binds one task workflow to its exact worker lease.
    pub fn for_task(
        owner: TaskJournalOwner<P>,
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        scope: Scope,
        maximum_payload_bytes: u64,
    ) -> Result<Self> {
        owner.validate_storage(&verifier, maximum_payload_bytes, &mut None)?;
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

    /// Uses the original sealed configuration and ledger before any SDK copy.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn for_task_with_native_work(
        owner: TaskJournalOwner<P>, host: Arc<FilesystemHost<A, O>>,
        volume: &VolumeRef, verifier: &AuthorityVerifier, scope: &Scope,
        maximum_payload_bytes: u64, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        owner.validate_storage(verifier, maximum_payload_bytes, &mut Some(&mut crate::store::history::native_work_io(work)?))?;
        let stream = owner.stream();
        let io = PreparationIo::from_native_work(work)?;
        ContentGrant::verify_original_volume_with_io(verifier, scope, volume, VolumeOperation::Read, &io)?;
        ContentGrant::verify_original_volume_with_io(verifier, scope, volume, VolumeOperation::Write, &io)?;
        let (verifier, verifier_custody) = verifier.clone_with_native_work(work)?;
        io.absorb_native_custody(verifier_custody)?;
        let (scope, scope_custody) = scope.clone_with_native_work(work)?;
        io.absorb_native_custody(scope_custody)?;
        let volume = volume.clone_with_io(&io)?;
        let identity_io = PreparationIo::from_native_work(work)?;
        let _identity;
        let identity = crate::contract::preparation_format(format_args!("{}", owner.operation_id()), Some(&identity_io))?;
        _identity = identity_io.guard_owned(identity.capacity() as u64)?;
        let mut journal = Self::configure(stream, host, &identity, volume, verifier, scope,
            maximum_payload_bytes, Some(&io))?;
        journal.owner = Some(owner.into_native_shared(work)?);
        Ok(journal)
    }

    /// Shares an already funded journal, admitting the actual SDK Arc attempt.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self.native_metadata.as_ref().is_some_and(|custody|
            custody.work().shares_ledger(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native journal sharing requires its original funded metadata",
            });
        }
        let io = PreparationIo::from_native_work(work)?;
        io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        Ok(Arc::new(self))
    }

    /// Binds a workflow identity and its private-volume grants.
    pub fn new(
        stream: StreamClient<P>, host: Arc<FilesystemHost<A, O>>, workflow_id: &str,
        volume: VolumeRef, verifier: AuthorityVerifier, scope: Scope, maximum_payload_bytes: u64,
    ) -> Result<Self> {
        Self::configure(stream, host, workflow_id, volume, verifier, scope, maximum_payload_bytes, None)
    }

    fn configure(
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
        let (validated, held) = Self::observation_stream(&stream, &path, io)?;
        stream_hold = held;
        drop(validated);
        drop(stream_hold);
        #[cfg(not(target_arch = "wasm32"))]
        let native_metadata = if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            Some(io.take_native_custody(io.owned_bytes()?)?)
        } else { None };
        if let Some(held) = path_hold { held.into_retained(); }
        Ok(Self {
            stream,
            host,
            path,
            volume,
            verifier,
            scope,
            maximum_payload_bytes,
            verified: Mutex::new(None),
            owner: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_metadata,
        })
    }

    fn id(&self) -> &str { &self.path["harness/v2/workflows/".len()..] }

    async fn read_record(
        &self, reference: &FileRef, verify_payloads: bool, verify_machine: bool,
        io: Option<&PreparationIo>,
    ) -> Result<WorkflowRecord> {
        self.require_machine_io(verify_machine, io)?;
        let _prefix;
        let prefix = crate::contract::preparation_format(
            format_args!(".system/workflows/{}/", self.id()), io,
        )?;
        _prefix = io.map(|io| io.guard_owned(prefix.capacity() as u64)).transpose()?;
        if reference.volume() != &self.volume
            || reference.descriptor().media_type() != "application/json"
            || !reference.path().starts_with(&prefix)
        {
            return Err(observation_boundary(io, crate::wire::ErrorCode::Unauthorized,
                "workflow record belongs to another private volume"));
        }
        let _bytes;
        let bytes = self.read_bound_content(reference, io).await?;
        _bytes = external_read_hold(&bytes, io)?;
        let record: WorkflowRecord = match io {
            Some(io) => io.decode_preparation::<WorkflowRecord>(&bytes)?.into_retained(),
            None => crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Storage(format!("workflow record is invalid: {error}")))?,
        };
        record.validate(io)?;
        if let Some(owner) = &self.owner {
            if verify_machine {
                owner.validate_workflow_record(&record, io)?;
            } else if record.prior.machine != owner.workflow_admission().initial.machine
                || record.next.machine != owner.workflow_admission().initial.machine
            {
                return Err(observation_boundary(io, crate::wire::ErrorCode::Unauthorized,
                    "observed workflow changed its pinned machine"));
            }
        }
        if verify_payloads {
            self.verify_command_payloads(&record, io).await?;
        }
        Ok(record)
    }

    fn require_machine_io(&self, verify_machine: bool, io: Option<&PreparationIo>) -> Result<()> {
        if verify_machine && io.is_some() {
            #[cfg(not(target_arch = "wasm32"))]
            if io.and_then(PreparationIo::original_native_work).is_some() {
                return self.owner.as_ref().map_or(Ok(()), |owner| owner.require_native_workflow_validation());
            }
            return Err(observation_boundary(io, crate::wire::ErrorCode::Unsupported,
                "finite workflow replay requires a native machine callback"));
        }
        Ok(())
    }

    async fn replay_records(
        &self, after: u64, maximum: u32, verify_payloads: bool,
        verify_machine: bool, io: Option<&PreparationIo>,
    ) -> Result<Vec<WorkflowRecord>> {
        self.require_machine_io(verify_machine, io)?;
        if maximum == 0 {
            return Err(observation_boundary(io, crate::wire::ErrorCode::Invalid,
                "workflow page bounds are invalid"));
        }
        let _path;
        let (stream, held) = Self::observation_stream(&self.stream, &self.path, io)?;
        _path = held;
        if let Some(io) = io { io.history_io().begin_call()?; }
        let _request_path = Self::retain_observation_request_path(&self.path, io)?;
        let entries = match stream
            .read(after, maximum.min(WORKFLOW_REPLAY_PAGE_RECORDS))
            .await
        {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => {
                if let Some(io) = io { io.check_cancellation()?; }
                return Ok(Vec::new());
            }
            Err(error) => return Err(Self::observation_stream_error(io, error)),
        };
        let mut entries = entries;
        let capacity = maximum.min(WORKFLOW_REPLAY_PAGE_RECORDS) as usize;
        let vector_bytes = capacity.checked_mul(std::mem::size_of::<WorkflowRecord>())
            .ok_or_else(|| observation_boundary(io, crate::wire::ErrorCode::Invalid,
                "workflow replay vector ownership overflow"))? as u64;
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
                return Err(observation_boundary(io, crate::wire::ErrorCode::Storage,
                    "workflow sequence or page bound is invalid"));
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
            let record = self.read_record(&reference, verify_payloads, verify_machine, io).await?;
            if record.prior.revision != sequence || record.next.revision != sequence + 1 {
                return Err(observation_boundary(io, crate::wire::ErrorCode::Storage,
                    "workflow checkpoint revision is discontinuous"));
            }
            records.push(record);
            drop(reference);
            drop(reference_hold);
        }
        if let Some(hold) = vector_hold { hold.into_retained(); }
        Ok(records)
    }

    /// Reads one committed transition for passive observation.
    pub(crate) async fn observe_transition(
        &self, revision: u64, io: Option<&PreparationIo>,
    ) -> Result<WorkflowRecord> {
        let after = revision.checked_sub(1)
            .ok_or_else(|| Error::Invalid("zero observation revision".into()))?;
        self.replay_records(after, 1, false, false, io)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Storage("observed workflow checkpoint is absent".into()))
    }

    /// Reads the exact historical transition using the original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn observe_transition_with_native_work(
        &self, revision: u64, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(WorkflowRecord, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let record = self.observe_transition(revision, Some(&io)).await?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok((record, custody))
    }

    /// Original authenticated outbox observation; no machine callback executes.
    pub(crate) async fn observe_latest_outbox(&self) -> Result<Option<crate::workflow::MachineTransition>> {
        Ok(self.observe_latest_record(None).await?.map(|record| record.transition))
    }

    /// Observes the original authenticated outbox without a machine callback.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn observe_latest_outbox_with_native_work<'a>(
        &'a self, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<crate::workflow::MachineTransition>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let transition = self.observe_latest_record(Some(&io)).await?
                .map(|record| record.transition);
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((transition, custody))
        })
    }

    pub(crate) async fn observe_latest_record(
        &self, io: Option<&PreparationIo>,
    ) -> Result<Option<WorkflowRecord>> {
        let owner = self.owner.as_ref().ok_or_else(||
            observation_boundary(io, crate::wire::ErrorCode::Unsupported,
                "task outbox observation requires its original owner"))?;
        self.verify_observation_owner(owner, io).await?;
        let Some(admission) = self.read_admission(io).await? else { return Ok(None); };
        if &admission != owner.workflow_admission() {
            return Err(observation_boundary(io, crate::wire::ErrorCode::Unauthorized,
                "observed outbox differs from original task admission"));
        }
        let _path;
        let (stream, held) = Self::observation_stream(&self.stream, &self.path, io)?;
        _path = held;
        if let Some(io) = io { io.history_io().begin_call()?; }
        let _request_path = Self::retain_observation_request_path(&self.path, io)?;
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => {
                if let Some(io) = io { io.check_cancellation()?; }
                return Ok(None);
            }
            Err(error) => return Err(Self::observation_stream_error(io, error)),
        };
        if let Some(io) = io { io.check_cancellation()?; }
        if bounds.tail == 0 { return Ok(None); }
        let record = self.replay_records(bounds.tail - 1, 1, true, false, io).await?
            .into_iter().next()
            .ok_or_else(|| observation_boundary(io, crate::wire::ErrorCode::Storage,
                "original outbox record is absent"))?;
        self.verify_observation_owner(owner, io).await?;
        Ok(Some(record))
    }

    pub(crate) async fn prove_release_with_io(
        &self, io: &PreparationIo,
    ) -> Result<Option<WorkflowReleaseWitness<'_, P>>> {
        let Some(record) = self.observe_latest_record(Some(io)).await? else { return Ok(None); };
        if !matches!(record.transition.status, MachineStatus::Suspended) { return Ok(None); }
        let owner = self.owner.as_ref().ok_or_else(|| {
            Error::Unsupported("workflow release requires its original task owner".into())
        })?;
        Ok(Some(WorkflowReleaseWitness { owner, path: &self.path, tail: record.next.revision }))
    }

    async fn verify_observation_owner(
        &self, owner: &TaskJournalOwner<P>, io: Option<&PreparationIo>,
    ) -> Result<()> {
        let mut history = io.map(PreparationIo::history_io);
        owner.verify(true, &mut history.as_mut().map(
            |history| history as &mut dyn crate::store::history::HistoryIo)).await
    }

    fn observation_stream<'a>(
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
        Ok((stream.stream(path).map_err(|error| Self::observation_stream_error(io, error))?, held))
    }

    fn observation_stream_error(io: Option<&PreparationIo>, source: StreamError) -> Error {
        #[cfg(not(target_arch = "wasm32"))]
        if io.and_then(PreparationIo::original_native_work).is_some() {
            return Error::NativeStream {
                code: crate::wire::ErrorCode::Storage, source, custody: None,
            };
        }
        Error::Storage(source.to_string())
    }

    fn retain_observation_request_path<'a>(
        path: &str, io: Option<&'a PreparationIo>,
    ) -> Result<Option<super::DecodeReservation<'a>>> {
        let Some(io) = io else { return Ok(None); };
        let copying = acyclic_fs::WorkCounters {
            bytes_copied: path.len() as u64, ..Default::default()
        };
        io.preflight(copying)?;
        let held = io.reserve_decode(path.len() as u64)?;
        io.allocation_attempt(path.len() as u64)?;
        io.charge(copying)?;
        Ok(Some(held))
    }

    async fn next_observation_record<'a>(
        entries: &mut acyclic_stream::RecordStream, io: Option<&'a PreparationIo>,
    ) -> Result<Option<(acyclic_stream::Record, Option<super::DecodeReservation<'a>>)>> {
        if let Some(io) = io { io.check_cancellation()?; }
        let held;
        let Some(record) = entries.try_next().await
            .map_err(|error| Self::observation_stream_error(io, error))? else {
            if let Some(io) = io { io.check_cancellation()?; }
            return Ok(None);
        };
        held = if let Some(io) = io {
            io.history_io().receive_record(record.value.len() as u64)?;
            Some(io.guard_owned(record.value.len() as u64)?)
        } else { None };
        if record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(observation_boundary(io, crate::wire::ErrorCode::Storage,
                "workflow Stream record exceeds protocol bound"));
        }
        Ok(Some((record, held)))
    }

    async fn read_bound_content(&self, reference: &FileRef, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
        match io {
            Some(io) => {
                if !io.uses_native_work() {
                    reference.validate_with_work(&mut Some(&mut io.history_io()))?;
                }
                if reference.volume() != &self.volume {
                    return Err(observation_boundary(Some(io), crate::wire::ErrorCode::Unauthorized,
                        "workflow content belongs to another private volume"));
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
        &self, admission: &WorkflowAdmission, operation_id: crate::OperationId,
        candidate: Option<&WorkflowRecord>, require_next: bool, io: Option<&PreparationIo>,
    ) -> Result<(u64, Option<WorkflowRecord>, bool)> {
        let mut identities = WorkflowIdentities {
            io, owned: 0, reserved: BTreeSet::new(), keys: BTreeSet::new(),
        };
        let mut previous_page_owned = 0;
        let mut matched_page_owned = 0;
        let mut previous: Option<WorkflowRecord> = None;
        let mut matched = None;
        let mut revision = 0;
        let mut terminal = false;
        let mut ready = false;
        loop {
            let before_page = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let page = self.replay_records(
                revision, WORKFLOW_REPLAY_PAGE_RECORDS, false, true, io,
            ).await?;
            let page_owned = io.map(|io| io.owned_bytes()?.checked_sub(before_page)
                .ok_or_else(|| Error::Invalid("workflow replay ownership decreased".into())))
                .transpose()?.unwrap_or(0);
            identities.owned = identities.owned.checked_add(page_owned)
                .ok_or_else(|| Error::Invalid("workflow replay ownership overflow".into()))?;
            let mut page_has_match = false;
            if page.is_empty() { break; }
            for record in page {
                let checkpoint = previous.as_ref().map_or(&admission.initial, |prior| &prior.next);
                validate_workflow_next(checkpoint, terminal, &identities.reserved, &identities.keys, &record, io)
                    .map_err(|error| match error {
                        Error::NativeWork { .. } => error,
                        _ => Error::Storage("workflow history diverges from admission or reuses an identity".into()),
                    })?;
                identities.advance(&record)?;
                revision = record.next.revision;
                terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
                if let Some(prior) = previous.take() {
                    if prior.operation_id == operation_id {
                        matched = Some(prior);
                        if previous_page_owned != 0 {
                            matched_page_owned = previous_page_owned;
                        } else {
                            page_has_match = true;
                            matched_page_owned = page_owned;
                        }
                    } else {
                        drop(prior);
                        if previous_page_owned != 0 {
                            if let Some(io) = io { io.release(previous_page_owned)?; }
                            identities.owned -= previous_page_owned;
                        }
                    }
                    previous_page_owned = 0;
                }
                previous = Some(record);
            }
            previous_page_owned = if page_has_match { 0 } else { page_owned };
        }
        if let Some(record) = candidate {
            if let Some(io) = io { io.check_cancellation()?; }
            for identity in record.operation_ids() {
                if let Some(io) = io { io.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?; }
                if identities.reserved.contains(&identity) && matched.is_none()
                    && previous.as_ref().is_none_or(|prior| prior.operation_id != operation_id) {
                    return Err(Error::Conflict("workflow command identity is already bound".into()));
                }
            }
            if require_next && revision == record.prior.revision {
                let checkpoint = previous.as_ref().map_or(&admission.initial, |prior| &prior.next);
                validate_workflow_next(checkpoint, terminal, &identities.reserved, &identities.keys, record, io)?;
                ready = true;
            }
        }
        if io.is_none()
            && let Ok(mut cached) = self.verified.lock()
            && cached.as_ref().is_none_or(|current| current.checkpoint.revision <= revision)
        {
            *cached = Some(VerifiedSummary {
                admission: admission.clone(),
                checkpoint: previous.as_ref().map_or(&admission.initial, |prior| &prior.next).clone(),
                reserved: std::mem::take(&mut identities.reserved),
                keys: std::mem::take(&mut identities.keys), terminal,
            });
        }
        if let Some(prior) = previous
            && prior.operation_id == operation_id {
            if previous_page_owned != 0 {
                matched_page_owned = previous_page_owned;
            }
            matched = Some(prior);
        }
        identities.owned = identities.owned.checked_sub(matched_page_owned)
            .ok_or_else(|| Error::Invalid("workflow matched custody underflow".into()))?;
        Ok((revision, matched, ready))
    }

    pub(crate) async fn verified_preflight_with_io(
        &self, admission: &WorkflowAdmission, record: &WorkflowRecord,
        io: Option<&PreparationIo>,
    ) -> Result<(u64, Option<WorkflowRecord>, bool)> {
        if let Some(io) = io {
            return self.verified_history(
                admission, record.operation_id, Some(record), true, Some(io),
            ).await;
        }
        if self.cached_preflight(admission, record)? {
            return Ok((record.prior.revision, None, true));
        }
        let (revision, matched, _) = self.verified_history(admission, record.operation_id, None, false, None).await?;
        let ready = matched.is_none() && revision == record.prior.revision
            && self.cached_preflight(admission, record)?;
        Ok((revision, matched, ready))
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
        if summary.admission != *admission || summary.checkpoint.revision != record.prior.revision {
            return Ok(false);
        }
        validate_workflow_next(&summary.checkpoint, summary.terminal, &summary.reserved,
            &summary.keys, record, None)?;
        Ok(true)
    }

    fn advance_cached(&self, admission: &WorkflowAdmission, record: &WorkflowRecord, io: Option<&PreparationIo>) {
        if io.is_some() { return; }
        if let Ok(mut cached) = self.verified.lock()
            && let Some(summary) = cached.as_mut()
            && summary.admission == *admission
            && summary.checkpoint.revision == record.prior.revision
        {
            summary.advance(record);
        }
    }

    pub(crate) async fn has_transition_with_io(&self, io: Option<&PreparationIo>) -> Result<bool> {
        let _path;
        let (stream, held) = Self::observation_stream(&self.stream, &self.path, io)?;
        _path = held;
        if let Some(io) = io { io.history_io().begin_call()?; }
        let mut entries = match stream.read(0, 1).await {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => {
                if let Some(io) = io { io.check_cancellation()?; }
                return Ok(false);
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        let _entry;
        let Some((entry, held)) = Self::next_observation_record(&mut entries, io).await? else {
            return Ok(false);
        };
        _entry = held;
        if entry.sequence != 0 || Self::next_observation_record(&mut entries, io).await?.is_some() {
            return Err(Error::Storage("workflow transition stream is invalid".into()));
        }
        Ok(true)
    }

    pub(crate) async fn read_admission(&self, io: Option<&PreparationIo>) -> Result<Option<WorkflowAdmission>> {
        let _path;
        let path = crate::contract::preparation_format(
            format_args!("harness/v2/workflow-admissions/{}", self.id()), io,
        )?;
        _path = io.map(|io| io.guard_owned(path.capacity() as u64)).transpose()?;
        let _stream_path;
        let (stream, held) = Self::observation_stream(&self.stream, &path, io)?;
        _stream_path = held;
        if let Some(io) = io { io.history_io().begin_call()?; }
        let _request_path = Self::retain_observation_request_path(&path, io)?;
        let mut entries = match stream.read(0, 2).await {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => {
                if let Some(io) = io { io.check_cancellation()?; }
                return Ok(None);
            }
            Err(error) => return Err(Self::observation_stream_error(io, error)),
        };
        let _entry;
        let Some((entry, held)) = Self::next_observation_record(&mut entries, io).await? else {
            return Ok(None);
        };
        _entry = held;
        if Self::next_observation_record(&mut entries, io).await?.is_some() {
            return Err(observation_boundary(io, crate::wire::ErrorCode::Storage,
                "workflow admission stream is invalid"));
        }
        if entry.sequence != 0 {
            return Err(observation_boundary(io, crate::wire::ErrorCode::Storage,
                "workflow admission sequence is invalid"));
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
            format_args!(".system/workflows/{}/admission.json", self.id()), io,
        )?;
        _expected_path = io.map(|io| io.guard_owned(expected_path.capacity() as u64)).transpose()?;
        if reference.volume() != &self.volume
            || reference.path() != expected_path
            || reference.descriptor().media_type() != "application/json"
        {
            return Err(observation_boundary(io, crate::wire::ErrorCode::Unauthorized,
                "workflow admission belongs to another volume"));
        }
        let _bytes;
        let bytes = self.read_bound_content(&reference, io).await?;
        _bytes = external_read_hold(&bytes, io)?;
        let admission: WorkflowAdmission = match io {
            Some(io) => io.decode_preparation::<WorkflowAdmission>(&bytes)?.into_retained(),
            None => crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Storage(format!("workflow admission is invalid: {error}")))?,
        };
        admission.validate(io)?;
        if self
            .owner
            .as_ref()
            .is_some_and(|owner| owner.workflow_admission() != &admission)
        {
            return Err(observation_boundary(io, crate::wire::ErrorCode::Conflict,
                "workflow differs from retained task admission"));
        }
        Ok(Some(admission))
    }

    async fn stage_payload<T: serde::Serialize>(
        &self, payload: &T, transition: Option<(u64, crate::OperationId)>, io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        let _bytes;
        let bytes = match io {
            Some(io) => io.encode(payload, self.maximum_payload_bytes, false)?,
            None => serde_json::to_vec(payload).map_err(|error| Error::Invalid(error.to_string()))?.into(),
        };
        _bytes = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        if bytes.len() as u64 > self.maximum_payload_bytes {
            return Err(Error::Invalid("workflow payload exceeds private-file limit".into()));
        }
        let _path;
        let path = match transition {
            Some((revision, operation)) => crate::contract::preparation_format(
                format_args!(".system/workflows/{}/{revision:020}-{operation}.json", self.id()), io)?,
            None => crate::contract::preparation_format(
                format_args!(".system/workflows/{}/admission.json", self.id()), io)?,
        };
        _path = io.map(|io| io.guard_owned(path.capacity() as u64)).transpose()?;
        let _key;
        let key = match transition {
            Some((_, operation)) => crate::contract::preparation_format(
                format_args!("workflow:{}:{operation}", self.id()), io)?,
            None => crate::contract::preparation_format(
                format_args!("workflow:{}:admission", self.id()), io)?,
        };
        _key = io.map(|io| io.guard_owned(key.capacity() as u64)).transpose()?;
        let key = IdempotencyKey::new(key)?;
        let _grant;
        let grant = match io {
            Some(io) => {
                let mut borrowed = io.history_io();
                ContentGrant::verify_with_work(
                    &self.verifier, &self.scope, &self.volume, VolumeOperation::Write,
                    io.remaining()?.bytes_encoded, &mut Some(&mut borrowed),
                )?
            }
            None => ContentGrant::verify(&self.verifier, &self.scope, &self.volume, VolumeOperation::Write)?,
        };
        _grant = io.map(|io| io.guard_owned(grant.logical_owned_bytes()?)).transpose()?;
        self.host.put_content_impl(&self.volume, &grant, &path, &bytes, "application/json",
            if transition.is_some() { "transition.json" } else { "admission.json" },
            self.maximum_payload_bytes, &key, Some(InternalContentClass::Workflow), None, io, None, None).await
    }

    async fn append_reference(
        &self, path: &str, retry: &str, revision: u64, operation: crate::OperationId,
        reference: &FileRef, io: Option<&PreparationIo>,
    ) -> Result<bool> {
        let admission = path != self.path;
        let _bytes;
        let bytes = match io {
            Some(io) => io.encode(reference, acyclic_stream::MAX_RECORD_BYTES as u64, false)?,
            None => serde_json::to_vec(reference).map_err(|error| Error::Invalid(error.to_string()))?.into(),
        };
        _bytes = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(Error::Invalid("workflow reference exceeds Stream limit".into()));
        }
        let digest = {
            let _input;
            let input = crate::contract::preparation_format(format_args!("{path}:{retry}"), io)?;
            _input = io.map(|io| io.guard_owned(input.capacity() as u64)).transpose()?;
            if let Some(io) = io {
                let hashing = acyclic_fs::WorkCounters { bytes_hashed: input.len() as u64, ..Default::default() };
                io.preflight(hashing)?;
                io.charge(hashing)?;
            }
            blake3::hash(input.as_bytes())
        };
        let _key;
        let key_bytes = super::fork_preparer_io::clone_bytes(digest.as_bytes(), io)?;
        _key = io.map(|io| io.guard_bytes(&key_bytes)).transpose()?.flatten();
        let key = StreamKey::new(key_bytes).map_err(|error| {
            #[cfg(not(target_arch = "wasm32"))]
            if io.is_some() {
                return Error::NativeStream { code: crate::wire::ErrorCode::Invalid, source: error, custody: None };
            }
            Error::Invalid(error.to_string())
        })?;
        if let Some(owner) = &self.owner {
            let mut borrowed = io.map(PreparationIo::history_io);
            let mut work = borrowed.as_mut().map(|work| work as &mut dyn crate::store::history::HistoryIo);
            return owner.append_workflow_with_work(admission, revision, &key, bytes, &mut work).await;
        }
        let _path;
        let (stream, held) = Self::observation_stream(&self.stream, path, io)?;
        _path = held;
        let identity = if admission { "workflow admission identity" } else { "workflow retry identity" };
        let appended = match io {
            Some(io) => {
                let mut borrowed = io.history_io();
                crate::distributed::append_keyed(
                    (&stream, &self.stream), bytes, Some(revision), key, operation,
                    identity, &mut Some(&mut borrowed),
                ).await
            }
            None => stream.append_batch(vec![bytes], Some(revision), Some(key)).await.map_err(|error| match error {
                StreamError::Unavailable => Error::Indeterminate(operation),
                StreamError::IdempotencyMismatch => Error::Conflict(format!("{identity} reused")),
                error => Error::Storage(error.to_string()),
            }),
        }?;
        match appended {
            AppendOutcome::Committed(receipt) if receipt.start == revision
                && Some(receipt.end) == revision.checked_add(1) => Ok(true),
            AppendOutcome::Committed(_) => Err(Error::Storage("workflow append receipt is invalid".into())),
            AppendOutcome::TailConflict { .. } => Ok(false),
        }
    }

    async fn verify_command_payloads(
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
                return Err(observation_boundary(io, crate::wire::ErrorCode::Unauthorized,
                    "workflow command payload belongs to another volume"));
            }
            let _bytes;
            let bytes = self.read_bound_content(&command.payload, io).await?;
            _bytes = external_read_hold(&bytes, io)?;
        }
        Ok(())
    }

    async fn reconcile(
        &self, record: &WorkflowRecord, io: Option<&PreparationIo>,
    ) -> Result<Option<WorkflowCommitOutcome>> {
        let admission = self.read_admission(io).await?.ok_or_else(|| {
            Error::Conflict("workflow transition has no retained admission".into())
        })?;
        let (_, existing, _) = self.verified_history(
            &admission, record.operation_id, Some(record), false, io,
        ).await?;
        let Some(existing) = existing else { return Ok(None); };
        let (_expected_hold, _actual_hold);
        let (expected, actual);
        if let Some(io) = io {
            expected = io.encode(record, self.maximum_payload_bytes, false)?;
            _expected_hold = io.guard_bytes(&expected)?;
            actual = io.encode(&existing, self.maximum_payload_bytes, false)?;
            _actual_hold = io.guard_bytes(&actual)?;
        }
        if existing != *record {
            return Err(Error::Conflict("workflow operation identity reused".into()));
        }
        self.verify_command_payloads(&existing, io).await?;
        Ok(Some(WorkflowCommitOutcome::Replayed(existing)))
    }

    pub(crate) async fn commit_with_io(
        &self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: WorkflowRecord,
        io: Option<&PreparationIo>,
    ) -> Result<WorkflowCommitOutcome> {
        self.require_machine_io(true, io)?;
        if let Some(io) = io { io.check_cancellation()?; }
        if let Some(owner) = &self.owner {
            let mut borrowed = io.map(PreparationIo::history_io);
            let mut work = borrowed.as_mut()
                .map(|work| work as &mut dyn crate::store::history::HistoryIo);
            owner.verify(false, &mut work).await?;
        }
        let _admission;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let admission = self.read_admission(io).await?.ok_or_else(|| {
            Error::Conflict("workflow transition has no retained admission".into())
        })?;
        _admission = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| Error::Invalid("workflow admission custody underflow".into()))?))
            .transpose()?;
        crate::workflow::validate_workflow_commit(
            expected_revision, &idempotency_key, &record, io,
        )?;
        if let Some(owner) = &self.owner {
            owner.validate_workflow_record(&record, io)?;
        }
        {
            let history_hold;
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let (revision, existing, ready) =
                self.verified_preflight_with_io(&admission, &record, io).await?;
            history_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| Error::Invalid("workflow preflight custody underflow".into()))?))
                .transpose()?;
            if let Some(existing) = existing {
                if existing != record {
                    return Err(Error::Conflict(
                        "workflow operation identity is already bound".into(),
                    ));
                }
                self.verify_command_payloads(&existing, io).await?;
                if let Some(held) = history_hold { held.into_retained(); }
                return Ok(WorkflowCommitOutcome::Replayed(existing));
            }
            if revision != expected_revision {
                return Err(Error::Conflict(
                    "workflow checkpoint revision is stale".into(),
                ));
            }
            if !ready {
                return Err(Error::Conflict(
                    "workflow transition does not follow its retained checkpoint".into(),
                ));
            }
        }
        let _reference;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        self.verify_command_payloads(&record, io).await?;
        let reference = self.stage_payload(&record, Some((record.prior.revision, record.operation_id)), io).await?;
        _reference = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| Error::Invalid("workflow staging custody underflow".into()))?))
            .transpose()?;
        {
            let _staged;
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let staged = self.read_record(&reference, true, true, io).await?;
            _staged = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| Error::Invalid("workflow staged-record custody underflow".into()))?))
                .transpose()?;
            if staged != record {
                return Err(Error::Conflict(
                    "staged workflow record disagrees with its transition".into(),
                ));
            }
        }
        let appended = self.append_reference(&self.path, idempotency_key.as_str(), expected_revision,
            record.operation_id, &reference, io).await;
        if self.owner.is_some() {
            if appended? {
                self.advance_cached(&admission, &record, io);
                return Ok(WorkflowCommitOutcome::Applied(record));
            }
            return match self.reconcile(&record, io).await? {
                Some(previous) => Ok(previous),
                None => Err(Error::Conflict("workflow checkpoint or ownership changed".into())),
            };
        }
        match appended {
            Ok(true) => {
                self.advance_cached(&admission, &record, io);
                Ok(WorkflowCommitOutcome::Applied(record))
            }
            Ok(false) => {
                match self.reconcile(&record, io).await? {
                    Some(previous) => Ok(previous),
                    None => Err(Error::Conflict(
                        "workflow checkpoint revision is stale".into(),
                    )),
                }
            }
            Err(Error::Indeterminate(_)) => match self.reconcile(&record, io).await {
                Ok(Some(previous)) => Ok(previous),
                Ok(None) | Err(Error::Storage(_)) => {
                    Err(Error::Indeterminate(record.operation_id))
                }
                #[cfg(not(target_arch = "wasm32"))]
                Err(Error::NativeStream { source: StreamError::Unavailable, .. }) =>
                    Err(Error::Indeterminate(record.operation_id)),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        }
    }
    pub(crate) async fn admit_with_io(
        &self, admission: WorkflowAdmission, io: Option<&PreparationIo>,
    ) -> Result<WorkflowAdmission> {
        admission.validate(io)?;
        if let Some(owner) = &self.owner {
            if &admission != owner.workflow_admission() {
                return Err(Error::Unauthorized("workflow differs from retained task admission".into()));
            }
            let mut borrowed = io.map(PreparationIo::history_io);
            let mut work = borrowed.as_mut()
                .map(|work| work as &mut dyn crate::store::history::HistoryIo);
            owner.verify(false, &mut work).await?;
        }
        if let Some(existing) = self.read_admission(io).await? {
            return if existing == admission { Ok(existing) } else {
                Err(Error::Conflict("workflow admission identity is already bound".into()))
            };
        }
        if self.has_transition_with_io(io).await? {
            return Err(Error::Conflict("workflow transitions exist without admission".into()));
        }
        let _reference;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let reference = self.stage_payload(&admission, None, io).await?;
        _reference = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| Error::Invalid("workflow admission reference custody underflow".into()))?))
            .transpose()?;
        {
            let _pinned_bytes;
            let pinned_bytes = self.read_bound_content(&reference, io).await?;
            _pinned_bytes = external_read_hold(&pinned_bytes, io)?;
            let _pinned;
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let pinned: WorkflowAdmission = match io {
                Some(io) => io.decode_preparation::<WorkflowAdmission>(&pinned_bytes)?.into_retained(),
                None => crate::contract::json_from_slice(&pinned_bytes)
                    .map_err(|error| Error::Storage(format!("workflow admission is invalid: {error}")))?,
            };
            _pinned = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| Error::Invalid("workflow pinned admission custody underflow".into()))?))
                .transpose()?;
            pinned.validate(io)?;
            if pinned != admission {
                return Err(Error::Conflict("staged workflow admission disagrees with its identity".into()));
            }
        }
        let _path;
        let path = crate::contract::preparation_format(
            format_args!("harness/v2/workflow-admissions/{}", self.id()), io)?;
        _path = io.map(|io| io.guard_owned(path.capacity() as u64)).transpose()?;
        let appended = self.append_reference(&path, "admission", 0, admission.operation_id,
            &reference, io).await;
        if self.owner.is_some() {
            if appended? { return Ok(admission); }
            return match self.read_admission(io).await? {
                Some(existing) if existing == admission => Ok(existing),
                Some(_) => Err(Error::Conflict("workflow admission identity is already bound".into())),
                None => Err(Error::Conflict("workflow admission ownership changed".into())),
            };
        }
        match appended {
            Ok(true) => Ok(admission),
            Ok(false) | Err(Error::Conflict(_)) =>
                match self.read_admission(io).await? {
                    Some(existing) if existing == admission => Ok(existing),
                    Some(_) => Err(Error::Conflict("workflow admission identity is already bound".into())),
                    None => Err(Error::Indeterminate(admission.operation_id)),
                },
            Err(Error::Indeterminate(_)) => match self.read_admission(io).await {
                Ok(Some(existing)) if existing == admission => Ok(existing),
                Ok(Some(_)) => Err(Error::Conflict("workflow admission identity is already bound".into())),
                Ok(None) | Err(Error::Storage(_)) => Err(Error::Indeterminate(admission.operation_id)),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        }
    }
}

impl<P, A, O> WorkflowJournal for FilesystemWorkflowJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn original_machine_identity(&self) -> Option<&crate::workflow::MachineIdentity> {
        self.owner.as_ref().map(|owner| &owner.workflow_admission().initial.machine)
    }

    fn task_binding(&self) -> Option<(crate::TaskId, &crate::scheduler::LeaseFence)> {
        self.owner.as_ref().map(|owner| owner.task_binding())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_work(&self) -> bool { true }

    #[cfg(not(target_arch = "wasm32"))]
    fn admission_with_native_work<'a>(
        &'a self, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<WorkflowAdmission>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            if let Some(owner) = &self.owner { self.verify_observation_owner(owner, Some(&io)).await?; }
            let admission = self.read_admission(Some(&io)).await?;
            if let Some(owner) = &self.owner { self.verify_observation_owner(owner, Some(&io)).await?; }
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((admission, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn replay_with_native_work<'a>(
        &'a self, after: u64, maximum: u32,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<WorkflowRecord>, acyclic_fs::NativeAllocation)>> {
        if let Some(owner) = &self.owner { owner.require_native_workflow_validation()?; }
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            if let Some(owner) = &self.owner { self.verify_observation_owner(owner, Some(&io)).await?; }
            let records = self.replay_records(after, maximum, true, true, Some(&io)).await?;
            if let Some(owner) = &self.owner { self.verify_observation_owner(owner, Some(&io)).await?; }
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((records, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn commit_with_native_work<'a>(
        &'a self, expected_revision: u64, idempotency_key: IdempotencyKey,
        record: WorkflowRecord, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (WorkflowCommitOutcome, acyclic_fs::NativeAllocation)>> {
        if let Some(owner) = &self.owner { owner.require_native_workflow_validation()?; }
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let outcome = self.commit_with_io(expected_revision, idempotency_key, record, Some(&io)).await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((outcome, custody))
        })
    }

    fn admit<'a>(
        &'a self, admission: WorkflowAdmission,
    ) -> BoxFuture<'a, Result<WorkflowAdmission>> {
        let span = obs_span!("acyclic.harness.workflow.admit");
        traced(span, self.admit_with_io(admission, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn admit_with_native_work<'a>(
        &'a self, admission: WorkflowAdmission,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (WorkflowAdmission, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let admission = self.admit_with_io(admission, Some(&io)).await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((admission, custody))
        })
    }

    fn admission(&self) -> BoxFuture<'_, Result<Option<WorkflowAdmission>>> {
        Box::pin(self.read_admission(None))
    }

    fn replay(&self, after: u64, maximum: u32) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>> {
        let span = obs_span!("acyclic.harness.workflow.replay", rev = after);
        traced(span, self.replay_records(after, maximum, true, true, None))
    }

    fn commit<'a>(
        &'a self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: WorkflowRecord,
    ) -> BoxFuture<'a, Result<WorkflowCommitOutcome>> {
        let span = obs_span!("acyclic.harness.workflow.commit", rev = expected_revision);
        traced(span, self.commit_with_io(expected_revision, idempotency_key, record, None))
    }
}
