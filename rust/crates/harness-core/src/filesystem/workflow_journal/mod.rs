//! Ref-only Stream journal for pinned resumable workflow transitions.

use super::interaction_host::interaction_boundary as observation_boundary;
use super::{FilesystemHost, InternalContentClass, PreparationIo};
use crate::obs::{obs_span, traced};
use crate::store::history::HistoryIo;
use crate::{
    Error, IdempotencyKey, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
    durable_host::TaskJournalOwner,
    workflow::{
        MachineStatus, WORKFLOW_REPLAY_PAGE_RECORDS, WorkflowAdmission, WorkflowJournal,
        WorkflowRecord, validate_workflow_next,
    },
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, OwnedBytes, StreamError, StreamProvider,
};
use std::{collections::BTreeSet, sync::Arc};

mod admission;
mod observation;
mod publication;
mod replay;

fn external_read_hold<'a>(
    bytes: &OwnedBytes,
    io: Option<&'a PreparationIo>,
) -> Result<Option<super::DecodeReservation<'a>>> {
    let Some(io) = io else {
        return Ok(None);
    };
    if io.original_native_work().is_some() {
        return Ok(None);
    }
    let retained = bytes.retained_capacity().ok_or_else(|| {
        observation_boundary(
            Some(io),
            crate::wire::ErrorCode::Unsupported,
            "workflow byte backing capacity is unavailable",
        )
    })?;
    io.guard_owned(retained as u64).map(Some)
}

struct WorkflowIdentities<'a> {
    reserved: BTreeSet<crate::OperationId>,
    keys: BTreeSet<IdempotencyKey>,
    hold: Option<super::DecodeReservation<'a>>,
}

impl WorkflowIdentities<'_> {
    fn advance(&mut self, record: &WorkflowRecord) -> Result<()> {
        let Some(hold) = &mut self.hold else {
            self.reserved.extend(record.operation_ids());
            self.keys.insert(record.idempotency_key.clone());
            return Ok(());
        };
        let io = hold.io();
        for identity in record.operation_ids() {
            let bytes = std::mem::size_of::<crate::OperationId>() as u64;
            let work = acyclic_fs::WorkCounters {
                bytes_copied: bytes,
                items_examined: 1,
                ..Default::default()
            };
            io.check_cancellation()?;
            io.preflight(work)?;
            io.retain_owned(bytes)?;
            hold.adopt(bytes)?;
            io.allocation_attempt(bytes)?;
            io.charge(work)?;
            self.reserved.insert(identity);
        }
        let copied = record.idempotency_key.0.len() as u64;
        let bytes = copied
            .checked_add(std::mem::size_of::<IdempotencyKey>() as u64)
            .ok_or_else(|| Error::Invalid("workflow identity ownership overflow".into()))?;
        let work = acyclic_fs::WorkCounters {
            bytes_copied: copied,
            items_examined: copied + 1,
            ..Default::default()
        };
        io.preflight(work)?;
        io.retain_owned(bytes)?;
        hold.adopt(bytes)?;
        io.allocation_attempt(bytes)?;
        io.charge(work)?;
        self.keys.insert(record.idempotency_key.clone());
        Ok(())
    }
}

/// One workflow's atomic transition ledger; Streams never carry state or command bodies.
pub struct FilesystemWorkflowJournal<P, A, O> {
    stream: std::sync::Arc<P>,
    host: Arc<FilesystemHost<A, O>>,
    path: String,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    maximum_payload_bytes: u64,
    pub(super) owner: Option<Arc<TaskJournalOwner<P>>>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

/// A private original-owner observation, usable only with an atomic workflow tail comparison.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct WorkflowReleaseWitness<'a> {
    binding: (crate::TaskId, &'a crate::scheduler::LeaseFence),
    path: &'a str,
    tail: u64,
}

#[cfg(not(target_arch = "wasm32"))]
impl WorkflowReleaseWitness<'_> {
    pub(crate) fn condition(
        &self,
    ) -> ((crate::TaskId, &crate::scheduler::LeaseFence), (&str, u64)) {
        (self.binding, (self.path, self.tail))
    }
}

impl<P, A, O> FilesystemWorkflowJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn require_execution_owner(
        &self,
        execution_owner: &TaskJournalOwner<P>,
        volume: &VolumeRef,
        authority: &crate::core::Authority,
    ) -> Result<()> {
        let owner = self.owner.as_ref().ok_or_else(|| {
            Error::Unsupported("workflow release requires its original task owner".into())
        })?;
        if owner.task_binding() != execution_owner.task_binding()
            || owner.workflow_admission() != execution_owner.workflow_admission()
            || &self.volume != volume
            || self.verifier.audience() != authority
        {
            return Err(Error::Unauthorized(
                "workflow and execution original owners differ".into(),
            ));
        }
        Ok(())
    }

    /// Binds one task workflow to its exact worker lease and original IO.
    pub fn for_task(
        owner: Arc<TaskJournalOwner<P>>,
        host: Arc<FilesystemHost<A, O>>,
        binding: super::FilesystemJournalBinding<'_>,
    ) -> Result<Arc<Self>> {
        let super::FilesystemJournalBinding {
            verifier,
            maximum_bytes,
            io,
            ..
        } = binding;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|history| history as &mut dyn crate::store::history::HistoryIo);
        owner.validate_storage(verifier, maximum_bytes, &mut work)?;
        let (_identity, identity) = {
            let identity = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(
                    format_args!("{}", owner.workflow_admission().operation_id),
                    work,
                )
            })?;
            let _identity = io
                .map(|io| io.guard_owned(identity.capacity() as u64))
                .transpose()?;
            (_identity, identity)
        };
        let mut journal = Self::new(owner.stream(), host, &identity, binding)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work)
            && !owner.native_shared_work_matches(work)
        {
            return Err(observation_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "workflow owner belongs to another native ledger",
            ));
        }
        Arc::get_mut(&mut journal)
            .ok_or_else(|| {
                observation_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "new workflow journal has aliases",
                )
            })?
            .owner = Some(owner);
        Ok(journal)
    }

    /// Binds a workflow identity and its private-volume grants using original IO.
    pub fn new(
        stream: Arc<P>,
        host: Arc<FilesystemHost<A, O>>,
        workflow_id: &str,
        binding: super::FilesystemJournalBinding<'_>,
    ) -> Result<Arc<Self>> {
        let super::FilesystemJournalBinding {
            volume,
            verifier,
            scope,
            maximum_bytes: maximum_payload_bytes,
            io,
        } = binding;
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
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
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|history| history as &mut dyn crate::store::history::HistoryIo);
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            let maximum = io
                .map(|io| io.remaining().map(|remaining| remaining.bytes_encoded))
                .transpose()?
                .unwrap_or(u64::MAX);
            ContentGrant::authorize_volume(verifier, scope, volume, operation, maximum, &mut work)?;
        }
        let volume = volume.copy(&mut work)?;
        let verifier = verifier.copy(&mut work)?;
        let scope = scope.copy(&mut work)?;
        let (path_hold, path) = {
            let path = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(
                    format_args!("harness/v2/workflows/{workflow_id}"),
                    work,
                )
            })?;
            let path_hold = io
                .map(|io| io.guard_owned(path.capacity() as u64))
                .transpose()?;
            (path_hold, path)
        };
        let (stream_hold, validated) = Self::observation_stream(&stream, &path, io)?;
        drop(validated);
        drop(stream_hold);
        #[cfg(not(target_arch = "wasm32"))]
        let native_metadata = if let Some(io) = io.filter(|io| io.original_native_work().is_some())
        {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
            Some(io.take_native_custody(io.owned_since(before)?)?)
        } else {
            None
        };
        if let Some(held) = path_hold {
            held.into_retained()?;
        }
        Ok(Arc::new(Self {
            stream,
            host,
            path,
            volume,
            verifier,
            scope,
            maximum_payload_bytes,
            owner: None,
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata: native_metadata,
        }))
    }

    fn id(&self) -> &str {
        self.path.rsplit('/').next().unwrap_or(&self.path)
    }
}

impl<P, A, O> WorkflowJournal for FilesystemWorkflowJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn original_machine_identity(&self) -> Option<&crate::workflow::MachineIdentity> {
        self.owner
            .as_ref()
            .map(|owner| &owner.workflow_admission().initial.machine)
    }

    fn task_binding(&self) -> Option<(crate::TaskId, &crate::scheduler::LeaseFence)> {
        self.owner.as_ref().map(|owner| owner.task_binding())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_work(&self) -> bool {
        true
    }

    fn admission<'a>(
        &'a self,
        io: Option<&'a PreparationIo>,
    ) -> Result<
        crate::store::history::HistoryContentFuture<
            'a,
            (Option<WorkflowAdmission>, crate::NativeCustody),
        >,
    > {
        use crate::store::history::HistoryContentFuture;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                observation_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "native admission requires original work",
                )
            })?;
            return crate::NativeFutureBundle::new(work, || async move {
                let io = PreparationIo::from_native_work(&work.recheck_admitted())?;
                if let Some(owner) = &self.owner {
                    self.verify_observation_owner(owner, true, Some(&io))
                        .await?;
                }
                let admission = self.read_admission(Some(&io)).await?;
                if let Some(owner) = &self.owner {
                    self.verify_observation_owner(owner, true, Some(&io))
                        .await?;
                }
                let retained = io.owned_bytes()?;
                Ok((admission, Some(io.into_native_custody(retained)?)))
            })
            .map(HistoryContentFuture::Native);
        }
        let _ = io;
        Ok(HistoryContentFuture::Ordinary {
            future: Box::pin(async move { Ok((self.read_admission(None).await?, None)) }),
            frame_bytes: 0,
        })
    }

    fn replay<'a>(
        &'a self,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<
        crate::store::history::HistoryContentFuture<
            'a,
            (Vec<WorkflowRecord>, crate::NativeCustody),
        >,
    > {
        use crate::store::history::HistoryContentFuture;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                observation_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "native replay requires original work",
                )
            })?;
            if let Some(owner) = &self.owner {
                owner.require_native_workflow_validation()?;
            }
            return crate::NativeFutureBundle::new(work, || async move {
                let io = PreparationIo::from_native_work(&work.recheck_admitted())?;
                if let Some(owner) = &self.owner {
                    self.verify_observation_owner(owner, true, Some(&io))
                        .await?;
                }
                let records = self
                    .replay_records(after, maximum, true, true, Some(&io))
                    .await?;
                if let Some(owner) = &self.owner {
                    self.verify_observation_owner(owner, true, Some(&io))
                        .await?;
                }
                let retained = io.owned_bytes()?;
                Ok((records, Some(io.into_native_custody(retained)?)))
            })
            .map(HistoryContentFuture::Native);
        }
        let _ = io;
        let span = obs_span!("acyclic.harness.workflow.replay", rev = after);
        Ok(HistoryContentFuture::Ordinary {
            future: traced(span, async move {
                Ok((
                    self.replay_records(after, maximum, true, true, None)
                        .await?,
                    None,
                ))
            }),
            frame_bytes: 0,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn commit_with_native_work<'a>(
        &'a self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: &'a WorkflowRecord,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (&'a WorkflowRecord, acyclic_fs::NativeAllocation)>>
    {
        if let Some(owner) = &self.owner {
            owner.require_native_workflow_validation()?;
        }
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.commit_borrowed_with_io(expected_revision, idempotency_key, record, Some(&io))
                .await?;
            let custody = io.into_native_custody(0)?;
            Ok((record, custody))
        })
    }

    fn admit<'a>(
        &'a self,
        admission: WorkflowAdmission,
    ) -> BoxFuture<'a, Result<WorkflowAdmission>> {
        let span = obs_span!("acyclic.harness.workflow.admit");
        traced(span, self.admit_with_io(admission, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn admit_with_native_work<'a>(
        &'a self,
        admission: WorkflowAdmission,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (WorkflowAdmission, acyclic_fs::NativeAllocation)>>
    {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let admission = self.admit_with_io(admission, Some(&io)).await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((admission, custody))
        })
    }

    fn commit<'a>(
        &'a self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: WorkflowRecord,
    ) -> BoxFuture<'a, Result<WorkflowRecord>> {
        let span = obs_span!("acyclic.harness.workflow.commit", rev = expected_revision);
        traced(
            span,
            self.commit_with_io(expected_revision, idempotency_key, record, None),
        )
    }
}
