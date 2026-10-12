//! Physical scheduler field copies share the original component-copy quota.
use super::*;
use crate::store::history::HistoryIo;
use crate::wire_codec::CodecQuota;
use std::mem::size_of;

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or_else(overflow)
}
fn slots<T>(count: usize) -> Result<u64> {
    count.checked_mul(size_of::<T>()).map(|bytes| bytes as u64).ok_or_else(overflow)
}
fn entrypoint_bytes(value: &EntrypointRef) -> Result<u64> {
    add(add(value.name.capacity() as u64, value.version.capacity() as u64)?,
        crate::contract::json_value_logical_owned_bytes(&value.result_schema)?)
}
fn resources_bytes(values: &BTreeMap<String, u64>) -> Result<u64> {
    values.keys().try_fold(slots::<(String, u64)>(values.len())?,
        |bytes, key| add(bytes, key.capacity() as u64))
}
fn fence_bytes(value: &LeaseFence) -> Result<u64> {
    add(value.reservation_id.capacity() as u64, value.placement.capacity() as u64)
}
fn reservation_bytes(value: &Reservation) -> Result<u64> {
    add(add(value.id.capacity() as u64, value.placement.capacity() as u64)?, resources_bytes(&value.admitted.0)?)
}
fn spec_bytes(value: &OperationSpec) -> Result<u64> {
    let parent = value.parent.as_ref().map_or(0, |parent| parent.slot.capacity() as u64);
    let reducer = match &value.orchestration {
        Orchestration::Reduce { reducer } => entrypoint_bytes(reducer)?, _ => 0,
    };
    let bytes = [value.owner.authority().id.capacity() as u64, value.owner_scope.logical_owned_bytes()?,
        entrypoint_bytes(&value.entrypoint)?, slots::<OperationId>(value.dependencies.len())?,
        resources_bytes(&value.resources.0)?, slots::<(String, String)>(value.placement.len())?,
        reducer, value.state.logical_owned_bytes()?].into_iter().try_fold(parent, add)?;
    value.placement.iter().try_fold(bytes, |bytes, (key, value)|
        add(add(bytes, key.capacity() as u64)?, value.capacity() as u64))
}
fn outcome_bytes(value: &Outcome<FileRef>) -> Result<u64> {
    match value {
        Outcome::Succeeded(file) => file.logical_owned_bytes(),
        Outcome::Failed { message } => Ok(message.capacity() as u64),
        Outcome::Cancelled | Outcome::Indeterminate { .. } => Ok(0),
    }
}
fn workflow_bytes(value: &WorkflowSuspension) -> Result<u64> {
    value.input.as_ref().map(FileRef::logical_owned_bytes).transpose().map(|bytes| bytes.unwrap_or(0))
}
fn state_bytes(value: &OperationState) -> Result<u64> {
    [spec_bytes(&value.spec)?, value.reservation.as_ref().map(reservation_bytes).transpose()?.unwrap_or(0),
        value.checkpoint.as_ref().map(CheckpointRef::logical_owned_bytes).transpose()?.unwrap_or(0),
        value.workflow.as_ref().map(workflow_bytes).transpose()?.unwrap_or(0),
        value.outcome.as_ref().map(outcome_bytes).transpose()?.unwrap_or(0)].into_iter().try_fold(0, add)
}
fn event_bytes(value: &SchedulerEvent) -> Result<u64> {
    match value {
        SchedulerEvent::SessionConfigured { .. } | SchedulerEvent::WaitingForCapacity { .. }
            | SchedulerEvent::CancellationRequested { .. } => Ok(0),
        SchedulerEvent::Declared { spec } => add(slots::<OperationSpec>(1)?, spec_bytes(spec)?),
        SchedulerEvent::Admitted { reservation, .. } | SchedulerEvent::PartiallyAdmitted { reservation, .. } => reservation_bytes(reservation),
        SchedulerEvent::Rejected { reason, .. } => Ok(reason.capacity() as u64),
        SchedulerEvent::ModelDispatchClaimed { fence, .. } | SchedulerEvent::Started { fence, .. }
            | SchedulerEvent::WaitingForChildren { fence, .. } | SchedulerEvent::WorkflowSuspended { fence, .. }
            | SchedulerEvent::ReconciliationResumed { fence, .. } | SchedulerEvent::LeaseReleased { fence, .. } => fence_bytes(fence),
        SchedulerEvent::Checkpointed { checkpoint, fence, .. } => add(checkpoint.logical_owned_bytes()?, fence_bytes(fence)?),
        SchedulerEvent::WorkflowResumed { input, .. } => input.logical_owned_bytes(),
        SchedulerEvent::Completed { outcome, fence, .. } => add(outcome_bytes(outcome)?,
            fence.as_ref().map(fence_bytes).transpose()?.unwrap_or(0)),
        SchedulerEvent::Orchestrated { outcome, cancel, reducer, .. } => [outcome_bytes(outcome)?,
            slots::<OperationId>(cancel.capacity())?, reducer.as_ref().map(entrypoint_bytes).transpose()?.unwrap_or(0)]
                .into_iter().try_fold(0, add),
    }
}
fn decision_bytes(value: &OrchestrationDecision) -> Result<u64> {
    let (bytes, values) = match value {
        OrchestrationDecision::Wait => return Ok(0),
        OrchestrationDecision::Complete { outcome, cancel } => return add(outcome_bytes(outcome)?, slots::<OperationId>(cancel.capacity())?),
        OrchestrationDecision::Assemble { values, cancel, .. } => (add(slots::<(String, FileRef)>(values.capacity())?, slots::<OperationId>(cancel.capacity())?)?, values),
        OrchestrationDecision::Reduce { reducer, values } => (add(entrypoint_bytes(reducer)?, slots::<(String, FileRef)>(values.capacity())?)?, values),
    };
    values.iter().try_fold(bytes, |bytes, (slot, file)|
        add(add(bytes, slot.capacity() as u64)?, file.logical_owned_bytes()?))
}

fn overflow() -> Error {
    diagnostic(&None, crate::wire::ErrorCode::Invalid, "scheduler clone ownership overflows")
}

fn clone_dto<T: Clone>(
    value: &T, work: &mut Option<&mut dyn HistoryIo>,
    copy: impl FnOnce(&T, &mut CodecQuota<'_, '_>) -> Result<T>,
) -> Result<T> {
    if work.is_none() { return Ok(value.clone()); }
    let mut quota = CodecQuota::new(work, 0)?;
    let output = copy(value, &mut quota)?;
    quota.transfer();
    Ok(output)
}

fn copy_text(source: &str, quota: &mut CodecQuota<'_, '_>) -> Result<String> {
    observe(quota.work())?;
    crate::core::copy_text(source, quota)
}

fn claim_slots<T>(count: usize, quota: &mut CodecQuota<'_, '_>) -> Result<()> {
    let bytes = count.checked_mul(size_of::<T>()).ok_or_else(|| {
        diagnostic(quota.work(), crate::wire::ErrorCode::Invalid, "scheduler collection ownership overflows")
    })? as u64;
    if let Some(io) = quota.work().as_mut() {
        io.admit_owned(bytes)?;
        io.retain_owned(bytes)?;
        quota.adopt(bytes)?;
    }
    Ok(())
}

fn copy_resources(source: &BTreeMap<String, u64>, quota: &mut CodecQuota<'_, '_>) -> Result<BTreeMap<String, u64>> {
    claim_slots::<(String, u64)>(source.len(), quota)?;
    let mut output = BTreeMap::new();
    for (key, value) in source {
        observe(quota.work())?;
        let key = copy_text(key, quota)?;
        output.insert(key, *value);
    }
    Ok(output)
}

fn copy_entrypoint(source: &EntrypointRef, quota: &mut CodecQuota<'_, '_>) -> Result<EntrypointRef> {
    let result_schema = crate::core::copy_value(&source.result_schema, quota)?;
    let name = copy_text(&source.name, quota)?;
    let version = copy_text(&source.version, quota)?;
    Ok(EntrypointRef { name, version, digest: source.digest, result_schema })
}

fn copy_fence(source: &LeaseFence, quota: &mut CodecQuota<'_, '_>) -> Result<LeaseFence> {
    observe(quota.work())?;
    let reservation_id = copy_text(&source.reservation_id, quota)?;
    let placement = copy_text(&source.placement, quota)?;
    Ok(LeaseFence { reservation_id, placement })
}

fn copy_reservation(source: &Reservation, quota: &mut CodecQuota<'_, '_>) -> Result<Reservation> {
    observe(quota.work())?;
    let id = copy_text(&source.id, quota)?;
    let placement = copy_text(&source.placement, quota)?;
    let admitted = ResourceRequest(copy_resources(&source.admitted.0, quota)?);
    Ok(Reservation { id, placement, admitted })
}

fn copy_checkpoint(source: &CheckpointRef, quota: &mut CodecQuota<'_, '_>) -> Result<CheckpointRef> {
    observe(quota.work())?;
    let reference = source.as_resource();
    let provider = crate::core::copy_provider(reference.provider(), quota)?;
    let key = crate::core::copy_copy_vector(reference.key(), quota)?;
    let version = reference.version().map(|version| copy_text(version, quota)).transpose()?;
    let reference = crate::resources::ResourceRef::new_with_work(
        reference.kind(), provider, key, version, quota.work(),
    )?;
    CheckpointRef::from_resource_with_work(reference, quota.work())
}

fn copy_spec(source: &OperationSpec, quota: &mut CodecQuota<'_, '_>) -> Result<OperationSpec> {
    observe(quota.work())?;
    let parent = source.parent.as_ref().map(|parent| {
        copy_text(&parent.slot, quota).map(|slot| ParentLink { operation_id: parent.operation_id, slot })
    }).transpose()?;
    observe(quota.work())?;
    let authority = crate::core::copy_authority(source.owner.authority(), quota)?;
    let owner = match &source.owner {
        DurableOwner::Attached { .. } => DurableOwner::Attached { authority },
        DurableOwner::Detached { .. } => DurableOwner::Detached { authority },
    };
    let owner_scope = source.owner_scope.copy(quota.work())?;
    quota.adopt(owner_scope.logical_owned_bytes()?)?;
    let entrypoint = copy_entrypoint(&source.entrypoint, quota)?;
    claim_slots::<OperationId>(source.dependencies.len(), quota)?;
    let mut dependencies = BTreeSet::new();
    for dependency in &source.dependencies { observe(quota.work())?; dependencies.insert(*dependency); }
    let resources = ResourceRequest(copy_resources(&source.resources.0, quota)?);
    claim_slots::<(String, String)>(source.placement.len(), quota)?;
    let mut placement = BTreeMap::new();
    for (key, value) in &source.placement {
        observe(quota.work())?;
        let key = copy_text(key, quota)?;
        let value = copy_text(value, quota)?;
        placement.insert(key, value);
    }
    let orchestration = match &source.orchestration {
        Orchestration::Leaf => Orchestration::Leaf,
        Orchestration::Join => Orchestration::Join,
        Orchestration::Race => Orchestration::Race,
        Orchestration::Quorum { required } => Orchestration::Quorum { required: *required },
        Orchestration::Reduce { reducer } => Orchestration::Reduce { reducer: copy_entrypoint(reducer, quota)? },
    };
    let state = crate::core::copy_file(&source.state, quota)?;
    Ok(OperationSpec { operation_id: source.operation_id, parent, owner, owner_scope,
        entrypoint, dependencies, resources, placement, orchestration, state })
}

fn copy_outcome(source: &Outcome<FileRef>, quota: &mut CodecQuota<'_, '_>) -> Result<Outcome<FileRef>> {
    observe(quota.work())?;
    Ok(match source {
        Outcome::Succeeded(file) => Outcome::Succeeded(crate::core::copy_file(file, quota)?),
        Outcome::Failed { message } => Outcome::Failed { message: copy_text(message, quota)? },
        Outcome::Cancelled => Outcome::Cancelled,
        Outcome::Indeterminate { operation_id } => Outcome::Indeterminate { operation_id: *operation_id },
    })
}

fn copy_workflow(source: &WorkflowSuspension, quota: &mut CodecQuota<'_, '_>) -> Result<WorkflowSuspension> {
    observe(quota.work())?;
    let input = source.input.as_ref().map(|input| crate::core::copy_file(input, quota)).transpose()?;
    Ok(WorkflowSuspension { revision: source.revision, waiting_command: source.waiting_command, input })
}

fn copy_state(source: &OperationState, quota: &mut CodecQuota<'_, '_>) -> Result<OperationState> {
    let spec = copy_spec(&source.spec, quota)?;
    let reservation = source.reservation.as_ref().map(|value| copy_reservation(value, quota)).transpose()?;
    let checkpoint = source.checkpoint.as_ref().map(|value| copy_checkpoint(value, quota)).transpose()?;
    let workflow = source.workflow.as_ref().map(|value| copy_workflow(value, quota)).transpose()?;
    let outcome = source.outcome.as_ref().map(|value| copy_outcome(value, quota)).transpose()?;
    Ok(OperationState { spec, phase: source.phase, reservation, checkpoint, workflow, outcome,
        cancellation_requested: source.cancellation_requested, revision: source.revision })
}

fn copy_event(source: &SchedulerEvent, quota: &mut CodecQuota<'_, '_>) -> Result<SchedulerEvent> {
    observe(quota.work())?;
    Ok(match source {
        SchedulerEvent::SessionConfigured { operation_id, limits } => SchedulerEvent::SessionConfigured { operation_id: *operation_id, limits: *limits },
        SchedulerEvent::Declared { spec } => SchedulerEvent::Declared { spec: crate::core::copy_boxed(spec.as_ref(), quota, copy_spec)? },
        SchedulerEvent::WaitingForCapacity { operation_id } => SchedulerEvent::WaitingForCapacity { operation_id: *operation_id },
        SchedulerEvent::Admitted { operation_id, reservation } => SchedulerEvent::Admitted { operation_id: *operation_id, reservation: copy_reservation(reservation, quota)? },
        SchedulerEvent::PartiallyAdmitted { operation_id, reservation } => SchedulerEvent::PartiallyAdmitted { operation_id: *operation_id, reservation: copy_reservation(reservation, quota)? },
        SchedulerEvent::Rejected { operation_id, reason } => SchedulerEvent::Rejected { operation_id: *operation_id, reason: copy_text(reason, quota)? },
        SchedulerEvent::ModelDispatchClaimed { operation_id, attempt_id, step, request_digest, fence, ceiling } => SchedulerEvent::ModelDispatchClaimed {
            operation_id: *operation_id, attempt_id: *attempt_id, step: *step, request_digest: *request_digest,
            fence: copy_fence(fence, quota)?, ceiling: *ceiling,
        },
        SchedulerEvent::Started { operation_id, fence } => SchedulerEvent::Started { operation_id: *operation_id, fence: copy_fence(fence, quota)? },
        SchedulerEvent::Checkpointed { operation_id, checkpoint, fence } => {
            let checkpoint = copy_checkpoint(checkpoint, quota)?;
            let fence = copy_fence(fence, quota)?;
            SchedulerEvent::Checkpointed { operation_id: *operation_id, checkpoint, fence }
        }
        SchedulerEvent::WaitingForChildren { operation_id, fence } => SchedulerEvent::WaitingForChildren { operation_id: *operation_id, fence: copy_fence(fence, quota)? },
        SchedulerEvent::WorkflowSuspended { operation_id, fence, workflow_revision, waiting_command } => SchedulerEvent::WorkflowSuspended {
            operation_id: *operation_id, fence: copy_fence(fence, quota)?, workflow_revision: *workflow_revision, waiting_command: *waiting_command,
        },
        SchedulerEvent::WorkflowResumed { operation_id, workflow_revision, input } => SchedulerEvent::WorkflowResumed {
            operation_id: *operation_id, workflow_revision: *workflow_revision, input: crate::core::copy_file(input, quota)?,
        },
        SchedulerEvent::ReconciliationResumed { operation_id, fence } => SchedulerEvent::ReconciliationResumed { operation_id: *operation_id, fence: copy_fence(fence, quota)? },
        SchedulerEvent::LeaseReleased { operation_id, fence } => SchedulerEvent::LeaseReleased { operation_id: *operation_id, fence: copy_fence(fence, quota)? },
        SchedulerEvent::CancellationRequested { operation_id, recursive } => SchedulerEvent::CancellationRequested { operation_id: *operation_id, recursive: *recursive },
        SchedulerEvent::Completed { operation_id, outcome, fence, execution_duration_ns } => {
            let outcome = copy_outcome(outcome, quota)?;
            let fence = fence.as_ref().map(|fence| copy_fence(fence, quota)).transpose()?;
            SchedulerEvent::Completed { operation_id: *operation_id, outcome, fence, execution_duration_ns: *execution_duration_ns }
        }
        SchedulerEvent::Orchestrated { operation_id, expected_revision, outcome, cancel, reducer, reduction_digest } => {
            let outcome = copy_outcome(outcome, quota)?;
            for _ in cancel { observe(quota.work())?; }
            let cancel = crate::core::copy_copy_vector(cancel, quota)?;
            let reducer = reducer.as_ref().map(|reducer| copy_entrypoint(reducer, quota)).transpose()?;
            SchedulerEvent::Orchestrated { operation_id: *operation_id, expected_revision: *expected_revision,
                outcome, cancel, reducer, reduction_digest: *reduction_digest }
        }
    })
}

macro_rules! closed_clone {
    ($kind:ty, $visit:ident, $copy:expr) => {
        impl $kind {
            pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
                clone_dto(self, work, $copy)
            }
            pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
                $visit(self)
            }
        }
    };
}

closed_clone!(SchedulerEvent, event_bytes, copy_event);
closed_clone!(OperationSpec, spec_bytes, copy_spec);
closed_clone!(EntrypointRef, entrypoint_bytes, copy_entrypoint);
closed_clone!(Reservation, reservation_bytes, copy_reservation);
closed_clone!(WorkflowSuspension, workflow_bytes, copy_workflow);
closed_clone!(Outcome<FileRef>, outcome_bytes, copy_outcome);
closed_clone!(LeaseFence, fence_bytes, copy_fence);

macro_rules! resource_clone {
    ($kind:ty) => {
        impl $kind {
            pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
                clone_dto(self, work, |source, quota| copy_resources(&source.0, quota).map(Self))
            }
            pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
                resources_bytes(&self.0)
            }
        }
    };
}
resource_clone!(ResourceRequest);
resource_clone!(ResourceSnapshot);

impl OrchestrationDecision {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        decision_bytes(self)
    }
}

pub(super) fn clone_file_with_work(file: &FileRef, work: &mut Option<&mut dyn HistoryIo>) -> Result<FileRef> {
    clone_dto(file, work, crate::core::copy_file)
}

impl LeaseFence {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn clone_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        io.admit_owned(size_of::<Self>() as u64)?;
        io.retain_owned(size_of::<Self>() as u64)?;
        let fence = self.clone_with_work(&mut Some(&mut io))?;
        Ok((fence, io.into_custody()))
    }
}

impl SchedulerEvent {
    /// Fields moving into the resident cache, excluding observer-only envelopes.
    pub(crate) fn retained_clone_owned_bytes(&self) -> Result<u64> {
        match self {
            Self::Declared { spec } => spec.logical_owned_bytes(),
            Self::Admitted { reservation, .. } | Self::PartiallyAdmitted { reservation, .. } => reservation.logical_owned_bytes(),
            Self::Rejected { reason, .. } => Ok(reason.capacity() as u64),
            Self::Checkpointed { checkpoint, .. } => checkpoint.logical_owned_bytes(),
            Self::WorkflowResumed { input, .. } => input.logical_owned_bytes(),
            Self::Completed { outcome, .. } | Self::Orchestrated { outcome, .. } => outcome.logical_owned_bytes(),
            _ => Ok(0),
        }
    }
}

impl OperationState {

    pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        clone_dto(self, work, copy_state)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        state_bytes(self)
    }

    pub(crate) fn work_lease_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Option<crate::distributed::WorkLease>> {
        observe(work)?;
        if !matches!(self.phase, OperationPhase::Admitted | OperationPhase::Running | OperationPhase::Reconciling) {
            return Ok(None);
        }
        let Some(reservation) = &self.reservation else { return Ok(None); };
        if work.is_none() {
            return Ok(Some(crate::distributed::WorkLease {
                operation: self.spec.clone(), reservation: reservation.clone(),
                checkpoint: self.checkpoint.clone(), operation_revision: self.revision,
            }));
        }
        let mut quota = CodecQuota::new(work, 0)?;
        // Workflow input and terminal outcome stay borrowed from the cache.
        let operation = copy_spec(&self.spec, &mut quota)?;
        let reservation = copy_reservation(reservation, &mut quota)?;
        let checkpoint = self.checkpoint.as_ref().map(|value| copy_checkpoint(value, &mut quota)).transpose()?;
        let lease = crate::distributed::WorkLease {
            operation, reservation, checkpoint, operation_revision: self.revision,
        };
        quota.transfer();
        Ok(Some(lease))
    }
}
