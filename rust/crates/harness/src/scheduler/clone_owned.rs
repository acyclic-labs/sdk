//! Physical scheduler field copies share the original component-copy quota.
//! Capacity inventories describe live DTO fields, never authorize a clone.
use super::*;
use crate::store::history::HistoryIo;
use crate::wire_codec::CodecQuota;
use std::mem::size_of;

#[derive(Default)]
struct Inventory { owned: u64 }

impl Inventory {
    fn add(&mut self, bytes: u64) -> Result<()> {
        self.owned = self.owned.checked_add(bytes).ok_or_else(overflow)?;
        Ok(())
    }
    fn text(&mut self, text: &String) -> Result<()> { self.add(text.capacity() as u64) }
    fn slots<T>(&mut self, count: usize) -> Result<()> {
        self.add(count.checked_mul(size_of::<T>()).ok_or_else(overflow)? as u64)
    }
    fn value(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::String(text) => self.text(text)?,
            Value::Array(values) => {
                self.slots::<Value>(values.capacity())?;
                for value in values { self.value(value)?; }
            }
            Value::Object(values) => {
                self.slots::<(String, Value)>(values.len())?;
                for (key, value) in values { self.text(key)?; self.value(value)?; }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
        Ok(())
    }
    fn entrypoint(&mut self, entrypoint: &EntrypointRef) -> Result<()> {
        self.text(&entrypoint.name)?;
        self.text(&entrypoint.version)?;
        self.value(&entrypoint.result_schema)
    }
    fn resources(&mut self, resources: &BTreeMap<String, u64>) -> Result<()> {
        self.slots::<(String, u64)>(resources.len())?;
        for key in resources.keys() { self.text(key)?; }
        Ok(())
    }
    fn fence(&mut self, fence: &LeaseFence) -> Result<()> {
        self.text(&fence.reservation_id)?;
        self.text(&fence.placement)
    }
    fn reservation(&mut self, reservation: &Reservation) -> Result<()> {
        self.text(&reservation.id)?;
        self.text(&reservation.placement)?;
        self.resources(&reservation.admitted.0)
    }
    fn file(&mut self, file: &FileRef) -> Result<()> { self.add(file.logical_owned_bytes()?) }
    fn checkpoint(&mut self, checkpoint: &CheckpointRef) -> Result<()> {
        self.add(checkpoint.logical_owned_bytes()?)
    }
    fn spec(&mut self, spec: &OperationSpec) -> Result<()> {
        if let Some(parent) = &spec.parent { self.text(&parent.slot)?; }
        self.text(&spec.owner.authority().id)?;
        self.add(spec.owner_scope.logical_owned_bytes()?)?;
        self.entrypoint(&spec.entrypoint)?;
        self.slots::<OperationId>(spec.dependencies.len())?;
        self.resources(&spec.resources.0)?;
        self.slots::<(String, String)>(spec.placement.len())?;
        for (key, value) in &spec.placement { self.text(key)?; self.text(value)?; }
        if let Orchestration::Reduce { reducer } = &spec.orchestration { self.entrypoint(reducer)?; }
        self.file(&spec.state)
    }
    fn outcome(&mut self, outcome: &Outcome<FileRef>) -> Result<()> {
        match outcome {
            Outcome::Succeeded(file) => self.file(file),
            Outcome::Failed { message } => self.text(message),
            Outcome::Cancelled | Outcome::Indeterminate { .. } => Ok(()),
        }
    }
    fn workflow(&mut self, workflow: &WorkflowSuspension) -> Result<()> {
        if let Some(input) = &workflow.input { self.file(input)?; }
        Ok(())
    }
    fn state(&mut self, state: &OperationState) -> Result<()> {
        self.spec(&state.spec)?;
        if let Some(reservation) = &state.reservation { self.reservation(reservation)?; }
        if let Some(checkpoint) = &state.checkpoint { self.checkpoint(checkpoint)?; }
        if let Some(workflow) = &state.workflow { self.workflow(workflow)?; }
        if let Some(outcome) = &state.outcome { self.outcome(outcome)?; }
        Ok(())
    }
    fn event(&mut self, event: &SchedulerEvent) -> Result<()> {
        match event {
            SchedulerEvent::SessionConfigured { .. } | SchedulerEvent::WaitingForCapacity { .. }
            | SchedulerEvent::CancellationRequested { .. } => Ok(()),
            SchedulerEvent::Declared { spec } => { self.slots::<OperationSpec>(1)?; self.spec(spec) }
            SchedulerEvent::Admitted { reservation, .. } | SchedulerEvent::PartiallyAdmitted { reservation, .. } => self.reservation(reservation),
            SchedulerEvent::Rejected { reason, .. } => self.text(reason),
            SchedulerEvent::ModelDispatchClaimed { fence, .. } | SchedulerEvent::Started { fence, .. }
            | SchedulerEvent::WaitingForChildren { fence, .. } | SchedulerEvent::WorkflowSuspended { fence, .. }
            | SchedulerEvent::ReconciliationResumed { fence, .. } | SchedulerEvent::LeaseReleased { fence, .. } => self.fence(fence),
            SchedulerEvent::Checkpointed { checkpoint, fence, .. } => { self.checkpoint(checkpoint)?; self.fence(fence) }
            SchedulerEvent::WorkflowResumed { input, .. } => self.file(input),
            SchedulerEvent::Completed { outcome, fence, .. } => {
                self.outcome(outcome)?;
                if let Some(fence) = fence { self.fence(fence)?; }
                Ok(())
            }
            SchedulerEvent::Orchestrated { outcome, cancel, reducer, .. } => {
                self.outcome(outcome)?;
                self.slots::<OperationId>(cancel.capacity())?;
                if let Some(reducer) = reducer { self.entrypoint(reducer)?; }
                Ok(())
            }
        }
    }
    fn decision(&mut self, decision: &OrchestrationDecision) -> Result<()> {
        match decision {
            OrchestrationDecision::Wait => {}
            OrchestrationDecision::Complete { outcome, cancel } => {
                self.outcome(outcome)?;
                self.slots::<OperationId>(cancel.capacity())?;
            }
            OrchestrationDecision::Assemble { values, cancel, .. } => {
                self.slots::<(String, FileRef)>(values.capacity())?;
                for (slot, file) in values { self.text(slot)?; self.file(file)?; }
                self.slots::<OperationId>(cancel.capacity())?;
            }
            OrchestrationDecision::Reduce { reducer, values } => {
                self.entrypoint(reducer)?;
                self.slots::<(String, FileRef)>(values.capacity())?;
                for (slot, file) in values { self.text(slot)?; self.file(file)?; }
            }
        }
        Ok(())
    }
}

fn overflow() -> Error {
    diagnostic(&None, crate::wire::ErrorCode::Invalid, "scheduler clone ownership overflows")
}

fn inventory<T>(value: &T, visit: impl FnOnce(&mut Inventory, &T) -> Result<()>) -> Result<u64> {
    let mut inventory = Inventory::default();
    visit(&mut inventory, value)?;
    Ok(inventory.owned)
}

fn clone_dto<T: Clone>(
    value: &T, work: &mut Option<&mut dyn HistoryIo>,
    copy: impl FnOnce(&T, &mut CodecQuota<'_, '_>) -> Result<T>,
) -> Result<(T, u64)> {
    if work.is_none() { return Ok((value.clone(), 0)); }
    // The quota outlives every temporary field and partial DTO. Only completed
    // physical copies transfer their actual retained ownership to the caller.
    let mut quota = CodecQuota::new(work, 0)?;
    let output = copy(value, &mut quota)?;
    let owned = quota.retained();
    quota.transfer();
    Ok((output, owned))
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
    let owner_scope = source.owner_scope.clone_with_work(quota.work())?;
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
                clone_dto(self, work, $copy).map(|(value, _)| value)
            }
            pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
                inventory(self, |inventory, value| inventory.$visit(value))
            }
        }
    };
}

closed_clone!(SchedulerEvent, event, copy_event);
closed_clone!(OperationSpec, spec, copy_spec);
closed_clone!(EntrypointRef, entrypoint, copy_entrypoint);
closed_clone!(Reservation, reservation, copy_reservation);
closed_clone!(WorkflowSuspension, workflow, copy_workflow);
closed_clone!(Outcome<FileRef>, outcome, copy_outcome);
closed_clone!(LeaseFence, fence, copy_fence);

macro_rules! resource_clone {
    ($kind:ty) => {
        impl $kind {
            pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
                clone_dto(self, work, |source, quota| copy_resources(&source.0, quota).map(Self))
                    .map(|(value, _)| value)
            }
            pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
                inventory(self, |inventory, value| inventory.resources(&value.0))
            }
        }
    };
}
resource_clone!(ResourceRequest);
resource_clone!(ResourceSnapshot);

impl OrchestrationDecision {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        inventory(self, Inventory::decision)
    }
}

pub(super) fn json_value_owned_bytes(value: &Value) -> Result<u64> {
    inventory(value, Inventory::value)
}

pub(super) fn clone_file_with_work(file: &FileRef, work: &mut Option<&mut dyn HistoryIo>) -> Result<FileRef> {
    clone_dto(file, work, crate::core::copy_file).map(|(value, _)| value)
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
        let mut inventory = Inventory::default();
        match self {
            Self::Declared { spec } => inventory.spec(spec)?,
            Self::Admitted { reservation, .. } | Self::PartiallyAdmitted { reservation, .. } => inventory.reservation(reservation)?,
            Self::Rejected { reason, .. } => inventory.text(reason)?,
            Self::Checkpointed { checkpoint, .. } => inventory.checkpoint(checkpoint)?,
            Self::WorkflowResumed { input, .. } => inventory.file(input)?,
            Self::Completed { outcome, .. } | Self::Orchestrated { outcome, .. } => inventory.outcome(outcome)?,
            _ => {}
        }
        Ok(inventory.owned)
    }
}

impl OperationState {
    /// Returns the actual physical field-copy claim, excluding the inline Self.
    pub(crate) fn clone_owned_with_work(
        &self, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Self, u64)> {
        clone_dto(self, work, copy_state)
    }

    pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        self.clone_owned_with_work(work).map(|(state, _)| state)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        inventory(self, Inventory::state)
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
