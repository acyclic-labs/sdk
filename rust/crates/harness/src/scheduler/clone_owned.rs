//! Borrowed inventory of the existing closed scheduler DTOs. Logical SDK
//! fields are counted; opaque tree nodes, allocator metadata and RSS are not.
use super::*;
use crate::store::history::HistoryIo;
use std::mem::size_of;

struct Inventory<'a> {
    io: Option<&'a mut dyn HistoryIo>,
    live: bool,
    owned: u64,
    copied: u64,
    allocations: u64,
}

impl Inventory<'_> {
    fn visit(&mut self) -> Result<()> {
        observe(self.io.as_deref_mut())
    }
    fn add(&mut self, owned: u64, copied: u64) -> Result<()> {
        self.owned = self.owned.checked_add(owned).ok_or_else(|| overflow(&self.io))?;
        self.copied = self.copied.checked_add(copied).ok_or_else(|| overflow(&self.io))?;
        Ok(())
    }
    fn allocation(&mut self, bytes: u64) -> Result<()> {
        if bytes != 0 {
            self.allocations = self.allocations.checked_add(1).ok_or_else(|| overflow(&self.io))?;
        }
        Ok(())
    }
    fn text(&mut self, text: &String) -> Result<()> {
        self.visit()?;
        self.allocation(text.len() as u64)?;
        self.add(if self.live { text.capacity() } else { text.len() } as u64, text.len() as u64)
    }
    fn slots<T>(&mut self, count: usize) -> Result<()> {
        let bytes = count.checked_mul(size_of::<T>()).ok_or_else(|| overflow(&self.io))? as u64;
        self.allocation(bytes)?;
        self.add(bytes, bytes)
    }
    fn logical_slots<T>(&mut self, count: usize) -> Result<()> {
        let bytes = count.checked_mul(size_of::<T>()).ok_or_else(|| overflow(&self.io))? as u64;
        self.add(bytes, bytes)
    }
    fn value(&mut self, value: &Value, depth: u32) -> Result<()> {
        self.visit()?;
        if depth > 256 {
            return Err(diagnostic(&self.io, crate::wire::ErrorCode::Invalid,
                "scheduler schema clone depth exceeds its bound"));
        }
        match value {
            Value::String(text) => self.text(text)?,
            Value::Array(values) => {
                self.slots::<Value>(if self.live { values.capacity() } else { values.len() })?;
                for value in values { self.value(value, depth + 1)?; }
            }
            Value::Object(values) => {
                self.logical_slots::<(String, Value)>(values.len())?;
                for (key, value) in values { self.text(key)?; self.value(value, depth + 1)?; }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
        Ok(())
    }
    fn entrypoint(&mut self, entrypoint: &EntrypointRef) -> Result<()> {
        self.text(&entrypoint.name)?;
        self.text(&entrypoint.version)?;
        self.value(&entrypoint.result_schema, 0)
    }
    fn resources(&mut self, resources: &BTreeMap<String, u64>) -> Result<()> {
        self.logical_slots::<(String, u64)>(resources.len())?;
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
    fn file(&mut self, file: &FileRef) -> Result<()> {
        self.visit()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let (allocations, copied) = file.clone_work_recipe()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let (allocations, copied) = {
            let volume = file.volume();
            let provider = volume.provider();
            let owner = match volume.owner() {
                crate::conversation::VolumeOwner::Project(owner) | crate::conversation::VolumeOwner::Session(owner) => owner.as_str(),
                crate::conversation::VolumeOwner::Agent(_) => "",
            };
            [provider.namespace(), provider.family(), provider.version(), volume.id(), owner,
                file.path(), file.version(), file.descriptor().media_type(), file.display_name()]
                .into_iter().try_fold((0_u64, 0_u64), |(allocations, copied), text| {
                    copied.checked_add(text.len() as u64).map(|copied| (allocations + u64::from(!text.is_empty()), copied))
                        .ok_or_else(|| overflow(&self.io))
                })?
        };
        self.allocations = self.allocations.checked_add(allocations).ok_or_else(|| overflow(&self.io))?;
        self.add(if self.live { file.logical_owned_bytes()? } else { copied }, copied)
    }
    fn checkpoint(&mut self, checkpoint: &CheckpointRef) -> Result<()> {
        self.visit()?;
        let reference = checkpoint.as_resource();
        let provider = reference.provider();
        let copied = [provider.namespace(), provider.family(), provider.version(), reference.version().unwrap_or("")]
            .into_iter().try_fold(reference.key().len() as u64, |sum, text| {
                self.allocation(text.len() as u64)?;
                sum.checked_add(text.len() as u64).ok_or_else(|| overflow(&self.io))
            })?;
        self.allocation(reference.key().len() as u64)?;
        self.add(if self.live { checkpoint.logical_owned_bytes()? } else { copied }, copied)
    }
    fn spec(&mut self, spec: &OperationSpec) -> Result<()> {
        if let Some(parent) = &spec.parent { self.text(&parent.slot)?; }
        self.text(&spec.owner.authority().id)?;
        let scope_copy = spec.owner_scope.logical_clone_owned_bytes()?;
        let mut known_scope = spec.owner_scope.id().len() as u64;
        self.allocation(known_scope)?;
        for capability in spec.owner_scope.capabilities().iter() {
            self.visit()?;
            self.allocation(capability.len() as u64)?;
            known_scope = known_scope.checked_add(size_of::<String>() as u64)
                .and_then(|bytes| bytes.checked_add(capability.len() as u64))
                .ok_or_else(|| overflow(&self.io))?;
        }
        // The immutable issuer is private. Its exact String length is the
        // residual of Scope's original closed-field recipe, not a signer clone.
        self.allocation(scope_copy.checked_sub(known_scope).ok_or_else(|| overflow(&self.io))?)?;
        self.add(if self.live { spec.owner_scope.logical_owned_bytes()? } else { scope_copy }, scope_copy)?;
        self.entrypoint(&spec.entrypoint)?;
        self.logical_slots::<OperationId>(spec.dependencies.len())?;
        for _ in &spec.dependencies { self.visit()?; }
        self.resources(&spec.resources.0)?;
        self.logical_slots::<(String, String)>(spec.placement.len())?;
        for (key, value) in &spec.placement { self.text(key)?; self.text(value)?; }
        if let Orchestration::Reduce { reducer } = &spec.orchestration { self.entrypoint(reducer)?; }
        self.file(&spec.state)
    }
    fn outcome(&mut self, outcome: &Outcome<FileRef>) -> Result<()> {
        self.visit()?;
        match outcome {
            Outcome::Succeeded(file) => self.file(file),
            Outcome::Failed { message } => self.text(message),
            Outcome::Cancelled | Outcome::Indeterminate { .. } => Ok(())
        }
    }
    fn workflow(&mut self, workflow: &WorkflowSuspension) -> Result<()> {
        self.visit()?;
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
        self.visit()?;
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
                self.slots::<OperationId>(if self.live { cancel.capacity() } else { cancel.len() })?;
                for _ in cancel { self.visit()?; }
                if let Some(reducer) = reducer { self.entrypoint(reducer)?; }
                Ok(())
            }
        }
    }
    fn decision(&mut self, decision: &OrchestrationDecision) -> Result<()> {
        self.visit()?;
        match decision {
            OrchestrationDecision::Wait => {}
            OrchestrationDecision::Complete { outcome, cancel } => {
                self.outcome(outcome)?;
                self.slots::<OperationId>(if self.live { cancel.capacity() } else { cancel.len() })?;
            }
            OrchestrationDecision::Assemble { values, cancel, .. } => {
                self.slots::<(String, FileRef)>(if self.live { values.capacity() } else { values.len() })?;
                for (slot, file) in values { self.text(slot)?; self.file(file)?; }
                self.slots::<OperationId>(if self.live { cancel.capacity() } else { cancel.len() })?;
            }
            OrchestrationDecision::Reduce { reducer, values } => {
                self.entrypoint(reducer)?;
                self.slots::<(String, FileRef)>(if self.live { values.capacity() } else { values.len() })?;
                for (slot, file) in values { self.text(slot)?; self.file(file)?; }
            }
        }
        Ok(())
    }
}

fn overflow(work: &Option<&mut dyn HistoryIo>) -> Error {
    diagnostic(work, crate::wire::ErrorCode::Invalid, "scheduler clone ownership overflows")
}

fn inventory<T>(value: &T, live: bool, io: Option<&mut dyn HistoryIo>, visit: impl FnOnce(&mut Inventory<'_>, &T) -> Result<()>) -> Result<(u64, u64, u64)> {
    let mut inventory = Inventory { io, live, owned: 0, copied: 0, allocations: 0 };
    visit(&mut inventory, value)?;
    Ok((inventory.owned, inventory.copied, inventory.allocations))
}

fn clone_dto<T: Clone>(value: &T, work: &mut Option<&mut dyn HistoryIo>, visit: impl Copy + Fn(&mut Inventory<'_>, &T) -> Result<()>) -> Result<T> {
    let Some(io) = work.as_deref_mut() else { return Ok(value.clone()); };
    let (owned, copied, allocations) = inventory(value, false, Some(&mut *io), visit)?;
    io.admit_owned(owned)?;
    io.admit_copy(copied)?;
    // Count each known SDK String/Vec or logical collection allocation attempt
    // before Clone enters its allocator. No opaque BTree node size is claimed.
    for _ in 0..allocations { io.admit_allocation(owned)?; }
    io.copy_bytes(copied)?;
    io.retain_owned(owned)?;
    Ok(value.clone())
}

macro_rules! closed_clone {
    ($kind:ty, $visit:expr) => {
        impl $kind {
            pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
                clone_dto(self, work, $visit)
            }
            pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
                inventory(self, true, None, $visit).map(|counts| counts.0)
            }
        }
    };
}

closed_clone!(SchedulerEvent, Inventory::event);
closed_clone!(OperationSpec, Inventory::spec);
closed_clone!(OperationState, Inventory::state);
closed_clone!(EntrypointRef, Inventory::entrypoint);
closed_clone!(Reservation, Inventory::reservation);
closed_clone!(WorkflowSuspension, Inventory::workflow);
closed_clone!(Outcome<FileRef>, Inventory::outcome);
closed_clone!(ResourceRequest, |inventory: &mut Inventory<'_>, value: &ResourceRequest| inventory.resources(&value.0));
closed_clone!(ResourceSnapshot, |inventory: &mut Inventory<'_>, value: &ResourceSnapshot| inventory.resources(&value.0));

impl OrchestrationDecision {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        inventory(self, true, None, Inventory::decision).map(|counts| counts.0)
    }
}

pub(super) fn json_value_owned_bytes(value: &Value) -> Result<u64> {
    inventory(value, true, None, |inventory, value| inventory.value(value, 0)).map(|counts| counts.0)
}

pub(super) fn clone_file_with_work(file: &FileRef, work: &mut Option<&mut dyn HistoryIo>) -> Result<FileRef> {
    clone_dto(file, work, Inventory::file)
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
        let reservation_id = io.clone_text(&self.reservation_id)?;
        let placement = io.clone_text(&self.placement)?;
        let fence = Self { reservation_id, placement };
        Ok((fence, io.into_custody()))
    }

    pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        clone_dto(self, work, Inventory::fence)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        inventory(self, true, None, Inventory::fence).map(|counts| counts.0)
    }
}

impl SchedulerEvent {
    /// Fields that move into the existing scheduler cache, excluding envelopes
    /// and observer-only fences, cancellation vectors and reducer contracts.
    pub(crate) fn retained_clone_owned_bytes(&self) -> Result<u64> {
        let mut inventory = Inventory { io: None, live: true, owned: 0, copied: 0, allocations: 0 };
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
    pub(crate) fn work_lease_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Option<crate::distributed::WorkLease>> {
        observe(work.as_deref_mut())?;
        if !matches!(self.phase, OperationPhase::Admitted | OperationPhase::Running | OperationPhase::Reconciling) {
            return Ok(None);
        }
        let Some(reservation) = &self.reservation else { return Ok(None); };
        // Only fields present in WorkLease are visited and copied. Workflow
        // input and terminal outcome remain borrowed from the resident cache.
        let Some(io) = work.as_deref_mut() else {
            return Ok(Some(crate::distributed::WorkLease {
                operation: self.spec.clone(), reservation: reservation.clone(),
                checkpoint: self.checkpoint.clone(), operation_revision: self.revision,
            }));
        };
        let mut counts = Inventory { io: Some(&mut *io), live: false, owned: 0, copied: 0, allocations: 0 };
        counts.spec(&self.spec)?;
        counts.reservation(reservation)?;
        if let Some(checkpoint) = &self.checkpoint { counts.checkpoint(checkpoint)?; }
        let (owned, copied, allocations) = (counts.owned, counts.copied, counts.allocations);
        io.admit_owned(owned)?;
        io.admit_copy(copied)?;
        for _ in 0..allocations { io.admit_allocation(owned)?; }
        io.copy_bytes(copied)?;
        io.retain_owned(owned)?;
        Ok(Some(crate::distributed::WorkLease {
            operation: self.spec.clone(), reservation: reservation.clone(),
            checkpoint: self.checkpoint.clone(), operation_revision: self.revision,
        }))
    }
}
