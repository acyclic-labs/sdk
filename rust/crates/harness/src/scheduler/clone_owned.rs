//! Borrowed inventory of the existing scheduler DTO clone, not JSON encoding
//! or a measurement of opaque allocator nodes/RSS.
use super::*;
use crate::store::history::HistoryIo;
use std::mem::size_of;

struct Inventory<'a> {
    io: Option<&'a mut dyn HistoryIo>,
    live: bool,
    owned: u64,
    copied: u64,
}

impl Inventory<'_> {
    fn visit(&mut self) -> Result<()> {
        if let Some(io) = self.io.as_deref_mut() {
            io.check_cancellation()?;
            io.codec_charge(crate::contract::JsonCodecCharge::Work {bytes:0,items:1})?;
        }
        Ok(())
    }
    fn add(&mut self, owned: u64, copied: u64) -> Result<()> {
        self.owned = self.owned.checked_add(owned)
            .ok_or_else(|| Error::Invalid("scheduler clone ownership overflows".into()))?;
        self.copied = self.copied.checked_add(copied)
            .ok_or_else(|| Error::Invalid("scheduler clone copy work overflows".into()))?;
        Ok(())
    }
    fn text(&mut self, text: &String) -> Result<()> {
        self.visit()?;
        self.add(if self.live {text.capacity()} else {text.len()} as u64,text.len() as u64)
    }
    fn slots<T>(&mut self, count: usize) -> Result<()> {
        let bytes = count.checked_mul(size_of::<T>())
            .ok_or_else(|| Error::Invalid("scheduler clone slots overflow".into()))? as u64;
        self.add(bytes,bytes)
    }
    fn value(&mut self, value: &Value, depth: u32) -> Result<()> {
        self.visit()?;
        if depth > 256 { return Err(Error::Invalid("scheduler schema clone depth exceeds its bound".into())); }
        match value {
            Value::String(text) => self.add(if self.live {text.capacity()} else {text.len()} as u64,text.len() as u64)?,
            Value::Array(values) => {
                self.slots::<Value>(if self.live {values.capacity()} else {values.len()})?;
                for value in values { self.value(value,depth+1)?; }
            },
            Value::Object(values) => {
                self.slots::<(String,Value)>(values.len())?;
                for (key,value) in values { self.text(key)?; self.value(value,depth+1)?; }
            },
            Value::Null | Value::Bool(_) | Value::Number(_) => {},
        }
        Ok(())
    }
    fn entrypoint(&mut self, entrypoint: &EntrypointRef) -> Result<()> {
        self.text(&entrypoint.name)?;
        self.text(&entrypoint.version)?;
        self.value(&entrypoint.result_schema,0)
    }
    fn resources(&mut self, resources: &ResourceRequest) -> Result<()> {
        self.slots::<(String,u64)>(resources.0.len())?;
        for key in resources.0.keys() { self.text(key)?; }
        Ok(())
    }
    fn fence(&mut self, fence: &LeaseFence) -> Result<()> {
        self.text(&fence.reservation_id)?;
        self.text(&fence.placement)
    }
    fn reservation(&mut self, reservation: &Reservation) -> Result<()> {
        self.text(&reservation.id)?;
        self.text(&reservation.placement)?;
        self.resources(&reservation.admitted)
    }
    fn file(&mut self, file: &FileRef) -> Result<()> {
        self.visit()?;
        // The source's actual capacities are a pre-clone upper bound. String
        // clones copy their lengths; no namespace builder/encoding is invoked.
        let provider = file.volume().provider();
        let owner = match file.volume().owner() {
            crate::conversation::VolumeOwner::Project(owner)
            | crate::conversation::VolumeOwner::Session(owner) => owner.len() as u64,
            crate::conversation::VolumeOwner::Agent(_) => 0,
        };
        let copied = [provider.namespace(),provider.family(),provider.version(),file.volume().id(),
            file.path(),file.version(),file.display_name(),file.descriptor().media_type()]
            .into_iter().try_fold(owner,|sum,text| sum.checked_add(text.len() as u64)
                .ok_or_else(|| Error::Invalid("file clone copy work overflows".into())))?;
        self.add(if self.live {file.logical_owned_bytes()?} else {copied},copied)
    }
    fn checkpoint(&mut self, checkpoint: &CheckpointRef) -> Result<()> {
        self.visit()?;
        let reference = checkpoint.as_resource();
        let provider = reference.provider();
        let copied = [provider.namespace(),provider.family(),provider.version(),reference.version().unwrap_or("")]
            .into_iter().try_fold(reference.key().len() as u64,|sum,text| sum.checked_add(text.len() as u64)
                .ok_or_else(|| Error::Invalid("checkpoint clone copy work overflows".into())))?;
        self.add(if self.live {checkpoint.logical_owned_bytes()?} else {copied},copied)
    }
    fn spec(&mut self, spec: &OperationSpec) -> Result<()> {
        if let Some(parent) = &spec.parent { self.text(&parent.slot)?; }
        self.text(&spec.owner.authority().id)?;
        let scope_copy = spec.owner_scope.logical_clone_owned_bytes()?;
        let scope_owned = if self.live {spec.owner_scope.logical_owned_bytes()?} else {scope_copy};
        self.add(scope_owned,scope_copy)?;
        self.entrypoint(&spec.entrypoint)?;
        self.slots::<OperationId>(spec.dependencies.len())?;
        for _ in &spec.dependencies { self.visit()?; }
        self.resources(&spec.resources)?;
        self.slots::<(String,String)>(spec.placement.len())?;
        for (key,value) in &spec.placement { self.text(key)?; self.text(value)?; }
        if let Orchestration::Reduce {reducer} = &spec.orchestration { self.entrypoint(reducer)?; }
        self.file(&spec.state)
    }
    fn outcome(&mut self, outcome: &Outcome<FileRef>) -> Result<()> {
        self.visit()?;
        match outcome {
            Outcome::Succeeded(file) => self.file(file),
            Outcome::Failed {message} => self.text(message),
            Outcome::Cancelled | Outcome::Indeterminate {..} => Ok(()),
        }
    }
    fn event(&mut self, event: &SchedulerEvent) -> Result<()> {
        self.visit()?;
        match event {
            SchedulerEvent::SessionConfigured {..} | SchedulerEvent::WaitingForCapacity {..}
            | SchedulerEvent::CancellationRequested {..} => Ok(()),
            SchedulerEvent::Declared {spec} => { self.slots::<OperationSpec>(1)?; self.spec(spec) },
            SchedulerEvent::Admitted {reservation,..} | SchedulerEvent::PartiallyAdmitted {reservation,..} => self.reservation(reservation),
            SchedulerEvent::Rejected {reason,..} => self.text(reason),
            SchedulerEvent::ModelDispatchClaimed {fence,..} | SchedulerEvent::Started {fence,..}
            | SchedulerEvent::WaitingForChildren {fence,..} | SchedulerEvent::WorkflowSuspended {fence,..}
            | SchedulerEvent::ReconciliationResumed {fence,..} | SchedulerEvent::LeaseReleased {fence,..} => self.fence(fence),
            SchedulerEvent::Checkpointed {checkpoint,fence,..} => { self.checkpoint(checkpoint)?; self.fence(fence) },
            SchedulerEvent::WorkflowResumed {input,..} => self.file(input),
            SchedulerEvent::Completed {outcome,fence,..} => {
                self.outcome(outcome)?;
                if let Some(fence) = fence { self.fence(fence)?; }
                Ok(())
            },
            SchedulerEvent::Orchestrated {outcome,cancel,reducer,..} => {
                self.outcome(outcome)?;
                self.slots::<OperationId>(cancel.len())?;
                for _ in cancel { self.visit()?; }
                if let Some(reducer) = reducer { self.entrypoint(reducer)?; }
                Ok(())
            },
        }
    }
}

impl SchedulerEvent {
    pub(crate) fn clone_with_work(
        &self, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        let Some(io) = work.as_deref_mut() else { return Ok(self.clone()); };
        let (owned,copied) = {
            let mut inventory = Inventory {io:Some(&mut *io),live:false,owned:0,copied:0};
            inventory.event(self)?;
            (inventory.owned,inventory.copied)
        };
        io.admit_owned(owned)?;
        io.codec_charge(crate::contract::JsonCodecCharge::Copy {bytes:copied})?;
        io.retain_owned(owned)?;
        Ok(self.clone())
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let mut inventory = Inventory {io:None,live:true,owned:0,copied:0};
        inventory.event(self)?;
        Ok(inventory.owned)
    }

    /// Clone fields which move into the existing projection; event envelopes,
    /// fences, reducers and cancellation vectors do not remain owned.
    pub(crate) fn retained_clone_owned_bytes(&self) -> Result<u64> {
        let mut inventory = Inventory {io:None,live:true,owned:0,copied:0};
        match self {
            Self::Declared {spec} => inventory.spec(spec)?,
            Self::Admitted {reservation,..} | Self::PartiallyAdmitted {reservation,..} => inventory.reservation(reservation)?,
            Self::Rejected {reason,..} => inventory.text(reason)?,
            Self::Checkpointed {checkpoint,..} => inventory.checkpoint(checkpoint)?,
            Self::WorkflowResumed {input,..} => inventory.file(input)?,
            Self::Completed {outcome,..} | Self::Orchestrated {outcome,..} => inventory.outcome(outcome)?,
            Self::SessionConfigured {..} | Self::WaitingForCapacity {..}
            | Self::CancellationRequested {..} | Self::ModelDispatchClaimed {..}
            | Self::Started {..} | Self::WaitingForChildren {..}
            | Self::WorkflowSuspended {..} | Self::ReconciliationResumed {..}
            | Self::LeaseReleased {..} => {},
        }
        Ok(inventory.owned)
    }
}
