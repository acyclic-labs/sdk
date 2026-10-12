use super::*;

pub(super) struct InputBuffers<'a, 'w> {
    carrier: &'a mut NativeDemandCarrier<'w>,
    bytes: u64,
    allocation: Option<crate::NativeAllocation>,
    pending: Option<crate::materializer::NativeWorkReservation>,
}

impl<'a, 'w> InputBuffers<'a, 'w> {
    pub(super) fn new(carrier: &'a mut NativeDemandCarrier<'w>) -> Result<Self, MountSourceError> {
        let allocation = carrier.original_native_work().map(|work|
            work.claim_allocation(0, 0, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)).transpose()?;
        carrier.with_work(|work| work.check()).map_err(super::lazy::mount_demand_error)?;
        Ok(Self { carrier, bytes: 0, allocation, pending: None })
    }

    pub(super) fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, MountSourceError> {
        let requested = count.checked_mul(std::mem::size_of::<T>()).and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let vector = if let Some(work) = self.carrier.original_native_work() {
            let mut allocation = work.claim_allocation(requested, u64::from(requested != 0), crate::WorkCounters::UNCHARGED)
                .map_err(MountSourceError::NativeWork)?;
            self.allocation.as_mut().expect("native inputs").absorb(&mut allocation).map_err(MountSourceError::NativeWork)?;
            let mut vector = Vec::new();
            vector.try_reserve_exact(count).map_err(|error| MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::AllocationFailed(error)))?;
            vector
        } else {
            let vector = self.carrier.with_work(|work| super::lazy::mount_vec::<T>(count, work))?;
            self.bytes = self.bytes.checked_add(requested).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            vector
        };
        let actual = vector.capacity().checked_mul(std::mem::size_of::<T>()).and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let extra = actual.checked_sub(requested).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        if extra != 0 {
            let result = if let Some(work) = self.carrier.original_native_work() {
                let (mut allocation, result) = work.record_completed_allocation(extra, None);
                self.allocation.as_mut().expect("native inputs").absorb(&mut allocation).map_err(MountSourceError::NativeWork)?;
                result.map_err(MountSourceError::NativeWork)
            } else {
                self.bytes = self.bytes.checked_add(extra).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                self.carrier.with_work(|work| work.record_adoption(extra, &work.capture_custody()))
                    .map_err(super::lazy::mount_demand_error)
            };
            if let Err(error) = result { drop(vector); return Err(error); }
        }
        Ok(vector)
    }

    pub(super) fn admit(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        if let Some(work) = self.carrier.original_native_work() {
            self.pending = Some(work.reserve_work(delta).map_err(MountSourceError::NativeWork)?);
            Ok(())
        } else {
            self.carrier.with_work(|work| work.admit(delta)).map_err(super::lazy::mount_demand_error)
        }
    }

    pub(super) fn record(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        if let Some(work) = self.carrier.original_native_work() {
            match &mut self.pending {
                Some(pending) => pending.record_completed(delta),
                None => work.record_admitted(delta),
            }.map_err(MountSourceError::NativeWork)
        } else {
            self.carrier.with_work(|work| work.record(delta)).map_err(super::lazy::mount_demand_error)
        }
    }

    pub(super) fn charge(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        match self.carrier.original_native_work() {
            Some(work) => work.charge(delta).map_err(MountSourceError::NativeWork),
            None => self.carrier.with_work(|work| { work.admit(delta)?; work.record(delta) })
                .map_err(super::lazy::mount_demand_error),
        }
    }

    pub(super) fn items(&mut self, count: usize) -> Result<(), MountSourceError> {
        self.charge(crate::WorkCounters { items_examined: count as u64, ..crate::WorkCounters::UNCHARGED })
    }

    pub(super) fn release(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        if let Some(allocation) = &mut self.allocation {
            drop(allocation.split_off(bytes).map_err(MountSourceError::NativeWork)?);
        } else {
            self.carrier.with_work(|work| work.release(bytes)).map_err(super::lazy::mount_demand_error)?;
            self.bytes = self.bytes.checked_sub(bytes).expect("owned input release");
        }
        Ok(())
    }

    pub(super) fn clone_path(&mut self, path: &Path) -> Result<std::path::PathBuf, MountSourceError> {
        let original = self.carrier.original_native_work().ok_or(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::InvalidLayout))?;
        let (path, mut allocation) = crate::native_host::clone_path_with_work(path, &mut &original)
            .map_err(MountSourceError::NativeWork)?;
        self.allocation.as_mut().expect("native host input").absorb(&mut allocation).map_err(MountSourceError::NativeWork)?;
        Ok(path)
    }

    pub(super) fn retain(mut self) -> Option<crate::NativeAllocation> {
        self.bytes = 0;
        self.allocation.take()
    }
}

impl Drop for InputBuffers<'_, '_> {
    fn drop(&mut self) {
        if self.allocation.is_none() {
            self.carrier.with_work(|work| work.release(self.bytes)).expect("owned input buffers balance");
        }
    }
}
