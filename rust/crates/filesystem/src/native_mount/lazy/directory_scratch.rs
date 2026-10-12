use super::*;

pub(in crate::native_mount::lazy) struct DirectoryScratch<'a> {
    pub(in crate::native_mount::lazy) work: &'a mut crate::demand::DemandWork,
    pub(in crate::native_mount::lazy) bytes: u64,
}

impl<'a> DirectoryScratch<'a> {
    pub(in crate::native_mount::lazy) fn new(work: &'a mut crate::demand::DemandWork) -> Self { Self { work, bytes: 0 } }

    pub(in crate::native_mount::lazy) fn track(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        self.bytes = self.bytes.checked_add(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        Ok(())
    }

    pub(in crate::native_mount::lazy) fn keep(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        self.bytes = self.bytes.checked_sub(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        Ok(())
    }


}

impl Drop for DirectoryScratch<'_> {
    fn drop(&mut self) {
        self.work.release(self.bytes).expect("directory scratch releases only its original allocations");
    }
}

pub(in crate::native_mount::lazy) struct DirectoryCarrierScratch<'a, 'w> {
    pub(in crate::native_mount::lazy) carrier: &'a mut crate::native_mount::adapter::NativeDemandCarrier<'w>,
    pub(in crate::native_mount::lazy) bytes: u64,
    pub(in crate::native_mount::lazy) reserved_items: u64,
    pub(in crate::native_mount::lazy) reserved_copies: u64,
    pub(in crate::native_mount::lazy) finalization: Option<(Box<Option<MountSourceError>>, Box<Option<crate::demand::DemandError>>, Option<crate::NativeAllocation>)>,
    pub(in crate::native_mount::lazy) native_backing: Option<crate::NativeAllocation>,
}

impl<'a, 'w> DirectoryCarrierScratch<'a, 'w> {
    pub(in crate::native_mount::lazy) fn new(carrier: &'a mut crate::native_mount::adapter::NativeDemandCarrier<'w>) -> Self {
        Self { carrier, bytes: 0, reserved_items: 0, reserved_copies: 0, finalization: None, native_backing: None }
    }

    pub(in crate::native_mount::lazy) fn track(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        self.bytes = self.bytes.checked_add(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        Ok(())
    }

    pub(in crate::native_mount::lazy) fn keep(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        self.bytes = self.bytes.checked_sub(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        Ok(())
    }

    pub(in crate::native_mount::lazy) fn release(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        self.keep(bytes)?;
        self.carrier.with_work(|work| work.release(bytes)).map_err(mount_demand_error)
    }

    pub(in crate::native_mount::lazy) fn release_backing(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        if self.carrier.original_native_work().is_some() {
            if let Some(allocation) = &mut self.native_backing {
                allocation.release_deferred(bytes);
            } else if bytes != 0 {
                return Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::MissingCustody));
            }
            Ok(())
        } else {
            self.release(bytes)
        }
    }

    pub(in crate::native_mount::lazy) fn absorb_native(&mut self, allocation: &mut crate::NativeAllocation) -> Result<(), MountSourceError> {
        if let Some(backing) = &mut self.native_backing {
            backing.absorb(allocation).map_err(MountSourceError::NativeWork)
        } else {
            self.native_backing = Some(allocation.split_off(allocation.bytes()).map_err(MountSourceError::NativeWork)?);
            Ok(())
        }
    }

    pub(in crate::native_mount::lazy) fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, MountSourceError> {
        if let Some(original) = self.carrier.original_native_work() {
            use crate::native_mount::adapter::CaptureInputScratch;
            let mut scratch = crate::native_mount::adapter::NativeCaptureScratchAllocation::new(&original)?;
            let mut owned = (scratch.vec::<T>(count)?, scratch.retain());
            self.absorb_native(&mut owned.1)?;
            Ok(owned.0)
        } else {
            let values = self.carrier.with_work(|work| mount_vec::<T>(count, work))?;
            let bytes = values.capacity().checked_mul(std::mem::size_of::<T>())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            self.track(bytes)?;
            Ok(values)
        }
    }

    pub(in crate::native_mount::lazy) fn prepare_finalization(&mut self) -> Result<(), MountSourceError> {
        let bytes = (std::mem::size_of::<Option<MountSourceError>>()
            + std::mem::size_of::<Option<crate::demand::DemandError>>()) as u64;
        let allocation = if let Some(original) = self.carrier.original_native_work() {
            Some(original.claim(bytes, 2, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?)
        } else {
            self.carrier.with_work(|work| work.claim_allocation(bytes, 2)).map_err(mount_demand_error)?;
            self.track(bytes)?;
            None
        };
        self.finalization = Some((Box::new(None), Box::new(None), allocation));
        Ok(())
    }

    pub(in crate::native_mount::lazy) fn finalization_error(
        &mut self, primary: MountSourceError, cleanup: crate::demand::DemandError,
    ) -> MountSourceError {
        let (mut slot, mut cleanup_slot, custody) = self.finalization.take()
            .expect("the original primary slot precedes every removal or rename effect");
        *slot = Some(primary);
        *cleanup_slot = Some(cleanup);
        if custody.is_none() {
            self.keep((std::mem::size_of::<Option<MountSourceError>>()
                + std::mem::size_of::<Option<crate::demand::DemandError>>()) as u64)
                .expect("the primary slot keeps only its original admitted allocation");
        }
        MountSourceError::MutationFinalization { primary: slot, cleanup: cleanup_slot, custody }
    }

    pub(in crate::native_mount::lazy) fn reserve_final_work(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        let items = self.reserved_items.checked_add(delta.items_examined)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let copies = self.reserved_copies.checked_add(delta.bytes_copied)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        self.carrier.with_work(|work| {
            work.reserve(crate::materializer::NativeResourceCredit::Items, delta.items_examined).map_err(mount_demand_error)?;
            if let Err(error) = work.reserve(crate::materializer::NativeResourceCredit::Copy, delta.bytes_copied) {
                work.refund(crate::materializer::NativeResourceCredit::Items, delta.items_examined).expect("original reservation is balanced");
                return Err(mount_demand_error(error));
            }
            Ok::<_, MountSourceError>(())
        })?;
        self.reserved_items = items;
        self.reserved_copies = copies;
        Ok(())
    }


}

impl Drop for DirectoryCarrierScratch<'_, '_> {
    fn drop(&mut self) {
        let items = std::mem::take(&mut self.reserved_items);
        let copies = std::mem::take(&mut self.reserved_copies);
        self.carrier.with_work(|work| {
            work.refund(crate::materializer::NativeResourceCredit::Items, items).expect("original reservation is balanced");
            work.refund(crate::materializer::NativeResourceCredit::Copy, copies).expect("original reservation is balanced");
        });
        drop(self.finalization.take());
        let bytes = self.bytes;
        if bytes == 0 { return; }
        if self.carrier.original_native_work().is_some() {
            self.carrier.with_work(|work| {
                if let Err(error) = work.release(bytes) {
                    work.record_native_io_failure(error);
                    work.poison_native_original_work();
                }
            });
        } else {
            self.carrier.with_work(|work| work.release(bytes))
                .expect("directory scratch releases only its original allocations");
        }
    }
}

pub(in crate::native_mount::lazy) fn directory_removal_bytes(names: &Vec<String>) -> Result<u64, MountSourceError> {
    let backing = names.capacity().checked_mul(std::mem::size_of::<String>())
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
    names.iter().try_fold(backing, |bytes, name| bytes.checked_add(name.capacity() as u64)
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow)))
}

pub(in crate::native_mount::lazy) fn directory_lazy_error(error: LazyWorkspaceError) -> MountSourceError {
    match error.semantic_error() {
        LazyWorkspaceError::Demand(DemandError::StaleVersion) => MountSourceError::Stale,
        _ => lazy_error(error),
    }
}

pub(in crate::native_mount::lazy) fn promotion_push<T>(
    values: &mut Vec<T>, value: T, scratch: &mut DirectoryCarrierScratch<'_, '_>,
) -> Result<(), MountSourceError> {
    let original = scratch.carrier.original_native_work();
    if let Some(original) = &original {
        original.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
            .map_err(MountSourceError::NativeWork)?;
    } else {
        scratch.carrier.with_work(|work| mount_items(work, 1))?;
    }
    if values.len() == values.capacity() {
        let capacity = values.capacity().checked_mul(2).map(|capacity| capacity.max(1))
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let copied = values.len().checked_mul(std::mem::size_of::<T>())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        let delta = crate::WorkCounters { bytes_copied: copied, ..crate::WorkCounters::UNCHARGED };
        let mut replacement = if let Some(original) = &original {
            use crate::native_mount::adapter::CaptureInputScratch;
            original.admit(delta).map_err(MountSourceError::NativeWork)?;
            let mut allocation = crate::native_mount::adapter::NativeCaptureScratchAllocation::new(original)?;
            let values = allocation.vec::<T>(capacity)?;
            let mut allocation = allocation.retain();
            if let Some(backing) = &mut scratch.native_backing {
                backing.absorb(&mut allocation).map_err(MountSourceError::NativeWork)?;
            } else {
                scratch.native_backing = Some(allocation);
            }
            values
        } else {
            scratch.carrier.with_work(|work| work.work.admit(&delta, &work.budget))
                .map_err(MountSourceError::Work)?;
            let values = scratch.carrier.with_work(|work| mount_vec(capacity, work))?;
            let bytes = values.capacity().checked_mul(std::mem::size_of::<T>())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            scratch.track(bytes)?;
            values
        };
        replacement.append(values);
        if let Some(original) = &original {
            original.record_admitted(delta).map_err(MountSourceError::NativeWork)?;
        } else {
            scratch.carrier.with_work(|work| work.record(delta)).map_err(mount_demand_error)?;
        }
        let old = std::mem::replace(values, replacement);
        let bytes = old.capacity().checked_mul(std::mem::size_of::<T>())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        drop(old);
        scratch.release_backing(bytes)?;
    }
    values.push(value);
    Ok(())
}
