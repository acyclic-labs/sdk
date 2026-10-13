use acyclic_stream::OwnedBytes;

pub(super) enum NativeDemandCarrier<'a> {
    Borrowed(&'a mut crate::demand::DemandWork),
    Shared(&'a Arc<crate::demand::NativeDemandReadMeter>),
    Native(&'a crate::NativeMaterializationWork),
}

impl NativeDemandCarrier<'_> {
    pub(super) fn with_work<T>(
        &mut self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        match self {
            Self::Borrowed(work) => operation(work),
            Self::Shared(meter) => operation(&mut meter.lock()),
            Self::Native(work) => work.with_original_leaf(operation),
        }
    }

    pub(super) fn cancellation(&mut self) -> CancellationToken {
        match self {
            Self::Native(work) => work.cancellation().clone(),
            _ => self.with_work(|work| work.cancellation().clone()),
        }
    }

    pub(super) fn original_native_work(&self) -> Option<crate::NativeMaterializationWork> {
        match self {
            Self::Native(work) => Some((**work).clone()),
            _ => None,
        }
    }

    fn remaining(&mut self) -> Result<(WorkBudget, u64), MountSourceError> {
        self.with_work(|work| {
            Ok((
                work.remaining().map_err(super::lazy::mount_demand_error)?,
                work.live_allocation_bytes(),
            ))
        })
    }

    pub(super) fn new_custody(
        &mut self,
    ) -> Result<std::sync::Arc<super::NativeMountAllocationCustody>, MountSourceError> {
        match self.original_native_work() {
            Some(work) => super::NativeMountAllocationCustody::new(
                &mut crate::NativeMaterializationWork::clone(&work),
            ),
            None => self.with_work(super::NativeMountAllocationCustody::new),
        }
        .map_err(super::lazy::mount_demand_error)
    }

    pub(super) fn accept_with_prior_live<T, E>(
        &mut self,
        result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
        prior_live: u64,
        map: impl FnOnce(E) -> MountSourceError,
    ) -> Result<T, MountSourceError> {
        let (result, completed) = match result {
            Ok(receipt) => (Ok(receipt.value), receipt.work),
            Err(failure) => (Err(map(failure.error)), *failure.work),
        };
        self.with_work(|work| work.merge(completed, prior_live))
            .map_err(super::lazy::mount_demand_error)?;
        result
    }

    fn finish_read(
        &mut self,
        chunk: crate::kernel::OwnedSourceChunk,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let bytes = if self.original_native_work().is_some() {
            0
        } else {
            chunk.logical_owned_bytes()
        };
        let result = if bytes == 0 {
            Ok(())
        } else {
            self.with_work(|work| work.record_native_owned_completed(bytes, &None).1)
                .map_err(super::lazy::mount_demand_error)
        }
        .and_then(|()| self.check(cancellation, lifetime));
        if let Err(error) = result {
            drop(chunk);
            if bytes != 0 {
                self.with_work(|work| work.release(bytes))
                    .map_err(super::lazy::mount_demand_error)?;
            }
            return Err(error);
        }
        Ok(chunk)
    }
}

fn capture_meter(
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<crate::NativeMaterializationWork, MountSourceError> {
    crate::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(error, _)| MountSourceError::NativeWork(error))
}

pub(super) fn boundary<T>(
    cancellation: &CancellationToken,
    operation: impl FnOnce(&crate::NativeMaterializationWork) -> Result<T, MountSourceError>,
) -> Result<T, MountSourceError> {
    let work = capture_meter(crate::performance::BOUNDARY_BUDGET, cancellation)?;
    operation(&work)
}
