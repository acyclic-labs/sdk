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

    pub(super) fn reborrow(&mut self) -> NativeDemandCarrier<'_> {
        match self {
            Self::Borrowed(work) => NativeDemandCarrier::Borrowed(work),
            Self::Shared(meter) => NativeDemandCarrier::Shared(meter),
            Self::Native(work) => NativeDemandCarrier::Native(work),
        }
    }

    pub(super) fn new_custody(
        &mut self,
    ) -> Result<super::NativeMountAllocationCustody, MountSourceError> {
        match self.original_native_work() {
            Some(work) => super::NativeMountAllocationCustody::new(
                &mut crate::NativeMaterializationWork::clone(&work),
            ),
            None => self.with_work(super::NativeMountAllocationCustody::new),
        }
        .map_err(super::lazy::mount_demand_error)
    }

    pub(super) fn empty_source_chunk(
        &self,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let Some(work) = self.original_native_work() else {
            return Ok(crate::kernel::OwnedSourceChunk::claimed(
                OwnedBytes::new(),
                0,
            ));
        };
        let allocation = work
            .claim(0, 0, crate::WorkCounters::UNCHARGED)
            .map_err(MountSourceError::NativeWork)?;
        let bytes = OwnedBytes::from_vec_with_guard(Vec::new(), allocation, work.runtime_meter())
            .map_err(|failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                MountSourceError::NativeWork(work.runtime_error(error))
            }
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout)
            }
        })?;
        Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, 0))
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
        self.with_work(|work| work.merge_with_prior_live(completed, prior_live))
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
            self.with_work(|work| work.record_adoption(bytes, &None))
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
) -> Result<crate::NativeMaterializationWork, OperationFailure<MountSourceError>> {
    crate::NativeMaterializationWork::new(budget, cancellation).map_err(|(error, completed)| {
        OperationFailure::new(MountSourceError::NativeWork(error), completed)
    })
}

pub(super) fn boundary<T>(
    cancellation: &CancellationToken,
    operation: impl FnOnce(&crate::NativeMaterializationWork) -> Result<T, MountSourceError>,
) -> Result<T, MountSourceError> {
    let work = capture_meter(crate::performance::BOUNDARY_BUDGET, cancellation)
        .map_err(|failure| failure.error)?;
    operation(&work)
}
