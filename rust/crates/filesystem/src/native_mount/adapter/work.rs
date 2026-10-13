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

    pub(super) fn new_custody(
        &mut self,
    ) -> Result<std::sync::Arc<super::NativeMountAllocationCustody>, MountSourceError> {
        match self.original_native_work() {
            Some(mut work) => super::NativeMountAllocationCustody::new(&mut work),
            None => self.with_work(super::NativeMountAllocationCustody::new),
        }
        .map_err(super::lazy::mount_demand_error)
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
