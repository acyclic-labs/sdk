use acyclic_stream::OwnedBytes;

pub(super) enum NativeDemandCarrier<'a> {
    Borrowed(&'a mut crate::demand::DemandWork),
    Native(&'a crate::NativeMaterializationWork),
}

impl<'a> NativeDemandCarrier<'a> {
    pub(super) fn with_work<T>(
        &mut self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        match self {
            Self::Borrowed(work) => operation(work),
            Self::Native(work) => work.with_original_leaf(operation),
        }
    }

    pub(super) fn cancellation(&mut self) -> CancellationToken {
        match self {
            Self::Native(work) => work.cancellation().clone(),
            _ => self.with_work(|work| work.cancellation().clone()),
        }
    }

    pub(super) fn original_native_work(&self) -> Option<&'a crate::NativeMaterializationWork> {
        match self {
            Self::Native(work) => Some(*work),
            _ => None,
        }
    }

    pub(super) fn new_custody(
        &mut self,
    ) -> Result<std::sync::Arc<super::NativeMountAllocationCustody>, MountSourceError> {
        match self.original_native_work().cloned() {
            Some(mut work) => super::NativeMountAllocationCustody::new(&mut work),
            None => self.with_work(super::NativeMountAllocationCustody::new),
        }
        .map_err(MountSourceError::from)
    }
}

fn capture_meter(
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<crate::NativeMaterializationWork, MountSourceError> {
    crate::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(error, _)| MountSourceError::NativeWork(error))
}

#[cfg(test)]
std::thread_local! {
    static CALLBACK_BOUNDARIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(in crate::native_mount) fn callback_boundary_count() -> usize {
    CALLBACK_BOUNDARIES.get()
}

pub(super) fn boundary<T>(
    cancellation: &CancellationToken,
    operation: impl FnOnce(&crate::NativeMaterializationWork) -> Result<T, MountSourceError>,
) -> Result<T, MountSourceError> {
    #[cfg(test)]
    CALLBACK_BOUNDARIES.with(|count| count.set(count.get() + 1));
    let work = capture_meter(crate::performance::BOUNDARY_BUDGET, cancellation)?;
    operation(&work)
}

impl NativeDemandCarrier<'_> {
    pub(in crate::native_mount) fn with_accounting<T>(
        &mut self,
        operation: impl FnOnce(&mut crate::kernel::KernelAccounting<'_>) -> Result<T, crate::FsError>,
    ) -> Result<T, MountSourceError> {
        match self {
            Self::Native(work) => operation(&mut crate::kernel::KernelAccounting::native(
                work,
                work.cancellation(),
            )),
            Self::Borrowed(work) => {
                work.record(crate::WorkCounters::UNCHARGED)
                    .map_err(MountSourceError::from)?;
                work.check().map_err(MountSourceError::from)?;
                let (allocations, counters, budget, cancellation) = work.allocation_parts();
                operation(&mut crate::kernel::KernelAccounting::borrowed_standalone(
                    allocations,
                    counters,
                    budget,
                    cancellation,
                ))
            }
        }
        .map_err(super::lazy::mount_engine_error)
    }
}
