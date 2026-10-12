use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct CaptureTransientCustody {
    pub(in crate::demand) retained: std::sync::atomic::AtomicU64,
    pub(in crate::demand) native_header: crate::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl CaptureTransientCustody {
    pub(crate) fn new_native(
            work: &crate::NativeMaterializationWork,
        ) -> Result<Arc<Self>, crate::NativeTreeMaterializationError> {
            let header = work.claim(std::mem::size_of::<Self>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
            Ok(Arc::new(Self {
                retained: std::sync::atomic::AtomicU64::new(0),
                native_header: header,
            }))
        }

    pub(crate) fn owns_native_work(&self, work: &crate::NativeMaterializationWork) -> bool {
            self.native_header.work().shares_ledger(work)
                && self.native_header.work().shares_resource_allowance(work)
        }

    pub(crate) fn take_retained_bytes(&self) -> u64 {
            self.retained.swap(0, std::sync::atomic::Ordering::AcqRel)
        }

    pub(crate) fn transfer_retained_bytes(&self, bytes: u64) -> Result<(), crate::WorkError> {
            self.retained.fetch_update(
                std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
                |retained| retained.checked_sub(bytes),
            ).map(|_| ()).map_err(|_| crate::WorkError::Overflow)
        }

    pub(in crate::demand) fn claim(&self, bytes: u64) {
            self.retained.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        }

    pub(in crate::demand) fn release(&self, bytes: u64) {
            let _ = self.retained.fetch_update(
                std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
                |retained| Some(retained.saturating_sub(bytes)),
            );
        }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeDemandReadMeter<T = DemandWork>(pub(crate) std::sync::Mutex<T>);

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::demand) fn same_resource_view(
    left: Option<&Arc<crate::materializer::NativeResourceAllowance>>,
    right: Option<&Arc<crate::materializer::NativeResourceAllowance>>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::NativeMaterializationWork {
    pub(crate) fn source_leaf<T>(&self, operation: impl FnOnce(&mut DemandWork) -> T) -> Result<T, DemandError> {
        let mut state = self.state.0.lock().map_err(|_| DemandError::OriginalWorkPoisoned)?;
        Ok(state.with_invocation_scope(self.invocation.as_ref().map(Arc::downgrade), |state|
            state.with_resource_scope(self.resource.as_ref().map(Arc::downgrade), |state|
                state.with_capture(self.capture_origin.clone(), operation))))
    }

    pub(crate) fn check_source(&self) -> Result<(), DemandError> {
        self.source_leaf(DemandWork::check)?
    }

    pub(crate) fn finish_source<T>(&self, result: Result<T, DemandError>) -> DemandResult<T> {
        let (checked, counters) = match self.state.0.lock() {
            Ok(mut state) => {
                let checked = state.reconcile_cancellation_registry().map_err(DemandError::from)
                    .and_then(|()| state.work.verify(state.budget).map_err(DemandError::from));
                (checked, state.work)
            }
            Err(poisoned) => (Err(DemandError::OriginalWorkPoisoned), poisoned.get_ref().work),
        };
        checked.and(result).map(|value| OperationReceipt { value, work: counters })
            .map_err(|error| OperationFailure::new(error, counters))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T: AsMut<crate::demand::DemandWork>> NativeDemandReadMeter<T> {
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, T> {
            match self.0.lock() {
                Ok(state) => state,
                Err(poisoned) => {
                    let mut state = poisoned.into_inner();
                    let work = state.as_mut();
                    work.cancellation_wait.native_poisoned = true;
                    let _ = work.record_native_io_failure(DemandError::OriginalWorkPoisoned);
                    state
                }
            }
        }

    pub(crate) fn with_original_leaf<R>(&self, operation: impl FnOnce(&mut T) -> R) -> R {
            operation(&mut *self.lock())
        }
}
