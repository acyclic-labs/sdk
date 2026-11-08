use super::*;
#[cfg(any(test, feature = "native-mount"))]
use crate::cancellation::{CancellationWaitError, Cancelled};
use crate::kernel::ProofDataCustody;
use crate::sync::atomic::{AtomicU64, Ordering, fence};
use std::sync::Arc;

#[path = "admissions_accounting.rs"]
mod accounting;
pub(crate) use accounting::NativeAllocationAccounting;
#[cfg(feature = "native-mount")]
#[path = "admissions_view_reads.rs"]
mod view_reads;
#[cfg(feature = "native-mount")]
pub(crate) use view_reads::ViewReadCompletion;
#[cfg(all(test, feature = "memory"))]
const SCAN: WorkCounters = WorkCounters {
    items_examined: 1,
    ..WorkCounters::UNCHARGED
};

#[derive(Default)]
pub(crate) struct Admissions {
    pub(super) pending: WorkCounters,
    pub(super) poisoned: bool,
    pub(super) io_failure: Option<DemandError>,
    allocation_accounting: Option<Arc<NativeAllocationAccounting>>,
    paid_non_native: u64,
}
impl Admissions {
    pub(super) fn live_bytes(&self, live: u64) -> u64 {
        let (released, unrecorded) = if let Some(shared) = &self.allocation_accounting {
            let actual = shared.non_native.load(Ordering::Acquire);
            (
                shared
                    .retired
                    .load(Ordering::Acquire)
                    .checked_add(self.paid_non_native.saturating_sub(actual))
                    .expect("balanced retirement"),
                actual.saturating_sub(self.paid_non_native),
            )
        } else {
            (0, 0)
        };
        live.checked_sub(released)
            .and_then(|live| live.checked_add(unrecorded))
            .expect("balanced retained bytes")
    }
}
impl DemandWork {
    pub(crate) fn reconcile_native_controls(&mut self) -> Result<(), crate::WorkError> {
        let Some(shared) = &self.roots.allocation_accounting else {
            return Ok(());
        };
        // Observe the publisher set before draining either completion class.
        // A last publisher leaving after this observation is drained next time.
        let owners = Arc::strong_count(shared);
        if owners == 1 {
            fence(Ordering::Acquire);
        }
        #[cfg(feature = "native-mount")]
        {
            let (completed, unused, failed) = shared.take_view_reads();
            let settled = completed
                .checked_add(unused)
                .ok_or(crate::WorkError::Overflow)?;
            let returned = WorkCounters {
                items_examined: settled,
                ..WorkCounters::UNCHARGED
            };
            self.roots.pending = returned.remaining(self.roots.pending)?;
            self.budget = self.budget.checked_add(returned)?;
            self.work = self.work.checked_add(WorkCounters {
                items_examined: completed,
                ..WorkCounters::UNCHARGED
            })?;
            if failed {
                self.roots.poisoned = true;
                return Err(crate::WorkError::Overflow);
            }
            self.work.verify(self.budget)?;
        }
        let retired = shared.take_retired();
        if let Err(error) = self.allocations.release(retired) {
            shared.retire_native(retired);
            return Err(error.into_work_or(crate::WorkError::Overflow));
        }
        if let Some(bytes) = shared.drain_for_final_owner(owners, self.roots.paid_non_native)? {
            drop(self.roots.allocation_accounting.take());
            self.roots.paid_non_native = 0;
            self.allocations
                .release(bytes)
                .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        }
        Ok(())
    }
    pub(crate) fn reconcile_cancellation_registry(&mut self) -> Result<(), crate::WorkError> {
        self.reconcile_native_controls()?;
        let mut failure = None;
        if let Some(shared) = &self.roots.allocation_accounting {
            let actual = shared.non_native.load(Ordering::Acquire);
            let paid = self.roots.paid_non_native;
            if actual < paid {
                self.allocations
                    .release(paid - actual)
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
            } else if actual > paid {
                self.allocations
                    .record_owned_bytes(actual - paid, &mut self.work)
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                if let Err(error) = self.work.verify(self.budget) {
                    failure.get_or_insert(error);
                }
            }
            self.roots.paid_non_native = actual;
        }
        failure.map_or(Ok(()), Err)
    }
    #[cfg(feature = "native-watch")]
    pub(super) fn owns_native_allocation_accounting(
        &self,
        shared: &Arc<NativeAllocationAccounting>,
    ) -> bool {
        self.roots
            .allocation_accounting
            .as_ref()
            .is_some_and(|original| Arc::ptr_eq(original, shared))
    }
    pub(crate) fn allocation_accounting(
        &mut self,
    ) -> Result<Arc<NativeAllocationAccounting>, DemandError> {
        if let Some(shared) = &self.roots.allocation_accounting {
            return Ok(shared.clone());
        }
        let bytes = NativeAllocationAccounting::ALLOCATION_BYTES;
        #[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
        let bytes = metadata_tests::admitted_bytes(bytes);
        // This shared control belongs to the original ledger, not the first
        // resource or capture that needs deferred retirement.
        self.scope(
            |work| &mut work.native_resource,
            None,
            |work| {
                work.scope(
                    |work| &mut work.capture_custody,
                    None,
                    |work| work.claim_allocation(bytes, 1, WorkCounters::UNCHARGED),
                )
            },
        )?;
        let shared = Arc::new(NativeAllocationAccounting::default());
        self.roots.allocation_accounting = Some(shared.clone());
        Ok(shared)
    }
    pub(crate) fn owns_native_checkout_token(&self, token: &Arc<ProofDataCustody>) -> bool {
        self.roots
            .allocation_accounting
            .as_ref()
            .is_some_and(|shared| token.is_registered_native_accounting(shared))
    }
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn prepare_cancelled(
        &mut self,
        token: &CancellationToken,
    ) -> Result<Cancelled<'static>, CancellationWaitError> {
        self.reconcile_cancellation_registry()?;
        token.prepare_cancelled_measured(&mut self.allocations, &mut self.work, self.budget)
    }
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn finish_cancelled(
        &mut self,
        prepared: Cancelled<'static>,
    ) -> Result<(), CancellationWaitError> {
        let reconciled = self
            .reconcile_cancellation_registry()
            .map_err(CancellationWaitError::from);
        let finished = prepared.finish(&mut self.allocations, &mut self.work, self.budget);
        reconciled.and(finished)
    }
}
impl NativeDemandReadMeter {
    pub(crate) fn checkout_control_sink(
        self: &Arc<Self>,
        token: &Arc<ProofDataCustody>,
    ) -> Option<Arc<ProofDataCustody>> {
        self.lock()
            .owns_native_checkout_token(token)
            .then(|| token.clone())
    }
    pub(crate) fn register_native_checkout_control(
        self: &Arc<Self>,
        native: &crate::NativeMaterializationWork,
        token: &Arc<ProofDataCustody>,
        header: &mut crate::NativeAllocation,
    ) -> Result<Arc<ProofDataCustody>, crate::NativeTreeMaterializationError> {
        use crate::NativeTreeMaterializationError as Error;
        if !header.is_checkout_header_for(self)
            || token.retained_payload_bytes() != 0
            || token.has_native_control_events()
        {
            return Err(Error::InvalidLayout);
        }
        let mut work = self.lock();
        work.scope(
            |work| &mut work.native_invocation,
            native.invocation.as_ref().map(Arc::downgrade),
            |work| work.check(),
        )?;
        if work.owns_native_checkout_token(token) || !token.accepts_native_original(self) {
            return Err(Error::InvalidLayout);
        }
        let shared = work.allocation_accounting()?;
        if !token.bind_native_original(self, &shared) {
            return Err(Error::InvalidLayout);
        }
        header.transfer_checkout_metadata(self, token.logical_owned_bytes())?;
        token.register_native_accounting(&shared)?;
        Ok(token.clone())
    }
}

#[cfg(test)]
#[cfg(feature = "native-watch")]
#[path = "admissions_custody_tests.rs"]
mod custody_tests;

#[cfg(test)]
#[cfg(feature = "memory")]
#[path = "admissions_tests.rs"]
mod tests;

#[cfg(test)]
#[cfg(all(loom, not(target_arch = "wasm32")))]
#[path = "admissions_loom_tests.rs"]
mod loom_tests;

#[cfg(test)]
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
#[path = "admissions_metadata_tests.rs"]
mod metadata_tests;
