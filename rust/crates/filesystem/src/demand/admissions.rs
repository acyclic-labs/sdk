use super::ownership::accounting as mount_accounting;
use super::*;
#[cfg(any(test, feature = "native-mount"))]
use crate::cancellation::{CancellationWaitError, Cancelled};
#[cfg(any(test, feature = "native-watch"))]
use crate::kernel::allocation::AllocationLedger;
use crate::kernel::{ProofDataCustody, allocation::LogicalVecCapacity};
use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::sync::Arc;

#[path = "admissions_accounting.rs"]
mod accounting;
pub(crate) use accounting::NativeProofAccounting;
#[cfg(any(test, feature = "native-watch"))]
pub(super) const SCAN: WorkCounters = WorkCounters {
    items_examined: 1,
    ..WorkCounters::UNCHARGED
};

#[cfg(any(test, feature = "native-watch"))]
pub(super) fn insert<T>(
    (values, capacity): &mut (Vec<T>, LogicalVecCapacity),
    value: T,
    ledger: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<usize, DemandError> {
    let copied = WorkCounters {
        bytes_copied: if values.len() == values.capacity() {
            (values.len() * std::mem::size_of::<T>()) as u64
        } else {
            0
        },
        ..WorkCounters::UNCHARGED
    };
    work.admit(&copied, &budget)?;
    capacity
        .ensure_for_push(values, usize::MAX, ledger, work, budget)
        .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
    work.charge(&copied, &budget)?;
    let index = values.len();
    values.push(value);
    Ok(index)
}
#[derive(Default)]
pub(crate) struct Admissions {
    pub(super) pending: WorkCounters,
    pub(super) poisoned: bool,
    pub(super) io_failure: Option<DemandError>,
    proof_accounting: Option<Arc<NativeProofAccounting>>,
    paid_non_native: u64,
    pub(super) native_mount_admission_pending: bool,
    pub(super) native_mount_dirty: Option<Arc<AtomicBool>>,
    pub(super) native_mount_admissions: (Vec<(Arc<AtomicU64>, u64)>, LogicalVecCapacity),
    #[cfg(feature = "native-mount")]
    pub(crate) view_gates: crate::native_mount::ViewGateReadAdmissions,
}
fn mount_bytes(entry: &(Arc<AtomicU64>, u64)) -> u64 {
    // The registry retains only this inert gauge. Upgrading custody here could
    // drop its last owner under the leaf and re-enter it through metadata Drop.
    mount_accounting::retained(Arc::strong_count(&entry.0), &entry.0)
}
impl Admissions {
    pub(super) fn live_bytes(&self, live: u64) -> u64 {
        let mounts = if self
            .native_mount_dirty
            .as_ref()
            .is_some_and(|dirty| dirty.load(Ordering::Acquire))
        {
            self.native_mount_admissions.0.as_slice()
        } else {
            &[]
        };
        let (released, unrecorded) = mounts
            .iter()
            .map(|entry| (entry.1, mount_bytes(entry)))
            .fold((0u64, 0u64), |(released, unrecorded), (paid, actual)| {
                (
                    released
                        .checked_add(paid.saturating_sub(actual))
                        .expect("balanced retirement"),
                    unrecorded
                        .checked_add(actual.saturating_sub(paid))
                        .expect("bounded live bytes"),
                )
            });
        let (released, unrecorded) = if let Some(shared) = &self.proof_accounting {
            let actual = shared.non_native.load(Ordering::Acquire);
            (
                released
                    .checked_add(shared.retired.load(Ordering::Acquire))
                    .and_then(|bytes| {
                        bytes.checked_add(self.paid_non_native.saturating_sub(actual))
                    })
                    .expect("balanced retirement"),
                unrecorded
                    .checked_add(actual.saturating_sub(self.paid_non_native))
                    .expect("bounded live bytes"),
            )
        } else {
            (released, unrecorded)
        };
        #[cfg(feature = "native-mount")]
        let released = released
            .checked_add(self.view_gates.freed_bytes())
            .expect("balanced view retirement");
        live.checked_sub(released)
            .and_then(|live| live.checked_add(unrecorded))
            .expect("balanced retained bytes")
    }
}
impl DemandWork {
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn admissions<T>(
        &mut self,
        select: fn(&mut Admissions) -> &mut T,
    ) -> Result<(&mut T, &mut AllocationLedger, &mut WorkCounters, WorkBudget), DemandError> {
        self.reconcile_cancellation_registry()?;
        Ok((
            select(&mut self.roots),
            &mut self.allocations,
            &mut self.work,
            self.budget,
        ))
    }
    pub(crate) fn reconcile_native_controls(&mut self) -> Result<(), crate::WorkError> {
        let Some(shared) = &self.roots.proof_accounting else {
            return Ok(());
        };
        let retired = shared.take_retired();
        if let Err(error) = self.allocations.release(retired) {
            shared.retire_native(retired);
            return Err(error.into_work_or(crate::WorkError::Overflow));
        }
        if accounting::acquire_final_owner(Arc::strong_count(shared)) {
            let bytes = NativeProofAccounting::ALLOCATION_BYTES
                .checked_add(self.roots.paid_non_native)
                .and_then(|bytes| bytes.checked_add(shared.take_retired()))
                .ok_or(crate::WorkError::Overflow)?;
            drop(self.roots.proof_accounting.take());
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
        if let Some(shared) = &self.roots.proof_accounting {
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
        self.reconcile_native_mounts(&mut failure)?;
        #[cfg(feature = "native-mount")]
        {
            self.roots.view_gates.reconcile(&mut self.allocations)?;
            self.roots
                .view_gates
                .reconcile_work(&mut self.work, &mut self.budget)?;
        }
        failure.map_or(Ok(()), Err)
    }
    fn reconcile_native_mounts(
        &mut self,
        failure: &mut Option<crate::WorkError>,
    ) -> Result<(), crate::WorkError> {
        let Some(dirty) = self.roots.native_mount_dirty.as_ref() else {
            return self.prune_native_mount_admissions();
        };
        if !mount_accounting::take_dirty(dirty) {
            return Ok(());
        }
        let mut empty = true;
        let mut retired_mount = false;
        for entry in &mut self.roots.native_mount_admissions.0 {
            let actual = mount_bytes(entry);
            let recorded = if actual < entry.1 {
                self.allocations
                    .release(entry.1 - actual)
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))
            } else if actual > entry.1 {
                self.allocations
                    .record_owned_bytes(actual - entry.1, &mut self.work)
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))
            } else {
                Ok(())
            };
            if let Err(error) = recorded {
                mount_accounting::publish(dirty);
                return Err(error);
            }
            if actual > entry.1
                && let Err(error) = self.work.verify(self.budget)
            {
                failure.get_or_insert(error);
            }
            entry.1 = actual;
            empty &= actual == 0;
            retired_mount |= actual == 0;
        }
        if retired_mount {
            self.prune_native_mount_admissions()?;
        }
        if empty && self.roots.native_mount_admission_pending {
            if let Some(dirty) = &self.roots.native_mount_dirty {
                mount_accounting::publish(dirty);
            }
        } else if empty {
            drop(self.roots.native_mount_dirty.take());
            self.allocations
                .release(std::mem::size_of::<AtomicBool>() as u64)
                .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        }
        Ok(())
    }
    fn prune_native_mount_admissions(&mut self) -> Result<(), crate::WorkError> {
        if self.roots.native_mount_admission_pending {
            if let Some(dirty) = &self.roots.native_mount_dirty {
                mount_accounting::publish(dirty);
            }
            return Ok(());
        }
        if self.roots.native_mount_admissions.0.is_empty() {
            return Ok(());
        }
        let owners = &self.roots.native_mount_admissions.0;
        let live = owners.iter().filter(|entry| entry.1 != 0).count();
        let moved = owners[..live].iter().filter(|entry| entry.1 == 0).count();
        let copied = WorkCounters {
            bytes_copied: (moved * std::mem::size_of::<(Arc<AtomicU64>, u64)>()) as u64,
            ..WorkCounters::UNCHARGED
        };
        // Retirement is unconditional. Moving survivors requires the current
        // cancellation, invocation, resource and original-ledger admission.
        let deferred = moved != 0 && self.charge_reconciled(copied).is_err();
        let (owners, capacity) = &mut self.roots.native_mount_admissions;
        let mut released = 0;
        while owners.last().is_some_and(|entry| entry.1 == 0) {
            owners.pop();
            released += super::ownership::GAUGE_ALLOCATION_BYTES;
        }
        if deferred {
            if let Some(dirty) = &self.roots.native_mount_dirty {
                mount_accounting::publish(dirty);
            }
            return self
                .allocations
                .release(released)
                .map_err(|error| error.into_work_or(crate::WorkError::Overflow));
        }
        let mut index = 0;
        while index < owners.len() {
            if owners[index].1 == 0 {
                owners.swap_remove(index);
                released += super::ownership::GAUGE_ALLOCATION_BYTES;
                while owners.last().is_some_and(|entry| entry.1 == 0) {
                    owners.pop();
                    released += super::ownership::GAUGE_ALLOCATION_BYTES;
                }
            } else {
                index += 1;
            }
        }
        if owners.is_empty() {
            let backing = std::mem::take(owners);
            released += std::mem::take(capacity).logical_bytes();
            drop(backing);
        }
        self.allocations
            .release(released)
            .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        Ok(())
    }
    fn proof_accounting(&mut self) -> Result<Arc<NativeProofAccounting>, DemandError> {
        if let Some(shared) = &self.roots.proof_accounting {
            return Ok(shared.clone());
        }
        let bytes = NativeProofAccounting::ALLOCATION_BYTES;
        #[cfg(test)]
        #[cfg(all(not(acyclic_fs_loom), not(target_arch = "wasm32")))]
        let bytes = metadata_tests::admitted_bytes(bytes);
        self.claim_allocation(bytes, 1, WorkCounters::UNCHARGED)?;
        let shared = Arc::new(NativeProofAccounting::default());
        self.roots.proof_accounting = Some(shared.clone());
        Ok(shared)
    }
    pub(crate) fn owns_native_checkout_token(&self, token: &Arc<ProofDataCustody>) -> bool {
        self.roots
            .proof_accounting
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
        )
        .map_err(crate::materializer::native_materialization_work_error)?;
        if work.owns_native_checkout_token(token) || !token.accepts_native_original(self) {
            return Err(Error::InvalidLayout);
        }
        let shared = work
            .proof_accounting()
            .map_err(crate::materializer::native_materialization_work_error)?;
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
#[path = "admissions_registry_tests.rs"]
mod registry_tests;

#[cfg(test)]
#[cfg(feature = "memory")]
#[path = "admissions_tests.rs"]
mod tests;

#[cfg(test)]
#[cfg(all(acyclic_fs_loom, not(target_arch = "wasm32")))]
#[path = "admissions_loom_tests.rs"]
mod loom_tests;

#[cfg(test)]
#[cfg(all(not(acyclic_fs_loom), not(target_arch = "wasm32")))]
#[path = "admissions_metadata_tests.rs"]
mod metadata_tests;
