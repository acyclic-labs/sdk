use super::*;
use crate::cancellation::{CancellationWaitError, Cancelled};
use crate::kernel::{
    ProofDataCustody,
    allocation::{AllocationLedger, LogicalVecCapacity},
};
use std::sync::{Arc, Weak};

struct Observer {
    token: Arc<ProofDataCustody>,
    paid: u64,
    retired: u128,
    native: bool,
    non_native_bytes: u64,
}
pub(super) const SCAN: WorkCounters = WorkCounters {
    items_examined: 1,
    ..WorkCounters::UNCHARGED
};

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
    observers: (Vec<Observer>, LogicalVecCapacity),
    pub(super) native_mount_admissions: (
        Vec<(Weak<NativeMountAllocationCustody>, u64)>,
        LogicalVecCapacity,
    ),
    #[cfg(feature = "native-mount")]
    pub(crate) view_gates: crate::native_mount::ViewGateReadAdmissions,
}
fn observer_bytes(entry: &Observer) -> u64 {
    if entry.native {
        if entry.token.native_accounting_pending() {
            entry.token.non_native_retained_payload_bytes()
        } else {
            entry.non_native_bytes
        }
    } else {
        entry.token.retained_payload_bytes()
    }
}
fn native_retired(entry: &Observer) -> u64 {
    if !entry.native || !entry.token.native_accounting_pending() {
        return 0;
    }
    let retired = entry
        .token
        .native_control_events()
        .1
        .checked_sub(entry.retired)
        .expect("monotonic retirement");
    u64::try_from(retired).expect("bounded live native bytes")
}
fn mount_bytes(entry: &(Weak<NativeMountAllocationCustody>, u64)) -> u64 {
    entry.0.upgrade().map_or(0, |owner| owner.retained_bytes())
}
fn reconcile_native_observer(
    entry: &mut Observer,
    allocations: &mut AllocationLedger,
) -> Result<(), crate::WorkError> {
    if !entry.native {
        return Ok(());
    }
    let Some((retired, non_native)) = entry.token.take_native_accounting_change()? else {
        return Ok(());
    };
    let reconciled = (|| {
        let released = u64::try_from(
            retired
                .checked_sub(entry.retired)
                .ok_or(crate::WorkError::Overflow)?,
        )
        .map_err(|_| crate::WorkError::Overflow)?;
        allocations
            .release(released)
            .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        entry.retired = retired;
        entry.non_native_bytes = non_native;
        Ok(())
    })();
    if reconciled.is_err() {
        entry.token.retry_native_accounting();
    }
    reconciled
}
impl Admissions {
    pub(super) fn live_bytes(&self, live: u64) -> u64 {
        let backing = self
            .observers
            .0
            .iter()
            .map(|entry| (entry.paid, observer_bytes(entry)))
            .chain(
                self.native_mount_admissions
                    .0
                    .iter()
                    .map(|entry| (entry.1, mount_bytes(entry))),
            );
        let (released, unrecorded) =
            backing.fold((0u64, 0u64), |(released, unrecorded), (paid, actual)| {
                (
                    released
                        .checked_add(paid.saturating_sub(actual))
                        .expect("balanced retirement"),
                    unrecorded
                        .checked_add(actual.saturating_sub(paid))
                        .expect("bounded live bytes"),
                )
            });
        let released = self
            .observers
            .0
            .iter()
            .map(native_retired)
            .try_fold(released, u64::checked_add)
            .expect("balanced retirement");
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
        for entry in &mut self.roots.observers.0 {
            reconcile_native_observer(entry, &mut self.allocations)?;
        }
        let mut index = 0;
        while index < self.roots.observers.0.len() {
            let entry = &self.roots.observers.0[index];
            if entry.native
                && Arc::strong_count(&entry.token) == 1
                && entry.token.retained_payload_bytes() == 0
            {
                let entry = self.roots.observers.0.swap_remove(index);
                let bytes = entry.token.logical_owned_bytes() + entry.paid;
                drop(entry);
                self.allocations
                    .release(bytes)
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
            } else {
                index += 1;
            }
        }
        if self.roots.observers.0.is_empty() {
            let (backing, capacity) = std::mem::take(&mut self.roots.observers);
            drop(backing);
            self.allocations
                .release(capacity.logical_bytes())
                .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        }
        Ok(())
    }
    pub(crate) fn reconcile_cancellation_registry(&mut self) -> Result<(), crate::WorkError> {
        self.reconcile_native_controls()?;
        let observers = self.roots.observers.0.iter_mut().map(|entry| {
            let actual = observer_bytes(entry);
            (&mut entry.paid, actual)
        });
        let mounts = self
            .roots
            .native_mount_admissions
            .0
            .iter_mut()
            .map(|entry| {
                let actual = mount_bytes(entry);
                (&mut entry.1, actual)
            });
        let mut failure = None;
        for (paid, actual) in observers.chain(mounts) {
            if actual < *paid {
                self.allocations
                    .release(*paid - actual)
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
            } else if actual > *paid {
                self.allocations
                    .record_owned_bytes(actual - *paid, &mut self.work)
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                if let Err(error) = self.work.verify(self.budget) {
                    failure.get_or_insert(error);
                }
            }
            *paid = actual;
        }
        #[cfg(feature = "native-mount")]
        {
            self.roots.view_gates.reconcile(&mut self.allocations)?;
            self.roots
                .view_gates
                .reconcile_work(&mut self.work, &mut self.budget)?;
        }
        failure.map_or(Ok(()), Err)
    }
    fn retain_observer(
        &mut self,
        token: Arc<ProofDataCustody>,
    ) -> Result<Option<usize>, DemandError> {
        for entry in &self.roots.observers.0 {
            self.work.charge(&SCAN, &self.budget)?;
            if Arc::ptr_eq(&entry.token, &token) {
                return Ok(None);
            }
        }
        insert(
            &mut self.roots.observers,
            Observer {
                token,
                paid: 0,
                retired: 0,
                native: false,
                non_native_bytes: 0,
            },
            &mut self.allocations,
            &mut self.work,
            self.budget,
        )
        .map(Some)
    }
    pub(crate) fn owns_native_checkout_token(&self, token: &Arc<ProofDataCustody>) -> bool {
        self.roots
            .observers
            .0
            .iter()
            .any(|entry| entry.native && Arc::ptr_eq(&entry.token, token))
    }
    pub(crate) fn prepare_cancelled(
        &mut self,
        token: &CancellationToken,
    ) -> Result<Cancelled<'static>, CancellationWaitError> {
        self.reconcile_cancellation_registry()?;
        token.prepare_cancelled_measured(&mut self.allocations, &mut self.work, self.budget)
    }
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
            || token.native_control_events() != (0, 0)
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
        if work.owns_native_checkout_token(token) || !token.bind_native_original(self) {
            return Err(Error::InvalidLayout);
        }
        let index = work
            .retain_observer(token.clone())
            .map_err(crate::materializer::native_materialization_work_error)?
            .ok_or(Error::InvalidLayout)?;
        header.transfer_checkout_metadata(self, token.logical_owned_bytes())?;
        let entry = work
            .roots
            .observers
            .0
            .get_mut(index)
            .ok_or(Error::InvalidLayout)?;
        entry.native = true;
        entry.retired = 0;
        Ok(token.clone())
    }
}
