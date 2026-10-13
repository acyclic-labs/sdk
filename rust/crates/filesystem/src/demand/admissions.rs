use super::*;
use crate::cancellation::{CancellationWaitAdmission, CancellationWaitError, PreparedCancelled};
use crate::kernel::{
    ProofDataCustody,
    allocation::{AllocationLedger, LogicalVecCapacity},
};
use std::sync::{Arc, Weak};

type Observer = (Arc<ProofDataCustody>, u64, Option<u128>);
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
    cancellations: (
        Vec<(CancellationToken, CancellationWaitAdmission)>,
        LogicalVecCapacity,
    ),
    observers: (Vec<Observer>, LogicalVecCapacity),
    pub(super) native_mount_admissions: (
        Vec<(Weak<NativeMountAllocationCustody>, u64)>,
        LogicalVecCapacity,
    ),
    #[cfg(feature = "native-mount")]
    pub(crate) view_gates: crate::native_mount::ViewGateReadAdmissions,
}
fn observer_bytes(entry: &Observer) -> u64 {
    if entry.2.is_some() {
        entry.0.non_native_retained_payload_bytes()
    } else {
        entry.0.retained_payload_bytes()
    }
}
fn native_retired(entry: &Observer) -> u64 {
    entry.2.map_or(0, |previous| {
        let retired = entry
            .0
            .native_control_events()
            .1
            .checked_sub(previous)
            .expect("monotonic retirement");
        u64::try_from(retired).expect("bounded live native bytes")
    })
}
fn mount_bytes(entry: &(Weak<NativeMountAllocationCustody>, u64)) -> u64 {
    entry.0.upgrade().map_or(0, |owner| owner.retained_bytes())
}
impl Admissions {
    pub(super) fn live_bytes(&self, live: u64) -> u64 {
        let backing = self
            .observers
            .0
            .iter()
            .map(|entry| (entry.1, observer_bytes(entry)))
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
            .cancellations
            .0
            .iter()
            .map(|(token, admission)| admission.freed_registry_bytes(token))
            .chain(self.observers.0.iter().map(native_retired))
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
            if entry.2.is_some() {
                let (born, retired) = entry.0.native_control_events();
                if retired > born {
                    return Err(crate::WorkError::Overflow);
                }
                self.allocations
                    .release(native_retired(entry))
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                entry.2 = Some(retired);
            }
        }
        Ok(())
    }
    pub(crate) fn reconcile_cancellation_registry(&mut self) -> Result<(), crate::WorkError> {
        for (token, admission) in &mut self.roots.cancellations.0 {
            admission.reconcile_registry(token, &mut self.allocations)?;
        }
        self.reconcile_native_controls()?;
        let observers = self.roots.observers.0.iter_mut().map(|entry| {
            let actual = observer_bytes(entry);
            (&mut entry.1, actual)
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
            if Arc::ptr_eq(&entry.0, &token) {
                return Ok(None);
            }
        }
        insert(
            &mut self.roots.observers,
            (token, 0, None),
            &mut self.allocations,
            &mut self.work,
            self.budget,
        )
        .map(Some)
    }
    pub(crate) fn retain_observer_custody(
        &mut self,
        token: Arc<ProofDataCustody>,
    ) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.retain_observer(token).map(|_| ())
    }
    pub(crate) fn retain_checkout_control(
        &mut self,
        token: &Arc<ProofDataCustody>,
    ) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        let inserted = self.retain_observer(token.clone())?.is_some();
        let metadata = if inserted {
            self.allocations
                .record_owned_bytes(token.logical_owned_bytes(), &mut self.work)
                .map_err(|error| error.into_work_or(DemandError::InvalidRequest))
                .and_then(|()| self.work.verify(self.budget).map_err(DemandError::from))
        } else {
            Ok(())
        };
        metadata.and(
            self.reconcile_cancellation_registry()
                .map_err(DemandError::from),
        )
    }
    pub(crate) fn owns_native_checkout_token(&self, token: &Arc<ProofDataCustody>) -> bool {
        self.roots
            .observers
            .0
            .iter()
            .any(|entry| entry.2.is_some() && Arc::ptr_eq(&entry.0, token))
    }
    pub(crate) fn prepare_cancelled(
        &mut self,
        token: &CancellationToken,
    ) -> Result<PreparedCancelled, CancellationWaitError> {
        self.reconcile_cancellation_registry()?;
        if token.is_cancelled() {
            return token.prepare_cancelled_measured(
                &mut self.allocations,
                &mut self.work,
                self.budget,
                &mut Default::default(),
            );
        }
        let mut found = None;
        for (index, (current, _)) in self.roots.cancellations.0.iter().enumerate() {
            self.work.charge(&SCAN, &self.budget)?;
            if current.same_state(token) {
                found = Some(index);
                break;
            }
        }
        let index = match found {
            Some(index) => index,
            None => insert(
                &mut self.roots.cancellations,
                (token.clone(), Default::default()),
                &mut self.allocations,
                &mut self.work,
                self.budget,
            )
            .map_err(|error| match error {
                DemandError::Work(error) => CancellationWaitError::Work(error),
                _ => CancellationWaitError::AllocationFailed,
            })?,
        };
        token.prepare_cancelled_measured(
            &mut self.allocations,
            &mut self.work,
            self.budget,
            &mut self.roots.cancellations.0[index].1,
        )
    }
    pub(crate) fn finish_cancelled(
        &mut self,
        prepared: PreparedCancelled,
    ) -> Result<(), CancellationWaitError> {
        let reconciled = self
            .reconcile_cancellation_registry()
            .map_err(CancellationWaitError::from);
        let mut empty = CancellationWaitAdmission::default();
        let admission = match self
            .roots
            .cancellations
            .0
            .iter_mut()
            .find(|(token, _)| prepared.same_token(token))
        {
            Some((_, admission)) => admission,
            None => {
                assert_eq!(prepared.retained_waiter_bytes(), 0);
                &mut empty
            }
        };
        let finished = prepared.finish(
            &mut self.allocations,
            &mut self.work,
            self.budget,
            admission,
        );
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
        work.roots.observers.0[index].2 = Some(0);
        Ok(token.clone())
    }
}
