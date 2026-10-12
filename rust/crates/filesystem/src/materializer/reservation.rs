use super::*;
use crate::{DemandError, WorkCounters};
use acyclic_native_runtime::{NativeOperationKind, NativeReadMeter};
use std::sync::Arc;

/// Completion work withheld from the original ledger.
pub struct NativeWorkReservation {
    meter: NativeMaterializationWork,
    state: parking_lot::Mutex<Reservation>,
    metadata: Option<NativeAllocation>,
}
#[derive(Default)]
struct Reservation {
    remaining: WorkCounters,
    pending: WorkCounters,
    host_pending: WorkCounters,
}
impl std::fmt::Debug for NativeWorkReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeWorkReservation")
            .field("remaining", &self.remaining_work())
            .finish_non_exhaustive()
    }
}
impl NativeWorkReservation {
    pub(super) fn new(meter: NativeMaterializationWork, remaining: WorkCounters) -> Self {
        Self {
            meter,
            state: parking_lot::Mutex::new(Reservation {
                remaining,
                ..Reservation::default()
            }),
            metadata: None,
        }
    }
    /// Original ledger and attribution.
    pub const fn work(&self) -> &NativeMaterializationWork {
        &self.meter
    }
    /// Additive work still owned by this reservation.
    pub fn remaining_work(&self) -> WorkCounters {
        self.state.lock().remaining
    }
    fn available(state: &Reservation) -> Result<WorkCounters, DemandError> {
        Ok(state
            .pending
            .checked_add(state.host_pending)?
            .remaining(state.remaining)?)
    }
    fn failure(&self, error: DemandError) -> std::io::Error {
        self.meter
            .with_original_leaf(|state| state.record_native_io_failure(error))
    }
    fn relay(&self, error: std::io::Error) -> NativeTreeMaterializationError {
        self.meter.with_original_leaf(|state| {
            let Some(cause) = state.take_native_io_failure() else {
                return error.into();
            };
            let result = match &cause {
                DemandError::Work(error) => NativeTreeMaterializationError::Work(*error),
                DemandError::Cancelled => NativeTreeMaterializationError::Cancelled,
                DemandError::OriginalWorkPoisoned => {
                    NativeTreeMaterializationError::OriginalWorkPoisoned
                }
                DemandError::InvalidRequest => NativeTreeMaterializationError::InvalidLayout,
                _ => error.into(),
            };
            state.record_native_io_failure(cause);
            result
        })
    }
    fn host_failure(&self, error: DemandError) -> NativeTreeMaterializationError {
        self.relay(self.failure(error))
    }
    fn preflight(&self, maximum: WorkCounters) -> std::io::Result<()> {
        Self::available(&self.state.lock())
            .and_then(|available| maximum.remaining(available).map(|_| ()).map_err(Into::into))
            .map_err(|error| self.failure(error))
    }
    fn reserve(&self, maximum: WorkCounters) -> std::io::Result<()> {
        let mut state = self.state.lock();
        maximum
            .remaining(Self::available(&state).map_err(|error| self.failure(error))?)
            .map_err(|error| self.failure(error.into()))?;
        state.pending = state
            .pending
            .checked_add(maximum)
            .map_err(|error| self.failure(error.into()))?;
        Ok(())
    }
    fn cancel(&self, maximum: WorkCounters) {
        let mut state = self.state.lock();
        match maximum.remaining(state.pending) {
            Ok(pending) => state.pending = pending,
            Err(error) => self.meter.poison(error.into()),
        }
    }
    fn finish(
        &self,
        state: &mut Reservation,
        credited: WorkCounters,
        actual: WorkCounters,
    ) -> Result<(), DemandError> {
        let excess = credited.remaining(actual)?;
        state.remaining = credited.remaining(state.remaining)?;
        self.meter.state(|ledger| {
            let credited = ledger.record_native_work(credited);
            let excess = ledger.record(excess);
            credited.and(excess)
        })
    }
    /// Records completed work, preserving excess before returning an error.
    pub fn record_completed(
        &self,
        actual: WorkCounters,
    ) -> Result<(), NativeTreeMaterializationError> {
        if actual.allocation_operations != 0 || actual.peak_allocation_bytes != 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let mut state = self.state.lock();
        let available = Self::available(&state).map_err(native_materialization_work_error)?;
        self.finish(&mut state, actual.intersect_budget(available), actual)
            .map_err(native_materialization_work_error)?;
        actual.remaining(available).map(|_| ()).map_err(Into::into)
    }
    pub(crate) fn record_deferred(&mut self, actual: WorkCounters) {
        if let Err(error) = self.record_completed(actual) {
            self.meter.poison(error);
        }
    }
    fn begin(&self, state: &mut Reservation, actual: WorkCounters) -> Result<(), DemandError> {
        if io_work(actual) != actual {
            return Err(DemandError::InvalidRequest);
        }
        let remaining = actual.remaining(state.remaining)?;
        self.meter.state(|ledger| {
            ledger.with_resource_scope(None, |ledger| {
                let mut begin = || ledger.start_native_reserved_work(actual);
                match &self.meter.resource {
                    Some(resource) => resource.start_reserved_work(actual, &mut begin),
                    None => begin(),
                }
            })
        })?;
        state.remaining = remaining;
        Ok(())
    }
    fn begin_drop(&self, state: &mut Reservation, actual: WorkCounters) {
        if let Err(error) = self.begin(state, actual) {
            let actual = io_work(actual);
            let credited = actual.intersect_budget(state.remaining);
            self.meter.poison(native_materialization_work_error(error));
            if let Err(error) = self.finish(state, credited, actual) {
                self.failure(error);
            }
        }
    }
    pub(super) fn started_io(&mut self, actual: WorkCounters) -> Result<(), DemandError> {
        self.begin(&mut self.state.lock(), actual)
    }
    pub(crate) fn started_drop_io(&mut self, actual: WorkCounters) {
        self.begin_drop(&mut self.state.lock(), actual);
    }
    fn complete(&self, maximum: Option<WorkCounters>, actual: WorkCounters) -> std::io::Result<()> {
        let mut state = self.state.lock();
        let available = match maximum {
            Some(maximum) => {
                state.pending = maximum
                    .remaining(state.pending)
                    .map_err(|error| self.failure(error.into()))?;
                maximum
            }
            None => Self::available(&state).map_err(|error| self.failure(error))?,
        };
        let credited = actual.intersect_budget(available);
        let result = self.finish(&mut state, credited, actual);
        if credited != actual {
            self.meter
                .poison(NativeTreeMaterializationError::InvalidLayout);
            return Err(self.failure(DemandError::InvalidRequest));
        }
        result.map_err(|error| self.failure(error))
    }
    pub(crate) fn partition_work(
        &self,
        maximum: WorkCounters,
    ) -> Result<Self, NativeTreeMaterializationError> {
        if maximum.allocation_operations != 0 || maximum.peak_allocation_bytes != 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let mut state = self.state.lock();
        maximum.remaining(Self::available(&state).map_err(native_materialization_work_error)?)?;
        state.remaining = maximum.remaining(state.remaining)?;
        Ok(Self::new(self.meter.clone(), maximum))
    }
    pub(crate) fn absorb(
        &mut self,
        other: &mut Self,
    ) -> Result<(), NativeTreeMaterializationError> {
        let invocation = match (&self.meter.invocation, &other.meter.invocation) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        let capture = match (&self.meter.capture_origin, &other.meter.capture_origin) {
            (Some(a), Some(b)) => std::sync::Weak::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        if !self.meter.shares_allocation_context(&other.meter) || !invocation || !capture {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        self.state.get_mut().remaining = self
            .state
            .get_mut()
            .remaining
            .checked_add(other.state.get_mut().remaining)?;
        other.state.get_mut().remaining = WorkCounters::UNCHARGED;
        Ok(())
    }
    pub(crate) fn loan<'a>(
        &'a mut self,
        wait: Option<&'a crate::CancellationToken>,
    ) -> NativeCleanupLoan<'a> {
        NativeCleanupLoan { credit: self, wait }
    }
    pub(crate) fn into_runtime_credit(
        mut self,
    ) -> Result<Arc<Self>, NativeTreeMaterializationError> {
        self.meter = self.meter.allocation_work();
        self.metadata = Some(self.meter.claim(
            std::mem::size_of::<Self>() as u64,
            1,
            WorkCounters::UNCHARGED,
        )?);
        Ok(Arc::new(self))
    }
    pub(crate) fn runtime_meter(self: &Arc<Self>) -> Arc<dyn NativeReadMeter> {
        self.clone()
    }
}
impl Drop for NativeWorkReservation {
    fn drop(&mut self) {
        let unused = self.state.get_mut().remaining;
        if let Err(error) = self.meter.state(|state| state.refund_native_work(unused)) {
            self.meter.poison(native_materialization_work_error(error));
        }
    }
}
fn io_work(actual: WorkCounters) -> WorkCounters {
    WorkCounters {
        backend_read_operations: actual.backend_read_operations,
        backend_write_operations: actual.backend_write_operations,
        durability_operations: actual.durability_operations,
        ..WorkCounters::UNCHARGED
    }
}

pub(crate) struct NativeCleanupLoan<'a> {
    pub(crate) credit: &'a NativeWorkReservation,
    pub(crate) wait: Option<&'a crate::CancellationToken>,
}
impl NativeCleanupLoan<'_> {
    pub(crate) fn check_wait(&self) -> Result<(), NativeTreeMaterializationError> {
        self.wait.map_or(Ok(()), |wait| {
            wait.check()
                .map_err(|_| NativeTreeMaterializationError::Cancelled)
        })
    }
    pub(crate) fn admit_completed(
        &self,
        maximum: WorkCounters,
    ) -> Result<NativeWorkReservation, NativeTreeMaterializationError> {
        self.check_wait()?;
        self.credit.partition_work(maximum)
    }
    pub(crate) fn started_io(
        &self,
        actual: WorkCounters,
    ) -> Result<(), NativeTreeMaterializationError> {
        self.check_wait()?;
        self.credit
            .begin(&mut self.credit.state.lock(), actual)
            .map_err(|error| match error {
                DemandError::InvalidRequest => NativeTreeMaterializationError::InvalidLayout,
                error => native_materialization_work_error(error),
            })
    }
    pub(crate) fn record_completed(
        &self,
        actual: WorkCounters,
    ) -> Result<(), NativeTreeMaterializationError> {
        self.credit.record_completed(actual)
    }
}

impl NativeReadMeter for NativeWorkReservation {
    fn check(&self) -> std::io::Result<()> {
        Self::available(&self.state.lock())
            .map(|_| ())
            .map_err(|error| self.failure(error))
    }
    fn allocation_owner(&self) -> Option<Arc<dyn NativeReadMeter>> {
        let owner = self.meter.runtime_meter();
        Some(owner.allocation_owner().unwrap_or(owner))
    }
    fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> std::io::Result<()> {
        self.reserve(crate::demand::DemandWork::operation_credit(kind).delta(count))
    }
    fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64) {
        self.cancel(crate::demand::DemandWork::operation_credit(kind).delta(count));
    }
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> std::io::Result<()> {
        let actual = crate::demand::DemandWork::operation_credit(kind).delta(1);
        let mut state = self.state.lock();
        state.pending = actual
            .remaining(state.pending)
            .map_err(|error| self.failure(error.into()))?;
        if let Err(error) = self.begin(&mut state, actual) {
            let error = self.failure(error);
            self.begin_drop(&mut state, actual);
            return Err(error);
        }
        Ok(())
    }
    fn reserve_bytes(&self, kind: NativeOperationKind, bytes: u64) -> std::io::Result<()> {
        self.reserve(crate::demand::DemandWork::byte_credit(kind)?.delta(bytes))
    }
    fn cancel_reserved_bytes(&self, kind: NativeOperationKind, bytes: u64) {
        match crate::demand::DemandWork::byte_credit(kind) {
            Ok(credit) => self.cancel(credit.delta(bytes)),
            Err(error) => {
                self.failure(error.into());
            }
        }
    }
    fn record_reserved_bytes(
        &self,
        kind: NativeOperationKind,
        maximum: u64,
        actual: u64,
    ) -> std::io::Result<()> {
        let credit = crate::demand::DemandWork::byte_credit(kind)?;
        self.complete(Some(credit.delta(maximum)), credit.delta(actual))
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        if bytes == 0 {
            self.check()
        } else {
            Err(self.failure(DemandError::InvalidRequest))
        }
    }
    fn reserve_allocation(&self, _: u64) -> std::io::Result<()> {
        Err(self.failure(DemandError::InvalidRequest))
    }
    fn cancel_reserved_allocation(&self, _: u64) {
        self.meter
            .poison(NativeTreeMaterializationError::InvalidLayout);
    }
    fn record_reserved_allocation(&self, _: u64) -> std::io::Result<()> {
        Err(self.failure(DemandError::InvalidRequest))
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
        self.failure(DemandError::Allocation(error))
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        if bytes == 0 {
            self.check()
        } else {
            Err(std::io::ErrorKind::Unsupported.into())
        }
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        self.meter.with_original_leaf(|state| {
            let (allocations, counters, budget, _) = state.allocation_parts();
            if let Err(error) = allocations.record_owned_bytes(bytes, counters) {
                return (
                    0,
                    Err(state
                        .record_native_io_failure(error.into_work_or(DemandError::InvalidRequest))),
                );
            }
            let original = counters.verify(budget).map_err(DemandError::from);
            let scoped = self.meter.resource.as_ref().map_or(Ok(()), |resource| {
                resource.completed(WorkCounters::UNCHARGED, bytes)
            });
            (
                bytes,
                original
                    .and(scoped)
                    .map_err(|error| state.record_native_io_failure(error)),
            )
        })
    }
    fn release(&self, bytes: u64) {
        self.allocation_owner()
            .expect("original owner")
            .release(bytes);
    }
    fn admit_write_bytes(&self, bytes: usize) -> std::io::Result<()> {
        self.preflight(NativeResourceCredit::WriteBytes.delta(bytes as u64))
    }
    fn admit_bytes(&self, bytes: usize) -> std::io::Result<()> {
        self.preflight(NativeResourceCredit::ReadBytes.delta(bytes as u64))
    }
    fn record_read(&self, bytes: usize) -> std::io::Result<()> {
        self.complete(None, NativeResourceCredit::ReadBytes.delta(bytes as u64))
    }
    fn record_write(&self, bytes: usize) -> std::io::Result<()> {
        self.complete(None, NativeResourceCredit::WriteBytes.delta(bytes as u64))
    }
    fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        self.reserve(WorkCounters {
            bytes_encoded: encoded,
            bytes_copied: copied,
            bytes_hashed: hashed,
            ..WorkCounters::UNCHARGED
        })
    }
    fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        let actual = WorkCounters {
            bytes_encoded: encoded,
            bytes_copied: copied,
            bytes_hashed: hashed,
            ..WorkCounters::UNCHARGED
        };
        self.complete(Some(actual), actual)
    }
    fn admit_examination(&self, items: u64, copied: u64) -> std::io::Result<()> {
        self.reserve(WorkCounters {
            items_examined: items,
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        })
    }
    fn record_examination(&self, items: u64, copied: u64) -> std::io::Result<()> {
        let actual = WorkCounters {
            items_examined: items,
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        };
        self.complete(Some(actual), actual)
    }
    fn examine(&self, items: u64, copied: u64) -> std::io::Result<()> {
        self.admit_examination(items, copied)?;
        self.record_examination(items, copied)
    }
    fn admit_read(&self, bytes: usize) -> std::io::Result<()> {
        self.start_transfer(
            NativeResourceCredit::ReadOperations,
            NativeResourceCredit::ReadBytes,
            bytes as u64,
        )
    }
    fn admit_write(&self, bytes: usize) -> std::io::Result<()> {
        self.start_transfer(
            NativeResourceCredit::WriteOperations,
            NativeResourceCredit::WriteBytes,
            bytes as u64,
        )
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        self.start_transfer(
            NativeResourceCredit::DurabilityOperations,
            NativeResourceCredit::Copy,
            0,
        )
    }
    fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
        self.reserve(NativeResourceCredit::WriteOperations.delta(1))?;
        Ok(1)
    }
    fn cancel_cleanup_write(&self, reservation: u64) {
        self.cancel(NativeResourceCredit::WriteOperations.delta(reservation));
    }
    fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> {
        if reservation != 1 {
            return Err(self.failure(DemandError::InvalidRequest));
        }
        self.record_reserved_operation(NativeOperationKind::Write)
    }
}
impl NativeWorkReservation {
    fn start_transfer(
        &self,
        operation: NativeResourceCredit,
        bytes: NativeResourceCredit,
        maximum: u64,
    ) -> std::io::Result<()> {
        let actual = operation.delta(1);
        let limit = actual
            .checked_add(bytes.delta(maximum))
            .map_err(|error| self.failure(error.into()))?;
        let mut state = self.state.lock();
        limit
            .remaining(Self::available(&state).map_err(|error| self.failure(error))?)
            .map_err(|error| self.failure(error.into()))?;
        self.begin(&mut state, actual)
            .map_err(|error| self.failure(error))
    }
}

impl crate::native_host::NativeHostWork for &Arc<NativeWorkReservation> {
    type Error = NativeTreeMaterializationError;
    type Allocation = acyclic_native_runtime::ReadAllocation;
    type CleanupWrite =
        acyclic_native_runtime::NativeOperationReservation<Arc<dyn NativeReadMeter>>;
    fn io_error_kind(error: &Self::Error) -> Option<std::io::ErrorKind> {
        match error {
            Self::Error::Io(error) => Some(error.kind()),
            _ => None,
        }
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        Self::Error::AllocationFailed(error)
    }
    fn invalid_request() -> Self::Error {
        Self::Error::InvalidLayout
    }
    fn check(&self) -> Result<(), Self::Error> {
        NativeReadMeter::check(self.as_ref()).map_err(|error| self.relay(error))
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        let mut state = self.state.lock();
        delta.remaining(
            NativeWorkReservation::available(&state).map_err(|error| self.host_failure(error))?,
        )?;
        state.host_pending = state.host_pending.checked_add(delta)?;
        Ok(())
    }
    fn record(&mut self, actual: WorkCounters) -> Result<(), Self::Error> {
        let mut state = self.state.lock();
        state.host_pending = actual
            .remaining(state.host_pending)
            .map_err(|error| self.host_failure(error.into()))?;
        let starts = io_work(actual);
        self.begin(&mut state, starts)
            .map_err(|error| self.host_failure(error))?;
        let completed = starts.remaining(actual)?;
        self.finish(&mut state, completed, completed)
            .map_err(|error| self.host_failure(error))
    }
    fn started_io(&mut self, actual: WorkCounters) -> Result<(), Self::Error> {
        let mut state = self.state.lock();
        match actual.remaining(state.host_pending) {
            Ok(pending) => state.host_pending = pending,
            Err(_) => {
                actual.remaining(
                    NativeWorkReservation::available(&state)
                        .map_err(|error| self.host_failure(error))?,
                )?;
            }
        }
        self.begin(&mut state, actual)
            .map_err(|error| self.host_failure(error))
    }
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error> {
        if bytes != 0 {
            return Err(self.host_failure(DemandError::InvalidRequest));
        }
        Ok(acyclic_native_runtime::ReadAllocation::empty(
            &self.allocation_owner().expect("original owner"),
        ))
    }
    fn reserve_opaque_decode(&mut self, _: u64) -> Result<Self::Allocation, Self::Error> {
        Err(self.host_failure(DemandError::InvalidRequest))
    }
    fn reconcile_allocation(
        &mut self,
        allocation: &mut Self::Allocation,
        actual: u64,
    ) -> Result<(), Self::Error> {
        allocation
            .reconcile_capacity_with_meter(
                usize::try_from(actual).map_err(|_| crate::WorkError::Overflow)?,
                self.as_ref(),
            )
            .map_err(|error| self.relay(error))
    }
    fn release(&mut self, allocation: Self::Allocation) -> Result<(), Self::Error> {
        drop(allocation);
        Ok(())
    }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        acyclic_native_runtime::NativeOperationReservation::new(
            self.runtime_meter(),
            NativeOperationKind::Write,
            1,
        )
        .map_err(|error| self.relay(error))
    }
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) {
        drop(reservation);
    }
    fn attempt_cleanup_write<T>(
        &mut self,
        mut reservation: Self::CleanupWrite,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>) {
        let (result, recorded) = reservation.attempt(operation);
        (result, recorded.map_err(|error| self.relay(error)))
    }
}
