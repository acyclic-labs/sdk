use super::*;
use crate::{DemandError, WorkCounters};
use std::sync::Arc;

/// Completion work withheld from the existing demand ledger.
pub struct NativeWorkReservation {
    meter: NativeMaterializationWork,
    state: parking_lot::Mutex<NativeReservationState>,
    _metadata: Option<NativeAllocation>,
}
struct NativeReservationState {
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
            state: parking_lot::Mutex::new(NativeReservationState {
                remaining,
                pending: WorkCounters::UNCHARGED,
                host_pending: WorkCounters::UNCHARGED,
            }),
            _metadata: None,
        }
    }
    /// Original accounting context.
    pub const fn work(&self) -> &NativeMaterializationWork {
        &self.meter
    }
    /// Still-owned additive credit.
    pub fn remaining_work(&self) -> WorkCounters {
        self.state.lock().remaining
    }
    fn available(state: &NativeReservationState) -> Result<WorkCounters, DemandError> {
        Ok(state
            .pending
            .checked_add(state.host_pending)?
            .remaining(state.remaining)?)
    }
    pub(crate) fn absorb(
        &mut self,
        other: &mut Self,
    ) -> Result<(), NativeTreeMaterializationError> {
        let same_invocation = match (&self.meter.invocation, &other.meter.invocation) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        let same_capture = match (&self.meter.capture_origin, &other.meter.capture_origin) {
            (Some(a), Some(b)) => std::sync::Weak::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        if !self.meter.shares_allocation_context(&other.meter) || !same_invocation || !same_capture
        {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let remaining = self
            .state
            .get_mut()
            .remaining
            .checked_add(other.state.get_mut().remaining)?;
        self.state.get_mut().remaining = remaining;
        other.state.get_mut().remaining = WorkCounters::UNCHARGED;
        Ok(())
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
    fn finish(
        &self,
        state: &mut NativeReservationState,
        credited: WorkCounters,
        actual: WorkCounters,
    ) -> Result<(), DemandError> {
        let excess = credited.remaining(actual)?;
        state.remaining = credited.remaining(state.remaining)?;
        self.meter.state(|demand| {
            let credited = demand.record_native_work(credited);
            let excess = demand.record(excess);
            credited.and(excess)
        })
    }
    /// Records completed work before reporting an overlarge prefix.
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
            self.meter.state(|demand| {
                demand.record_native_io_failure(match error {
                    NativeTreeMaterializationError::Work(error) => DemandError::Work(error),
                    NativeTreeMaterializationError::Cancelled => DemandError::Cancelled,
                    NativeTreeMaterializationError::OriginalWorkPoisoned => {
                        DemandError::OriginalWorkPoisoned
                    }
                    _ => DemandError::InvalidRequest,
                });
                demand.poison_native_original_work();
            });
        }
    }
    fn begin(
        &self,
        state: &mut NativeReservationState,
        actual: WorkCounters,
    ) -> Result<(), DemandError> {
        let starts = io_work(actual);
        if starts != actual {
            return Err(DemandError::InvalidRequest);
        }
        let remaining = actual.remaining(state.remaining)?;
        self.meter.state(|demand| {
            demand.with_resource_scope(None, |demand| {
                let mut begin = || demand.start_native_reserved_work(actual);
                match &self.meter.resource {
                    Some(resource) => resource.start_reserved_work(actual, &mut begin),
                    None => begin(),
                }
            })
        })?;
        state.remaining = remaining;
        Ok(())
    }
    pub(super) fn started_io(&mut self, actual: WorkCounters) -> Result<(), DemandError> {
        self.begin(&mut self.state.lock(), actual)
    }
    pub(crate) fn started_drop_io(&mut self, actual: WorkCounters) {
        self.begin_drop(&mut self.state.lock(), actual);
    }
    fn begin_drop(&self, state: &mut NativeReservationState, actual: WorkCounters) {
        if let Err(error) = self.begin(state, actual) {
            let actual = io_work(actual);
            let credited = actual.intersect_budget(state.remaining);
            self.meter.state(|demand| {
                demand.record_native_io_failure(error);
                demand.poison_native_original_work();
            });
            if let Err(error) = self.finish(state, credited, actual) {
                self.failure(error);
            }
        }
    }
    pub(crate) fn loan<'a>(
        &'a mut self,
        wait: Option<&'a crate::CancellationToken>,
    ) -> NativeCleanupLoan<'a> {
        NativeCleanupLoan { credit: self, wait }
    }
    pub(crate) fn into_runtime_credit(
        mut credit: Self,
    ) -> Result<Arc<Self>, NativeTreeMaterializationError> {
        credit.meter = credit.meter.allocation_work();
        credit._metadata = Some(credit.meter.claim(
            std::mem::size_of::<Self>() as u64,
            1,
            WorkCounters::UNCHARGED,
        )?);
        Ok(Arc::new(credit))
    }
    pub(crate) fn runtime_meter(
        self: &Arc<Self>,
    ) -> Arc<dyn acyclic_native_runtime::NativeReadMeter> {
        self.clone()
    }
    fn failure(&self, error: DemandError) -> std::io::Error {
        self.meter
            .with_original_leaf(|demand| demand.record_native_io_failure(error))
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
            Err(error) => self.meter.state(|demand| {
                demand.record_native_io_failure(error.into());
                demand.poison_native_original_work();
            }),
        }
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
        let overrun = credited != actual;
        if overrun {
            self.failure(DemandError::InvalidRequest);
        }
        let result = self.finish(&mut state, credited, actual);
        if overrun {
            self.meter
                .state(|demand| demand.poison_native_original_work());
            return Err(self.failure(DemandError::InvalidRequest));
        }
        result.map_err(|error| self.failure(error))
    }
}
impl Drop for NativeWorkReservation {
    fn drop(&mut self) {
        let unused = self.state.get_mut().remaining;
        if unused != WorkCounters::UNCHARGED {
            self.meter.state(|demand| {
                if let Err(error) = demand.refund_native_work(unused) {
                    demand.record_native_io_failure(error);
                    demand.poison_native_original_work();
                }
            });
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

impl acyclic_native_runtime::NativeReadMeter for NativeWorkReservation {
    fn check(&self) -> std::io::Result<()> {
        Self::available(&self.state.lock())
            .map(|_| ())
            .map_err(|_| std::io::ErrorKind::Other.into())
    }
    fn allocation_owner(&self) -> Option<Arc<dyn acyclic_native_runtime::NativeReadMeter>> {
        let owner = self.meter.runtime_meter();
        Some(owner.allocation_owner().unwrap_or(owner))
    }
    fn reserve_operations(
        &self,
        kind: acyclic_native_runtime::NativeOperationKind,
        count: u64,
    ) -> std::io::Result<()> {
        self.reserve(crate::demand::DemandWork::operation_credit(kind).delta(count))
    }
    fn cancel_reserved_operations(
        &self,
        kind: acyclic_native_runtime::NativeOperationKind,
        count: u64,
    ) {
        self.cancel(crate::demand::DemandWork::operation_credit(kind).delta(count));
    }
    fn record_reserved_operation(
        &self,
        kind: acyclic_native_runtime::NativeOperationKind,
    ) -> std::io::Result<()> {
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
    fn reserve_bytes(
        &self,
        kind: acyclic_native_runtime::NativeOperationKind,
        maximum: u64,
    ) -> std::io::Result<()> {
        self.reserve(crate::demand::DemandWork::byte_credit(kind)?.delta(maximum))
    }
    fn cancel_reserved_bytes(
        &self,
        kind: acyclic_native_runtime::NativeOperationKind,
        maximum: u64,
    ) {
        match crate::demand::DemandWork::byte_credit(kind) {
            Ok(credit) => self.cancel(credit.delta(maximum)),
            Err(error) => {
                self.failure(DemandError::Io(error));
            }
        }
    }
    fn record_reserved_bytes(
        &self,
        kind: acyclic_native_runtime::NativeOperationKind,
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
    fn reserve_allocation(&self, _bytes: u64) -> std::io::Result<()> {
        Err(self.failure(DemandError::InvalidRequest))
    }
    fn cancel_reserved_allocation(&self, _bytes: u64) {
        self.meter.state(|demand| {
            demand.record_native_io_failure(DemandError::InvalidRequest);
            demand.poison_native_original_work();
        });
    }
    fn record_reserved_allocation(&self, _bytes: u64) -> std::io::Result<()> {
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
        self.meter.with_original_leaf(|demand| {
            let (allocations, counters, budget, _) = demand.allocation_parts();
            if let Err(error) = allocations.record_owned_bytes(bytes, counters) {
                return (
                    0,
                    Err(demand
                        .record_native_io_failure(error.into_work_or(DemandError::InvalidRequest))),
                );
            }
            let original = counters.verify(budget).map_err(DemandError::from);
            let resource = self.meter.resource.as_ref().map_or(Ok(()), |resource| {
                resource.completed(WorkCounters::UNCHARGED, bytes)
            });
            (
                bytes,
                original
                    .and(resource)
                    .map_err(|error| demand.record_native_io_failure(error)),
            )
        })
    }
    fn release(&self, bytes: u64) {
        self.allocation_owner()
            .expect("original allocation owner")
            .release(bytes);
    }
    fn admit_write_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.preflight(NativeResourceCredit::WriteBytes.delta(maximum as u64))
    }
    fn admit_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.preflight(NativeResourceCredit::ReadBytes.delta(maximum as u64))
    }
    fn admit_operations(&self, count: u64) -> std::io::Result<()> {
        self.preflight(NativeResourceCredit::ReadOperations.delta(count))
    }
    fn admit_write_operations(&self, count: u64) -> std::io::Result<()> {
        self.preflight(NativeResourceCredit::WriteOperations.delta(count))
    }
    fn record_operations(&self, count: u64) -> std::io::Result<()> {
        self.complete(None, NativeResourceCredit::ReadOperations.delta(count))
    }
    fn record_read(&self, bytes: usize) -> std::io::Result<()> {
        self.complete(None, NativeResourceCredit::ReadBytes.delta(bytes as u64))
    }
    fn record_write(&self, bytes: usize) -> std::io::Result<()> {
        self.complete(None, NativeResourceCredit::WriteBytes.delta(bytes as u64))
    }
    fn record_write_operations(&self, count: u64) -> std::io::Result<()> {
        self.complete(None, NativeResourceCredit::WriteOperations.delta(count))
    }
    fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        self.reserve(WorkCounters {
            bytes_encoded: encoded,
            bytes_copied: copied,
            bytes_hashed: hashed,
            ..WorkCounters::UNCHARGED
        })
    }
    fn admit_examination(&self, items: u64, copied: u64) -> std::io::Result<()> {
        self.reserve(WorkCounters {
            items_examined: items,
            bytes_copied: copied,
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
    fn record_examination(&self, items: u64, copied: u64) -> std::io::Result<()> {
        let actual = WorkCounters {
            items_examined: items,
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        };
        self.complete(Some(actual), actual)
    }
    fn examine(&self, items: u64, copied: u64) -> std::io::Result<()> {
        let actual = WorkCounters {
            items_examined: items,
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        };
        self.reserve(actual)?;
        self.complete(Some(actual), actual)
    }
    fn admit_read(&self, maximum: usize) -> std::io::Result<()> {
        let actual = NativeResourceCredit::ReadOperations.delta(1);
        let maximum = actual
            .checked_add(NativeResourceCredit::ReadBytes.delta(maximum as u64))
            .map_err(|error| self.failure(error.into()))?;
        let mut state = self.state.lock();
        maximum
            .remaining(Self::available(&state).map_err(|error| self.failure(error))?)
            .map_err(|error| self.failure(error.into()))?;
        self.begin(&mut state, actual)
            .map_err(|error| self.failure(error))
    }
    fn admit_write(&self, maximum: usize) -> std::io::Result<()> {
        let actual = NativeResourceCredit::WriteOperations.delta(1);
        let maximum = actual
            .checked_add(NativeResourceCredit::WriteBytes.delta(maximum as u64))
            .map_err(|error| self.failure(error.into()))?;
        let mut state = self.state.lock();
        maximum
            .remaining(Self::available(&state).map_err(|error| self.failure(error))?)
            .map_err(|error| self.failure(error.into()))?;
        self.begin(&mut state, actual)
            .map_err(|error| self.failure(error))
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        let actual = NativeResourceCredit::DurabilityOperations.delta(1);
        let mut state = self.state.lock();
        actual
            .remaining(Self::available(&state).map_err(|error| self.failure(error))?)
            .map_err(|error| self.failure(error.into()))?;
        self.begin(&mut state, actual)
            .map_err(|error| self.failure(error))
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
        self.record_reserved_operation(acyclic_native_runtime::NativeOperationKind::Write)
    }
}

impl NativeWorkReservation {
    fn relay(&self, error: std::io::Error) -> NativeTreeMaterializationError {
        self.meter.with_original_leaf(|demand| {
            let Some(cause) = demand.take_native_io_failure() else {
                return NativeTreeMaterializationError::Io(error);
            };
            let result = match &cause {
                DemandError::Work(error) => NativeTreeMaterializationError::Work(*error),
                DemandError::Cancelled => NativeTreeMaterializationError::Cancelled,
                DemandError::OriginalWorkPoisoned => {
                    NativeTreeMaterializationError::OriginalWorkPoisoned
                }
                DemandError::InvalidRequest => NativeTreeMaterializationError::InvalidLayout,
                _ => NativeTreeMaterializationError::Io(error),
            };
            demand.record_native_io_failure(cause);
            result
        })
    }
    fn host_failure(&self, error: DemandError) -> NativeTreeMaterializationError {
        self.relay(self.failure(error))
    }
}
impl crate::native_host::NativeHostWork for &Arc<NativeWorkReservation> {
    type Error = NativeTreeMaterializationError;
    type Allocation = acyclic_native_runtime::ReadAllocation;
    type CleanupWrite = acyclic_native_runtime::NativeOperationReservation<
        Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    >;
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
        acyclic_native_runtime::NativeReadMeter::check(self.as_ref())
            .map_err(|_| Self::Error::OriginalWorkPoisoned)
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
        let owner = acyclic_native_runtime::NativeReadMeter::allocation_owner(self.as_ref())
            .expect("original allocation owner");
        Ok(acyclic_native_runtime::ReadAllocation::empty(&owner))
    }
    fn reserve_opaque_decode(&mut self, _maximum: u64) -> Result<Self::Allocation, Self::Error> {
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
            acyclic_native_runtime::NativeOperationKind::Write,
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

pub(crate) struct NativeCleanupWrite {
    pub(super) meter: NativeMaterializationWork,
    pub(super) additional_resource: Option<Arc<NativeResourceAllowance>>,
    pub(super) reserved: bool,
}
impl std::fmt::Debug for NativeCleanupWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeCleanupWrite").finish_non_exhaustive()
    }
}
impl NativeCleanupWrite {
    pub(crate) fn bind_current_work(
        &mut self,
        current: &NativeMaterializationWork,
    ) -> Result<(), NativeTreeMaterializationError> {
        if !self.reserved || !self.meter.shares_ledger(current) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let Some(resource) = &current.resource else {
            return Ok(());
        };
        if self
            .additional_resource
            .as_ref()
            .is_some_and(|bound| Arc::ptr_eq(bound, resource))
        {
            return Ok(());
        }
        if self.additional_resource.is_some() {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        self.meter
            .state(|demand| {
                demand.check()?;
                resource.reserve(
                    NativeResourceCredit::WriteOperations.delta(1),
                    0,
                    self.meter.resource.as_deref(),
                    &mut || Ok(()),
                )
            })
            .map_err(native_materialization_work_error)?;
        self.additional_resource = Some(resource.clone());
        Ok(())
    }
    pub(crate) fn attempt<T>(
        mut self,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), NativeTreeMaterializationError>) {
        self.reserved = false;
        let recorded = self
            .meter
            .state(|demand| {
                let original = demand.complete(NativeResourceCredit::WriteOperations, 1, 1);
                let current = self
                    .additional_resource
                    .as_ref()
                    .map_or(Ok(()), |resource| {
                        resource.complete(
                            NativeResourceCredit::WriteOperations.delta(1),
                            NativeResourceCredit::WriteOperations.delta(1),
                            self.meter.resource.as_deref(),
                        )
                    });
                original.and(current)
            })
            .map_err(native_materialization_work_error);
        (operation(), recorded)
    }
}
impl Drop for NativeCleanupWrite {
    fn drop(&mut self) {
        if self.reserved {
            self.meter.state(|demand| {
                demand
                    .refund(NativeResourceCredit::WriteOperations, 1)
                    .expect("balanced original credit");
                if let Some(resource) = &self.additional_resource {
                    if let Err(error) = resource.refund(
                        NativeResourceCredit::WriteOperations.delta(1),
                        self.meter.resource.as_deref(),
                    ) {
                        demand.record_native_io_failure(error);
                        demand.poison_native_original_work();
                    }
                }
            });
        }
    }
}
