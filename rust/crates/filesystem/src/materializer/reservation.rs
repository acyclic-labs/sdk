use super::*;
use crate::{DemandError, WorkCounters};
use std::sync::Arc;

/// Completion work withheld from its original SDK ledger.
#[derive(Debug)]
pub struct NativeWorkReservation {
    pub(super) meter: NativeMaterializationWork,
    pub(super) state: parking_lot::Mutex<Reservation>,
    metadata: Option<NativeAllocation>,
}
#[derive(Debug, Default)]
pub(super) struct Reservation {
    pub(super) remaining: WorkCounters,
    pub(super) pending: WorkCounters,
    pub(super) host_pending: WorkCounters,
}
impl Reservation {
    pub(super) fn available(&self) -> Result<WorkCounters, DemandError> {
        Ok(self
            .pending
            .checked_add(self.host_pending)?
            .remaining(self.remaining)?)
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
    /// Unspent additive work.
    pub fn remaining_work(&self) -> WorkCounters {
        self.state.lock().remaining
    }
    pub(super) fn failure(&self, error: DemandError) -> std::io::Error {
        self.meter
            .with_original_leaf(|state| state.record_native_io_failure(error))
    }
    pub(super) fn edit(
        &self,
        operation: impl FnOnce(&mut Reservation) -> Result<(), DemandError>,
    ) -> std::io::Result<()> {
        operation(&mut self.state.lock()).map_err(|error| self.failure(error))
    }
    pub(super) fn preflight(&self, maximum: WorkCounters) -> std::io::Result<()> {
        self.edit(|state| {
            maximum
                .remaining(state.available()?)
                .map(|_| ())
                .map_err(Into::into)
        })
    }
    pub(super) fn reserve(&self, maximum: WorkCounters) -> std::io::Result<()> {
        self.edit(|state| {
            maximum.remaining(state.available()?)?;
            state.pending = state.pending.checked_add(maximum)?;
            Ok(())
        })
    }
    pub(super) fn cancel(&self, maximum: WorkCounters) {
        let mut state = self.state.lock();
        match maximum.remaining(state.pending) {
            Ok(pending) => state.pending = pending,
            Err(error) => self.meter.poison(error.into()),
        }
    }
    // Preserve actual excess even if recording the reserved prefix fails.
    pub(super) fn finish(
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
    /// Records actual completed work before reporting an exhausted allowance.
    pub fn record_completed(
        &self,
        actual: WorkCounters,
    ) -> Result<(), NativeTreeMaterializationError> {
        if actual.allocation_operations != 0 || actual.peak_allocation_bytes != 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let mut state = self.state.lock();
        let available = state
            .available()
            .map_err(native_materialization_work_error)?;
        self.finish(&mut state, actual.intersect_budget(available), actual)
            .map_err(native_materialization_work_error)?;
        actual.remaining(available).map(|_| ()).map_err(Into::into)
    }
    pub(crate) fn record_deferred(&mut self, actual: WorkCounters) {
        if let Err(error) = self.record_completed(actual) {
            self.meter.poison(error);
        }
    }
    pub(super) fn begin(
        &self,
        state: &mut Reservation,
        actual: WorkCounters,
    ) -> Result<(), DemandError> {
        if io_work(actual) != actual {
            return Err(DemandError::InvalidRequest);
        }
        let remaining = actual.remaining(state.remaining)?;
        self.meter.state(|ledger| {
            ledger.with_resource_scope(None, |ledger| {
                let mut begin = || ledger.start_native_reserved_work(actual);
                match &self.meter.original.resource {
                    Some(resource) => resource.start_reserved_work(actual, &mut begin),
                    None => begin(),
                }
            })
        })?;
        state.remaining = remaining;
        Ok(())
    }
    pub(super) fn begin_drop(&self, state: &mut Reservation, actual: WorkCounters) {
        if let Err(error) = self.begin(state, actual) {
            self.meter.poison(native_materialization_work_error(error));
            let actual = io_work(actual);
            if let Err(error) = self.finish(state, actual.intersect_budget(state.remaining), actual)
            {
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
    pub(super) fn complete(
        &self,
        maximum: Option<WorkCounters>,
        actual: WorkCounters,
    ) -> std::io::Result<()> {
        self.edit(|state| {
            let available = match maximum {
                Some(maximum) => {
                    state.pending = maximum.remaining(state.pending)?;
                    maximum
                }
                None => state.available()?,
            };
            let credited = actual.intersect_budget(available);
            let recorded = self.finish(state, credited, actual);
            if credited != actual {
                self.meter
                    .poison(NativeTreeMaterializationError::InvalidLayout);
                return Err(DemandError::InvalidRequest);
            }
            recorded
        })
    }
    pub(crate) fn partition_work(
        &self,
        maximum: WorkCounters,
    ) -> Result<Self, NativeTreeMaterializationError> {
        if maximum.allocation_operations != 0 || maximum.peak_allocation_bytes != 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let mut state = self.state.lock();
        maximum.remaining(
            state
                .available()
                .map_err(native_materialization_work_error)?,
        )?;
        state.remaining = maximum.remaining(state.remaining)?;
        Ok(Self::new(self.meter.clone(), maximum))
    }
    pub(crate) fn absorb(
        &mut self,
        other: &mut Self,
    ) -> Result<(), NativeTreeMaterializationError> {
        let invocation = match (
            &self.meter.original.invocation,
            &other.meter.original.invocation,
        ) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        let capture = match (
            &self.meter.original.capture_origin,
            &other.meter.original.capture_origin,
        ) {
            (Some(a), Some(b)) => std::sync::Weak::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        if !self.meter.shares_allocation_context(&other.meter) || !invocation || !capture {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let remaining = &mut self.state.get_mut().remaining;
        let incoming = &mut other.state.get_mut().remaining;
        *remaining = remaining.checked_add(*incoming)?;
        *incoming = WorkCounters::UNCHARGED;
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
    pub(crate) fn runtime_meter(
        self: &Arc<Self>,
    ) -> Arc<dyn acyclic_native_runtime::NativeReadMeter> {
        self.clone()
    }
    pub(super) fn start_transfer(
        &self,
        operation: NativeResourceCredit,
        bytes: NativeResourceCredit,
        maximum: u64,
    ) -> std::io::Result<()> {
        self.edit(|state| {
            let actual = operation.delta(1);
            actual
                .checked_add(bytes.delta(maximum))?
                .remaining(state.available()?)?;
            self.begin(state, actual)
        })
    }
}
impl Drop for NativeWorkReservation {
    fn drop(&mut self) {
        if let Err(error) = self
            .meter
            .state(|state| state.refund_native_work(self.state.get_mut().remaining))
        {
            self.meter.poison(native_materialization_work_error(error));
        }
    }
}
pub(super) fn io_work(actual: WorkCounters) -> WorkCounters {
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
