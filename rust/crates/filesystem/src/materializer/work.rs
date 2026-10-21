use super::*;
use crate::{WorkBudget, WorkCounters};
use std::sync::Arc;

/// Original SDK demand context with its current cancellation and resource scope.
#[derive(Clone)]
pub struct NativeMaterializationWork {
    pub(crate) state: Arc<crate::demand::NativeDemandReadMeter>,
    pub(crate) resource: Option<Arc<NativeResourceAllowance>>,
    pub(crate) capture_origin: Option<std::sync::Weak<crate::demand::CaptureTransientCustody>>,
    pub(crate) invocation: Option<Arc<NativeInvocationView>>,
    #[cfg(feature = "native-mount")]
    pub(crate) execution: Option<Arc<NativeSourceExecutionView>>,
    cancellation: crate::CancellationToken,
    recheck_items: bool,
}
impl std::fmt::Debug for NativeMaterializationWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeMaterializationWork")
            .finish_non_exhaustive()
    }
}
macro_rules! demand_calls {
    ($($(#[$doc:meta])* $vis:vis $name:ident($argument:ident: $kind:ty) => $method:ident;)*) => {$ (
        $(#[$doc])*
        $vis fn $name(&self, $argument: $kind) -> Result<(), NativeTreeMaterializationError> {
            self.state(|state| state.$method($argument)).map_err(native_materialization_work_error)
        }
    )*};
}
impl NativeMaterializationWork {
    /// Registers a cancellation waiter funded by this original ledger.
    pub fn cancellation_wait(
        &self,
    ) -> Result<crate::Cancelled<'static>, NativeTreeMaterializationError> {
        self.cancellation
            .prepare_cancelled_with_native_work(self)
            .map_err(|cause| match cause {
                crate::cancellation::CancellationWaitError::Work(cause) => cause.into(),
                crate::cancellation::CancellationWaitError::Native(cause) => cause,
            })
    }

    /// Complete Arc allocation retained by the original shared ledger.
    pub const fn accounting_state_bytes() -> u64 {
        const {
            match crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<
                crate::demand::NativeDemandReadMeter,
            >()) {
                Ok(bytes) => bytes as u64,
                Err(_) => panic!("original ledger Arc layout overflow"),
            }
        }
    }
    /// Admits the original ledger before sharing it.
    pub fn new(
        budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, (NativeTreeMaterializationError, Box<WorkCounters>)> {
        let mut state = crate::demand::DemandWork::new(budget, cancellation);
        state
            .claim_allocation(
                Self::accounting_state_bytes(),
                1,
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(|error| {
                (
                    native_materialization_work_error(error),
                    Box::new(state.work),
                )
            })?;
        Ok(Self {
            cancellation: cancellation.clone(),
            state: Arc::new(crate::demand::NativeDemandReadMeter::new(state)),
            resource: None,
            recheck_items: false,
            capture_origin: None,
            invocation: None,
            #[cfg(feature = "native-mount")]
            execution: None,
        })
    }
    #[cfg(feature = "native-mount")]
    pub(crate) fn from_meter(state: Arc<crate::demand::NativeDemandReadMeter>) -> Self {
        let cancellation = state.lock().cancellation().clone();
        Self {
            state,
            resource: None,
            recheck_items: false,
            capture_origin: None,
            invocation: None,
            #[cfg(feature = "native-mount")]
            execution: None,
            cancellation,
        }
    }
    /// Current invocation cancellation.
    pub fn cancellation(&self) -> &crate::CancellationToken {
        self.invocation
            .as_ref()
            .map_or(&self.cancellation, |view| &view.cancellation)
    }
    // The leaf stays locked through operation. Do not re-enter demand or drop
    // owners whose destruction releases through this leaf.
    pub(crate) fn state<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.state.with_original_leaf(|state| {
            state.scope(
                |work| &mut work.native_invocation,
                self.invocation.as_ref().map(Arc::downgrade),
                |state| {
                    state.scope(
                        |work| &mut work.native_resource,
                        self.resource.as_ref().map(Arc::downgrade),
                        |state| {
                            state.scope(
                                |work| &mut work.capture_custody,
                                None,
                                |state| {
                                    state.scope(
                                        |work| &mut work.recheck_items,
                                        self.recheck_items,
                                        operation,
                                    )
                                },
                            )
                        },
                    )
                },
            )
        })
    }
    pub(crate) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.state(|state| {
            state.scope(
                |work| &mut work.capture_custody,
                self.capture_origin.clone(),
                operation,
            )
        })
    }
    pub(super) fn poison(&self, error: NativeTreeMaterializationError) {
        self.state(|state| {
            state.record_native_io_failure(crate::native_name::original_name_error(error));
            state.poison_native_original_work();
        });
    }
    /// Whether contexts share the original ledger.
    pub fn shares_ledger(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
    pub(crate) fn shares_resource_allowance(&self, other: &Self) -> bool {
        match (&self.resource, &other.resource) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }
    /// Whether contexts attribute allocations to the same resource.
    pub fn shares_allocation_context(&self, other: &Self) -> bool {
        self.shares_ledger(other) && self.shares_resource_allowance(other)
    }
    pub(crate) fn allocation_work(&self) -> Self {
        Self {
            recheck_items: false,
            #[cfg(feature = "native-mount")]
            execution: None,
            ..self.clone()
        }
    }
    /// Original or resource ceiling.
    pub fn budget(&self) -> WorkBudget {
        self.resource.as_ref().map_or_else(
            || self.state(|state| state.original_budget()),
            |resource| resource.budget,
        )
    }
    /// Existing runtime meter for this context.
    pub fn runtime_meter(&self) -> Arc<dyn acyclic_native_runtime::NativeReadMeter> {
        #[cfg(feature = "native-mount")]
        if let Some(execution) = &self.execution {
            return execution.clone();
        }
        if let Some(invocation) = &self.invocation
            && invocation.owns_resource(self.resource.as_ref())
        {
            return invocation.clone();
        }
        self.resource.as_ref().map_or_else(
            || self.state.clone() as Arc<dyn acyclic_native_runtime::NativeReadMeter>,
            |resource| resource.clone(),
        )
    }
    /// Original typed runtime failure.
    pub fn runtime_error(&self, error: std::io::Error) -> NativeTreeMaterializationError {
        self.state(|state| state.take_native_io_failure())
            .map_or_else(
                || NativeTreeMaterializationError::from_runtime_error(error),
                native_materialization_work_error,
            )
    }
    /// Remaining finite allowance.
    pub fn remaining(&self) -> Result<WorkBudget, NativeTreeMaterializationError> {
        self.state(|state| state.check().and_then(|()| state.remaining()))
            .map_err(native_materialization_work_error)
    }
    /// Reuses item admission while validating an already admitted operation.
    pub fn recheck_admitted(&self) -> Self {
        Self {
            recheck_items: true,
            ..self.clone()
        }
    }
    fn item_charge(&self, mut delta: WorkCounters) -> WorkCounters {
        if self.recheck_items {
            delta.items_examined = 0;
        }
        delta
    }
    /// Actual invocation work.
    pub fn receipt(&self) -> WorkCounters {
        self.state(|state| state.receipt())
    }
    /// Retains the original durable work prefix once.
    pub fn set_original_work(&self, work: WorkCounters) {
        self.state(|state| {
            state.original_work.get_or_insert(work);
        });
    }
    /// Durable prefix plus current invocation work.
    pub fn journal_work(&self) -> Result<WorkCounters, NativeTreeMaterializationError> {
        self.state(|state| {
            state
                .original_work
                .unwrap_or_default()
                .checked_add(state.receipt())
        })
        .map_err(Into::into)
    }
    /// Admits work before dispatch.
    pub fn admit(&self, delta: WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.state(|state| state.admit(delta))
            .map_err(native_materialization_work_error)
    }
    /// Charges work starting now.
    pub fn charge(&self, delta: WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.state(|state| state.charge(delta))
            .map_err(native_materialization_work_error)
    }
    /// Records work completed by a nested operation.
    pub fn spent(&self, delta: WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.state(|state| state.spent(delta))
            .map_err(native_materialization_work_error)
    }
    pub(crate) fn record_admitted(
        &self,
        delta: WorkCounters,
    ) -> Result<(), NativeTreeMaterializationError> {
        self.state(|state| state.record(delta))
            .map_err(native_materialization_work_error)
    }
    demand_calls! {
        /// Admits retained bytes.
        pub admit_retained(bytes: u64) => admit_retained;
    }
    /// Claims retained capacity on the existing ledger.
    pub fn claim(
        &self,
        bytes: u64,
        operations: u64,
        delta: WorkCounters,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.state(|state| state.claim_allocation(bytes, operations, delta))
            .map_err(native_materialization_work_error)?;
        Ok(NativeAllocation {
            meter: self.allocation_work(),
            bytes,
        })
    }
    /// Checks `copy` against every scope's limits, then claims `bytes` of
    /// storage, in one original-leaf pass, so copy limits anywhere in the
    /// chain are reported before any storage limit. Nothing is recorded for
    /// the copy: the caller charges it atomically once the storage exists.
    pub(crate) fn admit_and_claim(
        &self,
        cancellation: &crate::CancellationToken,
        bytes: u64,
        operations: u64,
        copy: WorkCounters,
    ) -> Result<NativeAllocation, crate::FsError> {
        self.state(|state| -> Result<(), crate::FsError> {
            // Existing ledger failures on every scope (an earlier overrun),
            // then caller cancellation, then the proposed copy's limits.
            let native = native_materialization_work_error;
            state.admit(WorkCounters::UNCHARGED).map_err(native)?;
            cancellation.check()?;
            state.admit_reconciled(copy).map_err(native)?;
            state
                .claim_reconciled(bytes, operations, WorkCounters::UNCHARGED)
                .map_err(native)?;
            Ok(())
        })?;
        Ok(NativeAllocation {
            meter: self.allocation_work(),
            bytes,
        })
    }
    /// Each resource scope's own (work, pending) counters, innermost first.
    #[cfg(test)]
    pub(crate) fn resource_chain(&self) -> Vec<(WorkCounters, WorkCounters)> {
        self.resource
            .as_ref()
            .map_or_else(Vec::new, |resource| resource.local_chain())
    }
    pub(crate) fn claim_root_metadata(
        &self,
        bytes: u64,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        let mut work = self.allocation_work();
        work.resource = None;
        work.capture_origin = None;
        work.claim(bytes, 1, WorkCounters::UNCHARGED)
    }
    /// Withholds completion work on the same ledger.
    pub fn reserve_work(
        &self,
        maximum: WorkCounters,
    ) -> Result<NativeWorkReservation, NativeTreeMaterializationError> {
        let maximum = self.item_charge(maximum);
        self.state(|state| state.reserve(maximum))
            .map_err(native_materialization_work_error)?;
        Ok(NativeWorkReservation::new(self.allocation_work(), maximum))
    }
    pub(crate) fn reserve_cleanup_write(
        &self,
    ) -> Result<NativeCleanupWrite, NativeTreeMaterializationError> {
        let meter = self.allocation_work();
        let reservation = acyclic_native_runtime::NativeOperationReservation::new(
            meter.runtime_meter(),
            acyclic_native_runtime::NativeOperationKind::Write,
            1,
        )
        .map_err(|error| meter.runtime_error(error))?;
        Ok(NativeCleanupWrite {
            meter,
            reservation,
            additional_resource: None,
        })
    }
    /// Current live allocation.
    pub fn live_allocation_bytes(&self) -> u64 {
        self.state(|state| state.live_allocation_bytes())
    }
    /// Observes the original root through `native_host`.
    pub fn root_identity(
        &self,
        path: &Path,
    ) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        Ok(crate::native_host::HostRoot::open(path, &mut &*self)?.identity())
    }
    pub(crate) fn record_completed_allocation(
        &self,
        bytes: u64,
    ) -> (NativeAllocation, Result<(), NativeTreeMaterializationError>) {
        let (bytes, result) = self.state(|state| state.record_native_owned_completed(bytes, &None));
        (
            NativeAllocation {
                meter: self.allocation_work(),
                bytes,
            },
            result.map_err(native_materialization_work_error),
        )
    }
    /// Maximum complete builtin payload.
    pub const MAXIMUM_BUILTIN_BYTES: usize = 262_144;
}

#[cfg(all(test, not(acyclic_fs_loom)))]
#[path = "work_normalization_tests.rs"]
mod tests;
