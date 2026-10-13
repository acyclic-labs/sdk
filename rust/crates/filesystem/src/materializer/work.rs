use super::*;
use std::sync::{Arc, Weak};

/// The original demand ledger and its current cancellation and resource scope.
#[derive(Clone)]
pub struct NativeMaterializationWork {
    pub(crate) state: Arc<crate::demand::NativeDemandReadMeter>,
    pub(crate) resource: Option<Arc<NativeResourceAllowance>>,
    pub(crate) capture_origin: Option<Weak<crate::demand::CaptureTransientCustody>>,
    pub(crate) invocation: Option<Arc<NativeInvocationView>>,
    #[cfg(feature = "native-mount")]
    pub(crate) execution: Option<Arc<NativeSourceExecutionView>>,
    cancellation: crate::CancellationToken,
}
impl std::fmt::Debug for NativeMaterializationWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeMaterializationWork")
            .finish_non_exhaustive()
    }
}
impl NativeMaterializationWork {
    /// Retained bytes needed for the original shared ledger.
    pub const fn accounting_state_bytes() -> u64 {
        std::mem::size_of::<crate::demand::NativeDemandReadMeter>() as u64
    }
    /// Admits the ledger before sharing it.
    pub fn new(
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, (NativeTreeMaterializationError, crate::WorkCounters)> {
        let mut state = crate::demand::DemandWork::new(budget, cancellation);
        state
            .claim_allocation(Self::accounting_state_bytes(), 1, crate::WorkCounters::UNCHARGED)
            .map_err(|error| (native_materialization_work_error(error), state.work))?;
        Ok(Self {
            state: Arc::new(crate::demand::NativeDemandReadMeter(std::sync::Mutex::new(
                state,
            ))),
            cancellation: cancellation.clone(),
            resource: None,
            capture_origin: None,
            invocation: None,
            #[cfg(feature = "native-mount")]
            execution: None,
        })
    }
    /// Current invocation cancellation.
    pub fn cancellation(&self) -> &crate::CancellationToken {
        self.invocation
            .as_ref()
            .map_or(&self.cancellation, |view| &view.cancellation)
    }
    pub(super) fn state<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.state.with_original_leaf(|state| {
            state.scope(|work| &mut work.native_invocation, self.invocation.as_ref().map(Arc::downgrade), |state| {
                state.scope(|work| &mut work.native_resource, self.resource.as_ref().map(Arc::downgrade), |state| {
                    state.scope(|work| &mut work.capture_custody, None, operation)
                })
            })
        })
    }
    pub(crate) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.state(|state| state.scope(|work| &mut work.capture_custody, self.capture_origin.clone(), operation))
    }
    pub(super) fn poison(&self, error: NativeTreeMaterializationError) {
        self.state(|state| {
            let error = match error {
                NativeTreeMaterializationError::Work(error) => crate::DemandError::Work(error),
                NativeTreeMaterializationError::Cancelled => crate::DemandError::Cancelled,
                NativeTreeMaterializationError::OriginalWorkPoisoned => {
                    crate::DemandError::OriginalWorkPoisoned
                }
                NativeTreeMaterializationError::Io(error) => crate::DemandError::Io(error),
                _ => crate::DemandError::InvalidRequest,
            };
            state.record_native_io_failure(error);
            state.poison_native_original_work();
        });
    }
    /// Whether both contexts share the original ledger.
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
    /// Whether both contexts attribute allocations to the same resource.
    pub fn shares_allocation_context(&self, other: &Self) -> bool {
        self.shares_ledger(other) && self.shares_resource_allowance(other)
    }
    pub(crate) fn from_meter(state: Arc<crate::demand::NativeDemandReadMeter>) -> Self {
        let cancellation = state.lock().cancellation().clone();
        Self {
            state,
            cancellation,
            resource: None,
            capture_origin: None,
            invocation: None,
            #[cfg(feature = "native-mount")]
            execution: None,
        }
    }
    pub(crate) fn allocation_work(&self) -> Self {
        Self {
            #[cfg(feature = "native-mount")]
            execution: None,
            ..self.clone()
        }
    }
    /// Links an invocation stop without changing its accounting ledger.
    pub fn invocation_view(
        &self,
    ) -> Result<(Self, crate::CancellationToken), NativeTreeMaterializationError> {
        let cancellation =
            crate::CancellationToken::linked_child_with_native_work(self.cancellation(), self)?;
        let metadata = self.claim(
            std::mem::size_of::<NativeInvocationView>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let mut work = self.allocation_work();
        work.invocation = Some(Arc::new_cyclic(|self_ref| NativeInvocationView {
            cancellation: cancellation.clone(),
            self_ref: self_ref.clone(),
            metadata,
        }));
        #[cfg(feature = "native-mount")]
        if let Some(execution) = &self.execution {
            work = work.with_source_io_lease(execution.lease.clone())?;
        }
        Ok((work, cancellation))
    }
    /// Adds a finite resource ceiling on the same original ledger.
    pub fn native_volume_work(
        &self,
        budget: crate::WorkBudget,
    ) -> Result<Self, NativeTreeMaterializationError> {
        let metadata =
            self.claim_root_metadata(std::mem::size_of::<NativeResourceAllowance>() as u64)?;
        let resource = Arc::new_cyclic(|self_ref| NativeResourceAllowance {
            budget,
            state: std::sync::Mutex::new(NativeResourceState::default()),
            self_ref: self_ref.clone(),
            parent: self.resource.clone(),
            metadata,
        });
        let mut work = Self {
            resource: Some(resource),
            capture_origin: None,
            ..self.allocation_work()
        };
        #[cfg(feature = "native-mount")]
        if let Some(execution) = &self.execution {
            work = work.with_source_io_lease(execution.lease.clone())?;
        }
        Ok(work)
    }
    /// Original or resource ceiling.
    pub fn budget(&self) -> crate::WorkBudget {
        self.resource.as_ref().map_or_else(
            || self.state(|state| state.original_budget()),
            |resource| resource.budget,
        )
    }
    /// Existing meter carrying the resource, cancellation and source lease.
    pub fn runtime_meter(&self) -> Arc<dyn acyclic_native_runtime::NativeReadMeter> {
        #[cfg(feature = "native-mount")]
        if let Some(execution) = &self.execution {
            return execution.clone();
        }
        if let Some(invocation) = &self.invocation {
            if invocation.owns_resource(self.resource.as_ref()) {
                return invocation.clone();
            }
        }
        match &self.resource {
            Some(resource) => resource.clone(),
            None => self.state.clone(),
        }
    }
    /// First typed failure retained by the original runtime meter.
    pub fn runtime_error(&self, error: std::io::Error) -> NativeTreeMaterializationError {
        self.state(|state| state.take_native_io_failure())
            .map_or_else(
                || NativeTreeMaterializationError::from_runtime_error(error),
                native_materialization_work_error,
            )
    }
    /// Remaining original and resource allowance.
    pub fn remaining(&self) -> Result<crate::WorkBudget, NativeTreeMaterializationError> {
        self.state(|state| state.check().and_then(|()| state.remaining()))
            .map_err(native_materialization_work_error)
    }
    /// Work actually performed by this invocation.
    pub fn receipt(&self) -> crate::WorkCounters {
        self.state(|state| state.receipt())
    }
    /// Retains the previously durable work prefix once.
    pub fn set_original_work(&self, work: crate::WorkCounters) {
        self.state(|state| {
            state.original_work.get_or_insert(work);
        });
    }
    /// Previously durable work plus the current invocation.
    pub fn journal_work(&self) -> Result<crate::WorkCounters, NativeTreeMaterializationError> {
        self.state(|state| {
            state
                .original_work
                .unwrap_or_default()
                .checked_add(state.receipt())
        })
        .map_err(Into::into)
    }
    /// Admits work before an effect.
    pub fn admit(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.state(|state| state.admit(delta))
            .map_err(native_materialization_work_error)
    }
    /// Charges work that is starting now.
    pub fn charge(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.state(|state| state.charge(delta))
            .map_err(native_materialization_work_error)
    }
    /// Records work completed by a nested operation.
    pub fn spent(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.state(|state| state.spent(delta))
            .map_err(native_materialization_work_error)
    }
    pub(crate) fn record_admitted(
        &self,
        delta: crate::WorkCounters,
    ) -> Result<(), NativeTreeMaterializationError> {
        self.state(|state| state.record(delta))
            .map_err(native_materialization_work_error)
    }
    /// Admits retained bytes before allocation.
    pub fn admit_retained(&self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        self.state(|state| state.admit_retained(bytes))
            .map_err(native_materialization_work_error)
    }
    /// Claims capacity on the original ledger and current resource.
    pub fn claim(
        &self,
        bytes: u64,
        operations: u64,
        delta: crate::WorkCounters,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.state(|state| state.claim_allocation(bytes, operations, delta))
            .map_err(native_materialization_work_error)?;
        Ok(NativeAllocation {
            meter: self.allocation_work(),
            bytes,
            capture: None,
        })
    }
    pub(crate) fn claim_root_metadata(
        &self,
        bytes: u64,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        Self {
            resource: None,
            capture_origin: None,
            ..self.allocation_work()
        }
        .claim(bytes, 1, crate::WorkCounters::UNCHARGED)
    }
    /// Withholds completion work on the same ledger.
    pub fn reserve_work(
        &self,
        maximum: crate::WorkCounters,
    ) -> Result<NativeWorkReservation, NativeTreeMaterializationError> {
        self.state(|state| state.reserve(maximum))
            .map_err(native_materialization_work_error)?;
        Ok(NativeWorkReservation::new(self.allocation_work(), maximum))
    }
    pub(crate) fn reserve_cleanup_write(
        &self,
    ) -> Result<NativeCleanupWrite, NativeTreeMaterializationError> {
        self.state(|state| state.reserve(NativeResourceCredit::WriteOperations.delta(1)))
            .map_err(native_materialization_work_error)?;
        Ok(NativeCleanupWrite {
            meter: self.allocation_work(),
            additional_resource: None,
            reserved: true,
        })
    }
    /// Current live allocation rather than historical peak.
    pub fn live_allocation_bytes(&self) -> u64 {
        self.state(|state| state.live_allocation_bytes())
    }
    /// Observes a held root through the current finite context.
    pub fn root_identity(
        &self,
        path: &Path,
    ) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        Ok(crate::native_host::HostRoot::open(path, &mut &*self)?.identity())
    }
    pub(crate) fn record_completed_allocation(
        &self,
        bytes: u64,
        capture: Option<Arc<crate::demand::CaptureTransientCustody>>,
    ) -> (NativeAllocation, Result<(), NativeTreeMaterializationError>) {
        let (bytes, result) = self.state(|state| state.record_native_owned_completed(bytes, &None));
        (
            NativeAllocation {
                meter: self.allocation_work(),
                bytes,
                capture,
            },
            result.map_err(native_materialization_work_error),
        )
    }
    /// Largest complete builtin payload.
    pub const MAXIMUM_BUILTIN_BYTES: usize = 262_144;
    /// Keeps the admitted source lease alive through native completion.
    #[cfg(feature = "native-mount")]
    pub fn with_source_io_lease(
        &self,
        lease: crate::native_mount::NativeSourceIoLease,
    ) -> Result<Self, NativeTreeMaterializationError> {
        let mut work = self.allocation_work();
        if let Some(source) = lease.original_work() {
            if !work.shares_ledger(source) {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
            work.resource = match (&work.resource, &source.resource) {
                (None, resource) | (resource, None) => resource.clone(),
                (Some(a), Some(b)) if a.contains_scope(b) => Some(a.clone()),
                (Some(a), Some(b)) if b.contains_scope(a) => Some(b.clone()),
                _ => return Err(NativeTreeMaterializationError::InvalidLayout),
            };
        }
        if let Some(view) = &work.invocation {
            if !view.owns_resource(work.resource.as_ref()) {
                let cancellation = view.cancellation.clone();
                let metadata = work.claim(
                    std::mem::size_of::<NativeInvocationView>() as u64,
                    1,
                    crate::WorkCounters::UNCHARGED,
                )?;
                work.invocation = Some(Arc::new_cyclic(|self_ref| NativeInvocationView {
                    cancellation,
                    metadata,
                    self_ref: self_ref.clone(),
                }));
            }
        }
        let metadata = work.claim(
            std::mem::size_of::<NativeSourceExecutionView>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        work.execution = Some(Arc::new(NativeSourceExecutionView { lease, metadata }));
        Ok(work)
    }
    pub(crate) fn with_capture_custody(
        &self,
        capture: &Arc<crate::demand::CaptureTransientCustody>,
    ) -> Result<Self, NativeTreeMaterializationError> {
        if !capture.owns_native_work(self) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let mut work = Self {
            capture_origin: Some(Arc::downgrade(capture)),
            ..self.allocation_work()
        };
        #[cfg(feature = "native-mount")]
        if let Some(execution) = &self.execution {
            work = work.with_source_io_lease(execution.lease.clone())?;
        }
        Ok(work)
    }
}
