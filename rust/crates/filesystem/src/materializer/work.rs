use super::*;
use std::sync::{Arc, Weak};

/// Existing demand accounting context retained across native completion.
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

impl NativeMaterializationWork {
    /// Logical storage required for the shared demand ledger.
    pub const fn accounting_state_bytes() -> u64 {
        std::mem::size_of::<crate::demand::NativeDemandReadMeter>() as u64
    }
    /// Admits the existing demand ledger before sharing it.
    pub fn new(
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, (NativeTreeMaterializationError, crate::WorkCounters)> {
        let mut demand = crate::demand::DemandWork::new(budget, cancellation);
        demand
            .allocate(Self::accounting_state_bytes())
            .map_err(|error| (native_materialization_work_error(error), demand.work))?;
        let state = Arc::new(crate::demand::NativeDemandReadMeter(std::sync::Mutex::new(
            demand,
        )));
        Ok(Self {
            state,
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
            .map_or(&self.cancellation, |view| view.cancellation())
    }
    pub(super) fn state<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.state.with_original_leaf(|demand| {
            demand.with_invocation_scope(self.invocation.as_ref().map(Arc::downgrade), |demand| {
                demand.with_resource_scope(self.resource.as_ref().map(Arc::downgrade), |demand| {
                    demand.with_capture(None, operation)
                })
            })
        })
    }
    pub(crate) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.state(|demand| demand.with_capture(self.capture_origin.clone(), operation))
    }
    /// Same demand ledger, regardless of local allowances.
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
    /// Same ledger and allocation attribution.
    pub fn shares_allocation_context(&self, other: &Self) -> bool {
        self.shares_ledger(other) && self.shares_resource_allowance(other)
    }
    pub(crate) fn original_demand_work(&self) -> crate::demand::OriginalDemandWork {
        crate::demand::OriginalDemandWork {
            state: self.state.clone(),
            resource: self.resource.clone(),
            capture_origin: self.capture_origin.clone(),
            invocation: self.invocation.clone(),
            #[cfg(feature = "native-mount")]
            execution: self.execution.clone(),
        }
    }
    pub(crate) fn from_original_demand_work(
        original: &crate::demand::OriginalDemandWork,
    ) -> Result<Self, crate::DemandError> {
        Ok(Self {
            state: original.state.clone(),
            resource: original.resource.clone(),
            capture_origin: original.capture_origin.clone(),
            invocation: original.invocation.clone(),
            #[cfg(feature = "native-mount")]
            execution: original.execution.clone(),
            cancellation: original.with_original_leaf(|demand| demand.cancellation().clone())?,
        })
    }
    #[cfg(feature = "native-mount")]
    pub(crate) fn from_meter(state: Arc<crate::demand::NativeDemandReadMeter>) -> Self {
        let cancellation = state.lock().cancellation().clone();
        Self {
            state,
            cancellation,
            resource: None,
            capture_origin: None,
            invocation: None,
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
    /// Links a local stop while retaining the original ledger and grant.
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
            _metadata: metadata,
        }));
        #[cfg(feature = "native-mount")]
        if let Some(execution) = &self.execution {
            work = work.with_source_io_lease(execution._lease.clone())?;
        }
        Ok((work, cancellation))
    }
    /// Limits a resource on its existing parent ledger.
    pub fn native_volume_work(
        &self,
        budget: crate::WorkBudget,
    ) -> Result<Self, NativeTreeMaterializationError> {
        let metadata =
            self.claim_root_metadata(std::mem::size_of::<NativeResourceAllowance>() as u64)?;
        let resource = Arc::new_cyclic(|self_ref| NativeResourceAllowance {
            budget,
            state: std::sync::Mutex::new(NativeResourceState {
                work: crate::WorkCounters::UNCHARGED,
                pending: crate::WorkCounters::UNCHARGED,
                live: 0,
                reserved_allocation_bytes: 0,
                poisoned: false,
            }),
            self_ref: self_ref.clone(),
            parent: self.resource.clone(),
            _root_only_metadata_guard: metadata,
        });
        let mut work = Self {
            resource: Some(resource),
            capture_origin: None,
            ..self.allocation_work()
        };
        #[cfg(feature = "native-mount")]
        if let Some(execution) = &self.execution {
            work = work.with_source_io_lease(execution._lease.clone())?;
        }
        Ok(work)
    }
    /// Immutable original or resource ceiling.
    pub fn budget(&self) -> crate::WorkBudget {
        self.resource.as_ref().map_or_else(
            || self.state(|demand| demand.original_budget()),
            |resource| resource.budget,
        )
    }
    /// Current original runtime meter, including the resource and source lease.
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
    /// Recovers the first typed accounting failure.
    pub fn runtime_error(&self, error: std::io::Error) -> NativeTreeMaterializationError {
        self.state(|demand| demand.take_native_io_failure())
            .map_or_else(
                || NativeTreeMaterializationError::from_runtime_error(error),
                native_materialization_work_error,
            )
    }
    /// Remaining demand and resource allowance.
    pub fn remaining(&self) -> Result<crate::WorkBudget, NativeTreeMaterializationError> {
        self.state(|demand| demand.check().and_then(|()| demand.remaining()))
            .map_err(native_materialization_work_error)
    }
    /// Actual invocation work.
    pub fn receipt(&self) -> crate::WorkCounters {
        self.state(|demand| demand.receipt())
    }
    /// Retains the previous durable prefix once.
    pub fn set_original_work(&self, work: crate::WorkCounters) {
        self.state(|demand| {
            demand.original_work.get_or_insert(work);
        });
    }
    /// Durable prefix plus this invocation's actual work.
    pub fn journal_work(&self) -> Result<crate::WorkCounters, NativeTreeMaterializationError> {
        self.state(|demand| {
            demand
                .original_work
                .unwrap_or_default()
                .checked_add(demand.receipt())
        })
        .map_err(Into::into)
    }
    /// Admits work before attempting it.
    pub fn admit(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.state(|demand| demand.admit(delta))
            .map_err(native_materialization_work_error)
    }
    /// Charges the operation actually starting now.
    pub fn charge(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.state(|demand| demand.charge(delta))
            .map_err(native_materialization_work_error)
    }
    /// Records completed nested work and its live-buffer peak.
    pub fn spent(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.state(|demand| demand.spent(delta))
            .map_err(native_materialization_work_error)
    }
    pub(crate) fn record_admitted(
        &self,
        delta: crate::WorkCounters,
    ) -> Result<(), NativeTreeMaterializationError> {
        self.state(|demand| demand.record(delta))
            .map_err(native_materialization_work_error)
    }
    /// Admits retained bytes before allocation.
    pub fn admit_retained(&self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        self.state(|demand| demand.admit_retained(bytes))
            .map_err(native_materialization_work_error)
    }
    /// Claims allocation capacity on the same demand ledger.
    pub fn claim(
        &self,
        bytes: u64,
        operations: u64,
        delta: crate::WorkCounters,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.state(|demand| demand.claim_native_owned(bytes, operations, delta, &None))
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
    /// Withholds completion work on the existing ledger and resource.
    pub fn reserve_work(
        &self,
        maximum: crate::WorkCounters,
    ) -> Result<NativeWorkReservation, NativeTreeMaterializationError> {
        self.state(|demand| demand.reserve_native_work(maximum))
            .map_err(native_materialization_work_error)?;
        Ok(NativeWorkReservation::new(self.allocation_work(), maximum))
    }
    pub(crate) fn reserve_cleanup_write(
        &self,
    ) -> Result<NativeCleanupWrite, NativeTreeMaterializationError> {
        self.state(|demand| demand.reserve(NativeResourceCredit::WriteOperations, 1))
            .map_err(native_materialization_work_error)?;
        Ok(NativeCleanupWrite {
            meter: self.allocation_work(),
            additional_resource: None,
            reserved: true,
        })
    }
    /// Actual live allocation, rather than the historical peak.
    pub fn live_allocation_bytes(&self) -> u64 {
        self.state(|demand| demand.live_allocation_bytes())
    }
    /// Observes a held root through the original demand context.
    pub fn root_identity(
        &self,
        path: &Path,
    ) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        Ok(crate::native_host::HostRoot::open(path, &mut &*self)?.identity())
    }
    /// Creates an admitted owned lexical path.
    pub fn path(
        &self,
        root: &Path,
        leaf: Option<&Path>,
    ) -> Result<(PathBuf, NativeAllocation), NativeTreeMaterializationError> {
        let copied = root
            .as_os_str()
            .len()
            .checked_add(leaf.map_or(0, |p| p.as_os_str().len()))
            .ok_or(crate::WorkError::Overflow)?;
        let capacity = copied
            .checked_add(usize::from(leaf.is_some()))
            .ok_or(crate::WorkError::Overflow)?;
        let delta = NativeResourceCredit::Copy.delta(copied as u64);
        self.admit(delta)?;
        let allocation = self.claim(
            capacity as u64,
            u64::from(capacity != 0),
            crate::WorkCounters::UNCHARGED,
        )?;
        let mut path = PathBuf::new();
        path.try_reserve_exact(capacity)
            .map_err(NativeTreeMaterializationError::AllocationFailed)?;
        path.push(root);
        if let Some(leaf) = leaf {
            path.push(leaf);
        }
        self.spent(delta)?;
        Ok((path, allocation))
    }
    /// Copies bytes into an admitted owned buffer.
    pub fn copy_bytes(
        &self,
        bytes: &[u8],
    ) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
        let mut allocation = self.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        let length = bytes
            .len()
            .min(usize::try_from(self.remaining()?.bytes_copied).unwrap_or(usize::MAX));
        if length == 0 && !bytes.is_empty() {
            self.admit(NativeResourceCredit::Copy.delta(1))?;
        }
        let delta = NativeResourceCredit::Copy.delta(length as u64);
        let copy = self.reserve_work(delta)?;
        let mut output = Vec::new();
        if length != 0 {
            allocation.grow(length.max(8) as u64)?;
            output
                .try_reserve_exact(length.max(8))
                .map_err(NativeTreeMaterializationError::AllocationFailed)?;
            allocation.resize_retained(output.capacity() as u64)?;
        }
        output.extend_from_slice(&bytes[..length]);
        copy.record_completed(delta)?;
        if length != bytes.len() {
            self.admit(NativeResourceCredit::Copy.delta(1))?;
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok((output, allocation))
    }
    /// Formats into an admitted native buffer.
    pub fn display<T: std::fmt::Display + ?Sized>(
        &self,
        value: &T,
    ) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
        use std::io::Write as _;
        let mut writer = NativeImageWriter::new(self)?;
        if let Err(error) = write!(&mut writer, "{value}") {
            return Err(writer.failure.take().unwrap_or_else(|| error.into()));
        }
        Ok((
            String::from_utf8(writer.image)
                .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?,
            writer.allocation,
        ))
    }
    pub(crate) fn owns_native_checkout_token(
        &self,
        token: &Arc<crate::kernel::ProofDataCustody>,
    ) -> bool {
        self.state(|demand| demand.owns_native_checkout_token(token))
    }
    pub(crate) fn checkout_control_sink(
        &self,
        producer: &Arc<crate::kernel::ProofDataCustody>,
    ) -> Result<crate::demand::NativeCheckoutControlWitness, NativeTreeMaterializationError> {
        self.state
            .checkout_control_witness(producer)
            .ok_or(NativeTreeMaterializationError::InvalidLayout)
    }
    pub(crate) fn register_native_checkout_control(
        &self,
        producer: &Arc<crate::kernel::ProofDataCustody>,
        header: &mut NativeAllocation,
    ) -> Result<crate::demand::NativeCheckoutControlWitness, NativeTreeMaterializationError> {
        self.state
            .register_native_checkout_control(self, producer, header)
    }
    pub(crate) fn transfer_capture_allocation(
        &self,
        bytes: u64,
        custody: &Arc<crate::demand::CaptureTransientCustody>,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        if !custody.owns_native_work(self) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        custody.transfer_retained_bytes(bytes)?;
        Ok(NativeAllocation {
            meter: self.allocation_work(),
            bytes,
            capture: None,
        })
    }
    pub(crate) fn record_completed_allocation(
        &self,
        bytes: u64,
        capture: Option<Arc<crate::demand::CaptureTransientCustody>>,
    ) -> (NativeAllocation, Result<(), NativeTreeMaterializationError>) {
        let (bytes, result) =
            self.state(|demand| demand.record_native_owned_completed(bytes, &None));
        (
            NativeAllocation {
                meter: self.allocation_work(),
                bytes,
                capture,
            },
            result.map_err(native_materialization_work_error),
        )
    }
    /// Maximum complete builtin payload.
    pub const MAXIMUM_BUILTIN_BYTES: usize = 262_144;

    /// Retains an admitted source lease on this accounting context.
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
                (None, b) => b.clone(),
                (a, None) => a.clone(),
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
                    self_ref: self_ref.clone(),
                    _metadata: metadata,
                }));
            }
        }
        let metadata = work.claim(
            std::mem::size_of::<NativeSourceExecutionView>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        work.execution = Some(Arc::new(NativeSourceExecutionView {
            _lease: lease,
            _metadata: metadata,
        }));
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
            work = work.with_source_io_lease(execution._lease.clone())?;
        }
        Ok(work)
    }
    pub(crate) fn allocations<T, E: From<NativeTreeMaterializationError>>(
        &self,
        operation: impl FnOnce(
            &mut crate::kernel::allocation::AllocationLedger,
            &mut crate::WorkCounters,
            crate::WorkBudget,
            &crate::CancellationToken,
        ) -> Result<T, E>,
    ) -> Result<(T, NativeAllocation), E> {
        let (result, bytes, cleanup) = self
            .state(|demand| {
                demand
                    .reconcile_native_controls()
                    .map_err(NativeTreeMaterializationError::from)?;
                let (allocations, counters, budget, cancellation) = demand.allocation_parts();
                let before = allocations.live_bytes();
                let result = operation(allocations, counters, budget, cancellation);
                let bytes = allocations.live_bytes().checked_sub(before);
                let cleanup = bytes
                    .filter(|_| result.is_err())
                    .map(|bytes| allocations.release(bytes));
                Ok::<_, NativeTreeMaterializationError>((result, bytes, cleanup))
            })
            .map_err(E::from)?;
        let bytes = bytes.ok_or_else(|| {
            E::from(NativeTreeMaterializationError::Work(
                crate::WorkError::Overflow,
            ))
        })?;
        if let Some(cleanup) = cleanup {
            cleanup.map_err(|error| {
                E::from(error.into_work_or(NativeTreeMaterializationError::Work(
                    crate::WorkError::Overflow,
                )))
            })?;
        }
        result.map(|value| {
            (
                value,
                NativeAllocation {
                    meter: self.allocation_work(),
                    bytes,
                    capture: None,
                },
            )
        })
    }
}
