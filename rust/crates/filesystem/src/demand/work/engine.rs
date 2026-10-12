use super::*;

#[cfg(not(target_arch = "wasm32"))]
/// Accounting context for bounded source operations.
pub struct DemandWork {
    pub(crate) work: WorkCounters,
    pub(crate) budget: WorkBudget,
    pub(in crate::demand) native_full_budget: WorkBudget,
    pub(crate) original_work: Option<WorkCounters>,
    pub(in crate::demand) allocations: crate::kernel::allocation::AllocationLedger,
    pub(in crate::demand) cancellation: CancellationToken,
    pub(in crate::demand) stop: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    pub(in crate::demand) cancellation_wait: DemandCancellationRoots,
    pub(in crate::demand) capture_custody: Option<std::sync::Weak<CaptureTransientCustody>>,
    pub(in crate::demand) native_resource: Option<std::sync::Weak<crate::materializer::NativeResourceAllowance>>,
    pub(in crate::demand) native_invocation: Option<std::sync::Weak<crate::materializer::NativeInvocationView>>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::demand) struct DemandScope<'a, T> {
    pub(in crate::demand) work: &'a mut DemandWork,
    pub(in crate::demand) previous: T,
    pub(in crate::demand) field: fn(&mut DemandWork) -> &mut T,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> Drop for DemandScope<'_, T> {
    fn drop(&mut self) {
        std::mem::swap((self.field)(self.work), &mut self.previous);
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl DemandWork {
    pub(in crate::demand) fn scope<T, R>(
                &mut self, field: fn(&mut Self) -> &mut T, next: T,
                operation: impl FnOnce(&mut Self) -> R,
            ) -> R {
                let previous = std::mem::replace(field(self), next);
                let scope = DemandScope { work: self, previous, field };
                operation(&mut *scope.work)
            }

    pub(crate) fn with_resource_scope<T>(
                &mut self,
                resource: Option<std::sync::Weak<crate::materializer::NativeResourceAllowance>>,
                operation: impl FnOnce(&mut Self) -> T,
            ) -> T {
                self.scope(|work| &mut work.native_resource, resource, operation)
            }

    pub(crate) fn with_capture<T>(
                &mut self, capture: Option<std::sync::Weak<CaptureTransientCustody>>,
                operation: impl FnOnce(&mut Self) -> T,
            ) -> T {
                self.scope(|work| &mut work.capture_custody, capture, operation)
            }

    pub(crate) fn with_invocation_scope<T>(
                &mut self, invocation: Option<std::sync::Weak<crate::materializer::NativeInvocationView>>,
                operation: impl FnOnce(&mut Self) -> T,
            ) -> T {
                self.scope(|work| &mut work.native_invocation, invocation, operation)
            }

    pub(in crate::demand) fn resource_scope(&self) -> Option<Arc<crate::materializer::NativeResourceAllowance>> {
                self.native_resource.as_ref().and_then(std::sync::Weak::upgrade)
            }

    pub(crate) const fn original_budget(&self) -> WorkBudget { self.native_full_budget }

    /// Starts a source operation with its budget and cancellation token.
            pub fn new(budget: WorkBudget, cancellation: &CancellationToken) -> Self {
                Self {
                    work: WorkCounters::default(),
                    budget,
                    native_full_budget: budget,
                    original_work: None,
                    allocations: crate::kernel::allocation::AllocationLedger::default(),
                    cancellation: cancellation.clone(),
                    stop: None,
                    cancellation_wait: DemandCancellationRoots::default(),
                    capture_custody: None,
                    native_resource: None,
                    native_invocation: None,
                }
            }

    pub(crate) fn native_queue_shell(&self) -> Self {
                Self::new(WorkBudget::default(), &self.cancellation)
            }

    pub(crate) fn take_for_native_queue(&mut self) -> Self {
                let shell = self.native_queue_shell();
                std::mem::replace(self, shell)
            }

    pub(crate) fn from_current(
                budget: WorkBudget, cancellation: &CancellationToken, work: WorkCounters,
                allocations: crate::kernel::allocation::AllocationLedger,
                cancellation_wait: DemandCancellationRoots,
            ) -> Self {
                #[cfg(feature = "native-mount")]
                let cancellation_wait = {
                    let mut roots = cancellation_wait;
                    roots.view_gates.budget_reconstructed();
                    roots
                };
                let mut original = Self::new(budget, cancellation);
                original.work = work;
                original.allocations = allocations;
                original.cancellation_wait = cancellation_wait;
                original.restore_budget(budget);
                original
            }

    pub(crate) fn claim_native_owned(
                &mut self, bytes: u64, operations: u64, delta: WorkCounters,
                capture: &Option<Arc<CaptureTransientCustody>>,
            ) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    let mut admitted = delta;
                    admitted.allocation_operations = admitted.allocation_operations.checked_add(operations)
                        .ok_or(crate::WorkError::Overflow)?;
                    return resource.admission(admitted, bytes, &mut || self.with_resource_scope(None,
                        |work| work.claim_native_owned(bytes, operations, delta, capture)));
                }
                self.reconcile_cancellation_registry()?;
                self.check()?;
                let mut allocations = self.allocations;
                let mut work = self.work;
                work.charge(&delta, &self.budget)?;
                allocations.claim_bytes(bytes, operations, &mut work, self.budget)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                self.allocations = allocations;
                self.work = work;
                if let Some(capture) = capture { capture.claim(bytes); }
                Ok(())
            }

    #[cfg(not(target_arch = "wasm32"))]
            pub(crate) fn retain_native_owned(
                &mut self, bytes: u64,
                capture: &Option<std::sync::Arc<CaptureTransientCustody>>,
            ) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    return resource.admission(WorkCounters::UNCHARGED, bytes, &mut || self.with_resource_scope(None, |work| work.retain_native_owned(bytes, capture)));
                }
                if self.cancellation_wait.native_poisoned { return Err(DemandError::OriginalWorkPoisoned); }
                self.allocations.claim_bytes(bytes, 0, &mut self.work, self.budget)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                if let Some(capture) = capture { capture.claim(bytes); }
                Ok(())
            }

    #[cfg(not(target_arch = "wasm32"))]
            pub(crate) fn record_native_owned_completed(
                &mut self, bytes: u64,
                capture: &Option<std::sync::Arc<CaptureTransientCustody>>,
            ) -> (u64, Result<(), DemandError>) {
                if let Some(resource) = self.resource_scope() {
                    let (claimed, recorded) = self.with_resource_scope(None, |work| work.record_native_owned_completed(bytes, capture));
                    let scoped = resource.completed(WorkCounters::UNCHARGED, claimed);
                    return (claimed, recorded.and(scoped));
                }
                if let Err(error) = self.allocations.record_owned_bytes(bytes, &mut self.work) {
                    return (0, Err(error.into_work_or(DemandError::InvalidRequest)));
                }
                if let Some(capture) = capture { capture.claim(bytes); }
                let result = self.check().and_then(|()| self.work.verify(self.budget).map_err(DemandError::from));
                (bytes, result)
            }

    #[cfg(not(target_arch = "wasm32"))]
            pub(crate) fn release_native_owned(
                &mut self, bytes: u64,
                capture: &Option<std::sync::Arc<CaptureTransientCustody>>,
            ) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    let released = self.with_resource_scope(None, |work| work.release_native_owned(bytes, capture));
                    let scoped = resource.retire(bytes);
                    if released.is_err() || scoped.is_err() { self.poison_native_original_work(); }
                    return released.and(scoped);
                }
                let released = self.allocations.release(bytes)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest));
                let capture_released = capture.as_ref().map_or(Ok(()), |capture| {
                    capture.transfer_retained_bytes(bytes).map_err(DemandError::from)
                });
                if let Err(error) = released.and(capture_released) {
                    self.record_native_io_failure(error);
                    self.poison_native_original_work();
                    return Err(DemandError::OriginalWorkPoisoned);
                }
                Ok(())
            }

    #[cfg(not(target_arch = "wasm32"))]
            pub(crate) fn poison_native_original_work(&mut self) {
                self.cancellation_wait.native_poisoned = true;
                self.record_native_io_failure(DemandError::OriginalWorkPoisoned);
            }

    pub(crate) fn reserve_native_allocation(&mut self, bytes: u64) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    return resource.reserve(crate::materializer::NativeResourceCredit::AllocationOperations.delta(1), bytes, None, &mut || {
                        self.with_resource_scope(None, |work| work.reserve_native_allocation(bytes))
                    });
                }
                self.reconcile_cancellation_registry()?;
                self.check()?;
                let pending = self.cancellation_wait.pending_work.allocation_operations
                    .checked_add(1).ok_or(crate::WorkError::Overflow)?;
                self.allocations.reserve_allocation(bytes, &self.work, self.budget)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                self.budget.allocation_operations -= 1;
                self.cancellation_wait.pending_work.allocation_operations = pending;
                Ok(())
            }

    pub(crate) fn cancel_reserved_native_allocation(&mut self, bytes: u64) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    return resource.settle_allocation(bytes, false, &mut || {
                        self.with_resource_scope(None, |work| work.cancel_reserved_native_allocation(bytes))
                    });
                }
                let pending = self.cancellation_wait.pending_work.allocation_operations
                    .checked_sub(1).ok_or(crate::WorkError::Overflow)?;
                let budget = self.budget.allocation_operations.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                self.allocations.cancel_reserved_allocation(bytes)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                self.cancellation_wait.pending_work.allocation_operations = pending;
                self.budget.allocation_operations = budget;
                Ok(())
            }

    pub(crate) fn record_reserved_native_allocation(&mut self, bytes: u64) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    return resource.settle_allocation(bytes, true, &mut || {
                        self.with_resource_scope(None, |work| work.record_reserved_native_allocation(bytes))
                    });
                }
                if self.cancellation_wait.native_poisoned { return Err(DemandError::OriginalWorkPoisoned); }
                let pending = self.cancellation_wait.pending_work.allocation_operations
                    .checked_sub(1).ok_or(crate::WorkError::Overflow)?;
                let budget = self.budget.allocation_operations.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                self.allocations.record_reserved_allocation(bytes, &mut self.work)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                self.cancellation_wait.pending_work.allocation_operations = pending;
                self.budget.allocation_operations = budget;
                if let Some(custody) = self.capture_custody() { custody.claim(bytes); }
                Ok(())
            }

    #[cfg(not(target_arch = "wasm32"))]
            pub(crate) fn reserve_native_work(&mut self, maximum: WorkCounters) -> Result<(), DemandError> {
                if maximum.peak_allocation_bytes != 0 || maximum.allocation_operations != 0 {
                    return Err(DemandError::InvalidRequest);
                }
                if let Some(resource) = self.resource_scope() {
                    return resource.reserve(maximum, 0, None, &mut || self.with_resource_scope(None, |work| work.reserve_native_work(maximum)));
                }
                self.admit(maximum)?;
                let budget = maximum.remaining(self.budget)?;
                let pending = self.cancellation_wait.pending_work.checked_add(maximum)?;
                self.budget = budget;
                self.cancellation_wait.pending_work = pending;
                Ok(())
            }

    #[cfg(not(target_arch = "wasm32"))]
            pub(crate) fn refund_native_work(&mut self, unused: WorkCounters) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    let original = self.with_resource_scope(None, |work| work.refund_native_work(unused));
                    let scoped = resource.refund_work(unused);
                    return original.and(scoped);
                }
                let pending = unused.remaining(self.cancellation_wait.pending_work)?;
                let budget = self.budget.checked_add(unused)?;
                self.cancellation_wait.pending_work = pending;
                self.budget = budget;
                Ok(())
            }

    #[cfg(not(target_arch = "wasm32"))]
            pub(crate) fn record_native_work(&mut self, actual: WorkCounters) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    let original = self.with_resource_scope(None, |work| work.record_native_work(actual));
                    let scoped = resource.complete_work(actual);
                    return original.and(scoped);
                }
                let restored = self.refund_native_work(actual);
                let recorded = self.record(actual);
                let invariant = if self.cancellation_wait.native_poisoned
                    || self.cancellation_wait.observer_overflow.as_ref()
                        .is_some_and(|overflow| overflow.is_poisoned()) {
                    Err(DemandError::OriginalWorkPoisoned)
                } else { Ok(()) };
                restored.and(recorded).and(invariant)
            }

    #[cfg(not(target_arch = "wasm32"))]
            pub(crate) fn start_native_reserved_work(&mut self, actual: WorkCounters) -> Result<(), DemandError> {
                if self.cancellation_wait.native_poisoned
                    || self.cancellation_wait.observer_overflow.as_ref()
                        .is_some_and(|overflow| overflow.is_poisoned()) {
                    return Err(DemandError::OriginalWorkPoisoned);
                }
                let pending = actual.remaining(self.cancellation_wait.pending_work)?;
                let budget = self.budget.checked_add(actual)?;
                let work = self.work.checked_add(actual)?;
                work.verify(budget)?;
                self.cancellation_wait.pending_work = pending;
                self.budget = budget;
                self.work = work;
                Ok(())
            }

    pub(crate) fn restore_budget(&mut self, budget: WorkBudget) {
                self.budget = budget;
                match self.cancellation_wait.pending_work.remaining(self.budget) {
                    Ok(effective) => self.budget = effective,
                    Err(error) => {
                        self.record_native_io_failure(error.into());
                        self.poison_native_original_work();
                    }
                }
                #[cfg(feature = "native-mount")]
                self.cancellation_wait.view_gates.budget_reconstructed();
            }

    pub(crate) fn into_current(mut self) -> (
                WorkCounters, crate::kernel::allocation::AllocationLedger,
                DemandCancellationRoots,
            ) {
                self.reconcile_cancellation_registry()
                    .expect("original reserved registry cleanup remains accountable across ownership transfer");
                (self.work, std::mem::take(&mut self.allocations), std::mem::take(&mut self.cancellation_wait))
            }

    pub(crate) fn cancellation(&self) -> &CancellationToken {
                &self.cancellation
            }

    pub(crate) fn capture_custody(&self) -> Option<std::sync::Arc<CaptureTransientCustody>> {
                self.capture_custody.as_ref().and_then(std::sync::Weak::upgrade)
            }

    pub(crate) fn check(&self) -> Result<(), DemandError> {
                #[cfg(not(target_arch = "wasm32"))]
                if self.cancellation_wait.native_poisoned || self.cancellation_wait.observer_overflow.as_ref()
                    .is_some_and(|overflow| overflow.is_poisoned()) {
                    return Err(DemandError::OriginalWorkPoisoned);
                }
                self.cancellation.check().map_err(|_| DemandError::Cancelled)?;
                if self.stop.as_ref().is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire)) {
                    return Err(DemandError::Cancelled);
                }
                if let Some(invocation) = self.native_invocation.as_ref().and_then(std::sync::Weak::upgrade) {
                    invocation.check()?;
                }
                Ok(())
            }

    pub(crate) fn live_allocation_bytes(&self) -> u64 {
                self.allocations.live_bytes()
                    .checked_sub(self.cancellation_wait.freed_bytes())
                    .and_then(|live| live.checked_add(self.cancellation_wait.unrecorded_observer_bytes()))
                    .expect("retained observer and registry data belong to this allocation ledger")
            }

    pub(crate) fn allocation_parts(&mut self) -> (
                &mut crate::kernel::allocation::AllocationLedger, &mut WorkCounters,
                WorkBudget, &CancellationToken,
            ) {
                (&mut self.allocations, &mut self.work, self.budget, &self.cancellation)
            }

    pub(crate) fn receipt(&mut self) -> WorkCounters {
                self.reconcile_native_controls().expect("original native constructor claims remain balanced");
                self.work
            }

    pub(crate) fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    resource.admit(delta, 0)?;
                    return self.with_resource_scope(None, |work| work.admit(delta));
                }
                self.reconcile_cancellation_registry()?;
                self.check()?;
                self.work.admit(&delta, &self.budget)?;
                Ok(())
            }

    pub(crate) fn admit_retained(&mut self, bytes: u64) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    resource.admit(WorkCounters::UNCHARGED, bytes)?;
                    return self.with_resource_scope(None, |work| work.admit_retained(bytes));
                }
                self.reconcile_cancellation_registry()?;
                self.check()?;
                let peak = self.allocations.live_bytes().checked_add(bytes)
                    .ok_or(crate::WorkError::Overflow)?;
                self.work.admit(&WorkCounters { peak_allocation_bytes: peak, ..WorkCounters::UNCHARGED }, &self.budget)?;
                Ok(())
            }

    pub(crate) fn spent(&mut self, mut delta: WorkCounters) -> Result<(), DemandError> {
                let resource = self.resource_scope().map_or(Ok(()), |resource| resource.completed(delta, 0));
                let reconciled = self.reconcile_cancellation_registry().map_err(DemandError::from);
                delta.peak_allocation_bytes = delta.peak_allocation_bytes
                    .checked_add(self.allocations.live_bytes()).ok_or(crate::WorkError::Overflow)?;
                self.work.try_add_assign(&delta)?;
                reconciled.and(resource)
            }

    pub(crate) fn charge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    return resource.admission(delta, 0, &mut || self.with_resource_scope(None, |work| work.charge(delta)));
                }
                self.reconcile_cancellation_registry()?;
                self.check()?;
                self.work.charge(&delta, &self.budget)?;
                Ok(())
            }

    pub(crate) fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
                let resource = self.resource_scope().map_or(Ok(()), |resource| resource.completed(delta, 0));
                let reconciled = self.reconcile_cancellation_registry().map_err(DemandError::from);
                let recorded = self.work.try_add_assign(&delta)
                    .and_then(|()| self.work.verify(self.budget)).map_err(DemandError::from);
                reconciled.and(recorded).and(resource)
            }

    pub(in crate::demand) fn pending_credit(&mut self, kind: crate::materializer::NativeResourceCredit) -> Option<&mut u64> {
                use crate::materializer::NativeResourceCredit as Credit;
                match kind {
                    Credit::Items => Some(&mut self.cancellation_wait.pending_work.items_examined),
                    Credit::ReturnedItems => Some(&mut self.cancellation_wait.pending_work.items_returned),
                    Credit::DurabilityOperations => Some(&mut self.cancellation_wait.pending_native_durability),
                    _ => None,
                }
            }

    pub(crate) fn reserve(&mut self, kind: crate::materializer::NativeResourceCredit, maximum: u64) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    return resource.reserve(kind.delta(maximum), 0, None, &mut || self.with_resource_scope(None, |work| work.reserve(kind, maximum)));
                }
                let mut delta = WorkCounters::UNCHARGED;
                *kind.counter(&mut delta) = maximum;
                self.admit(delta)?;
                let pending = self.pending_credit(kind).map(|pending| pending.checked_add(maximum).ok_or(crate::WorkError::Overflow)).transpose()?;
                *kind.counter(&mut self.budget) -= maximum;
                if let Some(pending) = pending { *self.pending_credit(kind).expect("tracked credit") = pending; }
                Ok(())
            }

    pub(crate) fn refund(&mut self, kind: crate::materializer::NativeResourceCredit, maximum: u64) -> Result<(), DemandError> {
                let pending = self.pending_credit(kind).map(|pending| pending.checked_sub(maximum).ok_or(DemandError::InvalidRequest)).transpose()?;
                let restored = kind.counter(&mut self.budget).checked_add(maximum).ok_or(crate::WorkError::Overflow)?;
                *kind.counter(&mut self.budget) = restored;
                if let Some(pending) = pending { *self.pending_credit(kind).expect("tracked credit") = pending; }
                self.resource_scope().map_or(Ok(()), |resource| resource.refund_credit(kind, maximum, None))
            }

    pub(crate) fn complete(&mut self, kind: crate::materializer::NativeResourceCredit, maximum: u64, actual: u64) -> Result<(), DemandError> {
                use crate::materializer::NativeResourceCredit as Credit;
                if let Some(resource) = self.resource_scope() {
                    let recorded = self.with_resource_scope(None, |work| work.complete(kind, maximum, actual));
                    return recorded.and(resource.complete_credit(kind, maximum, actual));
                }
                let mut delta = WorkCounters::UNCHARGED;
                *kind.counter(&mut delta) = actual;
                if matches!(kind, Credit::Items | Credit::ReturnedItems) {
                    if actual > maximum || self.pending_credit(kind).is_some_and(|pending| maximum > *pending) { return Err(DemandError::InvalidRequest); }
                    self.work.try_add_assign(&delta)?;
                    return self.refund(kind, maximum);
                }
                self.refund(kind, maximum)?;
                let recorded = if matches!(kind, Credit::Copy) { self.record(delta) }
                    else { self.work.try_add_assign(&delta).and_then(|()| self.work.verify(self.budget)).map_err(DemandError::from) };
                if actual > maximum {
                    return Err(if matches!(kind, Credit::Copy) { crate::WorkError::Overflow.into() } else { DemandError::InvalidRequest });
                }
                recorded
            }

    pub(crate) fn operation_credit(kind: NativeOperationKind) -> crate::materializer::NativeResourceCredit {
                use crate::materializer::NativeResourceCredit as Credit;
                match kind {
                    NativeOperationKind::Read => Credit::ReadOperations,
                    NativeOperationKind::Write => Credit::WriteOperations,
                    NativeOperationKind::Durability => Credit::DurabilityOperations,
                }
            }

    pub(crate) fn byte_credit(kind: NativeOperationKind) -> Result<crate::materializer::NativeResourceCredit, std::io::Error> {
                use crate::materializer::NativeResourceCredit as Credit;
                match kind {
                    NativeOperationKind::Read => Ok(Credit::ReadBytes),
                    NativeOperationKind::Write => Ok(Credit::WriteBytes),
                    NativeOperationKind::Durability => Err(std::io::ErrorKind::Unsupported.into()),
                }
            }

    pub(crate) fn record_native_io_failure(&mut self, error: DemandError) -> std::io::Error {
                if self.cancellation_wait.native_io_failure.is_none() {
                    self.cancellation_wait.native_io_failure = Some(error);
                }
                if matches!(self.cancellation_wait.native_io_failure, Some(DemandError::Allocation(_))) {
                    std::io::ErrorKind::OutOfMemory.into()
                } else {
                    std::io::ErrorKind::Other.into()
                }
            }

    pub(crate) fn take_native_io_failure(&mut self) -> Option<DemandError> {
                self.cancellation_wait.native_io_failure.take()
            }

    pub(crate) fn allocate(&mut self, bytes: u64) -> Result<(), DemandError> {
                self.claim_allocation(bytes, u64::from(bytes != 0))
            }

    pub(crate) fn claim_allocation(&mut self, bytes: u64, operations: u64) -> Result<(), DemandError> {
                self.claim_native_owned(bytes, operations, WorkCounters::UNCHARGED, &self.capture_custody())
            }

    pub(in crate::demand) fn allocate_for(&mut self, bytes: u64, delta: WorkCounters) -> Result<(), DemandError> {
                self.claim_native_owned(bytes, u64::from(bytes != 0), delta, &self.capture_custody())
            }

    pub(crate) fn adopt(&mut self, bytes: u64) -> Result<(), DemandError> {
                self.reconcile_cancellation_registry()?;
                self.check()?;
                self.adopt_retained(bytes)
            }

    pub(crate) fn adopt_retained(&mut self, bytes: u64) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    let delta = WorkCounters::UNCHARGED;
                    return resource.admission(delta, bytes, &mut || self.with_resource_scope(None, |work| work.adopt_retained(bytes)));
                }
                self.allocations.claim_bytes(bytes, 0, &mut self.work, self.budget)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                if let Some(custody) = self.capture_custody() { custody.claim(bytes); }
                Ok(())
            }

    pub(crate) fn reconcile_native_allocation(
                &mut self, allocation: &mut u64, actual: u64,
            ) -> Result<(), DemandError> {
                if actual < *allocation {
                    self.release(*allocation - actual)?;
                    *allocation = actual;
                }
                let (claimed, result) = self.record_native_owned_completed(actual - *allocation, &self.capture_custody());
                *allocation += claimed;
                result
            }

    pub(crate) fn record_adoption(&mut self, bytes: u64, capture: &Option<Arc<CaptureTransientCustody>>) -> Result<(), DemandError> {
                let resource = self.resource_scope().map_or(Ok(()), |resource| resource.completed(WorkCounters::UNCHARGED, bytes));
                let reconciled = self.reconcile_cancellation_registry().map_err(DemandError::from);
                let prior = self.allocations.live_bytes();
                let recorded = record_completed_retention(&mut self.allocations, &mut self.work, self.budget, bytes).map_err(DemandError::from);
                if prior.checked_add(bytes) == Some(self.allocations.live_bytes()) {
                    if let Some(capture) = capture { capture.claim(bytes); }
                }
                reconciled.and(recorded).and(resource)
            }

    pub(crate) fn release(&mut self, bytes: u64) -> Result<(), DemandError> {
                if let Some(resource) = self.resource_scope() {
                    let released = self.with_resource_scope(None, |work| work.release(bytes));
                    return released.and(resource.retire(bytes));
                }
                self.allocations
                    .release(bytes)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                if let Some(custody) = self.capture_custody() { custody.release(bytes); }
                Ok(())
            }

    pub(crate) fn remaining(&mut self) -> Result<WorkBudget, DemandError> {
                self.reconcile_native_controls()?;
                let mut budget = self.work.remaining(self.budget)?;
                budget.peak_allocation_bytes = budget.peak_allocation_bytes
                    .checked_sub(self.live_allocation_bytes())
                    .and_then(|bytes| bytes.checked_sub(self.allocations.reserved_bytes()))
                    .ok_or(crate::WorkError::Overflow)?;
                if let Some(resource) = self.resource_scope() { budget = budget.intersect_budget(resource.remaining()?); }
                Ok(budget)
            }

    pub(crate) fn merge(&mut self, work: WorkCounters) -> Result<(), DemandError> {
                self.cancellation_wait.reconcile(&mut self.allocations)?;
                #[cfg(feature = "native-mount")]
                let cleanup = self.cancellation_wait.view_gates.reconcile_work(&mut self.work, &mut self.budget);
                #[cfg(not(feature = "native-mount"))]
                let cleanup: Result<(), crate::WorkError> = Ok(());
                self.work = self.work.with_backend(work, self.allocations.live_bytes())?;
                let growth = self.cancellation_wait.record_observer_growth(
                    &mut self.allocations, &mut self.work, self.budget,
                );
                cleanup?;
                growth?;
                self.work.verify(self.budget)?;
                Ok(())
            }

    pub(crate) fn merge_with_prior_live(
                &mut self, spent: WorkCounters, prior_live: u64,
            ) -> Result<(), DemandError> {
                self.work = self.work.with_backend(spent, prior_live)?;
                self.reconcile_cancellation_registry()?;
                self.work.verify(self.budget)?;
                Ok(())
            }

    pub(crate) fn finish<T>(mut self, result: Result<T, DemandError>) -> DemandResult<T> {
                let reconciled = self.reconcile_cancellation_registry().map_err(DemandError::from);
                reconciled.and(result).map(|value| OperationReceipt { value, work: self.work })
                    .map_err(|error| OperationFailure::new(error, self.work))
            }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for DemandWork {
    fn drop(&mut self) {
        self.reconcile_native_controls().expect("final original native retirement remains balanced");
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl AsMut<DemandWork> for DemandWork {
    fn as_mut(&mut self) -> &mut Self { self }
}
