use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub(crate) struct DemandObserverCustody {
    pub(in crate::demand) token: std::sync::Arc<crate::kernel::ProofDataCustody>,
    pub(in crate::demand) adopted_bytes: u64,
    pub(in crate::demand) native_retired: Option<u128>,
}

#[cfg(not(target_arch = "wasm32"))]
impl DemandObserverCustody {
    pub(in crate::demand) fn imported_payload_bytes(&self) -> u64 {
            if self.native_retired.is_some() { self.token.non_native_retained_payload_bytes() }
            else { self.token.retained_payload_bytes() }
        }

    pub(in crate::demand) fn pending_native_retired(&self) -> u64 {
            self.native_retired.as_ref().map_or(0, |previous| {
                let (_, retired) = self.token.native_control_events();
                u64::try_from(retired.checked_sub(*previous)
                    .expect("native control retirement is monotonic"))
                    .expect("unreconciled original retirements cannot exceed its live ledger")
            })
        }

    pub(in crate::demand) fn reconcile_native(&mut self, allocations: &mut crate::kernel::allocation::AllocationLedger) -> Result<(), crate::WorkError> {
            let Some(previous) = &mut self.native_retired else { return Ok(()); };
            let (born, retired) = self.token.native_control_events();
            if retired > born { return Err(crate::WorkError::Overflow); }
            let released = u64::try_from(retired.checked_sub(*previous).ok_or(crate::WorkError::Overflow)?)
                .map_err(|_| crate::WorkError::Overflow)?;
            allocations.release(released).map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
            *previous = retired;
            Ok(())
        }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeCheckoutControlWitness {
    pub(in crate::demand) original: std::sync::Arc<NativeDemandReadMeter>,
    pub(in crate::demand) producer: std::sync::Arc<crate::kernel::ProofDataCustody>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeCheckoutControlWitness {
    pub(crate) fn owns(
            &self, original: &std::sync::Arc<NativeDemandReadMeter>,
            producer: &std::sync::Arc<crate::kernel::ProofDataCustody>,
        ) -> bool {
            std::sync::Arc::ptr_eq(&self.original, original)
                && std::sync::Arc::ptr_eq(&self.producer, producer)
        }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeDemandReadMeter {
    pub(crate) fn checkout_control_witness(
            self: &std::sync::Arc<Self>, producer: &std::sync::Arc<crate::kernel::ProofDataCustody>,
        ) -> Option<NativeCheckoutControlWitness> {
            if !self.lock().owns_native_checkout_token(producer) { return None; }
            Some(NativeCheckoutControlWitness {
                original: std::sync::Arc::clone(self), producer: std::sync::Arc::clone(producer),
            })
        }

    pub(crate) fn register_native_checkout_control(
            self: &Arc<Self>, native: &crate::NativeMaterializationWork,
            producer: &Arc<crate::kernel::ProofDataCustody>,
            header_guard: &mut crate::NativeAllocation,
        ) -> Result<NativeCheckoutControlWitness, crate::NativeTreeMaterializationError> {
            use crate::NativeTreeMaterializationError as Error;
            if !header_guard.is_checkout_header_for(self)
                || producer.retained_payload_bytes() != 0 || producer.native_control_events() != (0, 0) {
                return Err(Error::InvalidLayout);
            }
            let mut work = self.lock();
            work.with_invocation_scope(native.invocation_view_state().map(Arc::downgrade), |work| work.check())
                .map_err(crate::materializer::native_materialization_work_error)?;
            if work.cancellation_wait.owns_native_control(producer) || !producer.bind_native_original(self) {
                return Err(Error::InvalidLayout);
            }
            let work = &mut *work;
            if !work.cancellation_wait.retain_observer(Arc::clone(producer), &mut work.allocations, &mut work.work, work.budget)
                .map_err(crate::materializer::native_materialization_work_error)? {
                return Err(Error::InvalidLayout);
            }
            header_guard.transfer_checkout_metadata(self, std::mem::size_of::<crate::kernel::ProofDataCustody>() as u64)?;
            let mut overflow = work.cancellation_wait.observer_overflow.as_ref().map(|overflow| overflow.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
            let custody = work.cancellation_wait.observers.iter_mut().flatten()
                .chain(overflow.iter_mut().flat_map(|overflow| overflow.iter_mut()))
                .find(|custody| Arc::ptr_eq(&custody.token, producer)).expect("registered checkout control");
            custody.native_retired = Some(0);
            Ok(NativeCheckoutControlWitness { original: Arc::clone(self), producer: Arc::clone(producer) })
        }
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::demand) struct DemandWatchCustody {
    pub(in crate::demand) token: std::sync::Weak<SourceWatchAllocationCustody>,
    pub(in crate::demand) adopted_bytes: u64,
    pub(in crate::demand) pending: bool,
}

#[cfg(not(target_arch = "wasm32"))]
impl DemandWatchCustody {
    pub(in crate::demand) fn current_bytes(&self) -> u64 {
            self.token.upgrade().map_or(0, |token| {
                std::mem::size_of::<SourceWatchAllocationCustody>() as u64 + token.retained_bytes()
            })
        }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
pub(crate) struct DemandCancellationRoots {
    pub(in crate::demand) inline: [Option<(CancellationToken, crate::cancellation::CancellationWaitAdmission)>; 3],
    pub(in crate::demand) overflow: Vec<(CancellationToken, crate::cancellation::CancellationWaitAdmission)>,
    pub(in crate::demand) observers: [Option<DemandObserverCustody>; 3],
    pub(in crate::demand) observer_overflow: Option<std::sync::Arc<std::sync::Mutex<Vec<DemandObserverCustody>>>>,
    pub(in crate::demand) pending_work: WorkCounters,
    pub(in crate::demand) native_io_failure: Option<DemandError>,
    pub(in crate::demand) native_poisoned: bool,
    // Only original durability tokens may consume or restore these credits.
    pub(in crate::demand) pending_native_durability: u64,
    pub(in crate::demand) watches: [Option<DemandWatchCustody>; 3],
    pub(in crate::demand) watch_overflow: Vec<DemandWatchCustody>,
    pub(in crate::demand) watch_capacity: crate::kernel::allocation::LogicalVecCapacity,
    #[cfg(feature = "native-mount")]
    pub(in crate::demand) view_gates: crate::native_mount::ViewGateReadAdmissions,
    #[cfg(feature = "native-mount")]
    pub(in crate::demand) view_observers: crate::native_mount::ViewObserverAdmissions,
    #[cfg(feature = "native-mount")]
    pub(in crate::demand) view_ledgers: crate::native_mount::ViewLedgerAdmissions,
    pub(in crate::demand) native_mount_admissions: NativeMountAllocationAdmissions,
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::demand) fn grow_registry<T>(
    entries: &mut Vec<T>, capacity: usize,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters, budget: WorkBudget,
) -> Result<(), DemandError> {
    let width = std::mem::size_of::<T>();
    let bytes = capacity.checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
    let copied = WorkCounters { bytes_copied: entries.len().checked_mul(width)
        .ok_or(crate::WorkError::Overflow)? as u64, ..WorkCounters::UNCHARGED };
    work.admit(&copied, &budget)?;
    allocations.claim_bytes(bytes, 1, work, budget)
        .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
    let mut replacement = Vec::new();
    if let Err(error) = replacement.try_reserve_exact(capacity) {
        allocations.release(bytes).map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        return Err(DemandError::Allocation(error));
    }
    let actual = replacement.capacity().checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
    let recorded = allocations.record_owned_bytes(actual - bytes, work)
        .map_err(|error| error.into_work_or(DemandError::InvalidRequest));
    replacement.append(entries);
    std::mem::swap(entries, &mut replacement);
    let old_bytes = replacement.capacity().checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
    drop(replacement);
    allocations.release(old_bytes).map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
    work.try_add_assign(&copied)?;
    recorded?;
    work.verify(budget)?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
impl DemandCancellationRoots {
    pub(in crate::demand) fn owns_native_control(&self, token: &std::sync::Arc<crate::kernel::ProofDataCustody>) -> bool {
            let overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
            self.observers.iter().flatten()
                .chain(overflow.iter().flat_map(|overflow| overflow.iter()))
                .any(|custody| custody.native_retired.is_some() && std::sync::Arc::ptr_eq(&custody.token, token))
        }

    pub(in crate::demand) fn reconcile(&mut self, allocations: &mut crate::kernel::allocation::AllocationLedger) -> Result<(), crate::WorkError> {
            for root in self.inline.iter_mut().flatten().chain(self.overflow.iter_mut()) {
                root.1.reconcile_registry(&root.0, allocations)?;
            }
            let mut overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
            for custody in self.observers.iter_mut().flatten().chain(overflow.iter_mut().flat_map(|overflow| overflow.iter_mut())) {
                custody.reconcile_native(allocations)?;
                let current = custody.imported_payload_bytes();
                if current < custody.adopted_bytes {
                    allocations.release(custody.adopted_bytes - current)
                        .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                    custody.adopted_bytes = current;
                }
            }
            drop(overflow);
            for custody in self.watches.iter_mut().flatten().chain(self.watch_overflow.iter_mut()) {
                let current = custody.current_bytes();
                if current < custody.adopted_bytes {
                    allocations.release(custody.adopted_bytes - current)
                        .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                    custody.adopted_bytes = current;
                }
            }
            #[cfg(feature = "native-mount")]
            self.view_gates.reconcile(allocations)?;
            #[cfg(feature = "native-mount")]
            self.view_observers.reconcile(allocations)?;
            #[cfg(feature = "native-mount")]
            self.view_ledgers.reconcile(allocations)?;
            self.native_mount_admissions.reconcile(allocations)?;
            Ok(())
        }

    pub(in crate::demand) fn freed_bytes(&self) -> u64 {
            let freed = self.inline.iter().flatten().chain(self.overflow.iter())
                .map(|root| root.1.freed_registry_bytes(&root.0))
                .sum::<u64>();
            let freed = self.watches.iter().flatten().chain(self.watch_overflow.iter())
                .try_fold(freed, |sum, custody| sum.checked_add(
                    custody.adopted_bytes.saturating_sub(custody.current_bytes()),
                )).expect("original watch backing custody belongs to this ledger");
            #[cfg(feature = "native-mount")]
            let freed = freed.checked_add(self.view_gates.freed_bytes())
                .expect("retained registry proofs cannot exceed the ledger's owned byte range");
            #[cfg(feature = "native-mount")]
            let freed = freed.checked_add(self.view_observers.freed_bytes())
                .expect("retained observer registry proofs belong to the original ledger");
            #[cfg(feature = "native-mount")]
            let freed = freed.checked_add(self.view_ledgers.freed_bytes())
                .expect("known native change-map admissions belong to the original ledger");
            let freed = freed.checked_add(self.native_mount_admissions.freed_bytes())
                .expect("native lifecycle admissions belong to the original ledger");
            let overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
            self.observers.iter().flatten().chain(overflow.iter().flat_map(|overflow| overflow.iter()))
                .try_fold(freed, |sum, custody| sum.checked_add(
                    custody.adopted_bytes.saturating_sub(custody.imported_payload_bytes()),
                ).and_then(|sum| sum.checked_add(custody.pending_native_retired())))
                .expect("owned observer and registry data cannot exceed original ledger custody")
        }

    pub(in crate::demand) fn unrecorded_observer_bytes(&self) -> u64 {
            let overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
            self.observers.iter().flatten().chain(overflow.iter().flat_map(|overflow| overflow.iter()))
                .try_fold(0_u64, |sum, custody| sum.checked_add(
                    custody.imported_payload_bytes().saturating_sub(custody.adopted_bytes),
                ))
                .expect("completed observer data remains representable in its original ledger")
        }

    pub(in crate::demand) fn retain_observer(
            &mut self, token: std::sync::Arc<crate::kernel::ProofDataCustody>,
            allocations: &mut crate::kernel::allocation::AllocationLedger,
            work: &mut WorkCounters, budget: WorkBudget,
        ) -> Result<bool, DemandError> {
            let overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
            for custody in self.observers.iter().flatten().chain(overflow.iter().flat_map(|overflow| overflow.iter())) {
                work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)?;
                if std::sync::Arc::ptr_eq(&custody.token, &token) { return Ok(false); }
            }
            drop(overflow);
            let custody = DemandObserverCustody { token, adopted_bytes: 0, native_retired: None };
            if let Some(index) = self.observers.iter().position(Option::is_none) {
                self.observers[index] = Some(custody);
                return Ok(true);
            }
            if self.observer_overflow.is_none() {
                let bytes = std::mem::size_of::<std::sync::Mutex<Vec<DemandObserverCustody>>>() as u64;
                allocations.claim_bytes(bytes, 1, work, budget)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                self.observer_overflow = Some(std::sync::Arc::new(std::sync::Mutex::<Vec<DemandObserverCustody>>::default()));
            }
            let mut overflow = self.observer_overflow.as_ref().expect("original overflow owner").lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if overflow.len() == overflow.capacity() {
                let capacity = overflow.capacity().max(1).checked_mul(2).ok_or(crate::WorkError::Overflow)?;
                grow_registry(&mut overflow, capacity, allocations, work, budget)?;
            }
            overflow.push(custody);
            Ok(true)
        }

    pub(in crate::demand) fn prepare_watch(
            &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
            work: &mut WorkCounters, budget: WorkBudget,
        ) -> Result<usize, DemandError> {
            for (index, custody) in self.watches.iter_mut().enumerate() {
                work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, &budget)?;
                if custody.as_ref().is_none_or(|custody| !custody.pending && custody.adopted_bytes == 0 && custody.token.strong_count() == 0) {
                    *custody = Some(DemandWatchCustody { token: std::sync::Weak::new(), adopted_bytes: 0, pending: true });
                    return Ok(index);
                }
            }
            for (index, custody) in self.watch_overflow.iter_mut().enumerate() {
                work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, &budget)?;
                if !custody.pending && custody.adopted_bytes == 0 && custody.token.strong_count() == 0 {
                    let index = self.watches.len().checked_add(index).ok_or(crate::WorkError::Overflow)?;
                    *custody = DemandWatchCustody { token: std::sync::Weak::new(), adopted_bytes: 0, pending: true };
                    return Ok(index);
                }
            }
            if let Err(error) = self.watch_capacity.ensure_for_push::<DemandWatchCustody>(
                &mut self.watch_overflow, usize::MAX, allocations, work, budget,
            ) {
                return Err(error.into_work_or(DemandError::WorkerUnavailable));
            }
            let index = self.watches.len().checked_add(self.watch_overflow.len()).ok_or(crate::WorkError::Overflow)?;
            self.watch_overflow.push(DemandWatchCustody { token: std::sync::Weak::new(), adopted_bytes: 0, pending: true });
            Ok(index)
        }

    pub(in crate::demand) fn retain_prepared_watch(
            &mut self, prepared: usize, token: &std::sync::Arc<SourceWatchAllocationCustody>,
            allocations: &mut crate::kernel::allocation::AllocationLedger,
            work: &mut WorkCounters, budget: WorkBudget,
        ) -> Result<(), DemandError> {
            let custody = if prepared < self.watches.len() {
                self.watches[prepared].as_mut().expect("prepared original watch slot")
            } else { &mut self.watch_overflow[prepared - self.watches.len()] };
            custody.token = std::sync::Arc::downgrade(token);
            let bytes = custody.current_bytes();
            let recorded = record_completed_retention(allocations, work, budget, bytes);
            custody.adopted_bytes = bytes;
            custody.pending = false;
            recorded.map_err(DemandError::from)
        }

    pub(in crate::demand) fn abandon_prepared_watch(&mut self, prepared: usize) {
            let custody = if prepared < self.watches.len() {
                self.watches[prepared].as_mut().expect("prepared original watch slot")
            } else { &mut self.watch_overflow[prepared - self.watches.len()] };
            custody.pending = false;
        }

    pub(in crate::demand) fn record_observer_growth(
            &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
            work: &mut WorkCounters, budget: WorkBudget,
        ) -> Result<(), crate::WorkError> {
            let mut failure = None;
            let mut overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
            for custody in self.observers.iter_mut().flatten().chain(overflow.iter_mut().flat_map(|overflow| overflow.iter_mut())) {
                custody.reconcile_native(allocations)?;
                let current = custody.imported_payload_bytes();
                if current > custody.adopted_bytes {
                    let delta = current - custody.adopted_bytes;
                    let recorded = record_completed_retention(allocations, work, budget, delta);
                    custody.adopted_bytes = current;
                    if let Err(error) = recorded { failure.get_or_insert(error); }
                }
            }
            failure.map_or(Ok(()), Err)
        }

    pub(in crate::demand) fn find(
            &mut self, token: &CancellationToken, allocations: &mut crate::kernel::allocation::AllocationLedger,
            work: &mut WorkCounters, budget: WorkBudget,
        ) -> Result<&mut crate::cancellation::CancellationWaitAdmission, crate::cancellation::CancellationWaitError> {
            let mut found = None;
            for (index, root) in self.inline.iter().enumerate() {
                if let Some(root) = root {
                    work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)
                        .map_err(crate::cancellation::CancellationWaitError::Work)?;
                    if root.0.same_state(token) { found = Some(index); break; }
                }
            }
            if let Some(index) = found {
                return Ok(&mut self.inline[index].as_mut().expect("selected cancellation root").1);
            }
            let mut found = None;
            for (index, root) in self.overflow.iter().enumerate() {
                work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)
                    .map_err(crate::cancellation::CancellationWaitError::Work)?;
                if root.0.same_state(token) { found = Some(index); break; }
            }
            if let Some(index) = found { return Ok(&mut self.overflow[index].1); }
            let root = (token.clone(), Default::default());
            if let Some(index) = self.inline.iter().position(Option::is_none) {
                self.inline[index] = Some(root);
                return Ok(&mut self.inline[index].as_mut().expect("inserted cancellation root").1);
            }
            if self.overflow.len() == self.overflow.capacity() {
                let next = self.overflow.len().checked_add(1)
                    .ok_or(crate::cancellation::CancellationWaitError::Work(crate::WorkError::Overflow))?;
                let prior_peak = work.peak_allocation_bytes;
                if let Err(error) = grow_registry(&mut self.overflow, next, allocations, work, budget) {
                    return Err(match error {
                        DemandError::Allocation(_) => {
                            work.peak_allocation_bytes = prior_peak;
                            crate::cancellation::CancellationWaitError::AllocationFailed
                        }
                        DemandError::Work(error) => crate::cancellation::CancellationWaitError::Work(error),
                        _ => crate::cancellation::CancellationWaitError::Work(crate::WorkError::Overflow),
                    });
                }
            }
            self.overflow.push(root);
            Ok(&mut self.overflow.last_mut().expect("inserted overflow cancellation root").1)
        }

    pub(in crate::demand) fn for_prepared(&mut self, prepared: &crate::cancellation::PreparedCancelled) -> Option<&mut crate::cancellation::CancellationWaitAdmission> {
            self.inline.iter_mut().flatten().chain(self.overflow.iter_mut())
                .find(|root| prepared.same_token(&root.0))
                .map(|root| &mut root.1)
        }
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::demand) fn record_completed_retention(
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters, budget: WorkBudget, bytes: u64,
) -> Result<(), crate::WorkError> {
    if let Err(error) = allocations.claim_bytes(bytes, 0, work, budget) {
        let failure = error.into_work_or(crate::WorkError::Overflow);
        allocations.claim_bytes(bytes, 0, work, WorkBudget::UNBOUNDED)
            .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        return Err(failure);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
impl DemandWork {
    #[cfg(feature = "native-mount")]
            pub(crate) fn view_gate_admission_parts(&mut self) -> Result<(
                &mut crate::native_mount::ViewGateReadAdmissions,
                &mut crate::kernel::allocation::AllocationLedger,
                &mut WorkCounters,
                WorkBudget,
            ), DemandError> {
                self.reconcile_cancellation_registry()?;
                Ok((&mut self.cancellation_wait.view_gates, &mut self.allocations, &mut self.work, self.budget))
            }

    #[cfg(feature = "native-mount")]
            pub(crate) fn view_observer_admission_parts(&mut self) -> Result<(
                &mut crate::native_mount::ViewObserverAdmissions,
                &mut crate::kernel::allocation::AllocationLedger,
                &mut WorkCounters,
                WorkBudget,
            ), DemandError> {
                self.reconcile_cancellation_registry()?;
                Ok((&mut self.cancellation_wait.view_observers, &mut self.allocations, &mut self.work, self.budget))
            }

    #[cfg(feature = "native-mount")]
            pub(crate) fn view_ledger_admission_parts(&mut self) -> Result<(
                &mut crate::native_mount::ViewLedgerAdmissions,
                &mut crate::kernel::allocation::AllocationLedger,
                &mut WorkCounters,
                WorkBudget,
            ), DemandError> {
                self.reconcile_cancellation_registry()?;
                Ok((&mut self.cancellation_wait.view_ledgers, &mut self.allocations, &mut self.work, self.budget))
            }

    #[cfg(feature = "native-mount")]
            pub(crate) fn restore_view_ledger_admission(
                &mut self, slot: usize, admission: crate::native_mount::ViewLedgerAdmission,
            ) {
                self.cancellation_wait.view_ledgers.restore(slot, admission);
            }

    #[cfg(not(target_arch = "wasm32"))]
            pub(crate) fn native_mount_admission_parts(&mut self) -> Result<(
                &mut NativeMountAllocationAdmissions,
                &mut crate::kernel::allocation::AllocationLedger,
                &mut WorkCounters,
                WorkBudget,
            ), DemandError> {
                self.reconcile_cancellation_registry()?;
                Ok((&mut self.cancellation_wait.native_mount_admissions, &mut self.allocations, &mut self.work, self.budget))
            }

    #[cfg(not(target_arch = "wasm32"))]
            pub(crate) fn native_mount_completed_admission_parts(&mut self) -> (
                &mut NativeMountAllocationAdmissions,
                &mut crate::kernel::allocation::AllocationLedger,
                &mut WorkCounters,
                WorkBudget,
            ) {
                (&mut self.cancellation_wait.native_mount_admissions, &mut self.allocations, &mut self.work, self.budget)
            }

    pub(crate) fn prepare_cancelled(
                &mut self, token: &CancellationToken,
            ) -> Result<crate::cancellation::PreparedCancelled, crate::cancellation::CancellationWaitError> {
                self.reconcile_cancellation_registry().map_err(crate::cancellation::CancellationWaitError::Work)?;
                if token.is_cancelled() {
                    return token.prepare_cancelled_measured(
                        &mut self.allocations, &mut self.work, self.budget, &mut Default::default(),
                    );
                }
                let admission = self.cancellation_wait.find(token, &mut self.allocations, &mut self.work, self.budget)?;
                token.prepare_cancelled_measured(&mut self.allocations, &mut self.work, self.budget, admission)
            }

    pub(crate) fn finish_cancelled(
                &mut self, prepared: crate::cancellation::PreparedCancelled,
            ) -> Result<(), crate::cancellation::CancellationWaitError> {
                let reconciled = self.reconcile_cancellation_registry().map_err(crate::cancellation::CancellationWaitError::Work);
                let finished = if let Some(admission) = self.cancellation_wait.for_prepared(&prepared) {
                    prepared.finish(&mut self.allocations, &mut self.work, self.budget, admission)
                } else {
                    assert_eq!(prepared.retained_waiter_bytes(), 0, "prepared cancellation retains its original root proof");
                    prepared.finish(&mut self.allocations, &mut self.work, self.budget, &mut Default::default())
                };
                reconciled.and(finished)
            }

    pub(crate) fn new_cancellation(&mut self) -> Result<CancellationToken, crate::WorkError> {
                self.reconcile_cancellation_registry()?;
                CancellationToken::new_measured(&mut self.allocations, &mut self.work, self.budget)
            }

    pub(in crate::demand) fn reconcile_cancellation_registry(&mut self) -> Result<(), crate::WorkError> {
                self.cancellation_wait.reconcile(&mut self.allocations)?;
                #[cfg(feature = "native-mount")]
                self.cancellation_wait.view_gates.reconcile_work(&mut self.work, &mut self.budget)?;
                self.cancellation_wait.record_observer_growth(&mut self.allocations, &mut self.work, self.budget)?;
                Ok(())
            }

    pub(crate) fn retain_observer_custody(
                &mut self, token: std::sync::Arc<crate::kernel::ProofDataCustody>,
            ) -> Result<(), DemandError> {
                self.reconcile_cancellation_registry()?;
                self.check()?;
                self.cancellation_wait.retain_observer(
                    token, &mut self.allocations, &mut self.work, self.budget,
                ).map(|_| ())
            }

    pub(crate) fn retain_checkout_control(
                &mut self, token: &std::sync::Arc<crate::kernel::ProofDataCustody>,
            ) -> Result<(), DemandError> {
                self.reconcile_cancellation_registry()?;
                let inserted = self.cancellation_wait.retain_observer(
                    std::sync::Arc::clone(token), &mut self.allocations, &mut self.work, self.budget,
                )?;
                let metadata = if inserted {
                    record_completed_retention(
                        &mut self.allocations, &mut self.work, self.budget, token.logical_owned_bytes(),
                    ).map_err(DemandError::from)
                } else { Ok(()) };
                let payload = self.cancellation_wait.record_observer_growth(
                    &mut self.allocations, &mut self.work, self.budget,
                ).map_err(DemandError::from);
                metadata.and(payload)
            }

    pub(crate) fn prepare_watch_custody(&mut self) -> Result<usize, DemandError> {
                self.reconcile_cancellation_registry()?;
                self.check()?;
                self.cancellation_wait.prepare_watch(&mut self.allocations, &mut self.work, self.budget)
            }

    pub(crate) fn retain_prepared_watch_custody(
                &mut self, prepared: usize, token: &std::sync::Arc<SourceWatchAllocationCustody>,
            ) -> Result<(), DemandError> {
                self.cancellation_wait.retain_prepared_watch(
                    prepared, token, &mut self.allocations, &mut self.work, self.budget,
                )
            }

    pub(crate) fn abandon_prepared_watch_custody(&mut self, prepared: usize) {
                self.cancellation_wait.abandon_prepared_watch(prepared);
            }

    pub(crate) fn owns_native_checkout_token(
                &self, token: &std::sync::Arc<crate::kernel::ProofDataCustody>,
            ) -> bool {
                self.cancellation_wait.owns_native_control(token)
            }

    pub(crate) fn reconcile_native_controls(&mut self) -> Result<(), crate::WorkError> {
                let mut overflow = self.cancellation_wait.observer_overflow.as_ref().map(|overflow| overflow.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
                for custody in self.cancellation_wait.observers.iter_mut().flatten()
                    .chain(overflow.iter_mut().flat_map(|overflow| overflow.iter_mut())) {
                    custody.reconcile_native(&mut self.allocations)?;
                }
                Ok(())
            }
}
