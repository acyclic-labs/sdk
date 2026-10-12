use super::*;

/// Tracking proof of one checkout, shared with that checkout's observers.
pub(super) struct DependencyLedger {
    pub(super) proof: Arc<Mutex<Proof>>,
    pub(super) observing: bool,
    pub(super) data_custody: Arc<ProofDataCustody>, pub(super) accounting: parking_lot::Mutex<ProofDataAccounting>,
    // Checkout's base-root Vec drops before its ledger in declaration order.
    pub(super) base_root_control: ProofDataClaim,
}

/// One checkout's tracking proof, with the base paths whose complete
#[derive(Clone)]
pub(super) struct Proof {
    pub(super) dependencies: CheckoutDependencies,
    pub(super) observed_base: Option<ObjectId>,
    /// Each observed path, and whether its terminal record was observed.
    pub(super) observed_paths: Option<Arc<ObservedPathMap>>, pub(super) observed_data_bytes: u64, pub(super) observed_clone_data_bytes: u64, pub(super) observed_data_generation: u64,
    // Last: the actual Mutex payload keeps its claim across proof Arc aliases.
    pub(super) control: ProofDataClaim,
}

#[derive(Clone)]
pub(super) struct ObservedPathData { terminal: bool, pub(super) data: ProofDataClaim, }

impl PartialEq for ObservedPathData { fn eq(&self, other: &Self) -> bool { self.terminal == other.terminal } }


/// One actual Arc payload; its keys and DATA retire before its control claim.
#[derive(Default)]
pub(super) struct ObservedPathMap { entries: BTreeMap<NamespacePath, ObservedPathData>, pub(super) control: ProofDataClaim, }

impl Clone for ObservedPathMap {
    fn clone(&self) -> Self { Self { entries: self.entries.clone(), control: ProofDataClaim::default() } }
}

impl PartialEq for ObservedPathMap { fn eq(&self, other: &Self) -> bool { self.entries == other.entries } }


#[derive(Default)]
pub(super) struct ProofDataAccounting { allocations: AllocationLedger, }

pub(super) enum ProofDataAdmissionLedger<'a> {
    Local { accounting: &'a mut ProofDataAccounting, work: &'a mut WorkCounters, budget: WorkBudget, },
    #[cfg(not(target_arch = "wasm32"))]
    Original {
        original: &'a NativeMaterializationWork, staged: &'a mut NativeAllocation, pending: Option<NativeAllocation>,
        witness: crate::demand::NativeCheckoutControlWitness,
    },
}

pub(super) struct ProofDataAdmission<'a> {
    pub(super) ledger: ProofDataAdmissionLedger<'a>, pub(super) owner: &'a Arc<ProofDataCustody>, pub(super) cancellation: &'a CancellationToken,
}

impl ProofDataAdmission<'_> {

    pub(super) fn reconcile(&mut self) -> Result<(), FsError> {
        let accounting = match &mut self.ledger {
            ProofDataAdmissionLedger::Local { accounting, .. } => accounting,
            #[cfg(not(target_arch = "wasm32"))]
            ProofDataAdmissionLedger::Original { .. } => return Ok(()),
        };
        let (retained, _) = self.owner.live_payload_counts();
        let live = accounting.allocations.live_bytes();
        if retained > live {
            // These exact same-producer payloads were admitted by their
            let mut mirrored = WorkCounters::default();
            accounting.allocations.claim_bytes(
                retained - live, 0, &mut mirrored, WorkBudget::UNBOUNDED,
            ).map_err(crate::collection::handle_allocation_error)
        } else { accounting.allocations.release(live - retained) .map_err(crate::collection::handle_allocation_error) }
    }

    pub(super) fn admit(&mut self, bytes: u64, operations: u64, copied: u64, items: u64) -> Result<(), FsError> {
        self.cancellation.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let ProofDataAdmissionLedger::Original { original, pending, .. } = &mut self.ledger {
            let delta = WorkCounters { bytes_copied: copied, items_examined: items, ..WorkCounters::UNCHARGED };
            if operations != 0 || copied != 0 {
                if pending.as_ref().is_some_and(|guard| guard.bytes() != 0) {
                    return Err(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout));
                }
                let allocation = original.claim(bytes, operations, delta)?;
                *pending = Some(allocation);
            } else { original.charge(delta)?; }
            return Ok(());
        }
        self.reconcile()?;
        let (accounting, caller_work, budget) = match &mut self.ledger {
            ProofDataAdmissionLedger::Local { accounting, work, budget } => { (accounting, work, *budget) }
            #[cfg(not(target_arch = "wasm32"))]
            ProofDataAdmissionLedger::Original { .. } => unreachable!("original admission returned"),
        };
        let (_, standing) = self.owner.live_payload_counts();
        let mut allocations = accounting.allocations;
        let mut work = **caller_work;
        work.charge_items(items, &budget)?;
        crate::collection::charge_handle_copy(copied, &mut work, budget)?;
        // Delivered DATA and constructor/control payloads already belong to
        let earlier_peak = work.peak_allocation_bytes;
        work.peak_allocation_bytes = 0;
        let external = self.owner.operation_cache_bytes();
        let local_live = allocations.live_bytes().checked_sub(standing)
            .and_then(|live| live.checked_add(bytes)).ok_or(WorkError::Overflow)?;
        let peak = local_live.checked_add(external).ok_or(WorkError::Overflow)?;
        if peak > budget.peak_allocation_bytes {
            return Err(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", observed: peak, maximum: budget.peak_allocation_bytes,
            }.into());
        }
        let mut admission_budget = budget;
        admission_budget.peak_allocation_bytes = budget.peak_allocation_bytes
            .checked_sub(external).ok_or(WorkError::Overflow)?.saturating_add(standing);
        allocations.claim_bytes(bytes, operations, &mut work, admission_budget).map_err(crate::collection::handle_allocation_error)?;
        work.peak_allocation_bytes = earlier_peak.max(peak);
        work.verify(budget)?;
        self.owner.record_data_work(WorkCounters {
            items_examined: items, bytes_copied: copied, allocation_operations: operations, peak_allocation_bytes: peak,
            ..WorkCounters::default()
        })?;
        accounting.allocations = allocations;
        **caller_work = work;
        Ok(())
    }

    pub(super) fn control_claim(&mut self, bytes: u64) -> Result<ProofDataClaim, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let ProofDataAdmissionLedger::Original { pending, witness, .. } = &mut self.ledger {
            let pending = pending.as_mut().ok_or(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout))?;
            let mut allocation = pending.split_off(bytes)?;
            return allocation.transfer_checkout_control(self.owner, witness).map_err(Into::into);
        }
        Ok(ProofDataClaim::pending_control(self.owner, bytes))
    }

    pub(super) fn data_claim(&mut self, bytes: u64) -> Result<ProofDataClaim, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let ProofDataAdmissionLedger::Original { pending, witness, .. } = &mut self.ledger {
            let pending = pending.as_mut().ok_or(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout))?;
            return pending.transfer_checkout_data_bytes(bytes, self.owner, witness).map_err(Into::into);
        }
        Ok(ProofDataClaim::new(Some(self.owner), bytes))
    }
}

pub(super) fn path_clone_data(path: &NamespacePath) -> (u64, u64) {
    let bytes = (path.components().len() * size_of::<LogicalName>()) as u64
        + path.components().iter().map(|name| name.as_bytes().len() as u64).sum::<u64>();
    let operations = u64::from(!path.components().is_empty())
        + path.components().iter().filter(|name| !name.as_bytes().is_empty()).count() as u64;
    (bytes, operations)
}

impl Proof {

    pub(super) fn new(dependencies: CheckoutDependencies) -> Self {
        Self::new_with_control(dependencies, ProofDataClaim::default())
    }

    pub(super) fn new_with_control(dependencies: CheckoutDependencies, control: ProofDataClaim) -> Self {
        Self {
            dependencies,
            observed_base: None,
            observed_paths: Some(Arc::new(ObservedPathMap { entries: BTreeMap::new(), control })),
            observed_data_bytes: 0, observed_clone_data_bytes: 0, observed_data_generation: 0, control: ProofDataClaim::default(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.dependencies.clear();
        self.observed_paths = None;
        self.observed_base = None;
        self.observed_data_bytes = 0;
        self.observed_clone_data_bytes = 0;
        self.observed_data_generation = self.observed_data_generation.checked_add(1).expect("proof allocation generation exhausted");
    }

    /// Whether observing `path` against `base`, with its terminal when
    pub(super) fn observes_path(&self, base: ObjectId, path: &NamespacePath, terminal: bool) -> bool {
        self.observed_base == Some(base)
            && self.observed_paths.as_deref().is_some_and(|observed| {
                observed.entries.get(path).is_some_and(|entry| entry.terminal || !terminal)
            })
    }

    /// Absorbs an observation through the same bounded canonical admission.
    pub(super) fn observe_paths<'a>(
        &mut self,
        base: ObjectId,
        paths: impl IntoIterator<Item = &'a NamespacePath>,
        terminal: bool,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
        admission: &mut ProofDataAdmission<'_>,
    ) -> Result<(), FsError> {
        self.extend_observations(dependencies, maximum_dependencies, admission)?;
        let maximum_paths = usize::try_from(maximum_dependencies).unwrap_or(usize::MAX);
        if self.observed_base != Some(base) || self.observed_paths.is_none() {
            let generation = self.observed_data_generation.checked_add(1).ok_or(WorkError::Overflow)?;
            admission.admit(size_of::<ObservedPathMap>() as u64, 1, 0, 0)?;
            let control = admission.control_claim(size_of::<ObservedPathMap>() as u64)?;
            self.observed_paths = Some(Arc::new(ObservedPathMap { entries: BTreeMap::new(), control }));
            self.observed_base = Some(base);
            self.observed_data_bytes = 0;
            self.observed_clone_data_bytes = 0;
            self.observed_data_generation = generation;
            admission.reconcile()?;
        }
        for path in paths {
            admission.admit(0, 0, 0, 1)?;
            if self.observes_path(base, path, terminal) {
                continue;
            }
            let observed = self.observed_paths.as_deref().expect("the observed base owns a map");
            if !observed.entries.contains_key(path) && observed.entries.len() >= maximum_paths {
                let generation = self.observed_data_generation.checked_add(1).ok_or(WorkError::Overflow)?;
                admission.admit(size_of::<ObservedPathMap>() as u64, 1, 0, 0)?;
                let control = admission.control_claim(size_of::<ObservedPathMap>() as u64)?;
                self.observed_paths = Some(Arc::new(ObservedPathMap { entries: BTreeMap::new(), control }));
                self.observed_data_bytes = 0;
                self.observed_clone_data_bytes = 0;
                self.observed_data_generation = generation;
                admission.reconcile()?;
            }
            let observed_paths = self.observed_paths.as_mut().expect("the observed base owns a map");
            let copied = Arc::strong_count(observed_paths) > 1;
            let moved = !copied && Arc::weak_count(observed_paths) != 0;
            let generation = if copied || moved {
                self.observed_data_generation.checked_add(1).ok_or(WorkError::Overflow)?
            } else { self.observed_data_generation };
            let mut cloned_operations = 0_u64;
            if copied {
                for old in observed_paths.entries.keys() {
                    admission.admit(0, 0, 0, 1)?;
                    cloned_operations = cloned_operations.checked_add(path_clone_data(old).1).ok_or(WorkError::Overflow)?;
                }
            }
            if copied || moved {
                let (bytes, operations, copied_bytes) = if copied {
                    (
                        (size_of::<ObservedPathMap>() as u64).checked_add(self.observed_clone_data_bytes).ok_or(WorkError::Overflow)?,
                        cloned_operations.checked_add(1).ok_or(WorkError::Overflow)?, self.observed_clone_data_bytes,
                    )
                } else { (size_of::<ObservedPathMap>() as u64, 1, 0) };
                admission.admit(bytes, operations, copied_bytes, 0)?;
                let control = admission.control_claim(size_of::<ObservedPathMap>() as u64)?;
                let observed = Arc::make_mut(observed_paths);
                observed.control = control;
                if copied {
                    for (path, entry) in observed.entries.iter_mut() {
                        entry.data = admission.data_claim(path.retained_bytes())?;
                    }
                    self.observed_data_bytes = self.observed_clone_data_bytes;
                }
            }
            self.observed_data_generation = generation;
            let observed = Arc::make_mut(observed_paths);
            if let Some(entry) = observed.entries.get_mut(path) { entry.terminal |= terminal; continue; }
            let (bytes, operations) = path_clone_data(path);
            let retained = self.observed_data_bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
            let cloned = self.observed_clone_data_bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
            admission.admit(bytes, operations, bytes, 0)?;
            let path = path.clone();
            let data = admission.data_claim(path.retained_bytes())?;
            observed.entries.insert(path, ObservedPathData { terminal, data });
            self.observed_data_bytes = retained;
            self.observed_clone_data_bytes = cloned;
        }
        Ok(())
    }

    pub(super) fn extend_observations(
        &mut self, dependencies: Vec<Dependency>, maximum_dependencies: u32, admission: &mut ProofDataAdmission<'_>,
    ) -> Result<(), FsError> {
        let extension = self.dependencies.prepare_observations(dependencies, maximum_dependencies)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let ProofDataAdmissionLedger::Original { original, staged, .. } = &mut admission.ledger {
            return self.dependencies.commit_with_original_data_custody(
                extension, admission.owner, staged, original, admission.cancellation,
            );
        }
        let owner = Arc::clone(admission.owner);
        self.dependencies.commit_with_data_custody(
            extension, Some(&owner), |bytes, operations, copied, items| {
                admission.admit(bytes, operations, copied, items)
            },
        )
    }
}

impl std::ops::Deref for Proof {
    type Target = CheckoutDependencies;

    fn deref(&self) -> &Self::Target {
        &self.dependencies
    }
}

impl std::ops::DerefMut for Proof {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dependencies
    }
}

impl DependencyLedger {

    pub(super) fn new(dependencies: CheckoutDependencies) -> Self {
        Self::sharing(Proof::new(dependencies))
    }

    /// Constructs already admitted native map and proof payloads from their
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn new_with_native_custody(
        dependencies: CheckoutDependencies, data_custody: Arc<ProofDataCustody>, proof_control: ProofDataClaim,
        paths_control: ProofDataClaim,
    ) -> Self {
        let mut proof = Proof::new_with_control(dependencies, paths_control);
        proof.control = proof_control;
        Self::sharing_with_custody(proof, data_custody)
    }

    pub(super) fn sharing(proof: Proof) -> Self { Self::sharing_with_custody(proof, Arc::default()) }

    pub(super) fn sharing_with_custody(proof: Proof, data_custody: Arc<ProofDataCustody>) -> Self {
        Self {
            proof: Arc::new(Mutex::new(proof)),
            observing: false,
            data_custody, accounting: parking_lot::Mutex::default(), base_root_control: ProofDataClaim::default(),
        }
    }

    pub(super) fn proof(&self) -> MutexGuard<'_, Proof> {
        self.proof.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(feature = "native-mount")]
    pub(super) fn observing_with_custody(&self, data_custody: Arc<ProofDataCustody>) -> Self {
        Self {
            proof: Arc::clone(&self.proof),
            observing: true,
            data_custody, accounting: parking_lot::Mutex::default(), base_root_control: ProofDataClaim::default(),
        }
    }

    /// Uses the already admitted token so its original owner can register
    pub(super) fn independent_with_custody(&self, data_custody: Arc<ProofDataCustody>) -> Self {
        Self {
            proof: Arc::new(Mutex::new(self.proof().clone())), observing: false, data_custody,
            accounting: parking_lot::Mutex::default(), base_root_control: ProofDataClaim::default(),
        }
    }

    pub(super) fn observe_paths<'a>(
        &self, base: ObjectId, paths: impl IntoIterator<Item = &'a NamespacePath>, terminal: bool,
        dependencies: Vec<Dependency>, maximum_dependencies: u32, work: &mut WorkCounters, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let mut proof = self.proof();
        let mut accounting = self.accounting.lock();
        proof.observe_paths(base, paths, terminal, dependencies, maximum_dependencies, &mut ProofDataAdmission {
            ledger: ProofDataAdmissionLedger::Local { accounting: &mut accounting, work, budget }, owner: &self.data_custody, cancellation,
        })
    }

    pub(super) fn extend_observations(
        &self, dependencies: Vec<Dependency>, maximum_dependencies: u32, work: &mut WorkCounters, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let mut proof = self.proof();
        let mut accounting = self.accounting.lock();
        proof.extend_observations(dependencies, maximum_dependencies, &mut ProofDataAdmission {
            ledger: ProofDataAdmissionLedger::Local { accounting: &mut accounting, work, budget }, owner: &self.data_custody, cancellation,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn observe_paths_with_native_work<'a>(
        &self, base: ObjectId, paths: impl IntoIterator<Item = &'a NamespacePath>, terminal: bool,
        dependencies: Vec<Dependency>, staged: &mut NativeAllocation, maximum_dependencies: u32,
        original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let witness = original.checkout_control_sink(&self.data_custody)?;
        self.proof().observe_paths(
            base, paths, terminal, dependencies, maximum_dependencies, &mut ProofDataAdmission {
                ledger: ProofDataAdmissionLedger::Original { original, staged, pending: None, witness },
                owner: &self.data_custody, cancellation,
            },
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn extend_observations_with_native_work(
        &self, dependencies: Vec<Dependency>, staged: &mut NativeAllocation, maximum_dependencies: u32,
        original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let witness = original.checkout_control_sink(&self.data_custody)?;
        self.proof().extend_observations(
            dependencies, maximum_dependencies, &mut ProofDataAdmission {
                ledger: ProofDataAdmissionLedger::Original { original, staged, pending: None, witness },
                owner: &self.data_custody, cancellation,
            },
        )
    }

    pub(super) fn copy_observation_name_with_accounting(
        &self, name: &LogicalName, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(LogicalName, KernelOwnedAllocation), FsError> {
        cancellation.check()?;
        let bytes = name.as_bytes().len() as u64;
        let delta = WorkCounters { bytes_copied: bytes, items_examined: 1, ..WorkCounters::UNCHARGED };
        accounting.admit(delta)?;
        let allocation = accounting.claim_owned(bytes, u64::from(bytes != 0))?;
        let name = name.clone();
        let mut copied = (name, allocation);
        let actual = copied.0.retained_bytes() as u64;
        let capacity = accounting.reconcile_owned(&mut copied.1, bytes, actual);
        let completed = accounting.record_completed(delta);
        capacity?;
        completed?;
        cancellation.check()?;
        Ok((copied.0, copied.1))
    }
}

#[cfg(feature = "native-mount")]
impl<A, O> Checkout<A, O> {

    /// Drains only the undelivered receipt report, never the operative ledger.
    pub(crate) fn proof_data_take_completed_work(&self) -> WorkCounters {
        self.dependencies.data_custody.take_completed_data_work()
    }

    /// Custody control only; retaining this never retains a proof allocation.
    pub(crate) fn proof_data_custody(&self) -> Arc<ProofDataCustody> {
        Arc::clone(&self.dependencies.data_custody)
    }
}

