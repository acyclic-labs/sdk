use super::*;

/// Exact copies admitted before an authoritative root replaces owned buffers.
pub(super) struct PreparedCheckoutRoots {
    pub(super) base: (GenerationRoot, ProofDataClaim), pub(super) held: Option<(GenerationRoot, ProofDataClaim)>,
    pub(super) producer: Option<Arc<ProofDataCustody>>, pub(super) working_control: Option<ProofDataClaim>,
}

/// Sealed proof merge for the private candidate adoption fence. The snapshots
#[cfg(feature = "native-mount")]
pub(crate) struct PreparedCandidateAdoption {
    pub(super) dependencies: CheckoutDependencies, pub(super) live: CheckoutDependencies, pub(super) candidate: CheckoutDependencies,
}

impl<A, O> Checkout<A, O> {

    /// Prepares the exact candidate proof under the original caller's limits.
    #[cfg(feature = "native-mount")]
    pub(crate) fn prepare_candidate_adoption<W: FsWork>(
        &self, base: &Self, candidate: &Self, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Option<PreparedCandidateAdoption>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.prepare_candidate_adoption_with_accounting(base, candidate, &mut accounting);
        budget.finish(result, &accounting)
    }

    #[cfg(feature = "native-mount")]
    pub(super) fn prepare_candidate_adoption_with_accounting(
        &self, base: &Self, candidate: &Self, accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<PreparedCandidateAdoption>, FsError> {
        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if !self.candidate_base_unchanged(base) { return Ok(None); }
        // Only this candidate's original admitted producer can own NEW merge
        let producer = (candidate.dependencies.data_custody.physical_retained_bytes() != 0)
            .then_some(&candidate.dependencies.data_custody);
        let live = self.dependencies.proof().dependencies.clone();
        let candidate = candidate.dependencies.proof().dependencies.clone();
        let Some(dependencies) = candidate.merged_with_accounting(&live, producer, accounting)? else {
            return Ok(None);
        };
        Ok(Some(PreparedCandidateAdoption { dependencies, live, candidate }))
    }

    /// Installs only the proof sealed by preparation. Both snapshots retain
    #[cfg(feature = "native-mount")]
    pub(crate) fn adopt_prepared_candidate(
        &mut self, base: &Self, candidate: Self, prepared: PreparedCandidateAdoption,
    ) -> bool {
        if !self.candidate_base_unchanged(base)
            || !self.dependencies.proof().dependencies.same_capture(&prepared.live)
        { return false; }
        {
            let mut proof = candidate.dependencies.proof();
            if !proof.dependencies.same_capture(&prepared.candidate) { return false; }
            // Retain the candidate's absorbed-walk memory: its sealed proof
            proof.dependencies = prepared.dependencies;
        }
        *self = candidate;
        true
    }

    #[cfg(feature = "native-mount")]
    pub(super) fn candidate_base_unchanged(&self, base: &Self) -> bool {
        let Self {
            volume: _,
            generation_root,
            base_file_table,
            base_root,
            root,
            authority_head,
            authored_operation_id,
            live_operation_id,
            last_commit,
            publication_closure: _,
            prepared_merge_parent,
            dependencies: _, proof_owned_bytes: _,
            mode,
        } = self;
        *generation_root == base.generation_root
            && *base_file_table == base.base_file_table
            && *base_root == base.base_root
            && *root == base.root
            && *authority_head == base.authority_head
            && *authored_operation_id == base.authored_operation_id
            && *live_operation_id == base.live_operation_id
            && *last_commit == base.last_commit
            && *prepared_merge_parent == base.prepared_merge_parent
            && *mode == base.mode
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {

    pub(crate) fn private_candidate<W: FsWork>(
        &self, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Self, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.private_candidate_with_accounting(&mut accounting, cancellation);
        budget.finish(result, &accounting)
    }

    pub(super) fn checkout_custody(
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<Arc<ProofDataCustody>, FsError> {
        cancellation.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let mut header = original.claim_root_metadata(std::mem::size_of::<ProofDataCustody>() as u64)?;
            let custody = Arc::new(ProofDataCustody::default());
            cancellation.check()?;
            original.register_native_checkout_control(&custody, &mut header)?;
            return Ok(custody);
        }
        let (allocations, work, budget, _) = accounting.standalone_parts()
            .ok_or(FsError::HandleAllocationFailed)?;
        allocations.claim_elements::<ProofDataCustody>(1, work, budget)
            .map_err(crate::collection::handle_allocation_error)?;
        Ok(Arc::new(ProofDataCustody::default()))
    }

    pub(super) fn private_candidate_with_accounting(
        &self,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Self, FsError> {
        let custody = Self::checkout_custody(accounting, cancellation)?;
        let copied = WorkCounters {
            bytes_copied: crate::foundation::usize_to_u64(size_of::<Proof>()),
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        let proof_bytes = crate::foundation::usize_to_u64(size_of::<Mutex<Proof>>());
        // This guard is declared before the physical proof so every error
        let mut proof_allocation = accounting.claim_owned(proof_bytes, 1)?;
        // Immutable proof maps remain shared by the canonical Arc COW engine.
        let mut dependencies = self.dependencies.independent_with_custody(custody);
        accounting.record_completed(copied)?;
        dependencies.proof().control = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => {
                let original = accounting.native_work()
                    .ok_or(FsError::HandleAllocationFailed)?;
                let scope = original.checkout_control_sink(&dependencies.data_custody)?;
                match &mut proof_allocation {
                    crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =>
                        allocation.transfer_checkout_control(&dependencies.data_custody, &scope)?,
                    crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                        return Err(FsError::HandleAllocationFailed),
                }
            }
            _ => crate::kernel::ProofDataClaim::control(
                &dependencies.data_custody, proof_bytes,
            ),
        };
        let (base_root, base_control) = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => crate::collection::clone_generation_root_native(
                &self.base_root, accounting, &dependencies.data_custody,
            )?,
            _ => {
                let (base_root, _) = crate::collection::clone_generation_root_with_accounting(
                    &self.base_root, accounting,
                )?;
                let control = crate::kernel::ProofDataClaim::control(
                    &dependencies.data_custody, root_parent_bytes(&base_root)?,
                );
                (base_root, control)
            }
        };
        dependencies.base_root_control = base_control;
        let root = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => self.root.replicate_native_measured(
                accounting, &dependencies.data_custody,
            )?,
            _ => {
                let mut root = self.root.replicate_with_accounting(accounting, None)?;
                root.attach_control_custody(&dependencies.data_custody);
                root
            }
        };
        Ok(self.replica_with_roots(dependencies, self.mode, base_root, root))
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self.proof_owned_bytes
            + crate::foundation::usize_to_u64(self.base_root.parents.capacity() * size_of::<GenerationId>())
            + self.root.logical_owned_bytes()
    }

    pub(crate) fn private_control_custody(&self) -> Arc<crate::kernel::ProofDataCustody> {
        Arc::clone(&self.dependencies.data_custody)
    }

    pub(super) fn root_control_claim(
        root: &GenerationRoot,
        producer: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<crate::kernel::ProofDataClaim, FsError> {
        match producer {
            Some(producer) => Ok(crate::kernel::ProofDataClaim::pending_control(producer, root_parent_bytes(root)?)),
            None => Ok(crate::kernel::ProofDataClaim::default()),
        }
    }

    pub(super) fn prepare_root_replacement(
        &self, base: &GenerationRoot, working: &GenerationRoot,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
        producer: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<PreparedCheckoutRoots, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let producer = Arc::clone(producer.unwrap_or(&self.dependencies.data_custody));
            cancellation.check()?;
            original.cancellation().check()?;
            original.checkout_control_sink(&producer)?;
            let base = crate::collection::clone_generation_root_native(base, accounting, &producer)?;
            cancellation.check()?;
            let held = self.root.prepare_held_root_native(working, accounting, &producer)?;
            return Ok(PreparedCheckoutRoots { base, held, producer: Some(producer), working_control: None });
        }
        let producer = producer.or_else(|| {
            (self.dependencies.data_custody.physical_retained_bytes() != 0)
                .then_some(&self.dependencies.data_custody)
        }).map(Arc::clone);
        cancellation.check()?;
        let (allocations, work, bound, _) = accounting.standalone_parts()
            .ok_or(FsError::HandleAllocationFailed)?;
        let base = crate::collection::clone_generation_root(base, allocations, work, bound, cancellation)?;
        let control = Self::root_control_claim(&base, producer.as_ref())?;
        let base = (base, control);
        cancellation.check()?;
        let held = self.root.prepare_held_root(working, allocations, work, bound, cancellation)?
            .map(|root| {
                let control = Self::root_control_claim(&root, producer.as_ref())?;
                Ok::<_, FsError>((root, control))
            }).transpose()?;
        Ok(PreparedCheckoutRoots { base, held, producer, working_control: None })
    }

    /// Transfers only the authenticated working root's already-paid parent
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn prepare_root_working_control_with_native_work(
        &self,
        working: &GenerationRoot,
        guard: &mut crate::materializer::NativeAllocation,
        prepared: &mut PreparedCheckoutRoots,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        cancellation.check()?;
        let bytes = root_parent_bytes(working)?;
        let producer = prepared.producer.as_ref().ok_or(FsError::HandleAllocationFailed)?;
        // Keep the paid guard outside all accounting leaves on failure.
        original.cancellation().check()?;
        let mut parents = guard.split_off(bytes)?;
        let result = parents.work().checkout_control_sink(producer).and_then(|membership| parents.transfer_checkout_control(producer, &membership)).map_err(FsError::from);
        match result {
            Ok(control) => { prepared.working_control = Some(control); Ok(()) }
            Err(error) => { guard.absorb(&mut parents)?; Err(error) }
        }
    }

    pub(super) fn install_root_replacement(
        &mut self,
        generation: ObjectId,
        working: GenerationRoot,
        prepared: PreparedCheckoutRoots,
    ) {
        let control = prepared.working_control.unwrap_or_else(|| {
            Self::root_control_claim(&working, prepared.producer.as_ref())
                .expect("an admitted generation-root vector has a representable capacity")
        });
        self.root.set_prepared_root(generation, working, control, prepared.held);
        self.base_root = prepared.base.0;
        self.dependencies.base_root_control = prepared.base.1;
    }

    pub(crate) fn proof_data_bytes(
        &self, accounting: &mut KernelAccounting<'_>,
    ) -> Result<u64, FsError> {
        accounting.charge(WorkCounters { items_examined: 2, ..WorkCounters::UNCHARGED })?;
        let proof = self.dependencies.proof();
        proof.dependencies.logical_data().2.checked_add(proof.observed_data_bytes)
            .ok_or(FsError::Work(WorkError::Overflow))
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn into_unpublishable_capture_candidate(mut self) -> Self {
        self.mode = CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::Pinned,
            mutations: MutationMode::PrivateOverlay,
        };
        self.authority_head = None;
        self
    }

    /// Constructs an observing alias with independently admitted roots.
    #[cfg(feature = "native-mount")]
    pub(crate) fn observer<W: FsWork>(
        &self, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Self, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = (|| -> Result<Self, FsError> {
            let accounting = &mut accounting;
            cancellation.check()?;
            if !self.tracks_observations() {
                let mut checkout = self.private_candidate_with_accounting(accounting, cancellation)?;
                checkout.mode = CheckoutMode::read_only_pinned();
                return Ok(checkout);
            }
            let custody = Self::checkout_custody(accounting, cancellation)?;
            accounting.check()?;
            let mut dependencies = self.dependencies.observing_with_custody(custody);
            let base = match accounting {
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { .. } => crate::collection::clone_generation_root_native(
                    &self.base_root, accounting, &dependencies.data_custody,
                )?,
                _ => {
                    let root = crate::collection::clone_generation_root_with_accounting(&self.base_root, accounting)?;
                    if !matches!(root.1, crate::kernel::allocation::KernelOwnedAllocation::Standalone) {
                        return Err(FsError::HandleAllocationFailed);
                    }
                    let control = crate::kernel::ProofDataClaim::control(
                        &dependencies.data_custody, root_parent_bytes(&root.0)?,
                    );
                    (root.0, control)
                }
            };
            let root = match accounting {
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { .. } =>
                    self.root.replicate_native_measured(accounting, &dependencies.data_custody)?,
                _ => {
                    let mut root = self.root.replicate_with_accounting(accounting, None)?;
                    root.attach_control_custody(&dependencies.data_custody);
                    root
                }
            };
            dependencies.base_root_control = base.1;
            Ok(self.replica_with_roots(
                dependencies, CheckoutMode::read_only_pinned(), base.0, root,
            ))
        })();
        budget.finish(result, &accounting)
    }

    /// Constructs an isolated read-only candidate.
    #[cfg(feature = "native-mount")]
    pub(crate) fn inspector<W: FsWork>(
        &self, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Self, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.private_candidate_with_accounting(&mut accounting, cancellation).map(|mut checkout| {
            checkout.mode = CheckoutMode::read_only_pinned();
            checkout
        });
        budget.finish(result, &accounting)
    }

    pub(super) fn replica_with_roots(
        &self,
        dependencies: DependencyLedger,
        mode: CheckoutMode,
        base_root: GenerationRoot,
        root: crate::collection::CheckoutRoots,
    ) -> Self {
        Self {
            volume: self.volume.clone(),
            generation_root: self.generation_root,
            base_file_table: self.base_file_table,
            base_root,
            root,
            authority_head: self.authority_head,
            authored_operation_id: self.authored_operation_id,
            live_operation_id: self.live_operation_id,
            last_commit: self.last_commit,
            publication_closure: None,
            prepared_merge_parent: self.prepared_merge_parent,
            proof_owned_bytes: dependencies.data_custody.logical_owned_bytes()
                + if dependencies.observing { 0 } else { crate::foundation::usize_to_u64(size_of::<Mutex<Proof>>()) },
            dependencies,
            mode,
        }
    }
}

pub(super) fn root_parent_bytes(root: &GenerationRoot) -> Result<u64, FsError> {
    root.parents.capacity().checked_mul(size_of::<GenerationId>())
        .map(crate::foundation::usize_to_u64)
        .ok_or(FsError::Work(WorkError::Overflow))
}

pub(super) fn root_parent_buffer(
    parents: &[GenerationId],
    live_owned_bytes: u64,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<Vec<GenerationId>>, FsError> {
    let mut work = WorkCounters::default();
    let result = (|| {
        cancellation.check()?;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        allocations.claim_bytes(live_owned_bytes, 0, &mut work, budget)
            .map_err(|error| error.into_work_or(FsError::PendingMutationAllocationFailed))?;
        let mut values = Vec::new();
        if !parents.is_empty() {
            cancellation.check()?;
            let admitted = allocations.claim_elements::<GenerationId>(parents.len(), &mut work, budget)
                .map_err(|error| error.into_work_or(FsError::PendingMutationAllocationFailed))?;
            values.try_reserve_exact(parents.len()).map_err(|_| FsError::PendingMutationAllocationFailed)?;
            let actual = crate::foundation::usize_to_u64(values.capacity() * size_of::<GenerationId>());
            if actual > admitted {
                allocations.claim_bytes(actual - admitted, 0, &mut work, WorkBudget::UNBOUNDED)
                    .map_err(|error| error.into_work_or(FsError::PendingMutationAllocationFailed))?;
                work.verify(budget)?;
            }
        }
        let copy_bytes = parents.len().checked_mul(size_of::<GenerationId>())
            .map(crate::foundation::usize_to_u64).ok_or(FsError::Work(WorkError::Overflow))?;
        crate::collection::charge_handle_copy(copy_bytes, &mut work, budget)?;
        values.extend_from_slice(parents);
        Ok(values)
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

