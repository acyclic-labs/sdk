use super::*;

pub(super) struct DependencyLedger {
    pub(super) proof: Arc<Mutex<Proof>>,
    pub(super) observing: bool,
    pub(super) data_custody: Arc<ProofDataCustody>,
    pub(super) accounting: parking_lot::Mutex<AllocationLedger>,
    pub(super) base_root_control: ProofDataClaim,
}

#[derive(Clone)]
pub(super) struct Proof {
    pub(super) dependencies: CheckoutDependencies,
    pub(super) control: ProofDataClaim,
}

impl Proof {
    pub(super) fn new(dependencies: CheckoutDependencies) -> Self {
        Self {
            dependencies,
            control: ProofDataClaim::default(),
        }
    }
    pub(super) fn clear(&mut self) {
        self.dependencies.clear();
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
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn new_with_native_custody(
        dependencies: CheckoutDependencies,
        data_custody: Arc<ProofDataCustody>,
        control: ProofDataClaim,
    ) -> Self {
        Self::sharing_with_custody(
            Proof {
                dependencies,
                control,
            },
            data_custody,
        )
    }
    pub(super) fn sharing_with_custody(proof: Proof, data_custody: Arc<ProofDataCustody>) -> Self {
        Self {
            proof: Arc::new(Mutex::new(proof)),
            observing: false,
            data_custody,
            accounting: parking_lot::Mutex::default(),
            base_root_control: ProofDataClaim::default(),
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
            data_custody,
            accounting: parking_lot::Mutex::default(),
            base_root_control: ProofDataClaim::default(),
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    #[cfg(any(test, feature = "native-watch"))]
    pub(super) fn independent_with_custody(&self, data_custody: Arc<ProofDataCustody>) -> Self {
        Self::sharing_with_custody(self.proof().clone(), data_custody)
    }
    pub(super) fn admissible_observation_groups(
        &self,
        dependencies: &[Dependency],
        starts: impl Fn(&Dependency) -> bool,
        maximum: u32,
    ) -> (usize, Option<DependencyError>) {
        self.proof()
            .dependencies
            .admissible_observation_groups(dependencies, starts, maximum)
    }

    pub(super) fn extend_observations(
        &self,
        dependencies: Vec<Dependency>,
        staged: &mut KernelOwnedAllocation,
        maximum: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let _ = &staged;
        let mut proof = self.proof();
        let extension = proof
            .dependencies
            .prepare_observations(dependencies, maximum)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let KernelOwnedAllocation::NativeOriginal(staged) = staged else {
                return Err(NativeTreeMaterializationError::InvalidLayout.into());
            };
            return proof.dependencies.commit_with_original_data_custody(
                extension,
                &self.data_custody,
                staged,
                original,
                cancellation,
            );
        }
        let (_, work, budget, _) = accounting
            .standalone_parts()
            .ok_or(FsError::UnsupportedCheckout)?;
        let mut ledger = self.accounting.lock();
        proof.dependencies.commit_with_data_custody(
            extension,
            Some(&self.data_custody),
            |bytes, operations, copied, items| {
                cancellation.check()?;
                let (retained, standing) = self.data_custody.live_payload_counts();
                let live = ledger.live_bytes();
                if retained > live {
                    ledger
                        .claim_bytes(
                            retained - live,
                            0,
                            &mut WorkCounters::default(),
                            WorkBudget::UNBOUNDED,
                        )
                        .map_err(|error| error.into_work_or(FsError::HandleAllocationFailed))?;
                } else {
                    ledger
                        .release(live - retained)
                        .map_err(|error| error.into_work_or(FsError::HandleAllocationFailed))?;
                }
                let mut next_ledger = *ledger;
                let mut next_work = *work;
                next_work.charge_items(items, &budget)?;
                next_work.charge(
                    &WorkCounters {
                        bytes_copied: copied,
                        ..WorkCounters::UNCHARGED
                    },
                    &budget,
                )?;
                let earlier_peak = next_work.peak_allocation_bytes;
                next_work.peak_allocation_bytes = 0;
                let external = self.data_custody.operation_cache_bytes();
                let peak = next_ledger
                    .live_bytes()
                    .checked_sub(standing)
                    .and_then(|live| live.checked_add(bytes))
                    .and_then(|live| live.checked_add(external))
                    .ok_or(WorkError::Overflow)?;
                if peak > budget.peak_allocation_bytes {
                    return Err(WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes",
                        observed: peak,
                        maximum: budget.peak_allocation_bytes,
                    }
                    .into());
                }
                let mut allowance = budget;
                allowance.peak_allocation_bytes = budget
                    .peak_allocation_bytes
                    .checked_sub(external)
                    .ok_or(WorkError::Overflow)?
                    .saturating_add(standing);
                next_ledger
                    .claim_bytes(bytes, operations, &mut next_work, allowance)
                    .map_err(|error| error.into_work_or(FsError::HandleAllocationFailed))?;
                next_work.peak_allocation_bytes = earlier_peak.max(peak);
                next_work.verify(budget)?;
                self.data_custody.record_data_work(WorkCounters {
                    items_examined: items,
                    bytes_copied: copied,
                    allocation_operations: operations,
                    peak_allocation_bytes: peak,
                    ..WorkCounters::default()
                })?;
                *ledger = next_ledger;
                *work = next_work;
                Ok(())
            },
        )
    }
}

#[cfg(test)]
impl<A, O> Checkout<A, O> {
    pub(crate) fn directory_observations(&self) -> CheckoutDependencies {
        self.dependencies.proof().dependencies.clone()
    }

    // Leaves room for `extra` more distinct dependencies in this checkout only.
    pub(crate) fn limit_dependencies_after(&mut self, extra: u32) {
        let held = u32::try_from(self.dependencies.proof().dependencies.len())
            .expect("bounded dependency count");
        self.volume.config.limits.maximum_checkout_dependencies = held + extra;
    }
}
