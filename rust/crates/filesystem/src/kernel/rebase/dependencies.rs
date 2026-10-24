use super::*;
use std::borrow::Cow;

impl CheckoutDependencies {
    pub(crate) const fn empty_logical_owned_bytes() -> usize {
        std::mem::size_of::<CapturedMap>()
    }

    pub(crate) fn attach_fresh_control_custody(&mut self, owner: &Arc<ProofDataCustody>) -> bool {
        self.attach_fresh_control_with(owner, ProofDataClaim::control)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn attach_fresh_paid_control(&mut self, control: &mut ProofDataClaim) -> bool {
        let Some(captured) = Arc::get_mut(&mut self.captured) else {
            return false;
        };
        if !captured.entries.is_empty()
            || captured._control.bytes != 0
            || self.data_generation != 0
            || !control.physical
            || control.bytes != Self::empty_logical_owned_bytes() as u64
        {
            return false;
        }
        captured._control = std::mem::take(control);
        true
    }

    fn attach_fresh_control_with(
        &mut self,
        owner: &Arc<ProofDataCustody>,
        claim: fn(&Arc<ProofDataCustody>, u64) -> ProofDataClaim,
    ) -> bool {
        let Some(captured) = Arc::get_mut(&mut self.captured) else {
            return false;
        };
        if !captured.entries.is_empty() || captured._control.bytes != 0 || self.data_generation != 0
        {
            return false;
        }
        captured._control = claim(owner, Self::empty_logical_owned_bytes() as u64);
        true
    }

    /// Validates and normalizes exact observations and mutation preconditions.
    pub fn new(
        observations: impl IntoIterator<Item = Dependency>,
        mutations: impl IntoIterator<Item = Dependency>,
        maximum_dependencies: u32,
    ) -> Result<Self, DependencyError> {
        if maximum_dependencies == 0 {
            return Err(DependencyError::ZeroLimit);
        }
        let mut captured: BTreeMap<DependencyRegion, CapturedState> = BTreeMap::new();
        for (dependency, usage) in observations
            .into_iter()
            .map(|value| (value, DependencyUse::Observation))
            .chain(
                mutations
                    .into_iter()
                    .map(|value| (value, DependencyUse::Mutation)),
            )
        {
            dependency.region.validate()?;
            if let Some(existing) = captured.get_mut(&dependency.region) {
                if existing.expected != dependency.expected {
                    return Err(DependencyError::ContradictoryState);
                }
                if existing.usage != usage {
                    existing.usage = DependencyUse::ObservationAndMutation;
                }
                continue;
            }
            if u32::try_from(captured.len()).unwrap_or(u32::MAX) >= maximum_dependencies {
                return Err(DependencyError::TooManyDependencies {
                    maximum: maximum_dependencies,
                });
            }
            captured.insert(
                dependency.region,
                CapturedState {
                    expected: dependency.expected,
                    usage,
                    data: ProofDataClaim::default(),
                },
            );
        }
        Ok(Self {
            name_data_bytes: captured
                .keys()
                .map(|region| dependency_name_bytes(region, false))
                .sum(),
            cloned_name_data_bytes: captured
                .keys()
                .map(|region| dependency_name_bytes(region, true))
                .sum(),
            data_generation: 0,
            captured: Arc::new(CapturedMap {
                entries: captured,
                _control: ProofDataClaim::default(),
            }),
        })
    }

    /// Number of distinct exact regions in the proof.
    #[must_use]
    pub fn len(&self) -> usize {
        self.captured.entries.len()
    }

    /// Whether no operation has observed or mutated a region.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.captured.entries.is_empty()
    }

    #[allow(
        clippy::expect_used,
        reason = "Exhausting the generation counter must fail rather than reuse an ownership identity."
    )]
    pub(crate) fn clear(&mut self) {
        self.captured = Arc::default();
        self.name_data_bytes = 0;
        self.cloned_name_data_bytes = 0;
        self.data_generation = self
            .data_generation
            .checked_add(1)
            .expect("proof allocation generation exhausted");
    }

    pub(crate) fn observes(&self, region: &DependencyRegion) -> bool {
        self.captured.entries.get(region).is_some_and(|state| {
            matches!(
                state.usage,
                DependencyUse::Observation | DependencyUse::ObservationAndMutation
            )
        })
    }

    #[allow(
        clippy::expect_used,
        reason = "Exhausting the generation counter must fail rather than reuse an ownership identity."
    )]
    pub(crate) fn clear_mutations(&mut self) {
        let mut retained_bytes = 0_u64;
        let mut cloned_bytes = 0_u64;
        if Arc::strong_count(&self.captured) > 1 || Arc::weak_count(&self.captured) != 0 {
            self.data_generation = self
                .data_generation
                .checked_add(1)
                .expect("proof allocation generation exhausted");
        }
        Arc::make_mut(&mut self.captured)
            .entries
            .retain(|region, state| {
                let keep = match state.usage {
                    DependencyUse::Observation => true,
                    DependencyUse::Mutation => false,
                    DependencyUse::ObservationAndMutation => {
                        state.usage = DependencyUse::Observation;
                        true
                    }
                };
                if keep {
                    retained_bytes += dependency_name_bytes(region, false);
                    cloned_bytes += dependency_name_bytes(region, true);
                }
                keep
            });
        self.name_data_bytes = retained_bytes;
        self.cloned_name_data_bytes = cloned_bytes;
    }

    pub(crate) fn prepare_observations(
        &self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
    ) -> Result<DependencyExtension, DependencyError> {
        self.prepare(
            dependencies,
            maximum_dependencies,
            DependencyUse::Observation,
        )
    }

    pub(crate) fn prepare_mutations(
        &self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
    ) -> Result<DependencyExtension, DependencyError> {
        self.prepare(dependencies, maximum_dependencies, DependencyUse::Mutation)
    }

    pub(crate) fn commit_with_data_custody<E: From<WorkError>>(
        &mut self,
        extension: DependencyExtension,
        owner: Option<&Arc<ProofDataCustody>>,
        admit: impl FnMut(u64, u64, u64, u64) -> Result<(), E>,
    ) -> Result<(), E> {
        self.commit_with_data_claims(
            extension,
            admit,
            |bytes| {
                Ok(owner
                    .map(|owner| ProofDataClaim::pending_control(owner, bytes))
                    .unwrap_or_default())
            },
            |bytes, _| Ok(ProofDataClaim::new(owner, bytes)),
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn commit_with_original_data_custody(
        &mut self,
        extension: DependencyExtension,
        owner: &Arc<ProofDataCustody>,
        staged: &mut crate::NativeAllocation,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), crate::FsError> {
        if !staged.work().shares_ledger(original) {
            return Err(crate::FsError::NativeKernel(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        cancellation.check()?;
        let witness = original.checkout_control_sink(owner)?;
        // The guard owns only this invocation's new COW buffers. Physical
        // map cloning and replacement happen outside every original-root loan.
        let copied_allocation = std::cell::RefCell::new(None::<crate::NativeAllocation>);
        self.commit_with_data_claims(
            extension,
            |bytes, operations, copied, items| {
                cancellation.check()?;
                let delta = WorkCounters {
                    bytes_copied: copied,
                    items_examined: items,
                    ..WorkCounters::UNCHARGED
                };
                if operations != 0 || copied != 0 {
                    let allocation = original.claim(bytes, operations, delta)?;
                    let previous = copied_allocation.replace(Some(allocation));
                    debug_assert!(previous.as_ref().is_none_or(|guard| guard.bytes() == 0));
                    drop(previous);
                } else {
                    original.charge(delta)?;
                }
                Ok(())
            },
            |bytes| {
                let mut allocation = copied_allocation.borrow_mut();
                let allocation = allocation.as_mut().ok_or(crate::FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ))?;
                let mut control = allocation.split_off(bytes)?;
                control
                    .transfer_checkout_control(owner, &witness)
                    .map_err(Into::into)
            },
            |bytes, copied| {
                if copied {
                    let mut allocation = copied_allocation.borrow_mut();
                    let allocation = allocation.as_mut().ok_or(crate::FsError::NativeKernel(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ))?;
                    allocation
                        .transfer_checkout_data_bytes(bytes, owner, &witness)
                        .map_err(Into::into)
                } else {
                    staged
                        .transfer_checkout_data_bytes(bytes, owner, &witness)
                        .map_err(Into::into)
                }
            },
        )
    }

    fn commit_with_data_claims<E: From<WorkError>>(
        &mut self,
        extension: DependencyExtension,
        mut admit: impl FnMut(u64, u64, u64, u64) -> Result<(), E>,
        mut control: impl FnMut(u64) -> Result<ProofDataClaim, E>,
        mut data: impl FnMut(u64, bool) -> Result<ProofDataClaim, E>,
    ) -> Result<(), E> {
        let mut changes = false;
        for region in extension.staged.keys() {
            admit(0, 0, 0, 1)?;
            if self
                .captured
                .entries
                .get(region)
                .is_none_or(|state| state.usage.with(extension.usage) != state.usage)
            {
                changes = true;
            }
        }
        if !changes {
            return Ok(());
        }
        let copied = Arc::strong_count(&self.captured) > 1;
        let moved = !copied && Arc::weak_count(&self.captured) != 0;
        let generation = if copied || moved {
            self.data_generation
                .checked_add(1)
                .ok_or(WorkError::Overflow)?
        } else {
            self.data_generation
        };
        if copied {
            let mut operations = 0_u64;
            for region in self.captured.entries.keys() {
                admit(0, 0, 0, 1)?;
                operations = operations
                    .checked_add(u64::from(dependency_name_bytes(region, true) != 0))
                    .ok_or(WorkError::Overflow)?;
            }
            // Admit the actual new Arc payload with its actual name buffers
            // atomically: denial must not report a copy that never occurred.
            // Opaque BTree nodes and allocator/Arc headers remain excluded.
            let control_bytes = Self::empty_logical_owned_bytes() as u64;
            let bytes = self
                .cloned_name_data_bytes
                .checked_add(control_bytes)
                .ok_or(WorkError::Overflow)?;
            let operations = operations.checked_add(1).ok_or(WorkError::Overflow)?;
            admit(bytes, operations, self.cloned_name_data_bytes, 0)?;
            let captured = Arc::make_mut(&mut self.captured);
            captured._control = control(control_bytes)?;
            for (region, value) in captured.entries.iter_mut() {
                value.data = data(dependency_name_bytes(region, false), true)?;
            }
            self.name_data_bytes = self.cloned_name_data_bytes;
        } else if moved {
            let control_bytes = Self::empty_logical_owned_bytes() as u64;
            admit(control_bytes, 1, 0, 0)?;
            let claim = control(control_bytes)?;
            // Arc moves the keys without cloning them, but constructs a new
            // known payload. Only its control claim is replaced.
            Arc::make_mut(&mut self.captured)._control = claim;
        }
        self.data_generation = generation;
        let DependencyExtension { staged, usage } = extension;
        let captured = Arc::make_mut(&mut self.captured);
        for (region, expected) in staged {
            admit(0, 0, 0, 1)?;
            if let Some(existing) = captured.entries.get_mut(&region) {
                existing.usage = existing.usage.with(usage);
            } else {
                // The staged key owns its name buffer already; insertion
                // moves that buffer rather than allocating or copying it.
                let retained = dependency_name_bytes(&region, false);
                let name_data_bytes = self
                    .name_data_bytes
                    .checked_add(retained)
                    .ok_or(WorkError::Overflow)?;
                let cloned_name_data_bytes = self
                    .cloned_name_data_bytes
                    .checked_add(dependency_name_bytes(&region, true))
                    .ok_or(WorkError::Overflow)?;
                admit(retained, 0, 0, 0)?;
                let data = data(retained, false)?;
                captured.entries.insert(
                    region,
                    CapturedState {
                        expected,
                        usage,
                        data,
                    },
                );
                self.name_data_bytes = name_data_bytes;
                self.cloned_name_data_bytes = cloned_name_data_bytes;
            }
        }
        Ok(())
    }

    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn merged_with_accounting(
        &self,
        other: &Self,
        owner: Option<&Arc<ProofDataCustody>>,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<Option<Self>, crate::FsError> {
        if Arc::ptr_eq(&self.captured, &other.captured) {
            return Ok(Some(self.clone()));
        }
        let entries = (self.captured.entries.iter())
            .chain(other.captured.entries.iter())
            .map(|(region, state)| (Cow::Borrowed(region), state.expected, state.usage));
        self.rebuilt_with_accounting(entries, owner, accounting)
    }

    /// The ledger a rebase leaves once the candidate merge lands: each
    /// region a replacement settled ([`Settlement`]) expects the candidate's
    /// state, under its replacement's identity when it follows one. `settled`
    /// is sorted by region. The copy is built and charged here, before the
    /// rebased root is installed; `None` means two settled regions now
    /// expect different states of one region.
    pub(crate) fn followed_with_accounting(
        &self,
        settled: &[(&DependencyRegion, Settlement)],
        owner: Option<&Arc<ProofDataCustody>>,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<Option<Self>, crate::FsError> {
        let entries = self.captured.entries.iter().map(|(region, state)| {
            let settlement = settled
                .binary_search_by(|(settled, _)| (*settled).cmp(region))
                .ok()
                .and_then(|index| settled.get(index))
                .map(|(_, settlement)| *settlement);
            match settlement {
                None => (Cow::Borrowed(region), state.expected, state.usage),
                Some(Settlement::Expect(expected)) => {
                    (Cow::Borrowed(region), expected, state.usage)
                }
                Some(Settlement::Follow { file, expected }) => {
                    (Cow::Owned(region.of_file(file)), expected, state.usage)
                }
            }
        });
        self.rebuilt_with_accounting(entries, owner, accounting)
    }

    /// A new ledger of `entries`, each region cloned and charged; a region
    /// listed twice joins its uses, and `None` means it expects two states.
    fn rebuilt_with_accounting<'r>(
        &self,
        entries: impl Iterator<Item = (Cow<'r, DependencyRegion>, DependencyState, DependencyUse)>,
        owner: Option<&Arc<ProofDataCustody>>,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<Option<Self>, crate::FsError> {
        use super::allocation::KernelOwnedAllocation;
        #[cfg(not(target_arch = "wasm32"))]
        let witness = accounting
            .native_work()
            .map(|original| {
                original
                    .checkout_control_sink(owner.ok_or(crate::FsError::NativeKernel(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ))?)
                    .map_err(crate::FsError::from)
            })
            .transpose()?;
        let accounting = std::cell::RefCell::new(accounting);
        // Declared outside the physical merge so a rejected constructor drops
        // its actual payload before this untransferred ticket is released.
        let pending = std::cell::RefCell::new(None::<KernelOwnedAllocation>);
        self.rebuilt_with_data_claims(
            entries,
            |bytes, operations, copied, items| {
                let mut accounting = accounting.borrow_mut();
                accounting.charge(WorkCounters {
                    items_examined: items,
                    ..WorkCounters::UNCHARGED
                })?;
                if bytes != 0 || operations != 0 {
                    accounting.admit(WorkCounters {
                        bytes_copied: copied,
                        ..WorkCounters::UNCHARGED
                    })?;
                    let allocation = accounting.claim_owned(bytes, operations)?;
                    let previous = pending.replace(Some(allocation));
                    #[cfg(not(target_arch = "wasm32"))]
                    debug_assert!(previous.as_ref().is_none_or(|guard| match guard {
                        KernelOwnedAllocation::Standalone => true,
                        KernelOwnedAllocation::NativeOriginal(guard) => guard.bytes() == 0,
                    }));
                    drop(previous);
                }
                Ok(())
            },
            |copied| {
                accounting.borrow_mut().record_completed(WorkCounters {
                    bytes_copied: copied,
                    ..WorkCounters::UNCHARGED
                })
            },
            |bytes| {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(witness) = witness.as_ref() {
                    let mut pending = pending.borrow_mut();
                    let Some(KernelOwnedAllocation::NativeOriginal(allocation)) = pending.as_mut()
                    else {
                        return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
                    };
                    return allocation
                        .transfer_checkout_control(
                            owner.ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?,
                            witness,
                        )
                        .map_err(Into::into);
                }
                Ok(owner
                    .map(|owner| ProofDataClaim::pending_control(owner, bytes))
                    .unwrap_or_default())
            },
            |bytes| {
                if bytes == 0 {
                    return Ok(ProofDataClaim::default());
                }
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(witness) = witness.as_ref() {
                    let mut pending = pending.borrow_mut();
                    let Some(KernelOwnedAllocation::NativeOriginal(allocation)) = pending.as_mut()
                    else {
                        return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
                    };
                    return allocation
                        .transfer_checkout_data_bytes(
                            bytes,
                            owner.ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?,
                            witness,
                        )
                        .map_err(Into::into);
                }
                Ok(ProofDataClaim::new(owner, bytes))
            },
        )
    }

    #[allow(
        clippy::expect_used,
        reason = "The newly constructed Arc has not been shared before this mutable access."
    )]
    fn rebuilt_with_data_claims<'r, E: From<WorkError>>(
        &self,
        entries: impl Iterator<Item = (Cow<'r, DependencyRegion>, DependencyState, DependencyUse)>,
        mut admit: impl FnMut(u64, u64, u64, u64) -> Result<(), E>,
        mut completed: impl FnMut(u64) -> Result<(), E>,
        mut control: impl FnMut(u64) -> Result<ProofDataClaim, E>,
        mut data: impl FnMut(u64) -> Result<ProofDataClaim, E>,
    ) -> Result<Option<Self>, E> {
        let generation = self
            .data_generation
            .checked_add(1)
            .ok_or(WorkError::Overflow)?;
        let mut captured: BTreeMap<DependencyRegion, CapturedState> = BTreeMap::new();
        let mut name_data_bytes = 0_u64;
        for (region, expected, usage) in entries {
            admit(0, 0, 0, 1)?;
            if let Some(existing) = captured.get_mut(region.as_ref()) {
                if existing.expected != expected {
                    return Ok(None);
                }
                existing.usage = existing.usage.with(usage);
                continue;
            }
            let bytes = dependency_name_bytes(&region, true);
            let retained = name_data_bytes
                .checked_add(bytes)
                .ok_or(WorkError::Overflow)?;
            admit(bytes, u64::from(bytes != 0), bytes, 0)?;
            let region = region.into_owned();
            completed(bytes)?;
            let data = data(dependency_name_bytes(&region, false))?;
            captured.insert(
                region,
                CapturedState {
                    expected,
                    usage,
                    data,
                },
            );
            name_data_bytes = retained;
        }
        // The new Arc owns this exact map control payload, not a guessed
        // per-entry/node allocation. Stack-resident merge control is excluded.
        admit(Self::empty_logical_owned_bytes() as u64, 1, 0, 0)?;
        let mut captured = Arc::new(CapturedMap {
            entries: captured,
            _control: ProofDataClaim::default(),
        });
        completed(0)?;
        Arc::get_mut(&mut captured)
            .expect("the new merged map has no aliases")
            ._control = control(Self::empty_logical_owned_bytes() as u64)?;
        Ok(Some(Self {
            captured,
            name_data_bytes,
            cloned_name_data_bytes: name_data_bytes,
            data_generation: generation,
        }))
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn same_capture(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.captured, &other.captured)
            && self.data_generation == other.data_generation
    }

    fn prepare(
        &self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
        usage: DependencyUse,
    ) -> Result<DependencyExtension, DependencyError> {
        if maximum_dependencies == 0 {
            return Err(DependencyError::ZeroLimit);
        }
        let mut staged = BTreeMap::new();
        for dependency in dependencies {
            dependency.region.validate()?;
            if let Some(existing) = self.captured.entries.get(&dependency.region)
                && existing.expected != dependency.expected
            {
                return Err(DependencyError::ContradictoryState);
            }
            match staged.get(&dependency.region) {
                Some(expected) if *expected != dependency.expected => {
                    return Err(DependencyError::ContradictoryState);
                }
                Some(_) => {}
                None => {
                    staged.insert(dependency.region, dependency.expected);
                }
            }
        }
        let distinct_new = staged
            .keys()
            .filter(|region| !self.captured.entries.contains_key(*region))
            .count();
        if self.captured.entries.len().saturating_add(distinct_new)
            > usize::try_from(maximum_dependencies).unwrap_or(usize::MAX)
        {
            return Err(DependencyError::TooManyDependencies {
                maximum: maximum_dependencies,
            });
        }
        Ok(DependencyExtension { staged, usage })
    }
}

#[must_use]
pub(crate) struct DependencyExtension {
    staged: BTreeMap<DependencyRegion, DependencyState>,
    usage: DependencyUse,
}

impl DependencyExtension {
    pub(crate) fn retained_name_bytes_with_step<E: From<WorkError>>(
        &self,
        mut step: impl FnMut() -> Result<(), E>,
    ) -> Result<u64, E> {
        let mut bytes = 0_u64;
        for region in self.staged.keys() {
            step()?;
            let retained = match region {
                DependencyRegion::DirectoryName { name, .. }
                | DependencyRegion::DirectoryRange {
                    after: Some(name), ..
                } => name.retained_bytes() as u64,
                _ => 0,
            };
            bytes = bytes.checked_add(retained).ok_or(WorkError::Overflow)?;
        }
        Ok(bytes)
    }
}
