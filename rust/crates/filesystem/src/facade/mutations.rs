use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Checkout<A, O> {
    pub(super) async fn capture_mutation_dependencies_with_accounting(
        &self,
        operations: &[Mutation],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<Vec<Dependency>>, FsError> {
        let mut dependencies = Owned::<Vec<Dependency>>::allocate(0, accounting)?;
        let mut exact_regions =
            Owned::<Vec<(Option<FileRecord>, DependencyRegion)>>::allocate(0, accounting)?;
        for operation in operations {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            let mut records = [None; 2];
            if let Some(paths) = operation.paths() {
                for (index, path) in paths.into_iter().enumerate() {
                    if index == 1 && path == paths[0] {
                        records[1] = records[0];
                        continue;
                    }
                    let observed = crate::kernel::observe_path_async_with_accounting(
                        &self.volume.fs.inner.objects,
                        &self.base_root,
                        path,
                        self.volume.config,
                        accounting,
                        cancellation,
                        None,
                        false,
                    )
                    .await?;
                    *records.get_mut(index).ok_or(FsError::InvalidDiff)? = observed.0.lookup.record;
                    let bytes =
                        Self::mutation_dependency_bytes(&observed.0.dependencies, accounting)?;
                    Self::append_mutation_dependencies(
                        &mut dependencies,
                        Owned::new(observed.0.dependencies, observed.1, bytes),
                        accounting,
                    )?;
                }
            } else {
                let ids = match operation {
                    Mutation::File { file_id, .. } => [Some(*file_id), None],
                    Mutation::CloneFileRange {
                        source_file_id,
                        destination_file_id,
                        ..
                    } => [Some(*source_file_id), Some(*destination_file_id)],
                    _ => [None, None],
                };
                for (index, id) in ids.into_iter().enumerate() {
                    if index == 1 && id == ids[0] {
                        records[1] = records[0];
                    } else if let Some(id) = id {
                        *records.get_mut(index).ok_or(FsError::InvalidDiff)? =
                            lookup_file_record_async_with_accounting(
                                &self.volume.fs.inner.objects,
                                self.base_root.file_table,
                                id,
                                DecodeLimits::for_volume(self.volume.config),
                                accounting,
                                cancellation,
                                None,
                            )
                            .await?
                            .0;
                    }
                }
            }
            for region in
                super::mutation_regions::exact_mutation_regions(operation, records[0], records[1])
                    .into_iter()
                    .flatten()
            {
                exact_regions.ensure_for_push(usize::MAX, accounting)?;
                accounting.charge(WorkCounters {
                    bytes_copied: size_of::<(Option<FileRecord>, DependencyRegion)>() as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                exact_regions.value.push(region);
            }
        }
        Self::subsume_mutation_record_dependencies(&mut exact_regions.value, accounting)?;
        let (probe, probe_allocation) = AuthenticatedGenerationProbe::new_with_accounting(
            &self.volume.fs.inner.objects,
            probe_limits(self.volume.config),
            accounting,
        )?;
        let probe = (probe, probe_allocation);
        let region_bytes = exact_regions.bytes;
        let captured = probe
            .0
            .capture_records_many_with_accounting(
                exact_regions.value,
                self.volume.config.limits.maximum_checkout_dependencies,
                accounting,
                cancellation,
                None,
            )
            .await;
        crate::kernel::retire_allocation(exact_regions.allocation, region_bytes, accounting)?;
        {
            let (probe, _allocation) = probe;
            probe.retire_with_accounting(accounting)?;
        }
        let captured = captured?;
        let bytes = Self::mutation_dependency_bytes(&captured.0, accounting)?;
        Self::append_mutation_dependencies(
            &mut dependencies,
            Owned::new(captured.0, captured.1, bytes),
            accounting,
        )?;
        Ok(dependencies)
    }

    pub(super) fn mutation_region_name_bytes(region: &DependencyRegion) -> u64 {
        match region {
            DependencyRegion::DirectoryName { name, .. }
            | DependencyRegion::DirectoryRange {
                after: Some(name), ..
            } => name.retained_bytes() as u64,
            _ => 0,
        }
    }

    pub(super) fn mutation_dependency_bytes(
        values: &Vec<Dependency>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<u64, FsError> {
        let mut bytes = (values.capacity() as u64)
            .checked_mul(size_of::<Dependency>() as u64)
            .ok_or(WorkError::Overflow)?;
        for dependency in values {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            bytes = bytes
                .checked_add(Self::mutation_region_name_bytes(&dependency.region))
                .ok_or(WorkError::Overflow)?;
        }
        Ok(bytes)
    }

    pub(super) fn append_mutation_dependencies(
        destination: &mut Owned<Vec<Dependency>>,
        mut incoming: Owned<Vec<Dependency>>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        for dependency in incoming.value.drain(..) {
            destination.ensure_for_push(usize::MAX, accounting)?;
            let bytes = Self::mutation_region_name_bytes(&dependency.region);
            let allocation = crate::kernel::split_allocation(&mut incoming.allocation, bytes)?;
            let mut owned = (dependency, allocation);
            destination.allocation.absorb(&mut owned.1)?;
            incoming.bytes = incoming
                .bytes
                .checked_sub(bytes)
                .ok_or(WorkError::Overflow)?;
            destination.bytes = destination
                .bytes
                .checked_add(bytes)
                .ok_or(WorkError::Overflow)?;
            accounting.charge(WorkCounters {
                bytes_copied: size_of::<Dependency>() as u64,
                ..WorkCounters::UNCHARGED
            })?;
            destination.value.push(owned.0);
        }
        incoming.retire(accounting)
    }

    pub(super) fn subsume_mutation_record_dependencies(
        regions: &mut Vec<(Option<FileRecord>, DependencyRegion)>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let mut records = Owned::<Vec<FileId>>::allocate(regions.len(), accounting)?;
        for (_, region) in regions.iter() {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            if let DependencyRegion::FileRecord(file_id) = region {
                let position = crate::kernel::binary_search_with_accounting(
                    &records.value,
                    accounting,
                    |value| value.cmp(file_id),
                )?;
                if let Err(index) = position {
                    accounting.charge(WorkCounters {
                        bytes_copied: ((records.value.len() - index + 1) * size_of::<FileId>())
                            as u64,
                        ..WorkCounters::UNCHARGED
                    })?;
                    records.value.insert(index, *file_id);
                }
            }
        }
        let mut index = 0;
        while let Some((_, region)) = regions.get(index) {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            let file_id = match region {
                DependencyRegion::ContentRange { file_id, .. }
                | DependencyRegion::FileLength(file_id)
                | DependencyRegion::SparseSeek { file_id, .. } => Some(file_id),
                _ => None,
            };
            let removed = match file_id {
                Some(file_id) => crate::kernel::binary_search_with_accounting(
                    &records.value,
                    accounting,
                    |value| value.cmp(file_id),
                )?
                .is_ok(),
                None => false,
            };
            if removed {
                accounting.charge(WorkCounters {
                    bytes_copied: ((regions.len() - index - 1)
                        * size_of::<(Option<FileRecord>, DependencyRegion)>())
                        as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                regions.remove(index);
            } else {
                index += 1;
            }
        }
        records.retire(accounting)
    }

    /// Atomically applies one ordered mutation batch to this private checkout.
    pub async fn mutate(
        &mut self,
        operations: Vec<Mutation>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(FsError::Cancelled(CancellationError)))?;
        if self.mode.access != AccessMode::ReadWrite
            || !matches!(
                self.mode.mutations,
                MutationMode::PrivateOverlay | MutationMode::DirectLive
            )
        {
            return Err(OperationFailure::before_work(FsError::MutationNotAllowed));
        }
        let maximum = usize::try_from(self.volume.config.limits.maximum_mutations_per_batch)
            .unwrap_or(usize::MAX);
        if operations.capacity() > maximum {
            return Err(OperationFailure::before_work(
                FsError::ExcessiveMutationCapacity,
            ));
        }
        crate::heap_future::in_heap_measured(
            move |budget| async move {
                let mut accounting = KernelAccounting::standalone(budget, cancellation);
                let result = async {
                    accounting.check()?;
                    if self.live_operation_id.is_some() {
                        return Err(FsError::PendingLiveMutation);
                    }
                    if self.prepared_merge_parent.is_some() {
                        return Err(FsError::PreparedMergePending);
                    }
                    let bytes = Self::mutation_input_owned_bytes(&operations, &mut accounting)?;
                    let allocation = accounting.claim_owned(bytes, 0)?;
                    self.apply_mutations(
                        Owned::new(operations, allocation, bytes),
                        &mut accounting,
                        cancellation,
                    )
                    .await
                }
                .await;
                measured(result, &accounting)
            },
            budget,
            cancellation,
            FsError::Cancelled(CancellationError),
            None,
        )
        .await
    }

    pub(crate) async fn apply_mutations(
        &mut self,
        input: Owned<Vec<Mutation>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        match (accounting.native_work(), &input.allocation) {
            (Some(original), KernelOwnedAllocation::NativeOriginal(allocation)) => {
                if !allocation.work().shares_ledger(original) {
                    return Err(NativeTreeMaterializationError::InvalidLayout.into());
                }
                self.validate_native_work(original)?;
            }
            (None, KernelOwnedAllocation::Standalone) => {}
            _ => return Err(NativeTreeMaterializationError::InvalidLayout.into()),
        }
        if self.mode.access != AccessMode::ReadWrite
            || !matches!(
                self.mode.mutations,
                MutationMode::PrivateOverlay | MutationMode::DirectLive
            )
        {
            return Err(FsError::MutationNotAllowed);
        }
        if self.live_operation_id.is_some() {
            return Err(FsError::PendingLiveMutation);
        }
        if self.prepared_merge_parent.is_some() {
            return Err(FsError::PreparedMergePending);
        }
        let Owned {
            value: mut operations,
            allocation: mut operations_allocation,
            ..
        } = input;
        let maximum = usize::try_from(self.volume.config.limits.maximum_mutations_per_batch)
            .unwrap_or(usize::MAX);
        if operations.capacity() > maximum {
            return Err(FsError::ExcessiveMutationCapacity);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let KernelOwnedAllocation::NativeOriginal(guard) = &operations_allocation {
            let bytes = Self::mutation_input_owned_bytes(&operations, accounting)?;
            if guard.bytes() != bytes {
                return Err(NativeTreeMaterializationError::InvalidLayout.into());
            }
        }
        self.admit_mutation_names_with_accounting(
            &mut operations,
            &mut operations_allocation,
            accounting,
            cancellation,
        )
        .await?;
        let captured = self
            .capture_mutation_dependencies_with_accounting(&operations, accounting, cancellation)
            .await?;
        let extension = self.dependencies.proof().prepare_mutations(
            captured.value,
            self.volume.config.limits.maximum_checkout_dependencies,
        )?;
        let staged_names = extension.retained_name_bytes_with_step(|| {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })
        })?;
        let mut staged = (extension, captured.allocation);
        accounting.release_owned(
            &mut staged.1,
            captured
                .bytes
                .checked_sub(staged_names)
                .ok_or(WorkError::Overflow)?,
        )?;
        let final_input_bytes = Self::mutation_input_owned_bytes(&operations, accounting)?;
        let produced = crate::kernel::apply_generation_mutations_retaining_with_accounting(
            &self.volume.fs.inner.objects,
            &self.root,
            operations,
            self.volume.config,
            accounting,
            cancellation,
        )
        .await;
        let (receipt, returned_operations, allocation) = produced?;
        let mut working = (receipt.root, allocation);
        drop(returned_operations);
        accounting.release_owned(&mut operations_allocation, final_input_bytes)?;
        if working.0.file_table == self.root.file_table {
            let bytes = root_parent_bytes(&working.0)?;
            drop(working.0);
            crate::kernel::retire_allocation(working.1, bytes, accounting)?;
            drop(staged.0);
            accounting.release_owned(&mut staged.1, staged_names)?;
            return Ok(());
        }
        accounting.charge(WorkCounters {
            bytes_copied: size_of::<CheckoutDependencies>() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let mut prepared_dependencies = self.dependencies.proof().dependencies.clone();
        let producer = &self.dependencies.data_custody;
        let held = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => self
                .root
                .hold
                .as_ref()
                .map(|_| {
                    crate::collection::clone_generation_root(&working.0, accounting, Some(producer))
                })
                .transpose()?,
            _ => {
                let (allocations, work, budget, token) = accounting
                    .standalone_parts()
                    .ok_or(FsError::PendingMutationAllocationFailed)?;
                token.check()?;
                self.root
                    .hold
                    .as_ref()
                    .map(|_| {
                        crate::collection::clone_generation_root(
                            &working.0,
                            &mut KernelAccounting::borrowed_standalone(
                                allocations,
                                work,
                                budget,
                                token,
                            ),
                            None,
                        )
                    })
                    .transpose()?
                    .map(|(root, _)| {
                        let control = Self::root_control_claim(&root, Some(producer))?;
                        Ok::<_, FsError>((root, control))
                    })
                    .transpose()?
            }
        };
        if working.0.file_table == self.base_root.file_table {
            prepared_dependencies.clear_mutations();
            drop(staged.0);
            accounting.release_owned(&mut staged.1, staged_names)?;
        } else {
            match accounting {
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { work: original, .. } => {
                    let KernelOwnedAllocation::NativeOriginal(allocation) = &mut staged.1 else {
                        unreachable!("original captured names retain original custody");
                    };
                    prepared_dependencies.commit_with_original_data_custody(
                        staged.0,
                        producer,
                        allocation,
                        original,
                        cancellation,
                    )?;
                    // Duplicate keys have already physically died in commit.
                    allocation.release(allocation.bytes())?;
                }
                _ => prepared_dependencies.commit_with_data_custody(
                    staged.0,
                    Some(producer),
                    |bytes, operations, copied, items| {
                        let delta = WorkCounters {
                            bytes_copied: copied,
                            items_examined: items,
                            ..WorkCounters::UNCHARGED
                        };
                        accounting.admit(delta)?;
                        if operations != 0 || copied != 0 {
                            accounting.claim_owned(bytes, operations)?;
                        }
                        accounting.charge(delta)
                    },
                )?,
            }
        }
        let control = match &mut working.1 {
            #[cfg(not(target_arch = "wasm32"))]
            KernelOwnedAllocation::NativeOriginal(allocation) => {
                cancellation.check()?;
                let scope = allocation.work().checkout_control_sink(producer)?;
                allocation.transfer_checkout_control(producer, &scope)?
            }
            KernelOwnedAllocation::Standalone => {
                Self::root_control_claim(&working.0, Some(producer))?
            }
        };
        // Refuse invalid root custody before publishing the prepared proof.
        self.root.set(self.root.base, (working.0, control), held)?;
        self.dependencies.proof().dependencies = prepared_dependencies;
        Ok(())
    }

    pub(super) fn mutation_input_owned_bytes(
        operations: &Vec<Mutation>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<u64, FsError> {
        let mut bytes = (operations.capacity() as u64)
            .checked_mul(size_of::<Mutation>() as u64)
            .ok_or(WorkError::Overflow)?;
        for operation in operations {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            bytes = bytes
                .checked_add(mutation_path_owned_bytes(operation))
                .ok_or(WorkError::Overflow)?;
        }
        Ok(bytes)
    }

    pub(super) async fn observe_identity_regions_with_accounting(
        &mut self,
        regions: [Option<DependencyRegion>; 2],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let count = regions.iter().flatten().count();
            let mut requested = Owned::<Vec<DependencyRegion>>::allocate(count, accounting)?;
            for region in regions.into_iter().flatten() {
                let copied = WorkCounters {
                    bytes_copied: size_of::<DependencyRegion>() as u64,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                requested.value.push(region);
                accounting.record_completed(copied)?;
            }
            let (probe, allocation) = AuthenticatedGenerationProbe::new_with_accounting(
                &self.volume.fs.inner.objects,
                probe_limits(self.volume.config),
                accounting,
            )?;
            let probe = (
                probe.with_data_custody(Some(&self.dependencies.data_custody)),
                allocation,
            );
            let input = std::mem::take(&mut requested.value);
            let captured = probe
                .0
                .capture_many_with_accounting(
                    GenerationId::new(self.root.base.digest),
                    input,
                    self.volume.config.limits.maximum_checkout_dependencies,
                    accounting,
                    cancellation,
                )
                .await;
            // The canonical capture consumes the input backing before returning.
            requested.retire(accounting)?;
            let (value, allocation) = captured?;
            let bytes = value
                .capacity()
                .checked_mul(size_of::<Dependency>())
                .map(crate::foundation::usize_to_u64)
                .ok_or(WorkError::Overflow)?;
            let captured = Owned::new(value, allocation, bytes);
            {
                let (probe, _allocation) = probe;
                probe.retire_with_accounting(accounting)?;
            }
            self.extend_observations_with_accounting(captured, accounting, cancellation)
        }
        .await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }

    pub(super) async fn observe_base_regular_range_with_accounting(
        &mut self,
        file_id: FileId,
        range: ByteRange,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let record = {
                let (record, _allocation) =
                    crate::kernel::lookup_file_record_async_with_accounting(
                        &self.volume.fs.inner.objects,
                        self.base_root.file_table,
                        file_id,
                        DecodeLimits::for_volume(self.volume.config),
                        accounting,
                        cancellation,
                        Some(&self.dependencies.data_custody),
                    )
                    .await?;
                record
            };
            self.observe_identity_regions_with_accounting(
                regular_range_regions(record, range.offset, range.length),
                accounting,
                cancellation,
            )
            .await
        }
        .await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }

    pub(super) async fn observe_base_metadata_ids_with_accounting(
        &mut self,
        file_ids: &[FileId],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            accounting.check()?;
            let selected = {
                let proof = self.dependencies.proof();
                let mut count = 0_usize;
                for file_id in file_ids {
                    let examined = WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    };
                    accounting.admit(examined)?;
                    count += usize::from(!proof.observes(&DependencyRegion::Metadata(*file_id)));
                    accounting.record_completed(examined)?;
                }
                let mut selected = Owned::<Vec<FileId>>::allocate(count, accounting)?;
                for file_id in file_ids {
                    let examined = WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    };
                    accounting.admit(examined)?;
                    let observed = proof.observes(&DependencyRegion::Metadata(*file_id));
                    accounting.record_completed(examined)?;
                    if !observed {
                        let copied = WorkCounters {
                            bytes_copied: size_of::<FileId>() as u64,
                            ..WorkCounters::UNCHARGED
                        };
                        accounting.admit(copied)?;
                        selected.value.push(*file_id);
                        accounting.record_completed(copied)?;
                    }
                }
                selected
            };
            if selected.value.is_empty() {
                return selected.retire(accounting);
            }
            let maximum = u32::try_from(selected.value.len()).unwrap_or(u32::MAX);
            let (records, allocation) = crate::kernel::lookup_file_records_async_with_accounting(
                &self.volume.fs.inner.objects,
                self.base_root.file_table,
                &selected.value,
                maximum,
                DecodeLimits::for_volume(self.volume.config),
                accounting,
                cancellation,
                Some(&self.dependencies.data_custody),
            )
            .await?;
            let records = (records, allocation);
            let mut dependencies =
                Owned::<Vec<Dependency>>::allocate(selected.value.len(), accounting)?;
            for (file_id, record) in selected.value.iter().zip(&records.0) {
                let copied = WorkCounters {
                    items_examined: 1,
                    bytes_copied: size_of::<Dependency>() as u64,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                dependencies.value.push(Dependency {
                    region: DependencyRegion::Metadata(*file_id),
                    expected: record.map_or(DependencyState::Absent, |record| {
                        DependencyState::Present(record.metadata.digest)
                    }),
                });
                accounting.record_completed(copied)?;
            }
            drop(records);
            selected.retire(accounting)?;
            self.extend_observations_with_accounting(dependencies, accounting, cancellation)
        }
        .await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }
    pub(super) fn extend_observations_with_accounting(
        &mut self,
        mut dependencies: Owned<Vec<Dependency>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        self.dependencies.extend_observations(
            std::mem::take(&mut dependencies.value),
            &mut dependencies.allocation,
            self.volume.config.limits.maximum_checkout_dependencies,
            accounting,
            cancellation,
        )
    }
}
