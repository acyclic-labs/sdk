use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {

    pub(super) async fn capture_mutation_dependencies_with_accounting(
        &self, operations: &[Mutation], accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<Vec<Dependency>>, FsError> {
        use crate::kernel::Owned;
        let mut dependencies = Owned::<Vec<Dependency>>::allocate(0, accounting)?;
        let mut exact_regions = Owned::<Vec<(Option<FileRecord>, DependencyRegion)>>::allocate(
            0, accounting,
        )?;
        for operation in operations {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            let (first_record, second_record) = if let Some([first_path, second_path]) = operation.paths() {
                let first = crate::kernel::observe_path_async_with_accounting(
                    &self.volume.fs.inner.objects, &self.base_root, first_path,
                    self.volume.config, accounting, cancellation, None, false,
                ).await?;
                let first = first.into_parts();
                let first_record = first.0.lookup.record;
                let bytes = Self::mutation_dependency_bytes(&first.0.dependencies, accounting)?;
                Self::append_mutation_dependencies(
                    &mut dependencies, Owned::new(first.0.dependencies, first.1, bytes),
                    accounting,
                )?;
                let second_record = if second_path == first_path {
                    first_record
                } else {
                    let second = crate::kernel::observe_path_async_with_accounting(
                        &self.volume.fs.inner.objects, &self.base_root, second_path,
                        self.volume.config, accounting, cancellation, None, false,
                    ).await?;
                    let second = second.into_parts();
                    let second_record = second.0.lookup.record;
                    let bytes = Self::mutation_dependency_bytes(&second.0.dependencies, accounting)?;
                    Self::append_mutation_dependencies(
                        &mut dependencies, Owned::new(second.0.dependencies, second.1, bytes),
                        accounting,
                    )?;
                    second_record
                };
                (first_record, second_record)
            } else {
                let (first_id, second_id) = match operation {
                    Mutation::File { file_id, .. } => (Some(*file_id), None),
                    Mutation::CloneFileRange { source_file_id, destination_file_id, .. } =>
                        (Some(*source_file_id), Some(*destination_file_id)),
                    _ => (None, None),
                };
                let first = if let Some(file_id) = first_id {
                    let record = crate::kernel::lookup_file_record_async_with_accounting(
                        &self.volume.fs.inner.objects, self.base_root.file_table, file_id,
                        DecodeLimits::for_volume(self.volume.config), accounting, cancellation, None,
                    ).await?;
                    record.0
                } else { None };
                let second = if second_id == first_id {
                    first
                } else if let Some(file_id) = second_id {
                    let record = crate::kernel::lookup_file_record_async_with_accounting(
                        &self.volume.fs.inner.objects, self.base_root.file_table, file_id,
                        DecodeLimits::for_volume(self.volume.config), accounting, cancellation, None,
                    ).await?;
                    record.0
                } else { None };
                (first, second)
            };
            for region in exact_mutation_regions(operation, first_record, second_record)
                .into_iter().flatten()
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
            &self.volume.fs.inner.objects, probe_limits(self.volume.config), accounting,
        )?;
        let probe = (probe, probe_allocation);
        let region_bytes = exact_regions.bytes;
        let captured = probe.0.capture_records_many_with_accounting(
            exact_regions.value, self.volume.config.limits.maximum_checkout_dependencies,
            accounting, cancellation, None,
        ).await;
        crate::kernel::retire_allocation(
            exact_regions.allocation, region_bytes, accounting,
        )?;
        probe.0.retire_with_accounting(accounting)?;
        drop(probe.1);
        let captured = captured?;
        let bytes = Self::mutation_dependency_bytes(&captured.0, accounting)?;
        Self::append_mutation_dependencies(
            &mut dependencies, Owned::new(captured.0, captured.1, bytes), accounting,
        )?;
        Ok(dependencies)
    }

    pub(super) fn mutation_region_name_bytes(region: &DependencyRegion) -> u64 {
        match region {
            DependencyRegion::DirectoryName { name, .. }
            | DependencyRegion::DirectoryRange { after: Some(name), .. } => name.retained_bytes() as u64,
            _ => 0,
        }
    }

    pub(super) fn mutation_dependency_bytes(
        values: &Vec<Dependency>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<u64, FsError> {
        let mut bytes = (values.capacity() as u64).checked_mul(size_of::<Dependency>() as u64)
            .ok_or(WorkError::Overflow)?;
        for dependency in values {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            bytes = bytes.checked_add(Self::mutation_region_name_bytes(&dependency.region))
                .ok_or(WorkError::Overflow)?;
        }
        Ok(bytes)
    }

    pub(super) fn append_mutation_dependencies(
        destination: &mut crate::kernel::Owned<Vec<Dependency>>,
        mut incoming: crate::kernel::Owned<Vec<Dependency>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        for dependency in incoming.value.drain(..) {
            destination.ensure_for_push(usize::MAX, accounting)?;
            let bytes = Self::mutation_region_name_bytes(&dependency.region);
            let allocation = crate::kernel::split_allocation(
                &mut incoming.allocation, bytes,
            )?;
            let mut owned = (dependency, allocation);
            destination.allocation.absorb(&mut owned.1)?;
            incoming.bytes = incoming.bytes.checked_sub(bytes).ok_or(WorkError::Overflow)?;
            destination.bytes = destination.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
            accounting.charge(WorkCounters {
                bytes_copied: size_of::<Dependency>() as u64, ..WorkCounters::UNCHARGED
            })?;
            destination.value.push(owned.0);
        }
        incoming.retire(accounting)
    }

    pub(super) fn subsume_mutation_record_dependencies(
        regions: &mut Vec<(Option<FileRecord>, DependencyRegion)>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let mut records = crate::kernel::Owned::<Vec<FileId>>::allocate(
            regions.len(), accounting,
        )?;
        for (_, region) in regions.iter() {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if let DependencyRegion::FileRecord(file_id) = region {
                let position = crate::kernel::binary_search_with_accounting(
                    &records.value, accounting, |value| value.cmp(file_id),
                )?;
                if let Err(index) = position {
                    accounting.charge(WorkCounters {
                        bytes_copied: ((records.value.len() - index + 1) * size_of::<FileId>()) as u64,
                        ..WorkCounters::UNCHARGED
                    })?;
                    records.value.insert(index, *file_id);
                }
            }
        }
        let mut index = 0;
        while index < regions.len() {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            let file_id = match &regions[index].1 {
                DependencyRegion::ContentRange { file_id, .. }
                | DependencyRegion::FileLength(file_id)
                | DependencyRegion::SparseSeek { file_id, .. } => Some(file_id),
                _ => None,
            };
            let removed = match file_id {
                Some(file_id) => crate::kernel::binary_search_with_accounting(
                    &records.value, accounting, |value| value.cmp(file_id),
                )?.is_ok(),
                None => false,
            };
            if removed {
                accounting.charge(WorkCounters {
                    bytes_copied: ((regions.len() - index - 1) * size_of::<(Option<FileRecord>, DependencyRegion)>()) as u64,
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
        crate::heap_future::in_heap_measured(move |budget| async move {
            if self.mode.access != AccessMode::ReadWrite
                || !matches!(
                    self.mode.mutations,
                    MutationMode::PrivateOverlay | MutationMode::DirectLive
                )
            {
                return Err(OperationFailure::before_work(FsError::MutationNotAllowed));
            }
            self.mutate_candidate(operations, budget, cancellation)
                .await
        }, budget, cancellation, FsError::Cancelled(CancellationError), None)
        .await
    }

    pub(super) async fn mutate_candidate(
        &mut self,
        operations: Vec<Mutation>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.mutate_candidate_with_accounting(
            operations, None, &mut accounting, cancellation,
        ).await;
        measured(result, &accounting)
    }

    /// Transfers an already-paid operation vector to the canonical candidate
    pub(crate) async fn mutate_owned_with_accounting(
        &mut self,
        mut operations: crate::kernel::Owned<Vec<Mutation>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        use crate::kernel::allocation::KernelOwnedAllocation;
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        match (accounting.native_work(), &operations.allocation) {
            (Some(original), KernelOwnedAllocation::NativeOriginal(allocation)) => {
                if !allocation.work().shares_ledger(original) {
                    return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
                }
                self.validate_native_work(original)?;
            }
            (None, KernelOwnedAllocation::Standalone) => {}
            _ => return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout)),
        }
        if self.mode.access != AccessMode::ReadWrite
            || !matches!(self.mode.mutations, MutationMode::PrivateOverlay | MutationMode::DirectLive)
        {
            return Err(FsError::MutationNotAllowed);
        }
        self.mutate_candidate_with_accounting(
            operations.value, Some(&mut operations.allocation), accounting, cancellation,
        ).await
    }

    pub(super) async fn mutate_candidate_with_accounting(
        &mut self,
        mut operations: Vec<Mutation>,
        operations_allocation: Option<&mut crate::kernel::allocation::KernelOwnedAllocation>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
        accounting.check()?;
        if self.live_operation_id.is_some() { return Err(FsError::PendingLiveMutation); }
        if self.prepared_merge_parent.is_some() { return Err(FsError::PreparedMergePending); }
        let maximum = usize::try_from(self.volume.config.limits.maximum_mutations_per_batch)
            .unwrap_or(usize::MAX);
        if operations.capacity() > maximum { return Err(FsError::ExcessiveMutationCapacity); }
        let incoming_bytes = Self::mutation_input_owned_bytes(&operations, accounting)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(KernelOwnedAllocation::NativeOriginal(guard)) = operations_allocation.as_deref() {
            if guard.bytes() != incoming_bytes {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        if operations_allocation.is_none() && accounting.native_work().is_some() {
            return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
        }
        let mut ordinary_input_allocation;
        let operations_allocation = match operations_allocation {
            Some(allocation) => allocation,
            None => {
                ordinary_input_allocation = accounting.claim_owned(incoming_bytes, 0)?;
                &mut ordinary_input_allocation
            }
        };
        self.admit_mutation_names_with_accounting(
            &mut operations, operations_allocation, accounting, cancellation,
        ).await?;
        let captured = self.capture_mutation_dependencies_with_accounting(
            &operations, accounting, cancellation,
        ).await?;
        let extension = self.dependencies.proof().prepare_mutations(
            captured.value, self.volume.config.limits.maximum_checkout_dependencies,
        )?;
        let staged_names = extension.retained_name_bytes_with_step(|| {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
        })?;
        let mut staged = (extension, captured.allocation);
        accounting.release_owned(
            &mut staged.1, captured.bytes.checked_sub(staged_names).ok_or(WorkError::Overflow)?,
        )?;
        let final_input_bytes = Self::mutation_input_owned_bytes(&operations, accounting)?;
        let produced = crate::kernel::apply_generation_mutations_retaining_with_accounting(
            &self.volume.fs.inner.objects, &self.root, operations, self.volume.config,
            accounting, cancellation,
        ).await;
        let (receipt, returned_operations, allocation) = produced?;
        let mut working = (receipt.root, allocation);
        drop(returned_operations);
        accounting.release_owned(operations_allocation, final_input_bytes)?;
        if working.0.file_table == self.root.file_table {
            let bytes = root_parent_bytes(&working.0)?;
            drop(working.0);
            crate::kernel::retire_allocation(working.1, bytes, accounting)?;
            drop(staged.0);
            accounting.release_owned(&mut staged.1, staged_names)?;
            return Ok(());
        }
        accounting.charge(WorkCounters {
            bytes_copied: size_of::<CheckoutDependencies>() as u64, ..WorkCounters::UNCHARGED
        })?;
        let mut prepared_dependencies = self.dependencies.proof().dependencies.clone();
        let producer = &self.dependencies.data_custody;
        let held = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } =>
                self.root.prepare_held_root_native(&working.0, accounting, producer)?,
            _ => {
                let (allocations, work, budget, token) = accounting.standalone_parts()
                    .ok_or(FsError::PendingMutationAllocationFailed)?;
                token.check()?;
                self.root.prepare_held_root(&working.0, allocations, work, budget, token)?
                    .map(|root| {
                        let control = Self::root_control_claim(&root, Some(producer))?;
                        Ok::<_, FsError>((root, control))
                    }).transpose()?
            }
        };
        if working.0.file_table == self.base_file_table {
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
                        staged.0, producer, allocation, original, cancellation,
                    )?;
                    // Duplicate keys have already physically died in commit.
                    allocation.release(allocation.bytes())?;
                }
                _ => prepared_dependencies.commit_with_data_custody(
                    staged.0, Some(producer), |bytes, operations, copied, items| {
                        let delta = WorkCounters { bytes_copied: copied, items_examined: items,
                            ..WorkCounters::UNCHARGED };
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
            KernelOwnedAllocation::Standalone => Self::root_control_claim(&working.0, Some(producer))?,
        };
        // No fallible work follows the transfer into the actual root producer.
        self.root.set_prepared_working(working.0, control, held);
        self.dependencies.proof().dependencies = prepared_dependencies;
        Ok(())
    }

    pub(super) fn mutation_input_owned_bytes(
        operations: &Vec<Mutation>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<u64, FsError> {
        let mut bytes = (operations.capacity() as u64).checked_mul(size_of::<Mutation>() as u64)
            .ok_or(WorkError::Overflow)?;
        for operation in operations {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            bytes = bytes.checked_add(mutation_path_owned_bytes(operation)).ok_or(WorkError::Overflow)?;
        }
        Ok(bytes)
    }

    pub(super) async fn observe_identity_regions_with_accounting(
        &mut self, regions: [Option<DependencyRegion>; 2],
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let count = regions.iter().flatten().count();
            let mut requested = crate::kernel::Owned::<Vec<DependencyRegion>>::allocate(count, accounting)?;
            for region in regions.into_iter().flatten() {
                let copied = WorkCounters {
                    bytes_copied: size_of::<DependencyRegion>() as u64, ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                requested.value.push(region);
                accounting.record_completed(copied)?;
            }
            let (probe, allocation) = AuthenticatedGenerationProbe::new_with_accounting(
                &self.volume.fs.inner.objects, probe_limits(self.volume.config), accounting,
            )?;
            let probe = (probe.with_data_custody(Some(&self.dependencies.data_custody)), allocation);
            let input = std::mem::take(&mut requested.value);
            let captured = probe.0.capture_many_with_accounting(
                GenerationId::new(self.root.base().digest), input,
                self.volume.config.limits.maximum_checkout_dependencies, accounting, cancellation,
            ).await;
            // The canonical capture consumes the input backing before returning.
            requested.retire(accounting)?;
            let (value, allocation) = captured?;
            let bytes = value.capacity().checked_mul(size_of::<Dependency>())
                .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?;
            let captured = crate::kernel::Owned::new(value, allocation, bytes);
            let (probe, allocation) = probe;
            probe.retire_with_accounting(accounting)?;
            drop(allocation);
            self.extend_observations_with_accounting(captured, accounting, cancellation)
        }.await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }

    pub(super) async fn observe_base_regular_range_with_accounting(
        &mut self, file_id: FileId, range: ByteRange,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let (record, allocation) = crate::kernel::lookup_file_record_async_with_accounting(
                &self.volume.fs.inner.objects, self.base_root.file_table, file_id,
                DecodeLimits::for_volume(self.volume.config), accounting, cancellation,
                Some(&self.dependencies.data_custody),
            ).await?;
            drop(allocation);
            self.observe_identity_regions_with_accounting(
                regular_range_regions(record, range.offset, range.length), accounting, cancellation,
            ).await
        }.await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }

    pub(super) async fn observe_base_metadata_ids_with_accounting(
        &mut self, file_ids: &[FileId],
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            accounting.check()?;
            let selected = {
            let proof = self.dependencies.proof();
            let mut count = 0_usize;
            for file_id in file_ids {
                let examined = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
                accounting.admit(examined)?;
                count += usize::from(!proof.observes(&DependencyRegion::Metadata(*file_id)));
                accounting.record_completed(examined)?;
            }
            let mut selected = crate::kernel::Owned::<Vec<FileId>>::allocate(count, accounting)?;
            for file_id in file_ids {
                let examined = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
                accounting.admit(examined)?;
                let observed = proof.observes(&DependencyRegion::Metadata(*file_id));
                accounting.record_completed(examined)?;
                if !observed {
                    let copied = WorkCounters { bytes_copied: size_of::<FileId>() as u64, ..WorkCounters::UNCHARGED };
                    accounting.admit(copied)?;
                    selected.value.push(*file_id);
                    accounting.record_completed(copied)?;
                }
            }
                selected
            };
            if selected.value.is_empty() { return selected.retire(accounting); }
            let maximum = u32::try_from(selected.value.len()).unwrap_or(u32::MAX);
            let (records, allocation) = crate::kernel::lookup_file_records_async_with_accounting(
                &self.volume.fs.inner.objects, self.base_root.file_table, &selected.value, maximum,
                DecodeLimits::for_volume(self.volume.config), accounting, cancellation,
                Some(&self.dependencies.data_custody),
            ).await?;
            let records = (records, allocation);
            let mut dependencies = crate::kernel::Owned::<Vec<Dependency>>::allocate(selected.value.len(), accounting)?;
            for (file_id, record) in selected.value.iter().zip(&records.0) {
                let copied = WorkCounters {
                    items_examined: 1, bytes_copied: size_of::<Dependency>() as u64,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                dependencies.value.push(Dependency {
                    region: DependencyRegion::Metadata(*file_id),
                    expected: record.map_or(DependencyState::Absent, |record| DependencyState::Present(record.metadata.digest)),
                });
                accounting.record_completed(copied)?;
            }
            drop(records);
            selected.retire(accounting)?;
            self.extend_observations_with_accounting(dependencies, accounting, cancellation)
        }.await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }

    pub(super) fn extend_observations_with_accounting(
        &mut self,
        mut dependencies: crate::kernel::Owned<Vec<Dependency>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let values = std::mem::take(&mut dependencies.value);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(staged) = &mut dependencies.allocation else {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            };
            return self.dependencies.extend_observations_with_native_work(
                values, staged, self.volume.config.limits.maximum_checkout_dependencies, original, cancellation,
            );
        }
        let (_, work, budget, cancellation) = accounting.standalone_parts()
            .ok_or(FsError::UnsupportedCheckout)?;
        self.dependencies.extend_observations(
            values, self.volume.config.limits.maximum_checkout_dependencies, work, budget, cancellation,
        )
    }
}

pub(super) fn exact_mutation_regions(
    mutation: &Mutation,
    first: Option<FileRecord>,
    second: Option<FileRecord>,
) -> [Option<(Option<FileRecord>, DependencyRegion)>; 4] {
    let mut regions = [None, None, None, None];
    match mutation {
        Mutation::Create { .. } | Mutation::Link { .. } => {}
        Mutation::Restore { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::Remove { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::Rename { .. } => {
            regions[0] =
                second.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::SetMetadata { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::Metadata(record.file_id)));
        }
        Mutation::Write { offset, length, .. } | Mutation::ZeroRange { offset, length, .. } => {
            let [content, file_length] = regular_range_regions(first, *offset, *length);
            regions[0] = content.map(|region| (first, region));
            regions[1] = file_length.map(|region| (first, region));
        }
        Mutation::ValidateRegular { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::Preallocate { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileLength(record.file_id)));
        }
        Mutation::Resize { .. } => {
            // Resize can discard an arbitrarily large suffix. Until a compact
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::CloneRange {
            source_offset,
            destination_offset,
            length,
            ..
        } => {
            let [source_content, source_length] =
                regular_range_regions(first, *source_offset, *length);
            let [destination_content, destination_length] =
                regular_range_regions(second, *destination_offset, *length);
            regions[0] = source_content.map(|region| (first, region));
            regions[1] = source_length.map(|region| (first, region));
            regions[2] = destination_content.map(|region| (second, region));
            regions[3] = destination_length.map(|region| (second, region));
        }
        Mutation::File { file_id, mutation } => match mutation {
            FileMutation::SetMetadata { .. } => {
                regions[0] = Some((first, DependencyRegion::Metadata(*file_id)));
            }
            FileMutation::Write { offset, length, .. }
            | FileMutation::ZeroRange { offset, length, .. } => {
                let [content, file_length] = regular_range_regions(first, *offset, *length);
                regions[0] = content.map(|region| (first, region));
                regions[1] = file_length.map(|region| (first, region));
            }
            FileMutation::Preallocate { .. } => {
                regions[0] = Some((first, DependencyRegion::FileLength(*file_id)));
            }
            FileMutation::ReplaceRecord { .. }
            | FileMutation::ValidateRegular
            | FileMutation::Resize { .. } => {
                regions[0] = Some((first, DependencyRegion::FileRecord(*file_id)));
            }
        },
        Mutation::CloneFileRange {
            source_file_id,
            source_offset,
            destination_file_id,
            destination_offset,
            length,
        } => {
            let [source_content, source_length] =
                regular_range_regions(first, *source_offset, *length);
            let [destination_content, destination_length] =
                regular_range_regions(second, *destination_offset, *length);
            regions[0] = source_content.map(|region| (first, region));
            regions[1] = source_length.map(|region| (first, region));
            regions[2] = destination_content.map(|region| (second, region));
            regions[3] = destination_length.map(|region| (second, region));
            debug_assert!(first.is_none_or(|record| record.file_id == *source_file_id));
            debug_assert!(second.is_none_or(|record| record.file_id == *destination_file_id));
        }
    }
    regions
}

