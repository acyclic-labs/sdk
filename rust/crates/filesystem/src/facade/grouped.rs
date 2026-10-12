use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {


    /// Applies independent changes as if one at a time in order, landing
    pub async fn apply_group(
        &mut self,
        changes: Vec<GroupedChange>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Result<GroupedOutcome, FsError>>> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let input = crate::kernel::Owned::new(
            changes, crate::kernel::allocation::KernelOwnedAllocation::Standalone, 0,
        );
        let result = self.apply_group_with_accounting(input, &mut accounting, cancellation).await;
        let work = accounting.spent();
        result.map(|output| FsReceipt { value: output.value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }


    pub(crate) async fn apply_group_with_accounting(
        &mut self,
        input: crate::kernel::Owned<Vec<GroupedChange>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<Vec<Result<GroupedOutcome, FsError>>>, FsError> {
        use crate::kernel::{Owned, split_allocation};
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let expected = input.value.capacity().checked_mul(size_of::<GroupedChange>())
                .ok_or(WorkError::Overflow)? as u64;
            let KernelOwnedAllocation::NativeOriginal(guard) = &input.allocation else {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            };
            if guard.bytes() != expected || !guard.work().shares_ledger(original)
                || !guard.work().shares_resource_allowance(original)
            {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            }
        }
        let mut compiled = Owned::<Vec<Result<(Owned<Vec<Mutation>>, GroupedOutcome), FsError>>>::allocate(input.value.len(), accounting)?;
        let mut output = Owned::<Vec<Result<GroupedOutcome, FsError>>>::allocate(input.value.len(), accounting)?;
        let mut metadata = AuthoredMetadataCache::new();
        let mut input = input;
        let result = async {
            // Only the input slots move into this guard. Original paths,
            for change in input.value.drain(..) {
                let guard = split_allocation(&mut input.allocation, 0)?;
                let change_input = Owned::new(change, guard, 0);
                let admitted = accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED });
                let change = match admitted {
                    Ok(()) => self.compile_grouped_with_accounting(change_input, &mut metadata, accounting, cancellation).await,
                    Err(error) => { change_input.retire(accounting)?; Err(error) }
                };
                compiled.value.push(change);
            }
            let applicable = compiled.value.iter().filter(|change| change.is_ok()).count();
            let mut combined_succeeded = false;
            if applicable > 1 {
                let combined = (|| {
                    let count = compiled.value.iter().flatten().try_fold(0usize, |count, (operations, _)| {
                        count.checked_add(operations.value.len()).ok_or(FsError::Work(WorkError::Overflow))
                    })?;
                    let mut combined = Owned::allocate(count, accounting)?;
                    let copied = (|| {
                        for (operations, _) in compiled.value.iter().flatten() {
                            for mutation in &operations.value {
                                let cloned = clone_grouped_mutation_with_accounting(mutation, accounting, cancellation)?;
                                combined.bytes = combined.bytes.checked_add(cloned.bytes).ok_or(WorkError::Overflow)?;
                                let mut guard = cloned.allocation;
                                readonly_absorb_allocation(&mut combined.allocation, &mut guard)?;
                                combined.value.push(cloned.value);
                            }
                        }
                        Ok::<(), FsError>(())
                    })();
                    if let Err(error) = copied { combined.retire(accounting)?; return Err(error); }
                    Ok(combined)
                })();
                if let Ok(operations) = combined {
                    combined_succeeded = self.mutate_owned_with_accounting(operations, accounting, cancellation).await.is_ok();
                }
            }
            for change in compiled.value.drain(..) {
                output.value.push(match change {
                    Ok((operations, outcome)) if combined_succeeded => {
                        operations.retire(accounting)?;
                        Ok(outcome)
                    }
                    Ok((operations, outcome)) => self.mutate_owned_with_accounting(operations, accounting, cancellation).await.map(|()| outcome),
                    Err(error) => Err(error),
                });
            }
            Ok::<(), FsError>(())
        }.await;
        input.retire(accounting)?;
        compiled.retire(accounting)?;
        if let Err(error) = result { output.retire(accounting)?; return Err(error); }
        Ok(output)
    }


    pub(super) async fn compile_grouped_with_accounting(
        &mut self,
        input: crate::kernel::Owned<GroupedChange>,
        staged_metadata: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(crate::kernel::Owned<Vec<Mutation>>, GroupedOutcome), FsError> {
        let crate::kernel::Owned { value: change, allocation, bytes } = input;
        match change {
            GroupedChange::CreateFile { path, metadata, file_id, bytes: content } => {
                accounting.charge(WorkCounters {
                    bytes_copied: size_of::<FileMetadata>() as u64, ..WorkCounters::UNCHARGED
                })?;
                let cloned = match clone_authored_path(&path, accounting) {
                    Ok(cloned) => cloned,
                    Err(error) => {
                        drop((path, metadata, content));
                        crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                        return Err(error);
                    }
                };
                drop(path);
                crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                let crate::kernel::Owned { value: path, mut allocation, mut bytes } = cloned;
                let count = 1 + usize::from(content.len() > crate::kernel::MAXIMUM_INLINE_FILE_BYTES);
                let mut operations = match crate::kernel::Owned::allocate(count, accounting) {
                    Ok(operations) => operations,
                    Err(error) => {
                        drop((path, metadata, content));
                        crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                        return Err(error);
                    }
                };
                let result = self.compile_create_file_with_accounting(
                    path, content, *metadata, file_id, &mut operations, &mut allocation,
                    &mut bytes, staged_metadata, accounting, cancellation,
                ).await;
                drop(metadata);
                crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                match result {
                    Ok(file_id) => Ok((operations, GroupedOutcome::Created(file_id))),
                    Err(error) => { operations.retire(accounting)?; Err(error) }
                }
            }
            GroupedChange::Content { file_id, change, times } => {
                let input = crate::kernel::Owned::new((file_id, change), allocation, bytes);
                self.compile_content_with_accounting(input, times, accounting, cancellation).await
                    .map(|operations| (operations, GroupedOutcome::Changed))
            }
        }
    }
}

pub(super) fn clone_grouped_mutation_with_accounting(
    mutation: &Mutation,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<crate::kernel::Owned<Mutation>, FsError> {
    accounting.charge(WorkCounters { bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED })?;
    let mut allocation = accounting.claim_owned(0, 0)?;
    let mut bytes = 0_u64;
    let result = (|| {
    let mut path = |path: &NamespacePath| -> Result<NamespacePath, FsError> {
        let (cloned, mut guard) = path.clone_with_accounting(accounting, cancellation)?;
        let retained = cloned.retained_bytes();
        readonly_absorb_allocation(&mut allocation, &mut guard)?;
        bytes = bytes.checked_add(retained).ok_or(WorkError::Overflow)?;
        Ok(cloned)
    };
    let cloned = match mutation {
        Mutation::Create { path: source, record } => Mutation::Create { path: path(source)?, record: *record },
        Mutation::Restore { path: source, record } => Mutation::Restore { path: path(source)?, record: *record },
        Mutation::Remove { path: source, expected_file_id } => Mutation::Remove {
            path: path(source)?, expected_file_id: *expected_file_id,
        },
        Mutation::Rename { source, destination, replace } => Mutation::Rename {
            source: path(source)?, destination: path(destination)?, replace: *replace,
        },
        Mutation::Link { source, destination } => Mutation::Link {
            source: path(source)?, destination: path(destination)?,
        },
        Mutation::SetMetadata { path: source, metadata } => Mutation::SetMetadata {
            path: path(source)?, metadata: *metadata,
        },
        Mutation::Write { path: source, offset, length, content, content_offset } => Mutation::Write {
            path: path(source)?, offset: *offset, length: *length, content: *content, content_offset: *content_offset,
        },
        Mutation::ValidateRegular { path: source } => Mutation::ValidateRegular { path: path(source)? },
        Mutation::Resize { path: source, logical_bytes } => Mutation::Resize {
            path: path(source)?, logical_bytes: *logical_bytes,
        },
        Mutation::ZeroRange { path: source, offset, length, allocated, extend } => Mutation::ZeroRange {
            path: path(source)?, offset: *offset, length: *length, allocated: *allocated, extend: *extend,
        },
        Mutation::Preallocate { path: source, offset, length, keep_size } => Mutation::Preallocate {
            path: path(source)?, offset: *offset, length: *length, keep_size: *keep_size,
        },
        Mutation::CloneRange { source, source_offset, destination, destination_offset, length } => Mutation::CloneRange {
            source: path(source)?, source_offset: *source_offset, destination: path(destination)?,
            destination_offset: *destination_offset, length: *length,
        },
        Mutation::File { file_id, mutation } => Mutation::File { file_id: *file_id, mutation: mutation.clone() },
        Mutation::CloneFileRange { source_file_id, source_offset, destination_file_id, destination_offset, length } => Mutation::CloneFileRange {
            source_file_id: *source_file_id, source_offset: *source_offset, destination_file_id: *destination_file_id,
            destination_offset: *destination_offset, length: *length,
        },
    };
    Ok::<_, FsError>(cloned)
    })();
    match result {
        Ok(cloned) => Ok(crate::kernel::Owned::new(cloned, allocation, bytes)),
        Err(error) => {
            crate::kernel::retire_allocation(allocation, bytes, accounting)?;
            Err(error)
        }
    }
}

