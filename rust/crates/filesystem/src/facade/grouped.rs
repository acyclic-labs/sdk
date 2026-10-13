use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Applies independent changes in order with one budget per change.
    pub async fn apply_group(
        &mut self,
        changes: Vec<GroupedChange>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Result<GroupedOutcome, FsError>>> {
        let mut outer = KernelAccounting::standalone(budget, cancellation);
        let mut output =
            Owned::<Vec<Result<GroupedOutcome, FsError>>>::allocate(changes.len(), &mut outer)
                .map_err(|error| OperationFailure::new(error, outer.spent()))?;
        let live = output.bytes;
        let mut work = outer.spent();
        for change in changes {
            let allowance = remaining_owned(WorkCounters::UNCHARGED, budget, live)?;
            let mut accounting = KernelAccounting::standalone(allowance, cancellation);
            let result = async {
                let mut input = Owned::<Vec<GroupedChange>>::allocate(1, &mut accounting)?;
                accounting.charge(WorkCounters {
                    bytes_copied: size_of::<GroupedChange>() as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                input.value.push(change);
                self.apply_group_with_accounting(input, &mut accounting, cancellation)
                    .await
            }
            .await;
            let next = accounting.spent();
            work = work
                .with_backend(next, live)
                .map_err(|error| OperationFailure::new(FsError::from(error), work))?;
            match result {
                Ok(mut outcomes) => {
                    let outcome = outcomes
                        .value
                        .pop()
                        .ok_or_else(|| OperationFailure::new(FsError::InvalidDiff, work))?;
                    outcomes
                        .retire(&mut accounting)
                        .map_err(|error| OperationFailure::new(error, work))?;
                    output.value.push(outcome);
                }
                Err(error) => output.value.push(Err(error)),
            }
        }
        let value = output.value;
        drop(output.allocation);
        Ok(OperationReceipt { value, work })
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
            let expected = input
                .value
                .capacity()
                .checked_mul(size_of::<GroupedChange>())
                .ok_or(WorkError::Overflow)? as u64;
            let KernelOwnedAllocation::NativeOriginal(guard) = &input.allocation else {
                return Err(FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ));
            };
            if guard.bytes() != expected
                || !guard.work().shares_ledger(original)
                || !guard.work().shares_resource_allowance(original)
            {
                return Err(FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ));
            }
        }
        let mut compiled =
            Owned::<Vec<Result<(Owned<Vec<Mutation>>, GroupedOutcome), FsError>>>::allocate(
                input.value.len(),
                accounting,
            )?;
        let mut output =
            Owned::<Vec<Result<GroupedOutcome, FsError>>>::allocate(input.value.len(), accounting)?;
        let mut metadata = AuthoredMetadataCache::new();
        let mut input = input;
        let result = async {
            // Only the input slots move into this guard. Original paths,
            for change in input.value.drain(..) {
                let guard = split_allocation(&mut input.allocation, 0)?;
                let change_input = Owned::new(change, guard, 0);
                let admitted = accounting.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                });
                let change = match admitted {
                    Ok(()) => {
                        self.compile_grouped_with_accounting(
                            change_input,
                            &mut metadata,
                            accounting,
                            cancellation,
                        )
                        .await
                    }
                    Err(error) => {
                        change_input.retire(accounting)?;
                        Err(error)
                    }
                };
                compiled.value.push(change);
            }
            for change in compiled.value.drain(..) {
                output.value.push(match change {
                    Ok((operations, outcome)) => self
                        .mutate_owned_with_accounting(operations, accounting, cancellation)
                        .await
                        .map(|()| outcome),
                    Err(error) => Err(error),
                });
            }
            Ok::<(), FsError>(())
        }
        .await;
        input.retire(accounting)?;
        compiled.retire(accounting)?;
        if let Err(error) = result {
            output.retire(accounting)?;
            return Err(error);
        }
        Ok(output)
    }

    pub(super) async fn compile_grouped_with_accounting(
        &mut self,
        input: crate::kernel::Owned<GroupedChange>,
        staged_metadata: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(crate::kernel::Owned<Vec<Mutation>>, GroupedOutcome), FsError> {
        let crate::kernel::Owned {
            value: change,
            allocation,
            bytes,
        } = input;
        match change {
            GroupedChange::CreateFile {
                path,
                metadata,
                file_id,
                bytes: content,
            } => {
                accounting.charge(WorkCounters {
                    bytes_copied: size_of::<FileMetadata>() as u64,
                    ..WorkCounters::UNCHARGED
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
                let crate::kernel::Owned {
                    value: path,
                    mut allocation,
                    mut bytes,
                } = cloned;
                let count =
                    1 + usize::from(content.len() > crate::kernel::MAXIMUM_INLINE_FILE_BYTES);
                let mut operations = match crate::kernel::Owned::allocate(count, accounting) {
                    Ok(operations) => operations,
                    Err(error) => {
                        drop((path, metadata, content));
                        crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                        return Err(error);
                    }
                };
                let result = self
                    .compile_create_file_with_accounting(
                        path,
                        content,
                        *metadata,
                        file_id,
                        &mut operations,
                        &mut allocation,
                        &mut bytes,
                        staged_metadata,
                        accounting,
                        cancellation,
                    )
                    .await;
                drop(metadata);
                crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                match result {
                    Ok(file_id) => Ok((operations, GroupedOutcome::Created(file_id))),
                    Err(error) => {
                        operations.retire(accounting)?;
                        Err(error)
                    }
                }
            }
            GroupedChange::Content {
                file_id,
                change,
                times,
            } => {
                let input = crate::kernel::Owned::new((file_id, change), allocation, bytes);
                self.compile_content_with_accounting(input, times, accounting, cancellation)
                    .await
                    .map(|operations| (operations, GroupedOutcome::Changed))
            }
        }
    }
}
