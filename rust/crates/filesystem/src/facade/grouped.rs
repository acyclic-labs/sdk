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
        let output =
            Owned::<Vec<Result<GroupedOutcome, FsError>>>::allocate(changes.len(), &mut outer)
                .map_err(|error| OperationFailure::new(error, outer.spent()))?;
        let mut output = output;
        let mut work = outer.spent();
        for change in changes {
            let allowance = remaining_owned(WorkCounters::UNCHARGED, budget, output.bytes)?;
            let mut accounting = KernelAccounting::standalone(allowance, cancellation);
            let result = self
                .apply_grouped_change(
                    change,
                    &mut AuthoredMetadataCache::new(),
                    &mut accounting,
                    cancellation,
                )
                .await;
            work = work
                .with_backend(accounting.spent(), output.bytes)
                .map_err(|error| OperationFailure::new(FsError::from(error), work))?;
            output.value.push(result);
        }
        Ok(OperationReceipt {
            value: output.value,
            work,
        })
    }

    pub(crate) async fn apply_group_with_accounting(
        &mut self,
        input: Owned<Vec<GroupedChange>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<Vec<Result<GroupedOutcome, FsError>>>, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let expected = input
                .value
                .capacity()
                .checked_mul(size_of::<GroupedChange>())
                .ok_or(WorkError::Overflow)? as u64;
            let KernelOwnedAllocation::NativeOriginal(guard) = &input.allocation else {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            };
            if guard.bytes() != expected
                || !guard.work().shares_ledger(original)
                || !guard.work().shares_resource_allowance(original)
            {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            }
        }
        let mut output =
            Owned::<Vec<Result<GroupedOutcome, FsError>>>::allocate(input.value.len(), accounting)?;
        let mut input = input;
        let mut metadata = AuthoredMetadataCache::new();
        for change in input.value.drain(..) {
            output.value.push(
                self.apply_grouped_change(change, &mut metadata, accounting, cancellation)
                    .await,
            );
        }
        input.retire(accounting)?;
        Ok(output)
    }

    async fn apply_grouped_change(
        &mut self,
        change: GroupedChange,
        metadata: &mut AuthoredMetadataCache,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<GroupedOutcome, FsError> {
        accounting.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let (operations, outcome) = match change {
            GroupedChange::CreateFile {
                path,
                metadata: attributes,
                file_id,
                bytes: content,
            } => {
                accounting.charge(WorkCounters {
                    bytes_copied: size_of::<FileMetadata>() as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                let path = clone_authored_path(&path, accounting)?;
                let mut operations = Owned::<Vec<Mutation>>::allocate(
                    1 + usize::from(content.len() > crate::kernel::MAXIMUM_INLINE_FILE_BYTES),
                    accounting,
                )?;
                let Owned {
                    value: path,
                    mut allocation,
                    mut bytes,
                } = path;
                let result = self
                    .compile_create_file_with_accounting(
                        path,
                        content,
                        *attributes,
                        file_id,
                        &mut operations,
                        &mut allocation,
                        &mut bytes,
                        metadata,
                        accounting,
                        cancellation,
                    )
                    .await;
                retire_allocation(allocation, bytes, accounting)?;
                match result {
                    Ok(file_id) => (operations, GroupedOutcome::Created(file_id)),
                    Err(error) => {
                        operations.retire(accounting)?;
                        return Err(error);
                    }
                }
            }
            GroupedChange::Content {
                file_id,
                change,
                times,
            } => {
                let input = Owned::new((file_id, change), accounting.claim_owned(0, 0)?, 0);
                let operations = self
                    .compile_content_with_accounting(input, times, accounting, cancellation)
                    .await?;
                (operations, GroupedOutcome::Changed)
            }
        };
        self.apply_mutations(operations, accounting, cancellation)
            .await?;
        Ok(outcome)
    }
}
