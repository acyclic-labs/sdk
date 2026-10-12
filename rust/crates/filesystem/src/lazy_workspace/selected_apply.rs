use super::*;
pub(crate) struct SelectedImportEffects {
    pub(crate) paths: Vec<String>,
    pub(crate) records: Vec<(FileRecord, FileMetadata)>,
    pub(crate) changes: Vec<NamespacePath>,
    _allocation: KernelOwnedAllocation,
}
impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    #[cfg(feature = "native-mount")]
    pub(crate) async fn prepare_selected_import_into_checkout(
        &self,
        destination: LazySnapshotRef,
        plan: &SelectedImportPlan,
        checkout: &mut crate::Checkout<A, O>,
        operation: crate::OperationId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<SelectedImportEffects, LazyWorkspaceError> {
        self.validate_snapshot(destination, ctx)?;
        if plan.source_used && plan.source.source != self.source.reference() {
            return Err(LazyWorkspaceError::StaleSource);
        }
        if plan.source_config.profile != self.workspace.profile() {
            return Err(LazyWorkspaceError::UnsupportedNode);
        }
        let current = self.snapshot(ctx).await?;
        let (mut paths, _) = self.selected_paths(destination, &plan.roots, ctx).await?;
        let baseline = self
            .workspace
            .checkout_accounted(
                crate::model::GenerationSelector::Exact(destination.authored_generation),
                crate::model::CheckoutMode::tracking_transaction(),
                ctx,
                cancellation(ctx),
            )
            .await?;
        let mut baseline = crate::Transaction::from_checkout(
            &self.workspace,
            baseline,
            IdempotencyKey::from_bytes(operation.into_bytes()),
            false,
        );
        let candidate = checkout.private_candidate_with_accounting(ctx, cancellation(ctx))?;
        let mut transaction = crate::Transaction::from_checkout(
            &self.workspace,
            candidate,
            IdempotencyKey::from_bytes(operation.into_bytes()),
            false,
        );
        let mut records: Owned<Vec<(FileRecord, FileMetadata)>> =
            Owned::allocate(paths.value.len(), ctx)?;
        let mut changes = Owned::allocate(paths.value.len() + plan.entries.len(), ctx)?;
        let result = async {
            for path in &paths.value {
                self.stage_selected_path(&mut baseline.checkout, destination, path, ctx)
                    .await?;
                self.stage_selected_path(&mut transaction.checkout, current, path, ctx)
                    .await?;
                let before = self
                    .authored_record(&mut baseline.checkout, path, ctx)
                    .await?;
                let actual = self
                    .authored_record(&mut transaction.checkout, path, ctx)
                    .await?;
                ctx.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if !same_record(before, actual) {
                    return Err(LazyWorkspaceError::StaleIdentity);
                }
                if let Some(actual) = actual {
                    let mut duplicate = false;
                    for (record, _) in &records.value {
                        ctx.charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })?;
                        if record.file_id == actual.0.file_id {
                            duplicate = true;
                            break;
                        }
                    }
                    if !duplicate {
                        records.value.push(actual);
                    }
                }
            }
            for entry in &plan.entries {
                if !paths.value.iter().any(|path| path == &entry.path) {
                    let before = self
                        .authored_record(&mut baseline.checkout, &entry.path, ctx)
                        .await?;
                    let actual = self
                        .authored_record(&mut transaction.checkout, &entry.path, ctx)
                        .await?;
                    if !same_record(before, actual) {
                        return Err(LazyWorkspaceError::StaleIdentity);
                    }
                }
                if entry.record.is_none() {
                    continue;
                }
                for (end, _) in entry.path.match_indices('/').skip(1) {
                    let parent = &entry.path[..end];
                    if plan.entries.iter().any(|entry| {
                        entry.path == parent
                            && entry
                                .record
                                .is_some_and(|(record, _)| record.kind == FileKind::Directory)
                    }) {
                        continue;
                    }
                    if !self
                        .stage_selected_path(&mut transaction.checkout, current, parent, ctx)
                        .await?
                        .is_some_and(|(record, _)| record.kind == FileKind::Directory)
                    {
                        return Err(LazyWorkspaceError::NotDirectory);
                    }
                }
            }
            for path in paths.value.iter().rev() {
                if path == "/" {
                    continue;
                }
                if self
                    .authored_record(&mut transaction.checkout, path, ctx)
                    .await?
                    .is_some()
                {
                    accounted!(ctx, |work, cancel| transaction.remove(path, work, cancel))?;
                }
                let namespace = self.namespace_path(path, ctx)?;
                crate::Transaction::<A, O>::push_mutation(&mut changes, namespace, ctx)?;
            }
            for (index, entry) in plan.entries.iter().enumerate() {
                let Some((record, _)) = entry.record else {
                    continue;
                };
                let alias = (record.kind != FileKind::Directory)
                    .then(|| {
                        plan.entries[..index].iter().find(|entry| {
                            entry
                                .record
                                .is_some_and(|(other, _)| other.file_id == record.file_id)
                        })
                    })
                    .flatten();
                if let Some(alias) = alias {
                    accounted!(ctx, |work, cancel| transaction.hard_link(
                        &alias.path,
                        &entry.path,
                        work,
                        cancel
                    ))?;
                } else {
                    let namespace = self.namespace_path(&entry.path, ctx)?;
                    let compiled = transaction
                        .checkout
                        .compile_copied_record(namespace, record, ctx, cancellation(ctx))
                        .await?;
                    let operation =
                        Owned::new(compiled.value.0, compiled.allocation, compiled.bytes);
                    let mut operations = Owned::allocate(1, ctx)?;
                    crate::Transaction::<A, O>::push_mutation(&mut operations, operation, ctx)?;
                    transaction
                        .checkout
                        .mutate_owned_with_accounting(operations, ctx, cancellation(ctx))
                        .await?;
                }
                let namespace = self.namespace_path(&entry.path, ctx)?;
                crate::Transaction::<A, O>::push_mutation(&mut changes, namespace, ctx)?;
            }
            for root in &plan.roots {
                if paths.value.iter().any(|path| path == root) {
                    continue;
                }
                let path = self.canonical_path(root, ctx)?;
                if let Err(error) = paths.ensure_for_push(usize::MAX, ctx) {
                    path.retire(ctx)?;
                    return Err(error.into());
                }
                crate::Transaction::<A, O>::push_mutation(&mut paths, path, ctx)?;
            }
            if plan.source_used && plan.source.source != self.source.reference() {
                return Err(LazyWorkspaceError::StaleSource);
            }
            Ok::<_, LazyWorkspaceError>(())
        }
        .await;
        drop(baseline);
        if let Err(error) = result {
            drop(transaction);
            changes.retire(ctx)?;
            records.retire(ctx)?;
            paths.retire(ctx)?;
            return Err(error);
        }
        *checkout = transaction.checkout;
        paths.allocation.absorb(&mut records.allocation)?;
        paths.allocation.absorb(&mut changes.allocation)?;
        Ok(SelectedImportEffects {
            paths: paths.value,
            records: records.value,
            changes: changes.value,
            _allocation: paths.allocation,
        })
    }
    #[cfg(feature = "native-mount")]
    async fn stage_selected_path(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        cut: LazySnapshotRef,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(FileRecord, FileMetadata)>, LazyWorkspaceError> {
        if let Some(record) = self.authored_record(checkout, path, ctx).await? {
            return Ok(Some(record));
        }
        let (lookup, _, guard) = self.lookup_at_snapshot(cut, path, ctx).await?;
        let Some(lookup) = lookup else {
            crate::kernel::retire_allocation(guard, 0, ctx)?;
            return Ok(None);
        };
        let result = async {
            if matches!(&lookup,LazyLookup::Authored { path: original,.. } if original == path) {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            let expected = self.stable_file_id_for_lookup(path, &lookup, ctx).await?;
            let maximum = ctx.remaining()?.source_bytes_read;
            self.stage_into_checkout(checkout, Some(cut), path, expected, maximum, ctx)
                .await?;
            self.authored_record(checkout, path, ctx).await
        }
        .await;
        self.retire_lookup((lookup, guard), ctx)?;
        result
    }
}
fn same_record(
    before: Option<(FileRecord, FileMetadata)>,
    actual: Option<(FileRecord, FileMetadata)>,
) -> bool {
    match (before, actual) {
        (None, None) => true,
        (Some((before, metadata)), Some((mut actual, actual_metadata))) => {
            actual.link_count = before.link_count;
            actual == before && actual_metadata == metadata
        }
        _ => false,
    }
}
