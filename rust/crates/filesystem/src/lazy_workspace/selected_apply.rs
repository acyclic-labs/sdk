use super::*;
use crate::kernel::{KernelAccounting, Owned, allocation::KernelOwnedAllocation};

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
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<SelectedImportEffects, LazyWorkspaceError> {
        self.validate_snapshot(destination, accounting)?;
        if plan.source_used && plan.source.source != self.source.reference() {
            return Err(LazyWorkspaceError::StaleSource);
        }
        if plan.source_config.profile != self.workspace.profile() {
            return Err(LazyWorkspaceError::UnsupportedNode);
        }
        let current = self.snapshot(accounting).await?;
        let (mut paths, _) = self
            .selected_paths(destination, &plan.roots, accounting)
            .await?;
        let baseline_checkout = self
            .workspace
            .checkout_with_accounting(
                crate::model::GenerationSelector::Exact(destination.authored_generation),
                crate::model::CheckoutMode::tracking_transaction(),
                accounting,
                cancellation(accounting),
            )
            .await?;
        let mut baseline = crate::Transaction::from_checkout(
            &self.workspace,
            baseline_checkout,
            IdempotencyKey::from_bytes(operation.into_bytes()),
            false,
        );
        baseline.checkout.bind_authored_operation(operation);
        let candidate =
            checkout.private_candidate_with_accounting(accounting, cancellation(accounting))?;
        let mut transaction = crate::Transaction::from_checkout(
            &self.workspace,
            candidate,
            IdempotencyKey::from_bytes(operation.into_bytes()),
            false,
        );
        transaction.checkout.bind_authored_operation(operation);
        let mut records: Owned<Vec<(FileRecord, FileMetadata)>> =
            Owned::allocate(paths.value.len(), accounting)?;
        let mut changes = Owned::allocate(paths.value.len() + plan.entries.len(), accounting)?;
        let result = async {
            for path in &paths.value {
                self.stage_selected_path(&mut baseline.checkout, destination, path, accounting)
                    .await?;
                self.stage_selected_path(&mut transaction.checkout, current, path, accounting)
                    .await?;
                let before = self
                    .authored_record(&mut baseline.checkout, path, accounting)
                    .await?;
                let actual = self
                    .authored_record(&mut transaction.checkout, path, accounting)
                    .await?;
                accounting.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if !same_selected_record(before, actual) {
                    return Err(LazyWorkspaceError::StaleIdentity);
                }
                if let Some(actual) = actual {
                    let mut duplicate = false;
                    for (record, _) in &records.value {
                        accounting.charge(WorkCounters {
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
                        .authored_record(&mut baseline.checkout, &entry.path, accounting)
                        .await?;
                    let actual = self
                        .authored_record(&mut transaction.checkout, &entry.path, accounting)
                        .await?;
                    if !same_selected_record(before, actual) {
                        return Err(LazyWorkspaceError::StaleIdentity);
                    }
                }
                if entry.record.is_some() {
                    for (end, _) in entry.path.match_indices('/').skip(1) {
                        let parent = &entry.path[..end];
                        if plan.entries.iter().any(|selected| {
                            selected.path == parent
                                && matches!(
                                    selected.record,
                                    Some((
                                        FileRecord {
                                            kind: FileKind::Directory,
                                            ..
                                        },
                                        _
                                    ))
                                )
                        }) {
                            continue;
                        }
                        let actual = self
                            .stage_selected_path(
                                &mut transaction.checkout,
                                current,
                                parent,
                                accounting,
                            )
                            .await?;
                        if !matches!(
                            actual,
                            Some((
                                FileRecord {
                                    kind: FileKind::Directory,
                                    ..
                                },
                                _
                            ))
                        ) {
                            return Err(LazyWorkspaceError::NotDirectory);
                        }
                    }
                }
            }
            for path in paths.value.iter().rev() {
                if path == "/" {
                    continue;
                }
                if self
                    .authored_record(&mut transaction.checkout, path, accounting)
                    .await?
                    .is_some()
                {
                    transaction
                        .apply_workspace_path_with_accounting(
                            path,
                            |path| AuthoredMutation::Remove {
                                path,
                                expected_file_id: None,
                            },
                            accounting,
                            cancellation(accounting),
                        )
                        .await?;
                }
                let namespace = self.namespace_path(path, accounting)?;
                crate::Transaction::<A, O>::push_workspace_mutation(
                    &mut changes,
                    namespace,
                    accounting,
                )?;
            }
            for (index, entry) in plan.entries.iter().enumerate() {
                let Some((record, _)) = entry.record else {
                    continue;
                };
                let alias = if record.kind != FileKind::Directory {
                    plan.entries[..index].iter().find(|other| {
                        other
                            .record
                            .is_some_and(|(other, _)| other.file_id == record.file_id)
                    })
                } else {
                    None
                };
                if let Some(alias) = alias {
                    let operations = transaction.prepare_hard_link(
                        &alias.path,
                        &entry.path,
                        accounting,
                        cancellation(accounting),
                    )?;
                    transaction
                        .apply_workspace_authored_with_accounting(
                            operations,
                            accounting,
                            cancellation(accounting),
                        )
                        .await?;
                } else {
                    let namespace = self.namespace_path(&entry.path, accounting)?;
                    let compiled = transaction
                        .checkout
                        .compile_copied_record(
                            namespace,
                            record,
                            accounting,
                            cancellation(accounting),
                        )
                        .await?;
                    let operation =
                        Owned::new(compiled.value.0, compiled.allocation, compiled.bytes);
                    let mut operations = Owned::allocate(1, accounting)?;
                    crate::Transaction::<A, O>::push_workspace_mutation(
                        &mut operations,
                        operation,
                        accounting,
                    )?;
                    transaction
                        .checkout
                        .mutate_owned_with_accounting(
                            operations,
                            accounting,
                            cancellation(accounting),
                        )
                        .await?;
                }
                let namespace = self.namespace_path(&entry.path, accounting)?;
                crate::Transaction::<A, O>::push_workspace_mutation(
                    &mut changes,
                    namespace,
                    accounting,
                )?;
            }
            for root in &plan.roots {
                if !paths.value.iter().any(|path| path == root) {
                    let path = self.canonical_path(root, accounting)?;
                    paths.ensure_for_push(usize::MAX, accounting)?;
                    crate::Transaction::<A, O>::push_workspace_mutation(
                        &mut paths, path, accounting,
                    )?;
                }
            }
            if plan.source_used && plan.source.source != self.source.reference() {
                return Err(LazyWorkspaceError::StaleSource);
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => (),
            Err(error) => {
                drop(transaction);
                drop(baseline);
                changes.retire(accounting)?;
                records.retire(accounting)?;
                paths.retire(accounting)?;
                return Err(error);
            }
        };
        *checkout = transaction.checkout;
        drop(baseline);
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
        snapshot: LazySnapshotRef,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<(FileRecord, FileMetadata)>, LazyWorkspaceError> {
        if let Some(record) = self.authored_record(checkout, path, accounting).await? {
            return Ok(Some(record));
        }
        let (lookup, _, allocation) = self.lookup_at_snapshot(snapshot, path, accounting).await?;
        let Some(lookup) = lookup else {
            crate::kernel::retire_allocation(allocation, 0, accounting)?;
            return Ok(None);
        };
        let result = async {
            if matches!(&lookup, LazyLookup::Authored { path: actual, .. } if actual == path) {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            let expected = self
                .stable_file_id_for_lookup(path, &lookup, accounting)
                .await?;
            let maximum = accounting.remaining()?.source_bytes_read;
            self.stage_into_checkout(
                checkout,
                Some(snapshot),
                path,
                expected,
                maximum,
                accounting,
            )
            .await?;
            self.authored_record(checkout, path, accounting).await
        }
        .await;
        self.retire_lookup((lookup, allocation), accounting)?;
        result
    }
}

fn same_selected_record(
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
