use super::*;
use crate::kernel::{KernelAccounting, Owned};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Captures a visible subtree into complete authored state using bounded batches.
    pub async fn exactify(
        &self,
        root: &str,
        permit: crate::PublicationPermit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let lookup = self.inspect(root, accounting).await?;
        let directory = lookup.0.stat().kind == SourceNodeKind::Directory;
        self.retire_lookup(lookup, accounting)?;
        if !directory {
            return Err(LazyWorkspaceError::NotDirectory);
        }
        let capture = self.snapshot(accounting).await?;
        let state = LazyWorkspaceState::new(
            self.workspace.id(),
            capture.source,
            capture.overlay,
            capture.shadows,
        );
        self.exactify_at(root, capture, state, permit, accounting)
            .await
    }

    /// Materializes an immutable cut in a deterministic scratch fork.
    pub async fn exactify_snapshot(
        &self,
        snapshot: LazySnapshotRef,
        destination: &str,
        operation: IdempotencyKey,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        self.validate_snapshot(snapshot, accounting)?;
        let overlay = self
            .store
            .load_lazy_overlay(snapshot.overlay, accounting)
            .await?
            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
        retire_node(overlay, accounting)?;
        let shadows = self
            .store
            .load_lazy_shadow(snapshot.shadows, accounting)
            .await?
            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
        retire_node(shadows, accounting)?;
        let (child, _) = self
            .fork_snapshot(destination, snapshot, operation, accounting)
            .await?;
        child
            .exactify("/", crate::PublicationPermit::Unrestricted, accounting)
            .await
    }

    pub(super) async fn exactify_at(
        &self,
        root: &str,
        capture: LazySnapshotRef,
        state: LazyWorkspaceState,
        permit: crate::PublicationPermit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let (mut paths, _) = self
            .enumerate_paths(root, capture, &state, accounting)
            .await?;
        paths
            .value
            .sort_unstable_by(|a, b| exactify_path_order(a, b));
        let result = async {
            let limits = self.workspace.limits();
            let maximum = (limits.maximum_checkout_dependencies as usize
                / (limits.maximum_path_depth as usize + 4))
                .min(limits.maximum_mutations_per_batch as usize / 4)
                .max(1);
            let mut generation = capture.authored_generation;
            for (batch, chunk) in paths.value.chunks(maximum).enumerate() {
                accounting.check()?;
                let checkout = self.workspace.checkout_with_accounting(
                    crate::model::GenerationSelector::Exact(generation),
                    crate::model::CheckoutMode::tracking_transaction(),
                    accounting,
                    cancellation(accounting),
                ).await?;
                let key = exactify_batch_key(capture.id, root, batch as u64);
                let mut transaction = crate::Transaction::from_checkout(
                    &self.workspace, checkout, key, false,
                );
                let mut dirty = false;
                for path in chunk {
                    let (lookup, _, allocation) = self.inspect_unauthored(
                        &mut transaction.checkout,
                        path,
                        None,
                        InspectBasis {
                            state: Some(&state),
                            retain_pinned_observation: true,
                            ..InspectBasis::default()
                        },
                        accounting,
                    ).await?;
                    let lookup = lookup.ok_or(LazyWorkspaceError::NotFound)?;
                    let applied = async {
                        if matches!(&lookup, LazyLookup::Authored { path: authored, .. } if authored == path) {
                            return Ok(false);
                        }
                        if matches!(&lookup, LazyLookup::Source { .. }) {
                            accounting.charge(WorkCounters {
                                materializations: 1,
                                ..WorkCounters::UNCHARGED
                            })?;
                        }
                        if let Some(parent) = parent_path(path) {
                            transaction.create_dir_all_with_accounting(
                                parent, accounting, cancellation(accounting),
                            ).await?;
                        }
                        let maximum_bytes = accounting.remaining()?.source_bytes_read;
                        self.apply_resolved_promotion(
                            &mut transaction, path, &lookup, maximum_bytes, accounting,
                        ).await
                    }.await;
                    self.retire_lookup((lookup, allocation), accounting)?;
                    dirty |= applied?;
                }
                if dirty {
                    match self.commit(&mut transaction, permit, accounting).await? {
                        TransactionCommit::Committed(committed)
                        | TransactionCommit::AlreadyCommitted(committed) => generation = committed.id(),
                        TransactionCommit::Conflict { .. }
                        | TransactionCommit::Fenced
                        | TransactionCommit::IdempotencyConflict => return Err(LazyWorkspaceError::Concurrent),
                    }
                }
            }
            let cancellation = cancellation(accounting);
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                return self.workspace.generation(generation, original, cancellation)
                    .await.map_err(Into::into);
            }
            accept_receipt(
                self.workspace.generation(generation, accounting.remaining()?, cancellation).await,
                accounting,
            )
        }.await;
        paths.retire(accounting)?;
        result
    }

    pub(super) async fn enumerate_paths(
        &self,
        root: &str,
        capture: LazySnapshotRef,
        state: &LazyWorkspaceState,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Owned<Vec<String>>, bool), LazyWorkspaceError> {
        let root = self.canonical_path(root, accounting)?;
        let mut paths = Owned::allocate(1, accounting)?;
        crate::Transaction::<A, O>::push_workspace_mutation(&mut paths, root, accounting)?;
        let mut directories = Owned::allocate(1, accounting)?;
        let mut source_used = false;
        let mut checkout = self
            .checkout(
                crate::model::GenerationSelector::Exact(capture.authored_generation),
                accounting,
            )
            .await?;
        let result = async {
            let (lookup, used, allocation) = self
                .inspect_unauthored(
                    &mut checkout.value,
                    &paths.value[0],
                    Some(capture),
                    InspectBasis {
                        state: Some(state),
                        retain_pinned_observation: true,
                        ..InspectBasis::default()
                    },
                    accounting,
                )
                .await?;
            source_used |= used;
            if let Some(lookup) = lookup {
                if lookup.stat().kind == SourceNodeKind::Directory {
                    directories.value.push(0usize);
                }
                self.retire_lookup((lookup, allocation), accounting)?;
            } else {
                crate::kernel::retire_allocation(allocation, 0, accounting)?;
            }
            let mut next = 0;
            while next < directories.value.len() {
                accounting.check()?;
                let index = directories.value[next];
                next += 1;
                let namespace = self.namespace_path(&paths.value[index], accounting)?;
                let mut cursor = None;
                let enumerated = async {
                    loop {
                        let maximum = self
                            .workspace
                            .limits()
                            .maximum_directory_page_entries
                            .min(1024);
                        let (mut page, used, allocation) = self
                            .list_directory_at(
                                state,
                                &mut checkout.value,
                                &paths.value[index],
                                cursor,
                                maximum,
                                None,
                                accounting,
                            )
                            .await?;
                        source_used |= used;
                        let bytes = retained_directory_page_bytes(
                            &page.entries,
                            page.entries.iter().map(|entry| entry.name.retained_bytes()),
                        )?;
                        cursor = page.next.take();
                        let projected = (|| -> Result<(), LazyWorkspaceError> {
                            for entry in &page.entries {
                                let path = self
                                    .child_path(&namespace.value, &entry.name, accounting)?
                                    .ok_or(LazyWorkspaceError::UnsupportedNode)?;
                                paths.ensure_for_push(usize::MAX, accounting)?;
                                let index = paths.value.len();
                                crate::Transaction::<A, O>::push_workspace_mutation(
                                    &mut paths, path, accounting,
                                )?;
                                if entry.kind == SourceNodeKind::Directory {
                                    directories.ensure_for_push(usize::MAX, accounting)?;
                                    directories.value.push(index);
                                }
                            }
                            Ok(())
                        })();
                        drop(page);
                        crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                        if let Err(error) = projected {
                            if let Some(cursor) = cursor.take() {
                                cursor.retire(accounting)?;
                            }
                            return Err(error);
                        }
                        if cursor.is_none() {
                            break;
                        }
                    }
                    Ok(())
                }
                .await;
                namespace.retire(accounting)?;
                enumerated?;
            }
            Ok(())
        }
        .await;
        checkout.retire(accounting)?;
        directories.retire(accounting)?;
        if let Err(error) = result {
            paths.retire(accounting)?;
            return Err(error);
        }
        Ok((paths, source_used))
    }
}
