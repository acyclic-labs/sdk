use super::*;
impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Materializes one visible subtree through bounded SDK transactions.
    pub async fn exactify(
        &self,
        root: &str,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let lookup = self.inspect(root, ctx).await?;
        let directory = lookup.0.stat().kind == SourceNodeKind::Directory;
        self.retire_lookup(lookup, ctx)?;
        if !directory {
            return Err(LazyWorkspaceError::NotDirectory);
        }
        let cut = self.snapshot(ctx).await?;
        let state = LazyWorkspaceState::new(cut.workspace_id, cut.source, cut.overlay, cut.shadows);
        self.exactify_at(root, cut, state, permit, ctx).await
    }
    /// Materializes an immutable cut in a deterministic scratch fork.
    pub async fn exactify_snapshot(
        &self,
        cut: LazySnapshotRef,
        destination: &str,
        key: IdempotencyKey,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        self.validate_snapshot(cut, ctx).await?;
        retire_node(
            self.store
                .load_lazy_overlay(cut.overlay, ctx)
                .await?
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?,
            ctx,
        )?;
        retire_node(
            self.store
                .load_lazy_shadow(cut.shadows, ctx)
                .await?
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?,
            ctx,
        )?;
        let (child, _) = self.fork_snapshot(destination, cut, key, ctx).await?;
        child
            .exactify("/", crate::PublicationPermit::Unrestricted, ctx)
            .await
    }
    async fn exactify_at(
        &self,
        root: &str,
        cut: LazySnapshotRef,
        state: LazyWorkspaceState,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let (mut paths, _) = self.enumerate_paths(root, cut, &state, ctx).await?;
        paths
            .value
            .sort_unstable_by(|a, b| exactify_path_order(a, b));
        let result = async {
            let limits = self.workspace.limits();
            let maximum = (limits.maximum_checkout_dependencies as usize
                / (limits.maximum_path_depth as usize + 4))
                .min(limits.maximum_mutations_per_batch as usize / 4)
                .max(1);
            let mut generation = cut.authored_generation;
            for (batch, chunk) in paths.value.chunks(maximum).enumerate() {
                ctx.check()?;
                let checkout = self
                    .workspace
                    .checkout_accounted(
                        crate::model::GenerationSelector::Exact(generation),
                        crate::model::CheckoutMode::tracking_transaction(),
                        ctx,
                        cancellation(ctx),
                    )
                    .await?;
                let mut transaction = crate::Transaction::from_checkout(
                    &self.workspace,
                    checkout,
                    exactify_batch_key(cut.id, root, batch as u64),
                    false,
                );
                for path in chunk {
                    let (lookup, _, guard) = self
                        .inspect_unauthored(
                            &mut transaction.checkout,
                            path,
                            Some(cut),
                            InspectBasis {
                                state: Some(&state),
                                retain_pinned_observation: true,
                                ..InspectBasis::default()
                            },
                            ctx,
                        )
                        .await?;
                    if let Some(lookup) = lookup {
                        let promoted = self
                            .apply_resolved_promotion(
                                &mut transaction,
                                path,
                                &lookup,
                                u64::MAX,
                                ctx,
                            )
                            .await;
                        self.retire_lookup((lookup, guard), ctx)?;
                        promoted?;
                    } else {
                        crate::kernel::retire_allocation(guard, 0, ctx)?;
                    }
                }
                generation = match self.commit(&mut transaction, permit, ctx).await? {
                    TransactionCommit::Committed(generation)
                    | TransactionCommit::AlreadyCommitted(generation) => generation.id(),
                    _ => return Err(LazyWorkspaceError::Concurrent),
                };
            }
            accounted!(ctx, |work, cancel| self
                .workspace
                .generation(generation, work, cancel))
        }
        .await;
        paths.retire(ctx)?;
        result
    }

    pub(super) async fn enumerate_paths(
        &self,
        root: &str,
        cut: LazySnapshotRef,
        state: &LazyWorkspaceState,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Owned<Vec<String>>, bool), LazyWorkspaceError> {
        let root = self.canonical_path(root, ctx)?;
        let mut paths = Owned::allocate(1, ctx)?;
        crate::Transaction::<A, O>::push_mutation(&mut paths, root, ctx)?;
        let mut directories = Owned::allocate(1, ctx)?;
        let mut checkout = self
            .checkout(
                crate::model::GenerationSelector::Exact(cut.authored_generation),
                ctx,
            )
            .await?;
        let mut used = false;
        let result = async {
            let (lookup, source, guard) = self
                .inspect_unauthored(
                    &mut checkout.value,
                    &paths.value[0],
                    Some(cut),
                    InspectBasis {
                        state: Some(state),
                        retain_pinned_observation: true,
                        ..InspectBasis::default()
                    },
                    ctx,
                )
                .await?;
            used |= source;
            if let Some(lookup) = lookup {
                if lookup.stat().kind == SourceNodeKind::Directory {
                    directories.value.push(0usize);
                }
                self.retire_lookup((lookup, guard), ctx)?;
            } else {
                crate::kernel::retire_allocation(guard, 0, ctx)?;
            }
            let mut next = 0;
            while next < directories.value.len() {
                ctx.check()?;
                let index = directories.value[next];
                next += 1;
                let directory = self.namespace_path(&paths.value[index], ctx)?;
                let mut cursor = None;
                let listed = async {
                    loop {
                        let (mut page, source, guard) = self
                            .list_directory_at(
                                state,
                                &mut checkout.value,
                                &paths.value[index],
                                cursor.take(),
                                self.workspace
                                    .limits()
                                    .maximum_directory_page_entries
                                    .min(1024),
                                None,
                                ctx,
                            )
                            .await?;
                        used |= source;
                        let bytes = retained_directory_page_bytes(
                            &page.entries,
                            page.entries.iter().map(|e| e.name.retained_bytes()),
                        )?;
                        cursor = page.next.take();
                        let projected = (|| {
                            for entry in &page.entries {
                                let path = self
                                    .child_path(&directory.value, &entry.name, ctx)?
                                    .ok_or(LazyWorkspaceError::UnsupportedNode)?;
                                if let Err(error) = paths.ensure_for_push(usize::MAX, ctx) {
                                    path.retire(ctx)?;
                                    return Err(error.into());
                                }
                                let index = paths.value.len();
                                crate::Transaction::<A, O>::push_mutation(&mut paths, path, ctx)?;
                                if entry.kind == SourceNodeKind::Directory {
                                    directories.ensure_for_push(usize::MAX, ctx)?;
                                    directories.value.push(index);
                                }
                            }
                            Ok::<_, LazyWorkspaceError>(())
                        })();
                        drop(page);
                        crate::kernel::retire_allocation(guard, bytes, ctx)?;
                        projected?;
                        if cursor.is_none() {
                            return Ok::<_, LazyWorkspaceError>(());
                        }
                    }
                }
                .await;
                if let Some(cursor) = cursor {
                    cursor.retire(ctx)?;
                }
                directory.retire(ctx)?;
                listed?;
            }
            Ok::<_, LazyWorkspaceError>(())
        }
        .await;
        checkout.retire(ctx)?;
        directories.retire(ctx)?;
        if let Err(error) = result {
            paths.retire(ctx)?;
            return Err(error);
        }
        Ok((paths, used))
    }
}
