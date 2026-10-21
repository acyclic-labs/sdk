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
        let (paths, _) = self.enumerate_paths(root, cut, &state, ctx).await?;
        let start = crate::Generation {
            workspace: self.workspace.clone(),
            id: cut.authored_generation,
            publication_closure: None,
        };
        self.exactify_into(root, cut, paths, start, permit, ctx)
            .await
    }

    /// Materializes `paths` of an immutable cut of this view, with their
    /// ancestor directories, into a plain fork of the cut's authored
    /// generation, reading each path at the source version `witness` -- a
    /// view forked from `cut` -- first observed it.
    ///
    /// Unlike [`Self::exactify_snapshot`], the cut need not still be this
    /// view's current state: the fork carries no lazy binding, so it never
    /// claims the cut as a live view. Each promoted entry binds the same
    /// source-derived identity that materializing any view of the cut binds,
    /// which is what a merge base of two such views needs.
    ///
    /// A cut leaves unobserved files live, so the source as it is now is not
    /// the cut's content: an in-place edit after the fork would read as the
    /// base and erase itself from a merge. Only the witness's observation
    /// pins what the forked side started from. A path it never observed, or
    /// whose source has since changed version, has no recoverable base, and
    /// the result is `None`. Reads stay pinned to the observed version, so a
    /// change racing this call fails it rather than supplying a base.
    ///
    /// The scratch fork is named by the pinned cut and path set, so a retry
    /// resumes it and a different observation never reuses it.
    pub async fn exactify_cut(
        &self,
        cut: LazySnapshotRef,
        paths: &[String],
        witness: &Self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<crate::Generation<A, O>>, LazyWorkspaceError> {
        self.validate_snapshot(cut, ctx).await?;
        let witnessed = witness.state(ctx).await?;
        if witnessed.fork_origin.map(|origin| origin.snapshot) != Some(cut) {
            return Ok(None);
        }
        let live = self.source.reference();
        let mut overlay = cut.overlay;
        for path in paths {
            let Some(LazyOverlayChange::Observe { source, node }) =
                witness.overlay_fact(witnessed.overlay, path, ctx).await?
            else {
                return Ok(None);
            };
            if node.kind != SourceNodeKind::RegularFile
                || self
                    .source_lookup(live, path, ctx)
                    .await?
                    .is_none_or(|current| {
                        current.version != node.version
                            || current.file_identity != node.file_identity
                    })
            {
                return Ok(None);
            }
            overlay = self
                .insert_overlay(
                    overlay,
                    path,
                    LazyOverlayChange::Observe { source, node },
                    ctx,
                )
                .await?;
        }
        let pinned = LazySnapshotRef::new(
            cut.workspace_id,
            cut.authored_generation,
            cut.source,
            overlay,
            cut.shadows,
        );
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-lazy-cut-base-v1\0");
        hash.update(&pinned.id.into_bytes());
        for path in paths {
            hash.update(&(path.len() as u64).to_le_bytes());
            hash.update(path.as_bytes());
        }
        let hash = hash.finalize();
        let mut key = [0; 16];
        key.copy_from_slice(&hash.as_bytes()[..16]);
        let key = IdempotencyKey::from_bytes(key);
        let destination = format!("lazy-base-{}", hex::encode(&hash.as_bytes()[..12]));
        let mut selected = std::collections::BTreeSet::new();
        for path in paths {
            selected.insert(path.as_str());
            let mut child = path.as_str();
            while let Some((parent, _)) = child.rsplit_once('/') {
                if parent.is_empty() || !selected.insert(parent) {
                    break;
                }
                child = parent;
            }
        }
        let mut owned = Owned::allocate(selected.len(), ctx)?;
        for path in &selected {
            let path = self.canonical_path(path, ctx)?;
            crate::Transaction::<A, O>::push_mutation(&mut owned, path, ctx)?;
        }
        let generation = crate::Generation {
            workspace: self.workspace.clone(),
            id: cut.authored_generation,
            publication_closure: None,
        };
        let fork = accounted!(ctx, |work, cancel| self
            .workspace
            .fork::<_, _, WorkspaceError>(
                &destination,
                ForkOptions::from_generation(generation, key),
                crate::PublicationPermit::Unrestricted,
                work,
                cancel,
                (),
            ))?;
        // A retry resumes from the fork's head: promoted paths read as
        // authored and the remaining batches commit under their own keys.
        let head = accounted!(ctx, |work, cancel| fork.head(work, cancel))?;
        self.exactify_into(
            "/",
            pinned,
            owned,
            head,
            crate::PublicationPermit::Unrestricted,
            ctx,
        )
        .await
        .map(Some)
    }

    /// Promotes `paths` of the cut, under `root`'s batch keys, on top of
    /// `start`, a generation holding the cut's authored entries.
    async fn exactify_into(
        &self,
        root: &str,
        cut: LazySnapshotRef,
        mut paths: Owned<Vec<String>>,
        start: crate::Generation<A, O>,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let state = LazyWorkspaceState::new(cut.workspace_id, cut.source, cut.overlay, cut.shadows);
        let workspace = &start.workspace;
        paths
            .value
            .sort_unstable_by(|a, b| exactify_path_order(a, b));
        let result = async {
            let limits = self.workspace.limits();
            let maximum = (limits.maximum_checkout_dependencies as usize
                / (limits.maximum_path_depth as usize + 4))
                .min(limits.maximum_mutations_per_batch as usize / 4)
                .max(1);
            let mut generation = start.id;
            for (batch, chunk) in paths.value.chunks(maximum).enumerate() {
                ctx.check()?;
                let checkout = workspace
                    .checkout_accounted(
                        crate::model::GenerationSelector::Exact(generation),
                        crate::model::CheckoutMode::tracking_transaction(),
                        ctx,
                        cancellation(ctx),
                    )
                    .await?;
                let mut transaction = crate::Transaction::from_checkout(
                    workspace,
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
                if !transaction.checkout.has_pending_mutations() {
                    continue;
                }
                generation = match self.commit(&mut transaction, permit, ctx).await? {
                    TransactionCommit::Committed(generation)
                    | TransactionCommit::AlreadyCommitted(generation) => generation.id(),
                    _ => return Err(LazyWorkspaceError::Concurrent),
                };
            }
            accounted!(ctx, |work, cancel| workspace
                .generation(generation, work, cancel))
        }
        .await;
        paths.retire(ctx)?;
        result
    }

    /// Materializes an immutable cut in a deterministic scratch fork.
    pub async fn exactify_snapshot(
        &self,
        cut: LazySnapshotRef,
        destination: &str,
        key: IdempotencyKey,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let (child, _) = self
            .fork_snapshot(
                destination,
                cut,
                key,
                crate::PublicationPermit::Unrestricted,
                ctx,
            )
            .await?;
        child
            .exactify("/", crate::PublicationPermit::Unrestricted, ctx)
            .await
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
                    paths.value.first().ok_or(LazyWorkspaceError::Concurrent)?,
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
            while let Some(&index) = directories.value.get(next) {
                ctx.check()?;
                next += 1;
                let directory = self.namespace_path(
                    paths
                        .value
                        .get(index)
                        .ok_or(LazyWorkspaceError::Concurrent)?,
                    ctx,
                )?;
                let mut cursor = None;
                let listed = async {
                    loop {
                        let (mut page, source, guard) = self
                            .list_directory_at(
                                state,
                                &mut checkout.value,
                                paths
                                    .value
                                    .get(index)
                                    .ok_or(LazyWorkspaceError::Concurrent)?,
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
