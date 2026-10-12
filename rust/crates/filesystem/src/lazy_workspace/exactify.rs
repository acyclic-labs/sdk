use super::*;


impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{


    /// Captures every currently visible path into an exact authored generation.
    ///
    /// Enumeration completes before promotion begins so authored overlay
    /// changes cannot invalidate source cursors. Budget exhaustion and
    /// cancellation are explicit errors; callers must never present a partial
    /// capture as exact. A retry safely continues from already promoted paths.
    pub async fn exactify(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        self.exactify_with_permit(budget, cancellation, crate::PublicationPermit::Unrestricted)
            .await
    }

    /// Exactifies under the supplied operation-window or reservation permit.
    pub async fn exactify_with_permit(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        permit: crate::PublicationPermit,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        self.exactify_selected_with_permit("/", budget, cancellation, permit)
            .await
    }

    /// Captures one visible subtree into authored state without visiting its
    /// siblings. The selected directory and every descendant become exact;
    /// unresolved paths outside it retain their normal lazy semantics.
    pub async fn exactify_subtree(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        self.exactify_subtree_with_permit(
            path,
            budget,
            cancellation,
            crate::PublicationPermit::Unrestricted,
        )
        .await
    }

    /// Captures one visible subtree into authored state without visiting its
    /// siblings. The selected directory and every descendant become exact;
    /// unresolved paths outside it retain their normal lazy semantics.
    pub async fn exactify_subtree_with_permit(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        permit: crate::PublicationPermit,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        let root = self.canonical_path(path)?;
        let selected = self.lookup(&root).await?;
        if !matches!(
            selected,
            LazyLookup::Authored {
                stat: crate::WorkspaceStat {
                    kind: FileKind::Directory,
                    ..
                },
                ..
            } | LazyLookup::Source(SourceNode {
                kind: SourceNodeKind::Directory,
                ..
            }) | LazyLookup::Shadow {
                record: FileRecord {
                    kind: FileKind::Directory,
                    ..
                },
                ..
            }
        ) {
            return Err(LazyWorkspaceError::NotDirectory);
        }
        self.exactify_selected_with_permit(&root, budget, cancellation, permit)
            .await
    }

    #[allow(
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "exactifies the selected subtree as one ordered traversal that batches mutations under one permit"
    )]
    async fn exactify_selected_with_permit(
        &self,
        root: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        permit: crate::PublicationPermit,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        let state = self.state_measured(budget, cancellation).await?;
        let mut work = state.work;
        let head = self
            .workspace
            .head_measured(remaining_work(work, budget)?, cancellation)
            .await
            .map_err(workspace_error)?;
        work = account_work(work, head.work, budget)?;
        let mut pinned_generation = head.value;
        let capture = LazySnapshotRef::new(
            self.workspace.id(),
            pinned_generation.id(),
            state.value.source,
            state.value.overlay,
            state.value.shadows,
        )
        .id;
        let mut retained_bytes = 0_u64;
        let mut directories = Vec::new();
        let mut paths = Vec::new();
        reserve_exactify_path_slot(&mut directories, &mut retained_bytes, &mut work, budget)?;
        let root_key = root;
        let root = root.to_owned();
        retain_exactify_string(&root, &mut retained_bytes, &mut work, budget)?;
        directories.push(root.clone());
        if root != "/" {
            reserve_exactify_path_slot(&mut paths, &mut retained_bytes, &mut work, budget)?;
            retain_exactify_string(&root, &mut retained_bytes, &mut work, budget)?;
            paths.push(root);
        }
        let mut next_directory = 0_usize;
        while next_directory < directories.len() {
            cancellation
                .check()
                .map_err(|_| LazyWorkspaceError::Cancelled)?;
            let directory = directories
                .get_mut(next_directory)
                .map(std::mem::take)
                .ok_or(LazyWorkspaceError::Concurrent)?;
            next_directory += 1;
            let mut cursor = None;
            loop {
                cancellation
                    .check()
                    .map_err(|_| LazyWorkspaceError::Cancelled)?;
                let receipt = self
                    .list_directory_measured(
                        &directory,
                        cursor,
                        1_024,
                        remaining_work(work, budget)?,
                        cancellation,
                        None,
                        None,
                    )
                    .await
                    .map_err(|error| exactify_error("enumerate", &directory, error))?;
                work = account_nested_with_live_memory(work, receipt.work, retained_bytes, budget)?;
                let page = receipt.value;
                for entry in page.entries {
                    let path = logical_child_path(&directory, &entry.name)
                        .ok_or(LazyWorkspaceError::UnsupportedNode)?;
                    work.items_examined = work
                        .items_examined
                        .checked_add(1)
                        .ok_or_else(counter_overflow)?;
                    work.verify(budget)?;
                    if entry.kind == SourceNodeKind::Directory {
                        reserve_exactify_path_slot(
                            &mut directories,
                            &mut retained_bytes,
                            &mut work,
                            budget,
                        )?;
                        let child_directory = path.clone();
                        retain_exactify_string(
                            &child_directory,
                            &mut retained_bytes,
                            &mut work,
                            budget,
                        )?;
                        directories.push(child_directory);
                    }
                    reserve_exactify_path_slot(&mut paths, &mut retained_bytes, &mut work, budget)?;
                    retain_exactify_string(&path, &mut retained_bytes, &mut work, budget)?;
                    paths.push(path);
                }
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
            retained_bytes = retained_bytes
                .checked_sub(u64::try_from(directory.capacity()).unwrap_or(u64::MAX))
                .ok_or_else(counter_overflow)?;
        }
        let directory_vector_bytes = exactify_path_vector_bytes(&directories)?;
        retained_bytes = retained_bytes
            .checked_sub(directory_vector_bytes)
            .ok_or_else(counter_overflow)?;
        drop(directories);
        let path_vector_bytes = exactify_path_vector_bytes(&paths)?;
        paths.sort_unstable_by(|left, right| exactify_path_order(left, right));
        let limits = self.workspace.limits();
        // A path can add an ancestor proof for every component. Keep each
        // unpublished candidate well inside the volume dependency ceiling.
        let dependency_paths = usize::try_from(limits.maximum_checkout_dependencies)
            .unwrap_or(usize::MAX)
            / (usize::from(limits.maximum_path_depth) + 4);
        let mutation_paths =
            usize::try_from(limits.maximum_mutations_per_batch).unwrap_or(usize::MAX) / 4;
        let maximum_batch_paths = dependency_paths.min(mutation_paths).max(1);
        let mut seen_sources: Vec<(FileId, String)> = Vec::new();
        seen_sources
            .try_reserve_exact(maximum_batch_paths.min(paths.len()))
            .map_err(|_| LazyWorkspaceError::Work("exactify batch allocation failed".to_owned()))?;
        let seen_vector_bytes = u64::try_from(
            seen_sources
                .capacity()
                .checked_mul(std::mem::size_of::<(FileId, String)>())
                .ok_or_else(counter_overflow)?,
        )
        .map_err(|_| counter_overflow())?;
        retained_bytes = retained_bytes
            .checked_add(seen_vector_bytes)
            .ok_or_else(counter_overflow)?;
        work.peak_allocation_bytes = work.peak_allocation_bytes.max(retained_bytes);
        work.verify(budget)?;
        let mut transaction = None;
        let mut batch_paths = 0_usize;
        let mut batch_dirty = false;
        let mut batch_index = 0_u64;
        let mut pending_sources = Vec::new();
        let mut pending_source_bytes = 0_u64;
        let mut authored_batch = Vec::new();
        for index in 0..paths.len() {
            cancellation
                .check()
                .map_err(|_| LazyWorkspaceError::Cancelled)?;
            if batch_paths == maximum_batch_paths {
                if batch_dirty {
                    work = self
                        .flush_exactify_source_batch(
                            transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                            &mut pending_sources,
                            &mut pending_source_bytes,
                            &mut retained_bytes,
                            work,
                            budget,
                            cancellation,
                        )
                        .await?;
                    let current = transaction.take().ok_or(LazyWorkspaceError::Concurrent)?;
                    let committed = self
                        .commit_exactify_batch(
                            current,
                            permit,
                            remaining_work(work, budget)?,
                            cancellation,
                        )
                        .await?;
                    work = account_nested_with_live_memory(
                        work,
                        committed.work,
                        retained_bytes,
                        budget,
                    )?;
                    pinned_generation = committed.value;
                }
                transaction = None;
                batch_dirty = false;
                batch_paths = 0;
                batch_index = batch_index.checked_add(1).ok_or_else(counter_overflow)?;
                for (_, retained) in seen_sources.drain(..) {
                    retained_bytes = retained_bytes
                        .checked_sub(u64::try_from(retained.capacity()).unwrap_or(u64::MAX))
                        .ok_or_else(counter_overflow)?;
                }
            }
            if batch_paths == 0 {
                let end = index.saturating_add(maximum_batch_paths).min(paths.len());
                let namespace_paths = paths
                    .get(index..end)
                    .ok_or(LazyWorkspaceError::Concurrent)?
                    .iter()
                    .map(|path| self.namespace_path(path))
                    .collect::<Result<Vec<_>, _>>()?;
                let found = pinned_generation
                    .lookup_paths(
                        &namespace_paths,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| LazyWorkspaceError::Workspace(failure.error.to_string()))?;
                work = account_nested_with_live_memory(work, found.work, retained_bytes, budget)?;
                authored_batch = found
                    .value
                    .into_iter()
                    .map(|record| record.is_some())
                    .collect();
            }
            let authored_absent = !authored_batch
                .get(batch_paths)
                .copied()
                .ok_or(LazyWorkspaceError::Concurrent)?;
            let path = std::mem::take(paths.get_mut(index).ok_or(LazyWorkspaceError::Concurrent)?);
            let receipt = if authored_absent {
                self.inspect_snapshot_after_authored_absence(
                    &path,
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await
            } else {
                self.inspect_snapshot_measured(&path, remaining_work(work, budget)?, cancellation)
                    .await
            }
            .map_err(|error| exactify_error("inspect", &path, error))?;
            work = account_nested_with_live_memory(work, receipt.work, retained_bytes, budget)?;
            let lookup = receipt.value;
            if matches!(lookup.lookup, LazyLookup::Source(_)) {
                work.materializations = work
                    .materializations
                    .checked_add(1)
                    .ok_or_else(counter_overflow)?;
                work.verify(budget)?;
            }
            if transaction.is_none() {
                let begun = self
                    .workspace
                    .begin_pinned_transaction_measured(
                        &pinned_generation,
                        exactify_batch_key(capture, root_key, batch_index),
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(workspace_error)?;
                work = account_nested_with_live_memory(work, begun.work, retained_bytes, budget)?;
                transaction = Some(begun.value);
                if let Some(parent) = parent_path(root_key) {
                    let prepared = transaction
                        .as_mut()
                        .ok_or(LazyWorkspaceError::Concurrent)?
                        .create_dir_all_measured(
                            parent,
                            remaining_work(work, budget)?,
                            cancellation,
                        )
                        .await
                        .map_err(workspace_error)?;
                    work = account_nested_with_live_memory(work, prepared, retained_bytes, budget)?;
                }
            }
            let source_id = match &lookup.lookup {
                LazyLookup::Source(node)
                    if node.kind == SourceNodeKind::RegularFile && node.link_count != Some(1) =>
                {
                    Some(self.source_file_id(node))
                }
                _ => None,
            };
            if let Some((_, alias)) =
                source_id.and_then(|id| seen_sources.iter().find(|(seen, _)| *seen == id))
            {
                work = self
                    .flush_exactify_source_batch(
                        transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                        &mut pending_sources,
                        &mut pending_source_bytes,
                        &mut retained_bytes,
                        work,
                        budget,
                        cancellation,
                    )
                    .await?;
                let linked = transaction
                    .as_mut()
                    .ok_or(LazyWorkspaceError::Concurrent)?
                    .hard_link_measured(alias, &path, remaining_work(work, budget)?, cancellation)
                    .await
                    .map_err(workspace_error)
                    .map_err(|error| exactify_error("link", &path, error))?;
                work = account_nested_with_live_memory(work, linked, retained_bytes, budget)?;
                batch_dirty = true;
                retained_bytes = retained_bytes
                    .checked_sub(u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                    .ok_or_else(counter_overflow)?;
            } else if let LazyLookup::Source(node) = &lookup.lookup
                && node.kind == SourceNodeKind::RegularFile
            {
                let pinned = PinnedSourceNode {
                    source: lookup.source.ok_or(LazyWorkspaceError::Concurrent)?,
                    node: *node,
                };
                let staged = self
                    .stage_regular_source_content(
                        transaction.as_ref().ok_or(LazyWorkspaceError::Concurrent)?,
                        &path,
                        &pinned,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(|error| exactify_error("stage", &path, error))?;
                work = account_nested_with_live_memory(work, staged.work, retained_bytes, budget)?;
                let namespace_path = self.namespace_path(&path)?;
                queue_exactify_mutation(
                    &mut pending_sources,
                    AuthoredMutation::CreateFileFromContent {
                        path: namespace_path,
                        content: staged.value,
                        metadata: source_file_metadata(node.metadata),
                        file_id: Some(self.source_file_id(node)),
                    },
                    &mut pending_source_bytes,
                    &mut retained_bytes,
                    &mut work,
                    budget,
                )?;
                batch_dirty = true;
                if let Some(id) = source_id {
                    seen_sources.push((id, path));
                } else {
                    retained_bytes = retained_bytes
                        .checked_sub(u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                        .ok_or_else(counter_overflow)?;
                }
            } else if let LazyLookup::Source(node) = &lookup.lookup
                && node.kind == SourceNodeKind::Directory
            {
                queue_exactify_mutation(
                    &mut pending_sources,
                    AuthoredMutation::CreateDirectory {
                        path: self.namespace_path(&path)?,
                        metadata: source_file_metadata(node.metadata),
                    },
                    &mut pending_source_bytes,
                    &mut retained_bytes,
                    &mut work,
                    budget,
                )?;
                batch_dirty = true;
                retained_bytes = retained_bytes
                    .checked_sub(u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                    .ok_or_else(counter_overflow)?;
            } else {
                work = self
                    .flush_exactify_source_batch(
                        transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                        &mut pending_sources,
                        &mut pending_source_bytes,
                        &mut retained_bytes,
                        work,
                        budget,
                        cancellation,
                    )
                    .await?;
                let applied = self
                    .apply_resolved_promotion_measured(
                        transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                        &path,
                        lookup,
                        budget.source_bytes_read,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(|error| exactify_error("promote", &path, error))?;
                work = account_nested_with_live_memory(work, applied.work, retained_bytes, budget)?;
                batch_dirty |= applied.value;
                if let Some(id) = source_id.filter(|_| applied.value) {
                    seen_sources.push((id, path));
                } else {
                    retained_bytes = retained_bytes
                        .checked_sub(u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                        .ok_or_else(counter_overflow)?;
                }
            }
            batch_paths += 1;
        }
        if batch_dirty {
            work = self
                .flush_exactify_source_batch(
                    transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                    &mut pending_sources,
                    &mut pending_source_bytes,
                    &mut retained_bytes,
                    work,
                    budget,
                    cancellation,
                )
                .await?;
            let current = transaction.take().ok_or(LazyWorkspaceError::Concurrent)?;
            let committed = self
                .commit_exactify_batch(current, permit, remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_nested_with_live_memory(work, committed.work, retained_bytes, budget)?;
            pinned_generation = committed.value;
        }
        for (_, retained) in seen_sources.drain(..) {
            retained_bytes = retained_bytes
                .checked_sub(u64::try_from(retained.capacity()).unwrap_or(u64::MAX))
                .ok_or_else(counter_overflow)?;
        }
        retained_bytes = retained_bytes
            .checked_sub(seen_vector_bytes)
            .ok_or_else(counter_overflow)?;
        drop(seen_sources);
        retained_bytes = retained_bytes
            .checked_sub(path_vector_bytes)
            .ok_or_else(counter_overflow)?;
        debug_assert_eq!(retained_bytes, 0);
        let head = self
            .workspace
            .head_measured(remaining_work(work, budget)?, cancellation)
            .await
            .map_err(workspace_error)
            .map_err(|error| exactify_error("load head", "/", error))?;
        work = account_work(work, head.work, budget)?;
        if head.value.id() != pinned_generation.id() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(OperationReceipt {
            value: head.value,
            work,
        })
    }
}
