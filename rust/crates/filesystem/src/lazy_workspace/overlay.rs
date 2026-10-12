use super::*;


impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{


    async fn append_overlay(
        &self,
        path: String,
        change: LazyOverlayChange,
    ) -> Result<(), LazyWorkspaceError> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self.state().await?;
            let overlay_id = self
                .insert_overlay(state.overlay, path.clone(), change.clone())
                .await?;
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay: overlay_id,
                ..state.clone()
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(self.workspace.id(), state.revision, replacement)
                .await
                .map_err(store_error)?
            {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    fn insert_overlay<'a>(
        &'a self,
        root: LazyOverlayId,
        path: String,
        change: LazyOverlayChange,
    ) -> LazyFuture<'a, Result<LazyOverlayId, LazyWorkspaceError>> {
        Box::pin(async move {
            insert_immutable_treap(&OverlayTreap(&self.store), root, path, change).await
        })
    }

    async fn overlay_fact(
        &self,
        root: LazyOverlayId,
        path: &str,
    ) -> Result<Option<LazyOverlayChange>, LazyWorkspaceError> {
        immutable_treap_value(&OverlayTreap(&self.store), root, &path.to_owned()).await
    }

    async fn overlay_fact_measured(
        &self,
        mut root: LazyOverlayId,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyOverlayChange>>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        loop {
            let receipt = self
                .store
                .load_lazy_overlay_measured(root, remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, receipt.work, budget)?;
            let Some(node) = receipt.value else {
                return Ok(OperationReceipt { value: None, work });
            };
            let LazyOverlay::Node {
                path: node_path,
                change,
                left,
                right,
                ..
            } = node
            else {
                return Ok(OperationReceipt { value: None, work });
            };
            match path.cmp(node_path.as_str()) {
                std::cmp::Ordering::Equal => {
                    return Ok(OperationReceipt {
                        value: Some(change),
                        work,
                    });
                }
                std::cmp::Ordering::Less => root = left,
                std::cmp::Ordering::Greater => root = right,
            }
        }
    }

    /// Whether the overlay records a fact for any path strictly beneath
    /// `directory`: such paths are exactly those after `directory/` in the
    /// overlay's order that start with it, so one descent to the first of
    /// them decides.
    async fn overlay_has_beneath(
        &self,
        mut root: LazyOverlayId,
        directory: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        // The canonical copy and the prefix built from it are this search's
        // own allocations, admitted before they are made.
        let bytes = u64::try_from(directory.len())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let mut work = WorkCounters {
            allocation_operations: 2,
            peak_allocation_bytes: bytes.saturating_mul(2),
            bytes_copied: bytes.saturating_mul(2),
            ..WorkCounters::default()
        };
        work.verify(budget)?;
        let directory = self.canonical_path(directory)?;
        let prefix = if directory.ends_with('/') {
            directory
        } else {
            format!("{directory}/")
        };
        let mut first_after: Option<String> = None;
        loop {
            let receipt = self
                .store
                .load_lazy_overlay_measured(root, remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, receipt.work, budget)?;
            let Some(LazyOverlay::Node {
                path, left, right, ..
            }) = receipt.value
            else {
                break;
            };
            if path.as_str() >= prefix.as_str() {
                first_after = Some(path);
                root = left;
            } else {
                root = right;
            }
        }
        Ok(OperationReceipt {
            value: first_after.is_some_and(|path| path.starts_with(&prefix)),
            work,
        })
    }

    async fn shadow_record_measured(
        &self,
        mut root: LazyShadowId,
        file_id: FileId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<(FileRecord, crate::WorkspaceMetadata)>>, LazyWorkspaceError>
    {
        let mut work = WorkCounters::default();
        loop {
            let receipt = self
                .store
                .load_lazy_shadow_measured(root, remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, receipt.work, budget)?;
            let Some(node) = receipt.value else {
                return Ok(OperationReceipt { value: None, work });
            };
            let LazyShadow::Node {
                file_id: node_file_id,
                record,
                metadata,
                left,
                right,
                ..
            } = node
            else {
                return Ok(OperationReceipt { value: None, work });
            };
            match file_id.cmp(&node_file_id) {
                std::cmp::Ordering::Equal => {
                    let record = crate::kernel::decode_file_record(&record)
                        .map_err(|error| LazyWorkspaceError::Store(error.to_string()))?;
                    return Ok(OperationReceipt {
                        value: Some((record, *metadata)),
                        work,
                    });
                }
                std::cmp::Ordering::Less => root = left,
                std::cmp::Ordering::Greater => root = right,
            }
        }
    }

    fn insert_shadow<'a>(
        &'a self,
        root: LazyShadowId,
        file_id: FileId,
        record: FileRecord,
        metadata: crate::WorkspaceMetadata,
    ) -> LazyFuture<'a, Result<LazyShadowId, LazyWorkspaceError>> {
        Box::pin(async move {
            // A shadow outlives the checkout whose objects it names, which
            // publication need not flush: make them durable first, so no
            // durable shadow ever names content a crash could lose.
            // Held until the shadow is written, so no collection sweeps what
            // it names in between.
            let _hold = self
                .workspace
                .make_records_durable(std::slice::from_ref(&record))
                .await
                .map_err(workspace_error)?;
            insert_immutable_treap(
                &ShadowTreap(&self.store),
                root,
                file_id,
                ShadowValue {
                    record: crate::kernel::encode_file_record(record),
                    metadata: Box::new(metadata),
                },
            )
            .await
        })
    }

    async fn tombstoned_measured(
        &self,
        overlay_id: LazyOverlayId,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        let path = self.canonical_path(path)?;
        let path_bytes = u64::try_from(path.capacity()).map_err(|_| counter_overflow())?;
        let mut work = WorkCounters {
            bytes_copied: u64::try_from(path.len()).map_err(|_| counter_overflow())?,
            allocation_operations: u64::from(path_bytes != 0),
            peak_allocation_bytes: path_bytes,
            ..WorkCounters::default()
        };
        work.verify(budget)?;
        let ancestor_request = path.len();
        if ancestor_request != 0 {
            let requested = u64::try_from(ancestor_request).map_err(|_| counter_overflow())?;
            let mut attempted = work.checked_add(WorkCounters {
                bytes_copied: requested,
                allocation_operations: 1,
                ..WorkCounters::default()
            })?;
            attempted.peak_allocation_bytes = attempted.peak_allocation_bytes.max(
                path_bytes
                    .checked_add(requested)
                    .ok_or_else(counter_overflow)?,
            );
            attempted.verify(budget)?;
            work = attempted;
        }
        let root = self
            .overlay_fact_measured(overlay_id, "/", remaining_work(work, budget)?, cancellation)
            .await?;
        work = account_work(work, root.work, budget)?;
        if matches!(root.value, Some(LazyOverlayChange::Tombstone { .. })) {
            return Ok(OperationReceipt { value: true, work });
        }
        let mut ancestor = String::new();
        ancestor
            .try_reserve_exact(ancestor_request)
            .map_err(|_| LazyWorkspaceError::Work("tombstone path allocation failed".to_owned()))?;
        for component in path.trim_start_matches('/').split('/') {
            if component.is_empty() {
                continue;
            }
            ancestor.push('/');
            ancestor.push_str(component);
            let fact = self
                .overlay_fact_measured(
                    overlay_id,
                    &ancestor,
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await?;
            work = account_work(work, fact.work, budget)?;
            if matches!(fact.value, Some(LazyOverlayChange::Tombstone { .. })) {
                return Ok(OperationReceipt { value: true, work });
            }
        }
        Ok(OperationReceipt { value: false, work })
    }
}
