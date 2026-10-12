use super::*;


impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{


    async fn commit_promotion(
        &self,
        mut transaction: crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        let receipt = transaction
            .commit_with_permit_measured(permit, remaining_work(work, budget)?, cancellation)
            .await
            .map_err(workspace_error)?;
        work = account_work(work, receipt.work, budget)?;
        Ok(OperationReceipt {
            value: receipt.value,
            work,
        })
    }

    async fn existing_promotion(
        &self,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        let head = self
            .workspace
            .head_measured(remaining_work(work, budget)?, cancellation)
            .await
            .map_err(workspace_error)?;
        work = account_work(work, head.work, budget)?;
        Ok(OperationReceipt {
            value: TransactionCommit::AlreadyCommitted(head.value),
            work,
        })
    }

    async fn commit_exactify_batch(
        &self,
        transaction: crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        let receipt = self
            .commit_promotion(
                transaction,
                permit,
                WorkCounters::default(),
                budget,
                cancellation,
            )
            .await?;
        match receipt.value {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => Ok(OperationReceipt {
                value: generation,
                work: receipt.work,
            }),
            TransactionCommit::Conflict { .. }
            | TransactionCommit::Fenced
            | TransactionCommit::IdempotencyConflict => Err(LazyWorkspaceError::Concurrent),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the pending batch, its byte accounting, work, budget, and cancellation are all mutated by one flush"
    )]
    async fn flush_exactify_source_batch(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        pending: &mut Vec<AuthoredMutation>,
        pending_bytes: &mut u64,
        retained_bytes: &mut u64,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, LazyWorkspaceError> {
        if pending.is_empty() {
            return Ok(work);
        }
        let operations = std::mem::take(pending);
        let applied = transaction
            .apply_authored_bulk_measured(operations, remaining_work(work, budget)?, cancellation)
            .await
            .map_err(workspace_error)?;
        let work = account_nested_with_live_memory(work, applied, *retained_bytes, budget)?;
        *retained_bytes = retained_bytes
            .checked_sub(*pending_bytes)
            .ok_or_else(counter_overflow)?;
        *pending_bytes = 0;
        Ok(work)
    }

    /// Creates or replaces one complete authored file.
    ///
    /// Authored state always wins over the source overlay, so no companion
    /// metadata mutation is needed. An inherited tombstone deliberately stays
    /// attached to the underlying source fact: deleting the authored
    /// replacement later must not resurrect the source file.
    pub async fn write(
        &self,
        path: &str,
        bytes: Bytes,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let path = self.canonical_path(path)?;
        let source_identity = match self.lookup(&path).await {
            Ok(LazyLookup::Source(node)) => Some(self.source_file_id(&node)),
            Ok(LazyLookup::Authored { stat, .. }) => {
                self.promote_exact(&path, stat.file_id, u64::MAX, IdempotencyKey::new())
                    .await?;
                None
            }
            Ok(LazyLookup::Shadow { record, .. }) => {
                self.promote_exact(&path, record.file_id, u64::MAX, IdempotencyKey::new())
                    .await?;
                None
            }
            Err(LazyWorkspaceError::NotFound) => None,
            Err(error) => return Err(error),
        };
        let mut transaction = self
            .workspace
            .begin_transaction(IdempotencyKey::new())
            .await
            .map_err(workspace_error)?;
        if let Some(parent) = parent_path(&path) {
            transaction
                .create_dir_all(parent)
                .await
                .map_err(workspace_error)?;
        }
        transaction
            .write(&path, bytes)
            .await
            .map_err(workspace_error)?;
        if let Some(file_id) = source_identity {
            transaction
                .preserve_file_identity(&path, file_id)
                .await
                .map_err(workspace_error)?;
        }
        transaction.commit().await.map_err(workspace_error)
    }

    /// Prepares mounted whiteouts and identity updates under the checkout's
    /// exact publication identity. The caller must hold the mount mutation gate
    /// through checkout seal and then call `recover_pending_remove`.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn prepare_mount_removals(
        &self,
        paths: &[String],
        identity_records: &[(FileRecord, FileMetadata)],
        idempotency_key: IdempotencyKey,
    ) -> Result<(), LazyWorkspaceError> {
        if paths.is_empty() && identity_records.is_empty() {
            return Ok(());
        }
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self.state().await?;
            let mut overlay = state.overlay;
            let mut shadows = state.shadows;
            for path in paths {
                let path = self.canonical_path(path)?;
                let removed = self.removed_source_version(&state, &path).await?;
                overlay = self
                    .insert_overlay(overlay, path, LazyOverlayChange::Tombstone { removed })
                    .await?;
            }
            for (record, metadata) in identity_records {
                shadows = self
                    .insert_shadow(
                        shadows,
                        record.file_id,
                        *record,
                        crate::WorkspaceMetadata::from_engine(*metadata),
                    )
                    .await?;
            }
            let prepared = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay,
                shadows,
                pending_remove: Some(PendingLazyRemove {
                    prior_overlay: state.overlay,
                    prior_shadows: state.shadows,
                    prepared_overlay: overlay,
                    kind: PendingLazyRemoveKind::Mounted { idempotency_key },
                }),
                ..state.clone()
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(self.workspace.id(), state.revision, prepared)
                .await
                .map_err(store_error)?
            {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    /// Reads the durable decision for a prepared mounted overlay without
    /// finalizing it. An ambiguously acknowledged checkout must first retry
    /// its exact operation ID so its private candidate adopts the commit.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn mount_removal_publication(
        &self,
    ) -> Result<Option<bool>, LazyWorkspaceError> {
        let state = self
            .store
            .load_lazy_workspace(self.workspace.id())
            .await
            .map_err(store_error)?
            .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        let Some(PendingLazyRemove {
            kind: PendingLazyRemoveKind::Mounted { idempotency_key },
            ..
        }) = state.pending_remove
        else {
            return Ok(None);
        };
        let committed = self
            .workspace
            .operation_generation(idempotency_key)
            .await
            .map_err(workspace_error)?
            .is_some();
        Ok(Some(committed))
    }

    /// Resolves a prepared mounted overlay from the checkout's durable result.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn finish_mount_removals(&self) -> Result<bool, LazyWorkspaceError> {
        let Some(committed) = self.mount_removal_publication().await? else {
            return Ok(false);
        };
        self.recover_pending_remove().await?;
        Ok(committed)
    }

    /// Source paths this view deleted, sorted, each with the source version
    /// it deleted. No authored generation holds these, so a merge cannot
    /// carry them; its caller applies each to the parent while the source
    /// still holds that version. Hidden paths are not listed.
    ///
    /// # Errors
    ///
    /// Returns store failures, or a work failure past `maximum` tombstones.
    pub async fn source_tombstones(
        &self,
        maximum: usize,
    ) -> Result<Vec<(String, SourceVersion)>, LazyWorkspaceError> {
        let state = self.state().await?;
        let mut pending = vec![state.overlay];
        let mut tombstones = Vec::new();
        while let Some(id) = pending.pop() {
            let Some(LazyOverlay::Node {
                path,
                change,
                left,
                right,
                ..
            }) = self
                .store
                .load_lazy_overlay(id)
                .await
                .map_err(store_error)?
            else {
                continue;
            };
            if let LazyOverlayChange::Tombstone {
                removed: Some(version),
            } = change
            {
                if tombstones.len() == maximum {
                    return Err(LazyWorkspaceError::Work(format!(
                        "more than {maximum} source tombstones"
                    )));
                }
                tombstones.push((path, version));
            }
            pending.push(left);
            pending.push(right);
        }
        tombstones.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(tombstones)
    }

    /// Removes one path without confusing authored deletion with unresolved source state.
    pub async fn remove(&self, path: &str) -> Result<(), LazyWorkspaceError> {
        self.remove_if(path, None).await
    }

    /// Hides a path this view only reads from the source and never owned
    /// (one an ancestor gained after the fork): it reads as absent, but is
    /// no deletion of this view's, so no merge carries it anywhere.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::remove`].
    pub async fn hide_source_path(&self, path: &str) -> Result<(), LazyWorkspaceError> {
        self.remove_recording(path, None, Removal::Hidden).await
    }

    /// The source version the current source holds at `path`, bypassing
    /// this view's own observations; `None` when the source has no node there.
    ///
    /// # Errors
    ///
    /// Returns path, state, or source failures.
    pub async fn current_source_version(
        &self,
        path: &str,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        let path = self.namespace_path(path)?;
        let state = self.state().await?;
        self.source
            .lookup(state.source, &path, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value.map(|node| node.version))
            .or_else(|failure| match failure.error {
                DemandError::Absent | DemandError::NotDirectory => Ok(None),
                error => Err(LazyWorkspaceError::from(error)),
            })
    }

    /// The source version removing `path` deletes: this view's observation
    /// of it, or else the source's current node.
    async fn removed_source_version(
        &self,
        state: &LazyWorkspaceState,
        path: &str,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        match self.overlay_fact(state.overlay, path).await? {
            Some(LazyOverlayChange::Observe { node, .. }) => return Ok(Some(node.version)),
            Some(LazyOverlayChange::Tombstone { removed }) => return Ok(removed),
            None => {}
        }
        self.source
            .lookup(
                state.source,
                &self.namespace_path(path)?,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value.map(|node| node.version))
            .or_else(|failure| match failure.error {
                DemandError::Absent | DemandError::NotDirectory => Ok(None),
                error => Err(LazyWorkspaceError::from(error)),
            })
    }

    /// Removes one path only if it still has the caller's resolved identity.
    pub async fn remove_if(
        &self,
        path: &str,
        expected: Option<FileId>,
    ) -> Result<(), LazyWorkspaceError> {
        self.remove_recording(path, expected, Removal::Deleted)
            .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one removal records the overlay tombstone for every source and authored node kind"
    )]
    async fn remove_recording(
        &self,
        path: &str,
        expected: Option<FileId>,
        removal: Removal,
    ) -> Result<(), LazyWorkspaceError> {
        let path = self.canonical_path(path)?;
        let mut resolved = self.lookup(&path).await?;
        if let LazyLookup::Authored {
            path: authored_path,
            stat,
        } = &resolved
            && authored_path != &path
        {
            self.promote_exact(&path, stat.file_id, u64::MAX, IdempotencyKey::new())
                .await?;
            resolved = self.lookup(&path).await?;
        }
        let actual = self.direct_file_id_for_lookup(&resolved);
        if let Some(expected) = expected
            && actual != expected
        {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        let removal_kind = if matches!(resolved, LazyLookup::Authored { .. }) {
            PendingLazyRemoveKind::Authored {
                idempotency_key: IdempotencyKey::new(),
            }
        } else {
            PendingLazyRemoveKind::SourceOnly
        };
        let mut prepared = None;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self.state().await?;
            let shadows = if let LazyLookup::Authored { stat, .. } = &resolved {
                let record = self
                    .workspace
                    .record_by_id(stat.file_id)
                    .await
                    .map_err(workspace_error)?;
                self.insert_shadow(state.shadows, stat.file_id, record, stat.metadata)
                    .await?
            } else {
                state.shadows
            };
            let removed = match (removal, &resolved) {
                (Removal::Hidden, _) => None,
                (Removal::Deleted, LazyLookup::Source(node)) => Some(node.version),
                (Removal::Deleted, _) => self.removed_source_version(&state, &path).await?,
            };
            let tombstone_overlay = self
                .insert_overlay(
                    state.overlay,
                    path.clone(),
                    LazyOverlayChange::Tombstone { removed },
                )
                .await?;
            let pending_remove = PendingLazyRemove {
                prior_overlay: state.overlay,
                prior_shadows: state.shadows,
                prepared_overlay: tombstone_overlay,
                kind: removal_kind,
            };
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay: tombstone_overlay,
                shadows,
                pending_remove: Some(pending_remove),
                ..state.clone()
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(
                    self.workspace.id(),
                    state.revision,
                    replacement.clone(),
                )
                .await
                .map_err(store_error)?
            {
                prepared = Some(replacement);
                break;
            }
        }
        let prepared = prepared.ok_or(LazyWorkspaceError::Concurrent)?;

        let removal = if let PendingLazyRemoveKind::Authored { idempotency_key } = removal_kind {
            let mut transaction = self
                .workspace
                .begin_transaction(idempotency_key)
                .await
                .map_err(workspace_error)?;
            match transaction.remove_if(&path, actual).await {
                Ok(()) => transaction.commit().await,
                Err(error) => Err(error),
            }
        } else {
            Ok(TransactionCommit::AlreadyCommitted(
                self.workspace.head().await.map_err(workspace_error)?,
            ))
        };
        let prior_overlay = prepared
            .pending_remove
            .as_ref()
            .ok_or(LazyWorkspaceError::Concurrent)?
            .prior_overlay;
        let (overlay, result) = match removal {
            Ok(TransactionCommit::Committed(_))
            | Ok(TransactionCommit::AlreadyCommitted(_))
            | Err(WorkspaceError::NotFound) => (prepared.overlay, Ok(())),
            Ok(TransactionCommit::Conflict { .. }) | Ok(TransactionCommit::Fenced) => {
                (prior_overlay, Err(LazyWorkspaceError::StaleIdentity))
            }
            Ok(TransactionCommit::IdempotencyConflict) => {
                (prior_overlay, Err(LazyWorkspaceError::Concurrent))
            }
            Err(error) => (prior_overlay, Err(workspace_error(error))),
        };
        let replacement = LazyWorkspaceState {
            revision: prepared.revision.saturating_add(1),
            overlay,
            shadows: if result.is_ok() {
                prepared.shadows
            } else {
                prepared
                    .pending_remove
                    .as_ref()
                    .ok_or(LazyWorkspaceError::Concurrent)?
                    .prior_shadows
            },
            pending_remove: None,
            ..prepared.clone()
        };
        if !self
            .store
            .compare_and_swap_lazy_workspace(self.workspace.id(), prepared.revision, replacement)
            .await
            .map_err(store_error)?
        {
            return Err(LazyWorkspaceError::Concurrent);
        }
        result
    }
}
