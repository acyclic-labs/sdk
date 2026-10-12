use super::*;


#[derive(Clone, Copy)]
pub(super) struct ExactificationSource(pub(super) SourceReference);

impl LazySnapshotRef {
    pub(super) fn exactification_source(
        self,
        live_source: SourceReference,
    ) -> Result<ExactificationSource, LazyWorkspaceError> {
        if self.source.identity != live_source.identity {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok(ExactificationSource(live_source))
    }

    pub(super) fn exactification_state(
        self,
        workspace_id: WorkspaceId,
        parent_workspace_id: WorkspaceId,
        source: ExactificationSource,
    ) -> LazyWorkspaceState {
        LazyWorkspaceState {
            schema_version: LAZY_STATE_SCHEMA,
            revision: 1,
            workspace_id,
            parent_workspace_id: Some(parent_workspace_id),
            // Observed overlay records retain their pinned source epochs. This
            // binding is deliberately live for paths unresolved at capture.
            source: source.0,
            overlay: self.overlay,
            shadows: self.shadows,
            pending_remove: None,
        }
    }
}


impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{


    /// Forks in constant-size metadata while retaining the exact overlay snapshot.
    pub async fn fork(
        &self,
        destination: impl AsRef<str>,
        idempotency_key: IdempotencyKey,
    ) -> Result<Self, LazyWorkspaceError> {
        let parent = self.state().await?;
        let generation = self.workspace.head().await.map_err(workspace_error)?;
        let workspace = self
            .workspace
            .fork(
                destination,
                ForkOptions::from_generation(generation, idempotency_key),
            )
            .await
            .map_err(workspace_error)?;
        let state = LazyWorkspaceState {
            schema_version: LAZY_STATE_SCHEMA,
            revision: 1,
            workspace_id: workspace.id(),
            parent_workspace_id: Some(self.workspace.id()),
            source: parent.source,
            overlay: parent.overlay,
            shadows: parent.shadows,
            pending_remove: None,
        };
        if !self
            .store
            .compare_and_swap_lazy_workspace(workspace.id(), 0, state)
            .await
            .map_err(store_error)?
        {
            let existing = self
                .store
                .load_lazy_workspace(workspace.id())
                .await
                .map_err(store_error)?
                .ok_or(LazyWorkspaceError::Concurrent)?;
            if existing.parent_workspace_id != Some(self.workspace.id())
                || existing.source != parent.source
                || existing.overlay != parent.overlay
                || existing.shadows != parent.shadows
            {
                return Err(LazyWorkspaceError::Concurrent);
            }
        }
        Ok(Self {
            workspace,
            source: Arc::clone(&self.source),
            store: self.store.clone(),
        })
    }

    /// Materializes an immutable lazy snapshot in a deterministic scratch fork.
    ///
    /// Previously observed records remain pinned to the snapshot's source epoch;
    /// unresolved paths are observed from the injected live source when first
    /// demanded. The owning workspace is never moved or rewritten.
    pub async fn exactify_snapshot(
        &self,
        snapshot: LazySnapshotRef,
        destination: impl AsRef<str>,
        idempotency_key: IdempotencyKey,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        if snapshot.workspace_id != self.workspace.id() {
            return Err(LazyWorkspaceError::StaleSource);
        }
        let exactification_source = snapshot.exactification_source(self.source.reference())?;
        let overlay = self
            .store
            .load_lazy_overlay_measured(snapshot.overlay, budget, cancellation)
            .await?;
        let mut work = overlay.work;
        let shadow = self
            .store
            .load_lazy_shadow_measured(
                snapshot.shadows,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, shadow.work, budget)?;
        if overlay.value.is_none() || shadow.value.is_none() {
            return Err(LazyWorkspaceError::Store(
                "lazy snapshot index is absent".to_owned(),
            ));
        }
        let generation = self
            .workspace
            .generation_measured(
                snapshot.authored_generation,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await
            .map_err(workspace_error)?;
        work = account_work(work, generation.work, budget)?;
        let workspace = self
            .workspace
            .fork_measured(
                destination,
                ForkOptions::from_generation(generation.value, idempotency_key),
                remaining_work(work, budget)?,
                cancellation,
            )
            .await
            .map_err(workspace_error)?;
        work = account_work(work, workspace.work, budget)?;
        let workspace = workspace.value;
        let state = snapshot.exactification_state(
            workspace.id(),
            self.workspace.id(),
            exactification_source,
        );
        let inserted = self
            .store
            .compare_and_swap_lazy_workspace_measured(
                workspace.id(),
                0,
                state.clone(),
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, inserted.work, budget)?;
        if !inserted.value {
            let existing = self
                .store
                .load_lazy_workspace_measured(
                    workspace.id(),
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await?;
            work = account_work(work, existing.work, budget)?;
            let existing = existing.value.ok_or(LazyWorkspaceError::Concurrent)?;
            if existing.parent_workspace_id != state.parent_workspace_id
                || existing.source != state.source
                || existing.overlay != state.overlay
                || existing.shadows != state.shadows
            {
                return Err(LazyWorkspaceError::Concurrent);
            }
        }
        let exactified = Self {
            workspace,
            source: Arc::clone(&self.source),
            store: self.store.clone(),
        }
        .exactify(remaining_work(work, budget)?, cancellation)
        .await?;
        work = account_work(work, exactified.work, budget)?;
        Ok(OperationReceipt {
            value: exactified.value,
            work,
        })
    }

    /// Advances this binding to the provider's current invalidation epoch
    /// without enumerating the source. Prior overlay roots remain immutable;
    /// observations are refreshed on demand in the new epoch while authored
    /// state and tombstones remain shared.
    pub async fn rebind_source(&self) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        let source = self.source.reference();
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self
                .store
                .load_lazy_workspace(self.workspace.id())
                .await
                .map_err(store_error)?
                .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
            if state.schema_version != LAZY_STATE_SCHEMA
                || state.workspace_id != self.workspace.id()
                || state.source.identity != source.identity
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            if state.pending_remove.is_some() {
                return Err(LazyWorkspaceError::Concurrent);
            }
            if state.source == source {
                return Ok(state);
            }
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                source,
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
                return Ok(replacement);
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }
}
