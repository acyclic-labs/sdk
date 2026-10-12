use super::*;


impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Creates an empty authored workspace and binds a source without demanding any path.
    pub async fn attach(
        fs: &Fs<A, O>,
        name: impl AsRef<str>,
        source: Arc<D>,
        store: S,
    ) -> Result<Self, LazyWorkspaceError> {
        let workspace = fs.create_workspace(name).await.map_err(workspace_error)?;
        Self::attach_workspace(workspace, source, store).await
    }

    /// Creates an authored workspace with exact filesystem semantics and binds
    /// a source without demanding any path.
    pub async fn attach_with_config(
        fs: &Fs<A, O>,
        name: impl AsRef<str>,
        source: Arc<D>,
        store: S,
        config: VolumeConfig,
    ) -> Result<Self, LazyWorkspaceError> {
        let workspace = fs
            .create_workspace_with_config(name, config)
            .await
            .map_err(workspace_error)?;
        Self::attach_workspace(workspace, source, store).await
    }

    async fn attach_workspace(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
    ) -> Result<Self, LazyWorkspaceError> {
        let overlay = LazyOverlay::default();
        let overlay_id = overlay.id()?;
        store
            .put_lazy_overlay(overlay_id, overlay)
            .await
            .map_err(store_error)?;
        let shadows = LazyShadow::default();
        let shadow_id = shadows.id()?;
        store
            .put_lazy_shadow(shadow_id, shadows)
            .await
            .map_err(store_error)?;
        let state = LazyWorkspaceState {
            schema_version: LAZY_STATE_SCHEMA,
            revision: 1,
            workspace_id: workspace.id(),
            parent_workspace_id: None,
            source: source.reference(),
            overlay: overlay_id,
            shadows: shadow_id,
            pending_remove: None,
        };
        let existing = store
            .load_lazy_workspace(workspace.id())
            .await
            .map_err(store_error)?;
        match existing {
            Some(existing)
                if existing.source == state.source
                    && existing.schema_version == LAZY_STATE_SCHEMA => {}
            Some(_) => return Err(LazyWorkspaceError::StaleSource),
            None => {
                if !store
                    .compare_and_swap_lazy_workspace(workspace.id(), 0, state)
                    .await
                    .map_err(store_error)?
                {
                    return Err(LazyWorkspaceError::Concurrent);
                }
            }
        }
        let lazy = Self {
            workspace,
            source,
            store,
        };
        lazy.recover_pending_remove().await?;
        Ok(lazy)
    }

    /// Reopens `workspace`'s binding, or binds it to `source` afresh when no
    /// binding is durable.
    ///
    /// A binding is absent only when its attach never became durable. Every
    /// later change to the workspace's binding is ordered after the attach, so
    /// nothing durable was built on a lost one and binding afresh is exact.
    pub async fn open_or_attach(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
    ) -> Result<Self, LazyWorkspaceError> {
        if store
            .load_lazy_workspace(workspace.id())
            .await
            .map_err(store_error)?
            .is_none()
        {
            return Self::attach_workspace(workspace, source, store).await;
        }
        Self::open(workspace, source, store).await
    }

    /// Reopens an existing sparse workspace without demanding source state.
    pub async fn open(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
    ) -> Result<Self, LazyWorkspaceError> {
        let state = store
            .load_lazy_workspace(workspace.id())
            .await
            .map_err(store_error)?
            .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        if state.schema_version != LAZY_STATE_SCHEMA
            || state.workspace_id != workspace.id()
            || state.source.identity != source.reference().identity
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        if store
            .load_lazy_overlay(state.overlay)
            .await
            .map_err(store_error)?
            .is_none()
        {
            return Err(LazyWorkspaceError::Store(
                "lazy overlay is absent".to_owned(),
            ));
        }
        if store
            .load_lazy_shadow(state.shadows)
            .await
            .map_err(store_error)?
            .is_none()
        {
            return Err(LazyWorkspaceError::Store(
                "lazy identity shadow is absent".to_owned(),
            ));
        }
        let lazy = Self {
            workspace,
            source,
            store,
        };
        lazy.recover_pending_remove().await?;
        lazy.rebind_source().await?;
        Ok(lazy)
    }

    /// Authored SDK workspace used by merge, lineage, and publication primitives.
    #[must_use]
    pub const fn workspace(&self) -> &Workspace<A, O> {
        &self.workspace
    }

    /// Reopens another workspace bound to the same source capability.
    ///
    /// This is the constant-size branch/resume primitive for adapters that
    /// already resolved the destination workspace through durable lineage.
    pub async fn open_related(&self, workspace: Workspace<A, O>) -> Result<Self, LazyWorkspaceError>
    where
        S: Clone,
    {
        Self::open(workspace, Arc::clone(&self.source), self.store.clone()).await
    }

    /// Captures the complete logical lazy-tree identity without enumeration.
    pub async fn snapshot(&self) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let state = self.state().await?;
        let authored_generation = self.workspace.head().await.map_err(workspace_error)?.id();
        Ok(LazySnapshotRef::new(
            self.workspace.id(),
            authored_generation,
            state.source,
            state.overlay,
            state.shadows,
        ))
    }

    /// Loads the constant-size durable source/lineage binding.
    pub async fn binding(&self) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        self.state().await
    }

    async fn state(&self) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        let state = self
            .store
            .load_lazy_workspace(self.workspace.id())
            .await
            .map_err(store_error)?
            .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        if state.schema_version != LAZY_STATE_SCHEMA
            || state.workspace_id != self.workspace.id()
            || state.source != self.source.reference()
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        if state.pending_remove.is_some() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(state)
    }

    async fn state_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<LazyWorkspaceState>, LazyWorkspaceError> {
        let receipt = self
            .store
            .load_lazy_workspace_measured(self.workspace.id(), budget, cancellation)
            .await?;
        let mut state = receipt
            .value
            .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        if state.schema_version != LAZY_STATE_SCHEMA || state.workspace_id != self.workspace.id() {
            return Err(LazyWorkspaceError::StaleSource);
        }
        let live = self.source.reference();
        if state.source != live {
            // Every view of one shared source follows its epoch: the same
            // source, only advanced (a refresh of the physical root), is
            // adopted here rather than failing each view bound before it.
            if state.source.identity != live.identity || state.source.epoch > live.epoch {
                return Err(LazyWorkspaceError::StaleSource);
            }
            state = self.rebind_source().await?;
        }
        if state.pending_remove.is_some() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(OperationReceipt {
            value: state,
            work: receipt.work,
        })
    }

    async fn recover_pending_remove(&self) -> Result<(), LazyWorkspaceError> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self
                .store
                .load_lazy_workspace(self.workspace.id())
                .await
                .map_err(store_error)?
                .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
            if state.schema_version != LAZY_STATE_SCHEMA
                || state.workspace_id != self.workspace.id()
                || state.source.identity != self.source.reference().identity
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            let Some(pending) = state.pending_remove.as_ref() else {
                return Ok(());
            };
            let committed = match pending.kind {
                PendingLazyRemoveKind::SourceOnly => true,
                PendingLazyRemoveKind::Authored { idempotency_key }
                | PendingLazyRemoveKind::Mounted { idempotency_key } => self
                    .workspace
                    .operation_generation(idempotency_key)
                    .await
                    .map_err(workspace_error)?
                    .is_some(),
            };
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay: if committed {
                    pending.prepared_overlay
                } else {
                    pending.prior_overlay
                },
                shadows: if committed {
                    state.shadows
                } else {
                    pending.prior_shadows
                },
                pending_remove: None,
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

    async fn observe_measured(
        &self,
        path: &str,
        state: LazyWorkspaceState,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<(SourceReference, SourceNode)>, LazyWorkspaceError> {
        let path = self.canonical_path(path)?;
        if let Some(change) = self.overlay_fact(state.overlay, &path).await? {
            return match change {
                LazyOverlayChange::Observe { source, node } if source == state.source => {
                    Ok(OperationReceipt {
                        value: (source, node),
                        work: WorkCounters::default(),
                    })
                }
                LazyOverlayChange::Observe { .. } => {
                    self.observe_live_measured(path, state, cancellation).await
                }
                LazyOverlayChange::Tombstone { .. } => Err(LazyWorkspaceError::NotFound),
            };
        }
        self.observe_live_measured(path, state, cancellation).await
    }

    async fn observe_live_measured(
        &self,
        path: String,
        state: LazyWorkspaceState,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<(SourceReference, SourceNode)>, LazyWorkspaceError> {
        let receipt = self
            .source
            .lookup(state.source, &self.namespace_path(&path)?, cancellation)
            .await
            .map_err(|failure| failure.error)?;
        let node = receipt.value.ok_or(LazyWorkspaceError::NotFound)?;
        self.append_overlay(
            path,
            LazyOverlayChange::Observe {
                source: state.source,
                node,
            },
        )
        .await?;
        Ok(OperationReceipt {
            value: (state.source, node),
            work: receipt.work,
        })
    }
}
