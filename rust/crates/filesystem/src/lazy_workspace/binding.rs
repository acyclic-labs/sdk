use super::*;

impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Creates or recovers the original workspace creation and its sparse binding.
    pub async fn attach(
        fs: &Fs<A, O>,
        name: &str,
        source: Arc<D>,
        store: S,
        config: VolumeConfig,
        operation: crate::OperationId,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(Self, crate::Generation<A, O>), LazyWorkspaceError> {
        accounting.check()?;
        let reference = source.reference();
        let cancellation = cancellation(accounting);
        let generation = 'accounted: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                break 'accounted fs
                    .create_workspace_with_operation::<_, crate::NativeAuthorityGuard<
                        fn(
                            &crate::NativeMaterializationWork,
                        )
                            -> std::future::Ready<Result<(), WorkspaceError>>,
                    >, WorkspaceError>(
                        name, config, operation, original, cancellation, None
                    )
                    .await
                    .map_err(LazyWorkspaceError::from)?;
            }
            accept_receipt(
                fs.create_workspace_with_operation::<_, &mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>, WorkspaceError>(
                    name,
                    config,
                    operation,
                    accounting.remaining()?,
                    cancellation,
                    None,
                )
                .await,
                accounting,
            )?
        };
        let workspace = generation.workspace().clone();
        let lazy = Self::bind(workspace, source, store, accounting).await?;
        let state = lazy
            .store
            .load_lazy_workspace(lazy.workspace.id(), accounting)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        state.validate_root_binding(lazy.workspace.id(), reference)?;
        if lazy.source.reference() != reference {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok((lazy, generation))
    }

    /// Opens a sparse binding, creating its empty indexes when absent.
    pub async fn bind(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Self, LazyWorkspaceError> {
        let reference = source.reference();
        if store
            .load_lazy_workspace(workspace.id(), accounting)
            .await?
            .is_none()
        {
            let (overlay, shadows) = Self::empty_roots(&store, accounting).await?;
            let state = LazyWorkspaceState::new(workspace.id(), reference, overlay, shadows);
            if !store
                .compare_and_swap_lazy_workspace(workspace.id(), 0, state, accounting)
                .await?
            {
                let state = store
                    .load_lazy_workspace(workspace.id(), accounting)
                    .await?
                    .ok_or(LazyWorkspaceError::Concurrent)?;
                state.validate_root_binding(workspace.id(), reference)?;
            }
        }
        Self::open(workspace, source, store, accounting).await
    }

    pub(super) async fn empty_roots(
        store: &S,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(LazyOverlayId, LazyShadowId), LazyWorkspaceError> {
        let overlay = LazyOverlay::default();
        let overlay_id = LazyOverlayId(hash_lazy_value(&overlay, accounting)?);
        let allocation = accounting.claim_owned(0, 0)?;
        store
            .put_lazy_overlay(overlay_id, (overlay, allocation), accounting)
            .await?;
        let shadow = LazyShadow::default();
        let shadow_id = LazyShadowId(hash_lazy_value(&shadow, accounting)?);
        let allocation = accounting.claim_owned(0, 0)?;
        store
            .put_lazy_shadow(shadow_id, (shadow, allocation), accounting)
            .await?;
        Ok((overlay_id, shadow_id))
    }

    /// Reopens an existing sparse binding without demanding source paths.
    pub async fn open(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Self, LazyWorkspaceError> {
        let state = store
            .load_lazy_workspace(workspace.id(), accounting)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        if state.schema_version != LAZY_STATE_SCHEMA
            || state.workspace_id != workspace.id()
            || state.source.identity != source.reference().identity
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        state.require_ready_fork()?;
        let overlay = store
            .load_lazy_overlay(state.overlay, accounting)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        retire_node(overlay, accounting)?;
        let shadows = store
            .load_lazy_shadow(state.shadows, accounting)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        retire_node(shadows, accounting)?;
        let lazy = Self {
            workspace,
            source,
            store,
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        };
        lazy.recover_pending_remove(accounting).await?;
        lazy.rebind_source(accounting).await?;
        Ok(lazy)
    }

    /// Authored SDK workspace used by merge, lineage, and publication primitives.
    #[must_use]
    pub const fn workspace(&self) -> &Workspace<A, O> {
        &self.workspace
    }

    /// Reopens another workspace bound to the same source capability.
    pub async fn open_related(
        &self,
        workspace: Workspace<A, O>,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Self, LazyWorkspaceError>
    where
        S: Clone,
    {
        Self::open(
            workspace,
            Arc::clone(&self.source),
            self.store.clone(),
            accounting,
        )
        .await
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn watch_source(
        &self,
        sink: Arc<dyn crate::demand::SourceChangeSink>,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Option<Box<dyn crate::demand::SourceWatch>>, LazyWorkspaceError> {
        let cancellation = cancellation(accounting);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return self
                .source
                .watch(sink, original, cancellation)
                .map_err(Into::into);
        }
        let result = self
            .source
            .watch(sink, accounting.remaining()?, cancellation);
        accept_receipt(result, accounting)
    }

    pub(crate) fn source_reference(&self) -> SourceReference {
        self.source.reference()
    }

    /// Captures the ordinary logical cut without enumeration.
    pub async fn snapshot(
        &self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let state = self.state(accounting).await?;
        let generation = self.head(accounting).await?;
        accounting.charge(WorkCounters {
            bytes_hashed: LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(LazySnapshotRef::new(
            self.workspace.id(),
            generation.id(),
            state.source,
            state.overlay,
            state.shadows,
        ))
    }

    /// Loads a ready binding, following an advanced shared source epoch.
    pub async fn state(
        &self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        let state = self
            .store
            .load_lazy_workspace(self.workspace.id(), accounting)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        if state.schema_version != LAZY_STATE_SCHEMA || state.workspace_id != self.workspace.id() {
            return Err(LazyWorkspaceError::StaleSource);
        }
        state.require_ready_fork()?;
        if state.pending_remove.is_some() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        let live = self.source.reference();
        if state.source == live {
            return Ok(state);
        }
        if state.source.identity != live.identity || state.source.epoch > live.epoch {
            return Err(LazyWorkspaceError::StaleSource);
        }
        self.rebind_source(accounting).await
    }

    /// Advances the binding to its shared source's current epoch.
    pub async fn rebind_source(
        &self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        let source = self.source.reference();
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self
                .store
                .load_lazy_workspace(self.workspace.id(), accounting)
                .await?
                .ok_or(LazyWorkspaceError::NotFound)?;
            if state.schema_version != LAZY_STATE_SCHEMA
                || state.workspace_id != self.workspace.id()
                || state.source.identity != source.identity
                || state.source.epoch > source.epoch
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            state.require_ready_fork()?;
            if state.pending_remove.is_some() {
                return Err(LazyWorkspaceError::Concurrent);
            }
            if state.source == source {
                return Ok(state);
            }
            let revision = state.revision;
            let next = LazyWorkspaceState {
                revision: revision.checked_add(1).ok_or(crate::WorkError::Overflow)?,
                source,
                ..state
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(
                    self.workspace.id(),
                    revision,
                    next.clone(),
                    accounting,
                )
                .await?
            {
                return Ok(next);
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    pub(super) async fn recover_pending_remove(
        &self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self
                .store
                .load_lazy_workspace(self.workspace.id(), accounting)
                .await?
                .ok_or(LazyWorkspaceError::NotFound)?;
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
                    .operation_generation(idempotency_key, accounting)
                    .await?
                    .is_some(),
            };
            let next = LazyWorkspaceState {
                revision: state
                    .revision
                    .checked_add(1)
                    .ok_or(crate::WorkError::Overflow)?,
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
                .compare_and_swap_lazy_workspace(
                    self.workspace.id(),
                    state.revision,
                    next,
                    accounting,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    pub(crate) async fn source_lookup(
        &self,
        source: SourceReference,
        path: &str,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Option<SourceNode>, LazyWorkspaceError> {
        let namespace = self.namespace_path(path, accounting)?;
        let cancellation = cancellation(accounting);
        let result = 'accounted: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                break 'accounted self
                    .source
                    .lookup(source, &namespace.value, original, cancellation)
                    .await
                    .map_err(LazyWorkspaceError::from);
            }
            let receipt = self
                .source
                .lookup(
                    source,
                    &namespace.value,
                    accounting.remaining()?,
                    cancellation,
                )
                .await;
            accept_receipt(receipt, accounting)
        };
        namespace.retire(accounting)?;
        result
    }

    pub(super) async fn head(
        &self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let cancellation = cancellation(accounting);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return self
                .workspace
                .head(original, cancellation)
                .await
                .map_err(Into::into);
        }
        let receipt = self
            .workspace
            .head(accounting.remaining()?, cancellation)
            .await;
        accept_receipt(receipt, accounting)
    }

    pub(crate) async fn operation_generation(
        &self,
        operation: IdempotencyKey,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Option<crate::GenerationId>, LazyWorkspaceError> {
        let cancellation = cancellation(accounting);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return self
                .workspace
                .operation_generation(operation, original, cancellation)
                .await
                .map(|generation| generation.map(|generation| generation.id()))
                .map_err(Into::into);
        }
        let receipt = self
            .workspace
            .operation_generation(operation, accounting.remaining()?, cancellation)
            .await;
        accept_receipt(receipt, accounting)
            .map(|generation| generation.map(|generation| generation.id()))
    }
    pub(crate) async fn checkout(
        &self,
        selector: crate::model::GenerationSelector,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<crate::kernel::Owned<crate::Checkout<A, O>>, LazyWorkspaceError> {
        let cancellation = cancellation(accounting);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let value = self
                .workspace
                .volume
                .checkout(
                    selector,
                    crate::model::CheckoutMode::read_only_pinned(),
                    original,
                    cancellation,
                )
                .await?;
            return Ok(crate::kernel::Owned::new(
                value,
                accounting.claim_owned(0, 0)?,
                0,
            ));
        }
        let value = accept_receipt(
            self.workspace
                .volume
                .checkout(
                    selector,
                    crate::model::CheckoutMode::read_only_pinned(),
                    accounting.remaining()?,
                    cancellation,
                )
                .await,
            accounting,
        )?;
        let bytes = value.logical_owned_bytes();
        let (mut allocation, admitted) = accounting.record_actual_owned(bytes)?;
        if let Err(error) = admitted {
            drop(value);
            accounting.release_owned(&mut allocation, bytes)?;
            return Err(error.into());
        }
        Ok(crate::kernel::Owned::new(value, allocation, bytes))
    }

    /// Validates the cut's identity without rebinding an unresolved source.
    pub fn validate_snapshot(
        &self,
        snapshot: LazySnapshotRef,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        accounting.check()?;
        if snapshot.workspace_id != self.workspace.id()
            || snapshot.source.identity != self.source.reference().identity
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        accounting.charge(WorkCounters {
            bytes_hashed: LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
            ..WorkCounters::UNCHARGED
        })?;
        if LazySnapshotRef::new(
            snapshot.workspace_id,
            snapshot.authored_generation,
            snapshot.source,
            snapshot.overlay,
            snapshot.shadows,
        ) != snapshot
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok(())
    }
}
