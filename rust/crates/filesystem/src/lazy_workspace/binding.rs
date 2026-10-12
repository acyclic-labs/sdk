use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Creates or recovers the original SDK workspace and root binding.
    pub async fn attach(
        fs: &Fs<A, O>,
        name: &str,
        source: Arc<D>,
        store: S,
        config: VolumeConfig,
        operation: crate::OperationId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Self, crate::Generation<A, O>), LazyWorkspaceError> {
        let original = source.reference();
        let generation = accounted!(ctx, |work, cancel| fs
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                name, config, operation, work, cancel, None
            ))?;
        let lazy = Self::bind(generation.workspace().clone(), source, store, ctx).await?;
        lazy.state(ctx)
            .await?
            .validate_root_binding(lazy.workspace.id(), original)?;
        if lazy.source.reference() != original {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok((lazy, generation))
    }

    /// Binds an existing workspace when its original attach was not durable.
    pub async fn bind(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Self, LazyWorkspaceError> {
        let reference = source.reference();
        if store
            .load_lazy_workspace(workspace.id(), ctx)
            .await?
            .is_none()
        {
            let (overlay, shadows) = Self::empty_roots(&store, ctx).await?;
            if !store
                .compare_and_swap_lazy_workspace(
                    workspace.id(),
                    0,
                    LazyWorkspaceState::new(workspace.id(), reference, overlay, shadows),
                    ctx,
                )
                .await?
            {
                store
                    .load_lazy_workspace(workspace.id(), ctx)
                    .await?
                    .ok_or(LazyWorkspaceError::Concurrent)?
                    .validate_root_binding(workspace.id(), reference)?;
            }
        }
        Self::open(workspace, source, store, ctx).await
    }

    pub(super) async fn empty_roots(
        store: &S,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(LazyOverlayId, LazyShadowId), LazyWorkspaceError> {
        let overlay = LazyOverlay::Empty;
        let overlay_id = LazyOverlayId(hash_lazy_value(&overlay, ctx)?);
        let guard = ctx.claim_owned(0, 0)?;
        store
            .put_lazy_overlay(overlay_id, (overlay, guard), ctx)
            .await?;
        let shadows = LazyShadow::Empty;
        let shadow_id = LazyShadowId(hash_lazy_value(&shadows, ctx)?);
        let guard = ctx.claim_owned(0, 0)?;
        store
            .put_lazy_shadow(shadow_id, (shadows, guard), ctx)
            .await?;
        Ok((overlay_id, shadow_id))
    }

    /// Reopens a durable binding without enumerating its source.
    pub async fn open(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Self, LazyWorkspaceError> {
        let state = store
            .load_lazy_workspace(workspace.id(), ctx)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        if state.schema_version != LAZY_STATE_SCHEMA
            || state.workspace_id != workspace.id()
            || state.source.identity != source.reference().identity
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        state.require_ready_fork()?;
        retire_node(
            store
                .load_lazy_overlay(state.overlay, ctx)
                .await?
                .ok_or(LazyWorkspaceError::NotFound)?,
            ctx,
        )?;
        retire_node(
            store
                .load_lazy_shadow(state.shadows, ctx)
                .await?
                .ok_or(LazyWorkspaceError::NotFound)?,
            ctx,
        )?;
        let lazy = Self {
            workspace,
            source,
            store,
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        };
        lazy.recover_pending_remove(ctx).await?;
        lazy.state(ctx).await?;
        Ok(lazy)
    }

    /// Authored SDK workspace supplying publication and lineage.
    pub const fn workspace(&self) -> &Workspace<A, O> {
        &self.workspace
    }

    /// Reopens a related workspace over the same source capability.
    pub async fn open_related(
        &self,
        workspace: Workspace<A, O>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Self, LazyWorkspaceError> {
        Self::open(workspace, Arc::clone(&self.source), self.store.clone(), ctx).await
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn watch_source(
        &self,
        sink: Arc<dyn crate::demand::SourceChangeSink>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<Box<dyn crate::demand::SourceWatch>>, LazyWorkspaceError> {
        let cancel = cancellation(ctx);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = ctx.native_work() {
            return self.source.watch(sink, work, cancel).map_err(Into::into);
        }
        let result = self.source.watch(sink, ctx.remaining()?, cancel);
        accept_receipt(result, ctx)
    }

    pub(crate) fn source_reference(&self) -> SourceReference {
        self.source.reference()
    }

    /// Captures an ordinary complete cut without enumeration.
    pub async fn snapshot(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let state = self.state(ctx).await?;
        let head = self.head(ctx).await?;
        ctx.charge(WorkCounters {
            bytes_hashed: LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(LazySnapshotRef::new(
            self.workspace.id(),
            head.id(),
            state.source,
            state.overlay,
            state.shadows,
        ))
    }

    /// Loads the ready binding, following a shared source's newer epoch.
    pub async fn state(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self
                .store
                .load_lazy_workspace(self.workspace.id(), ctx)
                .await?
                .ok_or(LazyWorkspaceError::NotFound)?;
            let source = self.source.reference();
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
                .compare_and_swap_lazy_workspace(self.workspace.id(), revision, next.clone(), ctx)
                .await?
            {
                return Ok(next);
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    pub(super) async fn recover_pending_remove(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self
                .store
                .load_lazy_workspace(self.workspace.id(), ctx)
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
                    .operation_generation(idempotency_key, ctx)
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
                .compare_and_swap_lazy_workspace(self.workspace.id(), state.revision, next, ctx)
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
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceNode>, LazyWorkspaceError> {
        let path = self.namespace_path(path, ctx)?;
        let result = accounted!(ctx, |work, cancel| self.source.lookup(
            source,
            &path.value,
            work,
            cancel
        ));
        path.retire(ctx)?;
        result
    }

    pub(super) async fn head(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let checkout = self
            .checkout(crate::model::GenerationSelector::Head, ctx)
            .await?;
        let id = checkout.value.generation_id();
        checkout.retire(ctx)?;
        Ok(crate::Generation {
            workspace: self.workspace.clone(),
            id,
            publication_closure: None,
        })
    }

    pub(crate) async fn operation_generation(
        &self,
        key: IdempotencyKey,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<crate::GenerationId>, LazyWorkspaceError> {
        let original = self
            .workspace
            .volume
            .fs
            .workspace_operation_generation(
                &self.workspace.volume,
                key.operation_id(),
                ctx,
                cancellation(ctx),
            )
            .await?;
        Ok(original.0.as_ref().map(|(id, _)| *id))
    }
    /// Opens an accounted readonly checkout of the selected generation.
    pub async fn checkout(
        &self,
        selector: crate::model::GenerationSelector,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Owned<crate::Checkout<A, O>>, LazyWorkspaceError> {
        let value = self
            .workspace
            .checkout_accounted(
                selector,
                crate::model::CheckoutMode::read_only_pinned(),
                ctx,
                cancellation(ctx),
            )
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if ctx.native_work().is_some() {
            return Ok(Owned::new(value, ctx.claim_owned(0, 0)?, 0));
        }
        let bytes = value.logical_owned_bytes();
        let (guard, admitted) = ctx.record_actual_owned(bytes)?;
        let owned = Owned::new(value, guard, bytes);
        if let Err(error) = admitted {
            owned.retire(ctx)?;
            return Err(error.into());
        }
        Ok(owned)
    }

    /// Validates a cut and its available indexes and generation without rebinding.
    pub async fn validate_snapshot(
        &self,
        cut: LazySnapshotRef,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        ctx.charge(WorkCounters {
            bytes_hashed: LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
            ..WorkCounters::UNCHARGED
        })?;
        if cut.workspace_id != self.workspace.id()
            || cut.source.identity != self.source.reference().identity
            || cut
                != LazySnapshotRef::new(
                    cut.workspace_id,
                    cut.authored_generation,
                    cut.source,
                    cut.overlay,
                    cut.shadows,
                )
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
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
        self.checkout(
            crate::model::GenerationSelector::Exact(cut.authored_generation),
            ctx,
        )
        .await?
        .retire(ctx)?;
        Ok(())
    }
}
