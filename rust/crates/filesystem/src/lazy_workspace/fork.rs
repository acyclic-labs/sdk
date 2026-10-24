use super::*;
impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Forks from the first durable cut and returns the original child view.
    pub async fn fork(
        &self,
        destination: &str,
        key: IdempotencyKey,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Self, LazySnapshotRef), LazyWorkspaceError> {
        let (name, id) =
            self.workspace
                .volume
                .fs
                .workspace_name(destination, ctx, cancellation(ctx))?;
        drop(name);
        let cut = match self.store.load_lazy_workspace(id, ctx).await? {
            Some(state) => self.fork_origin(&state, id, key.operation_id())?.snapshot,
            None => self.snapshot(ctx).await?,
        };
        self.fork_snapshot(destination, cut, key, permit, ctx).await
    }
    /// Forks the authenticated immutable cut under its original creation operation.
    pub async fn fork_snapshot(
        &self,
        destination: &str,
        cut: LazySnapshotRef,
        key: IdempotencyKey,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Self, LazySnapshotRef), LazyWorkspaceError> {
        let cut = self
            .prepare_fork_snapshot(destination, cut, key.operation_id(), ctx)
            .await?;
        let generation = crate::Generation {
            workspace: self.workspace.clone(),
            id: cut.authored_generation,
            publication_closure: None,
        };
        let child = accounted!(ctx, |work, cancel| self
            .workspace
            .fork::<_, _, WorkspaceError>(
                destination,
                ForkOptions::from_generation(generation, key),
                permit,
                work,
                cancel,
                (),
            ))?;
        let original = accounted!(ctx, |work, cancel| self
            .workspace
            .volume
            .fs
            .workspace_for_creation_operation(
                child.name.as_str(),
                child.volume.config,
                key.operation_id(),
                work,
                cancel
            ))?
        .ok_or(LazyWorkspaceError::Concurrent)?;
        self.bind_prepared_fork(cut, child, original.id(), key.operation_id(), ctx)
            .await
    }
    /// Authenticates the creator's original generation before opening the child.
    pub async fn bind_prepared_fork(
        &self,
        cut: LazySnapshotRef,
        child: Workspace<A, O>,
        original_generation: crate::GenerationId,
        operation: crate::OperationId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Self, LazySnapshotRef), LazyWorkspaceError> {
        if !self.workspace.volume.fs.same_deployment(&child.volume.fs)
            || child.volume.config != self.workspace.volume.config
        {
            return Err(WorkspaceError::ForeignGeneration.into());
        }
        self.validate_snapshot(cut, ctx).await?;
        let original = accounted!(ctx, |work, cancel| self
            .workspace
            .volume
            .fs
            .workspace_for_creation_operation(
                child.name.as_str(),
                child.volume.config,
                operation,
                work,
                cancel
            ))?
        .ok_or(LazyWorkspaceError::Concurrent)?;
        if original.id() != original_generation || original.workspace_id() != child.id() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        drop(original);
        let mut ready = false;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self
                .store
                .load_lazy_workspace(child.id(), ctx)
                .await?
                .ok_or(LazyWorkspaceError::Concurrent)?;
            let origin = self.fork_origin(&state, child.id(), operation)?;
            if origin.snapshot != cut {
                return Err(LazyWorkspaceError::Concurrent);
            }
            if origin.phase == LazyForkPhase::Committed {
                ready = true;
                break;
            }
            let revision = state.revision;
            let next = LazyWorkspaceState {
                revision: revision.checked_add(1).ok_or(crate::WorkError::Overflow)?,
                fork_origin: Some(LazyForkOrigin {
                    phase: LazyForkPhase::Committed,
                    ..origin
                }),
                ..state
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(child.id(), revision, next, ctx)
                .await?
            {
                ready = true;
                break;
            }
        }
        if !ready {
            return Err(LazyWorkspaceError::Concurrent);
        }
        ctx.charge(WorkCounters {
            bytes_hashed: LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
            ..WorkCounters::UNCHARGED
        })?;
        let snapshot = LazySnapshotRef::new(
            child.id(),
            original_generation,
            cut.source,
            cut.overlay,
            cut.shadows,
        );
        Ok((
            Self {
                workspace: child,
                source: Arc::clone(&self.source),
                store: self.store.clone(),
                #[cfg(not(target_arch = "wasm32"))]
                _allocation: None,
            },
            snapshot,
        ))
    }
    /// Retains the source cut before an external creator publishes the child.
    pub async fn prepare_fork_snapshot(
        &self,
        destination: &str,
        cut: LazySnapshotRef,
        operation: crate::OperationId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let (name, id) =
            self.workspace
                .volume
                .fs
                .workspace_name(destination, ctx, cancellation(ctx))?;
        drop(name);
        if id == self.workspace.id() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        self.validate_snapshot(cut, ctx).await?;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            if let Some(state) = self.store.load_lazy_workspace(id, ctx).await? {
                let origin = self.fork_origin(&state, id, operation)?;
                return if origin.snapshot == cut {
                    Ok(cut)
                } else {
                    Err(LazyWorkspaceError::Concurrent)
                };
            }
            let hold = accounted!(ctx, |work, cancel| self
                .workspace
                .volume
                .fs
                .make_generation_durable(
                    &self.workspace.volume,
                    cut.authored_generation,
                    work,
                    cancel
                ))?;
            let state = LazyWorkspaceState {
                parent_workspace_id: Some(self.workspace.id()),
                fork_origin: Some(LazyForkOrigin {
                    operation_id: operation,
                    snapshot: cut,
                    config: self.workspace.volume.config,
                    phase: LazyForkPhase::Prepared,
                }),
                ..LazyWorkspaceState::new(id, cut.source, cut.overlay, cut.shadows)
            };
            let swapped = self
                .store
                .compare_and_swap_lazy_workspace(id, 0, state, ctx)
                .await;
            drop(hold);
            if swapped? {
                return Ok(cut);
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }
    fn fork_origin(
        &self,
        state: &LazyWorkspaceState,
        id: WorkspaceId,
        operation: crate::OperationId,
    ) -> Result<LazyForkOrigin, LazyWorkspaceError> {
        let origin = state.fork_origin.ok_or(LazyWorkspaceError::Concurrent)?;
        if state.schema_version != LAZY_STATE_SCHEMA
            || state.workspace_id != id
            || state.parent_workspace_id != Some(self.workspace.id())
            || origin.operation_id != operation
            || origin.config != self.workspace.volume.config
        {
            return Err(LazyWorkspaceError::Concurrent);
        }
        if origin.phase == LazyForkPhase::Prepared
            && (state.source != origin.snapshot.source
                || state.overlay != origin.snapshot.overlay
                || state.shadows != origin.snapshot.shadows
                || state.pending_remove.is_some())
        {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(origin)
    }
}
