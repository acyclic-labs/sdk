use super::*;
use crate::kernel::KernelAccounting;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Persists the first immutable cut before creating a child project.
    pub async fn prepare_fork_snapshot(
        &self,
        destination: &str,
        frozen: LazySnapshotRef,
        operation: crate::OperationId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let cancellation = cancellation(accounting);
        let (name, id) =
            self.workspace
                .volume
                .fs
                .workspace_name(destination, accounting, cancellation)?;
        drop(name);
        self.prepare_fork_at(id, frozen, operation, accounting)
            .await
    }

    pub(super) fn fork_origin(
        &self,
        state: &LazyWorkspaceState,
        id: WorkspaceId,
        operation: crate::OperationId,
        accounting: &mut KernelAccounting<'_>,
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
        self.validate_snapshot(origin.snapshot, accounting)?;
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

    pub(super) async fn prepare_fork_at(
        &self,
        id: WorkspaceId,
        frozen: LazySnapshotRef,
        operation: crate::OperationId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        if id == self.workspace.id() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        self.validate_snapshot(frozen, accounting)?;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            if let Some(state) = self.store.load_lazy_workspace(id, accounting).await? {
                let origin = self.fork_origin(&state, id, operation, accounting)?;
                return if origin.snapshot == frozen {
                    Ok(frozen)
                } else {
                    Err(LazyWorkspaceError::Concurrent)
                };
            }
            let cancellation = cancellation(accounting);
            let hold = 'accounted: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    break 'accounted self
                        .workspace
                        .volume
                        .fs
                        .make_generation_durable(
                            &self.workspace.volume,
                            frozen.authored_generation,
                            original,
                            cancellation,
                        )
                        .await
                        .map_err(LazyWorkspaceError::from)?;
                }
                accept_receipt(
                    self.workspace
                        .volume
                        .fs
                        .make_generation_durable(
                            &self.workspace.volume,
                            frozen.authored_generation,
                            accounting.remaining()?,
                            cancellation,
                        )
                        .await,
                    accounting,
                )?
            };
            let state = LazyWorkspaceState {
                parent_workspace_id: Some(self.workspace.id()),
                fork_origin: Some(LazyForkOrigin {
                    operation_id: operation,
                    snapshot: frozen,
                    config: self.workspace.volume.config,
                    phase: LazyForkPhase::Prepared,
                }),
                ..LazyWorkspaceState::new(id, frozen.source, frozen.overlay, frozen.shadows)
            };
            let swapped = self
                .store
                .compare_and_swap_lazy_workspace(id, 0, state, accounting)
                .await;
            drop(hold);
            if swapped? {
                return Ok(frozen);
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    /// Authenticates the original child creation before making its binding ready.
    pub async fn bind_prepared_fork(
        &self,
        frozen: LazySnapshotRef,
        child: Workspace<A, O>,
        original_generation: crate::GenerationId,
        operation: crate::OperationId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Self, LazySnapshotRef), LazyWorkspaceError> {
        if !self.workspace.volume.fs.same_deployment(&child.volume.fs)
            || child.volume.config != self.workspace.volume.config
        {
            return Err(WorkspaceError::ForeignGeneration.into());
        }
        let cancellation = cancellation(accounting);
        let observed = 'accounted: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                break 'accounted self
                    .workspace
                    .volume
                    .fs
                    .workspace_for_creation_operation(
                        child.name.as_str(),
                        child.volume.config,
                        operation,
                        original,
                        cancellation,
                    )
                    .await
                    .map_err(LazyWorkspaceError::from)?;
            }
            accept_receipt(
                self.workspace
                    .volume
                    .fs
                    .workspace_for_creation_operation(
                        child.name.as_str(),
                        child.volume.config,
                        operation,
                        accounting.remaining()?,
                        cancellation,
                    )
                    .await,
                accounting,
            )?
        }
        .ok_or(LazyWorkspaceError::Concurrent)?;
        if observed.id() != original_generation || observed.workspace_id() != child.id() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        drop(observed);
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self
                .store
                .load_lazy_workspace(child.id(), accounting)
                .await?
                .ok_or(LazyWorkspaceError::Concurrent)?;
            let origin = self.fork_origin(&state, child.id(), operation, accounting)?;
            if origin.snapshot != frozen {
                return Err(LazyWorkspaceError::Concurrent);
            }
            if origin.phase == LazyForkPhase::Committed {
                break;
            }
            let revision = state.revision;
            let state = LazyWorkspaceState {
                revision: revision.checked_add(1).ok_or(crate::WorkError::Overflow)?,
                fork_origin: Some(LazyForkOrigin {
                    phase: LazyForkPhase::Committed,
                    ..origin
                }),
                ..state
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(child.id(), revision, state, accounting)
                .await?
            {
                break;
            }
        }
        let state = self
            .store
            .load_lazy_workspace(child.id(), accounting)
            .await?
            .ok_or(LazyWorkspaceError::Concurrent)?;
        let origin = self.fork_origin(&state, child.id(), operation, accounting)?;
        if origin.phase != LazyForkPhase::Committed {
            return Err(LazyWorkspaceError::Concurrent);
        }
        accounting.charge(WorkCounters {
            bytes_hashed: LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
            ..WorkCounters::UNCHARGED
        })?;
        let snapshot = LazySnapshotRef::new(
            child.id(),
            original_generation,
            frozen.source,
            frozen.overlay,
            frozen.shadows,
        );
        Ok((
            Self {
                workspace: child,
                source: Arc::clone(&self.source),
                store: self.store.clone(),
                #[cfg(not(target_arch = "wasm32"))]
                allocation: None,
            },
            snapshot,
        ))
    }
}
