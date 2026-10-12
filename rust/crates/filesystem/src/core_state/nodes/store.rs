use super::*;

impl LazyWorkspaceStore for LocalCoreStateStore {
    async fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Option<LazyWorkspaceState>, crate::LazyWorkspaceError> {
        let work = core_lazy_work(accounting)?;
        let state = finish_core_lazy(
            self.materialization(work.clone())
                .load_logged::<LazyWorkspaceState>(id)
                .await,
            &work,
            accounting,
        )?;
        if state.as_ref().is_some_and(|state| {
            state.schema_version != crate::lazy_workspace::LAZY_STATE_SCHEMA
                || state.workspace_id != id
        }) {
            return Err(crate::LazyWorkspaceError::NativeRecordIntegrity);
        }
        Ok(state)
    }

    async fn compare_and_swap_lazy_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
        state: LazyWorkspaceState,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<bool, crate::LazyWorkspaceError> {
        let work = core_lazy_work(accounting)?;
        if state.schema_version != crate::lazy_workspace::LAZY_STATE_SCHEMA
            || state.workspace_id != id
        {
            return Err(crate::LazyWorkspaceError::NativeRecordIntegrity);
        }
        finish_core_lazy(
            self.swap_lazy_logged(id, revision, state, work.clone())
                .await,
            &work,
            accounting,
        )
    }

    async fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<
        Option<(
            LazyOverlay,
            crate::kernel::allocation::KernelOwnedAllocation,
        )>,
        crate::LazyWorkspaceError,
    > {
        let work = core_lazy_work(accounting)?;
        core_lazy_node(
            self.load_node::<LazyOverlay>(id, work.clone()).await,
            &work,
            accounting,
        )
    }

    async fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (
            LazyOverlay,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), crate::LazyWorkspaceError> {
        self.put_node(id, value, accounting).await
    }

    async fn load_lazy_shadow(
        &self,
        id: crate::LazyShadowId,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<
        Option<(
            crate::LazyShadow,
            crate::kernel::allocation::KernelOwnedAllocation,
        )>,
        crate::LazyWorkspaceError,
    > {
        let work = core_lazy_work(accounting)?;
        core_lazy_node(
            self.load_node::<crate::LazyShadow>(id, work.clone()).await,
            &work,
            accounting,
        )
    }

    async fn put_lazy_shadow(
        &self,
        id: crate::LazyShadowId,
        value: (
            crate::LazyShadow,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), crate::LazyWorkspaceError> {
        self.put_node(id, value, accounting).await
    }
}
