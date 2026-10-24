//! Reject mutations even if a future inspection helper tries to retain facts.
use super::*;

#[derive(Clone)]
pub(super) struct ReadStore<S>(pub(super) S);

impl<S: LazyWorkspaceStore> LazyWorkspaceStore for ReadStore<S> {
    async fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyWorkspaceState>, LazyWorkspaceError> {
        self.0.load_lazy_workspace(id, ctx).await
    }
    async fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.0.load_lazy_overlay(id, ctx).await
    }
    async fn load_lazy_shadow(
        &self,
        id: LazyShadowId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.0.load_lazy_shadow(id, ctx).await
    }
    async fn compare_and_swap_lazy_workspace(
        &self,
        _id: WorkspaceId,
        _revision: u64,
        _state: LazyWorkspaceState,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        ctx.check()?;
        Err(LazyWorkspaceError::Concurrent)
    }
    async fn put_lazy_overlay(
        &self,
        _id: LazyOverlayId,
        value: (LazyOverlay, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        retire_node(value, ctx)?;
        Err(LazyWorkspaceError::Concurrent)
    }
    async fn put_lazy_shadow(
        &self,
        _id: LazyShadowId,
        value: (LazyShadow, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        retire_node(value, ctx)?;
        Err(LazyWorkspaceError::Concurrent)
    }
}
