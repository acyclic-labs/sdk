use super::*;
use crate::kernel::Retained;
use crate::kernel::allocation::fixed_index::FixedIndex;
use crate::kernel::allocation::retained::RetainedAllocation;

/// Durable bindings and content-addressed source facts.
pub trait LazyWorkspaceStore: Clone + Send + Sync + 'static {
    /// Loads a workspace using the caller's accounting context.
    fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        ctx: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<Option<LazyWorkspaceState>, LazyWorkspaceError>> + Send;
    /// Loads a overlay using the caller's accounting context.
    fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        ctx: &mut KernelAccounting<'_>,
    ) -> impl Future<
        Output = Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError>,
    > + Send;
    /// Loads a shadow using the caller's accounting context.
    fn load_lazy_shadow(
        &self,
        id: LazyShadowId,
        ctx: &mut KernelAccounting<'_>,
    ) -> impl Future<
        Output = Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError>,
    > + Send;
    /// Replaces a binding at its expected revision.
    fn compare_and_swap_lazy_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
        state: LazyWorkspaceState,
        ctx: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<bool, LazyWorkspaceError>> + Send;
    /// Transfers an immutable overlay to storage.
    fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (LazyOverlay, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<(), LazyWorkspaceError>> + Send;
    /// Transfers an immutable shadow to storage.
    fn put_lazy_shadow(
        &self,
        id: LazyShadowId,
        value: (LazyShadow, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<(), LazyWorkspaceError>> + Send;
}
/// In-memory companion store.
#[derive(Clone, Default)]
pub struct MemoryLazyWorkspaceStore {
    inner: Arc<Mutex<MemoryLazyWorkspaceState>>,
}
#[derive(Default)]
struct MemoryLazyWorkspaceState {
    workspaces: FixedIndex<LazyWorkspaceState>,
    overlays: FixedIndex<Retained<LazyOverlay>, 32>,
    shadows: FixedIndex<Retained<LazyShadow>, 32>,
}
fn put_node<T: MemoryLazyNode>(
    table: &mut FixedIndex<Retained<T>, 32>,
    digest: [u8; 32],
    value: (T, KernelOwnedAllocation),
    ctx: &mut KernelAccounting<'_>,
) -> Result<(), LazyWorkspaceError> {
    let mut input = Some(value);
    let result = (|| {
        let (value, _) = input.as_ref().ok_or(LazyWorkspaceError::Concurrent)?;
        if hash_lazy_value(value, ctx)? != digest {
            return Err(LazyWorkspaceError::Concurrent);
        }
        ctx.charge(lazy_store_write_work())?;
        let stored = table.get_or_try_insert_with(digest, ctx, |ctx| {
            let (value, allocation) = input.as_mut().ok_or(crate::FsError::InvalidDiff)?;
            let allocation = RetainedAllocation::transfer(allocation, value.owned_bytes(), ctx)?;
            let (value, _) = input.take().ok_or(crate::FsError::InvalidDiff)?;
            Ok(Retained { value, allocation })
        })?;
        if input
            .as_ref()
            .is_some_and(|(value, _)| stored.value != *value)
        {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(())
    })();
    if let Some(node) = input {
        retire_node(node, ctx)?;
    }
    result
}
impl LazyWorkspaceStore for MemoryLazyWorkspaceStore {
    async fn compare_and_swap_lazy_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
        state: LazyWorkspaceState,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        if state.workspace_id != id || state.schema_version != LAZY_STATE_SCHEMA {
            return Err(LazyWorkspaceError::Concurrent);
        }
        ctx.charge(lazy_store_write_work())?;
        let mut stored = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let key = id.into_bytes();
        let current = stored.workspaces.get(key, ctx)?;
        let parent = match &state.fork_origin {
            Some(origin) if current.is_none_or(|state| state.fork_origin.is_none()) => stored
                .workspaces
                .get(origin.snapshot.workspace_id.into_bytes(), ctx)?,
            _ => None,
        };
        if !state.accepts_fork_transition(current, parent) {
            return Ok(false);
        }
        match stored.workspaces.get_mut(key, ctx)? {
            Some(workspace) => {
                if workspace.revision != revision {
                    return Ok(false);
                }
                *workspace = state;
            }
            None if revision == 0 => {
                stored
                    .workspaces
                    .get_or_try_insert_with(key, ctx, |_| Ok(state))?;
            }
            None => return Ok(false),
        }
        Ok(true)
    }
    async fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyWorkspaceState>, LazyWorkspaceError> {
        ctx.charge(lazy_store_read_work())?;
        let stored = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        Ok(stored.workspaces.get(id.into_bytes(), ctx)?.cloned())
    }
    async fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError> {
        ctx.charge(lazy_store_read_work())?;
        let stored = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        stored
            .overlays
            .get(id.into_bytes(), ctx)?
            .map(|value| value.value.copy(ctx))
            .transpose()
    }
    async fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (LazyOverlay, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let mut stored = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        put_node(&mut stored.overlays, id.into_bytes(), value, ctx)
    }
    async fn load_lazy_shadow(
        &self,
        id: LazyShadowId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError> {
        ctx.charge(lazy_store_read_work())?;
        let stored = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        stored
            .shadows
            .get(id.into_bytes(), ctx)?
            .map(|value| value.value.copy(ctx))
            .transpose()
    }
    async fn put_lazy_shadow(
        &self,
        id: LazyShadowId,
        value: (LazyShadow, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let mut stored = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        put_node(&mut stored.shadows, id.into_bytes(), value, ctx)
    }
}
