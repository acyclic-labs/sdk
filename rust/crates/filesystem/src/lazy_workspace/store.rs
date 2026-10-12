use super::*;
use crate::kernel::{KernelAccounting, KernelOwnedAllocation, Owned};

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
struct MemoryLazyWorkspaceState {
    workspaces: Owned<Vec<(WorkspaceId, LazyWorkspaceState)>>,
    overlays: Owned<Vec<(LazyOverlayId, (LazyOverlay, KernelOwnedAllocation))>>,
    shadows: Owned<Vec<(LazyShadowId, (LazyShadow, KernelOwnedAllocation))>>,
}
impl Default for MemoryLazyWorkspaceState {
    fn default() -> Self {
        Self {
            workspaces: Owned::new(Vec::new(), KernelOwnedAllocation::Standalone, 0),
            overlays: Owned::new(Vec::new(), KernelOwnedAllocation::Standalone, 0),
            shadows: Owned::new(Vec::new(), KernelOwnedAllocation::Standalone, 0),
        }
    }
}
fn position<K: Ord, V>(
    table: &Owned<Vec<(K, V)>>,
    id: &K,
    ctx: &mut KernelAccounting<'_>,
) -> Result<Result<usize, usize>, LazyWorkspaceError> {
    Ok(crate::kernel::binary_search_with_accounting(
        &table.value,
        ctx,
        |entry| entry.0.cmp(id),
    )?)
}
fn insert<K, V>(
    table: &mut Owned<Vec<(K, V)>>,
    index: usize,
    input: &mut Option<(K, V)>,
    ctx: &mut KernelAccounting<'_>,
) -> Result<(), LazyWorkspaceError> {
    let before = ctx
        .standalone_parts()
        .map(|(ledger, _, _, _)| ledger.live_bytes());
    let result = (|| {
        if table.value.len() == table.value.capacity() {
            if matches!(table.allocation, KernelOwnedAllocation::Standalone) {
                table.allocation = ctx.claim_owned(table.bytes, 0)?;
            }
            let count = table
                .value
                .capacity()
                .checked_mul(2)
                .ok_or(crate::WorkError::Overflow)?
                .max(1);
            let mut next = Owned::allocate(count, ctx)?;
            let copied = WorkCounters {
                items_examined: table.value.len() as u64,
                bytes_copied: (table.value.len() * std::mem::size_of::<(K, V)>()) as u64,
                ..WorkCounters::UNCHARGED
            };
            if let Err(error) = ctx.admit(copied) {
                next.retire(ctx)?;
                return Err(error.into());
            }
            next.value.append(&mut table.value);
            let old = std::mem::replace(table, next);
            old.retire(ctx)?;
            ctx.record_completed(copied)?;
        }
        let moved = WorkCounters {
            items_examined: (table.value.len() - index) as u64,
            bytes_copied: ((table.value.len() - index + 1) * std::mem::size_of::<(K, V)>()) as u64,
            ..WorkCounters::UNCHARGED
        };
        ctx.admit(moved)?;
        table
            .value
            .insert(index, input.take().expect("admitted entry"));
        ctx.record_completed(moved)?;
        Ok(())
    })();
    if let Some(before) = before {
        let (ledger, _, _, _) = ctx.standalone_parts().expect("standalone ledger");
        ledger
            .release(ledger.live_bytes() - before)
            .map_err(|e| e.into_work_or(crate::FsError::PendingMutationAllocationFailed))?;
    }
    result
}
fn put_node<K: Ord, T: MemoryLazyNode>(
    table: &mut Owned<Vec<(K, (T, KernelOwnedAllocation))>>,
    id: K,
    digest: [u8; 32],
    value: (T, KernelOwnedAllocation),
    ctx: &mut KernelAccounting<'_>,
) -> Result<(), LazyWorkspaceError> {
    let mut input = Some((id, value));
    let result = (|| {
        let (id, value) = input.as_ref().expect("untransferred node");
        if hash_lazy_value(&value.0, ctx)? != digest {
            return Err(LazyWorkspaceError::Concurrent);
        }
        ctx.charge(lazy_store_write_work())?;
        match position(table, id, ctx)? {
            Ok(i) if table.value[i].1.0 != value.0 => Err(LazyWorkspaceError::Concurrent),
            Ok(_) => Ok(()),
            Err(i) => {
                let bytes = value.0.owned_bytes();
                insert(table, i, &mut input, ctx)?;
                if let Some((ledger, _, _, _)) = ctx.standalone_parts() {
                    ledger.release(bytes).map_err(|e| {
                        e.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                    })?;
                }
                Ok(())
            }
        }
    })();
    if let Some((_, node)) = input {
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
        match position(&stored.workspaces, &id, ctx)? {
            Ok(i) if stored.workspaces.value[i].1.revision == revision => {
                stored.workspaces.value[i].1 = state
            }
            Ok(_) => return Ok(false),
            Err(i) if revision == 0 => {
                insert(&mut stored.workspaces, i, &mut Some((id, state)), ctx)?
            }
            Err(_) => return Ok(false),
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
        Ok(position(&stored.workspaces, &id, ctx)?
            .ok()
            .map(|i| stored.workspaces.value[i].1.clone()))
    }
    async fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError> {
        ctx.charge(lazy_store_read_work())?;
        let stored = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        position(&stored.overlays, &id, ctx)?
            .ok()
            .map(|i| stored.overlays.value[i].1.0.copy(ctx))
            .transpose()
    }
    async fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (LazyOverlay, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let mut stored = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        put_node(&mut stored.overlays, id, id.into_bytes(), value, ctx)
    }
    async fn load_lazy_shadow(
        &self,
        id: LazyShadowId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError> {
        ctx.charge(lazy_store_read_work())?;
        let stored = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        position(&stored.shadows, &id, ctx)?
            .ok()
            .map(|i| stored.shadows.value[i].1.0.copy(ctx))
            .transpose()
    }
    async fn put_lazy_shadow(
        &self,
        id: LazyShadowId,
        value: (LazyShadow, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let mut stored = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        put_node(&mut stored.shadows, id, id.into_bytes(), value, ctx)
    }
}
