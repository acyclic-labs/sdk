use super::*;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};

/// Durable sparse bindings and immutable observation indexes.
pub trait LazyWorkspaceStore: Clone + Send + Sync + 'static {
    /// Loads a binding using the caller's accounting context.
    fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<Option<LazyWorkspaceState>, LazyWorkspaceError>> + Send;
    /// Replaces a binding at its expected revision.
    fn compare_and_swap_lazy_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
        state: LazyWorkspaceState,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<bool, LazyWorkspaceError>> + Send;
    /// Loads an immutable overlay with its allocation guard.
    fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<
        Output = Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError>,
    > + Send;
    /// Transfers an immutable overlay to storage.
    fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (LazyOverlay, KernelOwnedAllocation),
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<(), LazyWorkspaceError>> + Send;
    /// Loads an immutable shadow with its allocation guard.
    fn load_lazy_shadow(
        &self,
        id: LazyShadowId,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<
        Output = Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError>,
    > + Send;
    /// Transfers an immutable shadow to storage.
    fn put_lazy_shadow(
        &self,
        id: LazyShadowId,
        value: (LazyShadow, KernelOwnedAllocation),
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<(), LazyWorkspaceError>> + Send;
}

/// In-memory companion storage.
#[derive(Clone, Default)]
pub struct MemoryLazyWorkspaceStore {
    inner: Arc<Mutex<MemoryLazyWorkspaceState>>,
}

struct MemoryLazyWorkspaceState {
    workspaces: crate::kernel::Owned<Vec<(WorkspaceId, LazyWorkspaceState)>>,
    overlays: crate::kernel::Owned<Vec<(LazyOverlayId, (LazyOverlay, KernelOwnedAllocation))>>,
    shadows: crate::kernel::Owned<Vec<(LazyShadowId, (LazyShadow, KernelOwnedAllocation))>>,
}

impl Default for MemoryLazyWorkspaceState {
    fn default() -> Self {
        Self {
            workspaces: crate::kernel::Owned::new(Vec::new(), KernelOwnedAllocation::Standalone, 0),
            overlays: crate::kernel::Owned::new(Vec::new(), KernelOwnedAllocation::Standalone, 0),
            shadows: crate::kernel::Owned::new(Vec::new(), KernelOwnedAllocation::Standalone, 0),
        }
    }
}

fn position<K: Ord, V>(
    table: &crate::kernel::Owned<Vec<(K, V)>>,
    key: &K,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Result<usize, usize>, LazyWorkspaceError> {
    crate::kernel::binary_search_with_accounting(&table.value, accounting, |entry| entry.0.cmp(key))
        .map_err(Into::into)
}

fn insert<K, V>(
    table: &mut crate::kernel::Owned<Vec<(K, V)>>,
    index: usize,
    entry: &mut Option<(K, V)>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), LazyWorkspaceError> {
    let before = accounting
        .standalone_parts()
        .map(|(ledger, _, _, _)| ledger.live_bytes());
    let result = (|| {
        if table.value.len() == table.value.capacity() {
            if matches!(table.allocation, KernelOwnedAllocation::Standalone) {
                accounting.claim_owned(table.bytes, 0)?;
            }
            let capacity = table
                .value
                .capacity()
                .checked_mul(2)
                .ok_or(crate::WorkError::Overflow)?
                .max(1);
            let next = crate::kernel::Owned::allocate(capacity, accounting)?;
            let copied = WorkCounters {
                items_examined: table.value.len() as u64,
                bytes_copied: (table.value.len() * std::mem::size_of::<(K, V)>()) as u64,
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            let mut old = std::mem::replace(table, next);
            table.value.extend(old.value.drain(..));
            drop(old);
            accounting.record_completed(copied)?;
        }
        let copied = WorkCounters {
            items_examined: (table.value.len() - index) as u64,
            bytes_copied: ((table.value.len() - index + 1) * std::mem::size_of::<(K, V)>()) as u64,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        table
            .value
            .insert(index, entry.take().expect("one admitted entry"));
        accounting.record_completed(copied)?;
        Ok(())
    })();
    // Resident backing transfers to the provider; native guards remain with its vector.
    if let Some(before) = before {
        let (ledger, _, _, _) = accounting.standalone_parts().expect("standalone context");
        ledger
            .release(ledger.live_bytes() - before)
            .map_err(|error| {
                LazyWorkspaceError::Filesystem(
                    error.into_work_or(crate::FsError::PendingMutationAllocationFailed),
                )
            })?;
    }
    result
}

impl LazyWorkspaceStore for MemoryLazyWorkspaceStore {
    async fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyWorkspaceState>, LazyWorkspaceError> {
        accounting.charge(lazy_store_read_work())?;
        let stored = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(position(&stored.workspaces, &id, accounting)?
            .ok()
            .map(|index| stored.workspaces.value[index].1.clone()))
    }

    async fn compare_and_swap_lazy_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
        state: LazyWorkspaceState,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        accounting.check()?;
        if state.workspace_id != id || state.schema_version != LAZY_STATE_SCHEMA {
            return Err(LazyWorkspaceError::Concurrent);
        }
        accounting.charge(lazy_store_write_work())?;
        let mut stored = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match position(&stored.workspaces, &id, accounting)? {
            Ok(index) => {
                if stored.workspaces.value[index].1.revision != revision {
                    return Ok(false);
                }
                stored.workspaces.value[index].1 = state;
            }
            Err(index) => {
                if revision != 0 {
                    return Ok(false);
                }
                insert(
                    &mut stored.workspaces,
                    index,
                    &mut Some((id, state)),
                    accounting,
                )?;
            }
        }
        Ok(true)
    }

    async fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError> {
        accounting.charge(lazy_store_read_work())?;
        let stored = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        position(&stored.overlays, &id, accounting)?
            .ok()
            .map(|index| stored.overlays.value[index].1.0.copy(accounting))
            .transpose()
    }

    async fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (LazyOverlay, KernelOwnedAllocation),
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let mut stored = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        put_node(&mut stored.overlays, id, id.into_bytes(), value, accounting)
    }

    async fn load_lazy_shadow(
        &self,
        id: LazyShadowId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError> {
        accounting.charge(lazy_store_read_work())?;
        let stored = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        position(&stored.shadows, &id, accounting)?
            .ok()
            .map(|index| stored.shadows.value[index].1.0.copy(accounting))
            .transpose()
    }

    async fn put_lazy_shadow(
        &self,
        id: LazyShadowId,
        value: (LazyShadow, KernelOwnedAllocation),
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let mut stored = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        put_node(&mut stored.shadows, id, id.into_bytes(), value, accounting)
    }
}

fn put_node<K: Ord, T: MemoryLazyNode>(
    table: &mut crate::kernel::Owned<Vec<(K, (T, KernelOwnedAllocation))>>,
    id: K,
    digest: [u8; 32],
    value: (T, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), LazyWorkspaceError> {
    let mut input = Some((id, value));
    let result = (|| {
        accounting.check()?;
        let (id, value) = input.as_ref().expect("input retained until transfer");
        if hash_lazy_value(&value.0, accounting)? != digest {
            return Err(LazyWorkspaceError::Concurrent);
        }
        accounting.charge(lazy_store_write_work())?;
        match position(table, id, accounting)? {
            Ok(index) => {
                if table.value[index].1.0 != value.0 {
                    return Err(LazyWorkspaceError::Concurrent);
                }
                Ok(())
            }
            Err(index) => {
                let bytes = value.0.owned_bytes();
                insert(table, index, &mut input, accounting)?;
                if let Some((ledger, _, _, _)) = accounting.standalone_parts() {
                    ledger.release(bytes).map_err(|error| {
                        LazyWorkspaceError::Filesystem(
                            error.into_work_or(crate::FsError::PendingMutationAllocationFailed),
                        )
                    })?;
                }
                Ok(())
            }
        }
    })();
    if let Some((_, value)) = input {
        retire_node(value, accounting)?;
    }
    result
}

pub(super) fn cancellation<'a>(accounting: &KernelAccounting<'a>) -> &'a CancellationToken {
    match accounting {
        KernelAccounting::Standalone { cancellation, .. }
        | KernelAccounting::BorrowedStandalone { cancellation, .. } => cancellation,
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { cancellation, .. } => cancellation,
    }
}

pub(super) fn retire_node<T: MemoryLazyNode>(
    node: (T, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), LazyWorkspaceError> {
    let bytes = node.0.owned_bytes();
    drop(node.0);
    crate::kernel::retire_allocation(node.1, bytes, accounting).map_err(Into::into)
}
