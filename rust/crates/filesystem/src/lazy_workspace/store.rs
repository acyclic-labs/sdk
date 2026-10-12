use super::*;

/// Durable companion storage for constant-size lazy bindings and immutable overlays.
#[async_trait]
pub trait LazyWorkspaceStore: Clone + Send + Sync + 'static {
    /// Backend error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads one workspace binding.
    async fn load_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<LazyWorkspaceState>, Self::Error>;

    /// Loads one binding under an explicit logical durable-read budget.
    async fn load_lazy_workspace_measured(
        &self,
        workspace_id: WorkspaceId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyWorkspaceState>>, LazyWorkspaceError> {
        measured_lazy_store_read(budget, cancellation)?;
        let value = self
            .load_lazy_workspace(workspace_id)
            .await
            .map_err(store_error)?;
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        Ok(OperationReceipt {
            value,
            work: lazy_store_read_work(),
        })
    }

    /// Replaces one binding only at the expected revision (`0` means absent).
    async fn compare_and_swap_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
    ) -> Result<bool, Self::Error>;

    /// Replaces one binding under an explicit logical durable-write budget.
    async fn compare_and_swap_lazy_workspace_measured(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        measured_lazy_store_write(budget, cancellation)?;
        let value = self
            .compare_and_swap_lazy_workspace(workspace_id, expected_revision, replacement)
            .await
            .map_err(store_error)?;
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        Ok(OperationReceipt {
            value,
            work: lazy_store_write_work(),
        })
    }

    /// Loads an immutable content-addressed overlay.
    async fn load_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
    ) -> Result<Option<LazyOverlay>, Self::Error>;

    /// Loads one immutable overlay under an explicit logical read budget.
    async fn load_lazy_overlay_measured(
        &self,
        overlay: LazyOverlayId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyOverlay>>, LazyWorkspaceError> {
        measured_lazy_store_read(budget, cancellation)?;
        let value = self.load_lazy_overlay(overlay).await.map_err(store_error)?;
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        Ok(OperationReceipt {
            value,
            work: lazy_store_read_work(),
        })
    }

    /// Stores an immutable overlay, idempotently rejecting digest mismatches.
    async fn put_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
    ) -> Result<(), Self::Error>;

    /// Loads one immutable source-identity shadow node.
    async fn load_lazy_shadow(
        &self,
        shadow: LazyShadowId,
    ) -> Result<Option<LazyShadow>, Self::Error>;

    /// Loads one immutable shadow under an explicit logical read budget.
    async fn load_lazy_shadow_measured(
        &self,
        shadow: LazyShadowId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyShadow>>, LazyWorkspaceError> {
        measured_lazy_store_read(budget, cancellation)?;
        let value = self.load_lazy_shadow(shadow).await.map_err(store_error)?;
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        Ok(OperationReceipt {
            value,
            work: lazy_store_read_work(),
        })
    }

    /// Stores one immutable source-identity shadow node.
    async fn put_lazy_shadow(
        &self,
        shadow: LazyShadowId,
        value: LazyShadow,
    ) -> Result<(), Self::Error>;
}

/// In-memory companion store for deterministic tests and embedded use.
#[derive(Clone, Default)]
pub struct MemoryLazyWorkspaceStore {
    pub(super) inner: Arc<Mutex<MemoryLazyWorkspaceState>>,
}

#[derive(Default)]
pub(super) struct MemoryLazyWorkspaceState {
    pub(super) workspaces: BTreeMap<WorkspaceId, LazyWorkspaceState>,
    pub(super) overlays: BTreeMap<LazyOverlayId, LazyOverlay>,
    pub(super) shadows: BTreeMap<LazyShadowId, LazyShadow>,
}

#[async_trait]
impl LazyWorkspaceStore for MemoryLazyWorkspaceStore {
    type Error = Infallible;

    async fn load_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<LazyWorkspaceState>, Self::Error> {
        let state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(state.workspaces.get(&workspace_id).cloned())
    }

    async fn compare_and_swap_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
    ) -> Result<bool, Self::Error> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if stored_revision(&state.workspaces, &workspace_id) != expected_revision {
            return Ok(false);
        }
        state.workspaces.insert(workspace_id, replacement);
        Ok(true)
    }

    async fn load_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
    ) -> Result<Option<LazyOverlay>, Self::Error> {
        let state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(state.overlays.get(&overlay).cloned())
    }

    async fn put_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
    ) -> Result<(), Self::Error> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = state.overlays.get(&overlay) {
            assert_eq!(existing, &value, "content-addressed overlay collision");
        } else {
            state.overlays.insert(overlay, value);
        }
        Ok(())
    }

    async fn load_lazy_shadow(
        &self,
        shadow: LazyShadowId,
    ) -> Result<Option<LazyShadow>, Self::Error> {
        let state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(state.shadows.get(&shadow).cloned())
    }

    async fn put_lazy_shadow(
        &self,
        shadow: LazyShadowId,
        value: LazyShadow,
    ) -> Result<(), Self::Error> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = state.shadows.get(&shadow) {
            assert_eq!(existing, &value, "content-addressed shadow collision");
        } else {
            state.shadows.insert(shadow, value);
        }
        Ok(())
    }
}
