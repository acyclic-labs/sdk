use super::*;

impl LazyWorkspaceStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<LazyWorkspaceState>, Self::Error> {
        self.load_lazy_logged(workspace_id, unbounded_lazy_work()?)
            .await
    }

    async fn compare_and_swap_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
    ) -> Result<bool, Self::Error> {
        self.swap_lazy_logged(
            workspace_id,
            expected_revision,
            replacement,
            unbounded_lazy_work()?,
        )
        .await
    }

    async fn load_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
    ) -> Result<Option<LazyOverlay>, Self::Error> {
        self.load_node::<LazyOverlay>(overlay, unbounded_lazy_work()?)
            .await
            .map(|value| value.map(|owned| owned.into_parts().0))
    }

    async fn put_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
    ) -> Result<(), Self::Error> {
        let work = unbounded_lazy_work()?;
        let allocation = work.claim(value.owned_bytes(), 0, crate::WorkCounters::UNCHARGED)?;
        self.put_node(
            overlay,
            NativeOwnedCoreState::from_parts(value, allocation),
            work,
        )
        .await
    }

    async fn load_lazy_shadow(
        &self,
        shadow: crate::LazyShadowId,
    ) -> Result<Option<crate::LazyShadow>, Self::Error> {
        self.load_node::<crate::LazyShadow>(shadow, unbounded_lazy_work()?)
            .await
            .map(|value| value.map(|owned| owned.into_parts().0))
    }

    async fn put_lazy_shadow(
        &self,
        shadow: crate::LazyShadowId,
        value: crate::LazyShadow,
    ) -> Result<(), Self::Error> {
        let work = unbounded_lazy_work()?;
        let allocation = work.claim(value.owned_bytes(), 0, crate::WorkCounters::UNCHARGED)?;
        self.put_node(
            shadow,
            NativeOwnedCoreState::from_parts(value, allocation),
            work,
        )
        .await
    }

    async fn load_lazy_workspace_measured(
        &self,
        workspace_id: WorkspaceId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Option<LazyWorkspaceState>>, crate::LazyWorkspaceError>
    {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        finish_lazy_receipt(
            self.load_lazy_logged(workspace_id, work.clone()).await,
            &work,
        )
    }

    async fn compare_and_swap_lazy_workspace_measured(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, crate::LazyWorkspaceError> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        finish_lazy_receipt(
            self.swap_lazy_logged(workspace_id, expected_revision, replacement, work.clone())
                .await,
            &work,
        )
    }

    async fn load_lazy_overlay_measured(
        &self,
        overlay: LazyOverlayId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Option<LazyOverlay>>, crate::LazyWorkspaceError> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        finish_lazy_receipt(
            self.load_node::<LazyOverlay>(overlay, work.clone())
                .await
                .map(|value| value.map(|owned| owned.into_parts().0)),
            &work,
        )
    }

    async fn put_lazy_overlay_measured(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<()>, crate::LazyWorkspaceError> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        let result = match work.claim(value.owned_bytes(), 0, crate::WorkCounters::UNCHARGED) {
            Ok(allocation) => {
                self.put_node(
                    overlay,
                    NativeOwnedCoreState::from_parts(value, allocation),
                    work.clone(),
                )
                .await
            }
            Err(error) => Err(error.into()),
        };
        finish_lazy_receipt(result, &work)
    }

    async fn load_lazy_shadow_measured(
        &self,
        shadow: crate::LazyShadowId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Option<crate::LazyShadow>>, crate::LazyWorkspaceError> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        finish_lazy_receipt(
            self.load_node::<crate::LazyShadow>(shadow, work.clone())
                .await
                .map(|value| value.map(|owned| owned.into_parts().0)),
            &work,
        )
    }

    async fn put_lazy_shadow_measured(
        &self,
        shadow: crate::LazyShadowId,
        value: crate::LazyShadow,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<()>, crate::LazyWorkspaceError> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        let result = match work.claim(value.owned_bytes(), 0, crate::WorkCounters::UNCHARGED) {
            Ok(allocation) => {
                self.put_node(
                    shadow,
                    NativeOwnedCoreState::from_parts(value, allocation),
                    work.clone(),
                )
                .await
            }
            Err(error) => Err(error.into()),
        };
        finish_lazy_receipt(result, &work)
    }

    async fn load_lazy_workspace_with_native_work(
        &self,
        id: WorkspaceId,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<NativeOwnedCoreState<LazyWorkspaceState>>, crate::LazyWorkspaceError> {
        let state = self
            .load_lazy_logged(id, work.clone())
            .await
            .map_err(native_core_error)?;
        state
            .map(|state| {
                work.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })
                .map_err(|error| native_core_error(error.into()))?;
                if state.schema_version != crate::lazy_workspace::LAZY_STATE_SCHEMA
                    || state.workspace_id != id
                {
                    return Err(crate::LazyWorkspaceError::NativeRecordIntegrity);
                }
                let allocation = work
                    .claim(0, 0, crate::WorkCounters::UNCHARGED)
                    .map_err(|error| native_core_error(error.into()))?;
                Ok(NativeOwnedCoreState::from_parts(state, allocation))
            })
            .transpose()
    }

    async fn compare_and_swap_lazy_workspace_with_native_work(
        &self,
        id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<bool, crate::LazyWorkspaceError> {
        work.charge(crate::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })
        .map_err(|error| native_core_error(error.into()))?;
        if replacement.schema_version != crate::lazy_workspace::LAZY_STATE_SCHEMA
            || replacement.workspace_id != id
        {
            return Err(crate::LazyWorkspaceError::NativeRecordIntegrity);
        }
        self.swap_lazy_logged(id, expected_revision, replacement, work.clone())
            .await
            .map_err(native_core_error)
    }

    async fn load_lazy_overlay_with_native_work(
        &self,
        id: LazyOverlayId,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<NativeOwnedCoreState<LazyOverlay>>, crate::LazyWorkspaceError> {
        self.load_node::<LazyOverlay>(id, work.clone())
            .await
            .map_err(native_core_error)
    }

    async fn put_lazy_overlay_with_native_work(
        &self,
        id: LazyOverlayId,
        value: NativeOwnedCoreState<LazyOverlay>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), crate::LazyWorkspaceError> {
        self.put_node(id, value, work.clone())
            .await
            .map_err(native_core_error)
    }

    async fn load_lazy_shadow_with_native_work(
        &self,
        id: crate::LazyShadowId,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<NativeOwnedCoreState<crate::LazyShadow>>, crate::LazyWorkspaceError> {
        self.load_node::<crate::LazyShadow>(id, work.clone())
            .await
            .map_err(native_core_error)
    }

    async fn put_lazy_shadow_with_native_work(
        &self,
        id: crate::LazyShadowId,
        value: NativeOwnedCoreState<crate::LazyShadow>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), crate::LazyWorkspaceError> {
        self.put_node(id, value, work.clone())
            .await
            .map_err(native_core_error)
    }
}
