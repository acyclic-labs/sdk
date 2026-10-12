use super::*;

impl GitCompatStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(&self, workspace_id: WorkspaceId) -> Result<Option<GitCompatState>, Self::Error> {
        self.load_record(GIT_COMPAT_NAMESPACE, workspace_id.into_bytes())
            .await
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: GitCompatState,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_record(
            GIT_COMPAT_NAMESPACE,
            workspace_id.into_bytes(),
            expected_revision,
            replacement,
        )
        .await
    }

    async fn compare_and_delete(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
    ) -> Result<bool, Self::Error> {
        self.compare_and_delete_record::<GitCompatState>(
            GIT_COMPAT_NAMESPACE,
            workspace_id.into_bytes(),
            expected_revision,
        )
        .await
    }
}

impl MultiRootPublicationStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MultiRootPublication>, Self::Error> {
        self.load_record(MULTI_ROOT_PUBLICATION_FAMILY, operation_id.into_bytes())
            .await
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MultiRootPublication,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_record(
            MULTI_ROOT_PUBLICATION_FAMILY,
            operation_id.into_bytes(),
            expected_revision,
            replacement,
        )
        .await
    }

    async fn list_operations(&self) -> Result<Vec<OperationId>, Self::Error> {
        self.transaction(|namespace| namespace.operation_records(MULTI_ROOT_PUBLICATION_FAMILY))
            .await
    }

    async fn compare_and_delete(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
    ) -> Result<bool, Self::Error> {
        self.compare_and_delete_record::<MultiRootPublication>(
            MULTI_ROOT_PUBLICATION_FAMILY,
            operation_id.into_bytes(),
            expected_revision,
        )
        .await
    }

    async fn claim_parent(
        &self,
        parent_context_id: WorkspaceContextId,
        operation_id: OperationId,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |namespace| {
            let mut paths = namespace.record(
                MULTI_ROOT_PARENT_CLAIM_FAMILY,
                &parent_context_id.into_bytes(),
            )?;
            with_lock(&mut paths, |paths| {
                if let Some((claim, _)) = read_recoverable::<MultiRootParentClaim>(paths)? {
                    return Ok(claim.operation_id == operation_id);
                }
                write_journaled(paths, &MultiRootParentClaim { operation_id })?;
                Ok(true)
            })
        })
        .await
    }

    async fn release_parent(
        &self,
        parent_context_id: WorkspaceContextId,
        operation_id: OperationId,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |namespace| {
            let mut paths = namespace.record(
                MULTI_ROOT_PARENT_CLAIM_FAMILY,
                &parent_context_id.into_bytes(),
            )?;
            with_lock(&mut paths, |paths| {
                if read_recoverable::<MultiRootParentClaim>(paths)?
                    .is_some_and(|(claim, _)| claim.operation_id == operation_id)
                {
                    remove_record(paths)?;
                }
                Ok(true)
            })
        })
        .await
    }
}
