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
            for _ in 0..2 {
                if let Some(claim) = read_locked::<MultiRootParentClaim>(&mut paths)? {
                    return Ok(claim.operation_id == operation_id);
                }
                if compare_and_swap(
                    &mut paths,
                    0,
                    &MultiRootParentClaim {
                        revision: 1,
                        operation_id,
                    },
                )? {
                    return Ok(true);
                }
            }
            Ok(false)
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
            let Some(claim) = read_locked::<MultiRootParentClaim>(&mut paths)? else {
                return Ok(true);
            };
            if claim.operation_id != operation_id {
                return Ok(true);
            }
            compare_and_delete::<MultiRootParentClaim>(&mut paths, claim.revision)
        })
        .await
    }
}
