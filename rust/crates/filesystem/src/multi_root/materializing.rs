use super::*;

impl<A, O, R, M> MultiRootPublisher for MaterializingWorkspaceMultiRootPublisher<A, O, R, M>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    R: WorkspaceResolver<A, O>,
    M: MultiRootMaterializer<A, O>,
{
    type Error = MaterializingWorkspaceMultiRootPublisherError<R::Error, M::Error>;

    async fn prepare(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
    ) -> Result<MultiRootPrepare, Self::Error> {
        self.publisher
            .prepare(operation_id, root_id, root, resolutions)
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Workspace)
    }

    async fn release(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        fence: &MultiRootFence,
    ) -> Result<(), Self::Error> {
        self.publisher
            .release(operation_id, root_id, fence)
            .await
            .map_err(Into::into)
    }

    async fn project_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: Option<&crate::MergePlan>,
    ) -> Result<GenerationId, Self::Error> {
        let to = self
            .publisher
            .project_conflict(operation_id, root_id, root, conflict)
            .await?;
        let workspace = self
            .publisher
            .resolver()
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        self.materializer
            .materialize(
                materialization_operation_id(operation_id, root_id),
                root_id,
                &workspace,
                root.target_generation,
                to,
            )
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Materializer)?;
        Ok(to)
    }

    async fn abort_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> Result<GenerationId, Self::Error> {
        let workspace = self
            .publisher
            .resolver()
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let from = workspace
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(WorkspaceMultiRootPublisherError::Workspace)?
            .id();
        let to = self
            .publisher
            .abort_conflict(operation_id, root_id, root, projected_generation)
            .await?;
        self.materializer
            .materialize(
                conflict_abort_materialization_operation_id(operation_id, root_id),
                root_id,
                &workspace,
                from,
                to,
            )
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Materializer)?;
        self.materializer
            .finalize(
                conflict_abort_materialization_operation_id(operation_id, root_id),
                root_id,
            )
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Materializer)?;
        Ok(to)
    }

    async fn declare_conflicts(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: &crate::MergePlan,
        paths: &[String],
    ) -> Result<BTreeSet<ConflictKey>, Self::Error> {
        self.publisher
            .declare_conflicts(operation_id, root_id, root, conflict, paths)
            .await
            .map_err(Into::into)
    }

    async fn finish_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> Result<MultiRootConflictFinish, Self::Error> {
        self.publisher
            .finish_conflict(operation_id, root_id, root, projected_generation)
            .await
            .map_err(Into::into)
    }

    async fn publish(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
        fence: &MultiRootFence,
    ) -> Result<MultiRootPublishRoot, Self::Error> {
        let published = self
            .publisher
            .publish(operation_id, root_id, root, resolutions, fence)
            .await?;
        if !matches!(
            published,
            MultiRootPublishRoot::Published(_) | MultiRootPublishRoot::AlreadyPublished(_)
        ) {
            return Ok(published);
        }
        let workspace = self
            .publisher
            .resolver()
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let key = publisher_key(operation_id, root_id, b"publish");
        let to = if let Some(generation) = workspace
            .operation_generation(key, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(WorkspaceMultiRootPublisherError::Workspace)?
        {
            generation.id()
        } else {
            let head = workspace
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
                .map_err(WorkspaceMultiRootPublisherError::Workspace)?;
            if head.id() != root.target_generation {
                return Err(
                    MaterializingWorkspaceMultiRootPublisherError::MissingPublishedGeneration,
                );
            }
            head.id()
        };
        self.materializer
            .materialize(
                materialization_operation_id(operation_id, root_id),
                root_id,
                &workspace,
                root.target_generation,
                to,
            )
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Materializer)?;
        Ok(published)
    }

    async fn finalize(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        _root: &MultiRootMergeRoot,
    ) -> Result<(), Self::Error> {
        self.materializer
            .finalize(materialization_operation_id(operation_id, root_id), root_id)
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Materializer)
    }
}
