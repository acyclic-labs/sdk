//! Read a registered agent's published view through the repository binding.
use super::*;

impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    pub(super) async fn diff_agent(
        &self,
        branch: &str,
        current: GitTreeRef,
    ) -> crate::Result<super::Output> {
        let repository = GitCompatRepository::new(self.repository_id, self.store.clone());
        // The normal repository entry point validates both the current view
        // identity and the operation lease before exposing its branch catalog.
        let catalog = repository
            .run(GitCommand::Branch { create: None }, current, self)
            .await
            .map_err(super::errors::run_error)?;
        let GitCommandOutput::Branches { branches, .. } = catalog else {
            return Err(Error::Conflict(
                "agent branch catalog is unavailable".into(),
            ));
        };
        let target = branches
            .into_iter()
            .find(|candidate| candidate.name == branch)
            .ok_or_else(|| Error::NotFound(branch.into()))?;
        let workspace = self.workspace(target.workspace_id).await?;
        let target_view = self.lazy_current.related(workspace).await?;
        let target_tree = if target.workspace_id == self.current.id() {
            target_view.view().tree(true, self.permit).await?
        } else {
            let coordinator = OperationWindowCoordinator::new(self.fs.operation_window_store());
            let now = now_millis();
            let head = target_view
                .workspace()
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(|failure| Error::from(failure.error))?
                .value;
            let lease = coordinator
                .begin(
                    target.workspace_id,
                    head.id(),
                    "git-agent-diff",
                    now,
                    self.lease
                        .as_ref()
                        .map_or(now.saturating_add(300_000), |lease| lease.expires_at_millis),
                )
                .await
                .map_err(Error::from)?;
            let tree = target_view
                .view()
                .tree(true, lease.publication_permit())
                .await;
            // Retire the target lease on failure too; the caller's permit is
            // never authority to exactify a different agent's native files.
            coordinator
                .finish_workspace(
                    target_view.workspace(),
                    &lease,
                    now_millis(),
                    acyclic_fs::OperationReconcileLimits::default(),
                )
                .await
                .map_err(Error::from)?;
            tree?
        };
        let tracked_paths = repository
            .tracked_paths()
            .await
            .map_err(super::errors::compat_error)?
            .union(&target.tracked_paths)
            .cloned()
            .collect();
        self.validate().await?;
        let result = self
            .execute(
                OperationId::new(),
                &GitFilesystemAction::Diff {
                    from: Some(current),
                    to: target_tree,
                    tracked_paths,
                },
            )
            .await?;
        self.validate().await?;
        Ok(super::Output {
            command: Some(GitCommandOutput::Filesystem(result)),
            switched: None,
        })
    }
}
