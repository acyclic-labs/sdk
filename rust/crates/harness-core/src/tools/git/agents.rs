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
        self.validate().await?;
        let repository = GitCompatRepository::new(self.repository_id, self.store.clone());
        // The repository validates the selected view; this inspection never
        // grants authority to materialize or publish another agent's view.
        let catalog = repository
            .execute(GitCommand::Branch { create: None }, current)
            .await
            .map_err(super::errors::compat_error)?;
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
        let target_tree = {
            if target.workspace_id == self.current.id() {
                current
            } else {
                match self.lazy_current {
                    View::Checkout(source) => GitTreeRef::Lazy(
                        source
                            .inspection_snapshot(
                                &workspace,
                                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                            )
                            .await?,
                    ),
                    View::Volume(_) => {
                        super::readonly::workspace_tree(View::Volume(&workspace)).await?
                    }
                }
            }
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
            .diff_read_only(Some(&current), &target_tree, &tracked_paths)
            .await?;
        self.validate().await?;
        Ok(super::Output {
            command: Some(GitCommandOutput::Filesystem(result)),
            switched: None,
        })
    }
}
