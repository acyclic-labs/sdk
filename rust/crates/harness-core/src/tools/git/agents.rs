//! Read a registered agent's published view through the repository binding.
use super::*;

impl<P, O, S> WorkspaceGitExecutor<'_, P, O, S>
where
    P: acyclic_fs::distributed::AuthorityProvider + ?Sized + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
{
    pub(super) async fn diff_agent(
        &self,
        branch: &str,
        current: GitTreeRef,
    ) -> crate::Result<super::Output<P, O>> {
        let repository = GitCompatRepository::new(self.repository_id, self.store.clone());
        // The normal repository entry point validates both the current view
        // identity and the operation lease before exposing its branch catalog.
        let catalog = repository
            .run(GitCommand::Branch { create: None }, current, self)
            .await
            .map_err(super::errors::run_error::<S>)?;
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
            // The authorized target view exactifies its own immutable snapshot.
            // A wall-clock lease adds no authority to this read and must not
            // revoke an inspection while native materialization is in flight.
            target_view
                .view()
                .tree(true, PublicationPermit::Unrestricted)
                .await?
        };
        let tracked_paths = repository
            .tracked_paths()
            .await
            .map_err(super::errors::compat_error::<S>)?
            .union(&target.tracked_paths)
            .cloned()
            .collect();
        self.validate_lease().await?;
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
        self.validate_lease().await?;
        Ok(super::Output {
            command: Some(GitCommandOutput::Filesystem(result)),
            switched: None,
        })
    }
}
