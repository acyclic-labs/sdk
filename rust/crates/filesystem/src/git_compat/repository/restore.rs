use super::*;

impl<S: GitCompatStore> GitCompatRepository<S> {
    /// Restores selected paths from a pinned published working tree.
    ///
    /// The source and selection occupy the same durable transition as a
    /// commit-based restore. Recovery therefore uses the original source cut.
    pub fn restore_paths_from<'a, E: GitFilesystemExecutor>(
        &'a self,
        source: GitTreeRef,
        paths: Vec<String>,
        workspace_tree: GitTreeRef,
        executor: &'a E,
    ) -> impl Future<Output = Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>>> + Send + 'a
    {
        Box::pin(self.restore_paths(source, paths, workspace_tree, executor))
    }

    async fn restore_paths<E: GitFilesystemExecutor>(
        &self,
        source: GitTreeRef,
        paths: Vec<String>,
        workspace_tree: GitTreeRef,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        Box::pin(self.validate_workspace_tree(workspace_tree)).await?;
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        let output = self
            .prepare_validated(|_| {
                Ok(GitCommandOutput::Action(
                    GitFilesystemAction::RestorePaths {
                        tree: source,
                        paths: paths.clone(),
                    },
                ))
            })
            .await?;
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        let GitCommandOutput::Prepared { transition, action } = output else {
            return Err(GitCompatError::InvalidState.into());
        };
        self.finish(transition, &action, executor).await
    }
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    use crate::git_compat::tests::{TestExecutor, tree, workspace};

    #[test]
    fn restore_boundary_keeps_the_state_machine_off_the_callers_stack() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
        let executor = TestExecutor::failing();
        let boundary =
            repository.restore_paths_from(tree(2), vec!["/selected".into()], tree(1), &executor);
        let body = repository.restore_paths(tree(2), vec!["/selected".into()], tree(1), &executor);
        let validation = repository.validate_workspace_tree(tree(1));
        let preparation = repository.prepare_validated(|_| Ok(GitCommandOutput::NoOp));
        let action = GitFilesystemAction::RestorePaths {
            tree: tree(2),
            paths: vec!["/selected".into()],
        };
        let settlement =
            repository.finish(GitTransitionId::from_bytes([1; 16]), &action, &executor);
        eprintln!(
            "published restore futures: boundary {}, body {}, validation {}, preparation {}, settlement {} bytes",
            std::mem::size_of_val(&boundary),
            std::mem::size_of_val(&body),
            std::mem::size_of_val(&validation),
            std::mem::size_of_val(&preparation),
            std::mem::size_of_val(&settlement),
        );
        assert!(std::mem::size_of_val(&boundary) < 1024);
        assert!(std::mem::size_of_val(&body) < 4096);
        assert!(std::mem::size_of_val(&preparation) < 128);
        assert!(std::mem::size_of_val(&settlement) < 128);
    }
}
