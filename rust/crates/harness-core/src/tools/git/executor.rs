use super::*;

impl<O, S> GitFilesystemExecutor for WorkspaceGitExecutor<'_, O, S>
where
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
{
    type Error = Error;

    fn validate(&self) -> acyclic_stream::BoxProviderFuture<'_, Result<(), Self::Error>> {
        acyclic_stream::erase_provider_future(move || self.validate_lease())
    }

    fn validate_merge_resolution<'a>(
        &'a self,
        operation: OperationId,
        action: &'a GitFilesystemAction,
        tree: GitTreeRef,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<GitFilesystemResult, Self::Error>> {
        acyclic_stream::erase_provider_future(move || {
            self.validated_merge_resolution(operation, action, tree)
        })
    }

    fn execute<'a>(
        &'a self,
        operation_id: OperationId,
        action: &'a GitFilesystemAction,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<GitFilesystemResult, Self::Error>> {
        acyclic_stream::erase_provider_future(move || self.perform(operation_id, action))
    }
}

impl<O, S> WorkspaceGitExecutor<'_, O, S>
where
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
{
    pub(super) async fn validate_lease(&self) -> Result<(), Error> {
        (self.before_effect)().await?;
        self.access.require(&self.current).await?;
        let Some(lease) = &self.lease else {
            return Ok(());
        };
        let snapshot = OperationWindowCoordinator::new(self.fs.operation_window_store())
            .snapshot(lease.workspace_id)
            .await
            .map_err(Error::from)?;
        let active = match snapshot.phase {
            acyclic_fs::OperationWindowPhase::Active { leases, .. } => {
                leases.get(&lease.lease_id).is_some_and(|active| {
                    active.expires_at_millis == lease.expires_at_millis
                        && active.expires_at_millis > now_millis()
                })
            }
            acyclic_fs::OperationWindowPhase::Idle
            | acyclic_fs::OperationWindowPhase::Reconciling { .. } => false,
        };
        if active {
            Ok(())
        } else {
            Err(Self::error("Git compatibility command lease expired"))
        }
    }

    async fn validated_merge_resolution(
        &self,
        operation: OperationId,
        action: &GitFilesystemAction,
        tree: GitTreeRef,
    ) -> Result<GitFilesystemResult, Error> {
        self.validate_lease().await?;
        let GitFilesystemAction::Join {
            target_tree,
            tracked_paths,
            ..
        } = action
        else {
            return Err(Error::Invalid("merge continuation requires a join".into()));
        };
        let pending = GitCompatRepository::new(self.repository_id, self.store.clone())
            .pending_transition()
            .await
            .map_err(super::errors::compat_error::<S>)?
            .filter(|pending| pending.id.operation_id() == operation)
            .ok_or_else(|| Self::error("Git merge transition changed"))?;
        let acyclic_fs::GitPendingMutation::Join {
            conflicts: Some(paths),
            ..
        } = &pending.mutation
        else {
            return Err(Self::error(
                "Git merge has unresolved entries; conflicts have not been inspected",
            ));
        };
        let current = self.generation(tree).await?;
        let Some(resolved) =
            acyclic_fs::git_merge_resolution(&current, paths, tracked_paths).await?
        else {
            return Err(Self::error(
                "Git merge still contains unresolved conflict markers",
            ));
        };
        // A continued join tracks the paths the join itself published, as an
        // unconflicted join does: never a reserved path, and never a file the
        // resolution added or edited. The joined generation is the immutable
        // one this operation published, so later source writes cannot change
        // what the merge tracks. A transition that never published admits no
        // new paths.
        let before = self.generation(*target_tree).await?;
        let joined = self
            .current
            .operation_generation(
                IdempotencyKey::from_bytes(operation.into_bytes()),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| Error::from(failure.error))?;
        let tracked_paths = acyclic_fs::git_tracked_paths_after(
            &before,
            joined.as_ref().unwrap_or(&before),
            &current,
            &self.repository,
            &resolved,
            u32::MAX,
            &CancellationToken::new(),
        )
        .await
        .map_err(Error::from)?;
        self.validate_lease().await?;
        Ok(GitFilesystemResult::Applied {
            tree: Some(tree),
            tracked_paths: Some(tracked_paths),
        })
    }

    async fn perform(
        &self,
        operation_id: OperationId,
        action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult, Error> {
        match action {
            GitFilesystemAction::CaptureCommit {
                workspace_tree,
                head_tree,
                head_workspace_tree,
                tracked_paths,
                ..
            } => {
                self.capture_commit(
                    operation_id,
                    workspace_tree,
                    head_tree.as_ref(),
                    head_workspace_tree.as_ref().as_ref(),
                    tracked_paths,
                )
                .await
            }
            GitFilesystemAction::ForkBranch {
                branch,
                source_tree,
                switch,
                ..
            } => {
                self.fork_branch(operation_id, branch, source_tree, *switch)
                    .await
            }
            GitFilesystemAction::SwitchWorkspace { workspace_id } => {
                self.switch_workspace(workspace_id).await
            }
            GitFilesystemAction::Diff {
                from,
                to,
                tracked_paths,
            } => self.diff(from.as_ref(), to, tracked_paths).await,
            GitFilesystemAction::RestoreGeneration { tree, paths } => {
                self.restore_generation(operation_id, tree, paths.as_ref())
                    .await
            }
            GitFilesystemAction::RestorePaths { tree, paths } => {
                self.restore_paths(operation_id, *tree, paths).await
            }
            GitFilesystemAction::Join {
                target_tree,
                source_workspace: source,
                rebase,
                tracked_paths: tracked,
            } => {
                // The join is the deepest facade action; it is erased so it
                // never sizes, or deepens, every other command's future.
                acyclic_stream::erase_provider_future(|| {
                    self.join(operation_id, *target_tree, *source, *rebase, tracked)
                })
                .await
            }
            GitFilesystemAction::ApplyCommit {
                base,
                source,
                paths,
                tracked_paths,
                ..
            } => {
                self.apply_commit(
                    operation_id,
                    base.as_ref(),
                    source.as_ref(),
                    paths,
                    tracked_paths,
                )
                .await
            }
            GitFilesystemAction::Archive { tree } => self.archive(tree).await,
            GitFilesystemAction::Grep {
                pattern,
                path,
                tree,
            } => self.grep(pattern, path.as_deref(), tree).await,
            GitFilesystemAction::Blame { path, commits } => self.blame(path, commits).await,
            GitFilesystemAction::Clean {
                dry_run,
                tree,
                tracked_paths,
            } => {
                self.clean(operation_id, *dry_run, tree, tracked_paths)
                    .await
            }
            GitFilesystemAction::ApplyPatch { patch } => {
                self.apply_patch(operation_id, patch).await
            }
            GitFilesystemAction::CheckIgnore { paths, tree } => {
                self.check_ignore(paths, tree).await
            }
            GitFilesystemAction::ShowFile { tree, path } => self.show_file(tree, path).await,
            unsupported => Err(Self::error(format!(
                "Git filesystem action is not yet available in the Codex adapter: {unsupported:?}"
            ))),
        }
    }
}
