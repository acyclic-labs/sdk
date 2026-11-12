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
        stat: bool,
    ) -> crate::Result<super::Output<P, O>> {
        let (target, target_view) = self.agent_view(branch, current).await?;
        // Both sides must include the same inherited source namespace before
        // attributing parent changes against the refined common ancestor.
        let parent_tree = self.lazy_current.tree(true, self.permit).await?;
        let target_tree = target_view
            .view()
            .tree(
                true,
                if target.workspace_id == self.current.id() {
                    self.permit
                } else {
                    PublicationPermit::Unrestricted
                },
            )
            .await?;
        let tracked_paths = GitCompatRepository::new(self.repository_id, self.store.clone())
            .tracked_paths()
            .await
            .map_err(super::errors::compat_error::<S>)?
            .union(&target.tracked_paths)
            .cloned()
            .collect();
        let child = self.generation(target_tree).await?;
        let parent = self.generation(parent_tree).await?;
        let cancellation = CancellationToken::new();
        let plan = child
            .workspace()
            .join_into(&parent.workspace())
            .plan_at(&child, &parent, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| Error::from(failure.error))?
            .value;
        let mut base = self
            .workspace(plan.common_ancestor_workspace_id())
            .await?
            .generation(plan.common_ancestor(), WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| Error::from(failure.error))?
            .value;
        #[cfg(not(target_arch = "wasm32"))]
        if let OwnedView::Checkout(witness) = &target_view {
            let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
            if let Some(origin) = witness.state(&mut accounting).await?.fork_origin
                && origin.snapshot.authored_generation == plan.common_ancestor()
            {
                let owner = self
                    .lazy_current
                    .related(self.workspace(origin.snapshot.workspace_id).await?)
                    .await?;
                let OwnedView::Checkout(owner) = owner else {
                    return Err(Error::Conflict("fork base has no source binding".into()));
                };
                // Ignore rules select each side's reported changes, not the
                // inherited source facts shared by both sides of the fork.
                let candidates = git_restore_paths(&base, &child, None, &cancellation).await?;
                let paths = candidates
                    .into_iter()
                    .map(|path| format!("/{path}"))
                    .collect::<Vec<_>>();
                base = owner
                    .exactify_observed_base(origin.snapshot, &paths, witness, &mut accounting)
                    .await?;
            }
        }
        self.validate_lease().await?;
        let diff = git_compatible_diff(
            &base,
            &child,
            &self.repository,
            &tracked_paths,
            None,
            &cancellation,
        )
        .await?;
        let mut value = json!({
            "from": hex::encode(base.id().to_bytes().as_slice()),
            "to": hex::encode(child.id().to_bytes().as_slice()),
            "parent": hex::encode(parent.id().to_bytes().as_slice()),
            "fileChanges": diff.counts.file_changes,
            "bindingChanges": diff.counts.binding_changes,
        });
        if !stat {
            let parent_diff = git_compatible_diff(
                &base,
                &parent,
                &self.repository,
                &tracked_paths,
                None,
                &cancellation,
            )
            .await?;
            let parent_paths = parent_diff
                .paths
                .iter()
                .map(|change| change.path.as_str())
                .collect::<BTreeSet<_>>();
            let child_paths = diff
                .paths
                .iter()
                .map(|change| change.path.as_str())
                .collect::<BTreeSet<_>>();
            value["parentPaths"] = serde_json::Value::Array(parent_diff.paths.iter().map(|change| json!({
                "path":change.path, "status":change.status,
                "side":if child_paths.contains(change.path.as_str()) { "both" } else { "parent" },
            })).collect());
            value["paths"] = serde_json::Value::Array(
                diff.paths
                    .iter()
                    .map(|change| json!({"path":change.path,"status":change.status,"side": if parent_paths.contains(change.path.as_str()) { "both" } else { "child" }}))
                    .collect(),
            );
        }
        self.validate_lease().await?;
        Ok(super::Output {
            command: Some(GitCommandOutput::Filesystem(GitFilesystemResult::Data {
                kind: "diff".into(),
                value,
            })),
            switched: None,
        })
    }

    async fn agent_view(
        &self,
        branch: &str,
        current: GitTreeRef,
    ) -> crate::Result<(acyclic_fs::GitBranch, OwnedView<P, O>)> {
        let repository = GitCompatRepository::new(self.repository_id, self.store.clone());
        // The normal repository entry point validates both the current view
        // identity and the operation lease before exposing its branch catalog.
        let catalog = repository
            .run(GitCommand::Branch { create: None }, current.clone(), self)
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
        Ok((target, target_view))
    }

    pub(super) async fn show_agent(
        &self,
        branch: &str,
        path: &str,
        offset: u64,
        current: GitTreeRef,
    ) -> crate::Result<super::Output<P, O>> {
        let (_, view) = self.agent_view(branch, current).await?;
        let path = workspace_path_argument(Path::new("."), path)?;
        self.validate_lease().await?;
        let result = match view {
            #[cfg(not(target_arch = "wasm32"))]
            OwnedView::Checkout(view) => {
                let cancellation = CancellationToken::new();
                let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
                let (bytes, total) = view
                    .read_page(
                        &path,
                        offset,
                        super::actions::SHOW_PAGE_BYTES,
                        &mut accounting,
                    )
                    .await?;
                super::actions::shown_file(
                    path.trim_start_matches('/'),
                    bytes.bytes.as_ref(),
                    offset,
                    total,
                )
            }
            OwnedView::Volume(view) => {
                let tree = View::Volume(&view).tree(false, self.permit).await?;
                self.show_file(&tree, path.trim_start_matches('/'), offset)
                    .await?
            }
        };
        self.validate_lease().await?;
        Ok(super::Output {
            command: Some(GitCommandOutput::Filesystem(result)),
            switched: None,
        })
    }
}
