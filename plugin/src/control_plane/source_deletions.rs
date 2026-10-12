//! Lineage-aware source deletion publication.

use super::*;

/// Adds every path that differs from `base` to `head` to `changed`.
async fn changed_paths_into(
    base: &LocalGeneration,
    head: &LocalGeneration,
    changed: &mut BTreeSet<String>,
) -> Result<(), String> {
    if base.id() == head.id() {
        return Ok(());
    }
    for change in base
        .diff_to(head, u32::MAX, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
        .await.map_err(|failure| failure.error)
        .map_err(display)?
        .changed_paths(u32::MAX, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
        .map_err(display)?
    {
        changed.insert(namespace_path_text(&change.path)?);
    }
    Ok(())
}

/// Most source deletions one merge carries into its parent.
const MAXIMUM_SOURCE_DELETIONS: usize = 100_000;

impl ControlPlane {
    /// The source paths a merged child deleted or renamed away, per root:
        /// each while the shared source still holds exactly the version the child
        /// deleted (an ancestor's newer edit is kept), and the child did not
        /// re-create it. A directory's version does not cover its contents, so a
        /// deleted directory is kept whole when the parent or any ancestor changed
        /// anything under it since the fork. Under a non-root parent the parent
        /// must still read the path from the source; a parent that wrote it keeps
        /// its own version. Computed from the published plan, so finishing the
        /// same publication again yields the same deletions.
        pub(super) async fn source_deletions(
            &self,
            parent_agent: &str,
            child_agent: &str,
            plan: &MultiRootMergePlan,
        ) -> Result<BTreeMap<WorkspaceRootId, Vec<String>>, String> {
            let route = self
                .state
                .routes
                .get(child_agent)
                .cloned()
                .ok_or_else(|| "merged agent route is unavailable".to_owned())?;
            let parent_route = (parent_agent != self.state.root_agent_id)
                .then(|| self.state.routes.get(parent_agent).cloned())
                .flatten();
            let mut deletions = BTreeMap::new();
            for (root_id, root) in &plan.roots {
                let view = self.lazy_workspace_root(&route, *root_id).await?;
                let tombstones = view
                    .source_tombstones(MAXIMUM_SOURCE_DELETIONS)
                    .await
                    .map_err(display)?;
                if tombstones.is_empty() {
                    continue;
                }
                let child_head = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, root.source_workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(display)?.generation(root.source_generation, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                    .map_err(display)?;
                let parent_view = match &parent_route {
                    Some(parent_route) => Some(self.lazy_workspace_root(parent_route, *root_id).await?),
                    None => None,
                };
                let changed = self.changed_since_fork(plan, *root_id, root).await?;
                let mut paths = Vec::new();
                for (path, deleted) in tombstones {
                    let beneath = format!("{path}/");
                    if child_head.stat(&path, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_ok()
                        || view.current_source_version(&path).await.map_err(display)? != Some(deleted)
                        || changed
                            .range(beneath.clone()..)
                            .next()
                            .is_some_and(|changed| changed.starts_with(&beneath))
                    {
                        continue;
                    }
                    if let Some(parent_view) = &parent_view
                        && !matches!(
                            parent_view.lookup(&path).await,
                            Ok(acyclic_fs::LazyLookup::Source(ref node)) if node.version == deleted
                        )
                    {
                        continue;
                    }
                    paths.push(path);
                }
                deletions.insert(*root_id, paths);
            }
            Ok(deletions)
        }

    /// Every path the merge's parent changed since the child forked, and
        /// every path each ancestor above it changed since that level forked
        /// (what the shared source shows the child, from further up).
        pub(super) async fn changed_since_fork(
            &self,
            plan: &MultiRootMergePlan,
            root_id: WorkspaceRootId,
            root: &MultiRootMergeRoot,
        ) -> Result<BTreeSet<String>, String> {
            let mut changed = BTreeSet::new();
            let target = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, root.target_workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(display)?;
            changed_paths_into(
                &target
                    .generation(root.base_generation, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                    .map_err(display)?,
                &target
                    .generation(root.target_generation, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                    .map_err(display)?,
                &mut changed,
            )
            .await?;
            let registry = WorkspaceContextRegistry::new(self.store.clone());
            let mut upper = registry
                .resolve(plan.parent_context_id)
                .await
                .map_err(display)?;
            while let Some(grandparent_id) = upper.parent_context_id {
                let grandparent = registry.resolve(grandparent_id).await.map_err(display)?;
                let (Some(upper_child), Some(upper_parent)) =
                    (upper.roots.get(&root_id), grandparent.roots.get(&root_id))
                else {
                    break;
                };
                let upper_source = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, upper_child.workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(display)?;
                let upper_target = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, upper_parent.workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(display)?;
                let fork_point = upper_source
                    .join_into(&upper_target)
                    .plan()
                    .await
                    .map_err(display)?
                    .common_ancestor();
                changed_paths_into(
                    &upper_target.generation(fork_point, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(display)?,
                    &upper_target.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(display)?,
                    &mut changed,
                )
                .await?;
                upper = grandparent;
            }
            Ok(changed)
        }

    /// Deletes in `parent` the source paths a merged child deleted. For the
        /// root that is the physical path itself, which the root's next refresh
        /// records like any other change on disk (the publication still fences
        /// the root workspace here); for any other parent, a removal in its view.
        pub(super) async fn apply_source_deletions(
            &mut self,
            parent: &str,
            deletions: BTreeMap<WorkspaceRootId, Vec<String>>,
        ) -> Result<(), String> {
            self.state.require_created_roots()?;
            let root_parent = parent == self.state.root_agent_id;
            for (root_id, paths) in deletions {
                if paths.is_empty() {
                    continue;
                }
                if root_parent {
                    let base = self
                        .state
                        .roots
                        .get(&root_key(root_id))
                        .map(|binding| binding.path.clone())
                        .ok_or_else(|| "physical root binding is missing".to_owned())?;
                    for path in paths {
                        let host = base.join(path.trim_start_matches('/'));
                        let removed = match fs::symlink_metadata(&host) {
                            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&host),
                            Ok(_) => fs::remove_file(&host),
                            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                            Err(error) => Err(error),
                        };
                        removed
                            .map_err(|error| format!("cannot delete {}: {error}", host.display()))?;
                    }
                } else {
                    let parent_route = self
                        .state
                        .routes
                        .get(parent)
                        .cloned()
                        .ok_or_else(|| "parent route is unavailable".to_owned())?;
                    let view = self.lazy_workspace_root(&parent_route, root_id).await?;
                    for path in &paths {
                        match view.remove(path).await {
                            Ok(()) | Err(acyclic_fs::LazyWorkspaceError::NotFound) => {}
                            Err(error) => return Err(format!("cannot delete {path}: {error}")),
                        }
                    }
                    // Removed around the parent's mount, which learns of it here.
                    if let Some(mount) = self.mounts.get(parent) {
                        mount.changed_outside(&route_name(root_id), &paths)?;
                    }
                }
            }
            Ok(())
        }

}
