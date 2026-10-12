//! Caller-authorized destination merges.

use super::*;

impl ControlPlane {
    #[cfg(test)]
        pub(crate) async fn agent_merge(&mut self, input: Value) -> Result<Value, String> {
            let caller = self.caller(&input)?;
            self.agent_merge_as(&caller, input).await
        }

    pub(crate) async fn agent_merge_as(
            &mut self,
            caller: &str,
            input: Value,
        ) -> Result<Value, String> {
            let agent = string(&input, "agent")?;
            self.ensure_agent_idle(&agent)?;
            self.ensure_agent_idle(caller)?;
            let route = self
                .state
                .routes
                .get(&agent)
                .cloned()
                .ok_or("unknown agent")?;
            let registry = WorkspaceContextRegistry::new(self.store.clone());
            let parent_context_id = self.context_for_agent(caller)?;
            if self
                .pending_conflict_for_parent(parent_context_id)
                .await?
                .is_some()
            {
                return Err(
                    "this parent workspace already has an unresolved agent merge; use `acyclic git merge --continue` or `--abort`"
                        .to_owned(),
                );
            }
            let child_context = registry
                .authorize_parent(
                    WorkspaceContextId::from_bytes(route.context_id),
                    parent_context_id,
                )
                .await
                .map_err(display)?;
            let parent_context = registry.resolve(parent_context_id).await.map_err(display)?;
            if let Some(mount) = self.mounts.get(&agent) {
                mount
                    .sync()
                    .await
                    .map_err(|error| format!("cannot synchronize child mount before merge: {error}"))?;
            }
            self.sync_agent(caller)
                .await
                .map_err(|error| format!("cannot synchronize parent before merge: {error}"))?;
            let mut roots = BTreeMap::new();
            let mut resolutions = BTreeMap::new();
            let mut target_heads = BTreeMap::new();
            for (root_id, child_root) in &child_context.roots {
                let parent_root = parent_context
                    .roots
                    .get(root_id)
                    .ok_or_else(|| "child context contains a root absent from its parent".to_owned())?;
                if child_root.parent_workspace_id != Some(parent_root.workspace_id) {
                    return Err("child root is not forked from its direct parent root".to_owned());
                }
                let source = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, child_root.workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(display)?;
                let target = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, parent_root.workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(display)?;
                let lazy_source = self.lazy_workspace_root(&route, *root_id).await?;
                // The child's generation holds exactly what it authored; what it
                // only reads from the physical root is no change of its own, as
                // the parent's generations hold only what the parent authored.
                // Source paths the child deleted travel as source deletions.
                let plan = source.join_into(&target).plan().await.map_err(display)?;
                let target_head = plan.target_head();
                let parent_repository_id = self.parent_repository_id(caller, *root_id, target.id())?;
                let tracked = GitCompatRepository::new(parent_repository_id, self.store.clone())
                    .tracked_paths()
                    .await
                    .map_err(display)?;
                let base = target
                    .generation(plan.common_ancestor(), WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                    .map_err(display)?;
                let source_head = source
                    .generation(plan.source_head(), WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                    .map_err(display)?;
                let changed = base
                    .diff_to(&source_head, u32::MAX, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                    .await.map_err(|failure| failure.error)
                    .map_err(display)?
                    .changed_paths(u32::MAX, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                    .map_err(display)?;
                let child_wins_bindings = git_ignore_policy(&lazy_source)
                    .await?
                    .newly_ignored_paths_at(&source_head, &changed, &tracked)
                    .await
                    .map_err(display)?;
                roots.insert(
                    *root_id,
                    MultiRootMergeRoot {
                        source_workspace_id: source.id(),
                        merge_workspace_id: None,
                        source_generation: plan.source_head(),
                        target_workspace_id: target.id(),
                        target_generation: target_head,
                        base_generation: plan.common_ancestor(),
                        child_wins_bindings,
                    },
                );
                resolutions.insert(*root_id, BTreeMap::new());
                target_heads.insert(*root_id, target_head);
            }
            if roots.is_empty() {
                return Err("child context has no roots to merge".to_owned());
            }
            let mut operation_hasher = blake3::Hasher::new();
            operation_hasher.update(b"acyclic-agent-merge-v1\0");
            operation_hasher.update(&route.context_id);
            operation_hasher.update(&parent_context_id.into_bytes());
            for (root_id, root) in &roots {
                operation_hasher.update(&root_id.into_bytes());
                operation_hasher.update(root.source_generation.digest().as_bytes());
                operation_hasher.update(root.target_generation.digest().as_bytes());
                for path in &root.child_wins_bindings {
                    operation_hasher.update(&(path.len() as u64).to_le_bytes());
                    operation_hasher.update(path.as_bytes());
                }
            }
            let mut operation_bytes = [0_u8; 16];
            operation_bytes.copy_from_slice(&operation_hasher.finalize().as_bytes()[..16]);
            let operation_id = OperationId::from_bytes(operation_bytes);
            let merge_plan = MultiRootMergePlan {
                operation_id,
                parent_context_id,
                child_context_id: WorkspaceContextId::from_bytes(route.context_id),
                roots,
            };
            let candidate = MultiRootMergeCandidate {
                plan: merge_plan,
                resolutions,
            };
            let coordinator = self.publication_coordinator().await?;
            let publication = match coordinator.publish_retained(candidate).await {
                Ok(publication) => publication,
                Err(MultiRootPublicationError::Publisher(
                    MaterializingWorkspaceMultiRootPublisherError::Workspace(
                        WorkspaceMultiRootPublisherError::Conflicted { plan },
                    ),
                )) => {
                    return Ok(json!({
                        "status":"conflicted",
                        "conflictCount":plan.conflicts.len(),
                        "conflicts":plan.conflicts.iter().map(typed_conflict_json).collect::<Vec<_>>(),
                        "truncated":plan.truncated
                    }));
                }
                Err(error) => return Err(error.to_string()),
            };
            let applied_publication = match publication {
                Publication::StaleBeforeCommit(_) => {
                    return Ok(json!({"status":"stale-target"}));
                }
                Publication::Paused(publication) => {
                    return Ok(json!({
                        "status":"paused",
                        "root":publication.paused_root.map(|id| hex::encode(id.into_bytes()))
                    }));
                }
                Publication::Conflicted(publication) => {
                    if let Some(parent_mount) = self.mounts.get(caller) {
                        parent_mount.advance_to_head().await.map_err(display)?;
                    }
                    let conflicts = publication
                        .conflicts
                        .values()
                        .flat_map(|plan| plan.conflicts.iter().map(typed_conflict_json))
                        .collect::<Vec<_>>();
                    return Ok(json!({
                        "status":"conflicted",
                        "operation":hex::encode(publication.candidate.plan.operation_id.into_bytes()),
                        "conflictCount":conflicts.len(),
                        "conflicts":conflicts,
                        "truncated":publication.conflicts.values().any(|plan| plan.truncated),
                        "help":"Resolve projected files with ordinary edits, declare every resolved path with `acyclic git add <paths>`, then run `acyclic git merge --continue`; use `acyclic git merge --abort` to restore the pre-merge working tree."
                    }));
                }
                Publication::Applied(publication) => publication,
            };
            if let Some(parent_mount) = self.mounts.get(caller) {
                parent_mount
                    .advance_to_head()
                    .await
                    .map_err(|error| format!("cannot advance parent mount after merge: {error}"))?;
            }
            #[cfg(test)]
            if self.fail_before_publication_history {
                self.fail_before_publication_history = false;
                return Err("injected failure before compatibility history".to_owned());
            }
            let route_root_id = route.root_id;
            self.finalize_applied_publication(&coordinator, caller, &agent, &applied_publication)
                .await?;
            let mut generations = BTreeMap::new();
            let mut changed = false;
            for (root_id, parent_root) in &parent_context.roots {
                let target = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, parent_root.workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(display)?;
                let generation = target.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(display)?.id();
                changed |= target_heads
                    .get(root_id)
                    .is_some_and(|before| *before != generation);
                generations.insert(
                    hex::encode(root_id.into_bytes()),
                    hex::encode(generation.digest().as_bytes()),
                );
            }
            let generation = generations
                .get(&hex::encode(route_root_id))
                .cloned()
                .ok_or_else(|| "routed parent root disappeared from its context".to_owned())?;
            Ok(json!({
                "status": if changed { "applied" } else { "no-changes" },
                "generation":generation,
                "roots":generations
            }))
        }

}
