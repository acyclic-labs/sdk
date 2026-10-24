//! Multi-root publication and recovery.

use super::*;

impl ControlPlane {
    pub(crate) async fn publication_coordinator(
        &self,
    ) -> Result<acyclic_harness::filesystem::WorkspacePublicationCoordinator, String> {
        if self.roots.is_empty() {
            return Err("root workspace is unavailable".to_owned());
        }
        let root_context = WorkspaceContextRegistry::new(self.store.clone())
            .resolve(WorkspaceContextId::from_bytes(self.state.root_context_id))
            .await
            .map_err(display)?;
        let physical_roots = root_context
            .roots
            .into_iter()
            .map(|(root_id, root)| {
                let binding = self
                    .state
                    .roots
                    .get(&root_key(root_id))
                    .ok_or_else(|| "publication has no original root binding".to_owned())?;
                let creation = binding.created()?;
                creation.validate()?;
                if binding.path != root.source_path {
                    return Err("publication root differs from original configured root".to_owned());
                }
                Ok((
                    root_id,
                    acyclic_harness::filesystem::CheckoutRoot {
                        workspace_id: root.workspace_id,
                        path: root.source_path,
                        source_root_identity: acyclic_fs::NativeRootIdentity::from_bytes(
                            creation.native_root_identity,
                        ),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, String>>()?;
        let views = self
            .state
            .roots
            .values()
            .map(|binding| {
                let root = WorkspaceRootId::from_bytes(binding.root_id);
                let workspace = self
                    .roots
                    .get(&root_key(root))
                    .ok_or_else(|| "root workspace is unavailable".to_owned())?;
                Ok((root, workspace))
            })
            .collect::<Result<Vec<_>, String>>()?;
        acyclic_harness::filesystem::workspace_publication_coordinator(
            self.fs.clone(),
            self.store.clone(),
            physical_roots,
            views,
        )
        .await
        .map_err(display)
    }

    pub(crate) async fn pending_conflict_for_parent(
        &self,
        parent_context_id: WorkspaceContextId,
    ) -> Result<Option<OperationId>, String> {
        let mut found = None;
        for operation_id in
            <LocalCoreStateStore as acyclic_fs::MultiRootPublicationStore>::list_operations(
                &self.store,
            )
            .await
            .map_err(display)?
        {
            let Some(publication) =
                <LocalCoreStateStore as acyclic_fs::MultiRootPublicationStore>::load(
                    &self.store,
                    operation_id,
                )
                .await
                .map_err(display)?
            else {
                continue;
            };
            if publication.candidate.plan.parent_context_id == parent_context_id
                && matches!(
                    publication.phase,
                    MultiRootPublicationPhase::Conflicted | MultiRootPublicationPhase::Aborting
                )
                && found.replace(operation_id).is_some()
            {
                return Err(
                    "parent context has multiple unresolved merge records; recovery is required"
                        .to_owned(),
                );
            }
        }
        Ok(found)
    }

    pub(crate) fn parent_repository_id(
        &self,
        parent_agent: &str,
        root_id: WorkspaceRootId,
        workspace_id: acyclic_fs::WorkspaceId,
    ) -> Result<acyclic_fs::WorkspaceId, String> {
        let repository_workspace_id = if parent_agent == self.state.root_agent_id {
            self.state
                .roots
                .get(&root_key(root_id))
                .ok_or_else(|| "published root binding is unavailable".to_owned())?
                .created()?
                .repository_workspace_id
        } else {
            self.state
                .routes
                .get(parent_agent)
                .ok_or_else(|| "published parent route is unavailable".to_owned())?
                .roots
                .get(&root_key(root_id))
                .ok_or_else(|| "published parent root is unavailable".to_owned())?
                .repository_workspace_id
        };
        Ok(repository_id(
            workspace_id.into_bytes(),
            repository_workspace_id,
        ))
    }

    pub(crate) async fn record_publication_history(
        &self,
        parent_agent: &str,
        child_agent: &str,
        publication: &MultiRootPublication,
    ) -> Result<(), String> {
        let child_route = self
            .state
            .routes
            .get(child_agent)
            .ok_or_else(|| "published child route is unavailable".to_owned())?;
        let repositories = publication
            .candidate
            .plan
            .roots
            .iter()
            .map(|(root_id, root)| {
                let parent =
                    self.parent_repository_id(parent_agent, *root_id, root.target_workspace_id)?;
                let binding = child_route
                    .roots
                    .get(&root_key(*root_id))
                    .ok_or_else(|| "published child root is unavailable".to_owned())?;
                Ok((
                    *root_id,
                    (
                        parent,
                        repository_id(
                            root.source_workspace_id.into_bytes(),
                            binding.repository_workspace_id,
                        ),
                    ),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, String>>()?;
        acyclic_harness::filesystem::record_agent_publication(
            &self.fs,
            &self.store,
            publication,
            &repositories,
            parent_agent,
            child_agent,
            i64::try_from(now_millis() / 1000).unwrap_or(i64::MAX),
        )
        .await
        .map_err(display)
    }

    pub(crate) fn finalize_applied_publication<'a>(
        &'a mut self,
        coordinator: &'a acyclic_harness::filesystem::WorkspacePublicationCoordinator,
        parent_agent: &str,
        child_agent: &str,
        publication: &'a MultiRootPublication,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send + 'a + use<'a> {
        let parent_agent = parent_agent.to_owned();
        let child_agent = child_agent.to_owned();
        async move {
            self.record_publication_history(&parent_agent, &child_agent, publication)
                .await
                .map_err(|error| format!("recording published compatibility history: {error}"))?;
            // Before the child rebases onto the new head: every path that
            // finishes a merge (a first attempt, `--continue`, recovery) carries
            // the child's deletions of source files, which no generation holds.
            let deletions = self
                .source_deletions(&parent_agent, &child_agent, &publication.candidate.plan)
                .await?;
            self.apply_source_deletions(&parent_agent, deletions)
                .await?;
            let limits = OperationReconcileLimits::default();
            let mut published_heads = BTreeMap::new();
            for (root_id, root) in &publication.candidate.plan.roots {
                let source = acyclic_fs::WorkspaceResolver::resolve(
                    &(self.fs.clone(), WorkspaceGraph::new(self.store.clone())),
                    root.source_workspace_id,
                )
                .await
                .map_err(display)?;
                let rebase_key = derived_idempotency_key(
                    IdempotencyKey::from_bytes(
                        publication.candidate.plan.operation_id.into_bytes(),
                    ),
                    *root_id,
                );
                let generation = match source
                    .live_rebase(
                        rebase_key,
                        limits.maximum_generations,
                        limits.maximum_changes,
                        limits.maximum_conflicts,
                    )
                    .await
                    .map_err(|error| {
                        format!("rebasing published child root {root_id:?}: {error}")
                    })? {
                    acyclic_fs::WorkspaceRebase::Rebased(generation)
                    | acyclic_fs::WorkspaceRebase::AlreadyRebased(generation)
                    | acyclic_fs::WorkspaceRebase::Current(generation) => generation,
                    acyclic_fs::WorkspaceRebase::Stale(_) => {
                        return Err(
                            "child workspace changed while finalizing its publication".to_owned()
                        );
                    }
                    acyclic_fs::WorkspaceRebase::Conflicted { .. } => {
                        return Err(
                            "published child could not advance to its parent result".to_owned()
                        );
                    }
                    acyclic_fs::WorkspaceRebase::Fenced => {
                        return Err("published child rebase was fenced".to_owned());
                    }
                    acyclic_fs::WorkspaceRebase::IdempotencyConflict => {
                        return Err(
                            "published child rebase reused an operation identity".to_owned()
                        );
                    }
                };
                published_heads.insert(*root_id, generation.id());
            }
            if let Some(child_mount) = self.mounts.get(&child_agent) {
                child_mount.advance_to_head().await.map_err(display)?;
            }
            let route = self
                .state
                .routes
                .get_mut(&child_agent)
                .ok_or_else(|| "merged agent route disappeared".to_owned())?;
            for (root_id, generation) in &published_heads {
                let routed = route
                    .roots
                    .get_mut(&root_key(*root_id))
                    .ok_or_else(|| "merged agent route root disappeared".to_owned())?;
                routed.published_generation = *generation.digest().as_bytes();
            }
            self.persist()?;
            coordinator
                .acknowledge_applied(publication)
                .await
                .map_err(display)
        }
    }

    pub(crate) async fn agent_merge_transition(
        &mut self,
        caller: &str,
        abort: bool,
    ) -> Result<Value, String> {
        self.ensure_agent_idle(caller)?;
        self.sync_agent(caller).await?;
        let parent_context_id = self.context_for_agent(caller)?;
        let operation_id = self
            .pending_conflict_for_parent(parent_context_id)
            .await?
            .ok_or_else(|| "no conflicted agent merge is pending in this workspace".to_owned())?;
        let coordinator = self.publication_coordinator().await?;
        let applied = if abort {
            coordinator
                .abort_conflicted(operation_id, parent_context_id)
                .await
                .map_err(display)?;
            None
        } else {
            let Publication::Applied(publication) = coordinator
                .continue_conflicted_retained(operation_id, parent_context_id)
                .await
                .map_err(display)?
            else {
                return Err("conflicted merge did not complete".to_owned());
            };
            Some(publication)
        };
        if let Some(mount) = self.mounts.get(caller) {
            mount.advance_to_head().await.map_err(display)?;
        }
        if let Some(publication) = &applied {
            let child_agent = self
                .state
                .routes
                .values()
                .find(|route| {
                    route.context_id == publication.candidate.plan.child_context_id.into_bytes()
                })
                .map(|route| route.agent_id.clone())
                .ok_or_else(|| "published child route is unavailable".to_owned())?;
            self.finalize_applied_publication(&coordinator, caller, &child_agent, publication)
                .await?;
        }
        self.persist()?;
        Ok(json!({
            "status": if abort { ControlStatus::Aborted } else { ControlStatus::Applied },
            "operation": hex::encode(operation_id.into_bytes())
        }))
    }

    pub(crate) async fn recover_multi_root_publications(&mut self) -> Result<(), String> {
        if self.roots.is_empty() {
            return Ok(());
        }
        let coordinator = self.publication_coordinator().await?;
        for operation_id in coordinator.pending_operations().await.map_err(display)? {
            match coordinator
                .resume_retained(operation_id)
                .await
                .map_err(display)?
            {
                Some(Publication::Applied(publication)) => {
                    let parent = self
                        .agent_for_context(publication.candidate.plan.parent_context_id)
                        .ok_or_else(|| "published parent route is unavailable".to_owned())?;
                    let child = self
                        .agent_for_context(publication.candidate.plan.child_context_id)
                        .ok_or_else(|| "published child route is unavailable".to_owned())?;
                    self.finalize_applied_publication(&coordinator, &parent, &child, &publication)
                        .await?;
                }
                Some(Publication::Paused(_))
                | Some(Publication::Conflicted(_))
                | Some(Publication::StaleBeforeCommit(_))
                | None => {}
            }
        }
        let agents = self.mounts.keys().cloned().collect::<Vec<_>>();
        for agent in agents {
            if let Some(mount) = self.mounts.get(&agent) {
                mount.advance_to_head().await.map_err(display)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn recover_expired_adapter_leases(&mut self) -> Result<(), String> {
        let now = now_millis();
        let expired = self
            .state
            .leases
            .values()
            .filter(|lease| lease.expires_at_millis <= now)
            .cloned()
            .collect::<Vec<_>>();
        if expired.is_empty() {
            return Ok(());
        }
        let recovered_agents = expired
            .iter()
            .map(|record| record.agent_id.clone())
            .collect::<BTreeSet<_>>();
        for agent_id in &recovered_agents {
            if let Some(mount) = self.mounts.remove(agent_id) {
                mount.abandon().map_err(|error| {
                    format!("cannot fence expired agent '{agent_id}' before recovery: {error}")
                })?;
                self.retire(mount);
            }
        }
        for record in expired {
            let route = self
                .state
                .routes
                .get(&record.agent_id)
                .cloned()
                .ok_or_else(|| "expired lease route is missing".to_owned())?;
            for root in record.roots.values() {
                let workspace = self
                    .workspace_root(&route, WorkspaceRootId::from_bytes(root.root_id))
                    .await?;
                OperationWindowCoordinator::new(self.fs.operation_window_store())
                    .recover_workspace(&workspace, now, OperationReconcileLimits::default())
                    .await
                    .map_err(display)?;
            }
        }
        for agent_id in recovered_agents {
            let route = self
                .state
                .routes
                .get(&agent_id)
                .cloned()
                .ok_or_else(|| "recovered lease route is missing".to_owned())?;
            if route.lifecycle.needs_mount() {
                self.mounts
                    .insert(agent_id, self.mount_route(&route).await?);
            }
        }
        self.state
            .leases
            .retain(|_, lease| lease.expires_at_millis > now);
        self.persist()
    }
}
