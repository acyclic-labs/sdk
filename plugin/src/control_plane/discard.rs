//! Subtree discard and mount retirement.

use super::*;

impl ControlPlane {
    #[cfg(test)]
        pub(crate) async fn agent_discard(&mut self, input: Value) -> Result<Value, String> {
            let caller = self.caller(&input)?;
            self.agent_discard_as(&caller, input).await
        }

    pub(crate) async fn agent_discard_as(
            &mut self,
            caller: &str,
            input: Value,
        ) -> Result<Value, String> {
            self.recover_expired_adapter_leases().await?;
            let agent = string(&input, "agent")?;
            let route = self
                .state
                .routes
                .get(&agent)
                .cloned()
                .ok_or("unknown agent")?;
            if route.parent_agent_id != caller {
                return Err("only the direct parent may discard this workspace".to_owned());
            }
            let descendants = self.descendants(&agent);
            let subtree = descendants
                .iter()
                .cloned()
                .chain(std::iter::once(agent.clone()))
                .collect::<BTreeSet<_>>();
            if self
                .state
                .leases
                .values()
                .any(|lease| subtree.contains(&lease.agent_id))
            {
                return Err(
                    "cannot discard an agent subtree while filesystem tools are active".to_owned(),
                );
            }
            let subtree_contexts = subtree
                .iter()
                .filter_map(|agent_id| self.state.routes.get(agent_id))
                .map(|route| WorkspaceContextId::from_bytes(route.context_id))
                .collect::<BTreeSet<_>>();
            for operation_id in
                <LocalCoreStateStore as acyclic_fs::MultiRootPublicationStore>::list_operations(
                    &self.store,
                )
                .await
                .map_err(display)?
            {
                let publication = <LocalCoreStateStore as acyclic_fs::MultiRootPublicationStore>::load(
                    &self.store,
                    operation_id,
                )
                .await
                .map_err(display)?;
                if publication.is_some_and(|publication| {
                    subtree_contexts.contains(&publication.candidate.plan.child_context_id)
                        || subtree_contexts.contains(&publication.candidate.plan.parent_context_id)
                }) {
                    return Err(
                        "cannot discard an agent subtree while publication recovery is pending"
                            .to_owned(),
                    );
                }
            }
            let abandoned_spawns = self
                .state
                .pending
                .iter()
                .filter(|spawn| subtree.contains(&spawn.parent_agent_id))
                .map(|spawn| spawn.fork_key)
                .collect::<Vec<_>>();
            for fork_key in abandoned_spawns {
                self.discard_pending_spawn(fork_key).await?;
            }
            if !self.state.pending_discards.contains_key(&agent) {
                let parent_context_id = self.context_for_agent(caller)?;
                let child_context_id = WorkspaceContextId::from_bytes(route.context_id);
                let mut discard_agents = Vec::new();
                for descendant in descendants
                    .into_iter()
                    .rev()
                    .chain(std::iter::once(agent.clone()))
                {
                    let removed = self
                        .state
                        .routes
                        .get(&descendant)
                        .cloned()
                        .ok_or_else(|| "discard subtree route disappeared".to_owned())?;
                    let removed_context = WorkspaceContextRegistry::new(self.store.clone())
                        .resolve(WorkspaceContextId::from_bytes(removed.context_id))
                        .await
                        .map_err(display)?;
                    let mut repository_ids = BTreeSet::new();
                    let mut workspace_ids = BTreeSet::new();
                    for root in removed.roots.values() {
                        let workspace_id = removed_context
                            .roots
                            .get(&WorkspaceRootId::from_bytes(root.root_id))
                            .ok_or_else(|| {
                                "discarded adapter route is absent from its core context".to_owned()
                            })?
                            .workspace_id;
                        let repository_id =
                            repository_id(workspace_id.into_bytes(), root.repository_workspace_id);
                        repository_ids.insert(repository_id);
                        workspace_ids.insert(repository_id);
                        workspace_ids.insert(workspace_id);
                        if let Some(state) = <LocalCoreStateStore as acyclic_fs::GitCompatStore>::load(
                            &self.store,
                            repository_id,
                        )
                        .await
                        .map_err(display)?
                        {
                            workspace_ids
                                .extend(state.branches.values().map(|branch| branch.workspace_id));
                        }
                    }
                    let mut workspaces = Vec::new();
                    for workspace_id in workspace_ids.into_iter().rev() {
                        let name = WorkspaceGraph::new(self.store.clone())
                            .resolve(workspace_id)
                            .await
                            .map_err(|error| {
                                format!("cannot resolve discarded workspace lineage: {error}")
                            })?
                            .workspace_name;
                        workspaces.push(DiscardWorkspace {
                            name,
                            delete_key: IdempotencyKey::new().into_bytes(),
                        });
                    }
                    discard_agents.push(DiscardAgent {
                        agent_id: descendant,
                        path: removed.mount_path,
                        repository_workspace_ids: repository_ids
                            .into_iter()
                            .map(|id| id.into_bytes())
                            .collect(),
                        mount_detached: false,
                        workspaces: workspaces.into(),
                    });
                }
                for member in &subtree {
                    let route = self
                        .state
                        .routes
                        .get_mut(member)
                        .ok_or_else(|| "discard subtree route disappeared".to_owned())?;
                    route.lifecycle = RouteLifecycle::Frozen;
                }
                self.state.pending_discards.insert(
                    agent.clone(),
                    PendingDiscard {
                        parent_context_id: parent_context_id.into_bytes(),
                        child_context_id: child_context_id.into_bytes(),
                        context_discarded: false,
                        agents: discard_agents.into(),
                    },
                );
                self.persist()?;
            }
            self.continue_pending_discard(&agent).await?;
            Ok(json!({"status":"discarded","agent":agent}))
        }

    pub(crate) async fn recover_pending_discards(&mut self) -> Result<(), String> {
            let roots = self
                .state
                .pending_discards
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for root in roots {
                self.continue_pending_discard(&root).await?;
            }
            Ok(())
        }

    pub(crate) async fn continue_pending_discard(&mut self, root: &str) -> Result<(), String> {
            let pending = self
                .state
                .pending_discards
                .get(root)
                .cloned()
                .ok_or_else(|| "pending discard is missing".to_owned())?;
            if !pending.context_discarded {
                match WorkspaceContextRegistry::new(self.store.clone())
                    .discard_subtree(
                        WorkspaceContextId::from_bytes(pending.parent_context_id),
                        WorkspaceContextId::from_bytes(pending.child_context_id),
                        10_000,
                    )
                    .await
                {
                    Ok(_) | Err(acyclic_fs::WorkspaceContextError::Discarded) => {}
                    Err(error) => return Err(display(error)),
                }
                #[cfg(test)]
                if self.fail_after_context_discard {
                    self.fail_after_context_discard = false;
                    return Err("injected failure after durable context discard".to_owned());
                }
                self.state
                    .pending_discards
                    .get_mut(root)
                    .ok_or_else(|| "pending discard disappeared".to_owned())?
                    .context_discarded = true;
                self.persist()?;
            }
            while let Some(agent) = self
                .state
                .pending_discards
                .get(root)
                .and_then(|discard| discard.agents.front())
                .cloned()
            {
                if !agent.mount_detached {
                    self.unmount_agent(&agent.agent_id).await?;
                    self.state
                        .pending_discards
                        .get_mut(root)
                        .and_then(|discard| discard.agents.front_mut())
                        .ok_or_else(|| "pending discard disappeared during unmount".to_owned())?
                        .mount_detached = true;
                    self.persist()?;
                }
                remove_tree_checked(&self.workspace_mount_root(), &agent.path)
                    .map_err(|error| format!("cannot remove discarded mount path: {error}"))?;
                for repository_id in agent.repository_workspace_ids {
                    let repository_id = acyclic_fs::WorkspaceId::from_bytes(repository_id);
                    if let Some(state) = <LocalCoreStateStore as acyclic_fs::GitCompatStore>::load(
                        &self.store,
                        repository_id,
                    )
                    .await
                    .map_err(display)?
                        && !<LocalCoreStateStore as acyclic_fs::GitCompatStore>::compare_and_delete(
                            &self.store,
                            repository_id,
                            state.revision,
                        )
                        .await
                        .map_err(display)?
                    {
                        return Err(
                            "Git compatibility state changed while discarding the agent".to_owned()
                        );
                    }
                }
                while let Some(workspace) = self
                    .state
                    .pending_discards
                    .get(root)
                    .and_then(|discard| discard.agents.front())
                    .and_then(|agent| agent.workspaces.front())
                    .cloned()
                {
                    match self
                        .fs
                        .delete_workspace(
                            &workspace.name,
                            IdempotencyKey::from_bytes(workspace.delete_key),
                        )
                        .await
                        .map_err(|error| format!("cannot delete discarded workspace: {error}"))?
                    {
                        WorkspaceDelete::Deleted => self.shared_roots.collection.deleted(),
                        WorkspaceDelete::AlreadyDeleted => {}
                        WorkspaceDelete::Conflict => {
                            return Err("discarded workspace deletion conflicted".to_owned());
                        }
                        WorkspaceDelete::IdempotencyConflict => {
                            return Err(
                                "discarded workspace deletion reused an incompatible identity"
                                    .to_owned(),
                            );
                        }
                    }
                    #[cfg(test)]
                    if self.fail_after_discard_delete {
                        self.fail_after_discard_delete = false;
                        return Err("injected failure after durable workspace deletion".to_owned());
                    }
                    self.state
                        .pending_discards
                        .get_mut(root)
                        .and_then(|discard| discard.agents.front_mut())
                        .ok_or_else(|| "pending discard disappeared during deletion".to_owned())?
                        .workspaces
                        .pop_front();
                    self.persist()?;
                }
                self.state.routes.remove(&agent.agent_id);
                self.state.turns.retain(|_, value| value != &agent.agent_id);
                self.state
                    .pending_discards
                    .get_mut(root)
                    .ok_or_else(|| "pending discard disappeared before completion".to_owned())?
                    .agents
                    .pop_front();
                self.persist()?;
            }
            self.state.pending_discards.remove(root);
            self.persist()
        }

    pub(crate) async fn unmount_agent(&mut self, agent_id: &str) -> Result<(), String> {
            self.ensure_agent_idle(agent_id)?;
            #[cfg(test)]
            if self.fail_next_unmount.remove(agent_id) {
                return Err("injected mount teardown failure".to_owned());
            }
            if let Some(mount) = self.mounts.get(agent_id) {
                mount.unmount().await?;
                if let Some(mount) = self.mounts.remove(agent_id) {
                    self.retire(mount);
                }
            }
            Ok(())
        }

    /// Tears a detached mount's source down off the caller's path: its
        /// namespace is gone and its effects are published, so nothing waits on
        /// the watch and checkout it still holds, whose release takes a while.
        /// Close joins every retirement before it lets the root go.
        pub(crate) fn retire(&mut self, mount: LocalMount) {
            self.retiring.retain(|retirement| !retirement.is_finished());
            self.retiring
                .push(tokio::task::spawn_blocking(move || drop(mount)));
        }

}
