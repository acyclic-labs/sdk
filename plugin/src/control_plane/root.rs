//! Host-managed root registration and recovery.

use super::*;

pub(crate) fn workspace_mount_root(config_root: &Path) -> PathBuf {
    config_root.join("w")
}

pub(crate) fn root_workspace_name(session_id: &str, root: &Path) -> String {
    format!(
        "root-{}-{}",
        short_hash(session_id.as_bytes()),
        short_hash(root.as_os_str().to_string_lossy().as_bytes()),
    )
}

pub(crate) fn validate_persisted_route_paths(
    config_root: &Path,
    route: &Route,
) -> Result<(), String> {
    let expected_mount = workspace_mount_root(config_root).join(compact_id(&route.context_id));
    if route.mount_path != expected_mount {
        return Err(format!(
            "persisted mount path for agent '{}' does not match its derived service path",
            route.agent_id
        ));
    }
    route.active_path()?;
    Ok(())
}

impl ControlPlane {
    pub(crate) fn workspace_mount_root(&self) -> PathBuf {
            workspace_mount_root(&self.config_root)
        }

    pub(crate) fn validate_route_paths(&self, route: &Route) -> Result<(), String> {
            validate_persisted_route_paths(&self.config_root, route)
        }

    pub(crate) fn validate_pending_spawn_paths(&self) -> Result<(), String> {
            for pending in &self.state.pending {
                if pending.mount_path != self.workspace_mount_root().join(pending.mount_name()) {
                    return Err(
                        "persisted pending spawn mount is outside its workspace root".to_owned(),
                    );
                }
            }
            Ok(())
        }

    pub(crate) async fn restore_root(&mut self) -> Result<(), String> {
            if self.state.root_session_id.is_empty() {
                return Ok(());
            }
            self.state.require_created_roots()?;
            if self.state.pending_root_registration {
                let root_id = WorkspaceRootId::from_bytes(self.state.root_id);
                let binding = self
                    .state
                    .roots
                    .get(&root_key(root_id))
                    .ok_or_else(|| "pending root registration has no durable binding".to_owned())?;
                let workspace = self
                    .fs
                    .open_workspace(&root_workspace_name(
                        &self.state.root_session_id,
                        &binding.path,
                    ))
                    .await
                    .map_err(display)?;
                if workspace.id().into_bytes() != binding.created()?.repository_workspace_id {
                    return Err("pending root workspace identity changed".to_owned());
                }
                WorkspaceGraph::new(self.store.clone())
                    .register_root(&workspace)
                    .await
                    .map_err(display)?;
                WorkspaceContextRegistry::new(self.store.clone())
                    .register_root(
                        WorkspaceContextId::from_bytes(self.state.root_context_id),
                        [WorkspaceContextRoot {
                            root_id,
                            source_path: binding.path.clone(),
                            workspace_id: workspace.id(),
                            workspace_name: workspace.name().as_str().to_owned(),
                            parent_workspace_id: None,
                            mount_path: None,
                        }],
                    )
                    .await
                    .map_err(display)?;
                self.state.pending_root_registration = false;
                self.persist()?;
            }
            let mut context = WorkspaceContextRegistry::new(self.store.clone())
                .resolve(WorkspaceContextId::from_bytes(self.state.root_context_id))
                .await
                .map_err(display)?;
            for key in self
                .state
                .pending_root_adoptions
                .iter()
                .cloned()
                .collect::<Vec<_>>()
            {
                let binding = self
                    .state
                    .roots
                    .get(&key)
                    .ok_or_else(|| "pending root adoption has no durable binding".to_owned())?;
                let root_id = WorkspaceRootId::from_bytes(binding.root_id);
                if !context.roots.contains_key(&root_id) {
                    let workspace = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, acyclic_fs::WorkspaceId::from_bytes(
                            binding.created()?.repository_workspace_id,
                        ), WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .await
                        .map_err(display)?;
                    context = WorkspaceContextRegistry::new(self.store.clone())
                        .adopt_root(
                            context.context_id,
                            WorkspaceContextRoot {
                                root_id,
                                source_path: binding.path.clone(),
                                workspace_id: workspace.id(),
                                workspace_name: workspace.name().as_str().to_owned(),
                                parent_workspace_id: None,
                                mount_path: None,
                            },
                        )
                        .await
                        .map_err(display)?;
                }
                self.state.pending_root_adoptions.remove(&key);
            }
            if self.state.roots.is_empty() {
                return Err("Acyclic core context has no registered roots".to_owned());
            }
            for root in self.state.roots.values() {
                if !context
                    .roots
                    .contains_key(&WorkspaceRootId::from_bytes(root.root_id))
                {
                    return Err("adapter root is absent from its core context".to_owned());
                }
            }
            if context.roots.len() != self.state.roots.len() {
                return Err("core context has a root without an adapter source binding".to_owned());
            }
            // Persist completion of any recovered adoption before reopening the source.
            self.persist()?;
            for (key, root) in self.state.roots.clone() {
                let physical = self
                    .shared_roots
                    .acquire(
                        &root.path,
                        SharedRootAdmission::Resume(SourceReference {
                            identity: root.created()?.source_identity,
                            epoch: root.created()?.source_epoch,
                        }),
                        native_root_source::budget(),
                        &CancellationToken::new(),
                    )
                    .await.map_err(|failure| format!("{}; work={:?}", failure.error, failure.work))?.value;
                let source = Arc::clone(&physical.source);
                verify_native_root_identity(&source, root.created()?.native_root_identity, &root.path)?;
                let refreshed = source.reference();
                if let Some(binding) = self.state.roots.get_mut(&key) {
                    binding.created_mut()?.source_epoch = refreshed.epoch;
                }
                let workspace = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, context
                            .roots
                            .get(&WorkspaceRootId::from_bytes(root.root_id))
                            .ok_or_else(|| "adapter root is absent from its core context".to_owned())?
                            .workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(display)?;
                self.roots.insert(
                    key.clone(),
                    LazyWorkspace::bind(workspace, Arc::clone(&source), self.store.clone(), &mut acyclic_fs::kernel::KernelAccounting::Standalone {
                        allocations: Default::default(),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                        budget: WorkBudget::UNBOUNDED,
                        cancellation: &CancellationToken::new(),
                    })
                        .await
                        .map_err(display)?,
                );
                self.physical_roots.insert(key, physical);
            }
            self.persist()
        }

    pub(crate) async fn validate_routes_against_contexts(&self) -> Result<(), String> {
            for route in self.state.routes.values() {
                self.validate_route_paths(route)?;
                let context_id = WorkspaceContextId::from_bytes(route.context_id);
                let context = WorkspaceContextRegistry::new(self.store.clone())
                    .resolve(context_id)
                    .await
                    .map_err(display)?;
                let expected_parent = self.context_for_agent(&route.parent_agent_id)?;
                if context.parent_context_id != Some(expected_parent) {
                    return Err(format!(
                        "route '{}' has a different direct parent in core state",
                        route.agent_id
                    ));
                }
                let adapter_roots = route
                    .roots
                    .values()
                    .map(|root| WorkspaceRootId::from_bytes(root.root_id))
                    .collect::<BTreeSet<_>>();
                let context_roots = context.roots.keys().copied().collect::<BTreeSet<_>>();
                if adapter_roots != context_roots {
                    return Err(format!(
                        "route '{}' root set differs from its core context",
                        route.agent_id
                    ));
                }
                let expected_state = if route.lifecycle.is_frozen() {
                    WorkspaceContextState::Frozen
                } else {
                    WorkspaceContextState::Active
                };
                let pending_discard = self.state.pending_discards.values().any(|discard| {
                    discard
                        .agents
                        .iter()
                        .any(|agent| agent.agent_id == route.agent_id)
                });
                if context.state != expected_state
                    && !(pending_discard && context.state == WorkspaceContextState::Discarded)
                {
                    return Err(format!(
                        "route '{}' lifecycle differs from its core context",
                        route.agent_id
                    ));
                }
            }
            Ok(())
        }

    pub(crate) async fn restore_mounts(&mut self) -> Result<(), String> {
            let discarding_agents = self
                .state
                .pending_discards
                .values()
                .flat_map(|discard| {
                    discard
                        .agents
                        .iter()
                        .filter(|agent| !agent.mount_detached)
                        .map(|agent| agent.agent_id.clone())
                })
                .collect::<BTreeSet<_>>();
            let routes = self.state.routes.values().cloned().collect::<Vec<_>>();
            let mut completed_mount_intent = false;
            for route in routes {
                if route.lifecycle.needs_mount() || discarding_agents.contains(&route.agent_id) {
                    self.validate_route_paths(&route)?;
                    fs::create_dir_all(&route.mount_path).map_err(display)?;
                    #[cfg(windows)]
                    for root in route.roots.values() {
                        let path = route.mount_path.join(root.mount_name());
                        if let Some(preserved) =
                            acyclic_fs::recover_native_mount_destination_preserving_residue(&path)
                                .map_err(display)?
                        {
                            eprintln!(
                                "Acyclic preserved unpublished crash residue at {}; the workspace resumes from its last durable generation",
                                preserved.display()
                            );
                        }
                    }
                    let mount = self.mount_route(&route).await?;
                    self.mounts.insert(route.agent_id.clone(), mount);
                    if route.lifecycle == RouteLifecycle::Mounting {
                        self.state
                            .routes
                            .get_mut(&route.agent_id)
                            .ok_or_else(|| "mounting route disappeared during recovery".to_owned())?
                            .lifecycle = RouteLifecycle::Active;
                        completed_mount_intent = true;
                    }
                }
            }
            if completed_mount_intent {
                self.persist()?;
            }
            Ok(())
        }

    pub(crate) async fn session_start(&mut self, input: Value) -> Result<Value, String> {
            self.state.require_created_roots()?;
            let session_id = string(&input, "session_id")?;
            let host = input.get("host").and_then(Value::as_str).unwrap_or("codex");
            let root_path = PathBuf::from(string(&input, "cwd")?);
            let canonical = root_path.canonicalize().map_err(display)?;
            if !self.state.root_session_id.is_empty() && self.state.root_session_id != session_id {
                return Err("plugin data is already bound to another root session".to_owned());
            }
            if !self.state.root_session_id.is_empty() {
                self.state.active = true;
                if self.roots.is_empty() || !self.state.pending_root_adoptions.is_empty() {
                    self.restore_root().await?;
                }
                let already_registered = self
                    .state
                    .roots
                    .values()
                    .any(|root| canonical.starts_with(&root.path));
                if !already_registered {
                    let root_id = WorkspaceRootId::new();
                    let workspace_name = root_workspace_name(&session_id, &canonical);
                    let physical = self
                        .shared_roots
                        .acquire(&canonical, SharedRootAdmission::Fresh, native_root_source::budget(), &CancellationToken::new())
                        .await.map_err(|failure| format!("{}; work={:?}", failure.error, failure.work))?.value;
                    let source = Arc::clone(&physical.source);
                    let source_reference = source.reference();
                    let workspace = self.fs
                        .create_workspace_with_config(
                            &workspace_name,
                            VolumeConfig::native(Lifecycle::Durable),
                            WorkBudget::UNBOUNDED,
                            &CancellationToken::new(),
                        )
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| display(failure.error))?;
                    let workspace = LazyWorkspace::bind(workspace, Arc::clone(&source), self.store.clone(), &mut acyclic_fs::kernel::KernelAccounting::Standalone {
                        allocations: Default::default(),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                        budget: WorkBudget::UNBOUNDED,
                        cancellation: &CancellationToken::new(),
                    })
                        .await
                        .map_err(display)?;
                    WorkspaceGraph::new(self.store.clone())
                        .register_root(workspace.workspace())
                        .await
                        .map_err(display)?;
                    let key = root_key(root_id);
                    self.state.roots.insert(
                        key.clone(),
                        RootBinding {
                            root_id: root_id.into_bytes(),
                            path: canonical.clone(),
                            allocation: RootAllocation::Created(RootCreation {
                                repository_workspace_id: workspace.workspace().id().into_bytes(),
                                source_identity: source_reference.identity,
                                source_epoch: source_reference.epoch,
                                native_root_identity: source.inner().root_identity().to_bytes(),
                            }),
                        },
                    );
                    self.state.pending_root_adoptions.insert(key.clone());
                    self.persist()?;
                    #[cfg(test)]
                    if self.fail_after_root_intent {
                        self.fail_after_root_intent = false;
                        return Err("injected failure after root adoption intent".to_owned());
                    }
                    WorkspaceContextRegistry::new(self.store.clone())
                        .adopt_root(
                            WorkspaceContextId::from_bytes(self.state.root_context_id),
                            WorkspaceContextRoot {
                                root_id,
                                source_path: canonical.clone(),
                                workspace_id: workspace.workspace().id(),
                                workspace_name: workspace_name.clone(),
                                parent_workspace_id: None,
                                mount_path: None,
                            },
                        )
                        .await
                        .map_err(display)?;
                    self.state.pending_root_adoptions.remove(&key);
                    self.roots.insert(key.clone(), workspace);
                    self.physical_roots.insert(key, physical);
                    self.persist()?;
                }
                return Ok(json!({
                    "hookSpecificOutput": {
                        "hookEventName": "SessionStart",
                        "additionalContext": guidance_for(host)
                    }
                }));
            }
            let workspace_name = root_workspace_name(&session_id, &canonical);
            let physical = self
                .shared_roots
                .acquire(&canonical, SharedRootAdmission::Fresh, native_root_source::budget(), &CancellationToken::new())
                .await.map_err(|failure| format!("{}; work={:?}", failure.error, failure.work))?.value;
            let source = Arc::clone(&physical.source);
            let source_reference = source.reference();
            // The root's binding, lineage, and context commit as one change set
            // with one flush. The durable intent below precedes it, so recovery
            // can repeat all three, binding the root afresh if its attach was lost.
            let (store, durability) = self.store.defer_durability();

            let workspace = self.fs
                .create_workspace_with_config(
                    &workspace_name,
                    VolumeConfig::native(Lifecycle::Durable),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| display(failure.error))?;
            let workspace = LazyWorkspace::bind(workspace, Arc::clone(&source), store.clone(), &mut acyclic_fs::kernel::KernelAccounting::Standalone {
                allocations: Default::default(),
                work: acyclic_fs::WorkCounters::UNCHARGED,
                budget: WorkBudget::UNBOUNDED,
                cancellation: &CancellationToken::new(),
            })
                .await
                .map_err(display)?;
            let context_id = if self.state.root_context_id == [0; 16] {
                WorkspaceContextId::new()
            } else {
                WorkspaceContextId::from_bytes(self.state.root_context_id)
            };
            let root_id = if self.state.root_id == [0; 16] {
                WorkspaceRootId::new()
            } else {
                WorkspaceRootId::from_bytes(self.state.root_id)
            };
            self.state.version = ADAPTER_STATE_VERSION;
            self.state.root_session_id = session_id.clone();
            self.state.active = true;
            self.state.root_agent_id = format!("root:{session_id}");
            self.state.root_context_id = context_id.into_bytes();
            self.state.root_id = root_id.into_bytes();
            let binding = RootBinding {
                root_id: root_id.into_bytes(),
                path: canonical.clone(),
                allocation: RootAllocation::Created(RootCreation {
                    repository_workspace_id: workspace.workspace().id().into_bytes(),
                    source_identity: source_reference.identity,
                    source_epoch: source_reference.epoch,
                    native_root_identity: source.inner().root_identity().to_bytes(),
                }),
            };
            self.state.roots.insert(root_key(root_id), binding);
            self.state.pending_root_registration = true;
            self.persist()?;
            #[cfg(test)]
            if self.fail_after_root_intent {
                self.fail_after_root_intent = false;
                return Err("injected failure after root registration intent".to_owned());
            }
            WorkspaceGraph::new(store.clone())
                .register_root(workspace.workspace())
                .await
                .map_err(display)?;
            WorkspaceContextRegistry::new(store.clone())
                .register_root(
                    context_id,
                    [WorkspaceContextRoot {
                        root_id,
                        source_path: canonical.clone(),
                        workspace_id: workspace.workspace().id(),
                        workspace_name,
                        parent_workspace_id: None,
                        mount_path: None,
                    }],
                )
                .await
                .map_err(display)?;
            durability.commit().await.map_err(display)?;
            self.state.pending_root_registration = false;
            self.roots.insert(root_key(root_id), workspace);
            self.physical_roots.insert(root_key(root_id), physical);
            // Losing this save leaves the flushed intent, from which restore_root
            // repeats both registrations idempotently.
            self.persist_unflushed()?;
            Ok(json!({
                "hookSpecificOutput": {
                    "hookEventName": "SessionStart",
                    "additionalContext": guidance_for(host)
                }
            }))
        }

}
