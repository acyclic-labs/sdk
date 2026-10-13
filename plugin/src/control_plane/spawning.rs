//! Durable child preparation and recovery.

use super::*;

impl ControlPlane {
    /// Builds and mounts the actual child workspace before the host is allowed
        /// to spawn it. `SubagentStart` is advisory in several hosts, while the
        /// spawn tool hook is a blocking boundary. Preparing here therefore closes
        /// the isolation gap without paying for a throwaway probe mount.
        /// Prepares a spawn's child workspaces and mount. `prepared` is how the
        /// final mark is saved: unflushed only when it ends the request.
        pub(crate) async fn prepare_pending_spawn(
            &mut self,
            fork_key: [u8; 16],
            prepared: Survives,
        ) -> Result<(), String> {
            let pending = self
                .state
                .pending
                .iter()
                .find(|pending| pending.fork_key == fork_key)
                .cloned()
                .ok_or_else(|| "pending spawn disappeared during preparation".to_owned())?;
            if pending.lifecycle == PendingSpawnLifecycle::Prepared
                && self.pending_mounts.contains_key(&pending.fork_key)
            {
                return Ok(());
            }
            if pending.lifecycle == PendingSpawnLifecycle::Discarding {
                return Err("pending spawn is already being discarded".to_owned());
            }
            let capabilities = acyclic_fs::probe_native_mount();
            if !capabilities.available || !capabilities.writable {
                return Err(format!(
                    "cannot isolate the requested subagent: {}",
                    capabilities
                        .unavailable_reason
                        .as_deref()
                        .unwrap_or("the native writable mount backend is unavailable")
                ));
            }
            self.sync_agent(&pending.parent_agent_id).await?;
            let parent_context_id = self.context_for_agent(&pending.parent_agent_id)?;
            let parent_context = WorkspaceContextRegistry::new(self.store.clone())
                .resolve(parent_context_id)
                .await
                .map_err(display)?;
            let active_root_id = self.pending_active_root(&pending)?;
            if !parent_context.roots.contains_key(&active_root_id) {
                return Err("active root is missing from parent context".to_owned());
            }
            let mut route_roots = pending.roots.clone();
            if pending.lifecycle == PendingSpawnLifecycle::Preparing {
                // Every root's fork binding, lineage, and the child context commit
                // as one change set, durable before the mount mark below.
                let (store, durability) = self.store.defer_durability();

                fs::create_dir_all(&pending.mount_path).map_err(display)?;
                if pending
                    .mount_path
                    .read_dir()
                    .map_err(display)?
                    .next()
                    .is_some()
                {
                    return Err("child workspace destination is not empty".to_owned());
                }
                let fork_key = IdempotencyKey::from_bytes(fork_key);
                let mut context_roots = Vec::with_capacity(parent_context.roots.len());
                for (root_id, parent_root) in &parent_context.roots {
                    let source_binding = self
                        .state
                        .roots
                        .get(&root_key(*root_id))
                        .ok_or_else(|| "physical root binding is missing".to_owned())?;
                    let source = self
                        .physical_roots
                        .get(&root_key(*root_id))
                        .map(|root| Arc::clone(&root.source))
                        .ok_or_else(|| "physical root source is unavailable".to_owned())?;
                    verify_native_root_identity(
                        &source,
                        source_binding.created()?.native_root_identity,
                        &source_binding.path,
                    )?;
                    let parent_workspace = WorkspaceGraph::new(store.clone()).workspace(&self.fs, parent_root.workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .await
                        .map_err(display)?;
                    let parent_lazy = LazyWorkspace::open(parent_workspace, source, store.clone(), &mut acyclic_fs::kernel::KernelAccounting::Standalone {
                        allocations: Default::default(),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                        budget: WorkBudget::UNBOUNDED,
                        cancellation: &CancellationToken::new(),
                    })
                        .await
                        .map_err(display)?;
                    let root_name = route_name(*root_id);
                    let child_name = format!("{}-{root_name}", pending.workspace_name);
                    let fork_generation = parent_lazy.workspace().head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(display)?.id();
                    let child = parent_lazy
                        .fork(&child_name, derived_idempotency_key(fork_key, *root_id), &mut acyclic_fs::kernel::KernelAccounting::Standalone {
                            allocations: Default::default(),
                            work: acyclic_fs::WorkCounters::UNCHARGED,
                            budget: WorkBudget::UNBOUNDED,
                            cancellation: &CancellationToken::new(),
                        })
                        .await
                        .map_err(display)?.0;
                    WorkspaceGraph::new(store.clone())
                        .register_existing_child(
                            parent_lazy.workspace(),
                            child.workspace(),
                            fork_generation,
                        )
                        .await
                        .map_err(display)?;
                    context_roots.push(WorkspaceContextRoot {
                        root_id: *root_id,
                        source_path: source_binding.path.clone(),
                        workspace_id: child.workspace().id(),
                        workspace_name: child_name,
                        parent_workspace_id: Some(parent_lazy.workspace().id()),
                        mount_path: Some(pending.mount_path.join(&root_name)),
                    });
                    route_roots.insert(
                        root_key(*root_id),
                        RouteRoot {
                            root_id: root_id.into_bytes(),
                            repository_workspace_id: child.workspace().id().into_bytes(),
                            published_generation: [0; 32],
                        },
                    );
                }
                WorkspaceContextRegistry::new(store.clone())
                    .register_child(pending.context_id(), parent_context_id, context_roots)
                    .await
                    .map_err(display)?;
                durability.commit().await.map_err(display)?;
                let prepared = self
                    .state
                    .pending
                    .iter_mut()
                    .find(|candidate| candidate.fork_key == fork_key.into_bytes())
                    .ok_or_else(|| "pending spawn disappeared before mount intent".to_owned())?;
                prepared.roots.clone_from(&route_roots);
                prepared.lifecycle = PendingSpawnLifecycle::Mounting;
                // Flushed: a mount can leave placeholders behind across a power
                // loss, which recovery accepts only after this mark.
                self.persist()?;
            }
            let mut roots = Vec::with_capacity(route_roots.len());
            for root in route_roots.values() {
                let root_id = root.id();
                let binding = self
                    .state
                    .roots
                    .get(&root_key(root_id))
                    .ok_or_else(|| "physical root binding is missing".to_owned())?;
                let source = self
                    .physical_roots
                    .get(&root_key(root_id))
                    .map(|root| Arc::clone(&root.source))
                    .ok_or_else(|| "physical root source is unavailable".to_owned())?;
                verify_native_root_identity(
                    &source,
                    binding.created()?.native_root_identity,
                    &binding.path,
                )?;
                let workspace = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, acyclic_fs::WorkspaceId::from_bytes(
                        root.repository_workspace_id,
                    ), WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(display)?;
                roots.push((
                    root.mount_name(),
                    LazyWorkspace::open(workspace, source, self.store.clone(), &mut acyclic_fs::kernel::KernelAccounting::Standalone {
                        allocations: Default::default(),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                        budget: WorkBudget::UNBOUNDED,
                        cancellation: &CancellationToken::new(),
                    })
                        .await
                        .map_err(display)?,
                ));
            }
            let mount = LocalMount::mount(roots, &pending.mount_path)
                .await
                .map_err(|error| {
                    format!("cannot isolate the requested subagent; native mount failed: {error}")
                })?;
            self.pending_mounts.insert(pending.fork_key, mount);
            self.state
                .pending
                .iter_mut()
                .find(|candidate| candidate.fork_key == fork_key)
                .ok_or_else(|| "pending spawn disappeared after mount".to_owned())?
                .lifecycle = PendingSpawnLifecycle::Prepared;
            match prepared {
                Survives::ServiceCrash => self.persist_unflushed(),
                Survives::PowerLoss => self.persist(),
            }
        }

    pub(crate) fn pending_active_root(
            &self,
            pending: &PendingSpawn,
        ) -> Result<WorkspaceRootId, String> {
            if let Some(root_id) = pending.active_root_id {
                return Ok(WorkspaceRootId::from_bytes(root_id));
            }
            if pending.parent_agent_id == self.state.root_agent_id {
                return Ok(WorkspaceRootId::from_bytes(self.state.root_id));
            }
            self.state
                .routes
                .get(&pending.parent_agent_id)
                .map(|route| WorkspaceRootId::from_bytes(route.root_id))
                .ok_or_else(|| "subagent parent route is missing".to_owned())
        }

    pub(crate) async fn discard_expired_pending_spawns(&mut self, now: u64) -> Result<(), String> {
            let expired = self
                .state
                .pending
                .iter()
                .filter(|pending| pending.expires_at_millis <= now)
                .map(|pending| pending.fork_key)
                .collect::<Vec<_>>();
            for fork_key in expired {
                self.discard_pending_spawn(fork_key).await?;
            }
            Ok(())
        }

    pub(crate) async fn recover_pending_spawns(&mut self) -> Result<(), String> {
            let pending = self
                .state
                .pending
                .iter()
                .map(|pending| {
                    (
                        pending.fork_key,
                        pending.expires_at_millis,
                        pending.lifecycle,
                    )
                })
                .collect::<Vec<_>>();
            let now = now_millis();
            for (fork_key, expires_at, lifecycle) in pending {
                if expires_at <= now || lifecycle == PendingSpawnLifecycle::Discarding {
                    self.discard_pending_spawn(fork_key).await?;
                } else {
                    self.prepare_pending_spawn(fork_key, Survives::PowerLoss)
                        .await?;
                }
            }
            Ok(())
        }

    pub(crate) async fn discard_pending_spawn(&mut self, fork_key: [u8; 16]) -> Result<(), String> {
            let pending = self
                .state
                .pending
                .iter_mut()
                .find(|pending| pending.fork_key == fork_key)
                .ok_or_else(|| "pending spawn disappeared during discard".to_owned())?;
            pending.lifecycle = PendingSpawnLifecycle::Discarding;
            let pending = pending.clone();
            self.persist()?;
            if let Some(mount) = self.pending_mounts.get(&pending.fork_key) {
                mount.unmount().await?;
                self.pending_mounts.remove(&pending.fork_key);
            }
            if let Ok(context) = WorkspaceContextRegistry::new(self.store.clone())
                .resolve(pending.context_id())
                .await
            {
                match WorkspaceContextRegistry::new(self.store.clone())
                    .discard_subtree(
                        context
                            .parent_context_id
                            .ok_or_else(|| "prepared child context has no parent".to_owned())?,
                        pending.context_id(),
                        1,
                    )
                    .await
                {
                    Ok(_) | Err(acyclic_fs::WorkspaceContextError::Discarded) => {}
                    Err(error) => return Err(display(error)),
                }
            }
            let root_ids = if pending.roots.is_empty() {
                WorkspaceContextRegistry::new(self.store.clone())
                    .resolve(self.context_for_agent(&pending.parent_agent_id)?)
                    .await
                    .map_err(display)?
                    .roots
                    .keys()
                    .copied()
                    .collect::<Vec<_>>()
            } else {
                pending.roots.values().map(RouteRoot::id).collect()
            };
            for root_id in root_ids {
                let name = format!("{}-{}", pending.workspace_name, route_name(root_id));
                match self
                    .fs
                    .delete_workspace(&name, derived_cleanup_key(fork_key, root_id))
                    .await
                    .map_err(display)?
                {
                    WorkspaceDelete::Deleted => self.shared_roots.collection.deleted(),
                    WorkspaceDelete::AlreadyDeleted => {}
                    WorkspaceDelete::Conflict => {
                        return Err("prepared workspace deletion conflicted".to_owned());
                    }
                    WorkspaceDelete::IdempotencyConflict => {
                        return Err("prepared workspace cleanup identity conflicted".to_owned());
                    }
                }
            }
            if pending.mount_path.exists() {
                remove_tree_checked(&self.workspace_mount_root(), &pending.mount_path)?;
            }
            self.state
                .pending
                .retain(|candidate| candidate.fork_key != fork_key);
            self.persist()
        }

}
