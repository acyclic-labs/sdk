//! Session construction and shutdown.

use super::*;

pub(crate) struct ControlPlane {
    pub(crate) data: PathBuf,
    pub(crate) config_root: PathBuf,
    pub(crate) fs: LocalFs,
    pub(crate) store: LocalCoreStateStore,
    pub(crate) shared_roots: SharedRootRegistry,
    pub(crate) state: AdapterState,
    pub(crate) roots: BTreeMap<String, LocalLazyWorkspace>,
    pub(crate) physical_roots: BTreeMap<String, Arc<SharedPhysicalRoot>>,
    pub(crate) mounts: BTreeMap<String, LocalMount>,
    pub(crate) pending_mounts: BTreeMap<[u8; 16], LocalMount>,
    /// Detached mounts whose sources are still being torn down; see
    /// [`ControlPlane::retire`].
    pub(crate) retiring: Vec<tokio::task::JoinHandle<()>>,
    /// The last save was left unflushed; see [`Survives::ServiceCrash`].
    pub(crate) unflushed: bool,
    pub(crate) slots: StateSlots,
    #[cfg(test)]
    pub(crate) owns_local_root: bool,
    #[cfg(test)]
    pub(crate) fail_next_flush: bool,
    #[cfg(test)]
    pub(crate) fail_next_unmount: BTreeSet<String>,
    #[cfg(test)]
    pub(crate) fail_after_discard_delete: bool,
    #[cfg(test)]
    pub(crate) fail_after_context_discard: bool,
    #[cfg(test)]
    pub(crate) fail_before_publication_history: bool,
    #[cfg(test)]
    pub(crate) fail_after_watch_poll: bool,
    #[cfg(test)]
    pub(crate) fail_after_root_intent: bool,
    /// Loaded native input stays owned until its paths and state are destroyed.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) native_input_custody: Option<acyclic_fs::NativeAllocation>,
}

impl ControlPlane {
    #[cfg(test)]
        pub(crate) async fn open(data: PathBuf) -> Result<Self, String> {
            fs::create_dir_all(&data).map_err(display)?;
            let fs = LocalFs::local(LocalOptions::new(data.join("filesystem")))
                .await
                .map_err(display)?;
            let store = LocalCoreStateStore::open_owned(data.join("core-state")).map_err(display)?;
            let mut control =
                Self::open_with(data.clone(), data, fs, store, SharedRootRegistry::default()).await?;
            control.owns_local_root = true;
            Ok(control)
        }

    pub(crate) async fn open_with(
            data: PathBuf,
            config_root: PathBuf,
            fs: LocalFs,
            store: LocalCoreStateStore,
            shared_roots: SharedRootRegistry,
        ) -> Result<Self, String> {
            fs::create_dir_all(&data).map_err(display)?;
            let (state, slots) = load_state(&data)?;
            state.task_binding.require_host_managed_recovery()?;
            let mut control = Self {
                data,
                config_root,
                fs,
                store,
                shared_roots,
                state,
                roots: BTreeMap::new(),
                physical_roots: BTreeMap::new(),
                mounts: BTreeMap::new(),
                pending_mounts: BTreeMap::new(),
                retiring: Vec::new(),
                unflushed: false,
                slots,
                #[cfg(test)]
                owns_local_root: false,
                #[cfg(test)]
                fail_next_flush: false,
                #[cfg(test)]
                fail_next_unmount: BTreeSet::new(),
                #[cfg(test)]
                fail_after_discard_delete: false,
                #[cfg(test)]
                fail_after_context_discard: false,
                #[cfg(test)]
                fail_before_publication_history: false,
                #[cfg(test)]
                fail_after_watch_poll: false,
                #[cfg(test)]
                fail_after_root_intent: false,
                #[cfg(not(target_arch = "wasm32"))]
                native_input_custody: None,
            };
            control.restore_root().await?;
            control.validate_pending_spawn_paths()?;
            control.validate_routes_against_contexts().await?;
            control.recover_multi_root_publications().await?;
            control.recover_expired_adapter_leases().await?;
            control.restore_mounts().await?;
            control.recover_pending_spawns().await?;
            control.recover_pending_discards().await?;
            let requested_stops = control
                .state
                .routes
                .values()
                .filter(|route| route.lifecycle == RouteLifecycle::StopRequested)
                .map(|route| route.agent_id.clone())
                .collect::<Vec<_>>();
            for agent_id in requested_stops {
                control.finalize_requested_stops(&agent_id).await?;
            }
            Ok(control)
        }

    /// Keeps the actual input guards on their original bootstrap ledger.
        /// Rejection returns the incoming guard intact while its payload is live.
        #[cfg(not(target_arch = "wasm32"))]
        pub(crate) fn retain_original_input(
            &mut self,
            mut incoming: acyclic_fs::NativeAllocation,
        ) -> Result<(), (acyclic_fs::NativeTreeMaterializationError, acyclic_fs::NativeAllocation)> {
            if let Some(held) = self.native_input_custody.as_mut() {
                held.absorb(&mut incoming).map_err(|cause| (cause, incoming))
            } else {
                self.native_input_custody = Some(incoming);
                Ok(())
            }
        }

    #[cfg(test)]
        pub(crate) async fn shutdown(self) -> Result<(), String> {
            self.close(false).await.map(drop)
        }

    /// Shuts the session down and hands back what it knows of its state
        /// slots. `terminal_save` says the caller follows a successful close with
        /// a flushed save of the whole session, which then makes every earlier
        /// save durable; otherwise the close leaves nothing unflushed itself.
        pub(crate) async fn close(mut self, terminal_save: bool) -> Result<StateSlots, String> {
            // Only finishing a lease or publishing a mount builds a durable
            // effect on the session's state, and nothing may build one on a save
            // a power loss could still undo. A close with neither saves nothing
            // until the terminal save.
            let has_leases = !self.state.leases.is_empty();
            if has_leases || !self.mounts.is_empty() || !self.pending_mounts.is_empty() {
                self.make_durable().await?;
            } else {
                self.fs.flush_deferred_authority().map_err(display)?;
            }
            #[cfg(test)]
            let root_released = if self.owns_local_root {
                self.fs.local_root_release_barrier()
            } else {
                None
            };
            let result = async {
                let operations = OperationWindowCoordinator::new(self.fs.operation_window_store());
                let now = now_millis();
                let active = self.state.leases.values().cloned().collect::<Vec<_>>();
                for record in &active {
                    let route = self
                        .state
                        .routes
                        .get(&record.agent_id)
                        .cloned()
                        .ok_or_else(|| "active lease route is missing during shutdown".to_owned())?;
                    let parent_context = WorkspaceContextRegistry::new(self.store.clone())
                        .resolve(self.context_for_agent(&route.parent_agent_id)?)
                        .await
                        .map_err(display)?;
                    for root_record in record.roots.values() {
                        let root_id = WorkspaceRootId::from_bytes(root_record.root_id);
                        let workspace = self.workspace_root(&route, root_id).await?;
                        let parent = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, parent_context
                                    .roots
                                    .get(&root_id)
                                    .ok_or_else(|| "active lease parent root is missing".to_owned())?
                                    .workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                            .await
                            .map_err(display)?;
                        operations
                            .observe_parent(workspace.id(), parent.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(display)?.id())
                            .await
                            .map_err(display)?;
                        if let acyclic_fs::OperationWindowFinish::Reconcile(reconcile) = operations
                            .finish(&root_record.lease(), now)
                            .await
                            .map_err(display)?
                        {
                            operations
                                .reconcile_workspace(
                                    &workspace,
                                    reconcile,
                                    OperationReconcileLimits::default(),
                                )
                                .await
                                .map_err(display)?;
                        }
                    }
                }
                if has_leases {
                    self.state.leases.clear();
                    self.persist()?;
                }
                let mounts = std::mem::take(&mut self.mounts);
                let mut first_error = None;
                for (agent_id, mount) in mounts {
                    if active.iter().any(|lease| lease.agent_id == agent_id) {
                        if let Err(error) = mount.abandon() {
                            first_error.get_or_insert_with(|| {
                                format!("cannot quarantine agent '{agent_id}' during shutdown: {error}")
                            });
                        }
                        continue;
                    }
                    if let Err(error) = mount.unmount().await {
                        first_error.get_or_insert_with(|| {
                            format!("cannot unmount agent '{agent_id}' during shutdown: {error}")
                        });
                    }
                }
                let pending_mounts = std::mem::take(&mut self.pending_mounts);
                for (key, mount) in pending_mounts {
                    if let Err(error) = mount.unmount().await {
                        first_error.get_or_insert_with(|| {
                            format!(
                                "cannot unmount prepared spawn '{}' during shutdown: {error}",
                                hex::encode(key)
                            )
                        });
                    }
                }
                drop(operations);
                for retirement in std::mem::take(&mut self.retiring) {
                    if let Err(error) = retirement.await {
                        first_error.get_or_insert_with(|| {
                            format!("cannot release a retired mount during shutdown: {error}")
                        });
                    }
                }
                first_error.map_or(Ok(()), Err)
            }
            .await;
            let result = match result {
                Ok(()) if !terminal_save => self.make_durable().await,
                result => result,
            };
            let slots = self.slots;
            // The shared service owns the physical root-release boundary. A standalone test control
            // owns its root directly, so it waits here after dropping every provider handle.
            drop(self);
            #[cfg(test)]
            if let Some(barrier) = root_released {
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    while !barrier.is_released() {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                })
                .await
                .map_err(|_| {
                    "standalone control retained filesystem handles after shutdown".to_owned()
                })?;
            }
            result.map(|()| slots)
        }

}
