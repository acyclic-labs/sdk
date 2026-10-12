//! Git tool dispatch under admitted leases.

use super::*;

// Renewal replaces the prior publication token even when both clock samples
// fall in the same millisecond. Reusing its expiry is not a renewal.
pub(crate) fn git_lease_renewal_expiry(now: u64, previous_expiry: u64) -> Result<u64, String> {
    now.checked_add(GIT_OPERATION_LEASE_MILLIS)
        .zip(previous_expiry.checked_add(1))
        .map(|(deadline, successor)| deadline.max(successor))
        .ok_or_else(|| "Git compatibility lease expiry overflowed".to_owned())
}

impl ControlPlane {
    pub(crate) async fn git_tool(
            &mut self,
            agent_id: &str,
            route: &Route,
            root_id: WorkspaceRootId,
            argv: Vec<String>,
        ) -> Result<Value, String> {
            let route_root = route
                .roots
                .get(&root_key(root_id))
                .ok_or_else(|| "selected route root is missing".to_owned())?;
            let route_path = route.mount_path.join(route_root.mount_name());
            let lazy_workspace = self.lazy_workspace_root(route, root_id).await?;
            let workspace = lazy_workspace.workspace().clone();
            self.sync_agent(&route.parent_agent_id)
                .await
                .map_err(|error| format!("cannot synchronize the parent workspace: {error}"))?;
            let parent_context = WorkspaceContextRegistry::new(self.store.clone())
                .resolve(self.context_for_agent(&route.parent_agent_id)?)
                .await
                .map_err(|error| format!("cannot resolve the parent workspace context: {error}"))?;
            let parent = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, parent_context
                        .roots
                        .get(&root_id)
                        .ok_or_else(|| "direct parent root is missing".to_owned())?
                        .workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(display)?;
            let operations = OperationWindowCoordinator::new(self.fs.operation_window_store());
            let now = now_millis();
            operations
                .recover_workspace(&workspace, now, OperationReconcileLimits::default())
                .await
                .map_err(display)?;
            if !matches!(
                operations
                    .snapshot(workspace.id())
                    .await
                    .map_err(display)?
                    .phase,
                acyclic_fs::OperationWindowPhase::Idle
            ) {
                return Err("Git compatibility commands require an idle agent workspace".to_owned());
            }
            let repository_id = repository_id(
                workspace.id().into_bytes(),
                route_root.repository_workspace_id,
            );
            let lease = operations
                .begin(
                    workspace.id(),
                    parent.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(display)?.id(),
                    format!("{agent_id}:git"),
                    now,
                    now.saturating_add(GIT_OPERATION_LEASE_MILLIS),
                )
                .await
                .map_err(display)?;
            let preparation: Result<_, String> = async {
                self.mounts
                    .get(agent_id)
                    .ok_or_else(|| "subagent mount is unavailable".to_owned())?
                    .sync()
                    .await
                    .map_err(display)?;
                let apply_patch = git_apply_patch(&lazy_workspace, &route_path, &argv).await?;
                let (ignore, merge_drivers) = git_policy(&lazy_workspace).await?;
                let head_tree =
                    git_workspace_tree(&lazy_workspace, &argv, Some(lease.publication_permit()))
                        .await?;
                Ok((apply_patch, ignore, merge_drivers, head_tree))
            }
            .await;
            let (apply_patch, ignore, merge_drivers, head_tree) = match preparation {
                Ok(prepared) => prepared,
                Err(error) => {
                    let _ = operations.finish(&lease, now).await;
                    return Err(error);
                }
            };
            let renewal_now = now_millis();
            let lease = match async {
                let expiry = git_lease_renewal_expiry(renewal_now, lease.expires_at_millis)?;
                operations
                    .renew(&lease, renewal_now, expiry)
                    .await
                    .map_err(display)
            }
            .await
            {
                Ok(renewed) => renewed,
                Err(error) => {
                    let _ = operations.finish(&lease, renewal_now).await;
                    return Err(error);
                }
            };
            let executor = PluginGitExecutor {
                fs: &self.fs,
                store: &self.store,
                current: workspace.clone(),
                lazy_current: lazy_workspace.clone(),
                repository_id,
                ignore,
                permit: lease.publication_permit(),
                lease: Some(lease.clone()),
                merge_drivers,
                merge_cache: AsyncMutex::new(MemoryMergeResolutionCache::default()),
                switched: Mutex::new(None),
            };
            let repository = GitCompatRepository::new(repository_id, self.store.clone());
            let command = run_git_command(
                &repository,
                &argv,
                head_tree,
                apply_patch.as_deref(),
                &self.author_for_argv(&argv, agent_id)?,
                i64::try_from(now / 1_000).unwrap_or(i64::MAX),
                &executor,
            )
            .await;
            let parent_head = parent.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map(|generation| generation.id());
            let observed: Result<(), String> = match parent_head {
                Ok(parent_head) => operations
                    .observe_parent(workspace.id(), parent_head)
                    .await
                    .map(|_| ())
                    .map_err(display),
                Err(error) => Err(display(error)),
            };
            let finish = operations
                .finish(&lease, now_millis())
                .await
                .map_err(display)?;
            match finish {
                acyclic_fs::OperationWindowFinish::Reconcile(reconcile) => {
                    match operations
                        .reconcile_workspace(&workspace, reconcile, OperationReconcileLimits::default())
                        .await
                        .map_err(display)?
                    {
                        acyclic_fs::WorkspaceRebase::Conflicted {
                            conflicts,
                            truncated,
                        } => {
                            return Err(format!(
                                "Git barrier close reported {} conflict(s); truncated={truncated}",
                                conflicts.len()
                            ));
                        }
                        acyclic_fs::WorkspaceRebase::Fenced => {
                            return Err("Git barrier close was fenced".to_owned());
                        }
                        acyclic_fs::WorkspaceRebase::IdempotencyConflict => {
                            return Err("Git barrier close reused an operation identity".to_owned());
                        }
                        _ => {}
                    }
                }
                acyclic_fs::OperationWindowFinish::StillActive { .. } => {
                    return Err("Git compatibility command overlapped another tool".to_owned());
                }
                acyclic_fs::OperationWindowFinish::AlreadyClosed => {
                    return Err("Git compatibility command lease expired".to_owned());
                }
            }
            observed?;
            let switched = executor.switched_workspace().map_err(display)?;
            drop(executor);
            self.mounts
                .get(agent_id)
                .ok_or_else(|| "subagent mount is unavailable".to_owned())?
                .advance_to_head()
                .await
                .map_err(display)?;
            if let Some(switched) = switched {
                self.unmount_agent(agent_id).await?;
                WorkspaceContextRegistry::new(self.store.clone())
                    .set_workspace(
                        WorkspaceContextId::from_bytes(route.context_id),
                        root_id,
                        switched.workspace().id(),
                        switched.workspace().name().as_str().to_owned(),
                        Some(parent.id()),
                    )
                    .await
                    .map_err(display)?;
                let current = self
                    .state
                    .routes
                    .get_mut(agent_id)
                    .ok_or_else(|| "subagent route is missing".to_owned())?;
                let active = current
                    .roots
                    .get_mut(&root_key(root_id))
                    .ok_or_else(|| "selected route root is missing".to_owned())?;
                active.published_generation = [0; 32];
                let current = current.clone();
                let mount = self.mount_route(&current).await?;
                self.mounts.insert(agent_id.to_owned(), mount);
            }
            let Some(output) = command? else {
                self.persist()?;
                return Err("recovered a pending Git transition; retry the current command".to_owned());
            };
            self.persist()?;
            serde_json::to_value(output).map_err(display)
        }

    pub(crate) async fn root_git_tool(
            &mut self,
            root_id: WorkspaceRootId,
            argv: Vec<String>,
        ) -> Result<Value, String> {
            self.sync_agent(&self.state.root_agent_id.clone()).await?;
            let lazy_workspace = self
                .roots
                .get(&root_key(root_id))
                .cloned()
                .ok_or_else(|| "root workspace is unavailable".to_owned())?;
            let root_binding = self
                .state
                .roots
                .get(&root_key(root_id))
                .cloned()
                .ok_or_else(|| "root binding is unavailable".to_owned())?;
            let workspace = lazy_workspace.workspace().clone();
            let before_tree = git_workspace_tree(&lazy_workspace, &argv, None).await?;
            let apply_patch = git_apply_patch(&lazy_workspace, &root_binding.path, &argv).await?;
            let repository_id = repository_id(
                workspace.id().into_bytes(),
                root_binding.created()?.repository_workspace_id,
            );
            let (ignore, merge_drivers) = git_policy(&lazy_workspace).await?;
            let executor = RootMaterializingGitExecutor {
                inner: PluginGitExecutor {
                    fs: &self.fs,
                    store: &self.store,
                    current: workspace.clone(),
                    lazy_current: lazy_workspace.clone(),
                    repository_id,
                    ignore,
                    permit: PublicationPermit::Unrestricted,
                    lease: None,
                    merge_drivers,
                    merge_cache: AsyncMutex::new(MemoryMergeResolutionCache::default()),
                    switched: Mutex::new(None),
                },
                root: &root_binding.path,
                source_root_identity: acyclic_fs::NativeRootIdentity::from_bytes(root_binding.created()?.native_root_identity),
                store: &self.store,
            };
            let repository = GitCompatRepository::new(repository_id, self.store.clone());
            let output = run_git_command(
                &repository,
                &argv,
                before_tree,
                apply_patch.as_deref(),
                &self.author_for_argv(&argv, &self.state.root_agent_id)?,
                i64::try_from(now_millis() / 1_000).unwrap_or(i64::MAX),
                &executor,
            )
            .await?;
            let switched = executor.switched_workspace().map_err(display)?;
            drop(executor);
            if let Some(switched) = switched {
                WorkspaceContextRegistry::new(self.store.clone())
                    .set_workspace(
                        WorkspaceContextId::from_bytes(self.state.root_context_id),
                        root_id,
                        switched.workspace().id(),
                        switched.workspace().name().as_str().to_owned(),
                        None,
                    )
                    .await
                    .map_err(display)?;
                self.roots.insert(root_key(root_id), switched);
            }
            self.persist()?;
            let Some(output) = output else {
                return Err("recovered a pending Git transition; retry the current command".to_owned());
            };
            serde_json::to_value(output).map_err(display)
        }

}
