//! Workspace access and native root synchronization.

use super::*;

/// Every path the workspace's head records, parents before children.
async fn recorded_paths(workspace: &LocalWorkspace) -> Result<Vec<NamespacePath>, String> {
    let head = workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(display)?;
    let mut recorded = Vec::new();
    let mut directories = vec!["/".to_owned()];
    while let Some(directory) = directories.pop() {
        let mut after = None;
        loop {
            let page = head.list_directory(&directory, after.as_ref(), 1_024, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .map_err(display)?;
            for entry in &page.entries {
                let name = entry
                    .name
                    .unicode_text()
                    .ok_or_else(|| "recorded name is not Unicode".to_owned())?;
                let path = if directory == "/" {
                    format!("/{name}")
                } else {
                    format!("{directory}/{name}")
                };
                recorded.push(path.clone());
                if entry.kind == FileKind::Directory {
                    directories.push(path);
                }
            }
            after = page.entries.last().map(|entry| entry.name.clone());
            if !page.has_more || after.is_none() {
                break;
            }
        }
    }
    recorded
        .iter()
        .map(|path| native_namespace_path(path))
        .collect()
}

impl ControlPlane {
    #[cfg(test)]
        pub(crate) async fn workspace(&self, route: &Route) -> Result<LocalWorkspace, String> {
            self.workspace_root(route, WorkspaceRootId::from_bytes(route.root_id))
                .await
        }

    pub(crate) async fn workspace_root(
            &self,
            route: &Route,
            root_id: WorkspaceRootId,
        ) -> Result<LocalWorkspace, String> {
            self.state.require_created_roots()?;
            if !route.roots.contains_key(&root_key(root_id)) {
                return Err("workspace route root is missing".to_owned());
            }
            let context = WorkspaceContextRegistry::new(self.store.clone())
                .resolve(WorkspaceContextId::from_bytes(route.context_id))
                .await
                .map_err(display)?;
            let workspace_id = context
                .roots
                .get(&root_id)
                .ok_or_else(|| "workspace route root is absent from its core context".to_owned())?
                .workspace_id;
            WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(display)
        }

    pub(crate) async fn lazy_workspace_root(
            &self,
            route: &Route,
            root_id: WorkspaceRootId,
        ) -> Result<LocalLazyWorkspace, String> {
            let workspace = self.workspace_root(route, root_id).await?;
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
            LazyWorkspace::open(workspace, source, self.store.clone())
                .await
                .map_err(display)
        }

    pub(crate) async fn mount_route(&self, route: &Route) -> Result<LocalMount, String> {
            self.validate_route_paths(route)?;
            let mut roots = Vec::with_capacity(route.roots.len());
            for root in route.roots.values() {
                let root_id = WorkspaceRootId::from_bytes(root.root_id);
                roots.push((
                    root.mount_name(),
                    self.lazy_workspace_root(route, root_id).await?,
                ));
            }
            LocalMount::mount(roots, &route.mount_path).await
        }

    pub(crate) async fn refresh_native_root(
            &mut self,
            root_id: WorkspaceRootId,
        ) -> Result<(), String> {
            self.state.require_created_roots()?;
            let key = root_key(root_id);
            let physical = self
                .physical_roots
                .get(&key)
                .cloned()
                .ok_or_else(|| "physical root source is unavailable".to_owned())?;
            let binding = self
                .state
                .roots
                .get(&key)
                .cloned()
                .ok_or_else(|| "physical root binding is missing".to_owned())?;
            physical
                .validate_path_identity(binding.path.clone())
                .await?;
            let observation = tokio::task::spawn_blocking({
                let physical = Arc::clone(&physical);
                move || physical.observe()
            })
            .await
            .map_err(|error| format!("physical root observation worker failed: {error}"))??;
            let source_advanced_elsewhere =
                binding.created()?.source_epoch != observation.prior_source.epoch;
            // On a file system that may drop a change without a trace (`ReFS`),
            // no batch proves the root unchanged: every path the root workspace
            // records is checked against the disk at each refresh. What it does
            // not record, forks read from the disk itself.
            let unreported = !physical
                .watcher
                .lock()
                .map_err(|_| "physical root watcher state is poisoned".to_owned())?
                .reports_every_change();
            if !observation_consumes_changes(&observation.batch) && !source_advanced_elsewhere && !unreported {
                return Ok(());
            }
            #[cfg(test)]
            if (observation_consumes_changes(&observation.batch) || source_advanced_elsewhere) && self.fail_after_watch_poll
            {
                self.fail_after_watch_poll = false;
                return Err("injected failure after shared watcher poll".to_owned());
            }
            let workspace = self
                .roots
                .get(&key)
                .ok_or_else(|| "root workspace is unavailable".to_owned())?
                .workspace()
                .clone();
            let root_identity = physical.source.inner().root_identity();
            let capture = CaptureOptions {
                source_root: binding.path.clone(),
                expected_root_identity: root_identity,
                maximum_paths: 262_144,
                maximum_extent_spans: 65_536,
            };
            let policy = CapturePolicy::excluding(reserved_root_paths()?).map_err(display)?;
            let mut checkout = workspace.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction(), acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .map_err(display)?;
            match observation.batch {
                WatchBatch::Changes { .. } if source_advanced_elsewhere => {
                    // The whole tree is captured, so all of it must report.
                    physical
                        .watcher
                        .lock()
                        .map_err(|_| "physical root watcher state is poisoned".to_owned())?
                        .watch_tree(&RESERVED_ROOT_NAMES)
                        .map_err(display)?;
                    capture_baseline(
                        &mut checkout,
                        &capture,
                        &policy,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(display)?;
                }
                batch @ WatchBatch::Changes { .. } => {
                    capture_watch_batch(
                        &mut checkout,
                        batch,
                        &capture,
                        &policy,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(display)?;
                }
                WatchBatch::RescanRequired { .. } => {
                    physical
                        .watcher
                        .lock()
                        .map_err(|_| "physical root watcher state is poisoned".to_owned())?
                        .begin_rescan()
                        .map_err(display)?;
                    // The whole tree is captured, so all of it must report; the
                    // watches go in before the scan, so nothing it reads can
                    // change unreported.
                    physical
                        .watcher
                        .lock()
                        .map_err(|_| "physical root watcher state is poisoned".to_owned())?
                        .watch_tree(&RESERVED_ROOT_NAMES)
                        .map_err(display)?;
                    capture_baseline(
                        &mut checkout,
                        &capture,
                        &policy,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(display)?;
                    let trailing = physical
                        .watcher
                        .lock()
                        .map_err(|_| "physical root watcher state is poisoned".to_owned())?
                        .finish_rescan()
                        .map_err(display)?;
                    capture_watch_batch(
                        &mut checkout,
                        trailing,
                        &capture,
                        &policy,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(display)?;
                }
            }
            if unreported {
                let recorded = recorded_paths(&workspace).await?;
                capture_watch_batch(
                    &mut checkout,
                    WatchBatch::Changes {
                        original: Default::default(),
                        epoch: 0,
                        first_sequence: 0,
                        next_sequence: 0,
                        changes: recorded.into_iter().map(WatchChange::Modified).collect(),
                    },
                    &capture,
                    &policy,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(display)?;
                if !observation_consumes_changes(&observation.batch)
                    && !source_advanced_elsewhere
                    && !checkout.has_pending_mutations()
                {
                    // Every recorded path still holds what the disk does.
                    return Ok(());
                }
            }
            if checkout.has_pending_mutations() {
                match checkout
                    .commit(
                        OperationId::new(),
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(display)?
                    .value
                {
                    CheckoutCommitOutcome::Committed { .. }
                    | CheckoutCommitOutcome::AlreadyCommitted { .. } => {}
                    CheckoutCommitOutcome::Conflict { .. }
                    | CheckoutCommitOutcome::Fenced { .. }
                    | CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                        return Err("physical root changed concurrently with reconciliation".to_owned());
                    }
                }
            }
            self.roots
                .get(&key)
                .ok_or_else(|| "root workspace is unavailable".to_owned())?
                .state(&mut acyclic_fs::kernel::KernelAccounting::Standalone {
                    allocations: Default::default(),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                    budget: WorkBudget::UNBOUNDED,
                    cancellation: &CancellationToken::new(),
                })
                .await
                .map_err(display)?;
            let binding = self
                .state
                .roots
                .get_mut(&key)
                .ok_or_else(|| "physical root binding is missing".to_owned())?;
            binding.created_mut()?.source_epoch = observation.source.epoch;
            self.persist()
        }

    pub(crate) async fn sync_agent(&mut self, agent_id: &str) -> Result<(), String> {
            self.ensure_agent_idle(agent_id)?;
            if agent_id == self.state.root_agent_id {
                let roots = self
                    .state
                    .roots
                    .values()
                    .map(|root| WorkspaceRootId::from_bytes(root.root_id))
                    .collect::<Vec<_>>();
                for root_id in roots {
                    self.refresh_native_root(root_id).await?;
                }
                return Ok(());
            }
            self.mounts
                .get(agent_id)
                .ok_or_else(|| "subagent mount is unavailable".to_owned())?
                .sync()
                .await
                .map_err(display)
        }

    pub(crate) async fn propagate_parent_advance(
            &self,
            parent_agent_id: &str,
            root_id: WorkspaceRootId,
        ) -> Result<(), String> {
            let parent_route = self
                .state
                .routes
                .get(parent_agent_id)
                .ok_or_else(|| "finished parent route is missing".to_owned())?;
            let parent = self.workspace_root(parent_route, root_id).await?;
            let mut pending = VecDeque::from([(
                parent_agent_id.to_owned(),
                parent.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(display)?.id(),
            )]);
            let root_key = root_key(root_id);
            let mut children = BTreeMap::<String, Vec<Route>>::new();
            for route in self.state.routes.values() {
                if route.roots.contains_key(&root_key) {
                    children
                        .entry(route.parent_agent_id.clone())
                        .or_default()
                        .push(route.clone());
                }
            }
            let operations = OperationWindowCoordinator::new(self.fs.operation_window_store());
            while let Some((parent_id, parent_head)) = pending.pop_front() {
                for route in children.remove(&parent_id).unwrap_or_default() {
                    let child = self.workspace_root(&route, root_id).await?;
                    let outcome = operations
                        .parent_advanced_workspace(
                            &child,
                            parent_head,
                            OperationReconcileLimits::default(),
                        )
                        .await
                        .map_err(display)?;
                    match outcome {
                        Some(
                            acyclic_fs::WorkspaceRebase::Rebased(generation)
                            | acyclic_fs::WorkspaceRebase::AlreadyRebased(generation)
                            | acyclic_fs::WorkspaceRebase::Current(generation),
                        ) => pending.push_back((route.agent_id, generation.id())),
                        Some(acyclic_fs::WorkspaceRebase::Conflicted { .. }) | None => {}
                        Some(
                            acyclic_fs::WorkspaceRebase::Stale(_)
                            | acyclic_fs::WorkspaceRebase::Fenced
                            | acyclic_fs::WorkspaceRebase::IdempotencyConflict,
                        ) => {
                            return Err("descendant parent reconciliation requires recovery".to_owned());
                        }
                    }
                }
            }
            Ok(())
        }

    pub(crate) fn ensure_agent_idle(&self, agent_id: &str) -> Result<(), String> {
            self.state.require_created_roots()?;
            if self
                .state
                .leases
                .values()
                .any(|lease| lease.agent_id == agent_id)
            {
                return Err(format!(
                    "agent '{agent_id}' has active filesystem operations; retry after they finish"
                ));
            }
            Ok(())
        }

}
