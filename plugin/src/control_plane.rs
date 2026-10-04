//! Routes, leases, and the workspace control plane.

use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RootBinding {
    pub(crate) root_id: [u8; 16],
    pub(crate) path: PathBuf,
    pub(crate) repository_workspace_id: [u8; 16],
    pub(crate) source_identity: [u8; 16],
    pub(crate) source_epoch: u64,
    pub(crate) native_root_identity: [u8; 16],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RouteRoot {
    pub(crate) root_id: [u8; 16],
    pub(crate) repository_workspace_id: [u8; 16],
    pub(crate) published_generation: [u8; 32],
}

impl RouteRoot {
    pub(crate) fn id(&self) -> WorkspaceRootId {
        WorkspaceRootId::from_bytes(self.root_id)
    }

    pub(crate) fn mount_name(&self) -> String {
        route_name(self.id())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Route {
    pub(crate) agent_id: String,
    pub(crate) turn_id: String,
    pub(crate) context_id: [u8; 16],
    pub(crate) root_id: [u8; 16],
    pub(crate) parent_agent_id: String,
    pub(crate) roots: BTreeMap<String, RouteRoot>,
    pub(crate) mount_path: PathBuf,
    pub(crate) lifecycle: RouteLifecycle,
}

impl Route {
    pub(crate) fn active_path(&self) -> Result<PathBuf, String> {
        let active = self
            .roots
            .get(&root_key(WorkspaceRootId::from_bytes(self.root_id)))
            .ok_or_else(|| "persisted route is missing its active root".to_owned())?;
        Ok(self.mount_path.join(active.mount_name()))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RouteLifecycle {
    #[default]
    Mounting,
    Active,
    StopRequested,
    Frozen,
}

impl RouteLifecycle {
    pub(crate) fn accepts_tools(self) -> bool {
        self == Self::Active
    }

    pub(crate) fn needs_mount(self) -> bool {
        self != Self::Frozen
    }

    pub(crate) fn is_frozen(self) -> bool {
        self == Self::Frozen
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PendingSpawn {
    pub(crate) parent_agent_id: String,
    pub(crate) tool_use_id: String,
    pub(crate) active_root_id: Option<[u8; 16]>,
    pub(crate) expires_at_millis: u64,
    pub(crate) workspace_name: String,
    pub(crate) fork_key: [u8; 16],
    pub(crate) roots: BTreeMap<String, RouteRoot>,
    pub(crate) mount_path: PathBuf,
    pub(crate) lifecycle: PendingSpawnLifecycle,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PendingSpawnLifecycle {
    #[default]
    Preparing,
    Mounting,
    Prepared,
    Discarding,
}

impl PendingSpawn {
    pub(crate) fn context_id(&self) -> WorkspaceContextId {
        WorkspaceContextId::from_bytes(self.fork_key)
    }

    pub(crate) fn mount_name(&self) -> String {
        compact_id(&self.fork_key)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct LeaseRecord {
    pub(crate) agent_id: String,
    pub(crate) turn_id: String,
    pub(crate) tool_name: String,
    pub(crate) roots: BTreeMap<String, RootLeaseRecord>,
    pub(crate) expires_at_millis: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RootLeaseRecord {
    pub(crate) root_id: [u8; 16],
    pub(crate) workspace_id: [u8; 16],
    pub(crate) lease_id: [u8; 16],
    pub(crate) pinned_parent: [u8; 32],
    pub(crate) lease_expires_at_millis: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DiscardWorkspace {
    pub(crate) name: String,
    pub(crate) delete_key: [u8; 16],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DiscardAgent {
    pub(crate) agent_id: String,
    pub(crate) path: PathBuf,
    pub(crate) repository_workspace_ids: Vec<[u8; 16]>,
    pub(crate) mount_detached: bool,
    pub(crate) workspaces: VecDeque<DiscardWorkspace>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PendingDiscard {
    pub(crate) parent_context_id: [u8; 16],
    pub(crate) child_context_id: [u8; 16],
    pub(crate) context_discarded: bool,
    pub(crate) agents: VecDeque<DiscardAgent>,
}

impl LeaseRecord {
    pub(crate) fn from_leases(
        agent_id: String,
        turn_id: String,
        tool_name: String,
        roots: impl IntoIterator<Item = (WorkspaceRootId, OperationWindowLease)>,
    ) -> Self {
        let roots = roots
            .into_iter()
            .map(|(root_id, lease)| {
                (
                    root_key(root_id),
                    RootLeaseRecord {
                        root_id: root_id.into_bytes(),
                        workspace_id: lease.workspace_id.into_bytes(),
                        lease_id: lease.lease_id.into_bytes(),
                        pinned_parent: *lease.pinned_parent.digest().as_bytes(),
                        lease_expires_at_millis: lease.expires_at_millis,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let expires_at_millis = roots
            .values()
            .map(|root| root.lease_expires_at_millis)
            .min()
            .unwrap_or_default();
        Self {
            agent_id,
            turn_id,
            tool_name,
            roots,
            expires_at_millis,
        }
    }
}

impl RootLeaseRecord {
    pub(crate) fn lease(&self) -> OperationWindowLease {
        OperationWindowLease {
            workspace_id: acyclic_fs::WorkspaceId::from_bytes(self.workspace_id),
            lease_id: acyclic_fs::OperationLeaseId::from_bytes(self.lease_id),
            pinned_parent: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(
                self.pinned_parent,
            )),
            expires_at_millis: self.lease_expires_at_millis,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct AdapterState {
    pub(crate) version: u32,
    pub(crate) root_session_id: String,
    pub(crate) active: bool,
    pub(crate) root_agent_id: String,
    pub(crate) root_turns: BTreeSet<String>,
    pub(crate) root_context_id: [u8; 16],
    pub(crate) root_id: [u8; 16],
    pub(crate) roots: BTreeMap<String, RootBinding>,
    pub(crate) pending_root_registration: bool,
    pub(crate) pending_root_adoptions: BTreeSet<String>,
    pub(crate) routes: BTreeMap<String, Route>,
    pub(crate) turns: BTreeMap<String, String>,
    pub(crate) pending: VecDeque<PendingSpawn>,
    pub(crate) leases: BTreeMap<String, LeaseRecord>,
    pub(crate) pending_discards: BTreeMap<String, PendingDiscard>,
}

pub(crate) struct ControlPlane {
    pub(crate) data: PathBuf,
    pub(crate) config_root: PathBuf,
    pub(crate) fs: LocalFs,
    pub(crate) store: LocalCoreStateStore,
    pub(crate) distributed: LocalDistributedFs,
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
}

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
        let mut control = Self {
            data,
            config_root,
            distributed: DistributedFs::new(fs.clone(), store.clone()),
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

    pub(crate) fn author_for_argv(
        &self,
        argv: &[String],
        fallback: &str,
    ) -> Result<String, String> {
        let uses_default = argv.first().is_some_and(|command| command == "commit")
            && !argv
                .iter()
                .any(|argument| argument == "--author" || argument.starts_with("--author="));
        if !uses_default {
            return Ok(fallback.to_owned());
        }
        let path = self.config_root.join("author.json");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(fallback.to_owned());
            }
            Err(error) => return Err(format!("cannot read {}: {error}", path.display())),
        };
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct AuthorConfiguration {
            version: u32,
            author: String,
        }
        let configuration: AuthorConfiguration = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid {}: {error}", path.display()))?;
        if configuration.version != 1 || configuration.author.trim().is_empty() {
            return Err(format!(
                "invalid {}: expected version 1 and a non-empty author",
                path.display()
            ));
        }
        Ok(configuration.author)
    }

    pub(crate) async fn publication_coordinator(
        &self,
    ) -> Result<LocalPublicationCoordinator, String> {
        if self.roots.is_empty() {
            return Err("root workspace is unavailable".to_owned());
        }
        let root_context = self
            .distributed
            .contexts()
            .resolve(WorkspaceContextId::from_bytes(self.state.root_context_id))
            .await
            .map_err(display)?;
        let physical_roots = root_context
            .roots
            .into_iter()
            .map(|(root_id, root)| {
                (
                    root_id,
                    PhysicalRoot {
                        workspace_id: root.workspace_id,
                        path: root.source_path,
                    },
                )
            })
            .collect();
        let mut publisher = MaterializingWorkspaceMultiRootPublisher::new(
            self.distributed.clone(),
            PluginRootMaterializer {
                state: self.store.clone(),
                physical_roots,
            },
        );
        for binding in self.state.roots.values() {
            let root_id = WorkspaceRootId::from_bytes(binding.root_id);
            let workspace = self
                .roots
                .get(&root_key(root_id))
                .ok_or_else(|| "root workspace is unavailable".to_owned())?;
            publisher =
                publisher.with_root_merge_drivers(root_id, merge_drivers_for(workspace).await?);
        }
        Ok(self.distributed.publications(
            publisher,
            LineageMultiRootPublicationAuthorizer::new(self.store.clone()),
        ))
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
            .cloned()
            .ok_or_else(|| "published child route is unavailable".to_owned())?;
        for (root_id, root) in &publication.candidate.plan.roots {
            let parent_repository_id =
                self.parent_repository_id(parent_agent, *root_id, root.target_workspace_id)?;
            let published_generation = publication
                .published_generations
                .get(root_id)
                .copied()
                .ok_or_else(|| "published root generation is unavailable".to_owned())?;
            let workspace = self
                .distributed
                .workspace(root.target_workspace_id)
                .await
                .map_err(display)?;
            let published = workspace
                .generation(published_generation)
                .await
                .map_err(display)?;
            let ignore_text = match published.read("/.gitignore", 1024 * 1024).await {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(WorkspaceError::NotFound) => String::new(),
                Err(error) => return Err(display(error)),
            };
            let ignore =
                GitIgnorePolicy::parse(&format!("{ignore_text}\n{}", reserved_ignore_rules()));
            let repository = self.distributed.git(parent_repository_id);
            let tracked = repository.tracked_paths().await.map_err(display)?;
            let child_binding = child_route
                .roots
                .get(&root_key(*root_id))
                .ok_or_else(|| "published child root is unavailable".to_owned())?;
            let child_repository_id = repository_id(
                root.source_workspace_id.into_bytes(),
                child_binding.repository_workspace_id,
            );
            let source_head = self
                .distributed
                .git(child_repository_id)
                .head()
                .await
                .map_err(display)?;
            let mut capture_hasher = blake3::Hasher::new();
            capture_hasher.update(b"acyclic-publication-capture-v1\0");
            capture_hasher.update(&publication.candidate.plan.operation_id.into_bytes());
            capture_hasher.update(&root_id.into_bytes());
            let mut capture_id = [0_u8; 16];
            capture_id.copy_from_slice(&capture_hasher.finalize().as_bytes()[..16]);
            let captured = capture_git_compatible_generation_at(
                &workspace,
                published_generation,
                &ignore,
                &tracked,
                OperationId::from_bytes(capture_id),
            )
            .await
            .map_err(|error| format!("capturing published root {root_id:?}: {error}"))?;
            let capture_proof = captured
                .authenticated_proof(
                    OperationId::from_bytes(capture_id),
                    &self.distributed.lineage(),
                )
                .await
                .map_err(display)?;
            repository
                .record_publication(GitPublicationRecord {
                    tree: GitTreeRef::exact(
                        captured.generation.workspace_id(),
                        captured.generation.id(),
                    ),
                    workspace_tree: GitTreeRef::exact(workspace.id(), published_generation),
                    source_head,
                    tracked_paths: captured.tracked_paths,
                    capture_proof,
                    message: format!("Merge agents/{child_agent}"),
                    author: parent_agent.to_owned(),
                    authored_at_seconds: i64::try_from(now_millis() / 1_000).unwrap_or(i64::MAX),
                })
                .await
                .map_err(display)?;
        }
        Ok(())
    }

    pub(crate) async fn finalize_applied_publication(
        &mut self,
        coordinator: &LocalPublicationCoordinator,
        parent_agent: &str,
        child_agent: &str,
        publication: &MultiRootPublication,
    ) -> Result<(), String> {
        self.record_publication_history(parent_agent, child_agent, publication)
            .await
            .map_err(|error| format!("recording published compatibility history: {error}"))?;
        // Before the child rebases onto the new head: every path that
        // finishes a merge (a first attempt, `--continue`, recovery) carries
        // the child's deletions of source files, which no generation holds.
        let deletions = self
            .source_deletions(parent_agent, child_agent, &publication.candidate.plan)
            .await?;
        self.apply_source_deletions(parent_agent, deletions).await?;
        let limits = OperationReconcileLimits::default();
        let mut published_heads = BTreeMap::new();
        for (root_id, root) in &publication.candidate.plan.roots {
            let source = self
                .distributed
                .workspace(root.source_workspace_id)
                .await
                .map_err(display)?;
            let rebase_key = derived_idempotency_key(
                IdempotencyKey::from_bytes(publication.candidate.plan.operation_id.into_bytes()),
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
                .map_err(|error| format!("rebasing published child root {root_id:?}: {error}"))?
            {
                acyclic_fs::WorkspaceRebase::Rebased(generation)
                | acyclic_fs::WorkspaceRebase::AlreadyRebased(generation)
                | acyclic_fs::WorkspaceRebase::Current(generation) => generation,
                acyclic_fs::WorkspaceRebase::Stale(_) => {
                    return Err(
                        "child workspace changed while finalizing its publication".to_owned()
                    );
                }
                acyclic_fs::WorkspaceRebase::Conflicted { .. } => {
                    return Err("published child could not advance to its parent result".to_owned());
                }
                acyclic_fs::WorkspaceRebase::Fenced => {
                    return Err("published child rebase was fenced".to_owned());
                }
                acyclic_fs::WorkspaceRebase::IdempotencyConflict => {
                    return Err("published child rebase reused an operation identity".to_owned());
                }
            };
            published_heads.insert(*root_id, generation.id());
        }
        if let Some(child_mount) = self.mounts.get(child_agent) {
            child_mount.advance_to_head().await.map_err(display)?;
        }
        let route = self
            .state
            .routes
            .get_mut(child_agent)
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
            "status": if abort { "aborted" } else { "applied" },
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
                self.distributed
                    .operations()
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

    pub(crate) async fn restore_root(&mut self) -> Result<(), String> {
        if self.state.root_session_id.is_empty() {
            return Ok(());
        }
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
            if workspace.id().into_bytes() != binding.repository_workspace_id {
                return Err("pending root workspace identity changed".to_owned());
            }
            self.distributed
                .lineage()
                .register_root(&workspace)
                .await
                .map_err(display)?;
            self.distributed
                .contexts()
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
        let mut context = self
            .distributed
            .contexts()
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
                let workspace = self
                    .distributed
                    .workspace(acyclic_fs::WorkspaceId::from_bytes(
                        binding.repository_workspace_id,
                    ))
                    .await
                    .map_err(display)?;
                context = self
                    .distributed
                    .contexts()
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
                        identity: root.source_identity,
                        epoch: root.source_epoch,
                    }),
                )
                .await?;
            let source = Arc::clone(&physical.source);
            verify_native_root_identity(&source, root.native_root_identity, &root.path)?;
            let refreshed = source.reference();
            if let Some(binding) = self.state.roots.get_mut(&key) {
                binding.source_epoch = refreshed.epoch;
            }
            let workspace = self
                .distributed
                .workspace(
                    context
                        .roots
                        .get(&WorkspaceRootId::from_bytes(root.root_id))
                        .ok_or_else(|| "adapter root is absent from its core context".to_owned())?
                        .workspace_id,
                )
                .await
                .map_err(display)?;
            self.roots.insert(
                key.clone(),
                self.distributed
                    .open_or_attach_lazy(workspace, Arc::clone(&source))
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
            let context = self
                .distributed
                .contexts()
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
                    .acquire(&canonical, SharedRootAdmission::Fresh)
                    .await?;
                let source = Arc::clone(&physical.source);
                let source_reference = source.reference();
                let workspace = self
                    .distributed
                    .attach_lazy_with_config(
                        &workspace_name,
                        Arc::clone(&source),
                        VolumeConfig::native(Lifecycle::Durable),
                    )
                    .await
                    .map_err(display)?;
                self.distributed
                    .lineage()
                    .register_root(workspace.workspace())
                    .await
                    .map_err(display)?;
                let key = root_key(root_id);
                self.state.roots.insert(
                    key.clone(),
                    RootBinding {
                        root_id: root_id.into_bytes(),
                        path: canonical.clone(),
                        repository_workspace_id: workspace.workspace().id().into_bytes(),
                        source_identity: source_reference.identity,
                        source_epoch: source_reference.epoch,
                        native_root_identity: source.inner().root_identity().to_bytes(),
                    },
                );
                self.state.pending_root_adoptions.insert(key.clone());
                self.persist()?;
                #[cfg(test)]
                if self.fail_after_root_intent {
                    self.fail_after_root_intent = false;
                    return Err("injected failure after root adoption intent".to_owned());
                }
                self.distributed
                    .contexts()
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
            .acquire(&canonical, SharedRootAdmission::Fresh)
            .await?;
        let source = Arc::clone(&physical.source);
        let source_reference = source.reference();
        // The root's binding, lineage, and context commit as one change set
        // with one flush. The durable intent below precedes it, so recovery
        // can repeat all three, binding the root afresh if its attach was lost.
        let (store, durability) = self.store.defer_durability();
        let distributed = DistributedFs::new(self.fs.clone(), store);
        let workspace = distributed
            .attach_lazy_with_config(
                &workspace_name,
                Arc::clone(&source),
                VolumeConfig::native(Lifecycle::Durable),
            )
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
            repository_workspace_id: workspace.workspace().id().into_bytes(),
            source_identity: source_reference.identity,
            source_epoch: source_reference.epoch,
            native_root_identity: source.inner().root_identity().to_bytes(),
        };
        self.state.roots.insert(root_key(root_id), binding);
        self.state.pending_root_registration = true;
        self.persist()?;
        #[cfg(test)]
        if self.fail_after_root_intent {
            self.fail_after_root_intent = false;
            return Err("injected failure after root registration intent".to_owned());
        }
        distributed
            .lineage()
            .register_root(workspace.workspace())
            .await
            .map_err(display)?;
        distributed
            .contexts()
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

    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn user_prompt(&mut self, input: Value) -> Result<Value, String> {
        let session_id = string(&input, "session_id")?;
        if session_id != self.state.root_session_id {
            return Err("user prompt belongs to another root session".to_owned());
        }
        let turn_id = string(&input, "turn_id")?;
        if self.state.turns.contains_key(&turn_id) {
            return Err("root turn identity is already bound to a subagent".to_owned());
        }
        self.remember_root_turn(turn_id);
        self.persist_unflushed()?;
        Ok(json!({"suppressOutput": true}))
    }

    pub(crate) async fn pre_tool(&mut self, input: Value) -> Result<Value, String> {
        self.require_session(&input)?;
        // Quarantine every expired writer before the mount can be advanced or
        // reused for a new operation. Recovery owns remounting after fencing.
        self.recover_expired_adapter_leases().await?;
        let tool_name = string(&input, "tool_name")?;
        let turn_id = string(&input, "turn_id")?;
        let tool_use_id = string(&input, "tool_use_id")?;
        let caller = self.resolve_turn(&turn_id)?;
        let caller_route = if caller == self.state.root_agent_id {
            None
        } else {
            let route = self
                .state
                .routes
                .get(&caller)
                .cloned()
                .ok_or_else(|| "subagent route is missing".to_owned())?;
            if !route.lifecycle.accepts_tools() {
                return Err("subagent workspace is sealed after SubagentStop".to_owned());
            }
            Some(route)
        };
        if is_spawn_tool(&tool_name) {
            let now = now_millis();
            self.discard_expired_pending_spawns(now).await?;
            if let Some(existing) =
                self.state.pending.iter().find(|spawn| {
                    spawn.parent_agent_id == caller && spawn.tool_use_id == tool_use_id
                })
            {
                return if existing.lifecycle == PendingSpawnLifecycle::Prepared {
                    Ok(json!({}))
                } else {
                    Err("the retried subagent spawn is not fully prepared".to_owned())
                };
            }
            if !self.state.pending.is_empty() {
                return Err(
                    "a subagent spawn handshake is already pending; retry after SubagentStart"
                        .to_owned(),
                );
            }
            let fork_key = IdempotencyKey::new();
            self.state.pending.push_back(PendingSpawn {
                parent_agent_id: caller,
                tool_use_id,
                active_root_id: input
                    .get("_caller_root_id")
                    .and_then(Value::as_str)
                    .and_then(|value| decode_fixed::<16>(value).ok()),
                expires_at_millis: now.saturating_add(2 * 60 * 1_000),
                workspace_name: format!(
                    "agent-{}-{}",
                    short_hash(self.state.root_session_id.as_bytes()),
                    hex::encode(fork_key.into_bytes())
                ),
                fork_key: fork_key.into_bytes(),
                roots: BTreeMap::new(),
                mount_path: self
                    .workspace_mount_root()
                    .join(compact_id(&fork_key.into_bytes())),
                lifecycle: PendingSpawnLifecycle::Preparing,
            });
            self.persist()?;
            if let Err(error) = self
                .prepare_pending_spawn(fork_key.into_bytes(), Survives::ServiceCrash)
                .await
            {
                let cleanup = self.discard_pending_spawn(fork_key.into_bytes()).await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(format!(
                        "{error}; failed to recover the rejected spawn: {cleanup}"
                    )),
                };
            }
            return Ok(json!({}));
        }
        if caller == self.state.root_agent_id {
            if tool_name.starts_with("mcp__acyclic__") {
                let mut updated =
                    normalize_tool_input(input.get("tool_input").cloned().unwrap_or(Value::Null))?;
                let object = updated
                    .as_object_mut()
                    .ok_or_else(|| "workspace control tool input must be an object".to_owned())?;
                object.insert("_caller_turn_id".to_owned(), Value::String(turn_id));
                object.insert(
                    "_session_id".to_owned(),
                    Value::String(self.state.root_session_id.clone()),
                );
                return Ok(json!({
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "allow",
                        "updatedInput": updated
                    }
                }));
            }
            let original =
                normalize_tool_input(input.get("tool_input").cloned().unwrap_or(Value::Null))?;
            if is_acyclic_cli_invocation(&tool_name, &original)? {
                return Ok(pre_tool_update(original));
            }
            return Ok(json!({}));
        }
        let agent_id = caller;
        if is_known_non_filesystem_tool(&tool_name) {
            return Ok(json!({}));
        }
        if !is_filesystem_tool(&tool_name) {
            return Err(format!(
                "unclassified tool '{tool_name}' is denied inside an isolated subagent workspace"
            ));
        }
        let route = caller_route.ok_or_else(|| "subagent route is missing".to_owned())?;
        let selected_root_id = input
            .get("_caller_root_id")
            .and_then(Value::as_str)
            .and_then(|value| decode_fixed::<16>(value).ok())
            .map_or_else(
                || WorkspaceRootId::from_bytes(route.root_id),
                WorkspaceRootId::from_bytes,
            );
        let selected_route_root = route
            .roots
            .get(&root_key(selected_root_id))
            .ok_or_else(|| "caller root is absent from its workspace context".to_owned())?;
        let selected_binding = self
            .state
            .roots
            .get(&root_key(selected_root_id))
            .ok_or_else(|| "caller physical root binding is missing".to_owned())?;
        let selected_path = route.mount_path.join(selected_route_root.mount_name());
        let original =
            normalize_tool_input(input.get("tool_input").cloned().unwrap_or(Value::Null))?;
        for binding in self.state.roots.values() {
            reject_original_root_references(&original, None, &binding.path.to_string_lossy())?;
        }
        let acyclic_cli = is_acyclic_cli_invocation(&tool_name, &original)?;
        let mut updated =
            rewrite_tool_input(&tool_name, original, &selected_binding.path, &selected_path)?;
        if acyclic_cli {
            // The CLI opens its own core operation lease. Wrapping it in the host-tool
            // lease would deadlock the compatibility command behind itself.
            return Ok(pre_tool_update(updated));
        }
        if tool_name.starts_with("mcp__acyclic__") {
            let object = updated
                .as_object_mut()
                .ok_or_else(|| "workspace control tool input must be an object".to_owned())?;
            object.insert("_caller_turn_id".to_owned(), Value::String(turn_id.clone()));
            object.insert(
                "_session_id".to_owned(),
                Value::String(self.state.root_session_id.clone()),
            );
        }
        let now = now_millis();
        if let Some(existing) = self.state.leases.get(&tool_use_id) {
            if existing.agent_id != agent_id {
                return Err("tool hook retry belongs to another agent".to_owned());
            }
            if existing.expires_at_millis > now {
                return Ok(pre_tool_update(updated));
            }
            self.state.leases.remove(&tool_use_id);
        }
        let first_overlapping_operation = !self
            .state
            .leases
            .values()
            .any(|lease| lease.agent_id == agent_id);
        if first_overlapping_operation {
            self.mounts
                .get(&agent_id)
                .ok_or_else(|| "subagent mount is unavailable".to_owned())?
                .advance_to_head()
                .await
                .map_err(display)?;
        }
        let operations = self.distributed.operations();
        let parent_context = self
            .distributed
            .contexts()
            .resolve(self.context_for_agent(&route.parent_agent_id)?)
            .await
            .map_err(display)?;
        let mut leases = Vec::with_capacity(route.roots.len());
        for route_root in route.roots.values() {
            let root_id = WorkspaceRootId::from_bytes(route_root.root_id);
            let acquired = async {
                let workspace = self.workspace_root(&route, root_id).await?;
                let parent_root = parent_context
                    .roots
                    .get(&root_id)
                    .ok_or_else(|| "direct parent root is missing".to_owned())?;
                let parent = self
                    .distributed
                    .workspace(parent_root.workspace_id)
                    .await
                    .map_err(display)?;
                operations
                    .recover_workspace(&workspace, now, OperationReconcileLimits::default())
                    .await
                    .map_err(display)?;
                operations
                    .begin(
                        workspace.id(),
                        parent.head().await.map_err(display)?.id(),
                        format!("{agent_id}:{tool_use_id}:{}", route_root.mount_name()),
                        now,
                        now.saturating_add(15 * 60 * 1_000),
                    )
                    .await
                    .map_err(display)
            }
            .await;
            match acquired {
                Ok(lease) => leases.push((root_id, lease)),
                Err(error) => {
                    self.rollback_started_leases(&route, &leases, now).await?;
                    return Err(error);
                }
            }
        }
        self.state.leases.insert(
            tool_use_id.clone(),
            LeaseRecord::from_leases(agent_id, turn_id, tool_name, leases),
        );
        // The record is the request's last transition. Losing it leaves the
        // opened leases unrecorded, as a crash before this save does, and the
        // store fences and reconciles them when they expire.
        if let Err(error) = self.persist_unflushed() {
            let leases = self
                .state
                .leases
                .get(&tool_use_id)
                .map_or_else(Vec::new, |record| {
                    record
                        .roots
                        .values()
                        .map(|root| (WorkspaceRootId::from_bytes(root.root_id), root.lease()))
                        .collect()
                });
            self.state.leases.remove(&tool_use_id);
            self.rollback_started_leases(&route, &leases, now).await?;
            return Err(error);
        }
        Ok(pre_tool_update(updated))
    }

    pub(crate) async fn rollback_started_leases(
        &self,
        route: &Route,
        leases: &[(WorkspaceRootId, OperationWindowLease)],
        now: u64,
    ) -> Result<(), String> {
        let operations = self.distributed.operations();
        for (root_id, lease) in leases.iter().rev() {
            let workspace = self.workspace_root(route, *root_id).await?;
            match operations.finish(lease, now).await.map_err(display)? {
                acyclic_fs::OperationWindowFinish::Reconcile(reconcile) => {
                    operations
                        .reconcile_workspace(
                            &workspace,
                            reconcile,
                            OperationReconcileLimits::default(),
                        )
                        .await
                        .map_err(display)?;
                }
                acyclic_fs::OperationWindowFinish::StillActive { .. }
                | acyclic_fs::OperationWindowFinish::AlreadyClosed => {}
            }
        }
        Ok(())
    }

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
        self.mounts
            .get(agent_id)
            .ok_or_else(|| "subagent mount is unavailable".to_owned())?
            .sync()
            .await
            .map_err(display)?;
        let lazy_workspace = self.lazy_workspace_root(route, root_id).await?;
        let workspace = lazy_workspace.workspace().clone();
        self.sync_agent(&route.parent_agent_id)
            .await
            .map_err(|error| format!("cannot synchronize the parent workspace: {error}"))?;
        let parent_context = self
            .distributed
            .contexts()
            .resolve(self.context_for_agent(&route.parent_agent_id)?)
            .await
            .map_err(|error| format!("cannot resolve the parent workspace context: {error}"))?;
        let parent = self
            .distributed
            .workspace(
                parent_context
                    .roots
                    .get(&root_id)
                    .ok_or_else(|| "direct parent root is missing".to_owned())?
                    .workspace_id,
            )
            .await
            .map_err(display)?;
        let operations = self.distributed.operations();
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
                parent.head().await.map_err(display)?.id(),
                format!("{agent_id}:git"),
                now,
                now.saturating_add(15 * 60 * 1_000),
            )
            .await
            .map_err(display)?;
        let preparation: Result<_, String> = async {
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
        let lease = match operations
            .renew(
                &lease,
                renewal_now,
                renewal_now.saturating_add(15 * 60 * 1_000),
            )
            .await
        {
            Ok(renewed) => renewed,
            Err(error) => {
                let _ = operations.finish(&lease, renewal_now).await;
                return Err(display(error));
            }
        };
        let executor = PluginGitExecutor {
            fs: &self.fs,
            distributed: &self.distributed,
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
        let repository = self.distributed.git(repository_id);
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
        // A retained Git transition owns the child's workspace until the
        // caller continues or aborts it.  Reconciling that conflicted tree
        // with the parent here would turn a local Git conflict into a parent
        // barrier failure, and would make the subsequent abort impossible.
        // Successful non-transition commands can still coalesce a parent
        // advance before closing their operation window.
        let local_abort = matches!(
            argv.as_slice(),
            [command, option]
                if matches!(command.as_str(), "merge" | "rebase") && option == "--abort"
        );
        let reconcile_parent = command.as_ref().is_ok_and(Option::is_some) && !local_abort;
        let observed: Result<(), String> = if reconcile_parent {
            let parent_head = parent.head().await.map(|generation| generation.id());
            match parent_head {
                Ok(parent_head) => operations
                    .observe_parent(workspace.id(), parent_head)
                    .await
                    .map(|_| ())
                    .map_err(display),
                Err(error) => Err(display(error)),
            }
        } else {
            Ok(())
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
            self.distributed
                .contexts()
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
            root_binding.repository_workspace_id,
        );
        let (ignore, merge_drivers) = git_policy(&lazy_workspace).await?;
        let executor = RootMaterializingGitExecutor {
            inner: PluginGitExecutor {
                fs: &self.fs,
                distributed: &self.distributed,
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
            store: &self.store,
        };
        let repository = self.distributed.git(repository_id);
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
            self.distributed
                .contexts()
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

    pub(crate) async fn subagent_start(&mut self, input: Value) -> Result<Value, String> {
        self.require_session(&input)?;
        let agent_id = string(&input, "agent_id")?;
        let turn_id = string(&input, "turn_id")?;
        let authenticated_parent = input
            .get("_authenticated_parent_agent_id")
            .and_then(Value::as_str);
        if self.state.root_turns.contains(&turn_id) {
            return Err("subagent turn identity is already bound to the root".to_owned());
        }
        if self
            .state
            .turns
            .get(&turn_id)
            .is_some_and(|bound| bound != &agent_id)
        {
            return Err("subagent turn identity is already bound to another agent".to_owned());
        }
        if self.state.routes.contains_key(&agent_id) {
            let route = self
                .state
                .routes
                .get(&agent_id)
                .cloned()
                .ok_or("route missing")?;
            if route.lifecycle == RouteLifecycle::Mounting {
                if authenticated_parent != Some(route.parent_agent_id.as_str()) {
                    return Err(
                        "subagent mount recovery is not authenticated by its direct parent"
                            .to_owned(),
                    );
                }
                fs::create_dir_all(&route.mount_path).map_err(display)?;
                let mount = self.mount_route(&route).await?;
                self.distributed
                    .contexts()
                    .set_active(WorkspaceContextId::from_bytes(route.context_id), true)
                    .await
                    .map_err(display)?;
                let recovered = self
                    .state
                    .routes
                    .get_mut(&agent_id)
                    .ok_or("route missing")?;
                recovered.lifecycle = RouteLifecycle::Active;
                recovered.turn_id.clone_from(&turn_id);
                self.mounts.insert(agent_id.clone(), mount);
                self.state.turns.insert(turn_id, agent_id.clone());
                self.persist()?;
                let route = self.state.routes.get(&agent_id).ok_or("route missing")?;
                return Ok(subagent_context(&route.active_path()?));
            }
            if route.lifecycle == RouteLifecycle::StopRequested {
                return Err("subagent stop is pending until its live descendants finish".to_owned());
            }
            if !route.lifecycle.is_frozen() && route.turn_id != turn_id {
                return Err("subagent identity is already bound to another turn".to_owned());
            }
            if route.lifecycle.is_frozen() {
                if authenticated_parent != Some(route.parent_agent_id.as_str()) {
                    return Err(
                        "subagent resume is not authenticated by its direct parent".to_owned()
                    );
                }
                fs::create_dir_all(&route.mount_path).map_err(display)?;
                let mount = self.mount_route(&route).await?;
                self.distributed
                    .contexts()
                    .set_active(WorkspaceContextId::from_bytes(route.context_id), true)
                    .await
                    .map_err(display)?;
                let resumed = self
                    .state
                    .routes
                    .get_mut(&agent_id)
                    .ok_or("route missing")?;
                resumed.lifecycle = RouteLifecycle::Active;
                resumed.turn_id.clone_from(&turn_id);
                self.mounts.insert(agent_id.clone(), mount);
            }
            self.state.turns.insert(turn_id, agent_id.clone());
            self.persist()?;
            let route = self.state.routes.get(&agent_id).ok_or("route missing")?;
            return Ok(subagent_context(&route.active_path()?));
        }
        let pending = self
            .state
            .pending
            .front()
            .cloned()
            .ok_or_else(|| "subagent start has no serialized spawn handshake".to_owned())?;
        if pending.expires_at_millis <= now_millis() {
            self.discard_pending_spawn(pending.fork_key).await?;
            return Err("subagent spawn handshake expired before SubagentStart".to_owned());
        }
        if authenticated_parent.is_some_and(|parent| parent != pending.parent_agent_id) {
            return Err("subagent start parent does not match its spawn permit".to_owned());
        }
        if pending.lifecycle != PendingSpawnLifecycle::Prepared {
            return Err("subagent spawn workspace is not fully prepared".to_owned());
        }
        let active_root_id = self.pending_active_root(&pending)?;
        if !pending.roots.contains_key(&root_key(active_root_id)) {
            return Err("active root is missing from prepared child context".to_owned());
        }
        let mount = self
            .pending_mounts
            .remove(&pending.fork_key)
            .ok_or_else(|| "prepared child mount is unavailable".to_owned())?;
        let route = Route {
            agent_id: agent_id.clone(),
            turn_id: turn_id.clone(),
            context_id: pending.fork_key,
            root_id: active_root_id.into_bytes(),
            parent_agent_id: pending.parent_agent_id.clone(),
            roots: pending.roots.clone(),
            mount_path: pending.mount_path.clone(),
            lifecycle: RouteLifecycle::Active,
        };
        let active_path = route.active_path()?;
        let consumed = self
            .state
            .pending
            .pop_front()
            .ok_or_else(|| "subagent spawn handshake disappeared during start".to_owned())?;
        if consumed.fork_key != pending.fork_key {
            return Err("subagent spawn handshake changed during start".to_owned());
        }
        self.state.turns.insert(turn_id, agent_id.clone());
        self.state.routes.insert(agent_id.clone(), route);
        // The route is the request's last transition. Losing it leaves the
        // prepared spawn, as a crash before this save does, which recovery
        // prepares again or discards.
        if let Err(error) = self.persist_unflushed() {
            self.state.routes.remove(&agent_id);
            self.state.turns.retain(|_, bound| bound != &agent_id);
            self.state.pending.push_front(consumed);
            self.pending_mounts.insert(pending.fork_key, mount);
            return Err(error);
        }
        self.mounts.insert(agent_id.clone(), mount);
        Ok(subagent_context(&active_path))
    }

    pub(crate) async fn post_tool(&mut self, input: Value) -> Result<Value, String> {
        self.require_session(&input)?;
        let turn_id = string(&input, "turn_id")?;
        let tool_name = string(&input, "tool_name")?;
        let caller = self.resolve_turn(&turn_id)?;
        let tool_use_id = string(&input, "tool_use_id")?;
        let Some(record) = self.state.leases.get(&tool_use_id).cloned() else {
            if caller != self.state.root_agent_id
                && !is_known_non_filesystem_tool(&tool_name)
                && !is_filesystem_tool(&tool_name)
                && !is_spawn_tool(&tool_name)
            {
                return Err(format!(
                    "unclassified post-tool '{tool_name}' is denied inside an isolated subagent workspace"
                ));
            }
            return Ok(json!({}));
        };
        if record.agent_id != caller
            || (!record.turn_id.is_empty() && record.turn_id != turn_id)
            || (!record.tool_name.is_empty() && record.tool_name != tool_name)
        {
            return Err("post-tool hook does not match its pre-tool owner".to_owned());
        }
        let route = self
            .state
            .routes
            .get(&record.agent_id)
            .cloned()
            .ok_or_else(|| "post-tool route is missing".to_owned())?;
        let now = now_millis();
        // A single live lease captures the shared mount once. Earlier closes
        // only release their fences; an expired peer cannot remain the capture
        // owner. The core still decides whether this close owns reconciliation.
        let last_live_child_lease = !self.state.leases.iter().any(|(id, lease)| {
            id != &tool_use_id && lease.agent_id == record.agent_id && lease.expires_at_millis > now
        });
        // An active parent tool has no newer stable generation yet; the child
        // catches up in a later operation window or explicit join.
        if last_live_child_lease && self.ensure_agent_idle(&route.parent_agent_id).is_ok() {
            self.sync_agent(&route.parent_agent_id)
                .await
                .map_err(|error| format!("cannot synchronize the parent workspace: {error}"))?;
        }
        // The final close rebases each root onto its parent's head itself.
        let operations = self.distributed.operations();
        let mut sync_error = None;
        let mut expired = false;
        let mut conflicts = Vec::new();
        let mut truncated = false;
        let mut finished_roots = Vec::new();
        for root_record in record.roots.values() {
            let root_id = WorkspaceRootId::from_bytes(root_record.root_id);
            let workspace = self
                .workspace_root(&route, root_id)
                .await
                .map_err(|error| format!("cannot resolve the child workspace root: {error}"))?;
            let lease = root_record.lease();
            if last_live_child_lease && sync_error.is_none() {
                let sync = match self.mounts.get(&record.agent_id) {
                    Some(mount) => mount
                        .sync_route_with_permit(
                            route_name(root_id).as_bytes(),
                            lease.publication_permit(),
                        )
                        .await
                        .map_err(display),
                    None => Err("subagent mount is quarantined after a fenced writer".to_owned()),
                };
                if let Err(error) = sync {
                    sync_error = Some(error);
                }
            }
            match operations
                .finish_workspace(&workspace, &lease, now, OperationReconcileLimits::default())
                .await
                .map_err(|error| format!("cannot close the filesystem operation lease: {error}"))?
            {
                WorkspaceOperationFinish::StillActive { .. } => {}
                WorkspaceOperationFinish::Reconciled(rebase) => {
                    finished_roots.push(root_id);
                    if let acyclic_fs::WorkspaceRebase::Conflicted {
                        conflicts: root_conflicts,
                        truncated: root_truncated,
                    } = rebase
                    {
                        conflicts.extend(root_conflicts);
                        truncated |= root_truncated;
                    }
                }
                WorkspaceOperationFinish::AlreadyClosed => expired = true,
            }
        }
        self.state.leases.remove(&tool_use_id);
        let propagated = if sync_error.is_none() && !expired {
            let mut propagated = Ok(());
            for root_id in finished_roots {
                propagated = self
                    .propagate_parent_advance(&record.agent_id, root_id)
                    .await;
                if propagated.is_err() {
                    break;
                }
            }
            propagated
        } else {
            Ok(())
        };
        // The close is saved last, so that it is the request's final effect;
        // the operation is already closed in the store, and recovery closes a
        // lease whose close was lost.
        self.persist_unflushed()
            .map_err(|error| format!("cannot persist the closed tool lease: {error}"))?;
        if let Some(error) = sync_error {
            let quarantine_error = self
                .mounts
                .remove(&record.agent_id)
                .and_then(|mount| mount.abandon().err())
                .map(|cleanup| format!("; mount quarantine also failed: {cleanup}"))
                .unwrap_or_default();
            return Err(format!(
                "filesystem tool publication was fenced before lease close: {error}{quarantine_error}"
            ));
        }
        if expired {
            return Err("filesystem tool lease expired; late writes were fenced".to_owned());
        }
        propagated?;
        if conflicts.is_empty() {
            Ok(json!({}))
        } else {
            Ok(json!({
                "systemMessage": format!(
                    "Acyclic workspace rebase reported {} typed conflict(s); truncated={truncated}",
                    conflicts.len()
                ),
                "conflicts": conflicts.iter().map(conflict_json).collect::<Vec<_>>(),
                "truncated": truncated
            }))
        }
    }

    pub(crate) async fn subagent_stop(&mut self, input: Value) -> Result<Value, String> {
        self.require_session(&input)?;
        self.recover_expired_adapter_leases().await?;
        let agent_id = string(&input, "agent_id")?;
        let turn_id = string(&input, "turn_id")?;
        if self.resolve_turn(&turn_id)? != agent_id {
            return Err("subagent stop does not belong to the routed agent turn".to_owned());
        }
        let route = self
            .state
            .routes
            .get(&agent_id)
            .cloned()
            .ok_or_else(|| "subagent stop refers to an unknown agent".to_owned())?;
        if route.lifecycle.is_frozen() {
            return Ok(json!({"suppressOutput": true}));
        }
        if self
            .state
            .leases
            .values()
            .any(|lease| lease.agent_id == agent_id)
        {
            return Err("subagent cannot stop while filesystem tools are still active".to_owned());
        }
        self.state
            .routes
            .get_mut(&agent_id)
            .ok_or_else(|| "subagent route disappeared during stop".to_owned())?
            .lifecycle = RouteLifecycle::StopRequested;
        // Persist the intent before unmounting. If this process dies, restore
        // remounts the route and the next lifecycle event can finish the same
        // transition without losing a descendant's publication target.
        self.persist()?;
        self.finalize_requested_stops(&agent_id).await?;
        let descendants = self
            .state
            .routes
            .values()
            .filter(|candidate| candidate.parent_agent_id == agent_id)
            .map(|candidate| format!("agents/{}", candidate.agent_id))
            .collect::<Vec<_>>();
        let changes = self
            .agent_changes_as(
                &route.parent_agent_id,
                json!({"agent": agent_id, "path": "."}),
            )
            .await
            .unwrap_or_else(|error| json!({"unavailable": error}));
        let reference = format!("agents/{agent_id}");
        let deferred = self
            .state
            .routes
            .get(&agent_id)
            .is_some_and(|route| route.lifecycle == RouteLifecycle::StopRequested);
        let summary = json!({
            "agent": reference,
            "state": if deferred { "stopping" } else { "frozen" },
            "changed": changes,
            "tests": {"status": "not-reported", "detail": "Acyclic does not infer test execution from process output"},
            "pendingDescendants": descendants,
            "actions": {
                "inspect": format!("acyclic git diff {reference}"),
                "merge": format!("acyclic git merge {reference}"),
                "discard": format!("acyclic discard {reference}"),
            }
        });
        Ok(json!({
            "suppressOutput": deferred,
            "systemMessage": if deferred {
                format!("Acyclic will freeze {reference} after its live descendants finish.")
            } else {
                format!(
                    "Acyclic stopped {reference}. Inspect with `acyclic git diff {reference}`; merge with `acyclic git merge {reference}`; discard with `acyclic discard {reference}`."
                )
            },
            "acyclicSummary": summary,
        }))
    }

    pub(crate) fn has_live_descendant(&self, ancestor: &str) -> bool {
        self.state.routes.values().any(|candidate| {
            if candidate.lifecycle.is_frozen() {
                return false;
            }
            let mut parent = candidate.parent_agent_id.as_str();
            let mut visited = BTreeSet::new();
            while parent != self.state.root_agent_id && visited.insert(parent.to_owned()) {
                if parent == ancestor {
                    return true;
                }
                let Some(route) = self.state.routes.get(parent) else {
                    break;
                };
                parent = &route.parent_agent_id;
            }
            false
        })
    }

    pub(crate) async fn finalize_requested_stops(&mut self, agent_id: &str) -> Result<(), String> {
        let mut candidate = Some(agent_id.to_owned());
        while let Some(agent_id) = candidate {
            let Some(route) = self.state.routes.get(&agent_id).cloned() else {
                break;
            };
            if route.lifecycle != RouteLifecycle::StopRequested
                || self.has_live_descendant(&agent_id)
            {
                break;
            }
            if self
                .state
                .leases
                .values()
                .any(|lease| lease.agent_id == agent_id)
            {
                return Err(format!(
                    "subagent '{agent_id}' cannot finish stopping while filesystem tools are active"
                ));
            }
            self.unmount_agent(&agent_id).await?;
            self.distributed
                .contexts()
                .set_active(WorkspaceContextId::from_bytes(route.context_id), false)
                .await
                .map_err(display)?;
            self.state
                .routes
                .get_mut(&agent_id)
                .ok_or_else(|| "subagent route disappeared during stop".to_owned())?
                .lifecycle = RouteLifecycle::Frozen;
            self.persist()?;
            candidate = (route.parent_agent_id != self.state.root_agent_id)
                .then_some(route.parent_agent_id);
        }
        Ok(())
    }

    pub(crate) async fn agents_status(&mut self, caller: &str) -> Result<Value, String> {
        let routes = self
            .state
            .routes
            .values()
            .filter(|route| {
                route.parent_agent_id == caller
                    || caller == self.state.root_agent_id
                    || route.agent_id == caller
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut agents = Vec::with_capacity(routes.len());
        for route in routes {
            let mut depth = 1_usize;
            let mut parent = route.parent_agent_id.as_str();
            let mut visited = BTreeSet::new();
            while parent != self.state.root_agent_id && visited.insert(parent.to_owned()) {
                let Some(ancestor) = self.state.routes.get(parent) else {
                    break;
                };
                depth += 1;
                parent = &ancestor.parent_agent_id;
            }
            let active_leases = self
                .state
                .leases
                .values()
                .filter(|lease| lease.agent_id == route.agent_id)
                .count();
            let descendants = self
                .state
                .routes
                .values()
                .filter(|candidate| candidate.parent_agent_id == route.agent_id)
                .count();
            let conflict = self
                .pending_conflict_for_parent(WorkspaceContextId::from_bytes(route.context_id))
                .await?
                .is_some();
            let context = self
                .distributed
                .contexts()
                .resolve(WorkspaceContextId::from_bytes(route.context_id))
                .await
                .map_err(display)?;
            let mut roots = Vec::with_capacity(route.roots.len());
            let mut pending_publications = 0_usize;
            for root in route.roots.values() {
                let workspace_id = context
                    .roots
                    .get(&WorkspaceRootId::from_bytes(root.root_id))
                    .ok_or_else(|| "adapter route is absent from its core context".to_owned())?
                    .workspace_id;
                let current = self
                    .distributed
                    .workspace(workspace_id)
                    .await
                    .map_err(display)?
                    .head()
                    .await
                    .map_err(display)?
                    .id();
                let current = *current.digest().as_bytes();
                let unpublished = root.published_generation != current;
                pending_publications += usize::from(unpublished);
                roots.push(json!({
                    "id": hex::encode(root.root_id),
                    "route": root.mount_name(),
                    "workspace": hex::encode(workspace_id.into_bytes()),
                    "generation": hex::encode(current),
                    "publishedGeneration": if root.published_generation == [0; 32] {
                        Value::Null
                    } else {
                        Value::String(hex::encode(root.published_generation))
                    },
                    "unpublished": unpublished,
                }));
            }
            let changes = if active_leases == 0 {
                self.agent_changes_as(caller, json!({"agent": route.agent_id, "path": "."}))
                    .await
                    .ok()
            } else {
                None
            };
            let file_changes = changes
                .as_ref()
                .and_then(|value| value.get("fileChanges"))
                .and_then(Value::as_u64);
            let binding_changes = changes
                .as_ref()
                .and_then(|value| value.get("bindingChanges"))
                .and_then(Value::as_u64);
            let changed_paths = changes
                .as_ref()
                .and_then(|value| value.get("roots"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .flat_map(|root| {
                    root.get("paths")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                })
                .take(128)
                .cloned()
                .collect::<Vec<_>>();
            agents.push(json!({
                "ref": format!("agents/{}", route.agent_id),
                "agent": route.agent_id,
                "parent": if route.parent_agent_id == self.state.root_agent_id {
                    Value::String("root".to_owned())
                } else {
                    Value::String(format!("agents/{}", route.parent_agent_id))
                },
                "depth": depth,
                "state": match route.lifecycle {
                    RouteLifecycle::Mounting => "mounting",
                    RouteLifecycle::Active => "running",
                    RouteLifecycle::StopRequested => "stopping",
                    RouteLifecycle::Frozen => "frozen",
                },
                "activeLeases": active_leases,
                "conflicts": usize::from(conflict),
                "descendants": descendants,
                "fileChanges": file_changes,
                "bindingChanges": binding_changes,
                "changedPaths": changed_paths,
                "workRemaining": file_changes.zip(binding_changes).map(|(files, bindings)| files.saturating_add(bindings)),
                "pendingPublications": pending_publications,
                "mount": route.active_path()?,
                "roots": roots,
            }));
        }
        agents.sort_by(|left, right| {
            left.get("depth")
                .and_then(Value::as_u64)
                .cmp(&right.get("depth").and_then(Value::as_u64))
                .then_with(|| {
                    left.get("ref")
                        .and_then(Value::as_str)
                        .cmp(&right.get("ref").and_then(Value::as_str))
                })
        });
        Ok(json!({"schemaVersion": 1, "root": self.state.root_agent_id, "agents": agents}))
    }

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
        let parent_context = self
            .distributed
            .contexts()
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
            let distributed = DistributedFs::new(self.fs.clone(), store);
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
                    source_binding.native_root_identity,
                    &source_binding.path,
                )?;
                let parent_workspace = distributed
                    .workspace(parent_root.workspace_id)
                    .await
                    .map_err(display)?;
                let parent_lazy = distributed
                    .open_lazy(parent_workspace, source)
                    .await
                    .map_err(display)?;
                let root_name = route_name(*root_id);
                let child_name = format!("{}-{root_name}", pending.workspace_name);
                let fork_generation = parent_lazy.workspace().head().await.map_err(display)?.id();
                let child = parent_lazy
                    .fork(&child_name, derived_idempotency_key(fork_key, *root_id))
                    .await
                    .map_err(display)?;
                distributed
                    .lineage()
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
            distributed
                .contexts()
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
            verify_native_root_identity(&source, binding.native_root_identity, &binding.path)?;
            let workspace = self
                .distributed
                .workspace(acyclic_fs::WorkspaceId::from_bytes(
                    root.repository_workspace_id,
                ))
                .await
                .map_err(display)?;
            roots.push((
                root.mount_name(),
                self.distributed
                    .open_lazy(workspace, source)
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
        if let Ok(context) = self
            .distributed
            .contexts()
            .resolve(pending.context_id())
            .await
        {
            match self
                .distributed
                .contexts()
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
            self.distributed
                .contexts()
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

    #[cfg(test)]
    pub(crate) async fn agent_changes(&mut self, input: Value) -> Result<Value, String> {
        let caller = self.caller(&input)?;
        self.agent_changes_as(&caller, input).await
    }

    pub(crate) async fn agent_changes_as(
        &mut self,
        caller: &str,
        input: Value,
    ) -> Result<Value, String> {
        let agent = string(&input, "agent")?;
        self.authorize_inspection(caller, &agent)?;
        self.ensure_agent_idle(&agent)?;
        if let Some(mount) = self.mounts.get(&agent) {
            mount.sync().await.map_err(display)?;
        }
        let route = self
            .state
            .routes
            .get(&agent)
            .cloned()
            .ok_or("unknown agent")?;
        let graph = self.distributed.lineage();
        let parent_context = self
            .distributed
            .contexts()
            .resolve(self.context_for_agent(&route.parent_agent_id)?)
            .await
            .map_err(display)?;
        let requested_path = input.get("path").and_then(Value::as_str);
        let selected_root = input
            .get("_caller_root_id")
            .and_then(Value::as_str)
            .and_then(|value| decode_fixed::<16>(value).ok());
        let mut roots = Vec::new();
        let mut file_changes = 0_usize;
        let mut binding_changes = 0_usize;
        let mut truncated = false;
        for route_root in route.roots.values() {
            if selected_root.is_some_and(|root| root != route_root.root_id) {
                continue;
            }
            let root_id = WorkspaceRootId::from_bytes(route_root.root_id);
            let workspace = self.workspace_root(&route, root_id).await?;
            let repository_id = repository_id(
                workspace.id().into_bytes(),
                route_root.repository_workspace_id,
            );
            let mut lineage_cursor = workspace.clone();
            let mut visited = BTreeSet::new();
            let mut branch_base = None;
            while lineage_cursor.id() != repository_id {
                if !visited.insert(lineage_cursor.id()) || visited.len() > 64 {
                    return Err("Git compatibility branch lineage is cyclic or too deep".to_owned());
                }
                let lineage = graph.resolve(lineage_cursor.id()).await.map_err(display)?;
                branch_base.get_or_insert(lineage.initial_generation);
                let parent_id = lineage.parent_workspace_id.ok_or_else(|| {
                    "Git compatibility branch does not reach its repository workspace".to_owned()
                })?;
                let parent = self
                    .distributed
                    .workspace(parent_id)
                    .await
                    .map_err(display)?;
                graph
                    .authorize_join(lineage_cursor.id(), parent.id())
                    .await
                    .map_err(display)?;
                lineage_cursor = parent;
            }
            let parent_root = parent_context
                .roots
                .get(&root_id)
                .ok_or_else(|| "direct parent root is missing".to_owned())?;
            let parent = self
                .distributed
                .workspace(parent_root.workspace_id)
                .await
                .map_err(display)?;
            let lineage = graph
                .authorize_join(lineage_cursor.id(), parent.id())
                .await
                .map_err(display)?;
            let base = workspace
                .generation(branch_base.unwrap_or(lineage.initial_generation))
                .await
                .map_err(display)?;
            let head = workspace.head().await.map_err(display)?;
            let changes = workspace
                .diff(&base, &head, 100_000)
                .await
                .map_err(display)?;
            let (root_files, root_bindings, paths) = if let Some(path) = requested_path {
                let filter = agent_change_filter(path, &route, route_root, workspace.profile())?;
                let paths = changes
                    .changed_paths(100_000)
                    .await
                    .map_err(display)?
                    .into_iter()
                    .filter(|change| change.path.is_within(&filter))
                    .collect::<Vec<_>>();
                let files = paths
                    .iter()
                    .filter(|change| change.before.is_some() && change.after.is_some())
                    .count();
                let bindings = paths.len().saturating_sub(files);
                let paths = paths
                    .iter()
                    .map(|change| namespace_path_text(&change.path))
                    .collect::<Result<Vec<_>, _>>()?;
                (files, bindings, paths)
            } else {
                (
                    changes.changes().files.len(),
                    changes.changes().bindings.len(),
                    Vec::new(),
                )
            };
            file_changes = file_changes.saturating_add(root_files);
            binding_changes = binding_changes.saturating_add(root_bindings);
            truncated |= changes.changes().truncated;
            roots.push(json!({
                "root": hex::encode(route_root.root_id),
                "workspace": hex::encode(workspace.id().into_bytes()),
                "generation": hex::encode(head.id().digest().as_bytes()),
                "fileChanges": root_files,
                "bindingChanges": root_bindings,
                "truncated": changes.changes().truncated,
                "paths": paths,
            }));
        }
        if roots.is_empty() {
            return Err("the selected path is outside the inspected workspace context".to_owned());
        }
        Ok(json!({
            "agent": agent,
            "fileChanges": file_changes,
            "bindingChanges": binding_changes,
            "truncated": truncated,
            "path": input.get("path").cloned().unwrap_or(Value::Null),
            "roots": roots,
        }))
    }

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
        let registry = self.distributed.contexts();
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
            let source = self
                .distributed
                .workspace(child_root.workspace_id)
                .await
                .map_err(display)?;
            let target = self
                .distributed
                .workspace(parent_root.workspace_id)
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
            let tracked = self
                .distributed
                .git(parent_repository_id)
                .tracked_paths()
                .await
                .map_err(display)?;
            let base = target
                .generation(plan.common_ancestor())
                .await
                .map_err(display)?;
            let source_head = source
                .generation(plan.source_head())
                .await
                .map_err(display)?;
            let changed = base
                .diff_to(&source_head, u32::MAX)
                .await
                .map_err(display)?
                .changed_paths(u32::MAX)
                .await
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
            let target = self
                .distributed
                .workspace(parent_root.workspace_id)
                .await
                .map_err(display)?;
            let generation = target.head().await.map_err(display)?.id();
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
                let removed_context = self
                    .distributed
                    .contexts()
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
                    let name = self
                        .distributed
                        .lineage()
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
            match self
                .distributed
                .contexts()
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

    #[cfg(test)]
    pub(crate) fn caller(&self, input: &Value) -> Result<String, String> {
        if input.get("_caller_agent_id").is_some() {
            return Err(
                "workspace control caller identity must be derived by the service".to_owned(),
            );
        }
        let turn = input
            .get("_caller_turn_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "workspace control call lacks a stable caller identity".to_owned())?;
        self.resolve_turn(turn)
            .map_err(|_| "workspace control caller is unknown".to_owned())
    }

    pub(crate) fn route_from_cwd(&self, cwd: &Path) -> Result<(String, Option<Route>), String> {
        self.route_root_from_cwd(cwd)
            .map(|(agent, route, _)| (agent, route))
    }

    /// Resolve a direct child workspace from a caller-owned parent path.
    ///
    /// The caller's process may be outside the child's mount, which is
    /// required on Windows because an unmount cannot complete while that
    /// process has the child as its current directory.  The selected path
    /// still has to identify a root in the caller's workspace so the service
    /// can select the corresponding child root.  Lineage is checked here;
    /// the agent id is a target selector, never a standalone grant.
    pub(crate) fn route_root_for_authorized_agent(
        &self,
        caller: &str,
        target_agent: &str,
        selected_cwd: &Path,
    ) -> Result<(String, Route, WorkspaceRootId), String> {
        if target_agent == caller {
            return Err("authorized workspace target must be a direct child".to_owned());
        }
        let root_id = self.root_id_for_caller_cwd(caller, selected_cwd)?;
        let route = self
            .state
            .routes
            .get(target_agent)
            .cloned()
            .ok_or_else(|| "authorized workspace target is unknown".to_owned())?;
        if route.parent_agent_id != caller {
            return Err("authorized workspace target is not a direct child".to_owned());
        }
        if !route.lifecycle.needs_mount() {
            return Err("authorized workspace target is not mounted".to_owned());
        }
        if !route.roots.contains_key(&root_key(root_id)) {
            return Err("authorized workspace target has no corresponding root".to_owned());
        }
        Ok((target_agent.to_owned(), route, root_id))
    }

    fn root_id_for_caller_cwd(&self, caller: &str, cwd: &Path) -> Result<WorkspaceRootId, String> {
        let canonical = cwd.canonicalize().map_err(display)?;
        let mut roots = Vec::new();
        if caller == self.state.root_agent_id {
            for root in self.state.roots.values() {
                if let Ok(path) = root.path.canonicalize()
                    && canonical.starts_with(&path)
                {
                    roots.push((
                        path.components().count(),
                        WorkspaceRootId::from_bytes(root.root_id),
                    ));
                }
            }
        } else {
            let route = self
                .state
                .routes
                .get(caller)
                .ok_or_else(|| "caller workspace route is unavailable".to_owned())?;
            for root in route.roots.values() {
                let path = route.mount_path.join(root.mount_name());
                if let Ok(path) = path.canonicalize()
                    && canonical.starts_with(&path)
                {
                    roots.push((
                        path.components().count(),
                        WorkspaceRootId::from_bytes(root.root_id),
                    ));
                }
            }
        }
        roots
            .into_iter()
            .max_by_key(|(depth, _)| *depth)
            .map(|(_, root_id)| root_id)
            .ok_or_else(|| {
                "authorized workspace routing requires the selected CWD in the caller workspace"
                    .to_owned()
            })
    }

    pub(crate) fn route_root_from_cwd(
        &self,
        cwd: &Path,
    ) -> Result<(String, Option<Route>, WorkspaceRootId), String> {
        let canonical = cwd.canonicalize().map_err(display)?;
        let mut mounted = Vec::new();
        for route in self
            .state
            .routes
            .values()
            .filter(|route| route.lifecycle.needs_mount())
        {
            for root in route.roots.values() {
                let path = route.mount_path.join(root.mount_name());
                if let Ok(path) = path.canonicalize()
                    && canonical.starts_with(&path)
                {
                    mounted.push((
                        path.components().count(),
                        route.clone(),
                        WorkspaceRootId::from_bytes(root.root_id),
                    ));
                }
            }
        }
        if let Some((_, route, root_id)) = mounted.into_iter().max_by_key(|(depth, _, _)| *depth) {
            return Ok((route.agent_id.clone(), Some(route), root_id));
        }
        if let Some((_, root)) = self
            .state
            .roots
            .values()
            .filter_map(|root| {
                root.path
                    .canonicalize()
                    .ok()
                    .filter(|path| canonical.starts_with(path))
                    .map(|path| (path.components().count(), root))
            })
            .max_by_key(|(depth, _)| *depth)
        {
            return Ok((
                self.state.root_agent_id.clone(),
                None,
                WorkspaceRootId::from_bytes(root.root_id),
            ));
        }
        Err("cwd is outside every root registered in this Acyclic session".to_owned())
    }

    pub(crate) fn require_session(&self, input: &Value) -> Result<(), String> {
        let session_id = string(input, "session_id")?;
        if session_id != self.state.root_session_id {
            return Err("lifecycle event belongs to another root session".to_owned());
        }
        Ok(())
    }

    pub(crate) fn context_for_agent(&self, agent_id: &str) -> Result<WorkspaceContextId, String> {
        if agent_id == self.state.root_agent_id {
            return Ok(WorkspaceContextId::from_bytes(self.state.root_context_id));
        }
        self.state
            .routes
            .get(agent_id)
            .map(|route| WorkspaceContextId::from_bytes(route.context_id))
            .ok_or_else(|| "workspace context alias is unknown".to_owned())
    }

    pub(crate) fn agent_for_context(&self, context_id: WorkspaceContextId) -> Option<String> {
        if self.state.root_context_id == context_id.into_bytes() {
            return Some(self.state.root_agent_id.clone());
        }
        self.state
            .routes
            .values()
            .find(|route| route.context_id == context_id.into_bytes())
            .map(|route| route.agent_id.clone())
    }

    pub(crate) fn resolve_turn(&self, turn_id: &str) -> Result<String, String> {
        if let Some(agent) = self.state.turns.get(turn_id) {
            return Ok(agent.clone());
        }
        if self.state.root_turns.contains(turn_id) {
            return Ok(self.state.root_agent_id.clone());
        }
        Err("tool call has no stable root or subagent identity".to_owned())
    }

    pub(crate) fn remember_root_turn(&mut self, turn_id: String) {
        self.state.root_turns.insert(turn_id);
        while self.state.root_turns.len() > MAXIMUM_ADAPTER_ROOT_TURNS {
            self.state.root_turns.pop_first();
        }
    }

    pub(crate) fn authorize_inspection(&self, caller: &str, target: &str) -> Result<(), String> {
        if caller == self.state.root_agent_id || caller == target {
            return Ok(());
        }
        let mut next = self.state.routes.get(target);
        while let Some(route) = next {
            if route.parent_agent_id == caller {
                return Ok(());
            }
            next = self.state.routes.get(&route.parent_agent_id);
        }
        Err("caller is not an ancestor of the requested agent".to_owned())
    }

    pub(crate) fn descendants(&self, agent: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut frontier = vec![agent.to_owned()];
        while let Some(parent) = frontier.pop() {
            for route in self.state.routes.values() {
                if route.parent_agent_id == parent {
                    result.push(route.agent_id.clone());
                    frontier.push(route.agent_id.clone());
                }
            }
        }
        result
    }

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
        if !route.roots.contains_key(&root_key(root_id)) {
            return Err("workspace route root is missing".to_owned());
        }
        let context = self
            .distributed
            .contexts()
            .resolve(WorkspaceContextId::from_bytes(route.context_id))
            .await
            .map_err(display)?;
        let workspace_id = context
            .roots
            .get(&root_id)
            .ok_or_else(|| "workspace route root is absent from its core context".to_owned())?
            .workspace_id;
        self.distributed
            .workspace(workspace_id)
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
        verify_native_root_identity(&source, binding.native_root_identity, &binding.path)?;
        self.distributed
            .open_lazy(workspace, source)
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
        let source_advanced_elsewhere = binding.source_epoch != observation.prior_source.epoch;
        // On a file system that may drop a change without a trace (`ReFS`),
        // no batch proves the root unchanged: every path the root workspace
        // records is checked against the disk at each refresh. What it does
        // not record, forks read from the disk itself.
        let unreported = !physical
            .watcher
            .lock()
            .map_err(|_| "physical root watcher state is poisoned".to_owned())?
            .reports_every_change();
        if !observation.consumed_changes && !source_advanced_elsewhere && !unreported {
            return Ok(());
        }
        #[cfg(test)]
        if (observation.consumed_changes || source_advanced_elsewhere) && self.fail_after_watch_poll
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
        let mut checkout = workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await
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
                capture_baseline_with_policy(
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
                capture_watch_batch_with_policy(
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
                capture_baseline_with_policy(
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
                capture_watch_batch_with_policy(
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
            capture_watch_batch_with_policy(
                &mut checkout,
                WatchBatch::Changes {
                    epoch: WatchEpoch::from_u64(0),
                    first_sequence: WatchSequence::from_u64(0),
                    next_sequence: WatchSequence::from_u64(0),
                    changes: recorded.into_iter().map(WatchChange::Modified).collect(),
                },
                &capture,
                &policy,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(display)?;
            if !observation.consumed_changes
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
            .rebind_source()
            .await
            .map_err(display)?;
        let binding = self
            .state
            .roots
            .get_mut(&key)
            .ok_or_else(|| "physical root binding is missing".to_owned())?;
        binding.source_epoch = observation.source.epoch;
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
            parent.head().await.map_err(display)?.id(),
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
        let operations = self.distributed.operations();
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

    pub(crate) fn persist(&mut self) -> Result<(), String> {
        // A save that survives a power loss survives it with every authority
        // write it may rest on.
        self.fs.flush_deferred_authority().map_err(display)?;
        self.slots
            .save(&self.data, &self.state, Survives::PowerLoss)?;
        // A flushed save is a whole snapshot, so it covers any unflushed one.
        self.unflushed = false;
        Ok(())
    }

    /// Saves the last transition of a request without flushing it; see
    /// [`Survives::ServiceCrash`].
    pub(crate) fn persist_unflushed(&mut self) -> Result<(), String> {
        self.slots
            .save(&self.data, &self.state, Survives::ServiceCrash)?;
        self.unflushed = true;
        Ok(())
    }

    /// Flushes an unflushed save and the authority writes of the requests
    /// before it. Every request starts here, so nothing ever acts on a
    /// transition that a power loss could still undo.
    pub(crate) async fn make_durable(&mut self) -> Result<(), String> {
        let fs = self.fs.clone();
        tokio::task::spawn_blocking(move || fs.flush_deferred_authority())
            .await
            .map_err(display)?
            .map_err(display)?;
        if !self.unflushed {
            return Ok(());
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_flush) {
            return Err("injected adapter-state flush failure".to_owned());
        }
        let data = self.data.clone();
        let mut slots = self.slots;
        self.slots = tokio::task::spawn_blocking(move || {
            slots.flush_unflushed(&data)?;
            Ok::<_, String>(slots)
        })
        .await
        .map_err(display)??;
        self.unflushed = false;
        Ok(())
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
            let operations = self.distributed.operations();
            let now = now_millis();
            let active = self.state.leases.values().cloned().collect::<Vec<_>>();
            for record in &active {
                let route = self
                    .state
                    .routes
                    .get(&record.agent_id)
                    .cloned()
                    .ok_or_else(|| "active lease route is missing during shutdown".to_owned())?;
                let parent_context = self
                    .distributed
                    .contexts()
                    .resolve(self.context_for_agent(&route.parent_agent_id)?)
                    .await
                    .map_err(display)?;
                for root_record in record.roots.values() {
                    let root_id = WorkspaceRootId::from_bytes(root_record.root_id);
                    let workspace = self.workspace_root(&route, root_id).await?;
                    let parent = self
                        .distributed
                        .workspace(
                            parent_context
                                .roots
                                .get(&root_id)
                                .ok_or_else(|| "active lease parent root is missing".to_owned())?
                                .workspace_id,
                        )
                        .await
                        .map_err(display)?;
                    operations
                        .observe_parent(workspace.id(), parent.head().await.map_err(display)?.id())
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

/// Every path the workspace's head records, parents before children.
async fn recorded_paths(workspace: &LocalWorkspace) -> Result<Vec<NamespacePath>, String> {
    let head = workspace.head().await.map_err(display)?;
    let mut recorded = Vec::new();
    let mut directories = vec!["/".to_owned()];
    while let Some(directory) = directories.pop() {
        let mut after = None;
        loop {
            let page = head
                .list_directory(&directory, after.as_ref(), 1_024)
                .await
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
        .diff_to(head, u32::MAX)
        .await
        .map_err(display)?
        .changed_paths(u32::MAX)
        .await
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
    async fn source_deletions(
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
            let child_head = self
                .distributed
                .workspace(root.source_workspace_id)
                .await
                .map_err(display)?
                .generation(root.source_generation)
                .await
                .map_err(display)?;
            let parent_view = match &parent_route {
                Some(parent_route) => Some(self.lazy_workspace_root(parent_route, *root_id).await?),
                None => None,
            };
            let changed = self.changed_since_fork(plan, *root_id, root).await?;
            let mut paths = Vec::new();
            for (path, deleted) in tombstones {
                let beneath = format!("{path}/");
                if child_head.stat(&path).await.is_ok()
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
    async fn changed_since_fork(
        &self,
        plan: &MultiRootMergePlan,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
    ) -> Result<BTreeSet<String>, String> {
        let mut changed = BTreeSet::new();
        let target = self
            .distributed
            .workspace(root.target_workspace_id)
            .await
            .map_err(display)?;
        changed_paths_into(
            &target
                .generation(root.base_generation)
                .await
                .map_err(display)?,
            &target
                .generation(root.target_generation)
                .await
                .map_err(display)?,
            &mut changed,
        )
        .await?;
        let registry = self.distributed.contexts();
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
            let upper_source = self
                .distributed
                .workspace(upper_child.workspace_id)
                .await
                .map_err(display)?;
            let upper_target = self
                .distributed
                .workspace(upper_parent.workspace_id)
                .await
                .map_err(display)?;
            let fork_point = upper_source
                .join_into(&upper_target)
                .plan()
                .await
                .map_err(display)?
                .common_ancestor();
            changed_paths_into(
                &upper_target.generation(fork_point).await.map_err(display)?,
                &upper_target.head().await.map_err(display)?,
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
    async fn apply_source_deletions(
        &mut self,
        parent: &str,
        deletions: BTreeMap<WorkspaceRootId, Vec<String>>,
    ) -> Result<(), String> {
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
