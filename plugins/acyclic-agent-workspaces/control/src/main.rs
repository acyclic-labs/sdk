use acyclic_fs::model::VolumeLimits;
use acyclic_fs::path::PortablePath;
use acyclic_fs::{
    ApplyOptions, CancellationToken, GitCommand, GitCompatRepository, GitFilesystemAction,
    GitFilesystemExecutor, GitFilesystemResult, GitIgnorePolicy, IdempotencyKey, JoinOutcome,
    JournaledMaterializer, LocalAuthorityBackend, LocalCoreStateStore, LocalFs, LocalObjectBackend,
    LocalOptions, MaterializationJournalStore, MaterializationRecovery, MaterializeOptions,
    MergeConflict, Mount, MountOptions, NativeTreeMaterializationBackend, OperationId,
    OperationReconcileLimits, OperationWindowCoordinator, OperationWindowLease, ReconcileOutcome,
    SourceOptions, TransactionCommit, WorkBudget, Workspace, WorkspaceGraph,
    WorkspaceOperationFinish, WorkspacePathApply, WorkspaceRestore, apply_git_patch,
    blame_git_generations, capture_git_compatible_generation, grep_git_generation, walk_git_tree,
};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

type LocalWorkspace = Workspace<LocalAuthorityBackend, LocalObjectBackend>;
type LocalMount = Mount<LocalAuthorityBackend, LocalObjectBackend>;

#[derive(Debug)]
struct PluginGitError(String);

impl std::fmt::Display for PluginGitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for PluginGitError {}

impl From<String> for PluginGitError {
    fn from(message: String) -> Self {
        Self(message)
    }
}

struct PluginGitExecutor<'a> {
    fs: &'a LocalFs,
    store: LocalCoreStateStore,
    current: LocalWorkspace,
    repository_id: acyclic_fs::WorkspaceId,
    ignore: GitIgnorePolicy,
    switched: Mutex<Option<LocalWorkspace>>,
}

impl PluginGitExecutor<'_> {
    fn error(message: impl Into<String>) -> PluginGitError {
        PluginGitError(message.into())
    }

    fn switched_workspace(&self) -> Result<Option<LocalWorkspace>, PluginGitError> {
        self.switched
            .lock()
            .map_err(|_| Self::error("Git workspace switch lock is unavailable"))
            .map(|workspace| workspace.clone())
    }

    async fn workspace(
        &self,
        workspace_id: acyclic_fs::WorkspaceId,
    ) -> Result<LocalWorkspace, PluginGitError> {
        if workspace_id == self.current.id() {
            return Ok(self.current.clone());
        }
        let record = WorkspaceGraph::new(self.store.clone())
            .resolve(workspace_id)
            .await
            .map_err(display)?;
        self.fs
            .open_workspace(&record.workspace_name)
            .await
            .map_err(display)
            .map_err(Into::into)
    }
}

impl GitFilesystemExecutor for PluginGitExecutor<'_> {
    type Error = PluginGitError;

    async fn execute(
        &self,
        operation_id: OperationId,
        action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult, Self::Error> {
        match action {
            GitFilesystemAction::CaptureCommit {
                workspace_generation,
                tracked_paths,
                ..
            } => {
                if self.current.head().await.map_err(display)?.id() != *workspace_generation {
                    return Err(Self::error("Git commit workspace changed before capture"));
                }
                let captured = capture_git_compatible_generation(
                    &self.current,
                    &self.ignore,
                    tracked_paths,
                    operation_id,
                )
                .await
                .map_err(display)?;
                Ok(GitFilesystemResult::Captured {
                    generation: captured.generation.id(),
                    workspace_id: captured.generation.workspace_id(),
                    tracked_paths: captured.tracked_paths,
                })
            }
            GitFilesystemAction::ForkBranch {
                branch,
                source_workspace,
                source_generation,
                switch,
                ..
            } => {
                if self.current.id() != *source_workspace
                    || self.current.head().await.map_err(display)?.id() != *source_generation
                {
                    return Err(Self::error("Git branch source workspace changed"));
                }
                let mut hasher = blake3::Hasher::new();
                hasher.update(&self.repository_id.into_bytes());
                hasher.update(branch.as_bytes());
                let destination = format!(
                    "git-branch-{}",
                    hex::encode(&hasher.finalize().as_bytes()[..12])
                );
                let workspace = WorkspaceGraph::new(self.store.clone())
                    .fork(
                        &self.current,
                        destination,
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                    )
                    .await
                    .map_err(display)?;
                if *switch {
                    *self
                        .switched
                        .lock()
                        .map_err(|_| Self::error("Git workspace switch lock is unavailable"))? =
                        Some(workspace.clone());
                }
                Ok(GitFilesystemResult::Forked {
                    workspace_id: workspace.id(),
                })
            }
            GitFilesystemAction::SwitchWorkspace { workspace_id } => {
                let record = WorkspaceGraph::new(self.store.clone())
                    .resolve(*workspace_id)
                    .await
                    .map_err(display)?;
                let workspace = self
                    .fs
                    .open_workspace(&record.workspace_name)
                    .await
                    .map_err(display)?;
                *self
                    .switched
                    .lock()
                    .map_err(|_| Self::error("Git workspace switch lock is unavailable"))? =
                    Some(workspace.clone());
                Ok(GitFilesystemResult::Applied {
                    generation: Some(workspace.head().await.map_err(display)?.id()),
                })
            }
            GitFilesystemAction::Diff { from, to } => {
                let to_workspace = self.workspace(to.workspace_id).await?;
                let to = to_workspace
                    .generation(to.generation)
                    .await
                    .map_err(display)?;
                let value = if let Some(from) = from {
                    let from_workspace = self.workspace(from.workspace_id).await?;
                    let from = from_workspace
                        .generation(from.generation)
                        .await
                        .map_err(display)?;
                    let diff = from.diff_to(&to, 100_000).await.map_err(display)?;
                    json!({
                        "from": hex::encode(from.id().digest().as_bytes()),
                        "to": hex::encode(to.id().digest().as_bytes()),
                        "fileChanges": diff.changes().files.len(),
                        "bindingChanges": diff.changes().bindings.len()
                    })
                } else {
                    json!({
                        "from": null,
                        "to": hex::encode(to.id().digest().as_bytes()),
                        "unborn": true
                    })
                };
                Ok(GitFilesystemResult::Data {
                    kind: "diff".to_owned(),
                    value,
                })
            }
            GitFilesystemAction::RestoreGeneration {
                workspace_id,
                generation,
                paths,
            } => {
                let current = self.current.head().await.map_err(display)?;
                let source_workspace = self.workspace(*workspace_id).await?;
                let source = source_workspace
                    .generation(*generation)
                    .await
                    .map_err(display)?;
                let generation = if let Some(paths) = paths {
                    match self
                        .current
                        .restore_paths_from(
                            &source,
                            &paths.iter().cloned().collect::<Vec<_>>(),
                            current.id(),
                            IdempotencyKey::from_bytes(operation_id.into_bytes()),
                        )
                        .await
                        .map_err(display)?
                    {
                        TransactionCommit::Committed(generation)
                        | TransactionCommit::AlreadyCommitted(generation) => generation,
                        TransactionCommit::Conflict { .. } => {
                            return Err(Self::error(
                                "Git restore raced with another workspace writer",
                            ));
                        }
                        TransactionCommit::Fenced => {
                            return Err(Self::error("Git restore was fenced"));
                        }
                        TransactionCommit::IdempotencyConflict => {
                            return Err(Self::error("Git restore retry identity was reused"));
                        }
                    }
                } else {
                    if *workspace_id != self.current.id() {
                        return Err(Self::error(
                            "an exact workspace restore cannot use a foreign generation",
                        ));
                    }
                    match self
                        .current
                        .restore_generation(
                            &source,
                            current.id(),
                            IdempotencyKey::from_bytes(operation_id.into_bytes()),
                        )
                        .await
                        .map_err(display)?
                    {
                        WorkspaceRestore::Restored(generation)
                        | WorkspaceRestore::AlreadyRestored(generation)
                        | WorkspaceRestore::Current(generation) => generation,
                        WorkspaceRestore::Stale(_) => {
                            return Err(Self::error(
                                "Git restore raced with another workspace writer",
                            ));
                        }
                        WorkspaceRestore::Fenced => {
                            return Err(Self::error("Git restore was fenced"));
                        }
                        WorkspaceRestore::IdempotencyConflict => {
                            return Err(Self::error("Git restore retry identity was reused"));
                        }
                    }
                };
                Ok(GitFilesystemResult::Applied {
                    generation: Some(generation.id()),
                })
            }
            GitFilesystemAction::RestorePaths {
                workspace_id,
                generation,
                paths,
            } => {
                let current = self.current.head().await.map_err(display)?;
                let source_workspace = self.workspace(*workspace_id).await?;
                let source = source_workspace
                    .generation(*generation)
                    .await
                    .map_err(display)?;
                let outcome = self
                    .current
                    .restore_paths_from(
                        &source,
                        paths,
                        current.id(),
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                    )
                    .await
                    .map_err(display)?;
                let generation = match outcome {
                    TransactionCommit::Committed(generation)
                    | TransactionCommit::AlreadyCommitted(generation) => generation,
                    TransactionCommit::Conflict { .. } => {
                        return Err(Self::error(
                            "Git path restore raced with another workspace writer",
                        ));
                    }
                    TransactionCommit::Fenced => {
                        return Err(Self::error("Git path restore was fenced"));
                    }
                    TransactionCommit::IdempotencyConflict => {
                        return Err(Self::error("Git path restore retry identity was reused"));
                    }
                };
                Ok(GitFilesystemResult::Applied {
                    generation: Some(generation.id()),
                })
            }
            GitFilesystemAction::Join {
                source_workspace,
                rebase,
            } => {
                let source = WorkspaceGraph::new(self.store.clone())
                    .resolve(*source_workspace)
                    .await
                    .map_err(display)?;
                let source = self
                    .fs
                    .open_workspace(&source.workspace_name)
                    .await
                    .map_err(display)?;
                let mut builder = source.join_into(&self.current);
                if *rebase {
                    builder = builder.history(acyclic_fs::JoinHistory::Rebase);
                }
                let plan = builder.plan().await.map_err(display)?;
                let outcome = plan
                    .apply(ApplyOptions {
                        if_target: plan.target_head(),
                        idempotency_key: IdempotencyKey::from_bytes(operation_id.into_bytes()),
                    })
                    .await
                    .map_err(display)?;
                let generation = match outcome {
                    JoinOutcome::Applied(generation)
                    | JoinOutcome::AlreadyApplied(generation)
                    | JoinOutcome::NoChanges(generation) => generation,
                    JoinOutcome::StaleTarget(_) => {
                        return Err(Self::error("Git join raced with another workspace writer"));
                    }
                    JoinOutcome::Conflicted {
                        conflicts,
                        truncated,
                    } => {
                        return Err(Self::error(format!(
                            "Git join has {} conflict(s); truncated={truncated}",
                            conflicts.len()
                        )));
                    }
                    JoinOutcome::Fenced => return Err(Self::error("Git join was fenced")),
                    JoinOutcome::IdempotencyConflict => {
                        return Err(Self::error("Git join retry identity was reused"));
                    }
                };
                Ok(GitFilesystemResult::Applied {
                    generation: Some(generation.id()),
                })
            }
            GitFilesystemAction::ApplyCommit {
                base,
                source,
                paths,
                ..
            } => {
                let base_workspace = match base {
                    Some(base) => Some(self.workspace(base.workspace_id).await?),
                    None => None,
                };
                let base = match (base, base_workspace.as_ref()) {
                    (Some(base), Some(workspace)) => Some(
                        workspace
                            .generation(base.generation)
                            .await
                            .map_err(display)?,
                    ),
                    _ => None,
                };
                let source_workspace = match source {
                    Some(source) => Some(self.workspace(source.workspace_id).await?),
                    None => None,
                };
                let source = match (source, source_workspace.as_ref()) {
                    (Some(source), Some(workspace)) => Some(
                        workspace
                            .generation(source.generation)
                            .await
                            .map_err(display)?,
                    ),
                    _ => None,
                };
                let current = self.current.head().await.map_err(display)?;
                let outcome = self
                    .current
                    .apply_paths_from(
                        base.as_ref(),
                        source.as_ref(),
                        &paths.iter().cloned().collect::<Vec<_>>(),
                        current.id(),
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                    )
                    .await
                    .map_err(display)?;
                let generation = match outcome {
                    WorkspacePathApply::Applied(generation)
                    | WorkspacePathApply::AlreadyApplied(generation)
                    | WorkspacePathApply::NoChanges(generation) => generation,
                    WorkspacePathApply::Conflicted(conflicts) => {
                        return Err(Self::error(
                            serde_json::to_string(&json!({
                                "kind": "conflicts",
                                "conflicts": conflicts
                                    .iter()
                                    .map(|conflict| json!({
                                        "path": conflict.path,
                                        "kind": format!("{:?}", conflict.kind),
                                    }))
                                    .collect::<Vec<_>>()
                            }))
                            .map_err(display)?,
                        ));
                    }
                    WorkspacePathApply::Stale(_) => {
                        return Err(Self::error(
                            "Git commit application raced with another workspace writer",
                        ));
                    }
                    WorkspacePathApply::Fenced => {
                        return Err(Self::error("Git commit application was fenced"));
                    }
                    WorkspacePathApply::IdempotencyConflict => {
                        return Err(Self::error(
                            "Git commit application retry identity was reused",
                        ));
                    }
                };
                Ok(GitFilesystemResult::Applied {
                    generation: Some(generation.id()),
                })
            }
            GitFilesystemAction::Archive {
                workspace_id,
                generation,
            } => {
                let workspace = self.workspace(*workspace_id).await?;
                let generation = workspace.generation(*generation).await.map_err(display)?;
                let entries = walk_git_tree(&generation, None, 100_000)
                    .await
                    .map_err(display)?;
                Ok(GitFilesystemResult::Data {
                    kind: "archive".to_owned(),
                    value: json!({
                        "format": "acyclic-fs-manifest-v1",
                        "generation": hex::encode(generation.id().digest().as_bytes()),
                        "entries": entries
                    }),
                })
            }
            GitFilesystemAction::Grep {
                pattern,
                path,
                generation,
            } => {
                let workspace = self.workspace(generation.workspace_id).await?;
                let generation = workspace
                    .generation(generation.generation)
                    .await
                    .map_err(display)?;
                let result = grep_git_generation(
                    &generation,
                    pattern,
                    path.as_deref(),
                    100_000,
                    16 * 1024 * 1024,
                    100_000,
                )
                .await
                .map_err(display)?;
                Ok(GitFilesystemResult::Data {
                    kind: "grep".to_owned(),
                    value: serde_json::to_value(result).map_err(display)?,
                })
            }
            GitFilesystemAction::Blame { path, commits } => {
                let mut history = Vec::with_capacity(commits.len());
                for commit in commits {
                    let workspace = self
                        .workspace(commit.generation_workspace_id.unwrap_or(self.current.id()))
                        .await?;
                    let generation = workspace
                        .generation(commit.generation)
                        .await
                        .map_err(display)?;
                    history.push((commit.clone(), generation));
                }
                let lines = blame_git_generations(&history, path, 16 * 1024 * 1024)
                    .await
                    .map_err(display)?;
                Ok(GitFilesystemResult::Data {
                    kind: "blame".to_owned(),
                    value: serde_json::to_value(lines).map_err(display)?,
                })
            }
            GitFilesystemAction::Clean {
                dry_run,
                generation,
                tracked_paths,
            } => {
                if generation.workspace_id != self.current.id() {
                    return Err(Self::error(
                        "Git clean generation is not the live workspace",
                    ));
                }
                let generation = self
                    .current
                    .generation(generation.generation)
                    .await
                    .map_err(display)?;
                let entries = walk_git_tree(&generation, None, 100_000)
                    .await
                    .map_err(display)?;
                let candidates: Vec<_> = entries
                    .into_iter()
                    .filter(|entry| {
                        entry.kind != "directory"
                            && !tracked_paths.contains(&entry.path)
                            && self.ignore.eligible(&entry.path, false, false)
                    })
                    .map(|entry| entry.path)
                    .collect();
                if *dry_run || candidates.is_empty() {
                    return Ok(GitFilesystemResult::Data {
                        kind: "clean".to_owned(),
                        value: json!({ "paths": candidates, "dryRun": dry_run }),
                    });
                }
                let mut transaction = self
                    .current
                    .begin_transaction(IdempotencyKey::from_bytes(operation_id.into_bytes()))
                    .await
                    .map_err(display)?;
                for path in &candidates {
                    transaction
                        .remove(&format!("/{path}"))
                        .await
                        .map_err(display)?;
                }
                let generation = match transaction.commit().await.map_err(display)? {
                    TransactionCommit::Committed(generation)
                    | TransactionCommit::AlreadyCommitted(generation) => generation,
                    TransactionCommit::Conflict { .. } => {
                        return Err(Self::error("Git clean raced with another workspace writer"));
                    }
                    TransactionCommit::Fenced => return Err(Self::error("Git clean was fenced")),
                    TransactionCommit::IdempotencyConflict => {
                        return Err(Self::error("Git clean retry identity was reused"));
                    }
                };
                Ok(GitFilesystemResult::Applied {
                    generation: Some(generation.id()),
                })
            }
            GitFilesystemAction::ApplyPatch { patch } => {
                let generation = match apply_git_patch(
                    &self.current,
                    patch,
                    IdempotencyKey::from_bytes(operation_id.into_bytes()),
                )
                .await
                .map_err(display)?
                {
                    TransactionCommit::Committed(generation)
                    | TransactionCommit::AlreadyCommitted(generation) => generation,
                    TransactionCommit::Conflict { .. } => {
                        return Err(Self::error(
                            "Git patch application raced with another workspace writer",
                        ));
                    }
                    TransactionCommit::Fenced => {
                        return Err(Self::error("Git patch application was fenced"));
                    }
                    TransactionCommit::IdempotencyConflict => {
                        return Err(Self::error(
                            "Git patch application retry identity was reused",
                        ));
                    }
                };
                Ok(GitFilesystemResult::Applied {
                    generation: Some(generation.id()),
                })
            }
            unsupported => Err(Self::error(format!(
                "Git filesystem action is not yet available in the Codex adapter: {unsupported:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Route {
    agent_id: String,
    turn_id: String,
    workspace_name: String,
    workspace_id: [u8; 16],
    #[serde(default)]
    repository_workspace_id: [u8; 16],
    parent_agent_id: String,
    path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingSpawn {
    parent_agent_id: String,
    #[serde(default)]
    expires_at_millis: u64,
    #[serde(default)]
    workspace_name: String,
    #[serde(default)]
    fork_key: [u8; 16],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LeaseRecord {
    agent_id: String,
    workspace_id: [u8; 16],
    lease_id: [u8; 16],
    pinned_parent: [u8; 32],
    expires_at_millis: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RootPublication {
    #[serde(default)]
    phase: RootPublicationPhase,
    operation_id: [u8; 16],
    from: [u8; 32],
    to: [u8; 32],
    operation_directory: PathBuf,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
enum RootPublicationPhase {
    #[default]
    JoinPending,
    JoinApplied,
}

impl LeaseRecord {
    fn from_lease(agent_id: String, lease: &OperationWindowLease) -> Self {
        Self {
            agent_id,
            workspace_id: lease.workspace_id.into_bytes(),
            lease_id: lease.lease_id.into_bytes(),
            pinned_parent: *lease.pinned_parent.digest().as_bytes(),
            expires_at_millis: lease.expires_at_millis,
        }
    }

    fn lease(&self) -> OperationWindowLease {
        OperationWindowLease {
            workspace_id: acyclic_fs::WorkspaceId::from_bytes(self.workspace_id),
            lease_id: acyclic_fs::OperationLeaseId::from_bytes(self.lease_id),
            pinned_parent: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(
                self.pinned_parent,
            )),
            expires_at_millis: self.expires_at_millis,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct AdapterState {
    version: u32,
    root_session_id: String,
    root_agent_id: String,
    #[serde(default)]
    root_turns: BTreeSet<String>,
    root_path: PathBuf,
    root_workspace_name: String,
    routes: BTreeMap<String, Route>,
    turns: BTreeMap<String, String>,
    pending: VecDeque<PendingSpawn>,
    leases: BTreeMap<String, LeaseRecord>,
    #[serde(default)]
    root_publication: Option<RootPublication>,
}

struct ControlPlane {
    data: PathBuf,
    fs: LocalFs,
    store: LocalCoreStateStore,
    state: AdapterState,
    root: Option<LocalWorkspace>,
    mounts: BTreeMap<String, LocalMount>,
}

impl ControlPlane {
    async fn open(data: PathBuf) -> Result<Self, String> {
        fs::create_dir_all(&data).map_err(display)?;
        let fs = LocalFs::local(LocalOptions::new(data.join("filesystem")))
            .await
            .map_err(display)?;
        let store = LocalCoreStateStore::new(data.join("core-state"));
        let state = load_state(&data)?;
        let mut control = Self {
            data,
            fs,
            store,
            state,
            root: None,
            mounts: BTreeMap::new(),
        };
        control.restore_root().await?;
        control.recover_root_publication().await?;
        control.restore_mounts().await?;
        Ok(control)
    }

    async fn restore_root(&mut self) -> Result<(), String> {
        if self.state.root_workspace_name.is_empty() || self.state.root_path.as_os_str().is_empty()
        {
            return Ok(());
        }
        self.root = Some(if self.state.root_publication.is_some() {
            self.fs
                .open_workspace(&self.state.root_workspace_name)
                .await
                .map_err(display)?
        } else {
            self.fs
                .attach_directory(
                    &self.state.root_workspace_name,
                    &self.state.root_path,
                    root_source_options()?,
                )
                .await
                .map_err(display)?
        });
        Ok(())
    }

    async fn restore_mounts(&mut self) -> Result<(), String> {
        for route in self.state.routes.values() {
            if route.path.exists() {
                let workspace = self
                    .fs
                    .open_workspace(&route.workspace_name)
                    .await
                    .map_err(display)?;
                let mount = workspace
                    .mount(&route.path, MountOptions::read_write())
                    .await
                    .map_err(display)?;
                self.mounts.insert(route.agent_id.clone(), mount);
            }
        }
        Ok(())
    }

    async fn call(&mut self, name: &str, arguments: Value) -> Result<Value, String> {
        match name {
            "agent_changes" => self.agent_changes(arguments).await,
            "agent_merge" => self.agent_merge(arguments).await,
            "agent_discard" => self.agent_discard(arguments).await,
            "_hook_session_start" => self.session_start(arguments).await,
            "_hook_user_prompt" => self.user_prompt(arguments),
            "_hook_pre_tool" => self.pre_tool(arguments).await,
            "_hook_subagent_start" => self.subagent_start(arguments).await,
            "_hook_post_tool" => self.post_tool(arguments).await,
            "_hook_subagent_stop" => self.subagent_stop(arguments).await,
            _ => Err(format!("unknown control-plane method '{name}'")),
        }
    }

    async fn session_start(&mut self, input: Value) -> Result<Value, String> {
        let session_id = string(&input, "session_id")?;
        let root_path = PathBuf::from(string(&input, "cwd")?);
        let canonical = root_path.canonicalize().map_err(display)?;
        if !self.state.root_session_id.is_empty()
            && (self.state.root_session_id != session_id || self.state.root_path != canonical)
        {
            return Err("plugin data is already bound to another root session".to_owned());
        }
        let workspace_name = format!(
            "codex-root-{}",
            short_hash(canonical.as_os_str().to_string_lossy().as_bytes())
        );
        let workspace = self
            .fs
            .attach_directory(&workspace_name, &canonical, root_source_options()?)
            .await
            .map_err(display)?;
        WorkspaceGraph::new(self.store.clone())
            .register_root(&workspace)
            .await
            .map_err(display)?;
        self.state.version = 1;
        self.state.root_session_id = session_id.clone();
        self.state.root_agent_id = format!("root:{session_id}");
        self.state.root_path = canonical;
        self.state.root_workspace_name = workspace_name;
        self.root = Some(workspace);
        self.persist()?;
        Ok(json!({"suppressOutput": true}))
    }

    fn user_prompt(&mut self, input: Value) -> Result<Value, String> {
        let session_id = string(&input, "session_id")?;
        if session_id != self.state.root_session_id {
            return Err("user prompt belongs to another root session".to_owned());
        }
        self.state.root_turns.insert(string(&input, "turn_id")?);
        self.persist()?;
        Ok(json!({"suppressOutput": true}))
    }

    async fn pre_tool(&mut self, input: Value) -> Result<Value, String> {
        let tool_name = string(&input, "tool_name")?;
        let turn_id = string(&input, "turn_id")?;
        let tool_use_id = string(&input, "tool_use_id")?;
        let caller = self.resolve_turn(&turn_id)?;
        if tool_name == "spawn_agent" || tool_name == "Agent" {
            let now = now_millis();
            self.state
                .pending
                .retain(|spawn| spawn.expires_at_millis > now);
            if !self.state.pending.is_empty() {
                return Err(
                    "a subagent spawn handshake is already pending; retry after SubagentStart"
                        .to_owned(),
                );
            }
            let fork_key = IdempotencyKey::new();
            self.state.pending.push_back(PendingSpawn {
                parent_agent_id: caller,
                expires_at_millis: now.saturating_add(2 * 60 * 1_000),
                workspace_name: format!("codex-agent-{}", hex::encode(fork_key.into_bytes())),
                fork_key: fork_key.into_bytes(),
            });
            self.persist()?;
            return Ok(json!({}));
        }
        if caller == self.state.root_agent_id {
            if tool_name.starts_with("mcp__acyclic_agent_workspaces__") {
                let mut updated =
                    normalize_tool_input(input.get("tool_input").cloned().unwrap_or(Value::Null))?;
                updated
                    .as_object_mut()
                    .ok_or_else(|| "workspace control tool input must be an object".to_owned())?
                    .insert("_caller_turn_id".to_owned(), Value::String(turn_id));
                self.persist()?;
                return Ok(json!({
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "allow",
                        "updatedInput": updated
                    }
                }));
            }
            self.persist()?;
            return Ok(json!({}));
        }
        let agent_id = caller;
        if is_pure_remote_tool(&tool_name) {
            return Ok(json!({}));
        }
        if !is_filesystem_tool(&tool_name) {
            return Err(format!(
                "unclassified tool '{tool_name}' is denied inside an isolated subagent workspace"
            ));
        }
        let route = self
            .state
            .routes
            .get(&agent_id)
            .cloned()
            .ok_or_else(|| "subagent route is missing".to_owned())?;
        let original =
            normalize_tool_input(input.get("tool_input").cloned().unwrap_or(Value::Null))?;
        let mut updated =
            rewrite_tool_input(&tool_name, original, &self.state.root_path, &route.path)?;
        if (matches!(tool_name.as_str(), "Bash" | "exec_command")
            || tool_name.ends_with("__exec_command"))
            && let Some(argv) = git_argv(&updated)?
        {
            return self.git_tool(&agent_id, &route, updated, argv).await;
        }
        require_process_sandbox(&tool_name)?;
        if tool_name.starts_with("mcp__acyclic_agent_workspaces__") {
            let object = updated
                .as_object_mut()
                .ok_or_else(|| "workspace control tool input must be an object".to_owned())?;
            object.insert("_caller_turn_id".to_owned(), Value::String(turn_id));
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
        let workspace = self.workspace(&route).await?;
        let parent = self.parent_workspace(&route).await?;
        let coordinator = OperationWindowCoordinator::new(self.store.clone());
        coordinator
            .recover_workspace(
                &workspace,
                now_millis(),
                OperationReconcileLimits::default(),
            )
            .await
            .map_err(display)?;
        let parent_generation = parent.head().await.map_err(display)?.id();
        let lease = coordinator
            .begin(
                workspace.id(),
                parent_generation,
                format!("{agent_id}:{tool_use_id}"),
                now,
                now.saturating_add(15 * 60 * 1_000),
            )
            .await
            .map_err(display)?;
        self.state
            .leases
            .insert(tool_use_id, LeaseRecord::from_lease(agent_id, &lease));
        self.persist()?;
        Ok(pre_tool_update(updated))
    }

    async fn git_tool(
        &mut self,
        agent_id: &str,
        route: &Route,
        mut updated: Value,
        argv: Vec<String>,
    ) -> Result<Value, String> {
        let workspace = self.workspace(route).await?;
        self.sync_agent(&route.parent_agent_id).await?;
        let parent = self.parent_workspace(route).await?;
        let coordinator = OperationWindowCoordinator::new(self.store.clone());
        coordinator
            .recover_workspace(
                &workspace,
                now_millis(),
                OperationReconcileLimits::default(),
            )
            .await
            .map_err(display)?;
        if !matches!(
            coordinator
                .snapshot(workspace.id())
                .await
                .map_err(display)?
                .phase,
            acyclic_fs::OperationWindowPhase::Idle
        ) {
            return Err("Git compatibility commands require an idle agent workspace".to_owned());
        }
        self.mounts
            .get(agent_id)
            .ok_or_else(|| "subagent mount is unavailable".to_owned())?
            .sync()
            .await
            .map_err(display)?;
        let head = workspace.head().await.map_err(display)?;
        let ignore_text = match head.read("/.gitignore", 1024 * 1024).await {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(acyclic_fs::WorkspaceError::NotFound) => String::new(),
            Err(error) => return Err(display(error)),
        };
        let ignore = GitIgnorePolicy::parse(&format!("{ignore_text}\n.git/\n.acyclic-sdk/\n"));
        let repository_id = if route.repository_workspace_id == [0; 16] {
            workspace.id()
        } else {
            acyclic_fs::WorkspaceId::from_bytes(route.repository_workspace_id)
        };
        let patch = if argv.first().is_some_and(|command| command == "apply") {
            let patch_path = git_apply_patch_path(&argv)?;
            validate_path(patch_path, &route.path)?;
            let patch_path = if Path::new(patch_path).is_absolute() {
                PathBuf::from(patch_path)
            } else {
                route.path.join(patch_path)
            };
            let patch = fs::read(&patch_path).map_err(display)?;
            if patch.len() > 64 * 1024 * 1024 {
                return Err("git apply patch exceeds the 64 MiB compatibility bound".to_owned());
            }
            Some(patch)
        } else {
            None
        };
        let now = now_millis();
        let lease = coordinator
            .begin(
                workspace.id(),
                parent.head().await.map_err(display)?.id(),
                format!("{agent_id}:git"),
                now,
                now.saturating_add(15 * 60 * 1_000),
            )
            .await
            .map_err(display)?;
        let executor = PluginGitExecutor {
            fs: &self.fs,
            store: self.store.clone(),
            current: workspace.clone(),
            repository_id,
            ignore,
            switched: Mutex::new(None),
        };
        let repository = GitCompatRepository::new(repository_id, self.store.clone());
        let resumed = repository.resume(&executor).await;
        let command = if let Ok(Some(_)) = resumed {
            Ok(None)
        } else if let Err(error) = resumed {
            Err(error)
        } else if let Some(patch) = patch {
            repository
                .run(GitCommand::Apply { patch }, head.id(), &executor)
                .await
                .map(Some)
        } else {
            repository
                .run_argv(
                    &argv,
                    head.id(),
                    agent_id,
                    i64::try_from(now / 1_000).unwrap_or(i64::MAX),
                    &executor,
                )
                .await
                .map(Some)
        };
        let parent_head = parent.head().await.map(|generation| generation.id());
        let observed: Result<(), String> = match parent_head {
            Ok(parent_head) => coordinator
                .observe_parent(workspace.id(), parent_head)
                .await
                .map(|_| ())
                .map_err(display),
            Err(error) => Err(display(error)),
        };
        let finish = coordinator
            .finish(&lease, now_millis())
            .await
            .map_err(display)?;
        match finish {
            acyclic_fs::OperationWindowFinish::Reconcile(reconcile) => {
                match coordinator
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
            let previous = self
                .mounts
                .remove(agent_id)
                .ok_or_else(|| "subagent mount is unavailable".to_owned())?;
            previous.unmount().await.map_err(display)?;
            let mount = switched
                .mount(&route.path, MountOptions::read_write())
                .await
                .map_err(display)?;
            let current = self
                .state
                .routes
                .get_mut(agent_id)
                .ok_or_else(|| "subagent route is missing".to_owned())?;
            current.workspace_name = switched.name().as_str().to_owned();
            current.workspace_id = switched.id().into_bytes();
            self.mounts.insert(agent_id.to_owned(), mount);
        }
        let Some(output) = command.map_err(display)? else {
            self.persist()?;
            return Err(
                "recovered a pending Git transition; retry the current command".to_owned(),
            );
        };
        set_shell_command(&mut updated, render_git_output(&output)?)?;
        self.persist()?;
        Ok(pre_tool_update(updated))
    }

    async fn subagent_start(&mut self, input: Value) -> Result<Value, String> {
        let agent_id = string(&input, "agent_id")?;
        let turn_id = string(&input, "turn_id")?;
        if self.state.routes.contains_key(&agent_id) {
            self.state.turns.insert(turn_id, agent_id.clone());
            self.persist()?;
            let route = self.state.routes.get(&agent_id).ok_or("route missing")?;
            return Ok(subagent_context(&route.path));
        }
        let pending = self
            .state
            .pending
            .pop_front()
            .ok_or_else(|| "subagent start has no serialized spawn handshake".to_owned())?;
        if pending.expires_at_millis <= now_millis() {
            self.persist()?;
            return Err("subagent spawn handshake expired before SubagentStart".to_owned());
        }
        self.sync_agent(&pending.parent_agent_id).await?;
        let parent = self.workspace_for_agent(&pending.parent_agent_id).await?;
        let workspace_name = if pending.workspace_name.is_empty() {
            format!("codex-agent-{}", short_hash(agent_id.as_bytes()))
        } else {
            pending.workspace_name
        };
        let fork_key = if pending.fork_key == [0; 16] {
            IdempotencyKey::new()
        } else {
            IdempotencyKey::from_bytes(pending.fork_key)
        };
        let workspace = WorkspaceGraph::new(self.store.clone())
            .fork(&parent, &workspace_name, fork_key)
            .await
            .map_err(display)?;
        let path = self
            .data
            .join("workspaces")
            .join(short_hash(agent_id.as_bytes()));
        fs::create_dir_all(&path).map_err(display)?;
        if path.read_dir().map_err(display)?.next().is_some() {
            return Err("child workspace destination is not empty".to_owned());
        }
        let mount = workspace
            .mount(&path, MountOptions::read_write())
            .await
            .map_err(display)?;
        let route = Route {
            agent_id: agent_id.clone(),
            turn_id: turn_id.clone(),
            workspace_name,
            workspace_id: workspace.id().into_bytes(),
            repository_workspace_id: workspace.id().into_bytes(),
            parent_agent_id: pending.parent_agent_id,
            path: path.clone(),
        };
        self.mounts.insert(agent_id.clone(), mount);
        self.state.turns.insert(turn_id, agent_id.clone());
        self.state.routes.insert(agent_id, route);
        self.persist()?;
        Ok(subagent_context(&path))
    }

    async fn post_tool(&mut self, input: Value) -> Result<Value, String> {
        let tool_use_id = string(&input, "tool_use_id")?;
        let Some(record) = self.state.leases.remove(&tool_use_id) else {
            return Ok(json!({}));
        };
        let route = self
            .state
            .routes
            .get(&record.agent_id)
            .cloned()
            .ok_or_else(|| "post-tool route is missing".to_owned())?;
        let workspace = self.workspace(&route).await?;
        self.sync_agent(&route.parent_agent_id).await?;
        let parent = self.parent_workspace(&route).await?;
        let coordinator = OperationWindowCoordinator::new(self.store.clone());
        coordinator
            .observe_parent(workspace.id(), parent.head().await.map_err(display)?.id())
            .await
            .map_err(display)?;
        let finish = coordinator
            .finish(&record.lease(), now_millis())
            .await
            .map_err(display)?;
        if matches!(finish, acyclic_fs::OperationWindowFinish::AlreadyClosed) {
            self.persist()?;
            return Err("filesystem tool lease expired; late writes were fenced".to_owned());
        }
        self.mounts
            .get(&record.agent_id)
            .ok_or_else(|| "subagent mount is unavailable".to_owned())?
            .sync()
            .await
            .map_err(display)?;
        let outcome = match finish {
            acyclic_fs::OperationWindowFinish::StillActive { remaining } => {
                WorkspaceOperationFinish::StillActive { remaining }
            }
            acyclic_fs::OperationWindowFinish::Reconcile(reconcile) => {
                WorkspaceOperationFinish::Reconciled(
                    coordinator
                        .reconcile_workspace(
                            &workspace,
                            reconcile,
                            OperationReconcileLimits::default(),
                        )
                        .await
                        .map_err(display)?,
                )
            }
            acyclic_fs::OperationWindowFinish::AlreadyClosed => unreachable!(),
        };
        self.persist()?;
        match outcome {
            WorkspaceOperationFinish::Reconciled(acyclic_fs::WorkspaceRebase::Conflicted {
                conflicts,
                truncated,
            }) => Ok(json!({
                "systemMessage": format!(
                    "Acyclic workspace rebase reported {} typed conflict(s); truncated={truncated}",
                    conflicts.len()
                ),
                "conflicts": conflicts.iter().map(conflict_json).collect::<Vec<_>>(),
                "truncated": truncated
            })),
            _ => Ok(json!({})),
        }
    }

    async fn subagent_stop(&mut self, input: Value) -> Result<Value, String> {
        let agent_id = string(&input, "agent_id")?;
        if let Some(mount) = self.mounts.get(&agent_id) {
            mount.sync().await.map_err(display)?;
        }
        Ok(json!({"suppressOutput": true}))
    }

    async fn agent_changes(&mut self, input: Value) -> Result<Value, String> {
        let agent = string(&input, "agent")?;
        let caller = self.caller(&input)?;
        self.authorize_inspection(&caller, &agent)?;
        if let Some(mount) = self.mounts.get(&agent) {
            mount.sync().await.map_err(display)?;
        }
        let route = self
            .state
            .routes
            .get(&agent)
            .cloned()
            .ok_or("unknown agent")?;
        let workspace = self.workspace(&route).await?;
        let lineage = WorkspaceGraph::new(self.store.clone())
            .authorize_join(workspace.id(), self.parent_workspace(&route).await?.id())
            .await
            .map_err(display)?;
        let base = workspace
            .generation(lineage.initial_generation)
            .await
            .map_err(display)?;
        let head = workspace.head().await.map_err(display)?;
        let changes = workspace
            .diff(&base, &head, 100_000)
            .await
            .map_err(display)?;
        Ok(json!({
            "agent": agent,
            "workspace": hex::encode(workspace.id().into_bytes()),
            "generation": hex::encode(head.id().digest().as_bytes()),
            "fileChanges": changes.changes().files.len(),
            "bindingChanges": changes.changes().bindings.len(),
            "truncated": changes.changes().truncated,
            "path": input.get("path").cloned().unwrap_or(Value::Null)
        }))
    }

    async fn agent_merge(&mut self, input: Value) -> Result<Value, String> {
        let agent = string(&input, "agent")?;
        let caller = self.caller(&input)?;
        let route = self
            .state
            .routes
            .get(&agent)
            .cloned()
            .ok_or("unknown agent")?;
        if route.parent_agent_id != caller {
            return Err("only the direct parent may merge this workspace".to_owned());
        }
        if let Some(mount) = self.mounts.get(&agent) {
            mount
                .sync()
                .await
                .map_err(|error| format!("cannot synchronize child mount before merge: {error}"))?;
        }
        self.sync_agent(&caller)
            .await
            .map_err(|error| format!("cannot synchronize parent before merge: {error}"))?;
        let source = self.workspace(&route).await?;
        let mut lineage_cursor = source.clone();
        let target = self.workspace_for_agent(&caller).await?;
        let graph = WorkspaceGraph::new(self.store.clone());
        let repository_id = if route.repository_workspace_id == [0; 16] {
            source.id()
        } else {
            acyclic_fs::WorkspaceId::from_bytes(route.repository_workspace_id)
        };
        let mut visited = BTreeSet::new();
        // Compatibility branches are private descendants of this agent's repository workspace,
        // not independently addressable agents. Validate every direct lineage edge, then publish
        // the agent's selected branch to its authorized parent with one filesystem CAS so a crash
        // cannot expose a partially advanced compatibility chain.
        while lineage_cursor.id() != repository_id {
            if !visited.insert(lineage_cursor.id()) || visited.len() > 64 {
                return Err("Git compatibility branch lineage is cyclic or too deep".to_owned());
            }
            let lineage = graph.resolve(lineage_cursor.id()).await.map_err(display)?;
            let parent_id = lineage
                .parent_workspace_id
                .ok_or_else(|| "Git compatibility branch does not reach its repository workspace".to_owned())?;
            let parent_name = lineage
                .parent_workspace_name
                .ok_or_else(|| "Git compatibility branch parent name is unavailable".to_owned())?;
            let parent = self.fs.open_workspace(&parent_name).await.map_err(display)?;
            if parent.id() != parent_id {
                return Err("Git compatibility branch parent identity changed".to_owned());
            }
            graph
                .authorize_join(lineage_cursor.id(), parent.id())
                .await
                .map_err(display)?;
            lineage_cursor = parent;
        }
        graph
            .authorize_join(lineage_cursor.id(), target.id())
            .await
            .map_err(display)?;
        let plan = source.join_into(&target).plan().await.map_err(display)?;
        let target_head = plan.target_head();
        if caller == self.state.root_agent_id {
            self.begin_root_publication(target_head)?;
        }
        let join_key = self
            .state
            .root_publication
            .as_ref()
            .filter(|_| caller == self.state.root_agent_id)
            .map_or_else(IdempotencyKey::new, |publication| {
                IdempotencyKey::from_bytes(publication.operation_id)
            });
        let outcome = plan
            .apply(ApplyOptions {
                if_target: target_head,
                idempotency_key: join_key,
            })
            .await;
        if caller == self.state.root_agent_id && outcome.is_err() {
            self.recover_root_publication().await?;
        }
        let outcome = outcome.map_err(display)?;
        let published = match &outcome {
            JoinOutcome::Applied(generation)
            | JoinOutcome::AlreadyApplied(generation)
            | JoinOutcome::NoChanges(generation) => Some(generation.clone()),
            _ => None,
        };
        if caller == self.state.root_agent_id
            && let Some(generation) = published
        {
            self.publish_root_generation(target_head, &generation)
                .await?;
        } else if caller == self.state.root_agent_id {
            self.state.root_publication = None;
            self.persist()?;
        }
        if matches!(
            outcome,
            JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) | JoinOutcome::NoChanges(_)
        ) && let Some(parent_mount) = self.mounts.get(&caller)
        {
            parent_mount
                .advance_to_head()
                .await
                .map_err(|error| format!("cannot advance parent mount after merge: {error}"))?;
        }
        Ok(match outcome {
            JoinOutcome::Applied(generation) => {
                json!({"status":"applied","generation":hex::encode(generation.id().digest().as_bytes())})
            }
            JoinOutcome::AlreadyApplied(generation) => {
                json!({"status":"already-applied","generation":hex::encode(generation.id().digest().as_bytes())})
            }
            JoinOutcome::NoChanges(generation) => {
                json!({"status":"no-changes","generation":hex::encode(generation.id().digest().as_bytes())})
            }
            JoinOutcome::StaleTarget(generation) => {
                json!({"status":"stale-target","generation":hex::encode(generation.id().digest().as_bytes())})
            }
            JoinOutcome::Conflicted {
                conflicts,
                truncated,
            } => {
                let typed = plan
                    .describe_conflicts(&conflicts, truncated)
                    .await
                    .map_err(display)?;
                json!({
                    "status":"conflicted",
                    "conflictCount":conflicts.len(),
                    "conflicts": typed.conflicts.iter().map(typed_conflict_json).collect::<Vec<_>>(),
                    "truncated":truncated
                })
            }
            JoinOutcome::Fenced => json!({"status":"fenced"}),
            JoinOutcome::IdempotencyConflict => json!({"status":"idempotency-conflict"}),
        })
    }

    async fn agent_discard(&mut self, input: Value) -> Result<Value, String> {
        let agent = string(&input, "agent")?;
        let caller = self.caller(&input)?;
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
        for descendant in descendants
            .into_iter()
            .rev()
            .chain(std::iter::once(agent.clone()))
        {
            if let Some(mount) = self.mounts.remove(&descendant) {
                mount.unmount().await.map_err(display)?;
            }
            if let Some(removed) = self.state.routes.remove(&descendant) {
                remove_tree_checked(&self.data.join("workspaces"), &removed.path)?;
                let repository_id = if removed.repository_workspace_id == [0; 16] {
                    acyclic_fs::WorkspaceId::from_bytes(removed.workspace_id)
                } else {
                    acyclic_fs::WorkspaceId::from_bytes(removed.repository_workspace_id)
                };
                let mut workspace_ids = BTreeSet::from([
                    repository_id,
                    acyclic_fs::WorkspaceId::from_bytes(removed.workspace_id),
                ]);
                if let Some(state) = <LocalCoreStateStore as acyclic_fs::GitCompatStore>::load(
                    &self.store,
                    repository_id,
                )
                .await
                .map_err(display)?
                {
                    workspace_ids.extend(state.branches.values().map(|branch| branch.workspace_id));
                    if !<LocalCoreStateStore as acyclic_fs::GitCompatStore>::compare_and_delete(
                        &self.store,
                        repository_id,
                        state.revision,
                    )
                    .await
                    .map_err(display)?
                    {
                        return Err(
                            "Git compatibility state changed while discarding the agent".to_owned(),
                        );
                    }
                }
                for workspace_id in workspace_ids.into_iter().rev() {
                    let Ok(record) = WorkspaceGraph::new(self.store.clone())
                        .resolve(workspace_id)
                        .await
                    else {
                        continue;
                    };
                    if let Ok(workspace) = self.fs.open_workspace(&record.workspace_name).await {
                        let _ = workspace.delete(IdempotencyKey::new()).await;
                    }
                }
            }
            self.state.turns.retain(|_, value| value != &descendant);
        }
        self.persist()?;
        Ok(json!({"status":"discarded","agent":agent}))
    }

    fn caller(&self, input: &Value) -> Result<String, String> {
        let turn = input
            .get("_caller_turn_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "workspace control call lacks a stable caller identity".to_owned())?;
        if self.state.root_turns.contains(turn) {
            return Ok(self.state.root_agent_id.clone());
        }
        self.state
            .turns
            .get(turn)
            .cloned()
            .ok_or_else(|| "workspace control caller is unknown".to_owned())
    }

    fn resolve_turn(&self, turn_id: &str) -> Result<String, String> {
        if let Some(agent) = self.state.turns.get(turn_id) {
            return Ok(agent.clone());
        }
        if self.state.root_turns.contains(turn_id) {
            return Ok(self.state.root_agent_id.clone());
        }
        Err("tool call has no stable root or subagent identity".to_owned())
    }

    fn authorize_inspection(&self, caller: &str, target: &str) -> Result<(), String> {
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

    fn descendants(&self, agent: &str) -> Vec<String> {
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

    async fn workspace(&self, route: &Route) -> Result<LocalWorkspace, String> {
        self.fs
            .open_workspace(&route.workspace_name)
            .await
            .map_err(display)
    }

    async fn sync_agent(&self, agent_id: &str) -> Result<(), String> {
        if agent_id == self.state.root_agent_id {
            let workspace = self
                .root
                .as_ref()
                .ok_or_else(|| "root source is unavailable".to_owned())?;
            let source = workspace
                .source()
                .ok_or_else(|| "root source handle is unavailable".to_owned())?;
            let outcome = source.reconcile().await.map_err(display)?;
            return match outcome {
                ReconcileOutcome::Clean(_) => Ok(()),
                ReconcileOutcome::NeedsRescan(_) => match source.rescan().await.map_err(display)? {
                    ReconcileOutcome::Clean(_) => Ok(()),
                    ReconcileOutcome::NeedsRescan(_) => {
                        Err("root source still requires a rescan".to_owned())
                    }
                    ReconcileOutcome::Conflict => {
                        Err("root source conflicts with its workspace".to_owned())
                    }
                },
                ReconcileOutcome::Conflict => {
                    Err("root source conflicts with its workspace".to_owned())
                }
            };
        }
        self.mounts
            .get(agent_id)
            .ok_or_else(|| "subagent mount is unavailable".to_owned())?
            .sync()
            .await
            .map_err(display)
    }

    async fn parent_workspace(&self, route: &Route) -> Result<LocalWorkspace, String> {
        self.workspace_for_agent(&route.parent_agent_id).await
    }

    async fn workspace_for_agent(&self, agent_id: &str) -> Result<LocalWorkspace, String> {
        if agent_id == self.state.root_agent_id {
            return self
                .root
                .clone()
                .ok_or_else(|| "root source is unavailable".to_owned());
        }
        let name = &self
            .state
            .routes
            .get(agent_id)
            .ok_or_else(|| "unknown parent agent".to_owned())?
            .workspace_name;
        self.fs.open_workspace(name).await.map_err(display)
    }

    fn persist(&self) -> Result<(), String> {
        save_state(&self.data, &self.state)
    }

    fn begin_root_publication(&mut self, from: acyclic_fs::GenerationId) -> Result<(), String> {
        if self.state.root_publication.is_some() {
            return Err("another root publication is already pending recovery".to_owned());
        }
        let operation_id = OperationId::new();
        self.state.root_publication = Some(RootPublication {
            phase: RootPublicationPhase::JoinPending,
            operation_id: operation_id.into_bytes(),
            from: *from.digest().as_bytes(),
            to: *from.digest().as_bytes(),
            operation_directory: self
                .state
                .root_path
                .join(".acyclic-sdk")
                .join("materializations")
                .join(hex::encode(operation_id.into_bytes())),
        });
        self.persist()
    }

    async fn publish_root_generation(
        &mut self,
        from: acyclic_fs::GenerationId,
        to: &acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>,
    ) -> Result<(), String> {
        if self.state.root_publication.is_none() {
            self.begin_root_publication(from)?;
        }
        let publication = self
            .state
            .root_publication
            .as_mut()
            .ok_or("root publication intent is missing")?;
        if publication.from != *from.digest().as_bytes() {
            return Err("root publication intent does not match the completed join".to_owned());
        }
        publication.to = *to.id().digest().as_bytes();
        publication.phase = RootPublicationPhase::JoinApplied;
        self.persist()?;
        self.recover_root_publication().await
    }

    async fn recover_root_publication(&mut self) -> Result<(), String> {
        let Some(publication) = self.state.root_publication.clone() else {
            return Ok(());
        };
        let workspace = self
            .root
            .clone()
            .ok_or_else(|| "root source is unavailable".to_owned())?;
        let operation_id = OperationId::from_bytes(publication.operation_id);
        let from = acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(publication.from));
        if publication.phase == RootPublicationPhase::JoinPending {
            let key = IdempotencyKey::from_bytes(publication.operation_id);
            if let Some(generation) = workspace.operation_generation(key).await.map_err(display)? {
                let intent = self
                    .state
                    .root_publication
                    .as_mut()
                    .ok_or("root publication intent disappeared during recovery")?;
                intent.to = *generation.id().digest().as_bytes();
                intent.phase = RootPublicationPhase::JoinApplied;
                self.persist()?;
                return Box::pin(self.recover_root_publication()).await;
            }
            if workspace.head().await.map_err(display)?.id() == from {
                self.state.root_publication = None;
                self.persist()?;
                return Ok(());
            }
            return Err(
                "root join outcome is ambiguous after interruption; refusing to materialize an unbound generation"
                    .to_owned(),
            );
        }
        let to_id = acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(publication.to));
        let target_path = publication.operation_directory.join("target");
        let existing = self.store.load(operation_id).await.map_err(display)?;
        if existing.is_none() {
            if publication.operation_directory.exists() {
                remove_tree_checked(
                    &self.state.root_path.join(".acyclic-sdk/materializations"),
                    &publication.operation_directory,
                )?;
            }
            fs::create_dir_all(&target_path).map_err(display)?;
            workspace
                .generation(to_id)
                .await
                .map_err(display)?
                .materialize(
                    &MaterializeOptions {
                        destination: target_path.clone(),
                        maximum_directory_entries: 4_096,
                        maximum_extent_spans: 65_536,
                        transfer_bytes: 8 * 1024 * 1024,
                    },
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(display)?;
            let backend = NativeTreeMaterializationBackend::new(
                &self.state.root_path,
                &publication.operation_directory,
            )
            .map_err(display)?;
            let plan = backend
                .plan(operation_id, from, to_id, &[".git", ".acyclic-sdk"])
                .map_err(display)?;
            JournaledMaterializer::new(self.store.clone(), backend)
                .apply(plan)
                .await
                .map_err(display)?;
        } else {
            let journal = existing.ok_or("root materialization journal disappeared")?;
            if journal.plan.from != from || journal.plan.to != to_id {
                return Err(
                    "root materialization journal does not match publication intent".to_owned(),
                );
            }
            let backend = NativeTreeMaterializationBackend::new(
                &self.state.root_path,
                &publication.operation_directory,
            )
            .map_err(display)?;
            JournaledMaterializer::new(self.store.clone(), backend)
                .recover(operation_id, MaterializationRecovery::Complete)
                .await
                .map_err(display)?;
        }
        if let Some(source) = workspace.source() {
            match source
                .acknowledge_materialization(IdempotencyKey::from_bytes(operation_id.into_bytes()))
                .await
                .map_err(display)?
            {
                ReconcileOutcome::Clean(_) => {}
                ReconcileOutcome::NeedsRescan(_) | ReconcileOutcome::Conflict => {
                    return Err(
                        "materialized root could not establish a clean source baseline".to_owned(),
                    );
                }
            }
        } else {
            self.root = Some(
                self.fs
                    .attach_directory(
                        &self.state.root_workspace_name,
                        &self.state.root_path,
                        root_source_options()?,
                    )
                    .await
                    .map_err(display)?,
            );
        }
        self.store
            .remove_materialization(operation_id)
            .map_err(display)?;
        remove_tree_checked(
            &self.state.root_path.join(".acyclic-sdk/materializations"),
            &publication.operation_directory,
        )?;
        self.state.root_publication = None;
        self.persist()
    }
}

fn git_apply_patch_path(argv: &[String]) -> Result<&str, String> {
    let arguments = argv
        .first()
        .is_some_and(|command| command == "apply")
        .then_some(&argv[1..])
        .ok_or_else(|| "Git apply argument parsing requires the apply subcommand".to_owned())?;
    let mut positional = Vec::new();
    let mut options_ended = false;
    for argument in arguments {
        if !options_ended && argument == "--" {
            options_ended = true;
        } else if !options_ended && argument.starts_with('-') {
            return Err(format!(
                "unsupported git apply option '{argument}'; use one standalone patch file"
            ));
        } else {
            positional.push(argument.as_str());
        }
    }
    match positional.as_slice() {
        [path] => Ok(path),
        _ => Err("git apply requires exactly one patch file".to_owned()),
    }
}

fn rewrite_tool_input(
    tool_name: &str,
    mut input: Value,
    root: &Path,
    child: &Path,
) -> Result<Value, String> {
    let root_text = root.to_string_lossy();
    let child_text = child.to_string_lossy();
    #[cfg(not(target_os = "linux"))]
    reject_original_root_references(&input, None, &root_text)?;
    rewrite_strings(&mut input, &root_text, &child_text);
    if tool_name == "Bash" || tool_name == "exec_command" {
        if let Some(object) = input.as_object_mut() {
            object.insert("workdir".to_owned(), Value::String(child_text.into_owned()));
        }
    } else if tool_name == "apply_patch" {
        let command = input
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| "apply_patch lacks a command string".to_owned())?
            .to_owned();
        let rewritten = command
            .lines()
            .map(|line| rewrite_patch_header(line, child))
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");
        input["command"] = Value::String(rewritten);
    }
    validate_tool_paths(tool_name, &input, child)?;
    Ok(input)
}

fn git_argv(input: &Value) -> Result<Option<Vec<String>>, String> {
    let command = input
        .get("cmd")
        .or_else(|| input.get("command"))
        .and_then(Value::as_str)
        .ok_or_else(|| "shell tool input lacks a string command".to_owned())?;
    let argv = split_standalone_command(command)?;
    let Some(program) = argv.first() else {
        return Ok(None);
    };
    let is_git = Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.eq_ignore_ascii_case("git") || name.eq_ignore_ascii_case("git.exe")
        });
    if is_git {
        if argv.len() == 1 {
            return Err("Git compatibility command is missing a subcommand".to_owned());
        }
        return Ok(Some(argv[1..].to_vec()));
    }
    if argv.iter().skip(1).any(|argument| {
        Path::new(argument)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.eq_ignore_ascii_case("git") || name.eq_ignore_ascii_case("git.exe")
            })
    }) {
        return Err(
            "Git compatibility commands must be issued as a standalone shell command".to_owned(),
        );
    }
    Ok(None)
}

fn split_standalone_command(command: &str) -> Result<Vec<String>, String> {
    let mut arguments = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in command.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        match quote {
            Some('\'') => {
                if character == '\'' {
                    quote = None;
                } else {
                    current.push(character);
                }
            }
            Some('"') => match character {
                '"' => quote = None,
                '\\' => escaped = true,
                '$' | '`' => {
                    return Err(
                        "shell expansion is not allowed in a Git compatibility command".to_owned(),
                    );
                }
                _ => current.push(character),
            },
            Some(_) => unreachable!(),
            None => match character {
                '\'' | '"' => quote = Some(character),
                '\\' => escaped = true,
                ' ' | '\t' => {
                    if !current.is_empty() {
                        arguments.push(std::mem::take(&mut current));
                    }
                }
                '\r' | '\n' | ';' | '|' | '&' | '<' | '>' | '$' | '`' | '(' | ')' => {
                    return Err(
                        "Git compatibility commands cannot be composed with shell operators"
                            .to_owned(),
                    );
                }
                _ => current.push(character),
            },
        }
    }
    if quote.is_some() || escaped {
        return Err("Git compatibility command has an unterminated quote or escape".to_owned());
    }
    if !current.is_empty() {
        arguments.push(current);
    }
    Ok(arguments)
}

fn set_shell_command(input: &mut Value, command: String) -> Result<(), String> {
    let object = input
        .as_object_mut()
        .ok_or_else(|| "shell tool input must be an object".to_owned())?;
    if let Some(value) = object.get_mut("cmd") {
        *value = Value::String(command);
        return Ok(());
    }
    if let Some(value) = object.get_mut("command") {
        *value = Value::String(command);
        return Ok(());
    }
    Err("shell tool input lacks a command field".to_owned())
}

fn render_git_output(output: &acyclic_fs::GitCommandOutput) -> Result<String, String> {
    let encoded = base64::engine::general_purpose::STANDARD
        .encode(serde_json::to_vec(output).map_err(display)?);
    #[cfg(target_os = "windows")]
    {
        Ok(format!(
            "$b='{encoded}';[Console]::Write([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($b)))"
        ))
    }
    #[cfg(not(target_os = "windows"))]
    {
        Ok(format!("printf '%s' '{encoded}' | base64 --decode"))
    }
}

fn pre_tool_update(updated: Value) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "updatedInput": updated
        }
    })
}

fn validate_tool_paths(tool_name: &str, input: &Value, child: &Path) -> Result<(), String> {
    if (tool_name == "Bash" || tool_name == "exec_command" || tool_name.ends_with("__exec_command"))
        && let Some(command) = input
            .get("cmd")
            .or_else(|| input.get("command"))
            .and_then(Value::as_str)
    {
        validate_shell_paths(command, child)?;
    }
    validate_structured_paths(input, None, child)
}

fn validate_structured_paths(value: &Value, key: Option<&str>, child: &Path) -> Result<(), String> {
    match value {
        Value::String(path) if key.is_some_and(is_path_field) => validate_path(path, child),
        Value::Array(values) => {
            for value in values {
                validate_structured_paths(value, key, child)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                validate_structured_paths(value, Some(key), child)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn is_path_field(key: &str) -> bool {
    matches!(
        key,
        "path"
            | "file"
            | "file_path"
            | "directory"
            | "destination"
            | "source"
            | "cwd"
            | "workdir"
            | "referenced_image_paths"
    )
}

fn validate_path(path: &str, child: &Path) -> Result<(), String> {
    let path = Path::new(path);
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err("parent traversal is denied in an isolated agent workspace".to_owned());
    }
    if path.is_absolute() && !path.starts_with(child) {
        return Err(format!(
            "absolute path '{}' escapes the isolated agent workspace",
            path.display()
        ));
    }
    let child_root = child.canonicalize().map_err(display)?;
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        child.join(path)
    };
    let mut existing = candidate.as_path();
    while fs::symlink_metadata(existing).is_err() {
        existing = existing
            .parent()
            .ok_or_else(|| "path has no existing isolated ancestor".to_owned())?;
    }
    let resolved = existing.canonicalize().map_err(display)?;
    if !resolved.starts_with(&child_root) {
        return Err(format!(
            "path '{}' resolves through a link outside the isolated agent workspace",
            path.display()
        ));
    }
    Ok(())
}

fn validate_shell_paths(command: &str, child: &Path) -> Result<(), String> {
    if command.contains(['$', '`'])
        || cfg!(target_os = "windows") && command.contains('%')
        || command
            .split_whitespace()
            .any(|token| token.starts_with('~'))
    {
        return Err(
            "shell expansion is denied in an isolated agent workspace; use paths relative to the workspace"
                .to_owned(),
        );
    }
    for token in command.split(|character: char| {
        character.is_whitespace()
            || matches!(
                character,
                '\'' | '"' | '(' | ')' | '[' | ']' | '{' | '}' | ';' | ','
            )
    }) {
        let token = token.trim_matches(|character: char| matches!(character, '`' | '&' | '|'));
        if token.is_empty() || token.starts_with('-') {
            continue;
        }
        validate_path(token, child)?;
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn reject_original_root_references(
    value: &Value,
    key: Option<&str>,
    root: &str,
) -> Result<(), String> {
    match value {
        Value::String(text) if !matches!(key, Some("workdir" | "cwd")) => {
            let contains = if cfg!(target_os = "windows") {
                text.to_lowercase().contains(&root.to_lowercase())
            } else {
                text.contains(root)
            };
            if contains {
                return Err(
                    "hard-coded parent checkout paths are denied in an isolated agent workspace"
                        .to_owned(),
                );
            }
            Ok(())
        }
        Value::Array(values) => {
            for value in values {
                reject_original_root_references(value, key, root)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                reject_original_root_references(value, Some(key), root)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn root_source_options() -> Result<SourceOptions, String> {
    Ok(SourceOptions {
        excluded_paths: vec![
            PortablePath::parse("/.git", VolumeLimits::default()).map_err(display)?,
            PortablePath::parse("/.acyclic-sdk", VolumeLimits::default()).map_err(display)?,
        ],
        ..SourceOptions::default()
    })
}

fn is_filesystem_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "Bash"
            | "exec_command"
            | "apply_patch"
            | "Edit"
            | "Write"
            | "Read"
            | "view_image"
            | "image_gen__imagegen"
    ) || tool_name.ends_with("__exec_command")
        || tool_name.starts_with("mcp__filesystem__")
        || tool_name.starts_with("mcp__acyclic_agent_workspaces__")
}

fn is_pure_remote_tool(tool_name: &str) -> bool {
    tool_name.starts_with("web__")
        || tool_name.starts_with("collaboration.")
        || tool_name.starts_with("mcp__codex_app__")
        || matches!(tool_name, "send_message" | "wait_agent" | "list_agents")
}

fn require_process_sandbox(tool_name: &str) -> Result<(), String> {
    if !(matches!(tool_name, "Bash" | "exec_command")
        || tool_name.ends_with("__exec_command"))
    {
        return Ok(());
    }
    if env::var("ACYCLIC_AGENT_WORKSPACE_PROCESS_SANDBOX").as_deref() == Ok("1") {
        return Ok(());
    }
    Err(
        "arbitrary shell execution is denied because this host has not established a process namespace rooted at the isolated agent workspace; use structured filesystem tools or the SDK Git compatibility commands"
            .to_owned(),
    )
}

fn normalize_tool_input(input: Value) -> Result<Value, String> {
    match input {
        Value::String(text) => serde_json::from_str(&text)
            .map_err(|error| format!("hook tool_input is not valid JSON: {error}")),
        value => Ok(value),
    }
}

fn rewrite_strings(value: &mut Value, from: &str, to: &str) {
    match value {
        Value::String(text) => *text = text.replace(from, to),
        Value::Array(values) => {
            for value in values {
                rewrite_strings(value, from, to);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                rewrite_strings(value, from, to);
            }
        }
        _ => {}
    }
}

fn rewrite_patch_header(line: &str, child: &Path) -> Result<String, String> {
    for prefix in ["*** Add File: ", "*** Update File: ", "*** Delete File: "] {
        if let Some(path) = line.strip_prefix(prefix) {
            let path = Path::new(path);
            if path
                .components()
                .any(|component| matches!(component, Component::ParentDir))
            {
                return Err("parent traversal is denied in an isolated agent patch".to_owned());
            }
            if path.is_absolute() {
                if !path.starts_with(child) {
                    return Err(format!(
                        "absolute patch path '{}' escapes the isolated agent workspace",
                        path.display()
                    ));
                }
                return Ok(line.to_owned());
            }
            return Ok(format!("{prefix}{}", child.join(path).display()));
        }
    }
    Ok(line.to_owned())
}

fn subagent_context(path: &Path) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "SubagentStart",
            "additionalContext": format!(
                "Your filesystem is an isolated Acyclic workspace mounted at {}. Tool paths are redirected automatically. Do not access the parent checkout by a hard-coded path.",
                path.display()
            )
        }
    })
}

fn load_state(data: &Path) -> Result<AdapterState, String> {
    let path = data.join("adapter-state.json");
    let previous = data.join("adapter-state.previous.json");
    match read_state(&path) {
        Ok(Some(state)) => Ok(state),
        Ok(None) | Err(_) => match read_state(&previous) {
            Ok(Some(state)) => Ok(state),
            Ok(None) => Ok(AdapterState::default()),
            Err(error) => Err(error),
        },
    }
}

fn save_state(data: &Path, state: &AdapterState) -> Result<(), String> {
    let path = data.join("adapter-state.json");
    let previous = data.join("adapter-state.previous.json");
    let next = data.join("adapter-state.next.json");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&next)
        .map_err(display)?;
    file.write_all(&serde_json::to_vec(state).map_err(display)?)
        .map_err(display)?;
    file.sync_all().map_err(display)?;
    drop(file);
    remove_file_if_present(&previous)?;
    if path.exists() {
        fs::rename(&path, &previous).map_err(display)?;
    }
    if let Err(error) = fs::rename(&next, &path) {
        if previous.exists() && !path.exists() {
            let _ = fs::rename(&previous, &path);
        }
        return Err(display(error));
    }
    remove_file_if_present(&previous)
}

fn read_state(path: &Path) -> Result<Option<AdapterState>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(display),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

fn remove_file_if_present(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(display(error)),
    }
}

fn remove_tree_checked(root: &Path, target: &Path) -> Result<(), String> {
    let root = root.canonicalize().map_err(display)?;
    let target = target.canonicalize().map_err(display)?;
    if target.parent() != Some(root.as_path()) {
        return Err("refusing to discard a path outside the plugin workspace root".to_owned());
    }
    fs::remove_dir_all(target).map_err(display)
}

fn string(value: &Value, field: &str) -> Result<String, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("missing string field '{field}'"))
}

fn short_hash(bytes: &[u8]) -> String {
    hex::encode(&blake3::hash(bytes).as_bytes()[..12])
}

fn conflict_json(conflict: &MergeConflict) -> Value {
    match conflict {
        MergeConflict::File(file_id) => json!({
            "kind": "file-record",
            "fileId": hex::encode(file_id.into_bytes())
        }),
        MergeConflict::Binding { directory_id, name } => json!({
            "kind": "directory-binding",
            "directoryId": hex::encode(directory_id.into_bytes()),
            "nameEncoding": format!("{:?}", name.encoding()),
            "nameBytes": hex::encode(name.as_bytes())
        }),
    }
}

fn typed_conflict_json(conflict: &acyclic_fs::ConflictView) -> Value {
    json!({
        "key": conflict.key,
        "path": conflict.path,
        "kind": conflict.kind,
        "base": conflict.base,
        "ours": conflict.ours,
        "theirs": conflict.theirs,
    })
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn public_tools() -> Value {
    json!([
        {"name":"agent_changes","description":"Inspect one descendant agent workspace without publishing it.","inputSchema":{"type":"object","properties":{"agent":{"type":"string"},"path":{"type":"string"}},"required":["agent"],"additionalProperties":false}},
        {"name":"agent_merge","description":"Publish one direct child's current workspace into its parent.","inputSchema":{"type":"object","properties":{"agent":{"type":"string"}},"required":["agent"],"additionalProperties":false}},
        {"name":"agent_discard","description":"Discard one direct child and its unpublished descendant subtree.","inputSchema":{"type":"object","properties":{"agent":{"type":"string"}},"required":["agent"],"additionalProperties":false}}
    ])
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let result = std::thread::Builder::new()
        .name("acyclic-agent-workspaces".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(32 * 1024 * 1024)
                .build()
                .map_err(display)?
                .block_on(run())
                .map_err(display)
        })?
        .join()
        .map_err(|_| io::Error::other("control-plane thread panicked"))?;
    result.map_err(io::Error::other)?;
    Ok(())
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let data = env::var_os("PLUGIN_DATA")
        .or_else(|| env::var_os("CLAUDE_PLUGIN_DATA"))
        .map(PathBuf::from)
        .unwrap_or_else(|| env::temp_dir().join("acyclic-agent-workspaces"));
    let mut control = ControlPlane::open(data).await.map_err(io::Error::other)?;
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = serde_json::from_str(&line)?;
        let Some(id) = request.get("id").cloned() else {
            continue;
        };
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let response = match method {
            "initialize" => {
                json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"acyclic-agent-workspaces","version":"0.1.0"}}})
            }
            "ping" => json!({"jsonrpc":"2.0","id":id,"result":{}}),
            "tools/list" => json!({"jsonrpc":"2.0","id":id,"result":{"tools":public_tools()}}),
            "tools/call" => {
                let params = request.get("params").cloned().unwrap_or(Value::Null);
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                let arguments = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                match control.call(name, arguments).await {
                    Ok(result) => {
                        json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":serde_json::to_string(&result)?}]}})
                    }
                    Err(error) => {
                        json!({"jsonrpc":"2.0","id":id,"result":{"isError":true,"content":[{"type":"text","text":error}]}})
                    }
                }
            }
            _ => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}})
            }
        };
        serde_json::to_writer(&mut stdout, &response)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_publication_is_direct_parent_only() {
        std::thread::Builder::new()
            .name("plugin-e2e".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(recursive_publication_case());
            })
            .expect("test thread")
            .join()
            .expect("plugin e2e thread");
    }

    #[test]
    fn compatibility_branch_publishes_through_repository_and_discards() {
        std::thread::Builder::new()
            .name("plugin-git-branch-e2e".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(compatibility_branch_case());
            })
            .expect("test thread")
            .join()
            .expect("plugin Git branch e2e thread");
    }

    async fn compatibility_branch_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root directory");
        let mut control = ControlPlane::open(temporary.path().join("plugin-data"))
            .await
            .expect("control plane");
        control
            .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
            .await
            .expect("root session");
        control
            .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
            .expect("root turn");
        control
            .pre_tool(json!({
                "turn_id":"root-turn","tool_use_id":"spawn-child",
                "tool_name":"spawn_agent","tool_input":{}
            }))
            .await
            .expect("child spawn");
        control
            .subagent_start(json!({
                "session_id":"session","turn_id":"child-turn",
                "agent_id":"child","agent_type":"explorer"
            }))
            .await
            .expect("child start");
        assert!(
            control
                .pre_tool(json!({
                    "turn_id":"child-turn","tool_use_id":"git-apply-missing",
                    "tool_name":"exec_command",
                    "tool_input":{"cmd":"git apply missing.patch","workdir":root.display().to_string()}
                }))
                .await
                .is_err()
        );
        let initial_route = control.state.routes["child"].clone();
        let initial_workspace = control
            .workspace(&initial_route)
            .await
            .expect("initial child workspace");
        assert!(matches!(
            OperationWindowCoordinator::new(control.store.clone())
                .snapshot(initial_workspace.id())
                .await
                .expect("operation window after rejected Git command")
                .phase,
            acyclic_fs::OperationWindowPhase::Idle
        ));
        control
            .pre_tool(json!({
                "turn_id":"child-turn","tool_use_id":"git-switch",
                "tool_name":"exec_command",
                "tool_input":{"cmd":"git switch -c feature","workdir":root.display().to_string()}
            }))
            .await
            .expect("Git branch switch");
        control
            .post_tool(json!({"tool_use_id":"git-switch"}))
            .await
            .expect("Git switch post hook");
        let route = control.state.routes["child"].clone();
        let branch = control.workspace(&route).await.expect("branch workspace");
        assert_ne!(
            branch.id(),
            acyclic_fs::WorkspaceId::from_bytes(route.repository_workspace_id)
        );
        let root_workspace = control.root.clone().expect("root workspace");
        assert!(
            WorkspaceGraph::new(control.store.clone())
                .authorize_join(branch.id(), root_workspace.id())
                .await
                .is_err(),
            "a compatibility branch cannot bypass its repository lineage"
        );
        let mut transaction = branch
            .begin_transaction(IdempotencyKey::new())
            .await
            .expect("branch transaction");
        transaction
            .write_text("/branch.txt", "branch")
            .await
            .expect("branch file");
        transaction.commit().await.expect("commit branch file");
        control.mounts["child"]
            .advance_to_head()
            .await
            .expect("advance branch mount");
        let merged = control
            .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .expect("publish compatibility branch");
        assert!(matches!(merged["status"].as_str(), Some("applied" | "already-applied")));
        assert_eq!(fs::read(root.join("branch.txt")).expect("root branch file"), b"branch");
        let repository_id = acyclic_fs::WorkspaceId::from_bytes(route.repository_workspace_id);
        let repository = GitCompatRepository::new(repository_id, control.store.clone());
        assert!(matches!(
            repository
                .execute(
                    GitCommand::Switch {
                        branch: "main".to_owned(),
                        create: false,
                    },
                    branch.head().await.expect("branch head").id(),
                )
                .await
                .expect("prepare interrupted switch"),
            acyclic_fs::GitCommandOutput::Prepared { .. }
        ));
        drop(control);

        let mut control = ControlPlane::open(temporary.path().join("plugin-data"))
            .await
            .expect("reopen with interrupted Git transition");
        assert!(
            GitCompatRepository::new(repository_id, control.store.clone())
                .pending_transition()
                .await
                .expect("pending transition")
                .is_some()
        );
        assert!(control
            .pre_tool(json!({
                "turn_id":"child-turn","tool_use_id":"recover-git-switch",
                "tool_name":"exec_command",
                "tool_input":{"cmd":"git status","workdir":root.display().to_string()}
            }))
            .await
            .is_err());
        drop(control);
        let mut control = ControlPlane::open(temporary.path().join("plugin-data"))
            .await
            .expect("reopen after leased Git recovery");
        assert_eq!(control.state.routes["child"].workspace_id, repository_id.into_bytes());
        control
            .pre_tool(json!({
                "turn_id":"child-turn","tool_use_id":"retry-git-status",
                "tool_name":"exec_command",
                "tool_input":{"cmd":"git status","workdir":root.display().to_string()}
            }))
            .await
            .expect("retry Git status after leased recovery");
        control
            .user_prompt(json!({"session_id":"session","turn_id":"root-turn-recovered"}))
            .expect("recovered root turn");
        control
            .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn-recovered"}))
            .await
            .expect("discard compatibility branch");
        assert!(!control.state.routes.contains_key("child"));
        assert!(!control.mounts.contains_key("child"));
        assert!(
            <LocalCoreStateStore as acyclic_fs::GitCompatStore>::load(
                &control.store,
                repository_id,
            )
            .await
            .expect("load discarded Git state")
            .is_none()
        );
        drop(control);

        let mut control = ControlPlane::open(temporary.path().join("plugin-data"))
            .await
            .expect("reopen control plane");
        control
            .user_prompt(json!({"session_id":"session","turn_id":"root-turn-reopened"}))
            .expect("reopened root turn");
        control
            .pre_tool(json!({
                "turn_id":"root-turn-reopened","tool_use_id":"spawn-reused-child",
                "tool_name":"spawn_agent","tool_input":{}
            }))
            .await
            .expect("reused child spawn");
        control
            .subagent_start(json!({
                "session_id":"session","turn_id":"child-turn-reused",
                "agent_id":"child","agent_type":"explorer"
            }))
            .await
            .expect("reuse discarded child identity");
        let reused = &control.state.routes["child"];
        assert_ne!(reused.repository_workspace_id, route.repository_workspace_id);
    }

    async fn recursive_publication_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root directory");
        let mut control = ControlPlane::open(temporary.path().join("plugin-data"))
            .await
            .expect("control plane");
        control
            .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
            .await
            .expect("root session");
        control
            .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
            .expect("root turn");
        control
            .user_prompt(json!({"session_id":"session","turn_id":"root-turn-2"}))
            .expect("second root turn");
        assert_eq!(
            control
                .resolve_turn("root-turn-2")
                .expect("resolve second root turn"),
            control.state.root_agent_id
        );
        control
            .pre_tool(json!({
                "turn_id":"root-turn","tool_use_id":"spawn-child",
                "tool_name":"spawn_agent","tool_input":{}
            }))
            .await
            .expect("child spawn");
        control
            .subagent_start(json!({
                "session_id":"session","turn_id":"child-turn",
                "agent_id":"child","agent_type":"explorer"
            }))
            .await
            .expect("child start");
        let child_path = control.state.routes["child"].path.clone();
        let git_commit = control
            .pre_tool(json!({
                "turn_id":"child-turn","tool_use_id":"git-commit",
                "tool_name":"exec_command",
                "tool_input":{"cmd":"git commit -m initial","workdir":root.display().to_string()}
            }))
            .await
            .expect("transparent Git commit");
        assert!(
            git_commit["hookSpecificOutput"]["updatedInput"]["cmd"]
                .as_str()
                .is_some_and(
                    |command| command.contains("base64") || command.contains("FromBase64String")
                )
        );
        control
            .post_tool(json!({"tool_use_id":"git-commit"}))
            .await
            .expect("Git echo post hook");
        let route = &control.state.routes["child"];
        let repository_id = acyclic_fs::WorkspaceId::from_bytes(route.repository_workspace_id);
        let status = GitCompatRepository::new(repository_id, control.store.clone())
            .execute(
                acyclic_fs::GitCommand::Status,
                control
                    .workspace(route)
                    .await
                    .expect("child workspace")
                    .head()
                    .await
                    .expect("child head")
                    .id(),
            )
            .await
            .expect("Git status");
        assert!(matches!(
            status,
            acyclic_fs::GitCommandOutput::Status(acyclic_fs::GitStatus { dirty: false, .. })
        ));
        let read_hook = json!({
            "turn_id":"child-turn","tool_use_id":"read-retry",
            "tool_name":"Read","tool_input":{"path":child_path.join("base.txt").display().to_string()}
        });
        let first = control
            .pre_tool(read_hook.clone())
            .await
            .expect("first read hook");
        let retry = control
            .pre_tool(read_hook)
            .await
            .expect("retried read hook");
        assert_eq!(first, retry);
        let child_workspace = control
            .workspace(&control.state.routes["child"])
            .await
            .expect("child workspace");
        let snapshot = OperationWindowCoordinator::new(control.store.clone())
            .snapshot(child_workspace.id())
            .await
            .expect("operation window");
        assert!(matches!(
            snapshot.phase,
            acyclic_fs::OperationWindowPhase::Active { ref leases, .. } if leases.len() == 1
        ));
        control
            .post_tool(json!({"tool_use_id":"read-retry"}))
            .await
            .expect("close read hook");
        control
            .pre_tool(json!({
                "turn_id":"child-turn","tool_use_id":"spawn-grandchild",
                "tool_name":"spawn_agent","tool_input":{}
            }))
            .await
            .expect("grandchild spawn");
        control
            .subagent_start(json!({
                "session_id":"session","turn_id":"grandchild-turn",
                "agent_id":"grandchild","agent_type":"explorer"
            }))
            .await
            .expect("grandchild start");
        let grandchild = control
            .workspace(&control.state.routes["grandchild"])
            .await
            .expect("grandchild workspace");
        let mut transaction = grandchild
            .begin_transaction(IdempotencyKey::new())
            .await
            .expect("grandchild transaction");
        transaction
            .write_text("/nested.txt", "nested")
            .await
            .expect("nested file");
        transaction.commit().await.expect("commit nested file");

        assert!(
            control
                .agent_merge(json!({
                    "agent":"grandchild","_caller_turn_id":"root-turn"
                }))
                .await
                .is_err()
        );
        control
            .agent_merge(json!({
                "agent":"grandchild","_caller_turn_id":"child-turn"
            }))
            .await
            .expect("merge into child");
        control
            .agent_merge(json!({
                "agent":"child","_caller_turn_id":"root-turn"
            }))
            .await
            .expect("merge into root");
        assert_eq!(
            fs::read(root.join("nested.txt")).expect("nested root file"),
            b"nested"
        );
    }

    #[test]
    fn patch_paths_are_rewritten_inside_the_child() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let child = temporary.path().join("workspace");
        fs::create_dir(&child).expect("child directory");
        let input = json!({"command": "*** Begin Patch\n*** Add File: src/new.rs\n*** End Patch"});
        let rewritten = rewrite_tool_input("apply_patch", input, temporary.path(), &child)
            .expect("rewrite patch");
        let command = rewritten["command"].as_str().expect("command");
        assert!(command.contains(&child.join("src/new.rs").display().to_string()));
    }

    #[test]
    fn patch_paths_cannot_escape_the_child() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        let child = temporary.path().join("workspace");
        fs::create_dir(&root).expect("root directory");
        fs::create_dir(&child).expect("child directory");
        let outside = temporary.path().join("outside.txt").display().to_string();
        for path in ["../parent.txt", &outside] {
            let input =
                json!({"command": format!("*** Begin Patch\n*** Add File: {path}\n*** End Patch")});
            assert!(rewrite_tool_input("apply_patch", input, &root, &child).is_err());
        }
    }

    #[test]
    fn structured_paths_must_remain_inside_the_child() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let child = temporary.path().join("workspace");
        fs::create_dir(&child).expect("child directory");
        let valid = json!({"path": child.join("src/lib.rs").display().to_string()});
        validate_tool_paths("Read", &valid, &child).expect("child path");
        assert!(validate_tool_paths("Read", &json!({"path": "../parent"}), &child).is_err());
        let outside = temporary.path().join("outside/file.rs");
        assert!(
            validate_tool_paths(
                "Read",
                &json!({"path": outside.display().to_string()}),
                &child
            )
            .is_err()
        );
    }

    #[test]
    fn shell_expansion_cannot_escape_the_child() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let child = temporary.path().join("workspace");
        fs::create_dir(&child).expect("child directory");
        assert!(validate_shell_paths("type $env:TEMP\\secret", &child).is_err());
        assert!(validate_shell_paths("cat `pwd`/secret", &child).is_err());
        assert!(validate_shell_paths("cat ~/secret", &child).is_err());
    }

    #[test]
    fn runtime_derived_shell_paths_fail_closed_without_a_process_namespace() {
        assert!(require_process_sandbox("exec_command").is_err());
        assert!(require_process_sandbox("mcp__host__exec_command").is_err());
        assert!(require_process_sandbox("Read").is_ok());
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn hard_coded_parent_root_is_rejected_but_tool_workdir_is_redirected() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        let child = temporary.path().join("workspace");
        fs::create_dir(&root).expect("root directory");
        fs::create_dir(&child).expect("child directory");
        let root_text = root.display().to_string();
        assert!(
            rewrite_tool_input(
                "exec_command",
                json!({"cmd": format!("type {root_text}\\secret"), "workdir": root_text}),
                &root,
                &child,
            )
            .is_err()
        );
        let rewritten = rewrite_tool_input(
            "exec_command",
            json!({"cmd": "type relative.txt", "workdir": root.display().to_string()}),
            &root,
            &child,
        )
        .expect("redirect workdir");
        assert_eq!(rewritten["workdir"], child.display().to_string());
    }

    #[test]
    fn git_commands_must_be_standalone_and_are_shell_split() {
        assert_eq!(
            split_standalone_command("git commit -m 'two words'").expect("split"),
            vec!["git", "commit", "-m", "two words"]
        );
        assert!(split_standalone_command("git status && echo escaped").is_err());
        assert!(git_argv(&json!({"cmd":"env git status"})).is_err());
        assert_eq!(
            git_argv(&json!({"cmd":"git status --short"})).expect("Git argv"),
            Some(vec!["status".to_owned(), "--short".to_owned()])
        );
        assert_eq!(
            git_apply_patch_path(&["apply".to_owned(), "--".to_owned(), "fix.patch".to_owned()])
                .expect("patch path"),
            "fix.patch"
        );
        assert!(
            git_apply_patch_path(&[
                "apply".to_owned(),
                "--check".to_owned(),
                "fix.patch".to_owned()
            ])
            .is_err()
        );
        assert!(
            git_apply_patch_path(&[
                "apply".to_owned(),
                "one.patch".to_owned(),
                "two.patch".to_owned()
            ])
            .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn structured_paths_reject_symlink_escapes() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("temporary directory");
        let child = temporary.path().join("workspace");
        let outside = temporary.path().join("outside");
        fs::create_dir(&child).expect("child directory");
        fs::create_dir(&outside).expect("outside directory");
        symlink(&outside, child.join("escape")).expect("escape symlink");
        assert!(
            validate_tool_paths(
                "Read",
                &json!({"path": child.join("escape/file").display().to_string()}),
                &child
            )
            .is_err()
        );
    }
}
