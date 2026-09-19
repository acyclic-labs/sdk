use acyclic_fs::model::VolumeLimits;
use acyclic_fs::path::PortablePath;
use acyclic_fs::{
    ApplyOptions, CancellationToken, GitCompatRepository, GitFilesystemAction,
    GitFilesystemExecutor, GitFilesystemResult, GitIgnorePolicy, IdempotencyKey, JoinOutcome,
    JournaledMaterializer, LocalAuthorityBackend, LocalCoreStateStore, LocalFs, LocalObjectBackend,
    LocalOptions, MaterializationJournalStore, MaterializationRecovery, MaterializeOptions,
    MergeConflict, Mount, MountOptions, NativeTreeMaterializationBackend, OperationId,
    OperationReconcileLimits, OperationWindowCoordinator, OperationWindowLease, ReconcileOutcome,
    SourceOptions, TransactionCommit, WorkBudget, Workspace, WorkspaceDelete, WorkspaceGraph,
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
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex as AsyncMutex, watch};

type LocalWorkspace = Workspace<LocalAuthorityBackend, LocalObjectBackend>;
type LocalMount = Mount<LocalAuthorityBackend, LocalObjectBackend>;

#[allow(
    dead_code,
    reason = "used by the authenticated acyclic-git IPC dispatcher"
)]
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

#[allow(
    dead_code,
    reason = "used by the authenticated acyclic-git IPC dispatcher"
)]
struct PluginGitExecutor<'a> {
    fs: &'a LocalFs,
    store: LocalCoreStateStore,
    current: LocalWorkspace,
    repository_id: acyclic_fs::WorkspaceId,
    ignore: GitIgnorePolicy,
    switched: Mutex<Option<LocalWorkspace>>,
}

#[allow(
    dead_code,
    reason = "used by the authenticated acyclic-git IPC dispatcher"
)]
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
    #[serde(default)]
    stopped: bool,
    #[serde(default)]
    published_generation: [u8; 32],
    #[serde(default)]
    route_epoch: u64,
    #[serde(skip)]
    workspace_token: [u8; 32],
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
    #[serde(default)]
    turn_id: String,
    #[serde(default)]
    tool_name: String,
    workspace_id: [u8; 16],
    lease_id: [u8; 16],
    pinned_parent: [u8; 32],
    expires_at_millis: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IpcToolRecord {
    agent_id: String,
    turn_id: String,
    tool_name: String,
    #[serde(default)]
    expires_at_millis: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DiscardWorkspace {
    name: String,
    delete_key: [u8; 16],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DiscardAgent {
    agent_id: String,
    path: PathBuf,
    repository_workspace_id: [u8; 16],
    #[serde(default)]
    mount_detached: bool,
    workspaces: Vec<DiscardWorkspace>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingDiscard {
    agents: Vec<DiscardAgent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RootPublication {
    #[serde(default)]
    phase: RootPublicationPhase,
    operation_id: [u8; 16],
    from: [u8; 32],
    to: [u8; 32],
    operation_directory: PathBuf,
    #[serde(default)]
    source_agent_id: String,
    #[serde(default)]
    source_generation: [u8; 32],
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
enum RootPublicationPhase {
    #[default]
    JoinPending,
    JoinApplied,
}

impl LeaseRecord {
    fn from_lease(
        agent_id: String,
        turn_id: String,
        tool_name: String,
        lease: &OperationWindowLease,
    ) -> Self {
        Self {
            agent_id,
            turn_id,
            tool_name,
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
    ipc_tools: BTreeMap<String, IpcToolRecord>,
    #[serde(default)]
    pending_discards: BTreeMap<String, PendingDiscard>,
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
    control_endpoint: Option<String>,
    cli_bin: Option<PathBuf>,
    #[cfg(test)]
    fail_next_unmount: BTreeSet<String>,
    #[cfg(test)]
    fail_after_discard_delete: bool,
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
            control_endpoint: None,
            cli_bin: env::var_os("PLUGIN_ROOT").map(|root| PathBuf::from(root).join("bin")),
            #[cfg(test)]
            fail_next_unmount: BTreeSet::new(),
            #[cfg(test)]
            fail_after_discard_delete: false,
        };
        control.rotate_route_capabilities()?;
        control.restore_root().await?;
        control.recover_root_publication().await?;
        control.restore_mounts().await?;
        control.recover_expired_adapter_leases().await?;
        control.recover_pending_discards().await?;
        Ok(control)
    }

    async fn recover_expired_adapter_leases(&mut self) -> Result<(), String> {
        let now = now_millis();
        let agents = self
            .state
            .leases
            .values()
            .filter(|lease| lease.expires_at_millis <= now)
            .map(|lease| lease.agent_id.clone())
            .collect::<BTreeSet<_>>();
        if agents.is_empty() {
            return Ok(());
        }
        for agent_id in agents {
            let route = self
                .state
                .routes
                .get(&agent_id)
                .cloned()
                .ok_or_else(|| "expired lease route is missing".to_owned())?;
            let workspace = self.workspace(&route).await?;
            OperationWindowCoordinator::new(self.store.clone())
                .recover_workspace(&workspace, now, OperationReconcileLimits::default())
                .await
                .map_err(display)?;
        }
        self.state
            .leases
            .retain(|_, lease| lease.expires_at_millis > now);
        self.persist()
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
        for route in self.state.routes.values() {
            if !route.stopped || discarding_agents.contains(&route.agent_id) {
                fs::create_dir_all(&route.path).map_err(display)?;
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
        let turn_id = string(&input, "turn_id")?;
        if self.state.turns.contains_key(&turn_id) {
            return Err("root turn identity is already bound to a subagent".to_owned());
        }
        self.state.root_turns.insert(turn_id);
        self.persist()?;
        Ok(json!({"suppressOutput": true}))
    }

    async fn pre_tool(&mut self, input: Value) -> Result<Value, String> {
        self.require_session(&input)?;
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
        if route.stopped {
            return Err("subagent workspace is sealed after SubagentStop".to_owned());
        }
        let original =
            normalize_tool_input(input.get("tool_input").cloned().unwrap_or(Value::Null))?;
        let mut updated =
            rewrite_tool_input(&tool_name, original, &self.state.root_path, &route.path)?;
        let acyclic_git = (matches!(tool_name.as_str(), "Bash" | "exec_command")
            || tool_name.ends_with("__exec_command"))
        .then(|| acyclic_git_argv(&updated))
        .transpose()?
        .flatten();
        if let Some(git_argv) = acyclic_git {
            inject_acyclic_workspace_context(
                &mut updated,
                self.control_endpoint.as_deref().ok_or_else(|| {
                    "acyclic git is unavailable because the local control endpoint is not configured"
                        .to_owned()
                })?,
                self.cli_bin.as_deref().ok_or_else(|| {
                    "acyclic git is unavailable because the packaged CLI directory is not configured"
                        .to_owned()
                })?,
                &route,
                &git_argv,
            )?;
            self.state.ipc_tools.insert(
                tool_use_id,
                IpcToolRecord {
                    agent_id,
                    turn_id,
                    tool_name,
                    expires_at_millis: now_millis().saturating_add(15 * 60 * 1_000),
                },
            );
            self.persist()?;
            return Ok(pre_tool_update(updated));
        }
        require_process_sandbox(&tool_name)?;
        if tool_name.starts_with("mcp__acyclic_agent_workspaces__") {
            let object = updated
                .as_object_mut()
                .ok_or_else(|| "workspace control tool input must be an object".to_owned())?;
            object.insert("_caller_turn_id".to_owned(), Value::String(turn_id.clone()));
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
        self.state.leases.insert(
            tool_use_id,
            LeaseRecord::from_lease(agent_id, turn_id, tool_name, &lease),
        );
        self.persist()?;
        Ok(pre_tool_update(updated))
    }

    async fn git_tool(
        &mut self,
        agent_id: &str,
        route: &Route,
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
            self.unmount_agent(agent_id).await?;
            let mount = match switched
                .mount(&route.path, MountOptions::read_write())
                .await
            {
                Ok(mount) => mount,
                Err(error) => {
                    let restored = self
                        .workspace(route)
                        .await?
                        .mount(&route.path, MountOptions::read_write())
                        .await
                        .map_err(|restore| {
                            format!(
                                "cannot mount switched workspace ({error}); cannot restore previous mount ({restore})"
                            )
                        })?;
                    self.mounts.insert(agent_id.to_owned(), restored);
                    return Err(format!("cannot mount switched workspace: {error}"));
                }
            };
            let current = self
                .state
                .routes
                .get_mut(agent_id)
                .ok_or_else(|| "subagent route is missing".to_owned())?;
            current.workspace_name = switched.name().as_str().to_owned();
            current.workspace_id = switched.id().into_bytes();
            current.published_generation = [0; 32];
            current.route_epoch = current.route_epoch.saturating_add(1);
            current.workspace_token = new_workspace_token()?;
            self.mounts.insert(agent_id.to_owned(), mount);
        }
        let Some(output) = command.map_err(display)? else {
            self.persist()?;
            return Err("recovered a pending Git transition; retry the current command".to_owned());
        };
        self.persist()?;
        serde_json::to_value(output).map_err(display)
    }

    async fn subagent_start(&mut self, input: Value) -> Result<Value, String> {
        self.require_session(&input)?;
        let agent_id = string(&input, "agent_id")?;
        let turn_id = string(&input, "turn_id")?;
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
            let route = self.state.routes.get(&agent_id).ok_or("route missing")?;
            if route.stopped {
                return Err("stopped subagent identity cannot be started again".to_owned());
            }
            if route.turn_id != turn_id {
                return Err("subagent identity is already bound to another turn".to_owned());
            }
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
            stopped: false,
            published_generation: [0; 32],
            route_epoch: 1,
            workspace_token: new_workspace_token()?,
        };
        self.mounts.insert(agent_id.clone(), mount);
        self.state.turns.insert(turn_id, agent_id.clone());
        self.state.routes.insert(agent_id, route);
        self.persist()?;
        Ok(subagent_context(&path))
    }

    async fn post_tool(&mut self, input: Value) -> Result<Value, String> {
        self.require_session(&input)?;
        let turn_id = string(&input, "turn_id")?;
        let tool_name = string(&input, "tool_name")?;
        let caller = self.resolve_turn(&turn_id)?;
        let tool_use_id = string(&input, "tool_use_id")?;
        if let Some(record) = self.state.ipc_tools.get(&tool_use_id).cloned() {
            if record.agent_id != caller
                || record.turn_id != turn_id
                || record.tool_name != tool_name
            {
                return Err("post-tool hook does not match its acyclic-git owner".to_owned());
            }
            self.state.ipc_tools.remove(&tool_use_id);
            self.persist()?;
            return Ok(json!({}));
        }
        let Some(record) = self.state.leases.get(&tool_use_id).cloned() else {
            if caller != self.state.root_agent_id
                && !is_pure_remote_tool(&tool_name)
                && !is_filesystem_tool(&tool_name)
                && !matches!(tool_name.as_str(), "spawn_agent" | "Agent")
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
            self.state.leases.remove(&tool_use_id);
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
        self.state.leases.remove(&tool_use_id);
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
        self.require_session(&input)?;
        self.recover_expired_adapter_leases().await?;
        self.prune_expired_ipc_tools()?;
        let agent_id = string(&input, "agent_id")?;
        let turn_id = string(&input, "turn_id")?;
        if self.resolve_turn(&turn_id)? != agent_id {
            return Err("subagent stop does not belong to the routed agent turn".to_owned());
        }
        let route = self
            .state
            .routes
            .get(&agent_id)
            .ok_or_else(|| "subagent stop refers to an unknown agent".to_owned())?;
        if route.stopped {
            return Ok(json!({"suppressOutput": true}));
        }
        if self
            .state
            .leases
            .values()
            .any(|lease| lease.agent_id == agent_id)
            || self
                .state
                .ipc_tools
                .values()
                .any(|tool| tool.agent_id == agent_id)
        {
            return Err("subagent cannot stop while filesystem tools are still active".to_owned());
        }
        self.unmount_agent(&agent_id).await?;
        self.state
            .routes
            .get_mut(&agent_id)
            .ok_or_else(|| "subagent route disappeared during stop".to_owned())?
            .stopped = true;
        let route = self
            .state
            .routes
            .get_mut(&agent_id)
            .ok_or_else(|| "subagent route disappeared during stop".to_owned())?;
        route.route_epoch = route.route_epoch.saturating_add(1);
        route.workspace_token = [0; 32];
        self.persist()?;
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
        let graph = WorkspaceGraph::new(self.store.clone());
        let repository_id = if route.repository_workspace_id == [0; 16] {
            workspace.id()
        } else {
            acyclic_fs::WorkspaceId::from_bytes(route.repository_workspace_id)
        };
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
            let parent_name = lineage
                .parent_workspace_name
                .ok_or_else(|| "Git compatibility branch parent name is unavailable".to_owned())?;
            let parent = self
                .fs
                .open_workspace(&parent_name)
                .await
                .map_err(display)?;
            if parent.id() != parent_id {
                return Err("Git compatibility branch parent identity changed".to_owned());
            }
            graph
                .authorize_join(lineage_cursor.id(), parent.id())
                .await
                .map_err(display)?;
            lineage_cursor = parent;
        }
        let lineage = graph
            .authorize_join(
                lineage_cursor.id(),
                self.parent_workspace(&route).await?.id(),
            )
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
            let parent_id = lineage.parent_workspace_id.ok_or_else(|| {
                "Git compatibility branch does not reach its repository workspace".to_owned()
            })?;
            let parent_name = lineage
                .parent_workspace_name
                .ok_or_else(|| "Git compatibility branch parent name is unavailable".to_owned())?;
            let parent = self
                .fs
                .open_workspace(&parent_name)
                .await
                .map_err(display)?;
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
        if route.published_generation != [0; 32] {
            return self
                .agent_merge_incremental(&agent, &caller, &route, &source, &target)
                .await;
        }
        let plan = source.join_into(&target).plan().await.map_err(display)?;
        let target_head = plan.target_head();
        let source_head = plan.source_head();
        if caller == self.state.root_agent_id {
            self.begin_root_publication(target_head, &agent, source_head)?;
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
        if matches!(
            outcome,
            JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) | JoinOutcome::NoChanges(_)
        ) && caller != self.state.root_agent_id
        {
            self.state
                .routes
                .get_mut(&agent)
                .ok_or_else(|| "merged agent route disappeared".to_owned())?
                .published_generation = *source_head.digest().as_bytes();
            self.persist()?;
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

    async fn agent_merge_incremental(
        &mut self,
        agent: &str,
        caller: &str,
        route: &Route,
        source: &LocalWorkspace,
        target: &LocalWorkspace,
    ) -> Result<Value, String> {
        let base_id = acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(
            route.published_generation,
        ));
        let base = source.generation(base_id).await.map_err(display)?;
        let head = source.head().await.map_err(display)?;
        let changes = base.diff_to(&head, 100_000).await.map_err(display)?;
        let paths = changes
            .changed_paths(100_000)
            .await
            .map_err(display)?
            .iter()
            .map(|change| namespace_path_text(&change.path))
            .collect::<Result<Vec<_>, _>>()?;
        let target_head = target.head().await.map_err(display)?.id();
        if paths.is_empty() {
            self.state
                .routes
                .get_mut(agent)
                .ok_or_else(|| "merged agent route disappeared".to_owned())?
                .published_generation = *head.id().digest().as_bytes();
            self.persist()?;
            return Ok(json!({
                "status":"no-changes",
                "generation":hex::encode(target_head.digest().as_bytes())
            }));
        }
        if caller == self.state.root_agent_id {
            self.begin_root_publication(target_head, agent, head.id())?;
        }
        let apply_key = self
            .state
            .root_publication
            .as_ref()
            .filter(|_| caller == self.state.root_agent_id)
            .map_or_else(IdempotencyKey::new, |publication| {
                IdempotencyKey::from_bytes(publication.operation_id)
            });
        let outcome = target
            .apply_paths_from(Some(&base), Some(&head), &paths, target_head, apply_key)
            .await;
        if caller == self.state.root_agent_id && outcome.is_err() {
            self.recover_root_publication().await?;
        }
        let outcome = outcome.map_err(display)?;
        let published = match &outcome {
            WorkspacePathApply::Applied(generation)
            | WorkspacePathApply::AlreadyApplied(generation)
            | WorkspacePathApply::NoChanges(generation) => Some(generation.clone()),
            _ => None,
        };
        if caller == self.state.root_agent_id
            && let Some(generation) = published.as_ref()
        {
            self.publish_root_generation(target_head, generation)
                .await?;
        } else if caller == self.state.root_agent_id {
            self.state.root_publication = None;
            self.persist()?;
        } else if published.is_some() {
            self.state
                .routes
                .get_mut(agent)
                .ok_or_else(|| "merged agent route disappeared".to_owned())?
                .published_generation = *head.id().digest().as_bytes();
            self.persist()?;
        }
        if published.is_some()
            && let Some(parent_mount) = self.mounts.get(caller)
        {
            parent_mount.advance_to_head().await.map_err(display)?;
        }
        Ok(match outcome {
            WorkspacePathApply::Applied(generation) => {
                json!({"status":"applied","generation":hex::encode(generation.id().digest().as_bytes())})
            }
            WorkspacePathApply::AlreadyApplied(generation) => {
                json!({"status":"already-applied","generation":hex::encode(generation.id().digest().as_bytes())})
            }
            WorkspacePathApply::NoChanges(generation) => {
                json!({"status":"no-changes","generation":hex::encode(generation.id().digest().as_bytes())})
            }
            WorkspacePathApply::Stale(generation) => {
                json!({"status":"stale-target","generation":hex::encode(generation.id().digest().as_bytes())})
            }
            WorkspacePathApply::Conflicted(conflicts) => json!({
                "status":"conflicted",
                "conflictCount":conflicts.len(),
                "conflicts": conflicts.iter().map(|conflict| json!({
                    "path": conflict.path,
                    "kind": format!("{:?}", conflict.kind),
                })).collect::<Vec<_>>(),
                "truncated":false
            }),
            WorkspacePathApply::Fenced => json!({"status":"fenced"}),
            WorkspacePathApply::IdempotencyConflict => {
                json!({"status":"idempotency-conflict"})
            }
        })
    }

    async fn agent_discard(&mut self, input: Value) -> Result<Value, String> {
        self.recover_expired_adapter_leases().await?;
        self.prune_expired_ipc_tools()?;
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
            || self
                .state
                .ipc_tools
                .values()
                .any(|tool| subtree.contains(&tool.agent_id))
        {
            return Err(
                "cannot discard an agent subtree while filesystem tools are active".to_owned(),
            );
        }
        self.state
            .pending
            .retain(|spawn| !subtree.contains(&spawn.parent_agent_id));
        if !self.state.pending_discards.contains_key(&agent) {
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
                }
                let mut workspaces = Vec::new();
                for workspace_id in workspace_ids.into_iter().rev() {
                    let name = if workspace_id
                        == acyclic_fs::WorkspaceId::from_bytes(removed.workspace_id)
                    {
                        removed.workspace_name.clone()
                    } else {
                        WorkspaceGraph::new(self.store.clone())
                            .resolve(workspace_id)
                            .await
                            .map_err(|error| {
                                format!("cannot resolve discarded workspace lineage: {error}")
                            })?
                            .workspace_name
                    };
                    workspaces.push(DiscardWorkspace {
                        name,
                        delete_key: IdempotencyKey::new().into_bytes(),
                    });
                }
                discard_agents.push(DiscardAgent {
                    agent_id: descendant,
                    path: removed.path,
                    repository_workspace_id: repository_id.into_bytes(),
                    mount_detached: false,
                    workspaces,
                });
            }
            for member in &subtree {
                let route = self
                    .state
                    .routes
                    .get_mut(member)
                    .ok_or_else(|| "discard subtree route disappeared".to_owned())?;
                route.stopped = true;
                route.route_epoch = route.route_epoch.saturating_add(1);
                route.workspace_token = [0; 32];
            }
            self.state.pending_discards.insert(
                agent.clone(),
                PendingDiscard {
                    agents: discard_agents,
                },
            );
            self.persist()?;
        }
        self.continue_pending_discard(&agent).await?;
        Ok(json!({"status":"discarded","agent":agent}))
    }

    async fn recover_pending_discards(&mut self) -> Result<(), String> {
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

    async fn continue_pending_discard(&mut self, root: &str) -> Result<(), String> {
        while let Some(agent) = self
            .state
            .pending_discards
            .get(root)
            .and_then(|discard| discard.agents.first())
            .cloned()
        {
            if !agent.mount_detached {
                self.unmount_agent(&agent.agent_id).await?;
                self.state
                    .pending_discards
                    .get_mut(root)
                    .expect("pending discard exists")
                    .agents[0]
                    .mount_detached = true;
                self.persist()?;
            }
            remove_tree_checked(&self.data.join("workspaces"), &agent.path)
                .map_err(|error| format!("cannot remove discarded mount path: {error}"))?;
            let repository_id = acyclic_fs::WorkspaceId::from_bytes(agent.repository_workspace_id);
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
                return Err("Git compatibility state changed while discarding the agent".to_owned());
            }
            while let Some(workspace) = self
                .state
                .pending_discards
                .get(root)
                .and_then(|discard| discard.agents.first())
                .and_then(|agent| agent.workspaces.first())
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
                    WorkspaceDelete::Deleted | WorkspaceDelete::AlreadyDeleted => {}
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
                    .expect("pending discard exists")
                    .agents[0]
                    .workspaces
                    .remove(0);
                self.persist()?;
            }
            self.state.routes.remove(&agent.agent_id);
            self.state.turns.retain(|_, value| value != &agent.agent_id);
            self.state
                .pending_discards
                .get_mut(root)
                .expect("pending discard exists")
                .agents
                .remove(0);
            self.persist()?;
        }
        self.state.pending_discards.remove(root);
        self.persist()
    }

    async fn unmount_agent(&mut self, agent_id: &str) -> Result<(), String> {
        #[cfg(test)]
        if self.fail_next_unmount.remove(agent_id) {
            return Err("injected mount teardown failure".to_owned());
        }
        if let Some(mount) = self.mounts.get(agent_id) {
            mount.sync().await.map_err(display)?;
            mount.unmount().await.map_err(display)?;
            self.mounts.remove(agent_id);
        }
        Ok(())
    }

    fn caller(&self, input: &Value) -> Result<String, String> {
        let turn = input
            .get("_caller_turn_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "workspace control call lacks a stable caller identity".to_owned())?;
        self.resolve_turn(turn)
            .map_err(|_| "workspace control caller is unknown".to_owned())
    }

    fn require_session(&self, input: &Value) -> Result<(), String> {
        let session_id = string(input, "session_id")?;
        if session_id != self.state.root_session_id {
            return Err("lifecycle event belongs to another root session".to_owned());
        }
        Ok(())
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

    fn prune_expired_ipc_tools(&mut self) -> Result<(), String> {
        let before = self.state.ipc_tools.len();
        let now = now_millis();
        self.state
            .ipc_tools
            .retain(|_, tool| tool.expires_at_millis > now);
        if self.state.ipc_tools.len() == before {
            Ok(())
        } else {
            self.persist()
        }
    }

    fn rotate_route_capabilities(&mut self) -> Result<(), String> {
        let had_ipc_tools = !self.state.ipc_tools.is_empty();
        self.state.ipc_tools.clear();
        if self.state.routes.is_empty() {
            return if had_ipc_tools {
                self.persist()
            } else {
                Ok(())
            };
        }
        for route in self.state.routes.values_mut() {
            route.route_epoch = route.route_epoch.saturating_add(1).max(1);
            route.workspace_token = if route.stopped {
                [0; 32]
            } else {
                new_workspace_token()?
            };
        }
        self.persist()
    }

    async fn shutdown(&mut self) -> Result<(), String> {
        let mounts = std::mem::take(&mut self.mounts);
        let mut first_error = None;
        for (agent_id, mount) in mounts {
            if let Err(error) = mount.sync().await {
                first_error.get_or_insert_with(|| {
                    format!("cannot synchronize agent '{agent_id}' during shutdown: {error}")
                });
            }
            if let Err(error) = mount.unmount().await {
                first_error.get_or_insert_with(|| {
                    format!("cannot unmount agent '{agent_id}' during shutdown: {error}")
                });
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn begin_root_publication(
        &mut self,
        from: acyclic_fs::GenerationId,
        source_agent_id: &str,
        source_generation: acyclic_fs::GenerationId,
    ) -> Result<(), String> {
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
            source_agent_id: source_agent_id.to_owned(),
            source_generation: *source_generation.digest().as_bytes(),
        });
        self.persist()
    }

    async fn publish_root_generation(
        &mut self,
        from: acyclic_fs::GenerationId,
        to: &acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>,
    ) -> Result<(), String> {
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
        if workspace.source().is_none() {
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
        let attached = self
            .root
            .clone()
            .ok_or_else(|| "root source is unavailable after publication".to_owned())?;
        let attached_head = attached.head().await.map_err(display)?;
        if attached_head.id() != to_id {
            let target = attached.generation(to_id).await.map_err(display)?;
            let mut recovery_key = operation_id.into_bytes();
            recovery_key[0] ^= 0x80;
            match attached
                .restore_generation(
                    &target,
                    attached_head.id(),
                    IdempotencyKey::from_bytes(recovery_key),
                )
                .await
                .map_err(display)?
            {
                WorkspaceRestore::Restored(_)
                | WorkspaceRestore::AlreadyRestored(_)
                | WorkspaceRestore::Current(_) => {}
                WorkspaceRestore::Stale(_) => {
                    return Err(
                        "root changed while restoring its interrupted publication".to_owned()
                    );
                }
                WorkspaceRestore::Fenced => {
                    return Err("interrupted root publication restore was fenced".to_owned());
                }
                WorkspaceRestore::IdempotencyConflict => {
                    return Err(
                        "interrupted root publication restore identity was reused".to_owned()
                    );
                }
            }
        }
        let source = attached
            .source()
            .ok_or_else(|| "root source handle is unavailable after publication".to_owned())?;
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
        self.store
            .remove_materialization(operation_id)
            .map_err(display)?;
        remove_tree_checked(
            &self.state.root_path.join(".acyclic-sdk/materializations"),
            &publication.operation_directory,
        )?;
        if !publication.source_agent_id.is_empty()
            && let Some(route) = self.state.routes.get_mut(&publication.source_agent_id)
        {
            route.published_generation = publication.source_generation;
        }
        self.state.root_publication = None;
        self.persist()
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

fn acyclic_git_argv(input: &Value) -> Result<Option<Vec<String>>, String> {
    let command = input
        .get("cmd")
        .or_else(|| input.get("command"))
        .and_then(Value::as_str)
        .ok_or_else(|| "shell tool input lacks a string command".to_owned())?;
    let argv = split_standalone_command(command)?;
    let is_acyclic = argv.first().is_some_and(|program| {
        program.eq_ignore_ascii_case("acyclic") || program.eq_ignore_ascii_case("acyclic.exe")
    });
    if !is_acyclic || argv.get(1).is_none_or(|command| command != "git") {
        return Ok(None);
    }
    if argv.len() == 2 {
        return Err("acyclic git is missing a Git-style subcommand".to_owned());
    }
    Ok(Some(argv[2..].to_vec()))
}

fn inject_acyclic_workspace_context(
    input: &mut Value,
    endpoint: &str,
    cli_bin: &Path,
    route: &Route,
    git_argv: &[String],
) -> Result<(), String> {
    if route.workspace_token == [0; 32] || route.stopped {
        return Err("acyclic workspace capability is revoked".to_owned());
    }
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(route.workspace_token);
    #[cfg(target_os = "windows")]
    let scoped = {
        let executable = cli_bin
            .join("acyclic.exe")
            .to_string_lossy()
            .replace('\'', "''");
        let arguments = git_argv
            .iter()
            .map(|argument| format!("'{}'", argument.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "$env:ACYCLIC_CONTEXT_VERSION='1';$env:ACYCLIC_CONTROL_ENDPOINT='{}';$env:ACYCLIC_WORKSPACE_TOKEN='{}';& '{executable}' 'git' {arguments}",
            endpoint.replace('\'', "''"),
            token.replace('\'', "''")
        )
    };
    #[cfg(not(target_os = "windows"))]
    let scoped = {
        let quote = |value: &str| format!("'{}'", value.replace('\'', "'\"'\"'"));
        let executable = quote(&cli_bin.join("acyclic").to_string_lossy());
        let arguments = git_argv
            .iter()
            .map(|argument| quote(argument))
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "ACYCLIC_CONTEXT_VERSION='1' ACYCLIC_CONTROL_ENDPOINT='{}' ACYCLIC_WORKSPACE_TOKEN='{}' {executable} 'git' {arguments}",
            endpoint.replace('\'', "'\"'\"'"),
            token.replace('\'', "'\"'\"'")
        )
    };
    set_shell_command(input, scoped)
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
    if !(matches!(tool_name, "Bash" | "exec_command") || tool_name.ends_with("__exec_command")) {
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
                "Your filesystem is an isolated Acyclic workspace mounted at {}. Tool paths are redirected automatically; do not access the parent checkout by a hard-coded path. For optional local Git-shaped operations use the authenticated workspace CLI, for example: `acyclic git status`, `acyclic git diff --cached`, `acyclic git commit -m \"msg\"`, or `acyclic git switch -c branch`. This is an ergonomic facade over this Acyclic workspace, not system Git; transport and object-database commands are unsupported. Ordinary `git` remains system Git. Your parent can inspect your unpublished changes with agent_changes, publish repeated incremental updates with agent_merge, or recursively discard your workspace and every unpublished descendant with agent_discard. Merge and discard are authorized only for the direct parent, so publish descendants into you before asking your parent to publish you.",
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
    let parent = target
        .parent()
        .ok_or_else(|| "discard target has no parent".to_owned())?
        .canonicalize()
        .map_err(display)?;
    if parent != root {
        return Err("refusing to discard a path outside the plugin workspace root".to_owned());
    }
    if !target.exists() {
        return Ok(());
    }
    let target = match target.canonicalize() {
        Ok(target) => target,
        #[cfg(target_os = "windows")]
        Err(error) if error.raw_os_error() == Some(369) => return Ok(()),
        Err(error) => return Err(display(error)),
    };
    if target.parent() != Some(root.as_path()) {
        return Err("refusing to discard a path outside the plugin workspace root".to_owned());
    }
    match fs::remove_dir_all(target) {
        Ok(()) => Ok(()),
        #[cfg(target_os = "windows")]
        Err(error) if error.raw_os_error() == Some(369) => Ok(()),
        Err(error) => Err(display(error)),
    }
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

fn namespace_path_text(path: &acyclic_fs::kernel::NamespacePath) -> Result<String, String> {
    let mut text = String::new();
    for component in path.components() {
        text.push('/');
        match component.encoding() {
            acyclic_fs::kernel::NameEncoding::Utf8 => text.push_str(
                std::str::from_utf8(component.as_bytes())
                    .map_err(|_| "changed path is not valid UTF-8".to_owned())?,
            ),
            acyclic_fs::kernel::NameEncoding::WindowsUtf16Le => {
                let bytes = component.as_bytes();
                if !bytes.len().is_multiple_of(2) {
                    return Err("changed Windows path has invalid UTF-16 bytes".to_owned());
                }
                let units = bytes
                    .chunks_exact(2)
                    .map(|unit| u16::from_le_bytes([unit[0], unit[1]]));
                let component = char::decode_utf16(units)
                    .collect::<Result<String, _>>()
                    .map_err(|_| "changed Windows path is not valid UTF-16".to_owned())?;
                text.push_str(&component);
            }
            acyclic_fs::kernel::NameEncoding::PosixBytes => {
                return Err(
                    "incremental publication cannot represent a non-UTF-8 POSIX path".to_owned(),
                );
            }
        }
    }
    if text.is_empty() {
        text.push('/');
    }
    Ok(text)
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn new_workspace_token() -> Result<[u8; 32], String> {
    let mut token = [0_u8; 32];
    getrandom::fill(&mut token)
        .map_err(|error| format!("operating-system workspace token generation failed: {error}"))?;
    Ok(token)
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn public_tools() -> Value {
    json!([
        {"name":"agent_changes","description":"Inspect an agent workspace without publishing it. The root may inspect any descendant; a subagent may inspect itself or its descendants. Returns bounded change counts and generation identity; path is an optional presentation hint.","inputSchema":{"type":"object","properties":{"agent":{"type":"string","description":"Agent identifier to inspect."},"path":{"type":"string","description":"Optional path hint for the inspection UI; it does not change authorization or publication."}},"required":["agent"],"additionalProperties":false}},
        {"name":"agent_merge","description":"Publish the current workspace of one direct child into the caller's workspace. Only that child's direct parent is authorized. The child remains available, so call again to publish later incremental changes. Descendants must first be merged into their own direct parent. Reports applied, no-changes, stale, fenced, or typed-conflict outcomes without bypassing Acyclic filesystem join semantics.","inputSchema":{"type":"object","properties":{"agent":{"type":"string","description":"Direct child agent to publish."}},"required":["agent"],"additionalProperties":false}},
        {"name":"agent_discard","description":"Permanently discard one direct child's unpublished workspace and recursively discard all of its unpublished descendants. Only the direct parent is authorized; inspect or merge desired work first.","inputSchema":{"type":"object","properties":{"agent":{"type":"string","description":"Direct child agent whose subtree should be discarded."}},"required":["agent"],"additionalProperties":false}}
    ])
}

const MAXIMUM_CONTROL_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Deserialize)]
struct ControlRequest {
    version: u32,
    token: String,
    command: String,
    argv: Vec<String>,
}

struct ControlEndpoint {
    endpoint: String,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), String>>,
    #[cfg(test)]
    accepted: Arc<tokio::sync::Notify>,
    #[cfg(unix)]
    socket_path: PathBuf,
    #[cfg(all(windows, test))]
    pipe_path: String,
}

impl ControlEndpoint {
    async fn shutdown(self) -> Result<(), String> {
        let _ = self.shutdown.send(true);
        let result = self.task.await.map_err(display)?;
        #[cfg(unix)]
        match fs::remove_file(&self.socket_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(display(error)),
        }
        result
    }
}

async fn start_control_endpoint(
    control: Arc<AsyncMutex<ControlPlane>>,
) -> Result<ControlEndpoint, String> {
    let opaque_id = hex::encode(OperationId::new().into_bytes());
    let (shutdown, receiver) = watch::channel(false);
    #[cfg(test)]
    let accepted = Arc::new(tokio::sync::Notify::new());

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let socket_path = env::temp_dir().join(format!("acyclic-{opaque_id}.sock"));
        let listener = tokio::net::UnixListener::bind(&socket_path).map_err(display)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).map_err(display)?;
        let endpoint = format!("unix://{}", socket_path.display());
        let task = tokio::spawn(serve_unix_control(
            listener,
            control,
            shutdown.clone(),
            receiver,
            #[cfg(test)]
            Arc::clone(&accepted),
        ));
        Ok(ControlEndpoint {
            endpoint,
            shutdown,
            task,
            #[cfg(test)]
            accepted,
            socket_path,
        })
    }

    #[cfg(windows)]
    {
        let opaque_name = format!("acyclic-agent-workspaces-{opaque_id}");
        let endpoint = format!("npipe://./pipe/{opaque_name}");
        let pipe_path = format!(r"\\.\pipe\{opaque_name}");
        let task = tokio::spawn(serve_windows_control(
            pipe_path.clone(),
            control,
            shutdown.clone(),
            receiver,
            #[cfg(test)]
            Arc::clone(&accepted),
        ));
        Ok(ControlEndpoint {
            endpoint,
            shutdown,
            task,
            #[cfg(test)]
            accepted,
            #[cfg(test)]
            pipe_path,
        })
    }
}

#[cfg(unix)]
async fn serve_unix_control(
    listener: tokio::net::UnixListener,
    control: Arc<AsyncMutex<ControlPlane>>,
    shutdown_sender: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
    #[cfg(test)] accepted: Arc<tokio::sync::Notify>,
) -> Result<(), String> {
    let mut connections = tokio::task::JoinSet::new();
    let result = loop {
        tokio::select! {
            incoming = listener.accept() => {
                let (stream, _) = match incoming {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(display(error)),
                };
                let control = Arc::clone(&control);
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    let _ = handle_control_connection(stream, control, connection_shutdown).await;
                });
                #[cfg(test)]
                accepted.notify_one();
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                let _ = completed;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break Ok(());
                }
            }
        }
    };
    let _ = shutdown_sender.send(true);
    while connections.join_next().await.is_some() {}
    result
}

#[cfg(windows)]
async fn serve_windows_control(
    pipe_path: String,
    control: Arc<AsyncMutex<ControlPlane>>,
    shutdown_sender: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
    #[cfg(test)] accepted: Arc<tokio::sync::Notify>,
) -> Result<(), String> {
    let mut first = true;
    let mut connections = tokio::task::JoinSet::new();
    let result = loop {
        let server = tokio::net::windows::named_pipe::ServerOptions::new()
            .first_pipe_instance(first)
            .reject_remote_clients(true)
            .create(&pipe_path);
        let server = match server {
            Ok(server) => server,
            Err(error) => break Err(display(error)),
        };
        first = false;
        tokio::select! {
            connected = server.connect() => {
                if let Err(error) = connected {
                    break Err(display(error));
                }
                let control = Arc::clone(&control);
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    let _ = handle_control_connection(server, control, connection_shutdown).await;
                });
                #[cfg(test)]
                accepted.notify_one();
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                let _ = completed;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break Ok(());
                }
            }
        }
    };
    let _ = shutdown_sender.send(true);
    while connections.join_next().await.is_some() {}
    result
}

async fn handle_control_connection<S>(
    mut stream: S,
    control: Arc<AsyncMutex<ControlPlane>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut request = Vec::new();
    let read = async {
        let reader = BufReader::new(&mut stream);
        let mut bounded = reader.take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64);
        bounded
            .read_until(b'\n', &mut request)
            .await
            .map_err(display)
    };
    tokio::select! {
        read = read => {
            read?;
        }
        changed = shutdown.changed() => {
            let _ = changed;
            return Ok(());
        }
    }
    let response = if request.len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
        control_response(Err(
            "Acyclic control request exceeds the 4 MiB bound".to_owned()
        ))
    } else if request.last() != Some(&b'\n') {
        control_response(Err(
            "Acyclic control request must end with a newline".to_owned()
        ))
    } else {
        request.pop();
        match serde_json::from_slice::<ControlRequest>(&request) {
            Ok(request) => control_response(dispatch_control_request(&control, request).await),
            Err(error) => {
                control_response(Err(format!("invalid Acyclic control request: {error}")))
            }
        }
    };
    let encoded = encode_control_response(&response)?;
    stream.write_all(&encoded).await.map_err(display)?;
    stream.write_all(b"\n").await.map_err(display)?;
    stream.flush().await.map_err(display)
}

async fn dispatch_control_request(
    control: &Arc<AsyncMutex<ControlPlane>>,
    request: ControlRequest,
) -> Result<Value, String> {
    if request.version != 1 || request.command != "git" || request.argv.is_empty() {
        return Err("unsupported Acyclic control request".to_owned());
    }
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(request.token.as_bytes())
        .map_err(|_| "invalid Acyclic workspace capability".to_owned())?;
    let token: [u8; 32] = token
        .try_into()
        .map_err(|_| "invalid Acyclic workspace capability".to_owned())?;
    let mut control = control.lock().await;
    control.prune_expired_ipc_tools()?;
    let route = control
        .state
        .routes
        .values()
        .find(|route| !route.stopped && constant_time_equal(&route.workspace_token, &token))
        .cloned()
        .ok_or_else(|| "invalid or revoked Acyclic workspace capability".to_owned())?;
    if !control
        .state
        .ipc_tools
        .values()
        .any(|tool| tool.agent_id == route.agent_id)
    {
        return Err("Acyclic workspace capability is not active for a hooked command".to_owned());
    }
    control
        .git_tool(&route.agent_id, &route, request.argv)
        .await
}

fn constant_time_equal(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn control_response(result: Result<Value, String>) -> Value {
    match result {
        Ok(result) => json!({"version":1,"ok":true,"result":result}),
        Err(error) => json!({"version":1,"ok":false,"error":error}),
    }
}

struct BoundedJsonBuffer {
    bytes: Vec<u8>,
}

impl BoundedJsonBuffer {
    fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(8 * 1024),
        }
    }
}

impl Write for BoundedJsonBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.bytes.len().saturating_add(buffer.len()) >= MAXIMUM_CONTROL_MESSAGE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "Acyclic control response exceeds the 4 MiB bound",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_control_response(response: &Value) -> Result<Vec<u8>, String> {
    let mut bounded = BoundedJsonBuffer::new();
    if serde_json::to_writer(&mut bounded, response).is_ok() {
        return Ok(bounded.bytes);
    }
    serde_json::to_vec(&control_response(Err(
        "Acyclic control response exceeds the 4 MiB bound".to_owned(),
    )))
    .map_err(display)
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
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_rpc(data, stdin.lock(), stdout.lock())
        .await
        .map_err(io::Error::other)?;
    Ok(())
}

async fn run_rpc(
    data: PathBuf,
    reader: impl BufRead,
    mut writer: impl Write,
) -> Result<(), String> {
    let control = Arc::new(AsyncMutex::new(ControlPlane::open(data).await?));
    let endpoint = start_control_endpoint(Arc::clone(&control)).await?;
    control.lock().await.control_endpoint = Some(endpoint.endpoint.clone());
    let service = async {
        for line in reader.lines() {
            let line = line.map_err(display)?;
            if line.trim().is_empty() {
                continue;
            }
            let request: Value = serde_json::from_str(&line).map_err(display)?;
            let mut locked = control.lock().await;
            let Some(response) = rpc_response(&mut locked, &request).await? else {
                continue;
            };
            drop(locked);
            serde_json::to_writer(&mut writer, &response).map_err(display)?;
            writer.write_all(b"\n").map_err(display)?;
            writer.flush().map_err(display)?;
        }
        Ok(())
    }
    .await;
    let endpoint_shutdown = endpoint.shutdown().await;
    let mut control = Arc::try_unwrap(control)
        .map_err(|_| "control endpoint retained an active request during shutdown".to_owned())?
        .into_inner();
    let control_shutdown = control.shutdown().await;
    // LocalFs owns the exclusive Stream journal. Release it before reporting
    // graceful EOF so a replacement host can reopen the same plugin data.
    drop(control);
    service.and(endpoint_shutdown).and(control_shutdown)
}

async fn rpc_response(
    control: &mut ControlPlane,
    request: &Value,
) -> Result<Option<Value>, String> {
    let Some(id) = request.get("id").cloned() else {
        return Ok(None);
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
                    json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":serde_json::to_string(&result).map_err(display)?}]}})
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
    Ok(Some(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::GitCommand;

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
    fn lifecycle_edges_recover_and_fail_closed() {
        std::thread::Builder::new()
            .name("plugin-lifecycle-e2e".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(lifecycle_hardening_case());
            })
            .expect("test thread")
            .join()
            .expect("plugin lifecycle e2e thread");
    }

    #[test]
    fn json_rpc_host_simulates_lifecycle_and_graceful_shutdown() {
        std::thread::Builder::new()
            .name("plugin-rpc-host".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(json_rpc_host_case());
            })
            .expect("test thread")
            .join()
            .expect("plugin RPC host thread");
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn endpoint_shutdown_cancels_inflight_requests_before_reopen() {
        std::thread::Builder::new()
            .name("plugin-endpoint-shutdown".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(endpoint_shutdown_case());
            })
            .expect("test thread")
            .join()
            .expect("plugin endpoint shutdown thread");
    }

    #[test]
    fn authenticated_control_protocol_dispatches_git() {
        std::thread::Builder::new()
            .name("plugin-control-protocol".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(authenticated_control_protocol_case());
            })
            .expect("test thread")
            .join()
            .expect("plugin control protocol thread");
    }

    async fn json_rpc_host_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root directory");
        let requests = [
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
            json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"_hook_session_start","arguments":{"session_id":"session","cwd":root.display().to_string()}}}),
            json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"_hook_user_prompt","arguments":{"session_id":"session","turn_id":"root-turn"}}}),
            json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"_hook_pre_tool","arguments":{"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn-child","tool_name":"spawn_agent","tool_input":{}}}}),
            json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"_hook_subagent_start","arguments":{"session_id":"session","turn_id":"child-turn","agent_id":"child","agent_type":"explorer"}}}),
        ];
        let input = requests
            .iter()
            .map(|request| serde_json::to_string(request).expect("serialize request"))
            .collect::<Vec<_>>()
            .join("\n");
        let mut output = Vec::new();
        run_rpc(
            temporary.path().join("plugin-data"),
            io::Cursor::new(input.into_bytes()),
            &mut output,
        )
        .await
        .expect("run deterministic RPC host");
        let responses = String::from_utf8(output)
            .expect("UTF-8 responses")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("JSON-RPC response"))
            .collect::<Vec<_>>();
        assert_eq!(responses.len(), requests.len());
        assert!(
            responses
                .iter()
                .all(|response| response.get("error").is_none())
        );
        let descriptions = responses[1]["result"]["tools"]
            .as_array()
            .expect("public tools")
            .iter()
            .filter_map(|tool| tool["description"].as_str())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(descriptions.contains("direct parent"));
        assert!(descriptions.contains("incremental"));

        let mut reopened = ControlPlane::open(temporary.path().join("plugin-data"))
            .await
            .expect("reopen after graceful RPC EOF");
        assert!(reopened.mounts.contains_key("child"));
        reopened.shutdown().await.expect("second graceful shutdown");
    }

    #[cfg(any(unix, windows))]
    async fn endpoint_shutdown_case() {
        use tokio::io::AsyncWriteExt as _;

        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root directory");
        let mut control = ControlPlane::open(data.clone())
            .await
            .expect("control plane");
        control
            .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
            .await
            .expect("root session");
        let shared = Arc::new(AsyncMutex::new(control));
        let endpoint = start_control_endpoint(Arc::clone(&shared))
            .await
            .expect("control endpoint");
        #[cfg(unix)]
        let mut client = tokio::net::UnixStream::connect(&endpoint.socket_path)
            .await
            .expect("connect control endpoint");
        #[cfg(windows)]
        let mut client = connect_test_pipe(&endpoint.pipe_path).await;
        endpoint.accepted.notified().await;
        client
            .write_all(b"{\"version\":1")
            .await
            .expect("write incomplete request");
        endpoint.shutdown().await.expect("endpoint shutdown");
        drop(client);

        let mut control = match Arc::try_unwrap(shared) {
            Ok(control) => control.into_inner(),
            Err(_) => panic!("endpoint retained an in-flight control request"),
        };
        control.shutdown().await.expect("control shutdown");
        drop(control);
        let mut reopened = ControlPlane::open(data)
            .await
            .expect("reopen after in-flight request shutdown");
        reopened.shutdown().await.expect("reopened shutdown");
    }

    #[cfg(windows)]
    async fn connect_test_pipe(
        pipe_path: &str,
    ) -> tokio::net::windows::named_pipe::NamedPipeClient {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match tokio::net::windows::named_pipe::ClientOptions::new().open(pipe_path) {
                Ok(client) => return client,
                Err(error) if std::time::Instant::now() < deadline => {
                    let _ = error;
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => panic!("connect control pipe: {error}"),
            }
        }
    }

    async fn lifecycle_hardening_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root directory");
        let mut control = ControlPlane::open(data.clone())
            .await
            .expect("control plane");
        control
            .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
            .await
            .expect("root session");
        control
            .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
            .expect("root turn");
        assert!(control
            .pre_tool(json!({"session_id":"other","turn_id":"root-turn","tool_use_id":"foreign","tool_name":"Read","tool_input":{}}))
            .await
            .is_err());
        assert!(control
            .pre_tool(json!({"session_id":"session","turn_id":"unknown-turn","tool_use_id":"unknown","tool_name":"Read","tool_input":{}}))
            .await
            .is_err());

        control
            .pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn-expiring","tool_name":"spawn_agent","tool_input":{}}))
            .await
            .expect("first serialized spawn");
        assert!(control
            .pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn-overlap","tool_name":"spawn_agent","tool_input":{}}))
            .await
            .is_err());
        control
            .state
            .pending
            .front_mut()
            .expect("pending spawn")
            .expires_at_millis = 0;
        assert!(control
            .subagent_start(json!({"session_id":"session","turn_id":"expired-turn","agent_id":"expired","agent_type":"explorer"}))
            .await
            .is_err());
        control
            .pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn-child","tool_name":"spawn_agent","tool_input":{}}))
            .await
            .expect("spawn after expiry");
        control
            .subagent_start(json!({"session_id":"session","turn_id":"child-turn","agent_id":"child","agent_type":"explorer"}))
            .await
            .expect("child start");
        assert!(control
            .subagent_start(json!({"session_id":"session","turn_id":"root-turn","agent_id":"collision","agent_type":"explorer"}))
            .await
            .is_err());
        assert!(
            control
                .user_prompt(json!({"session_id":"session","turn_id":"child-turn"}))
                .is_err()
        );
        assert!(control
            .subagent_start(json!({"session_id":"other","turn_id":"child-turn","agent_id":"child","agent_type":"explorer"}))
            .await
            .is_err());
        assert!(control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"unknown-tool","tool_name":"mystery_mutator","tool_input":{}}))
            .await
            .is_err());
        control.control_endpoint = Some("npipe://./pipe/test-control".to_owned());
        control.cli_bin = Some(temporary.path().join("plugin/bin"));
        let acyclic_git = control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"acyclic-status","tool_name":"exec_command","tool_input":{"cmd":"acyclic git status","workdir":root.display().to_string()}}))
            .await
            .expect("scope acyclic git command");
        let scoped = acyclic_git["hookSpecificOutput"]["updatedInput"]["cmd"]
            .as_str()
            .expect("scoped command");
        assert!(scoped.contains("ACYCLIC_CONTEXT_VERSION"));
        assert!(scoped.contains("ACYCLIC_CONTROL_ENDPOINT"));
        assert!(scoped.contains("ACYCLIC_WORKSPACE_TOKEN"));
        assert!(
            scoped.contains(
                &control
                    .cli_bin
                    .as_ref()
                    .expect("CLI bin")
                    .display()
                    .to_string()
            )
        );
        control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"acyclic-status","tool_name":"exec_command"}))
            .await
            .expect("close acyclic git barrier");
        assert!(control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"system-status","tool_name":"exec_command","tool_input":{"cmd":"git status","workdir":root.display().to_string()}}))
            .await
            .is_err());

        let child_path = control.state.routes["child"].path.clone();
        for tool_use_id in ["read-one", "read-two"] {
            control
                .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":tool_use_id,"tool_name":"Read","tool_input":{"path":child_path.join("missing.txt").display().to_string()}}))
                .await
                .expect("overlapping pre-tool hook");
        }
        assert!(control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"read-one","tool_name":"Write"}))
            .await
            .is_err());
        control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"read-two","tool_name":"Read"}))
            .await
            .expect("close second overlapping tool");
        control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"read-one","tool_name":"Read"}))
            .await
            .expect("close first overlapping tool");
        assert!(control
            .post_tool(json!({"session_id":"session","turn_id":"unknown-turn","tool_use_id":"unknown-post","tool_name":"Read"}))
            .await
            .is_err());

        let child = control
            .workspace(&control.state.routes["child"])
            .await
            .expect("child workspace");
        let mut transaction = child
            .begin_transaction(IdempotencyKey::new())
            .await
            .expect("first child transaction");
        transaction
            .write_text("/first.txt", "first")
            .await
            .expect("first file");
        transaction.commit().await.expect("commit first file");
        control.mounts["child"]
            .advance_to_head()
            .await
            .expect("advance child mount");

        let target = control.root.clone().expect("root workspace");
        WorkspaceGraph::new(control.store.clone())
            .authorize_join(child.id(), target.id())
            .await
            .expect("direct child lineage");
        let plan = child.join_into(&target).plan().await.expect("join plan");
        let target_head = plan.target_head();
        control
            .begin_root_publication(target_head, "child", plan.source_head())
            .expect("publication intent");
        let publication_key = IdempotencyKey::from_bytes(
            control
                .state
                .root_publication
                .as_ref()
                .expect("publication")
                .operation_id,
        );
        assert!(matches!(
            plan.apply(ApplyOptions {
                if_target: target_head,
                idempotency_key: publication_key
            })
            .await
            .expect("interrupted join apply"),
            JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_)
        ));
        let pre_restart_token = control.state.routes["child"].workspace_token;
        let persisted =
            fs::read_to_string(data.join("adapter-state.json")).expect("persisted adapter state");
        assert!(!persisted.contains("workspace_token"));
        drop(control);

        let mut control = ControlPlane::open(data.clone())
            .await
            .expect("recover interrupted root publication");
        assert_ne!(
            control.state.routes["child"].workspace_token,
            pre_restart_token
        );
        assert_eq!(
            fs::read(root.join("first.txt")).expect("published first file"),
            b"first"
        );
        assert!(control.state.root_publication.is_none());
        assert!(control.mounts.contains_key("child"));

        let route = &control.state.routes["child"];
        let child = control.workspace(route).await.expect("incremental child");
        let mut transaction = child
            .begin_transaction(IdempotencyKey::new())
            .await
            .expect("incremental transaction");
        transaction
            .write_text("/second.txt", "second")
            .await
            .expect("incremental file");
        transaction.commit().await.expect("incremental commit");
        control.mounts["child"]
            .advance_to_head()
            .await
            .expect("advance incremental mount");
        assert_ne!(
            *child
                .head()
                .await
                .expect("incremental child head")
                .id()
                .digest()
                .as_bytes(),
            route.published_generation
        );
        let incremental = control
            .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .expect("incremental merge");
        assert!(
            matches!(
                incremental["status"].as_str(),
                Some("applied" | "already-applied")
            ),
            "unexpected incremental merge outcome: {incremental}"
        );
        assert_eq!(
            fs::read(root.join("second.txt")).expect("published second file"),
            b"second"
        );

        control.fail_next_unmount.insert("child".to_owned());
        assert!(
            control
                .subagent_stop(
                    json!({"session_id":"session","turn_id":"child-turn","agent_id":"child"})
                )
                .await
                .is_err()
        );
        assert!(control.mounts.contains_key("child"));
        assert!(!control.state.routes["child"].stopped);
        drop(control);
        let mut control = ControlPlane::open(data.clone())
            .await
            .expect("reopen after failed stop teardown");
        assert!(control.mounts.contains_key("child"));
        assert!(!control.state.routes["child"].stopped);

        control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"active-read","tool_name":"Read","tool_input":{"path":child_path.join("first.txt").display().to_string()}}))
            .await
            .expect("active tool before stop");
        assert!(
            control
                .subagent_stop(
                    json!({"session_id":"session","turn_id":"child-turn","agent_id":"child"})
                )
                .await
                .is_err()
        );
        assert!(
            control
                .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn"}))
                .await
                .is_err()
        );
        control
            .state
            .leases
            .get_mut("active-read")
            .expect("active adapter lease")
            .expires_at_millis = 0;
        control.persist().expect("persist expired adapter lease");
        control
            .recover_expired_adapter_leases()
            .await
            .expect("recover expired adapter lease");
        assert!(control.state.leases.is_empty());
        control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"pending-descendant","tool_name":"spawn_agent","tool_input":{}}))
            .await
            .expect("pending descendant before discard");
        control
            .subagent_stop(
                json!({"session_id":"session","turn_id":"child-turn","agent_id":"child"}),
            )
            .await
            .expect("seal child");
        assert!(control.state.routes["child"].stopped);
        assert!(!control.mounts.contains_key("child"));
        drop(control);

        let mut control = ControlPlane::open(data.clone())
            .await
            .expect("reopen stopped child");
        assert!(!control.mounts.contains_key("child"));
        assert!(control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"after-stop","tool_name":"Read","tool_input":{"path":child_path.join("first.txt").display().to_string()}}))
            .await
            .is_err());
        assert_eq!(control.state.pending.len(), 1);
        control.fail_after_discard_delete = true;
        assert!(
            control
                .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn"}))
                .await
                .is_err()
        );
        assert!(control.state.pending_discards.contains_key("child"));
        drop(control);
        let mut control = ControlPlane::open(data)
            .await
            .expect("recover interrupted durable discard");
        assert!(control.state.pending.is_empty());
        assert!(control.state.pending_discards.is_empty());
        assert!(!control.state.routes.contains_key("child"));
        control.shutdown().await.expect("graceful shutdown");
    }

    async fn authenticated_control_protocol_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root directory");
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
            .pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn","tool_name":"spawn_agent","tool_input":{}}))
            .await
            .expect("spawn handshake");
        control
            .subagent_start(json!({"session_id":"session","turn_id":"child-turn","agent_id":"child","agent_type":"explorer"}))
            .await
            .expect("child start");
        control.control_endpoint = Some("npipe://./pipe/test-control".to_owned());
        control.cli_bin = Some(temporary.path().join("plugin/bin"));
        control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"git-switch","tool_name":"exec_command","tool_input":{"cmd":"acyclic git switch -c feature","workdir":root.display().to_string()}}))
            .await
            .expect("open authenticated command");
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(control.state.routes["child"].workspace_token);
        let shared = Arc::new(AsyncMutex::new(control));
        for argv in [
            vec!["checkout".to_owned(), "feature".to_owned()],
            vec!["apply".to_owned(), "change.patch".to_owned()],
            vec!["status".to_owned(), "&&".to_owned(), "push".to_owned()],
        ] {
            let rejected = dispatch_control_request(
                &shared,
                ControlRequest {
                    version: 1,
                    token: token.clone(),
                    command: "git".to_owned(),
                    argv,
                },
            )
            .await;
            assert!(rejected.is_err(), "strict Git argv must fail closed");
        }
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let (_connection_shutdown, receiver) = watch::channel(false);
        let handler = tokio::spawn(handle_control_connection(
            server,
            Arc::clone(&shared),
            receiver,
        ));
        let request = serde_json::to_vec(&json!({
            "version":1,"token":token,"command":"git","argv":["switch","-c","feature"]
        }))
        .expect("control request");
        client.write_all(&request).await.expect("write request");
        client.write_all(b"\n").await.expect("write newline");
        client.flush().await.expect("flush request");
        let mut response = String::new();
        BufReader::new(&mut client)
            .read_line(&mut response)
            .await
            .expect("read response");
        let response: Value = serde_json::from_str(&response).expect("response JSON");
        assert_eq!(response["version"], 1);
        assert_eq!(response["ok"], true);
        assert!(response["result"].is_object());
        handler
            .await
            .expect("handler task")
            .expect("handler result");

        let mut control = shared.lock().await;
        control
            .agent_changes(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .expect("inspect selected compatibility branch");
        control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"git-switch","tool_name":"exec_command"}))
            .await
            .expect("close authenticated command");
        control
            .subagent_stop(
                json!({"session_id":"session","turn_id":"child-turn","agent_id":"child"}),
            )
            .await
            .expect("stop child");
        let stale = ControlRequest {
            version: 1,
            token,
            command: "git".to_owned(),
            argv: vec!["status".to_owned()],
        };
        drop(control);
        assert!(dispatch_control_request(&shared, stale).await.is_err());
        shared.lock().await.shutdown().await.expect("shutdown");
    }

    #[allow(
        dead_code,
        reason = "retained for the authenticated acyclic-git IPC dispatcher"
    )]
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
                "session_id":"session",
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
                    "session_id":"session",
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
                "session_id":"session",
                "turn_id":"child-turn","tool_use_id":"git-switch",
                "tool_name":"exec_command",
                "tool_input":{"cmd":"git switch -c feature","workdir":root.display().to_string()}
            }))
            .await
            .expect("Git branch switch");
        control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"git-switch","tool_name":"exec_command"}))
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
        assert!(matches!(
            merged["status"].as_str(),
            Some("applied" | "already-applied")
        ));
        assert_eq!(
            fs::read(root.join("branch.txt")).expect("root branch file"),
            b"branch"
        );
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
        assert!(
            control
                .pre_tool(json!({
                    "session_id":"session",
                    "turn_id":"child-turn","tool_use_id":"recover-git-switch",
                    "tool_name":"exec_command",
                    "tool_input":{"cmd":"git status","workdir":root.display().to_string()}
                }))
                .await
                .is_err()
        );
        drop(control);
        let mut control = ControlPlane::open(temporary.path().join("plugin-data"))
            .await
            .expect("reopen after leased Git recovery");
        assert_eq!(
            control.state.routes["child"].workspace_id,
            repository_id.into_bytes()
        );
        control
            .pre_tool(json!({
                "session_id":"session",
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
                "session_id":"session",
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
        assert_ne!(
            reused.repository_workspace_id,
            route.repository_workspace_id
        );
    }

    async fn recursive_publication_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root directory");
        let mut control = ControlPlane::open(data.clone())
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
                "session_id":"session",
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
        assert!(control
            .pre_tool(json!({
                "session_id":"session",
                "turn_id":"child-turn","tool_use_id":"git-commit",
                "tool_name":"exec_command",
                "tool_input":{"cmd":"git commit -m initial","workdir":root.display().to_string()}
            }))
            .await
            .is_err());
        let read_hook = json!({
            "session_id":"session",
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
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"read-retry","tool_name":"Read"}))
            .await
            .expect("close read hook");
        control
            .pre_tool(json!({
                "session_id":"session",
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

        control.fail_next_unmount.insert("grandchild".to_owned());
        assert!(
            control
                .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn"}))
                .await
                .is_err()
        );
        assert!(control.mounts.contains_key("child"));
        assert!(control.mounts.contains_key("grandchild"));
        assert!(control.state.pending_discards.contains_key("child"));
        control.fail_after_discard_delete = true;
        assert!(
            control
                .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn"}))
                .await
                .is_err()
        );
        assert!(!control.mounts.contains_key("grandchild"));
        assert!(control.state.pending_discards.contains_key("child"));
        drop(control);

        let mut control = ControlPlane::open(data)
            .await
            .expect("recover recursive discard after durable delete");
        assert!(control.state.pending_discards.is_empty());
        assert!(control.state.routes.is_empty());
        assert!(control.state.turns.is_empty());
        assert!(control.mounts.is_empty());
        control
            .shutdown()
            .await
            .expect("shutdown after discard recovery");
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
    fn acyclic_git_commands_must_be_standalone_and_are_shell_split() {
        assert_eq!(
            split_standalone_command("git commit -m 'two words'").expect("split"),
            vec!["git", "commit", "-m", "two words"]
        );
        assert!(split_standalone_command("git status && echo escaped").is_err());
        assert_eq!(
            acyclic_git_argv(&json!({"cmd":"acyclic git status --short"}))
                .expect("Acyclic Git argv"),
            Some(vec!["status".to_owned(), "--short".to_owned()])
        );
        assert_eq!(
            acyclic_git_argv(&json!({"cmd":"./acyclic git status"}))
                .expect("path-qualified executable"),
            None
        );
    }

    #[test]
    fn capabilities_use_full_width_randomness_and_responses_are_bounded() {
        let first = new_workspace_token().expect("first OS-random capability");
        let second = new_workspace_token().expect("second OS-random capability");
        assert_ne!(first, [0; 32]);
        assert_ne!(first, second);

        let oversized = control_response(Ok(json!({
            "payload": "x".repeat(MAXIMUM_CONTROL_MESSAGE_BYTES)
        })));
        let encoded = encode_control_response(&oversized).expect("bounded response");
        assert!(encoded.len() < MAXIMUM_CONTROL_MESSAGE_BYTES);
        let response: Value = serde_json::from_slice(&encoded).expect("response JSON");
        assert_eq!(response["ok"], false);
        assert!(
            response["error"]
                .as_str()
                .is_some_and(|error| error.contains("4 MiB"))
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
