//! Unified Acyclic CLI, service, hook bridge, MCP bridge, and installer.

#![allow(clippy::cognitive_complexity, clippy::too_many_lines)]

use acyclic_fs::demand::native::NativeDemandSource;
use acyclic_fs::demand::{DemandSource, FilteredDemandSource, SourceReference};
use acyclic_fs::kernel::{FileKind, NameEncoding, NamespacePath};
use acyclic_fs::model::{CheckoutMode, GenerationSelector, VolumeLimits};
use acyclic_fs::native_host::HostRoot;
use acyclic_fs::path::PortablePath;
use acyclic_fs::{
    ApplyOptions, CancellationToken, CaptureOptions, CapturePolicy, CheckoutCommitOutcome,
    DefaultTextMergeDriver, DistributedFs, Generation, GitCommand, GitCommandOutput,
    GitCompatRepository, GitDirtyState, GitFilesystemAction, GitFilesystemExecutor,
    GitFilesystemResult, GitGenerationRef, GitIgnorePolicy, GitPublicationRecord, GitTreeRef,
    IdempotencyKey, JoinOutcome, LazyMount, LazyWorkspace, LineageMultiRootPublicationAuthorizer,
    LocalAuthorityBackend, LocalCoreStateStore, LocalFs, LocalObjectBackend, LocalOptions,
    MaterializeOptions, MaterializingWorkspaceMultiRootPublisher,
    MaterializingWorkspaceMultiRootPublisherError, MemoryMergeResolutionCache, MergeConflict,
    MergeDriverRegistry, MountOptions, MultiRootMaterializer, MultiRootMergeCandidate,
    MultiRootMergePlan, MultiRootMergeRoot, MultiRootPublication, MultiRootPublicationCoordinator,
    MultiRootPublicationError, MultiRootPublicationPhase, NativeWatch, NativeWatchOptions,
    OperationId, OperationReconcileLimits, OperationWindowLease, Publication, PublicationPermit,
    TransactionCommit, WatchBatch, WorkBudget, Workspace, WorkspaceContextId, WorkspaceContextRoot,
    WorkspaceDelete, WorkspaceError, WorkspaceMultiRootPublisherError, WorkspacePathApply,
    WorkspaceRestore, WorkspaceRootId, apply_git_patch_with_permit, blame_git_generations,
    capture_baseline_with_policy, capture_git_compatible_generation,
    capture_git_compatible_generation_at, capture_git_compatible_generation_incremental,
    capture_watch_batch_with_policy, grep_git_generation, resolve_merge_plan, walk_git_tree,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Read, Write};
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex as AsyncMutex, watch};

use acyclic_native_runtime::{RenameMode, durable_rename};
use fs2::FileExt as _;

type LocalWorkspace = Workspace<LocalAuthorityBackend, LocalObjectBackend>;
type LocalGeneration = Generation<LocalAuthorityBackend, LocalObjectBackend>;
type LocalDistributedFs =
    DistributedFs<LocalAuthorityBackend, LocalObjectBackend, LocalCoreStateStore>;
type LocalLazySource = FilteredDemandSource<NativeDemandSource>;
type LocalLazyWorkspace =
    LazyWorkspace<LocalAuthorityBackend, LocalObjectBackend, LocalLazySource, LocalCoreStateStore>;
type LocalSingleMount =
    LazyMount<LocalAuthorityBackend, LocalObjectBackend, LocalLazySource, LocalCoreStateStore>;

#[derive(Clone, Default)]
struct SharedRootRegistry {
    roots: Arc<AsyncMutex<BTreeMap<PathBuf, Weak<SharedPhysicalRoot>>>>,
}

#[derive(Clone, Copy)]
enum SharedRootAdmission {
    Fresh,
    Resume(SourceReference),
}

impl SharedRootAdmission {
    const fn reference(self) -> Option<SourceReference> {
        match self {
            Self::Fresh => None,
            Self::Resume(reference) => Some(reference),
        }
    }
}

struct SharedPhysicalRoot {
    source: Arc<LocalLazySource>,
    watcher: Mutex<NativeWatch>,
}

struct SharedRootObservation {
    batch: WatchBatch,
    prior_source: SourceReference,
    source: SourceReference,
    consumed_changes: bool,
}

impl SharedPhysicalRoot {
    async fn validate_path_identity(&self, path: PathBuf) -> Result<(), String> {
        let display_path = path.clone();
        let current =
            tokio::task::spawn_blocking(move || HostRoot::open(&path).map(|root| root.identity()))
                .await
                .map_err(|error| format!("physical root validation worker failed: {error}"))?
                .map_err(display)?;
        if current != self.source.inner().root_identity() {
            return Err(format!(
                "physical root {} changed identity while it was in use",
                display_path.display()
            ));
        }
        Ok(())
    }

    fn observe(&self) -> Result<SharedRootObservation, String> {
        let prior_source = self.source.reference();
        let batch = self
            .watcher
            .lock()
            .map_err(|_| "physical root watcher state is poisoned".to_owned())?
            .poll(65_536, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(display)?
            .value;
        let consumed_changes =
            !matches!(&batch, WatchBatch::Changes { changes, .. } if changes.is_empty());
        let source = if consumed_changes {
            self.source.inner().invalidate()
        } else {
            prior_source
        };
        Ok(SharedRootObservation {
            batch,
            prior_source,
            source,
            consumed_changes,
        })
    }
}

impl SharedRootRegistry {
    async fn acquire(
        &self,
        root: &Path,
        admission: SharedRootAdmission,
    ) -> Result<Arc<SharedPhysicalRoot>, String> {
        let canonical = root.canonicalize().map_err(display)?;
        let mut roots = self.roots.lock().await;
        if let Some(shared) = roots.get(&canonical).and_then(Weak::upgrade) {
            if admission
                .reference()
                .is_some_and(|expected| expected.identity != shared.source.reference().identity)
            {
                return Err(format!(
                    "physical root {} is already registered with another source identity",
                    canonical.display()
                ));
            }
            shared.validate_path_identity(canonical.clone()).await?;
            return Ok(shared);
        }
        roots.remove(&canonical);

        let source = open_lazy_source(&canonical, admission.reference()).await?;
        if matches!(admission, SharedRootAdmission::Resume(_)) {
            source.inner().invalidate();
        }
        let shared = Arc::new(SharedPhysicalRoot {
            source,
            watcher: Mutex::new(open_native_watcher(&canonical)?),
        });
        roots.insert(canonical, Arc::downgrade(&shared));
        Ok(shared)
    }

    #[cfg(test)]
    async fn live_roots(&self) -> usize {
        self.roots
            .lock()
            .await
            .values()
            .filter(|root| root.strong_count() > 0)
            .count()
    }

    async fn prune(&self) {
        self.roots
            .lock()
            .await
            .retain(|_, root| root.strong_count() > 0);
    }
}

struct LocalMount {
    routes: BTreeMap<String, LocalSingleMount>,
}

impl LocalMount {
    async fn mount(
        routes: impl IntoIterator<Item = (String, LocalLazyWorkspace)>,
        destination: &Path,
    ) -> Result<Self, String> {
        fs::create_dir_all(destination).map_err(display)?;
        let mut mounted = BTreeMap::new();
        for (name, workspace) in routes {
            let path = destination.join(&name);
            fs::create_dir_all(&path).map_err(display)?;
            match workspace.mount(&path, MountOptions::read_write()).await {
                Ok(mount) => {
                    mounted.insert(name, mount);
                }
                Err(error) => {
                    for mount in mounted.values() {
                        let _ = mount.unmount().await;
                    }
                    return Err(display(error));
                }
            }
        }
        Ok(Self { routes: mounted })
    }

    async fn sync_route_with_permit(
        &self,
        name: &[u8],
        permit: PublicationPermit,
    ) -> Result<(), String> {
        let name = std::str::from_utf8(name).map_err(display)?;
        self.routes
            .get(name)
            .ok_or_else(|| "mounted root route is missing".to_owned())?
            .sync_with_permit(permit)
            .await
            .map_err(display)
    }

    async fn sync(&self) -> Result<(), String> {
        for mount in self.routes.values() {
            mount.sync().await.map_err(display)?;
        }
        Ok(())
    }

    async fn advance_to_head(&self) -> Result<(), String> {
        for mount in self.routes.values() {
            mount.advance_to_head().await.map_err(display)?;
        }
        Ok(())
    }

    async fn unmount(&self) -> Result<(), String> {
        for mount in self.routes.values() {
            mount.unmount().await.map_err(display)?;
        }
        Ok(())
    }

    fn abandon(&self) -> Result<(), String> {
        for mount in self.routes.values() {
            mount.abandon().map_err(display)?;
        }
        Ok(())
    }
}
type LocalPublicationPublisher = MaterializingWorkspaceMultiRootPublisher<
    LocalAuthorityBackend,
    LocalObjectBackend,
    LocalDistributedFs,
    PluginRootMaterializer,
>;
type LocalPublicationCoordinator = MultiRootPublicationCoordinator<
    LocalCoreStateStore,
    LocalPublicationPublisher,
    LineageMultiRootPublicationAuthorizer<LocalCoreStateStore>,
>;

#[derive(Clone)]
struct PluginRootMaterializer {
    state: LocalCoreStateStore,
    physical_roots: BTreeMap<WorkspaceRootId, PhysicalRoot>,
}

#[derive(Clone)]
struct PhysicalRoot {
    workspace_id: acyclic_fs::WorkspaceId,
    path: PathBuf,
}

#[derive(Debug)]
struct PluginRootMaterializerError(String);

impl std::fmt::Display for PluginRootMaterializerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for PluginRootMaterializerError {}

impl MultiRootMaterializer<LocalAuthorityBackend, LocalObjectBackend> for PluginRootMaterializer {
    type Error = PluginRootMaterializerError;

    async fn materialize(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        workspace: &LocalWorkspace,
        from: acyclic_fs::GenerationId,
        to: acyclic_fs::GenerationId,
    ) -> Result<(), Self::Error> {
        let physical = self.physical_roots.get(&root_id).ok_or_else(|| {
            PluginRootMaterializerError(
                "publication selected a root not configured for physical materialization"
                    .to_owned(),
            )
        })?;
        // Non-root parents are projected directly from their SDK workspace;
        // advancing that mount is the complete materialization operation.
        if workspace.id() != physical.workspace_id {
            return Ok(());
        }
        let staging_parent = physical
            .path
            .parent()
            .ok_or_else(|| {
                PluginRootMaterializerError(
                    "root checkout has no same-filesystem staging parent".to_owned(),
                )
            })?
            .join(".acyclic-workspace-materializations")
            .join(short_hash(
                physical.path.as_os_str().to_string_lossy().as_bytes(),
            ));
        let directory = staging_parent
            .join(hex::encode(operation_id.into_bytes()))
            .join(hex::encode(root_id.into_bytes()));
        let options = MaterializeOptions {
            destination: directory.join("target"),
            maximum_directory_entries: 4_096,
            maximum_extent_spans: 65_536,
            transfer_bytes: 8 * 1024 * 1024,
        };
        let cancellation = CancellationToken::new();
        acyclic_fs::publish_native_workspace_generation(
            workspace,
            &self.state,
            acyclic_fs::NativeWorkspacePublication {
                root: &physical.path,
                operation_directory: &directory,
                operation_id,
                from,
                to,
                excluded_names: &[".git"],
                options: &options,
                budget: WorkBudget::UNBOUNDED,
                cancellation: &cancellation,
            },
        )
        .await
        .map_err(|error| PluginRootMaterializerError(error.to_string()))?;
        Ok(())
    }

    async fn finalize(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
    ) -> Result<(), Self::Error> {
        let physical = self.physical_roots.get(&root_id).ok_or_else(|| {
            PluginRootMaterializerError(
                "publication selected a root not configured for finalization".to_owned(),
            )
        })?;
        self.state
            .remove_materialization(operation_id)
            .map_err(|error| PluginRootMaterializerError(error.to_string()))?;
        let directory = physical
            .path
            .parent()
            .ok_or_else(|| {
                PluginRootMaterializerError(
                    "root checkout has no same-filesystem staging parent".to_owned(),
                )
            })?
            .join(".acyclic-workspace-materializations")
            .join(short_hash(
                physical.path.as_os_str().to_string_lossy().as_bytes(),
            ))
            .join(hex::encode(operation_id.into_bytes()))
            .join(hex::encode(root_id.into_bytes()));
        match fs::remove_dir_all(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(PluginRootMaterializerError(error.to_string())),
        }
        Ok(())
    }
}

async fn existing_tracked_paths(
    generation: &LocalGeneration,
    candidates: &BTreeSet<String>,
) -> Result<BTreeSet<String>, PluginGitError> {
    let mut tracked = BTreeSet::new();
    for path in candidates {
        match generation.stat(&format!("/{path}")).await {
            Ok(stat) if stat.kind != FileKind::Directory => {
                tracked.insert(path.clone());
            }
            Ok(_) | Err(WorkspaceError::NotFound) => {}
            Err(error) => return Err(PluginGitError(display(error))),
        }
    }
    Ok(tracked)
}

const AGENT_GUIDANCE: &str = "Acyclic gives native subagents isolated, recursively forked workspace contexts. Child commands run in their mounted cwd; never target a parent's original path. Use `acyclic git` for history inside managed workspaces; bare `git` is unrelated. Children appear under `agents/...`: inspect with `acyclic agents` and `acyclic git diff <ref>`, merge a direct child with `acyclic git merge <ref>`, and remove an unwanted subtree with `acyclic discard <ref>`. Start independent or dependent work speculatively as soon as you can state its assumptions; reconcile, merge, or restart when upstream changes. For debugging, freely add logs, probes, and tests in a child, then normally discard it after confirming the issue. For exploration, run hypotheses in parallel and merge only useful results. Descendants publish upward one parent at a time.";

fn capability_guidance(host: &str) -> &'static str {
    match host {
        "codex" => {
            "Codex lifecycle routing is active. Process-level escape confinement remains provisional until this host/platform installation passes the release escape suite."
        }
        "claude-code" => {
            "Claude Code lifecycle routing is active. Process-level escape confinement remains provisional until this host/platform installation passes the release escape suite."
        }
        "copilot" => {
            "Copilot lifecycle routing is provisional: subagentStart omits a stable child id and general-purpose subagents emit no lifecycle events."
        }
        "cursor" => {
            "Cursor support is provisional and CLI-only for child work: this adapter attaches the root and injects guidance, but does not claim transparent native-subagent isolation."
        }
        "opencode" => {
            "OpenCode support is provisional and CLI-only: its public plugin lifecycle does not expose enough correlated subagent lifecycle and cwd control to claim transparent isolation."
        }
        _ => {
            "This host exposes only its installed capability tier; unclassified filesystem tools fail closed."
        }
    }
}

fn guidance_for(host: &str) -> String {
    if matches!(host, "cursor" | "opencode") {
        format!(
            "Acyclic provides a Git-shaped local history CLI without intercepting bare `git`. Use `acyclic git` from the current workspace. {}",
            capability_guidance(host)
        )
    } else {
        format!("{AGENT_GUIDANCE} {}", capability_guidance(host))
    }
}

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
    distributed: &'a LocalDistributedFs,
    current: LocalWorkspace,
    lazy_current: LocalLazyWorkspace,
    repository_id: acyclic_fs::WorkspaceId,
    ignore: GitIgnorePolicy,
    permit: PublicationPermit,
    lease: Option<OperationWindowLease>,
    merge_drivers: Arc<MergeDriverRegistry>,
    merge_cache: AsyncMutex<MemoryMergeResolutionCache>,
    switched: Mutex<Option<LocalLazyWorkspace>>,
}

impl PluginGitExecutor<'_> {
    fn error(message: impl Into<String>) -> PluginGitError {
        PluginGitError(message.into())
    }

    fn switched_workspace(&self) -> Result<Option<LocalLazyWorkspace>, PluginGitError> {
        self.switched
            .lock()
            .map_err(|_| Self::error("Git workspace switch lock is unavailable"))
            .map(|workspace| workspace.clone())
    }

    async fn workspace(
        &self,
        workspace_id: acyclic_fs::WorkspaceId,
    ) -> Result<LocalWorkspace, PluginGitError> {
        self.distributed
            .workspace(workspace_id)
            .await
            .map_err(display)
            .map_err(PluginGitError)
    }

    async fn exact(
        &self,
        tree: GitTreeRef,
        operation: &str,
    ) -> Result<GitGenerationRef, PluginGitError> {
        match tree {
            GitTreeRef::Exact(reference) => Ok(reference),
            GitTreeRef::Lazy(snapshot) => {
                let workspace = self.workspace(snapshot.workspace_id).await?;
                let lazy = self
                    .lazy_current
                    .open_related(workspace)
                    .await
                    .map_err(display)?;
                let snapshot_bytes = snapshot.id.into_bytes();
                let destination = format!("git-exact-{}", hex::encode(&snapshot_bytes[..12]));
                let exact = lazy
                    .exactify_snapshot(
                        snapshot,
                        &destination,
                        IdempotencyKey::from_bytes(snapshot_bytes[..16].try_into().map_err(
                            |_| Self::error(format!("{operation} snapshot identity is invalid")),
                        )?),
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(display)?;
                self.distributed
                    .lineage()
                    .register_existing_child(
                        lazy.workspace(),
                        &self
                            .fs
                            .open_workspace(&destination)
                            .await
                            .map_err(display)?,
                        snapshot.authored_generation,
                    )
                    .await
                    .map_err(display)?;
                Ok(GitGenerationRef {
                    workspace_id: exact.value.workspace_id(),
                    generation: exact.value.id(),
                })
            }
        }
    }
}

fn git_requires_exact_workspace(argv: &[String]) -> bool {
    matches!(
        argv.first().map(String::as_str),
        Some("status" | "diff" | "blame" | "grep" | "clean" | "archive" | "check-ignore")
    )
}

impl GitFilesystemExecutor for PluginGitExecutor<'_> {
    type Error = PluginGitError;

    async fn validate(&self) -> Result<(), Self::Error> {
        let Some(lease) = &self.lease else {
            return Ok(());
        };
        let snapshot = self
            .distributed
            .operations()
            .snapshot(lease.workspace_id)
            .await
            .map_err(display)?;
        let active = match snapshot.phase {
            acyclic_fs::OperationWindowPhase::Active { leases, .. } => {
                leases.get(&lease.lease_id).is_some_and(|active| {
                    active.expires_at_millis == lease.expires_at_millis
                        && active.expires_at_millis > now_millis()
                })
            }
            acyclic_fs::OperationWindowPhase::Idle
            | acyclic_fs::OperationWindowPhase::Reconciling { .. } => false,
        };
        if active {
            Ok(())
        } else {
            Err(Self::error("Git compatibility command lease expired"))
        }
    }

    async fn execute(
        &self,
        operation_id: OperationId,
        action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult, Self::Error> {
        match action {
            GitFilesystemAction::CaptureCommit {
                workspace_tree,
                head_tree,
                head_workspace_tree,
                tracked_paths,
                ..
            } => {
                if matches!(workspace_tree, GitTreeRef::Lazy(_)) {
                    return Ok(GitFilesystemResult::Captured {
                        tree: *workspace_tree,
                        tracked_paths: tracked_paths.clone(),
                    });
                }
                let workspace_generation = self.exact(*workspace_tree, "commit").await?;
                if self.current.id() != workspace_generation.workspace_id
                    || self.current.head().await.map_err(display)?.id()
                        != workspace_generation.generation
                {
                    return Err(Self::error("Git commit workspace changed before capture"));
                }
                let captured = match (head_workspace_tree.as_ref(), head_tree) {
                    (Some(previous_live), Some(previous_capture)) => {
                        let previous_live =
                            self.exact(*previous_live, "incremental commit").await?;
                        let previous_capture =
                            self.exact(*previous_capture, "incremental commit").await?;
                        let previous_live = self
                            .workspace(previous_live.workspace_id)
                            .await?
                            .generation(previous_live.generation)
                            .await
                            .map_err(display)?;
                        let previous_capture = self
                            .workspace(previous_capture.workspace_id)
                            .await?
                            .generation(previous_capture.generation)
                            .await
                            .map_err(display)?;
                        capture_git_compatible_generation_incremental(
                            &self.current,
                            &previous_live,
                            &previous_capture,
                            &self.ignore,
                            tracked_paths,
                            operation_id,
                        )
                        .await
                    }
                    _ => {
                        capture_git_compatible_generation(
                            &self.current,
                            &self.ignore,
                            tracked_paths,
                            operation_id,
                        )
                        .await
                    }
                }
                .map_err(display)?;
                Ok(GitFilesystemResult::Captured {
                    tree: GitTreeRef::exact(
                        captured.generation.workspace_id(),
                        captured.generation.id(),
                    ),
                    tracked_paths: captured.tracked_paths,
                })
            }
            GitFilesystemAction::ForkBranch {
                branch,
                source_tree,
                switch,
                ..
            } => {
                let source_matches = match source_tree {
                    GitTreeRef::Exact(source) => {
                        self.current.id() == source.workspace_id
                            && self.current.head().await.map_err(display)?.id() == source.generation
                    }
                    GitTreeRef::Lazy(source) => {
                        self.lazy_current.snapshot().await.map_err(display)? == *source
                    }
                };
                if !source_matches {
                    return Err(Self::error("Git branch source workspace changed"));
                }
                let mut hasher = blake3::Hasher::new();
                hasher.update(&self.repository_id.into_bytes());
                hasher.update(branch.as_bytes());
                let destination = format!(
                    "git-branch-{}",
                    hex::encode(&hasher.finalize().as_bytes()[..12])
                );
                let fork_generation = self
                    .lazy_current
                    .workspace()
                    .head()
                    .await
                    .map_err(display)?
                    .id();
                let workspace = self
                    .lazy_current
                    .fork(
                        destination,
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                    )
                    .await
                    .map_err(display)?;
                self.distributed
                    .lineage()
                    .register_existing_child(
                        self.lazy_current.workspace(),
                        workspace.workspace(),
                        fork_generation,
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
                    workspace_id: workspace.workspace().id(),
                })
            }
            GitFilesystemAction::SwitchWorkspace { workspace_id } => {
                let workspace = self.workspace(*workspace_id).await?;
                let lazy_workspace = self
                    .lazy_current
                    .open_related(workspace.clone())
                    .await
                    .map_err(display)?;
                let tree = GitTreeRef::Lazy(lazy_workspace.snapshot().await.map_err(display)?);
                *self
                    .switched
                    .lock()
                    .map_err(|_| Self::error("Git workspace switch lock is unavailable"))? =
                    Some(lazy_workspace);
                Ok(GitFilesystemResult::Applied {
                    tree: Some(tree),
                    tracked_paths: None,
                })
            }
            GitFilesystemAction::Diff { from, to } => {
                let to = self.exact(*to, "diff").await?;
                let to_workspace = self.workspace(to.workspace_id).await?;
                let to = to_workspace
                    .generation(to.generation)
                    .await
                    .map_err(display)?;
                let value = if let Some(from) = from {
                    let from = self.exact(*from, "diff").await?;
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
            GitFilesystemAction::RestoreGeneration { tree, paths } => {
                let source_ref = self.exact(*tree, "restore").await?;
                let current = self.current.head().await.map_err(display)?;
                let source_workspace = self.workspace(source_ref.workspace_id).await?;
                let source = source_workspace
                    .generation(source_ref.generation)
                    .await
                    .map_err(display)?;
                let generation = if let Some(paths) = paths {
                    match self
                        .current
                        .restore_paths_from_with_permit(
                            &source,
                            &paths.iter().cloned().collect::<Vec<_>>(),
                            current.id(),
                            IdempotencyKey::from_bytes(operation_id.into_bytes()),
                            self.permit,
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
                    if source_ref.workspace_id != self.current.id() {
                        return Err(Self::error(
                            "an exact workspace restore cannot use a foreign generation",
                        ));
                    }
                    match self
                        .current
                        .restore_generation_with_permit(
                            &source,
                            current.id(),
                            IdempotencyKey::from_bytes(operation_id.into_bytes()),
                            self.permit,
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
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: None,
                })
            }
            GitFilesystemAction::RestorePaths { tree, paths } => {
                let source_ref = self.exact(*tree, "restore").await?;
                let current = self.current.head().await.map_err(display)?;
                let source_workspace = self.workspace(source_ref.workspace_id).await?;
                let source = source_workspace
                    .generation(source_ref.generation)
                    .await
                    .map_err(display)?;
                let outcome = self
                    .current
                    .restore_paths_from_with_permit(
                        &source,
                        paths,
                        current.id(),
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                        self.permit,
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
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: None,
                })
            }
            GitFilesystemAction::Join {
                source_workspace,
                rebase,
                tracked_paths,
                ..
            } => {
                let source = self.workspace(*source_workspace).await?;
                let mut builder = source.join_into(&self.current);
                if *rebase {
                    builder = builder.history(acyclic_fs::JoinHistory::Rebase);
                }
                let plan = builder.plan().await.map_err(display)?;
                let options = ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::from_bytes(operation_id.into_bytes()),
                };
                let initial = plan
                    .apply_with_permit(options, self.permit)
                    .await
                    .map_err(display)?;
                let mut projected_conflicts = None;
                let outcome = if let JoinOutcome::Conflicted {
                    conflicts,
                    truncated,
                } = initial
                {
                    let typed = plan
                        .describe_conflicts(&conflicts, truncated)
                        .await
                        .map_err(display)?;
                    let typed_json = serde_json::to_string(&typed).map_err(display)?;
                    let candidate = {
                        let mut cache = self.merge_cache.lock().await;
                        resolve_merge_plan(typed, &self.merge_drivers, &mut *cache, false).map_err(
                            |error| {
                                Self::error(format!(
                                    "Git join has unresolved typed conflicts: {typed_json}; {error}"
                                ))
                            },
                        )?
                    };
                    let has_markers = candidate.resolutions.values().any(|resolution| {
                        matches!(resolution, acyclic_fs::MergeResolution::Text(text)
                            if acyclic_fs::text_merge::has_conflict_markers(text))
                    });
                    let outcome = plan
                        .apply_candidate_with_permit(options, &candidate, self.permit)
                        .await
                        .map_err(display)?;
                    if has_markers {
                        projected_conflicts = Some(typed_json);
                    }
                    outcome
                } else {
                    initial
                };
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
                if let Some(conflicts) = projected_conflicts {
                    return Err(Self::error(format!(
                        "Git join projected unresolved text conflicts into the workspace: {conflicts}. Edit them, then run `acyclic git merge --continue`, or run `acyclic git merge --abort`"
                    )));
                }
                let resulting_tracked = existing_tracked_paths(&generation, tracked_paths).await?;
                Ok(GitFilesystemResult::Applied {
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: Some(resulting_tracked),
                })
            }
            GitFilesystemAction::ApplyCommit {
                base,
                source,
                paths,
                tracked_paths,
                ..
            } => {
                let base_workspace = match base {
                    Some(base) => {
                        let base = self.exact(*base, "apply commit").await?;
                        Some(self.workspace(base.workspace_id).await?)
                    }
                    None => None,
                };
                let base = match (base, base_workspace.as_ref()) {
                    (Some(base), Some(workspace)) => {
                        let base = self.exact(*base, "apply commit").await?;
                        Some(
                            workspace
                                .generation(base.generation)
                                .await
                                .map_err(display)?,
                        )
                    }
                    _ => None,
                };
                let source_workspace = match source {
                    Some(source) => {
                        let source = self.exact(*source, "apply commit").await?;
                        Some(self.workspace(source.workspace_id).await?)
                    }
                    None => None,
                };
                let source = match (source, source_workspace.as_ref()) {
                    (Some(source), Some(workspace)) => {
                        let source = self.exact(*source, "apply commit").await?;
                        Some(
                            workspace
                                .generation(source.generation)
                                .await
                                .map_err(display)?,
                        )
                    }
                    _ => None,
                };
                let current = self.current.head().await.map_err(display)?;
                let outcome = self
                    .current
                    .apply_paths_from_with_permit(
                        base.as_ref(),
                        source.as_ref(),
                        &paths.iter().cloned().collect::<Vec<_>>(),
                        current.id(),
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                        self.permit,
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
                let resulting_tracked = existing_tracked_paths(&generation, tracked_paths).await?;
                Ok(GitFilesystemResult::Applied {
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: Some(resulting_tracked),
                })
            }
            GitFilesystemAction::Archive { tree } => {
                let reference = self.exact(*tree, "archive").await?;
                let workspace = self.workspace(reference.workspace_id).await?;
                let generation = workspace
                    .generation(reference.generation)
                    .await
                    .map_err(display)?;
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
                tree,
            } => {
                let reference = self.exact(*tree, "grep").await?;
                let workspace = self.workspace(reference.workspace_id).await?;
                let generation = workspace
                    .generation(reference.generation)
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
                    let reference = self.exact(commit.tree, "blame").await?;
                    let workspace = self.workspace(reference.workspace_id).await?;
                    let generation = workspace
                        .generation(reference.generation)
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
                tree,
                tracked_paths,
            } => {
                let reference = self.exact(*tree, "clean").await?;
                if reference.workspace_id != self.current.id() {
                    return Err(Self::error(
                        "Git clean generation is not the live workspace",
                    ));
                }
                let generation = self
                    .current
                    .generation(reference.generation)
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
                let generation = match transaction
                    .commit_with_permit(self.permit)
                    .await
                    .map_err(display)?
                {
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
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: None,
                })
            }
            GitFilesystemAction::ApplyPatch { patch } => {
                let generation = match apply_git_patch_with_permit(
                    &self.current,
                    patch,
                    IdempotencyKey::from_bytes(operation_id.into_bytes()),
                    self.permit,
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
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: None,
                })
            }
            GitFilesystemAction::CheckIgnore { paths, tree } => {
                let reference = self.exact(*tree, "check-ignore").await?;
                if reference.workspace_id != self.current.id()
                    || self.current.head().await.map_err(display)?.id() != reference.generation
                {
                    return Err(Self::error(
                        "Git check-ignore live generation changed before inspection",
                    ));
                }
                let ignored = paths
                    .iter()
                    .filter(|path| !self.ignore.eligible(path, false, false))
                    .cloned()
                    .collect::<Vec<_>>();
                Ok(GitFilesystemResult::Data {
                    kind: "check-ignore".to_owned(),
                    value: json!({ "paths": ignored }),
                })
            }
            unsupported => Err(Self::error(format!(
                "Git filesystem action is not yet available in the Codex adapter: {unsupported:?}"
            ))),
        }
    }
}

struct RootMaterializingGitExecutor<'a> {
    inner: PluginGitExecutor<'a>,
    root: &'a Path,
    store: &'a LocalCoreStateStore,
}

impl RootMaterializingGitExecutor<'_> {
    fn switched_workspace(&self) -> Result<Option<LocalLazyWorkspace>, PluginGitError> {
        self.inner.switched_workspace()
    }

    async fn materialize(
        &self,
        operation_id: OperationId,
        from: LocalGeneration,
    ) -> Result<(), PluginGitError> {
        let target = self.inner.switched_workspace()?.map_or_else(
            || self.inner.current.clone(),
            |lazy| lazy.workspace().clone(),
        );
        let to = target.head().await.map_err(display)?.id();
        if from.id() == to {
            return Ok(());
        }
        let operation_directory =
            root_materialization_directory(self.root, operation_id).map_err(PluginGitError)?;
        let options = MaterializeOptions {
            destination: operation_directory.join("target"),
            maximum_directory_entries: 4_096,
            maximum_extent_spans: 65_536,
            transfer_bytes: 8 * 1024 * 1024,
        };
        let cancellation = CancellationToken::new();
        let to_generation = target.head().await.map_err(display)?;
        acyclic_fs::publish_native_generation_transition(
            &from,
            &to_generation,
            self.store,
            acyclic_fs::NativeWorkspacePublication {
                root: self.root,
                operation_directory: &operation_directory,
                operation_id,
                from: from.id(),
                to,
                excluded_names: &[".git"],
                options: &options,
                budget: WorkBudget::UNBOUNDED,
                cancellation: &cancellation,
            },
        )
        .await
        .map_err(display)?;
        self.store
            .remove_materialization(operation_id)
            .map_err(display)?;
        remove_tree_checked(
            &root_materialization_root(self.root).map_err(PluginGitError)?,
            &operation_directory,
        )
        .map_err(display)?;
        Ok(())
    }
}

impl GitFilesystemExecutor for RootMaterializingGitExecutor<'_> {
    type Error = PluginGitError;

    async fn execute(
        &self,
        operation_id: OperationId,
        action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult, Self::Error> {
        let from = self.inner.current.head().await.map_err(display)?;
        let result = self.inner.execute(operation_id, action).await;
        // Conflict projection is an intentional state transition: the join
        // executor leaves the Git transition pending and returns an error so
        // the caller can present the typed conflicts. Materialize that new
        // generation before propagating the error, otherwise the checkout
        // would never expose the files the user must edit.
        let materialized = self.materialize(operation_id, from).await;
        match (result, materialized) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), Ok(())) | (_, Err(error)) => Err(error),
        }
    }
}

fn git_transition_command(argv: &[String]) -> bool {
    matches!(
        argv,
        [command, option] if command == "merge" && matches!(option.as_str(), "--continue" | "--abort")
    ) || argv.first().is_some_and(|command| command == "add")
}

async fn run_git_command<E: GitFilesystemExecutor>(
    repository: &GitCompatRepository<LocalCoreStateStore>,
    argv: &[String],
    workspace_tree: GitTreeRef,
    patch: Option<&[u8]>,
    author: &str,
    authored_at_seconds: i64,
    executor: &E,
) -> Result<Option<GitCommandOutput>, String> {
    if !git_transition_command(argv)
        && repository
            .resume(executor)
            .await
            .map_err(display)?
            .is_some()
    {
        return Ok(None);
    }
    let output = if let Some(patch) = patch {
        repository
            .run(
                GitCommand::Apply {
                    patch: patch.to_vec(),
                },
                workspace_tree,
                executor,
            )
            .await
    } else {
        repository
            .run_argv(argv, workspace_tree, author, authored_at_seconds, executor)
            .await
    };
    output.map(Some).map_err(display)
}

async fn git_apply_patch(
    workspace: &LocalLazyWorkspace,
    root: &Path,
    argv: &[String],
) -> Result<Option<Vec<u8>>, String> {
    if argv.first().is_none_or(|command| command != "apply") {
        return Ok(None);
    }
    let [_, patch_path] = argv else {
        return Err("acyclic git apply requires exactly one patch file".to_owned());
    };
    if patch_path == "-" {
        return Err(
            "acyclic git apply does not accept stdin through the service; pass a patch file"
                .to_owned(),
        );
    }
    let patch_path = workspace_path_argument(root, patch_path)?;
    workspace
        .read(&patch_path, 16 * 1024 * 1024)
        .await
        .map(|bytes| Some(bytes.to_vec()))
        .map_err(display)
}

async fn git_workspace_tree(
    workspace: &LocalLazyWorkspace,
    argv: &[String],
    permit: Option<PublicationPermit>,
) -> Result<GitTreeRef, String> {
    if !git_requires_exact_workspace(argv) {
        return workspace
            .snapshot()
            .await
            .map(GitTreeRef::Lazy)
            .map_err(display);
    }
    let exact = match permit {
        Some(permit) => {
            workspace
                .exactify_with_permit(WorkBudget::UNBOUNDED, &CancellationToken::new(), permit)
                .await
        }
        None => {
            workspace
                .exactify(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
        }
    }
    .map_err(display)?;
    Ok(GitTreeRef::exact(
        workspace.workspace().id(),
        exact.value.id(),
    ))
}

async fn git_policy(
    workspace: &LocalLazyWorkspace,
) -> Result<(GitIgnorePolicy, Arc<MergeDriverRegistry>), String> {
    let ignore_text = match workspace.read("/.gitignore", 1024 * 1024).await {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(acyclic_fs::LazyWorkspaceError::NotFound) => String::new(),
        Err(error) => return Err(display(error)),
    };
    Ok((
        GitIgnorePolicy::parse(&format!("{ignore_text}\n.git/\n.acyclic-sdk/\n")),
        merge_drivers_for(workspace).await?,
    ))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RootBinding {
    root_id: [u8; 16],
    path: PathBuf,
    workspace_name: String,
    workspace_id: [u8; 16],
    #[serde(default)]
    repository_workspace_id: [u8; 16],
    source_identity: [u8; 16],
    source_epoch: u64,
    #[serde(default)]
    native_root_identity: [u8; 16],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RouteRoot {
    root_id: [u8; 16],
    workspace_name: String,
    workspace_id: [u8; 16],
    #[serde(default)]
    repository_workspace_id: [u8; 16],
    parent_workspace_id: [u8; 16],
    route_name: String,
    #[serde(default)]
    published_generation: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Route {
    agent_id: String,
    turn_id: String,
    context_id: [u8; 16],
    root_id: [u8; 16],
    parent_agent_id: String,
    #[serde(default)]
    roots: BTreeMap<String, RouteRoot>,
    #[serde(default)]
    mount_path: PathBuf,
    path: PathBuf,
    #[serde(default)]
    stopped: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingSpawn {
    parent_agent_id: String,
    #[serde(default)]
    active_root_id: Option<[u8; 16]>,
    expires_at_millis: u64,
    workspace_name: String,
    fork_key: [u8; 16],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LeaseRecord {
    agent_id: String,
    #[serde(default)]
    turn_id: String,
    #[serde(default)]
    tool_name: String,
    roots: BTreeMap<String, RootLeaseRecord>,
    expires_at_millis: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RootLeaseRecord {
    root_id: [u8; 16],
    route_name: String,
    workspace_id: [u8; 16],
    lease_id: [u8; 16],
    pinned_parent: [u8; 32],
    lease_expires_at_millis: u64,
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
    repository_workspace_ids: Vec<[u8; 16]>,
    #[serde(default)]
    mount_detached: bool,
    workspaces: VecDeque<DiscardWorkspace>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingDiscard {
    parent_context_id: [u8; 16],
    child_context_id: [u8; 16],
    context_discarded: bool,
    agents: VecDeque<DiscardAgent>,
}

impl LeaseRecord {
    fn from_leases(
        agent_id: String,
        turn_id: String,
        tool_name: String,
        roots: impl IntoIterator<Item = (WorkspaceRootId, String, OperationWindowLease)>,
    ) -> Self {
        let roots = roots
            .into_iter()
            .map(|(root_id, route_name, lease)| {
                (
                    root_key(root_id),
                    RootLeaseRecord {
                        root_id: root_id.into_bytes(),
                        route_name,
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
    fn lease(&self) -> OperationWindowLease {
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
struct AdapterState {
    version: u32,
    root_session_id: String,
    root_agent_id: String,
    #[serde(default)]
    root_turns: BTreeSet<String>,
    root_path: PathBuf,
    root_workspace_name: String,
    #[serde(default)]
    root_repository_workspace_id: [u8; 16],
    root_context_id: [u8; 16],
    root_id: [u8; 16],
    #[serde(default)]
    root_source_identity: [u8; 16],
    #[serde(default)]
    root_source_epoch: u64,
    #[serde(default)]
    root_native_identity: [u8; 16],
    #[serde(default)]
    roots: BTreeMap<String, RootBinding>,
    routes: BTreeMap<String, Route>,
    turns: BTreeMap<String, String>,
    pending: VecDeque<PendingSpawn>,
    leases: BTreeMap<String, LeaseRecord>,
    #[serde(default)]
    pending_discards: BTreeMap<String, PendingDiscard>,
}

struct ControlPlane {
    data: PathBuf,
    config_root: PathBuf,
    fs: LocalFs,
    store: LocalCoreStateStore,
    distributed: LocalDistributedFs,
    shared_roots: SharedRootRegistry,
    state: AdapterState,
    roots: BTreeMap<String, LocalLazyWorkspace>,
    physical_roots: BTreeMap<String, Arc<SharedPhysicalRoot>>,
    mounts: BTreeMap<String, LocalMount>,
    #[cfg(test)]
    fail_next_unmount: BTreeSet<String>,
    #[cfg(test)]
    fail_after_discard_delete: bool,
    #[cfg(test)]
    fail_after_context_discard: bool,
    #[cfg(test)]
    fail_before_publication_history: bool,
    #[cfg(test)]
    fail_after_watch_poll: bool,
}

impl ControlPlane {
    #[cfg(test)]
    async fn open(data: PathBuf) -> Result<Self, String> {
        fs::create_dir_all(&data).map_err(display)?;
        let fs = LocalFs::local(LocalOptions::new(data.join("filesystem")))
            .await
            .map_err(display)?;
        let store = LocalCoreStateStore::new(data.join("core-state"));
        Self::open_with(data.clone(), data, fs, store, SharedRootRegistry::default()).await
    }

    async fn open_with(
        data: PathBuf,
        config_root: PathBuf,
        fs: LocalFs,
        store: LocalCoreStateStore,
        shared_roots: SharedRootRegistry,
    ) -> Result<Self, String> {
        fs::create_dir_all(&data).map_err(display)?;
        let state = load_state(&data)?;
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
        };
        control.reconcile_routes_from_contexts().await?;
        control.restore_root().await?;
        control.recover_multi_root_publications().await?;
        control.recover_expired_adapter_leases().await?;
        control.restore_mounts().await?;
        control.recover_pending_discards().await?;
        Ok(control)
    }

    async fn reconcile_routes_from_contexts(&mut self) -> Result<(), String> {
        let registry = self.distributed.contexts();
        if self.state.root_context_id != [0; 16] {
            let root_context = registry
                .resolve(WorkspaceContextId::from_bytes(self.state.root_context_id))
                .await
                .map_err(display)?;
            for (root_id, context_root) in root_context.roots {
                if let Some(binding) = self.state.roots.get_mut(&root_key(root_id)) {
                    binding.workspace_id = context_root.workspace_id.into_bytes();
                    binding
                        .workspace_name
                        .clone_from(&context_root.workspace_name);
                }
                if root_id.into_bytes() == self.state.root_id {
                    self.state.root_workspace_name = context_root.workspace_name;
                }
            }
        }
        for route in self.state.routes.values_mut() {
            let context = registry
                .resolve(WorkspaceContextId::from_bytes(route.context_id))
                .await
                .map_err(display)?;
            for (root_id, context_root) in context.roots {
                let routed = route
                    .roots
                    .get_mut(&root_key(root_id))
                    .ok_or_else(|| "adapter route is missing a core context root".to_owned())?;
                routed.workspace_id = context_root.workspace_id.into_bytes();
                routed.workspace_name = context_root.workspace_name;
                routed.parent_workspace_id = context_root
                    .parent_workspace_id
                    .map_or([0; 16], acyclic_fs::WorkspaceId::into_bytes);
            }
        }
        self.persist()
    }

    fn author_for_argv(&self, argv: &[String], fallback: &str) -> Result<String, String> {
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

    async fn publication_coordinator(&self) -> Result<LocalPublicationCoordinator, String> {
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

    async fn pending_conflict_for_parent(
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

    async fn record_publication_history(
        &self,
        parent_agent: &str,
        child_agent: &str,
        publication: &MultiRootPublication,
    ) -> Result<(), String> {
        let parent_route = self.state.routes.get(parent_agent).cloned();
        let child_route = self
            .state
            .routes
            .get(child_agent)
            .cloned()
            .ok_or_else(|| "published child route is unavailable".to_owned())?;
        for (root_id, root) in &publication.candidate.plan.roots {
            let parent_repository_id = if parent_agent == self.state.root_agent_id {
                let binding = self
                    .state
                    .roots
                    .get(&root_key(*root_id))
                    .ok_or_else(|| "published root binding is unavailable".to_owned())?;
                repository_id(binding.workspace_id, binding.repository_workspace_id)
            } else {
                let route = parent_route
                    .as_ref()
                    .ok_or_else(|| "published parent route is unavailable".to_owned())?;
                let binding = route
                    .roots
                    .get(&root_key(*root_id))
                    .ok_or_else(|| "published parent root is unavailable".to_owned())?;
                repository_id(binding.workspace_id, binding.repository_workspace_id)
            };
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
            let ignore = GitIgnorePolicy::parse(&format!("{ignore_text}\n.git/\n.acyclic-sdk/\n"));
            let repository = self.distributed.git(parent_repository_id);
            let tracked = repository.tracked_paths().await.map_err(display)?;
            let child_binding = child_route
                .roots
                .get(&root_key(*root_id))
                .ok_or_else(|| "published child root is unavailable".to_owned())?;
            let child_repository_id = repository_id(
                child_binding.workspace_id,
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
                    message: format!("Merge agents/{child_agent}"),
                    author: parent_agent.to_owned(),
                    authored_at_seconds: i64::try_from(now_millis() / 1_000).unwrap_or(i64::MAX),
                })
                .await
                .map_err(display)?;
        }
        Ok(())
    }

    async fn agent_merge_transition(&mut self, caller: &str, abort: bool) -> Result<Value, String> {
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
            self.record_publication_history(caller, &child_agent, publication)
                .await?;
            if let Some(route) = self.state.routes.get_mut(&child_agent) {
                for (root_id, root) in &publication.candidate.plan.roots {
                    if let Some(routed) = route.roots.get_mut(&root_key(*root_id)) {
                        routed.published_generation = *root.source_generation.digest().as_bytes();
                    }
                }
            }
            coordinator
                .acknowledge_applied(publication)
                .await
                .map_err(display)?;
        }
        self.persist()?;
        Ok(json!({
            "status": if abort { "aborted" } else { "applied" },
            "operation": hex::encode(operation_id.into_bytes())
        }))
    }

    async fn recover_multi_root_publications(&mut self) -> Result<(), String> {
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
                    self.record_publication_history(&parent, &child, &publication)
                        .await?;
                    coordinator
                        .acknowledge_applied(&publication)
                        .await
                        .map_err(display)?;
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

    async fn recover_expired_adapter_leases(&mut self) -> Result<(), String> {
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
        let mut recovered_agents = BTreeSet::new();
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
            recovered_agents.insert(record.agent_id);
        }
        for agent_id in recovered_agents {
            if let Some(mount) = self.mounts.remove(&agent_id) {
                mount.abandon().map_err(display)?;
                let route = self
                    .state
                    .routes
                    .get(&agent_id)
                    .cloned()
                    .ok_or_else(|| "recovered lease route is missing".to_owned())?;
                if !route.stopped {
                    self.mounts
                        .insert(agent_id, self.mount_route(&route).await?);
                }
            }
        }
        self.state
            .leases
            .retain(|_, lease| lease.expires_at_millis > now);
        self.persist()
    }

    async fn restore_root(&mut self) -> Result<(), String> {
        if self.state.root_session_id.is_empty() {
            return Ok(());
        }
        if self.state.roots.is_empty() {
            if self.state.root_source_identity == [0; 16] {
                return Err("legacy eager Acyclic state is not reopened; remove the old per-user store manually".to_owned());
            }
            self.state.roots.insert(
                hex::encode(self.state.root_id),
                RootBinding {
                    root_id: self.state.root_id,
                    path: self.state.root_path.clone(),
                    workspace_name: self.state.root_workspace_name.clone(),
                    workspace_id: self.state.root_repository_workspace_id,
                    repository_workspace_id: self.state.root_repository_workspace_id,
                    source_identity: self.state.root_source_identity,
                    source_epoch: self.state.root_source_epoch,
                    native_root_identity: self.state.root_native_identity,
                },
            );
        }
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
            if root.root_id == self.state.root_id {
                self.state.root_source_epoch = refreshed.epoch;
            }
            let workspace = self
                .distributed
                .workspace(acyclic_fs::WorkspaceId::from_bytes(root.workspace_id))
                .await
                .map_err(display)?;
            self.roots.insert(
                key.clone(),
                self.distributed
                    .open_lazy(workspace, Arc::clone(&source))
                    .await
                    .map_err(display)?,
            );
            self.physical_roots.insert(key, physical);
        }
        self.persist()
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
                fs::create_dir_all(&route.mount_path).map_err(display)?;
                let mount = self.mount_route(route).await?;
                self.mounts.insert(route.agent_id.clone(), mount);
            }
        }
        Ok(())
    }

    #[cfg(test)]
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
        let host = input.get("host").and_then(Value::as_str).unwrap_or("codex");
        let root_path = PathBuf::from(string(&input, "cwd")?);
        let canonical = root_path.canonicalize().map_err(display)?;
        if !self.state.root_session_id.is_empty() && self.state.root_session_id != session_id {
            return Err("plugin data is already bound to another root session".to_owned());
        }
        if !self.state.root_session_id.is_empty() {
            if self.roots.is_empty() {
                self.restore_root().await?;
            }
            let already_registered = self
                .state
                .roots
                .values()
                .any(|root| canonical.starts_with(&root.path));
            if !already_registered {
                let root_id = WorkspaceRootId::new();
                let workspace_name = format!(
                    "root-{}-{}",
                    short_hash(session_id.as_bytes()),
                    short_hash(canonical.as_os_str().to_string_lossy().as_bytes()),
                );
                let physical = self
                    .shared_roots
                    .acquire(&canonical, SharedRootAdmission::Fresh)
                    .await?;
                let source = Arc::clone(&physical.source);
                let source_reference = source.reference();
                let workspace = self
                    .distributed
                    .attach_lazy(&workspace_name, Arc::clone(&source))
                    .await
                    .map_err(display)?;
                self.distributed
                    .lineage()
                    .register_root(workspace.workspace())
                    .await
                    .map_err(display)?;
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
                self.state.roots.insert(
                    root_key(root_id),
                    RootBinding {
                        root_id: root_id.into_bytes(),
                        path: canonical,
                        workspace_name,
                        workspace_id: workspace.workspace().id().into_bytes(),
                        repository_workspace_id: workspace.workspace().id().into_bytes(),
                        source_identity: source_reference.identity,
                        source_epoch: source_reference.epoch,
                        native_root_identity: source.inner().root_identity().to_bytes(),
                    },
                );
                self.roots.insert(root_key(root_id), workspace);
                self.physical_roots.insert(root_key(root_id), physical);
                self.persist()?;
            }
            return Ok(json!({
                "hookSpecificOutput": {
                    "hookEventName": "SessionStart",
                    "additionalContext": guidance_for(host)
                }
            }));
        }
        let workspace_name = format!(
            "root-{}-{}",
            short_hash(session_id.as_bytes()),
            short_hash(canonical.as_os_str().to_string_lossy().as_bytes()),
        );
        let physical = self
            .shared_roots
            .acquire(&canonical, SharedRootAdmission::Fresh)
            .await?;
        let source = Arc::clone(&physical.source);
        let source_reference = source.reference();
        let workspace = self
            .distributed
            .attach_lazy(&workspace_name, Arc::clone(&source))
            .await
            .map_err(display)?;
        self.distributed
            .lineage()
            .register_root(workspace.workspace())
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
        self.distributed
            .contexts()
            .register_root(
                context_id,
                [WorkspaceContextRoot {
                    root_id,
                    source_path: canonical.clone(),
                    workspace_id: workspace.workspace().id(),
                    workspace_name: workspace_name.clone(),
                    parent_workspace_id: None,
                    mount_path: None,
                }],
            )
            .await
            .map_err(display)?;
        self.state.version = 1;
        self.state.root_session_id = session_id.clone();
        self.state.root_agent_id = format!("root:{session_id}");
        self.state.root_path = canonical;
        self.state.root_workspace_name = workspace_name;
        if self.state.root_repository_workspace_id == [0; 16] {
            self.state.root_repository_workspace_id = workspace.workspace().id().into_bytes();
        }
        self.state.root_context_id = context_id.into_bytes();
        self.state.root_id = root_id.into_bytes();
        self.state.root_source_identity = source_reference.identity;
        self.state.root_source_epoch = source_reference.epoch;
        self.state.root_native_identity = source.inner().root_identity().to_bytes();
        let binding = RootBinding {
            root_id: root_id.into_bytes(),
            path: self.state.root_path.clone(),
            workspace_name: self.state.root_workspace_name.clone(),
            workspace_id: workspace.workspace().id().into_bytes(),
            repository_workspace_id: self.state.root_repository_workspace_id,
            source_identity: source_reference.identity,
            source_epoch: source_reference.epoch,
            native_root_identity: source.inner().root_identity().to_bytes(),
        };
        self.state.roots.insert(root_key(root_id), binding);
        self.roots.insert(root_key(root_id), workspace);
        self.physical_roots.insert(root_key(root_id), physical);
        self.persist()?;
        Ok(json!({
            "hookSpecificOutput": {
                "hookEventName": "SessionStart",
                "additionalContext": guidance_for(host)
            }
        }))
    }

    #[allow(clippy::needless_pass_by_value)]
    fn user_prompt(&mut self, input: Value) -> Result<Value, String> {
        let session_id = string(&input, "session_id")?;
        if session_id != self.state.root_session_id {
            return Err("user prompt belongs to another root session".to_owned());
        }
        let turn_id = string(&input, "turn_id")?;
        if self.state.turns.contains_key(&turn_id) {
            return Err("root turn identity is already bound to a subagent".to_owned());
        }
        self.remember_root_turn(turn_id);
        self.persist()?;
        Ok(json!({"suppressOutput": true}))
    }

    async fn pre_tool(&mut self, input: Value) -> Result<Value, String> {
        self.require_session(&input)?;
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
            if route.stopped {
                return Err("subagent workspace is sealed after SubagentStop".to_owned());
            }
            Some(route)
        };
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
            });
            self.persist()?;
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
        let selected_path = route.mount_path.join(&selected_route_root.route_name);
        let original =
            normalize_tool_input(input.get("tool_input").cloned().unwrap_or(Value::Null))?;
        #[cfg(not(target_os = "linux"))]
        for binding in self.state.roots.values() {
            reject_original_root_references(&original, None, &binding.path.to_string_lossy())?;
        }
        let mut updated =
            rewrite_tool_input(&tool_name, original, &selected_binding.path, &selected_path)?;
        if is_acyclic_cli_invocation(&tool_name, &updated)? {
            // The CLI opens its own core operation lease. Wrapping it in the host-tool
            // lease would deadlock the compatibility command behind itself.
            self.persist()?;
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
                        format!("{agent_id}:{tool_use_id}:{}", route_root.route_name),
                        now,
                        now.saturating_add(15 * 60 * 1_000),
                    )
                    .await
                    .map_err(display)
            }
            .await;
            match acquired {
                Ok(lease) => leases.push((root_id, route_root.route_name.clone(), lease)),
                Err(error) => {
                    self.rollback_started_leases(&route, &leases, now).await?;
                    return Err(error);
                }
            }
        }
        self.state.leases.insert(
            tool_use_id,
            LeaseRecord::from_leases(agent_id, turn_id, tool_name, leases),
        );
        self.persist()?;
        Ok(pre_tool_update(updated))
    }

    async fn rollback_started_leases(
        &self,
        route: &Route,
        leases: &[(WorkspaceRootId, String, OperationWindowLease)],
        now: u64,
    ) -> Result<(), String> {
        let operations = self.distributed.operations();
        for (root_id, _, lease) in leases.iter().rev() {
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

    async fn git_tool(
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
        let route_path = route.mount_path.join(&route_root.route_name);
        let lazy_workspace = self.lazy_workspace_root(route, root_id).await?;
        let workspace = lazy_workspace.workspace().clone();
        self.sync_agent(&route.parent_agent_id).await?;
        let parent_context = self
            .distributed
            .contexts()
            .resolve(self.context_for_agent(&route.parent_agent_id)?)
            .await
            .map_err(display)?;
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
        let repository_id =
            repository_id(route_root.workspace_id, route_root.repository_workspace_id);
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
        let parent_head = parent.head().await.map(|generation| generation.id());
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
            active.workspace_name = switched.workspace().name().as_str().to_owned();
            active.workspace_id = switched.workspace().id().into_bytes();
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

    async fn root_git_tool(
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
            root_binding.workspace_id,
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
            if root_id.into_bytes() == self.state.root_id {
                self.state.root_workspace_name = switched.workspace().name().as_str().to_owned();
            }
            if let Some(binding) = self.state.roots.get_mut(&root_key(root_id)) {
                binding.workspace_name = switched.workspace().name().as_str().to_owned();
                binding.workspace_id = switched.workspace().id().into_bytes();
            }
            self.roots.insert(root_key(root_id), switched);
        }
        self.persist()?;
        let Some(output) = output else {
            return Err("recovered a pending Git transition; retry the current command".to_owned());
        };
        serde_json::to_value(output).map_err(display)
    }

    async fn subagent_start(&mut self, input: Value) -> Result<Value, String> {
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
            if !route.stopped && route.turn_id != turn_id {
                return Err("subagent identity is already bound to another turn".to_owned());
            }
            if route.stopped {
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
                resumed.stopped = false;
                resumed.turn_id.clone_from(&turn_id);
                self.mounts.insert(agent_id.clone(), mount);
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
        if authenticated_parent.is_some_and(|parent| parent != pending.parent_agent_id) {
            self.persist()?;
            return Err("subagent start parent does not match its spawn permit".to_owned());
        }
        self.sync_agent(&pending.parent_agent_id).await?;
        let workspace_name_prefix = pending.workspace_name;
        let fork_key = IdempotencyKey::from_bytes(pending.fork_key);
        let mount_path = self
            .data
            .join("workspaces")
            .join(short_hash(agent_id.as_bytes()));
        fs::create_dir_all(&mount_path).map_err(display)?;
        if mount_path.read_dir().map_err(display)?.next().is_some() {
            return Err("child workspace destination is not empty".to_owned());
        }
        let context_id = WorkspaceContextId::from_bytes(fork_key.into_bytes());
        let parent_context_id = self.context_for_agent(&pending.parent_agent_id)?;
        let parent_context = self
            .distributed
            .contexts()
            .resolve(parent_context_id)
            .await
            .map_err(display)?;
        let active_root_id = if let Some(active_root_id) = pending.active_root_id {
            WorkspaceRootId::from_bytes(active_root_id)
        } else if pending.parent_agent_id == self.state.root_agent_id {
            WorkspaceRootId::from_bytes(self.state.root_id)
        } else {
            WorkspaceRootId::from_bytes(
                self.state
                    .routes
                    .get(&pending.parent_agent_id)
                    .ok_or_else(|| "subagent parent route is missing".to_owned())?
                    .root_id,
            )
        };
        let mut context_roots = Vec::with_capacity(parent_context.roots.len());
        let mut route_roots = BTreeMap::new();
        let mut mounted_roots = Vec::with_capacity(parent_context.roots.len());
        for (root_id, parent_root) in parent_context.roots {
            let source_binding = self
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
                source_binding.native_root_identity,
                &source_binding.path,
            )?;
            let parent_workspace = self
                .distributed
                .workspace(parent_root.workspace_id)
                .await
                .map_err(display)?;
            let parent_lazy = self
                .distributed
                .open_lazy(parent_workspace, source)
                .await
                .map_err(display)?;
            let root_name = route_name(root_id);
            let child_name = format!("{workspace_name_prefix}-{root_name}");
            let root_fork_key = derived_idempotency_key(fork_key, root_id);
            let fork_generation = parent_lazy.workspace().head().await.map_err(display)?.id();
            let child = parent_lazy
                .fork(&child_name, root_fork_key)
                .await
                .map_err(display)?;
            self.distributed
                .lineage()
                .register_existing_child(
                    parent_lazy.workspace(),
                    child.workspace(),
                    fork_generation,
                )
                .await
                .map_err(display)?;
            let child_path = mount_path.join(&root_name);
            context_roots.push(WorkspaceContextRoot {
                root_id,
                source_path: source_binding.path.clone(),
                workspace_id: child.workspace().id(),
                workspace_name: child_name.clone(),
                parent_workspace_id: Some(parent_lazy.workspace().id()),
                mount_path: Some(child_path),
            });
            route_roots.insert(
                root_key(root_id),
                RouteRoot {
                    root_id: root_id.into_bytes(),
                    workspace_name: child_name,
                    workspace_id: child.workspace().id().into_bytes(),
                    repository_workspace_id: child.workspace().id().into_bytes(),
                    parent_workspace_id: parent_lazy.workspace().id().into_bytes(),
                    route_name: root_name.clone(),
                    published_generation: [0; 32],
                },
            );
            mounted_roots.push((root_name, child));
        }
        let active = route_roots
            .get(&root_key(active_root_id))
            .ok_or_else(|| "active root is missing from parent context".to_owned())?;
        let active_path = mount_path.join(&active.route_name);
        self.distributed
            .contexts()
            .register_child(context_id, parent_context_id, context_roots)
            .await
            .map_err(display)?;
        let mount = LocalMount::mount(mounted_roots, &mount_path).await?;
        let route = Route {
            agent_id: agent_id.clone(),
            turn_id: turn_id.clone(),
            context_id: context_id.into_bytes(),
            root_id: active_root_id.into_bytes(),
            parent_agent_id: pending.parent_agent_id,
            roots: route_roots,
            mount_path: mount_path.clone(),
            path: active_path.clone(),
            stopped: false,
        };
        self.mounts.insert(agent_id.clone(), mount);
        self.state.turns.insert(turn_id, agent_id.clone());
        self.state.routes.insert(agent_id, route);
        self.persist()?;
        Ok(subagent_context(&active_path))
    }

    async fn post_tool(&mut self, input: Value) -> Result<Value, String> {
        self.require_session(&input)?;
        let turn_id = string(&input, "turn_id")?;
        let tool_name = string(&input, "tool_name")?;
        let caller = self.resolve_turn(&turn_id)?;
        let tool_use_id = string(&input, "tool_use_id")?;
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
        self.sync_agent(&route.parent_agent_id).await?;
        let operations = self.distributed.operations();
        let parent_context = self
            .distributed
            .contexts()
            .resolve(self.context_for_agent(&route.parent_agent_id)?)
            .await
            .map_err(display)?;
        let mut sync_error = None;
        let mut expired = false;
        let mut conflicts = Vec::new();
        let mut truncated = false;
        let now = now_millis();
        for root_record in record.roots.values() {
            let root_id = WorkspaceRootId::from_bytes(root_record.root_id);
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
                .observe_parent(workspace.id(), parent.head().await.map_err(display)?.id())
                .await
                .map_err(display)?;
            let lease = root_record.lease();
            if sync_error.is_none() {
                let sync = match self.mounts.get(&record.agent_id) {
                    Some(mount) => mount
                        .sync_route_with_permit(
                            root_record.route_name.as_bytes(),
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
            match operations.finish(&lease, now).await.map_err(display)? {
                acyclic_fs::OperationWindowFinish::StillActive { .. } => {}
                acyclic_fs::OperationWindowFinish::Reconcile(reconcile) => {
                    if let acyclic_fs::WorkspaceRebase::Conflicted {
                        conflicts: root_conflicts,
                        truncated: root_truncated,
                    } = operations
                        .reconcile_workspace(
                            &workspace,
                            reconcile,
                            OperationReconcileLimits::default(),
                        )
                        .await
                        .map_err(display)?
                    {
                        conflicts.extend(root_conflicts);
                        truncated |= root_truncated;
                    }
                }
                acyclic_fs::OperationWindowFinish::AlreadyClosed => expired = true,
            }
        }
        if let Some(error) = sync_error {
            self.state.leases.remove(&tool_use_id);
            if let Some(mount) = self.mounts.remove(&record.agent_id) {
                mount.abandon().map_err(display)?;
            }
            if !self
                .state
                .leases
                .values()
                .any(|lease| lease.agent_id == record.agent_id)
            {
                let mount = self.mount_route(&route).await?;
                self.mounts.insert(record.agent_id.clone(), mount);
            }
            self.persist()?;
            return Err(format!(
                "filesystem tool publication was fenced before lease close: {error}"
            ));
        }
        if expired {
            self.state.leases.remove(&tool_use_id);
            self.persist()?;
            return Err("filesystem tool lease expired; late writes were fenced".to_owned());
        }
        self.state.leases.remove(&tool_use_id);
        self.persist()?;
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

    async fn subagent_stop(&mut self, input: Value) -> Result<Value, String> {
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
        if route.stopped {
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
            .stopped = true;
        self.persist()?;
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
        let summary = json!({
            "agent": reference,
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
            "suppressOutput": false,
            "systemMessage": format!(
                "Acyclic stopped {reference}. Inspect with `acyclic git diff {reference}`; merge with `acyclic git merge {reference}`; discard with `acyclic discard {reference}`."
            ),
            "acyclicSummary": summary,
        }))
    }

    async fn agents_status(&mut self, caller: &str) -> Result<Value, String> {
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
            let mut roots = Vec::with_capacity(route.roots.len());
            let mut pending_publications = 0_usize;
            for root in route.roots.values() {
                let current = self
                    .distributed
                    .workspace(acyclic_fs::WorkspaceId::from_bytes(root.workspace_id))
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
                    "route": root.route_name,
                    "workspace": hex::encode(root.workspace_id),
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
                "state": if route.stopped { "frozen" } else { "running" },
                "activeLeases": active_leases,
                "conflicts": usize::from(conflict),
                "descendants": descendants,
                "fileChanges": file_changes,
                "bindingChanges": binding_changes,
                "changedPaths": changed_paths,
                "workRemaining": file_changes.zip(binding_changes).map(|(files, bindings)| files.saturating_add(bindings)),
                "pendingPublications": pending_publications,
                "mount": route.path,
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

    #[cfg(test)]
    async fn agent_changes(&mut self, input: Value) -> Result<Value, String> {
        let caller = self.caller(&input)?;
        self.agent_changes_as(&caller, input).await
    }

    async fn agent_changes_as(&mut self, caller: &str, input: Value) -> Result<Value, String> {
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
            let repository_id =
                repository_id(route_root.workspace_id, route_root.repository_workspace_id);
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
                let filter = agent_change_filter(path, &route, route_root)?;
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
    async fn agent_merge(&mut self, input: Value) -> Result<Value, String> {
        let caller = self.caller(&input)?;
        self.agent_merge_as(&caller, input).await
    }

    async fn agent_merge_as(&mut self, caller: &str, input: Value) -> Result<Value, String> {
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
        let mut source_heads = BTreeMap::new();
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
            lazy_source
                .exactify(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(display)?;
            let source_head = source.head().await.map_err(display)?.id();
            let ignore_text = match lazy_source.read("/.gitignore", 1024 * 1024).await {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(acyclic_fs::LazyWorkspaceError::NotFound) => String::new(),
                Err(error) => return Err(display(error)),
            };
            let parent_repository_id = if caller == self.state.root_agent_id {
                let binding = self
                    .state
                    .roots
                    .get(&root_key(*root_id))
                    .ok_or_else(|| "parent root binding is unavailable".to_owned())?;
                repository_id(binding.workspace_id, binding.repository_workspace_id)
            } else {
                let binding = self
                    .state
                    .routes
                    .get(caller)
                    .and_then(|route| route.roots.get(&root_key(*root_id)))
                    .ok_or_else(|| "parent route root is unavailable".to_owned())?;
                repository_id(binding.workspace_id, binding.repository_workspace_id)
            };
            let tracked = self
                .distributed
                .git(parent_repository_id)
                .tracked_paths()
                .await
                .map_err(display)?;
            let mut capture_hasher = blake3::Hasher::new();
            capture_hasher.update(b"acyclic-agent-publication-source-v1\0");
            capture_hasher.update(&route.context_id);
            capture_hasher.update(&root_id.into_bytes());
            capture_hasher.update(source_head.digest().as_bytes());
            let mut capture_id = [0_u8; 16];
            capture_id.copy_from_slice(&capture_hasher.finalize().as_bytes()[..16]);
            let captured = capture_git_compatible_generation(
                &source,
                &GitIgnorePolicy::parse(&format!("{ignore_text}\n.git/\n.acyclic-sdk/\n")),
                &tracked,
                OperationId::from_bytes(capture_id),
            )
            .await
            .map_err(display)?;
            let merge_source = captured.generation.workspace();
            let merge_workspace_id =
                (merge_source.id() != source.id()).then_some(merge_source.id());
            if let Some(initial_generation) = captured.initial_generation {
                self.distributed
                    .lineage()
                    .register_existing_child_at_generation(
                        &source,
                        &merge_source,
                        source_head,
                        initial_generation,
                    )
                    .await
                    .map_err(display)?;
            }
            let plan = merge_source
                .join_into(&target)
                .plan()
                .await
                .map_err(display)?;
            let target_head = plan.target_head();
            roots.insert(
                *root_id,
                MultiRootMergeRoot {
                    source_workspace_id: source.id(),
                    merge_workspace_id,
                    source_generation: captured.generation.id(),
                    target_workspace_id: target.id(),
                    target_generation: target_head,
                    base_generation: plan.common_ancestor(),
                },
            );
            resolutions.insert(*root_id, BTreeMap::new());
            source_heads.insert(*root_id, source_head);
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
        self.record_publication_history(caller, &agent, &applied_publication)
            .await?;
        let limits = OperationReconcileLimits::default();
        let mut published_source_heads = BTreeMap::new();
        for (root_id, child_root) in &child_context.roots {
            let source = self
                .distributed
                .workspace(child_root.workspace_id)
                .await
                .map_err(display)?;
            let rebase_key = derived_idempotency_key(
                IdempotencyKey::from_bytes(operation_id.into_bytes()),
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
                .map_err(display)?
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
            published_source_heads.insert(*root_id, generation.id());
        }
        if let Some(child_mount) = self.mounts.get(&agent) {
            child_mount.advance_to_head().await.map_err(display)?;
        }
        coordinator
            .acknowledge_applied(&applied_publication)
            .await
            .map_err(display)?;
        let route_root_id = route.root_id;
        let route = self
            .state
            .routes
            .get_mut(&agent)
            .ok_or_else(|| "merged agent route disappeared".to_owned())?;
        for (root_id, source_head) in &published_source_heads {
            let routed = route
                .roots
                .get_mut(&root_key(*root_id))
                .ok_or_else(|| "merged agent route root disappeared".to_owned())?;
            routed.published_generation = *source_head.digest().as_bytes();
        }
        self.persist()?;
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
    async fn agent_discard(&mut self, input: Value) -> Result<Value, String> {
        let caller = self.caller(&input)?;
        self.agent_discard_as(&caller, input).await
    }

    async fn agent_discard_as(&mut self, caller: &str, input: Value) -> Result<Value, String> {
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
        self.state
            .pending
            .retain(|spawn| !subtree.contains(&spawn.parent_agent_id));
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
                let mut repository_ids = BTreeSet::new();
                let mut workspace_ids = BTreeSet::new();
                for root in removed.roots.values() {
                    let repository_id =
                        repository_id(root.workspace_id, root.repository_workspace_id);
                    repository_ids.insert(repository_id);
                    workspace_ids.insert(repository_id);
                    workspace_ids.insert(acyclic_fs::WorkspaceId::from_bytes(root.workspace_id));
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
                    repository_workspace_id: repository_ids
                        .iter()
                        .next()
                        .map_or([0; 16], |id| id.into_bytes()),
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
                route.stopped = true;
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
            remove_tree_checked(&self.data.join("workspaces"), &agent.path)
                .map_err(|error| format!("cannot remove discarded mount path: {error}"))?;
            let repository_ids = if agent.repository_workspace_ids.is_empty() {
                vec![agent.repository_workspace_id]
            } else {
                agent.repository_workspace_ids.clone()
            };
            for repository_id in repository_ids {
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

    async fn unmount_agent(&mut self, agent_id: &str) -> Result<(), String> {
        self.ensure_agent_idle(agent_id)?;
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

    #[cfg(test)]
    fn caller(&self, input: &Value) -> Result<String, String> {
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

    fn route_from_cwd(&self, cwd: &Path) -> Result<(String, Option<Route>), String> {
        self.route_root_from_cwd(cwd)
            .map(|(agent, route, _)| (agent, route))
    }

    fn route_root_from_cwd(
        &self,
        cwd: &Path,
    ) -> Result<(String, Option<Route>, WorkspaceRootId), String> {
        let canonical = cwd.canonicalize().map_err(display)?;
        let mut mounted = Vec::new();
        for route in self.state.routes.values().filter(|route| !route.stopped) {
            for root in route.roots.values() {
                let path = route.mount_path.join(&root.route_name);
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

    fn require_session(&self, input: &Value) -> Result<(), String> {
        let session_id = string(input, "session_id")?;
        if session_id != self.state.root_session_id {
            return Err("lifecycle event belongs to another root session".to_owned());
        }
        Ok(())
    }

    fn context_for_agent(&self, agent_id: &str) -> Result<WorkspaceContextId, String> {
        if agent_id == self.state.root_agent_id {
            return Ok(WorkspaceContextId::from_bytes(self.state.root_context_id));
        }
        self.state
            .routes
            .get(agent_id)
            .map(|route| WorkspaceContextId::from_bytes(route.context_id))
            .ok_or_else(|| "workspace context alias is unknown".to_owned())
    }

    fn agent_for_context(&self, context_id: WorkspaceContextId) -> Option<String> {
        if self.state.root_context_id == context_id.into_bytes() {
            return Some(self.state.root_agent_id.clone());
        }
        self.state
            .routes
            .values()
            .find(|route| route.context_id == context_id.into_bytes())
            .map(|route| route.agent_id.clone())
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

    fn remember_root_turn(&mut self, turn_id: String) {
        self.state.root_turns.insert(turn_id);
        while self.state.root_turns.len() > MAXIMUM_ADAPTER_ROOT_TURNS {
            self.state.root_turns.pop_first();
        }
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

    #[cfg(test)]
    async fn workspace(&self, route: &Route) -> Result<LocalWorkspace, String> {
        self.workspace_root(route, WorkspaceRootId::from_bytes(route.root_id))
            .await
    }

    async fn workspace_root(
        &self,
        route: &Route,
        root_id: WorkspaceRootId,
    ) -> Result<LocalWorkspace, String> {
        let route_root = route
            .roots
            .get(&root_key(root_id))
            .ok_or_else(|| "workspace route root is missing".to_owned())?;
        self.distributed
            .workspace(acyclic_fs::WorkspaceId::from_bytes(route_root.workspace_id))
            .await
            .map_err(display)
    }

    async fn lazy_workspace_root(
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

    async fn mount_route(&self, route: &Route) -> Result<LocalMount, String> {
        let mut roots = Vec::with_capacity(route.roots.len());
        for root in route.roots.values() {
            let root_id = WorkspaceRootId::from_bytes(root.root_id);
            roots.push((
                root.route_name.clone(),
                self.lazy_workspace_root(route, root_id).await?,
            ));
        }
        LocalMount::mount(roots, &route.mount_path).await
    }

    async fn refresh_native_root(&mut self, root_id: WorkspaceRootId) -> Result<(), String> {
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
        let observation = physical.observe()?;
        let source_advanced_elsewhere = binding.source_epoch != observation.prior_source.epoch;
        if !observation.consumed_changes && !source_advanced_elsewhere {
            return Ok(());
        }
        #[cfg(test)]
        if observation.consumed_changes && self.fail_after_watch_poll {
            self.fail_after_watch_poll = false;
            return Err("injected failure after shared watcher poll".to_owned());
        }
        let workspace = self
            .roots
            .get(&key)
            .ok_or_else(|| "root workspace is unavailable".to_owned())?
            .workspace()
            .clone();
        let root_identity = physical
            .watcher
            .lock()
            .map_err(|_| "physical root watcher state is poisoned".to_owned())?
            .root_identity();
        let limits = VolumeLimits::default();
        let capture = CaptureOptions {
            source_root: binding.path.clone(),
            expected_root_identity: root_identity,
            maximum_paths: 262_144,
            maximum_extent_spans: 65_536,
        };
        let policy = CapturePolicy::excluding(
            ["/.git", "/.acyclic-sdk"]
                .into_iter()
                .map(|path| {
                    let path = PortablePath::parse(path, limits).map_err(display)?;
                    NamespacePath::from_portable(&path, limits).map_err(display)
                })
                .collect::<Result<Vec<_>, String>>()?,
        )
        .map_err(display)?;
        let mut checkout = workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await
            .map_err(display)?;
        match observation.batch {
            WatchBatch::Changes { .. } if source_advanced_elsewhere => {
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
        if self.state.root_id == root_id.into_bytes() {
            self.state.root_source_epoch = observation.source.epoch;
        }
        self.persist()
    }

    async fn sync_agent(&mut self, agent_id: &str) -> Result<(), String> {
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

    fn ensure_agent_idle(&self, agent_id: &str) -> Result<(), String> {
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

    fn persist(&self) -> Result<(), String> {
        save_state(&self.data, &self.state)
    }

    async fn shutdown(&mut self) -> Result<(), String> {
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
        self.state.leases.clear();
        self.persist()?;
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
        input
            .as_object_mut()
            .ok_or_else(|| "apply_patch input must be an object".to_owned())?
            .insert("command".to_owned(), Value::String(rewritten));
    }
    validate_tool_paths(tool_name, &input, child)?;
    Ok(input)
}

#[allow(clippy::needless_pass_by_value)]
fn pre_tool_update(updated: Value) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "updatedInput": updated
        }
    })
}

fn is_acyclic_cli_invocation(tool_name: &str, input: &Value) -> Result<bool, String> {
    if !(matches!(tool_name, "Bash" | "exec_command") || tool_name.ends_with("__exec_command")) {
        return Ok(false);
    }
    let command = input
        .get("cmd")
        .or_else(|| input.get("command"))
        .and_then(Value::as_str)
        .ok_or_else(|| "shell tool input lacks a string command".to_owned())?;
    let argv = split_standalone_command(command)?;
    Ok(argv.first().is_some_and(|program| {
        Path::new(program)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.eq_ignore_ascii_case("acyclic") || name.eq_ignore_ascii_case("acyclic.exe")
            })
    }))
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
                    return Err("shell expansion is not allowed in an Acyclic command".to_owned());
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
                        "Acyclic commands cannot be composed with shell operators".to_owned()
                    );
                }
                _ => current.push(character),
            },
        }
    }
    if quote.is_some() || escaped {
        return Err("Acyclic command has an unterminated quote or escape".to_owned());
    }
    if !current.is_empty() {
        arguments.push(current);
    }
    Ok(arguments)
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

fn workspace_path_argument(mount: &Path, argument: &str) -> Result<String, String> {
    let argument = Path::new(argument);
    let relative = if argument.is_absolute() {
        argument
            .strip_prefix(mount)
            .map_err(|_| "path is outside the managed workspace".to_owned())?
    } else {
        argument
    };
    let mut result = String::new();
    for component in relative.components() {
        match component {
            Component::Normal(name) => {
                let name = name
                    .to_str()
                    .ok_or_else(|| "compatibility path is not valid Unicode".to_owned())?;
                result.push('/');
                result.push_str(name);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err("path escapes the managed workspace".to_owned());
            }
        }
    }
    if result.is_empty() {
        return Err("path must name a file inside the managed workspace".to_owned());
    }
    Ok(result)
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
                '\'' | '"' | '(' | ')' | '[' | ']' | '{' | '}' | ';' | ',' | '<' | '>'
            )
    }) {
        let token = token.trim_matches(|character: char| matches!(character, '`' | '&' | '|'));
        if token.is_empty() {
            continue;
        }
        if token.starts_with('-') {
            if let Some((_, value)) = token.split_once('=')
                && (Path::new(value).is_absolute()
                    || value.starts_with('.')
                    || value.contains('/')
                    || value.contains('\\'))
            {
                validate_path(value, child)?;
            }
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

async fn open_lazy_source(
    root: &Path,
    reference: Option<SourceReference>,
) -> Result<Arc<LocalLazySource>, String> {
    let limits = VolumeLimits::default();
    let source = match reference {
        Some(reference) => {
            NativeDemandSource::open_with_reference(
                root,
                acyclic_fs::model::FilesystemProfile::Portable,
                limits,
                reference,
            )
            .await
        }
        None => {
            NativeDemandSource::open(root, acyclic_fs::model::FilesystemProfile::Portable, limits)
                .await
        }
    }
    .map_err(display)?;
    let excluded = ["/.git", "/.acyclic-sdk"]
        .into_iter()
        .map(|path| {
            let path = PortablePath::parse(path, limits).map_err(display)?;
            NamespacePath::from_portable(&path, limits).map_err(display)
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Arc::new(FilteredDemandSource::new(source, excluded)))
}

fn open_native_watcher(root: &Path) -> Result<NativeWatch, String> {
    let mut watcher = NativeWatch::open_with_profile(
        root,
        acyclic_fs::model::FilesystemProfile::Portable,
        NativeWatchOptions {
            limits: VolumeLimits::default(),
            maximum_queued_changes: 65_536,
            recursive: true,
        },
    )
    .map_err(display)?;
    watcher.accept_lazy_baseline().map_err(display)?;
    Ok(watcher)
}

fn verify_native_root_identity(
    source: &LocalLazySource,
    expected: [u8; 16],
    path: &Path,
) -> Result<(), String> {
    if expected == [0; 16] {
        return Err(format!(
            "native root identity is absent for {}; fresh Acyclic state is required",
            path.display()
        ));
    }
    let observed = source.inner().root_identity().to_bytes();
    if observed != expected {
        return Err(format!(
            "native root identity changed for {}; refusing to reuse its workspace",
            path.display()
        ));
    }
    Ok(())
}

async fn merge_drivers_for(
    workspace: &LocalLazyWorkspace,
) -> Result<Arc<MergeDriverRegistry>, String> {
    let attributes = match workspace.read("/.gitattributes", 1024 * 1024).await {
        Ok(bytes) => String::from_utf8(bytes.to_vec())
            .map_err(|_| ".gitattributes must be UTF-8".to_owned())?,
        Err(acyclic_fs::LazyWorkspaceError::NotFound) => String::new(),
        Err(error) => return Err(display(error)),
    };
    let mut registry = MergeDriverRegistry::new();
    registry
        .register("text", Arc::new(DefaultTextMergeDriver))
        .map_err(display)?;
    registry.set_default("text").map_err(display)?;
    if !attributes.trim().is_empty() {
        registry.set_git_attributes(&attributes).map_err(display)?;
    }
    Ok(Arc::new(registry))
}

fn agent_change_filter(
    path: &str,
    route: &Route,
    root: &RouteRoot,
) -> Result<NamespacePath, String> {
    let candidate = Path::new(path);
    let relative = if candidate.is_absolute() {
        candidate
            .strip_prefix(route.mount_path.join(&root.route_name))
            .map_err(|_| "the selected path is outside the inspected workspace root".to_owned())?
            .to_path_buf()
    } else {
        candidate.to_path_buf()
    };
    let mut portable = String::from("/");
    for component in relative.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(name) => {
                if portable.len() > 1 {
                    portable.push('/');
                }
                portable.push_str(
                    name.to_str()
                        .ok_or_else(|| "change filter path is not UTF-8".to_owned())?,
                );
            }
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err("change filter path must remain inside one workspace root".to_owned());
            }
        }
    }
    let limits = VolumeLimits::default();
    let portable = PortablePath::parse(&portable, limits).map_err(display)?;
    NamespacePath::from_portable(&portable, limits).map_err(display)
}

fn namespace_path_text(path: &NamespacePath) -> Result<String, String> {
    if path.is_root() {
        return Ok("/".to_owned());
    }
    let components = path
        .components()
        .iter()
        .map(|name| match name.encoding() {
            NameEncoding::Utf8 => std::str::from_utf8(name.as_bytes())
                .map(str::to_owned)
                .map_err(display),
            NameEncoding::WindowsUtf16Le => {
                let units = name
                    .as_bytes()
                    .chunks_exact(2)
                    .filter_map(|bytes| bytes.first().copied().zip(bytes.get(1).copied()))
                    .map(|(low, high)| u16::from_le_bytes([low, high]));
                char::decode_utf16(units)
                    .collect::<Result<String, _>>()
                    .map_err(display)
            }
            NameEncoding::PosixBytes => {
                Err("non-UTF-8 path cannot be presented to an agent".to_owned())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("/{}", components.join("/")))
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
        || tool_name.starts_with("mcp__acyclic__")
}

fn is_pure_remote_tool(tool_name: &str) -> bool {
    tool_name.starts_with("web__")
        || tool_name.starts_with("collaboration.")
        || tool_name.starts_with("mcp__codex_app__")
        || matches!(tool_name, "send_message" | "wait_agent" | "list_agents")
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
            validate_path(
                path.to_str()
                    .ok_or_else(|| "patch path is not valid Unicode".to_owned())?,
                child,
            )?;
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
            "additionalContext": format!("Your workspace mount is {}. {AGENT_GUIDANCE}", path.display())
        }
    })
}

const MAXIMUM_ADAPTER_STATE_BYTES: u64 = 4 * 1024 * 1024;
const MAXIMUM_ADAPTER_ROOTS: usize = 256;
const MAXIMUM_ADAPTER_ROUTES: usize = 4_096;
const MAXIMUM_ADAPTER_TURNS: usize = 16_384;
const MAXIMUM_ADAPTER_ROOT_TURNS: usize = 16_384;
const MAXIMUM_ADAPTER_PENDING: usize = 4_096;
const MAXIMUM_ADAPTER_LEASES: usize = 16_384;
const MAXIMUM_ADAPTER_DISCARDS: usize = 4_096;

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
    validate_state_bounds(state)?;
    let mut serialized = BoundedJsonBuffer::new();
    serde_json::to_writer(&mut serialized, state).map_err(|_| {
        format!("adapter state exceeds the {MAXIMUM_ADAPTER_STATE_BYTES} byte bound")
    })?;
    let path = data.join("adapter-state.json");
    let previous = data.join("adapter-state.previous.json");
    let next = data.join("adapter-state.next.json");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&next)
        .map_err(display)?;
    file.write_all(&serialized.bytes).map_err(display)?;
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
    match fs::File::open(path) {
        Ok(file) => {
            let length = file.metadata().map_err(display)?.len();
            if length > MAXIMUM_ADAPTER_STATE_BYTES {
                return Err(format!(
                    "adapter state exceeds the {MAXIMUM_ADAPTER_STATE_BYTES} byte bound"
                ));
            }
            let capacity = usize::try_from(length)
                .map_err(|_| "adapter state length does not fit this platform".to_owned())?;
            let mut bytes = Vec::with_capacity(capacity);
            file.take(MAXIMUM_ADAPTER_STATE_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(display)?;
            if bytes.len() as u64 > MAXIMUM_ADAPTER_STATE_BYTES {
                return Err(format!(
                    "adapter state exceeds the {MAXIMUM_ADAPTER_STATE_BYTES} byte bound"
                ));
            }
            let state = serde_json::from_slice(&bytes).map_err(display)?;
            validate_state_bounds(&state)?;
            Ok(Some(state))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

fn validate_state_bounds(state: &AdapterState) -> Result<(), String> {
    for (name, observed, maximum) in [
        ("roots", state.roots.len(), MAXIMUM_ADAPTER_ROOTS),
        ("routes", state.routes.len(), MAXIMUM_ADAPTER_ROUTES),
        ("turns", state.turns.len(), MAXIMUM_ADAPTER_TURNS),
        (
            "root turns",
            state.root_turns.len(),
            MAXIMUM_ADAPTER_ROOT_TURNS,
        ),
        (
            "pending spawns",
            state.pending.len(),
            MAXIMUM_ADAPTER_PENDING,
        ),
        ("leases", state.leases.len(), MAXIMUM_ADAPTER_LEASES),
        (
            "pending discards",
            state.pending_discards.len(),
            MAXIMUM_ADAPTER_DISCARDS,
        ),
    ] {
        if observed > maximum {
            return Err(format!(
                "adapter state has too many {name}: {observed} > {maximum}"
            ));
        }
    }
    if state
        .routes
        .values()
        .any(|route| route.roots.len() > MAXIMUM_ADAPTER_ROOTS)
        || state
            .leases
            .values()
            .any(|lease| lease.roots.len() > MAXIMUM_ADAPTER_ROOTS)
    {
        return Err("adapter route or lease exceeds the root bound".to_owned());
    }
    if state.pending_discards.values().any(|discard| {
        discard.agents.len() > MAXIMUM_ADAPTER_ROUTES
            || discard.agents.iter().any(|agent| {
                agent.repository_workspace_ids.len() > MAXIMUM_ADAPTER_ROOTS
                    || agent.workspaces.len() > MAXIMUM_ADAPTER_ROOTS
            })
    }) {
        return Err("adapter discard queue exceeds its structural bound".to_owned());
    }
    Ok(())
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

fn decode_fixed<const N: usize>(value: &str) -> Result<[u8; N], String> {
    let mut bytes = [0_u8; N];
    hex::decode_to_slice(value, &mut bytes).map_err(display)?;
    Ok(bytes)
}

fn short_hash(bytes: &[u8]) -> String {
    hex::encode(&blake3::hash(bytes).as_bytes()[..12])
}

fn root_key(root_id: WorkspaceRootId) -> String {
    hex::encode(root_id.into_bytes())
}

fn repository_id(
    workspace_id: [u8; 16],
    repository_workspace_id: [u8; 16],
) -> acyclic_fs::WorkspaceId {
    acyclic_fs::WorkspaceId::from_bytes(if repository_workspace_id == [0; 16] {
        workspace_id
    } else {
        repository_workspace_id
    })
}

fn route_name(root_id: WorkspaceRootId) -> String {
    let key = root_key(root_id);
    format!("root-{}", key.get(..12).unwrap_or(&key))
}

fn derived_idempotency_key(fork_key: IdempotencyKey, root_id: WorkspaceRootId) -> IdempotencyKey {
    let mut input = Vec::with_capacity(32);
    input.extend_from_slice(&fork_key.into_bytes());
    input.extend_from_slice(&root_id.into_bytes());
    let digest = blake3::hash(&input);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(digest.as_bytes().get(..16).unwrap_or(digest.as_bytes()));
    IdempotencyKey::from_bytes(bytes)
}

fn root_materialization_root(root: &Path) -> Result<PathBuf, String> {
    Ok(root
        .parent()
        .ok_or_else(|| "root checkout has no same-filesystem staging parent".to_owned())?
        .join(".acyclic-workspace-materializations")
        .join(short_hash(root.as_os_str().to_string_lossy().as_bytes())))
}

fn root_materialization_directory(
    root: &Path,
    operation_id: OperationId,
) -> Result<PathBuf, String> {
    Ok(root_materialization_root(root)?.join(hex::encode(operation_id.into_bytes())))
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

fn public_tools(commandless: bool) -> Value {
    if !commandless {
        return json!([]);
    }
    json!([{
        "name": "acyclic",
        "description": "Run the Acyclic CLI dispatcher without shell parsing. Pass the same argv used after the `acyclic` executable; use `-C <path>` to select a workspace explicitly.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "argv": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Acyclic arguments, for example [\"git\", \"status\"] or [\"-C\", \"/workspace\", \"agents\"]."
                }
            },
            "required": ["argv"],
            "additionalProperties": false
        }
    }])
}

const MAXIMUM_CONTROL_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const CONTROL_RESPONSE_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ControlCommand {
    Ping,
    Upgrade,
    Doctor,
    Hook,
    Git,
    Agents,
    Discard,
}

#[derive(Debug, Deserialize, Serialize)]
struct ControlRequest {
    version: u32,
    command: ControlCommand,
    cwd: PathBuf,
    #[serde(default)]
    argv: Vec<String>,
    #[serde(default)]
    name: String,
    #[serde(default)]
    arguments: Value,
}

struct ControlEndpoint {
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
    control: Arc<AsyncMutex<impl ControlRequestDispatcher + 'static>>,
    data: &Path,
) -> Result<ControlEndpoint, String> {
    #[cfg(windows)]
    let opaque_id = short_hash(data.as_os_str().to_string_lossy().as_bytes());
    let (shutdown, receiver) = watch::channel(false);
    #[cfg(test)]
    let accepted = Arc::new(tokio::sync::Notify::new());

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let socket_path = data.join("service.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).map_err(display)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).map_err(display)?;
        let task = tokio::spawn(serve_unix_control(
            listener,
            control,
            shutdown.clone(),
            receiver,
            #[cfg(test)]
            Arc::clone(&accepted),
        ));
        Ok(ControlEndpoint {
            shutdown,
            task,
            #[cfg(test)]
            accepted,
            socket_path,
        })
    }

    #[cfg(windows)]
    {
        let opaque_name = format!("acyclic-{opaque_id}");
        let pipe_path = format!(r"\\.\pipe\{opaque_name}");
        #[cfg(test)]
        let endpoint_pipe_path = pipe_path.clone();
        let task = tokio::spawn(serve_windows_control(
            pipe_path,
            control,
            shutdown.clone(),
            receiver,
            #[cfg(test)]
            Arc::clone(&accepted),
        ));
        Ok(ControlEndpoint {
            shutdown,
            task,
            #[cfg(test)]
            accepted,
            #[cfg(test)]
            pipe_path: endpoint_pipe_path,
        })
    }
}

#[cfg(unix)]
async fn serve_unix_control(
    listener: tokio::net::UnixListener,
    control: Arc<AsyncMutex<impl ControlRequestDispatcher + 'static>>,
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
                if !same_user_peer(&stream)? {
                    continue;
                }
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

#[cfg(any(target_os = "linux", target_os = "android"))]
#[allow(unsafe_code)]
fn same_user_peer(stream: &tokio::net::UnixStream) -> Result<bool, String> {
    let peer = stream.peer_cred().map_err(display)?;
    // SAFETY: geteuid has no preconditions and does not dereference memory.
    Ok(peer.uid() == unsafe { libc::geteuid() })
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
#[allow(unsafe_code)]
fn same_user_peer(stream: &tokio::net::UnixStream) -> Result<bool, String> {
    use std::os::fd::AsRawFd as _;
    let mut uid = 0;
    let mut gid = 0;
    // SAFETY: the stream owns a valid descriptor and both output pointers are valid.
    let status = unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) };
    if status != 0 {
        return Err(display(io::Error::last_os_error()));
    }
    // SAFETY: geteuid has no preconditions and does not dereference memory.
    Ok(uid == unsafe { libc::geteuid() })
}

#[cfg(windows)]
async fn serve_windows_control(
    pipe_path: String,
    control: Arc<AsyncMutex<impl ControlRequestDispatcher + 'static>>,
    shutdown_sender: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
    #[cfg(test)] accepted: Arc<tokio::sync::Notify>,
) -> Result<(), String> {
    let mut first = true;
    let mut connections = tokio::task::JoinSet::new();
    let result = 'result: loop {
        let server = create_current_user_pipe(&pipe_path, first);
        let server = match server {
            Ok(server) => server,
            Err(error) => break Err(display(error)),
        };
        first = false;
        let connected = loop {
            tokio::select! {
                connected = server.connect() => break connected,
                completed = connections.join_next(), if !connections.is_empty() => {
                    let _ = completed;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break 'result Ok(());
                    }
                }
            }
        };
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
    };
    let _ = shutdown_sender.send(true);
    while connections.join_next().await.is_some() {}
    result
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn create_current_user_pipe(
    pipe_path: &str,
    first: bool,
) -> Result<tokio::net::windows::named_pipe::NamedPipeServer, io::Error> {
    use std::ffi::c_void;
    use std::ptr;
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW;
    use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;

    // Protected DACL: the object owner and LocalSystem only. Remote clients
    // are rejected separately by the pipe mode below.
    let mut sddl = "D:P(A;;GA;;;OW)(A;;GA;;;SY)"
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut descriptor = ptr::null_mut::<c_void>();
    // SAFETY: `sddl` is a live NUL-terminated UTF-16 string and the output
    // pointer is valid for the duration of the call.
    let converted = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_mut_ptr(),
            1,
            &mut descriptor,
            ptr::null_mut(),
        )
    };
    if converted == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: u32::try_from(std::mem::size_of::<SECURITY_ATTRIBUTES>()).unwrap_or(u32::MAX),
        lpSecurityDescriptor: descriptor,
        bInheritHandle: 0,
    };
    let mut options = tokio::net::windows::named_pipe::ServerOptions::new();
    options
        .first_pipe_instance(first)
        .reject_remote_clients(true);
    // SAFETY: `attributes` and its descriptor remain valid until `create`
    // returns; the kernel copies the descriptor into the new object.
    let result = unsafe {
        options.create_with_security_attributes_raw(
            pipe_path,
            (&mut attributes as *mut SECURITY_ATTRIBUTES).cast(),
        )
    };
    // SAFETY: the descriptor was allocated by the conversion API above and is
    // no longer referenced after pipe creation returns.
    unsafe {
        LocalFree(descriptor.cast());
    }
    result
}

async fn handle_control_connection<S>(
    mut stream: S,
    control: Arc<AsyncMutex<impl ControlRequestDispatcher + 'static>>,
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
    let write = async {
        stream.write_all(&encoded).await.map_err(display)?;
        stream.write_all(b"\n").await.map_err(display)?;
        stream.flush().await.map_err(display)
    };
    tokio::pin!(write);
    tokio::select! {
        result = &mut write => result,
        changed = shutdown.changed() => {
            let _ = changed;
            tokio::time::timeout(CONTROL_RESPONSE_DRAIN_GRACE, &mut write)
                .await
                .unwrap_or(Ok(()))
        }
    }
}

async fn dispatch_control_request(
    control: &Arc<AsyncMutex<impl ControlRequestDispatcher>>,
    request: ControlRequest,
) -> Result<Value, String> {
    if request.version != 1 {
        return Err("unsupported Acyclic control request".to_owned());
    }
    let mut control = control.lock().await;
    control.dispatch_request(request).await
}

trait ControlRequestDispatcher: Send {
    fn dispatch_request(
        &mut self,
        request: ControlRequest,
    ) -> impl std::future::Future<Output = Result<Value, String>> + Send;
}

struct ServiceControl {
    data: PathBuf,
    fs: LocalFs,
    store: LocalCoreStateStore,
    shared_roots: SharedRootRegistry,
    sessions: BTreeMap<String, ControlPlane>,
    identity: String,
    upgrade: Arc<tokio::sync::Notify>,
}

impl ServiceControl {
    async fn open(data: PathBuf) -> Result<Self, String> {
        fs::create_dir_all(data.join("sessions")).map_err(display)?;
        let fs = LocalFs::local(LocalOptions::new(data.join("filesystem")))
            .await
            .map_err(display)?;
        let store = LocalCoreStateStore::new(data.join("core-state"));
        let shared_roots = SharedRootRegistry::default();
        let mut service = Self {
            data,
            fs,
            store,
            shared_roots: shared_roots.clone(),
            sessions: BTreeMap::new(),
            identity: service_identity()?,
            upgrade: Arc::new(tokio::sync::Notify::new()),
        };
        let entries = fs::read_dir(service.data.join("sessions")).map_err(display)?;
        for entry in entries {
            let entry = entry.map_err(display)?;
            if !entry.file_type().map_err(display)?.is_dir() {
                continue;
            }
            let control = ControlPlane::open_with(
                entry.path(),
                service.data.clone(),
                service.fs.clone(),
                service.store.clone(),
                shared_roots.clone(),
            )
            .await?;
            if !control.state.root_session_id.is_empty() {
                service
                    .sessions
                    .insert(control.state.root_session_id.clone(), control);
            }
        }
        Ok(service)
    }

    fn session_directory(&self, session_id: &str) -> PathBuf {
        self.data
            .join("sessions")
            .join(blake3::hash(session_id.as_bytes()).to_hex().as_str())
    }

    async fn create_session(&mut self, session_id: &str) -> Result<&mut ControlPlane, String> {
        if !self.sessions.contains_key(session_id) {
            let control = ControlPlane::open_with(
                self.session_directory(session_id),
                self.data.clone(),
                self.fs.clone(),
                self.store.clone(),
                self.shared_roots.clone(),
            )
            .await?;
            self.sessions.insert(session_id.to_owned(), control);
        }
        self.sessions
            .get_mut(session_id)
            .ok_or_else(|| "Acyclic session registration failed".to_owned())
    }

    fn session_for_cwd(&self, cwd: &Path) -> Result<Option<String>, String> {
        let cwd = cwd.canonicalize().map_err(display)?;
        let mut matches = Vec::new();
        for (session_id, control) in &self.sessions {
            let mut score = control
                .state
                .roots
                .values()
                .filter_map(|root| root.path.canonicalize().ok())
                .filter(|root| cwd.starts_with(root))
                .map(|root| root.components().count())
                .max();
            for route in control.state.routes.values().filter(|route| !route.stopped) {
                if let Ok(mount_path) = route.mount_path.canonicalize()
                    && cwd.starts_with(&mount_path)
                {
                    score = Some(score.unwrap_or(0).max(mount_path.components().count()));
                }
            }
            if let Some(score) = score {
                matches.push((score, session_id.clone()));
            }
        }
        matches.sort_by(|left, right| right.cmp(left));
        let Some((score, session_id)) = matches.first() else {
            return Ok(None);
        };
        if matches
            .get(1)
            .is_some_and(|candidate| candidate.0 == *score)
        {
            return Err(
                "cwd belongs to multiple Acyclic sessions; run inside a managed child mount"
                    .to_owned(),
            );
        }
        Ok(Some(session_id.clone()))
    }

    fn child_mount_for_cwd(&self, cwd: &Path) -> Result<Option<(String, String)>, String> {
        let cwd = cwd.canonicalize().map_err(display)?;
        let mut matches = Vec::new();
        for (session_id, control) in &self.sessions {
            for route in control.state.routes.values().filter(|route| !route.stopped) {
                if let Ok(mount_path) = route.mount_path.canonicalize()
                    && cwd.starts_with(&mount_path)
                {
                    matches.push((
                        mount_path.components().count(),
                        session_id.clone(),
                        route.agent_id.clone(),
                    ));
                }
            }
        }
        matches.sort_by(|left, right| right.cmp(left));
        let Some((depth, session_id, agent_id)) = matches.first() else {
            return Ok(None);
        };
        if matches
            .get(1)
            .is_some_and(|candidate| candidate.0 == *depth)
        {
            return Err("cwd belongs to multiple managed child mounts".to_owned());
        }
        Ok(Some((session_id.clone(), agent_id.clone())))
    }

    async fn session_for_cwd_or_register(&mut self, cwd: &Path) -> Result<String, String> {
        if let Some(session_id) = self.session_for_cwd(cwd)? {
            return Ok(session_id);
        }
        let canonical = cwd.canonicalize().map_err(display)?;
        let session_id = format!(
            "cwd:{}",
            blake3::hash(canonical.as_os_str().to_string_lossy().as_bytes()).to_hex()
        );
        let control = self.create_session(&session_id).await?;
        control
            .session_start(json!({
                "session_id": session_id,
                "cwd": canonical,
                "host": "cli"
            }))
            .await?;
        Ok(session_id)
    }

    async fn shutdown(&mut self) -> Result<(), String> {
        let sessions = std::mem::take(&mut self.sessions);
        let mut first_error = None;
        for (_, mut control) in sessions {
            if let Err(error) = control.shutdown().await {
                first_error.get_or_insert(error);
            }
        }
        self.shared_roots.prune().await;
        first_error.map_or(Ok(()), Err)
    }

    async fn dispatch_native_hook(
        &mut self,
        host: &str,
        event: &str,
        input: Value,
        cwd: &Path,
    ) -> Result<Value, String> {
        let session_id = hook_string(&input, "session_id", "sessionId")?;
        if matches!(event, "SessionStart" | "sessionStart") {
            let root = hook_root_path(&input).unwrap_or_else(|| cwd.to_path_buf());
            if let Some((owner_session, owner_agent)) = self.child_mount_for_cwd(&root)? {
                return Err(format!(
                    "session {session_id} cannot attach a managed child mount owned by {owner_agent} in session {owner_session} as a physical root"
                ));
            }
            let control = self.create_session(&session_id).await?;
            let output = control
                .session_start(json!({"session_id":session_id,"cwd":root,"host":host}))
                .await?;
            if host == "cursor" {
                return Ok(json!({
                    "additional_context": output
                        .pointer("/hookSpecificOutput/additionalContext")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                }));
            }
            return Ok(output);
        }
        if matches!(event, "SessionEnd" | "sessionEnd") {
            let mut control = self
                .sessions
                .remove(&session_id)
                .ok_or_else(|| "Acyclic native hook session is not registered".to_owned())?;
            let shutdown = control.shutdown().await;
            drop(control);
            self.shared_roots.prune().await;
            shutdown?;
            return Ok(json!({}));
        }
        let control = self
            .sessions
            .get_mut(&session_id)
            .ok_or_else(|| "Acyclic native hook session is not registered".to_owned())?;
        let hook_cwd = hook_path(&input, "cwd").unwrap_or_else(|| cwd.to_path_buf());
        let (caller, route, root_id) = control.route_root_from_cwd(&hook_cwd)?;
        let turn_id = route.as_ref().map_or_else(
            || format!("{host}:root:{session_id}"),
            |route| route.turn_id.clone(),
        );
        if caller == control.state.root_agent_id {
            control.remember_root_turn(turn_id.clone());
        }
        match event {
            "UserPromptSubmit" | "userPromptSubmitted" => control.user_prompt(json!({
                "session_id": session_id,
                "turn_id": hook_optional_string(&input, "prompt_id", "promptId").unwrap_or(turn_id)
            })),
            "PreToolUse" | "preToolUse" => {
                let mut tool_name = hook_string(&input, "tool_name", "toolName")?;
                if host == "copilot" && tool_name == "task" {
                    tool_name = "Agent".to_owned();
                }
                let tool_input = input
                    .get("tool_input")
                    .or_else(|| input.get("toolArgs"))
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let tool_use_id =
                    native_hook_tool_id(&input, &session_id, &tool_name, &tool_input)?;
                let output = control
                    .pre_tool(json!({
                        "session_id": session_id,
                        "turn_id": turn_id,
                        "tool_use_id": tool_use_id,
                        "tool_name": tool_name,
                        "tool_input": tool_input,
                        "_caller_root_id": hex::encode(root_id.into_bytes())
                    }))
                    .await?;
                if host == "copilot" {
                    let updated = output.pointer("/hookSpecificOutput/updatedInput").cloned();
                    Ok(json!({
                        "permissionDecision": "allow",
                        "permissionDecisionReason": "Acyclic routed this tool to its workspace context",
                        "modifiedArgs": updated
                    }))
                } else {
                    Ok(output)
                }
            }
            "PostToolUse" | "postToolUse" => {
                let mut tool_name = hook_string(&input, "tool_name", "toolName")?;
                if host == "copilot" && tool_name == "task" {
                    tool_name = "Agent".to_owned();
                }
                let tool_input = input
                    .get("tool_input")
                    .or_else(|| input.get("toolArgs"))
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let tool_use_id =
                    native_hook_tool_id(&input, &session_id, &tool_name, &tool_input)?;
                control
                    .post_tool(json!({
                        "session_id": session_id,
                        "turn_id": turn_id,
                        "tool_use_id": tool_use_id,
                        "tool_name": tool_name
                    }))
                    .await
            }
            "SubagentStart" | "subagentStart" => {
                let agent_id = hook_optional_string(&input, "agent_id", "agentId")
                    .or_else(|| hook_optional_string(&input, "agent_name", "agentName"))
                    .ok_or_else(|| "subagent start lacks a stable agent identity".to_owned())?;
                control
                    .subagent_start(json!({
                        "session_id": session_id,
                        "turn_id": format!("{host}:agent:{agent_id}"),
                        "agent_id": agent_id,
                        "_authenticated_parent_agent_id": caller,
                        "agent_type": hook_optional_string(&input, "agent_type", "agentType").unwrap_or_else(|| "subagent".to_owned())
                    }))
                    .await
            }
            "SubagentStop" | "subagentStop" => {
                let agent_id = hook_optional_string(&input, "agent_id", "agentId")
                    .or_else(|| hook_optional_string(&input, "agent_name", "agentName"))
                    .ok_or_else(|| "subagent stop lacks a stable agent identity".to_owned())?;
                control
                    .subagent_stop(json!({
                        "session_id": session_id,
                        "turn_id": format!("{host}:agent:{agent_id}"),
                        "agent_id": agent_id
                    }))
                    .await
            }
            "Stop" | "agentStop" => Ok(json!({})),
            _ => Err(format!("unsupported {host} lifecycle event '{event}'")),
        }
    }
}

fn hook_optional_string(input: &Value, snake: &str, camel: &str) -> Option<String> {
    input
        .get(snake)
        .or_else(|| input.get(camel))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn hook_string(input: &Value, snake: &str, camel: &str) -> Result<String, String> {
    hook_optional_string(input, snake, camel)
        .ok_or_else(|| format!("native hook is missing '{snake}'"))
}

fn hook_path(input: &Value, field: &str) -> Option<PathBuf> {
    input.get(field).and_then(Value::as_str).map(PathBuf::from)
}

fn hook_root_path(input: &Value) -> Option<PathBuf> {
    hook_path(input, "cwd").or_else(|| {
        input
            .get("workspace_roots")
            .and_then(Value::as_array)
            .and_then(|roots| roots.first())
            .and_then(Value::as_str)
            .map(PathBuf::from)
    })
}

fn native_tool_id(session_id: &str, tool_name: &str, tool_input: &Value) -> String {
    let mut digest = blake3::Hasher::new();
    digest.update(session_id.as_bytes());
    digest.update(&[0]);
    digest.update(tool_name.as_bytes());
    digest.update(&[0]);
    if let Ok(encoded) = serde_json::to_vec(tool_input) {
        digest.update(&encoded);
    }
    format!("native:{}", digest.finalize().to_hex())
}

fn native_hook_tool_id(
    input: &Value,
    session_id: &str,
    tool_name: &str,
    tool_input: &Value,
) -> Result<String, String> {
    if let Some(id) = hook_optional_string(input, "tool_use_id", "toolUseId") {
        return Ok(id);
    }
    if is_filesystem_tool(tool_name) {
        return Err(format!(
            "filesystem tool '{tool_name}' is missing a stable tool-use identity"
        ));
    }
    Ok(native_tool_id(session_id, tool_name, tool_input))
}

impl ControlRequestDispatcher for ServiceControl {
    async fn dispatch_request(&mut self, request: ControlRequest) -> Result<Value, String> {
        if matches!(request.command, ControlCommand::Ping) {
            return Ok(json!({ "identity": self.identity }));
        }
        if matches!(request.command, ControlCommand::Upgrade) {
            let expected = request
                .arguments
                .get("identity")
                .and_then(Value::as_str)
                .ok_or_else(|| "upgrade request is missing the active identity".to_owned())?;
            if expected != self.identity {
                return Err("upgrade request targets a different service binary".to_owned());
            }
            let notify = Arc::clone(&self.upgrade);
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                notify.notify_waiters();
            });
            return Ok(json!({ "draining": true }));
        }
        if matches!(request.command, ControlCommand::Doctor) {
            let routes = self
                .sessions
                .values()
                .map(|session| session.state.routes.len())
                .sum();
            let leases = self
                .sessions
                .values()
                .map(|session| session.state.leases.len())
                .sum();
            let pending_recovery = self.sessions.values().any(|session| {
                !session.state.pending_discards.is_empty() || !session.state.pending.is_empty()
            });
            return doctor_report(
                &self.data,
                &self.identity,
                self.sessions.len(),
                routes,
                leases,
                pending_recovery,
            );
        }
        if matches!(request.command, ControlCommand::Hook) {
            let (host, event) = request
                .name
                .split_once(':')
                .ok_or_else(|| "native hook request has no host/event pair".to_owned())?;
            return self
                .dispatch_native_hook(host, event, request.arguments, &request.cwd)
                .await;
        }
        let session_id = self.session_for_cwd_or_register(&request.cwd).await?;
        let control = self
            .sessions
            .get_mut(&session_id)
            .ok_or_else(|| "Acyclic session is not registered".to_owned())?;
        dispatch_plane_request(control, request).await
    }
}

impl ControlRequestDispatcher for ControlPlane {
    async fn dispatch_request(&mut self, request: ControlRequest) -> Result<Value, String> {
        dispatch_plane_request(self, request).await
    }
}

async fn dispatch_plane_request(
    control: &mut ControlPlane,
    request: ControlRequest,
) -> Result<Value, String> {
    match request.command {
        ControlCommand::Ping => Ok(json!({})),
        ControlCommand::Doctor => doctor_report(
            &control.config_root,
            &service_identity()?,
            1,
            control.state.routes.len(),
            control.state.leases.len(),
            !control.state.pending_discards.is_empty() || !control.state.pending.is_empty(),
        ),
        ControlCommand::Agents => {
            let (caller, _) = control.route_from_cwd(&request.cwd)?;
            control.agents_status(&caller).await
        }
        ControlCommand::Discard => {
            let (caller, _) = control.route_from_cwd(&request.cwd)?;
            let reference = request
                .argv
                .first()
                .and_then(|value| value.strip_prefix("agents/"))
                .ok_or_else(|| "discard requires agents/<ref>".to_owned())?;
            control
                .agent_discard_as(&caller, json!({"agent": reference}))
                .await
        }
        ControlCommand::Git => {
            if request.argv.is_empty() {
                return Err("acyclic git requires a Git-style subcommand".to_owned());
            }
            let (caller, route, root_id) = control.route_root_from_cwd(&request.cwd)?;
            if request.argv.as_slice() == ["merge", "--continue"] {
                return control.agent_merge_transition(&caller, false).await;
            }
            if request.argv.as_slice() == ["merge", "--abort"] {
                return control.agent_merge_transition(&caller, true).await;
            }
            if request.argv.first().is_some_and(|command| command == "add")
                && let Some(operation_id) = control
                    .pending_conflict_for_parent(control.context_for_agent(&caller)?)
                    .await?
            {
                control.sync_agent(&caller).await?;
                let paths = request
                    .argv
                    .iter()
                    .skip(1)
                    .filter(|argument| argument.as_str() != "--")
                    .filter(|argument| !argument.starts_with('-'))
                    .cloned()
                    .collect::<Vec<_>>();
                let paths = if paths.is_empty()
                    && request
                        .argv
                        .iter()
                        .skip(1)
                        .any(|argument| argument == "-A" || argument == "--all")
                {
                    vec![".".to_owned()]
                } else {
                    paths
                };
                if paths.is_empty() {
                    return Err("acyclic git add requires a conflicted path or -A".to_owned());
                }
                control
                    .publication_coordinator()
                    .await?
                    .declare_conflicted_paths(
                        operation_id,
                        control.context_for_agent(&caller)?,
                        root_id,
                        &paths,
                    )
                    .await
                    .map_err(display)?;
                return Ok(json!({"status":"resolved","automaticStaging":true}));
            }
            if request
                .argv
                .first()
                .is_some_and(|command| command == "merge")
                && let Some(reference) = request
                    .argv
                    .get(1)
                    .and_then(|value| value.strip_prefix("agents/"))
            {
                return control
                    .agent_merge_as(&caller, json!({"agent": reference}))
                    .await;
            }
            if request
                .argv
                .first()
                .is_some_and(|command| command == "diff")
                && let Some(reference) = request
                    .argv
                    .get(1)
                    .and_then(|value| value.strip_prefix("agents/"))
            {
                let mut paths = request.argv.get(2..).unwrap_or_default();
                if paths.first().is_some_and(|argument| argument == "--") {
                    paths = paths.get(1..).unwrap_or_default();
                }
                if paths.len() > 1 {
                    return Err("acyclic git diff agents/<ref> accepts at most one path".to_owned());
                }
                return control
                    .agent_changes_as(
                        &caller,
                        json!({
                            "agent": reference,
                            "path": paths.first(),
                            "_caller_root_id": hex::encode(root_id.into_bytes())
                        }),
                    )
                    .await;
            }
            match route {
                Some(route) => {
                    control
                        .git_tool(&route.agent_id, &route, root_id, request.argv)
                        .await
                }
                None => control.root_git_tool(root_id, request.argv).await,
            }
        }
        ControlCommand::Upgrade | ControlCommand::Hook => {
            Err("control command is invalid for a workspace session".to_owned())
        }
    }
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

/// Unified Acyclic CLI, local service, Codex hook bridge, and installer.
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if is_foreground_cli_invocation() {
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(display)?
            .block_on(run())
            .map_err(display);
        result.map_err(io::Error::other)?;
        return Ok(());
    }
    let result = std::thread::Builder::new()
        .name("acyclic".to_owned())
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

fn is_foreground_cli_invocation() -> bool {
    let mut arguments = env::args_os().skip(1);
    let mut command = arguments.next();
    if command.as_deref() == Some(std::ffi::OsStr::new("-C")) {
        let _ = arguments.next();
        command = arguments.next();
    }
    command.as_deref().is_some_and(|command| {
        matches!(
            command.to_str(),
            Some(
                "git"
                    | "agents"
                    | "doctor"
                    | "discard"
                    | "__hook"
                    | "install"
                    | "uninstall"
                    | "__service-drain"
                    | "__installer-rename"
                    | "--help"
                    | "-h"
                    | "--version"
                    | "-V"
            )
        )
    })
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut arguments = env::args().skip(1).collect::<Vec<_>>();
    let mut cwd = env::current_dir()?;
    if arguments.first().is_some_and(|argument| argument == "-C") {
        if arguments.len() < 2 {
            return Err(io::Error::other("acyclic -C requires a path").into());
        }
        cwd = PathBuf::from(arguments.remove(1)).canonicalize()?;
        arguments.remove(0);
    }
    if arguments
        .first()
        .is_some_and(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        println!(
            "Acyclic {}\n\nUsage: acyclic [COMMAND]\n\nCommands:\n  install HOST       Install host integration\n  uninstall HOST [--purge]\n                       Remove integration; preserve durable state unless purged\n  doctor [--json]    Diagnose the release installation\n  git ARGS...        Run Git compatibility commands\n  agents [--json]    List recursive agent workspace status\n  discard WORKSPACE  Discard a child workspace\n  mcp                 Serve MCP over standard input/output",
            env!("CARGO_PKG_VERSION")
        );
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| matches!(argument.as_str(), "--version" | "-V"))
    {
        println!("acyclic {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "__installer-rename")
    {
        let [_, from, to, mode] = arguments.as_slice() else {
            return Err(io::Error::other(
                "acyclic __installer-rename requires FROM TO replace|no-replace",
            )
            .into());
        };
        let mode = match mode.as_str() {
            "replace" => RenameMode::Replace,
            "no-replace" => RenameMode::NoReplace,
            _ => return Err(io::Error::other("invalid installer rename mode").into()),
        };
        durable_rename(Path::new(from), Path::new(to), mode)?;
        return Ok(());
    }
    if let Some(command) = arguments.first().map(String::as_str)
        && matches!(command, "git" | "agents" | "doctor" | "discard")
    {
        let data = default_data_directory();
        let request = ControlRequest {
            version: 1,
            command: match command {
                "git" => ControlCommand::Git,
                "agents" => ControlCommand::Agents,
                "doctor" => ControlCommand::Doctor,
                "discard" => ControlCommand::Discard,
                _ => unreachable!(),
            },
            cwd,
            argv: arguments.get(1..).unwrap_or_default().to_vec(),
            name: String::new(),
            arguments: Value::Null,
        };
        let response = send_cli_control_request(&data, &request)
            .await
            .map_err(io::Error::other)?;
        let json_output = arguments
            .get(1)
            .is_some_and(|argument| argument == "--json");
        let exit_code = if json_output {
            println!("{}", serde_json::to_string_pretty(&response)?);
            if command == "doctor" && response.get("ok").and_then(Value::as_bool) != Some(true) {
                1
            } else {
                0
            }
        } else if command == "doctor" {
            print_doctor_response(&response).map_err(io::Error::other)?
        } else {
            print_cli_response(response).map_err(io::Error::other)?
        };
        if exit_code != 0 {
            std::process::exit(exit_code);
        }
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "__hook")
    {
        let [_, host, event] = arguments.as_slice() else {
            return Err(io::Error::other("acyclic __hook requires a host and event").into());
        };
        if !matches!(
            host.as_str(),
            "codex" | "claude-code" | "copilot" | "cursor"
        ) {
            return Err(io::Error::other("unsupported native hook host").into());
        }
        let mut input = Vec::new();
        io::stdin()
            .take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64)
            .read_to_end(&mut input)?;
        if input.len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
            return Err(io::Error::other("native hook input exceeds the 4 MiB bound").into());
        }
        let input: Value = serde_json::from_slice(&input)?;
        let data = default_data_directory();
        ensure_service(&data).await.map_err(io::Error::other)?;
        let response = send_control_request(
            &data,
            &ControlRequest {
                version: 1,
                command: ControlCommand::Hook,
                cwd,
                argv: Vec::new(),
                name: format!("{host}:{event}"),
                arguments: input,
            },
        )
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
        serde_json::to_writer(io::stdout().lock(), &response)?;
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "install")
    {
        return install_command(arguments.get(1..).unwrap_or_default())
            .map_err(io::Error::other)
            .map_err(Into::into);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "uninstall")
    {
        return uninstall_command(arguments.get(1..).unwrap_or_default())
            .await
            .map_err(io::Error::other)
            .map_err(Into::into);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "__service-drain")
    {
        return drain_service(&default_data_directory())
            .await
            .map_err(io::Error::other)
            .map_err(Into::into);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "__service")
    {
        return run_service(default_data_directory())
            .await
            .map_err(io::Error::other)
            .map_err(Into::into);
    }
    let commandless = arguments
        .first()
        .is_some_and(|argument| argument == "__mcp-commandless");
    let data = default_data_directory();
    ensure_service(&data).await.map_err(io::Error::other)?;
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_rpc_proxy(&data, stdin.lock(), stdout.lock(), commandless)
        .await
        .map_err(io::Error::other)?;
    Ok(())
}

fn default_data_directory() -> PathBuf {
    #[cfg(windows)]
    if let Some(root) = env::var_os("LOCALAPPDATA") {
        return PathBuf::from(root).join("Acyclic").join("state-v2");
    }
    #[cfg(not(windows))]
    if let Some(root) = env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(root).join("acyclic").join("state-v2");
    }
    #[cfg(not(windows))]
    if let Some(root) = env::var_os("HOME") {
        return PathBuf::from(root)
            .join(".local")
            .join("state")
            .join("acyclic")
            .join("state-v2");
    }
    env::temp_dir().join("acyclic-state-v2")
}

fn service_identity() -> Result<String, String> {
    let executable = env::current_exe().map_err(display)?;
    let value = format!(
        "{}:{}:{}",
        env!("CARGO_PKG_VERSION"),
        executable.display(),
        sha256_file(&executable)?,
    );
    Ok(blake3::hash(value.as_bytes()).to_hex().to_string())
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = fs::File::open(path).map_err(display)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(display)?;
        if read == 0 {
            break;
        }
        let chunk = buffer
            .get(..read)
            .ok_or_else(|| "binary hash read exceeded its buffer".to_owned())?;
        hasher.update(chunk);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn doctor_check(name: &str, status: &str, detail: impl Into<String>) -> Value {
    json!({"name": name, "status": status, "detail": detail.into()})
}

fn valid_codex_mcp_manifest(document: &Value) -> bool {
    let Some(root) = document.as_object() else {
        return false;
    };
    let Some(servers) = root.get("mcpServers").and_then(Value::as_object) else {
        return false;
    };
    let Some(server) = servers.get("acyclic").and_then(Value::as_object) else {
        return false;
    };
    root.len() == 1
        && servers.len() == 1
        && server.len() == 4
        && server.get("command").and_then(Value::as_str) == Some("node")
        && server
            .get("args")
            .and_then(Value::as_array)
            .is_some_and(|arguments| match arguments.as_slice() {
                [launcher, mode] => {
                    launcher.as_str() == Some("./bin/acyclic.js")
                        && mode.as_str() == Some("__mcp-commandless")
                }
                _ => false,
            })
        && server.get("cwd").and_then(Value::as_str) == Some(".")
        && server
            .get("default_tools_approval_mode")
            .and_then(Value::as_str)
            == Some("prompt")
}

fn codex_json(arguments: &[&str]) -> Result<Value, String> {
    let output = std::process::Command::new("codex")
        .args(arguments)
        .output()
        .map_err(|error| format!("cannot run Codex plugin manager: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "Codex plugin manager exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(display)
}

#[allow(clippy::too_many_arguments)]
fn doctor_report(
    data: &Path,
    identity: &str,
    sessions: usize,
    routes: usize,
    leases: usize,
    pending_recovery: bool,
) -> Result<Value, String> {
    let executable = env::current_exe().map_err(display)?;
    let executable_sha256 = sha256_file(&executable)?;
    let mut checks = Vec::new();
    let binary_identity_path = executable
        .parent()
        .unwrap_or(Path::new("."))
        .join("installed-binary.json");
    let binary_identity = fs::read(&binary_identity_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    match binary_identity {
        Some(ref installed)
            if installed.get("version").and_then(Value::as_str)
                == Some(env!("CARGO_PKG_VERSION"))
                && installed.get("sha256").and_then(Value::as_str)
                    == Some(executable_sha256.as_str()) =>
        {
            checks.push(doctor_check(
                "binary",
                "pass",
                format!("{} sha256:{executable_sha256}", env!("CARGO_PKG_VERSION")),
            ));
        }
        Some(_) => checks.push(doctor_check(
            "binary",
            "fail",
            "installed binary bytes or version differ from installed-binary.json",
        )),
        None => checks.push(doctor_check(
            "binary",
            "warn",
            format!(
                "no packaged binary identity beside {}; development builds are not release-certified",
                executable.display()
            ),
        )),
    }

    let root = plugin_root();
    match &root {
        Ok(root) => {
            let package_version = fs::read(root.join("package.json"))
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|value| {
                    value
                        .get("version")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                });
            let status = if package_version.as_deref() == Some(env!("CARGO_PKG_VERSION")) {
                "pass"
            } else {
                "fail"
            };
            checks.push(doctor_check(
                "package-cache",
                status,
                format!(
                    "plugin={} binary={}",
                    package_version.unwrap_or_else(|| "unknown".to_owned()),
                    env!("CARGO_PKG_VERSION")
                ),
            ));
            let marketplace = root.join(".agents/plugins/marketplace.json");
            let marketplace_owned = fs::read(&marketplace)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_some_and(|value| {
                    value.get("name").and_then(Value::as_str) == Some("acyclic")
                        && value
                            .get("plugins")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .any(|plugin| {
                                plugin.get("name").and_then(Value::as_str) == Some("acyclic")
                            })
                });
            checks.push(doctor_check(
                "marketplace",
                if marketplace_owned { "pass" } else { "fail" },
                marketplace.display().to_string(),
            ));
            let configured = codex_json(&["plugin", "marketplace", "list", "--json"])
                .ok()
                .and_then(|value| value.get("marketplaces").and_then(Value::as_array).cloned())
                .into_iter()
                .flatten()
                .find(|entry| entry.get("name").and_then(Value::as_str) == Some("acyclic"));
            let installed = codex_json(&[
                "plugin",
                "list",
                "--marketplace",
                "acyclic",
                "--available",
                "--json",
            ])
            .ok()
            .and_then(|value| value.get("installed").and_then(Value::as_array).cloned())
            .into_iter()
            .flatten()
            .find(|entry| entry.get("pluginId").and_then(Value::as_str) == Some("acyclic@acyclic"));
            let configured_root = configured
                .as_ref()
                .and_then(|entry| entry.get("root"))
                .and_then(Value::as_str)
                .unwrap_or("unconfigured");
            let configured_owned = Path::new(configured_root)
                .canonicalize()
                .ok()
                .zip(root.canonicalize().ok())
                .is_some_and(|(configured, packaged)| configured == packaged);
            let installed_version = installed
                .as_ref()
                .and_then(|entry| entry.get("version"))
                .and_then(Value::as_str)
                .unwrap_or("not-installed");
            checks.push(doctor_check(
                "codex-install",
                if configured_owned
                    && installed.as_ref().is_some_and(|entry| {
                        entry.get("installed").and_then(Value::as_bool) == Some(true)
                            && entry.get("enabled").and_then(Value::as_bool) == Some(true)
                            && entry.get("version").and_then(Value::as_str)
                                == Some(env!("CARGO_PKG_VERSION"))
                    })
                {
                    "pass"
                } else {
                    "fail"
                },
                format!("marketplace={configured_root} plugin-version={installed_version}"),
            ));
            let hooks = root.join("hooks/hooks.json");
            let mcp = root.join(".mcp.json");
            let hooks_valid = fs::read(&hooks)
                .ok()
                .filter(|bytes| bytes.len() <= 1024 * 1024)
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_some_and(|document| {
                    [
                        "SessionStart",
                        "UserPromptSubmit",
                        "PreToolUse",
                        "PostToolUse",
                        "SubagentStart",
                        "SubagentStop",
                        "SessionEnd",
                    ]
                    .iter()
                    .all(|event| {
                        document
                            .pointer(&format!("/hooks/{event}"))
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .flat_map(|entry| {
                                entry
                                    .get("hooks")
                                    .and_then(Value::as_array)
                                    .into_iter()
                                    .flatten()
                            })
                            .filter_map(|hook| hook.get("command").and_then(Value::as_str))
                            .any(|command| {
                                command.contains("${PLUGIN_ROOT}/bin/acyclic.js")
                                    && command.contains(&format!("__hook codex {event}"))
                            })
                    })
                });
            let mcp_valid = fs::read(&mcp)
                .ok()
                .filter(|bytes| bytes.len() <= 1024 * 1024)
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_some_and(|document| valid_codex_mcp_manifest(&document));
            checks.push(doctor_check(
                "hooks",
                if hooks_valid { "pass" } else { "fail" },
                hooks.display().to_string(),
            ));
            checks.push(doctor_check(
                "agent-command",
                if mcp_valid { "pass" } else { "fail" },
                if mcp_valid {
                    "bundled MCP dispatcher; shell fallback is the npm bin"
                } else {
                    "bundled MCP dispatcher is missing"
                },
            ));
        }
        Err(error) => {
            checks.push(doctor_check("package-cache", "fail", error.clone()));
            checks.push(doctor_check("marketplace", "fail", error.clone()));
            checks.push(doctor_check("codex-install", "fail", error.clone()));
            checks.push(doctor_check("hooks", "fail", error.clone()));
            checks.push(doctor_check("agent-command", "fail", error));
        }
    }

    checks.push(doctor_check(
        "service",
        "pass",
        format!("identity={identity} sessions={sessions} routes={routes} leases={leases}"),
    ));
    let native = acyclic_fs::probe_native_mount();
    checks.push(doctor_check(
        "mount-backend",
        if native.available && native.writable {
            "pass"
        } else {
            "warn"
        },
        format!(
            "kind={:?} available={} writable={} provider-io-observable={}{}",
            native.kind,
            native.available,
            native.writable,
            native.provider_process_io_observable,
            native
                .unavailable_reason
                .as_deref()
                .map(|reason| format!(" reason={reason}"))
                .unwrap_or_default()
        ),
    ));
    checks.push(doctor_check(
        "persistent-state",
        if pending_recovery { "warn" } else { "pass" },
        if pending_recovery {
            "durable recovery work is pending"
        } else {
            "durable state loaded with no pending adapter recovery"
        },
    ));
    let cli_on_path = env::var_os("PATH").is_some_and(|paths| {
        env::split_paths(&paths).any(|directory| {
            ["acyclic", "acyclic.exe", "acyclic.cmd", "acyclic.ps1"]
                .iter()
                .any(|name| directory.join(name).is_file())
        })
    });
    checks.push(doctor_check(
        "cli-path",
        if cli_on_path { "pass" } else { "warn" },
        if cli_on_path {
            "acyclic shell launcher is on PATH"
        } else {
            "plugin MCP tool is available; install the npm package for shell PATH access"
        },
    ));
    let receipt_path = data.join("certification").join(format!(
        "native-mount-{}-{}.json",
        env::consts::OS,
        env::consts::ARCH
    ));
    let certified = fs::read(&receipt_path)
        .ok()
        .filter(|bytes| bytes.len() <= 1024 * 1024)
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .is_some_and(|receipt| {
            receipt.get("schema").and_then(Value::as_str)
                == Some("acyclic-native-mount-qualification-v1")
                && receipt.get("os").and_then(Value::as_str) == Some(env::consts::OS)
                && receipt.get("arch").and_then(Value::as_str) == Some(env::consts::ARCH)
                && receipt.get("passed").and_then(Value::as_bool) == Some(true)
        });
    checks.push(doctor_check(
        "platform-certification",
        if certified { "pass" } else { "fail" },
        if certified {
            receipt_path.display().to_string()
        } else {
            format!(
                "no passing live qualification receipt at {}",
                receipt_path.display()
            )
        },
    ));
    let ok = checks
        .iter()
        .all(|check| check.get("status").and_then(Value::as_str) != Some("fail"));
    Ok(json!({
        "schemaVersion": 1,
        "ok": ok,
        "certified": certified,
        "version": env!("CARGO_PKG_VERSION"),
        "platform": {"os": env::consts::OS, "arch": env::consts::ARCH},
        "checks": checks,
    }))
}

struct ServiceLock {
    _file: fs::File,
}

struct ServiceIdentityMarker {
    path: PathBuf,
}

impl ServiceIdentityMarker {
    fn create(data: &Path, identity: &str) -> Result<Self, String> {
        let path = data.join("service.identity");
        fs::write(&path, identity).map_err(display)?;
        Ok(Self { path })
    }
}

impl Drop for ServiceIdentityMarker {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn acquire_service_lock(data: &Path) -> Result<Option<ServiceLock>, String> {
    fs::create_dir_all(data).map_err(display)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(data, fs::Permissions::from_mode(0o700)).map_err(display)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(data.join("service.lock"))
        .map_err(display)?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(ServiceLock { _file: file })),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(display(error)),
    }
}

async fn run_service(data: PathBuf) -> Result<(), String> {
    let Some(_lock) = acquire_service_lock(&data)? else {
        return Ok(());
    };
    #[cfg(unix)]
    match fs::remove_file(data.join("service.sock")) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(display(error)),
    }
    let service = ServiceControl::open(data.clone()).await?;
    let upgrade = Arc::clone(&service.upgrade);
    let control = Arc::new(AsyncMutex::new(service));
    let endpoint = start_control_endpoint(Arc::clone(&control), &data).await?;
    let _identity_marker = ServiceIdentityMarker::create(&data, &service_identity()?)?;
    tokio::select! {
        signal = tokio::signal::ctrl_c() => signal.map_err(display)?,
        () = upgrade.notified() => {},
    }
    endpoint.shutdown().await?;
    let mut control = Arc::try_unwrap(control)
        .map_err(|_| "control service retained an active request during shutdown".to_owned())?
        .into_inner();
    control.shutdown().await
}

async fn ensure_service(data: &Path) -> Result<(), String> {
    let ping = ControlRequest {
        version: 1,
        command: ControlCommand::Ping,
        cwd: env::current_dir().map_err(display)?,
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    };
    let identity = service_identity()?;
    if let Ok(active) = send_control_request(data, &ping).await {
        if active.get("identity").and_then(Value::as_str) == Some(identity.as_str()) {
            return Ok(());
        }
        let active_identity = active
            .get("identity")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                "an older Acyclic service cannot hand off cleanly; stop it once, then retry"
                    .to_owned()
            })?;
        let upgrade = ControlRequest {
            version: 1,
            command: ControlCommand::Upgrade,
            cwd: env::current_dir().map_err(display)?,
            argv: Vec::new(),
            name: String::new(),
            arguments: json!({ "identity": active_identity }),
        };
        send_control_request(data, &upgrade)
            .await
            .map_err(|error| error.to_string())?;
        for _ in 0..100 {
            if send_control_request(data, &ping).await.is_err() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
    fs::create_dir_all(data).map_err(display)?;
    let mut command = std::process::Command::new(env::current_exe().map_err(display)?);
    command
        .arg("__service")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt as _;
        command.creation_flags(0x0800_0000);
    }
    command.spawn().map_err(display)?;
    let mut last = String::new();
    for _ in 0..100 {
        match send_control_request(data, &ping).await {
            Ok(active)
                if active.get("identity").and_then(Value::as_str) == Some(identity.as_str()) =>
            {
                return Ok(());
            }
            Ok(_) => last = "the previous Acyclic service is still draining".to_owned(),
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Err(format!("Acyclic service did not become ready: {last}"))
}

async fn send_cli_control_request(data: &Path, request: &ControlRequest) -> Result<Value, String> {
    let identity = service_identity()?;
    let marker_matches =
        fs::read_to_string(data.join("service.identity")).is_ok_and(|active| active == identity);
    if !marker_matches {
        ensure_service(data).await?;
    }
    match send_control_request(data, request).await {
        Ok(response) => Ok(response),
        Err(ControlRequestError::Transport(_)) if marker_matches => {
            ensure_service(data).await?;
            send_control_request(data, request)
                .await
                .map_err(|error| error.to_string())
        }
        Err(error) => Err(error.to_string()),
    }
}

fn control_request_from_argv(
    mut cwd: PathBuf,
    mut argv: Vec<String>,
) -> Result<ControlRequest, String> {
    if argv.first().is_some_and(|argument| argument == "-C") {
        if argv.len() < 2 {
            return Err("acyclic -C requires a path".to_owned());
        }
        cwd = PathBuf::from(argv.remove(1))
            .canonicalize()
            .map_err(display)?;
        argv.remove(0);
    }
    let command = match argv.first().map(String::as_str) {
        Some("git") => ControlCommand::Git,
        Some("agents") => ControlCommand::Agents,
        Some("doctor") => ControlCommand::Doctor,
        Some("discard") => ControlCommand::Discard,
        _ => return Err("unsupported Acyclic command".to_owned()),
    };
    Ok(ControlRequest {
        version: 1,
        command,
        cwd,
        argv: argv.get(1..).unwrap_or_default().to_vec(),
        name: String::new(),
        arguments: Value::Null,
    })
}

async fn run_rpc_proxy(
    data: &Path,
    reader: impl BufRead,
    mut writer: impl Write,
    commandless: bool,
) -> Result<(), String> {
    for line in reader.lines() {
        let line = line.map_err(display)?;
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = serde_json::from_str(&line).map_err(display)?;
        let Some(id) = request.get("id").cloned() else {
            continue;
        };
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let response = match method {
            "initialize" => {
                json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"acyclic","version":env!("CARGO_PKG_VERSION")}}})
            }
            "ping" => json!({"jsonrpc":"2.0","id":id,"result":{}}),
            "tools/list" => {
                json!({"jsonrpc":"2.0","id":id,"result":{"tools":public_tools(commandless)}})
            }
            "tools/call" => {
                let params = request.get("params").cloned().unwrap_or(Value::Null);
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                let arguments = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let forwarded = if commandless && name == "acyclic" {
                    let argv = arguments
                        .get("argv")
                        .and_then(Value::as_array)
                        .ok_or_else(|| "acyclic requires an argv string array".to_owned())?
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .map(str::to_owned)
                                .ok_or_else(|| "acyclic argv entries must be strings".to_owned())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    control_request_from_argv(env::current_dir().map_err(display)?, argv)?
                } else {
                    return Err("this Acyclic MCP endpoint does not expose that tool".to_owned());
                };
                match send_control_request(data, &forwarded).await {
                    Ok(result) => {
                        json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":serde_json::to_string(&result).map_err(display)?}]}})
                    }
                    Err(error) => {
                        json!({"jsonrpc":"2.0","id":id,"result":{"isError":true,"content":[{"type":"text","text":error.to_string()}]}})
                    }
                }
            }
            _ => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}})
            }
        };
        serde_json::to_writer(&mut writer, &response).map_err(display)?;
        writer.write_all(b"\n").map_err(display)?;
        writer.flush().map_err(display)?;
    }
    Ok(())
}

#[derive(Debug)]
enum ControlRequestError {
    Transport(String),
    Response(String),
}

impl std::fmt::Display for ControlRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(message) | Self::Response(message) => formatter.write_str(message),
        }
    }
}

async fn send_control_request(
    data: &Path,
    request: &ControlRequest,
) -> Result<Value, ControlRequestError> {
    let mut encoded = serde_json::to_vec(request)
        .map_err(|error| ControlRequestError::Transport(error.to_string()))?;
    encoded.push(b'\n');
    #[cfg(unix)]
    let stream = tokio::net::UnixStream::connect(data.join("service.sock"))
        .await
        .map_err(|error| {
            ControlRequestError::Transport(format!("Acyclic service is not running: {error}"))
        })?;
    #[cfg(windows)]
    let stream = {
        let pipe = format!(
            r"\\.\pipe\acyclic-{}",
            short_hash(data.as_os_str().to_string_lossy().as_bytes())
        );
        let mut last = None;
        let mut connected = None;
        for _ in 0..50 {
            match tokio::net::windows::named_pipe::ClientOptions::new().open(&pipe) {
                Ok(client) => {
                    connected = Some(client);
                    break;
                }
                Err(error) => {
                    last = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            }
        }
        connected.ok_or_else(|| {
            ControlRequestError::Transport(format!(
                "Acyclic service is not running: {}",
                last.map_or_else(
                    || "unknown connection failure".to_owned(),
                    |error| error.to_string()
                )
            ))
        })?
    };
    let mut stream = stream;
    stream
        .write_all(&encoded)
        .await
        .map_err(|error| ControlRequestError::Transport(error.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|error| ControlRequestError::Transport(error.to_string()))?;
    let mut response = Vec::new();
    let mut reader = BufReader::new(stream).take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64);
    reader
        .read_until(b'\n', &mut response)
        .await
        .map_err(|error| ControlRequestError::Transport(error.to_string()))?;
    if response.len() > MAXIMUM_CONTROL_MESSAGE_BYTES || response.last() != Some(&b'\n') {
        return Err(ControlRequestError::Transport(
            "invalid response from Acyclic service".to_owned(),
        ));
    }
    response.pop();
    let response: Value = serde_json::from_slice(&response)
        .map_err(|error| ControlRequestError::Transport(error.to_string()))?;
    if response.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    } else {
        Err(ControlRequestError::Response(
            response
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("Acyclic service request failed")
                .to_owned(),
        ))
    }
}

fn print_cli_response(response: Value) -> Result<i32, String> {
    if let Ok(output) = serde_json::from_value::<GitCommandOutput>(response.clone()) {
        return print_git_output(output);
    }
    if let Some(agents) = response.get("agents").and_then(Value::as_array) {
        for agent in agents {
            let reference = agent
                .get("ref")
                .and_then(Value::as_str)
                .unwrap_or("agents/?");
            let state = agent
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let parent = agent.get("parent").and_then(Value::as_str).unwrap_or("?");
            let depth = agent
                .get("depth")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let leases = agent
                .get("activeLeases")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let conflicts = agent
                .get("conflicts")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let work = agent
                .get("workRemaining")
                .and_then(Value::as_u64)
                .map_or_else(|| "busy".to_owned(), |work| work.to_string());
            println!(
                "{reference}\t{state}\tparent={parent}\tdepth={depth}\tleases={leases}\tconflicts={conflicts}\twork={work}"
            );
            if let Some(paths) = agent.get("changedPaths").and_then(Value::as_array) {
                for path in paths.iter().filter_map(Value::as_str) {
                    println!("  changed={path}");
                }
            }
            if let Some(roots) = agent.get("roots").and_then(Value::as_array) {
                for root in roots {
                    println!(
                        "  root={} route={} generation={} published={}{}",
                        root.get("id").and_then(Value::as_str).unwrap_or("?"),
                        root.get("route").and_then(Value::as_str).unwrap_or("?"),
                        root.get("generation")
                            .and_then(Value::as_str)
                            .unwrap_or("?"),
                        root.get("publishedGeneration")
                            .and_then(Value::as_str)
                            .unwrap_or("never"),
                        if root.get("unpublished").and_then(Value::as_bool) == Some(true) {
                            " pending"
                        } else {
                            ""
                        }
                    );
                }
            }
        }
        return Ok(0);
    }
    match response {
        Value::Null => Ok(0),
        Value::String(text) => {
            println!("{text}");
            Ok(0)
        }
        value => {
            println!("{}", serde_json::to_string_pretty(&value).map_err(display)?);
            Ok(0)
        }
    }
}

fn print_doctor_response(response: &Value) -> Result<i32, String> {
    let checks = response
        .get("checks")
        .and_then(Value::as_array)
        .ok_or_else(|| "doctor response has no checks".to_owned())?;
    for check in checks {
        let status = check
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("fail");
        let name = check
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let detail = check.get("detail").and_then(Value::as_str).unwrap_or("");
        println!("{status:<4} {name:<24} {detail}");
    }
    let ok = response.get("ok").and_then(Value::as_bool) == Some(true);
    println!("{}", if ok { "ready" } else { "not ready" });
    Ok(i32::from(!ok))
}

fn print_git_output(output: GitCommandOutput) -> Result<i32, String> {
    let mut exit_code = 0;
    match output {
        GitCommandOutput::NoOp => {}
        GitCommandOutput::Text(text) => println!("{text}"),
        GitCommandOutput::Paths(paths) => {
            for path in paths {
                println!("{path}");
            }
        }
        GitCommandOutput::Status(status) => {
            println!("On branch {}", status.branch);
            match status.dirty {
                GitDirtyState::Clean => println!("nothing to commit, working tree clean"),
                GitDirtyState::Dirty => {
                    println!("Changes to be committed:");
                    println!("  (all eligible workspace changes are staged automatically)");
                }
                GitDirtyState::Unknown => println!(
                    "working tree contains unresolved lazy paths; run an exact command to scan"
                ),
            }
        }
        GitCommandOutput::Branches { current, branches } => {
            for branch in branches {
                println!(
                    "{} {}",
                    if branch.name == current { "*" } else { " " },
                    branch.name
                );
            }
        }
        GitCommandOutput::Tags(tags) => {
            for tag in tags.keys() {
                println!("{tag}");
            }
        }
        GitCommandOutput::Commits(commits) => {
            for commit in commits {
                println!("commit {}", commit.id.to_hex());
                println!("Author: {}", commit.author);
                println!();
                println!("    {}", commit.message.replace('\n', "\n    "));
                println!();
            }
        }
        GitCommandOutput::Committed(commit) => {
            let id = commit.id.to_hex();
            println!("[{}] {}", id.get(..12).unwrap_or(&id), commit.message);
        }
        GitCommandOutput::Filesystem(GitFilesystemResult::Data { kind, value })
            if kind == "check-ignore" =>
        {
            let paths = value
                .get("paths")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>();
            exit_code = i32::from(paths.is_empty());
            for path in paths {
                println!("{path}");
            }
        }
        GitCommandOutput::Filesystem(GitFilesystemResult::Data { kind, value })
            if kind == "grep" =>
        {
            let matches = value
                .get("matches")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            exit_code = i32::from(matches.is_empty());
            for matched in matches {
                if let (Some(path), Some(text)) = (
                    matched.get("path").and_then(Value::as_str),
                    matched.get("text").and_then(Value::as_str),
                ) {
                    println!("{path}:{text}");
                }
            }
        }
        GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) => {
            println!("{}", serde_json::to_string_pretty(&value).map_err(display)?);
        }
        other => println!("{}", serde_json::to_string_pretty(&other).map_err(display)?),
    }
    Ok(exit_code)
}

fn install_command(arguments: &[String]) -> Result<(), String> {
    let project = arguments.iter().any(|argument| argument == "--project");
    let host = arguments
        .iter()
        .find(|argument| argument.as_str() != "--project")
        .ok_or_else(|| "install requires a host or --detected".to_owned())?;
    if host == "--detected" {
        let mut installed = 0;
        for (binary, target) in [
            ("codex", "codex"),
            ("claude", "claude-code"),
            ("cursor", "cursor"),
            ("copilot", "copilot"),
            ("opencode", "opencode"),
        ] {
            if executable_on_path(binary) {
                install_host(target, false)?;
                installed += 1;
            }
        }
        if installed == 0 {
            return Err("no supported local coding-agent executable was detected".to_owned());
        }
        return Ok(());
    }
    install_host(host, project)
}

async fn uninstall_command(arguments: &[String]) -> Result<(), String> {
    let purge = arguments.iter().any(|argument| argument == "--purge");
    let host = arguments
        .iter()
        .find(|argument| argument.as_str() != "--purge")
        .ok_or_else(|| "uninstall requires a host".to_owned())?;
    let data = default_data_directory();
    drain_service(&data).await?;
    uninstall_host(host)?;
    if purge {
        purge_durable_state(&data).await?;
        println!("purged Acyclic durable state");
    } else {
        println!("preserved Acyclic durable state; pass --purge to remove it explicitly");
    }
    Ok(())
}

async fn purge_durable_state(data: &Path) -> Result<(), String> {
    if !data.exists() {
        return Ok(());
    }
    drain_service(data).await?;
    let parent = data
        .parent()
        .ok_or_else(|| "durable state path has no parent".to_owned())?;
    remove_tree_checked(parent, data)
}

async fn drain_service(data: &Path) -> Result<(), String> {
    let ping = ControlRequest {
        version: 1,
        command: ControlCommand::Ping,
        cwd: env::current_dir().map_err(display)?,
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    };
    match send_control_request(data, &ping).await {
        Ok(active) => {
            let identity = active
                .get("identity")
                .and_then(Value::as_str)
                .ok_or_else(|| "cannot safely drain an unidentified Acyclic service".to_owned())?;
            let request = ControlRequest {
                version: 1,
                command: ControlCommand::Upgrade,
                cwd: env::current_dir().map_err(display)?,
                argv: Vec::new(),
                name: String::new(),
                arguments: json!({"identity": identity}),
            };
            send_control_request(data, &request)
                .await
                .map_err(|error| error.to_string())?;
            for _ in 0..250 {
                if send_control_request(data, &ping).await.is_err() {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(
                "Acyclic service did not drain; durable state and executable were preserved"
                    .to_owned(),
            )
        }
        Err(ControlRequestError::Transport(_)) => Ok(()),
        Err(error) => Err(format!(
            "cannot safely identify the Acyclic service: {error}"
        )),
    }
}

fn install_host(host: &str, project: bool) -> Result<(), String> {
    if host == "codex" && project {
        return Err("Codex plugin installation is per-user; omit --project".to_owned());
    }
    if project
        && !matches!(
            host,
            "codex" | "claude-code" | "cursor" | "opencode" | "vscode"
        )
    {
        return Err(format!("'{host}' has no project-scoped adapter"));
    }
    if host == "codex" && !project {
        return install_codex_plugin();
    }
    let executable = env::current_exe().map_err(display)?;
    if host == "claude-code" {
        let path = install_claude_hooks(&executable, project)?;
        println!(
            "installed claude-code lifecycle hooks at {}",
            path.display()
        );
        return Ok(());
    }
    if host == "copilot" {
        if project {
            return Err("Copilot CLI installation is per-user; omit --project".to_owned());
        }
        let path = install_copilot_hooks(&executable)?;
        println!(
            "installed Copilot CLI lifecycle hooks at {}",
            path.display()
        );
        println!(
            "capability: provisional because Copilot subagentStart omits a stable child id and general-purpose subagents emit no lifecycle events"
        );
        return Ok(());
    }
    if host == "cursor" {
        let path = install_cursor_hooks(&executable, project)?;
        println!(
            "installed Cursor root lifecycle hooks at {}",
            path.display()
        );
        println!("capability: provisional/CLI-only; native subagent isolation is not claimed");
        return Ok(());
    }
    if host == "opencode" {
        let path = install_opencode_guidance(project)?;
        println!("installed OpenCode guidance at {}", path.display());
        println!("capability: provisional/CLI-only; native subagent isolation is not claimed");
        return Ok(());
    }
    let (path, shape) = host_config(host, project)?;
    merge_mcp_config(&path, shape, &executable)?;
    println!(
        "installed {host} {} adapter at {}",
        if project { "project" } else { "per-user" },
        path.display()
    );
    Ok(())
}

fn uninstall_host(host: &str) -> Result<(), String> {
    if host == "codex" {
        let manifest_root = plugin_root()?;
        let ownership_path = codex_ownership_path();
        let ownership = read_codex_ownership(&ownership_path)?;
        let marketplaces = codex_json(&["plugin", "marketplace", "list", "--json"])?;
        let configured = marketplaces
            .get("marketplaces")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|entry| entry.get("name").and_then(Value::as_str) == Some("acyclic"));
        if let Some(configured) = configured {
            let configured_root = configured
                .get("root")
                .and_then(Value::as_str)
                .ok_or_else(|| "configured Acyclic marketplace has no root".to_owned())?;
            let owned = Path::new(configured_root)
                .canonicalize()
                .ok()
                .zip(manifest_root.canonicalize().ok())
                .is_some_and(|(configured, package)| configured == package);
            if !owned {
                return Err(format!(
                    "refusing to remove the Acyclic marketplace owned by {configured_root}"
                ));
            }
        }
        let plugin_was_installed = installed_codex_plugin()?.is_some();
        let Some(ownership) = ownership else {
            if configured.is_some() || plugin_was_installed {
                return Err(
                    "refusing to remove Codex integration without an Acyclic ownership record"
                        .to_owned(),
                );
            }
            return Ok(());
        };
        let owned_root = ownership
            .marketplace_root
            .canonicalize()
            .ok()
            .zip(manifest_root.canonicalize().ok())
            .is_some_and(|(owned, package)| owned == package);
        if ownership.version != 1 || !owned_root {
            return Err(
                "Codex integration ownership does not match this Acyclic package".to_owned(),
            );
        }
        if plugin_was_installed && !ownership.plugin_was_installed {
            run_codex(&["plugin", "remove", "acyclic@acyclic", "--json"])?;
        }
        if ownership.added_marketplace
            && configured.is_some()
            && run_codex(&["plugin", "marketplace", "remove", "acyclic"]).is_err()
        {
            if plugin_was_installed && !ownership.plugin_was_installed {
                let _ = run_codex(&["plugin", "add", "acyclic@acyclic", "--json"]);
            }
            return Err(
                "Codex marketplace removal failed; the prior plugin was restored".to_owned(),
            );
        }
        fs::remove_file(&ownership_path).map_err(display)?;
        println!("removed Acyclic-owned Codex integration and preserved prior state");
        return Ok(());
    }
    if host == "claude-code" {
        for project in [false, true] {
            remove_claude_hooks(project)?;
        }
        println!("removed Acyclic-owned Claude Code lifecycle hooks");
        return Ok(());
    }
    if host == "copilot" {
        let path = home_directory()?.join(".copilot/hooks/acyclic.json");
        remove_copilot_hooks_at(&path)?;
        println!(
            "removed Acyclic-owned Copilot CLI hooks from {}",
            path.display()
        );
        return Ok(());
    }
    if host == "cursor" {
        for project in [false, true] {
            remove_cursor_hooks(project)?;
        }
        println!("removed Acyclic-owned Cursor hooks");
        return Ok(());
    }
    if host == "opencode" {
        for project in [false, true] {
            remove_opencode_guidance(project)?;
        }
        println!("removed Acyclic-owned OpenCode guidance");
        return Ok(());
    }
    if host == "vscode" {
        let (path, shape) = host_config(host, true)?;
        remove_mcp_config(&path, shape)?;
        println!(
            "removed Acyclic-owned {host} configuration from {}",
            path.display()
        );
        return Ok(());
    }
    let (path, shape) = host_config(host, false)?;
    remove_mcp_config(&path, shape)?;
    println!(
        "removed Acyclic-owned {host} configuration from {}",
        path.display()
    );
    Ok(())
}

fn remove_copilot_hooks_at(path: &Path) -> Result<(), String> {
    let ownership_path = mcp_ownership_path(path, "__whole-file__");
    let Some(ownership) = read_mcp_ownership(&ownership_path)? else {
        if path.exists() {
            return Err(format!(
                "refusing to remove Copilot hooks without an Acyclic ownership record from {}",
                path.display()
            ));
        }
        return Ok(());
    };
    let current = fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    if ownership.version != 1
        || ownership.config_path != path
        || ownership.section != "__whole-file__"
        || current.as_ref() != Some(&ownership.installed)
    {
        return Err(format!(
            "the Acyclic Copilot hooks at {} were modified; leaving them untouched",
            path.display()
        ));
    }
    match ownership.prior {
        Some(prior) => write_json_with_backup(path, &prior)?,
        None => match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(display(error)),
        },
    }
    fs::remove_file(&ownership_path).map_err(display)?;
    Ok(())
}

fn install_claude_hooks(executable: &Path, project: bool) -> Result<PathBuf, String> {
    let path = if project {
        env::current_dir()
            .map_err(display)?
            .join(".claude/settings.json")
    } else {
        home_directory()?.join(".claude/settings.json")
    };
    let mut document = read_json_object(&path)?;
    let hooks = object_entry(&mut document, "hooks")?;
    for event in [
        "SessionStart",
        "UserPromptSubmit",
        "PreToolUse",
        "PostToolUse",
        "SubagentStart",
        "SubagentStop",
        "SessionEnd",
    ] {
        let command = format!(
            "{} __hook claude-code {event}",
            shell_command_path(executable)
        );
        let entries = hooks
            .entry(event.to_owned())
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or_else(|| format!("Claude Code hooks.{event} must be an array"))?;
        entries.retain(|entry| {
            !entry
                .get("hooks")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .any(|hook| {
                    hook.get("command")
                        .and_then(Value::as_str)
                        .is_some_and(|command| command.contains("__hook claude-code"))
                })
        });
        let mut group = serde_json::Map::new();
        if matches!(event, "PreToolUse" | "PostToolUse") {
            group.insert("matcher".to_owned(), Value::String(".*".to_owned()));
        }
        group.insert(
            "hooks".to_owned(),
            json!([{
                "type": "command",
                "command": command,
                "timeout": 120,
                "statusMessage": "Acyclic is routing the workspace"
            }]),
        );
        entries.push(Value::Object(group));
    }
    write_json_with_backup(&path, &Value::Object(document))?;
    Ok(path)
}

fn remove_claude_hooks(project: bool) -> Result<(), String> {
    let path = if project {
        env::current_dir()
            .map_err(display)?
            .join(".claude/settings.json")
    } else {
        home_directory()?.join(".claude/settings.json")
    };
    if !path.exists() {
        return Ok(());
    }
    let mut document = read_json_object(&path)?;
    if let Some(hooks) = document.get_mut("hooks").and_then(Value::as_object_mut) {
        for entries in hooks.values_mut() {
            if let Some(entries) = entries.as_array_mut() {
                entries.retain(|entry| {
                    !entry
                        .get("hooks")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .any(|hook| {
                            hook.get("command")
                                .and_then(Value::as_str)
                                .is_some_and(|command| command.contains("__hook claude-code"))
                        })
                });
            }
        }
        hooks.retain(|_, entries| entries.as_array().is_none_or(|entries| !entries.is_empty()));
    }
    write_json_with_backup(&path, &Value::Object(document))
}

fn install_copilot_hooks(executable: &Path) -> Result<PathBuf, String> {
    let path = home_directory()?.join(".copilot/hooks/acyclic.json");
    install_copilot_hooks_at(executable, &path)?;
    Ok(path)
}

fn install_copilot_hooks_at(executable: &Path, path: &Path) -> Result<(), String> {
    let command = executable.to_string_lossy();
    let mut hooks = serde_json::Map::new();
    for event in [
        "sessionStart",
        "preToolUse",
        "postToolUse",
        "subagentStart",
        "subagentStop",
        "sessionEnd",
    ] {
        hooks.insert(
            event.to_owned(),
            json!([{
                "type": "command",
                "exec": command,
                "args": ["__hook", "copilot", event],
                "timeout": 120
            }]),
        );
    }
    let installed = json!({"version":1,"hooks":Value::Object(hooks)});
    let ownership_path = mcp_ownership_path(path, "__whole-file__");
    let current = match fs::read(path) {
        Ok(bytes) => Some(serde_json::from_slice::<Value>(&bytes).map_err(|error| {
            format!(
                "refusing to overwrite invalid Copilot hooks at {}: {error}",
                path.display()
            )
        })?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(display(error)),
    };
    let prior = match read_mcp_ownership(&ownership_path)? {
        Some(ownership)
            if ownership.version == 1
                && ownership.config_path == path
                && ownership.section == "__whole-file__"
                && ownership.installed == installed
                && (current == ownership.prior || current == Some(installed.clone())) =>
        {
            ownership.prior
        }
        Some(_) => {
            return Err(format!(
                "Copilot hook ownership at {} is inconsistent; refusing to overwrite it",
                path.display()
            ));
        }
        None => current,
    };
    write_mcp_ownership(
        &ownership_path,
        &McpConfigOwnership {
            version: 1,
            config_path: path.to_path_buf(),
            section: "__whole-file__".to_owned(),
            prior,
            installed: installed.clone(),
        },
    )?;
    write_json_with_backup(path, &installed)?;
    Ok(())
}

fn cursor_hooks_path(project: bool) -> Result<PathBuf, String> {
    Ok(if project {
        env::current_dir()
            .map_err(display)?
            .join(".cursor/hooks.json")
    } else {
        home_directory()?.join(".cursor/hooks.json")
    })
}

fn install_cursor_hooks(executable: &Path, project: bool) -> Result<PathBuf, String> {
    let path = cursor_hooks_path(project)?;
    let mut document = read_json_object(&path)?;
    document.insert("version".to_owned(), json!(1));
    let hooks = object_entry(&mut document, "hooks")?;
    for event in ["sessionStart", "sessionEnd"] {
        let entries = hooks
            .entry(event.to_owned())
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or_else(|| format!("Cursor hooks.{event} must be an array"))?;
        entries.retain(|entry| {
            !entry
                .get("command")
                .and_then(Value::as_str)
                .is_some_and(|command| command.contains("__hook cursor"))
        });
        entries.push(json!({
            "command": format!("{} __hook cursor {event}", shell_command_path(executable)),
            "timeout": 120
        }));
    }
    write_json_with_backup(&path, &Value::Object(document))?;
    Ok(path)
}

fn remove_cursor_hooks(project: bool) -> Result<(), String> {
    let path = cursor_hooks_path(project)?;
    if !path.exists() {
        return Ok(());
    }
    let mut document = read_json_object(&path)?;
    if let Some(hooks) = document.get_mut("hooks").and_then(Value::as_object_mut) {
        for entries in hooks.values_mut() {
            if let Some(entries) = entries.as_array_mut() {
                entries.retain(|entry| {
                    !entry
                        .get("command")
                        .and_then(Value::as_str)
                        .is_some_and(|command| command.contains("__hook cursor"))
                });
            }
        }
        hooks.retain(|_, entries| entries.as_array().is_none_or(|entries| !entries.is_empty()));
    }
    write_json_with_backup(&path, &Value::Object(document))
}

const OPENCODE_GUIDANCE_START: &str = "<!-- acyclic:start -->";
const OPENCODE_GUIDANCE_END: &str = "<!-- acyclic:end -->";

fn opencode_guidance_path(project: bool) -> Result<PathBuf, String> {
    Ok(if project {
        env::current_dir().map_err(display)?.join("AGENTS.md")
    } else {
        config_directory()?.join("opencode/AGENTS.md")
    })
}

fn install_opencode_guidance(project: bool) -> Result<PathBuf, String> {
    let path = opencode_guidance_path(project)?;
    let existing = match fs::read_to_string(&path) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(display(error)),
    };
    let without = remove_marked_block(&existing, OPENCODE_GUIDANCE_START, OPENCODE_GUIDANCE_END);
    let block = format!(
        "{OPENCODE_GUIDANCE_START}\n{}\n{OPENCODE_GUIDANCE_END}",
        guidance_for("opencode")
    );
    let next = if without.trim().is_empty() {
        format!("{block}\n")
    } else {
        format!("{}\n\n{block}\n", without.trim_end())
    };
    write_text_with_backup(&path, &next)?;
    Ok(path)
}

fn remove_opencode_guidance(project: bool) -> Result<(), String> {
    let path = opencode_guidance_path(project)?;
    let existing = match fs::read_to_string(&path) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(display(error)),
    };
    let next = remove_marked_block(&existing, OPENCODE_GUIDANCE_START, OPENCODE_GUIDANCE_END);
    write_text_with_backup(&path, next.trim_end())
}

fn remove_marked_block(input: &str, start: &str, end: &str) -> String {
    let Some((before, marked)) = input.split_once(start) else {
        return input.to_owned();
    };
    let Some((_, after)) = marked.split_once(end) else {
        return input.to_owned();
    };
    let mut output = String::with_capacity(input.len());
    output.push_str(before);
    output.push_str(after);
    output.trim().to_owned()
}

fn shell_command_path(path: &Path) -> String {
    let path = path.to_string_lossy();
    if cfg!(windows) {
        format!("\"{}\"", path.replace('"', "\"\""))
    } else {
        format!("'{}'", path.replace('\'', "'\\''"))
    }
}

#[derive(Clone, Copy)]
enum McpConfigShape {
    Standard,
    VsCode,
}

#[derive(Deserialize, Serialize)]
struct McpConfigOwnership {
    version: u32,
    config_path: PathBuf,
    section: String,
    prior: Option<Value>,
    installed: Value,
}

#[derive(Deserialize, Serialize)]
struct CodexPluginOwnership {
    version: u32,
    marketplace_root: PathBuf,
    added_marketplace: bool,
    plugin_was_installed: bool,
}

fn codex_ownership_path() -> PathBuf {
    default_data_directory().join("install-ownership/codex-plugin.json")
}

fn read_codex_ownership(path: &Path) -> Result<Option<CodexPluginOwnership>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(display),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

fn write_codex_ownership(path: &Path, ownership: &CodexPluginOwnership) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(ownership).map_err(display)?,
    )
    .map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
    )
    .map_err(display)
}

fn mcp_ownership_path(path: &Path, section: &str) -> PathBuf {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-mcp-config-ownership-v1\0");
    hasher.update(path.as_os_str().to_string_lossy().as_bytes());
    hasher.update(&[0]);
    hasher.update(section.as_bytes());
    #[cfg(test)]
    return path.with_extension(format!(
        "{}.acyclic-owner",
        hasher
            .finalize()
            .to_hex()
            .chars()
            .take(16)
            .collect::<String>()
    ));
    #[cfg(not(test))]
    default_data_directory()
        .join("install-ownership")
        .join(format!(
            "{}.json",
            hasher
                .finalize()
                .to_hex()
                .chars()
                .take(32)
                .collect::<String>()
        ))
}

fn read_mcp_ownership(path: &Path) -> Result<Option<McpConfigOwnership>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(display),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

fn write_mcp_ownership(path: &Path, ownership: &McpConfigOwnership) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(ownership).map_err(display)?,
    )
    .map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
    )
    .map_err(display)
}

fn host_config(host: &str, project: bool) -> Result<(PathBuf, McpConfigShape), String> {
    let cwd = env::current_dir().map_err(display)?;
    let home = home_directory()?;
    let result = match (host, project) {
        ("vscode", true) => (cwd.join(".mcp.json"), McpConfigShape::VsCode),
        ("vscode", false) => {
            return Err("VS Code installation is project-scoped; pass --project".to_owned());
        }
        ("copilot", false) => (
            home.join(".copilot/mcp-config.json"),
            McpConfigShape::Standard,
        ),
        ("claude-desktop", false) => (claude_desktop_config()?, McpConfigShape::Standard),
        ("copilot-cloud", _) => {
            return Err("Copilot cloud is job-local; install the binary in the job and invoke its MCP bridge explicitly".to_owned());
        }
        ("pydantic-ai", _) => {
            return Err("Pydantic AI requires explicit SDK integration and has no host configuration to mutate".to_owned());
        }
        _ => return Err(format!("unsupported Acyclic host '{host}'")),
    };
    Ok(result)
}

fn merge_mcp_config(path: &Path, shape: McpConfigShape, executable: &Path) -> Result<(), String> {
    let mut document = read_json_object(path)?;
    let command = executable.to_string_lossy().into_owned();
    let (section, installed) = match shape {
        McpConfigShape::Standard => (
            "mcpServers",
            json!({"command":command,"args":["__mcp-commandless"]}),
        ),
        McpConfigShape::VsCode => (
            "servers",
            json!({"type":"stdio","command":command,"args":["__mcp-commandless"]}),
        ),
    };
    let ownership_path = mcp_ownership_path(path, section);
    let entries = object_entry(&mut document, section)?;
    let current = entries.get("acyclic").cloned();
    let ownership = match read_mcp_ownership(&ownership_path)? {
        Some(ownership)
            if ownership.version == 1
                && ownership.config_path == path
                && ownership.section == section
                && ownership.installed == installed
                && (current == ownership.prior || current == Some(installed.clone())) =>
        {
            ownership
        }
        Some(_) => {
            return Err(format!(
                "Acyclic does not own the current '{}' entry in {}; resolve it manually",
                section,
                path.display()
            ));
        }
        None if current.is_none() => McpConfigOwnership {
            version: 1,
            config_path: path.to_path_buf(),
            section: section.to_owned(),
            prior: None,
            installed: installed.clone(),
        },
        None => {
            return Err(format!(
                "refusing to replace the unowned 'acyclic' entry in {}",
                path.display()
            ));
        }
    };
    write_mcp_ownership(&ownership_path, &ownership)?;
    entries.insert("acyclic".to_owned(), installed);
    if let Err(error) = write_json_with_backup(path, &Value::Object(document)) {
        return Err(format!("{error}; ownership intent retained for recovery"));
    }
    Ok(())
}

fn remove_mcp_config(path: &Path, shape: McpConfigShape) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let mut document = read_json_object(path)?;
    let key = match shape {
        McpConfigShape::Standard => "mcpServers",
        McpConfigShape::VsCode => "servers",
    };
    let ownership_path = mcp_ownership_path(path, key);
    let Some(ownership) = read_mcp_ownership(&ownership_path)? else {
        return Err(format!(
            "refusing to remove an Acyclic entry without an ownership record from {}",
            path.display()
        ));
    };
    if ownership.version != 1
        || ownership.config_path != path
        || ownership.section != key
        || document
            .get(key)
            .and_then(Value::as_object)
            .and_then(|entries| entries.get("acyclic"))
            != Some(&ownership.installed)
    {
        return Err(format!(
            "the Acyclic entry in {} was modified; leaving it untouched",
            path.display()
        ));
    }
    if let Some(Value::Object(entries)) = document.get_mut(key) {
        match ownership.prior {
            Some(prior) => {
                entries.insert("acyclic".to_owned(), prior);
            }
            None => {
                entries.remove("acyclic");
            }
        }
    }
    write_json_with_backup(path, &Value::Object(document))?;
    fs::remove_file(&ownership_path).map_err(display)
}

fn install_codex_plugin() -> Result<(), String> {
    let manifest_root = plugin_root()?;
    let marketplace = manifest_root.join(".agents/plugins/marketplace.json");
    if !marketplace.is_file() {
        return Err(format!(
            "Codex marketplace manifest is missing at {}",
            marketplace.display()
        ));
    }
    let marketplaces = codex_json(&["plugin", "marketplace", "list", "--json"])?;
    let configured = marketplaces
        .get("marketplaces")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|entry| entry.get("name").and_then(Value::as_str) == Some("acyclic"));
    if let Some(configured) = configured {
        let configured_root = configured
            .get("root")
            .and_then(Value::as_str)
            .ok_or_else(|| "configured Acyclic marketplace has no root".to_owned())?;
        let same = Path::new(configured_root)
            .canonicalize()
            .ok()
            .zip(manifest_root.canonicalize().ok())
            .is_some_and(|(configured, package)| configured == package);
        if !same {
            return Err(format!(
                "refusing to replace the Acyclic marketplace owned by {configured_root}"
            ));
        }
    }
    let installed_before = installed_codex_plugin()?;
    let plugin_was_installed = installed_before.is_some();
    if plugin_was_installed && configured.is_none() {
        return Err(
            "Codex reports an installed Acyclic plugin without its marketplace; refusing mutation"
                .to_owned(),
        );
    }
    if let Some(installed) = installed_before.as_ref() {
        validate_existing_codex_plugin(installed)?;
    }
    let added_marketplace = configured.is_none();
    let ownership_path = codex_ownership_path();
    let existing_ownership = read_codex_ownership(&ownership_path)?;
    let ownership = if let Some(ownership) = existing_ownership.as_ref() {
        let same = ownership
            .marketplace_root
            .canonicalize()
            .ok()
            .zip(manifest_root.canonicalize().ok())
            .is_some_and(|(owned, package)| owned == package);
        if ownership.version != 1 || !same {
            return Err("existing Codex integration ownership is inconsistent".to_owned());
        }
        CodexPluginOwnership {
            version: ownership.version,
            marketplace_root: ownership.marketplace_root.clone(),
            added_marketplace: ownership.added_marketplace,
            plugin_was_installed: ownership.plugin_was_installed,
        }
    } else {
        CodexPluginOwnership {
            version: 1,
            marketplace_root: manifest_root.clone(),
            added_marketplace,
            plugin_was_installed,
        }
    };
    write_codex_ownership(&ownership_path, &ownership)?;
    let install = (|| {
        if added_marketplace {
            run_codex(&[
                "plugin",
                "marketplace",
                "add",
                manifest_root.to_string_lossy().as_ref(),
            ])?;
        }
        if !plugin_was_installed {
            run_codex(&["plugin", "add", "acyclic@acyclic", "--json"])?;
        }
        let installed = codex_json(&["plugin", "list", "--marketplace", "acyclic", "--json"])?;
        if !installed
            .get("installed")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|entry| {
                entry.get("pluginId").and_then(Value::as_str) == Some("acyclic@acyclic")
                    && entry.get("version").and_then(Value::as_str)
                        == Some(env!("CARGO_PKG_VERSION"))
                    && entry.get("installed").and_then(Value::as_bool) == Some(true)
                    && entry.get("enabled").and_then(Value::as_bool) == Some(true)
            })
        {
            return Err("Codex did not activate the expected Acyclic release".to_owned());
        }
        Ok(())
    })();
    if let Err(error) = install {
        if !plugin_was_installed {
            let _ = run_codex(&["plugin", "remove", "acyclic@acyclic", "--json"]);
        }
        if added_marketplace {
            let _ = run_codex(&["plugin", "marketplace", "remove", "acyclic"]);
        }
        if existing_ownership.is_none() {
            let _ = fs::remove_file(&ownership_path);
        }
        return Err(format!("Codex plugin installation rolled back: {error}"));
    }
    println!("installed Acyclic through the Codex plugin manager");
    Ok(())
}

fn run_codex(arguments: &[&str]) -> Result<(), String> {
    let status = std::process::Command::new("codex")
        .args(arguments)
        .status()
        .map_err(|error| format!("cannot run Codex plugin manager: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("Codex plugin manager exited with {status}"))
    }
}

fn installed_codex_plugin() -> Result<Option<Value>, String> {
    let plugins = codex_json(&["plugin", "list", "--json"])?;
    let installed = plugins
        .get("installed")
        .and_then(Value::as_array)
        .ok_or_else(|| "Codex plugin list has no installed array".to_owned())?;
    let matches = installed
        .iter()
        .filter(|entry| entry.get("pluginId").and_then(Value::as_str) == Some("acyclic@acyclic"))
        .cloned()
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [plugin] => Ok(Some(plugin.clone())),
        _ => Err("Codex reported duplicate installed Acyclic plugins".to_owned()),
    }
}

fn validate_existing_codex_plugin(installed: &Value) -> Result<(), String> {
    let version = installed
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let enabled = installed
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if version == env!("CARGO_PKG_VERSION") && enabled {
        Ok(())
    } else {
        Err(format!(
            "Codex already has Acyclic {version} (enabled={enabled}); refusing to replace prior plugin state. Update the local package and restart Codex, or explicitly remove the old plugin before retrying"
        ))
    }
}

fn plugin_root() -> Result<PathBuf, String> {
    let executable = env::current_exe().map_err(display)?;
    let installed = installed_plugin_root()?;
    discover_plugin_root(&executable, &installed)
        .ok_or_else(|| "cannot locate the installed Acyclic plugin root".to_owned())
}

fn discover_plugin_root(executable: &Path, installed: &Path) -> Option<PathBuf> {
    executable
        .ancestors()
        .take(4)
        .chain(std::iter::once(installed))
        .find(|root| {
            root.join("plugin.json").is_file()
                && root.join(".agents/plugins/marketplace.json").is_file()
        })
        .map(Path::to_path_buf)
}

fn installed_plugin_root() -> Result<PathBuf, String> {
    #[cfg(windows)]
    {
        env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .map(|path| path.join("Acyclic/plugin"))
            .ok_or_else(|| "LOCALAPPDATA is unavailable".to_owned())
    }
    #[cfg(not(windows))]
    {
        let data = env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or(home_directory()?.join(".local/share"));
        Ok(data.join("acyclic/plugin"))
    }
}

fn read_json_object(path: &Path) -> Result<serde_json::Map<String, Value>, String> {
    if !path.exists() {
        return Ok(serde_json::Map::new());
    }
    serde_json::from_slice::<Value>(&fs::read(path).map_err(display)?)
        .map_err(display)?
        .as_object()
        .cloned()
        .ok_or_else(|| format!("{} must contain a JSON object", path.display()))
}

fn object_entry<'a>(
    document: &'a mut serde_json::Map<String, Value>,
    key: &str,
) -> Result<&'a mut serde_json::Map<String, Value>, String> {
    document
        .entry(key.to_owned())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| format!("configuration field '{key}' must be an object"))
}

fn write_json_with_backup(path: &Path, value: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let backup = path.with_extension(format!(
        "{}.acyclic-backup",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("json")
    ));
    if path.exists() && !backup.exists() {
        fs::copy(path, &backup).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(value).map_err(display)?,
    )
    .map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
    )
    .map_err(display)
}

fn write_text_with_backup(path: &Path, value: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let backup = path.with_extension(format!(
        "{}.acyclic-backup",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("txt")
    ));
    if path.exists() && !backup.exists() {
        fs::copy(path, &backup).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(&temporary, value.as_bytes()).map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
    )
    .map_err(display)
}

fn executable_on_path(name: &str) -> bool {
    env::var_os("PATH").is_some_and(|paths| {
        env::split_paths(&paths).any(|directory| {
            directory.join(name).is_file()
                || cfg!(windows) && directory.join(format!("{name}.exe")).is_file()
        })
    })
}

fn home_directory() -> Result<PathBuf, String> {
    env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(PathBuf::from)
        .ok_or_else(|| "cannot locate the per-user configuration directory".to_owned())
}

fn config_directory() -> Result<PathBuf, String> {
    if cfg!(windows) {
        env::var_os("APPDATA")
            .map(PathBuf::from)
            .ok_or_else(|| "APPDATA is unavailable".to_owned())
    } else {
        Ok(env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or(home_directory()?.join(".config")))
    }
}

fn claude_desktop_config() -> Result<PathBuf, String> {
    if cfg!(target_os = "macos") {
        Ok(home_directory()?.join("Library/Application Support/Claude/claude_desktop_config.json"))
    } else {
        Ok(config_directory()?.join("Claude/claude_desktop_config.json"))
    }
}

#[cfg(test)]
async fn run_rpc(
    data: PathBuf,
    reader: impl BufRead,
    mut writer: impl Write,
) -> Result<(), String> {
    let control = Arc::new(AsyncMutex::new(ControlPlane::open(data.clone()).await?));
    let endpoint = start_control_endpoint(Arc::clone(&control), &data).await?;
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

#[cfg(test)]
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
            json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"acyclic","version":env!("CARGO_PKG_VERSION")}}})
        }
        "ping" => json!({"jsonrpc":"2.0","id":id,"result":{}}),
        "tools/list" => json!({"jsonrpc":"2.0","id":id,"result":{"tools":public_tools(false)}}),
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
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_in_result
)]
mod tests {
    use super::*;
    use acyclic_fs::{GitCommand, OperationWindowCoordinator, WorkspaceGraph};

    #[test]
    fn compatibility_commit_keeps_the_lazy_workspace_snapshot() {
        assert!(!git_requires_exact_workspace(&[
            "commit".to_owned(),
            "-m".to_owned(),
            "snapshot".to_owned(),
        ]));
        assert!(git_requires_exact_workspace(&["status".to_owned()]));
    }

    #[test]
    fn plugin_install_assets_are_discovered_without_build_paths() {
        let embedded_marketplace: Value =
            serde_json::from_str(include_str!("../.agents/plugins/marketplace.json"))
                .expect("embedded marketplace");
        assert_eq!(
            embedded_marketplace["plugins"][0]["source"]["path"], ".",
            "the embedded marketplace is rooted at the plugin directory"
        );

        let temporary = tempfile::tempdir().expect("temporary directory");
        let packaged = temporary.path().join("package");
        let installed = temporary.path().join("installed");
        for root in [&packaged, &installed] {
            fs::create_dir_all(root.join(".agents/plugins")).expect("manifest directory");
            fs::write(root.join("plugin.json"), b"{}").expect("plugin manifest");
            fs::write(root.join(".agents/plugins/marketplace.json"), b"{}").expect("marketplace");
        }

        assert_eq!(
            discover_plugin_root(&packaged.join("bin/acyclic"), &installed),
            Some(packaged)
        );
        assert_eq!(
            discover_plugin_root(&temporary.path().join("bin/acyclic"), &installed),
            Some(installed)
        );
    }

    #[test]
    fn shared_service_isolates_multiple_sessions() {
        run_large_stack("shared-service-e2e", shared_service_case);
    }

    #[test]
    fn shared_root_first_acquire_is_singleflight() {
        run_large_stack("shared-root-singleflight", shared_root_singleflight_case);
    }

    async fn shared_root_singleflight_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root");
        let registry = SharedRootRegistry::default();
        let (left, right) = tokio::join!(
            registry.acquire(&root, SharedRootAdmission::Fresh),
            registry.acquire(&root, SharedRootAdmission::Fresh)
        );
        let left = left.expect("first acquire");
        let right = right.expect("second acquire");
        assert!(Arc::ptr_eq(&left, &right));
        assert_eq!(registry.live_roots().await, 1);
    }

    #[cfg(unix)]
    #[test]
    fn shared_root_rejects_live_path_replacement() {
        run_large_stack("shared-root-replacement", shared_root_replacement_case);
    }

    #[cfg(unix)]
    async fn shared_root_replacement_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        let detached = temporary.path().join("detached");
        fs::create_dir(&root).expect("root");
        let registry = SharedRootRegistry::default();
        let held = registry
            .acquire(&root, SharedRootAdmission::Fresh)
            .await
            .expect("initial acquire");
        fs::rename(&root, &detached).expect("detach admitted root");
        fs::create_dir(&root).expect("replacement root");
        let Err(error) = registry.acquire(&root, SharedRootAdmission::Fresh).await else {
            panic!("replacement must fail closed");
        };
        assert!(error.contains("changed identity"), "{error}");
        drop(held);
    }

    async fn shared_service_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root_a = temporary.path().join("root-a");
        let root_b = temporary.path().join("root-b");
        let state = temporary.path().join("state");
        fs::create_dir(&root_a).expect("root a");
        fs::create_dir(&root_b).expect("root b");
        let mut service = ServiceControl::open(state.clone()).await.expect("service");
        for (session_id, root) in [("a", &root_a), ("b", &root_b)] {
            service
                .dispatch_request(ControlRequest {
                    version: 1,
                    command: ControlCommand::Hook,
                    cwd: root.clone(),
                    argv: Vec::new(),
                    name: "codex:SessionStart".to_owned(),
                    arguments: json!({
                        "session_id": session_id,
                        "cwd": root.display().to_string(),
                    }),
                })
                .await
                .expect("session start");
        }
        assert_eq!(service.sessions.len(), 2);
        assert_eq!(
            service.session_for_cwd(&root_a).expect("route a"),
            Some("a".to_owned())
        );
        assert_eq!(
            service.session_for_cwd(&root_b).expect("route b"),
            Some("b".to_owned())
        );
        service
            .dispatch_request(ControlRequest {
                version: 1,
                command: ControlCommand::Hook,
                cwd: root_a.clone(),
                argv: Vec::new(),
                name: "codex:SessionStart".to_owned(),
                arguments: json!({"session_id":"same-root","cwd":root_a}),
            })
            .await
            .expect("second session on same root");
        assert_ne!(
            service.sessions["a"].state.root_workspace_name,
            service.sessions["same-root"].state.root_workspace_name
        );
        assert_ne!(
            service.sessions["a"].state.root_repository_workspace_id,
            service.sessions["same-root"]
                .state
                .root_repository_workspace_id
        );
        let root_a_key = root_key(WorkspaceRootId::from_bytes(
            service.sessions["a"].state.root_id,
        ));
        let same_root_key = root_key(WorkspaceRootId::from_bytes(
            service.sessions["same-root"].state.root_id,
        ));
        assert!(Arc::ptr_eq(
            &service.sessions["a"].physical_roots[&root_a_key],
            &service.sessions["same-root"].physical_roots[&same_root_key],
        ));
        assert_eq!(service.shared_roots.live_roots().await, 2);

        let child_path = {
            let control = service.sessions.get_mut("a").expect("session a");
            control
                .user_prompt(json!({"session_id":"a","turn_id":"root-turn"}))
                .expect("root turn");
            control
                .pre_tool(json!({
                    "session_id":"a","turn_id":"root-turn","tool_use_id":"spawn-child",
                    "tool_name":"spawn_agent","tool_input":{}
                }))
                .await
                .expect("child spawn permit");
            control
                .subagent_start(json!({
                    "session_id":"a","turn_id":"child-turn","agent_id":"child",
                    "agent_type":"explorer"
                }))
                .await
                .expect("child start");
            control.state.routes["child"].path.clone()
        };
        assert!(
            service
                .dispatch_native_hook(
                    "codex",
                    "SessionStart",
                    json!({"session_id":"escape","cwd":child_path.display().to_string()}),
                    &child_path,
                )
                .await
                .is_err(),
            "a child mount must never be registered as another session's physical root"
        );
        {
            let control = service.sessions.get_mut("a").expect("session a");
            control
                .subagent_stop(json!({
                    "session_id":"a","turn_id":"child-turn","agent_id":"child"
                }))
                .await
                .expect("child stop");
            assert!(
                control
                    .pre_tool(json!({
                        "session_id":"a","turn_id":"child-turn","tool_use_id":"stopped-spawn",
                        "tool_name":"spawn_agent","tool_input":{}
                    }))
                    .await
                    .is_err(),
                "a stopped child must not create a spawn permit"
            );
            assert!(control.state.pending.is_empty());
        }

        fs::write(root_a.join("shared.txt"), b"shared update").expect("shared root update");
        for session_id in ["a", "same-root"] {
            let root_agent = service.sessions[session_id].state.root_agent_id.clone();
            let mut observed = false;
            for _ in 0..80 {
                service
                    .sessions
                    .get_mut(session_id)
                    .expect("session")
                    .sync_agent(&root_agent)
                    .await
                    .expect("shared root reconciliation");
                let control = &service.sessions[session_id];
                let workspace = control
                    .distributed
                    .workspace(acyclic_fs::WorkspaceId::from_bytes(
                        control.state.root_repository_workspace_id,
                    ))
                    .await
                    .expect("session root workspace");
                let head = workspace.head().await.expect("session root head");
                if head
                    .read("/shared.txt", 1024)
                    .await
                    .is_ok_and(|bytes| bytes.as_ref() == b"shared update")
                {
                    observed = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            assert!(
                observed,
                "{session_id} did not observe the shared root update"
            );
        }

        fs::write(root_a.join("recovered.txt"), b"recovered update")
            .expect("failed-consumer root update");
        service
            .sessions
            .get_mut("a")
            .expect("first shared-root session")
            .fail_after_watch_poll = true;
        let root_agent = service.sessions["a"].state.root_agent_id.clone();
        let mut injected = false;
        for _ in 0..80 {
            match service
                .sessions
                .get_mut("a")
                .expect("first shared-root session")
                .sync_agent(&root_agent)
                .await
            {
                Err(error) if error.contains("injected failure") => {
                    injected = true;
                    break;
                }
                Ok(()) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
                Err(error) => panic!("unexpected reconciliation failure: {error}"),
            }
        }
        assert!(injected, "shared watcher failure was not injected");
        let root_agent = service.sessions["same-root"].state.root_agent_id.clone();
        let mut recovered = false;
        for _ in 0..80 {
            service
                .sessions
                .get_mut("same-root")
                .expect("same-root session")
                .sync_agent(&root_agent)
                .await
                .expect("reconcile after peer capture failure");
            let control = &service.sessions["same-root"];
            let workspace = control
                .distributed
                .workspace(acyclic_fs::WorkspaceId::from_bytes(
                    control.state.root_repository_workspace_id,
                ))
                .await
                .expect("same-root workspace");
            if workspace
                .head()
                .await
                .expect("same-root head")
                .read("/recovered.txt", 1024)
                .await
                .is_ok_and(|bytes| bytes.as_ref() == b"recovered update")
            {
                recovered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(recovered, "peer lost the consumed shared watcher batch");
        service
            .dispatch_native_hook("codex", "SessionEnd", json!({"session_id":"a"}), &root_a)
            .await
            .expect("first shared-root session end");
        assert_eq!(service.shared_roots.live_roots().await, 2);
        service.shutdown().await.expect("shutdown");
        assert_eq!(service.shared_roots.live_roots().await, 0);
        drop(service);

        let mut resumed = ServiceControl::open(state).await.expect("resume service");
        assert_eq!(resumed.sessions.len(), 3);
        assert_eq!(resumed.shared_roots.live_roots().await, 2);
        let root_a_key = root_key(WorkspaceRootId::from_bytes(
            resumed.sessions["a"].state.root_id,
        ));
        let same_root_key = root_key(WorkspaceRootId::from_bytes(
            resumed.sessions["same-root"].state.root_id,
        ));
        assert!(Arc::ptr_eq(
            &resumed.sessions["a"].physical_roots[&root_a_key],
            &resumed.sessions["same-root"].physical_roots[&same_root_key],
        ));
        resumed.shutdown().await.expect("resumed shutdown");
    }

    #[test]
    fn cli_registers_fresh_cwd_without_init() {
        run_large_stack("fresh-cwd-e2e", fresh_cwd_case);
    }

    async fn fresh_cwd_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root");
        let mut service = ServiceControl::open(temporary.path().join("state"))
            .await
            .expect("service");
        let agents = service
            .dispatch_request(ControlRequest {
                version: 1,
                command: ControlCommand::Agents,
                cwd: root.clone(),
                argv: Vec::new(),
                name: String::new(),
                arguments: Value::Null,
            })
            .await
            .expect("implicit registration");
        assert!(agents["agents"].as_array().is_some_and(Vec::is_empty));
        assert_eq!(service.sessions.len(), 1);
        assert_eq!(
            service
                .sessions
                .values()
                .next()
                .expect("session")
                .state
                .root_path,
            root.canonicalize().expect("canonical root")
        );
        service.shutdown().await.expect("shutdown");
    }

    #[test]
    fn reopening_rejects_a_replaced_physical_root() {
        run_large_stack("root-identity-e2e", root_identity_case);
    }

    #[test]
    fn author_configuration_is_user_scoped_and_overridable() {
        run_large_stack("author-config-e2e", author_configuration_case);
    }

    async fn author_configuration_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("state");
        fs::create_dir_all(&data).expect("state directory");
        fs::write(
            data.join("author.json"),
            br#"{"version":1,"author":"Configured Author <configured@example.test>"}"#,
        )
        .expect("author configuration");
        let control = ControlPlane::open(data).await.expect("control");
        assert_eq!(
            control
                .author_for_argv(
                    &["commit".to_owned(), "-m".to_owned(), "x".to_owned()],
                    "host"
                )
                .expect("configured author"),
            "Configured Author <configured@example.test>"
        );
        assert_eq!(
            control
                .author_for_argv(
                    &[
                        "commit".to_owned(),
                        "--author=Explicit <explicit@example.test>".to_owned(),
                    ],
                    "host",
                )
                .expect("explicit author"),
            "host"
        );
    }

    async fn root_identity_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("state");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root");
        let mut control = ControlPlane::open(data.clone()).await.expect("control");
        control
            .session_start(json!({"session_id":"session","cwd":root}))
            .await
            .expect("session");
        control.shutdown().await.expect("shutdown");
        drop(control);
        let displaced = temporary.path().join("displaced-root");
        fs::rename(&root, &displaced).expect("displace original root");
        fs::create_dir(&root).expect("replacement root");
        let Err(error) = ControlPlane::open(data).await else {
            panic!("replaced root must be rejected");
        };
        assert!(error.contains("native root identity changed"), "{error}");
    }

    #[test]
    fn copilot_hooks_route_a_child_without_environment_state() {
        run_large_stack("copilot-hook-e2e", copilot_hook_case);
    }

    #[test]
    fn cursor_root_hook_is_capability_honest_and_uses_workspace_roots() {
        run_large_stack("cursor-hook-e2e", cursor_hook_case);
    }

    async fn cursor_hook_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root");
        let hook_directory = temporary.path().join("cursor-config");
        fs::create_dir(&hook_directory).expect("hook directory");
        let mut service = ServiceControl::open(temporary.path().join("state"))
            .await
            .expect("service");
        let output = service
            .dispatch_native_hook(
                "cursor",
                "sessionStart",
                json!({
                    "session_id":"cursor-session",
                    "workspace_roots":[root.display().to_string()]
                }),
                &hook_directory,
            )
            .await
            .expect("session start");
        let guidance = output["additional_context"]
            .as_str()
            .expect("Cursor guidance");
        assert!(guidance.contains("CLI-only"));
        assert!(!guidance.contains("native subagents isolated"));
        assert_eq!(
            service.sessions["cursor-session"].state.root_path,
            root.canonicalize().expect("canonical root")
        );
        service.shutdown().await.expect("shutdown");
    }

    async fn copilot_hook_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root");
        let mut service = ServiceControl::open(temporary.path().join("state"))
            .await
            .expect("service");
        service
            .dispatch_native_hook(
                "copilot",
                "sessionStart",
                json!({"sessionId":"session","cwd":root.display().to_string()}),
                &root,
            )
            .await
            .expect("session start");
        service
            .dispatch_native_hook(
                "copilot",
                "preToolUse",
                json!({
                    "sessionId":"session",
                    "cwd":root.display().to_string(),
                    "toolName":"task",
                    "toolArgs":{"description":"child"}
                }),
                &root,
            )
            .await
            .expect("spawn handshake");
        let child = service
            .dispatch_native_hook(
                "copilot",
                "subagentStart",
                json!({
                    "sessionId":"session",
                    "cwd":root.display().to_string(),
                    "agentName":"child",
                    "agentType":"custom"
                }),
                &root,
            )
            .await
            .expect("child start");
        assert!(child.to_string().contains("acyclic git"));
        let child_path = service.sessions["session"].state.routes["child"]
            .path
            .clone();
        assert!(
            service
                .dispatch_native_hook(
                    "claude-code",
                    "PreToolUse",
                    json!({
                        "session_id":"session",
                        "cwd":child_path.display().to_string(),
                        "tool_name":"Bash",
                        "tool_input":{"cmd":"pwd"}
                    }),
                    &child_path,
                )
                .await
                .is_err()
        );
        assert!(service.sessions["session"].state.leases.is_empty());
        let routed = service
            .dispatch_native_hook(
                "copilot",
                "preToolUse",
                json!({
                    "sessionId":"session",
                    "cwd":child_path.display().to_string(),
                    "toolUseId":"copilot-bash-1",
                    "toolName":"Bash",
                    "toolArgs":{"cmd":"pwd","workdir":root.display().to_string()}
                }),
                &child_path,
            )
            .await
            .expect("child tool");
        assert_eq!(
            routed["modifiedArgs"]["workdir"],
            child_path.display().to_string()
        );
        service.shutdown().await.expect("shutdown");
    }

    #[test]
    fn claude_hooks_preserve_tool_identity_rewrite_cwd_and_fail_closed() {
        run_large_stack("claude-hook-e2e", claude_hook_case);
    }

    fn run_large_stack<F>(name: &str, make: impl FnOnce() -> F + Send + 'static)
    where
        F: std::future::Future<Output = ()> + 'static,
    {
        std::thread::Builder::new()
            .name(name.to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(make());
            })
            .expect("test thread")
            .join()
            .expect("large-stack test thread");
    }

    async fn claude_hook_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root");
        let mut service = ServiceControl::open(temporary.path().join("state"))
            .await
            .expect("service");
        service
            .dispatch_native_hook(
                "claude-code",
                "SessionStart",
                json!({"session_id":"session","cwd":root.display().to_string()}),
                &root,
            )
            .await
            .expect("session start");
        service
            .dispatch_native_hook(
                "claude-code",
                "PreToolUse",
                json!({
                    "session_id":"session",
                    "cwd":root.display().to_string(),
                    "tool_name":"Agent",
                    "tool_use_id":"spawn-1",
                    "tool_input":{"description":"child"}
                }),
                &root,
            )
            .await
            .expect("spawn handshake");
        service
            .dispatch_native_hook(
                "claude-code",
                "SubagentStart",
                json!({
                    "session_id":"session",
                    "cwd":root.display().to_string(),
                    "agent_id":"child",
                    "agent_type":"general-purpose"
                }),
                &root,
            )
            .await
            .expect("child start");
        let child_path = service.sessions["session"].state.routes["child"]
            .path
            .clone();
        let routed = service
            .dispatch_native_hook(
                "claude-code",
                "PreToolUse",
                json!({
                    "session_id":"session",
                    "cwd":child_path.display().to_string(),
                    "tool_name":"Bash",
                    "tool_use_id":"tool-1",
                    "tool_input":{"cmd":"pwd","workdir":root.display().to_string()}
                }),
                &child_path,
            )
            .await
            .expect("child tool");
        assert_eq!(
            routed["hookSpecificOutput"]["updatedInput"]["workdir"],
            child_path.display().to_string()
        );
        assert!(
            service.sessions["session"]
                .state
                .leases
                .contains_key("tool-1")
        );
        assert!(
            service
                .dispatch_native_hook(
                    "claude-code",
                    "PreToolUse",
                    json!({
                        "session_id":"session",
                        "cwd":child_path.display().to_string(),
                        "tool_name":"UnknownWriter",
                        "tool_use_id":"tool-2",
                        "tool_input":{}
                    }),
                    &child_path,
                )
                .await
                .is_err()
        );
        service
            .dispatch_native_hook(
                "claude-code",
                "PostToolUse",
                json!({
                    "session_id":"session",
                    "cwd":child_path.display().to_string(),
                    "tool_name":"Bash",
                    "tool_use_id":"tool-1",
                    "tool_input":{"cmd":"pwd","workdir":root.display().to_string()}
                }),
                &child_path,
            )
            .await
            .expect("tool close");
        assert!(service.sessions["session"].state.leases.is_empty());
        let context_id = service.sessions["session"].state.root_context_id;
        service
            .dispatch_native_hook(
                "claude-code",
                "SessionEnd",
                json!({"session_id":"session"}),
                &root,
            )
            .await
            .expect("session end");
        assert!(!service.sessions.contains_key("session"));
        service
            .dispatch_native_hook(
                "claude-code",
                "SessionStart",
                json!({"session_id":"session","cwd":root.display().to_string(),"source":"resume"}),
                &root,
            )
            .await
            .expect("session resume");
        assert_eq!(
            service.sessions["session"].state.root_context_id,
            context_id
        );
        service.shutdown().await.expect("shutdown");
    }

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
    fn multi_root_contexts_fork_route_publish_and_resume_atomically() {
        std::thread::Builder::new()
            .name("plugin-multi-root-e2e".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(multi_root_publication_case());
            })
            .expect("test thread")
            .join()
            .expect("plugin multi-root e2e thread");
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

    #[cfg(windows)]
    #[test]
    fn windows_control_endpoint_keeps_the_next_pipe_while_reaping_connections() {
        std::thread::Builder::new()
            .name("plugin-endpoint-sequential".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(sequential_endpoint_case());
            })
            .expect("test thread")
            .join()
            .expect("plugin sequential endpoint thread");
    }

    #[test]
    fn authenticated_control_protocol_dispatches_git() {
        std::thread::Builder::new()
            .name("plugin-control-protocol".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_stack_size(32 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(authenticated_control_protocol_case());
            })
            .expect("test thread")
            .join()
            .expect("plugin control protocol thread");
    }

    #[test]
    fn root_git_uses_the_same_repository_and_materializer() {
        std::thread::Builder::new()
            .name("plugin-root-git".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(root_git_case());
            })
            .expect("test thread")
            .join()
            .expect("plugin root Git thread");
    }

    #[test]
    fn publication_history_recovers_after_a_crash_boundary() {
        std::thread::Builder::new()
            .name("plugin-publication-history-recovery".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(publication_history_recovery_case());
            })
            .expect("test thread")
            .join()
            .expect("publication history recovery thread");
    }

    async fn publication_history_recovery_case() {
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
            .pre_tool(json!({
                "session_id":"session","turn_id":"root-turn","tool_use_id":"spawn-child",
                "tool_name":"spawn_agent","tool_input":{}
            }))
            .await
            .expect("spawn child");
        control
            .subagent_start(json!({
                "session_id":"session","turn_id":"child-turn",
                "agent_id":"child","agent_type":"explorer"
            }))
            .await
            .expect("child start");
        let child = control
            .workspace(&control.state.routes["child"])
            .await
            .expect("child workspace");
        let mut transaction = child
            .begin_transaction(IdempotencyKey::new())
            .await
            .expect("child transaction");
        transaction
            .write_text("/published.txt", "published")
            .await
            .expect("child file");
        transaction
            .write_text("/.gitignore", "*.tmp\n")
            .await
            .expect("ignore policy");
        transaction
            .write_text("/scratch.tmp", "must not publish")
            .await
            .expect("ignored child file");
        transaction.commit().await.expect("child commit");
        control.fail_before_publication_history = true;
        let failure = control
            .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .expect_err("history fault must interrupt an otherwise complete publication");
        assert_eq!(failure, "injected failure before compatibility history");
        assert_eq!(
            fs::read(root.join("published.txt")).expect("physical publication committed"),
            b"published"
        );
        assert!(!root.join("scratch.tmp").exists());
        assert_eq!(
            <LocalCoreStateStore as acyclic_fs::MultiRootPublicationStore>::list_operations(
                &control.store
            )
            .await
            .expect("retained publication")
            .len(),
            1
        );
        let root_id = WorkspaceRootId::from_bytes(control.state.root_id);
        let root_workspace = control
            .roots
            .get(&root_key(root_id))
            .expect("root workspace")
            .workspace()
            .clone();
        let mut later_transaction = root_workspace
            .begin_transaction(IdempotencyKey::new())
            .await
            .expect("later root transaction");
        later_transaction
            .write_text("/later.txt", "must not enter recovered publication history")
            .await
            .expect("later root update");
        later_transaction.commit().await.expect("later root commit");
        drop(control);

        let mut reopened = ControlPlane::open(data)
            .await
            .expect("restart recovers compatibility history");
        let repository = GitCompatRepository::new(
            acyclic_fs::WorkspaceId::from_bytes(reopened.state.root_repository_workspace_id),
            reopened.store.clone(),
        );
        let root_workspace = reopened
            .roots
            .get(&root_key(root_id))
            .expect("reopened root workspace")
            .workspace()
            .clone();
        let output = repository
            .execute(
                GitCommand::Show { object: None },
                GitTreeRef::exact(
                    root_workspace.id(),
                    root_workspace.head().await.expect("root head").id(),
                ),
            )
            .await
            .expect("root compatibility head");
        let GitCommandOutput::Commits(commits) = output else {
            panic!("show must return the recovered publication commit");
        };
        let commit = commits.first().expect("publication commit");
        let published_workspace = reopened
            .distributed
            .workspace(commit.tree.workspace_id())
            .await
            .expect("published history workspace");
        let published_generation = published_workspace
            .generation(commit.tree.authored_generation())
            .await
            .expect("published history generation");
        assert_eq!(
            published_generation
                .read("/published.txt", 1024)
                .await
                .expect("published history file"),
            b"published"[..]
        );
        assert!(
            matches!(
                published_generation.read("/later.txt", 1024).await,
                Err(WorkspaceError::NotFound)
            ),
            "recovered compatibility history must remain pinned to the published generation"
        );
        assert!(
            <LocalCoreStateStore as acyclic_fs::MultiRootPublicationStore>::list_operations(
                &reopened.store
            )
            .await
            .expect("publication cleanup")
            .is_empty()
        );
        reopened.shutdown().await.expect("shutdown");
    }

    async fn root_git_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root directory");
        fs::write(root.join("base.txt"), b"base").expect("baseline file");
        fs::write(
            root.join("change.patch"),
            b"diff --git a/created.txt b/created.txt\nnew file mode 100644\n--- /dev/null\n+++ b/created.txt\n@@ -0,0 +1 @@\n+created by root git\n",
        )
        .expect("patch file");
        let mut control = ControlPlane::open(temporary.path().join("plugin-data"))
            .await
            .expect("control plane");
        control
            .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
            .await
            .expect("root session");
        assert_eq!(
            fs::read(root.join("base.txt")).expect("base after attach"),
            b"base"
        );
        let status = control
            .root_git_tool(
                WorkspaceRootId::from_bytes(control.state.root_id),
                vec!["status".to_owned()],
            )
            .await
            .expect("root status");
        assert!(!status.is_null());
        control
            .root_git_tool(
                WorkspaceRootId::from_bytes(control.state.root_id),
                vec!["diff".to_owned()],
            )
            .await
            .expect("fresh lazy root diff");
        control
            .root_git_tool(
                WorkspaceRootId::from_bytes(control.state.root_id),
                vec!["grep".to_owned(), "base".to_owned(), "base.txt".to_owned()],
            )
            .await
            .expect("file-targeted lazy root grep");
        let repository_id =
            acyclic_fs::WorkspaceId::from_bytes(control.state.root_repository_workspace_id);
        control
            .root_git_tool(
                WorkspaceRootId::from_bytes(control.state.root_id),
                vec!["commit".to_owned(), "-m".to_owned(), "initial".to_owned()],
            )
            .await
            .expect("root commit");
        fs::write(root.join("base.txt"), b"externally changed")
            .expect("external root modification");
        let mut external_status = Value::Null;
        for _ in 0..80 {
            external_status = control
                .root_git_tool(
                    WorkspaceRootId::from_bytes(control.state.root_id),
                    vec!["status".to_owned()],
                )
                .await
                .expect("status after external root modification");
            if external_status.to_string().contains("\"dirty\":\"dirty\"") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(
            external_status.to_string().contains("\"dirty\":\"dirty\""),
            "external root modification must be visible to Git: {external_status}"
        );
        control
            .root_git_tool(
                WorkspaceRootId::from_bytes(control.state.root_id),
                vec!["archive".to_owned(), "HEAD".to_owned()],
            )
            .await
            .expect("archive lazy compatibility commit");
        control
            .root_git_tool(
                WorkspaceRootId::from_bytes(control.state.root_id),
                vec!["blame".to_owned(), "base.txt".to_owned()],
            )
            .await
            .expect("blame lazy compatibility commit");
        control
            .root_git_tool(
                WorkspaceRootId::from_bytes(control.state.root_id),
                vec!["switch".to_owned(), "-c".to_owned(), "feature".to_owned()],
            )
            .await
            .expect("root branch switch");
        let state = <LocalCoreStateStore as acyclic_fs::GitCompatStore>::load(
            &control.store,
            repository_id,
        )
        .await
        .expect("load stable root repository")
        .expect("root repository state");
        assert_eq!(state.current_branch, "feature");
        assert!(state.branches.contains_key("main"));
        control
            .root_git_tool(
                WorkspaceRootId::from_bytes(control.state.root_id),
                vec!["apply".to_owned(), "change.patch".to_owned()],
            )
            .await
            .expect("root patch application");
        assert_eq!(
            fs::read_to_string(root.join("created.txt")).expect("materialized root file"),
            "created by root git\n"
        );
        control.shutdown().await.expect("control shutdown");
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
        assert_eq!(
            responses[1]["result"]["tools"]
                .as_array()
                .expect("public tools"),
            &Vec::<Value>::new()
        );
        assert!(
            responses[2]["result"]["content"][0]["text"]
                .as_str()
                .is_some_and(|text| text.contains("speculatively"))
        );

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
        let endpoint = start_control_endpoint(Arc::clone(&shared), &data)
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
    async fn sequential_endpoint_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let control = Arc::new(AsyncMutex::new(
            ControlPlane::open(data.clone())
                .await
                .expect("control plane"),
        ));
        let endpoint = start_control_endpoint(Arc::clone(&control), &data)
            .await
            .expect("control endpoint");
        for sequence in 0..64 {
            let error = send_control_request(
                &data,
                &ControlRequest {
                    version: 1,
                    command: ControlCommand::Upgrade,
                    cwd: temporary.path().to_path_buf(),
                    argv: Vec::new(),
                    name: String::new(),
                    arguments: Value::Null,
                },
            )
            .await
            .expect_err("unsupported request");
            assert!(
                matches!(
                    error,
                    ControlRequestError::Response(ref message)
                        if message == "control command is invalid for a workspace session"
                ),
                "request {sequence} must remain a non-retryable domain response: {error}"
            );
        }
        endpoint.shutdown().await.expect("endpoint shutdown");
        let mut control = match Arc::try_unwrap(control) {
            Ok(control) => control.into_inner(),
            Err(_) => panic!("endpoint retained a completed request"),
        };
        control.shutdown().await.expect("control shutdown");
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
        let shell = control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"system-status","tool_name":"exec_command","tool_input":{"cmd":"git status","workdir":root.display().to_string()}}))
            .await
            .expect("shell command is redirected to the child mount");
        assert_eq!(
            shell["hookSpecificOutput"]["updatedInput"]["workdir"],
            control.state.routes["child"].path.display().to_string()
        );
        control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"system-status","tool_name":"exec_command"}))
            .await
            .expect("close redirected shell lease");

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

        let first_merge = control
            .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .expect("first merge");
        assert!(matches!(
            first_merge["status"].as_str(),
            Some("applied" | "already-applied")
        ));
        assert_eq!(
            fs::read(root.join("first.txt")).expect("published first file"),
            b"first"
        );
        assert!(control.mounts.contains_key("child"));

        let route = &control.state.routes["child"];
        let published_generation = route
            .roots
            .get(&root_key(WorkspaceRootId::from_bytes(route.root_id)))
            .expect("active route root")
            .published_generation;
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
            published_generation
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
        let stopped = control
            .subagent_stop(
                json!({"session_id":"session","turn_id":"child-turn","agent_id":"child"}),
            )
            .await
            .expect("seal child");
        assert_eq!(stopped["suppressOutput"], false);
        assert_eq!(stopped["acyclicSummary"]["agent"], "agents/child");
        assert_eq!(stopped["acyclicSummary"]["tests"]["status"], "not-reported");
        assert!(
            stopped["acyclicSummary"]["actions"]["merge"]
                .as_str()
                .is_some_and(|command| command.contains("agents/child"))
        );
        let root_agent = control.state.root_agent_id.clone();
        let status = control
            .agents_status(&root_agent)
            .await
            .expect("recursive agent status");
        assert_eq!(status["schemaVersion"], 1);
        assert_eq!(status["agents"][0]["ref"], "agents/child");
        assert_eq!(status["agents"][0]["state"], "frozen");
        assert!(status["agents"][0]["roots"].is_array());
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
        control.fail_after_context_discard = true;
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
        let data = temporary.path().join("plugin-data");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root directory");
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
            .pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn","tool_name":"spawn_agent","tool_input":{}}))
            .await
            .expect("spawn handshake");
        control
            .subagent_start(json!({"session_id":"session","turn_id":"child-turn","agent_id":"child","agent_type":"explorer"}))
            .await
            .expect("child start");
        control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"git-switch","tool_name":"exec_command","tool_input":{"cmd":"acyclic git switch -c feature","workdir":root.display().to_string()}}))
            .await
            .expect("open authenticated command");
        let routed_workspace = |route: &Route| {
            route.roots[&root_key(WorkspaceRootId::from_bytes(route.root_id))].workspace_id
        };
        let original_workspace = routed_workspace(&control.state.routes["child"]);
        let child_cwd = control.state.routes["child"].path.clone();
        let shared = Arc::new(AsyncMutex::new(control));
        for argv in [vec![
            "status".to_owned(),
            "&&".to_owned(),
            "push".to_owned(),
        ]] {
            let rejected = dispatch_control_request(
                &shared,
                ControlRequest {
                    version: 1,
                    command: ControlCommand::Git,
                    cwd: child_cwd.clone(),
                    argv,
                    name: String::new(),
                    arguments: Value::Null,
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
            "version":1,"command":"git","cwd":child_cwd,"argv":["status"]
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
        assert_eq!(response["ok"], true, "{response}");
        assert!(response["result"].is_object());
        handler
            .await
            .expect("handler task")
            .expect("handler result");

        let (mut client, server) = tokio::io::duplex(1);
        let (connection_shutdown, receiver) = watch::channel(false);
        let handler = tokio::spawn(handle_control_connection(
            server,
            Arc::clone(&shared),
            receiver,
        ));
        let request = serde_json::to_vec(&json!({
            "version":1,"command":"git","cwd":child_cwd,"argv":["switch","-c","feature"]
        }))
        .expect("control request");
        client.write_all(&request).await.expect("write request");
        client.write_all(b"\n").await.expect("write newline");
        client.flush().await.expect("flush request");
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let control = shared.lock().await;
                if routed_workspace(&control.state.routes["child"]) != original_workspace {
                    break;
                }
                drop(control);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the accepted Git switch must finish before response cancellation");
        connection_shutdown
            .send(true)
            .expect("signal connection shutdown");
        tokio::time::timeout(std::time::Duration::from_secs(5), handler)
            .await
            .expect("blocked response shutdown")
            .expect("handler task")
            .expect("handler result");
        drop(client);

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
            command: ControlCommand::Git,
            cwd: child_cwd,
            argv: vec!["status".to_owned()],
            name: String::new(),
            arguments: Value::Null,
        };
        drop(control);
        assert!(dispatch_control_request(&shared, stale).await.is_err());
        let mut control = match Arc::try_unwrap(shared) {
            Ok(control) => control.into_inner(),
            Err(_) => panic!("response cancellation retained the control plane"),
        };
        control.shutdown().await.expect("shutdown");
        drop(control);
        let mut reopened = ControlPlane::open(data)
            .await
            .expect("reopen after blocked response shutdown");
        reopened.shutdown().await.expect("reopened shutdown");
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
        assert_eq!(
            fs::read(root.join("base.txt")).expect("base after fork"),
            b"base"
        );
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
        let repository_id = acyclic_fs::WorkspaceId::from_bytes(
            route
                .roots
                .get(&root_key(WorkspaceRootId::from_bytes(route.root_id)))
                .expect("active route root")
                .repository_workspace_id,
        );
        let branch = control.workspace(&route).await.expect("branch workspace");
        assert_ne!(branch.id(), repository_id);
        let root_workspace = control
            .roots
            .get(&root_key(WorkspaceRootId::from_bytes(
                control.state.root_id,
            )))
            .cloned()
            .expect("root workspace")
            .workspace()
            .clone();
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
            control.state.routes["child"].roots[&root_key(WorkspaceRootId::from_bytes(
                control.state.routes["child"].root_id,
            ))]
                .workspace_id,
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
            reused.roots[&root_key(WorkspaceRootId::from_bytes(reused.root_id))]
                .repository_workspace_id,
            repository_id.into_bytes()
        );
    }

    async fn recursive_publication_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root directory");
        fs::write(root.join("base.txt"), b"base").expect("unobserved root fixture");
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
        control
            .pre_tool(json!({
                "session_id":"session",
                "turn_id":"child-turn","tool_use_id":"git-commit",
                "tool_name":"exec_command",
                "tool_input":{"cmd":"git commit -m initial","workdir":root.display().to_string()}
            }))
            .await
            .expect("bare Git is allowed and remains unrelated to Acyclic");
        assert_eq!(
            fs::read(root.join("base.txt")).expect("base before command close"),
            b"base"
        );
        control
            .post_tool(json!({
                "session_id":"session","turn_id":"child-turn",
                "tool_use_id":"git-commit","tool_name":"exec_command"
            }))
            .await
            .expect("close bare Git lease");
        assert_eq!(
            fs::read(root.join("base.txt")).expect("base after command lease"),
            b"base"
        );
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
        assert_eq!(
            fs::read(root.join("base.txt")).expect("base after read lease"),
            b"base"
        );
        control
            .pre_tool(json!({
                "session_id":"session",
                "turn_id":"child-turn","tool_use_id":"spawn-grandchild",
                "tool_name":"spawn_agent","tool_input":{}
            }))
            .await
            .expect("grandchild spawn");
        assert_eq!(
            fs::read(root.join("base.txt")).expect("base before grandchild start"),
            b"base"
        );
        control
            .subagent_start(json!({
                "session_id":"session","turn_id":"grandchild-turn",
                "agent_id":"grandchild","agent_type":"explorer"
            }))
            .await
            .expect("grandchild start");
        assert_eq!(
            fs::read(root.join("base.txt")).expect("base after grandchild fork"),
            b"base"
        );
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
        assert_eq!(
            fs::read(root.join("base.txt")).expect("base after grandchild write"),
            b"base"
        );

        assert!(
            control
                .agent_merge(json!({
                    "agent":"grandchild","_caller_turn_id":"root-turn"
                }))
                .await
                .is_err()
        );
        assert_eq!(
            fs::read(root.join("base.txt")).expect("base after unauthorized merge"),
            b"base"
        );
        control
            .agent_merge(json!({
                "agent":"grandchild","_caller_turn_id":"child-turn"
            }))
            .await
            .expect("merge into child");
        assert_eq!(
            fs::read(root.join("base.txt")).expect("base before root merge"),
            b"base"
        );
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
        assert_eq!(
            fs::read(root.join("base.txt")).expect("unobserved root file survives publication"),
            b"base"
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

    async fn multi_root_publication_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let first = temporary.path().join("first");
        let second = temporary.path().join("second");
        fs::create_dir_all(&first).expect("first root");
        fs::create_dir_all(&second).expect("second root");
        fs::write(first.join("base.txt"), b"first").expect("first fixture");
        fs::write(second.join("base.txt"), b"second").expect("second fixture");

        let mut control = ControlPlane::open(data.clone())
            .await
            .expect("control plane");
        control
            .session_start(json!({"session_id":"session","cwd":first}))
            .await
            .expect("first root attach");
        control
            .session_start(json!({"session_id":"session","cwd":second}))
            .await
            .expect("second root adopt");
        assert_eq!(control.state.roots.len(), 2);
        let second_root = control
            .state
            .roots
            .values()
            .find(|root| root.root_id != control.state.root_id)
            .map(|root| WorkspaceRootId::from_bytes(root.root_id))
            .expect("second root binding");
        control
            .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
            .expect("root turn");
        control
            .pre_tool(json!({
                "session_id":"session",
                "turn_id":"root-turn",
                "tool_use_id":"spawn-child",
                "tool_name":"spawn_agent",
                "tool_input":{},
                "_caller_root_id":hex::encode(second_root.into_bytes())
            }))
            .await
            .expect("child spawn handshake");
        control
            .subagent_start(json!({
                "session_id":"session",
                "turn_id":"child-turn",
                "agent_id":"child"
            }))
            .await
            .expect("child start");
        let route = control.state.routes["child"].clone();
        assert_eq!(route.roots.len(), 2);
        assert_eq!(route.root_id, second_root.into_bytes());
        let root_keys = route.roots.keys().cloned().collect::<Vec<_>>();
        let first_key = root_keys.first().expect("first ordered root").clone();
        let last_key = root_keys.last().expect("last ordered root").clone();
        let original_workspace_id = control.state.routes["child"].roots[&last_key].workspace_id;
        control
            .state
            .routes
            .get_mut("child")
            .expect("child route")
            .roots
            .get_mut(&last_key)
            .expect("last root")
            .workspace_id = [u8::MAX; 16];
        assert!(
            control
                .pre_tool(json!({
                    "session_id":"session",
                    "turn_id":"child-turn",
                    "tool_use_id":"partial-lease",
                    "tool_name":"Read",
                    "tool_input":{"path":route.path.join("base.txt")}
                }))
                .await
                .is_err()
        );
        assert!(!control.state.leases.contains_key("partial-lease"));
        let first_route = &route.roots[&first_key];
        let first_workspace = control
            .workspace_root(&route, WorkspaceRootId::from_bytes(first_route.root_id))
            .await
            .expect("first child root");
        assert!(matches!(
            OperationWindowCoordinator::new(control.store.clone())
                .snapshot(first_workspace.id())
                .await
                .expect("rolled-back window")
                .phase,
            acyclic_fs::OperationWindowPhase::Idle
        ));
        control
            .state
            .routes
            .get_mut("child")
            .expect("child route")
            .roots
            .get_mut(&last_key)
            .expect("last root")
            .workspace_id = original_workspace_id;
        control
            .pre_tool(json!({
                "session_id":"session",
                "turn_id":"child-turn",
                "tool_use_id":"active-write",
                "tool_name":"Write",
                "tool_input":{"path":route.path.join("in-flight.txt")}
            }))
            .await
            .expect("active child write lease");
        for command in ["changes", "merge", "discard"] {
            let input = json!({
                "agent":"child",
                "_caller_agent_id":control.state.root_agent_id
            });
            let result = match command {
                "changes" => control.agent_changes(input).await,
                "merge" => control.agent_merge(input).await,
                "discard" => control.agent_discard(input).await,
                _ => unreachable!(),
            };
            assert!(
                result.is_err(),
                "untrusted MCP input must not supply caller identity for {command}"
            );
        }
        assert!(
            control
                .agent_changes(json!({"agent":"child","_caller_turn_id":"root-turn"}))
                .await
                .is_err(),
            "inspection must not flush a mount while a tool lease is active"
        );
        assert!(
            control
                .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
                .await
                .is_err(),
            "publication must not flush a mount while a tool lease is active"
        );
        control
            .post_tool(json!({
                "session_id":"session",
                "turn_id":"child-turn",
                "tool_use_id":"active-write",
                "tool_name":"Write"
            }))
            .await
            .expect("close child write lease");
        for route_root in route.roots.values() {
            let workspace = control
                .workspace_root(&route, WorkspaceRootId::from_bytes(route_root.root_id))
                .await
                .expect("child root workspace");
            let mut transaction = workspace
                .begin_transaction(IdempotencyKey::new())
                .await
                .expect("root transaction");
            transaction
                .write_text("/child.txt", &route_root.route_name)
                .await
                .expect("root write");
            transaction.commit().await.expect("root commit");
        }
        let changes = control
            .agent_changes(json!({
                "agent":"child",
                "path":"child.txt",
                "_caller_turn_id":"root-turn"
            }))
            .await
            .expect("multi-root filtered changes");
        assert_eq!(changes["roots"].as_array().map(Vec::len), Some(2));
        assert_eq!(changes["fileChanges"], 0);
        assert_eq!(changes["bindingChanges"], 2);
        assert!(changes["roots"].as_array().is_some_and(|roots| {
            roots
                .iter()
                .all(|root| root["paths"] == json!(["/child.txt"]))
        }));
        control.mounts["child"]
            .advance_to_head()
            .await
            .expect("advance child mounts");
        let result = control
            .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .expect("multi-root publication");
        assert_eq!(result["status"], "applied");
        assert!(first.join("child.txt").is_file());
        assert!(second.join("child.txt").is_file());
        control.shutdown().await.expect("shutdown");

        let mut resumed = ControlPlane::open(data)
            .await
            .expect("resume control plane");
        assert_eq!(resumed.state.roots.len(), 2);
        assert_eq!(resumed.state.routes["child"].roots.len(), 2);
        let (_, routed, root_id) = resumed
            .route_root_from_cwd(&resumed.state.routes["child"].path)
            .expect("route resumed child cwd");
        assert_eq!(routed.expect("child route").agent_id, "child");
        assert_eq!(root_id, second_root);
        resumed.shutdown().await.expect("resumed shutdown");
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
        assert!(validate_shell_paths("cat<../outside", &child).is_err());
        assert!(validate_shell_paths("echo value>../outside", &child).is_err());
        assert!(validate_shell_paths("echo value 2>../outside", &child).is_err());
    }

    #[test]
    fn commandless_hosts_receive_exactly_one_shell_free_tool() {
        assert_eq!(public_tools(false), json!([]));
        let tools = public_tools(true);
        assert_eq!(tools.as_array().map(Vec::len), Some(1));
        assert_eq!(tools[0]["name"], "acyclic");
        assert_eq!(tools[0]["inputSchema"]["required"], json!(["argv"]));
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
    fn control_responses_are_bounded() {
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

    #[test]
    fn configuration_writes_are_idempotent_and_keep_the_original_backup() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("host/config.json");
        fs::create_dir_all(path.parent().expect("configuration parent")).expect("create parent");
        fs::write(&path, br#"{"existing":true}"#).expect("original configuration");

        let installed = json!({"existing":true,"mcpServers":{"acyclic":{}}});
        write_json_with_backup(&path, &installed).expect("first write");
        write_json_with_backup(&path, &installed).expect("idempotent retry");
        let backup = path.with_extension("json.acyclic-backup");
        assert_eq!(
            fs::read(&backup).expect("original backup"),
            br#"{"existing":true}"#
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(&path).expect("installed config"))
                .expect("valid installed JSON"),
            installed
        );
        assert!(!path.with_extension("acyclic-next").exists());
    }

    #[test]
    fn mcp_configuration_requires_exact_durable_ownership() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("host/config.json");
        let executable = temporary.path().join("acyclic");
        fs::create_dir_all(path.parent().expect("configuration parent")).expect("create parent");
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "mcpServers": {"foreign": {"command":"foreign"}}
            }))
            .expect("foreign JSON"),
        )
        .expect("original configuration");

        merge_mcp_config(&path, McpConfigShape::Standard, &executable).expect("install");
        merge_mcp_config(&path, McpConfigShape::Standard, &executable).expect("idempotent install");
        let installed = fs::read(&path).expect("installed configuration");
        let mut modified: Value = serde_json::from_slice(&installed).expect("installed JSON");
        modified["mcpServers"]["acyclic"]["args"] = json!(["foreign"]);
        write_json_with_backup(&path, &modified).expect("user edit");
        assert!(remove_mcp_config(&path, McpConfigShape::Standard).is_err());
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(&path).expect("preserved user edit"))
                .expect("preserved JSON"),
            modified
        );
        fs::write(&path, installed).expect("restore exact installed value");
        remove_mcp_config(&path, McpConfigShape::Standard).expect("owned uninstall");
        let restored: Value =
            serde_json::from_slice(&fs::read(&path).expect("restored configuration"))
                .expect("restored JSON");
        assert!(restored["mcpServers"].get("acyclic").is_none());
        assert_eq!(restored["mcpServers"]["foreign"]["command"], "foreign");

        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "mcpServers": {"acyclic": {"command":"not-ours"}}
            }))
            .expect("foreign Acyclic JSON"),
        )
        .expect("foreign Acyclic configuration");
        assert!(merge_mcp_config(&path, McpConfigShape::Standard, &executable).is_err());
    }

    #[test]
    fn copilot_hooks_restore_prior_content_and_preserve_user_edits() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join(".copilot/hooks/acyclic.json");
        let executable = temporary.path().join("acyclic");
        fs::create_dir_all(path.parent().expect("hook parent")).expect("hook parent");
        let prior = json!({"version":1,"hooks":{"user":[{"exec":"user"}]}});
        fs::write(&path, serde_json::to_vec(&prior).expect("prior JSON")).expect("prior hooks");

        install_copilot_hooks_at(&executable, &path).expect("install hooks");
        install_copilot_hooks_at(&executable, &path).expect("idempotent install");
        let installed = fs::read(&path).expect("installed hooks");
        let mut edited: Value = serde_json::from_slice(&installed).expect("installed JSON");
        edited["hooks"]["sessionStart"][0]["timeout"] = json!(1);
        fs::write(&path, serde_json::to_vec(&edited).expect("edited JSON")).expect("edit hooks");
        assert!(remove_copilot_hooks_at(&path).is_err());
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(&path).expect("preserved edit"))
                .expect("preserved JSON"),
            edited
        );

        fs::write(&path, installed).expect("restore installed hooks");
        remove_copilot_hooks_at(&path).expect("owned uninstall");
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(&path).expect("restored prior"))
                .expect("restored JSON"),
            prior
        );

        fs::write(&path, b"not JSON").expect("malformed prior hooks");
        assert!(install_copilot_hooks_at(&executable, &path).is_err());
        assert_eq!(
            fs::read(&path).expect("preserved malformed hooks"),
            b"not JSON"
        );
    }

    #[test]
    fn codex_install_preserves_old_or_disabled_plugin_state() {
        assert!(
            validate_existing_codex_plugin(&json!({
                "version": "0.1.0-rc.1",
                "enabled": true
            }))
            .is_err()
        );
        assert!(
            validate_existing_codex_plugin(&json!({
                "version": env!("CARGO_PKG_VERSION"),
                "enabled": false
            }))
            .is_err()
        );
        assert!(
            validate_existing_codex_plugin(&json!({
                "version": env!("CARGO_PKG_VERSION"),
                "enabled": true
            }))
            .is_ok()
        );
    }

    #[test]
    fn opencode_guidance_markers_preserve_unrelated_instructions() {
        let original = "# Existing\n\nKeep this.\n";
        let installed =
            format!("{original}\n{OPENCODE_GUIDANCE_START}\nold\n{OPENCODE_GUIDANCE_END}\n");
        assert_eq!(
            remove_marked_block(&installed, OPENCODE_GUIDANCE_START, OPENCODE_GUIDANCE_END),
            original.trim()
        );
        assert_eq!(
            remove_marked_block(original, OPENCODE_GUIDANCE_START, OPENCODE_GUIDANCE_END),
            original
        );
    }

    #[test]
    fn adapter_state_loading_is_byte_and_structure_bounded() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let oversized = temporary.path().join("oversized.json");
        let file = fs::File::create(&oversized).expect("oversized state");
        file.set_len(MAXIMUM_ADAPTER_STATE_BYTES + 1)
            .expect("extend oversized state");
        assert!(
            read_state(&oversized)
                .expect_err("oversized state must fail")
                .contains("byte bound")
        );

        let mut state = AdapterState::default();
        for index in 0..=MAXIMUM_ADAPTER_TURNS {
            state.turns.insert(index.to_string(), "root".to_owned());
        }
        assert!(
            validate_state_bounds(&state)
                .expect_err("oversized turn map must fail")
                .contains("too many turns")
        );
        state.turns.clear();
        for index in 0..=MAXIMUM_ADAPTER_ROOT_TURNS {
            state.root_turns.insert(index.to_string());
        }
        assert!(
            validate_state_bounds(&state)
                .expect_err("oversized root turn set must fail")
                .contains("too many root turns")
        );
        state.root_turns.clear();
        state.root_session_id = "x"
            .repeat(usize::try_from(MAXIMUM_ADAPTER_STATE_BYTES).expect("state bound fits usize"));
        assert!(
            save_state(temporary.path(), &state)
                .expect_err("oversized state persistence must fail")
                .contains("byte bound")
        );
        assert!(!temporary.path().join("adapter-state.json").exists());
    }

    #[test]
    fn adapter_state_recovers_only_from_a_valid_bounded_previous_snapshot() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        fs::write(temporary.path().join("adapter-state.json"), b"not json")
            .expect("corrupt current state");
        let previous = AdapterState {
            version: 7,
            ..AdapterState::default()
        };
        fs::write(
            temporary.path().join("adapter-state.previous.json"),
            serde_json::to_vec(&previous).expect("previous state JSON"),
        )
        .expect("previous state");
        assert_eq!(
            load_state(temporary.path())
                .expect("recovered state")
                .version,
            7
        );
    }

    #[test]
    fn doctor_human_output_requires_the_canonical_check_shape() {
        assert!(
            print_doctor_response(&json!({
                "ok": true,
                "checks": [{"name":"service","status":"pass","detail":"ready"}]
            }))
            .is_ok()
        );
        assert!(print_doctor_response(&json!({"ok": false})).is_err());
    }

    #[test]
    fn doctor_requires_the_exact_safe_mcp_manifest() {
        let canonical = json!({"mcpServers":{"acyclic":{
            "command":"node",
            "args":["./bin/acyclic.js","__mcp-commandless"],
            "cwd":".",
            "default_tools_approval_mode":"prompt"
        }}});
        assert!(valid_codex_mcp_manifest(&canonical));
        let mut injected = canonical.clone();
        injected["mcpServers"]["acyclic"]["args"] = json!([
            "--require",
            "./evil.js",
            "./bin/acyclic.js",
            "__mcp-commandless"
        ]);
        assert!(!valid_codex_mcp_manifest(&injected));
        let mut auto_approved = canonical;
        auto_approved["mcpServers"]["acyclic"]["default_tools_approval_mode"] = json!("approve");
        assert!(!valid_codex_mcp_manifest(&auto_approved));
        let mut tool_override = json!({"mcpServers":{"acyclic":{
            "command":"node",
            "args":["./bin/acyclic.js","__mcp-commandless"],
            "cwd":".",
            "default_tools_approval_mode":"prompt",
            "tools":{"acyclic":{"approval_mode":"approve"}}
        }}});
        assert!(!valid_codex_mcp_manifest(&tool_override));
        tool_override["mcpServers"]["acyclic"]
            .as_object_mut()
            .expect("server")
            .remove("tools");
        tool_override["mcpServers"]["acyclic"]["env"] =
            json!({"NODE_OPTIONS":"--require ./evil.js"});
        assert!(!valid_codex_mcp_manifest(&tool_override));
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
        let patch = json!({
            "command": "*** Begin Patch\n*** Add File: escape/file\n*** End Patch"
        });
        assert!(rewrite_tool_input("apply_patch", patch, temporary.path(), &child).is_err());
    }
}
