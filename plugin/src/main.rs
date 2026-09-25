//! Unified Acyclic CLI, service, hook bridge, MCP bridge, and installer.

#![allow(clippy::cognitive_complexity, clippy::too_many_lines)]

mod control_protocol;

use acyclic_fs::demand::native::NativeDemandSource;
use acyclic_fs::demand::{
    DemandDirectoryObserver, DemandSource, FilteredDemandSource, SourceReference,
};
use acyclic_fs::kernel::{FileKind, NamespacePath};
use acyclic_fs::model::{
    CheckoutMode, FilesystemProfile, GenerationSelector, Lifecycle, VolumeConfig, VolumeLimits,
};
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
    WorkspaceContextState, WorkspaceDelete, WorkspaceError, WorkspaceMultiRootPublisherError,
    WorkspaceOperationFinish, WorkspacePathApply, WorkspaceRestore, WorkspaceRootId,
    apply_git_patch_with_permit, blame_git_generations, capture_baseline_with_policy,
    capture_git_compatible_generation, capture_git_compatible_generation_at,
    capture_git_compatible_generation_incremental, capture_watch_batch_with_policy,
    git_compatible_diff_counts, grep_git_generation, resolve_merge_plan, walk_git_tree,
};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Read, Seek, Write};
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
#[cfg(any(test, not(target_os = "linux")))]
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex as AsyncMutex, watch};

use acyclic_native_runtime::{Durability, RenameMode, durable_rename, sync_file, sync_parent};
use control_protocol::{ControlEnvelope, ControlLedger, LedgerDecision};
use fs2::FileExt as _;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicU64, Ordering};

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
    roots: Arc<AsyncMutex<BTreeMap<PathBuf, Arc<AsyncMutex<SharedRootRegistration>>>>>,
}

#[derive(Default)]
struct SharedRootRegistration {
    live: Weak<SharedPhysicalRoot>,
    reference: Option<Arc<Mutex<SourceReference>>>,
    native_identity: Option<[u8; 16]>,
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
    watcher: Arc<Mutex<NativeWatch>>,
    reference: Arc<Mutex<SourceReference>>,
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
        self.observe_with(|| {
            let mut watcher = self
                .watcher
                .lock()
                .map_err(|_| "physical root watcher state is poisoned".to_owned())?;
            watcher.fence(WATCH_FENCE_TIMEOUT).map_err(display)?;
            Ok(watcher
                .poll(65_536, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(display)?
                .value)
        })
    }

    fn observe_with(
        &self,
        poll: impl FnOnce() -> Result<WatchBatch, String>,
    ) -> Result<SharedRootObservation, String> {
        // Serialize the full observation transaction across sessions sharing this root.
        // A watcher poll and the source epoch it publishes must be indivisible.
        let mut reference = self
            .reference
            .lock()
            .map_err(|_| "physical root reference state is poisoned".to_owned())?;
        let prior_source = self.source.reference();
        let batch = poll()?;
        let consumed_changes =
            !matches!(&batch, WatchBatch::Changes { changes, .. } if changes.is_empty());
        let source = if consumed_changes {
            self.source.inner().invalidate()
        } else {
            prior_source
        };
        *reference = source;
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
        let registration =
            {
                let mut roots = self.roots.lock().await;
                Arc::clone(roots.entry(canonical.clone()).or_insert_with(|| {
                    Arc::new(AsyncMutex::new(SharedRootRegistration::default()))
                }))
            };
        let mut registration = registration.lock().await;
        if let Some(native_identity) = registration.native_identity {
            let identity_path = canonical.clone();
            let observed_identity = tokio::task::spawn_blocking(move || {
                HostRoot::open(&identity_path).map(|root| root.identity().to_bytes())
            })
            .await
            .map_err(|error| format!("physical root validation worker failed: {error}"))?
            .map_err(display)?;
            let live = registration.live.upgrade();
            if observed_identity == native_identity {
                let retained = *registration
                    .reference
                    .as_ref()
                    .ok_or_else(|| "shared root registration lost its source reference".to_owned())?
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if admission
                    .reference()
                    .is_some_and(|expected| expected.identity != retained.identity)
                {
                    return Err(format!(
                        "physical root {} is already registered with another source identity",
                        canonical.display()
                    ));
                }
                if let Some(shared) = live {
                    shared.validate_path_identity(canonical.clone()).await?;
                    return Ok(shared);
                }
            } else {
                if live.is_some() || matches!(admission, SharedRootAdmission::Resume(_)) {
                    return Err(format!(
                        "physical root {} changed identity while it was in use",
                        canonical.display()
                    ));
                }
                *registration = SharedRootRegistration::default();
            }
        }

        let retained = registration.reference.as_ref().map(|reference| {
            *reference
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        let watcher_path = canonical.clone();
        let watcher = tokio::task::spawn_blocking(move || open_native_watcher(&watcher_path))
            .await
            .map_err(|error| format!("physical root watcher worker failed: {error}"))??;
        let source = open_lazy_source(
            &canonical,
            retained.or(admission.reference()),
            Arc::clone(&watcher) as Arc<dyn DemandDirectoryObserver>,
        )
        .await?;
        let watched_identity = watcher
            .lock()
            .map_err(|_| "physical root watcher state is poisoned".to_owned())?
            .root_identity()
            .to_bytes();
        if source.inner().root_identity().to_bytes() != watched_identity {
            return Err(format!(
                "physical root {} changed identity during admission",
                canonical.display()
            ));
        }
        if matches!(admission, SharedRootAdmission::Resume(_)) {
            source.inner().invalidate();
        }
        let reference = Arc::new(Mutex::new(source.reference()));
        let native_identity = source.inner().root_identity();
        let shared = Arc::new(SharedPhysicalRoot {
            source,
            watcher,
            reference: Arc::clone(&reference),
        });
        *registration = SharedRootRegistration {
            live: Arc::downgrade(&shared),
            reference: Some(reference),
            native_identity: Some(native_identity.to_bytes()),
        };
        Ok(shared)
    }

    async fn prune(&self) {
        self.roots.lock().await.retain(|_, registration| {
            if Arc::strong_count(registration) != 1 {
                return true;
            }
            registration
                .try_lock()
                .map_or(true, |state| state.live.strong_count() != 0)
        });
    }

    #[cfg(test)]
    async fn live_roots(&self) -> usize {
        let registrations = self
            .roots
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut live = 0;
        for registration in registrations {
            live += usize::from(registration.lock().await.live.strong_count() > 0);
        }
        live
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
            match workspace
                .mount(
                    &path,
                    MountOptions::read_write().publication(acyclic_fs::MountPublication::Manual),
                )
                .await
            {
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

    /// Publishes every route, then detaches them all, so a publication
    /// failure leaves every route mounted and publishing again.
    async fn unmount(&self) -> Result<(), String> {
        self.sync().await?;
        // Each route's own unmount is exactly this publication followed by
        // its detach; publishing once per route is enough.
        self.abandon()
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
        let directory = root_materialization_directory(&physical.path, operation_id)
            .map_err(PluginRootMaterializerError)?;
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
            .remove_materialization_async(operation_id)
            .await
            .map_err(|error| PluginRootMaterializerError(error.to_string()))?;
        let directory = root_materialization_directory(&physical.path, operation_id)
            .map_err(PluginRootMaterializerError)?;
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

const AGENT_GUIDANCE: &str = "Acyclic gives native subagents independent, recursively forked workspace contexts. Supported filesystem tools are routed to the child's mounted workspace; never target a parent's original path. Use `acyclic git` for history inside managed workspaces; bare `git` is unrelated. Run each `acyclic ...` command as its own shell invocation, without shell operators or unrelated commands. Children appear under `agents/...`: inspect with `acyclic agents` and `acyclic git diff <ref>`, merge a direct child with `acyclic git merge <ref>`, and remove an unwanted subtree with `acyclic discard <ref>`. Start independent or dependent work speculatively as soon as you can state its assumptions; reconcile, merge, or restart when upstream changes. For debugging, freely add logs, probes, and tests in a child, then normally discard it after confirming the issue. For exploration, run hypotheses in parallel and merge only useful results. Descendants publish upward one parent at a time.";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChildWorkspaceSupport {
    RecursiveToolRouting,
    RootLifecycleOnly,
    CliOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HostAdapterProfile {
    child_workspaces: ChildWorkspaceSupport,
    stable_child_identity: bool,
}

fn host_adapter_profile(host: &str) -> HostAdapterProfile {
    match host {
        "codex" | "claude-code" => HostAdapterProfile {
            child_workspaces: ChildWorkspaceSupport::RecursiveToolRouting,
            stable_child_identity: true,
        },
        "copilot" => HostAdapterProfile {
            child_workspaces: ChildWorkspaceSupport::RootLifecycleOnly,
            stable_child_identity: false,
        },
        _ => HostAdapterProfile {
            child_workspaces: ChildWorkspaceSupport::CliOnly,
            stable_child_identity: false,
        },
    }
}

fn capability_guidance(host: &str) -> &'static str {
    let profile = host_adapter_profile(host);
    match (profile.child_workspaces, profile.stable_child_identity) {
        (ChildWorkspaceSupport::RecursiveToolRouting, true) => {
            "Recursive lifecycle routing is active. This claim covers recognized tool routing, not process-level confinement; full confinement requires a passing host/platform escape qualification."
        }
        (ChildWorkspaceSupport::RootLifecycleOnly, false) => {
            "Only root lifecycle routing is available because this host does not provide a stable child identity. Native child workspace routing is not claimed."
        }
        (ChildWorkspaceSupport::CliOnly, false) => {
            "Child work is CLI-only because this host does not expose a correlated native-subagent lifecycle. Native child workspace routing or confinement is not claimed."
        }
        _ => "This adapter has an invalid capability profile and cannot route child work.",
    }
}

fn guidance_for(host: &str) -> String {
    if host_adapter_profile(host).child_workspaces == ChildWorkspaceSupport::RecursiveToolRouting {
        format!("{AGENT_GUIDANCE} {}", capability_guidance(host))
    } else {
        format!(
            "Acyclic provides a Git-shaped local history CLI without intercepting bare `git`. Use `acyclic git` from the current workspace. {}",
            capability_guidance(host)
        )
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
                        proof: None,
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
                let proof = captured
                    .authenticated_proof(operation_id, &self.distributed.lineage())
                    .await
                    .map_err(display)?;
                Ok(GitFilesystemResult::Captured {
                    tree: GitTreeRef::exact(
                        captured.generation.workspace_id(),
                        captured.generation.id(),
                    ),
                    tracked_paths: captured.tracked_paths,
                    proof,
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
            GitFilesystemAction::Diff {
                from,
                to,
                tracked_paths,
            } => {
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
                    let diff = git_compatible_diff_counts(
                        &from,
                        &to,
                        tracked_paths,
                        100_000,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(display)?;
                    json!({
                        "from": hex::encode(from.id().digest().as_bytes()),
                        "to": hex::encode(to.id().digest().as_bytes()),
                        "fileChanges": diff.file_changes,
                        "bindingChanges": diff.binding_changes
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
                    JoinOutcome::Applied(application)
                    | JoinOutcome::AlreadyApplied(application) => application.into_generation(),
                    JoinOutcome::NoChanges(generation) => generation,
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
            .remove_materialization_async(operation_id)
            .await
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
    Ok((
        git_ignore_policy(workspace).await?,
        merge_drivers_for(workspace).await?,
    ))
}

async fn git_ignore_policy(workspace: &LocalLazyWorkspace) -> Result<GitIgnorePolicy, String> {
    let ignore_text = match workspace.read("/.gitignore", 1024 * 1024).await {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(acyclic_fs::LazyWorkspaceError::NotFound) => String::new(),
        Err(error) => return Err(display(error)),
    };
    Ok(GitIgnorePolicy::parse(&format!(
        "{ignore_text}\n{}",
        reserved_ignore_rules()
    )))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RootBinding {
    root_id: [u8; 16],
    path: PathBuf,
    repository_workspace_id: [u8; 16],
    source_identity: [u8; 16],
    source_epoch: u64,
    native_root_identity: [u8; 16],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RouteRoot {
    root_id: [u8; 16],
    repository_workspace_id: [u8; 16],
    published_generation: [u8; 32],
}

impl RouteRoot {
    fn id(&self) -> WorkspaceRootId {
        WorkspaceRootId::from_bytes(self.root_id)
    }

    fn mount_name(&self) -> String {
        route_name(self.id())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Route {
    agent_id: String,
    turn_id: String,
    context_id: [u8; 16],
    root_id: [u8; 16],
    parent_agent_id: String,
    roots: BTreeMap<String, RouteRoot>,
    mount_path: PathBuf,
    lifecycle: RouteLifecycle,
}

impl Route {
    fn active_path(&self) -> Result<PathBuf, String> {
        let active = self
            .roots
            .get(&root_key(WorkspaceRootId::from_bytes(self.root_id)))
            .ok_or_else(|| "persisted route is missing its active root".to_owned())?;
        Ok(self.mount_path.join(active.mount_name()))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RouteLifecycle {
    #[default]
    Mounting,
    Active,
    StopRequested,
    Frozen,
}

impl RouteLifecycle {
    fn accepts_tools(self) -> bool {
        self == Self::Active
    }

    fn needs_mount(self) -> bool {
        self != Self::Frozen
    }

    fn is_frozen(self) -> bool {
        self == Self::Frozen
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingSpawn {
    parent_agent_id: String,
    tool_use_id: String,
    active_root_id: Option<[u8; 16]>,
    expires_at_millis: u64,
    workspace_name: String,
    fork_key: [u8; 16],
    roots: BTreeMap<String, RouteRoot>,
    mount_path: PathBuf,
    lifecycle: PendingSpawnLifecycle,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PendingSpawnLifecycle {
    #[default]
    Preparing,
    Mounting,
    Prepared,
    Discarding,
}

impl PendingSpawn {
    fn context_id(&self) -> WorkspaceContextId {
        WorkspaceContextId::from_bytes(self.fork_key)
    }

    fn mount_name(&self) -> String {
        compact_id(&self.fork_key)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LeaseRecord {
    agent_id: String,
    turn_id: String,
    tool_name: String,
    roots: BTreeMap<String, RootLeaseRecord>,
    expires_at_millis: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RootLeaseRecord {
    root_id: [u8; 16],
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
    repository_workspace_ids: Vec<[u8; 16]>,
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
    active: bool,
    root_agent_id: String,
    root_turns: BTreeSet<String>,
    root_context_id: [u8; 16],
    root_id: [u8; 16],
    roots: BTreeMap<String, RootBinding>,
    pending_root_registration: bool,
    pending_root_adoptions: BTreeSet<String>,
    routes: BTreeMap<String, Route>,
    turns: BTreeMap<String, String>,
    pending: VecDeque<PendingSpawn>,
    leases: BTreeMap<String, LeaseRecord>,
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
    pending_mounts: BTreeMap<[u8; 16], LocalMount>,
    /// The last save was left unflushed; see [`Survives::ServiceCrash`].
    unflushed: bool,
    slots: StateSlots,
    #[cfg(test)]
    owns_local_root: bool,
    #[cfg(test)]
    fail_next_flush: bool,
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
    #[cfg(test)]
    fail_after_root_intent: bool,
}

fn workspace_mount_root(config_root: &Path) -> PathBuf {
    config_root.join("w")
}

fn root_workspace_name(session_id: &str, root: &Path) -> String {
    format!(
        "root-{}-{}",
        short_hash(session_id.as_bytes()),
        short_hash(root.as_os_str().to_string_lossy().as_bytes()),
    )
}

fn validate_persisted_route_paths(config_root: &Path, route: &Route) -> Result<(), String> {
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
    async fn open(data: PathBuf) -> Result<Self, String> {
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

    async fn open_with(
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

    fn workspace_mount_root(&self) -> PathBuf {
        workspace_mount_root(&self.config_root)
    }

    fn validate_route_paths(&self, route: &Route) -> Result<(), String> {
        validate_persisted_route_paths(&self.config_root, route)
    }

    fn validate_pending_spawn_paths(&self) -> Result<(), String> {
        for pending in &self.state.pending {
            if pending.mount_path != self.workspace_mount_root().join(pending.mount_name()) {
                return Err(
                    "persisted pending spawn mount is outside its workspace root".to_owned(),
                );
            }
        }
        Ok(())
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

    fn parent_repository_id(
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

    async fn record_publication_history(
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

    async fn finalize_applied_publication(
        &mut self,
        coordinator: &LocalPublicationCoordinator,
        parent_agent: &str,
        child_agent: &str,
        publication: &MultiRootPublication,
    ) -> Result<(), String> {
        self.record_publication_history(parent_agent, child_agent, publication)
            .await
            .map_err(|error| format!("recording published compatibility history: {error}"))?;
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
            self.finalize_applied_publication(&coordinator, caller, &child_agent, publication)
                .await?;
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
        let recovered_agents = expired
            .iter()
            .map(|record| record.agent_id.clone())
            .collect::<BTreeSet<_>>();
        for agent_id in &recovered_agents {
            if let Some(mount) = self.mounts.remove(agent_id) {
                mount.abandon().map_err(|error| {
                    format!("cannot fence expired agent '{agent_id}' before recovery: {error}")
                })?;
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

    async fn restore_root(&mut self) -> Result<(), String> {
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

    async fn validate_routes_against_contexts(&self) -> Result<(), String> {
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

    async fn session_start(&mut self, input: Value) -> Result<Value, String> {
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
        self.persist_unflushed()?;
        Ok(json!({"suppressOutput": true}))
    }

    async fn pre_tool(&mut self, input: Value) -> Result<Value, String> {
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

    async fn rollback_started_leases(
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
        let route_path = route.mount_path.join(route_root.mount_name());
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

    async fn post_tool(&mut self, input: Value) -> Result<Value, String> {
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

    fn has_live_descendant(&self, ancestor: &str) -> bool {
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

    async fn finalize_requested_stops(&mut self, agent_id: &str) -> Result<(), String> {
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
    async fn prepare_pending_spawn(
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

    fn pending_active_root(&self, pending: &PendingSpawn) -> Result<WorkspaceRootId, String> {
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

    async fn discard_expired_pending_spawns(&mut self, now: u64) -> Result<(), String> {
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

    async fn recover_pending_spawns(&mut self) -> Result<(), String> {
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

    async fn discard_pending_spawn(&mut self, fork_key: [u8; 16]) -> Result<(), String> {
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
                WorkspaceDelete::Deleted | WorkspaceDelete::AlreadyDeleted => {}
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
            mount.unmount().await?;
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
        let observation = tokio::task::spawn_blocking({
            let physical = Arc::clone(&physical);
            move || physical.observe()
        })
        .await
        .map_err(|error| format!("physical root observation worker failed: {error}"))??;
        let source_advanced_elsewhere = binding.source_epoch != observation.prior_source.epoch;
        if !observation.consumed_changes && !source_advanced_elsewhere {
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

    async fn propagate_parent_advance(
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

    fn persist(&mut self) -> Result<(), String> {
        self.slots
            .save(&self.data, &self.state, Survives::PowerLoss)?;
        // A flushed save is a whole snapshot, so it covers any unflushed one.
        self.unflushed = false;
        Ok(())
    }

    /// Saves the last transition of a request without flushing it; see
    /// [`Survives::ServiceCrash`].
    fn persist_unflushed(&mut self) -> Result<(), String> {
        self.slots
            .save(&self.data, &self.state, Survives::ServiceCrash)?;
        self.unflushed = true;
        Ok(())
    }

    /// Flushes an unflushed save. Every request starts here, so nothing ever
    /// acts on a transition that a power loss could still undo.
    async fn make_durable(&mut self) -> Result<(), String> {
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
    async fn shutdown(self) -> Result<(), String> {
        self.close(false).await.map(drop)
    }

    /// Shuts the session down and hands back what it knows of its state
    /// slots. `terminal_save` says the caller follows a successful close with
    /// a flushed save of the whole session, which then makes every earlier
    /// save durable; otherwise the close leaves nothing unflushed itself.
    async fn close(mut self, terminal_save: bool) -> Result<StateSlots, String> {
        // Only finishing a lease or publishing a mount builds a durable
        // effect on the session's state, and nothing may build one on a save
        // a power loss could still undo. A close with neither saves nothing
        // until the terminal save.
        let has_leases = !self.state.leases.is_empty();
        if has_leases || !self.mounts.is_empty() || !self.pending_mounts.is_empty() {
            self.make_durable().await?;
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

async fn wait_for_root_release(
    barrier: Option<acyclic_native_runtime::OwnershipReleaseBarrier>,
) -> Result<(), String> {
    let Some(barrier) = barrier else {
        return Ok(());
    };
    tokio::task::spawn_blocking(move || barrier.wait())
        .await
        .map_err(|error| format!("filesystem release barrier failed: {error}"))
}

fn rewrite_tool_input(
    tool_name: &str,
    mut input: Value,
    root: &Path,
    child: &Path,
) -> Result<Value, String> {
    let root_text = root.to_string_lossy();
    let child_text = child.to_string_lossy();
    reject_original_root_references(&input, None, &root_text)?;
    rewrite_strings(&mut input, &root_text, &child_text);
    rewrite_relative_tool_paths(&mut input, None, child)?;
    if matches!(tool_name, "Bash" | "PowerShell") {
        let command_key = if input.get("command").is_some() {
            "command"
        } else {
            "cmd"
        };
        let command = input
            .get(command_key)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{tool_name} tool input lacks a command string"))?;
        let command = shell_command_in_directory(tool_name, command, child);
        let object = input
            .as_object_mut()
            .ok_or_else(|| format!("{tool_name} tool input must be an object"))?;
        object.insert(command_key.to_owned(), Value::String(command));
        if object.contains_key("workdir") {
            object.insert(
                "workdir".to_owned(),
                Value::String(child.to_string_lossy().into_owned()),
            );
        }
    } else if is_exec_command_tool(tool_name) {
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

fn shell_command_in_directory(tool_name: &str, command: &str, directory: &Path) -> String {
    let directory = directory.to_string_lossy();
    if tool_name == "PowerShell" {
        format!(
            "Set-Location -LiteralPath '{}';\n{command}",
            directory.replace('\'', "''")
        )
    } else {
        let directory = if cfg!(windows) {
            directory.replace('\\', "/")
        } else {
            directory.into_owned()
        };
        format!(
            "cd -- '{}' &&\n{command}",
            directory.replace('\'', "'\"'\"'")
        )
    }
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
    if !(matches!(tool_name, "Bash" | "PowerShell") || is_exec_command_tool(tool_name)) {
        return Ok(false);
    }
    let command = input
        .get("cmd")
        .or_else(|| input.get("command"))
        .and_then(Value::as_str)
        .ok_or_else(|| "shell tool input lacks a string command".to_owned())?;
    let Some(program) = shell_program(command) else {
        return Ok(false);
    };
    let acyclic = Path::new(&program)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.eq_ignore_ascii_case("acyclic") || name.eq_ignore_ascii_case("acyclic.exe")
        });
    if !acyclic {
        return Ok(false);
    }
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

fn shell_program(command: &str) -> Option<String> {
    let mut program = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in command.trim_start().chars() {
        if escaped {
            program.push(character);
            escaped = false;
            continue;
        }
        match quote {
            Some(expected) if character == expected => quote = None,
            _ if character == '\\' => escaped = true,
            None if matches!(character, '\'' | '"') => quote = Some(character),
            None if character.is_whitespace() || ";|&<>$`()".contains(character) => break,
            Some(_) | None => program.push(character),
        }
    }
    (!program.is_empty() && quote.is_none() && !escaped).then_some(program)
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
    if (matches!(tool_name, "Bash" | "PowerShell") || is_exec_command_tool(tool_name))
        && let Some(command) = input
            .get("cmd")
            .or_else(|| input.get("command"))
            .and_then(Value::as_str)
    {
        validate_shell_paths(command, child)?;
    }
    validate_structured_paths(input, None, child)
}

fn is_exec_command_tool(tool_name: &str) -> bool {
    tool_name == "exec_command" || tool_name.ends_with("__exec_command")
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
    observer: Arc<dyn DemandDirectoryObserver>,
) -> Result<Arc<LocalLazySource>, String> {
    let limits = VolumeLimits::default();
    let source = match reference {
        Some(reference) => {
            NativeDemandSource::open_with_reference_and_observer(
                root,
                native_filesystem_profile(),
                limits,
                reference,
                observer,
            )
            .await
        }
        None => {
            NativeDemandSource::open_with_observer(
                root,
                native_filesystem_profile(),
                limits,
                observer,
            )
            .await
        }
    }
    .map_err(display)?;
    Ok(Arc::new(FilteredDemandSource::new(
        source,
        reserved_root_paths()?,
    )))
}

/// Host subtrees owned by version control or SDK state, never workspace
/// content. The order is the watcher fence placement preference: an existing
/// `.git` hosts fence cookies, and only a root without one gains `.acyclic-sdk`.
const RESERVED_ROOT_NAMES: [&str; 2] = [".acyclic-sdk", ".git"];

fn reserved_root_paths() -> Result<Vec<NamespacePath>, String> {
    let limits = VolumeLimits::default();
    RESERVED_ROOT_NAMES
        .iter()
        .map(|name| {
            let path = PortablePath::parse(&format!("/{name}"), limits).map_err(display)?;
            NamespacePath::from_portable_in_profile(&path, native_filesystem_profile(), limits)
                .map_err(display)
        })
        .collect()
}

fn reserved_ignore_rules() -> String {
    RESERVED_ROOT_NAMES
        .iter()
        .map(|name| format!("{name}/\n"))
        .collect()
}

fn open_native_watcher(root: &Path) -> Result<Arc<Mutex<NativeWatch>>, String> {
    let mut watcher = NativeWatch::open_with_profile(
        root,
        native_filesystem_profile(),
        NativeWatchOptions {
            limits: VolumeLimits::default(),
            maximum_queued_changes: 65_536,
            // Linux inotify has no constant-size recursive subscription.
            // NativeDemandSource registers only demanded directories instead.
            recursive: !cfg!(target_os = "linux"),
        },
    )
    .map_err(display)?;
    watcher
        .exclude_subtrees(&reserved_root_paths()?)
        .map_err(display)?;
    watcher.accept_lazy_baseline().map_err(display)?;
    Ok(Arc::new(Mutex::new(watcher)))
}

const fn native_filesystem_profile() -> FilesystemProfile {
    if cfg!(windows) {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    }
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
    profile: FilesystemProfile,
) -> Result<NamespacePath, String> {
    let candidate = Path::new(path);
    let relative = if candidate.is_absolute() {
        candidate
            .strip_prefix(route.mount_path.join(root.mount_name()))
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
    NamespacePath::from_portable_in_profile(&portable, profile, limits).map_err(display)
}

fn namespace_path_text(path: &NamespacePath) -> Result<String, String> {
    if path.is_root() {
        return Ok("/".to_owned());
    }
    let components = path
        .components()
        .iter()
        .map(|name| {
            name.unicode_text()
                .map(|value| value.into_owned())
                .ok_or_else(|| "non-Unicode path cannot be presented to an agent".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("/{}", components.join("/")))
}

fn is_filesystem_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "Bash"
            | "PowerShell"
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

fn is_known_non_filesystem_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "web__run"
            | "web.run"
            | "webrun"
            | "collaboration.followup_task"
            | "collaboration.interrupt_agent"
            | "collaboration.list_agents"
            | "collaboration.send_message"
            | "collaboration.wait_agent"
            | "collaborationfollowup_task"
            | "collaborationinterrupt_agent"
            | "collaborationlist_agents"
            | "collaborationsend_message"
            | "collaborationwait_agent"
            | "followup_task"
            | "interrupt_agent"
            | "list_agents"
            | "send_message"
            | "wait_agent"
            | "get_goal"
            | "functions.get_goal"
            | "functionsget_goal"
            | "update_goal"
            | "functions.update_goal"
            | "functionsupdate_goal"
            | "request_user_input"
            | "functions.request_user_input"
            | "functionsrequest_user_input"
            | "mcp__codex_app__attach_artifact"
            | "mcp__codex_app__automation_update"
            | "mcp__codex_app__capture_screen_context"
            | "mcp__codex_app__consume_usage_reset"
            | "mcp__codex_app__create_sidebar_section"
            | "mcp__codex_app__delete_sidebar_section"
            | "mcp__codex_app__end_realtime_voice_call"
            | "mcp__codex_app__get_handoff_status"
            | "mcp__codex_app__get_usage_limits"
            | "mcp__codex_app__list_archived_threads"
            | "mcp__codex_app__list_artifacts"
            | "mcp__codex_app__list_projects"
            | "mcp__codex_app__list_threads"
            | "mcp__codex_app__load_workspace_dependencies"
            | "mcp__codex_app__move_project_to_sidebar_section"
            | "mcp__codex_app__move_thread_to_sidebar_section"
            | "mcp__codex_app__navigate_to_codex_page"
            | "mcp__codex_app__read_thread"
            | "mcp__codex_app__read_thread_terminal"
            | "mcp__codex_app__remove_artifact"
            | "mcp__codex_app__rename_sidebar_section"
            | "mcp__codex_app__reorder_section"
            | "mcp__codex_app__reorder_sidebar_projects"
            | "mcp__codex_app__reorder_sidebar_sections"
            | "mcp__codex_app__send_message_to_thread"
            | "mcp__codex_app__set_thread_archived"
            | "mcp__codex_app__set_thread_title"
            | "mcp__codex_app__share_thread"
            | "mcp__codex_app__wait_threads"
    )
}

fn is_spawn_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "spawn_agent" | "Agent" | "collaboration.spawn_agent" | "collaborationspawn_agent"
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

fn rewrite_relative_tool_paths(
    value: &mut Value,
    key: Option<&str>,
    child: &Path,
) -> Result<(), String> {
    match value {
        Value::String(path) if key.is_some_and(is_path_field) => {
            let path = Path::new(path);
            if path
                .components()
                .any(|component| matches!(component, Component::ParentDir))
            {
                return Err("parent traversal is denied in an isolated agent workspace".to_owned());
            }
            if path.is_relative() {
                *value = Value::String(child.join(path).to_string_lossy().into_owned());
            }
            Ok(())
        }
        Value::Array(values) => {
            for value in values {
                rewrite_relative_tool_paths(value, key, child)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                rewrite_relative_tool_paths(value, Some(key), child)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn rewrite_patch_header(line: &str, child: &Path) -> Result<String, String> {
    for prefix in [
        "*** Add File: ",
        "*** Update File: ",
        "*** Delete File: ",
        "*** Move to: ",
        "*** Copy to: ",
    ] {
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

/// Bounds a watcher fence; a late notification forces a sound rescan.
const WATCH_FENCE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const MAXIMUM_ADAPTER_STATE_BYTES: u64 = 4 * 1024 * 1024;
const ADAPTER_STATE_VERSION: u32 = 4;
const MAXIMUM_ADAPTER_ROOTS: usize = 256;
const MAXIMUM_ADAPTER_ROUTES: usize = 4_096;
const MAXIMUM_ADAPTER_TURNS: usize = 16_384;
const MAXIMUM_ADAPTER_ROOT_TURNS: usize = 16_384;
const MAXIMUM_ADAPTER_PENDING: usize = 4_096;
const MAXIMUM_ADAPTER_LEASES: usize = 16_384;
const MAXIMUM_ADAPTER_DISCARDS: usize = 4_096;

/// Adapter state is saved into self-validating slots rewritten in place.
/// Flushed saves alternate between two slots, and a save only ever overwrites
/// the slot that does not hold the newest flushed save, so a torn write can
/// damage nothing but itself. Unflushed saves go to a third slot that loading
/// prefers only while it is intact and newer, so losing one to a power loss
/// leaves the last flushed save.
const ADAPTER_STATE_SLOTS: [&str; 3] = ["adapter-state.a", "adapter-state.b", "adapter-state.v"];

/// The failures an adapter-state transition must survive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Survives {
    /// Written without a flush, as the last action of a request. Nothing acts
    /// on it until it is durable: the session flushes it before its next
    /// request and before publishing it to other sessions, and loading a
    /// session makes it durable first. Losing it to a power loss therefore
    /// leaves exactly the state that a crash just before it leaves.
    ServiceCrash,
    /// Flushed before the caller continues.
    PowerLoss,
}
const ADAPTER_STATE_MAGIC: [u8; 8] = *b"ACYSTAT1";
/// Magic, little-endian generation, then the digest of generation and payload.
const ADAPTER_STATE_HEADER_BYTES: usize = 8 + 8 + 32;

enum StateSlot {
    Missing,
    Torn,
    Saved { generation: u64, payload: Vec<u8> },
}

impl StateSlot {
    const fn generation(&self) -> Option<u64> {
        match self {
            Self::Saved { generation, .. } => Some(*generation),
            Self::Missing | Self::Torn => None,
        }
    }
}

fn state_digest(generation: u64, payload: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&generation.to_le_bytes());
    hasher.update(payload);
    *hasher.finalize().as_bytes()
}

fn read_state_slot(path: &Path) -> Result<StateSlot, String> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(StateSlot::Missing),
        Err(error) => return Err(display(error)),
    };
    let bound = MAXIMUM_ADAPTER_STATE_BYTES + ADAPTER_STATE_HEADER_BYTES as u64;
    let mut bytes = Vec::new();
    file.take(bound + 1)
        .read_to_end(&mut bytes)
        .map_err(display)?;
    if bytes.len() as u64 > bound {
        return Err(format!(
            "adapter state exceeds the {MAXIMUM_ADAPTER_STATE_BYTES} byte bound"
        ));
    }
    let parsed = bytes.split_first_chunk::<8>().and_then(|(magic, rest)| {
        let (generation, rest) = rest.split_first_chunk::<8>()?;
        let (digest, payload) = rest.split_first_chunk::<32>()?;
        Some((magic, u64::from_le_bytes(*generation), digest, payload))
    });
    Ok(match parsed {
        Some((magic, generation, digest, payload))
            if *magic == ADAPTER_STATE_MAGIC && *digest == state_digest(generation, payload) =>
        {
            StateSlot::Saved {
                generation,
                payload: payload.to_vec(),
            }
        }
        _ => StateSlot::Torn,
    })
}

/// What the owning process knows of a session's state slots. It is read once,
/// when the session loads, and kept in step with every save, so a save writes
/// its target slot and reads nothing.
#[derive(Clone, Copy, Debug)]
struct StateSlots {
    /// The generation of each flushed slot's durable save; `None` when the
    /// slot is missing or torn, or its last write did not complete durably.
    /// Such a slot is never the newest flushed save, so it stays the target
    /// until a flushed save completes in it.
    flushed: [Option<u64>; 2],
    /// Whether each slot's directory entry is durable. The unflushed slot's
    /// is known only once this process flushed it.
    durable_entry: [bool; 3],
    /// The highest generation read or ever written, whether or not the write
    /// completed, so every save is newer than anything any slot can hold.
    last_generation: u64,
}

impl StateSlots {
    /// Reads every slot as `[flushed, flushed, unflushed]`.
    fn read(data: &Path) -> Result<(Self, [StateSlot; 3]), String> {
        let [first, second, unflushed] =
            ADAPTER_STATE_SLOTS.map(|name| read_state_slot(&data.join(name)));
        let slots = [first?, second?, unflushed?];
        let known = Self {
            flushed: [slots[0].generation(), slots[1].generation()],
            durable_entry: [
                !matches!(slots[0], StateSlot::Missing),
                !matches!(slots[1], StateSlot::Missing),
                false,
            ],
            last_generation: slots
                .iter()
                .filter_map(StateSlot::generation)
                .max()
                .unwrap_or(0),
        };
        Ok((known, slots))
    }

    /// The flushed slot holding the newest flushed save.
    fn newest_flushed(&self) -> usize {
        usize::from(self.flushed[1] > self.flushed[0])
    }

    /// Makes the unflushed slot's save durable, with its directory entry
    /// until that is.
    fn flush_unflushed(&mut self, data: &Path) -> Result<(), String> {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(data.join(ADAPTER_STATE_SLOTS[2]))
            .map_err(display)?;
        sync_file(&file, Durability::Full).map_err(display)?;
        if !self.durable_entry[2] {
            sync_parent(data, Durability::Full).map_err(display)?;
            self.durable_entry[2] = true;
        }
        Ok(())
    }

    fn save(
        &mut self,
        data: &Path,
        state: &AdapterState,
        survives: Survives,
    ) -> Result<(), String> {
        validate_state_version(state)?;
        validate_state_bounds(state)?;
        let mut serialized = BoundedJsonBuffer::new();
        serde_json::to_writer(&mut serialized, state)
            .ok()
            .filter(|()| serialized.bytes.len() as u64 <= MAXIMUM_ADAPTER_STATE_BYTES)
            .ok_or_else(|| {
                format!("adapter state exceeds the {MAXIMUM_ADAPTER_STATE_BYTES} byte bound")
            })?;
        let generation = self
            .last_generation
            .checked_add(1)
            .ok_or("adapter state generation is exhausted")?;
        self.last_generation = generation;
        let target = match survives {
            Survives::ServiceCrash => 2,
            Survives::PowerLoss => {
                let target = 1 - self.newest_flushed();
                if let Some(flushed) = self.flushed.get_mut(target) {
                    *flushed = None;
                }
                target
            }
        };
        let name = ADAPTER_STATE_SLOTS
            .get(target)
            .ok_or("adapter state slot is out of range")?;
        let mut bytes = Vec::with_capacity(ADAPTER_STATE_HEADER_BYTES + serialized.bytes.len());
        bytes.extend_from_slice(&ADAPTER_STATE_MAGIC);
        bytes.extend_from_slice(&generation.to_le_bytes());
        bytes.extend_from_slice(&state_digest(generation, &serialized.bytes));
        bytes.extend_from_slice(&serialized.bytes);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(data.join(name))
            .map_err(display)?;
        file.write_all(&bytes).map_err(display)?;
        file.set_len(bytes.len() as u64).map_err(display)?;
        if let (Some(flushed), Some(durable_entry)) = (
            self.flushed.get_mut(target),
            self.durable_entry.get_mut(target),
        ) {
            sync_file(&file, Durability::Full).map_err(display)?;
            if !*durable_entry {
                sync_parent(data, Durability::Full).map_err(display)?;
                *durable_entry = true;
            }
            *flushed = Some(generation);
        }
        Ok(())
    }
}

fn load_state(data: &Path) -> Result<(AdapterState, StateSlots), String> {
    let (mut slots, [first, second, unflushed]) = StateSlots::read(data)?;
    let (flushed, other) = if slots.newest_flushed() == 1 {
        (second, first)
    } else {
        (first, second)
    };
    let newest = if unflushed.generation() > flushed.generation() {
        // A process that exited before flushing its last save leaves it only
        // in memory; nothing may act on it before it is durable.
        slots.flush_unflushed(data)?;
        unflushed
    } else {
        flushed
    };
    let state = match (newest, other) {
        (StateSlot::Saved { payload, .. }, _) => {
            let state = serde_json::from_slice(&payload).map_err(display)?;
            validate_state_version(&state)?;
            validate_state_bounds(&state)?;
            state
        }
        // No flushed save was ever made, and no unflushed one survives.
        (StateSlot::Missing, StateSlot::Missing) => AdapterState {
            version: ADAPTER_STATE_VERSION,
            ..AdapterState::default()
        },
        _ => return Err("adapter state holds no completed save".to_owned()),
    };
    Ok((state, slots))
}

/// Loads a session directory's state without keeping its slots.
#[cfg(test)]
fn load_saved_state(data: &Path) -> Result<AdapterState, String> {
    load_state(data).map(|(state, _)| state)
}

/// Saves into a session directory that no loaded session owns.
#[cfg(test)]
fn save_state(data: &Path, state: &AdapterState, survives: Survives) -> Result<(), String> {
    StateSlots::read(data)?.0.save(data, state, survives)
}

/// When the session last completed a save, for newest-first recovery order.
fn state_modified(data: &Path) -> std::time::SystemTime {
    ADAPTER_STATE_SLOTS
        .iter()
        .filter_map(|slot| {
            data.join(slot)
                .metadata()
                .and_then(|metadata| metadata.modified())
                .ok()
        })
        .max()
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
}

fn validate_state_version(state: &AdapterState) -> Result<(), String> {
    if state.version == ADAPTER_STATE_VERSION {
        Ok(())
    } else {
        Err(format!(
            "unsupported Acyclic adapter state version {}; expected {ADAPTER_STATE_VERSION}. Leave the old store untouched or remove it manually",
            state.version
        ))
    }
}

fn validate_state_bounds(state: &AdapterState) -> Result<(), String> {
    for (name, observed, maximum) in [
        ("roots", state.roots.len(), MAXIMUM_ADAPTER_ROOTS),
        (
            "pending root adoptions",
            state.pending_root_adoptions.len(),
            MAXIMUM_ADAPTER_ROOTS,
        ),
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
    if !state
        .pending_root_adoptions
        .iter()
        .all(|key| state.roots.contains_key(key))
    {
        return Err("pending root adoption has no durable binding".to_owned());
    }
    if state
        .roots
        .iter()
        .any(|(key, root)| key != &root_key(WorkspaceRootId::from_bytes(root.root_id)))
        || state.routes.values().any(|route| {
            route
                .roots
                .iter()
                .any(|(key, root)| key != &root_key(WorkspaceRootId::from_bytes(root.root_id)))
        })
        || state.leases.values().any(|lease| {
            lease
                .roots
                .iter()
                .any(|(key, root)| key != &root_key(WorkspaceRootId::from_bytes(root.root_id)))
        })
    {
        return Err("adapter root map key differs from its typed root identity".to_owned());
    }
    if state.pending_root_registration
        && (state.root_session_id.is_empty()
            || state.root_id == [0; 16]
            || state.root_context_id == [0; 16]
            || state.roots.len() != 1
            || !state
                .roots
                .contains_key(&root_key(WorkspaceRootId::from_bytes(state.root_id))))
    {
        return Err("pending root registration is incomplete".to_owned());
    }
    if state
        .routes
        .values()
        .any(|route| route.roots.len() > MAXIMUM_ADAPTER_ROOTS)
        || state
            .pending
            .iter()
            .any(|pending| pending.roots.len() > MAXIMUM_ADAPTER_ROOTS)
        || state
            .leases
            .values()
            .any(|lease| lease.roots.len() > MAXIMUM_ADAPTER_ROOTS)
    {
        return Err("adapter route or lease exceeds the root bound".to_owned());
    }
    if state.pending.iter().any(|pending| {
        let pending_key = pending.mount_name();
        pending
            .mount_path
            .file_name()
            .and_then(|name| name.to_str())
            != Some(pending_key.as_str())
            || pending
                .mount_path
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                != Some("w")
            || pending
                .roots
                .iter()
                .any(|(key, root)| key != &root_key(root.id()))
            || (pending.lifecycle == PendingSpawnLifecycle::Preparing && !pending.roots.is_empty())
            || (matches!(
                pending.lifecycle,
                PendingSpawnLifecycle::Mounting | PendingSpawnLifecycle::Prepared
            ) && pending.roots.is_empty())
    }) {
        return Err("adapter pending spawn is structurally invalid".to_owned());
    }
    if state.pending_discards.values().any(|discard| {
        discard.agents.len() > MAXIMUM_ADAPTER_ROUTES
            || discard.agents.iter().any(|agent| {
                agent.repository_workspace_ids.is_empty()
                    || agent.repository_workspace_ids.len() > MAXIMUM_ADAPTER_ROOTS
                    || agent.workspaces.len() > MAXIMUM_ADAPTER_ROOTS
            })
    }) {
        return Err("adapter discard queue exceeds its structural bound".to_owned());
    }
    Ok(())
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

fn compact_id(bytes: &[u8; 16]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
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
    format!("r{}", compact_id(&root_id.into_bytes()))
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

fn derived_cleanup_key(fork_key: [u8; 16], root_id: WorkspaceRootId) -> IdempotencyKey {
    let mut input = Vec::with_capacity(39);
    input.extend_from_slice(&fork_key);
    input.extend_from_slice(&root_id.into_bytes());
    input.extend_from_slice(b"discard");
    let digest = blake3::hash(&input);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(digest.as_bytes().get(..16).unwrap_or(digest.as_bytes()));
    IdempotencyKey::from_bytes(bytes)
}

fn root_materialization_root(root: &Path) -> Result<PathBuf, String> {
    let digest = blake3::hash(root.as_os_str().to_string_lossy().as_bytes());
    Ok(root
        .parent()
        .ok_or_else(|| "root checkout has no same-filesystem staging parent".to_owned())?
        .join(".acyclic-m")
        .join(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&digest.as_bytes()[..16])))
}

fn root_materialization_directory(
    root: &Path,
    operation_id: OperationId,
) -> Result<PathBuf, String> {
    Ok(root_materialization_root(root)?.join(compact_id(&operation_id.into_bytes())))
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
const MAXIMUM_CONCURRENT_CONTROL_REQUESTS: usize = 64;
const CONTROL_RESPONSE_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(1);
const CONTROL_PROBE_WAIT: std::time::Duration = std::time::Duration::from_secs(2);
const CONTROL_HOOK_WAIT: std::time::Duration = std::time::Duration::from_secs(15);
const CONTROL_COMMAND_WAIT: std::time::Duration = std::time::Duration::from_secs(120);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ControlCommand {
    Ping,
    Doctor,
    Hook,
    Git,
    Agents,
    Discard,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ControlRequest {
    version: u32,
    command: ControlCommand,
    cwd: PathBuf,
    argv: Vec<String>,
    name: String,
    arguments: Value,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CliRouting {
    selected_cwd: PathBuf,
}

fn cli_routing(selected_cwd: PathBuf) -> Value {
    json!(CliRouting { selected_cwd })
}

fn selected_cli_cwd(request: &ControlRequest) -> Result<PathBuf, String> {
    if request.arguments.is_null() {
        return request.cwd.canonicalize().map_err(display);
    }
    let routing: CliRouting = serde_json::from_value(request.arguments.clone()).map_err(display)?;
    routing.selected_cwd.canonicalize().map_err(display)
}

struct ControlEndpoint {
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), String>>,
    #[cfg(all(test, not(target_os = "linux")))]
    accepted: Arc<tokio::sync::Notify>,
    #[cfg(all(unix, not(target_os = "linux")))]
    socket_path: PathBuf,
    #[cfg(target_os = "linux")]
    mailbox_path: PathBuf,
    #[cfg(all(windows, test))]
    pipe_path: String,
}

impl ControlEndpoint {
    async fn shutdown(self) -> Result<(), String> {
        let _ = self.shutdown.send(true);
        let result = self.task.await.map_err(display)?;
        #[cfg(all(unix, not(target_os = "linux")))]
        match fs::remove_file(&self.socket_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(display(error)),
        }
        #[cfg(target_os = "linux")]
        match fs::remove_dir_all(&self.mailbox_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(display(error)),
        }
        result
    }
}

async fn start_control_endpoint(
    control: Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    data: &Path,
) -> Result<ControlEndpoint, String> {
    fs::create_dir_all(data).map_err(display)?;
    let ledger = Arc::new(ControlLedger::open(data)?);
    #[cfg(windows)]
    let opaque_id = short_hash(data.as_os_str().to_string_lossy().as_bytes());
    let (shutdown, receiver) = watch::channel(false);
    #[cfg(all(test, not(target_os = "linux")))]
    let accepted = Arc::new(tokio::sync::Notify::new());

    #[cfg(target_os = "linux")]
    {
        let mailbox_path = prepare_linux_control_mailbox(data)?;
        let task = tokio::spawn(serve_linux_control_mailbox(
            mailbox_path.clone(),
            control,
            ledger,
            shutdown.clone(),
            receiver,
        ));
        Ok(ControlEndpoint {
            shutdown,
            task,
            mailbox_path,
        })
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let socket_path = prepare_unix_control_socket(data)?;
        match fs::remove_file(&socket_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(display(error)),
        }
        let listener = tokio::net::UnixListener::bind(&socket_path).map_err(display)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).map_err(display)?;
        let task = tokio::spawn(serve_unix_control(
            listener,
            control,
            ledger,
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
        // The first instance exists before this returns, so a started
        // endpoint accepts connections at once.
        let first = create_current_user_pipe(&pipe_path, true).map_err(display)?;
        let task = tokio::spawn(serve_windows_control(
            pipe_path,
            first,
            control,
            ledger,
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

/// Suffix of an exchange that its client is still building.
#[cfg(target_os = "linux")]
const LINUX_EXCHANGE_UNPUBLISHED: &str = ".new";
#[cfg(target_os = "linux")]
const LINUX_EXCHANGE_REQUEST: &str = "request";
#[cfg(target_os = "linux")]
const LINUX_EXCHANGE_CLAIMED: &str = "processing";
#[cfg(target_os = "linux")]
const LINUX_EXCHANGE_RESPONSE: &str = "response";

/// The mailbox of the Linux control transport, for hosts whose sandbox denies
/// connecting to a Unix socket but allows the runtime directory. Each request
/// is an exchange directory in it. The client builds the exchange under a name
/// ending in [`LINUX_EXCHANGE_UNPUBLISHED`], holding the request and a FIFO for
/// the response, then publishes it with one rename into the mailbox, which the
/// service watches. The service claims a published exchange by renaming its
/// request, writes the newline-terminated response into the FIFO and removes
/// the exchange; a client removes only an exchange that it gives up on.
#[cfg(target_os = "linux")]
fn linux_control_mailbox_path(data: &Path) -> PathBuf {
    unix_control_runtime_directory().join(format!(
        "service-{}.inbox",
        short_hash(data.as_os_str().as_encoded_bytes())
    ))
}

#[cfg(target_os = "linux")]
fn prepare_linux_control_mailbox(data: &Path) -> Result<PathBuf, String> {
    use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};

    let runtime = prepare_unix_control_runtime_directory()?;
    let mailbox = linux_control_mailbox_path(data);
    match fs::remove_dir_all(&mailbox) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(display(error)),
    }
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(&mailbox).map_err(display)?;
    debug_assert_eq!(mailbox.parent(), Some(runtime.as_path()));
    fs::set_permissions(&mailbox, fs::Permissions::from_mode(0o700)).map_err(display)?;
    Ok(mailbox)
}

#[cfg(target_os = "linux")]
fn open_linux_directory(
    directory: impl rustix::fd::AsFd,
    name: impl rustix::path::Arg,
) -> io::Result<rustix::fd::OwnedFd> {
    rustix::fs::openat(
        directory,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(errno_to_io)
}

#[cfg(target_os = "linux")]
async fn serve_linux_control_mailbox(
    mailbox: PathBuf,
    control: Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    ledger: Arc<ControlLedger>,
    shutdown_sender: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let mailbox_directory =
        Arc::new(open_linux_directory(rustix::fs::CWD, &mailbox).map_err(display)?);
    // Watching before the first scan means that every exchange is either found
    // by that scan or published later, which wakes another scan.
    let published = rustix::fs::inotify::init(
        rustix::fs::inotify::CreateFlags::CLOEXEC | rustix::fs::inotify::CreateFlags::NONBLOCK,
    )
    .and_then(|published| {
        rustix::fs::inotify::add_watch(
            &published,
            &mailbox,
            rustix::fs::inotify::WatchFlags::MOVED_TO
                | rustix::fs::inotify::WatchFlags::ONLYDIR
                | rustix::fs::inotify::WatchFlags::DONT_FOLLOW,
        )?;
        Ok(published)
    })
    .map_err(errno_to_io)
    .and_then(tokio::io::unix::AsyncFd::new)
    .map_err(display)?;

    let mut requests = tokio::task::JoinSet::new();
    let mut result = loop {
        if let Err(error) =
            claim_linux_mailbox_requests(&mailbox_directory, &control, &ledger, &mut requests)
        {
            break Err(error);
        }
        tokio::select! {
            ready = published.readable() => {
                let mut ready = match ready {
                    Ok(ready) => ready,
                    Err(error) => break Err(display(error)),
                };
                // Events only wake the next scan, which finds every published
                // exchange, including any whose event an overflow dropped.
                let mut events = [0_u8; 4096];
                while let Ok(Ok(_)) = ready.try_io(|published| {
                    rustix::io::read(published.get_ref(), &mut events).map_err(errno_to_io)
                }) {}
            }
            completed = requests.join_next(), if !requests.is_empty() => {
                if let Some(Err(error)) = completed {
                    break Err(format!("Acyclic mailbox request task failed: {error}"));
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break Ok(());
                }
            }
        }
    };
    let _ = shutdown_sender.send(true);
    while let Some(completed) = requests.join_next().await {
        if let (Ok(()), Err(error)) = (&result, completed) {
            result = Err(format!("Acyclic mailbox request task failed: {error}"));
        }
    }
    result
}

/// Claims every published exchange, up to the concurrency bound. The mailbox
/// is private and holds only exchanges in flight, so it is scanned in place.
#[cfg(target_os = "linux")]
fn claim_linux_mailbox_requests(
    mailbox_directory: &Arc<rustix::fd::OwnedFd>,
    control: &Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    ledger: &Arc<ControlLedger>,
    requests: &mut tokio::task::JoinSet<()>,
) -> Result<(), String> {
    use std::os::unix::ffi::OsStrExt as _;

    let entries = rustix::fs::Dir::read_from(&**mailbox_directory).map_err(display)?;
    for entry in entries {
        if requests.len() >= MAXIMUM_CONCURRENT_CONTROL_REQUESTS {
            break;
        }
        let entry = entry.map_err(display)?;
        let name = entry.file_name().to_bytes();
        if name.starts_with(b".")
            || name.ends_with(LINUX_EXCHANGE_UNPUBLISHED.as_bytes())
            || !entry.file_type().is_dir()
        {
            continue;
        }
        let name = std::ffi::OsStr::from_bytes(name).to_owned();
        let Ok(exchange) = open_linux_directory(&**mailbox_directory, &name) else {
            continue;
        };
        if rustix::fs::renameat(
            &exchange,
            LINUX_EXCHANGE_REQUEST,
            &exchange,
            LINUX_EXCHANGE_CLAIMED,
        )
        .is_err()
        {
            continue;
        }
        let exchange = LinuxMailboxExchange {
            mailbox: Arc::clone(mailbox_directory),
            exchange: Some(exchange),
            name,
        };
        let control = Arc::clone(control);
        let ledger = Arc::clone(ledger);
        requests.spawn(handle_linux_mailbox_request(exchange, control, ledger));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
async fn handle_linux_mailbox_request(
    exchange: LinuxMailboxExchange,
    control: Arc<impl ConcurrentControlRequestDispatcher>,
    ledger: Arc<ControlLedger>,
) {
    if let Some(directory) = &exchange.exchange {
        let mut response = match read_linux_control_file_at(directory, LINUX_EXCHANGE_CLAIMED) {
            Ok(request) if request.len() <= MAXIMUM_CONTROL_MESSAGE_BYTES => {
                match serde_json::from_slice::<ControlEnvelope<ControlRequest>>(&request) {
                    Ok(envelope) => dispatch_control_envelope(&control, &ledger, envelope).await,
                    Err(error) => invalid_control_request_response(&request, &error),
                }
            }
            Ok(_) => {
                uncorrelated_control_response("Acyclic control request exceeds the 4 MiB bound")
            }
            Err(error) => uncorrelated_control_response(&display(error)),
        };
        response.push(b'\n');
        // A client that gave up holds no reader, so opening fails at once.
        let sender = rustix::fs::openat(
            directory,
            LINUX_EXCHANGE_RESPONSE,
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(errno_to_io)
        .and_then(tokio::net::unix::pipe::Sender::from_owned_fd);
        if let Ok(mut sender) = sender {
            let _ = tokio::time::timeout(CONTROL_RESPONSE_DRAIN_GRACE, sender.write_all(&response))
                .await;
        }
    }
    exchange.remove();
}

/// Exchange files are bounded and live in a private runtime directory, so
/// reading one directly is cheaper than a hop through the blocking pool.
#[cfg(target_os = "linux")]
fn read_linux_control_file_at(directory: &rustix::fd::OwnedFd, name: &str) -> io::Result<Vec<u8>> {
    let file = rustix::fs::openat(
        directory,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(errno_to_io)?;
    let metadata = rustix::fs::fstat(&file).map_err(errno_to_io)?;
    if !rustix::fs::FileType::from_raw_mode(metadata.st_mode).is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Acyclic control message is not a regular file",
        ));
    }
    let file = std::fs::File::from(file);
    let mut request = Vec::with_capacity(MAXIMUM_CONTROL_MESSAGE_BYTES.min(64 * 1024));
    file.take(
        u64::try_from(MAXIMUM_CONTROL_MESSAGE_BYTES)
            .unwrap_or(u64::MAX)
            .saturating_add(1),
    )
    .read_to_end(&mut request)?;
    Ok(request)
}

/// One exchange directory in a Linux mailbox; see
/// [`linux_control_mailbox_path`].
#[cfg(target_os = "linux")]
struct LinuxMailboxExchange {
    mailbox: Arc<rustix::fd::OwnedFd>,
    exchange: Option<rustix::fd::OwnedFd>,
    name: std::ffi::OsString,
}

#[cfg(target_os = "linux")]
impl LinuxMailboxExchange {
    fn remove(&self) {
        if let Some(directory) = &self.exchange {
            for name in [
                LINUX_EXCHANGE_REQUEST,
                LINUX_EXCHANGE_CLAIMED,
                LINUX_EXCHANGE_RESPONSE,
            ] {
                let _ = rustix::fs::unlinkat(directory, name, rustix::fs::AtFlags::empty());
            }
        }
        let _ = rustix::fs::unlinkat(&*self.mailbox, &self.name, rustix::fs::AtFlags::REMOVEDIR);
    }
}

#[cfg(target_os = "linux")]
fn errno_to_io(error: rustix::io::Errno) -> io::Error {
    io::Error::from_raw_os_error(error.raw_os_error())
}

#[cfg(all(unix, not(target_os = "linux")))]
fn unix_control_socket_path(data: &Path) -> PathBuf {
    unix_control_runtime_directory().join(format!(
        "service-{}.sock",
        short_hash(data.as_os_str().as_encoded_bytes())
    ))
}

#[cfg(unix)]
#[allow(
    unsafe_code,
    reason = "geteuid has no preconditions and reads no memory"
)]
fn unix_control_runtime_directory() -> PathBuf {
    let uid = unsafe { libc::geteuid() };
    PathBuf::from("/tmp").join(format!("acyclic-{uid}"))
}

#[cfg(all(unix, not(target_os = "linux")))]
#[allow(
    unsafe_code,
    reason = "geteuid has no preconditions and reads no memory"
)]
fn prepare_unix_control_socket(data: &Path) -> Result<PathBuf, String> {
    let directory = prepare_unix_control_runtime_directory()?;

    // Unix-domain paths are short (typically 104-108 bytes), so an endpoint
    // cannot safely inherit the arbitrary length of the durable state path.
    // The installer allowlists this exact socket for Codex; peer credentials,
    // its private parent, and 0600 socket permissions authenticate clients.
    let socket = unix_control_socket_path(data);
    debug_assert_eq!(socket.parent(), Some(directory.as_path()));
    Ok(socket)
}

#[cfg(unix)]
#[allow(
    unsafe_code,
    reason = "geteuid has no preconditions and reads no memory"
)]
fn prepare_unix_control_runtime_directory() -> Result<PathBuf, String> {
    use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};

    let directory = unix_control_runtime_directory();
    match fs::symlink_metadata(&directory) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&directory) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(display(error)),
            }
        }
        Err(error) => return Err(display(error)),
    }
    let metadata = fs::symlink_metadata(&directory).map_err(display)?;
    let uid = unsafe { libc::geteuid() };
    if !metadata.file_type().is_dir() || metadata.uid() != uid {
        return Err("Acyclic workspace directory is not owned by the current user".to_owned());
    }
    if metadata.permissions().mode() & 0o777 != 0o700 {
        return Err("Acyclic runtime directory permissions must be 0700".to_owned());
    }
    Ok(directory)
}

#[cfg(all(unix, not(target_os = "linux")))]
async fn serve_unix_control(
    listener: tokio::net::UnixListener,
    control: Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    ledger: Arc<ControlLedger>,
    shutdown_sender: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
    #[cfg(test)] accepted: Arc<tokio::sync::Notify>,
) -> Result<(), String> {
    let mut connections = tokio::task::JoinSet::new();
    let result = loop {
        tokio::select! {
            incoming = listener.accept(), if connections.len() < MAXIMUM_CONCURRENT_CONTROL_REQUESTS => {
                let (stream, _) = match incoming {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(display(error)),
                };
                if !same_user_peer(&stream)? {
                    continue;
                }
                let control = Arc::clone(&control);
                let ledger = Arc::clone(&ledger);
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    let _ = handle_control_connection(stream, control, ledger, connection_shutdown).await;
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
    first: tokio::net::windows::named_pipe::NamedPipeServer,
    control: Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    ledger: Arc<ControlLedger>,
    shutdown_sender: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
    #[cfg(test)] accepted: Arc<tokio::sync::Notify>,
) -> Result<(), String> {
    let mut next = Some(first);
    let mut connections = tokio::task::JoinSet::new();
    let result = 'result: loop {
        let server = match next.take() {
            Some(server) => server,
            None => match create_current_user_pipe(&pipe_path, false) {
                Ok(server) => server,
                Err(error) => break Err(display(error)),
            },
        };
        let connected = loop {
            tokio::select! {
                connected = server.connect(), if connections.len() < MAXIMUM_CONCURRENT_CONTROL_REQUESTS => break connected,
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
        let ledger = Arc::clone(&ledger);
        let connection_shutdown = shutdown.clone();
        connections.spawn(async move {
            let _ = handle_control_connection(server, control, ledger, connection_shutdown).await;
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

#[cfg(any(test, not(target_os = "linux")))]
async fn handle_control_connection<S>(
    mut stream: S,
    control: Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    ledger: Arc<ControlLedger>,
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
    let dispatch = async {
        if request.len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
            uncorrelated_control_response("Acyclic control request exceeds the 4 MiB bound")
        } else if request.last() != Some(&b'\n') {
            uncorrelated_control_response("Acyclic control request must end with a newline")
        } else {
            request.pop();
            match serde_json::from_slice::<ControlEnvelope<ControlRequest>>(&request) {
                Ok(envelope) => dispatch_control_envelope(&control, &ledger, envelope).await,
                Err(error) => invalid_control_request_response(&request, &error),
            }
        }
    };
    tokio::pin!(dispatch);
    let mut peer_byte = [0_u8; 1];
    let response = tokio::select! {
        response = &mut dispatch => response,
        peer = stream.read(&mut peer_byte) => {
            match peer {
                Ok(0) => {
                    let _ = dispatch.await;
                    return Ok(());
                }
                Ok(_) => return Err("Acyclic control request included trailing bytes".to_owned()),
                Err(error) if matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::UnexpectedEof
                ) => {
                    let _ = dispatch.await;
                    return Ok(());
                }
                Err(error) => return Err(display(error)),
            }
        }
    };
    let write = async {
        stream.write_all(&response).await.map_err(display)?;
        stream.write_all(b"\n").await.map_err(display)?;
        stream.flush().await.map_err(display)
    };
    tokio::time::timeout(CONTROL_RESPONSE_DRAIN_GRACE, write)
        .await
        .map_err(|_| "Acyclic control response exceeded its drain deadline".to_owned())?
}

async fn dispatch_control_request(
    control: &Arc<impl ConcurrentControlRequestDispatcher>,
    request: ControlRequest,
) -> Result<Value, String> {
    if request.version != 1 {
        return Err("unsupported Acyclic control request".to_owned());
    }
    control.dispatch_request(request).await
}

async fn dispatch_control_envelope(
    control: &Arc<impl ConcurrentControlRequestDispatcher>,
    ledger: &Arc<ControlLedger>,
    envelope: ControlEnvelope<ControlRequest>,
) -> Vec<u8> {
    let request_id = envelope.request_id.clone();
    if let Err(error) = envelope.validate() {
        return control_response_for(&request_id, Err(error));
    }
    if matches!(
        envelope.request.command,
        ControlCommand::Ping | ControlCommand::Doctor | ControlCommand::Agents
    ) {
        return control_response_for(
            &request_id,
            dispatch_control_request(control, envelope.request).await,
        );
    }
    // Ledger transitions are single unflushed appends, cheaper than handing
    // them to a blocking worker.
    match ledger.begin(&envelope) {
        Err(error) => control_response_for(&request_id, Err(error)),
        Ok(LedgerDecision::Completed(response)) => response,
        Ok(LedgerDecision::Execute) => {
            let result = dispatch_control_request(control, envelope.request.clone()).await;
            // The ledger records exactly the bounded bytes that are sent.
            let response = control_response_for(&request_id, result);
            match ledger.complete(&envelope, &response) {
                Ok(()) => response,
                Err(error) => control_response_for(
                    &request_id,
                    Err(format!(
                        "Acyclic completed the operation but could not record its response: {error}"
                    )),
                ),
            }
        }
    }
}

trait ControlRequestDispatcher: Send {
    fn dispatch_request(
        &mut self,
        request: ControlRequest,
    ) -> impl std::future::Future<Output = Result<Value, String>> + Send;
}

trait ConcurrentControlRequestDispatcher: Send + Sync {
    fn dispatch_request(
        &self,
        request: ControlRequest,
    ) -> impl std::future::Future<Output = Result<Value, String>> + Send;
}

impl<T: ControlRequestDispatcher + Send> ConcurrentControlRequestDispatcher for AsyncMutex<T> {
    async fn dispatch_request(&self, request: ControlRequest) -> Result<Value, String> {
        self.lock().await.dispatch_request(request).await
    }
}

#[derive(Clone)]
struct ServiceResources {
    data: PathBuf,
    fs: LocalFs,
    store: LocalCoreStateStore,
    shared_roots: SharedRootRegistry,
    binary_identity: String,
    instance_id: String,
}

impl ServiceResources {
    async fn open(data: PathBuf) -> Result<(Self, BTreeMap<String, ControlPlane>), String> {
        fs::create_dir_all(data.join("sessions")).map_err(display)?;
        // A binary that has not been identified since it changed is hashed in
        // full; that runs alongside opening the filesystem.
        let identity_data = data.clone();
        let binary_identity = tokio::task::spawn_blocking(move || service_identity(&identity_data));
        let fs = LocalFs::local(LocalOptions::new(data.join("filesystem")))
            .await
            .map_err(display)?;
        let binary_identity = binary_identity.await.map_err(display)??;
        let resources = Self {
            store: LocalCoreStateStore::open_owned(data.join("core-state")).map_err(display)?,
            shared_roots: SharedRootRegistry::default(),
            binary_identity,
            instance_id: uuid::Uuid::new_v4().to_string(),
            data,
            fs,
        };
        let mut entries = fs::read_dir(resources.data.join("sessions"))
            .map_err(display)?
            .filter_map(|entry| match entry {
                Ok(entry) => match entry.file_type() {
                    Ok(kind) if kind.is_dir() => Some(Ok(entry.path())),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                },
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(display)?;
        entries.sort_by(|left, right| {
            state_modified(right)
                .cmp(&state_modified(left))
                .then_with(|| right.cmp(left))
        });
        let mut sessions = BTreeMap::new();
        let mut retained_sources = BTreeMap::<PathBuf, ([u8; 16], [u8; 16])>::new();
        for entry in entries {
            let (state, _) = load_state(&entry)?;
            if !state.active {
                continue;
            }
            let mut roots = Vec::with_capacity(state.roots.len());
            let mut stale = false;
            for root in state.roots.values() {
                let canonical = root.path.canonicalize().map_err(display)?;
                if retained_sources.get(&canonical).is_some_and(
                    |(source_identity, native_identity)| {
                        *native_identity == root.native_root_identity
                            && *source_identity != root.source_identity
                    },
                ) {
                    stale = true;
                    break;
                }
                roots.push((canonical, (root.source_identity, root.native_root_identity)));
            }
            if stale {
                fs::remove_dir_all(&entry).map_err(display)?;
                continue;
            }
            let control = resources.open_session_directory(entry).await?;
            if !control.state.root_session_id.is_empty() {
                retained_sources.extend(roots);
                sessions.insert(control.state.root_session_id.clone(), control);
            }
        }
        Ok((resources, sessions))
    }

    fn session_directory(&self, session_id: &str) -> PathBuf {
        self.data
            .join("sessions")
            .join(blake3::hash(session_id.as_bytes()).to_hex().as_str())
    }

    async fn open_session(&self, session_id: &str) -> Result<ControlPlane, String> {
        self.open_session_directory(self.session_directory(session_id))
            .await
    }

    async fn open_session_directory(&self, directory: PathBuf) -> Result<ControlPlane, String> {
        ControlPlane::open_with(
            directory,
            self.data.clone(),
            self.fs.clone(),
            self.store.clone(),
            self.shared_roots.clone(),
        )
        .await
    }
}

#[cfg(test)]
struct ServiceControl {
    resources: ServiceResources,
    sessions: BTreeMap<String, ControlPlane>,
}

#[cfg(test)]
impl std::ops::Deref for ServiceControl {
    type Target = ServiceResources;

    fn deref(&self) -> &Self::Target {
        &self.resources
    }
}

#[cfg(test)]
impl ServiceControl {
    async fn open(data: PathBuf) -> Result<Self, String> {
        let (resources, sessions) = ServiceResources::open(data).await?;
        Ok(Self {
            resources,
            sessions,
        })
    }

    async fn create_session(&mut self, session_id: &str) -> Result<&mut ControlPlane, String> {
        if !self.sessions.contains_key(session_id) {
            let control = self.resources.open_session(session_id).await?;
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
            for route in control
                .state
                .routes
                .values()
                .filter(|route| route.lifecycle.needs_mount())
            {
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

    async fn session_for_cwd_or_register(&mut self, cwd: &Path) -> Result<String, String> {
        if let Some(session_id) = self.session_for_cwd(cwd)? {
            return Ok(session_id);
        }
        if !self.sessions.is_empty() {
            return Err(
                "cwd is outside every registered Acyclic session; refusing service-assisted filesystem access"
                    .to_owned(),
            );
        }
        let canonical = cwd.canonicalize().map_err(display)?;
        let session_id = format!(
            "cwd:{}",
            blake3::hash(canonical.as_os_str().to_string_lossy().as_bytes()).to_hex()
        );
        self.create_session(&session_id)
            .await?
            .session_start(json!({
                "session_id": session_id,
                "cwd": canonical,
                "host": "cli"
            }))
            .await?;
        Ok(session_id)
    }

    fn child_mount_for_cwd(&self, cwd: &Path) -> Result<Option<(String, String)>, String> {
        let cwd = cwd.canonicalize().map_err(display)?;
        let mut matches = Vec::new();
        for (session_id, control) in &self.sessions {
            for route in control
                .state
                .routes
                .values()
                .filter(|route| route.lifecycle.needs_mount())
            {
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
            for pending in control
                .state
                .pending
                .iter()
                .filter(|pending| pending.lifecycle == PendingSpawnLifecycle::Prepared)
            {
                if let Ok(mount_path) = pending.mount_path.canonicalize()
                    && cwd.starts_with(&mount_path)
                {
                    matches.push((
                        mount_path.components().count(),
                        session_id.clone(),
                        format!("pending:{}", pending.mount_name()),
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

    async fn shutdown_sessions(&mut self, deactivate: bool) -> Result<(), String> {
        let sessions = self.sessions.keys().cloned().collect::<Vec<_>>();
        let mut failed_sessions = 0_usize;
        let mut first_error = None;
        for session_id in sessions {
            let error = self.close_session(&session_id, deactivate).await.err();
            let session_failed = error.is_some();
            if let Some(error) = error {
                first_error.get_or_insert_with(|| format!("session {session_id}: {error}"));
            }
            failed_sessions += usize::from(session_failed);
        }
        first_error.map_or(Ok(()), |error| {
            Err(format!(
                "Acyclic service shutdown failed for {failed_sessions} session(s); first error: {error}"
            ))
        })
    }

    async fn close_session(&mut self, session_id: &str, deactivate: bool) -> Result<(), String> {
        let control = self
            .sessions
            .remove(session_id)
            .ok_or_else(|| "Acyclic native hook session is not registered".to_owned())?;
        let mut terminal_state = control.state.clone();
        let directory = control.data.clone();
        let mut slots = match control.close(deactivate).await {
            Ok(slots) => slots,
            Err(shutdown_error) => {
                let recovered = ControlPlane::open_with(
                    directory,
                    self.data.clone(),
                    self.fs.clone(),
                    self.store.clone(),
                    self.shared_roots.clone(),
                )
                .await;
                return match recovered {
                    Ok(control) => {
                        self.sessions.insert(session_id.to_owned(), control);
                        Err(format!(
                            "Acyclic session shutdown failed and was restored: {shutdown_error}"
                        ))
                    }
                    Err(recovery_error) => Err(format!(
                        "Acyclic session shutdown failed: {shutdown_error}; live recovery failed: {recovery_error}"
                    )),
                };
            }
        };
        if deactivate {
            terminal_state.active = false;
            slots.save(&directory, &terminal_state, Survives::PowerLoss)?;
        }
        Ok(())
    }

    #[cfg(test)]
    async fn shutdown(mut self) -> Result<(), String> {
        let root_released = self.fs.local_root_release_barrier();
        let result = self.shutdown_sessions(false).await;
        // Publish service shutdown only after its final LocalFs handle has released the durable
        // Stream and Objects roots. A completed async future may otherwise retain `self` until the
        // executor drops the future, allowing an immediate replacement service to race the lock.
        drop(self);
        wait_for_root_release(root_released).await?;
        result
    }

    async fn dispatch_native_hook(
        &mut self,
        host: &str,
        event: &str,
        input: Value,
        cwd: &Path,
    ) -> Result<Value, String> {
        let session_id = hook_id(&input, "session_id", "sessionId")?;
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
            self.close_session(&session_id, true).await?;
            return Ok(json!({}));
        }
        let control = self
            .sessions
            .get_mut(&session_id)
            .ok_or_else(|| "Acyclic native hook session is not registered".to_owned())?;
        dispatch_native_session_hook(control, host, event, input, cwd).await
    }
}

fn native_tool_identity(
    control: &mut ControlPlane,
    host: &str,
    input: &Value,
    cwd: &Path,
) -> Result<(String, String, WorkspaceRootId), String> {
    if let Some(agent_id) = hook_optional_id(input, "agent_id", "agentId")? {
        let route = control
            .state
            .routes
            .get(&agent_id)
            .ok_or_else(|| format!("{host} tool reports an unknown subagent identity"))?
            .clone();
        if !route.lifecycle.accepts_tools() {
            return Err("subagent workspace is sealed after SubagentStop".to_owned());
        }
        let (cwd_caller, _, root_id) = control.route_root_from_cwd(cwd)?;
        let owns_reported_root = cwd_caller == agent_id
            || (cwd_caller == control.state.root_agent_id
                && route.roots.contains_key(&root_key(root_id)));
        if !owns_reported_root {
            return Err(format!(
                "{host} tool identity does not own its reported workspace cwd"
            ));
        }
        let turn_id = if host == "codex" {
            hook_id(input, "turn_id", "turnId")?
        } else {
            hook_optional_id(input, "turn_id", "turnId")?.unwrap_or_else(|| route.turn_id.clone())
        };
        if control.state.root_turns.contains(&turn_id)
            || control
                .state
                .turns
                .get(&turn_id)
                .is_some_and(|bound| bound != &agent_id)
        {
            return Err(format!(
                "{host} tool identity conflicts with its turn binding"
            ));
        }
        if control.state.turns.get(&turn_id) != Some(&agent_id) {
            control
                .state
                .turns
                .insert(turn_id.clone(), agent_id.clone());
            control.persist()?;
        }
        return Ok((agent_id, turn_id, root_id));
    }
    if host == "codex" {
        let turn_id = hook_id(input, "turn_id", "turnId")?;
        let caller = control.resolve_turn(&turn_id)?;
        let root_id = if caller == control.state.root_agent_id {
            let (cwd_caller, _, root_id) = control.route_root_from_cwd(cwd)?;
            if cwd_caller != caller {
                return Err("root tool cwd resolves to a child workspace".to_owned());
            }
            root_id
        } else {
            WorkspaceRootId::from_bytes(
                control
                    .state
                    .routes
                    .get(&caller)
                    .ok_or_else(|| "subagent route is missing".to_owned())?
                    .root_id,
            )
        };
        return Ok((caller, turn_id, root_id));
    }
    let (caller, route, root_id) = control.route_root_from_cwd(cwd)?;
    let turn_id = route.as_ref().map_or_else(
        || format!("{host}:root:{}", control.state.root_session_id),
        |route| route.turn_id.clone(),
    );
    if caller == control.state.root_agent_id {
        control.remember_root_turn(turn_id.clone());
    }
    Ok((caller, turn_id, root_id))
}

async fn dispatch_native_session_hook(
    control: &mut ControlPlane,
    host: &str,
    event: &str,
    input: Value,
    cwd: &Path,
) -> Result<Value, String> {
    let session_id = hook_id(&input, "session_id", "sessionId")?;
    let hook_cwd = hook_path(&input, "cwd").unwrap_or_else(|| cwd.to_path_buf());
    match event {
        "UserPromptSubmit" | "userPromptSubmitted" => {
            let (caller, _, _) = control.route_root_from_cwd(&hook_cwd)?;
            if caller != control.state.root_agent_id {
                return Err("root prompt hook originated inside a child mount".to_owned());
            }
            let turn_id = if host == "codex" {
                hook_optional_id(&input, "turn_id", "turnId")?
                    .ok_or_else(|| "Codex prompt hook lacks a stable turn identity".to_owned())?
            } else {
                hook_optional_id(&input, "turn_id", "turnId")?
                    .or(hook_optional_id(&input, "prompt_id", "promptId")?)
                    .unwrap_or_else(|| format!("{host}:root:{session_id}"))
            };
            control.user_prompt(json!({
                "session_id": session_id,
                "turn_id": turn_id
            }))
        }
        "PreToolUse" | "preToolUse" => {
            let (_, turn_id, root_id) = native_tool_identity(control, host, &input, &hook_cwd)?;
            let mut tool_name = hook_string(&input, "tool_name", "toolName")?;
            if host == "copilot" && tool_name == "task" {
                tool_name = "Agent".to_owned();
            }
            let tool_input = input
                .get("tool_input")
                .or_else(|| input.get("toolArgs"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            let tool_use_id = native_hook_tool_id(&input, &session_id, &tool_name, &tool_input)?;
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
        "PostToolUse" | "postToolUse" | "PostToolUseFailure" | "postToolUseFailure" => {
            let (_, turn_id, _) = native_tool_identity(control, host, &input, &hook_cwd)?;
            let mut tool_name = hook_string(&input, "tool_name", "toolName")?;
            if host == "copilot" && tool_name == "task" {
                tool_name = "Agent".to_owned();
            }
            let tool_input = input
                .get("tool_input")
                .or_else(|| input.get("toolArgs"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            let tool_use_id = native_hook_tool_id(&input, &session_id, &tool_name, &tool_input)?;
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
            let agent_id = hook_optional_id(&input, "agent_id", "agentId")?
                .or(hook_optional_id(&input, "agent_name", "agentName")?)
                .ok_or_else(|| "subagent start lacks a stable agent identity".to_owned())?;
            let turn_id = if host == "codex" {
                hook_id(&input, "turn_id", "turnId")?
            } else {
                hook_optional_id(&input, "turn_id", "turnId")?
                    .unwrap_or_else(|| format!("{host}:agent:{agent_id}"))
            };
            let parent = control
                .state
                .routes
                .get(&agent_id)
                .map(|route| route.parent_agent_id.clone())
                .or_else(|| {
                    control
                        .state
                        .pending
                        .front()
                        .map(|pending| pending.parent_agent_id.clone())
                })
                .ok_or_else(|| {
                    "subagent start has no serialized spawn or resume identity".to_owned()
                })?;
            control
                .subagent_start(json!({
                    "session_id": session_id,
                    "turn_id": turn_id,
                    "agent_id": agent_id,
                    "_authenticated_parent_agent_id": parent,
                    "agent_type": hook_optional_string(&input, "agent_type", "agentType").unwrap_or_else(|| "subagent".to_owned())
                }))
                .await
        }
        "SubagentStop" | "subagentStop" => {
            let agent_id = hook_optional_id(&input, "agent_id", "agentId")?
                .or(hook_optional_id(&input, "agent_name", "agentName")?)
                .ok_or_else(|| "subagent stop lacks a stable agent identity".to_owned())?;
            let turn_id = if host == "codex" {
                hook_id(&input, "turn_id", "turnId")?
            } else {
                hook_optional_id(&input, "turn_id", "turnId")?
                    .unwrap_or_else(|| format!("{host}:agent:{agent_id}"))
            };
            control
                .subagent_stop(json!({
                    "session_id": session_id,
                    "turn_id": turn_id,
                    "agent_id": agent_id
                }))
                .await
        }
        "Stop" | "agentStop" => Ok(json!({})),
        "SessionStart" | "sessionStart" | "SessionEnd" | "sessionEnd" => {
            Err("session boundary hook reached a workspace actor".to_owned())
        }
        _ => Err(format!("unsupported {host} lifecycle event '{event}'")),
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

fn hook_optional_id(input: &Value, snake: &str, camel: &str) -> Result<Option<String>, String> {
    hook_optional_string(input, snake, camel)
        .map(|value| validate_hook_id(snake, value))
        .transpose()
}

fn hook_id(input: &Value, snake: &str, camel: &str) -> Result<String, String> {
    hook_optional_id(input, snake, camel)?
        .ok_or_else(|| format!("native hook is missing '{snake}'"))
}

fn validate_hook_id(field: &str, value: String) -> Result<String, String> {
    const MAXIMUM_HOOK_ID_BYTES: usize = 512;
    if value.is_empty()
        || value.len() > MAXIMUM_HOOK_ID_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(format!(
            "native hook '{field}' must be 1-{MAXIMUM_HOOK_ID_BYTES} non-control bytes"
        ));
    }
    Ok(value)
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
    if let Some(id) = hook_optional_id(input, "tool_use_id", "toolUseId")? {
        return Ok(id);
    }
    if is_filesystem_tool(tool_name) {
        return Err(format!(
            "filesystem tool '{tool_name}' is missing a stable tool-use identity"
        ));
    }
    Ok(native_tool_id(session_id, tool_name, tool_input))
}

fn native_hook_is_process_local_noop(host: &str, event: &str, input: &Value) -> bool {
    matches!(host, "codex" | "claude-code" | "copilot" | "cursor")
        && matches!(
            event,
            "PreToolUse"
                | "preToolUse"
                | "PostToolUse"
                | "postToolUse"
                | "PostToolUseFailure"
                | "postToolUseFailure"
        )
        && hook_optional_string(input, "tool_name", "toolName")
            .is_some_and(|tool| is_known_non_filesystem_tool(&tool))
}

#[cfg(test)]
impl ControlRequestDispatcher for ServiceControl {
    async fn dispatch_request(&mut self, request: ControlRequest) -> Result<Value, String> {
        if matches!(request.command, ControlCommand::Ping) {
            return Ok(json!({
                "identity": self.binary_identity,
                "instanceId": self.instance_id,
                "sessions": self.sessions.len(),
            }));
        }
        if matches!(request.command, ControlCommand::Doctor) {
            parse_read_only_arguments(&request.argv)?;
            let executable = executable_digests_async().await?;
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
                &self.binary_identity,
                &executable.sha256,
                &executable.blake3,
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
        dispatch_session_request(control, request).await
    }
}

const SESSION_ACTIVE: u8 = 0;
const SESSION_CLOSING: u8 = 1;
const SESSION_CLOSED: u8 = 2;
const SESSION_CLOSE_FAILED: u8 = 3;
const SESSION_DATA_CAPACITY: usize = 64;
const SESSION_QUEUE_CAPACITY: usize = SESSION_DATA_CAPACITY + 1;
const SERVICE_RUNNING: u8 = 0;
const SERVICE_DRAINING: u8 = 1;
const SERVICE_CLOSED: u8 = 2;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SessionKey {
    session_id: String,
    epoch: u64,
}

struct SessionHandle {
    key: SessionKey,
    state: Arc<std::sync::atomic::AtomicU8>,
    admission: Mutex<()>,
    sender: tokio::sync::mpsc::Sender<SessionCommand>,
    data_slots: Arc<tokio::sync::Semaphore>,
    snapshot: Arc<std::sync::RwLock<Option<AdapterState>>>,
    close_result: watch::Sender<Option<Result<(), String>>>,
}

enum SessionCommand {
    Dispatch {
        request: ControlRequest,
        response: tokio::sync::oneshot::Sender<Result<Value, String>>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    },
    Shutdown {
        deactivate: bool,
        response: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    #[cfg(test)]
    Pause {
        entered: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    },
}

impl SessionHandle {
    fn new(key: SessionKey, control: ControlPlane, resources: ServiceResources) -> Self {
        let snapshot = Arc::new(std::sync::RwLock::new(Some(control.state.clone())));
        let actor_snapshot = Arc::clone(&snapshot);
        let state = Arc::new(std::sync::atomic::AtomicU8::new(SESSION_ACTIVE));
        let actor_state = Arc::clone(&state);
        let close_result = watch::channel(None).0;
        let actor_close_result = close_result.clone();
        let data_slots = Arc::new(tokio::sync::Semaphore::new(SESSION_DATA_CAPACITY));
        let (sender, mut receiver) = tokio::sync::mpsc::channel(SESSION_QUEUE_CAPACITY);
        tokio::spawn(async move {
            let mut control = Some(control);
            while let Some(command) = receiver.recv().await {
                match command {
                    SessionCommand::Dispatch {
                        request,
                        response,
                        _permit,
                    } => {
                        let result = match control.as_mut() {
                            Some(control) => dispatch_session_request(control, request).await,
                            None => Err("Acyclic session actor has no workspace".to_owned()),
                        };
                        // Other sessions see only durable state: an unflushed
                        // save is published once flushed, after the reply.
                        let mut pending = Some((response, result));
                        if control.as_ref().is_some_and(|control| control.unflushed)
                            && let Some((response, result)) = pending.take()
                        {
                            let _ = response.send(result);
                        }
                        let durable = match control.as_mut() {
                            Some(control) => control.make_durable().await.is_ok(),
                            None => true,
                        };
                        if durable {
                            let next = control.as_ref().map(|control| control.state.clone());
                            if let Ok(mut snapshot) = actor_snapshot.write() {
                                *snapshot = next;
                            }
                        }
                        if let Some((response, result)) = pending {
                            let _ = response.send(result);
                        }
                    }
                    SessionCommand::Shutdown {
                        deactivate,
                        response,
                    } => {
                        let current = control
                            .take()
                            .ok_or_else(|| "Acyclic session actor has no workspace".to_owned());
                        let result = match current {
                            Err(error) => Err(error),
                            Ok(current) => {
                                let mut terminal_state = current.state.clone();
                                let directory = current.data.clone();
                                match current.close(deactivate).await {
                                    Ok(mut slots) => {
                                        if deactivate {
                                            terminal_state.active = false;
                                            slots.save(
                                                &directory,
                                                &terminal_state,
                                                Survives::PowerLoss,
                                            )
                                        } else {
                                            Ok(())
                                        }
                                    }
                                    Err(shutdown_error) => {
                                        match ControlPlane::open_with(
                                            directory,
                                            resources.data.clone(),
                                            resources.fs.clone(),
                                            resources.store.clone(),
                                            resources.shared_roots.clone(),
                                        )
                                        .await
                                        {
                                            Ok(recovered) => {
                                                control = Some(recovered);
                                                Err(format!(
                                                    "Acyclic session shutdown failed and was restored: {shutdown_error}"
                                                ))
                                            }
                                            Err(recovery_error) => Err(format!(
                                                "Acyclic session shutdown failed: {shutdown_error}; live recovery failed: {recovery_error}"
                                            )),
                                        }
                                    }
                                }
                            }
                        };
                        let _ = actor_close_result.send(Some(result.clone()));
                        if result.is_ok() {
                            actor_state.store(SESSION_CLOSED, std::sync::atomic::Ordering::Release);
                            if let Ok(mut snapshot) = actor_snapshot.write() {
                                *snapshot = None;
                            }
                            let _ = response.send(result);
                            break;
                        }
                        actor_state
                            .store(SESSION_CLOSE_FAILED, std::sync::atomic::Ordering::Release);
                        let next = control.as_ref().map(|control| control.state.clone());
                        if let Ok(mut snapshot) = actor_snapshot.write() {
                            *snapshot = next;
                        }
                        let _ = response.send(result);
                    }
                    #[cfg(test)]
                    SessionCommand::Pause { entered, resume } => {
                        let _ = entered.send(());
                        let _ = resume.await;
                    }
                }
            }
        });
        Self {
            key,
            state,
            admission: Mutex::new(()),
            sender,
            data_slots,
            snapshot,
            close_result,
        }
    }

    fn snapshot(&self) -> Result<Option<AdapterState>, String> {
        self.snapshot
            .read()
            .map(|snapshot| snapshot.clone())
            .map_err(|_| "session routing snapshot lock is poisoned".to_owned())
    }

    fn is_active(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::Acquire) == SESSION_ACTIVE
    }

    async fn dispatch(&self, request: ControlRequest) -> Result<Value, String> {
        let (response, result) = tokio::sync::oneshot::channel();
        {
            let _admission = self
                .admission
                .lock()
                .map_err(|_| "session admission lock is poisoned".to_owned())?;
            if !self.is_active() {
                return Err("Acyclic session is closing".to_owned());
            }
            let permit = Arc::clone(&self.data_slots)
                .try_acquire_owned()
                .map_err(|_| "Acyclic session operation queue is full".to_owned())?;
            self.sender
                .try_send(SessionCommand::Dispatch {
                    request,
                    response,
                    _permit: permit,
                })
                .map_err(|error| match error {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        "Acyclic session operation queue is full".to_owned()
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        "Acyclic session actor stopped".to_owned()
                    }
                })?;
        }
        result
            .await
            .map_err(|_| "Acyclic session actor dropped an accepted operation".to_owned())?
    }

    async fn shutdown(&self, deactivate: bool) -> Result<(), String> {
        let (response, result) = tokio::sync::oneshot::channel();
        let mut existing = None;
        {
            let _admission = self
                .admission
                .lock()
                .map_err(|_| "session admission lock is poisoned".to_owned())?;
            match self.state.load(std::sync::atomic::Ordering::Acquire) {
                SESSION_ACTIVE => self
                    .state
                    .store(SESSION_CLOSING, std::sync::atomic::Ordering::Release),
                SESSION_CLOSING => existing = Some(self.close_result.subscribe()),
                SESSION_CLOSE_FAILED => {
                    self.state
                        .store(SESSION_CLOSING, std::sync::atomic::Ordering::Release);
                    let _ = self.close_result.send(None);
                }
                SESSION_CLOSED => return Ok(()),
                _ => return Err("invalid Acyclic session lifecycle state".to_owned()),
            }
            if existing.is_none()
                && let Err(error) = self.sender.try_send(SessionCommand::Shutdown {
                    deactivate,
                    response,
                })
            {
                let error = match error {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        "Acyclic session control queue is unexpectedly full during shutdown"
                            .to_owned()
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        "Acyclic session actor stopped before shutdown".to_owned()
                    }
                };
                self.state
                    .store(SESSION_CLOSE_FAILED, std::sync::atomic::Ordering::Release);
                let _ = self.close_result.send(Some(Err(error.clone())));
                return Err(error);
            }
        }
        if let Some(mut existing) = existing {
            if existing.borrow().is_none() {
                existing.changed().await.map_err(display)?;
            }
            return existing
                .borrow()
                .clone()
                .ok_or_else(|| "Acyclic session close completed without a result".to_owned())?;
        }
        result
            .await
            .map_err(|_| "Acyclic session actor dropped shutdown".to_owned())?
    }

    #[cfg(test)]
    #[allow(
        clippy::expect_used,
        reason = "this deterministic test gate must panic immediately if its actor fixture is broken"
    )]
    async fn pause(&self) -> tokio::sync::oneshot::Sender<()> {
        let (entered, accepted) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        self.sender
            .try_send(SessionCommand::Pause {
                entered,
                resume: paused,
            })
            .expect("session actor");
        accepted.await.expect("actor pause accepted");
        resume
    }
}

struct OpeningSession {
    key: SessionKey,
    cancelled: bool,
    complete: watch::Sender<bool>,
}

#[derive(Default)]
struct SessionCatalog {
    next_epoch: u64,
    opening: BTreeMap<String, OpeningSession>,
    active: BTreeMap<String, Arc<SessionHandle>>,
    draining: BTreeMap<SessionKey, Arc<SessionHandle>>,
    completed: BTreeSet<String>,
}

struct ConcurrentServiceControl {
    resources: ServiceResources,
    lifecycle: std::sync::atomic::AtomicU8,
    shutdown_deactivates: std::sync::atomic::AtomicBool,
    catalog: AsyncMutex<SessionCatalog>,
}

impl std::ops::Deref for ConcurrentServiceControl {
    type Target = ServiceResources;

    fn deref(&self) -> &Self::Target {
        &self.resources
    }
}

impl ConcurrentServiceControl {
    async fn open(data: PathBuf) -> Result<Self, String> {
        let (resources, sessions) = ServiceResources::open(data).await?;
        Ok(Self::from_parts(resources, sessions))
    }

    fn from_parts(resources: ServiceResources, sessions: BTreeMap<String, ControlPlane>) -> Self {
        let mut catalog = SessionCatalog::default();
        for (session_id, control) in sessions {
            catalog.next_epoch += 1;
            let key = SessionKey {
                session_id: session_id.clone(),
                epoch: catalog.next_epoch,
            };
            catalog.active.insert(
                session_id,
                Arc::new(SessionHandle::new(key, control, resources.clone())),
            );
        }
        Self {
            resources,
            lifecycle: std::sync::atomic::AtomicU8::new(SERVICE_RUNNING),
            shutdown_deactivates: std::sync::atomic::AtomicBool::new(false),
            catalog: AsyncMutex::new(catalog),
        }
    }

    fn ensure_running(&self) -> Result<(), String> {
        if self.lifecycle.load(std::sync::atomic::Ordering::Acquire) == SERVICE_RUNNING {
            Ok(())
        } else {
            Err("Acyclic service is draining".to_owned())
        }
    }

    async fn session(&self, session_id: &str) -> Result<Arc<SessionHandle>, String> {
        self.catalog
            .lock()
            .await
            .active
            .get(session_id)
            .cloned()
            .ok_or_else(|| "Acyclic native hook session is not registered".to_owned())
    }

    async fn start_session(
        &self,
        session_id: &str,
        host: &str,
        root: PathBuf,
    ) -> Result<(Arc<SessionHandle>, Value), String> {
        let root = root.canonicalize().map_err(display)?;
        if let Some(owner) = self.route_session(&root).await?
            && owner.key.session_id != session_id
        {
            return Err(format!(
                "session {session_id} cannot attach a workspace owned by session {} as a physical root",
                owner.key.session_id
            ));
        }
        loop {
            self.ensure_running()?;
            let (key, wait) = {
                let mut catalog = self.catalog.lock().await;
                if let Some(handle) = catalog.active.get(session_id) {
                    let handle = Arc::clone(handle);
                    drop(catalog);
                    let output = handle
                        .dispatch(ControlRequest {
                            version: 1,
                            command: ControlCommand::Hook,
                            cwd: root.clone(),
                            argv: Vec::new(),
                            name: format!("{host}:SessionStart"),
                            arguments: json!({
                                "session_id": session_id,
                                "cwd": root,
                            }),
                        })
                        .await?;
                    return Ok((handle, output));
                }
                if let Some(opening) = catalog.opening.get(session_id) {
                    (opening.key.clone(), Some(opening.complete.subscribe()))
                } else {
                    catalog.completed.remove(session_id);
                    catalog.next_epoch += 1;
                    let key = SessionKey {
                        session_id: session_id.to_owned(),
                        epoch: catalog.next_epoch,
                    };
                    catalog.opening.insert(
                        session_id.to_owned(),
                        OpeningSession {
                            key: key.clone(),
                            cancelled: false,
                            complete: watch::channel(false).0,
                        },
                    );
                    (key, None)
                }
            };
            if let Some(mut wait) = wait {
                if !*wait.borrow() {
                    wait.changed().await.map_err(display)?;
                }
                continue;
            }

            let mut control = self.resources.open_session(session_id).await?;
            let opened = control
                .session_start(json!({"session_id": session_id, "cwd": root, "host": host}))
                .await;
            let handle = Some(Arc::new(SessionHandle::new(
                key.clone(),
                control,
                self.resources.clone(),
            )));
            let (cancelled, complete) = {
                let mut catalog = self.catalog.lock().await;
                let opening = catalog
                    .opening
                    .remove(session_id)
                    .ok_or_else(|| "Acyclic session opening reservation disappeared".to_owned())?;
                if opening.key != key {
                    return Err("Acyclic session opening epoch changed".to_owned());
                }
                let cancelled = opened.is_err()
                    || opening.cancelled
                    || self.lifecycle.load(std::sync::atomic::Ordering::Acquire) != SERVICE_RUNNING;
                if let Some(handle) = &handle {
                    if cancelled {
                        catalog
                            .draining
                            .insert(handle.key.clone(), Arc::clone(handle));
                    } else {
                        catalog
                            .active
                            .insert(session_id.to_owned(), Arc::clone(handle));
                    }
                }
                (cancelled, opening.complete)
            };
            let output = match opened {
                Ok(output) => output,
                Err(error) => {
                    let cleanup = if let Some(handle) = &handle {
                        let cleanup = handle.shutdown(true).await;
                        if cleanup.is_ok() {
                            let mut catalog = self.catalog.lock().await;
                            catalog.draining.remove(&handle.key);
                            catalog.completed.insert(session_id.to_owned());
                        }
                        cleanup
                    } else {
                        Ok(())
                    };
                    let _ = complete.send(true);
                    return match cleanup {
                        Ok(()) => Err(error),
                        Err(cleanup) => Err(format!(
                            "{error}; failed session-start cleanup was retained: {cleanup}"
                        )),
                    };
                }
            };
            let handle = handle.ok_or_else(|| "Acyclic session did not open".to_owned())?;
            if cancelled {
                let cleanup = handle.shutdown(true).await;
                if cleanup.is_ok() {
                    let mut catalog = self.catalog.lock().await;
                    catalog.draining.remove(&handle.key);
                    catalog.completed.insert(session_id.to_owned());
                }
                let _ = complete.send(true);
                cleanup?;
                return Err("Acyclic session start was cancelled".to_owned());
            }
            let _ = complete.send(true);
            return Ok((handle, output));
        }
    }

    async fn end_session(&self, session_id: &str, deactivate: bool) -> Result<(), String> {
        let mut cancelled_opening = false;
        loop {
            let wait = {
                let mut catalog = self.catalog.lock().await;
                if let Some(opening) = catalog.opening.get_mut(session_id) {
                    opening.cancelled = true;
                    cancelled_opening = true;
                    Some(opening.complete.subscribe())
                } else {
                    None
                }
            };
            if let Some(mut wait) = wait {
                if !*wait.borrow() {
                    wait.changed().await.map_err(display)?;
                }
                continue;
            }
            break;
        }
        let handle = {
            let mut catalog = self.catalog.lock().await;
            if let Some(handle) = catalog.active.remove(session_id) {
                catalog
                    .draining
                    .insert(handle.key.clone(), Arc::clone(&handle));
                handle
            } else if let Some(handle) = catalog
                .draining
                .values()
                .find(|handle| handle.key.session_id == session_id)
                .cloned()
            {
                handle
            } else {
                if cancelled_opening {
                    return Ok(());
                }
                if catalog.completed.contains(session_id) {
                    return Ok(());
                }
                return Err("Acyclic native hook session is not registered".to_owned());
            }
        };
        let result = handle.shutdown(deactivate).await;
        {
            let mut catalog = self.catalog.lock().await;
            if result.is_ok() {
                catalog.draining.remove(&handle.key);
                catalog.completed.insert(session_id.to_owned());
            }
        }
        if result.is_ok() {
            self.shared_roots.prune().await;
        }
        result
    }

    async fn route_session(&self, cwd: &Path) -> Result<Option<Arc<SessionHandle>>, String> {
        let cwd = cwd.canonicalize().map_err(display)?;
        let handles = self.catalog.lock().await;
        let handles = handles
            .active
            .values()
            .chain(handles.draining.values())
            .cloned()
            .collect::<Vec<_>>();
        let mut matches = Vec::new();
        for handle in handles {
            let Some(snapshot) = handle.snapshot()? else {
                continue;
            };
            let mut score = snapshot
                .roots
                .values()
                .filter_map(|root| root.path.canonicalize().ok())
                .filter(|root| cwd.starts_with(root))
                .map(|root| root.components().count())
                .max();
            for route in snapshot
                .routes
                .values()
                .filter(|route| route.lifecycle.needs_mount())
            {
                if let Ok(mount) = route.mount_path.canonicalize()
                    && cwd.starts_with(&mount)
                {
                    score = Some(score.unwrap_or(0).max(mount.components().count()));
                }
            }
            if let Some(score) = score {
                matches.push((score, handle));
            }
        }
        matches.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
        let Some((score, handle)) = matches.first() else {
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
        Ok(Some(Arc::clone(handle)))
    }

    async fn drain_sessions(&self, deactivate: bool) -> Result<(), String> {
        self.lifecycle
            .store(SERVICE_DRAINING, std::sync::atomic::Ordering::Release);
        loop {
            let waits = {
                let mut catalog = self.catalog.lock().await;
                if catalog.opening.is_empty() {
                    break;
                }
                catalog
                    .opening
                    .values_mut()
                    .map(|opening| {
                        opening.cancelled = true;
                        opening.complete.subscribe()
                    })
                    .collect::<Vec<_>>()
            };
            for mut wait in waits {
                if !*wait.borrow() {
                    wait.changed().await.map_err(display)?;
                }
            }
        }
        let handles = {
            let mut catalog = self.catalog.lock().await;
            let active = std::mem::take(&mut catalog.active);
            for handle in active.values() {
                catalog
                    .draining
                    .insert(handle.key.clone(), Arc::clone(handle));
            }
            catalog.draining.values().cloned().collect::<Vec<_>>()
        };
        let mut failed_sessions = 0_usize;
        let mut first_error = None;
        for handle in handles {
            if let Err(error) = handle.shutdown(deactivate).await {
                failed_sessions += 1;
                first_error
                    .get_or_insert_with(|| format!("session {}: {error}", handle.key.session_id));
            } else {
                self.catalog.lock().await.draining.remove(&handle.key);
            }
        }
        if let Some(error) = first_error {
            Err(format!(
                "Acyclic service shutdown failed for {failed_sessions} session(s); first error: {error}"
            ))
        } else {
            self.lifecycle
                .store(SERVICE_CLOSED, std::sync::atomic::Ordering::Release);
            Ok(())
        }
    }

    /// Stops admitting requests ahead of a drain that ends every session, as a
    /// stop request asks; see [`ServiceMarker`].
    fn begin_drain(&self) {
        self.shutdown_deactivates
            .store(true, std::sync::atomic::Ordering::Release);
        self.lifecycle
            .store(SERVICE_DRAINING, std::sync::atomic::Ordering::Release);
    }

    async fn shutdown(self) -> Result<(), String> {
        let root_released = self.fs.local_root_release_barrier();
        let deactivate = self
            .shutdown_deactivates
            .load(std::sync::atomic::Ordering::Acquire);
        let result = self.drain_sessions(deactivate).await;
        drop(self);
        wait_for_root_release(root_released).await?;
        result
    }
}

impl ConcurrentControlRequestDispatcher for ConcurrentServiceControl {
    async fn dispatch_request(&self, request: ControlRequest) -> Result<Value, String> {
        if request.command == ControlCommand::Ping {
            let catalog = self.catalog.lock().await;
            let active_sessions = catalog.active.len();
            let opening_sessions = catalog.opening.len();
            let draining_sessions = catalog.draining.len();
            return Ok(json!({
                "identity": self.binary_identity,
                "instanceId": self.instance_id,
                "sessions": active_sessions + opening_sessions + draining_sessions,
                "activeSessions": active_sessions,
                "openingSessions": opening_sessions,
                "drainingSessions": draining_sessions,
            }));
        }
        self.ensure_running()?;
        if request.command == ControlCommand::Doctor {
            parse_read_only_arguments(&request.argv)?;
            let executable = executable_digests_async().await?;
            let (session_claims, handles) = {
                let catalog = self.catalog.lock().await;
                (
                    catalog.active.len() + catalog.opening.len() + catalog.draining.len(),
                    catalog
                        .active
                        .values()
                        .chain(catalog.draining.values())
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            };
            let mut routes = 0;
            let mut leases = 0;
            let mut pending_recovery = false;
            for handle in &handles {
                if let Some(state) = handle.snapshot()? {
                    routes += state.routes.len();
                    leases += state.leases.len();
                    pending_recovery |=
                        !state.pending_discards.is_empty() || !state.pending.is_empty();
                }
            }
            return doctor_report(
                &self.data,
                &self.binary_identity,
                &executable.sha256,
                &executable.blake3,
                session_claims,
                routes,
                leases,
                pending_recovery,
            );
        }
        if request.command == ControlCommand::Hook {
            let (host, event) = request
                .name
                .split_once(':')
                .ok_or_else(|| "native hook request has no host/event pair".to_owned())?;
            let session_id = hook_id(&request.arguments, "session_id", "sessionId")?;
            if matches!(event, "SessionStart" | "sessionStart") {
                let root =
                    hook_root_path(&request.arguments).unwrap_or_else(|| request.cwd.clone());
                let (_, output) = self.start_session(&session_id, host, root).await?;
                return Ok(output);
            }
            if matches!(event, "SessionEnd" | "sessionEnd") {
                self.end_session(&session_id, true).await?;
                return Ok(json!({}));
            }
            return self.session(&session_id).await?.dispatch(request).await;
        }
        let handle = if let Some(handle) = self.route_session(&request.cwd).await? {
            handle
        } else {
            let catalog = self.catalog.lock().await;
            if !catalog.active.is_empty()
                || !catalog.opening.is_empty()
                || !catalog.draining.is_empty()
            {
                return Err("cwd is outside every registered Acyclic session; refusing service-assisted filesystem access".to_owned());
            }
            drop(catalog);
            let canonical = request.cwd.canonicalize().map_err(display)?;
            let session_id = format!(
                "cwd:{}",
                blake3::hash(canonical.as_os_str().to_string_lossy().as_bytes()).to_hex()
            );
            self.start_session(&session_id, "cli", canonical).await?.0
        };
        handle.dispatch(request).await
    }
}

impl ControlRequestDispatcher for ControlPlane {
    async fn dispatch_request(&mut self, request: ControlRequest) -> Result<Value, String> {
        dispatch_session_request(self, request).await
    }
}

async fn dispatch_session_request(
    control: &mut ControlPlane,
    request: ControlRequest,
) -> Result<Value, String> {
    control.make_durable().await?;
    if request.command == ControlCommand::Hook {
        let (host, event) = request
            .name
            .split_once(':')
            .ok_or_else(|| "native hook request has no host/event pair".to_owned())?;
        return dispatch_native_session_hook(control, host, event, request.arguments, &request.cwd)
            .await;
    }
    let selected_cwd = selected_cli_cwd(&request)?;
    let (invocation_caller, _) = control.route_from_cwd(&request.cwd)?;
    let (selected_caller, _) = control.route_from_cwd(&selected_cwd)?;
    if selected_caller != invocation_caller {
        return Err("acyclic -C cannot cross workspace-context authority boundaries".to_owned());
    }
    let mut request = request;
    request.cwd = selected_cwd;
    request.arguments = Value::Null;
    dispatch_plane_request(control, request).await
}

async fn dispatch_plane_request(
    control: &mut ControlPlane,
    request: ControlRequest,
) -> Result<Value, String> {
    match request.command {
        ControlCommand::Ping => Ok(json!({})),
        ControlCommand::Doctor => {
            parse_read_only_arguments(&request.argv)?;
            let executable = executable_digests_async().await?;
            doctor_report(
                &control.config_root,
                &executable.service_identity,
                &executable.sha256,
                &executable.blake3,
                1,
                control.state.routes.len(),
                control.state.leases.len(),
                !control.state.pending_discards.is_empty() || !control.state.pending.is_empty(),
            )
        }
        ControlCommand::Agents => {
            parse_read_only_arguments(&request.argv)?;
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
        ControlCommand::Hook => {
            Err("control command is invalid for a workspace session".to_owned())
        }
    }
}

fn uncorrelated_control_response(error: &str) -> Vec<u8> {
    encode_control_response(&json!({"version":2,"ok":false,"error":error}), None)
}

fn invalid_control_request_response(request: &[u8], error: &serde_json::Error) -> Vec<u8> {
    let message = format!("invalid Acyclic control request: {error}");
    let request_id = serde_json::from_slice::<Value>(request)
        .ok()
        .and_then(|value| value.get("requestId")?.as_str().map(str::to_owned))
        .and_then(|value| control_protocol::RequestId::from_wire(value).ok());
    match request_id {
        Some(request_id) => control_response_for(&request_id, Err(message)),
        None => uncorrelated_control_response(&message),
    }
}

/// Encodes the response for one request, bounded to one control message.
fn control_response_for(
    request_id: &control_protocol::RequestId,
    result: Result<Value, String>,
) -> Vec<u8> {
    let response = match result {
        Ok(result) => {
            json!({"version":2,"requestId":request_id.as_str(),"ok":true,"result":result})
        }
        Err(error) => {
            json!({"version":2,"requestId":request_id.as_str(),"ok":false,"error":error})
        }
    };
    encode_control_response(&response, Some(request_id))
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

/// Encodes `response` in under one control message (leaving room for the
/// frame's newline), or, when it does not fit, the error that replaces it.
fn encode_control_response(
    response: &Value,
    request_id: Option<&control_protocol::RequestId>,
) -> Vec<u8> {
    let mut bounded = BoundedJsonBuffer::new();
    if serde_json::to_writer(&mut bounded, response).is_ok() {
        return bounded.bytes;
    }
    // Request identities are validated ASCII identifiers, so they need no
    // escaping.
    let correlation = request_id.map_or_else(String::new, |request_id| {
        format!(r#""requestId":"{}","#, request_id.as_str())
    });
    format!(
        r#"{{"version":2,{correlation}"ok":false,"error":"Acyclic control response exceeds the 4 MiB bound"}}"#
    )
    .into_bytes()
}

/// Unified Acyclic CLI, local service, Codex hook bridge, and installer.
fn main() {
    if let Err(error) = main_result() {
        if let Some((host, event)) = hook_invocation() {
            let notice = format!(
                "Acyclic is unavailable; this tool will run without an isolated workspace: {error}"
            );
            let response = if matches!(event.as_str(), "PreToolUse" | "preToolUse") {
                if host == "copilot" {
                    json!({
                        "permissionDecision": "allow",
                        "permissionDecisionReason": notice,
                        "systemMessage": notice
                    })
                } else {
                    json!({
                        "systemMessage": notice,
                        "hookSpecificOutput": {
                            "hookEventName": "PreToolUse",
                            "permissionDecision": "allow",
                            "permissionDecisionReason": notice,
                            "additionalContext": notice
                        }
                    })
                }
            } else {
                json!({"systemMessage": notice})
            };
            if serde_json::to_writer(io::stdout().lock(), &response).is_ok() {
                return;
            }
        }
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn hook_invocation() -> Option<(String, String)> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    arguments
        .windows(3)
        .find(|arguments| arguments.first().is_some_and(|value| value == "__hook"))
        .and_then(|arguments| Some((arguments.get(1)?.clone(), arguments.get(2)?.clone())))
}

fn main_result() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut arguments = env::args().skip(1);
    if arguments.next().as_deref() == Some("__hook") {
        return run_native_hook(&arguments.collect::<Vec<_>>());
    }
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

/// Answers a native hook. A hook that stays inside this process returns
/// before any runtime, path resolution or state directory is touched.
fn run_native_hook(arguments: &[String]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let [host, event] = arguments else {
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
    if native_hook_is_process_local_noop(host, event, &input) {
        serde_json::to_writer(io::stdout().lock(), &json!({}))?;
        return Ok(());
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(send_native_hook(host, event, input))
}

/// Sends one native hook to the service and prints its response.
async fn send_native_hook(
    host: &str,
    event: &str,
    input: Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The service canonicalizes whichever path a hook resolves to.
    let cwd = env::current_dir()?;
    let data = default_data_directory();
    let request = ControlRequest {
        version: 1,
        command: ControlCommand::Hook,
        cwd,
        argv: Vec::new(),
        name: format!("{host}:{event}"),
        arguments: input,
    };
    let envelope = ControlEnvelope::new(request);
    let response = if matches!(event, "SessionStart" | "sessionStart") {
        // Session boundaries are the one cheap, deterministic place to advance
        // an idle service to the installed binary. Tool hooks stay on the direct
        // single-round-trip path, and a service with live mounts remains intact.
        ensure_service(&data).await.map_err(io::Error::other)?;
        send_control_envelope(&data, &envelope)
            .await
            .map_err(|error| io::Error::other(error.to_string()))?
    } else {
        match send_control_envelope_once(&data, &envelope).await {
            Ok(response) => response,
            // An unavailable service never received the envelope, so this
            // is the only retransmission, and no deadline applies to it.
            Err(ControlRequestError::Unavailable(_)) => {
                ensure_service(&data).await.map_err(io::Error::other)?;
                send_control_envelope(&data, &envelope)
                    .await
                    .map_err(|error| io::Error::other(error.to_string()))?
            }
            Err(error) => return Err(io::Error::other(error.to_string()).into()),
        }
    };
    serde_json::to_writer(io::stdout().lock(), &response)?;
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
                    | "install"
                    | "uninstall"
                    | "__service-drain"
                    | "__installer-rename"
                    | "__verify-certification"
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
    let invocation_cwd = env::current_dir()?.canonicalize()?;
    let mut cwd = invocation_cwd.clone();
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
    if arguments
        .first()
        .is_some_and(|argument| argument == "__verify-certification")
    {
        let [_, receipt] = arguments.as_slice() else {
            return Err(io::Error::other("acyclic __verify-certification requires RECEIPT").into());
        };
        let bytes = fs::read(receipt)?;
        if bytes.len() > 1024 * 1024 {
            return Err(io::Error::other("certification receipt exceeds 1 MiB").into());
        }
        let receipt: Value = serde_json::from_slice(&bytes)?;
        let digest = blake3_file(&current_executable().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        if !valid_platform_receipt(&receipt, &digest) {
            return Err(io::Error::other(
                "certification receipt does not match this Acyclic executable",
            )
            .into());
        }
        return Ok(());
    }
    if let Some(command) = arguments.first().map(String::as_str)
        && matches!(command, "git" | "agents" | "doctor" | "discard")
    {
        let json_output = if matches!(command, "agents" | "doctor") {
            parse_read_only_arguments(arguments.get(1..).unwrap_or_default())?
        } else {
            arguments
                .get(1)
                .is_some_and(|argument| argument == "--json")
        };
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
            cwd: invocation_cwd,
            argv: arguments.get(1..).unwrap_or_default().to_vec(),
            name: String::new(),
            arguments: cli_routing(cwd),
        };
        let response = send_cli_control_request(&data, &request)
            .await
            .map_err(io::Error::other)?;
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
        if arguments.len() > 2 {
            return Err(
                io::Error::other("usage: acyclic __service-drain [EXPECTED_IDENTITY]").into(),
            );
        }
        let fence = drain_service(
            &default_data_directory(),
            arguments.get(1).map(String::as_str),
        )
        .await
        .map_err(io::Error::other)?;
        drop(fence);
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "__service-status")
    {
        serde_json::to_writer(
            io::stdout().lock(),
            &service_status(&default_data_directory())
                .await
                .map_err(io::Error::other)?,
        )?;
        return Ok(());
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
        return PathBuf::from(root).join("Acyclic").join("state-v5");
    }
    #[cfg(not(windows))]
    if let Some(root) = env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(root).join("acyclic").join("state-v5");
    }
    #[cfg(not(windows))]
    if let Some(root) = env::var_os("HOME") {
        return PathBuf::from(root)
            .join(".local")
            .join("state")
            .join("acyclic")
            .join("state-v5");
    }
    env::temp_dir().join("acyclic-state-v5")
}

/// The running executable's own path. macOS reports the path it was started
/// through, which for the npm-installed command is a link outside the package.
fn current_executable() -> Result<PathBuf, String> {
    let executable = env::current_exe().map_err(display)?;
    #[cfg(unix)]
    let executable = executable.canonicalize().map_err(display)?;
    Ok(executable)
}

fn service_identity(data: &Path) -> Result<String, String> {
    service_identity_for(data, &current_executable()?)
}

/// The identity of an executable's artifact bytes. Hashing tens of megabytes
/// on every session start is avoidable: the identity is cached under the
/// file's fingerprint, which every rewrite or replacement of the file changes.
fn service_identity_for(data: &Path, executable: &Path) -> Result<String, String> {
    let cache = data.join("executable-identity");
    let file = fs::File::open(executable).map_err(display)?;
    let (fingerprint, changed) = executable_fingerprint(&file)?;
    if let Some(identity) = fs::read(&cache)
        .ok()
        .and_then(|cached| cached_identity(&cached, &fingerprint))
    {
        return Ok(identity);
    }
    let mut hasher = blake3::Hasher::new();
    read_executable(&file, |bytes| {
        hasher.update(bytes);
    })?;
    let identity = service_identity_from_digest(&hasher.finalize().to_hex());
    // Cache only a hash of bytes that did not change while they were read and
    // whose last change is older than any timestamp granularity: a later write
    // in the same clock tick could otherwise keep the fingerprint (the racy
    // timestamp problem Git's index solves the same way). A cache that cannot
    // be written, or is lost, only costs the next caller one hash.
    let settled = std::time::SystemTime::now()
        .duration_since(changed)
        .is_ok_and(|age| age > SETTLED_EXECUTABLE_AGE);
    if settled && executable_fingerprint(&file)?.0 == fingerprint {
        let staged = data.join(format!("executable-identity.{}", std::process::id()));
        let entry = format!("{}\n{identity}", hex::encode(fingerprint));
        if fs::write(&staged, entry)
            .and_then(|()| fs::rename(&staged, &cache))
            .is_err()
        {
            let _ = fs::remove_file(&staged);
        }
    }
    Ok(identity)
}

fn cached_identity(cached: &[u8], fingerprint: &[u8; 32]) -> Option<String> {
    let (cached_fingerprint, identity) = std::str::from_utf8(cached).ok()?.split_once('\n')?;
    (cached_fingerprint == hex::encode(fingerprint)
        && identity.len() == 64
        && identity
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')))
    .then(|| identity.to_owned())
}

/// An executable changed longer ago than this has timestamps that any later
/// write must advance.
const SETTLED_EXECUTABLE_AGE: std::time::Duration = std::time::Duration::from_secs(1);

/// Digest of the file's identity, size, and modification and change times,
/// with the change time. The change time cannot be set by callers, so no write
/// can preserve it.
#[cfg(unix)]
fn executable_fingerprint(file: &fs::File) -> Result<([u8; 32], std::time::SystemTime), String> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = file.metadata().map_err(display)?;
    let mut hasher = blake3::Hasher::new();
    for field in [
        metadata.dev(),
        metadata.ino(),
        metadata.size(),
        metadata.mtime().cast_unsigned(),
        metadata.mtime_nsec().cast_unsigned(),
        metadata.ctime().cast_unsigned(),
        metadata.ctime_nsec().cast_unsigned(),
    ] {
        hasher.update(&field.to_le_bytes());
    }
    let changed = std::time::UNIX_EPOCH
        .checked_add(std::time::Duration::new(
            metadata.ctime().try_into().unwrap_or_default(),
            metadata.ctime_nsec().try_into().unwrap_or_default(),
        ))
        .unwrap_or(std::time::UNIX_EPOCH);
    Ok((*hasher.finalize().as_bytes(), changed))
}

/// Digest of the file's volume and identity, size, and write and change
/// times, with the change time. The change time cannot be set by callers, so
/// no write can preserve it.
#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "GetFileInformationByHandleEx fills fixed-size structures for a live handle"
)]
fn executable_fingerprint(file: &fs::File) -> Result<([u8; 32], std::time::SystemTime), String> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_BASIC_INFO, FILE_ID_INFO, FileBasicInfo, FileIdInfo, GetFileInformationByHandleEx,
    };
    fn query<T>(file: &fs::File, class: i32) -> Result<T, String> {
        let mut information = std::mem::MaybeUninit::<T>::zeroed();
        let size = u32::try_from(std::mem::size_of::<T>()).map_err(display)?;
        // SAFETY: the handle is live for the call and the buffer is exactly
        // `size` writable bytes of the structure this class returns.
        if unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle(),
                class,
                information.as_mut_ptr().cast(),
                size,
            )
        } == 0
        {
            return Err(display(io::Error::last_os_error()));
        }
        // SAFETY: the call succeeded, so it initialized the structure.
        Ok(unsafe { information.assume_init() })
    }
    let identity = query::<FILE_ID_INFO>(file, FileIdInfo)?;
    let basic = query::<FILE_BASIC_INFO>(file, FileBasicInfo)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&identity.VolumeSerialNumber.to_le_bytes());
    hasher.update(&identity.FileId.Identifier);
    hasher.update(&file.metadata().map_err(display)?.len().to_le_bytes());
    for time in [basic.CreationTime, basic.LastWriteTime, basic.ChangeTime] {
        hasher.update(&time.to_le_bytes());
    }
    // FILETIME counts 100 ns intervals from 1601; Unix time starts 11,644,473,600 s later.
    let changed = u64::try_from(basic.ChangeTime)
        .ok()
        .and_then(|ticks| ticks.checked_sub(116_444_736_000_000_000))
        .and_then(|ticks| {
            std::time::UNIX_EPOCH
                .checked_add(std::time::Duration::from_nanos(ticks.saturating_mul(100)))
        })
        .unwrap_or(std::time::UNIX_EPOCH);
    Ok((*hasher.finalize().as_bytes(), changed))
}

fn service_identity_from_digest(digest: &str) -> String {
    blake3::hash(format!("{}:{digest}", env!("CARGO_PKG_VERSION")).as_bytes())
        .to_hex()
        .to_string()
}

fn read_executable(mut file: &fs::File, mut update: impl FnMut(&[u8])) -> Result<(), String> {
    file.rewind().map_err(display)?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(display)?;
        if read == 0 {
            break;
        }
        let chunk = buffer
            .get(..read)
            .ok_or_else(|| "binary hash read exceeded its buffer".to_owned())?;
        update(chunk);
    }
    Ok(())
}

struct ExecutableDigests {
    service_identity: String,
    sha256: String,
    blake3: String,
}

fn executable_digests() -> Result<ExecutableDigests, String> {
    executable_digests_for(&current_executable()?)
}

async fn executable_digests_async() -> Result<ExecutableDigests, String> {
    tokio::task::spawn_blocking(executable_digests)
        .await
        .map_err(display)?
}

fn executable_digests_for(executable: &Path) -> Result<ExecutableDigests, String> {
    // Launchers and plugin caches can expose the same signed artifact at different paths. The
    // service belongs to the artifact, not to one of those aliases; including the path caused
    // identical clients to continuously drain and replace each other's service.
    let mut sha256 = Sha256::new();
    let mut blake3 = blake3::Hasher::new();
    read_executable(&fs::File::open(executable).map_err(display)?, |chunk| {
        sha256.update(chunk);
        blake3.update(chunk);
    })?;
    let sha256 = hex::encode(sha256.finalize());
    let blake3 = blake3.finalize().to_hex().to_string();
    Ok(ExecutableDigests {
        service_identity: service_identity_from_digest(&blake3),
        sha256,
        blake3,
    })
}

fn blake3_file(path: &Path) -> Result<String, String> {
    let mut hasher = blake3::Hasher::new();
    read_executable(&fs::File::open(path).map_err(display)?, |bytes| {
        hasher.update(bytes);
    })?;
    Ok(hasher.finalize().to_hex().to_string())
}

fn doctor_check(name: &str, status: &str, detail: impl Into<String>) -> Value {
    json!({"name": name, "status": status, "detail": detail.into()})
}

fn valid_platform_receipt(receipt: &Value, executable_blake3: &str) -> bool {
    const COVERAGE: &[&str] = &[
        "create-read-write",
        "atomic-save",
        "rename-delete",
        "rename-before-hydration",
        "nested-paths",
        "large-directory-paging",
        "concurrent-handles",
        "watchers",
        "crash-detach-recovery",
        "mount-restoration",
        "hard-links",
        "symbolic-links-reparse-points",
        "metadata",
        "case-behavior",
        "escape-attempts",
        "root-checkout-untouched",
        "git-administration-untouched",
    ];
    const CASES: &[&str] = &[
        "real-mount-mutation-matrix",
        "crash-detach-recovery",
        "checkout-and-git-untouched",
    ];
    let (required_kind, provider_process_io_observable) = match env::consts::OS {
        "linux" => ("linux-fuse", true),
        "macos" => ("macos-nfs", true),
        "windows" => ("windows-projfs", false),
        _ => return false,
    };
    let Some(document) = receipt.as_object() else {
        return false;
    };
    let expected_keys = [
        "schema",
        "os",
        "arch",
        "coverage",
        "capability",
        "required_kind",
        "release_version",
        "executable_blake3",
        "passed",
        "cases",
    ];
    if document.len() != expected_keys.len()
        || expected_keys.iter().any(|key| !document.contains_key(*key))
        || receipt.get("schema").and_then(Value::as_str)
            != Some("acyclic-native-mount-qualification-v2")
        || receipt.get("os").and_then(Value::as_str) != Some(env::consts::OS)
        || receipt.get("arch").and_then(Value::as_str) != Some(env::consts::ARCH)
        || receipt.get("required_kind").and_then(Value::as_str) != Some(required_kind)
        || receipt.get("release_version").and_then(Value::as_str) != Some(env!("CARGO_PKG_VERSION"))
        || receipt.get("executable_blake3").and_then(Value::as_str) != Some(executable_blake3)
        || receipt.get("passed").and_then(Value::as_bool) != Some(true)
    {
        return false;
    }
    let coverage = receipt.get("coverage").and_then(Value::as_array);
    if coverage.is_none_or(|values| {
        let observed = values
            .iter()
            .map(Value::as_str)
            .collect::<Option<BTreeSet<_>>>();
        observed.is_none_or(|observed| {
            observed.len() != values.len()
                || COVERAGE.iter().any(|expected| !observed.contains(expected))
        })
    }) {
        return false;
    }
    let capability = receipt.get("capability");
    if capability
        .and_then(|value| value.get("kind"))
        .and_then(Value::as_str)
        != Some(required_kind)
        || capability
            .and_then(|value| value.get("available"))
            .and_then(Value::as_bool)
            != Some(true)
        || capability
            .and_then(|value| value.get("writable"))
            .and_then(Value::as_bool)
            != Some(true)
        || capability
            .and_then(|value| value.get("provider_process_io_observable"))
            .and_then(Value::as_bool)
            != Some(provider_process_io_observable)
        || capability
            .and_then(|value| value.get("session_isolation"))
            .and_then(Value::as_str)
            != Some("SharedProcess")
        || capability
            .and_then(|value| value.get("unavailable_reason"))
            .is_none_or(|value| !value.is_null())
    {
        return false;
    }
    receipt
        .get("cases")
        .and_then(Value::as_array)
        .is_some_and(|cases| {
            let observed = cases
                .iter()
                .filter_map(|case| case.get("name").and_then(Value::as_str))
                .collect::<BTreeSet<_>>();
            observed.len() == cases.len()
                && CASES.iter().all(|expected| observed.contains(expected))
                && cases.iter().all(|case| {
                    case.get("status").and_then(Value::as_str) == Some("passed")
                        && case.get("reason").is_some_and(Value::is_null)
                })
        })
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
    executable_sha256: &str,
    executable_blake3: &str,
    sessions: usize,
    routes: usize,
    leases: usize,
    pending_recovery: bool,
) -> Result<Value, String> {
    let executable = current_executable()?;
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
                    == Some(executable_sha256) =>
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
            let ownership = read_codex_ownership(&codex_ownership_path()).ok().flatten();
            let configured_owned = ownership.as_ref().is_some_and(|ownership| {
                ownership.version == 1
                    && ownership
                        .marketplace_root
                        .canonicalize()
                        .ok()
                        .zip(root.canonicalize().ok())
                        .is_some_and(|(configured, packaged)| configured == packaged)
                    && plan_codex_config(ownership).is_ok()
                    && codex_plugin_configuration_matches(ownership, root)
            });
            checks.push(doctor_check(
                "codex-install",
                if configured_owned { "pass" } else { "fail" },
                ownership.map_or_else(
                    || "Acyclic has no Codex installation ownership record".to_owned(),
                    |ownership| {
                        format!(
                            "marketplace={} plugin-version={}",
                            ownership.marketplace_root.display(),
                            env!("CARGO_PKG_VERSION")
                        )
                    },
                ),
            ));
            let hooks = root.join("hooks/hooks.json");
            let mcp = root.join(".mcp.json");
            // The npm command links to `bin/acyclic`: the executable itself on
            // Unix, and on Windows a name that resolves to `acyclic.exe` once
            // the installer has removed the placeholder.
            let native = root.join(if cfg!(windows) {
                "bin/acyclic.exe"
            } else {
                "bin/acyclic"
            });
            let command_placeholder = cfg!(windows) && root.join("bin/acyclic").exists();
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
                            .any(|hook| {
                                hook.get("command").and_then(Value::as_str)
                                    == Some(format!("\"${{PLUGIN_ROOT}}/bin/acyclic\" __hook codex {event}").as_str())
                                    && (!cfg!(windows)
                                        || hook.get("commandWindows").and_then(Value::as_str)
                                            == Some(format!("& \"$env:PLUGIN_ROOT\\bin\\acyclic.exe\" __hook codex {event}").as_str()))
                            })
                    })
                });
            let command_valid = native.is_file() && !command_placeholder && !mcp.exists();
            checks.push(doctor_check(
                "hooks",
                if hooks_valid { "pass" } else { "fail" },
                hooks.display().to_string(),
            ));
            checks.push(doctor_check(
                "agent-command",
                if command_valid { "pass" } else { "fail" },
                if command_valid {
                    "npm command runs the native executable; no MCP bridge exposed to shell-capable Codex"
                } else {
                    "npm command does not reach the native executable, or a commandless MCP bridge is exposed"
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
        if native.available && native.writable && routes > 0 {
            "pass"
        } else {
            "warn"
        },
        format!(
            "kind={:?} available={} writable={} provider-io-observable={} qualification={}{}",
            native.kind,
            native.available,
            native.writable,
            native.provider_process_io_observable,
            if routes > 0 {
                "runtime-observed"
            } else {
                "first-spawn-runtime-check"
            },
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
        .is_some_and(|receipt| valid_platform_receipt(&receipt, executable_blake3));
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
        "schemaVersion": 2,
        "ok": ok,
        "capabilities": {
            "nativeMount": {
                "available": native.available,
                "writable": native.writable,
                "providerProcessIoObservable": native.provider_process_io_observable,
                "sessionIsolation": format!("{:?}", native.session_isolation),
                "qualifiedForThisBinary": certified,
            },
            "processConfinement": {
                "qualified": false,
                "reason": "requires a separate passing host/platform escape qualification"
            }
        },
        "version": env!("CARGO_PKG_VERSION"),
        "platform": {"os": env::consts::OS, "arch": env::consts::ARCH},
        "checks": checks,
    }))
}

struct ServiceLock {
    lifecycle_file: fs::File,
    data_file: Option<fs::File>,
}

impl ServiceLock {
    fn prepare_purge(&mut self) {
        if let Some(file) = self.data_file.take() {
            let _ = file.unlock();
        }
    }
}

impl Drop for ServiceLock {
    fn drop(&mut self) {
        self.prepare_purge();
        let _ = self.lifecycle_file.unlock();
    }
}

/// How any Acyclic binary identifies and stops the service of any other.
///
/// The control protocol changes between releases, and a service rejects a
/// protocol it does not speak, so a new binary could neither identify nor
/// drain an old service through it. This contract never changes, so every
/// binary from this one on can hand off to any other:
///
/// - The service holds an exclusive lock on `service.lock` for its whole
///   life, so a free lock proves that no service runs.
/// - While its endpoint accepts requests it publishes `service.identity`,
///   exactly `acyclic-service-v1\n{instance id}\n{binary identity}\n`.
/// - A file `service-stop/{instance id}` holding a drain ID of at most 128
///   ASCII letters, digits and hyphens asks that instance to drain every
///   session and exit. It then writes `service-drain.json`, version 1, with
///   its instance ID as `identity`, the `drainId` and whether teardown
///   succeeded, and releases its lock.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ServiceMarker {
    instance_id: String,
    binary_identity: String,
}

const SERVICE_MARKER_HEADER: &str = "acyclic-service-v1";
const SERVICE_STOP_DIRECTORY: &str = "service-stop";

impl ServiceMarker {
    fn path(data: &Path) -> PathBuf {
        data.join("service.identity")
    }

    fn encode(&self) -> String {
        format!(
            "{SERVICE_MARKER_HEADER}\n{}\n{}\n",
            self.instance_id, self.binary_identity
        )
    }

    /// The running service's marker, if one is published.
    fn read(data: &Path) -> Option<Self> {
        let text = fs::read_to_string(Self::path(data)).ok()?;
        let mut lines = text.strip_suffix('\n')?.split('\n');
        let (Some(SERVICE_MARKER_HEADER), Some(instance_id), Some(binary_identity), None) =
            (lines.next(), lines.next(), lines.next(), lines.next())
        else {
            return None;
        };
        is_handoff_id(instance_id).then(|| Self {
            instance_id: instance_id.to_owned(),
            binary_identity: binary_identity.to_owned(),
        })
    }
}

/// Instance and drain IDs name files, so they are short and plain.
fn is_handoff_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

/// Asks the service instance `instance_id` to drain; see [`ServiceMarker`].
fn request_service_stop(data: &Path, instance_id: &str, drain_id: &str) -> Result<(), String> {
    let directory = data.join(SERVICE_STOP_DIRECTORY);
    fs::create_dir_all(&directory).map_err(display)?;
    // Written aside and renamed, so the service never reads a partial ID.
    let staged = directory.join(format!("{instance_id}.{drain_id}.next"));
    fs::write(&staged, drain_id).map_err(display)?;
    fs::rename(&staged, directory.join(instance_id)).map_err(display)
}

/// The service's side of stop requests; see [`ServiceMarker`].
struct StopRequests {
    request: PathBuf,
    arrived: Arc<tokio::sync::Notify>,
    _watcher: acyclic_fs::watch::NativeEventWatcher,
}

impl StopRequests {
    fn open(data: &Path, instance_id: &str) -> Result<Self, String> {
        let directory = data.join(SERVICE_STOP_DIRECTORY);
        fs::create_dir_all(&directory).map_err(display)?;
        // Requests addressed to earlier instances can never be served.
        for entry in fs::read_dir(&directory).map_err(display)? {
            match fs::remove_file(entry.map_err(display)?.path()) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(display(error)),
            }
        }
        let arrived = Arc::new(tokio::sync::Notify::new());
        let notification = Arc::clone(&arrived);
        // Any event, or a watcher error, only prompts another look.
        let mut watcher = <acyclic_fs::watch::NativeEventWatcher as notify::Watcher>::new(
            move |_| notification.notify_one(),
            notify::Config::default(),
        )
        .map_err(display)?;
        notify::Watcher::watch(
            &mut watcher,
            &directory,
            notify::RecursiveMode::NonRecursive,
        )
        .map_err(display)?;
        Ok(Self {
            request: directory.join(instance_id),
            arrived,
            _watcher: watcher,
        })
    }

    /// Waits for a stop request and returns its drain ID.
    async fn next(&self) -> String {
        loop {
            if let Some(drain_id) = fs::read_to_string(&self.request)
                .ok()
                .filter(|drain_id| is_handoff_id(drain_id))
            {
                return drain_id;
            }
            self.arrived.notified().await;
        }
    }
}

/// The published marker, withdrawn when the service stops answering.
struct PublishedServiceMarker {
    path: PathBuf,
}

impl PublishedServiceMarker {
    fn create(data: &Path, marker: &ServiceMarker) -> Result<Self, String> {
        let path = ServiceMarker::path(data);
        fs::write(&path, marker.encode()).map_err(display)?;
        Ok(Self { path })
    }
}

impl Drop for PublishedServiceMarker {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn service_drain_completion_path(data: &Path) -> PathBuf {
    data.join("service-drain.json")
}

fn write_service_drain_completion(
    data: &Path,
    identity: &str,
    drain_id: &str,
    result: &Result<(), String>,
) -> Result<(), String> {
    let path = service_drain_completion_path(data);
    let next = data.join("service-drain.next.json");
    let value = json!({
        "version": 1,
        "identity": identity,
        "drainId": drain_id,
        "ok": result.is_ok(),
        "error": result.as_ref().err(),
    });
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&next)
        .map_err(display)?;
    serde_json::to_writer_pretty(&mut file, &value).map_err(display)?;
    file.write_all(b"\n").map_err(display)?;
    file.sync_all().map_err(display)?;
    drop(file);
    durable_rename(&next, &path, RenameMode::Replace).map_err(display)
}

fn verify_service_drain_completion(
    data: &Path,
    identity: &str,
    drain_id: &str,
) -> Result<(), String> {
    let path = service_drain_completion_path(data);
    let value: Value = serde_json::from_slice(&fs::read(&path).map_err(|error| {
        format!(
            "Acyclic service exited without durable drain confirmation at {}: {error}",
            path.display()
        )
    })?)
    .map_err(display)?;
    if value.get("version").and_then(Value::as_u64) != Some(1)
        || value.get("identity").and_then(Value::as_str) != Some(identity)
        || value.get("drainId").and_then(Value::as_str) != Some(drain_id)
    {
        return Err("Acyclic service drain confirmation does not match this request".to_owned());
    }
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(format!(
            "Acyclic service teardown failed: {}",
            value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown teardown error")
        ));
    }
    Ok(())
}

fn acquire_service_lock(data: &Path) -> Result<Option<ServiceLock>, String> {
    let parent = data
        .parent()
        .ok_or_else(|| "service data path has no parent".to_owned())?;
    fs::create_dir_all(parent).map_err(display)?;
    let lifecycle_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(parent.join(".acyclic-service-lifecycle.lock"))
        .map_err(display)?;
    match lifecycle_file.try_lock_exclusive() {
        Ok(()) => {}
        Err(error) if service_lock_is_contended(&error) => return Ok(None),
        Err(error) => return Err(display(error)),
    }
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
        Ok(()) => Ok(Some(ServiceLock {
            lifecycle_file,
            data_file: Some(file),
        })),
        Err(error) if service_lock_is_contended(&error) => Ok(None),
        Err(error) => Err(display(error)),
    }
}

fn service_lock_is_contended(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock || cfg!(windows) && error.raw_os_error() == Some(33)
}

async fn run_service(data: PathBuf) -> Result<(), String> {
    // Taken first, so nothing the service prints can reach its starter.
    let ready = acyclic_native_runtime::take_service_ready_signal().map_err(display)?;
    run_service_with_identity(data, None, ready).await
}

async fn shutdown_service_endpoint(
    endpoint: ControlEndpoint,
    control: Arc<ConcurrentServiceControl>,
) -> Result<(), String> {
    let endpoint_result = endpoint.shutdown().await;
    let control_result = match Arc::try_unwrap(control) {
        Ok(control) => control.shutdown().await,
        Err(_) => Err("control service retained an active request during shutdown".to_owned()),
    };
    endpoint_result.and(control_result)
}

/// Runs the service. `ready`, when its starter waits on it, is signalled once
/// the service answers requests, and closes unsignalled if it exits first,
/// for instance because another service holds the lock.
async fn run_service_with_identity(
    data: PathBuf,
    identity_override: Option<String>,
    ready: Option<acyclic_native_runtime::ServiceReadySignal>,
) -> Result<(), String> {
    let Some(lock) = acquire_service_lock(&data)? else {
        return Ok(());
    };
    let result = run_locked_service(data, identity_override, ready).await;
    drop(lock);
    result
}

async fn run_locked_service(
    data: PathBuf,
    identity_override: Option<String>,
    ready: Option<acyclic_native_runtime::ServiceReadySignal>,
) -> Result<(), String> {
    let mut service = ConcurrentServiceControl::open(data.clone()).await?;
    if let Some(identity) = identity_override {
        service.resources.binary_identity = identity;
    }
    let instance_id = service.instance_id.clone();
    let binary_identity = service.binary_identity.clone();
    let control = Arc::new(service);
    let endpoint = match start_control_endpoint(Arc::clone(&control), &data).await {
        Ok(endpoint) => endpoint,
        Err(endpoint_error) => {
            let control_result = match Arc::try_unwrap(control) {
                Ok(control) => control.shutdown().await,
                Err(_) => Err("failed endpoint retained the control service".to_owned()),
            };
            return match control_result {
                Ok(()) => Err(endpoint_error),
                Err(shutdown_error) => Err(format!(
                    "{endpoint_error}; endpoint startup cleanup failed: {shutdown_error}"
                )),
            };
        }
    };
    let published = StopRequests::open(&data, &instance_id).and_then(|stops| {
        let marker = ServiceMarker {
            instance_id: instance_id.clone(),
            binary_identity,
        };
        PublishedServiceMarker::create(&data, &marker).map(|marker| (stops, marker))
    });
    let (stops, _marker) = match published {
        Ok(published) => published,
        Err(marker_error) => {
            let cleanup = shutdown_service_endpoint(endpoint, control).await;
            return match cleanup {
                Ok(()) => Err(marker_error),
                Err(cleanup_error) => Err(format!(
                    "{marker_error}; identity publication cleanup failed: {cleanup_error}"
                )),
            };
        }
    };
    if let Some(ready) = ready {
        // A starter that stopped waiting has nothing to be told.
        let _ = ready.signal();
    }
    let (service_result, requested_drain) = tokio::select! {
        signal = tokio::signal::ctrl_c() => (signal.map_err(display), None),
        drain_id = stops.next() => {
            control.begin_drain();
            (Ok(()), Some(drain_id))
        }
    };
    drop(stops);
    let result = service_result.and(shutdown_service_endpoint(endpoint, control).await);
    if let Some(drain_id) = requested_drain {
        write_service_drain_completion(&data, &instance_id, &drain_id, &result)?;
    }
    result
}

async fn service_is_ready_for_identity(data: &Path, identity: &str) -> Result<bool, String> {
    let ping = ping_request()?;
    let answer = send_control_request_once(data, &ping).await;
    if let Ok(active) = &answer
        && active.get("identity").and_then(Value::as_str) == Some(identity)
    {
        return Ok(true);
    }
    // Only a service that holds its lock can still open an endpoint; when
    // none does, nothing needs waiting for.
    if claim_stopped_service(data, None)?.is_some() {
        return Ok(false);
    }
    // A running service says what it is through its marker, whatever
    // protocol it speaks, and a service of another binary is stopped
    // through the same contract.
    match ServiceMarker::read(data) {
        Some(marker) if marker.binary_identity != identity => {
            let fence = drain_service(data, Some(&marker.instance_id)).await?;
            clear_obsolete_runtime_state(data)?;
            drop(fence);
            Ok(false)
        }
        // This binary's service, or one still starting: it answers soon.
        _ => match answer {
            Ok(_) | Err(ControlRequestError::Unavailable(_)) => Ok(false),
            Err(error) => Err(format!(
                "cannot safely identify the Acyclic service: {error}"
            )),
        },
    }
}

fn clear_obsolete_runtime_state(data: &Path) -> Result<(), String> {
    for name in ["core-state", "filesystem", "sessions", "w"] {
        remove_tree_checked(data, &data.join(name))?;
    }
    Ok(())
}

async fn ensure_service(data: &Path) -> Result<(), String> {
    fs::create_dir_all(data).map_err(display)?;
    let identity = service_identity(data)?;
    if service_is_ready_for_identity(data, &identity).await? {
        return Ok(());
    }
    let ping = ping_request()?;
    let readiness = spawn_service_process(&current_executable()?)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    // The service signals once it answers requests. A channel that closes
    // unsignalled means it exited, typically because another service holds
    // the lock and is starting; only then does this poll for that one.
    wait_for_service_readiness(readiness, deadline).await?;
    let last = loop {
        let last = match send_control_request_once(data, &ping).await {
            Ok(active)
                if active.get("identity").and_then(Value::as_str) == Some(identity.as_str()) =>
            {
                return Ok(());
            }
            Ok(_) => "the previous Acyclic service is still draining".to_owned(),
            Err(error) => error.to_string(),
        };
        if std::time::Instant::now() >= deadline {
            break last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    Err(format!("Acyclic service did not become ready: {last}"))
}

fn spawn_service_process(
    executable: &Path,
) -> Result<acyclic_native_runtime::ServiceReadiness, String> {
    acyclic_native_runtime::spawn_service_process(executable).map_err(display)
}

/// Waits, until `deadline`, for a started service to signal readiness or
/// exit. The blocking read runs on its own thread, which the process may
/// leave behind when it exits.
async fn wait_for_service_readiness(
    readiness: acyclic_native_runtime::ServiceReadiness,
    deadline: std::time::Instant,
) -> Result<(), String> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("acyclic-service-readiness".to_owned())
        .spawn(move || {
            let _ = sender.send(readiness.wait());
        })
        .map_err(display)?;
    let wait = deadline.saturating_duration_since(std::time::Instant::now());
    let _ = tokio::time::timeout(wait, receiver).await;
    Ok(())
}

fn ping_request() -> Result<ControlRequest, String> {
    Ok(ControlRequest {
        version: 1,
        command: ControlCommand::Ping,
        cwd: env::current_dir().map_err(display)?,
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    })
}

async fn send_cli_control_request(data: &Path, request: &ControlRequest) -> Result<Value, String> {
    let identity = service_identity(data)?;
    // Sandboxed hosts may expose the already-running local endpoint while denying the client's
    // direct view of per-user state. Probe that endpoint before attempting a filesystem-backed
    // cold start. The published marker is an instance nonce, not a binary compatibility identity.
    let ping = ping_request()?;
    for attempt in 0..10 {
        match send_control_request(data, &ping).await {
            Ok(active)
                if active.get("identity").and_then(Value::as_str) == Some(identity.as_str()) =>
            {
                return send_control_request(data, request)
                    .await
                    .map_err(|error| error.to_string());
            }
            Err(ControlRequestError::Unavailable(_)) if attempt < 9 => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Ok(_) | Err(ControlRequestError::Unavailable(_)) => break,
            Err(error) => return Err(error.to_string()),
        }
    }
    ensure_service(data).await?;
    send_control_request(data, request)
        .await
        .map_err(|error| error.to_string())
}

fn control_request_from_argv(cwd: &Path, mut argv: Vec<String>) -> Result<ControlRequest, String> {
    let cwd = cwd.canonicalize().map_err(display)?;
    let mut selected_cwd = cwd.clone();
    if argv.first().is_some_and(|argument| argument == "-C") {
        if argv.len() < 2 {
            return Err("acyclic -C requires a path".to_owned());
        }
        selected_cwd = PathBuf::from(argv.remove(1))
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
        arguments: cli_routing(selected_cwd),
    })
}

async fn run_rpc_proxy(
    data: &Path,
    mut reader: impl BufRead,
    mut writer: impl Write,
    commandless: bool,
) -> Result<(), String> {
    while let Some(line) = read_bounded_rpc_line(&mut reader)? {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let request: Value = serde_json::from_slice(&line).map_err(display)?;
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
                    control_request_from_argv(&env::current_dir().map_err(display)?, argv)?
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

fn read_bounded_rpc_line(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>, String> {
    let mut line = Vec::new();
    let read = reader
        .take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64)
        .read_until(b'\n', &mut line)
        .map_err(display)?;
    if read == 0 {
        return Ok(None);
    }
    if line.len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
        return Err("Acyclic MCP request exceeds the maximum frame size".to_owned());
    }
    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
    Ok(Some(line))
}

#[derive(Debug)]
enum ControlRequestError {
    Unavailable(String),
    Indeterminate(String),
    Response(String),
}

impl std::fmt::Display for ControlRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(message) | Self::Indeterminate(message) | Self::Response(message) => {
                formatter.write_str(message)
            }
        }
    }
}

async fn send_control_request(
    data: &Path,
    request: &ControlRequest,
) -> Result<Value, ControlRequestError> {
    send_control_envelope(data, &ControlEnvelope::new(request.clone())).await
}

async fn send_control_envelope(
    data: &Path,
    envelope: &ControlEnvelope<ControlRequest>,
) -> Result<Value, ControlRequestError> {
    send_control_envelope_with_attempts(data, envelope, 50, control_request_wait(&envelope.request))
        .await
}

fn control_request_wait(request: &ControlRequest) -> std::time::Duration {
    match request.command {
        ControlCommand::Ping => CONTROL_PROBE_WAIT,
        ControlCommand::Hook if request.name.ends_with(":SessionEnd") => CONTROL_PROBE_WAIT,
        ControlCommand::Hook => CONTROL_HOOK_WAIT,
        ControlCommand::Doctor
        | ControlCommand::Git
        | ControlCommand::Agents
        | ControlCommand::Discard => CONTROL_COMMAND_WAIT,
    }
}

async fn send_control_request_once(
    data: &Path,
    request: &ControlRequest,
) -> Result<Value, ControlRequestError> {
    send_control_envelope_once(data, &ControlEnvelope::new(request.clone())).await
}

async fn send_control_envelope_once(
    data: &Path,
    envelope: &ControlEnvelope<ControlRequest>,
) -> Result<Value, ControlRequestError> {
    send_control_envelope_with_attempts(data, envelope, 1, CONTROL_PROBE_WAIT).await
}

async fn send_control_envelope_with_attempts(
    data: &Path,
    envelope: &ControlEnvelope<ControlRequest>,
    windows_connect_attempts: usize,
    response_wait: std::time::Duration,
) -> Result<Value, ControlRequestError> {
    let deadline = tokio::time::Instant::now() + response_wait;
    #[cfg(not(windows))]
    let _ = windows_connect_attempts;
    let mut encoded = serde_json::to_vec(envelope)
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
    encoded.push(b'\n');
    #[cfg(target_os = "linux")]
    return send_linux_mailbox_request(
        data,
        &encoded,
        &envelope.request_id,
        remaining_control_wait(deadline)?,
    )
    .await;
    #[cfg(not(target_os = "linux"))]
    {
        #[cfg(all(unix, not(target_os = "linux")))]
        let socket_path = unix_control_socket_path(data);
        #[cfg(all(unix, not(target_os = "linux")))]
        let stream = tokio::time::timeout(
            CONTROL_PROBE_WAIT.min(remaining_control_wait(deadline)?),
            tokio::net::UnixStream::connect(socket_path),
        )
        .await
        .map_err(|_| {
            ControlRequestError::Unavailable(
                "Acyclic service connection exceeded the probe deadline".to_owned(),
            )
        })?
        .map_err(|error| {
            ControlRequestError::Unavailable(format!("Acyclic service is not running: {error}"))
        })?;
        #[cfg(windows)]
        let stream = {
            let pipe = format!(
                r"\\.\pipe\acyclic-{}",
                short_hash(data.as_os_str().to_string_lossy().as_bytes())
            );
            let mut last = None;
            let mut connected = None;
            for attempt in 1..=windows_connect_attempts {
                let remaining = remaining_control_wait(deadline)?;
                match tokio::net::windows::named_pipe::ClientOptions::new().open(&pipe) {
                    Ok(client) => {
                        connected = Some(client);
                        break;
                    }
                    Err(error) => {
                        last = Some(error);
                        // Only a further attempt is worth waiting for.
                        if attempt < windows_connect_attempts {
                            tokio::time::sleep(std::time::Duration::from_millis(20).min(remaining))
                                .await;
                        }
                    }
                }
            }
            connected.ok_or_else(|| {
                ControlRequestError::Unavailable(format!(
                    "Acyclic service is not running: {}",
                    last.map_or_else(
                        || "unknown connection failure".to_owned(),
                        |error| error.to_string()
                    )
                ))
            })?
        };
        exchange_control_stream(
            stream,
            &encoded,
            &envelope.request_id,
            remaining_control_wait(deadline)?,
        )
        .await
    }
}

fn remaining_control_wait(
    deadline: tokio::time::Instant,
) -> Result<std::time::Duration, ControlRequestError> {
    let now = tokio::time::Instant::now();
    if now >= deadline {
        return Err(ControlRequestError::Unavailable(
            "Acyclic service did not answer before the request deadline".to_owned(),
        ));
    }
    Ok(deadline - now)
}

#[cfg(not(target_os = "linux"))]
async fn exchange_control_stream(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    encoded: &[u8],
    request_id: &control_protocol::RequestId,
    maximum_wait: std::time::Duration,
) -> Result<Value, ControlRequestError> {
    let exchange = async {
        stream
            .write_all(encoded)
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        stream
            .flush()
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        let mut response = Vec::new();
        let mut reader = BufReader::new(stream).take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64);
        reader
            .read_until(b'\n', &mut response)
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        Ok::<_, ControlRequestError>(response)
    };
    let mut response = tokio::time::timeout(maximum_wait, exchange)
        .await
        .map_err(|_| {
            ControlRequestError::Indeterminate(
                "Acyclic service did not answer before the request deadline".to_owned(),
            )
        })??;
    if response.len() > MAXIMUM_CONTROL_MESSAGE_BYTES || response.last() != Some(&b'\n') {
        return Err(ControlRequestError::Indeterminate(
            "invalid response from Acyclic service".to_owned(),
        ));
    }
    response.pop();
    decode_control_response(&response, request_id)
}

#[cfg(target_os = "linux")]
async fn send_linux_mailbox_request(
    data: &Path,
    encoded: &[u8],
    request_id: &control_protocol::RequestId,
    maximum_wait: std::time::Duration,
) -> Result<Value, ControlRequestError> {
    use std::time::{SystemTime, UNIX_EPOCH};

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let mailbox = linux_control_mailbox_path(data);
    let nonce = format!(
        "{}:{}:{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let name = short_hash(nonce.as_bytes());
    let unpublished = format!("{name}{LINUX_EXCHANGE_UNPUBLISHED}");
    let mailbox_directory = Arc::new(open_linux_directory(rustix::fs::CWD, &mailbox).map_err(
        |error| {
            ControlRequestError::Unavailable(format!("Acyclic service is not running: {error}"))
        },
    )?);
    let metadata = rustix::fs::fstat(&*mailbox_directory)
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
    if metadata.st_mode & 0o777 != 0o700 {
        return Err(ControlRequestError::Unavailable(
            "Acyclic service mailbox is not a private directory".to_owned(),
        ));
    }
    rustix::fs::mkdirat(&*mailbox_directory, &unpublished, rustix::fs::Mode::RWXU).map_err(
        |error| {
            ControlRequestError::Unavailable(format!(
                "cannot create Acyclic control exchange: {error}"
            ))
        },
    )?;
    let mut exchange = LinuxMailboxExchange {
        mailbox: Arc::clone(&mailbox_directory),
        exchange: None,
        name: unpublished.clone().into(),
    };
    let result = tokio::time::timeout(maximum_wait, async {
        let directory = open_linux_directory(&*mailbox_directory, &unpublished)
            .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        let directory = &*exchange.exchange.insert(directory);
        rustix::fs::mkfifoat(
            directory,
            LINUX_EXCHANGE_RESPONSE,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        // Holding the write end as well means the response ends at its
        // newline, never at an end of file before the service opens it.
        let mut response_pipe = rustix::fs::openat(
            directory,
            LINUX_EXCHANGE_RESPONSE,
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(errno_to_io)
        .and_then(tokio::net::unix::pipe::Receiver::from_owned_fd)
        .map(|pipe| BufReader::new(pipe).take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64))
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        let request_file = rustix::fs::openat(
            directory,
            LINUX_EXCHANGE_REQUEST,
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        std::fs::File::from(request_file)
            .write_all(encoded)
            .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        rustix::fs::renameat(
            &*mailbox_directory,
            &unpublished,
            &*mailbox_directory,
            &name,
        )
        .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        exchange.name = name.into();
        let mut response = Vec::new();
        response_pipe
            .read_until(b'\n', &mut response)
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        if response.len() > MAXIMUM_CONTROL_MESSAGE_BYTES || response.pop() != Some(b'\n') {
            return Err(ControlRequestError::Indeterminate(
                "invalid response from Acyclic service".to_owned(),
            ));
        }
        decode_control_response(&response, request_id)
    })
    .await
    .unwrap_or_else(|_| {
        Err(ControlRequestError::Indeterminate(
            "Acyclic service did not answer before the filesystem control deadline".to_owned(),
        ))
    });
    // The service removes every exchange that it answers.
    if result.is_err() {
        exchange.remove();
    }
    result
}

fn decode_control_response(
    response: &[u8],
    request_id: &control_protocol::RequestId,
) -> Result<Value, ControlRequestError> {
    let response: Value = serde_json::from_slice(response)
        .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
    if response.get("requestId").and_then(Value::as_str) != Some(request_id.as_str()) {
        return Err(ControlRequestError::Indeterminate(
            "Acyclic control response does not match the request identity".to_owned(),
        ));
    }
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
    let (host, project) = parse_install_arguments(arguments)?;
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

fn parse_install_arguments(arguments: &[String]) -> Result<(&str, bool), String> {
    match arguments {
        [host] if host == "--detected" => Ok((host, false)),
        [host] if !host.starts_with('-') => Ok((host, false)),
        [host, project] if !host.starts_with('-') && project == "--project" => Ok((host, true)),
        _ => Err("usage: acyclic install HOST [--project] | acyclic install --detected".to_owned()),
    }
}

async fn uninstall_command(arguments: &[String]) -> Result<(), String> {
    let (host, purge) = parse_uninstall_arguments(arguments)?;
    let data = default_data_directory();
    drop(drain_service(&data, None).await?);
    uninstall_host(host)?;
    if purge {
        purge_durable_state(&data).await?;
        println!("purged Acyclic durable state");
    } else {
        println!("preserved Acyclic durable state; pass --purge to remove it explicitly");
    }
    Ok(())
}

fn parse_uninstall_arguments(arguments: &[String]) -> Result<(&str, bool), String> {
    match arguments {
        [host] if !host.starts_with('-') => Ok((host, false)),
        [host, purge] if !host.starts_with('-') && purge == "--purge" => Ok((host, true)),
        _ => Err("usage: acyclic uninstall HOST [--purge]".to_owned()),
    }
}

fn parse_read_only_arguments(arguments: &[String]) -> Result<bool, String> {
    match arguments {
        [] => Ok(false),
        [json] if json == "--json" => Ok(true),
        _ => Err("usage: acyclic doctor [--json] | acyclic agents [--json]".to_owned()),
    }
}

async fn purge_durable_state(data: &Path) -> Result<(), String> {
    if !data.exists() {
        return Ok(());
    }
    let mut drain_fence = drain_service(data, None).await?;
    drain_fence.prepare_purge();
    let parent = data
        .parent()
        .ok_or_else(|| "durable state path has no parent".to_owned())?;
    remove_tree_checked(parent, data)
}

/// Takes the service lock when no service holds it, and clears the identity
/// the last service published. A service holds its lock for as long as it
/// runs, from before it opens its endpoint, so a free lock proves that none
/// is running or starting.
fn claim_stopped_service(
    data: &Path,
    expected_identity: Option<&str>,
) -> Result<Option<ServiceLock>, String> {
    let Some(lock) = acquire_service_lock(data)? else {
        return Ok(None);
    };
    if let Some(expected) = expected_identity {
        let marker = ServiceMarker::read(data).ok_or_else(|| {
            "cannot authenticate stale service: it published no identity".to_owned()
        })?;
        if marker.instance_id != expected {
            return Err("refusing to clean a replacement Acyclic service".to_owned());
        }
    }
    match fs::remove_file(ServiceMarker::path(data)) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(display(error)),
    }
    Ok(Some(lock))
}

/// Stops the running service, if any, through the contract every binary
/// understands (see [`ServiceMarker`]), and returns its lock. A service is
/// named by its instance ID; `expected_identity` refuses any other.
async fn drain_service(
    data: &Path,
    expected_identity: Option<&str>,
) -> Result<ServiceLock, String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    // A service publishes its marker once its endpoint opens, moments after
    // it takes the lock.
    let marker = loop {
        if let Some(lock) = claim_stopped_service(data, expected_identity)? {
            return Ok(lock);
        }
        if let Some(marker) = ServiceMarker::read(data) {
            break marker;
        }
        if std::time::Instant::now() >= deadline {
            return Err(
                "Acyclic service lock is held without a published identity; state was preserved"
                    .to_owned(),
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    if expected_identity.is_some_and(|expected| expected != marker.instance_id) {
        return Err("refusing to drain a replacement Acyclic service".to_owned());
    }
    let drain_id = uuid::Uuid::new_v4().to_string();
    request_service_stop(data, &marker.instance_id, &drain_id)?;
    for _ in 0..250 {
        if let Some(lock) = acquire_service_lock(data)? {
            verify_service_drain_completion(data, &marker.instance_id, &drain_id)?;
            return Ok(lock);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Err("Acyclic service did not drain; durable state and executable were preserved".to_owned())
}

async fn service_status(data: &Path) -> Result<Value, String> {
    let marker_identity = ServiceMarker::read(data).map(|marker| marker.instance_id);
    let ping = ControlRequest {
        version: 1,
        command: ControlCommand::Ping,
        cwd: env::current_dir().map_err(display)?,
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    };
    let reachable_identity = match send_control_request(data, &ping).await {
        Ok(response) => Some(
            response
                .get("instanceId")
                .or_else(|| response.get("identity"))
                .and_then(Value::as_str)
                .ok_or_else(|| "reachable service omitted its identity".to_owned())?
                .to_owned(),
        ),
        Err(ControlRequestError::Unavailable(_)) => None,
        Err(error) => return Err(format!("cannot inspect the Acyclic service: {error}")),
    };
    let lock = acquire_service_lock(data)?;
    let lock_acquirable = lock.is_some();
    drop(lock);
    Ok(json!({
        "version": 1,
        "markerIdentity": marker_identity,
        "reachableIdentity": reachable_identity,
        "lockAcquirable": lock_acquirable,
    }))
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
    let executable = current_executable()?;
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
        if ownership.added_marketplace && configured.is_some() {
            run_codex(&["plugin", "marketplace", "remove", "acyclic"])?;
        }
        // Restore configuration last. Every preceding step is idempotent, so
        // an interrupted uninstall can resume from the durable ownership record.
        restore_codex_config(&ownership)?;
        fs::remove_file(&ownership_path).map_err(display)?;
        println!("removed Acyclic-owned Codex integration and preserved prior state");
        return Ok(());
    }
    if host == "claude-code" {
        let user = home_directory()?.join(".claude/settings.json");
        let project = env::current_dir()
            .map_err(display)?
            .join(".claude/settings.json");
        let same_settings = same_existing_path(&user, &project);
        remove_claude_hooks(false)?;
        if !same_settings {
            remove_claude_hooks(true)?;
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

fn same_existing_path(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    left.canonicalize()
        .ok()
        .zip(right.canonicalize().ok())
        .is_some_and(|(left, right)| left == right)
}

fn remove_copilot_hooks_at(path: &Path) -> Result<(), String> {
    remove_owned_json(path, "copilot-hooks", "Copilot hooks")
}

fn install_claude_hooks(executable: &Path, project: bool) -> Result<PathBuf, String> {
    let path = if project {
        env::current_dir()
            .map_err(display)?
            .join(".claude/settings.json")
    } else {
        home_directory()?.join(".claude/settings.json")
    };
    install_owned_json_normalized(
        &path,
        "claude-hooks",
        "Claude Code hooks",
        normalize_claude_hook_document,
        |prior| {
            let mut document = prior
                .cloned()
                .unwrap_or_else(|| json!({}))
                .as_object()
                .cloned()
                .ok_or_else(|| format!("{} must contain a JSON object", path.display()))?;
            let hooks = object_entry(&mut document, "hooks")?;
            for event in [
                "SessionStart",
                "PreToolUse",
                "PostToolUse",
                "PostToolUseFailure",
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
                let mut group = serde_json::Map::new();
                if matches!(event, "PreToolUse" | "PostToolUse" | "PostToolUseFailure") {
                    group.insert("matcher".to_owned(), Value::String(".*".to_owned()));
                }
                group.insert(
                    "hooks".to_owned(),
                    json!([{
                        "type": "command",
                        "command": command,
                        "timeout": claude_hook_timeout(event),
                        "statusMessage": "Acyclic is routing the workspace"
                    }]),
                );
                entries.push(Value::Object(group));
            }
            Ok(Value::Object(document))
        },
    )?;
    Ok(path)
}

fn normalize_claude_hook_document(prior: Option<&Value>) -> Result<Option<Value>, String> {
    let Some(prior) = prior else {
        return Ok(None);
    };
    let mut document = prior
        .as_object()
        .cloned()
        .ok_or_else(|| "Claude Code settings must contain a JSON object".to_owned())?;
    let Some(hooks) = document.get_mut("hooks") else {
        return Ok(Some(Value::Object(document)));
    };
    let hooks = hooks
        .as_object_mut()
        .ok_or_else(|| "Claude Code settings hooks must contain a JSON object".to_owned())?;
    for event in [
        "SessionStart",
        "UserPromptSubmit",
        "PreToolUse",
        "PostToolUse",
        "PostToolUseFailure",
        "SubagentStart",
        "SubagentStop",
        "SessionEnd",
    ] {
        let Some(entries) = hooks.get_mut(event) else {
            continue;
        };
        let entries = entries
            .as_array_mut()
            .ok_or_else(|| format!("Claude Code hooks.{event} must be an array"))?;
        entries.retain(|entry| !is_acyclic_claude_hook_group(event, entry));
        if entries.is_empty() {
            hooks.remove(event);
        }
    }
    if hooks.is_empty() {
        document.remove("hooks");
    }
    Ok(Some(Value::Object(document)))
}

fn is_acyclic_claude_hook_group(event: &str, group: &Value) -> bool {
    let Some(group) = group.as_object() else {
        return false;
    };
    let expected_fields = if matches!(event, "PreToolUse" | "PostToolUse" | "PostToolUseFailure") {
        2
    } else {
        1
    };
    if group.len() != expected_fields
        || expected_fields == 2 && group.get("matcher").and_then(Value::as_str) != Some(".*")
    {
        return false;
    }
    let Some(hook) = group
        .get("hooks")
        .and_then(Value::as_array)
        .filter(|hooks| hooks.len() == 1)
        .and_then(|hooks| hooks.first())
        .and_then(Value::as_object)
    else {
        return false;
    };
    let timeout = hook.get("timeout").and_then(Value::as_u64);
    let owned_timeout = timeout == Some(claude_hook_timeout(event))
        || event == "SessionEnd" && timeout == Some(120);
    if hook.len() != 4
        || hook.get("type").and_then(Value::as_str) != Some("command")
        || !owned_timeout
        || hook.get("statusMessage").and_then(Value::as_str)
            != Some("Acyclic is routing the workspace")
    {
        return false;
    }
    let suffix = format!(" __hook claude-code {event}");
    let Some(executable) = hook
        .get("command")
        .and_then(Value::as_str)
        .and_then(|command| command.strip_suffix(&suffix))
    else {
        return false;
    };
    Path::new(executable.trim_matches(['\'', '"']))
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.eq_ignore_ascii_case("acyclic") || name.eq_ignore_ascii_case("acyclic.exe")
        })
}

fn claude_hook_timeout(event: &str) -> u64 {
    // Claude bounds SessionEnd separately from ordinary hooks. Keep teardown
    // within its terminal-hook budget so the process cannot exit while mounts
    // and service session ownership remain live.
    if event == "SessionEnd" { 3 } else { 120 }
}

fn remove_claude_hooks(project: bool) -> Result<(), String> {
    let path = if project {
        env::current_dir()
            .map_err(display)?
            .join(".claude/settings.json")
    } else {
        home_directory()?.join(".claude/settings.json")
    };
    remove_owned_json(&path, "claude-hooks", "Claude Code hooks")
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
    install_owned_json(path, "copilot-hooks", "Copilot hooks", |_| Ok(installed))
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
    install_owned_json(&path, "cursor-hooks", "Cursor hooks", |prior| {
        let mut document = prior
            .cloned()
            .unwrap_or_else(|| json!({}))
            .as_object()
            .cloned()
            .ok_or_else(|| format!("{} must contain a JSON object", path.display()))?;
        document.insert("version".to_owned(), json!(1));
        let hooks = object_entry(&mut document, "hooks")?;
        for event in ["sessionStart", "sessionEnd"] {
            let entries = hooks
                .entry(event.to_owned())
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .ok_or_else(|| format!("Cursor hooks.{event} must be an array"))?;
            entries.push(json!({
                "command": format!("{} __hook cursor {event}", shell_command_path(executable)),
                "timeout": 120
            }));
        }
        Ok(Value::Object(document))
    })?;
    Ok(path)
}

fn remove_cursor_hooks(project: bool) -> Result<(), String> {
    let path = cursor_hooks_path(project)?;
    remove_owned_json(&path, "cursor-hooks", "Cursor hooks")
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
    config_path: Option<PathBuf>,
    prior_default_permissions: Option<String>,
}

struct CodexConfigPlan {
    path: PathBuf,
    prior_default_permissions: Option<String>,
    installed: String,
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

fn codex_config_path() -> Result<PathBuf, String> {
    Ok(env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or(home_directory()?.join(".codex"))
        .join("config.toml"))
}

fn codex_plugin_configuration_matches(
    ownership: &CodexPluginOwnership,
    packaged_root: &Path,
) -> bool {
    let Some(config_path) = ownership.config_path.as_ref() else {
        return false;
    };
    let Ok(document) = fs::read_to_string(config_path)
        .map_err(display)
        .and_then(|text| text.parse::<toml_edit::DocumentMut>().map_err(display))
    else {
        return false;
    };
    let marketplace = document
        .get("marketplaces")
        .and_then(toml_edit::Item::as_table)
        .and_then(|marketplaces| marketplaces.get("acyclic"))
        .and_then(toml_edit::Item::as_table);
    let configured_root = marketplace
        .and_then(|marketplace| marketplace.get("source"))
        .and_then(toml_edit::Item::as_str)
        .map(Path::new);
    let source_is_local = marketplace
        .and_then(|marketplace| marketplace.get("source_type"))
        .and_then(toml_edit::Item::as_str)
        == Some("local");
    let enabled = document
        .get("plugins")
        .and_then(toml_edit::Item::as_table)
        .and_then(|plugins| plugins.get("acyclic@acyclic"))
        .and_then(toml_edit::Item::as_table)
        .and_then(|plugin| plugin.get("enabled"))
        .and_then(toml_edit::Item::as_bool)
        == Some(true);
    source_is_local
        && enabled
        && configured_root
            .and_then(|configured| configured.canonicalize().ok())
            .zip(packaged_root.canonicalize().ok())
            .is_some_and(|(configured, packaged)| configured == packaged)
}

fn read_optional_text(path: &Path) -> Result<Option<String>, String> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

fn toml_table<'a>(
    parent: &'a mut toml_edit::Table,
    key: &str,
) -> Result<&'a mut toml_edit::Table, String> {
    parent
        .entry(key)
        .or_insert_with(|| toml_edit::Item::Table(toml_edit::Table::new()))
        .as_table_mut()
        .ok_or_else(|| format!("Codex configuration field '{key}' must be a table"))
}

fn codex_permission_profile(data: &Path, base: &str) -> Result<toml_edit::Table, String> {
    let mut profile = toml_edit::Table::new();
    profile.insert("extends", toml_edit::value(base));
    let roots = toml_table(&mut profile, "workspace_roots")?;
    roots.insert(
        data.join("w").to_string_lossy().as_ref(),
        toml_edit::value(true),
    );
    #[cfg(unix)]
    {
        let runtime = unix_control_runtime_directory();
        roots.insert(runtime.to_string_lossy().as_ref(), toml_edit::value(true));
    }
    #[cfg(windows)]
    {
        let filesystem = toml_table(&mut profile, "filesystem")?;
        filesystem.insert(":minimal", toml_edit::value("read"));
        toml_table(filesystem, ":workspace_roots")?.insert(".", toml_edit::value("write"));
    }
    Ok(profile)
}

fn codex_profile_matches(profile: &toml_edit::Table, data: &Path, base_profile: &str) -> bool {
    let roots = profile
        .get("workspace_roots")
        .and_then(toml_edit::Item::as_table);
    #[cfg(unix)]
    {
        let runtime = unix_control_runtime_directory();
        profile.len() == 2
            && profile.get("extends").and_then(toml_edit::Item::as_str) == Some(base_profile)
            && roots.is_some_and(|roots| {
                roots.len() == 2
                    && roots
                        .get(data.join("w").to_string_lossy().as_ref())
                        .and_then(toml_edit::Item::as_bool)
                        == Some(true)
                    && roots
                        .get(runtime.to_string_lossy().as_ref())
                        .and_then(toml_edit::Item::as_bool)
                        == Some(true)
            })
    }
    #[cfg(not(unix))]
    {
        let filesystem = profile
            .get("filesystem")
            .and_then(toml_edit::Item::as_table);
        profile.len() == 3
            && profile.get("extends").and_then(toml_edit::Item::as_str) == Some(base_profile)
            && roots.is_some_and(|roots| {
                roots.len() == 1
                    && roots
                        .get(data.join("w").to_string_lossy().as_ref())
                        .and_then(toml_edit::Item::as_bool)
                        == Some(true)
            })
            && filesystem.is_some_and(|filesystem| {
                filesystem.len() == 2
                    && filesystem.get(":minimal").and_then(toml_edit::Item::as_str) == Some("read")
                    && filesystem
                        .get(":workspace_roots")
                        .and_then(toml_edit::Item::as_table)
                        .is_some_and(|roots| {
                            roots.len() == 1
                                && roots.get(".").and_then(toml_edit::Item::as_str) == Some("write")
                        })
            })
    }
}

fn plan_codex_config(ownership: &CodexPluginOwnership) -> Result<CodexConfigPlan, String> {
    let path = codex_config_path()?;
    let mut document = read_optional_text(&path)?
        .as_deref()
        .unwrap_or("")
        .parse::<toml_edit::DocumentMut>()
        .map_err(|error| format!("invalid Codex configuration at {}: {error}", path.display()))?;
    let root = document.as_table_mut();
    let base_profile = ownership
        .prior_default_permissions
        .as_deref()
        .unwrap_or(":workspace");
    if let Some(owned_path) = ownership.config_path.as_ref() {
        let installed_profile = root
            .get("permissions")
            .and_then(toml_edit::Item::as_table)
            .and_then(|permissions| permissions.get("acyclic"))
            .and_then(toml_edit::Item::as_table);
        let owned = owned_path == &path
            && root
                .get("default_permissions")
                .and_then(toml_edit::Item::as_str)
                == Some("acyclic")
            && installed_profile.is_some_and(|profile| {
                codex_profile_matches(profile, &default_data_directory(), base_profile)
            });
        if !owned {
            return Err(format!(
                "Codex configuration ownership at {} is inconsistent; refusing to overwrite it",
                path.display()
            ));
        }
        return Ok(CodexConfigPlan {
            path,
            prior_default_permissions: ownership.prior_default_permissions.clone(),
            installed: document.to_string(),
        });
    }
    if ownership.prior_default_permissions.is_some() {
        return Err("Codex configuration ownership record is incomplete".to_owned());
    }
    let prior_default_permissions = root
        .get("default_permissions")
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| "Codex default_permissions must be a string".to_owned())
        })
        .transpose()?;
    let expected_profile = codex_permission_profile(
        &default_data_directory(),
        prior_default_permissions.as_deref().unwrap_or(":workspace"),
    )?;
    if toml_table(root, "permissions")?.contains_key("acyclic") {
        return Err(format!(
            "refusing to replace the unowned 'acyclic' permission profile in {}",
            path.display()
        ));
    }
    root.insert("default_permissions", toml_edit::value("acyclic"));
    toml_table(root, "permissions")?.insert("acyclic", toml_edit::Item::Table(expected_profile));
    Ok(CodexConfigPlan {
        path,
        prior_default_permissions,
        installed: document.to_string(),
    })
}

fn apply_codex_config(plan: &CodexConfigPlan) -> Result<(), String> {
    write_text_with_backup(&plan.path, &plan.installed)
}

fn restore_codex_config(ownership: &CodexPluginOwnership) -> Result<(), String> {
    let Some(path) = ownership.config_path.as_ref() else {
        return Ok(());
    };
    let mut document = read_optional_text(path)?
        .as_deref()
        .unwrap_or("")
        .parse::<toml_edit::DocumentMut>()
        .map_err(|error| format!("invalid Codex configuration at {}: {error}", path.display()))?;
    let root = document.as_table_mut();
    let base_profile = ownership
        .prior_default_permissions
        .as_deref()
        .unwrap_or(":workspace");
    let owned = root
        .get("default_permissions")
        .and_then(toml_edit::Item::as_str)
        == Some("acyclic")
        && root
            .get("permissions")
            .and_then(toml_edit::Item::as_table)
            .and_then(|permissions| permissions.get("acyclic"))
            .and_then(toml_edit::Item::as_table)
            .is_some_and(|profile| {
                codex_profile_matches(profile, &default_data_directory(), base_profile)
            });
    if !owned {
        let restored_default = match ownership.prior_default_permissions.as_deref() {
            Some(prior) => {
                root.get("default_permissions")
                    .and_then(toml_edit::Item::as_str)
                    == Some(prior)
            }
            None => !root.contains_key("default_permissions"),
        };
        let profile_absent = root
            .get("permissions")
            .and_then(toml_edit::Item::as_table)
            .is_none_or(|permissions| !permissions.contains_key("acyclic"));
        if restored_default && profile_absent {
            return Ok(());
        }
        return Err(format!(
            "the Acyclic Codex permission profile at {} was modified; leaving it untouched",
            path.display()
        ));
    }
    match ownership.prior_default_permissions.as_deref() {
        Some(prior) => {
            root.insert("default_permissions", toml_edit::value(prior));
        }
        None => {
            root.remove("default_permissions");
        }
    }
    let permissions = toml_table(root, "permissions")?;
    permissions.remove("acyclic");
    write_text_with_backup(path, &document.to_string())
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

fn read_optional_json(path: &Path, label: &str) -> Result<Option<Value>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            format!(
                "refusing to overwrite invalid {label} at {}: {error}",
                path.display()
            )
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

fn install_owned_json(
    path: &Path,
    section: &str,
    label: &str,
    build: impl FnOnce(Option<&Value>) -> Result<Value, String>,
) -> Result<(), String> {
    install_owned_json_normalized(path, section, label, |prior| Ok(prior.cloned()), build)
}

fn install_owned_json_normalized(
    path: &Path,
    section: &str,
    label: &str,
    normalize: impl FnOnce(Option<&Value>) -> Result<Option<Value>, String>,
    build: impl FnOnce(Option<&Value>) -> Result<Value, String>,
) -> Result<(), String> {
    let ownership_path = mcp_ownership_path(path, section);
    let current = read_optional_json(path, label)?;
    let raw_prior = match read_mcp_ownership(&ownership_path)? {
        Some(ownership)
            if ownership.version == 1
                && ownership.config_path == path
                && ownership.section == section
                && (current == ownership.prior
                    || current.as_ref() == Some(&ownership.installed)) =>
        {
            ownership.prior
        }
        Some(_) => {
            return Err(format!(
                "{label} ownership at {} is inconsistent; refusing to overwrite it",
                path.display()
            ));
        }
        None => current,
    };
    let prior = normalize(raw_prior.as_ref())?;
    let installed = build(prior.as_ref())?;
    write_mcp_ownership(
        &ownership_path,
        &McpConfigOwnership {
            version: 1,
            config_path: path.to_path_buf(),
            section: section.to_owned(),
            prior,
            installed: installed.clone(),
        },
    )?;
    write_json_with_backup(path, &installed)
}

fn remove_owned_json(path: &Path, section: &str, label: &str) -> Result<(), String> {
    let ownership_path = mcp_ownership_path(path, section);
    let current = read_optional_json(path, label)?;
    let Some(ownership) = read_mcp_ownership(&ownership_path)? else {
        if current.is_some() {
            return Err(format!(
                "refusing to remove {label} without an Acyclic ownership record from {}",
                path.display()
            ));
        }
        return Ok(());
    };
    if ownership.version != 1
        || ownership.config_path != path
        || ownership.section != section
        || (current != ownership.prior && current.as_ref() != Some(&ownership.installed))
    {
        return Err(format!(
            "the Acyclic {label} at {} were modified; leaving them untouched",
            path.display()
        ));
    }
    if current != ownership.prior {
        match &ownership.prior {
            Some(prior) => write_json_with_backup(path, prior)?,
            None => match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(display(error)),
            },
        }
    }
    fs::remove_file(&ownership_path).map_err(display)
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
    let mut ownership = if let Some(ownership) = existing_ownership.as_ref() {
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
            config_path: ownership.config_path.clone(),
            prior_default_permissions: ownership.prior_default_permissions.clone(),
        }
    } else {
        CodexPluginOwnership {
            version: 1,
            marketplace_root: manifest_root.clone(),
            added_marketplace,
            plugin_was_installed,
            config_path: None,
            prior_default_permissions: None,
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
        let plan = plan_codex_config(&ownership)?;
        ownership.config_path = Some(plan.path.clone());
        ownership
            .prior_default_permissions
            .clone_from(&plan.prior_default_permissions);
        write_codex_ownership(&ownership_path, &ownership)?;
        apply_codex_config(&plan)?;
        Ok(())
    })();
    if let Err(error) = install {
        let _ = restore_codex_config(&ownership);
        if !plugin_was_installed {
            let _ = run_codex(&["plugin", "remove", "acyclic@acyclic", "--json"]);
        }
        if added_marketplace {
            let _ = run_codex(&["plugin", "marketplace", "remove", "acyclic"]);
        }
        match existing_ownership.as_ref() {
            Some(existing) => {
                let _ = write_codex_ownership(&ownership_path, existing);
            }
            None => {
                let _ = fs::remove_file(&ownership_path);
            }
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
    let executable = current_executable()?;
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
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_in_result
)]
mod tests {
    use super::*;
    use acyclic_fs::GitCommand;
    use acyclic_fs::kernel::NameEncoding;

    fn root_repository_workspace_id(control: &ControlPlane) -> [u8; 16] {
        control.state.roots[&root_key(WorkspaceRootId::from_bytes(control.state.root_id))]
            .repository_workspace_id
    }

    async fn active_root_workspace_id(control: &ControlPlane) -> acyclic_fs::WorkspaceId {
        let root_id = WorkspaceRootId::from_bytes(control.state.root_id);
        control
            .distributed
            .contexts()
            .resolve(WorkspaceContextId::from_bytes(
                control.state.root_context_id,
            ))
            .await
            .expect("root context")
            .roots[&root_id]
            .workspace_id
    }

    async fn root_has_file_contents(
        control: &ControlPlane,
        portable_name: &str,
        expected: &[u8],
    ) -> bool {
        let Ok(workspace) = control
            .distributed
            .workspace(active_root_workspace_id(control).await)
            .await
        else {
            return false;
        };
        let Ok(head) = workspace.head().await else {
            return false;
        };
        let Ok(page) = head.list_directory("/", None, 1024).await else {
            return false;
        };
        let limits = VolumeLimits::default();
        let Some(path) = page.entries.into_iter().find_map(|entry| {
            let path = NamespacePath::new(vec![entry.name], limits).ok()?;
            (namespace_path_text(&path).ok()?.trim_start_matches('/') == portable_name)
                .then_some(path)
        }) else {
            return false;
        };
        let Ok(lookup) = head
            .lookup_paths(&[path], WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
        else {
            return false;
        };
        matches!(
            lookup.value.as_slice(),
            [Some(acyclic_fs::kernel::FileRecord {
                payload: acyclic_fs::kernel::FilePayload::InlineRegular(data),
                ..
            })] if data.as_bytes() == expected
        )
    }

    #[test]
    fn native_posix_names_are_presented_when_they_are_valid_utf8() {
        use acyclic_fs::kernel::LogicalName;

        let limits = VolumeLimits::default();
        let readable = LogicalName::new(
            NameEncoding::PosixBytes,
            b"shared.txt".to_vec(),
            limits.maximum_component_bytes,
        )
        .expect("valid POSIX name");
        let invalid = LogicalName::new(
            NameEncoding::PosixBytes,
            vec![0xff],
            limits.maximum_component_bytes,
        )
        .expect("valid non-UTF-8 POSIX name");
        assert_eq!(
            namespace_path_text(&NamespacePath::new(vec![readable], limits).expect("path"))
                .expect("readable native name"),
            "/shared.txt"
        );
        assert!(
            namespace_path_text(&NamespacePath::new(vec![invalid], limits).expect("path")).is_err()
        );
    }

    fn route_path(route: &Route) -> PathBuf {
        route.active_path().expect("route active path")
    }

    #[test]
    #[ignore = "invoked as a separate process by mount lifecycle tests"]
    fn projected_mount_writer_child() {
        let Some(path) = std::env::var_os("ACYCLIC_TEST_PROJECTED_WRITE_PATH") else {
            return;
        };
        fs::write(PathBuf::from(path), b"late parent").expect("external writer completes");
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "taking ownership keeps temporary json! values ergonomic at every fixture call site"
    )]
    fn native_hook_request(
        host: &str,
        event: &str,
        session_id: &str,
        cwd: &Path,
        extra: Value,
    ) -> ControlRequest {
        let mut arguments = extra.as_object().cloned().unwrap_or_default();
        arguments.insert("session_id".to_owned(), json!(session_id));
        arguments.insert("cwd".to_owned(), json!(cwd));
        ControlRequest {
            version: 1,
            command: ControlCommand::Hook,
            cwd: cwd.to_path_buf(),
            argv: Vec::new(),
            name: format!("{host}:{event}"),
            arguments: Value::Object(arguments),
        }
    }

    #[test]
    fn a_session_ending_after_an_unflushed_save_keeps_it_in_its_terminal_state() {
        run_large_stack("terminal-save-covers-unflushed", terminal_save_case);
    }

    async fn terminal_save_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        let data = temporary.path().join("state");
        fs::create_dir_all(&root).expect("root");
        let service = ConcurrentServiceControl::open(data.clone())
            .await
            .expect("service");
        for (event, extra) in [
            ("SessionStart", json!({})),
            // Saved unflushed, the last effect of its request.
            ("UserPromptSubmit", json!({"turn_id":"last-turn"})),
            // Nothing is mounted or leased, so only its terminal save flushes.
            ("SessionEnd", json!({})),
        ] {
            service
                .dispatch_request(native_hook_request("codex", event, "session", &root, extra))
                .await
                .expect(event);
        }
        service.shutdown().await.expect("service shutdown");
        let directory = data
            .join("sessions")
            .join(blake3::hash(b"session").to_hex().as_str());
        let (slots, [_, _, unflushed]) = StateSlots::read(&directory).expect("slots");
        let state = load_saved_state(&directory).expect("terminal session state");
        assert!(!state.active);
        assert!(state.root_turns.contains("last-turn"));
        // The terminal save is the newest flushed save; the unflushed slot
        // holds an older generation and is never loaded again.
        assert!(unflushed.generation() < slots.flushed.into_iter().max().flatten());
    }

    #[test]
    fn concurrent_service_isolates_sessions_and_fences_reused_ids() {
        run_large_stack("concurrent-service", concurrent_service_case);
    }

    async fn concurrent_service_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root_a = temporary.path().join("root-a");
        let root_b = temporary.path().join("root-b");
        fs::create_dir_all(&root_a).expect("root a");
        fs::create_dir_all(&root_b).expect("root b");
        let service = ConcurrentServiceControl::open(temporary.path().join("state"))
            .await
            .expect("service");
        for (session, root) in [("a", &root_a), ("b", &root_b)] {
            service
                .dispatch_request(native_hook_request(
                    "claude-code",
                    "SessionStart",
                    session,
                    root,
                    json!({}),
                ))
                .await
                .expect("session start");
        }

        let handle_a = service.session("a").await.expect("session a");
        let resume_a = handle_a.pause().await;
        let request_b = ControlRequest {
            version: 1,
            command: ControlCommand::Agents,
            cwd: root_b.clone(),
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        };
        tokio::time::timeout(
            std::time::Duration::from_millis(250),
            service.dispatch_request(request_b),
        )
        .await
        .expect("session b must not queue behind session a")
        .expect("session b request");
        let _ = resume_a.send(());

        let first_epoch = handle_a.key.epoch;
        service.end_session("a", true).await.expect("end a");
        service
            .dispatch_request(native_hook_request(
                "claude-code",
                "SessionStart",
                "a",
                &root_a,
                json!({}),
            ))
            .await
            .expect("resume a");
        let resumed = service.session("a").await.expect("resumed a");
        assert!(resumed.key.epoch > first_epoch);
        assert!(!handle_a.is_active());

        service.drain_sessions(true).await.expect("service drain");
    }

    #[test]
    fn accepted_session_operation_survives_waiter_cancellation() {
        run_large_stack(
            "accepted-session-operation",
            accepted_session_operation_case,
        );
    }

    async fn accepted_session_operation_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root");
        let service = Arc::new(
            ConcurrentServiceControl::open(temporary.path().join("state"))
                .await
                .expect("service"),
        );
        service
            .dispatch_request(native_hook_request(
                "codex",
                "SessionStart",
                "session",
                &root,
                json!({}),
            ))
            .await
            .expect("session start");
        let handle = service.session("session").await.expect("session");
        let resume = handle.pause().await;
        let service_for_request = Arc::clone(&service);
        let root_for_request = root.clone();
        let waiter = tokio::spawn(async move {
            service_for_request
                .dispatch_request(native_hook_request(
                    "codex",
                    "UserPromptSubmit",
                    "session",
                    &root_for_request,
                    json!({"turn_id":"durable-turn"}),
                ))
                .await
        });
        tokio::task::yield_now().await;
        waiter.abort();
        let _ = resume.send(());
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if handle
                    .snapshot()
                    .expect("snapshot")
                    .is_some_and(|state| state.root_turns.contains("durable-turn"))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("accepted operation must complete after waiter cancellation");
        service.drain_sessions(true).await.expect("service drain");
    }

    #[test]
    fn a_stop_request_drains_without_waiting_for_a_busy_session() {
        run_large_stack("stop-before-drain", stop_before_drain_case);
    }

    async fn stop_before_drain_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        let data = temporary.path().join("state");
        fs::create_dir_all(&root).expect("root");
        let service = ConcurrentServiceControl::open(data.clone())
            .await
            .expect("service");
        service
            .dispatch_request(native_hook_request(
                "codex",
                "SessionStart",
                "session",
                &root,
                json!({}),
            ))
            .await
            .expect("session start");
        let handle = service.session("session").await.expect("session");
        let resume = handle.pause().await;
        service.begin_drain();
        assert_eq!(
            service.lifecycle.load(std::sync::atomic::Ordering::Acquire),
            SERVICE_DRAINING
        );
        let drain = tokio::spawn(async move { service.shutdown().await });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished(), "paused actor must delay actual drain");
        resume.send(()).expect("resume session actor");
        tokio::time::timeout(std::time::Duration::from_secs(10), drain)
            .await
            .expect("drain completes")
            .expect("drain task")
            .expect("service shutdown");
        let state = load_saved_state(
            &data
                .join("sessions")
                .join(blake3::hash(b"session").to_hex().as_str()),
        )
        .expect("terminal session state");
        assert!(!state.active, "explicit shutdown must deactivate sessions");
    }

    #[test]
    fn session_close_is_fifo_idempotent_and_durable() {
        run_large_stack("session-close-order", session_close_order_case);
    }

    #[test]
    fn saturated_session_data_queue_reserves_shutdown_capacity() {
        run_large_stack("session-saturated-close", session_saturated_close_case);
    }

    async fn session_saturated_close_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root");
        let service = Arc::new(
            ConcurrentServiceControl::open(temporary.path().join("state"))
                .await
                .expect("service"),
        );
        service
            .dispatch_request(native_hook_request(
                "codex",
                "SessionStart",
                "session",
                &root,
                json!({}),
            ))
            .await
            .expect("session start");
        let handle = service.session("session").await.expect("session");
        let resume = handle.pause().await;
        let mut operations = Vec::new();
        for _ in 0..SESSION_DATA_CAPACITY {
            let handle = Arc::clone(&handle);
            let root = root.clone();
            operations.push(tokio::spawn(async move {
                handle
                    .dispatch(ControlRequest {
                        version: 1,
                        command: ControlCommand::Ping,
                        cwd: root,
                        argv: Vec::new(),
                        name: String::new(),
                        arguments: Value::Null,
                    })
                    .await
            }));
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while handle.data_slots.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("data queue saturation");
        let overflow = handle
            .dispatch(ControlRequest {
                version: 1,
                command: ControlCommand::Ping,
                cwd: root,
                argv: Vec::new(),
                name: String::new(),
                arguments: Value::Null,
            })
            .await;
        assert_eq!(
            overflow.expect_err("overflow must fail"),
            "Acyclic session operation queue is full"
        );
        let close_service = Arc::clone(&service);
        let close = tokio::spawn(async move { close_service.end_session("session", true).await });
        tokio::task::yield_now().await;
        assert_eq!(
            handle.state.load(std::sync::atomic::Ordering::Acquire),
            SESSION_CLOSING
        );
        let _ = resume.send(());
        for operation in operations {
            operation.await.expect("operation task").expect("operation");
        }
        close.await.expect("close task").expect("close");
        assert_eq!(
            handle.state.load(std::sync::atomic::Ordering::Acquire),
            SESSION_CLOSED
        );
    }

    async fn session_close_order_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root");
        let service = Arc::new(
            ConcurrentServiceControl::open(temporary.path().join("state"))
                .await
                .expect("service"),
        );
        service
            .dispatch_request(native_hook_request(
                "codex",
                "SessionStart",
                "session",
                &root,
                json!({}),
            ))
            .await
            .expect("session start");
        let handle = service.session("session").await.expect("session");
        let resume = handle.pause().await;

        let operation_service = Arc::clone(&service);
        let operation_root = root.clone();
        let operation = tokio::spawn(async move {
            operation_service
                .dispatch_request(native_hook_request(
                    "codex",
                    "UserPromptSubmit",
                    "session",
                    &operation_root,
                    json!({"turn_id":"before-close"}),
                ))
                .await
        });
        tokio::task::yield_now().await;
        let close_service = Arc::clone(&service);
        let close = tokio::spawn(async move { close_service.end_session("session", true).await });
        tokio::task::yield_now().await;
        let duplicate_service = Arc::clone(&service);
        let duplicate =
            tokio::spawn(async move { duplicate_service.end_session("session", true).await });
        let _ = resume.send(());

        operation.await.expect("operation task").expect("operation");
        close.await.expect("close task").expect("close");
        duplicate
            .await
            .expect("duplicate task")
            .expect("duplicate close");
        service
            .end_session("session", true)
            .await
            .expect("late duplicate close");

        let state = load_saved_state(
            &service
                .data
                .join("sessions")
                .join(blake3::hash(b"session").to_hex().as_str()),
        )
        .expect("terminal state");
        assert!(!state.active);
        assert!(state.root_turns.contains("before-close"));
    }

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
    fn nested_roots_route_to_the_deepest_unambiguous_session() {
        run_large_stack("nested-root-routing", nested_root_routing_case);
    }

    async fn nested_root_routing_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let parent = temporary.path().join("root");
        let nested = parent.join("nested");
        let nested_child = nested.join("child");
        fs::create_dir_all(&nested_child).expect("nested roots");
        let mut service = ServiceControl::open(temporary.path().join("state"))
            .await
            .expect("service");
        for (session_id, cwd) in [("parent", &parent), ("nested", &nested)] {
            service
                .dispatch_native_hook(
                    "claude-code",
                    "SessionStart",
                    json!({"session_id":session_id,"cwd":cwd}),
                    cwd,
                )
                .await
                .expect("register root");
        }
        assert_eq!(
            service.session_for_cwd(&parent).expect("parent route"),
            Some("parent".to_owned())
        );
        assert_eq!(
            service
                .session_for_cwd(&nested_child)
                .expect("deepest route"),
            Some("nested".to_owned())
        );

        service
            .dispatch_native_hook(
                "claude-code",
                "SessionStart",
                json!({"session_id":"ambiguous","cwd":nested}),
                &nested,
            )
            .await
            .expect("register identical nested root");
        let error = service
            .session_for_cwd(&nested_child)
            .expect_err("equal-depth roots must fail closed");
        assert!(error.contains("multiple Acyclic sessions"), "{error}");
        service.shutdown().await.expect("shutdown");
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

    #[test]
    fn shared_root_observations_publish_monotonic_source_epochs() {
        run_large_stack("shared-root-observations", shared_root_observations_case);
    }

    async fn shared_root_observations_case() {
        use acyclic_fs::{WatchEpoch, WatchInvalidationReason, WatchSequence};
        use std::sync::mpsc;

        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root");
        let shared = SharedRootRegistry::default()
            .acquire(&root, SharedRootAdmission::Fresh)
            .await
            .expect("shared physical root");
        let (first_entered_tx, first_entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let first_root = Arc::clone(&shared);
        let first = std::thread::spawn(move || {
            first_root.observe_with(|| {
                first_entered_tx
                    .send(())
                    .expect("first observation entered");
                release_rx.recv().expect("release first observation");
                Ok(WatchBatch::RescanRequired {
                    epoch: WatchEpoch::from_u64(1),
                    reason: WatchInvalidationReason::NativeRescanRequired,
                })
            })
        });
        first_entered_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("first observation reached watcher poll");
        let (second_started_tx, second_started_rx) = mpsc::channel();
        let (second_entered_tx, second_entered_rx) = mpsc::channel();
        let second_root = Arc::clone(&shared);
        let second = std::thread::spawn(move || {
            second_started_tx.send(()).expect("second thread started");
            second_root.observe_with(|| {
                second_entered_tx
                    .send(())
                    .expect("second observation entered");
                Ok(WatchBatch::Changes {
                    epoch: WatchEpoch::from_u64(1),
                    first_sequence: WatchSequence::from_u64(1),
                    next_sequence: WatchSequence::from_u64(1),
                    changes: Vec::new(),
                })
            })
        });
        second_started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("second observer started");
        assert!(
            second_entered_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "second observation must wait for the first publication"
        );
        release_tx.send(()).expect("release first observation");
        let first = first
            .join()
            .expect("first thread")
            .expect("first observation");
        let second = second
            .join()
            .expect("second thread")
            .expect("second observation");
        assert_eq!(second.prior_source.epoch, first.source.epoch);
        assert_eq!(second.source.epoch, first.source.epoch);
        assert_eq!(
            shared.reference.lock().expect("published reference").epoch,
            second.source.epoch
        );
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
        assert!(
            error.contains("identity") && error.contains("changed"),
            "{error}"
        );
        drop(held);
        registry.prune().await;
        let replacement = registry
            .acquire(&root, SharedRootAdmission::Fresh)
            .await
            .expect("released registration must not pin the old root identity");
        assert_eq!(registry.live_roots().await, 1);
        drop(replacement);
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
        let first_context = service.sessions["a"]
            .distributed
            .contexts()
            .resolve(WorkspaceContextId::from_bytes(
                service.sessions["a"].state.root_context_id,
            ))
            .await
            .expect("first root context");
        let second_context = service.sessions["same-root"]
            .distributed
            .contexts()
            .resolve(WorkspaceContextId::from_bytes(
                service.sessions["same-root"].state.root_context_id,
            ))
            .await
            .expect("second root context");
        assert_ne!(
            first_context
                .roots
                .values()
                .next()
                .expect("first root")
                .workspace_name,
            second_context
                .roots
                .values()
                .next()
                .expect("second root")
                .workspace_name
        );
        assert_ne!(
            root_repository_workspace_id(&service.sessions["a"]),
            root_repository_workspace_id(&service.sessions["same-root"])
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

        let outside = temporary.path().join("outside");
        fs::create_dir(&outside).expect("outside root");
        let error = service
            .dispatch_request(ControlRequest {
                version: 1,
                command: ControlCommand::Agents,
                cwd: outside,
                argv: Vec::new(),
                name: String::new(),
                arguments: Value::Null,
            })
            .await
            .expect_err("an active service must reject an unregistered root");
        assert!(error.contains("outside every registered"), "{error}");
        assert_eq!(service.sessions.len(), 3);
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
            route_path(&control.state.routes["child"])
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
        let error = service
            .dispatch_request(ControlRequest {
                version: 1,
                command: ControlCommand::Agents,
                cwd: child_path.clone(),
                argv: Vec::new(),
                name: String::new(),
                arguments: cli_routing(root_a.clone()),
            })
            .await
            .expect_err("a child must not select its parent with -C");
        assert!(error.contains("authority boundaries"), "{error}");
        let error = service
            .dispatch_request(ControlRequest {
                version: 1,
                command: ControlCommand::Agents,
                cwd: root_a.clone(),
                argv: Vec::new(),
                name: String::new(),
                arguments: cli_routing(child_path.clone()),
            })
            .await
            .expect_err("a root must inspect descendants through agent refs, not -C");
        assert!(
            error.contains("authority boundaries") || error.contains("multiple Acyclic sessions"),
            "{error}"
        );
        service
            .dispatch_request(ControlRequest {
                version: 1,
                command: ControlCommand::Agents,
                cwd: child_path.clone(),
                argv: Vec::new(),
                name: String::new(),
                arguments: cli_routing(child_path.clone()),
            })
            .await
            .expect("a child may select its own mounted context");
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
        service.sessions["a"].physical_roots[&root_a_key]
            .source
            .inner()
            .invalidate();
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
                if root_has_file_contents(
                    &service.sessions[session_id],
                    "shared.txt",
                    b"shared update",
                )
                .await
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
        service.sessions["a"].physical_roots[&root_a_key]
            .source
            .inner()
            .invalidate();
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
            if root_has_file_contents(
                &service.sessions["same-root"],
                "recovered.txt",
                b"recovered update",
            )
            .await
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
        let shared_roots = service.shared_roots.clone();
        service.shutdown().await.expect("shutdown");
        assert_eq!(shared_roots.live_roots().await, 0);

        let resumed = ServiceControl::open(state).await.expect("resume service");
        assert_eq!(resumed.sessions.len(), 2);
        assert!(!resumed.sessions.contains_key("a"));
        assert_eq!(resumed.shared_roots.live_roots().await, 2);
        resumed.shutdown().await.expect("resumed shutdown");
    }

    #[test]
    fn sequential_same_root_sessions_reuse_the_durable_source_identity() {
        run_large_stack("sequential-same-root", sequential_same_root_case);
    }

    async fn sequential_same_root_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        let state = temporary.path().join("state");
        fs::create_dir(&root).expect("root");
        let mut service = ServiceControl::open(state.clone()).await.expect("service");
        service
            .dispatch_native_hook(
                "codex",
                "SessionStart",
                json!({"session_id":"first","cwd":root}),
                &root,
            )
            .await
            .expect("first session");
        let first_state = &service.sessions["first"].state;
        let first_identity = first_state.roots[&hex::encode(first_state.root_id)].source_identity;
        service
            .dispatch_native_hook("codex", "SessionEnd", json!({"session_id":"first"}), &root)
            .await
            .expect("first session end");
        assert_eq!(service.shared_roots.live_roots().await, 0);

        service
            .dispatch_native_hook(
                "codex",
                "SessionStart",
                json!({"session_id":"second","cwd":root}),
                &root,
            )
            .await
            .expect("second session");
        assert_eq!(
            service.sessions["second"].state.roots
                [&hex::encode(service.sessions["second"].state.root_id)]
                .source_identity,
            first_identity
        );
        service.shutdown().await.expect("shutdown");

        let resumed = ServiceControl::open(state)
            .await
            .expect("both durable sessions reopen without an identity collision");
        assert_eq!(resumed.sessions.keys().collect::<Vec<_>>(), vec!["second"]);
        resumed.shutdown().await.expect("resumed shutdown");
    }

    #[test]
    fn service_replay_deletes_the_older_conflicting_same_root_session() {
        run_large_stack("conflicting-same-root", conflicting_same_root_case);
    }

    async fn conflicting_same_root_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        let state = temporary.path().join("state");
        fs::create_dir(&root).expect("root");
        let mut service = ServiceControl::open(state.clone()).await.expect("service");
        for session_id in ["older", "newer"] {
            service
                .dispatch_native_hook(
                    "codex",
                    "SessionStart",
                    json!({"session_id":session_id,"cwd":root}),
                    &root,
                )
                .await
                .expect("session start");
            service
                .dispatch_native_hook(
                    "codex",
                    "SessionEnd",
                    json!({"session_id":session_id}),
                    &root,
                )
                .await
                .expect("session end");
        }
        let older = service.session_directory("older");
        let newer = service.session_directory("newer");
        service.shutdown().await.expect("shutdown");

        let mut conflicting = load_saved_state(&older).expect("older state");
        conflicting.active = true;
        for binding in conflicting.roots.values_mut() {
            binding.source_identity = [9; 16];
        }
        save_state(&older, &conflicting, Survives::PowerLoss).expect("conflicting state");
        std::thread::sleep(std::time::Duration::from_millis(20));
        let mut current = load_saved_state(&newer).expect("newer state");
        current.active = true;
        save_state(&newer, &current, Survives::PowerLoss).expect("refresh newer state");

        let resumed = ServiceControl::open(state)
            .await
            .expect("service replay discards the older conflict");
        assert!(!older.exists());
        assert!(newer.exists());
        assert_eq!(resumed.sessions.keys().collect::<Vec<_>>(), vec!["newer"]);
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
                .roots
                .values()
                .next()
                .expect("root binding")
                .path,
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
        fs::create_dir_all(&data).expect("state directory");
        let local = LocalFs::local(LocalOptions::new(data.join("filesystem")))
            .await
            .expect("local filesystem");
        let store = LocalCoreStateStore::open_owned(data.join("core-state")).expect("state owner");
        let shared_roots = SharedRootRegistry::default();
        let mut control = ControlPlane::open_with(
            data.clone(),
            data.clone(),
            local.clone(),
            store.clone(),
            shared_roots.clone(),
        )
        .await
        .expect("control");
        control
            .session_start(json!({"session_id":"session","cwd":root}))
            .await
            .expect("session");
        control.shutdown().await.expect("shutdown");
        let displaced = temporary.path().join("displaced-root");
        fs::rename(&root, &displaced).expect("displace original root");
        fs::create_dir(&root).expect("replacement root");
        let Err(error) =
            ControlPlane::open_with(data.clone(), data, local, store, shared_roots).await
        else {
            panic!("replaced root must be rejected");
        };
        assert!(error.contains("changed identity"), "{error}");
    }

    #[test]
    fn copilot_hooks_route_a_child_without_environment_state() {
        run_large_stack("copilot-hook-e2e", copilot_hook_case);
    }

    #[test]
    fn cursor_root_hook_is_capability_honest_and_uses_workspace_roots() {
        run_large_stack("cursor-hook-e2e", cursor_hook_case);
    }

    #[test]
    fn host_capability_profiles_never_conflate_routing_with_confinement() {
        for host in ["codex", "claude-code"] {
            let profile = host_adapter_profile(host);
            assert_eq!(
                profile.child_workspaces,
                ChildWorkspaceSupport::RecursiveToolRouting
            );
            assert!(profile.stable_child_identity);
            let guidance = guidance_for(host);
            assert!(guidance.contains("routing"));
            assert!(guidance.contains("not process-level confinement"));
            assert!(!guidance.contains("subagents isolated"));
        }
        let copilot = host_adapter_profile("copilot");
        assert_eq!(
            copilot.child_workspaces,
            ChildWorkspaceSupport::RootLifecycleOnly
        );
        assert!(!copilot.stable_child_identity);
        for host in ["cursor", "opencode", "unknown"] {
            assert_eq!(
                host_adapter_profile(host).child_workspaces,
                ChildWorkspaceSupport::CliOnly
            );
            assert!(guidance_for(host).contains("CLI-only"));
        }
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
            service.sessions["cursor-session"]
                .state
                .roots
                .values()
                .next()
                .expect("root binding")
                .path,
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
        let child_path = route_path(&service.sessions["session"].state.routes["child"]);
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

    #[test]
    fn codex_hooks_route_child_tools_by_documented_turn_identity() {
        run_large_stack("codex-hook-turn-identity", codex_hook_turn_identity_case);
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
                "UserPromptSubmit",
                json!({"session_id":"session","cwd":root.display().to_string()}),
                &root,
            )
            .await
            .expect("root prompt");
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
        let child_path = route_path(&service.sessions["session"].state.routes["child"]);
        let routed = service
            .dispatch_native_hook(
                "claude-code",
                "PreToolUse",
                json!({
                    "session_id":"session",
                    "cwd":root.display().to_string(),
                    "agent_id":"child",
                    "agent_type":"general-purpose",
                    "tool_name":"Bash",
                    "tool_use_id":"tool-1",
                    "tool_input":{"command":"pwd"}
                }),
                &root,
            )
            .await
            .expect("child tool");
        let child_shell_path = child_path.display().to_string().replace('\\', "/");
        assert!(
            routed["hookSpecificOutput"]["updatedInput"]["command"]
                .as_str()
                .is_some_and(|command| command.contains(&child_shell_path))
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
        assert!(
            service
                .dispatch_native_hook(
                    "claude-code",
                    "PreToolUse",
                    json!({
                        "session_id":"session",
                        "cwd":root.display().to_string(),
                        "agent_id":"unknown",
                        "agent_type":"general-purpose",
                        "tool_name":"Bash",
                        "tool_use_id":"tool-forged",
                        "tool_input":{"command":"pwd"}
                    }),
                    &root,
                )
                .await
                .is_err()
        );
        service
            .dispatch_native_hook(
                "claude-code",
                "PostToolUseFailure",
                json!({
                    "session_id":"session",
                    "cwd":root.display().to_string(),
                    "agent_id":"child",
                    "agent_type":"general-purpose",
                    "tool_name":"Bash",
                    "tool_use_id":"tool-1",
                    "tool_input":{"command":"pwd"}
                }),
                &root,
            )
            .await
            .expect("failed tool close");
        assert!(service.sessions["session"].state.leases.is_empty());
        service
            .dispatch_native_hook(
                "claude-code",
                "SubagentStop",
                json!({
                    "session_id":"session",
                    "cwd":child_path.display().to_string(),
                    "agent_id":"child"
                }),
                &child_path,
            )
            .await
            .expect("child stop");
        service
            .dispatch_native_hook(
                "claude-code",
                "SubagentStop",
                json!({
                    "session_id":"session",
                    "cwd":child_path.display().to_string(),
                    "agent_id":"child"
                }),
                &child_path,
            )
            .await
            .expect("duplicate child stop");
        assert!(
            service.sessions["session"].state.routes["child"]
                .lifecycle
                .is_frozen()
        );
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
            .expect("child resume");
        assert_eq!(
            service.sessions["session"].state.routes["child"].lifecycle,
            RouteLifecycle::Active
        );
        let context_id = service.sessions["session"].state.root_context_id;
        service
            .dispatch_native_hook(
                "claude-code",
                "SessionStart",
                json!({
                    "session_id":"session",
                    "cwd":root.display().to_string(),
                    "source":"compact"
                }),
                &root,
            )
            .await
            .expect("compact session start");
        assert_eq!(
            service.sessions["session"].state.root_context_id,
            context_id
        );
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

    async fn codex_hook_turn_identity_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root");
        let mut service = ServiceControl::open(temporary.path().join("state"))
            .await
            .expect("service");
        service
            .dispatch_native_hook(
                "codex",
                "SessionStart",
                json!({"session_id":"session","cwd":root}),
                &root,
            )
            .await
            .expect("session start");
        service
            .dispatch_native_hook(
                "codex",
                "UserPromptSubmit",
                json!({"session_id":"session","turn_id":"root-turn","cwd":root}),
                &root,
            )
            .await
            .expect("root turn");
        service
            .dispatch_native_hook(
                "codex",
                "PreToolUse",
                json!({
                    "session_id":"session","turn_id":"root-turn","cwd":root,
                    "tool_name":"Bash","tool_use_id":"root-agents",
                    "tool_input":{"command":"acyclic agents"}
                }),
                &root,
            )
            .await
            .expect("root CLI service access");
        service
            .dispatch_native_hook(
                "codex",
                "PreToolUse",
                json!({
                    "session_id":"session","turn_id":"root-turn","cwd":root,
                    "tool_name":"collaborationspawn_agent","tool_use_id":"spawn-child","tool_input":{}
                }),
                &root,
            )
            .await
            .expect("spawn handshake");
        service
            .dispatch_native_hook(
                "codex",
                "SubagentStart",
                json!({
                    "session_id":"session","turn_id":"child-turn","cwd":root,
                    "agent_id":"child","agent_type":"explorer"
                }),
                &root,
            )
            .await
            .expect("child start");
        let child = route_path(&service.sessions["session"].state.routes["child"]);
        let routed = service
            .dispatch_native_hook(
                "codex",
                "PreToolUse",
                json!({
                    "session_id":"session","turn_id":"child-tool-turn","agent_id":"child","cwd":root,
                    "tool_name":"Bash","tool_use_id":"child-command",
                    "tool_input":{"command":"pwd"}
                }),
                &root,
            )
            .await
            .expect("child tool routed from physical cwd");
        assert!(
            routed["hookSpecificOutput"]["updatedInput"]["command"]
                .as_str()
                .is_some_and(
                    |command| command.contains(&child.to_string_lossy().replace('\\', "/"))
                )
        );
        assert_eq!(
            service.sessions["session"].state.turns["child-tool-turn"],
            "child"
        );
        assert!(
            service
                .dispatch_native_hook(
                    "codex",
                    "PreToolUse",
                    json!({
                        "session_id":"session","turn_id":"forged-turn","agent_id":"unknown","cwd":root,
                        "tool_name":"Bash","tool_use_id":"forged-command",
                        "tool_input":{"command":"pwd"}
                    }),
                    &root,
                )
                .await
                .is_err()
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
    fn root_registration_intents_recover_before_native_reopen() {
        run_large_stack("core-root-recovery", core_root_recovery_case);
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

    #[cfg(any(unix, windows))]
    #[test]
    fn service_drain_waits_for_shutdown_completion_and_lock_release() {
        std::thread::Builder::new()
            .name("plugin-service-drain".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(async {
                        let temporary = tempfile::tempdir().expect("temporary directory");
                        let data = temporary.path().join("state");
                        let service_data = data.clone();
                        let service = tokio::spawn(async move { run_service(service_data).await });
                        for _ in 0..250 {
                            if data.join("service.identity").exists() {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        }
                        let identity = ServiceMarker::read(&data)
                            .expect("service identity")
                            .instance_id;
                        let mismatch = drain_service(&data, Some("replacement-service")).await;
                        assert!(matches!(
                            mismatch,
                            Err(error) if error == "refusing to drain a replacement Acyclic service"
                        ));
                        assert!(acquire_service_lock(&data).expect("service lock").is_none());
                        let fence = drain_service(&data, Some(&identity))
                            .await
                            .expect("identity-bound durable drain");
                        drop(fence);
                        tokio::time::timeout(std::time::Duration::from_secs(5), service)
                            .await
                            .expect("service exit deadline")
                            .expect("service task")
                            .expect("clean service shutdown");
                        assert!(!data.join("service.identity").exists());
                        assert!(service_drain_completion_path(&data).exists());
                        assert!(acquire_service_lock(&data).expect("service lock").is_some());
                    });
            })
            .expect("test thread")
            .join()
            .expect("service drain thread");
    }

    /// A service of another release speaks another control protocol, so it
    /// rejects every request of this one; it is still identified and stopped
    /// through the handoff contract.
    #[cfg(any(unix, windows))]
    #[test]
    fn a_service_speaking_another_protocol_is_identified_and_stopped() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("state");
        let service_data = data.clone();
        let older = std::thread::Builder::new()
            .name("older-protocol-service".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                control_protocol::TEST_PROTOCOL_MAJOR
                    .with(|major| major.set(Some(control_protocol::CONTROL_PROTOCOL_MAJOR - 1)));
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("older service runtime")
                    .block_on(run_service_with_identity(
                        service_data,
                        Some("older-service-binary".to_owned()),
                        None,
                    ))
            })
            .expect("older service thread");
        std::thread::Builder::new()
            .name("current-protocol-client".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("client runtime")
                    .block_on(async {
                        let mut marker = None;
                        for _ in 0..500 {
                            marker = ServiceMarker::read(&data);
                            if marker.is_some() {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        }
                        let marker = marker.expect("older service marker");
                        assert_eq!(marker.binary_identity, "older-service-binary");
                        let ping = ping_request().expect("ping");
                        let rejected = send_control_request_once(&data, &ping).await;
                        assert!(
                            matches!(rejected, Err(ControlRequestError::Response(ref error)) if error.contains("incompatible Acyclic control protocol")),
                            "{rejected:?}"
                        );
                        let identity = service_identity(&data).expect("current identity");
                        assert!(
                            !service_is_ready_for_identity(&data, &identity)
                                .await
                                .expect("stop the older service")
                        );
                        assert!(ServiceMarker::read(&data).is_none());
                        assert!(acquire_service_lock(&data).expect("service lock").is_some());
                        let completion: Value = serde_json::from_slice(
                            &fs::read(service_drain_completion_path(&data)).expect("drain record"),
                        )
                        .expect("drain record json");
                        assert_eq!(completion["identity"], marker.instance_id.as_str());
                        assert_eq!(completion["ok"], true);
                    });
            })
            .expect("client thread")
            .join()
            .expect("current protocol client");
        older
            .join()
            .expect("older service thread")
            .expect("older service drained cleanly");
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn failed_identity_publication_releases_endpoint_service_and_root() {
        std::thread::Builder::new()
            .name("plugin-service-marker-failure".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(async {
                        let temporary = tempfile::tempdir().expect("temporary directory");
                        let data = temporary.path().join("state");
                        fs::create_dir_all(data.join("service.identity"))
                            .expect("identity publication obstruction");
                        assert!(
                            run_service_with_identity(
                                data.clone(),
                                Some("service".to_owned()),
                                None
                            )
                            .await
                            .is_err()
                        );
                        #[cfg(unix)]
                        assert!(!data.join("service.sock").exists());
                        let fence = acquire_service_lock(&data)
                            .expect("service lifecycle lock")
                            .expect("failed startup released the service lifecycle");
                        drop(fence);
                        let reopened = ServiceControl::open(data)
                            .await
                            .expect("reopen root after failed service startup");
                        reopened.shutdown().await.expect("reopened shutdown");
                    });
            })
            .expect("test thread")
            .join()
            .expect("service marker failure thread");
    }

    #[test]
    fn service_lock_contention_uses_platform_error_semantics() {
        assert!(service_lock_is_contended(&io::Error::from(
            io::ErrorKind::WouldBlock
        )));
        #[cfg(windows)]
        assert!(service_lock_is_contended(&io::Error::from_raw_os_error(33)));
    }

    #[test]
    fn service_identity_follows_artifact_bytes_not_launcher_path() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path();
        let launcher = data.join("launcher");
        let plugin_cache = data.join("plugin-cache");
        fs::write(&launcher, b"same signed artifact").expect("launcher artifact");
        fs::write(&plugin_cache, b"same signed artifact").expect("cached artifact");
        let identity = |path: &Path| service_identity_for(data, path).expect("identity");

        assert_eq!(identity(&launcher), identity(&plugin_cache));
        assert_eq!(
            identity(&launcher),
            executable_digests_for(&launcher)
                .expect("full launcher digests")
                .service_identity
        );

        fs::write(&plugin_cache, b"replacement artifact").expect("replacement artifact");
        assert_ne!(identity(&launcher), identity(&plugin_cache));
    }

    #[test]
    fn cached_service_identity_is_keyed_by_the_file_fingerprint() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path();
        let executable = data.join("acyclic");
        fs::write(&executable, b"first artifact").expect("artifact");
        let identity = service_identity_for(data, &executable).expect("identity");
        let cache = data.join("executable-identity");
        assert!(
            !cache.exists(),
            "a just-written executable could change again within its timestamp tick"
        );
        std::thread::sleep(SETTLED_EXECUTABLE_AGE + std::time::Duration::from_millis(100));
        assert_eq!(
            service_identity_for(data, &executable).expect("settled identity"),
            identity
        );
        let entry = fs::read_to_string(&cache).expect("cached identity");
        assert!(entry.ends_with(&identity));

        // A hit returns the cached value without hashing the bytes again.
        let (fingerprint, _) = entry.split_once('\n').expect("cache entry");
        let marker = "0".repeat(64);
        fs::write(&cache, format!("{fingerprint}\n{marker}")).expect("marked cache");
        assert_eq!(
            service_identity_for(data, &executable).expect("cached identity"),
            marker
        );

        // Any rewrite changes the fingerprint, even to bytes of equal length.
        fs::write(&executable, b"other artifact").expect("rewritten artifact");
        let rewritten = service_identity_for(data, &executable).expect("rewritten identity");
        assert_ne!(rewritten, marker);
        assert_eq!(
            rewritten,
            executable_digests_for(&executable)
                .expect("rewritten digests")
                .service_identity
        );

        for torn in [b"".as_slice(), b"torn", entry.as_bytes().split_at(70).0] {
            fs::write(&cache, torn).expect("torn cache");
            assert_eq!(
                service_identity_for(data, &executable).expect("identity despite torn cache"),
                rewritten
            );
        }
    }

    #[test]
    fn unreachable_service_cleanup_clears_only_its_stale_marker_under_lock() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(async {
                let temporary = tempfile::tempdir().expect("temporary directory");
                let data = temporary.path().join("state");
                fs::create_dir_all(&data).expect("service data");
                fs::write(data.join("service.identity"), "dead-service").expect("stale identity");
                fs::write(data.join("durable-state"), "preserved").expect("durable sentinel");
                let fence = drain_service(&data, None)
                    .await
                    .expect("dead service cleanup fence");
                assert!(!data.join("service.identity").exists());
                assert_eq!(
                    fs::read_to_string(data.join("durable-state")).expect("durable sentinel"),
                    "preserved"
                );
                assert!(
                    acquire_service_lock(&data)
                        .expect("contended service lock")
                        .is_none()
                );
                drop(fence);
                assert!(
                    acquire_service_lock(&data)
                        .expect("released service lock")
                        .is_some()
                );
            });
    }

    #[test]
    fn parent_lifecycle_fence_survives_removal_of_the_service_data_tree() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("state");
        let mut fence = acquire_service_lock(&data)
            .expect("acquire service lock")
            .expect("uncontended service lock");
        fence.prepare_purge();
        fs::remove_dir_all(&data).expect("remove service data while parent fence remains held");
        assert!(
            acquire_service_lock(&data)
                .expect("contended lifecycle check")
                .is_none(),
            "a replacement service must not create a new in-tree lock during purge"
        );
        drop(fence);
        assert!(
            acquire_service_lock(&data)
                .expect("reacquire after purge")
                .is_some()
        );
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn service_handoff_durably_drains_a_mismatched_binary() {
        std::thread::Builder::new()
            .name("plugin-service-handoff".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(async {
                        let temporary = tempfile::tempdir().expect("temporary directory");
                        let data = temporary.path().join("state");
                        let service_data = data.clone();
                        let service = tokio::spawn(async move {
                            run_service_with_identity(
                                service_data,
                                Some("older-service-binary".to_owned()),
                                None,
                            )
                            .await
                        });
                        for _ in 0..250 {
                            if data.join("service.identity").exists() {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        }
                        let instance_id = ServiceMarker::read(&data)
                            .expect("service instance identity")
                            .instance_id;
                        assert!(uuid::Uuid::parse_str(&instance_id).is_ok());
                        assert!(
                            !service_is_ready_for_identity(&data, "replacement-service-binary")
                                .await
                                .expect("durable handoff")
                        );
                        tokio::time::timeout(std::time::Duration::from_secs(5), service)
                            .await
                            .expect("service exit deadline")
                            .expect("service task")
                            .expect("clean service shutdown");
                        assert!(service_drain_completion_path(&data).exists());
                        assert!(acquire_service_lock(&data).expect("service lock").is_some());
                    });
            })
            .expect("test thread")
            .join()
            .expect("service handoff thread");
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn service_handoff_durably_drains_live_sessions_before_replacement() {
        std::thread::Builder::new()
            .name("plugin-live-service-handoff".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(async {
                        let temporary = tempfile::tempdir().expect("temporary directory");
                        let data = temporary.path().join("state");
                        let root = temporary.path().join("root");
                        fs::create_dir(&root).expect("root directory");
                        let service_data = data.clone();
                        let service = tokio::spawn(async move {
                            run_service_with_identity(
                                service_data,
                                Some("older-service-binary".to_owned()),
                                None,
                            )
                            .await
                        });
                        for _ in 0..250 {
                            if data.join("service.identity").exists() {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        }
                        send_control_request(
                            &data,
                            &ControlRequest {
                                version: 1,
                                command: ControlCommand::Hook,
                                cwd: root.clone(),
                                argv: Vec::new(),
                                name: "codex:SessionStart".to_owned(),
                                arguments: json!({"session_id":"live","cwd":root.clone()}),
                            },
                        )
                        .await
                        .expect("start live session");

                        assert!(
                            !service_is_ready_for_identity(&data, "replacement-service-binary")
                                .await
                                .expect("drain live service")
                        );
                        tokio::time::timeout(std::time::Duration::from_secs(5), service)
                            .await
                            .expect("service exit deadline")
                            .expect("service task")
                            .expect("clean service shutdown");
                        let reopened = ServiceControl::open(data.clone())
                            .await
                            .expect("reopen drained service state");
                        assert!(
                            reopened.sessions.is_empty(),
                            "handoff-drained sessions must not reopen"
                        );
                    });
            })
            .expect("test thread")
            .join()
            .expect("live service handoff thread");
    }

    #[test]
    fn explicit_shutdown_processes_sessions_after_an_earlier_failure() {
        run_large_stack("complete-service-shutdown", || async {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let data = temporary.path().join("state");
            let mut service = ServiceControl::open(data.clone()).await.expect("service");

            {
                let broken = service
                    .create_session("a-broken")
                    .await
                    .expect("broken session");
                broken.state.active = true;
                broken.state.root_session_id = "a-broken".to_owned();
                broken.state.leases.insert(
                    "orphaned-lease".to_owned(),
                    LeaseRecord {
                        agent_id: "missing-agent".to_owned(),
                        turn_id: String::new(),
                        tool_name: "Bash".to_owned(),
                        roots: BTreeMap::new(),
                        expires_at_millis: 0,
                    },
                );
                broken.persist().expect("broken session state");
            }
            let broken_directory = service.session_directory("a-broken");
            {
                let healthy = service
                    .create_session("z-healthy")
                    .await
                    .expect("healthy session");
                healthy.state.active = true;
                healthy.state.root_session_id = "z-healthy".to_owned();
                healthy.persist().expect("healthy session state");
            }
            let healthy_directory = service.session_directory("z-healthy");

            let error = service
                .shutdown_sessions(true)
                .await
                .expect_err("orphaned lease must fail shutdown");
            assert!(error.contains("a-broken"));
            assert!(service.sessions.is_empty());
            assert!(
                load_saved_state(&broken_directory)
                    .expect("failed session remains recoverable")
                    .active,
                "failed teardown must not publish an inactive session"
            );
            assert!(
                !load_saved_state(&healthy_directory)
                    .expect("healthy session state")
                    .active,
                "a later session must be durably inactive despite an earlier failure"
            );
        });
    }

    #[cfg(any(windows, all(unix, not(target_os = "linux"))))]
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

    #[cfg(any(unix, windows))]
    #[test]
    fn concurrent_spawn_and_child_tool_hooks_both_complete() {
        run_large_stack("concurrent-hook-endpoint", concurrent_hook_endpoint_case);
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires a real Lean project path in ACYCLIC_E2E_LEAN_WORKSPACE"]
    fn real_lean_compiler_executes_inside_the_child_mount() {
        run_large_stack("real-lean-child-mount", real_lean_child_mount_case);
    }

    #[cfg(any(target_os = "linux", windows))]
    #[test]
    #[ignore = "requires a live native mount and the installed Rust compiler"]
    fn rustc_executes_inside_the_child_mount() {
        run_large_stack("rustc-child-mount", rustc_child_mount_case);
    }

    #[cfg(any(windows, all(unix, not(target_os = "linux"))))]
    #[test]
    fn client_disconnect_does_not_cancel_inflight_control_dispatch() {
        std::thread::Builder::new()
            .name("plugin-endpoint-disconnect".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(endpoint_disconnect_case());
            })
            .expect("test thread")
            .join()
            .expect("plugin endpoint disconnect thread");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mailbox_shutdown_drains_inflight_dispatch() {
        run_large_stack(
            "plugin-mailbox-shutdown",
            mailbox_shutdown_drains_dispatch_case,
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stalled_mailbox_respects_the_caller_deadline() {
        run_large_stack("plugin-mailbox-deadline", mailbox_deadline_case);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn malformed_mailbox_exchange_does_not_stop_the_endpoint() {
        run_large_stack("plugin-mailbox-malformed", mailbox_malformed_exchange_case);
    }

    #[cfg(any(windows, all(unix, not(target_os = "linux"))))]
    #[test]
    fn stalled_control_stream_respects_the_caller_deadline() {
        run_large_stack("plugin-stream-deadline", stream_deadline_case);
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
    fn unobserved_parent_directory_reports_a_typed_merge_conflict() {
        std::thread::Builder::new()
            .name("plugin-unobserved-directory-merge".to_owned())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(async {
                        let temporary = tempfile::tempdir().expect("temporary directory");
                        let root = temporary.path().join("root");
                        fs::create_dir_all(root.join("sub")).expect("unobserved directory");
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
                                "session_id":"session","turn_id":"root-turn",
                                "tool_use_id":"spawn-child","tool_name":"spawn_agent","tool_input":{}
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
                        let child_root = route_path(&control.state.routes["child"]);
                        fs::create_dir_all(child_root.join("sub"))
                            .expect("child mounted directory");
                        fs::write(child_root.join("sub/cache.tmp"), b"child")
                            .expect("child mounted file");
                        fs::write(root.join("sub/cache.tmp"), b"parent")
                            .expect("parent file");
                        let result = control
                            .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
                            .await
                            .expect("typed conflict, not an invalid candidate");
                        assert_eq!(result["status"], "conflicted");
                        assert!(result["conflicts"].as_array().is_some_and(|conflicts| {
                            conflicts.iter().any(|conflict| {
                                conflict["path"] == "/sub" && conflict["kind"] == "Binding"
                            })
                        }));
                        assert_eq!(
                            fs::read(root.join("sub/cache.tmp")).expect("parent file remains"),
                            b"parent"
                        );
                    });
            })
            .expect("test thread")
            .join()
            .expect("unobserved directory merge thread");
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
        fs::create_dir_all(root.join("sub")).expect("baseline nested directory");
        let mut control = ControlPlane::open(data.clone())
            .await
            .expect("control plane");
        control
            .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
            .await
            .expect("root session");
        control
            .root_git_tool(
                WorkspaceRootId::from_bytes(control.state.root_id),
                vec!["status".to_owned()],
            )
            .await
            .expect("observe baseline nested directory");
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
            .write_text("/scratch.tmp", "publish without tracking")
            .await
            .expect("ignored child file");
        transaction
            .create_dir_all("/sub")
            .await
            .expect("nested ignore directory");
        transaction
            .write_text("/sub/.gitignore", "*.tmp\n")
            .await
            .expect("nested ignore policy");
        transaction
            .write_text("/sub/cache.tmp", "nested child cache")
            .await
            .expect("nested ignored child file");
        transaction.commit().await.expect("child commit");
        fs::write(root.join("scratch.tmp"), b"competing parent cache")
            .expect("parent cache update");
        fs::write(root.join("sub/cache.tmp"), b"competing nested parent cache")
            .expect("parent nested cache update");
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
        assert_eq!(
            fs::read(root.join("scratch.tmp")).expect("ignored file reaches parent worktree"),
            b"publish without tracking"
        );
        assert_eq!(
            fs::read(root.join("sub/cache.tmp")).expect("nested ignored file reaches parent"),
            b"nested child cache"
        );
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
        drop(transaction);
        drop(later_transaction);
        drop(child);
        drop(root_workspace);
        drop(control);

        let mut reopened = ControlPlane::open(data)
            .await
            .expect("restart recovers compatibility history");
        let child_route = &reopened.state.routes["child"];
        let child_workspace = reopened
            .workspace(child_route)
            .await
            .expect("recovered child workspace");
        let child_head = child_workspace.head().await.expect("recovered child head");
        assert_eq!(
            child_route.roots[&root_key(WorkspaceRootId::from_bytes(reopened.state.root_id))]
                .published_generation,
            *child_head.id().digest().as_bytes(),
            "recovery must persist child finalization before removing the publication journal"
        );
        assert_eq!(
            child_head
                .read("/later.txt", 1024)
                .await
                .expect("child rebased onto latest parent"),
            b"must not enter recovered publication history"[..]
        );
        let repository = GitCompatRepository::new(
            acyclic_fs::WorkspaceId::from_bytes(root_repository_workspace_id(&reopened)),
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
                published_generation.read("/scratch.tmp", 1024).await,
                Err(WorkspaceError::NotFound)
            ),
            "newly ignored file must remain outside compatibility history"
        );
        assert!(
            matches!(
                published_generation.read("/sub/cache.tmp", 1024).await,
                Err(WorkspaceError::NotFound)
            ),
            "nested newly ignored file must remain outside compatibility history"
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
        let diff: GitCommandOutput = serde_json::from_value(
            reopened
                .root_git_tool(root_id, vec!["diff".to_owned()])
                .await
                .expect("diff after ignored-file publication"),
        )
        .expect("typed diff result");
        let GitCommandOutput::Filesystem(GitFilesystemResult::Data { kind, value }) = diff else {
            panic!("root diff must return filesystem data");
        };
        assert_eq!(kind, "diff");
        assert_eq!(
            value["bindingChanges"], 1,
            "only the later uncommitted file, not the ignored scratch file, belongs in diff: {value}"
        );
        drop(child_workspace);
        drop(child_head);
        drop(root_workspace);
        drop(published_workspace);
        drop(published_generation);
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
            acyclic_fs::WorkspaceId::from_bytes(root_repository_workspace_id(&control));
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

    #[cfg(any(windows, all(unix, not(target_os = "linux"))))]
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
        assert!(
            endpoint.socket_path.as_os_str().len() < 100,
            "the control socket must fit conservative Unix-domain path limits"
        );
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

        let control = match Arc::try_unwrap(shared) {
            Ok(control) => control.into_inner(),
            Err(_) => panic!("endpoint retained an in-flight control request"),
        };
        control.shutdown().await.expect("control shutdown");
        let reopened = ControlPlane::open(data)
            .await
            .expect("reopen after in-flight request shutdown");
        reopened.shutdown().await.expect("reopened shutdown");
    }

    #[cfg(any(unix, windows))]
    async fn concurrent_hook_endpoint_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let root = temporary.path().join("root");
        fs::create_dir_all(&root).expect("root directory");
        let service = Arc::new(AsyncMutex::new(
            ServiceControl::open(data.clone()).await.expect("service"),
        ));
        let endpoint = start_control_endpoint(Arc::clone(&service), &data)
            .await
            .expect("control endpoint");
        let hook = |event: &str, cwd: &Path, arguments: Value| ControlRequest {
            version: 1,
            command: ControlCommand::Hook,
            cwd: cwd.to_path_buf(),
            argv: Vec::new(),
            name: format!("claude-code:{event}"),
            arguments,
        };
        send_control_request(
            &data,
            &hook(
                "SessionStart",
                &root,
                json!({"session_id":"session","cwd":root}),
            ),
        )
        .await
        .expect("session start");
        let first_spawn = send_control_request(
            &data,
            &hook(
                "PreToolUse",
                &root,
                json!({
                    "session_id":"session","cwd":root,"tool_name":"Agent",
                    "tool_use_id":"spawn-one","tool_input":{}
                }),
            ),
        )
        .await;
        if first_spawn.is_err() {
            endpoint.shutdown().await.expect("endpoint shutdown");
            let service = Arc::try_unwrap(service)
                .unwrap_or_else(|_| panic!("endpoint retained service"))
                .into_inner();
            assert!(service.sessions["session"].state.pending.is_empty());
            service.shutdown().await.expect("service shutdown");
            return;
        }
        send_control_request(
            &data,
            &hook(
                "SubagentStart",
                &root,
                json!({"session_id":"session","cwd":root,"agent_id":"child"}),
            ),
        )
        .await
        .expect("child start");
        let child = route_path(&service.lock().await.sessions["session"].state.routes["child"]);
        let spawn = hook(
            "PreToolUse",
            &root,
            json!({
                "session_id":"session","cwd":root,"tool_name":"Agent",
                "tool_use_id":"spawn-two","tool_input":{}
            }),
        );
        let child_tool = hook(
            "PreToolUse",
            &child,
            json!({
                "session_id":"session","cwd":child,"tool_name":"Bash",
                "tool_use_id":"child-tool","tool_input":{"command":"pwd"}
            }),
        );
        let requests = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                send_control_request(&data, &spawn),
                send_control_request(&data, &child_tool)
            )
        })
        .await
        .expect("concurrent hook deadline");
        requests.0.expect("second spawn response");
        requests.1.expect("child tool response");
        send_control_request(
            &data,
            &hook(
                "PostToolUse",
                &child,
                json!({
                    "session_id":"session","cwd":child,"tool_name":"Bash",
                    "tool_use_id":"child-tool","tool_input":{"command":"pwd"}
                }),
            ),
        )
        .await
        .expect("child tool close");
        endpoint.shutdown().await.expect("endpoint shutdown");
        let service = Arc::try_unwrap(service)
            .unwrap_or_else(|_| panic!("endpoint retained service"))
            .into_inner();
        service.shutdown().await.expect("service shutdown");
    }

    #[cfg(any(target_os = "linux", windows))]
    async fn rustc_child_mount_case() {
        #[cfg(target_os = "linux")]
        let source = tempfile::tempdir_in("/dev/shm").expect("source workspace");
        #[cfg(windows)]
        let source = tempfile::tempdir().expect("source workspace");
        std::fs::write(
            source.path().join("acyclic-workflow.rs"),
            include_bytes!("../tests/fixtures/overlay_workflow.rs"),
        )
        .expect("Rust source");
        #[cfg(target_os = "linux")]
        let state = tempfile::tempdir_in("/dev/shm").expect("temporary state");
        #[cfg(windows)]
        let state = tempfile::tempdir().expect("temporary state");
        let mut service = ServiceControl::open(state.path().join("state"))
            .await
            .expect("service");
        service
            .dispatch_native_hook(
                "claude-code",
                "SessionStart",
                json!({"session_id":"rustc","cwd":source.path()}),
                source.path(),
            )
            .await
            .expect("session start");
        service
            .dispatch_native_hook(
                "claude-code",
                "PreToolUse",
                json!({
                    "session_id":"rustc","cwd":source.path(),"tool_name":"Agent",
                    "tool_use_id":"spawn","tool_input":{"description":"compile"}
                }),
                source.path(),
            )
            .await
            .expect("spawn handshake");
        service
            .dispatch_native_hook(
                "claude-code",
                "SubagentStart",
                json!({
                    "session_id":"rustc","cwd":source.path(),"agent_id":"compiler",
                    "agent_type":"general-purpose"
                }),
                source.path(),
            )
            .await
            .expect("child start");
        let child = route_path(&service.sessions["rustc"].state.routes["compiler"]);
        service
            .dispatch_native_hook(
                "claude-code",
                "PreToolUse",
                json!({
                    "session_id":"rustc","cwd":child,"agent_id":"compiler",
                    "tool_name":"Bash","tool_use_id":"rustc",
                    "tool_input":{"command":format!("rustc --edition 2021 acyclic-workflow.rs -o {}", if cfg!(windows) { "main.exe" } else { "main" })}
                }),
                &child,
            )
            .await
            .expect("tool lease");
        let started = std::time::Instant::now();
        let mut compiler = std::process::Command::new("rustc")
            .current_dir(&child)
            .args([
                "--edition",
                "2021",
                "acyclic-workflow.rs",
                "-o",
                if cfg!(windows) { "main.exe" } else { "main" },
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("start rustc");
        let deadline = started + std::time::Duration::from_secs(10);
        let timed_out = loop {
            if compiler.try_wait().expect("poll rustc").is_some() {
                break false;
            }
            if std::time::Instant::now() >= deadline {
                compiler.kill().expect("kill timed out rustc");
                break true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        let output = compiler.wait_with_output().expect("collect rustc output");
        let executable = child.join(if cfg!(windows) { "main.exe" } else { "main" });
        #[cfg(unix)]
        let executable_mode = std::fs::metadata(&executable)
            .map(|metadata| std::os::unix::fs::MetadataExt::mode(&metadata));
        #[cfg(windows)]
        let executable_mode = std::fs::metadata(&executable).map(|metadata| metadata.len());
        service
            .dispatch_native_hook(
                "claude-code",
                "PostToolUse",
                json!({
                    "session_id":"rustc","cwd":child,"agent_id":"compiler",
                    "tool_name":"Bash","tool_use_id":"rustc"
                }),
                &child,
            )
            .await
            .expect("close tool lease");
        let mut workflow = std::process::Command::new(&executable)
            .current_dir(&child)
            .arg("workflow-output.txt")
            .env("ACYCLIC_WORKFLOW_TOKEN", "qualified")
            .spawn()
            .expect("start mounted workflow");
        let workflow_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let execution_after_sync = loop {
            if let Some(status) = workflow.try_wait().expect("poll mounted workflow") {
                break Some(status);
            }
            if std::time::Instant::now() >= workflow_deadline {
                workflow.kill().expect("kill timed-out mounted workflow");
                break None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        assert_eq!(
            std::fs::read(child.join("workflow-output.txt")).expect("workflow output"),
            b"isolated"
        );
        service
            .dispatch_native_hook(
                "claude-code",
                "SubagentStop",
                json!({"session_id":"rustc","cwd":child,"agent_id":"compiler"}),
                &child,
            )
            .await
            .expect("child stop");
        service
            .dispatch_native_hook(
                "claude-code",
                "SessionEnd",
                json!({"session_id":"rustc","cwd":source.path()}),
                source.path(),
            )
            .await
            .expect("session end");
        service.shutdown().await.expect("service shutdown");
        assert!(!timed_out, "rustc exceeded its 10 second deadline");
        assert!(
            output.status.success(),
            "rustc failed after {:?}\nstdout:\n{}\nstderr:\n{}",
            started.elapsed(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            execution_after_sync
                .as_ref()
                .is_some_and(std::process::ExitStatus::success),
            "compiled binary did not execute after tool synchronization: mode={executable_mode:?}, result={execution_after_sync:?}"
        );
        assert!(
            !source
                .path()
                .join(if cfg!(windows) { "main.exe" } else { "main" })
                .exists()
        );
    }

    #[cfg(target_os = "linux")]
    async fn real_lean_child_mount_case() {
        let root = PathBuf::from(
            env::var_os("ACYCLIC_E2E_LEAN_WORKSPACE")
                .expect("set ACYCLIC_E2E_LEAN_WORKSPACE to a real Lean project"),
        )
        .canonicalize()
        .expect("Lean project path");
        assert!(root.join("lakefile.toml").exists() || root.join("lakefile.lean").exists());
        // Resolve the real project environment before mounting so this bounded gate measures
        // Lean compiler I/O, not Lake's whole dependency-graph freshness scan.
        let lean_environment = std::process::Command::new("lake")
            .args(["env", "printenv", "LEAN_PATH"])
            .current_dir(&root)
            .output()
            .expect("resolve Lean project environment");
        assert!(
            lean_environment.status.success(),
            "lake env failed: {}",
            String::from_utf8_lossy(&lean_environment.stderr)
        );
        let lean_path = String::from_utf8(lean_environment.stdout)
            .expect("UTF-8 LEAN_PATH")
            .trim()
            .to_owned();
        let temporary = tempfile::tempdir_in("/dev/shm").expect("temporary state");
        let mut service = ServiceControl::open(temporary.path().join("state"))
            .await
            .expect("service");
        service
            .dispatch_native_hook(
                "claude-code",
                "SessionStart",
                json!({"session_id":"lean","cwd":root}),
                &root,
            )
            .await
            .expect("session start");
        service
            .dispatch_native_hook(
                "claude-code",
                "PreToolUse",
                json!({
                    "session_id":"lean","cwd":root,"tool_name":"Agent",
                    "tool_use_id":"spawn","tool_input":{"description":"compile"}
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
                    "session_id":"lean","cwd":root,"agent_id":"compiler",
                    "agent_type":"general-purpose"
                }),
                &root,
            )
            .await
            .expect("child start");
        let child = route_path(&service.sessions["lean"].state.routes["compiler"]);
        service
            .dispatch_native_hook(
                "claude-code",
                "PreToolUse",
                json!({
                    "session_id":"lean","cwd":child,"agent_id":"compiler",
                    "tool_name":"Bash","tool_use_id":"lean-compile",
                    "tool_input":{"command":"lean YcDemo/Lemmas.lean -o .acyclic-lean-qualification.olean"}
                }),
                &child,
            )
            .await
            .expect("tool lease");
        let started = std::time::Instant::now();
        let mut compilation = std::process::Command::new("lean")
            .args([
                "YcDemo/Lemmas.lean",
                "-o",
                ".acyclic-lean-qualification.olean",
            ])
            .current_dir(&child)
            .env("LEAN_PATH", lean_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("start Lean compilation");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let timed_out = loop {
            if compilation
                .try_wait()
                .expect("poll Lean compilation")
                .is_some()
            {
                break false;
            }
            if std::time::Instant::now() >= deadline {
                compilation.kill().expect("kill timed out Lean compilation");
                break true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        let output = compilation
            .wait_with_output()
            .expect("collect Lean compilation output");
        let child_output_exists = child.join(".acyclic-lean-qualification.olean").is_file();
        service
            .dispatch_native_hook(
                "claude-code",
                "PostToolUse",
                json!({
                    "session_id":"lean","cwd":child,"agent_id":"compiler",
                    "tool_name":"Bash","tool_use_id":"lean-compile"
                }),
                &child,
            )
            .await
            .expect("close tool lease");
        service
            .dispatch_native_hook(
                "claude-code",
                "SubagentStop",
                json!({"session_id":"lean","cwd":child,"agent_id":"compiler"}),
                &child,
            )
            .await
            .expect("child stop");
        service
            .dispatch_native_hook(
                "claude-code",
                "SessionEnd",
                json!({"session_id":"lean","cwd":root}),
                &root,
            )
            .await
            .expect("session end");
        service.shutdown().await.expect("service shutdown");
        assert!(
            !timed_out,
            "Lean compilation exceeded the 30 second mount budget\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.status.success(),
            "Lean compilation failed after {:?}:\n{}",
            started.elapsed(),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(child_output_exists);
        assert!(!root.join(".acyclic-lean-qualification.olean").exists());
    }

    struct ControlledDispatcher {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        completed: Arc<tokio::sync::Notify>,
    }

    struct ReplayCountingDispatcher {
        executions: std::sync::atomic::AtomicUsize,
    }

    impl ConcurrentControlRequestDispatcher for ReplayCountingDispatcher {
        async fn dispatch_request(&self, _request: ControlRequest) -> Result<Value, String> {
            let execution = self
                .executions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            Ok(json!({"execution": execution}))
        }
    }

    impl ControlRequestDispatcher for ControlledDispatcher {
        async fn dispatch_request(&mut self, _request: ControlRequest) -> Result<Value, String> {
            self.started.notify_one();
            self.release.notified().await;
            self.completed.notify_one();
            Ok(json!({}))
        }
    }

    #[cfg(any(windows, all(unix, not(target_os = "linux"))))]
    async fn stream_deadline_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(tokio::sync::Notify::new());
        let control = Arc::new(AsyncMutex::new(ControlledDispatcher {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            completed: Arc::clone(&completed),
        }));
        let endpoint = start_control_endpoint(Arc::clone(&control), &data)
            .await
            .expect("control endpoint");
        let request = ControlRequest {
            version: 1,
            command: ControlCommand::Ping,
            cwd: temporary.path().to_path_buf(),
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        };
        #[cfg(unix)]
        let client = tokio::net::UnixStream::connect(&endpoint.socket_path)
            .await
            .expect("connect control endpoint");
        #[cfg(windows)]
        let client = connect_test_pipe(&endpoint.pipe_path).await;
        endpoint.accepted.notified().await;
        let envelope = ControlEnvelope::new(request);
        let request_id = envelope.request_id.clone();
        let mut request = serde_json::to_vec(&envelope).expect("encode request");
        request.push(b'\n');
        let began = std::time::Instant::now();
        let exchange = tokio::spawn(async move {
            exchange_control_stream(
                client,
                &request,
                &request_id,
                std::time::Duration::from_millis(25),
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), started.notified())
            .await
            .expect("stalled dispatcher never received the request");
        let error = exchange
            .await
            .expect("exchange task")
            .expect_err("stalled control response must time out");
        assert!(matches!(error, ControlRequestError::Indeterminate(_)));
        assert!(
            began.elapsed() < std::time::Duration::from_secs(1),
            "control stream ignored its request deadline"
        );
        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), completed.notified())
            .await
            .expect("timed-out request was cancelled before completion");
        tokio::time::timeout(std::time::Duration::from_secs(1), endpoint.shutdown())
            .await
            .expect("endpoint shutdown deadline")
            .expect("endpoint shutdown");
        assert!(
            Arc::try_unwrap(control).is_ok(),
            "timed-out stream retained dispatch state"
        );
    }

    #[cfg(any(windows, all(unix, not(target_os = "linux"))))]
    async fn endpoint_disconnect_case() {
        use tokio::io::AsyncWriteExt as _;

        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(tokio::sync::Notify::new());
        let control = Arc::new(AsyncMutex::new(ControlledDispatcher {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            completed: Arc::clone(&completed),
        }));
        let endpoint = start_control_endpoint(Arc::clone(&control), &data)
            .await
            .expect("control endpoint");
        #[cfg(unix)]
        let mut client = tokio::net::UnixStream::connect(&endpoint.socket_path)
            .await
            .expect("connect control endpoint");
        #[cfg(windows)]
        let mut client = connect_test_pipe(&endpoint.pipe_path).await;
        endpoint.accepted.notified().await;
        let envelope = ControlEnvelope::new(ControlRequest {
            version: 1,
            command: ControlCommand::Ping,
            cwd: temporary.path().to_path_buf(),
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        });
        let request = serde_json::to_vec(&envelope).expect("encode request");
        client.write_all(&request).await.expect("write request");
        client.write_all(b"\n").await.expect("finish request");
        started.notified().await;
        drop(client);

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), control.lock())
                .await
                .is_err(),
            "disconnect cancelled the accepted request"
        );
        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), completed.notified())
            .await
            .expect("disconnected request did not finish");
        endpoint.shutdown().await.expect("endpoint shutdown");
        assert!(
            Arc::try_unwrap(control).is_ok(),
            "endpoint retained disconnected dispatch state"
        );
    }

    #[tokio::test]
    async fn durable_hook_response_is_replayed_without_reexecution_after_restart() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        fs::create_dir(&data).expect("plugin data directory");
        let dispatcher = Arc::new(ReplayCountingDispatcher {
            executions: std::sync::atomic::AtomicUsize::new(0),
        });
        let envelope = ControlEnvelope::new(ControlRequest {
            version: 1,
            command: ControlCommand::Hook,
            cwd: temporary.path().to_path_buf(),
            argv: Vec::new(),
            name: "codex:PreToolUse".to_owned(),
            arguments: json!({"session_id":"session","tool_use_id":"tool"}),
        });

        let first_ledger = Arc::new(ControlLedger::open(&data).expect("first ledger"));
        let first = dispatch_control_envelope(&dispatcher, &first_ledger, envelope.clone()).await;
        drop(first_ledger);
        let reopened = Arc::new(ControlLedger::open(&data).expect("reopened ledger"));
        let replay = dispatch_control_envelope(&dispatcher, &reopened, envelope).await;

        let executed: Value = serde_json::from_slice(&first).expect("first response");
        assert_eq!(executed["ok"], true, "first hook must execute: {executed}");
        assert_eq!(
            first, replay,
            "a retry must receive the exact bytes first sent"
        );
        assert_eq!(
            dispatcher
                .executions
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a completed hook must never execute twice"
        );
    }

    #[tokio::test]
    async fn unledgered_control_commands_still_require_exact_protocol_negotiation() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        fs::create_dir(&data).expect("plugin data directory");
        let dispatcher = Arc::new(ReplayCountingDispatcher {
            executions: std::sync::atomic::AtomicUsize::new(0),
        });
        let ledger = Arc::new(ControlLedger::open(&data).expect("ledger"));

        for command in [ControlCommand::Ping, ControlCommand::Agents] {
            let mut envelope = ControlEnvelope::new(ControlRequest {
                version: 1,
                command,
                cwd: temporary.path().to_path_buf(),
                argv: Vec::new(),
                name: String::new(),
                arguments: Value::Null,
            });
            envelope.protocol.major = envelope.protocol.major.saturating_add(1);
            let response: Value = serde_json::from_slice(
                &dispatch_control_envelope(&dispatcher, &ledger, envelope).await,
            )
            .expect("response");
            assert_eq!(response["ok"], false, "mismatched protocol was accepted");
        }
        assert_eq!(
            dispatcher
                .executions
                .load(std::sync::atomic::Ordering::SeqCst),
            0,
            "invalid unledgered envelopes must not reach the dispatcher"
        );
    }

    #[tokio::test]
    async fn no_request_runs_on_top_of_an_unflushed_transition() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root directory");
        let mut control = ControlPlane::open(data.clone())
            .await
            .expect("control plane");
        control
            .session_start(json!({"session_id":"session","cwd":root}))
            .await
            .expect("session start");
        let prompt = |turn: &str| ControlRequest {
            version: 1,
            command: ControlCommand::Hook,
            cwd: root.clone(),
            argv: Vec::new(),
            name: "codex:UserPromptSubmit".to_owned(),
            arguments: json!({"session_id":"session","cwd":root,"turn_id":turn}),
        };
        dispatch_session_request(&mut control, prompt("first"))
            .await
            .expect("first prompt");
        assert!(
            control.unflushed,
            "a remembered root turn is saved unflushed"
        );

        // Until the transition is durable, no later request may act on it.
        control.fail_next_flush = true;
        let refused = dispatch_session_request(&mut control, prompt("second"))
            .await
            .expect_err("a request must wait for the previous transition to be durable");
        assert!(refused.contains("flush"), "{refused}");
        assert!(!control.state.root_turns.contains("second"));
        assert!(control.unflushed);

        dispatch_session_request(&mut control, prompt("second"))
            .await
            .expect("second prompt once the first is durable");
        assert!(control.state.root_turns.contains("second"));
        control
            .shutdown()
            .await
            .expect("shutdown flushes the last transition");
        let reopened = load_saved_state(&data).expect("durable state");
        assert!(reopened.root_turns.contains("second"));
    }

    #[tokio::test]
    async fn duplicate_spawn_hook_does_not_allocate_a_second_workspace_handshake() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root directory");
        let mut control = ControlPlane::open(data).await.expect("control plane");
        control
            .session_start(json!({"session_id":"session","cwd":root}))
            .await
            .expect("session start");
        control
            .user_prompt(json!({"session_id":"session","turn_id":"turn"}))
            .expect("root turn");
        let hook = json!({
            "session_id":"session",
            "turn_id":"turn",
            "tool_use_id":"spawn",
            "tool_name":"spawn_agent",
            "tool_input":{}
        });
        if control.pre_tool(hook.clone()).await.is_err() {
            assert!(control.state.pending.is_empty());
            assert!(control.pending_mounts.is_empty());
            control.shutdown().await.expect("shutdown after rejection");
            return;
        }
        let first = control
            .state
            .pending
            .front()
            .expect("pending spawn")
            .clone();
        control.pre_tool(hook).await.expect("duplicate spawn");
        assert_eq!(control.state.pending.len(), 1);
        assert_eq!(
            control
                .state
                .pending
                .front()
                .expect("pending spawn")
                .fork_key,
            first.fork_key
        );
        control.shutdown().await.expect("shutdown");
    }

    #[test]
    fn spawn_preparation_is_atomic_and_restartable() {
        run_large_stack("spawn-preparation", spawn_preparation_case);
    }

    #[test]
    fn direct_control_shutdown_waits_for_detached_root_owner() {
        run_large_stack("control-root-release", || async {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let data = temporary.path().join("plugin-data");
            let control = ControlPlane::open(data.clone())
                .await
                .expect("control plane");
            let detached = control.fs.clone();
            let shutdown = tokio::spawn(control.shutdown());
            tokio::pin!(shutdown);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), &mut shutdown)
                    .await
                    .is_err(),
                "shutdown acknowledged while a detached filesystem owner was still live"
            );
            drop(detached);
            tokio::time::timeout(std::time::Duration::from_secs(5), shutdown)
                .await
                .expect("root release deadline")
                .expect("shutdown task")
                .expect("shutdown");
            let reopened = ControlPlane::open(data)
                .await
                .expect("reopen after release");
            reopened.shutdown().await.expect("reopened shutdown");
        });
    }

    #[test]
    fn shared_control_shutdown_leaves_root_release_to_service() {
        run_large_stack("shared-control-root-release", || async {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let data = temporary.path().join("plugin-data");
            let fs = LocalFs::local(LocalOptions::new(data.join("filesystem")))
                .await
                .expect("shared filesystem");
            let release = fs.local_root_release_barrier().expect("release barrier");
            let control = ControlPlane::open_with(
                data.clone(),
                data.clone(),
                fs.clone(),
                LocalCoreStateStore::open_owned(data.join("core-state")).expect("state owner"),
                SharedRootRegistry::default(),
            )
            .await
            .expect("shared control");
            tokio::time::timeout(std::time::Duration::from_secs(5), control.shutdown())
                .await
                .expect("session shutdown deadline")
                .expect("session shutdown");
            assert!(
                !release.is_released(),
                "session shutdown released the service-owned filesystem"
            );
            drop(fs);
            wait_for_root_release(Some(release))
                .await
                .expect("service root release");
        });
    }

    async fn spawn_preparation_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let root = temporary.path().join("root");
        fs::create_dir(&root).expect("root directory");
        let mut control = ControlPlane::open(data.clone())
            .await
            .expect("control plane");
        control
            .session_start(json!({"session_id":"session","cwd":root}))
            .await
            .expect("session start");
        control
            .user_prompt(json!({"session_id":"session","turn_id":"turn"}))
            .expect("root turn");
        let incomplete_key = [9; 16];
        control.state.pending.push_back(PendingSpawn {
            parent_agent_id: control.state.root_agent_id.clone(),
            tool_use_id: "incomplete".to_owned(),
            active_root_id: None,
            expires_at_millis: now_millis() + 60_000,
            workspace_name: "incomplete".to_owned(),
            fork_key: incomplete_key,
            roots: BTreeMap::new(),
            mount_path: control
                .workspace_mount_root()
                .join(compact_id(&incomplete_key)),
            lifecycle: PendingSpawnLifecycle::Discarding,
        });
        assert!(
            control
                .pre_tool(json!({
                    "session_id":"session",
                    "turn_id":"turn",
                    "tool_use_id":"incomplete",
                    "tool_name":"spawn_agent",
                    "tool_input":{}
                }))
                .await
                .is_err(),
            "an incomplete retry must remain denied"
        );
        control.state.pending.clear();
        let spawn = control
            .pre_tool(json!({
                "session_id":"session",
                "turn_id":"turn",
                "tool_use_id":"spawn",
                "tool_name":"spawn_agent",
                "tool_input":{}
            }))
            .await;
        if spawn.is_err() {
            assert!(
                control.state.pending.is_empty(),
                "a rejected spawn retained durable preparation state"
            );
            assert!(
                control.pending_mounts.is_empty(),
                "a rejected spawn retained a native mount"
            );
            control.shutdown().await.expect("shutdown after rejection");
            return;
        }
        let prepared = control
            .state
            .pending
            .front()
            .expect("prepared spawn")
            .clone();
        assert_eq!(prepared.lifecycle, PendingSpawnLifecycle::Prepared);
        assert!(control.pending_mounts.contains_key(&prepared.fork_key));
        control.shutdown().await.expect("first shutdown");

        let mut recovered = ControlPlane::open(data)
            .await
            .expect("recover control plane");
        assert_eq!(
            recovered
                .state
                .pending
                .front()
                .expect("recovered spawn")
                .lifecycle,
            PendingSpawnLifecycle::Prepared
        );
        assert!(recovered.pending_mounts.contains_key(&prepared.fork_key));
        let started = recovered
            .subagent_start(json!({
                "session_id":"session",
                "turn_id":"child-turn",
                "agent_id":"child",
                "agent_type":"explorer"
            }))
            .await
            .expect("bind prepared spawn");
        let active_path = prepared
            .mount_path
            .join(route_name(WorkspaceRootId::from_bytes(
                recovered.state.root_id,
            )));
        assert!(
            started
                .pointer("/hookSpecificOutput/additionalContext")
                .and_then(Value::as_str)
                .is_some_and(|context| context.contains(active_path.to_string_lossy().as_ref()))
        );
        assert!(recovered.state.pending.is_empty());
        assert!(recovered.mounts.contains_key("child"));
        recovered.shutdown().await.expect("recovered shutdown");
    }

    #[cfg(target_os = "linux")]
    async fn mailbox_shutdown_drains_dispatch_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(tokio::sync::Notify::new());
        let control = Arc::new(AsyncMutex::new(ControlledDispatcher {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            completed: Arc::clone(&completed),
        }));
        let endpoint = start_control_endpoint(Arc::clone(&control), &data)
            .await
            .expect("control endpoint");
        let request = ControlRequest {
            version: 1,
            command: ControlCommand::Ping,
            cwd: temporary.path().to_path_buf(),
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        };
        let client_data = data.clone();
        let client =
            tokio::spawn(async move { send_control_request_once(&client_data, &request).await });
        started.notified().await;

        let shutdown = tokio::spawn(endpoint.shutdown());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), control.lock())
                .await
                .is_err(),
            "mailbox shutdown cancelled the accepted request"
        );
        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), completed.notified())
            .await
            .expect("mailbox request did not finish during shutdown");
        tokio::time::timeout(std::time::Duration::from_secs(1), shutdown)
            .await
            .expect("mailbox shutdown deadline")
            .expect("mailbox shutdown task")
            .expect("mailbox shutdown");
        let _ = client.await;
        assert!(
            Arc::try_unwrap(control).is_ok(),
            "mailbox retained cancelled dispatch state"
        );
    }

    #[cfg(target_os = "linux")]
    async fn mailbox_deadline_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        prepare_linux_control_mailbox(&data).expect("mailbox");
        let envelope = ControlEnvelope::new(ControlRequest {
            version: 1,
            command: ControlCommand::Hook,
            cwd: temporary.path().to_path_buf(),
            argv: Vec::new(),
            name: "claude-code:PreToolUse".to_owned(),
            arguments: Value::Null,
        });
        let request_id = envelope.request_id.clone();
        let request = serde_json::to_vec(&envelope).expect("request");
        let started = std::time::Instant::now();
        let error = send_linux_mailbox_request(
            &data,
            &request,
            &request_id,
            std::time::Duration::from_millis(25),
        )
        .await
        .expect_err("stalled mailbox must time out");
        assert!(matches!(error, ControlRequestError::Indeterminate(_)));
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "mailbox ignored its caller deadline"
        );
        assert!(
            fs::read_dir(linux_control_mailbox_path(&data))
                .expect("mailbox")
                .next()
                .is_none(),
            "a client that gives up must remove its exchange"
        );
    }

    #[cfg(target_os = "linux")]
    async fn mailbox_malformed_exchange_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let control = Arc::new(ReplayCountingDispatcher {
            executions: std::sync::atomic::AtomicUsize::new(0),
        });
        let endpoint = start_control_endpoint(Arc::clone(&control), &data)
            .await
            .expect("control endpoint");
        let malformed = endpoint.mailbox_path.join("000-malformed");
        fs::create_dir(&malformed).expect("malformed exchange");
        fs::write(malformed.join("request"), b"{}").expect("malformed request");
        fs::create_dir(malformed.join("processing")).expect("conflicting processing directory");

        let response = send_control_request_once(
            &data,
            &ControlRequest {
                version: 1,
                command: ControlCommand::Ping,
                cwd: temporary.path().to_path_buf(),
                argv: Vec::new(),
                name: String::new(),
                arguments: Value::Null,
            },
        )
        .await
        .expect("valid request after malformed exchange");
        assert_eq!(response["execution"], 1);
        assert_eq!(
            control.executions.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        endpoint.shutdown().await.expect("endpoint shutdown");
    }

    #[cfg(windows)]
    async fn sequential_endpoint_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let control = Arc::new(ReplayCountingDispatcher {
            executions: std::sync::atomic::AtomicUsize::new(0),
        });
        let endpoint = start_control_endpoint(Arc::clone(&control), &data)
            .await
            .expect("control endpoint");
        for sequence in 0..64 {
            let response = send_control_request(
                &data,
                &ControlRequest {
                    version: 1,
                    command: ControlCommand::Ping,
                    cwd: temporary.path().to_path_buf(),
                    argv: Vec::new(),
                    name: String::new(),
                    arguments: Value::Null,
                },
            )
            .await
            .expect("sequential request");
            assert_eq!(
                response["execution"],
                sequence + 1,
                "every accepted pipe must return its own complete response"
            );
        }
        endpoint.shutdown().await.expect("endpoint shutdown");
        assert!(
            Arc::try_unwrap(control).is_ok(),
            "endpoint retained a completed request"
        );
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
            .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
            .await
            .expect("duplicate session start is idempotent");
        control
            .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
            .expect("root turn");
        control
            .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
            .expect("duplicate root turn is idempotent");
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
        control
            .pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn-expiring","tool_name":"spawn_agent","tool_input":{}}))
            .await
            .expect("duplicate spawn hook is idempotent");
        assert_eq!(control.state.pending.len(), 1);
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
        control
            .subagent_start(json!({"session_id":"session","turn_id":"child-turn","agent_id":"child","agent_type":"explorer"}))
            .await
            .expect("duplicate child start is idempotent");
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
        let (caller, _, _) = native_tool_identity(
            &mut control,
            "claude-code",
            &json!({"agent_id":"child"}),
            &root,
        )
        .expect("the authenticated child identity permits cwd redirection from its physical root");
        assert_eq!(caller, "child");
        assert!(control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"unknown-tool","tool_name":"mystery_mutator","tool_input":{}}))
            .await
            .is_err());
        let leases_before_remote = control.state.leases.len();
        control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"remote-message","tool_name":"mcp__codex_app__send_message_to_thread","tool_input":{"threadId":"thread","prompt":"status"}}))
            .await
            .expect("known non-filesystem tool bypasses workspace routing");
        control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"remote-message","tool_name":"mcp__codex_app__send_message_to_thread"}))
            .await
            .expect("known non-filesystem post hook is a no-op");
        assert_eq!(control.state.leases.len(), leases_before_remote);
        let shell = control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"system-status","tool_name":"exec_command","tool_input":{"cmd":"git status","workdir":root.display().to_string()}}))
            .await
            .expect("shell command is redirected to the child mount");
        assert_eq!(
            shell["hookSpecificOutput"]["updatedInput"]["workdir"],
            route_path(&control.state.routes["child"])
                .display()
                .to_string()
        );
        control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"system-status","tool_name":"exec_command"}))
            .await
            .expect("close redirected shell lease");

        let child_path = route_path(&control.state.routes["child"]);
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
        control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"read-one","tool_name":"Read"}))
            .await
            .expect("duplicate post-tool is idempotent");
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
        drop(transaction);
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
        drop(child);
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
        drop(transaction);
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
        assert_eq!(
            control.state.routes["child"].lifecycle,
            RouteLifecycle::StopRequested
        );
        drop(child);
        drop(control);
        let mut control = ControlPlane::open(data.clone())
            .await
            .expect("reopen after failed stop teardown");
        assert!(!control.mounts.contains_key("child"));
        assert!(control.state.routes["child"].lifecycle.is_frozen());
        let root_agent = control.state.root_agent_id.clone();
        control
            .subagent_start(json!({
                "session_id":"session",
                "turn_id":"child-turn",
                "agent_id":"child",
                "agent_type":"explorer",
                "_authenticated_parent_agent_id":root_agent,
            }))
            .await
            .expect("resume child after durable stop recovery");
        assert!(control.mounts.contains_key("child"));
        assert_eq!(
            control.state.routes["child"].lifecycle,
            RouteLifecycle::Active
        );

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
            .pre_tool(json!({
                "session_id":"session",
                "turn_id":"child-turn",
                "tool_use_id":"recovered-read",
                "tool_name":"Read",
                "tool_input":{"path":child_path.join("first.txt").display().to_string()}
            }))
            .await
            .expect("next tool fences and recovers the expired writer first");
        assert!(!control.state.leases.contains_key("active-read"));
        assert!(control.state.leases.contains_key("recovered-read"));
        control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"recovered-read","tool_name":"Read"}))
            .await
            .expect("close recovered tool");
        control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"pending-descendant","tool_name":"spawn_agent","tool_input":{}}))
            .await
            .expect("pending descendant before discard");
        control
            .subagent_start(json!({
                "session_id":"session",
                "turn_id":"grandchild-turn",
                "agent_id":"grandchild",
                "agent_type":"explorer",
                "_authenticated_parent_agent_id":"child",
            }))
            .await
            .expect("start live grandchild");
        let stopped = control
            .subagent_stop(
                json!({"session_id":"session","turn_id":"child-turn","agent_id":"child"}),
            )
            .await
            .expect("request parent stop while grandchild is live");
        assert_eq!(stopped["suppressOutput"], true);
        assert_eq!(stopped["acyclicSummary"]["state"], "stopping");
        assert_eq!(stopped["acyclicSummary"]["agent"], "agents/child");
        assert_eq!(stopped["acyclicSummary"]["tests"]["status"], "not-reported");
        assert!(
            stopped["acyclicSummary"]["actions"]["merge"]
                .as_str()
                .is_some_and(|command| command.contains("agents/child"))
        );
        assert_eq!(
            control.state.routes["child"].lifecycle,
            RouteLifecycle::StopRequested
        );
        assert!(control.mounts.contains_key("child"));
        assert!(control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"after-stop-request","tool_name":"Read","tool_input":{"path":child_path.join("first.txt").display().to_string()}}))
            .await
            .is_err());
        control
            .subagent_stop(json!({
                "session_id":"session",
                "turn_id":"grandchild-turn",
                "agent_id":"grandchild",
            }))
            .await
            .expect("grandchild stop freezes it and its waiting parent");
        let root_agent = control.state.root_agent_id.clone();
        let status = control
            .agents_status(&root_agent)
            .await
            .expect("recursive agent status");
        assert_eq!(status["schemaVersion"], 1);
        assert_eq!(status["agents"][0]["ref"], "agents/child");
        assert_eq!(status["agents"][0]["state"], "frozen");
        assert!(status["agents"][0]["roots"].is_array());
        assert!(control.state.routes["child"].lifecycle.is_frozen());
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
        assert!(control.state.pending.is_empty());
        control.fail_after_context_discard = true;
        assert!(
            control
                .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn"}))
                .await
                .is_err()
        );
        assert!(control.state.pending_discards.contains_key("child"));
        drop(control);
        let control = ControlPlane::open(data)
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
        let child_context_id =
            WorkspaceContextId::from_bytes(control.state.routes["child"].context_id);
        let child_root_id = WorkspaceRootId::from_bytes(control.state.routes["child"].root_id);
        let original_workspace = control
            .distributed
            .contexts()
            .resolve(child_context_id)
            .await
            .expect("child context")
            .roots[&child_root_id]
            .workspace_id;
        let child_cwd = route_path(&control.state.routes["child"]);
        let shared = Arc::new(AsyncMutex::new(control));
        let ledger = Arc::new(ControlLedger::open(&data).expect("control ledger"));
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
            Arc::clone(&ledger),
            receiver,
        ));
        let envelope = ControlEnvelope::new(ControlRequest {
            version: 1,
            command: ControlCommand::Git,
            cwd: child_cwd.clone(),
            argv: vec!["status".to_owned()],
            name: String::new(),
            arguments: Value::Null,
        });
        let request = serde_json::to_vec(&envelope).expect("control request");
        client.write_all(&request).await.expect("write request");
        client.write_all(b"\n").await.expect("write newline");
        client.flush().await.expect("flush request");
        let mut response = String::new();
        BufReader::new(&mut client)
            .read_line(&mut response)
            .await
            .expect("read response");
        let response: Value = serde_json::from_str(&response).expect("response JSON");
        assert_eq!(response["version"], 2);
        assert_eq!(
            response["requestId"],
            Value::String(envelope.request_id.as_str().to_owned())
        );
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
            Arc::clone(&ledger),
            receiver,
        ));
        let switch_envelope = ControlEnvelope::new(ControlRequest {
            version: 1,
            command: ControlCommand::Git,
            cwd: child_cwd.clone(),
            argv: vec!["switch".to_owned(), "-c".to_owned(), "feature".to_owned()],
            name: String::new(),
            arguments: Value::Null,
        });
        let request = serde_json::to_vec(&switch_envelope).expect("control request");
        client.write_all(&request).await.expect("write request");
        client.write_all(b"\n").await.expect("write newline");
        client.flush().await.expect("flush request");
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let control = shared.lock().await;
                let current = control
                    .distributed
                    .contexts()
                    .resolve(child_context_id)
                    .await
                    .expect("switched child context")
                    .roots[&child_root_id]
                    .workspace_id;
                if current != original_workspace {
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
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), handler)
            .await
            .expect("blocked response shutdown")
            .expect("handler task")
            .expect_err("an unread bounded response must reach its drain deadline");
        assert!(
            error.contains("response exceeded its drain deadline"),
            "{error}"
        );
        drop(client);
        drop(ledger);
        let ledger = Arc::new(ControlLedger::open(&data).expect("reopen control ledger"));

        let (mut retry_client, retry_server) = tokio::io::duplex(64 * 1024);
        let (_retry_shutdown, retry_receiver) = watch::channel(false);
        let retry_handler = tokio::spawn(handle_control_connection(
            retry_server,
            Arc::clone(&shared),
            Arc::clone(&ledger),
            retry_receiver,
        ));
        let retry_request = serde_json::to_vec(&switch_envelope).expect("retry request");
        retry_client
            .write_all(&retry_request)
            .await
            .expect("write retry request");
        retry_client
            .write_all(b"\n")
            .await
            .expect("write retry newline");
        retry_client.flush().await.expect("flush retry request");
        let mut retry_response = String::new();
        BufReader::new(&mut retry_client)
            .read_line(&mut retry_response)
            .await
            .expect("read cached retry response");
        let retry_response: Value =
            serde_json::from_str(&retry_response).expect("cached response JSON");
        assert_eq!(retry_response["ok"], true, "{retry_response}");
        assert_eq!(
            retry_response["requestId"],
            Value::String(switch_envelope.request_id.as_str().to_owned())
        );
        retry_handler
            .await
            .expect("retry handler task")
            .expect("retry handler result");

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
        let control = match Arc::try_unwrap(shared) {
            Ok(control) => control.into_inner(),
            Err(_) => panic!("response cancellation retained the control plane"),
        };
        control.shutdown().await.expect("shutdown");
        let reopened = ControlPlane::open(data)
            .await
            .expect("reopen after blocked response shutdown");
        reopened.shutdown().await.expect("reopened shutdown");
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
        let child_path = route_path(&control.state.routes["child"]);
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
        let snapshot = control
            .distributed
            .operations()
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
        // A descendant can finish against the parent's last stable generation
        // while the parent still has an active tool. The parent's later close
        // supplies the next generation, rather than blocking the descendant.
        control
            .pre_tool(json!({
                "session_id":"session","turn_id":"child-turn",
                "tool_use_id":"parent-active-read","tool_name":"Read",
                "tool_input":{"path":child_path.join("base.txt").display().to_string()}
            }))
            .await
            .expect("parent starts overlapping read");
        assert_eq!(
            fs::read(child_path.join("base.txt")).expect("parent reads through its mount"),
            b"base"
        );
        let grandchild_path = route_path(&control.state.routes["grandchild"]);
        control
            .pre_tool(json!({
                "session_id":"session","turn_id":"grandchild-turn",
                "tool_use_id":"descendant-read","tool_name":"Read",
                "tool_input":{"path":grandchild_path.join("base.txt").display().to_string()}
            }))
            .await
            .expect("descendant starts while parent is active");
        control
            .post_tool(json!({
                "session_id":"session","turn_id":"grandchild-turn",
                "tool_use_id":"descendant-read","tool_name":"Read"
            }))
            .await
            .expect("descendant closes against stable parent generation");
        let writer = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "tests::projected_mount_writer_child",
                "--ignored",
            ])
            .env(
                "ACYCLIC_TEST_PROJECTED_WRITE_PATH",
                child_path.join("base.txt"),
            )
            .output()
            .expect("external writer process");
        assert!(
            writer.status.success(),
            "external writer failed: {}",
            String::from_utf8_lossy(&writer.stderr)
        );
        control
            .post_tool(json!({
                "session_id":"session","turn_id":"child-turn",
                "tool_use_id":"parent-active-read","tool_name":"Read"
            }))
            .await
            .expect("parent later closes");
        assert_eq!(
            control
                .workspace(&control.state.routes["child"])
                .await
                .expect("parent workspace")
                .read("/base.txt", 32)
                .await
                .expect("parent captured late write")
                .as_ref(),
            b"late parent"
        );
        let grandchild = control
            .workspace(&control.state.routes["grandchild"])
            .await
            .expect("grandchild workspace");
        let observed = grandchild
            .read("/base.txt", 32)
            .await
            .expect("descendant immediately observes completed parent");
        assert_eq!(observed.as_ref(), b"late parent");
        let mut transaction = grandchild
            .begin_transaction(IdempotencyKey::new())
            .await
            .expect("grandchild transaction");
        transaction
            .write_text("/nested.txt", "nested")
            .await
            .expect("nested file");
        transaction.commit().await.expect("commit nested file");
        drop(transaction);
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
            fs::read(root.join("base.txt")).expect("external child write reaches root"),
            b"late parent"
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
        drop(child_workspace);
        drop(grandchild);
        drop(control);

        let control = ControlPlane::open(data)
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
        let child_context_id = WorkspaceContextId::from_bytes(route.context_id);
        let last_root_id = WorkspaceRootId::from_bytes(route.roots[&last_key].root_id);
        let original_root = control
            .distributed
            .contexts()
            .resolve(child_context_id)
            .await
            .expect("child context")
            .roots[&last_root_id]
            .clone();
        control
            .distributed
            .contexts()
            .set_workspace(
                child_context_id,
                last_root_id,
                acyclic_fs::WorkspaceId::from_bytes([u8::MAX; 16]),
                "missing-workspace".to_owned(),
                original_root.parent_workspace_id,
            )
            .await
            .expect("inject missing core workspace");
        assert!(
            control
                .pre_tool(json!({
                    "session_id":"session",
                    "turn_id":"child-turn",
                    "tool_use_id":"partial-lease",
                    "tool_name":"Read",
                    "tool_input":{"path":route_path(&route).join("base.txt")}
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
            control
                .distributed
                .operations()
                .snapshot(first_workspace.id())
                .await
                .expect("rolled-back window")
                .phase,
            acyclic_fs::OperationWindowPhase::Idle
        ));
        control
            .distributed
            .contexts()
            .set_workspace(
                child_context_id,
                last_root_id,
                original_root.workspace_id,
                original_root.workspace_name,
                original_root.parent_workspace_id,
            )
            .await
            .expect("restore core workspace");
        control
            .pre_tool(json!({
                "session_id":"session",
                "turn_id":"child-turn",
                "tool_use_id":"active-write",
                "tool_name":"Write",
                "tool_input":{"path":route_path(&route).join("in-flight.txt")}
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
                .write_text("/child.txt", &route_root.mount_name())
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
        drop(first_workspace);
        control.shutdown().await.expect("shutdown");

        let resumed = ControlPlane::open(data)
            .await
            .expect("resume control plane");
        assert_eq!(resumed.state.roots.len(), 2);
        assert_eq!(resumed.state.routes["child"].roots.len(), 2);
        let (_, routed, root_id) = resumed
            .route_root_from_cwd(&route_path(&resumed.state.routes["child"]))
            .expect("route resumed child cwd");
        assert_eq!(routed.expect("child route").agent_id, "child");
        assert_eq!(root_id, second_root);
        resumed.shutdown().await.expect("resumed shutdown");
    }

    async fn core_root_recovery_case() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let first = temporary.path().join("first");
        let second = temporary.path().join("second");
        fs::create_dir_all(&first).expect("first root");
        fs::create_dir_all(&second).expect("second root");

        let initial_data = temporary.path().join("initial-plugin-data");
        let mut initial = ControlPlane::open(initial_data.clone())
            .await
            .expect("initial control plane");
        initial.fail_after_root_intent = true;
        let error = initial
            .session_start(json!({"session_id":"initial","cwd":first}))
            .await
            .expect_err("injected crash after durable initial root intent");
        assert_eq!(error, "injected failure after root registration intent");
        assert!(initial.state.pending_root_registration);
        let expected_context = initial.state.root_context_id;
        let expected_workspace = initial
            .state
            .roots
            .values()
            .next()
            .expect("root intent")
            .repository_workspace_id;
        drop(initial);
        // Only the root's attach reached the core log, unflushed: a power loss
        // leaves the flushed intent without the binding.
        let core_log = initial_data
            .join("core-state")
            .join("core-state-log-v1")
            .join(format!("{}.json", "0".repeat(32)));
        assert!(
            fs::metadata(&core_log).expect("core log").len() > 0,
            "the deferred attach reached the core log"
        );
        fs::write(&core_log, b"").expect("lose the unflushed attach");
        let resumed_initial = ControlPlane::open(initial_data)
            .await
            .expect("finish pending initial root registration");
        assert!(!resumed_initial.state.pending_root_registration);
        assert_eq!(resumed_initial.state.root_context_id, expected_context);
        assert_eq!(
            resumed_initial
                .state
                .roots
                .values()
                .next()
                .expect("recovered root")
                .repository_workspace_id,
            expected_workspace
        );
        resumed_initial.shutdown().await.expect("initial shutdown");

        // A power loss after the root registered loses the unflushed final
        // save; recovery repeats the registration against the durable intent.
        let registered_data = temporary.path().join("registered-plugin-data");
        let mut registered = ControlPlane::open(registered_data.clone())
            .await
            .expect("registered control plane");
        registered
            .session_start(json!({"session_id":"registered","cwd":first}))
            .await
            .expect("registered root");
        assert!(!registered.state.pending_root_registration);
        let expected_context = registered.state.root_context_id;
        drop(registered);
        fs::remove_file(registered_data.join(ADAPTER_STATE_SLOTS[2]))
            .expect("lose the unflushed final save");
        let reregistered = ControlPlane::open(registered_data)
            .await
            .expect("repeat the root registration");
        assert!(!reregistered.state.pending_root_registration);
        assert_eq!(reregistered.state.root_context_id, expected_context);
        reregistered
            .shutdown()
            .await
            .expect("reregistered shutdown");

        let mut control = ControlPlane::open(data.clone())
            .await
            .expect("control plane");
        control
            .session_start(json!({"session_id":"session","cwd":first}))
            .await
            .expect("first root attach");
        control.fail_after_root_intent = true;
        let error = control
            .session_start(json!({"session_id":"session","cwd":second}))
            .await
            .expect_err("injected crash after durable root intent");
        assert_eq!(error, "injected failure after root adoption intent");
        assert_eq!(control.state.pending_root_adoptions.len(), 1);
        let second_root = control
            .state
            .roots
            .values()
            .find(|root| root.root_id != control.state.root_id)
            .map(|root| WorkspaceRootId::from_bytes(root.root_id))
            .expect("second root binding");
        let second_key = root_key(second_root);
        let expected_workspace = control
            .state
            .roots
            .get(&second_key)
            .expect("durable root intent")
            .repository_workspace_id;
        drop(control);

        let resumed = ControlPlane::open(data)
            .await
            .expect("finish pending core root adoption");
        assert_eq!(resumed.state.roots.len(), 2);
        assert!(resumed.state.pending_root_adoptions.is_empty());
        assert_eq!(
            resumed.state.roots[&second_key].repository_workspace_id,
            expected_workspace
        );
        resumed.shutdown().await.expect("shutdown");

        let fenced_data = temporary.path().join("fenced-plugin-data");
        let mut fenced = ControlPlane::open(fenced_data.clone())
            .await
            .expect("fenced control plane");
        fenced
            .session_start(json!({"session_id":"fenced","cwd":first}))
            .await
            .expect("fenced first root");
        fenced.fail_after_root_intent = true;
        fenced
            .session_start(json!({"session_id":"fenced","cwd":second}))
            .await
            .expect_err("fenced adoption fault");
        let pending = fenced
            .state
            .pending_root_adoptions
            .first()
            .cloned()
            .expect("pending fenced root");
        fenced
            .state
            .roots
            .get_mut(&pending)
            .expect("pending fenced binding")
            .native_root_identity = [u8::MAX; 16];
        fenced.persist().expect("persist replacement identity");
        drop(fenced);
        let error = ControlPlane::open(fenced_data)
            .await
            .err()
            .expect("replacement root must be fenced");
        assert!(error.contains("identity changed"), "{error}");
    }

    #[test]
    fn patch_paths_are_rewritten_inside_the_child() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let child = temporary.path().join("workspace");
        fs::create_dir(&child).expect("child directory");
        let input = json!({"command": "*** Begin Patch\n*** Add File: src/new.rs\n*** Move to: src/moved.rs\n*** End Patch"});
        let rewritten = rewrite_tool_input("apply_patch", input, temporary.path(), &child)
            .expect("rewrite patch");
        let command = rewritten["command"].as_str().expect("command");
        assert!(command.contains(&child.join("src/new.rs").display().to_string()));
        assert!(command.contains(&child.join("src/moved.rs").display().to_string()));
    }

    #[test]
    fn patch_paths_cannot_escape_the_child() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        let child = temporary.path().join("workspace");
        fs::create_dir(&root).expect("root directory");
        fs::create_dir(&child).expect("child directory");
        let outside = temporary.path().join("outside.txt").display().to_string();
        for directive in [
            "Add File",
            "Update File",
            "Delete File",
            "Move to",
            "Copy to",
        ] {
            for path in ["../parent.txt", &outside] {
                let input = json!({
                    "command": format!("*** Begin Patch\n*** {directive}: {path}\n*** End Patch")
                });
                assert!(rewrite_tool_input("apply_patch", input, &root, &child).is_err());
            }
        }
    }

    #[test]
    fn remote_tool_hooks_are_process_local_noops_for_every_native_host() {
        for host in ["codex", "claude-code", "copilot", "cursor"] {
            for event in ["PreToolUse", "PostToolUse", "PostToolUseFailure"] {
                for tool in [
                    "mcp__codex_app__list_threads",
                    "mcp__codex_app__send_message_to_thread",
                ] {
                    assert!(native_hook_is_process_local_noop(
                        host,
                        event,
                        &json!({"tool_name":tool})
                    ));
                }
            }
        }
        assert!(!native_hook_is_process_local_noop(
            "codex",
            "PreToolUse",
            &json!({"tool_name":"Bash"})
        ));
        assert!(!native_hook_is_process_local_noop(
            "claude-code",
            "SessionStart",
            &json!({"tool_name":"mcp__codex_app__list_threads"})
        ));
        for tool in [
            "mcp__codex_app__create_thread",
            "mcp__codex_app__create_worktree",
            "mcp__codex_app__fork_thread",
            "mcp__codex_app__handoff_thread",
            "mcp__codex_app__open_in_codex",
            "mcp__codex_app__uninstall_plugin",
            "mcp__codex_app__future_unknown_tool",
        ] {
            assert!(
                !is_known_non_filesystem_tool(tool),
                "filesystem-capable or unknown app tool must fail closed: {tool}"
            );
        }
    }

    #[test]
    fn structured_paths_must_remain_inside_the_child() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let child = temporary.path().join("workspace");
        fs::create_dir(&child).expect("child directory");
        let valid = json!({"path": child.join("src/lib.rs").display().to_string()});
        validate_tool_paths("Read", &valid, &child).expect("child path");
        let rewritten = rewrite_tool_input(
            "Write",
            json!({"file_path":"relative.txt","content":"child"}),
            &temporary.path().join("root"),
            &child,
        )
        .expect("relative child path rewrite");
        assert_eq!(
            rewritten["file_path"],
            child.join("relative.txt").display().to_string()
        );
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
    fn persisted_mount_paths_are_derived_not_authoritative() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root_id = WorkspaceRootId::from_bytes([1; 16]);
        let mount_path = workspace_mount_root(temporary.path()).join(compact_id(&[2; 16]));
        assert_eq!(compact_id(&[2; 16]).len(), 22);
        assert_eq!(route_name(root_id).len(), 23);
        assert_eq!(
            mount_path
                .strip_prefix(temporary.path())
                .expect("relative mount"),
            Path::new("w").join(compact_id(&[2; 16]))
        );
        let mut different_context = [2; 16];
        different_context[15] = 3;
        assert_ne!(compact_id(&[2; 16]), compact_id(&different_context));
        let mut different_root = [1; 16];
        different_root[15] = 2;
        assert_ne!(
            route_name(root_id),
            route_name(WorkspaceRootId::from_bytes(different_root))
        );
        let staging = root_materialization_directory(
            &temporary.path().join("physical-root"),
            OperationId::from_bytes([4; 16]),
        )
        .expect("materialization directory");
        let staging_segments = staging
            .strip_prefix(temporary.path())
            .expect("relative staging")
            .components()
            .map(|component| component.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(staging_segments.len(), 3);
        assert_eq!(staging_segments[0], ".acyclic-m");
        assert_eq!(staging_segments[1].len(), 22);
        assert_eq!(staging_segments[2].len(), 22);
        let mut route = Route {
            agent_id: "child".to_owned(),
            turn_id: "turn".to_owned(),
            context_id: [2; 16],
            root_id: root_id.into_bytes(),
            parent_agent_id: "root".to_owned(),
            roots: BTreeMap::from([(
                root_key(root_id),
                RouteRoot {
                    root_id: root_id.into_bytes(),
                    repository_workspace_id: [3; 16],
                    published_generation: [0; 32],
                },
            )]),
            mount_path: mount_path.clone(),
            lifecycle: RouteLifecycle::Active,
        };
        assert_eq!(route_path(&route), mount_path.join(route_name(root_id)));
        validate_persisted_route_paths(temporary.path(), &route).expect("derived route paths");
        route.mount_path = temporary.path().join("outside");
        assert!(validate_persisted_route_paths(temporary.path(), &route).is_err());
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
    fn only_acyclic_commands_require_standalone_shell_syntax() {
        assert_eq!(
            is_acyclic_cli_invocation(
                "Bash",
                &json!({"command":"printf 'accepted\\n' > accepted.txt\ncat contract.txt"})
            ),
            Ok(false)
        );
        assert_eq!(
            is_acyclic_cli_invocation("Bash", &json!({"command":"acyclic agents"})),
            Ok(true)
        );
        assert_eq!(
            is_acyclic_cli_invocation("PowerShell", &json!({"command":"acyclic agents"})),
            Ok(true)
        );
        assert!(
            is_acyclic_cli_invocation("Bash", &json!({"command":"acyclic agents && whoami"}))
                .is_err()
        );
    }

    #[test]
    fn commandless_hosts_receive_exactly_one_shell_free_tool() {
        assert_eq!(public_tools(false), json!([]));
        let tools = public_tools(true);
        assert_eq!(tools.as_array().map(Vec::len), Some(1));
        assert_eq!(tools[0]["name"], "acyclic");
        assert_eq!(tools[0]["inputSchema"]["required"], json!(["argv"]));
    }

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
        let rewritten = rewrite_tool_input(
            "mcp__shell__exec_command",
            json!({"cmd": "pwd"}),
            &root,
            &child,
        )
        .expect("redirect namespaced exec workdir");
        assert_eq!(rewritten["workdir"], child.display().to_string());
        let rewritten = rewrite_tool_input(
            "PowerShell",
            json!({"command": "Get-Location"}),
            &root,
            &child,
        )
        .expect("redirect PowerShell cwd");
        assert!(
            rewritten["command"]
                .as_str()
                .is_some_and(|command| command.starts_with("Set-Location -LiteralPath"))
        );
    }

    #[test]
    fn control_responses_are_bounded() {
        let payload = "x".repeat(MAXIMUM_CONTROL_MESSAGE_BYTES);
        let encoded = encode_control_response(&json!({"payload": payload}), None);
        assert!(encoded.len() < MAXIMUM_CONTROL_MESSAGE_BYTES);
        let response: Value = serde_json::from_slice(&encoded).expect("response JSON");
        assert_eq!(response["ok"], false);
        assert!(
            response["error"]
                .as_str()
                .is_some_and(|error| error.contains("4 MiB"))
        );

        let request_id = control_protocol::RequestId::fresh();
        let encoded = control_response_for(&request_id, Ok(json!({"payload": payload})));
        assert!(encoded.len() < MAXIMUM_CONTROL_MESSAGE_BYTES);
        let response: Value = serde_json::from_slice(&encoded).expect("v2 response JSON");
        assert_eq!(response["version"], 2);
        assert_eq!(response["requestId"], request_id.as_str());
        assert_eq!(response["ok"], false);
    }

    #[test]
    fn malformed_v2_requests_keep_their_correlation_identity() {
        let request_id = control_protocol::RequestId::fresh();
        let request = serde_json::to_vec(&json!({
            "requestId": request_id.as_str(),
            "unexpected": true
        }))
        .expect("invalid envelope fixture");
        let error = serde_json::from_slice::<ControlEnvelope<ControlRequest>>(&request)
            .expect_err("fixture must not be a valid envelope");
        let response: Value =
            serde_json::from_slice(&invalid_control_request_response(&request, &error))
                .expect("correlated response");
        assert_eq!(response["version"], 2);
        assert_eq!(response["requestId"], request_id.as_str());
        assert_eq!(response["ok"], false);

        let uncorrelated = br#"{"unexpected":true}"#;
        let error = serde_json::from_slice::<ControlEnvelope<ControlRequest>>(uncorrelated)
            .expect_err("fixture must not be a valid envelope");
        let encoded = invalid_control_request_response(uncorrelated, &error);
        let response: Value = serde_json::from_slice(&encoded).expect("uncorrelated response");
        assert_eq!(response["version"], 2);
        assert!(response.get("requestId").is_none());
        assert!(matches!(
            decode_control_response(&encoded, &request_id),
            Err(ControlRequestError::Indeterminate(_))
        ));
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
    fn shared_hook_documents_restore_foreign_entries_and_preserve_edits() {
        for section in ["claude-hooks", "cursor-hooks"] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let path = temporary.path().join("hooks.json");
            let prior = json!({
                "hooks": {
                    "foreign": [{"command": format!("foreign __hook {section}")}]
                }
            });
            fs::write(&path, serde_json::to_vec(&prior).expect("prior JSON")).expect("prior hooks");
            install_owned_json(&path, section, "shared hooks", |prior| {
                let mut installed = prior.cloned().expect("prior hook document");
                installed["acyclicOwned"] = json!({"command":"acyclic"});
                Ok(installed)
            })
            .expect("install owned hooks");
            let installed = fs::read(&path).expect("installed hooks");
            let installed_value: Value =
                serde_json::from_slice(&installed).expect("installed JSON");
            assert_eq!(installed_value["hooks"], prior["hooks"]);

            let mut edited = installed_value;
            edited["acyclicOwned"]["command"] = json!("user-edited");
            fs::write(&path, serde_json::to_vec(&edited).expect("edited JSON"))
                .expect("edit hooks");
            assert!(remove_owned_json(&path, section, "shared hooks").is_err());
            assert_eq!(
                serde_json::from_slice::<Value>(&fs::read(&path).expect("preserved edit"))
                    .expect("preserved JSON"),
                edited
            );

            fs::write(&path, installed).expect("restore installed hooks");
            remove_owned_json(&path, section, "shared hooks").expect("owned uninstall");
            assert_eq!(
                serde_json::from_slice::<Value>(&fs::read(&path).expect("restored prior"))
                    .expect("restored JSON"),
                prior
            );
        }
    }

    #[test]
    fn claude_hook_reinstall_replaces_stale_acyclic_commands_without_owning_foreign_hooks() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("settings.json");
        let stale = |event: &str, matcher: bool| {
            let mut group = serde_json::Map::new();
            if matcher {
                group.insert("matcher".to_owned(), json!(".*"));
            }
            group.insert(
                "hooks".to_owned(),
                json!([{
                    "type": "command",
                    "command": format!("'/deleted/plugin/bin/acyclic' __hook claude-code {event}"),
                    "timeout": 120,
                    "statusMessage": "Acyclic is routing the workspace"
                }]),
            );
            Value::Object(group)
        };
        let foreign = json!({
            "hooks": [{"type":"command", "command":"foreign", "timeout":5}]
        });
        let similar_but_foreign = json!({
            "hooks": [{
                "type":"command",
                "command":"acyclic __hook claude-code SessionStart",
                "timeout":120,
                "statusMessage":"different"
            }]
        });
        let prior = json!({
            "theme": "dark",
            "hooks": {
                "SessionStart": [stale("SessionStart", false), foreign, similar_but_foreign],
                "PreToolUse": [stale("PreToolUse", true)],
                "SessionEnd": [stale("SessionEnd", false)]
            }
        });
        fs::write(&path, serde_json::to_vec(&prior).expect("prior JSON")).expect("prior hooks");

        install_owned_json_normalized(
            &path,
            "claude-hooks",
            "Claude Code hooks",
            normalize_claude_hook_document,
            |prior| {
                let mut installed = prior.cloned().expect("normalized prior");
                installed["currentAcyclic"] = json!(true);
                Ok(installed)
            },
        )
        .expect("replace stale hooks");

        let installed: Value = serde_json::from_slice(&fs::read(&path).expect("installed hooks"))
            .expect("installed JSON");
        assert_eq!(installed["theme"], "dark");
        assert_eq!(
            installed["hooks"]["SessionStart"],
            json!([foreign, similar_but_foreign])
        );
        assert!(installed["hooks"].get("PreToolUse").is_none());
        assert!(installed["hooks"].get("SessionEnd").is_none());
        assert_eq!(claude_hook_timeout("SessionEnd"), 3);
        assert_eq!(claude_hook_timeout("PreToolUse"), 120);

        remove_owned_json(&path, "claude-hooks", "Claude Code hooks")
            .expect("uninstall current hooks");
        let restored: Value = serde_json::from_slice(&fs::read(&path).expect("restored hooks"))
            .expect("restored JSON");
        assert_eq!(restored["theme"], "dark");
        assert_eq!(
            restored["hooks"]["SessionStart"],
            json!([foreign, similar_but_foreign])
        );
        assert!(restored["hooks"].get("PreToolUse").is_none());
        assert!(restored.get("currentAcyclic").is_none());
    }

    #[test]
    fn codex_install_preserves_old_or_disabled_plugin_state() {
        assert!(
            validate_existing_codex_plugin(&json!({
                "version": "0.0.0",
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
        let oversized = temporary.path().join("oversized");
        let file = fs::File::create(&oversized).expect("oversized state");
        file.set_len(MAXIMUM_ADAPTER_STATE_BYTES + ADAPTER_STATE_HEADER_BYTES as u64 + 1)
            .expect("extend oversized state");
        assert!(
            read_state_slot(&oversized)
                .err()
                .expect("oversized state must fail")
                .contains("byte bound")
        );

        let mut state = AdapterState {
            version: ADAPTER_STATE_VERSION,
            ..AdapterState::default()
        };
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
        state.roots.insert(
            "wrong-key".to_owned(),
            RootBinding {
                root_id: [1; 16],
                path: temporary.path().to_path_buf(),
                repository_workspace_id: [2; 16],
                source_identity: [3; 16],
                source_epoch: 0,
                native_root_identity: [4; 16],
            },
        );
        assert!(
            validate_state_bounds(&state)
                .expect_err("mismatched root map key must fail")
                .contains("typed root identity")
        );
        state.roots.clear();
        state.root_session_id = "x"
            .repeat(usize::try_from(MAXIMUM_ADAPTER_STATE_BYTES).expect("state bound fits usize"));
        assert!(
            save_state(temporary.path(), &state, Survives::PowerLoss)
                .expect_err("oversized state persistence must fail")
                .contains("byte bound")
        );
        assert!(
            ADAPTER_STATE_SLOTS
                .iter()
                .all(|slot| !temporary.path().join(slot).exists())
        );
    }

    #[test]
    fn adapter_state_requires_the_current_explicit_schema() {
        let incomplete = serde_json::from_value::<AdapterState>(json!({
            "version": 1,
            "root_session_id": "session",
            "root_agent_id": "agent",
            "root_path": "root",
            "root_workspace_name": "workspace",
            "root_context_id": vec![0; 16],
            "root_id": vec![0; 16],
            "routes": {},
            "turns": {},
            "leases": {},
            "pending": []
        }));
        assert!(
            incomplete.is_err(),
            "state without the current schema must be refused"
        );
        let unsupported = AdapterState {
            version: ADAPTER_STATE_VERSION + 1,
            active: true,
            ..AdapterState::default()
        };
        assert!(validate_state_version(&unsupported).is_err());
    }

    #[test]
    fn adapter_state_loads_the_last_completed_save_despite_any_torn_write() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path();
        let named = |name: &str| AdapterState {
            version: ADAPTER_STATE_VERSION,
            root_session_id: name.to_owned(),
            ..AdapterState::default()
        };
        let loaded = || load_saved_state(data).map(|state| state.root_session_id);
        assert_eq!(loaded(), Ok(String::new()));
        for name in ["first", "second", "third", "fourth"] {
            save_state(data, &named(name), Survives::PowerLoss).expect("adapter state save");
            assert_eq!(loaded().as_deref(), Ok(name));
        }
        let target = data.join(ADAPTER_STATE_SLOTS[0]);
        let written = fs::read(&target).expect("newest slot");
        let mut flipped = written.clone();
        *flipped.last_mut().expect("payload byte") ^= 1;
        let mut stale_tail = written.clone();
        stale_tail.extend_from_slice(b"}}");
        let prefixes = [
            0,
            1,
            ADAPTER_STATE_HEADER_BYTES - 1,
            ADAPTER_STATE_HEADER_BYTES,
            written.len() - 1,
        ]
        .map(|length| written[..length].to_vec());
        for torn in prefixes.into_iter().chain([flipped, stale_tail]) {
            fs::write(&target, torn).expect("torn slot");
            assert_eq!(loaded().as_deref(), Ok("third"));
            save_state(data, &named("fifth"), Survives::PowerLoss)
                .expect("save over the torn slot");
            assert_eq!(loaded().as_deref(), Ok("fifth"));
        }
        for slot in ADAPTER_STATE_SLOTS {
            fs::write(data.join(slot), b"torn").expect("torn slot");
        }
        assert!(
            loaded().is_err(),
            "state without a completed save must fail closed"
        );
    }

    #[test]
    fn unflushed_adapter_saves_never_touch_the_newest_flushed_save() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path();
        let named = |name: &str| AdapterState {
            version: ADAPTER_STATE_VERSION,
            root_session_id: name.to_owned(),
            ..AdapterState::default()
        };
        let loaded = || load_saved_state(data).map(|state| state.root_session_id);
        let flushed_slots = || {
            ADAPTER_STATE_SLOTS[..2]
                .iter()
                .map(|slot| fs::read(data.join(slot)).ok())
                .collect::<Vec<_>>()
        };
        let unflushed = data.join(ADAPTER_STATE_SLOTS[2]);

        // Losing every unflushed save before the first flushed one leaves the
        // state before any save.
        save_state(data, &named("volatile"), Survives::ServiceCrash).expect("unflushed save");
        assert_eq!(loaded().as_deref(), Ok("volatile"));
        fs::write(&unflushed, b"lost").expect("lose unflushed save");
        assert_eq!(loaded(), Ok(String::new()));

        save_state(data, &named("first"), Survives::PowerLoss).expect("flushed save");
        let durable = flushed_slots();
        for name in ["second", "third", "fourth"] {
            save_state(data, &named(name), Survives::ServiceCrash).expect("unflushed save");
            assert_eq!(loaded().as_deref(), Ok(name));
            assert_eq!(flushed_slots(), durable);
        }
        fs::write(&unflushed, b"lost").expect("lose unflushed save");
        assert_eq!(loaded().as_deref(), Ok("first"));

        save_state(data, &named("fifth"), Survives::ServiceCrash).expect("unflushed save");
        save_state(data, &named("sixth"), Survives::PowerLoss).expect("flushed save");
        assert_eq!(
            loaded().as_deref(),
            Ok("sixth"),
            "a later flushed save wins"
        );
        save_state(data, &named("seventh"), Survives::ServiceCrash).expect("unflushed save");
        let intact = fs::read(&unflushed).expect("unflushed slot");
        fs::write(&unflushed, &intact[..intact.len() - 1]).expect("tear unflushed save");
        assert_eq!(loaded().as_deref(), Ok("sixth"));
    }

    #[test]
    fn the_unflushed_slot_entry_is_flushed_until_known_durable() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path();
        let state = AdapterState {
            version: ADAPTER_STATE_VERSION,
            root_session_id: "session".to_owned(),
            ..AdapterState::default()
        };
        let (_, mut slots) = load_state(data).expect("fresh state");
        slots
            .save(data, &state, Survives::ServiceCrash)
            .expect("unflushed save");
        assert!(!slots.durable_entry[2], "a created entry is not durable");
        slots.flush_unflushed(data).expect("first flush");
        assert!(slots.durable_entry[2], "the first flush syncs the entry");
        // Another process cannot tell whether the entry was synced, so it
        // syncs it with its first flush, which loading performs here.
        assert!(!StateSlots::read(data).expect("slots").0.durable_entry[2]);
        let (_, reloaded) = load_state(data).expect("reloaded state");
        assert!(reloaded.durable_entry[2]);
    }

    #[test]
    fn a_session_saves_from_what_it_knows_of_its_slots() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path();
        let named = |name: &str| AdapterState {
            version: ADAPTER_STATE_VERSION,
            root_session_id: name.to_owned(),
            ..AdapterState::default()
        };
        let loaded = || load_saved_state(data).map(|state| state.root_session_id);
        let on_disk = || StateSlots::read(data).expect("slots").0;
        let (_, mut slots) = load_state(data).expect("fresh state");
        for (index, name) in ["a", "b", "c", "d", "e", "f", "g", "h"]
            .into_iter()
            .enumerate()
        {
            let survives = if index % 3 == 1 {
                Survives::ServiceCrash
            } else {
                Survives::PowerLoss
            };
            slots
                .save(data, &named(name), survives)
                .expect("adapter state save");
            assert_eq!(loaded().as_deref(), Ok(name));
            let read = on_disk();
            assert_eq!(read.flushed, slots.flushed);
            assert_eq!(read.durable_entry, slots.durable_entry);
            assert_eq!(read.last_generation, slots.last_generation);
        }

        // A write that fails stays the target of the next flushed save, so the
        // newest flushed save is never overwritten, and it consumes its
        // generation.
        let newest = data.join(ADAPTER_STATE_SLOTS[slots.newest_flushed()]);
        let target = data.join(ADAPTER_STATE_SLOTS[1 - slots.newest_flushed()]);
        let durable = fs::read(&newest).expect("newest flushed save");
        fs::remove_file(&target).expect("remove target slot");
        fs::create_dir(&target).expect("block target slot");
        let before = slots.last_generation;
        assert!(
            slots
                .save(data, &named("failed"), Survives::PowerLoss)
                .is_err()
        );
        assert_eq!(slots.last_generation, before + 1);
        fs::remove_dir(&target).expect("unblock target slot");
        slots
            .save(data, &named("retried"), Survives::PowerLoss)
            .expect("retried save");
        assert_eq!(fs::read(&newest).expect("newest flushed save"), durable);
        assert_eq!(loaded().as_deref(), Ok("retried"));
        assert_eq!(on_disk().last_generation, before + 2);
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
    fn rpc_line_reader_is_bounded_and_handles_stream_boundaries() {
        let mut input = io::Cursor::new(b"first\r\nsecond\nlast".to_vec());
        assert_eq!(
            read_bounded_rpc_line(&mut input).expect("first frame"),
            Some(b"first".to_vec())
        );
        assert_eq!(
            read_bounded_rpc_line(&mut input).expect("second frame"),
            Some(b"second".to_vec())
        );
        assert_eq!(
            read_bounded_rpc_line(&mut input).expect("final frame"),
            Some(b"last".to_vec())
        );
        assert_eq!(read_bounded_rpc_line(&mut input).expect("eof"), None);

        let mut oversized = io::Cursor::new(vec![b'x'; MAXIMUM_CONTROL_MESSAGE_BYTES + 1]);
        assert!(
            read_bounded_rpc_line(&mut oversized)
                .expect_err("oversized frame must fail closed")
                .contains("maximum frame size")
        );
    }

    #[test]
    fn doctor_requires_an_exact_binary_bound_platform_receipt() {
        let kind = match env::consts::OS {
            "linux" => "linux-fuse",
            "macos" => "macos-nfs",
            "windows" => "windows-projfs",
            _ => return,
        };
        let provider_process_io_observable = !cfg!(windows);
        let canonical = json!({
            "schema":"acyclic-native-mount-qualification-v2",
            "os":env::consts::OS,
            "arch":env::consts::ARCH,
            "coverage":[
                "create-read-write","atomic-save","rename-delete","rename-before-hydration",
                "nested-paths","large-directory-paging",
                "concurrent-handles","watchers","crash-detach-recovery",
                "mount-restoration","hard-links","symbolic-links-reparse-points",
                "metadata","case-behavior","escape-attempts",
                "root-checkout-untouched","git-administration-untouched"
            ],
            "capability":{
                "kind":kind,"available":true,"writable":true,
                "provider_process_io_observable":provider_process_io_observable,
                "session_isolation":"SharedProcess","unavailable_reason":null
            },
            "required_kind":kind,
            "release_version":env!("CARGO_PKG_VERSION"),
            "executable_blake3":"exact-digest",
            "passed":true,
            "cases":[
                {"name":"real-mount-mutation-matrix","status":"passed","elapsed_ms":1,"reason":null},
                {"name":"crash-detach-recovery","status":"passed","elapsed_ms":1,"reason":null},
                {"name":"checkout-and-git-untouched","status":"passed","elapsed_ms":1,"reason":null}
            ]
        });
        assert!(valid_platform_receipt(&canonical, "exact-digest"));
        assert!(!valid_platform_receipt(&canonical, "other-digest"));
        let mut expanded = canonical.clone();
        expanded["coverage"]
            .as_array_mut()
            .expect("coverage")
            .push(json!("future-coverage"));
        expanded["cases"]
            .as_array_mut()
            .expect("cases")
            .push(json!({
                "name":"future-case","status":"passed","elapsed_ms":1,"reason":null
            }));
        assert!(valid_platform_receipt(&expanded, "exact-digest"));
        let mut duplicate_case = expanded.clone();
        duplicate_case["cases"]
            .as_array_mut()
            .expect("cases")
            .push(json!({
                "name":"future-case","status":"passed","elapsed_ms":1,"reason":null
            }));
        assert!(!valid_platform_receipt(&duplicate_case, "exact-digest"));
        expanded["cases"][3]["status"] = json!("failed");
        assert!(!valid_platform_receipt(&expanded, "exact-digest"));
        let mut missing_coverage = canonical.clone();
        missing_coverage["coverage"]
            .as_array_mut()
            .expect("coverage")
            .pop();
        assert!(!valid_platform_receipt(&missing_coverage, "exact-digest"));
        let mut incomplete = canonical.clone();
        incomplete["cases"].as_array_mut().expect("cases").pop();
        assert!(!valid_platform_receipt(&incomplete, "exact-digest"));
        let mut extended = canonical;
        extended["untrusted"] = json!(true);
        assert!(!valid_platform_receipt(&extended, "exact-digest"));
    }

    #[test]
    fn host_lifecycle_arguments_are_exact_before_mutation() {
        let strings = |values: &[&str]| {
            values
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
        };

        let install = strings(&["codex", "--project"]);
        assert_eq!(parse_install_arguments(&install), Ok(("codex", true)));
        let detected = strings(&["--detected"]);
        assert_eq!(
            parse_install_arguments(&detected),
            Ok(("--detected", false))
        );
        for invalid in [
            strings(&[]),
            strings(&["--project", "codex"]),
            strings(&["--detected", "--project"]),
            strings(&["codex", "unexpected"]),
            strings(&["codex", "--project", "--project"]),
        ] {
            assert!(parse_install_arguments(&invalid).is_err());
        }

        let uninstall = strings(&["codex", "--purge"]);
        assert_eq!(parse_uninstall_arguments(&uninstall), Ok(("codex", true)));
        for invalid in [
            strings(&[]),
            strings(&["--purge", "codex"]),
            strings(&["codex", "unexpected"]),
            strings(&["codex", "--purge", "--purge"]),
        ] {
            assert!(parse_uninstall_arguments(&invalid).is_err());
        }

        assert_eq!(parse_read_only_arguments(&strings(&[])), Ok(false));
        assert_eq!(parse_read_only_arguments(&strings(&["--json"])), Ok(true));
        for invalid in [
            strings(&["unexpected"]),
            strings(&["--json", "unexpected"]),
            strings(&["--json", "--json"]),
        ] {
            assert!(parse_read_only_arguments(&invalid).is_err());
        }
    }

    #[test]
    fn native_hook_identities_are_nonempty_bounded_and_control_free() {
        assert_eq!(
            hook_id(
                &json!({"session_id":"session-1"}),
                "session_id",
                "sessionId"
            )
            .expect("valid session ID"),
            "session-1"
        );
        for invalid in [String::new(), "line\nbreak".to_owned(), "x".repeat(513)] {
            assert!(validate_hook_id("session_id", invalid).is_err());
        }
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
