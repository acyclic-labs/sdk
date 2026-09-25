//! Customer-facing native mount composition over the canonical checkout.

use super::adapter::HostCaptureScope;
use super::{
    CheckoutMountSource, LazyMountSource, MountPath, NativeMountError, NativeMountRequest,
    NativeMountSession, SharedCheckout, capture_root_identity, mount_native,
};
use crate::demand::{DemandSource, SourceNode, SourceNodeKind};
use crate::kernel::FileKind;
use crate::kernel::NamespacePath;
use crate::model::{CheckoutMode, GenerationSelector};
use crate::native_capture::NativeViewBaseline;
use crate::workspace::{Workspace, WorkspaceError, customer_path};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, GenerationId, IdempotencyKey, LazyLookup, LazyWorkspace,
    LazyWorkspaceError, LazyWorkspaceStore, MountId, MountSourceError, NativeRootIdentity,
    VolumeId,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// When authored native mutations become a durable workspace generation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MountPublication {
    /// Publish on native close/fsync/rename boundaries and explicit sync.
    #[default]
    CloseAndSync,
    /// Publish every independently admitted native mutation.
    PerMutation,
    /// Publish only through [`Mount::sync`] or orderly [`Mount::unmount`].
    Manual,
}

/// Explicit customer mount configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountOptions {
    writable: bool,
    subdirectory: String,
    publication: MountPublication,
}

impl MountOptions {
    /// Creates a writable tracking-safe sparse mount with close/fsync publication.
    #[must_use]
    pub fn read_write() -> Self {
        Self {
            writable: true,
            subdirectory: "/".to_owned(),
            publication: MountPublication::CloseAndSync,
        }
    }

    /// Creates a pinned immutable mount.
    #[must_use]
    pub fn read_only() -> Self {
        Self {
            writable: false,
            subdirectory: "/".to_owned(),
            publication: MountPublication::Manual,
        }
    }

    /// Projects one exact workspace directory as the native mount root.
    #[must_use]
    pub fn subdirectory(mut self, path: impl Into<String>) -> Self {
        self.subdirectory = path.into();
        self
    }

    /// Selects the exact publication policy for writable authored effects.
    #[must_use]
    pub const fn publication(mut self, publication: MountPublication) -> Self {
        self.publication = publication;
        self
    }
}

/// One process-owned customer mount over a canonical workspace checkout.
pub struct Mount<A, O> {
    source: Arc<CheckoutMountSource<A, O>>,
    session: Mutex<Option<NativeMountSession>>,
    destination: PathBuf,
}

/// One process-owned native mount over a demand-backed sparse workspace.
pub struct LazyMount<A, O, D, S> {
    source: Arc<LazyMountSource<A, O, D, S>>,
    session: Mutex<Option<NativeMountSession>>,
    destination: PathBuf,
}

/// SDK-owned capture and publication boundary for a pinned native directory.
///
/// The caller owns the physical volume-root directory and supplies exact
/// changed paths. A selected subdirectory retains its namespace path below
/// that physical root; its contents are not remapped to the root itself.
/// Filesystem semantics and fenced publication use an exact-generation
/// checkout. This handle never rebinds to the live lazy source or starts a
/// mount driver.
pub struct LazyWorkingSet<A, O> {
    source: Arc<CheckoutMountSource<A, O>>,
    workspace: Workspace<A, O>,
    source_root: PathBuf,
    source_identity: NativeRootIdentity,
    selected_root: PathBuf,
    expected_generation: GenerationId,
    baseline: Arc<NativeViewBaseline>,
    capture_budget: crate::WorkBudget,
}

struct AbortTaskOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortTaskOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<A, O> LazyWorkingSet<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Revalidates a prepared native view immediately before a quiescent
    /// presentation switch. This is optimistic admission, not a lock on the
    /// workspace head: publication remains fenced by the operation permit.
    /// It reads only the selected directory metadata and the current head.
    pub async fn validate_for_presentation(&self) -> Result<(), MountLifecycleError> {
        let expected = self.expected_generation;
        if self.workspace.head().await?.id() != expected {
            return Err(MountLifecycleError::Workspace(
                WorkspaceError::StaleGeneration,
            ));
        }
        let source_root = self.source_root.clone();
        let selected_root = self.selected_root.clone();
        let identity = self.source_identity;
        tokio::task::spawn_blocking(move || {
            validate_native_working_set_root(&source_root, &selected_root, identity)
        })
        .await
        .map_err(|error| MountSourceError::Engine(error.to_string()))?
        .map_err(MountLifecycleError::Source)?;
        if self.workspace.head().await?.id() != expected {
            return Err(MountLifecycleError::Workspace(
                WorkspaceError::StaleGeneration,
            ));
        }
        Ok(())
    }

    /// Captures only the supplied changed paths from a native working directory.
    ///
    /// The source root is authenticated by the core capture path. This is a
    /// path hint, not proof that other native writes did not occur. Call this
    /// at an operation boundary, then publish with [`Self::sync_with_permit`].
    pub async fn capture_host_paths(&self, paths: &[MountPath]) -> Result<(), MountLifecycleError> {
        self.source
            .capture_host_with_identity_budgeted(
                &self.source_root,
                paths,
                self.source_identity,
                HostCaptureScope::ExactPaths,
                Arc::clone(&self.baseline),
                self.capture_budget,
            )
            .await
            .map_err(MountLifecycleError::Source)
    }

    /// Reconciles a complete selected subtree after a watcher overflow or
    /// missed event. Both host and checkout descendants participate, so
    /// creations and deletions are captured without scanning sibling trees.
    pub async fn capture_host_subtree(&self, root: &MountPath) -> Result<(), MountLifecycleError> {
        self.source
            .capture_host_with_identity_budgeted(
                &self.source_root,
                std::slice::from_ref(root),
                self.source_identity,
                HostCaptureScope::Subtree,
                Arc::clone(&self.baseline),
                self.capture_budget,
            )
            .await
            .map_err(MountLifecycleError::Source)
    }

    /// Reconciles the selected subtree and publishes under an active, fenced
    /// operation permit. Path hints alone cannot establish completeness: a
    /// missing or delayed notification must not silently omit a native write.
    pub async fn sync_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountLifecycleError> {
        self.capture_host_subtree(&MountPath::root()).await?;
        self.source
            .sync_async_with_permit(permit)
            .await
            .map_err(MountLifecycleError::Source)
    }
}

fn flush_session_callbacks(
    session: &Mutex<Option<NativeMountSession>>,
) -> Result<(), MountLifecycleError> {
    with_live_session(session, NativeMountSession::flush_callbacks)
}

/// Waits until the kernel caches nothing a change to the source superseded.
fn revalidate_session(
    session: &Mutex<Option<NativeMountSession>>,
) -> Result<(), MountLifecycleError> {
    with_live_session(session, NativeMountSession::revalidate)
}

fn with_live_session(
    session: &Mutex<Option<NativeMountSession>>,
    operation: impl FnOnce(&NativeMountSession) -> Result<(), NativeMountError>,
) -> Result<(), MountLifecycleError> {
    let owner = match session.lock() {
        Ok(owner) => owner,
        Err(poisoned) => poisoned.into_inner(),
    };
    owner
        .as_ref()
        .map_or(Ok(()), operation)
        .map_err(MountLifecycleError::Native)
}

impl<A, O, D, S> LazyMount<A, O, D, S> {
    /// Exact mounted host path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.destination
    }
}

impl<A, O> Mount<A, O> {
    /// Exact mounted host path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.destination
    }
}

impl<A, O> Mount<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Publishes all pending authored effects with one fenced generation.
    ///
    /// # Errors
    ///
    /// Returns a typed publication failure without discarding pending state.
    pub async fn sync(&self) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source
            .sync_async()
            .await
            .map_err(MountLifecycleError::Source)?;
        Ok(())
    }

    /// Publishes all pending effects under one active operation lease.
    pub async fn sync_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source
            .sync_async_with_permit(permit)
            .await
            .map_err(MountLifecycleError::Source)
    }

    /// Publishes pending writes and advances the live checkout to workspace head.
    ///
    /// This keeps a direct-parent agent mount coherent after a child join
    /// without exposing remount orchestration to adapters.
    pub async fn refresh(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.advance_to_head().await
    }

    /// Advances an already-clean live checkout to the workspace head.
    ///
    /// Use this after an external, fenced workspace publication when the
    /// caller already synchronized the mount before that publication. Unlike
    /// [`Self::refresh`], this does not try to republish against the newer head.
    ///
    /// # Errors
    ///
    /// Returns a typed conflict or storage failure without changing the
    /// mounted generation.
    pub async fn advance_to_head(&self) -> Result<(), MountLifecycleError> {
        let advanced = self.source.advance_to_head_async().await;
        // Even a failed advance may have moved part of the view.
        revalidate_session(&self.session)?;
        advanced.map_err(MountLifecycleError::Source)
    }

    /// Publishes all pending effects on the source's dedicated callback runtime.
    ///
    /// # Errors
    ///
    /// Returns a typed publication failure without discarding pending state.
    pub fn sync_blocking(&self) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source.sync().map_err(MountLifecycleError::Source)
    }

    /// Publishes pending effects and detaches the native namespace exactly once.
    ///
    /// # Errors
    ///
    /// Publication failure leaves the live mount owned by this handle. A detach
    /// failure retains driver state and its destination fence for drop retry.
    pub async fn unmount(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.detach()
    }

    /// Detaches and cancels this mount without publishing pending callbacks.
    ///
    /// This is the fail-closed recovery path after a fenced publication. The
    /// caller may remount from the last durable generation.
    pub fn abandon(&self) -> Result<(), MountLifecycleError> {
        self.detach()
    }

    /// Synchronously publishes and detaches for foreign-runtime worker threads.
    ///
    /// # Errors
    ///
    /// Publication or detach failure retains the complete owner for retry.
    pub fn unmount_blocking(&self) -> Result<(), MountLifecycleError> {
        self.sync_blocking()?;
        self.detach()
    }

    fn detach(&self) -> Result<(), MountLifecycleError> {
        let mut owner = match self.session.lock() {
            Ok(owner) => owner,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(session) = owner.as_mut() else {
            return Ok(());
        };
        session.stop().map_err(MountLifecycleError::Native)?;
        owner.take();
        self.source.cancel();
        Ok(())
    }
}

impl<A, O, D, S> LazyMount<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Publishes all pending authored effects with one fenced generation.
    pub async fn sync(&self) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source
            .sync_async()
            .await
            .map_err(MountLifecycleError::Source)
    }

    /// Publishes all pending effects under one active operation lease.
    pub async fn sync_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source
            .sync_async_with_permit(permit)
            .await
            .map_err(MountLifecycleError::Source)
    }

    /// Publishes pending writes and advances the live checkout to workspace head.
    pub async fn refresh(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.advance_to_head().await
    }

    /// Advances an already-clean checkout to workspace head.
    pub async fn advance_to_head(&self) -> Result<(), MountLifecycleError> {
        let advanced = self.source.advance_to_head_async().await;
        // Even a failed advance may have moved part of the view.
        revalidate_session(&self.session)?;
        advanced.map_err(MountLifecycleError::Source)
    }

    /// Publishes pending effects on the source callback runtime.
    pub fn sync_blocking(&self) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source.sync().map_err(MountLifecycleError::Source)
    }

    /// Publishes pending effects and detaches the native namespace exactly once.
    pub async fn unmount(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.detach()
    }

    /// Detaches and cancels this lazy mount without publishing pending state.
    pub fn abandon(&self) -> Result<(), MountLifecycleError> {
        self.detach()
    }

    /// Synchronously publishes and detaches for foreign-runtime worker threads.
    pub fn unmount_blocking(&self) -> Result<(), MountLifecycleError> {
        self.sync_blocking()?;
        self.detach()
    }

    fn detach(&self) -> Result<(), MountLifecycleError> {
        let mut owner = match self.session.lock() {
            Ok(owner) => owner,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(session) = owner.as_mut() else {
            return Ok(());
        };
        session.stop().map_err(MountLifecycleError::Native)?;
        owner.take();
        self.source.cancel();
        Ok(())
    }
}

/// Customer mount admission, publication, and lifecycle failures.
#[derive(Debug, Error)]
pub enum MountLifecycleError {
    /// Workspace path, state, or storage failed admission.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Lazy source or sparse-overlay admission failed.
    #[error(transparent)]
    Lazy(#[from] LazyWorkspaceError),
    /// Native driver admission or teardown failed.
    #[error(transparent)]
    Native(#[from] NativeMountError),
    /// Native callback publication failed.
    #[error(transparent)]
    Source(#[from] MountSourceError),
    /// Combined exactification and materialization exceeded its work budget.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
}

impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Prepares an authenticated native working set from one exact generation.
    ///
    /// The destination must be an existing empty real directory. The SDK first
    /// exactifies only the selected subtree, materializes that exact generation,
    /// then admits the resulting directory. Preparation never starts a mount
    /// driver and fails if the workspace head advances before admission. The
    /// supplied work budget also caps each later capture or sync on the handle;
    /// those operations do not inherit the kernel-callback deadline or budget.
    #[allow(clippy::too_many_lines)]
    pub async fn prepare_native_working_set(
        &self,
        subdirectory: &str,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        permit: crate::PublicationPermit,
    ) -> Result<LazyWorkingSet<A, O>, MountLifecycleError> {
        if !options.destination.is_absolute() {
            return Err(MountLifecycleError::Source(MountSourceError::Invalid(
                "native working-set root must be absolute".to_owned(),
            )));
        }
        // Keep the cold exactification frame independent of materialization:
        // both are large state machines, and nesting them exhausts the small
        // default stacks used by some host test/runtime threads.
        let lazy = self.clone();
        let selected = subdirectory.to_owned();
        let token = cancellation.clone();
        let mut task = AbortTaskOnDrop(tokio::spawn(async move {
            lazy.exactify_subtree_with_permit(&selected, budget, &token, permit)
                .await
        }));
        let exact = (&mut task.0)
            .await
            .map_err(|error| MountSourceError::Engine(error.to_string()))??;
        let remaining = exact.work.remaining(budget)?;
        let materialized = if subdirectory == "/" {
            exact
                .value
                .materialize_with_mode(
                    options,
                    remaining,
                    cancellation,
                    super::MaterializeMode::ReconstructibleView,
                )
                .await?
        } else {
            exact
                .value
                .materialize_path_with_mode(
                    subdirectory,
                    options,
                    remaining,
                    cancellation,
                    super::MaterializeMode::ReconstructibleView,
                )
                .await?
        };
        // Admission stays inside this operation: an arbitrary directory must
        // never be rebound to a caller-claimed generation without having been
        // materialized from that exact generation first.
        let expected_generation = exact.value.id();
        let source_root = options.destination.clone();
        let identity_root = source_root.clone();
        let source_identity =
            tokio::task::spawn_blocking(move || capture_root_identity(&identity_root))
                .await
                .map_err(|error| MountSourceError::Engine(error.to_string()))?
                .map_err(|error| MountSourceError::Engine(error.to_string()))?;
        let options = MountOptions::read_write()
            .subdirectory(subdirectory)
            .publication(MountPublication::Manual);
        let (source, _, root, _) = self
            .prepare_authored_mount_source(&options, Some(expected_generation))
            .await?;
        let relative = crate::namespace_to_host_path(&root)
            .map_err(|error| MountSourceError::Engine(error.to_string()))?;
        let validated_root = source_root.clone();
        let selected_root = relative.clone();
        tokio::task::spawn_blocking(move || {
            validate_native_working_set_root(&validated_root, &selected_root, source_identity)
        })
        .await
        .map_err(|error| MountSourceError::Engine(error.to_string()))?
        .map_err(MountLifecycleError::Source)?;
        #[cfg(unix)]
        let baseline = NativeViewBaseline::new(source_identity);
        #[cfg(windows)]
        let baseline = NativeViewBaseline;
        let _ = materialized;
        let final_root = source_root.clone();
        let final_selected = relative.clone();
        tokio::task::spawn_blocking(move || {
            validate_native_working_set_root(&final_root, &final_selected, source_identity)
        })
        .await
        .map_err(|error| MountSourceError::Engine(error.to_string()))?
        .map_err(MountLifecycleError::Source)?;
        // Directory validation may wait for host I/O while another writer
        // advances the workspace. Do not return an already-stale preparation.
        if self.workspace().head().await?.id() != expected_generation {
            return Err(MountLifecycleError::Workspace(
                WorkspaceError::StaleGeneration,
            ));
        }
        Ok(LazyWorkingSet {
            source,
            workspace: self.workspace().clone(),
            source_root,
            source_identity,
            selected_root: relative,
            expected_generation,
            baseline: Arc::new(baseline),
            capture_budget: budget,
        })
    }

    async fn prepare_authored_mount_source(
        &self,
        options: &MountOptions,
        expected_generation: Option<GenerationId>,
    ) -> Result<
        (
            Arc<CheckoutMountSource<A, O>>,
            VolumeId,
            NamespacePath,
            String,
        ),
        MountLifecycleError,
    > {
        if let Some(expected) = expected_generation
            && self.workspace().head().await?.id() != expected
        {
            return Err(MountLifecycleError::Workspace(
                WorkspaceError::StaleGeneration,
            ));
        }
        let probe = self
            .workspace()
            .engine_checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned())
            .await?;
        let config = probe.volume_config();
        drop(probe);
        let root = customer_path(&options.subdirectory, config)?;
        let root_text = portable_namespace_path(&root)?;
        let selected = self.lookup(&root_text).await?;
        if !matches!(
            selected,
            LazyLookup::Authored {
                stat: crate::WorkspaceStat {
                    kind: FileKind::Directory,
                    ..
                },
                ..
            } | LazyLookup::Source(SourceNode {
                kind: SourceNodeKind::Directory,
                ..
            }) | LazyLookup::Shadow {
                record: crate::kernel::FileRecord {
                    kind: FileKind::Directory,
                    ..
                },
                ..
            }
        ) {
            return Err(MountLifecycleError::Workspace(WorkspaceError::NotDirectory));
        }
        if let LazyLookup::Source(node) = selected {
            if expected_generation.is_some() {
                return Err(MountLifecycleError::Workspace(
                    WorkspaceError::StaleGeneration,
                ));
            }
            let expected_source = self.source_file_id(&node);
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"acyclic-fs-lazy-mount-root-v1\0");
            hasher.update(&self.workspace().id().into_bytes());
            hasher.update(root_text.as_bytes());
            hasher.update(&node.version.0);
            let mut key = [0_u8; 16];
            key.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
            self.promote_exact(
                &root_text,
                expected_source,
                0,
                IdempotencyKey::from_bytes(key),
            )
            .await?;
        }

        let mode = if options.writable {
            CheckoutMode::tracking_transaction()
        } else {
            CheckoutMode::read_only_pinned()
        };
        let checkout = self
            .workspace()
            .engine_checkout(
                expected_generation.map_or(GenerationSelector::Head, GenerationSelector::Exact),
                mode,
            )
            .await?;
        let shared = Arc::new(SharedCheckout::with_publication(
            checkout,
            options.publication,
        ));
        let authored = Arc::new(CheckoutMountSource::new_at(shared, config, root.clone())?);
        if let Some(expected) = expected_generation
            && self.workspace().head().await?.id() != expected
        {
            return Err(MountLifecycleError::Workspace(
                WorkspaceError::StaleGeneration,
            ));
        }
        Ok((authored, self.workspace().id().volume_id(), root, root_text))
    }

    /// Mounts a demand-backed workspace without scanning its source tree.
    ///
    /// Admission observes only the selected root node. Unresolved descendants
    /// remain source-backed until a native callback addresses them.
    pub async fn mount(
        &self,
        destination: impl Into<PathBuf>,
        options: MountOptions,
    ) -> Result<LazyMount<A, O, D, S>, MountLifecycleError> {
        if options.writable && options.publication == MountPublication::PerMutation {
            return Err(MountLifecycleError::Source(MountSourceError::Unsupported(
                "lazy source whiteouts publish with a checkout boundary, not per callback"
                    .to_owned(),
            )));
        }
        let (authored, volume_id, _, root_text) =
            self.prepare_authored_mount_source(&options, None).await?;
        let source = Arc::new(LazyMountSource::new(
            Arc::new(self.clone()),
            authored,
            root_text,
        )?);
        let destination = destination.into();
        let session = mount_native(
            NativeMountRequest {
                mount_id: MountId::new(),
                volume_id,
                destination: destination.clone(),
                writable: options.writable,
            },
            Arc::clone(&source) as Arc<dyn super::MountFilesystem>,
        )?;
        Ok(LazyMount {
            source,
            session: Mutex::new(Some(session)),
            destination,
        })
    }
}

fn validate_native_working_set_root(
    source_root: &Path,
    selected_root: &Path,
    expected_identity: NativeRootIdentity,
) -> Result<(), MountSourceError> {
    let root = crate::native_host::HostRoot::open(source_root)
        .map_err(|error| MountSourceError::Engine(error.to_string()))?;
    if root.identity() != expected_identity {
        return Err(MountSourceError::Stale);
    }
    let metadata = root
        .symlink_metadata_held(selected_root)
        .map_err(|error| MountSourceError::Engine(error.to_string()))?;
    if !metadata.is_dir() {
        return Err(MountSourceError::Invalid(
            "selected native working-set directory is absent".to_owned(),
        ));
    }
    Ok(())
}

fn portable_namespace_path(
    path: &crate::kernel::NamespacePath,
) -> Result<String, MountLifecycleError> {
    let mut value = String::new();
    for component in path.components() {
        value.push('/');
        value.push_str(std::str::from_utf8(component.as_bytes()).map_err(|_| {
            MountLifecycleError::Source(MountSourceError::Unsupported(
                "lazy portable mounts cannot root at a non-UTF-8 path".to_owned(),
            ))
        })?);
    }
    if value.is_empty() {
        value.push('/');
    }
    Ok(value)
}

impl<A, O> Workspace<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Mounts the complete workspace or one exact subtree as a native directory.
    ///
    /// Admission authenticates only the selected root frontier; it never scans
    /// the workspace or materializes file bodies.
    ///
    /// # Errors
    ///
    /// Rejects invalid or non-directory roots, unavailable host capabilities,
    /// busy/nonempty destinations, and storage/authentication failures.
    pub async fn mount(
        &self,
        destination: impl Into<PathBuf>,
        options: MountOptions,
    ) -> Result<Mount<A, O>, MountLifecycleError> {
        let mode = if options.writable {
            CheckoutMode::tracking_transaction()
        } else {
            CheckoutMode::read_only_pinned()
        };
        let mut checkout = self.engine_checkout(GenerationSelector::Head, mode).await?;
        let config = checkout.volume_config();
        let root = customer_path(&options.subdirectory, config)?;
        let selected = checkout
            .lookup_no_follow(
                &root,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record
            .ok_or(WorkspaceError::NotFound)?;
        if selected.kind != FileKind::Directory {
            return Err(MountLifecycleError::Workspace(WorkspaceError::NotDirectory));
        }
        let shared = Arc::new(SharedCheckout::with_publication(
            checkout,
            options.publication,
        ));
        let source = Arc::new(CheckoutMountSource::new_at(shared, config, root)?);
        let destination = destination.into();
        let session = mount_native(
            NativeMountRequest {
                mount_id: MountId::new(),
                volume_id: self.id().volume_id(),
                destination: destination.clone(),
                writable: options.writable,
            },
            Arc::clone(&source) as Arc<dyn super::MountFilesystem>,
        )?;
        Ok(Mount {
            source,
            session: Mutex::new(Some(session)),
            destination,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::demand::native::NativeDemandSource;
    use crate::model::{FilesystemProfile, VolumeLimits};
    use crate::{Fs, MemoryLazyWorkspaceStore, PublicationPermit};

    #[tokio::test]
    async fn prepared_subtree_captures_native_edit_and_rejects_stale_head()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("hot"))?;
        std::fs::write(source.path().join("hot/file.txt"), b"base")?;
        std::fs::create_dir(source.path().join("hot/sub"))?;
        std::fs::write(source.path().join("hot/sub/child.txt"), b"old child")?;
        let native_view = tempfile::tempdir()?;
        let fs = Fs::memory();
        let demand = Arc::new(
            NativeDemandSource::open(
                source.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let lazy = LazyWorkspace::attach(
            &fs,
            "prepared-native-working-set",
            demand,
            MemoryLazyWorkspaceStore::default(),
        )
        .await?;
        let before_invalid = lazy.workspace().head().await?.id();
        assert!(matches!(
            lazy.prepare_native_working_set(
                "/hot",
                &crate::MaterializeOptions::native("relative-native-view"),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                PublicationPermit::Unrestricted,
            )
            .await,
            Err(MountLifecycleError::Source(MountSourceError::Invalid(_)))
        ));
        assert_eq!(lazy.workspace().head().await?.id(), before_invalid);
        let mut working_set = lazy
            .prepare_native_working_set(
                "/hot",
                &crate::MaterializeOptions::native(native_view.path()),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                PublicationPermit::Unrestricted,
            )
            .await?;
        let generation = working_set.expected_generation;
        let working = native_view.path();
        working_set.validate_for_presentation().await?;
        working_set.capture_budget = crate::WorkBudget::default();
        assert!(
            working_set
                .capture_host_subtree(&MountPath::root())
                .await
                .is_err()
        );
        assert_eq!(lazy.workspace().head().await?.id(), generation);
        working_set.capture_budget = crate::WorkBudget::UNBOUNDED;
        let name = if cfg!(windows) {
            "file.txt"
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect()
        } else {
            b"file.txt".to_vec()
        };
        let unchanged_head = lazy.workspace().head().await?.id();
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert_eq!(lazy.workspace().head().await?.id(), unchanged_head);
        #[cfg(unix)]
        let canonical_before = lazy
            .workspace()
            .head()
            .await?
            .stat("/hot/file.txt")
            .await?
            .metadata;
        #[cfg(unix)]
        {
            working_set
                .capture_host_paths(&[MountPath::root().child(name.clone())])
                .await?;
            working_set
                .sync_with_permit(PublicationPermit::Unrestricted)
                .await?;
            let canonical_after = lazy
                .workspace()
                .head()
                .await?
                .stat("/hot/file.txt")
                .await?
                .metadata;
            assert_eq!(canonical_after.changed_ns, canonical_before.changed_ns);
            #[cfg(target_os = "linux")]
            assert_eq!(canonical_after.created_ns, canonical_before.created_ns);
        }
        #[cfg(target_os = "linux")]
        {
            let accessed = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
            std::fs::File::open(working.join("hot/file.txt"))?
                .set_times(std::fs::FileTimes::new().set_accessed(accessed))?;
            working_set
                .sync_with_permit(PublicationPermit::Unrestricted)
                .await?;
            let canonical = lazy
                .workspace()
                .head()
                .await?
                .stat("/hot/file.txt")
                .await?
                .metadata;
            assert_eq!(canonical.accessed_ns, Some(1_700_000_000_000_000_000));
        }
        std::fs::write(working.join("hot/file.txt"), b"native-edit")?;
        working_set
            .capture_host_paths(&[MountPath::root().child(name)])
            .await?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert_eq!(
            lazy.workspace().read("/hot/file.txt", 32).await?.as_ref(),
            b"native-edit"
        );
        std::fs::write(working.join("hot/created.txt"), b"new")?;
        working_set.capture_host_subtree(&MountPath::root()).await?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert_eq!(
            lazy.workspace()
                .read("/hot/created.txt", 16)
                .await?
                .as_ref(),
            b"new"
        );
        // The publication boundary must capture writes even when the caller
        // receives no change notification and supplies no path hint.
        std::fs::write(working.join("hot/unreported.txt"), b"unreported")?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert_eq!(
            lazy.workspace()
                .read("/hot/unreported.txt", 32)
                .await?
                .as_ref(),
            b"unreported"
        );
        std::fs::remove_file(working.join("hot/unreported.txt"))?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert!(
            lazy.workspace()
                .read("/hot/unreported.txt", 32)
                .await
                .is_err()
        );
        std::fs::remove_dir_all(working.join("hot/sub"))?;
        std::fs::write(working.join("hot/sub"), b"replacement")?;
        working_set.capture_host_subtree(&MountPath::root()).await?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert_eq!(
            lazy.workspace().read("/hot/sub", 32).await?.as_ref(),
            b"replacement"
        );
        assert!(
            lazy.workspace()
                .read("/hot/sub/child.txt", 32)
                .await
                .is_err()
        );
        lazy.write("/later.txt", bytes::Bytes::from_static(b"later"))
            .await?;
        lazy.write("/hot/conflict.txt", bytes::Bytes::from_static(b"parent"))
            .await?;
        std::fs::write(working.join("hot/conflict.txt"), b"child")?;
        working_set.capture_host_subtree(&MountPath::root()).await?;
        assert!(matches!(
            working_set
                .sync_with_permit(PublicationPermit::Unrestricted)
                .await,
            Err(MountLifecycleError::Source(MountSourceError::Stale))
        ));
        assert_eq!(
            lazy.workspace()
                .read("/hot/conflict.txt", 16)
                .await?
                .as_ref(),
            b"parent"
        );
        assert!(matches!(
            working_set.validate_for_presentation().await,
            Err(MountLifecycleError::Workspace(
                WorkspaceError::StaleGeneration
            ))
        ));
        assert_ne!(lazy.workspace().head().await?.id(), generation);
        let latest_view = tempfile::tempdir()?;
        let ready = lazy
            .prepare_native_working_set(
                "/hot",
                &crate::MaterializeOptions::native(latest_view.path()),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                PublicationPermit::Unrestricted,
            )
            .await?;
        ready.validate_for_presentation().await?;
        assert_eq!(
            std::fs::read(latest_view.path().join("hot/file.txt"))?,
            b"native-edit"
        );
        std::fs::rename(
            latest_view.path(),
            source.path().join("retired-native-view"),
        )?;
        std::fs::create_dir(latest_view.path())?;
        std::fs::create_dir(latest_view.path().join("hot"))?;
        assert!(matches!(
            ready.validate_for_presentation().await,
            Err(MountLifecycleError::Source(MountSourceError::Stale))
        ));
        Ok(())
    }
}
