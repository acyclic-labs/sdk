//! Customer-facing native mount composition over the canonical checkout.

use super::{
    CheckoutMountSource, LazyMountSource, MountPath, NativeMountError, NativeMountRequest,
    NativeMountSession, SharedCheckout, capture_root_identity, mount_native,
};
use crate::demand::{DemandSource, SourceNode, SourceNodeKind};
use crate::kernel::FileKind;
use crate::kernel::NamespacePath;
use crate::model::{CheckoutMode, GenerationSelector};
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

/// SDK-owned capture and publication boundary for a native working directory.
///
/// The caller owns the physical volume-root directory and supplies exact
/// changed paths. A selected subdirectory retains its namespace path below
/// that physical root; its contents are not remapped to the root itself.
/// Filesystem semantics, lazy promotion, and fenced publication stay in the
/// same source used by native mounts. No mount driver is started by this handle.
pub struct LazyWorkingSet<A, O, D, S> {
    source: Arc<LazyMountSource<A, O, D, S>>,
    workspace: Workspace<A, O>,
    source_root: PathBuf,
    source_identity: NativeRootIdentity,
    selected_root: PathBuf,
    expected_generation: Option<GenerationId>,
}

impl<A, O, D, S> LazyWorkingSet<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Revalidates a prepared native view immediately before a quiescent
    /// presentation switch. This is optimistic admission, not a lock on the
    /// workspace head: publication remains fenced by the operation permit.
    /// It reads only the selected directory metadata and the current head.
    pub async fn validate_for_presentation(&self) -> Result<(), MountLifecycleError> {
        let expected = self.expected_generation.ok_or_else(|| {
            MountLifecycleError::Source(MountSourceError::Invalid(
                "working set was not prepared at an exact generation".to_owned(),
            ))
        })?;
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
    /// The source root is authenticated by the core capture path. Call this at
    /// an operation boundary, then publish with [`Self::sync_with_permit`].
    pub async fn capture_host_paths(&self, paths: &[MountPath]) -> Result<(), MountLifecycleError> {
        let source = Arc::clone(&self.source);
        let source_root = self.source_root.clone();
        let source_identity = self.source_identity;
        let paths = paths.to_vec();
        tokio::task::spawn_blocking(move || {
            source.capture_host_paths_with_identity(&source_root, &paths, source_identity)
        })
        .await
        .map_err(|error| MountSourceError::Engine(error.to_string()))?
        .map_err(MountLifecycleError::Source)
    }

    /// Reconciles a complete selected subtree after a watcher overflow or
    /// missed event. Both host and checkout descendants participate, so
    /// creations and deletions are captured without scanning sibling trees.
    pub async fn capture_host_subtree(&self, root: &MountPath) -> Result<(), MountLifecycleError> {
        let source = Arc::clone(&self.source);
        let source_root = self.source_root.clone();
        let source_identity = self.source_identity;
        let root = root.clone();
        tokio::task::spawn_blocking(move || {
            source.capture_host_subtree_with_identity(&source_root, &root, source_identity)
        })
        .await
        .map_err(|error| MountSourceError::Engine(error.to_string()))?
        .map_err(MountLifecycleError::Source)
    }

    /// Publishes pending capture under an active, fenced operation permit.
    pub async fn sync_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountLifecycleError> {
        self.source
            .sync_async_with_permit(permit)
            .await
            .map_err(MountLifecycleError::Source)
    }

    /// Rebinds a clean working set after an external workspace publication.
    pub async fn advance_to_head(&self) -> Result<(), MountLifecycleError> {
        self.source
            .advance_to_head_async()
            .await
            .map_err(MountLifecycleError::Source)
    }
}

fn flush_session_callbacks(
    session: &Mutex<Option<NativeMountSession>>,
) -> Result<(), MountLifecycleError> {
    let owner = match session.lock() {
        Ok(owner) => owner,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(session) = owner.as_ref() {
        session
            .flush_callbacks()
            .map_err(MountLifecycleError::Native)?;
    }
    Ok(())
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
        self.source
            .advance_to_head_async()
            .await
            .map_err(MountLifecycleError::Source)
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
        self.source
            .advance_to_head_async()
            .await
            .map_err(MountLifecycleError::Source)
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
}

impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Opens a manual-publication working set without starting a mount driver.
    ///
    /// Admission observes only the selected root frontier. The caller may use
    /// native `CoW` backing where the platform proves equivalent semantics, then
    /// capture changed paths and publish through the SDK operation permit. The
    /// source root is the volume root: a selected `/hot` subtree must be at
    /// `source_root/hot`, as produced by `Generation::materialize_path`.
    pub async fn working_set(
        &self,
        subdirectory: impl Into<String>,
        source_root: impl AsRef<Path>,
    ) -> Result<LazyWorkingSet<A, O, D, S>, MountLifecycleError> {
        self.open_working_set(subdirectory.into(), source_root.as_ref(), None)
            .await
    }

    /// Opens a native working set only if the prepared generation remains the
    /// current workspace head. A stale preparation cannot silently attach to
    /// a newer checkout; callers still revalidate before presentation.
    pub async fn working_set_at_generation(
        &self,
        subdirectory: impl Into<String>,
        source_root: impl AsRef<Path>,
        generation: GenerationId,
    ) -> Result<LazyWorkingSet<A, O, D, S>, MountLifecycleError> {
        self.open_working_set(subdirectory.into(), source_root.as_ref(), Some(generation))
            .await
    }

    async fn open_working_set(
        &self,
        subdirectory: String,
        source_root: &Path,
        expected_generation: Option<GenerationId>,
    ) -> Result<LazyWorkingSet<A, O, D, S>, MountLifecycleError> {
        let source_root = source_root.to_path_buf();
        if !source_root.is_absolute() {
            return Err(MountLifecycleError::Source(MountSourceError::Invalid(
                "native working-set root must be absolute".to_owned(),
            )));
        }
        let identity_root = source_root.clone();
        let source_identity =
            tokio::task::spawn_blocking(move || capture_root_identity(&identity_root))
                .await
                .map_err(|error| MountSourceError::Engine(error.to_string()))?
                .map_err(|error| MountSourceError::Engine(error.to_string()))?;
        let options = MountOptions::read_write()
            .subdirectory(subdirectory)
            .publication(MountPublication::Manual);
        let (source, _, root) = self
            .prepare_mount_source(&options, expected_generation)
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
        // Directory validation may wait for host I/O while another writer
        // advances the workspace. Do not return an already-stale preparation.
        if let Some(expected) = expected_generation
            && self.workspace().head().await?.id() != expected
        {
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
        })
    }

    async fn prepare_mount_source(
        &self,
        options: &MountOptions,
        expected_generation: Option<GenerationId>,
    ) -> Result<(Arc<LazyMountSource<A, O, D, S>>, VolumeId, NamespacePath), MountLifecycleError>
    {
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
        let source = Arc::new(LazyMountSource::new(
            Arc::new(self.clone()),
            authored,
            root_text,
        )?);
        if let Some(expected) = expected_generation
            && self.workspace().head().await?.id() != expected
        {
            return Err(MountLifecycleError::Workspace(
                WorkspaceError::StaleGeneration,
            ));
        }
        Ok((source, self.workspace().id().volume_id(), root))
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
        let (source, volume_id, _) = self.prepare_mount_source(&options, None).await?;
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
    async fn working_set_captures_a_native_directory_without_a_mount_driver()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("file.txt"), b"base")?;
        let physical_parent = tempfile::tempdir()?;
        let working = physical_parent.path().join("working");
        std::fs::create_dir(&working)?;
        std::fs::write(working.join("file.txt"), b"changed")?;
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
            "native-working-set",
            demand,
            MemoryLazyWorkspaceStore::default(),
        )
        .await?;
        let working_set = lazy.working_set("/", &working).await?;
        let name = if cfg!(windows) {
            "file.txt"
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect()
        } else {
            b"file.txt".to_vec()
        };
        working_set
            .capture_host_paths(&[MountPath::root().child(name.clone())])
            .await?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert_eq!(
            lazy.workspace().read("/file.txt", 16).await?.as_ref(),
            b"changed"
        );
        let prior = physical_parent.path().join("prior");
        std::fs::rename(&working, &prior)?;
        std::fs::create_dir(&working)?;
        std::fs::write(working.join("file.txt"), b"wrong-root")?;
        assert!(
            working_set
                .capture_host_paths(&[MountPath::root().child(name)])
                .await
                .is_err()
        );
        assert_eq!(
            lazy.workspace().read("/file.txt", 16).await?.as_ref(),
            b"changed"
        );
        Ok(())
    }

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
        let prepared = lazy
            .exactify_subtree_with_permit(
                "/hot",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                PublicationPermit::Unrestricted,
            )
            .await?;
        let generation = prepared.value.id();
        assert!(
            lazy.working_set_at_generation("/hot", native_view.path(), generation)
                .await
                .is_err()
        );
        prepared
            .value
            .materialize_path(
                "/hot",
                &crate::MaterializeOptions::native(native_view.path()),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?;
        let working = native_view.path();
        let working_set = lazy
            .working_set_at_generation("/hot", &working, generation)
            .await?;
        working_set.validate_for_presentation().await?;
        std::fs::write(working.join("hot/file.txt"), b"native-edit")?;
        let name = if cfg!(windows) {
            "file.txt"
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect()
        } else {
            b"file.txt".to_vec()
        };
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
        assert!(matches!(
            working_set.validate_for_presentation().await,
            Err(MountLifecycleError::Workspace(
                WorkspaceError::StaleGeneration
            ))
        ));
        assert!(matches!(
            lazy.working_set_at_generation("/hot", &working, generation)
                .await,
            Err(MountLifecycleError::Workspace(
                WorkspaceError::StaleGeneration
            ))
        ));
        let latest = lazy.workspace().head().await?.id();
        let ready = lazy
            .working_set_at_generation("/hot", &working, latest)
            .await?;
        ready.validate_for_presentation().await?;
        std::fs::rename(working, source.path().join("retired-native-view"))?;
        std::fs::create_dir(working)?;
        std::fs::create_dir(working.join("hot"))?;
        assert!(matches!(
            ready.validate_for_presentation().await,
            Err(MountLifecycleError::Source(MountSourceError::Stale))
        ));
        Ok(())
    }
}
