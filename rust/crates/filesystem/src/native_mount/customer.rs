//! Customer-facing native mount composition over the canonical checkout.

use super::{
    CheckoutMountSource, LazyMountSource, NativeMountError, NativeMountRequest, NativeMountSession,
    SharedCheckout, mount_native,
};
use crate::demand::{DemandSource, SourceNode, SourceNodeKind};
use crate::kernel::FileKind;
use crate::model::{CheckoutMode, GenerationSelector};
use crate::workspace::{Workspace, WorkspaceError, customer_path};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, IdempotencyKey, LazyLookup, LazyWorkspace,
    LazyWorkspaceError, LazyWorkspaceStore, MountId, MountSourceError, VolumeId,
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
    async fn prepare_mount_source(
        &self,
        options: &MountOptions,
    ) -> Result<(Arc<LazyMountSource<A, O, D, S>>, VolumeId), MountLifecycleError> {
        let probe = self
            .workspace()
            .engine_checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned())
            .await?;
        let config = probe.volume_config();
        drop(probe);
        let root = customer_path(&options.subdirectory, config.limits)?;
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
            })
        ) {
            return Err(MountLifecycleError::Workspace(WorkspaceError::NotDirectory));
        }
        if let LazyLookup::Source(node) = selected {
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
            .engine_checkout(GenerationSelector::Head, mode)
            .await?;
        let shared = Arc::new(SharedCheckout::with_publication(
            checkout,
            options.publication,
        ));
        let authored = Arc::new(CheckoutMountSource::new_at(shared, config, root)?);
        let source = Arc::new(LazyMountSource::new(
            Arc::new(self.clone()),
            authored,
            root_text,
        )?);
        Ok((source, self.workspace().id().volume_id()))
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
        let (source, volume_id) = self.prepare_mount_source(&options).await?;
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
        let root = customer_path(&options.subdirectory, config.limits)?;
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

/// Read-your-writes through the lazy mount source, at the callback boundary a
/// kernel driver uses. Every native mount backend (FUSE, loopback NFS,
/// `ProjFS`) sees exactly these answers, so these hold whatever the driver.
#[cfg(all(test, unix))]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod live_view_tests {
    use super::*;
    use crate::MemoryLazyWorkspaceStore;
    use crate::demand::native::NativeDemandSource;
    use crate::facade::Fs;
    use crate::kernel::FileMetadata;
    use crate::model::{FilesystemProfile, VolumeLimits};
    use crate::native_mount::{MountFilesystem, MountNodeKind, MountPath};
    use bytes::Bytes;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn at(path: &str) -> MountPath {
        path.split('/')
            .filter(|part| !part.is_empty())
            .fold(MountPath::root(), |parent, part| {
                parent.child(part.as_bytes().to_vec())
            })
    }

    /// Every name in one directory, paging `page` entries at a time. A page
    /// with no entries must be the last one: native drivers treat an empty
    /// page with a continuation as an error.
    fn list(source: &dyn MountFilesystem, path: &str, page: u32) -> Vec<String> {
        let mut names = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let listed = source
                .read_directory(&at(path), cursor.as_deref(), page)
                .unwrap_or_else(|error| panic!("directory page for {path}: {error:?}"));
            assert!(
                !(listed.entries.is_empty() && listed.next_cursor.is_some()),
                "empty page with a continuation"
            );
            names.extend(
                listed
                    .entries
                    .iter()
                    .map(|entry| String::from_utf8_lossy(&entry.name).into_owned()),
            );
            match listed.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        let mut sorted = names.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            names.len(),
            "a name was listed twice: {names:?}"
        );
        sorted
    }

    fn write(source: &dyn MountFilesystem, path: &str, bytes: &'static [u8]) {
        source
            .create_file(&at(path), FileMetadata::default())
            .expect("create file");
        source
            .open_file(&at(path))
            .expect("open created file")
            .write_range(0, Bytes::from_static(bytes))
            .expect("write created file");
    }

    async fn mounted_source(
        root: &Path,
    ) -> Result<Arc<dyn MountFilesystem>, Box<dyn std::error::Error + Send + Sync>> {
        std::fs::write(root.join("README.md"), b"source\n")?;
        std::fs::create_dir(root.join("src"))?;
        std::fs::write(root.join("src").join("lib.rs"), b"fn source() {}\n")?;
        let source = Arc::new(
            NativeDemandSource::open(root, FilesystemProfile::Portable, VolumeLimits::default())
                .await?,
        );
        let fs = Fs::memory();
        let lazy = LazyWorkspace::attach(
            &fs,
            "live-view",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await?;
        let (mount, _) = lazy
            .prepare_mount_source(&MountOptions::read_write())
            .await?;
        Ok(mount)
    }

    /// Runs blocking callbacks off the async runtime, as a kernel thread would.
    async fn callbacks(
        source: Arc<dyn MountFilesystem>,
        body: impl FnOnce(&dyn MountFilesystem) + Send + 'static,
    ) -> TestResult {
        tokio::task::spawn_blocking(move || body(source.as_ref())).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn created_files_and_directories_are_visible_before_publication() -> TestResult {
        let root = tempfile::tempdir()?;
        let source = mounted_source(root.path()).await?;
        callbacks(source, |fs| {
            write(fs, "/new.txt", b"hello\n");
            let found = fs
                .lookup(&at("/new.txt"))
                .expect("lookup")
                .expect("new file is visible");
            assert_eq!(
                found.node.logical_bytes, 6,
                "size reflects the unpublished write"
            );
            assert_eq!(
                fs.read_range(&at("/new.txt"), 0, 6).expect("read").as_ref(),
                b"hello\n"
            );

            fs.create_directory(&at("/pkg"), FileMetadata::default())
                .expect("mkdir");
            let dir = fs
                .lookup(&at("/pkg"))
                .expect("lookup dir")
                .expect("new directory is visible");
            assert_eq!(dir.node.kind, MountNodeKind::Directory);
            write(fs, "/pkg/mod.rs", b"x = 1\n");
            write(fs, "/src/extra.rs", b"// new beside a source file\n");

            for page in [1, 2, 512] {
                assert_eq!(list(fs, "/", page), ["README.md", "new.txt", "pkg", "src"]);
                assert_eq!(list(fs, "/pkg", page), ["mod.rs"]);
                assert_eq!(list(fs, "/src", page), ["extra.rs", "lib.rs"]);
            }
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unpublished_renames_and_removals_are_visible() -> TestResult {
        let root = tempfile::tempdir()?;
        let source = mounted_source(root.path()).await?;
        callbacks(source, |fs| {
            // Write-then-rename, the way editors and package managers save.
            write(fs, "/.tmp-save", b"saved\n");
            fs.rename(&at("/.tmp-save"), &at("/saved.txt"), false)
                .expect("rename new file");
            assert!(fs.lookup(&at("/.tmp-save")).expect("lookup").is_none());
            assert_eq!(
                fs.read_range(&at("/saved.txt"), 0, 6)
                    .expect("read")
                    .as_ref(),
                b"saved\n"
            );

            write(fs, "/scratch.txt", b"gone soon\n");
            fs.remove(&at("/scratch.txt"), None)
                .expect("remove new file");
            assert!(fs.lookup(&at("/scratch.txt")).expect("lookup").is_none());

            // A source file renamed away is gone from its old name at once.
            fs.rename(&at("/README.md"), &at("/README.old"), false)
                .expect("rename source file");
            assert!(fs.lookup(&at("/README.md")).expect("lookup").is_none());
            fs.remove(&at("/src/lib.rs"), None)
                .expect("remove source file");
            assert!(fs.lookup(&at("/src/lib.rs")).expect("lookup").is_none());

            for page in [1, 3, 512] {
                assert_eq!(list(fs, "/", page), ["README.old", "saved.txt", "src"]);
                assert!(list(fs, "/src", page).is_empty());
            }

            // Publishing must not bring a renamed-away source file back.
            fs.flush().expect("publish");
            assert!(fs.lookup(&at("/README.md")).expect("lookup").is_none());
            assert_eq!(list(fs, "/", 512), ["README.old", "saved.txt", "src"]);
        })
        .await
    }
}
