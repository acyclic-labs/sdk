//! Native projection of one demand-backed lazy workspace.

use super::adapter::CallbackRuntime;
use super::{
    CheckoutMountSource, MountAttributePage, MountAttributeWriteMode, MountDirectoryEntry,
    MountDirectoryPage, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountRangeAllocation, MountSeekTarget, MountSourceError,
};
use crate::demand::{DemandSource, SourceNode, SourceNodeKind};
use crate::kernel::{FileKind, FileMetadata, FilePayload, MetadataField};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, FileId, IdempotencyKey, LazyDirectoryCursor, LazyLookup,
    LazySeekTarget, LazyWorkspace, LazyWorkspaceError, LazyWorkspaceStore, WorkspaceMetadata,
};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const MAXIMUM_PROMOTION_BYTES: u64 = u64::MAX;
const MAXIMUM_LAZY_DIRECTORY_CURSORS: usize = 1_024;

struct CursorTable<T> {
    entries: Mutex<BTreeMap<u64, T>>,
    next: AtomicU64,
    maximum: usize,
}

impl<T> CursorTable<T> {
    fn new(maximum: usize) -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            next: AtomicU64::new(1),
            maximum,
        }
    }

    fn remember(&self, cursor: T) -> Result<Vec<u8>, MountSourceError> {
        let mut token = self.next.fetch_add(1, Ordering::Relaxed);
        if token == 0 {
            token = self.next.fetch_add(1, Ordering::Relaxed);
        }
        let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
        while entries.len() >= self.maximum {
            let Some(oldest) = entries.first_key_value().map(|(&key, _)| key) else {
                break;
            };
            entries.remove(&oldest);
        }
        entries.insert(token, cursor);
        Ok(token.to_le_bytes().to_vec())
    }

    fn take(&self, cursor: Option<&[u8]>) -> Result<Option<T>, MountSourceError> {
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        let bytes: [u8; 8] = cursor.try_into().map_err(|_| MountSourceError::Stale)?;
        self.entries
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .remove(&u64::from_le_bytes(bytes))
            .ok_or(MountSourceError::Stale)
            .map(Some)
    }

    fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }
}

/// One native callback adapter over a source-backed sparse workspace.
///
/// Reads demand only the addressed source facts. Mutations promote the exact
/// node into the authored checkout before delegating to the ordinary checkout
/// adapter, keeping all publication semantics in the SDK.
pub struct LazyMountSource<A, O, D, S> {
    lazy: Arc<LazyWorkspace<A, O, D, S>>,
    authored: Arc<CheckoutMountSource<A, O>>,
    root: String,
    runtime: Arc<CallbackRuntime>,
    cursors: CursorTable<LazyDirectoryCursor>,
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Composes lazy reads with one ordinary authored checkout adapter.
    pub(super) fn new(
        lazy: Arc<LazyWorkspace<A, O, D, S>>,
        authored: Arc<CheckoutMountSource<A, O>>,
        root: String,
    ) -> Result<Self, super::NativeMountError> {
        Ok(Self {
            lazy,
            authored,
            root,
            runtime: Arc::new(CallbackRuntime::create()?),
            cursors: CursorTable::new(MAXIMUM_LAZY_DIRECTORY_CURSORS),
        })
    }

    /// Cancels the authored callback adapter.
    pub fn cancel(&self) {
        self.cursors.clear();
        self.authored.cancel();
    }

    /// Publishes pending authored writes.
    pub async fn sync_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.authored.sync_async().await
    }

    /// Publishes pending authored writes under one authority lease permit.
    pub async fn sync_async_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.authored.sync_async_with_permit(permit).await
    }

    /// Publishes pending authored writes on the callback runtime.
    pub fn sync(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        self.authored.sync()
    }

    /// Advances the authored checkout after an external publication.
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.authored.advance_to_head_async().await
    }

    fn path(&self, path: &MountPath) -> Result<String, MountSourceError> {
        let mut value = String::new();
        for component in path.components() {
            value.push('/');
            let text = if cfg!(target_os = "windows") {
                if !component.len().is_multiple_of(2) {
                    return Err(MountSourceError::Invalid(
                        "native UTF-16 name has an odd byte length".to_owned(),
                    ));
                }
                let units = component
                    .chunks_exact(2)
                    .filter_map(|pair| pair.try_into().ok().map(u16::from_le_bytes))
                    .collect::<Vec<_>>();
                String::from_utf16(&units).map_err(|_| {
                    MountSourceError::Unsupported(
                        "lazy portable mount cannot represent an unpaired UTF-16 name".to_owned(),
                    )
                })?
            } else {
                std::str::from_utf8(component)
                    .map_err(|_| {
                        MountSourceError::Unsupported(
                            "lazy portable mount cannot represent a non-UTF-8 name".to_owned(),
                        )
                    })?
                    .to_owned()
            };
            value.push_str(&text);
        }
        if value.is_empty() {
            value.push('/');
        }
        if self.root == "/" {
            Ok(value)
        } else if value == "/" {
            Ok(self.root.clone())
        } else {
            Ok(format!("{}{value}", self.root))
        }
    }

    fn native_name(name: &crate::kernel::LogicalName) -> Result<Vec<u8>, MountSourceError> {
        let text = std::str::from_utf8(name.as_bytes()).map_err(|_| {
            MountSourceError::Unsupported(
                "lazy portable mount cannot project a non-UTF-8 name".to_owned(),
            )
        })?;
        if cfg!(target_os = "windows") {
            Ok(text.encode_utf16().flat_map(u16::to_le_bytes).collect())
        } else {
            Ok(text.as_bytes().to_vec())
        }
    }

    fn child(parent: &str, name: &crate::kernel::LogicalName) -> Result<String, MountSourceError> {
        let name = std::str::from_utf8(name.as_bytes()).map_err(|_| {
            MountSourceError::Unsupported(
                "lazy portable mount cannot address a non-UTF-8 name".to_owned(),
            )
        })?;
        Ok(if parent == "/" {
            format!("/{name}")
        } else {
            format!("{parent}/{name}")
        })
    }

    fn promotion_key(&self, path: &str, node: &SourceNode) -> IdempotencyKey {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-fs-lazy-mount-promotion-v1\0");
        hasher.update(&self.lazy.workspace().id().into_bytes());
        hasher.update(path.as_bytes());
        hasher.update(&node.version.0);
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        IdempotencyKey::from_bytes(bytes)
    }

    fn identity_promotion_key(&self, path: &str, file_id: FileId) -> IdempotencyKey {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-fs-lazy-mount-identity-promotion-v1\0");
        hasher.update(&self.lazy.workspace().id().into_bytes());
        hasher.update(path.as_bytes());
        hasher.update(&file_id.into_bytes());
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        IdempotencyKey::from_bytes(bytes)
    }

    async fn promote(&self, path: &str) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        let lookup = self.lazy.lookup(path).await.map_err(lazy_error)?;
        let (expected_source, key, needs_promotion) = match lookup {
            LazyLookup::Source(node) => {
                let identity = self.lazy.source_file_id(&node);
                (identity, self.promotion_key(path, &node), true)
            }
            LazyLookup::Authored {
                path: authored_path,
                stat,
            } => (
                stat.file_id,
                self.identity_promotion_key(path, stat.file_id),
                authored_path != path,
            ),
            LazyLookup::Shadow { record, .. } => (
                record.file_id,
                self.identity_promotion_key(path, record.file_id),
                true,
            ),
        };
        if needs_promotion {
            self.lazy
                .promote_exact(path, expected_source, MAXIMUM_PROMOTION_BYTES, key)
                .await
                .map_err(lazy_error)?;
            self.authored.advance_to_head_async().await?;
        }
        Ok(())
    }

    async fn promote_parents(&self, path: &str) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        let mut parents = Vec::new();
        let mut current = path;
        while let Some(index) = current.rfind('/') {
            if index == 0 {
                break;
            }
            current = current.get(..index).ok_or(MountSourceError::Stale)?;
            parents.push(current.to_owned());
        }
        for parent in parents.into_iter().rev() {
            self.promote(&parent).await?;
        }
        Ok(())
    }

    fn wait<T: Send, F>(&self, create: impl FnOnce() -> F + Send) -> Result<T, MountSourceError>
    where
        F: std::future::Future<Output = Result<T, MountSourceError>>,
    {
        self.runtime.wait(create)
    }

    fn remember_cursor(&self, cursor: LazyDirectoryCursor) -> Result<Vec<u8>, MountSourceError> {
        self.cursors.remember(cursor)
    }

    fn take_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<Option<LazyDirectoryCursor>, MountSourceError> {
        self.cursors.take(cursor)
    }
}

struct LazyOpenFile<A, O, D, S> {
    lazy: Arc<LazyWorkspace<A, O, D, S>>,
    authored: Arc<CheckoutMountSource<A, O>>,
    runtime: Arc<CallbackRuntime>,
    path: String,
    mount_path: MountPath,
    promotion_key: IdempotencyKey,
    expected_source: FileId,
    promoted: Mutex<Option<Arc<dyn MountOpenFile>>>,
}

impl<A, O, D, S> LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn authored(&self) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let mut promoted = self.promoted.lock().map_err(|_| MountSourceError::Stale)?;
        if let Some(file) = promoted.as_ref() {
            return Ok(Arc::clone(file));
        }
        self.runtime.wait(|| async {
            self.lazy
                .promote_exact(
                    &self.path,
                    self.expected_source,
                    MAXIMUM_PROMOTION_BYTES,
                    self.promotion_key,
                )
                .await
                .map_err(lazy_error)?;
            self.authored.advance_to_head_async().await
        })?;
        let file = self.authored.open_file(&self.mount_path)?;
        *promoted = Some(Arc::clone(&file));
        Ok(file)
    }
}

impl<A, O, D, S> MountOpenFile for LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        if let Some(file) = self
            .promoted
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .as_ref()
            .cloned()
        {
            return file.lookup();
        }
        self.runtime.wait(|| async {
            self.lazy
                .lookup(&self.path)
                .await
                .map(|lookup| mount_lookup(&self.lazy, lookup))
                .map_err(lazy_error)
        })
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        if let Some(file) = self
            .promoted
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .as_ref()
            .cloned()
        {
            return file.read_range(offset, length);
        }
        self.runtime.wait(|| async {
            self.lazy
                .read_range(&self.path, offset, u64::from(length))
                .await
                .map_err(lazy_error)
        })
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        if let Some(file) = self
            .promoted
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .as_ref()
            .cloned()
        {
            return file.seek(offset, target);
        }
        self.runtime.wait(|| async {
            self.lazy
                .seek(
                    &self.path,
                    offset,
                    match target {
                        MountSeekTarget::Data => LazySeekTarget::Data,
                        MountSeekTarget::Hole => LazySeekTarget::Hole,
                    },
                )
                .await
                .map_err(lazy_error)
        })
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.authored()?.write_range(offset, bytes)
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.authored()?.resize(logical_bytes)
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.authored()?.allocate_range(offset, length, operation)
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.authored()?.set_attributes(metadata, logical_bytes)
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        self.authored()?.read_attribute(name)
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.authored()?.list_attributes(cursor, maximum_entries)
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.authored()?.write_attribute(name, value, mode)
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        self.authored()?.remove_attribute(name)
    }
}

impl<A, O, D, S> MountFilesystem for LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        let path = self.path(path)?;
        self.wait(|| async move {
            match self.lazy.lookup(&path).await {
                Ok(lookup) => Ok(Some(mount_lookup(&self.lazy, lookup))),
                Err(LazyWorkspaceError::NotFound) => Ok(None),
                Err(error) => Err(lazy_error(error)),
            }
        })
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let path_text = self.path(path)?;
        let lookup_path = path_text.clone();
        let lookup =
            self.wait(|| async move { self.lazy.lookup(&lookup_path).await.map_err(lazy_error) })?;
        match lookup {
            LazyLookup::Authored {
                path: authored_path,
                ..
            } if authored_path == path_text => self.authored.open_file(path),
            LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
                self.promote_blocking(path)?;
                self.authored.open_file(path)
            }
            LazyLookup::Source(node) if node.kind == SourceNodeKind::RegularFile => {
                let promotion_key = self.promotion_key(&path_text, &node);
                let expected_source = self.lazy.source_file_id(&node);
                Ok(Arc::new(LazyOpenFile {
                    lazy: Arc::clone(&self.lazy),
                    authored: Arc::clone(&self.authored),
                    runtime: Arc::clone(&self.runtime),
                    path: path_text,
                    mount_path: path.clone(),
                    promotion_key,
                    expected_source,
                    promoted: Mutex::new(None),
                }))
            }
            LazyLookup::Source(_) => Err(MountSourceError::Invalid(
                "open requires a regular file".to_owned(),
            )),
        }
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let path_text = self.path(path)?;
        self.wait(|| async move { self.promote(&path_text).await })?;
        self.authored.detach_file(path)
    }

    fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError> {
        let path = self.path(path)?;
        self.wait(|| async move { self.lazy.read_link(&path).await.map_err(lazy_error) })
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountSourceError> {
        let path = self.path(path)?;
        self.wait(|| async move {
            self.lazy
                .read_range(&path, offset, u64::from(length))
                .await
                .map_err(lazy_error)
        })
    }

    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError> {
        let path = self.path(path)?;
        self.wait(|| async move {
            self.lazy
                .seek(
                    &path,
                    offset,
                    match target {
                        MountSeekTarget::Data => LazySeekTarget::Data,
                        MountSeekTarget::Hole => LazySeekTarget::Hole,
                    },
                )
                .await
                .map_err(lazy_error)
        })
    }

    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let path = self.path(path)?;
        let cursor = self.take_cursor(cursor)?;
        let page_path = path.clone();
        let page = self.wait(|| async move {
            self.lazy
                .list_directory(&page_path, cursor, maximum_entries)
                .await
                .map_err(lazy_error)
        })?;
        let mut entries = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let child = Self::child(&path, &entry.name)?;
            let lookup = self.wait(|| async {
                self.lazy
                    .inspect(&child)
                    .await
                    .map(|lookup| mount_lookup(&self.lazy, lookup))
                    .map_err(lazy_error)
            })?;
            entries.push(MountDirectoryEntry {
                name: Self::native_name(&entry.name)?,
                node: lookup.node,
                metadata: lookup.metadata,
            });
        }
        Ok(MountDirectoryPage {
            entries,
            next_cursor: page
                .next
                .map(|cursor| self.remember_cursor(cursor))
                .transpose()?,
        })
    }

    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let text = self.path(path)?;
        self.wait(|| async move { self.promote_parents(&text).await })?;
        self.authored.create_file(path, metadata)
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let text = self.path(path)?;
        self.wait(|| async move { self.promote_parents(&text).await })?;
        self.authored.create_directory(path, metadata)
    }

    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: Bytes,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let text = self.path(path)?;
        self.wait(|| async move { self.promote_parents(&text).await })?;
        self.authored.create_symbolic_link(path, target, metadata)
    }

    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let text = self.path(path)?;
        self.wait(|| async move { self.promote_parents(&text).await })?;
        self.authored.create_special(path, kind, device, metadata)
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.promote_blocking(path)?;
        self.authored.set_attributes(path, metadata, logical_bytes)
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
    ) -> Result<Option<Bytes>, MountSourceError> {
        self.promote_blocking(path)?;
        self.authored.read_attribute(path, name)
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.promote_blocking(path)?;
        self.authored.list_attributes(path, cursor, maximum_entries)
    }

    fn write_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.promote_blocking(path)?;
        self.authored.write_attribute(path, name, value, mode)
    }

    fn remove_attribute(&self, path: &MountPath, name: &[u8]) -> Result<(), MountSourceError> {
        self.promote_blocking(path)?;
        self.authored.remove_attribute(path, name)
    }

    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError> {
        self.promote_blocking(path)?;
        self.authored.write_range(path, offset, bytes)
    }

    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.promote_blocking(path)?;
        self.authored.resize(path, logical_bytes)
    }

    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.promote_blocking(path)?;
        self.authored
            .allocate_range(path, offset, length, operation)
    }

    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        self.promote_blocking(source)?;
        self.promote_blocking(destination)?;
        self.authored.clone_range(
            source,
            source_offset,
            destination,
            destination_offset,
            length,
        )
    }

    fn clone_range_by_id(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let _ = (
            source_file_id,
            source_offset,
            destination_file_id,
            destination_offset,
            length,
        );
        Err(MountSourceError::Unsupported(
            "clone-by-identity requires path-backed promotion in a lazy mount".to_owned(),
        ))
    }

    fn remove(&self, path: &MountPath, expected: Option<FileId>) -> Result<(), MountSourceError> {
        let text = self.path(path)?;
        self.wait(|| async move {
            self.lazy
                .remove_if(&text, expected)
                .await
                .map_err(lazy_error)?;
            self.authored.advance_to_head_async().await
        })
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
    ) -> Result<(), MountSourceError> {
        let source_text = self.path(source)?;
        let destination_text = self.path(destination)?;
        let source_lookup =
            self.wait(|| async move { self.lazy.lookup(&source_text).await.map_err(lazy_error) })?;
        if matches!(
            source_lookup,
            LazyLookup::Source(SourceNode {
                kind: SourceNodeKind::Directory,
                ..
            })
        ) {
            return Err(MountSourceError::Unsupported(
                "renaming an unresolved lazy directory requires a subtree remap".to_owned(),
            ));
        }
        self.promote_blocking(source)?;
        self.wait(|| async move { self.promote_parents(&destination_text).await })?;
        self.authored.rename(source, destination, replace)
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
    ) -> Result<(), MountSourceError> {
        self.promote_blocking(source)?;
        let destination_text = self.path(destination)?;
        self.wait(|| async move { self.promote_parents(&destination_text).await })?;
        self.authored.hard_link(source, destination)
    }

    fn flush(&self) -> Result<(), MountSourceError> {
        self.authored.sync()
    }

    fn capture_host_path(
        &self,
        _source_root: &Path,
        _path: &MountPath,
    ) -> Result<(), MountSourceError> {
        Err(MountSourceError::Unsupported(
            "host capture is unavailable for demand-backed mounts".to_owned(),
        ))
    }

    fn capture_host_subtree(
        &self,
        _source_root: &Path,
        _path: &MountPath,
    ) -> Result<(), MountSourceError> {
        Err(MountSourceError::Unsupported(
            "host subtree capture is unavailable for demand-backed mounts".to_owned(),
        ))
    }
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn promote_blocking(&self, path: &MountPath) -> Result<(), MountSourceError> {
        let text = self.path(path)?;
        self.wait(|| async move { self.promote(&text).await })
    }
}

fn mount_lookup<A, O, D, S>(lazy: &LazyWorkspace<A, O, D, S>, lookup: LazyLookup) -> MountLookup
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    match lookup {
        LazyLookup::Authored { stat, .. } => MountLookup {
            node: MountNode {
                file_id: stat.file_id,
                kind: file_kind(stat.kind),
                logical_bytes: stat.logical_bytes.unwrap_or(0),
                link_count: stat.link_count,
                device: None,
            },
            metadata: workspace_metadata(stat.metadata),
        },
        LazyLookup::Shadow {
            record,
            metadata,
            source_link_count,
        } => MountLookup {
            node: MountNode {
                file_id: record.file_id,
                kind: file_kind(record.kind),
                logical_bytes: record_logical_bytes(record.payload),
                link_count: source_link_count,
                device: match record.payload {
                    FilePayload::Device { major, minor } => Some((major, minor)),
                    _ => None,
                },
            },
            metadata: workspace_metadata(metadata),
        },
        LazyLookup::Source(node) => MountLookup {
            node: MountNode {
                file_id: lazy.source_file_id(&node),
                kind: source_kind(node.kind),
                logical_bytes: node.logical_bytes.unwrap_or(0),
                link_count: node.link_count.unwrap_or(1),
                device: node.device,
            },
            metadata: source_metadata(node.metadata),
        },
    }
}

const fn source_kind(kind: SourceNodeKind) -> MountNodeKind {
    match kind {
        SourceNodeKind::RegularFile => MountNodeKind::Regular,
        SourceNodeKind::Directory => MountNodeKind::Directory,
        SourceNodeKind::SymbolicLink => MountNodeKind::SymbolicLink,
        SourceNodeKind::Fifo => MountNodeKind::Fifo,
        SourceNodeKind::Socket => MountNodeKind::Socket,
        SourceNodeKind::CharacterDevice => MountNodeKind::CharacterDevice,
        SourceNodeKind::BlockDevice => MountNodeKind::BlockDevice,
        SourceNodeKind::Unsupported => MountNodeKind::Unsupported,
    }
}

fn record_logical_bytes(payload: FilePayload) -> u64 {
    match payload {
        FilePayload::InlineRegular(bytes) => {
            u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX)
        }
        FilePayload::Regular { logical_bytes, .. }
        | FilePayload::SymbolicLink {
            target_bytes: logical_bytes,
            ..
        }
        | FilePayload::ReparsePoint {
            payload_bytes: logical_bytes,
            ..
        } => logical_bytes,
        FilePayload::Directory { .. } | FilePayload::Empty | FilePayload::Device { .. } => 0,
    }
}

const fn file_kind(kind: FileKind) -> MountNodeKind {
    match kind {
        FileKind::Regular => MountNodeKind::Regular,
        FileKind::Directory => MountNodeKind::Directory,
        FileKind::SymbolicLink => MountNodeKind::SymbolicLink,
        FileKind::Fifo => MountNodeKind::Fifo,
        FileKind::Socket => MountNodeKind::Socket,
        FileKind::CharacterDevice => MountNodeKind::CharacterDevice,
        FileKind::BlockDevice => MountNodeKind::BlockDevice,
        FileKind::ReparsePoint | FileKind::MountBoundary => MountNodeKind::Unsupported,
    }
}

fn workspace_metadata(metadata: WorkspaceMetadata) -> FileMetadata {
    FileMetadata {
        posix_mode: field(metadata.posix_mode),
        posix_uid: field(metadata.posix_uid),
        posix_gid: field(metadata.posix_gid),
        posix_flags: field(metadata.posix_flags),
        windows_attributes: field(metadata.windows_attributes),
        created_ns: field(metadata.created_ns),
        modified_ns: field(metadata.modified_ns),
        accessed_ns: field(metadata.accessed_ns),
        changed_ns: field(metadata.changed_ns),
        ..FileMetadata::default()
    }
}

fn source_metadata(metadata: crate::demand::SourceMetadata) -> FileMetadata {
    FileMetadata {
        posix_mode: field(metadata.posix_mode),
        posix_uid: field(metadata.posix_uid),
        posix_gid: field(metadata.posix_gid),
        posix_flags: field(metadata.posix_flags),
        windows_attributes: field(metadata.windows_attributes),
        created_ns: field(
            metadata
                .created_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        modified_ns: field(
            metadata
                .modified_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        accessed_ns: field(
            metadata
                .accessed_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        changed_ns: field(
            metadata
                .changed_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        ..FileMetadata::default()
    }
}

fn field<T>(value: Option<T>) -> MetadataField<T> {
    value.map_or(MetadataField::Unavailable, MetadataField::Value)
}

#[allow(clippy::needless_pass_by_value)]
fn lazy_error(error: LazyWorkspaceError) -> MountSourceError {
    match error {
        LazyWorkspaceError::NotFound => MountSourceError::NotFound,
        LazyWorkspaceError::NotRegularFile | LazyWorkspaceError::InvalidPageBound => {
            MountSourceError::Invalid(error.to_string())
        }
        LazyWorkspaceError::UnsupportedNode | LazyWorkspaceError::UnresolvedHardLinks => {
            MountSourceError::Unsupported(error.to_string())
        }
        LazyWorkspaceError::StaleSource
        | LazyWorkspaceError::StaleCursor
        | LazyWorkspaceError::StaleIdentity
        | LazyWorkspaceError::Concurrent => MountSourceError::Stale,
        _ => MountSourceError::Engine(error.to_string()),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn abandoned_directory_cursors_are_bounded_and_cleared() {
        let cursors = CursorTable::new(2);
        let first = cursors.remember(1_u8).expect("first cursor");
        let second = cursors.remember(2_u8).expect("second cursor");
        let third = cursors.remember(3_u8).expect("third cursor");

        assert!(matches!(
            cursors.take(Some(&first)),
            Err(MountSourceError::Stale)
        ));
        assert_eq!(cursors.take(Some(&second)).expect("second token"), Some(2));
        assert_eq!(cursors.take(Some(&third)).expect("third token"), Some(3));

        let retained = cursors.remember(4_u8).expect("retained cursor");
        cursors.clear();
        assert!(matches!(
            cursors.take(Some(&retained)),
            Err(MountSourceError::Stale)
        ));
    }
}
