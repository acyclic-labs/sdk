//! Native projection of one demand-backed lazy workspace.

use super::adapter::CallbackRuntime;
use super::view_gate::{
    ViewGate as SourceViewGate, ViewReadLease as SourceViewLease,
    ViewWriteLease as SourceMutationLease,
};
use super::{
    CheckoutMountSource, MountAttributePage, MountAttributeWriteMode, MountDirectoryEntry,
    MountDirectoryPage, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountRangeAllocation, MountSeekTarget, MountSourceError, MountViewLease,
    capture_root_identity,
};
use crate::LazySeekTarget;
#[cfg(unix)]
use crate::demand::SourceReference;
use crate::demand::{DemandSource, SourceNode, SourceNodeKind};
use crate::kernel::{FileKind, FileMetadata, FilePayload, MetadataField};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, FileId, IdempotencyKey, LazyDirectoryCursor, LazyLookup,
    LazyWorkspace, LazyWorkspaceError, LazyWorkspaceStore, NativeRootIdentity, WorkspaceMetadata,
};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const MAXIMUM_PROMOTION_BYTES: u64 = u64::MAX;
const MAXIMUM_LAZY_DIRECTORY_CURSORS: usize = 1_024;

struct StampedCursor<T> {
    generation: u64,
    cursor: T,
}

struct CursorTable<T> {
    entries: Mutex<BTreeMap<u64, StampedCursor<T>>>,
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

    fn remember(&self, generation: u64, cursor: T) -> Result<Vec<u8>, MountSourceError> {
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
        entries.insert(token, StampedCursor { generation, cursor });
        Ok(token.to_le_bytes().to_vec())
    }

    fn take(&self, generation: u64, cursor: Option<&[u8]>) -> Result<Option<T>, MountSourceError> {
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        let bytes: [u8; 8] = cursor.try_into().map_err(|_| MountSourceError::Stale)?;
        let stamped = self
            .entries
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .remove(&u64::from_le_bytes(bytes))
            .ok_or(MountSourceError::Stale)?;
        if stamped.generation != generation {
            return Err(MountSourceError::Stale);
        }
        Ok(Some(stamped.cursor))
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
    source_view: Arc<SourceViewGate>,
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
            source_view: Arc::new(SourceViewGate::new()),
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
        let _mutation = self.source_view.write_stable(None).await?;
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
        let _mutation = self.source_view.write_stable(None).await?;
        self.authored.sync_async_with_permit(permit).await
    }

    /// Publishes pending authored writes on the callback runtime.
    pub fn sync(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let _mutation = self.mutation_lease(None)?;
        self.authored.sync()
    }

    /// Advances the authored checkout after an external publication.
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let _writer = self.source_view.write().await;
        self.source_view.begin_transition();
        let result = async {
            self.authored.advance_to_head_async().await?;
            self.lazy.rebind_source().await.map_err(lazy_error)?;
            self.cursors.clear();
            Ok(())
        }
        .await;
        if result.is_ok() {
            self.source_view.finish_transition();
        }
        result
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

    fn promote_parents_locked(&self, path: &MountPath) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        let components = path.components();
        let mut parent = MountPath::root();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            parent = parent.child((*component).to_vec());
            let authored = self.authored.lookup(&parent)?;
            if authored.is_some() {
                continue;
            }
            let text = self.path(&parent)?;
            self.wait(|| async move { self.promote(&text).await })?;
        }
        Ok(())
    }

    fn wait<T: Send, F>(&self, create: impl FnOnce() -> F + Send) -> Result<T, MountSourceError>
    where
        F: std::future::Future<Output = Result<T, MountSourceError>>,
    {
        self.runtime.wait(create)
    }

    fn remember_cursor(
        &self,
        generation: u64,
        cursor: LazyDirectoryCursor,
    ) -> Result<Vec<u8>, MountSourceError> {
        self.cursors.remember(generation, cursor)
    }

    fn take_cursor(
        &self,
        generation: u64,
        cursor: Option<&[u8]>,
    ) -> Result<Option<LazyDirectoryCursor>, MountSourceError> {
        self.cursors.take(generation, cursor)
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
    #[cfg(unix)]
    source: SourceReference,
    #[cfg(unix)]
    source_node: SourceNode,
    source_generation: u64,
    source_view: Arc<SourceViewGate>,
    promoted: Mutex<Option<Arc<dyn MountOpenFile>>>,
}

struct ViewBoundOpenFile {
    inner: Arc<dyn MountOpenFile>,
    runtime: Arc<CallbackRuntime>,
    source_view: Arc<SourceViewGate>,
    generation: u64,
}

impl ViewBoundOpenFile {
    fn with_view<T>(
        &self,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            let generation = self.generation;
            async move { source_view.read_for_callback(owner, Some(generation)).await }
        })?;
        operation(self.inner.as_ref())
    }

    fn with_mutation<T>(
        &self,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            let generation = self.generation;
            async move { source_view.write_stable(Some(generation)).await }
        })?;
        operation(self.inner.as_ref())
    }
}

impl MountOpenFile for ViewBoundOpenFile {
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.with_view(MountOpenFile::lookup)
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        self.with_view(|file| file.read_range(offset, length))
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
        self.with_view(|file| file.read_up_to(offset, maximum_bytes))
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.with_view(|file| file.seek(offset, target))
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.write_range(offset, bytes))
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.resize(logical_bytes))
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.allocate_range(offset, length, operation))
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.set_attributes(metadata, logical_bytes))
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        self.with_view(|file| file.read_attribute(name))
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.with_view(|file| file.list_attributes(cursor, maximum_entries))
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.write_attribute(name, value, mode))
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.remove_attribute(name))
    }
}

impl<A, O, D, S> LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn source_lease(&self) -> Result<SourceViewLease, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move {
                source_view
                    .read_for_callback(owner, Some(self.source_generation))
                    .await
            }
        })
    }

    fn with_authored<T>(
        &self,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.write_stable(Some(self.source_generation)).await }
        })?;
        let mut promoted = self.promoted.lock().map_err(|_| MountSourceError::Stale)?;
        if promoted.is_none() {
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
            *promoted = Some(self.authored.open_file(&self.mount_path)?);
        }
        let file = promoted.as_ref().ok_or(MountSourceError::Stale)?;
        operation(file.as_ref())
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
        let _lease = self.source_lease()?;
        if let Some(file) = self
            .promoted
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .as_ref()
            .cloned()
        {
            return file.lookup();
        }
        #[cfg(unix)]
        {
            let file_id = self.lazy.source_file_id(&self.source_node);
            Ok(mount_lookup(LazyLookup::Source(self.source_node), file_id))
        }
        #[cfg(not(unix))]
        self.runtime.wait(|| async {
            let lookup = self.lazy.lookup(&self.path).await.map_err(lazy_error)?;
            let file_id = self
                .lazy
                .stable_file_id_for_lookup(&self.path, &lookup)
                .await
                .map_err(lazy_error)?;
            Ok(mount_lookup(lookup, file_id))
        })
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self
            .promoted
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .as_ref()
            .cloned()
        {
            return file.read_range(offset, length);
        }
        #[cfg(unix)]
        {
            self.runtime.wait(|| async {
                self.lazy
                    .read_source_range(
                        &self.path,
                        self.source,
                        self.source_node,
                        offset,
                        u64::from(length),
                    )
                    .await
                    .map_err(lazy_error)
            })
        }
        #[cfg(not(unix))]
        self.runtime.wait(|| async {
            self.lazy
                .read_range(&self.path, offset, u64::from(length))
                .await
                .map_err(lazy_error)
        })
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self
            .promoted
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .as_ref()
            .cloned()
        {
            return file.read_up_to(offset, maximum_bytes);
        }
        #[cfg(unix)]
        {
            let length = self
                .source_node
                .logical_bytes
                .ok_or_else(|| MountSourceError::Invalid("source is not regular".to_owned()))?
                .saturating_sub(offset)
                .min(u64::from(maximum_bytes));
            if length == 0 {
                return Ok(Bytes::new());
            }
            self.runtime.wait(|| async {
                self.lazy
                    .read_source_range(&self.path, self.source, self.source_node, offset, length)
                    .await
                    .map_err(lazy_error)
            })
        }
        #[cfg(not(unix))]
        self.runtime.wait(|| async {
            let lookup = self.lazy.lookup(&self.path).await.map_err(lazy_error)?;
            let logical_bytes = match lookup {
                LazyLookup::Authored { stat, .. } => stat.logical_bytes,
                LazyLookup::Shadow { record, .. } => match record.payload {
                    FilePayload::InlineRegular(data) => {
                        Some(u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX))
                    }
                    FilePayload::Regular { logical_bytes, .. } => Some(logical_bytes),
                    _ => None,
                },
                LazyLookup::Source(node) => node.logical_bytes,
            }
            .ok_or_else(|| MountSourceError::Invalid("source is not regular".to_owned()))?;
            let length = logical_bytes
                .saturating_sub(offset)
                .min(u64::from(maximum_bytes));
            if length == 0 {
                return Ok(Bytes::new());
            }
            self.lazy
                .read_range(&self.path, offset, length)
                .await
                .map_err(lazy_error)
        })
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self
            .promoted
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .as_ref()
            .cloned()
        {
            return file.seek(offset, target);
        }
        #[cfg(unix)]
        {
            let length = self.source_node.logical_bytes.ok_or_else(|| {
                MountSourceError::Invalid("seek requires a regular file".to_owned())
            })?;
            if offset >= length {
                return Ok(None);
            }
            Ok(Some(match target {
                MountSeekTarget::Data => offset,
                MountSeekTarget::Hole => length,
            }))
        }
        #[cfg(not(unix))]
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
        self.with_authored(|file| file.write_range(offset, bytes))
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.resize(logical_bytes))
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.allocate_range(offset, length, operation))
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.set_attributes(metadata, logical_bytes))
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        self.with_authored(|file| file.read_attribute(name))
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.with_authored(|file| file.list_attributes(cursor, maximum_entries))
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.write_attribute(name, value, mode))
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.remove_attribute(name))
    }
}

impl<A, O, D, S> MountFilesystem for LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn supports_posix_named_attributes(&self) -> bool {
        self.authored.supports_posix_named_attributes()
    }

    fn flush_on_handle_close(&self) -> bool {
        false
    }

    fn view_is_stable(&self) -> bool {
        self.source_view.is_stable()
    }

    fn view_epoch(&self) -> Option<u64> {
        self.authored.view_epoch()
    }

    fn binding_epoch(&self) -> Option<u64> {
        Some(self.source_view.generation())
    }

    fn acquire_view_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        let lease = self.view_lease(None)?;
        if self.authored.view_epoch() != expected_epoch {
            return Err(MountSourceError::Stale);
        }
        Ok(Box::new(lease))
    }

    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        self.view_lease(expected_epoch)
            .map(|lease| Box::new(lease) as Box<dyn MountViewLease>)
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        let _lease = self.view_lease(None)?;
        // Newly authored objects are visible before the operation barrier has
        // published them into the lazy view. This is required for NFS CREATE
        // compounds, which immediately GETATTR the returned filehandle.
        if let Some(lookup) = self.authored.lookup(path)? {
            return Ok(Some(lookup));
        }
        let path = self.path(path)?;
        self.wait(|| async move {
            #[cfg(unix)]
            let lookup = self.lazy.inspect(&path).await;
            #[cfg(not(unix))]
            let lookup = self.lazy.lookup(&path).await;
            match lookup {
                Ok(lookup) => {
                    let file_id = self
                        .lazy
                        .stable_file_id_for_lookup(&path, &lookup)
                        .await
                        .map_err(lazy_error)?;
                    Ok(Some(mount_lookup(lookup, file_id)))
                }
                Err(LazyWorkspaceError::NotFound) => Ok(None),
                Err(error) => Err(lazy_error(error)),
            }
        })
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let lease = self.view_lease(None)?;
        // A create returns an open handle before the operation barrier publishes the authored
        // generation. Consult the authored checkout first so that the just-created file can be
        // written through that handle instead of falling through to the still-unaware lazy view.
        if self.authored.lookup(path)?.is_some() {
            return self
                .authored
                .open_file(path)
                .map(|file| self.bind_open_file(file, lease.generation));
        }
        let path_text = self.path(path)?;
        let lookup_path = path_text.clone();
        #[cfg(unix)]
        let (lookup, source) = self.wait(|| async move {
            self.lazy
                .inspect_resolved(&lookup_path)
                .await
                .map_err(lazy_error)
        })?;
        #[cfg(not(unix))]
        let (lookup, source) = self.wait(|| async move {
            self.lazy
                .lookup_resolved(&lookup_path)
                .await
                .map_err(lazy_error)
        })?;
        let source_generation = lease.generation;
        match lookup {
            LazyLookup::Authored {
                path: authored_path,
                ..
            } if authored_path == path_text => self
                .authored
                .open_file(path)
                .map(|file| self.bind_open_file(file, source_generation)),
            LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
                drop(lease);
                let _mutation = self.mutation_lease(Some(source_generation))?;
                self.promote_locked(path)?;
                self.authored
                    .open_file(path)
                    .map(|file| self.bind_open_file(file, source_generation))
            }
            LazyLookup::Source(node) if node.kind == SourceNodeKind::RegularFile => {
                let _source = source.ok_or(MountSourceError::Stale)?;
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
                    #[cfg(unix)]
                    source: _source,
                    #[cfg(unix)]
                    source_node: node,
                    source_generation,
                    source_view: Arc::clone(&self.source_view),
                    promoted: Mutex::new(None),
                }))
            }
            LazyLookup::Source(_) => Err(MountSourceError::Invalid(
                "open requires a regular file".to_owned(),
            )),
        }
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let lease = self.mutation_lease(None)?;
        self.promote_locked(path)?;
        self.authored
            .detach_file(path)
            .map(|file| self.bind_open_file(file, lease.generation))
    }

    fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError> {
        let _lease = self.view_lease(None)?;
        let path = self.path(path)?;
        self.wait(|| async move { self.lazy.read_link(&path).await.map_err(lazy_error) })
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountSourceError> {
        let _lease = self.view_lease(None)?;
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
        let _lease = self.view_lease(None)?;
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
        let lease = self.view_lease(None)?;
        let path = self.path(path)?;
        let cursor = self.take_cursor(lease.generation, cursor)?;
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
                let lookup = self.lazy.inspect(&child).await.map_err(lazy_error)?;
                let file_id = self
                    .lazy
                    .stable_file_id_for_lookup(&child, &lookup)
                    .await
                    .map_err(lazy_error)?;
                Ok(mount_lookup(lookup, file_id))
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
                .map(|cursor| self.remember_cursor(lease.generation, cursor))
                .transpose()?,
        })
    }

    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        self.authored.create_file(path, metadata)
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        self.authored.create_directory(path, metadata)
    }

    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: Bytes,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        self.authored.create_symbolic_link(path, target, metadata)
    }

    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        self.authored.create_special(path, kind, device, metadata)
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(path)?;
        self.authored.set_attributes(path, metadata, logical_bytes)
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
    ) -> Result<Option<Bytes>, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(path)?;
        self.authored.read_attribute(path, name)
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(path)?;
        self.authored.list_attributes(path, cursor, maximum_entries)
    }

    fn write_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(path)?;
        self.authored.write_attribute(path, name, value, mode)
    }

    fn remove_attribute(&self, path: &MountPath, name: &[u8]) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(path)?;
        self.authored.remove_attribute(path, name)
    }

    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(path)?;
        self.authored.write_range(path, offset, bytes)
    }

    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(path)?;
        self.authored.resize(path, logical_bytes)
    }

    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(path)?;
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
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(source)?;
        self.promote_locked(destination)?;
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
        let _mutation = self.mutation_lease(None)?;
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
        let _mutation = self.mutation_lease(None)?;
        if self.authored.lookup(source)?.is_none() {
            let source_text = self.path(source)?;
            let source_lookup = self
                .wait(|| async move { self.lazy.lookup(&source_text).await.map_err(lazy_error) })?;
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
        }
        self.promote_locked(source)?;
        self.promote_parents_locked(destination)?;
        self.authored.rename(source, destination, replace)
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(source)?;
        self.promote_parents_locked(destination)?;
        self.authored.hard_link(source, destination)
    }

    fn flush(&self) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.authored.flush()
    }

    fn capture_host_path(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        self.capture_host_paths(source_root, std::slice::from_ref(path))
    }

    fn capture_host_paths(
        &self,
        source_root: &Path,
        paths: &[MountPath],
    ) -> Result<(), MountSourceError> {
        if paths.is_empty() {
            return Ok(());
        }
        let expected_root_identity = capture_root_identity(source_root)
            .map_err(|error| MountSourceError::Engine(error.to_string()))?;
        self.capture_host_paths_with_identity(source_root, paths, expected_root_identity)
    }

    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        match self.promote_locked(path) {
            Ok(()) | Err(MountSourceError::NotFound) => {}
            Err(error) => return Err(error),
        }
        self.authored.capture_host_subtree(source_root, path)
    }
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(crate) fn capture_host_paths_with_identity(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
    ) -> Result<(), MountSourceError> {
        if paths.is_empty() {
            return Ok(());
        }
        let _mutation = self.mutation_lease(None)?;
        for path in paths {
            self.promote_parents_locked(path)?;
            match self.promote_locked(path) {
                Ok(()) | Err(MountSourceError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        self.authored
            .capture_host_paths_with_identity(source_root, paths, expected_root_identity)
    }

    fn bind_open_file(
        &self,
        file: Arc<dyn MountOpenFile>,
        generation: u64,
    ) -> Arc<dyn MountOpenFile> {
        Arc::new(ViewBoundOpenFile {
            inner: file,
            runtime: Arc::clone(&self.runtime),
            source_view: Arc::clone(&self.source_view),
            generation,
        })
    }

    fn mutation_lease(
        &self,
        expected: Option<u64>,
    ) -> Result<SourceMutationLease, MountSourceError> {
        self.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.write_stable(expected).await }
        })
    }

    fn view_lease(&self, expected: Option<u64>) -> Result<SourceViewLease, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        self.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.read_for_callback(owner, expected).await }
        })
    }

    fn promote_locked(&self, path: &MountPath) -> Result<(), MountSourceError> {
        if self.authored.lookup(path)?.is_some() {
            return Ok(());
        }
        let text = self.path(path)?;
        self.wait(|| async move { self.promote(&text).await })?;
        Ok(())
    }
}

fn mount_lookup(lookup: LazyLookup, file_id: FileId) -> MountLookup {
    match lookup {
        LazyLookup::Authored { stat, .. } => MountLookup {
            node: MountNode {
                file_id,
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
                file_id,
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
                file_id,
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
        let first = cursors.remember(2, 1_u8).expect("first cursor");
        let second = cursors.remember(2, 2_u8).expect("second cursor");
        let third = cursors.remember(2, 3_u8).expect("third cursor");

        assert!(matches!(
            cursors.take(2, Some(&first)),
            Err(MountSourceError::Stale)
        ));
        assert_eq!(
            cursors.take(2, Some(&second)).expect("second token"),
            Some(2)
        );
        assert_eq!(cursors.take(2, Some(&third)).expect("third token"), Some(3));

        let stale = cursors.remember(2, 4_u8).expect("stale cursor");
        assert!(matches!(
            cursors.take(4, Some(&stale)),
            Err(MountSourceError::Stale)
        ));
        let retained = cursors.remember(2, 5_u8).expect("retained cursor");
        cursors.clear();
        assert!(matches!(
            cursors.take(2, Some(&retained)),
            Err(MountSourceError::Stale)
        ));
    }

    #[tokio::test]
    async fn source_view_gate_serializes_rebinds_and_fences_old_handles() {
        let gate = Arc::new(SourceViewGate::new());
        let initial = Arc::clone(&gate).read(None).await.expect("initial lease");
        assert_eq!(initial.generation, SourceViewGate::INITIAL_GENERATION);

        let waiting_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { waiting_gate.write().await });
        tokio::task::yield_now().await;
        assert!(
            !writer.is_finished(),
            "a rebind must wait for active readers"
        );
        drop(initial);

        let writer = writer.await.expect("writer task");
        gate.begin_transition();
        assert!(!gate.is_stable());
        gate.finish_transition();
        drop(writer);

        assert!(matches!(
            Arc::clone(&gate)
                .read(Some(SourceViewGate::INITIAL_GENERATION))
                .await,
            Err(MountSourceError::Stale)
        ));
        assert_eq!(
            Arc::clone(&gate)
                .read(None)
                .await
                .expect("new lease")
                .generation,
            SourceViewGate::INITIAL_GENERATION + 2
        );
    }

    #[tokio::test]
    async fn source_view_gate_excludes_authored_mutations_from_read_callbacks() {
        let gate = Arc::new(SourceViewGate::new());
        let reader = Arc::clone(&gate).read(None).await.expect("reader lease");
        let mutation_gate = Arc::clone(&gate);
        let mutation = tokio::spawn(async move {
            mutation_gate
                .write_stable(Some(SourceViewGate::INITIAL_GENERATION))
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !mutation.is_finished(),
            "an authored mutation must wait for active read callbacks"
        );
        drop(reader);

        let mutation = mutation
            .await
            .expect("mutation task")
            .expect("mutation lease");
        assert_eq!(mutation.generation, SourceViewGate::INITIAL_GENERATION);
        let reader_gate = Arc::clone(&gate);
        let reader = tokio::spawn(async move { reader_gate.read(None).await });
        tokio::task::yield_now().await;
        assert!(
            !reader.is_finished(),
            "a read callback must wait for an authored mutation"
        );
        drop(mutation);
        assert_eq!(
            reader
                .await
                .expect("reader task")
                .expect("reader lease")
                .generation,
            SourceViewGate::INITIAL_GENERATION
        );
    }

    #[tokio::test]
    async fn source_view_gate_allows_nested_callback_reads_while_writer_waits() {
        let gate = Arc::new(SourceViewGate::new());
        let owner = SourceViewGate::callback_owner();
        let outer = Arc::clone(&gate)
            .read_for_callback(owner, None)
            .await
            .expect("outer lease");
        let writer_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { writer_gate.write().await });
        tokio::task::yield_now().await;
        assert!(!writer.is_finished(), "writer must wait for outer callback");

        let nested = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            Arc::clone(&gate).read_for_callback(owner, Some(outer.generation)),
        )
        .await
        .expect("a callback must not deadlock when its source method reacquires the view")
        .expect("nested lease");
        drop(nested);
        drop(outer);
        drop(writer.await.expect("writer task"));
    }

    #[tokio::test]
    async fn source_view_gate_gives_a_waiting_writer_priority_over_new_callbacks() {
        let gate = Arc::new(SourceViewGate::new());
        let active = Arc::clone(&gate).read(None).await.expect("active reader");
        let writer_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { writer_gate.write().await });
        tokio::task::yield_now().await;
        assert!(!writer.is_finished(), "writer must wait for active reader");

        let later_gate = Arc::clone(&gate);
        let later = tokio::spawn(async move { later_gate.read(None).await });
        tokio::task::yield_now().await;
        assert!(
            !later.is_finished(),
            "a new callback bypassed a waiting writer"
        );

        drop(active);
        let writer = tokio::time::timeout(std::time::Duration::from_secs(1), writer)
            .await
            .expect("writer made no progress")
            .expect("writer task");
        assert!(!later.is_finished(), "reader crossed the active writer");
        drop(writer);
        drop(
            tokio::time::timeout(std::time::Duration::from_secs(1), later)
                .await
                .expect("later reader made no progress")
                .expect("reader task")
                .expect("reader lease"),
        );
    }
}
