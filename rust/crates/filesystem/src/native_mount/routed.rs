//! One native mount fronting many independently checked-out sources.
//!
//! [`RoutedMountSource`] dispatches every callback by the first path
//! component ("the route name") to a live-mutable set of child
//! [`MountFilesystem`] sources. It exists so N logical checkouts (for
//! example, N repository forks) can share exactly one native mount session:
//! adding or removing a checkout becomes a map mutation instead of a
//! `mount_native`/unmount cycle.

use super::{
    MountAttributePage, MountAttributeWriteMode, MountDirectoryEntry, MountDirectoryPage,
    MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile, MountPath,
    MountRangeAllocation, MountSeekTarget, MountSourceError,
};
use crate::FileId;
use crate::kernel::FileMetadata;
use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

static ROUTE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Derives one process-unique 16-byte tag for a freshly added route.
///
/// Uniqueness comes from the monotonic sequence, not from hashing `name`;
/// the name only perturbs the tag for readability under a debugger.
fn next_route_tag(name: &[u8]) -> [u8; 16] {
    let sequence = ROUTE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    let mut tag = [0u8; 16];
    tag[..8].copy_from_slice(&sequence.to_le_bytes());
    tag[8..].copy_from_slice(&hasher.finish().to_le_bytes());
    tag
}

/// XORs one [`FileId`] with a route tag. Self-inverse: applying it twice
/// with the same tag returns the original identity.
fn remap_file_id(id: FileId, tag: [u8; 16]) -> FileId {
    let mut bytes = id.into_bytes();
    for (byte, tag_byte) in bytes.iter_mut().zip(tag) {
        *byte ^= tag_byte;
    }
    FileId::from_bytes(bytes)
}

fn cross_route_error() -> MountSourceError {
    MountSourceError::Unsupported("operation spans two different routes".to_owned())
}

struct Route {
    source: Arc<dyn MountFilesystem>,
    tag: [u8; 16],
}

/// One route resolved from an incoming [`MountPath`]: the child source, its
/// remap tag, its route name, and the remainder of the path within it.
struct Routed {
    source: Arc<dyn MountFilesystem>,
    tag: [u8; 16],
    name: Vec<u8>,
    sub_path: MountPath,
}

/// A [`MountFilesystem`] that routes by first path component to a
/// live-mutable set of child sources, each exposed as a subdirectory of one
/// synthetic root.
///
/// Every [`FileId`] a route hands back is `XOR`ed with that route's tag before
/// leaving this type, since routes fronting checkouts of the same volume
/// otherwise return colliding stable ids for unchanged files. The mapping is
/// deliberately one-way: `clone_range_by_id` and `remove`'s expected-id
/// precondition resolve a remapped id back to its owning route through an
/// index recorded every time an id is emitted, then undo the same XOR.
pub struct RoutedMountSource {
    routes: RwLock<BTreeMap<Vec<u8>, Route>>,
    file_id_index: RwLock<HashMap<FileId, Vec<u8>>>,
    root_id: FileId,
    /// Advances on every route change and is served as the synthetic root's
    /// mtime/ctime: kernels re-validate cached children when the parent
    /// changes, which is what makes a removed route disappear promptly.
    revision: AtomicI64,
}

impl RoutedMountSource {
    /// Creates one router with no routes.
    #[must_use]
    pub fn new() -> Self {
        Self {
            revision: AtomicI64::new(unix_nanos()),
            routes: RwLock::new(BTreeMap::new()),
            file_id_index: RwLock::new(HashMap::new()),
            root_id: FileId::new(),
        }
    }

    /// Adds one route under `name`, exposed as `/name` from the mount root.
    ///
    /// # Errors
    ///
    /// Returns [`MountSourceError::Invalid`] for an empty or `.`/`..` name,
    /// and [`MountSourceError::AlreadyExists`] for a name already routed.
    pub fn add_route(
        &self,
        name: Vec<u8>,
        source: Arc<dyn MountFilesystem>,
    ) -> Result<(), MountSourceError> {
        if name.is_empty() || name == b"." || name == b".." {
            return Err(MountSourceError::Invalid(
                "route name must be a non-empty, non-relative path component".to_owned(),
            ));
        }
        let tag = next_route_tag(&name);
        let mut routes = self.routes.write().unwrap_or_else(PoisonError::into_inner);
        if routes.contains_key(&name) {
            return Err(MountSourceError::AlreadyExists);
        }
        routes.insert(name, Route { source, tag });
        Ok(())
    }

    /// Removes the route named `name`, if any. Open handles into it keep
    /// working detached; only new lookups stop finding it.
    ///
    /// Returns whether a route was actually removed.
    pub fn remove_route(&self, name: &[u8]) -> bool {
        self.bump_revision();
        let removed = {
            let mut routes = self.routes.write().unwrap_or_else(PoisonError::into_inner);
            routes.remove(name)
        };
        if removed.is_some() {
            let mut index = self
                .file_id_index
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            index.retain(|_, owner| owner.as_slice() != name);
        }
        removed.is_some()
    }

    /// Returns whether no route is currently registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }

    fn locate(
        &self,
        name: &[u8],
    ) -> Result<(Arc<dyn MountFilesystem>, [u8; 16]), MountSourceError> {
        let routes = self.routes.read().unwrap_or_else(PoisonError::into_inner);
        let route = routes.get(name).ok_or(MountSourceError::NotFound)?;
        Ok((Arc::clone(&route.source), route.tag))
    }

    /// Splits `path` into its route and remainder. `Ok(None)` means `path`
    /// is the synthetic root itself.
    ///
    /// Unlike [`MountFilesystem::lookup`]'s usual "authentically absent"
    /// convention, an unrecognized route name is reported as
    /// [`MountSourceError::NotFound`] here rather than as an absent-but-valid
    /// result: it means there is no such mounted filesystem to query at all,
    /// not that a query against a known filesystem came back empty.
    fn route(&self, path: &MountPath) -> Result<Option<Routed>, MountSourceError> {
        let Some((name, rest)) = path.components().split_first() else {
            return Ok(None);
        };
        let (source, tag) = self.locate(name)?;
        let mut sub_path = MountPath::root();
        for component in rest {
            sub_path = sub_path.child(component.clone());
        }
        Ok(Some(Routed {
            source,
            tag,
            name: name.clone(),
            sub_path,
        }))
    }

    fn record_file_id(&self, id: FileId, name: Vec<u8>) {
        let mut index = self
            .file_id_index
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        index.insert(id, name);
    }

    fn remap_lookup(&self, mut lookup: MountLookup, tag: [u8; 16], name: &[u8]) -> MountLookup {
        lookup.node.file_id = remap_file_id(lookup.node.file_id, tag);
        self.record_file_id(lookup.node.file_id, name.to_vec());
        lookup
    }

    fn synthetic_root_lookup(&self) -> MountLookup {
        let revision = self.revision.load(Ordering::Acquire);
        let metadata = FileMetadata {
            modified_ns: crate::kernel::MetadataField::Value(revision),
            changed_ns: crate::kernel::MetadataField::Value(revision),
            ..FileMetadata::default()
        };
        MountLookup {
            node: MountNode {
                file_id: self.root_id,
                kind: MountNodeKind::Directory,
                logical_bytes: 0,
                link_count: 1,
                device: None,
            },
            metadata,
        }
    }

    fn bump_revision(&self) {
        self.revision.store(unix_nanos(), Ordering::Release);
    }

    #[allow(clippy::type_complexity)]
    fn read_synthetic_root(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let mut candidates: Vec<(Vec<u8>, Arc<dyn MountFilesystem>, [u8; 16])> = {
            let routes = self.routes.read().unwrap_or_else(PoisonError::into_inner);
            routes
                .iter()
                .filter(|(name, _)| cursor.is_none_or(|after| name.as_slice() > after))
                .map(|(name, route)| (name.clone(), Arc::clone(&route.source), route.tag))
                .collect()
        };
        let limit = usize::try_from(maximum_entries).unwrap_or(usize::MAX);
        let has_more = candidates.len() > limit;
        candidates.truncate(limit);
        let mut entries = Vec::with_capacity(candidates.len());
        for (name, source, tag) in candidates {
            let Some(lookup) = source.lookup(&MountPath::root())? else {
                continue;
            };
            let remapped = self.remap_lookup(lookup, tag, &name);
            entries.push(MountDirectoryEntry {
                name,
                node: remapped.node,
                metadata: remapped.metadata,
            });
        }
        let next_cursor = has_more
            .then(|| entries.last().map(|entry| entry.name.clone()))
            .flatten();
        Ok(MountDirectoryPage {
            entries,
            next_cursor,
        })
    }
}

impl Default for RoutedMountSource {
    fn default() -> Self {
        Self::new()
    }
}

struct RemappedOpenFile {
    inner: Arc<dyn MountOpenFile>,
    tag: [u8; 16],
}

impl MountOpenFile for RemappedOpenFile {
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        let mut lookup = self.inner.lookup()?;
        lookup.node.file_id = remap_file_id(lookup.node.file_id, self.tag);
        Ok(lookup)
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        self.inner.read_range(offset, length)
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.inner.seek(offset, target)
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.inner.write_range(offset, bytes)
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.inner.resize(logical_bytes)
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.inner.allocate_range(offset, length, operation)
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.inner.set_attributes(metadata, logical_bytes)
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        self.inner.read_attribute(name)
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.inner.list_attributes(cursor, maximum_entries)
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.inner.write_attribute(name, value, mode)
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        self.inner.remove_attribute(name)
    }
}

impl MountFilesystem for RoutedMountSource {
    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Ok(Some(self.synthetic_root_lookup()));
        };
        let result = routed.source.lookup(&routed.sub_path)?;
        Ok(result.map(|lookup| self.remap_lookup(lookup, routed.tag, &routed.name)))
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Invalid(
                "the synthetic mount root is not a regular file".to_owned(),
            ));
        };
        let inner = routed.source.open_file(&routed.sub_path)?;
        Ok(Arc::new(RemappedOpenFile {
            inner,
            tag: routed.tag,
        }))
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Invalid(
                "the synthetic mount root is not a regular file".to_owned(),
            ));
        };
        let inner = routed.source.detach_file(&routed.sub_path)?;
        Ok(Arc::new(RemappedOpenFile {
            inner,
            tag: routed.tag,
        }))
    }

    fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Invalid(
                "the synthetic mount root is not a symbolic link".to_owned(),
            ));
        };
        routed.source.read_link(&routed.sub_path)
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Invalid(
                "the synthetic mount root has no content".to_owned(),
            ));
        };
        routed.source.read_range(&routed.sub_path, offset, length)
    }

    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Invalid(
                "the synthetic mount root has no content".to_owned(),
            ));
        };
        routed.source.seek(&routed.sub_path, offset, target)
    }

    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return self.read_synthetic_root(cursor, maximum_entries);
        };
        let page = routed
            .source
            .read_directory(&routed.sub_path, cursor, maximum_entries)?;
        let entries = page
            .entries
            .into_iter()
            .map(|mut entry| {
                entry.node.file_id = remap_file_id(entry.node.file_id, routed.tag);
                self.record_file_id(entry.node.file_id, routed.name.clone());
                entry
            })
            .collect();
        Ok(MountDirectoryPage {
            entries,
            next_cursor: page.next_cursor,
        })
    }

    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "routes are managed through add_route, not directory creation".to_owned(),
            ));
        };
        let lookup = routed.source.create_file(&routed.sub_path, metadata)?;
        Ok(self.remap_lookup(lookup, routed.tag, &routed.name))
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "routes are managed through add_route, not directory creation".to_owned(),
            ));
        };
        let lookup = routed.source.create_directory(&routed.sub_path, metadata)?;
        Ok(self.remap_lookup(lookup, routed.tag, &routed.name))
    }

    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: Bytes,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "routes are managed through add_route, not directory creation".to_owned(),
            ));
        };
        let lookup = routed
            .source
            .create_symbolic_link(&routed.sub_path, target, metadata)?;
        Ok(self.remap_lookup(lookup, routed.tag, &routed.name))
    }

    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "routes are managed through add_route, not directory creation".to_owned(),
            ));
        };
        let lookup = routed
            .source
            .create_special(&routed.sub_path, kind, device, metadata)?;
        Ok(self.remap_lookup(lookup, routed.tag, &routed.name))
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "the synthetic mount root has no settable attributes".to_owned(),
            ));
        };
        routed
            .source
            .set_attributes(&routed.sub_path, metadata, logical_bytes)
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
    ) -> Result<Option<Bytes>, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Ok(None);
        };
        routed.source.read_attribute(&routed.sub_path, name)
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Ok(MountAttributePage {
                names: Vec::new(),
                next_cursor: None,
            });
        };
        routed
            .source
            .list_attributes(&routed.sub_path, cursor, maximum_entries)
    }

    fn write_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "the synthetic mount root has no settable attributes".to_owned(),
            ));
        };
        routed
            .source
            .write_attribute(&routed.sub_path, name, value, mode)
    }

    fn remove_attribute(&self, path: &MountPath, name: &[u8]) -> Result<(), MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "the synthetic mount root has no settable attributes".to_owned(),
            ));
        };
        routed.source.remove_attribute(&routed.sub_path, name)
    }

    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "the synthetic mount root has no content".to_owned(),
            ));
        };
        routed.source.write_range(&routed.sub_path, offset, bytes)
    }

    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "the synthetic mount root has no content".to_owned(),
            ));
        };
        routed.source.resize(&routed.sub_path, logical_bytes)
    }

    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "the synthetic mount root has no content".to_owned(),
            ));
        };
        routed
            .source
            .allocate_range(&routed.sub_path, offset, length, operation)
    }

    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let source_routed = self.route(source)?.ok_or_else(cross_route_error)?;
        let destination_routed = self.route(destination)?.ok_or_else(cross_route_error)?;
        if source_routed.name != destination_routed.name {
            return Err(cross_route_error());
        }
        source_routed.source.clone_range(
            &source_routed.sub_path,
            source_offset,
            &destination_routed.sub_path,
            destination_offset,
            length,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn clone_range_by_id(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let (source_name, destination_name) = {
            let index = self
                .file_id_index
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            let source_name = index
                .get(&source_file_id)
                .cloned()
                .ok_or(MountSourceError::NotFound)?;
            let destination_name = index
                .get(&destination_file_id)
                .cloned()
                .ok_or(MountSourceError::NotFound)?;
            (source_name, destination_name)
        };
        if source_name != destination_name {
            return Err(cross_route_error());
        }
        let (source, tag) = self.locate(&source_name)?;
        source.clone_range_by_id(
            remap_file_id(source_file_id, tag),
            source_offset,
            remap_file_id(destination_file_id, tag),
            destination_offset,
            length,
        )
    }

    fn remove(&self, path: &MountPath, expected: Option<FileId>) -> Result<(), MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "routes are removed through remove_route, not unlink".to_owned(),
            ));
        };
        let expected = expected.map(|id| remap_file_id(id, routed.tag));
        routed.source.remove(&routed.sub_path, expected)
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
    ) -> Result<(), MountSourceError> {
        let source_routed = self.route(source)?.ok_or_else(cross_route_error)?;
        let destination_routed = self.route(destination)?.ok_or_else(cross_route_error)?;
        if source_routed.name != destination_routed.name {
            return Err(cross_route_error());
        }
        source_routed.source.rename(
            &source_routed.sub_path,
            &destination_routed.sub_path,
            replace,
        )
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
    ) -> Result<(), MountSourceError> {
        let source_routed = self.route(source)?.ok_or_else(cross_route_error)?;
        let destination_routed = self.route(destination)?.ok_or_else(cross_route_error)?;
        if source_routed.name != destination_routed.name {
            return Err(cross_route_error());
        }
        source_routed
            .source
            .hard_link(&source_routed.sub_path, &destination_routed.sub_path)
    }

    fn flush(&self) -> Result<(), MountSourceError> {
        let sources: Vec<Arc<dyn MountFilesystem>> = self
            .routes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|route| Arc::clone(&route.source))
            .collect();
        for source in sources {
            source.flush()?;
        }
        Ok(())
    }

    fn capture_host_path(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "the synthetic mount root cannot capture host state".to_owned(),
            ));
        };
        routed
            .source
            .capture_host_path(source_root, &routed.sub_path)
    }
}

fn unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::NamespacePath;
    use crate::model::{
        AccessMode, CheckoutMode, ConsistencyMode, FilesystemProfile, GenerationSelector,
        Lifecycle, MutationMode, VolumeConfig, VolumeLimits,
    };
    use crate::path::PortablePath;
    use crate::{
        CancellationToken, Checkout, CheckoutMountSource, Fs, MemoryAuthorityStore,
        MemoryObjectStore, SharedCheckout, WorkBudget,
    };

    type MemoryCheckout = Checkout<MemoryAuthorityStore, MemoryObjectStore>;

    fn metadata() -> FileMetadata {
        FileMetadata::default()
    }

    fn component(name: &str) -> Vec<u8> {
        #[cfg(target_os = "windows")]
        {
            name.encode_utf16().flat_map(u16::to_le_bytes).collect()
        }
        #[cfg(not(target_os = "windows"))]
        {
            name.as_bytes().to_vec()
        }
    }

    fn test_path(name: &str) -> MountPath {
        MountPath::root().child(component(name))
    }

    fn namespace_path(
        limits: VolumeLimits,
        name: &str,
    ) -> Result<NamespacePath, Box<dyn std::error::Error>> {
        Ok(NamespacePath::from_portable(
            &PortablePath::parse(&format!("/{name}"), limits)?,
            limits,
        )?)
    }

    fn wrap(
        config: VolumeConfig,
        checkout: MemoryCheckout,
    ) -> Result<Arc<dyn MountFilesystem>, Box<dyn std::error::Error>> {
        let shared = Arc::new(SharedCheckout::new(checkout));
        Ok(Arc::new(CheckoutMountSource::new(shared, config)?))
    }

    fn memory_source() -> Result<Arc<dyn MountFilesystem>, Box<dyn std::error::Error>> {
        memory_source_with_profile(FilesystemProfile::Portable)
    }

    fn memory_source_with_profile(
        profile: FilesystemProfile,
    ) -> Result<Arc<dyn MountFilesystem>, Box<dyn std::error::Error>> {
        let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = profile;
        let fs = Fs::memory();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let checkout = runtime.block_on(async {
            let cancellation = CancellationToken::new();
            let volume = fs
                .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            volume
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode {
                        access: AccessMode::ReadWrite,
                        consistency: ConsistencyMode::Pinned,
                        mutations: MutationMode::PrivateOverlay,
                    },
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map(|receipt| receipt.value)
        })?;
        wrap(config, checkout)
    }

    /// Two independent checkouts of one volume that already shares a file
    /// at the same generation, reproducing the real `FileId` collision case
    /// that motivates remapping: forks are checkouts of one volume.
    #[allow(clippy::type_complexity)]
    fn two_routes_over_same_volume()
    -> Result<(Arc<dyn MountFilesystem>, Arc<dyn MountFilesystem>), Box<dyn std::error::Error>>
    {
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let limits = config.limits;
        let fs = Fs::memory();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let (checkout_a, checkout_b) = runtime.block_on(async {
            let cancellation = CancellationToken::new();
            let volume = fs
                .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            let mode = CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            };
            let mut seed = volume
                .checkout(
                    GenerationSelector::Head,
                    mode,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value;
            seed.create_file(
                namespace_path(limits, "shared.bin")?,
                Bytes::from_static(b"same"),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
            crate::seal_checkout(
                &mut seed,
                crate::OperationId::new(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
            drop(seed);
            let a = volume
                .checkout(
                    GenerationSelector::Head,
                    mode,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value;
            let b = volume
                .checkout(
                    GenerationSelector::Head,
                    mode,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value;
            Ok::<_, Box<dyn std::error::Error>>((a, b))
        })?;
        Ok((wrap(config, checkout_a)?, wrap(config, checkout_b)?))
    }

    #[test]
    fn unknown_route_is_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let router = RoutedMountSource::new();
        router.add_route(component("a"), memory_source()?)?;
        assert!(matches!(
            router.lookup(&test_path("b")),
            Err(MountSourceError::NotFound)
        ));
        assert!(matches!(
            router.read_range(&test_path("b").child(component("x")), 0, 1),
            Err(MountSourceError::NotFound)
        ));
        Ok(())
    }

    #[test]
    fn root_lists_route_names_as_directories() -> Result<(), Box<dyn std::error::Error>> {
        let router = RoutedMountSource::new();
        router.add_route(component("a"), memory_source()?)?;
        router.add_route(component("b"), memory_source()?)?;
        let page = router.read_directory(&MountPath::root(), None, 16)?;
        let mut names: Vec<&[u8]> = page
            .entries
            .iter()
            .map(|entry| entry.name.as_slice())
            .collect();
        names.sort_unstable();
        let mut expected = [component("a"), component("b")];
        expected.sort_unstable();
        assert_eq!(
            names,
            expected.iter().map(Vec::as_slice).collect::<Vec<_>>()
        );
        assert!(
            page.entries
                .iter()
                .all(|entry| entry.node.kind == MountNodeKind::Directory)
        );
        Ok(())
    }

    #[test]
    fn dispatch_reaches_the_correct_child_source() -> Result<(), Box<dyn std::error::Error>> {
        let router = RoutedMountSource::new();
        router.add_route(component("a"), memory_source()?)?;
        let path = test_path("a").child(component("hello.txt"));
        router.create_file(&path, metadata())?;
        router.write_range(&path, 0, Bytes::from_static(b"hi"))?;
        assert_eq!(router.read_range(&path, 0, 2)?.as_ref(), b"hi");
        Ok(())
    }

    #[test]
    fn file_id_remap_is_unique_across_routes_of_the_same_volume()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source_a, source_b) = two_routes_over_same_volume()?;
        let router = RoutedMountSource::new();
        router.add_route(component("a"), source_a)?;
        router.add_route(component("b"), source_b)?;
        let a = router
            .lookup(&test_path("a").child(component("shared.bin")))?
            .ok_or("route a lost the shared file")?;
        let b = router
            .lookup(&test_path("b").child(component("shared.bin")))?
            .ok_or("route b lost the shared file")?;
        assert_ne!(a.node.file_id, b.node.file_id);
        Ok(())
    }

    #[test]
    fn add_and_remove_route_while_serving() -> Result<(), Box<dyn std::error::Error>> {
        let router = RoutedMountSource::new();
        assert!(router.is_empty());
        router.add_route(component("a"), memory_source()?)?;
        assert!(!router.is_empty());
        assert!(router.lookup(&test_path("a"))?.is_some());
        assert!(router.remove_route(&component("a")));
        assert!(router.is_empty());
        assert!(matches!(
            router.lookup(&test_path("a")),
            Err(MountSourceError::NotFound)
        ));
        assert!(!router.remove_route(&component("a")));
        Ok(())
    }

    #[test]
    fn cross_route_rename_and_hard_link_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let router = RoutedMountSource::new();
        router.add_route(component("a"), memory_source()?)?;
        router.add_route(component("b"), memory_source()?)?;
        let source = test_path("a").child(component("x.txt"));
        router.create_file(&source, metadata())?;
        let destination = test_path("b").child(component("y.txt"));
        assert!(matches!(
            router.rename(&source, &destination, false),
            Err(MountSourceError::Unsupported(_))
        ));
        assert!(matches!(
            router.hard_link(&source, &destination),
            Err(MountSourceError::Unsupported(_))
        ));
        Ok(())
    }

    #[test]
    fn clone_range_by_id_rejects_cross_route_and_unknown_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let router = RoutedMountSource::new();
        router.add_route(component("a"), memory_source()?)?;
        router.add_route(component("b"), memory_source()?)?;
        let file_a = test_path("a").child(component("x.bin"));
        let file_b = test_path("b").child(component("y.bin"));
        let a = router.create_file(&file_a, metadata())?;
        let b = router.create_file(&file_b, metadata())?;
        assert!(matches!(
            router.clone_range_by_id(a.node.file_id, 0, b.node.file_id, 0, 0),
            Err(MountSourceError::Unsupported(_))
        ));
        assert!(matches!(
            router.clone_range_by_id(FileId::new(), 0, a.node.file_id, 0, 0),
            Err(MountSourceError::NotFound)
        ));
        Ok(())
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn sub_path_reconstruction_preserves_non_utf8_multi_component_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let router = RoutedMountSource::new();
        router.add_route(
            component("a"),
            memory_source_with_profile(FilesystemProfile::Posix)?,
        )?;
        let non_utf8 = vec![b'x', 0xFFu8, b'y'];
        let directory = test_path("a").child(non_utf8.clone());
        router.create_directory(&directory, metadata())?;
        let file = directory.child(b"leaf.bin".to_vec());
        router.create_file(&file, metadata())?;
        let page = router.read_directory(&directory, None, 16)?;
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].name, b"leaf.bin");
        assert!(router.lookup(&file)?.is_some());
        Ok(())
    }
}
