//! One native mount fronting many independently checked-out sources.
//!
//! [`RoutedMountSource`] dispatches every callback by the first path
//! component ("the route name") to a live-mutable set of child
//! [`MountFilesystem`] sources. It exists so N logical checkouts (for
//! example, N repository forks) can share exactly one native mount session:
//! adding or removing a checkout becomes a map mutation instead of a
//! `mount_native`/unmount cycle.

use super::view_ledger::ViewObservers;
use super::{
    ContentSink, MountAttributePage, MountAttributeWriteMode, MountContentPin, MountDirectoryEntry,
    MountDirectoryPage, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountRangeAllocation, MountSeekTarget, MountSourceError, MountViewLease,
    ViewObserver, ViewStamp,
};
use crate::FileId;
use crate::kernel::FileMetadata;
use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, RwLock, Weak};

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

/// XORs a route tag into leading identity bytes. Self-inverse: applying it
/// twice with the same tag returns the original bytes.
fn xor_route_tag(bytes: &mut [u8], tag: [u8; 16]) {
    for (byte, tag_byte) in bytes.iter_mut().zip(tag) {
        *byte ^= tag_byte;
    }
}

/// Binds one child [`FileId`] to its route.
fn remap_file_id(id: FileId, tag: [u8; 16]) -> FileId {
    let mut bytes = id.into_bytes();
    xor_route_tag(&mut bytes, tag);
    FileId::from_bytes(bytes)
}

/// Binds one child content pin to its route, so a pin issued by a removed
/// route never validates against a re-added one.
fn remap_content_pin(pin: MountContentPin, tag: [u8; 16]) -> MountContentPin {
    let mut bytes = pin.0;
    xor_route_tag(&mut bytes, tag);
    MountContentPin(bytes)
}

fn cross_route_error() -> MountSourceError {
    MountSourceError::Unsupported("operation spans two different routes".to_owned())
}

#[cfg(windows)]
fn host_route_name(name: &[u8]) -> Result<OsString, MountSourceError> {
    use std::os::windows::ffi::OsStringExt;

    let mut units = name.chunks_exact(2);
    let wide = units
        .by_ref()
        .map(|pair| {
            let Some((&lo, rest)) = pair.split_first() else {
                return 0;
            };
            let Some(&hi) = rest.first() else {
                return 0;
            };
            u16::from_le_bytes([lo, hi])
        })
        .collect::<Vec<_>>();
    if !units.remainder().is_empty()
        || wide.is_empty()
        || wide.iter().any(|unit| matches!(*unit, 0 | 0x2f | 0x5c))
        || wide == [u16::from(b'.')]
        || wide == [u16::from(b'.'), u16::from(b'.')]
    {
        return Err(MountSourceError::Invalid("invalid route name".to_owned()));
    }
    Ok(OsString::from_wide(&wide))
}

#[cfg(unix)]
fn host_route_name(name: &[u8]) -> Result<OsString, MountSourceError> {
    use std::os::unix::ffi::OsStringExt;

    if name.is_empty() || name == b"." || name == b".." || name.contains(&0) || name.contains(&b'/')
    {
        return Err(MountSourceError::Invalid("invalid route name".to_owned()));
    }
    Ok(OsString::from_vec(name.to_vec()))
}

struct Route {
    source: Arc<dyn MountFilesystem>,
    tag: [u8; 16],
}

type CaptureRouteBatch = BTreeMap<Vec<u8>, (Arc<dyn MountFilesystem>, Vec<MountPath>)>;

/// One route resolved from an incoming [`MountPath`]: the child source, its
/// remap tag, its route name, and the remainder of the path within it.
struct Routed {
    source: Arc<dyn MountFilesystem>,
    tag: [u8; 16],
    name: Vec<u8>,
    sub_path: MountPath,
}

#[derive(Default)]
struct RouteViewState {
    readers: usize,
    writer: bool,
    waiting_writers: usize,
}

#[derive(Default)]
struct RouteViewGate {
    state: Mutex<RouteViewState>,
    changed: Condvar,
}

struct RouteViewReadLease {
    gate: Arc<RouteViewGate>,
    _children: Vec<Box<dyn MountViewLease>>,
}

impl MountViewLease for RouteViewReadLease {}

impl Drop for RouteViewReadLease {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.readers = state.readers.saturating_sub(1);
        if state.readers == 0 {
            self.gate.changed.notify_all();
        }
    }
}

struct RouteViewWriteLease {
    gate: Arc<RouteViewGate>,
}

impl Drop for RouteViewWriteLease {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.writer = false;
        self.gate.changed.notify_all();
    }
}

impl RouteViewGate {
    fn read(self: &Arc<Self>) -> RouteViewReadLease {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while state.writer || state.waiting_writers != 0 {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state.readers = state.readers.saturating_add(1);
        drop(state);
        RouteViewReadLease {
            gate: Arc::clone(self),
            _children: Vec::new(),
        }
    }

    fn write(self: &Arc<Self>) -> RouteViewWriteLease {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.waiting_writers = state.waiting_writers.saturating_add(1);
        while state.writer || state.readers != 0 {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state.waiting_writers = state.waiting_writers.saturating_sub(1);
        state.writer = true;
        drop(state);
        RouteViewWriteLease {
            gate: Arc::clone(self),
        }
    }
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
    view_gate: Arc<RouteViewGate>,
    routes: RwLock<BTreeMap<Vec<u8>, Route>>,
    file_id_index: RwLock<HashMap<FileId, Vec<u8>>>,
    root_id: FileId,
    /// Advances on every route change and is served as the synthetic root's
    /// mtime/ctime: kernels re-validate cached children when the parent
    /// changes, which is what makes a removed route disappear promptly.
    revision: AtomicI64,
    /// Position of the latest route change in the order of view changes.
    routes_changed: AtomicU64,
    /// Observers of this router, registered on every route it gains.
    observers: ViewObservers,
}

impl RoutedMountSource {
    /// Creates one router with no routes.
    #[must_use]
    pub fn new() -> Self {
        Self {
            view_gate: Arc::new(RouteViewGate::default()),
            revision: AtomicI64::new(1),
            routes_changed: AtomicU64::new(0),
            observers: ViewObservers::default(),
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
        let _view = self.view_gate.write();
        let mut routes = self.routes.write().unwrap_or_else(PoisonError::into_inner);
        if routes.contains_key(&name) {
            return Err(MountSourceError::AlreadyExists);
        }
        for observer in self.observers.live() {
            source.observe_view(Arc::downgrade(&observer));
        }
        routes.insert(name, Route { source, tag });
        self.bump_revision();
        Ok(())
    }

    /// Removes the route named `name`, if any. Open handles into it keep
    /// working detached; only new lookups stop finding it.
    ///
    /// Returns whether a route was actually removed.
    pub fn remove_route(&self, name: &[u8]) -> bool {
        let _view = self.view_gate.write();
        let removed = {
            let mut routes = self.routes.write().unwrap_or_else(PoisonError::into_inner);
            routes.remove(name)
        };
        if removed.is_some() {
            self.bump_revision();
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

    /// Number of routes currently projected by this source.
    #[must_use]
    pub fn route_count(&self) -> usize {
        self.routes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
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
        self.revision.fetch_add(1, Ordering::AcqRel);
        self.observers
            .notify(ViewStamp::record(&self.routes_changed));
    }

    fn coherent_epoch_by(
        &self,
        mut select: impl FnMut(&dyn MountFilesystem) -> Option<u64>,
    ) -> u64 {
        let routes = self.routes.read().unwrap_or_else(PoisonError::into_inner);
        let mut hasher = DefaultHasher::new();
        self.revision.load(Ordering::Acquire).hash(&mut hasher);
        for (name, route) in routes.iter() {
            name.hash(&mut hasher);
            select(route.source.as_ref()).hash(&mut hasher);
        }
        hasher.finish()
    }

    fn acquire_epoch_lease(
        &self,
        expected_epoch: Option<u64>,
        binding: bool,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        let mut lease = self.view_gate.read();
        let epoch = binding.then(|| self.binding_epoch()).flatten();
        if expected_epoch.is_some_and(|expected| Some(expected) != epoch) {
            return Err(MountSourceError::Stale);
        }
        let sources = self
            .routes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|route| Arc::clone(&route.source))
            .collect::<Vec<_>>();
        let mut children = Vec::with_capacity(sources.len());
        for source in sources {
            children.push(if binding {
                source.acquire_binding_lease(source.binding_epoch())?
            } else {
                source.acquire_view_lease()?
            });
        }
        let current = binding.then(|| self.binding_epoch()).flatten();
        if !self.view_is_stable() || current != epoch {
            return Err(MountSourceError::Stale);
        }
        lease._children = children;
        Ok(Box::new(lease))
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
            let Some((lookup, pin)) = source.lookup_pinned(&MountPath::root())? else {
                continue;
            };
            let remapped = self.remap_lookup(lookup, tag, &name);
            entries.push(MountDirectoryEntry {
                name,
                node: remapped.node,
                metadata: remapped.metadata,
                pin: pin.map(|pin| remap_content_pin(pin, tag)),
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

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
        self.inner.read_up_to(offset, maximum_bytes)
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
    fn supports_posix_named_attributes(&self) -> bool {
        let routes = self.routes.read().unwrap_or_else(PoisonError::into_inner);
        !routes.is_empty()
            && routes
                .values()
                .all(|route| route.source.supports_posix_named_attributes())
    }

    fn view_is_stable(&self) -> bool {
        self.routes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .all(|route| route.source.view_is_stable())
    }

    fn view_stamp(&self) -> Option<ViewStamp> {
        let routes = self.routes.read().unwrap_or_else(PoisonError::into_inner);
        routes
            .values()
            .try_fold(ViewStamp::of(&self.routes_changed), |latest, route| {
                route.source.view_stamp().map(|stamp| latest.max(stamp))
            })
    }

    fn observe_view(&self, observer: Weak<dyn ViewObserver>) {
        for route in self
            .routes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
        {
            route.source.observe_view(Weak::clone(&observer));
        }
        self.observers.add(observer);
    }

    fn fence_changes(&self) -> Result<(), MountSourceError> {
        let routes = self
            .routes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|route| Arc::clone(&route.source))
            .collect::<Vec<_>>();
        routes.iter().try_for_each(|source| source.fence_changes())
    }

    fn unchanged_since(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp) -> bool {
        stamp.precedes_none_of(&self.routes_changed)
            && match self.route(path) {
                Ok(None) => true,
                Ok(Some(routed)) => routed.source.unchanged_since(
                    &routed.sub_path,
                    file_id.map(|file_id| remap_file_id(file_id, routed.tag)),
                    stamp,
                ),
                Err(_) => false,
            }
    }

    fn folded_path(&self, path: &MountPath) -> Option<MountPath> {
        // Route names are exact; a route's own names fold as its source does.
        let folds = self
            .routes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .any(|route| route.source.folded_path(&MountPath::root()).is_some());
        folds.then(|| match self.route(path).ok().flatten() {
            Some(routed) => {
                let sub_path = routed
                    .source
                    .folded_path(&routed.sub_path)
                    .unwrap_or(routed.sub_path);
                sub_path
                    .components()
                    .iter()
                    .fold(MountPath::root().child(routed.name), |folded, component| {
                        folded.child(component.clone())
                    })
            }
            None => path.clone(),
        })
    }

    fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp) -> bool {
        let owner = self
            .file_id_index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&file_id)
            .cloned();
        stamp.precedes_none_of(&self.routes_changed)
            && owner.is_some_and(|name| {
                self.locate(&name).is_ok_and(|(source, tag)| {
                    source.node_unchanged_since(remap_file_id(file_id, tag), stamp)
                })
            })
    }

    fn binding_epoch(&self) -> Option<u64> {
        Some(self.coherent_epoch_by(MountFilesystem::binding_epoch))
    }

    fn acquire_view_lease(&self) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        self.acquire_epoch_lease(None, false)
    }

    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        self.acquire_epoch_lease(expected_epoch, true)
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Ok(Some(self.synthetic_root_lookup()));
        };
        let result = routed.source.lookup(&routed.sub_path)?;
        Ok(result.map(|lookup| self.remap_lookup(lookup, routed.tag, &routed.name)))
    }

    fn lookup_pinned(
        &self,
        path: &MountPath,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Ok(Some((self.synthetic_root_lookup(), None)));
        };
        let result = routed.source.lookup_pinned(&routed.sub_path)?;
        Ok(result.map(|(lookup, pin)| {
            (
                self.remap_lookup(lookup, routed.tag, &routed.name),
                pin.map(|pin| remap_content_pin(pin, routed.tag)),
            )
        }))
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

    fn read_pinned(
        &self,
        path: &MountPath,
        pin: MountContentPin,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut ContentSink<'_>,
    ) -> Result<(), MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Invalid(
                "the synthetic mount root has no content".to_owned(),
            ));
        };
        routed.source.read_pinned(
            &routed.sub_path,
            remap_content_pin(pin, routed.tag),
            offset,
            length,
            piece,
            sink,
        )
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
                entry.pin = entry.pin.map(|pin| remap_content_pin(pin, routed.tag));
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
        self.capture_host_paths(source_root, std::slice::from_ref(path))
    }

    fn capture_host_paths(
        &self,
        source_root: &Path,
        paths: &[MountPath],
    ) -> Result<(), MountSourceError> {
        let mut routes = CaptureRouteBatch::new();
        for path in paths {
            let routed = self.route(path)?.ok_or_else(|| {
                MountSourceError::Unsupported(
                    "the synthetic mount root cannot capture host state".to_owned(),
                )
            })?;
            routes
                .entry(routed.name)
                .or_insert_with(|| (Arc::clone(&routed.source), Vec::new()))
                .1
                .push(routed.sub_path);
        }
        for (name, (source, paths)) in routes {
            // The child source's path and host root are both route-relative.
            source.capture_host_paths(&source_root.join(host_route_name(&name)?), &paths)?;
        }
        Ok(())
    }

    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        let Some(routed) = self.route(path)? else {
            return Err(MountSourceError::Unsupported(
                "the synthetic mount root cannot capture host state".to_owned(),
            ));
        };
        let route_root = source_root.join(host_route_name(&routed.name)?);
        routed
            .source
            .capture_host_subtree(&route_root, &routed.sub_path)
    }
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
    use crate::{CancellationToken, CheckoutMountSource, Fs, SharedCheckout, WorkBudget};

    type MemoryCheckout = crate::facade::MemoryCheckout;

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
        assert_eq!(router.route_count(), 0);
        router.add_route(component("a"), memory_source()?)?;
        router.add_route(component("b"), memory_source()?)?;
        assert_eq!(router.route_count(), 2);
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
    fn view_lease_pins_routes_until_callback_completion() -> Result<(), Box<dyn std::error::Error>>
    {
        let router = Arc::new(RoutedMountSource::new());
        let route = component("a");
        router.add_route(route.clone(), memory_source()?)?;
        let stamp = router.view_stamp().ok_or("router has no view stamp")?;
        let lease = router.acquire_view_lease()?;
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let writer = Arc::clone(&router);
        let writer_route = route.clone();
        let thread = std::thread::spawn(move || {
            assert!(started_tx.send(()).is_ok(), "signal writer");
            let removed = writer.remove_route(&writer_route);
            assert!(finished_tx.send(removed).is_ok(), "signal completion");
        });
        started_rx.recv()?;
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(
            finished_rx.try_recv().is_err(),
            "route mutation escaped an active callback lease"
        );
        drop(lease);
        assert!(finished_rx.recv_timeout(std::time::Duration::from_secs(1))?);
        thread
            .join()
            .map_err(|_| std::io::Error::other("writer thread panicked"))?;
        assert!(!router.unchanged_since(&test_path("a"), None, stamp));
        Ok(())
    }

    #[test]
    fn routed_native_writes_invalidate_cache_without_rebinding_other_routes()
    -> Result<(), Box<dyn std::error::Error>> {
        let router = RoutedMountSource::new();
        let first = component("first");
        let second = component("second");
        router.add_route(first.clone(), memory_source()?)?;
        router.add_route(second.clone(), memory_source()?)?;
        let binding = router.binding_epoch();
        let created = test_path("first").child(component("file"));
        let untouched = test_path("second").child(component("file"));
        let stamp = router.view_stamp().ok_or("router has no view stamp")?;
        router.create_file(&created, metadata())?;
        assert_eq!(router.binding_epoch(), binding);
        assert!(!router.unchanged_since(&created, None, stamp));
        assert!(router.unchanged_since(&untouched, None, stamp));
        let _lease = router.acquire_binding_lease(binding)?;
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
    fn host_capture_is_rooted_inside_its_route() -> Result<(), Box<dyn std::error::Error>> {
        let router = RoutedMountSource::new();
        #[cfg(windows)]
        let profile = FilesystemProfile::Windows;
        #[cfg(not(windows))]
        let profile = FilesystemProfile::Portable;
        router.add_route(component("a"), memory_source_with_profile(profile)?)?;
        router.add_route(component("b"), memory_source_with_profile(profile)?)?;
        let host = tempfile::tempdir()?;
        std::fs::create_dir(host.path().join("a"))?;
        std::fs::create_dir(host.path().join("b"))?;
        std::fs::write(host.path().join("b").join("captured.bin"), b"inside-b")?;
        let path = test_path("b").child(component("captured.bin"));
        router.capture_host_path(host.path(), &path)?;
        assert_eq!(router.read_range(&path, 0, 8)?.as_ref(), b"inside-b");
        assert!(
            router
                .lookup(&test_path("a").child(component("captured.bin")))?
                .is_none()
        );
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
