//! Exact invalidation of cached mount lookups.

use super::{MountSourceError, adapter::NativeDemandCarrier};
use crate::FileId;
use crate::demand::NativeMountAllocationCustody;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use crate::lock_order::{Mutex as RankedMutex, OBSERVERS};
use crate::sync::RwLock;
use notifications::ObserverEntries;
use std::cell::Cell;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, Weak};
use storage::ChangePositions;
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
pub(super) use storage::same_work;
pub(super) use storage::{copy_work, scan_work};
use unicode_normalization::UnicodeNormalization as _;
use writeable::Writeable as _;
mod notifications;
mod storage;

static VIEW_CHANGES: AtomicU64 = AtomicU64::new(0);
// Only the Linux and macOS drivers attribute their changes.
#[cfg(any(target_os = "linux", target_os = "macos", test))]
static NEXT_ORIGIN: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CURRENT_ORIGIN: Cell<ViewOrigin> = const { Cell::new(ViewOrigin::UNATTRIBUTED) };
}

/// Distinct keys one change map tracks before it folds them into its floor.
const MAXIMUM_TRACKED_KEYS: usize = 1 << 16;

/// One position in the process-wide order of mount view changes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ViewStamp(u64);

impl ViewStamp {
    /// Precedes every change: facts of unknown age are at least this old.
    #[cfg(target_os = "linux")]
    pub(super) const ORIGIN: Self = Self(0);

    /// A stamp at or after every change recorded anywhere so far.
    pub(super) fn current() -> Self {
        Self(VIEW_CHANGES.load(Ordering::Acquire))
    }

    /// The latest change a slot recorded.
    pub(super) fn of(slot: &AtomicU64) -> Self {
        Self(slot.load(Ordering::Acquire))
    }

    /// Records a change after every previously sampled stamp.
    #[cfg(all(test, target_os = "linux"))]
    pub(super) fn record(slot: &AtomicU64) -> Self {
        let position = Self::next();
        position.record_in(slot);
        position
    }

    /// Whether a slot recorded no change after this stamp was sampled.
    pub(super) fn precedes_none_of(self, slot: &AtomicU64) -> bool {
        slot.load(Ordering::Acquire) <= self.0
    }

    fn next() -> Self {
        Self(VIEW_CHANGES.fetch_add(1, Ordering::AcqRel) + 1)
    }

    fn record_in(self, slot: &AtomicU64) {
        slot.fetch_max(self.0, Ordering::AcqRel);
    }
}

/// Who made a view change: one mount session's callbacks, or anyone else.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ViewOrigin(u64);

impl ViewOrigin {
    /// Changes made outside every driver callback.
    const UNATTRIBUTED: Self = Self(0);

    /// A fresh origin, distinct from every other.
    #[cfg(any(target_os = "linux", target_os = "macos", test))]
    pub(super) fn new() -> Self {
        Self(NEXT_ORIGIN.fetch_add(1, Ordering::Relaxed))
    }

    /// Attributes changes to this origin for the returned scope.
    #[cfg(any(target_os = "linux", target_os = "macos", test))]
    pub(super) fn enter(self) -> ViewOriginScope {
        ViewOriginScope(CURRENT_ORIGIN.replace(self))
    }

    /// The origin this thread's changes take now.
    pub(super) fn current() -> Self {
        CURRENT_ORIGIN.get()
    }
}

/// Restores the thread's previous origin when dropped.
#[cfg(any(target_os = "linux", target_os = "macos", test))]
pub(super) struct ViewOriginScope(ViewOrigin);

#[cfg(any(target_os = "linux", target_os = "macos", test))]
impl Drop for ViewOriginScope {
    fn drop(&mut self) {
        CURRENT_ORIGIN.set(self.0);
    }
}

/// Learns of every change a source records.
pub trait ViewObserver: Send + Sync {
    /// One change was recorded at `position` by `origin`.
    fn view_changed(&self, position: ViewStamp, origin: ViewOrigin);

    /// Known native allocation ownership for an SDK observer.
    fn allocation_custody(&self) -> Option<Arc<NativeMountAllocationCustody>> {
        None
    }

    /// The fence could not confirm the source view at `position`.
    fn view_unconfirmed(&self, position: ViewStamp) {
        self.view_changed(position, ViewOrigin::current());
    }
}

/// What one checkout mutation changed in the mounted view.
pub(super) enum ViewChange<'a> {
    /// One node's content, metadata, or attributes.
    Node(FileId),
    /// A name now resolves where nothing did.
    Bound(&'a NamespacePath),
    /// A name binding and its node changed.
    Entry(&'a NamespacePath, FileId),
    /// A node moved between names, possibly replacing another node.
    Moved {
        from: &'a NamespacePath,
        to: &'a NamespacePath,
        moved: FileId,
        replaced: Option<FileId>,
    },
    /// The enumerated effects of installing a prepared candidate.
    Effect(&'a ViewEffect),
    /// An effect set that cannot be enumerated, such as a rebind.
    Everything,
    /// The fence could not confirm every source change.
    Unconfirmed,
}

/// Enumerated changes from installing a prepared candidate.
#[derive(Default)]
pub(super) struct ViewEffect {
    /// Nodes whose record changed.
    pub(super) nodes: Vec<FileId>,
    /// Names bound, unbound, or rebound within a directory at a known path.
    pub(super) names: Vec<(NamespacePath, LogicalName)>,
    /// Rebound names and the subtrees reached through them.
    pub(super) subtrees: Vec<NamespacePath>,
    /// Changed listings with unchanged name bindings.
    pub(super) listings: Vec<NamespacePath>,
    /// Whether the effect could not be enumerated at all.
    pub(super) everything: bool,
}

impl ViewEffect {
    /// Whether the effect changes nothing.
    pub(super) fn is_empty(&self) -> bool {
        self.nodes.is_empty()
            && self.names.is_empty()
            && self.subtrees.is_empty()
            && self.listings.is_empty()
            && !self.everything
    }
}

#[derive(Default)]
pub(crate) struct ViewLedger {
    latest: AtomicU64,
    everything: AtomicU64,
    unconfirmed: AtomicU64,
    bindings: RwLock<ChangePositions>,
    directories: RwLock<ChangePositions>,
    nodes: RwLock<ChangePositions>,
    observers: RankedMutex<Option<Arc<ObserverEntries>>, OBSERVERS>,
    _custody: Option<Arc<NativeMountAllocationCustody>>,
}

impl ViewLedger {
    pub(super) fn new(
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Arc<Self>, MountSourceError> {
        let custody = carrier.new_custody()?;
        carrier
            .with_work(|work| custody.claim(std::mem::size_of::<Self>() as u64, work))
            .map_err(MountSourceError::from)?;
        Ok(Arc::new(Self {
            _custody: Some(custody),
            ..Self::default()
        }))
    }

    pub(super) fn stamp(&self) -> ViewStamp {
        ViewStamp::of(&self.latest)
    }

    pub(super) fn record(
        &self,
        change: &ViewChange<'_>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        let (recorded, notify) = self.record_deferred(change, carrier)?;
        notify();
        recorded
    }

    pub(super) fn record_deferred(
        &self,
        change: &ViewChange<'_>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(Result<(), MountSourceError>, impl FnOnce() + Send + use<>), MountSourceError>
    {
        let position = ViewStamp::next();
        let observers = self
            .observers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        storage::charge(
            carrier,
            scan_work(
                observers
                    .as_ref()
                    .map_or(0, |entries| entries.entries.len()) as u64,
            ),
        )
        .inspect_err(|_| {
            position.record_in(&self.unconfirmed);
            position.record_in(&self.latest);
        })?;
        let recorded = (|| {
            match change {
                ViewChange::Node(id) => {
                    storage::record(&self.nodes, key_of(id), position, carrier)?;
                }
                ViewChange::Bound(path) => self.rebound(path, position, carrier)?,
                ViewChange::Entry(path, id) => {
                    self.rebound(path, position, carrier)?;
                    storage::record(&self.nodes, key_of(id), position, carrier)?;
                }
                ViewChange::Moved {
                    from,
                    to,
                    moved,
                    replaced,
                } => {
                    self.rebound(from, position, carrier)?;
                    self.rebound(to, position, carrier)?;
                    storage::record(&self.nodes, key_of(moved), position, carrier)?;
                    if let Some(id) = replaced {
                        storage::record(&self.nodes, key_of(id), position, carrier)?;
                    }
                }
                ViewChange::Effect(effect) => {
                    for id in &effect.nodes {
                        storage::record(&self.nodes, key_of(id), position, carrier)?;
                    }
                    for (directory, name) in &effect.names {
                        let directory = path_key(directory, carrier)?;
                        storage::record(
                            &self.bindings,
                            child_key(directory, name, carrier)?,
                            position,
                            carrier,
                        )?;
                        storage::record(&self.directories, directory, position, carrier)?;
                    }
                    for path in &effect.subtrees {
                        self.rebound(path, position, carrier)?;
                    }
                    for path in &effect.listings {
                        storage::record(
                            &self.directories,
                            path_key(path, carrier)?,
                            position,
                            carrier,
                        )?;
                    }
                    if effect.everything {
                        position.record_in(&self.everything);
                    }
                }
                ViewChange::Everything => position.record_in(&self.everything),
                ViewChange::Unconfirmed => position.record_in(&self.unconfirmed),
            }
            Ok::<_, MountSourceError>(())
        })();
        if recorded.is_err() {
            position.record_in(&self.unconfirmed);
        }
        position.record_in(&self.latest);
        let unconfirmed = recorded.is_err() || matches!(change, ViewChange::Unconfirmed);
        let origin = ViewOrigin::current();
        let notify = move || {
            for observer in observers
                .iter()
                .flat_map(|entries| &entries.entries)
                .filter_map(|entry| entry.observer.upgrade())
            {
                if unconfirmed {
                    observer.view_unconfirmed(position);
                } else {
                    observer.view_changed(position, origin);
                }
            }
        };
        Ok((recorded, notify))
    }

    fn rebound(
        &self,
        path: &NamespacePath,
        position: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        let (mut parent, mut terminal) = (None, key_of(&()));
        for name in path.components() {
            parent = Some(terminal);
            terminal = child_key(terminal, name, carrier)?;
        }
        storage::record(&self.bindings, terminal, position, carrier)?;
        if let Some(parent) = parent {
            storage::record(&self.directories, parent, position, carrier)?;
        }
        Ok(())
    }
    /// Whether no reported or unconfirmed change invalidated this lookup.
    pub(super) fn unchanged_since(
        &self,
        path: &NamespacePath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        work.check().map_err(MountSourceError::from)?;
        Ok(stamp.precedes_none_of(&self.unconfirmed)
            && self.reported_unchanged_since(path, file_id, stamp, work)?)
    }

    /// Whether no reported change invalidated this lookup.
    pub(super) fn reported_unchanged_since(
        &self,
        path: &NamespacePath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        admitted_names(path.components(), work, || {
            let mut terminal = key_of(&());
            stamp.precedes_none_of(&self.everything)
                && storage::unchanged_since(
                    &self.bindings,
                    stamp,
                    path_keys(path).inspect(|key| terminal = *key),
                )
                && storage::unchanged_since(&self.directories, stamp, [terminal])
                && file_id.is_none_or(|file_id| {
                    storage::unchanged_since(&self.nodes, stamp, [key_of(&file_id)])
                })
        })
    }

    /// Whether this directory listing is unchanged since the stamp.
    pub(super) fn listing_unchanged_since(
        &self,
        path: &NamespacePath,
        stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        admitted_names(path.components(), work, || {
            let directory = path.components().iter().fold(key_of(&()), hash_child);
            stamp.precedes_none_of(&self.unconfirmed)
                && stamp.precedes_none_of(&self.everything)
                && storage::unchanged_since(&self.directories, stamp, [directory])
        })
    }

    /// Whether this node is unchanged since the stamp.
    pub(super) fn node_unchanged_since(
        &self,
        file_id: FileId,
        stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        work.charge(scan_work(3)).map_err(MountSourceError::from)?;
        Ok(stamp.precedes_none_of(&self.unconfirmed)
            && stamp.precedes_none_of(&self.everything)
            && storage::unchanged_since(&self.nodes, stamp, [key_of(&file_id)]))
    }
}

fn path_keys(path: &NamespacePath) -> impl Iterator<Item = u64> + '_ {
    let root = key_of(&());
    std::iter::once(root).chain(path.components().iter().scan(root, |parent, name| {
        *parent = hash_child(*parent, name);
        Some(*parent)
    }))
}

fn path_key(
    path: &NamespacePath,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<u64, MountSourceError> {
    carrier.with_work(|work| {
        admitted_names(path.components(), work, || {
            path.components().iter().fold(key_of(&()), hash_child)
        })
    })
}

/// Keys a child by its normalized and folded spelling.
fn hash_child(parent: u64, name: &LogicalName) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write_u64(parent);
    hash_spelling_fold(name, &mut hasher);
    hasher.finish()
}

fn hash_spelling_fold(name: &LogicalName, hasher: &mut DefaultHasher) {
    let bytes = name.as_bytes();
    match name.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            if let Ok(text) = std::str::from_utf8(bytes) {
                hash_text(text.chars(), hasher);
                return;
            }
        }
        NameEncoding::WindowsUtf16Le => {
            if let Some(units) = crate::kernel::types::utf16le_units(bytes)
                && char::decode_utf16(units.clone()).all(|value| value.is_ok())
            {
                hash_text(char::decode_utf16(units).flatten(), hasher);
                return;
            }
        }
    }
    // Keep malformed names separate from text with the same bytes.
    hasher.write_u8(2);
    hasher.write(bytes);
}

fn hash_text(text: impl Iterator<Item = char>, hasher: &mut DefaultHasher) {
    hasher.write_u8(1);
    let mapper = icu_casemap::CaseMapper::new();
    let mut encoded = [0_u8; 4];
    for scalar in text.nfc() {
        // Hash writes cannot fail; stream the full fold without a temporary key.
        let _ = mapper
            .fold(scalar.encode_utf8(&mut encoded))
            .write_to(&mut HashWriter(hasher));
    }
}

struct HashWriter<'a>(&'a mut DefaultHasher);

impl std::fmt::Write for HashWriter<'_> {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.0.write(text.as_bytes());
        Ok(())
    }
}

fn key_of(value: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn child_key(
    parent: u64,
    name: &LogicalName,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<u64, MountSourceError> {
    carrier.with_work(|work| {
        admitted_names(std::slice::from_ref(name), work, || {
            hash_child(parent, name)
        })
    })
}

fn admitted_names<T>(
    names: &[LogicalName],
    work: &mut crate::demand::DemandWork,
    read: impl FnOnce() -> T,
) -> Result<T, MountSourceError> {
    let mut scratch = 0_u64;
    let mut items = 3_u64;
    for name in names {
        items += 4 * name.as_bytes().len() as u64 + 1;
        let unicode_scratch = match name.encoding() {
            NameEncoding::WindowsUtf16Le => crate::kernel::types::utf16le_units(name.as_bytes())
                .is_some_and(|units| {
                    units.clone().any(|unit| unit >= 0x80)
                        && char::decode_utf16(units).all(|value| value.is_ok())
                }),
            NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                !name.as_bytes().is_ascii() && std::str::from_utf8(name.as_bytes()).is_ok()
            }
        };
        if unicode_scratch {
            scratch = scratch.max(32 * name.as_bytes().len() as u64);
        }
    }
    work.charge(scan_work(items))
        .map_err(MountSourceError::from)?;
    if scratch != 0 {
        work.claim_allocation(
            scratch,
            u64::from((scratch) != 0),
            crate::WorkCounters::UNCHARGED,
        )
        .map_err(MountSourceError::from)?;
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(read));
    let released = work.release(scratch).map_err(MountSourceError::from);
    match result {
        Ok(value) => released.map(|()| value),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[cfg(test)]
mod tests;
