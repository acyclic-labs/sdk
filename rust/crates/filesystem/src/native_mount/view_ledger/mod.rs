//! Exact invalidation of cached mount lookups.

use crate::FileId;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use std::cell::Cell;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use unicode_normalization::UnicodeNormalization as _;
use super::{adapter::NativeDemandCarrier, MountSourceError};
use storage::ChangeMap;
use notifications::ViewObservers;
mod storage;
mod notifications;
mod node_change;
pub(super) use notifications::PreparedEverythingNotification;

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

    /// Records one change in a slot, at a position after every stamp
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

    /// Attributes every change this thread records to `self` until the
    pub(super) fn enter(self) -> ViewOriginScope {
        ViewOriginScope(CURRENT_ORIGIN.replace(self))
    }

    /// The origin this thread's changes take now.
    pub(super) fn current() -> Self {
        CURRENT_ORIGIN.get()
    }
}

/// Restores the thread's previous origin when dropped.
pub(super) struct ViewOriginScope(ViewOrigin);

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
    fn allocation_custody(&self) -> Option<crate::demand::NativeMountAllocationCustody> { None }

    /// Changes to the source may not all have been reported by `position`,
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
    /// An effect enumerated in full: a prepared candidate replaced the
    Effect(&'a ViewEffect),
    /// An effect set that cannot be enumerated, such as a rebind.
    Everything,
    /// Changes to the source may not all have been reported yet, though
    Unconfirmed,
}

/// One enumerated effect on the view: installing a candidate prepared from
#[derive(Default)]
pub(super) struct ViewEffect {
    /// Nodes whose record changed.
    pub(super) nodes: Vec<FileId>,
    /// Names bound, unbound, or rebound within a directory at a known path.
    pub(super) names: Vec<(NamespacePath, LogicalName)>,
    /// Names rebound together with everything reached through them, as
    pub(super) subtrees: Vec<NamespacePath>,
    /// Directories whose listing changed while every name in them still
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
    bindings: ChangeMap,
    directories: ChangeMap,
    nodes: ChangeMap,
    observers: ViewObservers,
    _custody: Option<crate::demand::NativeMountAllocationCustody>,
}

impl ViewLedger {
    pub(super) fn new(carrier: &mut NativeDemandCarrier<'_>) -> Result<Arc<Self>, MountSourceError> {
        let custody = carrier.new_custody()?;
        carrier.with_work(|work| custody.claim(std::mem::size_of::<Self>() as u64, work))
            .map_err(super::lazy::mount_demand_error)?;
        Ok(Arc::new(Self { _custody: Some(custody), ..Self::default() }))
    }

    pub(super) fn stamp(&self) -> ViewStamp { ViewStamp::of(&self.latest) }

    pub(super) fn record(&self, change: &ViewChange<'_>, carrier: &mut NativeDemandCarrier<'_>)
        -> Result<(), MountSourceError>
    {
        let observers = self.observers.live(carrier)?;
        storage::charge(carrier, crate::WorkCounters { items_examined: observers.0.len() as u64, ..crate::WorkCounters::UNCHARGED })?;
        let position = ViewStamp::next();
        let recorded = (|| {
            match change {
                ViewChange::Node(id) => self.nodes.record(key_of(id), position, carrier)?,
                ViewChange::Bound(path) => self.rebound(path, position, carrier)?,
                ViewChange::Entry(path, id) => {
                    self.rebound(path, position, carrier)?;
                    self.nodes.record(key_of(id), position, carrier)?;
                }
                ViewChange::Moved { from, to, moved, replaced } => {
                    self.rebound(from, position, carrier)?;
                    self.rebound(to, position, carrier)?;
                    self.nodes.record(key_of(moved), position, carrier)?;
                    if let Some(id) = replaced { self.nodes.record(key_of(id), position, carrier)?; }
                }
                ViewChange::Effect(effect) => {
                    for id in &effect.nodes { self.nodes.record(key_of(id), position, carrier)?; }
                    for (directory, name) in &effect.names {
                        let directory = path_key(directory, carrier)?;
                        self.bindings.record(child_key(directory, name, carrier)?, position, carrier)?;
                        self.directories.record(directory, position, carrier)?;
                    }
                    for path in &effect.subtrees { self.rebound(path, position, carrier)?; }
                    for path in &effect.listings { self.directories.record(path_key(path, carrier)?, position, carrier)?; }
                    if effect.everything { position.record_in(&self.everything); }
                }
                ViewChange::Everything => position.record_in(&self.everything),
                ViewChange::Unconfirmed => position.record_in(&self.unconfirmed),
            }
            Ok::<_, MountSourceError>(())
        })();
        position.record_in(&self.latest);
        let origin = ViewOrigin::current();
        for observer in &observers.0 {
            if matches!(change, ViewChange::Unconfirmed) { observer.view_unconfirmed(position); }
            else { observer.view_changed(position, origin); }
        }
        recorded
    }

    fn rebound(&self, path: &NamespacePath, position: ViewStamp, carrier: &mut NativeDemandCarrier<'_>)
        -> Result<(), MountSourceError>
    {
        let (mut parent, mut terminal) = (None, key_of(&()));
        for name in path.components() {
            parent = Some(terminal);
            terminal = child_key(terminal, name, carrier)?;
        }
        self.bindings.record(terminal, position, carrier)?;
        if let Some(parent) = parent { self.directories.record(parent, position, carrier)?; }
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
        work.check().map_err(super::lazy::mount_demand_error)?;
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
        query(path, work, || {
        let mut terminal = None;
        stamp.precedes_none_of(&self.everything)
            && self.bindings.unchanged_since(stamp, |unchanged| {
                PathKeys::new(path).all(|key| {
                    terminal = Some(key);
                    unchanged(key)
                })
            })
            && terminal.is_none_or(|key| {
                self.directories
                    .unchanged_since(stamp, |unchanged| unchanged(key))
            })
            && file_id.is_none_or(|file_id| {
                self.nodes
                    .unchanged_since(stamp, |unchanged| unchanged(key_of(&file_id)))
            })
            })
    }

    /// Whether every binding in this path is unchanged since the stamp.
    pub(super) fn binding_unchanged_since(&self, path: &NamespacePath, stamp: ViewStamp, work: &mut crate::demand::DemandWork) -> Result<bool, MountSourceError> {
        query(path, work, || {
        stamp.precedes_none_of(&self.unconfirmed)
            && stamp.precedes_none_of(&self.everything)
            && self
                .bindings
                .unchanged_since(stamp, |unchanged| PathKeys::new(path).all(unchanged))
            })
    }

    /// Whether this directory listing is unchanged since the stamp.
    pub(super) fn listing_unchanged_since(&self, path: &NamespacePath, stamp: ViewStamp, work: &mut crate::demand::DemandWork) -> Result<bool, MountSourceError> {
        query(path, work, || {
        let Some(directory) = PathKeys::new(path).last() else {
            return false;
        };
        stamp.precedes_none_of(&self.unconfirmed)
            && stamp.precedes_none_of(&self.everything)
            && self
                .directories
                .unchanged_since(stamp, |unchanged| unchanged(directory))
            })
    }

    /// Whether this node is unchanged since the stamp.
    pub(super) fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp, work: &mut crate::demand::DemandWork) -> Result<bool, MountSourceError> {
        work.charge(crate::WorkCounters { items_examined: 3, ..crate::WorkCounters::UNCHARGED }).map_err(super::lazy::mount_demand_error)?;
        Ok(stamp.precedes_none_of(&self.unconfirmed)
            && stamp.precedes_none_of(&self.everything)
            && self
                .nodes
                .unchanged_since(stamp, |unchanged| unchanged(key_of(&file_id))))
    }

}

/// Keys of a path's root and every successive prefix, including the path.
struct PathKeys<'a> {
    components: std::slice::Iter<'a, crate::kernel::LogicalName>,
    key: Option<u64>,
}

impl<'a> PathKeys<'a> {
    fn new(path: &'a NamespacePath) -> Self {
        Self {
            components: path.components().iter(),
            key: None,
        }
    }
}

impl Iterator for PathKeys<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        let next = match self.key {
            None => key_of(&()),
            Some(parent) => hash_child(parent, self.components.next()?),
        };
        self.key = Some(next);
        Some(next)
    }
}

fn path_key(path: &NamespacePath, carrier: &mut NativeDemandCarrier<'_>) -> Result<u64, MountSourceError> {
    carrier.with_work(|work| query(path, work, || PathKeys::new(path).last().unwrap_or_else(|| key_of(&()))))
}

/// Keys `name` beneath `parent` by its spelling fold: the name's Unicode
fn hash_child(parent: u64, name: &LogicalName) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write_u64(parent);
    hash_spelling_fold(name, &mut hasher);
    hasher.finish()
}

fn hash_spelling_fold(name: &LogicalName, hasher: &mut DefaultHasher) {
    /// Separates folded text from raw bytes that happen to equal it.
    const TEXT: u8 = 1;
    const RAW: u8 = 2;
    let bytes = name.as_bytes();
    let text = match name.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            if bytes.is_ascii() {
                hasher.write_u8(TEXT);
                hash_ascii_fold(bytes.iter().copied(), hasher);
                return;
            }
            std::str::from_utf8(bytes)
                .ok()
                .map(std::borrow::Cow::Borrowed)
        }
        NameEncoding::WindowsUtf16Le => match crate::kernel::types::utf16le_units(bytes) {
            Some(units) if units.clone().all(|unit| unit < 0x80) => {
                hasher.write_u8(TEXT);
                hash_ascii_fold(units.map(|unit| unit.to_le_bytes()[0]), hasher);
                return;
            }
            Some(units) => char::decode_utf16(units)
                .collect::<Result<String, _>>()
                .ok()
                .map(std::borrow::Cow::Owned),
            None => None,
        },
    };
    let Some(text) = text else {
        hasher.write_u8(RAW);
        hasher.write(bytes);
        return;
    };
    hasher.write_u8(TEXT);
    let mut encoded = [0_u8; 4];
    for folded in text
        .nfc()
        .flat_map(char::to_uppercase)
        .flat_map(char::to_lowercase)
    {
        hasher.write(folded.encode_utf8(&mut encoded).as_bytes());
    }
}

/// Hashes ASCII text lowercased, exactly as the Unicode fold hashes it.
fn hash_ascii_fold(bytes: impl Iterator<Item = u8>, hasher: &mut DefaultHasher) {
    let mut buffer = [0_u8; 64];
    let mut filled = 0;
    for byte in bytes {
        if let Some(slot) = buffer.get_mut(filled) {
            *slot = byte.to_ascii_lowercase();
            filled += 1;
        }
        if filled == buffer.len() {
            hasher.write(&buffer);
            filled = 0;
        }
    }
    hasher.write(buffer.get(..filled).unwrap_or_default());
}

fn key_of(value: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}


fn child_key(parent: u64, name: &LogicalName, carrier: &mut NativeDemandCarrier<'_>) -> Result<u64, MountSourceError> {
    carrier.with_work(|work| admitted_names(std::slice::from_ref(name), work, || hash_child(parent, name)))
}

fn query<T>(path: &NamespacePath, work: &mut crate::demand::DemandWork, read: impl FnOnce() -> T)
    -> Result<T, MountSourceError>
{
    admitted_names(path.components(), work, read)
}

fn admitted_names<T>(names: &[LogicalName], work: &mut crate::demand::DemandWork, read: impl FnOnce() -> T)
    -> Result<T, MountSourceError>
{
    let mut scratch = 0_u64;
    let mut items = 3_u64;
    for name in names {
        items = items.checked_add((name.as_bytes().len() as u64).checked_mul(4).and_then(|n| n.checked_add(1)).ok_or(crate::WorkError::Overflow)?).ok_or(crate::WorkError::Overflow)?;
        let unicode_scratch = match name.encoding() {
            NameEncoding::WindowsUtf16Le => crate::kernel::types::utf16le_units(name.as_bytes())
                .is_some_and(|units| units.clone().any(|unit| unit >= 0x80)
                    && char::decode_utf16(units).all(|value| value.is_ok())),
            NameEncoding::Utf8 | NameEncoding::PosixBytes => !name.as_bytes().is_ascii()
                && std::str::from_utf8(name.as_bytes()).is_ok(),
        };
        if unicode_scratch {
            scratch = scratch.max((name.as_bytes().len() as u64).checked_mul(32).ok_or(crate::WorkError::Overflow)?);
        }
    }
    work.charge(crate::WorkCounters { items_examined: items, ..crate::WorkCounters::UNCHARGED })
        .map_err(super::lazy::mount_demand_error)?;
    if scratch != 0 { work.allocate(scratch).map_err(super::lazy::mount_demand_error)?; }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(read));
    let released = work.release(scratch).map_err(super::lazy::mount_demand_error);
    match result {
        Ok(value) => released.map(|()| value),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[cfg(test)]
mod tests;
