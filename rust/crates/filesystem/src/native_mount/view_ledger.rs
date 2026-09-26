//! Exact invalidation of cached mount lookups.
//!
//! Every view change takes the next position in one process-wide order and
//! records it against exactly what it changed: a name binding, a directory's
//! listing and attributes, or one node. A driver samples a stamp before a
//! lookup and may reuse the result only while nothing that lookup depended on
//! has recorded a later position. Positions only grow and are never reused,
//! so a stale cache entry cannot validate again.
//!
//! Observers learn of every recorded position together with the
//! [`ViewOrigin`] that made the change, so a driver can tell changes it made
//! itself (which its kernel already applied) from changes made around it.
//!
//! Names are keyed by every spelling a host resolves to them: a lookup may
//! spell a name differently from the change that later invalidates it, as
//! when a case-insensitive source reports its own spelling of a name a
//! caller looked up in another case, or another Unicode normalization.

use crate::FileId;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use std::cell::Cell;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use unicode_normalization::UnicodeNormalization as _;

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
///
/// A stamp is at or after every change its source had recorded when it was
/// sampled, and before every later one. Only a source can sample a stamp,
/// and only that source can interpret it, through
/// [`super::MountFilesystem::unchanged_since`].
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
    /// sampled before it, and returns that position.
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
///
/// Changes recorded on a thread take the origin that thread entered, so a
/// driver that enters its own origin around every callback can tell its own
/// changes, which its kernel applied as it made them, from all others.
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
    /// returned scope drops.
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
///
/// Called while the recorder still holds the exclusive view it changed, so
/// an observer must only note the position and return: anything that waits
/// on the view, or on work that does, deadlocks.
pub trait ViewObserver: Send + Sync {
    /// One change was recorded at `position` by `origin`.
    fn view_changed(&self, position: ViewStamp, origin: ViewOrigin);

    /// Changes to the source may not all have been reported by `position`,
    /// though none is known to have been lost ([`ViewChange::Unconfirmed`]).
    /// An observer that cannot verify what it keeps treats it as a change.
    fn view_unconfirmed(&self, position: ViewStamp) {
        self.view_changed(position, ViewOrigin::current());
    }
}

/// The observers of one source of view changes.
#[derive(Default)]
pub(super) struct ViewObservers(Mutex<Vec<Weak<dyn ViewObserver>>>);

impl ViewObservers {
    pub(super) fn add(&self, observer: Weak<dyn ViewObserver>) {
        let mut observers = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        observers.retain(|observer| observer.strong_count() != 0);
        observers.push(observer);
    }

    /// Every observer still alive.
    pub(super) fn live(&self) -> Vec<Arc<dyn ViewObserver>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter_map(Weak::upgrade)
            .collect()
    }

    /// Tells every observer of one change this thread just recorded.
    pub(super) fn notify(&self, position: ViewStamp) {
        let origin = ViewOrigin::current();
        for observer in self.live() {
            observer.view_changed(position, origin);
        }
    }

    /// Tells every observer that changes may be unreported by `position`.
    pub(super) fn notify_unconfirmed(&self, position: ViewStamp) {
        for observer in self.live() {
            observer.view_unconfirmed(position);
        }
    }
}

/// What one checkout mutation changed in the mounted view.
pub(super) enum ViewChange<'a> {
    /// One node's content, metadata, or attributes.
    Node(FileId),
    /// A name now resolves where nothing did.
    Bound(&'a NamespacePath),
    /// A name no longer resolves to `FileId`, which may keep other names.
    Unbound(&'a NamespacePath, FileId),
    /// A name now resolves to an existing node under a second name.
    Linked(&'a NamespacePath, FileId),
    /// A node moved between names, possibly replacing another node.
    Moved {
        from: &'a NamespacePath,
        to: &'a NamespacePath,
        moved: FileId,
        replaced: Option<FileId>,
    },
    /// An effect enumerated in full: a prepared candidate replaced the
    /// checkout, or the source reported changes made to it outside the view.
    Effect(&'a ViewEffect),
    /// An effect set that cannot be enumerated, such as a rebind.
    Everything,
    /// Changes to the source may not all have been reported yet, though
    /// none is known to have been lost. Every fact read before is stale to
    /// [`ViewLedger::unchanged_since`], so it is read again, and kept where
    /// what is read again matches it: nothing reported changed it
    /// ([`ViewLedger::reported_unchanged_since`]).
    Unconfirmed,
}

/// One enumerated effect on the view: installing a candidate prepared from
/// a copy of the checkout, as the generation diff between the two reports
/// it, or changes the source reported having happened to it.
#[derive(Default)]
pub(super) struct ViewEffect {
    /// Nodes whose record changed.
    pub(super) nodes: Vec<FileId>,
    /// Names bound, unbound, or rebound within a directory at a known path.
    pub(super) names: Vec<(NamespacePath, LogicalName)>,
    /// Names rebound together with everything reached through them, as
    /// when names changed in directories at unknown paths beneath one, or
    /// the source reported a change to the entry a name binds.
    pub(super) subtrees: Vec<NamespacePath>,
    /// Directories whose listing changed while every name in them still
    /// resolves as it did, as when promotion moves a name's answer from the
    /// source into the checkout.
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

/// Change positions of one checkout's view.
///
/// Keys are 64-bit hashes, so distinct keys share a position only through a
/// hash collision; a shared position can only invalidate more than
/// necessary, never less.
pub(super) struct ViewLedger {
    latest: AtomicU64,
    everything: AtomicU64,
    unconfirmed: AtomicU64,
    bindings: ChangeMap,
    directories: ChangeMap,
    nodes: ChangeMap,
    observers: ViewObservers,
}

impl ViewLedger {
    pub(super) fn new() -> Self {
        Self {
            latest: AtomicU64::new(0),
            everything: AtomicU64::new(0),
            unconfirmed: AtomicU64::new(0),
            bindings: ChangeMap::default(),
            directories: ChangeMap::default(),
            nodes: ChangeMap::default(),
            observers: ViewObservers::default(),
        }
    }

    /// Tells `observer` of every change recorded from now on.
    pub(super) fn observe(&self, observer: Weak<dyn ViewObserver>) {
        self.observers.add(observer);
    }

    /// A stamp at or after every change this ledger has recorded.
    pub(super) fn stamp(&self) -> ViewStamp {
        ViewStamp::of(&self.latest)
    }

    /// Records one change at one fresh position. Callers hold the exclusive
    /// view of what changed, so no lookup overlaps the change it records;
    /// a change the source reported was made before it is recorded, so a
    /// lookup that overlaps it read the source either before it, and is
    /// invalidated here, or after it, and is already current.
    pub(super) fn record(&self, change: &ViewChange<'_>) {
        let position = ViewStamp::next();
        match change {
            ViewChange::Node(file_id) => self.node_changed(*file_id, position),
            ViewChange::Bound(path) => self.rebound(path, position),
            ViewChange::Unbound(path, file_id) | ViewChange::Linked(path, file_id) => {
                self.rebound(path, position);
                self.node_changed(*file_id, position);
            }
            ViewChange::Moved {
                from,
                to,
                moved,
                replaced,
            } => {
                self.rebound(from, position);
                self.rebound(to, position);
                self.node_changed(*moved, position);
                if let Some(replaced) = replaced {
                    self.node_changed(*replaced, position);
                }
            }
            ViewChange::Effect(effect) => {
                for file_id in &effect.nodes {
                    self.node_changed(*file_id, position);
                }
                for (directory, name) in &effect.names {
                    let directory = path_key(directory);
                    self.bindings.record(child_key(directory, name), position);
                    self.directories.record(directory, position);
                }
                for subtree in &effect.subtrees {
                    self.rebound(subtree, position);
                }
                for directory in &effect.listings {
                    self.directories.record(path_key(directory), position);
                }
                if effect.everything {
                    position.record_in(&self.everything);
                }
            }
            ViewChange::Everything => position.record_in(&self.everything),
            ViewChange::Unconfirmed => {
                position.record_in(&self.unconfirmed);
                position.record_in(&self.latest);
                self.observers.notify_unconfirmed(position);
                return;
            }
        }
        position.record_in(&self.latest);
        self.observers.notify(position);
    }

    /// Whether a lookup of `path` that resolved to `file_id` (or to nothing)
    /// after `stamp` still describes the view: no component of `path` was
    /// rebound, `path`'s own listing and attributes are unchanged, and the
    /// node itself is unchanged.
    pub(super) fn unchanged_since(
        &self,
        path: &NamespacePath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        stamp.precedes_none_of(&self.unconfirmed)
            && self.reported_unchanged_since(path, file_id, stamp)
    }

    /// [`Self::unchanged_since`], counting only reported changes: facts read
    /// after `stamp` and read again since, with the same result, still
    /// describe the view.
    pub(super) fn reported_unchanged_since(
        &self,
        path: &NamespacePath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
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
    }

    /// Whether facts read about the node `file_id` after `stamp` still
    /// describe it: nothing changed the node itself since. A directory's
    /// listing is keyed by its path, so only [`Self::unchanged_since`] covers
    /// facts that follow from a listing.
    pub(super) fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp) -> bool {
        stamp.precedes_none_of(&self.unconfirmed)
            && stamp.precedes_none_of(&self.everything)
            && self
                .nodes
                .unchanged_since(stamp, |unchanged| unchanged(key_of(&file_id)))
    }

    /// A name's binding changed: every lookup through it, and the listing
    /// and attributes of the directory holding it, are stale.
    fn rebound(&self, path: &NamespacePath, position: ViewStamp) {
        let mut parent = None;
        let mut name = None;
        for key in PathKeys::new(path) {
            parent = name.replace(key);
        }
        if let Some(name) = name {
            self.bindings.record(name, position);
        }
        if let Some(parent) = parent {
            self.directories.record(parent, position);
        }
    }

    fn node_changed(&self, file_id: FileId, position: ViewStamp) {
        self.nodes.record(key_of(&file_id), position);
    }
}

/// The latest change recorded against each key. A key that was never
/// recorded, or whose record was folded away, reads as the floor.
#[derive(Default)]
struct ChangeMap(RwLock<ChangePositions>);

#[derive(Default)]
struct ChangePositions {
    floor: u64,
    latest: HashMap<u64, u64>,
}

impl ChangeMap {
    fn record(&self, key: u64, position: ViewStamp) {
        let mut positions = self.0.write().unwrap_or_else(PoisonError::into_inner);
        if positions.latest.len() >= MAXIMUM_TRACKED_KEYS && !positions.latest.contains_key(&key) {
            // Every folded key reads as the floor, which is no earlier than
            // anything it recorded: folding only invalidates more.
            let positions = &mut *positions;
            positions.floor = positions
                .latest
                .drain()
                .fold(positions.floor, |floor, (_, at)| floor.max(at));
        }
        let latest = positions.latest.entry(key).or_insert(0);
        *latest = (*latest).max(position.0);
    }

    /// Runs `check` with a test of whether one key recorded nothing after
    /// `stamp`, under one read of the map.
    fn unchanged_since(
        &self,
        stamp: ViewStamp,
        check: impl FnOnce(&dyn Fn(u64) -> bool) -> bool,
    ) -> bool {
        let positions = self.0.read().unwrap_or_else(PoisonError::into_inner);
        check(&|key| {
            positions
                .latest
                .get(&key)
                .map_or(positions.floor, |at| (*at).max(positions.floor))
                <= stamp.0
        })
    }
}

/// Keys of a path's root and every successive prefix, including the path.
/// Names are keyed by their case-fold, so every spelling a folding volume
/// resolves to one entry shares that entry's key.
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
            Some(parent) => child_key(parent, self.components.next()?),
        };
        self.key = Some(next);
        Some(next)
    }
}

fn path_key(path: &NamespacePath) -> u64 {
    PathKeys::new(path).last().unwrap_or_else(|| key_of(&()))
}

/// Keys `name` beneath `parent` by its spelling fold: the name's Unicode
/// text, canonically composed and case-folded, whatever its encoding, or
/// its bytes when it is not Unicode. Every spelling a case- or
/// normalization-insensitive host resolves to one entry shares the key, and
/// distinct names that share it only share positions, which can only
/// invalidate more.
fn child_key(parent: u64, name: &LogicalName) -> u64 {
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
        NameEncoding::WindowsUtf16Le => {
            let units = || {
                bytes
                    .chunks_exact(2)
                    .map(|pair| <[u8; 2]>::try_from(pair).map_or(0, u16::from_le_bytes))
            };
            if units().all(|unit| unit < 0x80) {
                hasher.write_u8(TEXT);
                hash_ascii_fold(units().map(|unit| unit.to_le_bytes()[0]), hasher);
                return;
            }
            char::decode_utf16(units())
                .collect::<Result<String, _>>()
                .ok()
                .map(std::borrow::Cow::Owned)
        }
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

#[cfg(test)]
mod tests {
    use super::{MAXIMUM_TRACKED_KEYS, ViewChange, ViewLedger, ViewOrigin, ViewStamp};
    use crate::FileId;
    use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
    use crate::model::VolumeLimits;
    use std::sync::{Arc, Mutex};

    fn path(names: &[&str]) -> Result<NamespacePath, Box<dyn std::error::Error>> {
        let limits = VolumeLimits::default();
        let components = names
            .iter()
            .map(|name| {
                LogicalName::new(
                    NameEncoding::Utf8,
                    name.as_bytes().to_vec(),
                    limits.maximum_component_bytes,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(NamespacePath::new(components, limits)?)
    }

    #[test]
    fn unrelated_changes_never_invalidate_a_lookup() -> Result<(), Box<dyn std::error::Error>> {
        let ledger = ViewLedger::new();
        let watched = path(&["corpus", "file"])?;
        let file_id = FileId::new();
        let stamp = ledger.stamp();
        // Far more distinct names than any fixed slot table could separate.
        for index in 0..20_000 {
            let created = path(&["writes", &format!("file-{index}")])?;
            ledger.record(&ViewChange::Bound(&created));
            ledger.record(&ViewChange::Node(FileId::new()));
        }
        assert!(ledger.unchanged_since(&watched, Some(file_id), stamp));
        assert!(ledger.unchanged_since(&path(&["corpus"])?, None, stamp));
        assert!(!ledger.unchanged_since(&path(&["writes"])?, None, stamp));
        ledger.record(&ViewChange::Node(file_id));
        assert!(!ledger.unchanged_since(&watched, Some(file_id), stamp));
        Ok(())
    }

    /// A change reported under one spelling of a name reaches a lookup made
    /// under any other spelling a case- or normalization-insensitive host
    /// resolves to the same entry, whatever either name's encoding.
    #[test]
    fn every_spelling_of_a_name_shares_its_changes() -> Result<(), Box<dyn std::error::Error>> {
        let limits = VolumeLimits::default();
        let name = |encoding, text: &str| {
            let bytes = match encoding {
                NameEncoding::WindowsUtf16Le => {
                    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
                }
                NameEncoding::Utf8 | NameEncoding::PosixBytes => text.as_bytes().to_vec(),
            };
            LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
        };
        let directory = name(NameEncoding::Utf8, "src")?;
        let under = |name| NamespacePath::new(vec![directory.clone(), name], limits);
        let spellings = [
            (
                name(NameEncoding::Utf8, "Readme.MD")?,
                name(NameEncoding::PosixBytes, "README.md")?,
            ),
            (
                name(NameEncoding::WindowsUtf16Le, "ReadMe.md")?,
                name(NameEncoding::Utf8, "readme.MD")?,
            ),
            // Composed and decomposed forms of one accented name, in
            // different cases.
            (
                name(NameEncoding::PosixBytes, "\u{e9}t\u{e9}")?,
                name(NameEncoding::WindowsUtf16Le, "E\u{301}TE\u{301}")?,
            ),
        ];
        for (changed, looked_up) in spellings {
            let ledger = ViewLedger::new();
            let looked_up = under(looked_up)?;
            let stamp = ledger.stamp();
            ledger.record(&ViewChange::Bound(&under(changed)?));
            assert!(!ledger.unchanged_since(&looked_up, None, stamp));
        }
        let ledger = ViewLedger::new();
        let other = under(name(NameEncoding::Utf8, "readme.txt")?)?;
        let stamp = ledger.stamp();
        ledger.record(&ViewChange::Bound(&under(name(
            NameEncoding::Utf8,
            "readme.md",
        )?)?));
        assert!(
            ledger.unchanged_since(&other, None, stamp),
            "distinct names stay distinct"
        );
        // Bytes that are not Unicode are keyed as bytes.
        let raw = |bytes: &[u8]| {
            LogicalName::new(
                NameEncoding::PosixBytes,
                bytes.to_vec(),
                limits.maximum_component_bytes,
            )
        };
        let looked_up = under(raw(&[0xff, b'a'])?)?;
        let stamp = ledger.stamp();
        ledger.record(&ViewChange::Bound(&under(raw(&[0xff, b'A'])?)?));
        assert!(ledger.unchanged_since(&looked_up, None, stamp));
        Ok(())
    }

    #[test]
    fn folded_keys_invalidate_only_older_lookups() -> Result<(), Box<dyn std::error::Error>> {
        let ledger = ViewLedger::new();
        let watched = path(&["watched"])?;
        let before = ledger.stamp();
        for _ in 0..=MAXIMUM_TRACKED_KEYS {
            ledger.record(&ViewChange::Node(FileId::new()));
        }
        let after = ledger.stamp();
        let untouched = FileId::new();
        assert!(
            !ledger.unchanged_since(&watched, Some(untouched), before),
            "a folded map may not vouch for lookups older than what it folded"
        );
        assert!(ledger.unchanged_since(&watched, Some(untouched), after));
        Ok(())
    }

    #[test]
    fn observers_learn_each_change_with_its_origin() -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Default)]
        struct Recorded(Mutex<Vec<(ViewStamp, ViewOrigin)>>);

        impl super::ViewObserver for Recorded {
            fn view_changed(&self, position: ViewStamp, origin: ViewOrigin) {
                if let Ok(mut recorded) = self.0.lock() {
                    recorded.push((position, origin));
                }
            }
        }

        let ledger = ViewLedger::new();
        let recorded = Arc::new(Recorded::default());
        let observer: std::sync::Weak<Recorded> = Arc::downgrade(&recorded);
        ledger.observe(observer);
        let origin = ViewOrigin::new();
        ledger.record(&ViewChange::Everything);
        {
            let _scope = origin.enter();
            ledger.record(&ViewChange::Everything);
        }
        ledger.record(&ViewChange::Everything);
        let recorded = recorded.0.lock().map_err(|_| "poisoned")?.clone();
        let origins = recorded
            .iter()
            .map(|(_, origin)| *origin)
            .collect::<Vec<_>>();
        assert_eq!(
            origins,
            [ViewOrigin::UNATTRIBUTED, origin, ViewOrigin::UNATTRIBUTED]
        );
        assert!(recorded.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert_eq!(
            recorded.last().map(|(position, _)| *position),
            Some(ledger.stamp())
        );
        Ok(())
    }
}
