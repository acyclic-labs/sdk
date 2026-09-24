//! Exact invalidation of cached mount lookups.
//!
//! Every view change takes the next position in one process-wide order and
//! records it against exactly what it changed: a name binding, a directory's
//! listing and attributes, or one node. A driver samples a stamp before a
//! lookup and may reuse the result only while nothing that lookup depended on
//! has recorded a later position. Positions only grow and are never reused,
//! so a stale cache entry cannot validate again.

use crate::FileId;
use crate::kernel::NamespacePath;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

static VIEW_CHANGES: AtomicU64 = AtomicU64::new(0);

const LEDGER_SLOTS: u64 = 8_192;

/// One position in the process-wide order of mount view changes.
///
/// A stamp is at or after every change its source had recorded when it was
/// sampled, and before every later one. Only a source can sample a stamp,
/// and only that source can interpret it, through
/// [`super::MountFilesystem::unchanged_since`].
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ViewStamp(u64);

impl ViewStamp {
    /// A stamp at or after every change recorded anywhere so far.
    pub(super) fn current() -> Self {
        Self(VIEW_CHANGES.load(Ordering::Acquire))
    }

    /// The latest change a slot recorded.
    pub(super) fn of(slot: &AtomicU64) -> Self {
        Self(slot.load(Ordering::Acquire))
    }

    /// Records one change in a slot, at a position after every stamp
    /// sampled before it.
    pub(super) fn record(slot: &AtomicU64) {
        Self::next().record_in(slot);
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
    /// A lazily projected node became authored under the same identity and
    /// name; its ancestors may have been authored with it.
    Promoted(&'a NamespacePath, FileId),
    /// An effect set that cannot be enumerated, such as a rebind.
    Everything,
}

/// Change positions of one checkout's view.
///
/// Slots are hashed, so distinct keys may share one; a shared slot can only
/// invalidate more than necessary, never less.
pub(super) struct ViewLedger {
    latest: AtomicU64,
    everything: AtomicU64,
    bindings: Box<[AtomicU64]>,
    directories: Box<[AtomicU64]>,
    nodes: Box<[AtomicU64]>,
}

impl ViewLedger {
    pub(super) fn new() -> Self {
        let slots = || (0..LEDGER_SLOTS).map(|_| AtomicU64::new(0)).collect();
        Self {
            latest: AtomicU64::new(0),
            everything: AtomicU64::new(0),
            bindings: slots(),
            directories: slots(),
            nodes: slots(),
        }
    }

    /// A stamp at or after every change this ledger has recorded.
    pub(super) fn stamp(&self) -> ViewStamp {
        ViewStamp::of(&self.latest)
    }

    /// Records one change at one fresh position. Callers hold the exclusive
    /// view of what changed, so no lookup overlaps the change it records.
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
            ViewChange::Promoted(path, file_id) => {
                for key in PathKeys::new(path) {
                    position.record_in(self.slot(&self.directories, key));
                }
                self.node_changed(*file_id, position);
            }
            ViewChange::Everything => position.record_in(&self.everything),
        }
        position.record_in(&self.latest);
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
        let mut terminal = None;
        stamp.precedes_none_of(&self.everything)
            && PathKeys::new(path).all(|key| {
                terminal = Some(key);
                stamp.precedes_none_of(self.slot(&self.bindings, key))
            })
            && terminal.is_none_or(|key| stamp.precedes_none_of(self.slot(&self.directories, key)))
            && file_id.is_none_or(|file_id| {
                stamp.precedes_none_of(self.slot(&self.nodes, key_of(&file_id)))
            })
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
            position.record_in(self.slot(&self.bindings, name));
        }
        if let Some(parent) = parent {
            position.record_in(self.slot(&self.directories, parent));
        }
    }

    fn node_changed(&self, file_id: FileId, position: ViewStamp) {
        position.record_in(self.slot(&self.nodes, key_of(&file_id)));
    }

    /// A key's slot. `everything` stands in for an unaddressable slot: it
    /// only ever over-invalidates.
    fn slot<'a>(&'a self, slots: &'a [AtomicU64], key: u64) -> &'a AtomicU64 {
        usize::try_from(key % LEDGER_SLOTS)
            .ok()
            .and_then(|index| slots.get(index))
            .unwrap_or(&self.everything)
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
            Some(parent) => {
                let name = self.components.next()?;
                key_of(&(parent, name.case_fold_key()))
            }
        };
        self.key = Some(next);
        Some(next)
    }
}

fn key_of(value: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
