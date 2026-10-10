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
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};

fn unbounded_view_work() -> crate::demand::DemandWork {
    static TOKEN: std::sync::LazyLock<crate::CancellationToken> = std::sync::LazyLock::new(crate::CancellationToken::new);
    crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &TOKEN)
}

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
    /// though none is known to have been lost (`ViewChange::Unconfirmed`).
    /// An observer that cannot verify what it keeps treats it as a change.
    fn view_unconfirmed(&self, position: ViewStamp) {
        self.view_changed(position, ViewOrigin::current());
    }
}

/// The observers of one source of view changes.
#[derive(Default)]
pub(super) struct ViewObservers(Mutex<Vec<Weak<dyn ViewObserver>>>, AtomicU64);

impl ViewObservers {
    pub(super) fn add(&self, observer: Weak<dyn ViewObserver>) {
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let mut counters = crate::WorkCounters::UNCHARGED;
        self.add_measured(
            observer, &mut None, &mut allocations, &mut counters, crate::WorkBudget::UNBOUNDED,
        ).expect("unbounded observer registration failed");
    }

    fn add_measured(
        &self,
        observer: Weak<dyn ViewObserver>,
        registry: &mut Option<(u64, u64)>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
    ) -> Result<(), super::MountSourceError> {
        let mut observers = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let generation = self.1.load(Ordering::Acquire);
        if let Some((owned_generation, bytes)) = *registry
            && owned_generation != generation
        {
            allocations.release(bytes).map_err(observer_allocation_error)?;
            *registry = None;
        }
        let width = std::mem::size_of::<Weak<dyn ViewObserver>>();
        let old_bytes = observers.capacity().checked_mul(width)
            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        if registry.is_none() {
            allocations.claim_bytes(old_bytes, 0, work, budget).map_err(observer_allocation_error)?;
            *registry = Some((generation, old_bytes));
        }
        let sweep = crate::WorkCounters {
            items_examined: observers.len() as u64,
            bytes_copied: observers.len().checked_mul(width)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        work.admit(&sweep, &budget).map_err(super::MountSourceError::Work)?;
        let mut moved = 0_usize;
        let mut gap = false;
        observers.retain(|entry| {
            let alive = entry.strong_count() != 0;
            if alive { moved += usize::from(gap); } else { gap = true; }
            alive
        });
        // Strong owners can retire concurrently with the sweep. Charge only
        // the live slots that Vec::retain physically moved, not a snapshot.
        work.charge(&crate::WorkCounters {
            bytes_copied: (moved * width) as u64, ..sweep
        }, &budget).map_err(super::MountSourceError::Work)?;
        let required = observers.len().checked_add(1)
            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
        let grows = required > observers.capacity();
        if grows {
            let copied = crate::WorkCounters {
                items_examined: observers.len() as u64,
                bytes_copied: observers.len().checked_mul(width)
                    .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            work.admit(&copied, &budget).map_err(super::MountSourceError::Work)?;
            let next_generation = generation.checked_add(1)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
            let capacity = observers.capacity().max(1).checked_mul(2)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?.max(required);
            let bytes = capacity.checked_mul(width)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            allocations.claim_bytes(bytes, 1, work, budget).map_err(observer_allocation_error)?;
            let mut vector = Vec::new();
            if vector.try_reserve_exact(capacity).is_err() {
                allocations.release(bytes).map_err(observer_allocation_error)?;
                return Err(super::MountSourceError::Engine("observer registry allocation failed".into()));
            }
            vector.append(&mut observers);
            *observers = vector;
            self.1.store(next_generation, Ordering::Release);
            *registry = Some((next_generation, bytes));
            allocations.release(old_bytes).map_err(observer_allocation_error)?;
            work.charge(&copied, &budget).map_err(super::MountSourceError::Work)?;
        }
        let copied = crate::WorkCounters {
            bytes_copied: width as u64, ..crate::WorkCounters::UNCHARGED
        };
        work.admit(&copied, &budget).map_err(super::MountSourceError::Work)?;
        observers.push(observer);
        work.charge(&copied, &budget).map_err(super::MountSourceError::Work)?;
        Ok(())
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

fn observer_allocation_error(
    error: crate::kernel::allocation::AllocationError,
) -> super::MountSourceError {
    super::lazy::mount_engine_error(crate::collection::handle_allocation_error(error))
}

struct ObserverRegistryAdmission {
    ledger: Weak<ViewLedger>,
    registry: Option<(u64, u64)>,
}

impl ObserverRegistryAdmission {
    fn freed_bytes(&self) -> u64 {
        self.registry.map_or(0, |(generation, bytes)| {
            if self.ledger.upgrade().is_none_or(|ledger|
                ledger.observers.1.load(Ordering::Acquire) != generation)
            { bytes } else { 0 }
        })
    }
}

/// Original invocation custody of the canonical observer Vec backing.
#[derive(Default)]
pub(crate) struct ViewObserverAdmissions {
    first: Option<ObserverRegistryAdmission>,
    more: Vec<ObserverRegistryAdmission>,
}

impl ViewObserverAdmissions {
    pub(crate) fn freed_bytes(&self) -> u64 {
        self.first.iter().chain(self.more.iter()).try_fold(0_u64, |sum, entry|
            sum.checked_add(entry.freed_bytes()))
            .expect("observer registry custody cannot exceed original ledger ownership")
    }

    pub(crate) fn reconcile(
        &mut self,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
    ) -> Result<(), crate::WorkError> {
        for entry in self.first.iter_mut().chain(self.more.iter_mut()) {
            let bytes = entry.freed_bytes();
            if bytes != 0 {
                allocations.release(bytes)
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                entry.registry = None;
            }
        }
        Ok(())
    }

    fn entry_mut(
        &mut self,
        ledger: &Arc<ViewLedger>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
    ) -> Result<&mut ObserverRegistryAdmission, super::MountSourceError> {
        let identity = Arc::downgrade(ledger);
        let mut found = None;
        for (index, entry) in self.first.iter().chain(self.more.iter()).enumerate() {
            work.charge(&crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }, &budget).map_err(super::MountSourceError::Work)?;
            if Weak::ptr_eq(&entry.ledger, &identity) { found = Some(index); break; }
        }
        if let Some(index) = found {
            return Ok(if index == 0 {
                self.first.as_mut().expect("first observer admission")
            } else { &mut self.more[index - 1] });
        }
        if self.first.is_none() {
            self.first = Some(ObserverRegistryAdmission { ledger: identity, registry: None });
            return Ok(self.first.as_mut().expect("new first observer admission"));
        }
        if self.more.len() == self.more.capacity() {
            let capacity = self.more.capacity().max(1).checked_mul(2)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
            let width = std::mem::size_of::<ObserverRegistryAdmission>();
            let copied = crate::WorkCounters {
                items_examined: self.more.len() as u64,
                bytes_copied: self.more.len().checked_mul(width)
                    .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            work.admit(&copied, &budget).map_err(super::MountSourceError::Work)?;
            let bytes = capacity.checked_mul(width)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            allocations.claim_bytes(bytes, 1, work, budget).map_err(observer_allocation_error)?;
            let mut vector = Vec::new();
            if vector.try_reserve_exact(capacity).is_err() {
                allocations.release(bytes).map_err(observer_allocation_error)?;
                return Err(super::MountSourceError::Engine("observer admission allocation failed".into()));
            }
            let old_bytes = (self.more.capacity() * width) as u64;
            vector.append(&mut self.more);
            self.more = vector;
            work.charge(&copied, &budget).map_err(super::MountSourceError::Work)?;
            allocations.release(old_bytes).map_err(observer_allocation_error)?;
        }
        self.more.push(ObserverRegistryAdmission { ledger: identity, registry: None });
        Ok(self.more.last_mut().expect("new observer admission"))
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

/// Invocation-local ownership of backing buffers allocated by this ledger.
/// The canonical allocation ledger remains in `DemandWork`.
#[derive(Default)]
pub(super) struct ViewLedgerAdmission {
    bindings: Option<(u64, u64)>,
    directories: Option<(u64, u64)>,
    nodes: Option<(u64, u64)>,
}

impl ViewLedgerAdmission {
    pub(super) fn reconcile(
        &mut self,
        ledger: &ViewLedger,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), super::MountSourceError> {
        ledger.bindings.reconcile(&mut self.bindings, work)?;
        ledger.directories.reconcile(&mut self.directories, work)?;
        ledger.nodes.reconcile(&mut self.nodes, work)
    }
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

    pub(super) fn observe_measured(
        self: &Arc<Self>,
        observer: Weak<dyn ViewObserver>,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), super::MountSourceError> {
        work.check().map_err(super::lazy::mount_demand_error)?;
        let (admissions, allocations, counters, budget) =
            work.view_observer_admission_parts().map_err(super::lazy::mount_demand_error)?;
        let entry = admissions.entry_mut(self, allocations, counters, budget)?;
        self.observers.add_measured(
            observer, &mut entry.registry, allocations, counters, budget,
        )
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
        let mut work = unbounded_view_work();
        self.record_measured(change, &mut work, &mut ViewLedgerAdmission::default())
            .expect("unbounded view-ledger recording failed");
    }

    pub(super) fn record_measured(
        &self,
        change: &ViewChange<'_>,
        work: &mut crate::demand::DemandWork,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(), super::MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items, mount_vec};
        owned.reconcile(self, work)?;
        let observers = {
            let entries = self.observers.0.lock().unwrap_or_else(PoisonError::into_inner);
            let mut live = 0_usize;
            for observer in entries.iter() {
                mount_items(work, 1)?;
                if observer.strong_count() != 0 {
                    live = live.checked_add(1).ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
                }
            }
            // A weak entry cannot revive after reaching zero, and registration
            // is locked, so the second pass cannot exceed this admitted bound.
            let mut observers = mount_vec::<Arc<dyn ViewObserver>>(live, work)?;
            for observer in entries.iter() {
                mount_items(work, 1)?;
                if let Some(observer) = observer.upgrade() { observers.push(observer); }
            }
            // Notifications are cleanup for every recorded prefix. Admit them
            // before changing any key, then deliver even if cancellation wins.
            mount_items(work, observers.len())?;
            observers
        };
        let position = ViewStamp::next();
        let result = (|| {
            match change {
                ViewChange::Node(id) => self.node_changed_measured(*id, position, work, owned)?,
                ViewChange::Bound(path) => self.rebound_measured(path, position, work, owned)?,
                ViewChange::Unbound(path, id) | ViewChange::Linked(path, id) => {
                    self.rebound_measured(path, position, work, owned)?;
                    self.node_changed_measured(*id, position, work, owned)?;
                }
                ViewChange::Moved { from, to, moved, replaced } => {
                    self.rebound_measured(from, position, work, owned)?;
                    self.rebound_measured(to, position, work, owned)?;
                    self.node_changed_measured(*moved, position, work, owned)?;
                    if let Some(id) = replaced { self.node_changed_measured(*id, position, work, owned)?; }
                }
                ViewChange::Effect(effect) => {
                    for id in &effect.nodes { self.node_changed_measured(*id, position, work, owned)?; }
                    for (directory, name) in &effect.names {
                        let directory = path_key_measured(directory, work)?;
                        let child = child_key_measured(directory, name, work)?;
                        self.bindings.record_measured(child, position, work, &mut owned.bindings)?;
                        self.directories.record_measured(directory, position, work, &mut owned.directories)?;
                    }
                    for subtree in &effect.subtrees { self.rebound_measured(subtree, position, work, owned)?; }
                    for directory in &effect.listings {
                        let key = path_key_measured(directory, work)?;
                        self.directories.record_measured(key, position, work, &mut owned.directories)?;
                    }
                    if effect.everything { position.record_in(&self.everything); }
                }
                ViewChange::Everything => position.record_in(&self.everything),
                ViewChange::Unconfirmed => position.record_in(&self.unconfirmed),
            }
            Ok(())
        })();
        // A failed prefix only invalidates more; it never hides an installed
        // change. The observer sees the same position and origin as its keys.
        position.record_in(&self.latest);
        let origin = ViewOrigin::current();
        for observer in &observers {
            if matches!(change, ViewChange::Unconfirmed) {
                observer.view_unconfirmed(position);
            } else {
                observer.view_changed(position, origin);
            }
        }
        let bytes = (observers.capacity() * std::mem::size_of::<Arc<dyn ViewObserver>>()) as u64;
        drop(observers);
        work.release(bytes).map_err(mount_demand_error)?;
        result
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

    pub(super) fn unchanged_since_measured(
        &self, path: &NamespacePath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, super::MountSourceError> {
        work.check().map_err(super::lazy::mount_demand_error)?;
        if !stamp.precedes_none_of(&self.unconfirmed) { return Ok(false); }
        self.reported_unchanged_since_measured(path, file_id, stamp, work)
    }

    pub(super) fn reported_unchanged_since_measured(
        &self, path: &NamespacePath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, super::MountSourceError> {
        work.check().map_err(super::lazy::mount_demand_error)?;
        if !stamp.precedes_none_of(&self.everything) { return Ok(false); }
        let mut terminal = key_of(&());
        // One read of bindings covers every prefix, as in the ordinary query.
        let bindings = self.bindings.0.read().unwrap_or_else(PoisonError::into_inner);
        if !bindings.unchanged_key_measured(terminal, stamp, work)? { return Ok(false); }
        for name in path.components() {
            terminal = child_key_measured(terminal, name, work)?;
            if !bindings.unchanged_key_measured(terminal, stamp, work)? { return Ok(false); }
        }
        drop(bindings);
        if !self.directories.unchanged_key_measured(terminal, stamp, work)? { return Ok(false); }
        if let Some(file_id) = file_id {
            if !self.nodes.unchanged_key_measured(key_of(&file_id), stamp, work)? { return Ok(false); }
        }
        Ok(true)
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

    /// Whether `path` still names what it named after `stamp`: no component
    /// of it was rebound since. Changes beneath it, its listing's among
    /// them, do not count.
    pub(super) fn binding_unchanged_since(&self, path: &NamespacePath, stamp: ViewStamp) -> bool {
        stamp.precedes_none_of(&self.unconfirmed)
            && stamp.precedes_none_of(&self.everything)
            && self
                .bindings
                .unchanged_since(stamp, |unchanged| PathKeys::new(path).all(unchanged))
    }

    /// Whether the listing of the directory at `path` is unchanged since
    /// `stamp`: no name directly beneath it was bound, unbound, or rebound.
    /// Changes to `path`'s own binding, or deeper beneath it, do not count.
    pub(super) fn listing_unchanged_since(&self, path: &NamespacePath, stamp: ViewStamp) -> bool {
        let Some(directory) = PathKeys::new(path).last() else {
            return false;
        };
        stamp.precedes_none_of(&self.unconfirmed)
            && stamp.precedes_none_of(&self.everything)
            && self
                .directories
                .unchanged_since(stamp, |unchanged| unchanged(directory))
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
    fn rebound_measured(
        &self,
        path: &NamespacePath,
        position: ViewStamp,
        work: &mut crate::demand::DemandWork,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(), super::MountSourceError> {
        let mut parent = None;
        let mut terminal = key_of(&());
        for name in path.components() {
            parent = Some(terminal);
            terminal = child_key_measured(terminal, name, work)?;
        }
        self.bindings.record_measured(terminal, position, work, &mut owned.bindings)?;
        if let Some(parent) = parent {
            self.directories.record_measured(parent, position, work, &mut owned.directories)?;
        }
        Ok(())
    }

    fn node_changed_measured(
        &self,
        id: FileId,
        position: ViewStamp,
        work: &mut crate::demand::DemandWork,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(), super::MountSourceError> {
        super::lazy::mount_items(work, 1)?;
        self.nodes.record_measured(key_of(&id), position, work, &mut owned.nodes)
    }
}

/// The latest change recorded against each key. A key that was never
/// recorded, or whose record was folded away, reads as the floor.
#[derive(Default)]
struct ChangeMap(RwLock<ChangePositions>);

#[derive(Default)]
struct ChangePositions {
    floor: u64,
    slots: Vec<Option<(u64, u64)>>,
    length: usize,
    allocation_generation: u64,
}

impl ChangePositions {
    fn get(&self, key: u64) -> Option<u64> {
        self.get_visiting(key, || Ok::<(), std::convert::Infallible>(()))
            .expect("ordinary view probe is infallible")
    }

    fn get_visiting<E>(
        &self, key: u64, mut visit: impl FnMut() -> Result<(), E>,
    ) -> Result<Option<u64>, E> {
        if self.slots.is_empty() { return Ok(None); }
        let mask = self.slots.len() - 1;
        let mut index = key as usize & mask;
        loop {
            visit()?;
            match self.slots.get(index).copied().flatten() {
                None => return Ok(None),
                Some((stored, at)) if stored == key => return Ok(Some(at)),
                Some(_) => index = (index + 1) & mask,
            }
        }
    }

    fn unchanged_key_measured(
        &self, key: u64, stamp: ViewStamp, work: &mut crate::demand::DemandWork,
    ) -> Result<bool, super::MountSourceError> {
        Ok(self.get_visiting(key, || super::lazy::mount_items(work, 1))?
            .map_or(self.floor, |at| at.max(self.floor)) <= stamp.0)
    }
}

impl ChangeMap {
    fn reconcile(
        &self,
        owned: &mut Option<(u64, u64)>,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), super::MountSourceError> {
        use super::lazy::mount_demand_error;
        if let Some((generation, bytes)) = *owned {
            if generation != self.0.read().unwrap_or_else(PoisonError::into_inner).allocation_generation {
                work.release(bytes).map_err(mount_demand_error)?;
                *owned = None;
            }
        }
        Ok(())
    }

    fn record_measured(
        &self,
        key: u64,
        position: ViewStamp,
        work: &mut crate::demand::DemandWork,
        owned: &mut Option<(u64, u64)>,
    ) -> Result<(), super::MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items, mount_vec};
        let mut positions = self.0.write().unwrap_or_else(PoisonError::into_inner);
        if let Some((generation, bytes)) = *owned {
            if generation != positions.allocation_generation {
                work.release(bytes).map_err(mount_demand_error)?;
                *owned = None;
            }
        }
        loop {
            if !positions.slots.is_empty() {
                let mask = positions.slots.len() - 1;
                let mut index = key as usize & mask;
                loop {
                    mount_items(work, 1)?;
                    match positions.slots.get(index).copied().flatten() {
                        Some((stored, at)) if stored == key => {
                            *positions.slots.get_mut(index).ok_or(super::MountSourceError::Stale)? = Some((key, at.max(position.0)));
                            return Ok(());
                        }
                        Some(_) => index = (index + 1) & mask,
                        None => {
                            if positions.length < MAXIMUM_TRACKED_KEYS
                                && (positions.length + 1) * 2 <= positions.slots.len() {
                                *positions.slots.get_mut(index).ok_or(super::MountSourceError::Stale)? = Some((key, position.0));
                                positions.length += 1;
                                return Ok(());
                            }
                            break;
                        }
                    }
                }
            }
            if positions.length == MAXIMUM_TRACKED_KEYS {
                mount_items(work, positions.slots.len())?;
                positions.floor = positions.slots.iter().flatten().fold(positions.floor, |floor, (_, at)| floor.max(*at));
                positions.slots.fill(None);
                positions.length = 0;
                continue;
            }
            let old_bytes = (positions.slots.capacity() * std::mem::size_of::<Option<(u64, u64)>>()) as u64;
            let borrowed_old = owned.is_none();
            if borrowed_old { work.adopt(old_bytes).map_err(mount_demand_error)?; }
            let count = positions.slots.len().max(4).saturating_mul(2).min(MAXIMUM_TRACKED_KEYS * 2);
            let new_bytes = (count * std::mem::size_of::<Option<(u64, u64)>>()) as u64;
            let mut replacement = match mount_vec(count, work) {
                Ok(replacement) => replacement,
                Err(error) => {
                    if borrowed_old { work.release(old_bytes).map_err(mount_demand_error)?; }
                    return Err(error);
                }
            };
            replacement.resize(count, None);
            let rehashed = (|| -> Result<(), super::MountSourceError> {
            for entry in &positions.slots {
                mount_items(work, 1)?;
                if let Some((stored, at)) = entry {
                    let mut index = *stored as usize & (count - 1);
                    loop {
                        mount_items(work, 1)?;
                        let slot = replacement.get_mut(index).ok_or(super::MountSourceError::Stale)?;
                        if slot.is_none() {
                            work.charge(crate::WorkCounters {
                                bytes_copied: std::mem::size_of::<Option<(u64, u64)>>() as u64,
                                ..crate::WorkCounters::default()
                            }).map_err(mount_demand_error)?;
                            *slot = Some((*stored, *at));
                            break;
                        }
                        index = (index + 1) & (count - 1);
                    }
                }
            }
                Ok(())
            })();
            if let Err(error) = rehashed {
                work.release(new_bytes).map_err(mount_demand_error)?;
                if borrowed_old { work.release(old_bytes).map_err(mount_demand_error)?; }
                return Err(error);
            }
            positions.slots = replacement;
            positions.allocation_generation += 1;
            work.release(old_bytes).map_err(mount_demand_error)?;
            *owned = Some((positions.allocation_generation, new_bytes));
        }
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
            positions.get(key).map_or(positions.floor, |at| at.max(positions.floor))
                <= stamp.0
        })
    }

    fn unchanged_key_measured(
        &self, key: u64, stamp: ViewStamp, work: &mut crate::demand::DemandWork,
    ) -> Result<bool, super::MountSourceError> {
        self.0.read().unwrap_or_else(PoisonError::into_inner)
            .unchanged_key_measured(key, stamp, work)
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


/// Keys `name` beneath `parent` by its spelling fold: the name's Unicode
/// text, canonically composed and case-folded, whatever its encoding, or
/// its bytes when it is not Unicode. Every spelling a case- or
/// normalization-insensitive host resolves to one entry shares the key, and
/// distinct names that share it only share positions, which can only
/// invalidate more.
fn child_key(parent: u64, name: &LogicalName) -> u64 {
    let mut work = unbounded_view_work();
    child_key_measured(parent, name, &mut work).expect("unbounded view key failed")
}

fn path_key_measured(
    path: &NamespacePath,
    work: &mut crate::demand::DemandWork,
) -> Result<u64, super::MountSourceError> {
    let mut key = key_of(&());
    for name in path.components() { key = child_key_measured(key, name, work)?; }
    Ok(key)
}

fn child_key_measured(
    parent: u64,
    name: &LogicalName,
    work: &mut crate::demand::DemandWork,
) -> Result<u64, super::MountSourceError> {
    use super::lazy::mount_items;
    const TEXT: u8 = 1;
    const RAW: u8 = 2;
    mount_items(work, 1)?;
    let mut hasher = DefaultHasher::new();
    hasher.write_u64(parent);
    let bytes = name.as_bytes();
    match name.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            if bytes.is_ascii() {
                mount_items(work, bytes.len())?;
                hasher.write_u8(TEXT);
                hash_ascii_fold(bytes.iter().copied(), &mut hasher);
            } else if let Ok(text) = std::str::from_utf8(bytes) {
                hasher.write_u8(TEXT);
                hash_unicode_fold(text.chars(), &mut hasher, work)?;
            } else {
                mount_items(work, bytes.len())?;
                hasher.write_u8(RAW);
                hasher.write(bytes);
            }
        }
        NameEncoding::WindowsUtf16Le => {
            let units = crate::kernel::types::utf16le_units(bytes).ok_or(super::MountSourceError::Stale)?;
            mount_items(work, bytes.len() / 2)?;
            if units.clone().all(|unit| unit < 0x80) {
                hasher.write_u8(TEXT);
                hash_ascii_fold(units.map(|unit| unit.to_le_bytes()[0]), &mut hasher);
            } else {
                let mut valid = true;
                for decoded in char::decode_utf16(units.clone()) {
                    mount_items(work, 1)?;
                    if decoded.is_err() { valid = false; break; }
                }
                if valid {
                    hasher.write_u8(TEXT);
                    hash_unicode_fold(char::decode_utf16(units).filter_map(Result::ok), &mut hasher, work)?;
                } else {
                    mount_items(work, bytes.len())?;
                    hasher.write_u8(RAW);
                    hasher.write(bytes);
                }
            }
        }
    }
    Ok(hasher.finish())
}

/// The same NFC spelling fold, with a single explicitly owned buffer instead
/// of normalization iterators whose internal spill allocations are opaque.
fn hash_unicode_fold<I: Iterator<Item = char> + Clone>(
    text: I,
    hasher: &mut DefaultHasher,
    work: &mut crate::demand::DemandWork,
) -> Result<(), super::MountSourceError> {
    use super::lazy::{mount_demand_error, mount_items, mount_vec};
    use unicode_normalization::char::{canonical_combining_class, compose, decompose_canonical};
    let mut count = 0_usize;
    for scalar in text.clone() {
        mount_items(work, 1)?;
        decompose_canonical(scalar, |_| count += 1);
    }
    let mut decomposed = mount_vec::<(u8, char)>(count, work)?;
    for scalar in text {
        let mut emitted = 0;
        decompose_canonical(scalar, |_| emitted += 1);
        mount_items(work, emitted)?;
        decompose_canonical(scalar, |scalar| decomposed.push((canonical_combining_class(scalar), scalar)));
    }
    // Canonical ordering is stable within each combining sequence. In-place
    // insertion makes every comparison/move interruptible and uses no scratch.
    for index in 1..decomposed.len() {
        let value = *decomposed.get(index).ok_or(super::MountSourceError::Stale)?;
        if value.0 == 0 { continue; }
        let mut position = index;
        while position > 0 {
            mount_items(work, 1)?;
            let prior = *decomposed.get(position - 1).ok_or(super::MountSourceError::Stale)?;
            if prior.0 <= value.0 { break; }
            work.charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<(u8, char)>() as u64,
                ..crate::WorkCounters::default()
            }).map_err(mount_demand_error)?;
            *decomposed.get_mut(position).ok_or(super::MountSourceError::Stale)? = prior;
            position -= 1;
        }
        if position != index {
            *decomposed.get_mut(position).ok_or(super::MountSourceError::Stale)? = value;
        }
    }
    let mut written = 0;
    let mut starter: Option<usize> = None;
    let mut last_class = 0;
    for read in 0..decomposed.len() {
        mount_items(work, 1)?;
        let value = *decomposed.get(read).ok_or(super::MountSourceError::Stale)?;
        if let Some(starter) = starter {
            if last_class == 0 || last_class < value.0 {
                let prior = decomposed.get(starter).ok_or(super::MountSourceError::Stale)?.1;
                if let Some(composed) = compose(prior, value.1) {
                    decomposed.get_mut(starter).ok_or(super::MountSourceError::Stale)?.1 = composed;
                    continue;
                }
            }
        }
        if value.0 == 0 { starter = Some(written); }
        last_class = value.0;
        *decomposed.get_mut(written).ok_or(super::MountSourceError::Stale)? = value;
        written += 1;
    }
    let mut encoded = [0_u8; 4];
    for (_, scalar) in decomposed.get(..written).ok_or(super::MountSourceError::Stale)? {
        for folded in scalar.to_uppercase().flat_map(char::to_lowercase) {
            mount_items(work, 1)?;
            hasher.write(folded.encode_utf8(&mut encoded).as_bytes());
        }
    }
    drop(decomposed);
    work.release((count * std::mem::size_of::<(u8, char)>()) as u64).map_err(mount_demand_error)?;
    Ok(())
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
    use super::{MAXIMUM_TRACKED_KEYS, ViewChange, ViewEffect, ViewLedger, ViewLedgerAdmission, ViewObserver, ViewOrigin, ViewStamp};
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
    fn bounded_ledger_denial_retains_only_recorded_effect_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let first = FileId::from_bytes([1; 16]);
        let second = FileId::from_bytes([2; 16]);
        let probe = ViewLedger::new();
        let mut admitted = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        probe.record_measured(&ViewChange::Node(first), &mut admitted, &mut ViewLedgerAdmission::default())?;
        let ledger = ViewLedger::new();
        let before = ledger.stamp();
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget {
            items_examined: admitted.work.items_examined,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        let effect = ViewEffect { nodes: vec![first, second], ..ViewEffect::default() };
        let failure = ledger.record_measured(&ViewChange::Effect(&effect), &mut work, &mut ViewLedgerAdmission::default())
            .expect_err("the second key must not pass the first-key allowance");
        assert!(matches!(failure, super::super::MountSourceError::Work(_)));
        assert!(!ledger.node_unchanged_since(first, before));
        assert!(ledger.node_unchanged_since(second, before));
        assert_eq!(work.work, admitted.work);
        let untouched = ViewLedger::new();
        let mut denied = crate::demand::DemandWork::new(crate::WorkBudget {
            allocation_operations: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(untouched.record_measured(&ViewChange::Node(first), &mut denied, &mut ViewLedgerAdmission::default()).is_err());
        assert_eq!(denied.work.allocation_operations, 0);
        assert_eq!(denied.work.peak_allocation_bytes, 0);
        assert!(untouched.node_unchanged_since(first, before));
        Ok(())
    }

    #[test]
    fn bounded_ledger_reconciles_external_backing_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let ledger = ViewLedger::new();
        let first = FileId::from_bytes([1; 16]);
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let mut ownership = ViewLedgerAdmission::default();
        ledger.record_measured(&ViewChange::Node(first), &mut work, &mut ownership)?;
        let generation = ownership.nodes.expect("owned initial backing").0;
        assert!(work.live_allocation_bytes() > 0);
        for byte in 2..=8 {
            ledger.record(&ViewChange::Node(FileId::from_bytes([byte; 16])));
        }
        assert!(ledger.nodes.0.read().expect("change map").allocation_generation > generation);
        ledger.record_measured(&ViewChange::Node(first), &mut work, &mut ownership)?;
        assert!(ownership.nodes.is_none());
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(work.work.allocation_operations, 1);
        Ok(())
    }

    #[test]
    fn bounded_ledger_releases_external_replacement_before_observer_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        struct Count(std::sync::atomic::AtomicUsize);
        impl ViewObserver for Count {
            fn view_changed(&self, _: ViewStamp, _: ViewOrigin) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let cancellation = crate::CancellationToken::new();
        let ledger = ViewLedger::new();
        let first = FileId::from_bytes([1; 16]);
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let mut ownership = ViewLedgerAdmission::default();
        ledger.record_measured(&ViewChange::Node(first), &mut work, &mut ownership)?;
        let peak = work.work.peak_allocation_bytes;
        for byte in 2..=8 {
            ledger.record(&ViewChange::Node(FileId::from_bytes([byte; 16])));
        }
        let counted = Arc::new(Count(Default::default()));
        let observer: Arc<dyn ViewObserver> = counted.clone();
        ledger.observe(Arc::downgrade(&observer));
        work.budget.peak_allocation_bytes = peak;
        ledger.record_measured(&ViewChange::Node(first), &mut work, &mut ownership)?;
        assert_eq!(counted.0.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(ownership.nodes.is_none());
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(work.work.allocation_operations, 2);
        assert_eq!(work.work.peak_allocation_bytes, peak);
        Ok(())
    }

    #[test]
    fn bounded_unicode_fold_matches_canonical_nfc_model()
    -> Result<(), Box<dyn std::error::Error>> {
        use unicode_normalization::UnicodeNormalization as _;
        let cases = [
            "Résumé", "RE\u{301}SUME\u{301}", "\u{1100}\u{1161}\u{11a8}",
            "\u{301}\u{327}A", "a\u{315}\u{300}", "\u{344}\u{315}\u{300}",
            "Straße", "\u{212b}\u{301}", "\u{1e9b}\u{323}", "\u{1d160}\u{1d165}",
        ];
        for text in cases {
            let mut expected = DefaultHasher::new();
            expected.write_u64(37);
            expected.write_u8(1);
            let mut encoded = [0; 4];
            for scalar in text.nfc().flat_map(char::to_uppercase).flat_map(char::to_lowercase) {
                expected.write(scalar.encode_utf8(&mut encoded).as_bytes());
            }
            for encoding in [NameEncoding::Utf8, NameEncoding::PosixBytes, NameEncoding::WindowsUtf16Le] {
                let bytes = if encoding == NameEncoding::WindowsUtf16Le {
                    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
                } else { text.as_bytes().to_vec() };
                let name = LogicalName::new(encoding, bytes, 510)?;
                let cancellation = crate::CancellationToken::new();
                let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
                assert_eq!(child_key_measured(37, &name, &mut work)?, expected.finish(), "{text:?} {encoding:?}");
                assert!(work.work.items_examined > 0);
                assert_eq!(work.work.allocation_operations, 1);
            }
        }
        Ok(())
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
