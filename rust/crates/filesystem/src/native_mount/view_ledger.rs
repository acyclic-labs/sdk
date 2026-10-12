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

    /// Known native allocation custody, when this is an SDK-owned observer.
    /// This only returns an existing physical-ownership alias.
    fn allocation_custody(&self) -> Option<super::NativeMountAllocationCustody> {
        None
    }

    /// Changes to the source may not all have been reported by `position`,
    /// though none is known to have been lost (`ViewChange::Unconfirmed`).
    /// An observer that cannot verify what it keeps treats it as a change.
    fn view_unconfirmed(&self, position: ViewStamp) {
        self.view_changed(position, ViewOrigin::current());
    }
}

struct ViewObserverSlot {
    // Fields retire in declaration order: the real weak allocation must
    // retire before its physical-custody alias.
    observer: Weak<dyn ViewObserver>,
    _custody: Option<super::NativeMountAllocationCustody>,
}

#[derive(Default)]
struct ViewObserverRegistry {
    observers: Vec<ViewObserverSlot>,
    pending: Option<PendingViewNotification>,
    next_notification: u64,
}

enum PendingObserverState {
    Reserved(Weak<dyn ViewObserver>),
    Ready(Arc<dyn ViewObserver>),
    Dead,
}

struct PendingObserverDelivery {
    observer: PendingObserverState,
    sync: Arc<crate::demand::NativeDemandReadMeter>,
    credits: u64,
}

impl PendingObserverDelivery {
    fn visit(&mut self) {
        self.sync.lock().record_pending_items(1)
            .expect("an actual notification visit owns an original credit");
        self.credits = self.credits.checked_sub(1).expect("one notification visit");
    }
}

impl Drop for PendingObserverDelivery {
    fn drop(&mut self) {
        if self.credits != 0 {
            self.sync.lock().refund_pending_items(self.credits);
        }
    }
}

impl PendingDeliveryBuffer {
    fn reserve(
        &mut self, required: usize, work: &mut crate::demand::DemandWork,
    ) -> Result<(), super::MountSourceError> {
        use super::lazy::{mount_demand_error, mount_vec};
        if required <= self.entries.capacity() { return Ok(()); }
        let capacity = if self.entries.capacity() == 0 { required } else {
            self.entries.capacity().checked_mul(2)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?.max(required)
        };
        let width = std::mem::size_of::<PendingObserverDelivery>();
        let copied = crate::WorkCounters {
            bytes_copied: self.entries.len().checked_mul(width)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        work.admit(copied).map_err(mount_demand_error)?;
        let mut entries = mount_vec::<PendingObserverDelivery>(capacity, work)?;
        let bytes = entries.capacity().checked_mul(width)
            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        let custody = match super::NativeMountAllocationCustody::from_admitted(bytes, work) {
            Ok(custody) => custody,
            Err(error) => {
                drop(entries);
                work.release(bytes).map_err(mount_demand_error)?;
                return Err(super::lazy::mount_demand_error(error));
            }
        };
        entries.append(&mut self.entries);
        drop(std::mem::replace(&mut self.entries, entries));
        if let Some(old) = self.custody.replace(custody) {
            old.retire(self.bytes).map_err(super::MountSourceError::Work)?;
        }
        self.bytes = bytes;
        work.record(copied).map_err(mount_demand_error)
    }

    fn cancel_with_work(&mut self, work: &mut crate::demand::DemandWork) {
        for delivery in &mut self.entries {
            if delivery.credits != 0 {
                work.record_pending_items(1).expect("actual prepared cleanup owns a credit");
                delivery.credits -= 1;
                work.refund_pending_items(delivery.credits);
                delivery.credits = 0;
            }
        }
    }
}

#[derive(Default)]
struct PendingDeliveryBuffer {
    entries: Vec<PendingObserverDelivery>,
    bytes: u64,
    custody: Option<super::NativeMountAllocationCustody>,
}

impl Drop for PendingDeliveryBuffer {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.entries));
        if let Some(custody) = self.custody.take() {
            custody.retire(self.bytes).expect("notification backing retires once");
        }
    }
}

struct PendingViewNotification {
    id: u64,
    deliveries: PendingDeliveryBuffer,
    sync: Weak<crate::demand::NativeDemandReadMeter>,
}

pub(super) struct PreparedEverythingNotification {
    ledger: Arc<ViewLedger>,
    id: u64,
}

impl Drop for PreparedEverythingNotification {
    fn drop(&mut self) {
        if self.id == 0 { return; }
        let pending = {
            let mut hub = self.ledger.observers.0.lock().unwrap_or_else(PoisonError::into_inner);
            if hub.pending.as_ref().is_some_and(|pending| pending.id == self.id) {
                hub.pending.take()
            } else { None }
        };
        drop(pending);
    }
}

struct PreparedObserverJoin {
    id: u64,
    sync: Arc<crate::demand::NativeDemandReadMeter>,
    credits: u64,
}

impl Drop for PreparedObserverJoin {
    fn drop(&mut self) {
        if self.credits != 0 { self.sync.lock().refund_pending_items(self.credits); }
    }
}

fn join_matches(hub: &ViewObserverRegistry, join: Option<&PreparedObserverJoin>) -> bool {
    match (hub.pending.as_ref(), join) {
        (None, None) => true,
        (Some(pending), Some(join)) => pending.id == join.id,
        _ => false,
    }
}

fn prepare_join_buffer(
    hub: &mut ViewObserverRegistry, work: &mut crate::demand::DemandWork,
) -> Result<u64, super::MountSourceError> {
    let Some(pending) = hub.pending.as_mut() else { return Ok(0); };
    let bytes = std::mem::size_of::<PendingObserverDelivery>() as u64;
    work.reserve_copy(bytes).map_err(super::lazy::mount_demand_error)?;
    let result = pending.deliveries.entries.len().checked_add(1)
        .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))
        .and_then(|required| pending.deliveries.reserve(required, work));
    if let Err(error) = result {
        work.cancel_reserved_copy(bytes);
        return Err(error);
    }
    Ok(bytes)
}

fn append_join(
    hub: &mut ViewObserverRegistry, observer: &Weak<dyn ViewObserver>,
    join: &mut Option<PreparedObserverJoin>, work: &mut crate::demand::DemandWork,
    copy_credit: &mut u64,
) -> Result<(), super::MountSourceError> {
    let Some(join) = join.as_mut() else { return Ok(()); };
    let pending = hub.pending.as_mut().expect("registration matched its original event");
    pending.deliveries.entries.push(PendingObserverDelivery {
        observer: PendingObserverState::Reserved(Weak::clone(observer)),
        sync: Arc::clone(&join.sync), credits: std::mem::take(&mut join.credits),
    });
    let copied = std::mem::take(copy_credit);
    work.record_reserved_copy(copied, copied).map_err(super::lazy::mount_demand_error)
}

/// The observers of one source of view changes.
#[derive(Default)]
pub(super) struct ViewObservers(Mutex<ViewObserverRegistry>, AtomicU64);

impl ViewObservers {
    pub(super) fn add(&self, observer: Weak<dyn ViewObserver>) -> Result<(), super::MountSourceError> {
        let custody = observer.upgrade().and_then(|observer| observer.allocation_custody());
        let mut work = unbounded_view_work();
        loop {
            let mut join = self.prepare_join()?;
            let mut hub = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            if !join_matches(&hub, join.as_ref()) { drop(hub); drop(join); continue; }
            let mut copy_credit = prepare_join_buffer(&mut hub, &mut work)?;
            let (allocations, counters, budget, _) = work.allocation_parts();
            self.add_measured_locked(
                &mut hub.observers, observer.clone(), custody.clone(), &mut None,
                allocations, counters, budget,
            )?;
            append_join(&mut hub, &observer, &mut join, &mut work, &mut copy_credit)?;
            drop(hub);
            drop(join);
            return Ok(());
        }
    }

    fn prepare_join(&self) -> Result<Option<PreparedObserverJoin>, super::MountSourceError> {
        let pending = {
            let hub = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            hub.pending.as_ref().map(|pending| (pending.id, pending.sync.clone()))
        };
        let Some((id, original)) = pending else { return Ok(None); };
        let sync = original.upgrade().ok_or(super::MountSourceError::Stale)?;
        sync.lock().reserve_pending_items(2).map_err(super::lazy::mount_demand_error)?;
        Ok(Some(PreparedObserverJoin { id, sync, credits: 2 }))
    }

    fn add_measured_locked(
        &self,
        observers: &mut Vec<ViewObserverSlot>,
        observer: Weak<dyn ViewObserver>,
        custody: Option<super::NativeMountAllocationCustody>,
        registry: &mut Option<(u64, u64)>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
    ) -> Result<(), super::MountSourceError> {
        let generation = self.1.load(Ordering::Acquire);
        if let Some((owned_generation, bytes)) = *registry
            && owned_generation != generation
        {
            allocations.release(bytes).map_err(observer_allocation_error)?;
            *registry = None;
        }
        let width = std::mem::size_of::<ViewObserverSlot>();
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
            let alive = entry.observer.strong_count() != 0;
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
            vector.append(observers);
            *observers = vector;
            self.1.store(next_generation, Ordering::Release);
            let old_admission = *registry;
            *registry = Some((next_generation, bytes));
            if let Some((owned_generation, owned_bytes)) = old_admission
                && owned_generation == generation
            {
                allocations.release(owned_bytes).map_err(observer_allocation_error)?;
            }
            work.charge(&copied, &budget).map_err(super::MountSourceError::Work)?;
        }
        let copied = crate::WorkCounters {
            bytes_copied: width as u64, ..crate::WorkCounters::UNCHARGED
        };
        work.admit(&copied, &budget).map_err(super::MountSourceError::Work)?;
        observers.push(ViewObserverSlot { observer, _custody: custody });
        work.charge(&copied, &budget).map_err(super::MountSourceError::Work)?;
        Ok(())
    }

    /// Every observer still alive.
    pub(super) fn live(&self) -> Vec<Arc<dyn ViewObserver>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observers.iter()
            .filter_map(|entry| entry.observer.upgrade())
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
pub(crate) struct ViewLedger {
    latest: AtomicU64,
    everything: AtomicU64,
    unconfirmed: AtomicU64,
    bindings: ChangeMap,
    directories: ChangeMap,
    nodes: ChangeMap,
    observers: ViewObservers,
    // Intrinsic constructor custody survives every strong ledger alias.
    _native_allocation: Option<crate::NativeAllocation>,
}

/// Owns the actual observer backing, accepted completion credit, and the
/// canonical node slot until a synchronous body edit commits or aborts.
/// No await may occur while this permit holds the node map's write lock.
pub(super) struct PreparedNativeNodeChange<'a> {
    ledger: &'a ViewLedger,
    slot: PreparedChangeMapSlot<'a>,
    observers: ViewRecordBuffer<Arc<dyn ViewObserver>>,
    completion: crate::materializer::NativeWorkReservation,
}

/// Invocation-local ownership of backing buffers allocated by this ledger.
/// The canonical allocation ledger remains in `DemandWork`.
#[derive(Default)]
pub(crate) struct ChangeMapAdmissions {
    bindings: Option<(u64, u64)>,
    directories: Option<(u64, u64)>,
    nodes: Option<(u64, u64)>,
}

#[derive(Clone, Copy)]
enum AdmittedMap { Bindings, Directories, Nodes }

impl ChangeMapAdmissions {
    fn slot(&mut self, map: AdmittedMap) -> &mut Option<(u64, u64)> {
        match map {
            AdmittedMap::Bindings => &mut self.bindings,
            AdmittedMap::Directories => &mut self.directories,
            AdmittedMap::Nodes => &mut self.nodes,
        }
    }

    fn claims(&self) -> [Option<(u64, u64)>; 3] {
        [self.bindings, self.directories, self.nodes]
    }
}

/// Registered and local aliases share the original three buffer claims.
/// An ordinary, invocation-local proof needs no additional control allocation.
pub(crate) enum ViewLedgerAdmission {
    Inline(ChangeMapAdmissions),
    Shared(Arc<Mutex<ChangeMapAdmissions>>),
}

impl Default for ViewLedgerAdmission {
    fn default() -> Self { Self::Inline(ChangeMapAdmissions::default()) }
}

impl ViewLedgerAdmission {
    fn shared(
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        counters: &mut crate::WorkCounters, budget: crate::WorkBudget,
    ) -> Result<Self, super::MountSourceError> {
        allocations.claim_elements::<Mutex<ChangeMapAdmissions>>(1, counters, budget)
            .map_err(observer_allocation_error)?;
        Ok(Self::Shared(Arc::new(Mutex::new(ChangeMapAdmissions::default()))))
    }

    fn alias(&self) -> Self {
        match self {
            Self::Shared(control) => Self::Shared(Arc::clone(control)),
            Self::Inline(_) => unreachable!("registered map proof has shared custody"),
        }
    }

    fn claims(&self) -> [Option<(u64, u64)>; 3] {
        match self {
            Self::Inline(state) => state.claims(),
            Self::Shared(control) => control.lock().unwrap_or_else(PoisonError::into_inner).claims(),
        }
    }

    fn replace(&mut self, map: AdmittedMap, value: Option<(u64, u64)>) -> Option<(u64, u64)> {
        match self {
            Self::Inline(state) => std::mem::replace(state.slot(map), value),
            Self::Shared(control) => {
                let mut state = control.lock().unwrap_or_else(PoisonError::into_inner);
                std::mem::replace(state.slot(map), value)
            }
        }
    }

    fn claim(&self, map: AdmittedMap) -> Option<(u64, u64)> {
        self.claims()[match map {
            AdmittedMap::Bindings => 0, AdmittedMap::Directories => 1, AdmittedMap::Nodes => 2,
        }]
    }

    fn dead_claims(&self, ledger: Option<&ViewLedger>) -> u64 {
        self.claims().into_iter().zip([
            ledger.map(|ledger| &ledger.bindings),
            ledger.map(|ledger| &ledger.directories),
            ledger.map(|ledger| &ledger.nodes),
        ]).filter_map(|(owned, map)| owned.filter(|(generation, _)|
            map.is_none_or(|map| map.1.load(Ordering::Acquire) != *generation)))
            .try_fold(0_u64, |total, (_, bytes)| total.checked_add(bytes))
            .expect("original map backing cannot exceed admitted ownership")
    }

    fn retire_dead(&mut self, ledger: Option<&ViewLedger>) -> u64 {
        let mut retired = 0_u64;
        for (kind, map) in [
            (AdmittedMap::Bindings, ledger.map(|ledger| &ledger.bindings)),
            (AdmittedMap::Directories, ledger.map(|ledger| &ledger.directories)),
            (AdmittedMap::Nodes, ledger.map(|ledger| &ledger.nodes)),
        ] {
            if self.claim(kind).is_some_and(|(generation, _)|
                map.is_none_or(|map| map.1.load(Ordering::Acquire) != generation)) {
                if let Some((_, bytes)) = self.replace(kind, None) {
                    retired = retired.checked_add(bytes).expect("admitted map ownership fits");
                }
            }
        }
        retired
    }

    pub(super) fn reconcile(
        &mut self, ledger: &ViewLedger, work: &mut crate::demand::DemandWork,
    ) -> Result<(), super::MountSourceError> {
        // Never call into the original carrier while holding the proof mutex:
        // its central roots can reconcile this same state during any leaf.
        let retired = self.retire_dead(Some(ledger));
        work.release(retired).map_err(super::lazy::mount_demand_error)
    }
}

struct LedgerRegistryAdmission {
    ledger: Weak<ViewLedger>,
    admission: Option<ViewLedgerAdmission>,
    loans: u64,
}

impl LedgerRegistryAdmission {
    fn freed_bytes(&self) -> u64 {
        self.admission.as_ref().map_or(0, |admission|
            admission.dead_claims(self.ledger.upgrade().as_deref()))
    }

    fn reconcile(&mut self, allocations: &mut crate::kernel::allocation::AllocationLedger)
    -> Result<(), crate::WorkError> {
        let ledger = self.ledger.upgrade();
        if let Some(admission) = &mut self.admission {
            let retired = admission.retire_dead(ledger.as_deref());
            allocations.release(retired).map_err(|error|
                error.into_work_or(crate::WorkError::Overflow))?;
        }
        if ledger.is_none() && self.loans == 0 && self.admission.is_some() {
            drop(self.admission.take());
            allocations.release(std::mem::size_of::<Mutex<ChangeMapAdmissions>>() as u64)
                .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        }
        Ok(())
    }
}

/// Original carrier custody of the same three canonical change-map buffers.
#[derive(Default)]
pub(crate) struct ViewLedgerAdmissions {
    first: Option<LedgerRegistryAdmission>,
    more: Vec<LedgerRegistryAdmission>,
}

impl ViewLedgerAdmissions {
    pub(crate) fn freed_bytes(&self) -> u64 {
        self.first.iter().chain(&self.more).try_fold(0_u64, |total, entry|
            total.checked_add(entry.freed_bytes()))
            .expect("original registry cannot exceed admitted ownership")
    }

    pub(crate) fn reconcile(
        &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
    ) -> Result<(), crate::WorkError> {
        for entry in self.first.iter_mut().chain(&mut self.more) { entry.reconcile(allocations)?; }
        Ok(())
    }

    pub(crate) fn take(
        &mut self, ledger: &Arc<ViewLedger>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        counters: &mut crate::WorkCounters, budget: crate::WorkBudget,
    ) -> Result<(usize, ViewLedgerAdmission), super::MountSourceError> {
        let identity = Arc::downgrade(ledger);
        let mut reusable = None;
        for (slot, entry) in self.first.iter_mut().chain(&mut self.more).enumerate() {
            counters.charge_items(1, &budget).map_err(super::MountSourceError::Work)?;
            if entry.ledger.ptr_eq(&identity) {
                let loans = entry.loans.checked_add(1)
                    .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
                let admission = entry.admission.as_ref()
                    .expect("live registered map proof").alias();
                entry.loans = loans;
                return Ok((slot, admission));
            }
            if reusable.is_none() && entry.loans == 0 && entry.ledger.upgrade().is_none() {
                reusable = Some(slot);
            }
        }
        if let Some(slot) = reusable {
            let entry = if slot == 0 { self.first.as_mut().expect("first original root") }
                else { &mut self.more[slot - 1] };
            entry.reconcile(allocations).map_err(super::MountSourceError::Work)?;
            let admission = ViewLedgerAdmission::shared(allocations, counters, budget)?;
            let local = admission.alias();
            *entry = LedgerRegistryAdmission { ledger: identity, admission: Some(admission), loans: 1 };
            return Ok((slot, local));
        }
        if self.first.is_none() {
            let admission = ViewLedgerAdmission::shared(allocations, counters, budget)?;
            let local = admission.alias();
            self.first = Some(LedgerRegistryAdmission { ledger: identity, admission: Some(admission), loans: 1 });
            return Ok((0, local));
        }
        if self.more.len() == self.more.capacity() {
            let capacity = self.more.capacity().max(1).checked_mul(2)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
            let width = std::mem::size_of::<LedgerRegistryAdmission>();
            let copied = crate::WorkCounters {
                bytes_copied: self.more.len().checked_mul(width)
                    .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            counters.admit(&copied, &budget).map_err(super::MountSourceError::Work)?;
            let bytes = capacity.checked_mul(width)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            allocations.claim_bytes(bytes, 1, counters, budget).map_err(observer_allocation_error)?;
            let mut replacement = Vec::new();
            if replacement.try_reserve_exact(capacity).is_err() {
                allocations.release(bytes).map_err(observer_allocation_error)?;
                return Err(super::MountSourceError::Engine("view admission allocation failed".into()));
            }
            let old_bytes = (self.more.capacity() * width) as u64;
            replacement.append(&mut self.more);
            self.more = replacement;
            counters.charge(&copied, &budget).map_err(super::MountSourceError::Work)?;
            allocations.release(old_bytes).map_err(observer_allocation_error)?;
        }
        let admission = ViewLedgerAdmission::shared(allocations, counters, budget)?;
        let local = admission.alias();
        let slot = self.more.len() + 1;
        self.more.push(LedgerRegistryAdmission { ledger: identity, admission: Some(admission), loans: 1 });
        Ok((slot, local))
    }

    pub(crate) fn restore(&mut self, slot: usize, admission: ViewLedgerAdmission) {
        let entry = if slot == 0 { self.first.as_mut().expect("first original root") }
            else { self.more.get_mut(slot - 1).expect("original root slot") };
        let (Some(ViewLedgerAdmission::Shared(original)), ViewLedgerAdmission::Shared(local))
            = (&entry.admission, &admission) else { unreachable!("registered map custody"); };
        assert!(Arc::ptr_eq(original, local), "original map custody is restored");
        drop(admission);
        entry.loans = entry.loans.checked_sub(1).expect("original map proof loan");
    }
}

impl ViewLedger {
    pub(super) fn take_for_work(
        self: &Arc<Self>, work: &mut crate::demand::DemandWork,
    ) -> Result<(usize, ViewLedgerAdmission), super::MountSourceError> {
        work.check().map_err(super::lazy::mount_demand_error)?;
        let (roots, allocations, counters, budget) =
            work.view_ledger_admission_parts().map_err(super::lazy::mount_demand_error)?;
        roots.take(self, allocations, counters, budget)
    }

    pub(super) fn new() -> Self {
        Self {
            latest: AtomicU64::new(0),
            everything: AtomicU64::new(0),
            unconfirmed: AtomicU64::new(0),
            bindings: ChangeMap::default(),
            directories: ChangeMap::default(),
            nodes: ChangeMap::default(),
            observers: ViewObservers::default(),
            _native_allocation: None,
        }
    }

    pub(super) fn new_with_native_work(
        original: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, super::MountSourceError> {
        let allocation = original.claim_allocation(std::mem::size_of::<Self>() as u64, 1, crate::WorkCounters::UNCHARGED)
            .map_err(super::MountSourceError::NativeWork)?;
        let mut value = Self::new();
        value._native_allocation = Some(allocation);
        Ok(Arc::new(value))
    }

    pub(super) fn prepare_everything_notification(
        self: &Arc<Self>, original: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<PreparedEverythingNotification, super::MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        let mut work = original.lock();
        work.check().map_err(mount_demand_error)?;
        let mut hub = self.observers.0.lock().unwrap_or_else(PoisonError::into_inner);
        if hub.pending.is_some() { return Err(super::MountSourceError::Stale); }
        let id = hub.next_notification.checked_add(1)
            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
        let mut deliveries = PendingDeliveryBuffer::default();
        let prepared = (|| {
            let mut live = 0_usize;
            for slot in &hub.observers {
                mount_items(&mut work, 1)?;
                if slot.observer.strong_count() != 0 {
                    live = live.checked_add(1)
                        .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
                }
            }
            deliveries.reserve(live, &mut work)?;
            for slot in &hub.observers {
                mount_items(&mut work, 1)?;
                if slot.observer.strong_count() == 0 { continue; }
                let copied = crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<PendingObserverDelivery>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                };
                work.admit(copied).map_err(mount_demand_error)?;
                work.reserve_pending_items(2).map_err(mount_demand_error)?;
                deliveries.entries.push(PendingObserverDelivery {
                    observer: PendingObserverState::Reserved(Weak::clone(&slot.observer)),
                    sync: Arc::clone(original), credits: 2,
                });
                work.record(copied).map_err(mount_demand_error)?;
            }
            Ok::<_, super::MountSourceError>(())
        })();
        if let Err(error) = prepared {
            deliveries.cancel_with_work(&mut work);
            drop(deliveries);
            return Err(error);
        }
        hub.next_notification = id;
        hub.pending = Some(PendingViewNotification {
            id, deliveries, sync: Arc::downgrade(original),
        });
        Ok(PreparedEverythingNotification { ledger: Arc::clone(self), id })
    }

    /// Finishes only an already accepted original seal. All buffers and visits
    /// were admitted before the seal or before a later subscription joined it.
    pub(super) fn record_everything_prepared(
        &self, mut prepared: PreparedEverythingNotification,
    ) {
        assert!(std::ptr::eq(self, Arc::as_ptr(&prepared.ledger)), "original notification ledger");
        let (mut pending, position) = {
            let mut hub = self.observers.0.lock().unwrap_or_else(PoisonError::into_inner);
            let mut pending = hub.pending.take().expect("original prepared publication");
            assert_eq!(pending.id, prepared.id, "original publication notification");
            for delivery in &mut pending.deliveries.entries {
                let ready = match &delivery.observer {
                    PendingObserverState::Reserved(observer) => observer.upgrade(),
                    _ => unreachable!("only weak reservations survive the SDK await"),
                };
                delivery.observer = match ready {
                    Some(observer) => PendingObserverState::Ready(observer),
                    None => PendingObserverState::Dead,
                };
            }
            let position = ViewStamp::next();
            position.record_in(&self.everything);
            position.record_in(&self.latest);
            prepared.id = 0;
            (pending, position)
        };
        let origin = ViewOrigin::current();
        for delivery in &mut pending.deliveries.entries {
            delivery.visit();
            if let PendingObserverState::Ready(observer) =
                std::mem::replace(&mut delivery.observer, PendingObserverState::Dead)
            {
                delivery.visit();
                observer.view_changed(position, origin);
            }
        }
        drop(pending);
    }

    /// Tells `observer` of every change recorded from now on.
    pub(super) fn observe(&self, observer: Weak<dyn ViewObserver>) -> Result<(), super::MountSourceError> {
        self.observers.add(observer)
    }

    pub(super) fn observe_measured_with_meter(
        self: &Arc<Self>, observer: Weak<dyn ViewObserver>,
        custody: Option<super::NativeMountAllocationCustody>,
        original: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), super::MountSourceError> {
        use super::lazy::mount_demand_error;
        loop {
            // Future visits belong to the publication, never this borrowed
            // registration. Reserve without either registry or work lock.
            let mut join = self.observers.prepare_join()?;
            let result = {
                let mut work = original.lock();
                work.check().map_err(mount_demand_error)?;
                let mut hub = self.observers.0.lock().unwrap_or_else(PoisonError::into_inner);
                if !join_matches(&hub, join.as_ref()) {
                    None
                } else {
                    let mut copy_credit = prepare_join_buffer(&mut hub, &mut work)?;
                    let admitted = (|| {
                        let (admissions, allocations, counters, budget) =
                            work.view_observer_admission_parts().map_err(mount_demand_error)?;
                        let entry = admissions.entry_mut(self, allocations, counters, budget)?;
                        self.observers.add_measured_locked(
                            &mut hub.observers, observer.clone(), custody.clone(),
                            &mut entry.registry, allocations, counters, budget,
                        )?;
                        append_join(&mut hub, &observer, &mut join, &mut work, &mut copy_credit)
                    })();
                    if copy_credit != 0 { work.cancel_reserved_copy(copy_credit); }
                    Some(admitted)
                }
            };
            drop(join);
            if let Some(result) = result { return result; }
            original.lock().charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(mount_demand_error)?;
        }
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
    pub(super) fn record_with_native_work(
        &self, change: &ViewChange<'_>, original: &crate::NativeMaterializationWork,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(), super::MountSourceError> {
        let mut work = original.clone();
        let (observers, position, result) = self.prepare_record_measured(change, &mut work, owned)?;
        self.notify_recorded(change, observers, position, result, &mut work)
    }

    /// Admits everything a node notification needs before its caller changes
    /// the body. Preparing may grow the canonical backing, but records no
    /// position, folds no history, and invokes no observer.
    pub(super) fn prepare_node_change_with_native_work(
        &self, file_id: FileId, original: &crate::NativeMaterializationWork,
    ) -> Result<PreparedNativeNodeChange<'_>, super::MountSourceError> {
        let mut work = original.clone();
        let observers = self.prepare_record_observers(&mut work, false)?;
        record_items(&mut work, 1)?;
        let slot = self.nodes.prepare_slot_measured(
            key_of(&file_id), &mut work, &mut ViewLedgerAdmission::default(), AdmittedMap::Nodes, true,
        )?;
        let items_examined = (observers.len() as u64).checked_add(slot.terminal_items() as u64)
            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
        let completion = original.reserve_work(crate::WorkCounters {
            items_examined,
            bytes_copied: slot.terminal_copied()?,
            ..crate::WorkCounters::UNCHARGED
        }).map_err(super::MountSourceError::NativeWork)?;
        Ok(PreparedNativeNodeChange { ledger: self, slot, observers, completion })
    }

    /// Finishes an accepted synchronous node change without quota or
    /// cancellation readmission. Even an unexpected accounting failure must
    /// not erase the real node change or skip its observers.
    pub(super) fn record_node_change_prepared(
        &self, prepared: PreparedNativeNodeChange<'_>,
    ) -> Result<(), super::MountSourceError> {
        let PreparedNativeNodeChange { ledger, slot, observers, mut completion } = prepared;
        assert!(std::ptr::eq(self, ledger), "original prepared node ledger");
        let terminal = crate::WorkCounters {
            items_examined: slot.terminal_items() as u64,
            bytes_copied: slot.terminal_copied().expect("accepted node copy width"),
            ..crate::WorkCounters::UNCHARGED
        };
        let position = ViewStamp::next();
        slot.record(position);
        // The slot guard retires before callbacks: observers may query nodes.
        position.record_in(&self.latest);
        let mut result = completion.record_completed(terminal)
            .map_err(super::MountSourceError::NativeWork);
        let notified = notify_recorded_observers(false, &observers, position, || {
            let visited = completion.record_completed(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(super::MountSourceError::NativeWork);
            if result.is_ok() { result = visited; }
        });
        drop(observers);
        drop(completion);
        if let Err(panic) = notified { std::panic::resume_unwind(panic); }
        result
    }

    fn notify_recorded<W: ViewRecordAccounting>(
        &self, change: &ViewChange<'_>, observers: ViewRecordBuffer<Arc<dyn ViewObserver>>,
        position: ViewStamp, result: Result<(), super::MountSourceError>, work: &mut W,
    ) -> Result<(), super::MountSourceError> {
        let notified = notify_recorded_observers(
            matches!(change, ViewChange::Unconfirmed), &observers, position, || {},
        );
        let released = observers.retire(work);
        if let Err(panic) = notified { std::panic::resume_unwind(panic); }
        result.and(released)
    }

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
        self.record_with_carrier(
            change, &mut super::adapter::NativeDemandCarrier::Borrowed(work), owned,
        )
    }

    pub(super) fn record_with_carrier(
        &self,
        change: &ViewChange<'_>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(), super::MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            return self.record_with_native_work(change, &original, owned);
        }
        let (observers, position, result) =
            carrier.with_work(|work| self.prepare_record_measured(change, work, owned))?;
        let notified = notify_recorded_observers(
            matches!(change, ViewChange::Unconfirmed), &observers, position, || {},
        );
        let bytes = observers.retire_physical();
        let released = carrier.with_work(|work|
            work.release(bytes).map_err(super::lazy::mount_demand_error));
        if let Err(panic) = notified { std::panic::resume_unwind(panic); }
        result.and(released)
    }

    fn prepare_record_observers<W: ViewRecordAccounting>(
        &self, work: &mut W, include_delivery_visits: bool,
    ) -> Result<ViewRecordBuffer<Arc<dyn ViewObserver>>, super::MountSourceError> {
        loop {
            let live = {
                let entries = self.observers.0.lock().unwrap_or_else(PoisonError::into_inner);
                let mut live = 0_usize;
                for observer in entries.observers.iter() {
                    record_items(work, 1)?;
                    if observer.observer.strong_count() != 0 {
                        live = live.checked_add(1)
                            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
                    }
                }
                live
            };
            // Scalar admission and allocation hold neither the hub nor an SDK
            // lock. Registration may grow before the collection reacquires it.
            let mut observers = record_buffer::<Arc<dyn ViewObserver>, _>(live, work)?;
            let collected = {
                let entries = self.observers.0.lock().unwrap_or_else(PoisonError::into_inner);
                (|| {
                    for observer in entries.observers.iter() {
                        record_items(work, 1)?;
                        if let Some(observer) = observer.observer.upgrade() {
                            if observers.len() == observers.capacity() { return Ok(false); }
                            if !include_delivery_visits {
                                work.record_charge(crate::WorkCounters {
                                    bytes_copied: std::mem::size_of::<Arc<dyn ViewObserver>>() as u64,
                                    ..crate::WorkCounters::UNCHARGED
                                })?;
                            }
                            observers.push(observer);
                        }
                    }
                    if include_delivery_visits { record_items(work, observers.len())?; }
                    Ok(true)
                })()
            };
            match collected {
                Ok(true) => return Ok(observers),
                Ok(false) => observers.retire(work)?,
                Err(error) => {
                    let _ = observers.retire(work);
                    return Err(error);
                }
            }
        }
    }

    fn prepare_record_measured<W: ViewRecordAccounting>(
        &self,
        change: &ViewChange<'_>,
        work: &mut W,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(
        ViewRecordBuffer<Arc<dyn ViewObserver>>, ViewStamp, Result<(), super::MountSourceError>,
    ), super::MountSourceError> {
        if let Some(ordinary) = work.ordinary() { owned.reconcile(self, ordinary)?; }
        let observers = self.prepare_record_observers(work, true)?;
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
                        self.bindings.record_measured(child, position, work, owned, AdmittedMap::Bindings)?;
                        self.directories.record_measured(directory, position, work, owned, AdmittedMap::Directories)?;
                    }
                    for subtree in &effect.subtrees { self.rebound_measured(subtree, position, work, owned)?; }
                    for directory in &effect.listings {
                        let key = path_key_measured(directory, work)?;
                        self.directories.record_measured(key, position, work, owned, AdmittedMap::Directories)?;
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
        Ok((observers, position, result))
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

    pub(super) fn binding_unchanged_since_measured(
        &self,
        path: &NamespacePath,
        stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, super::MountSourceError> {
        work.check().map_err(super::lazy::mount_demand_error)?;
        if !stamp.precedes_none_of(&self.unconfirmed)
            || !stamp.precedes_none_of(&self.everything) { return Ok(false); }
        let mut key = key_of(&());
        let bindings = self.bindings.0.read().unwrap_or_else(PoisonError::into_inner);
        if !bindings.unchanged_key_measured(key, stamp, work)? { return Ok(false); }
        for name in path.components() {
            key = child_key_measured(key, name, work)?;
            if !bindings.unchanged_key_measured(key, stamp, work)? { return Ok(false); }
        }
        Ok(true)
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

    /// Queries the same identity history without allocating or adopting the
    /// borrowed change map; every original probe spends the caller's work.
    pub(super) fn node_unchanged_since_measured(
        &self,
        file_id: FileId,
        stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, super::MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        work.check().map_err(mount_demand_error)?;
        mount_items(work, 1)?;
        if !stamp.precedes_none_of(&self.unconfirmed) { return Ok(false); }
        mount_items(work, 1)?;
        if !stamp.precedes_none_of(&self.everything) { return Ok(false); }
        mount_items(work, 1)?;
        self.nodes.unchanged_key_measured(key_of(&file_id), stamp, work)
    }

    /// Builds the actual terminal listing key through the same measured
    /// namespace engine, then reads only that directory's original history.
    pub(super) fn listing_unchanged_since_measured(
        &self,
        path: &NamespacePath,
        stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, super::MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        work.check().map_err(mount_demand_error)?;
        mount_items(work, 1)?;
        let directory = path_key_measured(path, work)?;
        mount_items(work, 1)?;
        if !stamp.precedes_none_of(&self.unconfirmed) { return Ok(false); }
        mount_items(work, 1)?;
        if !stamp.precedes_none_of(&self.everything) { return Ok(false); }
        self.directories.unchanged_key_measured(directory, stamp, work)
    }

    /// A name's binding changed: every lookup through it, and the listing
    /// and attributes of the directory holding it, are stale.
    fn rebound_measured<W: ViewRecordAccounting>(
        &self,
        path: &NamespacePath,
        position: ViewStamp,
        work: &mut W,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(), super::MountSourceError> {
        let mut parent = None;
        let mut terminal = key_of(&());
        for name in path.components() {
            parent = Some(terminal);
            terminal = child_key_measured(terminal, name, work)?;
        }
        self.bindings.record_measured(terminal, position, work, owned, AdmittedMap::Bindings)?;
        if let Some(parent) = parent {
            self.directories.record_measured(parent, position, work, owned, AdmittedMap::Directories)?;
        }
        Ok(())
    }

    fn node_changed_measured<W: ViewRecordAccounting>(
        &self,
        id: FileId,
        position: ViewStamp,
        work: &mut W,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(), super::MountSourceError> {
        record_items(work, 1)?;
        self.nodes.record_measured(key_of(&id), position, work, owned, AdmittedMap::Nodes)
    }
}

/// The latest change recorded against each key. A key that was never
/// recorded, or whose record was folded away, reads as the floor.
#[derive(Default)]
struct ChangeMap(RwLock<ChangePositions>, AtomicU64);

#[derive(Default)]
struct ChangePositions {
    floor: u64,
    slots: Vec<Option<(u64, u64)>>,
    length: usize,
    allocation_generation: u64,
    // The physical map backing drops before this original allocation guard.
    native_allocation: Option<crate::NativeAllocation>,
}

/// A slot in the same canonical map, held stable across a synchronous edit.
struct PreparedChangeMapSlot<'a> {
    positions: std::sync::RwLockWriteGuard<'a, ChangePositions>,
    index: usize,
    key: u64,
    previous: Option<u64>,
    fold: bool,
}

impl PreparedChangeMapSlot<'_> {
    fn terminal_items(&self) -> usize {
        if self.fold { self.positions.slots.len() } else { 0 }
    }

    fn terminal_copied(&self) -> Result<u64, super::MountSourceError> {
        let stores = self.terminal_items().checked_add(1)
            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
        stores.checked_mul(std::mem::size_of::<Option<(u64, u64)>>())
            .map(|bytes| bytes as u64)
            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))
    }

    fn record(self, position: ViewStamp) {
        let Self { mut positions, index, key, previous, fold } = self;
        if fold {
            positions.floor = positions.slots.iter().flatten()
                .fold(positions.floor, |floor, (_, at)| floor.max(*at));
            positions.slots.fill(None);
            positions.length = 0;
        }
        positions.slots[index] = Some((key, previous.unwrap_or(0).max(position.0)));
        if previous.is_none() { positions.length += 1; }
    }
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

fn notify_recorded_observers(
    unconfirmed: bool, observers: &[Arc<dyn ViewObserver>], position: ViewStamp,
    mut visit: impl FnMut(),
) -> std::thread::Result<()> {
    let origin = ViewOrigin::current();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for observer in observers {
            visit();
            if unconfirmed {
                observer.view_unconfirmed(position);
            } else {
                observer.view_changed(position, origin);
            }
        }
    }))
}

/// Scalar accounting for the one canonical record/key engine. Native calls
/// borrow their original root only at admission and completion boundaries.
trait ViewRecordAccounting {
    fn ordinary(&mut self) -> Option<&mut crate::demand::DemandWork>;
    fn record_charge(&mut self, delta: crate::WorkCounters) -> Result<(), super::MountSourceError>;
    fn record_claim(&mut self, bytes: u64, operations: u64) -> Result<ViewRecordAllocation, super::MountSourceError>;
    fn record_capacity(&mut self, allocation: &mut ViewRecordAllocation, actual: u64) -> Result<(), super::MountSourceError>;
    fn release_numeric(&mut self, bytes: u64) -> Result<(), super::MountSourceError>;
}

struct ViewRecordAllocation {
    numeric: u64,
    native: Option<crate::NativeAllocation>,
}

impl ViewRecordAccounting for crate::demand::DemandWork {
    fn ordinary(&mut self) -> Option<&mut crate::demand::DemandWork> { Some(self) }
    fn record_charge(&mut self, delta: crate::WorkCounters) -> Result<(), super::MountSourceError> {
        self.charge(delta).map_err(super::lazy::mount_demand_error)
    }
    fn record_claim(&mut self, bytes: u64, operations: u64) -> Result<ViewRecordAllocation, super::MountSourceError> {
        self.claim_allocation(bytes, operations).map_err(super::lazy::mount_demand_error)?;
        Ok(ViewRecordAllocation { numeric: bytes, native: None })
    }
    fn record_capacity(&mut self, allocation: &mut ViewRecordAllocation, actual: u64) -> Result<(), super::MountSourceError> {
        self.reconcile_native_allocation(&mut allocation.numeric, actual)
            .map_err(super::lazy::mount_demand_error)
    }
    fn release_numeric(&mut self, bytes: u64) -> Result<(), super::MountSourceError> {
        self.release(bytes).map_err(super::lazy::mount_demand_error)
    }
}

impl ViewRecordAccounting for crate::NativeMaterializationWork {
    fn ordinary(&mut self) -> Option<&mut crate::demand::DemandWork> { None }
    fn record_charge(&mut self, delta: crate::WorkCounters) -> Result<(), super::MountSourceError> {
        self.charge(delta).map_err(super::MountSourceError::NativeWork)
    }
    fn record_claim(&mut self, bytes: u64, operations: u64) -> Result<ViewRecordAllocation, super::MountSourceError> {
        let native = self.claim_allocation(bytes, operations, crate::WorkCounters::UNCHARGED).map_err(super::MountSourceError::NativeWork)?;
        Ok(ViewRecordAllocation { numeric: 0, native: Some(native) })
    }
    fn record_capacity(&mut self, allocation: &mut ViewRecordAllocation, actual: u64) -> Result<(), super::MountSourceError> {
        allocation.native.as_mut().ok_or(super::MountSourceError::Stale)?
            .reconcile_capacity(actual).map_err(super::MountSourceError::NativeWork)
    }
    fn release_numeric(&mut self, bytes: u64) -> Result<(), super::MountSourceError> {
        if bytes != 0 { return Err(super::MountSourceError::Stale); }
        Ok(())
    }
}

struct ViewRecordBuffer<T> {
    values: Vec<T>,
    allocation: ViewRecordAllocation,
}

impl<T> std::ops::Deref for ViewRecordBuffer<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target { &self.values }
}
impl<T> std::ops::DerefMut for ViewRecordBuffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.values }
}
impl<T> ViewRecordBuffer<T> {
    fn retire_physical(self) -> u64 {
        let Self { values, allocation } = self;
        drop(values);
        let bytes = allocation.numeric;
        drop(allocation);
        bytes
    }
    fn retire(self, work: &mut impl ViewRecordAccounting) -> Result<(), super::MountSourceError> {
        work.release_numeric(self.retire_physical())
    }
}

fn record_items(work: &mut impl ViewRecordAccounting, count: usize) -> Result<(), super::MountSourceError> {
    work.record_charge(crate::WorkCounters { items_examined: count as u64, ..crate::WorkCounters::UNCHARGED })
}

fn record_buffer<T, W: ViewRecordAccounting>(
    count: usize, work: &mut W,
) -> Result<ViewRecordBuffer<T>, super::MountSourceError> {
    let bytes = count.checked_mul(std::mem::size_of::<T>())
        .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64;
    let mut allocation = work.record_claim(bytes, u64::from(bytes != 0))?;
    let mut values = Vec::new();
    if let Err(error) = values.try_reserve_exact(count) {
        drop(values);
        let _ = work.release_numeric(allocation.numeric);
        return Err(super::MountSourceError::Allocation(error));
    }
    let actual = (values.capacity() * std::mem::size_of::<T>()) as u64;
    if let Err(error) = work.record_capacity(&mut allocation, actual) {
        drop(values);
        let _ = work.release_numeric(allocation.numeric);
        return Err(error);
    }
    Ok(ViewRecordBuffer { values, allocation })
}

impl ChangeMap {
    fn record_measured<W: ViewRecordAccounting>(
        &self, key: u64, position: ViewStamp, work: &mut W,
        owned: &mut ViewLedgerAdmission, kind: AdmittedMap,
    ) -> Result<(), super::MountSourceError> {
        self.prepare_slot_measured(key, work, owned, kind, false)?.record(position);
        Ok(())
    }

    fn prepare_slot_measured<W: ViewRecordAccounting>(
        &self, key: u64, work: &mut W,
        owned: &mut ViewLedgerAdmission, kind: AdmittedMap, defer_fold: bool,
    ) -> Result<PreparedChangeMapSlot<'_>, super::MountSourceError> {
        use super::lazy::mount_demand_error;
        loop {
            let mut positions = self.0.write().unwrap_or_else(PoisonError::into_inner);
            if let Some(ordinary) = work.ordinary() {
                if let Some((generation, _)) = owned.claim(kind)
                    && generation != positions.allocation_generation
                    && let Some((_, bytes)) = owned.replace(kind, None)
                {
                    ordinary.release(bytes).map_err(mount_demand_error)?;
                }
            }
            if !positions.slots.is_empty() {
                let mask = positions.slots.len() - 1;
                let mut index = key as usize & mask;
                loop {
                    record_items(work, 1)?;
                    match positions.slots.get(index).copied().flatten() {
                        Some((stored, at)) if stored == key => {
                            return Ok(PreparedChangeMapSlot {
                                positions, index, key, previous: Some(at), fold: false,
                            });
                        }
                        Some(_) => index = (index + 1) & mask,
                        None => {
                            if positions.length < MAXIMUM_TRACKED_KEYS
                                && (positions.length + 1) * 2 <= positions.slots.len()
                            {
                                return Ok(PreparedChangeMapSlot {
                                    positions, index, key, previous: None, fold: false,
                                });
                            }
                            break;
                        }
                    }
                }
            }
            if positions.length == MAXIMUM_TRACKED_KEYS {
                if defer_fold {
                    let index = key as usize & (positions.slots.len() - 1);
                    return Ok(PreparedChangeMapSlot {
                        positions, index, key, previous: None, fold: true,
                    });
                }
                record_items(work, positions.slots.len())?;
                positions.floor = positions.slots.iter().flatten()
                    .fold(positions.floor, |floor, (_, at)| floor.max(*at));
                positions.slots.fill(None);
                positions.length = 0;
                continue;
            }
            let old_bytes = (positions.slots.capacity() * std::mem::size_of::<Option<(u64, u64)>>()) as u64;
            let old_generation = positions.allocation_generation;
            let next_generation = old_generation.checked_add(1)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
            let count = positions.slots.len().max(4).saturating_mul(2).min(MAXIMUM_TRACKED_KEYS * 2);
            // Ordinary receipts retain their existing borrowed-map admission.
            // Native reads never adopt a preexisting map: its physical guard
            // remains with that exact backing until the replacement drops it.
            let borrowed_old = if let Some(ordinary) = work.ordinary() {
                if owned.claim(kind).is_none() {
                    ordinary.adopt(old_bytes).map_err(mount_demand_error)?;
                    true
                } else { false }
            } else { false };
            drop(positions);
            let mut replacement = match record_buffer::<Option<(u64, u64)>, _>(count, work) {
                Ok(replacement) => replacement,
                Err(error) => {
                    if borrowed_old { let _ = work.release_numeric(old_bytes); }
                    return Err(error);
                }
            };
            if defer_fold {
                work.record_charge(crate::WorkCounters {
                    bytes_copied: count.checked_mul(std::mem::size_of::<Option<(u64, u64)>>())
                        .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64,
                    ..crate::WorkCounters::UNCHARGED
                })?;
            }
            replacement.resize(count, None);
            let mut positions = self.0.write().unwrap_or_else(PoisonError::into_inner);
            if positions.allocation_generation != old_generation {
                drop(positions);
                replacement.retire(work)?;
                if borrowed_old { work.release_numeric(old_bytes)?; }
                continue;
            }
            let rehashed = (|| {
                for entry in &positions.slots {
                    record_items(work, 1)?;
                    if let Some((stored, at)) = entry {
                        let mut index = *stored as usize & (count - 1);
                        loop {
                            record_items(work, 1)?;
                            let slot = replacement.get_mut(index).ok_or(super::MountSourceError::Stale)?;
                            if slot.is_none() {
                                work.record_charge(crate::WorkCounters {
                                    bytes_copied: std::mem::size_of::<Option<(u64, u64)>>() as u64,
                                    ..crate::WorkCounters::UNCHARGED
                                })?;
                                *slot = Some((*stored, *at));
                                break;
                            }
                            index = (index + 1) & (count - 1);
                        }
                    }
                }
                Ok::<(), super::MountSourceError>(())
            })();
            if let Err(error) = rehashed {
                drop(positions);
                let _ = replacement.retire(work);
                if borrowed_old { let _ = work.release_numeric(old_bytes); }
                return Err(error);
            }
            let ViewRecordBuffer { values, mut allocation } = replacement;
            let old = std::mem::replace(&mut positions.slots, values);
            let old_guard = std::mem::replace(&mut positions.native_allocation, allocation.native.take());
            positions.allocation_generation = next_generation;
            self.1.store(next_generation, Ordering::Release);
            if work.ordinary().is_some() {
                let _ = owned.replace(kind, Some((next_generation, allocation.numeric)));
            }
            drop(positions);
            drop(old);
            drop(old_guard);
            if let Some(ordinary) = work.ordinary() {
                ordinary.release(old_bytes).map_err(mount_demand_error)?;
            }
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

fn path_key_measured<W: ViewRecordAccounting>(
    path: &NamespacePath,
    work: &mut W,
) -> Result<u64, super::MountSourceError> {
    let mut key = key_of(&());
    for name in path.components() { key = child_key_measured(key, name, work)?; }
    Ok(key)
}

fn child_key_measured<W: ViewRecordAccounting>(
    parent: u64,
    name: &LogicalName,
    work: &mut W,
) -> Result<u64, super::MountSourceError> {
    const TEXT: u8 = 1;
    const RAW: u8 = 2;
    record_items(work, 1)?;
    let mut hasher = DefaultHasher::new();
    hasher.write_u64(parent);
    let bytes = name.as_bytes();
    match name.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            if bytes.is_ascii() {
                record_items(work, bytes.len())?;
                hasher.write_u8(TEXT);
                hash_ascii_fold(bytes.iter().copied(), &mut hasher);
            } else if let Ok(text) = std::str::from_utf8(bytes) {
                hasher.write_u8(TEXT);
                hash_unicode_fold(text.chars(), &mut hasher, work)?;
            } else {
                record_items(work, bytes.len())?;
                hasher.write_u8(RAW);
                hasher.write(bytes);
            }
        }
        NameEncoding::WindowsUtf16Le => {
            let units = crate::kernel::types::utf16le_units(bytes).ok_or(super::MountSourceError::Stale)?;
            record_items(work, bytes.len() / 2)?;
            if units.clone().all(|unit| unit < 0x80) {
                hasher.write_u8(TEXT);
                hash_ascii_fold(units.map(|unit| unit.to_le_bytes()[0]), &mut hasher);
            } else {
                let mut valid = true;
                for decoded in char::decode_utf16(units.clone()) {
                    record_items(work, 1)?;
                    if decoded.is_err() { valid = false; break; }
                }
                if valid {
                    hasher.write_u8(TEXT);
                    hash_unicode_fold(char::decode_utf16(units).filter_map(Result::ok), &mut hasher, work)?;
                } else {
                    record_items(work, bytes.len())?;
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
fn hash_unicode_fold<I: Iterator<Item = char> + Clone, W: ViewRecordAccounting>(
    text: I,
    hasher: &mut DefaultHasher,
    work: &mut W,
) -> Result<(), super::MountSourceError> {
    use unicode_normalization::char::{canonical_combining_class, compose, decompose_canonical};
    let mut count = 0_usize;
    for scalar in text.clone() {
        record_items(work, 1)?;
        decompose_canonical(scalar, |_| count += 1);
    }
    let mut decomposed = record_buffer::<(u8, char), _>(count, work)?;
    let result = (|| {
    for scalar in text {
        let mut emitted = 0;
        decompose_canonical(scalar, |_| emitted += 1);
        record_items(work, emitted)?;
        decompose_canonical(scalar, |scalar| decomposed.push((canonical_combining_class(scalar), scalar)));
    }
    // Canonical ordering is stable within each combining sequence. In-place
    // insertion makes every comparison/move interruptible and uses no scratch.
    for index in 1..decomposed.len() {
        let value = *decomposed.get(index).ok_or(super::MountSourceError::Stale)?;
        if value.0 == 0 { continue; }
        let mut position = index;
        while position > 0 {
            record_items(work, 1)?;
            let prior = *decomposed.get(position - 1).ok_or(super::MountSourceError::Stale)?;
            if prior.0 <= value.0 { break; }
            work.record_charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<(u8, char)>() as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
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
        record_items(work, 1)?;
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
            record_items(work, 1)?;
            hasher.write(folded.encode_utf8(&mut encoded).as_bytes());
        }
    }
    Ok(())
    })();
    let retired = decomposed.retire(work);
    result.and(retired)
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

    #[test]
    fn native_node_preadmission_denial_and_early_cancellation_leave_body_untouched()
    -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Default)]
        struct Counted(std::sync::atomic::AtomicU64);
        impl ViewObserver for Counted {
            fn view_changed(&self, _: ViewStamp, _: ViewOrigin) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let observer_bytes = std::mem::size_of::<Arc<dyn ViewObserver>>() as u64;
        let slot_bytes = std::mem::size_of::<Option<(u64, u64)>>() as u64;
        let cases = [
            (crate::WorkBudget { items_examined: 0, ..crate::WorkBudget::UNBOUNDED }, Some("items_examined")),
            (crate::WorkBudget { bytes_copied: 0, ..crate::WorkBudget::UNBOUNDED }, Some("bytes_copied")),
            (crate::WorkBudget { allocation_operations: 0, ..crate::WorkBudget::UNBOUNDED }, Some("allocation_operations")),
            (crate::WorkBudget { peak_allocation_bytes: 0, ..crate::WorkBudget::UNBOUNDED }, Some("peak_allocation_bytes")),
            (crate::WorkBudget { bytes_copied: observer_bytes + slot_bytes - 1, ..crate::WorkBudget::UNBOUNDED }, Some("bytes_copied")),
            (crate::WorkBudget { items_examined: 4, ..crate::WorkBudget::UNBOUNDED }, Some("items_examined")),
            (crate::WorkBudget::UNBOUNDED, None),
        ];
        for (budget, counter) in cases {
            let ledger = ViewLedger::new();
            let file = FileId::from_bytes([112; 16]);
            ledger.record(&ViewChange::Node(file));
            let before = ledger.stamp();
            let counted = Arc::new(Counted::default());
            let observer: Arc<dyn ViewObserver> = counted.clone();
            ledger.observe(Arc::downgrade(&observer))?;
            let cancellation = crate::CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED, &cancellation,
            ).map_err(|(error, _)| error)?;
            let work = original.native_volume_work(budget)?;
            let baseline = original.live_allocation_bytes();
            if counter.is_none() { cancellation.cancel(); }
            let mut body = *b"old!";
            let changed = (|| {
                let prepared = ledger.prepare_node_change_with_native_work(file, &work)?;
                body.copy_from_slice(b"new!");
                ledger.record_node_change_prepared(prepared)
            })();
            let error = changed.err().ok_or("denied node preparation accepted")?;
            match counter {
                Some(expected) => assert!(matches!(error,
                    super::super::MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::Work(
                            crate::WorkError::BudgetExceeded { counter, .. },
                        ),
                    ) if counter == expected)),
                None => assert!(matches!(error,
                    super::super::MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::Cancelled,
                    ))),
            }
            assert_eq!(&body, b"old!");
            assert_eq!(ledger.stamp(), before);
            assert!(ledger.node_unchanged_since(file, before));
            assert_eq!(counted.0.load(std::sync::atomic::Ordering::Relaxed), 0);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        Ok(())
    }

    #[test]
    fn accepted_native_node_credit_survives_alias_contention_and_late_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        struct Observed {
            ledger: std::sync::Weak<ViewLedger>,
            file: FileId,
            before: ViewStamp,
            origin: ViewOrigin,
            body: Arc<std::sync::atomic::AtomicU64>,
            calls: std::sync::atomic::AtomicU64,
        }
        impl ViewObserver for Observed {
            fn view_changed(&self, position: ViewStamp, origin: ViewOrigin) {
                let ledger = self.ledger.upgrade().expect("live source");
                assert_eq!(self.body.load(std::sync::atomic::Ordering::Acquire), 42);
                assert_eq!(origin, self.origin);
                assert_eq!(position, ledger.stamp());
                assert!(!ledger.node_unchanged_since(self.file, self.before),
                    "the node lock must retire before a reentrant observer query");
                self.calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let controller = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).map_err(|(error, _)| error)?.receipt();
        let observer_bytes = std::mem::size_of::<Arc<dyn ViewObserver>>() as u64;
        let slot_bytes = std::mem::size_of::<Option<(u64, u64)>>() as u64;
        let local_budget = crate::WorkBudget {
            items_examined: 5,
            bytes_copied: observer_bytes + slot_bytes,
            allocation_operations: 1,
            peak_allocation_bytes: observer_bytes,
            ..crate::WorkBudget::UNBOUNDED
        };
        for scoped in [false, true] {
            let ledger = Arc::new(ViewLedger::new());
            let file = FileId::from_bytes([113; 16]);
            ledger.record(&ViewChange::Node(file));
            let before = ledger.stamp();
            let origin = ViewOrigin::new();
            let body = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let observed = Arc::new(Observed {
                ledger: Arc::downgrade(&ledger), file, before, origin, body: body.clone(),
                calls: std::sync::atomic::AtomicU64::new(0),
            });
            let observer: Arc<dyn ViewObserver> = observed.clone();
            ledger.observe(Arc::downgrade(&observer))?;
            let cancellation = crate::CancellationToken::new();
            let root_budget = if scoped { crate::WorkBudget::UNBOUNDED } else {
                crate::WorkBudget {
                    items_examined: controller.items_examined + local_budget.items_examined,
                    bytes_copied: controller.bytes_copied + local_budget.bytes_copied,
                    allocation_operations: controller.allocation_operations + local_budget.allocation_operations,
                    peak_allocation_bytes: controller.peak_allocation_bytes + local_budget.peak_allocation_bytes,
                    ..crate::WorkBudget::UNBOUNDED
                }
            };
            let original = crate::NativeMaterializationWork::new(
                root_budget, &cancellation,
            ).map_err(|(error, _)| error)?;
            let work = if scoped { original.native_volume_work(local_budget)? } else { original.clone() };
            let baseline = original.live_allocation_bytes();
            let prepared = ledger.prepare_node_change_with_native_work(file, &work)?;
            let prefix = original.receipt();
            assert_eq!(ledger.stamp(), before);
            assert_eq!(observed.calls.load(std::sync::atomic::Ordering::Relaxed), 0);
            assert_eq!(work.remaining()?.items_examined, 0);
            assert_eq!(work.remaining()?.bytes_copied, 0);
            assert!(work.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).is_err(), "aliases cannot spend accepted callback credit");
            assert!(work.reserve_work(crate::WorkCounters {
                bytes_copied: 1, ..crate::WorkCounters::UNCHARGED
            }).is_err(), "aliases cannot spend accepted slot copy credit");
            body.store(42, std::sync::atomic::Ordering::Release);
            cancellation.cancel();
            {
                let _origin = origin.enter();
                ledger.record_node_change_prepared(prepared)?;
            }
            assert_eq!(observed.calls.load(std::sync::atomic::Ordering::Relaxed), 1);
            assert_eq!(original.receipt().items_examined, prefix.items_examined + 1);
            assert_eq!(original.receipt().bytes_copied, prefix.bytes_copied + slot_bytes);
            assert_eq!(original.live_allocation_bytes(), baseline);
            assert!(!ledger.node_unchanged_since(file, before));
        }
        Ok(())
    }

    #[test]
    fn aborting_native_node_permit_releases_only_physically_retired_buffers()
    -> Result<(), Box<dyn std::error::Error>> {
        struct Observer;
        impl ViewObserver for Observer {
            fn view_changed(&self, _: ViewStamp, _: ViewOrigin) { panic!("aborted node notified"); }
        }
        let ledger = ViewLedger::new();
        let observer: Arc<dyn ViewObserver> = Arc::new(Observer);
        let weak = Arc::downgrade(&observer);
        ledger.observe(weak.clone())?;
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let file = FileId::from_bytes([114; 16]);
        let before = ledger.stamp();
        let prepared = ledger.prepare_node_change_with_native_work(file, &original)?;
        let prefix = original.receipt();
        let map_bytes = (prepared.slot.positions.slots.capacity()
            * std::mem::size_of::<Option<(u64, u64)>>()) as u64;
        let observer_bytes = (prepared.observers.capacity()
            * std::mem::size_of::<Arc<dyn ViewObserver>>()) as u64;
        assert_eq!(original.live_allocation_bytes(), baseline + map_bytes + observer_bytes);
        drop(observer);
        assert!(weak.upgrade().is_some(), "prepared synchronous delivery owns its real observer");
        cancellation.cancel();
        let aborted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _prepared = prepared;
            panic!("body edit aborted before mutation");
        }));
        assert!(aborted.is_err());
        assert!(weak.upgrade().is_none());
        assert_eq!(ledger.stamp(), before);
        assert!(ledger.node_unchanged_since(file, before));
        assert_eq!(original.receipt(), prefix, "unused completion credit is not spent work");
        assert_eq!(original.live_allocation_bytes(), baseline + map_bytes,
            "the installed canonical backing remains physically live after abort");
        drop(ledger);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn panicking_native_node_observer_does_not_lose_node_or_backing_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        struct Observer;
        impl ViewObserver for Observer {
            fn view_changed(&self, _: ViewStamp, _: ViewOrigin) { panic!("observer failure"); }
        }
        let ledger = ViewLedger::new();
        let observer: Arc<dyn ViewObserver> = Arc::new(Observer);
        let weak = Arc::downgrade(&observer);
        ledger.observe(weak.clone())?;
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let file = FileId::from_bytes([115; 16]);
        let before = ledger.stamp();
        let prepared = ledger.prepare_node_change_with_native_work(file, &original)?;
        let prefix = original.receipt();
        let map_bytes = (prepared.slot.positions.slots.capacity()
            * std::mem::size_of::<Option<(u64, u64)>>()) as u64;
        drop(observer);
        cancellation.cancel();
        let notified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(||
            ledger.record_node_change_prepared(prepared)));
        assert!(notified.is_err());
        assert!(!ledger.node_unchanged_since(file, before));
        assert!(weak.upgrade().is_none());
        assert_eq!(original.receipt().items_examined, prefix.items_examined + 1);
        assert_eq!(original.live_allocation_bytes(), baseline + map_bytes);
        drop(ledger);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn prepared_native_node_serializes_concurrent_canonical_map_growth()
    -> Result<(), Box<dyn std::error::Error>> {
        let ledger = ViewLedger::new();
        let file = FileId::from_bytes([116; 16]);
        let before = ledger.stamp();
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let prepared = ledger.prepare_node_change_with_native_work(file, &original)?;
        assert!(ledger.nodes.0.try_write().is_err(), "the accepted physical slot is stable");
        std::thread::scope(|scope| -> Result<(), super::super::MountSourceError> {
            let (started, ready) = std::sync::mpsc::channel();
            let ledger = &ledger;
            let joined = scope.spawn(move || {
                started.send(()).expect("waiting recorder");
                for byte in 1..=20 {
                    ledger.record(&ViewChange::Node(FileId::from_bytes([byte; 16])));
                }
            });
            ready.recv().expect("concurrent recorder started");
            ledger.record_node_change_prepared(prepared)?;
            joined.join().expect("canonical recorder succeeds");
            Ok(())
        })?;
        assert!(!ledger.node_unchanged_since(file, before));
        for byte in 1..=20 {
            assert!(!ledger.node_unchanged_since(FileId::from_bytes([byte; 16]), before));
        }
        assert_eq!(ledger.nodes.0.read().expect("canonical map").length, 21);
        assert_eq!(original.live_allocation_bytes(), baseline,
            "ordinary growth physically retires the exact native backing");
        Ok(())
    }

    #[test]
    fn native_node_fold_is_deferred_and_uses_accepted_real_scan_and_copy_credit()
    -> Result<(), Box<dyn std::error::Error>> {
        let ledger = ViewLedger::new();
        let mut seed = super::unbounded_view_work();
        let mut owned = ViewLedgerAdmission::default();
        for key in 0..MAXIMUM_TRACKED_KEYS as u64 {
            let position = ViewStamp::next();
            ledger.nodes.record_measured(key, position, &mut seed, &mut owned, super::AdmittedMap::Nodes)?;
            position.record_in(&ledger.latest);
        }
        let count = MAXIMUM_TRACKED_KEYS * 2;
        let file = (0..=u8::MAX).map(|byte| FileId::from_bytes([byte; 16]))
            .find(|file| super::key_of(file) as usize & (count - 1) >= MAXIMUM_TRACKED_KEYS)
            .ok_or("no unused direct probe for the full canonical map")?;
        let before = ledger.stamp();
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(error, _)| error)?;
        let aborted = ledger.prepare_node_change_with_native_work(file, &original)?;
        assert!(aborted.slot.fold);
        assert_eq!(aborted.slot.positions.floor, 0);
        assert_eq!(aborted.slot.positions.length, MAXIMUM_TRACKED_KEYS);
        drop(aborted);
        assert_eq!(ledger.nodes.0.read().expect("unfolded map").floor, 0);
        assert_eq!(ledger.stamp(), before);
        let work = original.native_volume_work(crate::WorkBudget {
            items_examined: count as u64 + 2,
            bytes_copied: ((count + 1) * std::mem::size_of::<Option<(u64, u64)>>()) as u64,
            allocation_operations: 0,
            peak_allocation_bytes: 0,
            ..crate::WorkBudget::UNBOUNDED
        })?;
        let baseline = original.live_allocation_bytes();
        let prepared = ledger.prepare_node_change_with_native_work(file, &work)?;
        let prefix = original.receipt();
        assert_eq!(work.remaining()?.items_examined, 0);
        assert_eq!(work.remaining()?.bytes_copied, 0);
        assert_eq!(prepared.slot.positions.floor, 0);
        cancellation.cancel();
        ledger.record_node_change_prepared(prepared)?;
        let positions = ledger.nodes.0.read().expect("terminal folded map");
        assert_eq!(positions.length, 1);
        assert_eq!(positions.floor, before.0);
        assert_eq!(positions.get(super::key_of(&file)), Some(ledger.stamp().0));
        assert_eq!(original.receipt().items_examined, prefix.items_examined + count as u64);
        assert_eq!(original.receipt().bytes_copied, prefix.bytes_copied
            + ((count + 1) * std::mem::size_of::<Option<(u64, u64)>>()) as u64);
        assert_eq!(original.live_allocation_bytes(), baseline,
            "a borrowed canonical map is not adopted into native allocation custody");
        Ok(())
    }

    #[test]
    fn prepared_publication_uses_final_liveness_and_releases_borrowed_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Default)]
        struct Counted(std::sync::atomic::AtomicU64);
        impl ViewObserver for Counted {
            fn view_changed(&self, _: ViewStamp, _: ViewOrigin) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let ledger = Arc::new(ViewLedger::new());
        let early: Arc<dyn ViewObserver> = Arc::new(Counted::default());
        let dead = Arc::downgrade(&early);
        ledger.observe(dead.clone())?;
        let cancellation = crate::CancellationToken::new();
        let sync = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation),
        )));
        let prepared = ledger.prepare_everything_notification(&sync)?;
        drop(early);
        assert!(dead.upgrade().is_none(), "the SDK await retains only weak subscriptions");
        let counted = Arc::new(Counted::default());
        let late: Arc<dyn ViewObserver> = counted.clone();
        let mut registration = crate::demand::DemandWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        );
        super::super::adapter::BorrowedNativeDemandMeter::new(&mut registration)?.run(|meter| {
            ledger.observe_measured_with_meter(Arc::downgrade(&late), None, meter)
        })?;
        let registration_prefix = registration.work;
        let sync_prefix = sync.lock().work.items_examined;
        ledger.record_everything_prepared(prepared);
        assert_eq!(counted.0.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(registration.work, registration_prefix, "future visits belong to the original seal");
        assert_eq!(sync.lock().work.items_examined, sync_prefix + 3);
        assert!(ledger.observers.0.lock().expect("same hub").pending.is_none());
        assert_eq!(Arc::strong_count(&sync), 1, "every original delivery alias retired before loan restoration");
        Ok(())
    }

    #[test]
    fn denied_late_delivery_does_not_join_or_erase_original_publication()
    -> Result<(), Box<dyn std::error::Error>> {
        struct Observer;
        impl ViewObserver for Observer {
            fn view_changed(&self, _: ViewStamp, _: ViewOrigin) {}
        }
        let ledger = Arc::new(ViewLedger::new());
        let mut budget = crate::WorkBudget::UNBOUNDED;
        budget.items_examined = 0;
        let sync = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(budget, &crate::CancellationToken::new()),
        )));
        let prepared = ledger.prepare_everything_notification(&sync)?;
        let observer: Arc<dyn ViewObserver> = Arc::new(Observer);
        let registration = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )));
        assert!(ledger.observe_measured_with_meter(
            Arc::downgrade(&observer), None, &registration,
        ).is_err());
        let ordinary_error = ledger.observe(Arc::downgrade(&observer))
            .expect_err("ordinary registration must propagate the same original publication quota");
        assert!(matches!(ordinary_error.semantic_error(), super::super::MountSourceError::Work(_)));
        assert!(ledger.observers.0.lock().expect("same registry").observers.is_empty());
        let stamp = ledger.stamp();
        ledger.record_everything_prepared(prepared);
        assert_ne!(ledger.stamp(), stamp, "an accepted original publication still records its actual event");
        assert_eq!(sync.lock().work.items_examined, 0);
        Ok(())
    }

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
    fn shared_map_root_reuses_original_proof_for_repeated_source_loans()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let ledger = Arc::new(ViewLedger::new());
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let (slot, mut first) = {
            let (roots, allocations, counters, budget) = work.view_ledger_admission_parts()?;
            roots.take(&ledger, allocations, counters, budget)?
        };
        let named = path(&["original"])?;
        let effect = ViewEffect {
            nodes: vec![FileId::from_bytes([101; 16])],
            names: vec![(path(&[])?, named.components()[0].clone())],
            listings: vec![path(&[])?],
            ..ViewEffect::default()
        };
        let change = ViewChange::Effect(&effect);
        ledger.record_measured(&change, &mut work, &mut first)?;
        let live = work.live_allocation_bytes();
        let allocated = work.work.allocation_operations;
        let (same_slot, mut second) = {
            let (roots, allocations, counters, budget) = work.view_ledger_admission_parts()?;
            roots.take(&ledger, allocations, counters, budget)?
        };
        assert_eq!(slot, same_slot);
        let (ViewLedgerAdmission::Shared(left), ViewLedgerAdmission::Shared(right)) = (&first, &second)
            else { panic!("canonical shared three-map proof"); };
        assert!(Arc::ptr_eq(left, right));
        let maps = left.lock().expect("original three-map proof");
        assert!(maps.bindings.is_some() && maps.directories.is_some() && maps.nodes.is_some());
        drop(maps);
        ledger.record_measured(&change, &mut work, &mut second)?;
        assert_eq!(work.work.allocation_operations, allocated);
        assert_eq!(work.live_allocation_bytes(), live);
        work.restore_view_ledger_admission(slot, first);
        work.restore_view_ledger_admission(same_slot, second);
        work.record(crate::WorkCounters::UNCHARGED)?;
        assert_eq!(work.live_allocation_bytes(), live);
        assert_eq!(work.work.allocation_operations, allocated);
        Ok(())
    }

    #[test]
    fn shared_map_root_retires_external_generation_once_while_proof_is_loaned()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let ledger = Arc::new(ViewLedger::new());
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let (slot, mut loan) = {
            let (roots, allocations, counters, budget) = work.view_ledger_admission_parts()?;
            roots.take(&ledger, allocations, counters, budget)?
        };
        ledger.record_measured(&ViewChange::Node(FileId::from_bytes([1; 16])), &mut work, &mut loan)?;
        let ViewLedgerAdmission::Shared(proof) = &loan else { panic!("original shared proof"); };
        let (generation, bytes) = proof.lock().expect("original map proof").nodes.expect("owned nodes");
        let live = work.live_allocation_bytes();
        for byte in 2..=8 {
            ledger.record(&ViewChange::Node(FileId::from_bytes([byte; 16])));
        }
        assert!(ledger.nodes.0.read().expect("physical change map").allocation_generation > generation);
        work.record(crate::WorkCounters::UNCHARGED)?;
        assert_eq!(work.live_allocation_bytes(), live - bytes);
        assert!(proof.lock().expect("same loaned proof").nodes.is_none());
        let reconciled = work.live_allocation_bytes();
        work.record(crate::WorkCounters::UNCHARGED)?;
        assert_eq!(work.live_allocation_bytes(), reconciled);
        work.restore_view_ledger_admission(slot, loan);
        work.record(crate::WorkCounters::UNCHARGED)?;
        assert_eq!(work.live_allocation_bytes(), reconciled);
        Ok(())
    }

    #[test]
    fn shared_map_distinct_roots_restore_nested_non_lifo_success_and_failure_loans()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let first_source = Arc::new(ViewLedger::new());
        let second_source = Arc::new(ViewLedger::new());
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let (first_slot, mut first) = {
            let (roots, allocations, counters, budget) = work.view_ledger_admission_parts()?;
            roots.take(&first_source, allocations, counters, budget)?
        };
        let (second_slot, mut second) = {
            let (roots, allocations, counters, budget) = work.view_ledger_admission_parts()?;
            roots.take(&second_source, allocations, counters, budget)?
        };
        assert_ne!(first_slot, second_slot);
        let (nested_slot, mut nested) = {
            let (roots, allocations, counters, budget) = work.view_ledger_admission_parts()?;
            roots.take(&first_source, allocations, counters, budget)?
        };
        assert_eq!(nested_slot, first_slot);
        let (ViewLedgerAdmission::Shared(original), ViewLedgerAdmission::Shared(alias),
            ViewLedgerAdmission::Shared(other)) = (&first, &nested, &second)
            else { panic!("original roots retain actual shared map custody"); };
        assert!(Arc::ptr_eq(original, alias));
        assert!(!Arc::ptr_eq(original, other));
        first_source.record_measured(&ViewChange::Node(FileId::from_bytes([11; 16])), &mut work, &mut first)?;
        second_source.record_measured(&ViewChange::Node(FileId::from_bytes([12; 16])), &mut work, &mut second)?;
        work.restore_view_ledger_admission(first_slot, first);
        let live = work.live_allocation_bytes();
        work.budget.items_examined = work.work.items_examined;
        assert!(matches!(first_source.record_measured(
            &ViewChange::Node(FileId::from_bytes([13; 16])), &mut work, &mut nested,
        ), Err(super::super::MountSourceError::Work(_))));
        work.restore_view_ledger_admission(second_slot, second);
        cancellation.cancel();
        assert!(matches!(work.check(), Err(crate::demand::DemandError::Cancelled)));
        work.restore_view_ledger_admission(nested_slot, nested);
        work.record(crate::WorkCounters::UNCHARGED)?;
        assert_eq!(work.live_allocation_bytes(), live);
        work.budget = crate::WorkBudget::UNBOUNDED;
        let (restored_slot, restored) = {
            let (roots, allocations, counters, budget) = work.view_ledger_admission_parts()?;
            roots.take(&first_source, allocations, counters, budget)?
        };
        assert_eq!(restored_slot, first_slot);
        work.restore_view_ledger_admission(restored_slot, restored);
        Ok(())
    }

    #[test]
    fn bounded_readonly_query_visits_real_slots_without_adopting_their_backing()
    -> Result<(), Box<dyn std::error::Error>> {
        let ledger = ViewLedger::new();
        let file = FileId::from_bytes([91; 16]);
        ledger.record(&ViewChange::Node(file));
        let checkpoint = ledger.stamp();
        let root = path(&[])?;
        let cancellation = crate::CancellationToken::new();
        let mut denied = crate::demand::DemandWork::new(crate::WorkBudget {
            items_examined: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(matches!(ledger.unchanged_since_measured(&root, Some(file), checkpoint, &mut denied),
            Err(super::super::MountSourceError::Work(_))));
        assert_eq!(denied.work, crate::WorkCounters::UNCHARGED);
        let mut admitted = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        assert!(ledger.unchanged_since_measured(&root, Some(file), checkpoint, &mut admitted)?);
        assert!(admitted.work.items_examined > 0);
        assert_eq!(admitted.work.allocation_operations, 0);
        assert_eq!(admitted.live_allocation_bytes(), 0);
        ledger.record(&ViewChange::Node(file));
        assert!(!ledger.unchanged_since_measured(&root, Some(file), checkpoint, &mut admitted)?);
        assert!(!ledger.unchanged_since(&root, Some(file), checkpoint));
        Ok(())
    }

    #[test]
    fn bounded_unicode_key_denial_releases_the_actual_normalization_buffer()
    -> Result<(), Box<dyn std::error::Error>> {
        let text = "Café";
        let unicode = path(&[text])?;
        let cancellation = crate::CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget {
            items_examined: 1 + text.chars().count() as u64,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(super::child_key_measured(
            super::key_of(&()), &unicode.components()[0], &mut work,
        ).is_err());
        assert_eq!(work.work.items_examined, 1 + text.chars().count() as u64);
        assert_eq!(work.work.allocation_operations, 1);
        assert!(work.work.peak_allocation_bytes > 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
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
        ledger.observe(observer)?;
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
