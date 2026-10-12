//! Exact invalidation of cached mount lookups.

use crate::FileId;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use std::cell::Cell;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};

pub(super) fn unbounded_view_work() -> crate::demand::DemandWork {
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

const MAXIMUM_TRACKED_KEYS: usize = 1 << 16;

/// One position in the process-wide order of mount view changes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ViewStamp(u64);

impl ViewStamp {
    /// Precedes every change: facts of unknown age are at least this old.
    #[cfg(target_os = "linux")]
    pub(super) const ORIGIN: Self = Self(0);

    pub(super) fn current() -> Self {
        Self(VIEW_CHANGES.load(Ordering::Acquire))
    }

    pub(super) fn of(slot: &AtomicU64) -> Self {
        Self(slot.load(Ordering::Acquire))
    }

    /// Records one change in a slot, at a position after every stamp sampled before it, and returns that position.
    #[cfg(all(test, target_os = "linux"))]
    pub(super) fn record(slot: &AtomicU64) -> Self {
        let position = Self::next();
        position.record_in(slot);
        position
    }

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
    const UNATTRIBUTED: Self = Self(0);

    /// A fresh origin, distinct from every other.
    #[cfg(any(target_os = "linux", target_os = "macos", test))]
    pub(super) fn new() -> Self {
        Self(NEXT_ORIGIN.fetch_add(1, Ordering::Relaxed))
    }

    pub(super) fn enter(self) -> ViewOriginScope {
        ViewOriginScope(CURRENT_ORIGIN.replace(self))
    }

    pub(super) fn current() -> Self {
        CURRENT_ORIGIN.get()
    }
}

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

    /// Known native allocation custody, when this is an SDK-owned observer.
    fn allocation_custody(&self) -> Option<super::NativeMountAllocationCustody> {
        None
    }

    /// Changes to the source may not all have been reported by `position`, though none is known to have been lost (`ViewChange::Unconfirmed`).
    fn view_unconfirmed(&self, position: ViewStamp) {
        self.view_changed(position, ViewOrigin::current());
    }
}

#[derive(Default)]
struct ViewObserverRegistry {
    observers: Vec<(Weak<dyn ViewObserver>, Option<super::NativeMountAllocationCustody>)>,
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
        self.sync.lock().complete(crate::materializer::NativeResourceCredit::Items, 1, 1)
            .expect("an actual notification visit owns an original credit");
        self.credits = self.credits.checked_sub(1).expect("one notification visit");
    }
}

impl Drop for PendingObserverDelivery {
    fn drop(&mut self) {
        if self.credits != 0 {
            self.sync.lock().refund(crate::materializer::NativeResourceCredit::Items, self.credits).expect("original reservation is balanced");
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
        let old_bytes = (self.entries.capacity() * width) as u64;
        entries.append(&mut self.entries);
        drop(std::mem::replace(&mut self.entries, entries));
        if let Some(old) = self.custody.replace(custody) {
            old.retire(old_bytes).map_err(super::MountSourceError::Work)?;
        }
        work.record(copied).map_err(mount_demand_error)
    }

    fn cancel_with_work(&mut self, work: &mut crate::demand::DemandWork) {
        for delivery in &mut self.entries {
            if delivery.credits != 0 {
                work.complete(crate::materializer::NativeResourceCredit::Items, 1, 1).expect("actual prepared cleanup owns a credit");
                delivery.credits -= 1;
                work.refund(crate::materializer::NativeResourceCredit::Items, delivery.credits).expect("original reservation is balanced");
                delivery.credits = 0;
            }
        }
    }
}

#[derive(Default)]
struct PendingDeliveryBuffer {
    entries: Vec<PendingObserverDelivery>,
    custody: Option<super::NativeMountAllocationCustody>,
}

impl Drop for PendingDeliveryBuffer {
    fn drop(&mut self) {
        let bytes = (self.entries.capacity() * std::mem::size_of::<PendingObserverDelivery>()) as u64;
        drop(std::mem::take(&mut self.entries));
        if let Some(custody) = self.custody.take() {
            custody.retire(bytes).expect("notification backing retires once");
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
            let mut hub = self.ledger.observers.lock().unwrap_or_else(PoisonError::into_inner);
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
        if self.credits != 0 { self.sync.lock().refund(crate::materializer::NativeResourceCredit::Items, self.credits).expect("original reservation is balanced"); }
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
    work.reserve(crate::materializer::NativeResourceCredit::Copy, bytes).map_err(super::lazy::mount_demand_error)?;
    let result = pending.deliveries.entries.len().checked_add(1)
        .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))
        .and_then(|required| pending.deliveries.reserve(required, work));
    if let Err(error) = result {
        work.refund(crate::materializer::NativeResourceCredit::Copy, bytes).expect("original reservation is balanced");
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
    work.complete(crate::materializer::NativeResourceCredit::Copy, copied, copied).map_err(super::lazy::mount_demand_error)
}

impl ViewLedger {
    fn prepare_join(&self) -> Result<Option<PreparedObserverJoin>, super::MountSourceError> {
        let pending = {
            let hub = self.observers.lock().unwrap_or_else(PoisonError::into_inner);
            hub.pending.as_ref().map(|pending| (pending.id, pending.sync.clone()))
        };
        let Some((id, original)) = pending else { return Ok(None); };
        let sync = original.upgrade().ok_or(super::MountSourceError::Stale)?;
        sync.lock().reserve(crate::materializer::NativeResourceCredit::Items, 2).map_err(super::lazy::mount_demand_error)?;
        Ok(Some(PreparedObserverJoin { id, sync, credits: 2 }))
    }

    fn add_locked(
        &self,
        observers: &mut Vec<(Weak<dyn ViewObserver>, Option<super::NativeMountAllocationCustody>)>,
        observer: Weak<dyn ViewObserver>,
        custody: Option<super::NativeMountAllocationCustody>,
        registry: &mut Option<(u64, u64)>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
    ) -> Result<(), super::MountSourceError> {
        let generation = self.observer_generation.load(Ordering::Acquire);
        if let Some((owned_generation, bytes)) = *registry
            && owned_generation != generation
        {
            allocations.release(bytes).map_err(observer_allocation_error)?;
            *registry = None;
        }
        let width = std::mem::size_of::<(Weak<dyn ViewObserver>, Option<super::NativeMountAllocationCustody>)>();
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
            let alive = entry.0.strong_count() != 0;
            if alive { moved += usize::from(gap); } else { gap = true; }
            alive
        });
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
            self.observer_generation.store(next_generation, Ordering::Release);
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
        observers.push((observer, custody));
        work.charge(&copied, &budget).map_err(super::MountSourceError::Work)?;
        Ok(())
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
                ledger.observer_generation.load(Ordering::Acquire) != generation)
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

pub(super) enum ViewChange<'a> {
    /// One node's content, metadata, or attributes.
    Node(FileId),
    /// A name now resolves where nothing did.
    Bound(&'a NamespacePath),
    /// One binding and its node changed.
    Entry(&'a NamespacePath, FileId),
    /// A node moved between names, possibly replacing another node.
    Moved {
        from: &'a NamespacePath,
        to: &'a NamespacePath,
        moved: FileId,
        replaced: Option<FileId>,
    },
    /// An effect enumerated in full: a prepared candidate replaced the checkout, or the source reported changes made to it outside the view.
    Effect(&'a ViewEffect),
    /// An effect set that cannot be enumerated, such as a rebind.
    Everything,
    /// Changes to the source may not all have been reported yet, though none is known to have been lost.
    Unconfirmed,
}

/// Named and node effects of one source change.
#[derive(Default)]
pub(super) struct ViewEffect {
    pub(super) nodes: Vec<FileId>,
    pub(super) names: Vec<(NamespacePath, LogicalName)>,
    pub(super) subtrees: Vec<NamespacePath>,
    pub(super) listings: Vec<NamespacePath>,
    pub(super) everything: bool,
}

impl ViewEffect {
    pub(super) fn is_empty(&self) -> bool {
        self.nodes.is_empty()
            && self.names.is_empty()
            && self.subtrees.is_empty()
            && self.listings.is_empty()
            && !self.everything
    }
}

pub(crate) struct ViewLedger {
    latest: AtomicU64,
    everything: AtomicU64,
    unconfirmed: AtomicU64,
    bindings: ChangeMap,
    directories: ChangeMap,
    nodes: ChangeMap,
    observers: Mutex<ViewObserverRegistry>,
    observer_generation: AtomicU64,
    // Intrinsic constructor custody survives every strong ledger alias.
    _native_allocation: Option<crate::NativeAllocation>,
}

pub(super) struct PreparedNativeNodeChange<'a> {
    ledger: &'a ViewLedger,
    slot: PreparedChangeMapSlot<'a>,
    observers: (Vec<Arc<dyn ViewObserver>>, Option<crate::NativeAllocation>),
    completion: crate::materializer::NativeWorkReservation,
}

#[derive(Clone, Copy)]
enum AdmittedMap { Bindings, Directories, Nodes }

pub(crate) enum ViewLedgerAdmission {
    Inline([Option<(u64, u64)>; 3]),
    Shared(Arc<Mutex<[Option<(u64, u64)>; 3]>>),
}

impl Default for ViewLedgerAdmission {
    fn default() -> Self { Self::Inline([None; 3]) }
}

impl ViewLedgerAdmission {
    fn shared(
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        counters: &mut crate::WorkCounters, budget: crate::WorkBudget,
    ) -> Result<Self, super::MountSourceError> {
        allocations.claim_elements::<Mutex<[Option<(u64, u64)>; 3]>>(1, counters, budget)
            .map_err(observer_allocation_error)?;
        Ok(Self::Shared(Arc::new(Mutex::new([None; 3]))))
    }

    fn alias(&self) -> Self {
        match self {
            Self::Shared(control) => Self::Shared(Arc::clone(control)),
            Self::Inline(_) => unreachable!("registered map proof has shared custody"),
        }
    }

    fn claims(&self) -> [Option<(u64, u64)>; 3] {
        match self {
            Self::Inline(state) => *state,
            Self::Shared(control) => *control.lock().unwrap_or_else(PoisonError::into_inner),
        }
    }

    fn replace(&mut self, map: AdmittedMap, value: Option<(u64, u64)>) -> Option<(u64, u64)> {
        match self {
            Self::Inline(state) => std::mem::replace(&mut state[map as usize], value),
            Self::Shared(control) => {
                let mut state = control.lock().unwrap_or_else(PoisonError::into_inner);
                std::mem::replace(&mut state[map as usize], value)
            }
        }
    }

    fn claim(&self, map: AdmittedMap) -> Option<(u64, u64)> {
        self.claims()[map as usize]
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
        let retired = self.retire_dead(Some(ledger));
        work.release(retired).map_err(super::lazy::mount_demand_error)
    }
}

struct LedgerRegistryAdmission {
    ledger: Weak<ViewLedger>,
    admission: Option<ViewLedgerAdmission>,
}

impl LedgerRegistryAdmission {
    fn has_loans(&self) -> bool {
        matches!(&self.admission, Some(ViewLedgerAdmission::Shared(control)) if Arc::strong_count(control) > 1)
    }

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
        if ledger.is_none() && !self.has_loans() && self.admission.is_some() {
            drop(self.admission.take());
            allocations.release(std::mem::size_of::<Mutex<[Option<(u64, u64)>; 3]>>() as u64)
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
                let admission = entry.admission.as_ref()
                    .expect("live registered map proof").alias();
                return Ok((slot, admission));
            }
            if reusable.is_none() && !entry.has_loans() && entry.ledger.upgrade().is_none() {
                reusable = Some(slot);
            }
        }
        if let Some(slot) = reusable {
            let entry = if slot == 0 { self.first.as_mut().expect("first original root") }
                else { &mut self.more[slot - 1] };
            entry.reconcile(allocations).map_err(super::MountSourceError::Work)?;
            let admission = ViewLedgerAdmission::shared(allocations, counters, budget)?;
            let local = admission.alias();
            *entry = LedgerRegistryAdmission { ledger: identity, admission: Some(admission) };
            return Ok((slot, local));
        }
        if self.first.is_none() {
            let admission = ViewLedgerAdmission::shared(allocations, counters, budget)?;
            let local = admission.alias();
            self.first = Some(LedgerRegistryAdmission { ledger: identity, admission: Some(admission) });
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
        self.more.push(LedgerRegistryAdmission { ledger: identity, admission: Some(admission) });
        Ok((slot, local))
    }

    pub(crate) fn restore(&mut self, slot: usize, admission: ViewLedgerAdmission) {
        let entry = if slot == 0 { self.first.as_mut().expect("first original root") }
            else { self.more.get_mut(slot - 1).expect("original root slot") };
        let (Some(ViewLedgerAdmission::Shared(original)), ViewLedgerAdmission::Shared(local))
            = (&entry.admission, &admission) else { unreachable!("registered map custody"); };
        assert!(Arc::ptr_eq(original, local), "original map custody is restored");
        drop(admission);
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
            observers: Mutex::default(),
            observer_generation: AtomicU64::new(0),
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
        let mut hub = self.observers.lock().unwrap_or_else(PoisonError::into_inner);
        if hub.pending.is_some() { return Err(super::MountSourceError::Stale); }
        let id = hub.next_notification.checked_add(1)
            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
        let mut deliveries = PendingDeliveryBuffer::default();
        let prepared = (|| {
            let mut live = 0_usize;
            for slot in &hub.observers {
                mount_items(&mut work, 1)?;
                if slot.0.strong_count() != 0 {
                    live = live.checked_add(1)
                        .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
                }
            }
            deliveries.reserve(live, &mut work)?;
            for slot in &hub.observers {
                mount_items(&mut work, 1)?;
                if slot.0.strong_count() == 0 { continue; }
                let copied = crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<PendingObserverDelivery>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                };
                work.admit(copied).map_err(mount_demand_error)?;
                work.reserve(crate::materializer::NativeResourceCredit::Items, 2).map_err(mount_demand_error)?;
                deliveries.entries.push(PendingObserverDelivery {
                    observer: PendingObserverState::Reserved(Weak::clone(&slot.0)),
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

    pub(super) fn record_everything_prepared(
        &self, mut prepared: PreparedEverythingNotification,
    ) {
        assert!(std::ptr::eq(self, Arc::as_ptr(&prepared.ledger)), "original notification ledger");
        let (mut pending, position) = {
            let mut hub = self.observers.lock().unwrap_or_else(PoisonError::into_inner);
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

    pub(super) fn observe(
        self: &Arc<Self>, observer: Weak<dyn ViewObserver>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), super::MountSourceError> {
        use super::lazy::mount_demand_error;
        let custody = observer.upgrade().and_then(|observer| observer.allocation_custody());
        loop {
            let mut join = self.prepare_join()?;
            let result = carrier.with_work(|work| -> Result<_, super::MountSourceError> {
                work.check().map_err(mount_demand_error)?;
                let mut hub = self.observers.lock().unwrap_or_else(PoisonError::into_inner);
                if !join_matches(&hub, join.as_ref()) {
                    Ok(None)
                } else {
                    let mut copy_credit = prepare_join_buffer(&mut hub, work)?;
                    let admitted = (|| {
                        let (admissions, allocations, counters, budget) =
                            work.view_observer_admission_parts().map_err(mount_demand_error)?;
                        let entry = admissions.entry_mut(self, allocations, counters, budget)?;
                        self.add_locked(
                            &mut hub.observers, observer.clone(), custody.clone(),
                            &mut entry.registry, allocations, counters, budget,
                        )?;
                        append_join(&mut hub, &observer, &mut join, work, &mut copy_credit)
                    })();
                    if copy_credit != 0 { work.refund(crate::materializer::NativeResourceCredit::Copy, copy_credit).expect("original reservation is balanced"); }
                    Ok(Some(admitted))
                }
            })?;
            drop(join);
            if let Some(result) = result { return result; }
            carrier.with_work(|work| work.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            })).map_err(mount_demand_error)?;
        }
    }

    pub(super) fn stamp(&self) -> ViewStamp {
        ViewStamp::of(&self.latest)
    }

    pub(super) fn prepare_node_change_with_native_work(
        &self, file_id: FileId, original: &crate::NativeMaterializationWork,
    ) -> Result<PreparedNativeNodeChange<'_>, super::MountSourceError> {
        let mut work = ViewRecordWork::Native(original);
        let observers = self.prepare_record_observers(&mut work, false)?;
        record_items(&mut work, 1)?;
        let slot = self.nodes.prepare_slot(
            key_of(&file_id), &mut work, &mut ViewLedgerAdmission::default(), AdmittedMap::Nodes, true,
        )?;
        let items_examined = (observers.0.len() as u64).checked_add(slot.terminal_items() as u64)
            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
        let completion = original.reserve_work(crate::WorkCounters {
            items_examined,
            bytes_copied: slot.terminal_copied()?,
            ..crate::WorkCounters::UNCHARGED
        }).map_err(super::MountSourceError::NativeWork)?;
        Ok(PreparedNativeNodeChange { ledger: self, slot, observers, completion })
    }

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
        let notified = notify_recorded_observers(false, &observers.0, position, || {
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

    pub(super) fn record(
        &self, change: &ViewChange<'_>, carrier: &mut super::adapter::NativeDemandCarrier<'_>,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(), super::MountSourceError> {
        let original = carrier.original_native_work();
        let (observers, position, result) = match &original {
            Some(original) => self.prepare_record(change, &mut ViewRecordWork::Native(original), owned)?,
            None => carrier.with_work(|work|
                self.prepare_record(change, &mut ViewRecordWork::Ordinary(work), owned))?,
        };
        let notified = notify_recorded_observers(
            matches!(change, ViewChange::Unconfirmed), &observers.0, position, || {},
        );
        let bytes = retire_buffer(observers);
        let released = match &original {
            Some(original) => ViewRecordWork::Native(original).release_numeric(bytes),
            None => carrier.with_work(|work| work.release(bytes).map_err(super::lazy::mount_demand_error)),
        };
        if let Err(panic) = notified { std::panic::resume_unwind(panic); }
        result.and(released)
    }

    fn prepare_record_observers(
        &self, work: &mut ViewRecordWork<'_>, include_delivery_visits: bool,
    ) -> Result<(Vec<Arc<dyn ViewObserver>>, Option<crate::NativeAllocation>), super::MountSourceError> {
        loop {
            let live = {
                let entries = self.observers.lock().unwrap_or_else(PoisonError::into_inner);
                let mut live = 0_usize;
                for observer in entries.observers.iter() {
                    record_items(work, 1)?;
                    if observer.0.strong_count() != 0 {
                        live = live.checked_add(1)
                            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
                    }
                }
                live
            };
            let mut observers = record_buffer::<Arc<dyn ViewObserver>>(live, work)?;
            let collected = {
                let entries = self.observers.lock().unwrap_or_else(PoisonError::into_inner);
                (|| {
                    for observer in entries.observers.iter() {
                        record_items(work, 1)?;
                        if let Some(observer) = observer.0.upgrade() {
                            if observers.0.len() == observers.0.capacity() { return Ok(false); }
                            if !include_delivery_visits {
                                work.charge(crate::WorkCounters {
                                    bytes_copied: std::mem::size_of::<Arc<dyn ViewObserver>>() as u64,
                                    ..crate::WorkCounters::UNCHARGED
                                })?;
                            }
                            observers.0.push(observer);
                        }
                    }
                    if include_delivery_visits { record_items(work, observers.0.len())?; }
                    Ok(true)
                })()
            };
            match collected {
                Ok(true) => return Ok(observers),
                Ok(false) => work.release_numeric(retire_buffer(observers))?,
                Err(error) => {
                    let _ = work.release_numeric(retire_buffer(observers));
                    return Err(error);
                }
            }
        }
    }

    fn prepare_record(
        &self,
        change: &ViewChange<'_>,
        work: &mut ViewRecordWork<'_>,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(
        (Vec<Arc<dyn ViewObserver>>, Option<crate::NativeAllocation>), ViewStamp, Result<(), super::MountSourceError>,
    ), super::MountSourceError> {
        if let Some(ordinary) = work.ordinary() { owned.reconcile(self, ordinary)?; }
        let observers = self.prepare_record_observers(work, true)?;
        let position = ViewStamp::next();
        let result = (|| {
            match change {
                ViewChange::Node(id) => self.node_changed(*id, position, work, owned)?,
                ViewChange::Bound(path) => self.rebound(path, position, work, owned)?,
                ViewChange::Entry(path, id) => {
                    self.rebound(path, position, work, owned)?;
                    self.node_changed(*id, position, work, owned)?;
                }
                ViewChange::Moved { from, to, moved, replaced } => {
                    self.rebound(from, position, work, owned)?;
                    self.rebound(to, position, work, owned)?;
                    self.node_changed(*moved, position, work, owned)?;
                    if let Some(id) = replaced { self.node_changed(*id, position, work, owned)?; }
                }
                ViewChange::Effect(effect) => {
                    for id in &effect.nodes { self.node_changed(*id, position, work, owned)?; }
                    for (directory, name) in &effect.names {
                        let directory = path_key(directory, work)?;
                        let child = child_key(directory, name, work)?;
                        self.bindings.record(child, position, work, owned, AdmittedMap::Bindings)?;
                        self.directories.record(directory, position, work, owned, AdmittedMap::Directories)?;
                    }
                    for subtree in &effect.subtrees { self.rebound(subtree, position, work, owned)?; }
                    for directory in &effect.listings {
                        let key = path_key(directory, work)?;
                        self.directories.record(key, position, work, owned, AdmittedMap::Directories)?;
                    }
                    if effect.everything { position.record_in(&self.everything); }
                }
                ViewChange::Everything => position.record_in(&self.everything),
                ViewChange::Unconfirmed => position.record_in(&self.unconfirmed),
            }
            Ok(())
        })();
        position.record_in(&self.latest);
        Ok((observers, position, result))
    }

    pub(super) fn unchanged_since(
        &self, path: &NamespacePath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, super::MountSourceError> {
        work.check().map_err(super::lazy::mount_demand_error)?;
        if !stamp.precedes_none_of(&self.unconfirmed) { return Ok(false); }
        self.reported_unchanged_since(path, file_id, stamp, work)
    }

    pub(super) fn reported_unchanged_since(
        &self, path: &NamespacePath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, super::MountSourceError> {
        work.check().map_err(super::lazy::mount_demand_error)?;
        if !stamp.precedes_none_of(&self.everything) { return Ok(false); }
        let mut terminal = key_of(&());
        // One read of bindings covers every prefix, as in the ordinary query.
        let bindings = self.bindings.0.read().unwrap_or_else(PoisonError::into_inner);
        if !bindings.unchanged_key(terminal, stamp, work)? { return Ok(false); }
        for name in path.components() {
            terminal = child_key(terminal, name, &mut ViewRecordWork::Ordinary(work))?;
            if !bindings.unchanged_key(terminal, stamp, work)? { return Ok(false); }
        }
        drop(bindings);
        if !self.directories.unchanged_key(terminal, stamp, work)? { return Ok(false); }
        if let Some(file_id) = file_id {
            if !self.nodes.unchanged_key(key_of(&file_id), stamp, work)? { return Ok(false); }
        }
        Ok(true)
    }

    pub(super) fn binding_unchanged_since(
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
        if !bindings.unchanged_key(key, stamp, work)? { return Ok(false); }
        for name in path.components() {
            key = child_key(key, name, &mut ViewRecordWork::Ordinary(work))?;
            if !bindings.unchanged_key(key, stamp, work)? { return Ok(false); }
        }
        Ok(true)
    }

    pub(super) fn node_unchanged_since(
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
        self.nodes.unchanged_key(key_of(&file_id), stamp, work)
    }

    pub(super) fn listing_unchanged_since(
        &self,
        path: &NamespacePath,
        stamp: ViewStamp,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, super::MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        work.check().map_err(mount_demand_error)?;
        mount_items(work, 1)?;
        let directory = path_key(path, &mut ViewRecordWork::Ordinary(work))?;
        mount_items(work, 1)?;
        if !stamp.precedes_none_of(&self.unconfirmed) { return Ok(false); }
        mount_items(work, 1)?;
        if !stamp.precedes_none_of(&self.everything) { return Ok(false); }
        self.directories.unchanged_key(directory, stamp, work)
    }

    fn rebound(
        &self,
        path: &NamespacePath,
        position: ViewStamp,
        work: &mut ViewRecordWork<'_>,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(), super::MountSourceError> {
        let mut parent = None;
        let mut terminal = key_of(&());
        for name in path.components() {
            parent = Some(terminal);
            terminal = child_key(terminal, name, work)?;
        }
        self.bindings.record(terminal, position, work, owned, AdmittedMap::Bindings)?;
        if let Some(parent) = parent {
            self.directories.record(parent, position, work, owned, AdmittedMap::Directories)?;
        }
        Ok(())
    }

    fn node_changed(
        &self,
        id: FileId,
        position: ViewStamp,
        work: &mut ViewRecordWork<'_>,
        owned: &mut ViewLedgerAdmission,
    ) -> Result<(), super::MountSourceError> {
        record_items(work, 1)?;
        self.nodes.record(key_of(&id), position, work, owned, AdmittedMap::Nodes)
    }
}

/// The latest change recorded against each key.
#[derive(Default)]
struct ChangeMap(RwLock<ChangePositions>, AtomicU64);

#[derive(Default)]
struct ChangePositions {
    floor: u64,
    slots: HashMap<u64, u64>,
    // The physical map backing drops before this original allocation guard.
    native_allocation: Option<crate::NativeAllocation>,
}

struct PreparedChangeMapSlot<'a> {
    positions: std::sync::RwLockWriteGuard<'a, ChangePositions>,
    key: u64,
    fold: bool,
}

impl PreparedChangeMapSlot<'_> {
    fn terminal_items(&self) -> usize {
        if self.fold { self.positions.slots.len() } else { 0 }
    }

    fn terminal_copied(&self) -> Result<u64, super::MountSourceError> {
        let cleared = if self.fold { change_map_controls(self.positions.slots.capacity())? } else { 0 };
        cleared.checked_add(std::mem::size_of::<(u64, u64)>() as u64)
            .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))
    }

    fn record(self, position: ViewStamp) {
        let Self { mut positions, key, fold } = self;
        // The write guard preserves the prepared capacity.
        if fold {
            positions.floor = positions.slots.values()
                .fold(positions.floor, |floor, at| floor.max(*at));
            positions.slots.clear();
        }
        positions.slots.entry(key).and_modify(|at| *at = (*at).max(position.0)).or_insert(position.0);
    }
}

fn change_map_controls(capacity: usize) -> Result<u64, super::MountSourceError> {
    if capacity == 0 { return Ok(0); }
    (capacity as u64).checked_mul(2).and_then(|bytes| bytes.checked_add(32))
        .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))
}

fn change_map_bytes(capacity: usize) -> Result<u64, super::MountSourceError> {
    // Fund payload capacity plus a conservative control-storage allowance.
    (capacity as u64).checked_mul(2 * std::mem::size_of::<(u64, u64)>() as u64)
        .and_then(|bytes| bytes.checked_add(change_map_controls(capacity).ok()?))
        .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))
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

enum ViewRecordWork<'a> {
    Ordinary(&'a mut crate::demand::DemandWork),
    Native(&'a crate::NativeMaterializationWork),
}

impl ViewRecordWork<'_> {
    fn ordinary(&mut self) -> Option<&mut crate::demand::DemandWork> {
        match self { Self::Ordinary(work) => Some(work), Self::Native(_) => None }
    }
    fn charge(&mut self, delta: crate::WorkCounters) -> Result<(), super::MountSourceError> {
        match self {
            Self::Ordinary(work) => work.charge(delta).map_err(super::lazy::mount_demand_error),
            Self::Native(work) => work.charge(delta).map_err(super::MountSourceError::NativeWork),
        }
    }
    fn claim(&mut self, bytes: u64, operations: u64) -> Result<Option<crate::NativeAllocation>, super::MountSourceError> {
        match self {
            Self::Ordinary(work) => {
                work.claim_allocation(bytes, operations).map_err(super::lazy::mount_demand_error)?;
                Ok(None)
            }
            Self::Native(work) => work.claim_allocation(bytes, operations, crate::WorkCounters::UNCHARGED)
                .map(Some).map_err(super::MountSourceError::NativeWork),
        }
    }
    fn capacity(&mut self, allocation: &mut Option<crate::NativeAllocation>, numeric: &mut u64, actual: u64)
        -> Result<(), super::MountSourceError> {
        match self {
            Self::Ordinary(work) => work.reconcile_native_allocation(numeric, actual)
                .map_err(super::lazy::mount_demand_error),
            Self::Native(_) => allocation.as_mut().ok_or(super::MountSourceError::Stale)?
                .reconcile_capacity(actual).map_err(super::MountSourceError::NativeWork),
        }
    }
    fn release_numeric(&mut self, bytes: u64) -> Result<(), super::MountSourceError> {
        match self {
            Self::Ordinary(work) => work.release(bytes).map_err(super::lazy::mount_demand_error),
            Self::Native(_) if bytes == 0 => Ok(()),
            Self::Native(_) => Err(super::MountSourceError::Stale),
        }
    }
}

fn retire_buffer<T>(buffer: (Vec<T>, Option<crate::NativeAllocation>)) -> u64 {
    let bytes = if buffer.1.is_some() { 0 } else {
        (buffer.0.capacity() * std::mem::size_of::<T>()) as u64
    };
    drop(buffer);
    bytes
}

fn record_items(work: &mut ViewRecordWork<'_>, count: usize) -> Result<(), super::MountSourceError> {
    work.charge(crate::WorkCounters { items_examined: count as u64, ..crate::WorkCounters::UNCHARGED })
}

fn record_buffer<T>(
    count: usize, work: &mut ViewRecordWork<'_>,
) -> Result<(Vec<T>, Option<crate::NativeAllocation>), super::MountSourceError> {
    let bytes = count.checked_mul(std::mem::size_of::<T>())
        .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64;
    let mut allocation = work.claim(bytes, u64::from(bytes != 0))?;
    let mut numeric = if allocation.is_some() { 0 } else { bytes };
    let mut values = Vec::new();
    if let Err(error) = values.try_reserve_exact(count) {
        drop(values);
        let _ = work.release_numeric(numeric);
        return Err(super::MountSourceError::Allocation(error));
    }
    let actual = (values.capacity() * std::mem::size_of::<T>()) as u64;
    if let Err(error) = work.capacity(&mut allocation, &mut numeric, actual) {
        drop(values);
        let _ = work.release_numeric(numeric);
        return Err(error);
    }
    Ok((values, allocation))
}

fn retire_change_map(buffer: (HashMap<u64, u64>, Option<crate::NativeAllocation>)) -> u64 {
    let bytes = if buffer.1.is_some() { 0 } else {
        change_map_bytes(buffer.0.capacity()).expect("bounded change-map capacity")
    };
    drop(buffer);
    bytes
}

fn reserve_change_map(
    count: usize, work: &mut ViewRecordWork<'_>,
) -> Result<(HashMap<u64, u64>, Option<crate::NativeAllocation>), super::MountSourceError> {
    let bytes = change_map_bytes(count)?;
    let mut allocation = work.claim(bytes, 1)?;
    let mut numeric = if allocation.is_some() { 0 } else { bytes };
    let mut values = HashMap::new();
    if let Err(error) = values.try_reserve(count) {
        drop(values);
        let _ = work.release_numeric(numeric);
        return Err(super::MountSourceError::Allocation(error));
    }
    let actual = change_map_bytes(values.capacity())?;
    if let Err(error) = work.capacity(&mut allocation, &mut numeric, actual) {
        drop(values);
        let _ = work.release_numeric(numeric);
        return Err(error);
    }
    Ok((values, allocation))
}

impl ChangeMap {
    fn record(
        &self, key: u64, position: ViewStamp, work: &mut ViewRecordWork<'_>,
        owned: &mut ViewLedgerAdmission, kind: AdmittedMap,
    ) -> Result<(), super::MountSourceError> {
        let slot = self.prepare_slot(key, work, owned, kind, false)?;
        work.charge(crate::WorkCounters { bytes_copied: slot.terminal_copied()?, ..crate::WorkCounters::UNCHARGED })?;
        slot.record(position);
        Ok(())
    }

    fn prepare_slot(
        &self, key: u64, work: &mut ViewRecordWork<'_>,
        owned: &mut ViewLedgerAdmission, kind: AdmittedMap, defer_fold: bool,
    ) -> Result<PreparedChangeMapSlot<'_>, super::MountSourceError> {
        use super::lazy::mount_demand_error;
        loop {
            let mut positions = self.0.write().unwrap_or_else(PoisonError::into_inner);
            if let Some(ordinary) = work.ordinary() {
                if let Some((generation, _)) = owned.claim(kind)
                    && generation != self.1.load(Ordering::Acquire)
                    && let Some((_, bytes)) = owned.replace(kind, None)
                {
                    ordinary.release(bytes).map_err(mount_demand_error)?;
                }
            }
            if !positions.slots.is_empty() { record_items(work, 1)?; }
            if positions.slots.contains_key(&key) {
                return Ok(PreparedChangeMapSlot { positions, key, fold: false });
            }
            if positions.slots.len() == MAXIMUM_TRACKED_KEYS {
                if defer_fold {
                    return Ok(PreparedChangeMapSlot { positions, key, fold: true });
                }
                work.charge(crate::WorkCounters {
                    items_examined: positions.slots.len() as u64,
                    bytes_copied: change_map_controls(positions.slots.capacity())?,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                positions.floor = positions.slots.values()
                    .fold(positions.floor, |floor, at| floor.max(*at));
                positions.slots.clear();
                continue;
            }
            if positions.slots.len() < positions.slots.capacity() {
                return Ok(PreparedChangeMapSlot { positions, key, fold: false });
            }
            let old_bytes = change_map_bytes(positions.slots.capacity())?;
            let old_generation = self.1.load(Ordering::Acquire);
            let next_generation = old_generation.checked_add(1)
                .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))?;
            let count = positions.slots.capacity().max(4).saturating_mul(2).min(MAXIMUM_TRACKED_KEYS);
            let borrowed_old = if let Some(ordinary) = work.ordinary() {
                if owned.claim(kind).is_none() {
                    ordinary.adopt(old_bytes).map_err(mount_demand_error)?;
                    true
                } else { false }
            } else { false };
            drop(positions);
            let mut replacement = match reserve_change_map(count, work) {
                Ok(replacement) => replacement,
                Err(error) => {
                    if borrowed_old { let _ = work.release_numeric(old_bytes); }
                    return Err(error);
                }
            };
            let mut positions = self.0.write().unwrap_or_else(PoisonError::into_inner);
            if self.1.load(Ordering::Acquire) != old_generation {
                drop(positions);
                work.release_numeric(retire_change_map(replacement))?;
                if borrowed_old { work.release_numeric(old_bytes)?; }
                continue;
            }
            let copied = crate::WorkCounters {
                items_examined: positions.slots.len() as u64,
                bytes_copied: positions.slots.len().checked_mul(std::mem::size_of::<(u64, u64)>())
                    .ok_or(super::MountSourceError::Work(crate::WorkError::Overflow))? as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            if let Err(error) = work.charge(copied) {
                drop(positions);
                let _ = work.release_numeric(retire_change_map(replacement));
                if borrowed_old { let _ = work.release_numeric(old_bytes); }
                return Err(error);
            }
            replacement.0.extend(positions.slots.iter().map(|(&key, &at)| (key, at)));
            let (values, allocation) = replacement;
            let numeric = change_map_bytes(values.capacity())?;
            let old = std::mem::replace(&mut positions.slots, values);
            let old_guard = std::mem::replace(&mut positions.native_allocation, allocation);
            self.1.store(next_generation, Ordering::Release);
            if work.ordinary().is_some() {
                let _ = owned.replace(kind, Some((next_generation, numeric)));
            }
            drop(positions);
            drop(old);
            drop(old_guard);
            if let Some(ordinary) = work.ordinary() {
                ordinary.release(old_bytes).map_err(mount_demand_error)?;
            }
        }
    }

    fn unchanged_key(
        &self, key: u64, stamp: ViewStamp, work: &mut crate::demand::DemandWork,
    ) -> Result<bool, super::MountSourceError> {
        let positions = self.0.read().unwrap_or_else(PoisonError::into_inner);
        if !positions.slots.is_empty() { super::lazy::mount_items(work, 1)?; }
        let at = positions.slots.get(&key).copied().unwrap_or(0);
        Ok(at.max(positions.floor) <= stamp.0)
    }
}

fn path_key(
    path: &NamespacePath,
    work: &mut ViewRecordWork<'_>,
) -> Result<u64, super::MountSourceError> {
    let mut key = key_of(&());
    for name in path.components() { key = child_key(key, name, work)?; }
    Ok(key)
}

fn child_key(
    parent: u64,
    name: &LogicalName,
    work: &mut ViewRecordWork<'_>,
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

fn hash_unicode_fold<I: Iterator<Item = char> + Clone>(
    text: I,
    hasher: &mut DefaultHasher,
    work: &mut ViewRecordWork<'_>,
) -> Result<(), super::MountSourceError> {
    use unicode_normalization::char::{canonical_combining_class, compose, decompose_canonical};
    let mut count = 0_usize;
    for scalar in text.clone() {
        record_items(work, 1)?;
        decompose_canonical(scalar, |_| count += 1);
    }
    let mut decomposed = record_buffer::<(u8, char)>(count, work)?;
    let result = (|| {
    for scalar in text {
        let mut emitted = 0;
        decompose_canonical(scalar, |_| emitted += 1);
        record_items(work, emitted)?;
        decompose_canonical(scalar, |scalar| decomposed.0.push((canonical_combining_class(scalar), scalar)));
    }
    for index in 1..decomposed.0.len() {
        let value = *decomposed.0.get(index).ok_or(super::MountSourceError::Stale)?;
        if value.0 == 0 { continue; }
        let mut position = index;
        while position > 0 {
            record_items(work, 1)?;
            let prior = *decomposed.0.get(position - 1).ok_or(super::MountSourceError::Stale)?;
            if prior.0 <= value.0 { break; }
            work.charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<(u8, char)>() as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            *decomposed.0.get_mut(position).ok_or(super::MountSourceError::Stale)? = prior;
            position -= 1;
        }
        if position != index {
            *decomposed.0.get_mut(position).ok_or(super::MountSourceError::Stale)? = value;
        }
    }
    let mut written = 0;
    let mut starter: Option<usize> = None;
    let mut last_class = 0;
    for read in 0..decomposed.0.len() {
        record_items(work, 1)?;
        let value = *decomposed.0.get(read).ok_or(super::MountSourceError::Stale)?;
        if let Some(starter) = starter {
            if last_class == 0 || last_class < value.0 {
                let prior = decomposed.0.get(starter).ok_or(super::MountSourceError::Stale)?.1;
                if let Some(composed) = compose(prior, value.1) {
                    decomposed.0.get_mut(starter).ok_or(super::MountSourceError::Stale)?.1 = composed;
                    continue;
                }
            }
        }
        if value.0 == 0 { starter = Some(written); }
        last_class = value.0;
        *decomposed.0.get_mut(written).ok_or(super::MountSourceError::Stale)? = value;
        written += 1;
    }
    let mut encoded = [0_u8; 4];
    for (_, scalar) in decomposed.0.get(..written).ok_or(super::MountSourceError::Stale)? {
        for folded in scalar.to_uppercase().flat_map(char::to_lowercase) {
            record_items(work, 1)?;
            hasher.write(folded.encode_utf8(&mut encoded).as_bytes());
        }
    }
    Ok(())
    })();
    let retired = work.release_numeric(retire_buffer(decomposed));
    result.and(retired)
}

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

    #[derive(Default)]
    struct Counted(std::sync::atomic::AtomicU64);
    impl ViewObserver for Counted {
        fn view_changed(&self, _: ViewStamp, _: ViewOrigin) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    struct Panicking;
    impl ViewObserver for Panicking {
        fn view_changed(&self, _: ViewStamp, _: ViewOrigin) { panic!("observer failure"); }
    }

    #[test]
    fn native_node_preadmission_denial_and_early_cancellation_leave_body_untouched()
    -> Result<(), Box<dyn std::error::Error>> {
        let observer_bytes = std::mem::size_of::<Arc<dyn ViewObserver>>() as u64;
        let slot_bytes = std::mem::size_of::<(u64, u64)>() as u64;
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
            let ledger = Arc::new(ViewLedger::new());
            let file = FileId::from_bytes([112; 16]);
            ledger.record(&ViewChange::Node(file), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
            let before = ledger.stamp();
            let counted = Arc::new(Counted::default());
            let observer: Arc<dyn ViewObserver> = counted.clone();
            ledger.observe(Arc::downgrade(&observer), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()))?;
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
            assert!(ledger.node_unchanged_since(file, before, &mut super::unbounded_view_work()).expect("unbounded view query"));
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
                assert!(!ledger.node_unchanged_since(self.file, self.before, &mut super::unbounded_view_work()).expect("unbounded view query"),
                    "the node lock must retire before a reentrant observer query");
                self.calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let controller = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).map_err(|(error, _)| error)?.receipt();
        let observer_bytes = std::mem::size_of::<Arc<dyn ViewObserver>>() as u64;
        let slot_bytes = std::mem::size_of::<(u64, u64)>() as u64;
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
            ledger.record(&ViewChange::Node(file), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
            let before = ledger.stamp();
            let origin = ViewOrigin::new();
            let body = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let observed = Arc::new(Observed {
                ledger: Arc::downgrade(&ledger), file, before, origin, body: body.clone(),
                calls: std::sync::atomic::AtomicU64::new(0),
            });
            let observer: Arc<dyn ViewObserver> = observed.clone();
            ledger.observe(Arc::downgrade(&observer), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()))?;
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
            assert!(!ledger.node_unchanged_since(file, before, &mut super::unbounded_view_work()).expect("unbounded view query"));
        }
        Ok(())
    }

    #[test]
    fn aborting_native_node_permit_releases_only_physically_retired_buffers()
    -> Result<(), Box<dyn std::error::Error>> {
        let ledger = Arc::new(ViewLedger::new());
        let observer: Arc<dyn ViewObserver> = Arc::new(Panicking);
        let weak = Arc::downgrade(&observer);
        ledger.observe(weak.clone(), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()))?;
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let file = FileId::from_bytes([114; 16]);
        let before = ledger.stamp();
        let prepared = ledger.prepare_node_change_with_native_work(file, &original)?;
        let prefix = original.receipt();
        let map_bytes = super::change_map_bytes(prepared.slot.positions.slots.capacity())?;
        let observer_bytes = (prepared.observers.0.capacity()
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
        assert!(ledger.node_unchanged_since(file, before, &mut super::unbounded_view_work()).expect("unbounded view query"));
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
        let ledger = Arc::new(ViewLedger::new());
        let observer: Arc<dyn ViewObserver> = Arc::new(Panicking);
        let weak = Arc::downgrade(&observer);
        ledger.observe(weak.clone(), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()))?;
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let file = FileId::from_bytes([115; 16]);
        let before = ledger.stamp();
        let prepared = ledger.prepare_node_change_with_native_work(file, &original)?;
        let prefix = original.receipt();
        let map_bytes = super::change_map_bytes(prepared.slot.positions.slots.capacity())?;
        drop(observer);
        cancellation.cancel();
        let notified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(||
            ledger.record_node_change_prepared(prepared)));
        assert!(notified.is_err());
        assert!(!ledger.node_unchanged_since(file, before, &mut super::unbounded_view_work()).expect("unbounded view query"));
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
                    ledger.record(&ViewChange::Node(FileId::from_bytes([byte; 16])), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
                }
            });
            ready.recv().expect("concurrent recorder started");
            ledger.record_node_change_prepared(prepared)?;
            joined.join().expect("canonical recorder succeeds");
            Ok(())
        })?;
        assert!(!ledger.node_unchanged_since(file, before, &mut super::unbounded_view_work()).expect("unbounded view query"));
        for byte in 1..=20 {
            assert!(!ledger.node_unchanged_since(FileId::from_bytes([byte; 16]), before, &mut super::unbounded_view_work()).expect("unbounded view query"));
        }
        assert_eq!(original.live_allocation_bytes(), baseline,
            "ordinary growth physically retires the exact native backing");
        Ok(())
    }

    #[test]
    fn prepared_publication_uses_final_liveness_and_releases_borrowed_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let ledger = Arc::new(ViewLedger::new());
        let early: Arc<dyn ViewObserver> = Arc::new(Counted::default());
        let dead = Arc::downgrade(&early);
        ledger.observe(dead.clone(), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()))?;
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
            ledger.observe(Arc::downgrade(&late), &mut super::super::adapter::NativeDemandCarrier::Shared(meter))
        })?;
        let registration_prefix = registration.work;
        let sync_prefix = sync.lock().work.items_examined;
        ledger.record_everything_prepared(prepared);
        assert_eq!(counted.0.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(registration.work, registration_prefix, "future visits belong to the original seal");
        assert_eq!(sync.lock().work.items_examined, sync_prefix + 3);
        assert!(ledger.observers.lock().expect("same hub").pending.is_none());
        assert_eq!(Arc::strong_count(&sync), 1, "every original delivery alias retired before loan restoration");
        Ok(())
    }

    #[test]
    fn denied_late_delivery_does_not_join_or_erase_original_publication()
    -> Result<(), Box<dyn std::error::Error>> {
        let ledger = Arc::new(ViewLedger::new());
        let mut budget = crate::WorkBudget::UNBOUNDED;
        budget.items_examined = 0;
        let sync = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(budget, &crate::CancellationToken::new()),
        )));
        let prepared = ledger.prepare_everything_notification(&sync)?;
        let observer: Arc<dyn ViewObserver> = Arc::new(Counted::default());
        let registration = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )));
        assert!(ledger.observe(
            Arc::downgrade(&observer), &mut super::super::adapter::NativeDemandCarrier::Shared(&registration),
        ).is_err());
        let ordinary_error = ledger.observe(Arc::downgrade(&observer), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()))
            .expect_err("ordinary registration must propagate the same original publication quota");
        assert!(matches!(ordinary_error.semantic_error(), super::super::MountSourceError::Work(_)));
        assert!(ledger.observers.lock().expect("same registry").observers.is_empty());
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
        ledger.record(&change, &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), &mut first)?;
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
        assert!(maps.iter().all(Option::is_some));
        drop(maps);
        ledger.record(&change, &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), &mut second)?;
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
        ledger.record(&ViewChange::Node(FileId::from_bytes([1; 16])), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), &mut loan)?;
        let ViewLedgerAdmission::Shared(proof) = &loan else { panic!("original shared proof"); };
        let (generation, bytes) = proof.lock().expect("original map proof")[super::AdmittedMap::Nodes as usize].expect("owned nodes");
        let live = work.live_allocation_bytes();
        for byte in 2..=8 {
            ledger.record(&ViewChange::Node(FileId::from_bytes([byte; 16])), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
        }
        assert!(ledger.nodes.1.load(Ordering::Acquire) > generation);
        work.record(crate::WorkCounters::UNCHARGED)?;
        assert_eq!(work.live_allocation_bytes(), live - bytes);
        assert!(proof.lock().expect("same loaned proof")[super::AdmittedMap::Nodes as usize].is_none());
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
        first_source.record(&ViewChange::Node(FileId::from_bytes([11; 16])), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), &mut first)?;
        second_source.record(&ViewChange::Node(FileId::from_bytes([12; 16])), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), &mut second)?;
        work.restore_view_ledger_admission(first_slot, first);
        let live = work.live_allocation_bytes();
        work.budget.items_examined = work.work.items_examined;
        assert!(matches!(first_source.record(&ViewChange::Node(FileId::from_bytes([13; 16])), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), &mut nested), Err(super::super::MountSourceError::Work(_))));
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
        ledger.record(&ViewChange::Node(file), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
        let checkpoint = ledger.stamp();
        let root = path(&[])?;
        let cancellation = crate::CancellationToken::new();
        let mut denied = crate::demand::DemandWork::new(crate::WorkBudget {
            items_examined: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(matches!(ledger.unchanged_since(&root, Some(file), checkpoint, &mut denied),
            Err(super::super::MountSourceError::Work(_))));
        assert_eq!(denied.work, crate::WorkCounters::UNCHARGED);
        let mut admitted = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        assert!(ledger.unchanged_since(&root, Some(file), checkpoint, &mut admitted)?);
        assert!(admitted.work.items_examined > 0);
        assert_eq!(admitted.work.allocation_operations, 0);
        assert_eq!(admitted.live_allocation_bytes(), 0);
        ledger.record(&ViewChange::Node(file), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
        assert!(!ledger.unchanged_since(&root, Some(file), checkpoint, &mut admitted)?);
        assert!(!ledger.unchanged_since(&root, Some(file), checkpoint, &mut super::unbounded_view_work()).expect("unbounded view query"));
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
        assert!(super::child_key(
            super::key_of(&()), &unicode.components()[0], &mut super::ViewRecordWork::Ordinary(&mut work),
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
        probe.record(&ViewChange::Node(first), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut admitted), &mut ViewLedgerAdmission::default())?;
        let ledger = ViewLedger::new();
        let before = ledger.stamp();
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget {
            items_examined: admitted.work.items_examined,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        let effect = ViewEffect { nodes: vec![first, second], ..ViewEffect::default() };
        let failure = ledger.record(&ViewChange::Effect(&effect), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), &mut ViewLedgerAdmission::default())
            .expect_err("the second key must not pass the first-key allowance");
        assert!(matches!(failure, super::super::MountSourceError::Work(_)));
        assert!(!ledger.node_unchanged_since(first, before, &mut super::unbounded_view_work()).expect("unbounded view query"));
        assert!(ledger.node_unchanged_since(second, before, &mut super::unbounded_view_work()).expect("unbounded view query"));
        assert_eq!(work.work, admitted.work);
        let untouched = ViewLedger::new();
        let mut denied = crate::demand::DemandWork::new(crate::WorkBudget {
            allocation_operations: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(untouched.record(&ViewChange::Node(first), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut denied), &mut ViewLedgerAdmission::default()).is_err());
        assert_eq!(denied.work.allocation_operations, 0);
        assert_eq!(denied.work.peak_allocation_bytes, 0);
        assert!(untouched.node_unchanged_since(first, before, &mut super::unbounded_view_work()).expect("unbounded view query"));
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
            ledger.record(&ViewChange::Bound(&created), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
            ledger.record(&ViewChange::Node(FileId::new()), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
        }
        assert!(ledger.unchanged_since(&watched, Some(file_id), stamp, &mut super::unbounded_view_work()).expect("unbounded view query"));
        assert!(ledger.unchanged_since(&path(&["corpus"])?, None, stamp, &mut super::unbounded_view_work()).expect("unbounded view query"));
        assert!(!ledger.unchanged_since(&path(&["writes"])?, None, stamp, &mut super::unbounded_view_work()).expect("unbounded view query"));
        ledger.record(&ViewChange::Node(file_id), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
        assert!(!ledger.unchanged_since(&watched, Some(file_id), stamp, &mut super::unbounded_view_work()).expect("unbounded view query"));
        Ok(())
    }

    /// A change reported under one spelling of a name reaches a lookup made under any other spelling a case- or normalization-insensitive host resolves to the same entry, whatever either name's encoding.
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
            (
                name(NameEncoding::PosixBytes, "\u{e9}t\u{e9}")?,
                name(NameEncoding::WindowsUtf16Le, "E\u{301}TE\u{301}")?,
            ),
        ];
        for (changed, looked_up) in spellings {
            let ledger = ViewLedger::new();
            let looked_up = under(looked_up)?;
            let stamp = ledger.stamp();
            ledger.record(&ViewChange::Bound(&under(changed)?), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
            assert!(!ledger.unchanged_since(&looked_up, None, stamp, &mut super::unbounded_view_work()).expect("unbounded view query"));
        }
        let ledger = ViewLedger::new();
        let other = under(name(NameEncoding::Utf8, "readme.txt")?)?;
        let stamp = ledger.stamp();
        ledger.record(&ViewChange::Bound(&under(name(
            NameEncoding::Utf8,
            "readme.md",
        )?)?), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
        assert!(
            ledger.unchanged_since(&other, None, stamp, &mut super::unbounded_view_work()).expect("unbounded view query"),
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
        ledger.record(&ViewChange::Bound(&under(raw(&[0xff, b'A'])?)?), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
        assert!(ledger.unchanged_since(&looked_up, None, stamp, &mut super::unbounded_view_work()).expect("unbounded view query"));
        Ok(())
    }

    #[test]
    fn folded_keys_invalidate_only_older_lookups() -> Result<(), Box<dyn std::error::Error>> {
        let ledger = ViewLedger::new();
        let watched = path(&["watched"])?;
        let before = ledger.stamp();
        for _ in 0..=MAXIMUM_TRACKED_KEYS {
            ledger.record(&ViewChange::Node(FileId::new()), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
        }
        let after = ledger.stamp();
        let untouched = FileId::new();
        assert!(
            !ledger.unchanged_since(&watched, Some(untouched), before, &mut super::unbounded_view_work()).expect("unbounded view query"),
            "a folded map may not vouch for lookups older than what it folded"
        );
        assert!(ledger.unchanged_since(&watched, Some(untouched), after, &mut super::unbounded_view_work()).expect("unbounded view query"));
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

        let ledger = Arc::new(ViewLedger::new());
        let recorded = Arc::new(Recorded::default());
        let observer: std::sync::Weak<Recorded> = Arc::downgrade(&recorded);
        ledger.observe(observer, &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()))?;
        let origin = ViewOrigin::new();
        ledger.record(&ViewChange::Everything, &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
        {
            let _scope = origin.enter();
            ledger.record(&ViewChange::Everything, &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
        }
        ledger.record(&ViewChange::Everything, &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut super::unbounded_view_work()), &mut ViewLedgerAdmission::default()).expect("unbounded view recording");
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
