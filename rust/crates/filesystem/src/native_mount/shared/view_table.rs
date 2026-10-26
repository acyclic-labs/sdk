//! The views served by one shared native mount instance.
//!
//! A process runs one native instance per backend (one FUSE session, one
//! `ProjFS` virtualization root, one loopback NFS mount). Every task view and
//! volume projection is a namespace inside it, identified by a serial that is
//! never reused. Kernel-held identities (inodes, handles, file ids) or the
//! view's name carry the serial, and every callback resolves it through
//! [`ViewTable::enter`], the one choke point between the shared transport and
//! a view's core:
//!
//! - a callback runs only while it holds a [`ViewLease`] on a live view;
//! - a serial that is retired, or was never issued, resolves to nothing, so a
//!   late callback fails as stale and never reaches another view's core;
//! - retirement removes the view from the table first, so no new lease can be
//!   admitted, then waits for every admitted lease to drain, and only then
//!   drops the core. The wait blocks on a condition variable; nothing polls;
//! - only the [`ViewOwner`] returned by [`ViewTable::insert`] can retire a
//!   view, and dropping it retires the view, so every exit path releases it.
//!
//! Retirement must not run from a callback that holds a lease on the same
//! view; the harness retires views from its own threads.

use crate::sync::native::{Arc, Condvar, Mutex, RwLock};
use std::collections::HashMap;
use std::sync::PoisonError;

/// Serial of a view in one shared instance. Serials start at 1 and are never
/// reused for the lifetime of the table.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ViewSerial(u64);

impl ViewSerial {
    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

/// A serial issued once by [`ViewTable::reserve`] and not yet listed.
pub(crate) struct ReservedSerial<C> {
    serial: u64,
    table: Arc<ViewTable<C>>,
}

impl<C> ReservedSerial<C> {
    pub(crate) const fn get(&self) -> u64 {
        self.serial
    }
}

/// The table has issued every serial its identity encoding can carry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ViewSerialsExhausted;

pub(crate) struct ViewTable<C> {
    views: RwLock<Views<C>>,
    max_serial: u64,
}

struct Views<C> {
    next: u64,
    live: HashMap<u64, Live<C>>,
}

struct Live<C> {
    core: Arc<C>,
    drain: Arc<Drain>,
}

struct Drain {
    state: Mutex<DrainState>,
    drained: Condvar,
}

struct DrainState {
    leases: usize,
    retiring: bool,
}

impl Drain {
    fn new() -> Self {
        Self {
            state: Mutex::new(DrainState {
                leases: 0,
                retiring: false,
            }),
            drained: Condvar::new(),
        }
    }
}

impl<C> ViewTable<C> {
    /// A table that issues serials `1..=max_serial`.
    pub(crate) fn new(max_serial: u64) -> Self {
        assert!(max_serial > 0, "a view table issues at least one serial");
        Self {
            views: RwLock::new(Views {
                next: 1,
                live: HashMap::new(),
            }),
            max_serial,
        }
    }

    /// Adds a view and returns its only owner.
    #[cfg(any(test, acyclic_fs_loom))]
    pub(crate) fn insert(table: &Arc<Self>, core: C) -> Result<ViewOwner<C>, ViewSerialsExhausted> {
        let serial = table.reserve()?;
        Ok(Self::insert_reserved(table, serial, core))
    }

    /// Issues the next serial without listing a view, so a core can be built
    /// for it first. A reservation that is never inserted is never reissued.
    pub(crate) fn reserve(self: &Arc<Self>) -> Result<ReservedSerial<C>, ViewSerialsExhausted> {
        let mut views = self.views.write().unwrap_or_else(PoisonError::into_inner);
        let serial = views.next;
        if serial > self.max_serial {
            return Err(ViewSerialsExhausted);
        }
        views.next = serial + 1;
        Ok(ReservedSerial {
            serial,
            table: Arc::clone(self),
        })
    }

    /// Lists a view under its reserved serial and returns its only owner.
    pub(crate) fn insert_reserved(
        table: &Arc<Self>,
        serial: ReservedSerial<C>,
        core: C,
    ) -> ViewOwner<C> {
        let ReservedSerial {
            serial,
            table: reserved_table,
        } = serial;
        assert!(
            Arc::ptr_eq(table, &reserved_table),
            "a reservation belongs to its issuing table"
        );
        drop(reserved_table);
        let mut views = table.views.write().unwrap_or_else(PoisonError::into_inner);
        assert!(serial < views.next, "a reservation comes from this table");
        let previous = views.live.insert(
            serial,
            Live {
                core: Arc::new(core),
                drain: Arc::new(Drain::new()),
            },
        );
        assert!(previous.is_none(), "view serials are never reused");
        ViewOwner {
            table: Some(Arc::clone(table)),
            serial: ViewSerial(serial),
        }
    }

    /// Admits a callback to a live view, or returns `None` for a serial that
    /// is retired or was never issued.
    pub(crate) fn enter(&self, serial: u64) -> Option<ViewLease<C>> {
        let views = self.views.read().unwrap_or_else(PoisonError::into_inner);
        let live = views.live.get(&serial)?;
        // Admission happens under the table's read lock, which retirement's
        // removal excludes: a view is either still listed and its lease is
        // counted, or it is gone and nothing is admitted.
        let mut state = live
            .drain
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        debug_assert!(!state.retiring, "a retiring view is never listed");
        state.leases += 1;
        drop(state);
        Some(ViewLease {
            core: Arc::clone(&live.core),
            _count: LeaseCount {
                drain: Arc::clone(&live.drain),
            },
        })
    }

    /// Serials of the live views, in issue order.
    #[cfg(any(test, acyclic_fs_loom))]
    pub(crate) fn serials(&self) -> Vec<ViewSerial> {
        let views = self.views.read().unwrap_or_else(PoisonError::into_inner);
        let mut serials: Vec<_> = views.live.keys().copied().map(ViewSerial).collect();
        serials.sort_unstable();
        serials
    }

    #[cfg(any(test, acyclic_fs_loom))]
    pub(crate) fn len(&self) -> usize {
        self.views
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .live
            .len()
    }

    fn retire(&self, serial: ViewSerial) -> Option<C> {
        let Some(Live { core, drain }) = self
            .views
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .live
            .remove(&serial.0)
        else {
            unreachable!("a view owner retires its own listed view once");
        };
        let mut state = drain.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.retiring = true;
        #[cfg(acyclic_fs_loom)]
        let skip = faults::RETIRE_WITHOUT_DRAIN.with(std::cell::Cell::get);
        #[cfg(not(acyclic_fs_loom))]
        let skip = false;
        if !skip {
            while state.leases != 0 {
                state = drain
                    .drained
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
        drop(state);
        match Arc::try_unwrap(core) {
            Ok(core) => Some(core),
            // Only the broken model returns before its leases drain; the last
            // lease then drops the core.
            Err(_) if skip => None,
            Err(_) => unreachable!("every lease releases its core before it is counted out"),
        }
    }
}

/// The only handle that can retire a view. Dropping it retires the view.
pub(crate) struct ViewOwner<C> {
    table: Option<Arc<ViewTable<C>>>,
    serial: ViewSerial,
}

impl<C> ViewOwner<C> {
    pub(crate) fn serial(&self) -> ViewSerial {
        self.serial
    }

    /// Retires the view, waiting for in-flight callbacks, and returns its core.
    pub(crate) fn retire(mut self) -> C {
        let Some(table) = self.table.take() else {
            unreachable!("an owner retires once");
        };
        match table.retire(self.serial) {
            Some(core) => core,
            None => unreachable!("retirement drains every lease"),
        }
    }
}

impl<C> Drop for ViewOwner<C> {
    fn drop(&mut self) {
        if let Some(table) = self.table.take() {
            drop(table.retire(self.serial));
        }
    }
}

/// A callback's admission to one live view. Retirement waits for it.
pub(crate) struct ViewLease<C> {
    // Drop the core before counting this lease out; a retiring owner observing
    // zero leases then owns the last core reference, without an empty state.
    core: Arc<C>,
    _count: LeaseCount,
}

struct LeaseCount {
    drain: Arc<Drain>,
}

impl<C> std::ops::Deref for ViewLease<C> {
    type Target = C;

    fn deref(&self) -> &C {
        &self.core
    }
}

impl Drop for LeaseCount {
    fn drop(&mut self) {
        let mut state = self
            .drain
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.leases -= 1;
        let wake = state.retiring && state.leases == 0;
        drop(state);
        if wake {
            self.drain.drained.notify_all();
        }
    }
}

#[cfg(acyclic_fs_loom)]
pub(crate) mod faults {
    use std::cell::Cell;

    loom::thread_local! {
        /// Negative control: retirement returns without waiting for leases.
        pub(crate) static RETIRE_WITHOUT_DRAIN: Cell<bool> = Cell::new(false);
    }
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Core {
        drops: Arc<AtomicUsize>,
        id: u32,
    }

    impl Drop for Core {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn core(id: u32, drops: &Arc<AtomicUsize>) -> Core {
        Core {
            drops: Arc::clone(drops),
            id,
        }
    }

    #[test]
    fn lease_releases_its_core_before_counting_out() {
        struct Checked(Arc<Drain>);
        impl Drop for Checked {
            fn drop(&mut self) {
                assert_eq!(
                    self.0
                        .state
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .leases,
                    1,
                    "core must drop before lease counts out"
                );
            }
        }
        let drain = Arc::new(Drain::new());
        drain
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .leases = 1;
        let lease = ViewLease {
            core: Arc::new(Checked(Arc::clone(&drain))),
            _count: LeaseCount {
                drain: Arc::clone(&drain),
            },
        };
        drop(lease);
        assert_eq!(
            drain
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .leases,
            0
        );
    }

    #[test]
    fn serials_are_issued_once_and_retired_serials_stay_gone() {
        let drops = Arc::new(AtomicUsize::new(0));
        let table = Arc::new(ViewTable::new(u64::MAX));
        let first = ViewTable::insert(&table, core(1, &drops)).unwrap();
        let first_serial = first.serial();
        assert_eq!(table.enter(first_serial.get()).unwrap().id, 1);
        drop(first);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(table.enter(first_serial.get()).is_none());
        let second = ViewTable::insert(&table, core(2, &drops)).unwrap();
        assert!(second.serial() > first_serial);
        assert!(table.enter(first_serial.get()).is_none());
        assert_eq!(table.enter(second.serial().get()).unwrap().id, 2);
        assert!(table.enter(0).is_none());
        assert!(table.enter(u64::MAX).is_none());
        assert_eq!(table.serials(), vec![second.serial()]);
        // A reservation that is abandoned is skipped, not reissued.
        let abandoned = table.reserve().unwrap().get();
        let third = ViewTable::insert(&table, core(3, &drops)).unwrap();
        assert!(third.serial().get() > abandoned);
        assert!(table.enter(abandoned).is_none());
    }

    #[test]
    fn views_are_independent() {
        let drops = Arc::new(AtomicUsize::new(0));
        let table = Arc::new(ViewTable::new(u64::MAX));
        let a = ViewTable::insert(&table, core(1, &drops)).unwrap();
        let b = ViewTable::insert(&table, core(2, &drops)).unwrap();
        let lease_b = table.enter(b.serial().get()).unwrap();
        assert_eq!(a.retire().id, 1);
        assert_eq!(lease_b.id, 2);
        assert_eq!(table.len(), 1);
        drop(lease_b);
        drop(b);
        assert_eq!(table.len(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn exhausted_serials_are_refused_not_reused() {
        let drops = Arc::new(AtomicUsize::new(0));
        let table = Arc::new(ViewTable::new(2));
        let a = ViewTable::insert(&table, core(1, &drops)).unwrap();
        drop(ViewTable::insert(&table, core(2, &drops)).unwrap());
        assert_eq!(
            ViewTable::insert(&table, core(3, &drops)).err(),
            Some(ViewSerialsExhausted)
        );
        assert_eq!(table.serials(), vec![a.serial()]);
    }

    #[test]
    fn reservation_cannot_enter_another_table_with_the_same_counter() {
        let first = Arc::new(ViewTable::<u32>::new(2));
        let second = Arc::new(ViewTable::<u32>::new(2));
        let reservation = first.reserve().unwrap();
        drop(second.reserve().unwrap());
        assert!(
            std::panic::catch_unwind(|| { ViewTable::insert_reserved(&second, reservation, 7) })
                .is_err(),
            "a reservation must not cross instance identity"
        );
        assert_eq!(first.len(), 0);
        assert_eq!(second.len(), 0);
    }

    #[test]
    fn retirement_waits_for_an_in_flight_lease() {
        let drops = Arc::new(AtomicUsize::new(0));
        let table = Arc::new(ViewTable::new(u64::MAX));
        let owner = ViewTable::insert(&table, core(1, &drops)).unwrap();
        let lease = table.enter(owner.serial().get()).unwrap();
        let (entered, release) = std::sync::mpsc::channel::<()>();
        let retirer = std::thread::spawn(move || {
            let core = owner.retire();
            entered.send(()).unwrap();
            core.id
        });
        // The retirer cannot finish while the lease is held.
        assert!(
            release
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err()
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(lease.id, 1);
        drop(lease);
        assert_eq!(retirer.join().unwrap(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
