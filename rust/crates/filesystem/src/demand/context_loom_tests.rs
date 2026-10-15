use super::DemandWork;
use loom::sync::{Mutex, MutexGuard};
use std::cell::Cell;
use std::ops::{Deref, DerefMut};

// One original leaf per model. Ordinary tests retain their normal std mutexes.
// This guard instruments the real lock()/custody/allocation destructor path;
// it does not substitute a second implementation of demand accounting.
loom::lazy_static! {
    static ref LEAF: Mutex<()> = Mutex::new(());
}
thread_local! {
    static MODEL: Cell<bool> = const { Cell::new(false) };
}
pub(super) fn before_leaf() -> Option<MutexGuard<'static, ()>> {
    MODEL.with(|active| active.get().then(|| LEAF.lock().unwrap()))
}
pub(super) struct LeafGuard<G> {
    state: G,
    _model: Option<MutexGuard<'static, ()>>,
}
impl<G> LeafGuard<G> {
    pub(super) fn new(state: G, model: Option<MutexGuard<'static, ()>>) -> Self {
        Self {
            state,
            _model: model,
        }
    }
}
impl<G: Deref<Target = DemandWork>> Deref for LeafGuard<G> {
    type Target = DemandWork;
    fn deref(&self) -> &DemandWork {
        &self.state
    }
}
impl<G: DerefMut<Target = DemandWork>> DerefMut for LeafGuard<G> {
    fn deref_mut(&mut self) -> &mut DemandWork {
        &mut self.state
    }
}
#[cfg(feature = "native-watch")]
fn modeled<T>(operation: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            MODEL.with(|active| active.set(self.0));
        }
    }
    // Loom logical threads may share an OS thread. The parent model scope
    // outlives its joined children; each restores the host thread's prior flag.
    let _reset = Reset(MODEL.with(|active| active.replace(true)));
    operation()
}
#[cfg(feature = "native-watch")]
fn read_and_invalidate(legacy_inspection: bool) {
    use super::NativeMountAllocationCustody;
    use crate::{CancellationToken, NativeMaterializationWork, WorkCounters};
    use loom::sync::Condvar;
    use std::sync::Arc;
    crate::initialize_test_native_io();
    loom::model(move || {
        modeled(|| {
            let budget = crate::performance::BOUNDARY_BUDGET;
            let original = NativeMaterializationWork::new(budget, &CancellationToken::new())
                .map_err(|(error, _)| error)
                .unwrap();
            let mut resource = original.native_volume_work(budget).unwrap();
            let custody = NativeMountAllocationCustody::new(&mut resource).unwrap();
            resource
                .with_original_leaf(|work| custody.claim(37, work))
                .unwrap();
            // A real retirement notification admits the reconciliation scan.
            custody.retire(1).unwrap();
            let expected_items = original.receipt().items_examined + 1;
            let weak = Arc::downgrade(&custody);
            // Custody is now the sole owner of resource metadata. Its destruction
            // really calls NativeAllocation::drop -> release -> original leaf.
            drop(resource);
            // Callback result mutex: acquired only after dropping leaf, and
            // released before any demand call or joining invalidation.
            let complete = Arc::new((Mutex::new(None), Condvar::new()));
            let read_complete = complete.clone();
            let read_work = original.clone();
            let read = loom::thread::spawn(move || {
                modeled(|| {
                    let items = read_work.with_original_leaf(|work| {
                        if legacy_inspection {
                            // The previous mount_bytes implementation. Yield while its
                            // temporary Arc pins custody to explore final-owner release.
                            let owner = weak.upgrade();
                            loom::thread::yield_now();
                            std::hint::black_box(owner.map_or(0, |owner| owner.retained_bytes()));
                        } else {
                            loom::thread::yield_now();
                        }
                        work.charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })
                        .unwrap();
                        work.work.items_examined
                    });
                    // readdirplus waits for the source-read result without owning leaf.
                    let (result, ready) = &*read_complete;
                    *result.lock().unwrap() = Some(items);
                    ready.notify_one();
                })
            });
            let invalidation_work = original.clone();
            let invalidation = loom::thread::spawn(move || {
                modeled(|| {
                    drop(custody);
                    invalidation_work.charge(WorkCounters::UNCHARGED).unwrap();
                })
            });
            let (result, ready) = &*complete;
            let mut result = result.lock().unwrap();
            while result.is_none() {
                result = ready.wait(result).unwrap();
            }
            assert_eq!(*result, Some(expected_items));
            drop(result);
            read.join().unwrap();
            invalidation.join().unwrap();
            original.charge(WorkCounters::UNCHARGED).unwrap();
            assert_eq!(
                original.live_allocation_bytes(),
                NativeMaterializationWork::accounting_state_bytes()
            );
            let spent = original.receipt();
            assert_eq!(spent.items_examined, expected_items);
            spent.verify(budget).unwrap();
        })
    });
}
#[cfg(feature = "native-watch")]
#[test]
fn custody_inspection_completes_read_callback_and_invalidation() {
    read_and_invalidate(false);
}
#[cfg(feature = "native-watch")]
#[test]
#[should_panic(expected = "deadlock")]
fn previous_custody_inspection_deadlocks_the_real_leaf() {
    read_and_invalidate(true);
}

// Share the production gauge protocol, including retirement publication order.
// An explicit modeled reference count is necessary: Loom Arc::strong_count
// uses SeqCst inspection, whereas std Arc uses a relaxed load.
use loom::sync::atomic as custody_atomics;
#[path = "ownership_accounting.rs"]
mod accounting;

fn gauge_publication(publish_before_release: bool) {
    use custody_atomics::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    loom::model(move || {
        let gauge = Arc::new((
            AtomicUsize::new(2),
            AtomicU64::new(5),
            AtomicBool::new(true),
        ));
        let published = gauge.clone();
        let publisher = loom::thread::spawn(move || {
            let (owners, bytes, dirty) = &*published;
            bytes.fetch_sub(2, Ordering::AcqRel);
            dirty.store(true, Ordering::Release);
            if publish_before_release {
                dirty.store(true, Ordering::Release);
                owners.fetch_sub(1, Ordering::Release);
            } else {
                accounting::retire(dirty, || {
                    owners.fetch_sub(1, Ordering::Release);
                });
            }
        });
        let (owners, bytes, dirty) = &*gauge;
        let mut paid = 5;
        if dirty.swap(false, Ordering::AcqRel) {
            paid = accounting::retained(owners.load(Ordering::Relaxed), bytes);
            assert!(paid <= 5);
        }
        publisher.join().unwrap();
        // The next admitted operation must observe the final retirement, even
        // if the preceding operation consumed cleanup during invalidation.
        if dirty.swap(false, Ordering::AcqRel) {
            paid = accounting::retained(owners.load(Ordering::Relaxed), bytes);
        }
        assert_eq!(paid, 0);
    });
}
#[test]
fn gauge_cleanup_publication_cannot_strand_retirement() {
    gauge_publication(false);
}
#[test]
#[should_panic(expected = "assertion")]
fn publishing_cleanup_before_releasing_the_owner_loses_retirement() {
    gauge_publication(true);
}
