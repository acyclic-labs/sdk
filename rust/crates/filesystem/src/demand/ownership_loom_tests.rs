use super::accounting;
use crate::sync::atomic as custody_atomics;
use std::cell::Cell;

loom::thread_local! {
    pub(super) static RETIRE_DIRTY_EARLY: Cell<bool> = Cell::new(false);
    static CHECK_PUBLISHER: Cell<bool> = Cell::new(false);
    static PUBLISHER_FUNDED: Cell<bool> = Cell::new(true);
}

#[cfg(feature = "native-watch")]
pub(super) fn check_publisher_cut(original: Option<&crate::NativeMaterializationWork>) {
    if CHECK_PUBLISHER.with(Cell::get) {
        let original = original.unwrap();
        PUBLISHER_FUNDED.with(|flag| {
            flag.set(
                original.live_allocation_bytes()
                    >= crate::NativeMaterializationWork::accounting_state_bytes()
                        + super::DIRTY_ALLOCATION_BYTES,
            )
        });
    }
}

#[cfg(feature = "native-watch")]
fn dirty_backing_lifetime(retire_early: bool) {
    crate::initialize_test_native_io();
    loom::model(move || {
        let mut original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .unwrap();
        let baseline = original.live_allocation_bytes();
        let custody = super::NativeMountAllocationCustody::new(&mut original).unwrap();
        let publisher = loom::thread::spawn(move || {
            CHECK_PUBLISHER.with(|flag| flag.set(true));
            drop(custody);
            CHECK_PUBLISHER.with(|flag| flag.set(false));
            PUBLISHER_FUNDED.with(Cell::get)
        });
        RETIRE_DIRTY_EARLY.with(|flag| flag.set(retire_early));
        original.charge(crate::WorkCounters::UNCHARGED).unwrap();
        RETIRE_DIRTY_EARLY.with(|flag| flag.set(false));
        let funded = publisher.join().unwrap();
        original.charge(crate::WorkCounters::UNCHARGED).unwrap();
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert!(funded, "dirty flag backing retired before publisher");
    });
}

#[cfg(feature = "native-watch")]
#[test]
fn dirty_flag_backing_survives_every_live_publisher() {
    dirty_backing_lifetime(false);
}

#[cfg(feature = "native-watch")]
#[test]
#[should_panic(expected = "dirty flag backing retired before publisher")]
fn retiring_dirty_flag_at_gauge_release_loses_backing() {
    dirty_backing_lifetime(true);
}

fn gauge_publication(publish_before_release: bool, final_owner: bool) {
    use custody_atomics::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    loom::model(move || {
        // std Arc only pins storage. An explicit modeled reference count
        // matches std Arc's relaxed strong_count (Loom Arc inspects SeqCst).
        let first = Arc::new((AtomicUsize::new(2), AtomicU64::new(5)));
        let dirty = Arc::new(AtomicBool::new(true));
        let published = (first.clone(), dirty.clone());
        let publisher = loom::thread::spawn(move || {
            let ((owners, bytes), dirty) = (&*published.0, &*published.1);
            if final_owner {
                if publish_before_release {
                    accounting::publish(dirty);
                    owners.fetch_sub(1, Ordering::Release);
                } else {
                    accounting::retire(dirty, || {
                        owners.fetch_sub(1, Ordering::Release);
                    });
                }
            } else {
                if publish_before_release {
                    accounting::retire_bytes::<false>(dirty, bytes, 2, 0).unwrap();
                } else {
                    accounting::retire_bytes::<true>(dirty, bytes, 2, 0).unwrap();
                }
            }
        });
        // A second independent publisher can overwrite the first's dirty bit.
        // The release sequence must carry both owners' updates to consumption.
        let second = Arc::new((AtomicUsize::new(2), AtomicU64::new(5)));
        accounting::retire(&dirty, || {
            second.0.fetch_sub(1, Ordering::Release);
        });
        let mut entries = [(first, 5u64), (second, 5u64)];
        let refused = accounting::reconcile(
            &dirty,
            &mut entries,
            |entry| accounting::retained(entry.0.load(Ordering::Relaxed), &entry.1),
            |_, _| Err::<(), _>("ledger refused"),
        );
        assert_eq!(refused, Err("ledger refused"));
        assert_eq!(
            entries.each_ref().map(|(_, paid)| *paid),
            [5, 5],
            "refusal acknowledged unpaid retirement"
        );
        assert!(
            dirty.load(Ordering::Acquire),
            "refusal lost its dirty event"
        );
        let mut consume = || {
            accounting::reconcile(
                &dirty,
                &mut entries,
                |entry| accounting::retained(entry.0.load(Ordering::Relaxed), &entry.1),
                |paid, actual| {
                    assert!(actual <= paid);
                    Ok::<_, ()>(())
                },
            )
            .unwrap();
        };
        consume();
        publisher.join().unwrap();
        consume();
        assert_eq!(
            entries.map(|(_, paid)| paid),
            [if final_owner { 0 } else { 3 }, 0],
            "retirement accounting was stranded"
        );
    });
}
#[test]
fn publishers_cannot_strand_final_retirement() {
    gauge_publication(false, true);
}
#[test]
fn publishers_cannot_strand_partial_retirement() {
    gauge_publication(false, false);
}
#[test]
#[should_panic(expected = "retirement accounting was stranded")]
fn publishing_cleanup_before_releasing_the_owner_loses_retirement() {
    gauge_publication(true, true);
}

#[test]
#[should_panic(expected = "retirement accounting was stranded")]
fn publishing_cleanup_before_partial_retirement_loses_bytes() {
    gauge_publication(true, false);
}
