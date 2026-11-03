use super::accounting;
use crate::sync::atomic as custody_atomics;

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
