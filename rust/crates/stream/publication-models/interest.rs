use super::*;
use crate::probe::probe;
use loom::sync::atomic::{AtomicUsize, Ordering};
use std::{future::Future, task::Context};

fn park(interest: &mut Interest<u8>, wake: &std::task::Waker) {
    let mut next = Box::pin(interest.next(async || Ok(None::<()>)));
    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(wake))
            .is_pending()
    );
}

#[test]
fn publication_only_wakes_its_interested_followers() {
    loom::model(|| {
        let registry = Interests::new();
        let mut probes = Vec::new();
        let mut interests = Vec::new();
        for key in 0..3 {
            let mut interest = registry.subscribe(key).unwrap();
            let (count, wake) = probe();
            park(&mut interest, &wake);
            probes.push(count);
            interests.push(interest);
        }
        registry.notify(&1).unwrap();
        for (key, count) in probes.iter().enumerate() {
            assert_eq!(count.count.load(Ordering::SeqCst), usize::from(key == 1));
        }
        drop(interests);
        assert!(registry.entries.lock().unwrap().is_empty());
    });
}

fn cancellation(broken: bool) {
    loom::model(move || {
        let registry = Interests::new();
        let mut cancelled = registry.subscribe(0).unwrap();
        let mut kept = registry.subscribe(0).unwrap();
        let (_, cancelled_wake) = probe();
        let (count, wake) = probe();
        park(&mut cancelled, &cancelled_wake);
        park(&mut kept, &wake);
        let removing = registry.clone();
        let cancel = loom::thread::spawn(move || {
            if broken {
                let removed = removing.entries.lock().unwrap().remove(&0);
                drop(removed);
            }
            drop(cancelled);
        });
        registry.notify(&0).unwrap();
        cancel.join().unwrap();
        assert_eq!(
            count.count.load(Ordering::SeqCst),
            1,
            "cancellation removed another interest's publication"
        );
        drop(kept);
        assert!(registry.entries.lock().unwrap().is_empty());
    });
}
#[test]
fn cancelling_an_interest_preserves_its_sibling() {
    cancellation(false);
}
#[test]
#[should_panic(expected = "cancellation removed another interest's publication")]
fn interest_negative_control_removes_the_shared_entry() {
    cancellation(true);
}

#[test]
#[should_panic(expected = "wake must run outside the publication lock")]
fn lock_order_negative_control_subscribes_under_the_registry_lock() {
    loom::model(|| {
        let registry = Interests::new();
        let _interest = registry.subscribe(0).unwrap();
        let entries = registry.entries.lock().unwrap();
        let violation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            entries.get(&0).unwrap().0.subscribe().unwrap();
        }));
        drop(entries);
        if let Err(violation) = violation {
            std::panic::resume_unwind(violation);
        }
    });
}

fn refresh_race(broken: bool) {
    loom::model(move || {
        let refresh = Arc::new(Refresh::new());
        let published = Arc::new(AtomicUsize::new(1));
        let applied = Arc::new(AtomicUsize::new(0));
        assert!(refresh.request(true).unwrap());
        let drain = |refresh: &Refresh, published: &AtomicUsize, applied: &AtomicUsize| {
            while let Some(force) = refresh.take().unwrap() {
                if force {
                    applied.store(published.load(Ordering::SeqCst), Ordering::SeqCst);
                }
            }
        };
        let writer = loom::thread::spawn({
            let (refresh, published, applied) =
                (refresh.clone(), published.clone(), applied.clone());
            move || {
                published.store(2, Ordering::SeqCst);
                let start = if broken {
                    let mut state = refresh.state.lock().unwrap();
                    if state.running {
                        false
                    } else {
                        state.dirty = true;
                        state.force = true;
                        state.running = true;
                        true
                    }
                } else {
                    refresh.request(true).unwrap()
                };
                if start {
                    drain(&refresh, &published, &applied);
                }
            }
        });
        drain(&refresh, &published, &applied);
        writer.join().unwrap();
        assert_eq!(
            applied.load(Ordering::SeqCst),
            2,
            "refresh lost an overlapping publication"
        );
    });
}
#[test]
fn refresh_handoff_does_not_lose_an_overlapping_publication() {
    refresh_race(false);
}
#[test]
#[should_panic(expected = "refresh lost an overlapping publication")]
fn refresh_negative_control_ignores_requests_while_running() {
    refresh_race(true);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    #[test]
    fn entries_exist_exactly_while_a_subscriber_owns_them(
        actions in proptest::collection::vec((0usize..8, proptest::prelude::any::<bool>(), 0u8..4), 0..16),
    ) {
        loom::model(move || {
            let registry = Interests::new();
            let mut live: Vec<Option<Interest<u8>>> = (0..8).map(|_| None).collect();
            for &(slot, add, key) in &actions {
                if add && live[slot].is_none() { live[slot] = Some(registry.subscribe((slot % 4) as u8).unwrap()); }
                if !add { live[slot] = None; }
                registry.notify(&key).unwrap();
                let mut expected = BTreeMap::<u8, usize>::new();
                for entry in live.iter().flatten() { *expected.entry(entry.key).or_default() += 1; }
                let actual = registry.entries.lock().unwrap().iter().map(|(key, (_, count))| (*key, *count)).collect::<BTreeMap<_, _>>();
                assert_eq!(actual, expected);
            }
            drop(live);
            registry.notify_all().unwrap();
            assert!(registry.entries.lock().unwrap().is_empty());
        });
    }
}
