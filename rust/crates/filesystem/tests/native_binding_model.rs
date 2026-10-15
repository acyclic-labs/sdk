//! Exhaustive models of the production cached binding decision.

#[path = "../src/native_mount/cached_binding.rs"]
mod cached_binding;

#[cfg(all(not(kani), target_os = "linux"))]
mod interleavings {
    use super::cached_binding::cached_binding_matches;
    use loom::sync::atomic::{AtomicU8, Ordering};
    use loom::sync::{Arc, Mutex, RwLock};
    use loom::thread;

    #[test]
    fn cache_copy_source_lease_and_publication_cannot_deadlock_or_accept_torn_state() {
        for related in [false, true] {
            for reports in [false, true] {
                loom::model(move || {
                    // Source lease precedes cache for a writer. The reader must
                    // release cache before requesting that lease (real helper).
                    let gate = Arc::new(RwLock::new(()));
                    let cache = Arc::new(Mutex::new(Some((0_u8, 0_u8))));
                    let stamp = Arc::new(AtomicU8::new(0));
                    let epoch = Arc::new(AtomicU8::new(0));
                    let writer = {
                        let (gate, cache, stamp, epoch) =
                            (gate.clone(), cache.clone(), stamp.clone(), epoch.clone());
                        thread::spawn(move || {
                            let _lease = gate.write().unwrap();
                            stamp.store(1, Ordering::Release);
                            epoch.store(u8::from(related), Ordering::Release);
                            *cache.lock().unwrap() = None;
                        })
                    };
                    let reader = thread::spawn(move || {
                        // RwLock supplies the existing source lease contract;
                        // production owns the lookup/lease/decision ordering.
                        let sample = || {
                            let observed =
                                (stamp.load(Ordering::Acquire), epoch.load(Ordering::Acquire));
                            assert!(observed == (0, 0) || observed == (1, u8::from(related)));
                            observed
                        };
                        cached_binding_matches(
                            || *cache.lock().unwrap(),
                            || Ok::<_, ()>(gate.read().unwrap()),
                            |expected| reports && sample() == expected,
                            |expected| {
                                let observed = sample();
                                observed.1 == expected.1 && (observed.0 == expected.0 || !related)
                            },
                        );
                    });
                    reader.join().unwrap();
                    writer.join().unwrap();
                });
            }
        }
    }
}

#[cfg(kani)]
mod proofs {
    use super::cached_binding::cached_binding_matches;
    use std::cell::Cell;

    struct Lease<'a>(&'a Cell<bool>);

    impl Drop for Lease<'_> {
        fn drop(&mut self) {
            self.0.set(false);
        }
    }

    #[kani::proof]
    #[kani::unwind(1)]
    fn decision_requires_a_binding_and_lease_and_checks_each_path_at_most_once() {
        let present: bool = kani::any();
        let admitted: bool = kani::any();
        let current: bool = kani::any();
        let unchanged: bool = kani::any();
        let live = Cell::new(false);
        let lease_calls = Cell::new(0_u8);
        let current_calls = Cell::new(0_u8);
        let fallback_calls = Cell::new(0_u8);
        let matched = cached_binding_matches(
            || present.then_some(()),
            || {
                lease_calls.set(lease_calls.get() + 1);
                live.set(admitted);
                if admitted { Ok(Lease(&live)) } else { Err(()) }
            },
            |()| {
                assert!(live.get());
                current_calls.set(current_calls.get() + 1);
                current
            },
            |()| {
                assert!(live.get());
                fallback_calls.set(fallback_calls.get() + 1);
                unchanged
            },
        );
        assert_eq!(matched, present && admitted && (current || unchanged));
        assert_eq!(lease_calls.get(), u8::from(present));
        assert_eq!(current_calls.get(), u8::from(present && admitted));
        assert_eq!(
            fallback_calls.get(),
            u8::from(present && admitted && !current)
        );
        assert!(!live.get());
    }
}
