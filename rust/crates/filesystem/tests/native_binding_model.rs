//! Exhaustive models of the production cached binding decision.
#![cfg(not(target_arch = "wasm32"))]

#[path = "../src/native_mount/cached_binding.rs"]
mod cached_binding;
#[path = "../src/sync.rs"]
mod sync;

#[cfg(acyclic_fs_loom)]
#[path = "../src/native_mount/projfs/probe_guard.rs"]
mod probe_guard;

#[cfg(acyclic_fs_loom)]
mod probe_registration {
    use super::probe_guard::{ProbeGuard, with_guard};
    use crate::sync::{Arc, Mutex};
    use loom::thread;
    use std::collections::HashMap;

    #[test]
    fn live_io_retains_its_registration_without_holding_the_map_lock() {
        check(false);
    }

    #[test]
    #[should_panic(expected = "metadata I/O lost its probe registration")]
    fn dropping_the_registration_before_io_is_detected() {
        check(true);
    }

    fn check(drop_early: bool) {
        for same_path in [false, true] {
            loom::model(move || {
                let probes = Arc::new(Mutex::new(HashMap::new()));
                let mut threads = Vec::new();
                for index in 0_u8..2 {
                    let probes = probes.clone();
                    threads.push(thread::spawn(move || {
                        let path = if same_path { 0 } else { index };
                        with_guard(
                            || {
                                let guard = ProbeGuard::enter(&probes, &path);
                                if drop_early {
                                    drop(guard);
                                    None
                                } else {
                                    Some(guard)
                                }
                            },
                            || {
                                assert!(
                                    ProbeGuard::is_active(&probes, &path, u8::eq),
                                    "metadata I/O lost its probe registration"
                                );
                                let active = probes.lock().unwrap();
                                assert!((1..=2).contains(&active[&path]));
                            },
                        );
                    }));
                }
                for thread in threads {
                    thread.join().unwrap();
                }
                assert!(probes.lock().unwrap().is_empty());
            });
        }
    }
}

#[cfg(all(acyclic_fs_loom, not(kani)))]
mod interleavings {
    use super::cached_binding::cached_binding_matches;
    use crate::sync::atomic::{AtomicU8, Ordering};
    use crate::sync::{Arc, Mutex, RwLock};
    use loom::thread;
    use std::cell::Cell;

    #[test]
    fn cache_copy_source_lease_and_publication_cannot_deadlock_or_accept_torn_state() {
        check_decisions(false);
    }

    #[test]
    #[should_panic(expected = "cached binding disagrees with the leased source state")]
    fn accepting_a_stale_binding_is_detected() {
        check_decisions(true);
    }

    #[test]
    #[should_panic(expected = "projection cache must be released before source lease")]
    fn holding_the_projection_lock_while_acquiring_the_source_is_detected() {
        loom::model(|| {
            let cache = Mutex::new(Some(()));
            let guard = cache.lock().unwrap();
            // Deliberately broken wrapper keeps its cache guard through admission.
            let _ = super::cached_binding::cached_binding_decision(
                *guard,
                || {
                    assert!(
                        cache.try_lock().is_ok(),
                        "projection cache must be released before source lease"
                    );
                    Ok::<_, ()>(())
                },
                |()| true,
                |()| false,
            );
        });
    }

    fn check_decisions(accept_stale: bool) {
        // No/unrelated/related changes, independently of binding replacement.
        for (changed, related, rebound) in [
            (false, false, false),
            (true, false, false),
            (true, true, false),
            (false, false, true),
            (true, false, true),
            (true, true, true),
        ] {
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
                            if changed || rebound {
                                stamp.store(u8::from(changed), Ordering::Release);
                                epoch.store(u8::from(rebound), Ordering::Release);
                                *cache.lock().unwrap() = None;
                            }
                        })
                    };
                    {
                        // RwLock supplies the existing source lease contract;
                        // production owns the lookup/lease/decision ordering.
                        let sample = || {
                            let observed =
                                (stamp.load(Ordering::Acquire), epoch.load(Ordering::Acquire));
                            assert!(
                                observed == (0, 0)
                                    || observed == (u8::from(changed), u8::from(rebound))
                            );
                            observed
                        };
                        // Extend the model lease through the oracle so a
                        // writer cannot change the state being compared with
                        // the returned decision. Kani checks its real lifetime.
                        let mut lease = None;
                        let fallback = Cell::new(false);
                        let mut acquire = || {
                            lease = Some(gate.read().unwrap());
                            Ok::<_, ()>(())
                        };
                        let current = |expected| {
                            reports
                                && (accept_stale && (related || rebound) || sample() == expected)
                        };
                        let unchanged = |expected: (u8, u8)| {
                            fallback.set(true);
                            let observed = sample();
                            observed.1 == expected.1 && (observed.0 == expected.0 || !related)
                        };
                        let matched = cached_binding_matches(
                            &cache,
                            |cache| *cache,
                            &mut acquire,
                            current,
                            unchanged,
                        );
                        if lease.is_some() {
                            let observed = sample();
                            assert_eq!(
                                matched,
                                Ok(observed.1 == 0 && (!related || observed.0 == 0)),
                                "cached binding disagrees with the leased source state"
                            );
                            assert_eq!(fallback.get(), !reports || observed != (0, 0));
                        } else {
                            assert_eq!(
                                matched,
                                Ok(false),
                                "a missing cache entry cannot be accepted"
                            );
                            assert!(!fallback.get());
                        }
                        if !changed && !rebound {
                            assert_eq!(
                                matched,
                                Ok(true),
                                "an unchanged cached binding must be accepted"
                            );
                        }
                    }
                    writer.join().unwrap();
                });
            }
        }
    }
}

#[cfg(not(acyclic_fs_loom))]
mod proofs {
    use super::cached_binding::cached_binding_decision;
    use std::cell::Cell;

    struct Lease<'a>(&'a Cell<bool>);

    impl Drop for Lease<'_> {
        fn drop(&mut self) {
            self.0.set(false);
        }
    }

    #[cfg(kani)]
    #[kani::proof]
    #[kani::unwind(1)]
    fn decision_requires_a_binding_and_lease_and_checks_each_path_at_most_once() {
        check(kani::any(), kani::any(), kani::any(), kani::any());
    }

    #[cfg(not(kani))]
    #[test]
    fn every_boolean_decision_retains_its_lease_and_bounds_callbacks() {
        for flags in 0..16 {
            check(
                flags & 1 != 0,
                flags & 2 != 0,
                flags & 4 != 0,
                flags & 8 != 0,
            );
        }
    }

    fn check(present: bool, admitted: bool, current: bool, unchanged: bool) {
        let live = Cell::new(false);
        let lease_calls = Cell::new(0_u8);
        let current_calls = Cell::new(0_u8);
        let fallback_calls = Cell::new(0_u8);
        let matched = cached_binding_decision(
            present.then_some(()),
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
        assert_eq!(
            matched,
            if present && !admitted {
                Err(())
            } else {
                Ok(present && (current || unchanged))
            }
        );
        assert_eq!(lease_calls.get(), u8::from(present));
        assert_eq!(current_calls.get(), u8::from(present && admitted));
        assert_eq!(
            fallback_calls.get(),
            u8::from(present && admitted && !current)
        );
        assert!(!live.get());
    }
}

#[cfg(not(acyclic_fs_loom))]
#[cfg_attr(kani, kani::proof)]
#[cfg_attr(not(kani), test)]
fn projection_copy_releases_its_mutex_and_preserves_lease_errors() {
    let cache = sync::Mutex::new(Some(()));
    let decisions = std::cell::Cell::new(0_u8);
    let result = cached_binding::cached_binding_matches(
        &cache,
        |cache| *cache,
        || {
            assert!(cache.try_lock().is_ok());
            Err::<(), _>(7_u8)
        },
        |()| {
            decisions.set(decisions.get() + 1);
            true
        },
        |()| {
            decisions.set(decisions.get() + 1);
            true
        },
    );
    assert_eq!(result, Err(7));
    assert_eq!(decisions.get(), 0);
}
