// Run from the repository root:
// RUSTFLAGS='--cfg acyclic_sdk_docs_loom' cargo test --manifest-path rust/verification/sdk-fixtures/Cargo.toml --locked --test fixture-model
#![cfg(acyclic_sdk_docs_loom)]

use loom::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use sdk_fixture_verification::get_or_try_init;

#[path = "../../crates/sdk-generation/src/parallel.rs"]
mod parallel;

fn joined_workers(broken: bool) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(move || {
        let completed = Arc::new(AtomicUsize::new(0));
        let state = Arc::clone(&completed);
        let operation = move |index: usize| {
            state.fetch_or(1usize << index, Ordering::SeqCst);
            if index == 0 {
                Err(index)
            } else {
                Ok(index)
            }
        };
        let observed = if broken {
            completed.fetch_or(1, Ordering::SeqCst);
            let remaining = loom::thread::spawn(move || operation(1));
            // Record the erroneous early publication, but join before asserting
            // so a failing model never leaves an outstanding worker behind.
            let observed = completed.load(Ordering::SeqCst);
            assert_eq!(remaining.join().unwrap(), Ok(1));
            observed
        } else {
            assert_eq!(parallel::map(vec![0, 1], operation), vec![Err(0), Ok(1)]);
            completed.load(Ordering::SeqCst)
        };
        assert_eq!(observed, 3, "worker escaped publication barrier");
    });
}

#[test]
fn all_owner_compilers_join_before_errors_or_publication() {
    joined_workers(false);
}

#[test]
#[should_panic(expected = "worker escaped publication barrier")]
fn negative_control_detects_detached_owner_compiler() {
    joined_workers(true);
}

#[test]
fn a_panicking_owner_still_joins_every_other_worker() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let completed = Arc::new(AtomicUsize::new(0));
        let state = Arc::clone(&completed);
        let result = std::panic::catch_unwind(move || {
            parallel::map(vec![0, 1], move |index| {
                state.fetch_or(1usize << index, Ordering::SeqCst);
                assert_ne!(index, 0, "owner failed");
            })
        });
        assert!(result.is_err());
        assert_eq!(
            completed.load(Ordering::SeqCst),
            3,
            "panic escaped before all workers joined"
        );
    });
}

#[test]
fn joined_owners_share_one_immutable_plan() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let plan = parallel::Arc::new(vec![17, 29]);
        let shared = parallel::Arc::clone(&plan);
        let identity = parallel::Arc::as_ptr(&plan) as usize;
        assert_eq!(
            parallel::map(vec![0, 1], move |release| {
                assert_eq!(parallel::Arc::as_ptr(&shared) as usize, identity);
                shared[release]
            }),
            vec![17, 29]
        );
        assert_eq!(parallel::Arc::strong_count(&plan), 1);
    });
}

#[path = "../../crates/sdk-docs/src/concrete_versions.rs"]
mod concrete_versions;

fn concrete_version_model(broken: bool) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(move || {
        let versions = Arc::new(concrete_versions::Mutex::new(
            std::collections::BTreeMap::new(),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let jobs = [0, 1].map(|_| {
            let versions = Arc::clone(&versions);
            let calls = Arc::clone(&calls);
            loom::thread::spawn(move || {
                let probe = || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ()>("version".to_owned())
                };
                if broken {
                    let cached = versions.lock().unwrap().get(&0).cloned();
                    if let Some((_, value)) = cached {
                        return value;
                    }
                    let value = probe().unwrap();
                    versions.lock().unwrap().insert(0, (0, value.clone()));
                    value
                } else {
                    concrete_versions::query(&versions, 0, || Ok(0), probe, || ()).unwrap()
                }
            })
        });
        let results = jobs.map(|job| job.join().unwrap());
        assert_eq!(results, ["version", "version"]);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "concrete executable probed twice"
        );
    });
}

#[test]
fn concrete_executable_is_probed_once_across_readers() {
    concrete_version_model(false);
}

#[test]
#[should_panic(expected = "concrete executable probed twice")]
fn negative_control_detects_split_concrete_version_lookup() {
    concrete_version_model(true);
}

#[test]
fn concrete_identity_changes_cannot_publish_a_torn_version() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let versions = Arc::new(concrete_versions::Mutex::new(
            std::collections::BTreeMap::new(),
        ));
        let identity = Arc::new(AtomicUsize::new(0));
        let reader = {
            let versions = Arc::clone(&versions);
            let identity = Arc::clone(&identity);
            loom::thread::spawn(move || {
                concrete_versions::query(
                    &versions,
                    0,
                    || Ok(identity.load(Ordering::SeqCst)),
                    || Ok::<_, ()>(identity.load(Ordering::SeqCst).to_string()),
                    || (),
                )
            })
        };
        let writer = loom::thread::spawn(move || identity.store(1, Ordering::SeqCst));
        let _ = reader.join().unwrap();
        writer.join().unwrap();
        for (stamp, version) in versions.lock().unwrap().values() {
            assert_eq!(&stamp.to_string(), version);
        }
    });
}

#[test]
fn failed_concrete_probe_releases_the_lock_for_another_reader() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let versions = Arc::new(concrete_versions::Mutex::new(
            std::collections::BTreeMap::new(),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let jobs = [0, 1].map(|_| {
            let versions = Arc::clone(&versions);
            let calls = Arc::clone(&calls);
            loom::thread::spawn(move || {
                concrete_versions::query(
                    &versions,
                    0,
                    || Ok(0),
                    || {
                        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            Err(())
                        } else {
                            Ok("version".to_owned())
                        }
                    },
                    || (),
                )
            })
        });
        let results = jobs.map(|job| job.join().unwrap());
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(versions.lock().unwrap()[&0], (0, "version".to_owned()));
    });
}

#[path = "../../crates/sdk-generation/src/shared_cache.rs"]
mod shared_cache;

fn model(broken: bool) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(move || {
        let slot = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        let jobs = [0, 1].map(|_| {
            let slot = Arc::clone(&slot);
            let calls = Arc::clone(&calls);
            loom::thread::spawn(move || {
                let initialize = || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ()>(vec![1, 2])
                };
                if broken {
                    // Deliberately release the slot while constructing the package.
                    if let Some(value) = slot.lock().unwrap().as_ref() {
                        return Arc::clone(value);
                    }
                    let value = Arc::new(initialize().unwrap());
                    *slot.lock().unwrap() = Some(Arc::clone(&value));
                    value
                } else {
                    get_or_try_init(&slot, initialize).unwrap()
                }
            })
        });
        let [a, b] = jobs.map(|job| job.join().unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "package initialized twice");
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(a.as_slice(), &[1, 2]);
    });
}

#[test]
fn package_is_initialized_once_and_published_whole() {
    model(false);
}

#[test]
#[should_panic(expected = "package initialized twice")]
fn model_detects_initialization_outside_the_lock() {
    model(true);
}
#[test]
fn failed_initialization_leaves_the_slot_retryable() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let slot = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        let jobs = [0, 1].map(|_| {
            let slot = Arc::clone(&slot);
            let calls = Arc::clone(&calls);
            loom::thread::spawn(move || {
                get_or_try_init(&slot, || {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(())
                    } else {
                        Ok(vec![1, 2])
                    }
                })
            })
        });
        let results = jobs.map(|job| job.join().unwrap());
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(slot.lock().unwrap().as_ref().unwrap().as_slice(), &[1, 2]);
    });
}

#[test]
fn producer_then_cfg_slots_publish_without_nested_locks() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let producer = Arc::new(Mutex::new(None));
        let cfgs = Arc::new(Mutex::new(None));
        let calls = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let jobs = [0, 1].map(|_| {
            let producer = Arc::clone(&producer);
            let cfgs = Arc::clone(&cfgs);
            let calls = Arc::clone(&calls);
            loom::thread::spawn(move || {
                let producer = get_or_try_init(&producer, || {
                    calls[0].fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ()>(1)
                })
                .unwrap();
                let cfgs = get_or_try_init(&cfgs, || {
                    calls[1].fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ()>(vec![*producer, 2])
                })
                .unwrap();
                assert_eq!(cfgs.as_slice(), &[1, 2]);
            })
        });
        for job in jobs {
            job.join().unwrap();
        }
        for calls in calls.iter() {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    });
}

fn shared_identity_model(broken: bool, fail_first: bool) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(move || {
        let lock = shared_cache::Lock::new(Mutex::new(()));
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let jobs = [0, 1].map(|_| {
            let lock = Arc::clone(&lock);
            let bytes = Arc::clone(&bytes);
            let calls = Arc::clone(&calls);
            loom::thread::spawn(move || {
                let read = || {
                    let bytes = bytes.lock().unwrap();
                    Ok((bytes.as_slice() == [1, 2]).then(|| bytes.clone()))
                };
                let compute = || {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    if fail_first && call == 0 {
                        return Err(std::io::Error::other("unavailable"));
                    }
                    Ok(vec![1, 2])
                };
                let publish = |value: &Vec<u8>| {
                    bytes.lock().unwrap().push(value[0]);
                    loom::thread::yield_now();
                    bytes.lock().unwrap().push(value[1]);
                    Ok(())
                };
                if broken {
                    shared_cache::without_lock(read, compute, publish)
                } else {
                    shared_cache::get_or_compute(|| Ok(Arc::clone(&lock)), read, compute, publish)
                }
            })
        });
        let results = jobs.map(|job| job.join().unwrap());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if fail_first { 2 } else { 1 },
            "identity cache invariant: duplicate computation"
        );
        assert_eq!(
            results.iter().filter(|result| result.is_err()).count(),
            usize::from(fail_first)
        );
        for value in results.into_iter().flatten() {
            assert_eq!(value, [1, 2], "identity cache invariant: torn publication");
        }
        assert_eq!(
            *bytes.lock().unwrap(),
            [1, 2],
            "identity cache invariant: torn stored identity"
        );
    });
}

#[test]
fn shared_identity_is_computed_once_and_published_whole() {
    shared_identity_model(false, false);
}

#[test]
#[should_panic(expected = "identity cache invariant")]
fn shared_identity_model_detects_omitted_process_lock() {
    shared_identity_model(true, false);
}

#[test]
fn failed_shared_computation_releases_the_lock_for_retry() {
    shared_identity_model(false, true);
}

#[test]
#[should_panic(expected = "nested identity cache lock")]
fn shared_identity_lock_order_checker_rejects_nesting() {
    loom::model(|| {
        let a = shared_cache::Lock::new(Mutex::new(()));
        let b = shared_cache::Lock::new(Mutex::new(()));
        let _: std::io::Result<()> = shared_cache::get_or_compute(
            || Ok(Arc::clone(&a)),
            || {
                shared_cache::get_or_compute(
                    || Ok(Arc::clone(&b)),
                    || Ok(None),
                    || Ok(()),
                    |_| Ok(()),
                )?;
                Ok(None)
            },
            || Ok(()),
            |_| Ok(()),
        );
    });
}

fn warm_identity(broken: bool) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(move || {
        let lock = shared_cache::Lock::new(Mutex::new(()));
        // Record storage is a leaf below the identity lock; the fast read drops
        // it before opening the identity lock on a miss.
        let record = Arc::new(Mutex::new(vec![1, 2]));
        let opens = Arc::new(AtomicUsize::new(0));
        let jobs = [0, 1].map(|_| {
            let (lock, record, opens) =
                (Arc::clone(&lock), Arc::clone(&record), Arc::clone(&opens));
            loom::thread::spawn(move || {
                let open = || {
                    opens.fetch_add(1, Ordering::SeqCst);
                    Ok(lock)
                };
                let read = || Ok(Some(record.lock().unwrap().clone()));
                let compute = || panic!("validated hit was recomputed");
                let publish = |_: &Vec<usize>| panic!("validated hit was republished");
                if broken {
                    shared_cache::without_fast_read(open, read, compute, publish)
                } else {
                    shared_cache::get_or_compute(open, read, compute, publish)
                }
            })
        });
        for job in jobs {
            assert_eq!(job.join().unwrap().unwrap(), [1, 2]);
        }
        assert_eq!(
            opens.load(Ordering::SeqCst),
            0,
            "cached reader opened the exclusive lock"
        );
    });
}

#[test]
fn validated_identity_hits_never_open_the_exclusive_lock() {
    warm_identity(false);
}

#[test]
#[should_panic(expected = "cached reader opened the exclusive lock")]
fn negative_control_detects_serialized_identity_hits() {
    warm_identity(true);
}

#[test]
fn cargo_and_fixture_tool_outputs_publish_as_one_initialization() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let slot = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        let jobs = (0..2)
            .map(|_| {
                let slot = Arc::clone(&slot);
                let calls = Arc::clone(&calls);
                loom::thread::spawn(move || {
                    let setup = get_or_try_init(&slot, || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, ()>((17, vec![29, 31, 37]))
                    })
                    .unwrap();
                    assert_eq!(*setup, (17, vec![29, 31, 37]));
                    setup
                })
            })
            .collect::<Vec<_>>();
        let outputs = jobs
            .into_iter()
            .map(|job| job.join().unwrap())
            .collect::<Vec<_>>();
        assert!(Arc::ptr_eq(&outputs[0], &outputs[1]));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    });
}
