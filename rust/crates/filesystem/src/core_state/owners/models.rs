use super::*;
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};

#[derive(Debug)]
struct Seen {
    created: usize,
    expected: usize,
    shared: bool,
}

fn initialize<const FENCE: bool>(
    registry: &Owners<usize>,
    created: &AtomicUsize,
    ready: loom::sync::mpsc::Sender<()>,
    peer: loom::sync::mpsc::Receiver<()>,
) -> OwnerRef<usize> {
    join::<_, FENCE, true>(registry, "namespace".into(), [1; 16], true, || {
        let id = created.fetch_add(1, Ordering::SeqCst);
        if !FENCE {
            // Witness the unlocked initialization window with both constructors
            // in flight. Channels coordinate the control, not registry state.
            ready.send(()).unwrap();
            peer.recv().unwrap();
        }
        loom::thread::yield_now();
        Ok(Arc::new(id))
    })
    .unwrap()
}

fn race<const FENCE: bool>(see: &mut Observer<'_, Seen>) -> Result<(), Stop> {
    let registry = Arc::new(Owners::new(BTreeMap::new()));
    let created = Arc::new(AtomicUsize::new(0));
    let (first_ready, first_seen) = loom::sync::mpsc::channel();
    let (second_ready, second_seen) = loom::sync::mpsc::channel();
    let peer = {
        let registry = registry.clone();
        let created = created.clone();
        loom::thread::spawn(move || {
            initialize::<FENCE>(&registry, &created, first_ready, second_seen)
        })
    };
    // The model thread is the second actor: both joins still race, while each
    // independent oracle run needs only one spawned peer.
    let first = initialize::<FENCE>(&registry, &created, second_ready, first_seen);
    let owners = [first, peer.join().unwrap()];
    see.see(Seen {
        created: created.load(Ordering::SeqCst),
        expected: 1,
        shared: Arc::ptr_eq(owners[0].0.as_ref().unwrap(), owners[1].0.as_ref().unwrap()),
    })?;
    let reopened = join::<_, FENCE, true>(&registry, "namespace".into(), [1; 16], true, || {
        panic!("reopen ignored a live owner")
    })
    .unwrap();
    see.see(Seen {
        created: created.load(Ordering::SeqCst),
        expected: 1,
        shared: Arc::ptr_eq(owners[0].0.as_ref().unwrap(), reopened.0.as_ref().unwrap()),
    })?;
    drop((owners, reopened));
    join::<_, FENCE, true>(&registry, "namespace".into(), [2; 16], true, || {
        created.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(2))
    })
    .unwrap();
    see.see(Seen {
        created: created.load(Ordering::SeqCst),
        expected: 2,
        shared: true,
    })
}

#[test]
fn competing_joins_and_reopen_share_the_live_owner() {
    loom::model(|| {
        let oracle = Oracle::declare(|_: &(), fault: Option<&()>, see| {
            if fault.is_some() {
                race::<false>(see)
            } else {
                race::<true>(see)
            }
        })
        .invariant("registry-owner", |seen| {
            if seen.created == seen.expected && seen.shared {
                Ok(())
            } else {
                Err(format!("{seen:?}"))
            }
        })
        .control("registry-owner", (), ())
        .build();
        oracle.check(&());
    });
}

fn concurrent_reopen<const MODE: u8>() {
    loom::model(|| {
        let registry = Arc::new(Owners::new(BTreeMap::new()));
        let created = Arc::new(AtomicUsize::new(0));
        let first = join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
            Ok(Arc::new(created.fetch_add(1, Ordering::SeqCst)))
        })
        .unwrap();
        let release = loom::thread::spawn(move || {
            loom::thread::yield_now();
            let mut first = first;
            release::<_, true, MODE>(&first.1, &mut first.0);
        });
        let opener = {
            let registry = registry.clone();
            let created = created.clone();
            loom::thread::spawn(move || {
                join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
                    loom::thread::yield_now();
                    Ok(Arc::new(created.fetch_add(1, Ordering::SeqCst)))
                })
                .unwrap()
            })
        };
        let current = opener.join().unwrap();
        release.join().unwrap();
        // Never panic while the namespace gate is held: loom would abort on
        // the poisoned lock instead of reporting the failed invariant.
        let again = join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
            Ok(Arc::new(usize::MAX))
        })
        .unwrap();
        assert!(
            Arc::ptr_eq(current.0.as_ref().unwrap(), again.0.as_ref().unwrap()),
            "lost the owner retained by the reopening caller"
        );
        assert!(matches!(created.load(Ordering::SeqCst), 1 | 2));
    });
}

#[test]
fn concurrent_reopen_preserves_the_joined_owner() {
    concurrent_reopen::<ATOMIC>();
}

#[test]
#[should_panic(expected = "lost the owner retained by the reopening caller")]
fn negative_control_detects_final_release_decided_outside_the_gate() {
    concurrent_reopen::<UNGATED>();
}

struct NamespaceLock {
    locked: Arc<AtomicBool>,
    tearing_down: Arc<AtomicBool>,
}

impl Drop for NamespaceLock {
    fn drop(&mut self) {
        // The namespace is retiring its final owner inside the lifecycle gate.
        self.tearing_down.store(true, Ordering::SeqCst);
        loom::thread::yield_now();
        self.locked.store(false, Ordering::SeqCst);
    }
}

fn release_race<const FENCE: bool>() {
    loom::model(|| {
        let registry = Arc::new(Owners::new(BTreeMap::new()));
        let locked = Arc::new(AtomicBool::new(false));
        let tearing_down = Arc::new(AtomicBool::new(false));
        let overlap = AtomicBool::new(false);
        let create = || {
            overlap.fetch_or(locked.swap(true, Ordering::SeqCst), Ordering::SeqCst);
            Ok(Arc::new(NamespaceLock {
                locked: locked.clone(),
                tearing_down: tearing_down.clone(),
            }))
        };
        let first =
            join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, create).unwrap();
        let releasing = {
            loom::thread::spawn(move || {
                let mut first = first;
                release::<_, FENCE, ATOMIC>(&first.1, &mut first.0);
            })
        };
        // Force the final-strong-count window rather than letting reopen retain
        // the first owner before its destructor starts. With the fence, reopen
        // must wait for teardown; without it, loom must find an overlapping lock.
        while !tearing_down.load(Ordering::SeqCst) {
            loom::thread::yield_now();
        }
        let current =
            join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, create).unwrap();
        releasing.join().unwrap();
        assert!(
            !overlap.load(Ordering::SeqCst),
            "owner teardown raced reopen"
        );
        let again = join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
            panic!("reopen lost its owner")
        })
        .unwrap();
        assert!(Arc::ptr_eq(
            current.0.as_ref().unwrap(),
            again.0.as_ref().unwrap()
        ));
        drop(current);
        assert!(locked.load(Ordering::SeqCst));
        drop(again);
        assert!(!locked.load(Ordering::SeqCst));
    });
}

#[test]
fn last_release_and_reopen_share_the_registry_fence() {
    release_race::<true>();
}

#[test]
#[should_panic(expected = "owner teardown raced reopen")]
fn negative_control_detects_last_release_outside_the_registry_fence() {
    release_race::<false>();
}

fn nonfinal_locks<const MODE: u8>() {
    loom::model(|| {
        let registry = Owners::new(BTreeMap::new());
        let first = join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
            Ok(Arc::new(1))
        })
        .unwrap();
        let mut second = first.clone();
        let before = first.1.locks.load(Ordering::Relaxed);
        release::<_, true, MODE>(&second.1, &mut second.0);
        assert_eq!(
            first.1.locks.load(Ordering::Relaxed) - before,
            0,
            "nonfinal release acquired a namespace lock"
        );
    });
}

#[test]
fn nonfinal_release_is_atomic_and_lock_free() {
    nonfinal_locks::<ATOMIC>();
}

#[test]
#[should_panic(expected = "nonfinal release acquired a namespace lock")]
fn negative_control_detects_locking_nonfinal_releases() {
    nonfinal_locks::<LOCKED>();
}

fn simultaneous_releases<const MODE: u8>() {
    loom::model(|| {
        let registry = Arc::new(Owners::new(BTreeMap::new()));
        let locked = Arc::new(AtomicBool::new(false));
        let create = || {
            assert!(
                !locked.swap(true, Ordering::SeqCst),
                "owner teardown raced reopen"
            );
            Ok(Arc::new(NamespaceLock {
                locked: locked.clone(),
                tearing_down: Arc::new(AtomicBool::new(false)),
            }))
        };
        let first =
            join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, create).unwrap();
        let mut second = first.clone();
        let peer = loom::thread::spawn(move || {
            let mut first = first;
            release::<_, true, MODE>(&first.1, &mut first.0);
        });
        release::<_, true, MODE>(&second.1, &mut second.0);
        let current =
            join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, create).unwrap();
        peer.join().unwrap();
        assert!(locked.load(Ordering::SeqCst));
        drop(current);
        assert!(
            !locked.load(Ordering::SeqCst),
            "final release leaked the owner"
        );
    });
}

#[test]
fn simultaneous_nonfinal_releases_cannot_bypass_teardown() {
    simultaneous_releases::<ATOMIC>();
}

#[test]
#[should_panic(expected = "final release leaked the owner")]
fn negative_control_detects_split_nonfinal_decrements() {
    simultaneous_releases::<SPLIT>();
}

fn exclusive_admission<const RETAIN: bool>() {
    loom::model(|| {
        let registry = Arc::new(Owners::new(BTreeMap::new()));
        let exclusive =
            join::<_, true, RETAIN>(&registry, "namespace".into(), [1; 16], false, || {
                Ok(Arc::new(1))
            })
            .unwrap();
        let joining = {
            let registry = registry.clone();
            loom::thread::spawn(move || {
                join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
                    panic!("created over a live exclusive admission")
                })
            })
        };
        assert!(
            matches!(
                joining.join().unwrap(),
                Err(LocalCoreStateStoreError::OwnershipConflict(_))
            ),
            "exclusive admission became shareable"
        );
        drop(exclusive);
        join::<_, true, true>(&registry, "namespace".into(), [1; 16], true, || {
            Ok(Arc::new(2))
        })
        .unwrap();
    });
}

#[test]
fn exclusive_admissions_cannot_be_joined() {
    exclusive_admission::<true>();
}

#[test]
#[should_panic(expected = "exclusive admission became shareable")]
fn negative_control_detects_erasing_exclusive_admission() {
    exclusive_admission::<false>();
}
