use super::*;
use loom::{sync::Mutex, thread};
use std::task::{Context, Wake, Waker};

fn complete<F: Future>(future: F) -> F::Output {
    crate::sync::owned::block_on(future)
}

fn joined<T>(worker: thread::JoinHandle<T>) -> T {
    worker
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

fn checked(scenario: fn(bool), diagnostic: &str) {
    loom::model(move || scenario(false));
    controlled(|| loom::model(move || scenario(true)), diagnostic);
}

fn controlled(control: impl FnOnce() + std::panic::UnwindSafe, diagnostic: &str) {
    let failure = std::panic::catch_unwind(control)
        .expect_err("projection negative control escaped the model");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains(diagnostic),
        "unexpected control: {message}"
    );
}

fn concurrent_initializers() {
    let slot = StateArc::new(AsyncMutex::new(None));
    // A filesystem operation is a leaf; it never reacquires the projection.
    let count = StateArc::new(Mutex::new(0));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let slot = slot.clone();
            let count = count.clone();
            thread::spawn(move || {
                let value = complete(initialize(&slot, async || {
                    *count.lock().unwrap() += 1;
                    thread::yield_now();
                    Ok(7)
                }))
                .unwrap();
                assert_eq!(*value, Some(7));
            })
        })
        .collect();
    for worker in workers {
        joined(worker);
    }
    assert_eq!(*count.lock().unwrap(), 1, "projection initialized twice");
}

#[test]
fn first_native_users_share_one_initialization() {
    loom::model(concurrent_initializers);
}

fn logical_write(broken: bool) {
    let slot = StateArc::new(AsyncMutex::new(None));
    let head = StateArc::new(Mutex::new(0));
    let (writer_slot, writer_head) = (slot.clone(), head.clone());
    let writer = thread::spawn(move || {
        let mut native = complete(lock_owned(writer_slot.clone()));
        if broken {
            // The faulty publisher decides whether a projection exists before
            // releasing custody, then commits without refreshing a new mount.
            let mounted = native.is_some();
            drop(native);
            thread::yield_now();
            *writer_head.lock().unwrap() = 1;
            if mounted {
                *complete(lock_owned(writer_slot)) = Some(1);
            }
        } else {
            *writer_head.lock().unwrap() = 1;
            thread::yield_now();
            if let Some(native) = native.as_mut() {
                *native = 1;
            }
        }
    });
    let (mount_slot, mount_head) = (slot.clone(), head.clone());
    let mounting = thread::spawn(move || {
        complete(initialize(&mount_slot, async || {
            let head = *mount_head.lock().unwrap();
            thread::yield_now();
            Ok(head)
        }))
        .unwrap();
    });
    joined(writer);
    joined(mounting);
    assert_eq!(
        *complete(lock_owned(slot)),
        Some(*head.lock().unwrap()),
        "projection missed a write"
    );
}

#[test]
fn first_projection_and_logical_publication_share_custody() {
    checked(logical_write, "projection missed a write");
}

#[test]
fn failed_or_cancelled_initialization_wakes_waiters_without_partial_state() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    for cancelled in [false, true] {
        loom::model(move || {
            let slot = StateArc::new(AsyncMutex::new(None::<u8>));
            let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
            let waker = Waker::from(wakes.clone());
            let mut context = Context::from_waker(&waker);
            let (release, released) = tokio::sync::oneshot::channel::<()>();
            let mut creation = Box::pin(initialize(&slot, async move || {
                released
                    .await
                    .map_err(|_| Error::Invalid("creation source closed".into()))?;
                Err(Error::Invalid("creation failed".into()))
            }));
            assert!(creation.as_mut().poll(&mut context).is_pending());
            let mut waiter = Box::pin(initialize(&slot, async || Ok(7)));
            // Registration is observed before the original creator can exit.
            assert!(waiter.as_mut().poll(&mut context).is_pending());
            if !cancelled {
                release.send(()).unwrap();
                wakes.0.store(0, Ordering::Relaxed);
                assert!(matches!(
                    creation.as_mut().poll(&mut context),
                    std::task::Poll::Ready(Err(_))
                ));
            } else {
                wakes.0.store(0, Ordering::Relaxed);
            }
            drop(creation);
            assert!(
                wakes.0.load(Ordering::Relaxed) > 0,
                "initialization did not wake its registered successor"
            );
            let std::task::Poll::Ready(value) = waiter.as_mut().poll(&mut context) else {
                panic!("initialization wake did not release its registered successor");
            };
            assert_eq!(
                *value.unwrap(),
                Some(7),
                "{}",
                if cancelled {
                    "cancelled initialization published partial state"
                } else {
                    "failed initialization published partial state"
                }
            );
        });
    }
}

fn lock_order(broken: bool) {
    let state = StateArc::new(AsyncMutex::new(()));
    let projection = StateArc::new(AsyncMutex::new(None));
    let (other_state, other_projection) = (state.clone(), projection.clone());
    let other = thread::spawn(move || {
        let _state = complete(lock_owned(other_state));
        complete(initialize(&other_projection, async || {
            thread::yield_now();
            Ok(7)
        }))
        .unwrap();
    });
    if broken {
        let _projection = complete(initialize(&projection, async || Ok(7))).unwrap();
        thread::yield_now();
        let _state = complete(lock_owned(state));
    } else {
        let _state = complete(lock_owned(state));
        complete(initialize(&projection, async || {
            thread::yield_now();
            Ok(7)
        }))
        .unwrap();
    }
    joined(other);
}

#[test]
fn checkout_then_projection_order_cannot_deadlock() {
    const CONTROL: &str = "ACYCLIC_PROJECTION_LOCK_ORDER_CONTROL";
    if crate::native_environment::variable(CONTROL).is_some() {
        loom::model(|| lock_order(true));
        panic!("inverted lock order was not detected");
    }
    loom::model(|| lock_order(false));
    // Loom's intentional deadlock can also panic while unwinding owned guards.
    // Isolate that abort, retaining both the failing status and exact diagnostic.
    let name = concat!(
        module_path!(),
        "::checkout_then_projection_order_cannot_deadlock"
    );
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([name.split_once("::").unwrap().1, "--exact", "--nocapture"])
        .env(CONTROL, "1")
        .env("RUST_BACKTRACE", "0")
        .output()
        .unwrap();
    assert!(!output.status.success(), "inverted lock order passed");
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    assert!(diagnostic.contains("deadlock; threads ="), "{diagnostic}");
}

fn scope(slot: &StateArc<AsyncMutex<Option<u8>>>, users: &StateArc<SyncMutex<usize>>) {
    let entered = complete(enter(slot, users, async || Ok(7))).unwrap();
    assert!(
        complete(lock_owned(slot.clone())).is_some(),
        "projection retired under a running command"
    );
    complete(leave(slot, users, entered, async |slot, retire| {
        if retire {
            *slot = None;
        }
        Ok(())
    }))
    .unwrap();
}

// Initial admission has its own exhaustive model above. Start with one live
// scope so this model explores only competing admission versus retirement.
fn scoped_retirement() {
    let slot = StateArc::new(AsyncMutex::new(None));
    let users = StateArc::new(SyncMutex::new(0));
    let entered = complete(enter(&slot, &users, async || Ok(7))).unwrap();
    let worker = {
        let (slot, users) = (slot.clone(), users.clone());
        thread::spawn(move || scope(&slot, &users))
    };
    // Finish the other command while this original claim is still live.
    // The retirement check must observe that overlap directly, rather than
    // depend on scheduling inside Tokio's uninstrumented lock.
    joined(worker);
    complete(leave(&slot, &users, entered, async |slot, retire| {
        assert!(slot.is_some(), "projection retired under a running command");
        if retire {
            *slot = None;
        }
        Ok(())
    }))
    .unwrap();
    assert!(
        complete(lock_owned(slot)).is_none(),
        "projection outlived its last scope"
    );
}

#[test]
fn the_last_execution_scope_alone_retires_the_projection() {
    loom::model(scoped_retirement);
}

fn publication() {
    let slot = StateArc::new(AsyncMutex::new(None));
    let users = StateArc::new(SyncMutex::new(0));
    let a = complete(enter(&slot, &users, async || Ok(0))).unwrap();
    let b = complete(enter(&slot, &users, async || Ok(0))).unwrap();
    let mut published = 0;
    // B already holds its claim when A completes. A must publish even though
    // B keeps the native namespace attached.
    complete(leave(&slot, &users, a, async |slot, retire| {
        published += 1;
        if retire {
            *slot = None;
        }
        Ok(())
    }))
    .unwrap();
    assert_eq!(published, 1, "successful command skipped publication");
    assert!(complete(lock_owned(slot.clone())).is_some());
    complete(leave(&slot, &users, b, async |slot, retire| {
        published += 1;
        assert!(retire);
        *slot = None;
        Ok(())
    }))
    .unwrap();
    assert_eq!(published, 2);
}

#[test]
fn every_completed_command_publishes_before_its_acknowledgement() {
    loom::model(publication);
}

fn competing_completions() {
    let slot = StateArc::new(AsyncMutex::new(None));
    let users = StateArc::new(SyncMutex::new(0));
    let a = complete(enter(&slot, &users, async || Ok(7))).unwrap();
    let b = complete(enter(&slot, &users, async || Ok(7))).unwrap();
    let published = StateArc::new(Mutex::new(0));
    let completion = move |slot: StateArc<AsyncMutex<Option<u8>>>,
                           users: StateArc<SyncMutex<usize>>,
                           entered: Entered,
                           published: StateArc<Mutex<usize>>| {
        let publish = async |slot: &mut Option<u8>, retire| {
            assert!(slot.is_some(), "completion lost its unpublished projection");
            *published.lock().unwrap() += 1;
            if retire {
                *slot = None;
            }
            Ok(())
        };
        complete(leave(&slot, &users, entered, publish)).unwrap();
    };
    let worker = {
        let (slot, users, published) = (slot.clone(), users.clone(), published.clone());
        thread::spawn(move || completion(slot, users, b, published))
    };
    completion(slot.clone(), users, a, published.clone());
    joined(worker);
    assert_eq!(*published.lock().unwrap(), 2);
    assert!(complete(lock_owned(slot)).is_none());
}

#[test]
fn every_completing_worker_retains_its_projection_until_publication() {
    loom::model(competing_completions);
}

fn execution_cleanup(broken: bool) {
    let execution = StateArc::new(AsyncMutex::new(()));
    let selected = complete(lock_owned(execution.clone()));
    let retired = StateArc::new(Mutex::new(false));
    let physical_cleanup = StateArc::new(Mutex::new(false));
    let worker = {
        let (execution, retired, physical_cleanup) =
            (execution.clone(), retired.clone(), physical_cleanup.clone());
        thread::spawn(move || {
            let _selected = complete(lock_owned(execution));
            assert!(
                *physical_cleanup.lock().unwrap(),
                "next entrant outran physical cleanup"
            );
            assert!(
                *retired.lock().unwrap(),
                "next entrant outran unpublished retirement"
            );
        })
    };
    let selected = if broken {
        drop(selected);
        None
    } else {
        Some(selected)
    };
    thread::yield_now();
    *physical_cleanup.lock().unwrap() = true;
    *retired.lock().unwrap() = true;
    drop(selected);
    joined(worker);
}

#[test]
fn execution_guard_covers_physical_cleanup_and_unpublished_retirement() {
    checked(execution_cleanup, "next entrant outran");
}
