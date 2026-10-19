use super::*;
use loom::{sync::Mutex, thread};
use std::task::{Context, Poll, Waker};

fn complete<F: Future>(future: F) -> F::Output {
    match std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("modeled projection unexpectedly pending"),
    }
}

fn joined<T>(worker: thread::JoinHandle<T>) -> T {
    worker
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

fn checked(scenario: fn(bool), diagnostic: &str) {
    loom::model(move || scenario(false));
    let failure = std::panic::catch_unwind(|| loom::model(move || scenario(true)))
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

fn concurrent_initializers(broken: bool) {
    let slot = StateArc::new(AsyncMutex::new(None));
    // A filesystem operation is a leaf; it never reacquires the projection.
    let count = StateArc::new(Mutex::new(0));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let slot = if broken {
                StateArc::new(AsyncMutex::new(None))
            } else {
                slot.clone()
            };
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
    checked(concurrent_initializers, "projection initialized twice");
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

fn failure(broken: bool) {
    let slot = StateArc::new(AsyncMutex::new(None::<u8>));
    let mut first_poll = true;
    let mut creation = Box::pin(async {
        if broken {
            let mut guard = lock_owned(slot.clone()).await;
            *guard = Some(0);
        }
        initialize(&slot, async || {
            std::future::poll_fn(|_| {
                if std::mem::take(&mut first_poll) {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            Err(Error::Invalid("creation failed".into()))
        })
        .await
        .map(|_| ())
    });
    if broken {
        // The deliberate partial install skips creation and exposes its value.
        assert!(complete(creation.as_mut()).is_ok());
    } else {
        assert!(
            creation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    let reader_slot = slot.clone();
    let reader = thread::spawn(move || {
        assert_eq!(
            *complete(initialize(&reader_slot, async || Ok(7))).unwrap(),
            Some(7),
            "failed initialization published partial state"
        );
    });
    thread::yield_now();
    if !broken {
        assert!(complete(creation.as_mut()).is_err());
    }
    joined(reader);
}

#[test]
fn failed_initialization_leaves_no_published_projection() {
    checked(failure, "failed initialization published partial state");
}

fn cancellation(broken: bool) {
    let slot = StateArc::new(AsyncMutex::new(None));
    let mut creation = Box::pin(async {
        if broken {
            let mut guard = lock_owned(slot.clone()).await;
            *guard = Some(0);
            std::future::pending::<()>().await;
        } else {
            initialize(&slot, async || {
                std::future::pending::<()>().await;
                Ok(7)
            })
            .await
            .unwrap();
        }
    });
    assert!(
        creation
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let waiter_slot = slot.clone();
    let waiter = thread::spawn(move || {
        assert_eq!(
            *complete(initialize(&waiter_slot, async || Ok(7))).unwrap(),
            Some(7),
            "cancelled initialization published partial state"
        );
    });
    thread::yield_now();
    drop(creation);
    joined(waiter);
}

#[test]
fn cancelling_initialization_releases_waiters_without_partial_state() {
    checked(
        cancellation,
        "cancelled initialization published partial state",
    );
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
