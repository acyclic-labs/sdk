use super::*;
use crate::sync::Mutex;
use loom::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Cleanup(Arc<AtomicBool>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
        loom::thread::yield_now();
    }
}

fn check(reversed: bool) {
    loom::model(move || {
        // One lock: reopening must wait until the former owner's cleanup ends.
        let fence = Arc::new(Mutex::new(()));
        let exists = Arc::new(AtomicBool::new(true));
        let guard = fence.lock().unwrap();
        let owner = OwnershipAnchor::new(Cleanup(exists.clone()));
        let other_fence = fence.clone();
        let other_exists = exists.clone();
        let opener = loom::thread::spawn(move || {
            let _guard = other_fence.lock().unwrap();
            other_exists.store(true, Ordering::SeqCst);
        });
        if reversed {
            // Former tuple order: a waiting opener can publish before cleanup.
            drop((guard, owner));
        } else {
            // The exact production drop implementation, with the real shim lock.
            let shared = Arc::new(SharedOwner::new(owner));
            let retained = shared.clone();
            shared.retain(guard);
            drop(shared);
            loom::thread::yield_now();
            drop(retained);
        }
        opener.join().unwrap();
        assert!(
            exists.load(Ordering::SeqCst),
            "cleanup deleted a reopened store"
        );
    });
}

#[test]
fn cleanup_precedes_reopen_under_every_interleaving() {
    check(false);
}

#[test]
#[should_panic(expected = "cleanup deleted a reopened store")]
fn reversed_release_order_is_detected() {
    check(true);
}

struct Fence(Arc<AtomicBool>);
impl Drop for Fence {
    fn drop(&mut self) {
        assert!(
            !self.0.load(Ordering::SeqCst),
            "fence released before shared cleanup"
        );
    }
}

fn registration(discard: bool) {
    loom::model(move || {
        let exists = Arc::new(AtomicBool::new(true));
        let owner = Arc::new(SharedOwner::new(OwnershipAnchor::new(Cleanup(
            exists.clone(),
        ))));
        let first = owner.clone();
        let second = owner.clone();
        let first_fence = Fence(exists.clone());
        let second_fence = Fence(exists.clone());
        let a = loom::thread::spawn(move || {
            if discard {
                drop(first_fence);
            } else {
                first.retain(first_fence);
            }
        });
        let b = loom::thread::spawn(move || {
            second.retain(second_fence);
        });
        drop(owner);
        a.join().unwrap();
        b.join().unwrap();
        assert!(!exists.load(Ordering::SeqCst));
    });
}

#[test]
fn concurrent_fence_registration_survives_last_store_release() {
    registration(false);
}

#[test]
#[should_panic(expected = "fence released before shared cleanup")]
fn discarded_fence_is_detected() {
    registration(true);
}

struct ProvisionalFence(Arc<AtomicBool>);
impl Drop for ProvisionalFence {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

fn initialization(commit_early: bool, cancel: bool, success: bool) {
    loom::model(move || {
        let exists = Arc::new(AtomicBool::new(true));
        let held = Arc::new(AtomicBool::new(true));
        let owner = OwnershipAnchor::new(Cleanup(exists.clone()));
        // Exactly the caller clone that outlives a failed attempt.
        let retained = owner.clone();
        let fence = ProvisionalFence(held.clone());
        let mut worker = None;
        let mut attempt = Box::pin(async {
            let mut initialize = |provisional: OwnershipAnchor| {
                let physical_work = provisional.clone();
                let held = held.clone();
                worker = Some(loom::thread::spawn(move || {
                    assert!(held.load(Ordering::SeqCst), "active worker lost its fence");
                    drop(physical_work);
                }));
                async move {
                    let _provisional = provisional;
                    if cancel {
                        std::future::pending::<()>().await;
                    }
                    if success { Ok(_provisional) } else { Err(()) }
                }
            };
            if commit_early {
                initialize(OwnershipAnchor::fenced(owner, fence)).await
            } else {
                owner.initialize_fenced(fence, initialize).await
            }
        });
        let result = attempt
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        assert_eq!(result.is_pending(), cancel);
        drop(result);
        drop(attempt);
        worker.take().unwrap().join().unwrap();
        assert_eq!(
            held.load(Ordering::SeqCst),
            success && !cancel,
            "failed initialization retained its reopen fence"
        );
        assert!(exists.load(Ordering::SeqCst));
        drop(retained);
        assert!(!held.load(Ordering::SeqCst));
        assert!(!exists.load(Ordering::SeqCst));
    });
}

#[test]
fn failed_initialization_releases_its_fence_after_physical_work() {
    initialization(false, false, false);
}

#[test]
fn cancelled_initialization_releases_its_fence_after_physical_work() {
    initialization(false, true, false);
}

#[test]
fn successful_initialization_retains_its_fence_for_existing_clones() {
    initialization(false, false, true);
}

#[test]
#[should_panic(expected = "failed initialization retained its reopen fence")]
fn premature_registration_blocks_failure_retry() {
    initialization(true, false, false);
}
