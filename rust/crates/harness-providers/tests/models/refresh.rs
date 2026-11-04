#[path = "../../../harness-core/src/sync/owned.rs"]
mod owned;

mod refresh {
    include!("../../src/refresh.rs");
}
use crate::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use loom::sync::mpsc;
use std::future::Future;

fn complete<T>(operation: impl Future<Output = T>) -> T {
    owned::block_on(operation)
}

fn sharing_model(fail: bool) {
    loom::model(move || {
        let state = Arc::new(refresh::Refresh::<u16>::new());
        let owner = complete(state.acquire()).ok().unwrap();
        let requests = Arc::new(AtomicUsize::new(1));
        let active = Arc::new(AtomicUsize::new(1));
        let (ready, admitted) = mpsc::channel();
        let waiter_state = state.clone();
        let waiter_requests = requests.clone();
        let waiter_active = active.clone();
        let waiter = loom::thread::spawn(move || {
            // Sample through the real entry point while the original owner is
            // held. No private generation ticket or alternate Refresh instance.
            let acquisition = waiter_state.acquire();
            ready.send(()).unwrap();
            let result = complete(async {
                let permit = acquisition.await?;
                assert_eq!(
                    waiter_active.fetch_add(1, Ordering::SeqCst),
                    0,
                    "refresh ownership overlapped"
                );
                waiter_requests.fetch_add(1, Ordering::SeqCst);
                waiter_active.fetch_sub(1, Ordering::SeqCst);
                waiter_state.complete(permit, fail.then_some(503));
                Ok::<_, u16>(())
            });
            if fail {
                assert!(matches!(result, Ok(()) | Err(503)));
            } else {
                assert_eq!(result, Ok(()));
            }
        });
        admitted.recv().unwrap();
        loom::thread::yield_now();
        assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
        state.complete(owner, fail.then_some(503));
        waiter.join().unwrap();
        assert_eq!(
            requests.load(Ordering::SeqCst),
            if fail { 1 } else { 2 },
            "failed refresh was repeated by a waiter"
        );
        assert_eq!(active.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn refresh_attempt_failure_retains_exclusive_ownership() {
    sharing_model(true);
}

#[test]
fn successful_refresh_permits_later_work() {
    // Success publishes no cached failure. Skipping redundant provider I/O is
    // the caller's credential re-read, outside this primitive's contract.
    sharing_model(false);
}

#[test]
fn cancelling_the_holder_releases_the_guard_and_wakes_the_waiter() {
    loom::model(|| {
        let state = Arc::new(refresh::Refresh::<u16>::new());
        let holder = complete(state.acquire()).ok().unwrap();
        let (registered, waiting) = mpsc::channel();
        let waiter_state = state.clone();
        let waiter = loom::thread::spawn(move || {
            let mut acquisition = std::pin::pin!(waiter_state.acquire());
            let mut registered = Some(registered);
            let permit = complete(std::future::poll_fn(|context| {
                let result = acquisition.as_mut().poll(context);
                if result.is_pending() {
                    if let Some(registered) = registered.take() {
                        registered.send(()).unwrap();
                    }
                }
                result
            }))
            .ok()
            .unwrap();
            waiter_state.complete(permit, None);
        });
        waiting.recv().unwrap();
        drop(holder);
        waiter.join().unwrap();
    });
}
