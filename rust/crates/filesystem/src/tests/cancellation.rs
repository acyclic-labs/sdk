use super::*;
use crate::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::task::Wake;

struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any("cancellation wake panic");
    }
}

fn waiter_count(token: &CancellationToken) -> usize {
    let mut waiter = token
        .state
        .waiters
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .upgrade();
    let mut count = 0;
    while let Some(current) = waiter {
        count += 1;
        waiter = current
            .links
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .and_then(|(_, next)| next.upgrade());
    }
    count
}

fn poison<T>(mutex: &Mutex<T>) {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _guard = match mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        std::panic::panic_any("poison cancellation lock");
    }));
    assert!(result.is_err());
}

#[test]
fn cancellation_is_monotonic_and_wakes_registered_future() {
    let token = CancellationToken::new();
    let mut future = std::pin::pin!(token.cancelled());
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&counter));
    let mut context = Context::from_waker(&waker);
    assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);
    assert_eq!(waiter_count(&token), 1);
    token.cancel();
    token.cancel();
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(future.as_mut().poll(&mut context), Poll::Ready(()));
    assert_eq!(waiter_count(&token), 0);
    assert_eq!(token.check(), Err(CancellationError));
}

#[test]
fn dropped_cancellation_futures_remove_every_registered_waker() {
    let token = CancellationToken::new();
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter);
    let mut context = Context::from_waker(&waker);
    for _ in 0..1_000 {
        let mut future = Box::pin(token.cancelled());
        assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);
        assert_eq!(waiter_count(&token), 1);
        drop(future);
        assert_eq!(waiter_count(&token), 0);
    }
    token.cancel();
}

#[test]
fn cancellation_contains_panicking_wakers_and_completes_every_waiter() {
    let token = CancellationToken::new();
    let mut panicking = std::pin::pin!(token.cancelled());
    let panic_waker = Waker::from(Arc::new(PanicWake));
    let mut panic_context = Context::from_waker(&panic_waker);
    assert_eq!(panicking.as_mut().poll(&mut panic_context), Poll::Pending);

    let mut observed = std::pin::pin!(token.cancelled());
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let counted_waker = Waker::from(Arc::clone(&counter));
    let mut counted_context = Context::from_waker(&counted_waker);
    assert_eq!(observed.as_mut().poll(&mut counted_context), Poll::Pending);
    assert_eq!(waiter_count(&token), 2);

    token.cancel();
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(panicking.as_mut().poll(&mut panic_context), Poll::Ready(()));
    assert_eq!(
        observed.as_mut().poll(&mut counted_context),
        Poll::Ready(())
    );
    assert_eq!(waiter_count(&token), 0);
}

#[test]
fn poisoned_waiter_registries_and_wakers_remain_cancellable_and_drop_safe() {
    let registry_poisoned = CancellationToken::new();
    poison(&registry_poisoned.state.waiters);
    registry_poisoned.cancel();
    assert!(registry_poisoned.is_cancelled());

    let waker_poisoned = CancellationToken::new();
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let mut future = Box::pin(waker_poisoned.cancelled());
    let waker = Waker::from(Arc::clone(&counter));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    poison(&future.waiter.as_ref().expect("registered waiter").waker);
    waker_poisoned.cancel();
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);

    let poll_registry_poisoned = CancellationToken::new();
    let mut future = Box::pin(poll_registry_poisoned.cancelled());
    poison(&poll_registry_poisoned.state.waiters);
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter);
    let mut context = Context::from_waker(&waker);
    assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);
    drop(future);
    assert_eq!(waiter_count(&poll_registry_poisoned), 0);

    let poll_waker_poisoned = CancellationToken::new();
    let mut future = Box::pin(poll_waker_poisoned.cancelled());
    let waker = Waker::noop();
    assert_eq!(
        future.as_mut().poll(&mut Context::from_waker(waker)),
        Poll::Pending
    );
    poison(&future.waiter.as_ref().expect("registered waiter").waker);
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter);
    let mut context = Context::from_waker(&waker);
    assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);
    drop(future);
    assert_eq!(waiter_count(&poll_waker_poisoned), 0);
}

#[cfg(not(target_arch = "wasm32"))]
struct CancelOnDrop(CancellationToken);
#[cfg(not(target_arch = "wasm32"))]
impl Wake for CancelOnDrop {
    fn wake(self: Arc<Self>) {}
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_waiter_replacement_is_reentrant_and_releases_its_original_owner() {
    let token = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            items_examined: 6,
            ..crate::WorkBudget::UNBOUNDED
        },
        &token,
    )
    .map_err(|(error, _)| error)
    .unwrap();
    let state = Arc::downgrade(&token.state);
    let accounting = Arc::downgrade(&work.state);
    let mut prepared = token.prepare_cancelled_with_native_work(&work).unwrap();
    let waker = Waker::from(Arc::new(CancelOnDrop(token.clone())));
    assert!(
        Pin::new(&mut prepared)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    assert!(
        Pin::new(&mut prepared)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert!(token.is_cancelled());
    drop(token);
    drop(work);
    assert!(accounting.upgrade().is_some());
    drop(prepared);
    assert!(state.upgrade().is_none());
    assert!(accounting.upgrade().is_none());
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn linked_children_preserve_local_stop_parent_propagation_and_release_their_owner() {
    for stop_parent in [false, true] {
        let parent = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &parent)
            .map_err(|(error, _)| error)
            .unwrap();
        let child = CancellationToken::linked_child_with_native_work(&parent, &original).unwrap();
        let parent_state = Arc::downgrade(&parent.state);
        let child_state = Arc::downgrade(&child.state);
        let accounting = Arc::downgrade(&original.state);
        let scope = original
            .native_volume_work(crate::WorkBudget {
                items_examined: 6,
                ..crate::WorkBudget::UNBOUNDED
            })
            .unwrap();
        let mut prepared = child.prepare_cancelled_with_native_work(&scope).unwrap();
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        assert!(
            Pin::new(&mut prepared)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        if stop_parent {
            parent.cancel();
        } else {
            child.cancel();
        }
        assert!(child.is_cancelled());
        assert_eq!(parent.is_cancelled(), stop_parent);
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        assert!(
            Pin::new(&mut prepared)
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
        scope.receipt().verify(scope.budget()).unwrap();
        drop((waker, prepared, scope, original, parent));
        drop(child);
        assert!(parent_state.upgrade().is_none());
        assert!(child_state.upgrade().is_none());
        assert!(accounting.upgrade().is_none());
    }
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[test]
fn stopped_parent_prevents_commit_through_either_linked_child() {
    let parent = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &parent)
        .map_err(|(error, _)| error)
        .unwrap();
    let first = CancellationToken::linked_child_with_native_work(&parent, &original).unwrap();
    let second = CancellationToken::linked_child_with_native_work(&parent, &original).unwrap();
    let mut installed = 0;
    first
        .commit_if_both_live(&second, || installed += 1)
        .unwrap();
    assert_eq!(installed, 1);
    parent.cancel();
    assert!(parent.commit_if_live(|| installed += 1).is_err());
    assert!(
        first
            .commit_if_both_live(&second, || installed += 1)
            .is_err()
    );
    assert_eq!(installed, 1);
}
