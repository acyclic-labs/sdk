use super::*;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::task::Wake;

struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct PanicWake;

#[allow(clippy::panic)]
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any("cancellation wake panic");
    }
}

fn waiter_count(token: &CancellationToken) -> usize {
    match token.state.waiters.lock() {
        Ok(waiters) => waiters.len(),
        Err(poisoned) => poisoned.into_inner().len(),
    }
}

#[allow(clippy::panic)]
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
    let waiter = Arc::new(CancellationWaiter::default());
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    {
        let mut waker = match waiter.waker.lock() {
            Ok(waker) => waker,
            Err(poisoned) => poisoned.into_inner(),
        };
        *waker = Some(Waker::from(Arc::clone(&counter)));
    }
    match waker_poisoned.state.waiters.lock() {
        Ok(mut waiters) => waiters.push(Arc::downgrade(&waiter)),
        Err(poisoned) => poisoned.into_inner().push(Arc::downgrade(&waiter)),
    }
    poison(&waiter.waker);
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
    poison(&future.waiter.waker);
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter);
    let mut context = Context::from_waker(&waker);
    assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);
    drop(future);
    assert_eq!(waiter_count(&poll_waker_poisoned), 0);
}

#[test]
fn cancellation_ignores_waiters_dropped_before_delivery() {
    let token = CancellationToken::new();
    let waiter = Arc::new(CancellationWaiter::default());
    match token.state.waiters.lock() {
        Ok(mut waiters) => waiters.push(Arc::downgrade(&waiter)),
        Err(poisoned) => poisoned.into_inner().push(Arc::downgrade(&waiter)),
    }
    drop(waiter);
    token.cancel();
    assert!(token.is_cancelled());
    assert_eq!(waiter_count(&token), 0);
}
