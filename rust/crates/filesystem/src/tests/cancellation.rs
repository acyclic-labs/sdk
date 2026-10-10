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

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any("cancellation wake panic");
    }
}

fn waiter_count(token: &CancellationToken) -> usize {
    match token.state.waiters.lock() {
        Ok(waiters) => waiters.iter().flatten().count(),
        Err(poisoned) => poisoned.into_inner().iter().flatten().count(),
    }
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
        Ok(mut waiters) => waiters.push(Some(Arc::downgrade(&waiter))),
        Err(poisoned) => poisoned.into_inner().push(Some(Arc::downgrade(&waiter))),
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
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter);
    let mut context = Context::from_waker(&waker);
    assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);
    poison(&future.waiter.as_ref().expect("polled waiter").waker);
    assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);
    drop(future);
    assert_eq!(waiter_count(&poll_waker_poisoned), 0);
}

#[test]
fn cancellation_ignores_waiters_dropped_before_delivery() {
    let token = CancellationToken::new();
    let waiter = Arc::new(CancellationWaiter::default());
    match token.state.waiters.lock() {
        Ok(mut waiters) => waiters.push(Some(Arc::downgrade(&waiter))),
        Err(poisoned) => poisoned.into_inner().push(Some(Arc::downgrade(&waiter))),
    }
    drop(waiter);
    token.cancel();
    assert!(token.is_cancelled());
    assert_eq!(waiter_count(&token), 0);
}

#[test]
fn measured_cancel_denial_keeps_original_live_waiters_until_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let token = CancellationToken::new();
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&counter));
    let mut context = Context::from_waker(&waker);
    let mut first = Box::pin(token.cancelled());
    let mut second = Box::pin(token.cancelled());
    assert_eq!(first.as_mut().poll(&mut context), Poll::Pending);
    assert_eq!(second.as_mut().poll(&mut context), Poll::Pending);
    let mut work = crate::WorkCounters { bytes_read: 7, ..crate::WorkCounters::UNCHARGED };
    let before = work;
    assert!(token.cancel_measured(&mut work, crate::WorkBudget {
        items_examined: 1, ..crate::WorkBudget::UNBOUNDED
    }).is_err());
    assert_eq!(work, before);
    assert!(!token.is_cancelled());
    assert_eq!(waiter_count(&token), 2);
    assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    token.cancel_measured(&mut work, crate::WorkBudget {
        items_examined: 2, ..crate::WorkBudget::UNBOUNDED
    })?;
    assert_eq!(work.items_examined, 2);
    assert_eq!(work.bytes_read, 7);
    assert_eq!(work.allocation_operations, 0);
    assert_eq!(counter.0.load(Ordering::Relaxed), 2);
    assert_eq!(first.as_mut().poll(&mut context), Poll::Ready(()));
    assert_eq!(second.as_mut().poll(&mut context), Poll::Ready(()));
    let completed = work;
    token.cancel_measured(&mut work, completed)?;
    assert_eq!(work, completed);
    Ok(())
}

#[cfg(feature = "native-mount")]
#[test]
fn original_cancel_wins_before_the_physical_commit() {
    let token = CancellationToken::new();
    token.cancel();
    let mut installed = false;
    assert!(token.commit_if_live(|| installed = true).is_err());
    assert!(!installed);
}

#[test]
fn measured_cancellation_precancel_and_denial_allocate_nothing() {
    for cancelled in [false, true] {
        let token = CancellationToken::new();
        if cancelled { token.cancel(); }
        let mut ledger = crate::kernel::allocation::AllocationLedger::default();
        let mut work = crate::WorkCounters::default();
        let mut admission = CancellationWaitAdmission::default();
        let mut context = Context::from_waker(Waker::noop());
        let mut future = token.cancelled_measured(
            &mut ledger, &mut work, crate::WorkBudget {
                allocation_operations: 0, ..crate::WorkBudget::UNBOUNDED
            }, &mut admission,
        );
        let result = Pin::new(&mut future).poll(&mut context);
        assert!(if cancelled { matches!(result, Poll::Ready(Ok(()))) }
            else { matches!(result, Poll::Ready(Err(CancellationWaitError::Work(_)))) });
        drop(future);
        assert_eq!(work, crate::WorkCounters::default());
        assert_eq!(ledger.live_bytes(), 0);
        assert_eq!(waiter_count(&token), 0);
    }
}

#[test]
fn prepared_cancellation_uses_original_registration_and_meter() {
    let token = CancellationToken::new();
    let mut ledger = crate::kernel::allocation::AllocationLedger::default();
    let mut work = crate::WorkCounters::default();
    let mut admission = CancellationWaitAdmission::default();
    let budget = crate::WorkBudget { items_examined: 1, ..crate::WorkBudget::UNBOUNDED };
    let mut prepared = token.prepare_cancelled_measured(&mut ledger, &mut work, budget, &mut admission)
        .expect("actual first registration admitted");
    assert_eq!(prepared.cleanup_items(), 1);
    assert_eq!(prepared.retained_waiter_bytes(), std::mem::size_of::<CancellationWaiter>() as u64);
    assert_eq!(work.allocation_operations, 2);
    assert_eq!(work.items_examined, 0);
    let owned = ledger.live_bytes();
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&counter));
    let mut context = Context::from_waker(&waker);
    assert_eq!(Pin::new(&mut prepared).poll(&mut context), Poll::Pending);
    assert_eq!(ledger.live_bytes(), owned);
    token.cancel();
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(Pin::new(&mut prepared).poll(&mut context), Poll::Ready(()));
    prepared.finish(&mut ledger, &mut work, budget, &mut admission).expect("reserved real cleanup");
    assert_eq!(work.items_examined, 1);
    assert_eq!(work.allocation_operations, 2);
    assert_eq!(ledger.live_bytes(), 0);
    assert_eq!(waiter_count(&token), 0);
}

#[test]
fn measured_cancellation_reuses_admitted_registry_slots() {
    let token = CancellationToken::new();
    let mut ledger = crate::kernel::allocation::AllocationLedger::default();
    let mut work = crate::WorkCounters::default();
    let mut admission = CancellationWaitAdmission::default();
    let mut context = Context::from_waker(Waker::noop());
    for expected_operations in [2, 3] {
        {
            let mut future = token.cancelled_measured(
                &mut ledger, &mut work, crate::WorkBudget::UNBOUNDED, &mut admission,
            );
            assert!(Pin::new(&mut future).poll(&mut context).is_pending());
            assert_eq!(waiter_count(&token), 1);
        }
        assert_eq!(waiter_count(&token), 0);
        assert_eq!(work.allocation_operations, expected_operations);
    }
    assert_eq!(work.items_examined, 3);
    token.cancel();
    {
        let mut future = token.cancelled_measured(
            &mut ledger, &mut work, crate::WorkBudget::UNBOUNDED, &mut admission,
        );
        assert!(matches!(Pin::new(&mut future).poll(&mut context), Poll::Ready(Ok(()))));
    }
    assert_eq!(ledger.live_bytes(), 0);
}

#[test]
fn measured_cancellation_reconciles_external_registry_growth() {
    let token = CancellationToken::new();
    let mut ledger = crate::kernel::allocation::AllocationLedger::default();
    let mut work = crate::WorkCounters::default();
    let mut admission = CancellationWaitAdmission::default();
    let budget = crate::WorkBudget::UNBOUNDED;
    let mut context = Context::from_waker(Waker::noop());
    let prepared = token.prepare_cancelled_measured(
        &mut ledger, &mut work, budget, &mut admission,
    ).expect("measured registration");
    let generation = token.state.registry_generation.load(Ordering::Relaxed);
    let mut external = [token.cancelled(), token.cancelled()];
    for future in &mut external {
        assert!(Pin::new(future).poll(&mut context).is_pending());
    }
    assert!(token.state.registry_generation.load(Ordering::Relaxed) > generation);
    prepared.finish(&mut ledger, &mut work, budget, &mut admission)
        .expect("original registration cleanup");
    assert_eq!(work.allocation_operations, 2);
    assert_eq!(work.items_examined, 1);
    assert_eq!(ledger.live_bytes(), 0);
    assert_eq!(waiter_count(&token), 2);
    drop(external);
    assert_eq!(waiter_count(&token), 0);
}

#[test]
fn measured_waiter_waker_drop_cancels_without_losing_release_work() {
    struct CancelOnDrop(CancellationToken);
    impl Wake for CancelOnDrop {
        fn wake(self: Arc<Self>) {}
    }
    impl Drop for CancelOnDrop {
        fn drop(&mut self) { self.0.cancel(); }
    }
    let token = CancellationToken::new();
    let mut ledger = crate::kernel::allocation::AllocationLedger::default();
    let mut work = crate::WorkCounters::default();
    let mut admission = CancellationWaitAdmission::default();
    let budget = crate::WorkBudget { items_examined: 1, ..crate::WorkBudget::UNBOUNDED };
    let mut prepared = token.prepare_cancelled_measured(&mut ledger, &mut work, budget, &mut admission).unwrap();
    let waker = Waker::from(Arc::new(CancelOnDrop(token.clone())));
    {
        let mut context = Context::from_waker(&waker);
        assert!(Pin::new(&mut prepared).poll(&mut context).is_pending());
    }
    drop(waker);
    prepared.finish(&mut ledger, &mut work, budget, &mut admission).unwrap();
    assert!(token.is_cancelled());
    assert_eq!(ledger.live_bytes(), 0);
    assert_eq!(work.items_examined, 1);
    assert_eq!(work.allocation_operations, 2);
}
