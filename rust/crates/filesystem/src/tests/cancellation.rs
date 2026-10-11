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
    let mut work = crate::WorkCounters { object_bytes_read: 7, ..crate::WorkCounters::UNCHARGED };
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
    assert_eq!(work.object_bytes_read, 7);
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

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_wait_pre_cancel_and_allocation_denial_leave_registry_unallocated() {
    let token = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget { allocation_operations: 1, ..crate::WorkBudget::UNBOUNDED },
        &token,
    ).map_err(|(error, _)| error).unwrap();
    let prefix = work.receipt();
    assert!(matches!(token.prepare_cancelled_with_native_work(&work),
        Err(CancellationWaitError::Native(crate::NativeTreeMaterializationError::Work(_)))));
    assert_eq!(work.receipt(), prefix);
    assert_eq!(token.state.waiters.lock().unwrap().capacity(), 0);
    token.cancel();
    let prepared = token.prepare_cancelled_with_native_work(&work).unwrap();
    assert_eq!(prepared.cleanup_items(), 0);
    drop(prepared);
    assert_eq!(work.receipt(), prefix);
    assert_eq!(token.state.waiters.lock().unwrap().capacity(), 0);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_original_token_registry_has_no_strong_cycle_after_last_future_drop() {
    let token = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token)
        .map_err(|(error, _)| error).unwrap();
    let state = Arc::downgrade(&token.state);
    let original = work.original_demand_work();
    let accounting = Arc::downgrade(original.meter());
    let prepared = token.prepare_cancelled_with_native_work(&work).unwrap();
    drop(original);
    drop(work);
    drop(token);
    assert!(accounting.upgrade().is_some());
    assert!(state.upgrade().is_some());
    drop(prepared);
    assert!(accounting.upgrade().is_none());
    assert!(state.upgrade().is_none());
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn concurrent_native_registration_and_last_unregister_leave_no_unpaid_registry() {
    let token = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(crate::WorkBudget {
        items_examined: 4_096, allocation_operations: 4_096,
        bytes_copied: 65_536,
        peak_allocation_bytes: 1_048_576, ..crate::WorkBudget::UNBOUNDED
    }, &token).map_err(|(error, _)| error).unwrap();
    let live = work.live_allocation_bytes();
    std::thread::scope(|scope| {
        for _ in 0..2 {
            scope.spawn(|| {
                for _ in 0..100 {
                    let prepared = token.prepare_cancelled_with_native_work(&work).unwrap();
                    std::thread::yield_now();
                    drop(prepared);
                }
            });
        }
    });
    assert_eq!(waiter_count(&token), 0);
    assert_eq!(token.state.waiters.lock().unwrap().capacity(), 0);
    assert_eq!(work.live_allocation_bytes(), live);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_waker_replacement_can_cancel_the_same_token_without_lock_reentrancy() {
    struct CancelOnDrop(CancellationToken);
    impl Wake for CancelOnDrop { fn wake(self: Arc<Self>) {} }
    impl Drop for CancelOnDrop { fn drop(&mut self) { self.0.cancel(); } }
    let token = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(crate::WorkBudget {
        items_examined: 6, ..crate::WorkBudget::UNBOUNDED
    }, &token).map_err(|(error, _)| error).unwrap();
    let live = work.live_allocation_bytes();
    let mut prepared = token.prepare_cancelled_with_native_work(&work).unwrap();
    let old = Waker::from(Arc::new(CancelOnDrop(token.clone())));
    assert!(Pin::new(&mut prepared).poll(&mut Context::from_waker(&old)).is_pending());
    drop(old);
    assert!(Pin::new(&mut prepared).poll(&mut Context::from_waker(Waker::noop())).is_ready());
    assert!(token.is_cancelled());
    drop(prepared);
    assert_eq!(work.live_allocation_bytes(), live);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_linked_child_stop_is_local_and_parent_propagation_uses_prepaid_visits() {
    for stop_parent in [false, true] {
        let parent = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &parent)
            .map_err(|(error, _)| error).unwrap();
        let child = CancellationToken::linked_child_with_native_work(&parent, &original).unwrap();
        let scope = original.native_volume_work(crate::WorkBudget {
            items_examined: 6, ..crate::WorkBudget::UNBOUNDED
        }).unwrap();
        let mut prepared = child.prepare_cancelled_with_native_work(&scope).unwrap();
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let wake = Waker::from(Arc::clone(&counter));
        assert!(Pin::new(&mut prepared).poll(&mut Context::from_waker(&wake)).is_pending());
        assert_eq!(scope.receipt().items_examined, original.receipt().items_examined);
        assert!(scope.charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).is_err(), "future lookup, slot visits, and cleanup remain reserved, not completed");
        let before = original.receipt().items_examined;
        if stop_parent { parent.cancel(); } else { child.cancel(); }
        assert!(child.is_cancelled());
        assert_eq!(parent.is_cancelled(), stop_parent);
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        assert!(Pin::new(&mut prepared).poll(&mut Context::from_waker(&wake)).is_ready());
        assert_eq!(waiter_count(&parent), 0, "local stop detaches its original parent subscription");
        assert_eq!(original.receipt().items_examined, before + if stop_parent { 5 } else { 3 },
            "child lookup, delivery, and parent unregister complete; parent stop also completes its own lookup and delivery");
        drop(prepared);
        assert_eq!(original.receipt().items_examined, before + if stop_parent { 6 } else { 4 },
            "the actual child unregister lookup consumes its separate original cleanup credit");
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_linked_child_precancel_and_initial_budget_denial_allocate_nothing() {
    for pre_cancel in [false, true] {
        let parent = CancellationToken::new();
        let budget = if pre_cancel { crate::WorkBudget::UNBOUNDED } else {
            crate::WorkBudget { allocation_operations: 1, ..crate::WorkBudget::UNBOUNDED }
        };
        let original = crate::NativeMaterializationWork::new(budget, &parent)
            .map_err(|(error, _)| error).unwrap();
        if pre_cancel { parent.cancel(); }
        let before = original.receipt();
        assert!(CancellationToken::linked_child_with_native_work(&parent, &original).is_err());
        assert_eq!(original.receipt(), before);
        assert_eq!(waiter_count(&parent), 0);
        assert_eq!(parent.state.waiters.lock().unwrap().capacity(), 0);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_linked_child_parent_registration_and_guards_have_no_strong_cycle() {
    let parent = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &parent)
        .map_err(|(error, _)| error).unwrap();
    let child = CancellationToken::linked_child_with_native_work(&parent, &original).unwrap();
    let parent_state = Arc::downgrade(&parent.state);
    let child_state = Arc::downgrade(&child.state);
    let accounting = Arc::downgrade(original.native_read_meter());
    drop(original);
    drop(parent);
    assert!(accounting.upgrade().is_some());
    drop(child);
    assert!(child_state.upgrade().is_none());
    assert!(parent_state.upgrade().is_none());
    assert!(accounting.upgrade().is_none());
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[test]
fn native_linked_children_share_one_ancestor_commit_lock_and_never_commit_after_parent_stop() {
    let parent = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &parent)
        .map_err(|(error, _)| error).unwrap();
    let first = CancellationToken::linked_child_with_native_work(&parent, &original).unwrap();
    let second = CancellationToken::linked_child_with_native_work(&parent, &original).unwrap();
    let mut installed = 0;
    first.commit_if_both_live(&second, || installed += 1).unwrap();
    assert_eq!(installed, 1, "the shared parent is deduplicated, not locked recursively twice");
    parent.cancel();
    assert!(first.commit_if_both_live(&second, || installed += 1).is_err());
    assert_eq!(installed, 1);
}
