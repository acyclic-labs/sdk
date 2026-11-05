use super::*;

#[test]
fn cancellation_wakes_an_os_condition_without_a_timer() {
    let token = CancellationToken::new();
    let gate = Arc::new(std::sync::Mutex::new(()));
    let changed = Arc::new(std::sync::Condvar::new());
    let notification_gate = Arc::clone(&gate);
    let notification_changed = Arc::clone(&changed);
    let registration = token
        .on_cancel(None, move || {
            let _guard = notification_gate.lock().unwrap();
            notification_changed.notify_all();
        })
        .unwrap();
    let (entering, entered) = std::sync::mpsc::channel();
    let waiting_token = token.clone();
    let waiter = std::thread::spawn(move || {
        let mut guard = gate.lock().unwrap();
        entering.send(()).unwrap();
        while !waiting_token.is_cancelled() {
            guard = changed.wait(guard).unwrap();
        }
    });
    entered.recv().unwrap();
    token.cancel();
    waiter.join().unwrap();
    drop(registration);
}

#[test]
fn cancellation_notification_ends_with_its_registration() {
    for cancelled_first in [false, true] {
        let token = CancellationToken::new();
        let observed = Arc::new(AtomicBool::new(false));
        if cancelled_first {
            token.cancel();
        }
        let notification_observed = Arc::clone(&observed);
        let registration = token
            .on_cancel(None, move || {
                notification_observed.store(true, Ordering::Release);
            })
            .unwrap();
        assert_eq!(observed.load(Ordering::Acquire), cancelled_first);
        drop(registration);
        token.cancel();
        assert_eq!(observed.load(Ordering::Acquire), cancelled_first);
    }
}

fn budget(items: u64) -> crate::WorkBudget {
    crate::WorkBudget {
        items_examined: items,
        allocation_operations: 64,
        bytes_hashed: 0,
        bytes_copied: 4096,
        peak_allocation_bytes: 16384,
        ..crate::WorkBudget::UNBOUNDED
    }
}

#[test]
fn cancelled_cleanup_wait_preserves_live_registry_and_original_prefix() {
    let root = CancellationToken::new();
    let token = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(budget(64), &root)
        .map_err(|(error, _)| error)
        .unwrap();
    let registered = token.prepare_cancelled_with_native_work(&original).unwrap();
    root.cancel();
    let wait = CancellationToken::new();
    wait.cancel();
    let before = original.receipt();
    assert!(matches!(
        token.prepare_cancel_cleanup(Some(&wait)),
        Err(crate::NativeTreeMaterializationError::Cancelled)
    ));
    assert!(!token.is_cancelled());
    assert_eq!(original.receipt(), before);
    drop(registered);
}

#[test]
fn ordinary_preexisting_registry_cannot_fabricate_native_delivery_credit() {
    let token = CancellationToken::new();
    let mut ordinary = token.cancelled();
    assert!(
        Pin::new(&mut ordinary)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(matches!(
        token.prepare_cancel_cleanup(Some(&CancellationToken::new())),
        Err(crate::NativeTreeMaterializationError::InvalidLayout)
    ));
    assert!(!token.is_cancelled());
    drop(ordinary);
}

struct DropWaiterOnWake {
    waiter: Mutex<Option<Cancelled<'static>>>,
    work: crate::NativeMaterializationWork,
    retired: Arc<AtomicBool>,
}

impl std::task::Wake for DropWaiterOnWake {
    fn wake(self: Arc<Self>) {
        let before = self.work.live_allocation_bytes();
        drop(
            self.waiter
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        );
        self.retired.store(
            self.work.live_allocation_bytes() < before,
            Ordering::Release,
        );
    }
}

#[test]
fn delivery_does_not_retain_a_dropped_later_waiters_allocation() {
    let token = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(budget(64), &token)
        .map_err(|(error, _)| error)
        .unwrap();
    let backing = |layout| crate::arc_layout::allocation_bytes(layout).unwrap() as u64;
    let state_bytes = backing(std::alloc::Layout::new::<CancellationState>());
    let waiter_bytes = backing(std::alloc::Layout::new::<CancellationWaiter>());
    let baseline = work.live_allocation_bytes();
    for linked in [false, true] {
        let child = if linked {
            CancellationToken::linked_child_with_native_work(&token, &work)
        } else {
            CancellationToken::new_with_native_work(&work)
        }
        .unwrap();
        let extra = if linked {
            waiter_bytes + backing(std::alloc::Layout::new::<ChildCancellationWake>())
        } else {
            0
        };
        assert_eq!(
            work.live_allocation_bytes(),
            baseline + state_bytes + extra,
            "cancellation owners must retain their complete Arc backing"
        );
        drop(child);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    let notify: fn() = || {};
    let registration = token.on_cancel(Some(&work), notify).unwrap();
    assert_eq!(
        work.live_allocation_bytes(),
        baseline
            + waiter_bytes
            + backing(std::alloc::Layout::new::<
                notification::CancellationNotification<fn()>,
            >()),
        "cancellation notification must retain its complete Arc backing"
    );
    drop(registration);
    assert_eq!(work.live_allocation_bytes(), baseline);
    let later = token.prepare_cancelled_with_native_work(&work).unwrap();
    let mut first = token.prepare_cancelled_with_native_work(&work).unwrap();
    assert_eq!(work.live_allocation_bytes(), baseline + 2 * waiter_bytes);
    let retired = Arc::new(AtomicBool::new(false));
    let waker = Waker::from(Arc::new(DropWaiterOnWake {
        waiter: Mutex::new(Some(later)),
        work: work.clone(),
        retired: retired.clone(),
    }));
    assert!(
        Pin::new(&mut first)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    token.cancel();
    assert!(retired.load(Ordering::Acquire));
    drop((waker, first));
    assert_eq!(
        work.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    work.receipt().verify(budget(64)).unwrap();

    let token = CancellationToken::new();
    let mut caller = crate::kernel::KernelAccounting::standalone(budget(64), &token);
    let anchor =
        crate::kernel::allocation::retained::RetainedAllocation::claim(0, 0, &mut caller).unwrap();
    let (allocations, counters, budget, _) = caller.standalone_parts().unwrap();
    let before = allocations.live_bytes();
    let registered = token
        .prepare_cancelled_measured(allocations, counters, budget)
        .unwrap();
    assert_eq!(
        caller.standalone_parts().unwrap().0.live_bytes(),
        before + waiter_bytes,
        "standalone cancellation waiter must retain its complete Arc backing"
    );
    drop(registered);
    assert_eq!(caller.standalone_parts().unwrap().0.live_bytes(), before);
    drop(anchor);
}
