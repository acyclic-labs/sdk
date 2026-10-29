use super::*;

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
    let later = token.prepare_cancelled_with_native_work(&work).unwrap();
    let mut first = token.prepare_cancelled_with_native_work(&work).unwrap();
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
}

#[cfg(feature = "native-watch")]
#[test]
fn blocking_notifications_preserve_early_events_original_credit_and_retirement() {
    static NOTIFICATIONS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    fn notify() {
        NOTIFICATIONS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    let root = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(budget(128), &root)
        .map_err(|(error, _)| error)
        .unwrap();
    let baseline = original.live_allocation_bytes();
    for cancel_before_registration in [false, true] {
        let token = CancellationToken::new();
        if cancel_before_registration {
            token.cancel();
        }
        let before = NOTIFICATIONS.load(std::sync::atomic::Ordering::SeqCst);
        let mut wait = token.prepare_cancelled_with_native_work(&original).unwrap();
        if !cancel_before_registration {
            assert!(original.live_allocation_bytes() > baseline);
            // Delivery detached the node before its notification was armed.
            token.cancel();
        }
        wait.notify(notify);
        assert_eq!(
            NOTIFICATIONS.load(std::sync::atomic::Ordering::SeqCst),
            before + 1
        );
        drop(wait);
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    let token = CancellationToken::new();
    let mut wait = token.prepare_cancelled_with_native_work(&original).unwrap();
    wait.notify(notify);
    let before = NOTIFICATIONS.load(std::sync::atomic::Ordering::SeqCst);
    token.cancel();
    assert_eq!(
        NOTIFICATIONS.load(std::sync::atomic::Ordering::SeqCst),
        before + 1
    );
    drop(wait);
    assert_eq!(original.live_allocation_bytes(), baseline);

    let token = CancellationToken::new();
    let mut wait = token.prepare_cancelled_with_native_work(&original).unwrap();
    wait.notify(notify);
    drop(wait);
    let before = NOTIFICATIONS.load(std::sync::atomic::Ordering::SeqCst);
    token.cancel();
    assert_eq!(
        NOTIFICATIONS.load(std::sync::atomic::Ordering::SeqCst),
        before
    );
    assert_eq!(original.live_allocation_bytes(), baseline);
    original.receipt().verify(budget(128)).unwrap();

    let mut demand = crate::demand::DemandWork::new(
        crate::WorkBudget {
            allocation_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &root,
    );
    let token = CancellationToken::new();
    assert!(matches!(
        demand.prepare_cancelled(&token),
        Err(CancellationWaitError::Work(_))
            | Err(CancellationWaitError::Accounting(crate::FsError::Work(_)))
    ));
    assert!(
        token
            .state
            .waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .upgrade()
            .is_none()
    );
    token.cancel();
    assert_eq!(
        NOTIFICATIONS.load(std::sync::atomic::Ordering::SeqCst),
        before
    );
}
