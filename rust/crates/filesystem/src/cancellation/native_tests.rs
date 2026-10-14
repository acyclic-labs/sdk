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
