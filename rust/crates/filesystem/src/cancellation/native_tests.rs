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
        token.prepare_cancel_with_cleanup_wait(&wait),
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
        token.prepare_cancel_with_cleanup_wait(&CancellationToken::new()),
        Err(crate::NativeTreeMaterializationError::InvalidLayout)
    ));
    assert!(!token.is_cancelled());
    drop(ordinary);
}
