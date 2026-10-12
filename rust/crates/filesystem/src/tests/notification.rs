use super::*;
use crate::foundation::{Digest, Epoch, Sequence};

fn head(epoch: u64, sequence: u64, digest: u8) -> Head {
    Head {
        epoch: Epoch::new(epoch).unwrap_or(Epoch::GENESIS),
        sequence: Sequence::new(sequence),
        digest: Digest::from_bytes([digest; 32]),
    }
}

#[tokio::test]
async fn memory_notifications_coalesce_monotonically_and_never_claim_authority() {
    let store = MemoryNotificationStore::new();
    let authority = AuthorityId::from_bytes([7; 16]);
    let baseline = head(1, 4, 4);
    assert!(matches!(
        store.poll_hint_after(authority, baseline, WorkBudget::UNBOUNDED, &CancellationToken::new()).await,
        Ok(OperationReceipt {
            value: NotificationPoll::Unchanged,
            ..
        })
    ));
    assert!(
        store
            .publish_hint(authority, head(1, 6, 6), WorkBudget::UNBOUNDED, &CancellationToken::new()).await
            .is_ok()
    );
    assert!(
        store
            .publish_hint(authority, head(1, 5, 5), WorkBudget::UNBOUNDED, &CancellationToken::new()).await
            .is_ok()
    );
    assert!(matches!(
        store.poll_hint_after(authority, baseline, WorkBudget::UNBOUNDED, &CancellationToken::new()).await,
        Ok(OperationReceipt {
            value: NotificationPoll::Advanced(value),
            ..
        }) if value == head(1, 6, 6)
    ));
    assert!(
        store
            .publish_hint(authority, head(2, 0, 9), WorkBudget::UNBOUNDED, &CancellationToken::new()).await
            .is_ok()
    );
    assert!(matches!(
        store.poll_hint_after(authority, head(1, 99, 8), WorkBudget::UNBOUNDED, &CancellationToken::new()).await,
        Ok(OperationReceipt {
            value: NotificationPoll::Advanced(value),
            ..
        }) if value == head(2, 0, 9)
    ));
}

#[tokio::test]
async fn cancellation_and_budget_reject_before_adapter_access() {
    let store = MemoryNotificationStore::new();
    let authority = AuthorityId::from_bytes([8; 16]);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = AsyncNotificationStore::publish_hint(
        &store,
        authority,
        head(1, 1, 1),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ).await;
    assert!(matches!(
        result,
        Err(OperationFailure {
            error: NotificationError::Cancelled,
            ..
        })
    ));
    assert!(matches!(
        store.publish_hint(authority, head(1, 1, 1), WorkBudget::default(), &CancellationToken::new()).await,
        Err(OperationFailure {
            error: NotificationError::Work(_),
            ..
        })
    ));
}

#[tokio::test]
async fn poll_hints_are_isolated_monotonic_bounded_and_cancellable() {
    let store = MemoryNotificationStore::new();
    let first = AuthorityId::from_bytes([9; 16]);
    let second = AuthorityId::from_bytes([10; 16]);
    let current = head(1, 2, 2);
    let published = store.publish_hint(first, current, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.ok();
    assert_eq!(
        published.map(|receipt| receipt.work.backend_write_operations),
        Some(1)
    );
    assert!(matches!(
        store.poll_hint_after(first, current, WorkBudget::UNBOUNDED, &CancellationToken::new()).await,
        Ok(OperationReceipt {
            value: NotificationPoll::Unchanged,
            ..
        })
    ));
    assert!(
        store
            .publish_hint(first, head(1, 2, 99), WorkBudget::UNBOUNDED, &CancellationToken::new()).await
            .is_ok()
    );
    assert!(matches!(
        store.poll_hint_after(first, head(1, 1, 1), WorkBudget::UNBOUNDED, &CancellationToken::new()).await,
        Ok(OperationReceipt {
            value: NotificationPoll::Advanced(value),
            ..
        }) if value == current
    ));
    assert!(matches!(
        store.poll_hint_after(second, Head::genesis(Epoch::GENESIS), WorkBudget::UNBOUNDED, &CancellationToken::new()).await,
        Ok(OperationReceipt {
            value: NotificationPoll::Unchanged,
            work,
        }) if work.backend_read_operations == 1
    ));

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        AsyncNotificationStore::poll_hint_after(
            &store,
            first,
            Head::genesis(Epoch::GENESIS),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ).await,
        Err(OperationFailure {
            error: NotificationError::Cancelled,
            ..
        })
    ));
    assert!(matches!(
        store.poll_hint_after(first, current, WorkBudget::default(), &CancellationToken::new()).await,
        Err(OperationFailure {
            error: NotificationError::Work(_),
            ..
        })
    ));
}
