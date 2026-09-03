use super::*;
use crate::foundation::{Digest, Epoch, Sequence};

fn head(epoch: u64, sequence: u64, digest: u8) -> Head {
    Head {
        epoch: Epoch::new(epoch).unwrap_or(Epoch::GENESIS),
        sequence: Sequence::new(sequence),
        digest: Digest::from_bytes([digest; 32]),
    }
}

#[test]
fn memory_notifications_coalesce_monotonically_and_never_claim_authority() {
    let store = MemoryNotificationStore::new();
    let authority = AuthorityId::from_bytes([7; 16]);
    let baseline = head(1, 4, 4);
    assert!(matches!(
        store.poll_after(authority, baseline, WorkBudget::UNBOUNDED),
        Ok(OperationReceipt {
            value: NotificationPoll::Unchanged,
            ..
        })
    ));
    assert!(
        store
            .publish(authority, head(1, 6, 6), WorkBudget::UNBOUNDED)
            .is_ok()
    );
    assert!(
        store
            .publish(authority, head(1, 5, 5), WorkBudget::UNBOUNDED)
            .is_ok()
    );
    assert!(matches!(
        store.poll_after(authority, baseline, WorkBudget::UNBOUNDED),
        Ok(OperationReceipt {
            value: NotificationPoll::Advanced(value),
            ..
        }) if value == head(1, 6, 6)
    ));
    assert!(
        store
            .publish(authority, head(2, 0, 9), WorkBudget::UNBOUNDED)
            .is_ok()
    );
    assert!(matches!(
        store.poll_after(authority, head(1, 99, 8), WorkBudget::UNBOUNDED),
        Ok(OperationReceipt {
            value: NotificationPoll::Advanced(value),
            ..
        }) if value == head(2, 0, 9)
    ));
}

#[test]
fn cancellation_and_budget_reject_before_adapter_access() {
    let store = MemoryNotificationStore::new();
    let authority = AuthorityId::from_bytes([8; 16]);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = crate::async_storage::poll_ready(AsyncNotificationStore::publish_hint(
        &store,
        authority,
        head(1, 1, 1),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        result,
        Some(Err(OperationFailure {
            error: NotificationError::Cancelled,
            ..
        }))
    ));
    assert!(matches!(
        store.publish(authority, head(1, 1, 1), WorkBudget::default()),
        Err(OperationFailure {
            error: NotificationError::Work(_),
            ..
        })
    ));
}

#[test]
fn asynchronous_notification_adapter_preserves_successful_receipts()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryNotificationStore::new();
    let authority = AuthorityId::from_bytes([11; 16]);
    let cancellation = CancellationToken::new();
    let published = crate::async_storage::poll_ready(AsyncNotificationStore::publish_hint(
        &store,
        authority,
        head(3, 7, 7),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory notification adapter blocked")??;
    assert_eq!(published.work.backend_write_operations, 1);
    let polled = crate::async_storage::poll_ready(AsyncNotificationStore::poll_hint_after(
        &store,
        authority,
        head(3, 6, 6),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory notification poll blocked")??;
    assert_eq!(polled.value, NotificationPoll::Advanced(head(3, 7, 7)));
    assert_eq!(polled.work.backend_read_operations, 1);
    Ok(())
}

#[test]
fn poll_hints_are_isolated_monotonic_bounded_and_cancellable() {
    let store = MemoryNotificationStore::new();
    let first = AuthorityId::from_bytes([9; 16]);
    let second = AuthorityId::from_bytes([10; 16]);
    let current = head(1, 2, 2);
    let published = store.publish(first, current, WorkBudget::UNBOUNDED).ok();
    assert_eq!(
        published.map(|receipt| receipt.work.backend_write_operations),
        Some(1)
    );
    assert!(matches!(
        store.poll_after(first, current, WorkBudget::UNBOUNDED),
        Ok(OperationReceipt {
            value: NotificationPoll::Unchanged,
            ..
        })
    ));
    assert!(
        store
            .publish(first, head(1, 2, 99), WorkBudget::UNBOUNDED)
            .is_ok()
    );
    assert!(matches!(
        store.poll_after(first, head(1, 1, 1), WorkBudget::UNBOUNDED),
        Ok(OperationReceipt {
            value: NotificationPoll::Advanced(value),
            ..
        }) if value == current
    ));
    assert!(matches!(
        store.poll_after(second, Head::genesis(Epoch::GENESIS), WorkBudget::UNBOUNDED),
        Ok(OperationReceipt {
            value: NotificationPoll::Unchanged,
            work,
        }) if work.backend_read_operations == 1
    ));

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        crate::async_storage::poll_ready(AsyncNotificationStore::poll_hint_after(
            &store,
            first,
            Head::genesis(Epoch::GENESIS),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        Some(Err(OperationFailure {
            error: NotificationError::Cancelled,
            ..
        }))
    ));
    assert!(matches!(
        store.poll_after(first, current, WorkBudget::default()),
        Err(OperationFailure {
            error: NotificationError::Work(_),
            ..
        })
    ));
}
