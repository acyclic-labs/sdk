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
async fn notifications_coalesce_monotonically_and_isolate_authorities() {
    let store = MemoryNotificationStore::new();
    let authority = AuthorityId::from_bytes([7; 16]);
    let other = AuthorityId::from_bytes([10; 16]);
    let cancellation = CancellationToken::new();
    let baseline = head(1, 4, 4);
    assert_eq!(
        store
            .poll_hint_after(authority, baseline, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap()
            .value,
        NotificationPoll::Unchanged
    );
    for hint in [head(1, 6, 6), head(1, 5, 5), head(1, 6, 99)] {
        store
            .publish_hint(authority, hint, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .poll_hint_after(authority, baseline, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap()
            .value,
        NotificationPoll::Advanced(head(1, 6, 6))
    );
    assert_eq!(
        store
            .poll_hint_after(
                authority,
                head(1, 6, 6),
                WorkBudget::UNBOUNDED,
                &cancellation
            )
            .await
            .unwrap()
            .value,
        NotificationPoll::Unchanged
    );
    assert_eq!(
        store
            .poll_hint_after(
                other,
                Head::genesis(Epoch::GENESIS),
                WorkBudget::UNBOUNDED,
                &cancellation
            )
            .await
            .unwrap()
            .value,
        NotificationPoll::Unchanged
    );
    store
        .publish_hint(
            authority,
            head(2, 0, 9),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .poll_hint_after(
                authority,
                head(1, 99, 8),
                WorkBudget::UNBOUNDED,
                &cancellation
            )
            .await
            .unwrap()
            .value,
        NotificationPoll::Advanced(head(2, 0, 9))
    );
}

#[tokio::test]
async fn cancellation_and_budget_reject_publish_and_poll() {
    let store = MemoryNotificationStore::new();
    let authority = AuthorityId::from_bytes([8; 16]);
    let hint = head(1, 1, 1);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let live = CancellationToken::new();
    for (budget, cancellation) in [
        (WorkBudget::UNBOUNDED, &cancelled),
        (WorkBudget::default(), &live),
    ] {
        for error in [
            store
                .publish_hint(authority, hint, budget, cancellation)
                .await
                .unwrap_err()
                .error,
            store
                .poll_hint_after(authority, hint, budget, cancellation)
                .await
                .unwrap_err()
                .error,
        ] {
            if cancellation.is_cancelled() {
                assert!(matches!(error, NotificationError::Cancelled));
            } else {
                assert!(matches!(error, NotificationError::Work(_)));
            }
        }
    }
    assert_eq!(
        store
            .poll_hint_after(
                authority,
                Head::genesis(Epoch::GENESIS),
                WorkBudget::UNBOUNDED,
                &live
            )
            .await
            .unwrap()
            .value,
        NotificationPoll::Unchanged
    );
}
