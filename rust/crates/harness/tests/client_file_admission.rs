use super::*;
use std::cell::Cell;
#[path = "../../harness-fuzz/support/client_offline.rs"]
mod offline;

async fn lazy_quota(length: u64, copy_early: bool) {
    let outbox = volume(Arc::new(Provider::default()), "client/files").await;
    let target = volume(Arc::new(Provider::default()), "agent/files").await;
    let mut client = FileClient::open(
        outbox.clone(),
        target,
        FileClientLimits {
            commands: 1,
            bytes: 3,
            ..LIMITS
        },
    )
    .await
    .unwrap();
    let before = outbox.head().await.unwrap();
    let copied = Cell::new(copy_early);
    let result = client
        .enqueue_with(request(1).key, length, || {
            copied.set(true);
            Ok(vec![0].into())
        })
        .await;
    assert!(matches!(
        result,
        Err(FileClientError::ByteQuota { limit: 3 })
    ));
    assert!(!copied.get(), "quota rejection copied the platform payload");
    assert_eq!(client.pending(), 0);
    assert_eq!(outbox.head().await.unwrap(), before);
}

async fn reject_then_continue(id: u8, checkpoint_failure: bool) {
    let provider = Arc::new(Provider::default());
    let outbox = volume(provider.clone(), "client/files").await;
    let target = volume(Arc::new(Provider::default()), "agent/files").await;
    let rejected = request(id);
    let good = request(id.wrapping_add(1));
    target
        .append(
            target.head().await.unwrap(),
            &Write::new(rejected.key.clone(), vec![id ^ 1]),
        )
        .await
        .unwrap();
    let mut client = FileClient::open(outbox.clone(), target.clone(), LIMITS)
        .await
        .unwrap();
    client.enqueue(rejected.clone()).await.unwrap();
    client.enqueue(good.clone()).await.unwrap();
    if checkpoint_failure {
        provider.fail_commit.store(
            provider.commits.load(Ordering::SeqCst) + 1,
            Ordering::SeqCst,
        );
        assert!(matches!(
            client.flush().await,
            Err(FileClientError::Unavailable { .. })
        ));
        assert_eq!(
            client.pending(),
            2,
            "uncommitted rejection cannot retire intent"
        );
    }
    assert!(
        matches!(client.flush().await, Err(FileClientError::Rejected { request, .. }) if request == rejected.key)
    );
    assert_eq!(client.pending(), 1);
    drop(client);
    let mut client = FileClient::open(outbox.clone(), target.clone(), LIMITS)
        .await
        .unwrap();
    assert_eq!(client.pending(), 1);
    assert_eq!(client.flush().await.unwrap(), 1);
    assert_eq!(
        target
            .snapshot(&Query::File(good.key.clone()), 1)
            .await
            .unwrap()
            .files,
        vec![good]
    );
    drop(client);
    assert_eq!(
        FileClient::open(outbox, target, LIMITS)
            .await
            .unwrap()
            .pending(),
        0
    );
}

#[test]
fn negative_control_copying_before_admission_is_detected() {
    let failure = std::panic::catch_unwind(|| {
        futures::executor::block_on(lazy_quota(16 * 1024 * 1024, true))
    })
    .expect_err("copy-before-admission passed");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("quota rejection copied"), "{message}");
}

proptest::proptest! {
    #[test]
    fn offline_policy_preserves_only_reference_commands(bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..512)) {
        offline::exercise(&bytes, acyclic_harness::client::validate_offline_request);
    }
    #[test]
    fn quota_rejection_never_invokes_the_payload_producer(length in 4u64..=u64::MAX) {
        futures::executor::block_on(lazy_quota(length, false));
    }
    #[test]
    fn terminal_rejection_retires_only_after_checkpoint(id: u8, fail: bool) {
        futures::executor::block_on(reject_then_continue(id, fail));
    }
}

#[test]
fn offline_oracle_detects_both_missing_policy_and_always_rejection() {
    for broken in [offline::broken_allow_all, offline::broken_reject_all] {
        assert!(std::panic::catch_unwind(|| offline::exercise(b"{}", broken)).is_err());
    }
}

#[test]
fn negative_control_ignoring_owner_token_is_detected() {
    let failure = std::panic::catch_unwind(|| {
        offline::exercise(b"pinned", offline::broken_discard_owner_token)
    })
    .expect_err("owner token bypass was accepted");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("nested owner metadata"), "{message}");
}
