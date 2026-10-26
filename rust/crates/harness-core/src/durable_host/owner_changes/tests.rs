use super::*;
use crate::durable_host::tests::fixture::{Fixture, fixture};
use crate::runtime::TaskSpawner as _;
use acyclic_stream::SystemUnixMillisClock;
use std::time::Duration;

async fn scope_expiring_fixture() -> Result<(Fixture, u64)> {
    struct TaskClock;
    impl UnixMillisClock for TaskClock {
        fn now_unix_millis(&self) -> u64 {
            0
        }
    }
    let mut fixture = fixture().await?;
    let now = SystemUnixMillisClock.now_unix_millis() / 1000;
    let expiry = now + 2;
    let host = Arc::get_mut(&mut fixture.host)
        .ok_or_else(|| Error::Storage("fixture host is shared".into()))?;
    host.owner_scope = host.owner_scope.clone().with_workload_expiry(
        expiry,
        &fixture.issuer.public_workload_trust()?,
        fixture.issuer.audience(),
        now,
    )?;
    // Authorization uses the trusted system clock, independently of a task's
    // injected deadline clock. This task has no deadline or cancellation.
    host.clock = Arc::new(TaskClock);
    Ok((fixture, expiry * 1000))
}

#[tokio::test]
async fn signed_scope_expiry_invalidates_cached_dispatch_before_the_driver_runs() -> Result<()> {
    let (fixture, expiry) = scope_expiring_fixture().await?;
    let owner = fixture.running(1).await?;
    let changes = owner.owner_changes().await?;
    let signals = changes.signals();
    owner.verify(false, &mut None).await?;
    assert!(!signals.needs_verification());
    // Block this current-thread runtime so the asynchronous driver cannot
    // rescue a stale cached dispatch check after the signed scope expires.
    std::thread::sleep(Duration::from_millis(
        expiry.saturating_sub(SystemUnixMillisClock.now_unix_millis()),
    ));
    assert!(
        signals.needs_verification(),
        "signed scope expiry left cached dispatch authorized"
    );
    assert!(owner.verify(false, &mut None).await.is_err());
    Ok(())
}

#[tokio::test]
async fn signed_scope_expiry_wakes_a_quiet_owner_without_a_task_deadline() -> Result<()> {
    let (fixture, expiry) = scope_expiring_fixture().await?;
    let owner = fixture.running(1).await?;
    let mut changes = owner.owner_changes().await?;
    owner.verify(false, &mut None).await?;
    tokio::time::sleep(Duration::from_millis(
        expiry.saturating_sub(SystemUnixMillisClock.now_unix_millis()),
    ))
    .await;
    tokio::time::timeout(Duration::from_millis(200), changes.changed())
        .await
        .map_err(|_| Error::Storage("signed scope expiry did not wake a quiet owner".into()))??;
    assert!(owner.verify(false, &mut None).await.is_err());
    Ok(())
}

#[tokio::test]
async fn deadline_clock_jump_wakes_a_quiet_owner() -> Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    struct Clock(AtomicU64);
    impl UnixMillisClock for Clock {
        fn now_unix_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    let mut fixture = fixture().await?;
    let clock = Arc::new(Clock(AtomicU64::new(0)));
    Arc::get_mut(&mut fixture.host)
        .ok_or_else(|| Error::Storage("fixture host is shared".into()))?
        .clock = clock.clone();
    let mut admission = fixture.admission(1, serde_json::json!(0));
    admission.run_limits.deadline_epoch_ms = Some(10_000);
    fixture.host.admit(admission).await?;
    fixture.claim(1, "deadline-jump").await?;
    let owner = fixture.owner(1).await?;
    let mut changes = owner.owner_changes().await?;
    owner.verify(false, &mut None).await?;
    let mut changed = std::pin::pin!(changes.changed());
    assert!(futures::poll!(changed.as_mut()).is_pending());
    let started = std::time::Instant::now();
    clock.0.store(10_001, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_millis(200), changed)
        .await
        .map_err(|_| Error::Storage("wall deadline jump did not wake a quiet owner".into()))??;
    assert!(owner.verify(false, &mut None).await.is_err());
    eprintln!(
        "OWNER_DEADLINE_JUMP wake_us={}",
        started.elapsed().as_micros()
    );
    Ok(())
}

#[tokio::test]
async fn external_cancellation_after_registration_cannot_be_lost() -> Result<()> {
    let fixture = fixture().await?;
    let owner = fixture.running(1).await?;
    let mut changes = owner.owner_changes().await?;
    owner.verify(false, &mut None).await?;
    let mut external = DistributedCoordinator::open(
        &fixture.stream,
        fixture.reader.clone(),
        Some(Arc::clone(&fixture.host.payloads)),
        &mut None,
    )
    .await?;
    external
        .apply(
            IdempotencyKey::new("external-owner-cancel")?,
            SchedulerEvent::CancellationRequested {
                operation_id: owner.operation_id(),
                recursive: true,
            },
            &mut None,
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(1), changes.changed())
        .await
        .map_err(|cause| Error::Storage(cause.to_string()))??;
    assert!(owner.verify(false, &mut None).await.is_err());
    Ok(())
}

#[tokio::test]
async fn unrelated_tasks_do_not_wake_a_quiet_owner_and_drop_reclaims_routes() -> Result<()> {
    let fixture = fixture().await?;
    let owner = fixture.running(1).await?;
    let sibling = fixture.running(2).await?;
    let mut changes = owner.owner_changes().await?;
    owner.verify(false, &mut None).await?;
    fixture.host.cancel(sibling.task_id()).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), changes.changed())
            .await
            .is_err()
    );
    owner.verify(false, &mut None).await?;
    let hub = fixture
        .host
        .owner_changes
        .get()
        .ok_or_else(|| Error::Storage("missing owner follow".into()))?;
    assert_eq!(locked(&hub.subscribers).by_operation.len(), 1);
    drop(changes);
    assert!(locked(&hub.subscribers).by_operation.is_empty());
    assert!(locked(&hub.subscribers).deadlines.is_empty());
    assert!(locked(&hub.subscribers).scope_deadlines.is_empty());
    Ok(())
}

#[tokio::test]
async fn invocation_cancellation_wakes_without_a_scheduler_commit() -> Result<()> {
    let fixture = fixture().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let owner = fixture
        .running(1)
        .await?
        .with_invocation_cancellation(&cancellation);
    let mut changes = owner.owner_changes().await?;
    owner.verify(false, &mut None).await?;
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(1), changes.changed())
        .await
        .map_err(|cause| Error::Storage(cause.to_string()))??;
    assert!(owner.verify(false, &mut None).await.is_err());
    Ok(())
}

#[test]
fn ancestor_and_orchestration_routes_are_targeted_and_fail_closed() {
    let (parent, child, sibling) = (
        OperationId::from_bytes([1; 16]),
        OperationId::from_bytes([2; 16]),
        OperationId::from_bytes([3; 16]),
    );
    let ready = Arc::new(Notify::new());
    let other = Arc::new(Notify::new());
    let mut subscribers = Subscribers::default();
    subscribers
        .by_operation
        .insert(parent, BTreeMap::from([(0, ready.clone())]));
    subscribers
        .by_operation
        .insert(child, BTreeMap::from([(0, ready.clone())]));
    subscribers
        .by_operation
        .insert(sibling, BTreeMap::from([(1, other.clone())]));
    subscribers.publish(&SchedulerEvent::CancellationRequested {
        operation_id: parent,
        recursive: true,
    });
    assert!(futures::FutureExt::now_or_never(ready.notified()).is_some());
    assert!(futures::FutureExt::now_or_never(other.notified()).is_none());
    subscribers.publish(&SchedulerEvent::Orchestrated {
        operation_id: OperationId::from_bytes([4; 16]),
        expected_revision: 1,
        outcome: Outcome::Cancelled,
        cancel: vec![child],
        reducer: None,
        reduction_digest: None,
    });
    assert!(futures::FutureExt::now_or_never(ready.notified()).is_some());
    assert!(futures::FutureExt::now_or_never(other.notified()).is_none());
    subscribers.fail("coordinator follow broke".into());
    assert!(subscribers.check().is_err());
    assert!(futures::FutureExt::now_or_never(ready.notified()).is_some());
    assert!(futures::FutureExt::now_or_never(other.notified()).is_some());
}
