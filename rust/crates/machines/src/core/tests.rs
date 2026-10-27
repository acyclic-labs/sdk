//! Internal state-machine vectors on real LocalStream journals, not provider captures.
use super::*;
use acyclic_stream::{
    LocalStream, LocalStreamLimits, MemoryStream, ReadRequest, StreamPath, StreamProvider,
};
use futures::{StreamExt, TryStreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn open(path: &std::path::Path) -> Result<OperationRegistry, Box<dyn std::error::Error>> {
    Ok(OperationRegistry::new(
        Arc::new(LocalStream::open(path, LocalStreamLimits::default()).await?),
        "account",
    )?)
}

fn fresh(admission: Admission) -> Result<OperationId, ProviderError> {
    match admission {
        Admission::Fresh(id) => Ok(id),
        Admission::Replay(_) => Err(ProviderError::Failed),
    }
}

#[tokio::test]
async fn restart_preserves_success_and_exact_settlement_identity() -> TestResult {
    let dir = tempfile::tempdir()?;
    let key = IdempotencyKey::new();
    let machine = MachineId::new();
    let core = open(dir.path()).await?;
    let id = fresh(core.admit(key, &"suspend").await?)?;
    core.bind_created(id, "new-resource").await?;
    core.complete(id, MutationOutcome::Suspended(machine))
        .await?;
    let settled = core.settled(key).await?;
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(settled, core.settled(key).await?);
    assert!(
        matches!(core.admit(key, &"suspend").await?, Admission::Replay(value) if *value == settled.outcome)
    );
    assert!(matches!(
        core.admit(key, &"wake").await,
        Err(ProviderError::Conflict(_))
    ));
    assert!(
        core.cancel(id)
            .await?
            .is_some_and(|(_, resources)| resources.is_empty())
    );
    assert_eq!(settled, core.settled(key).await?);
    Ok(())
}

#[tokio::test]
async fn pending_restart_never_authorizes_redispatch() -> TestResult {
    let dir = tempfile::tempdir()?;
    let key = IdempotencyKey::new();
    let core = open(dir.path()).await?;
    fresh(core.admit(key, &"create").await?)?;
    drop(core);
    let core = open(dir.path()).await?;
    let calls = AtomicUsize::new(0);
    let result = core
        .apply(
            key,
            &"create",
            async |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(MutationOutcome::Woken(MachineId::new()))
            },
            async |_| Ok(()),
        )
        .await;
    assert!(matches!(result, Err(ProviderError::Indeterminate(value)) if value == key));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn independent_cores_share_admission_and_follow_without_polling() -> TestResult {
    let streams = Arc::new(MemoryStream::new(Default::default()));
    let left = OperationRegistry::new(streams.clone(), "account")?;
    let right = OperationRegistry::new(streams, "account")?;
    let key = IdempotencyKey::new();
    let (a, b) = tokio::join!(left.admit(key, &"create"), right.admit(key, &"create"));
    let id = match (a, b) {
        (Ok(Admission::Fresh(id)), Err(ProviderError::Indeterminate(_)))
        | (Err(ProviderError::Indeterminate(_)), Ok(Admission::Fresh(id))) => id,
        value => return Err(format!("admission exclusion failed: {value:?}").into()),
    };
    let mut watch = left.watch(id).await?;
    assert_eq!(
        watch.next().await.transpose()?.map(|value| value.phase),
        Some(OperationPhase::Pending)
    );
    right
        .complete(id, MutationOutcome::Woken(MachineId::new()))
        .await?;
    assert_eq!(
        watch.next().await.transpose()?.map(|value| value.phase),
        Some(OperationPhase::Succeeded)
    );
    assert!(watch.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn cancellation_retains_cleanup_until_each_delete_is_acknowledged() -> TestResult {
    let dir = tempfile::tempdir()?;
    let key = IdempotencyKey::new();
    let core = open(dir.path()).await?;
    let id = fresh(core.admit(key, &"fork").await?)?;
    assert!(core.bind_created(id, "child-0").await?);
    core.bind_target(id, "existing-parent").await?;
    core.fail(id, &ProviderError::Unavailable).await?;
    core.cancel(id).await?;
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(core.created(id).await?, ["child-0"]);
    assert!(!core.bind_created(id, "late-child").await?);
    core.deleted(id, "child-0").await?;
    assert_eq!(core.created(id).await?, ["late-child"]);
    core.deleted(id, "late-child").await?;
    assert!(core.created(id).await?.is_empty());
    core.complete(id, MutationOutcome::Woken(MachineId::new()))
        .await?;
    assert!(matches!(
        core.settled(key).await,
        Err(ProviderError::Cancelled)
    ));
    Ok(())
}

#[tokio::test]
async fn failed_operation_retains_cleanup_and_never_replays() -> TestResult {
    let core = OperationRegistry::new(Arc::new(MemoryStream::new(Default::default())), "account")?;
    let key = IdempotencyKey::new();
    let id = fresh(core.admit(key, &"fork").await?)?;
    core.bind_created(id, "child").await?;
    core.fail(id, &ProviderError::Failed).await?;
    assert!(
        core.cancel(id).await?.is_some_and(
            |(obs, resources)| obs.phase == OperationPhase::Failed && resources == ["child"]
        )
    );
    assert!(matches!(
        core.admit(key, &"fork").await,
        Err(ProviderError::Failed)
    ));
    core.deleted(id, "child").await?;
    assert!(core.created(id).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn before_dispatch_refusal_can_release_key_but_unknown_outcome_cannot() -> TestResult {
    let core = OperationRegistry::new(Arc::new(MemoryStream::new(Default::default())), "account")?;
    let key = IdempotencyKey::new();
    assert!(matches!(
        core.apply(
            key,
            &"create",
            async |_| Err(ProviderError::Unsupported("unmapped image".into())),
            async |_| Ok(())
        )
        .await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(core.record_for_key(key).await?.is_none());
    assert!(matches!(
        core.apply(
            key,
            &"create",
            async |_| Err(ProviderError::Unavailable),
            async |_| Ok(())
        )
        .await,
        Err(ProviderError::Unavailable)
    ));
    assert!(matches!(
        core.admit(key, &"create").await,
        Err(ProviderError::Indeterminate(_))
    ));
    Ok(())
}

#[tokio::test]
async fn observation_event_is_atomic_and_pages_survive_restart() -> TestResult {
    let dir = tempfile::tempdir()?;
    let key = IdempotencyKey::new();
    let request = CreateMachine::new(key, Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(mut observation) =
        SimulatedMachines::default().create(request).await?
    else {
        return Err("expected machine".into());
    };
    let streams = Arc::new(LocalStream::open(dir.path(), LocalStreamLimits::default()).await?);
    let core = OperationRegistry::new(streams.clone(), "account")?;
    core.observe_machine(observation.clone(), 1).await?;
    core.observe_machine(observation.clone(), 2).await?;
    let path = StreamPath::new(format!("machines/account/{}", observation.id))?;
    let event_path = StreamPath::new(format!("{}/events", path.as_str()))?;
    let state = streams
        .read(ReadRequest {
            path,
            from: 0,
            limit: 1,
        })
        .await?
        .try_next()
        .await?
        .ok_or("missing state")?;
    let event = streams
        .read(ReadRequest {
            path: event_path,
            from: 0,
            limit: 1,
        })
        .await?
        .try_next()
        .await?
        .ok_or("missing event")?;
    assert_eq!(state.commit_id, event.commit_id);
    observation.state = MachineState::Suspended;
    core.observe_machine(observation.clone(), 3).await?;
    observation.state = MachineState::Running;
    core.observe_machine(observation.clone(), 4).await?;
    let (events, next) = core.events(observation.id, None, 2).await?;
    assert_eq!(events.len(), 2);
    assert_eq!(next, Some(2));
    core.mark_destroyed(observation.id, 5).await?;
    core.observe_machine(observation.clone(), 6).await?;
    drop(core);
    drop(streams);
    let core = open(dir.path()).await?;
    let (rest, next) = core.events(observation.id, next, 2).await?;
    assert_eq!(rest.len(), 2);
    assert_eq!(
        rest.last().map(|event| &event.fact),
        Some(&EventFact::State(MachineState::Destroyed))
    );
    assert_eq!(next, None);
    assert_eq!(
        core.machine(observation.id).await?.map(|value| value.state),
        Some(MachineState::Destroyed)
    );
    assert_eq!(core.machines().await?.len(), 1);
    Ok(())
}

#[test]
fn operation_identity_matches_existing_recovery_keys() -> TestResult {
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000001")?;
    assert_eq!(
        operation_id(key).to_string(),
        "5826b47d-5e02-5c80-a94c-145737646d7f"
    );
    Ok(())
}

#[tokio::test]
async fn large_settled_fork_replays_every_original_observation() -> TestResult {
    let dir = tempfile::tempdir()?;
    let core = open(dir.path()).await?;
    let key = IdempotencyKey::new();
    let id = fresh(core.admit(key, &"large fork").await?)?;
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(observation) =
        SimulatedMachines::default().create(request).await?
    else {
        return Err("expected machine".into());
    };
    let outcome = MutationOutcome::Forked(
        (0..MAX_FORK_CHILDREN)
            .map(|_| {
                let mut observation = observation.clone();
                observation.id = MachineId::new();
                observation
            })
            .collect(),
    );
    assert!(serde_json::to_vec(&outcome)?.len() > acyclic_stream::MAX_RECORD_BYTES);
    core.complete(id, outcome.clone()).await?;
    let settled = core.settled(key).await?;
    assert_eq!(settled.outcome, outcome);
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(core.settled(key).await?, settled);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn native_observation_deadline_bounds_a_stalled_read() -> TestResult {
    let key = IdempotencyKey::new();
    let result = settle::<(), _, _>(
        key,
        Duration::from_millis(1),
        Duration::from_millis(5),
        || async { futures::future::pending().await },
    )
    .await;
    assert!(matches!(result, Err(ProviderError::Indeterminate(value)) if value == key));
    Ok(())
}

#[tokio::test]
async fn failed_admission_performs_no_native_dispatch() -> TestResult {
    let streams = MemoryStream::new(acyclic_stream::MemoryLimits {
        records: 0,
        ..Default::default()
    });
    let core = OperationRegistry::new(Arc::new(streams), "account")?;
    let calls = AtomicUsize::new(0);
    let result = core
        .apply(
            IdempotencyKey::new(),
            &"create",
            async |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(MutationOutcome::Woken(MachineId::new()))
            },
            async |_| Ok(()),
        )
        .await;
    assert!(matches!(result, Err(ProviderError::Unavailable)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}
