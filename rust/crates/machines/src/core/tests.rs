//! Internal state-machine vectors on real LocalStream journals, not provider captures.
use super::*;
use acyclic_stream::{
    LocalStream, LocalStreamLimits, MemoryStream, ReadRequest, StreamPath, StreamProvider,
};
use futures::{StreamExt, TryStreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};

fn machine_resource(value: &str) -> NativeResource {
    NativeResource::Machine(value.into())
}

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
    core.bind_created(id, &machine_resource("new-resource"))
        .await?;
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
    assert!(core.bind_created(id, &machine_resource("child-0")).await?);
    core.bind_target(id, "existing-parent").await?;
    core.fail(id, &ProviderError::Unavailable).await?;
    core.cancel(id).await?;
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(core.created(id).await?, [machine_resource("child-0")]);
    assert!(
        !core
            .bind_created(id, &machine_resource("late-child"))
            .await?
    );
    core.deleted(id, &machine_resource("child-0")).await?;
    assert_eq!(core.created(id).await?, [machine_resource("late-child")]);
    core.deleted(id, &machine_resource("late-child")).await?;
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
    core.bind_created(id, &machine_resource("child")).await?;
    core.fail(id, &ProviderError::Failed).await?;
    assert!(
        core.cancel(id)
            .await?
            .is_some_and(|(obs, resources)| obs.phase == OperationPhase::Failed
                && resources == [machine_resource("child")])
    );
    assert!(matches!(
        core.admit(key, &"fork").await,
        Err(ProviderError::Failed)
    ));
    core.deleted(id, &machine_resource("child")).await?;
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

struct NativeLifecycle {
    provider: &'static str,
    checkpoint_owner: Option<(OperationRegistry, IdempotencyKey)>,
    value: tokio::sync::Mutex<MachineObservation>,
    mutations: AtomicUsize,
    reads: AtomicUsize,
}

#[async_trait]
impl RawMachines for NativeLifecycle {
    fn name(&self) -> &str {
        self.provider
    }

    async fn raw_observe(
        &self,
        _: MachineId,
        _: Option<&NativeBinding>,
        _: Option<&MachineObservation>,
    ) -> Result<RawObservation, ProviderError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(RawObservation {
            machine: self.value.lock().await.clone(),
            settled: true,
        })
    }
    async fn raw_suspend(&self, _: MachineId) -> Result<(), ProviderError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.value.lock().await.state = MachineState::Suspended;
        Ok(())
    }
    async fn raw_wake(&self, _: MachineId) -> Result<(), ProviderError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.value.lock().await.state = MachineState::Running;
        Ok(())
    }
    async fn raw_set_suspension_policy(
        &self,
        _: MachineId,
        policy: SuspensionPolicy,
    ) -> Result<(), ProviderError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.value.lock().await.contract.suspension = policy;
        Ok(())
    }
    async fn raw_checkpoint(
        &self,
        _: &MachineObservation,
        _: Option<&NativeBinding>,
        key: IdempotencyKey,
    ) -> Result<String, ProviderError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(CheckpointId::from_digest(Sha256::digest(key.as_bytes()).into()).to_string())
    }
    async fn raw_observe_checkpoint(
        &self,
        handle: &str,
        source: &MachineObservation,
    ) -> Result<Option<CheckpointObservation>, ProviderError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if let Some((core, key)) = &self.checkpoint_owner {
            if core.created(operation_id(*key)).await?
                != [NativeResource::Checkpoint(handle.into())]
            {
                return Err(ProviderError::Rejected(
                    "internal readiness oracle observed a snapshot before durable ownership".into(),
                ));
            }
        }
        Ok(Some(CheckpointObservation {
            id: CheckpointId::parse(handle)
                .map_err(|error| ProviderError::Rejected(error.to_string()))?,
            source: source.id,
            contract: source.contract.clone(),
            forkable: true,
            created_at_unix_ms: source.changed_at_unix_ms,
        }))
    }
    async fn raw_destroy(&self, _: &NativeResource) -> Result<(), ProviderError> {
        Err(ProviderError::Rejected(
            "internal lifecycle oracle must never delete an existing target".into(),
        ))
    }
}

#[tokio::test]
async fn shared_transition_reopens_exact_result_without_native_dispatch_or_read() -> TestResult {
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(value) = SimulatedMachines::default().create(request).await?
    else {
        return Err("expected machine".into());
    };
    let machine = value.id;
    let native = NativeLifecycle {
        provider: "internal",
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    let dir = tempfile::tempdir()?;
    let key = IdempotencyKey::new();
    let core = open(dir.path()).await?;
    assert_eq!(
        core.transition(
            &native,
            machine,
            key,
            false,
            Duration::from_millis(1),
            Duration::from_secs(1),
            3
        )
        .await?,
        MutationOutcome::Suspended(machine)
    );
    let settled = core.settled(key).await?;
    let reads = native.reads.load(Ordering::SeqCst);
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(
        core.transition(
            &native,
            machine,
            key,
            false,
            Duration::from_millis(1),
            Duration::from_secs(1),
            4
        )
        .await?,
        settled.outcome
    );
    assert_eq!(core.settled(key).await?, settled);
    assert_eq!(native.mutations.load(Ordering::SeqCst), 1);
    assert_eq!(native.reads.load(Ordering::SeqCst), reads);
    assert_eq!(
        core.transition(
            &native,
            machine,
            IdempotencyKey::new(),
            true,
            Duration::from_millis(1),
            Duration::from_secs(1),
            5
        )
        .await?,
        MutationOutcome::Woken(machine)
    );
    core.mark_destroyed(machine, 6).await?;
    assert_eq!(
        core.inspect_machine(&native, machine, 7).await?.state,
        MachineState::Destroyed
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn definite_refusal_retries_but_unknown_dispatch_does_not() -> TestResult {
    let key = IdempotencyKey::new();
    let calls = AtomicUsize::new(0);
    let value = retry_refused(
        key,
        Duration::from_millis(1),
        Duration::from_secs(1),
        || async {
            if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                Ok(Dispatch::Refused)
            } else {
                Ok(Dispatch::Accepted(7))
            }
        },
    )
    .await?;
    assert_eq!(value, 7);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    calls.store(0, Ordering::SeqCst);
    let unknown = retry_refused::<(), _, _>(
        key,
        Duration::from_millis(1),
        Duration::from_secs(1),
        || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(ProviderError::Unavailable)
        },
    )
    .await;
    assert!(matches!(unknown, Err(ProviderError::Unavailable)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn failed_fork_round_still_joins_every_started_sibling() -> TestResult {
    let completed = AtomicUsize::new(0);
    let finished = &completed;
    let result = fork_tree(0, 3, |index, _source| async move {
        if index == 1 {
            return Err(ProviderError::Failed);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        finished.fetch_add(1, Ordering::SeqCst);
        Ok(index + 1)
    })
    .await;
    assert!(matches!(result, Err(ProviderError::Failed)));
    assert_eq!(completed.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn native_binding_observation_event_and_ownership_reopen_from_one_commit() -> TestResult {
    // This is an internal raw-adapter oracle, not an external provider capture.
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(value) = SimulatedMachines::default().create(request).await?
    else {
        return Err("expected machine".into());
    };
    let native = NativeLifecycle {
        provider: "internal",
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value.clone()),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    let dir = tempfile::tempdir()?;
    let streams = Arc::new(LocalStream::open(dir.path(), LocalStreamLimits::default()).await?);
    let core = OperationRegistry::new(streams.clone(), "account")?;
    let key = IdempotencyKey::new();
    let operation = fresh(core.admit(key, &"create").await?)?;
    let binding = NativeBinding {
        provider: "internal".into(),
        handle: "opaque-native-id".into(),
        endpoint: Some("https://internal.invalid".into()),
    };
    core.claim_machine(&native, operation, value.clone(), binding.clone(), false)
        .await?;
    let mut commits = Vec::new();
    for (suffix, from) in [
        (value.id.to_string(), 0),
        (format!("{}/events", value.id), 0),
        (format!("ops/{operation}"), 1),
    ] {
        let record = streams
            .read(ReadRequest {
                path: StreamPath::new(format!("machines/account/{suffix}"))?,
                from,
                limit: 1,
            })
            .await?
            .try_next()
            .await?
            .ok_or("missing atomic publication")?;
        commits.push(record.commit_id);
    }
    assert!(commits.windows(2).all(|pair| pair.first() == pair.get(1)));
    drop(core);
    drop(streams);
    let core = open(dir.path()).await?;
    assert_eq!(
        core.native_binding(value.id, "internal").await?,
        Some(binding)
    );
    assert_eq!(
        core.created(operation).await?,
        [machine_resource("opaque-native-id")]
    );
    let other = NativeLifecycle {
        provider: "other",
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value.clone()),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    assert!(matches!(
        core.inspect_machine(&other, value.id, 3).await,
        Err(ProviderError::Unsupported(_))
    ));
    assert_eq!(other.reads.load(Ordering::SeqCst), 0);
    assert!(matches!(
        core.cancel_with(&other, operation).await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(matches!(
        core.apply_with(&other, key, &"create", async |_| {
            other.mutations.fetch_add(1, Ordering::SeqCst);
            Ok(MutationOutcome::Suspended(value.id))
        })
        .await,
        Err(ProviderError::Unsupported(_))
    ));
    assert_eq!(other.mutations.load(Ordering::SeqCst), 0);
    assert_eq!(
        core.inspect(operation).await?.map(|value| value.phase),
        Some(OperationPhase::Pending)
    );
    core.cancel(operation).await?;
    let mut late = value;
    late.id = MachineId::new();
    let binding = NativeBinding {
        provider: "internal".into(),
        handle: "late-native-id".into(),
        endpoint: None,
    };
    assert!(matches!(
        core.claim_machine(&native, operation, late.clone(), binding.clone(), false)
            .await,
        Err(ProviderError::Cancelled)
    ));
    assert_eq!(
        core.native_binding(late.id, "internal").await?,
        Some(binding)
    );
    assert_eq!(
        core.created(operation).await?,
        [
            machine_resource("opaque-native-id"),
            machine_resource("late-native-id")
        ]
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_wait_requires_durable_typed_ownership_and_replays_after_reopen() -> TestResult {
    let dir = tempfile::tempdir()?;
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(value) = SimulatedMachines::default().create(request).await?
    else {
        return Err("expected machine".into());
    };
    let core = open(dir.path()).await?;
    let key = IdempotencyKey::new();
    let mut native = NativeLifecycle {
        provider: "internal",
        checkpoint_owner: Some((core.clone(), key)),
        value: tokio::sync::Mutex::new(value.clone()),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    let result = core
        .create_checkpoint(
            &native,
            value.id,
            key,
            Duration::from_millis(1),
            Duration::from_secs(5),
            10,
        )
        .await?;
    let MutationOutcome::Checkpointed(checkpoint) = &result else {
        return Err("expected checkpoint".into());
    };
    assert_eq!(
        core.machine(value.id)
            .await?
            .and_then(|value| value.last_checkpoint),
        Some(checkpoint.id)
    );
    assert_eq!(
        core.checkpoint(checkpoint.id).await?,
        Some(checkpoint.clone())
    );
    assert!(core.created(operation_id(key)).await?.is_empty());
    let settlement = core.settled(key).await?;
    let reads = native.reads.load(Ordering::SeqCst);
    native.checkpoint_owner = None;
    drop(core);
    let reopened = open(dir.path()).await?;
    assert_eq!(
        reopened
            .create_checkpoint(
                &native,
                value.id,
                key,
                Duration::from_millis(1),
                Duration::from_secs(5),
                20
            )
            .await?,
        result
    );
    assert_eq!(reopened.settled(key).await?, settlement);
    assert_eq!(native.mutations.load(Ordering::SeqCst), 1);
    assert_eq!(native.reads.load(Ordering::SeqCst), reads);
    // Unacknowledged checkpoint deletion remains typed and owned after reopening.
    let key = IdempotencyKey::new();
    let operation = fresh(
        reopened
            .admit(key, &MutationIntent::Checkpoint(value.id))
            .await?,
    )?;
    let resource = NativeResource::Checkpoint("pending-snapshot".into());
    reopened.claim(&native, operation, &resource, false).await?;
    reopened.cancel_with(&native, operation).await?;
    drop(reopened);
    let reopened = open(dir.path()).await?;
    assert_eq!(reopened.created(operation).await?, [resource]);
    assert_eq!(
        reopened.machine(value.id).await?.map(|value| value.state),
        Some(MachineState::Running)
    );
    Ok(())
}
