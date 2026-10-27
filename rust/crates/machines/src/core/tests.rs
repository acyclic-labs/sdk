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
        matches!(core.admit(key, &"suspend").await?, Admission::Replay(value) if *value == settled)
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
    let cancelled = core.join_settlement(key, id).await?;
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
    let terminal = core.join_settlement(key, id).await?;
    assert_eq!(terminal, cancelled);
    assert_eq!(
        terminal.operation,
        OperationObservation {
            id,
            phase: OperationPhase::Cancelled
        }
    );
    assert!(terminal.outcome.is_none());
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
    let terminal = core.join_settlement(key, id).await?;
    assert_eq!(
        terminal.operation,
        OperationObservation {
            id,
            phase: OperationPhase::Failed
        }
    );
    assert!(terminal.outcome.is_none());
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
    assert_eq!(terminal, core.join_settlement(key, id).await?);
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
    allow_delete: bool,
    expected_handle: Option<&'static str>,
    checkpoint_owner: Option<(OperationRegistry, IdempotencyKey)>,
    value: tokio::sync::Mutex<MachineObservation>,
    children: tokio::sync::Mutex<BTreeMap<MachineId, MachineObservation>>,
    mutations: AtomicUsize,
    reads: AtomicUsize,
}

impl NativeLifecycle {
    fn check_binding(&self, native: Option<&NativeBinding>) -> Result<(), ProviderError> {
        if self
            .expected_handle
            .is_some_and(|expected| native.is_none_or(|value| value.handle != expected))
        {
            return Err(ProviderError::Rejected("internal mutation oracle received a logical machine UUID instead of its native address".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl NativeResources for NativeLifecycle {
    fn name(&self) -> &str {
        self.provider
    }

    async fn raw_destroy(&self, resource: &NativeResource) -> Result<u64, ProviderError> {
        if !self.allow_delete {
            return Err(ProviderError::Rejected(
                "internal lifecycle oracle must never delete an existing target".into(),
            ));
        }
        match resource {
            NativeResource::Machine(handle) => {
                self.check_binding(Some(&NativeBinding {
                    provider: self.provider.into(),
                    handle: handle.clone(),
                    endpoint: None,
                }))?;
                self.value.lock().await.state = MachineState::Destroyed;
            }
            NativeResource::Checkpoint(handle) if handle.starts_with("opaque-checkpoint-") => {}
            NativeResource::Checkpoint(_) => {
                return Err(ProviderError::Rejected(
                    "internal deletion oracle requires the opaque checkpoint address".into(),
                ));
            }
        }
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(self.value.lock().await.changed_at_unix_ms + 1)
    }
}

#[async_trait]
impl RawMachines for NativeLifecycle {
    async fn raw_create(&self, _: &CreateMachine) -> Result<RawCreated, ProviderError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(RawCreated {
            handle: "opaque-created-vm".into(),
            endpoint: None,
            observation: Ok(self.value.lock().await.clone()),
            adopted: false,
        })
    }

    async fn raw_observe(
        &self,
        machine: MachineId,
        _: Option<&NativeBinding>,
        _: Option<&MachineObservation>,
    ) -> Result<RawObservation, ProviderError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let child = self.children.lock().await.get(&machine).cloned();
        Ok(RawObservation {
            machine: match child {
                Some(value) => value,
                None => self.value.lock().await.clone(),
            },
            settled: true,
        })
    }
    async fn raw_suspend(
        &self,
        _: MachineId,
        native: Option<&NativeBinding>,
    ) -> Result<(), ProviderError> {
        self.check_binding(native)?;
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.value.lock().await.state = MachineState::Suspended;
        Ok(())
    }
    async fn raw_wake(
        &self,
        _: MachineId,
        native: Option<&NativeBinding>,
    ) -> Result<(), ProviderError> {
        self.check_binding(native)?;
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.value.lock().await.state = MachineState::Running;
        Ok(())
    }
    async fn raw_set_suspension_policy(
        &self,
        _: MachineId,
        native: Option<&NativeBinding>,
        policy: SuspensionPolicy,
    ) -> Result<(), ProviderError> {
        self.check_binding(native)?;
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
        Ok(format!("opaque-checkpoint-{key}"))
    }
    async fn raw_observe_checkpoint(
        &self,
        handle: &str,
        source: MachineId,
        contract: &MachineContract,
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
            id: CheckpointId::from_digest(Sha256::digest(handle.as_bytes()).into()),
            source,
            contract: contract.clone(),
            forkable: true,
            created_at_unix_ms: self.value.lock().await.changed_at_unix_ms,
        }))
    }
    async fn raw_fork(
        &self,
        checkpoint: CheckpointId,
        native: Option<&NativeBinding>,
        contract: &MachineContract,
        slot: ForkSlot,
        key: IdempotencyKey,
    ) -> Result<RawCreated, ProviderError> {
        if native.is_none_or(|native| !native.handle.starts_with("opaque-checkpoint-")) {
            return Err(ProviderError::Rejected(
                "internal fork oracle requires the canonical opaque checkpoint address".into(),
            ));
        }
        self.mutations.fetch_add(1, Ordering::SeqCst);
        let id = MachineId::from_digest(Sha256::digest(format!("{key}/{}", slot.index)).into());
        let mut child = self.value.lock().await.clone();
        child.id = id;
        child.contract = contract.clone();
        child.last_checkpoint = Some(checkpoint);
        self.children.lock().await.insert(id, child.clone());
        Ok(RawCreated {
            handle: format!("opaque-fork-{id}"),
            endpoint: None,
            observation: Ok(child),
            adopted: false,
        })
    }
    async fn raw_fork_machine(
        &self,
        parent: &MachineObservation,
        source: MachineId,
        native: Option<&NativeBinding>,
        slot: ForkSlot,
        key: IdempotencyKey,
    ) -> Result<Dispatch<RawCreated>, ProviderError> {
        let expected = if source == parent.id {
            "opaque-created-vm".into()
        } else {
            format!("opaque-live-fork-{source}")
        };
        if native.is_none_or(|native| native.handle != expected) {
            return Err(ProviderError::Rejected(
                "internal live-fork oracle requires the canonical source address".into(),
            ));
        }
        self.mutations.fetch_add(1, Ordering::SeqCst);
        let id = MachineId::from_digest(Sha256::digest(format!("{key}/{}", slot.index)).into());
        let mut child = parent.clone();
        child.id = id;
        child.last_checkpoint = None;
        self.children.lock().await.insert(id, child.clone());
        Ok(Dispatch::Accepted(RawCreated {
            handle: format!("opaque-live-fork-{id}"),
            endpoint: None,
            observation: Ok(child),
            adopted: false,
        }))
    }
    async fn raw_finish_fork(
        &self,
        child: MachineId,
        native: Option<&NativeBinding>,
        _: &MachineObservation,
        _: &str,
        _: ForkSlot,
        _: IdempotencyKey,
    ) -> Result<(), ProviderError> {
        if native.is_none_or(|native| native.handle != format!("opaque-live-fork-{child}")) {
            return Err(ProviderError::Rejected(
                "internal metadata oracle requires canonical child custody".into(),
            ));
        }
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn raw_fork_children(
        &self,
        _: MachineId,
        native: Option<&NativeBinding>,
    ) -> Result<Vec<MachineState>, ProviderError> {
        self.check_binding(native)?;
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .children
            .lock()
            .await
            .values()
            .map(|child| child.state)
            .collect())
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
        allow_delete: false,
        expected_handle: Some("opaque-running-vm"),
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value),
        children: tokio::sync::Mutex::default(),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    let dir = tempfile::tempdir()?;
    let key = IdempotencyKey::new();
    let core = open(dir.path()).await?;
    let created = native.value.lock().await.clone();
    let creation = fresh(
        core.admit(IdempotencyKey::new(), &"internal creation")
            .await?,
    )?;
    core.claim_machine(
        &native,
        creation,
        created.clone(),
        NativeBinding {
            provider: "internal".into(),
            handle: "opaque-running-vm".into(),
            endpoint: None,
        },
        false,
    )
    .await?;
    core.complete(creation, MutationOutcome::Created(created))
        .await?;
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
        .await?
        .outcome,
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
        settled
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
        .await?
        .outcome,
        MutationOutcome::Woken(machine)
    );
    let policy_key = IdempotencyKey::new();
    assert_eq!(
        core.set_suspension_policy(&native, machine, SuspensionPolicy::Manual, policy_key, 6)
            .await?
            .outcome,
        MutationOutcome::SuspensionPolicySet(machine, SuspensionPolicy::Manual),
    );
    assert_eq!(
        core.record_for_key(policy_key)
            .await?
            .map(|record| record.targets),
        Some(vec!["opaque-running-vm".into()])
    );
    core.mark_destroyed(machine, 7).await?;
    assert_eq!(
        core.inspect_machine(&native, machine, 8).await?.state,
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
        allow_delete: false,
        expected_handle: None,
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value.clone()),
        children: tokio::sync::Mutex::default(),
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
        allow_delete: false,
        expected_handle: None,
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value.clone()),
        children: tokio::sync::Mutex::default(),
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
        allow_delete: false,
        expected_handle: None,
        checkpoint_owner: Some((core.clone(), key)),
        value: tokio::sync::Mutex::new(value.clone()),
        children: tokio::sync::Mutex::default(),
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
    let MutationOutcome::Checkpointed(checkpoint) = &result.outcome else {
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
    assert_eq!(
        reopened
            .native_checkpoint_binding(checkpoint.id, "internal")
            .await?
            .map(|native| native.handle),
        Some(format!("opaque-checkpoint-{key}"))
    );
    assert!(matches!(
        reopened
            .native_checkpoint_binding(checkpoint.id, "another-provider")
            .await,
        Err(ProviderError::Unsupported(_))
    ));
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

#[tokio::test]
async fn creation_reopens_exact_native_binding_and_settlement_without_dispatch() -> TestResult {
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(value) =
        SimulatedMachines::default().create(request.clone()).await?
    else {
        return Err("expected machine".into());
    };
    let native = NativeLifecycle {
        provider: "internal",
        allow_delete: false,
        expected_handle: None,
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value.clone()),
        children: tokio::sync::Mutex::default(),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    let dir = tempfile::tempdir()?;
    let core = open(dir.path()).await?;
    let interval = Duration::from_millis(1);
    let timeout = Duration::from_secs(5);
    let original = core
        .create_machine(&native, request.clone(), interval, timeout)
        .await?;
    let settlement = core.settled(request.idempotency_key).await?;
    assert_eq!(original, settlement);
    let reads = native.reads.load(Ordering::SeqCst);
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(
        core.native_binding(value.id, "internal")
            .await?
            .map(|native| native.handle),
        Some("opaque-created-vm".into())
    );
    assert_eq!(
        core.create_machine(&native, request.clone(), interval, timeout)
            .await?,
        original
    );
    assert_eq!(core.settled(request.idempotency_key).await?, settlement);
    assert_eq!(native.mutations.load(Ordering::SeqCst), 1);
    assert_eq!(native.reads.load(Ordering::SeqCst), reads);
    let malformed_key = IdempotencyKey::new();
    let malformed = core
        .apply_with(
            &native,
            malformed_key,
            &"internal malformed native acceptance",
            async |operation| {
                core.accept_created(
                    &native,
                    operation,
                    RawCreated {
                        handle: "accepted-with-invalid-metadata".into(),
                        endpoint: None,
                        observation: Err(ProviderError::Rejected(
                            "internal malformed metadata vector".into(),
                        )),
                        adopted: false,
                    },
                    malformed_key,
                )
                .await
                .map(MutationOutcome::Created)
            },
        )
        .await;
    assert_eq!(malformed, Err(ProviderError::Indeterminate(malformed_key)));
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(
        core.created(operation_id(malformed_key)).await?,
        [machine_resource("accepted-with-invalid-metadata")]
    );
    assert_eq!(
        core.inspect(operation_id(malformed_key))
            .await?
            .map(|operation| operation.phase),
        Some(OperationPhase::Indeterminate)
    );
    Ok(())
}

#[tokio::test]
async fn shared_checkpoint_fork_keeps_order_native_addresses_and_exact_replay() -> TestResult {
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(value) = SimulatedMachines::default().create(request).await?
    else {
        return Err("expected machine".into());
    };
    let native = NativeLifecycle {
        provider: "internal",
        allow_delete: false,
        expected_handle: None,
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value.clone()),
        children: tokio::sync::Mutex::default(),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    let dir = tempfile::tempdir()?;
    let core = open(dir.path()).await?;
    let interval = Duration::from_millis(1);
    let timeout = Duration::from_secs(5);
    let MutationOutcome::Checkpointed(checkpoint) = core
        .create_checkpoint(
            &native,
            value.id,
            IdempotencyKey::new(),
            interval,
            timeout,
            10,
        )
        .await?
        .outcome
    else {
        return Err("expected checkpoint".into());
    };
    let key = IdempotencyKey::new();
    let count = NonZeroU32::new(2).ok_or("invalid internal count")?;
    let original = core
        .fork_checkpoint(&native, checkpoint.id, count, key, interval, timeout)
        .await?;
    let MutationOutcome::Forked(children) = &original.outcome else {
        return Err("expected fork".into());
    };
    for (index, child) in children.iter().enumerate() {
        assert_eq!(
            child.id,
            MachineId::from_digest(Sha256::digest(format!("{key}/{index}")).into())
        );
        assert_eq!(
            core.native_binding(child.id, "internal")
                .await?
                .map(|native| native.handle),
            Some(format!("opaque-fork-{}", child.id))
        );
    }
    let settlement = core.settled(key).await?;
    let reads = native.reads.load(Ordering::SeqCst);
    let mutations = native.mutations.load(Ordering::SeqCst);
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(
        core.fork_checkpoint(&native, checkpoint.id, count, key, interval, timeout)
            .await?,
        original
    );
    assert_eq!(core.settled(key).await?, settlement);
    assert_eq!(native.reads.load(Ordering::SeqCst), reads);
    assert_eq!(native.mutations.load(Ordering::SeqCst), mutations);
    assert!(matches!(
        core.fork_checkpoint(
            &native,
            CheckpointId::new(),
            count,
            IdempotencyKey::new(),
            interval,
            timeout
        )
        .await,
        Err(ProviderError::NotFound(_))
    ));
    assert_eq!(native.mutations.load(Ordering::SeqCst), mutations);
    Ok(())
}

#[tokio::test]
async fn conflicting_logical_identity_keeps_new_native_cleanup_without_rebinding() -> TestResult {
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(value) =
        SimulatedMachines::default().create(request.clone()).await?
    else {
        return Err("expected machine".into());
    };
    let native = NativeLifecycle {
        provider: "internal",
        allow_delete: false,
        expected_handle: None,
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value.clone()),
        children: tokio::sync::Mutex::default(),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    let dir = tempfile::tempdir()?;
    let core = open(dir.path()).await?;
    core.create_machine(
        &native,
        request,
        Duration::from_millis(1),
        Duration::from_secs(5),
    )
    .await?;
    let key = IdempotencyKey::new();
    let result = core
        .apply_with(
            &native,
            key,
            &"internal duplicate logical identity",
            async |operation| {
                core.accept_created(
                    &native,
                    operation,
                    RawCreated {
                        handle: "new-handle-for-duplicate-logical-id".into(),
                        endpoint: None,
                        observation: Ok(value.clone()),
                        adopted: false,
                    },
                    key,
                )
                .await
                .map(MutationOutcome::Created)
            },
        )
        .await;
    assert!(matches!(result, Err(ProviderError::Conflict(_))));
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(
        core.native_binding(value.id, "internal")
            .await?
            .map(|native| native.handle),
        Some("opaque-created-vm".into())
    );
    assert_eq!(
        core.created(operation_id(key)).await?,
        [machine_resource("new-handle-for-duplicate-logical-id")]
    );
    Ok(())
}

#[tokio::test]
async fn shared_destruction_reopens_bindings_guards_children_and_replays_exactly() -> TestResult {
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(value) =
        SimulatedMachines::default().create(request.clone()).await?
    else {
        return Err("expected machine".into());
    };
    let native = NativeLifecycle {
        provider: "internal",
        allow_delete: true,
        expected_handle: Some("opaque-created-vm"),
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value.clone()),
        children: tokio::sync::Mutex::default(),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    let dir = tempfile::tempdir()?;
    let core = open(dir.path()).await?;
    let interval = Duration::from_millis(1);
    let timeout = Duration::from_secs(5);
    core.create_machine(&native, request, interval, timeout)
        .await?;
    let MutationOutcome::Checkpointed(checkpoint) = core
        .create_checkpoint(
            &native,
            value.id,
            IdempotencyKey::new(),
            interval,
            timeout,
            10,
        )
        .await?
        .outcome
    else {
        return Err("expected checkpoint".into());
    };
    let checkpoint_key = IdempotencyKey::new();
    core.destroy_checkpoint(&native, checkpoint.id, checkpoint_key)
        .await?;
    let checkpoint_settlement = core.settled(checkpoint_key).await?;
    // The target is the opaque creation address, not the logical checkpoint UUID.
    assert_eq!(
        core.record_for_key(checkpoint_key)
            .await?
            .map(|record| record.targets),
        Some(vec![
            core.native_checkpoint_binding(checkpoint.id, "internal")
                .await?
                .ok_or("missing checkpoint binding")?
                .handle
        ])
    );
    let mutations = native.mutations.load(Ordering::SeqCst);
    let mut live_child = value.clone();
    live_child.id = MachineId::new();
    native
        .children
        .lock()
        .await
        .insert(live_child.id, live_child);
    assert!(matches!(
        core.destroy_machine(&native, value.id, IdempotencyKey::new(), 11)
            .await,
        Err(ProviderError::Conflict(_))
    ));
    assert_eq!(native.mutations.load(Ordering::SeqCst), mutations);
    native.children.lock().await.clear();
    let machine_key = IdempotencyKey::new();
    core.destroy_machine(&native, value.id, machine_key, 12)
        .await?;
    assert_eq!(
        core.record_for_key(machine_key)
            .await?
            .map(|record| record.targets),
        Some(vec!["opaque-created-vm".into()])
    );
    let machine_settlement = core.settled(machine_key).await?;
    let mutations = native.mutations.load(Ordering::SeqCst);
    let reads = native.reads.load(Ordering::SeqCst);
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(
        core.destroy_checkpoint(&native, checkpoint.id, checkpoint_key)
            .await?,
        checkpoint_settlement
    );
    assert_eq!(core.settled(checkpoint_key).await?, checkpoint_settlement);
    assert_eq!(
        core.destroy_machine(&native, value.id, machine_key, 13)
            .await?,
        machine_settlement
    );
    assert_eq!(core.settled(machine_key).await?, machine_settlement);
    assert_eq!(native.mutations.load(Ordering::SeqCst), mutations);
    assert_eq!(native.reads.load(Ordering::SeqCst), reads);
    assert_eq!(
        core.machine(value.id).await?.map(|machine| machine.state),
        Some(MachineState::Destroyed)
    );
    assert!(
        !core
            .inspect_checkpoint(&native, checkpoint.id)
            .await?
            .forkable
    );
    Ok(())
}

#[tokio::test]
async fn shared_native_fork_uses_reopened_addresses_and_replays_ordered_settlement() -> TestResult {
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(value) =
        SimulatedMachines::default().create(request.clone()).await?
    else {
        return Err("expected machine".into());
    };
    let native = NativeLifecycle {
        provider: "internal",
        allow_delete: false,
        expected_handle: None,
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value.clone()),
        children: tokio::sync::Mutex::default(),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    let dir = tempfile::tempdir()?;
    let core = open(dir.path()).await?;
    let interval = Duration::from_millis(1);
    let timeout = Duration::from_secs(5);
    core.create_machine(&native, request, interval, timeout)
        .await?;
    drop(core);
    let core = open(dir.path()).await?;
    let key = IdempotencyKey::new();
    let count = NonZeroU32::new(4).ok_or("invalid internal count")?;
    let outcome = core
        .fork_machine(&native, value.id, count, key, interval, timeout, 10)
        .await?;
    let MutationOutcome::MachineForked {
        source,
        fidelity,
        children,
    } = &outcome.outcome
    else {
        return Err("expected native fork".into());
    };
    assert_eq!(*source, value.id);
    assert_eq!(*fidelity, ForkFidelity::MemoryAndDisk);
    assert_eq!(children.len(), 4);
    for (index, child) in children.iter().enumerate() {
        assert_eq!(
            child.id,
            MachineId::from_digest(Sha256::digest(format!("{key}/{index}")).into())
        );
        assert_eq!(
            core.native_binding(child.id, "internal")
                .await?
                .map(|native| native.handle),
            Some(format!("opaque-live-fork-{}", child.id))
        );
    }
    let settled = core.settled(key).await?;
    let reads = native.reads.load(Ordering::SeqCst);
    let mutations = native.mutations.load(Ordering::SeqCst);
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(
        core.fork_machine(&native, value.id, count, key, interval, timeout, 11)
            .await?,
        outcome
    );
    assert_eq!(core.settled(key).await?, settled);
    assert_eq!(native.reads.load(Ordering::SeqCst), reads);
    assert_eq!(native.mutations.load(Ordering::SeqCst), mutations);
    Ok(())
}

#[tokio::test]
async fn bounded_admission_is_atomic_releases_refusals_and_reopens_its_count() -> TestResult {
    let dir = tempfile::tempdir()?;
    let streams = Arc::new(LocalStream::open(dir.path(), LocalStreamLimits::default()).await?);
    let limits = RegistryLimits {
        operations: Some(1),
        ..Default::default()
    };
    let a = OperationRegistry::with_limits(streams.clone(), "bounded", limits)?;
    let b = OperationRegistry::with_limits(streams.clone(), "bounded", limits)?;
    let (one, two) = tokio::join!(
        a.admit(IdempotencyKey::new(), &"one"),
        b.admit(IdempotencyKey::new(), &"two")
    );
    let operation = match (one, two) {
        (Ok(Admission::Fresh(id)), Err(ProviderError::Rejected(_)))
        | (Err(ProviderError::Rejected(_)), Ok(Admission::Fresh(id))) => id,
        _ => return Err("bounded concurrent admission did not retain exactly one slot".into()),
    };
    // This is a definite refusal before native dispatch; its reservation releases
    // atomically with the absent operation projection, never a local decrement.
    a.forget(operation).await?;
    let key = IdempotencyKey::new();
    let operation = fresh(b.admit(key, &"three").await?)?;
    let outcome = MutationOutcome::MachineDestroyed(MachineId::new());
    b.complete(operation, outcome.clone()).await?;
    let settled = b.settled(key).await?;
    drop(a);
    drop(b);
    drop(streams);
    let core = OperationRegistry::with_limits(
        Arc::new(LocalStream::open(dir.path(), LocalStreamLimits::default()).await?),
        "bounded",
        limits,
    )?;
    assert_eq!(core.settled(key).await?, settled);
    assert!(
        matches!(core.admit(key, &"three").await?, Admission::Replay(value) if value.outcome == outcome)
    );
    assert!(matches!(
        core.admit(IdempotencyKey::new(), &"four").await,
        Err(ProviderError::Rejected(_))
    ));
    Ok(())
}

#[tokio::test]
async fn event_admission_does_not_rewrite_the_canonical_machine() -> TestResult {
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(value) = SimulatedMachines::default().create(request).await?
    else {
        return Err("expected machine".into());
    };
    let core = OperationRegistry::with_limits(
        Arc::new(MemoryStream::new(Default::default())),
        "bounded-events",
        RegistryLimits {
            machine_events: Some(1),
            ..Default::default()
        },
    )?;
    core.observe_machine(value.clone(), 2).await?;
    let mut changed = value.clone();
    changed.state = MachineState::Suspended;
    assert!(matches!(
        core.observe_machine(changed, 3).await,
        Err(ProviderError::Rejected(_))
    ));
    assert_eq!(core.machine(value.id).await?, Some(value.clone()));
    let (events, next) = core.events(value.id, None, 10).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events.first().map(|event| &event.fact),
        Some(&EventFact::State(MachineState::Running))
    );
    assert_eq!(next, None);
    Ok(())
}

#[tokio::test]
async fn reopened_cleanup_publishes_destruction_with_debt_release_and_stable_settlement()
-> TestResult {
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(value) = SimulatedMachines::default().create(request).await?
    else {
        return Err("expected internal machine".into());
    };
    let driver = NativeLifecycle {
        provider: "internal",
        allow_delete: true,
        expected_handle: Some("opaque-cleanup-machine"),
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value.clone()),
        children: tokio::sync::Mutex::default(),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    let dir = tempfile::tempdir()?;
    let core = open(dir.path()).await?;
    let key = IdempotencyKey::new();
    let operation = fresh(core.admit(key, &"internal accepted machine").await?)?;
    core.claim_machine(
        &driver,
        operation,
        value.clone(),
        NativeBinding {
            provider: driver.name().into(),
            handle: "opaque-cleanup-machine".into(),
            endpoint: None,
        },
        false,
    )
    .await?;
    core.cancel(operation).await?;
    let settlement = core.join_settlement(key, operation).await?;
    drop(core);
    let core = open(dir.path()).await?;
    core.cancel_with(&driver, operation).await?;
    assert!(core.created(operation).await?.is_empty());
    let destroyed = core
        .machine(value.id)
        .await?
        .ok_or("missing destroyed point")?;
    assert_eq!(destroyed.state, MachineState::Destroyed);
    assert_eq!(destroyed.changed_at_unix_ms, value.changed_at_unix_ms + 1);
    assert_eq!(core.join_settlement(key, operation).await?, settlement);
    let operation_row = core
        .journal
        .load::<StoredOperation>(&format!("ops/{operation}"))
        .await?;
    let machine_row = core
        .journal
        .load::<StoredMachine>(&value.id.to_string())
        .await?;
    assert_eq!(operation_row.anchor, machine_row.anchor);
    let mut event_records = core
        .journal
        .provider
        .read(ReadRequest {
            path: core.journal.path(&format!("{}/events", value.id))?,
            from: 1,
            limit: 1,
        })
        .await?;
    assert_eq!(
        event_records
            .try_next()
            .await?
            .map(|record| record.commit_id),
        machine_row.anchor
    );
    core.cancel_with(&driver, operation).await?;
    assert_eq!(driver.mutations.load(Ordering::SeqCst), 1);
    // A delayed native read cannot resurrect the acknowledged deletion.
    core.observe_machine(value.clone(), value.changed_at_unix_ms)
        .await?;
    assert_eq!(core.events(value.id, None, 10).await?.0.len(), 2);
    drop(event_records);
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(
        core.machine(value.id).await?.map(|value| value.state),
        Some(MachineState::Destroyed)
    );
    assert_eq!(core.join_settlement(key, operation).await?, settlement);
    Ok(())
}

#[tokio::test]
async fn checkpoint_cleanup_reopens_the_admitted_projection_and_revokes_forkability_atomically()
-> TestResult {
    let request = CreateMachine::new(IdempotencyKey::new(), Image::custom([7; 32])?, [8; 32]);
    let MutationOutcome::Created(value) = SimulatedMachines::default().create(request).await?
    else {
        return Err("expected internal machine".into());
    };
    let driver = NativeLifecycle {
        provider: "internal",
        allow_delete: true,
        expected_handle: None,
        checkpoint_owner: None,
        value: tokio::sync::Mutex::new(value.clone()),
        children: tokio::sync::Mutex::default(),
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    };
    let checkpoint = CheckpointObservation {
        id: CheckpointId::new(),
        source: value.id,
        contract: value.contract,
        forkable: true,
        created_at_unix_ms: 10,
    };
    let dir = tempfile::tempdir()?;
    let core = open(dir.path()).await?;
    let key = IdempotencyKey::new();
    let operation = fresh(core.admit(key, &"internal accepted checkpoint").await?)?;
    core.claim_checkpoint(
        &driver,
        operation,
        checkpoint.clone(),
        NativeBinding {
            provider: driver.name().into(),
            handle: "opaque-checkpoint-cleanup".into(),
            endpoint: None,
        },
    )
    .await?;
    core.cancel(operation).await?;
    let settlement = core.join_settlement(key, operation).await?;
    drop(core);
    let core = open(dir.path()).await?;
    core.cancel_with(&driver, operation).await?;
    assert!(core.created(operation).await?.is_empty());
    assert_eq!(
        core.checkpoint(checkpoint.id).await?,
        Some(CheckpointObservation {
            forkable: false,
            ..checkpoint.clone()
        })
    );
    let operation_row = core
        .journal
        .load::<StoredOperation>(&format!("ops/{operation}"))
        .await?;
    let checkpoint_row = core
        .journal
        .load::<StoredCheckpoint>(&format!("checkpoints/{}", checkpoint.id))
        .await?;
    assert_eq!(operation_row.anchor, checkpoint_row.anchor);
    assert_eq!(core.join_settlement(key, operation).await?, settlement);
    core.cancel_with(&driver, operation).await?;
    assert_eq!(driver.mutations.load(Ordering::SeqCst), 1);
    drop(core);
    let core = open(dir.path()).await?;
    assert_eq!(
        core.checkpoint(checkpoint.id)
            .await?
            .map(|value| value.forkable),
        Some(false)
    );
    assert_eq!(core.join_settlement(key, operation).await?, settlement);
    let retry = fresh(
        core.admit(IdempotencyKey::new(), &"internal reused revoked checkpoint")
            .await?,
    )?;
    assert!(matches!(
        core.claim_checkpoint(
            &driver,
            retry,
            checkpoint.clone(),
            NativeBinding {
                provider: driver.name().into(),
                handle: "opaque-checkpoint-cleanup".into(),
                endpoint: None,
            }
        )
        .await,
        Err(ProviderError::Conflict(_))
    ));
    assert_eq!(
        core.checkpoint(checkpoint.id)
            .await?
            .map(|value| value.forkable),
        Some(false)
    );
    assert_eq!(driver.mutations.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn owned_fork_sources_are_not_copied_into_existing_targets() -> TestResult {
    let core = OperationRegistry::new(Arc::new(MemoryStream::new(Default::default())), "account")?;
    let operation = fresh(
        core.admit(IdempotencyKey::new(), &"internal bounded fork custody")
            .await?,
    )?;
    core.bind_target(operation, "existing-parent").await?;
    for index in 0..MAX_FORK_CHILDREN {
        // Internal identity vectors with the native UUID address width, not
        // provider captures or a claim about arbitrary opaque address lengths.
        let resource = machine_resource(&format!("00000000-0000-4000-8000-{index:012x}"));
        assert!(core.bind_created(operation, &resource).await?);
        core.bind_target(operation, resource.handle()).await?;
    }
    let row = core
        .journal
        .load::<StoredOperation>(&format!("ops/{operation}"))
        .await?;
    assert_eq!(row.tail, u64::from(MAX_FORK_CHILDREN) + 2);
    let record = row.value.ok_or("missing internal custody point")?;
    assert_eq!(record.created.len(), MAX_FORK_CHILDREN as usize);
    assert_eq!(record.targets, ["existing-parent"]);
    assert!(serde_json::to_vec(&record)?.len() <= acyclic_stream::MAX_RECORD_BYTES);
    Ok(())
}
