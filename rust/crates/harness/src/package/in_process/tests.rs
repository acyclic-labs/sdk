use super::*;
use acyclic_stream::MemoryStream;
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

mod histories;
mod outbox;
mod ownership;

struct Counter {
    manifest: Manifest,
    schema: Value,
}
impl ActorPackage for Counter {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    fn machine(&self) -> Arc<dyn ResumableMachine> {
        Arc::new(Self {
            manifest: self.manifest.clone(),
            schema: self.schema.clone(),
        })
    }
}
impl ResumableMachine for Counter {
    fn identity(&self) -> &MachineIdentity {
        &self.manifest.agent
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        let state = json!(
            state
                .as_u64()
                .ok_or_else(|| Error::Invalid("counter state".into()))?
                + input.as_u64().unwrap_or(0)
        );
        Ok(MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: if input.is_null() {
                MachineStatus::Completed { value: state }
            } else {
                MachineStatus::Suspended
            },
        })
    }
}
struct Policy(AtomicBool);
impl AdmissionPolicy for Policy {
    fn authorize<'a>(
        &'a self,
        _: &'a Manifest,
        _: Option<&'a Placement>,
        _: Option<&'a Event>,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            if self.0.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(Error::Unauthorized("fixture workload denied".into()))
            }
        })
    }
}
#[derive(Default)]
struct Effects {
    fences: AtomicUsize,
    unavailable: AtomicBool,
}
impl EffectHost for Effects {
    fn execute(&self, _: Placement, _: WorkflowCommand) -> BoxProviderFuture<'_, Result<Event>> {
        Box::pin(async { Err(Error::Unsupported("counter has no effects".into())) })
    }
    fn fence(&self, _: Placement) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            self.fences.fetch_add(1, Ordering::SeqCst);
            if self.unavailable.load(Ordering::SeqCst) {
                Err(Error::Storage("fixture effect is still draining".into()))
            } else {
                Ok(())
            }
        })
    }
}
fn package() -> Arc<Counter> {
    Arc::new(Counter {
        manifest: Manifest {
            agent: MachineIdentity {
                name: "test.actor.counter".into(),
                version: "1".into(),
                digest: [1; 32],
            },
            effects: Vec::new(),
            bindings: Bindings {
                streams: "runtime".into(),
                volume: "project".into(),
                placement: VolumePlacement::Portable,
                model_https: "https://model.example".into(),
                model_secret: "fixture-model".into(),
                compute: "fixture-compute".into(),
            },
        },
        schema: json!({"type":"integer","minimum":0}),
    })
}
fn placement() -> Placement {
    Placement {
        actor: AgentId::from_bytes([1; 16]),
        node: "node-1".into(),
        epoch: 1,
    }
}
fn event(id: u8, input: Value) -> Event {
    Event {
        operation_id: OperationId::from_bytes([id; 16]),
        idempotency_key: IdempotencyKey::new(format!("event-{id}")).unwrap(),
        input,
    }
}
fn runtime(
    stream: Arc<MemoryStream>,
    package: &Counter,
    policy: Arc<Policy>,
    effects: Arc<Effects>,
) -> InProcessRuntime<MemoryStream> {
    InProcessRuntime::new(stream, package.manifest.bindings.clone(), policy, effects)
}
async fn fixture() -> (
    Arc<MemoryStream>,
    Arc<Counter>,
    Arc<Policy>,
    Arc<Effects>,
    InProcessRuntime<MemoryStream>,
) {
    let stream = Arc::new(MemoryStream::default());
    let package = package();
    let policy = Arc::new(Policy(AtomicBool::new(true)));
    let effects = Arc::new(Effects::default());
    let runtime = runtime(stream.clone(), &package, policy.clone(), effects.clone());
    runtime.install(package.clone()).await.unwrap();
    (stream, package, policy, effects, runtime)
}

#[tokio::test]
async fn retry_returns_original_checkpoint_and_reopen_replays_the_pinned_machine() {
    let (stream, package, policy, effects, host) = fixture().await;
    let owner = placement();
    host.place(owner.clone(), json!(10)).await.unwrap();
    let first = host
        .deliver(owner.clone(), event(2, json!(3)))
        .await
        .unwrap();
    assert_eq!(first.checkpoint.state, json!(13));
    assert_eq!(first.consumed, 1);
    host.deliver(owner.clone(), event(3, json!(4)))
        .await
        .unwrap();
    assert_eq!(
        host.deliver(owner.clone(), event(2, json!(3)))
            .await
            .unwrap(),
        first
    );
    assert!(matches!(
        host.deliver(owner.clone(), event(2, json!(99))).await,
        Err(Error::Conflict(_))
    ));
    let reopened = runtime(stream.clone(), &package, policy, effects);
    reopened.install(package).await.unwrap();
    let recovered = reopened.recover(owner.clone()).await.unwrap();
    assert_eq!(recovered.checkpoint.state, json!(17));
    assert_eq!((recovered.checkpoint.revision, recovered.consumed), (2, 2));
    assert_eq!(
        stream
            .tail(StreamPath::new(format!("runtime/{}", Manifest::inbox(owner.actor))).unwrap())
            .await
            .unwrap(),
        2
    );
    assert!(reopened.place(owner, json!(11)).await.is_err());
}

#[tokio::test]
async fn move_fences_old_owner_before_recovery_and_rejects_stale_writes() {
    let (_, package, policy, effects, _) = fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .unwrap(),
    );
    let host = InProcessRuntime::new(
        stream,
        package.manifest.bindings.clone(),
        policy,
        effects.clone(),
    );
    host.install(package).await.unwrap();
    let old = placement();
    host.place(old.clone(), json!(0)).await.unwrap();
    host.deliver(old.clone(), event(2, json!(5))).await.unwrap();
    let new = Placement {
        epoch: 2,
        node: "node-2".into(),
        ..old.clone()
    };
    host.place(new.clone(), json!(0)).await.unwrap();
    assert_eq!(effects.fences.load(Ordering::SeqCst), 1);
    let actions = [
        conformance::Action::Recover(old.clone()),
        conformance::Action::Deliver {
            placement: old,
            event: event(3, json!(10)),
        },
        conformance::Action::Recover(new),
    ];
    let results = conformance::history(&host, &actions).await;
    assert!(results[0].is_err());
    assert!(results[1].is_err());
    assert_eq!(results[2].as_ref().unwrap().checkpoint.state, json!(5));
}

#[tokio::test]
async fn cancellation_request_survives_failed_drain_and_recovery_settles_it() {
    let (_, package, policy, effects, _) = fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .unwrap(),
    );
    let host = InProcessRuntime::new(
        stream.clone(),
        package.manifest.bindings.clone(),
        policy.clone(),
        effects.clone(),
    );
    host.install(package.clone()).await.unwrap();
    let owner = placement();
    host.place(owner.clone(), json!(0)).await.unwrap();
    effects.unavailable.store(true, Ordering::SeqCst);
    assert!(
        host.cancel(owner.clone(), event(2, json!({"cancel":true})))
            .await
            .is_err()
    );
    assert!(
        host.deliver(owner.clone(), event(3, json!(1)))
            .await
            .is_err()
    );
    let reopened = InProcessRuntime::new(
        stream,
        package.manifest.bindings.clone(),
        policy,
        effects.clone(),
    );
    reopened.install(package).await.unwrap();
    effects.unavailable.store(false, Ordering::SeqCst);
    let settled = reopened.recover(owner.clone()).await.unwrap();
    assert!(settled.cancelled);
    assert_eq!(settled.consumed, 1);
    assert!(settled.pending.is_empty());
    assert_eq!(
        reopened
            .cancel(owner.clone(), event(2, json!({"cancel":true})))
            .await
            .unwrap(),
        settled
    );
    assert!(
        reopened
            .deliver(owner, event(2, json!({"cancel":true})))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn unconfirmed_cancellation_does_not_fence_live_effects() {
    let (_, _, _, effects, host) = fixture().await;
    let owner = placement();
    host.place(owner.clone(), json!(0)).await.unwrap();
    assert!(
        host.cancel(owner, event(2, json!({"cancel":true})))
            .await
            .is_err()
    );
    assert_eq!(effects.fences.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn admission_revocation_and_binding_mismatch_publish_nothing() {
    let (stream, package, policy, _, host) = fixture().await;
    let owner = placement();
    policy.0.store(false, Ordering::SeqCst);
    assert!(matches!(
        host.place(owner.clone(), json!(0)).await,
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        stream
            .tail(StreamPath::new(format!("runtime/{}", Manifest::workflow(owner.actor))).unwrap())
            .await,
        Err(acyclic_stream::StreamError::NotFound)
    ));
    policy.0.store(true, Ordering::SeqCst);
    let mut other = package.manifest.clone();
    other.bindings.compute = "other-executor".into();
    let other = Arc::new(Counter {
        manifest: other,
        schema: package.schema.clone(),
    });
    assert!(host.install(other).await.is_err());
    host.place(owner.clone(), json!(0)).await.unwrap();
    policy.0.store(false, Ordering::SeqCst);
    assert!(host.deliver(owner, event(2, json!(1))).await.is_err());
}

#[tokio::test]
async fn invalid_initial_state_never_creates_an_actor() {
    let (stream, _, _, _, host) = fixture().await;
    let owner = placement();
    assert!(host.place(owner.clone(), json!("invalid")).await.is_err());
    assert!(matches!(
        stream
            .tail(StreamPath::new(format!("runtime/{}", Manifest::workflow(owner.actor))).unwrap())
            .await,
        Err(acyclic_stream::StreamError::NotFound)
    ));
    host.place(owner, json!(0)).await.unwrap();
}

#[tokio::test]
async fn completed_actor_accepts_only_exact_retries() {
    let (_, _, _, _, host) = fixture().await;
    let owner = placement();
    host.place(owner.clone(), json!(7)).await.unwrap();
    let terminal = host
        .deliver(owner.clone(), event(2, Value::Null))
        .await
        .unwrap();
    assert_eq!(
        terminal.status,
        MachineStatus::Completed { value: json!(7) }
    );
    assert_eq!(
        host.deliver(owner.clone(), event(2, Value::Null))
            .await
            .unwrap(),
        terminal
    );
    assert!(host.deliver(owner, event(3, json!(1))).await.is_err());
}

#[tokio::test]
async fn admitted_inbox_input_survives_a_crash_before_checkpoint_commit() {
    let (stream, package, policy, effects, host) = fixture().await;
    let owner = placement();
    host.place(owner.clone(), json!(10)).await.unwrap();
    let journal = host.journal(&host.package().unwrap(), &owner).unwrap();
    journal
        .admit_event(event(2, json!(3)), false)
        .await
        .unwrap();
    assert_eq!(journal.load().await.unwrap().last.snapshot.consumed, 0);
    drop(host);
    let reopened = runtime(stream, &package, policy, effects);
    reopened.install(package).await.unwrap();
    let recovered = reopened.recover(owner.clone()).await.unwrap();
    assert_eq!(recovered.checkpoint.state, json!(13));
    assert_eq!((recovered.checkpoint.revision, recovered.consumed), (1, 1));
    assert_eq!(
        reopened.deliver(owner, event(2, json!(3))).await.unwrap(),
        recovered
    );
}
