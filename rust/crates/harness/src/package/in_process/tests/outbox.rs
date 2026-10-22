use super::*;
use crate::{
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};

struct Machine {
    package: Arc<Counter>,
}
impl ActorPackage for Machine {
    fn manifest(&self) -> &Manifest {
        &self.package.manifest
    }
    fn machine(&self) -> Arc<dyn ResumableMachine> {
        Arc::new(Self {
            package: self.package.clone(),
        })
    }
}
impl ResumableMachine for Machine {
    fn identity(&self) -> &MachineIdentity {
        &self.package.manifest.agent
    }
    fn state_schema(&self) -> &Value {
        &self.package.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        if let Some(value) = input.get("command") {
            return Ok(MachineTransition {
                state: state.clone(),
                commands: vec![
                    serde_json::from_value(value.clone())
                        .map_err(|error| journal::codec_error(&error))?,
                ],
                status: MachineStatus::Suspended,
            });
        }
        self.package.transition(state, input)
    }
}

struct Host {
    calls: AtomicUsize,
    unavailable: AtomicBool,
}
impl EffectHost for Host {
    fn execute(
        &self,
        _: Placement,
        command: WorkflowCommand,
    ) -> BoxProviderFuture<'_, Result<Event>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.unavailable.load(Ordering::SeqCst) {
                return Err(Error::Indeterminate(command.operation_id));
            }
            Ok(Event {
                operation_id: effect_result_operation(command.operation_id),
                idempotency_key: IdempotencyKey::new(format!(
                    "effect-result:{}",
                    command.operation_id
                ))?,
                input: json!(3),
            })
        })
    }
    fn fence(&self, _: Placement) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
fn command() -> WorkflowCommand {
    let payload = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("fixture".into(), "filesystem".into(), "1".into(), &mut None).unwrap(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(placement().actor),
            &mut None,
        )
        .unwrap(),
        "commands/effect.json".into(),
        "immutable-version".into(),
        FileDescriptor::from_bytes(b"{}", "application/json", &mut None).unwrap(),
        "effect.json".into(),
        &mut None,
    )
    .unwrap();
    WorkflowCommand {
        operation_id: OperationId::from_bytes([9; 16]),
        kind: "model.https".into(),
        payload,
    }
}
fn machine() -> Arc<Machine> {
    let mut counter = package().manifest.clone();
    counter.effects = vec!["model.https".into()];
    Arc::new(Machine {
        package: Arc::new(Counter {
            manifest: counter,
            schema: json!({"type":"integer","minimum":0}),
        }),
    })
}

#[tokio::test]
async fn caller_inputs_cannot_acknowledge_a_runtime_effect_or_reserve_its_result() {
    let directory = tempfile::tempdir().unwrap();
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .unwrap(),
    );
    let package = machine();
    let effects = Arc::new(Host {
        calls: AtomicUsize::new(0),
        unavailable: AtomicBool::new(false),
    });
    let host = InProcessRuntime::new(
        stream,
        package.manifest().bindings.clone(),
        Arc::new(Policy(AtomicBool::new(true))),
        effects.clone(),
    );
    host.install(package).await.unwrap();
    let owner = placement();
    host.place(owner.clone(), json!(0)).await.unwrap();
    host.deliver(owner.clone(), event(2, json!({"command":command()})))
        .await
        .unwrap();
    let forged = Event {
        operation_id: effect_result_operation(command().operation_id),
        idempotency_key: IdempotencyKey::new("forged-result").unwrap(),
        input: json!(99),
    };
    assert!(matches!(
        host.deliver(owner.clone(), forged.clone()).await,
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        host.cancel(owner.clone(), forged).await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(effects.calls.load(Ordering::SeqCst), 0);
    let settled = host.recover(owner.clone()).await.unwrap();
    assert_eq!(settled.checkpoint.state, json!(3));
    assert_eq!((settled.checkpoint.revision, settled.consumed), (2, 2));
    assert!(settled.pending.is_empty());
    // Reusing a historical result cannot bypass the ordinary admission boundary.
    let result = Event {
        operation_id: effect_result_operation(command().operation_id),
        idempotency_key: IdempotencyKey::new(format!("effect-result:{}", command().operation_id))
            .unwrap(),
        input: json!(3),
    };
    assert!(matches!(
        host.deliver(owner, result).await,
        Err(Error::Unauthorized(_))
    ));
}

#[tokio::test]
async fn command_results_cannot_reuse_an_earlier_input_identity() {
    let stream = Arc::new(MemoryStream::default());
    let package = machine();
    let host = InProcessRuntime::new(
        stream,
        package.manifest().bindings.clone(),
        Arc::new(Policy(AtomicBool::new(true))),
        Arc::new(Host {
            calls: AtomicUsize::new(0),
            unavailable: AtomicBool::new(false),
        }),
    );
    host.install(package).await.unwrap();
    let owner = placement();
    host.place(owner.clone(), json!(0)).await.unwrap();
    let mut earlier = event(2, json!(1));
    earlier.operation_id = effect_result_operation(command().operation_id);
    host.deliver(owner.clone(), earlier).await.unwrap();
    assert!(
        host.deliver(owner.clone(), event(3, json!({"command":command()})))
            .await
            .is_err()
    );
    let valid = host.deliver(owner, event(3, json!(1))).await.unwrap();
    assert_eq!((valid.checkpoint.revision, valid.consumed), (2, 2));
    assert!(valid.pending.is_empty());
}

#[tokio::test]
async fn committed_outbox_survives_uncertain_effect_and_reopen_without_reexecuting_handler() {
    let directory = tempfile::tempdir().unwrap();
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .unwrap(),
    );
    let package = machine();
    let effects = Arc::new(Host {
        calls: AtomicUsize::new(0),
        unavailable: AtomicBool::new(true),
    });
    let policy = Arc::new(Policy(AtomicBool::new(true)));
    let host = InProcessRuntime::new(
        stream.clone(),
        package.manifest().bindings.clone(),
        policy.clone(),
        effects.clone(),
    );
    host.install(package.clone()).await.unwrap();
    let owner = placement();
    host.place(owner.clone(), json!(10)).await.unwrap();
    let admitted = host
        .deliver(owner.clone(), event(2, json!({"command":command()})))
        .await
        .unwrap();
    assert_eq!((admitted.checkpoint.revision, admitted.consumed), (1, 1));
    assert_eq!(admitted.pending, [command()]);
    assert!(matches!(
        host.recover(owner.clone()).await,
        Err(Error::Indeterminate(_))
    ));
    let reopened = InProcessRuntime::new(
        stream.clone(),
        package.manifest().bindings.clone(),
        policy,
        effects.clone(),
    );
    reopened.install(package).await.unwrap();
    effects.unavailable.store(false, Ordering::SeqCst);
    let recovered = reopened.recover(owner.clone()).await.unwrap();
    assert_eq!(recovered.checkpoint.state, json!(13));
    assert_eq!((recovered.checkpoint.revision, recovered.consumed), (2, 2));
    assert!(recovered.pending.is_empty());
    assert_eq!(effects.calls.load(Ordering::SeqCst), 2);
    assert_eq!(reopened.recover(owner.clone()).await.unwrap(), recovered);
    assert_eq!(effects.calls.load(Ordering::SeqCst), 2);
    // The inbox and both retry indexes share an admission envelope. Consumed
    // cursor, checkpoint and complete outbox share the later workflow record.
    let log = stream
        .read(acyclic_stream::ReadRequest {
            path: StreamPath::new(format!("runtime/{}", Manifest::workflow(owner.actor))).unwrap(),
            from: 1,
            limit: 1,
        })
        .await
        .unwrap();
    let inbox = stream
        .read(acyclic_stream::ReadRequest {
            path: StreamPath::new(format!("runtime/{}", Manifest::inbox(owner.actor))).unwrap(),
            from: 0,
            limit: 1,
        })
        .await
        .unwrap();
    use futures::TryStreamExt;
    let log = log.try_collect::<Vec<_>>().await.unwrap();
    let inbox = inbox.try_collect::<Vec<_>>().await.unwrap();
    let entry: journal::Entry = serde_json::from_slice(&log[0].value).unwrap();
    assert_eq!(entry.snapshot, admitted);
    assert_eq!(
        entry.workflow.as_ref().unwrap().operation_id,
        event(2, Value::Null).operation_id
    );
    let index = stream
        .read(acyclic_stream::ReadRequest {
            path: StreamPath::new(format!(
                "runtime/{}/operations/{}",
                Manifest::workflow(owner.actor),
                event(2, Value::Null).operation_id
            ))
            .unwrap(),
            from: 0,
            limit: 1,
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(index[0].commit_id, inbox[0].commit_id);
}

#[tokio::test]
async fn unconfirmed_outbox_never_dispatches_and_undeclared_effect_never_commits() {
    let stream = Arc::new(MemoryStream::default());
    let package = machine();
    let effects = Arc::new(Host {
        calls: AtomicUsize::new(0),
        unavailable: AtomicBool::new(false),
    });
    let host = InProcessRuntime::new(
        stream,
        package.manifest().bindings.clone(),
        Arc::new(Policy(AtomicBool::new(true))),
        effects.clone(),
    );
    host.install(package).await.unwrap();
    let owner = placement();
    host.place(owner.clone(), json!(0)).await.unwrap();
    let mut undeclared = command();
    undeclared.kind = "not-declared".into();
    assert!(
        host.deliver(owner.clone(), event(2, json!({"command":undeclared})))
            .await
            .is_err()
    );
    let current = host.recover(owner.clone()).await.unwrap();
    assert_eq!((current.checkpoint.revision, current.consumed), (0, 0));
    assert!(current.pending.is_empty());
    host.deliver(owner.clone(), event(2, json!({"command":command()})))
        .await
        .unwrap();
    assert!(host.recover(owner).await.is_err());
    assert_eq!(effects.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn reused_effect_identity_is_rejected_before_it_can_poison_the_inbox() {
    let stream = Arc::new(MemoryStream::default());
    let package = machine();
    let host = InProcessRuntime::new(
        stream.clone(),
        package.manifest().bindings.clone(),
        Arc::new(Policy(AtomicBool::new(true))),
        Arc::new(Host {
            calls: AtomicUsize::new(0),
            unavailable: AtomicBool::new(false),
        }),
    );
    host.install(package).await.unwrap();
    let owner = placement();
    host.place(owner.clone(), json!(0)).await.unwrap();
    host.deliver(owner.clone(), event(2, json!({"command":command()})))
        .await
        .unwrap();
    assert!(
        host.deliver(owner.clone(), event(3, json!({"command":command()})))
            .await
            .is_err()
    );
    // The rejected identity has not been bound to an immutable bad input.
    let valid = host.deliver(owner, event(3, json!(1))).await.unwrap();
    assert_eq!((valid.checkpoint.revision, valid.consumed), (2, 2));
    assert_eq!(valid.checkpoint.state, json!(1));
    assert_eq!(valid.pending, [command()]);
}

#[tokio::test]
async fn cancellation_can_close_an_inbox_with_an_unprocessable_earlier_input() {
    let directory = tempfile::tempdir().unwrap();
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .unwrap(),
    );
    let package = machine();
    let host = InProcessRuntime::new(
        stream,
        package.manifest().bindings.clone(),
        Arc::new(Policy(AtomicBool::new(true))),
        Arc::new(Host {
            calls: AtomicUsize::new(0),
            unavailable: AtomicBool::new(false),
        }),
    );
    host.install(package).await.unwrap();
    let owner = placement();
    host.place(owner.clone(), json!(0)).await.unwrap();
    let journal = host.journal(&host.package().unwrap(), &owner).unwrap();
    journal
        .admit_event(event(2, json!({"command":"invalid"})), false)
        .await
        .unwrap();
    assert!(host.recover(owner.clone()).await.is_err());
    let settled = host
        .cancel(owner.clone(), event(3, Value::Null))
        .await
        .unwrap();
    assert!(settled.cancelled);
    assert_eq!(settled.consumed, 2);
    assert_eq!(settled.checkpoint.revision, 0);
    assert_eq!(host.recover(owner).await.unwrap(), settled);
}

#[tokio::test]
async fn cancellation_inbox_admission_survives_a_crash_before_effect_drain() {
    let directory = tempfile::tempdir().unwrap();
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .unwrap(),
    );
    let package = machine();
    let policy = Arc::new(Policy(AtomicBool::new(true)));
    let effects = Arc::new(Host {
        calls: AtomicUsize::new(0),
        unavailable: AtomicBool::new(false),
    });
    let host = InProcessRuntime::new(
        stream.clone(),
        package.manifest().bindings.clone(),
        policy.clone(),
        effects.clone(),
    );
    host.install(package.clone()).await.unwrap();
    let owner = placement();
    host.place(owner.clone(), json!(0)).await.unwrap();
    let journal = host.journal(&host.package().unwrap(), &owner).unwrap();
    journal
        .admit_event(event(2, json!({"command":"invalid"})), false)
        .await
        .unwrap();
    journal
        .admit_event(event(3, Value::Null), true)
        .await
        .unwrap();
    drop(host);
    let reopened =
        InProcessRuntime::new(stream, package.manifest().bindings.clone(), policy, effects);
    reopened.install(package).await.unwrap();
    let settled = reopened.recover(owner).await.unwrap();
    assert!(settled.cancelled);
    assert_eq!(settled.consumed, 2);
    assert_eq!(settled.checkpoint.revision, 0);
}
