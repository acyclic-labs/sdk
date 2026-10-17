//! End-to-end ref-only effect intent and result across Filesystem and Stream.
#![cfg(feature = "filesystem")]
#![allow(
    clippy::too_many_lines,
    reason = "one ordered scenario keeps each step next to the state it checks"
)]

use crate::stream_fixture as test_stream;

use acyclic_fs::Fs;
use acyclic_harness::filesystem::{FilesystemContentVerifier, FilesystemHost};
use acyclic_harness::runtime::TaskSpawner;
use acyclic_harness::{
    AgentId, Capabilities, EffectAttemptId, IdempotencyKey, OperationId, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{
        Action, AggregateKind, Authority, AuthorityIssuer, Command, EffectGuarantee, EffectStatus,
        LifecycleState, SchemaRegistry,
    },
    effects::{EffectDispatch, EffectObservation, EffectProvider, EffectRegistry},
    resources::ProviderRef,
    store::{HistoryCursor, HistoryReadLimits, HistoryReader, StreamAggregate},
};
use acyclic_stream::{BoxProviderFuture, MemoryStream};
use futures::TryStreamExt as _;
use std::{collections::BTreeSet, sync::Arc};

struct ResultProvider(FileRef);

impl EffectProvider for ResultProvider {
    fn id(&self) -> &str {
        "test.effects"
    }
    fn guarantees(&self, _: &str) -> BTreeSet<EffectGuarantee> {
        BTreeSet::from([EffectGuarantee::IdempotentRetry])
    }
    fn linearizable_reconciliation(&self) -> bool {
        false
    }
    fn dispatch<'a>(
        &'a self,
        request: EffectDispatch,
    ) -> BoxProviderFuture<'a, Result<EffectObservation>> {
        Box::pin(async move {
            Ok(EffectObservation {
                provider: request.provider,
                effect_id: request.effect_id,
                attempt_id: request.attempt_id,
                request_digest: request.request_digest,
                guarantee: request.guarantee,
                status: EffectStatus::Succeeded {
                    result: self.0.clone(),
                },
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: EffectAttemptId,
    ) -> BoxProviderFuture<'a, Result<Option<EffectObservation>>> {
        Box::pin(async { Ok(None) })
    }
}

#[tokio::test]
async fn effect_request_and_result_bodies_never_enter_stream() -> Result<()> {
    let provider = ProviderRef::new(
        "effect-e2e".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let volume = VolumeRef::new(
        provider,
        "effect-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([61; 16])),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "effect-task".into(),
    };
    let issuer = AuthorityIssuer::new("effect-e2e", [62; 32], authority.clone());
    let scope = issuer
        .issue(
            "effect-owner",
            &Capabilities::new([
                "effect:run",
                "lifecycle:manage",
                "effect:plan",
                "effect:provider:test.effects",
                "operation:declare",
                "operation:observe",
                "operation:cancel",
                "task:spawn:test.effects@1",
                "conversation:bind",
                "fork:publish",
                &volume.capability(VolumeOperation::Read, &mut None)?,
                &volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(AgentId::from_bytes([61; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    let write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &volume,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let mut files = Vec::new();
    for (path, bytes, mime, name, key) in [
        (
            "effects/request.json",
            br#"{"secret":"request-body"}"#.as_slice(),
            "application/json",
            "request.json",
            "request-upload",
        ),
        (
            "effects/result.json",
            br#"{"receipt":"result-body"}"#.as_slice(),
            "application/json",
            "result.json",
            "result-upload",
        ),
        (
            "effects/result-schema.json",
            br#"{"type":"object","required":["receipt"]}"#.as_slice(),
            "application/schema+json",
            "result-schema.json",
            "schema-upload",
        ),
        (
            "effects/invalid-schema.json",
            br#"{"type":17}"#.as_slice(),
            "application/schema+json",
            "invalid-schema.json",
            "invalid-schema-upload",
        ),
    ] {
        files.push(
            host.put_content(
                &volume,
                &write,
                path,
                bytes,
                mime,
                name,
                4_096,
                &IdempotencyKey::new(key)?,
                None,
                None,
            )
            .await?,
        );
    }
    let [request, result, schema, invalid_schema]: [FileRef; 4] = files.try_into().unwrap();
    let verifier = FilesystemContentVerifier::new(
        host.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        4_096,
        None,
    )?;
    let provider = Arc::new(test_stream::LostSessionAck::<MemoryStream>::default());
    let stream = provider.clone();
    let mut aggregate = StreamAggregate::open(
        &stream,
        (
            authority.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(verifier.clone());
    let task = acyclic_harness::TaskId::from_bytes([66; 16]);
    let effect_command = OperationId::from_bytes([67; 16]);
    let effect_id = acyclic_harness::effect_host::task_effect_id(task, effect_command, &mut None)?;
    let command = |number: u8, expected_revision, action| Command {
        operation_id: OperationId::from_bytes([number; 16]),
        idempotency_key: IdempotencyKey(format!("effect-{number}")),
        expected_revision,
        scope: scope.clone(),
        causal_parent: None,
        action,
    };
    assert!(
        aggregate
            .execute(
                command(
                    9,
                    0,
                    Action::PlanEffect {
                        effect_id,
                        provider: "test.effects".into(),
                        guarantee: EffectGuarantee::IdempotentRetry,
                        effect_kind: "test.write".into(),
                        request: request.clone(),
                        result_schema: invalid_schema,
                    }
                ),
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(aggregate.reducer().revision(), 0);
    let owner = running_owner(stream.clone(), host.clone(), &volume, &issuer, &scope, task).await?;
    aggregate
        .execute(
            command(
                68,
                0,
                Action::BindConversation {
                    agent: AgentId::from_bytes([61; 16]),
                },
            ),
            Some(&owner),
            &mut None,
        )
        .await?;
    let mut registry = EffectRegistry::new(None, Some(verifier.clone()))?;
    registry.register(Arc::new(ResultProvider(result)), None)?;
    let effects = acyclic_harness::effect_host::ConversationEffectHost::new(
        stream.clone(),
        (
            authority.clone(),
            issuer.copy(&mut None)?,
            scope.copy(&mut None)?,
        ),
        SchemaRegistry::new(),
        verifier.clone(),
        registry,
        None,
        None,
    )?;
    effects
        .run_task_effect(
            &owner,
            effect_command,
            &acyclic_harness::effect_host::TaskEffectPlan {
                provider: "test.effects".into(),
                guarantee: EffectGuarantee::IdempotentRetry,
                effect_kind: "test.write".into(),
                request,
                result_schema: schema,
            },
            None,
            false,
        )
        .await?;
    aggregate = StreamAggregate::open(
        &stream,
        (
            authority.clone(),
            issuer.verifier(&mut None)?,
            SchemaRegistry::new(),
        ),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(verifier.clone());
    let history = HistoryReader::new(&stream, &authority, &issuer.verifier(&mut None)?, &mut None)?;
    let historical = |through_revision| HistoryCursor {
        authority: authority.clone(),
        after_revision: 0,
        through_revision,
    };
    let read_limits = HistoryReadLimits {
        maximum_events: 3,
        maximum_bytes: 1_000_000,
    };
    let planned = history
        .effect(&historical(2), effect_id, read_limits, None)
        .await?
        .ok_or_else(|| acyclic_harness::Error::NotFound("planned effect".into()))?
        .0;
    let dispatched = history
        .effect(&historical(3), effect_id, read_limits, None)
        .await?
        .ok_or_else(|| acyclic_harness::Error::NotFound("dispatched effect".into()))?
        .0;
    assert!(matches!(
        aggregate
            .reducer()
            .effect(effect_id)
            .map(|state| &state.status),
        Some(EffectStatus::Succeeded { .. })
    ));
    let raw = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&stream),
        authority.stream_path(&mut None)?,
    )
    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
    .read(0, 5)
    .await
    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
    .try_collect::<Vec<_>>()
    .await
    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert_eq!(raw.len(), 4);
    for record in &raw {
        for body in [
            b"request-body".as_slice(),
            b"result-body".as_slice(),
            b"required".as_slice(),
        ] {
            assert!(
                !record
                    .value
                    .windows(body.len())
                    .any(|window| window == body)
            );
        }
    }
    use std::sync::atomic::Ordering;
    let probe_reader = HistoryReader::new(
        &stream,
        &authority,
        &(issuer.verifier(&mut None).expect("verifier copy")),
        &mut None,
    )?;
    provider.observation_bytes.store(0, Ordering::SeqCst);
    probe_reader
        .effect(
            &probe_reader.pin(0, None).await?,
            effect_id,
            HistoryReadLimits {
                maximum_events: 3,
                maximum_bytes: 1_000_000,
            },
            None,
        )
        .await
        .map(|state| state.map(|(state, _)| state))?;
    let bytes = provider.observation_bytes.load(Ordering::SeqCst);
    let limits = HistoryReadLimits {
        maximum_events: 3,
        maximum_bytes: bytes,
    };
    let completed = aggregate
        .reducer()
        .effect(effect_id)
        .cloned()
        .ok_or_else(|| acyclic_harness::Error::Invalid("completed effect missing".into()))?;
    let original_cut = probe_reader.pin(0, None).await?;
    provider.forbid_writes.store(true, Ordering::SeqCst);
    provider.hide_effect_node_read.store(true, Ordering::SeqCst);
    assert!(
        probe_reader
            .effect(&original_cut, effect_id, limits, None)
            .await
            .map(|state| state.map(|(state, _)| state))
            .is_err()
    );
    provider
        .hide_effect_node_read
        .store(false, Ordering::SeqCst);
    provider
        .hide_effect_transition_position
        .store(3, Ordering::SeqCst);
    assert!(
        probe_reader
            .effect(&original_cut, effect_id, limits, None)
            .await
            .map(|state| state.map(|(state, _)| state))
            .is_err()
    );
    provider
        .hide_effect_transition_position
        .store(0, Ordering::SeqCst);
    assert_eq!(
        probe_reader
            .effect(&original_cut, effect_id, limits, None)
            .await
            .map(|state| state.map(|(state, _)| state))?,
        Some(completed.clone())
    );
    provider.forbid_writes.store(false, Ordering::SeqCst);
    const N: u64 = 6;
    let mut observations = Vec::new();
    for retained in 4..=4 + 10 * N {
        if retained > 4 {
            provider.forbid_writes.store(false, Ordering::SeqCst);
            let revision = aggregate.reducer().revision();
            aggregate
                .execute(
                    Command {
                        operation_id: OperationId::from_bytes(
                            uuid::Uuid::from_u128(u128::from(retained) + 100_000).into_bytes(),
                        ),
                        idempotency_key: IdempotencyKey::new(format!(
                            "retained-effect-history-{retained}"
                        ))?,
                        expected_revision: revision,
                        scope: scope.clone(),
                        causal_parent: None,
                        action: Action::TransitionLifecycle {
                            to: if retained.is_multiple_of(2) {
                                LifecycleState::Waiting
                            } else {
                                LifecycleState::Active
                            },
                            reason: None,
                        },
                    },
                    #[cfg(feature = "filesystem")]
                    None,
                    &mut None,
                )
                .await?;
        }
        if retained == 4 + N || retained == 4 + 10 * N {
            provider.forbid_writes.store(true, Ordering::SeqCst);
            provider.observation_reads.store(0, Ordering::SeqCst);
            provider.observation_maximum.store(0, Ordering::SeqCst);
            provider.observation_tails.store(0, Ordering::SeqCst);
            let io = acyclic_harness::filesystem::PreparationIo::from_budget(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &acyclic_fs::CancellationToken::new(),
            )?;
            let mut history_io = &io;
            let cold = HistoryReader::new(
                &stream,
                &authority,
                &(issuer.verifier(&mut None).expect("verifier copy")),
                &mut Some(&mut history_io),
            )?;
            let cursor = cold.pin(0, Some(&mut history_io)).await?;
            assert_eq!(
                cold.effect(&cursor, effect_id, limits, Some(&mut history_io))
                    .await
                    .map(|state| state.map(|(state, _)| state))?,
                Some(completed.clone())
            );
            let work = match &io {
                acyclic_harness::filesystem::PreparationIo::Owned { work, .. } => work,
                #[cfg(not(target_arch = "wasm32"))]
                acyclic_harness::filesystem::PreparationIo::Native { .. } => {
                    unreachable!("standalone measurement")
                }
            };
            observations.push(work.lock().expect("work counters").0);
        }
    }
    let [small, large] = observations.as_slice() else {
        panic!("missing growth observations")
    };
    assert!(small.allocation_operations > 0 && small.peak_allocation_bytes > 0);
    let small = serde_json::to_value(small).expect("work counters");
    let large = serde_json::to_value(large).expect("work counters");
    for (counter, value) in small.as_object().expect("work counters") {
        assert!(
            large
                .get(counter)
                .and_then(serde_json::Value::as_u64)
                .expect("counter")
                <= value.as_u64().expect("counter"),
            "{counter} must stay bounded"
        );
    }
    let reader = HistoryReader::new(
        &stream,
        &authority,
        &(issuer.verifier(&mut None).expect("verifier copy")),
        &mut None,
    )?;
    let cursor = reader.pin(0, None).await?;
    for (cut, expected) in [(2, planned), (3, dispatched), (4, completed.clone())] {
        let pinned = HistoryCursor {
            through_revision: cut,
            ..cursor.clone()
        };
        provider.observation_bytes.store(0, Ordering::SeqCst);
        reader
            .effect(
                &pinned,
                effect_id,
                HistoryReadLimits {
                    maximum_events: u32::try_from(cut)
                        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?,
                    maximum_bytes: 1_000_000,
                },
                None,
            )
            .await
            .map(|state| state.map(|(state, _)| state))?;
        let exact = HistoryReadLimits {
            maximum_events: u32::try_from(cut)
                .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?,
            maximum_bytes: provider.observation_bytes.load(Ordering::SeqCst),
        };
        assert_eq!(
            reader
                .effect(
                    &HistoryCursor {
                        through_revision: cut,
                        ..cursor.clone()
                    },
                    effect_id,
                    exact,
                    None
                )
                .await
                .map(|state| state.map(|(state, _)| state))?,
            Some(expected)
        );
        assert!(
            reader
                .effect(
                    &pinned,
                    effect_id,
                    HistoryReadLimits {
                        maximum_bytes: exact.maximum_bytes - 1,
                        ..exact
                    },
                    None
                )
                .await
                .map(|state| state.map(|(state, _)| state))
                .is_err()
        );
    }
    for fault in 1..=4 {
        provider.history_read_fault.store(fault, Ordering::SeqCst);
        assert!(
            reader
                .effect(&cursor, effect_id, limits, None)
                .await
                .map(|state| state.map(|(state, _)| state))
                .is_err()
        );
        assert_eq!(
            reader
                .effect(&cursor, effect_id, limits, None)
                .await
                .map(|state| state.map(|(state, _)| state))?,
            Some(completed.clone())
        );
    }
    let wrong = HistoryReader::new(
        &stream,
        &authority,
        &(AuthorityIssuer::new("effect-e2e", [99; 32], authority.clone())
            .verifier(&mut None)
            .expect("verifier copy")),
        &mut None,
    )?;
    assert!(
        wrong
            .effect(&cursor, effect_id, limits, None)
            .await
            .map(|state| state.map(|(state, _)| state))
            .is_err()
    );
    assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);

    Ok(())
}

async fn running_owner<P: acyclic_stream::StreamProvider + 'static>(
    stream: Arc<P>,
    filesystem: Arc<
        FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>,
    >,
    volume: &VolumeRef,
    issuer: &AuthorityIssuer,
    signed: &acyclic_harness::core::Scope,
    task: acyclic_harness::TaskId,
) -> Result<acyclic_harness::durable_host::TaskJournalOwner<P>> {
    use acyclic_harness::{
        runtime::{RuntimeScope, TaskAdmissionRecord, TaskDefinition, TaskRegistry},
        workflow::{MachineIdentity, MachineRegistry, ResumableMachine},
    };
    let schema = serde_json::json!({"type":"object"});
    let machine = Arc::new(EffectMachine {
        identity: MachineIdentity {
            name: "test.effects".into(),
            version: "1".into(),
            digest: [69; 32],
        },
        schema: schema.clone(),
    });
    let definition = TaskDefinition::<serde_json::Value, serde_json::Value>::resumable(
        machine.clone(),
        schema.clone(),
        schema.clone(),
    )?;
    let identity = definition.identity().clone();
    let machine_identity = machine.identity().clone();
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
    let scope = RuntimeScope::new(
        signed.capabilities().clone(),
        acyclic_harness::conversation::Limits::default(),
    )?;
    let runtime = acyclic_harness::filesystem::FilesystemTaskRuntime::open(
        stream,
        filesystem,
        volume,
        None,
        &issuer.verifier(&mut None)?,
        signed,
        &scope,
        &tasks,
        &machines,
        &acyclic_harness::tool::ToolRegistry::default(),
        acyclic_harness::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 4,
            depth: 4,
            model_steps: 4,
        },
        1,
        scope.limits().file_bytes,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        None,
        &[],
        &acyclic_harness::durable_tool::ResumableToolRegistry::default(),
        None,
    )
    .await?;
    let host = runtime.task_host();
    host.admit(TaskAdmissionRecord {
        operation_id: OperationId::from_bytes(task.into_bytes()),
        resource_proofs: Vec::new(),
        task: identity,
        machine: machine_identity,
        input: serde_json::json!({}),
        input_schema: schema.clone(),
        output_schema: schema,
        parent: None,
        grants: scope.grants().clone(),
        limits: scope.limits(),
        run_limits: scope.run_limits(),
        policy: None,
        extensions: None,
        execution: None,
    })
    .await?;
    let worker = acyclic_harness::distributed::Worker {
        id: "effect-worker".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let acyclic_harness::distributed::WorkPull::Claimed(lease) =
        host.pull_work(&worker, None, &mut None).await?
    else {
        return Err(acyclic_harness::Error::NotFound("effect task lease".into()));
    };
    host.start_task(&lease, &mut None).await?;
    host.journal_owner(task, &(&lease.reservation).into(), None)
        .await
}

struct EffectMachine {
    identity: acyclic_harness::workflow::MachineIdentity,
    schema: serde_json::Value,
}
impl acyclic_harness::workflow::ResumableMachine for EffectMachine {
    fn identity(&self) -> &acyclic_harness::workflow::MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &serde_json::Value {
        &self.schema
    }
    fn initialize(&self, input: &serde_json::Value) -> Result<serde_json::Value> {
        Ok(input.clone())
    }
    fn transition(
        &self,
        state: &serde_json::Value,
        _: &serde_json::Value,
    ) -> Result<acyclic_harness::workflow::MachineTransition> {
        Ok(acyclic_harness::workflow::MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: acyclic_harness::workflow::MachineStatus::Suspended,
        })
    }
}
