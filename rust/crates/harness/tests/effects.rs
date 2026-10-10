//! End-to-end ref-only effect intent and result across Filesystem and Stream.
#![cfg(feature = "filesystem")]
#![allow(
    clippy::too_many_lines,
    reason = "one ordered scenario keeps each step next to the state it checks"
)]

#[path = "support/stream.rs"]
mod test_stream;

use acyclic_fs::Fs;
use acyclic_harness::filesystem::{FilesystemContentVerifier, FilesystemHost};
use acyclic_harness::{
    AgentId, Capabilities, EffectAttemptId, EffectId, IdempotencyKey, OperationId, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{
        Action, AggregateKind, Authority, AuthorityIssuer, Command, EffectGuarantee, EffectStatus,
        LifecycleState, SchemaRegistry,
    },
    effects::{EffectDispatch, EffectObservation, EffectProvider, EffectRegistry},
    resources::ProviderRef,
    store::{HistoryCursor, HistoryReadLimits, HistoryReader, StreamAggregate},
};
use acyclic_stream::{MemoryStream, StreamClient};
use futures::{TryStreamExt as _, future::BoxFuture};
use std::{collections::BTreeSet, sync::Arc};

struct ResultProvider(FileRef, bool);

impl EffectProvider for ResultProvider {
    fn id(&self) -> &str {
        if self.1 {
            "test.uncertain"
        } else {
            "test.effects"
        }
    }
    fn guarantees(&self, _: &str) -> BTreeSet<EffectGuarantee> {
        BTreeSet::from([EffectGuarantee::IdempotentRetry])
    }
    fn linearizable_reconciliation(&self) -> bool {
        false
    }
    fn dispatch<'a>(&'a self, request: EffectDispatch) -> BoxFuture<'a, Result<EffectObservation>> {
        Box::pin(async move {
            Ok(EffectObservation {
                provider: request.provider,
                effect_id: request.effect_id,
                attempt_id: request.attempt_id,
                request_digest: request.request_digest,
                guarantee: request.guarantee,
                status: if self.1 {
                    EffectStatus::Indeterminate
                } else {
                    EffectStatus::Succeeded {
                        result: self.0.clone(),
                    }
                },
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: EffectAttemptId,
    ) -> BoxFuture<'a, Result<Option<EffectObservation>>> {
        Box::pin(async { Ok(None) })
    }
}

#[tokio::test]
async fn effect_request_and_result_bodies_never_enter_stream() -> Result<()> {
    let provider = ProviderRef::new("effect-e2e", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let volume = VolumeRef::new(
        provider,
        "effect-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([61; 16])),
    )?;
    host.create_volume(&volume).await?;
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "effect-task".into(),
    };
    let issuer = AuthorityIssuer::new("effect-e2e", [62; 32], authority.clone());
    let scope = issuer.root_for_agent(
        AgentId::from_bytes([61; 16]),
        "effect-owner",
        Capabilities::new([
            "effect:run",
            "lifecycle:manage",
            "effect:plan",
            "effect:provider:test.effects",
            "effect:provider:test.uncertain",
            &volume.capability(VolumeOperation::Read)?,
            &volume.capability(VolumeOperation::Write)?,
        ]),
    );
    let write = ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Write)?;
    let request = host
        .put_content(
            &volume,
            &write,
            "effects/request.json",
            br#"{"secret":"request-body"}"#,
            "application/json",
            "request.json",
            4_096,
            &IdempotencyKey::new("request-upload")?,
        )
        .await?;
    let result = host
        .put_content(
            &volume,
            &write,
            "effects/result.json",
            br#"{"receipt":"result-body"}"#,
            "application/json",
            "result.json",
            4_096,
            &IdempotencyKey::new("result-upload")?,
        )
        .await?;
    let schema = host
        .put_content(
            &volume,
            &write,
            "effects/result-schema.json",
            br#"{"type":"object","required":["receipt"]}"#,
            "application/schema+json",
            "result-schema.json",
            4_096,
            &IdempotencyKey::new("schema-upload")?,
        )
        .await?;
    let invalid_schema = host
        .put_content(
            &volume,
            &write,
            "effects/invalid-schema.json",
            br#"{"type":17}"#,
            "application/schema+json",
            "invalid-schema.json",
            4_096,
            &IdempotencyKey::new("invalid-schema-upload")?,
        )
        .await?;
    let verifier = Arc::new(FilesystemContentVerifier::new(
        host,
        issuer.verifier(),
        scope.clone(),
        4_096,
    )?);
    let provider = Arc::new(test_stream::LostSessionAck::<MemoryStream>::default());
    let stream = StreamClient::new(provider.clone());
    let mut aggregate = StreamAggregate::open(
        &stream,
        authority.clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
    )
    .await?
    .with_content_verifier(verifier.clone());
    let effect_id = EffectId::from_bytes([63; 16]);
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
            .execute(command(
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
            ))
            .await
            .is_err()
    );
    assert_eq!(aggregate.reducer().revision(), 0);
    aggregate
        .execute(command(
            1,
            0,
            Action::PlanEffect {
                effect_id,
                provider: "test.effects".into(),
                guarantee: EffectGuarantee::IdempotentRetry,
                effect_kind: "test.write".into(),
                request,
                result_schema: schema,
            },
        ))
        .await?;
    let planned = aggregate
        .reducer()
        .effect(effect_id)
        .cloned()
        .ok_or_else(|| acyclic_harness::Error::Invalid("planned effect missing".into()))?;
    let attempt_id = EffectAttemptId::from_bytes([64; 16]);
    aggregate
        .execute(command(
            2,
            1,
            Action::MarkEffectDispatched {
                effect_id,
                attempt_id,
            },
        ))
        .await?;
    let dispatched = aggregate
        .reducer()
        .effect(effect_id)
        .cloned()
        .ok_or_else(|| acyclic_harness::Error::Invalid("dispatched effect missing".into()))?;
    let mut registry = EffectRegistry::default().with_result_resolver(verifier.clone());
    registry.register(Arc::new(ResultProvider(result.clone(), false)))?;
    registry.register(Arc::new(ResultProvider(result, true)))?;
    let state = aggregate
        .reducer()
        .effect(effect_id)
        .ok_or_else(|| acyclic_harness::Error::Invalid("planned effect missing".into()))?;
    let dispatch = EffectDispatch::from_state(effect_id, state)?;
    let attestation = registry
        .dispatch_and_attest(&issuer, effect_id, state, dispatch)
        .await?;
    aggregate
        .execute(command(
            3,
            2,
            Action::ResolveEffect {
                observation: attestation.clone(),
            },
        ))
        .await?;
    assert!(matches!(
        aggregate
            .reducer()
            .effect(effect_id)
            .map(|state| &state.status),
        Some(EffectStatus::Succeeded { .. })
    ));
    let raw = stream
        .stream(authority.stream_path()?)
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .read(0, 4)
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert_eq!(raw.len(), 3);
    let canonical_bytes = raw
        .iter()
        .map(|record| record.value.len() as u64)
        .sum::<u64>();
    for record in &raw {
        assert!(
            !record
                .value
                .windows(b"request-body".len())
                .any(|window| window == b"request-body")
        );
        assert!(
            !record
                .value
                .windows(b"result-body".len())
                .any(|window| window == b"result-body")
        );
        assert!(
            !record
                .value
                .windows(b"required".len())
                .any(|window| window == b"required")
        );
    }
    // Original effect transitions remain constant while unrelated canonical
    // history grows. Each cold reader reconstructs the original typed state
    // through the shared core reducer, never a new provider dispatch.
    use std::sync::atomic::Ordering;
    let effect_path = format!("harness/v2/effect-transitions/tasks/effect-task/{effect_id}");
    let indexes = stream
        .stream(&effect_path)?
        .read(0, 3)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(indexes.len(), 3);
    let bytes = canonical_bytes
        + indexes
            .iter()
            .map(|record| record.value.len() as u64)
            .sum::<u64>();
    let limits = HistoryReadLimits {
        maximum_events: 3,
        maximum_bytes: bytes,
    };
    let completed = aggregate
        .reducer()
        .effect(effect_id)
        .cloned()
        .ok_or_else(|| acyclic_harness::Error::Invalid("completed effect missing".into()))?;
    for atomic in [false, true] {
        let copied = copied_effect_history(&authority, &raw, &indexes, atomic).await?;
        let copied_reader = HistoryReader::new(&copied, &authority, issuer.verifier())?;
        let result = copied_reader
            .effect(&copied_reader.pin(0).await?, effect_id, limits)
            .await;
        if atomic {
            assert_eq!(result?, Some(completed.clone()));
        } else {
            assert!(result.is_err());
        }
    }
    for retained in 3..=10_003_u64 {
        if retained > 3 {
            provider.forbid_writes.store(false, Ordering::SeqCst);
            let revision = aggregate.reducer().revision();
            aggregate
                .execute(Command {
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
                            LifecycleState::Active
                        } else {
                            LifecycleState::Waiting
                        },
                        reason: None,
                    },
                })
                .await?;
        }
        if matches!(retained, 3 | 1_003 | 10_003) {
            provider.forbid_writes.store(true, Ordering::SeqCst);
            provider.observation_reads.store(0, Ordering::SeqCst);
            provider.observation_maximum.store(0, Ordering::SeqCst);
            let cold = HistoryReader::new(&stream, &authority, issuer.verifier())?;
            let cursor = cold.pin(0).await?;
            assert_eq!(
                cold.effect(&cursor, effect_id, limits).await?,
                Some(completed.clone())
            );
            assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 6);
            assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 1);
        }
    }
    let reader = HistoryReader::new(&stream, &authority, issuer.verifier())?;
    let cursor = reader.pin(0).await?;
    for (cut, expected) in [(1, planned), (2, dispatched), (3, completed.clone())] {
        assert_eq!(
            reader
                .effect(
                    &HistoryCursor {
                        through_revision: cut,
                        ..cursor.clone()
                    },
                    effect_id,
                    limits
                )
                .await?,
            Some(expected)
        );
    }
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(
        reader
            .effect(
                &cursor,
                effect_id,
                HistoryReadLimits {
                    maximum_events: 2,
                    ..limits
                }
            )
            .await
            .is_err()
    );
    assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
    assert!(
        reader
            .effect(
                &cursor,
                effect_id,
                HistoryReadLimits {
                    maximum_bytes: bytes - 1,
                    ..limits
                }
            )
            .await
            .is_err()
    );
    let first_bytes = raw
        .first()
        .ok_or_else(|| acyclic_harness::Error::Invalid("canonical control missing".into()))?
        .value
        .len() as u64
        + indexes
            .first()
            .ok_or_else(|| acyclic_harness::Error::Invalid("index control missing".into()))?
            .value
            .len() as u64;
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(
        reader
            .effect(
                &cursor,
                effect_id,
                HistoryReadLimits {
                    maximum_bytes: first_bytes,
                    ..limits
                }
            )
            .await
            .is_err()
    );
    assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
    for fault in 1..=4 {
        provider.history_read_fault.store(fault, Ordering::SeqCst);
        assert!(reader.effect(&cursor, effect_id, limits).await.is_err());
        assert_eq!(
            reader.effect(&cursor, effect_id, limits).await?,
            Some(completed.clone())
        );
    }
    assert!(
        reader
            .effect(&cursor, EffectId::from_bytes([99; 16]), limits)
            .await
            .is_err()
    );
    assert_eq!(
        reader
            .effect(
                &HistoryCursor {
                    through_revision: 0,
                    ..cursor.clone()
                },
                effect_id,
                limits
            )
            .await?,
        None
    );
    let wrong = HistoryReader::new(
        &stream,
        &authority,
        AuthorityIssuer::new("effect-e2e", [99; 32], authority.clone()).verifier(),
    )?;
    assert!(wrong.effect(&cursor, effect_id, limits).await.is_err());
    assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);

    // terminal_effect_retirement_probe: fixed active work and a growing terminal
    // archive, including actual Filesystem validation and issuer attestations.
    provider.forbid_writes.store(false, Ordering::SeqCst);
    aggregate = aggregate.with_resident_terminal_effect_limit(2)?;
    let fresh = |ordinal: u128, expected_revision, action| Command {
        operation_id: OperationId::from_bytes(
            uuid::Uuid::from_u128(ordinal + 3_000_000).into_bytes(),
        ),
        idempotency_key: IdempotencyKey(format!("retirement-{ordinal}")),
        expected_revision,
        scope: scope.clone(),
        causal_parent: None,
        action,
    };
    let plan = |id, uncertain| Action::PlanEffect {
        effect_id: id,
        provider: if uncertain {
            "test.uncertain".into()
        } else {
            completed.provider.clone()
        },
        guarantee: completed.guarantee,
        effect_kind: completed.effect_kind.clone(),
        request: completed.request.clone(),
        result_schema: completed.result_schema.clone(),
    };
    let mut ordinal = 0_u128;
    let active = [
        EffectId::from_bytes([200; 16]),
        EffectId::from_bytes([201; 16]),
        EffectId::from_bytes([202; 16]),
    ];
    for (position, id) in active.into_iter().enumerate() {
        ordinal += 1;
        aggregate
            .execute(fresh(
                ordinal,
                aggregate.reducer().revision(),
                plan(id, position == 2),
            ))
            .await?;
        if position == 0 {
            continue;
        }
        ordinal += 1;
        aggregate
            .execute(fresh(
                ordinal,
                aggregate.reducer().revision(),
                Action::MarkEffectDispatched {
                    effect_id: id,
                    attempt_id: EffectAttemptId::from_bytes(id.into_bytes()),
                },
            ))
            .await?;
        if position == 2 {
            let state = aggregate
                .reducer()
                .effect(id)
                .ok_or_else(|| acyclic_harness::Error::Invalid("active effect missing".into()))?;
            let observation = registry
                .dispatch_and_attest(&issuer, id, state, EffectDispatch::from_state(id, state)?)
                .await?;
            ordinal += 1;
            aggregate
                .execute(fresh(
                    ordinal,
                    aggregate.reducer().revision(),
                    Action::ResolveEffect { observation },
                ))
                .await?;
        }
    }
    for retained in 1..=1_000_u128 {
        let id = EffectId::from_bytes(uuid::Uuid::from_u128(retained + 10_000_000).into_bytes());
        ordinal += 1;
        aggregate
            .execute(fresh(
                ordinal,
                aggregate.reducer().revision(),
                plan(id, false),
            ))
            .await?;
        ordinal += 1;
        aggregate
            .execute(fresh(
                ordinal,
                aggregate.reducer().revision(),
                Action::MarkEffectDispatched {
                    effect_id: id,
                    attempt_id: EffectAttemptId::from_bytes(id.into_bytes()),
                },
            ))
            .await?;
        let state = aggregate
            .reducer()
            .effect(id)
            .ok_or_else(|| acyclic_harness::Error::Invalid("new effect missing".into()))?;
        let observation = registry
            .dispatch_and_attest(&issuer, id, state, EffectDispatch::from_state(id, state)?)
            .await?;
        ordinal += 1;
        aggregate
            .execute(fresh(
                ordinal,
                aggregate.reducer().revision(),
                Action::ResolveEffect { observation },
            ))
            .await?;
        if matches!(retained, 1 | 100 | 1_000) {
            assert_eq!(aggregate.reducer().resident_terminal_effect_count(), 2);
            assert_eq!(aggregate.reducer().resident_effect_count(), 5);
            provider.forbid_writes.store(true, Ordering::SeqCst);
            provider.observation_reads.store(0, Ordering::SeqCst);
            provider.observation_maximum.store(0, Ordering::SeqCst);
            let started = std::time::Instant::now();
            let cold = StreamAggregate::open_with_projection_limits(
                &stream,
                authority.clone(),
                issuer.verifier(),
                SchemaRegistry::new(),
                acyclic_harness::store::default_projection_read_limits(),
                2,
            )
            .await?
            .with_content_verifier(verifier.clone());
            let reads = provider.observation_reads.load(Ordering::SeqCst);
            let maximum = provider.observation_maximum.load(Ordering::SeqCst);
            assert_eq!(cold.reducer().revision(), aggregate.reducer().revision());
            assert_eq!(cold.reducer().resident_terminal_effect_count(), 2);
            assert_eq!(cold.reducer().resident_effect_count(), 5);
            for (id, expected) in active.into_iter().zip([
                EffectStatus::Planned,
                EffectStatus::Dispatched,
                EffectStatus::Indeterminate,
            ]) {
                assert_eq!(
                    cold.reducer().effect(id).map(|state| &state.status),
                    Some(&expected)
                );
            }
            let snapshot_bytes = serde_json::to_vec(&cold.reducer().snapshot_with_event_limit(1)?)
                .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?
                .len();
            assert!(snapshot_bytes < 32 * 1_024);
            assert!(reads <= 4 + 2 * 64);
            assert!(maximum <= 64);
            assert_eq!(
                cold.effect(effect_id, limits).await?,
                Some(completed.clone())
            );
            if retained > 1 {
                assert!(cold.reducer().effect(effect_id).is_none());
            }
            println!(
                "terminal retirement retained={retained} resident=5 snapshot_bytes={snapshot_bytes} cold_reads={reads} max_batch={maximum} elapsed_us={}",
                started.elapsed().as_micros()
            );
            aggregate = cold;
            provider.forbid_writes.store(false, Ordering::SeqCst);
        }
    }

    // Lost acknowledgements and malformed receipts must not turn a committed
    // terminal effect into guessed absence or repeat a provider dispatch.
    for fault in 1..=5 {
        let id = EffectId::from_bytes([210 + fault; 16]);
        ordinal += 1;
        aggregate
            .execute(fresh(
                ordinal,
                aggregate.reducer().revision(),
                plan(id, false),
            ))
            .await?;
        ordinal += 1;
        aggregate
            .execute(fresh(
                ordinal,
                aggregate.reducer().revision(),
                Action::MarkEffectDispatched {
                    effect_id: id,
                    attempt_id: EffectAttemptId::from_bytes(id.into_bytes()),
                },
            ))
            .await?;
        let state = aggregate
            .reducer()
            .effect(id)
            .ok_or_else(|| acyclic_harness::Error::Invalid("fault effect missing".into()))?;
        let observation = registry
            .dispatch_and_attest(&issuer, id, state, EffectDispatch::from_state(id, state)?)
            .await?;
        let expected_status = observation.status.clone();
        ordinal += 1;
        let resolve = fresh(
            ordinal,
            aggregate.reducer().revision(),
            Action::ResolveEffect { observation },
        );
        let before = aggregate.reducer().revision();
        provider
            .aggregate_commit_fault
            .store(fault, Ordering::SeqCst);
        let first = aggregate.execute(resolve.clone()).await;
        if matches!(fault, 1 | 3 | 4 | 5) {
            assert!(first.is_err());
        }
        let tail = stream.stream(authority.stream_path()?)?.tail().await?;
        assert_eq!(tail, before + u64::from(fault != 1));
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let reopened = StreamAggregate::open_with_projection_limits(
            &stream,
            authority.clone(),
            issuer.verifier(),
            SchemaRegistry::new(),
            acyclic_harness::store::default_projection_read_limits(),
            2,
        )
        .await;
        aggregate = match reopened {
            Ok(cold) => cold,
            Err(error) => {
                assert_eq!(fault, 3, "unexpected cold-open failure: {error}");
                StreamAggregate::open_with_projection_limits(
                    &stream,
                    authority.clone(),
                    issuer.verifier(),
                    SchemaRegistry::new(),
                    acyclic_harness::store::default_projection_read_limits(),
                    2,
                )
                .await?
            }
        }
        .with_content_verifier(verifier.clone());
        provider.forbid_writes.store(false, Ordering::SeqCst);
        let retried = aggregate.execute(resolve).await?;
        assert_eq!(
            matches!(retried, acyclic_harness::core::ApplyResult::Replayed { .. }),
            fault != 1
        );
        let recovered = aggregate
            .effect(id, acyclic_harness::store::default_projection_read_limits())
            .await?
            .ok_or_else(|| acyclic_harness::Error::Invalid("recovered effect missing".into()))?;
        assert_eq!(recovered.status, expected_status);
        assert_eq!(
            recovered.attempts,
            vec![EffectAttemptId::from_bytes(id.into_bytes())]
        );
        assert_eq!(aggregate.reducer().revision(), before + 1);
        assert_eq!(aggregate.reducer().resident_terminal_effect_count(), 2);
        assert_eq!(aggregate.reducer().resident_effect_count(), 5);
    }
    // Retirement must also fence pure planning before the event cache evicts.
    let short_provider = Arc::new(test_stream::LostSessionAck::<MemoryStream>::default());
    let short_stream = StreamClient::new(short_provider.clone());
    let mut short = StreamAggregate::open_with_projection_limits(
        &short_stream,
        authority.clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
        acyclic_harness::store::default_projection_read_limits(),
        1,
    )
    .await?
    .with_content_verifier(verifier.clone());
    let mut before_terminal = None;
    let mut terminal_operations = Vec::new();
    for number in 1..=3_u128 {
        let id = EffectId::from_bytes(uuid::Uuid::from_u128(number + 20_000_000).into_bytes());
        short
            .execute(fresh(
                number * 3,
                short.reducer().revision(),
                plan(id, false),
            ))
            .await?;
        short
            .execute(fresh(
                number * 3 + 1,
                short.reducer().revision(),
                Action::MarkEffectDispatched {
                    effect_id: id,
                    attempt_id: EffectAttemptId::from_bytes(id.into_bytes()),
                },
            ))
            .await?;
        if number == 1 {
            before_terminal = Some(short.reducer().snapshot_with_event_limit(1)?);
        }
        let state = short
            .reducer()
            .effect(id)
            .ok_or_else(|| acyclic_harness::Error::Invalid("short effect missing".into()))?;
        let observation = registry
            .dispatch_and_attest(&issuer, id, state, EffectDispatch::from_state(id, state)?)
            .await?;
        terminal_operations.push((
            id,
            OperationId::from_bytes(uuid::Uuid::from_u128(number * 3 + 2 + 3_000_000).into_bytes()),
        ));
        short
            .execute(fresh(
                number * 3 + 2,
                short.reducer().revision(),
                Action::ResolveEffect { observation },
            ))
            .await?;
    }
    assert_eq!(short.reducer().revision(), 9);
    assert_eq!(short.reducer().resident_effect_count(), 1);
    assert!(matches!(
        short
            .reducer()
            .plan(&fresh(100, 9, plan(EffectId::from_bytes([240; 16]), false))),
        Err(acyclic_harness::Error::Unsupported(_))
    ));

    let before_terminal = before_terminal
        .ok_or_else(|| acyclic_harness::Error::Invalid("pre-terminal snapshot missing".into()))?;
    let mut terminal_proof_bytes = 0_u64;
    for (id, operation) in terminal_operations {
        for (path, position) in [
            (
                format!("harness/v2/aggregate-operations/tasks/effect-task/{operation}"),
                0,
            ),
            (
                format!("harness/v2/effect-transitions/tasks/effect-task/{id}"),
                2,
            ),
        ] {
            let records = short_stream
                .stream(path)?
                .read(position, 1)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            let record = records.first().ok_or_else(|| {
                acyclic_harness::Error::Invalid("original retirement proof missing".into())
            })?;
            assert_eq!(records.len(), 1);
            terminal_proof_bytes += record.value.len() as u64;
        }
    }
    let suffix = short_stream
        .stream(authority.stream_path()?)?
        .read(2, 7)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(suffix.len(), 7);
    let snapshot_bytes = serde_json::to_vec(&before_terminal)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?
        .len() as u64;
    let exact = HistoryReadLimits {
        maximum_events: 8,
        maximum_bytes: snapshot_bytes
            + terminal_proof_bytes
            + suffix
                .iter()
                .map(|record| record.value.len() as u64)
                .sum::<u64>(),
    };
    short_provider.forbid_writes.store(true, Ordering::SeqCst);
    for allowance in [exact.maximum_bytes - 1, exact.maximum_bytes] {
        short_provider.observation_reads.store(0, Ordering::SeqCst);
        let restored = StreamAggregate::open_from_snapshot_with_projection_limits(
            &short_stream,
            authority.clone(),
            issuer.verifier(),
            SchemaRegistry::new(),
            before_terminal.clone(),
            HistoryReadLimits {
                maximum_bytes: allowance,
                ..exact
            },
            1,
        )
        .await;
        if allowance == exact.maximum_bytes {
            let restored = restored?;
            assert_eq!(restored.reducer().revision(), 9);
            assert_eq!(restored.reducer().resident_effect_count(), 1);
            assert_eq!(restored.reducer().resident_terminal_effect_count(), 1);
        } else {
            assert!(matches!(restored, Err(acyclic_harness::Error::Invalid(_))));
        }
        assert_eq!(short_provider.observation_reads.load(Ordering::SeqCst), 7);
    }
    short_provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(matches!(
        StreamAggregate::open_from_snapshot_with_projection_limits(
            &short_stream,
            authority.clone(),
            issuer.verifier(),
            SchemaRegistry::new(),
            before_terminal,
            HistoryReadLimits {
                maximum_events: 7,
                ..exact
            },
            1,
        )
        .await,
        Err(acyclic_harness::Error::Invalid(_))
    ));
    assert_eq!(short_provider.observation_reads.load(Ordering::SeqCst), 0);
    assert_eq!(short_provider.observation_writes.load(Ordering::SeqCst), 0);

    // A fresh archived transition shares the configured read allowance and
    // cannot hydrate the cache or append when its proof exceeds that allowance.
    let archived_revision = aggregate.reducer().revision();
    aggregate = aggregate.with_effect_history_read_limits(HistoryReadLimits {
        maximum_events: 2,
        maximum_bytes: 32 * 1024 * 1024,
    })?;
    ordinal += 1;
    assert!(matches!(
        aggregate
            .execute(fresh(
                ordinal,
                archived_revision,
                Action::MarkEffectDispatched {
                    effect_id,
                    attempt_id: EffectAttemptId::from_bytes([243; 16]),
                },
            ))
            .await,
        Err(acyclic_harness::Error::Invalid(_))
    ));
    assert_eq!(aggregate.reducer().revision(), archived_revision);
    assert!(aggregate.reducer().effect(effect_id).is_none());
    aggregate = aggregate.with_effect_history_read_limits(HistoryReadLimits {
        maximum_events: 64,
        maximum_bytes: 32 * 1024 * 1024,
    })?;

    // A fresh command targeting known archived state must reach the original
    // provider authorization and terminal transition check, not cached absence.
    ordinal += 1;
    assert!(matches!(
        aggregate
            .execute(fresh(
                ordinal,
                aggregate.reducer().revision(),
                Action::MarkEffectDispatched {
                    effect_id,
                    attempt_id: EffectAttemptId::from_bytes([244; 16]),
                }
            ))
            .await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    ordinal += 1;
    assert!(matches!(
        aggregate
            .execute(fresh(
                ordinal,
                aggregate.reducer().revision(),
                Action::ResolveEffect {
                    observation: attestation.clone(),
                }
            ))
            .await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    ordinal += 1;
    let mut denied = fresh(
        ordinal,
        aggregate.reducer().revision(),
        Action::MarkEffectDispatched {
            effect_id,
            attempt_id: EffectAttemptId::from_bytes([245; 16]),
        },
    );
    denied.scope = issuer.root_for_agent(
        AgentId::from_bytes([61; 16]),
        "no-provider-grant",
        Capabilities::new(["effect:run"]),
    );
    assert!(matches!(
        aggregate.execute(denied).await,
        Err(acyclic_harness::Error::Unauthorized(_))
    ));
    assert_eq!(aggregate.reducer().resident_terminal_effect_count(), 2);
    assert_eq!(aggregate.reducer().resident_effect_count(), 5);
    let revision = aggregate.reducer().revision();
    assert!(aggregate.reducer().effect(effect_id).is_none());
    assert!(matches!(
        aggregate
            .execute(command(1, 0, plan(effect_id, false)))
            .await?,
        acyclic_harness::core::ApplyResult::Replayed { .. }
    ));
    assert!(matches!(
        aggregate
            .execute(command(
                3,
                2,
                Action::ResolveEffect {
                    observation: attestation
                }
            ))
            .await?,
        acyclic_harness::core::ApplyResult::Replayed { .. }
    ));
    assert_eq!(aggregate.reducer().revision(), revision);
    ordinal += 1;
    let reuse = fresh(ordinal, revision, plan(effect_id, false));
    assert!(aggregate.reducer().plan(&reuse).is_err());
    assert!(matches!(
        aggregate.execute(reuse).await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    assert_eq!(aggregate.reducer().revision(), revision);
    assert_eq!(
        stream.stream(authority.stream_path()?)?.tail().await?,
        revision
    );
    assert_eq!(aggregate.effect(effect_id, limits).await?, Some(completed));
    Ok(())
}

async fn copied_effect_history(
    authority: &Authority,
    canonical: &[acyclic_stream::Record],
    indexes: &[acyclic_stream::Record],
    atomic: bool,
) -> Result<StreamClient<MemoryStream>> {
    use acyclic_stream::{
        CommitCondition, CommitMutation, CommitOutcome, CommitRequest, StreamPath,
    };
    let client = StreamClient::new(Arc::new(MemoryStream::default()));
    let canonical_path = StreamPath::new(authority.stream_path()?)?;
    let location: serde_json::Value = serde_json::from_slice(
        &indexes
            .first()
            .ok_or_else(|| acyclic_harness::Error::Invalid("index control missing".into()))?
            .value,
    )
    .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let effect: EffectId = serde_json::from_value(
        location
            .get("effect_id")
            .ok_or_else(|| {
                acyclic_harness::Error::Invalid("copied effect identity missing".into())
            })?
            .clone(),
    )
    .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let index_path = StreamPath::new(format!(
        "harness/v2/effect-transitions/tasks/effect-task/{effect}"
    ))?;
    if atomic {
        for (position, (event, index)) in canonical.iter().zip(indexes).enumerate() {
            let expected = position as u64;
            let condition = |path: StreamPath| {
                if expected == 0 {
                    CommitCondition::Absent { path }
                } else {
                    CommitCondition::Tail { path, expected }
                }
            };
            let result = client
                .commit(CommitRequest {
                    conditions: vec![
                        condition(canonical_path.clone()),
                        condition(index_path.clone()),
                    ],
                    mutations: vec![
                        CommitMutation::Append {
                            path: canonical_path.clone(),
                            records: vec![event.value.clone()],
                        },
                        CommitMutation::Append {
                            path: index_path.clone(),
                            records: vec![index.value.clone()],
                        },
                    ],
                    idempotency_key: acyclic_stream::IdempotencyKey::new(format!(
                        "copy-effect-{position}"
                    ))?,
                })
                .await?;
            assert!(matches!(result, CommitOutcome::Committed(_)));
        }
    } else {
        client
            .stream(canonical_path.as_str())?
            .append_batch(
                canonical
                    .iter()
                    .map(|record| record.value.clone())
                    .collect(),
                Some(0),
                None,
            )
            .await?;
        client
            .stream(index_path.as_str())?
            .append_batch(
                indexes.iter().map(|record| record.value.clone()).collect(),
                Some(0),
                None,
            )
            .await?;
    }
    Ok(client)
}
