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
    fn dispatch<'a>(&'a self, request: EffectDispatch) -> BoxFuture<'a, Result<EffectObservation>> {
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
    let mut registry = EffectRegistry::default().with_result_resolver(verifier);
    registry.register(Arc::new(ResultProvider(result)))?;
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
                observation: attestation,
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
