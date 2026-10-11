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
    use std::sync::atomic::Ordering;
    let effect_path = format!("harness/v2/effect-transitions/tasks/effect-task/{effect_id}");
    let indexes = stream
        .stream(&effect_path)?
        .read(0, 3)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(indexes.len(), 3);
    let legacy_bytes = canonical_bytes
        + indexes
            .iter()
            .map(|record| record.value.len() as u64)
            .sum::<u64>();
    let probe_reader = HistoryReader::new(&stream, &authority, issuer.verifier())?;
    provider.observation_bytes.store(0, Ordering::SeqCst);
    probe_reader
        .effect(
            &probe_reader.pin(0).await?,
            effect_id,
            HistoryReadLimits {
                maximum_events: 3,
                maximum_bytes: 1_000_000,
            },
        )
        .await?;
    let bytes = provider.observation_bytes.load(Ordering::SeqCst);
    assert!(
        bytes > legacy_bytes,
        "complete cut proof must be included in byte accounting"
    );
    let limits = HistoryReadLimits {
        maximum_events: 3,
        maximum_bytes: bytes,
    };
    let completed = aggregate
        .reducer()
        .effect(effect_id)
        .cloned()
        .ok_or_else(|| acyclic_harness::Error::Invalid("completed effect missing".into()))?;
    let original_cut = probe_reader.pin(0).await?;
    provider.forbid_writes.store(true, Ordering::SeqCst);
    provider.hide_effect_node_read.store(true, Ordering::SeqCst);
    assert!(
        probe_reader
            .effect(&original_cut, effect_id, limits)
            .await
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
            .effect(&original_cut, effect_id, limits)
            .await
            .is_err()
    );
    provider
        .hide_effect_transition_position
        .store(0, Ordering::SeqCst);
    assert_eq!(
        probe_reader
            .effect(&original_cut, effect_id, limits)
            .await?,
        Some(completed.clone())
    );
    provider.forbid_writes.store(false, Ordering::SeqCst);
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
            provider.observation_tails.store(0, Ordering::SeqCst);
            let cold = HistoryReader::new(&stream, &authority, issuer.verifier())?;
            let cursor = cold.pin(0).await?;
            assert_eq!(
                cold.effect(&cursor, effect_id, limits).await?,
                Some(completed.clone())
            );
            
        }
    }
    let reader = HistoryReader::new(&stream, &authority, issuer.verifier())?;
    let cursor = reader.pin(0).await?;
    for (cut, expected) in [(1, planned), (2, dispatched), (3, completed.clone())] {
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
            )
            .await?;
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
                    exact
                )
                .await?,
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
                    }
                )
                .await
                .is_err()
        );
    }
    provider.observation_bytes.store(0, Ordering::SeqCst);
    reader
        .effect(
            &cursor,
            effect_id,
            HistoryReadLimits {
                maximum_events: 3,
                maximum_bytes: 1_000_000,
            },
        )
        .await?;
    let bytes = provider.observation_bytes.load(Ordering::SeqCst);
    let limits = HistoryReadLimits {
        maximum_events: 3,
        maximum_bytes: bytes,
    };
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
    
    for fault in 1..=4 {
        provider.history_read_fault.store(fault, Ordering::SeqCst);
        assert!(reader.effect(&cursor, effect_id, limits).await.is_err());
        assert_eq!(
            reader.effect(&cursor, effect_id, limits).await?,
            Some(completed.clone())
        );
    }
    assert_eq!(
        reader
            .effect(&cursor, EffectId::from_bytes([99; 16]), limits)
            .await?,
        None
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
        let gap = if position == 1 {
            let planned = aggregate
                .reducer()
                .effect(id)
                .cloned()
                .ok_or_else(|| acyclic_harness::Error::Invalid("gap effect missing".into()))?;
            ordinal += 1;
            aggregate
                .execute(fresh(
                    ordinal,
                    aggregate.reducer().revision(),
                    Action::TransitionLifecycle {
                        to: LifecycleState::Active,
                        reason: None,
                    },
                ))
                .await?;
            provider.forbid_writes.store(true, Ordering::SeqCst);
            let reader = HistoryReader::new(&stream, &authority, issuer.verifier())?;
            let cursor = reader.pin(0).await?;
            provider.observation_bytes.store(0, Ordering::SeqCst);
            reader
                .effect(
                    &cursor,
                    id,
                    HistoryReadLimits {
                        maximum_events: 1,
                        maximum_bytes: 1_000_000,
                    },
                )
                .await?;
            let exact = HistoryReadLimits {
                maximum_events: 1,
                maximum_bytes: provider.observation_bytes.load(Ordering::SeqCst),
            };
            assert_eq!(
                reader.effect(&cursor, id, exact).await?,
                Some(planned.clone())
            );
            provider.forbid_writes.store(false, Ordering::SeqCst);
            Some((cursor, exact, planned))
        } else {
            None
        };
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
        if let Some((cursor, exact, planned)) = gap {
            provider.forbid_writes.store(true, Ordering::SeqCst);
            let reader = HistoryReader::new(&stream, &authority, issuer.verifier())?;
            assert_eq!(reader.effect(&cursor, id, exact).await?, Some(planned));
            assert!(
                reader
                    .effect(
                        &cursor,
                        id,
                        HistoryReadLimits {
                            maximum_bytes: exact.maximum_bytes - 1,
                            ..exact
                        }
                    )
                    .await
                    .is_err()
            );
            provider.forbid_writes.store(false, Ordering::SeqCst);
        }
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
    let final_reader = HistoryReader::new(&stream, &authority, issuer.verifier())?;
    let final_cut = final_reader.pin(0).await?;
    provider.observation_bytes.store(0, Ordering::SeqCst);
    assert_eq!(
        final_reader
            .effect(
                &final_cut,
                effect_id,
                HistoryReadLimits {
                    maximum_events: 3,
                    maximum_bytes: 1_000_000
                }
            )
            .await?,
        Some(completed.clone())
    );
    let final_limits = HistoryReadLimits {
        maximum_events: 3,
        maximum_bytes: provider.observation_bytes.load(Ordering::SeqCst),
    };
    assert_eq!(
        aggregate.effect(effect_id, final_limits).await?,
        Some(completed.clone())
    );
    assert!(
        final_reader
            .effect(
                &final_cut,
                effect_id,
                HistoryReadLimits {
                    maximum_bytes: final_limits.maximum_bytes - 1,
                    ..final_limits
                }
            )
            .await
            .is_err()
    );
    let worst_id = EffectId::from_bytes(uuid::Uuid::from_u128(u128::MAX).into_bytes());
    let mut worst_completed = None;
    
    let worst_completed = worst_completed
        .ok_or_else(|| acyclic_harness::Error::Invalid("depth projection missing".into()))?;
    provider.forbid_writes.store(true, Ordering::SeqCst);
    
    let worst_reader = HistoryReader::new(&stream, &authority, issuer.verifier())?;
    let worst_cut = worst_reader.pin(0).await?;
    assert_eq!(
        worst_reader
            .effect(
                &worst_cut,
                worst_id,
                HistoryReadLimits {
                    maximum_events: 3,
                    maximum_bytes: 1_048_576
                }
            )
            .await?,
        Some(worst_completed.clone())
    );

    let worst_limits = HistoryReadLimits {
        maximum_events: 3,
        maximum_bytes: worst_io.read_bytes,
    };
    assert_eq!(
        worst_reader
            .effect(&worst_cut, worst_id, worst_limits)
            .await?,
        Some(worst_completed.clone())
    );
    assert!(
        worst_reader
            .effect(
                &worst_cut,
                worst_id,
                HistoryReadLimits {
                    maximum_bytes: worst_limits.maximum_bytes - 1,
                    ..worst_limits
                }
            )
            .await
            .is_err()
    );
    
    provider.forbid_writes.store(false, Ordering::SeqCst);
    ordinal += 1;
    aggregate
        .execute(fresh(
            ordinal,
            aggregate.reducer().revision(),
            Action::TransitionLifecycle {
                to: LifecycleState::Waiting,
                reason: None,
            },
        ))
        .await?;
    provider.forbid_writes.store(true, Ordering::SeqCst);
    assert_eq!(
        worst_reader
            .effect(&worst_cut, worst_id, worst_limits)
            .await?,
        Some(worst_completed)
    );
    Ok(())
}
