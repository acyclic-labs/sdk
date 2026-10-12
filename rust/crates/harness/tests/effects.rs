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
use acyclic_stream::{MemoryStream};
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
    let mut files = Vec::new();
    for (path, bytes, mime, name, key) in [
        ("effects/request.json", br#"{"secret":"request-body"}"#.as_slice(), "application/json", "request.json", "request-upload"),
        ("effects/result.json", br#"{"receipt":"result-body"}"#.as_slice(), "application/json", "result.json", "result-upload"),
        ("effects/result-schema.json", br#"{"type":"object","required":["receipt"]}"#.as_slice(), "application/schema+json", "result-schema.json", "schema-upload"),
        ("effects/invalid-schema.json", br#"{"type":17}"#.as_slice(), "application/schema+json", "invalid-schema.json", "invalid-schema-upload"),
    ] {
        files.push(host.put_content(&volume, &write, path, bytes, mime, name, 4_096, &IdempotencyKey::new(key)?).await?);
    }
    let [request, result, schema, invalid_schema]: [FileRef; 4] = files.try_into().unwrap();
    let verifier = Arc::new(FilesystemContentVerifier::new(
        host,
        issuer.verifier(),
        scope.clone(),
        4_096,
    )?);
    let provider = Arc::new(test_stream::LostSessionAck::<MemoryStream>::default());
    let stream = provider.clone();
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
            ), #[cfg(feature = "filesystem")] None, &mut None)
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
        ), #[cfg(feature = "filesystem")] None, &mut None)
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
        ), #[cfg(feature = "filesystem")] None, &mut None)
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
        ), #[cfg(feature = "filesystem")] None, &mut None)
        .await?;
    assert!(matches!(
        aggregate
            .reducer()
            .effect(effect_id)
            .map(|state| &state.status),
        Some(EffectStatus::Succeeded { .. })
    ));
    let raw = acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), authority.stream_path()?)
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .read(0, 4)
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert_eq!(raw.len(), 3);
    for record in &raw {
        for body in [b"request-body".as_slice(), b"result-body".as_slice(), b"required".as_slice()] {
            assert!(!record.value.windows(body.len()).any(|window| window == body));
        }
    }
    use std::sync::atomic::Ordering;
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
                }, #[cfg(feature = "filesystem")] None, &mut None)
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
    for fault in 1..=4 {
        provider.history_read_fault.store(fault, Ordering::SeqCst);
        assert!(reader.effect(&cursor, effect_id, limits).await.is_err());
        assert_eq!(
            reader.effect(&cursor, effect_id, limits).await?,
            Some(completed.clone())
        );
    }
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
        aggregate.execute(fresh(ordinal, aggregate.reducer().revision(), plan(id, position == 2)), #[cfg(feature = "filesystem")] None, &mut None).await?;
        if position > 0 {
            ordinal += 1;
            aggregate.execute(fresh(ordinal, aggregate.reducer().revision(), Action::MarkEffectDispatched {
                effect_id: id, attempt_id: EffectAttemptId::from_bytes(id.into_bytes()),
            }), #[cfg(feature = "filesystem")] None, &mut None).await?;
        }
        if position == 2 {
            let state = aggregate.reducer().effect(id).unwrap();
            let observation = registry.dispatch_and_attest(&issuer, id, state, EffectDispatch::from_state(id, state)?).await?;
            ordinal += 1;
            aggregate.execute(fresh(ordinal, aggregate.reducer().revision(), Action::ResolveEffect { observation }), #[cfg(feature = "filesystem")] None, &mut None).await?;
        }
    }
    let retirement_limits = HistoryReadLimits { maximum_events: 3, maximum_bytes: 1_000_000 };
    for retained in 1..=1_000_u128 {
        let id = EffectId::from_bytes(uuid::Uuid::from_u128(retained + 10_000_000).into_bytes());
        complete(&mut aggregate, &registry, &issuer, id, plan(id, false), &fresh, &mut ordinal).await?;
        if matches!(retained, 1 | 100 | 1_000) {
            provider.forbid_writes.store(true, Ordering::SeqCst);
            let cold = StreamAggregate::open_with_projection_limits(
                &stream, authority.clone(), issuer.verifier(), SchemaRegistry::new(),
                acyclic_harness::store::default_projection_read_limits(), 2,
            ).await?.with_content_verifier(verifier.clone());
            assert_eq!(cold.reducer().revision(), aggregate.reducer().revision());
            assert_eq!(cold.reducer().resident_terminal_effect_count(), 2);
            assert_eq!(cold.reducer().resident_effect_count(), 5);
            for (id, expected) in active.into_iter().zip([
                EffectStatus::Planned, EffectStatus::Dispatched, EffectStatus::Indeterminate,
            ]) {
                assert_eq!(cold.reducer().effect(id).map(|state| &state.status), Some(&expected));
            }
            assert_eq!(cold.effect(effect_id, retirement_limits).await?, Some(completed.clone()));
            if retained > 1 { assert!(cold.reducer().effect(effect_id).is_none()); }
            aggregate = cold;
            provider.forbid_writes.store(false, Ordering::SeqCst);
        }
    }
    let worst_id = EffectId::from_bytes(uuid::Uuid::from_u128(u128::MAX).into_bytes());
    let mut worst_completed = None;
    for bit in 0..=128_u32 {
        let bits = if bit == 0 { u128::MAX } else { u128::MAX ^ (1_u128 << (bit - 1)) };
        let id = EffectId::from_bytes(uuid::Uuid::from_u128(bits).into_bytes());
        let state = complete(&mut aggregate, &registry, &issuer, id, plan(id, false), &fresh, &mut ordinal).await?;
        if bit == 0 { worst_completed = Some(state); }
        assert_eq!(aggregate.reducer().resident_effect_count(), 5);
    }
    let worst_reader = HistoryReader::new(&stream, &authority, issuer.verifier())?;
    let worst_cut = worst_reader.pin(0).await?;
    provider.forbid_writes.store(true, Ordering::SeqCst);
    assert_eq!(worst_reader.effect(&worst_cut, worst_id, retirement_limits).await?, worst_completed);
    provider.forbid_writes.store(false, Ordering::SeqCst);
    ordinal += 1;
    aggregate.execute(fresh(ordinal, aggregate.reducer().revision(), Action::TransitionLifecycle {
        to: LifecycleState::Waiting, reason: None,
    }), #[cfg(feature = "filesystem")] None, &mut None).await?;
    provider.forbid_writes.store(true, Ordering::SeqCst);
    assert_eq!(worst_reader.effect(&worst_cut, worst_id, retirement_limits).await?, worst_completed);
    Ok(())
}

async fn complete<P: acyclic_stream::StreamProvider>(
    aggregate: &mut StreamAggregate<P>, registry: &EffectRegistry, issuer: &AuthorityIssuer,
    id: EffectId, plan: Action, command: &impl Fn(u128, u64, Action) -> Command, ordinal: &mut u128,
) -> Result<acyclic_harness::core::EffectState> {
    for action in [plan, Action::MarkEffectDispatched {
        effect_id: id, attempt_id: EffectAttemptId::from_bytes(id.into_bytes()),
    }] {
        *ordinal += 1;
        aggregate.execute(command(*ordinal, aggregate.reducer().revision(), action), #[cfg(feature = "filesystem")] None, &mut None).await?;
    }
    let state = aggregate.reducer().effect(id).unwrap();
    let observation = registry.dispatch_and_attest(issuer, id, state, EffectDispatch::from_state(id, state)?).await?;
    *ordinal += 1;
    aggregate.execute(command(*ordinal, aggregate.reducer().revision(), Action::ResolveEffect { observation }), #[cfg(feature = "filesystem")] None, &mut None).await?;
    Ok(aggregate.reducer().effect(id).unwrap().clone())
}
