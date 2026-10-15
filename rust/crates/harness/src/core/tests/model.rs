//! Atomic admission, replay, and checkpoint equivalence.
use super::*;

#[test]
fn fixed_lifecycle_rejects_terminal_reopening() -> Result<()> {
    let owner = AuthorityIssuer::new(
        "test",
        [7; 32],
        Authority {
            kind: AggregateKind::Turn,
            id: "turn-1".into(),
        },
    );
    let mut reducer = Reducer::new(
        owner.audience.clone(),
        owner.verifier(&mut None)?,
        schemas(),
    );
    let transition = |id, revision, to| {
        let mut command = command(
            operation(id),
            revision,
            Action::TransitionLifecycle { to, reason: None },
        );
        command.scope = owner
            .issue(
                "turn",
                &Capabilities::new(["lifecycle:manage"]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof");
        command
    };
    reducer.apply(&transition(40, 0, LifecycleState::Active))?;
    reducer.apply(&transition(41, 1, LifecycleState::Completed))?;
    assert!(matches!(
        reducer.apply(&transition(42, 2, LifecycleState::Active)),
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[test]
fn malformed_admission_metadata_is_rejected() -> Result<()> {
    let mut reducer = reducer_fixture();
    let action = Action::AppendCustom {
        schema: "example.message".into(),
        version: 1,
        content: request_file(br#"{"text":"hello"}"#)?,
    };
    let before = reducer.copy(&mut None)?;
    let mut empty_key = command(operation(30), 0, action.clone());
    empty_key.idempotency_key = IdempotencyKey(String::new());
    assert!(matches!(reducer.apply(&empty_key), Err(Error::Invalid(_))));
    let mut future_cause = command(operation(31), 0, action.clone());
    future_cause.causal_parent = Some(EventReference {
        authority: reducer.authority().clone(),
        revision: 1,
    });
    assert!(matches!(
        reducer.apply(&future_cause),
        Err(Error::Invalid(_))
    ));
    assert_eq!(reducer, before);
    let accepted = command(operation(32), 0, action);
    reducer.apply(&accepted)?;
    assert!(matches!(
        reducer.apply(&accepted)?,
        ApplyResult::Replayed { .. }
    ));
    let mut conflict = accepted;
    conflict.idempotency_key = IdempotencyKey("another intent".into());
    assert!(matches!(reducer.apply(&conflict), Err(Error::Conflict(_))));
    assert_eq!(restore(&reducer, &issuer())?, reducer);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_component_model_transition_work_grows_linearly_with_bounded_residency() -> Result<()>
{
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let budget = acyclic_fs::WorkBudget {
        allocation_operations: 4096,
        peak_allocation_bytes: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        source_bytes_read: 16_777_216,
        items_examined: 16_777_216,
        ..acyclic_fs::WorkBudget::default()
    };
    use crate::store::{StreamAggregate, tests::MigrationContentStore};
    use std::sync::{Arc, Mutex};
    let owner = issuer();
    let primed_bytes = br#"{"text":"original primed entry"}"#;
    let request_bytes = br#"{"prompt":"actual provider fixture request"}"#;
    let schema_bytes = br#"{"type":"object"}"#;
    let custom_bytes = br#"{"text":"steady active extension state"}"#;
    let primed = request_file(primed_bytes)?;
    let request = request_file(request_bytes)?;
    let schema = schema_file(&json!({"type":"object"}))?;
    let custom = request_file(custom_bytes)?;
    let content = MigrationContentStore(Arc::new(Mutex::new(
        [
            (&primed, primed_bytes.as_slice()),
            (&request, request_bytes.as_slice()),
            (&schema, schema_bytes.as_slice()),
            (&custom, custom_bytes.as_slice()),
        ]
        .into_iter()
        .map(|(file, bytes)| Ok((MigrationContentStore::key(file)?, bytes.to_vec())))
        .collect::<Result<_>>()?,
    )));
    let client = Arc::new(acyclic_stream::MemoryStream::default());
    let mut reference = StreamAggregate::open(
        &client,
        (
            owner.audience.clone(),
            owner.verifier(&mut None)?,
            schemas(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        2,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(Arc::new(content));
    reference.set_resident_event_limit(4)?;
    reference
        .execute(
            command(
                operation(1),
                0,
                Action::AppendCustom {
                    schema: "example.message".into(),
                    version: 1,
                    content: primed,
                },
            ),
            None,
            &mut None,
        )
        .await?;
    let mut state = reference.reducer().copy(&mut None)?;
    let available =
        budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
    const N: u64 = 8;
    const WARMUP: u64 = 16;
    const TRANSITIONS: u64 = WARMUP + 10 * N;
    let mut recent = VecDeque::new();
    let mut total = acyclic_fs::WorkCounters::default();
    let mut input_bytes = 0_u64;
    let mut small = None;
    for index in 0_u64..TRANSITIONS {
        let mut id = [0_u8; 16];
        id[..8].copy_from_slice(&(index / 4 + 1).to_le_bytes());
        let effect_id = EffectId::from_bytes(id);
        let attempt_id = EffectAttemptId::from_bytes(id);
        let action = match index % 4 {
            0 => Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: custom.clone(),
            },
            1 => Action::PlanEffect {
                effect_id,
                provider: "example.provider".into(),
                guarantee: EffectGuarantee::AtMostOnce,
                effect_kind: "example.model_output".into(),
                request: request.clone(),
                result_schema: schema.clone(),
            },
            2 => Action::MarkEffectDispatched {
                effect_id,
                attempt_id,
            },
            _ => Action::ResolveEffect {
                observation: owner.attest_effect(
                    effect_id,
                    reference
                        .reducer()
                        .effect(effect_id)
                        .expect("admitted effect"),
                    attempt_id,
                    EffectStatus::Failed {
                        message: "actual attested provider rejection".into(),
                    },
                    &mut None,
                )?,
            },
        };
        let mut operation = [0_u8; 16];
        operation[..8].copy_from_slice(&(reference.reducer().revision() + 1).to_le_bytes());
        let ApplyResult::Applied { event } = reference
            .execute(
                command(
                    OperationId::from_bytes(operation),
                    reference.reducer().revision(),
                    action,
                ),
                None,
                &mut None,
            )
            .await?
        else {
            panic!("fresh model event replayed")
        };
        if index >= WARMUP {
            input_bytes += serde_json::to_vec(&(
                &owner.audience,
                event.revision,
                event.operation_id,
                event.intent_digest,
                &event.scope,
                &event.causal_parent,
                &event.payload,
            ))
            .expect("signed input")
            .len() as u64;
        }
        let query = NativeMaterializationWork::new(budget, &CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = crate::filesystem::PreparationIo::from_native_work(&query)?;
        let ApplyResult::Applied { event } =
            state.apply_committed(event, &mut Some(&mut io.history_io()))?
        else {
            panic!("fresh model application replayed")
        };
        state.mark_indexed_terminal_effect_with_work(&event, &mut Some(&mut io.history_io()))?;
        drop(io);
        let receipt = query.receipt();
        if index >= WARMUP {
            total = total.checked_add(receipt).expect("work counters fit");
        }
        if index + 1 == WARMUP + N {
            small = Some((
                serde_json::to_value(total).expect("work counters"),
                input_bytes,
            ));
        }
        assert_eq!(&state, reference.reducer(), "transition {index}");
        assert!(state.events.len() <= 4);
        assert!(
            state.projection.effects.len() <= 3
                && state.projection.indexed_terminal_effects.len() <= 2
        );
        assert_eq!(state.projection.extension_states.len(), 1);
        recent.push_back((index, query));
        if recent.len() > 16 {
            let (index, retired) = recent.pop_front().expect("observation window");
            if index >= 64 {
                retired.admit_retained(available).map_err(Error::from)?;
            }
        }
    }
    let (small, small_bytes) = small.expect("small workload");
    let input_bound = input_bytes.max(10 * small_bytes);
    let large = serde_json::to_value(total).expect("work counters");
    for (counter, value) in small.as_object().expect("work counters") {
        assert!(
            u128::from(
                large
                    .get(counter)
                    .and_then(serde_json::Value::as_u64)
                    .expect("counter")
            ) * u128::from(small_bytes)
                <= u128::from(value.as_u64().expect("counter")) * u128::from(input_bound),
            "{counter} grows faster than the signed input: {small:?} -> {large:?}"
        );
    }
    assert_eq!(state.revision(), TRANSITIONS + 1);
    let mut oldest = [0_u8; 16];
    oldest[..8].copy_from_slice(&1_u64.to_le_bytes());
    assert!(state.effect(EffectId::from_bytes(oldest)).is_none());
    drop(state);
    for (_, work) in recent {
        work.admit_retained(available).map_err(Error::from)?;
    }
    Ok(())
}
