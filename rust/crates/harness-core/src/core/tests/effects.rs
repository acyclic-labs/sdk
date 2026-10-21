use super::*;

#[test]
fn effect_guarantees_and_attestations_control_retry() -> Result<()> {
    let effect_id = EffectId::from_bytes([9; 16]);
    let first_attempt = EffectAttemptId::from_bytes([10; 16]);
    let retry_attempt = EffectAttemptId::from_bytes([11; 16]);
    for guarantee in [
        EffectGuarantee::IdempotentRetry,
        EffectGuarantee::AtMostOnce,
        EffectGuarantee::ExactlyOnce,
    ] {
        let mut reducer = reducer_fixture();
        let plan = Action::PlanEffect {
            effect_id,
            provider: "example.provider".into(),
            guarantee,
            effect_kind: "example.send".into(),
            request: request_file(br#"{"value":1}"#)?,
            result_schema: schema_file(&json!({"type":"object","required":["receipt"]}))?,
        };
        apply(&mut reducer, 1, plan)?;
        let dispatch = Action::MarkEffectDispatched {
            effect_id,
            attempt_id: first_attempt,
        };
        apply(&mut reducer, 2, dispatch)?;
        let observe = |reducer: &Reducer, attempt, status| {
            issuer().attest_effect(
                effect_id,
                reducer.effect(effect_id).unwrap(),
                attempt,
                status,
                &mut None,
            )
        };
        if guarantee == EffectGuarantee::ExactlyOnce {
            let invalid = observe(
                &reducer,
                first_attempt,
                EffectStatus::Succeeded {
                    result: schema_file(&json!({}))?,
                },
            )?;
            let action = Action::ResolveEffect {
                observation: invalid,
            };
            assert!(apply(&mut reducer, 32, action).is_err());
            let mut forged = observe(&reducer, first_attempt, EffectStatus::Indeterminate)?;
            forged.provider = "attacker".into();
            let action = Action::ResolveEffect {
                observation: forged,
            };
            assert!(apply(&mut reducer, 33, action).is_err());
        }
        let observation = observe(&reducer, first_attempt, EffectStatus::Indeterminate)?;
        apply(&mut reducer, 3, Action::ResolveEffect { observation })?;
        assert_eq!(
            reducer.effect(effect_id).unwrap().status,
            EffectStatus::Indeterminate
        );
        let dispatch = Action::MarkEffectDispatched {
            effect_id,
            attempt_id: retry_attempt,
        };
        let retry = apply(&mut reducer, 4, dispatch);
        if guarantee == EffectGuarantee::IdempotentRetry {
            retry?;
            let observation = observe(
                &reducer,
                retry_attempt,
                EffectStatus::Succeeded {
                    result: request_file(br#"{"receipt":"confirmed"}"#)?,
                },
            )?;
            apply(&mut reducer, 5, Action::ResolveEffect { observation })?;
            assert!(matches!(
                reducer.effect(effect_id).unwrap().status,
                EffectStatus::Succeeded { .. }
            ));
        } else {
            assert!(retry.is_err());
        }
    }
    Ok(())
}
