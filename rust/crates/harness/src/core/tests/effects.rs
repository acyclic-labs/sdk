//! tests effects semantics.
use super::*;
#[test]
fn effect_guarantees_and_attestations_control_retry() -> Result<()> {
    let effect_id = EffectId::from_bytes([9; 16]);
    let first_attempt = EffectAttemptId::from_bytes([10; 16]);
    let retry_attempt = EffectAttemptId::from_bytes([11; 16]);
    for (guarantee, request, result_schema) in [
        (
            EffectGuarantee::IdempotentRetry,
            br#"{"value":1}"#.as_slice(),
            json!({"type": "object", "required": ["receipt"]}),
        ),
        (EffectGuarantee::AtMostOnce, b"null".as_slice(), json!({})),
        (
            EffectGuarantee::ExactlyOnce,
            br#"{"value":1}"#.as_slice(),
            json!({"type": "string"}),
        ),
    ] {
        let mut reducer = reducer_fixture();
        reducer.apply(command(
            operation(1),
            0,
            Action::PlanEffect {
                effect_id,
                provider: "example.provider".into(),
                guarantee,
                effect_kind: "example.send".into(),
                request: request_file(request)?,
                result_schema: schema_file(&result_schema)?,
            },
        ))?;
        assert!(
            !serde_json::to_string(&reducer.snapshot(usize::MAX, &mut None)?.0)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .contains("\\\"value\\\"")
        );
        reducer.apply(command(
            operation(2),
            1,
            Action::MarkEffectDispatched {
                effect_id,
                attempt_id: first_attempt,
            },
        ))?;
        if guarantee == EffectGuarantee::ExactlyOnce {
            let effect = reducer
                .effect(effect_id)
                .ok_or_else(|| Error::NotFound("effect".into()))?;
            let invalid_result = issuer().attest_effect(
                effect_id,
                effect,
                first_attempt,
                EffectStatus::Succeeded {
                    result: FileRef::new(
                        request_file(b"1")?.volume().clone(),
                        "results/invalid.txt".into(),
                        "generation-1".into(),
                        crate::conversation::FileDescriptor::from_bytes(
                            b"1",
                            "text/plain",
                            &mut None,
                        )?,
                        "invalid.txt".into(),
                        &mut None,
                    )?,
                },
                &mut None,
            )?;
            assert!(matches!(
                reducer.apply(command(
                    operation(32),
                    2,
                    Action::ResolveEffect {
                        observation: invalid_result
                    }
                )),
                Err(Error::Invalid(_))
            ));
            let mut forged = issuer().attest_effect(
                effect_id,
                reducer
                    .effect(effect_id)
                    .ok_or_else(|| Error::NotFound("effect".into()))?,
                first_attempt,
                EffectStatus::Indeterminate,
                &mut None,
            )?;
            forged.provider = "attacker".into();
            assert!(matches!(
                reducer.apply(command(
                    operation(33),
                    2,
                    Action::ResolveEffect {
                        observation: forged
                    }
                )),
                Err(Error::Unauthorized(_))
            ));
        }
        let indeterminate = issuer().attest_effect(
            effect_id,
            reducer
                .effect(effect_id)
                .ok_or_else(|| Error::NotFound("effect".into()))?,
            first_attempt,
            EffectStatus::Indeterminate,
            &mut None,
        )?;
        reducer.apply(command(
            operation(3),
            2,
            Action::ResolveEffect {
                observation: indeterminate,
            },
        ))?;
        assert_eq!(
            reducer.effect(effect_id).map(|effect| &effect.status),
            Some(&EffectStatus::Indeterminate)
        );
        let retry = reducer.apply(command(
            operation(4),
            3,
            Action::MarkEffectDispatched {
                effect_id,
                attempt_id: retry_attempt,
            },
        ));
        if guarantee == EffectGuarantee::IdempotentRetry {
            retry?;
            let succeeded = issuer().attest_effect(
                effect_id,
                reducer
                    .effect(effect_id)
                    .ok_or_else(|| Error::NotFound("effect".into()))?,
                retry_attempt,
                EffectStatus::Succeeded {
                    result: request_file(br#"{"receipt":"confirmed"}"#)?,
                },
                &mut None,
            )?;
            reducer.apply(command(
                operation(5),
                4,
                Action::ResolveEffect {
                    observation: succeeded,
                },
            ))?;
            assert!(matches!(
                reducer.effect(effect_id).map(|effect| &effect.status),
                Some(EffectStatus::Succeeded { .. })
            ));
            assert_eq!(
                reducer
                    .effect(effect_id)
                    .map(|effect| effect.attempts.len()),
                Some(2)
            );
        } else {
            assert!(matches!(retry, Err(Error::Conflict(_))));
        }
    }
    Ok(())
}
