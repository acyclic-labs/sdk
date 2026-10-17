use super::*;
use proptest::prelude::*;

#[test]
fn native_result_envelope_covers_a_near_limit_escaped_value() -> Result<()> {
    let outcome =
        json!({"Succeeded":{"text":"\\\"".repeat((acyclic_stream::MAX_RECORD_BYTES-512)/4)}});
    let stored = serde_json::to_vec(&outcome).map_err(|e| invalid(&e))?;
    assert!(stored.len() > acyclic_stream::MAX_RECORD_BYTES - 1024);
    assert!(stored.len() < acyclic_stream::MAX_RECORD_BYTES);
    let delivered = serde_json::to_vec(&tool_result(outcome)?).map_err(|e| invalid(&e))?;
    // Canonical data once, escaped text at most twice, plus fixed native fields.
    assert!(delivered.len() <= 3 * acyclic_stream::MAX_RECORD_BYTES + 1024);
    assert!(delivered.len() > acyclic_stream::MAX_RECORD_BYTES);
    Ok(())
}

#[test]
fn malformed_tool_answers_cannot_implicitly_allow_native_execution() -> Result<()> {
    for host in [
        ExternalHost::Codex,
        ExternalHost::ClaudeCode,
        ExternalHost::Pi,
    ] {
        let event = if host == ExternalHost::Pi {
            "tool_call"
        } else {
            "PreToolUse"
        };
        for value in [
            Value::Null,
            json!([]),
            json!(true),
            json!({}),
            json!({"block":"false"}),
            json!({"hookSpecificOutput":{"hookEventName":"PreToolUse"}}),
            json!({"hookSpecificOutput":{"hookEventName":"Other","permissionDecision":"allow"}}),
        ] {
            assert!(
                hook_answer(
                    host,
                    event,
                    &serde_json::to_vec(&value).map_err(|e| invalid(&e))?
                )
                .is_err()
            );
        }
        let denied = hook_failure(host.name(), event, "rejected");
        let bytes = serde_json::to_vec(&denied).map_err(|e| invalid(&e))?;
        assert_eq!(hook_answer(host, event, &bytes)?, denied);
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Property: every successful canonical tool value is delivered verbatim;
    // neither failure nor an unfinished dispatch can become success.
    #[test]
    fn canonical_tool_result_substitution_preserves_the_value(text in ".{0,128}") {
        let value = json!({"text":text});
        let outcome = json!({"Succeeded":value});
        let payload = outcome["Succeeded"]["text"].as_str().unwrap().as_ptr();
        let result = tool_result(outcome)?;
        prop_assert_eq!(result["details"]["text"].as_str().unwrap().as_ptr(),payload);
        prop_assert_eq!(result["details"].clone(),value.clone());
        prop_assert_eq!(serde_json::from_str::<Value>(result["content"][0]["text"].as_str().unwrap())?,value);
        prop_assert_eq!(result["isError"].as_bool(),Some(false));
        prop_assert!(tool_result(json!({"Cancelled":null})).is_err(), "cancelled results must fail");
        prop_assert!(tool_result(json!({})).is_err(), "missing outcomes must fail");
    }

    // Property: a durable failure is never interpreted as native permission or
    // a successful lifecycle acknowledgement, for any catalogued host event.
    #[test]
    fn failed_results_reject_every_native_hook(message in ".{0,64}") {
        let answer = serde_json::to_vec(&acyclic_harness::Outcome::<Value>::Failed {message})?;
        for host in [ExternalHost::Codex, ExternalHost::ClaudeCode, ExternalHost::Pi] {
            for spec in hooks(host) {
                prop_assert!(hook_answer(host, spec.event, &answer).is_err());
            }
        }
    }

    // Property: explicit native decisions and their rewritten inputs round trip
    // exactly; deleting the decision always rejects the answer.
    #[test]
    fn native_decision_round_trip_never_defaults_to_allow(
        host in prop_oneof![Just(ExternalHost::Codex), Just(ExternalHost::ClaudeCode), Just(ExternalHost::Pi)],
        permit in any::<bool>(), reason in ".{0,64}", command in ".{0,64}",
    ) {
        let (event, mut answer) = if host == ExternalHost::Pi {
            ("tool_call", json!({"block":!permit,"reason":reason,"updatedInput":{"command":command}}))
        } else {
            ("PreToolUse", json!({"hookSpecificOutput":{"hookEventName":"PreToolUse",
                "permissionDecision":if permit {"allow"} else {"deny"},
                "permissionDecisionReason":reason,"updatedInput":{"command":command}}}))
        };
        prop_assert_eq!(hook_answer(host, event, &serde_json::to_vec(&answer)?)?, answer.clone());
        let target = if host == ExternalHost::Pi {
            answer.as_object_mut()
        } else {
            answer.get_mut("hookSpecificOutput").and_then(Value::as_object_mut)
        }.ok_or_else(|| Error::Invalid("missing fixture object".into()))?;
        target.remove(if host == ExternalHost::Pi {"block"} else {"permissionDecision"});
        prop_assert!(hook_answer(host, event, &serde_json::to_vec(&answer)?).is_err());
    }
}
