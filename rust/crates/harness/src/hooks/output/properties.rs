use super::*;
use proptest::prelude::*;
use serde_json::json;

fn event(index: usize) -> HookEvent {
    [
        HookEvent::SessionStart,
        HookEvent::PreToolUse,
        HookEvent::PostToolUse,
        HookEvent::Stop,
    ][index]
}

fn json_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|number| json!(number)),
        ".{0,24}".prop_map(Value::String),
    ]
    .prop_recursive(3, 32, 8, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..5).prop_map(Value::Array),
            prop::collection::btree_map("[a-z]{1,8}", inner, 0..5)
                .prop_map(|fields| Value::Object(fields.into_iter().collect())),
        ]
    })
}

proptest! {
    // Property: semantically identical JSON encodings have identical decisions,
    // including strings requiring escaping and nested replacement tool arguments.
    #[test]
    fn structured_output_round_trip_preserves_decisions(
        index in 0usize..4,
        continue_run in any::<bool>(),
        deny in any::<bool>(),
        permission in 0usize..3,
        reason in ".{0,48}",
        context in ".{0,48}",
        argument in json_value(),
    ) {
        let event = event(index);
        let value = json!({
            "continue": continue_run,
            "stopReason": reason,
            "decision": if deny { "block" } else { "approve" },
            "reason": reason,
            "hookSpecificOutput": {
                "hookEventName": event.name(),
                "permissionDecision": (["allow", "ask", "deny"][permission]),
                "permissionDecisionReason": reason,
                "additionalContext": context,
                "updatedInput": { "argument": argument },
            },
        });
        let encoded = serde_json::to_vec(&value).unwrap();
        let decoded: Value = serde_json::from_slice(&encoded).unwrap();
        prop_assert_eq!(&decoded, &value);
        let pretty = serde_json::to_vec_pretty(&decoded).unwrap();
        let mut compact_output = HookOutput::default();
        let mut pretty_output = HookOutput::default();
        compact_output.observe(event, Some(0), &encoded, b"").unwrap();
        pretty_output.observe(event, Some(0), &pretty, b"").unwrap();
        prop_assert_eq!(&compact_output, &pretty_output);
        prop_assert!(compact_output.diagnostics.is_empty());
        if !continue_run || matches!(event, HookEvent::PreToolUse | HookEvent::Stop)
            && (deny || permission == 2)
        {
            prop_assert_eq!(compact_output.blocked.as_deref(), Some(reason.as_str()));
        } else {
            prop_assert!(compact_output.blocked.is_none());
        }
        prop_assert_eq!(compact_output.approval.as_deref(),
            (event == HookEvent::PreToolUse && permission == 1).then_some(reason.as_str()));
        let mut expected_context = Vec::new();
        if event == HookEvent::PostToolUse && deny {
            expected_context.push(reason);
        }
        expected_context.push(context);
        prop_assert_eq!(compact_output.additional_context, expected_context);
        prop_assert_eq!(
            compact_output.updated_input,
            (event == HookEvent::PreToolUse).then(|| json!({ "argument": argument }))
        );
    }

    // Property: every recognized invalid field rejects before any decision,
    // context or argument mutation; apply then implements the failure policy.
    #[test]
    fn invalid_fields_are_atomic_and_obey_failure_policy(
        index in 0usize..4,
        field in 0usize..11,
        invalid in prop_oneof![Just(Value::Null), any::<bool>().prop_map(Value::Bool), any::<i64>().prop_map(|n| json!(n))],
        reason in ".{0,48}",
    ) {
        let event = event(index);
        let mut value = json!({
            "continue": false, "stopReason": reason,
            "hookSpecificOutput": { "additionalContext": reason },
        });
        match field {
            0..=2 => value[["stopReason", "decision", "reason"][field]] = invalid,
            3 => value["continue"] = json!(reason),
            4 => value["hookSpecificOutput"] = invalid,
            5..=8 => value["hookSpecificOutput"][
                ["hookEventName", "permissionDecision", "permissionDecisionReason", "additionalContext"][field - 5]
            ] = invalid,
            9 => value["hookSpecificOutput"]["updatedInput"] = invalid,
            _ => value = invalid,
        }
        let bytes = serde_json::to_vec(&value).unwrap();
        let mut output = HookOutput::default();
        prop_assert!(output.observe(event, Some(0), &bytes, b"").is_err());
        prop_assert_eq!(&output, &HookOutput::default());
        for policy in [HookFailure::Report, HookFailure::Block] {
            let mut output = HookOutput::default();
            output.apply(event, policy, Ok((Some(0), bytes.clone(), vec![])));
            prop_assert_eq!(output.diagnostics.len(), 1);
            prop_assert_eq!(output.blocked.is_some(), policy == HookFailure::Block && event != HookEvent::Stop);
            prop_assert!(output.approval.is_none());
            prop_assert!(output.updated_input.is_none());
            prop_assert!(output.additional_context.is_empty());
        }
    }

    // Property: later successful, invalid or failed commands never erase the
    // first denial. This is the runner's deterministic config-order merge rule.
    #[test]
    fn first_denial_is_monotonic(
        index in 0usize..4,
        bytes in prop::collection::vec(any::<u8>(), 0..512),
        code in prop::option::of(-3i32..5),
        block_failure in any::<bool>(),
    ) {
        let mut output = HookOutput::default();
        output.apply(HookEvent::PreToolUse, HookFailure::Report,
            Ok((Some(2), vec![], b"first denial".to_vec())));
        output.apply(event(index), if block_failure { HookFailure::Block } else { HookFailure::Report },
            Ok((code, bytes, b"later failure".to_vec())));
        prop_assert_eq!(output.blocked.as_deref(), Some("first denial"));
    }
}
