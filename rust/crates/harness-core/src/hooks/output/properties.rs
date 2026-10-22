use super::*;
use proptest::prelude::*;
use serde_json::json;

fn event(index: usize) -> HookEvent {
    HookEvent::ALL[index]
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
    // Every explicit or failure-policy block supplies exactly one moved reason.
    // Ignoring typed failures is the deliberately broken projection.
    #[test]
    fn tool_block_reason_preserves_typed_failure_policy(
        reason in ".{0,96}", explicit in any::<bool>(), blocking in any::<bool>(),
    ) {
        let mut output = HookOutput {
            blocked: explicit.then(|| reason.clone()),
            diagnostics: vec![HookDiagnostic { cause: crate::ProcessError::Timeout {
                exit_code:Some(3), message:reason.clone() }, blocking }],
            ..Default::default()
        };
        let expected = output.blocks();
        let broken = output.blocked.is_some();
        let moved = output.take_block_reason();
        prop_assert_eq!(moved.is_some(), expected);
        if expected { prop_assert_eq!(moved.as_deref(), Some(reason.as_str())); }
        prop_assert_eq!(&output.diagnostics[0].cause,
            &crate::ProcessError::Timeout { exit_code: Some(3), message: reason.clone() });
        if blocking && !explicit { prop_assert_ne!(broken, expected); }
    }

    // Property: retaining a decision as an outcome file preserves every field,
    // including nested replacement arguments, escaped text and empty contexts.
    #[test]
    fn retained_output_round_trip(
        blocked in prop::option::of(".{0,48}"),
        approval in prop::option::of(".{0,48}"),
        context in prop::collection::vec(".{0,48}", 0..5),
        diagnostics in prop::collection::vec(".{0,48}", 0..5),
        updated_input in prop::option::of(
            prop::collection::btree_map("[a-z]{1,8}", json_value(), 0..5)
                .prop_map(|fields| Value::Object(fields.into_iter().collect())),
        ),
    ) {
        let output = HookOutput { blocked, approval, additional_context: context,
            updated_input, diagnostics: diagnostics.into_iter().map(|message| HookDiagnostic { cause: crate::ProcessError::Io { exit_code: None, message }, blocking: false }).collect() };
        let bytes = serde_json::to_vec(&output)?;
        let decoded: HookOutput = serde_json::from_slice(&bytes)?;
        prop_assert_eq!(&decoded, &output);
        prop_assert_eq!(serde_json::to_vec(&decoded)?, bytes);
    }

    // Property: once an earlier hook supplies approval, a block-plus-ask reply
    // moves its denial reason without allocating beyond JSON decoding.
    #[test]
    fn prior_approval_avoids_copying_later_denial_reason(
        first_reason in ".{0,48}",
        reason in ".{1,128}",
    ) {
        let first = serde_json::to_vec(&json!({ "hookSpecificOutput": {
            "permissionDecision": "ask", "permissionDecisionReason": first_reason,
        }})).unwrap();
        let second = serde_json::to_string(&json!({
            "decision": "block",
            "hookSpecificOutput": {
                "permissionDecision": "ask", "permissionDecisionReason": reason,
            },
        })).unwrap();
        let encoded_reason = serde_json::to_string(&reason).unwrap();
        let mut decoded = None;
        let decoding = acyclic_test_support::measure(|| {
            decoded = Some(serde_json::from_str::<String>(&encoded_reason).unwrap());
        });
        let decoded = decoded.unwrap();
        prop_assert_eq!(decoded.as_str(), reason.as_str());
        for extra_copy in [false, true] {
            let mut output = HookOutput::default();
            output.observe(HookEvent::PreToolUse, Some(0), &first, b"").unwrap();
            let mut result = None;
            let merging = acyclic_test_support::measure(|| {
                result = Some(output.observe(HookEvent::PreToolUse, Some(0), second.as_bytes(), b""));
                if extra_copy {
                    // Negative control: restore the unnecessary reason-sized copy.
                    std::hint::black_box(output.blocked.clone());
                }
            });
            result.unwrap().unwrap();
            prop_assert_eq!(output.approval.as_deref(), Some(first_reason.as_str()));
            prop_assert_eq!(output.blocked.as_deref(), Some(reason.as_str()));
            prop_assert!(output.diagnostics.is_empty());
            prop_assert_eq!(
                (merging.count_total, merging.bytes_total) == (decoding.count_total, decoding.bytes_total),
                !extra_copy,
                "allocation detector must reject an extra denial-reason copy"
            );
        }
    }

    // Property: semantically identical JSON encodings have identical decisions,
    // including strings requiring escaping and nested replacement tool arguments.
    #[test]
    fn structured_output_round_trip_preserves_decisions(
        index in 0usize..HookEvent::ALL.len(),
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
        if !continue_run
            || matches!(event, HookEvent::PreToolUse | HookEvent::UserPromptSubmit | HookEvent::Stop)
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
        index in 0usize..HookEvent::ALL.len(),
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
            prop_assert_eq!(output.blocks(), policy == HookFailure::Block && event != HookEvent::Stop);
            prop_assert!(output.approval.is_none());
            prop_assert!(output.updated_input.is_none());
            prop_assert!(output.additional_context.is_empty());
        }
    }

    // Property: later successful, invalid or failed commands never erase the
    // first denial. This is the runner's deterministic config-order merge rule.
    #[test]
    fn first_denial_is_monotonic(
        index in 0usize..HookEvent::ALL.len(),
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
