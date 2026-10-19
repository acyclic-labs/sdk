use super::*;
use proptest::prelude::*;
use serde_json::json;

proptest! {
    // Constructing and selecting borrowed input never copies a tool payload.
    #[test]
    fn selecting_borrowed_input_allocates_nothing(text in ".{1,128}", post in any::<bool>()) {
        let invocation = ToolInvocation { operation_id:crate::OperationId::from_bytes([1;16]),
            name:"custom".into(), call_id:"one".into(), arguments:json!({"text":text}) };
        let response = ToolResult { value:json!({"result":text}) };
        let event = if post { HookEvent::PostToolUse } else { HookEvent::PreToolUse };
        #[cfg(not(target_arch = "wasm32"))]
        for extra_copy in [false, true] {
            let measured = acyclic_test_support::measure(|| {
                let input = HookInput::Tool { invocation:&invocation,
                    response:post.then_some(&response), session_id:None };
                std::hint::black_box(input.subject(event));
                if extra_copy { std::hint::black_box(invocation.arguments.clone()); }
            });
            prop_assert_eq!((measured.count_total,measured.bytes_total) == (0,0), !extra_copy);
        }
    }

    // Property: stdin preserves the admitted arguments, result and identities
    // exactly. Authoritative event/cwd fields override caller-supplied values.
    #[test]
    fn borrowed_stdin_round_trips_exact_tool_fields(
        text in ".{0,128}", number in any::<i64>(), post in any::<bool>(),
        identity in any::<[u8;16]>(),
    ) {
        let invocation = ToolInvocation {
            operation_id: crate::OperationId::from_bytes(identity),
            call_id: "call-1".into(), name: "custom".into(),
            arguments: json!({"text":text,"number":number}),
        };
        let response = ToolResult { value: json!({"returned":number,"text":text}) };
        let event = if post { HookEvent::PostToolUse } else { HookEvent::PreToolUse };
        let task = crate::TaskId::from_bytes(identity);
        let input = HookInput::Tool { invocation: &invocation, response: post.then_some(&response), session_id: Some(task) };
        let bytes = input.encode(event, Path::new("workspace"))?;
        let decoded: Value = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(&decoded["tool_input"], &invocation.arguments);
        prop_assert_eq!(&decoded["tool_name"], &invocation.name);
        prop_assert_eq!(&decoded["tool_use_id"], &invocation.call_id);
        prop_assert_eq!(&decoded["session_id"], &json!(task));
        prop_assert_eq!(&decoded["hook_event_name"], event.name());
        prop_assert_eq!(&decoded["cwd"], "workspace");
        prop_assert_eq!(decoded.get("tool_response"), post.then_some(&response.value));
        prop_assert_eq!(input.subject(event), "custom");
        let supplied = json!({"cwd":"forged", "hook_event_name":"Stop", "text":text, "number":number});
        let bytes = HookInput::Fields(&supplied).encode(event, Path::new("workspace"))?;
        let decoded: Value = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(&decoded["text"], &supplied["text"]);
        prop_assert_eq!(&decoded["number"], &supplied["number"]);
        prop_assert_eq!(&decoded["hook_event_name"], event.name());
        prop_assert_eq!(&decoded["cwd"], "workspace");
    }
}

#[test]
fn non_object_fields_are_rejected_before_dispatch() {
    for input in [Value::Null, json!(false), json!(["not fields"]), json!(1)] {
        assert!(
            HookInput::Fields(&input)
                .encode(HookEvent::Stop, Path::new("."))
                .is_err()
        );
    }
}
