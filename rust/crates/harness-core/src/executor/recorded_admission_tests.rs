use super::*;
use crate::model::ToolCall;
use proptest::prelude::*;
use serde_json::json;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn optional_policies_preserve_admission_and_scope_authority(
        file_max in proptest::option::of(1u64..128),
        render_max in proptest::option::of(1u64..128),
        child_file_max in proptest::option::of(1u64..128),
        child_render_max in proptest::option::of(1u64..128),
        event_max in proptest::option::of(1usize..12),
        call_max in proptest::option::of(1usize..8),
        message_max in proptest::option::of(1usize..12),
        attachment_max in proptest::option::of(1usize..8),
        path_max in proptest::option::of(1usize..12),
        child_event_max in proptest::option::of(1usize..12),
        child_call_max in proptest::option::of(1usize..8),
        child_message_max in proptest::option::of(1usize..12),
        child_attachment_max in proptest::option::of(1usize..8),
        child_path_max in proptest::option::of(1usize..12),
        input in proptest::collection::vec(0u8..5, 0..24),
    ) {
        let limits = Limits {
            file_bytes: file_max,
            render_bytes: render_max,
            model_events_per_step: event_max.and_then(std::num::NonZeroUsize::new),
            tool_calls_per_step: call_max.and_then(std::num::NonZeroUsize::new),
            context_messages: message_max.and_then(std::num::NonZeroUsize::new),
            attachments: attachment_max.and_then(std::num::NonZeroUsize::new),
            path_bytes: path_max.and_then(std::num::NonZeroUsize::new),
            ..Limits::default()
        };
        let child = Limits {
            file_bytes: child_file_max,
            render_bytes: child_render_max,
            model_events_per_step: child_event_max.and_then(std::num::NonZeroUsize::new),
            tool_calls_per_step: child_call_max.and_then(std::num::NonZeroUsize::new),
            context_messages: child_message_max.and_then(std::num::NonZeroUsize::new),
            attachments: child_attachment_max.and_then(std::num::NonZeroUsize::new),
            path_bytes: child_path_max.and_then(std::num::NonZeroUsize::new),
            ..Limits::default()
        };
        let scope = crate::runtime::RuntimeScope::new(Default::default(), limits).unwrap();
        prop_assert_eq!(scope.narrow(Default::default(), child).is_ok(),
            file_max.is_none_or(|max| child_file_max.is_some_and(|value| value <= max))
            && render_max.is_none_or(|max| child_render_max.is_some_and(|value| value <= max))
            && event_max.is_none_or(|max| child_event_max.is_some_and(|value| value <= max))
            && call_max.is_none_or(|max| child_call_max.is_some_and(|value| value <= max))
            && message_max.is_none_or(|max| child_message_max.is_some_and(|value| value <= max))
            && attachment_max.is_none_or(|max| child_attachment_max.is_some_and(|value| value <= max))
            && path_max.is_none_or(|max| child_path_max.is_some_and(|value| value <= max)));
            {
            prop_assert_eq!(super::transcript::limits_admit(limits, child),
                child_file_max.is_none_or(|max| file_max.is_some_and(|value| value <= max))
                && child_render_max.is_none_or(|max| render_max.is_some_and(|value| value <= max))
                && child_message_max.is_none_or(|max| message_max.is_some_and(|value| value <= max))
                && child_attachment_max.is_none_or(|max| attachment_max.is_some_and(|value| value <= max))
                && child_path_max.is_none_or(|max| path_max.is_some_and(|value| value <= max)));
        }
        let restricted = crate::context::restrict_context_limits(limits, child, None).unwrap();
        for (actual, ancestor, descendant) in [
            (restricted.model_events_per_step, event_max, child_event_max),
            (restricted.tool_calls_per_step, call_max, child_call_max),
            (restricted.context_messages, message_max, child_message_max),
            (restricted.attachments, attachment_max, child_attachment_max),
            (restricted.path_bytes, path_max, child_path_max),
        ] {
            prop_assert_eq!(actual.map(std::num::NonZeroUsize::get),
                ancestor.into_iter().chain(descendant).min());
        }
        for (actual, ancestor, descendant) in [
            (restricted.file_bytes, file_max, child_file_max),
            (restricted.render_bytes, render_max, child_render_max),
        ] { prop_assert_eq!(actual, ancestor.into_iter().chain(descendant).min()); }
        // Isolate count admission from the byte-policy lattice above; finite
        // body admission has independent real-content controls.
        let admission_limits = Limits { file_bytes: None, render_bytes: None, ..limits };
        let mut admission = ModelEventAdmission::default();
        let mut calls = std::collections::BTreeSet::new();
        let mut count = 0;
        let mut completed = false;
        for kind in input {
            let event = match kind {
                0..=2 => ModelEvent::ToolCall(ToolCall::new(format!("call-{kind}"), "echo".into(), Value::Null)),
                3 => ModelEvent::Content { delta: "x".into() },
                _ => ModelEvent::Completed { metadata: Value::Null },
            };
            let accepted = !completed && event_max.is_none_or(|max| count < max)
                && (kind > 2 || (call_max.is_none_or(|max| calls.len() < max) && !calls.contains(&format!("call-{kind}"))));
            prop_assert_eq!(admission.observe(&event, admission_limits, None).is_ok(), accepted);
            if accepted {
                count += 1;
                if kind <= 2 { calls.insert(format!("call-{kind}")); }
                completed |= kind == 4;
            }
            prop_assert_eq!(admission.count, count);
            prop_assert_eq!(admission.completed, completed);
            prop_assert_eq!(admission.calls.iter().collect::<Vec<_>>(), calls.iter().collect::<Vec<_>>());
        }
    }

    #[test]
    fn provisional_admission_is_exact_for_every_authority_identity(
        id in any::<[u8;16]>(), arguments in ".{0,64}", signature in ".{0,64}", offset in 0u64..3,
    ) {
        let invocation = ToolInvocation { operation_id:OperationId::from_bytes(id), call_id:"call".into(), name:"echo".into(), arguments:json!({"payload":arguments}) };
        let metadata = json!({"signature":signature});
        let size = serde_json::to_vec(&crate::tool::RecordedInvocation { invocation:&invocation, metadata:&metadata, target:None, filesystem:None }).unwrap().len() as u64;
        let maximum = size + offset - 1;
        let event = ModelEvent::ToolCall(ToolCall::new(invocation.call_id, invocation.name, invocation.arguments).with_metadata(metadata));
        let limits = Limits { file_bytes: Some(maximum), render_bytes: Some(maximum), ..Limits::default() };
        let mut admission = ModelEventAdmission::default();
        prop_assert_eq!(admission.observe(&event, limits, None).is_ok(), maximum >= size);
    }
}

#[test]
fn individually_bounded_signature_negative_control_cannot_reach_a_tool_handler() {
    let metadata = json!({"signature":"x".repeat(1000)});
    let arguments = json!({});
    let limits = Limits {
        file_bytes: Some(1024),
        render_bytes: Some(1024),
        ..Limits::default()
    };
    assert!(serde_json::to_vec(&metadata).unwrap().len() < limits.file_bytes.unwrap() as usize);
    assert!(serde_json::to_vec(&arguments).unwrap().len() < limits.file_bytes.unwrap() as usize);
    let event = ModelEvent::ToolCall(
        ToolCall::new("call".into(), "echo".into(), arguments).with_metadata(metadata),
    );
    let mut admission = ModelEventAdmission::default();
    assert!(admission.observe(&event, limits, None).is_err());
    assert_eq!(admission.count, 0);
    assert!(admission.calls.is_empty());
}
