//! Tool-call arguments through the real stream against the recording rule.
use crate::{Adapter, stream};
use acyclic_harness::{
    OperationId,
    model::{ModelDispatch, ModelEvent},
};
use bytes::Bytes;
use futures::TryStreamExt as _;

/// The stream's per-event byte limit used by the fuzz target.
const MAXIMUM: usize = 1 << 20;
const PROVIDER_ID: &str = "call";
const NAME: &str = "echo";
const RETAINED_LIMIT: &str = "retained model state exceeds aggregate byte or item limit";

fn chat_event(text: &str) -> serde_json::Value {
    serde_json::json!({"choices":[{"delta":{"tool_calls":[{
        "index":0,"id":PROVIDER_ID,"function":{"name":NAME,"arguments":text}
    }]}}]})
}

fn canonical_id(dispatch: ModelDispatch) -> String {
    format!(
        "{}:{:08x}:{:016x}",
        dispatch.operation_id, dispatch.step, 0u64
    )
}

fn binding_bytes() -> usize {
    // Three map entries and their keys; the provider ID moves from the call
    // into the binding, so its storage is already counted before finish.
    ["provider_call", "provider", "id"]
        .iter()
        .map(|key| std::mem::size_of::<(String, serde_json::Value)>() + key.len())
        .sum::<usize>()
        + "chat".len()
}

fn retained_bytes(text: &str, dispatch: ModelDispatch) -> usize {
    // Independent inventory of this one-delta fixture, not Budget::used or
    // production's charge helpers. Appended buffers start with capacity eight;
    // the provider ID is installed directly, with exactly its input length.
    std::mem::size_of::<(u64, stream::Call)>()
        + PROVIDER_ID.len()
        + NAME.len().max(8)
        + if text.is_empty() {
            0
        } else {
            text.len().max(8)
        }
        + canonical_id(dispatch).len()
        + binding_bytes()
}

fn retained_violation(
    events: &acyclic_harness::Result<Vec<ModelEvent>>,
    retained: usize,
    maximum: usize,
) -> Option<&'static str> {
    let rejected =
        matches!(events, Err(acyclic_harness::Error::Invalid(reason)) if reason == RETAINED_LIMIT);
    match (retained > maximum, rejected) {
        (true, false) => Some("missing retained-state rejection"),
        (false, true) => Some("unexpected retained-state rejection"),
        _ => None,
    }
}

/// Tool-call arguments from raw bytes, through the real Chat stream. A call is
/// emitted exactly when its arguments are a JSON object that the independent
/// recording reference admits, and then unchanged; anything else fails the
/// whole stream rather than failing later when the executor records the call.
/// An event larger than the stream's limit fails with its typed limit error.
pub fn assert_tool_arguments(input: &[u8]) {
    check(input, MAXIMUM);
    // Compact byte seeds must also reach retained-state finalization limits,
    // rather than requiring a megabyte-sized corpus entry to exercise them.
    check(input, 1024);
}

pub(super) fn check(input: &[u8], maximum: usize) {
    let Ok(text) = std::str::from_utf8(input) else {
        return;
    };
    let event = format!("data: {}", chat_event(text));
    let sse = format!("{event}\n\ndata: [DONE]\n\n");
    let body = Box::pin(futures::stream::iter([Ok(Bytes::from(sse))]));
    let dispatch = ModelDispatch {
        operation_id: OperationId::from_bytes([1; 16]),
        step: 0,
        request_digest: [0; 32],
    };
    let events: Result<Vec<_>, _> = futures::executor::block_on(
        stream::events(Adapter::Chat, body, maximum, dispatch).try_collect(),
    );
    // Framing precedes parsing and finalization's retained-state admission.
    let limited = |reason: &str| matches!(&events, Err(acyclic_harness::Error::Invalid(actual)) if actual == reason);
    if event.len() > maximum {
        assert!(
            limited("SSE event exceeds configured byte limit"),
            "an over-limit event must fail with the stream's limit error, got {events:?}"
        );
        return;
    }
    let retained = retained_bytes(text, dispatch);
    if limited(RETAINED_LIMIT) {
        assert_eq!(retained_violation(&events, retained, maximum), None);
        return;
    }
    // Shallow values only, so serde's own recursion limit is never the cause.
    let literal = crate::projection::arguments::literal(input)
        .ok()
        .filter(|value| depth(value) <= 64);
    if literal
        .as_ref()
        .is_some_and(|value| value.is_object() && portable(value))
    {
        assert_eq!(retained_violation(&events, retained, maximum), None);
    }
    let emitted = events.ok().and_then(|events| {
        events.into_iter().find_map(|event| match event {
            ModelEvent::ToolCall(call) => Some(call),
            _ => None,
        })
    });
    match (emitted, literal) {
        (Some(call), literal) => {
            assert_eq!(call.call_id, canonical_id(dispatch));
            assert_eq!(call.name, NAME);
            assert_eq!(
                call.metadata,
                serde_json::json!({"provider_call":{"provider":"chat","id":PROVIDER_ID}})
            );
            assert!(
                call.arguments.is_object() && portable(&call.arguments),
                "the stream emitted arguments the recording rule rejects"
            );
            if let Some(literal) = literal {
                assert_eq!(call.arguments, literal);
            }
        }
        (None, Some(literal)) => assert!(
            !(literal.is_object() && portable(&literal)),
            "the stream rejected portable object arguments"
        ),
        (None, None) => {}
    }
}

fn depth(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Array(values) => 1 + values.iter().map(depth).max().unwrap_or(0),
        serde_json::Value::Object(values) => 1 + values.values().map(depth).max().unwrap_or(0),
        _ => 0,
    }
}

/// Independent reference for the recording rule, deliberately not
/// `acyclic_harness::conversation`: no private serde marker key, and every
/// number round-trips through a JavaScript Number exactly.
pub(crate) fn portable(value: &serde_json::Value) -> bool {
    const SAFE: u64 = (1 << 53) - 1;
    match value {
        serde_json::Value::Number(number) => match (number.as_i64(), number.as_u64()) {
            (Some(integer), _) => integer.unsigned_abs() <= SAFE,
            (None, Some(integer)) => integer <= SAFE,
            (None, None) => number.as_f64().is_some_and(|float| {
                float.is_finite()
                    && float.to_bits() != (-0.0f64).to_bits()
                    && (float.fract() != 0.0 || float.abs() <= SAFE as f64)
            }),
        },
        serde_json::Value::Array(values) => values.iter().all(portable),
        serde_json::Value::Object(values) => values.iter().all(|(key, value)| {
            key != "$serde_json::private::RawValue"
                && key != "$serde_json::private::Number"
                && portable(value)
        }),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    fn finalize(
        text: &str,
        maximum: usize,
        dispatch: ModelDispatch,
    ) -> acyclic_harness::Result<Vec<ModelEvent>> {
        let mut state = stream::State::new(maximum);
        // These must both succeed: each boundary exercises finalization's
        // additional identity/binding charge, not earlier parse admission.
        stream::parse(&mut state, Adapter::Chat, &chat_event(text).to_string()).unwrap();
        stream::parse(&mut state, Adapter::Chat, "[DONE]").unwrap();
        stream::finish(state, Adapter::Chat, dispatch)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn tool_arguments_are_admitted_by_the_recording_rule(
            entries in prop::collection::vec((0usize..4, prop::collection::vec(0usize..8, 1..3), any::<bool>()), 0..5),
            wrapped in any::<bool>(),
        ) {
            const KEYS: [&str; 4] = ["a", "$serde_json::private::RawValue", "$serde_json::private::Number", "$serde_json::private"];
            const NUMBERS: [&str; 8] = ["0", "-0.0", "-0.5", "9007199254740991", "-9007199254740991", "9007199254740992", "1e16", "18446744073709551615"];
            let fields: Vec<String> = entries.iter().map(|(key, numbers, array)| {
                let numbers: Vec<_> = numbers.iter().map(|number| NUMBERS[*number]).collect();
                let value = if *array { format!("[{}]", numbers.join(",")) } else { numbers[0].to_owned() };
                format!("{}:{value}", json!(KEYS[*key]))
            }).collect();
            let object = format!("{{{}}}", fields.join(","));
            let text = if wrapped { format!("[{object}]") } else { object };
            assert_tool_arguments(text.as_bytes());
        }

        #[test]
        fn finalization_admits_exactly_the_retained_byte_boundary(
            padding in 1usize..1024, operation in any::<[u8;16]>(), step in any::<u32>(),
        ) {
            let dispatch = ModelDispatch {
                operation_id: OperationId::from_bytes(operation), step, request_digest: [0;32],
            };
            let base = json!({"a":"x".repeat(padding)}).to_string();
            let maximum = retained_bytes(&base, dispatch);
            for size in [padding - 1, padding, padding + 1] {
                let text = json!({"a":"x".repeat(size)}).to_string();
                let required = maximum + size - padding;
                prop_assert_eq!(retained_bytes(&text, dispatch), required);
                let events = finalize(&text, maximum, dispatch);
                prop_assert_eq!(retained_violation(&events, required, maximum), None);
                prop_assert_eq!(events.is_ok(), size <= padding);
                check(text.as_bytes(), maximum);
            }
        }
    }

    #[test]
    fn negative_controls_detect_missing_finalization_charges_and_early_rejection() {
        let text = json!({"a":"x".repeat(700)}).to_string();
        let dispatch = ModelDispatch {
            operation_id: OperationId::from_bytes([1; 16]),
            step: 0,
            request_digest: [0; 32],
        };
        let required = retained_bytes(&text, dispatch);
        let rejected = finalize(&text, required - 1, dispatch);
        assert_eq!(retained_violation(&rejected, required, required - 1), None);
        // Both omissions reproduce the old oracle's false rejection diagnosis.
        for omitted in [canonical_id(dispatch).len(), binding_bytes()] {
            assert_eq!(
                retained_violation(&rejected, required - omitted, required - 1),
                Some("unexpected retained-state rejection"),
            );
        }
        // Supplying excess budget through the real finish path simulates
        // failing to charge finalization; lowering it simulates early denial.
        let accepted = finalize(&text, required, dispatch);
        assert_eq!(retained_violation(&accepted, required, required), None);
        assert_eq!(
            retained_violation(&accepted, required, required - 1),
            Some("missing retained-state rejection"),
        );
        assert_eq!(
            retained_violation(&rejected, required, required),
            Some("unexpected retained-state rejection"),
        );
    }

    // Greptile on #872: a portable object larger than the event limit is a
    // typed limit rejection, not a rejected portable object.
    #[test]
    fn arguments_beyond_the_event_limit_expect_the_limit_rejection() {
        let text = format!("{{\"a\":\"{}\"}}", "x".repeat(2 << 20));
        assert_tool_arguments(text.as_bytes());
    }

    #[test]
    fn under_event_limit_arguments_can_exhaust_finalization_storage() {
        let text = json!({"a":"x".repeat(1_048_268)}).to_string();
        assert!(format!("data: {}", chat_event(&text)).len() < MAXIMUM);
        assert_tool_arguments(text.as_bytes());
    }

    #[test]
    fn checked_in_byte_corpus() {
        for bytes in [
            include_bytes!("../../fuzz/corpus/tool_arguments/portable-boundaries").as_slice(),
            include_bytes!("../../fuzz/corpus/tool_arguments/negative-zero").as_slice(),
            include_bytes!("../../fuzz/corpus/tool_arguments/unsafe-integer").as_slice(),
            include_bytes!("../../fuzz/corpus/tool_arguments/raw-value-marker").as_slice(),
            include_bytes!("../../fuzz/corpus/tool_arguments/number-marker-nested").as_slice(),
            include_bytes!("../../fuzz/corpus/tool_arguments/non-object").as_slice(),
            include_bytes!("../../fuzz/corpus/tool_arguments/duplicate-keys").as_slice(),
            include_bytes!("../../fuzz/corpus/tool_arguments/lone-surrogate").as_slice(),
            include_bytes!("../../fuzz/corpus/tool_arguments/over-event-limit").as_slice(),
            include_bytes!("../../fuzz/corpus/tool_arguments/retained-finalization").as_slice(),
        ] {
            assert_tool_arguments(bytes);
        }
    }

    #[test]
    fn negative_control_reference_rejects_what_the_old_stream_rule_admitted() {
        // The deleted stream rule checked only magnitude, so it admitted both.
        for value in [
            json!({"value":-0.0}),
            json!({"$serde_json::private::RawValue":"null"}),
            json!({"n":[{"$serde_json::private::Number":"1"}]}),
        ] {
            assert!(!portable(&value), "{value}");
            assert!(acyclic_harness::conversation::validate_portable_json(&value).is_err());
        }
        for value in [
            json!({"value":9_007_199_254_740_992_u64}),
            json!({"value":1e16}),
        ] {
            assert!(!portable(&value), "{value}");
        }
        assert!(portable(
            &json!({"max":9_007_199_254_740_991_u64, "half":-0.5, "$serde_json::private":1})
        ));
    }
}
