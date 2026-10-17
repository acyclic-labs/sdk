//! Tool-call arguments through the real stream against the recording rule.
use crate::{Adapter, stream};
use acyclic_harness::{
    OperationId,
    model::{ModelDispatch, ModelEvent},
};
use bytes::Bytes;
use futures::TryStreamExt as _;

/// Tool-call arguments from raw bytes, through the real Chat stream. A call is
/// emitted exactly when its arguments are a JSON object that the independent
/// recording reference admits, and then unchanged; anything else fails the
/// whole stream rather than failing later when the executor records the call.
pub fn assert_tool_arguments(input: &[u8]) {
    let Ok(text) = std::str::from_utf8(input) else {
        return;
    };
    let calls =
        serde_json::json!([{"index":0,"id":"call","function":{"name":"echo","arguments":text}}]);
    let sse = format!(
        "data: {}\n\ndata: [DONE]\n\n",
        serde_json::json!({"choices":[{"delta":{"tool_calls":calls}}]})
    );
    let body = Box::pin(futures::stream::iter([Ok(Bytes::from(sse))]));
    let dispatch = ModelDispatch {
        operation_id: OperationId::from_bytes([1; 16]),
        step: 0,
        request_digest: [0; 32],
    };
    let events: Result<Vec<_>, _> = futures::executor::block_on(
        stream::events(Adapter::Chat, body, 1 << 20, dispatch).try_collect(),
    );
    let emitted = events.ok().and_then(|events| {
        events.into_iter().find_map(|event| match event {
            ModelEvent::ToolCall(call) => Some(call),
            _ => None,
        })
    });
    // Shallow values only, so serde's own recursion limit is never the cause.
    let literal = crate::projection::arguments::literal(input)
        .ok()
        .filter(|value| depth(value) <= 64);
    match (emitted, literal) {
        (Some(call), literal) => {
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
    use super::{assert_tool_arguments, portable};
    use proptest::prelude::*;
    use serde_json::json;

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
