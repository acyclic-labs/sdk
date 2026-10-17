use super::*;
use proptest::prelude::*;
use serde_json::json;

fn fixtures() -> [(Adapter, Value, &'static str); 4] {
    [
        (
            Adapter::Anthropic,
            json!({"model":"fixture","max_tokens":128,"messages":[
                {"role":"user","content":[{"type":"text","text":"question"}]},
                {"role":"assistant","content":[{"type":"thinking","thinking":"one\ntwo","signature":"native"},{"type":"tool_use","id":"toolu_1","name":"lookup","input":{}}]},
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"answer"}]}
            ]}),
            "ANT-TOOL-04",
        ),
        (
            Adapter::Responses,
            json!({"model":"fixture","store":false,"input":[
                {"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"native"},
                {"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"},
                {"type":"function_call_output","call_id":"call_1","output":"answer"}
            ]}),
            "OAR-TOOL-04",
        ),
        (
            Adapter::Chat,
            json!({"model":"fixture","messages":[
                {"role":"user","content":"question"},
                {"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},
                {"role":"tool","tool_call_id":"call_1","content":"answer"}
            ]}),
            "OAC-TOOL-01",
        ),
        (
            Adapter::Gemini,
            json!({"contents":[
                {"role":"user","parts":[{"text":"question"}]},
                {"role":"model","parts":[{"functionCall":{"name":"lookup","args":{}},"thoughtSignature":"native"},{"functionCall":{"name":"zero_args"}}]},
                {"role":"user","parts":[{"functionResponse":{"name":"lookup","response":{"output":"one"}}},{"functionResponse":{"name":"zero_args","response":{"output":"two"}}}]}
            ]}),
            "GEM-TOOL-05",
        ),
    ]
}

#[test]
fn native_tool_batches_are_accepted_and_missing_results_are_detected() {
    for (adapter, mut body, missing_result_rule) in fixtures() {
        assert!(validate_input(adapter, &body).is_empty());
        let items = match adapter {
            Adapter::Anthropic | Adapter::Chat => "messages",
            Adapter::Responses => "input",
            Adapter::Gemini => "contents",
        };
        body[items].as_array_mut().unwrap().pop();
        assert!(validate_input(adapter, &body).contains(&missing_result_rule));
    }
}

fn arbitrary_json() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|number| json!(number)),
        ".{0,64}".prop_map(Value::String)
    ]
    .prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map("[a-z_]{0,16}", inner, 0..4)
                .prop_map(|values| Value::Object(values.into_iter().collect())),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Local diagnostics depend only on decoded input, including malformed
    // argument strings; the JSON codec cannot change their rule IDs.
    #[test]
    fn diagnostics_are_stable_across_json_round_trip(body in arbitrary_json()) {
        let encoded=serde_json::to_vec(&body).unwrap();
        let replay=serde_json::from_slice(&encoded).unwrap();
        for adapter in [Adapter::Anthropic,Adapter::Responses,Adapter::Chat,Adapter::Gemini] {
            prop_assert_eq!(validate_input(adapter,&body),validate_input(adapter,&replay));
        }
    }
}

#[path = "protocol_tests.rs"]
mod protocol;

#[path = "shape_table_tests.rs"]
mod shape_table;
