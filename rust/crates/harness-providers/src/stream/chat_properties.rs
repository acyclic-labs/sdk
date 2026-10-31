use super::*;
use proptest::prelude::*;
use serde_json::json;

fn parse_delta(state: &mut State<'_>, delta: Value) -> Result<Vec<ModelEvent>> {
    // Generated parser inputs, not captured external response fixtures.
    parse(state, &json!({"choices":[{"delta":delta}]}))
}

proptest! {
    #[test]
    fn nullable_optional_deltas_equal_omission_without_retaining_empty_blocks(
        text in ".{0,80}",
    ) {
        let mut omitted = State::new(4096);
        let mut nullable = State::new(4096);
        let expected = parse_delta(&mut omitted, json!({})).unwrap();
        let actual = parse_delta(&mut nullable, json!({
            "content":null, "reasoning_content":null, "reasoning":null, "tool_calls":null
        })).unwrap();
        prop_assert_eq!(actual, expected);
        prop_assert!(nullable.thinking.is_empty());
        prop_assert_eq!(nullable.budget.used(), omitted.budget.used());

        let expected = parse_delta(&mut omitted, json!({"reasoning":text})).unwrap();
        let actual = parse_delta(&mut nullable, json!({"reasoning_content":null,"reasoning":text})).unwrap();
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(nullable.budget.used(), omitted.budget.used());

        let expected = parse_delta(&mut omitted, json!({"tool_calls":[{"index":0}]})).unwrap();
        let actual = parse_delta(&mut nullable, json!({"tool_calls":[{"index":0,"type":null,"id":null,"function":null}]})).unwrap();
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(nullable.budget.used(), omitted.budget.used());

        let expected = parse_delta(&mut omitted, json!({"tool_calls":[{"index":0,"function":{}}]})).unwrap();
        let actual = parse_delta(&mut nullable, json!({"tool_calls":[{"index":0,"function":{"name":null,"arguments":null}}]})).unwrap();
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(nullable.budget.used(), omitted.budget.used());

        // Null is absence; a present malformed scalar must still refuse.
        for malformed in [json!(true), json!(7), json!([]), json!({})] {
            prop_assert!(
                parse_delta(&mut State::new(4096), json!({"reasoning_content":malformed})).is_err(),
                "present malformed reasoning must refuse"
            );
        }
    }
}
