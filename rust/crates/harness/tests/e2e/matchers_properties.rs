use super::{ordered, subset};
use proptest::prelude::*;
use serde_json::{Value, json};

fn text() -> impl Strategy<Value = String> {
    proptest::collection::vec(any::<char>(), 0..32)
        .prop_map(|characters| characters.into_iter().collect())
}

fn value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|number| json!(number)),
        text().prop_map(Value::String),
    ]
    .prop_recursive(4, 48, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            proptest::collection::btree_map("k[0-4]", inner, 0..4)
                .prop_map(|fields| Value::Object(fields.into_iter().collect())),
        ]
    })
}

// Enumerate every increasing choice of slots, independently of the production
// greedy walk. The top-level empty-array assertion deliberately means emptiness.
fn subsequence(expected: &[Value], actual: &[Value]) -> bool {
    fn choices(expected: &[Value], actual: &[Value]) -> bool {
        let Some((first, rest)) = expected.split_first() else {
            return true;
        };
        actual
            .iter()
            .enumerate()
            .any(|(index, item)| subset(first, item) && choices(rest, &actual[index + 1..]))
    }
    if expected.is_empty() {
        actual.is_empty()
    } else {
        choices(expected, actual)
    }
}

proptest! {
    // Ordinary JSON is reflexive. Extra object fields are harmless, while absent
    // keys and different scalar/container types can never satisfy an assertion.
    #[test]
    fn subsets_preserve_values_presence_and_types(value in value(), extra in value()) {
        prop_assert!(subset(&value, &value));
        let encoded = serde_json::to_string(&value).unwrap();
        prop_assert_eq!(serde_json::from_str::<Value>(&encoded).unwrap(), value.clone());
        prop_assert!(subset(&json!({"$json": value}), &Value::String(encoded)), "encoded JSON subset");
        let expected = json!({"required": value});
        let actual = json!({"required": value, "extra": extra});
        prop_assert!(subset(&expected, &actual));
        prop_assert!(!subset(&expected, &json!({"extra": extra})), "required key is absent");
        for actual in [Value::Null, Value::Bool(false), json!(0), json!("text"), json!([])] {
            prop_assert!(!subset(&expected, &actual));
        }
    }

    // Arrays and observation lists match iff distinct increasing slots exist;
    // duplicate expectations consume separate slots, including nested subsets.
    #[test]
    fn ordered_matches_equal_exhaustive_slot_choices(
        expected in proptest::collection::vec(value(), 0..5),
        actual in proptest::collection::vec(value(), 0..7),
        selected in any::<u8>(),
    ) {
        let possible = subsequence(&expected, &actual);
        prop_assert_eq!(subset(&Value::Array(expected.clone()), &Value::Array(actual.clone())), possible);
        prop_assert_eq!(ordered(&expected, &actual, "generated observations").is_ok(), possible);
        let mut overfull = actual.clone();
        overfull.push(Value::Null);
        prop_assert!(!subset(&Value::Array(overfull), &Value::Array(actual.clone())));

        // Unique indices make selected positive matches and reversed-order
        // failures independent of repeated or mutually matching payloads.
        let frames: Vec<_> = actual.into_iter().enumerate()
            .map(|(index, payload)| json!({"index": index, "payload": payload}))
            .collect();
        let mut expected: Vec<_> = frames.iter().enumerate()
            .filter(|(index, _)| selected & (1_u8 << *index) != 0 || *index == 0)
            .map(|(_, frame)| frame.clone()).collect();
        prop_assert!(subset(&json!(expected), &json!(frames)));
        prop_assert!(ordered(&expected, &frames, "selected observations").is_ok());
        if expected.len() > 1 {
            expected.reverse();
            prop_assert!(!subset(&json!(expected), &json!(frames)));
            prop_assert!(ordered(&expected, &frames, "reversed observations").is_err());
        }
    }

    // The sentinel must be detected inside arbitrary surrounding Unicode text,
    // object keys and structured parts, rather than only whole-file equality.
    #[test]
    fn body_sentinels_survive_wrapping_and_structured_content(
        needle in proptest::collection::vec(any::<char>(), 1..16)
            .prop_map(|characters| characters.into_iter().collect::<String>()),
        prefix in text(), suffix in text(), unrelated in value(),
    ) {
        let expected = json!({"$contains": needle});
        let wrapped = format!("{prefix}{needle}{suffix}");
        for actual in [
            json!(wrapped),
            json!({"parts":[unrelated, {"kind":"text", "text":wrapped}]}),
            Value::Object([(wrapped, unrelated)].into_iter().collect()),
        ] {
            prop_assert!(subset(&expected, &actual));
        }
        prop_assert!(!subset(&expected, &Value::Null));
    }
}
