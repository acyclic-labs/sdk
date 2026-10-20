use serde_json::Value;

// Objects are recursive subsets. Arrays are ordered subsequences with distinct
// matches; an empty array asserts emptiness. Scalar values match exactly.
// Arguments are complete JSON payloads: extra keys or elements are corruption.
// Content parts have an exact shape: no additional, missing or reordered parts.
// File metadata inside a declared part can still use object subset assertions.
// A sole $json key matches JSON-encoded text; $contains searches text recursively.
pub fn subset(expected: &Value, actual: &Value) -> bool {
    matches(expected, actual, false)
}

fn matches(expected: &Value, actual: &Value, model_request: bool) -> bool {
    if let Some(expected) = expected.as_object().filter(|object| object.len() == 1)
        && let Some(needle) = expected.get("$contains")
    {
        return needle
            .as_str()
            .is_some_and(|needle| !needle.is_empty() && contains_text(actual, needle));
    }
    match (expected, actual) {
        (Value::Object(expected), Value::String(actual)) if expected.len() == 1 => {
            expected.get("$json").is_some_and(|expected| {
                serde_json::from_str(actual)
                    .is_ok_and(|actual| matches(expected, &actual, model_request))
            })
        }
        (Value::Object(expected), Value::Object(actual)) => expected.iter().all(|(key, value)| {
            actual.get(key).is_some_and(|actual| {
                if key == "arguments" {
                    value == actual
                } else if model_request
                    && key == "content"
                    && let Value::Array(expected) = value
                {
                    actual.as_array().is_some_and(|actual| {
                        expected.len() == actual.len()
                            && expected
                                .iter()
                                .zip(actual)
                                .all(|(expected, actual)| matches(expected, actual, model_request))
                    })
                } else {
                    matches(value, actual, model_request)
                }
            })
        }),
        (Value::Array(expected), Value::Array(actual)) => {
            if expected.is_empty() {
                return actual.is_empty();
            }
            let mut remaining = actual.iter();
            expected
                .iter()
                .all(|expected| remaining.any(|actual| matches(expected, actual, model_request)))
        }
        _ => expected == actual,
    }
}

fn contains_text(actual: &Value, needle: &str) -> bool {
    match actual {
        Value::String(text) => text.contains(needle),
        Value::Array(values) => values.iter().any(|value| contains_text(value, needle)),
        Value::Object(values) => values
            .iter()
            .any(|(key, value)| key.contains(needle) || contains_text(value, needle)),
        _ => false,
    }
}

// Model content parts are complete ordered payloads, while surrounding request
// metadata and message history retain the explicit subset/subsequence contract.
pub fn model_requests(expected: &[Value], actual: &[Value]) -> Result<(), String> {
    ordered_with(expected, actual, "requests", |expected, actual| {
        matches(expected, actual, true)
    })
}

pub fn ordered(expected: &[Value], actual: &[Value], label: &str) -> Result<(), String> {
    ordered_with(expected, actual, label, subset)
}

fn ordered_with(
    expected: &[Value],
    actual: &[Value],
    label: &str,
    compare: fn(&Value, &Value) -> bool,
) -> Result<(), String> {
    if expected.is_empty() && !actual.is_empty() {
        return Err(format!(
            "{label}: expected no observations, got {}",
            actual.len()
        ));
    }
    let mut remaining = actual.iter().enumerate();
    let mut after = 0;
    for (index, expected) in expected.iter().enumerate() {
        let Some((found, _)) = remaining.find(|(_, actual)| compare(expected, actual)) else {
            return Err(format!(
                "{label}[{index}] missing after position {after}: {expected}\nactual: {}",
                Value::Array(actual.to_vec())
            ));
        };
        after = found + 1;
    }
    Ok(())
}

#[test]
fn recursive_subsets_keep_types_null_and_array_order() {
    use serde_json::json;
    assert!(subset(
        &json!({"a":{"b":1}}),
        &json!({"a":{"b":1,"c":2},"d":3})
    ));
    assert!(subset(
        &json!([{"id":1},{"id":3}]),
        &json!([{"id":1},2,{"id":3}])
    ));
    for (expected, actual) in [
        (json!({"a":null}), json!({})),
        (json!({"a":1}), json!({"a":"1"})),
        (json!([2, 1]), json!([1, 2])),
        (json!([1, 1]), json!([1])),
        (json!([]), json!([1])),
        (json!({}), json!([])),
    ] {
        assert!(
            !subset(&expected, &actual),
            "{expected} unexpectedly matched {actual}"
        );
    }
}

#[test]
fn ordered_matchers_do_not_reuse_observations() {
    use serde_json::json;
    assert!(ordered(&[], &[], "frames").is_ok());
    assert!(ordered(&[], &[json!(1)], "frames").is_err());
    assert!(
        ordered(
            &[json!({"kind":"a"}), json!({"kind":"a"})],
            &[json!({"kind":"a"})],
            "frames"
        )
        .is_err()
    );
    assert!(ordered(&[json!(2)], &[json!(1), json!(2)], "frames").is_ok());
    assert!(ordered(&[json!(2), json!(1)], &[json!(1), json!(2)], "frames").is_err());
}

#[test]
fn json_text_matchers_keep_subset_order_and_reject_invalid_encodings() {
    use serde_json::json;
    assert!(subset(
        &json!({"$json":[{"name":"first"},{"name":"last"}]}),
        &json!(r#"[{"name":"first","extra":1},{"name":"middle"},{"name":"last"}]"#)
    ));
    assert!(subset(&json!({"$json":[]}), &json!("[]")));
    for (expected, actual) in [
        (json!({"$json":[]}), json!("[1]")),
        (json!({"$json":{"name":"a"}}), json!("not JSON")),
        (json!({"$json":[2,1]}), json!("[1,2]")),
        (json!({"$json":[1,1]}), json!("[1]")),
        (json!({"$json":{"name":"a"}}), json!({"name":"a"})),
        (json!({"$json":[],"extra":true}), json!("[]")),
        (json!({"name":"a"}), json!(r#"{"name":"a"}"#)),
    ] {
        assert!(!subset(&expected, &actual), "{expected} matched {actual}");
    }
}

#[test]
fn text_matchers_find_fragments_in_nested_content_without_vacuous_matches() {
    use serde_json::json;
    for actual in [
        json!("SECRET BODY"),
        json!("before SECRET BODY after"),
        json!([{"kind":"text","text":"before SECRET BODY after"}]),
        json!({"content":{"nested":["before SECRET BODY after"]}}),
    ] {
        assert!(subset(&json!({"$contains":"SECRET BODY"}), &actual));
    }
    for (expected, actual) in [
        (json!({"$contains":"SECRET BODY"}), json!("public metadata")),
        (json!({"$contains":"SECRET BODY"}), json!(null)),
        (json!({"$contains":""}), json!("anything")),
        (json!({"$contains":7}), json!("7")),
    ] {
        assert!(!subset(&expected, &actual), "{expected} matched {actual}");
    }
}
