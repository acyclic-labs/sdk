use serde_json::Value;

// Objects are recursive subsets. Arrays are ordered subsequences with distinct
// matches; an empty array asserts emptiness. Scalar values match exactly.
pub fn subset(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => expected
            .iter()
            .all(|(key, value)| actual.get(key).is_some_and(|actual| subset(value, actual))),
        (Value::Array(expected), Value::Array(actual)) => {
            if expected.is_empty() {
                return actual.is_empty();
            }
            let mut remaining = actual.iter();
            expected
                .iter()
                .all(|expected| remaining.any(|actual| subset(expected, actual)))
        }
        _ => expected == actual,
    }
}

pub fn ordered(expected: &[Value], actual: &[Value], label: &str) -> Result<(), String> {
    if expected.is_empty() && !actual.is_empty() {
        return Err(format!(
            "{label}: expected no observations, got {}",
            actual.len()
        ));
    }
    let mut remaining = actual.iter().enumerate();
    let mut after = 0;
    for (index, expected) in expected.iter().enumerate() {
        let Some((found, _)) = remaining.find(|(_, actual)| subset(expected, actual)) else {
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
