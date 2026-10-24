use serde_json::Value;

// Objects are recursive subsets. Arrays are ordered subsequences with distinct
// matches; an empty array asserts emptiness. Scalar values match exactly.
// Arguments are complete JSON payloads: extra keys or elements are corruption.
// A sole $json key matches JSON-encoded text; $contains searches text recursively.
pub fn subset(expected: &Value, actual: &Value) -> bool {
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
                serde_json::from_str(actual).is_ok_and(|actual| subset(expected, &actual))
            })
        }
        (Value::Object(expected), Value::Object(actual)) => expected.iter().all(|(key, value)| {
            actual.get(key).is_some_and(|actual| {
                if key == "arguments" {
                    value == actual
                } else {
                    subset(value, actual)
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
                .all(|expected| remaining.any(|actual| subset(expected, actual)))
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
