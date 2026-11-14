use serde_json::Value;

// Objects are recursive subsets. Arrays are ordered subsequences with distinct
// matches; an empty array asserts emptiness. Scalar values match exactly.
// Arguments are complete JSON payloads: extra keys or elements are corruption.
// A sole $json key matches JSON-encoded text; $contains searches text recursively.
// $skill_catalog uses the public SkillSummary schema for both sides, in exact order.
// Its expected id is "*": identity is opaque, but must be a nonempty string.
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
            encoded_json(expected, actual, false)
        }
        (Value::Object(expected), Value::Object(actual)) => expected.iter().all(|(key, value)| {
            actual.get(key).is_some_and(|actual| {
                if key == "arguments" {
                    match (value, actual) {
                        (Value::Object(expected), Value::String(actual)) if expected.len() == 1 => {
                            encoded_json(expected, actual, true)
                        }
                        _ => value == actual,
                    }
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

fn encoded_json(expected: &serde_json::Map<String, Value>, actual: &str, exact: bool) -> bool {
    if !exact && let Some(expected) = expected.get("$skill_catalog") {
        use acyclic_harness::context::SkillSummary;
        let Ok(expected) = serde_json::from_value::<Vec<SkillSummary>>(expected.clone()) else {
            return false;
        };
        let Ok(actual) = serde_json::from_str::<Vec<SkillSummary>>(actual) else {
            return false;
        };
        return expected.len() == actual.len()
            && expected.iter().zip(&actual).all(|(expected, actual)| {
                expected.id == "*"
                    && !actual.id.is_empty()
                    && expected.name == actual.name
                    && expected.description == actual.description
            });
    }
    expected.get("$json").is_some_and(|expected| {
        serde_json::from_str(actual).is_ok_and(|actual| {
            if exact {
                expected == &actual
            } else {
                subset(expected, &actual)
            }
        })
    })
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

#[cfg(test)]
#[test]
fn skill_catalog_matches_public_schema_and_rejects_storage_metadata() {
    use acyclic_harness::context::SkillSummary;
    use serde_json::json;

    let expected =
        json!({"$skill_catalog":[{"id":"*","name":"inspect","description":"Inspect files"}]});
    let catalog = serde_json::to_value([SkillSummary {
        id: "opaque-source".into(),
        name: "inspect".into(),
        description: "Inspect files".into(),
    }])
    .unwrap();
    assert!(subset(&expected, &json!(catalog.to_string())));
    for actual in [
        json!([{"name":"inspect","description":"Inspect files"}]),
        json!([{"id":7,"name":"inspect","description":"Inspect files"}]),
        json!([{"id":"","name":"inspect","description":"Inspect files"}]),
        json!([{"id":"opaque-source","name":"inspect","description":"Changed"}]),
        json!([{"id":"opaque-source","name":"inspect","description":"Inspect files","source":{"path":"private"}}]),
        json!([{"id":"opaque-source","name":"inspect","description":"Inspect files","fields":{}}]),
        json!([]),
        json!([catalog[0].clone(), catalog[0].clone()]),
    ] {
        assert!(
            !subset(&expected, &json!(actual.to_string())),
            "matched {actual}"
        );
    }
    assert!(!subset(&expected, &json!("invalid JSON")));
    assert!(!subset(&expected, &catalog));
    let mut stale_expected = expected;
    stale_expected["$skill_catalog"][0]["source"] = json!({"path":"private"});
    assert!(!subset(&stale_expected, &json!(catalog.to_string())));
}
