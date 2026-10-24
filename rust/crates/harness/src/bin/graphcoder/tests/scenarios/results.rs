use super::TestResult;
use serde_json::Value;
use std::collections::BTreeMap;

pub fn outputs(request: &Value, protocol: &str) -> TestResult<Vec<(String, Value)>> {
    let mut outputs = Vec::new();
    if protocol == "responses" {
        for item in request["input"]
            .as_array()
            .ok_or("Responses input missing")?
        {
            if item["type"] == "function_call_output" {
                outputs.push((
                    item["call_id"].as_str().ok_or("call id missing")?.into(),
                    result_value(item["output"].as_str().ok_or("tool output missing")?),
                ));
            }
        }
    } else {
        for message in request["messages"]
            .as_array()
            .ok_or("Messages input missing")?
        {
            for part in message["content"].as_array().into_iter().flatten() {
                if part["type"] == "tool_result" {
                    let text = match &part["content"] {
                        Value::String(text) => text.clone(),
                        Value::Array(parts) => parts
                            .iter()
                            .filter_map(|part| part["text"].as_str())
                            .collect(),
                        _ => return Err("tool output content missing".into()),
                    };
                    outputs.push((
                        part["tool_use_id"]
                            .as_str()
                            .ok_or("tool use id missing")?
                            .into(),
                        result_value(&text),
                    ));
                }
            }
        }
    }
    Ok(outputs)
}

fn result_value(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.into()))
}

pub fn unique_outputs(
    outputs: impl IntoIterator<Item = (String, Value)>,
) -> TestResult<BTreeMap<String, Value>> {
    let mut unique = BTreeMap::new();
    for (id, value) in outputs {
        if unique.insert(id.clone(), value).is_some() {
            return Err(format!("HTTP repeated tool result identity: {id}").into());
        }
    }
    Ok(unique)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    proptest! {
        // Both wire projections retain exact Unicode/JSON values. Repeating an
        // observed result must fail before a map can hide the extra delivery.
        #[test]
        fn result_projection_is_lossless_and_rejects_duplicate_deliveries(
            call in "[a-z]{1,12}", text in ".{0,64}",
        ) {
            let value = json!({"text":text});
            let serialized = serde_json::to_string(&value)?;
            for (protocol, field, message) in [
                ("responses", "input", json!({"type":"function_call_output","call_id":call,"output":serialized})),
                ("anthropic", "messages", json!({"role":"user","content":[{"type":"tool_result","tool_use_id":call,"content":serialized}]})),
            ] {
                let body = json!({(field):[message]});
                let decoded: Value = serde_json::from_slice(&serde_json::to_vec(&body)?)?;
                let actual = outputs(&decoded, protocol).map_err(|error| TestCaseError::fail(error.to_string()))?;
                prop_assert_eq!(&actual, &vec![(call.clone(), value.clone())]);
                let unique = unique_outputs(actual).map_err(|error| TestCaseError::fail(error.to_string()))?;
                prop_assert_eq!(unique.get(&call), Some(&value));
                let mut doubled = decoded;
                let duplicate = doubled[field][0].clone();
                doubled[field].as_array_mut().unwrap().push(duplicate);
                let actual = outputs(&doubled, protocol).map_err(|error| TestCaseError::fail(error.to_string()))?;
                prop_assert!(unique_outputs(actual).is_err());
            }
        }
    }
}
