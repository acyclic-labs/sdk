use super::{TestResult, json_matchers};
use acyclic_harness::conversation::FileDescriptor;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub fn check(case: &Value, requests: &BTreeMap<String, Vec<Value>>, protocol: &str) -> TestResult {
    for (name, expected) in case["expect"]["model_requests"]
        .as_object()
        .into_iter()
        .flatten()
    {
        let actual = requests
            .get(name)
            .ok_or_else(|| format!("missing HTTP model {name}"))?;
        let expected = expected
            .as_array()
            .ok_or("model request expectations must be an array")?;
        let mut remaining = actual.iter();
        for expected in expected {
            let mut matched = false;
            for actual in remaining.by_ref() {
                if matches(expected, actual, protocol)? {
                    matched = true;
                    break;
                }
            }
            if !matched {
                return Err(format!("HTTP model history {name} omitted {expected}").into());
            }
        }
    }
    Ok(())
}

pub fn matches(expected: &Value, actual: &Value, protocol: &str) -> TestResult<bool> {
    let fields = expected
        .as_object()
        .ok_or("model request expectation must be an object")?;
    if fields.keys().any(|key| key != "messages") {
        return Err(format!("model request assertion has no wire projection: {expected}").into());
    }
    let Some(messages) = expected.get("messages") else {
        return Ok(true);
    };
    let mut expected = Vec::new();
    for message in messages
        .as_array()
        .ok_or("expected messages must be an array")?
    {
        let fields = message
            .as_object()
            .ok_or("message expectation must be an object")?;
        if fields
            .keys()
            .any(|key| !matches!(key.as_str(), "role" | "content"))
        {
            return Err(format!("message assertion has no wire projection: {message}").into());
        }
        let parts = match &message["content"] {
            Value::Array(parts) => parts.clone(),
            Value::String(text) => vec![json!({"kind":"text","text":text})],
            Value::Object(_) => vec![message["content"].clone()],
            _ => return Err("message expectation needs content".into()),
        };
        if parts.is_empty() {
            return Err("empty message assertion has no wire projection".into());
        }
        for part in parts {
            match (message["role"].as_str(), part["kind"].as_str()) {
                (Some("tool"), Some("tool_result")) => {
                    expected.push(json!({"role":"tool","content":part}))
                }
                (Some("user"), Some("text")) => {
                    expected.push(json!({"role":"user","content":part}))
                }
                (Some("user"), Some("file")) => {
                    if part["policy"] != "bounded_full"
                        || part.as_object().is_none_or(|fields| {
                            fields
                                .keys()
                                .any(|key| !matches!(key.as_str(), "kind" | "policy" | "file"))
                        })
                        || part["file"].as_object().is_none_or(|fields| {
                            fields.len() != 1 || !fields.contains_key("descriptor")
                        })
                    {
                        return Err(format!(
                            "file assertion needs its full-text wire projection: {part}"
                        )
                        .into());
                    }
                    expected.push(json!({"role":"user","descriptor":part["file"]["descriptor"]}));
                }
                _ => {
                    return Err(
                        format!("message assertion has no wire projection: {message}").into(),
                    );
                }
            }
        }
    }
    let actual = messages_on_wire(actual, protocol)?;
    Ok(json_matchers::ordered(&expected, &actual, "HTTP message history").is_ok())
}

fn messages_on_wire(body: &Value, protocol: &str) -> TestResult<Vec<Value>> {
    let field = if protocol == "responses" {
        "input"
    } else {
        "messages"
    };
    let messages = body[field].as_array().ok_or("HTTP messages missing")?;
    let mut names = BTreeMap::new();
    for message in messages {
        if message["type"] == "function_call" {
            names.insert(
                message["call_id"].as_str().ok_or("HTTP call id missing")?,
                &message["name"],
            );
        }
        for part in message["content"].as_array().into_iter().flatten() {
            if part["type"] == "tool_use" {
                names.insert(
                    part["id"].as_str().ok_or("HTTP call id missing")?,
                    &part["name"],
                );
            }
        }
    }
    let mut result = Vec::new();
    let mut seen = BTreeSet::new();
    for message in messages {
        if message["type"] == "function_call_output" {
            result.push(tool_result(
                &message["call_id"],
                &message["output"],
                &names,
                &mut seen,
            )?);
        }
        for part in message["content"].as_array().into_iter().flatten() {
            if part["type"] == "tool_result" {
                result.push(tool_result(
                    &part["tool_use_id"],
                    &part["content"],
                    &names,
                    &mut seen,
                )?);
            } else if message["role"] == "user"
                && matches!(part["type"].as_str(), Some("input_text" | "text"))
            {
                let text = part["text"].as_str().ok_or("HTTP user text missing")?;
                // BoundedFull file references are resolved to exact text on the
                // provider wire. Assert the descriptor against those actual bytes.
                let descriptor =
                    FileDescriptor::from_bytes(text.as_bytes(), "text/plain", &mut None)?;
                result.push(json!({"role":"user","content":{"kind":"text","text":text},"descriptor":descriptor}));
            }
        }
    }
    Ok(result)
}

fn tool_result<'a>(
    id: &'a Value,
    output: &Value,
    names: &BTreeMap<&str, &Value>,
    seen: &mut BTreeSet<&'a str>,
) -> TestResult<Value> {
    let id = id.as_str().ok_or("HTTP result id missing")?;
    if !seen.insert(id) {
        return Err(format!("HTTP repeated tool result identity: {id}").into());
    }
    let name = names
        .get(id)
        .ok_or_else(|| format!("HTTP result has no matching call: {id}"))?;
    let text = output
        .as_str()
        .ok_or("HTTP result must contain serialized content")?;
    let value: Value = serde_json::from_str(text)?;
    Ok(
        json!({"role":"tool","content":{"kind":"tool_result","call_id":id,"name":name,
        "content":{"kind":"json","value":value}}}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn history_rejects_missing_duplicated_and_changed_results(text in ".{0,64}") {
            let expected = json!({"messages":[{"role":"tool","content":{"kind":"tool_result",
                "call_id":"call","name":"wait","content":{"kind":"json","value":{"answer":text}}}}]});
            let output = serde_json::to_string(&json!({"answer":text}))?;
            let call = json!({"type":"function_call","call_id":"call","name":"wait","arguments":"{}"});
            let result = json!({"type":"function_call_output","call_id":"call","output":output});
            let body = json!({"input":[call,result]});
            prop_assert!(matches(&expected, &body, "responses").map_err(|error| TestCaseError::fail(error.to_string()))?);
            let mut missing = body.clone(); missing["input"].as_array_mut().unwrap().pop();
            prop_assert!(!matches(&expected, &missing, "responses").map_err(|error| TestCaseError::fail(error.to_string()))?);
            let mut renamed = body.clone(); renamed["input"][0]["name"] = "archive".into();
            prop_assert!(!matches(&expected, &renamed, "responses").map_err(|error| TestCaseError::fail(error.to_string()))?);
            let mut changed = body.clone(); changed["input"][1]["output"] = serde_json::to_string(&json!({"answer":format!("{text}! ")}))?.into();
            prop_assert!(!matches(&expected, &changed, "responses").map_err(|error| TestCaseError::fail(error.to_string()))?);
            let mut duplicate = expected.clone();
            duplicate["messages"].as_array_mut().unwrap().push(expected["messages"][0].clone());
            prop_assert!(!matches(&duplicate, &body, "responses").map_err(|error| TestCaseError::fail(error.to_string()))?);
        }

        #[test]
        fn extra_result_delivery_cannot_hide_in_a_history_subsequence(text in ".{0,64}") {
            let expected = json!({"messages":[{"role":"tool","content":{"kind":"tool_result",
                "call_id":"call","name":"wait","content":{"kind":"json","value":{"answer":text}}}}]});
            let output = serde_json::to_string(&json!({"answer":text}))?;
            for (protocol, field, call, result) in [
                ("responses", "input",
                    json!({"type":"function_call","call_id":"call","name":"wait","arguments":"{}"}),
                    json!({"type":"function_call_output","call_id":"call","output":output})),
                ("anthropic", "messages",
                    json!({"role":"assistant","content":[{"type":"tool_use","id":"call","name":"wait","input":{}}]}),
                    json!({"role":"user","content":[{"type":"tool_result","tool_use_id":"call","content":output}]})),
            ] {
                let mut body = json!({(field):[call,result]});
                prop_assert!(matches(&expected, &body, protocol).map_err(|error| TestCaseError::fail(error.to_string()))?);
                let duplicate = body[field][1].clone();
                body[field].as_array_mut().unwrap().push(duplicate);
                let error = matches(&expected, &body, protocol).unwrap_err();
                prop_assert!(error.to_string().contains("repeated tool result identity"));
            }
        }

        #[test]
        fn file_history_is_bound_to_exact_utf8_bytes(text in ".{1,64}") {
            let descriptor = FileDescriptor::from_bytes(text.as_bytes(), "text/plain", &mut None).map_err(|error| TestCaseError::fail(error.to_string()))?;
            let expected = json!({"messages":[{"role":"user","content":[{"kind":"file","policy":"bounded_full","file":{"descriptor":descriptor}}]}]});
            for (protocol, field, kind) in [("responses","input","input_text"), ("anthropic","messages","text")] {
                let body = json!({(field):[{"role":"user","content":[{"type":kind,"text":text}]}]});
                prop_assert!(matches(&expected, &body, protocol).map_err(|error| TestCaseError::fail(error.to_string()))?);
                let changed = json!({(field):[{"role":"user","content":[{"type":kind,"text":format!("{text}!")}]}]});
                prop_assert!(!matches(&expected, &changed, protocol).map_err(|error| TestCaseError::fail(error.to_string()))?);
            }
        }
    }
}
