use super::{check, name, text};
use serde_json::Value;

pub(super) fn validate(body: &Value, errors: &mut Vec<&'static str>) {
    let Some(contents) = body["contents"].as_array() else {
        check(false, "GEM-SEQ-03", errors);
        return;
    };
    check(!contents.is_empty(), "GEM-SEQ-03", errors);
    check(
        contents
            .first()
            .is_some_and(|content| content["role"] == "user")
            && contents
                .last()
                .is_some_and(|content| content["role"] == "user"),
        "GEM-SEQ-05",
        errors,
    );
    let mut previous = "";
    let mut pending = Vec::new();
    for content in contents {
        let role = content["role"].as_str().unwrap_or("");
        check(matches!(role, "user" | "model"), "GEM-SEQ-01", errors);
        check(role != previous, "GEM-SEQ-04", errors);
        let Some(parts) = content["parts"].as_array() else {
            check(false, "GEM-SEQ-03", errors);
            continue;
        };
        check(!parts.is_empty(), "GEM-SEQ-03", errors);
        let mut calls = Vec::new();
        let mut results = Vec::new();
        for part in parts {
            if let Some(call) = part.get("functionCall") {
                check(role == "model", "GEM-TOOL-06", errors);
                check(
                    name(&call["name"], 128, true, b"_.:-"),
                    "GEM-TOOL-01",
                    errors,
                );
                check(
                    call.get("args").is_none_or(Value::is_object),
                    "GEM-TOOL-03",
                    errors,
                );
                calls.push((call["name"].as_str(), call["id"].as_str()));
            } else if let Some(result) = part.get("functionResponse") {
                check(role == "user", "GEM-TOOL-06", errors);
                check(result["response"].is_object(), "GEM-TOOL-03", errors);
                results.push((result["name"].as_str(), result["id"].as_str()));
            } else if part.get("text").is_some() {
                check(
                    part["text"].as_str().is_some_and(|text| !text.is_empty())
                        || part["thoughtSignature"].is_string(),
                    "GEM-SEQ-03",
                    errors,
                );
            } else {
                check(
                    part.get("inlineData").is_some() || part.get("fileData").is_some(),
                    "GEM-SEQ-03",
                    errors,
                );
            }
        }
        check(results == pending, "GEM-TOOL-05", errors);
        pending = calls;
        previous = role;
    }
    check(pending.is_empty(), "GEM-TOOL-05", errors);
    if let Some(parts) = body["systemInstruction"]["parts"].as_array() {
        check(!parts.is_empty(), "GEM-SEQ-03", errors);
        for part in parts {
            check(text(&part["text"]), "GEM-SEQ-02", errors);
        }
    }
}
