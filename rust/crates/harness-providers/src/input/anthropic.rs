use super::{check, id, name, text};
use crate::protocol::{
    sequence::{results_lead, tool_result, user_boundary},
    shape,
};
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn validate(body: &Value, errors: &mut Vec<&'static str>) {
    let Some(messages) = body["messages"].as_array() else {
        check(false, "ANT-SEQ-09", errors);
        return;
    };
    check(!messages.is_empty(), "ANT-SEQ-09", errors);
    check(messages.len() <= 100_000, "ANT-SEQ-05", errors);
    check(
        user_boundary(
            messages
                .first()
                .and_then(|message| message["role"].as_str()),
        ),
        "ANT-SEQ-04",
        errors,
    );
    check(
        user_boundary(messages.last().and_then(|message| message["role"].as_str())),
        "ANT-SEQ-06",
        errors,
    );
    let mut pending = BTreeSet::new();
    for message in messages {
        let role = message["role"].as_str().unwrap_or("");
        check(matches!(role, "user" | "assistant"), "ANT-SEQ-01", errors);
        let Some(parts) = message["content"].as_array() else {
            check(pending.is_empty(), "ANT-TOOL-04", errors);
            check(text(&message["content"]), "ANT-TXT-01", errors);
            continue;
        };
        check(!parts.is_empty(), "ANT-SEQ-08", errors);
        check(pending.is_empty() || role == "user", "ANT-TOOL-04", errors);
        check(
            results_lead(parts.iter().map(tool_result)),
            "ANT-TOOL-05",
            errors,
        );
        let mut calls = BTreeSet::new();
        for part in parts {
            match part["type"].as_str() {
                Some("text") => {
                    check(text(&part["text"]), "ANT-TXT-02", errors);
                }
                Some("tool_use") => {
                    check(role == "assistant", "ANT-TXT-03", errors);
                    check(id(&part["id"], usize::MAX), "ANT-TOOL-02", errors);
                    check(
                        name(&part["name"], 200, false, b"_-"),
                        "ANT-TOOL-01",
                        errors,
                    );
                    check(part["input"].is_object(), "ANT-TOOL-03", errors);
                    if let Some(id) = part["id"].as_str() {
                        check(calls.insert(id), "ANT-TOOL-07", errors);
                    }
                }
                Some("tool_result") => {
                    let matched = part["tool_use_id"]
                        .as_str()
                        .is_some_and(|id| pending.remove(id));
                    check(role == "user" && matched, "ANT-TOOL-06", errors);
                }
                Some("thinking") => {
                    check(
                        role == "assistant" && shape::THINKING.accepts(part),
                        "ANT-THK-02",
                        errors,
                    );
                    check(part.get("cache_control").is_none(), "ANT-THK-07", errors);
                }
                Some("redacted_thinking") => {
                    check(
                        role == "assistant" && shape::REDACTED.accepts(part),
                        "ANT-THK-02",
                        errors,
                    );
                }
                Some("image" | "document" | "search_result") => {}
                _ => {
                    check(false, "ANT-TXT-03", errors);
                }
            }
        }
        check(pending.is_empty(), "ANT-TOOL-04", errors);
        pending = calls;
    }
    check(pending.is_empty(), "ANT-TOOL-04", errors);
    if let Some(tools) = body["tools"].as_array() {
        for tool in tools {
            check(
                name(&tool["name"], 128, false, b"_-"),
                "ANT-TOOL-01",
                errors,
            );
            check(tool["input_schema"].is_object(), "ANT-TOOL-12", errors);
        }
    }
}
