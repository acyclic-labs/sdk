use super::{check, id, name};
use crate::protocol::{content, shape};
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn validate(body: &Value, errors: &mut Vec<&'static str>) {
    let Some(messages) = body["messages"].as_array() else {
        check(false, "OAC-SEQ-01", errors);
        return;
    };
    let mut pending = BTreeSet::new();
    for message in messages {
        let role = message["role"].as_str().unwrap_or("");
        let position = content::position(content::CHAT, role);
        check(position.is_some(), "OAC-SEQ-01", errors);
        if role == "tool" {
            let matched = message["tool_call_id"]
                .as_str()
                .is_some_and(|id| pending.remove(id));
            check(matched, "OAC-TOOL-02", errors);
        } else {
            check(pending.is_empty(), "OAC-TOOL-01", errors);
            pending.clear();
        }
        if let Some(calls) = message["tool_calls"].as_array() {
            check(role == "assistant", "OAC-SEQ-03", errors);
            for call in calls.iter() {
                check(id(&call["id"], 40), "OAC-TOOL-04", errors);
                check(shape::CHAT_CALL.accepts(call), "OAC-TOOL-03", errors);
                if let Some(id) = call["id"].as_str() {
                    check(pending.insert(id), "OAC-TOOL-01", errors);
                }
            }
        }
        let content = &message["content"];
        if let Some(position) = position {
            let has_calls = message["tool_calls"]
                .as_array()
                .is_some_and(|calls| !calls.is_empty());
            if let Some(rule) = position.violation(content, has_calls) {
                check(false, rule, errors);
            }
        }
    }
    check(pending.is_empty(), "OAC-TOOL-01", errors);
    if let Some(tools) = body["tools"].as_array() {
        check(tools.len() <= 128, "OAC-TOOL-03", errors);
        for tool in tools {
            check(
                tool["type"] == "function" && name(&tool["function"]["name"], 64, false, b"_-"),
                "OAC-TOOL-03",
                errors,
            );
        }
    }
    if let Some(stop) = body["stop"].as_array() {
        check(stop.len() <= 4, "OAC-TOOL-03", errors);
    }
}
