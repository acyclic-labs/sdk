use super::{check, id, name};
use crate::protocol::{content, shape};
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn validate(body: &Value, errors: &mut Vec<&'static str>) {
    if let Some(tools) = body["tools"].as_array() {
        for tool in tools {
            check(
                tool["type"] == "function" && name(&tool["name"], 64, false, b"_-"),
                "OAR-TOOL-01",
                errors,
            );
        }
    }
    if body["input"].is_string() {
        return;
    }
    let Some(items) = body["input"].as_array() else {
        check(false, "OAR-SEQ-01", errors);
        return;
    };
    let mut pending = BTreeSet::new();
    let mut reasoning = false;
    for item in items {
        let kind = item["type"].as_str().unwrap_or("message");
        check(
            !reasoning
                || kind == "function_call"
                || (kind == "message" && item["role"] == "assistant"),
            "OAR-RSN-02",
            errors,
        );
        match kind {
            "message" => {
                let role = item["role"].as_str().unwrap_or("");
                let position = content::position(content::RESPONSES, role);
                check(position.is_some(), "OAR-SEQ-01", errors);
                if let Some(position) = position {
                    if let Some(rule) = position.violation(&item["content"], false) {
                        check(false, rule, errors);
                    }
                }
                reasoning = false;
            }
            "function_call" => {
                check(id(&item["call_id"], 64), "OAR-TOOL-03", errors);
                check(shape::RESPONSE_CALL.accepts(item), "OAR-TOOL-02", errors);
                if let Some(id) = item["call_id"].as_str() {
                    check(pending.insert(id), "OAR-TOOL-04", errors);
                }
                reasoning = false;
            }
            "function_call_output" => {
                check(!reasoning, "OAR-RSN-02", errors);
                let matched = item["call_id"]
                    .as_str()
                    .is_some_and(|id| pending.remove(id));
                check(matched, "OAR-TOOL-04", errors);
                if let Some(rule) = content::RESPONSE_TOOL_OUTPUT.violation(&item["output"], false)
                {
                    check(false, rule, errors);
                }
            }
            "reasoning" => {
                check(
                    item["id"].is_string() && item["summary"].is_array(),
                    "OAR-RSN-04",
                    errors,
                );
                if body["store"] == false {
                    check(shape::REASONING.accepts(item), "OAR-RSN-03", errors);
                }
                reasoning = true;
            }
            _ => check(false, "OAR-SEQ-01", errors),
        }
    }
    check(!reasoning, "OAR-RSN-02", errors);
    check(pending.is_empty(), "OAR-TOOL-04", errors);
}
