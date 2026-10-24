use super::TestResult;
use serde_json::{Value, json};

fn event(value: Value) -> String {
    format!("data: {value}\n\n")
}

// The fixture only writes provider bytes. The shipped registry and SSE decoder
// are responsible for turning them back into model events.
pub fn encode(turn: &Value, protocol: &str) -> TestResult<(Vec<String>, bool)> {
    let events = match turn {
        Value::String(text) => vec![
            json!({"type":"event","kind":"content","delta":text}),
            json!({"type":"event","kind":"completed","metadata":null}),
        ],
        Value::Array(events) => events.clone(),
        _ => return Err("scripted turn must be text or events".into()),
    };
    let mut wire = Vec::new();
    if protocol == "anthropic" {
        wire.push(event(
            json!({"type":"message_start","message":{"id":"fixture","usage":{"input_tokens":32}}}),
        ));
    } else if protocol != "responses" {
        return Err(format!("unsupported scripted wire protocol: {protocol}").into());
    }
    for (index, value) in events.iter().enumerate() {
        if value["type"] == "pending" {
            return Ok((wire, true));
        }
        if value["type"] == "error" {
            wire.push(event(
                json!({"type":"error","error":{"type":"api_error","message":value["error"]}}),
            ));
            continue;
        }
        if value["type"] != "event" {
            return Err(format!("scripted event needs a wire fault: {value}").into());
        }
        match (protocol, value["kind"].as_str()) {
            ("responses", Some("content" | "reasoning")) => {
                let kind = if value["kind"] == "content" {
                    "response.output_text.delta"
                } else {
                    "response.reasoning_summary_text.delta"
                };
                wire.push(event(json!({"type":kind,"delta":value["delta"]})));
            }
            ("responses", Some("tool_call")) => {
                wire.push(event(
                    json!({"type":"response.output_item.added","output_index":index,
                    "item":{"type":"function_call","call_id":value["call_id"],
                    "name":value["name"],"arguments":""}}),
                ));
                let arguments = serde_json::to_string(&value["arguments"])?;
                // Real deltas, including a split JSON string, not one decoded DTO.
                let middle = arguments.floor_char_boundary(arguments.len() / 2);
                for delta in [&arguments[..middle], &arguments[middle..]] {
                    wire.push(event(
                        json!({"type":"response.function_call_arguments.delta",
                        "output_index":index,"delta":delta}),
                    ));
                }
            }
            ("responses", Some("completed")) => {
                wire.push(event(json!({"type":"response.completed","response":{
                    "id":"fixture","status":"completed","usage":{"input_tokens":32,"output_tokens":1}}})));
            }
            ("anthropic", Some("content")) => {
                wire.push(event(json!({"type":"content_block_start","index":index,
                    "content_block":{"type":"text","text":""}})));
                wire.push(event(json!({"type":"content_block_delta","index":index,
                    "delta":{"type":"text_delta","text":value["delta"]}})));
                wire.push(event(json!({"type":"content_block_stop","index":index})));
            }
            ("anthropic", Some("tool_call")) => {
                wire.push(event(json!({"type":"content_block_start","index":index,
                    "content_block":{"type":"tool_use","id":value["call_id"],"name":value["name"],"input":{}}})));
                wire.push(event(json!({"type":"content_block_delta","index":index,
                    "delta":{"type":"input_json_delta","partial_json":serde_json::to_string(&value["arguments"])?}})));
                wire.push(event(json!({"type":"content_block_stop","index":index})));
            }
            ("anthropic", Some("completed")) => {
                let calls = events.iter().any(|event| event["kind"] == "tool_call");
                wire.push(event(json!({"type":"message_delta","delta":{
                    "stop_reason":if calls { "tool_use" } else { "end_turn" }},"usage":{"output_tokens":1}})));
                wire.push(event(json!({"type":"message_stop"})));
            }
            _ => {
                return Err(format!("event has no {protocol} wire representation: {value}").into());
            }
        }
    }
    Ok((wire, false))
}

#[path = "results.rs"]
mod results;
pub use results::{outputs, unique_outputs};
