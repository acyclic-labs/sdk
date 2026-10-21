use crate::Step;
use serde_json::{Value, json};

fn sse(value: Value) -> String {
    let event = value["type"]
        .as_str()
        .map(|name| format!("event: {name}\n"))
        .unwrap_or_default();
    format!("{event}data: {value}\n\n")
}

pub(crate) fn events(path: &str, step: &Step, input_tokens: u64) -> Option<Vec<String>> {
    let mut output = Vec::new();
    let usage = json!({"input_tokens":input_tokens,"output_tokens":2});
    match path {
        "responses" => {
            output.push(sse(json!({"type":"response.created","response":{"id":"mock-response","status":"in_progress"}})));
            for text in &step.text {
                output.push(sse(json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":text})));
            }
            for (index, tool) in step.tools.iter().enumerate() {
                let arguments = tool.arguments.to_string();
                output.push(sse(json!({"type":"response.output_item.added","output_index":index+1,"item":{"type":"function_call","id":format!("fc_{}",tool.id),"call_id":tool.id,"name":tool.name,"arguments":""}})));
                output.push(sse(json!({"type":"response.function_call_arguments.delta","output_index":index+1,"delta":arguments})));
                output.push(sse(json!({"type":"response.function_call_arguments.done","output_index":index+1,"arguments":arguments})));
            }
            if !step.hold {
                output.push(sse(json!({"type":"response.completed","response":{"id":"mock-response","status":"completed","usage":usage}})));
            }
        }
        "chat/completions" => {
            for text in &step.text {
                output.push(sse(json!({"id":"mock-chat","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":text},"finish_reason":null}]})));
            }
            for (index, tool) in step.tools.iter().enumerate() {
                output.push(sse(json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":index,"id":tool.id,"type":"function","function":{"name":tool.name,"arguments":tool.arguments.to_string()}}]},"finish_reason":null}]})));
            }
            if !step.hold {
                let reason = if step.tools.is_empty() {
                    "stop"
                } else {
                    "tool_calls"
                };
                output.push(sse(json!({"choices":[{"index":0,"delta":{},"finish_reason":reason}],"usage":{"prompt_tokens":input_tokens,"completion_tokens":2}})));
                output.push("data: [DONE]\n\n".into());
            }
        }
        "messages" => {
            output.push(sse(json!({"type":"message_start","message":{"id":"mock-message","type":"message","role":"assistant","content":[],"usage":usage}})));
            if !step.text.is_empty() {
                output.push(sse(json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})));
                for text in &step.text {
                    output.push(sse(json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}})));
                }
                output.push(sse(json!({"type":"content_block_stop","index":0})));
            }
            for (index, tool) in step.tools.iter().enumerate() {
                output.push(sse(json!({"type":"content_block_start","index":index+1,"content_block":{"type":"tool_use","id":tool.id,"name":tool.name,"input":{}}})));
                output.push(sse(json!({"type":"content_block_delta","index":index+1,"delta":{"type":"input_json_delta","partial_json":tool.arguments.to_string()}})));
                output.push(sse(json!({"type":"content_block_stop","index":index+1})));
            }
            if !step.hold {
                let reason = if step.tools.is_empty() {
                    "end_turn"
                } else {
                    "tool_use"
                };
                output.push(sse(json!({"type":"message_delta","delta":{"stop_reason":reason},"usage":{"output_tokens":2}})));
                output.push(sse(json!({"type":"message_stop"})));
            }
        }
        _ => return None,
    }
    Some(output)
}
