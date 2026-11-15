use crate::{Cached, Step};
use serde_json::{Map, Value, json};

fn sse(value: &Value) -> String {
    let event = value
        .get("type")
        .and_then(Value::as_str)
        .map(|name| format!("event: {name}\n"))
        .unwrap_or_default();
    format!("{event}data: {value}\n\n")
}

pub(crate) fn events(
    path: &str,
    step: &Step,
    input_tokens: u64,
    cached: Cached,
    completed_text: Option<&str>,
) -> Option<Vec<String>> {
    let mut output = Vec::new();
    // Cache tokens are part of the request's input; never more than it.
    let read = cached.read.min(input_tokens);
    let write = cached.write.min(input_tokens - read);
    let total_tokens = input_tokens.checked_add(2)?;
    if step.gemini.is_some() && !gemini_path(path) {
        return None;
    }
    if step
        .gemini
        .as_ref()
        .is_some_and(|gemini| gemini.cached_tokens > input_tokens)
    {
        return None;
    }
    let mut usage = Map::new();
    usage.insert("input_tokens".into(), json!(input_tokens));
    usage.insert("output_tokens".into(), json!(2));
    usage.insert("total_tokens".into(), json!(total_tokens));
    if read > 0 && path == "responses" {
        usage.insert("input_tokens_details".into(), json!({"cached_tokens":read}));
    }
    if path != "responses"
        && (!step.custom_tools.is_empty() || step.tools.iter().any(|tool| tool.namespace.is_some()))
    {
        return None;
    }
    match path {
        "responses" => {
            responses(step, &usage, completed_text, &mut output);
        }
        "chat/completions" => {
            for text in &step.text {
                output.push(sse(&json!({"id":"mock-chat","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":text},"finish_reason":null}]})));
            }
            for (index, tool) in step.tools.iter().enumerate() {
                output.push(sse(&json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":index,"id":tool.id,"type":"function","function":{"name":tool.name,"arguments":tool.arguments.to_string()}}]},"finish_reason":null}]})));
            }
            if !step.hold {
                let reason = if step.tools.is_empty() {
                    "stop"
                } else {
                    "tool_calls"
                };
                let mut usage = Map::new();
                usage.insert("prompt_tokens".into(), json!(input_tokens));
                usage.insert("completion_tokens".into(), json!(2));
                usage.insert("total_tokens".into(), json!(total_tokens));
                if read > 0 || write > 0 {
                    usage.insert(
                        "prompt_tokens_details".into(),
                        json!({"cached_tokens":read,"cache_write_tokens":write}),
                    );
                }
                output.push(sse(&json!({"choices":[{"index":0,"delta":{},"finish_reason":reason}],"usage":usage})));
                output.push("data: [DONE]\n\n".into());
            }
        }
        "messages" => {
            if read > 0 || write > 0 {
                // Anthropic's input_tokens is only the uncached remainder.
                usage.insert("input_tokens".into(), json!(input_tokens - read - write));
                usage.insert("cache_read_input_tokens".into(), json!(read));
                usage.insert("cache_creation_input_tokens".into(), json!(write));
            }
            output.push(sse(&json!({"type":"message_start","message":{"id":"mock-message","type":"message","role":"assistant","content":[],"usage":usage}})));
            if !step.text.is_empty() {
                output.push(sse(&json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})));
                for text in &step.text {
                    output.push(sse(&json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}})));
                }
                output.push(sse(&json!({"type":"content_block_stop","index":0})));
            }
            for (index, tool) in step.tools.iter().enumerate() {
                output.push(sse(&json!({"type":"content_block_start","index":index+1,"content_block":{"type":"tool_use","id":tool.id,"name":tool.name,"input":{}}})));
                output.push(sse(&json!({"type":"content_block_delta","index":index+1,"delta":{"type":"input_json_delta","partial_json":tool.arguments.to_string()}})));
                output.push(sse(&json!({"type":"content_block_stop","index":index+1})));
            }
            if !step.hold {
                let reason = if step.tools.is_empty() {
                    "end_turn"
                } else {
                    "tool_use"
                };
                output.push(sse(&json!({"type":"message_delta","delta":{"stop_reason":reason},"usage":{"output_tokens":2}})));
                output.push(sse(&json!({"type":"message_stop"})));
            }
        }
        path if gemini_path(path) => {
            let total_tokens = total_tokens.checked_add(
                step.gemini
                    .as_ref()
                    .map_or(0, |gemini| gemini.thought_tokens),
            )?;
            gemini(&mut output, step, input_tokens, read, total_tokens);
        }
        _ => return None,
    }
    Some(output)
}

fn responses(
    step: &Step,
    usage: &Map<String, Value>,
    completed_text: Option<&str>,
    output: &mut Vec<String>,
) {
    output.push(sse(
        &json!({"type":"response.created","response":{"id":"mock-response","status":"in_progress"}}),
    ));
    for text in &step.text {
        output.push(sse(&json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":text})));
    }
    let mut items = Vec::new();
    if (!step.text.is_empty() || completed_text.is_some()) && !step.hold {
        let text = completed_text.map_or_else(|| step.text.concat(), str::to_owned);
        let item = json!({"type":"message","id":"mock-message","role":"assistant",
            "content":[{"type":"output_text","text":text}]});
        output.push(sse(
            &json!({"type":"response.output_item.done","output_index":0,"item":item}),
        ));
        items.push(item);
    }
    let tool_offset = usize::from(!step.text.is_empty() || completed_text.is_some());
    for (index, tool) in step.tools.iter().enumerate() {
        let index = index + tool_offset;
        let arguments = tool.arguments.to_string();
        let id = format!("fc_{}", tool.id);
        let mut item = Map::new();
        item.insert("type".into(), json!("function_call"));
        item.insert("id".into(), json!(id));
        item.insert("call_id".into(), json!(tool.id));
        item.insert("name".into(), json!(tool.name));
        item.insert("arguments".into(), json!(""));
        if let Some(namespace) = &tool.namespace {
            item.insert("namespace".into(), json!(namespace));
        }
        output.push(sse(
            &json!({"type":"response.output_item.added","output_index":index,"item":item}),
        ));
        output.push(sse(&json!({"type":"response.function_call_arguments.delta","output_index":index,"item_id":id,"delta":arguments})));
        output.push(sse(&json!({"type":"response.function_call_arguments.done","output_index":index,"item_id":id,"arguments":arguments})));
        item.insert("arguments".into(), json!(arguments));
        output.push(sse(
            &json!({"type":"response.output_item.done","output_index":index,"item":item}),
        ));
        items.push(Value::Object(item));
    }
    for (index, tool) in step.custom_tools.iter().enumerate() {
        let index = step.tools.len() + index + tool_offset;
        let id = format!("ctc_{}", tool.id);
        let mut item = Map::new();
        item.insert("type".into(), json!("custom_tool_call"));
        item.insert("id".into(), json!(id));
        item.insert("call_id".into(), json!(tool.id));
        item.insert("name".into(), json!(tool.name));
        item.insert("input".into(), json!(""));
        if let Some(namespace) = &tool.namespace {
            item.insert("namespace".into(), json!(namespace));
        }
        output.push(sse(
            &json!({"type":"response.output_item.added","output_index":index,"item":item}),
        ));
        output.push(sse(&json!({"type":"response.custom_tool_call_input.delta","output_index":index,"item_id":id,"delta":tool.input})));
        output.push(sse(&json!({"type":"response.custom_tool_call_input.done","output_index":index,"item_id":id,"input":tool.input})));
        item.insert("input".into(), json!(tool.input));
        output.push(sse(
            &json!({"type":"response.output_item.done","output_index":index,"item":item}),
        ));
        items.push(Value::Object(item));
    }
    if !step.hold {
        output.push(sse(&json!({"type":"response.completed","response":{"id":"mock-response","status":"completed","usage":usage,"output":items}})));
    }
}

fn gemini_path(path: &str) -> bool {
    path.strip_prefix("models/")
        .and_then(|model| model.strip_suffix(":streamGenerateContent"))
        .is_some_and(|model| !model.is_empty() && !model.contains('/'))
}

fn gemini(output: &mut Vec<String>, step: &Step, input_tokens: u64, read: u64, total_tokens: u64) {
    for text in &step.text {
        output.push(sse(
            &json!({"candidates":[{"content":{"role":"model","parts":[{"text":text}]}}]}),
        ));
    }
    for tool in &step.tools {
        output.push(sse(&json!({"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"id":tool.id,"name":tool.name,"args":tool.arguments}}]}}]})));
    }
    if let Some(gemini) = &step.gemini
        && !gemini.parts.is_empty()
    {
        output.push(sse(
            &json!({"candidates":[{"content":{"role":"model","parts":gemini.parts}}]}),
        ));
    }
    if !step.hold {
        let mut usage = Map::new();
        usage.insert("promptTokenCount".into(), json!(input_tokens));
        usage.insert("candidatesTokenCount".into(), json!(2));
        usage.insert("totalTokenCount".into(), json!(total_tokens));
        if let Some(gemini) = &step.gemini {
            usage.insert(
                "cachedContentTokenCount".into(),
                json!(gemini.cached_tokens),
            );
            usage.insert("thoughtsTokenCount".into(), json!(gemini.thought_tokens));
        }
        if read > 0 && usage.get("cachedContentTokenCount").is_none_or(|n| n == 0) {
            usage.insert("cachedContentTokenCount".into(), json!(read));
        }
        output.push(sse(
            &json!({"candidates":[{"finishReason":"STOP"}],"usageMetadata":usage}),
        ));
    }
}
