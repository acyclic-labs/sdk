use super::{ModelEvent, Result, State, Value, index, invalid, string, text};

pub(super) fn parse(state: &mut State, value: &Value, data: &str) -> Result<Vec<ModelEvent>> {
    let mut events = Vec::new();
    match value["type"].as_str() {
        Some("message_start") => {
            state.identity(&value["message"])?;
            state.usage(&value["message"]["usage"])?;
        }
        Some("content_block_start") => {
            let block = &value["content_block"];
            match block["type"].as_str() {
                Some("tool_use") => {
                    let index = index(value, "index")?;
                    if state.calls.contains_key(&index) {
                        return Err(invalid("repeated tool block"));
                    }
                    let (call, budget) = state.call(index)?;
                    budget.string(&mut call.id, string(&block["id"])?)?;
                    budget.string(&mut call.name, string(&block["name"])?)?;
                    if block["input"]
                        .as_object()
                        .is_some_and(|map| !map.is_empty())
                    {
                        budget.encoded(&mut call.arguments, &block["input"])?;
                    }
                }
                Some("text") => text(&mut events, &block["text"], false),
                Some("thinking") => {
                    #[derive(serde::Deserialize)]
                    struct Event<'a> {
                        #[serde(borrow)]
                        content_block: &'a serde_json::value::RawValue,
                    }
                    let raw: Event<'_> = serde_json::from_str(data)
                        .map_err(|_| invalid("invalid thinking block"))?;
                    state.raw_part(
                        index(value, "index")?,
                        block,
                        Some(raw.content_block.get()),
                        false,
                    )?;
                    text(&mut events, &block["thinking"], true);
                }
                Some("redacted_thinking") => {
                    #[derive(serde::Deserialize)]
                    struct Event<'a> {
                        #[serde(borrow)]
                        content_block: &'a serde_json::value::RawValue,
                    }
                    let raw: Event<'_> = serde_json::from_str(data)
                        .map_err(|_| invalid("invalid thinking block"))?;
                    state.raw_part(
                        index(value, "index")?,
                        block,
                        Some(raw.content_block.get()),
                        false,
                    )?;
                }
                _ => {}
            }
        }
        Some("content_block_delta") => {
            let delta = &value["delta"];
            match delta["type"].as_str() {
                Some("text_delta") => text(&mut events, &delta["text"], false),
                Some("thinking_delta" | "signature_delta") => {
                    let key = if delta["type"] == "thinking_delta" {
                        "thinking"
                    } else {
                        "signature"
                    };
                    state.thinking_delta(index(value, "index")?, key, &delta[key])?;
                    if key == "thinking" {
                        text(&mut events, &delta["thinking"], true);
                    }
                }
                Some("input_json_delta") => {
                    let index = index(value, "index")?;
                    if !state.calls.contains_key(&index) {
                        return Err(invalid("tool delta without block"));
                    }
                    let (call, budget) = state.call(index)?;
                    budget.append(&mut call.arguments, delta.get("partial_json"))?;
                }
                _ => {}
            }
        }
        Some("content_block_stop") => {
            let index = index(value, "index")?;
            if state.calls.contains_key(&index) {
                let (call, budget) = state.call(index)?;
                if call.arguments.is_empty() {
                    budget.string(&mut call.arguments, "{}")?;
                }
            }
        }
        Some("message_delta") => {
            state.usage(&value["usage"])?;
            if let Some(reason) = value["delta"]["stop_reason"].as_str() {
                state.finish_reason(if reason == "max_tokens" {
                    "length"
                } else {
                    reason
                })?;
            }
        }
        Some("message_stop") => state.terminal = true,
        _ => {}
    }
    Ok(events)
}
