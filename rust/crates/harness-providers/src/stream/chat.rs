use super::{ModelEvent, Result, State, Value, index, invalid, string, text};
use crate::protocol::present;

pub(super) fn parse(state: &mut State, value: &Value) -> Result<Vec<ModelEvent>> {
    state.identity(value)?;
    state.usage(&value["usage"])?;
    let Some(choice) = value["choices"]
        .as_array()
        .and_then(|values| values.first())
    else {
        return Ok(Vec::new());
    };
    if let Some(reason) = choice["finish_reason"].as_str() {
        if matches!(reason, "content_filter" | "error") {
            return Err(invalid("chat generation was blocked"));
        }
        state.finish_reason(reason)?;
        state.terminal = true;
    }
    let delta = &choice["delta"];
    let mut events = Vec::new();
    text(&mut events, &delta["content"], false);
    let reasoning =
        present(delta.get("reasoning_content")).or_else(|| present(delta.get("reasoning")));
    if let Some(reasoning) = reasoning {
        text(&mut events, reasoning, true);
        state.thinking_delta(0, "text", reasoning)?;
    }
    if let Some(calls) = delta["tool_calls"].as_array() {
        for raw in calls {
            let index = index(raw, "index")?;
            if present(raw.get("type")).is_some_and(|kind| kind != "function") {
                return Err(invalid("unsupported chat tool type"));
            }
            let (call, budget) = state.call(index)?;
            if let Some(id) = present(raw.get("id")) {
                let id = string(id)?;
                if !call.id.is_empty() && call.id != id {
                    return Err(invalid("tool call ID changed"));
                }
                budget.string(&mut call.id, id)?;
            }
            if let Some(function) = present(raw.get("function")) {
                if !function.is_object() {
                    return Err(invalid("tool function must be an object"));
                }
                budget.append(&mut call.name, function.get("name"))?;
                budget.append(&mut call.arguments, function.get("arguments"))?;
            }
        }
    }
    Ok(events)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "chat_properties.rs"]
mod properties;
