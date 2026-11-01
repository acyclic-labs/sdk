use super::{ModelEvent, Result, State, Value, index, invalid, string, text};
use crate::protocol::present;

pub(super) fn parse(state: &mut State, value: &Value) -> Result<Vec<ModelEvent>> {
    let choices = present(value.get("choices"))
        .map(|choices| {
            choices
                .as_array()
                .ok_or_else(|| invalid("chat choices must be an array"))
        })
        .transpose()?;
    let choice = match choices.map(Vec::as_slice) {
        None | Some([]) => None,
        Some([choice]) if choice.is_object() => Some(choice),
        _ => return Err(invalid("chat stream requires one original candidate")),
    };
    if let Some(choice) = choice {
        if present(choice.get("index")).is_some_and(|index| index.as_u64() != Some(0)) {
            return Err(invalid("chat candidate index differs from selection"));
        }
        if state.terminal {
            return Err(invalid("chat candidate continued after completion"));
        }
    }
    state.identity(value)?;
    state.usage(&value["usage"])?;
    let Some(choice) = choice else {
        return Ok(Vec::new());
    };
    let delta = present(choice.get("delta"));
    if delta.is_some_and(|delta| !delta.is_object()) {
        return Err(invalid("chat delta must be an object"));
    }
    let delta = &choice["delta"];
    if present(delta.get("role")).is_some_and(|role| role != "assistant") {
        return Err(invalid("chat delta role must be assistant"));
    }
    if present(delta.get("content")).is_some_and(|content| !content.is_string()) {
        return Err(invalid("chat text delta must be a string"));
    }
    if present(delta.get("tool_calls")).is_some_and(|calls| !calls.is_array()) {
        return Err(invalid("chat tool calls must be an array"));
    }
    if let Some(reason) = present(choice.get("finish_reason")) {
        let reason = string(reason)?;
        if matches!(reason, "content_filter" | "error") {
            return Err(invalid("chat generation was blocked"));
        }
        state.finish_reason(reason)?;
        state.terminal = true;
    }
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
