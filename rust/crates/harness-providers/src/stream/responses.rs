use super::{ModelEvent, Result, State, Value, index, invalid, string, text};

/// Only these output item kinds have a semantic path through this decoder.
/// Server tools require native item retention before they can run.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Output {
    Message,
    Reasoning,
    Function,
    Freeform,
}

impl Output {
    fn admit(item: &Value) -> Result<Self> {
        match item["type"].as_str() {
            Some("message") => Ok(Self::Message),
            Some("reasoning") => Ok(Self::Reasoning),
            Some("function_call" | "custom_tool_call") => {
                if crate::protocol::present(item.get("namespace")).is_some() {
                    return Err(invalid("namespaced Responses calls require native replay"));
                }
                Ok(if item["type"] == "custom_tool_call" {
                    Self::Freeform
                } else {
                    Self::Function
                })
            }
            _ => Err(invalid(
                "unsupported Responses output item requires native replay",
            )),
        }
    }
}

pub(super) fn parse(state: &mut State, value: &Value, data: &str) -> Result<Vec<ModelEvent>> {
    let mut events = Vec::new();
    match value["type"].as_str() {
        Some("response.output_text.delta" | "response.refusal.delta") => {
            text(&mut events, &value["delta"], false)
        }
        Some("response.reasoning_summary_text.delta") => text(&mut events, &value["delta"], true),
        Some("response.output_item.added" | "response.output_item.done") => {
            let item = &value["item"];
            let kind = Output::admit(item)?;
            if value["type"] == "response.output_item.done" && kind == Output::Message {
                state.complete_message(index(value, "output_index")?, item)?;
            }
            if kind == Output::Reasoning {
                #[derive(serde::Deserialize)]
                struct Event<'a> {
                    #[serde(borrow)]
                    item: &'a serde_json::value::RawValue,
                }
                let event: Event<'_> =
                    serde_json::from_str(data).map_err(|_| invalid("invalid reasoning item"))?;
                state.raw_part(index(value, "output_index")?, item, Some(event.item.get()))?;
            }
            if matches!(kind, Output::Function | Output::Freeform) {
                admit_call(
                    state,
                    index(value, "output_index")?,
                    item,
                    kind,
                    value["type"] == "response.output_item.done",
                )?;
            }
        }
        Some("response.custom_tool_call_input.delta" | "response.custom_tool_call_input.done") => {
            let index = index(value, "output_index")?;
            if !state.calls.get(&index).is_some_and(|call| call.freeform) {
                return Err(invalid("freeform tool input without custom item"));
            }
            validate_item_id(&state.calls[&index], value.get("item_id"))?;
            let (call, budget) = state.call(index)?;
            if value["type"] == "response.custom_tool_call_input.delta" {
                append_input(call, budget, &value["delta"])?;
            } else {
                complete_input(
                    call,
                    budget,
                    value["input"]
                        .as_str()
                        .ok_or_else(|| invalid("freeform tool input must be a string"))?,
                )?;
            }
        }
        Some("response.function_call_arguments.delta") => {
            let index = index(value, "output_index")?;
            if !state.calls.get(&index).is_some_and(|call| !call.freeform) {
                return Err(invalid("tool delta without item"));
            }
            validate_item_id(&state.calls[&index], value.get("item_id"))?;
            let (call, budget) = state.call(index)?;
            append_input(call, budget, &value["delta"])?;
        }
        Some("response.function_call_arguments.done") => {
            let index = index(value, "output_index")?;
            if !state.calls.get(&index).is_some_and(|call| !call.freeform) {
                return Err(invalid("tool done without item"));
            }
            validate_item_id(&state.calls[&index], value.get("item_id"))?;
            let (call, budget) = state.call(index)?;
            complete_input(
                call,
                budget,
                value["arguments"]
                    .as_str()
                    .ok_or_else(|| invalid("tool arguments must be a string"))?,
            )?;
        }
        Some("response.completed" | "response.incomplete") => {
            let response = &value["response"];
            if response["status"]
                .as_str()
                .is_some_and(|status| !matches!(status, "completed" | "incomplete"))
            {
                return Err(invalid("Responses generation did not complete"));
            }
            if let Some(output) = crate::protocol::present(response.get("output")) {
                let output = output
                    .as_array()
                    .ok_or_else(|| invalid("response output must be an array"))?;
                if let Some((_, text)) = &mut state.completed_message {
                    state.budget.string(text, "")?;
                }
                state.completed_message = Some((0, String::new()));
                let mut calls = 0usize;
                for (position, item) in output.iter().enumerate() {
                    let kind = Output::admit(item)?;
                    let position = u64::try_from(position)
                        .map_err(|_| invalid("response output index overflow"))?;
                    match kind {
                        Output::Message => state.complete_message(position, item)?,
                        Output::Function | Output::Freeform => {
                            admit_call(state, position, item, kind, true)?;
                            calls += 1;
                        }
                        Output::Reasoning => {}
                    }
                }
                if calls != state.calls.len() {
                    return Err(invalid("Responses terminal output omitted a staged call"));
                }
            }
            state.identity(response)?;
            state.usage(&response["usage"])?;
            if value["type"] == "response.incomplete" || response["status"] == "incomplete" {
                if response["incomplete_details"]["reason"] != "max_output_tokens" {
                    return Err(invalid("Responses generation was incomplete"));
                }
                state.finish_reason("length")?;
            }
            state.terminal = true;
        }
        _ => {}
    }
    Ok(events)
}

fn validate_item_id(call: &super::Call, value: Option<&Value>) -> Result<()> {
    if let Some(value) = crate::protocol::present(value) {
        let id = string(value)?;
        if call.item_id != id {
            return Err(invalid("Responses tool item identity changed"));
        }
    }
    Ok(())
}

/// Added, done and authoritative terminal items share one identity admission.
/// A later frame may complete input, but cannot replace the original callable.
fn admit_call(
    state: &mut State,
    index: u64,
    item: &Value,
    kind: Output,
    complete: bool,
) -> Result<()> {
    let id = string(&item["call_id"])?;
    let name = string(&item["name"])?;
    if id.trim().is_empty() || name.trim().is_empty() {
        return Err(invalid("incomplete Responses tool identity"));
    }
    let arguments = item
        .get(if kind == Output::Freeform {
            "input"
        } else {
            "arguments"
        })
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| invalid("Responses tool input must be a string"))
        })
        .transpose()?;
    if complete && arguments.is_none() {
        return Err(invalid(
            "Responses tool completion omitted input; recovery: return its complete input string",
        ));
    }
    if let Some(call) = state.calls.get(&index) {
        if call.freeform != (kind == Output::Freeform) {
            return Err(invalid("Responses tool kind changed"));
        }
        if call.id != id || call.name != name {
            return Err(invalid("Responses tool identity changed"));
        }
        validate_item_id(call, item.get("id"))?;
        if call.input_complete && arguments.is_some_and(|input| input != call.arguments) {
            return Err(invalid(
                "Responses completed tool input changed; recovery: preserve its original complete input",
            ));
        }
    } else if let Some(id) = crate::protocol::present(item.get("id")) {
        if string(id)?.is_empty() {
            return Err(invalid("incomplete Responses tool item identity"));
        }
    }
    let (call, budget) = state.call(index)?;
    call.freeform = kind == Output::Freeform;
    budget.string(&mut call.id, id)?;
    budget.string(&mut call.name, name)?;
    if let Some(id) = crate::protocol::present(item.get("id")) {
        budget.string(&mut call.item_id, string(id)?)?;
    }
    if let Some(arguments) = arguments.filter(|_| !call.input_complete) {
        budget.string(&mut call.arguments, arguments)?;
    }
    call.input_complete |= complete;
    Ok(())
}

fn append_input(
    call: &mut super::Call,
    budget: &mut super::budget::Budget<'_>,
    delta: &Value,
) -> Result<()> {
    if call.input_complete {
        return Err(invalid(
            "Responses tool delta followed completed input; recovery: end deltas before completing input",
        ));
    }
    budget.append_str(
        &mut call.arguments,
        delta.as_str().ok_or_else(|| {
            invalid("Responses tool delta decode failed; recovery: return a string delta")
        })?,
    )
}

fn complete_input(
    call: &mut super::Call,
    budget: &mut super::budget::Budget<'_>,
    input: &str,
) -> Result<()> {
    if call.input_complete {
        return if call.arguments == input {
            Ok(())
        } else {
            Err(invalid(
                "Responses completed tool input changed; recovery: preserve its original complete input",
            ))
        };
    }
    budget.string(&mut call.arguments, input)?;
    call.input_complete = true;
    Ok(())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "responses_properties.rs"]
mod properties;

impl State<'_> {
    fn complete_message(&mut self, position: u64, item: &Value) -> Result<()> {
        if item["role"] != "assistant" {
            return Err(invalid("response message must be assistant-authored"));
        }
        let content = item["content"]
            .as_array()
            .ok_or_else(|| invalid("response message content must be an array"))?;
        let (part, budget) = self.text_part(position)?;
        budget.string(&mut part.text, "")?;
        for value in content {
            let value = match value["type"].as_str() {
                Some("output_text") => &value["text"],
                Some("refusal") => &value["refusal"],
                _ => return Err(invalid("unsupported assistant message content")),
            };
            budget.append_str(
                &mut part.text,
                value
                    .as_str()
                    .ok_or_else(|| invalid("assistant message text must be a string"))?,
            )?;
        }
        if self
            .completed_message
            .as_ref()
            .is_some_and(|(last, _)| *last > position)
        {
            return Ok(());
        }
        let (last, text) = self
            .completed_message
            .get_or_insert_with(|| (position, String::new()));
        self.budget.string(text, &self.texts[&position].text)?;
        *last = position;
        Ok(())
    }
}
