use super::{ModelEvent, Result, State, Value, index, invalid, string, text};

pub(super) fn parse(state: &mut State, value: &Value, data: &str) -> Result<Vec<ModelEvent>> {
    let mut events = Vec::new();
    match value["type"].as_str() {
        Some("response.output_text.delta" | "response.refusal.delta") => {
            text(&mut events, &value["delta"], false)
        }
        Some("response.reasoning_summary_text.delta" | "response.reasoning_text.delta") => {
            text(&mut events, &value["delta"], true)
        }
        Some("response.output_item.added" | "response.output_item.done") => {
            let item = &value["item"];
            if item["type"] == "reasoning" {
                #[derive(serde::Deserialize)]
                struct Event<'a> {
                    #[serde(borrow)]
                    item: &'a serde_json::value::RawValue,
                }
                let event: Event<'_> =
                    serde_json::from_str(data).map_err(|_| invalid("invalid reasoning item"))?;
                state.raw_part(
                    index(value, "output_index")?,
                    item,
                    Some(event.item.get()),
                    false,
                )?;
            }
            if item["type"] == "function_call" {
                let (call, budget) = state.call(index(value, "output_index")?)?;
                budget.string(&mut call.id, string(&item["call_id"])?)?;
                budget.string(&mut call.name, string(&item["name"])?)?;
                if let Some(arguments) = item["arguments"].as_str() {
                    budget.string(&mut call.arguments, arguments)?;
                }
            }
        }
        Some("response.function_call_arguments.delta") => {
            let index = index(value, "output_index")?;
            if !state.calls.contains_key(&index) {
                return Err(invalid("tool delta without item"));
            }
            let (call, budget) = state.call(index)?;
            budget.append(&mut call.arguments, value.get("delta"))?;
        }
        Some("response.function_call_arguments.done") => {
            let index = index(value, "output_index")?;
            if !state.calls.contains_key(&index) {
                return Err(invalid("tool done without item"));
            }
            let (call, budget) = state.call(index)?;
            budget.string(
                &mut call.arguments,
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
