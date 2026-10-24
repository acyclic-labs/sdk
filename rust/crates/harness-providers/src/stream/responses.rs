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
            if value["type"] == "response.output_item.done" && item["type"] == "message" {
                state.complete_message(index(value, "output_index")?, item)?;
            }
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
            if let Some(output) = response.get("output") {
                let output = output
                    .as_array()
                    .ok_or_else(|| invalid("response output must be an array"))?;
                if let Some((_, text)) = &mut state.completed_message {
                    state.budget.string(text, "")?;
                }
                state.completed_message = Some((0, String::new()));
                for (position, item) in output.iter().enumerate() {
                    if item["type"] == "message" {
                        state.complete_message(
                            u64::try_from(position)
                                .map_err(|_| invalid("response output index overflow"))?,
                            item,
                        )?;
                    }
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

impl State<'_> {
    fn complete_message(&mut self, position: u64, item: &Value) -> Result<()> {
        if item["role"] != "assistant" {
            return Err(invalid("response message must be assistant-authored"));
        }
        if self
            .completed_message
            .as_ref()
            .is_some_and(|(last, _)| *last > position)
        {
            return Ok(());
        }
        let content = item["content"]
            .as_array()
            .ok_or_else(|| invalid("response message content must be an array"))?;
        let (last, text) = self
            .completed_message
            .get_or_insert_with(|| (position, String::new()));
        self.budget.string(text, "")?;
        for part in content {
            let value = match part["type"].as_str() {
                Some("output_text") => &part["text"],
                Some("refusal") => &part["refusal"],
                _ => return Err(invalid("unsupported assistant message content")),
            };
            self.budget.append_str(
                text,
                value
                    .as_str()
                    .ok_or_else(|| invalid("assistant message text must be a string"))?,
            )?;
        }
        *last = position;
        Ok(())
    }
}
