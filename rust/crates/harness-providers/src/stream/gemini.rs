use super::{ModelEvent, Result, State, Value, invalid, string, text};

#[derive(serde::Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    candidates: Vec<Candidate<'a>>,
}
#[derive(serde::Deserialize)]
struct Candidate<'a> {
    #[serde(borrow)]
    content: Option<Content<'a>>,
}
#[derive(serde::Deserialize)]
struct Content<'a> {
    #[serde(default, borrow)]
    parts: Vec<&'a serde_json::value::RawValue>,
}

pub(super) fn parse(state: &mut State, value: &Value, data: &str) -> Result<Vec<ModelEvent>> {
    state.usage(&value["usageMetadata"])?;
    if let Some(id) = value.get("responseId") {
        state.budget.member(&mut state.metadata, "id", id)?;
    }
    if value["promptFeedback"].get("blockReason").is_some() {
        return Err(invalid("Gemini prompt blocked"));
    }
    let Some(candidate) = value["candidates"]
        .as_array()
        .and_then(|values| values.first())
    else {
        return Ok(Vec::new());
    };
    let mut events = Vec::new();
    if let Some(parts) = candidate["content"]["parts"].as_array() {
        if state.terminal && !parts.is_empty() {
            return Err(invalid("Gemini content after terminal marker"));
        }
        let raw: Envelope<'_> =
            serde_json::from_str(data).map_err(|_| invalid("invalid Gemini content parts"))?;
        let raw = &raw
            .candidates
            .first()
            .and_then(|candidate| candidate.content.as_ref())
            .ok_or_else(|| invalid("missing Gemini content"))?
            .parts;
        for (part, raw) in parts.iter().zip(raw) {
            if part
                .get("thought")
                .is_some_and(|value| !value.is_null() && !value.is_boolean())
                || part
                    .get("thoughtSignature")
                    .is_some_and(|value| !value.is_null() && !value.is_string())
                || (part["text"].is_string() && part.get("functionCall").is_some())
            {
                return Err(invalid("invalid Gemini content part"));
            }
            text(&mut events, &part["text"], part["thought"] == true);
            let position = (state.calls.len() + state.thinking.len()) as u64;
            if part["thought"] == true || part["thoughtSignature"].is_string() {
                state.raw_part(
                    position,
                    part,
                    Some(raw.get()),
                    part["thought"] != true && part["text"].is_string(),
                )?;
            } else if let Some(text) = part["text"].as_str() {
                state.visible_text(text)?;
            }
            if let Some(function) = part.get("functionCall") {
                let index = (state.calls.len() + state.thinking.len()) as u64;
                let (call, budget) = state.call(index)?;
                if let Some(id) = function["id"].as_str() {
                    budget.string(&mut call.id, id)?;
                }
                budget.string(&mut call.name, string(&function["name"])?)?;
                match function.get("args") {
                    None => budget.string(&mut call.arguments, "{}")?,
                    Some(args) if args.is_object() => budget.encoded(&mut call.arguments, args)?,
                    Some(_) => return Err(invalid("tool arguments must be a JSON object")),
                }
            }
        }
    }
    if let Some(reason) = candidate["finishReason"].as_str() {
        if !matches!(reason, "STOP" | "MAX_TOKENS") {
            return Err(invalid("Gemini generation was blocked"));
        }
        state.finish_reason(if reason == "MAX_TOKENS" {
            "length"
        } else {
            reason
        })?;
        state.discard_unsigned_final_text()?;
        state.terminal = true;
    }
    Ok(events)
}
