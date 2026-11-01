use super::{ModelEvent, Result, State, Value, invalid, string, text};
use crate::protocol::present;

#[derive(serde::Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    candidates: [Candidate<'a>; 1],
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
    if let Some(id) = present(value.get("responseId")) {
        state.budget.member(&mut state.metadata, "id", id)?;
    }
    if present(value["promptFeedback"].get("blockReason"))
        .is_some_and(|reason| reason != "BLOCK_REASON_UNSPECIFIED")
    {
        return Err(invalid("Gemini prompt blocked"));
    }
    let Some(candidates) = present(value.get("candidates")) else {
        return Ok(Vec::new());
    };
    let candidates = candidates.as_array().ok_or_else(|| {
        invalid("Gemini candidate decode failed; recovery: return an array of candidates")
    })?;
    let Some(candidate) = candidates.first() else {
        return Ok(Vec::new());
    };
    // The request uses the native single-candidate default. Silently taking
    // the first of several candidates would discard calls and signatures.
    if candidates.len() != 1
        || !candidate.is_object()
        || present(candidate.get("index")).is_some_and(|index| index.as_u64() != Some(0))
    {
        return Err(invalid(
            "Gemini candidate selection failed; recovery: return the single requested candidate at index 0",
        ));
    }
    let content = present(candidate.get("content"));
    if content.is_some_and(|content| {
        !content.is_object()
            || present(content.get("role")).is_some_and(|role| role != "model")
            || present(content.get("parts")).is_some_and(|parts| !parts.is_array())
    }) || present(candidate.get("finishReason")).is_some_and(|reason| !reason.is_string())
    {
        return Err(invalid(
            "Gemini candidate content decode failed; recovery: return model content with an array of parts and a string finishReason",
        ));
    }
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
            // Value decoding cannot retain duplicate executable fields. Admit
            // the original tokens for unsigned calls and native replay alike.
            crate::protocol::gemini::native_part(raw)?;
            text(&mut events, &part["text"], part["thought"] == true);
            let position = (state.calls.len() + state.thinking.len() + state.texts.len()) as u64;
            // A signature authenticates its exact part: keep the original
            // bytes on that part, whatever kind it is.
            let signed = part["thoughtSignature"].is_string().then(|| raw.get());
            if part["thought"] == true {
                state.raw_part(position, part, Some(raw.get()))?;
            } else if let Some(function) = present(part.get("functionCall")) {
                let (call, budget) = state.call(position)?;
                if let Some(id) = function.get("id") {
                    budget.string(&mut call.id, string(id)?)?;
                }
                budget.string(&mut call.name, string(&function["name"])?)?;
                if let Some(signed) = signed {
                    budget.string(&mut call.native, signed)?;
                }
                match function.get("args") {
                    None => budget.string(&mut call.arguments, "{}")?,
                    Some(args) if args.is_object() => budget.encoded(&mut call.arguments, args)?,
                    Some(_) => return Err(invalid("tool arguments must be a JSON object")),
                }
            } else {
                state.gemini_text(position, part["text"].as_str().unwrap_or_default(), signed)?;
            }
        }
    }
    if let Some(reason) = candidate["finishReason"]
        .as_str()
        .filter(|reason| *reason != "FINISH_REASON_UNSPECIFIED")
    {
        if !matches!(reason, "STOP" | "MAX_TOKENS") {
            return Err(invalid("Gemini generation was blocked"));
        }
        if reason == "MAX_TOKENS" && !state.calls.is_empty() {
            return Err(invalid(
                "Gemini generation ended before completing its tool batch",
            ));
        }
        state.finish_reason(if reason == "MAX_TOKENS" {
            "length"
        } else {
            reason
        })?;
        state.terminal = true;
    }
    Ok(events)
}

impl State<'_> {
    /// A signed text part stays its own part; unsigned text joins the
    /// unsigned text part right before it.
    pub(super) fn gemini_text(
        &mut self,
        position: u64,
        text: &str,
        signed: Option<&str>,
    ) -> Result<()> {
        let index = match self.texts.last_key_value() {
            Some((last, part))
                if signed.is_none() && *last + 1 == position && part.native.is_empty() =>
            {
                *last
            }
            _ if signed.is_none() && text.is_empty() => return Ok(()),
            _ => position,
        };
        let (part, budget) = self.text_part(index)?;
        budget.append_str(&mut part.text, text)?;
        if let Some(signed) = signed {
            budget.string(&mut part.native, signed)?;
        }
        Ok(())
    }
}
