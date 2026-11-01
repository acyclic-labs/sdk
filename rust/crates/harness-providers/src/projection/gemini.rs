use serde::Deserialize;
use serde_json::{Value, value::RawValue};
use std::borrow::Cow;

#[derive(Deserialize)]
struct Text<'a>(#[serde(borrow)] Cow<'a, str>);

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn ignored_nested_values_still_require_valid_native_strings(surrogate in 0xd800u16..=0xdfff) {
            let raw = format!(r#"{{"functionCall":{{"name":"read","args":{{"nested":["\u{surrogate:04x}"]}}}},"thoughtSignature":"original=="}}"#);
            // Negative control: skipping functionCall alone accepts malformed
            // nested string tokens. Native replay must validate the whole part.
            prop_assert!(serde_json::from_str::<crate::protocol::gemini::Part<&RawValue>>(&raw).is_ok());
            prop_assert!(part(raw.as_bytes()).is_err());
        }
    }
}

// GEM-THK-01/04: every signed part keeps its original bytes on its own
// canonical part (text, call or thought), so replay needs no retained state.

/// One retained part's exact bytes, validated without re-encoding.
pub(crate) fn part(
    bytes: &[u8],
) -> acyclic_harness_core::Result<(&RawValue, crate::protocol::gemini::NativePart<'_>)> {
    let raw: &RawValue = serde_json::from_slice(bytes)
        .map_err(|_| crate::invalid("invalid retained Gemini part"))?;
    if raw.get().as_bytes() != bytes || !crate::protocol::strict::valid(raw.get()) {
        return Err(crate::invalid("invalid retained Gemini part"));
    }
    Ok((raw, crate::protocol::gemini::native_part(raw)?))
}

fn decode<'a>(raw: &'a RawValue) -> acyclic_harness_core::Result<Cow<'a, str>> {
    let Text(text): Text<'a> = serde_json::from_str(raw.get())
        .map_err(|_| crate::invalid("invalid retained Gemini part"))?;
    Ok(text)
}

/// A retained thought: any part but a call.
pub fn thought(bytes: &[u8]) -> acyclic_harness_core::Result<&RawValue> {
    match part(bytes)? {
        (raw, crate::protocol::gemini::NativePart { function: None, .. }) => Ok(raw),
        _ => Err(crate::invalid("retained Gemini thought carries a call")),
    }
}

/// A retained visible text part, which must carry exactly its canonical text.
pub fn text<'a>(bytes: &'a [u8], text: &str) -> acyclic_harness_core::Result<&'a RawValue> {
    let (raw, native) = part(bytes)?;
    let thought = native
        .fields
        .thought
        .is_some_and(|thought| thought.get() == "true");
    let retained = native.fields.text.map(decode).transpose()?;
    if native.function.is_some() || thought || retained.as_deref().unwrap_or_default() != text {
        return Err(crate::invalid(
            "retained Gemini text differs from its canonical text",
        ));
    }
    Ok(raw)
}

/// A retained signed call, which must carry exactly its canonical call.
pub fn call<'a>(
    bytes: &'a [u8],
    id: Option<&str>,
    name: &str,
    arguments: &Value,
) -> acyclic_harness_core::Result<&'a RawValue> {
    let (raw, native) = part(bytes)?;
    let Some(original) = native.function else {
        return Err(crate::invalid("retained Gemini call carries no call"));
    };
    let same_arguments = match original.args {
        Some(raw) => crate::json(raw.get().as_bytes())? == *arguments,
        None => arguments.as_object().is_some_and(|args| args.is_empty()),
    };
    if original.id.map(decode).transpose()?.as_deref() != id
        || decode(original.name)? != name
        || !same_arguments
    {
        return Err(crate::invalid(
            "retained Gemini call differs from its canonical call",
        ));
    }
    Ok(raw)
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(crate) fn assert_visible_replay(events: &[acyclic_harness_core::model::ModelEvent]) {
    use acyclic_harness_core::model::ModelEvent;
    let mut parts = String::new();
    let mut streamed = String::new();
    for event in events {
        match event {
            ModelEvent::Content { delta } => streamed.push_str(delta),
            ModelEvent::TextPart {
                text: visible,
                native,
            } => {
                if let Some(native) = native {
                    text(&native.continuity, visible).unwrap();
                }
                parts.push_str(visible);
            }
            ModelEvent::ToolCall(tool) => {
                if let Some(native) = &tool.native {
                    let id = tool.metadata["provider_call"]["id"].as_str();
                    call(&native.continuity, id, &tool.name, &tool.arguments).unwrap();
                }
            }
            ModelEvent::ReasoningBlock { block } => {
                thought(&block.continuity).unwrap();
            }
            _ => {}
        }
    }
    assert_eq!(
        parts, streamed,
        "text parts must carry each streamed byte once"
    );
}
