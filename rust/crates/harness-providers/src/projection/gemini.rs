use super::wire::Item;
use acyclic_harness_core::model::ModelRole;
use serde::Deserialize;
use serde_json::{Value, value::RawValue};
use std::{borrow::Cow, collections::VecDeque};

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
            prop_assert!(Replay::default().native(raw.as_bytes(), false).is_err());
        }
    }
}

// GEM-THK-01/04: native visible parts own their original signature boundaries.
// The final assistant answer also contains their streamed text. Consume that
// duplicate prefix across text chunks and tool steps, without merging parts.
#[derive(Default)]
pub(super) struct Replay<'a> {
    call: Option<(&'a RawValue, crate::protocol::gemini::Function<'a>)>,
    visible: VecDeque<Cow<'a, str>>,
    offset: usize,
}

impl<'a> Replay<'a> {
    pub fn begin(&mut self, role: ModelRole) -> acyclic_harness_core::Result<()> {
        if role != ModelRole::Assistant {
            self.finish()?;
        }
        if role == ModelRole::User {
            self.visible.clear();
            self.offset = 0;
        }
        Ok(())
    }

    pub fn finish(&self) -> acyclic_harness_core::Result<()> {
        if self.call.is_some() {
            return Err(crate::invalid("retained Gemini call has no canonical call"));
        }
        Ok(())
    }

    pub fn native(
        &mut self,
        bytes: &'a [u8],
        suppress_display: bool,
    ) -> acyclic_harness_core::Result<Option<Item<'a>>> {
        // One original part must be consumed by its immediately following
        // canonical call. Never overwrite, drop or reorder signed calls.
        self.finish()?;
        let raw: &RawValue = serde_json::from_slice(bytes)
            .map_err(|_| crate::invalid("invalid retained Gemini part"))?;
        if raw.get().as_bytes() != bytes || !crate::protocol::strict::valid(raw.get()) {
            return Err(crate::invalid("invalid retained Gemini part"));
        }
        let crate::protocol::gemini::NativePart {
            fields: part,
            function,
        } = crate::protocol::gemini::native_part(raw)?;
        if let Some(function) = function {
            // A signature authenticates this exact part, including the call.
            // Retain its bytes rather than reconstructing a signed envelope.
            self.call = Some((raw, function));
            return Ok(None);
        }
        self.call = None;
        if suppress_display && part.thought.is_none_or(|thought| thought.get() != "true") {
            if let Some(text) = part.text {
                let Text(text): Text<'a> = serde_json::from_str(text.get())
                    .map_err(|_| crate::invalid("invalid retained Gemini text"))?;
                if !text.is_empty() {
                    self.visible.push_back(text);
                }
            }
        }
        Ok(Some(Item::Native(raw)))
    }

    /// Binds the executable call to its exact original signed part. Validation
    /// never changes the bytes used for native replay.
    pub fn take_call(
        &mut self,
        id: Option<&str>,
        name: &str,
        arguments: &Value,
    ) -> acyclic_harness_core::Result<Option<&'a RawValue>> {
        let Some((_, original)) = self.call else {
            return Ok(None);
        };
        let decode = |raw: &'a RawValue| -> acyclic_harness_core::Result<Cow<'a, str>> {
            let Text(text): Text<'a> = serde_json::from_str(raw.get())
                .map_err(|_| crate::invalid("invalid retained Gemini call"))?;
            Ok(text)
        };
        if original.id.map(decode).transpose()?.as_deref() != id || decode(original.name)? != name {
            return Err(crate::invalid(
                "retained Gemini call differs from its canonical call",
            ));
        }
        let same_arguments = match original.args {
            Some(raw) => crate::json(raw.get().as_bytes())? == *arguments,
            None => arguments.as_object().is_some_and(|args| args.is_empty()),
        };
        if !same_arguments {
            return Err(crate::invalid(
                "retained Gemini call differs from its canonical call",
            ));
        }
        Ok(self.call.take().map(|(raw, _)| raw))
    }

    pub fn remaining<'text>(&mut self, mut text: &'text str) -> &'text str {
        while !text.is_empty() {
            let Some(part) = self.visible.front() else {
                break;
            };
            let pending = &part[self.offset..];
            let count = pending.len().min(text.len());
            if pending.as_bytes()[..count] != text.as_bytes()[..count] {
                self.visible.clear();
                self.offset = 0;
                break;
            }
            // Matching UTF-8 prefixes end at the shorter string's boundary.
            text = &text[count..];
            self.offset += count;
            if self.offset == part.len() {
                self.visible.pop_front();
                self.offset = 0;
            }
        }
        text
    }
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(crate) fn assert_visible_replay(events: &[acyclic_harness_core::model::ModelEvent]) {
    use acyclic_harness_core::model::ModelEvent;
    use serde_json::Value;
    let mut replay = Replay::default();
    let mut native_text = String::new();
    for event in events {
        if matches!(event, ModelEvent::ToolCall(_)) {
            replay.call.take();
        }
        if let ModelEvent::ReasoningBlock { block } = event {
            if let Some(item) = replay
                .native(
                    &block.continuity,
                    block.outcome
                        == acyclic_harness_core::model::ReasoningOutcome::SuppressedDisplay,
                )
                .unwrap()
            {
                let bytes = serde_json::to_vec(&item).unwrap();
                assert_eq!(bytes, block.continuity, "native part bytes changed");
                let part: Value = crate::projection::arguments::literal(&bytes).unwrap();
                if part["thought"] != true {
                    if let Some(text) = part["text"].as_str() {
                        native_text.push_str(text);
                    }
                }
            }
        }
    }
    replay.finish().unwrap();
    let visible: String = events
        .iter()
        .filter_map(|event| match event {
            ModelEvent::Content { delta } => Some(delta.as_str()),
            _ => None,
        })
        .collect();
    for event in events {
        if let ModelEvent::Content { delta } = event {
            native_text.push_str(replay.remaining(delta));
        }
    }
    assert_eq!(
        native_text, visible,
        "native parts and answer must replay each byte once"
    );
}
