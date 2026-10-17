use super::wire::Item;
use acyclic_harness::model::ModelRole;
use serde::Deserialize;
use serde_json::{Value, value::RawValue};
use std::{borrow::Cow, collections::VecDeque};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Part<'a> {
    #[serde(default, borrow)]
    text: Cow<'a, str>,
    thought: Option<bool>,
    #[serde(borrow)]
    thought_signature: Option<&'a RawValue>,
    function_call: Option<serde::de::IgnoredAny>,
}

// GEM-THK-01/04: native visible parts own their original signature boundaries.
// The final assistant answer also contains their streamed text. Consume that
// duplicate prefix across text chunks and tool steps, without merging parts.
#[derive(Default)]
pub(super) struct Replay<'a> {
    pub signature: Option<Value>,
    visible: VecDeque<Cow<'a, str>>,
    offset: usize,
}

impl<'a> Replay<'a> {
    pub fn begin(&mut self, role: ModelRole) {
        if role != ModelRole::Assistant {
            self.signature = None;
        }
        if role == ModelRole::User {
            self.visible.clear();
            self.offset = 0;
        }
    }

    pub fn native(&mut self, bytes: &'a [u8], suppress_display: bool) -> Option<Item<'a>> {
        let raw: &RawValue = serde_json::from_slice(bytes).ok()?;
        let part: Part<'_> = serde_json::from_str(raw.get()).ok()?;
        if part.function_call.is_some() {
            self.signature = part
                .thought_signature
                .and_then(|signature| crate::json(signature.get().as_bytes()).ok());
            return None;
        }
        self.signature = None;
        if suppress_display && part.thought != Some(true) && !part.text.is_empty() {
            self.visible.push_back(part.text);
        }
        Some(Item::Native(raw))
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
pub(crate) fn assert_visible_replay(events: &[acyclic_harness::model::ModelEvent]) {
    use acyclic_harness::model::ModelEvent;
    let mut replay = Replay::default();
    let mut native_text = String::new();
    for event in events {
        if let ModelEvent::ReasoningBlock { block } = event {
            if let Some(item) = replay.native(
                &block.continuity,
                block.outcome == acyclic_harness::model::ReasoningOutcome::SuppressedDisplay,
            ) {
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
