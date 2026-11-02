mod anthropic;
mod budget;
mod chat;
mod error;
pub(crate) use error::context_overflow_bytes;
mod gemini;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod limits_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod message_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod optional_properties;
mod reasoning;
mod responses;
pub(crate) use reasoning::family;

use crate::{Adapter, invalid, json, sse};
use acyclic_harness_core::{
    Result,
    conversation::validate_portable_json,
    model::{ModelDispatch, ModelEvent},
};
use acyclic_stream::BoxProviderStream;
use bytes::Bytes;
use futures::StreamExt as _;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(crate) struct Call {
    id: String,
    item_id: String,
    name: String,
    arguments: String,
    /// The exact original call part, when the provider signs it.
    native: String,
    freeform: bool,
    // Responses completion seals input across its delta, item and terminal views.
    input_complete: bool,
}

/// One visible text part and, when the provider signs it, its exact bytes.
#[derive(Default)]
pub(crate) struct Text {
    text: String,
    native: String,
}

pub(crate) struct State<'work> {
    calls: BTreeMap<u64, Call>,
    texts: BTreeMap<u64, Text>,
    metadata: serde_json::Map<String, Value>,
    pub(crate) terminal: bool,
    completed_message: Option<(u64, String)>,
    thinking: BTreeMap<u64, reasoning::Thinking>,
    origin: Option<acyclic_harness_core::model::ReasoningOrigin>,
    budget: budget::Budget<'work>,
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(crate) fn events<'a>(
    adapter: Adapter,
    body: BoxProviderStream<'a, Result<Bytes>>,
    maximum: usize,
    dispatch: ModelDispatch,
) -> BoxProviderStream<'a, Result<ModelEvent>> {
    Box::pin(events_named(
        adapter, body, maximum, dispatch, None, None, None,
    ))
}

pub(crate) fn events_named<'a>(
    adapter: Adapter,
    body: BoxProviderStream<'a, Result<Bytes>>,
    maximum: usize,
    dispatch: ModelDispatch,
    names: Option<&'a crate::tool_names::Names<'a>>,
    origin: Option<acyclic_harness_core::model::ReasoningOrigin>,
    request_id: Option<::http::HeaderValue>,
) -> impl futures::Stream<Item = Result<ModelEvent>> + acyclic_stream::ProviderTask + 'a {
    async_stream::try_stream! {
        let mut input = sse::decode(body, maximum);
        let mut state = State::new(maximum);
        state.set_origin(origin)?;
        if acyclic_harness_core::model::RESEARCH_CAPTURE_ENABLED {
            state.request_id(request_id.as_ref().and_then(|value| value.to_str().ok()))?;
        }
        while let Some(data) = input.next().await {
            let data = data?;
            let deltas = parse(&mut state, adapter, &data)?;
            for event in deltas { yield event; }
            if state.terminal && (data == "[DONE]" || matches!(adapter, Adapter::Anthropic | Adapter::Responses)) { break; }
        }
        if let Some(names) = names { state.restore_names(names, |_| Ok(()))?; }
        let events = finish(state, adapter, dispatch)?;
        for event in events { yield event; }
    }
}

pub(crate) fn parse(state: &mut State, adapter: Adapter, data: &str) -> Result<Vec<ModelEvent>> {
    if data == "[DONE]" {
        if adapter != Adapter::Chat {
            return Err(invalid("unexpected stream terminator"));
        }
        state.terminal = true;
        return Ok(Vec::new());
    }
    let value = crate::json(data.as_bytes())?;
    if !value.is_object() {
        return Err(invalid("model event must be an object"));
    }
    if value.get("error").is_some_and(|error| !error.is_null())
        || matches!(value["type"].as_str(), Some("error" | "response.failed"))
    {
        return Err(error::classify(adapter, &value));
    }
    match adapter {
        Adapter::Chat => chat::parse(state, &value),
        Adapter::Anthropic => anthropic::parse(state, &value, data),
        Adapter::Responses => responses::parse(state, &value, data),
        Adapter::Gemini => gemini::parse(state, &value, data),
    }
}

pub(crate) fn finish(
    mut state: State,
    adapter: Adapter,
    dispatch: ModelDispatch,
) -> Result<Vec<ModelEvent>> {
    if !state.terminal {
        // The body ended early: the connection was cut off, not the protocol violated.
        return Err(crate::transport_failure(
            false,
            acyclic_harness_core::ModelTransportFailure::Interrupted,
            "model stream ended without a terminal marker",
        ));
    }
    let mut calls = Vec::with_capacity(
        state.calls.len()
            + state.thinking.len()
            + state.texts.len()
            + 1
            + usize::from(state.completed_message.is_some()),
    );
    if state
        .metadata
        .get("finish_reason")
        .is_some_and(|reason| reason == "length")
    {
        state.calls.clear();
    }
    // Validate the whole tool batch before exposing any call to the executor.
    let mut seen = BTreeSet::new();
    for call in state.calls.values() {
        if adapter == Adapter::Responses && call.freeform && !call.input_complete {
            return Err(invalid(
                "Responses custom-tool completion failed; recovery: return its complete input before completing the response",
            ));
        }
        if (adapter != Adapter::Gemini && call.id.trim().is_empty())
            || call.name.trim().is_empty()
            || (!call.id.is_empty() && !seen.insert(call.id.as_str()))
        {
            return Err(invalid("incomplete or duplicate model tool call"));
        }
    }
    drop(seen);
    let mut thinking = std::mem::take(&mut state.thinking).into_iter().peekable();
    let mut texts = std::mem::take(&mut state.texts).into_iter().peekable();
    let mut batch = std::mem::take(&mut state.calls).into_iter().peekable();
    let mut ordinal = 0u64;
    // Parts keep their response order; at one index, reasoning precedes
    // text, and text precedes calls.
    while let Some((_, next)) = [
        thinking.peek().map(|(index, _)| (*index, 0)),
        texts.peek().map(|(index, _)| (*index, 1)),
        batch.peek().map(|(index, _)| (*index, 2)),
    ]
    .into_iter()
    .flatten()
    .min()
    {
        if next == 0 {
            let (_index, value) = thinking
                .next()
                .ok_or_else(|| invalid("missing reasoning item"))?;
            let block = reasoning::block(adapter, value, state.origin.as_ref(), &mut state.budget)?;
            calls.push(ModelEvent::ReasoningBlock { block });
            continue;
        }
        if next == 1 {
            let (_, part) = texts.next().ok_or_else(|| invalid("missing text item"))?;
            let native = native(
                adapter,
                part.native,
                state.origin.as_ref(),
                &mut state.budget,
            )?;
            calls.push(ModelEvent::TextPart {
                text: part.text,
                native,
            });
            continue;
        }
        let (_, call) = batch.next().ok_or_else(|| invalid("missing call item"))?;
        let arguments = if call.freeform {
            state
                .budget
                .add(std::mem::size_of::<(String, Value)>() + "input".len())?;
            let mut fields = serde_json::Map::new();
            fields.insert("input".into(), Value::String(call.arguments));
            Value::Object(fields)
        } else {
            json(call.arguments.as_bytes())?
        };
        if !arguments.is_object() {
            return Err(invalid("tool arguments must be a JSON object"));
        }
        // The recording rule owns numbers and marker keys, so a call emitted
        // here is never rejected later when the executor records it.
        validate_portable_json(&arguments)?;
        // Admit the canonical ID and binding graph before allocating either.
        // The original provider ID is moved into the binding without copying.
        state.budget.add(
            ModelDispatch::TOOL_CALL_ID_BYTES
                + (3 + usize::from(call.freeform)) * std::mem::size_of::<(String, Value)>()
                + "provider_call".len()
                + "provider".len()
                + "id".len()
                + family(adapter).len()
                + if call.freeform { "freeform".len() } else { 0 },
        )?;
        let mut binding = serde_json::Map::new();
        binding.insert("provider".into(), Value::String(family(adapter).into()));
        binding.insert(
            "id".into(),
            if call.id.is_empty() {
                Value::Null
            } else {
                Value::String(call.id)
            },
        );
        let mut metadata = serde_json::Map::new();
        metadata.insert("provider_call".into(), Value::Object(binding));
        if call.freeform {
            metadata.insert("freeform".into(), Value::Bool(true));
        }
        calls.push(ModelEvent::ToolCall(
            acyclic_harness_core::model::ToolCall::new(
                dispatch.tool_call_id(ordinal),
                call.name,
                arguments,
            )
            .with_metadata(Value::Object(metadata))
            .with_native(native(
                adapter,
                call.native,
                state.origin.as_ref(),
                &mut state.budget,
            )?),
        ));
        ordinal += 1;
    }
    if let Some((_, text)) = state.completed_message {
        calls.push(ModelEvent::MessageCompleted { text });
    }
    calls.push(ModelEvent::Completed {
        metadata: Value::Object(state.metadata),
    });
    Ok(calls)
}

/// A part's exact native bytes with the response's origin; none when the
/// provider kept none.
fn native(
    adapter: Adapter,
    bytes: String,
    origin: Option<&acyclic_harness_core::model::ReasoningOrigin>,
    budget: &mut budget::Budget<'_>,
) -> Result<Option<acyclic_harness_core::model::NativePart>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    Ok(Some(acyclic_harness_core::model::NativePart {
        origin: reasoning::native_origin(adapter, origin, budget)?,
        continuity: bytes.into_bytes(),
    }))
}

impl<'work> State<'work> {
    pub(crate) fn restore_names(
        &mut self,
        names: &crate::tool_names::Names,
        mut fund: impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        if self
            .metadata
            .get("finish_reason")
            .is_some_and(|reason| reason == "length")
        {
            return Ok(());
        }
        for call in self.calls.values_mut() {
            if let Some(name) = names.canonical(&call.name)? {
                fund(name.len())?;
                self.budget.string(&mut call.name, name)?;
            }
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(self.budget.bytes()? as u64)
            .ok_or_else(|| invalid("provider retained capacity overflow"))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn arguments(&self) -> impl Iterator<Item = &str> {
        // Freeform input moves its already-admitted String into one object
        // member at finish; it never allocates a decoded JSON argument graph.
        self.calls
            .values()
            .filter(|call| !call.freeform)
            .map(|call| call.arguments.as_str())
    }

    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            calls: BTreeMap::new(),
            texts: BTreeMap::new(),
            metadata: serde_json::Map::new(),
            terminal: false,
            completed_message: None,
            thinking: BTreeMap::new(),
            origin: None,
            budget: budget::Budget::new(maximum),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native(
        maximum: usize,
        work: &'work acyclic_fs::NativeMaterializationWork,
    ) -> Self {
        Self {
            budget: budget::Budget::native(maximum, work),
            ..Self::new(maximum)
        }
    }

    fn call(&mut self, index: u64) -> Result<(&mut Call, &mut budget::Budget<'work>)> {
        let call = match self.calls.entry(index) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                self.budget.add(std::mem::size_of::<(u64, Call)>())?;
                entry.insert(Call::default())
            }
        };
        Ok((call, &mut self.budget))
    }

    fn text_part(&mut self, index: u64) -> Result<(&mut Text, &mut budget::Budget<'work>)> {
        let part = match self.texts.entry(index) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                self.budget.add(std::mem::size_of::<(u64, Text)>())?;
                entry.insert(Text::default())
            }
        };
        Ok((part, &mut self.budget))
    }

    /// Appends streamed visible text to the part at `index`.
    fn visible(&mut self, index: u64, delta: &Value) -> Result<()> {
        match delta.as_str().filter(|delta| !delta.is_empty()) {
            Some(delta) => {
                let (part, budget) = self.text_part(index)?;
                budget.append_str(&mut part.text, delta)
            }
            None => Ok(()),
        }
    }

    fn usage(&mut self, usage: &Value) -> Result<()> {
        let Some(object) = usage.as_object() else {
            return Ok(());
        };
        if !self.metadata.contains_key("usage") {
            self.budget.member(
                &mut self.metadata,
                "usage",
                &Value::Object(serde_json::Map::new()),
            )?;
        }
        let retained = self
            .metadata
            .get_mut("usage")
            .ok_or_else(|| invalid("missing usage"))?;
        if let Some(map) = retained.as_object_mut() {
            for (key, value) in object {
                self.budget.member(map, key, value)?;
            }
        }
        Ok(())
    }

    pub(crate) fn request_id(&mut self, value: Option<&str>) -> Result<()> {
        if let Some(value) = value {
            self.budget
                .member_str(&mut self.metadata, "provider_request_id", value)?;
        }
        Ok(())
    }

    fn identity(&mut self, value: &Value) -> Result<()> {
        let fields = if acyclic_harness_core::model::RESEARCH_CAPTURE_ENABLED {
            &["id", "model", "system_fingerprint"][..]
        } else {
            &["id", "model"][..]
        };
        for &field in fields {
            if let Some(value) = crate::protocol::present(value.get(field)) {
                self.budget.member(&mut self.metadata, field, value)?;
            }
        }
        Ok(())
    }
    fn finish_reason(&mut self, reason: &str) -> Result<()> {
        self.budget
            .member_str(&mut self.metadata, "finish_reason", reason)
    }
}

fn text(events: &mut Vec<ModelEvent>, value: &Value, reasoning: bool) {
    if let Some(delta) = value.as_str().filter(|delta| !delta.is_empty()) {
        events.push(if reasoning {
            ModelEvent::Reasoning {
                delta: delta.into(),
            }
        } else {
            ModelEvent::Content {
                delta: delta.into(),
            }
        });
    }
}

fn index(value: &Value, field: &str) -> Result<u64> {
    value[field]
        .as_u64()
        .filter(|value| *value <= acyclic_harness_core::conversation::MAX_EXACT_JS_INTEGER)
        .ok_or_else(|| invalid("invalid tool index"))
}

fn string(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| invalid("missing tool identity or string"))
}
