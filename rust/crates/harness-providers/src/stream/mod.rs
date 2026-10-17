mod anthropic;
mod budget;
mod chat;
mod error;
mod gemini;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod limits_tests;
mod reasoning;
mod responses;
pub(crate) use reasoning::family;

use crate::{Adapter, invalid, json, sse};
use acyclic_harness::{
    Result,
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
    name: String,
    arguments: String,
    metadata: Value,
}

pub(crate) struct State<'work> {
    calls: BTreeMap<u64, Call>,
    metadata: serde_json::Map<String, Value>,
    pub(crate) terminal: bool,
    thinking: BTreeMap<u64, reasoning::Thinking>,
    origin: Option<acyclic_harness::model::ReasoningOrigin>,
    budget: budget::Budget<'work>,
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(crate) fn events<'a>(
    adapter: Adapter,
    body: BoxProviderStream<'a, Result<Bytes>>,
    maximum: usize,
    dispatch: ModelDispatch,
) -> BoxProviderStream<'a, Result<ModelEvent>> {
    events_named(adapter, body, maximum, dispatch, None, None)
}

pub(crate) fn events_named<'a>(
    adapter: Adapter,
    body: BoxProviderStream<'a, Result<Bytes>>,
    maximum: usize,
    dispatch: ModelDispatch,
    names: Option<&'a crate::tool_names::Names<'a>>,
    origin: Option<acyclic_harness::model::ReasoningOrigin>,
) -> BoxProviderStream<'a, Result<ModelEvent>> {
    Box::pin(async_stream::try_stream! {
        let mut input = sse::decode(body, maximum);
        let mut state = State::new(maximum);
        state.set_origin(origin)?;
        while let Some(data) = input.next().await {
            let data = data?;
            let deltas = parse(&mut state, adapter, &data)?;
            for event in deltas { yield event; }
            if state.terminal && (data == "[DONE]" || matches!(adapter, Adapter::Anthropic | Adapter::Responses)) { break; }
        }
        if let Some(names) = names { state.restore_names(names, |_| Ok(()))?; }
        let events = finish(state, adapter, dispatch)?;
        for event in events { yield event; }
    })
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
        return Err(acyclic_harness::Error::Storage(
            "model stream ended without a terminal marker".into(),
        ));
    }
    let mut calls = Vec::with_capacity(state.calls.len() + state.thinking.len() + 1);
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
        if (adapter != Adapter::Gemini && call.id.trim().is_empty())
            || call.name.trim().is_empty()
            || (!call.id.is_empty() && !seen.insert(call.id.as_str()))
        {
            return Err(invalid("incomplete or duplicate model tool call"));
        }
    }
    drop(seen);
    let mut thinking = std::mem::take(&mut state.thinking).into_iter().peekable();
    let mut batch = std::mem::take(&mut state.calls).into_iter().peekable();
    let mut ordinal = 0u64;
    while thinking.peek().is_some() || batch.peek().is_some() {
        if thinking
            .peek()
            .is_some_and(|(index, _)| batch.peek().is_none_or(|(call, _)| index <= call))
        {
            let (_index, value) = thinking
                .next()
                .ok_or_else(|| invalid("missing reasoning item"))?;
            let block = reasoning::block(adapter, value, state.origin.as_ref(), &mut state.budget)?;
            calls.push(ModelEvent::ReasoningBlock { block });
            continue;
        }
        let (_, call) = batch.next().ok_or_else(|| invalid("missing call item"))?;
        let arguments = json(call.arguments.as_bytes())?;
        if !arguments.is_object() {
            return Err(invalid("tool arguments must be a JSON object"));
        }
        validate_numbers(&arguments)?;
        // Admit the canonical ID and binding graph before allocating either.
        // The original provider ID is moved into the binding without copying.
        let native = !call.metadata.is_null();
        state.budget.add(
            ModelDispatch::TOOL_CALL_ID_BYTES
                + (3 + usize::from(native)) * std::mem::size_of::<(String, Value)>()
                + "provider_call".len()
                + "provider".len()
                + "id".len()
                + family(adapter).len()
                + if native { "native".len() } else { 0 },
            3 + usize::from(native),
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
        if native {
            metadata.insert("native".into(), call.metadata);
        }
        calls.push(ModelEvent::ToolCall(
            acyclic_harness::model::ToolCall::new(
                dispatch.tool_call_id(ordinal),
                call.name,
                arguments,
            )
            .with_metadata(Value::Object(metadata)),
        ));
        ordinal += 1;
    }
    calls.push(ModelEvent::Completed {
        metadata: Value::Object(state.metadata),
    });
    Ok(calls)
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
        self.calls.values().map(|call| call.arguments.as_str())
    }

    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            calls: BTreeMap::new(),
            metadata: serde_json::Map::new(),
            terminal: false,
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
                self.budget.add(std::mem::size_of::<(u64, Call)>(), 1)?;
                entry.insert(Call::default())
            }
        };
        Ok((call, &mut self.budget))
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

    fn identity(&mut self, value: &Value) -> Result<()> {
        for field in ["id", "model"] {
            if let Some(value) = value.get(field) {
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
        .filter(|value| *value <= 9_007_199_254_740_991)
        .ok_or_else(|| invalid("invalid tool index"))
}

fn string(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| invalid("missing tool identity or string"))
}

fn validate_numbers(value: &Value) -> Result<()> {
    match value {
        Value::Number(number)
            if number
                .as_f64()
                .is_none_or(|number| number.abs() > 9_007_199_254_740_991.0) =>
        {
            Err(invalid("tool number exceeds the portable exact range"))
        }
        Value::Array(values) => values.iter().try_for_each(validate_numbers),
        Value::Object(values) => values.values().try_for_each(validate_numbers),
        _ => Ok(()),
    }
}
