use super::call::Call;
use serde::{
    Serialize,
    ser::{SerializeMap, SerializeSeq},
};
use serde_json::{Map, Value, value::RawValue};

/// Native payloads borrow transcript bytes until HTTP serialization completes.
#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum Item<'a> {
    Json(Value),
    Native(&'a RawValue),
    Call(Call<'a>),
}

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum Message<'a> {
    Item(Item<'a>),
    Chat(Chat<'a>),
    Anthropic {
        role: &'static str,
        content: Vec<Item<'a>>,
    },
    Gemini {
        role: &'static str,
        parts: Vec<Item<'a>>,
    },
}

#[derive(Serialize)]
pub(super) struct Chat<'a> {
    pub role: &'static str,
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<Call<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
}

pub(crate) struct Body<'a> {
    pub(crate) fields: Map<String, Value>,
    pub(super) messages_field: &'static str,
    pub(super) messages: Vec<Message<'a>>,
}

pub(crate) enum SystemKey {
    Chat(String),
    Responses(Value),
}

impl SystemKey {
    pub(crate) fn owned_bytes(&self) -> acyclic_harness_core::Result<u64> {
        match self {
            Self::Chat(text) => Ok(text.capacity() as u64),
            Self::Responses(value) => {
                acyclic_harness_core::executor::json::json_value_logical_owned_bytes(value)
            }
        }
    }
}

fn system_key<'a>(message: &'a Message<'_>) -> Option<SystemKeyRef<'a>> {
    match message {
        Message::Chat(chat)
            if chat.role == "system"
                && chat.tool_calls.is_empty()
                && chat.reasoning_content.is_none() =>
        {
            chat.content.as_deref().map(SystemKeyRef::Chat)
        }
        Message::Item(Item::Json(value))
            if value.get("type").and_then(Value::as_str) == Some("message")
                && value.get("role").and_then(Value::as_str) == Some("system") =>
        {
            Some(SystemKeyRef::Responses(value))
        }
        _ => None,
    }
}

enum SystemKeyRef<'a> {
    Chat(&'a str),
    Responses(&'a Value),
}
impl Serialize for Body<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.serialize_fields(serializer, None, None)
    }
}

impl Body<'_> {
    pub(crate) fn serialize_fields<S: serde::Serializer>(
        &self,
        serializer: S,
        include: Option<&[&str]>,
        model: Option<&str>,
    ) -> Result<S::Ok, S::Error> {
        self.serialize_fields_prefix(serializer, include, model, None)
    }

    pub(crate) fn matches_system_prefix(&self, keys: &[SystemKey]) -> bool {
        let mut actual = self.messages.iter().map_while(system_key);
        keys.iter().all(|key| match (key, actual.next()) {
            (SystemKey::Chat(a), Some(SystemKeyRef::Chat(b))) => a == b,
            (SystemKey::Responses(a), Some(SystemKeyRef::Responses(b))) => a == b,
            _ => false,
        }) && actual.next().is_none()
    }

    pub(crate) fn encode_system_prefix(
        &self,
    ) -> Result<(Vec<SystemKey>, Vec<Box<RawValue>>), serde_json::Error> {
        let mut keys = Vec::new();
        let mut encoded = Vec::new();
        for message in &self.messages {
            let Some(key) = system_key(message) else {
                break;
            };
            keys.push(match key {
                SystemKeyRef::Chat(text) => SystemKey::Chat(text.to_owned()),
                SystemKeyRef::Responses(value) => SystemKey::Responses(value.clone()),
            });
            encoded.push(serde_json::value::to_raw_value(message)?);
        }
        Ok((keys, encoded))
    }

    fn serialize_fields_prefix<S: serde::Serializer>(
        &self,
        serializer: S,
        include: Option<&[&str]>,
        model: Option<&str>,
        chat_system: Option<&[Box<RawValue>]>,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        // Keep the reusable tool/instruction prefix before request controls.
        for name in ["tools", "system", "instructions", "systemInstruction"] {
            if let Some(value) = self.fields.get(name)
                && include.is_none_or(|fields| fields.contains(&name))
            {
                map.serialize_entry(name, value)?;
            }
        }
        for (name, value) in &self.fields {
            if matches!(
                name.as_str(),
                "tools" | "system" | "instructions" | "systemInstruction"
            ) {
                continue;
            }
            if include.is_none_or(|fields| fields.contains(&name.as_str()))
                && !(name == "model" && model.is_some())
            {
                map.serialize_entry(name, value)?;
            }
        }
        if let Some(model) = model {
            map.serialize_entry("model", model)?;
        }
        if matches!(self.messages.first(), Some(Message::Anthropic { .. })) {
            map.serialize_entry(self.messages_field, &CacheMessages(&self.messages))?;
        } else if let Some(prefix) = chat_system {
            map.serialize_entry(
                self.messages_field,
                &PrefixMessages(
                    prefix,
                    self.messages.get(prefix.len()..).ok_or_else(|| {
                        <S::Error as serde::ser::Error>::custom(
                            "encoded system prefix exceeds messages",
                        )
                    })?,
                ),
            )?;
        } else {
            map.serialize_entry(self.messages_field, &self.messages)?;
        }
        map.end()
    }
}

pub(crate) struct WithChatPrefix<'a, 'b>(pub &'a Body<'b>, pub Option<&'a [Box<RawValue>]>);
impl Serialize for WithChatPrefix<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0
            .serialize_fields_prefix(serializer, None, None, self.1)
    }
}

struct PrefixMessages<'a, 'b>(&'a [Box<RawValue>], &'a [Message<'b>]);
impl Serialize for PrefixMessages<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.1.len() + self.0.len()))?;
        for message in self.0 {
            sequence.serialize_element(message)?;
        }
        for message in self.1 {
            sequence.serialize_element(message)?;
        }
        sequence.end()
    }
}

struct CacheMessages<'a, 'b>(&'a [Message<'b>]);
impl Serialize for CacheMessages<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let last = self.0.iter().rposition(|message| {
            matches!(message,
            Message::Anthropic { content, .. } if content.iter().any(cacheable))
        });
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for (index, message) in self.0.iter().enumerate() {
            if Some(index) == last
                && let Message::Anthropic { role, content } = message
            {
                #[derive(Serialize)]
                struct Marked<'a, 'b> {
                    role: &'a str,
                    content: CacheContent<'a, 'b>,
                }
                sequence.serialize_element(&Marked {
                    role,
                    content: CacheContent(content),
                })?;
            } else {
                sequence.serialize_element(message)?;
            }
        }
        sequence.end()
    }
}

fn cacheable(item: &Item<'_>) -> bool {
    match item {
        Item::Json(value) => !matches!(
            value["type"].as_str(),
            Some("thinking" | "redacted_thinking")
        ),
        Item::Call(_) => true,
        // Signed reasoning bytes must remain untouched, and thinking cannot
        // itself carry a breakpoint. Cache the preceding eligible block.
        Item::Native(_) => false,
    }
}

struct CacheContent<'a, 'b>(&'a [Item<'b>]);
impl Serialize for CacheContent<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let last = self.0.iter().rposition(cacheable);
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for (index, item) in self.0.iter().enumerate() {
            if Some(index) == last {
                #[derive(Serialize)]
                struct Marked<'a, 'b> {
                    #[serde(flatten)]
                    item: &'a Item<'b>,
                    cache_control: Control,
                }
                sequence.serialize_element(&Marked {
                    item,
                    cache_control: Control { kind: "ephemeral" },
                })?;
            } else {
                sequence.serialize_element(item)?;
            }
        }
        sequence.end()
    }
}

#[derive(Serialize)]
struct Control {
    #[serde(rename = "type")]
    kind: &'static str,
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
impl<'a> Body<'a> {
    pub(crate) fn fixture(mut value: Value, field: &'static str) -> Self {
        let mut fields = std::mem::take(value.as_object_mut().unwrap());
        let Value::Array(items) = fields.remove(field).unwrap() else {
            panic!("fixture messages must be an array")
        };
        let messages = items
            .into_iter()
            .map(|item| Message::Item(Item::Json(item)))
            .collect();
        Self {
            fields,
            messages_field: field,
            messages,
        }
    }

    /// Every wire call's argument reference, in message order: what final
    /// serialization will read.
    pub(crate) fn arguments(&mut self) -> Vec<&mut &'a Value> {
        let mut args = Vec::new();
        for message in &mut self.messages {
            match message {
                Message::Item(Item::Call(call)) => args.push(&mut call.arguments),
                Message::Chat(chat) => {
                    args.extend(chat.tool_calls.iter_mut().map(|call| &mut call.arguments))
                }
                Message::Anthropic { content, .. } | Message::Gemini { parts: content, .. } => {
                    args.extend(content.iter_mut().filter_map(|item| match item {
                        Item::Call(call) => Some(&mut call.arguments),
                        _ => None,
                    }));
                }
                _ => {}
            }
        }
        args
    }
}

/// Replays role-alternating wire messages through the production sequence
/// fold, as request projection pushes them. `None` when a message has a role
/// or content shape projection never emits.
#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(crate) fn replay_turns(adapter: crate::Adapter, messages: &[Value]) -> Option<Vec<Value>> {
    replay_turns_with(adapter, messages, super::sequence::RULES)
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(super) fn replay_turns_with(
    adapter: crate::Adapter,
    messages: &[Value],
    rules: super::sequence::Rules,
) -> Option<Vec<Value>> {
    use crate::Adapter;
    let (field, roles) = match adapter {
        Adapter::Anthropic => ("content", ["user", "assistant"]),
        Adapter::Gemini => ("parts", ["user", "model"]),
        Adapter::Responses | Adapter::Chat => return None,
    };
    let mut projected = Vec::new();
    for message in messages {
        let role = roles.into_iter().find(|role| message["role"] == *role)?;
        let parts = message[field].as_array()?.iter().cloned().map(Item::Json);
        let next = if adapter == Adapter::Anthropic {
            Message::Anthropic {
                role,
                content: parts.collect(),
            }
        } else {
            Message::Gemini {
                role,
                parts: parts.collect(),
            }
        };
        super::sequence::push(adapter, &mut projected, next).unwrap();
    }
    super::sequence::finish_with(adapter, &mut projected, rules);
    match crate::json(&serde_json::to_vec(&projected).unwrap()).unwrap() {
        Value::Array(output) => Some(output),
        _ => unreachable!("a message list encodes as an array"),
    }
}
