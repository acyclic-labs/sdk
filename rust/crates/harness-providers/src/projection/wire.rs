use super::call::Call;
use serde::{Serialize, ser::SerializeMap};
use serde_json::{Map, Value, value::RawValue};

/// Native payloads borrow transcript bytes until HTTP serialization completes.
pub(super) enum Item<'a> {
    Json(Value),
    Image(super::image::Image<'a>),
    Parts {
        fields: Value,
        key: &'static str,
        parts: Vec<Item<'a>>,
    },
    Native(&'a RawValue),
    Call(Call<'a>),
}

impl Item<'_> {
    pub fn text(&self) -> Option<&str> {
        match self {
            Self::Json(value) => value["text"].as_str(),
            _ => None,
        }
    }
}
impl Serialize for Item<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Json(value) => value.serialize(serializer),
            Self::Native(value) => value.serialize(serializer),
            Self::Call(value) => value.serialize(serializer),
            Self::Image(value) => value.serialize(serializer),
            Self::Parts { fields, key, parts } => {
                let mut map = serializer.serialize_map(None)?;
                for (name, value) in fields.as_object().expect("projected object") {
                    map.serialize_entry(name, value)?;
                }
                map.serialize_entry(key, parts)?;
                map.end()
            }
        }
    }
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
impl Serialize for Body<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.serialize_fields(serializer, None, None)
    }
}

impl Body<'_> {
    /// Size the wire buffer without encoding image bytes or constructing a JSON graph.
    pub(crate) fn capacity(&self) -> acyclic_harness::Result<usize> {
        use super::encoded_len;
        fn item(value: &Item<'_>) -> acyclic_harness::Result<usize> {
            match value {
                Item::Image(image) => {
                    let empty = super::image::Image {
                        adapter: image.adapter,
                        mime: image.mime,
                        policy: image.policy,
                        bytes: &[],
                    };
                    encoded_len(&empty)?
                        .checked_add(image.bytes.len().div_ceil(3) * 4)
                        .ok_or_else(|| crate::invalid("image size overflow"))
                }
                Item::Parts { fields, key, parts } => {
                    let mut bytes = encoded_len(fields)? + encoded_len(key)? + 4;
                    for part in parts {
                        bytes = bytes
                            .checked_add(item(part)? + 1)
                            .ok_or_else(|| crate::invalid("image size overflow"))?;
                    }
                    Ok(bytes)
                }
                _ => encoded_len(value),
            }
        }
        let mut bytes = encoded_len(&self.fields)? + encoded_len(&self.messages_field)? + 4;
        for message in &self.messages {
            let size = match message {
                Message::Item(value) => item(value)?,
                Message::Chat(value) => encoded_len(value)?,
                Message::Anthropic { content, .. } | Message::Gemini { parts: content, .. } => {
                    let mut size = 64;
                    for part in content {
                        size += item(part)? + 1;
                    }
                    size
                }
            };
            bytes = bytes
                .checked_add(size + 1)
                .ok_or_else(|| crate::invalid("image size overflow"))?;
        }
        Ok(bytes)
    }
    pub(crate) fn serialize_fields<S: serde::Serializer>(
        &self,
        serializer: S,
        include: Option<&[&str]>,
        model: Option<&str>,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (name, value) in &self.fields {
            if include.is_none_or(|fields| fields.contains(&name.as_str()))
                && !(name == "model" && model.is_some())
            {
                map.serialize_entry(name, value)?;
            }
        }
        if let Some(model) = model {
            map.serialize_entry("model", model)?;
        }
        map.serialize_entry(self.messages_field, &self.messages)?;
        map.end()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
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
