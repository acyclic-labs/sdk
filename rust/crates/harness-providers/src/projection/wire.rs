use super::call::Call;
use serde::{Serialize, ser::SerializeMap};
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

    pub(crate) fn borrowed_arguments(&self) -> Vec<&'a Value> {
        let mut args = Vec::new();
        for message in &self.messages {
            match message {
                Message::Item(Item::Call(call)) => args.push(call.arguments),
                Message::Chat(chat) => {
                    args.extend(chat.tool_calls.iter().map(|call| call.arguments))
                }
                Message::Anthropic { content, .. } | Message::Gemini { parts: content, .. } => {
                    args.extend(content.iter().filter_map(|item| match item {
                        Item::Call(call) => Some(call.arguments),
                        _ => None,
                    }));
                }
                _ => {}
            }
        }
        args
    }
}
