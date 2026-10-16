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
        let mut map = serializer.serialize_map(Some(self.fields.len() + 1))?;
        for (name, value) in &self.fields {
            map.serialize_entry(name, value)?;
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

/// A count request borrows the already projected generation without cloning its tree.
#[cfg(any(all(target_arch = "wasm32", feature = "browser"), all(test, not(loom))))]
pub(crate) struct BorrowedCountBody<'a, 'b> {
    pub generation: &'a Body<'b>,
    pub adapter: crate::Adapter,
}
#[cfg(any(all(target_arch = "wasm32", feature = "browser"), all(test, not(loom))))]
impl Serialize for BorrowedCountBody<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        if self.adapter == crate::Adapter::Gemini {
            #[derive(Serialize)]
            struct Gemini<'a, 'b> {
                #[serde(rename = "generateContentRequest")]
                request: &'a Body<'b>,
            }
            // The preparation inserts the Gemini model before either serialization.
            return Gemini {
                request: self.generation,
            }
            .serialize(serializer);
        }
        let mut map = serializer.serialize_map(None)?;
        for (field, value) in &self.generation.fields {
            if crate::counting::wire::fields(self.adapter).contains(&field.as_str()) {
                map.serialize_entry(field, value)?;
            }
        }
        map.serialize_entry(self.generation.messages_field, &self.generation.messages)?;
        map.end()
    }
}
