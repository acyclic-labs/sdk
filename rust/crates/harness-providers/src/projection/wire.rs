use serde::{Serialize, ser::SerializeMap};
use serde_json::{Map, Value, value::RawValue};

/// Native payloads borrow transcript bytes until HTTP serialization completes.
#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum Item<'a> {
    Json(Value),
    Native(&'a RawValue),
}

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum Message<'a> {
    Item(Item<'a>),
    Anthropic {
        role: &'static str,
        content: Vec<Item<'a>>,
    },
    Gemini {
        role: &'static str,
        parts: Vec<Item<'a>>,
    },
}

pub(crate) struct Body<'a> {
    pub(super) fields: Map<String, Value>,
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
