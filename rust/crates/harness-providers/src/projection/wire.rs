use super::call::Call;
use acyclic_model_render::{Field, Fields};
use serde::Serialize;
use serde_json::{Value, value::RawValue};

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "trailer_tests.rs"]
mod tests;

/// Native payloads borrow transcript bytes until HTTP serialization completes.
#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum Item<'a> {
    Json(Value),
    Native(&'a RawValue),
    Call(Call<'a>),
    Result(super::result::ToolResult<'a>),
}

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum Message<'a> {
    Notice(#[serde(serialize_with = "notice_wire")] acyclic_model_render::WireItem),
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

fn notice_wire<S: serde::Serializer>(
    item: &acyclic_model_render::WireItem,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    item.wire().serialize(serializer)
}

#[derive(Serialize)]
pub(super) struct Chat<'a> {
    pub role: &'static str,
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<Call<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<&'a RawValue>,
}

impl acyclic_model_render::Segment for Message<'_> {
    fn write(
        &self,
        _: &acyclic_model_render::Provenance,
        writer: &mut impl std::io::Write,
    ) -> Result<Option<bool>, acyclic_model_render::RenderError> {
        serde_json::to_writer(writer, self).map_err(acyclic_model_render::json_error)?;
        Ok(match self {
            Self::Gemini { role, .. } => Some(*role == "model"),
            _ => None,
        })
    }
}

/// A projected request. Its only encoder is model-render: the header slots and
/// tools, the conversation, then the per-call trailer.
pub(crate) struct Request<'a> {
    pub(super) target: acyclic_model_render::Provenance,
    pub(super) fields: Fields<'a>,
    pub(super) messages: Vec<Message<'a>>,
}

impl Request<'_> {
    pub(crate) fn header(&self) -> acyclic_harness_core::Result<acyclic_model_render::Header> {
        acyclic_model_render::Header::new(
            self.target.clone(),
            acyclic_model_render::RENDERER_VERSION,
            &self.fields,
            None,
        )
        .map_err(rendered)
    }

    pub(crate) fn tail(&self) -> acyclic_harness_core::Result<acyclic_model_render::Tail> {
        acyclic_model_render::Tail::new(&self.fields).map_err(rendered)
    }

    pub(crate) fn render(
        &self,
        writer: &mut impl std::io::Write,
    ) -> acyclic_harness_core::Result<()> {
        acyclic_model_render::render(&self.header()?, &self.messages, &self.tail()?, writer)
            .map_err(rendered)
    }

    pub(crate) fn encode(&self) -> acyclic_harness_core::Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(128);
        self.render(&mut bytes)?;
        Ok(bytes)
    }

    /// The count endpoint's subset of this same request, in place.
    pub(crate) fn into_count(mut self, model: &str, adapter: crate::Adapter) -> Self {
        if adapter == crate::Adapter::Gemini {
            self.fields
                .insert(Field::Model, Value::String(format!("models/{model}")));
        } else {
            let include = crate::counting::wire::fields(adapter);
            self.fields
                .retain(|name| include.iter().any(|field| *field == name));
        }
        self
    }
}

fn rendered(error: acyclic_model_render::RenderError) -> acyclic_harness_core::Error {
    crate::invalid(error.to_string())
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
impl<'a> Request<'a> {
    /// Generated root fields and conversation; `tools` must already be encoded.
    pub(crate) fn fixture(mut value: Value, field: &'static str) -> Self {
        let mut values = std::mem::take(value.as_object_mut().unwrap());
        let Value::Array(items) = values.remove(field).unwrap() else {
            panic!("fixture messages must be an array")
        };
        let family = match field {
            "input" => crate::Adapter::Responses,
            "contents" => crate::Adapter::Gemini,
            _ => crate::Adapter::Anthropic,
        };
        let mut fields = Fields::default();
        if let Some(tools) = values.remove("tools") {
            assert_eq!(
                tools,
                serde_json::json!([]),
                "fixture tools are typed declarations"
            );
            let empty = acyclic_model_render::ToolDeclarations::<Value>::new(family, Vec::new());
            fields.set_tools(family, &empty.unwrap()).unwrap();
        }
        for (name, value) in values {
            let field = Field::named(&name).expect("generated input must use the protocol schema");
            fields.insert(field, value);
        }
        let messages = items
            .into_iter()
            .map(|item| Message::Item(Item::Json(item)))
            .collect();
        Self {
            target: acyclic_model_render::Provenance {
                family,
                provider: "generated".into(),
                model: "generated".into(),
                endpoint: "https://generated.invalid".into(),
                credential_scope: None,
            },
            fields,
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

    pub(crate) fn results(&mut self) -> Vec<&mut super::result::ToolResult<'a>> {
        let mut results = Vec::new();
        for message in &mut self.messages {
            match message {
                Message::Item(Item::Result(result)) => results.push(result),
                Message::Anthropic { content, .. } | Message::Gemini { parts: content, .. } => {
                    results.extend(content.iter_mut().filter_map(|item| match item {
                        Item::Result(result) => Some(result),
                        _ => None,
                    }));
                }
                _ => {}
            }
        }
        results
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
    super::sequence::finish_with(adapter, &mut projected, rules).ok()?;
    match crate::json(&serde_json::to_vec(&projected).unwrap()).unwrap() {
        Value::Array(output) => Some(output),
        _ => unreachable!("a message list encodes as an array"),
    }
}
