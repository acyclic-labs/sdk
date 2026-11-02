use super::call::Call;
use acyclic_model_render::Fields;
use acyclic_model_render::ToolDeclarations;
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

pub(crate) struct Projection<'a> {
    pub(super) target: acyclic_model_render::Provenance,
    pub(super) fields: Fields,
    pub(super) tools:
        Option<ToolDeclarations<'a, acyclic_harness_core::tool::schema::ModelInput<'a>>>,
    #[cfg(test)]
    pub(super) fixture_tools: Option<Value>,
    #[cfg(test)]
    pub(super) messages_field: &'static str,
    pub(super) messages: Vec<Message<'a>>,
}
#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum Tools<'a, 'body> {
    Admitted(&'a ToolDeclarations<'body, acyclic_harness_core::tool::schema::ModelInput<'body>>),
    #[cfg(test)]
    Generated(&'a Value),
}
impl Projection<'_> {
    pub(super) fn tools(&self) -> Option<Tools<'_, '_>> {
        #[cfg(test)]
        if let Some(tools) = &self.fixture_tools {
            return Some(Tools::Generated(tools));
        }
        self.tools.as_ref().map(Tools::Admitted)
    }
    pub(crate) fn encode(&self) -> acyclic_harness_core::Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(128);
        self.write_selected(&mut bytes, None, None::<&str>)?;
        Ok(bytes)
    }

    #[cfg(test)]
    pub(crate) fn value(&self) -> acyclic_harness_core::Result<Value> {
        crate::json(&self.encode()?)
    }

    pub(crate) fn write_selected(
        &self,
        writer: &mut impl std::io::Write,
        include: Option<&[&str]>,
        model: Option<impl Serialize>,
    ) -> acyclic_harness_core::Result<()> {
        let tools = self.tools();
        acyclic_model_render::RequestView {
            target: &self.target,
            fields: &self.fields,
            tools: tools.as_ref(),
            include,
            model,
        }
        .write(&self.messages, writer)
        .map_err(|error| crate::invalid(format!("model request encoding: {error}")))
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
impl<'a> Projection<'a> {
    pub(crate) fn fixture(mut value: Value, field: &'static str) -> Self {
        let mut fields = std::mem::take(value.as_object_mut().unwrap());
        let Value::Array(items) = fields.remove(field).unwrap() else {
            panic!("fixture messages must be an array")
        };
        let messages = items
            .into_iter()
            .map(|item| Message::Item(Item::Json(item)))
            .collect();
        let family = match field {
            "input" => crate::Adapter::Responses,
            "contents" => crate::Adapter::Gemini,
            _ => crate::Adapter::Anthropic,
        };
        let fixture_tools = fields.remove("tools");
        let mut configuration = Fields::default();
        for (name, value) in fields {
            configuration.insert(
                acyclic_model_render::Field::named(&name).expect("generated protocol field"),
                value,
            );
        }
        Self {
            target: acyclic_model_render::Provenance {
                family,
                provider: "generated".into(),
                model: "generated".into(),
                endpoint: "https://generated.invalid".into(),
                credential_scope: None,
            },
            fields: configuration,
            fixture_tools,
            tools: None,
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
