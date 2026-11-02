//! Replay equivalence over the original closed request types. This is a hash
//! projection, never provider wire output or a persistence/index format.
use super::*;
use serde::{Serialize, Serializer, ser::SerializeSeq};

pub(super) fn key(request: &ModelRequest) -> Result<[u8; 32]> {
    // Destructuring is exhaustive: a new request field must participate in
    // replay identity rather than silently inheriting an older key policy.
    let ModelRequest {
        model,
        messages,
        compose_tools,
        tools,
        sampling,
        max_output_tokens,
    } = request;
    #[derive(Serialize)]
    struct Request<'a> {
        model: &'a Model,
        messages: Messages<'a>,
        compose_tools: bool,
        tools: &'a [crate::tool::ToolDefinition],
        sampling: &'a ModelSampling,
        max_output_tokens: Option<u32>,
    }
    crate::executor::json::serialized_json_digest(&(
        "model-request-replay-v1",
        Request {
            model,
            messages: Messages(messages),
            compose_tools: *compose_tools,
            tools,
            sampling,
            max_output_tokens: *max_output_tokens,
        },
    ))
}

struct Messages<'a>(&'a [ModelMessage]);
impl Serialize for Messages<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for message in self.0 {
            seq.serialize_element(&(message.role, Content(&message.content)))?;
        }
        seq.end()
    }
}

struct Content<'a>(&'a ModelContent);
impl Serialize for Content<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self.0 {
            ModelContent::Text(text) => text.serialize(serializer),
            ModelContent::Part(part) => Part(part).serialize(serializer),
            ModelContent::Parts(parts) => {
                let mut seq = serializer.serialize_seq(Some(parts.len()))?;
                for part in parts {
                    seq.serialize_element(&Part(part))?;
                }
                seq.end()
            }
        }
    }
}

struct Part<'a>(&'a ModelContentPart);
impl Serialize for Part<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self.0 {
            ModelContentPart::File { file, policy } => {
                ("file", File(file, policy), policy).serialize(serializer)
            }
            ModelContentPart::ToolResult {
                call_id,
                name,
                content: ToolResultContent::Parts { parts },
            } => ("tool_result", call_id, name, Data(parts)).serialize(serializer),
            // In particular, JSON inside a tool result and text inside any
            // role cannot impersonate a file, notice or discovery generation.
            part => part.serialize(serializer),
        }
    }
}

struct Data<'a>(&'a [ModelDataPart]);
impl Serialize for Data<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for part in self.0 {
            match part {
                ModelDataPart::File { file, policy } => {
                    seq.serialize_element(&("file", File(file, policy), policy))?
                }
                part => seq.serialize_element(part)?,
            }
        }
        seq.end()
    }
}

struct File<'a>(&'a FileRef, &'a FileProjectionPolicy);
impl Serialize for File<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self.1 {
            // These policies read verified bytes. The descriptor pins those
            // bytes and media type; the locator is not model-visible content.
            // ProjectInstructions keeps its separate original frame identity
            // in the policy above, and native configuration stays exact there.
            FileProjectionPolicy::BoundedFull
            | FileProjectionPolicy::ProjectInstructions(_)
            | FileProjectionPolicy::Native(_) => self.0.descriptor().serialize(serializer),
            FileProjectionPolicy::Reference => self.0.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod tests;
