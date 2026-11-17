use super::*;
use crate::tool::{ToolInvocation, ToolResult};
use serde::{Serialize, Serializer, ser::SerializeMap};
use std::path::Path;

/// Borrowed stdin fields for a configured hook. Tool payloads are serialized
/// directly from the admitted invocation/result, without building a JSON copy.
#[derive(Clone, Copy)]
pub enum HookInput<'a> {
    /// Event-specific fields supplied by the caller.
    Fields(&'a Value),
    /// A `hooks/<name>` program's admitted event input. Its stdin is exactly
    /// this object, the value its input schema and the approval policy
    /// judged, as an authored tool's stdin is its admitted arguments. The
    /// runner adds no fields; the process starts in the hook's workspace.
    Program(&'a Value),
    /// The effective tool call and, after execution, its canonical result.
    Tool {
        /// Exact implementation admitted for this call. An absent binding uses
        /// the registry's model declaration, as for external host calls.
        binding: Option<&'a crate::tool::Tool>,
        /// Admitted invocation, including its original call identity.
        invocation: &'a ToolInvocation,
        /// The result exists only after the tool has finished.
        response: Option<&'a ToolResult>,
        /// Owning durable session/task.
        session_id: Option<crate::TaskId>,
    },
}

impl<'a> From<&'a Value> for HookInput<'a> {
    fn from(value: &'a Value) -> Self {
        Self::Fields(value)
    }
}

impl<'a> HookInput<'a> {
    pub(super) fn subject(self, event: HookEvent) -> &'a str {
        match self {
            Self::Tool { invocation, .. } => &invocation.name,
            Self::Fields(fields) | Self::Program(fields) => match event {
                HookEvent::SessionStart => fields.get("source"),
                HookEvent::PreToolUse | HookEvent::PostToolUse => fields.get("tool_name"),
                HookEvent::FileChanged => fields.get("path"),
                HookEvent::StreamRecord => fields.get("stream_path"),
                HookEvent::UserPromptSubmit | HookEvent::Stop => None,
            }
            .and_then(Value::as_str)
            .unwrap_or(""),
        }
    }

    /// The tool input a before-tool hook judges.
    pub(super) fn tool_input(self) -> Option<&'a Value> {
        match self {
            Self::Tool { invocation, .. } => Some(&invocation.arguments),
            Self::Fields(fields) | Self::Program(fields) => fields.get("tool_input"),
        }
    }

    /// Its host's stdin envelope. `rewritten` replaces the tool input a
    /// rewrite earlier in the before-tool chain changed.
    /// Its host's stdin envelope.
    pub fn encode(
        self,
        event: HookEvent,
        cwd: &Path,
        rewritten: Option<&Value>,
    ) -> Result<Vec<u8>> {
        let mut bytes = match self {
            Self::Program(value) if value.is_object() => serde_json::to_vec(value),
            Self::Program(_) => return Err(Error::Invalid("hook input must be an object".into())),
            _ => serde_json::to_vec(&Envelope(self, event, cwd, rewritten)),
        }
        .map_err(|error| Error::Invalid(error.to_string()))?;
        bytes.push(b'\n');
        Ok(bytes)
    }
}

struct Envelope<'a>(HookInput<'a>, HookEvent, &'a Path, Option<&'a Value>);

impl Serialize for Envelope<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut fields = serializer.serialize_map(None)?;
        match self.0 {
            HookInput::Fields(value) => {
                let object = value
                    .as_object()
                    .ok_or_else(|| serde::ser::Error::custom("hook input must be an object"))?;
                for (name, value) in object {
                    match (name.as_str(), self.3) {
                        ("cwd" | "hook_event_name", _) => {}
                        ("tool_input", Some(rewritten)) => {
                            fields.serialize_entry(name, rewritten)?;
                        }
                        _ => fields.serialize_entry(name, value)?,
                    }
                }
            }
            HookInput::Tool {
                invocation,
                response,
                session_id,
                ..
            } => {
                fields.serialize_entry("tool_name", &invocation.name)?;
                fields.serialize_entry("tool_input", self.3.unwrap_or(&invocation.arguments))?;
                fields.serialize_entry("tool_use_id", &invocation.call_id)?;
                fields.serialize_entry("session_id", &session_id)?;
                if let Some(response) = response {
                    fields.serialize_entry("tool_response", &response.value)?;
                }
            }
            // `encode` writes a program's admitted input as it is.
            HookInput::Program(_) => {
                return Err(serde::ser::Error::custom("program input has no envelope"));
            }
        }
        fields.serialize_entry("hook_event_name", self.1.name())?;
        fields.serialize_entry("cwd", &self.2.to_string_lossy())?;
        fields.end()
    }
}

#[cfg(test)]
#[path = "input/tests.rs"]
mod tests;
