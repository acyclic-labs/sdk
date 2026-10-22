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
    /// The effective tool call and, after execution, its canonical result.
    Tool {
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
            Self::Fields(fields) => match event {
                HookEvent::SessionStart => fields.get("source"),
                HookEvent::PreToolUse | HookEvent::PostToolUse => fields.get("tool_name"),
                HookEvent::UserPromptSubmit | HookEvent::Stop => None,
            }
            .and_then(Value::as_str)
            .unwrap_or(""),
        }
    }

    pub(super) fn encode(self, event: HookEvent, cwd: &Path) -> Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec(&Envelope(self, event, cwd))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        bytes.push(b'\n');
        Ok(bytes)
    }
}

struct Envelope<'a>(HookInput<'a>, HookEvent, &'a Path);

impl Serialize for Envelope<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut fields = serializer.serialize_map(None)?;
        match self.0 {
            HookInput::Fields(value) => {
                let object = value
                    .as_object()
                    .ok_or_else(|| serde::ser::Error::custom("hook input must be an object"))?;
                for (name, value) in object {
                    if !matches!(name.as_str(), "cwd" | "hook_event_name") {
                        fields.serialize_entry(name, value)?;
                    }
                }
            }
            HookInput::Tool {
                invocation,
                response,
                session_id,
            } => {
                fields.serialize_entry("tool_name", &invocation.name)?;
                fields.serialize_entry("tool_input", &invocation.arguments)?;
                fields.serialize_entry("tool_use_id", &invocation.call_id)?;
                fields.serialize_entry("session_id", &session_id)?;
                if let Some(response) = response {
                    fields.serialize_entry("tool_response", &response.value)?;
                }
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
