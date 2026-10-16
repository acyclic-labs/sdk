//! Canonical message files, resolved identically for live delivery and replay.

use crate::{
    Error, Result,
    conversation::{ContentResidencyVerifier, ConversationMessage, Limits, MessageKind},
    model::ToolResultContent,
};
use serde_json::Value;

pub(super) enum MessageContent {
    Call { name: String, arguments: Value },
    Output(ToolResultContent),
    Usage(Value),
}

impl MessageContent {
    pub(super) async fn read(
        message: &ConversationMessage,
        files: Option<&dyn ContentResidencyVerifier>,
    ) -> Result<Option<Self>> {
        let metadata = message.extensions.get("acyclic.model.metadata");
        if !matches!(
            message.kind,
            MessageKind::ToolCall | MessageKind::ToolResult
        ) && metadata.is_none()
        {
            return Ok(None);
        }
        let files = files.ok_or_else(|| {
            Error::Unsupported("message projection requires a filesystem provider".into())
        })?;
        let limits = Limits::default();
        Ok(Some(match message.kind {
            MessageKind::ToolCall => {
                let call = crate::projection::read_tool_call(
                    files,
                    &message.content,
                    limits.render_bytes,
                    None,
                )
                .await?;
                crate::tool::ToolInvocation::validate_identity(
                    &call.call_id,
                    Some(&call.name),
                    &mut None,
                )?;
                if message.tool_call_id.as_deref() != Some(call.call_id.as_str()) {
                    return Err(Error::Invalid(
                        "tool artifact identity differs from its record".into(),
                    ));
                }
                Self::Call {
                    name: call.name,
                    arguments: call.arguments,
                }
            }
            MessageKind::ToolResult => Self::Output(
                crate::projection::read_tool_output(message, files, limits, None).await?,
            ),
            MessageKind::Assistant => {
                let metadata =
                    metadata.ok_or_else(|| Error::Invalid("assistant metadata missing".into()))?;
                Self::Usage(
                    crate::projection::read_value(files, metadata, limits.render_bytes, None)
                        .await?
                        .0,
                )
            }
            _ => {
                return Err(Error::Invalid(
                    "model metadata belongs to an assistant message".into(),
                ));
            }
        }))
    }

    pub(super) fn requires_files(message: &ConversationMessage) -> bool {
        matches!(
            message.kind,
            MessageKind::ToolCall | MessageKind::ToolResult
        ) || message.extensions.contains_key("acyclic.model.metadata")
    }
}
