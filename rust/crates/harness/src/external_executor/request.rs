use super::{ChildSession, ExternalHost};
use crate::OperationId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Immutable hook request; authentication accompanies its submission separately.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalRequest {
    /// Original operation identity, retained across hook retries.
    pub operation_id: OperationId,
    /// Native executor implementation.
    pub host: ExternalHost,
    /// Native session whose attachment resolves the task.
    pub session_id: String,
    /// Native turn binding, when supplied by the host.
    pub turn_id: Option<String>,
    /// Native parent/caller binding, when supplied by the host.
    pub caller: Option<String>,
    /// Requested harness operation.
    pub action: ExternalAction,
}

pub(super) fn valid_text(text: &str) -> bool {
    !text.is_empty() && !text.contains('\0')
}

impl ExternalRequest {
    pub(super) fn child_completion(&self) -> crate::Result<Self> {
        if !self.valid() || !matches!(self.action, ExternalAction::Join { .. }) {
            return Err(crate::Error::Invalid(
                "request is not a native child stop".into(),
            ));
        }
        let mut child = self.child_attachment("")?;
        child.action = ExternalAction::Detach;
        child.operation_id = super::normalize::operation(
            child.host,
            &child.session_id,
            &child.caller,
            &child.action,
        );
        Ok(child)
    }

    pub(super) fn child_attachment(&self, workspace: &str) -> crate::Result<Self> {
        let (ExternalAction::Fork { child_session, .. }
        | ExternalAction::Join { child_session, .. }) = &self.action
        else {
            return Err(crate::Error::Invalid(
                "request does not select a native child".into(),
            ));
        };
        let action = ExternalAction::Attach {
            workspace: workspace.into(),
        };
        Ok(Self {
            operation_id: super::normalize::operation(
                child_session.host,
                &child_session.session_id,
                &child_session.caller,
                &action,
            ),
            host: child_session.host,
            session_id: child_session.session_id.clone(),
            turn_id: None,
            caller: child_session.caller.clone(),
            action,
        })
    }
    /// Durable session identity, independent of tool calls and turn changes.
    #[must_use]
    pub fn task_id(&self) -> crate::TaskId {
        let operation = super::normalize::operation(
            self.host,
            &self.session_id,
            &self.caller,
            &ExternalAction::Attach {
                workspace: String::new(),
            },
        );
        crate::TaskId::from_bytes(operation.into_bytes())
    }

    pub(super) fn valid(&self) -> bool {
        let action = match &self.action {
            ExternalAction::Attach { workspace } => valid_text(workspace),
            ExternalAction::RunTool { call_id, name, .. }
            | ExternalAction::HostTool { call_id, name, .. } => {
                valid_text(call_id) && valid_text(name)
            }
            ExternalAction::ToolFinished { call_id, .. }
            | ExternalAction::CancelTool { call_id } => valid_text(call_id),
            ExternalAction::Fork {
                child_id,
                kind,
                child_session,
            } => {
                valid_text(child_id)
                    && kind.as_deref().is_none_or(valid_text)
                    && child_session.valid()
            }
            ExternalAction::Join {
                child_id,
                child_session,
            } => valid_text(child_id) && child_session.valid(),
            ExternalAction::Detach => true,
        };
        action
            && valid_text(&self.session_id)
            && self.turn_id.as_deref().is_none_or(valid_text)
            && (!matches!(self.action, ExternalAction::Attach { .. }) || self.turn_id.is_none())
            && self.caller.as_deref().is_none_or(valid_text)
            && self.operation_id
                == super::normalize::operation(
                    self.host,
                    &self.session_id,
                    &self.caller,
                    &self.action,
                )
    }
}

/// Requests consumed by the shared task owner, never by a plugin state machine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalAction {
    /// Attach a native executor to a task for this workspace.
    Attach {
        /// Native workspace path resolved through the actor's volume binding.
        workspace: String,
    },
    /// Execute one original tool call.
    RunTool {
        /// Native tool invocation identity.
        call_id: String,
        /// Native tool name, interpreted by the host tool catalog.
        name: String,
        /// Complete arguments after the native call has been assembled.
        arguments: Value,
    },
    /// Observe a tool executed by the native host, never by the harness.
    HostTool {
        /// Native invocation identity shared with its completion record.
        call_id: String,
        /// Complete native tool name, including a foreign MCP namespace.
        name: String,
        /// Original host-executed arguments retained for authenticated history.
        arguments: Value,
    },
    /// Stop one tool and await its original result and physical cleanup.
    CancelTool {
        /// Original native invocation identity; cancellation has its own retry ID.
        call_id: String,
    },
    /// Record the native tool completion.
    ToolFinished {
        /// Original tool invocation identity.
        call_id: String,
        /// Native host reported failure.
        failed: bool,
        /// Native result, if included in the hook protocol.
        result: Option<Value>,
    },
    /// Fork and attach a native child executor.
    Fork {
        /// Parent's stable label for this child invocation.
        child_id: String,
        /// Native child kind, when supplied.
        kind: Option<String>,
        /// Exact native identity admitted before its process or tools start.
        child_session: ChildSession,
    },
    /// Join the previously attached child task.
    Join {
        /// Original label within the parent invocation.
        child_id: String,
        /// Exact identity declared by the corresponding start.
        child_session: ChildSession,
    },
    /// Release this executor's attachment.
    Detach,
}
