use super::*;
use crate::hooks::{HookEvent, HookInput, HookOutput, HookSite};

impl StockExecution<'_> {
    pub(super) fn append_hook_context(
        &self,
        messages: &mut Vec<ModelMessage>,
        mut output: HookOutput,
    ) -> Result<()> {
        let blocked = output.take_block_reason();
        for text in blocked.into_iter().chain(output.additional_context) {
            let message = ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(text),
            };
            message
                .content
                .validate_limits(self.stock.limits, &mut None)?;
            stock_vec_push(messages, message, self.io)?;
        }
        Ok(())
    }

    pub(super) async fn tool_hook(
        &self,
        operation: OperationId,
        site: HookSite,
        invocation: &ToolInvocation,
        response: Option<&ToolResult>,
        retained_only: bool,
    ) -> Result<HookOutput> {
        self.tool_hook_output(operation, site, invocation, response, retained_only)
            .await?
            .ok_or_else(|| {
                if retained_only || self.stock.reconciliation_only {
                    Error::Indeterminate(operation)
                } else {
                    Error::Invalid("started tool has no retained hook outcome".into())
                }
            })
    }

    /// None proves no hook claim exists. A claimed but unfinished effect remains
    /// indeterminate in the workspace executor and must never be replayed as absent.
    pub(super) async fn tool_hook_output(
        &self,
        _operation: OperationId,
        site: HookSite,
        invocation: &ToolInvocation,
        response: Option<&ToolResult>,
        retained_only: bool,
    ) -> Result<Option<HookOutput>> {
        let Some(hooks) = &self.stock.hooks else {
            return Ok(Some(HookOutput::default()));
        };
        let event = HookEvent::only(site)?;
        let input = HookInput::Tool {
            invocation,
            response,
            session_id: self.stock.task.as_ref().map(|(_, task, _)| *task),
        };
        if hooks
            .rules
            .commands(event, input, &self.stock.tools)?
            .next()
            .is_none()
        {
            return Ok(Some(HookOutput::default()));
        }
        if self.io.is_some() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Unsupported,
                "hooks require ordinary task execution",
            ));
        }
        let (task, _) = self
            .context
            .ok_or_else(|| Error::Unauthorized("tool hook has no issued task context".into()))?;
        let executor = task.harness.workspace_tools.as_ref().ok_or_else(|| {
            Error::Unsupported("tool hook workspace execution is not bound".into())
        })?;
        let phase = phase_operation(invocation.operation_id, event);
        let context =
            crate::runtime::ToolContext::new(Arc::clone(task), phase, &invocation.call_id, None)?;
        match executor
            .run_hooks(
                (&hooks.rules, hooks.manifest),
                event,
                input,
                &self.stock.tools,
                context,
                retained_only || self.stock.reconciliation_only,
            )
            .await?
        {
            Some(output) => {
                crate::hooks::report(event, &output);
                Ok(Some(output))
            }
            None => Ok(None),
        }
    }

    pub(super) async fn hook_approval(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        invocation: &ToolInvocation,
        tool: &crate::tool::Tool,
        prompt: String,
        started: bool,
    ) -> Result<Option<ExecutionProgress<()>>> {
        let hooks =
            self.stock.hooks.as_ref().ok_or_else(|| {
                Error::Invalid("hook approval has no pinned configuration".into())
            })?;
        let task = self.stock.task.as_ref().map_or(
            TaskId::from_bytes(operation.into_bytes()),
            |(_, task, _)| *task,
        );
        let identity = ComponentIdentity {
            name: "hooks.pre-tool".into(),
            version: "1".into(),
            digest: hooks.manifest,
        };
        let ((approval, request), _hold) = tool_approval_request(
            task,
            &identity,
            &tool.definition,
            invocation,
            (prompt, None),
            None,
        )?;
        self.settle_approval(journal, operation, approval, &request, invocation, started)
            .await
    }
}

impl StockExecution<'_> {
    /// Fires the turn-end site when a model step requests no tool. A blocking
    /// outcome's reason is returned for the model to continue from; failure
    /// policy never blocks Stop, only explicit exit-2 or structured denials.
    pub(super) async fn stop_hook(
        &self,
        operation: OperationId,
        step: u32,
        active: bool,
    ) -> Result<Option<HookOutput>> {
        let Some(hooks) = &self.stock.hooks else {
            return Ok(None);
        };
        let event = HookEvent::only(HookSite::TurnEnd)?;
        let fields = serde_json::json!({
            "session_id": self.stock.task.as_ref().map(|(_, task, _)| *task),
            "stop_hook_active": active,
        });
        let input = HookInput::Fields(&fields);
        if hooks
            .rules
            .commands(event, input, &self.stock.tools)?
            .next()
            .is_none()
        {
            return Ok(None);
        }
        if self.io.is_some() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Unsupported,
                "hooks require ordinary task execution",
            ));
        }
        let (task, _) = self
            .context
            .ok_or_else(|| Error::Unauthorized("stop hook has no issued task context".into()))?;
        let executor = task.harness.workspace_tools.as_ref().ok_or_else(|| {
            Error::Unsupported("stop hook workspace execution is not bound".into())
        })?;
        let context = crate::runtime::ToolContext::new(
            Arc::clone(task),
            step_operation(operation, step),
            "stop",
            None,
        )?;
        match executor
            .run_hooks(
                (&hooks.rules, hooks.manifest),
                event,
                input,
                &self.stock.tools,
                context,
                self.stock.reconciliation_only,
            )
            .await?
        {
            Some(mut output) => {
                crate::hooks::report(event, &output);
                Ok(output.take_block_reason().map(|reason| {
                    output.blocked = Some(reason);
                    output
                }))
            }
            None if self.stock.reconciliation_only => Err(Error::Indeterminate(operation)),
            None => Err(Error::Invalid(
                "finished model step has no retained stop hook outcome".into(),
            )),
        }
    }

    /// The model continues from its own answer and the Stop hook's reason.
    pub(super) fn continue_after_stop(
        &self,
        messages: &mut Vec<ModelMessage>,
        answer: &str,
        output: HookOutput,
    ) -> Result<()> {
        let answer = (!answer.is_empty()).then(|| ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text(answer.to_owned()),
        });
        if let Some(message) = answer {
            message
                .content
                .validate_limits(self.stock.limits, &mut None)?;
            stock_vec_push(messages, message, self.io)?;
        }
        self.append_hook_context(messages, output)?;
        Ok(())
    }
}

/// Each finished model step has its own retained Stop claim.
fn step_operation(operation: OperationId, step: u32) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness:stop-hook:v1\0");
    hasher.update(&operation.into_bytes());
    hasher.update(&step.to_le_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(id)
}

pub(crate) fn phase_operation(tool: OperationId, event: HookEvent) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness:tool-hook:v1\0");
    hasher.update(&tool.into_bytes());
    hasher.update(event.name().as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(id)
}
