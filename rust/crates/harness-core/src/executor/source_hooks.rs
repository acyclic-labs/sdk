//! Registered `hooks/<name>` source programs run on the workspace hook runner,
//! once per delivery, after the turn's pinned policy allows their canonical
//! call. Hooks and authored tools share one manifest, one producer and one
//! policy boundary.
use super::*;
use crate::hooks::{HookEvent, HookInput};

impl StockExecutor {
    pub(super) async fn source_hook(&self, hook: &crate::agent::HookTrigger) -> Result<()> {
        let program = self
            .tools
            .load_hook(&hook.name)
            .await?
            .ok_or_else(|| Error::NotFound(format!("hooks/{}", hook.name)))?;
        if program.on().map(crate::config::ProgramTrigger::event) != Some(hook.event) {
            return Err(Error::Conflict(
                "hook program no longer selects this event".into(),
            ));
        }
        let (task, _) = self.task_context.as_ref().ok_or_else(|| {
            Error::Unauthorized("source hooks require the turn's task context".into())
        })?;
        let home = task.harness.target(None).map_err(|_| {
            Error::Unsupported("source hook workspace execution is not bound".into())
        })?;
        let fields = match hook.event {
            HookEvent::FileChanged => {
                serde_json::json!({"path": hook.path, "sequence": hook.sequence})
            }
            _ => serde_json::json!({"stream_path": hook.path, "sequence": hook.sequence}),
        };
        let rules = program.hook_rules()?;
        let manifest =
            json::canonical_json_digest(&("acyclic.agent.hooks/v1", &rules), None, &mut None)?;
        let shell = self
            .tool_binding(crate::tools::shell::PLATFORM.name)?
            .ok_or_else(|| {
                Error::Unauthorized("source hooks require the shell capability".into())
            })?;
        let bound = program.bind_hook(shell)?;
        // The hook runs on the home target, so its device and owner rules apply.
        let target = crate::interaction::ApprovalTarget {
            device: home.record.name.clone(),
            owner: home.record.owner.clone(),
        };
        self.admit_source_hook(bound.binding(), hook, &fields, &target)
            .await?;
        let context = crate::runtime::ToolContext::new(Arc::clone(task), hook.operation, "hook")?;
        if let Some(output) = home
            .executor
            .run_hooks(
                (&rules, manifest),
                hook.event,
                HookInput::Fields(&fields),
                &self.tools,
                context,
                self.reconciliation_only,
            )
            .await?
        {
            crate::hooks::report(hook.event, &output);
        }
        Ok(())
    }

    /// The hook's canonical shell call passes the same admission as a
    /// model's call (the turn's tool grants, schema and resources), then the
    /// pinned policy. No person, model or parent answers a source event, so
    /// a call the policy's automatic rules do not allow is refused.
    async fn admit_source_hook(
        &self,
        tool: &crate::tool::Tool,
        hook: &crate::agent::HookTrigger,
        fields: &Value,
        target: &crate::interaction::ApprovalTarget,
    ) -> Result<()> {
        let policy = self
            .policy
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("no approval policy admits this hook".into()))?;
        let invocation = ToolInvocation {
            operation_id: hook.operation,
            call_id: format!("hooks/{}", hook.name),
            name: tool.definition.name.clone(),
            arguments: fields.clone(),
        };
        StockExecution::ordinary(self).admit_unattended(tool, &invocation)?;
        let canonical = tool.executor.canonical(&invocation, None)?;
        let judged = canonical.as_deref().unwrap_or(&invocation);
        let (decision, _custody) = policy.evaluate(judged, &self.tool_scope, None)?.await?;
        match decision {
            ToolPolicyDecision::Allow => Ok(()),
            ToolPolicyDecision::Deny { reason } => Err(Error::Unauthorized(reason)),
            ToolPolicyDecision::RequireApproval { .. } => match policy.unattended(judged, Some(target)) {
                Some(crate::approvals::RuleVerdict::Allow) => Ok(()),
                Some(_) => Err(Error::Unauthorized(
                    "the approval rules refuse this source hook".into(),
                )),
                None => Err(Error::Unauthorized(
                    "a source hook runs only when the approval policy's rules allow its command;                      a model, human or parent approver cannot answer a background event"
                        .into(),
                )),
            },
        }
    }
}
