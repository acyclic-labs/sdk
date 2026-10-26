//! Configured lifecycle hooks of one agent turn. The turn's context manifest
//! carries the hook configuration; prompt events run once before the model
//! sees the turn, and tool and stop events run in the stock loop.
use super::*;
use crate::{
    context::{Context, ContextPlacement, SourceStage},
    hooks::{BoundHooks, HookEvent, HookSite, Hooks},
    runtime::{TaskContext, ToolContext},
};

impl StockExecutor {
    /// Binds the hooks this turn's context manifest configures, then runs its
    /// prompt events: `SessionStart` on the agent's first turn and
    /// `UserPromptSubmit` on every turn. Their context precedes the turn; a
    /// block fails the turn when it runs, before any model request.
    pub async fn with_agent_hooks(
        self,
        context: &TaskContext,
        turn: OperationId,
        prompt: &ModelContent,
        first_turn: bool,
    ) -> Result<Self> {
        let mut hooks = None::<Hooks>;
        let mut digest = blake3::Hasher::new_derive_key("acyclic.agent.hooks/v1");
        for source in self.context.configuration_sources() {
            if source.descriptor().media_type() != Hooks::MEDIA_TYPE {
                continue;
            }
            let bytes = context.read_file(source, None).await?;
            digest.update(blake3::hash(&bytes).as_bytes());
            let next: Hooks = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Invalid(format!("hook configuration: {error}")))?;
            match &mut hooks {
                Some(hooks) => hooks.append(next),
                None => hooks = Some(next),
            }
        }
        let Some(rules) = hooks else {
            return Ok(self);
        };
        let bound = Arc::new(BoundHooks {
            rules,
            manifest: *digest.finalize().as_bytes(),
        });
        let prompt = prompt_text(context, prompt).await?;
        let context = Arc::new(context.clone());
        let mut executor = self;
        let mut stages = Vec::new();
        for event in HookEvent::at(HookSite::Prompt) {
            let input = match event {
                HookEvent::SessionStart if first_turn => {
                    serde_json::json!({"source": "startup", "session_id": turn.to_string()})
                }
                HookEvent::SessionStart => continue,
                HookEvent::UserPromptSubmit => serde_json::json!({
                    "prompt": prompt,
                    "session_id": turn.to_string(),
                }),
                HookEvent::PreToolUse | HookEvent::PostToolUse | HookEvent::Stop => {
                    return Err(Error::Invalid("prompt hook site fires a tool event".into()));
                }
            };
            let text = match executor
                .prompt_hook(&bound, event, &input, &context, turn)
                .await?
            {
                Ok(Some(text)) => text,
                Ok(None) => continue,
                Err(reason) => {
                    executor.prompt_block = Some(reason);
                    return Ok(executor);
                }
            };
            stages.push(Arc::new(SourceStage::new(
                match event {
                    HookEvent::SessionStart => "hooks.session-start",
                    _ => "hooks.user-prompt-submit",
                },
                "1",
                Arc::new(Context {
                    messages: text
                        .into_iter()
                        .map(|text| ModelMessage {
                            role: ModelRole::System,
                            content: ModelContent::Text(text),
                        })
                        .collect(),
                    ..Default::default()
                }),
                ContextPlacement::Prepend,
            )));
        }
        // Prepending in reverse keeps the prompt events' message order.
        for stage in stages.into_iter().rev() {
            executor = executor.with_context_stage(stage)?;
        }
        Ok(executor.with_hooks(Some(bound)))
    }

    /// One prompt event's added context, or `None` when no hook matches; or
    /// the reason a block, explicit or by failure policy, fails the turn.
    async fn prompt_hook(
        &self,
        bound: &BoundHooks,
        event: HookEvent,
        input: &serde_json::Value,
        context: &Arc<TaskContext>,
        turn: OperationId,
    ) -> Result<std::result::Result<Option<Vec<String>>, String>> {
        if bound
            .rules
            .commands(event, input, &self.tools)?
            .next()
            .is_none()
        {
            return Ok(Ok(None));
        }
        let executor =
            context.harness.workspace_tools.as_ref().ok_or_else(|| {
                Error::Unsupported("hook workspace execution is not bound".into())
            })?;
        let issued = ToolContext::new(Arc::clone(context), turn, prompt_call(event))?;
        let mut output = executor
            .run_hooks(
                (&bound.rules, bound.manifest),
                event,
                input.into(),
                &self.tools,
                issued,
                self.reconciliation_only,
            )
            .await?
            .ok_or(Error::Indeterminate(turn))?;
        crate::hooks::report(event, &output);
        if let Some(reason) = output.take_block_reason() {
            return Ok(Err(format!(
                "{} hook blocked the prompt: {reason}",
                event.name()
            )));
        }
        Ok(Ok(Some(output.additional_context)))
    }
}

/// Distinct call identities for each prompt event of one turn.
fn prompt_call(event: HookEvent) -> &'static str {
    match event {
        HookEvent::SessionStart => "session-start",
        _ => "user-prompt-submit",
    }
}

/// The submitted text a UserPromptSubmit hook inspects: text parts and the
/// message body file an agent turn's input names, decoded lossily.
async fn prompt_text(context: &TaskContext, prompt: &ModelContent) -> Result<String> {
    let parts = match prompt {
        ModelContent::Text(text) => return Ok(text.clone()),
        content => content.parts(),
    };
    let mut text = Vec::new();
    for part in parts {
        match part {
            crate::model::ModelContentPart::Text { text: part } => text.push(part.clone()),
            crate::model::ModelContentPart::File { file, .. } => {
                let bytes = context.read_file(file, None).await?;
                text.push(String::from_utf8_lossy(&bytes).into_owned());
            }
            _ => {}
        }
    }
    Ok(text.join(
        "
",
    ))
}
