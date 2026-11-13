//! Configured lifecycle hooks of one agent turn. The host's journal pins
//! the hook configuration; prompt events run once before the model
//! sees the turn, and tool and stop events run in the stock loop.
use super::*;
use crate::{
    context::{Context, ContextPlacement, SourceStage},
    hooks::{BoundHooks, HookEvent, HookSite, Hooks},
    runtime::{TaskContext, ToolContext},
};

impl StockExecutor {
    /// Binds the turn's pinned imported host configuration `source` ahead of
    /// admitted `plugins`, then runs its
    /// prompt events: `SessionStart` once per agent `session` and
    /// `UserPromptSubmit` on every turn. `SessionStart` is keyed by the
    /// session, not the turn, so it runs on the agent's first turn (a forked
    /// child's included) and every later turn reads back its recorded
    /// outcome, keeping its context. Their context precedes the turn; a block
    /// fails the turn when it runs, before any model request. Hooks bind per
    /// turn for prompt events; step selection may replace plugin execution
    /// rules while retaining these original host rules and the frozen prefix.
    /// The source reader retains host custody only for loading this pinned
    /// configuration; hook commands keep the original task context and scope.
    pub async fn with_agent_hooks(
        self,
        context: &TaskContext,
        turn: OperationId,
        prompt: &ModelContent,
        session: OperationId,
        source: Option<(&FileRef, &dyn crate::conversation::ContentResidencyVerifier)>,
        plugins: &crate::plugins::Enabled,
    ) -> Result<Self> {
        let Some((source, reader)) = source else {
            return Ok(self);
        };
        if source.descriptor().media_type() != Hooks::MEDIA_TYPE {
            return Err(Error::Invalid(
                "hook configuration has the wrong media type".into(),
            ));
        }
        context.scope().limits().validate_file(source, &mut None)?;
        let (bytes, _hold) = crate::conversation::verified_content_bytes(
            reader,
            source,
            context.scope().limits().file_bytes,
            None,
            None,
        )
        .await?;
        let rules: Hooks = serde_json::from_slice(&bytes)
            .map_err(|error| Error::Invalid(format!("hook configuration: {error}")))?;
        let mut executor = self;
        if rules.is_empty() && plugins.rules.is_empty() {
            return Ok(executor);
        }
        let bound = crate::hooks::bind_rules(&rules, &plugins.rules, None)?;
        executor.host_hooks = (!rules.is_empty()).then(|| Arc::new(rules));
        let prompt = prompt_text(context, prompt).await?;
        let context = Arc::new(context.clone());
        let mut stages = Vec::new();
        for event in HookEvent::at(HookSite::Prompt) {
            let (input, operation) = match event {
                HookEvent::SessionStart => (
                    serde_json::json!({"source": "startup", "session_id": session.to_string()}),
                    session,
                ),
                HookEvent::UserPromptSubmit => (
                    serde_json::json!({
                        "prompt": prompt,
                        "session_id": session.to_string(),
                    }),
                    turn,
                ),
                HookEvent::PreToolUse
                | HookEvent::PostToolUse
                | HookEvent::Stop
                | HookEvent::FileChanged
                | HookEvent::StreamRecord => {
                    return Err(Error::Invalid("prompt hook site fires a tool event".into()));
                }
            };
            let text = match executor
                .prompt_hook(&bound, event, &input, &context, operation)
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
    /// The event runs once per `operation`.
    async fn prompt_hook(
        &self,
        bound: &BoundHooks,
        event: HookEvent,
        input: &serde_json::Value,
        context: &Arc<TaskContext>,
        operation: OperationId,
    ) -> Result<std::result::Result<Option<Vec<String>>, String>> {
        if bound
            .rules
            .commands(event, input, &self.tools)?
            .next()
            .is_none()
        {
            return Ok(Ok(None));
        }
        let home = context
            .harness
            .target(None)
            .map_err(|_| Error::Unsupported("hook workspace execution is not bound".into()))?;
        let executor = &home.executor;
        let issued = ToolContext::new(Arc::clone(context), operation, prompt_call(event))?;
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
            .ok_or(Error::Indeterminate(operation))?;
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
            crate::model::ModelContentPart::Text { text: part, .. } => text.push(part.clone()),
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
