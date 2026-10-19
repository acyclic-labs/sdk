use super::*;
use crate::{
    context::{Context, ContextPlacement, SourceStage},
    hooks::{BoundHooks, HookEvent, HookSite, Hooks},
    model::{ModelMessage, ModelRole},
};

/// Prompt-site hooks either prepare the model command or fail it.
pub(super) enum PromptHooks {
    /// The pipeline with any added context, and the hooks bound for tool sites.
    Ready(ContextPipeline, Option<Arc<BoundHooks>>),
    /// A deterministic, retained block; the command fails with this message.
    Blocked(String),
}

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn prompt_hooks(
        &self,
        pipeline: ContextPipeline,
        tools: &crate::tool::ToolRegistry,
        context: Arc<TaskContext>,
        (fence, operation, reconcile_only, digest): (&LeaseFence, OperationId, bool, [u8; 32]),
        prompt: &ModelContent,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<PromptHooks> {
        if !pipeline
            .configuration_sources()
            .any(|file| file.descriptor().media_type() == Hooks::MEDIA_TYPE)
        {
            return Ok(PromptHooks::Ready(pipeline, None));
        }
        if io.is_some() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "hooks require ordinary task execution",
            ));
        }
        let mut hooks = None::<Hooks>;
        for source in pipeline.configuration_sources() {
            if source.descriptor().media_type() != Hooks::MEDIA_TYPE {
                continue;
            }
            let bytes = context.read_file(source, None).await?;
            let next: Hooks = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            match &mut hooks {
                Some(hooks) => hooks.append(next),
                None => hooks = Some(next),
            }
        }
        let bound = Arc::new(BoundHooks {
            rules: hooks.unwrap_or_default(),
            manifest: digest,
        });
        let session_id = context.durable_task_id().map(|task| task.to_string());
        let mut messages = Vec::new();
        for event in HookEvent::at(HookSite::Prompt) {
            let input = match event {
                HookEvent::SessionStart => {
                    serde_json::json!({"source":"startup", "session_id": session_id})
                }
                HookEvent::UserPromptSubmit => {
                    serde_json::json!({"prompt": prompt_text(prompt), "session_id": session_id})
                }
                HookEvent::PreToolUse | HookEvent::PostToolUse | HookEvent::Stop => {
                    return Err(Error::Invalid("prompt hook site fires a tool event".into()));
                }
            };
            let output = match self
                .prompt_hook(
                    &bound,
                    event,
                    &input,
                    tools,
                    &context,
                    (fence, operation, reconcile_only),
                )
                .await?
            {
                Ok(output) => output,
                Err(reason) => return Ok(PromptHooks::Blocked(reason)),
            };
            messages.extend(output.into_iter().map(|text| ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(text),
            }));
        }
        if messages.is_empty() {
            return Ok(PromptHooks::Ready(pipeline, Some(bound)));
        }
        let source = Context {
            messages,
            ..Default::default()
        };
        Ok(PromptHooks::Ready(
            pipeline.with(Arc::new(SourceStage::new(
                "hooks.prompt",
                "1",
                Arc::new(source),
                ContextPlacement::Prepend,
            ))),
            Some(bound),
        ))
    }

    /// Runs one prompt-scoped event once per model command, retaining its outcome.
    /// Returns the context it adds, or the reason an explicit or failure-policy
    /// block fails the command before any provider request.
    async fn prompt_hook(
        &self,
        bound: &BoundHooks,
        event: HookEvent,
        input: &serde_json::Value,
        tools: &crate::tool::ToolRegistry,
        context: &Arc<TaskContext>,
        (fence, operation, reconcile_only): (&LeaseFence, OperationId, bool),
    ) -> Result<std::result::Result<Vec<String>, String>> {
        if bound.rules.commands(event, input, tools)?.next().is_none() {
            return Ok(Ok(Vec::new()));
        }
        let executor =
            context.harness.workspace_tools.as_ref().ok_or_else(|| {
                Error::Unsupported("hook workspace execution is not bound".into())
            })?;
        let task = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("hook dispatch has no task".into()))?;
        let owned = Arc::new(self.runtime.task_context(task, fence.clone()).await?);
        let issued = ToolContext::new(owned, operation, prompt_hook_call(event), None)?;
        let output = executor
            .run_hooks(
                (&bound.rules, bound.manifest),
                event,
                input.into(),
                tools,
                issued,
                reconcile_only,
            )
            .await?
            .ok_or_else(|| Error::Invalid("started model has no prompt hook outcome".into()))?;
        crate::hooks::report(event, &output);
        let mut output = output;
        if let Some(reason) = output.take_block_reason() {
            return Ok(Err(format!(
                "{} hook blocked the prompt: {reason}",
                event.name()
            )));
        }
        Ok(Ok(output.additional_context))
    }
}

/// Distinct retained claim identities for each prompt-scoped event of one command.
pub(super) fn prompt_hook_call(event: HookEvent) -> &'static str {
    match event {
        HookEvent::SessionStart => "session-start",
        HookEvent::UserPromptSubmit => "user-prompt-submit",
        HookEvent::PreToolUse => "pre-tool-use",
        HookEvent::PostToolUse => "post-tool-use",
        HookEvent::Stop => "stop",
    }
}

/// The submitted text a UserPromptSubmit hook inspects; attachments are not inlined.
fn prompt_text(prompt: &ModelContent) -> String {
    match prompt {
        ModelContent::Text(text) => text.clone(),
        content => content
            .parts()
            .iter()
            .filter_map(|part| match part {
                crate::model::ModelContentPart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(
                "
",
            ),
    }
}
