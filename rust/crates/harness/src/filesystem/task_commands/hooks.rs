use super::*;
use crate::{
    context::{Context, ContextPlacement, SourceStage},
    hooks::{HookEvent, Hooks},
    model::{ModelMessage, ModelRole},
};

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn session_start(
        &self,
        pipeline: ContextPipeline,
        tools: &crate::tool::ToolRegistry,
        context: Arc<TaskContext>,
        (fence, operation, reconcile_only, digest): (&LeaseFence, OperationId, bool, [u8; 32]),
        io: Option<&super::super::PreparationIo>,
    ) -> Result<ContextPipeline> {
        if !pipeline
            .configuration_sources()
            .any(|file| file.descriptor().media_type() == Hooks::MEDIA_TYPE)
        {
            return Ok(pipeline);
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
        let hooks = hooks.unwrap_or_default();
        let event = HookEvent::SessionStart;
        let input = serde_json::json!({"source":"startup",
            "session_id": context.durable_task_id().map(|task| task.to_string())});
        if hooks.commands(event, &input, tools)?.next().is_none() {
            return Ok(pipeline);
        }
        let executor =
            context.harness.workspace_tools.as_ref().ok_or_else(|| {
                Error::Unsupported("hook workspace execution is not bound".into())
            })?;
        let task = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("hook dispatch has no task".into()))?;
        let owned = Arc::new(self.runtime.task_context(task, fence.clone()).await?);
        let issued = ToolContext::new(owned, operation, "session-start", None)?;
        let output = executor
            .run_hooks(
                (&hooks, digest),
                event,
                input,
                tools,
                issued,
                reconcile_only,
            )
            .await?;
        for failure in &output.diagnostics {
            tracing::warn!(hook_event = event.name(), cause = ?failure.cause,
                exit_code = failure.cause.exit_code(), blocking = failure.blocking,
                error = %failure.cause.message(), "configured hook failed");
        }
        let output = output.into_result()?;
        if let Some(reason) = output.blocked {
            return Err(Error::Unauthorized(reason));
        }
        if output.additional_context.is_empty() {
            return Ok(pipeline);
        }
        let source = Context {
            messages: output
                .additional_context
                .into_iter()
                .map(|text| ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(text),
                })
                .collect(),
            ..Default::default()
        };
        Ok(pipeline.with(Arc::new(SourceStage::new(
            "hooks.session-start",
            "1",
            Arc::new(source),
            ContextPlacement::Prepend,
        ))))
    }
}
