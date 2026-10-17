//! A selected provider binding must survive ordinary fenced command dispatch.
use super::*;
use crate::{conversation::ContentPublisher, runtime::ContentBindings};

struct SelectedTool {
    authorization: Arc<PendingTool>,
    writer: Arc<dyn ContentPublisher>,
    answer: String,
}

impl ToolExecutor for SelectedTool {
    fn authorize(
        &self,
        scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        self.authorization
            .authorize(scope, context, invocation, work)?;
        let writer = context
            .and_then(|context| context.task().content_binding(None))
            .and_then(|binding| binding.writer.as_ref())
            .ok_or_else(|| Error::Unauthorized("selected writer was lost".into()))?;
        if !Arc::ptr_eq(writer, &self.writer) {
            return Err(Error::Unauthorized("selected writer was replaced".into()));
        }
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            context
                .as_ref()
                .unwrap()
                .task()
                .owned_mailbox
                .as_ref()
                .ok_or_else(|| Error::Unauthorized("selected tool has no execution owner".into()))?
                .verify_owner(None)?
                .await?;
            Ok(ToolResult {
                value: json!(self.answer),
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            Ok(None)
        })
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn default_file_dispatch_restores_its_writer_and_replays_without_another_effect(text in ".{0,48}") {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let mut fixture = Fixture::new().await?;
            let grants = Capabilities::new(fixture.scope.grants().iter()
                .chain(std::iter::once("tool:call:write")));
            fixture.signed = fixture.issuer.issue("owner", &grants, fixture.signed.agent(), None, &mut None)?;
            fixture.scope = RuntimeScope::new(grants, fixture.scope.limits())?
                .with_run_limits(fixture.scope.run_limits(), &mut None)?;
            fixture.tools = crate::tools::defaults()?;
            let runtime = fixture.runtime(None).await?;
            let input = serde_json::to_value(crate::filesystem::ToolTaskCommand {
                name:"write".into(), revision:fixture.tools.get("write", None, None)?.unwrap().definition.revision.clone(),
                arguments:json!({"path":"default.txt","text":text,"media_type":"text/plain","display_name":"default.txt"}),
            }).map_err(|error| Error::Invalid(error.to_string()))?;
            let lease = fixture.admit_command(&runtime, crate::filesystem::TOOL_TASK_COMMAND_KIND, input.clone()).await?;
            let lease = match runtime.resume_task(lease, &runtime.commands(), 1, None).await {
                TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) => lease,
                TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
                _ => return Err(Error::Invalid("write fixture did not yield its outbox".into())),
            };
            let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
            let fence = LeaseFence::from(&lease.reservation);
            let context = runtime.harness().durable_context(task, OperationId::from_bytes(task.into_bytes()), None).await?;
            assert!(context.content_binding(None).unwrap().writer.is_none());
            let admission = runtime.task_host().observe_admission(task).await?;
            let command: WorkflowCommand = serde_json::from_value(admission.input["command"].clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let answer = runtime.commands().execute(context.as_ref().clone(), fence.clone(), command.clone(), input.clone()).await?;
            let TaskCommandProgress::Ready(value) = answer else { return Err(Error::Invalid("write did not settle".into())); };
            let reference: crate::conversation::FileRef = serde_json::from_value(value["Succeeded"]["file"].clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(reference.volume(), &fixture.volume);
            assert_eq!(context.read_file(&reference, None).await?.as_ref(), text.as_bytes());
            let retry = runtime.commands().execute(context.as_ref().clone(), fence, command, input).await?;
            let TaskCommandProgress::Ready(retry) = retry else { return Err(Error::Invalid("write retry did not settle".into())); };
            assert_eq!(retry, value);
            Ok(())
        }).unwrap();
    }

    #[test]
    fn dispatch_retains_the_selected_content_provider_and_binds_its_owner(answer in ".{0,48}") {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let mut fixture = Fixture::new().await?;
            let verifier = fixture.issuer.verifier(&mut None)?;
            let reader = FilesystemContentVerifier::new(fixture.filesystem.clone(), &verifier,
                &fixture.signed, 65_536, None)?;
            let writer: Arc<dyn ContentPublisher> = FilesystemContentPublisher::new(
                fixture.filesystem.clone(), &fixture.volume, &verifier, &fixture.signed, 65_536, None)?;
            let definition = fixture.tools.get("test.deadline", Some("1"), None)?.unwrap().definition.clone();
            fixture.tools = ToolRegistry::default();
            fixture.tools.register((Tool {
                hook_names: &[], definition,
                executor: Arc::new(SelectedTool { authorization: fixture.tool.clone(), writer: writer.clone(), answer: answer.clone() }),
                projection: Arc::new(crate::tool::JsonToolProjection),
            }, None), &mut None, None)?;
            let runtime = fixture.runtime(None).await?;
            let lease = fixture.admit(&runtime).await?;
            let lease = match runtime.resume_task(lease, &runtime.commands(), 1, None).await {
                TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) => lease,
                TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
                _ => return Err(Error::Invalid("binding fixture did not yield its outbox".into())),
            };
            let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
            let fence = LeaseFence::from(&lease.reservation);
            let context = runtime.harness().durable_context(task, OperationId::from_bytes(task.into_bytes()), None).await?;
            assert!(context.owned_mailbox.is_none());
            let context = context.with_output_bindings(ContentBindings { reader, writer: Some(writer) }).await?;
            let admission = runtime.task_host().observe_admission(task).await?;
            let command = serde_json::from_value(admission.input["command"].clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let payload = serde_json::to_value(crate::filesystem::ToolTaskCommand {
                name: "test.deadline".into(), revision: "1".into(), arguments: Value::Null,
            }).map_err(|error| Error::Invalid(error.to_string()))?;
            let result = runtime.commands().execute(context.as_ref().clone(), fence, command, payload).await?;
            match result {
                TaskCommandProgress::Ready(value) => assert_eq!(value, json!({"Succeeded":answer})),
                _ => return Err(Error::Invalid("selected tool did not settle".into())),
            }
            Ok(())
        }).unwrap();
    }
}
