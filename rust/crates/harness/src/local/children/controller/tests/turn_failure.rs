use super::*;
use crate::{
    context::ModelContextCapacity,
    distributed::{WorkPull, Worker},
    filesystem::TaskCommandProgress,
    model::{Model, ModelEvent, ToolCall},
    runtime::{RuntimeTask, TaskContext},
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
};
use crate::{
    hooks::{HookEvent, HookInput, HookOutput, Hooks},
    runtime::ToolContext,
    tool::{ToolExecutor, ToolInvocation, ToolRegistry, ToolResult},
};
use serde_json::Value;

struct RejectedHook;
impl ToolExecutor for RejectedHook {
    fn run_hooks<'a>(
        &'a self,
        _: (&'a Hooks, [u8; 32]),
        event: HookEvent,
        _: HookInput<'a>,
        _: &'a ToolRegistry,
        _: ToolContext,
        _: bool,
    ) -> BoxProviderFuture<'a, Result<Option<HookOutput>>> {
        Box::pin(async move {
            if event == HookEvent::PreToolUse {
                Err(Error::Unauthorized("scripted child hook rejection".into()))
            } else {
                Ok(Some(HookOutput::default()))
            }
        })
    }
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async { panic!("rejected child tool must not execute") })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

struct WaitingParent<'a>(&'a dyn TaskCommandHost, &'a ToolRegistry);
impl TaskCommandHost for WaitingParent<'_> {
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            if command.kind != crate::filesystem::MODEL_TASK_COMMAND_KIND {
                return self.0.execute(context, fence, command, payload).await;
            }
            let cancellation = context.invocation_cancellation().unwrap().clone();
            let context = Arc::new(context);
            let tool = self.1.get("wait", None, None)?.unwrap();
            let invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "parent-wait".into(),
                name: "wait".into(),
                arguments: serde_json::json!({"agents":["failed"],"mode":"all"}),
            };
            let issued = ToolContext::new(
                context.clone(),
                invocation.operation_id,
                &invocation.call_id,
                None,
            )?;
            let result = tool.executor.execute(invocation, Some(issued)).await?;
            assert!(
                !cancellation.is_cancelled(),
                "child failure cancelled parent invocation"
            );
            assert!(
                result.value["agents"][0]["result"]["outcome"]["Failed"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("scripted child hook rejection"),
                "{result:?}"
            );
            // A fresh publication after wait retains the parent's authority.
            context
                .ordinary_journal_owner::<LocalStream>()?
                .condition(crate::distributed::JournalWrite::Fresh, &mut None)
                .await?;
            Ok(TaskCommandProgress::Ready(serde_json::json!({"Succeeded":{
                "text":"parent completed", "metadata":null,"steps":1,"attachments":[]
            }})))
        })
    }
}

#[tokio::test]
async fn child_tool_failure_is_a_typed_wait_result_and_parent_completes() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let home = tempfile::tempdir().map_err(storage)?;
    std::fs::create_dir_all(home.path().join(".claude")).map_err(storage)?;
    std::fs::write(home.path().join(".claude/settings.json"), serde_json::json!({"hooks":{
        "PreToolUse":[{"matcher":"Bash","hooks":[{"type":"command","command":"unused-hook","onFailure":"block"}]}]
    }}).to_string()).map_err(storage)?;
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                    "child-tool".into(),
                    "bash".into(),
                    serde_json::json!({"command":"true"}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]),
            ScriptedTurn::Text("child recovered".into()),
        ])
        .with_context_capacity(ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: Some(4096),
        }),
    );
    let model = Model::new(
        "scripted".into(),
        "child-failure".into(),
        "1".into(),
        Value::Null,
    )?;
    let config =
        crate::test_support::local_harness_config(directory.path(), (model, provider.clone()))?
            .settings(
                crate::native_settings::SettingsConfig::new(directory.path(), home.path())
                    .codex_home(home.path().join(".codex")),
            );
    let mut harness = LocalHarness::open(config).await?;
    harness.runtime = Arc::new(
        Arc::try_unwrap(harness.runtime)
            .map_err(|_| Error::Conflict("test runtime shared".into()))?
            .with_workspace_tools(Arc::new(RejectedHook))?,
    );
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = harness.submit(operation, "parent").await?
    else {
        panic!("parent admission")
    };
    let host = harness.runtime.task_host();
    let worker = Worker {
        id: "child-failure-wait".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
    else {
        panic!("parent lease")
    };
    host.start_task(&lease, &mut None).await?;
    let context = harness
        .runtime
        .task_context(parent, LeaseFence::from(&lease.reservation))
        .await?;
    let files = harness.task_files.clone();
    let reference = Component::new("failed").map_err(storage)?;
    let request = file(
        &reference,
        ChildFile::Request,
        b"fail during tool call".to_vec(),
    );
    let cut = files.inner.head().await.map_err(storage)?;
    let (condition, deadline) = context
        .owned_mailbox
        .as_ref()
        .unwrap()
        .file_publication_condition()
        .await?;
    files
        .write_if(cut, &request, parent, condition, deadline)
        .await
        .map_err(storage)?;
    let inner = harness.commands()?;
    harness
        .advance_with_children(
            parent,
            &WaitingParent(&inner, harness.tools()),
            files.clone(),
            None,
        )
        .await?;
    assert!(matches!(
        host.outcome(parent).await?,
        Some(crate::Outcome::Succeeded(_))
    ));
    let snapshot = files
        .observe(Query::Child(reference.clone()), None, 16)
        .await
        .map_err(storage)?
        .snapshot
        .unwrap();
    let values = snapshot
        .files
        .into_iter()
        .map(|f| (f.key.clone(), f))
        .collect();
    let status = execution(&reference, &values)?.unwrap();
    assert!(status.settled);
    assert_eq!(status.inputs, 1);
    let result = &values[&FileKey::Child {
        reference: reference.clone(),
        file: ChildFile::Result(1),
    }];
    let result = crate::task_volume::ChildTurnResult::parse(&result.bytes).map_err(storage)?;
    assert!(
        matches!(result.outcome,crate::Outcome::Failed {ref message} if message.contains("scripted child hook rejection"))
    );
    // The failed turn leaves the same child admission available for more input.
    let cut = files.inner.head().await.map_err(storage)?;
    files
        .write(
            cut,
            &file(
                &reference,
                ChildFile::Inbox(Component::new("next").map_err(storage)?),
                b"continue".to_vec(),
            ),
        )
        .await
        .map_err(storage)?;
    let request = files
        .inner
        .child_request(&reference)
        .await
        .map_err(storage)?;
    harness
        .service_child(files.clone(), reference.clone(), request, None)
        .await?;
    let snapshot = files
        .observe(Query::Child(reference), None, 16)
        .await
        .map_err(storage)?
        .snapshot
        .unwrap();
    assert!(snapshot.files.iter().any(|f| matches!(
        f.key,
        FileKey::Child {
            file: ChildFile::Result(2),
            ..
        }
    )));
    host.cancel(status.task).await?;
    Ok(())
}
