use super::*;
use acyclic_harness::{
    filesystem::{FilesystemContentPublisher, FilesystemContentVerifier},
    model::ToolResultContent,
    runtime::{AgentHarness, ContentBindings, TaskContext, TaskDefinition},
    tool::{files::FileResult, plan::update_plan},
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct Case {
    name: String,
    updates: Vec<Value>,
    markdown: Vec<String>,
}

struct Fixture {
    context: Arc<TaskContext>,
    harness: Arc<AgentHarness>,
}

async fn fixture() -> Result<Fixture> {
    let agent = AgentId::new();
    let (host, private, issuer) = filesystem("plan-e2e", "plan-owner", agent, 81).await?;
    let signed = scope(&issuer, &private, agent, &["tool:call:update_plan".into()])?;
    let verifier = issuer.verifier(&mut None)?;
    let reader = FilesystemContentVerifier::new(host.clone(), &verifier, &signed, 65_536, None)?;
    let writer =
        FilesystemContentPublisher::new(host.clone(), &private, &verifier, &signed, 65_536, None)?;
    let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?;
    let journal: Arc<dyn ExecutionJournal> = Arc::new(FilesystemExecutionJournal::new(
        Arc::new(MemoryStream::default()),
        host,
        &SchemaRegistry::new(),
        acyclic_harness::filesystem::FilesystemJournalBinding {
            volume: &private,
            verifier: &verifier,
            scope: &signed,
            maximum_bytes: 65_536,
            io: None,
        },
    )?);
    let mut tools = ToolRegistry::default();
    tools.register((update_plan()?, None), &mut None, None)?;
    let mut bindings = Bindings::local();
    bindings.scope = scope.clone();
    bindings.state = Some(Arc::new(TestTaskState(scope.clone())));
    bindings.content = Some(ContentBindings {
        reader,
        writer: Some(writer),
    });
    bindings.tasks.register(TaskDefinition::live(
        "test.plan",
        "1",
        move |context, case: Case| {
            let journal = journal.clone();
            let tools = tools.clone();
            let scope = scope.clone();
            async move { execute_case(case, Arc::new(context), journal, tools, scope).await }
        },
    )?)?;
    let harness = bindings.build(None)?;
    let context = harness
        .durable_context(TaskId::new(), OperationId::new(), None)
        .await?;
    Ok(Fixture { context, harness })
}

#[tokio::test]
async fn plan_scenarios_execute_model_tools_publish_markdown_and_replay() -> Result<()> {
    let cases: Vec<Case> = serde_json::from_str(include_str!("../fixtures/plans.json"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    futures::future::try_join_all(cases.into_iter().map(run_case)).await?;
    Ok(())
}

async fn run_case(case: Case) -> Result<()> {
    let fixture = fixture().await?;
    let definition = fixture.harness.task::<Case, ()>("test.plan")?;
    let outcome = fixture
        .harness
        .spawn(&definition, case)
        .await?
        .result()
        .await?;
    assert_eq!(outcome, Outcome::Succeeded(()));
    Ok(())
}

async fn execute_case(
    case: Case,
    context: Arc<TaskContext>,
    journal: Arc<dyn ExecutionJournal>,
    tools: ToolRegistry,
    scope: RuntimeScope,
) -> Result<()> {
    let turns = case
        .updates
        .iter()
        .enumerate()
        .map(|(index, arguments)| {
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall {
                    call_id: format!("plan-{index}"),
                    name: "update_plan".into(),
                    arguments: arguments.clone(),
                }),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ])
        })
        .chain([ScriptedTurn::Text("Done".into())]);
    let provider = Arc::new(ScriptedModelProvider::new(turns));
    let executor = StockExecutor::new(
        Model::new("test".into(), "plan".into(), "1".into(), Value::Null)?,
        provider.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_compaction_policy(acyclic_harness::context::CompactionPolicy::Disabled)
    .with_tool_authority(scope, None)?;
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text(case.name.clone()),
        selected_context: None,
        max_steps: u32::try_from(case.updates.len() + 1)
            .map_err(|e| Error::Invalid(e.to_string()))?,
    };
    let result = executor
        .execute_with_context(&context, input.clone(), journal.as_ref())
        .await?;
    assert_eq!(result.text, "Done", "{}", case.name);
    assert_eq!(provider.requests().len(), case.updates.len() + 1);
    assert_eq!(provider.remaining(), 0);
    assert_publications(&case, &context, journal.as_ref(), input.operation_id).await?;
    let replay = executor
        .execute_with_context(&context, input, journal.as_ref())
        .await?;
    assert_eq!(result, replay);
    assert_eq!(provider.requests().len(), case.updates.len() + 1);
    Ok(())
}

async fn assert_publications(
    case: &Case,
    context: &TaskContext,
    journal: &dyn ExecutionJournal,
    operation: OperationId,
) -> Result<()> {
    let records = journal.replay(operation, 0, 128, None)?.await?;
    let mut files = Vec::new();
    for record in records {
        if let ExecutionEvent::ToolCompleted {
            result, projection, ..
        } = record.event
        {
            let bytes = journal.load(&result, None)?.await?;
            let result: ToolResult =
                serde_json::from_slice(&bytes).map_err(|e| Error::Invalid(e.to_string()))?;
            let publication: FileResult =
                serde_json::from_value(result.value).map_err(|e| Error::Invalid(e.to_string()))?;
            let projected = journal.load(&projection, None)?.await?;
            let projected: ToolResultContent =
                serde_json::from_slice(&projected).map_err(|e| Error::Invalid(e.to_string()))?;
            assert_eq!(
                projected,
                ToolResultContent::Json {
                    value: json!({"file":publication.file})
                }
            );
            assert_eq!(publication.file.path(), "plan.md");
            assert_eq!(publication.file.volume().class(), VolumeClass::AgentPrivate);
            assert_eq!(publication.file.descriptor().media_type(), "text/markdown");
            files.push(publication.file);
        }
    }
    assert_eq!(files.len(), case.markdown.len(), "{}", case.name);
    for (file, expected) in files.iter().zip(&case.markdown) {
        assert_eq!(
            context.read_file(file, None).await?.as_ref(),
            expected.as_bytes()
        );
    }
    let last = files
        .last()
        .ok_or_else(|| Error::Invalid("scenario published no plan".into()))?;
    let (current, bytes) = context
        .read_private_path(last.volume(), "", "plan.md", None)
        .await?;
    // Journal records advance the volume generation after publishing the plan.
    assert_ne!(current.version(), last.version());
    assert_eq!(current.volume(), last.volume());
    assert_eq!(current.path(), last.path());
    assert_eq!(current.descriptor(), last.descriptor());
    assert_eq!(current.display_name(), last.display_name());
    assert_eq!(
        Some(bytes.as_ref()),
        case.markdown.last().map(String::as_bytes)
    );
    Ok(())
}

#[tokio::test]
async fn plan_requires_original_context_and_write_authority() -> Result<()> {
    let fixture = fixture().await?;
    let tool = update_plan()?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "plan".into(),
        name: "update_plan".into(),
        arguments: json!({"plan":[]}),
    };
    assert!(
        tool.executor
            .execute(invocation.clone(), None)
            .await
            .is_err()
    );
    let wrong = ToolContext::new(fixture.context.clone(), OperationId::new(), "plan", None)?;
    assert!(
        tool.executor
            .execute(invocation.clone(), Some(wrong))
            .await
            .is_err()
    );
    let restricted = fixture.context.scoped(
        Capabilities::new(["tool:call:update_plan"]),
        Limits::default(),
    )?;
    let readonly = ToolContext::new(Arc::new(restricted), invocation.operation_id, "plan", None)?;
    assert!(
        tool.executor
            .execute(invocation, Some(readonly))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn plan_reconciliation_observes_original_without_overwriting_a_later_edit() -> Result<()> {
    let fixture = fixture().await?;
    let tool = update_plan()?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "plan".into(),
        name: "update_plan".into(),
        arguments: json!({"plan":[]}),
    };
    let context = ToolContext::new(
        fixture.context.clone(),
        invocation.operation_id,
        "plan",
        None,
    )?;
    let first = tool
        .executor
        .execute(invocation.clone(), Some(context.clone()))
        .await?;
    let changed = fixture
        .context
        .stage_file(
            None,
            OperationId::new(),
            "plan.md",
            b"# User plan\n",
            "text/markdown",
            "plan.md",
        )
        .await?;
    let replay = tool.executor.reconcile(invocation, Some(context)).await?;
    assert_eq!(replay, Some(first));
    let (current, bytes) = fixture
        .context
        .read_private_path(changed.volume(), "", "plan.md", None)
        .await?;
    assert_eq!(current, changed);
    assert_eq!(bytes.as_ref(), b"# User plan\n");
    Ok(())
}
