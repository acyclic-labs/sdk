use super::*;
use acyclic_harness::tool::{JsonToolProjection, Tool, ToolExecutor, ToolInvocation, ToolResult};

struct Echo(AtomicUsize);

impl ToolExecutor for Echo {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<acyclic_harness::runtime::ToolContext>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ToolResult {
                value: serde_json::json!("after-cut"),
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<acyclic_harness::runtime::ToolContext>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

fn echo_tool(executor: Arc<Echo>) -> Tool {
    Tool {
        hook_names: &[],
        definition: ToolDefinition {
            name: "echo".into(),
            revision: "1".into(),
            description: "Return a bounded followup".into(),
            input_schema: serde_json::json!({"type":"object","additionalProperties":false}),
            output_schema: serde_json::json!({"type":"string"}),
            projection_schema: acyclic_harness::tool::json_projection_schema(
                serde_json::json!({"type":"string"}),
            ),
        },
        executor,
        projection: Arc::new(JsonToolProjection),
    }
}

struct StepInstruction(Arc<AtomicUsize>);

impl acyclic_harness::context::ContextStage for StepInstruction {
    fn name(&self) -> &str {
        "step-instruction"
    }

    fn contract(
        &self,
        _: Option<&acyclic_harness::conversation::ContentIo>,
    ) -> Result<serde_json::Value> {
        Ok(serde_json::json!({"name":self.name(),"revision":1}))
    }

    fn apply<'a>(
        &'a self,
        input: &'a acyclic_harness::context::ContextInput,
        mut context: Context,
        _: Limits,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, Context>> {
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            context.messages.insert(
                0,
                ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(format!("instruction for step {}", input.step)),
                },
            );
            context.current_input_index = context.current_input_index.map(|index| index + 1);
            Ok(context)
        })
    }
}

fn followup_model() -> Arc<ScriptedModelProvider> {
    summary_model_with_turns([
        ScriptedTurn::Text("reply".repeat(600)),
        ScriptedTurn::Text("retained summary".into()),
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(acyclic_harness::model::ToolCall::new(
                "cut-followup".into(),
                "echo".into(),
                serde_json::json!({}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ]),
        ScriptedTurn::Text("finished after cut".into()),
    ])
    .0
}

fn build_followup(
    storage: &MemoryHarnessStorage,
    provider: &Arc<ScriptedModelProvider>,
    tool: &Arc<Echo>,
    stages: &Arc<AtomicUsize>,
) -> Result<acyclic_harness::Harness> {
    storage
        .builder()
        .model(
            Model::new(
                "synthetic".into(),
                "byte-counter".into(),
                "1".into(),
                serde_json::json!({}),
            )?,
            provider.clone(),
        )
        .grant("model:generate")
        .grant("tool:call:echo")
        .without_default_tools()
        .tool(echo_tool(tool.clone()))
        .context(ContextPipeline::new([
            Arc::new(StepInstruction(stages.clone()))
                as Arc<dyn acyclic_harness::context::ContextStage>,
        ]))
        .build()
}

fn assert_followup(requests: &[ModelRequest], old: &FileRef) -> Result<()> {
    assert_eq!(requests.len(), 4, "the followup repeated the summary");
    let followup = requests
        .last()
        .ok_or_else(|| Error::Invalid("missing followup request".into()))?;
    assert!(
        !followup
            .messages
            .iter()
            .flat_map(|message| message.content.file_refs())
            .any(|file| file == old),
        "followup resurrected the compacted prefix"
    );
    assert_eq!(
        followup
            .messages
            .iter()
            .filter(|message| message.role == ModelRole::System)
            .count(),
        1
    );
    assert_eq!(
        followup.messages.first().map(|message| &message.content),
        Some(&ModelContent::Text("instruction for step 1".into()))
    );
    assert!(
        followup
            .messages
            .iter()
            .any(|message| message.role == ModelRole::Tool)
    );
    Ok(())
}

#[tokio::test]
async fn tool_followup_keeps_the_published_canonical_cut_after_resume() -> Result<()> {
    let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 131_072).await?);
    let provider = followup_model();
    let tool = Arc::new(Echo(AtomicUsize::new(0)));
    let stages = Arc::new(AtomicUsize::new(0));
    let bundle = build_followup(&storage, &provider, &tool, &stages)?;
    let first = OperationId::new();
    let old = storage
        .stage(
            first,
            "turns/old.txt",
            "q".repeat(60_000).as_bytes(),
            "text/plain",
            "old.txt",
        )
        .await?;
    storage
        .run_conversation(&bundle, first, old.clone(), Vec::new(), 1)
        .await?;
    let operation = OperationId::new();
    let input = storage
        .stage(
            operation,
            "turns/current.txt",
            "continue".repeat(7_500).as_bytes(),
            "text/plain",
            "current.txt",
        )
        .await?;
    let output = storage
        .run_conversation(&bundle, operation, input.clone(), Vec::new(), 2)
        .await?;
    assert_followup(&provider.requests(), &old)?;
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    let stage_calls = stages.load(Ordering::SeqCst);
    let reopened = build_followup(&storage, &provider, &tool, &stages)?;
    assert_eq!(
        storage
            .run_conversation(&reopened, operation, input, Vec::new(), 2)
            .await?,
        output
    );
    assert_eq!(provider.requests().len(), 4);
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    assert_eq!(stages.load(Ordering::SeqCst), stage_calls);
    Ok(())
}
