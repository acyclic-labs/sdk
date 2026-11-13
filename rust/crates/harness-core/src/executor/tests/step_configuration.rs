use super::*;

struct Configurations([Arc<StockExecutor>; 2]);

impl StepConfiguration for Configurations {
    fn select<'a>(
        &'a self,
        _: &'a dyn ExecutionJournal,
        _: OperationId,
        step: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<Arc<StockExecutor>>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            Ok(Some(Arc::clone(&self.0[usize::from(step != 0)])))
        })
    }
}

#[tokio::test]
async fn a_step_rebinds_the_complete_provider_configuration_without_restarting_the_turn()
-> Result<()> {
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let first = Arc::new(ScriptedModelProvider::new([tool_turn([echo_call(
        "change",
        json!({"value": "change configuration"}),
    )])]));
    let second = Arc::new(ScriptedModelProvider::new([done_turn()]));
    let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
    let tools = echo_tools(
        tool.clone(),
        json!({"type": "object"}),
        json!({"type": "object"}),
        crate::tool::json_projection_schema(json!({"type": "object"})),
    )?;
    let bind = |name: &str, provider: Arc<dyn ModelProvider>, temperature| {
        uncompacted_executor(
            Model::new("fixture".into(), name.into(), "1".into())?,
            provider,
            ContextPipeline::default(),
            tools.clone(),
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits::default(),
            )?,
            allowed()?,
        )?
        .with_sampling(crate::model::ModelSampling {
            temperature: Some(temperature),
            ..Default::default()
        })
    };
    let initial = Arc::new(bind("first", first.clone(), 0.1)?);
    let next = Arc::new(bind("second", second.clone(), 0.8)?);
    let executor = initial
        .as_ref()
        .clone()
        .with_step_configuration(Arc::new(Configurations([initial, next])));
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("question".into()),
        selected_context: None,
        max_steps: None,
    };
    assert!(matches!(
        executor
            .execute_progress(input.clone(), journal.as_ref())
            .await?,
        ExecutionProgress::Ready(_)
    ));
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    let first_requests = first.requests();
    let second_requests = second.requests();
    assert_eq!((first_requests.len(), second_requests.len()), (1, 1));
    assert_eq!(first_requests[0].model.name, "first");
    assert_eq!(second_requests[0].model.name, "second");
    assert_eq!(first_requests[0].sampling.temperature, Some(0.1));
    assert_eq!(second_requests[0].sampling.temperature, Some(0.8));
    assert!(second_requests[0].messages.iter().any(|message| matches!(
        &message.content, ModelContent::Part(ModelContentPart::ToolResult { call_id, .. })
        if call_id == "change"
    )));
    assert!(matches!(
        executor.execute_progress(input, journal.as_ref()).await?,
        ExecutionProgress::Ready(_)
    ));
    assert_eq!((first.requests().len(), second.requests().len()), (1, 1));
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    Ok(())
}
