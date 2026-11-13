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
    for instruction in [None, Some("original instruction")] {
        let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
        let journal = storage.journal();
        let first = Arc::new(ScriptedModelProvider::new([tool_turn([echo_call(
            "change",
            json!({"value": "change configuration"}),
        )])]));
        let second = Arc::new(ScriptedModelProvider::new([done_turn()]));
        let models = crate::model::ModelResolver::new("fixture/first", {
            let first = Arc::clone(&first);
            let second = Arc::clone(&second);
            move |selection| {
                let (name, provider): (_, Arc<dyn ModelProvider>) = match selection {
                    "fixture/first" => ("first", first.clone()),
                    "fixture/second" => ("second", second.clone()),
                    _ => return Err(Error::NotFound(selection.into())),
                };
                Ok((
                    Model::new("fixture".into(), name.into(), "1".into())?,
                    provider,
                ))
            }
        });
        let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
        let tools = echo_tools(
            tool.clone(),
            json!({"type": "object"}),
            json!({"type": "object"}),
            crate::tool::json_projection_schema(json!({"type": "object"})),
        )?;
        let bind = |selection: &str, temperature, instruction: Option<&str>| {
            StockExecutor::from_binding(
                models.resolve_shared(selection)?,
                instruction.map_or_else(ContextPipeline::default, |text| {
                    ContextPipeline::default().with(Arc::new(crate::context::SourceStage::new(
                        "instructions",
                        blake3::hash(text.as_bytes()).to_hex().to_string(),
                        Arc::new(crate::context::Context {
                            messages: vec![ModelMessage {
                                role: ModelRole::System,
                                content: ModelContent::Text(text.into()),
                            }],
                            ..Default::default()
                        }),
                        crate::context::ContextPlacement::Prepend,
                    )))
                }),
                tools.clone(),
            )
            .with_compaction_policy(crate::context::CompactionPolicy::Disabled)
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
        let initial = Arc::new(bind("fixture/first", 0.1, instruction)?);
        let next = Arc::new(bind(
            "fixture/second",
            0.8,
            instruction.map(|_| "rewritten instruction"),
        )?);
        let executor = initial
            .as_ref()
            .clone()
            .with_step_configuration(Arc::new(Configurations([initial, Arc::clone(&next)])));
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
        if let Some(original) = instruction {
            for request in [&first_requests[0], &second_requests[0]] {
                assert!(matches!(
                    request.messages.first().map(|message| &message.content),
                    Some(ModelContent::Text(text)) if text == original
                ));
                assert!(!request.messages.iter().any(|message| matches!(
                    &message.content, ModelContent::Text(text) if text == "rewritten instruction"
                )));
            }
        }
        assert!(second_requests[0].messages.iter().any(|message| matches!(
            &message.content, ModelContent::Part(ModelContentPart::ToolResult { call_id, .. })
            if call_id == "change"
        )));
        // Summary entry and retained working cuts both recheck this parent
        // identity after a step changes the selected model and sampling.
        let mut selected = StockExecution::ordinary(&executor);
        let original_identity = selected.request_digest(&input)?;
        selected.stock = ExecutionConfiguration::Shared(next);
        assert_eq!(selected.request_digest(&input)?, original_identity);
        selected.ensure_started(journal.as_ref(), &input).await?;
        let mut different_input = input.clone();
        different_input.input = ModelContent::Text("different question".into());
        assert!(
            selected
                .ensure_started(journal.as_ref(), &different_input)
                .await
                .is_err()
        );
        assert!(matches!(
            executor.execute_progress(input, journal.as_ref()).await?,
            ExecutionProgress::Ready(_)
        ));
        assert_eq!((first.requests().len(), second.requests().len()), (1, 1));
        assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
