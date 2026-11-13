use super::*;

struct Configurations([Arc<StepBinding>; 2]);

impl StepConfiguration for Configurations {
    fn select<'a>(
        &'a self,
        _: &'a dyn ExecutionJournal,
        _: OperationId,
        step: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<Arc<StepBinding>>>> {
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
        let second = Arc::new(ScriptedModelProvider::new([
            tool_turn([echo_call(
                "after-change",
                json!({"value": "use original authority"}),
            )]),
            done_turn(),
        ]));
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
        let initial = bind("fixture/first", 0.1, instruction)?;
        let next = bind(
            "fixture/second",
            0.8,
            instruction.map(|_| "rewritten instruction"),
        )?
        .without_tool_authority()
        .into_step_binding(None)?;
        let executor = initial
            .clone()
            .with_step_configuration(Arc::new(Configurations([
                initial.into_step_binding(None)?,
                Arc::clone(&next),
            ])));
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
        assert_eq!(tool.0.load(Ordering::SeqCst), 2);
        let first_requests = first.requests();
        let second_requests = second.requests();
        assert_eq!((first_requests.len(), second_requests.len()), (1, 2));
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
        selected.selected = Some(next);
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
        assert_eq!((first.requests().len(), second.requests().len()), (1, 2));
        assert_eq!(tool.0.load(Ordering::SeqCst), 2);
    }
    #[cfg(not(target_arch = "wasm32"))]
    for denied_at in [0, 1] {
        let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
        let journal = storage.journal();
        let first = Arc::new(ScriptedModelProvider::new([tool_turn([echo_call(
            "before-denial",
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
        let compose = |name: &str, provider: Arc<dyn ModelProvider>| -> Result<StockExecutor> {
            StockExecutor::new(
                Model::new("fixture".into(), name.into(), "1".into())?,
                provider,
                ContextPipeline::default(),
                tools.clone(),
            )
            .with_compaction_policy(crate::context::CompactionPolicy::Disabled)
            .with_tool_authority(
                RuntimeScope::new(
                    Capabilities::new(["tool:call:example.echo"]),
                    Limits::default(),
                )?,
                allowed()?,
            )
        };
        let initial = compose("before-denial", first.clone())?;
        let mut selected = [initial.clone(), compose("after-denial", second.clone())?];
        // This is the admitted denial retained by with_agent_hooks. Selecting
        // it must enforce that decision before a model request, also on replay.
        selected[denied_at].prompt_block = Some("selected prompt denied".into());
        let [first_binding, second_binding] = selected;
        let executor = initial.with_step_configuration(Arc::new(Configurations([
            first_binding.into_step_binding(None)?,
            second_binding.into_step_binding(None)?,
        ])));
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text("question".into()),
            selected_context: None,
            max_steps: None,
        };
        for _ in 0..2 {
            let result = executor.execute(input.clone(), journal.as_ref()).await;
            assert!(
                matches!(result, Err(Error::Invalid(ref reason)) if reason == "selected prompt denied")
            );
            assert_eq!(first.requests().len(), denied_at);
            assert_eq!(tool.0.load(Ordering::SeqCst), denied_at);
            assert!(
                second.requests().is_empty(),
                "selected prompt denial reached the model"
            );
        }
    }
    // Both selections fit the original window. Only the replacement's window
    // refuses the mandatory question after the first tool has changed settings.
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let first = Arc::new(ScriptedModelProvider::new([tool_turn([echo_call(
        "change-window",
        json!({"value": "select smaller model"}),
    )])]));
    let second = Arc::new(
        ScriptedModelProvider::new([done_turn()])
            .with_context_capacity(crate::context::ModelContextCapacity {
                context_tokens: 1,
                output_tokens: None,
            })
            .with_token_counter(fixture_token_count),
    );
    let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
    let tools = echo_tools(
        tool.clone(),
        json!({"type": "object"}),
        json!({"type": "object"}),
        crate::tool::json_projection_schema(json!({"type": "object"})),
    )?;
    let compose = |name: &str, provider: Arc<dyn ModelProvider>| -> Result<StockExecutor> {
        StockExecutor::new(
            Model::new("fixture".into(), name.into(), "1".into())?,
            provider,
            ContextPipeline::default(),
            tools.clone(),
        )
        .with_compaction_policy(crate::context::CompactionPolicy::default())
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits::default(),
            )?,
            allowed()?,
        )
    };
    let original = compose("large-window", first.clone())?;
    let executor = original
        .clone()
        .with_step_configuration(Arc::new(Configurations([
            original.into_step_binding(None)?,
            compose("small-window", second.clone())?.into_step_binding(None)?,
        ])));
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("mandatory question".into()),
        selected_context: None,
        max_steps: None,
    };
    let refused = executor.execute_progress(input, journal.as_ref()).await?;
    assert!(
        matches!(refused, ExecutionProgress::PreDispatchFailed(failure)
        if failure.message().ends_with("mandatory content exceeds selected model capacity")),
        "a model change must re-admit the mandatory input against its selected window"
    );
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    assert_eq!(first.requests().len(), 1);
    assert!(
        second.requests().is_empty(),
        "oversized input reached the new provider"
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancel = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                peak_allocation_bytes: 1_000_000,
                ..acyclic_fs::WorkBudget::UNBOUNDED
            },
            &cancel,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let configuration = || -> Result<StockExecutor> {
            Ok(StockExecutor::new(
                Model::new("fixture".into(), "owned".into(), "1".into())?,
                Arc::new(ScriptedModelProvider::new([])),
                ContextPipeline::default(),
                ToolRegistry::default(),
            ))
        };
        let before = work.live_allocation_bytes();
        let bound = configuration()?.into_step_binding(Some(&io))?;
        assert!(work.live_allocation_bytes() > before);
        drop(bound);
        assert_eq!(work.live_allocation_bytes(), before);
        let occupied = work.claim(
            work.remaining()?.peak_allocation_bytes,
            1,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let refused = configuration()?.into_step_binding(Some(&io));
        assert!(matches!(
            refused,
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes",
                        ..
                    }
                ),
                ..
            })
        ));
        drop(occupied);
        assert_eq!(work.live_allocation_bytes(), before);
        cancel.cancel();
        let refused = configuration()?.into_step_binding(Some(&io));
        assert!(
            matches!(refused, Err(ref error) if error.code() == crate::wire::ErrorCode::Cancelled)
        );
        assert_eq!(work.live_allocation_bytes(), before);
    }
    Ok(())
}
