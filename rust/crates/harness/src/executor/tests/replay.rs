use super::*;

#[tokio::test]
async fn execution_journal_rejects_noncanonical_json_before_replay() -> Result<()> {
    let journal = Journal::default();
    let operation = OperationId::new();
    let reference = journal
        .stage(
            operation,
            "noncanonical",
            br#"{ "b":2,"a":1 }"#,
            "application/json",
            None,
        )?
        .await?;
    assert!(
        matches!(load_json::<Value>(&journal, &reference, None).await,
            Err(Error::Storage(message)) if message.contains("not canonical"))
    );
    Ok(())
}

#[tokio::test]
async fn admitted_replay_does_not_reload_or_transform_sources() -> Result<()> {
    let stage = Arc::new(OnceStage(AtomicUsize::new(0)));
    let provider = Arc::new(PrefixBoundaryModel::default());
    let executor = uncompacted_executor(
        Model::new("test".into(), "model".into(), "1".into(), Value::Null)?,
        provider.clone(),
        ContextPipeline::new([stage.clone() as Arc<dyn crate::context::ContextStage>]),
        ToolRegistry::default(),
    )
    .with_max_output_tokens(8_192)?;
    let journal = Journal::default();
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("exact ÃƒÂ©\0\r\n".into()),
        selected_context: None,
        max_steps: 1,
    };
    assert!(executor.model_step(&journal, &input, 1, &[]).await.is_err());
    let first = executor.model_step(&journal, &input, 0, &[]).await?;
    assert_eq!(first, executor.model_step(&journal, &input, 0, &[]).await?);
    assert_eq!(
        crate::model::PreparedModelRequest::decode(
            &provider.0.lock().unwrap()[0],
            Limits::default(),
            None,
        )?
        .request()
        .max_output_tokens,
        Some(8_192)
    );
    assert!(
        executor
            .clone()
            .with_max_output_tokens(16_384)?
            .model_step(&journal, &input, 0, &[])
            .await
            .is_err()
    );
    assert_eq!(stage.0.load(Ordering::SeqCst), 1);
    assert_eq!(provider.0.lock().unwrap().len(), 1);
    journal.1.lock().unwrap().clear();
    assert!(executor.execute(input, &journal).await.is_err());
    assert_eq!(stage.0.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn stock_prefix_reaches_provider_and_is_pinned_by_replay() -> Result<()> {
    let model = Model::new("example".into(), "model".into(), "1".into(), Value::Null)?;
    let root = crate::model::PreparedModelRequest::prepare(
        ModelRequest {
            model: model.clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("parent ÃƒÂ©\0Ã°Å¸Â¦â‚¬".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: Some(4096),
        },
        Limits::default(),
        None,
    )?;
    let journal = Arc::new(Journal::default());
    let root_file = journal
        .stage_source(
            OperationId::from_bytes([66; 16]),
            "root".into(),
            crate::model::ModelPrefix::select(&root, None)?.canonical_bytes(None)?,
            crate::model::ModelPrefix::MEDIA_TYPE,
        )
        .await?;
    let provider = Arc::new(PrefixBoundaryModel::default());
    let executor = uncompacted_executor(
        model,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_inherited_prefix(root_file, journal.clone())?;
    let input = TurnInput {
        operation_id: OperationId::from_bytes([67; 16]),
        input: ModelContent::Text("child task; fresh scratch".into()),
        selected_context: None,
        max_steps: 1,
    };
    executor.execute(input.clone(), journal.as_ref()).await?;
    executor.execute(input.clone(), journal.as_ref()).await?;
    let mut expected = root.request().clone();
    expected.max_output_tokens = None;
    expected.messages.push(ModelMessage {
        role: ModelRole::User,
        content: input.input.clone(),
    });
    let expected = crate::model::PreparedModelRequest::prepare(expected, Limits::default(), None)?;
    assert_eq!(*provider.0.lock().unwrap(), vec![expected.bytes().to_vec()]);
    let unbound = uncompacted_executor(
        root.request().model.clone(),
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    assert!(matches!(
        unbound.execute(input, journal.as_ref()).await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn stock_limits_bound_admission_and_pin_replay() -> Result<()> {
    let model = Arc::new(ScriptedModel::default());
    let tools = echo_tools(
        Arc::new(FakeTool(AtomicUsize::new(0))),
        json!({"type": "object"}),
        json!({"type": "object"}),
        crate::tool::json_projection_schema(json!({"type": "object"})),
    )?;
    let base = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
        model.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo"]),
            Limits::default(),
        )?,
        None,
    )?;
    let journal = Journal::default();
    let input = TurnInput {
        operation_id: OperationId::from_bytes([14; 16]),
        input: ModelContent::Text("bounded".into()),
        selected_context: None,
        max_steps: 2,
    };
    let narrow = Limits {
        model_steps: 1,
        ..Limits::default()
    };
    assert!(matches!(
        base.clone()
            .with_limits(narrow)
            .execute(input.clone(), &journal)
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    let _ = base.execute(input.clone(), &journal).await?;
    assert!(
        model
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.max_output_tokens.is_none())
    );
    let changed = Limits {
        model_steps: 3,
        ..Limits::default()
    };
    assert!(matches!(
        base.with_limits(changed).execute(input, &journal).await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn execution_pages_select_bounded_records_and_reject_cross_page_corruption() -> Result<()> {
    let journal = Journal::default();
    let operation = OperationId::from_bytes([29; 16]);
    for step in 0..70 {
        journal
            .append(
                operation,
                &format!("step-{step}"),
                &ExecutionEvent::ModelStarted {
                    step,
                    purpose: crate::executor::ModelPurpose::Response,
                    request_digest: [1; 32],
                    request: journal
                        .stage(operation, "page-fixture", b"null", "application/json", None)?
                        .await?,
                },
                None,
            )?
            .await?;
    }
    let (tail, selected) = replay_execution(
        &journal,
        operation,
        1,
        |event| matches!(event, ExecutionEvent::ModelStarted { step: 69, .. }),
        None,
    )
    .await?;
    assert_eq!(tail, 70);
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].sequence, 70);
    assert_eq!(journal.replay(operation, 0, 64, None)?.await?.len(), 64);
    assert_eq!(journal.replay(operation, 64, 64, None)?.await?.len(), 6);
    assert_eq!(journal.replay(operation, 0, 65, None)?.await?.len(), 65);
    assert!(journal.replay(operation, 0, 0, None)?.await.is_err());
    assert!(
        replay_execution(&journal, operation, 1, |_| true, None)
            .await
            .is_err()
    );
    let original = {
        let mut records = journal.0.lock().unwrap();
        let original = records[69].idempotency_key.clone();
        records[69].idempotency_key = records[0].idempotency_key.clone();
        original
    };
    assert!(matches!(
        replay_execution(&journal, operation, 0, |_| false, None).await,
        Err(Error::Conflict(_))
    ));
    let mut cursor = ExecutionReplay::new(operation);
    assert_eq!(
        cursor
            .next_page(&journal, None)
            .await?
            .map(|page| page.len()),
        Some(64)
    );
    for _ in 0..2 {
        assert!(matches!(
            cursor.next_page(&journal, None).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(
            cursor.tail(),
            64,
            "a corrupt page must not advance partially"
        );
    }
    {
        let mut records = journal.0.lock().unwrap();
        records[69].idempotency_key = original;
        records[69].sequence += 1;
    }
    assert!(matches!(
        replay_execution(&journal, operation, 0, |_| false, None).await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn finite_canonical_replay_rejects_unmeasured_custom_journal_before_work() -> Result<()> {
    let journal = Journal::default();
    let operation = OperationId::from_bytes([193; 16]);
    let cancellation = acyclic_fs::CancellationToken::new();
    let io = PreparationIo::from_budget(acyclic_fs::WorkBudget::default(), &cancellation)?;
    assert!(matches!(
        canonical_checkpoint_for_operation(&journal, operation, Limits::default(), Some(&io)).await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(io.receipt(), acyclic_fs::WorkCounters::default());
    assert_eq!(journal.3.load(Ordering::SeqCst), 0);
    assert_eq!(journal.4.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn prefix_replay_accepts_platform_maximum_counts() -> Result<()> {
    let mut executor = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
        Arc::new(ScriptedModel::default()),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    executor.limits.model_events_per_step = usize::MAX;
    executor.limits.tool_calls_per_step = usize::MAX;
    let journal = Journal::default();
    let operation = OperationId::from_bytes([27; 16]);
    journal
        .append(
            operation,
            &"execution:started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let records = StockExecution::ordinary(&executor)
        .prefix_records(&journal, operation, 0)
        .await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].sequence, 1);
    Ok(())
}

#[test]
fn independently_selected_event_budget_still_rejects_excess_observations() -> Result<()> {
    let limits = Limits {
        model_events_per_step: 1,
        ..Limits::default()
    };
    limits.validate(&mut None)?;
    let mut admission = ModelEventAdmission::default();
    let event = ModelEvent::ToolCall {
        call_id: "call-1".into(),
        name: "example.echo".into(),
        arguments: Value::Null,
    };
    admission.observe(&event, limits, None)?;
    assert!(matches!(
        admission.observe(&ModelEvent::Completed { metadata: Value::Null }, limits, None),
        Err(Error::Invalid(reason)) if reason == "model event limit exceeded"
    ));
    Ok(())
}

#[tokio::test]
async fn journal_retry_keys_and_sequences_are_operation_scoped() -> Result<()> {
    let journal = Journal::default();
    let first = OperationId::from_bytes([20; 16]);
    let second = OperationId::from_bytes([21; 16]);
    for (operation_id, digest) in [(first, [1; 32]), (second, [2; 32])] {
        journal
            .append(
                operation_id,
                &"execution:started",
                &ExecutionEvent::Started {
                    request_digest: digest,
                },
                None,
            )?
            .await?;
    }
    let replayed_first = journal
        .replay(first, 0, EXECUTION_REPLAY_PAGE_RECORDS, None)?
        .await?;
    let [first_entry] = replayed_first.as_slice() else {
        unreachable!("expected exactly one replayed event for the first operation");
    };
    assert_eq!(first_entry.sequence, 1);
    let replayed_second = journal
        .replay(second, 0, EXECUTION_REPLAY_PAGE_RECORDS, None)?
        .await?;
    let [second_entry] = replayed_second.as_slice() else {
        unreachable!("expected exactly one replayed event for the second operation");
    };
    assert_eq!(second_entry.sequence, 1);
    Ok(())
}

#[tokio::test]
async fn stock_file_authority_precedes_journal_replay() -> Result<()> {
    let scope = RuntimeScope::new(
        crate::Capabilities::new([
            "task:spawn:context-probe@1",
            "model:generate",
            "tool:call:acyclic.write_file",
        ]),
        Limits::default(),
    )?;
    let context = capture_admitted_context(scope.clone()).await?;
    let tool = crate::tool::files::write_file()?;
    let mut tools = ToolRegistry::default();
    tools.register((tool, None), &mut None, None)?;
    let operation = OperationId::new();
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "publication".into(),
        name: "acyclic.write_file".into(),
        arguments: json!({"path":"destination.txt","text":"body","media_type":"text/plain","display_name":"destination.txt"}),
    };
    let base = StockExecutor::new(
        Model::new("test".into(), "scoped".into(), "1".into(), Value::Null)?,
        Arc::new(ScriptedModel::default()),
        ContextPipeline::new(Vec::<Arc<dyn crate::context::ContextStage>>::new()),
        tools,
    )
    .with_tool_authority(scope, None)?;
    let journal = Journal::default();
    let mut messages = Vec::new();
    assert!(matches!(
        StockExecution::ordinary(&base)
            .settle_tool_call(&journal, operation, 0, invocation.clone(), &mut messages)
            .await,
        Err(Error::Unauthorized(_))
    ));
    let contextual = base.with_task_context(&context, operation)?;
    assert!(matches!(
        StockExecution::ordinary(&contextual)
            .settle_tool_call(&journal, operation, 0, invocation, &mut messages)
            .await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(journal.3.load(Ordering::SeqCst), 0);
    assert_eq!(journal.4.load(Ordering::SeqCst), 0);
    assert!(journal.0.lock().unwrap().is_empty());
    assert!(journal.1.lock().unwrap().is_empty());
    assert!(messages.is_empty());
    Ok(())
}
