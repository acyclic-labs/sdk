use super::*;

#[tokio::test]
async fn original_context_survives_tool_response_loss_and_rejects_retargeted_replay() -> Result<()>
{
    let scope = RuntimeScope::new(
        crate::Capabilities::new([
            "task:spawn:context-probe@1",
            "model:generate",
            "tool:call:example.echo",
        ]),
        Limits::default(),
    )?;
    let original = capture_admitted_context(scope.clone()).await?;
    let replacement = capture_admitted_context(scope.clone()).await?;
    assert_ne!(original.id(), replacement.id());
    let tool = Arc::new(ContextOnlyTool {
        admission: original.id(),
        calls: AtomicUsize::new(0),
        reconciles: AtomicUsize::new(0),
    });
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            Tool {
                definition: ToolDefinition {
                    name: "example.echo".into(),
                    revision: "1".into(),
                    description: "Echo".into(),
                    input_schema: json!({"type":"object"}),
                    output_schema: json!({"type":"object"}),
                    projection_schema: crate::tool::json_projection_schema(
                        json!({"type":"object"}),
                    ),
                },
                executor: tool.clone(),
                projection: Arc::new(Projection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let model = Arc::new(ScriptedModel::default());
    let base = StockExecutor::new(
        Model::new("fixture".into(), "exact".into(), "1".into(), Value::Null)?,
        model.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(scope.clone(), None)?;
    let operation = OperationId::new();
    let executor = base.clone().with_task_context(&original, operation)?;
    let input = TurnInput {
        operation_id: operation,
        input: ModelContent::Text("input".into()),
        selected_context: None,
        max_steps: 2,
    };
    let journal = Journal::default();
    let previous = Journal::default();
    StockExecution::ordinary(&base)
        .ensure_started(&previous, &input)
        .await?;
    assert!(matches!(
        StockExecution::ordinary(&executor)
            .ensure_started(&previous, &input)
            .await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        executor.execute(input.clone(), &journal).await,
        Err(Error::Indeterminate(_))
    ));
    let retained = journal.0.lock().unwrap().len();
    let retargeted = base.clone().with_task_context(&replacement, operation)?;
    assert!(matches!(
        retargeted.execute(input.clone(), &journal).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(journal.0.lock().unwrap().len(), retained);
    let narrowed = scope.with_run_limits(
        crate::runtime::TaskRunLimits {
            max_steps: Some(1),
            ..Default::default()
        },
        &mut None,
    )?;
    let retargeted = executor.clone().with_tool_authority(narrowed, None)?;
    let reads = journal.3.load(Ordering::SeqCst);
    assert!(matches!(
        retargeted.execute(input.clone(), &journal).await,
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        retargeted
            .completed_tool_prefix(&journal, &input, 0, Some("call-1"))
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(journal.0.lock().unwrap().len(), retained);
    assert_eq!(journal.3.load(Ordering::SeqCst), reads);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let result = executor.execute(input.clone(), &journal).await?;
    assert_eq!(result.text, "done");
    let reads = journal.3.load(Ordering::SeqCst);
    executor
        .completed_tool_prefix(&journal, &input, 0, Some("call-1"))
        .await?;
    assert!(journal.3.load(Ordering::SeqCst) > reads);
    assert_eq!(executor.execute(input, &journal).await?, result);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(tool.reconciles.load(Ordering::SeqCst), 1);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn stock_approval_wait_distinguishes_pending_and_known_rejection() -> Result<()> {
    struct ApprovalPolicy;
    impl ToolPolicy for ApprovalPolicy {
        fn identity(&self) -> ComponentIdentity {
            ComponentIdentity {
                name: "test.approval".into(),
                version: "1".into(),
                digest: [7; 32],
            }
        }
        fn evaluate<'a>(
            &'a self,
            _: &'a ToolInvocation,
            _: &'a RuntimeScope,
        ) -> BoxFuture<'a, Result<ToolPolicyDecision>> {
            Box::pin(async {
                Ok(ToolPolicyDecision::RequireApproval {
                    prompt: "Approve echo".into(),
                })
            })
        }
    }
    for outcome in [InteractionOutcome::Approved, InteractionOutcome::Denied] {
        let model = Arc::new(ScriptedModel::default());
        let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
        let mut tools = ToolRegistry::default();
        tools.register(
            (
                crate::tool::Tool {
                    definition: crate::tool::ToolDefinition {
                        name: "example.echo".into(),
                        revision: "1".into(),
                        description: "Echo".into(),
                        input_schema: json!({"type": "object"}),
                        output_schema: json!({"type": "object"}),
                        projection_schema: crate::tool::json_projection_schema(
                            json!({"type": "object"}),
                        ),
                    },
                    executor: tool_executor.clone(),
                    projection: Arc::new(Projection),
                },
                None,
            ),
            &mut None,
            None,
        )?;
        let executor = uncompacted_executor(
            Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo", capability::INTERACTION_ROUTE]),
                Limits::default(),
            )?,
            Some(Arc::new(ApprovalPolicy)),
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([28; 16]),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 4,
        };
        let invocation = ToolInvocation::for_model_call(
            input.operation_id,
            0,
            "call-1".into(),
            "example.echo".into(),
            json!({"value":"hello"}),
            None,
        )?;
        for _ in 0..2 {
            assert!(
                matches!(executor.execute_progress(input.clone(), &journal).await?, ExecutionProgress::Pending(operation) if operation == invocation.operation_id)
            );
        }
        assert!(
            matches!(executor.execute(input.clone(), &journal).await, Err(Error::Indeterminate(operation)) if operation == invocation.operation_id)
        );
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
        let (id, request) = {
            let interactions = journal.2.lock().unwrap();
            assert_eq!(interactions.len(), 1);
            interactions
                .iter()
                .next()
                .map(|(id, (request, _))| (*id, request.clone()))
                .ok_or_else(|| Error::NotFound("approval".into()))?
        };
        assert!(
            matches!(request, Interaction::Approval { operation_id, .. } if operation_id == invocation.operation_id)
        );
        journal
            .2
            .lock()
            .unwrap()
            .get_mut(&id)
            .ok_or_else(|| Error::NotFound("approval".into()))?
            .1 = Some(outcome.clone());
        if outcome == InteractionOutcome::Approved {
            assert!(matches!(
                executor.execute_progress(input.clone(), &journal).await?,
                ExecutionProgress::Ready(TurnOutput { steps: 2, .. })
            ));
            assert_eq!(model.calls.load(Ordering::SeqCst), 2);
            assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
            executor.execute(input, &journal).await?;
            assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
        } else {
            assert!(matches!(
                executor.execute_progress(input.clone(), &journal).await?,
                ExecutionProgress::Rejected(crate::InteractionRejection::Denied)
            ));
            assert_eq!(model.calls.load(Ordering::SeqCst), 1);
            assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
            let reference = stage_json(
                &journal,
                input.operation_id,
                "tool-start",
                &invocation,
                None,
            )
            .await?;
            journal
                .append(input.operation_id, &"tool:0:call-1:started", &ExecutionEvent::ToolStarted {
                        step: 0,
                        call_id: invocation.call_id.clone(),
                        invocation: reference,
                    }, None)?
                .await?;
            assert!(
                matches!(executor.execute_progress(input, &journal).await, Err(Error::Indeterminate(operation)) if operation == invocation.operation_id)
            );
            assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
        }
    }
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one actual retained two-call batch and later-call recovery controls"
)]
async fn whole_tool_batch_prefix_requires_every_ordered_retained_exchange() -> Result<()> {
    let model = Arc::new({
        let mut model = ScriptedModel::default();
        model.responses[0].insert(1, Ok(echo_call("call-2", json!({"value":"later"}))));
        model
    });
    let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            crate::tool::Tool {
                definition: crate::tool::ToolDefinition {
                    name: "example.echo".into(),
                    revision: "1".into(),
                    description: "Echo".into(),
                    input_schema: json!({"type":"object"}),
                    output_schema: json!({"type":"object"}),
                    projection_schema: crate::tool::json_projection_schema(
                        json!({"type":"object"}),
                    ),
                },
                executor: tool.clone(),
                projection: Arc::new(Projection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let executor = uncompacted_executor(
        Model::new("example".into(), "batch".into(), "1".into(), Value::Null)?,
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
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("ordered batch".into()),
        selected_context: None,
        max_steps: 3,
    };
    let journal = Journal::default();
    executor.execute(input.clone(), &journal).await?;
    let batch = executor
        .completed_tool_prefix(&journal, &input, 0, None)
        .await?;
    let first = executor
        .completed_tool_prefix(&journal, &input, 0, Some("call-1"))
        .await?;
    assert_eq!(first.request().messages.len(), 3);
    assert_eq!(batch.request().messages.len(), 5);
    assert_eq!(
        batch.request().messages.get(..3),
        Some(first.request().messages.as_slice())
    );
    let dispatched = model
        .requests
        .lock()
        .unwrap()
        .get(1)
        .cloned()
        .ok_or_else(|| Error::NotFound("batch continuation".into()))?;
    assert_eq!(
        batch.bytes(),
        crate::model::PreparedModelRequest::prepare(dispatched, Limits::default(), None)?.bytes()
    );
    let order: Vec<_> = batch
        .request()
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            ModelContent::Part(ModelContentPart::ToolCall { call_id, .. }) => {
                Some(call_id.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(order, ["call-1", "call-2"]);
    let records = journal.0.lock().unwrap().clone();
    let artifacts = journal.1.lock().unwrap().clone();
    let interactions = journal.2.lock().unwrap().clone();
    let reopened = Journal(
        Mutex::new(records.clone()),
        Mutex::new(artifacts.clone()),
        Mutex::new(interactions.clone()),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    );
    assert_eq!(
        executor
            .completed_tool_prefix(&reopened, &input, 0, None)
            .await?
            .bytes(),
        batch.bytes()
    );
    for uncertain in [false, true] {
        let partial = records
            .iter()
            .take_while(|record| !match &record.event {
                ExecutionEvent::ToolStarted { call_id, .. } if !uncertain => call_id == "call-2",
                ExecutionEvent::ToolCompleted { call_id, .. } if uncertain => call_id == "call-2",
                _ => false,
            })
            .cloned()
            .collect::<Vec<_>>();
        let length = partial.len();
        let pending = Journal(
            Mutex::new(partial),
            Mutex::new(artifacts.clone()),
            Mutex::new(interactions.clone()),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        );
        assert_eq!(
            executor
                .completed_tool_prefix(&pending, &input, 0, Some("call-1"))
                .await?
                .bytes(),
            first.bytes()
        );
        assert!(matches!(
            executor
                .completed_tool_prefix(&pending, &input, 0, None)
                .await,
            Err(Error::Indeterminate(_))
        ));
        assert_eq!(pending.0.lock().unwrap().len(), length);
    }
    let projection = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ToolCompleted {
                call_id,
                projection,
                ..
            } if call_id == "call-2" => Some(projection),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("later projection".into()))?;
    let mut missing = artifacts;
    missing.retain(|_, (file, _)| &*file != projection);
    let unavailable = Journal(
        Mutex::new(records.clone()),
        Mutex::new(missing),
        Mutex::new(interactions),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    );
    assert!(
        executor
            .completed_tool_prefix(&unavailable, &input, 0, None)
            .await
            .is_err()
    );
    assert_eq!(
        executor
            .completed_tool_prefix(&unavailable, &input, 0, Some("call-1"))
            .await?
            .bytes(),
        first.bytes()
    );
    assert_eq!(unavailable.0.lock().unwrap().len(), records.len());
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(tool.0.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn stock_loop_replays_without_reinvoking_models_or_tools() -> Result<()> {
    let model = Arc::new(ScriptedModel::default());
    let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            crate::tool::Tool {
                definition: crate::tool::ToolDefinition {
                    name: "example.echo".into(),
                    revision: "1".into(),
                    description: "Echo".into(),
                    input_schema: json!({"type": "object"}),
                    output_schema: json!({"type": "object"}),
                    projection_schema: crate::tool::json_projection_schema(
                        json!({"type": "object"}),
                    ),
                },
                executor: tool_executor.clone(),
                projection: Arc::new(Projection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let executor = uncompacted_executor(
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
        operation_id: OperationId::from_bytes([1; 16]),
        input: ModelContent::Text("hello".into()),
        selected_context: None,
        max_steps: 4,
    };
    assert!(
        executor
            .completed_tool_prefix(&journal, &input, 0, Some("call-1"))
            .await
            .is_err()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    let first = executor.execute(input.clone(), &journal).await?;
    let completed_prefix = executor
        .completed_tool_prefix(&journal, &input, 0, Some("call-1"))
        .await?;
    let dispatched = model
        .requests
        .lock()
        .unwrap()
        .get(1)
        .cloned()
        .ok_or_else(|| Error::NotFound("second model input".into()))?;
    assert_eq!(
        completed_prefix.bytes(),
        crate::model::PreparedModelRequest::prepare(dispatched, Limits::default(), None)?.bytes()
    );
    assert!(matches!(
        executor
            .completed_tool_prefix(&journal, &input, 0, Some("missing-call"))
            .await,
        Err(Error::Indeterminate(_))
    ));

    let mut replay_context = Vec::new();
    let changed_call = ToolInvocation::for_model_call(
        input.operation_id,
        0,
        "call-1".into(),
        "example.echo".into(),
        json!({"value": "changed"}),
        None,
    )?;
    assert!(matches!(
        StockExecution::ordinary(&executor)
            .resolve_tool_call(
                &journal,
                input.operation_id,
                0,
                changed_call,
                &mut replay_context,
            )
            .await,
        Err(Error::Conflict(_))
    ));
    assert!(replay_context.is_empty());
    let replayed = executor.execute(input.clone(), &journal).await?;
    assert_eq!(first, replayed);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
    let reopened = Journal(
        Mutex::new(journal.0.lock().unwrap().clone()),
        Mutex::new(journal.1.lock().unwrap().clone()),
        Mutex::new(journal.2.lock().unwrap().clone()),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    );
    assert_eq!(executor.execute(input.clone(), &reopened).await?, first);
    let incomplete = Journal(
        Mutex::new(
            reopened
                .0
                .lock()
                .unwrap()
                .iter()
                .take_while(|record| !matches!(record.event, ExecutionEvent::ToolCompleted { .. }))
                .cloned()
                .collect(),
        ),
        Mutex::new(reopened.1.lock().unwrap().clone()),
        Mutex::new(reopened.2.lock().unwrap().clone()),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    );
    assert!(matches!(
        executor
            .completed_tool_prefix(&incomplete, &input, 0, Some("call-1"))
            .await,
        Err(Error::Indeterminate(_))
    ));
    let gapped = Journal(
        Mutex::new(
            incomplete
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|record| record.sequence != 2)
                .cloned()
                .collect(),
        ),
        Mutex::new(incomplete.1.lock().unwrap().clone()),
        Mutex::new(incomplete.2.lock().unwrap().clone()),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    );
    assert!(matches!(
        executor
            .completed_tool_prefix(&gapped, &input, 0, Some("call-1"))
            .await,
        Err(Error::Conflict(_))
    ));
    let request_ref = reopened
        .0
        .lock()
        .unwrap()
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelStarted {
                step: 0, request, ..
            } => Some(request.clone()),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("recorded model start".into()))?;
    let request_key = reopened
        .1
        .lock()
        .unwrap()
        .iter()
        .find_map(|(key, (reference, _))| (reference == &request_ref).then(|| key.clone()))
        .ok_or_else(|| Error::NotFound("recorded request".into()))?;
    reopened
        .1
        .lock()
        .unwrap()
        .get_mut(&request_key)
        .ok_or_else(|| Error::NotFound("recorded request".into()))?
        .1 = acyclic_stream::OwnedBytes::from_static(b"{}");
    assert!(executor.execute(input.clone(), &reopened).await.is_err());
    reopened.1.lock().unwrap().remove(&request_key);
    assert!(matches!(
        executor.execute(input, &reopened).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
    let durable = serde_json::to_string(&*journal.0.lock().unwrap())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(
        !durable.contains("hello") && !durable.contains("done") && !durable.contains("partial-")
    );
    assert!(!journal.1.lock().unwrap().is_empty());
    let requests = model.requests.lock().unwrap();
    assert_eq!(
        requests.get(1).map(|request| request
            .messages
            .iter()
            .map(|message| message.role)
            .collect::<Vec<_>>()),
        Some(vec![ModelRole::User, ModelRole::Assistant, ModelRole::Tool])
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn stock_turn_emits_spans_without_prompt_or_tool_fields() -> Result<()> {
    use crate::obs::capture;
    let (seen, _guard) = capture::install();
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            crate::tool::Tool {
                definition: crate::tool::ToolDefinition {
                    name: "example.echo".into(),
                    revision: "1".into(),
                    description: "Echo".into(),
                    input_schema: json!({"type": "object"}),
                    output_schema: json!({"type": "object"}),
                    projection_schema: crate::tool::json_projection_schema(
                        json!({"type": "object"}),
                    ),
                },
                executor: Arc::new(FakeTool(AtomicUsize::new(0))),
                projection: Arc::new(Projection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let executor = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
        Arc::new(ScriptedModel::default()),
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
        operation_id: OperationId::from_bytes([1; 16]),
        input: ModelContent::Text("hello".into()),
        selected_context: None,
        max_steps: 4,
    };
    executor.execute(input.clone(), &journal).await?;
    executor.execute(input, &journal).await?;

    let has = |span, field, value| capture::has(&seen, span, field, value);
    assert!(has("acyclic.harness.executor.execute", "outcome", "ok"));
    let step = "acyclic.harness.executor.model_step";
    assert!(has(step, "model", "model"));
    assert!(has(step, "step", "1"));
    assert!(has(step, "phase", "dispatch"));
    assert!(has(step, "phase", "replay"));
    assert!(has(step, "items", "2"));
    assert!(has(step, "outcome", "ok"));
    assert!(has("acyclic.harness.executor.tool_call", "step", "0"));
    assert!(has("acyclic.harness.executor.tool_call", "outcome", "ok"));
    capture::assert_clean(&seen, &["hello", "done", "example.echo"]);
    Ok(())
}

#[tokio::test]
async fn concurrent_stock_hosts_do_not_redispatch_model_or_tool() -> Result<()> {
    let model = Arc::new(ScriptedModel::default());
    let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            crate::tool::Tool {
                definition: crate::tool::ToolDefinition {
                    name: "example.echo".into(),
                    revision: "1".into(),
                    description: "Echo".into(),
                    input_schema: json!({"type":"object"}),
                    output_schema: json!({"type":"object"}),
                    projection_schema: crate::tool::json_projection_schema(
                        json!({"type":"object"}),
                    ),
                },
                executor: tool_executor.clone(),
                projection: Arc::new(Projection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let executor = uncompacted_executor(
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
        operation_id: OperationId::from_bytes([77; 16]),
        input: ModelContent::Text("hello".into()),
        selected_context: None,
        max_steps: 4,
    };
    let (left, right) = tokio::join!(
        executor.execute(input.clone(), &journal),
        executor.execute(input.clone(), &journal)
    );
    assert!(
        left.is_ok()
            || right.is_ok()
            || matches!(left, Err(Error::Indeterminate(_)))
                && matches!(right, Err(Error::Indeterminate(_)))
    );
    let _ = executor.execute(input, &journal).await?;
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn malformed_tool_arguments_fail_closed_before_dispatch() -> Result<()> {
    let model = Arc::new(ScriptedModel::slipping());
    let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            crate::tool::Tool {
                definition: crate::tool::ToolDefinition {
                    name: "example.echo".into(),
                    revision: "1".into(),
                    description: "Echo".into(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {"value": {"type": "string"}},
                        "additionalProperties": false,
                    }),
                    output_schema: json!({"type": "object"}),
                    projection_schema: crate::tool::json_projection_schema(
                        json!({"type": "object"}),
                    ),
                },
                executor: tool_executor.clone(),
                projection: Arc::new(Projection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let executor = uncompacted_executor(
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
        operation_id: OperationId::from_bytes([79; 16]),
        input: ModelContent::Text("hello".into()),
        selected_context: None,
        max_steps: 4,
    };

    assert!(matches!(
        executor.execute(input, &journal).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn an_ungranted_tool_is_refused_before_its_schema_can_be_probed() -> Result<()> {
    let model = Arc::new(ScriptedModel::slipping());
    let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            crate::tool::Tool {
                definition: crate::tool::ToolDefinition {
                    name: "example.echo".into(),
                    revision: "1".into(),
                    description: "Echo".into(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {"value": {"type": "string"}},
                        "additionalProperties": false,
                    }),
                    output_schema: json!({"type": "object"}),
                    projection_schema: crate::tool::json_projection_schema(
                        json!({"type": "object"}),
                    ),
                },
                executor: tool_executor.clone(),
                projection: Arc::new(Projection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let executor = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
        model,
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["tool:call:example.other"]),
            Limits::default(),
        )?,
        None,
    )?;
    let journal = Journal::default();
    let input = TurnInput {
        operation_id: OperationId::from_bytes([80; 16]),
        input: ModelContent::Text("hello".into()),
        selected_context: None,
        max_steps: 4,
    };
    let outcome = executor.execute(input, &journal).await;
    assert!(
        matches!(&outcome, Err(Error::Unauthorized(message)) if message.contains("example.echo")),
        "expected an unauthorized refusal, got {outcome:?}"
    );
    let rendered = format!("{outcome:?}");
    assert!(
        !rendered.contains("additionalProperties") && !rendered.contains("failed validation"),
        "schema detail leaked through the refusal: {rendered}"
    );
    assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn completed_tool_prefix_validates_retained_result_and_projection_independently() -> Result<()>
{
    let definition = crate::tool::ToolDefinition {
        name: "example.echo".into(),
        revision: "1".into(),
        description: "Structured result, text projection".into(),
        input_schema: json!({"type": "object"}),
        output_schema: json!({"type": "object"}),
        projection_schema: crate::tool::json_projection_schema(json!({"type": "string"})),
    };
    for (canonical, projected, accepted) in [
        (
            json!({"count": 2}),
            json!({"kind":"json","value":"two"}),
            true,
        ),
        (
            json!("bad result"),
            json!({"kind":"json","value":"two"}),
            false,
        ),
        (
            json!({"count": 2}),
            json!({"kind":"json","value":{"bad":"projection"}}),
            false,
        ),
    ] {
        let journal = Journal::default();
        let operation = OperationId::new();
        let invocation = ToolInvocation::for_model_call(
            operation,
            0,
            "call".into(),
            definition.name.clone(),
            json!({}),
            None,
        )?;
        let invocation_ref =
            stage_json(&journal, operation, "invocation", &invocation, None).await?;
        let result = stage_json(
            &journal,
            operation,
            "result",
            &ToolResult { value: canonical },
            None,
        )
        .await?;
        let projection = stage_json(&journal, operation, "projection", &projected, None).await?;
        journal
            .append(operation, &"started", &ExecutionEvent::ToolStarted {
                    step: 0,
                    call_id: invocation.call_id.clone(),
                    invocation: invocation_ref,
                }, None)?
            .await?;
        journal
            .append(operation, &"completed", &ExecutionEvent::ToolCompleted {
                    step: 0,
                    call_id: invocation.call_id.clone(),
                    result,
                    projection,
                }, None)?
            .await?;
        let records = journal.replay(operation, 0, 64, None)?.await?;
        let retained = completed_tool_projection(
            &journal,
            &records,
            0,
            &invocation,
            std::slice::from_ref(&definition),
            Limits::default(),
            None,
        )
        .await;
        if accepted {
            assert_eq!(
                serde_json::to_value(retained?)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                projected
            );
        } else {
            assert!(retained.is_err());
        }
    }
    Ok(())
}

#[tokio::test]
async fn stock_tool_validation_failure_replays_as_terminal_without_redispatch() -> Result<()> {
    for (output_schema, projection_schema, expected) in [
        (
            json!({"type":"string"}),
            json!({"type":"object"}),
            ToolFailureKind::InvalidOutput,
        ),
        (
            json!({"type":"object"}),
            json!({"type":"string"}),
            ToolFailureKind::ProjectionRejected,
        ),
    ] {
        let model = Arc::new(ScriptedModel::default());
        let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
        let mut tools = ToolRegistry::default();
        tools.register(
            (
                crate::tool::Tool {
                    definition: crate::tool::ToolDefinition {
                        name: "example.echo".into(),
                        revision: "1".into(),
                        description: "Echo".into(),
                        input_schema: json!({"type":"object"}),
                        output_schema,
                        projection_schema: crate::tool::json_projection_schema(projection_schema),
                    },
                    executor: tool_executor.clone(),
                    projection: Arc::new(Projection),
                },
                None,
            ),
            &mut None,
            None,
        )?;
        let executor = uncompacted_executor(
            Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
            model,
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
            operation_id: OperationId::from_bytes([78; 16]),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 4,
        };
        assert!(matches!(executor.execute(input.clone(), &journal).await,
            Err(Error::Invalid(message)) if message == expected.message()));
        assert!(matches!(executor.execute(input, &journal).await,
            Err(Error::Invalid(message)) if message == expected.message()));
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
