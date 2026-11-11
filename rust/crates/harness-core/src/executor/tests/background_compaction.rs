use super::*;
use tokio::sync::Notify;

struct ReleaseSummary(Arc<Notify>);

struct GrowingResults(Arc<Notify>);

impl ToolExecutor for GrowingResults {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let bytes = match invocation.call_id.as_str() {
                "small" => {
                    self.0.notify_one();
                    2_000
                }
                "large" => 58_000,
                _ => return Err(Error::Invalid("unexpected fixture call".into())),
            };
            Ok(ToolResult {
                value: json!({"value":"r".repeat(bytes)}),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

impl ToolExecutor for ReleaseSummary {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            // The worker reached tool execution while the context call was
            // blocked. Serial compaction cannot reach this event.
            self.0.notify_one();
            Ok(ToolResult {
                value: invocation.arguments,
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

fn source() -> crate::context::Context {
    crate::context::Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("keep".into()),
            },
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("o".repeat(28_000)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("a".repeat(28_000)),
            },
        ],
        ..Default::default()
    }
}

fn executor(provider: Arc<ScriptedModelProvider>, tools: ToolRegistry) -> Result<StockExecutor> {
    StockExecutor::new(
        Model::new("test".into(), "background-context".into(), "1".into())?,
        provider,
        ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
            "history",
            "1",
            Arc::new(source()),
            crate::context::ContextPlacement::Prepend,
        )) as Arc<dyn crate::context::ContextStage>]),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["model:generate", "tool:call:example.echo"]),
            Limits::default(),
        )?,
        allowed()?,
    )
}

fn input() -> TurnInput {
    TurnInput {
        operation_id: OperationId::new(),
        max_steps: None,
        input: ModelContent::Text("c".repeat(2_000)),
        selected_context: None,
    }
}

#[tokio::test]
async fn background_compaction_overlaps_tools_and_adopts_only_at_capacity() -> Result<()> {
    background_case(false).await
}

#[tokio::test]
async fn failed_background_compaction_keeps_original_history_even_after_replay() -> Result<()> {
    background_case(true).await
}

async fn background_case(fail: bool) -> Result<()> {
    let input = input();
    let operation = input.operation_id;
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let tools = echo_tools(
        Arc::new(ReleaseSummary(release.clone())),
        json!(true),
        json!(true),
        json!(true),
    )?;
    let (provider, _) = counted_model(
        65_536,
        None,
        [
            tool_turn([echo_call("call", json!({"value": "r".repeat(4_000)}))]),
            if fail {
                ScriptedTurn::Events(vec![ScriptedEvent::Error {
                    error: "context model failed".into(),
                }])
            } else {
                ScriptedTurn::Text("background-summary".into())
            },
            ScriptedTurn::Text("answer".into()),
        ],
        [],
    );
    let provider = Arc::new(
        Arc::into_inner(provider)
            .expect("unique provider")
            .with_dispatch_observer(move |dispatch| {
                let started = started.clone();
                let release = release.clone();
                Box::pin(async move {
                    if dispatch.operation_id == operation && dispatch.step == 0 {
                        started.notified().await;
                    } else if dispatch.operation_id != operation {
                        started.notify_one();
                        release.notified().await;
                    }
                    Ok(())
                })
            }),
    );
    let executor = executor(provider.clone(), tools)?;
    let journal = Journal::default();
    let executing = executor.execute(input.clone(), &journal);
    tokio::pin!(executing);
    assert!(futures::poll!(&mut executing).is_pending());
    assert_eq!(
        provider.request_count(),
        2,
        "the worker reached its gate without admitting the background call"
    );
    let result = executing.await;
    if fail {
        assert!(result.is_err());
        assert!(executor.execute(input, &journal).await.is_err());
        assert_eq!(
            provider.requests().len(),
            2,
            "a failed context call was redispatched"
        );
        assert!(
            !journal
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
        );
        return Ok(());
    }
    let result = result?;
    assert_eq!(result.text, "answer");
    assert_eq!(result.steps, 2, "context work consumed a worker step");
    assert_eq!(executor.execute(input.clone(), &journal).await?, result);
    let requests = provider.requests();
    assert_eq!(requests.len(), 3, "replay dispatched another context call");
    assert!(!requests[0].summary);
    assert!(requests[1].summary);
    assert!(!requests[2].summary);
    assert_eq!(&requests[0].messages[..3], source().messages);
    assert_eq!(&requests[1].messages[..3], &requests[0].messages[..3]);
    let ModelContent::Part(ModelContentPart::File { file, .. }) = &requests[2].messages[0].content
    else {
        panic!("capacity rewrite has no durable summary");
    };
    assert_eq!(
        journal.load(file, None)?.await?.as_ref(),
        b"background-summary"
    );
    let records = journal.0.lock().unwrap();
    let windows: Vec<_> = records
        .iter()
        .filter_map(|record| match &record.event {
            ExecutionEvent::ModelStarted {
                purpose: ModelPurpose::Response,
                context,
                ..
            } => *context,
            _ => None,
        })
        .collect();
    assert_eq!(windows.len(), 2);
    assert!(windows[0].input_tokens > 57_344);
    assert!(windows[1].input_tokens < 20_000);
    assert!(records.iter().any(|record| matches!(
        record.event,
        ExecutionEvent::ContextCompacted { step: 1, .. }
    )));
    Ok(())
}

#[tokio::test]
async fn unused_background_call_is_stopped_without_waiting_for_the_model() -> Result<()> {
    let input = input();
    let operation = input.operation_id;
    let started = Arc::new(Notify::new());
    let (provider, _) = counted_model(
        65_536,
        None,
        [
            ScriptedTurn::Text("answer".into()),
            ScriptedTurn::Events(vec![ScriptedEvent::Pending {}]),
        ],
        [],
    );
    let provider = Arc::new(
        Arc::into_inner(provider)
            .expect("unique provider")
            .with_dispatch_observer(move |dispatch| {
                let started = started.clone();
                Box::pin(async move {
                    if dispatch.operation_id == operation {
                        started.notified().await;
                    } else {
                        started.notify_one();
                    }
                    Ok(())
                })
            }),
    );
    let journal = Journal::default();
    let executor = executor(provider.clone(), ToolRegistry::default())?;
    let executing = executor.execute(input, &journal);
    tokio::pin!(executing);
    assert!(futures::poll!(&mut executing).is_pending());
    assert_eq!(
        provider.request_count(),
        2,
        "the context producer never started"
    );
    let result = executing.await?;
    assert_eq!(result.text, "answer");
    assert_eq!(provider.requests().len(), 2);
    assert!(journal.0.lock().unwrap().iter().any(|record| matches!(
        record.event,
        ExecutionEvent::ModelStopped {
            step: 0,
            purpose: ModelPurpose::Summary,
            worker_paused: false
        }
    )));
    Ok(())
}

#[tokio::test]
async fn approval_pause_retires_speculation_without_failing_capacity_on_resume() -> Result<()> {
    let input = input();
    let operation = input.operation_id;
    let started = Arc::new(Notify::new());
    let (provider, _) = counted_model(
        65_536,
        None,
        [
            tool_turn([echo_call("approved", json!({"value":"r".repeat(4_000)}))]),
            ScriptedTurn::Events(vec![ScriptedEvent::Pending {}]),
            ScriptedTurn::Text("answer".into()),
        ],
        [],
    );
    let provider = Arc::new(
        Arc::into_inner(provider)
            .expect("unique provider")
            .with_dispatch_observer(move |dispatch| {
                let started = started.clone();
                Box::pin(async move {
                    if dispatch.operation_id == operation && dispatch.step == 0 {
                        started.notified().await;
                    } else if dispatch.operation_id != operation {
                        started.notify_one();
                    }
                    Ok(())
                })
            }),
    );
    let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
    let tools = echo_tools(tool.clone(), json!(true), json!(true), json!(true))?;
    let executor = executor(provider.clone(), tools)?.with_tool_authority(
        RuntimeScope::new(
            Capabilities::new([
                "model:generate",
                "tool:call:example.echo",
                capability::INTERACTION_ROUTE,
            ]),
            Limits::default(),
        )?,
        Arc::new(crate::test_support::FixedToolPolicy::new(
            ToolPolicyDecision::RequireApproval {
                prompt: "Approve echo".into(),
            },
        )?),
    )?;
    let journal = Journal::default();
    let pending = executor.execute_progress(input.clone(), &journal);
    tokio::pin!(pending);
    assert!(futures::poll!(&mut pending).is_pending());
    assert_eq!(provider.request_count(), 2);
    assert!(matches!(pending.await?, ExecutionProgress::Pending(_)));
    assert!(journal.0.lock().unwrap().iter().any(|record| matches!(
        record.event,
        ExecutionEvent::ModelStopped {
            purpose: ModelPurpose::Summary,
            worker_paused: true,
            ..
        }
    )));
    assert!(matches!(
        executor.execute_progress(input.clone(), &journal).await?,
        ExecutionProgress::Pending(_)
    ));
    assert_eq!(
        provider.request_count(),
        2,
        "a paused summary was redispatched"
    );
    {
        let mut interactions = journal.2.lock().unwrap();
        assert_eq!(interactions.len(), 1);
        interactions
            .values_mut()
            .next()
            .ok_or_else(|| Error::NotFound("approval".into()))?
            .1 = Some(InteractionOutcome::Approved {});
    }
    let output = executor.execute(input.clone(), &journal).await?;
    assert_eq!(output.text, "answer");
    assert_eq!(output.steps, 2);
    assert_eq!(tool.0.load(Ordering::SeqCst), 1);
    assert_eq!(provider.request_count(), 3);
    assert_eq!(executor.execute(input, &journal).await?, output);
    assert_eq!(provider.request_count(), 3);
    Ok(())
}

#[tokio::test]
async fn stale_summary_is_replaced_before_a_single_capacity_rewrite_is_committed() -> Result<()> {
    let input = input();
    let operation = input.operation_id;
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (provider, _) = counted_model(
        65_536,
        None,
        [
            tool_turn([echo_call("small", json!({}))]),
            ScriptedTurn::Text("background-summary".into()),
            tool_turn([echo_call("large", json!({}))]),
            ScriptedTurn::Text("answer".into()),
        ],
        [],
    );
    let summary_release = release.clone();
    let provider = Arc::new(
        Arc::into_inner(provider)
            .expect("unique provider")
            .with_dispatch_observer(move |dispatch| {
                let started = started.clone();
                let release = summary_release.clone();
                Box::pin(async move {
                    if dispatch.operation_id == operation && dispatch.step == 0 {
                        started.notified().await;
                    } else if dispatch.operation_id != operation {
                        started.notify_one();
                        release.notified().await;
                    }
                    Ok(())
                })
            }),
    );
    let tools = echo_tools(
        Arc::new(GrowingResults(release)),
        json!(true),
        json!(true),
        json!(true),
    )?;
    let executor = executor(provider.clone(), tools)?;
    let journal = Journal::default();
    let executing = executor.execute(input.clone(), &journal);
    tokio::pin!(executing);
    assert!(futures::poll!(&mut executing).is_pending());
    assert_eq!(provider.request_count(), 2);
    let output = executing.await?;
    assert_eq!(output.text, "answer");
    assert_eq!(output.steps, 3);
    assert_eq!(provider.request_count(), 4);
    let request = provider
        .request(3)
        .ok_or_else(|| Error::NotFound("final request".into()))?;
    let ModelContent::Part(ModelContentPart::File { file, .. }) = &request.messages[0].content
    else {
        panic!("capacity rewrite has no summary");
    };
    assert_eq!(
        journal.load(file, None)?.await?.as_ref(),
        super::super::compaction::BACKSTOP_MARKER.as_bytes()
    );
    assert!(
        !request
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .any(
                |part| matches!(part, ModelContentPart::ToolCall(call) if call.call_id == "small")
            )
    );
    assert!(
        request
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .any(
                |part| matches!(part, ModelContentPart::ToolCall(call) if call.call_id == "large")
            )
    );
    assert_eq!(
        journal
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
            .count(),
        1,
        "a rejected preview was published"
    );
    assert_eq!(executor.execute(input, &journal).await?, output);
    assert_eq!(provider.request_count(), 4);
    Ok(())
}

#[tokio::test]
async fn last_admitted_worker_step_does_not_dispatch_speculation() -> Result<()> {
    no_speculation(false).await?;
    no_speculation(true).await
}

async fn no_speculation(startup_only: bool) -> Result<()> {
    let mut input = input();
    input.max_steps = (!startup_only).then_some(1);
    let release = Arc::new(Notify::new());
    let observed_release = release.clone();
    let (provider, _) = counted_model(65_536, None, [ScriptedTurn::Text("answer".into())], []);
    let provider = Arc::new(
        Arc::into_inner(provider)
            .expect("unique provider")
            .with_dispatch_observer(move |_| {
                let release = observed_release.clone();
                Box::pin(async move {
                    release.notified().await;
                    Ok(())
                })
            }),
    );
    let executor = if startup_only {
        let mut startup = source();
        startup.messages[2].role = ModelRole::User;
        Ok(StockExecutor::new(
            Model::new("test".into(), "background-context".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
                "startup",
                "1",
                Arc::new(startup),
                crate::context::ContextPlacement::Prepend,
            )) as Arc<dyn crate::context::ContextStage>]),
            ToolRegistry::default(),
        ))
    } else {
        executor(provider.clone(), ToolRegistry::default())
    }?;
    let journal = Journal::default();
    let executing = executor.execute(input, &journal);
    tokio::pin!(executing);
    assert!(futures::poll!(&mut executing).is_pending());
    assert_eq!(
        provider.request_count(),
        1,
        "a call with no future adoption point started"
    );
    release.notify_one();
    assert_eq!(executing.await?.text, "answer");
    assert_eq!(provider.request_count(), 1);
    Ok(())
}
