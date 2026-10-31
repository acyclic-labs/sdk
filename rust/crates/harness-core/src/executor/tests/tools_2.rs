use super::*;

#[tokio::test]
async fn stock_tool_input_obeys_narrow_executor_bound_before_journal_io() -> Result<()> {
    let wide = RuntimeScope::new(
        Capabilities::new(["tool:call:example.echo"]),
        Limits::default(),
    )?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "bounded".into(),
        name: "example.echo".into(),
        arguments: json!({"text":"x".repeat(256)}),
    };
    let adapter = Arc::new(FakeTool(AtomicUsize::new(0)));
    crate::tool::ToolExecutor::authorize(adapter.as_ref(), Some(&wide), None, &invocation, None)?;
    crate::executor::json::json_byte_length(&invocation, wide.limits().file_bytes, &mut None)
        .map(|_| ())?;
    let tools = echo_tools(
        adapter.clone(),
        json!({"type":"object"}),
        json!({"type":"object"}),
        crate::tool::json_projection_schema(json!({"type":"object"})),
    )?;
    let narrow = Limits {
        file_bytes: 128,
        ..wide.limits()
    };
    let executor = StockExecutor::new(
        Model::new("test".into(), "scoped".into(), "1".into(), Value::Null)?,
        Arc::new(scripted_model()),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(wide, allowed()?)?
    .with_limits(narrow);
    assert!(executor.tool_scope.limits().file_bytes > executor.limits.file_bytes);
    let journal = Journal::default();
    let mut messages = vec![call_message(&invocation)];
    let before = messages.clone();
    assert!(matches!(
        StockExecution::ordinary(&executor)
            .settle_tool_call(
                &journal,
                OperationId::new(),
                0,
                invocation,
                0,
                &mut messages,
                &mut Vec::new(),
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(adapter.0.load(Ordering::SeqCst), 0);
    assert_eq!(journal.3.load(Ordering::SeqCst), 0);
    assert_eq!(journal.4.load(Ordering::SeqCst), 0);
    assert!(journal.0.lock().unwrap().is_empty());
    assert!(journal.1.lock().unwrap().is_empty());
    assert_eq!(messages, before);
    Ok(())
}

#[tokio::test]
async fn policy_denial_returns_to_the_model_before_dispatch() -> Result<()> {
    struct FixedPolicy(bool);
    impl ToolPolicy for FixedPolicy {
        fn identity(&self) -> ComponentIdentity {
            ComponentIdentity {
                name: "test.deny".into(),
                version: "1".into(),
                digest: [1 + u8::from(self.0); 32],
            }
        }
        fn evaluate<'a>(
            &'a self,
            _: &'a ToolInvocation,
            _: &'a RuntimeScope,
            _io: Option<&'a crate::executor::PreparationIo>,
        ) -> Result<
            crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>,
        > {
            let decision = if self.0 {
                ToolPolicyDecision::Deny {
                    reason: "echo is\nforbidden here".into(),
                }
            } else {
                ToolPolicyDecision::Allow
            };
            Ok(crate::store::HistoryContentFuture::Ordinary {
                future: Box::pin(async move { Ok((decision, None)) }),
                frame_bytes: 0,
            })
        }
    }
    // The allowing policy is the negative control: the same call dispatches and no error returns.
    for deny in [true, false] {
        let model = Arc::new(scripted_model());
        let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
        let tools = echo_tools(
            tool_executor.clone(),
            json!({"type": "object"}),
            json!({"type": "object"}),
            crate::tool::json_projection_schema(json!({"type": "object"})),
        )?;
        let executor = super::uncompacted_executor(
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
            Arc::new(FixedPolicy(deny)),
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([29; 16]),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 4,
        };
        let output = executor.execute(input.clone(), &journal).await?;
        assert_eq!(output.text, "done");
        assert_eq!(model.requests().len(), 2);
        let text = super::tools_1::tool_error_text(&model.requests()[1].messages, "call-1");
        assert_eq!(text.is_some(), deny);
        if let Some(text) = text {
            assert_eq!(
                text,
                "tool execution failed: unauthorized: echo is forbidden here"
            );
        }
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), usize::from(!deny));
        assert_eq!(executor.execute(input, &journal).await?, output);
        assert_eq!(model.requests().len(), 2);
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), usize::from(!deny));
        // A later denial cannot settle an effect whose start was already recorded.
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "started".into(),
            name: "example.echo".into(),
            arguments: json!({}),
        };
        let tool = executor.tools.get(&invocation.name, None, None)?.unwrap();
        let started = StockExecution::ordinary(&executor)
            .require_tool_policy(&journal, invocation.operation_id, &invocation, tool, true)
            .await;
        if deny {
            assert!(
                matches!(started, Err(Error::Indeterminate(id)) if id == invocation.operation_id)
            );
        } else {
            assert!(matches!(started, Ok(None)));
        }
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), usize::from(!deny));
    }
    Ok(())
}

// A tool's own refusal (authorization, a missing file, invalid input) and its execution failure
// are the model's to act on: each settles as `tool execution failed: <reason>` and the run
// continues. Only a harness fault, here a storage failure, ends the turn.
#[tokio::test]
async fn tool_refusals_and_failures_settle_and_only_harness_faults_end_the_turn() -> Result<()> {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Stage {
        Authorize,
        Execute,
    }
    struct Failing(Stage, fn() -> Error, AtomicUsize);
    impl crate::tool::ToolExecutor for Failing {
        fn authorize(
            &self,
            _: Option<&RuntimeScope>,
            _: Option<&crate::runtime::ToolContext>,
            _: &ToolInvocation,
            _: crate::NativeWork<'_>,
        ) -> Result<()> {
            if self.0 == Stage::Authorize {
                return Err((self.1)());
            }
            Ok(())
        }
        fn execute<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<crate::runtime::ToolContext>,
        ) -> BoxFuture<'a, Result<ToolResult>> {
            self.2.fetch_add(1, Ordering::SeqCst);
            let error = (self.1)();
            Box::pin(async move { Err(error) })
        }
        fn reconcile<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<crate::runtime::ToolContext>,
        ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            Box::pin(async { Ok(None) })
        }
    }
    let unauthorized: fn() -> Error =
        || Error::Unauthorized("file tool requires original source read grant".into());
    let missing: fn() -> Error = || Error::NotFound("src/lib.rs".into());
    let invalid: fn() -> Error = || Error::Invalid("tool arguments are malformed".into());
    let unavailable: fn() -> Error = || Error::ModelDiagnostic {
        code: crate::wire::ErrorCode::Unavailable,
        message: "tool reply producer was lost".into(),
        custody: None,
    };
    let storage: fn() -> Error = || Error::Storage("journal unavailable".into());
    let refused: fn() -> Error = || Error::ToolRejected {
        code: crate::wire::ErrorCode::Unauthorized,
        reason: "original read grant is absent",
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let native_boundary: fn() -> Error = || Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "native producer invariant failed",
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let native_accounting: fn() -> Error = || {
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(16_384, 16 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )
        .unwrap();
        work.claim(u64::MAX, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .unwrap_err()
            .into()
    };
    for stage in [Stage::Authorize, Stage::Execute] {
        for (error, expected) in [
            (
                unauthorized,
                Some(
                    "tool execution failed: unauthorized: file tool requires original source read grant",
                ),
            ),
            (
                missing,
                Some("tool execution failed: resource not found: src/lib.rs"),
            ),
            (
                invalid,
                Some("tool execution failed: invalid request: tool arguments are malformed"),
            ),
            (
                refused,
                Some("tool execution failed: tool rejected: original read grant is absent"),
            ),
            // Negative control: a harness fault never settles as a model-visible result.
            (storage, None),
            (unavailable, None),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            (native_boundary, None),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            (native_accounting, None),
        ] {
            let model = Arc::new(scripted_model());
            let tool = Arc::new(Failing(stage, error, AtomicUsize::new(0)));
            let tools = echo_tools(
                tool.clone(),
                json!({"type": "object"}),
                json!({"type": "object"}),
                crate::tool::json_projection_schema(json!({"type": "object"})),
            )?;
            let executor = super::uncompacted_executor(
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
                super::allowed()?,
            )?;
            let journal = Journal::default();
            let input = TurnInput {
                operation_id: OperationId::from_bytes([41; 16]),
                input: ModelContent::Text("hello".into()),
                selected_context: None,
                max_steps: 4,
            };
            let output = executor.execute(input, &journal).await;
            let dispatched = usize::from(stage == Stage::Execute);
            match expected {
                Some(expected) => {
                    assert_eq!(output?.text, "done", "{stage:?}");
                    assert_eq!(model.requests().len(), 2);
                    assert_eq!(
                        super::tools_1::tool_error_text(&model.requests()[1].messages, "call-1")
                            .as_deref(),
                        Some(expected),
                        "{stage:?}"
                    );
                }
                None => {
                    assert!(output.is_err(), "{stage:?}");
                    assert_eq!(model.requests().len(), 1, "{stage:?}");
                }
            }
            assert_eq!(tool.2.load(Ordering::SeqCst), dispatched, "{stage:?}");
        }
    }
    Ok(())
}

// Review P1: an interruption after the next step's `ContextPrepared` left a projection whose
// refusal lost its in-memory provenance on reload, so resume rejected its own history. The
// refusal is now a recorded settlement and its result is admitted by shape on every load.
#[tokio::test]
async fn refused_calls_survive_an_interruption_after_the_next_context_is_prepared() -> Result<()> {
    let refusing = || {
        ScriptedModelProvider::new([
            tool_turn([
                ModelEvent::ToolCall(crate::model::ToolCall::new(
                    "ghost-call".into(),
                    "example.ghost".into(),
                    json!({}),
                )),
                echo_call("bad-call", json!(7)),
            ]),
            done_turn(),
        ])
        .with_token_counter(fixture_token_count)
    };
    let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
    let executor = |model: Arc<ScriptedModelProvider>| -> Result<StockExecutor> {
        uncompacted_executor(
            Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
            model,
            ContextPipeline::default(),
            echo_tools(
                tool_executor.clone(),
                json!({"type":"object"}),
                json!({"type":"object"}),
                crate::tool::json_projection_schema(json!({"type":"object"})),
            )?,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits::default(),
            )?,
            allowed()?,
        )
    };
    let input = TurnInput {
        operation_id: OperationId::from_bytes([83; 16]),
        input: ModelContent::Text("hello".into()),
        selected_context: None,
        max_steps: 4,
    };
    let journal = Journal::default();
    let first = Arc::new(refusing());
    let output = executor(first.clone())?
        .execute(input.clone(), &journal)
        .await?;
    assert_eq!(output.text, "done");
    // Interrupt after step 1's context commit, before its model dispatch.
    {
        let mut records = journal.0.lock().unwrap();
        let prepared = records
            .iter()
            .position(|record| {
                matches!(
                    record.event,
                    ExecutionEvent::ContextPrepared { step: 1, .. }
                )
            })
            .expect("step 1 context");
        records.truncate(prepared + 1);
    }
    let resumed =
        Arc::new(ScriptedModelProvider::new([done_turn()]).with_token_counter(fixture_token_count));
    let recovered = executor(resumed.clone())?.execute(input, &journal).await?;
    assert_eq!(recovered.text, "done");
    let requests = resumed.requests();
    assert_eq!(requests.len(), 1);
    // The resumed request carries both refusals exactly as the first run delivered them.
    for call in ["ghost-call", "bad-call"] {
        assert_eq!(
            super::tools_1::tool_error_text(&requests[0].messages, call),
            super::tools_1::tool_error_text(&first.requests()[1].messages, call),
            "{call}"
        );
        assert!(
            super::tools_1::tool_error_text(&requests[0].messages, call).is_some(),
            "{call}"
        );
    }
    // Both refusals are settlements recorded without a start, and nothing dispatched.
    let refusals = journal
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|record| matches!(record.event, ExecutionEvent::ToolFailed { .. }))
        .count();
    assert_eq!(refusals, 2);
    assert!(
        !journal
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ToolStarted { .. }))
    );
    assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
    // Negative control: the first run sent step 1 once, and the resumed run never replayed
    // step 0.
    assert_eq!(first.requests().len(), 2);
    assert_eq!(tool_executor.0.load(Ordering::SeqCst), 0);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn semantic_tool_preflight_replays_a_failure_but_native_infrastructure_stays_strict()
-> Result<()> {
    struct Rejected(bool);
    impl ToolExecutor for Rejected {
        fn authorize(
            &self,
            _: Option<&RuntimeScope>,
            _: Option<&crate::runtime::ToolContext>,
            _: &ToolInvocation,
            _: crate::NativeWork<'_>,
        ) -> Result<()> {
            Err(if self.0 {
                Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "invalid selection",
                }
            } else {
                Error::tool_rejected("invalid selection")
            })
        }
        fn execute<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<crate::runtime::ToolContext>,
        ) -> BoxFuture<'a, Result<ToolResult>> {
            panic!("a rejected call must never dispatch")
        }
        fn reconcile<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<crate::runtime::ToolContext>,
        ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            panic!("a rejected call must never reconcile")
        }
    }
    for broken in [false, true] {
        let tools = echo_tools(
            Arc::new(Rejected(broken)),
            json!({"type":"object"}),
            json!({}),
            crate::tool::json_projection_schema(json!({})),
        )?;
        let executor = uncompacted_executor(
            Model::new("fixture".into(), "exact".into(), "1".into(), Value::Null)?,
            Arc::new(scripted_model()),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits::default(),
            )?,
            allowed()?,
        )?;
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "selection".into(),
            name: "example.echo".into(),
            arguments: json!({}),
        };
        let operation = OperationId::new();
        let journal = Journal::default();
        let mut messages = vec![call_message(&invocation)];
        let result = StockExecution::ordinary(&executor)
            .settle_tool_call(
                &journal,
                operation,
                0,
                invocation.clone(),
                0,
                &mut messages,
                &mut Vec::new(),
            )
            .await;
        if broken {
            // Negative control: the former boundary classification aborts before a record.
            assert!(matches!(result, Err(Error::NativeBoundary { .. })));
            assert!(journal.0.lock().unwrap().is_empty());
        } else {
            assert!(matches!(result?, ExecutionProgress::Ready(())));
            let mut replayed = vec![call_message(&invocation)];
            StockExecution::ordinary(&executor)
                .settle_tool_call(
                    &journal,
                    operation,
                    0,
                    invocation,
                    0,
                    &mut replayed,
                    &mut Vec::new(),
                )
                .await?;
            assert_eq!(messages, replayed);
            let records = journal.0.lock().unwrap();
            assert_eq!(records.len(), 1);
            assert!(matches!(
                records[0].event,
                ExecutionEvent::ToolFailed {
                    reason: ToolFailureKind::InvalidInput,
                    ..
                }
            ));
        }
    }
    Ok(())
}
