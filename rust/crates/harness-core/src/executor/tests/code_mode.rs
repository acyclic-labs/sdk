use super::*;

struct InnerApproval;
impl ToolPolicy for InnerApproval {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            name: "inner.approval".into(),
            version: "1".into(),
            digest: [42; 32],
        }
    }
    fn evaluate<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        _: &'a RuntimeScope,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>>
    {
        let decision = if invocation.name == "run_js" {
            ToolPolicyDecision::Allow
        } else {
            ToolPolicyDecision::RequireApproval {
                prompt: "Approve inner echo".into(),
            }
        };
        let _ = io;
        Ok(crate::store::HistoryContentFuture::Ordinary {
            future: Box::pin(async move { Ok((decision, None)) }),
            frame_bytes: 0,
        })
    }
}

fn executor(
    code: &str,
    policy: Arc<dyn ToolPolicy>,
    grant_echo: bool,
) -> Result<(StockExecutor, Arc<ScriptedModelProvider>, Arc<FakeTool>)> {
    let echo = Arc::new(FakeTool(AtomicUsize::new(0)));
    let object = json!({"type":"object"});
    let mut tools = echo_tools(
        echo.clone(),
        object.clone(),
        object.clone(),
        crate::tool::json_projection_schema(object),
    )?;
    let js = crate::tools::catalog()
        .find(|spec| spec.name == "run_js")
        .unwrap()
        .bind()?;
    tools.register((js, None), &mut None, None)?;
    let model = Arc::new(
        ScriptedModelProvider::new([
            tool_turn([ModelEvent::ToolCall(crate::model::ToolCall::new(
                "program".into(),
                "run_js".into(),
                json!({"code":code}),
            ))]),
            done_turn(),
        ])
        .with_token_counter(fixture_token_count),
    );
    let mut grants = vec!["tool:call:run_js", capability::INTERACTION_ROUTE];
    if grant_echo {
        grants.push("tool:call:example.echo");
    }
    let executor = uncompacted_executor(
        Model::new("test".into(), "model".into(), "1".into())?,
        model.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(Capabilities::new(grants), Limits::default())?,
        policy,
    )?;
    Ok((executor, model, echo))
}

fn input() -> TurnInput {
    TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("compose".into()),
        selected_context: None,
        max_steps: 2,
    }
}

fn result(model: &ScriptedModelProvider) -> Value {
    let requests = model.requests();
    let messages = &requests[1].messages;
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.role == ModelRole::Tool)
            .count(),
        1
    );
    messages
        .iter()
        .find_map(|message| match &message.content {
            ModelContent::Part(ModelContentPart::ToolResult {
                content: crate::model::ToolResultContent::Json { value },
                ..
            }) => Some(value.clone()),
            _ => None,
        })
        .unwrap()
}

#[tokio::test]
async fn inner_approval_replays_receipts_and_refusal_is_a_js_exception_with_a_log() -> Result<()> {
    for approved in [false, true] {
        let (executor, model, echo) = executor(
            "await tools['example.echo']({value: 42})",
            Arc::new(InnerApproval),
            true,
        )?;
        let input = input();
        let journal = Journal::default();
        assert!(matches!(
            executor.execute_progress(input.clone(), &journal).await?,
            ExecutionProgress::Pending(_)
        ));
        assert_eq!(echo.0.load(Ordering::SeqCst), 0);
        let mut approvals = journal.2.lock().unwrap();
        assert_eq!(approvals.len(), 1);
        approvals.values_mut().next().unwrap().1 = Some(if approved {
            InteractionOutcome::Approved {}
        } else {
            InteractionOutcome::Denied {}
        });
        drop(approvals);
        executor.execute(input.clone(), &journal).await?;
        let value = result(&model);
        assert_eq!(value["calls"][0]["tool"], "example.echo");
        assert_eq!(value["calls"][0]["ok"], approved);
        if approved {
            assert_eq!(value["value"]["value"], 42);
        } else {
            assert!(
                value["error"]
                    .as_str()
                    .unwrap()
                    .contains("tool execution failed: interaction denied")
            );
        }
        assert_eq!(echo.0.load(Ordering::SeqCst), usize::from(approved));
        executor.execute(input, &journal).await?;
        assert_eq!(echo.0.load(Ordering::SeqCst), usize::from(approved));
        let records = journal.0.lock().unwrap();
        assert_eq!(records.iter().filter(|record| matches!(&record.event, ExecutionEvent::ToolCompleted { call_id, .. } if call_id != "program")).count(), usize::from(approved));
    }
    Ok(())
}

#[tokio::test]
async fn retained_inner_context_survives_both_replay_entry_points_without_running_js() -> Result<()>
{
    let code = "await tools['example.echo']({value:42})";
    let (executor, model, echo) = executor(code, allowed()?, true)?;
    let input = input();
    let journal = Journal::default();
    executor.execute(input.clone(), &journal).await?;
    let (reference, _projection) = journal
        .0
        .lock()
        .unwrap()
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ToolCompleted {
                call_id,
                result,
                projection,
                ..
            } if call_id == "program" => Some((result.clone(), projection.clone())),
            _ => None,
        })
        .unwrap();
    let (mut retained, _hold) = load_json::<ToolResult>(&journal, &reference, None).await?;
    // Extend the real engine's receipt with retained hook context. It is private
    // receipt metadata, and must not change the previously captured projection.
    retained.value["context"] = json!(["inner before", "inner after"]);
    let reference = stage_json(
        &journal,
        input.operation_id,
        "retained-context",
        &retained,
        None,
    )
    .await?;
    for record in journal.0.lock().unwrap().iter_mut() {
        if let ExecutionEvent::ToolCompleted {
            call_id, result, ..
        } = &mut record.event
            && call_id == "program"
        {
            *result = reference.clone();
        }
    }
    let execution = StockExecution::ordinary(&executor);
    let invocation = ToolInvocation::for_model_call(
        input.operation_id,
        0,
        "program".into(),
        "run_js".into(),
        json!({"code":code}),
        None,
    )?;
    let mut context = Vec::new();
    let mut messages = vec![ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall(crate::model::ToolCall::new(
            "program".into(),
            "run_js".into(),
            json!({"code":code}),
        ))),
    }];
    let progress = execution
        .settle_tool_call(
            &journal,
            input.operation_id,
            0,
            invocation,
            0,
            &mut messages,
            &mut context,
        )
        .await?;
    assert!(matches!(progress, ExecutionProgress::Ready(())));
    let ModelContent::Part(ModelContentPart::ToolResult { content, .. }) =
        &messages.last().unwrap().content
    else {
        panic!("replay did not return its retained projection");
    };
    assert_eq!(
        serde_json::to_value(content).unwrap()["value"],
        result(&model)
    );
    assert_eq!(
        context
            .iter()
            .map(|message| &message.content)
            .collect::<Vec<_>>(),
        vec![
            &ModelContent::Text("inner before".into()),
            &ModelContent::Text("inner after".into())
        ]
    );
    let prefix = execution
        .completed_tool_prefix(&journal, &input, 0, None)
        .await?;
    let restored = prefix
        .request()
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            ModelContent::Text(text) if text.starts_with("inner ") => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(restored, ["inner before", "inner after"]);
    assert_eq!(echo.0.load(Ordering::SeqCst), 1);
    // Negative control: metadata is never included in the model-visible output.
    assert!(result(&model).get("context").is_none());
    Ok(())
}

#[tokio::test]
async fn value_encoding_and_exception_formatting_cannot_abandon_inner_calls() -> Result<()> {
    for code in [
        "({toJSON() { tools['example.echo']({value:42}); return 1; }})",
        "throw {toString() { tools['example.echo']({value:42}); return 'failed'; }}",
    ] {
        let (executor, model, echo) = executor(code, allowed()?, true)?;
        let journal = Journal::default();
        executor.execute(input(), &journal).await?;
        let value = result(&model);
        assert_eq!(echo.0.load(Ordering::SeqCst), 1);
        assert_eq!(value["calls"].as_array().unwrap().len(), 1);
        assert_eq!(value["calls"][0]["ok"], true);
        assert_eq!(journal.0.lock().unwrap().iter().filter(|record| matches!(&record.event, ExecutionEvent::ToolCompleted { call_id, .. } if call_id != "program")).count(), 1);
        if code.starts_with('(') {
            assert_eq!(value["value"], 1);
        } else {
            assert_eq!(value["error"], "failed");
        }
    }
    Ok(())
}

#[tokio::test]
async fn mutating_builtins_cannot_abandon_queued_calls() -> Result<()> {
    let code = r#"
        for (let value = 0; value < 3; value++) tools['example.echo']({value});
        Array.from = null;
        Set.prototype.add = Set.prototype.delete = null;
        Object.defineProperty(Set.prototype, 'size', {get() { throw 'mutated size'; }});
        Promise.allSettled = null;
        Promise.prototype.then = () => { throw 'mutated then'; };
        Object.defineProperty(Promise.prototype, 'constructor', {get() { throw 'mutated constructor'; }});
        Object.defineProperty(Promise, Symbol.species, {get() { throw 'mutated species'; }});
        Object.defineProperty = null;
        42
    "#;
    let (executor, model, echo) = executor(code, allowed()?, true)?;
    let journal = Journal::default();
    executor.execute(input(), &journal).await?;
    let value = result(&model);
    assert_eq!(value["value"], 42);
    assert_eq!(echo.0.load(Ordering::SeqCst), 3);
    assert_eq!(value["calls"].as_array().unwrap().len(), 3);
    assert!(
        value["calls"]
            .as_array()
            .unwrap()
            .iter()
            .all(|call| call["ok"] == true)
    );
    assert_eq!(journal.0.lock().unwrap().iter().filter(|record| matches!(&record.event, ExecutionEvent::ToolCompleted { call_id, .. } if call_id != "program")).count(), 3);
    Ok(())
}

#[tokio::test]
async fn sandbox_exposes_only_granted_tools_and_their_original_schema() -> Result<()> {
    let code = r#"let refused = false; try { Function('return process.env')(); } catch (e) { refused = e instanceof ReferenceError; } ({refused, sum: [1,2,3].reduce((a,b) => a+b), host: [typeof fetch, typeof require, typeof process, typeof std, typeof os, typeof readFile, typeof Deno, typeof Bun, typeof WebSocket, typeof Date], hidden: typeof tools['example.echo'], bound: typeof tools.run_js, schema: tools.run_js.input_schema.required})"#;
    let (executor, model, echo) = executor(code, allowed()?, false)?;
    executor.execute(input(), &Journal::default()).await?;
    let value = result(&model);
    assert_eq!(value["value"]["host"], json!(vec!["undefined"; 10]));
    assert_eq!(value["value"]["hidden"], "undefined");
    assert_eq!(value["value"]["bound"], "function");
    assert_eq!(value["value"]["schema"], json!(["code"]));
    assert_eq!(echo.0.load(Ordering::SeqCst), 0);
    assert_eq!(value["value"]["refused"], true);
    // Negative control: ordinary JavaScript still runs in this same sandbox.
    assert_eq!(value["value"]["sum"], 6);
    Ok(())
}

#[tokio::test]
async fn intermediate_data_stays_in_js_and_both_final_values_and_logs_are_paged() -> Result<()> {
    for code in [
        "let total=0; for(let i=0;i<400;i++) { const x=await tools['example.echo']({data:'x'.repeat(10000)}); total+=x.data.length; } total",
        "'x'.repeat(1000000)",
        "throw new Error('x'.repeat(1000000))",
    ] {
        let (executor, model, _) = executor(code, allowed()?, true)?;
        executor.execute(input(), &Journal::default()).await?;
        let value = result(&model);
        assert!(serde_json::to_vec(&value).unwrap().len() <= super::super::code_mode::OUTPUT_BYTES);
        if code.starts_with("let") {
            assert_eq!(value["value"], 4_000_000);
            assert!(value["omitted_calls"].as_u64().unwrap() > 0);
        } else if code.starts_with("'x'") {
            assert_eq!(value["value"]["truncated"], true);
        } else {
            assert!(value["error"].as_str().unwrap().len() <= 2048);
        }
    }
    Ok(())
}

#[tokio::test]
async fn oversized_call_names_are_omitted_without_losing_results_or_receipts() -> Result<()> {
    let length = super::super::code_mode::OUTPUT_BYTES + 1;
    let name = "x".repeat(length);
    let code = format!(
        "await tools['x'.repeat({length})]({{value:41}}); (await tools['example.echo']({{value:42}})).value"
    );
    let (mut executor, model, echo) = executor(&code, allowed()?, true)?;
    let mut tool = executor
        .tools
        .get("example.echo", None, None)?
        .unwrap()
        .clone();
    tool.definition.name = name.clone();
    executor.tools.register((tool, None), &mut None, None)?;
    executor = executor.with_tool_authority(
        RuntimeScope::new(
            Capabilities::new([
                "tool:call:run_js".to_owned(),
                "tool:call:example.echo".to_owned(),
                format!("tool:call:{name}"),
                capability::INTERACTION_ROUTE.to_owned(),
            ]),
            Limits::default(),
        )?,
        allowed()?,
    )?;
    let journal = Journal::default();
    executor.execute(input(), &journal).await?;
    let value = result(&model);
    assert_eq!(value["value"], 42);
    assert_eq!(value["omitted_calls"], 1);
    assert_eq!(echo.0.load(Ordering::SeqCst), 2);
    assert_eq!(value["calls"].as_array().unwrap().len(), 1);
    // Negative control: the later fitting entry is retained after the omission.
    assert_eq!(value["calls"][0]["tool"], "example.echo");
    assert_eq!(value["calls"][0]["ok"], true);
    assert!(serde_json::to_vec(&value).unwrap().len() <= super::super::code_mode::OUTPUT_BYTES);
    assert_eq!(journal.0.lock().unwrap().iter().filter(|record| matches!(&record.event, ExecutionEvent::ToolCompleted { call_id, .. } if call_id != "program")).count(), 2);
    Ok(())
}

#[tokio::test]
async fn escaped_diagnostics_fit_the_remaining_page_and_retain_the_call_log() -> Result<()> {
    for (expression, expected) in [
        (r#"'\n'.repeat(2048)"#, "\n".repeat(2048)),
        (r#"'\u0001'.repeat(2048)"#, "\u{1}".repeat(2048)),
        ("'💡'.repeat(2048)", "💡".repeat(1024)),
        (r#"'short "error"'"#, "short \"error\"".into()),
    ] {
        let code = format!("await tools['example.echo']({{value:42}}); throw {expression}");
        let (mut executor, model, echo) = executor(&code, allowed()?, true)?;
        executor.limits.render_bytes = 4096;
        executor.execute(input(), &Journal::default()).await?;
        let value = result(&model);
        let error = value["error"].as_str().unwrap();
        assert!(!error.is_empty());
        assert!(expected.starts_with(error));
        assert!(serde_json::to_vec(&value).unwrap().len() <= 4096);
        assert_eq!(echo.0.load(Ordering::SeqCst), 1);
        assert_eq!(value["calls"].as_array().unwrap().len(), 1);
        assert_eq!(value["calls"][0]["ok"], true);
        // Negative control: a fitting error is preserved exactly.
        if expression.starts_with("'short") {
            assert_eq!(error, expected);
        } else {
            assert!(error.len() < expected.len());
        }
    }
    Ok(())
}

#[tokio::test]
async fn cancelling_the_parent_interrupts_cpu_code_and_records_retirement() -> Result<()> {
    struct Ready(Mutex<Option<tokio::sync::oneshot::Sender<()>>>);
    impl ToolExecutor for Ready {
        fn execute<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<crate::runtime::ToolContext>,
        ) -> BoxFuture<'a, Result<ToolResult>> {
            Box::pin(async {
                if let Some(send) = self.0.lock().unwrap().take() {
                    let _ = send.send(());
                }
                Ok(ToolResult { value: json!({}) })
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
    let (mut executor, _, _) = executor(
        "await tools['example.echo']({}); while (true) {}",
        allowed()?,
        true,
    )?;
    let (send, ready) = tokio::sync::oneshot::channel();
    let mut tool = executor
        .tools
        .get("example.echo", None, None)?
        .unwrap()
        .clone();
    tool.definition.revision = "ready".into();
    tool.executor = Arc::new(Ready(Mutex::new(Some(send))));
    executor.tools.register((tool, None), &mut None, None)?;
    executor
        .tools
        .select_model_version("example.echo", "ready")?;
    let journal = Journal::default();
    let (execution, ()) = tokio::join!(executor.execute_progress(input(), &journal), async {
        ready.await.unwrap();
        journal.10.cancel();
    });
    assert!(matches!(
        execution?,
        ExecutionProgress::Failed(ToolFailureKind::Interrupted)
    ));
    assert!(journal.0.lock().unwrap().iter().any(|record| matches!(&record.event, ExecutionEvent::ToolFailed { call_id, reason: ToolFailureKind::Interrupted, .. } if call_id == "program")));
    Ok(())
}
