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
        let decision = if invocation.name == "code" {
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
        .find(|spec| spec.name == "code")
        .unwrap()
        .bind()?;
    tools.register((js, None), &mut None, None)?;
    let model = Arc::new(
        ScriptedModelProvider::new([
            tool_turn([ModelEvent::ToolCall(crate::model::ToolCall::new(
                "program".into(),
                "code".into(),
                json!({"code":code}),
            ))]),
            done_turn(),
        ])
        .with_token_counter(fixture_token_count),
    );
    let mut grants = vec!["tool:call:code", capability::INTERACTION_ROUTE];
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

fn logs(journal: &Journal) -> Value {
    let mut entries = journal
        .1
        .lock()
        .unwrap()
        .values()
        .filter_map(|(_, bytes)| {
            let value: Value = serde_json::from_slice(bytes).ok()?;
            (value["kind"] == "code.call").then_some(value)
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by_key(|entry| entry["ordinal"].as_u64().unwrap());
    Value::Array(entries)
}

#[tokio::test]
async fn dynamic_tools_update_notices_and_program_bindings_without_changing_declarations()
-> Result<()> {
    let (first, first_model, _) = executor("typeof tools.late", allowed()?, false)?;
    first.execute(input(), &Journal::default()).await?;
    assert_eq!(result(&first_model), "undefined");
    let (mut second, second_model, echo) = executor(
        "(await Promise.all([tools.late({value:42}), tools['example.echo']({value:43})])).reduce((sum, item) => sum + item.value, 0)",
        allowed()?,
        true,
    )?;
    let mut tool = second
        .tools
        .get("example.echo", None, None)?
        .unwrap()
        .clone();
    tool.definition.name = "late".into();
    second.tools.register((tool, None), &mut None, None)?;
    second = second.with_tool_authority(
        RuntimeScope::new(
            Capabilities::new([
                "tool:call:code",
                "tool:call:example.echo",
                "tool:call:late",
                capability::INTERACTION_ROUTE,
            ]),
            Limits::default(),
        )?,
        allowed()?,
    )?;
    let journal = Journal::default();
    second.execute(input(), &journal).await?;
    assert_eq!(result(&second_model), 85);
    assert_eq!(echo.0.load(Ordering::SeqCst), 2);
    let before = first_model.requests();
    let after = second_model.requests();
    assert_eq!(
        before[0].model_tools().collect::<Vec<_>>(),
        after[0].model_tools().collect::<Vec<_>>()
    );
    assert_eq!(
        after[0]
            .model_tools()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        ["code"]
    );
    let notice = |request: &ModelRequest| {
        request
            .messages
            .iter()
            .find_map(|message| {
                let (notices, _) = message.content.notice_parts()?;
                notices
                    .iter()
                    .find(|notice| notice.source == "tools" && notice.kind == "tools.changed")
                    .map(|notice| notice.body.clone())
            })
            .unwrap()
    };
    assert_eq!(notice(&before[0])["tools"].as_array().unwrap().len(), 1);
    let changed = notice(&after[0]);
    assert_eq!(changed["tools"].as_array().unwrap().len(), 3);
    assert_eq!(changed["tools"][2]["name"], "late");
    assert_eq!(
        changed["tools"][2]["input_schema"],
        json!({"type":"object"})
    );
    assert_eq!(
        after[0].messages,
        after[1].messages[..after[0].messages.len()]
    );
    assert_eq!(logs(&journal).as_array().unwrap().len(), 2);
    assert_eq!(logs(&journal)[0]["input_bytes"], 12);
    assert_eq!(logs(&journal)[0]["output_bytes"], 12);
    // Removing the capability updates the same notice and the next VM table.
    let (mut revoked, revoked_model, _) =
        executor("typeof tools['example.echo']", allowed()?, true)?;
    revoked.tools.remove_from_model("example.echo")?;
    revoked.execute(input(), &Journal::default()).await?;
    assert_eq!(result(&revoked_model), "undefined");
    assert_eq!(
        notice(&revoked_model.requests()[0])["tools"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    Ok(())
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
        assert_eq!(logs(&journal)[0]["tool"], "example.echo");
        assert_eq!(logs(&journal)[0]["ok"], approved);
        if approved {
            assert_eq!(value["value"], 42);
        } else {
            assert!(
                value["error"]
                    .as_str()
                    .unwrap()
                    .contains("tool execution failed: interaction denied")
            );
        }
        assert_eq!(echo.0.load(Ordering::SeqCst), usize::from(approved));
        let execution = StockExecution::ordinary(&executor);
        let invocation = ToolInvocation::for_model_call(
            input.operation_id,
            0,
            "program".into(),
            "code".into(),
            json!({"code":"await tools['example.echo']({value: 42})"}),
            None,
        )?;
        let loads = journal.4.load(Ordering::SeqCst);
        let ExecutionProgress::Ready(replayed) = execution
            .code(
                &journal,
                (input.operation_id, 0),
                &invocation,
                &mut Vec::new(),
            )
            .await?
        else {
            panic!("settled inner call did not return its original receipt");
        };
        assert_eq!(crate::tools::code_mode::output(&replayed.value)?, &value);
        // Invocation, result and projection are each loaded once. Restoring
        // the post-settlement result lookup fails this real-engine control.
        assert_eq!(
            journal.4.load(Ordering::SeqCst) - loads,
            3 * usize::from(approved)
        );
        executor.execute(input, &journal).await?;
        assert_eq!(echo.0.load(Ordering::SeqCst), usize::from(approved));
        let records = journal.0.lock().unwrap();
        assert_eq!(records.iter().filter(|record| matches!(&record.event, ExecutionEvent::ToolCompleted { call_id, .. } if call_id != "program")).count(), usize::from(approved));
    }
    Ok(())
}

#[tokio::test]
async fn logs_publish_before_the_next_call_and_the_parent_result() -> Result<()> {
    struct Observe(Arc<Journal>);
    impl ToolExecutor for Observe {
        fn execute<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<crate::runtime::ToolContext>,
        ) -> BoxFuture<'a, Result<ToolResult>> {
            Box::pin(async {
                let observed = logs(&self.0);
                assert_eq!(observed.as_array().unwrap().len(), 1);
                assert_eq!(observed[0]["tool"], "example.echo");
                assert_eq!(observed[0]["ok"], true);
                assert_eq!(observed[0]["call_id"], "program");
                assert!(observed[0].get("value").is_none());
                assert!(!self.0.0.lock().unwrap().iter().any(|record| matches!(&record.event, ExecutionEvent::ToolCompleted { call_id, .. } if call_id == "program")));
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
    let (mut executor, model, _) = executor(
        "await tools['example.echo']({value:42}); await tools.observe({}); 7",
        allowed()?,
        true,
    )?;
    let journal = Arc::new(Journal::default());
    let mut tool = executor
        .tools
        .get("example.echo", None, None)?
        .unwrap()
        .clone();
    tool.definition.name = "observe".into();
    tool.executor = Arc::new(Observe(journal.clone()));
    executor.tools.register((tool, None), &mut None, None)?;
    executor = executor.with_tool_authority(
        RuntimeScope::new(
            Capabilities::new([
                "tool:call:code",
                "tool:call:example.echo",
                "tool:call:observe",
                capability::INTERACTION_ROUTE,
            ]),
            Limits::default(),
        )?,
        allowed()?,
    )?;
    executor.execute(input(), journal.as_ref()).await?;
    assert_eq!(result(&model), 7);
    assert_eq!(logs(&journal).as_array().unwrap().len(), 2);
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
        "code".into(),
        json!({"code":code}),
        None,
    )?;
    let mut context = Vec::new();
    let mut messages = vec![ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall(crate::model::ToolCall::new(
            "program".into(),
            "code".into(),
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
    let ExecutionProgress::Ready(Some((receipt, _hold))) = progress else {
        panic!("settled replay did not retain its result");
    };
    assert_eq!(receipt, retained);
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
        assert_eq!(logs(&journal).as_array().unwrap().len(), 1);
        assert_eq!(logs(&journal)[0]["ok"], true);
        assert_eq!(journal.0.lock().unwrap().iter().filter(|record| matches!(&record.event, ExecutionEvent::ToolCompleted { call_id, .. } if call_id != "program")).count(), 1);
        if code.starts_with('(') {
            assert_eq!(value, 1);
        } else {
            assert_eq!(value["error"], "failed");
        }
    }
    Ok(())
}

#[tokio::test]
async fn producerless_promises_fail_after_tool_retirement_and_live_producers_complete() -> Result<()>
{
    for (code, calls, stalled) in [
        ("await new Promise(() => {})", 0, true),
        (
            "tools['example.echo']({value:1}); await new Promise(() => {})",
            1,
            true,
        ),
        (
            "await new Promise(resolve => Promise.resolve().then(() => resolve(42)))",
            0,
            false,
        ),
        (
            "await new Promise(resolve => tools['example.echo']({value:42}).then(value => resolve(value.value)))",
            1,
            false,
        ),
    ] {
        let (executor, model, echo) = executor(code, allowed()?, true)?;
        let journal = Journal::default();
        executor.execute(input(), &journal).await?;
        let value = result(&model);
        if stalled {
            assert_eq!(
                value["error"],
                "JavaScript promise cannot settle: no pending jobs or tool calls"
            );
        } else {
            assert_eq!(value, 42);
            assert!(value.get("error").is_none());
        }
        assert_eq!(echo.0.load(Ordering::SeqCst), calls);
        assert_eq!(logs(&journal).as_array().unwrap().len(), calls);
        assert_eq!(journal.0.lock().unwrap().iter().filter(|record| matches!(&record.event, ExecutionEvent::ToolCompleted { call_id, .. } if call_id != "program")).count(), calls);
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
    assert_eq!(value, 42);
    assert_eq!(echo.0.load(Ordering::SeqCst), 3);
    assert_eq!(logs(&journal).as_array().unwrap().len(), 3);
    assert!(
        logs(&journal)
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
    let code = r#"let refused = false; try { Function('return process.env')(); } catch (e) { refused = e instanceof ReferenceError; } ({refused, sum: [1,2,3].reduce((a,b) => a+b), host: [typeof fetch, typeof require, typeof process, typeof std, typeof os, typeof readFile, typeof Deno, typeof Bun, typeof WebSocket, typeof Date], hidden: typeof tools['example.echo'], bound: typeof tools.code, schema: tools.code.input_schema.required})"#;
    let (executor, model, echo) = executor(code, allowed()?, false)?;
    let journal = Journal::default();
    executor.execute(input(), &journal).await?;
    let value = result(&model);
    assert_eq!(value["host"], json!(vec!["undefined"; 10]));
    assert_eq!(value["hidden"], "undefined");
    assert_eq!(value["bound"], "function");
    assert_eq!(value["schema"], json!(["code"]));
    assert_eq!(echo.0.load(Ordering::SeqCst), 0);
    assert_eq!(value["refused"], true);
    // Negative control: ordinary JavaScript still runs in this same sandbox.
    assert_eq!(value["sum"], 6);
    Ok(())
}

#[tokio::test]
async fn intermediate_data_stays_in_js_and_results_use_the_admitted_render_budget() -> Result<()> {
    let render_bytes = 65_536;
    let string_bytes = render_bytes - crate::tool::schema::json_envelope_bytes()? - 2;
    let code = format!("'x'.repeat({string_bytes})");
    let (mut boundary_executor, model, _) = executor(&code, allowed()?, true)?;
    boundary_executor.limits.render_bytes = render_bytes as u64;
    boundary_executor
        .execute(input(), &Journal::default())
        .await?;
    // Each result retains its complete admitted per-part allowance even though
    // context framing and the other messages exceed that same byte count.
    assert_eq!(result(&model).as_str().unwrap().len(), string_bytes);
    let (mut fitting, model, _) = executor("'x'.repeat(40000)", allowed()?, true)?;
    fitting.limits.render_bytes = render_bytes as u64;
    let mut turn = input();
    turn.input = ModelContent::Text("context".repeat(1000));
    fitting.execute(turn, &Journal::default()).await?;
    assert_eq!(result(&model).as_str().unwrap().len(), 40000);
    for code in [
        "let total=0; for(let i=0;i<400;i++) { const x=await tools['example.echo']({data:'x'.repeat(10000)}); total+=x.data.length; } total",
        "'x'.repeat(1000000)",
        "throw new Error('x'.repeat(1000000))",
    ] {
        let (mut executor, model, _) = executor(code, allowed()?, true)?;
        executor.limits.render_bytes = 65_536;
        let journal = Journal::default();
        executor.execute(input(), &journal).await?;
        let value = result(&model);
        assert!(serde_json::to_vec(&value).unwrap().len() <= 65_536);
        if code.starts_with("let") {
            assert_eq!(value, 4_000_000);
            assert_eq!(logs(&journal).as_array().unwrap().len(), 400);
        } else if code.starts_with("'x'") {
            assert_eq!(value["truncated"], true);
        } else {
            assert!(value["error"].as_str().unwrap().len() <= 2048);
        }
    }
    Ok(())
}

#[tokio::test]
async fn large_call_names_stream_without_losing_results_or_receipts() -> Result<()> {
    let length = 65_536 + 1;
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
                "tool:call:code".to_owned(),
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
    assert_eq!(value, 42);
    assert!(value.get("omitted_calls").is_none());
    assert_eq!(echo.0.load(Ordering::SeqCst), 2);
    assert_eq!(logs(&journal).as_array().unwrap().len(), 2);
    // Both entries are published, independent of the final result size.
    assert_eq!(logs(&journal)[0]["tool"], name);
    assert_eq!(logs(&journal)[1]["tool"], "example.echo");
    assert_eq!(logs(&journal)[1]["ok"], true);
    assert!(serde_json::to_vec(&value).unwrap().len() <= 65_536);
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
        let journal = Journal::default();
        executor.execute(input(), &journal).await?;
        let value = result(&model);
        let error = value["error"].as_str().unwrap();
        assert!(!error.is_empty());
        assert!(expected.starts_with(error));
        assert!(serde_json::to_vec(&value).unwrap().len() <= 4096);
        assert_eq!(echo.0.load(Ordering::SeqCst), 1);
        assert_eq!(logs(&journal).as_array().unwrap().len(), 1);
        assert_eq!(logs(&journal)[0]["ok"], true);
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

#[tokio::test]
async fn retained_dynamic_exchange_keeps_its_validation_schema() -> Result<()> {
    let (executor, _, _) = executor("42", allowed()?, true)?;
    let execution = StockExecution::ordinary(&executor);
    let prior = vec![
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::ToolCall(crate::model::ToolCall::new(
                "old".into(),
                "example.echo".into(),
                json!({"value":7}),
            ))),
        },
        ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: "old".into(),
                name: "example.echo".into(),
                content: crate::model::ToolResultContent::Json {
                    value: json!({"value":7}),
                },
            }),
        },
    ];
    let prepared = execution.prepare_request(&input(), 1, &prior).await?;
    let request = prepared.request();
    assert!(request.tools.iter().any(|tool| tool.name == "example.echo"));
    assert_eq!(
        request
            .model_tools()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        ["code"]
    );
    // Hiding a declaration must neither drop validation nor admit a bad result.
    let mut corrupted = request.clone();
    let result = corrupted
        .messages
        .iter_mut()
        .find_map(|message| match &mut message.content {
            ModelContent::Part(ModelContentPart::ToolResult { content, .. }) => Some(content),
            _ => None,
        })
        .unwrap();
    *result = crate::model::ToolResultContent::Json { value: json!(7) };
    assert!(crate::model::PreparedModelRequest::prepare(corrupted, executor.limits, None).is_err());
    Ok(())
}

#[tokio::test]
async fn multiple_results_reach_continuation_with_per_part_render_bounds() -> Result<()> {
    let (mut executor, _, _) = executor("42", allowed()?, true)?;
    let model = Arc::new(
        ScriptedModelProvider::new([
            tool_turn(["first", "second"].map(|id| {
                ModelEvent::ToolCall(crate::model::ToolCall::new(
                    id.into(),
                    "code".into(),
                    json!({"code":"'x'.repeat(40000)"}),
                ))
            })),
            done_turn(),
        ])
        .with_token_counter(fixture_token_count),
    );
    executor.provider = model.clone();
    executor.limits.render_bytes = 65_536;
    let mut turn = input();
    turn.input = ModelContent::Text("context".repeat(1000));
    let journal = Journal::default();
    executor.execute(turn.clone(), &journal).await?;
    let requests = model.requests();
    let values = requests[1]
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            ModelContent::Part(ModelContentPart::ToolResult {
                content: crate::model::ToolResultContent::Json { value },
                ..
            }) => Some(value),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(values[0].as_str().unwrap().len(), 40000);
    assert_eq!(values[1].as_str().unwrap().len(), 40000);
    assert!(serde_json::to_vec(&requests[1].messages).unwrap().len() > 65_536);
    // The same durable exchange remains valid on replay, without a second model call.
    executor.execute(turn, &journal).await?;
    assert_eq!(model.requests().len(), 2);
    Ok(())
}
