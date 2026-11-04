use super::*;

/// Deterministic model that releases every request after its stream. Recording
/// providers deliberately clone inputs for their test observers; that makes
/// their encoded buffers shared and exercises the explicit copy boundary.
struct ContinuingModel {
    turns: u32,
    seen: AtomicUsize,
}

impl ContinuingModel {
    fn answer(
        &self,
        request: &crate::model::PreparedModelRequest,
        dispatch: crate::model::ModelDispatch,
    ) -> Result<Vec<ModelEvent>> {
        if dispatch.step > 0 && request.cached_message_digests(None, None)?.is_none() {
            return Err(Error::Invalid(
                "stock turn lost its admitted request continuation".into(),
            ));
        }
        if request.request().messages.len() != 1 + 2 * dispatch.step as usize {
            return Err(Error::Invalid(
                "stock continuation lost or duplicated messages".into(),
            ));
        }
        if [0, 1, 10].contains(&dispatch.step) || dispatch.step == self.turns {
            let canonical = crate::contract::canonical_json_bytes(request.request())?;
            if request.bytes() != canonical
                || request.request_digest() != *blake3::hash(&canonical).as_bytes()
            {
                return Err(Error::Invalid(
                    "stock continuation changed canonical bytes".into(),
                ));
            }
        }
        self.seen.fetch_add(1, Ordering::SeqCst);
        let event = if dispatch.step < self.turns {
            ModelEvent::ToolCall(crate::model::ToolCall::new(
                format!("call-{}", dispatch.step),
                "example.echo".into(),
                json!({}),
            ))
        } else {
            ModelEvent::Content {
                delta: "done".into(),
            }
        };
        Ok(vec![
            event,
            ModelEvent::Completed {
                metadata: Value::Null,
            },
        ])
    }
}

impl ModelProvider for ContinuingModel {
    fn generate<'a>(
        &'a self,
        request: &'a crate::model::PreparedModelRequest,
        dispatch: crate::model::ModelDispatch,
    ) -> crate::model::ModelStream<'a> {
        let events = match self.answer(request, dispatch) {
            Ok(events) => events.into_iter().map(Ok).collect(),
            Err(error) => vec![Err(error)],
        };
        crate::model::ModelStream::new(Box::pin(futures::stream::iter(events)))
    }
}

#[test]
fn one_stock_turn_preserves_admitted_continuations_across_step_limits() -> Result<()> {
    use crate::filesystem::ExecutionReplayObservation as ReplayObservation;
    use acyclic_test_support::differential::observe;

    #[cfg(not(target_arch = "wasm32"))]
    let visits =
        |trace: &[ReplayObservation], event| trace.iter().filter(|kind| **kind == event).count();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let storage = runtime.block_on(crate::filesystem::MemoryHarnessStorage::new(
        crate::AgentId::new(),
        65_536,
    ))?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    // Exercise both sides of the message-vector growth boundary and each step
    // limit outcome here. The shared continuation properties measure encoding,
    // validation and buffer growth through 1,000 messages independently.
    for turns in [1, 16, 17, 32] {
        let budgets = if turns == 17 {
            vec![None, Some(1), Some(turns + 1)]
        } else {
            vec![None]
        };
        for max_steps in budgets {
            let provider = Arc::new(ContinuingModel {
                turns,
                seen: AtomicUsize::new(0),
            });
            let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
            let executor = uncompacted_executor(
                Model::new("test".into(), "continuation".into(), "1".into())?,
                provider.clone(),
                ContextPipeline::default(),
                echo_tools(tool.clone(), json!(true), json!(true), json!(true))?,
            )
            .with_tool_authority(
                RuntimeScope::new(
                    Capabilities::new(["model:generate", "tool:call:example.echo"]),
                    Limits::default(),
                )?,
                allowed()?,
            )?;
            let input = TurnInput {
                max_steps,
                operation_id: OperationId::new(),
                input: ModelContent::Text("input".into()),
                selected_context: None,
            };
            // Capture the shipped ordinary entry point, including the real
            // authenticated journal. No fixture substitutes its indexed reads.
            let (result, _trace) = observe::<_, _, ReplayObservation>(
                || (),
                |()| runtime.block_on(executor.execute_progress(input.clone(), journal)),
            );
            #[cfg(not(target_arch = "wasm32"))]
            assert_eq!(
                visits(&_trace, ReplayObservation::Requested),
                0,
                "ordinary continuation requested journal replay at {turns} steps ({max_steps:?})"
            );
            if let Some(maximum) = max_steps.filter(|maximum| *maximum <= turns) {
                assert!(
                    matches!(result, Err(Error::Conflict(message)) if message == "executor step limit reached")
                );
                assert_eq!(provider.seen.load(Ordering::SeqCst), maximum as usize);
                assert_eq!(tool.0.load(Ordering::SeqCst), maximum as usize);
                continue;
            }
            assert!(
                result.is_ok(),
                "stock execution failed: {:?}",
                result.as_ref().err()
            );
            let ExecutionProgress::Ready(output) = result? else {
                return Err(Error::Invalid("scripted stock turn did not settle".into()));
            };
            assert_eq!(output.text, "done");
            assert_eq!(output.steps, turns + 1);
            #[cfg(feature = "research")]
            {
                let cut = output
                    .journal_cut
                    .expect("persisted terminal append receipt");
                assert_eq!(cut.version, 1);
                assert_eq!(cut.operation, input.operation_id);
                let (_, records) =
                    runtime.block_on(StockExecution::ordinary(&executor).model_records(
                        journal,
                        input.operation_id,
                        turns,
                        ModelPurpose::Response,
                    ))?;
                let terminal = records
                    .iter()
                    .find(|record| {
                        matches!(
                            &record.event,
                            ExecutionEvent::Model { usage: Some(_), .. }
                                | ExecutionEvent::ModelInline { usage: Some(_), .. }
                        )
                    })
                    .expect("original terminal usage");
                assert_eq!(cut.sequence, terminal.sequence);
            }
            let (replayed, _trace) = observe::<_, _, ReplayObservation>(
                || (),
                |()| runtime.block_on(executor.execute(input.clone(), journal)),
            );
            let replayed = replayed?;
            #[cfg(not(target_arch = "wasm32"))]
            assert_eq!(
                visits(&_trace, ReplayObservation::Requested),
                0,
                "restored continuation requested journal replay at {turns} steps ({max_steps:?})"
            );
            assert_eq!(replayed.text, output.text);
            assert_eq!(replayed.steps, output.steps);
            #[cfg(feature = "research")]
            assert_eq!(replayed.journal_cut, output.journal_cut);
            assert_eq!(tool.0.load(Ordering::SeqCst), turns as usize);
            assert_eq!(provider.seen.load(Ordering::SeqCst), turns as usize + 1);
            // Read one original record through the same journal's replay
            // entry point. Indexed point reads share the record decoder, so
            // observe replay requests separately from decoded records. This
            // control keeps both observations connected to the real journal.
            #[cfg(not(target_arch = "wasm32"))]
            if turns == 32 {
                let (record, _trace) = observe::<_, _, ReplayObservation>(
                    || (),
                    |()| {
                        runtime.block_on(async {
                            journal.replay(input.operation_id, 0, 1, None)?.await
                        })
                    },
                );
                assert_eq!(record?.len(), 1);
                assert_eq!(visits(&_trace, ReplayObservation::Requested), 1);
                assert_eq!(visits(&_trace, ReplayObservation::Decoded), 1);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn prepared_accounting_recovers_with_its_projection_before_model_dispatch() -> Result<()> {
    let (provider, counts) = counted_model(65_536, None, [ScriptedTurn::Text("done".into())], []);
    let executor = StockExecutor::new(
        Model::new("test".into(), "continuation".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    let input = TurnInput {
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("input".into()),
        selected_context: None,
    };
    let execution = StockExecution::ordinary(&executor);
    execution.ensure_started(journal, &input).await?;
    let (tail, _) = execution
        .model_records(journal, input.operation_id, 0, ModelPurpose::Response)
        .await?;
    let (projection, accounting, prepared) = execution
        .capture_response(journal, &input, 0, &[], tail, None)
        .await?;
    let (_, records) = execution
        .model_records(journal, input.operation_id, 0, ModelPurpose::Response)
        .await?;
    let reference = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ContextPrepared { projection, .. } => Some(projection.clone()),
            _ => None,
        })
        .unwrap();
    let bytes = journal.load(&reference, None)?.await?;
    let retained = serde_json::json!({
        "context": { "messages": prepared.request().messages, "metadata": projection.context.metadata, "current_input_index": projection.context.current_input_index },
        "canonical": projection.canonical, "checkpoint": projection.checkpoint,
        "accounting": accounting,
    });
    assert_eq!(
        &*bytes,
        crate::contract::canonical_json_bytes(&retained)?,
        "projection and accounting must share one immutable publication"
    );
    assert!(
        !records
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ModelStarted { .. }))
    );
    assert_eq!(counts.load(Ordering::SeqCst), 1);
    assert!(provider.requests().is_empty());
    let recovered = StockExecution::ordinary(&executor.clone())
        .prepared_response(journal, &input, 0, &[], None)
        .await?;
    assert_eq!(recovered.bytes(), prepared.bytes());
    assert_eq!(
        counts.load(Ordering::SeqCst),
        1,
        "recovery must not recount"
    );
    let resumed = executor.execute(input.clone(), journal).await?;
    assert_eq!(resumed.text, "done");
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(counts.load(Ordering::SeqCst), 1);
    let (_, records) = execution
        .model_records(journal, input.operation_id, 0, ModelPurpose::Response)
        .await?;
    #[cfg(feature = "research")]
    {
        let cut = resumed
            .journal_cut
            .expect("original terminal append receipt");
        assert_eq!(cut.version, 1);
        assert_eq!(cut.operation, input.operation_id);
        let terminal = records
            .iter()
            .find(|record| {
                matches!(
                    &record.event,
                    ExecutionEvent::Model { usage: Some(_), .. }
                        | ExecutionEvent::ModelInline { usage: Some(_), .. }
                )
            })
            .expect("original terminal usage");
        assert_eq!(cut.sequence, terminal.sequence);
        let replayed = executor.execute(input.clone(), journal).await?;
        assert_eq!(replayed.journal_cut, Some(cut));
        assert_eq!(
            provider.requests().len(),
            1,
            "replay must not generate again"
        );
    }
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.event, ExecutionEvent::ContextPrepared { .. }))
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn continued_tool_results_stop_before_aggregate_render_overflow_is_published_or_dispatched()
-> Result<()> {
    let limits = Limits {
        render_bytes: 512,
        ..Limits::default()
    };
    let provider = Arc::new(ContinuingModel {
        turns: 10,
        seen: AtomicUsize::new(0),
    });
    let executor = uncompacted_executor(
        Model::new("test".into(), "continuation".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        echo_tools(
            Arc::new(FakeTool(AtomicUsize::new(0))),
            json!(true),
            json!(true),
            json!(true),
        )?,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["model:generate", "tool:call:example.echo"]),
            limits,
        )?,
        allowed()?,
    )?
    .with_limits(limits);
    let journal = Journal::default();
    let input = TurnInput {
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("input".into()),
        selected_context: None,
    };
    let result = executor.execute(input, &journal).await;
    assert!(
        result.is_err(),
        "continued aggregate render overflow reached the provider"
    );
    let references: Vec<_> = journal
        .0
        .lock()
        .unwrap()
        .iter()
        .filter_map(|record| match &record.event {
            ExecutionEvent::ContextPrepared { projection, .. } => Some(projection.clone()),
            _ => None,
        })
        .collect();
    assert!(!references.is_empty());
    assert!(references.len() < 11);
    assert_eq!(references.len(), provider.seen.load(Ordering::SeqCst));
    for reference in references {
        let (projection, _) =
            crate::executor::transcript::load_projection(&journal, &reference, limits, None)
                .await?;
        crate::context::validate_projected_context(&projection.context, limits, None)?;
    }
    Ok(())
}
