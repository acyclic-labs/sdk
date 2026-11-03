use super::*;

/// Deterministic model that releases every request after its stream. Recording
/// providers deliberately clone inputs for their test observers; that makes
/// their encoded buffers shared and exercises the explicit copy boundary.
struct ContinuingModel {
    turns: u32,
    warm: bool,
    seen: AtomicUsize,
}

impl ContinuingModel {
    fn answer(
        &self,
        request: &crate::model::PreparedModelRequest,
        dispatch: crate::model::ModelDispatch,
    ) -> Result<Vec<ModelEvent>> {
        if self.warm && dispatch.step > 0 && request.cached_message_digests(None, None)?.is_none() {
            return Err(Error::Invalid(
                "stock turn lost its admitted request continuation".into(),
            ));
        }
        if request.request().messages.len() != 1 + 2 * dispatch.step as usize {
            return Err(Error::Invalid(
                "stock continuation lost or duplicated messages".into(),
            ));
        }
        if [0, 1, 10, 100, 1_000].contains(&dispatch.step) {
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
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        let events = match self.answer(request, dispatch) {
            Ok(events) => events.into_iter().map(Ok).collect(),
            Err(error) => vec![Err(error)],
        };
        Box::pin(futures::stream::iter(events))
    }
}

#[tokio::test]
async fn one_stock_turn_continues_its_admitted_request_from_one_to_a_thousand_steps() -> Result<()>
{
    for turns in [1, 16, 17, 100, 1_000] {
        let budgets = if turns == 17 {
            vec![None, Some(1), Some(turns + 1)]
        } else {
            vec![None]
        };
        for max_steps in budgets {
            let provider = Arc::new(ContinuingModel {
                turns,
                warm: true,
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
            let issuer = crate::core::AuthorityRoot::new(
                "continuation",
                [89; 32],
                crate::core::Authority {
                    kind: crate::core::AggregateKind::Conversation,
                    id: crate::ConversationId::from_bytes([89; 16]).to_string(),
                },
            );
            let journal = Journal {
                8: Some(issuer.verifier(&mut None)?),
                ..Default::default()
            };
            let result = StockExecution::ordinary(&executor)
                .execute_progress(&input, &journal)
                .await;
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
            assert!(
                output.journal_cut.is_none(),
                "unsupported receipts cannot certify closure"
            );
            assert_eq!(tool.0.load(Ordering::SeqCst), turns as usize);
            assert_eq!(provider.seen.load(Ordering::SeqCst), turns as usize + 1);
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
    let mut retained = projection;
    retained.context.messages = prepared.request().messages.clone();
    retained.accounting = Some(accounting);
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
