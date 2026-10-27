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
    for turns in [1, 10, 100, 1_000] {
        let provider = Arc::new(ContinuingModel {
            turns,
            warm: true,
            seen: AtomicUsize::new(0),
        });
        let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
        let executor = uncompacted_executor(
            Model::new(
                "test".into(),
                "continuation".into(),
                "1".into(),
                Value::Null,
            )?,
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
            operation_id: OperationId::new(),
            input: ModelContent::Text("input".into()),
            selected_context: None,
            max_steps: turns + 1,
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
        assert_eq!(tool.0.load(Ordering::SeqCst), turns as usize);
        assert_eq!(provider.seen.load(Ordering::SeqCst), turns as usize + 1);
    }
    Ok(())
}

#[tokio::test]
async fn main_context_prepared_resumes_before_model_started_with_one_inline_schema() -> Result<()> {
    let provider = Arc::new(ContinuingModel {
        turns: 0,
        warm: false,
        seen: AtomicUsize::new(0),
    });
    let executor = uncompacted_executor(
        Model::new(
            "test".into(),
            "continuation".into(),
            "1".into(),
            Value::Null,
        )?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let journal = Journal::default();
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("input".into()),
        selected_context: None,
        max_steps: 1,
    };
    let execution = StockExecution::ordinary(&executor);
    execution.ensure_started(&journal, &input).await?;
    let (projection, _, prepared) = execution
        .capture_response(&journal, &input, 0, &[], 1)
        .await?;
    let reference = journal
        .0
        .lock()
        .unwrap()
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ContextPrepared { projection, .. } => Some(projection.clone()),
            _ => None,
        })
        .unwrap();
    let bytes = journal.load(&reference, None)?.await?;
    let main = serde_json::json!({
        "context": { "messages": prepared.request().messages, "metadata": projection.context.metadata, "current_input_index": projection.context.current_input_index },
        "canonical": projection.canonical, "checkpoint": projection.checkpoint,
    });
    assert_eq!(
        &*bytes,
        crate::contract::canonical_json_bytes(&main)?,
        "durable projection changed main's schema"
    );
    assert!(
        !journal
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ModelStarted { .. }))
    );
    let resumed = executor.execute(input, &journal).await?;
    assert_eq!(resumed.text, "done");
    assert_eq!(provider.seen.load(Ordering::SeqCst), 1);
    assert_eq!(
        journal
            .0
            .lock()
            .unwrap()
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
        warm: true,
        seen: AtomicUsize::new(0),
    });
    let executor = uncompacted_executor(
        Model::new(
            "test".into(),
            "continuation".into(),
            "1".into(),
            Value::Null,
        )?,
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
        operation_id: OperationId::new(),
        input: ModelContent::Text("input".into()),
        selected_context: None,
        max_steps: 11,
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
