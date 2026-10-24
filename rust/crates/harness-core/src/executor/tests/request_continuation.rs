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
            None,
        )?;
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text("input".into()),
            selected_context: None,
            max_steps: turns + 1,
        };
        let issuer = crate::core::AuthorityIssuer::new(
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
