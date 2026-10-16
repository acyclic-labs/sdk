use super::*;
use crate::{
    OperationId, Result,
    conversation::Limits,
    model::{
        Model, ModelContent, ModelDispatch, ModelEvent, ModelMessage, ModelProvider, ModelRequest,
        ModelRole, PreparedModelRequest,
    },
};
use serde_json::json;

fn request() -> Result<(PreparedModelRequest, ModelDispatch)> {
    let request = PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new("scripted".into(), "test".into(), "1".into(), json!({}))?,
            messages: vec![
                ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text("instructions".into()),
                },
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("prompt".into()),
                },
            ],
            tools: vec![crate::tool::ToolDefinition {
                name: "read".into(),
                revision: "1".into(),
                description: "read a file".into(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
                projection_schema: crate::tool::json_projection_schema(json!({"type":"object"})),
            }],
            max_output_tokens: Some(64),
        },
        Limits::default(),
        None,
    )?;
    let dispatch = ModelDispatch {
        operation_id: OperationId::new(),
        step: 0,
        request_digest: request.request_digest(),
    };
    Ok((request, dispatch))
}

#[tokio::test]
async fn text_replay_records_exact_request_and_exhausts() -> Result<()> {
    let provider = ScriptedModelProvider::new([ScriptedTurn::Text("answer".into())]);
    let (request, dispatch) = request()?;
    let expected = request.request().clone();
    let count = provider.count_tokens(&request).await?;
    assert_eq!(count.request_digest, request.request_digest());
    assert_eq!(count.message_tokens.len(), 2);
    let events = provider
        .generate(request.clone(), dispatch)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        events.into_iter().collect::<Result<Vec<_>>>()?,
        vec![
            ModelEvent::Content {
                delta: "answer".into()
            },
            ModelEvent::Completed {
                metadata: serde_json::Value::Null
            },
        ]
    );
    assert_eq!(provider.requests(), vec![expected]);
    assert_eq!(provider.dispatches(), vec![dispatch]);
    assert_eq!(provider.remaining(), 0);
    let events = provider
        .generate(request, dispatch)
        .collect::<Vec<_>>()
        .await;
    assert!(
        matches!(events.as_slice(), [Err(crate::Error::Invalid(message))]
        if message == "scripted model exhausted")
    );
    assert_eq!(provider.requests().len(), 2);
    Ok(())
}

#[tokio::test]
async fn exact_events_preserve_tools_errors_and_missing_completion() -> Result<()> {
    let turns: Vec<ScriptedTurn> = serde_json::from_value(json!([
        [{"type":"event","kind":"reasoning","delta":"thinking"},
         {"type":"event","kind":"tool_call","call_id":"call","name":"read","arguments":{"path":"a"}},
         {"type":"event","kind":"completed","metadata":{"tokens":3}}],
        [{"type":"event","kind":"content","delta":"partial"},{"type":"error","error":"offline"}],
        []
    ]))
    .map_err(|e| crate::Error::Invalid(e.to_string()))?;
    let provider = ScriptedModelProvider::new(turns);
    let (request, dispatch) = request()?;
    let events = provider
        .generate(request.clone(), dispatch)
        .collect::<Vec<_>>()
        .await;
    let events = events.into_iter().collect::<Result<Vec<_>>>()?;
    assert!(matches!(events.as_slice(), [ModelEvent::Reasoning { .. },
        ModelEvent::ToolCall(crate::model::ToolCall { arguments, .. }), ModelEvent::Completed { metadata }]
        if arguments == &json!({"path":"a"}) && metadata == &json!({"tokens":3})));
    let events = provider
        .generate(request.clone(), dispatch)
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(events.as_slice(), [Ok(ModelEvent::Content { .. }),
        Err(crate::Error::Storage(message))] if message == "offline"));
    assert!(
        provider
            .generate(request, dispatch)
            .collect::<Vec<_>>()
            .await
            .is_empty()
    );
    assert_eq!(provider.remaining(), 0);
    Ok(())
}

#[tokio::test]
async fn mismatched_dispatch_does_not_consume_script() -> Result<()> {
    let provider = ScriptedModelProvider::new([ScriptedTurn::Text("answer".into())]);
    let (request, mut dispatch) = request()?;
    dispatch.request_digest = [0; 32];
    let events = provider
        .generate(request, dispatch)
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(events.as_slice(), [Err(crate::Error::Invalid(_))]));
    assert_eq!(provider.remaining(), 1);
    assert!(provider.requests().is_empty());
    Ok(())
}

#[test]
fn malformed_script_events_are_rejected() {
    for event in [
        json!({"type":"event","kind":"content","delta":"x","typo":true}),
        json!({"type":"error","error":"x","typo":true}),
        json!({"type":"event","kind":"typo"}),
        json!({"type":"pending","typo":true}),
    ] {
        assert!(serde_json::from_value::<ScriptedEvent>(event).is_err());
    }
}

#[tokio::test]
async fn pending_script_signals_recorded_dispatch_and_reconciles_after_drop() -> Result<()> {
    let prefix = ModelEvent::Content {
        delta: "partial".into(),
    };
    let turns: Vec<ScriptedTurn> = serde_json::from_value(json!([
        [{"type":"event","kind":"content","delta":"partial"},{"type":"pending"}]
    ]))
    .map_err(|error| crate::Error::Invalid(error.to_string()))?;
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let provider = std::sync::Arc::new_cyclic(|weak: &std::sync::Weak<ScriptedModelProvider>| {
        let weak = weak.clone();
        ScriptedModelProvider::new(turns)
            .with_reconciliations([ScriptedReconciliation {
                orphan: false,
                observed: vec![prefix.clone()],
                continuation: Some(vec![ModelEvent::Completed {
                    metadata: json!(null),
                }]),
            }])
            .with_dispatch_observer(move |dispatch| {
                // Reenter the observer accessors to verify the provider unlocked its state.
                assert_eq!(
                    weak.upgrade().map(|provider| provider.dispatches()),
                    Some(vec![dispatch])
                );
                assert!(sent.send(dispatch).is_ok());
            })
    });
    let (request, dispatch) = request()?;
    let mut stream = provider.generate(request, dispatch);
    assert_eq!(received.try_recv(), Ok(dispatch));
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(provider.remaining(), 0);
    assert_eq!(stream.next().await.transpose()?, Some(prefix.clone()));
    assert!(futures::poll!(stream.next()).is_pending());
    drop(stream);
    assert!(matches!(provider.reconcile(dispatch, &[prefix]).await?,
        Some(events) if matches!(events.as_slice(), [ModelEvent::Completed { .. }])));
    assert_eq!(provider.dispatches(), vec![dispatch]);
    assert_eq!(provider.reconciliations(), vec![dispatch]);
    assert_eq!(
        serde_json::to_value(ScriptedEvent::Pending {})
            .map_err(|error| crate::Error::Invalid(error.to_string()))?,
        json!({"type":"pending"})
    );
    Ok(())
}

#[tokio::test]
async fn invalid_or_exhausted_dispatch_does_not_signal() -> Result<()> {
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let provider = ScriptedModelProvider::new([ScriptedTurn::Text("answer".into())])
        .with_dispatch_observer(move |dispatch| {
            assert!(sent.send(dispatch).is_ok());
        });
    let (request, dispatch) = request()?;
    let invalid = ModelDispatch {
        request_digest: [0; 32],
        ..dispatch
    };
    assert!(matches!(
        provider.generate(request.clone(), invalid).next().await,
        Some(Err(_))
    ));
    assert!(matches!(
        received.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    provider
        .generate(request.clone(), dispatch)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(received.try_recv(), Ok(dispatch));
    assert!(matches!(
        provider.generate(request, dispatch).next().await,
        Some(Err(_))
    ));
    assert!(matches!(
        received.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    Ok(())
}

#[tokio::test]
async fn orphan_reconciliation_checks_prefix_and_records_dispatch_without_generation() -> Result<()>
{
    let prefix = vec![ModelEvent::Content {
        delta: "partial-".into(),
    }];
    let continuation = vec![
        ModelEvent::Content {
            delta: "rest".into(),
        },
        ModelEvent::Completed {
            metadata: json!({"finish":"stop"}),
        },
    ];
    let provider = ScriptedModelProvider::new([]).with_reconciliations([ScriptedReconciliation {
        orphan: true,
        observed: prefix.clone(),
        continuation: Some(continuation.clone()),
    }]);
    let (_, dispatch) = request()?;
    assert!(matches!(provider.reconcile(dispatch, &[]).await,
        Err(crate::Error::Conflict(message)) if message == "scripted reconciliation prefix mismatch"));
    assert_eq!(provider.recoveries().len(), 1);
    assert_eq!(
        provider.reconcile(dispatch, &prefix).await?,
        Some(continuation)
    );
    assert_eq!(provider.requests().len(), 0);
    assert_eq!(provider.reconciliations(), vec![dispatch, dispatch]);
    assert_eq!(provider.reconciliations().len(), 2);
    assert_eq!(provider.recoveries().len(), 0);
    assert!(matches!(provider.reconcile(dispatch, &prefix).await,
        Err(crate::Error::Conflict(message)) if message == "scripted reconciliation dispatch was not generated"));
    Ok(())
}

#[tokio::test]
async fn reconciliation_preserves_unrecoverable_and_incomplete_replies() -> Result<()> {
    let provider = ScriptedModelProvider::new([ScriptedTurn::Text("answer".into())])
        .with_reconciliations([
            ScriptedReconciliation {
                orphan: false,
                observed: vec![],
                continuation: None,
            },
            ScriptedReconciliation {
                orphan: false,
                observed: vec![],
                continuation: Some(vec![]),
            },
        ]);
    let (request, dispatch) = request()?;
    provider
        .generate(request, dispatch)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(provider.reconcile(dispatch, &[]).await?, None);
    assert_eq!(provider.reconcile(dispatch, &[]).await?, Some(vec![]));
    assert_eq!(provider.reconciliations().len(), 2);
    assert_eq!(provider.recoveries().len(), 0);
    assert!(matches!(provider.reconcile(dispatch, &[]).await,
        Err(crate::Error::Invalid(message)) if message == "scripted reconciliation exhausted"));
    Ok(())
}

#[tokio::test]
async fn reconciliation_rejects_unknown_operations_and_steps_without_consuming_replies()
-> Result<()> {
    let provider = ScriptedModelProvider::new([ScriptedTurn::Text("answer".into())])
        .with_reconciliations([ScriptedReconciliation {
            orphan: false,
            observed: vec![],
            continuation: None,
        }]);
    let (request, dispatch) = request()?;
    provider
        .generate(request, dispatch)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    for unknown in [
        ModelDispatch {
            operation_id: OperationId::new(),
            ..dispatch
        },
        ModelDispatch {
            step: dispatch.step + 1,
            ..dispatch
        },
    ] {
        assert!(matches!(provider.reconcile(unknown, &[]).await,
            Err(crate::Error::Conflict(message)) if message == "scripted reconciliation dispatch was not generated"));
        assert_eq!(provider.recoveries().len(), 1);
    }
    assert_eq!(provider.reconcile(dispatch, &[]).await?, None);
    assert!(provider.recoveries().is_empty());
    assert_eq!(provider.dispatches(), vec![dispatch]);
    Ok(())
}

#[test]
fn reconciliation_scripts_reject_unknown_fields_and_malformed_events() {
    for reply in [
        json!({"observed":[],"continuation":null,"typo":true}),
        json!({"observed":[{"type":"event","kind":"typo"}],"continuation":[]}),
        json!({"observed":[],"continuation":[{"type":"error","error":"not a model event"}]}),
    ] {
        assert!(serde_json::from_value::<ScriptedReconciliation>(reply).is_err());
    }
}

#[test]
fn capacity_and_token_accounting_are_configurable() -> Result<()> {
    fn tokens(request: &PreparedModelRequest) -> Result<crate::context::ModelTokenCount> {
        Ok(crate::context::ModelTokenCount::from_upper_bounds(
            request.request_digest(),
            7,
            vec![3; request.request().messages.len()],
        ))
    }
    let capacity = crate::context::ModelContextCapacity {
        context_tokens: 512,
        output_tokens: 64,
    };
    let provider = ScriptedModelProvider::new([])
        .with_context_capacity(capacity)
        .with_token_counter(tokens);
    let (request, _) = request()?;
    assert_eq!(
        provider.context_capacity(&request.request().model)?,
        capacity
    );
    let count = futures::executor::block_on(provider.count_tokens(&request))?;
    assert_eq!(count.request_digest, request.request_digest());
    assert_eq!(count.fixed_tokens, 7);
    assert_eq!(count.message_tokens, vec![3, 3]);
    assert_eq!(provider.requests().len(), 0);
    Ok(())
}

#[test]
fn tagged_response_errors_name_the_invalid_event() {
    for (script, expected) in [
        (
            r#"[{"type":"unsupported"}]"#,
            "unknown variant `unsupported`",
        ),
        (
            r#"[{"kind":"content","delta":"x"}]"#,
            "missing field `type`",
        ),
        (
            r#"[{"type":"event","kind":"content","delat":"x"}]"#,
            "delat",
        ),
    ] {
        let error = serde_json::from_str::<ScriptedTurn>(script)
            .err()
            .map(|e| e.to_string());
        assert!(
            error.as_ref().is_some_and(|error| error.contains(expected)),
            "{error:?}"
        );
    }
}

#[test]
fn tagged_response_round_trips_without_a_wrapper_object() -> Result<()> {
    let event = ScriptedEvent::Event(ModelEvent::Content {
        delta: "answer".into(),
    });
    let value = serde_json::to_value(&event).map_err(|e| crate::Error::Invalid(e.to_string()))?;
    assert_eq!(
        value,
        json!({"type":"event","kind":"content","delta":"answer"})
    );
    let turn: ScriptedTurn =
        serde_json::from_value(json!([value])).map_err(|e| crate::Error::Invalid(e.to_string()))?;
    assert!(matches!(turn, ScriptedTurn::Events(events) if events.len() == 1));
    Ok(())
}

#[tokio::test]
async fn reconciliation_keeps_the_original_request_binding() -> Result<()> {
    let provider = ScriptedModelProvider::new([ScriptedTurn::Text("answer".into())])
        .with_reconciliations([ScriptedReconciliation {
            orphan: false,
            observed: vec![],
            continuation: None,
        }]);
    let (request, dispatch) = request()?;
    provider
        .generate(request, dispatch)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let changed = ModelDispatch {
        request_digest: [0; 32],
        ..dispatch
    };
    assert!(matches!(provider.reconcile(changed, &[]).await,
        Err(crate::Error::Conflict(message)) if message == "scripted reconciliation dispatch mismatch"));
    assert_eq!(provider.recoveries().len(), 1);
    assert_eq!(provider.reconcile(dispatch, &[]).await?, None);
    Ok(())
}

#[test]
fn poisoned_state_remains_observable() {
    let provider = ScriptedModelProvider::new([ScriptedTurn::Text("answer".into())]);
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = provider.lock();
        std::panic::resume_unwind(Box::new(()));
    }));
    assert!(poisoned.is_err());
    assert_eq!(provider.remaining(), 1);
    assert!(provider.requests().is_empty());
    assert!(provider.dispatches().is_empty());
    assert_eq!(provider.requests().len(), 0);
}

#[test]
fn token_counter_can_capture_configuration_without_changing_the_request() -> Result<()> {
    let tokens = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(7));
    let captured = tokens.clone();
    let provider = ScriptedModelProvider::new([]).with_token_counter(move |request| {
        Ok(crate::context::ModelTokenCount::from_upper_bounds(
            request.request_digest(),
            captured.load(std::sync::atomic::Ordering::SeqCst),
            vec![0; request.request().messages.len()],
        ))
    });
    let (request, _) = request()?;
    assert_eq!(
        futures::executor::block_on(provider.count_tokens(&request))?.fixed_tokens,
        7
    );
    tokens.store(11, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        futures::executor::block_on(provider.count_tokens(&request))?.fixed_tokens,
        11
    );
    Ok(())
}

#[tokio::test]
async fn unavailable_accounting_still_records_and_replays_generation() -> Result<()> {
    let provider =
        ScriptedModelProvider::new([ScriptedTurn::Text("answer".into())]).without_accounting();
    let (request, dispatch) = request()?;
    assert!(
        matches!(provider.context_capacity(&request.request().model),
        Err(crate::Error::Unsupported(message)) if message == "selected model context capacity is unavailable")
    );
    assert!(matches!(provider.count_tokens(&request).await,
        Err(crate::Error::Unsupported(message)) if message == "selected model token counting is unavailable"));
    let events = provider
        .generate(request, dispatch)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    assert!(
        matches!(events.as_slice(), [ModelEvent::Content { delta }, ModelEvent::Completed { .. }] if delta == "answer")
    );
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(provider.remaining(), 0);
    Ok(())
}
