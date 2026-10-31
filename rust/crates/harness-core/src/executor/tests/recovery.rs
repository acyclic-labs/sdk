use super::*;

#[tokio::test]
async fn fixed_custom_context_uses_its_full_message_allowance() -> Result<()> {
    let journal = Journal::default();
    let (provider, _accounting_calls) =
        counted_model(65_536, None, [ScriptedTurn::Text("answer".into())], []);
    let source = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text("fixed instruction".into()),
        }],
        ..crate::context::Context::default()
    };
    let executor = StockExecutor::new(
        Model::new("synthetic".into(), "fixed-context".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
            "fixed",
            "1",
            Arc::new(source),
            crate::context::ContextPlacement::Prepend,
        )) as Arc<dyn crate::context::ContextStage>]),
        ToolRegistry::default(),
    )
    .with_limits(Limits {
        context_messages: 2,
        ..Limits::default()
    });
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("small input".into()),
        selected_context: None,
        max_steps: 1,
    };
    let output = executor.execute(input.clone(), &journal).await?;
    assert_eq!(executor.execute(input, &journal).await?, output);
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].messages.len(), 2);
    assert_eq!(
        requests[0].messages[0].content,
        ModelContent::Text("fixed instruction".into())
    );
    Ok(())
}

#[tokio::test]
async fn prepared_context_survives_recovery_before_model_dispatch() -> Result<()> {
    let journal = Journal::default();
    let source = Arc::new(ChangingContextSource(AtomicUsize::new(0)));
    let provider = Arc::new(recoverable_model(1));
    let executor = uncompacted_executor(
        Model::new("test".into(), "model".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
            "changing-source",
            "1",
            source.clone(),
            crate::context::ContextPlacement::Prepend,
        )) as Arc<dyn crate::context::ContextStage>]),
        ToolRegistry::default(),
    );
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("current".into()),
        selected_context: None,
        max_steps: 1,
    };
    StockExecution::ordinary(&executor)
        .ensure_started(&journal, &input)
        .await?;
    let original = StockExecution::ordinary(&executor)
        .prepared_response(&journal, &input, 0, &[], &mut DispatchPhase::new())
        .await?;
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 0);
    let reopened = executor.clone();
    let recovered = StockExecution::ordinary(&reopened)
        .prepared_response(&journal, &input, 0, &[], &mut DispatchPhase::new())
        .await?;
    assert_eq!(original.bytes(), recovered.bytes());
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert!(reopened.model_step(&journal, &input, 0, &[]).await.is_err());
    let events = reopened.model_step(&journal, &input, 0, &[]).await?;
    assert_eq!(events.len(), 72);
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(provider.reconciliations().len(), 1);
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
async fn operation_identity_rejects_changed_input() -> Result<()> {
    let model = Arc::new(scripted_model());
    let executor = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into())?,
        model,
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let journal = Journal::default();
    let operation_id = OperationId::from_bytes([9; 16]);
    let _ = executor
        .execute(
            TurnInput {
                operation_id,
                input: ModelContent::Text("first".into()),
                selected_context: None,
                max_steps: 1,
            },
            &journal,
        )
        .await;
    assert!(matches!(
        executor
            .execute(
                TurnInput {
                    operation_id,
                    input: ModelContent::Text("changed".into()),
                    selected_context: None,
                    max_steps: 1,
                },
                &journal,
            )
            .await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn identical_model_bytes_keep_distinct_dispatch_identities() -> Result<()> {
    let provider = Arc::new(
        ScriptedModelProvider::new((0..2).map(|_| {
            ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::Completed {
                metadata: Value::Null,
            })])
        }))
        .with_token_counter(fixture_token_count),
    );
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "identity".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let journal = Journal::default();
    let operations = [
        OperationId::from_bytes([31; 16]),
        OperationId::from_bytes([32; 16]),
    ];
    let input = |operation_id| TurnInput {
        operation_id,
        input: ModelContent::Text("same input".into()),
        selected_context: None,
        max_steps: 1,
    };
    for operation in operations {
        executor.execute(input(operation), &journal).await?;
    }
    executor.execute(input(operations[0]), &journal).await?;
    let dispatches = provider.dispatches();
    let requests = provider.requests();
    let captures = dispatches.iter().zip(&requests).collect::<Vec<_>>();
    assert_eq!(captures.len(), 2);
    assert_eq!(captures[0].0.operation_id, operations[0]);
    assert_eq!(captures[1].0.operation_id, operations[1]);
    assert_eq!(captures[0].0.step, 0);
    assert_eq!(captures[1].0.step, 0);
    assert_eq!(captures[0].0.request_digest, captures[1].0.request_digest);
    assert_eq!(
        crate::contract::canonical_json_bytes(captures[0].1)?,
        crate::contract::canonical_json_bytes(captures[1].1)?
    );
    assert!(provider.reconciliations().is_empty());
    Ok(())
}

#[tokio::test]
async fn native_result_reads_obey_effective_limits_and_attenuated_grants() -> Result<()> {
    let journal = Journal::default();
    let file = journal
        .stage(
            OperationId::new(),
            "media",
            &[1, 2, 3, 4],
            "image/png",
            None,
        )?
        .await?;
    let options = journal
        .stage(
            OperationId::new(),
            "options",
            b"{}",
            "application/json",
            None,
        )?
        .await?;
    let content = ModelContent::Part(ModelContentPart::ToolResult {
        call_id: "media".into(),
        name: "test.media".into(),
        content: crate::model::ToolResultContent::Parts {
            parts: vec![crate::model::ModelDataPart::File {
                file: file.clone(),
                policy: crate::model::FileProjectionPolicy::Native(Box::new(
                    crate::model::NativeMediaPolicy {
                        intent: crate::model::NativeMediaIntent::Image {
                            detail: crate::model::ImageDetail::Auto,
                        },
                        maximum_bytes: 4,
                        maximum_work: 4,
                        configuration: Some(crate::model::NativeConfigurationBinding {
                            source: crate::core::EventReference {
                                authority: crate::core::Authority {
                                    kind: crate::core::AggregateKind::Agent,
                                    id: "test-agent".into(),
                                },
                                revision: 1,
                            },
                            configuration: crate::core::ExtensionConfiguration {
                                extension: crate::core::ExtensionDependency {
                                    name: "test.media".into(),
                                    version: 1,
                                },
                                schema_digest: [1; 32],
                                content: options.clone(),
                            },
                            implementation_digest: [2; 32],
                        }),
                    },
                )),
            }],
        },
    });
    let denied = RuntimeScope::new(crate::Capabilities::default(), Limits::default())?;
    assert!(matches!(
        verify_model_contents_scoped(
            &journal,
            std::iter::once(&content),
            Limits::default(),
            Some(&denied),
            None,
            None,
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    let media_only = RuntimeScope::new(
        crate::Capabilities::new([file.read_capability(&mut None)?]),
        Limits::default(),
    )?;
    assert!(matches!(
        verify_model_contents_scoped(
            &journal,
            std::iter::once(&content),
            Limits::default(),
            Some(&media_only),
            None,
            None,
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    let allowed = RuntimeScope::new(
        crate::Capabilities::new([
            file.read_capability(&mut None)?,
            options.read_capability(&mut None)?,
        ]),
        Limits::default(),
    )?;
    assert!(matches!(
        verify_model_contents_scoped(
            &journal,
            std::iter::once(&content),
            Limits {
                file_bytes: 3,
                ..Limits::default()
            },
            Some(&allowed),
            None,
            None,
        )
        .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(journal.4.load(Ordering::SeqCst), 0);
    assert!(matches!(
        verify_model_contents_scoped(
            &journal,
            [
                ModelContent::Part(ModelContentPart::File {
                    file: file.clone(),
                    policy: crate::model::FileProjectionPolicy::Reference
                }),
                content
            ]
            .iter(),
            Limits::default(),
            Some(&allowed),
            None,
            None,
        )
        .await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(journal.4.load(Ordering::SeqCst), 0);
    let references = ModelContent::Parts(
        [file, options]
            .map(|file| ModelContentPart::File {
                file,
                policy: crate::model::FileProjectionPolicy::Reference,
            })
            .to_vec(),
    );
    verify_model_contents_scoped(
        &journal,
        std::iter::once(&references),
        Limits::default(),
        Some(&allowed),
        None,
        None,
    )
    .await?;
    assert_eq!(journal.4.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn direct_model_steps_reject_empty_and_partial_eof() -> Result<()> {
    for events in [
        vec![],
        vec![ScriptedEvent::Event(ModelEvent::Content {
            delta: "partial".into(),
        })],
    ] {
        let provider = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Events(events)]));
        let executor = uncompacted_executor(
            Model::new("test".into(), "model".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text("question".into()),
            selected_context: None,
            max_steps: 1,
        };
        assert!(
            matches!(executor.model_step(&Journal::default(), &input, 0, &[]).await,
            Err(Error::Indeterminate(id)) if id == input.operation_id)
        );
        assert_eq!(provider.requests().len(), 1);
        assert_eq!(provider.remaining(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn an_unowned_empty_model_attempt_is_not_reissued() -> Result<()> {
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Events(vec![ScriptedEvent::Pending {}])])
            .with_token_counter(fixture_token_count)
            .with_reconciliations([ScriptedReconciliation {
                orphan: false,
                observed: vec![],
                continuation: None,
            }]),
    );
    let executor = uncompacted_executor(
        Model::new("fixture".into(), "pending".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let journal = Journal::default();
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("start".into()),
        selected_context: None,
        max_steps: 1,
    };
    {
        let pending = executor.model_step(&journal, &input, 0, &[]);
        futures::pin_mut!(pending);
        assert!(futures::poll!(&mut pending).is_pending());
        assert_eq!(provider.dispatches().len(), 1);
    }
    let records = journal.0.lock().unwrap().clone();
    assert!(matches!(
        executor.model_step(&journal, &input, 0, &[]).await,
        Err(Error::Indeterminate(operation)) if operation == input.operation_id
    ));
    assert_eq!(provider.dispatches().len(), 1);
    assert_eq!(*journal.0.lock().unwrap(), records);
    assert_eq!(provider.reconciliations(), provider.dispatches());
    Ok(())
}

/// A stream cut and process loss leave the same retained incomplete prefix.
async fn unrecoverable_prefix(unsettled: bool) -> Result<()> {
    let prefix = vec![ModelEvent::Content {
        delta: "partial".into(),
    }];
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Events(vec![
            ScriptedEvent::Event(prefix[0].clone()),
            ScriptedEvent::Error {
                error: "stream lost".into(),
            },
        ])])
        .with_token_counter(fixture_token_count)
        .with_reconciliations([ScriptedReconciliation {
            orphan: false,
            observed: prefix,
            continuation: None,
        }]),
    );
    let executor = uncompacted_executor(
        Model::new("fixture".into(), "partial".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let journal = Journal::default();
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("start".into()),
        selected_context: None,
        max_steps: 1,
    };
    assert!(matches!(
        executor.execute(input.clone(), &journal).await,
        Err(Error::Storage(_))
    ));
    let records_before = journal.0.lock().unwrap().len();
    let original = super::super::unsettled_resume::enabled();
    super::super::unsettled_resume::set(unsettled);
    let result = executor.clone().execute(input, &journal).await;
    super::super::unsettled_resume::set(original);
    assert_eq!(
        provider.dispatches().len(),
        1,
        "incomplete response was redispatched"
    );
    assert_eq!(provider.reconciliations(), provider.dispatches());
    assert_eq!(
        journal.0.lock().unwrap().len(),
        records_before,
        "retained prefix changed"
    );
    match result {
        Err(
            _error @ Error::ModelUnavailable {
                failure: crate::ModelTransportFailure::Interrupted,
                ..
            },
        ) => {
            #[cfg(feature = "filesystem")]
            assert!(!_error.leaves_outcome_uncertain());
            Ok(())
        }
        Err(error) => Err(error),
        Ok(_) => panic!("incomplete response was treated as completed"),
    }
}

#[tokio::test]
async fn an_unrecoverable_partial_model_stream_fails_without_redispatch() -> Result<()> {
    unrecoverable_prefix(false).await
}

#[tokio::test]
async fn negative_control_retaining_an_unrecoverable_partial_stream_strands_it() {
    assert!(matches!(
        unrecoverable_prefix(true).await,
        Err(Error::Indeterminate(_))
    ));
}
