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
        context_messages: std::num::NonZeroUsize::new(2),
        ..Limits::default()
    });
    let input = TurnInput {
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("small input".into()),
        selected_context: None,
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
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("current".into()),
        selected_context: None,
    };
    StockExecution::ordinary(&executor)
        .ensure_started(&journal, &input)
        .await?;
    let original = StockExecution::ordinary(&executor)
        .prepared_response(&journal, &input, 0, &[], None)
        .await?;
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 0);
    let reopened = executor.clone();
    let recovered = StockExecution::ordinary(&reopened)
        .prepared_response(&journal, &input, 0, &[], None)
        .await?;
    assert_eq!(original.bytes(), recovered.bytes());
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert!(reopened.model_step(&journal, &input, 0, &[]).await.is_err());
    let events = reopened.model_step(&journal, &input, 0, &[]).await?;
    assert_eq!(events.len(), 72);
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(provider.reconciliations().len(), 1);
    let completion = journal
        .0
        .lock()
        .unwrap()
        .iter()
        .find(|record| {
            matches!(
                record.event,
                ExecutionEvent::Model { usage: Some(_), .. }
                    | ExecutionEvent::ModelInline { usage: Some(_), .. }
            )
        })
        .unwrap()
        .clone();
    let (_, usage) = super::super::model_usage::project_completion(
        &journal,
        input.operation_id,
        "unbound",
        &completion,
    )
    .await?;
    let usage = usage.expect("recovered completion has its original usage artifact");
    assert_eq!(
        (usage.started_at, usage.completed_at, usage.latency_ns),
        (None, None, None),
        "recovery invented a new call interval"
    );
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
                max_steps: None,
                operation_id,
                input: ModelContent::Text("first".into()),
                selected_context: None,
            },
            &journal,
        )
        .await;
    assert!(matches!(
        executor
            .execute(
                TurnInput {
                    max_steps: None,
                    operation_id,
                    input: ModelContent::Text("changed".into()),
                    selected_context: None,
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
        max_steps: None,
        operation_id,
        input: ModelContent::Text("same input".into()),
        selected_context: None,
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
                file_bytes: Some(3),
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
            max_steps: None,
            operation_id: OperationId::new(),
            input: ModelContent::Text("question".into()),
            selected_context: None,
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

/// A process lost before the provider's first event leaves a started attempt
/// with nothing observed. Unreconcilable, it settles like a lost prefix.
async fn unreconciled_empty_attempt(unsettled: bool) -> Result<Vec<ModelEvent>> {
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
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("start".into()),
        selected_context: None,
    };
    {
        let pending = executor.model_step(&journal, &input, 0, &[]);
        let mut pending = core::pin::pin!(pending);
        assert!(futures::poll!(&mut pending).is_pending());
        assert_eq!(provider.dispatches().len(), 1);
    }
    let records = journal.0.lock().unwrap().clone();
    let original = super::super::unsettled_resume::enabled();
    super::super::unsettled_resume::set(unsettled);
    let result = executor.model_step(&journal, &input, 0, &[]).await;
    super::super::unsettled_resume::set(original);
    assert_eq!(
        provider.dispatches().len(),
        1,
        "uncertain request was reissued"
    );
    assert_eq!(*journal.0.lock().unwrap(), records);
    assert_eq!(provider.reconciliations(), provider.dispatches());
    result
}

#[tokio::test]
async fn an_unreconciled_empty_model_attempt_fails_without_redispatch() -> Result<()> {
    match unreconciled_empty_attempt(false).await {
        // Nothing recorded a cause, and nothing observed a transport failure.
        Err(error @ Error::ModelInterrupted(_)) => {
            assert!(!error.leaves_outcome_uncertain());
            assert!(!error.is_model_failure(), "{error}");
            Ok(())
        }
        Err(error) => Err(error),
        Ok(_) => panic!("an empty attempt was treated as completed"),
    }
}

#[tokio::test]
async fn negative_control_retaining_an_unreconciled_empty_attempt_strands_it() {
    assert!(matches!(
        unreconciled_empty_attempt(true).await,
        Err(Error::Indeterminate(_))
    ));
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
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("start".into()),
        selected_context: None,
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
        // The later delivery names the stream's own failure, not a provider
        // transport failure that never happened.
        Err(error @ Error::ModelInterrupted(_)) => {
            assert!(!error.leaves_outcome_uncertain());
            assert!(
                error
                    .to_string()
                    .ends_with(": storage failure: stream lost"),
                "{error}"
            );
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

/// Fails the first commit of a received model event, as a lost journal write.
struct LosesFirstModelCommit(Journal, std::sync::atomic::AtomicBool);

impl ExecutionJournal for LosesFirstModelCommit {
    fn acquire_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<crate::executor::ExecutionAttempt<'a>> {
        self.0.acquire_execution(operation, io)
    }
    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        self.0.replay(operation, after, maximum, io)
    }
    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if matches!(event, ExecutionEvent::Model { .. }) && !self.1.swap(true, Ordering::SeqCst) {
            return Err(Error::Storage("journal write lost".into()));
        }
        self.0.append(operation, key, event, io)
    }
    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        tail: u64,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        self.0.append_if_tail(operation, tail, key, event, io)
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media: &'static str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        self.0.stage(operation, key, bytes, media, io)
    }
    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.0.load(reference, io)
    }
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.0.open_interaction(id, interaction, io)
    }
    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        self.0.interaction_outcome(id, io)
    }
}

/// A local journal failure while recording a received event is the attempt's
/// cause. A later delivery its provider cannot resume reports that cause.
#[tokio::test]
async fn a_lost_journal_write_is_the_cause_a_later_delivery_reports() -> Result<()> {
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Events(vec![ScriptedEvent::Event(
            ModelEvent::Content {
                delta: "partial".into(),
            },
        )])])
        .with_token_counter(fixture_token_count)
        .with_reconciliations([ScriptedReconciliation {
            orphan: false,
            observed: vec![],
            continuation: None,
        }]),
    );
    let executor = uncompacted_executor(
        Model::new("fixture".into(), "lost-write".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let journal = LosesFirstModelCommit(Journal::default(), Default::default());
    let input = TurnInput {
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("start".into()),
        selected_context: None,
    };
    let first = executor.execute(input.clone(), &journal).await;
    assert!(matches!(first, Err(Error::Storage(_))), "{first:?}");
    let result = executor.clone().execute(input, &journal).await;
    assert_eq!(
        provider.dispatches().len(),
        1,
        "lost write was redispatched"
    );
    match result {
        Err(error @ Error::ModelInterrupted(_)) => {
            assert!(
                error
                    .to_string()
                    .ends_with(": storage failure: journal write lost"),
                "{error}"
            );
            Ok(())
        }
        Err(error) => Err(error),
        Ok(_) => panic!("an incomplete response was treated as completed"),
    }
}
