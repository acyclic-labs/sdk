use super::*;

#[tokio::test]
async fn fixed_custom_context_uses_its_full_message_allowance() -> Result<()> {
    let journal = Journal::default();
    let provider = Arc::new(CountedModel {
        capacity: 65_536,
        ..CountedModel::default()
    });
    let source = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text("fixed instruction".into()),
        }],
        ..crate::context::Context::default()
    };
    let executor = StockExecutor::new(
        Model::new(
            "synthetic".into(),
            "fixed-context".into(),
            "1".into(),
            json!({}),
        )?,
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
    let requests = provider.requests.lock().unwrap();
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
    let provider = Arc::new(ScriptedModel::recoverable());
    let executor = uncompacted_executor(
        Model::new("test".into(), "model".into(), "1".into(), json!({}))?,
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
        .prepared_response(&journal, &input, 0, &[])
        .await?;
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    let reopened = executor.clone();
    let recovered = StockExecution::ordinary(&reopened)
        .prepared_response(&journal, &input, 0, &[])
        .await?;
    assert_eq!(original.bytes(), recovered.bytes());
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert!(reopened.model_step(&journal, &input, 0, &[]).await.is_err());
    let events = reopened.model_step(&journal, &input, 0, &[]).await?;
    assert_eq!(events.len(), 72);
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.reconciliations.load(Ordering::SeqCst), 1);
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
    let model = Arc::new(ScriptedModel::default());
    let executor = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
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
async fn interrupted_model_stream_reconciles_without_redispatch() -> Result<()> {
    struct DispatchObserver {
        claims: AtomicUsize,
        owned: std::sync::atomic::AtomicBool,
    }
    impl crate::runtime::TaskSpawner for DispatchObserver {
        fn policy_identity(&self) -> Option<ComponentIdentity> {
            None
        }
        fn admit<'a>(
            &'a self,
            _: crate::runtime::TaskAdmissionRecord,
        ) -> BoxFuture<'a, Result<crate::Admission<TaskId>>> {
            Box::pin(async { Err(Error::Unsupported("test observer".into())) })
        }
    }
    impl DurableTaskHost for DispatchObserver {
        fn verify_execution_owner<'a>(
            &'a self,
            _: TaskId,
            _: crate::scheduler::LeaseFence,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                if self.owned.load(Ordering::SeqCst) {
                    Ok(())
                } else {
                    Err(Error::Conflict("stale test owner".into()))
                }
            })
        }
        fn claim_model_dispatch<'a>(
            &'a self,
            _: TaskId,
            _: OperationId,
            _: u32,
            _: [u8; 32],
            _: crate::scheduler::LeaseFence,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.claims.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    impl crate::runtime::TaskStateProvider for DispatchObserver {
        fn outcome<'a>(&'a self, _: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
            Box::pin(async { Err(Error::Unsupported("test observer".into())) })
        }
        fn cancel<'a>(&'a self, _: TaskId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Err(Error::Unsupported("test observer".into())) })
        }

        fn policy_identity(&self) -> Option<ComponentIdentity> {
            None
        }

        fn observe_admission<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<crate::runtime::TaskAdmissionRecord>> {
            Box::pin(async {
                Err(Error::Unsupported(
                    "full durable admission observation is not bound".into(),
                ))
            })
        }

        fn resume_scope<'a>(
            &'a self,
            _task_id: TaskId,
            _operation_id: OperationId,
        ) -> BoxFuture<'a, Result<RuntimeScope>> {
            Box::pin(async {
                Err(Error::Unsupported(
                    "durable task context authentication is not bound".into(),
                ))
            })
        }
    }
    let host = Arc::new(DispatchObserver {
        claims: AtomicUsize::new(0),
        owned: std::sync::atomic::AtomicBool::new(true),
    });
    let model = Arc::new(ScriptedModel::recoverable());
    let executor = uncompacted_executor(
        Model::new(
            "example".into(),
            "recoverable".into(),
            "1".into(),
            Value::Null,
        )?,
        model.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_durable_task(
        host.clone(),
        TaskId::from_bytes([11; 16]),
        crate::scheduler::LeaseFence {
            reservation_id: "lease".into(),
            placement: "worker".into(),
        },
    );
    let journal = Journal::default();
    let input = TurnInput {
        operation_id: OperationId::from_bytes([10; 16]),
        input: ModelContent::Text("hello".into()),
        selected_context: None,
        max_steps: 1,
    };

    assert!(matches!(
        executor.execute(input.clone(), &journal).await,
        Err(Error::Storage(_))
    ));
    let recovered = executor.execute(input.clone(), &journal).await?;
    let replayed = executor.execute(input.clone(), &journal).await?;

    assert_eq!(recovered.text, format!("{}restored", "partial-".repeat(70)));
    assert_eq!(replayed, recovered);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(model.reconciliations.load(Ordering::SeqCst), 1);
    assert_eq!(host.claims.load(Ordering::SeqCst), 1);
    assert_eq!(
        journal
            .replay(input.operation_id, 0, 64, None)?
            .await?
            .len(),
        64
    );
    assert_eq!(
        journal
            .replay(input.operation_id, 64, 64, None)?
            .await?
            .len(),
        11
    );
    assert!(
        journal
            .replay(input.operation_id, 75, 64, None)?
            .await?
            .is_empty()
    );
    host.owned.store(false, Ordering::SeqCst);
    let records_before = journal.0.lock().unwrap().len();
    let artifacts_before = journal.1.lock().unwrap().len();
    assert!(matches!(
        executor
            .clone()
            .with_max_output_tokens(1024)?
            .summarize(
                &journal,
                OperationId::new(),
                crate::context::Context {
                    messages: vec![ModelMessage {
                        role: ModelRole::User,
                        content: ModelContent::Text("retained source".into()),
                    }],
                    metadata: Default::default(),
                    current_input_index: None,
                },
                ModelContent::Text("summarize".into()),
            )
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(journal.0.lock().unwrap().len(), records_before);
    assert_eq!(journal.1.lock().unwrap().len(), artifacts_before);
    assert!(matches!(
        executor.execute(input, &journal).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(model.reconciliations.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn identical_model_bytes_keep_distinct_dispatch_identities() -> Result<()> {
    #[derive(Default)]
    struct Provider(Mutex<Vec<(crate::model::ModelDispatch, Vec<u8>)>>);
    impl ModelProvider for Provider {
        fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
            Ok(crate::context::ModelContextCapacity {
                context_tokens: 131_072,
                output_tokens: 4_096,
            })
        }

        fn count_tokens(
            &self,
            request: &crate::model::PreparedModelRequest,
        ) -> Result<crate::context::ModelTokenCount> {
            fixture_token_count(request)
        }

        fn generate<'a>(
            &'a self,
            request: crate::model::PreparedModelRequest,
            dispatch: crate::model::ModelDispatch,
        ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
            Box::pin(stream::once(async move {
                assert_eq!(dispatch.request_digest, request.request_digest());
                self.0
                    .lock()
                    .unwrap()
                    .push((dispatch, request.bytes().to_vec()));
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                })
            }))
        }
        fn reconcile<'a>(
            &'a self,
            _dispatch: crate::model::ModelDispatch,
            _observed: &'a [crate::model::ModelEvent],
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Err(Error::Unsupported("completed attempts must replay".into())) })
        }
    }
    let provider = Arc::new(Provider::default());
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "identity".into(), "1".into(), Value::Null)?,
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
    let captures = provider.0.lock().unwrap();
    assert_eq!(captures.len(), 2);
    assert_eq!(captures[0].0.operation_id, operations[0]);
    assert_eq!(captures[1].0.operation_id, operations[1]);
    assert_eq!(captures[0].0.step, 0);
    assert_eq!(captures[1].0.step, 0);
    assert_eq!(captures[0].0.request_digest, captures[1].0.request_digest);
    assert_eq!(captures[0].1, captures[1].1);
    Ok(())
}

#[tokio::test]
async fn native_result_reads_obey_effective_limits_and_attenuated_grants() -> Result<()> {
    let journal = Journal::default();
    let file = journal
        .stage(
            OperationId::new(),
            "media".into(),
            vec![1, 2, 3, 4],
            "image/png",
        )
        .await?;
    let options = journal
        .stage(
            OperationId::new(),
            "options".into(),
            b"{}".to_vec(),
            "application/json",
        )
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
            None
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    let media_only = RuntimeScope::new(
        crate::Capabilities::new([file.read_capability()?]),
        Limits::default(),
    )?;
    assert!(matches!(
        verify_model_contents_scoped(
            &journal,
            std::iter::once(&content),
            Limits::default(),
            Some(&media_only),
            None
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    let allowed = RuntimeScope::new(
        crate::Capabilities::new([file.read_capability()?, options.read_capability()?]),
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
            None
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
            None
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
    )
    .await?;
    assert_eq!(journal.4.load(Ordering::SeqCst), 2);
    Ok(())
}
