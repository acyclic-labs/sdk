use super::*;

#[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one actual pipeline fixture checks forwarding and both exact placement boundaries"
    )]
    async fn admitted_limits_reach_sources_and_stages_and_bound_message_joins() -> Result<()> {
        let input = ContextInput {
            input: ModelContent::Text("current".into()),
            selected_context: None,
            step: 0,
            prior_messages: Vec::new(),
        };
        let limits = Limits {
            context_messages: 2,
            render_bytes: 2_048,
            ..payload_limits()
        };
        let source = Arc::new(LimitSpy(std::sync::Mutex::new(Vec::new())));
        let stage = Arc::new(LimitSpy(std::sync::Mutex::new(Vec::new())));
        let pipeline = ContextPipeline::new([
            Arc::new(SourceStage::new(
                "source",
                "1",
                source.clone(),
                ContextPlacement::Prepend,
            )) as Arc<dyn ContextStage>,
            stage.clone(),
        ]);
        let projected = pipeline.run(&input, limits, None).await?;
        assert_eq!(projected.current_input_index, Some(1));
        assert_eq!(
            *source
                .0
                .lock().unwrap(),
            vec![limits]
        );
        assert_eq!(
            *stage
                .0
                .lock().unwrap(),
            vec![limits]
        );
        let base = ContextPipeline::base_context(&input, limits, None)?;
        let contribution = vec![ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text("source".into()),
        }];
        for placement in [ContextPlacement::Prepend, ContextPlacement::Append] {
            let joined =
                selection::place_messages_with_io(base.clone(), contribution.clone(), placement, limits, None)?;
            assert_eq!(
                joined.current_input_index,
                Some(u32::from(placement == ContextPlacement::Prepend))
            );
            let exact = crate::contract::canonical_json_bytes(&joined)?.len() as u64;
            assert_eq!(
                selection::place_messages_with_io(
                    base.clone(),
                    contribution.clone(),
                    placement,
                    Limits {
                        render_bytes: exact,
                        ..limits
                    },
                    None,
                )?,
                joined
            );
            for rejected in [
                Limits {
                    context_messages: 1,
                    ..limits
                },
                Limits {
                    render_bytes: exact - 1,
                    ..limits
                },
            ] {
                assert!(matches!(
                    selection::place_messages_with_io(
                        base.clone(),
                        contribution.clone(),
                        placement,
                        rejected,
                        None,
                    ),
                    Err(crate::Error::Invalid(_))
                ));
            }
        }
        let oversized = Context {
            messages: vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("x".repeat(2_048)),
            }],
            ..Context::default()
        };
        assert!(matches!(
            oversized.load(&input, limits, None)?.await,
            Err(crate::Error::Invalid(_))
        ));
        let too_many = Context {
            messages: projected.messages,
            ..Context::default()
        };
        assert!(matches!(
            too_many
                .load(
                    &input,
                    Limits {
                        context_messages: 1,
                        ..limits
                    }
                , None)?
                .await,
            Err(crate::Error::Invalid(_))
        ));
        let file = message(ModelRole::System, "instruction")?
            .content
            .file_refs()
            .into_iter()
            .next()
            .cloned()
            .ok_or_else(|| crate::Error::Invalid("fixture instruction is missing".into()))?;
        let discovered = DiscoveredContext {
            instructions: vec![file.clone(), file.clone()],
            skills: Vec::new(),
        };
        assert!(matches!(
            discovered
                .load(
                    &input,
                    Limits {
                        context_messages: 1,
                        ..limits
                    }
                , None)?
                .await,
            Err(crate::Error::Invalid(_))
        ));
        assert!(matches!(
            discovered
                .load(
                    &input,
                    Limits {
                        file_bytes: 1,
                        ..limits
                    }
                , None)?
                .await,
            Err(crate::Error::Invalid(_))
        ));
        Ok(())
    }
#[tokio::test]
    async fn selection_stage_narrows_limits_before_source_verification_and_rendering() -> Result<()>
    {
        use std::sync::atomic::Ordering;
        let payloads = Arc::new(TestPayloadStore::new()?);
        let verifier = Arc::new(CountingVerifier {
            count: std::sync::atomic::AtomicUsize::new(0),
            payloads,
        });
        let renderer = Arc::new(LimitSpy(std::sync::Mutex::new(Vec::new())));
        let file = message(ModelRole::System, "0123456789")?
            .content
            .file_refs()
            .into_iter()
            .next()
            .cloned()
            .ok_or_else(|| crate::Error::Invalid("fixture source is missing".into()))?;
        let pinned = Limits {
            render_bytes: 2_048,
            ..payload_limits()
        };
        let stage = SelectionStage::new(
            "selection".into(),
            ContextSelection {
                source: ContextSourceValue::File { file: file.clone() },
                extent: ContextExtent::Whole,
                representation: ContextRepresentation::Full,
            },
            renderer.clone(),
            verifier.clone(),
            ContextRenderMode::Prompt,
            ContextPlacement::Prepend,
            pinned,
        )?;
        let input = ContextInput {
            input: ModelContent::Text("current".into()),
            selected_context: None,
            step: 0,
            prior_messages: Vec::new(),
        };
        let admitted = Limits {
            file_bytes: 9,
            render_bytes: 4_096,
            context_messages: 2,
            ..payload_limits()
        };
        let base = ContextPipeline::base_context(&input, admitted, None)?;
        assert!(matches!(
            stage.apply(&input, base.clone(), admitted, None)?.await,
            Err(crate::Error::Invalid(_))
        ));
        assert_eq!(verifier.count.load(Ordering::SeqCst), 0);
        assert!(
            renderer
                .0
                .lock().unwrap()
                .is_empty()
        );
        let admitted = Limits {
            file_bytes: 10,
            ..admitted
        };
        let projected = stage.apply(&input, base, admitted, None)?.await?;
        assert_eq!(projected.current_input_index, Some(1));
        assert_eq!(verifier.count.load(Ordering::SeqCst), 1);
        assert_eq!(
            *renderer
                .0
                .lock().unwrap(),
            vec![Limits {
                render_bytes: pinned.render_bytes,
                ..admitted
            }]
        );
        Ok(())
    }
#[tokio::test]
    async fn explicit_base_boundary_reruns_input_dependent_custom_transforms_and_reload()
    -> Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pipeline = ContextPipeline::new([Arc::new(InputDependentProjection {
            revision: 1,
            calls: calls.clone(),
        }) as Arc<dyn ContextStage>]);
        let mut input = ContextInput {
            input: ModelContent::Text("first input".into()),
            selected_context: None,
            step: 0,
            prior_messages: Vec::new(),
        };
        let raw = ContextPipeline::base_context(&input, Limits::default(), None)?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(ContextPipeline::default().run(&input, Limits::default(), None).await?, raw);
        let retained = retained_input_base(input.input.clone());
        let first = pipeline
            .transform(&input, retained.clone(), Limits::default(), None)
            .await?;
        assert_eq!(first.current_input_index, Some(1));
        assert_eq!(first.messages[1].content, input.input);
        assert_eq!(
            first.messages[0].content,
            ModelContent::Text("stage:1:\"first input\"".into())
        );
        assert_eq!(retained.messages.len(), 3);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        input.input = ModelContent::Text("later input".into());
        let mut next_base = retained.clone();
        next_base.messages[2].content = input.input.clone();
        let next = pipeline
            .transform(&input, next_base.clone(), Limits::default(), None)
            .await?;
        assert_eq!(
            next.messages[0].content,
            ModelContent::Text("stage:1:\"later input\"".into())
        );
        let reloaded =
            pipeline.reload(ContextPipeline::new([Arc::new(InputDependentProjection {
                revision: 2,
                calls: calls.clone(),
            })
                as Arc<dyn ContextStage>]))?;
        let next = reloaded
            .transform(&input, next_base, Limits::default(), None)
            .await?;
        assert_eq!(
            next.messages[0].content,
            ModelContent::Text("stage:2:\"later input\"".into())
        );
        assert_eq!(
            next.messages
                .iter()
                .filter(|message| message.role == ModelRole::System)
                .count(),
            1
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        let invalid = Context {
            current_input_index: None,
            ..retained
        };
        assert!(
            pipeline
                .transform(&input, invalid, Limits::default(), None)
                .await
                .is_err()
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        Ok(())
    }
#[tokio::test]
    async fn context_stages_preserve_explicit_input_position() -> Result<()> {
        let input = ContextInput {
            input: ModelContent::Text("active input".into()),
            selected_context: None,
            step: 0,
            prior_messages: vec![ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("prior answer".into()),
            }],
        };
        let source = Arc::new(Context {
            messages: vec![message(ModelRole::User, "additional-facts")?],
            ..Context::default()
        });
        let pipeline = ContextPipeline::new([
            Arc::new(SourceStage::new(
                "prepend",
                "1",
                source.clone(),
                ContextPlacement::Prepend,
            )) as Arc<dyn ContextStage>,
            Arc::new(SourceStage::new(
                "append",
                "1",
                source,
                ContextPlacement::Append,
            )),
        ]);
        let projected = pipeline.run(&input, Limits::default(), None).await?;
        assert_eq!(projected.current_input_index, Some(1));
        assert_eq!(projected.messages[1].content, input.input);
        let summary = ContextSummary {
            operation_id: crate::OperationId::new(),
            step: 0,
            source_messages: 4,
            source_digest: crate::contract::canonical_json_digest(&projected)?,
            output: message(ModelRole::Assistant, "summary")?
                .content
                .file_refs().next().unwrap()
                .clone(),
        };
        let (compacted, reference) = DurableContextProvider::compact(&projected, 3, Some(summary), CompactionRetention::default(), None)?;
        validate_compaction(&reference, &projected, &compacted, None)?;
        assert_eq!(compacted.current_input_index, Some(1));
        assert_eq!(compacted.messages[1].content, input.input);
        assert_eq!(compacted.messages.last(), projected.messages.last());
        assert!(
            ContextPipeline::new([Arc::new(RemoveInputMarker) as Arc<dyn ContextStage>])
                .run(&input, Limits::default(), None)
                .await
                .is_err()
        );

        let invalid = Context {
            current_input_index: Some(4),
            ..projected
        };
        assert!(
            validate_projected_context(&invalid, crate::conversation::Limits::default(), None).is_err()
        );
        assert!(
            DurableContextProvider::compact(&invalid, 4, None, CompactionRetention::default(), None)
                .is_err()
        );
        Ok(())
    }
#[tokio::test]
    async fn attribute_update_reload_and_bounds_use_one_pinned_state() -> Result<()> {
        let input = ContextInput {
            input: ModelContent::Text("task".into()),
            selected_context: None,
            step: 0,
            prior_messages: Vec::new(),
        };
        let old = attribute_pipeline(
            "old",
            ContextRenderMode::Prompt,
            ContextRepresentation::Full,
        )?;
        let snapshot = old.clone();
        let current = old.reload(attribute_pipeline(
            "new",
            ContextRenderMode::Prompt,
            ContextRepresentation::Full,
        )?)?;
        assert_ne!(snapshot.contracts(None)?, current.contracts(None)?);
        let rebuilt = current.run(&input, Limits::default(), None).await?;
        let update = attribute_pipeline(
            "new",
            ContextRenderMode::Update,
            ContextRepresentation::Full,
        )?
        .run(&input, Limits::default(), None)
        .await?;
        assert_eq!(
            rebuilt.messages.first().map(|m| &m.content),
            Some(&ModelContent::Text("Prompt:Full:new:\"new\"".into()))
        );
        assert_eq!(
            update.messages.last().map(|m| &m.content),
            Some(&ModelContent::Text("Update:Full:new:\"new\"".into()))
        );
        for representation in [
            ContextRepresentation::Full,
            ContextRepresentation::Summary,
            ContextRepresentation::Reference,
        ] {
            let view = attribute_pipeline("new", ContextRenderMode::Prompt, representation)?
                .run(&input, Limits::default(), None)
                .await?;
            assert_eq!(
                view.messages.first().map(|m| &m.content),
                Some(&ModelContent::Text(format!(
                    "Prompt:{representation:?}:new:\"new\""
                )))
            );
        }
        assert_eq!(snapshot.run(&input, Limits::default(), None).await?, old.run(&input, Limits::default(), None).await?);
        assert!(
            old.reload(ContextPipeline::new([Arc::new(CompactionStage {
                max_messages: 0,
                summary: None,
                retention: CompactionRetention::default(),
            })
                as Arc<dyn ContextStage>]))
                .is_err()
        );
        assert_eq!(old.contracts(None)?, snapshot.contracts(None)?);
        assert!(
            ContextAttribute::typed(
                "example.role".into(),
                "1".into(),
                serde_json::json!({"type":"integer"}),
                "2".into(),
                &"invalid"
            )
            .is_err()
        );
        let tiny = SelectionStage::new(
            "role".into(),
            ContextSelection {
                source: ContextSourceValue::Attribute {
                    attribute: ContextAttribute::typed(
                        "example.role".into(),
                        "1".into(),
                        serde_json::json!({"type":"string"}),
                        "1".into(),
                        &"role",
                    )?,
                },
                extent: ContextExtent::Whole,
                representation: ContextRepresentation::Full,
            },
            Arc::new(AttributeRenderer),
            Arc::new(RefVerifier),
            ContextRenderMode::Prompt,
            ContextPlacement::Append,
            crate::conversation::Limits {
                render_bytes: 1,
                ..crate::conversation::Limits::default()
            },
        );
        assert!(tiny.is_err());
        Ok(())
    }
proptest::proptest! {
        #[test]
        fn file_spans_are_exact_and_bounded(start in 0u64..20, end in 0u64..20) {
            let content = message(ModelRole::User, "1234567890").unwrap();
            let ModelContent::Part(ModelContentPart::File { file, .. }) = content.content else { unreachable!() };
            let selection = ContextSelection { source: ContextSourceValue::File { file },
                extent: ContextExtent::Span { start, end }, representation: ContextRepresentation::Full };
            proptest::prop_assert_eq!(selection.validate(crate::conversation::Limits::default(), &mut None).is_ok(), start < end && end <= 10);
        }
    }
