use super::*;

#[derive(Default)]
struct Probe(std::sync::Mutex<Vec<Limits>>);

impl ContextSource for Probe {
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.0.lock().unwrap().push(limits);
            Ok(vec![text(ModelRole::System, "source")])
        })
    }
}

impl ContextRenderer for Probe {
    fn contract(&self, _: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        Ok(serde_json::json!({"revision": 1}))
    }
    fn render<'a>(
        &'a self,
        selection: &'a ContextSelection,
        _: ContextRenderMode,
        _: &'a ContextInput,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.0.lock().unwrap().push(limits);
            let value = match &selection.source {
                ContextSourceValue::Attribute { attribute } => attribute.value.to_string(),
                _ => "source".into(),
            };
            Ok(vec![text(ModelRole::System, &value)])
        })
    }
}

struct Transform(u32);

impl ContextStage for Transform {
    fn name(&self) -> &str {
        "transform"
    }
    fn contract(&self, _: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        Ok(serde_json::json!({"revision": self.0}))
    }
    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        mut context: Context,
        _: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Context>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            context.messages.push(text(
                ModelRole::System,
                &format!("{}:{:?}", self.0, input.input),
            ));
            context.messages.reverse();
            context.current_input_index = context
                .current_input_index
                .map(|index| context.messages.len() as u32 - 1 - index);
            Ok(context)
        })
    }
}

#[tokio::test]
async fn sources_joins_and_selection_narrow_admitted_limits() -> Result<()> {
    let input = input();
    let limits = Limits {
        context_messages: 2,
        render_bytes: 2048,
        ..payload_limits()
    };
    let probe = Arc::new(Probe::default());
    for placement in [ContextPlacement::Prepend, ContextPlacement::Append] {
        let pipeline = ContextPipeline::new([Arc::new(SourceStage::new(
            "source",
            "1",
            probe.clone(),
            placement,
        )) as Arc<dyn ContextStage>]);
        let projected = pipeline.run(&input, limits, None).await?;
        assert_eq!(
            projected.current_input_index,
            Some(u32::from(placement == ContextPlacement::Prepend))
        );
        let exact = crate::contract::canonical_json_bytes(&projected)?.len() as u64;
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
            assert!(pipeline.run(&input, rejected, None).await.is_err());
        }
        assert_eq!(
            pipeline
                .run(
                    &input,
                    Limits {
                        render_bytes: exact,
                        ..limits
                    },
                    None
                )
                .await?,
            projected
        );
    }
    assert_eq!(probe.0.lock().unwrap()[0], limits);
    let oversized = context(vec![text(ModelRole::System, &"x".repeat(2048))], None);
    assert!(oversized.load(&input, limits, None)?.await.is_err());
    let file = message(ModelRole::System, "0123456789")?
        .content
        .file_refs()
        .next()
        .unwrap()
        .clone();
    let discovered = DiscoveredContext {
        instructions: vec![file.clone(), file.clone()],
        skills: Vec::new(),
    };
    assert!(
        discovered
            .load(
                &input,
                Limits {
                    context_messages: 1,
                    ..limits
                },
                None
            )?
            .await
            .is_err()
    );
    assert!(
        discovered
            .load(
                &input,
                Limits {
                    file_bytes: 1,
                    ..limits
                },
                None
            )?
            .await
            .is_err()
    );
    let renderer = Arc::new(Probe::default());
    let selection = ContextSelection {
        source: ContextSourceValue::File { file },
        extent: ContextExtent::Whole,
        representation: ContextRepresentation::Full,
    };
    let stage = SelectionStage::new(
        "selection".into(),
        selection,
        renderer.clone(),
        Arc::new(RefVerifier),
        ContextRenderMode::Prompt,
        ContextPlacement::Prepend,
        limits,
    )?;
    let admitted = Limits {
        file_bytes: 9,
        render_bytes: 4096,
        ..limits
    };
    let base = ContextPipeline::base_context(&input, admitted, None)?;
    assert!(
        stage
            .apply(&input, base.clone(), admitted, None)?
            .await
            .is_err()
    );
    assert!(renderer.0.lock().unwrap().is_empty());
    let admitted = Limits {
        file_bytes: 10,
        ..admitted
    };
    stage.apply(&input, base, admitted, None)?.await?;
    assert_eq!(
        *renderer.0.lock().unwrap(),
        vec![Limits {
            render_bytes: limits.render_bytes,
            ..admitted
        }]
    );
    Ok(())
}

#[tokio::test]
async fn explicit_base_transforms_reload_and_preserve_current_input() -> Result<()> {
    let mut input = input();
    let base = context(
        vec![
            text(ModelRole::User, "old"),
            text(ModelRole::Assistant, "answer"),
            text(ModelRole::User, "current"),
        ],
        Some(2),
    );
    let pipeline = ContextPipeline::new([Arc::new(Transform(1)) as Arc<dyn ContextStage>]);
    let first = pipeline
        .transform(&input, base.clone(), payload_limits(), None)
        .await?;
    assert_eq!(first.current_input_index, Some(1));
    assert_eq!(first.messages[1].content, input.input);
    input.input = ModelContent::Text("later".into());
    let mut next = base.clone();
    next.messages[2].content = input.input.clone();
    let reloaded = pipeline.reload(ContextPipeline::new([
        Arc::new(Transform(2)) as Arc<dyn ContextStage>
    ]))?;
    let old = pipeline
        .transform(&input, next.clone(), payload_limits(), None)
        .await?;
    let new = reloaded
        .transform(&input, next, payload_limits(), None)
        .await?;
    assert_ne!(old.messages[0], first.messages[0]);
    assert_ne!(new.messages[0], old.messages[0]);
    assert_eq!(new.messages[1].content, input.input);
    assert_eq!(base.messages.len(), 3);
    let mut invalid = base;
    invalid.current_input_index = None;
    assert!(
        pipeline
            .transform(&input, invalid, payload_limits(), None)
            .await
            .is_err()
    );
    assert!(
        pipeline
            .reload(ContextPipeline::new([Arc::new(CompactionStage {
                max_messages: 0,
                summary: None,
                retention: CompactionRetention::default()
            })
                as Arc<dyn ContextStage>]))
            .is_err()
    );
    Ok(())
}

fn attribute_pipeline(
    value: &str,
    mode: ContextRenderMode,
    representation: ContextRepresentation,
) -> Result<ContextPipeline> {
    let attribute = ContextAttribute::typed(
        "example.role".into(),
        "1".into(),
        serde_json::json!({"type":"string"}),
        value.into(),
        &value,
    )?;
    Ok(ContextPipeline::new([Arc::new(SelectionStage::new(
        "role".into(),
        ContextSelection {
            source: ContextSourceValue::Attribute { attribute },
            extent: ContextExtent::Whole,
            representation,
        },
        Arc::new(Probe::default()),
        Arc::new(RefVerifier),
        mode,
        ContextPlacement::Prepend,
        crate::conversation::Limits::default(),
    )?) as Arc<dyn ContextStage>]))
}

#[tokio::test]
async fn attribute_reload_modes_and_schema_bounds() -> Result<()> {
    let input = input();
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
    let before = snapshot.run(&input, payload_limits(), None).await?;
    let prompt = current.run(&input, payload_limits(), None).await?;
    let update = attribute_pipeline(
        "new",
        ContextRenderMode::Update,
        ContextRepresentation::Full,
    )?
    .run(&input, payload_limits(), None)
    .await?;
    assert_ne!(before.messages[0], prompt.messages[0]);
    assert_eq!(prompt.messages.last().unwrap().content, input.input);
    assert_eq!(update.messages.first().unwrap().content, input.input);
    assert_eq!(snapshot.run(&input, payload_limits(), None).await?, before);
    for representation in [
        ContextRepresentation::Full,
        ContextRepresentation::Summary,
        ContextRepresentation::Reference,
    ] {
        let pipeline = attribute_pipeline("new", ContextRenderMode::Prompt, representation)?;
        assert!(
            pipeline
                .run(
                    &input,
                    Limits {
                        render_bytes: 1,
                        ..payload_limits()
                    },
                    None
                )
                .await
                .is_err()
        );
        assert_eq!(
            pipeline
                .run(&input, payload_limits(), None)
                .await?
                .messages
                .len(),
            2
        );
    }
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
