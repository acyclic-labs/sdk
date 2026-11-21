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

struct Transform(u32);

fn admit(context: Context) -> AdmittedContext {
    AdmittedContext::admit(context, payload_limits(), None).unwrap()
}

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
        context: AdmittedContext,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, AdmittedContext>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let mut context = context.into_context();
            context.messages.push(text(
                ModelRole::System,
                &format!("{}:{:?}", self.0, input.input),
            ));
            context.messages.reverse();
            let length = u32::try_from(context.messages.len())
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            context.current_input_index =
                context.current_input_index.map(|index| length - 1 - index);
            AdmittedContext::admit(context, limits, io)
        })
    }
}

/// The test input with a notice batch in its history, which bounds measure.
#[cfg(not(target_arch = "wasm32"))]
fn notice_input() -> ContextInput {
    let mut input = input();
    input.prior_messages.push(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Part(ModelContentPart::Notices {
            notices: vec![crate::conversation::wire::Notice {
                source: "tools".into(),
                kind: "tools.changed".into(),
                body: serde_json::json!({"history": "x".repeat(512)}),
                wake: false,
                coalesce_key: None,
            }],
        }),
    });
    input
}

/// Runs `work` under a seeded schedule and counts its bound measurements.
#[cfg(not(target_arch = "wasm32"))]
fn measure<T>(work: impl std::future::Future<Output = T>) -> (T, u64) {
    use acyclic_native_runtime::{cost::Cost, testing::Schedule};
    let (schedule, mut boundaries) = Schedule::seeded(1);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let output = runtime.block_on(async {
        let driver = async {
            while let Some(boundary) = boundaries.recv().await {
                boundary.release();
            }
        };
        tokio::select! {
            output = schedule.scope(work) => output,
            () = driver => unreachable!("the schedule outlives its driver"),
        }
    });
    (output, schedule.ledger().unattributed.count(Cost::Measure))
}

/// Base assembly bounds each message once: it measures exactly what one
/// admission of the context it assembles measures.
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn base_assembly_bounds_each_message_once() {
    let input = notice_input();
    let (base, assembled) =
        measure(async { ContextPipeline::base_context(&input, payload_limits(), None) });
    let context = base.unwrap().into_context();
    let (admitted, once) =
        measure(async { AdmittedContext::admit(context, payload_limits(), None) });
    admitted.unwrap();
    assert_ne!(once, 0, "admission bounds the notice batch");
    assert_eq!(assembled, once);
}

/// Admission walks each message once. Source stages that place their output
/// beside an admitted base add no measurement of the base's notice batch:
/// a pipeline of two such stages measures exactly what base assembly did.
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn stages_never_walk_admitted_history_again() {
    let measured = |stages: usize| {
        let input = notice_input();
        let probe = Arc::new(Probe::default());
        let pipeline = ContextPipeline::new((0..stages).map(|_| {
            Arc::new(SourceStage::new(
                "source",
                "1",
                probe.clone(),
                ContextPlacement::Append,
            )) as Arc<dyn ContextStage>
        }));
        let (projected, measures) = measure(pipeline.run(&input, payload_limits(), None));
        assert_eq!(projected.unwrap().context().messages.len(), 2 + stages);
        measures
    };
    let base = measured(0);
    assert_ne!(base, 0, "base assembly admits the notice batch");
    assert_eq!(measured(2), base);
}

#[tokio::test]
async fn sources_and_joins_narrow_admitted_limits() -> Result<()> {
    let input = input();
    let limits = Limits {
        context_messages: std::num::NonZeroUsize::new(2),
        render_bytes: Some(2048),
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
        let exact = "current".len() as u64;
        for rejected in [
            Limits {
                context_messages: std::num::NonZeroUsize::new(1),
                ..limits
            },
            Limits {
                render_bytes: Some(exact - 1),
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
                        render_bytes: Some(exact),
                        ..limits
                    },
                    None
                )
                .await?,
            projected
        );
    }
    assert_eq!(probe.0.lock().unwrap()[0], limits);
    let full_parts = context(
        vec![
            text(ModelRole::System, &"x".repeat(2048)),
            text(ModelRole::Assistant, &"y".repeat(2048)),
        ],
        None,
    );
    assert_eq!(
        full_parts.load(&input, limits, None)?.await?,
        full_parts.messages
    );
    let oversized = context(vec![text(ModelRole::System, &"x".repeat(2049))], None);
    assert!(oversized.load(&input, limits, None)?.await.is_err());
    let file = message(ModelRole::System, "0123456789")?
        .content
        .file_refs()
        .next()
        .unwrap()
        .clone();
    let discovered = DiscoveredContext {
        instructions: vec![file.clone().into(), file.clone().into()],
        skills: Vec::new(),
    };
    assert!(
        discovered
            .load(
                &input,
                Limits {
                    context_messages: std::num::NonZeroUsize::new(1),
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
                    file_bytes: Some(1),
                    ..limits
                },
                None
            )?
            .await
            .is_err()
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
        .transform(&input, admit(base.clone()), payload_limits(), None)
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
        .transform(&input, admit(next.clone()), payload_limits(), None)
        .await?;
    let new = reloaded
        .transform(&input, admit(next), payload_limits(), None)
        .await?;
    assert_ne!(old.messages[0], first.messages[0]);
    assert_ne!(new.messages[0], old.messages[0]);
    assert_eq!(new.messages[1].content, input.input);
    assert_eq!(base.messages.len(), 3);
    let mut invalid = base;
    invalid.current_input_index = None;
    assert!(
        pipeline
            .transform(&input, admit(invalid), payload_limits(), None)
            .await
            .is_err()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
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
