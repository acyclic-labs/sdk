use super::*;
#[cfg(not(target_arch = "wasm32"))]
use crate::test_support::bounds::{measure, notice_message};

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
    input.prior_messages.push(notice_message());
    input
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

/// Extending an admission bounds only the appended message, and copying all
/// or part of one bounds nothing: an admitted history of 8 or 512 notice
/// batches measures the same to extend by one more.
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn extending_an_admission_bounds_only_appended_messages() {
    let measured = |count: usize| {
        let context = Context {
            messages: (0..count).map(|_| notice_message()).collect(),
            ..Context::default()
        };
        let admitted = AdmittedContext::admit(context, payload_limits(), None).unwrap();
        let (copied, copying) = measure(async {
            let range = admitted.copy_range(0..count / 2, None)?;
            admitted.copy(None).map(|copy| (copy, range))
        });
        let (copy, range) = copied.unwrap();
        assert_eq!((copy == admitted, range.messages.len()), (true, count / 2));
        assert_eq!(copying, 0, "a copy keeps its admission");
        let (extended, measures) = measure(async { copy.push(notice_message(), None) });
        assert_eq!(extended.unwrap().messages.len(), count + 1);
        measures
    };
    let small = measured(8);
    assert_ne!(small, 0, "the appended notice batch is bounded");
    assert_eq!(measured(512), small);
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
