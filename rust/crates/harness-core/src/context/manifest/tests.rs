use super::*;
use proptest::prelude::*;

proptest! {
    // Property: any selected definition mismatch rejects atomically. On success
    // the exact requested selection replaces visibility while admitted versions remain.
    #[test]
    fn pinning_is_exact_and_transactional(description in ".{0,80}", hide in any::<bool>(), tamper in any::<bool>()) {
        let mut tools = ToolRegistry::default();
        let mut tool = crate::tool::files::write()?;
        tool.definition.description = description;
        tools.register((tool, None), &mut None, None)?;
        let before = tools.definitions(None)?;
        let first = before.first().ok_or_else(|| TestCaseError::fail("registered definition missing"))?;
        let name = &first.name;
        let revision = &first.revision;
        let mut selected = if hide { Vec::new() } else { before.clone() };
        if tamper && !hide {
            selected.first_mut().ok_or_else(|| TestCaseError::fail("selected definition missing"))?.description.push('!');
        }
        let result = tools.pin_model_definitions(&selected, None);
        if tamper && !hide {
            prop_assert!(matches!(result, Err(Error::Conflict(_))));
            prop_assert_eq!(tools.definitions(None)?, before.clone());
        } else {
            prop_assert!(result.is_ok());
            prop_assert_eq!(tools.definitions(None)?, selected);
        }
        prop_assert!(tools.get(name, Some(revision), None)?.is_some());
    }
}

#[tokio::test]
async fn optional_environment_joins_after_all_required_stages_on_public_pipeline_routes()
-> Result<()> {
    let required = Context {
        messages: vec![ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text("required".into()),
        }],
        ..Default::default()
    };
    let pipeline = ContextPipeline::new([
        Arc::new(ManifestStage {
            context: Context::default(),
            sources: Vec::new(),
            environment: Some(EnvironmentFacts {
                cwd: Some("/bound".into()),
                ..Default::default()
            }),
        }) as Arc<dyn ContextStage>,
        Arc::new(SourceStage::new(
            "required",
            "1",
            Arc::new(required),
            ContextPlacement::Prepend,
        )),
    ]);
    let input = ContextInput {
        input: ModelContent::Text("current".into()),
        selected_context: None,
        step: 0,
        prior_messages: Vec::new(),
    };
    for (message_limit, current_input_index) in [(2, 1), (3, 2)] {
        let limits = Limits {
            context_messages: std::num::NonZeroUsize::new(message_limit),
            ..Limits::default()
        };
        let run = pipeline.run(&input, limits, None).await?;
        let base = ContextPipeline::base_context(&input, limits, None)?;
        assert_eq!(pipeline.transform(&input, base, limits, None).await?, run);
        assert_eq!(run.messages.len(), message_limit);
        assert_eq!(run.current_input_index, Some(current_input_index));
        assert_eq!(
            run.messages[0].content,
            ModelContent::Text("required".into())
        );
        if message_limit == 3 {
            assert!(
                matches!(&run.messages[1].content, ModelContent::Text(text) if text.starts_with("<environment_context>"))
            );
        }
    }
    Ok(())
}
