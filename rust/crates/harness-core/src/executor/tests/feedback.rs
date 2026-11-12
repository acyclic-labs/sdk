use super::*;

const NOTICE: &str =
    "What slowed you down or blocked you? Append feedback records; reply none if nothing.";

#[tokio::test]
async fn headless_feedback_retrospective_runs_once_per_settlement_and_preserves_the_prefix()
-> Result<()> {
    let seeded = crate::plugins::seed().await?;
    assert!(seeded.names.iter().any(|name| name == "feedback"));
    struct Seed;
    impl crate::config::Tree for Seed {
        fn read<'a>(
            &'a self,
            path: &'a str,
        ) -> BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
            Box::pin(async move {
                Ok(crate::seed::file(path).map(acyclic_stream::OwnedBytes::from_static))
            })
        }
    }
    for retrospective in [true, false] {
        let override_source = format!(
            "export default {{ feedback: {{ enabled: true, config: {{ retrospective: {retrospective} }} }} }};"
        );
        let disabled;
        let plugins = if retrospective {
            seeded.as_ref()
        } else {
            disabled = crate::plugins::load(
                &Seed,
                &[crate::layers::Layer {
                    name: "home",
                    generation: "test",
                    bytes: Some(override_source.as_bytes()),
                }],
                "headless",
            )
            .await?;
            &disabled
        };
        let per_settlement = if retrospective { 2 } else { 1 };
        let provider = Arc::new(
            ScriptedModelProvider::new((0..2 * per_settlement).map(|step| {
                ScriptedTurn::Text(
                    if retrospective && step % per_settlement != 0 {
                        "none"
                    } else {
                        "task complete"
                    }
                    .into(),
                )
            }))
            .with_token_counter(fixture_token_count),
        );
        let executor = uncompacted_executor(
            Model::new("test".into(), "feedback".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_plugins(plugins)?;
        let journal = Journal::default();
        let first = TurnInput {
            max_steps: None,
            operation_id: OperationId::new(),
            input: ModelContent::Text("headless task".into()),
            selected_context: None,
        };
        assert_eq!(
            executor.execute(first.clone(), &journal).await?.text,
            "task complete"
        );
        assert_eq!(
            executor.execute(first, &journal).await?.text,
            "task complete"
        );
        assert_eq!(
            provider.request_count(),
            per_settlement,
            "replay cannot ask again"
        );
        executor
            .execute(
                TurnInput {
                    max_steps: None,
                    operation_id: OperationId::new(),
                    input: ModelContent::Text("another task".into()),
                    selected_context: None,
                },
                &journal,
            )
            .await?;
        let requests = provider.requests();
        assert_eq!(requests.len(), 2 * per_settlement);
        assert!(
            requests[0]
                .messages
                .iter()
                .any(|message| matches!(&message.content,
            ModelContent::Text(text) if text.contains("Feedback is cheap and wanted")))
        );
        for settlement in 0..2 {
            let initial = &requests[settlement * per_settlement];
            let final_request = &requests[(settlement + 1) * per_settlement - 1];
            let notice_count = final_request.messages.iter().filter(|message|
                matches!(&message.content, ModelContent::Text(text) if text == NOTICE)).count();
            assert_eq!(notice_count, usize::from(retrospective));
            assert_eq!(
                crate::contract::canonical_json_bytes(&initial.messages)?,
                crate::contract::canonical_json_bytes(
                    &final_request.messages[..initial.messages.len()]
                )?,
                "the notice must be an appended delta, byte-stable over the original prefix"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn an_accepted_answer_survives_a_retrospective_step_limit_or_provider_failure() -> Result<()>
{
    for maximum in [Some(1), None] {
        let provider = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Text("task complete".into()),
                ScriptedTurn::Events(vec![ScriptedEvent::Error {
                    error: "response unavailable".into(),
                }]),
            ])
            .with_token_counter(fixture_token_count),
        );
        let executor = uncompacted_executor(
            Model::new("test".into(), "feedback".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_plugins(crate::plugins::seed().await?.as_ref())?;
        let journal = Journal::default();
        let input = TurnInput {
            max_steps: maximum,
            operation_id: OperationId::new(),
            input: ModelContent::Text("finish the task".into()),
            selected_context: None,
        };
        let output = executor.execute(input.clone(), &journal).await?;
        assert_eq!(output.text, "task complete");
        assert_eq!(
            output
                .transcript
                .iter()
                .filter(
                    |message| matches!(&message.content, ModelContent::Text(text) if text == NOTICE)
                )
                .count(),
            1,
            "the appended notice survives even when its response cannot finish"
        );
        let requests = provider.request_count();
        assert_eq!(requests, if maximum.is_some() { 1 } else { 2 });
        assert_eq!(executor.execute(input, &journal).await?.text, output.text);
        assert_eq!(provider.request_count(), requests);
    }
    Ok(())
}
