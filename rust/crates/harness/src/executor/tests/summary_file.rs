use super::*;
use proptest::prelude::*;

#[test]
fn file_summary_returns_collected_text_without_reading_its_published_output() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::default()
        .run(
            &("[a-zé猫 ]{1,32}", "[a-zé猫 ]{1,32}"),
            |(source, text)| {
                runtime.block_on(async {
                    let journal = Journal::default();
                    let operation = OperationId::new();
                    let source = crate::context::Context {
                        messages: vec![ModelMessage {
                            role: ModelRole::User,
                            content: ModelContent::Text(source),
                        }],
                        ..Default::default()
                    };
                    let manifest = journal
                        .stage(operation, "manifest", b"{}", "application/json", None)?
                        .await?;
                    let source = crate::context::SummarySource {
                        context: source,
                        boundary: crate::context::SummaryBoundary {
                            task: crate::TaskId::from_bytes([1; 16]),
                            through: crate::task_volume::Cursor {
                                volume: [0; 32],
                                epoch: uuid::Uuid::nil(),
                                schema: crate::task_volume::SCHEMA,
                                query: [0; 32],
                                position: 1,
                                accepted: 1,
                                durable: 1,
                                commit: [0; 32],
                            },
                            captured_at: 1,
                            previous: None,
                            outcome_included: false,
                            frozen: false,
                            manifest,
                            model: "summary".into(),
                        },
                    };
                    let encoded = crate::contract::canonical_json_bytes(&source)?;
                    prop_assert_eq!(
                        serde_json::from_slice::<crate::context::SummarySource>(&encoded).unwrap(),
                        source
                    );
                    let source = journal
                        .stage(
                            operation,
                            "selected-source",
                            &encoded,
                            "application/json",
                            None,
                        )?
                        .await?;
                    let scope = RuntimeScope::new(
                        crate::Capabilities::new([
                            source.read_capability(&mut None)?,
                            "model:generate".into(),
                        ]),
                        Limits::default(),
                    )?;
                    let (provider, _) =
                        counted_model(8_192, None, [ScriptedTurn::Text(text.clone())], []);
                    // The default automatic policy does not fit this small model.
                    // A selected summary uses only its explicit output ceiling.
                    let executor = StockExecutor::new(
                        Model::new("test".into(), "summary".into(), "1".into(), Value::Null)?,
                        provider.clone(),
                        ContextPipeline::default(),
                        ToolRegistry::default(),
                    )
                    .with_tool_authority(scope, None)?
                    .with_max_output_tokens(1_024)?
                    .with_summary_source(Some(Box::new(source.clone())));
                    let output = executor
                        .execute_progress(
                            TurnInput {
                                operation_id: operation,
                                input: ModelContent::Text("Summarize".into()),
                                selected_context: None,
                                max_steps: 1,
                            },
                            &journal,
                        )
                        .await?
                        .into_output()?;
                    prop_assert_eq!(output.text, text);
                    let checkpoint: crate::context::SummaryCheckpoint =
                        serde_json::from_value(output.metadata)?;
                    prop_assert_eq!(&checkpoint.source, &source);
                    let source_publications = || {
                        journal
                            .1
                            .lock()
                            .unwrap()
                            .keys()
                            .filter(|key| {
                                key.ends_with(":selected-source")
                                    || key.ends_with(":summary:0:source")
                            })
                            .count()
                    };
                    prop_assert_eq!(source_publications(), 1);
                    // The former redundant source stage must fail the same detector.
                    journal
                        .stage(
                            operation,
                            "summary:0:source",
                            &encoded,
                            "application/json",
                            None,
                        )?
                        .await?;
                    prop_assert_ne!(source_publications(), 1);
                    prop_assert!(provider.requests()[0].tools.is_empty());
                    let no_readback = || journal.5.load(Ordering::SeqCst) == 0;
                    prop_assert!(no_readback(), "summary reread its just-published text");
                    // Reproduce the former readback through the same journal/load
                    // observer and require the same detector to reject it.
                    let output = journal
                        .1
                        .lock()
                        .unwrap()
                        .values()
                        .find(|(file, _)| file.descriptor().media_type() == "text/plain")
                        .unwrap()
                        .0
                        .clone();
                    journal.load(&output, None)?.await?;
                    prop_assert!(!no_readback(), "detector missed the readback control");
                    Ok(())
                })
            },
        )
        .unwrap();
    Ok(())
}

async fn compact_sequence_source(
    journal: &Journal,
    provider: Arc<ScriptedModelProvider>,
    text: String,
) -> Result<TurnOutput> {
    let operation = OperationId::new();
    let cursor = crate::task_volume::Cursor {
        volume: [0; 32],
        epoch: uuid::Uuid::nil(),
        schema: crate::task_volume::SCHEMA,
        query: [0; 32],
        position: 1,
        accepted: 1,
        durable: 1,
        commit: [0; 32],
    };
    let checkpoint = crate::context::summary_source::tests::checkpoint(cursor);
    let mut context = crate::context::Context::default();
    context.messages.push(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text(text),
    });
    let source = crate::context::SummarySource {
        context,
        boundary: checkpoint.boundary,
    };
    let source = journal
        .stage(
            operation,
            "selected-source",
            &crate::contract::canonical_json_bytes(&source)?,
            "application/json",
            None,
        )?
        .await?;
    let scope = RuntimeScope::new(
        crate::Capabilities::new([source.read_capability(&mut None)?, "model:generate".into()]),
        Limits::default(),
    )?;
    uncompacted_executor(
        Model::new("test".into(), "summary".into(), "1".into(), Value::Null)?,
        provider,
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_tool_authority(scope, None)?
    .with_max_output_tokens(1024)?
    .with_summary_source(Some(Box::new(source)))
    .execute_progress(
        TurnInput {
            operation_id: operation,
            input: ModelContent::Text("Summarize".into()),
            selected_context: None,
            max_steps: 1,
        },
        journal,
    )
    .await?
    .into_output()
}

#[test]
fn summary_sequence_sends_summary_only_facts_and_new_turns_to_the_next_request() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::default()
        .run(
            &(
                "[A-Z]{1,32}",
                proptest::collection::vec("[a-z]{1,16}", 1..8),
            ),
            |(fact, turns)| {
                runtime.block_on(async {
                    let journal = Journal::default();
                    let fact = format!("SUMMARY_ONLY_{fact}");
                    let (provider, _) = counted_model(
                        8192,
                        None,
                        [
                            ScriptedTurn::Text(fact.clone()),
                            ScriptedTurn::Text("second".into()),
                            ScriptedTurn::Text("broken".into()),
                        ],
                        [],
                    );
                    let first = compact_sequence_source(
                        &journal,
                        provider.clone(),
                        "original input".into(),
                    )
                    .await?;
                    let source = |keep| {
                        let mut source = if keep {
                            crate::context::summary_source::retained_source::<true>(
                                Some(first.text.clone()),
                                None,
                                Limits::default(),
                            )
                        } else {
                            crate::context::summary_source::retained_source::<false>(
                                Some(first.text.clone()),
                                None,
                                Limits::default(),
                            )
                        }
                        .unwrap();
                        for turn in &turns {
                            source.push_str(turn);
                            source.push('\n');
                        }
                        source
                    };
                    compact_sequence_source(&journal, provider.clone(), source(true)).await?;
                    compact_sequence_source(&journal, provider.clone(), source(false)).await?;
                    let requests = provider.requests();
                    let preserves = |index| {
                        let request = serde_json::to_string(&requests[index]).unwrap();
                        request.contains(&fact) && turns.iter().all(|turn| request.contains(turn))
                    };
                    prop_assert!(preserves(1));
                    prop_assert!(
                        !preserves(2),
                        "detector missed original-input reconstruction"
                    );
                    Ok(())
                })
            },
        )
        .unwrap();
    Ok(())
}
