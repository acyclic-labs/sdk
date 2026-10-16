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
                        crate::Capabilities::new([
                            source.read_capability(&mut None)?,
                            "model:generate".into(),
                        ]),
                        Limits::default(),
                    )?;
                    let (provider, _) =
                        counted_model(131_072, None, [ScriptedTurn::Text(text.clone())], []);
                    let executor = uncompacted_executor(
                        Model::new("test".into(), "summary".into(), "1".into(), Value::Null)?,
                        provider.clone(),
                        ContextPipeline::default(),
                        ToolRegistry::default(),
                    )
                    .with_tool_authority(scope, None)?
                    .with_max_output_tokens(1_024)?
                    .with_summary_source(Some(Box::new(source)));
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
