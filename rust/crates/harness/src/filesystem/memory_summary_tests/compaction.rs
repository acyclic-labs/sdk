use super::*;

#[tokio::test]
async fn default_compaction_bounds_small_message_continuation() -> Result<()> {
    const TURNS: u16 = 10_000;
    let mut storage = MemoryHarnessStorage::new(AgentId::new(), 262_144).await?;
    let model = Arc::new(SummaryModel::default());
    let limits = Limits {
        context_messages: 8,
        ..Limits::default()
    };
    let instructions = Arc::new(Context {
        messages: [
            "Keep the user's constraints.",
            "Preserve original references.",
        ]
        .into_iter()
        .map(|text| crate::model::ModelMessage {
            role: crate::model::ModelRole::System,
            content: crate::model::ModelContent::Text(text.into()),
        })
        .collect(),
        ..Context::default()
    });
    let build_bundle = |storage: &MemoryHarnessStorage| -> Result<crate::bundle::HarnessBundle> {
        storage
            .builder()
            .model(
                Model::new(
                    "synthetic".into(),
                    "small-message-continuation".into(),
                    "1".into(),
                    Value::Null,
                )?,
                model.clone(),
            )
            .grant("model:generate")
            .limits(limits)
            .context(crate::context::ContextPipeline::new([Arc::new(
                crate::context::SourceStage::new(
                    "instructions",
                    "1",
                    instructions.clone(),
                    crate::context::ContextPlacement::Prepend,
                ),
            )
                as Arc<dyn crate::context::ContextStage>]))
            .build()
    };
    let mut bundle = build_bundle(&storage)?;
    let mut original = None;
    for index in 0..TURNS {
        let operation = OperationId::new();
        let input = storage
            .stage(
                operation,
                &format!("turns/{index}/input.txt"),
                b"small input",
                "text/plain",
                "input.txt",
            )
            .await
            .map_err(|error| Error::Storage(format!("retained turn {index} input: {error}")))?;
        let output = storage
            .run_conversation(&bundle, operation, input.clone(), Vec::new(), 1)
            .await
            .map_err(|error| Error::Storage(format!("retained turn {index}: {error}")))?;
        if original.is_none() {
            original = Some((operation, input, output));
        }
        let aggregate = storage.journal.conversation_projection(limits).await?;
        let conversation = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("conversation not bound".into()))?;
        assert_eq!(conversation.logical_revision(), u64::from(index + 1) * 2);
        assert!(conversation.messages().len() <= limits.context_messages);
        if index >= 3 {
            assert!(conversation.resident_after_sequence() > 0);
        }
        drop(aggregate);
        if index == TURNS - 3 {
            storage.journal = Arc::new(
                FilesystemExecutionJournal::new(
                    storage.stream.clone(),
                    storage.host.clone(),
                    &crate::core::SchemaRegistry::new(),
                    crate::filesystem::FilesystemJournalBinding {
                        volume: &storage.volume,
                        verifier: &storage.verifier(),
                        scope: &storage.scope,
                        maximum_bytes: storage.maximum_file_bytes,
                        io: None,
                    },
                )?
                .with_input_verifier(storage.content_verifier.clone()),
            );
            bundle = build_bundle(&storage)?;
        }
    }
    let requests = model
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .len();
    assert!(
        requests > usize::from(TURNS),
        "count pressure must admit summary requests"
    );
    let (operation, input, expected) =
        original.ok_or_else(|| Error::Invalid("missing original turn".into()))?;
    assert_eq!(
        storage
            .run_conversation(&bundle, operation, input, Vec::new(), 1)
            .await?,
        expected
    );
    assert_eq!(
        model
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        requests
    );
    Ok(())
}
