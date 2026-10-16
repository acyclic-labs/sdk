use super::*;

async fn continuation_storage(turns: usize) -> Result<MemoryHarnessStorage> {
    let (filesystem, stream) = continuation_providers(turns)?;
    MemoryHarnessStorage::from_providers(AgentId::new(), 262_144, filesystem, stream).await
}

fn continuation_providers(
    turns: usize,
) -> Result<(
    Fs<MemoryAuthorityBackend, MemoryObjectBackend>,
    Arc<MemoryStream>,
)> {
    // Immutable payloads and journal facts survive compaction.
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_bucket(
        "continuation",
        acyclic_objects::v1::MemoryOptions {
            maximum_bytes: turns * 262_144 * 32,
            maximum_entries: turns * 1_024,
        },
    )
    .map_err(|error| Error::Storage(error.to_string()))?;
    let retained = turns * 1_024;
    let stream = Arc::new(MemoryStream::new(acyclic_stream::MemoryLimits {
        paths: retained,
        path_bytes: retained * 512,
        records: retained,
        payload_bytes: turns * 262_144 * 32,
        commits: retained,
        idempotency_results: retained,
    }));
    let filesystem = Fs::from_memory_providers(stream.clone(), Arc::new(objects), bucket);
    Ok((filesystem, stream))
}

#[tokio::test]
async fn payload_staging_work_grows_linearly_with_retained_history() -> Result<()> {
    let storage = continuation_storage(192 + 256).await?;
    staging_growth(&[storage]).await
}

#[tokio::test]
async fn alternating_volumes_preserve_linear_payload_staging_work() -> Result<()> {
    let (filesystem, stream) = continuation_providers(2 * (192 + 256))?;
    let mut storages = Vec::new();
    for _ in 0..2 {
        storages.push(
            MemoryHarnessStorage::from_providers(
                AgentId::new(),
                262_144,
                filesystem.clone(),
                stream.clone(),
            )
            .await?,
        );
    }
    staging_growth(&storages).await
}

async fn staging_growth(storages: &[MemoryHarnessStorage]) -> Result<()> {
    const WARMUP: usize = 192;
    const SMALL: usize = 64;
    const LARGE: usize = 256;
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut small = acyclic_fs::WorkCounters::default();
    let mut total = small;
    for index in 0..WARMUP + LARGE {
        for storage in storages {
            let path = format!("turns/{index:04}/input.txt");
            let io = crate::filesystem::PreparationIo::from_budget(
                acyclic_fs::performance::work_budget(100_000, 16_000_000),
                &cancellation,
            )?;
            let (_file, _hold) = storage
                .publisher
                .stage_once(
                    OperationId::new(),
                    &path,
                    b"small input",
                    "text/plain",
                    "input.txt",
                    Some(&io),
                )?
                .await?;
            // Compare steady-state work after the initially shallow trees have filled.
            if index < WARMUP {
                continue;
            }
            total = total
                .checked_add(io.receipt())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if index + 1 == WARMUP + SMALL {
                small = total;
            }
        }
    }
    for (counter, first, last) in [
        ("bytes_copied", small.bytes_copied, total.bytes_copied),
        ("items_examined", small.items_examined, total.items_examined),
        (
            "allocation_operations",
            small.allocation_operations,
            total.allocation_operations,
        ),
    ] {
        assert!(first > 0, "{counter} must observe actual staging work");
        assert!(
            u128::from(last) * SMALL as u128 * 4 <= u128::from(first) * LARGE as u128 * 5,
            "{counter}: work for {SMALL} stages was {first}, for {LARGE} was {last}; {small:?}, {total:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn default_compaction_bounds_small_message_continuation() -> Result<()> {
    const TURNS: u16 = 12;
    let mut storage = continuation_storage(usize::from(TURNS)).await?;
    let model = Arc::new(summary_model(usize::from(TURNS) * 2));
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
    let requests = model.dispatches().len();
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
    assert_eq!(model.dispatches().len(), requests);
    Ok(())
}
