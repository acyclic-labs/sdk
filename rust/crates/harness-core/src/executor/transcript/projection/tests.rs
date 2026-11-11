use super::*;

#[tokio::test]
async fn segmented_projection_recovers_accounting_and_checkpoint_with_original_custody()
-> Result<()> {
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), Some(65_536)).await?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    let mut context = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("paused on main".into()),
        }],
        current_input_index: Some(0),
        ..Default::default()
    };
    let checkpoint = stage_json(journal, OperationId::new(), "checkpoint", &context, None).await?;
    context
        .metadata
        .insert("original".into(), checkpoint.clone());
    let prepared = crate::model::PreparedModelRequest::prepare(
        crate::model::ModelRequest {
            summary: false,
            compose_tools: false,
            model: crate::model::Model::new("fixture".into(), "projection".into(), "1".into())?,
            messages: context.messages.clone(),
            tools: Vec::new(),
            sampling: Default::default(),
            max_output_tokens: None,
        },
        Limits::default(),
        None,
    )?;
    for (compacted, counted) in [(false, false), (false, true), (true, false), (true, true)] {
        let accounting = ContextAccounting {
            capacity: crate::context::ModelContextCapacity {
                context_tokens: 65_536,
                output_tokens: Some(4_096),
            },
            count: counted.then(|| {
                crate::context::ModelTokenCount::from_upper_bounds(
                    prepared.request_digest(),
                    0,
                    vec![128],
                )
                .into()
            }),
        };
        let expected_accounting =
            serde_json::to_value(&accounting).map_err(|error| Error::Invalid(error.to_string()))?;
        let projection = ResponseProjection {
            context: crate::context::Context {
                messages: Vec::new(),
                metadata: context.metadata.clone(),
                current_input_index: context.current_input_index,
            },
            steering: None,
            canonical: None,
            checkpoint: compacted.then(|| checkpoint.clone()),
            working: Some(checkpoint.clone()),
            accounting: None,
        };
        let reference = stage_projection(
            journal,
            OperationId::new(),
            "main-projection",
            (&projection, &prepared),
            Some(&accounting),
            None,
            None,
        )
        .await?;
        for native in [false, true] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = if native {
                PreparationIo::from_native_work(&work)?
            } else {
                PreparationIo::from_budget(budget, &cancellation)?
            };
            let (projection, hold) =
                load_projection(journal, &reference, Limits::default(), Some(&io)).await?;
            assert_eq!(projection.context, context);
            assert_eq!(
                projection.checkpoint.as_ref(),
                compacted.then_some(&checkpoint)
            );
            assert_eq!(projection.working.as_ref(), Some(&checkpoint));
            assert_eq!(
                serde_json::to_value(&projection.accounting)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                expected_accounting
            );
            let full_bytes = projection
                .decoded_owned_bytes()?
                .ok_or_else(|| invalid(Some(&io)))?;
            drop(projection);
            drop(hold);
            assert_eq!(
                io.owned_bytes()?,
                0,
                "native={native} compacted={compacted}"
            );
            let continuation =
                ResponseProjection::continuation_metadata(journal, &reference, Some(&io)).await?;
            assert!(continuation.context.messages.is_empty());
            assert_eq!(continuation.context.metadata, context.metadata);
            assert_eq!(
                continuation.context.current_input_index,
                context.current_input_index
            );
            assert_eq!(continuation.working.as_ref(), Some(&checkpoint));
            assert!(continuation.canonical.is_none());
            assert!(continuation.checkpoint.is_none());
            assert!(continuation.accounting.is_none());
            let retained = continuation
                .decoded_owned_bytes()?
                .ok_or_else(|| invalid(Some(&io)))?;
            assert!(retained < full_bytes);
            assert_eq!(io.owned_bytes()?, retained);
            let hold = io.guard_owned(retained)?;
            drop(continuation);
            drop(hold);
            assert_eq!(io.owned_bytes()?, 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn projection_append_writes_only_new_content_and_metadata_reads_skip_history() -> Result<()> {
    let failed = |error: &dyn std::fmt::Display| Error::Storage(error.to_string());
    acyclic_native_runtime::initialize_native_io_services().map_err(|error| failed(&error))?;
    let directory = tempfile::tempdir().map_err(|error| failed(&error))?;
    let objects = acyclic_objects::LocalObjects::open(directory.path(), Default::default(), None)
        .await
        .map_err(|error| failed(&error))?;
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let filesystem = acyclic_fs::Fs::new(
        acyclic_fs::MemoryAuthorityBackend::new(stream.clone()),
        acyclic_fs::LogicalObjectStore::new(Arc::new(objects)),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let storage = crate::filesystem::MemoryHarnessStorage::from_providers(
        crate::AgentId::new(),
        16 * 1024 * 1024,
        filesystem,
        stream,
    )
    .await?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    let operation = OperationId::new();
    let mut previous = None;
    let mut request = None;
    let mut steering = Vec::new();
    let mut metadata_peak = None;
    let mut first_write = None;
    let mut first_log_write = None;
    let mut first_metadata_read = None;
    for history in [1, 8, 64, 512] {
        let messages = |start| {
            (start..history).map(|index| ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(format!("tool output {index}: {}", "x".repeat(4096))),
            })
        };
        let prepared = match request.take() {
            Some(request) => {
                let request: crate::model::PreparedModelRequest = request;
                let start = request.request().messages.len();
                let mut continued = request.into_continuation();
                continued.messages().extend(messages(start));
                continued.finish(Limits::default(), None)?
            }
            None => crate::model::PreparedModelRequest::prepare(
                ModelRequest {
                    summary: false,
                    compose_tools: false,
                    model: Model::new("fixture".into(), "history".into(), "1".into())?,
                    messages: messages(0).collect(),
                    tools: Vec::new(),
                    sampling: Default::default(),
                    max_output_tokens: Some(4096),
                },
                Limits::default(),
                None,
            )?,
        };
        steering.extend((steering.len()..history).map(|index| ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text(format!("steer {index}: {}", "s".repeat(1024))),
        }));
        let mut projection = ResponseProjection {
            context: crate::context::Context {
                current_input_index: Some(0),
                ..Default::default()
            },
            steering: Some(crate::agent::SteeringSnapshot {
                through: history as u64,
                messages: steering.clone(),
            }),
            canonical: None,
            checkpoint: None,
            working: None,
            accounting: None,
        };
        let base = stage_projection(
            journal,
            operation,
            &format!("context:{history}"),
            (&projection, &prepared),
            None,
            previous.as_ref(),
            None,
        )
        .await?;
        let mut continued = prepared.into_continuation();
        continued.messages().push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("appended tool output".repeat(200)),
        });
        let prepared = continued.finish(Limits::default(), None)?;
        let snapshot = projection.steering.as_mut().unwrap();
        snapshot.through += 1;
        snapshot.messages.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("appended steering".repeat(50)),
        });
        let accounting = ContextAccounting {
            capacity: crate::context::ModelContextCapacity {
                context_tokens: u32::MAX,
                output_tokens: Some(4096),
            },
            count: Some(
                crate::context::ModelTokenCount::from_upper_bounds(
                    prepared.request_digest(),
                    1,
                    vec![1; prepared.request().messages.len()],
                )
                .into(),
            ),
        };
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 16 * 1024 * 1024);
        let io = PreparationIo::from_budget(budget, &cancellation)?;
        let log_length = || {
            std::fs::metadata(directory.path().join("log"))
                .map(|metadata| metadata.len())
                .map_err(|error| failed(&error))
        };
        let before = log_length()?;
        let reference = stage_projection(
            journal,
            operation,
            &format!("context:{history}:append"),
            (&projection, &prepared),
            Some(&accounting),
            Some(&base),
            Some(&io),
        )
        .await?;
        let written = io.receipt().object_bytes_written;
        assert_eq!(io.owned_bytes()?, reference.logical_owned_bytes()?);
        let log_written = log_length()? - before;
        let new_bytes =
            crate::contract::canonical_json_bytes(prepared.request().messages.last().unwrap())?
                .len()
                + crate::contract::canonical_json_bytes(
                    projection
                        .steering
                        .as_ref()
                        .unwrap()
                        .messages
                        .last()
                        .unwrap(),
                )?
                .len();
        // Compare identical appends, including the real filesystem's page and
        // authority overhead. A 512-fold history increase must not change their
        // order of growth; an extra tree-page split may double fixed overhead.
        assert!(
            written >= new_bytes as u64 && written <= 2 * *first_write.get_or_insert(written),
            "history={history} written={written}"
        );
        assert!(
            log_written > 0 && log_written <= 2 * *first_log_write.get_or_insert(log_written),
            "history={history} log_written={log_written}"
        );
        if history == 512 {
            assert!(
                prepared.bytes().len() as u64 > 2 * first_write.unwrap(),
                "the former full-snapshot writer must violate the same oracle"
            );
        }
        eprintln!(
            "projection_append history={history} new_bytes={new_bytes} object_bytes_written={written} objects_log_written={log_written}"
        );

        let metadata_io = PreparationIo::from_budget(budget, &cancellation)?;
        let metadata =
            ResponseProjection::continuation_metadata(journal, &reference, Some(&metadata_io))
                .await?;
        assert!(metadata.context.messages.is_empty());
        let read = metadata_io.receipt().object_bytes_read;
        assert!(
            read > 0 && read <= 2 * *first_metadata_read.get_or_insert(read),
            "history={history} metadata_read={read}"
        );
        let peak = metadata_io.receipt().peak_allocation_bytes;
        assert!(
            peak <= *metadata_peak.get_or_insert(peak) + 4096,
            "history={history} metadata_peak={peak}"
        );
        let hold = metadata_io.guard_owned(metadata.decoded_owned_bytes()?.unwrap())?;
        drop(metadata);
        drop(hold);
        assert_eq!(metadata_io.owned_bytes()?, 0);
        let stored = load_projection_record(journal, &reference, None).await?;
        let restored = load_request(journal, &stored.request, Limits::default(), None).await?;
        assert_eq!(restored.bytes(), prepared.bytes());
        let restored_steering = ResponseProjection::steering_snapshot(journal, &reference)
            .await?
            .unwrap();
        let snapshot = projection.steering.as_ref().unwrap();
        assert_eq!(restored_steering.through, snapshot.through);
        assert_eq!(restored_steering.messages, snapshot.messages);
        steering = projection.steering.take().unwrap().messages;
        previous = Some(reference);
        request = Some(prepared);
    }
    Ok(())
}
