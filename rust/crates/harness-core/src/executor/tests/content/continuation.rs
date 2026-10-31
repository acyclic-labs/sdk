use super::*;

pub(super) fn request(count: usize) -> Result<crate::model::ModelRequest> {
    Ok(crate::model::ModelRequest {
        model: Model::new("test".into(), "content-admission".into(), "1".into())?,
        messages: (0..count)
            .map(|_| ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("retained text".into()),
            })
            .collect(),
        tools: Vec::new(),
        max_output_tokens: None,
    })
}

fn executor() -> Result<StockExecutor> {
    Ok(uncompacted_executor(
        request(1)?.model,
        Arc::new(ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    ))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn content_admission_rechecks_only_new_text_and_keeps_scope_binding() -> Result<()> {
    for native in [false, true] {
        let mut expected = None;
        let mut full_work = Vec::new();
        for count in [1, 10, 100, 1_000] {
            let storage =
                crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
            let journal = storage.journal();
            let stock = executor()?;
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = if native {
                PreparationIo::from_native_work(&work)?
            } else {
                PreparationIo::from_budget(budget, &cancellation)?
            };
            let execution = StockExecution {
                io: Some(&io),
                ..StockExecution::ordinary(&stock)
            };
            let mut prepared = crate::model::PreparedModelRequest::prepare(
                crate::model::clone_request(&request(count)?, Some(&io))?,
                stock.limits,
                Some(&io),
            )?;
            execution
                .verify_model_request_content(journal.as_ref(), &mut prepared)
                .await?;
            let mut continuation = prepared.into_continuation();
            stock_vec_push(
                continuation.messages(),
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text(stock_string_copy("new text", Some(&io))?),
                },
                Some(&io),
            )?;
            let mut prepared = continuation.finish(stock.limits, Some(&io))?;
            let before = io.receipt();
            execution
                .verify_model_request_content(journal.as_ref(), &mut prepared)
                .await?;
            let after = io.receipt();
            let incremental = (
                after.items_examined - before.items_examined,
                after.backend_read_operations - before.backend_read_operations,
                after.authority_bytes_read - before.authority_bytes_read,
                after.object_bytes_read - before.object_bytes_read,
                after.source_bytes_read - before.source_bytes_read,
                after.bytes_hashed - before.bytes_hashed,
            );
            assert_eq!(
                *expected.get_or_insert(incremental),
                incremental,
                "content admission revisited historical text at {count}, native={native}"
            );
            // A different executor cannot inherit the previous scope's shape
            // admission, even with the same request and journal values.
            let rebound = StockExecution {
                io: Some(&io),
                ..StockExecution::ordinary(&stock)
            };
            let before = io.receipt();
            rebound
                .verify_model_request_content(journal.as_ref(), &mut prepared)
                .await?;
            let full = io.receipt().items_examined - before.items_examined;
            full_work.push(full);
            eprintln!(
                "CONTINUATION_CONTENT native={native} messages={count} incremental={incremental:?} rebound_items={full}"
            );
        }
        assert!(
            full_work.windows(2).all(|pair| pair[1] > pair[0]),
            "rebound scope did not revalidate historical text"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_continued_request_reauthorizes_old_file_messages() -> Result<()> {
    let journal = Journal::default();
    let operation = OperationId::new();
    let file = journal
        .stage(operation, "input", b"original", "text/plain", None)?
        .await?;
    let mut input = request(1)?;
    input.messages.insert(
        0,
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Part(ModelContentPart::File {
                file: file.clone(),
                policy: crate::model::FileProjectionPolicy::Reference,
            }),
        },
    );
    let stock = executor()?;
    let execution = StockExecution::ordinary(&stock);
    let mut prepared = crate::model::PreparedModelRequest::prepare(input, stock.limits, None)?;
    execution
        .verify_model_request_content(&journal, &mut prepared)
        .await?;
    let reads = journal.4.load(Ordering::SeqCst);
    let mut continuation = prepared.into_continuation();
    continuation.messages().push(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text("next".into()),
    });
    let mut prepared = continuation.finish(stock.limits, None)?;
    execution
        .verify_model_request_content(&journal, &mut prepared)
        .await?;
    assert_eq!(
        journal.4.load(Ordering::SeqCst),
        reads + 1,
        "continued request skipped old file authority"
    );
    {
        let mut staged = journal.1.lock().unwrap();
        let (_, bytes) = staged
            .get_mut(&format!("{operation}:input"))
            .ok_or_else(|| Error::Invalid("fixture input disappeared".into()))?;
        *bytes = b"modified".to_vec().into();
    }
    let mut continuation = prepared.into_continuation();
    continuation.messages().push(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text("after mutation".into()),
    });
    let mut prepared = continuation.finish(stock.limits, None)?;
    assert!(
        execution
            .verify_model_request_content(&journal, &mut prepared)
            .await
            .is_err(),
        "continued request skipped old file authority"
    );
    Ok(())
}
