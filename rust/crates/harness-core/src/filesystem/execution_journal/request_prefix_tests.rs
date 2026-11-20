use super::*;
use crate::{
    context::{ContextPipeline, ModelContextCapacity, ModelTokenCount},
    executor::{Executor, StockExecutor, TurnInput},
    model::{Model, ModelContent, ModelMessage, ModelRole},
    test_support::{ScriptedModelProvider, ScriptedTurn},
    tool::ToolRegistry,
};

fn dump_log(path: &std::path::Path, from: u64) -> Vec<(String, usize)> {
    let bytes = std::fs::read(path).unwrap();
    let mut offset = usize::try_from(from).unwrap();
    let mut out = Vec::new();
    while offset + 8 <= bytes.len() {
        let len = usize::try_from(u64::from_le_bytes(
            bytes[offset..offset + 8].try_into().unwrap(),
        ))
        .unwrap();
        let body = &bytes[offset + 8..offset + 8 + len];
        let op = body[0];
        let klen = u32::from_le_bytes(body[1..5].try_into().unwrap()) as usize;
        let key = String::from_utf8_lossy(&body[5..5 + klen]).to_string();
        let vlen = len - 5 - klen;
        out.push((format!("{op}:{key}"), vlen));
        offset += 8 + len + 32;
    }
    out
}

fn selection(messages: Vec<ModelMessage>) -> crate::projection::SelectedModelContext {
    crate::projection::SelectedModelContext {
        selection: crate::conversation::ModelContextSelection {
            conversation_revision: messages.len() as u64,
            message_ids: (1..=messages.len())
                .map(|id| uuid::Uuid::from_u128(id as u128))
                .collect(),
            checkpoint: None,
        },
        messages,
    }
}

fn input(
    operation_id: OperationId,
    selected: &crate::projection::SelectedModelContext,
) -> TurnInput {
    TurnInput {
        max_steps: None,
        operation_id,
        input: selected.messages.last().unwrap().content.clone(),
        selected_context: Some(selected.clone()),
    }
}

fn reopen<P, A, O>(
    journal: &FilesystemExecutionJournal<P, A, O>,
) -> Result<FilesystemExecutionJournal<P, A, O>>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    FilesystemExecutionJournal::new(
        Arc::clone(&journal.stream),
        Arc::clone(&journal.host),
        crate::filesystem::FilesystemJournalBinding {
            volume: &journal.volume,
            verifier: &journal.verifier,
            scope: &journal.scope,
            io: None,
        },
    )
}

fn executor() -> Result<(StockExecutor, Arc<ScriptedModelProvider>)> {
    let provider = Arc::new(
        ScriptedModelProvider::new((0..9).map(|_| ScriptedTurn::Text("answer".into())))
            .with_context_capacity(ModelContextCapacity {
                context_tokens: u32::MAX,
                output_tokens: Some(64),
                output_shares_context: false,
            })
            .with_token_counter(|request| {
                Ok(ModelTokenCount::from_upper_bounds(
                    request.request_digest(),
                    1,
                    vec![1; request.request().messages.len()],
                ))
            }),
    );
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "prefix".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_compaction_policy(crate::context::CompactionPolicy::Disabled);
    Ok((executor, provider))
}

#[tokio::test]
async fn first_step_reuses_preceding_turn_and_writes_only_appended_content() -> Result<()> {
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
    let (mut journal, _issuer) = test_support::journal_with_backends(stream, filesystem).await?;
    let (executor, provider) = executor()?;
    let log_length = || {
        std::fs::metadata(directory.path().join("log"))
            .map(|metadata| metadata.len())
            .map_err(|error| failed(&error))
    };
    let mut first_log_write = None;
    for history in [1, 8, 64, 512] {
        let mut selected = selection(
            (0..history)
                .map(|id| ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text(format!("message {id}: {}", "x".repeat(4096))),
                })
                .collect(),
        );
        let preceding = OperationId::new();
        journal = journal
            .with_preceding_turn(None)
            .await?
            .with_bound_selection(selected.clone());
        let output = executor
            .execute(input(preceding, &selected), &journal)
            .await?;
        selected.messages.extend(output.transcript);
        selected.messages.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("new content".repeat(400)),
        });
        selected = selection(selected.messages);
        let new_bytes =
            crate::contract::canonical_json_bytes(&selected.messages.get(history..).unwrap())?
                .len();
        let before = log_length()?;
        journal = reopen(&journal)?
            .with_preceding_turn(Some(preceding))
            .await?
            .with_bound_selection(selected.clone());
        let current = input(OperationId::new(), &selected);
        let output = executor.execute(current.clone(), &journal).await?;
        let written = log_length()? - before;
        if history == 1 || history == 512 {
            let frames = dump_log(&directory.path().join("log"), before);
            let mut by_size: std::collections::BTreeMap<usize, usize> =
                std::collections::BTreeMap::new();
            for (_, vlen) in &frames {
                *by_size.entry(*vlen).or_default() += 1;
            }
            eprintln!(
                "frames history={history} count={} total={}",
                frames.len(),
                frames.iter().map(|(_, vlen)| vlen).sum::<usize>()
            );
            for (size, count) in by_size {
                eprintln!("frames history={history} size={size} count={count}");
            }
        }
        let baseline = *first_log_write.get_or_insert(written);
        // Measure the real persistent object log, including namespace and
        // durability records, rather than an obsolete staging span. Appending
        // the same suffix must remain bounded as the preceding history grows.
        eprintln!(
            "first_step_append history={history} new_bytes={new_bytes} objects_log_written={written}"
        );
        assert!(
            written >= new_bytes as u64 && written <= 2 * baseline,
            "history={history} objects_log_written={written} baseline={baseline}"
        );
        assert_eq!(executor.execute(current.clone(), &journal).await?, output);
        journal = reopen(&journal)?
            .with_preceding_turn(Some(current.operation_id))
            .await?;
        let restored = crate::executor::load_model_request(
            &journal,
            journal.preceding_request.as_ref().unwrap(),
            Limits::default(),
            None,
        )
        .await?;
        assert_eq!(restored.request().messages, selected.messages);
        if history == 512 {
            selected.messages[0].content = ModelContent::Text("changed prefix".repeat(400));
            journal = journal.with_bound_selection(selected.clone());
            let before = log_length()?;
            let changed = OperationId::new();
            executor
                .execute(input(changed, &selected), &journal)
                .await?;
            assert!(
                log_length()? > before,
                "a changed prefix must persist its replacement content"
            );
            journal = reopen(&journal)?.with_preceding_turn(Some(changed)).await?;
            let restored = crate::executor::load_model_request(
                &journal,
                journal.preceding_request.as_ref().unwrap(),
                Limits::default(),
                None,
            )
            .await?;
            assert_eq!(restored.request().messages, selected.messages);
        }
    }
    assert_eq!(provider.request_count(), 9);
    Ok(())
}
