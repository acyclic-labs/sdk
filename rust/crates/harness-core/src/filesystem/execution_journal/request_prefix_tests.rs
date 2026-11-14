use super::*;
use crate::{
    context::{ContextPipeline, ModelContextCapacity, ModelTokenCount},
    executor::{Executor, StockExecutor, TurnInput},
    model::{Model, ModelContent, ModelMessage, ModelRole},
    test_support::{ScriptedModelProvider, ScriptedTurn},
    tool::ToolRegistry,
};

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
    let reopened = FilesystemExecutionJournal::new(
        Arc::clone(&journal.stream),
        Arc::clone(&journal.host),
        crate::filesystem::FilesystemJournalBinding {
            volume: &journal.volume,
            verifier: &journal.verifier,
            scope: &journal.scope,
            io: None,
        },
    )?;
    Ok(match &journal.traces {
        Some((traces, owner)) => reopened.with_trace_interest(traces.clone(), owner.clone()),
        None => reopened,
    })
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

async fn staged_bytes(
    trace: &acyclic_stream::Stream<acyclic_stream::MemoryStream>,
    from: u64,
) -> Result<u64> {
    let through = trace.tail().await?;
    let count = u32::try_from(through - from)
        .map_err(|_| Error::Invalid("trace read exceeds its count representation".into()))?;
    if count == 0 {
        return Ok(0);
    }
    let mut query = acyclic_stream::trace::TraceQuery::new(
        trace.read(from, count).await?,
        Default::default(),
    );
    let mut total = 0_u64;
    while let Some(observation) = query
        .next()
        .await
        .map_err(|error| Error::Storage(error.to_string()))?
    {
        if let acyclic_stream::trace::Observation::Completed {
            start, bytes, result, ..
        } = observation
            && start.name == "harness.private_stage"
        {
            assert_eq!(result, acyclic_stream::trace::Outcome::Succeeded);
            let bytes = bytes.ok_or_else(|| Error::Invalid("staged byte count absent".into()))?;
            total = total
                .checked_add(bytes)
                .ok_or_else(|| Error::Invalid("staged byte count overflow".into()))?;
        }
    }
    if query.unpaired_finishes() != 0 || query.active().len() != 0 {
        return Err(Error::Invalid("staged work trace is incomplete".into()));
    }
    Ok(total)
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
    let trace = acyclic_stream::Stream::new(Arc::clone(&stream), "traces/request-prefix")?;
    let traces = acyclic_stream::trace::Registry::new(Arc::clone(&stream));
    let owner = crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([71; 16]));
    let _interest = traces.subscribe(
        acyclic_stream::StreamPath::new(owner.as_str())?,
        trace.path().clone(),
    )?;
    let filesystem = acyclic_fs::Fs::new(
        acyclic_fs::MemoryAuthorityBackend::new(stream.clone()),
        acyclic_fs::LogicalObjectStore::new(Arc::new(objects)),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let (mut journal, _issuer) = test_support::journal_with_backends(stream, filesystem).await?;
    journal = journal.with_trace_interest(traces, owner);
    let (executor, provider) = executor()?;
    let log_length = || {
        std::fs::metadata(directory.path().join("log"))
            .map(|metadata| metadata.len())
            .map_err(|error| failed(&error))
    };
    let mut first_payload = None;
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
        let trace_before = trace.tail().await?;
        journal = reopen(&journal)?
            .with_preceding_turn(Some(preceding))
            .await?
            .with_bound_selection(selected.clone());
        let current = input(OperationId::new(), &selected);
        let output = executor.execute(current.clone(), &journal).await?;
        let written = log_length()? - before;
        let payload = staged_bytes(&trace, trace_before).await?;
        if history == 1 {
            assert!(
                staged_bytes(&trace, trace_before + 1).await.is_err(),
                "omitting an original start must not undercount staged work"
            );
        }
        let baseline = *first_payload.get_or_insert(payload);
        first_log_write.get_or_insert(written);
        // The journal boundary counts original staged payloads. The physical
        // log additionally contains namespace, reference and durability frontiers;
        // their cost must not be mistaken for repeated transcript serialization.
        eprintln!(
            "first_step_append history={history} new_bytes={new_bytes} staged_payload_bytes={payload} objects_log_written={written}"
        );
        assert!(
            payload >= new_bytes as u64 && payload <= 2 * baseline,
            "history={history} staged_payload_bytes={payload} baseline={baseline}"
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
            let trace_before = trace.tail().await?;
            let changed = OperationId::new();
            executor
                .execute(input(changed, &selected), &journal)
                .await?;
            assert!(
                staged_bytes(&trace, trace_before).await? > 2 * baseline,
                "a changed prefix must persist its replacement content"
            );
            assert!(
                log_length()? - before > 2 * first_log_write.unwrap(),
                "a changed prefix must not reuse the old message chain"
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
