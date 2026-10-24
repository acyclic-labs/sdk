use super::*;
use crate::context::{CompactionPolicy, ModelContextCapacity, ThresholdCompaction};
use crate::filesystem::MemoryHarnessStorage;
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};

type Journal = FilesystemExecutionJournal<
    acyclic_stream::MemoryStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

async fn published_summary() -> Result<(MemoryHarnessStorage, Journal, OperationId)> {
    let filesystem = acyclic_fs::Fs::memory();
    let storage = MemoryHarnessStorage::from_providers(
        crate::AgentId::new(),
        20_000,
        filesystem.clone(),
        Arc::new(acyclic_stream::MemoryStream::default()),
    )
    .await?;
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Text("reply".repeat(600)),
            ScriptedTurn::Text("bounded summary".into()),
            ScriptedTurn::Text("done".into()),
        ])
        .with_context_capacity(ModelContextCapacity {
            context_tokens: 20_000,
            output_tokens: Some(4_096),
        })
        .with_token_counter(|request| {
            request
                .serialized_token_upper_bound(None)
                .map(|(count, _)| count)
        }),
    );
    let model = crate::model::Model::new(
        "synthetic".into(),
        "counter".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let bundle = storage
        .builder()
        .model(model, provider)
        .grant("model:generate")
        .without_default_tools()
        .compaction(CompactionPolicy::Threshold(ThresholdCompaction {
            response_reserve_tokens: 4_096,
            recent_tokens: 1,
            ..Default::default()
        }))
        .build()?;
    let first = OperationId::new();
    let old = storage
        .stage(
            first,
            "turns/old.txt",
            &vec![b'q'; 8_000],
            "text/plain",
            "old.txt",
        )
        .await?;
    storage
        .run_conversation(&bundle, first, old, vec![], 1)
        .await?;
    let operation = OperationId::new();
    let current = storage
        .stage(
            operation,
            "turns/current.txt",
            &vec![b'x'; 8_000],
            "text/plain",
            "current.txt",
        )
        .await?;
    storage
        .run_conversation(&bundle, operation, current, vec![], 1)
        .await?;
    let host = FilesystemHost::new(filesystem, storage.volume().provider(), None)?;
    let input = crate::filesystem::FilesystemContentVerifier::new(
        host.clone(),
        &storage.verifier(),
        storage.owner_scope(),
        20_000,
        None,
    )?;
    let journal = FilesystemExecutionJournal::new(
        storage.stream().clone(),
        host,
        &SchemaRegistry::new(),
        crate::filesystem::FilesystemJournalBinding {
            volume: storage.volume(),
            verifier: &storage.verifier(),
            scope: storage.owner_scope(),
            maximum_bytes: 20_000,
            io: None,
        },
    )?
    .with_input_verifier(input);
    Ok((storage, journal, operation))
}

#[test]
fn published_summary_authentication_does_not_scan_later_execution_steps() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let (_storage, concrete, operation) = runtime.block_on(published_summary())?;
    let journal = &concrete;
    let limits = Limits::default();
    let (envelope, source, proof, prepared) = runtime.block_on(async {
        let reference =
            crate::executor::canonical_checkpoint_for_operation(journal, operation, limits, None)
                .await?
                .ok_or_else(|| Error::Invalid("fixture did not compact".into()))?;
        let (envelope, _) =
            crate::executor::load_canonical_checkpoint(journal, &reference, limits, None, None)
                .await?;
        let source =
            crate::executor::load_json::<crate::context::Context>(journal, &envelope.source, None)
                .await?
                .0;
        let proof = crate::executor::load_json::<crate::context::CompactionReference>(
            journal,
            &envelope.compaction,
            None,
        )
        .await?
        .0;
        let records = journal.replay(operation, 0, 128, None)?.await?;
        let prepared = records
            .into_iter()
            .find_map(|record| {
                matches!(
                    record.event,
                    ExecutionEvent::ContextPrepared { step: 0, .. }
                )
                .then_some(record.event)
            })
            .ok_or_else(|| Error::Invalid("fixture has no prepared response".into()))?;
        Ok::<_, Error>((envelope, source, proof, prepared))
    })?;
    let mut samples = Vec::new();
    for step in 1..=1_000 {
        let ExecutionEvent::ContextPrepared {
            projection,
            accounting,
            ..
        } = &prepared
        else {
            return Err(Error::Invalid("fixture response changed".into()));
        };
        runtime.block_on(async {
            journal
                .append(
                    operation,
                    &format!("later:{step}"),
                    &ExecutionEvent::ContextPrepared {
                        step,
                        projection: projection.clone(),
                        accounting: accounting.clone(),
                    },
                    None,
                )?
                .await
        })?;
        if ![1, 10, 100, 1_000].contains(&step) {
            continue;
        }
        let started = std::time::Instant::now();
        let (result, trace) =
            acyclic_test_support::differential::observe::<_, _, (&'static str, u64)>(
                || (),
                |()| {
                    runtime.block_on(journal.verify_canonical_summary_with_io(
                        operation, &envelope, &source, &proof, limits, None,
                    ))
                },
            );
        result?;
        let decoded: u64 = trace
            .iter()
            .filter(|(name, _)| *name == "execution-record-decoded")
            .map(|(_, amount)| *amount)
            .sum();
        println!(
            "canonical summary later_steps={step} decoded={decoded} time_us={:.3}",
            started.elapsed().as_secs_f64() * 1e6
        );
        samples.push(decoded);
    }
    assert!(
        samples.windows(2).all(|pair| pair[0] == pair[1]),
        "canonical summary authentication rescanned later history: {samples:?}"
    );
    Ok(())
}
