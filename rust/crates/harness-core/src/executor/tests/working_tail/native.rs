use super::*;

struct CutFixture {
    storage: crate::filesystem::MemoryHarnessStorage,
    executor: StockExecutor,
    reference: FileRef,
}

async fn private_cut() -> Result<CutFixture> {
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let (provider, _) = counted_model(65_536, None, [], []);
    let executor = StockExecutor::new(
        Model::new("fixture".into(), "working-cut".into(), "1".into())?,
        provider,
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("current question".into()),
        selected_context: None,
        max_steps: 3,
    };
    let source = crate::context::Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::User,
                content: input.input.clone(),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("old ".repeat(12_000)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("recent".into()),
            },
        ],
        current_input_index: Some(0),
        ..Default::default()
    };
    let summarized = crate::context::Context {
        messages: source
            .messages
            .get(..2)
            .ok_or_else(|| Error::Invalid("missing source".into()))?
            .to_vec(),
        current_input_index: Some(0),
        ..Default::default()
    };
    let summary = crate::context::ContextSummary {
        operation_id: input.operation_id,
        step: 1,
        source_messages: 2,
        source_digest: preparation_digest(&summarized, None)?,
        output: journal
            .stage(
                input.operation_id,
                "fixture:summary",
                b"bounded summary",
                "text/plain",
                None,
            )?
            .await?,
    };
    let (retained, proof) = crate::context::DurableContextProvider::compact(
        &source,
        3,
        Some(summary),
        &crate::context::CompactionRetention::default(),
        None,
    )?;
    let proof = stage_json(
        journal.as_ref(),
        input.operation_id,
        "fixture:proof",
        &proof,
        None,
    )
    .await?;
    let reference = StockExecution::ordinary(&executor)
        .stage_working_cut(
            journal.as_ref(),
            crate::executor::working_tail::WorkingCompaction {
                input: &input,
                step: 1,
                prior: 2,
                source: &source,
                retained: &retained,
                compaction: &proof,
            },
        )
        .await?;
    Ok(CutFixture {
        storage,
        executor,
        reference,
    })
}

#[tokio::test]
async fn private_working_cut_retains_only_original_custody_through_value_drop() -> Result<()> {
    let fixture = private_cut().await?;
    let journal = fixture.storage.journal();
    for native in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let execution = StockExecution {
            io: Some(&io),
            ..StockExecution::ordinary(&fixture.executor)
        };
        let retained = execution
            .load_working_cut(journal.as_ref(), &fixture.reference)
            .await?;
        let held = if native {
            work.live_allocation_bytes()
        } else {
            io.owned_bytes()?
        };
        assert!(
            held > baseline,
            "resident working cut has no original funding"
        );
        drop(retained);
        assert_eq!(
            io.owned_bytes()?,
            0,
            "working cut retained decode scratch or lost its value guard"
        );
        drop(execution);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_private_working_cut_load_releases_all_original_custody() -> Result<()> {
    let fixture = private_cut().await?;
    let journal = fixture.storage.journal();
    for native in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        cancellation.cancel();
        let execution = StockExecution {
            io: Some(&io),
            ..StockExecution::ordinary(&fixture.executor)
        };
        let result = execution
            .load_working_cut(journal.as_ref(), &fixture.reference)
            .await;
        assert!(
            result.is_err(),
            "cancelled cut acquired new resident authority"
        );
        drop(result);
        drop(execution);
        assert_eq!(io.owned_bytes()?, 0);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
