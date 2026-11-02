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
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("current question".into()),
        selected_context: None,
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
    let (retained, proof) = crate::context::compact_projection(&source, 3, Some(summary), None)?;
    let (_, reference) = StockExecution::ordinary(&executor)
        .stage_working_cut(
            journal.as_ref(),
            crate::executor::working_tail::WorkingCompaction {
                input: &input,
                step: 1,
                prior: 2,
                source: &source,
                retained: &retained,
                proof: &proof,
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
async fn private_working_cut_transfers_decoded_values_without_copying_them_again() -> Result<()> {
    let fixture = private_cut().await?;
    let journal = fixture.storage.journal();
    let cut = load_json::<Value>(journal.as_ref(), &fixture.reference, None)
        .await?
        .0;
    let context_ref: FileRef = serde_json::from_value(cut["retained"].clone()).unwrap();
    let (first, second): ([u8; 32], [u8; 32]) =
        serde_json::from_value(cut["proof"].clone()).unwrap();
    let mut signature = [0; 64];
    signature[..32].copy_from_slice(&first);
    signature[32..].copy_from_slice(&second);
    let expected = load_json::<crate::context::Context>(journal.as_ref(), &context_ref, None)
        .await?
        .0;
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
    let decode_io = PreparationIo::from_budget(budget, &cancellation)?;
    {
        // Inventory the required reads/decodes, signature payload, retained
        // validation and caller's reference. No output reconstruction is included.
        let scope = crate::filesystem::PreparationScope::new(Some(&decode_io))?;
        let cut = load_json::<crate::executor::working_tail::WorkingCut>(
            journal.as_ref(),
            &fixture.reference,
            scope.io(),
        )
        .await?;
        let digest = cut.0.payload_digest(scope.io())?;
        with_history_io(scope.io(), |work| {
            journal
                .working_checkpoint_authority()
                .unwrap()
                .verify_working_checkpoint(digest, &signature, work)
        })?;
        let context =
            load_json::<crate::context::Context>(journal.as_ref(), &context_ref, scope.io())
                .await?;
        crate::context::validate_projected_context(
            &context.0,
            fixture.executor.limits,
            scope.io(),
        )?;
        let reference = crate::model::clone_file(&fixture.reference, scope.io())?;
        drop(reference);
        drop(context);
        drop(cut);
    }
    assert_eq!(decode_io.owned_bytes()?, 0);
    budget.bytes_copied = decode_io.receipt().bytes_copied;
    let io = PreparationIo::from_budget(budget, &cancellation)?;
    let execution = StockExecution {
        io: Some(&io),
        ..StockExecution::ordinary(&fixture.executor)
    };
    let retained = execution
        .load_working_cut(journal.as_ref(), &fixture.reference)
        .await?;
    assert_eq!(io.receipt().bytes_copied, budget.bytes_copied);
    // Restoring the old reconstruction exceeds the independently inventoried
    // allowance, even though all retained bytes have already been admitted.
    assert!(crate::context::copy_context(&expected, Some(&io)).is_err());
    drop(retained);
    assert_eq!(io.owned_bytes()?, 0);
    Ok(())
}

#[tokio::test]
async fn private_working_cut_retains_only_original_custody_through_value_drop() -> Result<()> {
    let fixture = private_cut().await?;
    let journal = fixture.storage.journal();
    for _native in [
        false,
        #[cfg(not(target_arch = "wasm32"))]
        true,
    ] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
        #[cfg(not(target_arch = "wasm32"))]
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        #[cfg(not(target_arch = "wasm32"))]
        let baseline = work.live_allocation_bytes();
        #[cfg(target_arch = "wasm32")]
        let baseline = 0;
        #[cfg(target_arch = "wasm32")]
        let io = PreparationIo::from_budget(budget, &cancellation)?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = if _native {
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
        #[cfg(target_arch = "wasm32")]
        let held = io.owned_bytes()?;
        #[cfg(not(target_arch = "wasm32"))]
        let held = if _native {
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
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_private_working_cut_load_releases_all_original_custody() -> Result<()> {
    let fixture = private_cut().await?;
    let journal = fixture.storage.journal();
    for _native in [
        false,
        #[cfg(not(target_arch = "wasm32"))]
        true,
    ] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
        #[cfg(not(target_arch = "wasm32"))]
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        #[cfg(not(target_arch = "wasm32"))]
        let baseline = work.live_allocation_bytes();
        #[cfg(target_arch = "wasm32")]
        let io = PreparationIo::from_budget(budget, &cancellation)?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = if _native {
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
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
