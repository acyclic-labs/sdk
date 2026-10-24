use super::*;

struct PublishedCut {
    storage: crate::filesystem::MemoryHarnessStorage,
    executor: StockExecutor,
    input: TurnInput,
    retained: crate::context::Context,
}

async fn published_cut() -> Result<PublishedCut> {
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 20_000).await?;
    let model = Model::new(
        "synthetic".into(),
        "counter".into(),
        "1".into(),
        Value::Null,
    )?;
    let (provider, _) = counted_model(
        20_000,
        None,
        [
            ScriptedTurn::Text("reply".repeat(600)),
            ScriptedTurn::Text("bounded summary".into()),
            ScriptedTurn::Text("done".into()),
        ],
        [],
    );
    let bundle = storage
        .builder()
        .model(model.clone(), provider.clone())
        .grant("model:generate")
        .without_default_tools()
        .compaction(crate::context::CompactionPolicy::Threshold(
            crate::context::ThresholdCompaction {
                response_reserve_tokens: 4_096,
                recent_tokens: 1,
                ..Default::default()
            },
        ))
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
    let journal = storage.journal();
    let reference =
        canonical_checkpoint_for_operation(journal.as_ref(), operation, Limits::default(), None)
            .await?
            .ok_or_else(|| Error::Invalid("fixture did not compact".into()))?;
    let (envelope, retained) =
        load_canonical_checkpoint(journal.as_ref(), &reference, Limits::default(), None, None)
            .await?;
    let source = retain_preparation(
        load_json::<crate::context::Context>(journal.as_ref(), &envelope.source, None).await?,
    )?;
    let current_input = source
        .current_input_index
        .and_then(|index| source.messages.get(index as usize))
        .ok_or_else(|| Error::Invalid("fixture has no current input".into()))?
        .content
        .clone();
    let input = TurnInput {
        operation_id: operation,
        input: current_input,
        selected_context: Some(SelectedModelContext {
            selection: envelope.selection,
            messages: source.messages,
        }),
        max_steps: 2,
    };
    let read = storage
        .volume()
        .capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let executor = StockExecutor::new(
        model,
        provider,
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_tool_authority(
        RuntimeScope::new(Capabilities::new([read]), Limits::default())?,
        None,
    )?;
    Ok(PublishedCut {
        storage,
        executor,
        input,
        retained,
    })
}

#[tokio::test]
async fn finite_and_native_working_cuts_keep_their_original_custody_until_drop() -> Result<()> {
    let fixture = published_cut().await?;
    let journal = fixture.storage.journal();
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    for native in [false, true] {
        println!("working custody native={native}");
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let mut execution = StockExecution {
            io: Some(&io),
            ..StockExecution::ordinary(&fixture.executor)
        };
        let mut input = ContextInput::copy_for_turn(&fixture.input, 1, &[], None)?;
        let context = execution
            .canonical_base_for_turn(journal.as_ref(), fixture.input.operation_id, &mut input)
            .await?;
        assert_eq!(context, fixture.retained);
        // Native guards transfer custody out of `io`, but remain charged to `work`.
        let funded = if native {
            work.live_allocation_bytes()
        } else {
            io.owned_bytes()?
        };
        let cache = execution.working_context.take();
        assert!(
            matches!(&cache, Some(Some(_))),
            "checkpoint was not retained"
        );
        drop(cache);
        let remaining = if native {
            work.live_allocation_bytes()
        } else {
            io.owned_bytes()?
        };
        let result_bytes = io.owned_bytes()?;
        assert!(
            funded > remaining,
            "negative control omitted retained-cache custody"
        );
        assert!(result_bytes > 0, "returned context has no original funding");
        let output = io.guard_owned(result_bytes)?;
        drop(context);
        drop(output);
        assert_eq!(io.owned_bytes()?, 0, "checkpoint decoding retained scratch");
        drop(execution);
        drop(io);
    }
    drop(journal);
    drop(fixture);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn denied_or_cancelled_native_working_cuts_leave_no_cached_authority() -> Result<()> {
    let fixture = published_cut().await?;
    let journal = fixture.storage.journal();
    for cancelled in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = PreparationIo::from_native_work(&work)?;
        let mut executor = fixture.executor.clone();
        if cancelled {
            cancellation.cancel();
        } else {
            executor.tool_scope = RuntimeScope::default();
        }
        let execution = StockExecution {
            io: Some(&io),
            ..StockExecution::ordinary(&executor)
        };
        let mut input = ContextInput::copy_for_turn(&fixture.input, 1, &[], None)?;
        let outcome = execution
            .canonical_base_for_turn(journal.as_ref(), fixture.input.operation_id, &mut input)
            .await;
        assert!(outcome.is_err());
        assert!(execution.working_context.get().is_none());
        drop(outcome);
        drop(execution);
        assert_eq!(io.owned_bytes()?, 0);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
