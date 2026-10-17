use super::*;

const MAX_TASK_RUNTIME_FUTURE_BYTES: usize = 4096;

#[tokio::test]
async fn stock_model_deadline_preserves_original_attempt_and_never_dispatches_expired_work()
-> Result<()> {
    for expired_before_dispatch in [true, false] {
        let fixture = Fixture::new().await?;
        let runtime = fixture.runtime(None).await?;
        let lease = fixture.admit(&runtime).await?;
        let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
        runtime.task_host().start_task(&lease, &mut None).await?;
        let fence = LeaseFence::from(&lease.reservation);
        let turn = OperationId::from_bytes([106; 16]);
        let model = Model::new(
            "synthetic".into(),
            "deadline".into(),
            "1".into(),
            Value::Null,
        )?;
        let provider = Arc::new(PendingModel {
            clock: fixture.clock.clone(),
            generated: AtomicUsize::new(0),
            reconciled: AtomicUsize::new(0),
        });
        let execution = runtime
            .stock_execution(
                task,
                fence.clone(),
                turn,
                model.clone(),
                provider.clone(),
                ContextPipeline::default(),
                fixture.tools.clone(),
                None,
            )
            .await?
            .with_compaction_policy(crate::context::CompactionPolicy::Disabled);
        let input = TurnInput {
            operation_id: execution.operation_id(),
            input: crate::model::ModelContent::Text("question".into()),
            selected_context: None,
            max_steps: 1,
        };
        if expired_before_dispatch {
            fixture.clock.0.store(20, Ordering::SeqCst);
        }
        assert!(matches!(execution.execute(input.clone()).await,
            Err(Error::Indeterminate(operation)) if operation == input.operation_id));
        assert_eq!(
            provider.generated.load(Ordering::SeqCst),
            usize::from(!expired_before_dispatch)
        );
        let state = fixture.scheduler_state(task).await?;
        assert!(state.cancellation_requested);
        assert_eq!(state.reservation, Some(lease.reservation.clone()));
        assert!(state.outcome.is_none());
        assert!(
            runtime
                .task_host()
                .verify_dispatch_owner(task, &fence, None)?
                .await
                .is_err()
        );
        drop(execution);
        drop(runtime);
        let reopened = fixture.runtime(None).await?;
        let execution = reopened
            .stock_execution(
                task,
                fence.clone(),
                turn,
                model,
                provider.clone(),
                ContextPipeline::default(),
                fixture.tools.clone(),
                None,
            )
            .await?
            .with_compaction_policy(crate::context::CompactionPolicy::Disabled);
        if expired_before_dispatch {
            assert!(matches!(
                execution.reconcile(input, 1_000).await,
                Err(Error::Indeterminate(_))
            ));
            assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
        } else {
            assert!(execution.reconcile(input, 1_000).await?.is_some());
            assert_eq!(provider.reconciled.load(Ordering::SeqCst), 1);
            assert!(
                reopened
                    .execution_journal(task, fence, turn)
                    .await?
                    .is_quiescent(None)
                    .await?
            );
        }
        assert_eq!(
            provider.generated.load(Ordering::SeqCst),
            usize::from(!expired_before_dispatch)
        );
        assert!(fixture.scheduler_state(task).await?.outcome.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn native_stock_model_original_lost_ack_uses_native_provider_and_settlement() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.machine.native.store(true, Ordering::SeqCst);

    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let model = Model::new(
        "synthetic".into(),
        "deadline".into(),
        "1".into(),
        Value::Null,
    )?;
    let compaction = crate::context::CompactionPolicy::Disabled;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work =
        acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let opening = fixture.runtime(Some(&io));
    let bytes = std::mem::size_of_val(&opening);
    eprintln!("native runtime open future: {bytes} bytes");
    assert!(
        bytes <= MAX_TASK_RUNTIME_FUTURE_BYTES,
        "native runtime open future is {bytes} bytes"
    );
    let runtime = opening.await?;
    let input = super::super::super::ModelTaskCommand {
        summary_source: None,
        context_manifest: super::manifest::manifest(&fixture, true, "").await?.0,
        input: crate::model::ModelContent::Text("question".into()),
        selected_context: None,
        max_steps: 1,
        max_output_tokens: None,
        selected_output_volume: None,
        mailbox_operation: None,
    };
    let (lease, custody) = fixture
        .native_lease_command(
            &runtime,
            super::super::super::MODEL_TASK_COMMAND_KIND,
            serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?,
            &work,
        )
        .await?;
    let original = lease.clone();
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let commands = runtime.commands().with_model(
        &model,
        provider.clone(),
        &compaction,
        Some(&super::super::super::PreparationIo::from_native_work(
            &work,
        )?),
    )?;
    let recovery = runtime.resume_task_with_native_work(lease, custody, &commands, 2, &work);
    let bytes = std::mem::size_of_val(&recovery);
    eprintln!("native resume future: {bytes} bytes");
    assert!(
        bytes <= MAX_TASK_RUNTIME_FUTURE_BYTES,
        "native resume future is {bytes} bytes"
    );
    let (attempt, custody) = recovery.await?;
    let lease = match attempt {
        TaskWorkerAttempt::Unresolved {
            lease,
            error: Error::Indeterminate(_),
        } => {
            assert_eq!(lease, original);
            lease
        }
        TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
        _ => {
            return Err(Error::Invalid(
                "native model lost acknowledgement did not preserve original lease".into(),
            ));
        }
    };
    assert_eq!(provider.generated.load(Ordering::SeqCst), 1);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    let before = work.receipt();
    drop(commands);
    drop(runtime);
    let opening = fixture.runtime(Some(&io));
    let bytes = std::mem::size_of_val(&opening);
    eprintln!("native runtime reopen future: {bytes} bytes");
    assert!(
        bytes <= MAX_TASK_RUNTIME_FUTURE_BYTES,
        "native runtime reopen future is {bytes} bytes"
    );
    let reopened = opening.await?;
    let commands = reopened.commands().with_model(
        &model,
        provider.clone(),
        &compaction,
        Some(&super::super::super::PreparationIo::from_native_work(
            &work,
        )?),
    )?;
    let recovery =
        reopened.reconcile_task_with_native_work(lease, custody, &commands, 1, 1_000, &work);
    let bytes = std::mem::size_of_val(&recovery);
    eprintln!("native reconciliation future: {bytes} bytes");
    assert!(
        bytes <= MAX_TASK_RUNTIME_FUTURE_BYTES,
        "native reconciliation future is {bytes} bytes"
    );
    let (attempt, custody) = recovery.await?;
    if let TaskWorkerAttempt::Unresolved { error, .. } = attempt {
        return Err(error);
    }
    assert!(
        matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task: completed }) if completed == task)
    );
    assert_eq!(provider.generated.load(Ordering::SeqCst), 1);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    assert!(work.receipt().backend_read_operations > before.backend_read_operations);
    assert!(custody.work().shares_ledger(&work));
    assert!(matches!(
        reopened.task_host().outcome(task).await?,
        Some(Outcome::Cancelled)
    ));
    // The worker has settled on its original ledger. Passive journal inspection
    // is a separate bounded query, not additional work for the completed worker.
    let settled_receipt = work.receipt();
    let inspection =
        acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let replay_io = crate::executor::PreparationIo::from_native_work(&inspection)?;
    let owner = reopened
        .task_host()
        .original_journal_owner(
            task,
            OperationId::from_bytes(task.into_bytes()),
            &LeaseFence::from(&original.reservation),
            &replay_io,
        )
        .await?;
    assert!(
        owner
            .verify(false, &mut Some(&mut replay_io.history_io()))
            .await
            .is_err()
    );
    let witness = crate::filesystem::FilesystemWorkflowJournal::for_task(
        owner.into_native_shared(&inspection)?,
        fixture.filesystem.clone(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &fixture.volume,
            verifier: &fixture.issuer.verifier(&mut None)?,
            scope: &fixture.signed,
            maximum_bytes: 65_536.min(fixture.scope.limits().file_bytes),
            io: Some(&replay_io),
        },
    )?;
    let records =
        crate::workflow::WorkflowJournal::replay(witness.as_ref(), 0, 64, Some(&replay_io))?
            .await?;
    assert!(!records.0.is_empty());
    assert!(
        records
            .1
            .as_ref()
            .unwrap()
            .work()
            .shares_ledger(&inspection)
    );
    assert!(!records.1.as_ref().unwrap().work().shares_ledger(&work));
    assert_eq!(work.receipt(), settled_receipt);
    drop(records);
    drop(witness);
    drop(attempt);
    drop(custody);
    Ok(())
}

#[tokio::test]
async fn native_model_selected_writer_requires_original_created_binding_before_provider()
-> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.machine.native.store(true, Ordering::SeqCst);

    let input = super::super::super::ModelTaskCommand {
        summary_source: None,
        context_manifest: super::manifest::manifest(&fixture, true, "").await?.0,
        input: crate::model::ModelContent::Text("must not dispatch".into()),
        selected_context: None,
        max_steps: 1,
        max_output_tokens: None,
        selected_output_volume: Some(fixture.volume.clone()),
        mailbox_operation: None,
    };
    let payload = serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?;
    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let model = Model::new(
        "synthetic".into(),
        "deadline".into(),
        "1".into(),
        Value::Null,
    )?;
    let compaction = crate::context::CompactionPolicy::Disabled;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work =
        acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let runtime = fixture.runtime(Some(&io)).await?;
    let commands = runtime.commands().with_model(
        &model,
        provider.clone(),
        &compaction,
        Some(&super::super::super::PreparationIo::from_native_work(
            &work,
        )?),
    )?;
    let (lease, custody) = fixture
        .native_lease_command(
            &runtime,
            super::super::super::MODEL_TASK_COMMAND_KIND,
            payload,
            &work,
        )
        .await?;
    let (attempt, custody) = runtime
        .resume_task_with_native_work(lease, custody, &commands, 2, &work)
        .await?;
    assert!(matches!(
        &attempt,
        TaskWorkerAttempt::Unresolved {
            error: Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                ..
            },
            ..
        }
    ));
    assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
    assert!(custody.work().shares_ledger(&work));
    drop(attempt);
    drop(custody);
    Ok(())
}

#[tokio::test]
async fn native_stock_model_configuration_denies_before_copy_or_provider_on_original_work()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let model = Model::new(
        "synthetic".into(),
        "original-config".into(),
        "1".into(),
        serde_json::json!({"configured": true}),
    )?;
    let compaction = crate::context::CompactionPolicy::Disabled;
    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    for cancelled in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
            ..Default::default()
        };
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        if cancelled {
            cancellation.cancel();
        }
        let before = work.receipt();
        let result =
            runtime
                .commands()
                .with_model(&model, provider.clone(), &compaction, Some(&io));
        assert!(result.as_ref().is_err_and(Error::is_native_failure));
        assert_eq!(
            work.receipt().allocation_operations,
            before.allocation_operations
        );
        assert_eq!(
            work.receipt().backend_read_operations,
            before.backend_read_operations
        );
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
        assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
        drop(result);
    }
    Ok(())
}

#[tokio::test]
async fn native_stock_model_configuration_retains_original_custody_and_rejects_foreign_rebinding()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let model = Model::new(
        "synthetic".into(),
        "original-config".into(),
        "1".into(),
        json!({"configured": "retained"}),
    )?;
    let compaction = crate::context::CompactionPolicy::Disabled;
    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let cancellation = acyclic_fs::CancellationToken::new();
    let work =
        acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let before = work.receipt();
    let commands = runtime.commands().with_model(
        &model,
        provider.clone(),
        &compaction,
        Some(&super::super::super::PreparationIo::from_native_work(
            &work,
        )?),
    )?;
    assert!(work.receipt().allocation_operations > before.allocation_operations);
    assert!(work.receipt().bytes_copied > before.bytes_copied);
    let foreign =
        acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let before = foreign.receipt();
    let result = commands.with_model(
        &model,
        provider.clone(),
        &compaction,
        Some(&super::super::super::PreparationIo::from_native_work(
            &foreign,
        )?),
    );
    assert!(matches!(
        result,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(foreign.receipt(), before);
    assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn genuine_stock_native_context_preserves_mcp_current_and_settlement_source() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    let cancellation = acyclic_fs::CancellationToken::new();
    let work =
        acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let runtime = fixture.runtime(Some(&io)).await?;
    let (lease, recovery_custody) = fixture.native_lease(&runtime, &work).await?;
    runtime
        .task_host()
        .start_task(
            &lease,
            &mut Some(&mut crate::filesystem::PreparationIo::from_native_work(&work)?.history_io()),
        )
        .await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    let shared = runtime
        .task_context_with_native_work(task, &fence, &work)
        .await?;
    assert!(shared.original_task_binding_matches(task, &fence));
    let operation = lease.operation.operation_id;
    let call_id = work.display(&operation)?;
    let tool = ToolContext::new(
        Arc::clone(&shared),
        operation,
        &call_id.0,
        Some(&PreparationIo::from_native_work(&work)?),
    )?;
    drop(call_id);
    tool.validate_native_mcp_context(&work)?;
    tool.validate_native_mcp_reconciliation_context(&work)?;
    let read = runtime
        .harness()
        .durable_context(
            task,
            operation,
            Some(&crate::filesystem::PreparationIo::from_native_work(&work)?),
        )
        .await?;
    let read_tool = ToolContext::new(
        read,
        operation,
        "read-context-control",
        Some(&PreparationIo::from_native_work(&work)?),
    )?;
    assert!(matches!(
        read_tool.validate_native_mcp_context(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert!(matches!(
        read_tool.validate_native_mcp_reconciliation_context(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    fixture.clock.0.store(20, Ordering::SeqCst);
    runtime
        .task_host()
        .expire_task_deadline(
            task,
            Some(&fence),
            &mut Some(&mut crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&work)?,
            )),
        )
        .await?;
    let settlement = runtime
        .task_context_for_original_owner_with_native_work(task, &fence, true, &work)
        .await?;
    let settlement_tool = ToolContext::new(
        settlement,
        operation,
        "settlement-context-control",
        Some(&PreparationIo::from_native_work(&work)?),
    )?;
    assert!(matches!(
        settlement_tool.validate_native_mcp_context(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    settlement_tool.validate_native_mcp_reconciliation_context(&work)?;
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    drop(settlement_tool);
    drop(read_tool);
    drop(tool);
    drop(shared);
    drop(lease);
    drop(recovery_custody);
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn ordinary_stock_context_keeps_its_owner_and_rejects_changed_fences(suffix in "[a-z]{1,8}") {
        let executor = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let result: Result<()> = executor.block_on(async {
            let fixture = Fixture::new().await?;
            let runtime = fixture.runtime(None).await?;
            let lease = fixture.admit(&runtime).await?;
            let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
            runtime.task_host().start_task(&lease, &mut None).await?;
            let fence = LeaseFence::from(&lease.reservation);
            let cancellation = acyclic_fs::CancellationToken::new();
            let context = Arc::new(runtime.task_context_with_cancellation(task, fence.clone(), Some(&cancellation)).await?);
            let owner = Arc::clone(context.owned_mailbox.as_ref().unwrap());
            let provider = Arc::new(PendingModel {
                clock: fixture.clock.clone(),
                generated: AtomicUsize::new(0),
                reconciled: AtomicUsize::new(0),
            });
            let model = Model::new("synthetic".into(), "retained".into(), "1".into(), Value::Null)?;
            let mut changed = fence.clone();
            changed.reservation_id.push_str(&suffix);
            assert!(matches!(runtime.stock_execution_with_context(
                Arc::clone(&context), changed.clone(), OperationId::new(), model.clone(), provider.clone(),
                ContextPipeline::default(), fixture.tools.clone(), None,
            ).await, Err(Error::Unauthorized(_))));
            #[cfg(not(target_arch = "wasm32"))]
            {
                let original = context.ordinary_journal_owner::<acyclic_stream::MemoryStream>()?;
                let journal = runtime.workflow_journal_with_context(&context)?;
                assert!(Arc::ptr_eq(&original, journal.owner.as_ref().unwrap()));
                let commands = runtime.commands();
                let command_journal = commands.command_workflow_journal(&context, task, &fence, None).await?;
                assert!(Arc::ptr_eq(&original, command_journal.owner.as_ref().unwrap()));
                assert!(matches!(commands.command_workflow_journal(&context, task, &changed, None).await, Err(Error::Unauthorized(_))));
                assert!(matches!(commands.command_workflow_journal(&context, TaskId::new(), &fence, None).await, Err(Error::Unauthorized(_))));
                assert!(matches!(runtime.harness().open_registered_task(
                    task, &changed, journal.clone(), Some(Arc::clone(&context)),
                ).await, Err(Error::Unauthorized(_))));
                let retained = Arc::strong_count(&owner);
                let session = runtime.harness().open_registered_task(
                    task, &fence, journal, Some(Arc::clone(&context)),
                ).await?;
                assert_eq!(Arc::strong_count(&owner), retained + 1);
                drop(session);
                assert_eq!(Arc::strong_count(&owner), retained);
                let dispatch = crate::model::ModelDispatch { operation_id: OperationId::new(), step: 0, request_digest: [3;32] };
                assert!(matches!(owner.claim_model_dispatch(&context, dispatch.clone(), &changed).await, Err(Error::Unauthorized(_))));
                let narrowed = context.scoped(crate::Capabilities::default(), context.scope().limits())?;
                assert!(matches!(owner.claim_model_dispatch(&narrowed, dispatch.clone(), &fence).await, Err(Error::Unauthorized(_))));
                let other = fixture.runtime(None).await?;
                assert!(matches!(other.task_host().claim_model_from_context(&context, dispatch.clone(), &fence).await, Err(Error::Unauthorized(_))));
                for _ in 0..2 { owner.claim_model_dispatch(&context, dispatch.clone(), &fence).await?; }
                let mut changed_request = dispatch;
                changed_request.request_digest[0] ^= 1;
                assert!(owner.claim_model_dispatch(&context, changed_request, &fence).await.is_err());
            }
            let retained = Arc::strong_count(&owner);
            let execution = runtime.stock_execution_with_context(
                context, fence, OperationId::new(), model, provider.clone(),
                ContextPipeline::default(), fixture.tools.clone(), None,
            ).await?.with_compaction_policy(crate::context::CompactionPolicy::Disabled);
            // Ownership identity, rather than elapsed time, proves composition
            // retained the issued owner instead of reopening its proof chain.
            assert_eq!(Arc::strong_count(&owner), retained);
            assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
            cancellation.cancel();
            let input = TurnInput { operation_id: execution.operation_id(),
                input: crate::model::ModelContent::Text("stopped invocation".into()),
                selected_context: None, max_steps: 1 };
            let stopped = execution.execute(input).await;
            assert!(matches!(stopped,
                Err(Error::InteractionRejected(crate::InteractionRejection::Cancelled))), "{stopped:?}");
            assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
            drop(execution);
            assert_eq!(Arc::strong_count(&owner), retained - 1);
            Ok(())
        });
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}
