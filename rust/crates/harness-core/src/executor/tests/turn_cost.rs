use super::*;

mod identity;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native_growth;

fn lookup_executor() -> Result<StockExecutor> {
    Ok(uncompacted_executor(
        Model::new("test".into(), "lookup".into(), "1".into(), Value::Null)?,
        Arc::new(scripted_model()),
        ContextPipeline::default(),
        ToolRegistry::default(),
    ))
}

// Count decoded records as well as wall time: scheduling and shared bb2 load
// must not turn a complexity regression into a noisy timing assertion.
#[tokio::test]
async fn journal_lookups_do_not_replay_prefixes_from_one_to_a_thousand_turns() -> Result<()> {
    let executor = lookup_executor()?;
    let execution = StockExecution::ordinary(&executor);
    let journal = Journal::default();
    let operation = OperationId::new();
    let input = TurnInput {
        operation_id: operation,
        input: ModelContent::Text("input".into()),
        selected_context: None,
        max_steps: 1000,
    };
    execution.ensure_started(&journal, &input).await?;
    execution.ensure_started(&journal, &input).await?;
    let started = journal.0.lock().unwrap()[0].clone();
    let mut old_times = Vec::new();
    let mut new_times = Vec::new();
    for step in 0..1_000 {
        journal.0.lock().unwrap().push(ExecutionRecord {
            operation_id: operation,
            sequence: u64::from(step) + 2,
            idempotency_key: format!("step:{step}"),
            event: ExecutionEvent::ModelStopped {
                step,
                purpose: ModelPurpose::Response,
            },
        });
        let before = journal.3.load(Ordering::SeqCst);
        let decoded = journal.7.load(Ordering::SeqCst);
        let start = std::time::Instant::now();
        let (tail, records) = execution
            .model_records(&journal, operation, step, ModelPurpose::Response)
            .await?;
        new_times.push(start.elapsed());
        assert_eq!(tail, u64::from(step) + 2);
        assert_eq!(records.len(), 1);
        // One suffix page plus the tail check, regardless of the prefix length.
        assert_eq!(
            journal.3.load(Ordering::SeqCst) - before,
            2,
            "turn {} rescanned its prefix",
            step + 1
        );
        assert_eq!(
            journal.7.load(Ordering::SeqCst) - decoded,
            1,
            "lookup decoded a historical record"
        );
        let reads = journal.3.load(Ordering::SeqCst);
        assert!(
            execution
                .tool_records(&journal, operation, step, "absent")
                .await?
                .1
                .is_empty()
        );
        assert!(
            execution
                .model_records(&journal, operation, step, ModelPurpose::Summary)
                .await?
                .1
                .is_empty()
        );
        assert_eq!(
            execution.prefix_records(&journal, operation, step).await?,
            std::iter::once(started.clone())
                .chain(records.iter().cloned())
                .collect::<Vec<_>>()
        );
        execution.ensure_started(&journal, &input).await?;
        assert_eq!(journal.3.load(Ordering::SeqCst) - reads, 4);
        assert_eq!(
            journal.7.load(Ordering::SeqCst) - decoded,
            1,
            "another lookup decoded the prefix"
        );
        let before = journal.3.load(Ordering::SeqCst);
        let start = std::time::Instant::now();
        let (old_tail, old_records, _) = replay_execution(&journal, operation, 1,
            |event| matches!(event, ExecutionEvent::ModelStopped { step: observed, .. } if *observed == step), None).await?;
        old_times.push(start.elapsed());
        assert_eq!((old_tail, old_records), (tail, records));
        if step == 999 {
            // The old production path fails the very same constant-work bound.
            assert!(journal.3.load(Ordering::SeqCst) - before > 2);
            assert_eq!(journal.7.load(Ordering::SeqCst) - decoded, 1002);
        }
    }
    for range in [0..1, 0..10, 90..100, 990..1000] {
        let mean = |times: &[std::time::Duration]| {
            (times[range.clone()].iter().sum::<std::time::Duration>()
                / u32::try_from(range.len()).unwrap())
            .as_secs_f64()
                * 1e6
        };
        eprintln!(
            "journal lookup turns {}..{}: old={:.1}us indexed={:.1}us",
            range.start + 1,
            range.end,
            mean(&old_times),
            mean(&new_times)
        );
    }
    Ok(())
}

#[tokio::test]
async fn index_checks_new_pages_and_does_not_share_authority_or_operation() -> Result<()> {
    let executor = lookup_executor()?;
    let execution = StockExecution::ordinary(&executor);
    let journal = Journal::default();
    let operation = OperationId::new();
    let record = |sequence, key: &str, operation_id| ExecutionRecord {
        operation_id,
        sequence,
        idempotency_key: key.into(),
        event: ExecutionEvent::ModelStopped {
            step: 0,
            purpose: ModelPurpose::Response,
        },
    };
    journal
        .0
        .lock()
        .unwrap()
        .push(record(1, "first", operation));
    assert_eq!(
        execution
            .model_records(&journal, operation, 0, ModelPurpose::Response)
            .await?
            .0,
        1
    );
    // A duplicate retry identity in a later suffix cannot reuse a validated key.
    journal
        .0
        .lock()
        .unwrap()
        .push(record(2, "first", operation));
    assert!(
        execution
            .model_records(&journal, operation, 0, ModelPurpose::Response)
            .await
            .is_err()
    );
    journal.0.lock().unwrap()[1].idempotency_key = "second".into();
    assert_eq!(
        execution
            .model_records(&journal, operation, 0, ModelPurpose::Response)
            .await?
            .1
            .len(),
        2
    );
    let other = Journal::default();
    assert!(
        execution
            .model_records(&other, operation, 0, ModelPurpose::Response)
            .await?
            .1
            .is_empty()
    );
    let another_operation = OperationId::new();
    journal
        .0
        .lock()
        .unwrap()
        .push(record(1, "first", another_operation));
    assert_eq!(
        execution
            .model_records(&journal, another_operation, 0, ModelPurpose::Response)
            .await?
            .0,
        1
    );
    // Cold recovery can ask for an older step after indexing a later one.
    assert_eq!(
        execution
            .model_records(&journal, operation, 0, ModelPurpose::Response)
            .await?
            .0,
        2
    );
    Ok(())
}

#[tokio::test]
async fn indexed_selectors_preserve_every_event_and_wire_order() -> Result<()> {
    let executor = lookup_executor()?;
    let execution = StockExecution::ordinary(&executor);
    let journal = Journal::default();
    let operation = OperationId::new();
    let file = journal
        .stage(operation, "file", b"{}", "application/json", None)?
        .await?;
    use ExecutionEvent::*;
    // Started deliberately comes last: the index must not reorder even corrupt
    // observations before the caller checks the initial request binding.
    let events = [
        ModelStarted {
            step: 0,
            purpose: ModelPurpose::Response,
            request_digest: [1; 32],
            request: file.clone(),
        },
        ContextPrepared {
            step: 1,
            projection: file.clone(),
            accounting: Some(file.clone()),
        },
        ContextCompacted {
            step: 1,
            projection: file.clone(),
            compaction: file.clone(),
            accounting: file.clone(),
        },
        ModelStarted {
            step: 1,
            purpose: ModelPurpose::Response,
            request_digest: [2; 32],
            request: file.clone(),
        },
        Model {
            step: 1,
            purpose: ModelPurpose::Response,
            event: file.clone(),
        },
        ModelStopped {
            step: 1,
            purpose: ModelPurpose::Response,
        },
        ModelStarted {
            step: 1,
            purpose: ModelPurpose::Summary,
            request_digest: [3; 32],
            request: file.clone(),
        },
        Model {
            step: 1,
            purpose: ModelPurpose::Summary,
            event: file.clone(),
        },
        ModelStopped {
            step: 1,
            purpose: ModelPurpose::Summary,
        },
        ToolStarted {
            step: 1,
            call_id: "a".into(),
            invocation: file.clone(),
        },
        ToolCompleted {
            step: 1,
            call_id: "a".into(),
            result: file.clone(),
            projection: file,
        },
        ToolFailed {
            step: 1,
            call_id: "b".into(),
            reason: ToolFailureKind::Unavailable,
            cause: ToolFailureCause {
                class: ToolErrorClass::Invalid,
                message: "unavailable".into(),
            },
        },
        Started {
            request_digest: [4; 32],
        },
    ];
    for (index, event) in events.into_iter().enumerate() {
        journal.0.lock().unwrap().push(ExecutionRecord {
            operation_id: operation,
            sequence: index as u64 + 1,
            idempotency_key: format!("record:{index}"),
            event,
        });
    }
    for step in [0, 1, 0] {
        for purpose in [ModelPurpose::Response, ModelPurpose::Summary] {
            let indexed = execution
                .model_records(&journal, operation, step, purpose)
                .await?;
            let cold = StockExecution::ordinary(&executor)
                .model_records(&journal, operation, step, purpose)
                .await?;
            assert_eq!(indexed, cold);
        }
        let prefix = execution.prefix_records(&journal, operation, step).await?;
        let expected: Vec<_> = journal
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|record| {
                matches!(record.event, Started { .. })
                    || record.event.step() == Some(step)
                        && !matches!(
                            record.event,
                            ModelStarted {
                                purpose: ModelPurpose::Summary,
                                ..
                            } | Model {
                                purpose: ModelPurpose::Summary,
                                ..
                            } | ModelStopped {
                                purpose: ModelPurpose::Summary,
                                ..
                            }
                        )
            })
            .cloned()
            .collect();
        assert_eq!(prefix, expected);
        for call in ["a", "b", "absent"] {
            let indexed = execution
                .tool_records(&journal, operation, step, call)
                .await?;
            let cold = StockExecution::ordinary(&executor)
                .tool_records(&journal, operation, step, call)
                .await?;
            assert_eq!(indexed, cold);
        }
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_index_custody_is_released_and_a_cancelled_tail_is_not_reused() -> Result<()> {
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    // This exercises the fallback driver cache. The Filesystem journal's
    // persisted index can now answer without retaining a full driver index.
    let replay_only = native_growth::ReplayOnly(journal.as_ref());
    let journal: &dyn ExecutionJournal = &replay_only;
    let operation = OperationId::new();
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    journal
        .append(
            operation,
            "failed",
            &ExecutionEvent::ToolFailed {
                step: 0,
                call_id: "failed-call".into(),
                reason: ToolFailureKind::Unavailable,
                cause: ToolFailureCause {
                    class: ToolErrorClass::Invalid,
                    message: "the failed call retains this diagnostic".into(),
                },
            },
            None,
        )?
        .await?;
    let executor = lookup_executor()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let before = work.live_allocation_bytes();
    {
        let io = PreparationIo::from_native_work(&work)?;
        let execution = StockExecution {
            io: Some(&io),
            ..StockExecution::ordinary(&executor)
        };
        for _ in 0..2 {
            let records = execution.prefix_records(journal, operation, 0).await?;
            assert_eq!(records.len(), 2);
            let output = io.guard_owned(io.owned_bytes()?)?;
            drop(records);
            drop(output);
            let owned = io.owned_bytes()?;
            assert!(
                execution
                    .ensure_started(
                        journal,
                        &TurnInput {
                            operation_id: operation,
                            input: ModelContent::Text("different binding".into()),
                            selected_context: None,
                            max_steps: 1,
                        }
                    )
                    .await
                    .is_err()
            );
            assert_eq!(
                io.owned_bytes()?,
                owned,
                "binding records escaped temporary custody"
            );
        }
        assert!(
            work.live_allocation_bytes() > before,
            "the retained index must own its custody"
        );
        cancellation.cancel();
        assert!(
            execution
                .prefix_records(journal, operation, 0)
                .await
                .is_err()
        );
    }
    assert_eq!(
        work.live_allocation_bytes(),
        before,
        "dropping the driver must release its index"
    );
    Ok(())
}
