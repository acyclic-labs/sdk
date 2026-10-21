use super::*;
use crate::context::CompactionPolicy;

struct PrivateVolume(VolumeRef);

impl crate::conversation::ContentPublisher for PrivateVolume {
    fn volume(&self) -> &VolumeRef {
        &self.0
    }
}

struct NoReads;
impl crate::conversation::ContentResidencyVerifier for NoReads {}

/// An admitted task context, with or without an owner-bound private writer,
/// and the scope that grants writes to it.
async fn admitted(writer: bool) -> Result<(crate::runtime::TaskContext, RuntimeScope)> {
    let volume = VolumeRef::new(
        ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([7; 16])),
        &mut None,
    )?;
    let write = volume.capability(acyclic_fs::access::VolumeOperation::Write, &mut None)?;
    let scope = RuntimeScope::new(
        Capabilities::new([
            "task:spawn:context-probe@1".to_owned(),
            "model:generate".to_owned(),
            write,
        ]),
        Limits::default(),
    )?;
    let captured = Arc::new(Mutex::new(None));
    let sink = captured.clone();
    let mut tasks = crate::runtime::TaskRegistry::default();
    tasks.register(crate::runtime::TaskDefinition::live(
        "context-probe",
        "1",
        move |context, _: ()| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(context);
                Ok(())
            }
        },
    )?)?;
    let harness = crate::runtime::Bindings {
        tasks,
        tools: ToolRegistry::default(),
        scope: scope.clone(),
        concurrency: 1,
        artifacts: writer.then(|| crate::runtime::ContentBindings {
            reader: Arc::new(NoReads),
            writer: Some(Arc::new(PrivateVolume(volume))),
        }),
        ..crate::runtime::Bindings::local()
    }
    .build(None)?;
    let definition = harness.task::<(), ()>("context-probe@1")?;
    assert!(matches!(
        harness.spawn(&definition, ()).await?.result().await?,
        Outcome::Succeeded(())
    ));
    let context = captured.lock().unwrap().take();
    Ok((
        context.ok_or_else(|| Error::NotFound("admitted context".into()))?,
        scope,
    ))
}

fn executor(
    (context, scope): &(crate::runtime::TaskContext, RuntimeScope),
    operation: OperationId,
    policy: Option<CompactionPolicy>,
) -> Result<StockExecutor> {
    let executor = StockExecutor::new(
        Model::new("test".into(), "resolver".into(), "1".into(), Value::Null)?,
        Arc::new(ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    // The execution's context is narrowed to this authority.
    .with_tool_authority(scope.clone(), None)?
    .with_task_context(context, operation)?;
    Ok(match policy {
        Some(policy) => executor.with_compaction_policy(policy),
        None => executor,
    })
}

fn native_work() -> Result<acyclic_fs::NativeMaterializationWork> {
    acyclic_fs::NativeMaterializationWork::new(
        // Use the production operation allowance. Funding only the accounting
        // state made the supposed successful reference an allocation failure.
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, _)| Error::NativeWork {
        cause,
        custody: None,
    })
}

// Every writer, override and session combination through the real resolver.
#[tokio::test]
async fn compaction_policy_matrix_resolves_through_the_task_writer() -> Result<()> {
    let threshold = CompactionPolicy::default();
    let overrides = [
        None,
        Some(threshold.clone()),
        Some(CompactionPolicy::Disabled),
    ];
    for writer in [true, false] {
        let context = admitted(writer).await?;
        for policy in &overrides {
            for summary in [false, true] {
                let operation = OperationId::new();
                let executor = executor(&context, operation, policy.clone())?;
                let mut execution = StockExecution::ordinary(&executor);
                if summary {
                    execution.summary = Some((ContextPipeline::default(), Some(64)));
                }
                let expected = match (summary, policy) {
                    (true, _) => &CompactionPolicy::Disabled,
                    (false, Some(policy)) => policy,
                    (false, None) if writer => &threshold,
                    (false, None) => &CompactionPolicy::Disabled,
                };
                let case = format!("writer {writer}, override {policy:?}, summary {summary}");
                let armed = matches!(expected, CompactionPolicy::Threshold(_));
                if armed && !writer {
                    assert!(
                        matches!(execution.compaction(), Err(Error::Unauthorized(_))),
                        "{case}"
                    );
                    assert!(
                        execution.verify_task_context_binding(operation).is_err(),
                        "{case}"
                    );
                    continue;
                }
                let resolved = execution.compaction()?;
                assert_eq!(resolved.policy, expected, "{case}");
                assert_eq!(
                    resolved
                        .destination
                        .and_then(crate::runtime::SummaryDestination::writer)
                        .is_some(),
                    armed && writer,
                    "{case}"
                );
                // An armed turn without a writer is rejected at entry.
                assert_eq!(
                    execution.verify_task_context_binding(operation).is_err(),
                    armed && !writer,
                    "{case}"
                );
            }
        }
    }
    Ok(())
}

// The default may omit unavailable authority, but must never convert a failed
// native operation into successful admission. Each fault uses the real writer
// resolver, original ledger and entry check, with both default and explicit policy.
#[tokio::test]
async fn native_resolution_failures_reject_before_journal_or_model_work() -> Result<()> {
    let context = admitted(true).await?;
    for fault in 0..5 {
        for policy in [None, Some(CompactionPolicy::default())] {
            let operation = OperationId::new();
            let executor = executor(&context, operation, policy)?;
            let cancellation = acyclic_fs::CancellationToken::new();
            let mut budget = acyclic_fs::performance::work_budget(1 << 20, 1 << 20);
            match fault {
                0 => budget.bytes_hashed = 0,
                1 => budget.allocation_operations = 1, // the original ledger only
                2 => budget.items_examined = 0,
                4 => {
                    budget.peak_allocation_bytes =
                        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                }
                _ => {}
            }
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).map_err(
                |(cause, _)| Error::NativeWork {
                    cause,
                    custody: None,
                },
            )?;
            let io = crate::executor::PreparationIo::from_native_work(&work)?;
            if fault == 3 {
                cancellation.cancel();
            }
            let execution = StockExecution {
                io: Some(&io),
                ..StockExecution::ordinary(&executor)
            };
            let journal = Journal::default();
            let input = TurnInput {
                operation_id: operation,
                input: ModelContent::Text("rejected".into()),
                selected_context: None,
                max_steps: 1,
            };
            // Inspect the entry verifier directly: a later ledger operation
            // must not mask a swallowed error by failing on the poisoned work.
            let admission = execution.verify_task_context_binding(operation);
            assert!(
                match (&admission, fault) {
                    (
                        Err(Error::NativeWork {
                            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                            ..
                        }),
                        3,
                    ) => true,
                    (
                        Err(Error::NativeWork {
                            cause: acyclic_fs::NativeTreeMaterializationError::Work(_),
                            ..
                        }),
                        0..=2,
                    ) => true,
                    (Err(Error::NativeFilesystem { .. }), 4) => true,
                    _ => false,
                },
                "fault {fault}: {admission:?}"
            );
            let result = execution.ensure_started(&journal, &input).await;
            assert!(
                matches!(
                    result,
                    Err(Error::NativeWork { .. } | Error::NativeFilesystem { .. })
                ),
                "fault {fault}: {result:?}"
            );
            assert!(journal.0.lock().unwrap().is_empty());
            assert!(journal.1.lock().unwrap().is_empty());
            // Negative control: the previous `.ok()` fallback treats this same
            // failed writer lookup as an unavailable writer and admits Disabled.
            let rejected = execution.context.unwrap().0.private_summaries(Some(&io));
            assert!(matches!(
                rejected,
                Err(Error::NativeWork { .. } | Error::NativeFilesystem { .. })
            ));
            let dropped = rejected.ok();
            let mutant = if dropped.is_some() {
                CompactionPolicy::default()
            } else {
                CompactionPolicy::Disabled
            };
            assert_eq!(
                mutant,
                CompactionPolicy::Disabled,
                "failure-swallowing mutant escaped"
            );
            let mutant_executor = executor.clone().with_compaction_policy(mutant);
            let mutant_execution = StockExecution {
                io: Some(&io),
                ..StockExecution::ordinary(&mutant_executor)
            };
            assert!(
                mutant_execution
                    .verify_task_context_binding(operation)
                    .is_ok(),
                "the negative control must violate the same rejection detector"
            );
        }
    }
    Ok(())
}

// Missing authority is cached without constructing discarded diagnostics.
#[tokio::test]
async fn writerless_summary_reports_one_error_without_repeating_resolution() -> Result<()> {
    let context = admitted(false).await?;
    for policy in [
        None,
        Some(CompactionPolicy::default()),
        Some(CompactionPolicy::Disabled),
    ] {
        for prepared in [false, true] {
            let operation = OperationId::new();
            let executor = executor(&context, operation, policy.clone())?;
            let execution = StockExecution::ordinary(&executor);
            if prepared {
                let _ = execution.compaction();
            }
            let expected = acyclic_test_support::measure(|| {
                assert!(matches!(
                    execution.context.unwrap().0.private_summaries(None),
                    Err(Error::Unauthorized(_))
                ));
            });
            let journal = Journal::default();
            let instruction = ModelContent::Text(String::new());
            let actual = acyclic_test_support::measure(|| {
                let result = futures::FutureExt::now_or_never(execution.summarize(
                    &journal,
                    None,
                    operation,
                    0,
                    crate::context::Context::default(),
                    instruction,
                ));
                assert!(matches!(result, Some(Err(Error::Unauthorized(_)))));
            });
            assert_eq!(actual, expected, "policy {policy:?}, prepared {prepared}");
            // Restore the old default's discarded error followed by the manual
            // summary's second lookup: the same exact allocation guard rejects it.
            let mutant = acyclic_test_support::measure(|| {
                let _ = execution.context.unwrap().0.private_summaries(None).ok();
                let _ = execution.context.unwrap().0.private_summaries(None);
            });
            assert!(mutant.count_total > expected.count_total);
        }
    }
    Ok(())
}

// One successful resolution on the original native IO; later reads reuse it.
#[tokio::test]
async fn compaction_resolves_once_with_original_work() -> Result<()> {
    // Initialize the process-wide default before measuring.
    let unbound = StockExecutor::new(
        Model::new("test".into(), "resolver".into(), "1".into(), Value::Null)?,
        Arc::new(ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let _ = StockExecution::ordinary(&unbound).compaction_policy()?;
    for writer in [true, false] {
        let context = admitted(writer).await?;
        for policy in [None, Some(CompactionPolicy::default())] {
            let operation = OperationId::new();
            let executor = executor(&context, operation, policy.clone())?;
            let case = format!("writer {writer}, override {policy:?}");
            // Reference: exactly one writer resolution on its own ledger.
            let reference_work = native_work()?;
            let reference_io = crate::executor::PreparationIo::from_native_work(&reference_work)?;
            let reference = acyclic_test_support::measure(|| {
                let result = std::hint::black_box(
                    StockExecution::ordinary(&executor)
                        .context
                        .unwrap()
                        .0
                        .private_summaries(Some(&reference_io)),
                );
                assert_eq!(
                    result.is_ok(),
                    writer,
                    "reference did not authorize its writer"
                );
            });
            let work = native_work()?;
            let io = crate::executor::PreparationIo::from_native_work(&work)?;
            let execution = StockExecution {
                io: Some(&io),
                ..StockExecution::ordinary(&executor)
            };
            let first = acyclic_test_support::measure(|| {
                let result = std::hint::black_box(execution.verify_task_context_binding(operation));
                assert_eq!(result.is_ok(), writer || policy.is_none(), "{case}");
            });
            assert_eq!(first.count_total, reference.count_total, "{case}");
            assert_eq!(first.bytes_total, reference.bytes_total, "{case}");
            assert_eq!(work.receipt(), reference_work.receipt(), "{case}");
            if !writer && policy.is_some() {
                // A rejected turn returns that first error to its caller.
                continue;
            }
            let resolved = work.receipt();
            let repeated = acyclic_test_support::measure(|| {
                for _ in 0..3 {
                    std::hint::black_box(execution.compaction_policy().unwrap());
                    std::hint::black_box(
                        execution
                            .compaction()
                            .unwrap()
                            .destination
                            .and_then(crate::runtime::SummaryDestination::writer)
                            .is_some(),
                    );
                }
            });
            assert_eq!(repeated, Default::default(), "{case}");
            assert_eq!(work.receipt(), resolved, "{case}");
        }
    }
    Ok(())
}

// Negative control: restoring the pre-resolution repeats (policy, entry check
// and summary each re-resolving the writer) trips both detectors above. A
// writerless resolution fails with a static native error and charges nothing,
// so only the writer-backed resolution has repeats to detect.
#[tokio::test]
async fn repeated_resolution_negative_control_is_detected() -> Result<()> {
    let context = admitted(true).await?;
    let operation = OperationId::new();
    let executor = executor(&context, operation, Some(CompactionPolicy::default()))?;
    let work = native_work()?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let execution = StockExecution {
        io: Some(&io),
        ..StockExecution::ordinary(&executor)
    };
    std::hint::black_box(execution.compaction().unwrap());
    let resolved = work.receipt();
    let repeated = acyclic_test_support::measure(|| {
        for _ in 0..3 {
            std::hint::black_box(execution.compaction_policy().unwrap());
            assert!(
                std::hint::black_box(execution.context.unwrap().0.private_summaries(Some(&io)))
                    .is_ok()
            );
        }
    });
    assert!(
        repeated.count_total > 0 && work.receipt() != resolved,
        "repeats went undetected ({repeated:?})"
    );
    Ok(())
}
