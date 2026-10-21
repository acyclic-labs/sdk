use super::*;
use crate::{
    client::{CommandControl, CommandRequest, CommandResolution, CommandTaskScope},
    runtime::RuntimeTask,
    test_support::{ScriptedModelProvider, admission_work::measure},
};
use proptest::prelude::*;

pub(super) mod compaction_race;

async fn open(directory: &Path) -> Result<LocalHarness> {
    let volume = super::super::tests::volume(directory).await?;
    let config = super::super::tests::volume_config(
        directory,
        volume,
        Arc::new(ScriptedModelProvider::new([])),
    )?
    .without_default_tools()
    .compaction(crate::context::CompactionPolicy::Disabled);
    LocalHarness::open(config).await
}
async fn child(local: &LocalHarness, parent: Option<TaskId>) -> Result<TaskId> {
    match local
        .submit_to(OperationId::new(), parent, "continue")
        .await?
    {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => Ok(task_id),
        _ => panic!("admission rejected"),
    }
}
async fn write(local: &LocalHarness, task: TaskId, name: &str, value: Value) -> Result<()> {
    let key = key(task, name)?;
    let cut = local
        .task_files
        .inner
        .snapshot(&Query::File(key.clone()), 1)
        .await
        .map_err(storage)?;
    local
        .task_files
        .inner
        .append(
            cut.generation,
            &FileValue::new(key, serde_json::to_vec(&value).map_err(storage)?),
        )
        .await
        .map_err(storage)?;
    Ok(())
}
// Drives each production consumer once per admitted turn. No provider/model
// executes; the counter is at TaskStateProvider::observe_admission itself.
async fn conversation(turns: usize, full_collect: bool) -> Result<Vec<u64>> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let local = open(directory.path()).await?;
    let foreign = TaskId::from_bytes([0xff; 16]);
    CommandControl::change(
        &local.task_files.inner,
        Some(foreign),
        &[],
        Some(true),
        local.limits.file_bytes,
    )
    .await?;
    let mut parent = None;
    let mut root = None;
    let mut prefixes = Vec::with_capacity(turns);
    let mut total = 0;
    for _ in 0..turns {
        let (result, calls) = measure(async {
            let task = child(&local, parent).await?;
            let root = *root.get_or_insert(task);
            if full_collect {
                // The removed lineage() implementation, using the same owner
                // and ancestors stream. This work must violate the paired bound.
                let record = local.runtime.task_host().observe_admission(task).await?;
                let context = local
                    .runtime
                    .harness()
                    .durable_context(task, record.operation_id, None)
                    .await?;
                let _ =
                    futures::TryStreamExt::try_collect::<Vec<_>>(context.ancestors(task)?).await?;
            }
            local.stock_commands(Some(task)).await?;
            assert_eq!(local.prompt_context(Some(task), "next").await?, "next");
            assert!(local.command_has_ancestor(Some(task), root).await?);
            assert!(!local.command_has_ancestor(Some(task), foreign).await?);
            local.wait_command_control(task).await?;
            parent = Some(task);
            Ok::<_, Error>(())
        })
        .await;
        result?;
        total += calls;
        prefixes.push(total);
    }
    Ok(prefixes)
}
async fn assert_paired_work(turns: usize, full_collect: bool) -> Result<()> {
    // The first N turns of the 2N run are the same initially empty conversation:
    // retain its cumulative count instead of admitting that prefix a second time.
    // Candidate and full-collect control still own independent native stores.
    let prefixes = conversation(2 * turns, full_collect).await?;
    assert_linear_work(turns, full_collect, &prefixes);
    Ok(())
}
fn assert_linear_work(turns: usize, full_collect: bool, prefixes: &[u64]) {
    let small = prefixes[turns - 1];
    let large = prefixes[2 * turns - 1];
    println!("N={turns}, full_collect={full_collect}: {small}/{large} observe_admission calls");
    assert!(
        large <= 2 * small + 1,
        "nonlinear admission reads: {turns}={small}, {}={large}",
        2 * turns
    );
}
async fn paired(turns: usize) -> Result<()> {
    assert_paired_work(turns, false).await?;
    let failure = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
        assert_paired_work(turns, true),
    ))
    .await
    .expect_err("full-collect negative control did not panic at the linear assertion");
    check_negative(failure);
    Ok(())
}
fn check_negative(failure: Box<dyn std::any::Any + Send>) {
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains("nonlinear admission reads"),
        "unexpected negative control: {message}"
    );
}
const MAX_TURNS: usize = 8;

#[tokio::test]
async fn whole_conversation_admission_work_grows_linearly_and_detects_full_collect() -> Result<()> {
    paired(MAX_TURNS).await
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    #[test]
    fn linear_admission_work_shrinks_across_conversation_lengths(turns in 2usize..=MAX_TURNS) {
        // Every generated N selects the identical causal prefix of these runs.
        // Measure each turn and exercise every consumer as before, retaining only
        // immutable counts after both independent native stores have been dropped.
        static PREFIXES: std::sync::OnceLock<(Vec<u64>, Vec<u64>)> = std::sync::OnceLock::new();
        let (positive, negative) = PREFIXES.get_or_init(|| {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
                .block_on(async {
                    Ok::<_, Error>((conversation(2 * MAX_TURNS, false).await?,
                        conversation(2 * MAX_TURNS, true).await?))
                }).unwrap()
        });
        assert_linear_work(turns, false, positive);
        check_negative(std::panic::catch_unwind(|| assert_linear_work(turns, true, negative))
            .expect_err("full-collect negative control did not panic at the linear assertion"));
    }
}
#[tokio::test]
async fn inherited_bindings_are_pinned_before_admission_and_survive_reopening() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let local = open(directory.path()).await?;
    let root = child(&local, None).await?;
    write(&local, root, "settings", json!({"model":"first"})).await?;
    write(&local, root, "context", json!({"summary":"remember first"})).await?;
    let operation = OperationId::new();
    let pending = crate::runtime::task_id_for_admission(operation);
    local.prepare_inheritance(pending, Some(root)).await?;
    let next = child(&local, Some(root)).await?;
    write(&local, root, "settings", json!({"model":"later"})).await?;
    write(&local, root, "context", json!({"summary":"later summary"})).await?;
    let later = child(&local, Some(root)).await?;
    drop(local);
    let local = open(directory.path()).await?;
    let admitted = local.submit_to(operation, Some(root), "continue").await?;
    assert!(
        matches!(admitted, Admission::Accepted(RuntimeTask::Durable {task_id, ..}) if task_id == pending)
    );
    let record = local.runtime.task_host().observe_admission(pending).await?;
    let command: WorkflowCommand =
        serde_json::from_value(record.input["model"].clone()).map_err(storage)?;
    let context = local
        .runtime
        .harness()
        .durable_context(pending, operation, None)
        .await?;
    let bytes = context.read_file(&command.payload, None).await?;
    let input: crate::filesystem::ModelTaskCommand =
        serde_json::from_slice(&bytes).map_err(storage)?;
    assert!(
        matches!(input.input, ModelContent::Text(text) if text.contains("remember first") && !text.contains("later summary")),
        "resumed admission composed a prompt from a newer ancestor instead of its retained binding"
    );
    for (task, model, summary) in [
        (next, "first", "remember first"),
        (later, "later", "later summary"),
    ] {
        let inherited = local.resolved_inheritance(task).await?;
        assert_eq!(inherited["model"], model);
        assert_eq!(inherited["summary"], summary);
    }
    write(&local, next, "settings", json!({"model":"child override"})).await?;
    let grandchild = child(&local, Some(next)).await?;
    assert_eq!(
        local.resolved_inheritance(grandchild).await?["model"],
        "child override"
    );
    // Subtree controls remain live, including scopes added after admission.
    local
        .dispatch_command(
            OperationId::new(),
            Some(root),
            CommandResolution::Submit {
                request: CommandRequest::Pause {
                    scope: CommandTaskScope::Subtree,
                },
            },
        )
        .await?;
    let matched = std::sync::atomic::AtomicBool::new(false);
    let mut waiting = Box::pin(CommandControl::wait_matching(
        &local.task_files.inner,
        |scope| {
            let local = &local;
            let matched = &matched;
            Box::pin(async move {
                let applies = local.command_has_ancestor(Some(grandchild), scope).await?;
                if applies {
                    matched.store(true, std::sync::atomic::Ordering::Release);
                }
                Ok(applies)
            })
        },
        local.limits.file_bytes,
    ));
    // The only scope is the paused root. Observe its completed match and a
    // pending waiter in the same poll, so pending storage I/O cannot pass this.
    futures::future::poll_fn(|cx| {
        assert!(
            std::future::Future::poll(waiting.as_mut(), cx).is_pending(),
            "paused waiter completed"
        );
        if matched.load(std::sync::atomic::Ordering::Acquire) {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    local
        .dispatch_command(
            OperationId::new(),
            Some(next),
            CommandResolution::Submit {
                request: CommandRequest::Resume {
                    scope: CommandTaskScope::Subtree,
                },
            },
        )
        .await?;
    waiting.await?;
    assert!(local.command_has_ancestor(Some(grandchild), root).await?);
    assert!(!local.command_has_ancestor(Some(later), next).await?);
    Ok(())
}

#[tokio::test]
async fn legacy_admissions_are_upgraded_once_then_reopen_without_ancestry_reads() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let local = open(directory.path()).await?;
    let root = child(&local, None).await?;
    write(&local, root, "settings", json!({"model":"inherited"})).await?;
    write(
        &local,
        root,
        "context",
        json!({"summary":"retained context"}),
    )
    .await?;
    let definition = local
        .runtime
        .harness()
        .task::<Value, crate::executor::TurnOutput>("acyclic.stock_turn@1")?;
    let mut task = root;
    for _ in 0..4 {
        let record = local.runtime.task_host().observe_admission(task).await?;
        let context = local
            .runtime
            .harness()
            .durable_context(task, record.operation_id, None)
            .await?;
        let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) = context
            .admit(OperationId::new(), &definition, record.input)
            .await?
        else {
            panic!("legacy admission");
        };
        task = task_id;
    }
    let (value, cold) = measure(local.resolved_inheritance(task)).await;
    assert_eq!(value?["summary"], "retained context");
    assert_eq!(cold, 5, "one lookup plus the four missing parent edges");
    drop(local);
    let local = open(directory.path()).await?;
    let (value, warm) = measure(local.resolved_inheritance(task)).await;
    assert_eq!(value?["model"], "inherited");
    assert_eq!(warm, 0);
    Ok(())
}

#[tokio::test]
async fn individually_valid_model_and_summary_do_not_overflow_an_inheritance_file() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let volume = super::super::tests::volume(directory.path()).await?;
    let config = || {
        Ok::<_, Error>(
            super::super::tests::volume_config(
                directory.path(),
                volume.clone(),
                Arc::new(ScriptedModelProvider::new([])),
            )?
            .without_default_tools()
            .compaction(crate::context::CompactionPolicy::Disabled)
            .limits(Limits {
                file_bytes: 4096,
                ..Limits::default()
            }),
        )
    };
    let local = LocalHarness::open(config()?).await?;
    let root = child(&local, None).await?;
    let model = "m".repeat(1500);
    let summary = "s".repeat(3000);
    for (name, value) in [
        ("settings", json!({"model":model})),
        ("context", json!({"summary":summary})),
    ] {
        assert!(serde_json::to_vec(&value).unwrap().len() <= 4096);
        write(&local, root, name, value).await?;
    }
    // The previous combined envelope rejects these individually valid files.
    let mut old = local.resolved_inheritance(root).await?;
    old["parent"] = json!(root);
    assert!(matches!(
        retain(&local.task_files.inner, key(TaskId::new(), "old-envelope")?, old, 4096).await,
        Err(Error::Invalid(message)) if message == "inherited state exceeds file bound"
    ));
    let next = child(&local, Some(root)).await?;
    drop(local);
    let local = LocalHarness::open(config()?).await?;
    assert_eq!(local.resolved_value(next, "model").await?, Some(model));
    assert_eq!(local.resolved_value(next, "summary").await?, Some(summary));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn inherited_value_files_preserve_each_source_bound(
        model in prop::collection::vec(any::<char>(), 0..32),
        summary in prop::collection::vec(any::<char>(), 0..32),
        frozen in any::<bool>(),
    ) {
        let model: String = model.into_iter().collect();
        let summary: String = summary.into_iter().collect();
        let mut selected: crate::context::SummarySelection = serde_json::from_str(include_str!("../../../fuzz-context/corpus/context_manifest/summary-selection.json")).unwrap();
        selected.continuation.as_mut().unwrap().frozen = frozen;
        for summary in [json!(summary), serde_json::to_value(selected).unwrap()] {
        let bound = [json!({"model":model}), json!({"summary":summary})]
            .iter().map(|value| serde_json::to_vec(value).unwrap().len()).max().unwrap().max(40) as u64;
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let volume = TaskVolume::open(
                Arc::new(acyclic_stream::MemoryStream::default()),
                acyclic_stream::StreamPath::new("inherited-source-bounds").unwrap(),
            ).await.unwrap();
            let task = TaskId::new();
            let expected = json!({"model":model,"summary":summary,"parent":TaskId::new()});
            assert_eq!(retain_inheritance(&volume, task, expected.clone(), bound).await.unwrap(), expected);
            assert_eq!(read_inheritance(&volume, task, bound).await.unwrap(), Some(expected));
        });
        }
    }
}

#[tokio::test]
async fn retried_operation_cannot_change_its_prepared_parent_or_bypass_subtree_pause() -> Result<()>
{
    let directory = tempfile::tempdir().map_err(storage)?;
    let local = open(directory.path()).await?;
    let a = child(&local, None).await?;
    let b = child(&local, Some(a)).await?;
    let operation = OperationId::new();
    let task = crate::runtime::task_id_for_admission(operation);
    local.prepare_inheritance(task, Some(a)).await?;
    drop(local);
    let local = open(directory.path()).await?;
    local
        .dispatch_command(
            OperationId::new(),
            Some(b),
            CommandResolution::Submit {
                request: CommandRequest::Pause {
                    scope: CommandTaskScope::Subtree,
                },
            },
        )
        .await?;
    assert!(matches!(
        local.submit_to(operation, Some(b), "continue").await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        local.prepare_inheritance(task, None).await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        local.runtime.task_host().observe_admission(task).await,
        Err(Error::NotFound(_))
    ));
    assert!(
        matches!(local.submit_to(operation, Some(a), "continue").await?,
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) if task_id == task)
    );
    assert_eq!(
        local
            .runtime
            .task_host()
            .observe_admission(task)
            .await?
            .parent,
        Some(a)
    );
    assert!(local.command_has_ancestor(Some(task), a).await?);
    assert!(!local.command_has_ancestor(Some(task), b).await?);
    // The admitted operation remains bound to A on a later retry too.
    assert!(matches!(
        local.submit_to(operation, Some(b), "continue").await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn legacy_retry_cannot_publish_a_parent_that_disagrees_with_owner_admission() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let local = open(directory.path()).await?;
    let root = child(&local, None).await?;
    let a = child(&local, Some(root)).await?;
    let b = child(&local, Some(root)).await?;
    // Admit through the public runtime without LocalHarness inheritance, as in
    // an older installation or admission recovered after interrupted publication.
    let runtime = local.task_runtime();
    let record = runtime.task_host().observe_admission(a).await?;
    let definition = runtime
        .harness()
        .task::<Value, crate::executor::TurnOutput>("acyclic.stock_turn@1")?;
    let operation = OperationId::new();
    let task = crate::runtime::task_id_for_admission(operation);
    assert!(
        matches!(runtime.harness().durable_context(a, record.operation_id, None)
        .await?.admit(operation, &definition, record.input).await?,
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) if task_id == task)
    );
    drop(local);
    let local = open(directory.path()).await?;
    local
        .dispatch_command(
            OperationId::new(),
            Some(a),
            CommandResolution::Submit {
                request: CommandRequest::Pause {
                    scope: CommandTaskScope::Subtree,
                },
            },
        )
        .await?;
    assert!(matches!(
        local
            .dispatch_command(
                operation,
                Some(b),
                CommandResolution::Prompt {
                    text: "continue".into(),
                    source: None,
                    mentions: Vec::new(),
                }
            )
            .await,
        Err(Error::Conflict(_))
    ));
    // Reject before publishing even the parent claim, not just before admission.
    for name in ["inherited", "inherited-model", "inherited-summary"] {
        assert_eq!(
            read(
                &local.task_files.inner,
                &key(task, name)?,
                local.limits.file_bytes
            )
            .await?,
            None
        );
    }
    assert_eq!(
        local
            .task_runtime()
            .task_host()
            .observe_admission(task)
            .await?
            .parent,
        Some(a)
    );
    drop(local);
    let local = open(directory.path()).await?;
    assert!(local.command_has_ancestor(Some(task), a).await?);
    assert!(!local.command_has_ancestor(Some(task), b).await?);
    let matched = std::sync::atomic::AtomicBool::new(false);
    let mut waiting = Box::pin(CommandControl::wait_matching(
        &local.task_files.inner,
        |scope| {
            let local = &local;
            let matched = &matched;
            Box::pin(async move {
                let applies = local.command_has_ancestor(Some(task), scope).await?;
                if applies {
                    matched.store(true, std::sync::atomic::Ordering::Release);
                }
                Ok(applies)
            })
        },
        local.limits.file_bytes,
    ));
    futures::future::poll_fn(|cx| {
        assert!(
            std::future::Future::poll(waiting.as_mut(), cx).is_pending(),
            "legacy task bypassed its admitted parent's subtree pause"
        );
        if matched.load(std::sync::atomic::Ordering::Acquire) {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    Ok(())
}

fn same_generation(
    before: &crate::resources::GenerationRef,
    after: &crate::resources::GenerationRef,
) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(
        before,
        after,
        "unchanged receive input published another generation"
    );
    Ok(())
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    #[test]
    fn constant_receive_inputs_do_not_publish_per_turn(turns in 2usize..9) {
        let (before, after, old) = tokio::runtime::Builder::new_current_thread().enable_all()
            .build().unwrap().block_on(async {
                let directory = tempfile::tempdir().map_err(storage)?;
                let local = open(directory.path()).await?;
                let (receive, model) = local.stage_turn(OperationId::new(), "input").await?;
                let publisher = local.input_publisher()?;
                let reference = crate::filesystem::workspace_ref(
                    local.journal.provider().clone(), &local.journal.storage_name(&mut None)?,
                )?;
                let before = local.files.resolve(&reference, None).await?.generation;
                let mut identities = HashSet::new();
                for _ in 0..turns {
                    let (next, _) = local.finish_turn_inputs(OperationId::new(),
                        model.payload.clone(), publisher.as_ref()).await?;
                    assert_eq!(next.payload, receive.payload);
                    assert!(identities.insert(next.operation_id));
                }
                let after = local.files.resolve(&reference, None).await?.generation;
                // The previous production path publishes the same bytes again
                // under each command identity. It must fail the same assertion.
                publisher.stage_once(OperationId::new(), "inputs/old.receive.json",
                    b"{\"after\":0}", "application/json", "mailbox receive", None)?
                    .await?;
                let old = local.files.resolve(&reference, None).await?.generation;
                // Recovery must retain the original stage_once fingerprint checks.
                let operation = OperationId::new();
                let bad = format!("{{\"after\":{turns}}}");
                publisher.stage_once(super::super::turn::receive_operation(operation),
                    &format!("inputs/{operation}.receive.json"), bad.as_bytes(),
                    "application/json", "mailbox receive", None)?.await?;
                assert!(matches!(local.finish_turn_inputs(operation,
                    model.payload.clone(), publisher.as_ref()).await, Err(Error::Conflict(_))));
                Ok::<_, Error>((before, after, old))
            }).unwrap();
        same_generation(&before, &after)?;
        prop_assert!(same_generation(&before, &old).is_err());
    }
}

#[tokio::test]
async fn legacy_receive_input_keeps_its_original_reference_on_retry_and_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let local = open(directory.path()).await?;
    let operation = OperationId::new();
    let (receive, model) = local.stage_turn(operation, "input").await?;
    let publisher = local.input_publisher()?;
    let legacy = publisher
        .stage_once(
            receive.operation_id,
            &format!("inputs/{operation}.receive.json"),
            b"{\"after\":0}",
            "application/json",
            "mailbox receive",
            None,
        )?
        .await?
        .0;
    let (retried, _) = local
        .finish_turn_inputs(operation, model.payload.clone(), publisher.as_ref())
        .await?;
    assert_eq!(retried.payload, legacy);
    assert_eq!(retried.operation_id, receive.operation_id);
    drop(publisher);
    drop(local);
    let local = open(directory.path()).await?;
    let (retried, _) = local
        .finish_turn_inputs(operation, model.payload, local.input_publisher()?.as_ref())
        .await?;
    assert_eq!(retried.payload, legacy);
    assert_eq!(retried.operation_id, receive.operation_id);
    Ok(())
}
