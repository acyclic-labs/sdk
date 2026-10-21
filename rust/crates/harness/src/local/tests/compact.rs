use super::*;
use crate::runtime::RuntimeTask;

mod continuations;

fn admitted(admission: Admission<RuntimeTask<crate::executor::TurnOutput>>) -> crate::TaskId {
    match admission {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => task_id,
        _ => panic!("durable admission required"),
    }
}

#[tokio::test]
async fn manual_summary_recovers_source_command_and_mailbox_publication_cuts_without_recapture()
-> Result<()> {
    futures::future::try_join_all((0..3).flat_map(|cut| {
        [false, true]
            .into_iter()
            .map(move |after| publication_cut(cut, after))
    }))
    .await?;
    Ok(())
}

async fn publication_cut(cut: usize, after: bool) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Text("late target answer".into()),
        ScriptedTurn::Text("retained summary".into()),
    ]);
    let config = || volume_config(directory.path(), project.clone(), provider.clone());
    let local = LocalHarness::open(config()?).await?;
    let target_operation = OperationId::new();
    local
        .retain_settings(target_operation, &json!({"model":"selected-summary"}))
        .await?;
    let target = admitted(local.submit(target_operation, "original prompt").await?);
    let operation = OperationId::new();
    let publisher = local.input_publisher()?;
    let interrupted = context::crash_cuts::Interrupted::new(publisher.as_ref(), cut, after);
    assert!(matches!(
        local.stage_summary_with(operation, target, "keep the invariant", &interrupted).await,
        Err(Error::Storage(message)) if message == "injected publication interruption"
    ));
    assert!(provider.requests().is_empty());
    // Change the source after the selected commit/ack boundary: execution
    // publishes an outcome and transcript that the earlier source lacked.
    assert_eq!(local.resume(target).await?.text, "late target answer");
    drop(interrupted);
    drop(publisher);
    drop(local);
    let local = LocalHarness::open(config()?).await?;
    assert_eq!(
        local
            .compact(operation, target, "keep the invariant")
            .await?["summary"],
        "retained summary"
    );
    let request = provider.requests().last().unwrap().clone();
    let source = request
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            crate::model::ModelContent::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(source.contains("original prompt"));
    assert!(source.contains("keep the invariant"));
    assert_eq!(
        source.contains("late target answer"),
        cut == 0 && !after,
        "recaptured a published source at cut {cut}, after={after}"
    );
    assert!(request.tools.is_empty());
    assert!(request.max_output_tokens.is_some());
    assert_eq!(request.model.name, "selected-summary");
    let requests = provider.requests().len();
    // Replaying the same request reuses its admitted summary task.
    assert_eq!(
        local
            .compact(operation, target, "keep the invariant")
            .await?["summary"],
        "retained summary"
    );
    assert_eq!(provider.requests().len(), requests);
    Ok(())
}

#[tokio::test]
async fn manual_summary_rejects_tool_calls_while_an_ordinary_turn_executes_them() -> Result<()> {
    for summary in [true, false] {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(directory.path()).await?;
        let provider = scripted([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                    "attempt".into(),
                    "test.custom".into(),
                    json!({}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]),
            ScriptedTurn::Text("tool executed".into()),
        ]);
        let local = LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?.tool(tool("test.custom")),
        )
        .await?;
        let target = admitted(local.submit(OperationId::new(), "original prompt").await?);
        if summary {
            assert!(local.compact(OperationId::new(), target, "").await.is_err());
            assert_eq!(provider.requests().len(), 1);
            assert!(provider.requests()[0].tools.is_empty());
        } else {
            // The same malicious script is accepted by the coding loop, so the
            // detector would fail if /compact were routed through an ordinary turn.
            assert_eq!(local.resume(target).await?.text, "tool executed");
            assert_eq!(provider.requests().len(), 2);
            assert!(!provider.requests()[0].tools.is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn manual_summary_reserves_actual_input_on_a_small_model() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let capacity = ModelContextCapacity {
        context_tokens: 8_192,
        output_tokens: Some(8_192),
    };
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("small-model summary".into())])
            .with_context_capacity(capacity),
    );
    let local = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let target = admitted(
        local
            .submit(OperationId::new(), &"source".repeat(200))
            .await?,
    );
    let operation = OperationId::new();
    assert_eq!(
        local.compact(operation, target, "keep facts").await?["summary"],
        "small-model summary"
    );
    let requests = provider.requests();
    let request = crate::model::PreparedModelRequest::prepare(
        requests[0].clone(),
        crate::conversation::Limits::default(),
        None,
    )?;
    let count = provider.count_tokens(&request).await?;
    let input = count.validate(&request, &mut None)?;
    let output = request.request().max_output_tokens.unwrap();
    assert!(output > 0);
    assert!(input + u64::from(output) <= u64::from(capacity.context_tokens));
    assert!(request.request().tools.is_empty());
    // The old ceiling leaves only two input tokens; the same admission
    // predicate must reject it for this exact captured request.
    assert!(input + 8_190 > u64::from(capacity.context_tokens));
    let requests_before = requests.len();
    drop(requests);
    local.compact(operation, target, "keep facts").await?;
    assert_eq!(provider.requests().len(), requests_before);
    Ok(())
}

proptest::proptest! {
    // Every accepted ceiling leaves room for the complete counted input;
    // rejection is exactly exhausted input capacity. Values shrink to boundaries.
    #[test]
    fn summary_budget_preserves_input_and_output_bounds(
        context in 1u32..1_000_000,
        ceiling in 1u32..1_000_000,
        input in 0u64..1_000_001,
    ) {
        let actual = super::super::compact::summary_output_budget(context, ceiling, input);
        if input >= u64::from(context) {
            proptest::prop_assert!(actual.is_err());
        } else {
            let output = actual.unwrap();
            proptest::prop_assert_eq!(u64::from(output), u64::from(ceiling).min(u64::from(context) - input));
            let fits = |output: u32| output > 0 && output <= ceiling
                && input + u64::from(output) <= u64::from(context);
            proptest::prop_assert!(fits(output));
            if u64::from(ceiling) > u64::from(context) - input {
                // Deliberately omit input reservation; the same detector must fail.
                proptest::prop_assert!(!fits(ceiling));
            }
        }
    }
}

#[tokio::test]
async fn checkpoints_reject_stale_replay_pin_branches_and_continue_completed_tasks() -> Result<()> {
    use crate::context::SummarySelection;
    use crate::task_volume::{Channel, Component, FileKey, FileValue, TaskFile};
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Text("completed original".into()),
        ScriptedTurn::Text("FACT_ONLY_IN_A".into()),
        ScriptedTurn::Text("SUMMARY_B".into()),
        ScriptedTurn::Text("branch answer".into()),
        ScriptedTurn::Text("rewound answer".into()),
    ]);
    let config = || volume_config(directory.path(), project.clone(), provider.clone());
    let local = LocalHarness::open(config()?).await?;
    let target_operation = OperationId::new();
    let target = admitted(local.submit(target_operation, "original prompt").await?);
    local.resume(target).await?;
    // The raw child API must keep rejecting children of this completed task.
    let definition = local
        .runtime
        .harness()
        .task::<Value, crate::executor::TurnOutput>("acyclic.stock_turn@1")?;
    let record = local.runtime.task_host().observe_admission(target).await?;
    let owner = local
        .runtime
        .harness()
        .durable_context(target, record.operation_id, None)
        .await?;
    assert!(
        owner
            .admit(OperationId::new(), &definition, record.input)
            .await
            .is_err()
    );
    drop(owner);
    let a = OperationId::new();
    local.compact(a, target, "retain A").await?;
    let (selected_a, checkpoint_a) = local.selected_checkpoint(target).await?.unwrap();
    assert!(
        matches!(local.submit_to(target_operation, Some(target), "original prompt").await,
        Err(Error::Conflict(message)) if message == "task admission belongs to another conversation")
    );
    // Admission through the production prompt path creates a retained root
    // continuation. Its binding is A, even if B is published before dispatch.
    let branch_operation = OperationId::new();
    let branch = serde_json::from_value(
        local
            .dispatch_command(
                branch_operation,
                Some(target),
                crate::client::CommandResolution::Prompt {
                    text: "branch prompt".into(),
                    source: None,
                    mentions: Vec::new(),
                },
            )
            .await?["task"]
            .clone(),
    )
    .map_err(super::super::storage)?;
    assert!(
        local
            .runtime
            .task_host()
            .observe_admission(branch)
            .await?
            .parent
            .is_none()
    );
    assert!(
        matches!(local.submit_to(branch_operation, Some(branch), "changed parent").await,
        Err(Error::Conflict(message)) if message == "continuation belongs to another conversation")
    );
    let mut chunk = FileValue::new(
        FileKey::Task {
            task: Component::new(target.to_string()).map_err(super::super::storage)?,
            file: TaskFile::Chunk {
                turn: 99,
                sequence: 0,
            },
        },
        b"TURN_AFTER_A".to_vec(),
    );
    chunk.channel = Some(Channel::Text);
    local
        .task_files
        .inner
        .create(
            local
                .task_files
                .inner
                .head()
                .await
                .map_err(super::super::storage)?,
            &chunk,
        )
        .await
        .map_err(super::super::storage)?;
    let b = OperationId::new();
    local.compact(b, target, "retain B").await?;
    let request = serde_json::to_string(&provider.requests()[2]).map_err(super::super::storage)?;
    assert!(request.contains("FACT_ONLY_IN_A"));
    assert!(request.contains("TURN_AFTER_A"));
    assert!(!request.contains("completed original"));
    let (selected_b, _) = local.selected_checkpoint(target).await?.unwrap();
    local.compact(a, target, "retain A").await?;
    assert_eq!(
        local.selected_checkpoint(target).await?.unwrap().0,
        selected_b
    );
    assert_eq!(provider.requests().len(), 3);
    // Broken publication ignores predecessor/fence and replaces B with A.
    super::super::compact::checkpoints::select::<_, false>(
        &local.task_files.inner,
        target,
        &checkpoint_a,
        &selected_a.checkpoint,
        local.limits.file_bytes,
    )
    .await?;
    assert_ne!(
        local.selected_checkpoint(target).await?.unwrap().0,
        selected_b
    );
    // Restore the selected boundary using the ordinary user-owned file.
    let key = super::super::commands::compact_key(target)?;
    local
        .task_files
        .inner
        .append(
            local
                .task_files
                .inner
                .head()
                .await
                .map_err(super::super::storage)?,
            &FileValue::new(
                key.clone(),
                serde_json::to_vec(&selected_b).map_err(super::super::storage)?,
            ),
        )
        .await
        .map_err(super::super::storage)?;
    local.close().await?;
    let local = LocalHarness::open(config()?).await?;
    assert_eq!(local.resume(branch).await?.text, "branch answer");
    let branch_request =
        serde_json::to_string(&provider.requests()[3]).map_err(super::super::storage)?;
    assert!(branch_request.contains("FACT_ONLY_IN_A"));
    assert!(!branch_request.contains("SUMMARY_B"));
    // Rewind is a file selection at a fixed conversation cursor, with a fresh
    // selection fence. Replaying B cannot undo this later user decision.
    let rewind = SummarySelection {
        continuation: None,
        checkpoint: selected_a.checkpoint,
        selected_at: local
            .task_files
            .inner
            .head()
            .await
            .map_err(super::super::storage)?
            .position,
        through: Some(checkpoint_a.boundary.through),
    };
    local
        .task_files
        .inner
        .append(
            local
                .task_files
                .inner
                .head()
                .await
                .map_err(super::super::storage)?,
            &FileValue::new(
                key,
                serde_json::to_vec(&rewind).map_err(super::super::storage)?,
            ),
        )
        .await
        .map_err(super::super::storage)?;
    local.compact(b, target, "retain B").await?;
    assert_eq!(local.selected_checkpoint(target).await?.unwrap().0, rewind);
    let rewound = serde_json::from_value(
        local
            .dispatch_command(
                OperationId::new(),
                Some(target),
                crate::client::CommandResolution::Prompt {
                    text: "after rewind".into(),
                    source: None,
                    mentions: Vec::new(),
                },
            )
            .await?["task"]
            .clone(),
    )
    .map_err(super::super::storage)?;
    assert_eq!(local.resume(rewound).await?.text, "rewound answer");
    let request = serde_json::to_string(&provider.requests()[4]).map_err(super::super::storage)?;
    assert!(request.contains("FACT_ONLY_IN_A"));
    assert!(!request.contains("SUMMARY_B"));
    Ok(())
}

proptest::proptest! {
    #[test]
    fn checkpoint_selection_requires_the_exact_predecessor_and_observation_fence(
        captured in proptest::num::u64::ANY, selected_at in proptest::num::u64::ANY,
        predecessor in proptest::bool::ANY, changed in proptest::bool::ANY,
    ) {
        use crate::context::SummarySelection;
        use crate::task_volume::{Cursor,FileKey};
        use super::super::compact::checkpoints::advances;
        let cursor = Cursor {volume:[0;32],epoch:uuid::Uuid::nil(),schema:crate::task_volume::SCHEMA,
            query:[0;32],position:1,accepted:1,durable:1,commit:[0;32]};
        let mut cp = crate::context::summary_source::checkpoint_fixture::checkpoint(cursor).unwrap();
        let key = FileKey::parse(".tasks/task/context/compaction-next/manifest.json").unwrap();
        let previous = FileKey::parse(".tasks/task/context/compaction-previous/manifest.json").unwrap();
        cp.boundary.captured_at = captured;
        let current = SummarySelection {continuation:None,checkpoint:previous,selected_at,through:None};
        let identity=*blake3::hash(&serde_json::to_vec(&current).unwrap()).as_bytes();
        cp.boundary.previous=predecessor.then_some(identity);
        if changed { if let Some(previous)=&mut cp.boundary.previous { previous[0]^=1; } }
        let expected = predecessor && !changed && captured > selected_at;
        proptest::prop_assert_eq!(advances::<true>(Some(&current),Some(identity),&cp,&key),expected);
        if !expected { proptest::prop_assert!(advances::<false>(Some(&current),Some(identity),&cp,&key)); }
    }
}
