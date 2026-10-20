//! The bound file writer and query use the same durable private provider.
use super::*;
use crate::task_volume::{Component, FileKey, FileValue, Query, TaskFile};
use futures::StreamExt as _;

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn model_text_and_shell_output_share_the_same_turn_after_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::Content {
                delta: "before shell".into(),
            }),
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                "shell-call".into(),
                "bash".into(),
                json!({"command":"printf shell-output"}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]),
        ScriptedTurn::Text("done".into()),
    ]);
    let config = || {
        Ok::<_, Error>(
            super::checkout_config(directory.path(), (model()?, provider.clone()))?
                .state_directory(state.path()),
        )
    };
    let harness = LocalHarness::open(config()?).await?;
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(OperationId::new(), "text and shell").await?
    else {
        panic!("durable admission")
    };
    harness.resume(task_id).await?;
    let task = Component::new(task_id.to_string())?;
    let snapshot = harness
        .task_files()
        .inner
        .snapshot(&Query::Task(task.clone()), 64)
        .await?;
    let turn = snapshot
        .files
        .iter()
        .find_map(|file| match &file.key {
            FileKey::Task {
                file: TaskFile::Chunk { turn, .. },
                ..
            } if file.bytes.as_ref() == b"before shell" => Some(*turn),
            _ => None,
        })
        .expect("canonical model text");
    let query = Query::Turn { task, turn };
    let selected = harness.task_files().inner.snapshot(&query, 32).await?;
    for file in [
        crate::task_volume::ToolFile::Request,
        crate::task_volume::ToolFile::Stdout(0),
        crate::task_volume::ToolFile::Result,
    ] {
        assert!(
            selected.files.iter().any(|value| matches!(&value.key,
            FileKey::Task { file: TaskFile::Tool { file: actual, .. }, .. } if actual == &file)),
            "the text turn must include {file:?}"
        );
    }
    harness.disconnect().await?;
    let reopened = LocalHarness::open(config()?).await?;
    assert_eq!(
        reopened
            .task_files()
            .inner
            .snapshot(&query, 32)
            .await?
            .files,
        selected.files
    );
    reopened.disconnect().await
}

#[tokio::test]
async fn private_request_files_follow_immediately_and_survive_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    // The reopen below must find the same owner, so both opens share one injected state directory.
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([]);
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    let files = harness.task_files();
    assert_eq!(files.volume().class(), VolumeClass::AgentPrivate);
    assert_ne!(files.volume(), harness.scratch());
    assert_ne!(files.volume(), harness.journal_volume());
    let task = Component::new("task").map_err(super::super::storage)?;
    let mut initial = files
        .observe(Query::Requests(task.clone()), None, 8)
        .await
        .map_err(super::super::storage)?;
    assert!(!initial.reset);
    let snapshot = initial.snapshot.take().unwrap();
    assert!(snapshot.files.is_empty());
    let request = FileValue::new(
        FileKey::Task {
            task: task.clone(),
            file: TaskFile::Request(Component::new("request").map_err(super::super::storage)?),
        },
        br#"{"input":"hi"}"#.to_vec(),
    );
    let generation = files
        .write(snapshot.generation, &request)
        .await
        .map_err(super::super::storage)?;
    let change = initial
        .changes
        .next()
        .await
        .unwrap()
        .map_err(super::super::storage)?;
    assert_eq!(change.file.as_ref(), Some(&request));
    assert_eq!(change.cursor.position, generation.position);
    assert_eq!(
        files
            .write(generation, &request)
            .await
            .map_err(super::super::storage)?,
        generation
    );
    let status = FileValue::new(
        FileKey::Task {
            task: task.clone(),
            file: TaskFile::Status,
        },
        b"{}".to_vec(),
    );
    assert!(matches!(
        files.write(generation, &status).await,
        Err(crate::task_volume::Error::Unauthorized)
    ));
    let mut altered = request.clone();
    altered.bytes = acyclic_stream::OwnedBytes::from_static(b"different");
    assert!(matches!(
        files.write(generation, &altered).await,
        Err(crate::task_volume::Error::ImmutableConflict(_))
    ));
    let cursor = change.cursor;
    drop(initial);
    drop(harness);

    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model()?, provider))?
            .state_directory(state.path()),
    )
    .await?;
    let reopened = harness
        .task_files()
        .observe(Query::Requests(task.clone()), None, 8)
        .await
        .map_err(super::super::storage)?;
    let snapshot = reopened.snapshot.unwrap();
    assert_eq!(snapshot.files, vec![request]);
    assert_eq!(snapshot.generation, generation);
    let resumed = harness
        .task_files()
        .observe(Query::Requests(task), Some(cursor), 8)
        .await
        .map_err(super::super::storage)?;
    assert!(!resumed.reset);
    assert!(resumed.snapshot.is_none());
    assert!(!directory.path().join(".tasks").exists());
    Ok(())
}

#[tokio::test]
async fn default_spawn_dispatch_writes_the_bound_private_volume() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                "spawn-worker".into(),
                "spawn".into(),
                json!({"name":"worker","task":"inspect files","grants":["model:generate","mail:read","tool:call:read"]}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]),
        ScriptedTurn::Text("request published".into()),
        ScriptedTurn::Text("request published".into()),
    ]);
    let harness = LocalHarness::open(volume_config(directory.path(), project, provider)?).await?;
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(OperationId::new(), "spawn a reader").await?
    else {
        panic!("parent was not admitted");
    };
    assert_eq!(harness.resume(task_id).await?.text, "request published");
    let snapshot = harness
        .task_files()
        .observe(
            Query::Child(Component::new("worker").map_err(super::super::storage)?),
            None,
            8,
        )
        .await
        .map_err(super::super::storage)?
        .snapshot
        .unwrap();
    assert_eq!(
        snapshot
            .files
            .iter()
            .map(|file| file.key.path())
            .collect::<Vec<_>>(),
        [
            "agents/worker/request.md",
            "agents/worker/status.json",
            "agents/worker/delivered.json",
            "agents/worker/results/1.json",
        ]
    );
    assert_eq!(snapshot.files[0].requester(), Some(task_id));
    let result = snapshot
        .files
        .iter()
        .find(|file| file.key.path() == "agents/worker/results/1.json")
        .unwrap();
    let result =
        crate::task_volume::ChildTurnResult::parse(&result.bytes).map_err(super::super::storage)?;
    assert_eq!(result.turn, 1);
    let delivered = snapshot
        .files
        .iter()
        .find(|file| file.key.path() == "agents/worker/delivered.json")
        .unwrap();
    let frontier: (crate::TaskId, u64) =
        serde_json::from_slice(&delivered.bytes).map_err(super::super::storage)?;
    assert_eq!(frontier, (result.task, 1));
    assert!(matches!(result.outcome, crate::Outcome::Succeeded(_)));
    assert!(
        std::str::from_utf8(&snapshot.files[0].bytes)
            .unwrap()
            .contains("inspect files")
    );
    assert!(
        std::str::from_utf8(&snapshot.files[0].bytes)
            .unwrap()
            .contains("\"grants\":[\"model:generate\",\"mail:read\",\"tool:call:read\"]")
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_parent_cannot_publish_a_late_agent_file() -> Result<()> {
    use crate::{
        distributed::{WorkPull, Worker},
        runtime::{RuntimeTask, TaskStateProvider, ToolContext},
        scheduler::LeaseFence,
        task_volume::ChildFile,
        tool::ToolInvocation,
    };
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let harness =
        LocalHarness::open(volume_config(directory.path(), project, scripted([]))?).await?;
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) =
        harness.submit(operation, "parent").await?
    else {
        panic!("parent was not admitted");
    };
    let host = harness.runtime.task_host();
    let WorkPull::Claimed(lease) = host
        .pull_work(
            &Worker {
                id: "agent-file-fence".into(),
                available: Default::default(),
                labels: Default::default(),
            },
            Some(operation),
            &mut None,
        )
        .await?
    else {
        panic!("parent was not claimed");
    };
    host.start_task(&lease, &mut None).await?;
    let context = Arc::new(
        harness
            .runtime
            .task_context(task_id, LeaseFence::from(&lease.reservation))
            .await?,
    );
    let owner = context.owned_mailbox.as_ref().unwrap();
    let (condition, deadline) = owner.file_publication_condition().await?;
    let files = harness.task_files();
    let snapshot = files
        .observe(Query::Children, None, 8)
        .await
        .map_err(super::super::storage)?
        .snapshot
        .unwrap();
    host.cancel(task_id).await?;
    let request = FileValue::new(
        FileKey::Child {
            reference: Component::new("worker").map_err(super::super::storage)?,
            file: ChildFile::Request,
        },
        b"late work".to_vec(),
    );
    assert!(matches!(
        files
            .write_if(
                snapshot.generation,
                &request,
                owner.task_id(),
                condition,
                deadline
            )
            .await,
        Err(crate::task_volume::Error::FenceConflict)
    ));
    let registry = crate::tools::defaults()?;
    let tool = registry.get("spawn", None, None)?.unwrap();
    let operation = OperationId::new();
    let call = ToolInvocation {
        operation_id: operation,
        call_id: "late-spawn".into(),
        name: "spawn".into(),
        arguments: json!({"name":"worker","task":"late work"}),
    };
    assert!(
        tool.executor
            .execute(
                call,
                Some(ToolContext::new(context, operation, "late-spawn", None)?)
            )
            .await
            .is_err()
    );
    let after = files
        .observe(Query::Children, None, 8)
        .await
        .map_err(super::super::storage)?
        .snapshot
        .unwrap();
    assert_eq!(after.generation, snapshot.generation);
    assert!(after.files.is_empty());
    // The caller's own file authority survives; only the stale task is fenced.
    files
        .write(after.generation, &request)
        .await
        .map_err(super::super::storage)?;
    Ok(())
}

#[tokio::test]
async fn agent_file_deadline_uses_the_admission_clock_at_commit() -> Result<()> {
    use crate::{
        distributed::{WorkPull, Worker},
        runtime::{RuntimeTask, TaskRunLimits},
        scheduler::LeaseFence,
        task_volume::ChildFile,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    struct Clock(AtomicU64);
    impl acyclic_stream::UnixMillisClock for Clock {
        fn now_unix_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    let clock = Arc::new(Clock(AtomicU64::new(4_000_000_000_000)));
    let deadline = acyclic_stream::UnixMillisClock::now_unix_millis(clock.as_ref()) + 100;
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let config = volume_config(directory.path(), project, scripted([]))?
        .clock(clock.clone())
        .run_limits(TaskRunLimits {
            deadline_epoch_ms: Some(deadline),
            ..Default::default()
        });
    let harness = LocalHarness::open(config).await?;
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) =
        harness.submit(operation, "parent").await?
    else {
        panic!("parent was not admitted");
    };
    let host = harness.runtime.task_host();
    let WorkPull::Claimed(lease) = host
        .pull_work(
            &Worker {
                id: "agent-file-deadline".into(),
                available: Default::default(),
                labels: Default::default(),
            },
            Some(operation),
            &mut None,
        )
        .await?
    else {
        panic!("parent was not claimed");
    };
    host.start_task(&lease, &mut None).await?;
    let context = harness
        .runtime
        .task_context(task_id, LeaseFence::from(&lease.reservation))
        .await?;
    let (condition, bound) = context
        .owned_mailbox
        .as_ref()
        .unwrap()
        .file_publication_condition()
        .await?;
    assert_eq!(bound, Some(deadline));
    let files = harness.task_files();
    let snapshot = files
        .observe(Query::Children, None, 8)
        .await
        .map_err(super::super::storage)?
        .snapshot
        .unwrap();
    // Expire after ownership was checked, before the provider accepts the file.
    clock.0.store(deadline, Ordering::SeqCst);
    let request = FileValue::new(
        FileKey::Child {
            reference: Component::new("late").map_err(super::super::storage)?,
            file: ChildFile::Request,
        },
        b"late work".to_vec(),
    );
    assert!(matches!(
        files
            .write_if(snapshot.generation, &request, task_id, condition, bound)
            .await,
        Err(crate::task_volume::Error::Storage(
            acyclic_stream::StreamError::DeadlineElapsed
        ))
    ));
    let after = files
        .observe(Query::Children, None, 8)
        .await
        .map_err(super::super::storage)?
        .snapshot
        .unwrap();
    assert_eq!(after.generation, snapshot.generation);
    assert!(after.files.is_empty());
    Ok(())
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn delegate_dispatch_uses_spawn_and_publishes_a_child_result() -> Result<()> {
    use crate::client::{CommandResolution, CommandTool};
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([ScriptedTurn::Text("delegated answer".into())]);
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
    .await?;
    let result = harness
        .dispatch_command(
            OperationId::new(),
            None,
            CommandResolution::Tool {
                tool: CommandTool::Spawn,
                input: json!({"task":"delegate this task", "name":"delegate"}),
            },
        )
        .await?;
    assert_eq!(result, json!({"agent":"delegate"}));
    let snapshot = harness
        .task_files()
        .observe(
            Query::File(FileKey::Child {
                reference: Component::new("delegate").map_err(super::super::storage)?,
                file: crate::task_volume::ChildFile::Result(1),
            }),
            None,
            1,
        )
        .await
        .map_err(super::super::storage)?
        .snapshot
        .unwrap();
    let result = crate::task_volume::ChildTurnResult::parse(&snapshot.files[0].bytes)
        .map_err(super::super::storage)?;
    assert!(
        matches!(result.outcome, crate::Outcome::Succeeded(ref answer) if answer.text == "delegated answer")
    );
    assert_eq!(provider.requests().len(), 1);
    harness.close().await?;
    Ok(())
}
