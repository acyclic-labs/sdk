use super::*;
use crate::task_volume::Query;
use crate::{Admission, LocalHarness, model::Model, test_support::ScriptedModelProvider};

#[tokio::test]
async fn client_goal_write_between_bash_chunks_preserves_the_command() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let model = Model::new(
        "scripted".into(),
        "output".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(
            directory.path(),
            (model, Arc::new(ScriptedModelProvider::new([]))),
        )?
        .state_directory(state.path()),
    )
    .await?;
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(operation, "bash output").await?
    else {
        panic!("durable admission");
    };
    let worker = crate::distributed::Worker {
        id: "output-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let crate::distributed::WorkPull::Claimed(lease) = harness
        .runtime
        .task_host()
        .pull_work(&worker, Some(operation), &mut None)
        .await?
    else {
        panic!("task claim");
    };
    harness
        .runtime
        .task_host()
        .start_task(&lease, &mut None)
        .await?;
    let task = Arc::new(
        harness
            .runtime
            .task_context(
                task_id,
                crate::scheduler::LeaseFence::from(&lease.reservation),
            )
            .await?,
    );
    let context = ToolContext::new(task.clone(), OperationId::new(), "bash-call", None)?;
    let task_key = Component::new(task_id.to_string())?;
    let turn = harness
        .task_files()
        .inner
        .model_turn(
            task_key.clone(),
            Component::new(context.model_turn.0.to_string())?,
            context.model_turn.1,
            None,
        )
        .await?;
    let head = harness.task_files().inner.head().await?;
    let mut chunk = Write::new(
        FileKey::Task {
            task: task_key.clone(),
            file: TaskFile::Chunk { turn, sequence: 0 },
        },
        b"model text".to_vec(),
    );
    chunk.channel = Some(crate::task_volume::Channel::Text);
    harness.task_files().inner.append(head, &chunk).await?;
    let mut output =
        Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    output = tokio::task::spawn_blocking(move || {
        output.observe(ProcessOutput::Stdout(b"before"))?;
        Ok::<_, std::io::Error>(output)
    })
    .await
    .map_err(storage)?
    .map_err(storage)?;
    let files = harness.task_files();
    let snapshot = files
        .observe(Query::Root, None, 16)
        .await?
        .snapshot
        .unwrap();
    files
        .write(
            snapshot.generation,
            &Write::new(FileKey::Goal, b"client goal".to_vec()),
        )
        .await?;
    output = tokio::task::spawn_blocking(move || {
        output.observe(ProcessOutput::Stdout(b"after"))?;
        Ok::<_, std::io::Error>(output)
    })
    .await
    .map_err(storage)?
    .map_err(storage)?;
    output
        .finish(&ToolResult {
            value: serde_json::json!({"success":true}),
        })
        .await?;
    let snapshot = files
        .observe(Query::Root, None, 16)
        .await?
        .snapshot
        .unwrap();
    assert!(
        snapshot
            .files
            .iter()
            .any(|file| file.key == FileKey::Goal && file.bytes.as_ref() == b"client goal")
    );
    let chunks: Vec<_> = snapshot
        .files
        .iter()
        .filter(|file| {
            matches!(
                file.key,
                FileKey::Task {
                    file: TaskFile::Tool {
                        file: ToolFile::Stdout(_),
                        ..
                    },
                    ..
                }
            )
        })
        .map(|file| file.bytes.as_ref())
        .collect();
    assert_eq!(chunks, [b"before".as_slice(), b"after".as_slice()]);
    let first_turn = files
        .inner
        .snapshot(
            &Query::Turn {
                task: task_key.clone(),
                turn,
            },
            16,
        )
        .await?;
    assert_eq!(
        first_turn.files.len(),
        5,
        "text, request, both stdout chunks and result share one turn"
    );
    // A later input restarts its model step at zero, under a new execution.
    let mut next = ToolContext::new(task, OperationId::new(), "next-call", None)?;
    next.model_turn = (OperationId::new(), 0);
    let mut second = Output::open(&next, files.clone(), b"next bash".to_vec()).await?;
    second
        .finish(&ToolResult {
            value: serde_json::json!({}),
        })
        .await?;
    assert_ne!(second.step, turn);
    assert_eq!(
        files
            .inner
            .snapshot(
                &Query::Turn {
                    task: task_key,
                    turn
                },
                16
            )
            .await?
            .files,
        first_turn.files,
        "a later input cannot overwrite or join the earlier turn"
    );
    harness.disconnect_unfinished().await?;
    Ok(())
}

/// Holds the first clock read after arming, as a loaded host delays a journal
/// fence step. Every other read is the system clock.
#[derive(Default)]
struct HeldClock {
    armed: std::sync::atomic::AtomicBool,
    held: std::sync::atomic::AtomicUsize,
}

impl acyclic_stream::UnixMillisClock for HeldClock {
    fn now_unix_millis(&self) -> u64 {
        if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(
                u64::from(CONTROL_TIMEOUT_MS) + 250,
            ));
            self.held.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock)
    }
}

// Regression: under host load the journal can answer a fenced output
// publication after more than the native control bound. The chunk must still
// be published, so a successful command is never reported as a failed
// capture. Negative control: bounding `await_fence` by `CONTROL_TIMEOUT_MS`
// fails this test with "deadline has elapsed", the main-gate failure of the
// bash stderr and both-streams scenarios.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chunk_whose_fence_step_is_held_past_the_control_bound_is_published() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let clock = Arc::new(HeldClock::default());
    let model = Model::new(
        "scripted".into(),
        "output".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(
            directory.path(),
            (model, Arc::new(ScriptedModelProvider::new([]))),
        )?
        .state_directory(state.path())
        .clock(clock.clone()),
    )
    .await?;
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(operation, "held output").await?
    else {
        panic!("durable admission");
    };
    let worker = crate::distributed::Worker {
        id: "held-output".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let crate::distributed::WorkPull::Claimed(lease) = harness
        .runtime
        .task_host()
        .pull_work(&worker, Some(operation), &mut None)
        .await?
    else {
        panic!("task claim");
    };
    harness
        .runtime
        .task_host()
        .start_task(&lease, &mut None)
        .await?;
    let task = Arc::new(
        harness
            .runtime
            .task_context(
                task_id,
                crate::scheduler::LeaseFence::from(&lease.reservation),
            )
            .await?,
    );
    let context = ToolContext::new(task, OperationId::new(), "bash-call", None)?;
    let mut output =
        Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    clock.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    tokio::task::spawn_blocking(move || output.observe(ProcessOutput::Stderr(b"held diagnostic")))
        .await
        .map_err(storage)?
        .map_err(storage)?;
    assert_eq!(
        clock.held.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the publication's fence step was not held"
    );
    let snapshot = harness
        .task_files()
        .observe(Query::Root, None, 16)
        .await?
        .snapshot
        .unwrap();
    assert!(
        snapshot
            .files
            .iter()
            .any(|file| file.bytes.as_ref() == b"held diagnostic"),
        "the held chunk was not published"
    );
    Ok(())
}
