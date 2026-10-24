use super::*;
use crate::task_volume::{Component, FileKey, Query, TaskFile, ToolFile, Write};
use crate::{Admission, LocalHarness, model::Model, test_support::ScriptedModelProvider};
use futures::StreamExt;

mod cancellation;
mod health;
mod mount_owner;
mod status_cost;

#[tokio::test]
async fn client_goal_write_between_bash_chunks_preserves_the_command() -> Result<()> {
    let (harness, context, directory, state) = fixture(None).await?;
    let task = Arc::new(context.task().clone());
    let task_id = context.task().durable_task_id().expect("durable fixture");
    exercise_goal_write(&harness, context, task, task_id).await?;
    harness.disconnect_unfinished().await?;
    drop((directory, state));
    Ok(())
}

#[tokio::test]
async fn small_output_publication_cost() -> Result<()> {
    let (harness, context, _directory, _state) = fixture(None).await?;
    let mut output =
        Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    let before = harness.task_files().inner.head().await?.position;
    let started = std::time::Instant::now();
    output = tokio::task::spawn_blocking(move || {
        for _ in 0..128 {
            output.observe(ProcessOutput::Stdout(b"stdout line\n"))?;
            output.observe(ProcessOutput::Stderr(b"stderr line\n"))?;
        }
        Ok::<_, std::io::Error>(output)
    })
    .await
    .map_err(storage)?
    .map_err(storage)?;
    output
        .finish(&ToolResult {
            value: serde_json::json!({"success": true}),
        })
        .await?;
    let elapsed = started.elapsed();
    let files = snapshot(&harness, &context).await?;
    let counts = publications(&files);
    eprintln!(
        "OUTPUT_COST chunks=256 bytes=3072 stdout_publications={} stderr_publications={} elapsed_us={}",
        counts.0,
        counts.1,
        elapsed.as_micros()
    );
    assert_eq!(
        counts,
        (1, 1),
        "small chunks must coalesce independently of pipe fragmentation"
    );
    assert_eq!(
        files.generation.position - before,
        3,
        "two output batches and the result"
    );
    assert_eq!(stream(&files, false), b"stdout line\n".repeat(128));
    assert_eq!(stream(&files, true), b"stderr line\n".repeat(128));
    harness.disconnect_unfinished().await?;
    Ok(())
}

#[tokio::test]
async fn bounded_batches_preserve_binary_output_and_flush_both_tails() -> Result<()> {
    let (harness, context, _directory, _state) = fixture(None).await?;
    let output = Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    let stdout: Vec<_> = (0u8..=255).cycle().take(2 * 64 * 1024 + 17).collect();
    let stderr = vec![0xff; 64 * 1024 + 3];
    let expected = (stdout.clone(), stderr.clone());
    let mut output = tokio::task::spawn_blocking(move || output_chunks(output, &stdout, &stderr))
        .await
        .map_err(storage)?
        .map_err(storage)?;
    let during = snapshot(&harness, &context).await?;
    let counts = publications(&during);
    assert!(
        counts.0 >= 2 && counts.1 >= 1,
        "full batches publish during capture"
    );
    output
        .finish(&ToolResult {
            value: serde_json::json!({"success": false, "exit_code": 1}),
        })
        .await?;
    let files = snapshot(&harness, &context).await?;
    assert_eq!(stream(&files, false), expected.0);
    assert_eq!(stream(&files, true), expected.1);
    for file in &files.files {
        if matches!(
            &file.key,
            FileKey::Task {
                file: TaskFile::Tool {
                    file: ToolFile::Stdout(_) | ToolFile::Stderr(_),
                    ..
                },
                ..
            }
        ) {
            assert!(!file.bytes.is_empty());
            assert!(file.bytes.len() <= 64 * 1024);
            assert!(crate::task_volume::record_bytes(file)? <= acyclic_stream::MAX_RECORD_BYTES);
        }
    }
    harness.disconnect_unfinished().await?;
    Ok(())
}

fn output_chunks(mut output: Output, stdout: &[u8], stderr: &[u8]) -> std::io::Result<Output> {
    output.observe(ProcessOutput::Stdout(&[]))?;
    output.observe(ProcessOutput::Stderr(&[]))?;
    let (first, rest) = stdout.split_at(3);
    output.observe(ProcessOutput::Stdout(first))?;
    output.observe(ProcessOutput::Stderr(stderr))?;
    output.observe(ProcessOutput::Stdout(rest))?;
    Ok(output)
}

#[tokio::test]
async fn cancellation_settles_captured_tails_under_the_original_owner() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let (harness, context, _directory, _state) = fixture(Some(&cancellation)).await?;
    let mut output =
        Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    output = tokio::task::spawn_blocking(move || output_chunks(output, b"captured", b"stopped"))
        .await
        .map_err(storage)?
        .map_err(storage)?;
    cancellation.cancel();
    assert!(output.owner.verify(false, &mut None).await.is_err());
    output.owner.verify(true, &mut None).await?;
    assert!(
        output
            .writer
            .lock()
            .await
            .flush(false, false)
            .await
            .is_err(),
        "cancelled owners cannot publish a fresh batch"
    );
    output
        .finish(&ToolResult {
            value: serde_json::json!({"success": false, "stop": "cancelled"}),
        })
        .await?;
    let files = snapshot(&harness, &context).await?;
    assert_eq!(stream(&files, false), b"captured");
    assert_eq!(stream(&files, true), b"stopped");
    harness.disconnect_unfinished().await?;
    Ok(())
}

async fn query(harness: &LocalHarness, context: &ToolContext) -> Result<Query> {
    let task = Component::new(
        context
            .task()
            .durable_task_id()
            .ok_or_else(|| Error::Conflict("output fixture has no durable task".into()))?
            .to_string(),
    )?;
    let turn = harness
        .task_files()
        .inner
        .model_turn(
            task.clone(),
            Component::new(context.model_turn.0.to_string())?,
            context.model_turn.1,
            None,
        )
        .await?;
    Ok(Query::Tool {
        task,
        turn,
        call: Component::new(context.operation_id().to_string())?,
    })
}

async fn snapshot(
    harness: &LocalHarness,
    context: &ToolContext,
) -> Result<crate::task_volume::Snapshot> {
    Ok(harness
        .task_files()
        .inner
        .snapshot(&query(harness, context).await?, 512)
        .await?)
}

fn publications(snapshot: &crate::task_volume::Snapshot) -> (usize, usize) {
    let mut counts = (0, 0);
    for file in &snapshot.files {
        match &file.key {
            FileKey::Task {
                file:
                    TaskFile::Tool {
                        file: ToolFile::Stdout(_),
                        ..
                    },
                ..
            } => counts.0 += 1,
            FileKey::Task {
                file:
                    TaskFile::Tool {
                        file: ToolFile::Stderr(_),
                        ..
                    },
                ..
            } => counts.1 += 1,
            _ => {}
        }
    }
    counts
}

#[tokio::test]
async fn quiet_command_publishes_output_before_exit() -> Result<()> {
    let (harness, context, _directory, _state) = fixture(None).await?;
    let mut output =
        Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    let mut observation = harness
        .task_files()
        .observe(query(&harness, &context).await?, None, 16)
        .await?;
    output = tokio::task::spawn_blocking(move || output_chunks(output, b"ready", b"warning"))
        .await
        .map_err(storage)?
        .map_err(storage)?;
    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut seen = (false, false);
        while !seen.0 || !seen.1 {
            let change = observation
                .changes
                .next()
                .await
                .expect("output follow remains open")?;
            for file in change.file.iter().chain(&change.files) {
                match &file.key {
                    FileKey::Task {
                        file:
                            TaskFile::Tool {
                                file: ToolFile::Stdout(_),
                                ..
                            },
                        ..
                    } => seen.0 = true,
                    FileKey::Task {
                        file:
                            TaskFile::Tool {
                                file: ToolFile::Stderr(_),
                                ..
                            },
                        ..
                    } => seen.1 = true,
                    _ => {}
                }
            }
        }
        Ok::<_, crate::Error>(())
    })
    .await
    .expect("quiet commands must publish both streams before exit")?;
    eprintln!(
        "OUTPUT_VISIBILITY elapsed_us={}",
        started.elapsed().as_micros()
    );
    let files = snapshot(&harness, &context).await?;
    assert_eq!(stream(&files, false), b"ready");
    assert_eq!(stream(&files, true), b"warning");
    assert!(!files.files.iter().any(|file| matches!(
        &file.key,
        FileKey::Task {
            file: TaskFile::Tool {
                file: ToolFile::Result,
                ..
            },
            ..
        }
    )));
    output
        .finish(&ToolResult {
            value: serde_json::json!({"success": true}),
        })
        .await?;
    harness.disconnect_unfinished().await?;
    Ok(())
}

#[tokio::test]
async fn dropped_caller_settles_both_captured_tails() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let (harness, context, _directory, _state) = fixture(Some(&cancellation)).await?;
    let output = Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    let mut observation = harness
        .task_files()
        .observe(query(&harness, &context).await?, None, 16)
        .await?;
    let output =
        tokio::task::spawn_blocking(move || output_chunks(output, b"captured", b"stopped"))
            .await
            .map_err(storage)?
            .map_err(storage)?;
    cancellation.cancel();
    drop(output);
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut seen = (false, false);
        while !seen.0 || !seen.1 {
            let change = observation
                .changes
                .next()
                .await
                .expect("cleanup follow remains open")?;
            for file in change.file.iter().chain(&change.files) {
                match &file.key {
                    FileKey::Task {
                        file:
                            TaskFile::Tool {
                                file: ToolFile::Stdout(_),
                                ..
                            },
                        ..
                    } => seen.0 = true,
                    FileKey::Task {
                        file:
                            TaskFile::Tool {
                                file: ToolFile::Stderr(_),
                                ..
                            },
                        ..
                    } => seen.1 = true,
                    _ => {}
                }
            }
        }
        Ok::<_, crate::Error>(())
    })
    .await
    .expect("dropped callers must settle both captured streams")?;
    let files = snapshot(&harness, &context).await?;
    assert_eq!(stream(&files, false), b"captured");
    assert_eq!(stream(&files, true), b"stopped");
    harness.disconnect_unfinished().await?;
    Ok(())
}

fn stream(snapshot: &crate::task_volume::Snapshot, stderr: bool) -> Vec<u8> {
    snapshot.files.iter().filter(|file| matches!(&file.key,
        FileKey::Task { file: TaskFile::Tool { file: ToolFile::Stdout(_), .. }, .. } if !stderr
    ) || matches!(&file.key,
        FileKey::Task { file: TaskFile::Tool { file: ToolFile::Stderr(_), .. }, .. } if stderr
    )).flat_map(|file| file.bytes.iter().copied()).collect()
}

async fn fixture(
    cancellation: Option<&acyclic_fs::CancellationToken>,
) -> Result<(
    LocalHarness,
    ToolContext,
    tempfile::TempDir,
    tempfile::TempDir,
)> {
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
        return Err(Error::Conflict(
            "output fixture admission was not durable".into(),
        ));
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
        return Err(Error::Conflict(
            "output fixture work was not claimed".into(),
        ));
    };
    harness
        .runtime
        .task_host()
        .start_task(&lease, &mut None)
        .await?;
    let task = Arc::new(
        harness
            .runtime
            .task_context_with_cancellation(
                task_id,
                crate::scheduler::LeaseFence::from(&lease.reservation),
                cancellation,
            )
            .await?,
    );
    let context = ToolContext::new(task, OperationId::new(), "bash-call", None)?;
    Ok((harness, context, directory, state))
}

async fn exercise_goal_write(
    harness: &LocalHarness,
    context: ToolContext,
    task: Arc<crate::runtime::TaskContext>,
    task_id: crate::TaskId,
) -> Result<()> {
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
    let mut output = Output::open(
        &context,
        Arc::clone(harness.task_files()),
        b"shell".to_vec(),
    )
    .await?;
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
        .ok_or_else(|| Error::Conflict("goal write observation has no snapshot".into()))?;
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
        .ok_or_else(|| Error::Conflict("output observation has no snapshot".into()))?;
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
    assert_eq!(chunks.concat(), b"beforeafter");
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
        chunks.len() + 3,
        "text, request, stdout and result share one turn"
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
    let Query::Tool {
        turn: next_turn, ..
    } = query(harness, &next).await?
    else {
        unreachable!()
    };
    assert_ne!(next_turn, turn);
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
    output = tokio::task::spawn_blocking(move || {
        // A full frame exercises the synchronous publication fence even when
        // short pipe reads are buffered for the timer or command exit.
        let mut chunk = b"held diagnostic".to_vec();
        chunk.resize(64 * 1024, b'x');
        output.observe(ProcessOutput::Stderr(&chunk))?;
        Ok::<_, std::io::Error>(output)
    })
    .await
    .map_err(storage)?
    .map_err(storage)?;
    assert_eq!(
        clock.held.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the publication's fence step was not held"
    );
    output
        .finish(&ToolResult {
            value: serde_json::json!({"success": true, "exit_code": 0, "stop": null}),
        })
        .await?;
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
            .any(|file| file.bytes.starts_with(b"held diagnostic")),
        "the held chunk was not published"
    );
    Ok(())
}
mod storage;
