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
    harness
        .task_files()
        .inner
        .append(
            head,
            &Write::new(
                FileKey::Task {
                    task: task_key.clone(),
                    file: TaskFile::Chunk { turn, sequence: 0 },
                },
                b"model text".to_vec(),
            ),
        )
        .await?;
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
