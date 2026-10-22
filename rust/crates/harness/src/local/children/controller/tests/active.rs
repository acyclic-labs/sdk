use super::*;
use crate::{
    context::ModelContextCapacity,
    distributed::{WorkPull, Worker},
    model::{Model, ModelEvent, ToolCall},
    runtime::RuntimeTask,
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
};

#[tokio::test]
async fn active_controls_interrupt_drains_descendants_and_resumes_the_same_child() -> Result<()> {
    exercise(Control::Interrupt).await
}

#[tokio::test]
async fn active_controls_cancel_drains_descendants_and_preserves_siblings() -> Result<()> {
    exercise(Control::Cancel).await
}

#[tokio::test]
async fn active_controls_archive_drains_descendants_and_preserves_siblings() -> Result<()> {
    exercise(Control::Archive).await
}

fn running_shell_command() -> &'static str {
    if cfg!(windows) {
        "[Console]::Write('child-ready'); Start-Sleep -Seconds 300"
    } else {
        "printf child-ready; exec sleep 300"
    }
}

async fn exercise(control: Control) -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let (dispatches, mut dispatched) = tokio::sync::mpsc::unbounded_channel();
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                    "running-shell".into(),
                    crate::tools::shell::PLATFORM.name.into(),
                    serde_json::json!({"command":running_shell_command()}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: serde_json::Value::Null,
                }),
            ]),
            ScriptedTurn::Events(vec![ScriptedEvent::Pending {}]),
            ScriptedTurn::Text("sibling answer".into()),
            ScriptedTurn::Text("resumed answer".into()),
        ])
        .with_context_capacity(ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: Some(4096),
        })
        .with_dispatch_observer(move |dispatch| {
            dispatches.send(dispatch).unwrap();
        }),
    );
    let model = Model::new(
        "scripted".into(),
        "test".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let config =
        crate::test_support::local_harness_config(directory.path(), (model, provider.clone()))?
            .compaction(crate::context::CompactionPolicy::Disabled);
    let harness = LocalHarness::open(config).await?;
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = harness.submit(operation, "parent").await?
    else {
        panic!("parent admission")
    };
    let host = harness.runtime.task_host();
    let worker = Worker {
        id: "active-control-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
    else {
        panic!("parent lease")
    };
    host.start_task(&lease, &mut None).await?;
    let context = harness
        .runtime
        .task_context(parent, LeaseFence::from(&lease.reservation))
        .await?;
    let files = Arc::clone(&harness.task_files);
    let stopped = Component::new("stopped").map_err(storage)?;
    let sibling = Component::new("sibling").map_err(storage)?;
    let mut requests = Vec::new();
    for reference in [&stopped] {
        let mut request = file(reference, ChildFile::Request, b"work".to_vec());
        let generation = files.inner.head().await.map_err(storage)?;
        let (condition, deadline) = context
            .owned_mailbox
            .as_ref()
            .unwrap()
            .file_publication_condition()
            .await?;
        files
            .write_if(generation, &request, parent, condition, deadline)
            .await
            .map_err(storage)?;
        request.requester = Some(parent);
        requests.push(request);
    }
    let running = harness.service_child(files.clone(), stopped.clone(), requests[0].clone(), None);
    let client = async {
        // The actual provider dispatch is the synchronization point: no clocks
        // and no repeated snapshots decide whether the child is running.
        dispatched.recv().await.expect("first child dispatch");
        let (child, _) = harness
            .admit_child_request(files.as_ref(), &requests[0])
            .await?;
        let (nested, context) = harness.child_files(child, files.as_ref()).await?;
        // Observe actual shell output before sending a lifecycle intent. The
        // sleeping command is stopped by control; its duration is no oracle.
        let mut shell = nested
            .observe(
                Query::Task(Component::new(child.to_string()).map_err(storage)?),
                None,
                16,
            )
            .await
            .map_err(storage)?;
        let stdout = |file: &FileValue| {
            matches!(
                file.key,
                FileKey::Task {
                    file: crate::task_volume::TaskFile::Tool {
                        file: crate::task_volume::ToolFile::Stdout(_),
                        ..
                    },
                    ..
                }
            ) && !file.bytes.is_empty()
        };
        let mut started = shell.snapshot.take().unwrap().files.iter().any(stdout);
        while !started {
            let change = shell
                .changes
                .next()
                .await
                .expect("shell output follow ended")
                .map_err(storage)?;
            started = change.file.iter().chain(&change.files).any(stdout);
        }
        let grandchild = Component::new("grandchild").map_err(storage)?;
        let request = file(&grandchild, ChildFile::Request, b"nested work".to_vec());
        let generation = nested.inner.head().await.map_err(storage)?;
        let (condition, deadline) = context
            .owned_mailbox
            .as_ref()
            .unwrap()
            .file_publication_condition()
            .await?;
        nested
            .write_if(generation, &request, child, condition, deadline)
            .await
            .map_err(storage)?;
        dispatched.recv().await.expect("grandchild dispatch");
        let generation = nested.inner.head().await.map_err(storage)?;
        let (condition, deadline) = context
            .owned_mailbox
            .as_ref()
            .unwrap()
            .file_publication_condition()
            .await?;
        nested
            .write_if(
                generation,
                &file(&sibling, ChildFile::Request, b"sibling work".to_vec()),
                child,
                condition,
                deadline,
            )
            .await
            .map_err(storage)?;
        let generation = nested.inner.head().await.map_err(storage)?;
        crate::task_volume::publish(
            &nested.inner,
            generation,
            &file(
                &grandchild,
                ChildFile::Control,
                Control::Cancel.bytes().to_vec(),
            ),
            || async { Ok::<_, crate::task_volume::Error>((Vec::new(), None)) },
        )
        .await
        .map_err(storage)?;
        dispatched
            .recv()
            .await
            .expect("sibling dispatch survives grandchild cancellation");
        let mut observation = nested
            .observe(Query::Child(sibling.clone()), None, 8)
            .await
            .map_err(storage)?;
        let snapshot = observation.snapshot.take().unwrap();
        let mut values = snapshot
            .files
            .into_iter()
            .map(|file| (file.key.clone(), file))
            .collect();
        let mut generation = snapshot.generation;
        loop {
            if let Some(status) = execution(&sibling, &values)?
                && status.settled
            {
                assert_eq!(status.state, ChildState::Running);
                let result = &values[&FileKey::Child {
                    reference: sibling.clone(),
                    file: ChildFile::Result(1),
                }];
                let result =
                    crate::task_volume::ChildTurnResult::parse(&result.bytes).map_err(storage)?;
                assert!(
                    matches!(result.outcome, crate::Outcome::Succeeded(ref answer) if answer.text == "sibling answer")
                );
                break;
            }
            let change = observation
                .changes
                .next()
                .await
                .expect("sibling follow ended")
                .map_err(storage)?;
            advance_fold(&mut values, &mut generation, change);
        }
        let generation = files.inner.head().await.map_err(storage)?;
        files
            .write(
                generation,
                &file(&stopped, ChildFile::Control, control.bytes().to_vec()),
            )
            .await
            .map_err(storage)?;
        Ok::<_, Error>((nested, grandchild))
    };
    let (_, (nested, grandchild)) = futures::try_join!(running, client)?;
    let shell = nested
        .observe(Query::Tasks, None, 32)
        .await
        .map_err(storage)?
        .snapshot
        .unwrap();
    let shell = shell
        .files
        .iter()
        .find(|file| {
            matches!(
                file.key,
                FileKey::Task {
                    file: crate::task_volume::TaskFile::Tool {
                        file: crate::task_volume::ToolFile::Result,
                        ..
                    },
                    ..
                }
            )
        })
        .expect("drained shell result");
    let shell: serde_json::Value = serde_json::from_slice(&shell.bytes).map_err(storage)?;
    assert_eq!(shell["stdout"].as_str(), Some("child-ready"));
    assert_eq!(shell["stop"]["cleanup_completed"].as_bool(), Some(true));
    let snapshot = nested
        .observe(Query::Child(grandchild.clone()), None, 8)
        .await
        .map_err(storage)?
        .snapshot
        .unwrap();
    let values = snapshot
        .files
        .into_iter()
        .map(|file| (file.key.clone(), file))
        .collect();
    let descendant = execution(&grandchild, &values)?.unwrap();
    assert_eq!(descendant.state, ChildState::Cancelled);
    assert!(descendant.settled);
    assert!(matches!(
        host.outcome(descendant.task).await?,
        Some(crate::Outcome::Cancelled)
    ));
    let snapshot = nested
        .observe(Query::Child(sibling.clone()), None, 8)
        .await
        .map_err(storage)?
        .snapshot
        .unwrap();
    let values = snapshot
        .files
        .into_iter()
        .map(|file| (file.key.clone(), file))
        .collect();
    let sibling_status = execution(&sibling, &values)?.unwrap();
    assert!(sibling_status.settled);
    assert_eq!(
        sibling_status.state,
        ChildState::Cancelled,
        "an idle descendant survived parent stop"
    );
    let snapshot = files
        .observe(Query::Children, None, 16)
        .await
        .map_err(storage)?
        .snapshot
        .unwrap();
    let values = snapshot
        .files
        .into_iter()
        .map(|file| (file.key.clone(), file))
        .collect();
    let status = execution(&stopped, &values)?.unwrap();
    assert_eq!(status.state, ChildState::Running.apply(control));
    assert!(status.settled);
    let result = &values[&FileKey::Child {
        reference: stopped.clone(),
        file: ChildFile::Result(1),
    }];
    let result = crate::task_volume::ChildTurnResult::parse(&result.bytes).map_err(storage)?;
    assert!(matches!(result.outcome, crate::Outcome::Cancelled));
    assert_eq!(
        provider.requests().len(),
        3,
        "unexpected model dispatches: {:?}",
        provider.dispatches()
    );
    if control == Control::Interrupt {
        let generation = files.inner.head().await.map_err(storage)?;
        let generation = files
            .write(
                generation,
                &file(
                    &stopped,
                    ChildFile::Control,
                    Control::Resume.bytes().to_vec(),
                ),
            )
            .await
            .map_err(storage)?;
        files
            .write(
                generation,
                &file(
                    &stopped,
                    ChildFile::Inbox(Component::new("next").map_err(storage)?),
                    b"continue".to_vec(),
                ),
            )
            .await
            .map_err(storage)?;
        harness
            .service_child(files.clone(), stopped.clone(), requests[0].clone(), None)
            .await?;
        let snapshot = files
            .observe(Query::Child(stopped.clone()), None, 16)
            .await
            .map_err(storage)?
            .snapshot
            .unwrap();
        let values = snapshot
            .files
            .into_iter()
            .map(|file| (file.key.clone(), file))
            .collect();
        let resumed = execution(&stopped, &values)?.unwrap();
        assert_eq!(resumed.task, status.task);
        assert_eq!(resumed.inputs, 2);
        assert!(resumed.settled);
        assert_eq!(provider.requests().len(), 4);
    }
    host.cancel(parent).await?;
    Ok(())
}
