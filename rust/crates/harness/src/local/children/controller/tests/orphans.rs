use super::*;
use crate::{
    distributed::{WorkPull, Worker},
    runtime::RuntimeTask,
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
};

async fn add_child(
    harness: &LocalHarness,
    files: &Files,
    parent: TaskId,
    name: &str,
) -> Result<(TaskId, Arc<Files>, FileValue)> {
    let WorkPull::Claimed(lease) = harness
        .runtime
        .task_host()
        .recover_work(parent, &mut None)
        .await?
    else {
        panic!("parent lease");
    };
    let context = harness
        .runtime
        .task_context(parent, LeaseFence::from(&lease.reservation))
        .await?;
    let reference = Component::new(name).map_err(storage)?;
    let mut request = file(&reference, ChildFile::Request, b"work".to_vec());
    let (condition, deadline) = context
        .owned_mailbox
        .as_ref()
        .unwrap()
        .file_publication_condition()
        .await?;
    files
        .write_if(
            files.inner.head().await.map_err(storage)?,
            &request,
            parent,
            condition,
            deadline,
        )
        .await
        .map_err(storage)?;
    request.requester = Some(parent);
    let child = harness.admit_child_task(files, &request).await?;
    let (nested, _) = harness.child_files(child, files).await?;
    Ok((child, nested, request))
}

// Exercises the file-backed controller and real scheduler/lease/journal paths.
// All nodes have retained reservations; a child is interrupted at the provider
// boundary. The replacement must retire the whole tree without redispatch.
async fn recover_tree(head_only: bool, cancel_before_reopen: bool) -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let (started, boundary) = tokio::sync::oneshot::channel();
    let started = std::sync::Mutex::new(Some(started));
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![ScriptedEvent::Pending {}]),
            ScriptedTurn::Text("replacement".into()),
        ])
        .with_context_capacity(crate::context::ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: Some(4096),
        })
        .with_dispatch_observer(move |_| {
            if let Some(started) = started.lock().unwrap().take() {
                let _ = started.send(());
            }
        }),
    );
    let model = crate::model::Model::new(
        "scripted".into(),
        "orphans".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let config = || {
        crate::test_support::local_harness_config(
            directory.path(),
            (model.clone(), provider.clone()),
        )
        .map(|config| config.state_directory(state.path()))
    };
    let harness = LocalHarness::open(config()?).await?;
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = harness.submit(operation, "parent").await?
    else {
        panic!("parent admission");
    };
    let host = harness.runtime.task_host();
    let worker = Worker {
        id: "orphan-tree".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
    else {
        panic!("parent lease");
    };
    host.start_task(&lease, &mut None).await?;
    let files = Arc::clone(&harness.task_files);
    let (child, nested, request) = add_child(&harness, &files, parent, "child").await?;
    let (grandchild, _, _) = add_child(&harness, &nested, child, "grandchild").await?;
    let (sibling, _, _) = add_child(&harness, &files, parent, "sibling").await?;
    // Grandchild and sibling stay leased; child enters an actual model call.
    let running = harness.service_child(
        files.clone(),
        Component::new("child").map_err(storage)?,
        request.clone(),
        None,
    );
    match futures::future::select(running, boundary).await {
        futures::future::Either::Right((Ok(()), running)) => {
            let duplicate = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                harness.service_child(
                    files.clone(),
                    Component::new("child").map_err(storage)?,
                    request,
                    None,
                ),
            )
            .await
            .expect("duplicate child controller waited on live execution");
            assert!(matches!(duplicate, Err(Error::Conflict(message))
                if message == "local turn already has an active driver"));
            assert_eq!(provider.dispatches().len(), 1, "live child was reissued");
            drop(running);
        }
        _ => panic!("no child model boundary"),
    }
    if cancel_before_reopen {
        host.cancel(parent).await?;
        // Crash after a parent-only recovery: the head is already terminal,
        // but each running descendant still owns its original reservation.
        host.settle_task(
            parent,
            &LeaseFence::from(&lease.reservation),
            crate::Outcome::Cancelled,
            &mut None,
        )
        .await?;
    }
    drop(nested);
    drop(files);
    harness.disconnect().await?;
    let harness = LocalHarness::open(config()?).await?;
    crate::local::orphans::HEAD_ONLY.set(head_only);
    let admission = harness.submit(OperationId::new(), "replacement").await;
    crate::local::orphans::HEAD_ONLY.set(false);
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: replacement,
        ..
    }) = admission?
    else {
        panic!("replacement admission");
    };
    for task in [parent, child, grandchild, sibling] {
        if !matches!(
            harness.runtime.task_host().outcome(task).await?,
            Some(crate::Outcome::Cancelled)
        ) {
            return Err(Error::Conflict(
                "orphan descendant kept its reservation".into(),
            ));
        }
        assert!(matches!(
            harness
                .runtime
                .task_host()
                .recover_work(task, &mut None)
                .await?,
            WorkPull::Idle
        ));
    }
    assert_eq!(
        provider.dispatches().len(),
        1,
        "settlement dispatched a model"
    );
    assert_eq!(harness.resume(replacement).await?.text, "replacement");
    assert_eq!(provider.dispatches().len(), 2);
    let snapshot = harness
        .task_files
        .inner
        .child_lifecycle(None)
        .await
        .map_err(storage)?;
    let values = snapshot
        .files
        .into_iter()
        .map(|file| (file.key.clone(), file))
        .collect();
    for name in ["child", "sibling"] {
        let reference = Component::new(name).map_err(storage)?;
        let status = execution(&reference, &values)?.unwrap();
        assert!(status.settled);
        assert_eq!(status.state, ChildState::Cancelled);
        assert_eq!(desired(&reference, &values)?, ChildState::Cancelled);
    }
    harness.close().await
}

#[tokio::test]
async fn next_prompt_retires_an_orphan_subtree_and_its_file_controllers() -> Result<()> {
    for cancelled in [false, true] {
        recover_tree(false, cancelled).await?;
    }
    Ok(())
}

#[tokio::test]
async fn negative_control_parent_only_settlement_leaves_descendant_reservations() {
    assert!(
        matches!(recover_tree(true, false).await, Err(Error::Conflict(message)) if message == "orphan descendant kept its reservation")
    );
}
