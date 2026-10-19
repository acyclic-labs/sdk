use super::*;
use crate::{
    context::ModelContextCapacity,
    distributed::{WorkPull, Worker},
    model::Model,
    runtime::RuntimeTask,
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
};

#[tokio::test]
async fn reopen_finishes_a_retained_model_stop_before_running_the_next_message() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let (sent, mut dispatched) = tokio::sync::mpsc::unbounded_channel();
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![ScriptedEvent::Pending {}]),
            ScriptedTurn::Text("resumed answer".into()),
        ])
        .with_context_capacity(ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: Some(4096),
        })
        .with_dispatch_observer(move |_| {
            sent.send(()).unwrap();
        }),
    );
    let model = Model::new(
        "scripted".into(),
        "stopped".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    // Every reopen shares one state directory outside the checkout.
    let state = tempfile::tempdir().map_err(crate::local::storage)?;
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
        panic!("parent admission")
    };
    let host = harness.runtime.task_host();
    let worker = Worker {
        id: "stopped-reopen".into(),
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
    let files = harness.task_files.clone();
    let reference = Component::new("worker").map_err(storage)?;
    let mut request = file(&reference, ChildFile::Request, b"first message".to_vec());
    let cut = files.inner.head().await.map_err(storage)?;
    let (condition, deadline) = context
        .owned_mailbox
        .as_ref()
        .unwrap()
        .file_publication_condition()
        .await?;
    files
        .write_if(cut, &request, parent, condition, deadline)
        .await
        .map_err(storage)?;
    request.requester = Some(parent);
    crate::local::children::cuts::arm("model-stopped");
    let run = harness.service_child(files.clone(), reference.clone(), request.clone(), None);
    let interrupt = async {
        dispatched.recv().await.expect("child dispatch");
        let cut = files.inner.head().await.map_err(storage)?;
        files
            .write(
                cut,
                &file(
                    &reference,
                    ChildFile::Control,
                    Control::Interrupt.bytes().to_vec(),
                ),
            )
            .await
            .map_err(storage)?;
        Ok::<_, Error>(())
    };
    let (result, client) = futures::join!(run, interrupt);
    client?;
    assert!(
        !crate::local::children::cuts::armed(),
        "stopped journal cut was not reached"
    );
    // The cut stops the model command; its cause reaches the caller.
    assert!(matches!(
        result,
        Err(Error::CommandIndeterminate {
            cause: crate::CommandIndeterminacy {
                command,
                reason: crate::CommandIndeterminacyReason::Failed { code, .. },
            },
            ..
        }) if command == crate::filesystem::MODEL_TASK_COMMAND_KIND
            && code == "ERROR_CODE_INDETERMINATE"
    ));
    let snapshot = files
        .observe(Query::Child(reference.clone()), None, 16)
        .await
        .map_err(storage)?
        .snapshot
        .unwrap();
    let values = snapshot
        .files
        .into_iter()
        .map(|f| (f.key.clone(), f))
        .collect();
    let before = execution(&reference, &values)?.unwrap();
    assert!(!before.settled);
    assert_eq!(provider.requests().len(), 1);
    drop(context);
    drop(files);
    harness.disconnect().await?;
    let reopened = LocalHarness::open(config()?).await?;
    let files = reopened.task_files.clone();
    let cut = files.inner.head().await.map_err(storage)?;
    let cut = files
        .write(
            cut,
            &file(
                &reference,
                ChildFile::Control,
                Control::Resume.bytes().to_vec(),
            ),
        )
        .await
        .map_err(storage)?;
    files
        .write(
            cut,
            &file(
                &reference,
                ChildFile::Inbox(Component::new("next").map_err(storage)?),
                b"next message".to_vec(),
            ),
        )
        .await
        .map_err(storage)?;
    reopened
        .service_child(files.clone(), reference.clone(), request, None)
        .await?;
    let snapshot = files
        .observe(Query::Child(reference.clone()), None, 16)
        .await
        .map_err(storage)?
        .snapshot
        .unwrap();
    let values: BTreeMap<_, _> = snapshot
        .files
        .into_iter()
        .map(|f| (f.key.clone(), f))
        .collect();
    let after = execution(&reference, &values)?.unwrap();
    assert_eq!(after.task, before.task);
    assert!(after.settled);
    assert_eq!(after.inputs, 2);
    for (turn, cancelled) in [(1, true), (2, false)] {
        let value = &values[&FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Result(turn),
        }];
        let result = crate::task_volume::ChildTurnResult::parse(&value.bytes).map_err(storage)?;
        assert_eq!(
            matches!(result.outcome, crate::Outcome::Cancelled),
            cancelled
        );
        if !cancelled {
            assert!(
                matches!(result.outcome, crate::Outcome::Succeeded(ref output) if output.text == "resumed answer")
            );
        }
    }
    assert_eq!(
        provider.requests().len(),
        2,
        "reopen redispatched the stopped turn"
    );
    Ok(())
}
