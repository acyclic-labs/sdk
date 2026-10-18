use super::*;
use crate::{
    context::ModelContextCapacity,
    distributed::{WorkPull, Worker},
    filesystem::TaskCommandProgress,
    model::{Model, ModelEvent, ToolCall},
    runtime::{RuntimeTask, TaskContext},
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
};
use acyclic_fs::facade::FsWork as _;
use serde_json::Value;
use std::io::Write as _;

struct Parent<'a> {
    inner: &'a dyn TaskCommandHost,
    scopes: tokio::sync::mpsc::UnboundedSender<acyclic_fs::CancellationToken>,
    fail: bool,
    trigger: acyclic_fs::CancellationToken,
}
impl TaskCommandHost for Parent<'_> {
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            if command.kind != crate::filesystem::MODEL_TASK_COMMAND_KIND {
                return self.inner.execute(context, fence, command, payload).await;
            }
            let token = context
                .invocation_cancellation()
                .expect("parent effect has an invocation scope")
                .clone();
            self.scopes.send(token.clone()).unwrap();
            if self.fail {
                self.trigger.cancelled().await;
                Err(Error::Storage("parent failure".into()))
            } else {
                token.cancelled().await;
                Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled,
                ))
            }
        })
    }
    fn reconcile<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        self.inner.reconcile(context, fence, command, payload)
    }
}

#[tokio::test]
async fn child_error_drains_a_siblings_native_capture_and_result() -> Result<()> {
    exercise(false).await
}
#[tokio::test]
async fn parent_error_drains_a_childs_native_capture_and_result() -> Result<()> {
    exercise(true).await
}

async fn exercise(parent_fails: bool) -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let gate_directory = tempfile::tempdir().map_err(storage)?;
    let gate = gate_directory.path().join("resume");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&gate)
            .status()
            .map_err(storage)?
            .success()
    );
    // Holding both ends makes the test's write finite even in the broken case
    // where the native effect's consumer future was dropped.
    let mut release = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&gate)
        .map_err(storage)?;
    let path = gate
        .to_str()
        .ok_or_else(|| Error::Invalid("test FIFO path".into()))?
        .replace('\'', "'\"'\"'");
    let shell = format!(
        "printf 'sibling-ready\n'; read marker < '{path}'; printf sibling-published > sibling.txt"
    );
    let mut turns = vec![ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            "shell".into(),
            "bash".into(),
            serde_json::json!({"command":shell}),
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: Value::Null,
        }),
    ])];
    if !parent_fails {
        turns.push(ScriptedTurn::Events(vec![ScriptedEvent::Error {
            error: "child failure".into(),
        }]));
    }
    turns.push(ScriptedTurn::Text("sibling answer".into()));
    let provider = Arc::new(ScriptedModelProvider::new(turns).with_context_capacity(
        ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: 4096,
        },
    ));
    let model = Model::new("scripted".into(), "errors".into(), "1".into(), Value::Null)?;
    let config =
        crate::test_support::local_harness_config(directory.path(), (model, provider.clone()))?;
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
        id: "sibling-drain".into(),
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
    let sibling = Component::new("sibling").map_err(storage)?;
    let mut request = file(&sibling, ChildFile::Request, b"shell sibling".to_vec());
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
    let (scopes, mut scope) = tokio::sync::mpsc::unbounded_channel();
    let trigger = acyclic_fs::CancellationToken::new();
    let inner = harness.commands()?;
    let commands = Parent {
        inner: &inner,
        scopes,
        fail: parent_fails,
        trigger: trigger.clone(),
    };
    let run = harness.advance_with_children(parent, &commands, files.clone(), None);
    let client = async {
        let parent_scope = scope.recv().await.expect("parent model starts");
        let mut observation = files
            .observe(Query::Child(sibling.clone()), None, 16)
            .await
            .map_err(storage)?;
        let snapshot = observation.snapshot.take().unwrap();
        let mut values = snapshot
            .files
            .into_iter()
            .map(|f| (f.key.clone(), f))
            .collect();
        let mut generation = snapshot.generation;
        let child = loop {
            if let Some(status) = execution(&sibling, &values)? {
                break status.task;
            }
            let change = observation
                .changes
                .next()
                .await
                .expect("child status follow")
                .map_err(storage)?;
            advance_fold(&mut values, &mut generation, change);
        };
        let (child_files, child_context) = harness.child_files(child, &files).await?;
        let mut output = child_files
            .observe(
                Query::Task(Component::new(child.to_string()).map_err(storage)?),
                None,
                32,
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
            )
        };
        let mut bytes = Vec::new();
        for value in output
            .snapshot
            .take()
            .unwrap()
            .files
            .iter()
            .filter(|f| stdout(f))
        {
            bytes.extend_from_slice(&value.bytes);
        }
        while !bytes.contains(&b'\n') {
            let change = output
                .changes
                .next()
                .await
                .expect("native stdout follow")
                .map_err(storage)?;
            for value in change
                .file
                .iter()
                .chain(&change.files)
                .filter(|f| stdout(f))
            {
                bytes.extend_from_slice(&value.bytes);
            }
        }
        assert_eq!(bytes, b"sibling-ready\n");
        if parent_fails {
            trigger.cancel();
        } else {
            let failed = Component::new("failed").map_err(storage)?;
            let request = file(&failed, ChildFile::Request, b"fail".to_vec());
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
        }
        // The controller has seen the failure. The sibling finishes through
        // this explicit file event; there is no wall-clock correctness oracle.
        parent_scope.cancelled().await;
        release.write_all(b"resume\n").map_err(storage)?;
        drop(child_context);
        Ok::<_, Error>(child)
    };
    // The client waits only for events the controller publishes before it can
    // finish, so a controller that finishes first fails here with its result
    // instead of leaving the client waiting.
    let (result, child) =
        match futures::future::select(std::pin::pin!(run), std::pin::pin!(client)).await {
            futures::future::Either::Left((result, _)) => {
                panic!("the controller finished before the client released the sibling: {result:?}")
            }
            // A client error can precede the release the controller waits on.
            futures::future::Either::Right((child, run)) => {
                let child = child?;
                (run.await, child)
            }
        };
    assert!(result.is_err());
    let snapshot = files
        .observe(Query::Child(sibling.clone()), None, 16)
        .await
        .map_err(storage)?
        .snapshot
        .unwrap();
    let values = snapshot
        .files
        .into_iter()
        .map(|f| (f.key.clone(), f))
        .collect();
    let status = execution(&sibling, &values)?.unwrap();
    assert_eq!(status.task, child);
    assert!(status.settled);
    assert_eq!(status.state, ChildState::Running);
    let value = &values[&FileKey::Child {
        reference: sibling,
        file: ChildFile::Result(1),
    }];
    let result = crate::task_volume::ChildTurnResult::parse(&value.bytes).map_err(storage)?;
    assert!(
        matches!(result.outcome, crate::Outcome::Succeeded(ref out) if out.text == "sibling answer")
    );
    let (_, view) = harness.admit_child_request(&files, &request).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let mut work = budget.accounting(&cancellation);
    assert_eq!(
        view.read("/sibling.txt", 1024, &mut work)
            .await
            .map_err(storage)?
            .bytes
            .as_ref(),
        b"sibling-published"
    );
    assert_eq!(provider.requests().len(), if parent_fails { 2 } else { 3 });
    host.cancel(parent).await?;
    Ok(())
}
