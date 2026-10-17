//! Crash cuts at accepted production boundaries; absent from release builds.
use super::*;
use crate::{
    context::ModelContextCapacity,
    distributed::{WorkPull, Worker},
    runtime::{RuntimeTask, TaskStateProvider, ToolContext},
    task_volume::{ChildFile, Component, Control, FileKey, FileValue, Query},
    test_support::{ScriptedModelProvider, ScriptedTurn},
    tool::ToolInvocation,
};
use acyclic_fs::facade::FsWork as _;
use std::cell::Cell;

thread_local! {
    static CUT: Cell<Option<&'static str>> = const { Cell::new(None) };
}

pub(super) fn arm(point: &'static str) {
    CUT.with(|cut| cut.set(Some(point)));
}

pub(super) fn armed() -> bool {
    CUT.with(|cut| cut.get().is_some())
}

pub(super) fn stop(point: &'static str, operation: OperationId) -> Result<()> {
    if CUT.with(|cut| cut.get() == Some(point)) {
        CUT.with(|cut| cut.set(None));
        return Err(Error::Indeterminate(operation));
    }
    Ok(())
}

#[tokio::test]
async fn child_file_recovery_prepared_before_merge() -> Result<()> {
    child_file_recovery("prepared").await
}

#[tokio::test]
async fn child_file_recovery_admitted_before_merge() -> Result<()> {
    child_file_recovery("admitted").await
}

#[tokio::test]
async fn child_file_recovery_input_sent_before_merge() -> Result<()> {
    child_file_recovery("input-sent").await
}

#[tokio::test]
async fn child_file_recovery_status_before_merge() -> Result<()> {
    child_file_recovery("status").await
}

async fn child_file_recovery(point: &'static str) -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("retained answer".into())])
            .with_context_capacity(ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: 4096,
            }),
    );
    let model = crate::model::Model::new(
        "scripted".into(),
        "recovery".into(),
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
    let worker = Worker {
        id: "child-file-crash-cuts".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let host = harness.runtime.task_host();
    let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
    else {
        panic!("parent lease")
    };
    host.start_task(&lease, &mut None).await?;
    let fence = LeaseFence::from(&lease.reservation);
    let context = Arc::new(harness.runtime.task_context(parent, fence.clone()).await?);
    // Pin authored content. An uncaptured native source is a shared live
    // capability and is not an authored SDK generation.
    write_source(&harness, &context, "original").await?;
    let files = Arc::clone(&harness.task_files);
    let reference = Component::new("worker").map_err(storage)?;
    let mut request = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Request,
        },
        b"answer".to_vec(),
    );
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
    let expected = TaskId::from_bytes(
        admission::child_operation(files.volume(), reference.as_str(), b"child")?.into_bytes(),
    );
    CUT.with(|cut| cut.set(Some(point)));
    let interrupted = harness
        .service_child(files.clone(), reference.clone(), request.clone(), None)
        .await;
    let reached = CUT.with(|cut| cut.replace(None).is_none());
    assert!(
        reached
            && matches!(interrupted,
            Err(Error::Indeterminate(operation)) if operation == OperationId::from_bytes(expected.into_bytes())),
        "crash cut {point} was not reached"
    );
    assert!(provider.requests().is_empty());
    drop(context);
    drop(files);
    harness.disconnect().await?;

    let reopened = LocalHarness::open(config()?).await?;
    let files = Arc::clone(&reopened.task_files);
    let parent_context = Arc::new(reopened.runtime.task_context(parent, fence.clone()).await?);
    write_source(&reopened, &parent_context, "changed after cut").await?;
    drop(parent_context);
    let (child, view) = reopened
        .admit_child_request(files.as_ref(), &request)
        .await?;
    assert_eq!(child, expected);
    let (same, replay) = reopened
        .admit_child_request(files.as_ref(), &request)
        .await?;
    assert_eq!(
        (same, replay.workspace().id()),
        (child, view.workspace().id())
    );
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let mut accounting = budget.accounting(&cancellation);
    assert_eq!(
        view.read("/source.txt", 1024, &mut accounting)
            .await
            .map_err(storage)?
            .bytes
            .as_ref(),
        b"original"
    );
    drop(accounting);
    let host = reopened.runtime.task_host();
    let WorkPull::Claimed(lease) = host
        .pull_work(
            &worker,
            Some(OperationId::from_bytes(child.into_bytes())),
            &mut None,
        )
        .await?
    else {
        panic!("child lease")
    };
    host.start_task(&lease, &mut None).await?;
    let context = reopened
        .runtime
        .task_context(child, LeaseFence::from(&lease.reservation))
        .await?;
    let admitted = host.observe_admission(child).await?;
    let command: crate::workflow::WorkflowCommand =
        serde_json::from_value(admitted.input["model"].clone()).map_err(storage)?;
    let bytes = context.read_file(&command.payload, None).await?;
    let model: crate::filesystem::ModelTaskCommand =
        crate::contract::json_from_slice(&bytes).map_err(storage)?;
    let project = model.selected_output_volume.unwrap();
    context
        .stage_file(
            Some(&project),
            OperationId::new(),
            "child.txt",
            b"retained change",
            "text/plain",
            "child.txt",
        )
        .await?;
    drop(context);
    reopened
        .service_child(files.clone(), reference.clone(), request.clone(), None)
        .await?;
    let generation = files.inner.head().await.map_err(storage)?;
    files
        .write(
            generation,
            &FileValue::new(
                FileKey::Child {
                    reference: reference.clone(),
                    file: ChildFile::Control,
                },
                Control::Archive.bytes().to_vec(),
            ),
        )
        .await
        .map_err(storage)?;
    reopened
        .service_child(files.clone(), reference.clone(), request, None)
        .await?;
    let result = files
        .observe(
            Query::File(FileKey::Child {
                reference,
                file: ChildFile::Result(1),
            }),
            None,
            1,
        )
        .await
        .map_err(storage)?
        .snapshot
        .unwrap();
    let result =
        crate::task_volume::ChildTurnResult::parse(&result.files[0].bytes).map_err(storage)?;
    assert!(
        matches!(result.outcome, crate::Outcome::Succeeded(ref answer)
        if answer.text == "retained answer")
    );
    assert_eq!(provider.requests().len(), 1);
    let parent_context = reopened.runtime.task_context(parent, fence).await?;
    let operation = OperationId::new();
    let context = ToolContext::new(Arc::new(parent_context), operation, "merge", None)?;
    let tool = reopened.tools().get("fs", None, None)?.unwrap();
    let result = tool
        .executor
        .execute(
            ToolInvocation {
                operation_id: operation,
                call_id: "merge".into(),
                name: "fs".into(),
                arguments: serde_json::json!({"command":"merge agents/worker"}),
            },
            Some(context),
        )
        .await?;
    let command: acyclic_fs::GitCommandOutput =
        serde_json::from_value(result.value["result"].clone()).map_err(storage)?;
    let acyclic_fs::GitCommandOutput::Committed(commit) = command else {
        panic!("child merge did not commit: {command:?}");
    };
    assert_eq!(commit.message, "merge agents/worker");
    assert!(matches!(commit.tree, acyclic_fs::GitTreeRef::Exact(_)));

    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let mut accounting = budget.accounting(&cancellation);
    assert_eq!(
        reopened
            .workspace()
            .unwrap()
            .view()
            .read("/child.txt", 1024, &mut accounting)
            .await
            .map_err(storage)?
            .bytes
            .as_ref(),
        b"retained change"
    );
    reopened.disconnect().await?;
    Ok(())
}

async fn write_source(
    harness: &LocalHarness,
    context: &Arc<crate::runtime::TaskContext>,
    text: &str,
) -> Result<()> {
    let operation = OperationId::new();
    let tool = harness.tools().get("write", None, None)?.unwrap();
    tool.executor
        .execute(
            ToolInvocation {
                operation_id: operation,
                call_id: "source-write".into(),
                name: "write".into(),
                arguments: serde_json::json!({"path":"source.txt","text":text,
            "media_type":"text/plain","display_name":"source.txt"}),
            },
            Some(ToolContext::new(
                Arc::clone(context),
                operation,
                "source-write",
                None,
            )?),
        )
        .await?;
    Ok(())
}

mod follow_probe;
pub(super) use follow_probe::probe as follow_probe;
