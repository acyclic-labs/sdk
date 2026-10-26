//! A child's checkout commands never wait for its parent's running command.
use super::*;
use crate::runtime::RuntimeTask;
use serde_json::json;
use std::time::Duration;

async fn shell(tools: &CheckoutTools, task: &Arc<crate::runtime::TaskContext>) -> Result<()> {
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "shell".into(),
        name: crate::tools::shell::PLATFORM.name.into(),
        arguments: json!({"command": "echo child"}),
    };
    let context = ToolContext::new(
        task.clone(),
        invocation.operation_id,
        &invocation.call_id,
        None,
    )?;
    let result = tools.execute(invocation, Some(context)).await?;
    assert_eq!(result.value["exit_code"], 0, "{}", result.value);
    Ok(())
}

#[tokio::test]
async fn a_child_command_runs_while_its_parent_command_holds_the_checkout() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let project = super::super::tests::volume(directory.path()).await?;
    let provider = Arc::new(
        crate::test_support::ScriptedModelProvider::new([]).with_context_capacity(
            crate::context::ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: Some(4096),
            },
        ),
    );
    let harness = LocalHarness::open(super::super::tests::volume_config(
        directory.path(),
        project,
        provider,
    )?)
    .await?;
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = harness.submit(OperationId::new(), "parent").await?
    else {
        panic!("parent admission")
    };
    let workspace = harness.workspace.as_ref().unwrap().clone();
    let root = CheckoutTools::new(
        Arc::clone(&workspace),
        &harness.project,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        &harness.signed,
        Arc::clone(&harness.task_files),
    )
    .await?;
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable { task_id: child, .. }) =
        harness.submit_to(operation, Some(parent), "child")?.await?
    else {
        panic!("child admission")
    };
    let worker = crate::distributed::Worker {
        id: "child-progress".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let crate::distributed::WorkPull::Claimed(lease) = harness
        .runtime
        .task_host()
        .pull_work(&worker, Some(operation), &mut None)
        .await?
    else {
        panic!("child claim")
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
                child,
                crate::scheduler::LeaseFence::from(&lease.reservation),
            )
            .await?,
    );
    let context = ToolContext::new(task.clone(), OperationId::new(), "select", None)?;
    // The child's first command forks it from the parent's selected branch,
    // which alone takes the parent's guard.
    shell(&root.select(&context).await?.unwrap(), &task).await?;

    let command = lock_owned(StateArc::clone(&root.state)).await;
    let child_command = async {
        let tools = root.select(&context).await?.unwrap();
        shell(&tools, &task).await
    };
    tokio::time::timeout(Duration::from_secs(60), child_command)
        .await
        .map_err(|_| Error::Conflict("the child waited for its parent's command".into()))??;
    // Negative control: a command of the parent itself does wait.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(200),
            serialized(&root.state, async |_| ())
        )
        .await
        .is_err(),
        "the parent's command did not hold its checkout"
    );
    drop(command);

    assert_eq!(workspace.children.live(), 1);
    let mounted = root.select(&context).await?.unwrap().workspace.path.clone();
    assert!(mounted.is_dir());
    workspace.settle_child(child).await?;
    assert_eq!(workspace.children.live(), 0);
    // Retirement removes the directory the view was mounted on.
    assert!(!mounted.exists(), "{mounted:?} outlived its view");
    // A settled child that runs again gets a fresh view.
    shell(&root.select(&context).await?.unwrap(), &task).await?;
    assert_eq!(workspace.children.live(), 1);
    workspace.settle_child(child).await?;
    assert_eq!(workspace.children.live(), 0);
    Ok(())
}
