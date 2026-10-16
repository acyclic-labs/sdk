//! Count actual native advances through the production owner/tool path.
use super::*;
use crate::runtime::{RuntimeTask, TaskStateProvider};
use serde_json::json;
use std::sync::atomic::Ordering;

async fn execute(
    tools: &CheckoutTools,
    task: &Arc<crate::runtime::TaskContext>,
    name: &str,
    command: &str,
) -> Result<ToolResult> {
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: command.into(),
        name: name.into(),
        arguments: json!({"command":command}),
    };
    let context = ToolContext::new(
        task.clone(),
        invocation.operation_id,
        &invocation.call_id,
        None,
    )?;
    Ok(tools
        .execute(invocation, Some(context))
        .await
        .unwrap_or_else(|error| panic!("{name} {command}: {error:?}")))
}

async fn mounted_workspace(
    tools: &CheckoutTools,
    task: &crate::runtime::TaskContext,
) -> Result<Arc<LocalWorkspace>> {
    serialized(&tools.state, async |mut state| {
        tools
            .selected_workspace(&mut state, task.owned_mailbox.as_ref().unwrap().as_ref())
            .await
    })
    .await
}

#[tokio::test]
async fn in_place_git_advances_switched_and_unswitched_mounts_once() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let project = super::super::tests::volume(directory.path()).await?;
    let provider = Arc::new(
        crate::test_support::ScriptedModelProvider::new([]).with_context_capacity(
            crate::context::ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: 4096,
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
    let root = CheckoutTools::new(
        harness.workspace.as_ref().unwrap().clone(),
        &harness.project,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        Arc::clone(&harness.task_files),
    )
    .await?;
    // Each case starts on its own task branch and commits its baseline there.
    for switched in [false, true] {
        eprintln!("counted mount case switched={switched}");
        let operation = OperationId::new();
        let Admission::Accepted(RuntimeTask::Durable { task_id: child, .. }) =
            harness.submit_to(operation, Some(parent), "child").await?
        else {
            panic!("child admission")
        };
        let worker = crate::distributed::Worker {
            id: "count-mount-advances".into(),
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
        let tools = root.select(&context).await?.unwrap();

        if switched {
            execute(&tools, &task, "fs", "git switch -c selected").await?;
        }
        execute(&tools, &task, "bash", "printf original > marker").await?;
        execute(&tools, &task, "fs", "git add marker").await?;
        execute(&tools, &task, "fs", "git commit -m baseline").await?;
        for command in ["git status", "git restore --source HEAD -- marker"] {
            if command.contains("restore") {
                execute(&tools, &task, "bash", "printf changed > marker").await?;
            }
            let workspace = mounted_workspace(&tools, &task).await?;
            let before = workspace.mount_advances.load(Ordering::Relaxed);
            execute(&tools, &task, "fs", command).await?;
            let advances = workspace.mount_advances.load(Ordering::Relaxed) - before;
            eprintln!("{command}, switched={switched}: advances={advances}");
            assert_eq!(
                advances, 1,
                "{command}, switched={switched}: expected one final native advance",
            );
            assert!(
                workspace.checkout_selection().await?.1.is_none(),
                "unacknowledged publication"
            );
            assert_eq!(
                execute(&tools, &task, "bash", "cat marker").await?.value["stdout"],
                "original"
            );
        }
    }
    Ok(())
}
