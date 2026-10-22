//! A task view reaches auxiliary volumes through their existing bindings.
use super::*;
use crate::runtime::RuntimeTask;
use serde_json::json;

async fn shell(
    tools: &CheckoutTools,
    task: &Arc<crate::runtime::TaskContext>,
    arguments: serde_json::Value,
) -> Result<serde_json::Value> {
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "shell".into(),
        name: crate::tools::shell::PLATFORM.name.into(),
        arguments,
    };
    let context = ToolContext::new(
        task.clone(),
        invocation.operation_id,
        &invocation.call_id,
        None,
    )?;
    let result = tools.execute(invocation, Some(context)).await?;
    assert_eq!(result.value["exit_code"], 0, "{}", result.value);
    Ok(result.value)
}

// The child view is itself a native projection. Projecting its scratch volume
// below it once failed on Windows: ProjFS cannot nest virtualization roots.
#[tokio::test]
async fn child_view_projects_volumes_outside_itself() -> Result<()> {
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
    let root = CheckoutTools::new(
        harness.workspace.as_ref().unwrap().clone(),
        &harness.project,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        Arc::clone(&harness.task_files),
    )
    .await?;
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable { task_id: child, .. }) =
        harness.submit_to(operation, Some(parent), "child").await?
    else {
        panic!("child admission")
    };
    let worker = crate::distributed::Worker {
        id: "task-view-volumes".into(),
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

    // Every command enters the volume projections first.
    shell(&tools, &task, json!({"command": if cfg!(windows) { "[IO.File]::WriteAllText('marker', 'kid')" } else { "printf kid > marker" }})).await?;
    shell(
        &tools,
        &task,
        json!({"command": if cfg!(windows) { "[IO.File]::WriteAllText((Join-Path (Get-Location) 'note'), 'scratch')" } else { "printf scratch > note" }, "cwd": ".volumes/scratch"}),
    )
    .await?;
    let read = shell(
        &tools,
        &task,
        json!({"command": if cfg!(windows) { "[Console]::Write((Get-Content -Raw note))" } else { "cat note" }, "cwd": ".volumes/scratch"}),
    )
    .await?;
    assert_eq!(read["stdout"], "scratch");
    // The shared binding lives in the parent's checkout while it is
    // projected. Its provider-owned siblings never enter the child's view.
    let listed = shell(&tools, &task, json!({"command": if cfg!(windows) { "Get-ChildItem -Force .volumes | Select-Object -ExpandProperty Name" } else { "ls -A .volumes" }})).await?;
    let listed = listed["stdout"].as_str().unwrap();
    assert!(!listed.contains("acyclic"), "{listed}");

    let view = &tools.workspace.path;
    let parent = &root.workspace.mounts;
    let mut volumes = 0;
    for (relative, mount) in tools.workspace.mounts.mounts() {
        if relative.is_empty() {
            continue;
        }
        volumes += 1;
        let source = mount.view.source.as_ref().unwrap();
        assert!(!source.destination.starts_with(view), "{relative}");
        assert_eq!(mount.native_root.as_deref(), Some(&*source.destination));
        let (_, bound) = parent.named(Some(&mount.view.name))?;
        assert!(Arc::ptr_eq(source, bound.view.source.as_ref().unwrap()));
    }
    assert_eq!(volumes, 1);
    Ok(())
}
