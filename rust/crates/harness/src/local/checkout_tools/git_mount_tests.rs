//! Count actual native advances through the production owner/tool path.
use super::*;
use crate::runtime::RuntimeTask;
use serde_json::json;
use std::sync::atomic::Ordering;

async fn execute(
    tools: &CheckoutTools,
    task: &Arc<crate::runtime::TaskContext>,
    name: &str,
    command: &str,
) -> Result<ToolResult> {
    execute_with_grants(tools, task, name, command, None).await
}

async fn execute_with_grants(
    tools: &CheckoutTools,
    task: &Arc<crate::runtime::TaskContext>,
    name: &str,
    command: &str,
    grants: Option<Capabilities>,
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
    let context = match grants {
        Some(grants) => context.scoped(grants, task.scope().limits())?,
        None => context,
    };
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
async fn volume_search_does_not_recover_or_advance_a_native_checkout() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let project = super::super::tests::volume(directory.path()).await?;
    let provider = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
    let harness = LocalHarness::open(super::super::tests::volume_config(
        directory.path(),
        project,
        provider,
    )?)
    .await?;
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) =
        harness.submit(operation, "search").await?
    else {
        panic!("search admission")
    };
    let worker = crate::distributed::Worker {
        id: "search-native-preflight".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let crate::distributed::WorkPull::Claimed(lease) = harness
        .runtime
        .task_host()
        .pull_work(&worker, Some(operation), &mut None)
        .await?
    else {
        panic!("search claim")
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
    let workspace = harness.workspace.as_ref().unwrap().clone();
    // Prepare a real selected branch: repository roots deliberately cannot
    // carry reconciliation markers because they have no parent to rebase.
    let setup = crate::tools::git::run(
        crate::tools::git::Workspace {
            filesystem: &workspace.filesystem,
            store: &workspace.store,
            view: crate::tools::git::View::Checkout(workspace.view()),
            root: &workspace.path,
            repository: workspace.view().workspace().id(),
            lease: None,
            checkout: None,
            reserved: &[],
            author: "test",
            authored_at_seconds: 1,
            read_allowed: &|_| true,
            search_progress: None,
        },
        &["switch".into(), "-c".into(), "search-startup".into()],
    )
    .await?;
    let selected = setup.switched.unwrap().into_checkout()?;
    let before = selected
        .workspace()
        .head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(storage)?
        .value;
    let startup_pending = (
        acyclic_fs::OperationId::from_bytes(OperationId::new().into_bytes()),
        acyclic_fs::GitGenerationRef {
            workspace_id: before.workspace().id(),
            generation: before.id(),
        },
    );
    reconciliation::prepare(&workspace, startup_pending.0, startup_pending.1).await?;
    // Exercise first binding construction again after installing the marker;
    // retain the configured tool capabilities in the admitted task.
    lock_owned(StateArc::clone(&workspace.state))
        .await
        .initialized = false;
    let tools = CheckoutTools::new(
        Arc::clone(&workspace),
        &harness.project,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        &harness.signed,
        Arc::clone(&harness.task_files),
    )
    .await?;
    // Constructor control: eager recovery consumed this marker and a fresh
    // checkout's eager watcher recursively enumerated before the first grep.
    assert_eq!(
        workspace.checkout_selection().await?.1,
        Some(startup_pending)
    );
    assert!(
        lock_owned(StateArc::clone(&tools.state))
            .await
            .watcher
            .is_none()
    );
    selected
        .workspace()
        .write_text("/startup.txt", "candidate")
        .await
        .map_err(storage)?;
    let read_grants = Capabilities::new(
        task.scope()
            .grants()
            .iter()
            .filter(|grant| *grant != tools.write_grant)
            .map(str::to_owned),
    );
    let result = execute_with_grants(
        &tools,
        &task,
        "fs",
        "git grep absent startup.txt",
        Some(read_grants.clone()),
    )
    .await?;
    assert_eq!(
        result.value["result"]["Filesystem"]["Data"]["value"]["complete"],
        true
    );
    assert_eq!(
        workspace.checkout_selection().await?.1,
        Some(startup_pending)
    );
    let state = lock_owned(StateArc::clone(&tools.state)).await;
    assert!(state.watcher.is_none());
    assert!(state.children.is_empty());
    drop(state);
    // Negative control: physical commands still require write authority.
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "write-control".into(),
        name: "bash".into(),
        arguments: json!({"command":"true"}),
    };
    let context = ToolContext::new(
        Arc::clone(&task),
        invocation.operation_id,
        &invocation.call_id,
        None,
    )?
    .scoped(read_grants, task.scope().limits())?;
    assert!(matches!(
        tools.execute(invocation, Some(context)).await,
        Err(Error::Unauthorized(_))
    ));
    execute(&tools, &task, "fs", "git switch -c search-view").await?;
    execute(&tools, &task, "bash", "printf needle > marker").await?;
    let workspace = mounted_workspace(&tools, &task).await?;
    let pending = serialized(&tools.state, async |state| {
        let view = state.active.as_ref().unwrap_or_else(|| workspace.view());
        let generation = view
            .workspace()
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(storage)?
            .value;
        let pending = (
            acyclic_fs::OperationId::from_bytes(OperationId::new().into_bytes()),
            acyclic_fs::GitGenerationRef {
                workspace_id: view.workspace().id(),
                generation: generation.id(),
            },
        );
        reconciliation::prepare(&workspace, pending.0, pending.1).await?;
        Ok::<_, Error>(pending)
    })
    .await?;
    let advances = workspace.mount_advances.load(Ordering::Relaxed);
    let result = execute(&tools, &task, "fs", "git grep needle marker").await?;
    assert!(result.value.to_string().contains("needle"), "{result:?}");
    assert_eq!(workspace.mount_advances.load(Ordering::Relaxed), advances);
    assert_eq!(workspace.checkout_selection().await?.1, Some(pending));
    let snapshot = harness
        .task_files
        .inner
        .snapshot(
            &crate::task_volume::Query::Task(crate::task_volume::Component::new(
                task_id.to_string(),
            )?),
            64,
        )
        .await?;
    let progress: Vec<serde_json::Value> = snapshot
        .files
        .iter()
        .filter_map(|file| {
            if matches!(
                file.key,
                crate::task_volume::FileKey::Task {
                    file: crate::task_volume::TaskFile::Tool {
                        file: crate::task_volume::ToolFile::Stdout(_),
                        ..
                    },
                    ..
                }
            ) {
                serde_json::from_slice::<serde_json::Value>(&file.bytes)
                    .ok()
                    .filter(|value| value.get("files_examined").is_some())
            } else {
                None
            }
        })
        .collect();
    assert!(
        progress
            .iter()
            .any(|value| value["files_examined"] == 0 && value["complete"] == false)
    );
    assert!(progress.iter().any(|value| value["files_examined"] == 1
        && value["complete"] == false
        && value["matches"][0]["text"] == "needle"));
    assert!(progress.iter().any(|value| value["complete"] == true));
    assert_eq!(
        progress
            .iter()
            .map(|value| value["matches"].as_array().unwrap().len())
            .sum::<usize>(),
        1,
        "progress duplicated a match in later metadata or terminal updates"
    );

    // A command that needs an exact checkout still performs recovery. Without
    // the search preflight bypass the preceding grep also clears this marker.
    execute(&tools, &task, "fs", "git status").await?;
    assert!(workspace.checkout_selection().await?.1.is_none());
    // Projected checkouts recover the same marker without a native mount.
    // Where a mount exists, the physical control must advance it as well.
    if workspace.mounted.is_some() {
        assert!(workspace.mount_advances.load(Ordering::Relaxed) > advances);
    }
    Ok(())
}

#[tokio::test]
async fn in_place_git_advances_switched_and_unswitched_mounts_once() -> Result<()> {
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
        &harness.signed,
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
        execute(&tools, &task, "shell", "printf original > marker").await?;
        execute(&tools, &task, "fs", "git add marker").await?;
        execute(&tools, &task, "fs", "git commit -m baseline").await?;
        for command in ["git status", "git restore --source HEAD -- marker"] {
            if command.contains("restore") {
                execute(&tools, &task, "shell", "printf changed > marker").await?;
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
                execute(&tools, &task, "shell", "cat marker").await?.value["stdout"],
                "original"
            );
        }
    }
    Ok(())
}
