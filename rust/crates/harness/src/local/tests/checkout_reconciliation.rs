//! Reconciliation and recovered branch switches keep disk and selection aligned.
use super::*;
use acyclic_fs::{CancellationToken, WorkBudget, facade::FsWork as _};

#[tokio::test]
async fn git_reconciliation_publishes_parent_changes_at_close_and_recovery() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "bash", "printf base > note.txt"),
        super::checked_commands::call(1, "fs", "commit -m base"),
        super::checked_commands::call(2, "fs", "checkout -b agents/child"),
        ScriptedTurn::Text("done".into()),
        super::checked_commands::call(3, "fs", "status"),
        super::checked_commands::call(4, "bash", "test $(cat upstream.txt) = closed"),
        ScriptedTurn::Text("done".into()),
        super::checked_commands::call(5, "fs", "status"),
        super::checked_commands::call(6, "bash", "test $(cat upstream.txt) = recovered"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (model()?, provider),
    )?)
    .await?;
    super::checkout_reopen::run(&harness).await?;
    for (expired, expected) in [(false, "closed"), (true, "recovered")] {
        let checkout = harness.workspace().unwrap();
        let selected = checkout.selected_view().await?.0.unwrap();
        let windows = acyclic_fs::OperationWindowCoordinator::new(
            checkout.filesystem.operation_window_store(),
        );
        if expired {
            let parent = selected
                .workspace()
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(super::super::storage)?
                .value;
            windows
                .begin(selected.workspace().id(), parent.id(), "abandoned", 0, 1)
                .await
                .map_err(super::super::storage)?;
        }
        checkout
            .view()
            .workspace()
            .write_text("/upstream.txt", expected)
            .await
            .map_err(super::super::storage)?;
        super::checkout_reopen::run(&harness).await?;
        assert_eq!(
            std::fs::read(directory.path().join("upstream.txt")).map_err(super::super::storage)?,
            expected.as_bytes()
        );
        let head = selected
            .workspace()
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(super::super::storage)?
            .value;
        assert_eq!(
            head.read(
                "/upstream.txt",
                64,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map_err(super::super::storage)?
            .value
            .as_ref(),
            expected.as_bytes()
        );
    }
    Ok(())
}

#[tokio::test]
async fn recovered_branch_switch_is_selected_even_when_the_command_needs_retry() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "bash", "printf main > note.txt"),
        super::checked_commands::call(1, "fs", "commit -m main"),
        super::checked_commands::call(2, "fs", "checkout -b agents/child"),
        super::checked_commands::call(3, "bash", "printf child > note.txt"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (model()?, provider),
    )?)
    .await?;
    super::checkout_reopen::run(&harness).await?;
    let checkout = harness.workspace().unwrap();
    let selected = checkout.selected_view().await?.0.unwrap();
    let tree = selected
        .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()))
        .await
        .map_err(super::super::storage)?;
    let repository = acyclic_fs::GitCompatRepository::new(
        checkout.view().workspace().id(),
        checkout.store.clone(),
    );
    assert!(matches!(
        repository
            .execute(
                acyclic_fs::GitCommand::Switch {
                    branch: "main".into(),
                    create: false
                },
                acyclic_fs::GitTreeRef::Lazy(tree)
            )
            .await
            .map_err(super::super::storage)?,
        acyclic_fs::GitCommandOutput::Prepared { .. }
    ));

    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id: task, .. }) = harness
        .submit(operation, "recover the pending switch")
        .await?
    else {
        panic!("durable admission");
    };
    let worker = crate::distributed::Worker {
        id: "recover-switch".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let crate::distributed::WorkPull::Claimed(lease) = harness
        .runtime
        .task_host()
        .pull_work(&worker, Some(operation), &mut None)
        .await?
    else {
        panic!("claimed task");
    };
    harness
        .runtime
        .task_host()
        .start_task(&lease, &mut None)
        .await?;
    let task = Arc::new(
        harness
            .runtime
            .task_context(task, crate::scheduler::LeaseFence::from(&lease.reservation))
            .await?,
    );
    let tools = harness.tools();
    for (name, command, recovered) in [
        ("fs", "status", true),
        ("bash", "printf selected > note.txt", false),
        ("fs", "status", false),
    ] {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: name.into(),
            name: name.into(),
            arguments: json!({"command": command}),
        };
        let context = ToolContext::new(
            task.clone(),
            invocation.operation_id,
            &invocation.call_id,
            None,
        )?;
        let result = tools
            .get(name, None, None)?
            .unwrap()
            .executor
            .execute(invocation, Some(context))
            .await;
        if recovered {
            assert!(matches!(result, Err(Error::Conflict(_))), "{result:?}");
        } else {
            result?;
        }
    }
    assert!(
        repository
            .pending_transition()
            .await
            .map_err(super::super::storage)?
            .is_none()
    );
    assert_eq!(
        repository
            .checkout_selection()
            .await
            .map_err(super::super::storage)?
            .0,
        checkout.view().workspace().id()
    );
    let head = checkout
        .view()
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(super::super::storage)?
        .value;
    assert_eq!(
        head.read(
            "/note.txt",
            64,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await
        .map_err(super::super::storage)?
        .value
        .as_ref(),
        b"selected"
    );
    let child = selected
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(super::super::storage)?
        .value;
    assert_eq!(
        child
            .read(
                "/note.txt",
                64,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map_err(super::super::storage)?
            .value
            .as_ref(),
        b"child"
    );
    Ok(())
}
