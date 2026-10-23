//! Reconciliation and recovered branch switches keep disk and selection aligned.
use super::*;
use acyclic_fs::{CancellationToken, WorkBudget, facade::FsWork as _};

#[tokio::test]
async fn git_reconciliation_publishes_parent_changes_at_close_and_recovery() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
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
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider))?
            .state_directory(state.path()),
    )
    .await?;
    assert!(state.path().join("owner.json").is_file());
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
        assert!(state.path().join("owner.json").is_file());
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
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "bash", "printf main > note.txt"),
        super::checked_commands::call(1, "fs", "commit -m main"),
        super::checked_commands::call(2, "fs", "checkout -b agents/child"),
        super::checked_commands::call(3, "bash", "printf child > note.txt"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider))?
            .state_directory(state.path()),
    )
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

#[tokio::test]
async fn projected_merge_conflicts_are_visible_and_resolvable_in_the_same_stock_task() -> Result<()>
{
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "bash", "printf 'base\n' > note.txt"),
        super::checked_commands::call(1, "fs", "commit -m base"),
        super::checked_commands::call(2, "fs", "checkout -b topic"),
        super::checked_commands::call(3, "bash", "printf 'child\n' > note.txt"),
        super::checked_commands::call(4, "fs", "commit -m child"),
        super::checked_commands::call(5, "fs", "checkout main"),
        super::checked_commands::call(6, "bash", "printf 'parent\n' > note.txt"),
        super::checked_commands::call(7, "fs", "commit -m parent"),
        super::checked_commands::call(8, "fs", "merge topic"),
        super::checked_commands::call(9, "fs", "status"),
        super::checked_commands::call(10, "fs", "log"),
        super::checked_commands::call(11, "bash", "false && printf 'wrong\n' > note.txt"),
        super::checked_commands::call(12, "fs", "merge --continue"),
        super::checked_commands::call(
            13,
            "bash",
            "grep -q '^<<<<<<<' note.txt && mv note.txt renamed.txt && printf 'resolved\n' > renamed.txt",
        ),
        super::checked_commands::call(14, "fs", "merge --continue"),
        super::checked_commands::call(15, "fs", "clean -f"),
        ScriptedTurn::Text("resolved".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path())
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) = harness
        .submit(
            OperationId::new(),
            "merge, inspect markers and resolve them",
        )
        .await?
    else {
        panic!("durable admission")
    };
    let commands = harness.commands()?;
    harness
        .advance(task_id, &super::checked_commands::Checked(&commands))
        .await?;
    assert_eq!(harness.resume(task_id).await?.text, "resolved");
    assert_eq!(
        std::fs::read(directory.path().join("renamed.txt")).map_err(super::super::storage)?,
        b"resolved\n"
    );
    assert!(!directory.path().join("note.txt").exists());
    let requests = provider.requests();
    let results: std::collections::BTreeMap<_, _> = requests
        .last()
        .unwrap()
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id,
                content: crate::model::ToolResultContent::Json { value },
                ..
            } => Some((call_id.as_str(), value)),
            _ => None,
        })
        .collect();
    assert!(
        results["checkout-8"]["error"]
            .as_str()
            .unwrap()
            .contains("conflict")
    );
    for call in ["checkout-9", "checkout-10"] {
        assert!(results[call].get("result").is_some(), "{}", results[call]);
    }
    assert_eq!(results["checkout-11"]["success"], false);
    assert!(
        results["checkout-12"]["error"]
            .as_str()
            .unwrap()
            .contains("unresolved")
    );
    assert!(
        results["checkout-14"].get("result").is_some(),
        "merge continuation failed: {}",
        results["checkout-14"]
    );
    assert!(
        results["checkout-15"].get("result").is_some(),
        "{}",
        results["checkout-15"]
    );
    Ok(())
}

// The GraphCoder `fs-conflict-continue-plain` scenario: a seeded checkout
// edited only through the write tool, merged with a conflict, resolved and
// continued. `show HEAD:seed.txt` must then read the resolution from HEAD.
#[tokio::test]
async fn written_conflicts_continue_to_a_head_that_holds_the_resolution() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::write(directory.path().join("seed.txt"), "base\n").map_err(super::super::storage)?;
    let write = |index: usize, text: &str| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                format!("checkout-{index}"),
                "write".into(),
                json!({"path":"seed.txt","text":text,"media_type":"text/plain","display_name":"seed.txt"}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ])
    };
    let fs = |index: usize, command: &str| super::checked_commands::call(index, "fs", command);
    let provider = scripted([
        fs(0, "branch topic"),
        fs(1, "switch topic"),
        write(2, "topic\n"),
        fs(3, "add seed.txt"),
        fs(4, "commit -m \"topic\""),
        fs(5, "switch main"),
        write(6, "main\n"),
        fs(7, "add seed.txt"),
        fs(8, "commit -m \"main\""),
        fs(9, "merge topic"),
        write(10, "resolved\n"),
        fs(11, "add seed.txt"),
        fs(12, "merge --continue"),
        fs(13, "show HEAD:seed.txt"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path())
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) = harness
        .submit(OperationId::new(), "conflict and continue")
        .await?
    else {
        panic!("durable admission")
    };
    super::checked_commands::advance_until_idle(&harness, task_id, &provider).await?;
    assert_eq!(harness.resume(task_id).await?.text, "done");
    let requests = provider.requests();
    let results: std::collections::BTreeMap<_, _> = requests
        .last()
        .unwrap()
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id, content, ..
            } => Some((call_id.clone(), serde_json::to_string(content).unwrap())),
            _ => None,
        })
        .collect();
    assert!(
        results["checkout-9"].contains("conflict"),
        "{}",
        results["checkout-9"]
    );
    assert!(
        !results["checkout-12"].contains("\"error\""),
        "{}",
        results["checkout-12"]
    );
    assert!(
        results["checkout-13"].contains("resolved"),
        "{}",
        results["checkout-13"]
    );
    assert_eq!(
        std::fs::read(directory.path().join("seed.txt")).map_err(super::super::storage)?,
        b"resolved\n"
    );
    Ok(())
}
