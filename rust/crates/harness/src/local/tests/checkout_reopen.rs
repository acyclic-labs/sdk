//! The durable branch catalog selects capture before the first reopened command.
use super::*;

#[tokio::test]
async fn immediate_reopen_after_drop_and_close_has_no_retiring_fence() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let config = || {
        super::checkout_config(directory.path(), (model()?, text()))
            .map(|config| config.state_directory(state.path()))
    };
    let harness = LocalHarness::open(config()?).await?;
    drop(harness);
    let harness = LocalHarness::open(config()?).await?;
    harness.close().await?;
    LocalHarness::open(config()?).await?.close().await?;
    Ok(())
}

pub(super) async fn run(harness: &LocalHarness) -> Result<()> {
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id: task, .. }) = harness
        .submit(OperationId::new(), "exercise the selected checkout")
        .await?
    else {
        return Err(Error::Conflict("checkout turn was not admitted".into()));
    };
    let commands = harness.commands()?;
    harness
        .advance(task, &super::checked_commands::Checked(&commands))
        .await?;
    assert_eq!(harness.resume(task).await?.text, "done");
    Ok(())
}

fn commands(commands: &[(&str, &str)]) -> Arc<ScriptedModelProvider> {
    scripted(
        commands
            .iter()
            .enumerate()
            .map(|(index, (name, command))| super::checked_commands::call(index, name, command))
            .chain([ScriptedTurn::Text("done".into())]),
    )
}

#[cfg(unix)]
async fn main_bytes(harness: &LocalHarness) -> Result<acyclic_stream::OwnedBytes> {
    let view = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .view();
    let cancellation = acyclic_fs::CancellationToken::new();
    let head = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(super::super::storage)?
        .value;
    Ok(head
        .read(
            "/note.txt",
            1024,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(super::super::storage)?
        .value)
}

#[cfg(unix)]
#[tokio::test]
async fn reopen_selects_the_persisted_branch_before_capture_and_can_checkout_main() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir(directory.path().join(".git")).map_err(super::super::storage)?;
    let first = commands(&[
        ("bash", "printf main > note.txt"),
        ("fs", "commit -m main"),
        ("fs", "checkout -b agents/child"),
        ("bash", "printf child > note.txt"),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, first.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    run(&harness).await?;
    assert_eq!(main_bytes(&harness).await?.as_ref(), b"main");
    assert_eq!(
        std::fs::read(directory.path().join("note.txt")).map_err(super::super::storage)?,
        b"child"
    );
    assert_eq!(first.requests().len(), 5);
    drop(harness);

    let resumed = commands(&[
        ("fs", "status"),
        ("bash", "printf after-reopen > note.txt"),
        ("fs", "checkout main"),
        ("fs", "status"),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, resumed.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    assert_eq!(main_bytes(&harness).await?.as_ref(), b"main");
    run(&harness).await?;
    assert_eq!(main_bytes(&harness).await?.as_ref(), b"main");
    assert_eq!(
        std::fs::read(directory.path().join("note.txt")).map_err(super::super::storage)?,
        b"main"
    );
    assert_eq!(resumed.requests().len(), 5);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn default_write_publishes_to_the_selected_checkout_and_bash_reads_it() -> Result<()> {
    let _diagnostics = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .with_test_writer()
            .finish(),
    );
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir(directory.path().join(".git")).map_err(super::super::storage)?;
    let write = |index: usize, text: &str| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                format!("write-{index}"),
                "write".into(),
                json!({"path":"from-write.txt","text":text,"media_type":"text/plain","display_name":"from-write.txt"}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ])
    };
    let provider = scripted([
        write(0, "main file"),
        super::checked_commands::call(1, "bash", "cat from-write.txt"),
        super::checked_commands::call(2, "fs", "commit -m initial"),
        super::checked_commands::call(3, "fs", "checkout -b agents/child"),
        write(4, "child file"),
        super::checked_commands::call(5, "bash", "cat from-write.txt"),
        super::checked_commands::call(6, "fs", "checkout main"),
        super::checked_commands::call(7, "bash", "cat from-write.txt"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    run(&harness).await?;
    let requests = provider.requests();
    assert_eq!(requests.len(), 9);
    for content in requests
        .iter()
        .flat_map(|request| &request.messages)
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            crate::model::ModelContentPart::ToolResult { content, .. } => Some(content),
            _ => None,
        })
    {
        let result = serde_json::to_string(content).map_err(super::super::storage)?;
        assert!(!result.contains("\"error\""), "{result}");
    }
    for (id, expected) in [
        ("checkout-1", "main file"),
        ("checkout-5", "child file"),
        ("checkout-7", "main file"),
    ] {
        let result = requests
            .iter()
            .flat_map(|request| &request.messages)
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                crate::model::ModelContentPart::ToolResult {
                    call_id, content, ..
                } if call_id == id => Some(content),
                _ => None,
            })
            .expect("bash result");
        assert!(
            serde_json::to_string(result)
                .map_err(super::super::storage)?
                .contains(expected),
            "{id}: {result:?}"
        );
    }
    assert_eq!(
        std::fs::read(directory.path().join("from-write.txt")).map_err(super::super::storage)?,
        b"main file"
    );
    assert!(!directory.path().join(".system").exists());
    Ok(())
}

#[tokio::test]
async fn echo_reuses_the_lazy_checkout_without_reading_untouched_files() -> Result<()> {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let unread = directory.path().join("unread.txt");
    std::fs::write(&unread, b"unrelated source bytes").map_err(super::super::storage)?;
    #[cfg(unix)]
    std::fs::set_permissions(&unread, std::fs::Permissions::from_mode(0))
        .map_err(super::super::storage)?;
    #[cfg(windows)]
    let _unread_guard = {
        use std::os::windows::fs::OpenOptionsExt as _;
        // Deny every subsequent open until the test ends. An eager importer
        // cannot read this file, while the held lazy checkout can run echo.
        std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(&unread)
            .map_err(super::super::storage)?
    };
    let provider = commands(&[
        ("bash", "echo first"),
        ("bash", "echo second"),
        ("bash", "echo third"),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    let view = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .view();
    let cancellation = acyclic_fs::CancellationToken::new();
    let before = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(super::super::storage)?
        .value
        .id();
    run(&harness).await?;
    let after = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(super::super::storage)?
        .value
        .id();
    assert_eq!(
        after, before,
        "read-only shell commands authored the lazy source tree"
    );
    assert_eq!(provider.requests().len(), 4);
    assert!(
        !directory.path().join(".git").exists(),
        "watch fencing changed the checkout"
    );
    #[cfg(unix)]
    assert_eq!(
        std::fs::metadata(&unread)
            .map_err(super::super::storage)?
            .permissions()
            .mode()
            & 0o777,
        0
    );
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    // Missing catalogs select the root; persisted valid catalogs select exactly
    // their current branch. Malformed catalogs never supply a capture target.
    #[test]
    fn selected_workspace_is_derived_only_from_a_valid_catalog(
        root in proptest::prelude::any::<[u8; 16]>(),
        child in proptest::prelude::any::<[u8; 16]>(),
        branch in "[a-z]{1,16}",
        persisted in proptest::prelude::any::<bool>(),
        valid in proptest::prelude::any::<bool>(),
        select_child in proptest::prelude::any::<bool>(),
        unpublished in proptest::prelude::any::<bool>(),
    ) {
        use acyclic_fs::{GitBranch, GitCompatRepository, GitCompatState, GitCompatStore, MemoryGitCompatStore, WorkspaceId};
        let root = WorkspaceId::from_bytes(root);
        let child = WorkspaceId::from_bytes(child);
        let marker = (acyclic_fs::OperationId::from_bytes([7;16]), acyclic_fs::GitGenerationRef {
            workspace_id: if select_child { child } else { root },
            generation: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes([9;32])),
        });
        let store = MemoryGitCompatStore::default();
        let result = futures::executor::block_on(async {
            if persisted {
                let branch = format!("agents/{branch}");
                let mut state = GitCompatState::new("main", root);
                state.revision = 1;
                state.checkout_reconciliation = unpublished.then_some(marker);
                state.branches.insert(branch.clone(), GitBranch {
                    name: branch.clone(), workspace_id: child, head: None, tracked_paths: Default::default(),
                });
                state.current_branch = if !valid { "missing".into() } else if select_child { branch } else { "main".into() };
                assert!(store.compare_and_swap(root, 0, state).await.unwrap());
            }
            GitCompatRepository::new(root, store).checkout_selection().await
        });
        proptest::prop_assert_eq!(result.is_ok(), !persisted || valid);
        if let Ok((selected, pending)) = result {
            proptest::prop_assert_eq!(pending, (persisted && unpublished).then_some(marker));
            proptest::prop_assert_eq!(selected, if persisted && select_child { child } else { root });
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn arriving_directories_remain_watched_after_creation_and_rename() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(
            0,
            "bash",
            "mkdir -p sub/deep; printf first > sub/deep/note.txt",
        ),
        super::checked_commands::call(1, "bash", "printf created-edit > sub/deep/note.txt"),
        ScriptedTurn::Text("done".into()),
        super::checked_commands::call(2, "bash", "mv sub moved"),
        super::checked_commands::call(3, "bash", "printf renamed-edit > moved/deep/note.txt"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider))?
            .state_directory(state.path()),
    )
    .await?;
    let workspace = harness.workspace().unwrap().view().workspace();
    let cancellation = acyclic_fs::CancellationToken::new();
    for (path, expected) in [
        ("/sub/deep/note.txt", b"created-edit".as_slice()),
        ("/moved/deep/note.txt", b"renamed-edit".as_slice()),
    ] {
        run(&harness).await?;
        let head = workspace
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(super::super::storage)?
            .value;
        let bytes = head
            .read(path, 1024, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(super::super::storage)?
            .value;
        assert_eq!(bytes.as_ref(), expected);
    }
    Ok(())
}

#[tokio::test]
async fn git_recovers_expired_and_interrupted_operation_windows_after_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "fs", "status"),
        ScriptedTurn::Text("done".into()),
        super::checked_commands::call(1, "fs", "status"),
        ScriptedTurn::Text("done".into()),
    ]);
    let mut harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    for reconciling in [false, true] {
        let checkout = harness.workspace().unwrap();
        let workspace = checkout.view().workspace();
        let coordinator = acyclic_fs::OperationWindowCoordinator::new(
            checkout.filesystem.operation_window_store(),
        );
        let parent = workspace
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(super::super::storage)?
            .value
            .id();
        coordinator
            .begin(workspace.id(), parent, "abandoned-worker", 0, 1)
            .await
            .map_err(super::super::storage)?;
        if reconciling {
            assert!(
                coordinator
                    .claim_reconcile(workspace.id(), 2)
                    .await
                    .map_err(super::super::storage)?
                    .is_some()
            );
        }
        let workspace_id = workspace.id();
        drop(coordinator);
        drop(harness);
        harness = LocalHarness::open(
            super::checkout_config(directory.path(), (model()?, provider.clone()))?
                .state_directory(state.path()),
        )
        .await?;
        let coordinator = acyclic_fs::OperationWindowCoordinator::new(
            harness
                .workspace()
                .unwrap()
                .filesystem
                .operation_window_store(),
        );
        run(&harness).await?;
        assert!(matches!(
            coordinator
                .inspect(workspace_id)
                .await
                .map_err(super::super::storage)?
                .phase,
            acyclic_fs::OperationWindowPhase::Idle
        ));
    }
    Ok(())
}
