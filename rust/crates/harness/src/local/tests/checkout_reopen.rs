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

#[cfg(unix)]
#[tokio::test]
async fn reopen_binds_changed_auxiliary_parent_permissions_as_a_new_publication() -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let config = || {
        super::checkout_config(directory.path(), (model()?, text()))
            .map(|config| config.state_directory(state.path()))
    };
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::WorkBudget::UNBOUNDED;
    let head = async |harness: &LocalHarness| {
        Ok::<_, Error>(
            harness
                .workspace()
                .ok_or_else(|| Error::NotFound("checkout".into()))?
                .view()
                .workspace()
                .head(work, &cancellation)
                .await
                .map_err(super::super::storage)?
                .value
                .id(),
        )
    };
    let harness = LocalHarness::open(config()?).await?;
    let before = head(&harness).await?;
    harness.close().await?;
    let parent = directory.path().join(".volumes");
    let prior = std::fs::metadata(&parent)
        .map_err(super::super::storage)?
        .permissions()
        .mode();
    let changed = if prior & 0o777 == 0o700 { 0o750 } else { 0o700 };
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(changed))
        .map_err(super::super::storage)?;
    let harness = LocalHarness::open(config()?).await?;
    let after = head(&harness).await?;
    assert_ne!(after, before, "changed parent permissions were not bound");
    assert_eq!(
        std::fs::metadata(&parent)
            .map_err(super::super::storage)?
            .permissions()
            .mode()
            & 0o777,
        changed
    );
    harness.close().await?;
    let harness = LocalHarness::open(config()?).await?;
    assert_eq!(
        head(&harness).await?,
        after,
        "unchanged reopen published again"
    );
    harness.close().await?;
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
    assert_eq!(first.request_count(), 5);
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
    assert_eq!(resumed.request_count(), 5);
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
    std::fs::create_dir(directory.path().join(".volumes")).map_err(super::super::storage)?;
    let sibling = directory.path().join(".volumes/unread.txt");
    for path in [&unread, &sibling] {
        std::fs::write(path, b"unrelated source bytes").map_err(super::super::storage)?;
    }
    #[cfg(unix)]
    for path in [&unread, &sibling] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0))
            .map_err(super::super::storage)?;
    }
    #[cfg(windows)]
    let _unread_guard = {
        use std::os::windows::fs::OpenOptionsExt as _;
        // Deny every subsequent open until the test ends. An eager importer
        // cannot read this file, while the held lazy checkout can run echo.
        [&unread, &sibling]
            .map(|path| {
                std::fs::OpenOptions::new()
                    .read(true)
                    .share_mode(0)
                    .open(path)
            })
            .into_iter()
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(super::super::storage)?
    };
    let provider = commands(&[
        (crate::tools::shell::PLATFORM.name, "echo first"),
        (crate::tools::shell::PLATFORM.name, "echo second"),
        (crate::tools::shell::PLATFORM.name, "echo third"),
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
    assert_eq!(provider.request_count(), 4);
    assert!(
        !directory.path().join(".git").exists(),
        "watch fencing changed the checkout"
    );
    #[cfg(unix)]
    for path in [&unread, &sibling] {
        assert_eq!(
            std::fs::metadata(path)
                .map_err(super::super::storage)?
                .permissions()
                .mode()
                & 0o777,
            0
        );
    }
    Ok(())
}

#[tokio::test]
async fn auxiliary_parent_binding_keeps_authored_sibling_changes_visible() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = commands(&[(
        crate::tools::shell::PLATFORM.name,
        if cfg!(windows) {
            "[IO.File]::WriteAllText('.volumes/user-owned', 'authored')"
        } else {
            "printf authored > .volumes/user-owned"
        },
    )]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider))?
            .state_directory(state.path()),
    )
    .await?;
    run(&harness).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let head = harness
        .workspace()
        .unwrap()
        .view()
        .workspace()
        .head(budget, &cancellation)
        .await
        .map_err(super::super::storage)?
        .value;
    assert_eq!(
        head.read("/.volumes/user-owned", 64, budget, &cancellation)
            .await
            .map_err(super::super::storage)?
            .value
            .as_ref(),
        b"authored"
    );
    assert!(matches!(
        head.stat("/.volumes/scratch", budget, &cancellation).await,
        Err(acyclic_fs::OperationFailure {
            error: acyclic_fs::WorkspaceError::NotFound,
            ..
        })
    ));
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

#[tokio::test]
async fn a_checkout_cloned_again_at_the_same_path_gets_new_state() -> Result<()> {
    let parent = tempfile::tempdir().map_err(super::super::storage)?;
    let users = tempfile::tempdir().map_err(super::super::storage)?;
    let workspace = parent.path().join("checkout");
    std::fs::create_dir(&workspace).map_err(super::super::storage)?;
    let config = |state: &std::path::Path| {
        super::checkout_config(&workspace, (model()?, text()))
            .map(|config| config.state_directory(state))
    };
    let state = || {
        super::super::owner::state_directory_in(
            users.path(),
            &config(users.path())?.project,
            Some(&super::super::owner::Checkout::capture(&workspace)?),
        )
    };
    let first = state()?;
    assert_eq!(state()?, first, "an unchanged checkout keeps its state");
    let harness = LocalHarness::open(config(&first)?).await?;
    assert_eq!(harness.run("first clone").await?.text, "done");
    harness.close().await?;
    // Delete and re-clone. A filesystem that frees the inode may hand the
    // same device and object id to the new directory; its file handle's
    // generation, or its birth time, differs.
    std::fs::remove_dir_all(&workspace).map_err(super::super::storage)?;
    std::fs::create_dir(&workspace).map_err(super::super::storage)?;
    // The previous clone's state refuses the new clone.
    let Err(Error::Unauthorized(message)) = LocalHarness::open(config(&first)?).await else {
        panic!("the previous clone's state accepted a new checkout");
    };
    assert!(message.contains("different workspace"), "{message}");
    let second = state()?;
    assert_ne!(second, first);
    let harness = LocalHarness::open(config(&second)?).await?;
    assert_eq!(harness.run("second clone").await?.text, "done");
    harness.close().await?;
    Ok(())
}

proptest::proptest! {
    /// A recycled object id is a new incarnation whenever its file handle or
    /// birth time differs, with or without the other; identities are
    /// injected, so this never depends on the filesystem reusing one.
    #[test]
    fn a_recycled_object_id_is_a_new_incarnation(
        object: [u8; 16],
        handles: ([u8; 32], [u8; 32]),
        births: (u64, u64),
        timed: bool,
    ) {
        use super::super::owner::incarnation_of;
        use std::time::Duration;
        let born = |nanos: u64| timed.then(|| Duration::from_nanos(nanos));
        let first = incarnation_of(object, Some(handles.0), born(births.0)).unwrap();
        proptest::prop_assert_eq!(
            first,
            incarnation_of(object, Some(handles.0), born(births.0)).unwrap(),
            "an unchanged checkout keeps its incarnation"
        );
        // Without a birth time, the handle generation alone separates a re-clone.
        if handles.0 != handles.1 {
            proptest::prop_assert_ne!(
                first,
                incarnation_of(object, Some(handles.1), born(births.0)).unwrap(),
                "a re-clone on a recycled inode kept its state"
            );
        }
        // Without a handle, the birth time alone separates a re-clone.
        let untimed = |nanos| incarnation_of(object, None, Some(Duration::from_nanos(nanos)));
        if births.0 != births.1 {
            proptest::prop_assert_ne!(untimed(births.0).unwrap(), untimed(births.1).unwrap());
        }
        // A handle never collides with a birth time, nor one object with another.
        proptest::prop_assert_ne!(first, untimed(births.0).unwrap());
        let mut other = object;
        other[0] ^= 1;
        proptest::prop_assert_ne!(
            first,
            incarnation_of(other, Some(handles.0), born(births.0)).unwrap()
        );
        // A host recording neither cannot tell a re-clone apart, so it is refused.
        proptest::prop_assert!(matches!(
            incarnation_of(object, None, None),
            Err(Error::Unsupported(_))
        ));
    }
}

/// Opening a checkout reads its root once for state selection and owner
/// validation together; the negative control repeats the read per consumer,
/// as before, and the same guard counts it.
#[tokio::test]
async fn opening_a_checkout_captures_its_root_once() -> Result<()> {
    use super::super::owner::{CAPTURES, Checkout, Owner, state_directory_in};
    let workspace = tempfile::tempdir().map_err(super::super::storage)?;
    let users = tempfile::tempdir().map_err(super::super::storage)?;
    let config = super::checkout_config(workspace.path(), (model()?, text()))?;
    let captures = |run: &mut dyn FnMut() -> Result<()>| {
        CAPTURES.with(|captures| captures.set(0));
        run()?;
        Ok::<_, Error>(CAPTURES.with(std::cell::Cell::get))
    };
    let state = super::super::owner::directory(&users.path().join("state"))?;
    let shared = captures(&mut || {
        let checkout = Checkout::capture(workspace.path())?;
        state_directory_in(users.path(), &config.project, Some(&checkout))?;
        Owner::open(&state, &config.project, Some(&checkout)).map(drop)
    })?;
    assert_eq!(shared, 1);
    let repeated = captures(&mut || {
        let selected = Checkout::capture(workspace.path())?;
        state_directory_in(users.path(), &config.project, Some(&selected))?;
        let validated = Checkout::capture(workspace.path())?;
        Owner::open(&state, &config.project, Some(&validated)).map(drop)
    })?;
    assert_eq!(repeated, 2, "the guard missed a duplicate root read");
    // The whole open path holds to the same single capture.
    CAPTURES.with(|captures| captures.set(0));
    let harness = LocalHarness::open(config.state_directory(users.path().join("opened"))).await?;
    assert_eq!(CAPTURES.with(std::cell::Cell::get), 1);
    harness.close().await?;
    Ok(())
}

/// A kill after any durable step of the first open's owner publication leaves
/// a checkout that reopens and accepts a prompt. The negative control publishes
/// the record in place, and a kill mid-write bricks the checkout.
#[tokio::test]
async fn a_kill_at_any_step_of_owner_publication_reopens_and_accepts_a_prompt() -> Result<()> {
    use super::super::owner::publication;
    async fn killed_then_reopened(step: usize, torn: bool) -> Result<Result<()>> {
        let workspace = tempfile::tempdir().map_err(super::super::storage)?;
        let state = tempfile::tempdir().map_err(super::super::storage)?;
        let config = || {
            super::checkout_config(workspace.path(), (model()?, text()))
                .map(|config| config.state_directory(state.path()))
        };
        publication::tear(torn);
        publication::kill_at(Some(step));
        let killed = LocalHarness::open(config()?).await;
        publication::kill_at(None);
        publication::tear(false);
        assert!(killed.is_err(), "step {step} was not killed");
        Ok(match LocalHarness::open(config()?).await {
            Ok(harness) => {
                run(&harness).await?;
                harness.close().await
            }
            Err(error) => Err(error),
        })
    }
    for step in 0..publication::STEPS {
        killed_then_reopened(step, false).await??;
    }
    assert!(
        killed_then_reopened(1, true).await?.is_err(),
        "a torn owner record did not brick the checkout"
    );
    Ok(())
}
