use super::*;
use acyclic_fs::LocalFs;
use proptest::prelude::*;
type LocalWorkspace = FsWorkspace<LocalAuthorityBackend, LocalObjectBackend>;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn fixture() -> TestResult<(
    tempfile::TempDir,
    LocalFs,
    LocalCoreStateStore,
    LocalWorkspace,
)> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let filesystem =
        LocalFs::local(acyclic_fs::LocalOptions::new(directory.path().join("fs"))).await?;
    let store = LocalCoreStateStore::new(directory.path().join("core"));
    let workspace = registered_workspace(&filesystem, &store, "project").await?;
    Ok((directory, filesystem, store, workspace))
}

async fn registered_workspace(
    filesystem: &LocalFs,
    store: &LocalCoreStateStore,
    name: &str,
) -> TestResult<LocalWorkspace> {
    let workspace = filesystem
        .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
            name,
            acyclic_fs::VolumeInitialization {
                config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
                access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
            },
            OperationId::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            None,
        )
        .await?
        .value
        .workspace();
    WorkspaceGraph::new(store.clone())
        .register_root(
            &workspace,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    Ok(workspace)
}

async fn execute(
    filesystem: &LocalFs,
    store: &LocalCoreStateStore,
    view: View<'_>,
    repository: acyclic_fs::WorkspaceId,
    args: &[&str],
) -> crate::Result<Output> {
    execute_request(filesystem, store, view, repository, args, None).await
}

async fn execute_request(
    filesystem: &LocalFs,
    store: &LocalCoreStateStore,
    view: View<'_>,
    repository: acyclic_fs::WorkspaceId,
    args: &[&str],
    request: Option<acyclic_fs::GitRequest>,
) -> crate::Result<Output> {
    run(
        Workspace {
            access: &crate::filesystem::local_owner_workspace_access("test")?,
            filesystem,
            store,
            view,
            root: Path::new("."),
            repository,
            lease: None,
            checkout: None,
            author: "test",
            authored_at_seconds: 1,
            request,
        },
        &args
            .iter()
            .map(|argument| (*argument).into())
            .collect::<Vec<_>>(),
    )
    .await
}

#[tokio::test]
async fn completed_source_cut_survives_pending_join_recovery() -> TestResult {
    let (_directory, filesystem, store, main) = fixture().await?;
    main.write_text("/base", "base\n").await?;
    execute(
        &filesystem,
        &store,
        (&main).into(),
        main.id(),
        &["commit", "-m", "base"],
    )
    .await?;
    let child = execute(
        &filesystem,
        &store,
        (&main).into(),
        main.id(),
        &["checkout", "-b", "child"],
    )
    .await?
    .switched
    .ok_or("missing child")?;
    child
        .workspace()
        .write_text("/completed", "completed\n")
        .await?;
    let cut = child
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    execute(
        &filesystem,
        &store,
        child.view(),
        main.id(),
        &["checkout", "main"],
    )
    .await?;
    let request = acyclic_fs::GitRequest {
        operation: OperationId::new(),
        unanswered: Arc::new(|_| true),
        merge_source: Some(acyclic_fs::GitGenerationRef {
            workspace_id: child.workspace().id(),
            generation: cut.id(),
        }),
    };
    let repository =
        GitCompatRepository::new(main.id(), store.clone()).for_request(request.clone());
    let target = main
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    repository
        .execute_argv(
            &["merge".into(), "child".into()],
            GitTreeRef::exact(main.id(), target.id()),
            "test",
            1,
        )
        .await?;
    // A later turn changes the same workspace after the request's immutable
    // action was persisted. Recover through the real executor, without the
    // original caller's in-memory source selection.
    child
        .workspace()
        .write_text("/completed", "unfinished replacement\n")
        .await?;
    child
        .workspace()
        .write_text("/unfinished", "unfinished\n")
        .await?;
    execute_request(
        &filesystem,
        &store,
        (&main).into(),
        main.id(),
        &["merge", "child"],
        Some(acyclic_fs::GitRequest {
            merge_source: None,
            ..request.clone()
        }),
    )
    .await?;
    let merged = main
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    let completed = merged
        .read_range(
            "/completed",
            0,
            b"completed\n".len() as u64,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    assert_eq!(completed.bytes.as_ref(), b"completed\n");
    let unfinished = merged
        .stat(
            "/unfinished",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await;
    assert!(unfinished.is_err_and(|failure| failure.error.is_absent()));
    assert!(repository.request_result().await?.is_some());
    Ok(())
}

#[tokio::test]
async fn saved_command_replay_authenticates_its_original_repository_sources() -> TestResult {
    use acyclic_fs::GitCompatStore;
    let (_directory, filesystem, store, main) = fixture().await?;
    let request = acyclic_fs::GitRequest {
        merge_source: None,
        operation: OperationId::new(),
        unanswered: Arc::new(|_| true),
    };
    let first = execute_request(
        &filesystem,
        &store,
        (&main).into(),
        main.id(),
        &["branch", "saved"],
        Some(request.clone()),
    )
    .await?
    .command
    .ok_or("no original result")?;
    let replay = execute_request(
        &filesystem,
        &store,
        (&main).into(),
        main.id(),
        &["apply", "missing.patch"],
        Some(request.clone()),
    )
    .await?
    .command
    .ok_or("no replay result")?;
    assert_eq!(replay, first);
    let mut invalid = GitCompatStore::load(&store, main.id())
        .await?
        .ok_or("missing repository")?;
    let branch = invalid.current_branch.clone();
    let expected = invalid.revision;
    invalid.current_branch = "missing-branch".into();
    invalid.revision += 1;
    assert!(GitCompatStore::compare_and_swap(&store, main.id(), expected, invalid).await?);
    let corrupt_replay = execute_request(
        &filesystem,
        &store,
        (&main).into(),
        main.id(),
        &["apply", "missing.patch"],
        Some(request.clone()),
    )
    .await;
    assert!(matches!(corrupt_replay, Err(Error::Storage(_))));
    let mut restored = GitCompatStore::load(&store, main.id())
        .await?
        .ok_or("missing repository")?;
    let expected = restored.revision;
    restored.current_branch = branch;
    restored.revision += 1;
    assert!(GitCompatStore::compare_and_swap(&store, main.id(), expected, restored).await?);
    let foreign = registered_workspace(&filesystem, &store, "foreign-replay-source").await?;
    let mut state = GitCompatStore::load(&store, main.id())
        .await?
        .ok_or("missing repository")?;
    let expected = state.revision;
    state
        .branches
        .get_mut("saved")
        .ok_or("missing saved branch")?
        .workspace_id = foreign.id();
    state.revision += 1;
    assert!(GitCompatStore::compare_and_swap(&store, main.id(), expected, state).await?);
    let denied = execute_request(
        &filesystem,
        &store,
        (&main).into(),
        main.id(),
        &["apply", "missing.patch"],
        Some(request),
    )
    .await;
    assert!(
        matches!(denied, Err(ref error) if error.code() == crate::wire::ErrorCode::Unauthorized),
        "{:?}",
        denied.as_ref().err()
    );
    Ok(())
}

#[tokio::test]
async fn two_agent_forks_repair_same_line_conflicts_before_merge_completes() -> TestResult {
    let (_directory, filesystem, store, second_agent) = fixture().await?;
    let repository = second_agent.id();
    second_agent.write_text("/note", "base\n").await?;
    execute(
        &filesystem,
        &store,
        (&second_agent).into(),
        repository,
        &["commit", "-m", "base"],
    )
    .await?;
    let first_agent = execute(
        &filesystem,
        &store,
        (&second_agent).into(),
        repository,
        &["checkout", "-b", "first-agent"],
    )
    .await?
    .switched
    .ok_or("first agent has no fork")?;
    first_agent
        .workspace()
        .write_text("/note", "first\n")
        .await?;
    execute(
        &filesystem,
        &store,
        first_agent.view(),
        repository,
        &["commit", "-m", "first agent edit"],
    )
    .await?;
    execute(
        &filesystem,
        &store,
        first_agent.view(),
        repository,
        &["checkout", "main"],
    )
    .await?;
    second_agent.write_text("/note", "second\n").await?;
    execute(
        &filesystem,
        &store,
        (&second_agent).into(),
        repository,
        &["commit", "-m", "second agent edit"],
    )
    .await?;
    let conflict = execute(
        &filesystem,
        &store,
        (&second_agent).into(),
        repository,
        &["merge", "first-agent"],
    )
    .await
    .err()
    .ok_or("content disagreement was auto-resolved")?;
    assert!(matches!(&conflict, Error::MergeConflict { paths } if paths == &["/note"]));
    let result = crate::tools::rejected_input(conflict);
    assert_eq!(result.value["kind"], "merge conflict");
    assert_eq!(result.value["paths"], serde_json::json!(["/note"]));
    let markers = View::Volume(&second_agent).read("/note", 1024).await?;
    let markers = std::str::from_utf8(markers.as_ref())?;
    for expected in [
        "<<<<<<< ours\n",
        "second\n",
        "=======\n",
        "first\n",
        ">>>>>>> theirs\n",
    ] {
        assert!(
            markers.contains(expected),
            "missing {expected:?}: {markers}"
        );
    }
    let compatibility = GitCompatRepository::new(repository, store.clone());
    let pending = compatibility.pending_transition().await?;
    assert!(
        pending.is_some(),
        "conflicted merge completed before repair"
    );
    assert!(matches!(
        execute(
            &filesystem,
            &store,
            (&second_agent).into(),
            repository,
            &["merge", "--continue"]
        )
        .await,
        Err(Error::MergeConflict { .. })
    ));
    assert_eq!(compatibility.pending_transition().await?, pending);
    second_agent
        .write_text("/note", "first and second repaired\n")
        .await?;
    execute(
        &filesystem,
        &store,
        (&second_agent).into(),
        repository,
        &["merge", "--continue"],
    )
    .await?;
    assert!(compatibility.pending_transition().await?.is_none());
    assert_eq!(
        View::Volume(&second_agent)
            .read("/note", 1024)
            .await?
            .as_ref(),
        b"first and second repaired\n"
    );
    execute(
        &filesystem,
        &store,
        (&second_agent).into(),
        repository,
        &["status"],
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn manual_binary_resolution_can_continue_without_committing_markers() -> TestResult {
    let (_directory, filesystem, store, main) = fixture().await?;
    let repository = main.id();
    main.write_text("/note", "base\n").await?;
    execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["commit", "-m", "base"],
    )
    .await?;
    let child = execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["checkout", "-b", "topic"],
    )
    .await?
    .switched
    .ok_or("missing child")?;
    child
        .workspace()
        .write("/note", bytes::Bytes::from_static(b"\xffchild"))
        .await?;
    child
        .workspace()
        .write_text("/clean", "source change\n")
        .await?;
    execute(
        &filesystem,
        &store,
        child.view(),
        repository,
        &["commit", "-m", "child"],
    )
    .await?;
    execute(
        &filesystem,
        &store,
        child.view(),
        repository,
        &["checkout", "main"],
    )
    .await?;
    main.write("/note", bytes::Bytes::from_static(b"\xfeparent"))
        .await?;
    execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["commit", "-m", "parent"],
    )
    .await?;
    let merge = super::super::run_with_guard(
        Workspace {
            access: &crate::filesystem::local_owner_workspace_access("test")?,
            filesystem: &filesystem,
            store: &store,
            view: (&main).into(),
            root: Path::new("."),
            repository,
            lease: None,
            checkout: None,
            author: "test",
            authored_at_seconds: 1,
            request: None,
        },
        &["merge".into(), "topic".into()],
        || async {
            let pending = GitCompatRepository::new(repository, store.clone())
                .pending_transition()
                .await
                .map_err(super::super::errors::compat_error::<acyclic_fs::LocalCoreStateStore>)?;
            if let Some(pending) = pending {
                let published = main
                    .operation_generation(
                        IdempotencyKey::from_bytes(pending.id.operation_id().into_bytes()),
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| Error::from(failure.error))?
                    .value
                    .is_some();
                if !published
                    && matches!(
                        pending.mutation,
                        acyclic_fs::GitPendingMutation::Join {
                            conflicts: Some(_),
                            ..
                        }
                    )
                {
                    return Err(Error::Unauthorized(
                        "interrupted before merge projection".into(),
                    ));
                }
            }
            Ok(())
        },
    )
    .await;
    assert!(matches!(merge, Err(Error::MergeConflict { .. })));
    assert_eq!(
        View::Volume(&main).read("/clean", 64).await?.as_ref(),
        b"source change\n"
    );
    let compatibility = GitCompatRepository::new(repository, store.clone());
    let before = compatibility
        .pending_transition()
        .await?
        .ok_or("missing pending merge")?;
    assert_eq!(
        match &before.mutation {
            acyclic_fs::GitPendingMutation::Join {
                conflicts: Some(paths),
                ..
            } => paths.keys().cloned().collect::<BTreeSet<_>>(),
            _ => panic!("conflict paths"),
        },
        BTreeSet::from(["/note".into()])
    );
    for command in ["status", "log"] {
        let inspected = execute(&filesystem, &store, (&main).into(), repository, &[command]).await;
        assert!(inspected?.command.is_some());
        assert_eq!(
            compatibility.pending_transition().await?,
            Some(before.clone())
        );
    }
    main.write_text("/note", ">>>>>>> unresolved\n").await?;
    let continuation = execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["merge", "--continue"],
    )
    .await;
    assert!(matches!(continuation, Err(Error::MergeConflict { ref paths }) if paths == &["/note"]));
    assert_eq!(
        compatibility.pending_transition().await?,
        Some(before.clone())
    );
    let stop = CancellationToken::new();
    let mut transaction = main
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await?
        .value;
    transaction
        .rename("/note", "/renamed", false, WorkBudget::UNBOUNDED, &stop)
        .await?;
    transaction
        .commit::<WorkspaceError, _, _>(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                })
            },
        )
        .await?;
    main.write("/renamed", bytes::Bytes::from_static(b"\xffresolved"))
        .await?;
    main.write_text("/untracked", "not part of the merge")
        .await?;
    let current = main.head(WorkBudget::UNBOUNDED, &stop).await?.value;
    let GitFilesystemAction::Join { tracked_paths, .. } = &before.action else {
        panic!("pending join");
    };
    // Negative control: filtering only the original names loses the rename.
    assert!(
        !existing_tracked_paths(&current, tracked_paths)
            .await?
            .contains("renamed")
    );
    execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["merge", "--continue"],
    )
    .await?;
    assert!(compatibility.pending_transition().await?.is_none());
    let tracked = compatibility.tracked_paths().await?;
    assert!(tracked.contains("renamed"));
    assert!(!tracked.contains("note"));
    assert!(!tracked.contains("untracked"));
    execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["clean", "-f"],
    )
    .await?;
    assert_eq!(
        View::Volume(&main).read("/renamed", 64).await?.as_ref(),
        b"\xffresolved"
    );
    let current = main.head(WorkBudget::UNBOUNDED, &stop).await?.value;
    assert!(matches!(
        current
            .stat("/untracked", WorkBudget::UNBOUNDED, &stop)
            .await,
        Err(acyclic_fs::OperationFailure {
            error: WorkspaceError::NotFound,
            ..
        })
    ));

    Ok(())
}

#[tokio::test]
async fn delete_modify_conflicts_accept_manual_content_and_deletion() -> TestResult {
    let (_directory, filesystem, store, main) = fixture().await?;
    let cases = [
        (main, false, false),
        (
            registered_workspace(&filesystem, &store, "ours-deleted").await?,
            true,
            false,
        ),
        (
            registered_workspace(&filesystem, &store, "delete-resolution").await?,
            false,
            true,
        ),
    ];
    let filesystem = &filesystem;
    let store = &store;
    futures::future::try_join_all(cases.into_iter().map(
        |(main, ours_deleted, delete_resolution)| async move {
            let repository = main.id();
            main.write_text("/note", "base\n").await?;
            execute(
                filesystem,
                store,
                (&main).into(),
                repository,
                &["commit", "-m", "base"],
            )
            .await?;
            let child = execute(
                filesystem,
                store,
                (&main).into(),
                repository,
                &["checkout", "-b", "topic"],
            )
            .await?
            .switched
            .ok_or("missing child")?;
            if ours_deleted {
                child.workspace().write_text("/note", "child\n").await?;
            } else {
                child.workspace().remove("/note").await?;
            }
            child
                .workspace()
                .write_text("/clean", "nonconflicting source\n")
                .await?;
            execute(
                filesystem,
                store,
                child.view(),
                repository,
                &["commit", "-m", "child"],
            )
            .await?;
            execute(
                filesystem,
                store,
                child.view(),
                repository,
                &["checkout", "main"],
            )
            .await?;
            if ours_deleted {
                main.remove("/note").await?;
            } else {
                main.write_text("/note", "parent\n").await?;
            }
            execute(
                filesystem,
                store,
                (&main).into(),
                repository,
                &["commit", "-m", "parent"],
            )
            .await?;
            let merge = execute(
                filesystem,
                store,
                (&main).into(),
                repository,
                &["merge", "topic"],
            )
            .await;
            assert!(
                matches!(merge, Err(Error::MergeConflict { .. })),
                "merge: {:?}",
                merge.err()
            );
            assert_eq!(
                View::Volume(&main).read("/note", 64).await?.as_ref(),
                if ours_deleted {
                    b"child\n".as_slice()
                } else {
                    b"parent\n".as_slice()
                }
            );
            if delete_resolution {
                main.remove("/note").await?;
            } else {
                main.write_text("/note", "resolved\n").await?;
            }
            execute(
                filesystem,
                store,
                (&main).into(),
                repository,
                &["merge", "--continue"],
            )
            .await?;
            assert_eq!(
                View::Volume(&main).read("/clean", 64).await?.as_ref(),
                b"nonconflicting source\n"
            );
            if !delete_resolution {
                assert_eq!(
                    View::Volume(&main).read("/note", 64).await?.as_ref(),
                    b"resolved\n"
                );
            } else {
                assert!(
                    !GitCompatRepository::new(repository, store.clone())
                        .tracked_paths()
                        .await?
                        .contains("note")
                );
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        },
    ))
    .await?;
    Ok(())
}

#[tokio::test]
async fn continuation_checks_manual_paths_even_without_a_join_generation() -> TestResult {
    let (_directory, filesystem, store, main) = fixture().await?;
    let repository = main.id();
    main.write_text("/note", "base\n").await?;
    execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["commit", "-m", "base"],
    )
    .await?;
    let child = execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["checkout", "-b", "topic"],
    )
    .await?
    .switched
    .ok_or("missing child")?;
    execute(
        &filesystem,
        &store,
        child.view(),
        repository,
        &["checkout", "main"],
    )
    .await?;
    let compatibility = GitCompatRepository::new(repository, store.clone());
    let tree = View::Volume(&main)
        .tree(true, PublicationPermit::Unrestricted)
        .await?;
    let GitCommandOutput::Prepared { transition, .. } = compatibility
        .execute(
            GitCommand::Merge {
                branch: "topic".into(),
            },
            tree,
        )
        .await?
    else {
        return Err("pending merge".into());
    };
    assert!(
        execute(
            &filesystem,
            &store,
            (&main).into(),
            repository,
            &["merge", "--continue"]
        )
        .await
        .is_err()
    );
    assert!(compatibility.pending_transition().await?.is_some());
    compatibility
        .record_merge_conflicts(
            transition.operation_id(),
            &BTreeMap::from([("/note".into(), None)]),
        )
        .await?;
    assert!(
        main.operation_generation(
            IdempotencyKey::from_bytes(transition.operation_id().into_bytes()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await?
        .value
        .is_none()
    );
    main.write_text("/note", "resolved manually\n").await?;
    execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["merge", "--continue"],
    )
    .await?;
    assert!(compatibility.pending_transition().await?.is_none());
    assert_eq!(
        View::Volume(&main).read("/note", 64).await?.as_ref(),
        b"resolved manually\n"
    );
    Ok(())
}

#[tokio::test]
async fn volume_branches_use_the_same_facade_without_a_native_source() -> TestResult {
    let (_directory, filesystem, store, main) = fixture().await?;
    main.write_text("/note", "base\n").await?;
    let repository = main.id();
    execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["add", "note"],
    )
    .await?;
    execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["commit", "-m", "base"],
    )
    .await?;
    let child = execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["checkout", "-b", "topic"],
    )
    .await?
    .switched
    .ok_or("no selected branch")?;
    assert!(matches!(&child, OwnedView::Volume(_)));
    assert_eq!(
        GitCompatRepository::new(repository, store.clone())
            .checkout_selection()
            .await?
            .0,
        child.workspace().id()
    );
    child.workspace().write_text("/note", "child\n").await?;
    execute(
        &filesystem,
        &store,
        child.view(),
        repository,
        &["add", "note"],
    )
    .await?;
    execute(
        &filesystem,
        &store,
        child.view(),
        repository,
        &["commit", "-m", "child"],
    )
    .await?;
    assert_eq!(
        View::Volume(&main).read("/note", 32).await?.as_ref(),
        b"base\n"
    );
    let selected = execute(
        &filesystem,
        &store,
        child.view(),
        repository,
        &["checkout", "main"],
    )
    .await?
    .switched
    .ok_or("no selected main branch")?;
    assert_eq!(selected.workspace().id(), main.id());
    assert_eq!(
        GitCompatRepository::new(repository, store.clone())
            .checkout_selection()
            .await?
            .0,
        main.id()
    );
    execute(
        &filesystem,
        &store,
        selected.view(),
        repository,
        &["merge", "topic"],
    )
    .await?;
    assert_eq!(
        selected.view().read("/note", 32).await?.as_ref(),
        b"child\n"
    );
    Ok(())
}

#[test]
fn volume_view_pins_exact_generations_for_both_capture_modes() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (_directory, _filesystem, _store, workspace) = runtime.block_on(fixture())?;
    // Property: for arbitrary UTF-8 bytes, exact and lazy requests over an SDK
    // volume select the identical current generation and return identical bytes.
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32)).run(
        &".{0,64}",
        |text| {
            runtime.block_on(async {
                workspace.write_text("/note", &text).await.unwrap();
                let view = View::Volume(&workspace);
                let lazy = view
                    .tree(false, PublicationPermit::Unrestricted)
                    .await
                    .unwrap();
                let exact = view
                    .tree(true, PublicationPermit::Unrestricted)
                    .await
                    .unwrap();
                prop_assert_eq!(lazy, exact);
                prop_assert!(matches!(exact, GitTreeRef::Exact(_)));
                let bytes = view.read("/note", 256).await.unwrap();
                prop_assert_eq!(bytes.as_ref(), text.as_bytes());
                Ok(())
            })
        },
    )?;
    Ok(())
}

#[tokio::test]
async fn volume_merge_revalidates_the_execution_owner_without_a_checkout() -> TestResult {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (_directory, filesystem, store, main) = fixture().await?;
    let repository = main.id();
    main.write_text("/note", "base").await?;
    execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["commit", "-m", "base"],
    )
    .await?;
    let child = execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["checkout", "-b", "topic"],
    )
    .await?
    .switched
    .ok_or("missing child")?;
    child.workspace().write_text("/note", "child").await?;
    execute(
        &filesystem,
        &store,
        child.view(),
        repository,
        &["commit", "-m", "child"],
    )
    .await?;
    execute(
        &filesystem,
        &store,
        child.view(),
        repository,
        &["checkout", "main"],
    )
    .await?;
    let before = main
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value
        .id();
    let checks = AtomicUsize::new(0);
    let result = super::super::run_with_guard(
        Workspace {
            access: &crate::filesystem::local_owner_workspace_access("test")?,
            filesystem: &filesystem,
            store: &store,
            view: (&main).into(),
            root: Path::new("."),
            repository,
            lease: None,
            checkout: None,
            author: "test",
            authored_at_seconds: 1,
            request: None,
        },
        &["merge".into(), "topic".into()],
        || {
            std::future::ready(if checks.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(())
            } else {
                Err(Error::Unauthorized("execution owner lost".into()))
            })
        },
    )
    .await;
    assert!(matches!(result, Err(Error::Unauthorized(_))));
    assert!(checks.load(Ordering::Relaxed) >= 2);
    assert_eq!(
        main.head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?
            .value
            .id(),
        before
    );
    // Negative control: the same merge with no ownership validation publishes.
    execute(
        &filesystem,
        &store,
        (&main).into(),
        repository,
        &["merge", "topic"],
    )
    .await?;
    assert_ne!(
        main.head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?
            .value
            .id(),
        before
    );
    Ok(())
}
