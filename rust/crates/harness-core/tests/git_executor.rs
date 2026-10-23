//! Managed git facade behavior through the harness public API.

use acyclic_fs::facade::FsWork as _;
use acyclic_fs::{
    CancellationToken, GitCommandOutput, IdempotencyKey, LazyWorkspace, LocalCoreStateStore,
    LocalFs, LocalOptions, OperationId, PublicationPermit, WorkBudget,
    demand::native::NativeDemandSource,
    model::{Lifecycle, VolumeConfig},
};
use acyclic_harness_core::{Error, tools::git};
use std::{borrow::Cow, sync::Arc};

#[path = "../src/tools/git/path.rs"]
mod workspace_paths;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

type Lazy = LazyWorkspace<
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;

#[tokio::test]
async fn branch_switch_accepts_only_the_destination_workspace() -> Result<()> {
    use acyclic_fs::{
        Digest, GenerationId, GitCommand, GitCompatError, GitCompatRepository, GitFilesystemResult,
        GitTreeRef, MemoryGitCompatStore, WorkspaceId,
    };
    let main = WorkspaceId::from_bytes([1; 16]);
    let child = WorkspaceId::from_bytes([2; 16]);
    let foreign = WorkspaceId::from_bytes([3; 16]);
    let generation = GenerationId::new(Digest::from_bytes([4; 32]));
    let repository = GitCompatRepository::new(main, MemoryGitCompatStore::default());
    repository
        .register_branch_workspace("agents/child", child, None, false)
        .await?;
    let GitCommandOutput::Prepared { transition, .. } = repository
        .execute(
            GitCommand::Switch {
                branch: "agents/child".into(),
                create: false,
            },
            GitTreeRef::exact(main, generation),
        )
        .await?
    else {
        return Err("expected a prepared branch switch".into());
    };
    for wrong in [main, foreign] {
        assert!(matches!(
            repository
                .complete_transition_result(
                    transition,
                    &GitFilesystemResult::Applied {
                        tree: Some(GitTreeRef::exact(wrong, generation)),
                        tracked_paths: None
                    }
                )
                .await,
            Err(GitCompatError::WorkspaceMismatch)
        ));
        assert_eq!(
            repository
                .pending_transition()
                .await?
                .map(|pending| pending.id),
            Some(transition)
        );
    }
    repository
        .complete_transition_result(
            transition,
            &GitFilesystemResult::Applied {
                tree: Some(GitTreeRef::exact(child, generation)),
                tracked_paths: None,
            },
        )
        .await?;
    let GitCommandOutput::Status(status) = repository
        .execute(GitCommand::Status, GitTreeRef::exact(child, generation))
        .await?
    else {
        return Err("expected branch status".into());
    };
    assert_eq!(status.branch, "agents/child");
    Ok(())
}

async fn run(
    fs: &LocalFs,
    store: &LocalCoreStateStore,
    lazy: &Lazy,
    repository_id: acyclic_fs::WorkspaceId,
    args: &[&str],
) -> Result<(GitCommandOutput, Option<Lazy>)> {
    run_reserving(fs, store, lazy, repository_id, &[], args).await
}

async fn run_reserving(
    fs: &LocalFs,
    store: &LocalCoreStateStore,
    lazy: &Lazy,
    repository_id: acyclic_fs::WorkspaceId,
    reserved: &[String],
    args: &[&str],
) -> Result<(GitCommandOutput, Option<Lazy>)> {
    let argv: Vec<_> = args.iter().map(|arg| (*arg).to_owned()).collect();
    let output = git::run(
        git::Workspace {
            filesystem: fs,
            store,
            view: lazy.into(),
            root: std::path::Path::new("."),
            repository: repository_id,
            lease: None,
            checkout: None,
            reserved,
            author: "test",
            authored_at_seconds: 1,
        },
        &argv,
    )
    .await?;
    Ok((
        output.command.ok_or("pending git transition")?,
        output
            .switched
            .map(git::OwnedView::into_checkout)
            .transpose()?,
    ))
}

#[tokio::test]
async fn facade_commits_branches_merges_and_inspects_managed_files() -> Result<()> {
    let (_temp, _root, fs, store, main) = fixture().await?;
    let stop = CancellationToken::new();
    let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
    main.write(
        "/file.txt",
        bytes::Bytes::from_static(b"base\n").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut work,
    )
    .await?;
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(main.workspace())
        .await?;
    let repo = main.workspace().id();
    assert!(matches!(
        run(&fs, &store, &main, repo, &["commit", "-m", "base"])
            .await?
            .0,
        GitCommandOutput::Committed(_)
    ));
    let (_, child) = run(
        &fs,
        &store,
        &main,
        repo,
        &["checkout", "-b", "agents/child"],
    )
    .await?;
    let child = child.unwrap();
    child
        .write(
            "/file.txt",
            bytes::Bytes::from_static(b"child\n").into(),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut work,
        )
        .await
        .unwrap();
    run(&fs, &store, &child, repo, &["commit", "-m", "child edit"]).await?;
    child
        .write(
            "/published.txt",
            bytes::Bytes::from_static(b"without a git commit\n").into(),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut work,
        )
        .await?;
    run(&fs, &store, &child, repo, &["checkout", "main"]).await?;
    let difference = run(&fs, &store, &main, repo, &["diff", "agents/child"])
        .await?
        .0;
    let acyclic_fs::GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data {
        kind,
        value,
    }) = difference
    else {
        return Err("expected a published agent diff".into());
    };
    assert_eq!(kind, "diff");
    assert_eq!(value["fileChanges"], 2);
    assert_eq!(
        main.read("/file.txt", 64, &mut work).await?.bytes.as_ref(),
        b"base\n"
    );
    assert!(matches!(
        git::run(
            git::Workspace {
                filesystem: &fs,
                store: &store,
                view: (&main).into(),
                root: std::path::Path::new("."),
                repository: repo,
                lease: None,
                checkout: None,
                reserved: &[],
                author: "test",
                authored_at_seconds: 1,
            },
            &["diff".into(), "agents/absent".into()],
        )
        .await,
        Err(acyclic_harness_core::Error::NotFound(_))
    ));
    run(&fs, &store, &main, repo, &["merge", "agents/child"]).await?;
    assert_eq!(
        main.read("/published.txt", 64, &mut work)
            .await?
            .bytes
            .as_ref(),
        b"without a git commit\n",
    );
    assert_eq!(
        main.read("/file.txt", 64, &mut work)
            .await
            .unwrap()
            .bytes
            .as_ref(),
        b"child\n"
    );
    for (args, invalid) in [
        (&["apply", "../escape.patch"][..], true),
        (&["apply", "-"][..], true),
        (&["fetch"][..], false),
    ] {
        let error = match run(&fs, &store, &main, repo, args).await {
            Err(error) => error,
            Ok(_) => return Err("invalid command unexpectedly succeeded".into()),
        };
        let error = error
            .downcast_ref::<acyclic_harness_core::Error>()
            .ok_or("lost harness error type")?;
        if invalid {
            assert!(matches!(error, acyclic_harness_core::Error::Invalid(_)));
        } else {
            assert!(matches!(error, acyclic_harness_core::Error::Unsupported(_)));
        }
    }
    for args in [
        &["diff"][..],
        &["grep", "child"],
        &["archive"],
        &["blame", "file.txt"],
    ] {
        assert!(matches!(
            run(&fs, &store, &main, repo, args).await?.0,
            GitCommandOutput::Filesystem(_)
        ));
    }
    let missing = run(&fs, &store, &main, repo, &["apply", "missing.patch"]).await;
    let error = missing
        .err()
        .ok_or("missing patch unexpectedly succeeded")?;
    assert!(matches!(
        error.downcast_ref::<acyclic_harness_core::Error>(),
        Some(acyclic_harness_core::Error::NativeSource { source, .. })
            if matches!(source.as_ref(), acyclic_fs::LazyWorkspaceError::NotFound)
    ));
    main.write(
        "/.volumes/user.txt",
        bytes::Bytes::from_static(b"user-owned\n").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut work,
    )
    .await?;
    let GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data { value, .. }) = run(
        &fs,
        &store,
        &main,
        repo,
        &["check-ignore", ".volumes/user.txt"],
    )
    .await?
    .0
    else {
        return Err("expected check-ignore paths".into());
    };
    assert_eq!(value["paths"], serde_json::json!([]));
    run(&fs, &store, &main, repo, &["clean", "-f"]).await?;
    assert!(matches!(
        main.read("/.volumes/user.txt", 64, &mut work).await,
        Err(acyclic_fs::LazyWorkspaceError::NotFound)
    ));
    child
        .write(
            "/.gitattributes",
            bytes::Bytes::from_static(b"*.lock merge=ours\n").into(),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut work,
        )
        .await?;
    child
        .write(
            "/.gitignore",
            bytes::Bytes::from_static(b"ignored/\n").into(),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut work,
        )
        .await?;
    for path in ["/ignored/child.txt", "/.acyclic-sdk/state", "/result.txt"] {
        child
            .write(
                path,
                bytes::Bytes::from_static(b"child\n").into(),
                IdempotencyKey::new(),
                PublicationPermit::Unrestricted,
                &mut work,
            )
            .await?;
    }
    let plan = acyclic_harness_core::filesystem::plan_agent_merge(
        &fs,
        &store,
        &child,
        main.workspace(),
        repo,
    )
    .await?;
    assert!(
        plan.child_wins_bindings
            .contains(&"/ignored/child.txt".to_owned())
    );
    assert!(
        plan.child_wins_bindings
            .contains(&"/.acyclic-sdk/state".to_owned())
    );
    assert!(!plan.child_wins_bindings.contains(&"/result.txt".to_owned()));
    Ok(())
}

async fn fixture() -> Result<(
    tempfile::TempDir,
    std::path::PathBuf,
    LocalFs,
    LocalCoreStateStore,
    Lazy,
)> {
    fixture_native(false).await
}

async fn fixture_native(
    native_file: bool,
) -> Result<(
    tempfile::TempDir,
    std::path::PathBuf,
    LocalFs,
    LocalCoreStateStore,
    Lazy,
)> {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    if native_file {
        std::fs::write(root.join("native.txt"), b"native origin\n")?;
    }
    let fs = LocalFs::local(LocalOptions::new(temp.path().join("fs")))
        .await
        .unwrap();
    let store = LocalCoreStateStore::open_owned(temp.path().join("core")).unwrap();
    let config = VolumeConfig::native(Lifecycle::Durable);
    let stop = CancellationToken::new();
    let source = NativeDemandSource::open(
        Cow::Borrowed(&root),
        config.profile,
        config.limits,
        None,
        None,
        WorkBudget::UNBOUNDED,
        &stop,
    )
    .await
    .unwrap()
    .value;
    let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
    let (main, _) = LazyWorkspace::attach(
        &fs,
        "main",
        Arc::new(source),
        store.clone(),
        config,
        OperationId::new(),
        &mut work,
    )
    .await
    .unwrap();
    Ok((temp, root, fs, store, main))
}

/// A child projection authors its volume alias beside its own work. Merging
/// the child must publish and track the work, never the alias, and record
/// each parent once.
async fn merge_child_with_alias(reserved: &[String]) -> Result<AliasMerge> {
    let (_temp, _root, fs, store, main) = fixture().await?;
    let stop = CancellationToken::new();
    let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
    main.write(
        "/base.txt",
        bytes::Bytes::from_static(b"base\n").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut work,
    )
    .await?;
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(main.workspace())
        .await?;
    let repo = main.workspace().id();
    let GitCommandOutput::Committed(base) = run_reserving(
        &fs,
        &store,
        &main,
        repo,
        reserved,
        &["commit", "-m", "base"],
    )
    .await?
    .0
    else {
        return Err("expected the base commit".into());
    };
    let (_, child) = run_reserving(
        &fs,
        &store,
        &main,
        repo,
        reserved,
        &["checkout", "-b", "agents/kid"],
    )
    .await?;
    let child = child.ok_or("missing child view")?;
    for path in ["/newkid.txt", "/.volumes/user.txt"] {
        child
            .write(
                path,
                bytes::Bytes::from_static(b"kid\n").into(),
                IdempotencyKey::new(),
                PublicationPermit::Unrestricted,
                &mut work,
            )
            .await?;
    }
    author_volume_alias(&child).await?;
    run_reserving(&fs, &store, &child, repo, reserved, &["checkout", "main"]).await?;
    let GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data { value, .. }) =
        run_reserving(&fs, &store, &main, repo, reserved, &["diff", "agents/kid"])
            .await?
            .0
    else {
        return Err("expected an agent diff".into());
    };
    run_reserving(&fs, &store, &main, repo, reserved, &["merge", "agents/kid"]).await?;
    let GitCommandOutput::Commits(log) =
        run_reserving(&fs, &store, &main, repo, reserved, &["log"])
            .await?
            .0
    else {
        return Err("expected the merged log".into());
    };
    let merge = log.first().ok_or("empty log")?.clone();
    let alias_merged = main
        .workspace()
        .head(WorkBudget::UNBOUNDED, &stop)
        .await?
        .value
        .stat("/.volumes/scratch", WorkBudget::UNBOUNDED, &stop)
        .await
        .is_ok();
    Ok(AliasMerge {
        base: base.id,
        file_changes: value
            .get("fileChanges")
            .and_then(serde_json::Value::as_u64)
            .ok_or("missing file count")?,
        merge,
        alias_merged,
    })
}

/// Authors a volume alias in `child` the way a mounted child projection
/// does: a symbolic link to the host volume, published by the child itself.
async fn author_volume_alias(child: &Lazy) -> Result<()> {
    let stop = CancellationToken::new();
    let mut alias = child
        .workspace()
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await?
        .value;
    alias
        .create_symbolic_link(
            "/.volumes/scratch",
            acyclic_stream::OwnedBytes::from_static(b"/host/volumes/scratch"),
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await?;
    let committed = alias
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| {
                std::future::ready(Ok::<
                    _,
                    acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>,
                >(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    assert!(matches!(
        committed,
        acyclic_fs::TransactionCommit::Committed(_)
    ));
    Ok(())
}

struct AliasMerge {
    base: acyclic_fs::GitCommitId,
    file_changes: u64,
    merge: acyclic_fs::GitCommit,
    alias_merged: bool,
}

#[tokio::test]
async fn agent_merge_reserves_host_aliases_and_tracks_authored_files() -> Result<()> {
    let merged = merge_child_with_alias(&[".volumes/scratch".to_owned()]).await?;
    assert_eq!(merged.file_changes, 2);
    assert!(!merged.alias_merged);
    assert_eq!(
        merged.merge.tracked_paths,
        std::collections::BTreeSet::from([
            ".volumes/user.txt".to_owned(),
            "base.txt".to_owned(),
            "newkid.txt".to_owned(),
        ])
    );
    // The child branch was forked at HEAD, so HEAD is its only parent.
    assert_eq!(merged.merge.parents, vec![merged.base]);

    // Negative control: an unreserved view diffs, merges, and tracks the
    // alias as if the child had authored it.
    let leaked = merge_child_with_alias(&[]).await?;
    assert_eq!(leaked.file_changes, 3);
    assert!(leaked.alias_merged);
    assert!(leaked.merge.tracked_paths.contains(".volumes/scratch"));
    Ok(())
}

#[tokio::test]
async fn agent_diff_exactifies_native_files_with_the_target_lease() -> Result<()> {
    let (_temp, root, fs, store, main) = fixture_native(true).await?;
    let repository = main.workspace().id();
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(main.workspace())
        .await?;
    let (_, child) = run(
        &fs,
        &store,
        &main,
        repository,
        &["checkout", "-b", "agents/child"],
    )
    .await?;
    let child = child.ok_or("missing child")?;
    run(&fs, &store, &child, repository, &["checkout", "main"]).await?;
    let coordinator = acyclic_fs::OperationWindowCoordinator::new(fs.operation_window_store());
    let now =
        acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock);
    let head = main
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    let lease = coordinator
        .begin(repository, head.id(), "agent-diff", now, now + 60_000)
        .await?;
    // Negative control: the caller's lease cannot publish this target's
    // still-native file, even though both belong to the same repository.
    assert!(
        child
            .exactify(
                "/",
                lease.publication_permit(),
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new())
            )
            .await
            .is_err()
    );
    let output = git::run(
        git::Workspace {
            filesystem: &fs,
            store: &store,
            view: (&main).into(),
            root: &root,
            repository,
            lease: Some(lease.clone()),
            checkout: None,
            reserved: &[],
            author: "test",
            authored_at_seconds: 1,
        },
        &["diff".into(), "agents/child".into()],
    )
    .await?;
    assert!(output.command.is_some());
    assert_eq!(
        child
            .read(
                "/native.txt",
                64,
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new())
            )
            .await?
            .bytes
            .as_ref(),
        b"native origin\n"
    );
    coordinator
        .finish_workspace(
            main.workspace(),
            &lease,
            now + 1,
            acyclic_fs::OperationReconcileLimits::default(),
        )
        .await?;
    assert!(matches!(
        coordinator.snapshot(child.workspace().id()).await?.phase,
        acyclic_fs::OperationWindowPhase::Idle
    ));
    Ok(())
}

#[tokio::test]
async fn checkout_and_mounted_commands_both_reject_expired_leases() -> Result<()> {
    let (_temp, root, fs, store, main) = fixture().await?;
    let repo = main.workspace().id();
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(main.workspace())
        .await?;
    main.write(
        "/file.txt",
        bytes::Bytes::from_static(b"base\n").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
    )
    .await?;
    run(&fs, &store, &main, repo, &["commit", "-m", "base"]).await?;
    let head = main
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value
        .id();
    // Historical times make expiry deterministic, without sleeps or a clock override.
    let lease = acyclic_fs::OperationWindowCoordinator::new(fs.operation_window_store())
        .begin(repo, head, "expired-tool", 1, 2)
        .await?;
    for (checkout, tag) in [
        (None, "mounted-expired"),
        (
            Some(acyclic_fs::NativeRootIdentity::from_bytes([1; 16])),
            "checkout-expired",
        ),
    ] {
        let error = git::run(
            git::Workspace {
                filesystem: &fs,
                store: &store,
                view: (&main).into(),
                root: &root,
                repository: repo,
                lease: Some(lease.clone()),
                checkout,
                reserved: &[],
                author: "test",
                authored_at_seconds: 1,
            },
            &["tag".into(), tag.into()],
        )
        .await
        .err()
        .ok_or("expired lease created a tag")?;
        assert!(
            matches!(error, acyclic_harness_core::Error::Conflict(ref reason) if reason.contains("lease expired")),
            "{error:?}"
        );
        let error = git::run(
            git::Workspace {
                filesystem: &fs,
                store: &store,
                view: (&main).into(),
                root: &root,
                repository: repo,
                lease: Some(lease.clone()),
                checkout,
                reserved: &[],
                author: "test",
                authored_at_seconds: 1,
            },
            &["diff".into(), "agents/absent".into()],
        )
        .await
        .err()
        .ok_or("expired lease inspected an agent")?;
        assert!(
            matches!(error, acyclic_harness_core::Error::Conflict(ref reason)
            if reason.contains("lease expired")),
            "{error:?}"
        );
    }
    let GitCommandOutput::Tags(tags) = run(&fs, &store, &main, repo, &["tag"]).await?.0 else {
        return Err("tag listing returned another result".into());
    };
    assert!(tags.is_empty());
    Ok(())
}

#[tokio::test]
async fn publication_merge_drivers_do_not_read_unused_ignore_rules() -> Result<()> {
    let (_temp, _root, fs, store, main) = fixture().await?;
    let stop = CancellationToken::new();
    let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
    main.write(
        "/.gitignore",
        bytes::Bytes::from(vec![b'x'; 1024 * 1024 + 1]).into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut work,
    )
    .await?;
    main.write(
        "/.gitattributes",
        bytes::Bytes::from_static(b"*.txt merge=text\n").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut work,
    )
    .await?;
    let error = run(&fs, &store, &main, main.workspace().id(), &["status"])
        .await
        .err()
        .ok_or("oversized ignore rules unexpectedly loaded")?;
    assert!(
        matches!(
            error.downcast_ref::<acyclic_harness_core::Error>(),
            Some(acyclic_harness_core::Error::NativeSource { source, .. })
                if matches!(source.as_ref(), acyclic_fs::LazyWorkspaceError::TooLarge)
        ),
        "{error:?}"
    );
    acyclic_harness_core::filesystem::workspace_publication_coordinator(
        fs,
        store,
        Default::default(),
        [(acyclic_fs::WorkspaceRootId::from_bytes([3; 16]), &main)],
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn publication_identity_change_after_guard_preserves_conflict_code() -> Result<()> {
    use acyclic_fs::{
        MaterializeOptions, NativeMaterializationWork, NativeTreeMaterializationError,
    };
    let (_temp, root, _fs, store, main) = fixture().await?;
    let stop = CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &stop)
        .map_err(|(cause, _)| cause)?;
    let generation = main.workspace().head(&work, &stop).await?;
    let identity = work.root_identity(&root)?;
    let publication = acyclic_harness_core::filesystem::CheckoutPublication::new(
        &root,
        identity,
        OperationId::new(),
        &work,
    )?;
    let options = MaterializeOptions {
        destination: publication.directory().join("target"),
        expected_root_identity: None,
        maximum_directory_entries: 32,
        maximum_extent_spans: 32,
        transfer_bytes: 4096,
    };
    let mut guards = 0;
    let error = publication
        .publish(&generation, &generation, &store, &options, &work, || {
            guards += 1;
            let changed = std::fs::rename(&root, root.with_extension("original"))
                .and_then(|()| std::fs::create_dir(&root));
            std::future::ready(changed.map_err(acyclic_fs::NativeWorkspacePublicationError::from))
        })
        .await
        .expect_err("the checkout identity changed after authorization");
    assert_eq!(guards, 1);
    assert_eq!(
        error.code(),
        acyclic_harness_core::wire::ErrorCode::Conflict
    );
    assert!(matches!(
        error,
        acyclic_harness_core::Error::NativeWork {
            cause: NativeTreeMaterializationError::AllocationChanged { expected, actual },
            ..
        } if expected == identity && actual != identity
    ));
    Ok(())
}

#[tokio::test]
async fn user_owned_volumes_directory_is_committed_and_cleaned() -> Result<()> {
    use acyclic_fs::GitFilesystemResult;

    let (_temp, _root, fs, store, main) = fixture().await?;
    let repo = main.workspace().id();
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(main.workspace())
        .await?;
    let stop = CancellationToken::new();
    let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
    main.write(
        "/.volumes/project.txt",
        bytes::Bytes::from_static(b"user-owned directory\n").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut work,
    )
    .await?;
    let GitCommandOutput::Filesystem(GitFilesystemResult::Data { kind, value }) = run(
        &fs,
        &store,
        &main,
        repo,
        &["check-ignore", ".volumes/project.txt"],
    )
    .await?
    .0
    else {
        return Err("expected check-ignore data".into());
    };
    assert_eq!(kind, "check-ignore");
    assert_eq!(value["paths"], serde_json::json!([]));
    run(&fs, &store, &main, repo, &["commit", "-m", "user files"]).await?;
    let GitCommandOutput::Paths(paths) = run(&fs, &store, &main, repo, &["ls-files"]).await?.0
    else {
        return Err("expected tracked paths".into());
    };
    assert!(paths.iter().any(|path| path == ".volumes/project.txt"));

    main.write(
        "/.volumes/untracked.txt",
        bytes::Bytes::from_static(b"untracked\n").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut work,
    )
    .await?;
    let GitCommandOutput::Filesystem(GitFilesystemResult::Data { kind, value }) =
        run(&fs, &store, &main, repo, &["clean", "-n"]).await?.0
    else {
        return Err("expected clean preview".into());
    };
    assert_eq!(kind, "clean");
    assert_eq!(
        value["paths"],
        serde_json::json!([".volumes/untracked.txt"])
    );
    run(&fs, &store, &main, repo, &["clean", "-f"]).await?;
    assert!(matches!(
        main.read("/.volumes/untracked.txt", 64, &mut work).await,
        Err(acyclic_fs::LazyWorkspaceError::NotFound)
    ));
    assert_eq!(
        main.read("/.volumes/project.txt", 64, &mut work)
            .await?
            .bytes
            .as_ref(),
        b"user-owned directory\n"
    );
    Ok(())
}

async fn write_text(view: &Lazy, path: &str, text: &'static [u8]) -> Result<()> {
    let stop = CancellationToken::new();
    let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
    view.write(
        path,
        bytes::Bytes::from_static(text).into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut work,
    )
    .await?;
    Ok(())
}

/// `merge --continue` tracks what the conflicted join published, never what
/// the source happens to hold when the resolution finishes, and never a
/// target-only untracked file the resolution edits.
#[tokio::test]
async fn merge_continue_tracks_the_joined_generation_not_the_live_source() -> Result<()> {
    let (_temp, _root, fs, store, main) = fixture().await?;
    write_text(&main, "/base.txt", b"base\n").await?;
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(main.workspace())
        .await?;
    let repo = main.workspace().id();
    run(&fs, &store, &main, repo, &["commit", "-m", "base"]).await?;
    let (_, child) = run(&fs, &store, &main, repo, &["checkout", "-b", "agents/kid"]).await?;
    let child = child.ok_or("missing child view")?;
    write_text(&child, "/base.txt", b"kid\n").await?;
    write_text(&child, "/newkid.txt", b"kid\n").await?;
    run(&fs, &store, &child, repo, &["checkout", "main"]).await?;
    write_text(&main, "/base.txt", b"main\n").await?;
    run(&fs, &store, &main, repo, &["commit", "-m", "main edit"]).await?;
    write_text(&main, "/local.txt", b"untracked\n").await?;
    assert!(
        run(&fs, &store, &main, repo, &["merge", "agents/kid"])
            .await
            .is_err(),
        "the text conflict leaves the merge pending"
    );

    // The child keeps working while the parent resolves: it deletes the file
    // the join already published and authors one the parent also creates.
    child.workspace().remove("/newkid.txt").await?;
    write_text(&child, "/notes.txt", b"kid notes\n").await?;
    write_text(&main, "/base.txt", b"resolved\n").await?;
    write_text(&main, "/notes.txt", b"resolution notes\n").await?;
    write_text(&main, "/local.txt", b"edited while resolving\n").await?;
    let GitCommandOutput::Committed(merge) =
        run(&fs, &store, &main, repo, &["merge", "--continue"])
            .await?
            .0
    else {
        return Err("expected the continued merge commit".into());
    };
    assert_eq!(
        merge.tracked_paths,
        std::collections::BTreeSet::from(["base.txt".to_owned(), "newkid.txt".to_owned()])
    );
    Ok(())
}

/// Git merges by path. A side that replaced a file under a new identity (an
/// atomic save, or a native checkout rewriting it) still holds that path's
/// content, so the merge combines both sides' edits instead of reporting a
/// delete/modify conflict on the replaced identity.
#[tokio::test]
async fn merge_combines_edits_when_one_side_replaced_the_file() -> Result<()> {
    for (ours_replaces, ours_text, expected) in [
        (true, &b"one\ntwo\nthree\n"[..], &b"one\ntwo\nTHREE\n"[..]),
        (true, b"ONE\ntwo\nthree\n", b"ONE\ntwo\nTHREE\n"),
        (false, b"ONE\ntwo\nthree\n", b"ONE\ntwo\nTHREE\n"),
    ] {
        let (_temp, _root, fs, store, main) = fixture().await?;
        write_text(&main, "/notes.txt", b"one\ntwo\nthree\n").await?;
        acyclic_fs::WorkspaceGraph::new(store.clone())
            .register_root(main.workspace())
            .await?;
        let repo = main.workspace().id();
        run(&fs, &store, &main, repo, &["commit", "-m", "base"]).await?;
        let (_, topic) = run(&fs, &store, &main, repo, &["checkout", "-b", "topic"]).await?;
        let topic = topic.ok_or("missing topic view")?;
        if !ours_replaces {
            topic.workspace().remove("/notes.txt").await?;
        }
        write_text(&topic, "/notes.txt", b"one\ntwo\nTHREE\n").await?;
        run(&fs, &store, &topic, repo, &["commit", "-m", "topic"]).await?;
        run(&fs, &store, &topic, repo, &["checkout", "main"]).await?;
        if ours_replaces {
            main.workspace().remove("/notes.txt").await?;
        }
        write_text(&main, "/notes.txt", ours_text).await?;
        if ours_text != b"one\ntwo\nthree\n" {
            run(&fs, &store, &main, repo, &["commit", "-m", "main"]).await?;
        }
        let (output, _) = run(&fs, &store, &main, repo, &["merge", "topic"]).await?;
        assert!(
            matches!(output, GitCommandOutput::Committed(_)),
            "{ours_replaces} {output:?}"
        );
        let stop = CancellationToken::new();
        let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
        assert_eq!(
            main.read("/notes.txt", 64, &mut work).await?.bytes.as_ref(),
            expected,
            "ours replaces: {ours_replaces}"
        );
        assert_eq!(
            main.workspace()
                .stat("/notes.txt", WorkBudget::UNBOUNDED, &stop)
                .await
                .map_err(|failure| failure.error)?
                .value
                .link_count,
            1
        );
    }
    Ok(())
}

/// Continuing re-examines the same resolution generation each time: a
/// conflicted identity the resolution removed has no paths, and asking again
/// reports the remaining markers instead of failing the lookup.
#[tokio::test]
async fn merge_continue_reexamines_a_resolution_whose_conflicted_identity_is_gone() -> Result<()> {
    let (_temp, _root, fs, store, main) = fixture().await?;
    write_text(&main, "/notes.txt", b"base\n").await?;
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(main.workspace())
        .await?;
    let repo = main.workspace().id();
    run(&fs, &store, &main, repo, &["commit", "-m", "base"]).await?;
    let (_, topic) = run(&fs, &store, &main, repo, &["checkout", "-b", "topic"]).await?;
    let topic = topic.ok_or("missing topic view")?;
    write_text(&topic, "/notes.txt", b"topic\n").await?;
    run(&fs, &store, &topic, repo, &["commit", "-m", "topic"]).await?;
    run(&fs, &store, &topic, repo, &["checkout", "main"]).await?;
    main.workspace().remove("/notes.txt").await?;
    run(&fs, &store, &main, repo, &["commit", "-m", "remove"]).await?;
    assert!(
        run(&fs, &store, &main, repo, &["merge", "topic"])
            .await
            .is_err(),
        "delete/modify leaves the merge pending"
    );
    main.workspace().remove("/notes.txt").await?;
    write_text(
        &main,
        "/notes.txt",
        b"<<<<<<< ours\n=======\ntopic\n>>>>>>> theirs\n",
    )
    .await?;
    for _ in 0..2 {
        let error = match run(&fs, &store, &main, repo, &["merge", "--continue"]).await {
            Ok(_) => return Err("a marker-bearing resolution continued".into()),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("conflict markers"), "{error}");
    }
    write_text(&main, "/notes.txt", b"topic\n").await?;
    let GitCommandOutput::Committed(merge) =
        run(&fs, &store, &main, repo, &["merge", "--continue"])
            .await?
            .0
    else {
        return Err("expected the continued merge commit".into());
    };
    assert!(merge.tracked_paths.contains("notes.txt"));
    Ok(())
}

/// A merge cut at every effect boundary, then resumed by the next command,
/// commits the same merge: in particular a resumed transition whose join
/// already published still tracks the child's new file, although the
/// re-planned join sees no changes against the advanced target.
#[tokio::test]
async fn merges_cut_at_every_step_resume_tracking_what_the_join_published() -> Result<()> {
    let mut cut = 0;
    loop {
        cut += 1;
        let (_temp, _root, fs, store, main) = fixture().await?;
        write_text(&main, "/base.txt", b"base\n").await?;
        acyclic_fs::WorkspaceGraph::new(store.clone())
            .register_root(main.workspace())
            .await?;
        let repo = main.workspace().id();
        run(&fs, &store, &main, repo, &["commit", "-m", "base"]).await?;
        let (_, child) = run(&fs, &store, &main, repo, &["checkout", "-b", "agents/kid"]).await?;
        let child = child.ok_or("missing child view")?;
        write_text(&child, "/newkid.txt", b"kid\n").await?;
        run(&fs, &store, &child, repo, &["checkout", "main"]).await?;

        let calls = std::sync::atomic::AtomicUsize::new(0);
        let interrupted = git::run_with_guard(
            git::Workspace {
                filesystem: &fs,
                store: &store,
                view: (&main).into(),
                root: std::path::Path::new("."),
                repository: repo,
                lease: None,
                checkout: None,
                reserved: &[],
                author: "test",
                authored_at_seconds: 1,
            },
            &["merge".to_owned(), "agents/kid".to_owned()],
            || {
                let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                std::future::ready(if call == cut {
                    Err(Error::Unauthorized("cut".into()))
                } else {
                    Ok(())
                })
            },
        )
        .await;
        let completed = interrupted.is_ok();

        // The next command first recovers any pending transition.
        let argv = ["log".to_owned()];
        let mut log = None;
        for _ in 0..2 {
            let output = git::run(
                git::Workspace {
                    filesystem: &fs,
                    store: &store,
                    view: (&main).into(),
                    root: std::path::Path::new("."),
                    repository: repo,
                    lease: None,
                    checkout: None,
                    reserved: &[],
                    author: "test",
                    authored_at_seconds: 1,
                },
                &argv,
            )
            .await?;
            if let Some(GitCommandOutput::Commits(commits)) = output.command {
                log = Some(commits);
                break;
            }
        }
        let mut log = log.ok_or("log never answered")?;
        if log.len() == 1 {
            // Cut before the transition was recorded: nothing to resume, so
            // the merge is simply run again.
            run(&fs, &store, &main, repo, &["merge", "agents/kid"]).await?;
            let GitCommandOutput::Commits(commits) =
                run(&fs, &store, &main, repo, &["log"]).await?.0
            else {
                return Err("expected the merged log".into());
            };
            log = commits;
        }
        assert_eq!(
            log.first().ok_or("empty log")?.tracked_paths,
            std::collections::BTreeSet::from(["base.txt".to_owned(), "newkid.txt".to_owned()]),
            "cut at effect {cut}"
        );
        if completed {
            assert!(cut > 1, "the merge has effect boundaries to cut");
            return Ok(());
        }
    }
}

/// A child forked from a demand-backed checkout edits a host file the parent
/// never touched. Both sides materialize that file under the same
/// source-derived identity, which the fork's authored generation never bound,
/// so the merge must take its base from the materialized fork cut: the edit
/// merges cleanly instead of conflicting with the parent's unchanged copy.
#[tokio::test]
async fn agent_merge_takes_an_unmaterialized_host_file_base_from_the_fork_cut() -> Result<()> {
    let (_temp, root, fs, store, main) = fixture().await?;
    std::fs::write(root.join("calc.py"), b"return a - b\n")?;
    std::fs::write(root.join("notes.txt"), b"one\ntwo\nthree\n")?;
    write_text(&main, "/parent.txt", b"BASE\n").await?;
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(main.workspace())
        .await?;
    let repo = main.workspace().id();
    let (_, child) = run(&fs, &store, &main, repo, &["checkout", "-b", "agents/kid"]).await?;
    let child = child.ok_or("missing child view")?;
    write_text(&child, "/calc.py", b"return a + b\n").await?;
    write_text(&child, "/notes.txt", b"one\ntwo\nTHREE\n").await?;
    write_text(&child, "/child-new.txt", b"CHILD_NEW\n").await?;
    run(&fs, &store, &child, repo, &["checkout", "main"]).await?;
    write_text(&main, "/parent.txt", b"BASE\nPARENT_ONLY\n").await?;
    // The parent also edits a host file the child left alone.
    write_text(&main, "/notes.txt", b"ONE\ntwo\nthree\n").await?;
    let (output, _) = run(&fs, &store, &main, repo, &["merge", "agents/kid"]).await?;
    let GitCommandOutput::Committed(merge) = output else {
        return Err(format!("expected the merge commit, got {output:?}").into());
    };
    assert_eq!(
        merge.tracked_paths,
        std::collections::BTreeSet::from([
            "calc.py".to_owned(),
            "child-new.txt".to_owned(),
            "notes.txt".to_owned(),
        ])
    );
    let stop = CancellationToken::new();
    let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
    for (path, expected) in [
        ("/calc.py", &b"return a + b\n"[..]),
        ("/notes.txt", b"ONE\ntwo\nTHREE\n"),
        ("/parent.txt", b"BASE\nPARENT_ONLY\n"),
        ("/child-new.txt", b"CHILD_NEW\n"),
    ] {
        assert_eq!(
            main.read(path, 64, &mut work).await?.bytes.as_ref(),
            expected,
            "{path}"
        );
    }
    Ok(())
}

/// The fork cut leaves an unobserved host file live, so the host as it is at
/// merge time is not the content the child started from. Here the child
/// edits the file, and then the parent edits the host file in place. Reading
/// today's host as the base would make the parent look unchanged, and an
/// unresolved parent file would read as absent; either way the child's
/// version would silently replace the parent's edit. Only the child's pinned
/// observation could supply the base, and it no longer matches the host, so
/// the merge must stop on the conflict with the parent's edit intact, whether
/// or not the parent materialized the file first.
#[tokio::test]
async fn agent_merge_never_takes_a_base_from_a_host_file_edited_after_the_child_read_it()
-> Result<()> {
    for materialized in [true, false] {
        let (_temp, root, fs, store, main) = fixture().await?;
        std::fs::write(
            root.join("calc.txt"),
            b"one
two
three
",
        )?;
        write_text(
            &main,
            "/parent.txt",
            b"BASE
",
        )
        .await?;
        acyclic_fs::WorkspaceGraph::new(store.clone())
            .register_root(main.workspace())
            .await?;
        let repo = main.workspace().id();
        let (_, child) = run(&fs, &store, &main, repo, &["checkout", "-b", "agents/kid"]).await?;
        let child = child.ok_or("missing child view")?;
        write_text(
            &child,
            "/calc.txt",
            b"one
two
THREE
",
        )
        .await?;
        run(&fs, &store, &child, repo, &["checkout", "main"]).await?;
        // The parent edits the host file itself, never through its view.
        std::fs::write(
            root.join("calc.txt"),
            b"PARENT ONE
two
three
",
        )?;
        if materialized {
            run(&fs, &store, &main, repo, &["status"]).await?;
        }
        let merged = run(&fs, &store, &main, repo, &["merge", "agents/kid"]).await;
        assert!(
            matches!(&merged, Err(error) if error.to_string().contains("conflict")),
            "materialized={materialized}: the child's version replaced an unrecoverable base: {:?}",
            merged.map(|(output, _)| output)
        );
        let stop = CancellationToken::new();
        let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
        assert_eq!(
            main.read("/calc.txt", 64, &mut work).await?.bytes.as_ref(),
            b"PARENT ONE
two
three
",
            "materialized={materialized}"
        );
    }
    Ok(())
}

/// `discard agents/<ref>` drops the parent's reference to a child's branch:
/// its changes can no longer be diffed or merged into the parent, the parent
/// tree never sees them, and the child's own workspace stays intact for its
/// owner. The checked-out branch can never be discarded.
#[tokio::test]
async fn agent_discard_retires_only_the_parent_reference() -> Result<()> {
    let (_temp, _root, fs, store, main) = fixture().await?;
    let stop = CancellationToken::new();
    let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(main.workspace())
        .await?;
    let repo = main.workspace().id();
    let (_, child) = run(&fs, &store, &main, repo, &["checkout", "-b", "agents/kid"]).await?;
    let child = child.ok_or("missing child view")?;
    child
        .write(
            "/child.txt",
            bytes::Bytes::from_static(b"child\n").into(),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut work,
        )
        .await?;
    run(&fs, &store, &child, repo, &["checkout", "main"]).await?;
    assert!(
        run(&fs, &store, &main, repo, &["diff", "agents/kid"])
            .await
            .is_ok()
    );
    assert!(
        run(&fs, &store, &main, repo, &["discard", "main"])
            .await
            .is_err(),
        "the checked-out branch was discarded"
    );
    let (discarded, _) = run(&fs, &store, &main, repo, &["discard", "agents/kid"]).await?;
    let GitCommandOutput::Branches { current, branches } = discarded else {
        return Err(format!("discard lists the remaining branches: {discarded:?}").into());
    };
    assert_eq!(current, "main");
    assert_eq!(
        branches
            .iter()
            .map(|branch| branch.name.as_str())
            .collect::<Vec<_>>(),
        ["main"]
    );
    for command in [
        ["diff", "agents/kid"],
        ["merge", "agents/kid"],
        ["discard", "agents/kid"],
    ] {
        assert!(
            run(&fs, &store, &main, repo, &command).await.is_err(),
            "{command:?} still reaches the discarded branch"
        );
    }
    assert!(main.read("/child.txt", 64, &mut work).await.is_err());
    assert_eq!(
        child
            .read("/child.txt", 64, &mut work)
            .await?
            .bytes
            .as_ref(),
        b"child\n"
    );
    Ok(())
}
