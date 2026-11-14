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
    let argv: Vec<_> = args.iter().map(|arg| (*arg).to_owned()).collect();
    let output = git::run(
        git::Workspace {
            access: &acyclic_harness_core::filesystem::local_owner_workspace_access("test")?,
            filesystem: fs,
            store,
            view: lazy.into(),
            root: std::path::Path::new("."),
            repository: repository_id,
            lease: None,
            checkout: None,
            author: "test",
            authored_at_seconds: 1,
            request: None,
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

async fn read_reported_generation(
    fs: &LocalFs,
    store: &LocalCoreStateStore,
    config: VolumeConfig,
    reference: &serde_json::Value,
) -> Result<Vec<u8>> {
    let bytes = hex::decode(reference.as_str().ok_or("missing generation reference")?)?;
    let generation = acyclic_fs::GenerationId::from_bytes(&bytes)
        .ok_or("invalid canonical generation reference")?;
    let stop = CancellationToken::new();
    let owner = fs
        .generation_workspace_id(generation, config, WorkBudget::UNBOUNDED, &stop)
        .await?
        .value;
    let workspace = acyclic_fs::WorkspaceResolver::resolve(
        &(fs.clone(), acyclic_fs::WorkspaceGraph::new(store.clone())),
        owner,
    )
    .await?;
    let retained = workspace
        .generation(generation, WorkBudget::UNBOUNDED, &stop)
        .await?
        .value;
    Ok(retained
        .read("/file.txt", 64, WorkBudget::UNBOUNDED, &stop)
        .await?
        .value
        .to_vec())
}

#[tokio::test]
async fn facade_commits_branches_merges_and_inspects_managed_files() -> Result<()> {
    let (temp, _root, fs, store, main) = fixture().await?;
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
    for path in ["/binary.dat", "/deleted.txt"] {
        main.write(
            path,
            bytes::Bytes::from_static(b"base\0").into(),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut work,
        )
        .await?;
    }
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(
            main.workspace(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
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
    for (field, expected) in [
        ("from", b"base\n".as_slice()),
        ("to", b"child\n".as_slice()),
    ] {
        assert_eq!(
            read_reported_generation(&fs, &store, main.workspace().volume_config(), &value[field])
                .await?,
            expected
        );
    }
    assert_eq!(
        main.read("/file.txt", 64, &mut work).await?.bytes.as_ref(),
        b"base\n"
    );
    assert!(matches!(
        git::run(
            git::Workspace {
                access: &acyclic_harness_core::filesystem::local_owner_workspace_access("test")?,
                filesystem: &fs,
                store: &store,
                view: (&main).into(),
                root: std::path::Path::new("."),
                repository: repo,
                lease: None,
                checkout: None,
                author: "test",
                authored_at_seconds: 1,
                request: None,
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
    for args in [&["diff"][..], &["grep", "child"], &["blame", "file.txt"]] {
        assert!(matches!(
            run(&fs, &store, &main, repo, args).await?.0,
            GitCommandOutput::Filesystem(_)
        ));
    }
    let GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data { kind, value }) =
        run(&fs, &store, &main, repo, &["archive"]).await?.0
    else {
        return Err("expected a generation archive".into());
    };
    assert_eq!(kind, "archive");
    assert_eq!(value["format"], "acyclic-fs-manifest-v1");
    assert_eq!(
        read_reported_generation(
            &fs,
            &store,
            main.workspace().volume_config(),
            &value["generation"]
        )
        .await?,
        b"child\n"
    );
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
    for (workspace, bytes) in [(&main, b"ours\n".as_slice()), (&child, b"theirs\n")] {
        workspace
            .write(
                "/file.txt",
                bytes::Bytes::copy_from_slice(bytes).into(),
                IdempotencyKey::new(),
                PublicationPermit::Unrestricted,
                &mut work,
            )
            .await?;
    }
    // Force native object-backed content as well as the inline text path. These
    // are authored filesystem inputs; descriptors must retain their records,
    // rather than embedding opaque bodies or borrowing the current HEAD.
    for (workspace, prefix) in [(&main, b"ours\0".as_slice()), (&child, b"theirs\0")] {
        let mut body = vec![0; 4096];
        body[..prefix.len()].copy_from_slice(prefix);
        workspace
            .write(
                "/binary.dat",
                bytes::Bytes::from(body).into(),
                IdempotencyKey::new(),
                PublicationPermit::Unrestricted,
                &mut work,
            )
            .await?;
    }
    main.write(
        "/deleted.txt",
        bytes::Bytes::from_static(b"modified\0").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut work,
    )
    .await?;
    child.remove("/deleted.txt", None, &mut work).await?;
    let (plan, join) = acyclic_harness_core::filesystem::plan_agent_merge(
        &fs,
        &store,
        &child,
        main.workspace(),
        repo,
    )
    .await?;
    assert_eq!(join.source_head(), plan.source_generation);
    assert_eq!(join.target_head(), plan.target_generation);
    assert_eq!(join.common_ancestor(), plan.base_generation);
    assert!(
        plan.child_wins_bindings
            .contains(&"/ignored/child.txt".to_owned())
    );
    assert!(
        !plan
            .child_wins_bindings
            .contains(&"/.acyclic-sdk/state".to_owned())
    );
    assert!(!plan.child_wins_bindings.contains(&"/result.txt".to_owned()));

    // Use the configured native planner's retained join, then reopen completed
    // proposals after retiring its preview and the original filesystem handles.
    // This checks durable files, not recovery of an interrupted page cursor.
    let projection_cancel = CancellationToken::new();
    let preview = join
        .preview_pages(
            acyclic_fs::ApplyOptions {
                if_target: plan.target_generation,
                idempotency_key: IdempotencyKey::new(),
            },
            WorkBudget::UNBOUNDED,
            &projection_cancel,
        )
        .await?
        .value;
    let staging = fs
        .create_workspace_with_config(
            "merge-proposals",
            VolumeConfig::native(Lifecycle::Durable),
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await?
        .value;
    let mut directory = preview.directory(staging.clone(), OperationId::new())?;
    let conflict_id = main
        .workspace()
        .stat("/file.txt", WorkBudget::UNBOUNDED, &stop)
        .await?
        .value
        .file_id;
    let conflict_path =
        directory.conflict_file_path(&acyclic_fs::ConflictKey::File(conflict_id))?;
    let mut non_text = Vec::new();
    for (path, kind) in [
        (
            "/binary.dat",
            acyclic_fs::workspace::MergeConflictFileKind::Binary,
        ),
        (
            "/deleted.txt",
            acyclic_fs::workspace::MergeConflictFileKind::Deleted,
        ),
    ] {
        let id = main
            .workspace()
            .stat(path, WorkBudget::UNBOUNDED, &stop)
            .await?
            .value
            .file_id;
        non_text.push((
            id,
            path,
            kind,
            format!(
                "{}.conflict.json",
                directory.conflict_file_path(&acyclic_fs::ConflictKey::File(id))?
            ),
        ));
    }
    let step = std::num::NonZeroU32::new(1).ok_or("zero page size")?;
    while !directory.plan().complete {
        let before = directory.plan().examined_clean + directory.plan().exposed_conflicts;
        let section = if directory.plan().conflicts_complete {
            acyclic_fs::workspace::MergeDirectorySection::Clean
        } else {
            acyclic_fs::workspace::MergeDirectorySection::Conflicts
        };
        directory
            .write_page(section, step, PublicationPermit::Unrestricted)
            .await?;
        let after = directory.plan().examined_clean + directory.plan().exposed_conflicts;
        assert!(
            after - before <= 1,
            "native page consumed an unbounded delta"
        );
    }
    let expected = directory.plan().clone();
    assert_eq!(expected.ours, plan.target_generation);
    assert_eq!(expected.theirs, plan.source_generation);
    assert_eq!(expected.base, plan.base_generation);
    assert!(expected.clean > 1, "fixture must cross the page boundary");
    assert!(
        expected.conflicts >= 3,
        "fixture must stage text, binary and deletion conflicts"
    );
    assert_eq!(
        main.workspace()
            .head(WorkBudget::UNBOUNDED, &stop)
            .await?
            .value
            .id(),
        expected.ours,
        "proposal staging published the target"
    );
    let plan_path = format!("{}/plan.json", directory.path());
    let binary_key = acyclic_fs::ConflictKey::File(non_text[0].0);
    let resolution_path = format!(
        "{}.resolve.json",
        directory.conflict_file_path(&binary_key)?
    );
    staging
        .write_text(&resolution_path, r#"{"take":"theirs"}"#)
        .await?;
    let selected = staging.head(WorkBudget::UNBOUNDED, &stop).await?.value;
    assert!(matches!(
        directory.resolution(&selected, &binary_key).await?,
        acyclic_fs::MergeResolution::Select(acyclic_fs::ConflictSide::Theirs)
    ));
    projection_cancel.cancel();
    assert!(matches!(
        directory.resolution(&selected, &binary_key).await,
        Err(acyclic_fs::DrivenJoinError::Workspace(
            acyclic_fs::WorkspaceError::Cancelled(_)
        ))
    ));
    assert!(matches!(
        directory
            .write_page(
                acyclic_fs::workspace::MergeDirectorySection::Conflicts,
                step,
                PublicationPermit::Unrestricted,
            )
            .await,
        Err(acyclic_fs::WorkspaceError::Cancelled(_))
    ));
    assert_eq!(
        staging.head(WorkBudget::UNBOUNDED, &stop).await?.value.id(),
        selected.id(),
        "cancelled projection wrote another staging generation"
    );
    drop(directory);
    drop(staging);
    drop(child);
    drop(main);
    drop(fs);
    let reopened = LocalFs::local(LocalOptions::new(temp.path().join("fs"))).await?;
    let staging = reopened
        .open_workspace("merge-proposals", WorkBudget::UNBOUNDED, &stop)
        .await?
        .value;
    let bytes = staging
        .read(&plan_path, 16384, WorkBudget::UNBOUNDED, &stop)
        .await?
        .value;
    let restored: acyclic_fs::workspace::MergeDirectoryPlan = serde_json::from_slice(&bytes)?;
    assert_eq!(restored, expected);
    let text = staging
        .read(&conflict_path, 16384, WorkBudget::UNBOUNDED, &stop)
        .await?
        .value;
    let text = std::str::from_utf8(&text)?;
    assert!(text.contains("<<<<<<<") && text.contains("=======") && text.contains(">>>>>>>"));
    assert!(text.contains("ours\n") && text.contains("theirs\n"));
    for (file_id, source_path, kind, path) in non_text {
        let bytes = staging
            .read(&path, 16384, WorkBudget::UNBOUNDED, &stop)
            .await?
            .value;
        let descriptor: acyclic_fs::workspace::MergeConflictFile = serde_json::from_slice(&bytes)?;
        assert_eq!(descriptor.version, 1);
        assert_eq!(descriptor.kind, kind);
        assert_eq!(descriptor.conflict.path.as_deref(), Some(source_path));
        assert_eq!(
            descriptor.conflict.key,
            acyclic_fs::ConflictKey::File(file_id)
        );
        assert_eq!(
            descriptor.conflict.base,
            acyclic_fs::ConflictValue::Record {
                generation: expected.base,
                file_id,
            }
        );
        assert_eq!(
            descriptor.conflict.ours,
            acyclic_fs::ConflictValue::Record {
                generation: expected.ours,
                file_id,
            }
        );
        let theirs = if kind == acyclic_fs::workspace::MergeConflictFileKind::Deleted {
            acyclic_fs::ConflictValue::Absent
        } else {
            acyclic_fs::ConflictValue::Record {
                generation: expected.theirs,
                file_id,
            }
        };
        assert_eq!(
            descriptor.conflict.theirs, theirs,
            "durable non-text descriptor substituted content or a live-head identity"
        );
    }
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
    let (temp, root, fs, store, main, _) = fixture_native_source(native_file).await?;
    Ok((temp, root, fs, store, main))
}

async fn fixture_native_source(
    native_file: bool,
) -> Result<(
    tempfile::TempDir,
    std::path::PathBuf,
    LocalFs,
    LocalCoreStateStore,
    Lazy,
    Arc<NativeDemandSource>,
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
    let source = Arc::new(
        NativeDemandSource::open(
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
        .value,
    );
    let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
    let (main, _) = LazyWorkspace::attach(
        &fs,
        "main",
        Arc::clone(&source),
        store.clone(),
        acyclic_fs::VolumeInitialization {
            config,
            access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
        },
        OperationId::new(),
        &mut work,
    )
    .await
    .unwrap();
    Ok((temp, root, fs, store, main, source))
}

/// A child projection authors its volume alias beside its own work. Merging
/// the child must publish and track the work, never the alias, and record
/// each parent once.
#[tokio::test]
async fn agent_diff_exactifies_only_its_authorized_target() -> Result<()> {
    let (_temp, root, fs, store, main) = fixture_native(true).await?;
    let repository = main.workspace().id();
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(
            main.workspace(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
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
            access: &acyclic_harness_core::filesystem::local_owner_workspace_access("test")?,
            filesystem: &fs,
            store: &store,
            view: (&main).into(),
            root: &root,
            repository,
            lease: Some(lease.clone()),
            checkout: None,
            author: "test",
            authored_at_seconds: 1,
            request: None,
        },
        &["diff".into(), "agents/child".into()],
    )
    .await?;
    let Some(GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data { value, .. })) =
        output.command
    else {
        return Err("agent diff returned no data".into());
    };
    assert_eq!(
        value["fileChanges"], 0,
        "source materialization is not a child write"
    );
    assert_eq!(value["paths"], serde_json::json!([]));
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

/// GC-217: a parent showing a path its child holds as a directory made a
/// definitive refusal look like a storage failure. That failed the parent's
/// delivery and stopped the owner serving the whole tree. The refusal must
/// settle the call as the model's error; a storage cause still must not.
#[tokio::test]
async fn child_show_of_a_directory_settles_as_the_models_error() -> Result<()> {
    let (_temp, _root, fs, store, main) = fixture().await?;
    let repository = main.workspace().id();
    let cancellation = CancellationToken::new();
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(main.workspace(), WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?;
    let (_, child) = run(
        &fs,
        &store,
        &main,
        repository,
        &["checkout", "-b", "agents/audit"],
    )
    .await?;
    let child = child.ok_or("missing child")?;
    run(&fs, &store, &child, repository, &["checkout", "main"]).await?;
    child
        .write(
            "/findings/report.md",
            bytes::Bytes::from_static(b"finding\n").into(),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
        )
        .await?;
    let (shown, _) = run(
        &fs,
        &store,
        &main,
        repository,
        &["show", "agents/audit:findings/report.md"],
    )
    .await?;
    let GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data { value, .. }) = shown
    else {
        return Err("expected the child's file".into());
    };
    assert_eq!(value["text"], "finding\n");
    let Err(refused) = run(
        &fs,
        &store,
        &main,
        repository,
        &["show", "agents/audit:findings"],
    )
    .await
    else {
        return Err("a directory has no file page".into());
    };
    let refused = refused
        .downcast::<Error>()
        .map_err(|other| format!("expected a harness error, got {other}"))?;
    assert_eq!(
        refused.code(),
        acyclic_harness_core::wire::ErrorCode::Invalid,
        "{refused:?}"
    );
    assert!(refused.settles_tool_call(), "{refused:?}");
    // Negative control: a lost lazy record is not the request's error.
    let lost = Error::from(acyclic_fs::LazyWorkspaceError::Store("record lost".into()));
    assert!(!lost.settles_tool_call(), "{lost:?}");
    // Actual failed accounting, whether direct or wrapped by a lazy checkout,
    // must retain a harness failure rather than settle the accepted model call.
    use acyclic_fs::{LazyWorkspaceError, WorkCounters, WorkspaceError};
    let mut counters = WorkCounters::UNCHARGED;
    let exhausted = counters
        .charge_items(1, &WorkBudget::UNCHARGED)
        .expect_err("the operation has no admitted item credit");
    let prior = WorkCounters {
        items_examined: u64::MAX,
        ..WorkCounters::UNCHARGED
    };
    let backend = WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    };
    let overflow = prior
        .checked_add(backend)
        .expect_err("the completed work sum is not representable");
    for lazy in [false, true] {
        for source in [
            WorkspaceError::Work(exhausted),
            WorkspaceError::WorkMerge {
                prior: Box::new(prior),
                backend: Box::new(backend),
                error: overflow,
            },
        ] {
            let error = if lazy {
                Error::from(LazyWorkspaceError::WorkspaceCause(source))
            } else {
                Error::from(source)
            };
            assert_eq!(
                error.code(),
                acyclic_harness_core::wire::ErrorCode::Storage,
                "{error:?}"
            );
            assert!(error.leaves_outcome_uncertain(), "{error:?}");
            assert!(!error.settles_tool_call(), "{error:?}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn child_ignore_rules_do_not_erase_inherited_fork_base() -> Result<()> {
    use acyclic_fs::{GitFilesystemResult, GitPathStatus};

    let (_temp, root, fs, store, main) = fixture_native(true).await?;
    std::fs::create_dir(root.join("nested"))?;
    std::fs::write(root.join("nested/native.txt"), b"nested origin\n")?;
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(
            main.workspace(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    let repository = main.workspace().id();
    let (_, child) = run(
        &fs,
        &store,
        &main,
        repository,
        &["checkout", "-b", "agents/ignored"],
    )
    .await?;
    let child = child.ok_or("missing child")?;
    run(&fs, &store, &child, repository, &["checkout", "main"]).await?;
    // Neither inherited native file was authored or committed before the fork.
    // The child hides both, while each side also has real visible and ignored work.
    write_text(&child, "/.gitignore", b"native.txt\nchild-only.txt\n").await?;
    write_text(&child, "/child-only.txt", b"ignored child\n").await?;
    write_text(&child, "/visible-child.txt", b"child finding\n").await?;
    write_text(&main, "/.gitignore", b"parent-only.txt\n").await?;
    write_text(&main, "/parent-only.txt", b"ignored parent\n").await?;
    write_text(&main, "/visible-parent.txt", b"parent finding\n").await?;

    for stat in [false, true] {
        let args = if stat {
            vec!["diff", "agents/ignored", "--stat"]
        } else {
            vec!["diff", "agents/ignored"]
        };
        let (output, _) = run(&fs, &store, &main, repository, &args).await?;
        let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = output else {
            return Err("expected child diff data".into());
        };
        assert_eq!(value["fileChanges"], 2);
        assert_eq!(value["bindingChanges"], 2);
        if !stat {
            assert_eq!(
                value["paths"],
                serde_json::json!([
                    {"path":".gitignore", "status":GitPathStatus::Added, "side":"both"},
                    {"path":"visible-child.txt", "status":GitPathStatus::Added, "side":"child"},
                ])
            );
            assert_eq!(
                value["parentPaths"],
                serde_json::json!([
                    {"path":".gitignore", "status":GitPathStatus::Added, "side":"both"},
                    {"path":"visible-parent.txt", "status":GitPathStatus::Added, "side":"parent"},
                ]),
                "inherited files ignored only by the child are not parent additions"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn exactification_preserves_deletions_and_admits_new_inherited_source_entries() -> Result<()>
{
    use acyclic_fs::GitFilesystemResult;
    // Exercise both a still-current source and an explicitly advanced source
    // cut. Completion cannot certify absence in either live namespace.
    for invalidate in [false, true] {
        let (_temp, root, fs, store, main, source) = fixture_native_source(true).await?;
        std::fs::create_dir(root.join("recreated"))?;
        std::fs::write(root.join("recreated/deleted.md"), b"deleted\n")?;
        let cancellation = CancellationToken::new();
        let mut work = WorkBudget::UNBOUNDED.accounting(&cancellation);
        main.remove("/recreated", None, &mut work).await?;
        main.write(
            "/recreated/new.md",
            bytes::Bytes::from_static(b"new\n").into(),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut work,
        )
        .await?;
        for _ in 0..2 {
            main.exactify("/recreated", PublicationPermit::Unrestricted, &mut work)
                .await?;
            assert!(
                main.inspect("/recreated/deleted.md", &mut work)
                    .await
                    .is_err()
            );
            let (page, _guard) = main
                .list_directory("/recreated", None, 16, &mut work)
                .await?;
            assert_eq!(page.entries.len(), 1);
            assert_eq!(
                page.entries[0]
                    .name
                    .unicode_text(WorkBudget::UNBOUNDED, &cancellation)?
                    .value,
                "new.md"
            );
        }
        main.exactify("/", PublicationPermit::Unrestricted, &mut work)
            .await?;
        std::fs::write(root.join("inherited-later.md"), b"inherited\n")?;
        if invalidate {
            source.invalidate();
        }
        let repository = main.workspace().id();
        acyclic_fs::WorkspaceGraph::new(store.clone())
            .register_root(main.workspace(), WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?;
        let (_, child) = run(
            &fs,
            &store,
            &main,
            repository,
            &["checkout", "-b", "agents/later-source"],
        )
        .await?;
        let child = child.ok_or("later-source checkout missing")?;
        run(&fs, &store, &child, repository, &["checkout", "main"]).await?;
        for _ in 0..2 {
            let (output, _) = run(
                &fs,
                &store,
                &main,
                repository,
                &["diff", "agents/later-source"],
            )
            .await?;
            let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = output
            else {
                return Err("expected untouched child diff".into());
            };
            assert_eq!(value["fileChanges"], 0, "invalidate={invalidate}: {value}");
            assert_eq!(value["paths"], serde_json::json!([]));
        }
        // The base was fully saved with a native deletion mask and an
        // authored replacement. Replacing that saved directory with a file
        // deletes its visible child, never the already-masked native file.
        child.remove("/recreated/new.md", None, &mut work).await?;
        child.remove("/recreated", None, &mut work).await?;
        child
            .write(
                "/recreated",
                bytes::Bytes::from_static(b"replacement\n").into(),
                IdempotencyKey::new(),
                PublicationPermit::Unrestricted,
                &mut work,
            )
            .await?;
        let (output, _) = run(
            &fs,
            &store,
            &main,
            repository,
            &["diff", "agents/later-source"],
        )
        .await?;
        let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = output else {
            return Err("expected masked replacement diff".into());
        };
        assert_eq!(value["fileChanges"], 2);
        assert_eq!(
            value["paths"],
            serde_json::json!([
                {"path":"recreated", "status":"added", "side":"child"},
                {"path":"recreated/new.md", "status":"deleted", "side":"child"},
            ]),
            "invalidate={invalidate}: {value}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn child_directory_deletions_distinguish_saved_and_partial_source_bases() -> Result<()> {
    use acyclic_fs::GitFilesystemResult;
    for (saved, late_before_fork, invalidate) in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (true, true, true),
    ] {
        let (_temp, root, fs, store, main, source) = fixture_native_source(true).await?;
        std::fs::create_dir_all(root.join("saved/nested"))?;
        std::fs::write(root.join("saved/first.md"), b"first\n")?;
        std::fs::write(root.join("saved/nested/second.md"), b"second\n")?;
        let repository = main.workspace().id();
        let cancellation = CancellationToken::new();
        let mut work = WorkBudget::UNBOUNDED.accounting(&cancellation);
        // Creating this file promotes only its parents. Directory presence
        // alone is not proof that the source descendants were saved.
        main.write(
            "/saved/authored.md",
            bytes::Bytes::from_static(b"authored\n").into(),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut work,
        )
        .await?;
        assert!(
            main.workspace()
                .head(WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .stat("/saved/first.md", WorkBudget::UNBOUNDED, &cancellation)
                .await
                .is_err()
        );
        if saved {
            main.exactify("/saved", PublicationPermit::Unrestricted, &mut work)
                .await?;
        }
        if late_before_fork {
            std::fs::write(root.join("saved/new-before-fork.md"), b"new\n")?;
            if invalidate {
                source.invalidate();
            }
        }
        acyclic_fs::WorkspaceGraph::new(store.clone())
            .register_root(main.workspace(), WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?;
        let (_, child) = run(
            &fs,
            &store,
            &main,
            repository,
            &["checkout", "-b", "agents/saved-review"],
        )
        .await?;
        let child = child.ok_or("saved-directory checkout missing")?;
        run(&fs, &store, &child, repository, &["checkout", "main"]).await?;
        if late_before_fork {
            let lookup = child.inspect("/saved/new-before-fork.md", &mut work).await;
            if invalidate {
                child.retire_lookup(lookup?, &mut work)?;
            } else {
                assert!(lookup.is_err());
            }
        }
        child.remove("/saved/authored.md", None, &mut work).await?;
        if saved {
            child.remove("/saved/first.md", None, &mut work).await?;
            child
                .remove("/saved/nested/second.md", None, &mut work)
                .await?;
            child.remove("/saved/nested", None, &mut work).await?;
        }
        child.remove("/saved", None, &mut work).await?;
        if saved && !late_before_fork {
            // This changes the live directory version after deletion. The
            // original saved generation still contains all comparison data.
            std::fs::write(root.join("saved/new-after-fork.md"), b"later\n")?;
        }
        for _ in 0..2 {
            let (output, _) = run(
                &fs,
                &store,
                &main,
                repository,
                &["diff", "agents/saved-review"],
            )
            .await?;
            let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = output
            else {
                return Err("expected saved-directory deletion diff".into());
            };
            assert_eq!(
                value["fileChanges"],
                if invalidate { 4 } else { 3 },
                "saved={saved}, late={late_before_fork}, invalidate={invalidate}: {value}"
            );
            let mut expected = vec![
                serde_json::json!({"path":"saved/authored.md", "status":"deleted", "side":"child"}),
                serde_json::json!({"path":"saved/first.md", "status":"deleted", "side":"child"}),
                serde_json::json!({"path":"saved/nested/second.md", "status":"deleted", "side":"child"}),
            ];
            if invalidate {
                expected.push(serde_json::json!({"path":"saved/new-before-fork.md", "status":"deleted", "side":"child"}));
            }
            assert_eq!(value["paths"], serde_json::json!(expected), "saved={saved}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn child_inspection_replays_its_fork_delta_and_reads_files_without_merging() -> Result<()> {
    use acyclic_fs::{GitFilesystemResult, GitPathStatus};
    for native in [false, true] {
        let (_temp, _root, fs, store, main) = fixture_native(native).await?;
        if native {
            std::fs::create_dir_all(_root.join("unseen-directory/nested"))?;
            std::fs::write(_root.join("unseen-directory/first.md"), b"first\n")?;
            std::fs::write(_root.join("unseen-directory/nested/second.md"), b"second\n")?;
            std::fs::write(_root.join("unseen-directory/ignored.tmp"), b"ignored\n")?;
        }
        #[cfg(unix)]
        if native {
            std::os::unix::fs::symlink("native.txt", _root.join("native-link"))?;
            std::fs::hard_link(_root.join("native-link"), _root.join("native-link-alias"))?;
        }
        let repository = main.workspace().id();
        let cancellation = CancellationToken::new();
        let mut work = WorkBudget::UNBOUNDED.accounting(&cancellation);
        for (path, bytes) in [
            ("/shared.md", b"base\n".as_slice()),
            ("/removed.md", b"remove me\n"),
            ("/.gitignore", b"*.tmp\n"),
        ] {
            main.write(
                path,
                bytes::Bytes::copy_from_slice(bytes).into(),
                IdempotencyKey::new(),
                PublicationPermit::Unrestricted,
                &mut work,
            )
            .await?;
        }
        acyclic_fs::WorkspaceGraph::new(store.clone())
            .register_root(main.workspace(), WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?;
        let (_, child) = run(
            &fs,
            &store,
            &main,
            repository,
            &["checkout", "-b", "agents/review"],
        )
        .await?;
        let child = child.ok_or("child checkout missing")?;
        run(&fs, &store, &child, repository, &["checkout", "main"]).await?;
        if native {
            // Remove an inherited directory before any diff can materialize
            // its descendants. A directory tombstone must retain every file
            // deletion, including nested files, under the original ignores.
            let (_, deleted) = run(
                &fs,
                &store,
                &main,
                repository,
                &["checkout", "-b", "agents/deleted-directory"],
            )
            .await?;
            let deleted = deleted.ok_or("deleted-directory checkout missing")?;
            run(&fs, &store, &deleted, repository, &["checkout", "main"]).await?;
            assert!(
                deleted
                    .workspace()
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await?
                    .value
                    .stat("/unseen-directory", WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .is_err()
            );
            deleted.remove("/unseen-directory", None, &mut work).await?;
            for _ in 0..2 {
                let (output, _) = run(
                    &fs,
                    &store,
                    &main,
                    repository,
                    &["diff", "agents/deleted-directory"],
                )
                .await?;
                let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = output
                else {
                    return Err("expected directory deletion diff".into());
                };
                assert_eq!(value["fileChanges"], 2);
                assert_eq!(
                    value["paths"],
                    serde_json::json!([
                        {"path":"unseen-directory/first.md", "status":"deleted", "side":"child"},
                        {"path":"unseen-directory/nested/second.md", "status":"deleted", "side":"child"},
                    ])
                );
            }
        }
        for (path, bytes) in [
            ("/shared.md", b"parent edit\n".as_slice()),
            ("/parent-only.md", b"parent finding\n"),
        ] {
            main.write(
                path,
                bytes::Bytes::copy_from_slice(bytes).into(),
                IdempotencyKey::new(),
                PublicationPermit::Unrestricted,
                &mut work,
            )
            .await?;
        }
        // GC-176: the parent advanced after this fork; a child with no writes
        // has no changes, including after demand materialization and replay.
        #[cfg(unix)]
        if native {
            assert_eq!(
                child
                    .read_link("/native-link", None, &mut work)
                    .await?
                    .bytes
                    .as_ref(),
                b"native.txt"
            );
            let (shown, _) = run(
                &fs,
                &store,
                &main,
                repository,
                &["show", "agents/review:native-link"],
            )
            .await?;
            let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = shown
            else {
                return Err("expected native link target".into());
            };
            assert_eq!(value["text"], "native.txt");
            // The promoted identity is shared by an alias with no authored
            // namespace binding. Paging must read that exact Shadow record.
            child
                .promote(
                    "/native-link",
                    64 * 1024,
                    IdempotencyKey::new(),
                    PublicationPermit::Unrestricted,
                    &mut work,
                )
                .await?;
            assert!(
                child
                    .workspace()
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await?
                    .value
                    .stat("/native-link-alias", WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .is_err()
            );
            let lookup = child.lookup("/native-link-alias", &mut work).await?;
            assert!(matches!(lookup.0, acyclic_fs::LazyLookup::Shadow { .. }));
            child.retire_lookup(lookup, &mut work)?;
            let (shown, _) = run(
                &fs,
                &store,
                &main,
                repository,
                &["show", "agents/review:native-link-alias", "--offset", "3"],
            )
            .await?;
            let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = shown
            else {
                return Err("expected shared link target page".into());
            };
            assert_eq!(value["text"], "ive.txt");
            assert_eq!(value["offset"], "3");
        }
        for args in [
            vec!["diff", "agents/review"],
            vec!["diff", "agents/review", "--stat"],
            vec!["diff", "--stat", "agents/review"],
        ] {
            let (output, switched) = run(&fs, &store, &main, repository, &args).await?;
            assert!(switched.is_none());
            let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = output
            else {
                return Err("expected child diff data".into());
            };
            assert_eq!(value["fileChanges"], 0);
            assert_eq!(value["bindingChanges"], 0);
            if args.len() == 2 {
                assert_eq!(value["paths"], serde_json::json!([]));
            }
        }
        for (path, bytes) in [
            ("/shared.md", b"child edit\n".as_slice()),
            ("/finding.md", b"live finding\n"),
            ("/ignored.tmp", b"ignored\n"),
        ] {
            child
                .write(
                    path,
                    bytes::Bytes::copy_from_slice(bytes).into(),
                    IdempotencyKey::new(),
                    PublicationPermit::Unrestricted,
                    &mut work,
                )
                .await?;
        }
        child.remove("/removed.md", None, &mut work).await?;
        if native {
            child.remove("/native.txt", None, &mut work).await?;
        }
        let parent_head = main
            .workspace()
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .id();
        let child_head = child
            .workspace()
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .id();
        let (shown, switched) = run(
            &fs,
            &store,
            &main,
            repository,
            &["show", "agents/review:finding.md"],
        )
        .await?;
        assert!(switched.is_none());
        let GitCommandOutput::Filesystem(GitFilesystemResult::Data { kind, value }) = shown else {
            return Err("expected child file".into());
        };
        assert_eq!(kind, "file");
        assert_eq!(value["text"], "live finding\n");
        assert_eq!(
            main.workspace()
                .head(WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .id(),
            parent_head
        );
        assert_eq!(
            child
                .workspace()
                .head(WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .id(),
            child_head
        );
        for _replay in 0..2 {
            let (output, switched) =
                run(&fs, &store, &main, repository, &["diff", "agents/review"]).await?;
            assert!(switched.is_none());
            let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = output
            else {
                return Err("expected child delta".into());
            };
            assert_eq!(value["fileChanges"], if native { 4 } else { 3 });
            let mut expected = vec![
                serde_json::json!({"path":"finding.md", "status":GitPathStatus::Added, "side":"child"}),
                serde_json::json!({"path":"removed.md", "status":GitPathStatus::Deleted, "side":"child"}),
                serde_json::json!({"path":"shared.md", "status":GitPathStatus::Modified, "side":"both"}),
            ];
            if native {
                expected.insert(1, serde_json::json!({"path":"native.txt", "status":GitPathStatus::Deleted, "side":"child"}));
            }
            assert_eq!(value["paths"], serde_json::json!(expected));
            assert_eq!(
                value["parentPaths"],
                serde_json::json!([
                    {"path":"parent-only.md", "status":GitPathStatus::Added, "side":"parent"},
                    {"path":"shared.md", "status":GitPathStatus::Modified, "side":"both"},
                ])
            );
        }
        assert_eq!(
            main.read("/shared.md", 64, &mut work).await?.bytes.as_ref(),
            b"parent edit\n"
        );
        assert!(main.read("/finding.md", 64, &mut work).await.is_err());
        assert!(
            run(
                &fs,
                &store,
                &main,
                repository,
                &["show", "agents/review:../outside"]
            )
            .await
            .is_err()
        );
        // Each request reads one page, even though the file and its complete
        // JSON projection would be larger. Both child and committed inspection
        // use the same byte continuation and can reach the complete file.
        let page = 64 * 1024;
        let mut content = vec![b'p'; 2 * page + 7];
        // Valid whole-file UTF-8 straddles the byte-page boundary. Each page
        // describes its own exact bytes, independently of adjacent pages.
        content[page - 1..page + 2].copy_from_slice("€".as_bytes());
        for view in [&main, &child] {
            view.write(
                "/paged.bin",
                bytes::Bytes::copy_from_slice(&content).into(),
                IdempotencyKey::new(),
                PublicationPermit::Unrestricted,
                &mut work,
            )
            .await?;
        }
        run(
            &fs,
            &store,
            &main,
            repository,
            &["commit", "-m", "inspect pages"],
        )
        .await?;
        for object in ["agents/review:paged.bin", "HEAD:paged.bin"] {
            let mut joined = Vec::new();
            for offset in [0, page, 2 * page, content.len()] {
                let offset_text = offset.to_string();
                let (shown, _) = run(
                    &fs,
                    &store,
                    &main,
                    repository,
                    &["show", object, "--offset", &offset_text],
                )
                .await?;
                let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = shown
                else {
                    return Err("expected file page".into());
                };
                let length = (content.len() - offset).min(page);
                let bytes = if let Some(text) = value["text"].as_str() {
                    text.as_bytes().to_vec()
                } else {
                    base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        value["base64"].as_str().unwrap(),
                    )?
                };
                assert_eq!(bytes, &content[offset..offset + length]);
                joined.extend_from_slice(&bytes);
                assert_eq!(value["byteLength"], content.len().to_string());
                assert_eq!(value["offset"], offset.to_string());
                assert_eq!(
                    value["nextOffset"],
                    serde_json::json!(
                        (offset + length < content.len()).then(|| (offset + length).to_string())
                    )
                );
            }
            assert_eq!(joined, content);
            let (shown, _) = run(
                &fs,
                &store,
                &main,
                repository,
                &["show", object, "--offset", "18446744073709551615"],
            )
            .await?;
            let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = shown
            else {
                return Err("expected exact maximum offset".into());
            };
            assert_eq!(value["offset"], u64::MAX.to_string());
            assert!(value["nextOffset"].is_null());
        }
    }
    Ok(())
}

#[tokio::test]
async fn checkout_and_mounted_commands_both_reject_expired_leases() -> Result<()> {
    let (_temp, root, fs, store, main) = fixture().await?;
    let repo = main.workspace().id();
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(
            main.workspace(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
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
            Some(acyclic_fs::hold_root(&root)?.identity()),
            "checkout-expired",
        ),
    ] {
        let error = git::run(
            git::Workspace {
                access: &acyclic_harness_core::filesystem::local_owner_workspace_access("test")?,
                filesystem: &fs,
                store: &store,
                view: (&main).into(),
                root: &root,
                repository: repo,
                lease: Some(lease.clone()),
                checkout,
                author: "test",
                authored_at_seconds: 1,
                request: None,
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
                access: &acyclic_harness_core::filesystem::local_owner_workspace_access("test")?,
                filesystem: &fs,
                store: &store,
                view: (&main).into(),
                root: &root,
                repository: repo,
                lease: Some(lease.clone()),
                checkout,
                author: "test",
                authored_at_seconds: 1,
                request: None,
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

// Internal authority model for the public publication boundary. Authenticated
// request/decision files are exercised by the journal suite.
struct PublicationDecision {
    interaction: acyclic_harness_core::InteractionId,
    operation: acyclic_harness_core::OperationId,
    digest: [u8; 32],
    approved: bool,
}
impl acyclic_harness_core::tool::ToolApprovalVerifier for PublicationDecision {
    fn verify<'a>(
        &'a self,
        _: acyclic_harness_core::InteractionId,
        _: acyclic_harness_core::OperationId,
        _: [u8; 32],
    ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness_core::Result<()>> {
        Box::pin(std::future::ready(Err(
            acyclic_harness_core::Error::Unsupported(
                "selected approval requires original native work".into(),
            ),
        )))
    }
    fn verify_with_native_work<'a>(
        &'a self,
        interaction: acyclic_harness_core::InteractionId,
        operation: acyclic_harness_core::OperationId,
        digest: [u8; 32],
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness_core::Result<acyclic_fs::NativeFutureBundle<'a, (), crate::Error>> {
        acyclic_fs::NativeFutureBundle::new(work, || async move {
            if self.approved
                && self.interaction == interaction
                && self.operation == operation
                && self.digest == digest
            {
                Ok(())
            } else {
                Err(Error::Unauthorized("denied or foreign publication".into()))
            }
        })
    }
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
    write_text(
        &main,
        "/guard-probe",
        b"must not reach the replaced checkout",
    )
    .await?;
    let candidate = main.workspace().head(&work, &stop).await?;
    let identity = work.root_identity(&root)?;
    let operation = OperationId::new();
    let publication = acyclic_harness_core::filesystem::CheckoutPublication::new(
        &root, identity, operation, &store, &work,
    )?;
    let options = MaterializeOptions {
        materialized_view: None,
        destination: publication.directory().join("target"),
        expected_root_identity: None,
        maximum_directory_entries: 32,
        maximum_extent_spans: Some(32),
        transfer_bytes: 4096,
    };
    let paths = ["guard-probe".to_owned()];
    let prepared = publication
        .prepare_selected(
            &generation,
            &candidate,
            &store,
            &options,
            &work,
            (&paths, ()),
        )
        .await?;
    let approved = publication.approval_value(&generation, &candidate, &paths, &prepared, ())?;
    let confirmation = approved.digest(&work)?;
    let mut decision = PublicationDecision {
        interaction: acyclic_harness_core::InteractionId::from_bytes([101; 16]),
        operation: acyclic_harness_core::OperationId::from_bytes(operation.into_bytes()),
        digest: confirmation,
        approved: false,
    };
    assert!(
        approved
            .authorize(&decision, decision.interaction, &confirmation, &work)
            .await
            .is_err()
    );
    assert!(!root.join("guard-probe").exists());
    decision.approved = true;
    let approved = publication
        .approval_value(&generation, &candidate, &paths, &prepared, ())?
        .authorize(&decision, decision.interaction, &confirmation, &work)
        .await?;
    let mut guards = 0;
    let error = approved
        .publish(&store, &options, || {
            guards += 1;
            let changed = std::fs::rename(&root, root.with_extension("original"))
                .and_then(|()| std::fs::create_dir(&root));
            std::future::ready(changed.map_err(|cause| {
                acyclic_harness_core::Error::from(NativeTreeMaterializationError::Io(cause))
            }))
        })
        .await
        .expect_err("the checkout identity changed after authorization");
    assert_eq!(guards, 1);
    assert!(!root.join("guard-probe").exists());
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
async fn checkout_state_layout_is_refused_before_logical_git_effects() -> Result<()> {
    let (_temp, root, fs, _store, main) = fixture().await?;
    write_text(&main, "/authored", b"retain").await?;
    let stop = CancellationToken::new();
    let before = main
        .workspace()
        .head(WorkBudget::UNBOUNDED, &stop)
        .await?
        .value
        .id();
    let identity = acyclic_fs::hold_root(&root)?.identity();
    let stores = std::iter::once(LocalCoreStateStore::new(root.join("state")));
    #[cfg(target_os = "linux")]
    let other = {
        use std::os::unix::fs::MetadataExt as _;
        let device = std::fs::metadata(&root)?.dev();
        // TMPDIR may select either filesystem; choose a distinct one rather
        // than assuming the fixture lives outside shared memory.
        let base = if std::fs::metadata("/dev/shm")?.dev() != device {
            "/dev/shm"
        } else {
            "/tmp"
        };
        assert_ne!(
            std::fs::metadata(base)?.dev(),
            device,
            "cross-device qualification needs a second filesystem"
        );
        tempfile::tempdir_in(base)?
    };
    #[cfg(target_os = "linux")]
    let stores = stores.chain(std::iter::once(LocalCoreStateStore::new(
        other.path().join("state"),
    )));
    for store in stores {
        let result = git::run(
            git::Workspace {
                access: &acyclic_harness_core::filesystem::local_owner_workspace_access("test")?,
                filesystem: &fs,
                store: &store,
                view: (&main).into(),
                root: &root,
                repository: main.workspace().id(),
                lease: None,
                checkout: Some(identity),
                author: "test",
                authored_at_seconds: 1,
                request: None,
            },
            &["clean".into(), "-f".into()],
        )
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => return Err("invalid host state accepted cleaning authored files".into()),
        };
        assert!(
            matches!(
                error,
                Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                    ..
                }
            ),
            "{error:?}"
        );
        assert_eq!(
            main.workspace()
                .head(WorkBudget::UNBOUNDED, &stop)
                .await?
                .value
                .id(),
            before
        );
        assert_eq!(
            read_text(&main, "/authored").await?.as_deref(),
            Some(b"retain".as_slice())
        );
        assert_eq!(
            std::fs::read_dir(&root)?.count(),
            0,
            "admission wrote harness state inside checkout"
        );
        assert!(
            !store.root().exists(),
            "admission created an unusable state directory"
        );
    }
    Ok(())
}

#[tokio::test]
async fn every_workspace_name_is_committed_restored_and_cleaned() -> Result<()> {
    use acyclic_fs::GitFilesystemResult;

    let (_temp, _root, fs, store, main) = fixture().await?;
    let repo = main.workspace().id();
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(
            main.workspace(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    let stop = CancellationToken::new();
    let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
    let names = [
        ".volumes/project.txt",
        ".system/user.txt",
        ".acyclic-sdk/state",
        ".home-seed-v1",
        ".git/info/exclude",
        ".GIT/HEAD",
    ];
    for path in names {
        write_text(&main, &format!("/{path}"), b"user-owned directory\n").await?;
    }
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
    for path in names {
        assert!(
            paths.iter().any(|tracked| tracked == path),
            "missing {path}: {paths:?}"
        );
        write_text(&main, &format!("/{path}"), b"edited\n").await?;
    }
    run(&fs, &store, &main, repo, &["restore", "."]).await?;
    for path in names {
        assert_eq!(
            main.read(&format!("/{path}"), 64, &mut work)
                .await?
                .bytes
                .as_ref(),
            b"user-owned directory\n",
            "{path}"
        );
    }

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
    for continuation in [["merge", "--continue"], ["merge", "agents/kid"]] {
        let (_temp, _root, fs, store, main) = fixture().await?;
        write_text(&main, "/base.txt", b"base\n").await?;
        acyclic_fs::WorkspaceGraph::new(store.clone())
            .register_root(
                main.workspace(),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?;
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
        assert!(
            run(&fs, &store, &main, repo, &["merge", "main"])
                .await
                .is_err(),
            "only the conflicted branch continues its merge"
        );
        let GitCommandOutput::Committed(merge) =
            run(&fs, &store, &main, repo, &continuation).await?.0
        else {
            return Err("expected the continued merge commit".into());
        };
        assert_eq!(
            merge.tracked_paths,
            std::collections::BTreeSet::from(["base.txt".to_owned(), "newkid.txt".to_owned()])
        );
    }
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
            .register_root(
                main.workspace(),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?;
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
        .register_root(
            main.workspace(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
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
        assert!(error.starts_with("merge conflict: /notes.txt."), "{error}");
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
            .register_root(
                main.workspace(),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?;
        let repo = main.workspace().id();
        run(&fs, &store, &main, repo, &["commit", "-m", "base"]).await?;
        let (_, child) = run(&fs, &store, &main, repo, &["checkout", "-b", "agents/kid"]).await?;
        let child = child.ok_or("missing child view")?;
        write_text(&child, "/newkid.txt", b"kid\n").await?;
        run(&fs, &store, &child, repo, &["checkout", "main"]).await?;

        let calls = std::sync::atomic::AtomicUsize::new(0);
        let interrupted = git::run_with_guard(
            git::Workspace {
                access: &acyclic_harness_core::filesystem::local_owner_workspace_access("test")?,
                filesystem: &fs,
                store: &store,
                view: (&main).into(),
                root: std::path::Path::new("."),
                repository: repo,
                lease: None,
                checkout: None,
                author: "test",
                authored_at_seconds: 1,
                request: None,
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
                    access: &acyclic_harness_core::filesystem::local_owner_workspace_access(
                        "test",
                    )?,
                    filesystem: &fs,
                    store: &store,
                    view: (&main).into(),
                    root: std::path::Path::new("."),
                    repository: repo,
                    lease: None,
                    checkout: None,
                    author: "test",
                    authored_at_seconds: 1,
                    request: None,
                },
                &argv,
            )
            .await?;
            if let Some(GitCommandOutput::Commits { commits, .. }) = output.command {
                log = Some(commits);
                break;
            }
        }
        let mut log = log.ok_or("log never answered")?;
        if log.len() == 1 {
            // Cut before the transition was recorded: nothing to resume, so
            // the merge is simply run again.
            run(&fs, &store, &main, repo, &["merge", "agents/kid"]).await?;
            let GitCommandOutput::Commits { commits, .. } =
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
        .register_root(
            main.workspace(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
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
            .register_root(
                main.workspace(),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?;
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
        // Git leaves the conflict in the worktree: the parent's host edit is
        // ours, the child's version theirs, and the unrecoverable base empty.
        let stop = CancellationToken::new();
        let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
        assert_eq!(
            main.read("/calc.txt", 1 << 10, &mut work)
                .await?
                .bytes
                .as_ref(),
            b"<<<<<<< ours
PARENT ONE
two
three
||||||| original
=======
one
two
THREE
>>>>>>> theirs
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
        .register_root(
            main.workspace(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
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

/// Reads `path` from the view's head, or `None` when it is absent.
async fn read_text(view: &Lazy, path: &str) -> Result<Option<Vec<u8>>> {
    let stop = CancellationToken::new();
    let mut work = WorkBudget::UNBOUNDED.accounting(&stop);
    Ok(view
        .read(path, 64, &mut work)
        .await
        .ok()
        .map(|read| read.bytes.as_ref().to_vec()))
}

/// A stash cut at every effect boundary, followed by a workspace write
/// before the next command, still completes: the retried restore replays
/// the transaction its operation committed instead of building another one
/// from the moved head under the same identity.
#[tokio::test]
async fn a_stash_cut_at_every_step_and_written_after_still_completes() -> Result<()> {
    let mut cut = 0;
    loop {
        cut += 1;
        let (_temp, _root, fs, store, main) = fixture().await?;
        write_text(&main, "/base.txt", b"base\n").await?;
        let repo = main.workspace().id();
        run(&fs, &store, &main, repo, &["add", "base.txt"]).await?;
        run(&fs, &store, &main, repo, &["commit", "-m", "base"]).await?;
        write_text(&main, "/base.txt", b"changed\n").await?;
        write_text(&main, "/nested/new.txt", b"new\n").await?;

        let calls = std::sync::atomic::AtomicUsize::new(0);
        let interrupted = git::run_with_guard(
            git::Workspace {
                access: &acyclic_harness_core::filesystem::local_owner_workspace_access("test")?,
                filesystem: &fs,
                store: &store,
                view: (&main).into(),
                root: std::path::Path::new("."),
                repository: repo,
                lease: None,
                checkout: None,
                author: "test",
                authored_at_seconds: 1,
                request: None,
            },
            &["stash".to_owned()],
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
        // The workspace moves on before the next command recovers the stash.
        write_text(&main, "/later.txt", b"later\n").await?;
        let mut answered = false;
        for _ in 0..2 {
            let output = git::run(
                git::Workspace {
                    access: &acyclic_harness_core::filesystem::local_owner_workspace_access(
                        "test",
                    )?,
                    filesystem: &fs,
                    store: &store,
                    view: (&main).into(),
                    root: std::path::Path::new("."),
                    repository: repo,
                    lease: None,
                    checkout: None,
                    author: "test",
                    authored_at_seconds: 1,
                    request: None,
                },
                &["status".to_owned()],
            )
            .await?;
            if output.command.is_some() {
                answered = true;
                break;
            }
        }
        assert!(answered, "status never answered after cut {cut}");
        if read_text(&main, "/base.txt").await?.as_deref() == Some(b"changed\n".as_slice()) {
            // Cut before the transition was recorded: nothing to resume, so
            // the stash is simply run again.
            run(&fs, &store, &main, repo, &["stash"]).await?;
        }
        assert_eq!(
            read_text(&main, "/base.txt").await?.as_deref(),
            Some(b"base\n".as_slice()),
            "cut at effect {cut}"
        );
        assert_eq!(
            read_text(&main, "/nested/new.txt").await?,
            None,
            "cut at effect {cut}"
        );
        if completed {
            assert!(cut > 1, "the stash has effect boundaries to cut");
            return Ok(());
        }
    }
}

/// The host path every tree-applying entry point below must leave in place.
#[tokio::test]
async fn raw_git_requires_the_grant_and_rejects_an_unrelated_registered_workspace() -> Result<()> {
    use acyclic_harness_core::{
        Capabilities,
        core::{AggregateKind, Authority, AuthorityRoot},
        filesystem::WorkspaceAccess,
    };
    let (_temp, _root, fs, store, main) = fixture().await?;
    let graph = acyclic_fs::WorkspaceGraph::new(store.clone());
    graph
        .register_root(
            main.workspace(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    let root = AuthorityRoot::new(
        "git-access",
        [91; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "git-access".into(),
        },
    );
    let mut narrow =
        acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL);
    narrow
        .layers
        .first_mut()
        .and_then(|layer| layer.first_mut())
        .ok_or("whole-volume fixture has no grant")?
        .resource
        .path = "public/**".into();
    let grants = Capabilities::new([narrow.to_scope_label()?]);
    let scope = root.bootstrap_scope("path-only", &grants, None, &mut None)?;
    let access = WorkspaceAccess::new(&root.verifier(&mut None)?, &scope, &grants, None)?;
    let denied = git::run(
        git::Workspace {
            access: &access,
            filesystem: &fs,
            store: &store,
            view: (&main).into(),
            root: std::path::Path::new("."),
            repository: main.workspace().id(),
            lease: None,
            checkout: None,
            author: "test",
            authored_at_seconds: 1,
            request: None,
        },
        &["status".into()],
    )
    .await;
    assert!(
        matches!(denied, Err(Error::FilesystemAccessConfiguration(_))),
        "Git exposed a workspace outside its path grant"
    );
    // Replacing the narrow workload with a host grant restores the old unchecked path.
    // The same command succeeds, so the denial above is an authority observation.
    run(&fs, &store, &main, main.workspace().id(), &["status"]).await?;

    let foreign = acyclic_harness_core::filesystem::local_owner_workspace(&fs, "foreign").await?;
    graph
        .register_root(
            &foreign,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    acyclic_fs::GitCompatRepository::new(main.workspace().id(), store.clone())
        .register_branch_workspace("foreign", foreign.id(), None, false)
        .await?;
    // The old workspace resolver accepts this real, registered workspace.
    let unchecked =
        acyclic_fs::WorkspaceResolver::resolve(&(fs.clone(), graph), foreign.id()).await?;
    assert_eq!(unchecked.id(), foreign.id());
    let denied = run(
        &fs,
        &store,
        &main,
        main.workspace().id(),
        &["checkout", "foreign"],
    )
    .await;
    assert!(
        denied
            .as_ref()
            .err()
            .is_some_and(|error| error.to_string().contains("outside its repository lineage")),
        "Git followed an unrelated workspace"
    );
    Ok(())
}

#[tokio::test]
async fn recovery_authenticates_pending_sources_outside_the_branch_catalog() -> Result<()> {
    use acyclic_fs::{
        GitCompatState, GitCompatStore, GitFilesystemAction, GitPendingMutation,
        GitPendingTransition, GitTransitionId, WorkspaceId,
    };
    let (_temp, _root, fs, store, main) = fixture().await?;
    let root = main.workspace().id();
    let store = &store;
    let mut state = GitCompatStore::load(store, root)
        .await?
        .unwrap_or_else(|| GitCompatState::new("main", root));
    let prior = state.revision;
    state.revision += 1;
    state.pending = Some(GitPendingTransition {
        id: GitTransitionId::new(),
        request: None,
        action: GitFilesystemAction::SwitchWorkspace { workspace_id: root },
        mutation: GitPendingMutation::NoOp,
    });
    assert!(GitCompatStore::compare_and_swap(store, root, prior, state.clone()).await?);
    let recovered = git::run(
        git::Workspace {
            access: &acyclic_harness_core::filesystem::local_owner_workspace_access("test")?,
            filesystem: &fs,
            store,
            view: (&main).into(),
            root: std::path::Path::new("."),
            repository: root,
            lease: None,
            checkout: None,
            author: "test",
            authored_at_seconds: 1,
            request: None,
        },
        &["status".into()],
    )
    .await?;
    assert!(recovered.command.is_none());

    state = GitCompatStore::load(store, root)
        .await?
        .ok_or("missing recovered state")?;
    state.pending = Some(GitPendingTransition {
        id: GitTransitionId::new(),
        request: None,
        action: GitFilesystemAction::SwitchWorkspace { workspace_id: root },
        mutation: GitPendingMutation::NoOp,
    });
    // A persisted intent can refer to a source absent from every branch and
    // commit. Recovery must authenticate that source before executing anything.
    let unknown = WorkspaceId::from_bytes([247; 16]);
    assert!(
        !state
            .branches
            .values()
            .any(|branch| branch.workspace_id == unknown)
    );
    let prior = state.revision;
    state.revision += 1;
    state
        .pending
        .as_mut()
        .ok_or_else(|| Error::Invalid("pending fixture is absent".into()))?
        .action = GitFilesystemAction::SwitchWorkspace {
        workspace_id: unknown,
    };
    assert!(GitCompatStore::compare_and_swap(store, root, prior, state.clone()).await?);
    assert!(run(&fs, store, &main, root, &["status"]).await.is_err());

    // Negative control: omitting the pending source restores the same admission
    // the former catalog-only check would have granted.
    let prior = state.revision;
    state.revision += 1;
    state.pending = None;
    assert!(GitCompatStore::compare_and_swap(store, root, prior, state).await?);
    run(&fs, store, &main, root, &["status"]).await?;
    Ok(())
}

#[tokio::test]
async fn nested_repository_reads_and_diffs_its_own_child_without_merging() -> Result<()> {
    use acyclic_fs::GitFilesystemResult;
    let (_temp, _root, fs, store, root) = fixture().await?;
    let (_, parent) = run(
        &fs,
        &store,
        &root,
        root.workspace().id(),
        &["checkout", "-b", "agents/parent"],
    )
    .await?;
    let parent = parent.ok_or("missing parent fork")?;
    let repository = parent.workspace().id();
    let (_, reviewer) = run(
        &fs,
        &store,
        &parent,
        repository,
        &["checkout", "-b", "agents/reviewer"],
    )
    .await?;
    let reviewer = reviewer.ok_or("missing reviewer fork")?;
    write_text(&reviewer, "/review.md", b"Reviewed changes\n").await?;
    // Branch creation selects the child; return this repository to its parent.
    run(&fs, &store, &reviewer, repository, &["checkout", "main"]).await?;
    let (shown, _) = run(
        &fs,
        &store,
        &parent,
        repository,
        &["show", "agents/reviewer:review.md"],
    )
    .await?;
    let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = shown else {
        return Err("expected reviewer file".into());
    };
    assert_eq!(value["text"], "Reviewed changes\n");
    let (diff, _) = run(
        &fs,
        &store,
        &parent,
        repository,
        &["diff", "agents/reviewer", "--stat"],
    )
    .await?;
    let GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) = diff else {
        return Err("expected nested child diff".into());
    };
    assert_eq!(value["fileChanges"], 1);
    assert_eq!(read_text(&parent, "/review.md").await?, None);
    assert_eq!(read_text(&root, "/review.md").await?, None);
    Ok(())
}
