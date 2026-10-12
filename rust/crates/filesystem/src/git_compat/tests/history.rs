use super::*;

#[tokio::test]
async fn auto_staging_commit_and_machine_readable_history() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    let GitCommandOutput::Status(status) = repository.execute(GitCommand::Status, tree(1)).await.expect("status")
    else {
        panic!("expected status");
    };
    assert_eq!(status.dirty, GitDirtyState::Dirty);
    assert!(status.all_changes_staged);
    let GitCommandOutput::Prepared { transition, action: GitFilesystemAction::CaptureCommit { .. } } = repository
        .execute(
            GitCommand::Commit { message: "initial".to_owned(), author: "agent".to_owned(), authored_at_seconds: 10 },
            tree(1),
        )
        .await
        .expect("capture commit")
    else {
        panic!("expected commit capture");
    };
    let GitCommandOutput::Committed(commit) = repository
        .complete_transition_result(
            transition,
            &GitFilesystemResult::Captured {
                tree: tree(1),
                tracked_paths: BTreeSet::from(["tracked.txt".to_owned()]),
                proof: None,
            },
        )
        .await
        .expect("record commit")
    else {
        panic!("expected commit");
    };
    let GitCommandOutput::Commits(log) =
        repository.execute(GitCommand::Log { maximum: 10 }, tree(1)).await.expect("log")
    else {
        panic!("expected log");
    };
    assert_eq!(log, vec![commit]);
}

#[tokio::test]
async fn composed_run_finalizes_commit_and_branch_actions() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    let commit_executor = TestExecutor::returning(GitFilesystemResult::Captured {
        tree: tree(9),
        tracked_paths: BTreeSet::from(["tracked.txt".to_owned()]),
        proof: None,
    });
    let GitCommandOutput::Committed(commit) = repository
        .run(
            GitCommand::Commit { message: "initial".to_owned(), author: "agent".to_owned(), authored_at_seconds: 10 },
            tree(1),
            &commit_executor,
        )
        .await
        .expect("composed commit")
    else {
        panic!("expected composed commit");
    };
    assert_eq!(commit.tree, tree(9));
    assert_eq!(commit.workspace_tree, tree(1));
    assert!(matches!(
        repository.execute(GitCommand::Status, tree(1)).await.expect("status"),
        GitCommandOutput::Status(GitStatus { dirty: GitDirtyState::Clean, .. })
    ));
    let child = WorkspaceId::derive(
        [9; 16],
        &WorkspaceName::new("feature-run", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("valid workspace name")
            .value,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .expect("workspace identity")
    .value;
    let branch_executor = TestExecutor::returning(GitFilesystemResult::Forked { workspace_id: child });
    repository
        .run(GitCommand::Branch { create: Some("feature-run".to_owned()) }, tree(1), &branch_executor)
        .await
        .expect("composed branch");
    assert!(matches!(
        repository
            .execute(GitCommand::Branch { create: None }, tree(1))
            .await
            .expect("branches"),
        GitCommandOutput::Branches { branches, .. }
            if branches.iter().any(|branch| branch.workspace_id == child)
    ));
}

#[tokio::test]
async fn composed_run_leaves_failed_transition_for_exact_resume() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    let commit_executor = TestExecutor::returning(GitFilesystemResult::Captured {
        tree: tree(1),
        tracked_paths: BTreeSet::new(),
        proof: None,
    });
    repository
        .run(
            GitCommand::Commit { message: "initial".to_owned(), author: "agent".to_owned(), authored_at_seconds: 10 },
            tree(1),
            &commit_executor,
        )
        .await
        .expect("initial commit");
    let child = WorkspaceId::derive(
        [9; 16],
        &WorkspaceName::new("recover-run", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("valid workspace name")
            .value,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .expect("workspace identity")
    .value;
    repository
        .run(
            GitCommand::Branch { create: Some("recover-run".to_owned()) },
            tree(1),
            &TestExecutor::returning(GitFilesystemResult::Forked { workspace_id: child }),
        )
        .await
        .expect("branch");
    let failed = repository
        .run(GitCommand::Switch { branch: "recover-run".to_owned(), create: false }, tree(2), &TestExecutor::failing())
        .await;
    assert!(matches!(failed, Err(GitCompatRunError::Executor(_))));
    let pending =
        repository.pending_transition().await.expect("pending transition").expect("durable pending transition");
    let executor = TestExecutor::returning(GitFilesystemResult::Applied { tree: Some(tree(1)), tracked_paths: None });
    assert!(repository.resume(&executor).await.expect("resume").is_some());
    assert!(repository.pending_transition().await.expect("pending cleared").is_none());
    assert_eq!(executor.operations.lock().expect("operation lock").as_slice(), &[pending.id.operation_id()]);
}

#[tokio::test]
async fn conflicted_merge_can_continue_or_abort_without_a_second_sequencer() {
    async fn pending_merge(repository: &GitCompatRepository<MemoryGitCompatStore>, child: WorkspaceId) {
        repository
            .run(
                GitCommand::Branch { create: Some("feature".to_owned()) },
                tree(1),
                &TestExecutor::returning(GitFilesystemResult::Forked { workspace_id: child }),
            )
            .await
            .expect("feature branch");
        assert!(matches!(
            repository
                .run(GitCommand::Merge { branch: "feature".to_owned() }, tree(2), &TestExecutor::failing(),)
                .await,
            Err(GitCompatRunError::Executor(_))
        ));
    }

    let child = WorkspaceId::derive(
        [9; 16],
        &WorkspaceName::new("merge-control", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("valid workspace name")
            .value,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .expect("workspace identity")
    .value;
    let continued = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    pending_merge(&continued, child).await;
    assert_eq!(
        continued
            .run(
                GitCommand::Add { paths: vec!["conflicted.txt".to_owned()] },
                tree(3),
                &TestExecutor::returning(GitFilesystemResult::Applied { tree: None, tracked_paths: None }),
            )
            .await
            .expect("stage conflict resolution"),
        GitCommandOutput::NoOp
    );
    assert!(continued.pending_transition().await.expect("merge remains pending after add").is_some());
    assert!(matches!(
        continued
            .run(
                GitCommand::MergeContinue,
                tree(3),
                &TestExecutor::returning(GitFilesystemResult::Applied {
                    tree: None,
                    tracked_paths: None,
                }),
            )
            .await
            .expect("continue merge"),
        GitCommandOutput::Committed(GitCommit { tree: GitTreeRef::Exact(tree), .. })
            if tree.generation == generation(3)
    ));
    assert!(continued.pending_transition().await.expect("continued state").is_none());

    let aborted = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    pending_merge(&aborted, child).await;
    let executor = TestExecutor::returning(GitFilesystemResult::Applied { tree: Some(tree(2)), tracked_paths: None });
    assert!(matches!(
        aborted.run(GitCommand::MergeAbort, tree(4), &executor).await.expect("abort merge"),
        GitCommandOutput::Filesystem(GitFilesystemResult::Applied { .. })
    ));
    assert!(aborted.pending_transition().await.expect("aborted state").is_none());
}

#[tokio::test]
async fn filtered_commit_completes_and_clears_its_transition() {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("git-filtered-commit", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("workspace");
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::from_bytes([0x6b; 16]), WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("transaction");
    transaction.write_text("/keep.txt", "keep").await.expect("kept");
    transaction.write_text("/drop.tmp", "drop").await.expect("ignored");
    publish_fixture(transaction).await;
    let live = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("live");
    let repository = GitCompatRepository::new(workspace.id(), MemoryGitCompatStore::new());
    let GitCommandOutput::Prepared { transition, action: GitFilesystemAction::CaptureCommit { .. } } = repository
        .execute(
            GitCommand::Commit { message: "filtered".to_owned(), author: "agent".to_owned(), authored_at_seconds: 1 },
            GitTreeRef::exact(workspace.id(), live.id()),
        )
        .await
        .expect("prepare")
    else {
        panic!("expected capture transition");
    };
    let captured = capture_git_compatible_generation(
        &workspace,
        &GitIgnorePolicy::parse("*.tmp\n"),
        &BTreeSet::new(),
        transition.operation_id(),
    )
    .await
    .expect("capture");
    let proof = captured.verified_proof(transition.operation_id()).await.expect("verified proof");
    let result = GitFilesystemResult::Captured {
        tree: GitTreeRef::exact(captured.generation.workspace().id(), captured.generation.id()),
        tracked_paths: captured.tracked_paths,
        proof,
    };
    let GitCommandOutput::Committed(commit) =
        repository.complete_transition_result(transition, &result).await.expect("complete")
    else {
        panic!("expected commit");
    };
    assert!(repository.pending_transition().await.expect("pending").is_none());
    assert!(commit.tracked_paths.contains("keep.txt"));
    assert!(!commit.tracked_paths.contains("drop.tmp"));

    let mut ignored_only = workspace
        .begin_transaction(IdempotencyKey::from_bytes([0x6c; 16]), WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("transaction");
    ignored_only.write_text("/new.tmp", "not committed").await.expect("ignored file");
    publish_fixture(ignored_only).await;
    let live_after = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("live after ignored change");
    let GitCommandOutput::Prepared { transition, .. } = repository
        .execute(
            GitCommand::Commit {
                message: "ignored only".to_owned(),
                author: "agent".to_owned(),
                authored_at_seconds: 2,
            },
            GitTreeRef::exact(workspace.id(), live_after.id()),
        )
        .await
        .expect("prepare ignored-only commit")
    else {
        panic!("expected prepared transition");
    };
    let ignored_capture = capture_git_compatible_generation_incremental(
        &workspace,
        &live,
        &captured.generation,
        &GitIgnorePolicy::parse("*.tmp\n"),
        &commit.tracked_paths,
        transition.operation_id(),
    )
    .await
    .expect("ignored-only capture");
    assert_eq!(ignored_capture.generation.id(), captured.generation.id());
    assert!(matches!(
        repository
            .complete_transition_result(
                transition,
                &GitFilesystemResult::Captured {
                    tree: GitTreeRef::exact(
                        ignored_capture.generation.workspace_id(),
                        ignored_capture.generation.id(),
                    ),
                    tracked_paths: ignored_capture.tracked_paths,
                    proof: None,
                },
            )
            .await,
        Err(GitCompatError::NothingToCommit)
    ));
    assert!(repository.pending_transition().await.expect("pending").is_none());
}

#[tokio::test]
async fn bisect_uses_durable_first_parent_generation_checkouts() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    let mut commits = Vec::new();
    let mut expected = None;
    for byte in 1..=5 {
        let GitCommandOutput::Committed(commit) = repository
            .record_commit(
                expected,
                generation(byte),
                BTreeSet::from(["tracked.txt".to_owned()]),
                format!("commit {byte}"),
                "agent".to_owned(),
                i64::from(byte),
            )
            .await
            .expect("record commit")
        else {
            panic!("expected recorded commit");
        };
        expected = Some(commit.id);
        commits.push(commit.id);
    }

    let GitCommandOutput::Bisect(started) = repository
        .run(
            GitCommand::Bisect { arguments: vec!["start".to_owned(), commits[4].to_hex(), commits[0].to_hex()] },
            tree(5),
            &TestExecutor::returning(GitFilesystemResult::Applied { tree: Some(tree(3)), tracked_paths: None }),
        )
        .await
        .expect("start bisect")
    else {
        panic!("expected bisect state");
    };
    assert_eq!(started.current, Some(commits[2]));
    assert_eq!(started.remaining, 3);

    let GitCommandOutput::Bisect(narrowed) = repository
        .run(
            GitCommand::Bisect { arguments: vec!["good".to_owned()] },
            tree(3),
            &TestExecutor::returning(GitFilesystemResult::Applied { tree: Some(tree(4)), tracked_paths: None }),
        )
        .await
        .expect("mark good")
    else {
        panic!("expected narrowed bisect state");
    };
    assert_eq!(narrowed.current, Some(commits[3]));

    let GitCommandOutput::Bisect(done) = repository
        .run(
            GitCommand::Bisect { arguments: vec!["bad".to_owned()] },
            tree(4),
            &TestExecutor::returning(GitFilesystemResult::Applied { tree: Some(tree(4)), tracked_paths: None }),
        )
        .await
        .expect("mark bad")
    else {
        panic!("expected completed bisect state");
    };
    assert_eq!(done.first_bad, Some(commits[3]));
    assert_eq!(done.remaining, 0);

    let GitCommandOutput::Bisect(reset) = repository
        .run(
            GitCommand::Bisect { arguments: vec!["reset".to_owned()] },
            tree(4),
            &TestExecutor::returning(GitFilesystemResult::Applied { tree: Some(tree(5)), tracked_paths: None }),
        )
        .await
        .expect("reset bisect")
    else {
        panic!("expected reset bisect state");
    };
    assert!(!reset.active);
}

#[tokio::test]
async fn distributed_publication_records_one_idempotent_merge_commit() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    let GitCommandOutput::Committed(initial) = repository
        .record_commit(None, generation(1), BTreeSet::from(["tracked.txt".to_owned()]), "initial", "parent", 1)
        .await
        .expect("initial commit")
    else {
        panic!("expected initial commit");
    };
    let source = WorkspaceId::derive(
        [0x51; 16],
        &WorkspaceName::new("publication-source", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("workspace name")
            .value,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .expect("workspace identity")
    .value;
    repository.register_branch_workspace("agents/child", source, Some(initial.id), false).await.expect("source branch");
    assert!(matches!(
        repository
            .record_publication(GitPublicationRecord {
                tree: tree(1),
                workspace_tree: GitTreeRef::exact(source, generation(1)),
                source_head: None,
                tracked_paths: BTreeSet::new(),
                capture_proof: None,
                message: "foreign no-op".to_owned(),
                author: "parent".to_owned(),
                authored_at_seconds: 2,
            })
            .await,
        Err(GitCompatError::WorkspaceMismatch)
    ));
    let tracked = BTreeSet::from(["tracked.txt".to_owned(), "new.txt".to_owned()]);
    let GitCommandOutput::Committed(merged) = repository
        .record_publication(GitPublicationRecord {
            tree: tree(2),
            workspace_tree: tree(3),
            source_head: Some(initial.id),
            tracked_paths: tracked.clone(),
            capture_proof: None,
            message: "Merge agents/child".to_owned(),
            author: "parent".to_owned(),
            authored_at_seconds: 2,
        })
        .await
        .expect("publication commit")
    else {
        panic!("expected publication commit");
    };
    assert_eq!(merged.parents, vec![initial.id]);
    assert_eq!(merged.tracked_paths, tracked);
    assert_eq!(repository.tracked_paths().await.expect("tracked paths"), tracked);
    assert_eq!(
        repository
            .record_publication(GitPublicationRecord {
                tree: tree(2),
                workspace_tree: tree(3),
                source_head: Some(initial.id),
                tracked_paths: BTreeSet::new(),
                capture_proof: None,
                message: "retry".to_owned(),
                author: "parent".to_owned(),
                authored_at_seconds: 99,
            })
            .await
            .expect("exact retry"),
        GitCommandOutput::NoOp
    );
}

#[tokio::test]
async fn cached_diff_is_head_to_workspace_and_add_is_noop() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    let GitCommandOutput::Prepared { transition, action: GitFilesystemAction::CaptureCommit { .. } } = repository
        .execute(
            GitCommand::Commit { message: "initial".to_owned(), author: "agent".to_owned(), authored_at_seconds: 10 },
            tree(1),
        )
        .await
        .expect("capture commit")
    else {
        panic!("expected commit capture");
    };
    repository
        .complete_transition_result(
            transition,
            &GitFilesystemResult::Captured { tree: tree(1), tracked_paths: BTreeSet::new(), proof: None },
        )
        .await
        .expect("record commit");
    assert_eq!(
        repository.execute(GitCommand::Add { paths: vec![".".to_owned()] }, tree(2),).await.expect("add"),
        GitCommandOutput::NoOp
    );
    assert!(matches!(
        repository
            .execute(GitCommand::Diff { cached: true }, tree(2))
            .await
            .expect("diff"),
        GitCommandOutput::Prepared {
            action: GitFilesystemAction::Diff {
                from: Some(from),
                to,
                tracked_paths,
            },
            ..
        } if from == tree(1)
            && to == tree(2)
            && tracked_paths.is_empty()
    ));
}

#[tokio::test]
async fn every_branch_requests_and_registers_a_distinct_workspace() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    let GitCommandOutput::Prepared {
        transition,
        action: GitFilesystemAction::ForkBranch { source_tree, head, switch, .. },
    } = repository
        .execute(GitCommand::Switch { branch: "feature".to_owned(), create: true }, tree(4))
        .await
        .expect("fork request")
    else {
        panic!("expected a core workspace fork request");
    };
    assert_eq!(source_tree, tree(4));
    assert!(head.is_none());
    assert!(switch);
    let child = WorkspaceId::derive(
        [9; 16],
        &WorkspaceName::new("feature", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("valid workspace name")
            .value,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .expect("workspace identity")
    .value;
    repository
        .complete_transition_result(transition, &GitFilesystemResult::Forked { workspace_id: child })
        .await
        .expect("register branch workspace");
    let GitCommandOutput::Status(status) =
        repository.execute(GitCommand::Status, GitTreeRef::exact(child, generation(4))).await.expect("feature status")
    else {
        panic!("expected status");
    };
    assert_eq!(status.branch, "feature");
}

#[tokio::test]
async fn state_changing_filesystem_actions_are_prepared_then_completed() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    let GitCommandOutput::Prepared { transition, action: GitFilesystemAction::CaptureCommit { .. } } = repository
        .execute(
            GitCommand::Commit { message: "initial".to_owned(), author: "agent".to_owned(), authored_at_seconds: 10 },
            tree(1),
        )
        .await
        .expect("prepare commit")
    else {
        panic!("expected commit capture");
    };
    let GitCommandOutput::Committed(commit) = repository
        .complete_transition_result(
            transition,
            &GitFilesystemResult::Captured { tree: tree(1), tracked_paths: BTreeSet::new(), proof: None },
        )
        .await
        .expect("record commit")
    else {
        panic!("expected commit");
    };
    repository
        .register_branch_workspace("feature", workspace(), Some(commit.id), false)
        .await
        .expect("register branch");
    let GitCommandOutput::Prepared { transition, .. } = repository
        .execute(GitCommand::Switch { branch: "feature".to_owned(), create: false }, tree(2))
        .await
        .expect("prepare switch")
    else {
        panic!("expected prepared switch");
    };
    assert!(matches!(
        repository
            .execute(GitCommand::Status, tree(2))
            .await,
        Err(GitCompatError::TransitionPending(id)) if id == transition
    ));
    repository
        .complete_transition_result(transition, &GitFilesystemResult::Applied { tree: None, tracked_paths: None })
        .await
        .expect("complete switch");
    let GitCommandOutput::Status(status) = repository.execute(GitCommand::Status, tree(1)).await.expect("status")
    else {
        panic!("expected status");
    };
    assert_eq!(status.branch, "feature");
}
