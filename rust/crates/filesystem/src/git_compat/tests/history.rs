use super::*;

#[tokio::test]
async fn auto_staging_commit_and_machine_readable_history() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let GitCommandOutput::Status(status) = repository
        .execute(GitCommand::Status, tree(1))
        .await
        .expect("status")
    else {
        panic!("expected status");
    };
    assert_eq!(status.dirty, GitDirtyState::Dirty);
    assert!(status.all_changes_staged);
    let GitCommandOutput::Prepared {
        transition,
        action: GitFilesystemAction::CaptureCommit { .. },
    } = repository
        .execute(
            GitCommand::Commit {
                message: "initial".to_owned(),
                author: "agent".to_owned(),
                authored_at_seconds: 10,
            },
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
    let GitCommandOutput::Commits(log) = repository
        .execute(GitCommand::Log { maximum: 10 }, tree(1))
        .await
        .expect("log")
    else {
        panic!("expected log");
    };
    assert_eq!(log, vec![commit]);
}

#[tokio::test]
async fn composed_run_finalizes_commit_and_branch_actions() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let commit_executor = TestExecutor::returning(GitFilesystemResult::Captured {
        tree: tree(9),
        tracked_paths: BTreeSet::from(["tracked.txt".to_owned()]),
        proof: None,
    });
    let GitCommandOutput::Committed(commit) = repository
        .run(
            GitCommand::Commit {
                message: "initial".to_owned(),
                author: "agent".to_owned(),
                authored_at_seconds: 10,
            },
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
        repository
            .execute(GitCommand::Status, tree(1))
            .await
            .expect("status"),
        GitCommandOutput::Status(GitStatus {
            dirty: GitDirtyState::Clean,
            ..
        })
    ));
    let child = workspace_identity([9; 16], "feature-run");
    let branch_executor = TestExecutor::returning(GitFilesystemResult::Forked {
        workspace_id: child,
    });
    repository
        .run(
            GitCommand::Branch {
                create: Some("feature-run".to_owned()),
            },
            tree(1),
            &branch_executor,
        )
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
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let commit_executor = TestExecutor::returning(GitFilesystemResult::Captured {
        tree: tree(1),
        tracked_paths: BTreeSet::new(),
        proof: None,
    });
    repository
        .run(
            GitCommand::Commit {
                message: "initial".to_owned(),
                author: "agent".to_owned(),
                authored_at_seconds: 10,
            },
            tree(1),
            &commit_executor,
        )
        .await
        .expect("initial commit");
    let child = workspace_identity([9; 16], "recover-run");
    repository
        .run(
            GitCommand::Branch {
                create: Some("recover-run".to_owned()),
            },
            tree(1),
            &TestExecutor::returning(GitFilesystemResult::Forked {
                workspace_id: child,
            }),
        )
        .await
        .expect("branch");
    let failed = repository
        .run(
            GitCommand::Switch {
                branch: "recover-run".to_owned(),
                create: false,
            },
            tree(2),
            &TestExecutor::failing(),
        )
        .await;
    assert!(matches!(failed, Err(GitCompatRunError::Executor(_))));
    let pending = repository
        .pending_transition()
        .await
        .expect("pending transition")
        .expect("durable pending transition");
    let executor = TestExecutor::returning(GitFilesystemResult::Applied {
        tree: Some(GitTreeRef::exact(child, generation(1))),
        tracked_paths: None,
    });
    assert!(
        repository
            .resume(&executor)
            .await
            .expect("resume")
            .is_some()
    );
    assert!(
        repository
            .pending_transition()
            .await
            .expect("pending cleared")
            .is_none()
    );
    assert_eq!(
        executor
            .operations
            .lock()
            .expect("operation lock")
            .as_slice(),
        &[pending.id.operation_id()]
    );
}

#[tokio::test]
async fn conflicted_merge_can_continue_or_abort_without_a_second_sequencer() {
    async fn pending_merge(
        repository: &GitCompatRepository<MemoryGitCompatStore>,
        child: WorkspaceId,
    ) {
        repository
            .run(
                GitCommand::Branch {
                    create: Some("feature".to_owned()),
                },
                tree(1),
                &TestExecutor::returning(GitFilesystemResult::Forked {
                    workspace_id: child,
                }),
            )
            .await
            .expect("feature branch");
        assert!(matches!(
            repository
                .run(
                    GitCommand::Merge {
                        branch: "feature".to_owned()
                    },
                    tree(2),
                    &TestExecutor::failing(),
                )
                .await,
            Err(GitCompatRunError::Executor(_))
        ));
    }

    let child = workspace_identity([9; 16], "merge-control");
    let continued = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    pending_merge(&continued, child).await;
    assert_eq!(
        continued
            .run(
                GitCommand::Add {
                    paths: vec!["conflicted.txt".to_owned()]
                },
                tree(3),
                &TestExecutor::returning(GitFilesystemResult::Applied {
                    tree: None,
                    tracked_paths: None
                }),
            )
            .await
            .expect("stage conflict resolution"),
        GitCommandOutput::NoOp
    );
    assert!(
        continued
            .pending_transition()
            .await
            .expect("merge remains pending after add")
            .is_some()
    );
    assert!(matches!(
        continued
            .run(GitCommand::MergeContinue, tree(3), &TestExecutor::failing())
            .await,
        Err(GitCompatRunError::Executor(_))
    ));
    assert!(
        continued
            .pending_transition()
            .await
            .expect("pending after rejected resolution")
            .is_some()
    );
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
    assert!(
        continued
            .pending_transition()
            .await
            .expect("continued state")
            .is_none()
    );

    let aborted = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    pending_merge(&aborted, child).await;
    let executor = TestExecutor::returning(GitFilesystemResult::Applied {
        tree: Some(tree(2)),
        tracked_paths: None,
    });
    assert!(matches!(
        aborted
            .run(GitCommand::MergeAbort, tree(4), &executor)
            .await
            .expect("abort merge"),
        GitCommandOutput::Filesystem(GitFilesystemResult::Applied { .. })
    ));
    assert!(
        aborted
            .pending_transition()
            .await
            .expect("aborted state")
            .is_none()
    );
}

#[tokio::test]
async fn discard_retires_only_an_idle_non_current_branch() {
    fn names(output: &GitCommandOutput) -> Vec<&str> {
        let GitCommandOutput::Branches { branches, .. } = output else {
            panic!("discard lists the remaining branches: {output:?}");
        };
        branches.iter().map(|branch| branch.name.as_str()).collect()
    }
    fn argv(command: &str, branch: &str) -> Vec<String> {
        vec![command.to_owned(), branch.to_owned()]
    }
    let child = workspace_identity([9; 16], "discard");
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    repository
        .run(
            GitCommand::Branch {
                create: Some("agents/kid".to_owned()),
            },
            tree(1),
            &TestExecutor::returning(GitFilesystemResult::Forked {
                workspace_id: child,
            }),
        )
        .await
        .expect("child branch");
    // The checked-out branch is the destination of every merge; dropping it
    // would leave HEAD without a branch.
    assert!(matches!(
        repository
            .run_argv(&argv("discard", "main"), tree(1), "agent", 0, &TestExecutor::failing())
            .await,
        Err(GitCompatRunError::Compat(GitCompatError::InvalidCommand(message)))
            if message.contains("checked-out")
    ));
    assert!(matches!(
        repository
            .run_argv(
                &argv("discard", "agents/none"),
                tree(1),
                "agent",
                0,
                &TestExecutor::failing()
            )
            .await,
        Err(GitCompatRunError::Compat(GitCompatError::UnknownObject(_)))
    ));
    assert!(matches!(
        repository
            .execute_argv(&["discard".to_owned()], tree(1), "agent", 0)
            .await,
        Err(GitCompatError::InvalidCommand(_))
    ));

    // A conflicted merge of the branch is still pending: its continuation
    // needs the source, so the catalog is frozen until it ends.
    assert!(matches!(
        repository
            .run(
                GitCommand::Merge {
                    branch: "agents/kid".to_owned()
                },
                tree(2),
                &TestExecutor::failing(),
            )
            .await,
        Err(GitCompatRunError::Executor(_))
    ));
    assert!(matches!(
        repository
            .run(
                GitCommand::Discard {
                    branch: "agents/kid".to_owned()
                },
                tree(2),
                &TestExecutor::failing(),
            )
            .await,
        Err(GitCompatRunError::Compat(
            GitCompatError::TransitionPending(_)
        ))
    ));
    repository
        .run(
            GitCommand::MergeAbort,
            tree(3),
            &TestExecutor::returning(GitFilesystemResult::Applied {
                tree: Some(tree(1)),
                tracked_paths: None,
            }),
        )
        .await
        .expect("abort merge");

    // The filesystem is never asked to act: only the catalog entry goes.
    let discarded = repository
        .run_argv(
            &argv("discard", "agents/kid"),
            tree(1),
            "agent",
            0,
            &TestExecutor::failing(),
        )
        .await
        .expect("discard child branch");
    assert_eq!(names(&discarded), ["main"]);
    for command in ["merge", "rebase", "discard"] {
        assert!(
            matches!(
                repository
                    .run_argv(
                        &argv(command, "agents/kid"),
                        tree(1),
                        "agent",
                        0,
                        &TestExecutor::failing()
                    )
                    .await,
                Err(GitCompatRunError::Compat(GitCompatError::UnknownObject(_)))
            ),
            "{command} still reaches a discarded branch"
        );
    }
    let listed = repository
        .run(
            GitCommand::Branch { create: None },
            tree(1),
            &TestExecutor::failing(),
        )
        .await
        .expect("branch catalog");
    assert_eq!(names(&listed), ["main"]);
    assert!(
        repository
            .pending_transition()
            .await
            .expect("state")
            .is_none()
    );
}

#[tokio::test]
async fn filtered_commit_completes_and_clears_its_transition() {
    let workspace = memory_workspace("git-filtered-commit").await;
    let mut transaction = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x6b; 16])).await;
    transaction
        .write_text("/keep.txt", "keep")
        .await
        .expect("kept");
    transaction
        .write_text("/drop.tmp", "drop")
        .await
        .expect("ignored");
    publish_fixture(transaction).await;
    let live = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("live")
        .value;
    let repository = GitCompatRepository::new(workspace.id(), MemoryGitCompatStore::default());
    let GitCommandOutput::Prepared {
        transition,
        action: GitFilesystemAction::CaptureCommit { .. },
    } = repository
        .execute(
            GitCommand::Commit {
                message: "filtered".to_owned(),
                author: "agent".to_owned(),
                authored_at_seconds: 1,
            },
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
    let proof = captured
        .authenticated_proof(
            transition.operation_id(),
            &crate::WorkspaceGraph::new(crate::MemoryWorkspaceLineageStore::new()),
        )
        .await
        .expect("verified proof");
    let result = GitFilesystemResult::Captured {
        tree: GitTreeRef::exact(
            captured.generation.workspace().id(),
            captured.generation.id(),
        ),
        tracked_paths: captured.tracked_paths,
        proof,
    };
    let GitCommandOutput::Committed(commit) = repository
        .complete_transition_result(transition, &result)
        .await
        .expect("complete")
    else {
        panic!("expected commit");
    };
    assert!(
        repository
            .pending_transition()
            .await
            .expect("pending")
            .is_none()
    );
    assert!(commit.tracked_paths.contains("keep.txt"));
    assert!(!commit.tracked_paths.contains("drop.tmp"));

    let mut ignored_only = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x6c; 16])).await;
    ignored_only
        .write_text("/new.tmp", "not committed")
        .await
        .expect("ignored file");
    publish_fixture(ignored_only).await;
    let live_after = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("live after ignored change")
        .value;
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
    assert!(
        repository
            .pending_transition()
            .await
            .expect("pending")
            .is_none()
    );
}

#[tokio::test]
async fn bisect_uses_durable_first_parent_generation_checkouts() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
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
            GitCommand::Bisect {
                arguments: vec!["start".to_owned(), commits[4].to_hex(), commits[0].to_hex()],
            },
            tree(5),
            &TestExecutor::returning(GitFilesystemResult::Applied {
                tree: Some(tree(3)),
                tracked_paths: None,
            }),
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
            GitCommand::Bisect {
                arguments: vec!["good".to_owned()],
            },
            tree(3),
            &TestExecutor::returning(GitFilesystemResult::Applied {
                tree: Some(tree(4)),
                tracked_paths: None,
            }),
        )
        .await
        .expect("mark good")
    else {
        panic!("expected narrowed bisect state");
    };
    assert_eq!(narrowed.current, Some(commits[3]));

    let GitCommandOutput::Bisect(done) = repository
        .run(
            GitCommand::Bisect {
                arguments: vec!["bad".to_owned()],
            },
            tree(4),
            &TestExecutor::returning(GitFilesystemResult::Applied {
                tree: Some(tree(4)),
                tracked_paths: None,
            }),
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
            GitCommand::Bisect {
                arguments: vec!["reset".to_owned()],
            },
            tree(4),
            &TestExecutor::returning(GitFilesystemResult::Applied {
                tree: Some(tree(5)),
                tracked_paths: None,
            }),
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
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let GitCommandOutput::Committed(initial) = repository
        .record_commit(
            None,
            generation(1),
            BTreeSet::from(["tracked.txt".to_owned()]),
            "initial",
            "parent",
            1,
        )
        .await
        .expect("initial commit")
    else {
        panic!("expected initial commit");
    };
    let source = workspace_identity([0x51; 16], "publication-source");
    repository
        .register_branch_workspace("agents/child", source, Some(initial.id), false)
        .await
        .expect("source branch");
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
    assert_eq!(
        repository.tracked_paths().await.expect("tracked paths"),
        tracked
    );
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
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let GitCommandOutput::Prepared {
        transition,
        action: GitFilesystemAction::CaptureCommit { .. },
    } = repository
        .execute(
            GitCommand::Commit {
                message: "initial".to_owned(),
                author: "agent".to_owned(),
                authored_at_seconds: 10,
            },
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
            &GitFilesystemResult::Captured {
                tree: tree(1),
                tracked_paths: BTreeSet::new(),
                proof: None,
            },
        )
        .await
        .expect("record commit");
    assert_eq!(
        repository
            .execute(
                GitCommand::Add {
                    paths: vec![".".to_owned()]
                },
                tree(2),
            )
            .await
            .expect("add"),
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
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let GitCommandOutput::Prepared {
        transition,
        action:
            GitFilesystemAction::ForkBranch {
                source_tree,
                head,
                switch,
                ..
            },
    } = repository
        .execute(
            GitCommand::Switch {
                branch: "feature".to_owned(),
                create: true,
            },
            tree(4),
        )
        .await
        .expect("fork request")
    else {
        panic!("expected a core workspace fork request");
    };
    assert_eq!(source_tree, tree(4));
    assert!(head.is_none());
    assert!(switch);
    let child = workspace_identity([9; 16], "feature");
    repository
        .complete_transition_result(
            transition,
            &GitFilesystemResult::Forked {
                workspace_id: child,
            },
        )
        .await
        .expect("register branch workspace");
    let GitCommandOutput::Status(status) = repository
        .execute(GitCommand::Status, GitTreeRef::exact(child, generation(4)))
        .await
        .expect("feature status")
    else {
        panic!("expected status");
    };
    assert_eq!(status.branch, "feature");
}

#[tokio::test]
async fn state_changing_filesystem_actions_are_prepared_then_completed() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let GitCommandOutput::Prepared {
        transition,
        action: GitFilesystemAction::CaptureCommit { .. },
    } = repository
        .execute(
            GitCommand::Commit {
                message: "initial".to_owned(),
                author: "agent".to_owned(),
                authored_at_seconds: 10,
            },
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
            &GitFilesystemResult::Captured {
                tree: tree(1),
                tracked_paths: BTreeSet::new(),
                proof: None,
            },
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
        .execute(
            GitCommand::Switch {
                branch: "feature".to_owned(),
                create: false,
            },
            tree(2),
        )
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
        .complete_transition_result(
            transition,
            &GitFilesystemResult::Applied {
                tree: None,
                tracked_paths: None,
            },
        )
        .await
        .expect("complete switch");
    let GitCommandOutput::Status(status) = repository
        .execute(GitCommand::Status, tree(1))
        .await
        .expect("status")
    else {
        panic!("expected status");
    };
    assert_eq!(status.branch, "feature");
}

async fn merge_reachable_branch(advance_head: bool) -> (GitCommit, GitCommit) {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let GitCommandOutput::Committed(initial) = repository
        .record_commit(
            None,
            generation(1),
            BTreeSet::from(["base.txt".to_owned()]),
            "initial",
            "parent",
            1,
        )
        .await
        .expect("initial commit")
    else {
        panic!("expected initial commit");
    };
    let source = workspace_identity([0x61; 16], "reachable-merge-source");
    repository
        .register_branch_workspace("agents/kid", source, Some(initial.id), false)
        .await
        .expect("source branch");
    let head = if advance_head {
        let GitCommandOutput::Committed(advanced) = repository
            .record_commit(
                Some(initial.id),
                generation(2),
                BTreeSet::from(["base.txt".to_owned()]),
                "advanced",
                "parent",
                2,
            )
            .await
            .expect("advanced commit")
        else {
            panic!("expected advanced commit");
        };
        advanced
    } else {
        initial
    };
    let tracked = BTreeSet::from(["base.txt".to_owned(), "kid.txt".to_owned()]);
    let GitCommandOutput::Committed(merged) = repository
        .run(
            GitCommand::Merge {
                branch: "agents/kid".to_owned(),
            },
            head.tree,
            &TestExecutor::returning(GitFilesystemResult::Applied {
                tree: Some(tree(3)),
                tracked_paths: Some(tracked.clone()),
            }),
        )
        .await
        .expect("merge")
    else {
        panic!("expected a merge commit");
    };
    assert_eq!(merged.tracked_paths, tracked);
    (head, merged)
}

#[tokio::test]
async fn merging_a_branch_reachable_from_head_records_head_once() {
    // The branch head is HEAD itself: [HEAD, HEAD] would repeat a parent.
    let (head, merged) = merge_reachable_branch(false).await;
    assert_eq!(merged.parents, vec![head.id]);
    // The branch head is an ancestor of HEAD and adds no ancestry.
    let (head, merged) = merge_reachable_branch(true).await;
    assert_eq!(merged.parents, vec![head.id]);
}

fn commit_with_parents(seed: u8, parents: Vec<GitCommitId>) -> GitCommit {
    GitCommit::new(
        tree(seed),
        tree(seed),
        None,
        BTreeSet::new(),
        parents,
        "agent",
        i64::from(seed),
        format!("commit {seed}"),
    )
}

#[test]
fn commits_record_each_parent_once_and_earlier_history_stays_readable() {
    let parent = GitCommitId([7; 32]);
    let commit = commit_with_parents(1, vec![parent, parent]);
    assert_eq!(commit.parents, vec![parent]);

    // History is content addressed and immutable: a repeated-parent commit
    // that an earlier version recorded keeps validating, so the repository
    // still loads. Only construction collapses repeats.
    let mut repeated = commit;
    repeated.parents = vec![parent, parent];
    repeated.id = repeated.hash_id();
    let mut state = GitCompatState::new("main", workspace());
    state.external_parents.insert(parent);
    state.commits.insert(repeated.id, repeated.clone());
    state.branches.get_mut("main").expect("branch").head = Some(repeated.id);
    assert!(validate_git_commits(&state, &BTreeSet::from([workspace()])).is_ok());
}

#[test]
fn merge_ancestry_uses_reachability_across_merge_parents() {
    // A <- S <- X, and HEAD = merge(X, A). S is reachable from HEAD through X.
    let a = commit_with_parents(1, Vec::new());
    let s = commit_with_parents(2, vec![a.id]);
    let x = commit_with_parents(3, vec![s.id]);
    let head = commit_with_parents(4, vec![x.id, a.id]);
    let unrelated = commit_with_parents(5, vec![a.id]);
    let mut state = GitCompatState::new("main", workspace());
    for commit in [&a, &s, &x, &head, &unrelated] {
        state.commits.insert(commit.id, commit.clone());
    }
    assert!(reaches(&state, head.id, s.id));
    assert!(reaches(&state, head.id, head.id));
    assert!(!reaches(&state, head.id, unrelated.id));
    assert!(!reaches(&state, s.id, head.id));
    // Negative control: the nearest common ancestor ties S with A on combined
    // distance and picks A, so it cannot answer whether S is an ancestor.
    assert_ne!(nearest_common_ancestor(&state, head.id, s.id), Some(s.id));
}

/// How far a child branch got before the parent discards it.
#[derive(Clone, Copy, Debug)]
enum DiscardedChild {
    Uncommitted,
    Committed,
    PartiallyMerged,
    Stashed,
}

#[tokio::test]
async fn discard_keeps_every_record_a_discarded_branch_produced() {
    let child = workspace_identity([9; 16], "discard-history");
    let child_tree = |byte| GitTreeRef::exact(child, generation(byte));
    let applied = |tree: GitTreeRef, tracked: Option<&[&str]>| {
        TestExecutor::returning(GitFilesystemResult::Applied {
            tree: Some(tree),
            tracked_paths: tracked
                .map(|paths| paths.iter().map(|path| (*path).to_owned()).collect()),
        })
    };
    let commit = |message: &str| GitCommand::Commit {
        message: message.to_owned(),
        author: "agent".to_owned(),
        authored_at_seconds: 1,
    };
    let captured = |tree: GitTreeRef| {
        TestExecutor::returning(GitFilesystemResult::Captured {
            tree,
            tracked_paths: BTreeSet::from(["child.txt".to_owned()]),
            proof: None,
        })
    };
    let switch = |branch: &str| GitCommand::Switch {
        branch: branch.to_owned(),
        create: false,
    };
    for scenario in [
        DiscardedChild::Uncommitted,
        DiscardedChild::Committed,
        DiscardedChild::PartiallyMerged,
        DiscardedChild::Stashed,
    ] {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
        repository
            .run(
                GitCommand::Branch {
                    create: Some("agents/kid".to_owned()),
                },
                tree(1),
                &TestExecutor::returning(GitFilesystemResult::Forked {
                    workspace_id: child,
                }),
            )
            .await
            .expect("child branch");
        let mut merged = None;
        if !matches!(scenario, DiscardedChild::Uncommitted) {
            repository
                .run(switch("agents/kid"), tree(1), &applied(child_tree(1), None))
                .await
                .expect("enter child");
            let GitCommandOutput::Committed(first) = repository
                .run(
                    commit("child work"),
                    child_tree(2),
                    &captured(child_tree(2)),
                )
                .await
                .expect("child commit")
            else {
                panic!("child commit records history");
            };
            if matches!(scenario, DiscardedChild::Stashed) {
                repository
                    .run(
                        GitCommand::StashPush,
                        child_tree(3),
                        &applied(child_tree(2), None),
                    )
                    .await
                    .expect("child stash");
            }
            repository
                .run(switch("main"), child_tree(2), &applied(tree(1), None))
                .await
                .expect("leave child");
            if matches!(scenario, DiscardedChild::PartiallyMerged) {
                repository
                    .run(
                        GitCommand::Merge {
                            branch: "agents/kid".to_owned(),
                        },
                        tree(1),
                        &applied(tree(4), Some(&["child.txt"])),
                    )
                    .await
                    .expect("merge first child commit");
                // The child keeps working past what the parent merged.
                repository
                    .run(switch("agents/kid"), tree(4), &applied(child_tree(2), None))
                    .await
                    .expect("re-enter child");
                repository
                    .run(commit("unmerged"), child_tree(5), &captured(child_tree(5)))
                    .await
                    .expect("unmerged child commit");
                repository
                    .run(switch("main"), child_tree(5), &applied(tree(4), None))
                    .await
                    .expect("leave child again");
                merged = Some(first);
            }
        }
        let main_tree = if merged.is_some() { tree(4) } else { tree(1) };
        let discarded = repository
            .run(
                GitCommand::Discard {
                    branch: "agents/kid".to_owned(),
                },
                main_tree,
                &TestExecutor::failing(),
            )
            .await
            .unwrap_or_else(|error| panic!("{scenario:?}: discard failed: {error:?}"));
        let GitCommandOutput::Branches { branches, .. } = discarded else {
            panic!("{scenario:?}: discard lists the remaining branches");
        };
        assert_eq!(branches.len(), 1, "{scenario:?}");
        // The repository stays writable and merged history stays reachable.
        let GitCommandOutput::Commits(log) = repository
            .run(
                GitCommand::Log { maximum: 10 },
                main_tree,
                &TestExecutor::failing(),
            )
            .await
            .unwrap_or_else(|error| panic!("{scenario:?}: log failed: {error:?}"))
        else {
            panic!("{scenario:?}: log lists commits");
        };
        if let Some(first) = &merged {
            assert!(
                log.contains(first),
                "{scenario:?}: merged child commit lost"
            );
        }
        repository
            .run(commit("parent after discard"), tree(6), &captured(tree(6)))
            .await
            .unwrap_or_else(|error| panic!("{scenario:?}: commit after discard: {error:?}"));
        if matches!(scenario, DiscardedChild::Stashed) {
            repository
                .run(GitCommand::StashPop, tree(6), &applied(tree(7), None))
                .await
                .unwrap_or_else(|error| panic!("stash from a discarded branch: {error:?}"));
        }
    }
}

/// Runs `git show OBJECT` and settles any prepared read so the next command
/// is not blocked behind it; returns the action or output it produced.
async fn shown(
    repository: &GitCompatRepository<MemoryGitCompatStore>,
    object: &str,
) -> Result<GitCommandOutput, GitCompatError<MemoryGitCompatStoreError>> {
    let output = repository
        .execute(
            GitCommand::Show {
                object: Some(object.to_owned()),
            },
            tree(2),
        )
        .await?;
    let GitCommandOutput::Prepared { transition, action } = output else {
        return Ok(output);
    };
    repository
        .complete_transition_result(
            transition,
            &GitFilesystemResult::Data {
                kind: "file".to_owned(),
                value: serde_json::Value::Null,
            },
        )
        .await?;
    Ok(GitCommandOutput::Action(action))
}

#[tokio::test]
async fn show_rev_path_reads_a_tracked_file_from_the_commit_tree() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let executor = TestExecutor::returning(GitFilesystemResult::Captured {
        tree: tree(9),
        tracked_paths: BTreeSet::from(["tracked.txt".to_owned()]),
        proof: None,
    });
    let GitCommandOutput::Committed(commit) = repository
        .run(
            GitCommand::Commit {
                message: "initial".to_owned(),
                author: "agent".to_owned(),
                authored_at_seconds: 10,
            },
            tree(1),
            &executor,
        )
        .await
        .expect("commit")
    else {
        panic!("expected commit");
    };
    let file = GitCommandOutput::Action(GitFilesystemAction::ShowFile {
        tree: commit.tree,
        path: "tracked.txt".to_owned(),
    });
    // The committed tree, not the live one, answers REV:PATH.
    assert_eq!(
        shown(&repository, "HEAD:tracked.txt")
            .await
            .expect("show file"),
        file
    );
    let short = commit.id.to_hex()[..12].to_owned();
    assert_eq!(
        shown(&repository, &format!("{short}:tracked.txt"))
            .await
            .expect("show by id"),
        file
    );
    // A bare revision still shows the commit itself.
    assert_eq!(
        shown(&repository, "HEAD").await.expect("show commit"),
        GitCommandOutput::Commits(vec![commit])
    );
    // Negative controls: an untracked path, an unknown revision and the index
    // form never fall back to another tree.
    assert!(matches!(
        shown(&repository, "HEAD:untracked.txt").await,
        Err(GitCompatError::UnknownObject(object)) if object == "HEAD:untracked.txt"
    ));
    assert!(matches!(
        shown(&repository, "missing:tracked.txt").await,
        Err(GitCompatError::UnknownObject(object)) if object == "missing"
    ));
    assert!(matches!(
        shown(&repository, ":tracked.txt").await,
        Err(GitCompatError::InvalidCommand(_))
    ));
}
