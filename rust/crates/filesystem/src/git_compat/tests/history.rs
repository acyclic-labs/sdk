use super::*;

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
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("verified proof")
        .value;
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
