use super::*;

#[test]
fn current_state_round_trips_and_obsolete_versions_are_rejected() {
    let state = GitCompatState::new("main", WorkspaceId::from_bytes([1; 16]));
    let encoded = serde_json::to_vec(&state).expect("encode current state");
    let mut decoded: GitCompatState = serde_json::from_slice(&encoded).expect("decode current state");
    assert!(validate_git_state(&decoded).is_ok());
    for version in [0, 2, 9, u32::MAX] {
        decoded.version = version;
        assert!(validate_git_state(&decoded).is_err());
    }
}

#[tokio::test]
async fn foreign_workspace_tree_is_rejected_before_executor_side_effects() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    let foreign_workspace = WorkspaceId::derive(
        [8; 16],
        &WorkspaceName::new("foreign", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("valid foreign workspace")
            .value,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .expect("workspace identity")
    .value;
    let executor = TestExecutor::returning(GitFilesystemResult::Applied { tree: None, tracked_paths: None });

    let error = repository
        .run(GitCommand::Status, GitTreeRef::exact(foreign_workspace, generation(1)), &executor)
        .await
        .expect_err("foreign tree must fail closed");

    assert!(matches!(error, GitCompatRunError::Compat(GitCompatError::WorkspaceMismatch)));
    assert!(
        executor.operations.lock().expect("operation lock").is_empty(),
        "foreign trees must never reach the filesystem executor"
    );
}

#[tokio::test]
async fn foreign_completion_tree_is_rejected_without_consuming_the_transition() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
    let GitCommandOutput::Prepared { transition, .. } = repository
        .execute(
            GitCommand::Commit { message: "initial".to_owned(), author: "agent".to_owned(), authored_at_seconds: 10 },
            tree(1),
        )
        .await
        .expect("prepare commit")
    else {
        panic!("expected prepared commit");
    };
    let foreign = WorkspaceId::derive(
        [8; 16],
        &WorkspaceName::new("foreign-result", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("valid workspace")
            .value,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .expect("workspace identity")
    .value;
    assert!(matches!(
        repository
            .complete_transition_result(
                transition,
                &GitFilesystemResult::Captured {
                    tree: GitTreeRef::exact(foreign, generation(1)),
                    tracked_paths: BTreeSet::new(),
                    proof: None,
                },
            )
            .await,
        Err(GitCompatError::WorkspaceMismatch)
    ));
    assert_eq!(
        repository.pending_transition().await.expect("pending transition").map(|pending| pending.id),
        Some(transition)
    );
}

#[tokio::test]
async fn persisted_commit_hashes_are_validated_before_use() {
    let store = MemoryGitCompatStore::new();
    let mut state = GitCompatState::new("main", workspace());
    let mut commit = GitCommit::new(tree(1), tree(1), None, BTreeSet::new(), Vec::new(), "agent", 10, "initial");
    commit.message = "forged".to_owned();
    state.revision = 1;
    state.commits.insert(commit.id, commit.clone());
    state.branches.get_mut("main").expect("main branch").head = Some(commit.id);
    {
        let mut states = store.states.0.lock().expect("state lock");
        states.insert(workspace(), state);
    }
    let repository = GitCompatRepository::new(workspace(), store);
    assert!(matches!(repository.execute(GitCommand::Status, tree(1)).await, Err(GitCompatError::InvalidState)));
}

#[test]
fn generated_commit_uses_exact_resulting_tracked_paths() {
    let mut state = GitCompatState::new("main", workspace());
    state.current_mut().expect("current branch").tracked_paths =
        BTreeSet::from(["deleted.txt".to_owned(), "kept.txt".to_owned()]);
    let output = complete_pending::<MemoryGitCompatStoreError>(
        &mut state,
        GitPendingMutation::ApplyCommit { commit: GitCommitId::from_bytes([7; 32]), reverse: false },
        &GitFilesystemResult::Applied {
            tree: Some(tree(2)),
            tracked_paths: Some(BTreeSet::from(["kept.txt".to_owned()])),
        },
    )
    .expect("generated commit");
    let GitCommandOutput::Committed(commit) = output else {
        panic!("expected generated commit");
    };
    assert_eq!(commit.tracked_paths, BTreeSet::from(["kept.txt".to_owned()]));
    assert_eq!(state.current().expect("current branch").tracked_paths, commit.tracked_paths);
}
