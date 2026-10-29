use super::*;

#[test]
fn commit_identity_authenticates_every_generation_locator() {
    let generation = |locator| {
        GenerationId::from_root(crate::ObjectId {
            kind: crate::ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([183; 32]),
            storage: Some(crate::ObjectStorage {
                locator: Digest::from_bytes([locator; 32]),
                root_file_id: crate::FileId::from_bytes([184; 16]),
                origin_generation: Digest::from_bytes([185; 32]),
            }),
        })
    };
    let tree = GitTreeRef::exact(workspace(), generation(186));
    let other = GitTreeRef::exact(workspace(), generation(187));
    let commit = GitCommit::new(
        tree,
        tree,
        Some(GitCaptureProof {
            fork_parent: tree,
            initial_generation: generation(186),
            operation_id: OperationId::from_bytes([188; 16]),
        }),
        BTreeSet::new(),
        Vec::new(),
        "author",
        0,
        "message",
    );
    assert_eq!(commit.id, commit.hash_id());
    for field in 0..4 {
        let mut alias = commit.clone();
        match field {
            0 => alias.tree = other,
            1 => alias.workspace_tree = other,
            2 => alias.capture_proof.as_mut().unwrap().fork_parent = other,
            _ => alias.capture_proof.as_mut().unwrap().initial_generation = generation(187),
        }
        assert_ne!(alias.hash_id(), commit.id);
    }
}

#[test]
fn current_state_round_trips_and_obsolete_versions_are_rejected() {
    let state = GitCompatState::new("main", WorkspaceId::from_bytes([1; 16]));
    let encoded = serde_json::to_vec(&state).expect("encode current state");
    let mut decoded: GitCompatState =
        serde_json::from_slice(&encoded).expect("decode current state");
    assert!(validate_git_state(&decoded).is_ok());
    for version in [0, 2, 9, u32::MAX] {
        decoded.version = version;
        assert!(validate_git_state(&decoded).is_err());
    }
}

#[test]
fn records_saved_before_discard_existed_reopen_unchanged() {
    // A repository persisted before branches could be discarded has no
    // retired workspaces; it must reopen with its history intact.
    let mut state = GitCompatState::new("main", WorkspaceId::from_bytes([1; 16]));
    state.revision = 7;
    let mut saved = serde_json::to_value(&state).expect("encode state");
    saved
        .as_object_mut()
        .expect("state object")
        .remove("retired_workspaces")
        .expect("current records carry retired workspaces");
    let reopened: GitCompatState =
        serde_json::from_value(saved).expect("pre-discard record decodes");
    assert!(reopened.retired_workspaces.is_empty());
    assert_eq!(reopened, state);
    assert!(validate_git_state(&reopened).is_ok());
}

#[tokio::test]
async fn foreign_workspace_tree_is_rejected_before_executor_side_effects() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let foreign_workspace = workspace_identity([8; 16], "foreign");
    let executor = TestExecutor::returning(GitFilesystemResult::Applied {
        tree: None,
        tracked_paths: None,
    });

    let error = repository
        .run(
            GitCommand::Status,
            GitTreeRef::exact(foreign_workspace, generation(1)),
            &executor,
        )
        .await
        .expect_err("foreign tree must fail closed");

    assert!(matches!(
        error,
        GitCompatRunError::Compat(GitCompatError::WorkspaceMismatch)
    ));
    assert!(
        executor
            .operations
            .lock()
            .expect("operation lock")
            .is_empty(),
        "foreign trees must never reach the filesystem executor"
    );
}

#[tokio::test]
async fn foreign_completion_tree_is_rejected_without_consuming_the_transition() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let GitCommandOutput::Prepared { transition, .. } = repository
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
        panic!("expected prepared commit");
    };
    let foreign = workspace_identity([8; 16], "foreign-result");
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
        repository
            .pending_transition()
            .await
            .expect("pending transition")
            .map(|pending| pending.id),
        Some(transition)
    );
}

#[tokio::test]
async fn persisted_commit_hashes_are_validated_before_use() {
    let store = MemoryGitCompatStore::default();
    let mut state = GitCompatState::new("main", workspace());
    let mut commit = GitCommit::new(
        tree(1),
        tree(1),
        None,
        BTreeSet::new(),
        Vec::new(),
        "agent",
        10,
        "initial",
    );
    commit.message = "forged".to_owned();
    state.revision = 1;
    state.commits.insert(commit.id, commit.clone());
    state.branches.get_mut("main").expect("main branch").head = Some(commit.id);
    {
        let mut states = store.states.0.lock().expect("state lock");
        states.insert(workspace(), state);
    }
    let repository = GitCompatRepository::new(workspace(), store);
    assert!(matches!(
        repository.execute(GitCommand::Status, tree(1)).await,
        Err(GitCompatError::InvalidState)
    ));
}

#[test]
fn generated_commit_uses_exact_resulting_tracked_paths() {
    let mut state = GitCompatState::new("main", workspace());
    state.current_mut().expect("current branch").tracked_paths =
        BTreeSet::from(["deleted.txt".to_owned(), "kept.txt".to_owned()]);
    let output = complete_pending::<MemoryGitCompatStoreError>(
        &mut state,
        GitPendingMutation::ApplyCommit {
            commit: GitCommitId::from_bytes([7; 32]),
            reverse: false,
        },
        &GitFilesystemResult::Applied {
            tree: Some(tree(2)),
            tracked_paths: Some(BTreeSet::from(["kept.txt".to_owned()])),
        },
    )
    .expect("generated commit");
    let GitCommandOutput::Committed(commit) = output else {
        panic!("expected generated commit");
    };
    assert_eq!(
        commit.tracked_paths,
        BTreeSet::from(["kept.txt".to_owned()])
    );
    assert_eq!(
        state.current().expect("current branch").tracked_paths,
        commit.tracked_paths
    );
}
