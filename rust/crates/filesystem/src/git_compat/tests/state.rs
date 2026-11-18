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
        tree.clone(),
        tree.clone(),
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
            0 => alias.tree = other.clone(),
            1 => alias.workspace_tree = other.clone(),
            2 => alias.capture_proof.as_mut().unwrap().fork_parent = other.clone(),
            _ => alias.capture_proof.as_mut().unwrap().initial_generation = generation(187),
        }
        assert_ne!(alias.hash_id(), commit.id);
    }
}

#[test]
fn current_state_round_trips_and_incompatible_records_are_rejected() {
    let state = GitCompatState::new("main", WorkspaceId::from_bytes([1; 16]));
    let encoded = serde_json::to_vec(&state).expect("encode current state");
    let mut decoded: GitCompatState =
        serde_json::from_slice(&encoded).expect("decode current state");
    assert!(validate_git_state(&decoded).is_ok());
    let mut incomplete = serde_json::to_value(&state).expect("encode current state");
    incomplete
        .as_object_mut()
        .unwrap()
        .remove("retired_workspaces");
    assert!(serde_json::from_value::<GitCompatState>(incomplete).is_err());
    for version in [0, 2, 9, u32::MAX] {
        decoded.version = version;
        assert!(validate_git_state(&decoded).is_err());
    }
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
