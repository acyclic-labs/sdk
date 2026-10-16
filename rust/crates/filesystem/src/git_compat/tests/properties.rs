use super::*;
use proptest::prelude::*;

proptest! {
    // Property: completing a switch requires both the prepared action and its
    // result to name the destination, for arbitrary workspace identities.
    #[test]
    fn switch_completion_cannot_cross_workspace_authority(
        identities in any::<[[u8; 16]; 3]>(),
        action in 0usize..3,
        result in 0usize..3,
        destination_exists in any::<bool>(),
    ) {
        let ids = identities.map(WorkspaceId::from_bytes);
        let mut state = GitCompatState::new("main", ids[0]);
        if destination_exists {
            state.branches.insert("child".into(), GitBranch {
                name: "child".into(), workspace_id: ids[1], head: None,
                tracked_paths: BTreeSet::new(),
            });
        }
        let pending = GitPendingTransition {
            id: GitTransitionId::from_bytes([0; 16]),
            action: GitFilesystemAction::SwitchWorkspace { workspace_id: ids[action] },
            mutation: GitPendingMutation::Switch { branch: "child".into() },
        };
        let result_workspace = ids[result];
        let result = GitFilesystemResult::Applied {
            tree: Some(GitTreeRef::exact(ids[result], generation(1))), tracked_paths: None,
        };
        let accepted = validate_completion_result::<MemoryGitCompatStoreError>(
            &state, &pending, &result,
        ).is_ok();
        prop_assert_eq!(accepted,
            destination_exists && ids[action] == ids[1] && result_workspace == ids[1]);
    }

    // Property: every accepted untrusted argv has a lossless typed wire form.
    #[test]
    fn argv_command_wire_round_trip(
        args in prop::collection::vec(".{0,64}", 0..8),
        command in prop::sample::select(vec!["status", "diff", "log", "show", "add",
            "commit", "branch", "checkout", "merge", "discard", "clean", "check-ignore"]),
        author in ".{0,64}",
        now in any::<i64>(),
    ) {
        let mut argv = vec![command.to_owned()];
        argv.extend(args);
        if let Ok(parsed) = parse_git_argv::<MemoryGitCompatStoreError>(&argv, &author, now) {
            let wire = serde_json::to_vec(&parsed).unwrap();
            let restored: GitCommand = serde_json::from_slice(&wire).unwrap();
            prop_assert_eq!(parsed, restored);
        }
    }
}
