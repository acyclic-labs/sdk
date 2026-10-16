use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn conflict_paths_belong_to_one_pending_transition(path in "[a-z]{1,16}", stale in any::<bool>()) {
        futures::executor::block_on(async {
            let main = WorkspaceId::from_bytes([1; 16]);
            let child = WorkspaceId::from_bytes([2; 16]);
            let repository = GitCompatRepository::new(main, MemoryGitCompatStore::default());
            repository.register_branch_workspace("topic", child, None, false).await.unwrap();
            let GitCommandOutput::Prepared { transition, .. } = repository.execute(
                GitCommand::Merge { branch: "topic".into() }, GitTreeRef::exact(main, generation(1)),
            ).await.unwrap() else { panic!("pending join"); };
            let paths = BTreeMap::from([(format!("/{path}"), Some(crate::FileId::from_bytes([3; 16])))]);
            let operation = if stale { OperationId::from_bytes([0; 16]) } else { transition.operation_id() };
            let result = repository.record_merge_conflicts(operation, &paths).await;
            prop_assert_eq!(result.is_ok(), !stale);
            let pending = repository.pending_transition().await.unwrap().unwrap();
            let GitPendingMutation::Join { conflicts, .. } = &pending.mutation else { panic!("pending join"); };
            prop_assert_eq!(conflicts, &(!stale).then_some(paths));
            let restored: GitPendingTransition = serde_json::from_slice(&serde_json::to_vec(&pending).unwrap()).unwrap();
            prop_assert_eq!(&pending, &restored);
            if !stale {
                let executor = TestExecutor::returning(GitFilesystemResult::Applied {
                    tree: Some(GitTreeRef::exact(main, generation(2))),
                    tracked_paths: Some(BTreeSet::from([format!("/{path}")])),
                });
                prop_assert!(repository.resume(&executor).await.unwrap().is_none());
                for command in [GitCommand::Status, GitCommand::Log { maximum: 10 }] {
                    repository.run(command, GitTreeRef::exact(main, generation(2)), &executor).await.unwrap();
                }
                let rejected = repository.run(GitCommand::Commit {
                    message: "not a resolution".into(), author: "test".into(), authored_at_seconds: 0,
                }, GitTreeRef::exact(main, generation(2)), &executor).await;
                prop_assert!(matches!(rejected,
                    Err(GitCompatRunError::Compat(GitCompatError::TransitionPending(id))) if id == transition));
                prop_assert!(executor.operations.lock().unwrap().is_empty());
                let retained = repository.pending_transition().await.unwrap();
                prop_assert_eq!(retained.as_ref(), Some(&pending));
            } else {
                let inspected = repository.execute(GitCommand::Status, GitTreeRef::exact(main, generation(2))).await;
                prop_assert!(matches!(inspected, Err(GitCompatError::TransitionPending(id)) if id == transition));
            }
            Ok(())
        })?;
    }
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
