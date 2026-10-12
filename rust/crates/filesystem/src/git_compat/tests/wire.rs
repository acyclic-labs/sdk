use super::*;

#[test]
fn public_command_kinds_cover_every_canonical_command_variant() {
    let public_wire = GIT_COMPAT_PUBLIC_COMMAND_KINDS
        .iter()
        .map(|(_, wire)| *wire)
        .collect::<BTreeSet<_>>();
    let canonical = GIT_COMPAT_COMMAND_VARIANTS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    assert_eq!(public_wire, canonical);
    assert_eq!(GIT_COMPAT_PUBLIC_COMMAND_KINDS.len(), canonical.len());
}

#[test]
fn public_command_projection_uses_canonical_defaults_and_integer_bounds() {
    assert_eq!(
        parse_git_public_command(r#"{"kind":"status"}"#).expect("status"),
        GitCommand::Status,
    );
    assert_eq!(
        parse_git_public_command(r#"{"kind":"diff"}"#).expect("diff"),
        GitCommand::Diff { cached: false },
    );
    assert_eq!(
        parse_git_public_command(r#"{"kind":"show"}"#).expect("show"),
        GitCommand::Show { object: None },
    );
    assert_eq!(
        parse_git_public_command(r#"{"kind":"branch"}"#).expect("branch"),
        GitCommand::Branch { create: None },
    );
    assert_eq!(
        parse_git_public_command(r#"{"kind":"archive"}"#).expect("archive"),
        GitCommand::Archive { object: None },
    );
    assert_eq!(
        parse_git_public_command(r#"{"kind":"symbolic-ref"}"#).expect("symbolic-ref"),
        GitCommand::SymbolicRef { short: false },
    );
    assert_eq!(
        parse_git_public_command(
            r#"{"kind":"commit","message":"m","author":"a","authoredAtSeconds":"9007199254740991"}"#,
        )
        .expect("safe timestamp"),
        GitCommand::Commit {
            message: "m".to_owned(),
            author: "a".to_owned(),
            authored_at_seconds: 9_007_199_254_740_991,
        },
    );
    assert!(
        parse_git_public_command(
            r#"{"kind":"commit","message":"m","author":"a","authoredAtSeconds":"9007199254740992"}"#,
        )
        .is_err()
    );
    assert!(
        parse_git_public_command(r#"{"kind":"reset","target":"HEAD","mode":"invalid"}"#).is_err()
    );
}

#[test]
fn json_projectors_reject_raw_malformed_fixtures() {
    assert_eq!(
        canonicalize_git_output_json(r#""NoOp""#).expect("canonical no-op"),
        r#""NoOp""#,
    );
    assert!(canonicalize_git_output_json(r#"{"Status":{"branch":"main"}}"#).is_err());
    assert!(
        canonicalize_git_pending_transition_json(
            r#"{"action":{"ApplyPatch":{"patch":[256]}},"mutation":"NoOp"}"#,
        )
        .is_err()
    );
}

#[test]
fn git_wire_contract_matches_serde_tags_and_identity_widths() {
    let commit = GitCommit::new(
        tree(1),
        tree(1),
        None,
        BTreeSet::new(),
        Vec::new(),
        "agent",
        0,
        "fixture",
    );
    let action = GitFilesystemAction::ApplyPatch {
        patch: vec![1, 2, 3],
    };
    let outputs = vec![
        GitCommandOutput::NoOp,
        GitCommandOutput::Status(GitStatus {
            branch: "main".to_owned(),
            head: None,
            workspace: tree(1),
            dirty: GitDirtyState::Clean,
            all_changes_staged: true,
        }),
        GitCommandOutput::Commits(vec![commit.clone()]),
        GitCommandOutput::Branches {
            current: "main".to_owned(),
            branches: Vec::new(),
        },
        GitCommandOutput::Tags(std::collections::BTreeMap::new()),
        GitCommandOutput::Committed(commit),
        GitCommandOutput::Bisect(GitBisectResult {
            active: false,
            good: None,
            bad: None,
            current: None,
            remaining: 0,
            first_bad: None,
        }),
        GitCommandOutput::Action(action.clone()),
        GitCommandOutput::Prepared {
            transition: GitTransitionId::from_bytes([3; 16]),
            action,
        },
        GitCommandOutput::Filesystem(GitFilesystemResult::Data {
            kind: "fixture".to_owned(),
            value: serde_json::json!({}),
        }),
        GitCommandOutput::Text("fixture".to_owned()),
        GitCommandOutput::Paths(vec!["fixture".to_owned()]),
    ];

    let names: Vec<_> = outputs
        .iter()
        .filter(|output| output.variant_name() != "NoOp")
        .map(|output| output.variant_name())
        .collect();
    assert_eq!(names, GIT_COMPAT_OUTPUT_VARIANTS);
    for output in &outputs {
        let json = serde_json::to_value(output).expect("Git output JSON");
        if output.variant_name() == "NoOp" {
            assert_eq!(json, serde_json::json!("NoOp"));
        } else {
            let object = json.as_object().expect("tagged Git output object");
            assert_eq!(object.len(), 1);
            assert!(object.contains_key(output.variant_name()));
        }
    }

    let proof_operation = OperationId::from_bytes([3; 16]);
    let committed_with_proof = GitCommit::new(
        tree(2),
        tree(1),
        Some(GitCaptureProof {
            fork_parent: tree(1),
            initial_generation: generation(1),
            operation_id: proof_operation,
        }),
        std::collections::BTreeSet::new(),
        Vec::new(),
        "agent",
        0,
        "fixture",
    );
    let committed_json = serde_json::to_value(GitCommandOutput::Committed(committed_with_proof))
        .expect("committed output JSON");
    assert_eq!(
        committed_json["Committed"]["capture_proof"]["operation_id"],
        serde_json::json!("03030303-0303-0303-0303-030303030303")
    );
    assert!(GIT_COMPAT_UUID_WIRE_PATHS.contains(&"capture_proof.operation_id"));

    let status_json = serde_json::to_value(GitCommandOutput::Status(GitStatus {
        branch: "main".to_owned(),
        head: None,
        workspace: tree(1),
        dirty: GitDirtyState::Clean,
        all_changes_staged: true,
    }))
    .expect("status output JSON");
    assert!(status_json["Status"]["workspace"].is_object());
    assert!(
        !GIT_COMPAT_IDENTITY_WIRE_FIELDS
            .iter()
            .any(|(key, _)| *key == "workspace")
    );
    assert!(
        !GIT_COMPAT_BYTE_WIRE_FIELDS
            .iter()
            .any(|(key, _)| *key == "workspace")
    );

    for (key, width) in GIT_COMPAT_IDENTITY_WIRE_FIELDS {
        assert!(!key.is_empty());
        assert!(*width > 0);
    }
    assert_eq!(
        GIT_COMPAT_IDENTITY_WIRE_FIELDS
            .iter()
            .find(|(key, _)| *key == "workspace_id")
            .map(|(_, width)| *width),
        Some(std::mem::size_of::<WorkspaceId>())
    );
    assert_eq!(
        GIT_COMPAT_IDENTITY_WIRE_FIELDS
            .iter()
            .find(|(key, _)| *key == "generation")
            .map(|(_, width)| *width),
        Some(std::mem::size_of::<GenerationId>())
    );
    assert_eq!(
        GIT_COMPAT_TRANSITION_ID_BYTES,
        std::mem::size_of::<GitTransitionId>()
    );
}

#[test]
fn generated_typescript_variant_fields_match_rust_serde_fixtures() {
    let commit = GitCommit::new(
        tree(1),
        tree(1),
        None,
        BTreeSet::new(),
        Vec::new(),
        "agent",
        0,
        "fixture",
    );
    let reset_modes = [GitResetMode::Soft, GitResetMode::Mixed, GitResetMode::Hard];
    let dirty_states = [
        GitDirtyState::Clean,
        GitDirtyState::Dirty,
        GitDirtyState::Unknown,
    ];
    assert_eq!(
        GIT_COMPAT_RESET_MODE_VARIANTS.len(),
        GIT_COMPAT_RESET_MODE_TYPESCRIPT_VARIANTS.len()
    );
    assert_eq!(
        GIT_COMPAT_RESET_MODE_VARIANTS.len(),
        GIT_COMPAT_RESET_MODE_WIRE.len()
    );
    assert_eq!(
        GIT_COMPAT_DIRTY_STATE_VARIANTS.len(),
        GIT_COMPAT_DIRTY_STATE_TYPESCRIPT_VARIANTS.len()
    );
    assert_eq!(
        GIT_COMPAT_DIRTY_STATE_VARIANTS.len(),
        GIT_COMPAT_DIRTY_STATE_WIRE.len()
    );
    for (index, mode) in reset_modes.into_iter().enumerate() {
        let json = serde_json::to_value(mode).expect("reset mode JSON");
        let actual = json.as_str().expect("reset mode tag");
        let (typescript, wire) = GIT_COMPAT_RESET_MODE_WIRE[index];
        assert_eq!(actual, wire, "reset mode serde tag drift");
        assert_eq!(mode.variant_name(), wire, "reset mode matcher drift");
        assert_eq!(GIT_COMPAT_RESET_MODE_VARIANTS[index], wire);
        assert_eq!(GIT_COMPAT_RESET_MODE_TYPESCRIPT_VARIANTS[index], typescript);
    }
    for (index, state) in dirty_states.into_iter().enumerate() {
        let json = serde_json::to_value(state).expect("dirty state JSON");
        let actual = json.as_str().expect("dirty state tag");
        let (typescript, wire) = GIT_COMPAT_DIRTY_STATE_WIRE[index];
        assert_eq!(actual, wire, "dirty state serde tag drift");
        assert_eq!(state.variant_name(), wire, "dirty state matcher drift");
        assert_eq!(GIT_COMPAT_DIRTY_STATE_VARIANTS[index], wire);
        assert_eq!(
            GIT_COMPAT_DIRTY_STATE_TYPESCRIPT_VARIANTS[index],
            typescript
        );
    }
    assert_eq!(
        GIT_COMPAT_RESET_MODE_VARIANTS
            .iter()
            .collect::<BTreeSet<_>>()
            .len(),
        GIT_COMPAT_RESET_MODE_VARIANTS.len()
    );
    assert_eq!(
        GIT_COMPAT_RESET_MODE_WIRE
            .iter()
            .map(|(_, wire)| wire)
            .collect::<BTreeSet<_>>()
            .len(),
        GIT_COMPAT_RESET_MODE_WIRE.len()
    );
    assert_eq!(
        GIT_COMPAT_DIRTY_STATE_VARIANTS
            .iter()
            .collect::<BTreeSet<_>>()
            .len(),
        GIT_COMPAT_DIRTY_STATE_VARIANTS.len()
    );
    assert_eq!(
        GIT_COMPAT_DIRTY_STATE_WIRE
            .iter()
            .map(|(_, wire)| wire)
            .collect::<BTreeSet<_>>()
            .len(),
        GIT_COMPAT_DIRTY_STATE_WIRE.len()
    );
    assert_command_typescript_fields_match_serde(vec![
        GitCommand::Status,
        GitCommand::Diff { cached: true },
        GitCommand::Log { maximum: 2 },
        GitCommand::Show {
            object: Some(GitObjectName("HEAD".to_owned())),
        },
        GitCommand::Add {
            paths: vec!["path".to_owned()],
        },
        GitCommand::Commit {
            message: "message".to_owned(),
            author: "agent".to_owned(),
            authored_at_seconds: 0,
        },
        GitCommand::Branch {
            create: Some("topic".to_owned()),
        },
        GitCommand::Switch {
            branch: "topic".to_owned(),
            create: true,
        },
        GitCommand::Restore {
            source: Some(GitObjectName("HEAD".to_owned())),
            paths: vec!["path".to_owned()],
        },
        GitCommand::Reset {
            target: GitObjectName("HEAD".to_owned()),
            mode: GitResetMode::Hard,
        },
        GitCommand::Merge {
            branch: "topic".to_owned(),
        },
        GitCommand::MergeContinue,
        GitCommand::MergeAbort,
        GitCommand::Rebase {
            branch: "main".to_owned(),
        },
        GitCommand::StashPush,
        GitCommand::StashPop,
        GitCommand::CherryPick {
            object: GitObjectName("HEAD".to_owned()),
        },
        GitCommand::Revert {
            object: GitObjectName("HEAD".to_owned()),
        },
        GitCommand::Tag {
            name: Some("v1".to_owned()),
            target: Some(GitObjectName("HEAD".to_owned())),
            delete: false,
        },
        GitCommand::Blame {
            path: "path".to_owned(),
        },
        GitCommand::Grep {
            pattern: "needle".to_owned(),
            path: Some("path".to_owned()),
        },
        GitCommand::Clean { dry_run: true },
        GitCommand::Archive {
            object: Some(GitObjectName("HEAD".to_owned())),
        },
        GitCommand::Apply { patch: vec![1, 2] },
        GitCommand::Bisect {
            arguments: vec!["start".to_owned()],
        },
        GitCommand::RevParse {
            argument: "HEAD".to_owned(),
        },
        GitCommand::SymbolicRef { short: true },
        GitCommand::MergeBase {
            left: GitObjectName("HEAD".to_owned()),
            right: GitObjectName("main".to_owned()),
        },
        GitCommand::LsFiles,
        GitCommand::CheckIgnore {
            paths: vec!["path".to_owned()],
        },
    ]);
    let lazy_tree: GitTreeRef = serde_json::from_value(serde_json::json!({
        "kind": "lazy",
        "id": vec![1; 32],
        "workspace_id": vec![2; 16],
        "authored_generation": vec![3; 32],
        "source": { "identity": vec![4; 16], "epoch": 7 },
        "overlay": vec![5; 32],
        "shadows": vec![6; 32],
    }))
    .expect("lazy tree fixture");
    let lazy_tree_json = serde_json::to_value(lazy_tree).expect("lazy tree fixture");
    let nested_values = [
        (
            "GitCompatStatus",
            serde_json::to_value(GitStatus {
                branch: "main".to_owned(),
                head: Some(commit.id),
                workspace: tree(1),
                dirty: GitDirtyState::Dirty,
                all_changes_staged: true,
            })
            .expect("status fixture"),
        ),
        (
            "GitBisectResult",
            serde_json::to_value(GitBisectResult {
                active: true,
                good: Some(commit.id),
                bad: None,
                current: Some(commit.id),
                remaining: 1,
                first_bad: None,
            })
            .expect("bisect fixture"),
        ),
        (
            "GitGenerationRef",
            serde_json::to_value(GitGenerationRef {
                workspace_id: workspace(),
                generation: generation(1),
            })
            .expect("generation fixture"),
        ),
        (
            "GitTreeRefExact",
            serde_json::to_value(tree(1)).expect("exact tree fixture"),
        ),
        ("GitTreeRefLazy", lazy_tree_json.clone()),
        ("GitLazySource", lazy_tree_json["source"].clone()),
        (
            "GitCaptureProof",
            serde_json::to_value(GitCaptureProof {
                fork_parent: tree(1),
                initial_generation: generation(1),
                operation_id: OperationId::from_bytes([7; 16]),
            })
            .expect("capture proof fixture"),
        ),
        (
            "GitCommit",
            serde_json::to_value(&commit).expect("commit fixture"),
        ),
        (
            "GitBranch",
            serde_json::to_value(GitBranch {
                name: "main".to_owned(),
                workspace_id: workspace(),
                head: Some(commit.id),
                tracked_paths: BTreeSet::from(["path".to_owned()]),
            })
            .expect("branch fixture"),
        ),
    ];
    for (type_name, value) in nested_values {
        let (_, declaration) = GIT_COMPAT_NESTED_TYPESCRIPT_TYPES
            .iter()
            .find(|(name, _)| *name == type_name)
            .expect("nested TypeScript declaration");
        assert_nested_typescript_projection(type_name, declaration, &value);
    }
    let actions = vec![
        GitFilesystemAction::CaptureCommit {
            workspace_tree: tree(1),
            head_tree: Some(tree(2)),
            head_workspace_tree: Box::new(Some(tree(3))),
            tracked_paths: BTreeSet::from(["tracked".to_owned()]),
            message: "message".to_owned(),
            author: "agent".to_owned(),
            authored_at_seconds: 0,
            expected_head: Some(commit.id),
        },
        GitFilesystemAction::ForkBranch {
            branch: "topic".to_owned(),
            source_tree: tree(1),
            head: Some(commit.id),
            switch: true,
        },
        GitFilesystemAction::SwitchWorkspace {
            workspace_id: workspace(),
        },
        GitFilesystemAction::Diff {
            from: Some(tree(1)),
            to: tree(2),
            tracked_paths: BTreeSet::new(),
        },
        GitFilesystemAction::RestoreGeneration {
            tree: tree(1),
            paths: Some(BTreeSet::from(["path".to_owned()])),
        },
        GitFilesystemAction::RestorePaths {
            tree: tree(1),
            paths: vec!["path".to_owned()],
        },
        GitFilesystemAction::Join {
            target_tree: tree(1),
            source_workspace: workspace(),
            rebase: false,
            tracked_paths: BTreeSet::new(),
        },
        GitFilesystemAction::ApplyCommit {
            commit: commit.id,
            reverse: false,
            base: Some(tree(1)),
            source: Some(tree(2)),
            paths: BTreeSet::new(),
            tracked_paths: BTreeSet::new(),
        },
        GitFilesystemAction::Blame {
            path: "path".to_owned(),
            commits: vec![commit.clone()],
        },
        GitFilesystemAction::Grep {
            pattern: "needle".to_owned(),
            path: Some("path".to_owned()),
            tree: tree(1),
        },
        GitFilesystemAction::Clean {
            dry_run: true,
            tree: tree(1),
            tracked_paths: BTreeSet::new(),
        },
        GitFilesystemAction::Archive { tree: tree(1) },
        GitFilesystemAction::ApplyPatch { patch: vec![1, 2] },
        GitFilesystemAction::CheckIgnore {
            paths: vec!["path".to_owned()],
            tree: tree(1),
        },
    ];
    let action_values = actions.into_iter().map(|action| {
        let variant_name = action.variant_name();
        let variant = serde_json::to_value(&action)
            .expect("action JSON")
            .as_object()
            .expect("tagged action")
            .keys()
            .next()
            .expect("action tag")
            .clone();
        assert_eq!(variant, variant_name, "action serde tag drift");
        (variant, serde_json::to_value(action).expect("action JSON"))
    });
    assert_typescript_fields_match_serde(
        "action",
        GIT_COMPAT_ACTION_TYPESCRIPT_TYPES,
        GIT_COMPAT_ACTION_VARIANTS,
        action_values,
    );

    let results = vec![
        GitFilesystemResult::Captured {
            tree: tree(1),
            tracked_paths: BTreeSet::new(),
            proof: None,
        },
        GitFilesystemResult::Forked {
            workspace_id: workspace(),
        },
        GitFilesystemResult::Applied {
            tree: Some(tree(1)),
            tracked_paths: Some(BTreeSet::new()),
        },
        GitFilesystemResult::Data {
            kind: "fixture".to_owned(),
            value: serde_json::json!({ "value": true }),
        },
    ];
    let result_values = results.into_iter().map(|result| {
        let variant_name = result.variant_name();
        let value = serde_json::to_value(&result).expect("result JSON");
        let variant = value
            .as_object()
            .expect("tagged result")
            .keys()
            .next()
            .expect("result tag")
            .clone();
        assert_eq!(variant, variant_name, "result serde tag drift");
        (variant, value)
    });
    assert_typescript_fields_match_serde(
        "result",
        GIT_COMPAT_RESULT_TYPESCRIPT_TYPES,
        GIT_COMPAT_RESULT_VARIANTS,
        result_values,
    );

    let outputs = vec![
        GitCommandOutput::Status(GitStatus {
            branch: "main".to_owned(),
            head: Some(commit.id),
            workspace: tree(1),
            dirty: GitDirtyState::Dirty,
            all_changes_staged: true,
        }),
        GitCommandOutput::Commits(vec![commit.clone()]),
        GitCommandOutput::Branches {
            current: "main".to_owned(),
            branches: vec![GitBranch {
                name: "main".to_owned(),
                workspace_id: workspace(),
                head: Some(commit.id),
                tracked_paths: BTreeSet::new(),
            }],
        },
        GitCommandOutput::Tags(BTreeMap::from([("v1".to_owned(), commit.id)])),
        GitCommandOutput::Committed(commit),
        GitCommandOutput::Bisect(GitBisectResult {
            active: true,
            good: None,
            bad: None,
            current: None,
            remaining: 1,
            first_bad: None,
        }),
        GitCommandOutput::Action(GitFilesystemAction::Archive { tree: tree(1) }),
        GitCommandOutput::Prepared {
            transition: GitTransitionId::from_bytes([4; 16]),
            action: GitFilesystemAction::Archive { tree: tree(1) },
        },
        GitCommandOutput::Filesystem(GitFilesystemResult::Data {
            kind: "fixture".to_owned(),
            value: serde_json::json!({}),
        }),
        GitCommandOutput::Text("fixture".to_owned()),
        GitCommandOutput::Paths(vec!["path".to_owned()]),
    ];
    let output_values = outputs.into_iter().map(|output| {
        let variant_name = output.variant_name();
        let value = serde_json::to_value(&output).expect("output JSON");
        let variant = value
            .as_object()
            .expect("tagged output")
            .keys()
            .next()
            .expect("output tag")
            .clone();
        assert_eq!(variant, variant_name, "output serde tag drift");
        (variant, value)
    });
    assert_typescript_fields_match_serde(
        "output",
        GIT_COMPAT_OUTPUT_TYPESCRIPT_TYPES,
        GIT_COMPAT_OUTPUT_VARIANTS,
        output_values,
    );
}

#[test]
fn git_wire_identity_contract_matches_native_bytes() {
    let operation = OperationId::from_bytes(std::array::from_fn(|index| {
        u8::try_from(index).unwrap_or_default()
    }));
    let generation = GenerationId::new(Digest::from_bytes(std::array::from_fn(|index| {
        u8::try_from(index).unwrap_or_default()
    })));
    let tree = GitTreeRef::exact(WorkspaceId::from_bytes(operation.into_bytes()), generation);
    assert_eq!(
        serde_json::to_value(GitTransitionId::from_bytes(operation.into_bytes()))
            .expect("transition JSON"),
        serde_json::json!("00010203-0405-0607-0809-0a0b0c0d0e0f")
    );
    assert_eq!(
        serde_json::to_value(GitCaptureProof {
            fork_parent: tree,
            initial_generation: generation,
            operation_id: operation,
        })
        .expect("proof JSON"),
        serde_json::json!({
            "fork_parent": {
                "kind": "exact",
                "workspace_id": (0u8..16).collect::<Vec<_>>(),
                "generation": (0u8..32).collect::<Vec<_>>()
            },
            "initial_generation": (0u8..32).collect::<Vec<_>>(),
            "operation_id": "00010203-0405-0607-0809-0a0b0c0d0e0f"
        })
    );
}
