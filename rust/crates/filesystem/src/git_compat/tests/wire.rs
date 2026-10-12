use super::*;
use serde_json::{Value, json};

fn wire_values<T: serde::de::DeserializeOwned + Serialize>(
    values: Vec<Value>,
    tag: fn(&T) -> &'static str,
) -> Vec<(String, Value)> {
    values
        .into_iter()
        .map(|fixture| {
            let typed: T = serde_json::from_value(fixture.clone()).expect("typed wire fixture");
            let value = serde_json::to_value(&typed).expect("wire serialization");
            assert_eq!(value, fixture, "wire round trip");
            let name = tag(&typed);
            match &value {
                Value::String(actual) => assert_eq!(actual, name),
                Value::Object(actual) => {
                    assert_eq!(actual.len(), 1);
                    assert!(actual.contains_key(name));
                }
                _ => panic!("invalid enum encoding"),
            }
            (name.to_owned(), value)
        })
        .collect()
}

#[test]
fn generated_typescript_variant_fields_match_rust_serde_fixtures() {
    for (mode, (typescript, wire)) in [GitResetMode::Soft, GitResetMode::Mixed, GitResetMode::Hard]
        .into_iter()
        .zip(GIT_COMPAT_RESET_MODE_WIRE)
    {
        assert_eq!(serde_json::to_value(mode).unwrap(), *wire);
        assert_eq!(mode.variant_name(), *wire);
        assert_eq!(wire.to_ascii_lowercase(), *typescript);
    }
    for state in [
        GitDirtyState::Clean,
        GitDirtyState::Dirty,
        GitDirtyState::Unknown,
    ] {
        assert_eq!(serde_json::to_value(state).unwrap(), state.variant_name());
    }
    let commands = vec![
        json!("Status"),
        json!({"Diff": {"cached": true}}),
        json!({"Log": {"maximum": 2}}),
        json!({"Show": {"object": "HEAD"}}),
        json!({"Add": {"paths": ["path"]}}),
        json!({"Commit": {"message": "message", "author": "agent", "authored_at_seconds": 0}}),
        json!({"Branch": {"create": "topic"}}),
        json!({"Switch": {"branch": "topic", "create": true}}),
        json!({"Restore": {"source": "HEAD", "paths": ["path"]}}),
        json!({"Reset": {"target": "HEAD", "mode": "Hard"}}),
        json!({"Merge": {"branch": "topic"}}),
        json!("MergeContinue"),
        json!("MergeAbort"),
        json!({"Rebase": {"branch": "main"}}),
        json!("StashPush"),
        json!("StashPop"),
        json!({"CherryPick": {"object": "HEAD"}}),
        json!({"Revert": {"object": "HEAD"}}),
        json!({"Tag": {"name": "v1", "target": "HEAD", "delete": false}}),
        json!({"Blame": {"path": "path"}}),
        json!({"Grep": {"pattern": "needle", "path": "path"}}),
        json!({"Clean": {"dry_run": true}}),
        json!({"Archive": {"object": "HEAD"}}),
        json!({"Apply": {"patch": [1, 2]}}),
        json!({"Bisect": {"arguments": ["start"]}}),
        json!({"RevParse": {"argument": "HEAD"}}),
        json!({"SymbolicRef": {"short": true}}),
        json!({"MergeBase": {"left": "HEAD", "right": "main"}}),
        json!("LsFiles"),
        json!({"CheckIgnore": {"paths": ["path"]}}),
    ];
    let commands = wire_values::<GitCommand>(commands, GitCommand::variant_name)
        .into_iter()
        .map(|(_, value)| serde_json::from_value(value).unwrap())
        .collect();
    assert_command_typescript_fields_match_serde(commands);

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
    let status = json!({"branch": "main", "head": commit.id, "workspace": tree(1), "dirty": "dirty", "all_changes_staged": true});
    let branch = json!({"name": "main", "workspace_id": workspace(), "head": commit.id, "tracked_paths": ["path"]});
    let bisect = json!({"active": true, "good": commit.id, "bad": null, "current": commit.id, "remaining": 1, "first_bad": null});
    let lazy = json!({
        "kind": "lazy", "id": vec![1; 32], "workspace_id": vec![2; 16], "authored_generation": vec![3; 32],
        "source": {"identity": vec![4; 16], "epoch": 7}, "overlay": vec![5; 32], "shadows": vec![6; 32],
    });
    let lazy: GitTreeRef = serde_json::from_value(lazy).expect("lazy tree");
    let lazy = serde_json::to_value(lazy).unwrap();
    let proof = GitCaptureProof {
        fork_parent: tree(1),
        initial_generation: generation(1),
        operation_id: OperationId::from_bytes([7; 16]),
    };
    for (name, value) in [
        ("GitCompatStatus", status.clone()),
        ("GitBisectResult", bisect.clone()),
        (
            "GitGenerationRef",
            json!({"workspace_id": workspace(), "generation": generation(1)}),
        ),
        ("GitTreeRefExact", json!(tree(1))),
        ("GitTreeRefLazy", lazy.clone()),
        ("GitLazySource", lazy["source"].clone()),
        ("GitCaptureProof", json!(proof)),
        ("GitCommit", json!(commit)),
        ("GitBranch", branch.clone()),
    ] {
        let (_, declaration) = GIT_COMPAT_NESTED_TYPESCRIPT_TYPES
            .iter()
            .find(|(candidate, _)| *candidate == name)
            .unwrap();
        assert_nested_typescript_projection(name, declaration, &value);
    }

    let actions = wire_values::<GitFilesystemAction>(
        vec![
            json!({"CaptureCommit": {
                "workspace_tree": tree(1), "head_tree": tree(2), "head_workspace_tree": tree(3),
                "tracked_paths": ["tracked"], "message": "message", "author": "agent",
                "authored_at_seconds": 0, "expected_head": commit.id,
            }}),
            json!({"ForkBranch": {"branch": "topic", "source_tree": tree(1), "head": commit.id, "switch": true}}),
            json!({"SwitchWorkspace": {"workspace_id": workspace()}}),
            json!({"Diff": {"from": tree(1), "to": tree(2), "tracked_paths": []}}),
            json!({"RestoreGeneration": {"tree": tree(1), "paths": ["path"]}}),
            json!({"RestorePaths": {"tree": tree(1), "paths": ["path"]}}),
            json!({"Join": {"target_tree": tree(1), "source_workspace": workspace(), "rebase": false, "tracked_paths": []}}),
            json!({"ApplyCommit": {
                "commit": commit.id, "reverse": false, "base": tree(1), "source": tree(2), "paths": [], "tracked_paths": [],
            }}),
            json!({"Blame": {"path": "path", "commits": [commit]}}),
            json!({"Grep": {"pattern": "needle", "path": "path", "tree": tree(1)}}),
            json!({"Clean": {"dry_run": true, "tree": tree(1), "tracked_paths": []}}),
            json!({"Archive": {"tree": tree(1)}}),
            json!({"ApplyPatch": {"patch": [1, 2]}}),
            json!({"CheckIgnore": {"paths": ["path"], "tree": tree(1)}}),
        ],
        GitFilesystemAction::variant_name,
    );
    assert_typescript_fields_match_serde(
        "action",
        GIT_COMPAT_ACTION_TYPESCRIPT_TYPES,
        GIT_COMPAT_ACTION_VARIANTS,
        actions,
    );

    let results = wire_values::<GitFilesystemResult>(
        vec![
            json!({"Captured": {"tree": tree(1), "tracked_paths": [], "proof": null}}),
            json!({"Forked": {"workspace_id": workspace()}}),
            json!({"Applied": {"tree": tree(1), "tracked_paths": []}}),
            json!({"Data": {"kind": "fixture", "value": {"value": true}}}),
        ],
        GitFilesystemResult::variant_name,
    );
    assert_typescript_fields_match_serde(
        "result",
        GIT_COMPAT_RESULT_TYPESCRIPT_TYPES,
        GIT_COMPAT_RESULT_VARIANTS,
        results,
    );

    let outputs = wire_values::<GitCommandOutput>(
        vec![
            json!({"Status": status}),
            json!({"Commits": [commit]}),
            json!({"Branches": {"current": "main", "branches": [branch]}}),
            json!({"Tags": {"v1": commit.id}}),
            json!({"Committed": commit}),
            json!({"Bisect": bisect}),
            json!({"Action": {"Archive": {"tree": tree(1)}}}),
            json!({"Prepared": {"transition": GitTransitionId::from_bytes([4; 16]), "action": {"Archive": {"tree": tree(1)}}}}),
            json!({"Filesystem": {"Data": {"kind": "fixture", "value": {}}}}),
            json!({"Text": "fixture"}),
            json!({"Paths": ["path"]}),
        ],
        GitCommandOutput::variant_name,
    );
    assert_typescript_fields_match_serde(
        "output",
        GIT_COMPAT_OUTPUT_TYPESCRIPT_TYPES,
        GIT_COMPAT_OUTPUT_VARIANTS,
        outputs,
    );

    let output = GitCommandOutput::NoOp;
    assert_eq!(serde_json::to_value(&output).unwrap(), json!("NoOp"));
    assert_eq!(output.variant_name(), "NoOp");
    let mut with_proof = commit;
    with_proof.capture_proof = Some(proof);
    let output = json!(GitCommandOutput::Committed(with_proof));
    assert_eq!(
        output["Committed"]["capture_proof"]["operation_id"],
        json!("07070707-0707-0707-0707-070707070707")
    );
    assert!(GIT_COMPAT_UUID_WIRE_PATHS.contains(&"capture_proof.operation_id"));
    assert!(
        !GIT_COMPAT_BYTE_WIRE_FIELDS
            .iter()
            .any(|(field, _)| *field == "workspace")
    );
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
    assert!(parse_git_public_command(
            r#"{"kind":"commit","message":"m","author":"a","authoredAtSeconds":"9007199254740992"}"#,
        )
        .is_err());
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
