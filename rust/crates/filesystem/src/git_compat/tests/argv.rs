use super::*;

#[tokio::test]
async fn argv_covers_local_history_and_rejects_transport() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    assert!(matches!(
        repository
            .execute_argv(
                &["reset".to_owned(), "--hard".to_owned(), "HEAD".to_owned()],
                tree(1),
                "agent",
                10,
            )
            .await,
        Err(GitCompatError::UnbornHead)
    ));
    assert!(matches!(
        repository
            .execute_argv(&["clean".to_owned()], tree(1), "agent", 10,)
            .await,
        Err(GitCompatError::InvalidCommand(message)) if message.contains("requires -f")
    ));
    assert!(matches!(
        repository
            .execute_argv(
                &["clean".to_owned(), "--dry-run".to_owned()],
                tree(1),
                "agent",
                10,
            )
            .await,
        Ok(GitCommandOutput::Prepared {
            action: GitFilesystemAction::Clean { dry_run: true, .. },
            ..
        })
    ));
    assert!(matches!(
        repository
            .execute_argv(&["push".to_owned()], tree(1), "agent", 10,)
            .await,
        Err(GitCompatError::Unsupported { .. })
    ));
}

#[test]
fn argv_parser_has_an_explicit_command_conformance_matrix() {
    let cases: &[(&[&str], &str)] = &[
        (&["status", "--short"], r#""Status""#),
        (&["diff", "--staged"], r#"{"Diff":{"cached":true}}"#),
        (&["log", "--max-count=7"], r#"{"Log":{"maximum":7}}"#),
        (&["show", "HEAD"], r#"{"Show":{"object":"HEAD"}}"#),
        (&["add", "--all"], r#"{"Add":{"paths":[]}}"#),
        (
            &["commit", "--message=ready", "--author=agent"],
            r#"{"Commit":{"message":"ready","author":"agent","authored_at_seconds":10}}"#,
        ),
        (&["branch", "topic"], r#"{"Branch":{"create":"topic"}}"#),
        (
            &["switch", "-c", "topic"],
            r#"{"Switch":{"branch":"topic","create":true}}"#,
        ),
        (
            &["restore", "--source=HEAD", "--", "file"],
            r#"{"Restore":{"source":"HEAD","paths":["file"]}}"#,
        ),
        (
            &["reset", "--hard", "HEAD"],
            r#"{"Reset":{"target":"HEAD","mode":"Hard"}}"#,
        ),
        (&["merge", "topic"], r#"{"Merge":{"branch":"topic"}}"#),
        (&["rebase", "main"], r#"{"Rebase":{"branch":"main"}}"#),
        (&["stash", "push"], r#""StashPush""#),
        (&["stash", "pop"], r#""StashPop""#),
        (
            &["cherry-pick", "HEAD"],
            r#"{"CherryPick":{"object":"HEAD"}}"#,
        ),
        (&["revert", "HEAD"], r#"{"Revert":{"object":"HEAD"}}"#),
        (
            &["tag", "v1", "HEAD"],
            r#"{"Tag":{"name":"v1","target":"HEAD","delete":false}}"#,
        ),
        (&["blame", "file"], r#"{"Blame":{"path":"file"}}"#),
        (
            &["grep", "needle", "src"],
            r#"{"Grep":{"pattern":"needle","path":"src"}}"#,
        ),
        (&["clean", "-n"], r#"{"Clean":{"dry_run":true}}"#),
        (&["archive", "HEAD"], r#"{"Archive":{"object":"HEAD"}}"#),
        (
            &["bisect", "start", "bad", "good"],
            r#"{"Bisect":{"arguments":["start","bad","good"]}}"#,
        ),
        (
            &["checkout", "topic"],
            r#"{"Switch":{"branch":"topic","create":false}}"#,
        ),
        (
            &["checkout", "-b", "topic"],
            r#"{"Switch":{"branch":"topic","create":true}}"#,
        ),
        (
            &["checkout", "HEAD", "--", "src/lib.rs"],
            r#"{"Restore":{"source":"HEAD","paths":["src/lib.rs"]}}"#,
        ),
        (
            &["rev-parse", "--abbrev-ref", "HEAD"],
            r#"{"RevParse":{"argument":"--abbrev-ref HEAD"}}"#,
        ),
        (
            &["symbolic-ref", "--short", "HEAD"],
            r#"{"SymbolicRef":{"short":true}}"#,
        ),
        (
            &["merge-base", "HEAD", "topic"],
            r#"{"MergeBase":{"left":"HEAD","right":"topic"}}"#,
        ),
        (&["ls-files"], r#""LsFiles""#),
        (
            &["check-ignore", "--", "target/file"],
            r#"{"CheckIgnore":{"paths":["target/file"]}}"#,
        ),
    ];
    for (argv, expected) in cases {
        let argv = argv
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        let expected: GitCommand = serde_json::from_str(expected).expect("canonical command");
        assert_eq!(
            parse_argv::<MemoryGitCompatStoreError>(&argv, "default", 10).expect("supported argv"),
            expected,
            "argv: {argv:?}"
        );
    }
    assert_eq!(
        parse_argv::<MemoryGitCompatStoreError>(
            &["add".to_owned(), "--".to_owned(), "--all".to_owned()],
            "default",
            10
        )
        .expect("literal option-looking path"),
        GitCommand::Add {
            paths: vec!["--all".to_owned()]
        }
    );
}

#[test]
fn argv_rejects_semantically_unrepresentable_and_shell_composed_commands() {
    for command in [
        "clone",
        "fetch",
        "pull",
        "push",
        "remote",
        "gc",
        "repack",
        "cat-file",
        "hash-object",
        "init",
        "fsck",
        "prune",
        "pack-objects",
        "index-pack",
        "receive-pack",
        "upload-pack",
        "read-tree",
        "write-tree",
        "commit-tree",
        "update-index",
        "worktree",
        "submodule",
        "apply",
    ] {
        let argv = vec![command.to_owned()];
        assert!(
            matches!(
                parse_argv::<MemoryGitCompatStoreError>(&argv, "agent", 10),
                Err(GitCompatError::Unsupported { .. })
            ),
            "command should be explicitly unsupported: {command}"
        );
    }
    for argv in [
        vec!["status", "&&", "push"],
        vec!["status", "|", "cat"],
        vec!["status", ">result"],
        vec!["status", "$(push)"],
    ] {
        let argv = argv.into_iter().map(str::to_owned).collect::<Vec<_>>();
        assert!(matches!(
            parse_argv::<MemoryGitCompatStoreError>(&argv, "agent", 10),
            Err(GitCompatError::Unsupported { reason, .. }) if reason.contains("shell composition")
        ));
    }
    for argv in [
        vec!["status", "unexpected"],
        vec!["diff", "--stat"],
        vec!["reset", "HEAD", "path"],
        vec!["merge", "topic", "other"],
        vec!["restore", "--source", "HEAD"],
        vec!["clean", "-d"],
        vec!["clean", "-fd"],
        vec!["clean", "-nd"],
    ] {
        let argv = argv.into_iter().map(str::to_owned).collect::<Vec<_>>();
        assert!(parse_argv::<MemoryGitCompatStoreError>(&argv, "agent", 10).is_err());
    }
}
