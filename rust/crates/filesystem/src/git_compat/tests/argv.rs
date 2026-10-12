use super::*;

#[tokio::test]
async fn argv_covers_local_history_and_rejects_transport() {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    assert!(matches!(
        repository
            .execute_argv(&["reset".to_owned(), "--hard".to_owned(), "HEAD".to_owned()], tree(1), "agent", 10,)
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
        repository.execute_argv(&["clean".to_owned(), "--dry-run".to_owned()], tree(1), "agent", 10,).await,
        Ok(GitCommandOutput::Prepared { action: GitFilesystemAction::Clean { dry_run: true, .. }, .. })
    ));
    assert!(matches!(
        repository.execute_argv(&["push".to_owned()], tree(1), "agent", 10,).await,
        Err(GitCompatError::Unsupported { .. })
    ));
}

#[test]
fn argv_parser_has_an_explicit_command_conformance_matrix() {
    let cases = [
        ((&["status", "--short"] as &[&str]), GitCommand::Status),
        ((&["diff", "--staged"] as &[&str]), GitCommand::Diff { cached: true }),
        ((&["log", "--max-count=7"] as &[&str]), GitCommand::Log { maximum: 7 }),
        ((&["show", "HEAD"] as &[&str]), GitCommand::Show { object: Some(GitObjectName("HEAD".to_owned())) }),
        ((&["add", "--all"] as &[&str]), GitCommand::Add { paths: Vec::new() }),
        (
            (&["commit", "--message=ready", "--author=agent"] as &[&str]),
            GitCommand::Commit { message: "ready".to_owned(), author: "agent".to_owned(), authored_at_seconds: 10 },
        ),
        ((&["branch", "topic"] as &[&str]), GitCommand::Branch { create: Some("topic".to_owned()) }),
        ((&["switch", "-c", "topic"] as &[&str]), GitCommand::Switch { branch: "topic".to_owned(), create: true }),
        (
            (&["restore", "--source=HEAD", "--", "file"] as &[&str]),
            GitCommand::Restore { source: Some(GitObjectName("HEAD".to_owned())), paths: vec!["file".to_owned()] },
        ),
        (
            (&["reset", "--hard", "HEAD"] as &[&str]),
            GitCommand::Reset { target: GitObjectName("HEAD".to_owned()), mode: GitResetMode::Hard },
        ),
        ((&["merge", "topic"] as &[&str]), GitCommand::Merge { branch: "topic".to_owned() }),
        ((&["rebase", "main"] as &[&str]), GitCommand::Rebase { branch: "main".to_owned() }),
        ((&["stash", "push"] as &[&str]), GitCommand::StashPush),
        ((&["stash", "pop"] as &[&str]), GitCommand::StashPop),
        ((&["cherry-pick", "HEAD"] as &[&str]), GitCommand::CherryPick { object: GitObjectName("HEAD".to_owned()) }),
        ((&["revert", "HEAD"] as &[&str]), GitCommand::Revert { object: GitObjectName("HEAD".to_owned()) }),
        (
            (&["tag", "v1", "HEAD"] as &[&str]),
            GitCommand::Tag {
                name: Some("v1".to_owned()),
                target: Some(GitObjectName("HEAD".to_owned())),
                delete: false,
            },
        ),
        ((&["blame", "file"] as &[&str]), GitCommand::Blame { path: "file".to_owned() }),
        (
            (&["grep", "needle", "src"] as &[&str]),
            GitCommand::Grep { pattern: "needle".to_owned(), path: Some("src".to_owned()) },
        ),
        ((&["clean", "-n"] as &[&str]), GitCommand::Clean { dry_run: true }),
        ((&["archive", "HEAD"] as &[&str]), GitCommand::Archive { object: Some(GitObjectName("HEAD".to_owned())) }),
        (
            (&["bisect", "start", "bad", "good"] as &[&str]),
            GitCommand::Bisect { arguments: vec!["start".to_owned(), "bad".to_owned(), "good".to_owned()] },
        ),
        ((&["checkout", "topic"] as &[&str]), GitCommand::Switch { branch: "topic".to_owned(), create: false }),
        ((&["checkout", "-b", "topic"] as &[&str]), GitCommand::Switch { branch: "topic".to_owned(), create: true }),
        (
            (&["checkout", "HEAD", "--", "src/lib.rs"] as &[&str]),
            GitCommand::Restore {
                source: Some(GitObjectName("HEAD".to_owned())),
                paths: vec!["src/lib.rs".to_owned()],
            },
        ),
        (
            (&["rev-parse", "--abbrev-ref", "HEAD"] as &[&str]),
            GitCommand::RevParse { argument: "--abbrev-ref HEAD".to_owned() },
        ),
        ((&["symbolic-ref", "--short", "HEAD"] as &[&str]), GitCommand::SymbolicRef { short: true }),
        (
            (&["merge-base", "HEAD", "topic"] as &[&str]),
            GitCommand::MergeBase { left: GitObjectName("HEAD".to_owned()), right: GitObjectName("topic".to_owned()) },
        ),
        ((&["ls-files"] as &[&str]), GitCommand::LsFiles),
        (
            (&["check-ignore", "--", "target/file"] as &[&str]),
            GitCommand::CheckIgnore { paths: vec!["target/file".to_owned()] },
        ),
    ];
    for (argv, expected) in cases {
        let argv = argv.iter().map(|value| (*value).to_owned()).collect::<Vec<_>>();
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
            10,
        )
        .expect("literal option-looking path"),
        GitCommand::Add { paths: vec!["--all".to_owned()] }
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
    for argv in
        [vec!["status", "&&", "push"], vec!["status", "|", "cat"], vec!["status", ">result"], vec!["status", "$(push)"]]
    {
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
