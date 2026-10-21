use super::*;

const LEGACY: &str = ".system/meta/a.json";

fn host_policy() -> GitIgnorePolicy {
    // A negation that would re-include the namespace if it were an ignore rule.
    GitIgnorePolicy::parse("!.system/\n").with_host_namespaces([".system"])
}

fn absent(path: &str) -> bool {
    !path.starts_with(".system")
}

async fn host_fixture(
    name: &str,
) -> (
    Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>,
    Generation<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>,
) {
    let workspace = memory_workspace(name).await;
    let mut transaction = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x71; 16])).await;
    for directory in ["/src", "/.system/meta"] {
        transaction
            .create_dir_all(directory, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("directory");
    }
    transaction
        .write_text("/src/a.txt", "needle\n")
        .await
        .expect("content");
    transaction
        .write_text(&format!("/{LEGACY}"), "needle\n")
        .await
        .expect("host metadata");
    let generation = publish_fixture(transaction).await;
    (workspace, generation)
}

async fn missing(
    generation: &Generation<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>,
) -> bool {
    generation
        .stat("/.system", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .is_err()
}

#[tokio::test]
async fn host_namespaces_stay_outside_every_repository_view() {
    let policy = host_policy();
    // A tracked set recorded before the namespace was host-owned.
    let legacy = BTreeSet::from([LEGACY.to_owned()]);
    assert!(!policy.eligible(LEGACY, false, true));
    assert!(!policy.eligible(".system", true, false));
    assert!(policy.eligible("src/a.txt", false, false));

    let (workspace, live) = host_fixture("git-host-namespaces").await;
    let captured = capture_git_compatible_generation(
        &workspace,
        &policy,
        &legacy,
        OperationId::from_bytes([0x72; 16]),
    )
    .await
    .expect("full capture");
    assert!(missing(&captured.generation).await);
    assert!(captured.tracked_paths.iter().all(|path| absent(path)));
    assert!(captured.tracked_paths.contains("src/a.txt"));

    // A capture that kept the namespace before the fix is rebuilt, not reused.
    let legacy_capture = capture_git_compatible_generation(
        &workspace,
        &GitIgnorePolicy::default(),
        &BTreeSet::new(),
        OperationId::from_bytes([0x73; 16]),
    )
    .await
    .expect("legacy capture");
    assert!(legacy_capture.tracked_paths.contains(LEGACY));
    let mut edit = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x74; 16])).await;
    edit.write_text("/src/a.txt", "needle again\n")
        .await
        .expect("edit");
    edit.write_text("/.system/meta/b.json", "metadata")
        .await
        .expect("more host metadata");
    let after = publish_fixture(edit).await;
    let incremental = capture_git_compatible_generation_incremental(
        &workspace,
        &live,
        &legacy_capture.generation,
        &policy,
        &legacy_capture.tracked_paths,
        OperationId::from_bytes([0x75; 16]),
    )
    .await
    .expect("incremental capture");
    assert!(missing(&incremental.generation).await);
    assert!(incremental.tracked_paths.iter().all(|path| absent(path)));

    // Host metadata never counts, even when an older commit tracked it.
    let counts = git_compatible_diff_counts(
        &live,
        &after,
        &policy,
        &legacy,
        100,
        &CancellationToken::new(),
    )
    .await
    .expect("diff");
    let unscoped = git_compatible_diff_counts(
        &live,
        &after,
        &GitIgnorePolicy::default(),
        &legacy,
        100,
        &CancellationToken::new(),
    )
    .await
    .expect("unscoped diff");
    assert_eq!(counts.file_changes + 1, unscoped.file_changes);
    assert_eq!(counts.binding_changes + 1, unscoped.binding_changes);
    assert!(counts.file_changes >= 1);

    let entries = walk_git_tree(&after, &policy, None, 100)
        .await
        .expect("walk");
    assert!(entries.iter().all(|entry| absent(&entry.path)));
    assert!(entries.iter().any(|entry| entry.path == "src/a.txt"));
    assert!(
        walk_git_tree(&after, &policy, Some(".system"), 100)
            .await
            .expect("walk host root")
            .is_empty()
    );

    let found = grep_git_generation(&after, &policy, "needle", None, 100, 1024, 100)
        .await
        .expect("grep");
    assert!(found.matches.iter().all(|found| absent(&found.path)));
    assert_eq!(found.matches.len(), 1);
    assert!(
        grep_git_generation(&after, &policy, "needle", Some(".system"), 100, 1024, 100)
            .await
            .expect("grep host root")
            .matches
            .is_empty()
    );

    let commit = GitCommit::new(
        tree(1),
        tree(1),
        None,
        legacy.clone(),
        Vec::new(),
        "agent",
        10,
        "legacy",
    );
    assert!(
        blame_git_generations(&[(commit, live)], &policy, LEGACY, 1024)
            .await
            .expect("blame")
            .is_empty()
    );
}
