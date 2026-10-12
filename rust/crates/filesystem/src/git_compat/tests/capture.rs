use super::*;

#[tokio::test]
async fn persisted_foreign_commit_tree_needs_capture_provenance() {
    let store = MemoryGitCompatStore::default();
    let mut state = GitCompatState::new("main", workspace());
    let foreign = WorkspaceId::derive(
        [8; 16],
        &WorkspaceName::new(
            "foreign-persisted-tree",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .expect("workspace name")
        .value,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .expect("workspace identity")
    .value;
    let commit = GitCommit::new(
        GitTreeRef::exact(foreign, generation(7)),
        tree(1),
        None,
        BTreeSet::from(["foreign.txt".to_owned()]),
        Vec::new(),
        "agent",
        10,
        "forged tree",
    );
    state.revision = 1;
    state.branches.get_mut("main").expect("branch").head = Some(commit.id);
    state
        .branches
        .get_mut("main")
        .expect("branch")
        .tracked_paths = commit.tracked_paths.clone();
    state.commits.insert(commit.id, commit);
    store
        .states
        .0
        .lock()
        .expect("state lock")
        .insert(workspace(), state);
    let repository = GitCompatRepository::new(workspace(), store);
    assert!(matches!(
        repository.execute(GitCommand::Status, tree(1)).await,
        Err(GitCompatError::InvalidState)
    ));
}

#[tokio::test]
async fn capture_omits_newly_ignored_paths_but_keeps_tracked_descendants() {
    let workspace = memory_workspace("git-capture-policy").await;
    let mut transaction = begin_fixture(&workspace, IdempotencyKey::new()).await;
    transaction
        .create_dir_all(
            "/target/nested",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("directories")
        .value;
    transaction
        .write_text("/target/drop.bin", "drop")
        .await
        .expect("ignored file");
    transaction
        .write_text("/target/nested/keep.txt", "keep")
        .await
        .expect("tracked file");
    publish_fixture(transaction).await;
    let captured = capture_git_compatible_generation(
        &workspace,
        &GitIgnorePolicy::parse("target/\n"),
        &BTreeSet::from(["target/nested/keep.txt".to_owned()]),
        OperationId::from_bytes([0x31; 16]),
    )
    .await
    .expect("capture");
    assert!(
        captured
            .generation
            .stat(
                "/target/drop.bin",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
    assert!(
        captured
            .generation
            .stat(
                "/target/nested/keep.txt",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_ok()
    );
    assert_eq!(
        captured.tracked_paths,
        BTreeSet::from(["target/nested/keep.txt".to_owned()])
    );
    let recovered = capture_git_compatible_generation(
        &workspace,
        &GitIgnorePolicy::parse("target/\n"),
        &BTreeSet::from(["target/nested/keep.txt".to_owned()]),
        OperationId::from_bytes([0x31; 16]),
    )
    .await
    .expect("recover capture");
    assert_eq!(recovered.generation.id(), captured.generation.id());
}

#[tokio::test]
async fn capture_at_generation_excludes_later_workspace_updates() {
    let workspace = memory_workspace("git-capture-pinned-generation").await;
    let mut initial = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x35; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("initial transaction")
        .value;
    initial
        .write_text("/published.txt", "published")
        .await
        .expect("published file");
    let published = publish_fixture(initial).await;
    let mut later = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x36; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("later transaction")
        .value;
    later
        .write_text("/later.txt", "later")
        .await
        .expect("later file");
    publish_fixture(later).await;

    let captured = capture_git_compatible_generation_at(
        &workspace,
        published.id(),
        &GitIgnorePolicy::default(),
        &BTreeSet::new(),
        OperationId::from_bytes([0x37; 16]),
    )
    .await
    .expect("pinned capture");
    assert!(
        captured
            .generation
            .stat(
                "/published.txt",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_ok()
    );
    assert!(
        captured
            .generation
            .stat(
                "/later.txt",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
}

#[tokio::test]
async fn capture_preserves_metadata_and_hard_links_through_public_fork_primitives() {
    let workspace = memory_workspace("git-capture-records").await;
    let mut transaction = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x41; 16])).await;
    transaction
        .write_text("/source.txt", "linked")
        .await
        .expect("source");
    transaction
        .set_metadata(
            "/source.txt",
            FileMetadata {
                posix_mode: MetadataField::Value(0o100_640),
                ..FileMetadata::default()
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("metadata")
        .value;
    transaction
        .hard_link(
            "/source.txt",
            "/linked.txt",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("hard link")
        .value;
    transaction
        .create_dir_all("/ignored", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("ignored directory")
        .value;
    transaction
        .write_text("/ignored/drop.txt", "drop")
        .await
        .expect("ignored file");
    publish_fixture(transaction).await;

    let captured = capture_git_compatible_generation(
        &workspace,
        &GitIgnorePolicy::parse("ignored/\n"),
        &BTreeSet::new(),
        OperationId::from_bytes([0x42; 16]),
    )
    .await
    .expect("capture");
    let source = captured
        .generation
        .stat(
            "/source.txt",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("source stat")
        .value;
    let linked = captured
        .generation
        .stat(
            "/linked.txt",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("link stat")
        .value;
    assert_eq!(source.file_id, linked.file_id);
    assert_eq!(source.link_count, 2);
    assert_eq!(source.metadata.posix_mode, Some(0o100_640));
    assert!(
        captured
            .generation
            .stat("/ignored", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
}

#[tokio::test]
async fn exact_capture_inherits_nested_ignore_rules_with_local_negation() {
    let workspace = memory_workspace("git-capture-nested-policy").await;
    let mut transaction = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x49; 16])).await;
    transaction
        .create_dir_all("/src", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("directory")
        .value;
    transaction
        .write_text("/src/.gitignore", "*.tmp\n!keep.tmp\n")
        .await
        .expect("nested policy");
    transaction
        .write_text("/src/drop.tmp", "drop")
        .await
        .expect("ignored file");
    transaction
        .write_text("/src/keep.tmp", "keep")
        .await
        .expect("negated file");
    transaction
        .write_text("/outside.tmp", "outside")
        .await
        .expect("outside file");
    publish_fixture(transaction).await;

    let captured = capture_git_compatible_generation(
        &workspace,
        &GitIgnorePolicy::default(),
        &BTreeSet::new(),
        OperationId::from_bytes([0x4a; 16]),
    )
    .await
    .expect("capture");
    assert!(
        captured
            .generation
            .stat(
                "/src/drop.tmp",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
    assert!(
        captured
            .generation
            .stat(
                "/src/keep.tmp",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_ok()
    );
    assert!(
        captured
            .generation
            .stat(
                "/outside.tmp",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_ok()
    );
}

#[tokio::test]
async fn diff_uses_nested_ignores_without_creating_a_capture_workspace() {
    let workspace = memory_workspace("git-diff-policy").await;
    let before = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("baseline")
        .value;
    let mut transaction = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x4b; 16])).await;
    transaction
        .create_dir_all("/src", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("directory")
        .value;
    transaction
        .write_text("/src/.gitignore", "*.tmp\n!keep.tmp\n")
        .await
        .expect("ignore policy");
    transaction
        .write_text("/src/drop.tmp", "drop")
        .await
        .expect("ignored");
    transaction
        .write_text("/src/keep.tmp", "keep")
        .await
        .expect("kept");
    publish_fixture(transaction).await;
    let after = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("changed generation")
        .value;

    let counts = git_compatible_diff_counts(
        &before,
        &after,
        &BTreeSet::new(),
        100,
        &CancellationToken::new(),
    )
    .await
    .expect("read-only diff");
    assert_eq!(counts.binding_changes, 2);
    assert_eq!(counts.file_changes, 2);
    assert_eq!(
        workspace
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("unchanged head")
            .value
            .id(),
        after.id()
    );

    let tracked_counts = git_compatible_diff_counts(
        &before,
        &after,
        &BTreeSet::from(["src/drop.tmp".to_owned()]),
        100,
        &CancellationToken::new(),
    )
    .await
    .expect("tracked override");
    assert_eq!(tracked_counts.binding_changes, 3);
    assert_eq!(tracked_counts.file_changes, 3);
}

#[tokio::test]
async fn diff_uses_the_source_ignore_policy_for_deleted_paths() {
    let workspace = memory_workspace("git-diff-deleted-policy").await;
    let mut initial = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x4c; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("initial transaction")
        .value;
    initial
        .write_text("/drop.tmp", "old")
        .await
        .expect("source file");
    publish_fixture(initial).await;
    let before = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("before")
        .value;

    let mut changed = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x4d; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("changed transaction")
        .value;
    changed
        .write_text("/.gitignore", "*.tmp\n")
        .await
        .expect("new ignore policy");
    changed
        .remove(
            "/drop.tmp",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("delete old file")
        .value;
    publish_fixture(changed).await;
    let after = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("after")
        .value;

    let counts = git_compatible_diff_counts(
        &before,
        &after,
        &BTreeSet::new(),
        100,
        &CancellationToken::new(),
    )
    .await
    .expect("diff");
    assert_eq!(counts.binding_changes, 2);
    assert_eq!(counts.file_changes, 2);
}

#[tokio::test]
async fn ignored_ancestor_cannot_be_reincluded_by_nested_negation() {
    let workspace = memory_workspace("git-ignored-parent").await;
    let before = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("before")
        .value;
    let mut transaction = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x4e; 16])).await;
    transaction
        .write_text("/.gitignore", "build/\n")
        .await
        .expect("root ignore");
    transaction
        .create_dir_all("/build", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("directory")
        .value;
    transaction
        .write_text("/build/.gitignore", "!keep.txt\n")
        .await
        .expect("nested negation");
    transaction
        .write_text("/build/keep.txt", "ignored")
        .await
        .expect("file");
    publish_fixture(transaction).await;
    let after = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("after")
        .value;

    let counts = git_compatible_diff_counts(
        &before,
        &after,
        &BTreeSet::new(),
        100,
        &CancellationToken::new(),
    )
    .await
    .expect("diff");
    assert_eq!(counts.file_changes, 1, "only root .gitignore is eligible");
    assert_eq!(counts.binding_changes, 1);
    let captured = capture_git_compatible_generation(
        &workspace,
        &GitIgnorePolicy::parse("build/\n"),
        &BTreeSet::new(),
        OperationId::from_bytes([0x4f; 16]),
    )
    .await
    .expect("capture");
    assert!(
        captured
            .generation
            .stat(
                "/build/keep.txt",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
}

#[tokio::test]
async fn filtered_capture_retry_keeps_original_fork_generation() {
    let workspace = memory_workspace("git-capture-retry").await;
    let mut transaction = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x69; 16])).await;
    transaction
        .write_text("/keep.txt", "keep")
        .await
        .expect("kept");
    transaction
        .write_text("/drop.tmp", "drop")
        .await
        .expect("ignored");
    publish_fixture(transaction).await;
    let operation_id = OperationId::from_bytes([0x6a; 16]);
    let policy = GitIgnorePolicy::parse("*.tmp\n");
    let first =
        capture_git_compatible_generation(&workspace, &policy, &BTreeSet::new(), operation_id)
            .await
            .expect("first capture");
    let retry =
        capture_git_compatible_generation(&workspace, &policy, &BTreeSet::new(), operation_id)
            .await
            .expect("retry");
    assert_eq!(first.generation.id(), retry.generation.id());
    let graph = crate::WorkspaceGraph::new(crate::MemoryWorkspaceLineageStore::new());
    let proof = first
        .authenticated_proof(operation_id, &graph)
        .await
        .expect("first proof")
        .expect("filtered proof");
    let replay = retry
        .authenticated_proof(operation_id, &graph)
        .await
        .expect("retry proof");
    assert_eq!(Some(proof.clone()), replay);
    assert_ne!(proof.initial_generation, first.generation.id());
}

#[test]
fn ignore_policy_keeps_tracked_paths_and_honors_last_match() {
    let policy = GitIgnorePolicy::parse("target/\n*.log\n!important.log\n");
    assert!(!policy.eligible("target", true, false));
    assert!(!policy.eligible("target/cache.bin", false, false));
    assert!(!policy.eligible("nested/target/cache.bin", false, false));
    assert!(!policy.eligible("nested/debug.log", false, false));
    assert!(policy.eligible("nested/important.log", false, false));
    assert!(policy.eligible("target/cache", false, true));
}
