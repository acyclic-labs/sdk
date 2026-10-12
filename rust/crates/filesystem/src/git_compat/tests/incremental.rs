use super::*;

#[tokio::test]
async fn incremental_capture_honors_unchanged_nested_ignore() {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "git-incremental-nested",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("workspace");
    let mut initial = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x65; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("transaction");
    initial
        .create_dir_all("/src", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("directory");
    initial
        .write_text("/src/.gitignore", "*.tmp\n")
        .await
        .expect("ignore");
    initial
        .write_text("/src/kept.txt", "before")
        .await
        .expect("kept");
    let previous_live = publish_fixture(initial).await;
    let previous_capture = capture_git_compatible_generation(
        &workspace,
        &GitIgnorePolicy::default(),
        &BTreeSet::new(),
        OperationId::from_bytes([0x66; 16]),
    )
    .await
    .expect("capture");
    let mut changed = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x67; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("transaction");
    changed
        .write_text("/src/new.tmp", "ignored")
        .await
        .expect("ignored");
    changed
        .write_text("/src/kept.txt", "after")
        .await
        .expect("edit");
    publish_fixture(changed).await;
    let captured = capture_git_compatible_generation_incremental(
        &workspace,
        &previous_live,
        &previous_capture.generation,
        &GitIgnorePolicy::default(),
        &previous_capture.tracked_paths,
        OperationId::from_bytes([0x68; 16]),
    )
    .await
    .expect("incremental capture");
    assert!(
        captured
            .generation
            .stat(
                "/src/new.tmp",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
    assert_eq!(
        captured
            .generation
            .read(
                "/src/kept.txt",
                64,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .expect("kept")
            .as_ref(),
        b"after"
    );
    assert!(!captured.tracked_paths.contains("src/new.tmp"));
}

#[tokio::test]
async fn newly_ignored_merge_paths_exclude_tracked_changes() {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "git-ignored-merge-paths",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("workspace");
    let mut initial = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x70; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("initial transaction");
    initial
        .create_dir_all("/sub", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("subdirectory");
    initial
        .write_text("/sub/.gitignore", "*.tmp\n")
        .await
        .expect("nested ignore");
    publish_fixture(initial).await;
    let before = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("before");
    workspace
        .write_text("/scratch.tmp", "child")
        .await
        .expect("child change");
    workspace
        .write_text("/sub/cache.tmp", "child")
        .await
        .expect("nested child change");
    let mut build = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x71; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("build transaction");
    build
        .create_dir_all("/build", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("build directory");
    build
        .write_text("/build/output.bin", "generated")
        .await
        .expect("generated file");
    publish_fixture(build).await;
    let after = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("after");
    let changes = before
        .diff_to(&after, 16, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)
        .expect("diff")
        .changed_paths(16, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)
        .expect("changed paths");
    let policy = GitIgnorePolicy::parse("*.tmp\nbuild/\n");
    assert_eq!(
        policy
            .newly_ignored_paths_at(&after, &changes, &BTreeSet::new())
            .await
            .expect("untracked policy"),
        BTreeSet::from([
            "/scratch.tmp".to_owned(),
            "/sub/cache.tmp".to_owned(),
            "/build/output.bin".to_owned(),
        ])
    );
    assert!(
        policy
            .newly_ignored_paths_at(
                &after,
                &changes,
                &BTreeSet::from([
                    "scratch.tmp".to_owned(),
                    "sub/cache.tmp".to_owned(),
                    "build/output.bin".to_owned(),
                ]),
            )
            .await
            .expect("tracked policy")
            .is_empty()
    );
}

#[cfg(feature = "local")]
#[tokio::test]
async fn incremental_capture_applies_only_changed_eligible_paths() {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "git-capture-incremental",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("workspace");
    let mut transaction = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x51; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("transaction");
    transaction
        .write_text("/kept.txt", "before")
        .await
        .expect("tracked file");
    transaction
        .write_text("/unchanged.txt", "same")
        .await
        .expect("unchanged file");
    transaction
        .write_text("/previous.tmp", "ignored before capture")
        .await
        .expect("ignored fixture");
    let previous_live = publish_fixture(transaction).await;
    let policy = GitIgnorePolicy::parse("*.tmp\n");
    let previous_capture = capture_git_compatible_generation(
        &workspace,
        &policy,
        &BTreeSet::new(),
        OperationId::from_bytes([0x52; 16]),
    )
    .await
    .expect("base capture");
    let state_root = tempfile::tempdir().expect("lineage state root");
    let state_store = crate::LocalCoreStateStore::new(state_root.path());
    let graph = crate::WorkspaceGraph::new(state_store.clone());
    previous_capture
        .authenticated_proof(OperationId::from_bytes([0x52; 16]), &graph)
        .await
        .expect("durable first capture lineage")
        .expect("filtered first capture proof");

    let mut transaction = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x53; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("transaction");
    transaction
        .write_text("/kept.txt", "after")
        .await
        .expect("edit");
    transaction
        .write_text("/new.tmp", "ignored")
        .await
        .expect("ignored addition");
    publish_fixture(transaction).await;

    let captured = capture_git_compatible_generation_incremental(
        &workspace,
        &previous_live,
        &previous_capture.generation,
        &policy,
        &previous_capture.tracked_paths,
        OperationId::from_bytes([0x54; 16]),
    )
    .await
    .expect("incremental capture");
    captured
        .authenticated_proof(OperationId::from_bytes([0x54; 16]), &graph)
        .await
        .expect("durable incremental lineage")
        .expect("filtered incremental proof");
    let lineage = graph
        .resolve(captured.generation.workspace_id())
        .await
        .expect("reopen incremental lineage");
    assert_eq!(
        lineage.parent_workspace_id,
        Some(previous_capture.generation.workspace_id()),
        "incremental capture must fork from the prior filtered capture"
    );
    assert_eq!(
        captured
            .generation
            .read(
                "/kept.txt",
                64,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .expect("edited file")
            .as_ref(),
        b"after"
    );
    assert_eq!(
        captured
            .generation
            .read(
                "/unchanged.txt",
                64,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .expect("unchanged file")
            .as_ref(),
        b"same"
    );
    assert!(
        captured
            .generation
            .stat("/new.tmp", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
    assert!(
        captured
            .generation
            .stat(
                "/previous.tmp",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
    assert_eq!(
        captured.tracked_paths,
        BTreeSet::from(["kept.txt".to_owned(), "unchanged.txt".to_owned()])
    );
}

#[tokio::test]
async fn nested_gitignore_change_forces_exact_capture() {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "git-capture-nested-ignore",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("workspace");
    let mut transaction = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x61; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("transaction");
    transaction
        .create_dir_all("/src", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("directory");
    transaction
        .write_text("/src/ignored.txt", "present before ignore")
        .await
        .expect("file");
    let previous_live = publish_fixture(transaction).await;
    let previous_capture = capture_git_compatible_generation(
        &workspace,
        &GitIgnorePolicy::default(),
        &BTreeSet::new(),
        OperationId::from_bytes([0x62; 16]),
    )
    .await
    .expect("base capture");

    let mut transaction = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x63; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("transaction");
    transaction
        .write_text("/src/.gitignore", "ignored.txt\n")
        .await
        .expect("nested ignore");
    publish_fixture(transaction).await;

    let captured = capture_git_compatible_generation_incremental(
        &workspace,
        &previous_live,
        &previous_capture.generation,
        &GitIgnorePolicy::parse("src/ignored.txt\n"),
        &BTreeSet::new(),
        OperationId::from_bytes([0x64; 16]),
    )
    .await
    .expect("capture");
    assert!(
        captured
            .generation
            .stat(
                "/src/ignored.txt",
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
                "/src/.gitignore",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_ok()
    );
}
