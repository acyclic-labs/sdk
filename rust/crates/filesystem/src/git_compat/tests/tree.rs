use super::*;

#[tokio::test]
async fn tree_walk_and_grep_are_generation_bound_and_portable() {
    let fs = crate::Fs::memory();
    let workspace = fs
        .create_workspace(
            "git-inspection",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("workspace");
    let mut transaction = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x32; 16]),
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
        .write_text("/src/a.txt", "one\nneedle\n")
        .await
        .expect("first file");
    transaction
        .write_text("/src/b.txt", "needle two\n")
        .await
        .expect("second file");
    transaction
        .write_text("/src/large.txt", "needle is too large")
        .await
        .expect("large file");
    transaction
        .write(
            "/src/binary.bin",
            bytes::Bytes::from_static(b"needle\xffbinary"),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("binary file");
    let generation = publish_fixture(transaction).await;
    let entries = walk_git_tree(&generation, Some("src"), 10)
        .await
        .expect("walk");
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        vec!["src/a.txt", "src/b.txt", "src/binary.bin", "src/large.txt"]
    );
    let result = grep_git_generation(&generation, "needle", Some("src"), 10, 16, 10)
        .await
        .expect("grep");
    assert!(result.truncated);
    assert_eq!(result.matches.len(), 2);
    assert_eq!(result.matches[0].line, 2);
    assert_eq!(result.matches[1].line, 1);

    let single = grep_git_generation(&generation, "needle", Some("src/a.txt"), 1, 16, 10)
        .await
        .expect("grep one file");
    assert!(!single.truncated);
    assert_eq!(single.matches.len(), 1);
    assert_eq!(single.matches[0].path, "src/a.txt");
    assert_eq!(single.matches[0].line, 2);
}

#[tokio::test]
async fn patch_application_is_atomic_conflict_checked_and_idempotent() {
    let fs = crate::Fs::memory();
    let workspace = fs
        .create_workspace(
            "git-patch",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("workspace");
    let mut transaction = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x33; 16]),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("transaction");
    transaction
        .write_text("/a.txt", "one\nold\n")
        .await
        .expect("fixture");
    transaction
        .write_text("/gone.txt", "gone")
        .await
        .expect("no-newline fixture");
    publish_fixture(transaction).await;
    let patch = b"--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,2 @@\n one\n-old\n+new\n";
    let operation = IdempotencyKey::from_bytes([0x34; 16]);
    let TransactionCommit::Committed(generation) = apply_git_patch(
        &workspace,
        patch,
        operation,
        crate::PublicationPermit::Unrestricted,
    )
    .await
    .expect("apply") else {
        panic!("patch did not commit");
    };
    assert_eq!(
        generation
            .read(
                "/a.txt",
                1_024,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .expect("patched file")
            .as_ref(),
        b"one\nnew\n"
    );
    assert!(matches!(
        apply_git_patch(
            &workspace,
            patch,
            operation,
            crate::PublicationPermit::Unrestricted
        )
        .await
        .expect("retry"),
        TransactionCommit::AlreadyCommitted(_)
    ));

    let conflicting = b"--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,2 @@\n one\n-old\n+other\n";
    assert!(matches!(
        apply_git_patch(
            &workspace,
            conflicting,
            IdempotencyKey::from_bytes([0x35; 16]),
            crate::PublicationPermit::Unrestricted
        )
        .await,
        Err(GitPatchError::Conflict { .. })
    ));

    let add_and_delete = b"--- /dev/null\n+++ b/nested/new.txt\n@@ -0,0 +1 @@\n+made\n--- a/gone.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-gone\n\\ No newline at end of file\n";
    let TransactionCommit::Committed(generation) = apply_git_patch(
        &workspace,
        add_and_delete,
        IdempotencyKey::from_bytes([0x36; 16]),
        crate::PublicationPermit::Unrestricted,
    )
    .await
    .expect("add and delete") else {
        panic!("add/delete patch did not commit");
    };
    assert_eq!(
        generation
            .read(
                "/nested/new.txt",
                1_024,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .expect("created file")
            .as_ref(),
        b"made\n"
    );
    assert!(
        generation
            .stat(
                "/gone.txt",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
    assert!(matches!(
        parse_unified_patch(b"--- C:/outside\n+++ C:/outside\n@@ -0,0 +1 @@\n+x\n"),
        Err(GitPatchError::Invalid(_))
    ));
}

#[test]
fn portable_git_names_decode_canonical_windows_names() {
    let bytes = "source-λ.rs"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    let name =
        crate::kernel::LogicalName::new(crate::kernel::NameEncoding::WindowsUtf16Le, bytes, 255)
            .expect("valid Windows name");
    assert_eq!(name.unicode_text().as_deref(), Some("source-λ.rs"));
}
