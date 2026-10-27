#[macro_use]
#[path = "../../../tests/support/unbounded.rs"]
mod unbounded;

use super::*;

use crate as acyclic_fs;

#[tokio::test]
async fn tree_walk_and_grep_are_generation_bound_and_portable() {
    let workspace = memory_workspace("git-inspection").await;
    let mut transaction = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x32; 16])).await;
    unbounded!(transaction, create_dir_all("/src")).expect("directory");
    for (path, text) in [
        ("/src/a.txt", "one\nneedle\n"),
        ("/src/b.txt", "needle two\n"),
        ("/src/large.txt", "needle is too large"),
    ] {
        transaction
            .write_text(path, text)
            .await
            .expect("fixture text");
    }
    unbounded!(
        transaction,
        write(
            "/src/binary.bin",
            bytes::Bytes::from_static(b"needle\xffbinary")
        )
    )
    .expect("binary file");
    let generation = publish_fixture(transaction).await;
    let entries = walk_git_tree(&generation, &GitIgnorePolicy::default(), Some("src"), 10)
        .await
        .expect("walk");
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        vec!["src/a.txt", "src/b.txt", "src/binary.bin", "src/large.txt"]
    );
    let result = grep_git_generation(
        &generation,
        &GitIgnorePolicy::default(),
        "needle",
        Some("src"),
        10,
        16,
        10,
    )
    .await
    .expect("grep");
    assert!(result.truncated);
    assert_eq!(result.matches.len(), 2);
    assert_eq!(result.matches[0].line, 2);
    assert_eq!(result.matches[1].line, 1);

    let single = grep_git_generation(
        &generation,
        &GitIgnorePolicy::default(),
        "needle",
        Some("src/a.txt"),
        1,
        16,
        10,
    )
    .await
    .expect("grep one file");
    assert!(!single.truncated);
    assert_eq!(single.matches.len(), 1);
    assert_eq!(single.matches[0].path, "src/a.txt");
    assert_eq!(single.matches[0].line, 2);
}

#[tokio::test]
async fn patch_application_is_atomic_conflict_checked_and_idempotent() {
    let workspace = memory_workspace("git-patch").await;
    let mut transaction = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x33; 16])).await;
    for (path, text) in [("/a.txt", "one\nold\n"), ("/gone.txt", "gone")] {
        transaction
            .write_text(path, text)
            .await
            .expect("fixture text");
    }
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
        unbounded!(generation, read("/a.txt", 1_024))
            .expect("patched file")
            .as_ref(),
        b"one\nnew\n"
    );
    assert!(matches!(
        apply_git_patch(
            &workspace,
            patch,
            operation,
            crate::PublicationPermit::Unrestricted,
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
            crate::PublicationPermit::Unrestricted,
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
        unbounded!(generation, read("/nested/new.txt", 1_024))
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
            .is_err()
    );
    assert!(matches!(
        apply_git_patch(
            &workspace,
            b"--- C:/outside\n+++ C:/outside\n@@ -0,0 +1 @@\n+x\n",
            IdempotencyKey::from_bytes([0x37; 16]),
            crate::PublicationPermit::Unrestricted
        )
        .await,
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
    assert_eq!(
        name.unicode_text(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .ok()
        .map(|receipt| receipt.value)
        .as_deref(),
        Some("source-λ.rs")
    );
}

#[tokio::test]
async fn independent_git_patches_share_one_append_and_recover_each_operation() {
    use acyclic_stream::StreamProvider;
    use futures::TryStreamExt;
    use std::sync::Arc;

    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let fs = Fs::new(
        crate::StreamAuthorityStore::new(Arc::clone(&stream)),
        crate::LogicalObjectStore::new(Arc::new(objects), bucket),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let workspace = unbounded!(fs, create_workspace("git-group")).expect("workspace");
    let mut seed =
        unbounded!(workspace, begin_transaction(IdempotencyKey::new())).expect("seed transaction");
    for name in ["/a.txt", "/b.txt"] {
        seed.write_text(name, "old\n").await.expect("seed file");
    }
    let outcome = seed
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            |_| {
                std::future::ready(Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                    crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    },
                ))
            },
        )
        .await
        .expect("seed commit")
        .value;
    assert!(matches!(outcome, TransactionCommit::Committed(_)));
    let records_path = acyclic_stream::StreamPath::new(format!(
        "fs/authorities/{}/records",
        hex::encode(crate::kernel::volume_authority_id(workspace.volume.id()).into_bytes())
    ))
    .expect("authority records");
    let before: Vec<_> = stream
        .read(acyclic_stream::ReadRequest {
            path: records_path.clone(),
            from: 1,
            limit: 1024,
        })
        .await
        .expect("before records")
        .try_collect()
        .await
        .expect("before rows");
    let patches = [
        b"--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-old\n+left\n".as_slice(),
        b"--- a/b.txt\n+++ b/b.txt\n@@ -1 +1 @@\n-old\n+right\n".as_slice(),
    ];
    let operations = [IdempotencyKey::new(), IdempotencyKey::new()];
    let (left, right) = futures::join!(
        apply_git_patch(
            &workspace,
            patches[0],
            operations[0],
            crate::PublicationPermit::Unrestricted
        ),
        apply_git_patch(
            &workspace,
            patches[1],
            operations[1],
            crate::PublicationPermit::Unrestricted
        ),
    );
    for outcome in [left, right] {
        assert!(matches!(
            outcome.expect("patch"),
            TransactionCommit::Committed(_)
        ));
    }
    let after: Vec<_> = stream
        .read(acyclic_stream::ReadRequest {
            path: records_path,
            from: before.len() as u64 + 1,
            limit: 2,
        })
        .await
        .expect("batch records")
        .try_collect()
        .await
        .expect("batch rows");
    assert_eq!(after.len(), 2);
    assert_eq!(
        after[0].commit_id, after[1].commit_id,
        "Git adapter bypassed group publication"
    );
    for (index, (name, expected)) in [
        ("/a.txt", b"left\n".as_slice()),
        ("/b.txt", b"right\n".as_slice()),
    ]
    .into_iter()
    .enumerate()
    {
        let read = unbounded!(workspace, read(name, 16)).expect("final file");
        assert_eq!(read.as_ref(), expected);
        assert!(matches!(
            apply_git_patch(
                &workspace,
                patches[index],
                operations[index],
                crate::PublicationPermit::Unrestricted
            )
            .await
            .expect("original retry"),
            TransactionCommit::AlreadyCommitted(_)
        ));
    }
}

#[tokio::test]
async fn git_patch_preimage_cannot_overwrite_a_newer_transaction_base() {
    let workspace = memory_workspace("git-preimage-fence").await;
    let mut seed = begin_fixture(&workspace, IdempotencyKey::new()).await;
    seed.write_text("/a.txt", "old\n").await.expect("seed");
    let preimage = publish_fixture(seed).await;
    let mut other = begin_fixture(&workspace, IdempotencyKey::new()).await;
    other
        .write_text("/a.txt", "other\n")
        .await
        .expect("other write");
    publish_fixture(other).await;
    let outcome = super::super::patch::apply_git_patch_at(
        &workspace,
        b"--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-old\n+patch\n",
        &preimage,
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
    )
    .await
    .expect("stale patch outcome");
    assert!(
        !matches!(
            outcome,
            TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_)
        ),
        "stale Git preimage overwrote the newer base"
    );
    assert_eq!(
        unbounded!(workspace, read("/a.txt", 16))
            .expect("current file")
            .as_ref(),
        b"other\n"
    );
}
