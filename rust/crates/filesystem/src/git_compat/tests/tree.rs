#[macro_use]
#[path = "../../../tests/support/unbounded.rs"]
mod unbounded;

use super::*;

use crate as acyclic_fs;

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
    let TransactionCommit::Committed(patched) = apply_git_patch(
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
        unbounded!(patched, read("/a.txt", 1_024))
            .expect("patched file")
            .as_ref(),
        b"one\nnew\n"
    );

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
            patch,
            operation,
            crate::PublicationPermit::Unrestricted,
        )
        .await
        .expect("retry"),
        TransactionCommit::AlreadyCommitted(recovered) if recovered.id() == patched.id()
    ));
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

#[tokio::test]
async fn patch_hunks_end_where_their_headers_say() {
    let workspace = memory_workspace("git-patch-extents").await;
    let mut transaction = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x38; 16])).await;
    for (path, text) in [
        ("/a.txt", "one\ntwo\nthree\n"),
        ("/q.sql", "select 1;\n-- drop me\nselect 2;\n"),
        ("/z.txt", "l1\nl2\nl3\n"),
        ("/dash.txt", "one\n-\n"),
    ] {
        transaction
            .write_text(path, text)
            .await
            .expect("fixture text");
    }
    publish_fixture(transaction).await;
    let patch = b"From 0000000000000000000000000000000000000000 Mon Sep 17 00:00:00 2001\n\
Subject: [PATCH] change\n\n---\n a.txt | 2 +-\n q.sql | 1 -\n z.txt | 1 +\n\n\
diff --git a/a.txt b/a.txt\nindex 4cb29ea..ddc897f 100644\n--- a/a.txt\n+++ b/a.txt\n\
@@ -1,3 +1,3 @@\n one\n-two\n+TWO\n three\n\
diff --git a/q.sql b/q.sql\nindex 0a85231..707b1d1 100644\n--- a/q.sql\n+++ b/q.sql\n\
@@ -1,3 +1,2 @@\n select 1;\n--- drop me\n select 2;\n\
diff --git a/z.txt b/z.txt\nindex 0970e47..ee7596b 100644\n--- a/z.txt\n+++ b/z.txt\n\
@@ -2,0 +3 @@ l2\n+NEW\n\
diff --git a/dash.txt b/dash.txt\n--- a/dash.txt\n+++ b/dash.txt\n\
@@ -1,2 +1 @@\n one\n--\n-- \n2.43.0\n\n";
    let TransactionCommit::Committed(patched) = apply_git_patch(
        &workspace,
        patch,
        IdempotencyKey::from_bytes([0x39; 16]),
        crate::PublicationPermit::Unrestricted,
    )
    .await
    .expect("apply") else {
        panic!("patch did not commit");
    };
    for (path, expected) in [
        ("/a.txt", "one\nTWO\nthree\n"),
        ("/q.sql", "select 1;\nselect 2;\n"),
        ("/z.txt", "l1\nl2\nNEW\nl3\n"),
        ("/dash.txt", "one\n"),
    ] {
        assert_eq!(
            unbounded!(patched, read(path, 1_024))
                .expect("patched file")
                .as_ref(),
            expected.as_bytes(),
            "{path}"
        );
    }
    for damaged in [
        b"--- a/a.txt\n+++ b/a.txt\n@@ -1,3 +1,3 @@\n one\n-TWO\n+two\n".as_slice(),
        b"--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1 @@\n one\n+extra\n-TWO\n",
        b"--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n one\n-TWO\n+two\n",
        b"--- a/dash.txt\n+++ b/dash.txt\n@@ -1 +1 @@\n one\n--\n",
        b"--- a/dash.txt\n+++ b/dash.txt\n@@ -1 +1 @@\n one\n--",
        b"--- a/dash.txt\n+++ b/dash.txt\n@@ -1 +1 @@\n one\n--  \n",
        b"--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-one\n+ONE\n\n-TWO\n+two\n",
        b"--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-one\n+ONE\nindex 1111111..2222222 100644\n-TWO\n+two\n",
    ] {
        assert!(matches!(
            apply_git_patch(
                &workspace,
                damaged,
                IdempotencyKey::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await,
            Err(GitPatchError::Invalid(_))
        ));
        assert_eq!(
            unbounded!(workspace, head()).expect("head after refusal").id(),
            patched.id(),
            "refused patch published a partial edit"
        );
    }
}

#[test]
fn portable_git_names_decode_canonical_windows_names() {
    let bytes = "source-λ.rs"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    let name = crate::kernel::LogicalName::new(crate::kernel::NameEncoding::WindowsUtf16Le, bytes)
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
