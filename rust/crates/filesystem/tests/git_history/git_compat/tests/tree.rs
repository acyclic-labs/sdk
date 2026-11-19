use acyclic_fs::kernel::{LogicalName, NameEncoding};
use acyclic_fs::{
    CancellationToken, Fs, GitGrepMatch, GitPatchError, IdempotencyKey, PublicationPermit,
    TransactionCommit, apply_git_patch, grep_git_generation, walk_git_tree,
};
use acyclic_fs_work::performance::WorkBudget;
use acyclic_stream::OwnedBytes;

use crate::git_history::transaction::commit;

#[tokio::test]
async fn tree_walk_and_grep_are_generation_bound_and_portable()
-> Result<(), Box<dyn std::error::Error>> {
    let workspace = unbounded!(Fs::memory(), create_workspace("git-inspection"))?;
    let mut transaction = unbounded!(
        workspace,
        begin_transaction(IdempotencyKey::from_bytes([0x32; 16]))
    )?;
    unbounded!(transaction, create_dir_all("/src"))?;
    for (path, text) in [
        ("/src/a.txt", "one\nneedle\n"),
        ("/src/b.txt", "needle two\n"),
        ("/src/large.txt", "needle is too large"),
    ] {
        transaction.write_text(path, text).await?;
    }
    unbounded!(
        transaction,
        write(
            "/src/binary.bin",
            OwnedBytes::from_static(b"needle\xffbinary")
        )
    )?;
    let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
        return Err("grep fixture did not commit".into());
    };
    let entries = walk_git_tree(&generation, Some("src"), 10).await?;
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        vec!["src/a.txt", "src/b.txt", "src/binary.bin", "src/large.txt"]
    );
    let result = grep_git_generation(&generation, "needle", Some("src"), 10, 16, 10).await?;
    assert!(result.truncated);
    let [first, second] = result.matches.as_slice() else {
        return Err("expected two grep matches".into());
    };
    assert_eq!(first.line, 2);
    assert_eq!(second.line, 1);

    let single = grep_git_generation(&generation, "needle", Some("src/a.txt"), 1, 16, 10).await?;
    assert!(!single.truncated);
    let [hit] = single.matches.as_slice() else {
        return Err("expected one grep match".into());
    };
    assert_eq!(hit.path, "src/a.txt");
    assert_eq!(hit.line, 2);
    Ok(())
}

#[tokio::test]
async fn unbounded_grep_matches_utf8_lines_across_physical_read_chunks()
-> Result<(), Box<dyn std::error::Error>> {
    let workspace = unbounded!(Fs::memory(), create_workspace("git-chunked-grep"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let text = format!(
        "{}needle\r\nnext éneedle\r\nlast needle\r",
        "é".repeat(32_766)
    );
    transaction.write_text("/text", &text).await?;
    let mut binary = b"needle\n".to_vec();
    binary.extend(vec![b'x'; 65_536]);
    binary.push(0xff);
    unbounded!(transaction, write("/binary", OwnedBytes::from(binary)))?;
    let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
        return Err("grep fixture did not commit".into());
    };
    let found = grep_git_generation(&generation, "needle", None, None, None, None).await?;
    let expected: Vec<_> = text
        .lines()
        .enumerate()
        .filter(|(_, line)| line.contains("needle"))
        .map(|(index, line)| GitGrepMatch {
            path: "text".to_owned(),
            line: index as u64 + 1,
            text: line.to_owned(),
        })
        .collect();
    assert_eq!(found.matches, expected);
    assert!(!found.truncated);
    let bounded = grep_git_generation(&generation, "needle", None, None, None, 1).await?;
    assert_eq!(
        bounded.matches,
        expected.into_iter().take(1).collect::<Vec<_>>()
    );
    assert!(bounded.truncated);
    assert_eq!(
        unbounded!(generation, read("/text", None))?.as_ref(),
        text.as_bytes()
    );
    assert!(unbounded!(generation, read("/text", 16)).is_err());
    Ok(())
}

#[tokio::test]
async fn patch_application_is_atomic_conflict_checked_and_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let workspace = unbounded!(Fs::memory(), create_workspace("git-patch"))?;
    let mut transaction = unbounded!(
        workspace,
        begin_transaction(IdempotencyKey::from_bytes([0x33; 16]))
    )?;
    for (path, text) in [("/a.txt", "one\nold\n"), ("/gone.txt", "gone")] {
        transaction.write_text(path, text).await?;
    }
    let TransactionCommit::Committed(_) = commit(&mut transaction).await? else {
        return Err("patch fixture did not commit".into());
    };
    let patch = b"--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,2 @@\n one\n-old\n+new\n";
    let operation = IdempotencyKey::from_bytes([0x34; 16]);
    let TransactionCommit::Committed(patched) = apply_git_patch(
        &workspace,
        patch,
        operation,
        PublicationPermit::Unrestricted,
    )
    .await?
    else {
        return Err("patch did not commit".into());
    };
    assert_eq!(
        unbounded!(patched, read("/a.txt", 1_024))?.as_ref(),
        b"one\nnew\n"
    );

    let conflicting = b"--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,2 @@\n one\n-old\n+other\n";
    assert!(matches!(
        apply_git_patch(
            &workspace,
            conflicting,
            IdempotencyKey::from_bytes([0x35; 16]),
            PublicationPermit::Unrestricted,
        )
        .await,
        Err(GitPatchError::Conflict { .. })
    ));

    let add_and_delete = b"--- /dev/null\n+++ b/nested/new.txt\n@@ -0,0 +1 @@\n+made\n--- a/gone.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-gone\n\\ No newline at end of file\n";
    let TransactionCommit::Committed(generation) = apply_git_patch(
        &workspace,
        add_and_delete,
        IdempotencyKey::from_bytes([0x36; 16]),
        PublicationPermit::Unrestricted,
    )
    .await?
    else {
        return Err("add/delete patch did not commit".into());
    };
    assert_eq!(
        unbounded!(generation, read("/nested/new.txt", 1_024))?.as_ref(),
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
            PublicationPermit::Unrestricted,
        )
        .await?,
        TransactionCommit::AlreadyCommitted(recovered) if recovered.id() == patched.id()
    ));
    assert!(matches!(
        apply_git_patch(
            &workspace,
            b"--- C:/outside\n+++ C:/outside\n@@ -0,0 +1 @@\n+x\n",
            IdempotencyKey::from_bytes([0x37; 16]),
            PublicationPermit::Unrestricted
        )
        .await,
        Err(GitPatchError::Invalid(_))
    ));
    Ok(())
}

#[tokio::test]
async fn patch_hunks_end_where_their_headers_say() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = unbounded!(Fs::memory(), create_workspace("git-patch-extents"))?;
    let mut transaction = unbounded!(
        workspace,
        begin_transaction(IdempotencyKey::from_bytes([0x38; 16]))
    )?;
    for (path, text) in [
        ("/a.txt", "one\ntwo\nthree\n"),
        ("/q.sql", "select 1;\n-- drop me\nselect 2;\n"),
        ("/z.txt", "l1\nl2\nl3\n"),
        ("/dash.txt", "one\n-\n"),
    ] {
        transaction.write_text(path, text).await?;
    }
    let TransactionCommit::Committed(_) = commit(&mut transaction).await? else {
        return Err("patch fixture did not commit".into());
    };
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
        PublicationPermit::Unrestricted,
    )
    .await?
    else {
        return Err("patch did not commit".into());
    };
    for (path, expected) in [
        ("/a.txt", "one\nTWO\nthree\n"),
        ("/q.sql", "select 1;\nselect 2;\n"),
        ("/z.txt", "l1\nl2\nNEW\nl3\n"),
        ("/dash.txt", "one\n"),
    ] {
        assert_eq!(
            unbounded!(patched, read(path, 1_024))?.as_ref(),
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
                PublicationPermit::Unrestricted,
            )
            .await,
            Err(GitPatchError::Invalid(_))
        ));
        assert_eq!(
            unbounded!(workspace, head())?.id(),
            patched.id(),
            "refused patch published a partial edit"
        );
    }
    Ok(())
}

#[test]
fn portable_git_names_decode_canonical_windows_names() -> Result<(), Box<dyn std::error::Error>> {
    let bytes = "source-λ.rs"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    let name = LogicalName::new(NameEncoding::WindowsUtf16Le, bytes)?;
    assert_eq!(
        name.unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .ok()
            .map(|receipt| receipt.value)
            .as_deref(),
        Some("source-λ.rs")
    );
    Ok(())
}
