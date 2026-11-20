use super::*;

use crate as acyclic_fs;

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
