use super::*;

#[tokio::test]
async fn frozen_reads_listing_and_write_replay_keep_the_original_authored_head() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "frozen",
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    root.workspace()
        .write_text("/authored.txt", "before")
        .await
        .expect("initial write");
    let snapshot = root.snapshot(accounting!()).await.expect("snapshot");
    let key = IdempotencyKey::new();
    let batch = || std::iter::once(("/new.txt", Some(Bytes::from_static(b"published").into())));
    let (commit, published) = root
        .write_at_snapshot::<_, _, Box<dyn std::error::Error + Send + Sync>>(
            snapshot,
            batch(),
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!(),
            &mut (),
        )
        .await
        .expect("publish frozen batch");
    assert!(matches!(commit, TransactionCommit::Committed(_)));
    let published = published.expect("publication snapshot");
    root.workspace()
        .write_text("/authored.txt", "after")
        .await
        .expect("later write");
    root.workspace()
        .write_text("/later.txt", "later")
        .await
        .expect("later path");
    root.workspace()
        .write_text("/new.txt", "later")
        .await
        .expect("advance published file");
    let (chunk, used_source) = root
        .read_at_snapshot(snapshot, "/authored.txt", None, 32, accounting!())
        .await
        .expect("frozen read");
    assert_eq!(chunk.bytes.as_ref(), b"before");
    assert!(!used_source);
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut cursor = None;
    let mut authored = Vec::new();
    loop {
        let (page, _, _) = root
            .list_directory_at_snapshot(snapshot, "/", cursor, 16, &mut accounting)
            .await
            .expect("frozen page");
        authored.extend(
            page.entries
                .iter()
                .filter(|entry| entry.authored)
                .map(|entry| entry.name.as_bytes().to_vec()),
        );
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(authored, vec![b"authored.txt".to_vec()]);
    let (commit, replayed) = root
        .write_at_snapshot::<_, _, Box<dyn std::error::Error + Send + Sync>>(
            snapshot,
            batch(),
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!(),
            &mut (),
        )
        .await
        .expect("replay original DATA");
    assert!(matches!(commit, TransactionCommit::AlreadyCommitted(_)));
    assert_eq!(replayed.expect("original snapshot"), published);
    assert_eq!(
        root.read_at_snapshot(published, "/new.txt", None, 32, accounting!())
            .await
            .expect("original content")
            .0
            .bytes
            .as_ref(),
        b"published"
    );
    let (commit, proof) = root
        .write_at_snapshot::<_, _, Box<dyn std::error::Error + Send + Sync>>(
            snapshot,
            std::iter::once(("/new.txt", Some(Bytes::from_static(b"changed").into()))),
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!(),
            &mut (),
        )
        .await
        .expect("known DATA survives changed retry input");
    assert!(matches!(commit, TransactionCommit::AlreadyCommitted(_)));
    let error = proof.err().expect("changed input rejects");
    assert!(matches!(
        error.downcast_ref::<LazyWorkspaceError>(),
        Some(LazyWorkspaceError::StaleIdentity)
    ));
}
