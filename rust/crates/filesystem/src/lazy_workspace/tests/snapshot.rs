use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn frozen_views_and_write_replay_keep_the_original_generation() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "root",
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    root.workspace()
        .write_text("/authored.txt", "before")
        .await
        .unwrap();
    let basis = root.snapshot(accounting!()).await.unwrap();
    let key = IdempotencyKey::new();
    let batch = || {
        std::iter::once((
            "/new.txt",
            Some(acyclic_stream::OwnedBytes::from_static(b"published")),
        ))
    };
    let (commit, published) = root
        .write_at_snapshot::<_, _, Box<dyn std::error::Error + Send + Sync>>(
            basis,
            batch(),
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!(),
            &mut (),
        )
        .await
        .unwrap();
    assert!(matches!(commit, TransactionCommit::Committed(_)));
    let published = published.unwrap();
    root.workspace()
        .write_text("/authored.txt", "after")
        .await
        .unwrap();
    root.workspace()
        .write_text("/later.txt", "later")
        .await
        .unwrap();
    root.workspace()
        .write_text("/new.txt", "later")
        .await
        .unwrap();
    let (bytes, source) = root
        .read_at_snapshot(basis, "/authored.txt", None, 32, accounting!())
        .await
        .unwrap();
    assert_eq!(bytes.bytes.as_slice(), b"before");
    assert!(!source);
    let mut cursor = None;
    let mut authored = Vec::new();
    loop {
        let (page, _, _) = root
            .list_directory_at_snapshot(basis, "/", cursor, 16, accounting!())
            .await
            .unwrap();
        authored.extend(
            page.entries
                .iter()
                .filter(|e| e.authored)
                .map(|e| e.name.as_bytes().to_vec()),
        );
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(authored, [b"authored.txt".to_vec()]);
    let (commit, replayed) = root
        .write_at_snapshot::<_, _, Box<dyn std::error::Error + Send + Sync>>(
            basis,
            batch(),
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!(),
            &mut (),
        )
        .await
        .unwrap();
    assert!(matches!(commit, TransactionCommit::AlreadyCommitted(_)));
    assert_eq!(replayed.unwrap(), published);
    assert_eq!(
        root.read_at_snapshot(published, "/new.txt", None, 32, accounting!())
            .await
            .unwrap()
            .0
            .bytes
            .as_slice(),
        b"published"
    );
    let changed = std::iter::once((
        "/new.txt",
        Some(acyclic_stream::OwnedBytes::from_static(b"changed")),
    ));
    let (commit, proof) = root
        .write_at_snapshot::<_, _, Box<dyn std::error::Error + Send + Sync>>(
            basis,
            changed,
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!(),
            &mut (),
        )
        .await
        .unwrap();
    assert!(matches!(commit, TransactionCommit::AlreadyCommitted(_)));
    assert!(matches!(
        proof.err().unwrap().downcast_ref::<LazyWorkspaceError>(),
        Some(LazyWorkspaceError::StaleIdentity)
    ));
}
