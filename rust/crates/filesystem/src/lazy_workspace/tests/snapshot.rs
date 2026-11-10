use super::*;

#[tokio::test]
async fn snapshot_validation_requires_available_indexes_and_generation() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let cut = root.snapshot(accounting!()).await.unwrap();
    root.validate_snapshot(&cut, accounting!()).await.unwrap();
    for (generation, overlay, shadows) in [
        (
            cut.authored_generation,
            LazyOverlayId::from_bytes([0xa1; 32]),
            cut.shadows,
        ),
        (
            cut.authored_generation,
            cut.overlay,
            LazyShadowId::from_bytes([0xa2; 32]),
        ),
        (
            crate::GenerationId::new(crate::Digest::from_bytes([0xa3; 32])),
            cut.overlay,
            cut.shadows,
        ),
    ] {
        let missing = LazySnapshotRef::new(
            cut.workspace_id,
            generation,
            cut.source.clone(),
            overlay,
            shadows,
        );
        assert!(
            root.validate_snapshot(&missing, accounting!())
                .await
                .is_err()
        );
        assert_eq!(root.snapshot(accounting!()).await.unwrap(), cut);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn frozen_views_and_write_replay_keep_the_original_generation() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
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
    let publish = async |bytes| {
        root.write_at_snapshot::<_, _, Box<dyn std::error::Error + Send + Sync>>(
            basis.clone(),
            std::iter::once((
                "/new.txt",
                Some(acyclic_stream::OwnedBytes::from_static(bytes)),
            )),
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!(),
            &mut (),
        )
        .await
        .unwrap()
    };
    let (commit, published) = publish(b"published").await;
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
        .read_at_snapshot(&basis, "/authored.txt", None, 32, accounting!())
        .await
        .unwrap();
    assert_eq!(bytes.bytes.as_slice(), b"before");
    assert!(!source);
    let mut cursor = None;
    let mut authored = Vec::new();
    loop {
        let (page, _, _) = root
            .list_directory_at_snapshot(&basis, "/", cursor, 16, accounting!())
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
    let (commit, replayed) = publish(b"published").await;
    assert!(matches!(commit, TransactionCommit::AlreadyCommitted(_)));
    assert_eq!(replayed.unwrap(), published);
    assert_eq!(
        root.read_at_snapshot(&published, "/new.txt", None, 32, accounting!())
            .await
            .unwrap()
            .0
            .bytes
            .as_slice(),
        b"published"
    );
    let (commit, proof) = publish(b"changed").await;
    assert!(matches!(commit, TransactionCommit::AlreadyCommitted(_)));
    assert!(matches!(
        proof.err().unwrap().downcast_ref::<LazyWorkspaceError>(),
        Some(LazyWorkspaceError::StaleIdentity)
    ));
}

/// A fork owns both cuts even when the source can still serve its old epoch.
/// Advancing the parent must neither replace the child's source bytes nor
/// invalidate an explicitly retained source snapshot.
#[tokio::test]
async fn fork_retains_available_source_and_authored_cuts() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source before")));
    let root = attach(&fs, source.clone(), MemoryLazyWorkspaceStore::default())
        .await
        .unwrap();
    root.workspace()
        .write_text("/authored.txt", "authored before")
        .await
        .unwrap();
    let basis = root.snapshot(accounting!()).await.unwrap();
    let (child, cut) = root
        .fork(
            "child",
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
    source.replace(Bytes::from_static(b"source after"));
    root.workspace()
        .write_text("/authored.txt", "authored after")
        .await
        .unwrap();
    assert_eq!(
        root.read("/source.txt", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"source after"
    );
    assert_eq!(
        child
            .read("/source.txt", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"source before"
    );
    assert_eq!(
        child
            .read("/authored.txt", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"authored before"
    );
    assert_eq!(
        root.read_at_snapshot(&basis, "/source.txt", None, 64, accounting!())
            .await
            .unwrap()
            .0
            .bytes
            .as_slice(),
        b"source before"
    );
    assert_eq!(
        root.stat_at_snapshot(&basis, "/unobserved.txt", accounting!())
            .await
            .unwrap()
            .unwrap()
            .logical_bytes,
        Some(b"source before".len() as u64)
    );
    let (page, _) = child
        .list_directory("/", None, 16, accounting!())
        .await
        .unwrap();
    let listed = page.entries.iter().find_map(|entry| entry.source).unwrap();
    assert_eq!(
        listed.version,
        SourceVersion(*blake3::hash(b"source before").as_bytes())
    );
    let reopened = root
        .open_related(child.workspace().clone(), accounting!())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .read_at_snapshot(&cut, "/source.txt", None, 64, accounting!())
            .await
            .unwrap()
            .0
            .bytes
            .as_slice(),
        b"source before"
    );
    source.versions.lock().unwrap().remove(&cut.source.epoch);
    assert!(matches!(
        child.read("/source.txt", 64, accounting!()).await,
        Err(LazyWorkspaceError::Demand(DemandError::StaleSource))
    ));
}

/// Nested routing must retain each historical provider cut, not a sum of
/// epochs or the references each provider happens to expose at read time.
#[tokio::test]
async fn routed_forks_retain_both_layers_after_independent_provider_progress() {
    use crate::demand::{Empty, Routed};
    let fs = Fs::memory();
    let ui = Arc::new(CountingSource::new(Bytes::from_static(b"old ui")));
    let mut other = CountingSource::new(Bytes::from_static(b"old context"));
    other.node.file_identity = [10; 32];
    let context = Arc::new(other);
    let name =
        |text: &str| LogicalName::new(NameEncoding::Utf8, text.as_bytes().to_vec(), 255).unwrap();
    let source = Arc::new(Routed::new(
        Routed::new(
            Empty,
            name("ui"),
            ui.clone(),
            crate::VolumeLimits::default(),
        ),
        name("context"),
        context.clone(),
        crate::VolumeLimits::default(),
    ));
    let root = attach(&fs, source, MemoryLazyWorkspaceStore::default())
        .await
        .unwrap();
    let basis = root.snapshot(accounting!()).await.unwrap();
    let (child, _) = root
        .fork(
            "child",
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
    ui.replace(Bytes::from_static(b"new ui"));
    context.replace(Bytes::from_static(b"new context"));
    for (path, old, new) in [
        ("/ui/file.txt", b"old ui".as_slice(), b"new ui".as_slice()),
        (
            "/context/file.txt",
            b"old context".as_slice(),
            b"new context".as_slice(),
        ),
    ] {
        assert_eq!(
            root.read(path, 64, accounting!())
                .await
                .unwrap()
                .bytes
                .as_slice(),
            new
        );
        assert_eq!(
            child
                .read(path, 64, accounting!())
                .await
                .unwrap()
                .bytes
                .as_slice(),
            old
        );
        assert_eq!(
            root.read_at_snapshot(&basis, path, None, 64, accounting!())
                .await
                .unwrap()
                .0
                .bytes
                .as_slice(),
            old
        );
    }
    let reopened = root
        .open_related(child.workspace().clone(), accounting!())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .read("/context/file.txt", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"old context"
    );
    // Publication uses the same retained source selector as reads, including
    // after reopening the child while both upstream providers have advanced.
    let child_basis = reopened.snapshot(accounting!()).await.unwrap();
    let (commit, published) = reopened
        .write_at_snapshot::<_, _, Box<dyn std::error::Error + Send + Sync>>(
            child_basis.clone(),
            std::iter::once((
                "/ui/authored.txt",
                Some(acyclic_stream::OwnedBytes::from_static(b"child write")),
            )),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
            &mut (),
        )
        .await
        .unwrap();
    assert!(matches!(commit, TransactionCommit::Committed(_)));
    assert_eq!(published.unwrap().source, child_basis.source);
    assert_eq!(
        reopened
            .read("/ui/authored.txt", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"child write"
    );
}
