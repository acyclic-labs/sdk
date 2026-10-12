use super::*;

#[tokio::test]
async fn observation_and_transient_inspection_pin_their_versions() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"before")));
    let root = attach(
        &fs,
        "root",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let state = root.state(accounting!()).await.unwrap();
    let (lookup, _) = root.inspect("/file.txt", accounting!()).await.unwrap();
    let LazyLookup::Source {
        source: pinned,
        node,
    } = lookup
    else {
        panic!("source required");
    };
    assert_eq!(root.state(accounting!()).await.unwrap(), state);
    source.replace(Bytes::from_static(b"after"));
    let file = root
        .open_source_file("/file.txt", pinned, node, accounting!())
        .await
        .unwrap();
    assert_eq!(
        file.read_range(0, 6, accounting!())
            .unwrap()
            .bytes
            .as_slice(),
        b"before"
    );
    assert_eq!(
        root.read_range("/file.txt", 1, 3, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"fte"
    );
    let first = root.snapshot(accounting!()).await.unwrap();
    root.stat("/file.txt", accounting!()).await.unwrap();
    assert_eq!(first, root.snapshot(accounting!()).await.unwrap());
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn aliases_preserve_identity_metadata_and_latest_content() {
    for link_count in [Some(2), None] {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"shared"));
        source.node.link_count = link_count;
        source.node.metadata.posix_mode = Some(0o100640);
        source.node.metadata.modified_ns = Some(123456);
        let source = Arc::new(source);
        let root = attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .unwrap();
        root.promote(
            "/a",
            64,
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
        root.write(
            "/a",
            acyclic_stream::OwnedBytes::from_static(b"changed"),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
        let reads = source.counts.ranges.load(Ordering::Relaxed);
        assert_eq!(
            root.read("/b", 64, accounting!())
                .await
                .unwrap()
                .bytes
                .as_slice(),
            b"changed"
        );
        root.promote(
            "/b",
            64,
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), reads);
        let mut checkout = root
            .checkout(crate::model::GenerationSelector::Head, accounting!())
            .await
            .unwrap();
        let a = root
            .authored_record(&mut checkout.value, "/a", accounting!())
            .await
            .unwrap()
            .unwrap();
        let b = root
            .authored_record(&mut checkout.value, "/b", accounting!())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.0.file_id, b.0.file_id);
        assert_eq!(
            a.0.file_id,
            root.source_file_id(&source.node.file_identity, accounting!())
                .unwrap()
        );
        assert_eq!(a.0.link_count, 2);
        assert_eq!(a.1.posix_mode, MetadataField::Value(0o100640));
        assert_eq!(b.1.modified_ns, MetadataField::Value(123456));
    }
}

#[tokio::test]
async fn last_alias_removal_keeps_the_identity_shadow_after_reopen() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"source"));
    source.node.link_count = Some(2);
    let source = Arc::new(source);
    let store = MemoryLazyWorkspaceStore::default();
    let root = attach(&fs, "root", Arc::clone(&source), store.clone())
        .await
        .unwrap();
    root.write(
        "/a",
        acyclic_stream::OwnedBytes::from_static(b"changed"),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    root.remove("/a", None, accounting!()).await.unwrap();
    let reopened = LazyWorkspace::open(
        fs.open_workspace("root").await.unwrap(),
        source,
        store,
        accounting!(),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .read("/b", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"changed"
    );
    reopened
        .promote(
            "/b",
            64,
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
    assert_eq!(
        reopened
            .read("/b", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"changed"
    );
    assert!(matches!(
        reopened.stat("/a", accounting!()).await,
        Err(LazyWorkspaceError::NotFound)
    ));
}

#[tokio::test]
async fn stale_identity_is_rejected_and_unresolved_seek_is_dense() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
    let root = attach(
        &fs,
        "root",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        root.remove("/file", Some(FileId::from_bytes([0x99; 16])), accounting!())
            .await,
        Err(LazyWorkspaceError::StaleIdentity)
    ));
    assert_eq!(
        root.seek("/a", 2, LazySeekTarget::Data, accounting!())
            .await
            .unwrap(),
        Some(2)
    );
    assert_eq!(
        root.seek("/b", 2, LazySeekTarget::Hole, accounting!())
            .await
            .unwrap(),
        Some(6)
    );
    assert_eq!(
        root.seek("/b", 6, LazySeekTarget::Data, accounting!())
            .await
            .unwrap(),
        None
    );
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn promotion_is_bounded_and_idempotent() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
    let root = attach(
        &fs,
        "root",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let head = root.head(accounting!()).await.unwrap().id();
    assert!(matches!(
        root.promote(
            "/file",
            3,
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!()
        )
        .await,
        Err(LazyWorkspaceError::TooLarge)
    ));
    assert_eq!(root.head(accounting!()).await.unwrap().id(), head);
    let key = IdempotencyKey::new();
    root.promote(
        "/file",
        64,
        key,
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    let promoted = root.head(accounting!()).await.unwrap().id();
    root.promote(
        "/file",
        64,
        key,
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    assert_eq!(root.head(accounting!()).await.unwrap().id(), promoted);
    let (lookup, _) = root.lookup("/file", accounting!()).await.unwrap();
    let id = root
        .stable_file_id_for_lookup("/file", &lookup, accounting!())
        .await
        .unwrap();
    root.write(
        "/file",
        acyclic_stream::OwnedBytes::from_static(b"direct"),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    root.promote_exact("/file", id, 64, IdempotencyKey::new(), accounting!())
        .await
        .unwrap();
    assert_eq!(
        root.read("/file", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"direct"
    );
}
