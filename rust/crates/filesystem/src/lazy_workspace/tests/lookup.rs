use super::*;

#[tokio::test]
async fn frozen_directory_identity_survives_source_rebinding_and_pending_removal() {
    for rebind in [false, true] {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::new());
        source.node.kind = SourceNodeKind::Directory;
        source.node.logical_bytes = None;
        let source = Arc::new(source);
        let store = MemoryLazyWorkspaceStore::default();
        let root = attach(&fs, source.clone(), store.clone()).await.unwrap();
        root.promote(
            "/directory",
            0,
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
        let expected = root
            .source_file_id(&source.node.file_identity, accounting!())
            .unwrap();
        let (child, cut) = root
            .fork(
                "child",
                IdempotencyKey::new(),
                crate::PublicationPermit::Unrestricted,
                accounting!(),
            )
            .await
            .unwrap();
        let before = child
            .workspace_stat_at_snapshot(&cut, "/directory", accounting!())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.file_id, expected);
        if rebind {
            let _live = child
                .bind_fork_source(Arc::new(crate::demand::Empty), accounting!())
                .await
                .unwrap();
        } else {
            // A real durable in-progress removal at another path must not
            // affect a previously retained directory cut.
            let prior = child.state(accounting!()).await.unwrap();
            let overlay = child
                .insert_overlay(
                    prior.overlay,
                    "/unrelated",
                    LazyOverlayChange::Tombstone { removed: None },
                    accounting!(),
                )
                .await
                .unwrap();
            let pending = LazyWorkspaceState {
                revision: prior.revision + 1,
                overlay,
                pending_remove: Some(PendingLazyRemove {
                    prior_overlay: prior.overlay,
                    prior_shadows: prior.shadows,
                    prepared_overlay: overlay,
                    kind: PendingLazyRemoveKind::SourceOnly,
                }),
                ..prior.clone()
            };
            assert!(
                store
                    .compare_and_swap_lazy_workspace(
                        child.workspace().id(),
                        prior.revision,
                        pending,
                        accounting!(),
                    )
                    .await
                    .unwrap()
            );
        }
        assert_eq!(
            child
                .workspace_stat_at_snapshot(&cut, "/directory", accounting!())
                .await
                .unwrap(),
            Some(before),
        );
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn an_authored_file_hides_descendants_at_live_and_frozen_cuts() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    root.workspace()
        .write_text("/file.txt", "authored")
        .await
        .unwrap();
    let path = root
        .namespace_path("/file.txt/child", accounting!())
        .unwrap();
    let stop = CancellationToken::new();
    let generation = root
        .workspace()
        .head(WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    let (lookup, _allocation) = generation
        .lookup_paths(
            std::slice::from_ref(&path.value),
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .unwrap()
        .value;
    assert!(lookup.entries[0].record.is_none());
    let mut context = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &stop);
    let mut checkout = root
        .checkout(crate::model::GenerationSelector::Head, &mut context)
        .await
        .unwrap();
    assert!(
        root.authored_record(&mut checkout.value, "/file.txt/child", &mut context)
            .await
            .unwrap()
            .is_none()
    );
    checkout.retire(&mut context).unwrap();
    let cut = root.snapshot(accounting!()).await.unwrap();
    assert!(
        root.lookup_at_snapshot(&cut, "/file.txt/child", accounting!())
            .await
            .unwrap()
            .0
            .is_none()
    );
    assert!(matches!(
        root.stat("/file.txt/child", accounting!()).await,
        Err(LazyWorkspaceError::NotFound)
    ));
    assert_eq!(
        root.read("/file.txt", 32, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"authored"
    );
}

#[tokio::test]
async fn observation_and_transient_inspection_pin_their_versions() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"before")));
    let root = attach(
        &fs,
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
    root.retain_observation(
        "/file.txt",
        &LazyLookup::Source {
            source: pinned.clone(),
            node,
        },
        accounting!(),
    )
    .await
    .unwrap();
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

    let (removed, allocation) = root.inspect("/file.txt", accounting!()).await.unwrap();
    root.remove("/file.txt", None, accounting!()).await.unwrap();
    let deleted = root.state(accounting!()).await.unwrap();
    root.retain_observation("/file.txt", &removed, accounting!())
        .await
        .unwrap();
    assert_eq!(root.state(accounting!()).await.unwrap(), deleted);
    assert!(matches!(
        root.stat("/file.txt", accounting!()).await,
        Err(LazyWorkspaceError::NotFound)
    ));
    root.retire_lookup((removed, allocation), accounting!())
        .unwrap();
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
        let mut record = async |path| {
            root.authored_record(&mut checkout.value, path, accounting!())
                .await
                .unwrap()
                .unwrap()
        };
        let a = record("/a").await;
        let b = record("/b").await;
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
    let root = attach(&fs, Arc::clone(&source), store.clone())
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
        fs.open_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .unwrap(),
        source,
        store,
        accounting!(),
    )
    .await
    .unwrap();
    let read = async || reopened.read("/b", 64, accounting!()).await.unwrap().bytes;
    assert_eq!(read().await.as_slice(), b"changed");
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
    assert_eq!(read().await.as_slice(), b"changed");
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
        root.seek("/a", 2, crate::ExtentSeekTarget::Data, accounting!())
            .await
            .unwrap(),
        Some(2)
    );
    assert_eq!(
        root.seek("/b", 2, crate::ExtentSeekTarget::Hole, accounting!())
            .await
            .unwrap(),
        Some(6)
    );
    assert_eq!(
        root.seek("/b", 6, crate::ExtentSeekTarget::Data, accounting!())
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
    let promote = async || {
        root.promote(
            "/file",
            64,
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap()
    };
    promote().await;
    let promoted = root.head(accounting!()).await.unwrap().id();
    promote().await;
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
