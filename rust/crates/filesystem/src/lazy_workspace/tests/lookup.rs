use super::*;
use crate::PublicationPermit::Unrestricted;

#[tokio::test]
async fn inspection_is_transient_observations_are_cached_and_reads_pin_versions() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
    let root = attach(
        &fs,
        "inspection",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let before = root.state(accounting!()).await.unwrap();
    let (lookup, _) = root.inspect("/file.txt", accounting!()).await.unwrap();
    let LazyLookup::Source {
        source: reference,
        node,
    } = lookup
    else {
        panic!("source file");
    };
    assert_eq!(root.state(accounting!()).await.unwrap(), before);
    for _ in 0..2 {
        assert_eq!(
            root.stat("/deep/file.txt", accounting!())
                .await
                .unwrap()
                .logical_bytes,
            Some(6)
        );
    }
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    for (target, expected) in [
        (LazySeekTarget::Data, Some(2)),
        (LazySeekTarget::Hole, Some(6)),
    ] {
        assert_eq!(
            root.seek("/deep/file.txt", 2, target, accounting!())
                .await
                .unwrap(),
            expected
        );
    }
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    assert_eq!(
        root.read_range("/deep/file.txt", 2, 3, accounting!())
            .await
            .unwrap()
            .bytes
            .as_ref(),
        b"cde"
    );
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
    source.replace(Bytes::from_static(b"after"));
    assert_eq!(
        root.open_source_file("/file.txt", reference, node, accounting!())
            .await
            .unwrap()
            .read_range(0, 6, accounting!())
            .unwrap()
            .bytes
            .as_ref(),
        b"abcdef"
    );
    root.rebind_source(accounting!()).await.unwrap();
    let (lookup, _) = root.inspect("/file.txt", accounting!()).await.unwrap();
    let LazyLookup::Source { node: rebound, .. } = lookup else {
        panic!("source file");
    };
    assert_ne!(rebound.version, node.version);
}

#[tokio::test]
async fn alias_promotions_keep_identity_metadata_and_written_content() {
    for promote_before_write in [true, false] {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(if promote_before_write {
            b"shared"
        } else {
            b"source"
        }));
        source.node.link_count = promote_before_write.then_some(2);
        source.node.metadata.posix_mode = Some(0o100640);
        source.node.metadata.modified_ns = Some(123_456);
        let source = Arc::new(source);
        let root = attach(
            &fs,
            "aliases",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .unwrap();
        let mut identity = None;
        for path in ["/a", "/b"] {
            let (lookup, _) = root.lookup(path, accounting!()).await.unwrap();
            let actual = root
                .stable_file_id_for_lookup(path, &lookup, accounting!())
                .await
                .unwrap();
            assert_eq!(*identity.get_or_insert(actual), actual);
        }
        root.promote("/a", 64, IdempotencyKey::new(), Unrestricted, accounting!())
            .await
            .unwrap();
        if promote_before_write {
            root.promote("/b", 64, IdempotencyKey::new(), Unrestricted, accounting!())
                .await
                .unwrap();
        }
        root.write(
            "/a",
            Bytes::from_static(b"changed").into(),
            IdempotencyKey::new(),
            Unrestricted,
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
                .as_ref(),
            b"changed"
        );
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), reads);
        if !promote_before_write {
            root.promote("/b", 64, IdempotencyKey::new(), Unrestricted, accounting!())
                .await
                .unwrap();
        }
        for path in ["/a", "/b"] {
            let (lookup, _) = root.lookup(path, accounting!()).await.unwrap();
            let LazyLookup::Authored { stat, .. } = lookup else {
                panic!("authored alias");
            };
            assert_eq!(Some(stat.file_id), identity);
            assert_eq!(stat.link_count, 2);
            assert_eq!(stat.metadata.posix_mode, Some(0o100640));
            assert_eq!(stat.metadata.modified_ns, Some(123_456));
        }
        assert_eq!(
            root.read("/b", 64, accounting!())
                .await
                .unwrap()
                .bytes
                .as_ref(),
            b"changed"
        );
    }
}

#[tokio::test]
async fn direct_write_preserves_source_identity_and_survives_last_alias_removal() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"source"));
    source.node.link_count = Some(2);
    let source = Arc::new(source);
    let store = MemoryLazyWorkspaceStore::default();
    let root = attach(&fs, "shadow", Arc::clone(&source), store.clone())
        .await
        .unwrap();
    let expected = root
        .source_file_id(&source.node.file_identity, accounting!())
        .unwrap();
    root.write(
        "/a",
        Bytes::from_static(b"changed").into(),
        IdempotencyKey::new(),
        Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    root.promote_exact(
        "/a",
        expected,
        64,
        IdempotencyKey::new(),
        Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    assert_eq!(
        root.read("/a", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_ref(),
        b"changed"
    );
    root.remove("/a", None, accounting!()).await.unwrap();
    assert_eq!(
        root.read("/b", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_ref(),
        b"changed"
    );
    let reopened = LazyWorkspace::open(root.workspace().clone(), source, store, accounting!())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .read("/b", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_ref(),
        b"changed"
    );
}

#[tokio::test]
async fn remove_if_rejects_a_stale_source_identity() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "stale-remove",
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        root.remove(
            "/file.txt",
            Some(FileId::from_bytes([0xff; 16])),
            accounting!()
        )
        .await,
        Err(LazyWorkspaceError::StaleIdentity)
    ));
    assert!(root.stat("/file.txt", accounting!()).await.is_ok());
}

#[tokio::test]
async fn promotion_demands_only_the_selected_file_and_is_idempotent() {
    let fs = Fs::memory();
    let bytes = Bytes::from(vec![b'x'; 1024 * 1024 + 17]);
    let source = Arc::new(CountingSource::new(bytes.clone()));
    let root = attach(
        &fs,
        "promotion",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let key = IdempotencyKey::from_bytes([21; 16]);
    assert!(matches!(
        root.promote(
            "/deep/file.txt",
            bytes.len() as u64,
            key,
            Unrestricted,
            accounting!()
        )
        .await
        .unwrap(),
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        root.read("/deep/file.txt", bytes.len() as u64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_ref(),
        bytes.as_ref()
    );
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
    let reads = source.counts.ranges.load(Ordering::Relaxed);
    assert!(reads > 0);
    assert!(matches!(
        root.promote("/deep/file.txt", 0, key, Unrestricted, accounting!())
            .await
            .unwrap(),
        TransactionCommit::AlreadyCommitted(_)
    ));
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), reads);
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[tokio::test]
async fn metadata_inspection_keeps_cursors_valid_and_directory_promotion_keeps_children_lazy() {
    use crate::demand::native::NativeDemandSource;
    use crate::model::{FilesystemProfile, VolumeLimits};
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("a.txt"), b"a").unwrap();
    std::fs::write(directory.path().join("b.txt"), b"b").unwrap();
    std::fs::create_dir(directory.path().join("repo")).unwrap();
    std::fs::write(
        directory.path().join("repo/HEAD"),
        b"ref: refs/heads/main\n",
    )
    .unwrap();
    let cancellation = CancellationToken::new();
    let source = NativeDemandSource::open(
        directory.path().into(),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
    .unwrap()
    .value;
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "native",
        Arc::new(source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let (first, _) = root
        .list_directory("/", None, 1, &mut accounting)
        .await
        .unwrap();
    assert_eq!(first.entries.len(), 1);
    root.stat("/a.txt", &mut accounting).await.unwrap();
    let (second, _) = root
        .list_directory("/", first.next, 1, &mut accounting)
        .await
        .unwrap();
    assert_eq!(second.entries.len(), 1);
    let (lookup, _) = root.lookup("/repo", &mut accounting).await.unwrap();
    let expected = root
        .stable_file_id_for_lookup("/repo", &lookup, &mut accounting)
        .await
        .unwrap();
    root.promote(
        "/repo",
        0,
        IdempotencyKey::new(),
        Unrestricted,
        &mut accounting,
    )
    .await
    .unwrap();
    let (promoted, _) = root.lookup("/repo", &mut accounting).await.unwrap();
    assert_eq!(
        root.stable_file_id_for_lookup("/repo", &promoted, &mut accounting)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        root.read("/repo/HEAD", 64, &mut accounting)
            .await
            .unwrap()
            .bytes
            .as_ref(),
        b"ref: refs/heads/main\n"
    );
}
