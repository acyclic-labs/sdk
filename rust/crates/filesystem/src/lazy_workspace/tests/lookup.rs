use super::*;

#[tokio::test]
async fn exact_observation_is_shared_and_content_is_range_demanded() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
    let store = MemoryLazyWorkspaceStore::default();
    let root = attach(&fs, "root", Arc::clone(&source), store)
        .await
        .expect("attach");

    assert_eq!(
        root.stat("/deep/file.txt", accounting!())
            .await
            .expect("stat")
            .logical_bytes,
        Some(6)
    );
    assert_eq!(
        root.stat("/deep/file.txt", accounting!())
            .await
            .expect("cached stat")
            .logical_bytes,
        Some(6)
    );
    assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    assert_eq!(
        root.read_range("/deep/file.txt", 2, 3, accounting!())
            .await
            .map(|chunk| Bytes::copy_from_slice(&chunk.bytes))
            .expect("range"),
        Bytes::from_static(b"cde")
    );
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 1);
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn inspection_is_transient_and_resolved_reads_pin_the_source_version() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"before")));
    let root = attach(
        &fs,
        "transient-inspection",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let before = root
        .state(accounting!())
        .await
        .expect("state before inspection");

    let (lookup, _) = root
        .inspect("/file.txt", accounting!())
        .await
        .expect("inspect source file");
    let LazyLookup::Source {
        source: source_reference,
        node,
    } = lookup
    else {
        panic!("inspection must resolve the source file");
    };
    assert_eq!(
        root.state(accounting!())
            .await
            .expect("state after inspection"),
        before
    );

    source.replace(Bytes::from_static(b"after"));
    assert_eq!(
        root.open_source_file("/file.txt", source_reference, node, accounting!())
            .await
            .expect("open pinned version")
            .read_range(0, 6, accounting!())
            .expect("read pinned version")
            .bytes
            .as_ref(),
        b"before"
    );

    root.rebind_source(accounting!())
        .await
        .expect("rebind source");
    let LazyLookup::Source { node: rebound, .. } = root
        .inspect("/file.txt", accounting!())
        .await
        .map(|(lookup, _)| lookup)
        .expect("reinspect")
    else {
        panic!("rebound inspection must resolve the source file");
    };
    assert_ne!(rebound.version, node.version);
}

#[tokio::test]
async fn independent_alias_promotions_preserve_source_hard_links() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"shared"));
    source.node.link_count = Some(2);
    source.node.metadata.posix_mode = Some(0o100640);
    source.node.metadata.modified_ns = Some(123_456);
    let source = Arc::new(source);
    let root = attach(
        &fs,
        "hard-link-promotions",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");

    root.promote(
        "/a",
        64,
        IdempotencyKey::from_bytes([0x71; 16]),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("promote first alias");
    root.promote(
        "/b",
        64,
        IdempotencyKey::from_bytes([0x72; 16]),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("promote second alias");
    let first = root
        .workspace()
        .stat("/a", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("first stat")
        .value;
    let second = root
        .workspace()
        .stat("/b", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("second stat")
        .value;
    assert_eq!(first.file_id, second.file_id);
    assert_eq!(
        first.file_id,
        root.source_file_id(&source.node.file_identity, accounting!())
            .expect("source identity")
    );
    assert_eq!(first.link_count, 2);
    assert_eq!(second.link_count, 2);
    assert_eq!(first.metadata.posix_mode, Some(0o100640));
    assert_eq!(second.metadata.modified_ns, Some(123_456));

    root.write(
        "/a",
        Bytes::from_static(b"changed").into(),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("mutate alias");
    assert_eq!(
        root.workspace()
            .read("/b", 64, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| Bytes::copy_from_slice(receipt.value.as_ref()))
            .expect("read other alias")
            .as_ref(),
        b"changed"
    );
}

#[tokio::test]
async fn unresolved_alias_with_unknown_link_count_reads_latest_authored_identity() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"source"));
    source.node.link_count = None;
    let source = Arc::new(source);
    let root = attach(
        &fs,
        "hard-link-late-alias",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");

    root.promote(
        "/a",
        64,
        IdempotencyKey::from_bytes([0x73; 16]),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("promote first alias");
    root.write(
        "/a",
        Bytes::from_static(b"changed").into(),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("mutate first alias");
    let source_reads = source.counts.ranges.load(Ordering::Relaxed);
    assert_eq!(
        root.read("/b", 64, accounting!())
            .await
            .map(|chunk| Bytes::copy_from_slice(&chunk.bytes))
            .expect("read alias")
            .as_ref(),
        b"changed"
    );
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), source_reads);

    root.promote(
        "/b",
        64,
        IdempotencyKey::from_bytes([0x74; 16]),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("promote late alias");
    let first = root
        .workspace()
        .stat("/a", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("first stat")
        .value;
    let second = root
        .workspace()
        .stat("/b", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("second stat")
        .value;
    assert_eq!(first.file_id, second.file_id);
    assert_eq!(
        first.file_id,
        root.source_file_id(&source.node.file_identity, accounting!())
            .expect("source identity")
    );
    assert_eq!(
        root.read("/b", 64, accounting!())
            .await
            .map(|chunk| Bytes::copy_from_slice(&chunk.bytes))
            .expect("read promoted alias")
            .as_ref(),
        b"changed"
    );
}

#[tokio::test]
async fn identity_shadow_survives_last_promoted_alias_removal() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"source"));
    source.node.link_count = Some(2);
    let source = Arc::new(source);
    let store = MemoryLazyWorkspaceStore::default();
    let root = attach(&fs, "hard-link-shadow", Arc::clone(&source), store.clone())
        .await
        .expect("attach");

    root.write(
        "/a",
        Bytes::from_static(b"changed").into(),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("direct identity-preserving write");
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    root.remove("/a", None, accounting!())
        .await
        .expect("remove promoted alias");
    assert_eq!(
        root.read("/b", 64, accounting!())
            .await
            .map(|chunk| Bytes::copy_from_slice(&chunk.bytes))
            .expect("read shadow alias")
            .as_ref(),
        b"changed"
    );

    let reopened = LazyWorkspace::open(
        fs.open_workspace(
            "hard-link-shadow",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("workspace")
        .value,
        source,
        store,
        accounting!(),
    )
    .await
    .expect("reopen");
    assert_eq!(
        reopened
            .read("/b", 64, accounting!())
            .await
            .map(|chunk| Bytes::copy_from_slice(&chunk.bytes))
            .expect("reopened shadow")
            .as_ref(),
        b"changed"
    );
}

#[tokio::test]
async fn remove_if_rejects_a_stale_source_identity() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let root = attach(
        &fs,
        "stale-remove",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let stale = FileId::from_bytes([0xff; 16]);
    assert!(matches!(
        root.remove("/file.txt", Some(stale), accounting!()).await,
        Err(LazyWorkspaceError::StaleIdentity)
    ));
    assert!(root.stat("/file.txt", accounting!()).await.is_ok());
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[tokio::test]
async fn metadata_inspection_does_not_stale_a_native_directory_cursor() {
    use crate::demand::native::NativeDemandSource;
    use crate::model::{FilesystemProfile, VolumeLimits};

    let directory = tempfile::tempdir().expect("temporary source");
    std::fs::write(directory.path().join("a.txt"), b"a").expect("first file");
    std::fs::write(directory.path().join("b.txt"), b"b").expect("second file");
    let source = Arc::new(
        NativeDemandSource::open(
            (directory.path()).into(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("source")
        .value,
    );
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "paged-source",
        source,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let first = root
        .list_directory("/", None, 1, accounting!())
        .await
        .map(|(page, _)| page)
        .expect("first page");
    let name = std::str::from_utf8(first.entries[0].name.as_bytes()).expect("portable name");
    root.inspect(&format!("/{name}"), accounting!())
        .await
        .map(|(lookup, _)| lookup)
        .expect("entry metadata");
    let second = root
        .list_directory("/", first.next, 1, accounting!())
        .await
        .map(|(page, _)| page)
        .expect("second page remains valid");
    assert_eq!(second.entries.len(), 1);
}

#[tokio::test]
async fn unresolved_aliases_have_one_stable_identity_and_dense_seek_needs_no_content() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
    let root = attach(
        &fs,
        "root",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let first = match root
        .lookup("/first", accounting!())
        .await
        .map(|(lookup, _)| lookup)
        .expect("first")
    {
        LazyLookup::Source { node, .. } => node,
        LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
            panic!("source lookup expected")
        }
    };
    let second = match root
        .lookup("/second", accounting!())
        .await
        .map(|(lookup, _)| lookup)
        .expect("second")
    {
        LazyLookup::Source { node, .. } => node,
        LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
            panic!("source lookup expected")
        }
    };
    assert_eq!(
        root.source_file_id(&first.file_identity, accounting!())
            .expect("source identity"),
        root.source_file_id(&second.file_identity, accounting!())
            .expect("source identity")
    );
    assert_eq!(
        root.seek("/first", 2, LazySeekTarget::Data, accounting!())
            .await
            .expect("seek data"),
        Some(2)
    );
    assert_eq!(
        root.seek("/first", 2, LazySeekTarget::Hole, accounting!())
            .await
            .expect("seek hole"),
        Some(6)
    );
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn promotion_demands_only_the_selected_file_and_is_idempotent() {
    let fs = Fs::memory();
    let bytes = Bytes::from(vec![b'x'; 1024 * 1024 + 17]);
    let source = Arc::new(CountingSource::new(bytes.clone()));
    let root = attach(
        &fs,
        "root",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let key = IdempotencyKey::from_bytes([21; 16]);

    let first = root
        .promote(
            "/deep/file.txt",
            bytes.len() as u64,
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .expect("promote");
    assert!(matches!(first, TransactionCommit::Committed(_)));
    assert_eq!(
        root.workspace()
            .read(
                "/deep/file.txt",
                bytes.len() as u64,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| Bytes::copy_from_slice(receipt.value.as_ref()))
            .expect("authored content"),
        bytes
    );
    assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 2);
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);

    let second = root
        .promote(
            "/deep/file.txt",
            0,
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .expect("idempotent promotion");
    assert!(matches!(second, TransactionCommit::AlreadyCommitted(_)));
    assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 2);
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[tokio::test]
async fn promoted_source_directory_keeps_its_identity_and_lazy_children() {
    use crate::demand::native::NativeDemandSource;
    use crate::model::{FilesystemProfile, VolumeLimits};

    let directory = tempfile::tempdir().expect("temporary source");
    std::fs::create_dir(directory.path().join("repo")).expect("source directory");
    std::fs::write(
        directory.path().join("repo/HEAD"),
        b"ref: refs/heads/main\n",
    )
    .expect("source child");
    let source = Arc::new(
        NativeDemandSource::open(
            (directory.path()).into(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("source")
        .value,
    );
    let root = attach(
        &Fs::memory(),
        "directory-identity",
        source,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let source_node = match root
        .lookup("/repo", accounting!())
        .await
        .map(|(lookup, _)| lookup)
        .expect("source directory")
    {
        LazyLookup::Source { node, .. } => node,
        LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
            panic!("expected source directory")
        }
    };
    let source_id = root
        .source_file_id(&source_node.file_identity, accounting!())
        .expect("source identity");

    root.promote(
        "/repo",
        0,
        IdempotencyKey::from_bytes([0x52; 16]),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("promote directory");

    let promoted = root
        .lookup("/repo", accounting!())
        .await
        .map(|(lookup, _)| lookup)
        .expect("promoted directory");
    assert_eq!(
        root.stable_file_id_for_lookup("/repo", &promoted, accounting!())
            .await
            .expect("stable projection identity"),
        source_id
    );
    assert_eq!(
        root.read("/repo/HEAD", 64, accounting!())
            .await
            .map(|chunk| Bytes::copy_from_slice(&chunk.bytes))
            .expect("lazy child"),
        Bytes::from_static(b"ref: refs/heads/main\n")
    );
}

#[tokio::test]
async fn exact_promotion_accepts_an_identity_preserving_direct_write() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let root = attach(
        &fs,
        "root",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let source_node = match root
        .lookup("/file.txt", accounting!())
        .await
        .map(|(lookup, _)| lookup)
        .expect("source lookup")
    {
        LazyLookup::Source { node, .. } => node,
        LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
            panic!("expected source node")
        }
    };
    let expected_source = root
        .source_file_id(&source_node.file_identity, accounting!())
        .expect("source identity");
    root.write(
        "/file.txt",
        Bytes::from_static(b"replacement").into(),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("authored replacement");

    root.promote_exact(
        "/file.txt",
        expected_source,
        64,
        IdempotencyKey::from_bytes([0x73; 16]),
        accounting!(),
    )
    .await
    .expect("identity-preserving write is already promoted");
    assert_eq!(
        root.read("/file.txt", 64, accounting!())
            .await
            .map(|chunk| Bytes::copy_from_slice(&chunk.bytes))
            .expect("replacement remains"),
        Bytes::from_static(b"replacement")
    );
}
