use super::*;

#[tokio::test]
async fn directory_pages_emit_authored_entries_once_after_source_phase() {
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
    root.write(
        "/file.txt",
        Bytes::from_static(b"authored").into(),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("write");

    let source_page = root
        .list_directory("/", None, 16, accounting!())
        .await
        .map(|(page, _)| page)
        .expect("source phase");
    assert!(source_page.entries.is_empty());
    let authored_page = root
        .list_directory("/", source_page.next, 16, accounting!())
        .await
        .map(|(page, _)| page)
        .expect("authored phase");
    assert_eq!(authored_page.entries.len(), 1);
    assert!(authored_page.entries[0].authored);
    assert!(authored_page.next.is_none());
}

#[tokio::test]
async fn directory_cursor_rejects_overlay_changes_between_pages() {
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
    let first = root
        .list_directory("/", None, 16, accounting!())
        .await
        .map(|(page, _)| page)
        .expect("first page");
    root.stat("/newly-observed.txt", accounting!())
        .await
        .expect("observe");

    assert!(matches!(
        root.list_directory("/", first.next, 16, accounting!())
            .await
            .map(|(page, _)| page),
        Err(LazyWorkspaceError::StaleCursor)
    ));
}

#[tokio::test]
async fn failed_authored_remove_rolls_back_source_tombstone() {
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
    root.write(
        "/directory/file.txt",
        Bytes::from_static(b"authored").into(),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("write");
    let before = root.state(accounting!()).await.expect("state");

    assert!(matches!(
        root.remove("/directory", None, accounting!()).await,
        Err(LazyWorkspaceError::WorkspaceCause(_))
    ));
    let after = root
        .state(accounting!())
        .await
        .expect("state after failure");
    assert_eq!(after.overlay, before.overlay);
    assert_eq!(after.shadows, before.shadows);
    assert!(after.pending_remove.is_none());
    assert!(
        root.stat("/directory", accounting!())
            .await
            .expect("directory")
            .authored
    );
}

#[tokio::test]
async fn open_recovers_remove_intent_from_authored_state() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let store = MemoryLazyWorkspaceStore::default();
    let root = attach(&fs, "root", Arc::clone(&source), store.clone())
        .await
        .expect("attach");
    root.write(
        "/file.txt",
        Bytes::from_static(b"authored").into(),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("write");
    let prior = root.state(accounting!()).await.expect("state");
    let LazyLookup::Authored { stat, .. } = root
        .lookup("/file.txt", accounting!())
        .await
        .map(|(lookup, _)| lookup)
        .expect("authored")
    else {
        panic!("expected authored file");
    };
    let record = root
        .workspace()
        .record_by_id(stat.file_id)
        .await
        .expect("authored record");
    let staged_shadows = root
        .insert_shadow(
            prior.shadows,
            stat.file_id,
            record,
            stat.metadata,
            accounting!(),
        )
        .await
        .expect("staged identity shadow");
    assert_ne!(staged_shadows, prior.shadows);
    let tombstone = root
        .insert_overlay(
            prior.overlay,
            "/file.txt",
            LazyOverlayChange::Tombstone { removed: None },
            accounting!(),
        )
        .await
        .expect("tombstone");
    let first_key = IdempotencyKey::from_bytes([0x31; 16]);
    let pending = LazyWorkspaceState {
        revision: prior.revision + 1,
        overlay: tombstone,
        shadows: staged_shadows,
        pending_remove: Some(PendingLazyRemove {
            prior_overlay: prior.overlay,
            prior_shadows: prior.shadows,
            prepared_overlay: tombstone,
            kind: PendingLazyRemoveKind::Authored {
                idempotency_key: first_key,
            },
        }),
        ..prior.clone()
    };
    assert!(
        store
            .compare_and_swap_lazy_workspace(
                root.workspace().id(),
                prior.revision,
                pending,
                accounting!()
            )
            .await
            .expect("prepare")
    );

    let reopened = LazyWorkspace::open(
        fs.open_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("workspace")
            .value,
        Arc::clone(&source),
        store.clone(),
        accounting!(),
    )
    .await
    .expect("recover before physical remove");
    let recovered = reopened
        .state(accounting!())
        .await
        .expect("recovered state");
    assert_eq!(recovered.overlay, prior.overlay);
    assert_eq!(recovered.shadows, prior.shadows);
    assert!(recovered.pending_remove.is_none());

    let committed_key = IdempotencyKey::from_bytes([0x32; 16]);
    let prepared = LazyWorkspaceState {
        revision: recovered.revision + 1,
        overlay: tombstone,
        shadows: staged_shadows,
        pending_remove: Some(PendingLazyRemove {
            prior_overlay: recovered.overlay,
            prior_shadows: recovered.shadows,
            prepared_overlay: tombstone,
            kind: PendingLazyRemoveKind::Authored {
                idempotency_key: committed_key,
            },
        }),
        ..recovered.clone()
    };
    assert!(
        store
            .compare_and_swap_lazy_workspace(
                reopened.workspace().id(),
                recovered.revision,
                prepared,
                accounting!()
            )
            .await
            .expect("prepare second")
    );
    let mut removal = reopened
        .workspace()
        .begin_transaction(
            committed_key,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("removal transaction")
        .value;
    removal
        .remove(
            "/file.txt",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("stage remove");
    assert!(matches!(
        reopened
            .commit(
                &mut removal,
                crate::PublicationPermit::Unrestricted,
                accounting!(),
            )
            .await
            .expect("physical remove"),
        TransactionCommit::Committed(_)
    ));

    let finalized = LazyWorkspace::open(
        fs.open_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("workspace")
            .value,
        Arc::clone(&source),
        store,
        accounting!(),
    )
    .await
    .expect("recover after physical remove");
    let final_state = finalized.state(accounting!()).await.expect("final state");
    assert_eq!(final_state.overlay, tombstone);
    assert_eq!(final_state.shadows, staged_shadows);
    assert!(matches!(
        finalized.stat("/file.txt", accounting!()).await,
        Err(LazyWorkspaceError::NotFound)
    ));
}

#[tokio::test]
async fn source_only_remove_recovery_keeps_tombstone_across_a_competing_create() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let store = MemoryLazyWorkspaceStore::default();
    let root = attach(&fs, "root", Arc::clone(&source), store.clone())
        .await
        .expect("attach");
    root.stat("/file.txt", accounting!())
        .await
        .expect("observe source");
    let prior = root.state(accounting!()).await.expect("state");
    let tombstone = root
        .insert_overlay(
            prior.overlay,
            "/file.txt",
            LazyOverlayChange::Tombstone { removed: None },
            accounting!(),
        )
        .await
        .expect("tombstone");
    let pending = LazyWorkspaceState {
        revision: prior.revision + 1,
        overlay: tombstone,
        pending_remove: Some(PendingLazyRemove {
            prior_overlay: prior.overlay,
            prior_shadows: prior.shadows,
            prepared_overlay: tombstone,
            kind: PendingLazyRemoveKind::SourceOnly,
        }),
        ..prior.clone()
    };
    assert!(
        store
            .compare_and_swap_lazy_workspace(
                root.workspace().id(),
                prior.revision,
                pending,
                accounting!()
            )
            .await
            .expect("prepare")
    );
    root.workspace()
        .write("/file.txt", Bytes::from_static(b"authored"))
        .await
        .expect("competing authored create");

    let reopened = LazyWorkspace::open(
        fs.open_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("workspace")
            .value,
        Arc::clone(&source),
        store,
        accounting!(),
    )
    .await
    .expect("recover source removal");
    assert_eq!(
        reopened.state(accounting!()).await.expect("state").overlay,
        tombstone
    );
    assert_eq!(
        reopened
            .read("/file.txt", 64, accounting!())
            .await
            .expect("authored replacement")
            .bytes
            .as_ref(),
        b"authored"
    );
    reopened
        .remove("/file.txt", None, accounting!())
        .await
        .expect("remove authored");
    assert!(matches!(
        reopened.stat("/file.txt", accounting!()).await,
        Err(LazyWorkspaceError::NotFound)
    ));
}
