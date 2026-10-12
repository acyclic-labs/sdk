use super::*;

#[tokio::test]
async fn directory_pages_keep_authored_phase_and_reject_changed_cuts() {
    for changed in [false, true] {
        let fs = Fs::memory();
        let root = attach(
            &fs,
            "root",
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .unwrap();
        if !changed {
            root.write(
                "/file.txt",
                Bytes::from_static(b"authored").into(),
                IdempotencyKey::new(),
                crate::PublicationPermit::Unrestricted,
                accounting!(),
            )
            .await
            .unwrap();
        }
        let (first, _) = root
            .list_directory("/", None, 16, accounting!())
            .await
            .unwrap();
        if changed {
            root.stat("/newly-observed.txt", accounting!())
                .await
                .unwrap();
            assert!(matches!(
                root.list_directory("/", first.next, 16, accounting!())
                    .await,
                Err(LazyWorkspaceError::StaleCursor),
            ));
        } else {
            assert!(first.entries.is_empty());
            let (authored, _) = root
                .list_directory("/", first.next, 16, accounting!())
                .await
                .unwrap();
            assert_eq!(authored.entries.len(), 1);
            assert!(authored.entries[0].authored);
            assert!(authored.next.is_none());
        }
    }
}

#[tokio::test]
async fn failed_authored_remove_rolls_back_source_tombstone() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "root",
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    root.write(
        "/directory/file.txt",
        Bytes::from_static(b"authored").into(),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    let before = root.state(accounting!()).await.unwrap();
    assert!(matches!(
        root.remove("/directory", None, accounting!()).await,
        Err(LazyWorkspaceError::WorkspaceCause(_)),
    ));
    let after = root.state(accounting!()).await.unwrap();
    assert_eq!(after.overlay, before.overlay);
    assert_eq!(after.shadows, before.shadows);
    assert!(after.pending_remove.is_none());
    assert!(
        root.stat("/directory", accounting!())
            .await
            .unwrap()
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
        .unwrap();
    root.write(
        "/file.txt",
        Bytes::from_static(b"authored").into(),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    let original = root.state(accounting!()).await.unwrap();
    let LazyLookup::Authored { stat, .. } =
        root.lookup("/file.txt", accounting!()).await.unwrap().0
    else {
        panic!("expected authored file");
    };
    let cancellation = CancellationToken::new();
    let mut ctx = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut checkout = root
        .checkout(crate::model::GenerationSelector::Head, &mut ctx)
        .await
        .unwrap();
    let record = root
        .authored_record(&mut checkout.value, "/file.txt", &mut ctx)
        .await
        .unwrap()
        .unwrap()
        .0;
    checkout.retire(&mut ctx).unwrap();
    let shadows = root
        .insert_shadow(
            original.shadows,
            stat.file_id,
            record,
            stat.metadata,
            accounting!(),
        )
        .await
        .unwrap();
    let overlay = root
        .insert_overlay(
            original.overlay,
            "/file.txt",
            LazyOverlayChange::Tombstone { removed: None },
            accounting!(),
        )
        .await
        .unwrap();
    for committed in [false, true] {
        let prior = root.state(accounting!()).await.unwrap();
        let key = IdempotencyKey::new();
        let pending = LazyWorkspaceState {
            revision: prior.revision + 1,
            overlay,
            shadows,
            pending_remove: Some(PendingLazyRemove {
                prior_overlay: prior.overlay,
                prior_shadows: prior.shadows,
                prepared_overlay: overlay,
                kind: PendingLazyRemoveKind::Authored {
                    idempotency_key: key,
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
                    accounting!(),
                )
                .await
                .unwrap()
        );
        if committed {
            let cancel = CancellationToken::new();
            let mut transaction = root
                .workspace()
                .begin_transaction(key, WorkBudget::UNBOUNDED, &cancel)
                .await
                .unwrap()
                .value;
            transaction
                .remove("/file.txt", WorkBudget::UNBOUNDED, &cancel)
                .await
                .unwrap();
            assert!(matches!(
                root.commit(
                    &mut transaction,
                    crate::PublicationPermit::Unrestricted,
                    accounting!()
                )
                .await
                .unwrap(),
                TransactionCommit::Committed(_),
            ));
        }
        let reopened = LazyWorkspace::open(
            fs.open_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.unwrap().value,
            Arc::clone(&source),
            store.clone(),
            accounting!(),
        )
        .await
        .unwrap();
        let recovered = reopened.state(accounting!()).await.unwrap();
        assert_eq!(
            recovered.overlay,
            if committed { overlay } else { prior.overlay }
        );
        assert_eq!(
            recovered.shadows,
            if committed { shadows } else { prior.shadows }
        );
        assert!(recovered.pending_remove.is_none());
        if committed {
            assert!(matches!(
                reopened.stat("/file.txt", accounting!()).await,
                Err(LazyWorkspaceError::NotFound),
            ));
        } else {
            assert!(
                reopened
                    .stat("/file.txt", accounting!())
                    .await
                    .unwrap()
                    .authored
            );
        }
    }
}

#[tokio::test]
async fn source_only_remove_recovery_keeps_tombstone_across_a_competing_create() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let store = MemoryLazyWorkspaceStore::default();
    let root = attach(&fs, "root", Arc::clone(&source), store.clone())
        .await
        .unwrap();
    root.stat("/file.txt", accounting!()).await.unwrap();
    let prior = root.state(accounting!()).await.unwrap();
    let overlay = root
        .insert_overlay(
            prior.overlay,
            "/file.txt",
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
                root.workspace().id(),
                prior.revision,
                pending,
                accounting!(),
            )
            .await
            .unwrap()
    );
    root.workspace()
        .write("/file.txt", Bytes::from_static(b"authored"))
        .await
        .unwrap();
    let reopened = LazyWorkspace::open(
        fs.open_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.unwrap().value,
        source,
        store,
        accounting!(),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened.state(accounting!()).await.unwrap().overlay,
        overlay
    );
    assert_eq!(
        reopened
            .read("/file.txt", 64, accounting!())
            .await
            .unwrap()
            .bytes,
        acyclic_stream::OwnedBytes::from_static(b"authored"),
    );
    reopened
        .remove("/file.txt", None, accounting!())
        .await
        .unwrap();
    assert!(matches!(
        reopened.stat("/file.txt", accounting!()).await,
        Err(LazyWorkspaceError::NotFound),
    ));
}
