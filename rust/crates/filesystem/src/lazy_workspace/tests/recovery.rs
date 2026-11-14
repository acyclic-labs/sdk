use super::*;

#[test]
fn refused_exactification_evidence_returns_its_allocation_credit() {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(
        WorkBudget {
            bytes_copied: 0,
            peak_allocation_bytes: 32,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    for _ in 0..2 {
        assert!(
            super::super::transaction::exactify_evidence(
                LazyOverlayId::from_bytes([7; 32]),
                &mut accounting
            )
            .is_err()
        );
        assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);
    }
}

#[tokio::test]
async fn directory_pages_keep_authored_phase_and_reject_changed_cuts() {
    for changed in [false, true] {
        let fs = Fs::memory();
        let root = attach(
            &fs,
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
            assert!(matches!(
                authored.entries[0].node,
                crate::LazyDirectoryNode::Authored { .. }
            ));
            assert!(authored.next.is_none());
        }
    }
}

#[tokio::test]
async fn failed_authored_remove_rolls_back_source_tombstone() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
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
    let root = attach(&fs, Arc::clone(&source), store.clone())
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
    let LazyLookup::Authored {
        record, metadata, ..
    } = root.lookup("/file.txt", accounting!()).await.unwrap().0
    else {
        panic!("expected authored file");
    };
    let shadows = root
        .insert_shadow(
            original.shadows,
            record.file_id,
            record,
            metadata,
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
            fs.open_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .unwrap()
                .value,
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
    let root = attach(&fs, Arc::clone(&source), store.clone())
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
        fs.open_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .unwrap()
            .value,
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

#[tokio::test]
async fn exactification_recovery_seals_the_original_operation_before_rollback() {
    for published in [false, true] {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = attach(&fs, Arc::clone(&source), store.clone())
            .await
            .unwrap();
        let cut = root.snapshot(accounting!()).await.unwrap();
        let prior = root.state(accounting!()).await.unwrap();
        let lookup = root.inspect("/file.txt", accounting!()).await.unwrap();
        let overlay = root
            .observed_overlay(prior.overlay, "/file.txt", &lookup.0, accounting!())
            .await
            .unwrap();
        let key = super::super::work::exactify_batch_key(cut.id, "/", 0, Some(prior.revision));
        let mut transaction = root
            .workspace()
            .begin_transaction(key, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .unwrap()
            .value;
        root.apply_resolved_promotion(
            &mut transaction,
            "/file.txt",
            &lookup.0,
            u64::MAX,
            accounting!(),
        )
        .await
        .unwrap();
        root.retire_lookup(lookup, accounting!()).unwrap();
        assert!(
            store
                .compare_and_swap_lazy_workspace(
                    root.workspace().id(),
                    prior.revision,
                    LazyWorkspaceState {
                        revision: prior.revision + 1,
                        overlay,
                        pending_remove: Some(PendingLazyRemove {
                            prior_overlay: prior.overlay,
                            prior_shadows: prior.shadows,
                            prepared_overlay: overlay,
                            kind: PendingLazyRemoveKind::Exactify {
                                idempotency_key: key
                            },
                        }),
                        ..prior.clone()
                    },
                    accounting!()
                )
                .await
                .unwrap()
        );
        if published {
            let evidence =
                super::super::transaction::exactify_evidence(overlay, accounting!()).unwrap();
            assert!(matches!(
                root.commit_context(
                    &mut transaction,
                    Some((
                        crate::Digest::from_bytes(overlay.into_bytes()),
                        evidence,
                        crate::PublicationPermit::Unrestricted
                    )),
                    crate::PublicationPermit::Unrestricted,
                    accounting!()
                )
                .await
                .unwrap(),
                TransactionCommit::Committed(_)
            ));
        } else {
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            let evidence =
                super::super::transaction::exactify_evidence(overlay, accounting!()).unwrap();
            assert!(
                root.commit_context(
                    &mut transaction,
                    Some((
                        crate::Digest::from_bytes(overlay.into_bytes()),
                        evidence,
                        crate::PublicationPermit::Unrestricted
                    )),
                    crate::PublicationPermit::Unrestricted,
                    &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancelled),
                )
                .await
                .is_err()
            );
            assert!(
                store
                    .load_lazy_workspace(root.workspace().id(), accounting!())
                    .await
                    .unwrap()
                    .unwrap()
                    .pending_remove
                    .is_some()
            );
        }
        // Recovery also works through a fresh call on the interrupted handle.
        assert!(
            root.state(accounting!())
                .await
                .unwrap()
                .pending_remove
                .is_none()
        );
        let reopened = LazyWorkspace::open(
            root.workspace().clone(),
            Arc::clone(&source),
            store.clone(),
            accounting!(),
        )
        .await
        .unwrap();
        let state = reopened.state(accounting!()).await.unwrap();
        assert!(state.pending_remove.is_none());
        assert_eq!(
            state.overlay,
            if published { overlay } else { prior.overlay }
        );
        let evidence =
            super::super::transaction::exactify_evidence(overlay, accounting!()).unwrap();
        let late = root
            .commit_context(
                &mut transaction,
                Some((
                    crate::Digest::from_bytes(overlay.into_bytes()),
                    evidence,
                    crate::PublicationPermit::Unrestricted,
                )),
                crate::PublicationPermit::Unrestricted,
                accounting!(),
            )
            .await
            .unwrap();
        assert!(if published {
            matches!(late, TransactionCommit::AlreadyCommitted(_))
        } else {
            matches!(late, TransactionCommit::IdempotencyConflict)
        });
        let head = root.head(accounting!()).await.unwrap();
        let file = head
            .stat(
                "/file.txt",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await;
        assert_eq!(file.is_ok(), published);
        if published {
            assert!(matches!(
                reopened
                    .overlay_fact(state.overlay, "/file.txt", accounting!())
                    .await
                    .unwrap(),
                Some(LazyOverlayChange::Observe { .. })
            ));
        }
        // The fenced old attempt must remain terminal, while a new call on
        // the recovered binding can materialize the same logical content cut.
        let fresh = reopened
            .exactify("/", crate::PublicationPermit::Unrestricted, accounting!())
            .await
            .unwrap();
        assert!(
            fresh
                .stat(
                    "/file.txt",
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new()
                )
                .await
                .is_ok()
        );
    }
}
