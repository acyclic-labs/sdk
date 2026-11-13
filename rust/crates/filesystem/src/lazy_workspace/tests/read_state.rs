use super::*;

#[tokio::test]
async fn source_epoch_reads_never_publish_bookkeeping() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let store = MemoryLazyWorkspaceStore::default();
    let root = attach(&fs, Arc::clone(&source), store.clone())
        .await
        .unwrap();
    let before = store
        .load_lazy_workspace(root.workspace().id(), accounting!())
        .await
        .unwrap();
    source.replace(Bytes::from_static(b"new epoch"));
    let (first, second) = tokio::join!(async { root.state(accounting!()).await }, async {
        root.state(accounting!()).await
    });
    assert_eq!(first.unwrap().source, source.reference());
    assert_eq!(second.unwrap().source, source.reference());
    assert_eq!(
        store
            .load_lazy_workspace(root.workspace().id(), accounting!())
            .await
            .unwrap(),
        before,
        "reading a new epoch must not create a competing writer"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn publication_accepts_projected_epochs_without_read_side_writes() {
    fn allow(_: WorkBudget) -> std::future::Ready<crate::FsResult<()>> {
        std::future::ready(Ok(OperationReceipt {
            value: (),
            work: WorkCounters::UNCHARGED,
        }))
    }
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let store = MemoryLazyWorkspaceStore::default();
    let root = attach(&fs, Arc::clone(&source), store.clone())
        .await
        .unwrap();
    for prepared in [false, true] {
        source.replace(Bytes::from_static(b"new source epoch"));
        let before = store
            .load_lazy_workspace(root.workspace().id(), accounting!())
            .await
            .unwrap();
        let cut = root.publication_cut(None, accounting!()).await.unwrap();
        assert_eq!(cut.source, source.reference());
        assert_eq!(
            store
                .load_lazy_workspace(root.workspace().id(), accounting!())
                .await
                .unwrap(),
            before,
            "clean publication qualification is a read"
        );
        let key = IdempotencyKey::new();
        if prepared {
            root.prepare_mount_removals(&[], &[], &[], key, accounting!())
                .await
                .unwrap();
            assert_eq!(
                root.publication_cut(Some(key.operation_id()), accounting!())
                    .await
                    .unwrap(),
                cut
            );
            let stored = store
                .load_lazy_workspace(root.workspace().id(), accounting!())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.source, cut.source);
            assert!(stored.pending_remove.is_some());
            assert!(matches!(
                root.inspect_related(root.workspace().clone(), accounting!())
                    .await,
                Err(LazyWorkspaceError::Concurrent)
            ));
            assert_eq!(
                store
                    .load_lazy_workspace(root.workspace().id(), accounting!())
                    .await
                    .unwrap(),
                Some(stored),
                "an inspector must not roll back another owner's pending DATA"
            );
        }
        let mut checkout = root
            .workspace()
            .checkout_accounted(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                accounting!(),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        checkout
            .commit_even_if_clean(
                key.operation_id(),
                None,
                None,
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                &mut allow,
                None,
            )
            .await
            .unwrap();
        let generation = root.operation_generation(key, accounting!()).await.unwrap();
        assert!(generation.is_some());
        let context = NativeSnapshotContext::Lazy(cut);
        assert_eq!(
            root.publication_companion_settled(
                key.operation_id(),
                context,
                generation,
                accounting!()
            )
            .await
            .unwrap(),
            !prepared
        );
        if prepared {
            assert!(root.finish_mount_removals(accounting!()).await.unwrap());
        }
        assert!(
            root.publication_companion_settled(
                key.operation_id(),
                context,
                generation,
                accounting!()
            )
            .await
            .unwrap()
        );
    }
}
