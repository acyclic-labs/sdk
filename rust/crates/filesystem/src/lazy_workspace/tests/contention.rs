use super::*;
use tokio::sync::oneshot;

/// Pauses one real conditional commit after its transition has been built.
/// The winner still writes the real companion store, not a canned CAS result.
#[derive(Clone, Default)]
struct ContendedStore {
    inner: MemoryLazyWorkspaceStore,
    gate: Arc<Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>>,
    conflicts: Arc<AtomicUsize>,
}
impl ContendedStore {
    fn pause_next_commit(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, entering) = oneshot::channel();
        let (release, released) = oneshot::channel();
        assert!(
            self.gate
                .lock()
                .unwrap()
                .replace((entered, released))
                .is_none()
        );
        (entering, release)
    }
}
impl LazyWorkspaceStore for ContendedStore {
    async fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyWorkspaceState>, LazyWorkspaceError> {
        self.inner.load_lazy_workspace(id, ctx).await
    }
    async fn compare_and_swap_lazy_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
        state: LazyWorkspaceState,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let gate = self.gate.lock().unwrap().take();
        if let Some((entered, released)) = gate {
            entered.send(()).unwrap();
            released.await.unwrap();
        }
        let committed = self
            .inner
            .compare_and_swap_lazy_workspace(id, revision, state, ctx)
            .await?;
        if !committed {
            self.conflicts.fetch_add(1, Ordering::SeqCst);
        }
        Ok(committed)
    }
    async fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.inner.load_lazy_overlay(id, ctx).await
    }
    async fn load_lazy_shadow(
        &self,
        id: LazyShadowId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.inner.load_lazy_shadow(id, ctx).await
    }
    async fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (LazyOverlay, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        self.inner.put_lazy_overlay(id, value, ctx).await
    }
    async fn put_lazy_shadow(
        &self,
        id: LazyShadowId,
        value: (LazyShadow, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        self.inner.put_lazy_shadow(id, value, ctx).await
    }
}

#[tokio::test]
async fn conditional_binding_conflicts_rebuild_without_losing_the_winner() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let store = ContendedStore::default();
    let root = attach(&fs, Arc::clone(&source), store.clone())
        .await
        .unwrap();
    source.replace(Bytes::from_static(b"new epoch"));
    // The idle reader loses its epoch CAS to a writer, and must return a fresh
    // state that includes both the new source epoch and the writer's overlay.
    let (entering, release) = store.pause_next_commit();
    let (reader, ()) = tokio::join!(async { root.state(accounting!()).await.unwrap() }, async {
        entering.await.unwrap();
        root.append_overlay(
            "/winner",
            LazyOverlayChange::Tombstone { removed: None },
            accounting!(),
        )
        .await
        .unwrap();
        release.send(()).unwrap();
    });
    assert_eq!(reader.source, source.reference());
    assert!(matches!(
        root.overlay_fact(reader.overlay, "/winner", accounting!())
            .await
            .unwrap(),
        Some(LazyOverlayChange::Tombstone { .. })
    ));
    // Two authored facts built from the same cut must both survive. Reusing
    // the loser's precomputed overlay would erase the winning publication.
    let (entering, release) = store.pause_next_commit();
    let ((), ()) = tokio::join!(
        async {
            root.append_overlay(
                "/loser",
                LazyOverlayChange::Tombstone { removed: None },
                accounting!(),
            )
            .await
            .unwrap()
        },
        async {
            entering.await.unwrap();
            root.append_overlay(
                "/second-winner",
                LazyOverlayChange::Tombstone { removed: None },
                accounting!(),
            )
            .await
            .unwrap();
            release.send(()).unwrap();
        }
    );
    // A read observation loses its publication to a completed remove of the
    // very same path. It may return its original read, but cannot resurrect
    // the deleted source binding in any later lookup or fork.
    let (entering, release) = store.pause_next_commit();
    let (observed, ()) = tokio::join!(
        async { root.lookup("/file.txt", accounting!()).await.unwrap() },
        async {
            entering.await.unwrap();
            root.remove("/file.txt", None, accounting!()).await.unwrap();
            release.send(()).unwrap();
        }
    );
    root.retire_lookup(observed, accounting!()).unwrap();
    assert!(matches!(
        root.stat("/file.txt", accounting!()).await,
        Err(LazyWorkspaceError::NotFound)
    ));
    let state = root.state(accounting!()).await.unwrap();
    for path in ["/winner", "/loser", "/second-winner"] {
        assert!(
            matches!(
                root.overlay_fact(state.overlay, path, accounting!())
                    .await
                    .unwrap(),
                Some(LazyOverlayChange::Tombstone { .. })
            ),
            "lost {path}"
        );
    }
    // A recovering opener can lose to another recovery followed by a new
    // operation. Re-read the state, but never clear that new owner's intent.
    let pending = PendingLazyRemove {
        prior_overlay: state.overlay,
        prior_shadows: state.shadows,
        prepared_overlay: state.overlay,
        kind: PendingLazyRemoveKind::SourceOnly,
    };
    assert!(
        store
            .compare_and_swap_lazy_workspace(
                root.workspace().id(),
                state.revision,
                LazyWorkspaceState {
                    revision: state.revision + 1,
                    pending_remove: Some(pending),
                    ..state
                },
                accounting!()
            )
            .await
            .unwrap()
    );
    let (entering, release) = store.pause_next_commit();
    let ((), next) = tokio::join!(
        async { root.recover_pending_remove(accounting!()).await.unwrap() },
        async {
            entering.await.unwrap();
            root.recover_pending_remove(accounting!()).await.unwrap();
            let state = root.state(accounting!()).await.unwrap();
            let overlay = root
                .insert_overlay(
                    state.overlay,
                    "/next-operation",
                    LazyOverlayChange::Tombstone { removed: None },
                    accounting!(),
                )
                .await
                .unwrap();
            let next = LazyWorkspaceState {
                revision: state.revision + 1,
                overlay,
                pending_remove: Some(PendingLazyRemove {
                    prior_overlay: state.overlay,
                    prior_shadows: state.shadows,
                    prepared_overlay: overlay,
                    kind: PendingLazyRemoveKind::SourceOnly,
                }),
                ..state
            };
            assert!(
                store
                    .compare_and_swap_lazy_workspace(
                        root.workspace().id(),
                        next.revision - 1,
                        next.clone(),
                        accounting!()
                    )
                    .await
                    .unwrap()
            );
            release.send(()).unwrap();
            next
        }
    );
    assert_eq!(
        store
            .load_lazy_workspace(root.workspace().id(), accounting!())
            .await
            .unwrap(),
        Some(next)
    );
    root.recover_pending_remove(accounting!()).await.unwrap();
    assert_eq!(store.conflicts.load(Ordering::SeqCst), 4);
    // Stale identity is still a real refusal, never retried as contention.
    source.epoch.store(1, Ordering::Relaxed);
    assert!(matches!(
        root.state(accounting!()).await,
        Err(LazyWorkspaceError::StaleSource)
    ));
}
