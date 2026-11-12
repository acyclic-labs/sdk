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
