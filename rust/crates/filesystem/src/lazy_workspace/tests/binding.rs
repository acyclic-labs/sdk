use super::*;

#[tokio::test]
async fn tombstones_distinguish_deletion_from_hiding() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let head = root.head(accounting!()).await.unwrap().id();
    root.remove("/removed", None, accounting!()).await.unwrap();
    assert_eq!(root.head(accounting!()).await.unwrap().id(), head);
    root.hide_source_path("/hidden", accounting!())
        .await
        .unwrap();
    assert!(matches!(
        root.stat("/hidden", accounting!()).await,
        Err(LazyWorkspaceError::NotFound)
    ));
    let (deleted, _) = root.source_tombstones(16, accounting!()).await.unwrap();
    assert_eq!(
        deleted
            .iter()
            .map(|(path, _)| path.as_str())
            .collect::<Vec<_>>(),
        ["/removed"]
    );
    assert!(root.source_tombstones(0, accounting!()).await.is_err());
}

#[tokio::test]
async fn exactification_denials_do_not_publish_or_create_children() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
    let root = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let cut = root.snapshot(accounting!()).await.unwrap();
    let cancellation = CancellationToken::new();
    source.cancel_on_page.store(true, Ordering::Relaxed);
    assert!(
        root.exactify(
            "/",
            crate::PublicationPermit::Unrestricted,
            &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation)
        )
        .await
        .is_err()
    );
    assert!(cancellation.is_cancelled());
    assert_eq!(
        root.head(accounting!()).await.unwrap().id(),
        cut.authored_generation
    );
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    let budget = WorkBudget {
        authority_records_appended: 0,
        ..WorkBudget::UNBOUNDED
    };
    assert!(
        root.exactify(
            "/",
            crate::PublicationPermit::Unrestricted,
            &mut KernelAccounting::standalone(budget, &CancellationToken::new())
        )
        .await
        .is_err()
    );
    assert_eq!(
        root.head(accounting!()).await.unwrap().id(),
        cut.authored_generation
    );
    for (name, budget, cancel) in [
        ("cancelled", WorkBudget::UNBOUNDED, cancellation),
        ("bounded", WorkBudget::default(), CancellationToken::new()),
    ] {
        assert!(
            root.exactify_snapshot(
                cut,
                name,
                IdempotencyKey::new(),
                &mut KernelAccounting::standalone(budget, &cancel)
            )
            .await
            .is_err()
        );
        assert!(
            fs.open_workspace(name, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map(|receipt| receipt.value)
                .is_err()
        );
    }
}

#[tokio::test]
async fn full_directory_page_is_funded_before_projection() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        Arc::new(CountingSource {
            directory_entries: 1024,
            ..CountingSource::new(Bytes::from_static(b"content"))
        }),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let cancel = CancellationToken::new();
    let mut ctx = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
    let (page, _) = root
        .list_directory("/", None, 1024, &mut ctx)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1024);
    let output_bytes =
        (page.entries.len() * std::mem::size_of::<crate::LazyDirectoryEntry>()) as u64;
    assert!(ctx.spent().peak_allocation_bytes >= output_bytes);
    let budget = WorkBudget {
        peak_allocation_bytes: output_bytes - 1,
        ..WorkBudget::UNBOUNDED
    };
    let fs = Fs::memory();
    let root = attach(
        &fs,
        Arc::new(CountingSource {
            directory_entries: 1024,
            ..CountingSource::new(Bytes::from_static(b"content"))
        }),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    assert!(
        root.list_directory(
            "/",
            None,
            1024,
            &mut KernelAccounting::standalone(budget, &cancel)
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn attach_open_and_fork_demand_no_source_work() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let store = MemoryLazyWorkspaceStore::default();
    let root = attach(&fs, Arc::clone(&source), store.clone())
        .await
        .unwrap();
    let reopened = LazyWorkspace::open(
        fs.open_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .unwrap(),
        Arc::clone(&source),
        store,
        accounting!(),
    )
    .await
    .unwrap();
    let (child, original) = reopened
        .fork(
            "child",
            IdempotencyKey::from_bytes([5; 16]),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
    let first = child.snapshot(accounting!()).await.unwrap();
    let second = child.snapshot(accounting!()).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(first, original);
    let resumed = LazyWorkspace::open(
        fs.open_workspace("child", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .unwrap(),
        Arc::clone(&root.source),
        root.store.clone(),
        accounting!(),
    )
    .await
    .unwrap();
    assert_eq!(resumed.snapshot(accounting!()).await.unwrap(), original);
    assert_eq!(
        child
            .state(accounting!())
            .await
            .unwrap()
            .parent_workspace_id,
        Some(root.workspace().id())
    );
    assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
}
