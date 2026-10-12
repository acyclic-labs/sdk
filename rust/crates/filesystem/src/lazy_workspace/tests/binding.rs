use super::*;
/// A view bound before its shared source advanced (a refresh of the
#[tokio::test]
async fn a_view_follows_its_shared_source_to_a_newer_epoch() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let root = attach(
        &fs,
        "follows",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    root.stat("/observed.txt", accounting!())
        .await
        .expect("observe");
    source.invalidate();
    assert_eq!(
        root.stat("/new.txt", accounting!())
            .await
            .expect("follows")
            .logical_bytes,
        Some(6)
    );
    assert_eq!(
        root.snapshot(accounting!()).await.expect("snapshot").source,
        source.reference()
    );
}

#[tokio::test]
async fn source_tombstones_list_only_removed_source_paths() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let root = attach(
        &fs,
        "tombstones",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    root.stat("/kept.txt", accounting!())
        .await
        .expect("observe kept");
    root.stat("/removed.txt", accounting!())
        .await
        .expect("observe removed");
    root.remove("/removed.txt", None, accounting!())
        .await
        .expect("remove source path");
    root.remove("/also-removed.txt", None, accounting!())
        .await
        .expect("remove unobserved source path");
    root.stat("/hidden.txt", accounting!())
        .await
        .expect("observe hidden");
    root.hide_source_path("/hidden.txt", accounting!())
        .await
        .expect("hide source path");
    assert!(root.stat("/hidden.txt", accounting!()).await.is_err());
    let tombstones = root
        .source_tombstones(16, accounting!())
        .await
        .map(|(paths, _)| paths)
        .expect("tombstones");
    assert_eq!(
        tombstones
            .iter()
            .map(|(path, _)| path.as_str())
            .collect::<Vec<_>>(),
        ["/also-removed.txt", "/removed.txt"]
    );
    let current = root
        .current_source_version("/removed.txt", accounting!())
        .await
        .expect("current version");
    assert_eq!(current, tombstones.get(1).map(|(_, version)| *version));
    assert!(matches!(
        root.source_tombstones(1, accounting!())
            .await
            .map(|(paths, _)| paths),
        Err(LazyWorkspaceError::Demand(DemandError::Work(_)))
    ));
}

#[tokio::test]
async fn exactify_threads_cancellation_into_nested_source_work() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
    let root = attach(
        &fs,
        "cancelled-exactify",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let cancellation = CancellationToken::new();
    source.cancel_next_page();

    assert!(matches!(
        root.exactify(
            "/",
            crate::PublicationPermit::Unrestricted,
            &mut crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation)
        )
        .await,
        Err(LazyWorkspaceError::Cancelled)
    ));
    assert!(cancellation.is_cancelled());
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), 1);
    assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn exactify_budget_exhaustion_cannot_publish_the_current_path() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
    let root = attach(
        &fs,
        "budgeted-exactify",
        source,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let before = root
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("head before exactify")
        .value
        .id();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.authority_records_appended = 0;

    assert!(matches!(
        root.exactify(
            "/",
            crate::PublicationPermit::Unrestricted,
            &mut crate::kernel::KernelAccounting::standalone(budget, &CancellationToken::new())
        )
        .await,
        Err(LazyWorkspaceError::WorkspaceCause(
            WorkspaceError::Filesystem(crate::FsError::Work(_))
        ))
    ));
    assert_eq!(
        root.workspace()
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("head after rejection")
            .value
            .id(),
        before
    );
    assert!(matches!(
        root.workspace()
            .stat(
                "/file.txt",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error),
        Err(WorkspaceError::NotFound)
    ));
}

#[tokio::test]
async fn exactify_snapshot_rejects_before_destination_publication() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "snapshot-source",
        Arc::new(CountingSource::new(Bytes::from_static(b"content"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let snapshot = root.snapshot(accounting!()).await.expect("snapshot");

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        root.exactify_snapshot(
            snapshot,
            "cancelled-destination",
            IdempotencyKey::from_bytes([0x91; 16]),
            &mut crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancelled)
        )
        .await,
        Err(LazyWorkspaceError::Cancelled)
    ));
    assert!(
        fs.open_workspace(
            "cancelled-destination",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .is_err()
    );

    assert!(matches!(
        root.exactify_snapshot(
            snapshot,
            "budget-destination",
            IdempotencyKey::from_bytes([0x92; 16]),
            &mut crate::kernel::KernelAccounting::standalone(
                WorkBudget::default(),
                &CancellationToken::new()
            )
        )
        .await,
        Err(LazyWorkspaceError::Filesystem(crate::FsError::Work(_)))
    ));
    assert!(
        fs.open_workspace(
            "budget-destination",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .is_err()
    );
}

#[tokio::test]
async fn full_directory_page_peak_is_admitted_before_projection() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "page-memory",
        Arc::new(CountingSource::new(Bytes::from_static(b"content")).with_directory_entries(1_024)),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let (page, allocation) = root
        .list_directory("/", None, 1_024, &mut accounting)
        .await
        .expect("page");
    assert_eq!(page.entries.len(), 1_024);
    let peak = accounting.spent().peak_allocation_bytes;
    let bytes = retained_directory_page_bytes(
        &page.entries,
        page.entries.iter().map(|entry| entry.name.retained_bytes()),
    )
    .expect("page bytes");
    if let Some(cursor) = page.next {
        cursor.retire(&mut accounting).expect("cursor retirement");
    }
    drop(page.entries);
    crate::kernel::retire_allocation(allocation, bytes, &mut accounting).expect("page retirement");
    let mut insufficient = WorkBudget::UNBOUNDED;
    insufficient.peak_allocation_bytes = peak - 1;
    let mut accounting = crate::kernel::KernelAccounting::standalone(insufficient, &cancellation);
    assert!(
        root.list_directory("/", None, 1_024, &mut accounting)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn attach_open_and_fork_demand_no_source_work() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
    let store = MemoryLazyWorkspaceStore::default();
    let root = attach(&fs, "root", Arc::clone(&source), store.clone())
        .await
        .expect("attach");
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
    .expect("open");
    let child = reopened
        .fork("child", IdempotencyKey::from_bytes([5; 16]), accounting!())
        .await
        .map(|(child, _)| child)
        .expect("fork");
    let first_snapshot = child.snapshot(accounting!()).await.expect("snapshot");
    let second_snapshot = child
        .snapshot(accounting!())
        .await
        .expect("repeat snapshot");
    let resumed_child = root
        .open_related(
            fs.open_workspace("child", WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .expect("child workspace")
                .value,
            accounting!(),
        )
        .await
        .expect("resume related child");
    let sibling = root
        .fork(
            "sibling",
            IdempotencyKey::from_bytes([6; 16]),
            accounting!(),
        )
        .await
        .map(|(child, _)| child)
        .expect("sibling fork");
    let resumed_sibling = resumed_child
        .open_related(
            fs.open_workspace("sibling", WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .expect("sibling workspace")
                .value,
            accounting!(),
        )
        .await
        .expect("constant-work branch switch");
    assert_eq!(
        sibling
            .snapshot(accounting!())
            .await
            .expect("sibling snapshot"),
        resumed_sibling
            .snapshot(accounting!())
            .await
            .expect("resumed sibling snapshot")
    );

    assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    assert_eq!(
        child
            .state(accounting!())
            .await
            .expect("child state")
            .parent_workspace_id,
        Some(root.workspace().id())
    );
    assert_eq!(first_snapshot, second_snapshot);
}
