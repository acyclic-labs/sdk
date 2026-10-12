use super::*;

#[tokio::test]
async fn selected_fork_invalid_native_paths_preflight_before_allocation_and_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    use crate::workspace::WorkspaceError;
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("selected-fork-invalid-paths", B, &cancel)
        .await?
        .value;
    workspace.write_text("/file", "body").await?;
    let source = workspace.head(B, &cancel).await?.value;
    let cancellation = CancellationToken::new();
    let checkout = workspace
        .checkout(
            GenerationSelector::Exact(source.id()),
            CheckoutMode::read_only_pinned(),
            B,
            &cancellation,
        )
        .await?
        .value;
    let limits = source.workspace.volume.config().limits;
    for invalid in [
        "/../file".to_owned(),
        "/bad\0name".to_owned(),
        format!(
            "/{}",
            "x".repeat(limits.maximum_component_bytes as usize + 1)
        ),
        format!("/{}", "a/".repeat(limits.maximum_path_depth as usize + 1)),
        format!("/{}", "x".repeat(limits.maximum_path_bytes as usize + 1)),
    ] {
        let paths = vec![invalid];
        let owned = (paths.capacity() * std::mem::size_of::<String>() + paths[0].capacity()) as u64;
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget {
                allocation_operations: 0,
                ..B
            },
            &cancellation,
        );
        let failure = fs
            .select_fork_paths_with_accounting(
                &source,
                checkout.root(),
                paths,
                &mut accounting,
                &cancellation,
            )
            .await
            .err()
            .ok_or("invalid selected path unexpectedly succeeded")?;
        assert!(matches!(failure, WorkspaceError::NamespacePath(_)));
        let work = accounting.spent();
        assert_eq!(work.allocation_operations, 0);
        assert_eq!(work.backend_read_operations, 0);
        assert_eq!(work.page_reads, 0);
        assert_eq!(work.bytes_copied, 0);
        assert_eq!(work.peak_allocation_bytes, owned);
    }
    Ok(())
}

#[tokio::test]
async fn selected_fork_nested_hardlink_closure_replays_finite_work_and_root_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    use crate::workspace::{ForkOptions, WorkspaceError};
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("selected-fork-closure-controls", B, &cancel)
        .await?
        .value;
    let mut transaction = workspace
        .begin_transaction(crate::IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction
        .create_dir_all("/tree/nested", B, &cancel)
        .await?
        .value;
    transaction
        .write_text("/tree/nested/file", "linked body")
        .await?;
    transaction
        .hard_link("/tree/nested/file", "/tree/nested/alias", B, &cancel)
        .await?
        .value;
    transaction
        .hard_link("/tree/nested/file", "/outside-alias", B, &cancel)
        .await?
        .value;
    transaction.write_text("/omitted", "not inherited").await?;
    transaction
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            B,
            &cancel,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::default(),
                })
            },
        )
        .await?
        .value;
    let source = workspace.head(B, &cancel).await?.value;
    let paths = vec![
        "/tree".to_owned(),
        "/tree/nested/file".to_owned(),
        "/tree".to_owned(),
    ];
    let child = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "selected-fork-closure-child",
            ForkOptions::from_generation(source.clone(), crate::IdempotencyKey::new())
                .inherit_paths(paths.clone()),
            crate::PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    let initial = child.head(B, &cancel).await?.value;
    assert_eq!(
        (child
            .read("/tree/nested/alias", 32, B, &cancel)
            .await?
            .value)
            .as_ref(),
        b"linked body"
    );
    assert!(child.read("/outside-alias", 32, B, &cancel).await.is_err());
    assert!(child.read("/omitted", 32, B, &cancel).await.is_err());
    let file = initial.stat("/tree/nested/file", B, &cancel).await?.value;
    let alias = initial.stat("/tree/nested/alias", B, &cancel).await?.value;
    assert_eq!(file.file_id, alias.file_id);
    assert_eq!(file.link_count, 2);
    assert!(
        initial
            .matches_fork_selection(&source, Some(paths.clone()), B, &cancel)
            .await?
            .value
    );
    assert!(
        initial
            .matches_fork_selection(&source, Some(vec!["/tree".to_owned()]), B, &cancel)
            .await?
            .value
    );
    let cancellation = CancellationToken::new();
    let baseline = initial
        .matches_fork_selection(&source, Some(paths.clone()), B, &cancellation)
        .await?;
    assert!(baseline.value);
    assert!(baseline.work.allocation_operations > 0);
    assert!(baseline.work.backend_read_operations > 0);
    let finite = WorkBudget {
        authority_records_read: 1_000_000,
        authority_records_appended: 1_000_000,
        authority_bytes_read: 1_000_000,
        authority_bytes_written: 1_000_000,
        object_probes: 1_000_000,
        backend_read_operations: 1_000_000,
        backend_write_operations: 1_000_000,
        durability_operations: 1_000_000,
        page_reads: 1_000_000,
        page_writes: 1_000_000,
        object_bytes_read: 1_000_000,
        object_bytes_written: 1_000_000,
        bytes_hashed: 1_000_000,
        bytes_copied: 1_000_000,
        bytes_encoded: 1_000_000,
        source_bytes_read: 1_000_000,
        source_path_components: 1_000_000,
        source_entries_visited: 1_000_000,
        output_bytes: 1_000_000,
        items_examined: 1_000_000,
        items_returned: 1_000_000,
        allocation_operations: 1_000_000,
        peak_allocation_bytes: 64 * 1024 * 1024,
        materializations: 1_000_000,
    };
    let replay = initial
        .matches_fork_selection(&source, Some(paths.clone()), finite, &cancellation)
        .await?;
    assert_eq!(replay.value, baseline.value);
    assert_eq!(replay.work, baseline.work);
    let denied = initial
        .matches_fork_selection(
            &source,
            Some(paths),
            WorkBudget {
                peak_allocation_bytes: baseline.work.peak_allocation_bytes - 1,
                ..finite
            },
            &cancellation,
        )
        .await
        .err()
        .ok_or("tight peak unexpectedly admitted the entire selection")?;
    assert!(matches!(denied.error, WorkspaceError::Work(_)));
    assert!(denied.work.peak_allocation_bytes > 0);
    assert!(denied.work.backend_read_operations > 0);
    assert!(denied.work.allocation_operations > 0);
    let all = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "selected-fork-root-child",
            ForkOptions::from_generation(source.clone(), crate::IdempotencyKey::new())
                .inherit_paths(vec!["/tree".to_owned(), "/".to_owned(), "/".to_owned()]),
            crate::PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    let all = all.head(B, &cancel).await?.value;
    assert!(
        all.matches_fork_selection(&source, None, B, &cancel)
            .await?
            .value
    );
    assert!(
        all.matches_fork_selection(&source, Some(vec!["/".to_owned()]), B, &cancel)
            .await?
            .value
    );
    assert!(
        !initial
            .matches_fork_selection(&source, Some(vec!["/".to_owned()]), B, &cancel)
            .await?
            .value
    );
    Ok(())
}

#[tokio::test]
async fn selected_fork_verification_zero_allocation_and_cancellation_keep_failed_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    use crate::workspace::{ForkOptions, WorkspaceError};
    let tracked = Arc::new(std::sync::Mutex::new(None));
    let reads = Arc::new(AtomicUsize::new(0));
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        CountingObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            tracked: Arc::clone(&tracked),
            reads: Arc::clone(&reads),
            unreadable: Arc::default(),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let workspace = (fs.create_workspace("selected-fork-verification-gates", B, &cancel))
        .await?
        .value;
    (workspace.write_text("/file", "body")).await?;
    let source = (async {
        workspace
            .head(B, &cancel)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    })
    .await?;
    let child = (workspace.fork::<_, _, crate::WorkspaceError>(
        "selected-fork-verification-child",
        ForkOptions::from_generation(source.clone(), crate::IdempotencyKey::new())
            .inherit_paths(vec!["/file".to_owned()]),
        crate::PublicationPermit::Unrestricted,
        B,
        &cancel,
        &mut |_| {
            std::future::ready(Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            }))
        },
    ))
    .await?
    .value;
    let initial = (async {
        child
            .head(B, &cancel)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    })
    .await?;
    *tracked.lock().map_err(|_| "tracking lock poisoned")? = Some(ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: source.id().digest(),
    });
    reads.store(0, Ordering::Relaxed);
    let cancellation = CancellationToken::new();
    for budget in [
        WorkBudget::default(),
        WorkBudget {
            allocation_operations: 0,
            ..B
        },
    ] {
        let failed = (initial.matches_fork_selection(
            &source,
            Some(vec!["/file".to_owned()]),
            budget,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("verification ignored its allocation gate")?;
        assert!(matches!(failed.error, WorkspaceError::Work(_)));
        assert_eq!(failed.work.allocation_operations, 0);
        assert_eq!(failed.work.backend_read_operations, 0);
        assert_eq!(reads.load(Ordering::Relaxed), 0);
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failed =
        (initial.matches_fork_selection(&source, Some(vec!["/file".to_owned()]), B, &cancelled))
            .await
            .err()
            .ok_or("verification ignored cancellation")?;
    assert!(matches!(failed.error, WorkspaceError::Cancelled(_)));
    assert_eq!(failed.work.allocation_operations, 0);
    assert_eq!(failed.work.backend_read_operations, 0);
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    Ok(())
}
