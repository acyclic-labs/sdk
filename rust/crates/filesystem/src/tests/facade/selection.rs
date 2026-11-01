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
        let (allocation, admitted) = accounting.record_actual_owned(owned)?;
        admitted?;
        let paths = crate::kernel::Owned::new(paths, allocation, owned);
        let failure = fs
            .select_fork_paths(
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
    #[cfg(not(target_arch = "wasm32"))]
    selected_record_membership_cost_and_custody()?;
    let cancel = CancellationToken::new();
    use crate::workspace::ForkOptions;
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("selected-fork-closure-controls", B, &cancel)
        .await
        .map_err(|error| format!("create selected-fork source: {error:?}"))?
        .value;
    let mut transaction = workspace
        .begin_transaction(crate::IdempotencyKey::new(), B, &cancel)
        .await
        .map_err(|error| format!("begin selected-fork source transaction: {error:?}"))?
        .value;
    transaction
        .create_dir_all("/tree/nested", B, &cancel)
        .await
        .map_err(|error| format!("create selected source directories: {error:?}"))?;
    transaction
        .write_text("/tree/nested/file", "linked body")
        .await
        .map_err(|error| format!("write selected source file: {error:?}"))?;
    transaction
        .hard_link("/tree/nested/file", "/tree/nested/alias", B, &cancel)
        .await
        .map_err(|error| format!("link selected source alias: {error:?}"))?;
    transaction
        .hard_link("/tree/nested/file", "/outside-alias", B, &cancel)
        .await
        .map_err(|error| format!("link omitted source alias: {error:?}"))?;
    transaction
        .write_text("/tree/nested/retained", "all links retained")
        .await
        .map_err(|error| format!("write fully selected source file: {error:?}"))?;
    transaction
        .hard_link(
            "/tree/nested/retained",
            "/tree/nested/retained-alias",
            B,
            &cancel,
        )
        .await
        .map_err(|error| format!("link fully selected source file: {error:?}"))?;
    transaction
        .write_text("/omitted", "not inherited")
        .await
        .map_err(|error| format!("write omitted source file: {error:?}"))?;
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
        .await
        .map_err(|error| format!("commit selected-fork source: {error:?}"))?;
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
        .await
        .map_err(|error| format!("fork selected source paths: {error:?}"))?
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
    let retained = initial
        .stat("/tree/nested/retained", B, &cancel)
        .await?
        .value;
    let retained_alias = initial
        .stat("/tree/nested/retained-alias", B, &cancel)
        .await?
        .value;
    assert_eq!(retained.file_id, retained_alias.file_id);
    assert_eq!(retained.link_count, 2);
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
        peak_allocation_bytes: 64 * 1024 * 1024,
        ..crate::performance::work_budget(1_000_000, 1_000_000)
    };
    let replay = initial
        .matches_fork_selection(&source, Some(paths.clone()), finite, &cancellation)
        .await?;
    assert_eq!(replay.value, baseline.value);
    replay.work.verify(finite)?;
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

#[cfg(not(target_arch = "wasm32"))]
fn selected_record_membership_cost_and_custody() -> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::fixed_index::FixedIndex;
    use crate::kernel::allocation::retained::RetainedAllocation;
    use crate::kernel::{FilePayload, FileRecord, KernelAccounting};
    let own = FileRecord {
        file_id: crate::FileId::from_bytes([255; 16]),
        kind: crate::kernel::FileKind::Fifo,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: crate::Digest::from_bytes([7; 32]),
            storage: None,
        },
        bindings: None,
        payload: FilePayload::Empty,
    };
    // The same original ordinary ledger can gain independently retained
    // provider/index owners during a read. Cleanup owns only call scratch.
    let cancel = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(B, &cancel);
    let caller = crate::kernel::Owned::<Vec<u8>>::allocate(97, &mut accounting)?;
    let independent = RetainedAllocation::claim(61, 1, &mut accounting)?;
    let before = crate::facade::readonly_standalone_live(&accounting);
    let initial_live = accounting
        .standalone_parts()
        .ok_or("ordinary ledger missing")?
        .0
        .live_bytes();
    let mut retained = FixedIndex::default();
    retained.get_or_try_insert_with(own.file_id.into_bytes(), &mut accounting, |_| Ok(own))?;
    let retained_live = accounting
        .standalone_parts()
        .ok_or("ordinary ledger missing")?
        .0
        .live_bytes();
    let discarded = crate::kernel::Owned::<Vec<u8>>::allocate(37, &mut accounting)?;
    drop(discarded);
    let ledger = accounting
        .standalone_parts()
        .ok_or("ordinary ledger missing")?
        .0;
    // The original all-live calculation attempts to refund the new retained
    // node. The original ledger must refuse it, without changing any credit.
    let old_retired = ledger
        .live_bytes()
        .checked_sub(initial_live)
        .ok_or("ordinary live count underflow")?;
    assert!(ledger.release(old_retired).is_err());
    crate::facade::readonly_cleanup_standalone(&mut accounting, before, 0)?;
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("ordinary ledger missing")?
            .0
            .live_bytes(),
        retained_live
    );
    assert_eq!(
        retained.get(own.file_id.into_bytes(), &mut accounting)?,
        Some(&own)
    );
    let output = crate::kernel::Owned::<Vec<u8>>::allocate(43, &mut accounting)?;
    crate::facade::readonly_cleanup_standalone(&mut accounting, before, output.bytes)?;
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("ordinary ledger missing")?
            .0
            .scratch_bytes(),
        caller.bytes + output.bytes
    );
    output.retire(&mut accounting)?;
    drop(retained);
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("ordinary ledger missing")?
            .0
            .live_bytes(),
        initial_live
    );
    drop(independent);
    caller.retire(&mut accounting)?;
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("ordinary ledger missing")?
            .0
            .scratch_bytes(),
        0
    );
    let mut visits = Vec::new();
    let mut censuses = Vec::new();
    let mut moved = Vec::new();
    for population in [1_u64, 100_000] {
        let bound =
            acyclic_collections::FixedIndex::<FileRecord, RetainedAllocation, 16>::insertion_bound(
                population + 2,
            )
            .ok_or("selected record bound overflow")?;
        let budget = WorkBudget {
            items_examined: bound.examined_items + 4096,
            allocation_operations: 2 * population + 8,
            peak_allocation_bytes: bound.allocation_bytes + 65_536,
            ..B
        };
        let cancel = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(budget, &cancel).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancel);
        let mut records = FixedIndex::default();
        let mut old = Vec::new();
        for ordinal in (1..=population).rev() {
            let mut bytes = [0; 16];
            bytes[8..].copy_from_slice(&ordinal.to_be_bytes());
            let record = FileRecord {
                file_id: crate::FileId::from_bytes(bytes),
                ..own
            };
            records.get_or_try_insert_with(bytes, &mut accounting, |_| Ok(record))?;
            old.push(record);
        }
        records.get_or_try_insert_with(own.file_id.into_bytes(), &mut accounting, |_| Ok(own))?;
        old.reverse();
        old.push(own);
        let before = accounting.spent();
        let record = records
            .get_mut(own.file_id.into_bytes(), &mut accounting)?
            .ok_or("selected hard link disappeared")?;
        record.link_count += 1;
        visits.push(accounting.spent().items_examined - before.items_examined);
        assert_eq!(
            accounting.spent().allocation_operations,
            before.allocation_operations
        );
        assert_eq!(accounting.spent().bytes_copied, before.bytes_copied);
        assert_eq!(
            records
                .get(own.file_id.into_bytes(), &mut accounting)?
                .map(|record| record.link_count),
            Some(2)
        );

        // Restore the original override census and sorted insertion on the
        // same exact identities. Their counts must expose the forbidden slope.
        let mut examined = 0;
        assert!(
            old.iter()
                .find(|record| {
                    examined += 1;
                    record.file_id == own.file_id
                })
                .is_some()
        );
        censuses.push(examined);
        old.last_mut()
            .ok_or("original selected record disappeared")?
            .link_count = 2;
        let first = FileRecord {
            file_id: crate::FileId::from_bytes([0; 16]),
            ..own
        };
        let position = old
            .binary_search_by_key(&first.file_id, |record| record.file_id)
            .unwrap_err();
        old.push(first);
        moved.push((old.len() - position) * std::mem::size_of::<FileRecord>());
        old[position..].rotate_right(1);
        records
            .get_or_try_insert_with(first.file_id.into_bytes(), &mut accounting, |_| Ok(first))?;
        let mut entry = records.first(&mut accounting)?;
        for expected in &old {
            let (key, record) = entry.ok_or("selected record order ended early")?;
            assert_eq!(*key, expected.file_id.into_bytes());
            assert_eq!(record, expected);
            entry = records.seek_after(*key, &mut accounting)?;
        }
        assert!(entry.is_none());
        cancel.cancel();
        assert!(
            records
                .get_mut(own.file_id.into_bytes(), &mut accounting)
                .is_err()
        );
        drop(records);
        assert_eq!(original.live_allocation_bytes(), baseline);
        original.receipt().verify(budget)?;
    }
    assert_eq!(visits[0], visits[1]);
    assert!(visits[0] <= 145);
    assert_eq!(censuses, [2, 100_001]);
    assert!(moved[1] > moved[0]);
    println!(
        "selected record visits={visits:?}; original override census={censuses:?}; original sorted moved bytes={moved:?}"
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
        storage: None,
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
    }
    reads.store(0, Ordering::Relaxed);
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
