use super::*;

#[tokio::test]
async fn customer_reads_are_bounded_sparse_link_aware_and_generation_exact()
-> Result<(), Box<dyn Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    reverse_selection_cost_and_custody()?;
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("read-shapes"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, create_directory("/tree"))?;
    unbounded!(
        transaction,
        write("/tree/a", OwnedBytes::from_static(b"abcdef"))
    )?;
    unbounded!(
        transaction,
        write("/tree/sparse", OwnedBytes::from(vec![1; 300]))
    )?;
    unbounded!(transaction, hard_link("/tree/a", "/tree/b"))?;
    unbounded!(
        transaction,
        create_symbolic_link("/tree/link", OwnedBytes::from_static(b"a"))
    )?;
    unbounded!(
        transaction,
        zero_range(
            "/tree/a",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false
        )
    )?;
    unbounded!(transaction, resize("/tree/a", 8))?;
    unbounded!(
        transaction,
        zero_range(
            "/tree/sparse",
            crate::ByteRange {
                offset: 100,
                length: 20,
            },
            true,
            false
        )
    )?;
    unbounded!(transaction, resize("/tree/sparse", 400))?;
    let TransactionCommit::Committed(exact) = commit(&mut transaction).await? else {
        return Err("shape publication did not commit".into());
    };

    let file_id = unbounded!(workspace, stat("/tree/a"))?.file_id;
    let directory_id = unbounded!(workspace, stat("/tree"))?.file_id;
    let missing_id = FileId::from_bytes([0xff; 16]);
    let paths = exact
        .paths_for_file_ids([missing_id, file_id, directory_id, file_id], 32)
        .await?;
    assert_eq!(
        paths.get(&file_id),
        Some(&vec!["/tree/a".to_owned(), "/tree/b".to_owned()])
    );
    assert_eq!(paths.get(&directory_id), Some(&vec!["/tree".to_owned()]));
    assert!(!paths.contains_key(&missing_id));

    assert_eq!(
        unbounded!(workspace, read_range("/tree/a", 1, 5))?.bytes,
        OwnedBytes::from_static(b"b\0\0ef")
    );
    let a = unbounded!(workspace, stat("/tree/a"))?;
    let b = unbounded!(workspace, stat("/tree/b"))?;
    assert_eq!(a.file_id, b.file_id);
    assert_eq!(a.link_count, 2);
    assert_eq!(a.logical_bytes, Some(8));
    assert_eq!(a.kind, FileKind::Regular);
    assert_eq!(
        unbounded!(workspace, read_symbolic_link("/tree/link"))?.bytes,
        OwnedBytes::from_static(b"a")
    );

    let listing = unbounded!(workspace, head())?;
    let first = unbounded!(listing, list_directory("/tree", None, 1))?.0;
    assert_eq!(first.entries.len(), 1);
    assert!(first.has_more);
    workspace.write_text("/tree/late", "new").await?;
    let second = unbounded!(
        listing,
        list_directory("/tree", Some(&first.entries[0].name), 16)
    )?
    .0;
    assert_eq!(second.entries.len(), 3);
    assert!(!second.has_more);

    let extents = workspace.plan_extents("/tree/sparse", 0, 400, 16).await?;
    assert!(
        extents
            .spans
            .iter()
            .any(|span| span.kind == WorkspaceExtentKind::AllocatedZero)
    );
    assert!(
        extents
            .spans
            .iter()
            .any(|span| span.kind == WorkspaceExtentKind::Content)
    );

    workspace.write_text("/tree/a", "changed").await?;
    assert_eq!(
        unbounded!(exact, read("/tree/a", 16))?,
        OwnedBytes::from_static(b"ab\0\0ef\0\0")
    );
    assert_eq!(
        unbounded!(workspace, read("/tree/a", 16))?,
        OwnedBytes::from_static(b"changed")
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn reverse_selection_cost_and_custody() -> Result<(), Box<dyn Error>> {
    use crate::kernel::KernelAccounting;
    use crate::kernel::allocation::retained::RetainedAllocation;
    let cancel = CancellationToken::new();
    let mut counts = Vec::new();
    for population in [1_u64, 100_000] {
        let bound = acyclic_collections::FixedIndex::<(), RetainedAllocation, 16>::insertion_bound(
            population,
        )
        .ok_or(crate::WorkError::Overflow)?;
        let budget = WorkBudget {
            items_examined: 430 * 2 * population + 4,
            bytes_copied: 16 * population,
            allocation_operations: 2 * population + 4,
            peak_allocation_bytes: bound.allocation_bytes + 16 * population + 65_536,
            ..crate::performance::BOUNDARY_BUDGET
        };
        let original =
            crate::NativeMaterializationWork::new(budget, &cancel).map_err(|(error, _)| error)?;
        let mut accounting = KernelAccounting::native(&original, &cancel);
        let prior = RetainedAllocation::claim(29, 1, &mut accounting)?;
        let baseline = original.live_allocation_bytes();
        let before = accounting.spent();
        let ids = sorted_changed_file_ids(
            (1..=population).rev().flat_map(|ordinal| {
                let id = FileId::from_bytes(u128::from(ordinal).to_be_bytes());
                [id, id]
            }),
            &mut accounting,
        )?;
        let spent = accounting.spent();
        assert_eq!(ids.0.len() as u64, population);
        for (ordinal, id) in ids.0.iter().enumerate() {
            assert_eq!(id.into_bytes(), (ordinal as u128 + 1).to_be_bytes());
        }
        assert!(spent.items_examined - before.items_examined <= budget.items_examined);
        assert_eq!(spent.bytes_copied - before.bytes_copied, 16 * population);
        counts.push((
            spent.items_examined - before.items_examined,
            spent.bytes_copied - before.bytes_copied,
        ));
        let bytes = workspace_vec_bytes(&ids.0);
        reverse_drop(ids, bytes, &mut accounting)?;
        assert_eq!(original.live_allocation_bytes(), baseline);
        drop(prior);
    }
    eprintln!("reverse identity input=[1,100000] examinations/copy-bytes={counts:?}");

    // Every bit participates in the original full-key order, including zero.
    let mut input: Vec<_> = (0..128)
        .rev()
        .map(|bit| FileId::from_bytes((1_u128 << bit).to_be_bytes()))
        .collect();
    input.extend([FileId::from_bytes([0; 16]); 2]);
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let ids = sorted_changed_file_ids(input, &mut accounting)?;
    assert_eq!(ids.0.len(), 129);
    assert_eq!(ids.0[0], FileId::from_bytes([0; 16]));
    assert!(ids.0.windows(2).all(|pair| pair[0] < pair[1]));
    let bytes = workspace_vec_bytes(&ids.0);
    reverse_drop(ids, bytes, &mut accounting)?;

    let input = [3_u128, 1, 2, 1].map(|id| FileId::from_bytes(id.to_be_bytes()));
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let ids = sorted_changed_file_ids(input, &mut accounting)?;
    let spent = accounting.spent();
    let bytes = workspace_vec_bytes(&ids.0);
    reverse_drop(ids, bytes, &mut accounting)?;
    let budgets = (0..spent.items_examined)
        .map(|items_examined| WorkBudget {
            items_examined,
            ..crate::performance::BOUNDARY_BUDGET
        })
        .chain([
            WorkBudget {
                bytes_copied: spent.bytes_copied - 1,
                ..crate::performance::BOUNDARY_BUDGET
            },
            WorkBudget {
                allocation_operations: 1,
                ..crate::performance::BOUNDARY_BUDGET
            },
        ]);
    for budget in budgets {
        for native in [false, true] {
            let original = crate::NativeMaterializationWork::new(budget, &cancel)
                .map_err(|(error, _)| error)?;
            let mut accounting = if native {
                KernelAccounting::native(&original, &cancel)
            } else {
                KernelAccounting::standalone(budget, &cancel)
            };
            // Prior independent credit stays live while this call exhausts
            // the remaining allowance, including its original ledger owner.
            let prior = RetainedAllocation::claim(29, 0, &mut accounting)?;
            let live = |accounting: &KernelAccounting<'_>| match accounting {
                KernelAccounting::Standalone { allocations, .. } => allocations.live_bytes(),
                KernelAccounting::BorrowedStandalone { allocations, .. } => {
                    allocations.live_bytes()
                }
                KernelAccounting::NativeOriginal { work, .. } => work.live_allocation_bytes(),
            };
            let baseline = live(&accounting);
            assert!(sorted_changed_file_ids(input, &mut accounting).is_err());
            assert_eq!(live(&accounting), baseline);
            drop(prior);
        }
    }
    cancel.cancel();
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    assert!(sorted_changed_file_ids([], &mut accounting).is_err());
    Ok(())
}

#[tokio::test]
async fn generation_lookup_paths_keeps_original_work_backing_and_authority()
-> Result<(), Box<dyn Error>> {
    use crate::access::{Action, INITIAL_POLICY, fixtures};
    use crate::workspace::accounting_tests::{Observation, ObservedAuthority, Prefunded};
    use std::sync::{Arc, Mutex};

    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let authority = Arc::new(ObservedAuthority::default());
    let fs = Fs::new(
        authority.clone(),
        Arc::new(crate::MemoryObjectStore::default()),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let workspace = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "lookup-paths",
            crate::VolumeInitialization {
                config: crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                access_policy: Some(INITIAL_POLICY),
            },
            crate::OperationId::new(),
            budget,
            &cancellation,
            None,
        )
        .await?
        .value
        .workspace();
    workspace.write_text("/present", "body").await?;
    let reader = workspace
        .clone()
        .bind_coarse_access(&Action::ALL, &cancellation)
        .await?;
    let generation = reader.head(budget, &cancellation).await?.value;
    let config = workspace.volume_config();
    let present = customer_path("/present", config)?;
    let paths = [
        present.clone(),
        customer_path("/absent", config)?,
        present.clone(),
    ];
    // Later publications cannot change the selected immutable batch.
    workspace.write_text("/present", "later").await?;
    for reads in [0, budget.backend_read_operations] {
        let source = Prefunded {
            budget: WorkBudget {
                backend_read_operations: reads,
                ..budget
            },
            held: Mutex::new(None),
        };
        let (result, spent, live, baseline) = generation
            .lookup_paths(&paths, &source, &cancellation)
            .await;
        spent.verify(source.budget)?;
        if reads == 0 {
            assert!(matches!(
                result,
                Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                    counter: "backend_read_operations",
                    ..
                }))
            ));
            assert_eq!(spent.backend_read_operations, 0);
            assert_eq!(live, baseline);
        } else {
            let (lookup, allocation) = result?;
            assert!(allocation.is_none());
            assert_eq!(lookup.work, spent);
            assert_eq!(lookup.entries.len(), paths.len());
            assert_eq!(
                regular_file_bytes(
                    lookup.entries[0]
                        .record
                        .ok_or("selected file disappeared")?
                )?,
                4
            );
            assert!(lookup.entries[1].record.is_none());
            assert_eq!(lookup.entries[0], lookup.entries[2]);
            assert_eq!(live, baseline + lookup.retained_allocation_bytes);
            assert!(spent.items_examined > 1);
        }
    }

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = generation
        .lookup_paths(&paths, budget, &cancelled)
        .await
        .unwrap_err();
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));

    for withdraw in [false, true] {
        let cancellation = CancellationToken::new();
        let source = Prefunded {
            budget,
            held: Mutex::new(None),
        };
        let funded = &source;
        let mut accounting = crate::facade::FsWork::accounting(&funded, &cancellation);
        let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
        let (producer, completion) = tokio::sync::oneshot::channel();
        *authority.observation.lock().unwrap() = Some(Observation {
            minimum: baseline,
            completion: Some(completion),
        });
        let mut operation = Box::pin(crate::workspace::read_core::lookup_paths(
            &reader,
            GenerationSelector::Exact(generation.id()),
            &paths,
            &mut accounting,
            &cancellation,
        ));
        assert!(futures::poll!(operation.as_mut()).is_pending());
        assert!(authority.observation.lock().unwrap().is_none());
        if withdraw {
            cancellation.cancel();
            drop(operation);
            drop(producer);
        } else {
            drop(producer);
            assert!(matches!(
                operation.await,
                Err(WorkspaceError::Filesystem(FsError::WorkspaceDeleted))
            ));
        }
        assert_eq!(
            accounting.standalone_parts().unwrap().0.live_bytes(),
            baseline
        );
        assert!(accounting.spent().backend_read_operations > 0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let original =
        crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    #[cfg(not(target_arch = "wasm32"))]
    let native_baseline = original.live_allocation_bytes();
    #[cfg(not(target_arch = "wasm32"))]
    {
        let (lookup, allocation) = generation
            .lookup_paths(&paths, &original, &cancellation)
            .await?;
        let allocation = allocation.ok_or("native batch lost its original result allocation")?;
        assert_eq!(allocation.bytes(), lookup.retained_allocation_bytes);
        assert!(original.live_allocation_bytes() >= native_baseline + allocation.bytes());
        assert_eq!(lookup.entries[0], lookup.entries[2]);
        assert!(lookup.entries[1].record.is_none());
        drop((lookup, allocation));
    }

    // An old generation cannot bypass a later policy revocation.
    fixtures::publish_policy(&workspace, &fixtures::denied(Action::Read, "present")).await?;
    let source = Prefunded {
        budget,
        held: Mutex::new(None),
    };
    let (result, _, live, baseline) = generation
        .lookup_paths(&paths, &source, &cancellation)
        .await;
    assert!(matches!(result, Err(WorkspaceError::AccessDenied)));
    assert_eq!(live, baseline);
    drop((generation, reader, workspace, fs, authority));
    #[cfg(not(target_arch = "wasm32"))]
    assert_eq!(original.live_allocation_bytes(), native_baseline);
    Ok(())
}

/// An immutable generation's cached absence is exact: asking again for
/// identities that have no paths answers empty rather than validating an
/// empty path batch.
#[tokio::test]
async fn repeated_lookups_of_absent_identities_answer_empty() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("absent-identities"))?;
    workspace.write_text("/kept", "kept").await?;
    workspace.write_text("/gone", "gone").await?;
    let gone = unbounded!(workspace, stat("/gone"))?.file_id;
    workspace.remove("/gone").await?;
    let head = unbounded!(workspace, head())?;
    for _ in 0..2 {
        assert!(head.paths_for_file_ids([gone], 32).await?.is_empty());
    }
    Ok(())
}
