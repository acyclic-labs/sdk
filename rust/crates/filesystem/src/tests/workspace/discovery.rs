use super::*;

/// One measured listing of the pinned root: its page and the operation's total work.
type Listing = (crate::kernel::DirectoryPage, crate::WorkCounters);

/// The discovery oracle, independent of the implementation's own output: the fixture wrote exactly one file into the
/// pinned generation, so the page is exactly `visible.txt` with nothing after it. Retained workspaces and later writes
/// must not change a warm listing at all, including every work counter (reads, bytes, copies, allocations, items).
fn check_discovery(before: &Listing, after: &Listing) -> Result<(), String> {
    for (label, (page, _)) in [("before", before), ("after", after)] {
        let names = page
            .entries
            .iter()
            .map(|entry| (entry.name.as_bytes(), entry.kind))
            .collect::<Vec<_>>();
        if names != [(b"visible.txt".as_slice(), crate::kernel::FileKind::Regular)]
            || page.has_more
            || page.next_residency.is_some()
        {
            return Err(format!(
                "{label}: listing differs from the fixture: {page:?}"
            ));
        }
    }
    if before != after {
        return Err(format!("warm listings differ: {before:?} != {after:?}"));
    }
    let (page, work) = after;
    if page.work.source_entries_visited != 0
        || work.source_entries_visited != 0
        || work.materializations != 0
    {
        return Err(format!(
            "a pinned listing visited source entries or materialized: {work:?}"
        ));
    }
    Ok(())
}

#[tokio::test]
async fn pinned_directory_discovery_has_constant_work_as_retained_workspaces_grow()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("discovered-agent"))?;
    workspace.write_text("/visible.txt", "visible").await?;
    let pinned = unbounded!(workspace, head())?;
    let cancellation = CancellationToken::new();
    let list = || async {
        let listed = pinned
            .list_directory("/", None, 1, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        Ok::<Listing, Box<dyn Error>>((listed.value.0, listed.work))
    };
    // The first listing decodes and caches the root's objects (#788); both measured listings start from that same
    // warm cache state, so their work must be equal, not merely no larger.
    let cold = list().await?;
    let before = list().await?;
    // Exact receipts detect even one extra read or copy; a large fixture adds no guarantee.
    const RETAINED: usize = 64;
    let mut retained = Vec::with_capacity(RETAINED);
    for index in 0..RETAINED {
        let other = unbounded!(fs, create_workspace(&format!("retained-agent-{index}")))?;
        retained.push(unbounded!(other, head())?);
    }
    workspace
        .write_text("/late.txt", "not in pinned view")
        .await?;
    let after = list().await?;
    assert_eq!(retained.len(), RETAINED);
    check_discovery(&before, &after)?;
    // The cold listing reads the same page; the cache only ever removes work from it.
    assert_eq!(cold.0.entries, after.0.entries);
    assert!(after.1.backend_read_operations <= cold.1.backend_read_operations);
    // Negative controls: each fault the oracle targets is detected.
    let mut more = after.clone();
    more.0.has_more = true;
    let mut wrong = after.clone();
    wrong.0.entries.clear();
    let mut extra_read = after.clone();
    extra_read.1.backend_read_operations += 1;
    let mut extra_copy = after.clone();
    extra_copy.0.work.bytes_copied += 1;
    for (fault, observed) in [
        ("has_more on both listings", (more.clone(), more)),
        ("constant empty listing", (wrong.clone(), wrong)),
        ("an added backend read", (before.clone(), extra_read)),
        ("an added copy", (before.clone(), extra_copy)),
    ] {
        assert!(
            check_discovery(&observed.0, &observed.1).is_err(),
            "oracle missed {fault}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn bounded_direct_paths_retain_all_admitted_aliases() -> Result<(), Box<dyn Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("bounded-direct-paths"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    transaction.write_text("/a", "body").await?;
    unbounded!(transaction, hard_link("/a", "/b"))?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    let generation = unbounded!(workspace, head())?;
    let id = unbounded!(generation, stat("/a"))?.file_id;
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let refused = generation
        .paths_for_file_ids(
            [id],
            16,
            WorkBudget {
                backend_read_operations: 0,
                ..budget
            },
            &cancellation,
        )
        .await;
    assert!(
        matches!(
            refused,
            Err(crate::OperationFailure {
                error: WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                    counter: "backend_read_operations",
                    ..
                }),
                ..
            })
        ),
        "reverse paths ignored caller read admission"
    );
    let complete = generation
        .paths_for_file_ids([id], 16, budget, &cancellation)
        .await?
        .value;
    assert_eq!(
        complete.entries(),
        &[(id, vec!["/a".to_owned(), "/b".to_owned()])]
    );
    assert!(complete.is_complete());
    let bounded = generation
        .paths_for_file_ids([id], 1, budget, &cancellation)
        .await?
        .value;
    assert_eq!(bounded.entries()[0].1.len(), 1);
    assert!(!bounded.is_complete());
    for admitted_reads in [0, budget.backend_read_operations] {
        let source = crate::workspace::accounting_tests::Prefunded {
            budget: WorkBudget {
                backend_read_operations: admitted_reads,
                ..budget
            },
            held: std::sync::Mutex::new(None),
        };
        let (result, spent, live, baseline) = generation
            .paths_for_file_ids([id, id], 16, &source, &cancellation)
            .await;
        spent.verify(source.budget)?;
        if admitted_reads == 0 {
            assert!(result.is_err());
            assert_eq!(live, baseline, "refused paths retired independent backing");
        } else {
            let result = result?;
            assert_eq!(result.entries(), complete.entries());
            assert!(spent.backend_read_operations > 0);
            assert!(spent.bytes_encoded >= 4);
            assert!(
                live > baseline,
                "reverse paths returned without original backing"
            );
        }
    }
    {
        let mut original = crate::AuthorityWork::new(budget, &cancellation);
        let mut accounting = original.accounting();
        let independent = crate::kernel::Owned::<Vec<u8>>::allocate(73, &mut accounting)?;
        let independent = crate::kernel::allocation::retained::Retained::from_owned(
            independent,
            &mut accounting,
        )?;
        let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
        let returned = generation
            .paths_for_file_ids_with_accounting([id], 16, &mut accounting, &cancellation)
            .await?;
        assert_eq!(returned.entries(), complete.entries());
        assert!(accounting.standalone_parts().unwrap().0.live_bytes() > baseline);
        drop(returned);
        assert_eq!(
            accounting.standalone_parts().unwrap().0.live_bytes(),
            baseline,
            "ordinary paths did not retire exactly their own backing"
        );
        drop(independent);
        accounting.spent().verify(budget)?;
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        generation
            .paths_for_file_ids([id], 16, budget, &cancelled)
            .await,
        Err(crate::OperationFailure {
            error: WorkspaceError::Cancelled(_),
            ..
        })
    ));
    assert!(
        generation
            .paths_for_file_ids(
                [id],
                16,
                WorkBudget {
                    output_bytes: 0,
                    ..budget
                },
                &cancellation
            )
            .await
            .is_err()
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let birth = original.live_allocation_bytes();
        let mut work = crate::AuthorityWork::native(&original, &cancellation);
        let independent = crate::kernel::Owned::<Vec<u8>>::allocate(73, &mut work.accounting())?;
        let baseline = original.live_allocation_bytes();
        let returned = generation
            .paths_for_file_ids([id], 16, &original, &cancellation)
            .await?;
        assert_eq!(returned.entries(), complete.entries());
        assert!(original.live_allocation_bytes() > baseline);
        drop(returned);
        assert_eq!(
            original.live_allocation_bytes(),
            baseline,
            "paths did not retire exactly their own backing"
        );
        independent.retire(&mut work.accounting())?;
        assert_eq!(original.live_allocation_bytes(), birth);
        original.receipt().verify(budget)?;
    }
    Ok(())
}
