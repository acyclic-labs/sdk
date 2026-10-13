#[tokio::test]
async fn selected_batch_proves_absent_descendants_in_the_original_authenticated_walk()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store).await?;
    let child = LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 255)?;
    let impossible = main
        .child_measured(
            &child,
            config().limits,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            0,
        )?
        .value;
    let missing = NamespacePath::new(
        vec![
            LogicalName::new(NameEncoding::Utf8, b"missing".to_vec(), 255)?,
            child,
        ],
        config().limits,
    )?;
    let queries = [
        impossible.clone(),
        main.clone(),
        missing,
        impossible.clone(),
    ];
    let selected = async {
        let cancellation = &CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
        lookup_paths_with_accounting(
            &store,
            &generation,
            crate::kernel::PathQueries::Owned(&queries),
            config(),
            &mut accounting,
            cancellation,
            None,
            crate::kernel::PathBatchMode::SelectedRestore,
        )
        .await
        .map(|result| result.0)
        .map_err(|error| OperationFailure::new(path_error(error), accounting.spent()))
    }
    .await?;
    assert_eq!(selected.entries.len(), queries.len());
    assert!(selected.entries[0].record.is_none());
    assert!(selected.entries[0].parent.is_none());
    assert_eq!(selected.entries[0].resolved_components, main.depth() as u16);
    assert_eq!(selected.entries[0], selected.entries[3]);
    assert_eq!(
        selected.entries[1].record.map(|record| record.kind),
        Some(FileKind::Regular)
    );
    assert!(selected.entries[2].record.is_none());
    assert_eq!(selected.entries[2].resolved_components, 0);
    assert!(selected.work.backend_read_operations > 0);
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let live_before = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let actual = lookup_paths_with_accounting(
            &store,
            &generation,
            crate::kernel::PathQueries::Owned(&queries),
            config(),
            &mut accounting,
            &cancellation,
            None,
            crate::kernel::PathBatchMode::SelectedRestore,
        )
        .await?;
        assert_eq!(actual.0.entries, selected.entries);
        let (value, allocation) = actual;
        let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
            return Err("selected original lookup returned standalone custody".into());
        };
        assert_eq!(allocation.bytes(), value.retained_allocation_bytes);
        assert_eq!(
            original.live_allocation_bytes(),
            live_before + allocation.bytes()
        );
        drop(value);
        drop(allocation);
        assert_eq!(original.live_allocation_bytes(), live_before);
    }
    let ordinary = lookup_paths(
        &store,
        &generation,
        &[impossible],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .expect_err("ordinary no-follow semantics remain an error");
    assert!(matches!(ordinary.error, PathLookupError::NotDirectory));
    assert!(ordinary.work.backend_read_operations > 0);
    Ok(())
}

#[tokio::test]
async fn selected_batch_keeps_malformed_bindings_and_resource_failures_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) =
        binding_fixture(&store, FileKind::Regular, Some(FileKind::Directory)).await?;
    let query = path
        .child_measured(
            &LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 255)?,
            config().limits,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            0,
        )?
        .value;
    let failure = async {
        let cancellation = &CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
        lookup_paths_with_accounting(
            &store,
            &generation,
            crate::kernel::PathQueries::Owned(&[query.clone()]),
            config(),
            &mut accounting,
            cancellation,
            None,
            crate::kernel::PathBatchMode::SelectedRestore,
        )
        .await
        .map(|result| result.0)
        .map_err(|error| OperationFailure::new(path_error(error), accounting.spent()))
    }
    .await
    .expect_err("a malformed binding is not authenticated absence");
    assert!(matches!(failure.error, PathLookupError::KindMismatch));
    assert!(failure.work.backend_read_operations > 0);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = async {
        let cancellation = &cancellation;
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
        lookup_paths_with_accounting(
            &store,
            &generation,
            crate::kernel::PathQueries::Owned(&[query.clone()]),
            config(),
            &mut accounting,
            cancellation,
            None,
            crate::kernel::PathBatchMode::SelectedRestore,
        )
        .await
        .map(|result| result.0)
        .map_err(|error| OperationFailure::new(path_error(error), accounting.spent()))
    }
    .await
    .expect_err("cancelled selected batch must fail before allocation/read");
    assert_eq!(failure.work.allocation_operations, 0);
    assert_eq!(failure.work.backend_read_operations, 0);
    let failure = async {
        let cancellation = &CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(
            WorkBudget {
                allocation_operations: 0,
                ..WorkBudget::UNBOUNDED
            },
            cancellation,
        );
        lookup_paths_with_accounting(
            &store,
            &generation,
            crate::kernel::PathQueries::Owned(&[query]),
            config(),
            &mut accounting,
            cancellation,
            None,
            crate::kernel::PathBatchMode::SelectedRestore,
        )
        .await
        .map(|result| result.0)
        .map_err(|error| OperationFailure::new(path_error(error), accounting.spent()))
    }
    .await
    .expect_err("unadmitted selected batch must fail before allocation/read");
    assert_eq!(failure.work.allocation_operations, 0);
    assert_eq!(failure.work.backend_read_operations, 0);
    Ok(())
}
