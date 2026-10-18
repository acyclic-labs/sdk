#[tokio::test]
async fn batch_rejects_authenticated_child_bounds_and_preserves_failure_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store).await?;
    let root_id = FileId::from_bytes([1; 16]);
    let src_id = FileId::from_bytes([2; 16]);
    let metadata = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let leaf = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"src".to_vec(), 255)?,
                file_id: src_id,
                kind: FileKind::Directory,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let malformed_root_tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Internal(vec![Child {
                first: LogicalName::new(NameEncoding::Utf8, b"zzz".to_vec(), 255)?,
                page: leaf,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let file_table = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![FileRecord {
                file_id: root_id,
                kind: FileKind::Directory,
                link_count: 1,
                metadata,
                payload: FilePayload::Directory {
                    entries: malformed_root_tree,
                },
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let malformed_generation = GenerationRoot {
        volume_id: generation.volume_id,
        root_file_id: root_id,
        file_table,
        parents: Vec::new(),
        required_features: 0,
    };
    let src = NamespacePath::new(
        vec![LogicalName::new(NameEncoding::Utf8, b"src".to_vec(), 255)?],
        config().limits,
    )?;
    let failure = lookup_paths(
        &store,
        &malformed_generation,
        &[main, src],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("malformed routing unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::Tree(TreeReadError::ChildBoundsMismatch)
    ));
    assert!(failure.work.backend_read_operations > 0);
    assert!(failure.work.page_reads > 0);
    assert!(failure.work.items_examined > 0);
    assert!(failure.work.peak_allocation_bytes > 0);
    Ok(())
}

#[tokio::test]
async fn batch_paths_share_prefix_and_file_table_frontiers_and_preserve_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store).await?;
    let missing = NamespacePath::new(
        vec![
            LogicalName::new(NameEncoding::Utf8, b"src".to_vec(), 255)?,
            LogicalName::new(NameEncoding::Utf8, b"missing".to_vec(), 255)?,
        ],
        config().limits,
    )?;
    let root = NamespacePath::new(Vec::new(), config().limits)?;
    let batch = lookup_paths(
        &store,
        &generation,
        &[main.clone(), missing, main.clone(), root.clone()],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(batch.entries.len(), 4);
    assert_eq!(
        batch.entries[0].record.map(|record| record.kind),
        Some(FileKind::Regular)
    );
    assert_eq!(batch.entries[1].record, None);
    assert_eq!(batch.entries[1].resolved_components, 1);
    assert_eq!(
        batch.entries[0].parent.map(|record| record.file_id),
        Some(FileId::from_bytes([2; 16]))
    );
    assert_eq!(batch.entries[1].parent, batch.entries[0].parent);
    assert_eq!(batch.entries[0], batch.entries[2]);
    assert_eq!(
        batch.entries[3].record.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    assert_eq!(batch.entries[3].parent, None);
    assert_eq!(batch.work.page_reads, 5);
    assert_eq!(batch.work.backend_read_operations, 3);
    let borrowed_paths = [&main, &root];
    let borrowed = (lookup_path_refs(
        &store,
        &generation,
        &borrowed_paths,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    let owned = lookup_paths(
        &store,
        &generation,
        &[main, root],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(borrowed, owned);
    Ok(())
}

#[tokio::test]
async fn batch_observation_captures_exactly_each_paths_observation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store).await?;
    let name = |bytes: &[u8]| LogicalName::new(NameEncoding::Utf8, bytes.to_vec(), 255);
    let paths = [
        main.clone(),
        NamespacePath::new(vec![name(b"src")?, name(b"missing")?], config().limits)?,
        NamespacePath::new(vec![name(b"absent")?, name(b"deeper")?], config().limits)?,
        NamespacePath::new(vec![name(b"alias")?, name(b"main.rs")?], config().limits)?,
        main,
        NamespacePath::new(Vec::new(), config().limits)?,
        NamespacePath::new(vec![name(b"src")?], config().limits)?,
    ];
    let cancellation = CancellationToken::new();
    let observed = (observe_paths_async(
        &store,
        &generation,
        &paths,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let looked_up = lookup_paths(
        &store,
        &generation,
        &paths,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(observed.lookup.entries, looked_up.entries);
    let mut expected = std::collections::BTreeMap::new();
    for path in &paths {
        let single = (observe_path_async(
            &store,
            &generation,
            path,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await?;
        for dependency in single.dependencies {
            expected.insert(dependency.region, dependency.expected);
        }
    }
    let mut batched = std::collections::BTreeMap::new();
    for dependency in observed.dependencies {
        let prior = batched.insert(dependency.region, dependency.expected);
        assert!(prior.is_none_or(|prior| prior == dependency.expected));
    }
    assert_eq!(batched, expected);
    // Binding-only mode drops exactly the terminal records, never a name
    // binding, and matches the single-path walk in the same mode.
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let bindings = observe_paths_async_with_accounting::<false, _>(
        &store,
        &generation,
        &paths,
        config(),
        &mut accounting,
        &cancellation,
        None,
    )
    .await?
    .0;
    assert_eq!(bindings.lookup.entries, looked_up.entries);
    let mut single_bindings = std::collections::BTreeMap::new();
    for path in &paths {
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let single = observe_path_async_with_accounting(
            &store,
            &generation,
            path,
            config(),
            &mut accounting,
            &cancellation,
            None,
            false,
        )
        .await?
        .0;
        for dependency in single.dependencies {
            single_bindings.insert(dependency.region, dependency.expected);
        }
    }
    let mut batched_bindings = std::collections::BTreeMap::new();
    for dependency in bindings.dependencies {
        let prior = batched_bindings.insert(dependency.region, dependency.expected);
        assert!(prior.is_none_or(|prior| prior == dependency.expected));
    }
    assert_eq!(batched_bindings, single_bindings);
    let names_only = expected
        .into_iter()
        .filter(|(region, _)| !matches!(region, DependencyRegion::FileRecord(_)))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(batched_bindings, names_only);
    Ok(())
}

#[tokio::test]
async fn hardlinked_directory_aliases_share_one_directory_frontier()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store).await?;
    let alias = NamespacePath::new(
        vec![
            LogicalName::new(NameEncoding::Utf8, b"alias".to_vec(), 255)?,
            LogicalName::new(NameEncoding::Utf8, b"main.rs".to_vec(), 255)?,
        ],
        config().limits,
    )?;
    let batch = lookup_paths(
        &store,
        &generation,
        &[main, alias],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(batch.entries[0], batch.entries[1]);
    assert_eq!(batch.work.page_reads, 5);
    assert_eq!(batch.work.backend_read_operations, 3);
    Ok(())
}

#[tokio::test]
async fn batch_lookup_does_not_reserve_worst_case_page_frontiers()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, _) = fixture(&store).await?;
    let missing = LogicalName::new(NameEncoding::Utf8, b"missing".to_vec(), 255)?;
    let path = NamespacePath::new(vec![missing; 10], config().limits)?;
    let paths = vec![&path; 1_224];
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = 16 * 1024 * 1024;
    let result = lookup_path_refs(
        &store,
        &generation,
        &paths,
        config(),
        budget,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(result.entries.len(), paths.len());
    assert!(result.entries.iter().all(|entry| entry.record.is_none()));
    assert!(result.work.peak_allocation_bytes <= budget.peak_allocation_bytes);
    Ok(())
}

#[tokio::test]
async fn mixed_depth_batches_finish_prefixes_and_reject_non_directory_descent()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store).await?;
    let src_name = LogicalName::new(NameEncoding::Utf8, b"src".to_vec(), 255)?;
    let src = NamespacePath::new(vec![src_name.clone()], config().limits)?;
    let root = NamespacePath::new(Vec::new(), config().limits)?;
    let mixed = lookup_paths(
        &store,
        &generation,
        &[main.clone(), src, root],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        mixed.entries[0].record.map(|record| record.kind),
        Some(FileKind::Regular)
    );
    assert_eq!(
        mixed.entries[1].record.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    assert_eq!(
        mixed.entries[2].record.map(|record| record.kind),
        Some(FileKind::Directory)
    );

    let mut invalid_components = main.components().to_vec();
    invalid_components.push(LogicalName::new(
        NameEncoding::Utf8,
        b"child".to_vec(),
        255,
    )?);
    let invalid = NamespacePath::new(invalid_components, config().limits)?;
    let failure = lookup_paths(
        &store,
        &generation,
        &[invalid],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("non-directory descent succeeded")?;
    assert!(matches!(failure.error, PathLookupError::NotDirectory));
    assert!(failure.work.backend_read_operations > 0);
    Ok(())
}
