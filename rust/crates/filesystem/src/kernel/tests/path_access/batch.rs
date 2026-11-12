#[tokio::test]
async fn batch_rejects_authenticated_child_bounds_and_preserves_failure_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store).await?;
    let root_id = FileId::from_bytes([1; 16]);
    let src_id = FileId::from_bytes([2; 16]);
    let metadata = put_file_metadata(&store, metadata()).await?;
    let leaf = put_tree_page(
        &store,
        &TreePage::Leaf(vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"src".to_vec())?,
            file_id: src_id,
            kind: FileKind::Directory,
        }]),
        8,
    )
    .await?;
    let malformed_root_tree = put_tree_page(
        &store,
        &TreePage::Internal(vec![Child {
            first: LogicalName::new(NameEncoding::Utf8, b"zzz".to_vec())?,
            page: leaf,
        }]),
        8,
    )
    .await?;
    let file_table = put_file_table_page(
        &store,
        &FileTablePage::Leaf(vec![FileRecord {
            bindings: None,
            file_id: root_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata,
            payload: FilePayload::Directory {
                entries: malformed_root_tree,
            },
        }]),
        8,
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
        vec![LogicalName::new(NameEncoding::Utf8, b"src".to_vec())?],
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
            LogicalName::new(NameEncoding::Utf8, b"src".to_vec())?,
            LogicalName::new(NameEncoding::Utf8, b"missing".to_vec())?,
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
    let config = || {
        let mut config = config();
        config.limits.maximum_paths_per_batch = 2;
        config
    };
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store).await?;
    let name = |bytes: &[u8]| LogicalName::new(NameEncoding::Utf8, bytes.to_vec());
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
            LogicalName::new(NameEncoding::Utf8, b"alias".to_vec())?,
            LogicalName::new(NameEncoding::Utf8, b"main.rs".to_vec())?,
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
    let missing = LogicalName::new(NameEncoding::Utf8, b"missing".to_vec())?;
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
    // The same complete result must fit with a smaller paid traversal chunk,
    // including the final partial chunk. The result itself stays resident.
    for width in [1, 7] {
        let mut chunked = config();
        chunked.limits.maximum_paths_per_batch = width;
        let borrowed = lookup_path_refs(
            &store,
            &generation,
            &paths,
            chunked,
            budget,
            &CancellationToken::new(),
        )
        .await?;
        assert_eq!(borrowed.entries, result.entries);
        assert!(borrowed.work.peak_allocation_bytes < result.work.peak_allocation_bytes);
        let mut exact = budget;
        exact.peak_allocation_bytes = borrowed.work.peak_allocation_bytes;
        let owned = vec![path.clone(); paths.len()];
        let bounded = lookup_paths(
            &store,
            &generation,
            &owned,
            chunked,
            exact,
            &CancellationToken::new(),
        )
        .await?;
        assert_eq!(bounded.entries, result.entries);
        assert_eq!(
            bounded.work.peak_allocation_bytes,
            exact.peak_allocation_bytes
        );
    }
    Ok(())
}

#[tokio::test]
async fn mixed_depth_batches_finish_prefixes_and_reject_non_directory_descent()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store).await?;
    let src_name = LogicalName::new(NameEncoding::Utf8, b"src".to_vec())?;
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
    invalid_components.push(LogicalName::new(NameEncoding::Utf8, b"child".to_vec())?);
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

#[tokio::test]
async fn finished_batch_paths_add_only_linear_work_beside_a_deep_lookup()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_test_support::scale::{Bound, assert_scales};

    let mut measured = Vec::new();
    for size in [16_u64, 128] {
        let store = MemoryObjectStore::default();
        let metadata = put_file_metadata(&store, metadata()).await?;
        let name = LogicalName::new(NameEncoding::Utf8, b"child".to_vec())?;
        let id = |index: u64| FileId::from_bytes(u128::from(index + 1).to_be_bytes());
        let mut records = Vec::new();
        for index in 0..=size {
            let entries = if index == size {
                Vec::new()
            } else {
                vec![TreeEntry {
                    name: name.clone(),
                    file_id: id(index + 1),
                    kind: FileKind::Directory,
                }]
            };
            let entries = put_tree_page(&store, &TreePage::Leaf(entries), 8).await?;
            records.push(FileRecord {
                bindings: None,
                file_id: id(index),
                kind: FileKind::Directory,
                link_count: 1,
                metadata,
                payload: FilePayload::Directory { entries },
            });
        }
        let file_table = put_file_table_page(&store, &FileTablePage::Leaf(records), 256).await?;
        let generation = GenerationRoot {
            volume_id: VolumeId::from_bytes([17; 16]),
            root_file_id: id(0),
            file_table,
            parents: Vec::new(),
            required_features: 0,
        };
        let deep = NamespacePath::new(vec![name; usize::try_from(size)?], config().limits)?;
        let cancellation = CancellationToken::new();
        let single = lookup_paths(
            &store,
            &generation,
            std::slice::from_ref(&deep),
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let root = NamespacePath::new(Vec::new(), config().limits)?;
        let mut paths = vec![root; usize::try_from(size)?];
        paths.insert(paths.len() / 2, deep);
        let batch = lookup_paths(
            &store,
            &generation,
            &paths,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(batch.entries[paths.len() / 2], single.entries[0]);
        for (path, entry) in paths.iter().zip(&batch.entries) {
            if path.is_root() {
                assert_eq!(entry.record.map(|record| record.file_id), Some(id(0)));
                assert_eq!(entry.resolved_components, 0);
            }
        }
        assert_eq!(batch.work.page_reads, single.work.page_reads);
        assert_eq!(
            batch.work.backend_read_operations,
            single.work.backend_read_operations
        );
        let additional = batch
            .work
            .items_examined
            .checked_sub(single.work.items_examined)
            .ok_or("batch work omitted the deep lookup")?;
        assert!(
            additional > 0,
            "original item counter must observe root queries"
        );
        assert!(
            additional <= 8 * size + 16,
            "finished paths were revisited: {additional}"
        );
        measured.push((size, additional));
    }
    assert_scales(Bound::Linear, measured[0], measured[1]);
    Ok(())
}
