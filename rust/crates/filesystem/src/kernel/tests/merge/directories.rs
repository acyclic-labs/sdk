use super::*;

#[tokio::test]
async fn merge_request_guards_fail_before_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let valid = root(1, 2, 3);
    let cancellation = CancellationToken::new();

    let mut zero_changes = request(valid.clone(), valid.clone(), valid.clone());
    zero_changes.maximum_changes = 0;
    let failure = merge_error(zero_changes, &cancellation).await?;
    assert!(matches!(failure.error, MergeGenerationError::ChangeLimit));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut zero_conflicts = request(valid.clone(), valid.clone(), valid.clone());
    zero_conflicts.maximum_conflicts = 0;
    assert!(matches!(
        merge_error(zero_conflicts, &cancellation).await?.error,
        MergeGenerationError::ChangeLimit
    ));

    assert!(matches!(
        merge_error(
            request(valid.clone(), root(9, 2, 3), valid.clone()),
            &cancellation
        )
        .await?
        .error,
        MergeGenerationError::InvalidDiff
    ));
    assert!(matches!(
        merge_error(
            request(valid.clone(), valid.clone(), root(1, 9, 3)),
            &cancellation
        )
        .await?
        .error,
        MergeGenerationError::InvalidDiff
    ));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = merge_error(request(valid.clone(), valid.clone(), valid), &cancelled).await?;
    assert!(matches!(failure.error, MergeGenerationError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn directory_scalar_conflicts_are_exact_and_bounded() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let directory_id = FileId::from_bytes([7; 16]);

    // Authored metadata (the mode) changed differently on each side: a real conflict.
    let with_metadata = |metadata| FileRecord {
        metadata,
        ..directory(7, 1, 1, 8)
    };
    let metadata_conflict = (merge_directory_record_async(
        &store,
        directory_id,
        with_metadata(put_metadata(&store, timed(1, 0o755)).await?),
        with_metadata(put_metadata(&store, timed(1, 0o700)).await?),
        with_metadata(put_metadata(&store, timed(1, 0o770)).await?),
        1,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(metadata_conflict.value.record.is_none());
    assert_eq!(
        metadata_conflict.value.conflicts,
        vec![MergeConflict::File(directory_id)]
    );
    assert!(!metadata_conflict.value.truncated);
    assert_eq!(metadata_conflict.value.examined_changes, 0);

    let bounded_link_conflict = (merge_directory_record_async(
        &store,
        directory_id,
        directory(7, 1, 1, 8),
        directory(7, 1, 2, 8),
        directory(7, 1, 3, 8),
        1,
        0,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(bounded_link_conflict.value.record.is_none());
    assert!(bounded_link_conflict.value.conflicts.is_empty());
    assert!(bounded_link_conflict.value.truncated);

    let non_directory = record(7, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let result = (merge_directory_record_async(
        &store,
        directory_id,
        non_directory,
        non_directory,
        non_directory,
        1,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await;
    let failure = result
        .err()
        .ok_or_else(|| std::io::Error::other("directory merge accepted a non-directory record"))?;
    assert!(matches!(failure.error, MergeGenerationError::InvalidDiff));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn tree_mutation_preserves_exact_names_and_file_identity() -> Result<(), Box<dyn std::error::Error>>
{
    let name = LogicalName::new(NameEncoding::Utf8, b"entry".to_vec(), 16)?;
    let before = TreeEntry {
        name: name.clone(),
        file_id: FileId::from_bytes([1; 16]),
        kind: FileKind::Regular,
    };
    let after = TreeEntry {
        name: name.clone(),
        file_id: FileId::from_bytes([2; 16]),
        kind: FileKind::Directory,
    };
    assert!(matches!(
        tree_mutation(name.clone(), None, Some(after.clone())),
        Some(TreeMutation::Insert(value)) if value == after
    ));
    assert!(matches!(
        tree_mutation(name.clone(), Some(before.clone()), None),
        Some(TreeMutation::Remove { name: value, expected_file_id: Some(file_id) })
            if value == name && file_id == before.file_id
    ));
    assert!(matches!(
        tree_mutation(name.clone(), Some(before.clone()), Some(after.clone())),
        Some(TreeMutation::Replace { entry, expected_file_id })
            if entry == after && expected_file_id == before.file_id
    ));
    assert!(tree_mutation(name, None, None).is_none());
    Ok(())
}

#[tokio::test]
async fn link_count_adjustment_is_bounded_exact_and_rename_neutral()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let target_id = FileId::from_bytes([31; 16]);
    let directory_id = FileId::from_bytes([32; 16]);
    let target = record(31, 1, 2, FilePayload::Empty, FileKind::Fifo);
    let a = LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 8)?;
    let b = LogicalName::new(NameEncoding::Utf8, b"b".to_vec(), 8)?;
    let entry = |name: LogicalName| TreeEntry {
        name,
        file_id: target_id,
        kind: FileKind::Fifo,
    };
    let base_tree = put_tree(&store, vec![entry(a.clone())]).await?;
    let empty_tree = put_tree(&store, Vec::new()).await?;
    let renamed_tree = put_tree(&store, vec![entry(b)]).await?;
    let base_directory = FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata: object(ObjectKind::Metadata, 1),
        payload: FilePayload::Directory { entries: base_tree },
    };
    let removed_directory = FileRecord {
        payload: FilePayload::Directory {
            entries: empty_tree,
        },
        ..base_directory
    };
    let mut resolutions = BTreeMap::from([
        (
            directory_id,
            (Some(base_directory), Some(removed_directory)),
        ),
        (target_id, (Some(target), Some(target))),
    ]);
    let mut remaining_changes = 8;
    let mut work = WorkCounters::default();
    (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut remaining_changes,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut work,
    ))
    .await?;
    assert_eq!(
        resolutions
            .get(&target_id)
            .and_then(|(_, record)| *record)
            .ok_or("adjusted target record missing")?
            .link_count,
        1
    );

    // A file introduced on one side may lose its directory binding when the
    // other side wins a same-name replacement. It must not remain orphaned in
    // the merged file table merely because no binding changed relative to ours.
    let introduced = record(33, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let introduced_id = introduced.file_id;
    let mut resolutions = BTreeMap::from([(introduced_id, (None, Some(introduced)))]);
    (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut 8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut WorkCounters::default(),
    ))
    .await?;
    assert_eq!(
        resolutions.get(&introduced_id).map(|(_, record)| *record),
        Some(None)
    );

    let renamed_directory = FileRecord {
        payload: FilePayload::Directory {
            entries: renamed_tree,
        },
        ..base_directory
    };
    let mut resolutions = BTreeMap::from([(
        directory_id,
        (Some(base_directory), Some(renamed_directory)),
    )]);
    let mut remaining_changes = 8;
    (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut remaining_changes,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut WorkCounters::default(),
    ))
    .await?;
    assert!(!resolutions.contains_key(&target_id));

    let mut resolutions = BTreeMap::from([(
        directory_id,
        (Some(base_directory), Some(removed_directory)),
    )]);
    let limit = (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut 0,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut WorkCounters::default(),
    ))
    .await
    .err()
    .ok_or("zero link frontier unexpectedly succeeded")?;
    assert!(matches!(limit.error, MergeGenerationError::ChangeLimit));
    Ok(())
}

#[tokio::test]
async fn pruning_an_unbound_directory_recomputes_descendant_links()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let directory_id = FileId::from_bytes([90; 16]);
    let child = record(91, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let child_id = child.file_id;
    let entries = put_tree(
        &store,
        vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 8)?,
            file_id: child_id,
            kind: FileKind::Fifo,
        }],
    )
    .await?;
    let directory = FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata: object(ObjectKind::Metadata, 1),
        payload: FilePayload::Directory { entries },
    };
    let mut resolutions = BTreeMap::from([
        (directory_id, (None, Some(directory))),
        (child_id, (None, Some(child))),
    ]);
    (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut 8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        &mut WorkCounters::default(),
    ))
    .await?;
    assert!(resolutions.values().all(|(_, resolved)| resolved.is_none()));
    Ok(())
}

#[tokio::test]
async fn deleting_a_record_with_remaining_links_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let linked = record(92, 1, 2, FilePayload::Empty, FileKind::Fifo);
    let mut resolutions = BTreeMap::from([(linked.file_id, (Some(linked), None))]);
    let failure = (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut 8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        &mut WorkCounters::default(),
    ))
    .await
    .err()
    .ok_or("remaining-link deletion unexpectedly succeeded")?;
    assert!(matches!(failure.error, MergeGenerationError::InvalidDiff));
    Ok(())
}
