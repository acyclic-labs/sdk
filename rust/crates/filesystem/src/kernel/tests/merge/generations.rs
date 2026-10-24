use super::*;

#[tokio::test]
async fn generation_merge_covers_union_directory_and_field_resolution_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let base_first = record(1, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let base_second = record(2, 2, 1, FilePayload::Empty, FileKind::Fifo);
    let ours_first = record(1, 3, 1, FilePayload::Empty, FileKind::Fifo);
    let theirs_second = record(2, 4, 1, FilePayload::Empty, FileKind::Fifo);
    let base_table = put_table(&store, vec![base_first, base_second]).await?;
    let ours_table = put_table(&store, vec![ours_first, base_second]).await?;
    let theirs_table = put_table(&store, vec![base_first, theirs_second]).await?;
    let generation = |table| GenerationRoot {
        file_table: table,
        ..root(1, 9, 1)
    };
    let union_limit = (merge_generation_async(
        &store,
        MergeGenerationRequest {
            maximum_changes: 1,
            ..request(
                generation(base_table),
                generation(ours_table),
                generation(theirs_table),
            )
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("union-limited generation merge succeeded")?;
    assert!(matches!(
        union_limit.error,
        MergeGenerationError::ChangeLimit
    ));

    let base_third = record(3, 3, 1, FilePayload::Empty, FileKind::Fifo);
    let ours_second = record(2, 5, 1, FilePayload::Empty, FileKind::Fifo);
    let theirs_second_overlap = record(2, 6, 1, FilePayload::Empty, FileKind::Fifo);
    let theirs_third = record(3, 7, 1, FilePayload::Empty, FileKind::Fifo);
    let three_way_union_limit = (merge_generation_async(
        &store,
        MergeGenerationRequest {
            maximum_changes: 2,
            ..request(
                generation(put_table(&store, vec![base_first, base_second, base_third]).await?),
                generation(put_table(&store, vec![ours_first, ours_second, base_third]).await?),
                generation(
                    put_table(
                        &store,
                        vec![base_first, theirs_second_overlap, theirs_third],
                    )
                    .await?,
                ),
            )
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("three-file union-limited generation merge succeeded")?;
    assert!(matches!(
        three_way_union_limit.error,
        MergeGenerationError::ChangeLimit
    ));

    let shared_change_table = put_table(&store, vec![ours_first, base_second]).await?;
    let unchanged = (merge_generation_async(
        &store,
        stored_request(
            &store,
            generation(base_table),
            generation(shared_change_table),
            generation(shared_change_table),
        )
        .await?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(matches!(
        unchanged.value,
        MergeGenerationOutcome::Prepared { root, .. } if root.file_table == shared_change_table
    ));

    let ours_field = record(1, 5, 1, FilePayload::Empty, FileKind::Fifo);
    let theirs_field = record(1, 1, 2, FilePayload::Empty, FileKind::Fifo);
    let base_field_table = put_table(&store, vec![base_first]).await?;
    let ours_field_table = put_table(&store, vec![ours_field]).await?;
    let theirs_field_table = put_table(&store, vec![theirs_field]).await?;
    let combined = (merge_generation_async(
        &store,
        stored_request(
            &store,
            generation(base_field_table),
            generation(ours_field_table),
            generation(theirs_field_table),
        )
        .await?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(matches!(
        combined.value,
        MergeGenerationOutcome::Prepared { .. }
    ));

    let mut linear_request = stored_request(
        &store,
        generation(base_field_table),
        generation(ours_field_table),
        generation(theirs_field_table),
    )
    .await?;
    linear_request.retain_theirs_parent = false;
    let linear = (merge_generation_async(
        &store,
        linear_request,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(matches!(
        linear.value,
        MergeGenerationOutcome::Prepared { root, .. } if root.parents.len() == 1
    ));

    let empty = put_tree(&store, Vec::new()).await?;
    let ours_tree = put_tree(
        &store,
        vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"ours".to_vec(), 16)?,
            file_id: FileId::from_bytes([7; 16]),
            kind: FileKind::Fifo,
        }],
    )
    .await?;
    let theirs_tree = put_tree(
        &store,
        vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"theirs".to_vec(), 16)?,
            file_id: FileId::from_bytes([8; 16]),
            kind: FileKind::Fifo,
        }],
    )
    .await?;
    let base_directory = directory(3, 1, 1, 1);
    let directory_with = |entries| FileRecord {
        payload: FilePayload::Directory { entries },
        ..base_directory
    };
    let directory_limit = (merge_generation_async(
        &store,
        MergeGenerationRequest {
            maximum_changes: 1,
            ..request(
                generation(put_table(&store, vec![directory_with(empty)]).await?),
                generation(put_table(&store, vec![directory_with(ours_tree)]).await?),
                generation(put_table(&store, vec![directory_with(theirs_tree)]).await?),
            )
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("directory-limited generation merge succeeded")?;
    assert!(matches!(
        directory_limit.error,
        MergeGenerationError::ChangeLimit
    ));

    let shared_name = LogicalName::new(NameEncoding::Utf8, b"shared".to_vec(), 16)?;
    let tree_for = async |file: u8| {
        put_tree(
            &store,
            vec![TreeEntry {
                name: shared_name.clone(),
                file_id: FileId::from_bytes([file; 16]),
                kind: FileKind::Fifo,
            }],
        )
        .await
    };
    let conflicted = (merge_generation_async(
        &store,
        stored_request(
            &store,
            generation(put_table(&store, vec![directory_with(tree_for(10).await?)]).await?),
            generation(put_table(&store, vec![directory_with(tree_for(11).await?)]).await?),
            generation(put_table(&store, vec![directory_with(tree_for(12).await?)]).await?),
        )
        .await?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(matches!(
        conflicted.value,
        MergeGenerationOutcome::Conflicted { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn directory_merge_and_link_recount_fail_at_each_distinct_change_frontier()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let directory_id = FileId::from_bytes([41; 16]);
    let entry = |name: &'static [u8], file: u8| -> Result<TreeEntry, Box<dyn std::error::Error>> {
        Ok(TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, name.to_vec(), 16)?,
            file_id: FileId::from_bytes([file; 16]),
            kind: FileKind::Fifo,
        })
    };
    let empty = put_tree(&store, Vec::new()).await?;
    let a = entry(b"a", 42)?;
    let b = entry(b"b", 43)?;
    let both = put_tree(&store, vec![a.clone(), b.clone()]).await?;
    let only_a = put_tree(&store, vec![a]).await?;
    let only_b = put_tree(&store, vec![b]).await?;
    let base = FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata: object(ObjectKind::Metadata, 1),
        payload: FilePayload::Directory { entries: empty },
    };
    let with = |entries| FileRecord {
        payload: FilePayload::Directory { entries },
        ..base
    };

    let truncated_side = (merge_directory_record_async(
        &store,
        directory_id,
        base,
        with(both),
        base,
        1,
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("side-truncated directory merge succeeded")?;
    assert!(matches!(
        truncated_side.error,
        MergeGenerationError::ChangeLimit
    ));

    let union_limit = (merge_directory_record_async(
        &store,
        directory_id,
        base,
        with(only_a),
        with(only_b),
        1,
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("union-limited directory merge succeeded")?;
    assert!(matches!(
        union_limit.error,
        MergeGenerationError::ChangeLimit
    ));

    let mut resolutions = BTreeMap::from([(directory_id, (Some(base), Some(with(both))))]);
    let mut remaining_changes = 1;
    let failure = (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut remaining_changes,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut WorkCounters::default(),
    ))
    .await
    .err()
    .ok_or("truncated link recount succeeded")?;
    assert!(matches!(failure.error, MergeGenerationError::ChangeLimit));
    Ok(())
}

#[test]
fn metadata_timestamps_reconcile_and_authored_fields_conflict() {
    let base = timed(10, 0o644);
    // Both sides touched the record: the later time wins, the mode is unchanged.
    let merged = merge_metadata_fields(base, timed(30, 0o644), timed(20, 0o644))
        .expect("divergent timestamps reconcile");
    assert_eq!(merged.modified_ns, MetadataField::Value(30));
    assert_eq!(merged.posix_mode, MetadataField::Value(0o644));
    // One side changed the mode, the other only its time: both land.
    let merged = merge_metadata_fields(base, timed(10, 0o600), timed(50, 0o644))
        .expect("one-sided mode change reconciles");
    assert_eq!(merged.posix_mode, MetadataField::Value(0o600));
    assert_eq!(merged.modified_ns, MetadataField::Value(50));
    // A time only one side can represent is kept.
    let mut unavailable = timed(10, 0o644);
    unavailable.modified_ns = MetadataField::Unavailable;
    let merged = merge_metadata_fields(base, unavailable, timed(40, 0o644))
        .expect("unavailable versus value reconciles");
    assert_eq!(merged.modified_ns, MetadataField::Value(40));
    // Authored state changed differently on each side conflicts.
    assert!(merge_metadata_fields(base, timed(10, 0o600), timed(10, 0o700)).is_none());
    let mut ours = base;
    ours.posix_uid = MetadataField::Value(1);
    let mut theirs = base;
    theirs.posix_uid = MetadataField::Value(2);
    assert!(merge_metadata_fields(base, ours, theirs).is_none());
}

#[test]
fn one_file_added_on_both_sides_keeps_the_later_times() {
    let merged = merge_added_metadata_fields(timed(30, 0o644), timed(20, 0o644))
        .expect("the same file added twice reconciles");
    assert_eq!(merged.modified_ns, MetadataField::Value(30));
    assert_eq!(merged.posix_mode, MetadataField::Value(0o644));
    // Without a base, authored state must agree.
    assert!(merge_added_metadata_fields(timed(30, 0o600), timed(20, 0o644)).is_none());
}

#[tokio::test]
async fn sibling_additions_merge_entries_and_reconcile_directory_times()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let directory_id = FileId::from_bytes([51; 16]);
    let entry = |name: &'static [u8], file: u8| -> Result<TreeEntry, Box<dyn std::error::Error>> {
        Ok(TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, name.to_vec(), 16)?,
            file_id: FileId::from_bytes([file; 16]),
            kind: FileKind::Fifo,
        })
    };
    let a = entry(b"a", 52)?;
    let b = entry(b"b", 53)?;
    let empty = put_tree(&store, Vec::new()).await?;
    let only_a = put_tree(&store, vec![a.clone()]).await?;
    let only_b = put_tree(&store, vec![b.clone()]).await?;
    let both = put_tree(&store, vec![a, b]).await?;
    let dir = |metadata, entries| FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata,
        payload: FilePayload::Directory { entries },
    };
    let base = dir(put_metadata(&store, timed(10, 0o755)).await?, empty);
    let ours = dir(put_metadata(&store, timed(20, 0o755)).await?, only_a);
    let theirs = dir(put_metadata(&store, timed(30, 0o755)).await?, only_b);

    let merged = (merge_directory_record_async(
        &store,
        directory_id,
        base,
        ours,
        theirs,
        8,
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(merged.value.conflicts.is_empty());
    let record = merged
        .value
        .record
        .ok_or("sibling directory merge produced no record")?;
    assert_eq!(record.payload, FilePayload::Directory { entries: both });
    assert_eq!(record.metadata, file_metadata_id(timed(30, 0o755))?);

    // A genuine metadata conflict (mode changed both ways) resolved by a driver
    // still merges the entries; the resolution contributes metadata only.
    let ours = dir(put_metadata(&store, timed(20, 0o700)).await?, only_a);
    let theirs = dir(put_metadata(&store, timed(30, 0o770)).await?, only_b);
    let unresolved = (merge_directory_record_async(
        &store,
        directory_id,
        base,
        ours,
        theirs,
        8,
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(
        unresolved.value.conflicts,
        vec![MergeConflict::File(directory_id)]
    );
    let chosen = put_metadata(&store, timed(30, 0o770)).await?;
    let resolutions = BTreeMap::from([(
        MergeConflict::File(directory_id),
        MergeConflictResolution::File(Some(dir(chosen, only_b))),
    )]);
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut state = MergeState::new(8, &mut accounting)?;
    let record = (merge_directory_record_with_resolutions_async(
        &store,
        directory_id,
        base,
        ours,
        theirs,
        ResolutionView::Map(&resolutions),
        8,
        DecodeLimits::default(),
        &mut state,
        &mut accounting,
        &cancellation,
    ))
    .await?
    .ok_or("resolved directory merge produced no record")?;
    state.records.retire(&mut accounting)?;
    state.conflicts.retire(&mut accounting)?;
    state.folds.retire(&mut accounting)?;
    state.file_folds.retire(&mut accounting)?;
    assert_eq!(record.metadata, chosen);
    assert_eq!(record.payload, FilePayload::Directory { entries: both });
    Ok(())
}

mod laws {
    use super::{FileMetadata, MetadataField, merge_metadata_fields, resolve_three};
    use crate::foundation::Digest;
    use crate::storage::{ObjectId, ObjectKind};
    use proptest::prelude::*;

    // Tiny domains so sides often agree with the base or each other.
    fn field_of<T: Clone + std::fmt::Debug>(
        value: impl Strategy<Value = T>,
    ) -> impl Strategy<Value = MetadataField<T>> {
        prop::option::of(value)
            .prop_map(|value| value.map_or(MetadataField::Unavailable, MetadataField::Value))
    }

    fn object_of(kind: ObjectKind) -> impl Strategy<Value = MetadataField<ObjectId>> {
        field_of((0_u8..2).prop_map(move |byte| ObjectId {
            kind,
            digest: Digest::from_bytes([byte; 32]),
        }))
    }

    fn metadata() -> impl Strategy<Value = FileMetadata> {
        let small = || field_of(0_u32..2);
        let times = prop::array::uniform4(field_of(0_i64..3));
        let objects = (
            object_of(ObjectKind::AttributePage),
            object_of(ObjectKind::Blob),
            object_of(ObjectKind::Blob),
        );
        (
            (small(), small(), small(), field_of(0_u64..2), small()),
            times,
            objects,
        )
            .prop_map(
                |(
                    (posix_mode, posix_uid, posix_gid, posix_flags, windows_attributes),
                    [created_ns, modified_ns, accessed_ns, changed_ns],
                    (named_attributes, acl, security_descriptor),
                )| FileMetadata {
                    posix_mode,
                    posix_uid,
                    posix_gid,
                    posix_flags,
                    windows_attributes,
                    created_ns,
                    modified_ns,
                    accessed_ns,
                    changed_ns,
                    named_attributes,
                    acl,
                    security_descriptor,
                },
            )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn resolve_three_keeps_one_sided_and_identical_changes(
            base in 0_u8..3,
            ours in 0_u8..3,
            theirs in 0_u8..3,
        ) {
            prop_assert_eq!(resolve_three(&base, &base, &theirs), Some(theirs));
            prop_assert_eq!(resolve_three(&base, &ours, &base), Some(ours));
            prop_assert_eq!(resolve_three(&base, &ours, &ours), Some(ours));
            prop_assert_eq!(resolve_three(&base, &ours, &theirs), resolve_three(&base, &theirs, &ours));
        }

        #[test]
        fn metadata_merge_keeps_one_sided_and_identical_changes(
            base in metadata(),
            ours in metadata(),
            theirs in metadata(),
        ) {
            prop_assert_eq!(merge_metadata_fields(base, base, theirs), Some(theirs));
            prop_assert_eq!(merge_metadata_fields(base, ours, base), Some(ours));
            prop_assert_eq!(merge_metadata_fields(base, ours, ours), Some(ours));
            prop_assert_eq!(
                merge_metadata_fields(base, ours, theirs),
                merge_metadata_fields(base, theirs, ours)
            );
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_merge_preserves_concurrent_bindings_hardlinks_and_parent_lineage()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata = put_metadata(&store, FileMetadata::default()).await?;
    let a = FileId::from_bytes([2; 16]);
    let b = FileId::from_bytes([3; 16]);
    let entry = |name: &str, file_id| -> Result<TreeEntry, Box<dyn std::error::Error>> {
        Ok(TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, name.as_bytes().to_vec(), 16)?,
            file_id,
            kind: FileKind::Fifo,
        })
    };
    let base_entries = put_tree(&store, vec![entry("alpha", a)?, entry("beta", b)?]).await?;
    let ours_entries = put_tree(
        &store,
        vec![entry("alpha", a)?, entry("beta", b)?, entry("left", a)?],
    )
    .await?;
    let theirs_entries = put_tree(
        &store,
        vec![entry("alpha", a)?, entry("beta", b)?, entry("right", b)?],
    )
    .await?;
    let directory = |entries| FileRecord {
        metadata,
        payload: FilePayload::Directory { entries },
        ..directory(1, 0, 1, 0)
    };
    let first = FileRecord {
        metadata,
        ..record(2, 0, 1, FilePayload::Empty, FileKind::Fifo)
    };
    let second = FileRecord {
        metadata,
        ..record(3, 0, 1, FilePayload::Empty, FileKind::Fifo)
    };
    let base = GenerationRoot {
        file_table: put_table(&store, vec![directory(base_entries), first, second]).await?,
        ..root(1, 1, 0)
    };
    let ours = GenerationRoot {
        file_table: put_table(
            &store,
            vec![
                directory(ours_entries),
                FileRecord {
                    link_count: 2,
                    ..first
                },
                second,
            ],
        )
        .await?,
        ..root(1, 1, 0)
    };
    let ours_table = ours.file_table;
    let theirs = GenerationRoot {
        file_table: put_table(
            &store,
            vec![
                directory(theirs_entries),
                first,
                FileRecord {
                    link_count: 2,
                    ..second
                },
            ],
        )
        .await?,
        ..root(1, 1, 0)
    };
    let request = stored_request(&store, base, ours, theirs).await?;
    let second_parent = GenerationId::new(request.theirs_generation.digest);
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let (outcome, allocation) = (merge_generation_with_accounting(
        &store,
        request,
        ContentMerge::Extents,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    ))
    .await?;
    let MergeGenerationOutcome::Prepared {
        generation_root,
        root,
        generation_id,
    } = outcome
    else {
        return Err("independent native additions conflicted".into());
    };
    assert_eq!(generation_id, GenerationId::new(generation_root.digest));
    assert_eq!(root.parents.len(), 2);
    assert_eq!(root.parents.get(1), Some(&second_parent));
    let KernelOwnedAllocation::NativeOriginal(guard) = allocation else {
        return Err("native merge lost output custody".into());
    };
    assert_eq!(
        guard.bytes(),
        (root.parents.capacity() * std::mem::size_of::<GenerationId>()) as u64
    );
    let changes = (crate::kernel::diff_file_records_with_accounting(
        &store,
        Some(ours_table),
        Some(root.file_table),
        8,
        DecodeLimits::default(),
        &mut crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED,
            &cancellation,
        ),
        &cancellation,
    ))
    .await?
    .0;
    let merged_second = changes
        .changes
        .iter()
        .find(|change| change.key == b)
        .and_then(|change| change.after)
        .ok_or("second hardlink delta missing")?;
    assert_eq!(merged_second.link_count, 2);
    let merged_directory = changes
        .changes
        .iter()
        .find(|change| change.key == FileId::from_bytes([1; 16]))
        .and_then(|change| change.after)
        .ok_or("merged directory missing")?;
    let entries =
        directory_entries(Some(merged_directory)).ok_or("merged directory payload missing")?;
    let read = {
        let __object_args = (
            entries,
            DecodeLimits::default().maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::read(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?;
    let TreePage::Leaf(entries) =
        crate::kernel::decode_tree_page(&read.value.bytes, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("small merged directory was not a leaf".into());
    };
    assert_eq!(entries.len(), 4);
    assert!(
        entries
            .iter()
            .any(|entry| entry.name.as_bytes() == b"left" && entry.file_id == a)
    );
    assert!(
        entries
            .iter()
            .any(|entry| entry.name.as_bytes() == b"right" && entry.file_id == b)
    );
    let stored = {
        let __object_args = (
            generation_root,
            DecodeLimits::default().maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::read(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?;
    assert_eq!(
        crate::kernel::decode_generation_root(
            &stored.value.bytes,
            DecodeLimits::default(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(())
        )?,
        root
    );
    assert!(original.receipt().page_reads > 0);
    assert!(original.receipt().page_writes > 0);
    drop(root);
    drop(guard);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_merge_original_allocation_denial_precedes_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = GenerationRoot {
        file_table: put_table(
            &store,
            vec![record(1, 1, 1, FilePayload::Empty, FileKind::Fifo)],
        )
        .await?,
        ..root(1, 1, 0)
    };
    let ours = GenerationRoot {
        file_table: put_table(
            &store,
            vec![record(1, 2, 1, FilePayload::Empty, FileKind::Fifo)],
        )
        .await?,
        ..root(1, 1, 0)
    };
    let request = request(base.clone(), ours, base);
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let baseline = original
        .allocations(|ledger, _, _, _| Ok::<_, FsError>(ledger.live_bytes()))
        .map(|(value, _allocation)| value)?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let result = (merge_generation_with_accounting(
        &store,
        request,
        ContentMerge::Extents,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    ))
    .await;
    assert!(matches!(
        result,
        Err(FsError::NativeKernel(
            crate::NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "allocation_operations",
                ..
            })
        ))
    ));
    assert_eq!(original.receipt(), before);
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.receipt().backend_write_operations, 0);
    assert_eq!(
        original
            .allocations(|ledger, _, _, _| Ok::<_, FsError>(ledger.live_bytes()))
            .map(|(value, _allocation)| value)?,
        baseline
    );
    Ok(())
}
