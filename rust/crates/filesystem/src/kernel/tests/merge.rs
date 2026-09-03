use super::*;
use crate::async_storage::poll_ready;
use crate::foundation::{Digest, VolumeId};
use crate::kernel::persistent_diff::ValueChange;
use crate::kernel::{FileKind, NameEncoding, TreeEntry, TreePage, encode_tree_page, tree_page_id};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectKind, ObjectStore};
use bytes::Bytes;

fn object(kind: ObjectKind, byte: u8) -> ObjectId {
    ObjectId {
        kind,
        digest: Digest::from_bytes([byte; 32]),
    }
}

fn root(volume: u8, root_file: u8, table: u8) -> GenerationRoot {
    GenerationRoot {
        volume_id: VolumeId::from_bytes([volume; 16]),
        root_file_id: FileId::from_bytes([root_file; 16]),
        file_table: object(ObjectKind::FileTablePage, table),
        parents: Vec::new(),
        required_features: 0,
    }
}

fn request(
    base: GenerationRoot,
    ours: GenerationRoot,
    theirs: GenerationRoot,
) -> MergeGenerationRequest {
    MergeGenerationRequest {
        base_generation: object(ObjectKind::GenerationRoot, 10),
        base,
        ours_generation: None,
        ours,
        theirs_generation: object(ObjectKind::GenerationRoot, 11),
        theirs,
        retain_theirs_parent: true,
        maximum_changes: 8,
        maximum_conflicts: 8,
    }
}

fn record(
    file: u8,
    metadata: u8,
    link_count: u64,
    payload: FilePayload,
    kind: FileKind,
) -> FileRecord {
    FileRecord {
        file_id: FileId::from_bytes([file; 16]),
        kind,
        link_count,
        metadata: object(ObjectKind::Metadata, metadata),
        payload,
    }
}

fn directory(file: u8, metadata: u8, link_count: u64, entries: u8) -> FileRecord {
    record(
        file,
        metadata,
        link_count,
        FilePayload::Directory {
            entries: object(ObjectKind::TreePage, entries),
        },
        FileKind::Directory,
    )
}

fn put_tree(
    store: &MemoryObjectStore,
    entries: Vec<TreeEntry>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let page = TreePage::Leaf(entries);
    let object = tree_page_id(&page, 8)?;
    ObjectStore::put(
        store,
        object,
        Bytes::from(encode_tree_page(&page, 8)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(object)
}

fn put_table(
    store: &MemoryObjectStore,
    records: Vec<FileRecord>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let page = super::super::FileTablePage::Leaf(records);
    let object = super::super::file_table_page_id(&page, 8)?;
    ObjectStore::put(
        store,
        object,
        Bytes::from(super::super::encode_file_table_page(&page, 8)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(object)
}

fn put_generation(
    store: &MemoryObjectStore,
    generation: &GenerationRoot,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let encoded = super::super::encode_generation_root(generation)?;
    let object = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: crate::storage::object_digest(ObjectKind::GenerationRoot, &encoded),
    };
    ObjectStore::put(store, object, Bytes::from(encoded), WorkBudget::UNBOUNDED)?;
    Ok(object)
}

fn stored_request(
    store: &MemoryObjectStore,
    base: GenerationRoot,
    ours: GenerationRoot,
    theirs: GenerationRoot,
) -> Result<MergeGenerationRequest, Box<dyn std::error::Error>> {
    Ok(MergeGenerationRequest {
        base_generation: put_generation(store, &base)?,
        base,
        ours_generation: None,
        ours,
        theirs_generation: put_generation(store, &theirs)?,
        theirs,
        retain_theirs_parent: true,
        maximum_changes: 8,
        maximum_conflicts: 8,
    })
}

fn merge_error(
    request: MergeGenerationRequest,
    cancellation: &CancellationToken,
) -> Result<OperationFailure<MergeGenerationError>, std::io::Error> {
    let result = poll_ready(merge_generation_async(
        &MemoryObjectStore::default(),
        request,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        cancellation,
    ))
    .ok_or_else(|| std::io::Error::other("request guard did not complete synchronously"))?;
    result
        .err()
        .ok_or_else(|| std::io::Error::other("request guard admitted an invalid request"))
}

#[test]
fn merge_request_guards_fail_before_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let valid = root(1, 2, 3);
    let cancellation = CancellationToken::new();

    let mut zero_changes = request(valid.clone(), valid.clone(), valid.clone());
    zero_changes.maximum_changes = 0;
    let failure = merge_error(zero_changes, &cancellation)?;
    assert!(matches!(failure.error, MergeGenerationError::ChangeLimit));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut zero_conflicts = request(valid.clone(), valid.clone(), valid.clone());
    zero_conflicts.maximum_conflicts = 0;
    assert!(matches!(
        merge_error(zero_conflicts, &cancellation)?.error,
        MergeGenerationError::ChangeLimit
    ));

    assert!(matches!(
        merge_error(
            request(valid.clone(), root(9, 2, 3), valid.clone()),
            &cancellation
        )?
        .error,
        MergeGenerationError::InvalidDiff
    ));
    assert!(matches!(
        merge_error(
            request(valid.clone(), valid.clone(), root(1, 9, 3)),
            &cancellation
        )?
        .error,
        MergeGenerationError::InvalidDiff
    ));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = merge_error(request(valid.clone(), valid.clone(), valid), &cancelled)?;
    assert!(matches!(failure.error, MergeGenerationError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn directory_scalar_conflicts_are_exact_and_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let directory_id = FileId::from_bytes([7; 16]);

    let metadata_conflict = poll_ready(merge_directory_record_async(
        &store,
        directory_id,
        directory(7, 1, 1, 8),
        directory(7, 2, 1, 8),
        directory(7, 3, 1, 8),
        1,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or_else(|| std::io::Error::other("scalar conflict did not complete synchronously"))??;
    assert!(metadata_conflict.value.record.is_none());
    assert_eq!(
        metadata_conflict.value.conflicts,
        vec![MergeConflict::File(directory_id)]
    );
    assert!(!metadata_conflict.value.truncated);
    assert_eq!(metadata_conflict.value.examined_changes, 0);
    assert_eq!(metadata_conflict.work, WorkCounters::default());

    let bounded_link_conflict = poll_ready(merge_directory_record_async(
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
    .ok_or_else(|| std::io::Error::other("bounded conflict did not complete synchronously"))??;
    assert!(bounded_link_conflict.value.record.is_none());
    assert!(bounded_link_conflict.value.conflicts.is_empty());
    assert!(bounded_link_conflict.value.truncated);

    let non_directory = record(7, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let result = poll_ready(merge_directory_record_async(
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
    .ok_or_else(|| std::io::Error::other("record validation did not complete synchronously"))?;
    let failure = result
        .err()
        .ok_or_else(|| std::io::Error::other("directory merge accepted a non-directory record"))?;
    assert!(matches!(failure.error, MergeGenerationError::InvalidDiff));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn optional_and_change_index_resolution_is_total() {
    let base = record(1, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let ours = record(1, 2, 1, FilePayload::Empty, FileKind::Fifo);
    let theirs = record(1, 3, 1, FilePayload::Empty, FileKind::Fifo);

    assert!(matches!(
        resolve_optional(Some(base), Some(ours), Some(ours)),
        OptionalResolution::Resolved(Some(value)) if value == ours
    ));
    assert!(matches!(
        resolve_optional(Some(base), Some(ours), Some(base)),
        OptionalResolution::Resolved(Some(value)) if value == ours
    ));
    assert!(matches!(
        resolve_optional(Some(base), Some(base), Some(theirs)),
        OptionalResolution::Resolved(Some(value)) if value == theirs
    ));
    assert!(matches!(
        resolve_optional(Some(base), Some(ours), Some(theirs)),
        OptionalResolution::Conflict
    ));
    assert!(matches!(
        resolve_optional(None, None, None),
        OptionalResolution::Resolved(None)
    ));

    let second_id = FileId::from_bytes([2; 16]);
    let indexed = changes_by_file(vec![
        ValueChange {
            key: base.file_id,
            before: Some(base),
            after: Some(ours),
        },
        ValueChange {
            key: second_id,
            before: None,
            after: Some(theirs),
        },
    ]);
    assert_eq!(indexed.get(&base.file_id), Some(&(Some(base), Some(ours))));
    assert_eq!(indexed.get(&second_id), Some(&(None, Some(theirs))));

    assert!(is_directory(Some(directory(4, 1, 1, 5))));
    assert!(!is_directory(Some(base)));
    assert_eq!(
        directory_entries(Some(directory(4, 1, 1, 5))),
        Some(object(ObjectKind::TreePage, 5))
    );
    assert_eq!(directory_entries(Some(base)), None);
    assert_eq!(directory_entries(None), None);
}

#[test]
fn diff_failure_translation_preserves_exact_receipts() {
    let prior = WorkCounters {
        object_probes: 2,
        ..WorkCounters::default()
    };
    let nested = WorkCounters {
        page_reads: 3,
        ..WorkCounters::default()
    };

    let cases = [
        PersistentDiffError::WrongRootKind,
        PersistentDiffError::InvalidLimit,
        PersistentDiffError::AllocationFailed,
        PersistentDiffError::Storage(ObjectStoreError::Missing),
        PersistentDiffError::Decode(CanonicalDecodeError::Truncated),
        PersistentDiffError::Work(WorkError::Overflow),
        PersistentDiffError::Cancelled,
    ];
    for (ordinal, error) in cases.into_iter().enumerate() {
        let mapped = map_diff_failure(OperationFailure::new(error, nested), prior);
        assert_eq!(mapped.work.object_probes, 2);
        assert_eq!(mapped.work.page_reads, 3);
        match ordinal {
            0 | 1 => assert!(matches!(mapped.error, MergeGenerationError::InvalidDiff)),
            2 => assert!(matches!(
                mapped.error,
                MergeGenerationError::AllocationFailed
            )),
            3 => assert!(matches!(
                mapped.error,
                MergeGenerationError::Object(ObjectStoreError::Missing)
            )),
            4 => assert!(matches!(
                mapped.error,
                MergeGenerationError::Decode(CanonicalDecodeError::Truncated)
            )),
            5 => assert!(matches!(
                mapped.error,
                MergeGenerationError::Work(WorkError::Overflow)
            )),
            6 => assert!(matches!(mapped.error, MergeGenerationError::Cancelled(_))),
            _ => unreachable!(),
        }
    }

    let overflow = map_diff_failure(
        OperationFailure::new(
            PersistentDiffError::Cancelled,
            WorkCounters {
                object_probes: 1,
                ..WorkCounters::default()
            },
        ),
        WorkCounters {
            object_probes: u64::MAX,
            ..WorkCounters::default()
        },
    );
    assert!(matches!(
        overflow.error,
        MergeGenerationError::Work(WorkError::Overflow)
    ));
    assert_eq!(overflow.work.object_probes, u64::MAX);
}

#[test]
fn work_helpers_preserve_exact_receipts() -> Result<(), Box<dyn std::error::Error>> {
    let prior = WorkCounters {
        object_probes: 2,
        ..WorkCounters::default()
    };
    let nested = WorkCounters {
        page_reads: 3,
        ..WorkCounters::default()
    };

    assert_eq!(
        remaining(prior, WorkBudget::UNBOUNDED)?.object_probes,
        u64::MAX - 2
    );
    let exhausted = remaining(prior, WorkCounters::default())
        .err()
        .ok_or_else(|| std::io::Error::other("spent work was admitted by an empty budget"))?;
    assert!(matches!(
        exhausted.error,
        MergeGenerationError::Work(WorkError::BudgetExceeded {
            counter: "object_probes",
            observed: 2,
            maximum: 0
        })
    ));
    assert_eq!(*exhausted.work, prior);

    assert_eq!(
        add(prior, nested)?,
        WorkCounters {
            object_probes: 2,
            page_reads: 3,
            ..WorkCounters::default()
        }
    );
    let add_overflow = add(
        WorkCounters {
            bytes_hashed: u64::MAX,
            ..WorkCounters::default()
        },
        WorkCounters {
            bytes_hashed: 1,
            ..WorkCounters::default()
        },
    )
    .err()
    .ok_or_else(|| std::io::Error::other("exact accounting saturated an overflow"))?;
    assert!(matches!(
        add_overflow.error,
        MergeGenerationError::Work(WorkError::Overflow)
    ));

    let invalid = invalid(prior);
    assert!(matches!(invalid.error, MergeGenerationError::InvalidDiff));
    assert_eq!(*invalid.work, prior);
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

#[test]
#[allow(clippy::too_many_lines)]
fn link_count_adjustment_is_bounded_exact_and_rename_neutral()
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
    let base_tree = put_tree(&store, vec![entry(a.clone())])?;
    let empty_tree = put_tree(&store, Vec::new())?;
    let renamed_tree = put_tree(&store, vec![entry(b)])?;
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
    poll_ready(adjust_link_counts(
        &store,
        &mut resolutions,
        &mut remaining_changes,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut work,
    ))
    .ok_or("link-count adjustment unexpectedly suspended")??;
    assert_eq!(
        resolutions
            .get(&target_id)
            .and_then(|(_, record)| *record)
            .ok_or("adjusted target record missing")?
            .link_count,
        1
    );

    let renamed_directory = FileRecord {
        payload: FilePayload::Directory {
            entries: renamed_tree,
        },
        ..base_directory
    };
    let mut resolutions = BTreeMap::from([
        (
            directory_id,
            (Some(base_directory), Some(renamed_directory)),
        ),
        (target_id, (Some(target), Some(target))),
    ]);
    let mut remaining_changes = 8;
    poll_ready(adjust_link_counts(
        &store,
        &mut resolutions,
        &mut remaining_changes,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut WorkCounters::default(),
    ))
    .ok_or("rename-neutral link adjustment unexpectedly suspended")??;
    assert_eq!(
        resolutions
            .get(&target_id)
            .and_then(|(_, record)| *record)
            .ok_or("rename-neutral target record missing")?
            .link_count,
        2
    );

    let mut resolutions = BTreeMap::from([(
        directory_id,
        (Some(base_directory), Some(removed_directory)),
    )]);
    let limit = poll_ready(adjust_link_counts(
        &store,
        &mut resolutions,
        &mut 0,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut WorkCounters::default(),
    ))
    .ok_or("zero link frontier unexpectedly suspended")?
    .err()
    .ok_or("zero link frontier unexpectedly succeeded")?;
    assert!(matches!(limit.error, MergeGenerationError::ChangeLimit));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn generation_merge_covers_union_directory_and_field_resolution_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let base_first = record(1, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let base_second = record(2, 2, 1, FilePayload::Empty, FileKind::Fifo);
    let ours_first = record(1, 3, 1, FilePayload::Empty, FileKind::Fifo);
    let theirs_second = record(2, 4, 1, FilePayload::Empty, FileKind::Fifo);
    let base_table = put_table(&store, vec![base_first, base_second])?;
    let ours_table = put_table(&store, vec![ours_first, base_second])?;
    let theirs_table = put_table(&store, vec![base_first, theirs_second])?;
    let generation = |table| GenerationRoot {
        file_table: table,
        ..root(1, 9, 1)
    };
    let union_limit = poll_ready(merge_generation_async(
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
    .ok_or("union-limited generation merge suspended")?
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
    let three_way_union_limit = poll_ready(merge_generation_async(
        &store,
        MergeGenerationRequest {
            maximum_changes: 2,
            ..request(
                generation(put_table(
                    &store,
                    vec![base_first, base_second, base_third],
                )?),
                generation(put_table(
                    &store,
                    vec![ours_first, ours_second, base_third],
                )?),
                generation(put_table(
                    &store,
                    vec![base_first, theirs_second_overlap, theirs_third],
                )?),
            )
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("three-file union-limited generation merge suspended")?
    .err()
    .ok_or("three-file union-limited generation merge succeeded")?;
    assert!(matches!(
        three_way_union_limit.error,
        MergeGenerationError::ChangeLimit
    ));

    let shared_change_table = put_table(&store, vec![ours_first, base_second])?;
    let unchanged = poll_ready(merge_generation_async(
        &store,
        stored_request(
            &store,
            generation(base_table),
            generation(shared_change_table),
            generation(shared_change_table),
        )?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("shared-change generation merge suspended")??;
    assert!(matches!(
        unchanged.value,
        MergeGenerationOutcome::Prepared { root, .. } if root.file_table == shared_change_table
    ));

    let ours_field = record(1, 5, 1, FilePayload::Empty, FileKind::Fifo);
    let theirs_field = record(1, 1, 2, FilePayload::Empty, FileKind::Fifo);
    let base_field_table = put_table(&store, vec![base_first])?;
    let ours_field_table = put_table(&store, vec![ours_field])?;
    let theirs_field_table = put_table(&store, vec![theirs_field])?;
    let combined = poll_ready(merge_generation_async(
        &store,
        stored_request(
            &store,
            generation(base_field_table),
            generation(ours_field_table),
            generation(theirs_field_table),
        )?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("field-combining generation merge suspended")??;
    assert!(matches!(
        combined.value,
        MergeGenerationOutcome::Prepared { .. }
    ));

    let mut linear_request = stored_request(
        &store,
        generation(base_field_table),
        generation(ours_field_table),
        generation(theirs_field_table),
    )?;
    linear_request.retain_theirs_parent = false;
    let linear = poll_ready(merge_generation_async(
        &store,
        linear_request,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("linear-history generation merge suspended")??;
    assert!(matches!(
        linear.value,
        MergeGenerationOutcome::Prepared { root, .. } if root.parents.len() == 1
    ));

    let empty = put_tree(&store, Vec::new())?;
    let ours_tree = put_tree(
        &store,
        vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"ours".to_vec(), 16)?,
            file_id: FileId::from_bytes([7; 16]),
            kind: FileKind::Fifo,
        }],
    )?;
    let theirs_tree = put_tree(
        &store,
        vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"theirs".to_vec(), 16)?,
            file_id: FileId::from_bytes([8; 16]),
            kind: FileKind::Fifo,
        }],
    )?;
    let base_directory = directory(3, 1, 1, 1);
    let directory_with = |entries| FileRecord {
        payload: FilePayload::Directory { entries },
        ..base_directory
    };
    let directory_limit = poll_ready(merge_generation_async(
        &store,
        MergeGenerationRequest {
            maximum_changes: 1,
            ..request(
                generation(put_table(&store, vec![directory_with(empty)])?),
                generation(put_table(&store, vec![directory_with(ours_tree)])?),
                generation(put_table(&store, vec![directory_with(theirs_tree)])?),
            )
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("directory-limited generation merge suspended")?
    .err()
    .ok_or("directory-limited generation merge succeeded")?;
    assert!(matches!(
        directory_limit.error,
        MergeGenerationError::ChangeLimit
    ));

    let shared_name = LogicalName::new(NameEncoding::Utf8, b"shared".to_vec(), 16)?;
    let tree_for = |file: u8| {
        put_tree(
            &store,
            vec![TreeEntry {
                name: shared_name.clone(),
                file_id: FileId::from_bytes([file; 16]),
                kind: FileKind::Fifo,
            }],
        )
    };
    let conflicted = poll_ready(merge_generation_async(
        &store,
        stored_request(
            &store,
            generation(put_table(&store, vec![directory_with(tree_for(10)?)])?),
            generation(put_table(&store, vec![directory_with(tree_for(11)?)])?),
            generation(put_table(&store, vec![directory_with(tree_for(12)?)])?),
        )?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("binding-conflicted generation merge suspended")??;
    assert!(matches!(
        conflicted.value,
        MergeGenerationOutcome::Conflicted { .. }
    ));
    Ok(())
}

#[test]
fn directory_merge_and_link_recount_fail_at_each_distinct_change_frontier()
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
    let empty = put_tree(&store, Vec::new())?;
    let a = entry(b"a", 42)?;
    let b = entry(b"b", 43)?;
    let both = put_tree(&store, vec![a.clone(), b.clone()])?;
    let only_a = put_tree(&store, vec![a])?;
    let only_b = put_tree(&store, vec![b])?;
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

    let truncated_side = poll_ready(merge_directory_record_async(
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
    .ok_or("side-truncated directory merge suspended")?
    .err()
    .ok_or("side-truncated directory merge succeeded")?;
    assert!(matches!(
        truncated_side.error,
        MergeGenerationError::ChangeLimit
    ));

    let union_limit = poll_ready(merge_directory_record_async(
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
    .ok_or("union-limited directory merge suspended")?
    .err()
    .ok_or("union-limited directory merge succeeded")?;
    assert!(matches!(
        union_limit.error,
        MergeGenerationError::ChangeLimit
    ));

    let mut resolutions = BTreeMap::from([(directory_id, (Some(base), Some(with(both))))]);
    let mut remaining_changes = 1;
    let failure = poll_ready(adjust_link_counts(
        &store,
        &mut resolutions,
        &mut remaining_changes,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut WorkCounters::default(),
    ))
    .ok_or("truncated link recount suspended")?
    .err()
    .ok_or("truncated link recount succeeded")?;
    assert!(matches!(failure.error, MergeGenerationError::ChangeLimit));
    Ok(())
}
