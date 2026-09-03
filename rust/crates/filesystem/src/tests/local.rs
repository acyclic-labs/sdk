use super::*;
use crate::storage::{ObjectKind, object_digest};
use tempfile::tempdir;

fn chunk(bytes: &[u8]) -> ObjectId {
    ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, bytes),
    }
}

#[test]
fn budget_subtraction_preserves_every_independent_counter() -> Result<(), Box<dyn std::error::Error>>
{
    let budget = WorkBudget {
        authority_records_read: 101,
        authority_records_appended: 102,
        authority_bytes_read: 103,
        authority_bytes_written: 104,
        object_probes: 105,
        backend_read_operations: 106,
        backend_write_operations: 107,
        durability_operations: 108,
        page_reads: 109,
        page_writes: 110,
        object_bytes_read: 111,
        object_bytes_written: 112,
        bytes_hashed: 113,
        bytes_copied: 114,
        bytes_encoded: 115,
        source_bytes_read: 116,
        output_bytes: 117,
        items_examined: 118,
        items_returned: 119,
        allocation_operations: 120,
        peak_allocation_bytes: 121,
        materializations: 122,
    };
    let spent = WorkCounters {
        authority_records_read: 1,
        authority_records_appended: 2,
        authority_bytes_read: 3,
        authority_bytes_written: 4,
        object_probes: 5,
        backend_read_operations: 6,
        backend_write_operations: 7,
        durability_operations: 8,
        page_reads: 9,
        page_writes: 10,
        object_bytes_read: 11,
        object_bytes_written: 12,
        bytes_hashed: 13,
        bytes_copied: 14,
        bytes_encoded: 15,
        source_bytes_read: 16,
        output_bytes: 17,
        items_examined: 18,
        items_returned: 19,
        allocation_operations: 20,
        peak_allocation_bytes: 21,
        materializations: 22,
    };
    assert_eq!(
        subtract_budget(budget, spent)?,
        WorkBudget {
            authority_records_read: 100,
            authority_records_appended: 100,
            authority_bytes_read: 100,
            authority_bytes_written: 100,
            object_probes: 100,
            backend_read_operations: 100,
            backend_write_operations: 100,
            durability_operations: 100,
            page_reads: 100,
            page_writes: 100,
            object_bytes_read: 100,
            object_bytes_written: 100,
            bytes_hashed: 100,
            bytes_copied: 100,
            bytes_encoded: 100,
            source_bytes_read: 100,
            output_bytes: 100,
            items_examined: 100,
            items_returned: 100,
            allocation_operations: 100,
            peak_allocation_bytes: 121,
            materializations: 100,
        }
    );
    Ok(())
}

#[test]
fn object_name_and_size_boundaries_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let work = WorkCounters::default();
    assert!(parse_object_id(ObjectKind::BlobChunk, "0", &"0".repeat(62), work).is_err());
    assert!(parse_object_id(ObjectKind::BlobChunk, "00", &"0".repeat(61), work).is_err());
    assert!(parse_object_id(ObjectKind::BlobChunk, "00", &"0".repeat(62), work).is_ok());

    let directory = tempdir()?;
    let store = LocalObjectStore::open(directory.path(), 6)?;
    assert!(
        store
            .put(
                chunk(b"abcdef"),
                Bytes::from_static(b"abcdef"),
                WorkBudget::UNBOUNDED
            )
            .is_ok()
    );
    assert!(matches!(
        store.put(
            chunk(b"abcdefg"),
            Bytes::from_static(b"abcdefg"),
            WorkBudget::UNBOUNDED
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge {
                observed: 7,
                maximum: 6
            },
            ..
        })
    ));
    Ok(())
}

#[test]
fn quarantine_distinguishes_missing_from_other_rename_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = LocalObjectStore::open(directory.path(), 1024)?;
    let object_id = chunk(b"quarantine");
    assert!(
        store
            .quarantine_corrupt(&directory.path().join("absent"), object_id)
            .is_ok()
    );
    assert!(matches!(
        store.quarantine_corrupt(&store.root, object_id),
        Err(ObjectStoreError::QuarantineFailed(_))
    ));
    Ok(())
}

#[test]
fn io_error_classification_and_read_admission_are_exact() -> Result<(), Box<dyn std::error::Error>>
{
    assert!(has_io_kind(
        &std::io::Error::from(std::io::ErrorKind::NotFound),
        std::io::ErrorKind::NotFound
    ));
    assert!(!has_io_kind(
        &std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        std::io::ErrorKind::NotFound
    ));
    assert!(has_io_kind(
        &std::io::Error::from(std::io::ErrorKind::AlreadyExists),
        std::io::ErrorKind::AlreadyExists
    ));
    assert_eq!(classify_entry_creation(Ok(()))?, EntryCreation::Created);
    assert_eq!(
        classify_entry_creation(Err(std::io::ErrorKind::AlreadyExists.into()))?,
        EntryCreation::Existing
    );
    let denied = classify_entry_creation(Err(std::io::ErrorKind::PermissionDenied.into()));
    assert!(matches!(
        denied,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied
    ));

    let directory = tempdir()?;
    let store = LocalObjectStore::open(directory.path(), 1024)?;
    let object_id = chunk(b"abcdef");
    store.put(
        object_id,
        Bytes::from_static(b"abcdef"),
        WorkBudget::UNBOUNDED,
    )?;
    for budget in [
        WorkBudget {
            object_bytes_read: 5,
            ..WorkBudget::UNBOUNDED
        },
        WorkBudget {
            bytes_hashed: 6 + OBJECT_DIGEST_ENVELOPE_BYTES - 1,
            ..WorkBudget::UNBOUNDED
        },
        WorkBudget {
            allocation_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
    ] {
        assert!(matches!(
            store.read(object_id, 6, budget),
            Err(ObjectFailure {
                error: ObjectStoreError::Work(_),
                ..
            })
        ));
    }
    Ok(())
}

#[test]
fn object_survives_reopen_and_read_is_exact() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let object_id = chunk(b"abcdef");
    let put = LocalObjectStore::open(directory.path(), 1024)?.put(
        object_id,
        Bytes::from_static(b"abcdef"),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        put.work,
        WorkCounters {
            object_probes: 1,
            backend_write_operations: 1,
            object_bytes_written: 6,
            durability_operations: 2,
            bytes_hashed: 6 + OBJECT_DIGEST_ENVELOPE_BYTES,
            ..WorkCounters::default()
        }
    );
    let reopened = LocalObjectStore::open(directory.path(), 1024)?;
    let read = reopened.read(object_id, 6, WorkBudget::UNBOUNDED)?;
    assert_eq!(read.value.bytes, Bytes::from_static(b"abcdef"));
    assert_eq!(
        read.value.retention,
        ObjectReadRetention::Owned { logical_bytes: 6 }
    );
    assert_eq!(
        read.work,
        WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            object_bytes_read: 6,
            bytes_hashed: 6 + OBJECT_DIGEST_ENVELOPE_BYTES,
            allocation_operations: 1,
            peak_allocation_bytes: 6,
            ..WorkCounters::default()
        }
    );

    let duplicate = reopened.put(
        object_id,
        Bytes::from_static(b"abcdef"),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        duplicate.work,
        WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            object_bytes_read: 6,
            bytes_hashed: 2 * (6 + OBJECT_DIGEST_ENVELOPE_BYTES),
            allocation_operations: 1,
            peak_allocation_bytes: 6,
            ..WorkCounters::default()
        }
    );
    Ok(())
}

#[test]
fn owned_read_is_budgeted_before_allocating_or_reading_body()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = LocalObjectStore::open(directory.path(), 1024)?;
    let object_id = chunk(b"abcdef");
    store.put(
        object_id,
        Bytes::from_static(b"abcdef"),
        WorkBudget::UNBOUNDED,
    )?;
    let failure = store
        .read(
            object_id,
            6,
            WorkBudget {
                peak_allocation_bytes: 5,
                ..WorkBudget::UNBOUNDED
            },
        )
        .err()
        .ok_or("underbudgeted owned read unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ObjectStoreError::Work(crate::performance::WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed: 6,
            maximum: 5,
        })
    ));
    assert_eq!(failure.work.object_bytes_read, 0);
    assert_eq!(failure.work.allocation_operations, 0);
    assert_eq!(failure.work.peak_allocation_bytes, 0);
    Ok(())
}

#[test]
fn corruption_is_quarantined_before_a_clean_retry_repairs_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = LocalObjectStore::open(directory.path(), 1024)?;
    let object_id = chunk(b"stable");
    store.put(
        object_id,
        Bytes::from_static(b"stable"),
        WorkBudget::UNBOUNDED,
    )?;
    fs::write(store.object_path(object_id), b"broken")?;
    assert!(matches!(
        store.read(object_id, 1024, WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    assert_eq!(fs::read_dir(&store.quarantine)?.count(), 1);
    store.put(
        object_id,
        Bytes::from_static(b"stable"),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        store
            .read(object_id, 1024, WorkBudget::UNBOUNDED)?
            .value
            .bytes,
        Bytes::from_static(b"stable")
    );
    Ok(())
}

#[test]
fn garbage_collection_requires_exclusive_maintenance_and_preserves_reachable_objects()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = LocalObjectStore::open(directory.path(), 1024)?;
    let reachable = chunk(b"reachable");
    let orphan = chunk(b"orphan");
    for (object_id, bytes) in [
        (reachable, Bytes::from_static(b"reachable")),
        (orphan, Bytes::from_static(b"orphan")),
    ] {
        store.put(object_id, bytes, WorkBudget::UNBOUNDED)?;
    }
    assert!(LocalObjectStore::open_for_maintenance(directory.path(), 1024).is_err());
    assert!(
        store
            .collect_garbage(&[reachable], 2, WorkBudget::UNBOUNDED)
            .is_err()
    );
    drop(store);

    let store = LocalObjectStore::open_for_maintenance(directory.path(), 1024)?;
    let first = store.collect_garbage(&[reachable], 2, WorkBudget::UNBOUNDED)?;
    assert_eq!(
        first.value,
        LocalGarbageCollection {
            examined: 2,
            removed: 1,
            temporary_files_removed: 0,
        }
    );
    assert_eq!(
        first.work,
        WorkCounters {
            object_probes: 2,
            backend_read_operations: 9,
            backend_write_operations: 1,
            durability_operations: 1,
            items_examined: 7,
            ..WorkCounters::default()
        }
    );
    assert!(!store.contains(orphan, WorkBudget::UNBOUNDED)?.value);
    assert!(store.contains(reachable, WorkBudget::UNBOUNDED)?.value);
    Ok(())
}

#[test]
fn maintenance_gc_bounds_and_removes_crash_left_temporaries()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = LocalObjectStore::open(directory.path(), 1024)?;
    let retained = chunk(b"retained");
    store.put(
        retained,
        Bytes::from_static(b"retained"),
        WorkBudget::UNBOUNDED,
    )?;
    let object_parent = store
        .object_path(retained)
        .parent()
        .ok_or("object path has no parent")?
        .to_path_buf();
    drop(store);

    let temporary = object_parent.join(".00000000-0000-0000-0000-000000000000.tmp");
    fs::write(&temporary, b"torn publication")?;
    let store = LocalObjectStore::open_for_maintenance(directory.path(), 1024)?;
    let collected = store.collect_garbage(&[retained], 2, WorkBudget::UNBOUNDED)?;
    assert_eq!(collected.value.examined, 2);
    assert_eq!(collected.value.removed, 0);
    assert_eq!(collected.value.temporary_files_removed, 1);
    assert!(!temporary.exists());
    assert!(store.contains(retained, WorkBudget::UNBOUNDED)?.value);
    Ok(())
}
