use acyclic_fs::kernel::{
    Child, DecodeLimits, FileKind, FilePayload, FileRecord, FileRecordReadError, FileTablePage,
    lookup_file_record, lookup_file_records,
};
use acyclic_fs::{CancellationToken, MemoryObjectStore};
use acyclic_fs_format::{
    identity::{Digest, FileId},
    object_reference::{ObjectId, ObjectKind},
};
use acyclic_fs_work::performance::{WorkBudget, WorkCounters};
use std::task::{Context, Poll, Waker};

use super::super::file_table_objects::put_file_table_page as put_memory_page;

fn fifo_record(byte: u8) -> FileRecord {
    FileRecord {
        bindings: None,
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: ObjectId {
            storage: None,
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([byte; 32]),
        },
        payload: FilePayload::Empty,
    }
}

async fn two_level_file_table(
    store: &MemoryObjectStore,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let first = put_memory_page(store, &FileTablePage::Leaf(vec![fifo_record(1)]), 8).await?;
    let second = put_memory_page(store, &FileTablePage::Leaf(vec![fifo_record(2)]), 8).await?;
    put_memory_page(
        store,
        &FileTablePage::Internal(vec![
            Child {
                first: FileId::from_bytes([1; 16]),
                page: first,
            },
            Child {
                first: FileId::from_bytes([2; 16]),
                page: second,
            },
        ]),
        8,
    )
    .await
}

#[tokio::test]
async fn lookup_reads_only_one_file_table_frontier() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = two_level_file_table(&store).await?;
    let found = lookup_file_record(
        &store,
        root,
        FileId::from_bytes([2; 16]),
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(found.value, Some(fifo_record(2)));
    assert_eq!(found.work.page_reads, 2);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut future = std::pin::pin!(lookup_file_record(
        &store,
        root,
        FileId::from_bytes([2; 16]),
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancelled,
    ));
    let mut context = Context::from_waker(Waker::noop());
    let Poll::Ready(result) = future.as_mut().poll(&mut context) else {
        return Err("pre-cancelled asynchronous file lookup remained pending".into());
    };
    let failure = result
        .err()
        .ok_or("pre-cancelled asynchronous file lookup unexpectedly succeeded")?;
    assert!(matches!(failure.error, FileRecordReadError::Cancelled));
    assert_eq!(*failure.work().unwrap(), WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn file_record_batch_shares_frontiers_and_preserves_order()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = two_level_file_table(&store).await?;
    let batch = lookup_file_records(
        &store,
        root,
        &[
            FileId::from_bytes([2; 16]),
            FileId::from_bytes([1; 16]),
            FileId::from_bytes([2; 16]),
            FileId::from_bytes([3; 16]),
        ],
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        batch.value,
        vec![
            Some(fifo_record(2)),
            Some(fifo_record(1)),
            Some(fifo_record(2)),
            None,
        ]
    );
    assert_eq!(batch.work.page_reads, 3);
    assert_eq!(batch.work.backend_read_operations, 2);
    Ok(())
}

#[tokio::test]
async fn file_lookup_limits_kinds_and_authenticated_bounds_fail_exactly()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = two_level_file_table(&store).await?;
    let wrong = lookup_file_record(
        &store,
        ObjectId {
            storage: None,
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([55; 32]),
        },
        FileId::from_bytes([1; 16]),
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("wrong file-table root kind unexpectedly succeeded")?;
    assert!(matches!(wrong.error, FileRecordReadError::WrongRootKind));
    assert_eq!(wrong.work().unwrap().backend_read_operations, 0);

    let invalid_limits = DecodeLimits {
        maximum_page_height: 0,
        ..DecodeLimits::default()
    };
    let invalid = lookup_file_record(
        &store,
        root,
        FileId::from_bytes([1; 16]),
        invalid_limits,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("invalid file lookup limits unexpectedly succeeded")?;
    assert!(matches!(
        invalid.error,
        FileRecordReadError::InvalidHeightLimit
    ));
    assert_eq!(invalid.work().unwrap().backend_read_operations, 0);
    let height_one = DecodeLimits {
        maximum_page_height: 1,
        ..DecodeLimits::default()
    };
    let height = lookup_file_record(
        &store,
        root,
        FileId::from_bytes([1; 16]),
        height_one,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("file lookup height limit unexpectedly succeeded")?;
    assert!(matches!(height.error, FileRecordReadError::HeightExceeded));
    assert_eq!(height.work().unwrap().page_reads, 1);

    let empty = lookup_file_records(
        &store,
        root,
        &[],
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("empty file lookup batch unexpectedly succeeded")?;
    assert!(matches!(empty.error, FileRecordReadError::EmptyBatch));
    assert_eq!(empty.work().unwrap().backend_read_operations, 0);

    let leaf = put_memory_page(&store, &FileTablePage::Leaf(vec![fifo_record(1)]), 8).await?;
    let forged = put_memory_page(
        &store,
        &FileTablePage::Internal(vec![Child {
            first: FileId::from_bytes([0; 16]),
            page: leaf,
        }]),
        8,
    )
    .await?;
    let failure = lookup_file_record(
        &store,
        forged,
        FileId::from_bytes([1; 16]),
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("forged file-table bound unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        FileRecordReadError::ChildBoundsMismatch
    ));
    assert_eq!(failure.work.as_deref().unwrap().page_reads, 2);
    Ok(())
}
