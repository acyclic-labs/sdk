use crate::async_storage::AsyncObjectStore;
use super::*;
use crate::foundation::{Digest, FileId};
use crate::kernel::{FileKind, NameEncoding, TreePage, encode_tree_page, tree_page_id};
use crate::memory::MemoryObjectStore;
use crate::performance::WorkCounters;
use crate::storage::ObjectKind;
use acyclic_stream::OwnedBytes;
use std::task::{Context, Poll, Waker};

use crate::kernel::Child;
fn name(value: &str) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(
        NameEncoding::Utf8,
        value.as_bytes().to_vec(),
        255,
    )?)
}

fn unlimited() -> WorkBudget {
    WorkBudget::UNBOUNDED
}

async fn put_page(
    store: &MemoryObjectStore,
    page: &TreePage,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = tree_page_id(page, 16)?;
    AsyncObjectStore::put(
        store,
        id,
        OwnedBytes::from(encode_tree_page(page, 16, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?),
        WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
    Ok(id)
}

#[tokio::test]
async fn lookup_reads_only_one_frontier() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let leaf = TreePage::Leaf(vec![TreeEntry {
        name: name("target")?,
        file_id: FileId::from_bytes([4; 16]),
        kind: FileKind::Regular,
    }]);
    let leaf_id = put_page(&store, &leaf).await?;
    let root = TreePage::Internal(vec![Child {
        first: name("target")?,
        page: leaf_id,
    }]);
    let root_id = put_page(&store, &root).await?;

    let result = lookup_tree_entry(
        &store,
        root_id,
        &name("target")?,
        DecodeLimits::default(),
        unlimited(),
        &CancellationToken::new(),
    ).await?;
    assert_eq!(
        result.value.map(|entry| entry.file_id),
        Some(FileId::from_bytes([4; 16]))
    );
    assert_eq!(result.work.page_reads, 2);
    Ok(())
}

#[tokio::test]
async fn budget_stops_before_an_unadmitted_child_read() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let leaf_id = put_page(&store, &TreePage::Leaf(Vec::new())).await?;
    let root_id = put_page(
        &store,
        &TreePage::Internal(vec![Child {
            first: name("a")?,
            page: leaf_id,
        }]),
    ).await?;
    let mut budget = unlimited();
    budget.page_reads = 1;
    assert!(matches!(
        lookup_tree_entry(
            &store,
            root_id,
            &name("a")?,
            DecodeLimits::default(),
            budget,
            &CancellationToken::new(),
        ).await,
        Err(OperationFailure {
            error: TreeReadError::Work(WorkError::BudgetExceeded {
                counter: "page_reads",
                ..
            }),
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn wrong_root_kind_fails_without_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    assert!(matches!(
        lookup_tree_entry(
            &store,
            ObjectId {
                kind: ObjectKind::Blob,
                digest: Digest::ZERO,
            },
            &name("a")?,
            DecodeLimits::default(),
            unlimited(),
            &CancellationToken::new(),
        ).await,
        Err(OperationFailure {
            error: TreeReadError::WrongRootKind,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn zero_height_limit_fails_without_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let failure = lookup_tree_entry(
        &store,
        ObjectId {
            kind: ObjectKind::TreePage,
            digest: Digest::ZERO,
        },
        &name("a")?,
        DecodeLimits {
            maximum_page_height: 0,
            ..DecodeLimits::default()
        },
        unlimited(),
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("zero-height lookup unexpectedly succeeded")?;
    assert!(matches!(failure.error, TreeReadError::InvalidHeightLimit));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn forged_parent_lower_bound_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let leaf = put_page(
        &store,
        &TreePage::Leaf(vec![TreeEntry {
            name: name("b")?,
            file_id: FileId::from_bytes([8; 16]),
            kind: FileKind::Regular,
        }]),
    ).await?;
    let root = put_page(
        &store,
        &TreePage::Internal(vec![Child {
            first: name("a")?,
            page: leaf,
        }]),
    ).await?;
    assert!(matches!(
        lookup_tree_entry(
            &store,
            root,
            &name("a")?,
            DecodeLimits::default(),
            unlimited(),
            &CancellationToken::new(),
        ).await,
        Err(OperationFailure {
            error: TreeReadError::ChildBoundsMismatch,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_async_lookup_reads_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put_page(&store, &TreePage::Leaf(Vec::new())).await?;
    let target = name("target")?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut future = std::pin::pin!(lookup_tree_entry(
        &store,
        root,
        &target,
        DecodeLimits::default(),
        unlimited(),
        &cancellation,
    ));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let Poll::Ready(result) = future.as_mut().poll(&mut context) else {
        return Err("pre-cancelled asynchronous lookup remained pending".into());
    };
    let failure = result
        .err()
        .ok_or("pre-cancelled asynchronous lookup unexpectedly succeeded")?;
    assert!(matches!(failure.error, TreeReadError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn batch_lookup_reads_each_shared_frontier_once_and_preserves_input_order()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let left = put_page(
        &store,
        &TreePage::Leaf(vec![
            TreeEntry {
                name: name("a")?,
                file_id: FileId::from_bytes([1; 16]),
                kind: FileKind::Regular,
            },
            TreeEntry {
                name: name("b")?,
                file_id: FileId::from_bytes([2; 16]),
                kind: FileKind::Regular,
            },
        ]),
    ).await?;
    let right = put_page(
        &store,
        &TreePage::Leaf(vec![
            TreeEntry {
                name: name("m")?,
                file_id: FileId::from_bytes([3; 16]),
                kind: FileKind::Regular,
            },
            TreeEntry {
                name: name("n")?,
                file_id: FileId::from_bytes([4; 16]),
                kind: FileKind::Regular,
            },
        ]),
    ).await?;
    let root = put_page(
        &store,
        &TreePage::Internal(vec![
            Child {
                first: name("a")?,
                page: left,
            },
            Child {
                first: name("m")?,
                page: right,
            },
        ]),
    ).await?;
    let queries = vec![name("n")?, name("a")?, name("n")?, name("z")?];
    let result = lookup_tree_entries(
        &store,
        root,
        &queries,
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await?;
    assert_eq!(
        result
            .value
            .iter()
            .map(|entry| entry.as_ref().map(|entry| entry.file_id))
            .collect::<Vec<_>>(),
        vec![
            Some(FileId::from_bytes([4; 16])),
            Some(FileId::from_bytes([1; 16])),
            Some(FileId::from_bytes([4; 16])),
            None,
        ]
    );
    assert_eq!(result.work.page_reads, 3);
    assert_eq!(result.work.backend_read_operations, 2);
    assert_eq!(result.work.items_returned, 4);

    Ok(())
}

#[tokio::test]
async fn batch_cancellation_and_query_admission_precede_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put_page(&store, &TreePage::Leaf(Vec::new())).await?;
    let queries = vec![name("a")?, name("b")?];
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = (lookup_tree_entries(
        &store,
        root,
        &queries,
        2,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("pre-cancelled batch unexpectedly succeeded")?;
    assert!(matches!(failure.error, TreeReadError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());

    let failure = lookup_tree_entries(
        &store,
        root,
        &queries,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("oversized query batch unexpectedly succeeded")?;
    assert!(matches!(failure.error, TreeReadError::TooManyQueries));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn batch_error_translation_is_total() {
    use persistent_batch::Error as BatchError;

    assert!(matches!(
        map_batch_error(BatchError::Cancelled),
        TreeReadError::Cancelled
    ));
    assert!(matches!(
        map_batch_error(BatchError::Empty),
        TreeReadError::EmptyBatch
    ));
    assert!(matches!(
        map_batch_error(BatchError::TooManyQueries),
        TreeReadError::TooManyQueries
    ));
    assert!(matches!(
        map_batch_error(BatchError::WrongRootKind),
        TreeReadError::WrongRootKind
    ));
    assert!(matches!(
        map_batch_error(BatchError::InvalidLimits),
        TreeReadError::InvalidHeightLimit
    ));
    assert!(matches!(
        map_batch_error(BatchError::HeightExceeded),
        TreeReadError::HeightExceeded
    ));
    assert!(matches!(
        map_batch_error(BatchError::CycleOrAlias),
        TreeReadError::Cycle
    ));
    assert!(matches!(
        map_batch_error(BatchError::ChildBoundsMismatch),
        TreeReadError::ChildBoundsMismatch
    ));
    assert!(matches!(
        map_batch_error(BatchError::InvalidRouting),
        TreeReadError::InvalidRouting
    ));
    assert!(matches!(
        map_batch_error(BatchError::AllocationFailed),
        TreeReadError::AllocationFailed
    ));
    assert!(matches!(
        map_batch_error(BatchError::Storage(ObjectStoreError::Missing)),
        TreeReadError::Storage(ObjectStoreError::Missing)
    ));
    assert!(matches!(
        map_batch_error(BatchError::Decode(CanonicalDecodeError::LengthOverflow)),
        TreeReadError::Decode(CanonicalDecodeError::LengthOverflow)
    ));
    assert!(matches!(
        map_batch_error(BatchError::Work(WorkError::Overflow)),
        TreeReadError::Work(WorkError::Overflow)
    ));
}
