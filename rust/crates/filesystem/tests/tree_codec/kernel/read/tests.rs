use acyclic_fs::CancellationToken;
use acyclic_fs::kernel::{
    Child, DecodeLimits, FileKind, LogicalName, NameEncoding, TreeEntry, TreePage, TreeReadError,
    lookup_tree_entries, lookup_tree_entry,
};
use acyclic_fs_format::{
    identity::{Digest, FileId},
    object_reference::{ObjectId, ObjectKind},
};
use acyclic_fs_work::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use std::task::{Context, Poll, Waker};

use super::super::memory::memory_objects;
use super::super::tree_objects::put_tree_page;

fn name(value: &str) -> Result<LogicalName, acyclic_fs::kernel::TreePageError> {
    LogicalName::new(NameEncoding::Utf8, value.as_bytes().to_vec())
}

#[tokio::test]
async fn lookup_reads_only_one_frontier() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let leaf = TreePage::Leaf(vec![TreeEntry {
        name: name("target")?,
        file_id: FileId::from_bytes([4; 16]),
        kind: FileKind::Regular,
    }]);
    let leaf_id = put_tree_page(&store, &leaf, 16).await?;
    let root = TreePage::Internal(vec![Child {
        first: name("target")?,
        page: leaf_id,
    }]);
    let root_id = put_tree_page(&store, &root, 16).await?;

    let result = lookup_tree_entry(
        &store,
        root_id,
        &name("target")?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        result.value.map(|entry| entry.file_id),
        Some(FileId::from_bytes([4; 16]))
    );
    assert_eq!(result.work.page_reads, 2);
    Ok(())
}

#[tokio::test]
async fn budget_stops_before_an_unadmitted_child_read() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let leaf_id = put_tree_page(&store, &TreePage::Leaf(Vec::new()), 16).await?;
    let root_id = put_tree_page(
        &store,
        &TreePage::Internal(vec![Child {
            first: name("a")?,
            page: leaf_id,
        }]),
        16,
    )
    .await?;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.page_reads = 1;
    assert!(matches!(
        lookup_tree_entry(
            &store,
            root_id,
            &name("a")?,
            DecodeLimits::default(),
            budget,
            &CancellationToken::new(),
        )
        .await,
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
    let store = memory_objects();
    assert!(matches!(
        lookup_tree_entry(
            &store,
            ObjectId {
                storage: None,
                kind: ObjectKind::Blob,
                digest: Digest::ZERO,
            },
            &name("a")?,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await,
        Err(OperationFailure {
            error: TreeReadError::WrongRootKind,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn zero_height_limit_fails_without_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let failure = lookup_tree_entry(
        &store,
        ObjectId {
            storage: None,
            kind: ObjectKind::TreePage,
            digest: Digest::ZERO,
        },
        &name("a")?,
        DecodeLimits {
            maximum_page_height: 0,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("zero-height lookup unexpectedly succeeded")?;
    assert!(matches!(failure.error, TreeReadError::InvalidHeightLimit));
    assert_eq!(*failure.work()?, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn forged_parent_lower_bound_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let leaf = put_tree_page(
        &store,
        &TreePage::Leaf(vec![TreeEntry {
            name: name("b")?,
            file_id: FileId::from_bytes([8; 16]),
            kind: FileKind::Regular,
        }]),
        16,
    )
    .await?;
    let root = put_tree_page(
        &store,
        &TreePage::Internal(vec![Child {
            first: name("a")?,
            page: leaf,
        }]),
        16,
    )
    .await?;
    assert!(matches!(
        lookup_tree_entry(
            &store,
            root,
            &name("a")?,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await,
        Err(OperationFailure {
            error: TreeReadError::ChildBoundsMismatch,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_async_lookup_reads_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let root = put_tree_page(&store, &TreePage::Leaf(Vec::new()), 16).await?;
    let target = name("target")?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut future = std::pin::pin!(lookup_tree_entry(
        &store,
        root,
        &target,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
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
    assert_eq!(*failure.work()?, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn batch_cancellation_precedes_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let root = put_tree_page(&store, &TreePage::Leaf(Vec::new()), 16).await?;
    let queries = vec![name("a")?, name("b")?];
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = (lookup_tree_entries(
        &store,
        root,
        &queries,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("pre-cancelled batch unexpectedly succeeded")?;
    assert!(matches!(failure.error, TreeReadError::Cancelled));
    assert_eq!(*failure.work()?, WorkCounters::default());

    Ok(())
}
