use super::*;
use crate::foundation::{Digest, FileId};
use crate::kernel::{FileKind, NameEncoding, TreeChild, TreePage, encode_tree_page, tree_page_id};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectKind, ObjectStore};
use crate::{CachedObjectStore, ObjectCacheOptions};
use bytes::Bytes;
use std::task::{Context, Poll, Waker};

fn name(value: &str) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(
        NameEncoding::Utf8,
        value.as_bytes().to_vec(),
        255,
    )?)
}

fn entry(value: &str, byte: u8) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    Ok(TreeEntry {
        name: name(value)?,
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Regular,
    })
}

fn put(store: &MemoryObjectStore, page: &TreePage) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = tree_page_id(page, 8)?;
    ObjectStore::put(
        store,
        id,
        Bytes::from(encode_tree_page(page, 8)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(id)
}

fn tree(store: &MemoryObjectStore) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let a = put(store, &TreePage::Leaf(vec![entry("a", 1)?, entry("b", 2)?]))?;
    let c = put(store, &TreePage::Leaf(vec![entry("c", 3)?, entry("d", 4)?]))?;
    let e = put(store, &TreePage::Leaf(vec![entry("e", 5)?, entry("f", 6)?]))?;
    put(
        store,
        &TreePage::Internal(vec![
            TreeChild {
                first_name: name("a")?,
                page: a,
            },
            TreeChild {
                first_name: name("c")?,
                page: c,
            },
            TreeChild {
                first_name: name("e")?,
                page: e,
            },
        ]),
    )
}

fn four_leaf_tree(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, ObjectId), Box<dyn std::error::Error>> {
    let mut children = Vec::new();
    let mut third = None;
    for (index, value) in ["a", "b", "c", "d"].into_iter().enumerate() {
        let page = put(
            store,
            &TreePage::Leaf(vec![entry(
                value,
                u8::try_from(index + 1).map_err(|_| "fixture index overflow")?,
            )?]),
        )?;
        if index == 2 {
            third = Some(page);
        }
        children.push(TreeChild {
            first_name: name(value)?,
            page,
        });
    }
    Ok((
        put(store, &TreePage::Internal(children))?,
        third.ok_or("missing third fixture leaf")?,
    ))
}

#[test]
fn bounded_listing_exposes_one_authenticated_unvisited_successor()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, expected) = four_leaf_tree(&store)?;
    let limits = DecodeLimits::default();
    let page = list_tree_entries(&store, root, None, 1, limits, WorkBudget::UNBOUNDED)?;
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.name.as_bytes())
            .collect::<Vec<_>>(),
        [b"a".as_slice()]
    );
    assert!(page.has_more);
    assert_eq!(
        page.next_residency,
        Some(ResidencyHint {
            request: crate::ObjectReadRequest {
                object_id: expected,
                maximum_bytes: limits.maximum_page_object_bytes(),
            },
            reason: ResidencyReason::DirectorySuccessor,
        })
    );
    assert_eq!(page.work.page_reads, 3);
    Ok(())
}

#[test]
fn cursor_page_reads_only_frontier_and_required_successors()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store)?;
    let cursor = name("c")?;
    let page = list_tree_entries(
        &store,
        root,
        Some(&cursor),
        2,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.name.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"d".as_slice(), b"e".as_slice()]
    );
    assert!(page.has_more);
    assert_eq!(page.work.page_reads, 3);
    assert_eq!(page.work.items_returned, 2);

    let cancellation = CancellationToken::new();
    let mut future = std::pin::pin!(list_tree_entries_async(
        &store,
        root,
        Some(&cursor),
        2,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation
    ));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let Poll::Ready(asynchronous) = future.as_mut().poll(&mut context) else {
        return Err("memory async listing remained pending".into());
    };
    assert_eq!(asynchronous?, page);
    Ok(())
}

#[test]
fn cached_pagination_traverses_shared_internal_and_leaf_pages_without_backend_rereads()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::new(1024 * 1024)?;
    let root = tree(&backend)?;
    let store = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 16,
            maximum_bytes: 1024 * 1024,
            maximum_in_flight: 4,
            maximum_waiters_per_object: 4,
        },
    )?;
    let cursor = name("b")?;
    let cancellation = CancellationToken::new();
    let cold = crate::async_storage::poll_ready(list_tree_entries_async(
        &store,
        root,
        Some(&cursor),
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cache-backed cold pagination suspended")??;
    assert_eq!(
        cold.entries
            .iter()
            .map(|entry| entry.name.as_bytes())
            .collect::<Vec<_>>(),
        [
            b"c".as_slice(),
            b"d".as_slice(),
            b"e".as_slice(),
            b"f".as_slice()
        ]
    );
    assert!(!cold.has_more);
    assert_eq!(cold.work.backend_read_operations, 4);

    let warm = crate::async_storage::poll_ready(list_tree_entries_async(
        &store,
        root,
        Some(&cursor),
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cache-backed warm pagination suspended")??;
    assert_eq!(warm.entries, cold.entries);
    assert_eq!(warm.work.backend_read_operations, 0);
    assert_eq!(warm.work.object_bytes_read, 0);
    assert_eq!(store.stats()?.decoded_hits, 4);
    Ok(())
}

#[test]
fn empty_tail_is_proved_without_scanning_earlier_subtrees() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let root = tree(&store)?;
    let page = list_tree_entries(
        &store,
        root,
        Some(&name("z")?),
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(page.entries.is_empty());
    assert!(!page.has_more);
    assert_eq!(page.work.page_reads, 2);
    Ok(())
}

#[test]
fn output_bound_is_admitted_before_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store)?;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.items_returned = 1;
    let failure = list_tree_entries(&store, root, None, 2, DecodeLimits::default(), budget)
        .err()
        .ok_or("unadmitted page unexpectedly read")?;
    assert!(matches!(
        failure.error,
        DirectoryReadError::Work(WorkError::BudgetExceeded {
            counter: "items_returned",
            ..
        })
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn volume_object_ceiling_does_not_size_a_tiny_cursor_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store)?;
    let limits = DecodeLimits {
        maximum_visited_pages: u32::MAX,
        maximum_page_height: 64,
        ..DecodeLimits::default()
    };
    let page = list_tree_entries(&store, root, None, 1, limits, WorkBudget::UNBOUNDED)?;
    assert_eq!(page.entries.len(), 1);
    assert!(page.work.peak_allocation_bytes < 100_000);
    assert_eq!(page.work.page_reads, 2);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn pagination_rejects_every_invalid_admission_and_authenticated_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store)?;
    let zero = list_tree_entries(
        &store,
        root,
        None,
        0,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("zero directory page limit unexpectedly succeeded")?;
    assert!(matches!(zero.error, DirectoryReadError::ZeroLimit));
    assert_eq!(*zero.work, WorkCounters::default());

    let wrong = list_tree_entries(
        &store,
        ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([8; 32]),
        },
        None,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("wrong directory root kind unexpectedly succeeded")?;
    assert!(matches!(wrong.error, DirectoryReadError::WrongRootKind));
    assert_eq!(*wrong.work, WorkCounters::default());

    let invalid = list_tree_entries(
        &store,
        root,
        None,
        1,
        DecodeLimits {
            maximum_page_height: 0,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("invalid directory limits unexpectedly succeeded")?;
    assert!(matches!(invalid.error, DirectoryReadError::InvalidLimits));
    assert_eq!(*invalid.work, WorkCounters::default());

    let height = list_tree_entries(
        &store,
        root,
        None,
        1,
        DecodeLimits {
            maximum_page_height: 1,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("directory height limit unexpectedly succeeded")?;
    assert!(matches!(height.error, DirectoryReadError::HeightExceeded));
    assert_eq!(height.work.page_reads, 1);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = crate::async_storage::poll_ready(list_tree_entries_async(
        &store,
        root,
        None,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancelled,
    ))
    .ok_or("cancelled directory listing blocked")?
    .err()
    .ok_or("cancelled directory listing unexpectedly succeeded")?;
    assert!(matches!(failure.error, DirectoryReadError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());

    let leaf = put(
        &store,
        &TreePage::Leaf(vec![entry("c", 3)?, entry("d", 4)?]),
    )?;
    let forged = put(
        &store,
        &TreePage::Internal(vec![TreeChild {
            first_name: name("a")?,
            page: leaf,
        }]),
    )?;
    let failure = list_tree_entries(
        &store,
        forged,
        None,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("forged directory lower bound unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        DirectoryReadError::ChildBoundsMismatch
    ));
    assert_eq!(failure.work.page_reads, 2);
    Ok(())
}

#[test]
fn pagination_error_translation_is_total() {
    use persistent_pagination::Error as PaginationError;

    assert!(matches!(
        map_error(PaginationError::Cancelled),
        DirectoryReadError::Cancelled
    ));
    assert!(matches!(
        map_error(PaginationError::ZeroLimit),
        DirectoryReadError::ZeroLimit
    ));
    assert!(matches!(
        map_error(PaginationError::LimitOverflow),
        DirectoryReadError::LimitOverflow
    ));
    assert!(matches!(
        map_error(PaginationError::WrongRootKind),
        DirectoryReadError::WrongRootKind
    ));
    assert!(matches!(
        map_error(PaginationError::InvalidLimits),
        DirectoryReadError::InvalidLimits
    ));
    assert!(matches!(
        map_error(PaginationError::HeightExceeded),
        DirectoryReadError::HeightExceeded
    ));
    assert!(matches!(
        map_error(PaginationError::CycleOrAlias),
        DirectoryReadError::Cycle
    ));
    assert!(matches!(
        map_error(PaginationError::ChildBoundsMismatch),
        DirectoryReadError::ChildBoundsMismatch
    ));
    assert!(matches!(
        map_error(PaginationError::TraversalState),
        DirectoryReadError::TraversalState
    ));
    assert!(matches!(
        map_error(PaginationError::AllocationFailed),
        DirectoryReadError::AllocationFailed
    ));
    assert!(matches!(
        map_error(PaginationError::Storage(ObjectStoreError::Missing)),
        DirectoryReadError::Storage(ObjectStoreError::Missing)
    ));
    assert!(matches!(
        map_error(PaginationError::Decode(
            CanonicalDecodeError::LengthOverflow
        )),
        DirectoryReadError::Decode(CanonicalDecodeError::LengthOverflow)
    ));
    assert!(matches!(
        map_error(PaginationError::Work(WorkError::Overflow)),
        DirectoryReadError::Work(WorkError::Overflow)
    ));
}
