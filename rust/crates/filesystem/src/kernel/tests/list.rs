use super::*;
use crate::foundation::{Digest, FileId};
use crate::kernel::{FileKind, NameEncoding, TreePage, encode_tree_page, tree_page_id};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectKind, };
use acyclic_stream::OwnedBytes;

use crate::kernel::Child;
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

async fn put(store: &MemoryObjectStore, page: &TreePage) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = tree_page_id(page, 8)?;
    AsyncObjectStore::put(
        store,
        id,
        OwnedBytes::from(encode_tree_page(page, 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?),
        WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
    Ok(id)
}

async fn tree(store: &MemoryObjectStore) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let a = put(store, &TreePage::Leaf(vec![entry("a", 1)?, entry("b", 2)?])).await?;
    let c = put(store, &TreePage::Leaf(vec![entry("c", 3)?, entry("d", 4)?])).await?;
    let e = put(store, &TreePage::Leaf(vec![entry("e", 5)?, entry("f", 6)?])).await?;
    put(
        store,
        &TreePage::Internal(vec![
            Child {
                first: name("a")?,
                page: a,
            },
            Child {
                first: name("c")?,
                page: c,
            },
            Child {
                first: name("e")?,
                page: e,
            },
        ]),
    ).await
}

async fn four_leaf_tree(
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
        ).await?;
        if index == 2 {
            third = Some(page);
        }
        children.push(Child {
            first: name(value)?,
            page,
        });
    }
    Ok((
        put(store, &TreePage::Internal(children)).await?,
        third.ok_or("missing third fixture leaf")?,
    ))
}

#[tokio::test]
async fn bounded_listing_exposes_one_authenticated_unvisited_successor()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, expected) = four_leaf_tree(&store).await?;
    let limits = DecodeLimits::default();
    let page = list_tree_entries(&store, root, None, 1, limits, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
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

#[tokio::test]
async fn cursor_page_reads_only_frontier_and_required_successors()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store).await?;
    let cursor = name("c")?;
    let page = list_tree_entries(
        &store,
        root,
        Some(&cursor),
        2,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await?;
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

    Ok(())
}

#[tokio::test]
async fn empty_tail_is_proved_without_scanning_earlier_subtrees() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let root = tree(&store).await?;
    let page = list_tree_entries(
        &store,
        root,
        Some(&name("z")?),
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await?;
    assert!(page.entries.is_empty());
    assert!(!page.has_more);
    assert_eq!(page.work.page_reads, 2);
    Ok(())
}

#[tokio::test]
async fn output_bound_is_admitted_before_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store).await?;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.items_returned = 1;
    let failure = list_tree_entries(&store, root, None, 2, DecodeLimits::default(), budget, &CancellationToken::new())
        .await
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

#[tokio::test]
async fn volume_object_ceiling_does_not_size_a_tiny_cursor_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store).await?;
    let limits = DecodeLimits {
        maximum_visited_pages: u32::MAX,
        maximum_page_height: 64,
        ..DecodeLimits::default()
    };
    let page = list_tree_entries(&store, root, None, 1, limits, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
    assert_eq!(page.entries.len(), 1);
    assert!(page.work.peak_allocation_bytes < 100_000);
    assert_eq!(page.work.page_reads, 2);
    Ok(())
}

#[tokio::test]
async fn pagination_rejects_every_invalid_admission_and_authenticated_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store).await?;
    let zero = list_tree_entries(
        &store,
        root,
        None,
        0,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("zero directory page limit unexpectedly succeeded")?;
    assert!(matches!(zero.error, DirectoryReadError::ZeroLimit));
    assert_eq!(*zero.work, WorkCounters::default());

    // Host bindings build cursors without the volume's name bound.
    let oversized = LogicalName::new(NameEncoding::Utf8, vec![b'a'; 256], u32::MAX)?;
    let cursor = list_tree_entries(
        &store,
        root,
        Some(&oversized),
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("oversized directory cursor unexpectedly succeeded")?;
    assert!(matches!(
        cursor.error,
        DirectoryReadError::Decode(CanonicalDecodeError::FieldTooLarge {
            observed: 256,
            maximum: 255,
        })
    ));
    assert_eq!(*cursor.work, WorkCounters::default());

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
        &CancellationToken::new(),
    )
    .await
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
        &CancellationToken::new(),
    )
    .await
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
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("directory height limit unexpectedly succeeded")?;
    assert!(matches!(height.error, DirectoryReadError::HeightExceeded));
    assert_eq!(height.work.page_reads, 1);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = (list_tree_entries(
        &store,
        root,
        None,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancelled,
    )).await
    .err()
    .ok_or("cancelled directory listing unexpectedly succeeded")?;
    assert!(matches!(failure.error, DirectoryReadError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());

    let leaf = put(
        &store,
        &TreePage::Leaf(vec![entry("c", 3)?, entry("d", 4)?]),
    ).await?;
    let forged = put(
        &store,
        &TreePage::Internal(vec![Child {
            first: name("a")?,
            page: leaf,
        }]),
    ).await?;
    let failure = list_tree_entries(
        &store,
        forged,
        None,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("forged directory lower bound unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        DirectoryReadError::ChildBoundsMismatch
    ));
    assert_eq!(failure.work.page_reads, 2);
    Ok(())
}

#[tokio::test]
async fn custody_listing_retains_only_known_sdk_prefix_when_provider_panics()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncObjectStore;
    use crate::storage::{ObjectRead, ObjectReadRequest, ObjectResult};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct PanickingRead {
        inner: MemoryObjectStore,
        reached: AtomicUsize,
    }
    impl AsyncObjectStore for PanickingRead {
        async fn put(&self, id: ObjectId, bytes: OwnedBytes, budget: WorkBudget, cancellation: &CancellationToken)
        -> ObjectResult<()> {
            AsyncObjectStore::put(&self.inner, id, bytes, budget, cancellation).await
        }
        async fn read(&self, _id: ObjectId, _maximum: u64, _budget: WorkBudget, _cancellation: &CancellationToken)
        -> ObjectResult<ObjectRead> {
            self.reached.fetch_add(1, Ordering::SeqCst);
            panic!("provider unwinds without returning its private work");
        }
        async fn read_many(&self, requests: &[ObjectReadRequest], budget: WorkBudget, cancellation: &CancellationToken)
        -> ObjectResult<Vec<ObjectRead>> {
            AsyncObjectStore::read_many(&self.inner, requests, budget, cancellation).await
        }
        async fn contains(&self, id: ObjectId, budget: WorkBudget, cancellation: &CancellationToken)
        -> ObjectResult<bool> {
            AsyncObjectStore::contains(&self.inner, id, budget, cancellation).await
        }
    }
    let inner = MemoryObjectStore::default();
    let root = tree(&inner).await?;
    let store = PanickingRead { inner, reached: AtomicUsize::new(0) };
    let cancellation = CancellationToken::new();
    let owner = crate::kernel::ProofDataCustody::default();
    let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(async {
        (list_tree_entries_async_with_custody(
            &store, root, None, 2, DecodeLimits::default(), WorkBudget::UNBOUNDED,
            &cancellation, Some(&owner),
        )).await
    })).await;
    assert!(result.is_err());
    assert_eq!(store.reached.load(Ordering::SeqCst), 1);
    let known = owner.pending_data_work();
    assert_eq!(known.page_reads, 1);
    assert_eq!(known.allocation_operations, 4, "owned output, visited/frontier buffers, and actual read-future frame");
    assert_eq!(known.backend_read_operations, 0, "no provider receipt was returned");
    assert_eq!(known.object_bytes_read, 0);
    assert_eq!(known.bytes_copied, 0);
    assert_eq!(owner.operation_cache_bytes(), 0);
    Ok(())
}
