use super::*;
use crate::foundation::FileId;
use crate::kernel::tree_mutation::TreeFormat;
use crate::kernel::{
    FileKind, LogicalName, NameEncoding, TreeEntry, TreePage, encode_tree_page, tree_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectKind, ObjectStore, object_digest};
use crate::test_support::OwnedReadObjectStore;
use crate::{CachedObjectStore, ObjectCacheOptions, ObjectCacheStats};
use acyclic_stream::OwnedBytes;

fn put_tree_page(
    store: &MemoryObjectStore,
    name: &[u8],
    file_byte: u8,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let page = TreePage::Leaf(vec![TreeEntry {
        name: LogicalName::new(NameEncoding::Utf8, name.to_vec(), 255)?,
        file_id: FileId::from_bytes([file_byte; 16]),
        kind: FileKind::Regular,
    }]);
    let object = tree_page_id(&page, 8)?;
    ObjectStore::put(
        store,
        object,
        OwnedBytes::from(encode_tree_page(&page, 8)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(object)
}

fn only_page_name(page: &OwnedPage<TreeFormat>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let page = match &page.page {
        PageLease::Owned(page) => page,
        PageLease::Shared { page, .. } => page,
    };
    let Page::Leaf(entries) = page else {
        return Err("expected a leaf page".into());
    };
    let entry = entries.first().ok_or("expected one leaf entry")?;
    Ok(entry.name.as_bytes().to_vec())
}

fn assert_resident_cache_shape(stats: ObjectCacheStats, decoded_hits: u64) {
    assert_eq!(stats.decoded_hits, decoded_hits);
    assert_eq!(stats.resident_decoded_pages, 2);
    assert_eq!(
        stats.resident_entries,
        stats.resident_canonical_objects + stats.resident_decoded_pages
    );
    assert_eq!(
        stats.resident_bytes,
        stats.resident_canonical_bytes + stats.resident_decoded_bytes
    );
}

#[test]
fn decoded_pages_are_shared_across_operations_and_mutations_copy_on_write()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::new(1024 * 1024)?;
    let page = put_tree_page(&backend, b"cached", 9)?;
    let store = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 8,
            maximum_bytes: 1024 * 1024,
            maximum_in_flight: 2,
            maximum_waiters_per_object: 2,
        },
    )?;
    let cancellation = CancellationToken::new();

    let mut first_allocations = AllocationLedger::default();
    let mut first_work = WorkCounters::default();
    let first = crate::async_storage::poll_ready(read_page::<_, TreeFormat>(
        &store,
        page,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut first_allocations,
        &mut first_work,
    ))
    .ok_or("immediate read suspended")??;
    assert!(matches!(first.page, PageLease::Shared { .. }));
    assert_eq!(first.logical_bytes, 0);
    assert_eq!(first_allocations.live_bytes(), 0);
    assert_eq!(first_work.backend_read_operations, 1);

    let mut warm_allocations = AllocationLedger::default();
    let mut warm_work = WorkCounters::default();
    let warm = crate::async_storage::poll_ready(read_page::<_, TreeFormat>(
        &store,
        page,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut warm_allocations,
        &mut warm_work,
    ))
    .ok_or("immediate warm read suspended")??;
    assert!(matches!(warm.page, PageLease::Shared { .. }));
    assert_eq!(warm_work.page_reads, 1);
    assert_eq!(warm_work.backend_read_operations, 0);
    assert_eq!(warm_work.object_bytes_read, 0);
    assert_eq!(warm_allocations.live_bytes(), 0);
    let warm_stats = store.stats()?;
    assert_eq!(warm_stats.decoded_hits, 1);
    assert_eq!(warm_stats.resident_decoded_pages, 1);
    assert!(warm_stats.resident_decoded_bytes > 0);

    let mut mutation_allocations = AllocationLedger::default();
    let mut mutation_work = WorkCounters::default();
    let mutable = crate::async_storage::poll_ready(read_page_mutable::<_, TreeFormat>(
        &store,
        page,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut mutation_allocations,
        &mut mutation_work,
    ))
    .ok_or("immediate mutable read suspended")??;
    assert!(matches!(mutable.page, PageLease::Owned(_)));
    assert!(mutable.logical_bytes > 0);
    assert_eq!(mutation_work.backend_read_operations, 0);
    assert!(mutation_work.bytes_copied >= mutable.logical_bytes);
    mutation_allocations.release(mutable.logical_bytes)?;
    assert_eq!(mutation_allocations.live_bytes(), 0);

    let mut denied_allocations = AllocationLedger::default();
    let mut denied_work = WorkCounters::default();
    let denied = crate::async_storage::poll_ready(read_page_mutable::<_, TreeFormat>(
        &store,
        page,
        DecodeLimits::default(),
        WorkBudget {
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
        &mut denied_allocations,
        &mut denied_work,
    ))
    .ok_or("immediate denied mutable read suspended")?
    .err()
    .ok_or("cached mutable page bypassed its allocation budget")?;
    assert!(matches!(denied, Error::Allocation(_)));
    assert_eq!(denied_allocations.live_bytes(), 0);
    Ok(())
}

#[test]
fn decoded_cache_rejection_releases_owned_page_allocation() -> Result<(), Box<dyn std::error::Error>>
{
    let store = OwnedReadObjectStore::default();
    let page = put_tree_page(&store.inner, b"rejected", 41)?;
    store
        .reject_decoded_admission
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let failure = crate::async_storage::poll_ready(read_page::<_, TreeFormat>(
        &store,
        page,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        &mut allocations,
        &mut work,
    ))
    .ok_or("owned page read suspended")?
    .err()
    .ok_or("decoded-cache rejection unexpectedly succeeded")?;
    assert!(matches!(failure, Error::Storage(ObjectStoreError::Corrupt)));
    assert_eq!(allocations.live_bytes(), 0);
    assert_eq!(work.backend_read_operations, 1);
    Ok(())
}

#[test]
fn mixed_page_batches_read_only_cold_identities_and_preserve_order()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::new(1024 * 1024)?;
    let first = put_tree_page(&backend, b"first", 1)?;
    let second = put_tree_page(&backend, b"second", 2)?;
    let store = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 8,
            maximum_bytes: 1024 * 1024,
            maximum_in_flight: 2,
            maximum_waiters_per_object: 2,
        },
    )?;
    let limits = DecodeLimits::default();
    let cancellation = CancellationToken::new();

    let mut seed_allocations = AllocationLedger::default();
    let mut seed_work = WorkCounters::default();
    let seed = crate::async_storage::poll_ready(read_page::<_, TreeFormat>(
        &store,
        first,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut seed_allocations,
        &mut seed_work,
    ))
    .ok_or("seed read suspended")??;
    assert!(matches!(seed.page, PageLease::Shared { .. }));
    assert_eq!(seed_allocations.live_bytes(), 0);

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut batch = crate::async_storage::poll_ready(read_pages::<_, TreeFormat, _>(
        &store,
        [first, second, first].into_iter(),
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    ))
    .ok_or("mixed batch suspended")??;
    assert_eq!(work.page_reads, 3);
    assert_eq!(work.backend_read_operations, 1);
    let mut names = Vec::new();
    for _ in 0..3 {
        let page = batch.next(
            limits,
            WorkBudget::UNBOUNDED,
            &cancellation,
            &mut allocations,
            &mut work,
        )?;
        assert!(matches!(page.page, PageLease::Shared { .. }));
        names.push(only_page_name(&page)?);
    }
    batch.finish(&mut allocations)?;
    assert_eq!(
        names,
        [b"first".to_vec(), b"second".to_vec(), b"first".to_vec()]
    );
    assert_eq!(allocations.live_bytes(), 0);

    assert_resident_cache_shape(store.stats()?, 2);

    let mut warm_allocations = AllocationLedger::default();
    let mut warm_work = WorkCounters::default();
    let mut warm = crate::async_storage::poll_ready(read_pages::<_, TreeFormat, _>(
        &store,
        [first, second, first].into_iter(),
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut warm_allocations,
        &mut warm_work,
    ))
    .ok_or("warm batch suspended")??;
    assert_eq!(warm_work.backend_read_operations, 0);
    assert_eq!(warm_work.object_bytes_read, 0);
    for _ in 0..3 {
        assert!(matches!(
            warm.next(
                limits,
                WorkBudget::UNBOUNDED,
                &cancellation,
                &mut warm_allocations,
                &mut warm_work,
            )?
            .page,
            PageLease::Shared { .. }
        ));
    }
    warm.finish(&mut warm_allocations)?;
    assert_eq!(warm_allocations.live_bytes(), 0);
    assert_eq!(store.stats()?.decoded_hits, 5);
    Ok(())
}

#[test]
fn post_read_budget_failure_releases_every_decoded_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let page = TreePage::Leaf(vec![TreeEntry {
        name: LogicalName::new(NameEncoding::Utf8, b"name".to_vec(), 255)?,
        file_id: FileId::from_bytes([7; 16]),
        kind: FileKind::Regular,
    }]);
    let object = tree_page_id(&page, 8)?;
    ObjectStore::put(
        &store,
        object,
        OwnedBytes::from(encode_tree_page(&page, 8)?),
        WorkBudget::UNBOUNDED,
    )?;
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_copied = 0;
    let cancellation = CancellationToken::new();
    let result = crate::async_storage::poll_ready(read_page::<_, TreeFormat>(
        &store,
        object,
        DecodeLimits::default(),
        budget,
        &cancellation,
        &mut allocations,
        &mut work,
    ))
    .ok_or("memory-backed page read remained pending")?;
    assert!(matches!(
        result,
        Err(Error::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            ..
        }))
    ));
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}

#[test]
fn single_page_read_authenticates_decode_and_releases_backend_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let object = put_tree_page(&store, b"entry", 9)?;
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let cancellation = CancellationToken::new();
    let owned = crate::async_storage::poll_ready(read_page::<_, TreeFormat>(
        &store,
        object,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    ))
    .ok_or("memory-backed page read remained pending")??;
    assert!(matches!(owned.page, PageLease::Owned(Page::Leaf(_))));
    assert!(owned.logical_bytes > 0);
    assert_eq!(allocations.live_bytes(), owned.logical_bytes);
    assert_eq!(work.page_reads, 1);
    assert_eq!(work.backend_read_operations, 1);
    allocations.release(owned.logical_bytes)?;
    assert_eq!(allocations.live_bytes(), 0);

    let malformed = OwnedBytes::from_static(b"not a canonical tree page");
    let malformed_id = ObjectId {
        kind: ObjectKind::TreePage,
        digest: object_digest(ObjectKind::TreePage, &malformed),
    };
    ObjectStore::put(&store, malformed_id, malformed, WorkBudget::UNBOUNDED)?;
    let mut allocations = AllocationLedger::default();
    let mut malformed_work = WorkCounters::default();
    let failure = crate::async_storage::poll_ready(read_page::<_, TreeFormat>(
        &store,
        malformed_id,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut malformed_work,
    ))
    .ok_or("malformed page read remained pending")?
    .err()
    .ok_or("malformed page decoded")?;
    assert!(matches!(failure, Error::Decode(_)));
    assert_eq!(allocations.live_bytes(), 0);
    assert_eq!(malformed_work.page_reads, 1);
    assert_eq!(malformed_work.backend_read_operations, 1);
    Ok(())
}

#[test]
fn shape_valid_malformed_tree_page_releases_decoded_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let page = TreePage::Leaf(vec![TreeEntry {
        name: LogicalName::new(NameEncoding::Utf8, b"shape".to_vec(), 255)?,
        file_id: FileId::from_bytes([43; 16]),
        kind: FileKind::Regular,
    }]);
    let mut malformed = encode_tree_page(&page, 8)?;
    *malformed.last_mut().ok_or("encoded tree page is empty")? = u8::MAX;
    let object = ObjectId {
        kind: ObjectKind::TreePage,
        digest: object_digest(ObjectKind::TreePage, &malformed),
    };
    ObjectStore::put(
        &store,
        object,
        OwnedBytes::from(malformed),
        WorkBudget::UNBOUNDED,
    )?;
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let failure = crate::async_storage::poll_ready(read_page::<_, TreeFormat>(
        &store,
        object,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        &mut allocations,
        &mut work,
    ))
    .ok_or("shape-valid malformed page read suspended")?
    .err()
    .ok_or("shape-valid malformed tree page unexpectedly decoded")?;
    assert!(matches!(failure, Error::Decode(_)));
    assert_eq!(allocations.live_bytes(), 0);
    assert_eq!(work.backend_read_operations, 1);
    Ok(())
}

#[test]
fn page_batches_release_all_ownership_on_cancel_discard_and_early_finish()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put_tree_page(&store, b"first", 1)?;
    let second = put_tree_page(&store, b"second", 2)?;
    let limits = DecodeLimits::default();
    let cancellation = CancellationToken::new();

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut batch = crate::async_storage::poll_ready(read_pages::<_, TreeFormat, _>(
        &store,
        [first, second].into_iter(),
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    ))
    .ok_or("memory-backed batch read remained pending")??;
    assert!(allocations.live_bytes() > 0);
    assert_eq!(work.page_reads, 2);
    let page = batch.next(
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    )?;
    assert!(matches!(page.page, PageLease::Owned(Page::Leaf(_))));
    allocations.release(page.logical_bytes)?;
    cancellation.cancel();
    let failure = batch
        .next(
            limits,
            WorkBudget::UNBOUNDED,
            &cancellation,
            &mut allocations,
            &mut work,
        )
        .err()
        .ok_or("cancelled batch continued")?;
    assert!(matches!(
        failure,
        Error::Storage(ObjectStoreError::Cancelled)
    ));
    assert_eq!(allocations.live_bytes(), 0);

    let cancellation = CancellationToken::new();
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let batch = crate::async_storage::poll_ready(read_pages::<_, TreeFormat, _>(
        &store,
        [first, second].into_iter(),
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    ))
    .ok_or("second batch read remained pending")??;
    let failure = batch
        .finish(&mut allocations)
        .err()
        .ok_or("unfinished batch was accepted")?;
    assert!(matches!(
        failure,
        Error::Storage(ObjectStoreError::Rejected(_))
    ));
    assert_eq!(allocations.live_bytes(), 0);

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let batch = crate::async_storage::poll_ready(read_pages::<_, TreeFormat, _>(
        &store,
        [first, second].into_iter(),
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    ))
    .ok_or("discard batch read remained pending")??;
    batch.discard(&mut allocations)?;
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}

#[test]
fn invalid_and_precancelled_batches_do_no_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let object = put_tree_page(&store, b"entry", 4)?;
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let cancellation = CancellationToken::new();
    let empty = crate::async_storage::poll_ready(read_pages::<_, TreeFormat, _>(
        &store,
        std::iter::empty(),
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    ))
    .ok_or("empty batch remained pending")?
    .err()
    .ok_or("empty batch succeeded")?;
    assert!(matches!(
        empty,
        Error::Storage(ObjectStoreError::Rejected(_))
    ));
    assert_eq!(work, WorkCounters::default());
    assert_eq!(allocations.live_bytes(), 0);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = crate::async_storage::poll_ready(read_pages::<_, TreeFormat, _>(
        &store,
        [object].into_iter(),
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    ))
    .ok_or("cancelled batch remained pending")?
    .err()
    .ok_or("cancelled batch succeeded")?;
    assert!(matches!(
        cancelled,
        Error::Storage(ObjectStoreError::Cancelled)
    ));
    assert_eq!(work, WorkCounters::default());
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}

#[test]
fn fully_consumed_batches_finish_and_reject_reads_past_the_exact_result()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put_tree_page(&store, b"first", 1)?;
    let second = put_tree_page(&store, b"second", 2)?;
    let limits = DecodeLimits::default();
    let cancellation = CancellationToken::new();

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut batch = crate::async_storage::poll_ready(read_pages::<_, TreeFormat, _>(
        &store,
        [first, second].into_iter(),
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    ))
    .ok_or("complete batch remained pending")??;
    for _ in 0..2 {
        let page = batch.next(
            limits,
            WorkBudget::UNBOUNDED,
            &cancellation,
            &mut allocations,
            &mut work,
        )?;
        allocations.release(page.logical_bytes)?;
    }
    batch.finish(&mut allocations)?;
    assert_eq!(allocations.live_bytes(), 0);

    let mut batch = crate::async_storage::poll_ready(read_pages::<_, TreeFormat, _>(
        &store,
        [first].into_iter(),
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    ))
    .ok_or("single batch remained pending")??;
    let page = batch.next(
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    )?;
    allocations.release(page.logical_bytes)?;
    assert!(matches!(
        batch.next(
            limits,
            WorkBudget::UNBOUNDED,
            &cancellation,
            &mut allocations,
            &mut work,
        ),
        Err(Error::Storage(ObjectStoreError::Rejected(_)))
    ));
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}

#[test]
fn late_batch_decode_failure_releases_every_pending_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put_tree_page(&store, b"first", 1)?;
    let malformed = OwnedBytes::from_static(b"not a canonical tree page");
    let second = ObjectId {
        kind: ObjectKind::TreePage,
        digest: object_digest(ObjectKind::TreePage, &malformed),
    };
    ObjectStore::put(&store, second, malformed, WorkBudget::UNBOUNDED)?;
    let limits = DecodeLimits::default();
    let cancellation = CancellationToken::new();
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut batch = crate::async_storage::poll_ready(read_pages::<_, TreeFormat, _>(
        &store,
        [first, second].into_iter(),
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    ))
    .ok_or("malformed batch remained pending")??;
    let page = batch.next(
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut allocations,
        &mut work,
    )?;
    allocations.release(page.logical_bytes)?;
    assert!(matches!(
        batch.next(
            limits,
            WorkBudget::UNBOUNDED,
            &cancellation,
            &mut allocations,
            &mut work,
        ),
        Err(Error::Decode(_))
    ));
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}
