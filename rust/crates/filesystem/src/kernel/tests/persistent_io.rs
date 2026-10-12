use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::FileId;
use crate::kernel::allocation::KernelAccounting;
use crate::kernel::tree_mutation::TreeFormat;
use crate::kernel::{
    FileKind, LogicalName, NameEncoding, TreeEntry, TreePage, encode_tree_page, tree_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectKind, object_digest};
use crate::test_support::OwnedReadObjectStore;
use crate::{CachedObjectStore, ObjectCacheOptions, ObjectCacheStats};
use acyclic_stream::OwnedBytes;

async fn put_tree_page(
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
    {
        let __object_args = (
            object,
        OwnedBytes::from(encode_tree_page(&page, 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?),
        WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::put(store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?;
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

#[tokio::test]
async fn decoded_pages_are_shared_across_operations_and_mutations_copy_on_write()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::new(1024 * 1024)?;
    let page = put_tree_page(&backend, b"cached", 9).await?;
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
    let first = (read_page_with_accounting::<_, TreeFormat>(&store, page, DecodeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut first_allocations, &mut first_work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await.map(|(page, _)| page)?;
    assert!(matches!(first.page, PageLease::Shared { .. }));
    assert_eq!(first.logical_bytes, 0);
    assert_eq!(first_allocations.live_bytes(), 0);
    assert_eq!(first_work.backend_read_operations, 1);

    let mut warm_allocations = AllocationLedger::default();
    let mut warm_work = WorkCounters::default();
    let warm = (read_page_with_accounting::<_, TreeFormat>(&store, page, DecodeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut warm_allocations, &mut warm_work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await.map(|(page, _)| page)?;
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
    let mut accounting = super::super::allocation::KernelAccounting::borrowed_standalone(
        &mut mutation_allocations, &mut mutation_work, WorkBudget::UNBOUNDED, &cancellation,
    );
    let (shared, allocation) = (read_page_with_accounting::<_, TreeFormat>(
        &store, page, DecodeLimits::default(), &mut accounting, &cancellation, None,
    )).await?;
    let ((mut mutable, bytes), mut allocation) = shared.into_owned_with_accounting(allocation, &mut accounting)?;
    assert!(bytes > 0);
    if let Page::Leaf(entries) = &mut mutable {
        entries[0].name = LogicalName::new(NameEncoding::Utf8, b"changed".to_vec(), 255)?;
    }
    assert_eq!(only_page_name(&first)?, b"cached");
    drop(mutable);
    accounting.release_owned(&mut allocation, bytes)?;
    assert_eq!(mutation_work.backend_read_operations, 0);
    assert!(mutation_work.bytes_copied >= bytes);
    assert_eq!(mutation_allocations.live_bytes(), 0);

    let mut denied_allocations = AllocationLedger::default();
    let mut denied_work = WorkCounters::default();
    let mut accounting = super::super::allocation::KernelAccounting::borrowed_standalone(
        &mut denied_allocations, &mut denied_work,
        WorkBudget { peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    );
    let (shared, allocation) = (read_page_with_accounting::<_, TreeFormat>(
        &store, page, DecodeLimits::default(), &mut accounting, &cancellation, None,
    )).await?;
    let denied = shared.into_owned_with_accounting(allocation, &mut accounting)
        .err().ok_or("cached mutable page bypassed its allocation budget")?;
    assert!(matches!(denied, crate::FsError::Work(_)));
    assert_eq!(denied_allocations.live_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn decoded_cache_rejection_releases_owned_page_allocation() -> Result<(), Box<dyn std::error::Error>>
{
    let store = OwnedReadObjectStore::default();
    let page = put_tree_page(&store.inner, b"rejected", 41).await?;
    store
        .reject_decoded_admission
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let failure = (read_page_with_accounting::<_, TreeFormat>(&store, page, DecodeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &CancellationToken::new()), &CancellationToken::new(), None)).await.map(|(page, _)| page)
    .err()
    .ok_or("decoded-cache rejection unexpectedly succeeded")?;
    assert!(matches!(failure, crate::FsError::Object(ObjectStoreError::Corrupt)));
    assert_eq!(allocations.live_bytes(), 0);
    assert_eq!(work.backend_read_operations, 1);
    Ok(())
}

#[tokio::test]
async fn mixed_page_batches_read_only_cold_identities_and_preserve_order()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::new(1024 * 1024)?;
    let first = put_tree_page(&backend, b"first", 1).await?;
    let second = put_tree_page(&backend, b"second", 2).await?;
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
    let seed = (read_page_with_accounting::<_, TreeFormat>(&store, first, limits, &mut KernelAccounting::borrowed_standalone(&mut seed_allocations, &mut seed_work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await.map(|(page, _)| page)?;
    assert!(matches!(seed.page, PageLease::Shared { .. }));
    assert_eq!(seed_allocations.live_bytes(), 0);

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut batch = (read_pages_with_accounting::<_, TreeFormat, _>(&store, [first, second, first].into_iter(), limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await?;
    assert_eq!(work.page_reads, 3);
    assert_eq!(work.backend_read_operations, 1);
    let mut names = Vec::new();
    for _ in 0..3 {
        let page = batch.next_with_accounting(limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation).map(|(page, _)| page)?;
        assert!(matches!(page.page, PageLease::Shared { .. }));
        names.push(only_page_name(&page)?);
    }
    batch.finish_with_accounting(&mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation)?;
    assert_eq!(
        names,
        [b"first".to_vec(), b"second".to_vec(), b"first".to_vec()]
    );
    assert_eq!(allocations.live_bytes(), 0);

    assert_resident_cache_shape(store.stats()?, 2);

    let mut warm_allocations = AllocationLedger::default();
    let mut warm_work = WorkCounters::default();
    let mut warm = (read_pages_with_accounting::<_, TreeFormat, _>(&store, [first, second, first].into_iter(), limits, &mut KernelAccounting::borrowed_standalone(&mut warm_allocations, &mut warm_work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await?;
    assert_eq!(warm_work.backend_read_operations, 0);
    assert_eq!(warm_work.object_bytes_read, 0);
    for _ in 0..3 {
        assert!(matches!(
            warm.next_with_accounting(limits, &mut KernelAccounting::borrowed_standalone(&mut warm_allocations, &mut warm_work, WorkBudget::UNBOUNDED, &cancellation), &cancellation).map(|(page, _)| page)?
            .page,
            PageLease::Shared { .. }
        ));
    }
    warm.finish_with_accounting(&mut KernelAccounting::borrowed_standalone(&mut warm_allocations, &mut warm_work, WorkBudget::UNBOUNDED, &cancellation), &cancellation)?;
    assert_eq!(warm_allocations.live_bytes(), 0);
    assert_eq!(store.stats()?.decoded_hits, 5);
    Ok(())
}

#[tokio::test]
async fn post_read_budget_failure_releases_every_decoded_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let page = TreePage::Leaf(vec![TreeEntry {
        name: LogicalName::new(NameEncoding::Utf8, b"name".to_vec(), 255)?,
        file_id: FileId::from_bytes([7; 16]),
        kind: FileKind::Regular,
    }]);
    let object = tree_page_id(&page, 8)?;
    {
        let __object_args = (
            object,
        OwnedBytes::from(encode_tree_page(&page, 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?),
        WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::put(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?;
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_copied = 0;
    let cancellation = CancellationToken::new();
    let result = (read_page_with_accounting::<_, TreeFormat>(&store, object, DecodeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, budget, &cancellation), &cancellation, None)).await.map(|(page, _)| page);
    assert!(matches!(
        result,
        Err(crate::FsError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            ..
        }))
    ));
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn single_page_read_authenticates_decode_and_releases_backend_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let object = put_tree_page(&store, b"entry", 9).await?;
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let cancellation = CancellationToken::new();
    let owned = (read_page_with_accounting::<_, TreeFormat>(&store, object, DecodeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await.map(|(page, _)| page)?;
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
    {
        let __object_args = (
            malformed_id,
            malformed,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::put(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?;
    let mut allocations = AllocationLedger::default();
    let mut malformed_work = WorkCounters::default();
    let failure = (read_page_with_accounting::<_, TreeFormat>(&store, malformed_id, DecodeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut malformed_work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await.map(|(page, _)| page)
    .err()
    .ok_or("malformed page decoded")?;
    assert!(matches!(failure, crate::FsError::Decode(_)));
    assert_eq!(allocations.live_bytes(), 0);
    assert_eq!(malformed_work.page_reads, 1);
    assert_eq!(malformed_work.backend_read_operations, 1);
    Ok(())
}

#[tokio::test]
async fn shape_valid_malformed_tree_page_releases_decoded_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let page = TreePage::Leaf(vec![TreeEntry {
        name: LogicalName::new(NameEncoding::Utf8, b"shape".to_vec(), 255)?,
        file_id: FileId::from_bytes([43; 16]),
        kind: FileKind::Regular,
    }]);
    let mut malformed = encode_tree_page(&page, 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
    *malformed.last_mut().ok_or("encoded tree page is empty")? = u8::MAX;
    let object = ObjectId {
        kind: ObjectKind::TreePage,
        digest: object_digest(ObjectKind::TreePage, &malformed),
    };
    {
        let __object_args = (
            object,
            OwnedBytes::from(malformed),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::put(
        &store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?;
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let failure = (read_page_with_accounting::<_, TreeFormat>(&store, object, DecodeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &CancellationToken::new()), &CancellationToken::new(), None)).await.map(|(page, _)| page)
    .err()
    .ok_or("shape-valid malformed tree page unexpectedly decoded")?;
    assert!(matches!(failure, crate::FsError::Decode(_)));
    assert_eq!(allocations.live_bytes(), 0);
    assert_eq!(work.backend_read_operations, 1);
    Ok(())
}

#[tokio::test]
async fn page_batches_release_all_ownership_on_cancel_discard_and_early_finish()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put_tree_page(&store, b"first", 1).await?;
    let second = put_tree_page(&store, b"second", 2).await?;
    let limits = DecodeLimits::default();
    let cancellation = CancellationToken::new();

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut batch = (read_pages_with_accounting::<_, TreeFormat, _>(&store, [first, second].into_iter(), limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await?;
    assert!(allocations.live_bytes() > 0);
    assert_eq!(work.page_reads, 2);
    let page = batch.next_with_accounting(limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation).map(|(page, _)| page)?;
    assert!(matches!(page.page, PageLease::Owned(Page::Leaf(_))));
    allocations.release(page.logical_bytes)?;
    cancellation.cancel();
    let failure = batch.next_with_accounting(limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation).map(|(page, _)| page)
        .err()
        .ok_or("cancelled batch continued")?;
    assert!(matches!(
        failure,
        crate::FsError::Cancelled(_)
    ));
    assert_eq!(allocations.live_bytes(), 0);

    let cancellation = CancellationToken::new();
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let batch = (read_pages_with_accounting::<_, TreeFormat, _>(&store, [first, second].into_iter(), limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await?;
    let failure = batch.finish_with_accounting(&mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation)
        .err()
        .ok_or("unfinished batch was accepted")?;
    assert!(matches!(
        failure,
        crate::FsError::Object(ObjectStoreError::Rejected(_))
    ));
    assert_eq!(allocations.live_bytes(), 0);

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let batch = (read_pages_with_accounting::<_, TreeFormat, _>(&store, [first, second].into_iter(), limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await?;
    batch.discard_with_accounting(&mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation)?;
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn invalid_and_precancelled_batches_do_no_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let object = put_tree_page(&store, b"entry", 4).await?;
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let cancellation = CancellationToken::new();
    let empty = (read_pages_with_accounting::<_, TreeFormat, _>(&store, std::iter::empty(), DecodeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await
    .err()
    .ok_or("empty batch succeeded")?;
    assert!(matches!(
        empty,
        crate::FsError::Object(ObjectStoreError::Rejected(_))
    ));
    assert_eq!(work, WorkCounters::default());
    assert_eq!(allocations.live_bytes(), 0);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = (read_pages_with_accounting::<_, TreeFormat, _>(&store, [object].into_iter(), DecodeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await
    .err()
    .ok_or("cancelled batch succeeded")?;
    assert!(matches!(
        cancelled,
        crate::FsError::Cancelled(_)
    ));
    assert_eq!(work, WorkCounters::default());
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn fully_consumed_batches_finish_and_reject_reads_past_the_exact_result()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put_tree_page(&store, b"first", 1).await?;
    let second = put_tree_page(&store, b"second", 2).await?;
    let limits = DecodeLimits::default();
    let cancellation = CancellationToken::new();

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut batch = (read_pages_with_accounting::<_, TreeFormat, _>(&store, [first, second].into_iter(), limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await?;
    for _ in 0..2 {
        let page = batch.next_with_accounting(limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation).map(|(page, _)| page)?;
        allocations.release(page.logical_bytes)?;
    }
    batch.finish_with_accounting(&mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation)?;
    assert_eq!(allocations.live_bytes(), 0);

    let mut batch = (read_pages_with_accounting::<_, TreeFormat, _>(&store, [first].into_iter(), limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await?;
    let page = batch.next_with_accounting(limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation).map(|(page, _)| page)?;
    allocations.release(page.logical_bytes)?;
    assert!(matches!(
        batch.next_with_accounting(limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation).map(|(page, _)| page),
        Err(crate::FsError::Object(ObjectStoreError::Rejected(_)))
    ));
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn late_batch_decode_failure_releases_every_pending_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put_tree_page(&store, b"first", 1).await?;
    let malformed = OwnedBytes::from_static(b"not a canonical tree page");
    let second = ObjectId {
        kind: ObjectKind::TreePage,
        digest: object_digest(ObjectKind::TreePage, &malformed),
    };
    {
        let __object_args = (
            second,
            malformed,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::put(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?;
    let limits = DecodeLimits::default();
    let cancellation = CancellationToken::new();
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut batch = (read_pages_with_accounting::<_, TreeFormat, _>(&store, [first, second].into_iter(), limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation, None)).await?;
    let page = batch.next_with_accounting(limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation).map(|(page, _)| page)?;
    allocations.release(page.logical_bytes)?;
    assert!(matches!(
        batch.next_with_accounting(limits, &mut KernelAccounting::borrowed_standalone(&mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation), &cancellation).map(|(page, _)| page),
        Err(crate::FsError::Decode(_))
    ));
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}
