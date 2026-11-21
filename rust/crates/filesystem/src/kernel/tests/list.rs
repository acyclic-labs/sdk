use super::*;
use crate::foundation::Digest;
use crate::kernel::TreePage;
use crate::memory::MemoryObjectStore;
use acyclic_stream::OwnedBytes;

use crate::kernel::Child;

use crate::test_support::put_tree_page as put;
use crate::test_support::tree_entries::{entry as tree_entry, name};

#[path = "../../../tests/support/listing_tree.rs"]
mod fixture;
use fixture::{entry, tree};

#[tokio::test]
async fn custody_listing_retains_only_known_sdk_prefix_when_provider_panics()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncObjectStore;
    use crate::storage::ObjectReadRequest;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct PanickingRead {
        inner: MemoryObjectStore,
        reached: AtomicUsize,
    }
    impl AsyncObjectStore for PanickingRead {
        async fn put(
            &self,
            id: ObjectId,
            bytes: OwnedBytes,
            context: &mut crate::AuthorityWork<'_>,
        ) -> Result<(), crate::ObjectStoreError> {
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
            let budget = context.work.remaining(context.budget)?;
            let cancellation = context.cancellation;
            let result = {
                let mut work = crate::AuthorityWork::new(budget, cancellation);
                let result = AsyncObjectStore::put(&self.inner, id, bytes, &mut work).await;
                work.receipt(result)
            };
            context.accept(result)
        }
        async fn read(
            &self,
            _id: ObjectId,
            _maximum: u64,
            context: &mut crate::AuthorityWork<'_>,
        ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
            let _budget = context.read_budget()?;
            let _cancellation = context.cancellation;
            let result = async {
                self.reached.fetch_add(1, Ordering::SeqCst);
                panic!("provider unwinds without returning its private work");
            }
            .await;
            context.accept(result)
        }
        async fn read_many(
            &self,
            requests: &[ObjectReadRequest],
            context: &mut crate::AuthorityWork<'_>,
        ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError>
        {
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
            let budget = context.work.remaining(context.budget)?;
            let cancellation = context.cancellation;
            let result = {
                let mut work = crate::AuthorityWork::new(budget, cancellation);
                let result = AsyncObjectStore::read_many(&self.inner, requests, &mut work)
                    .await
                    .map(|(values, _allocation)| values);
                work.receipt(result)
            };
            context.accept(result).map(|values| (values, None))
        }
        async fn contains(
            &self,
            id: ObjectId,
            context: &mut crate::AuthorityWork<'_>,
        ) -> Result<bool, crate::ObjectStoreError> {
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
            let budget = context.work.remaining(context.budget)?;
            let cancellation = context.cancellation;
            let result = {
                let mut work = crate::AuthorityWork::new(budget, cancellation);
                let result = AsyncObjectStore::contains(&self.inner, id, &mut work).await;
                work.receipt(result)
            };
            context.accept(result)
        }
    }
    let inner = MemoryObjectStore::default();
    let root = tree(&inner).await?;
    let store = PanickingRead {
        inner,
        reached: AtomicUsize::new(0),
    };
    let cancellation = CancellationToken::new();
    let owner = crate::kernel::ProofDataCustody::default();
    let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(async {
        let mut accounting =
            crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        persistent_pagination::paginate_with_accounting::<TreeFormat>(
            &store,
            (
                root,
                Bound::Unbounded,
                2,
                acyclic_collections::page::Direction::Ascending,
                false,
            ),
            DecodeLimits::default(),
            &mut accounting,
            &cancellation,
            Some(&owner),
        )
        .await
    }))
    .await;
    assert!(result.is_err());
    assert_eq!(store.reached.load(Ordering::SeqCst), 1);
    let known = owner.take_completed_data_work();
    assert_eq!(known.page_reads, 1);
    assert_eq!(
        known.allocation_operations, 4,
        "owned output, visited/frontier buffers, and actual read-future frame"
    );
    assert_eq!(
        known.backend_read_operations, 0,
        "no provider receipt was returned"
    );
    assert_eq!(known.object_bytes_read, 0);
    assert_eq!(owner.operation_cache_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn reverse_pages_seek_only_the_selected_frontier() -> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::KernelAccounting;
    use acyclic_collections::page::Direction;
    let cancel = CancellationToken::new();
    let store = MemoryObjectStore::default();
    let root = tree(&store).await?;
    // Every cut crosses original child boundaries, including exclusive first keys.
    for direction in [Direction::Ascending, Direction::Descending] {
        for cursor in [
            None,
            Some("0"),
            Some("a"),
            Some("b"),
            Some("c"),
            Some("cc"),
            Some("e"),
            Some("f"),
            Some("z"),
        ] {
            let cursor = cursor.map(name).transpose()?;
            for maximum in 1..=4 {
                for bound in [
                    cursor.as_ref().map_or(Bound::Unbounded, Bound::Excluded),
                    cursor.as_ref().map_or(Bound::Unbounded, Bound::Included),
                ] {
                    let mut accounting =
                        KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
                    let (page, _) = persistent_pagination::paginate_with_accounting::<TreeFormat>(
                        &store,
                        (root, bound, maximum, direction, true),
                        DecodeLimits::default(),
                        &mut accounting,
                        &cancel,
                        None,
                    )
                    .await?;
                    let mut expected: Vec<_> = ["a", "b", "c", "d", "e", "f"]
                        .into_iter()
                        .filter(|value| match (direction, bound) {
                            (_, Bound::Unbounded) => true,
                            (Direction::Ascending, Bound::Excluded(cursor)) => {
                                value.as_bytes() > cursor.as_bytes()
                            }
                            (Direction::Ascending, Bound::Included(cursor)) => {
                                value.as_bytes() >= cursor.as_bytes()
                            }
                            (Direction::Descending, Bound::Excluded(cursor)) => {
                                value.as_bytes() < cursor.as_bytes()
                            }
                            (Direction::Descending, Bound::Included(cursor)) => {
                                value.as_bytes() <= cursor.as_bytes()
                            }
                        })
                        .collect();
                    if direction == Direction::Descending {
                        expected.reverse();
                    }
                    assert_eq!(
                        page.values
                            .iter()
                            .map(|value| value.name.as_bytes())
                            .collect::<Vec<_>>(),
                        expected
                            .iter()
                            .take(maximum as usize)
                            .map(|value| value.as_bytes())
                            .collect::<Vec<_>>()
                    );
                    assert_eq!(page.has_more, expected.len() > maximum as usize);
                    assert_eq!(page.position.is_some(), page.has_more);
                    if let Some(position) = &page.position {
                        let resolved =
                            crate::kernel::key_at_position_with_accounting::<TreeFormat>(
                                &store,
                                root,
                                position,
                                DecodeLimits::default(),
                                &mut accounting,
                                &cancel,
                                None,
                            )
                            .await?;
                        assert_eq!(
                            Some(&resolved.value),
                            page.values.last().map(|entry| &entry.name)
                        );
                        resolved.retire(&mut accounting)?;
                        let mut forged = position.clone();
                        forged.offset = u32::MAX;
                        assert!(
                            crate::kernel::key_at_position_with_accounting::<TreeFormat>(
                                &store,
                                root,
                                &forged,
                                DecodeLimits::default(),
                                &mut accounting,
                                &cancel,
                                None,
                            )
                            .await
                            .is_err()
                        );
                        forged.root.digest = Digest::from_bytes([91; 32]);
                        assert!(
                            crate::kernel::key_at_position_with_accounting::<TreeFormat>(
                                &store,
                                root,
                                &forged,
                                DecodeLimits::default(),
                                &mut accounting,
                                &cancel,
                                None,
                            )
                            .await
                            .is_err()
                        );
                    }
                }
            }
        }
    }
    // A selected child must obey the actual adjacent separator, including when
    // its successor is outside the bounded selected frontier.
    let bad = put(
        &store,
        &TreePage::Leaf(vec![entry("a", 1)?, entry("z", 2)?]),
        8,
    )
    .await?;
    let good = put(&store, &TreePage::Leaf(vec![entry("c", 3)?]), 8).await?;
    let forged = put(
        &store,
        &TreePage::Internal(vec![
            Child {
                first: name("a")?,
                page: bad,
            },
            Child {
                first: name("c")?,
                page: good,
            },
        ]),
        8,
    )
    .await?;
    let before = name("c")?;
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
    let failure = persistent_pagination::paginate_with_accounting::<TreeFormat>(
        &store,
        (
            forged,
            Bound::Excluded(&before),
            1,
            Direction::Descending,
            false,
        ),
        DecodeLimits::default(),
        &mut accounting,
        &cancel,
        None,
    )
    .await
    .err()
    .ok_or("forged upper child boundary was accepted")?;
    assert!(matches!(
        failure,
        crate::FsError::Directory(DirectoryReadError::ChildBoundsMismatch)
    ));
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("ordinary ledger absent")?
            .0
            .scratch_bytes(),
        0
    );

    let mut costs = Vec::new();
    for population in [1_u32, 100_000] {
        let store = MemoryObjectStore::default();
        let mut level = Vec::new();
        for start in (0..population).step_by(32) {
            let values = (start..(start + 32).min(population))
                .map(|index| entry(&format!("a{index:06}"), 1))
                .collect::<Result<Vec<_>, _>>()?;
            let first = values[0].name.clone();
            let page = put(&store, &TreePage::Leaf(values), 32).await?;
            level.push(Child { first, page });
        }
        while level.len() > 1 {
            let mut next = Vec::new();
            let mut children = level.into_iter();
            while children.len() != 0 {
                let batch: Vec<_> = children.by_ref().take(32).collect();
                let first = batch[0].first.clone();
                let page = put(&store, &TreePage::Internal(batch), 32).await?;
                next.push(Child { first, page });
            }
            level = next;
        }
        let foreign = level.pop().ok_or("foreign subtree absent")?;
        let selected = put(
            &store,
            &TreePage::Leaf(vec![
                entry("z000", 2)?,
                entry("z001", 3)?,
                entry("z002", 4)?,
            ]),
            32,
        )
        .await?;
        let root = put(
            &store,
            &TreePage::Internal(vec![
                foreign,
                Child {
                    first: name("z000")?,
                    page: selected,
                },
            ]),
            32,
        )
        .await?;
        // Provider point selection also contributes examined items. Its fixed
        // full-reference width bounds that work independently of stored history.
        let key_bound =
            acyclic_collections::FixedIndex::<(), (), { ObjectId::ENCODED_LEN }>::insertion_bound(
                1,
            )
            .ok_or("reference selection bound overflow")?
            .examined_items;
        let mut accounting = KernelAccounting::standalone(
            WorkBudget {
                page_reads: 2,
                items_examined: 2 * key_bound + 64,
                ..WorkBudget::UNBOUNDED
            },
            &cancel,
        );
        let (page, _) = persistent_pagination::paginate_with_accounting::<TreeFormat>(
            &store,
            (root, Bound::Unbounded, 1, Direction::Descending, false),
            DecodeLimits::default(),
            &mut accounting,
            &cancel,
            None,
        )
        .await?;
        assert_eq!(page.values[0].name.as_bytes(), b"z002");
        assert!(page.has_more);
        assert_eq!(page.work.page_reads, 2);
        costs.push((
            page.work.page_reads,
            page.work.items_examined,
            page.work.object_bytes_read,
        ));
        // Restore the historical enumeration with the same examined-name credit.
        // It must refuse instead of silently scanning to the last name.
        let mut accounting = KernelAccounting::standalone(
            WorkBudget {
                items_examined: page.work.items_examined,
                ..WorkBudget::UNBOUNDED
            },
            &cancel,
        );
        let failure = persistent_pagination::paginate_with_accounting::<TreeFormat>(
            &store,
            (
                root,
                Bound::Unbounded,
                population + 3,
                Direction::Ascending,
                false,
            ),
            DecodeLimits::default(),
            &mut accounting,
            &cancel,
            None,
        )
        .await
        .err()
        .ok_or("historical enumeration fit selected-name credit")?;
        assert!(matches!(
            failure,
            crate::FsError::Directory(DirectoryReadError::Work(WorkError::BudgetExceeded {
                counter: crate::WorkCounter::ItemsExamined,
                ..
            }))
        ));
        assert_eq!(
            accounting
                .standalone_parts()
                .ok_or("ordinary ledger absent")?
                .0
                .scratch_bytes(),
            0
        );
    }
    assert_eq!(costs[0].0, costs[1].0);
    assert_eq!(costs[0].2, costs[1].2);
    eprintln!("reverse selected pages/examined/read bytes at 1 and 100000 earlier names={costs:?}");
    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::root(
            WorkBudget::UNBOUNDED,
            &cancel,
            &crate::services::ServicesInitialized::for_kernel_tests(),
        )
        .map_err(|(error, _)| error)?;
        let meter = std::sync::Arc::downgrade(&original.state);
        let baseline = original.live_allocation_bytes().unwrap();
        assert_eq!(
            baseline,
            crate::NativeMaterializationWork::accounting_state_bytes()
        );
        let mut accounting = KernelAccounting::native(&original, &cancel);
        let (page, allocation) = persistent_pagination::paginate_with_accounting::<TreeFormat>(
            &store,
            (root, Bound::Unbounded, 2, Direction::Descending, false),
            DecodeLimits::default(),
            &mut accounting,
            &cancel,
            None,
        )
        .await?;
        assert_eq!(page.values[0].name.as_bytes(), b"f");
        assert!(original.live_allocation_bytes().unwrap() > baseline);
        drop(page);
        drop(allocation);
        assert_eq!(original.live_allocation_bytes().unwrap(), baseline);
        drop(accounting);
        drop(original);
        assert!(meter.upgrade().is_none());
    }
    Ok(())
}
