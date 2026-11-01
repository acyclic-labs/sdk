use super::*;
use crate::test_support::PausedStore;
use std::future::{Future, poll_fn};
use std::sync::{Arc, Mutex};
use std::task::Poll;

const B: WorkBudget = WorkBudget::UNBOUNDED;

#[derive(Clone, Copy)]
enum Interruption {
    OwnClear,
    PeerClear,
    ProducerLoss,
}

fn options() -> ObjectCacheOptions {
    ObjectCacheOptions {
        maximum_entries: 8,
        maximum_bytes: 1_048_576,
    }
}

#[tokio::test]
async fn decoded_misses_keep_the_original_epoch_through_scalar_and_batch_io()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::KernelAccounting;
    use crate::kernel::persistent_btree::Page;
    use crate::kernel::persistent_io::{
        PageLease, read_page_with_accounting, read_pages_with_accounting,
    };
    use crate::kernel::persistent_point::Owned;
    use crate::kernel::tree_mutation::TreeFormat;
    use crate::kernel::{FileKind, LogicalName, NameEncoding, TreeEntry, TreePage};
    crate::initialize_test_native_io();
    for native in [false, true] {
        for batch in [false, true] {
            for interruption in [
                Interruption::OwnClear,
                Interruption::PeerClear,
                Interruption::ProducerLoss,
            ] {
                let inner = Arc::new(MemoryObjectStore::default());
                let mut ids = Vec::new();
                for marker in [1, 2] {
                    ids.push(
                        crate::test_support::put_tree_page(
                            &*inner,
                            &TreePage::Leaf(vec![TreeEntry {
                                name: LogicalName::new(
                                    NameEncoding::Utf8,
                                    vec![b'a' + marker],
                                    255,
                                )?,
                                file_id: crate::FileId::from_bytes([marker; 16]),
                                kind: FileKind::Regular,
                            }]),
                            8,
                        )
                        .await?,
                    );
                }
                let (release, receiver) = tokio::sync::oneshot::channel();
                let cache = CachedObjectStore::new(
                    PausedStore {
                        inner,
                        release: Mutex::new(Some(receiver)),
                    },
                    options(),
                )?;
                let peer = cache.share_cache_with(())?;
                let cancel = CancellationToken::new();
                let original = crate::NativeMaterializationWork::new(B, &cancel)
                    .map_err(|(error, _)| error)?;
                let baseline = original.live_allocation_bytes();
                let mut accounting = if native {
                    KernelAccounting::native(&original, &cancel)
                } else {
                    KernelAccounting::standalone(B, &cancel)
                };
                let limits = DecodeLimits::default();
                let pending = async {
                    let mut values = Vec::new();
                    if batch {
                        let mut pages = read_pages_with_accounting::<_, TreeFormat, _>(
                            &cache,
                            ids.iter().copied(),
                            limits,
                            &mut accounting,
                            &cancel,
                            None,
                        )
                        .await?;
                        for _ in &ids {
                            values.push(pages.next_with_accounting(
                                limits,
                                &mut accounting,
                                &cancel,
                            )?);
                        }
                        pages.finish_with_accounting(&mut accounting, &cancel)?;
                    } else {
                        values.push(
                            read_page_with_accounting::<_, TreeFormat>(
                                &cache,
                                ids[0],
                                limits,
                                &mut accounting,
                                &cancel,
                                None,
                            )
                            .await?,
                        );
                    }
                    Ok::<_, crate::FsError>(values)
                };
                let mut pending = Box::pin(pending);
                poll_fn(|cx| {
                    assert!(pending.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                match interruption {
                    Interruption::OwnClear => {
                        cache.clear()?;
                        release.send(()).map_err(|_| "read receiver lost")?;
                    }
                    Interruption::PeerClear => {
                        peer.clear()?;
                        release.send(()).map_err(|_| "read receiver lost")?;
                    }
                    Interruption::ProducerLoss => drop(release),
                }
                let result = pending.await;
                if matches!(interruption, Interruption::ProducerLoss) {
                    assert!(
                        matches!(result, Err(crate::FsError::Object(ObjectStoreError::Rejected(ref why))) if why == "cache fill producer lost")
                    );
                } else {
                    let values = result?;
                    let count = if batch { 2 } else { 1 };
                    assert_eq!(values.len(), count);
                    for (index, (page, allocation)) in values.into_iter().enumerate() {
                        let tree = match &page.page {
                            PageLease::Owned(page) => page,
                            PageLease::Shared { page, .. } => &**page,
                        };
                        let Page::Leaf(entries) = tree else {
                            return Err("expected original leaf".into());
                        };
                        assert_eq!(
                            entries[0].file_id,
                            crate::FileId::from_bytes([index as u8 + 1; 16])
                        );
                        let bytes = page.logical_bytes;
                        if native {
                            assert!(original.live_allocation_bytes() > baseline);
                        }
                        Owned::new(page, allocation, bytes).retire(&mut accounting)?;
                    }
                    assert_eq!(
                        cache.stats()?.resident_decoded_pages,
                        if matches!(interruption, Interruption::PeerClear) {
                            count as u64
                        } else {
                            0
                        }
                    );
                    assert_eq!(accounting.spent().backend_read_operations, 1);
                }
                assert_eq!(cache.stats()?.in_flight, 0);
                if let Some((allocations, _, _, _)) = accounting.standalone_parts() {
                    assert_eq!(allocations.live_bytes(), 0);
                }
                cache.clear()?;
                drop(accounting);
                assert_eq!(original.live_allocation_bytes(), baseline);
                // A fresh miss can populate the new epoch, using the same
                // decoder and immutable source rather than replaying a prefix.
                let mut fresh = KernelAccounting::standalone(B, &cancel);
                let (page, allocation) = read_page_with_accounting::<_, TreeFormat>(
                    &cache, ids[0], limits, &mut fresh, &cancel, None,
                )
                .await?;
                let bytes = page.logical_bytes;
                Owned::new(page, allocation, bytes).retire(&mut fresh)?;
                assert_eq!(cache.stats()?.resident_decoded_pages, 1);
            }
        }
    }
    Ok(())
}
