use super::*;

#[tokio::test]
async fn cached_read_withdrawal_retains_original_admission_before_provider_completion()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = Arc::new(MemoryObjectStore::default());
    let setup = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let (id, bytes) = object(b"held cached read");
    inner
        .put(id, bytes.clone(), &mut AuthorityWork::new(budget, &setup))
        .await?;
    for batch in [false, true] {
        for finish in 0..4 {
            let (producer, receiver) = futures::channel::oneshot::channel();
            let reads = Arc::new(AtomicUsize::new(0));
            let cached = CachedObjectStore::new(
                GatedStore {
                    inner: Arc::clone(&inner),
                    gate: futures::FutureExt::shared(receiver),
                    reads: Arc::clone(&reads),
                    reads_before_gate: 0,
                    complete_before_gate: false,
                },
                options(4, 1024),
            )?;
            let cancellation = CancellationToken::new();
            let mut work = AuthorityWork::new(budget, &cancellation);
            work.accounting().charge_items(7)?;
            let mut earlier = work.vec_owned::<u8>(31)?;
            earlier.value.resize(31, 7);
            let mut output = crate::kernel::allocation::retained::Retained::allocate(
                17,
                &mut work.accounting(),
            )?;
            output.value.resize(17, 9_u8);
            let before = work.spent();
            let live = work.allocations.live_bytes();
            if finish == 3 {
                work.budget.items_examined = before.items_examined;
            }
            let requests = [ObjectReadRequest {
                object_id: id,
                maximum_bytes: bytes.len() as u64,
            }];
            let mut read = Box::pin(async {
                if batch {
                    cached
                        .read_many(&requests, &mut work)
                        .await
                        .map(|(values, allocation)| {
                            let retained = (values.capacity() * size_of::<ObjectRead>()) as u64;
                            drop(values);
                            drop(allocation);
                            retained
                        })
                } else {
                    cached
                        .read(id, bytes.len() as u64, &mut work)
                        .await
                        .map(|_| 0)
                }
            });
            if finish == 3 {
                assert!(matches!(
                    futures::poll!(read.as_mut()),
                    Poll::Ready(Err(ObjectStoreError::Work(
                        crate::WorkError::BudgetExceeded {
                            counter: "items_examined",
                            ..
                        }
                    )))
                ));
                drop(read);
                drop(producer);
                assert_eq!(reads.load(Ordering::Acquire), 0);
                assert_eq!(work.spent(), before);
                assert_eq!(work.allocations.live_bytes(), live);
                continue;
            }
            assert!(futures::poll!(read.as_mut()).is_pending());
            assert_eq!(reads.load(Ordering::Acquire), 1);
            let mut independent = AuthorityWork::new(budget, &setup);
            assert!(inner.contains(id, &mut independent).await?);
            if finish == 1 {
                drop(producer);
                assert!(matches!(
                    read.await,
                    Err(ObjectStoreError::Io(error))
                        if error.kind() == std::io::ErrorKind::BrokenPipe
                ));
            } else if finish == 0 {
                cancellation.cancel();
                drop(read);
                drop(producer);
            } else {
                producer.send(()).map_err(|_| "read producer was dropped")?;
                let retained = read.await?;
                assert_eq!(work.allocations.live_bytes(), live + retained);
                work.allocations.release_owned(retained)?;
            }
            assert!(work.spent().items_examined > before.items_examined);
            assert!(work.spent().allocation_operations > before.allocation_operations);
            assert_eq!(cached.stats()?.in_flight, 0);
            assert_eq!(work.allocations.live_bytes(), live);
            assert_eq!(earlier.value, [7; 31]);
            assert_eq!(output.value, [9; 17]);
            drop(output);
            assert_eq!(work.allocations.live_bytes(), live - 17);
            earlier.retire(&mut work.accounting())?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn cached_reads_keep_only_returned_owned_storage_on_the_original_ledger()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let (id, bytes) = object(b"owned cache read");
    for maximum_bytes in [1, 1024] {
        for batch in [false, true] {
            let inner =
                crate::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory()));
            inner
                .put(
                    id,
                    bytes.clone(),
                    &mut AuthorityWork::new(budget, &cancellation),
                )
                .await?;
            let cached = CachedObjectStore::new(inner, options(4, maximum_bytes))?;
            let mut work = AuthorityWork::new(budget, &cancellation);
            let earlier = work.vec_owned::<u8>(31)?;
            let mut output = crate::kernel::allocation::retained::Retained::allocate(
                17,
                &mut work.accounting(),
            )?;
            output.value.resize(17, 9_u8);
            let baseline = work.allocations.live_bytes();
            let retained = if batch {
                let requests = [ObjectReadRequest {
                    object_id: id,
                    maximum_bytes: bytes.len() as u64,
                }; 2];
                let (values, allocation) = cached.read_many(&requests, &mut work).await?;
                assert_eq!(values.len(), 2);
                assert_eq!(values[0].bytes, bytes);
                assert_eq!(values[1].bytes, bytes);
                let retained = (values.capacity() * size_of::<ObjectRead>()) as u64;
                assert_eq!(work.allocations.live_bytes(), baseline + retained);
                drop(values);
                drop(allocation);
                retained
            } else {
                let read = cached.read(id, bytes.len() as u64, &mut work).await?;
                assert_eq!(read.bytes, bytes);
                let retained = if maximum_bytes == 1 {
                    assert_eq!(
                        read.retention,
                        ObjectReadRetention::Owned {
                            logical_bytes: bytes.len() as u64
                        }
                    );
                    bytes.len() as u64
                } else {
                    assert_eq!(read.retention, ObjectReadRetention::Shared);
                    0
                };
                assert_eq!(work.allocations.live_bytes(), baseline + retained);
                drop(read);
                retained
            };
            work.allocations.release_owned(retained)?;
            assert_eq!(work.allocations.live_bytes(), baseline);
            assert_eq!(cached.stats()?.in_flight, 0);
            drop(output);
            earlier.retire(&mut work.accounting())?;
            work.spent().verify(budget)?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn sequential_batch_retains_original_prefix_on_withdrawal_and_producer_loss()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = Arc::new(MemoryObjectStore::default());
    let setup = CancellationToken::new();
    let objects = [object(b"first"), object(b"second")];
    for (id, bytes) in &objects {
        object_work!(inner, put(*id, bytes.clone()), B, &setup).await?;
    }
    let requests = objects.map(|(object_id, bytes)| ObjectReadRequest {
        object_id,
        maximum_bytes: bytes.len() as u64,
    });
    let mut prefixes = [WorkCounters::UNCHARGED; 2];
    for (request, prefix) in requests.iter().zip(&mut prefixes) {
        let mut work = AuthorityWork::new(B, &setup);
        drop(
            inner
                .read(request.object_id, request.maximum_bytes, &mut work)
                .await?,
        );
        *prefix = work.spent();
    }
    for (complete_before_gate, exit, stored) in [false, true]
        .into_iter()
        .flat_map(|timing| (0..6).map(move |exit| (timing, exit)))
        .flat_map(|(timing, exit)| [false, true].map(|stored| (timing, exit, stored)))
    {
        let completed_reads = u64::from(exit >= 4);
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(
            WorkBudget {
                allocation_operations: 2,
                peak_allocation_bytes: 31 + (2 * size_of::<ObjectRead>()) as u64,
                ..B
            },
            &cancellation,
        );
        work.charge(WorkCounters {
            items_examined: 7,
            ..WorkCounters::UNCHARGED
        })?;
        let mut earlier = crate::kernel::Owned::<Vec<u8>>::allocate(31, &mut work.accounting())?;
        earlier.value.extend([7; 31]);
        let before = work.spent();
        let live = work.allocations.live_bytes();
        let (producer, receiver) = futures::channel::oneshot::channel();
        let reads = Arc::new(AtomicUsize::new(0));
        let store = GatedStore {
            reads_before_gate: completed_reads as usize,
            complete_before_gate,
            inner: Arc::clone(&inner),
            gate: futures::FutureExt::shared(receiver),
            reads: Arc::clone(&reads),
        };
        let mut batch = Box::pin(async {
            if stored {
                store.read_many_stored(&requests, &mut work).await
            } else {
                store.read_many(&requests, &mut work).await
            }
        });
        assert!(futures::poll!(batch.as_mut()).is_pending());
        assert_eq!(reads.load(Ordering::Acquire), 1 + completed_reads as usize);
        let mut sibling = AuthorityWork::new(B, &setup);
        assert!(inner.contains(requests[0].object_id, &mut sibling).await?);
        match exit {
            0 => {
                producer
                    .send(())
                    .map_err(|_| "original read receiver closed")?;
                let (values, allocation) = batch.await?;
                assert_eq!(values[0].bytes.as_ref(), b"first");
                assert_eq!(values[1].bytes.as_ref(), b"second");
                let retained = (values.capacity() * size_of::<ObjectRead>()) as u64
                    + values
                        .iter()
                        .map(|read| read.retention.owned_bytes())
                        .sum::<u64>();
                assert_eq!(work.allocations.live_bytes(), live + retained);
                assert!(allocation.is_none());
                drop(values);
                drop(allocation);
                work.allocations.release_owned(retained)?;
            }
            1 => {
                drop(producer);
                assert!(matches!(
                    batch.await,
                    Err(ObjectStoreError::Io(error))
                        if error.kind() == std::io::ErrorKind::BrokenPipe
                ));
            }
            _ => {
                if exit == 3 || exit == 5 {
                    cancellation.cancel();
                }
                drop(batch);
                drop(producer);
            }
        }
        let completed = work.spent();
        // The old outer child frame loses even this admitted prefix on drop.
        assert_eq!(
            completed.allocation_operations,
            before.allocation_operations + 1
        );
        assert!(completed.peak_allocation_bytes >= live + (2 * size_of::<ObjectRead>()) as u64);
        if exit == 0 {
            assert_eq!(reads.load(Ordering::Acquire), 2);
            assert_eq!(completed.backend_read_operations, 2);
            assert_eq!(completed.items_returned, 2);
        } else {
            assert_eq!(reads.load(Ordering::Acquire), 1 + completed_reads as usize);
            // The current read has performed real provider work before its
            // delivery gate. Withdrawal must preserve that work too.
            let performed = (completed_reads + u64::from(complete_before_gate)) as usize;
            let prefix = &prefixes[..performed];
            assert_eq!(
                completed.items_examined,
                before.items_examined
                    + 2
                    + prefix.iter().map(|work| work.items_examined).sum::<u64>()
            );
            assert_eq!(completed.backend_read_operations, performed as u64);
            assert_eq!(
                completed.object_bytes_read,
                prefix
                    .iter()
                    .map(|work| work.object_bytes_read)
                    .sum::<u64>()
            );
            assert_eq!(
                completed.bytes_hashed,
                prefix.iter().map(|work| work.bytes_hashed).sum::<u64>()
            );
            assert_eq!(completed.items_returned, 0);
        }
        assert_eq!(earlier.value, [7; 31]);
        assert_eq!(work.allocations.live_bytes(), live);
        earlier.retire(&mut work.accounting())?;
        assert_eq!(work.allocations.live_bytes(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn malformed_backend_batch_cardinality_releases_all_flights()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = Arc::new(MemoryObjectStore::default());
    let cancellation = CancellationToken::new();
    let objects = [object(b"first-short"), object(b"second-short")];
    for (object_id, bytes) in &objects {
        (object_work!(inner, put(*object_id, bytes.clone()), B, &cancellation)).await?;
    }
    let cached = CachedObjectStore::new(ShortBatchStore { inner }, options(4, 64))?;
    let requests = objects.map(|(object_id, bytes)| ObjectReadRequest {
        object_id,
        maximum_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    });
    let failure = (object_work!(cached, read_many(&requests), B, &cancellation))
        .await
        .err()
        .ok_or("short backend batch was accepted")?;
    assert!(matches!(failure.error, ObjectStoreError::Corrupt));
    assert_eq!(cached.stats()?.in_flight, 0);
    Ok(())
}

#[tokio::test]
async fn mixed_batches_preserve_backend_multiget_order_duplicates_and_exact_limits()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    verify_batch_leader_admission()?;
    let budget = crate::performance::BOUNDARY_BUDGET;
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let objects = [object(b"warm"), object(b"second"), object(b"third")];
    for (object_id, bytes) in &objects {
        (object_work!(inner, put(*object_id, bytes.clone()), budget, &cancellation)).await?;
    }
    let (counting, scalar_reads, batch_reads) = counting_store(inner);
    let cached = CachedObjectStore::new(counting, options(8, 128))?;
    object_work!(cached, read(objects[0].0, 4), budget, &cancellation).await?;
    let requests = [
        ObjectReadRequest {
            object_id: objects[0].0,
            maximum_bytes: 4,
        },
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 6,
        },
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 6,
        },
        ObjectReadRequest {
            object_id: objects[2].0,
            maximum_bytes: 5,
        },
    ];
    let receipt = object_work!(cached, read_many(&requests), budget, &cancellation).await?;
    assert_eq!(
        receipt
            .value
            .iter()
            .map(|value| value.bytes.as_ref())
            .collect::<Vec<_>>(),
        vec![b"warm".as_slice(), b"second", b"second", b"third"]
    );

    assert_eq!(scalar_reads.load(Ordering::Acquire), 1);
    assert_eq!(batch_reads.load(Ordering::Acquire), 1);

    let unequal = [
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 1,
        },
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 6,
        },
    ];
    let failure = (object_work!(cached, read_many(&unequal), budget, &cancellation))
        .await
        .err()
        .ok_or("per-request byte limit was ignored")?;
    assert!(matches!(
        failure.error,
        ObjectStoreError::TooLarge {
            observed: 6,
            maximum: 1
        }
    ));
    assert_eq!(batch_reads.load(Ordering::Acquire), 1);

    // Cold duplicates merge their provider bound while retaining each caller's
    // output limit and original position, even when the first limit is smaller.
    cached.clear()?;
    let failure = (object_work!(cached, read_many(&unequal), budget, &cancellation))
        .await
        .err()
        .ok_or("cold duplicate limit was ignored")?;
    assert!(matches!(
        failure.error,
        ObjectStoreError::TooLarge {
            observed: 6,
            maximum: 1
        }
    ));
    assert_eq!(batch_reads.load(Ordering::Acquire), 2);
    assert_eq!(cached.stats()?.in_flight, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn verify_batch_leader_admission() -> Result<(), Box<dyn std::error::Error>> {
    use crate::NativeMaterializationWork;
    use crate::kernel::KernelAccounting;
    use crate::kernel::allocation::retained::RetainedAllocation;
    use std::sync::atomic::AtomicU64;

    // Count the old ordered mechanism's actual comparisons independently from
    // the original admission ledger that counts the shared radix's node visits.
    #[derive(Eq, PartialEq)]
    struct Ordered([u8; ObjectId::ENCODED_LEN]);
    static COMPARES: AtomicU64 = AtomicU64::new(0);
    impl Ord for Ordered {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            COMPARES.fetch_add(1, Ordering::Relaxed);
            self.0.cmp(&other.0)
        }
    }
    impl PartialOrd for Ordered {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    let mut visits = Vec::new();
    let mut comparisons = Vec::new();
    let own = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: crate::Digest::from_bytes([255; 32]),
        storage: None,
    };
    for population in [1_u64, 100_000] {
        let bound = acyclic_collections::FixedIndex::<
            usize,
            RetainedAllocation,
            { ObjectId::ENCODED_LEN },
        >::insertion_bound(population + 3)
        .ok_or("batch index bound overflow")?;
        let budget = WorkBudget {
            items_examined: bound.examined_items,
            allocation_operations: 2 * population + 16,
            peak_allocation_bytes: bound.allocation_bytes + 65_536,
            ..crate::performance::BOUNDARY_BUDGET
        };
        let cancel = CancellationToken::new();
        let original =
            NativeMaterializationWork::new(budget, &cancel).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancel);
        let mut leaders = reads::BatchLeaders::default();
        let mut old = BTreeMap::new();
        // Actual request construction is distinct from the fixed-key probe.
        for ordinal in (0..population).rev() {
            let mut digest = [0; 32];
            digest[24..].copy_from_slice(&ordinal.to_be_bytes());
            let key = ObjectId {
                digest: crate::Digest::from_bytes(digest),
                ..own
            }
            .index_key();
            leaders.get_or_try_insert_with(key, &mut accounting, |_| Ok(ordinal as usize))?;
            old.insert(Ordered(key), ordinal as usize);
        }
        leaders.get_or_try_insert_with(own.index_key(), &mut accounting, |_| Ok(7))?;
        old.insert(Ordered(own.index_key()), 7);
        let before = accounting.spent();
        assert_eq!(leaders.get(own.index_key(), &mut accounting)?, Some(&7));
        visits.push(accounting.spent().items_examined - before.items_examined);
        COMPARES.store(0, Ordering::Relaxed);
        assert_eq!(old.get(&Ordered(own.index_key())), Some(&7));
        comparisons.push(COMPARES.load(Ordering::Relaxed));
        let allocated = accounting.spent().allocation_operations;
        assert_eq!(
            *leaders.get_or_try_insert_with(own.index_key(), &mut accounting, |_| {
                panic!("duplicate batch leader must retain its original position")
            })?,
            7
        );
        assert_eq!(accounting.spent().allocation_operations, allocated);
        let sealed = ObjectId {
            storage: Some(crate::ObjectStorage {
                locator: own.digest,
                root_file_id: crate::FileId::from_bytes([0; 16]),
                origin_generation: own.digest,
            }),
            ..own
        };
        leaders.get_or_try_insert_with(sealed.index_key(), &mut accounting, |_| Ok(9))?;
        assert_eq!(leaders.get(sealed.index_key(), &mut accounting)?, Some(&9));
        assert_eq!(leaders.get(own.index_key(), &mut accounting)?, Some(&7));
        cancel.cancel();
        assert!(leaders.get(own.index_key(), &mut accounting).is_err());
        drop(leaders);
        assert_eq!(original.live_allocation_bytes(), baseline);
        original.receipt().verify(budget)?;
    }
    assert_eq!(visits[0], visits[1]);
    assert!(visits[0] <= ObjectId::ENCODED_LEN as u64 + 2);
    assert!(comparisons[1] > comparisons[0]);
    println!("batch leader visits={visits:?}; original ordered comparisons={comparisons:?}");

    // The last construction claim belongs to the local leader leaf. Refusing
    // that original claim must release the already installed flight and vectors.
    let cancel = CancellationToken::new();
    let request = ObjectReadRequest {
        object_id: own,
        maximum_bytes: 1,
    };
    let cached = CachedObjectStore::new(MemoryObjectStore::default(), options(4, 64))?;
    let original = NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let prepared = cached.prepare_batch(
        &[request, request],
        &mut AuthorityWork::native(&original, &cancel),
    )?;
    let operations = original.receipt().allocation_operations;
    assert_eq!(prepared.leaders.value.len(), 1);
    drop(prepared);
    assert_eq!(cached.stats()?.in_flight, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    let denied_budget = WorkBudget {
        allocation_operations: operations - 1,
        ..crate::performance::BOUNDARY_BUDGET
    };
    let denied =
        NativeMaterializationWork::new(denied_budget, &cancel).map_err(|(error, _)| error)?;
    let baseline = denied.live_allocation_bytes();
    assert!(
        cached
            .prepare_batch(
                &[request, request],
                &mut AuthorityWork::native(&denied, &cancel)
            )
            .is_err()
    );
    assert_eq!(cached.stats()?.in_flight, 0);
    assert_eq!(denied.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn fs_composition_consumes_the_cache_without_a_second_runtime_or_adapter()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"embedded-fs");
    (object_work!(inner, put(object_id, bytes.clone()), B, &cancellation)).await?;
    let (counting, scalar_reads, _) = counting_store(inner);
    let cached = CachedObjectStore::new(counting, options(8, 128))?;
    let fs = crate::Fs::new(
        crate::MemoryAuthorityStore::default(),
        cached,
        crate::EmbeddedCapabilities::MEMORY,
    );
    let first = fs.export_object(object_id, 11, B, &cancellation).await?;
    let warm = fs.export_object(object_id, 11, B, &cancellation).await?;
    assert_eq!(first.value.bytes, bytes);
    assert_eq!(warm.value.bytes, bytes);

    assert_eq!(scalar_reads.load(Ordering::Acquire), 1);

    let (imported_id, imported_bytes) = object(b"facade-import");
    let imported = fs
        .import_object(imported_id, imported_bytes.clone(), B, &cancellation)
        .await?;
    assert_eq!(imported.value, ());

    let exported = fs.export_object(imported_id, 13, B, &cancellation).await?;
    assert_eq!(exported.value.bytes, imported_bytes);

    Ok(())
}
