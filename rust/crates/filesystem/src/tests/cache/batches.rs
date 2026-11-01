use super::*;

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
