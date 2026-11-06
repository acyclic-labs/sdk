use super::*;

#[tokio::test]
async fn native_spill_alias_retains_actual_buffer_after_cache_and_store_drop()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, _provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    let (id, bytes) = spill_fixture(&store, &token).await?;
    let work =
        NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(error, _)| error)?;
    let read = store
        .read(
            id,
            bytes.len() as u64,
            &mut crate::AuthorityWork::native(&work, &token),
        )
        .await?;
    assert_eq!(read.bytes, bytes);
    let alias = read.bytes.clone();
    let held = work.live_allocation_bytes();
    assert!(held >= NativeMaterializationWork::accounting_state_bytes() + bytes.len() as u64);
    assert_eq!(work.receipt().object_bytes_read, bytes.len() as u64);
    let prefix = work.receipt();
    drop(
        store
            .read(
                id,
                bytes.len() as u64,
                &mut crate::AuthorityWork::native(&work, &token),
            )
            .await?,
    );
    assert_eq!(
        work.receipt().backend_read_operations,
        prefix.backend_read_operations
    );
    assert_eq!(
        work.receipt().allocation_operations,
        prefix.allocation_operations
    );
    drop(read);
    drop(store);
    assert_eq!(work.live_allocation_bytes(), held);
    assert_eq!(alias, bytes);
    drop(alias);
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn native_spill_denial_corruption_and_abandoned_frame_preserve_original_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, _provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    let (id, bytes) = spill_fixture(&store, &token).await?;
    let mut budget = native_read_budget();
    budget.backend_read_operations = 0;
    let denied = NativeMaterializationWork::new(budget, &token).map_err(|(error, _)| error)?;
    assert!(matches!(
        store
            .read(
                id,
                bytes.len() as u64,
                &mut crate::AuthorityWork::native(&denied, &token),
            )
            .await,
        Err(ObjectStoreError::Native(
            NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded {
                counter: "backend_read_operations",
                ..
            })
        ))
    ));
    assert_eq!(denied.receipt().backend_read_operations, 0);
    assert_eq!(denied.receipt().object_bytes_read, 0);
    assert_eq!(
        denied.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );

    let (resident_id, resident) = blob(OwnedBytes::from_static(b"independent resident read"));
    store
        .put(
            resident_id,
            resident.clone(),
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
        )
        .await?;
    for exit in 0..3 {
        let caller = CancellationToken::new();
        let original_token = CancellationToken::new();
        let original = NativeMaterializationWork::new(native_read_budget(), &original_token)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let before = original.receipt();
        let mut work = if exit == 0 {
            AuthorityWork::new(native_read_budget(), &caller)
        } else {
            AuthorityWork::native(&original, &caller)
        };
        let spill_file = store.spill_io.write().await;
        let mut future = Box::pin(store.read(id, bytes.len() as u64, &mut work));
        assert!(futures::poll!(future.as_mut()).is_pending());
        let mut independent = AuthorityWork::new(native_read_budget(), &token);
        let mut read = std::pin::pin!(store.read(resident_id, 1024, &mut independent));
        let std::task::Poll::Ready(result) = futures::poll!(read.as_mut()) else {
            panic!("a spill lease blocked an independent resident read");
        };
        assert_eq!(result?.bytes, resident);
        if exit == 2 {
            original_token.cancel();
        } else {
            caller.cancel();
        }
        // The original lease producer releases this exact queued read.
        drop(spill_file);
        let result = future.await;
        if exit == 2 {
            assert!(matches!(
                result,
                Err(ObjectStoreError::Native(
                    NativeTreeMaterializationError::Cancelled
                ))
            ));
        } else {
            assert!(matches!(result, Err(ObjectStoreError::Cancelled)));
        }
        let completed = if exit == 0 {
            work.work
        } else {
            original.receipt()
        };
        assert_eq!(completed.backend_read_operations, 0);
        assert_eq!(completed.object_bytes_read, 0);
        assert_eq!(completed.bytes_hashed, 0);
        assert_eq!(
            completed.allocation_operations,
            if exit == 0 {
                0
            } else {
                before.allocation_operations + 1
            }
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert!(matches!(store.lookup(id), Lookup::Spilled { .. }));
    }

    let work =
        NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(error, _)| error)?;
    let spill_file = store.spill_io.write().await;
    let mut future = Box::pin(async {
        store
            .read(
                id,
                bytes.len() as u64,
                &mut crate::AuthorityWork::native(&work, &token),
            )
            .await
    });
    assert!(
        Future::poll(
            future.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    assert!(work.live_allocation_bytes() > NativeMaterializationWork::accounting_state_bytes());
    drop(future);
    drop(spill_file);
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );

    store
        .spill_file(&mut *store.spill_io.write().await, None)
        .await?
        .write_all_batch_async(vec![OwnedWrite {
            offset: 0,
            bytes: bytes::Bytes::from_static(b"bad! spill bytes"),
        }])
        .await?;
    let work =
        NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(error, _)| error)?;
    assert!(matches!(
        store
            .read(
                id,
                bytes.len() as u64,
                &mut crate::AuthorityWork::native(&work, &token),
            )
            .await,
        Err(ObjectStoreError::Corrupt)
    ));
    assert_eq!(work.receipt().object_bytes_read, bytes.len() as u64);
    assert_eq!(work.receipt().bytes_hashed, bytes.len() as u64);
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    assert!(matches!(store.lookup(id), Lookup::Spilled { .. }));
    Ok(())
}

#[tokio::test]
async fn native_staged_decoded_alias_keeps_original_guard_after_resident_ejection()
-> Result<(), Box<dyn std::error::Error>> {
    struct Page {
        _custody: Arc<NativeAllocation>,
    }
    let directory = tempfile::tempdir()?;
    let (store, _provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    let (id, bytes) = blob(OwnedBytes::from_static(b"resident native page"));
    {
        let __object_args = (id, bytes.clone(), WorkBudget::UNBOUNDED, &token);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    let work =
        NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(error, _)| error)?;
    let initial = work.receipt().allocation_operations;
    let read = store
        .read(
            id,
            bytes.len() as u64,
            &mut crate::AuthorityWork::native(&work, &token),
        )
        .await?;
    assert_eq!(read.bytes.as_ptr(), bytes.as_ptr());
    assert_eq!(work.receipt().allocation_operations, initial);
    assert_eq!(work.receipt().backend_read_operations, 0);
    let mut allocation = work.claim(
        std::mem::size_of::<Page>() as u64,
        u64::from((std::mem::size_of::<Page>() as u64) != 0),
        crate::WorkCounters::UNCHARGED,
    )?;
    let mut control = work.claim(
        std::mem::size_of::<NativeAllocation>() as u64,
        u64::from((std::mem::size_of::<NativeAllocation>() as u64) != 0),
        crate::WorkCounters::UNCHARGED,
    )?;
    allocation.absorb(&mut control)?;
    let custody = Arc::new(allocation);
    let page = Arc::new(Page {
        _custody: Arc::clone(&custody),
    });
    let key = DecodedCacheKey::new::<Page>(id, DecodeLimits::default());
    let prefix = work.receipt();
    drop({
        let __decoded_args = (
            key,
            DecodedCacheValue {
                value: page,
                logical_bytes: std::mem::size_of::<Page>() as u64,
                native_custody: Some(custody),
            },
            &work,
        );
        __decoded_args
            .2
            .remaining()
            .map_err(crate::ObjectStoreError::from)
            .and_then(|_| {
                crate::async_storage::publish_decoded(&store, __decoded_args.0, __decoded_args.1)
            })
    }?);
    let cached = {
        let __decoded_args = (key, &work);
        __decoded_args
            .1
            .remaining()
            .map_err(crate::ObjectStoreError::from)
            .and_then(|_| crate::async_storage::cached_value(&store, __decoded_args.0))
    }?
    .ok_or("resident native decoded miss")?;
    assert_eq!(
        work.receipt().allocation_operations,
        prefix.allocation_operations
    );
    let alias = Arc::clone(&cached.value);
    let held = work.live_allocation_bytes();
    drop(cached);
    store.index_mut().remove(id);
    assert_eq!(work.live_allocation_bytes(), held);
    drop(alias);
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn native_staged_batch_preserves_two_actual_scalar_spill_reads_and_header_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, _provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    let (first, first_bytes) = spill_fixture(&store, &token).await?;
    let (second, second_bytes) = blob(OwnedBytes::from_static(b"second spill bytes"));
    {
        let __object_args = (second, second_bytes.clone(), WorkBudget::UNBOUNDED, &token);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    {
        let mut spill_file = store.spill_io.write().await;
        let offset = store.index().spill_end;
        let spill_bytes = second_bytes.clone();
        store
            .spill_file(&mut spill_file, None)
            .await?
            .control_async(
                move |file, _| {
                    acyclic_native_runtime::write_all_at(file, offset, spill_bytes.as_slice(), None)
                },
                None,
            )
            .expect("unmetered control construction cannot fail")
            .await?;
        let mut index = store.index_mut();
        if let Some(Staged::Resident { order, .. }) = index.objects.get(&second) {
            let order = *order;
            index.order.remove(&order);
        }
        index.mark_spilled(&[(second, second_bytes.clone())], offset);
    }
    // Unrelated staged objects must not turn a durable provider batch into
    // scalar reads. Exercise both representations on the actual local medium.
    let durable = [
        blob(OwnedBytes::from_static(b"first durable batch member")),
        blob(OwnedBytes::from_static(b"second durable batch member")),
    ];
    for (id, bytes) in &durable {
        store
            .inner()
            .put(
                *id,
                bytes.clone(),
                &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
            )
            .await?;
    }
    let durable_requests = durable.each_ref().map(|(id, bytes)| ObjectReadRequest {
        object_id: *id,
        maximum_bytes: bytes.len() as u64,
    });
    for native in [false, true] {
        for stored in [false, true] {
            let original = NativeMaterializationWork::new(native_read_budget(), &token)
                .map_err(|(cause, _)| cause)?;
            let baseline = original.live_allocation_bytes();
            let mut work = if native {
                AuthorityWork::native(&original, &token)
            } else {
                AuthorityWork::new(native_read_budget(), &token)
            };
            let (values, allocation) = if stored {
                store.read_many_stored(&durable_requests, &mut work).await?
            } else {
                store.read_many(&durable_requests, &mut work).await?
            };
            assert_eq!(values.len(), durable.len());
            for (read, (_, bytes)) in values.iter().zip(&durable) {
                assert_eq!(&read.bytes, bytes);
            }
            assert_eq!(work.spent().object_probes, 2);
            if !native {
                assert_eq!(
                    work.spent().backend_read_operations,
                    1,
                    "durable reads must preserve the provider batch"
                );
            }
            let alias = values[0].bytes.clone();
            drop(values);
            drop(allocation);
            if native {
                assert!(original.live_allocation_bytes() > baseline);
            }
            drop(alias);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
    }
    // Interleaving resident members cannot split the original durable miss
    // batch. Duplicate misses retain their positions and their original aliases.
    let resident = blob(OwnedBytes::from_static(b"mixed resident member"));
    store
        .put(
            resident.0,
            resident.1.clone(),
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
        )
        .await?;
    let mixed = [
        durable[0].clone(),
        resident.clone(),
        durable[1].clone(),
        durable[0].clone(),
    ];
    let mixed_requests = mixed.each_ref().map(|(id, bytes)| ObjectReadRequest {
        object_id: *id,
        maximum_bytes: bytes.len() as u64,
    });
    let mut batch_preserved = true;
    let mut concurrent_admission = true;
    for native in [false, true] {
        for stored in [false, true] {
            let original = NativeMaterializationWork::new(native_read_budget(), &token)
                .map_err(|(cause, _)| cause)?;
            let baseline = original.live_allocation_bytes();
            let mut work = if native {
                AuthorityWork::native(&original, &token)
            } else {
                AuthorityWork::new(native_read_budget(), &token)
            };
            let (values, allocation) = if native {
                let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::seeded(50);
                let mut read = Box::pin(schedule.scope(async {
                    if stored {
                        store.read_many_stored(&mixed_requests, &mut work).await
                    } else {
                        store.read_many(&mixed_requests, &mut work).await
                    }
                }));
                let first_poll = futures::poll!(read.as_mut());
                // All native submission barriers are still held. Counting
                // accepted jobs here cannot mistake a later scalar for overlap.
                let admitted = schedule.pending_native_work();
                let driver = tokio::spawn(async move {
                    while let Some(boundary) = events.recv().await {
                        boundary.release();
                    }
                });
                let result = match first_poll {
                    std::task::Poll::Ready(result) => result,
                    std::task::Poll::Pending => read.await,
                };
                schedule.drained().await;
                driver.abort();
                let _ = driver.await;
                concurrent_admission &= admitted >= 2;
                result?
            } else if stored {
                store.read_many_stored(&mixed_requests, &mut work).await?
            } else {
                store.read_many(&mixed_requests, &mut work).await?
            };
            assert_eq!(values.len(), mixed.len());
            for (read, (_, expected)) in values.iter().zip(&mixed) {
                assert_eq!(&read.bytes, expected, "mixed batch changed input order");
            }
            assert_eq!(work.spent().object_probes, 3);
            if !native {
                batch_preserved &= work.spent().backend_read_operations == 1;
            }
            let alias = values[2].bytes.clone();
            drop(values);
            drop(allocation);
            if native {
                assert!(original.live_allocation_bytes() > baseline);
            }
            drop(alias);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
    }
    assert_eq!(
        (batch_preserved, concurrent_admission),
        (true, true),
        "mixed reads must preserve the provider batch and admit independent native misses before first release"
    );
    for native in [false, true] {
        for cancelled in [false, true] {
            let caller = CancellationToken::new();
            let mut budget = native_read_budget();
            if !cancelled {
                budget.items_returned = 0;
            }
            let original =
                NativeMaterializationWork::new(budget, &caller).map_err(|(cause, _)| cause)?;
            let baseline = original.live_allocation_bytes();
            let mut work = if native {
                AuthorityWork::native(&original, &caller)
            } else {
                AuthorityWork::new(budget, &caller)
            };
            if cancelled {
                caller.cancel();
            }
            let result = store.read_many(&mixed_requests, &mut work).await;
            if cancelled {
                assert!(matches!(
                    result,
                    Err(ObjectStoreError::Cancelled)
                        | Err(ObjectStoreError::Native(
                            NativeTreeMaterializationError::Cancelled
                        ))
                ));
            } else {
                assert!(matches!(
                    result,
                    Err(ObjectStoreError::Work(crate::WorkError::BudgetExceeded {
                        counter: "items_returned",
                        ..
                    })) | Err(ObjectStoreError::Native(
                        NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded {
                            counter: "items_returned",
                            ..
                        })
                    ))
                ));
            }
            assert_eq!(work.spent().object_probes, 0);
            assert_eq!(work.spent().backend_read_operations, 0);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
    }
    // No provider result owner exists for a wholly resident batch.
    let original =
        NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let resident_request = ObjectReadRequest {
        object_id: resident.0,
        maximum_bytes: resident.1.len() as u64,
    };
    let (values, allocation) = store
        .read_many(
            &[resident_request, resident_request],
            &mut AuthorityWork::native(&original, &token),
        )
        .await?;
    assert!(values.iter().all(|read| read.bytes == resident.1));
    drop(values);
    drop(allocation);
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    let work =
        NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(error, _)| error)?;
    let requests = [
        durable_requests[0],
        ObjectReadRequest {
            object_id: first,
            maximum_bytes: first_bytes.len() as u64,
        },
        ObjectReadRequest {
            object_id: second,
            maximum_bytes: second_bytes.len() as u64,
        },
        durable_requests[1],
    ];
    let batch = store
        .read_many(&requests, &mut crate::AuthorityWork::native(&work, &token))
        .await
        .map(|(values, allocation)| (values, allocation.expect("native batch allocation")))?;
    assert_eq!(batch.0.len(), 4);
    assert_eq!(batch.0[0].bytes, durable[0].1);
    assert_eq!(batch.0[1].bytes, first_bytes);
    assert_eq!(batch.0[2].bytes, second_bytes);
    assert_eq!(batch.0[3].bytes, durable[1].1);
    assert_eq!(
        work.receipt().object_bytes_read,
        (first_bytes.len() + second_bytes.len() + durable[0].1.len() + durable[1].1.len()) as u64
    );
    let alias = batch.0[1].bytes.clone();
    let (values, allocation) = batch;
    assert_eq!(
        allocation.bytes(),
        (values.capacity() * std::mem::size_of::<ObjectRead>()) as u64
    );
    drop(values);
    drop(allocation);
    drop(store);
    assert!(work.live_allocation_bytes() > NativeMaterializationWork::accounting_state_bytes());
    drop(alias);
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}
