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
        let wake = Arc::new(CancellationWake::default());
        let waker = std::task::Waker::from(Arc::clone(&wake));
        assert!(
            future
                .as_mut()
                .poll(&mut std::task::Context::from_waker(&waker))
                .is_pending()
        );
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
        assert!(
            wake.0.load(Ordering::Relaxed),
            "cancellation must wake the read"
        );
        let std::task::Poll::Ready(result) = futures::poll!(future.as_mut()) else {
            panic!("a cancelled read still waits for the spill lease");
        };
        drop(future);
        drop(spill_file);
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
                // The waiter and its original standalone ledger control.
                2
            } else {
                // The suspended frame and both distinct cancellation sources.
                before.allocation_operations + 3
            },
            "every pending lease waiter must be charged to its original work"
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert!(matches!(store.lookup(id), Lookup::Spilled { .. }));
    }

    let durable = [
        blob(OwnedBytes::from_static(b"completed before private wait")),
        blob(OwnedBytes::from_static(b"retained after private wait")),
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
    let requests = [
        ObjectReadRequest {
            object_id: durable[0].0,
            maximum_bytes: durable[0].1.len() as u64,
        },
        ObjectReadRequest {
            object_id: id,
            maximum_bytes: bytes.len() as u64,
        },
        ObjectReadRequest {
            object_id: durable[1].0,
            maximum_bytes: durable[1].1.len() as u64,
        },
    ];
    // Measure the real provider's retained batch and the same private wait's
    // scratch separately. The mixed wait must retain both, including the
    // provider vector after its first result has moved into the output.
    let private_live = {
        let caller = CancellationToken::new();
        let work = NativeMaterializationWork::new(native_read_budget(), &token)
            .map_err(|(error, _)| error)?;
        let baseline = work.live_allocation_bytes();
        let private_requests = [requests[1]; 3];
        let spill_file = store.spill_io.write().await;
        let mut context = AuthorityWork::native(&work, &caller);
        let mut read = Box::pin(store.read_many(&private_requests, &mut context));
        assert!(futures::poll!(read.as_mut()).is_pending());
        let live = work.live_allocation_bytes() - baseline;
        drop(read);
        drop(spill_file);
        assert_eq!(work.live_allocation_bytes(), baseline);
        live
    };
    let provider_live = {
        let work = NativeMaterializationWork::new(native_read_budget(), &token)
            .map_err(|(error, _)| error)?;
        let baseline = work.live_allocation_bytes();
        let (values, allocation) = store
            .inner()
            .read_many(
                &[requests[0], requests[2]],
                &mut AuthorityWork::native(&work, &token),
            )
            .await?;
        assert_eq!(values[0].bytes, durable[0].1);
        assert_eq!(values[1].bytes, durable[1].1);
        assert_eq!(
            allocation
                .as_ref()
                .expect("native provider allocation")
                .bytes(),
            (values.capacity() * std::mem::size_of::<ObjectRead>()) as u64
        );
        let live = work.live_allocation_bytes() - baseline;
        drop(values);
        drop(allocation);
        assert_eq!(work.live_allocation_bytes(), baseline);
        live
    };
    for (batch, mixed, exit) in [
        (false, false, 0),
        (true, false, 0),
        (true, true, 0),
        (true, true, 1),
        (true, true, 2),
    ] {
        let caller = CancellationToken::new();
        let original_token = CancellationToken::new();
        let work = NativeMaterializationWork::new(native_read_budget(), &original_token)
            .map_err(|(error, _)| error)?;
        let spill_file = store.spill_io.write().await;
        let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
        let driver = tokio::spawn(async move {
            while let Some(boundary) = events.recv().await {
                boundary.release();
            }
        });
        let mut future = Box::pin(schedule.scope(async {
            let mut work = AuthorityWork::native(&work, &caller);
            if batch {
                let requests = if mixed {
                    requests.as_slice()
                } else {
                    &requests[1..2]
                };
                store.read_many(requests, &mut work).await.map(drop)
            } else {
                store
                    .read(id, bytes.len() as u64, &mut work)
                    .await
                    .map(drop)
            }
        }));
        assert!(futures::poll!(future.as_mut()).is_pending());
        // Complete the original durable jobs, then let their caller retain the
        // actual results while the private member waits on this spill lease.
        tokio::select! {
            _ = schedule.drained() => {}
            _ = future.as_mut() => panic!("read ended before the spill lease was released"),
        }
        let wake = Arc::new(CancellationWake::default());
        let waker = std::task::Waker::from(Arc::clone(&wake));
        assert!(
            future
                .as_mut()
                .poll(&mut std::task::Context::from_waker(&waker))
                .is_pending()
        );
        let prefix = work.receipt();
        assert!(
            future
                .as_mut()
                .poll(&mut std::task::Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(
            work.receipt(),
            prefix,
            "a pending lease must reuse its waiters"
        );
        assert_eq!(prefix.object_probes, if mixed { 2 } else { 0 });
        if !mixed {
            assert_eq!(prefix.backend_read_operations, 0);
        }
        assert_eq!(
            prefix.object_bytes_read,
            if mixed {
                (durable[0].1.len() + durable[1].1.len()) as u64
            } else {
                0
            }
        );
        assert!(work.live_allocation_bytes() > NativeMaterializationWork::accounting_state_bytes());
        if mixed {
            assert_eq!(
                work.live_allocation_bytes(),
                NativeMaterializationWork::accounting_state_bytes() + private_live + provider_live,
                "the pending mixed batch must retain the original provider backing"
            );
        }
        if exit == 0 {
            drop(future);
            drop(spill_file);
        } else {
            if exit == 1 {
                caller.cancel();
            } else {
                original_token.cancel();
            }
            assert!(
                wake.0.load(Ordering::Relaxed),
                "cancellation must wake the batch"
            );
            let std::task::Poll::Ready(result) = futures::poll!(future.as_mut()) else {
                panic!("a cancelled mixed batch still waits for the spill lease");
            };
            drop(future);
            drop(spill_file);
            if exit == 1 {
                assert!(matches!(result, Err(ObjectStoreError::Cancelled)));
            } else {
                assert!(matches!(
                    result,
                    Err(ObjectStoreError::Native(
                        NativeTreeMaterializationError::Cancelled
                    ))
                ));
            }
        }
        schedule.drained().await;
        driver.abort();
        let _ = driver.await;
        let mut retired = work.receipt();
        // Only the two original waiters' bounded cancellation/unlink work
        // follows this prefix; no I/O, copy or allocation may follow it.
        assert!(retired.items_examined - prefix.items_examined <= 3);
        retired.items_examined = prefix.items_examined;
        assert_eq!(retired, prefix);
        assert_eq!(
            work.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes()
        );
    }

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
    let (second, second_bytes) = spill_bytes_fixture(
        &store,
        &token,
        OwnedBytes::from_static(b"second spill bytes"),
    )
    .await?;
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
    let resident = blob(OwnedBytes::from_static(b"resident between durable misses"));
    store
        .put(
            resident.0,
            resident.1.clone(),
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
        )
        .await?;
    let mixed_requests = [
        durable_requests[0],
        ObjectReadRequest {
            object_id: resident.0,
            maximum_bytes: resident.1.len() as u64,
        },
        durable_requests[1],
    ];
    let durable_values = durable.each_ref().map(|(_, bytes)| bytes);
    let mixed_values = [&durable[0].1, &resident.1, &durable[1].1];
    for native in [false, true] {
        for stored in [false, true] {
            let (draining, draining_bytes) = spill_bytes_fixture(
                &store,
                &token,
                OwnedBytes::from(format!("draining spill {native} {stored}")),
            )
            .await?;
            let draining_requests = [
                durable_requests[0],
                ObjectReadRequest {
                    object_id: draining,
                    maximum_bytes: draining_bytes.len() as u64,
                },
                mixed_requests[1],
                durable_requests[1],
            ];
            let draining_values = [&durable[0].1, &draining_bytes, &resident.1, &durable[1].1];
            for (requests, expected) in [
                (durable_requests.as_slice(), durable_values.as_slice()),
                (mixed_requests.as_slice(), mixed_values.as_slice()),
                (draining_requests.as_slice(), draining_values.as_slice()),
            ] {
                let budget = WorkBudget {
                    items_returned: requests.len() as u64 + u64::from(native),
                    ..native_read_budget()
                };
                let original =
                    NativeMaterializationWork::new(budget, &token).map_err(|(cause, _)| cause)?;
                let baseline = original.live_allocation_bytes();
                let mut work = if native {
                    AuthorityWork::native(&original, &token)
                } else {
                    AuthorityWork::new(budget, &token)
                };
                let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
                let mut batch = Box::pin(schedule.scope(async {
                    if stored {
                        store.read_many_stored(requests, &mut work).await
                    } else {
                        store.read_many(requests, &mut work).await
                    }
                }));
                assert!(futures::poll!(batch.as_mut()).is_pending());
                let observers = schedule
                    .trace()
                    .iter()
                    .filter(|phase| phase.as_str() == "task:dispatch")
                    .count();
                let mut held = Vec::new();
                // Persistence submits its original completion through an owned
                // observer. Let those observers reach native admission while
                // retaining every physical execution boundary.
                while schedule.pending_native_work() < observers {
                    tokio::select! {
                        boundary = events.recv() => {
                            let boundary = boundary.ok_or("native admission producer lost")?;
                            if boundary.name == "native:submitted" {
                                held.push(boundary);
                            } else {
                                boundary.release();
                            }
                        }
                        _ = batch.as_mut() => panic!("read ended before native admission"),
                    }
                }
                // Both original LocalMedium persistence jobs must be accepted
                // before the driver releases any native execution boundary.
                let admitted = schedule.pending_native_work();
                if native {
                    // A separate physical read shares the original ledger
                    // while this batch waits. Its returned item cannot cover
                    // any of this batch's private members.
                    drop(
                        store
                            .inner()
                            .read_many(
                                &durable_requests[..1],
                                &mut AuthorityWork::native(&original, &token),
                            )
                            .await?,
                    );
                }
                if requests.len() == draining_requests.len() {
                    // The read has classified this identity as private. Drain it
                    // through the real publication path while its independent
                    // durable reads are still held before physical execution.
                    assert!(matches!(store.lookup(draining), Lookup::Spilled { .. }));
                    let closure = [draining];
                    let mut publication =
                        AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &token);
                    let hold = store
                        .flush_before_publish(
                            PublicationScope {
                                objects: &closure,
                                proven_at: store.collection.sweeps(),
                            },
                            &mut publication,
                        )
                        .await?;
                    assert!(matches!(store.lookup(draining), Lookup::Absent));
                    drop(hold);
                }
                for boundary in held {
                    boundary.release();
                }
                let driver = tokio::spawn(async move {
                    while let Some(boundary) = events.recv().await {
                        boundary.release();
                    }
                });
                let result = batch.await;
                schedule.drained().await;
                driver.abort();
                let _ = driver.await;
                assert_eq!(admitted, 2, "durable misses must overlap before release");
                let (values, allocation) = result?;
                assert_eq!(values.len(), expected.len());
                assert_eq!(
                    work.spent().items_returned,
                    requests.len() as u64 + u64::from(native)
                );
                for (read, bytes) in values.iter().zip(expected) {
                    assert_eq!(&read.bytes, *bytes);
                }
                let drained = u64::from(requests.len() == draining_requests.len());
                assert_eq!(work.spent().object_probes, 2 + drained + u64::from(native));
                if !native {
                    assert_eq!(
                        work.spent().backend_read_operations,
                        1 + drained,
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
    }
    // The real provider still has both durable identities in its index, but
    // its original pinned log can no longer supply their bytes. A private
    // member must not make that completed failure wait on its spill lease.
    std::fs::OpenOptions::new()
        .write(true)
        .open(directory.path().join("objects").join("log"))?
        .set_len(0)?;
    for native in [false, true] {
        for stored in [false, true] {
            let original = NativeMaterializationWork::new(native_read_budget(), &token)
                .map_err(|(error, _)| error)?;
            let baseline = original.live_allocation_bytes();
            let mut work = if native {
                AuthorityWork::native(&original, &token)
            } else {
                AuthorityWork::new(native_read_budget(), &token)
            };
            let spill_file = store.spill_io.write().await;
            assert!(matches!(store.lookup(first), Lookup::Spilled { .. }));
            let failed_requests = [
                durable_requests[0],
                ObjectReadRequest {
                    object_id: first,
                    maximum_bytes: first_bytes.len() as u64,
                },
                durable_requests[1],
            ];
            let result = if stored {
                store.read_many_stored(&failed_requests, &mut work).await
            } else {
                store.read_many(&failed_requests, &mut work).await
            };
            let Err(ObjectStoreError::LogicalObjects(error)) = result else {
                panic!("the original durable read must report its unavailable log");
            };
            assert_eq!(error.code, acyclic_objects::wire::ErrorCode::Unavailable);
            assert_eq!(work.spent().object_probes, 2);
            assert_eq!(work.spent().object_bytes_read, 0);
            assert_eq!(work.spent().bytes_hashed, 0);
            assert_eq!(work.spent().items_returned, 0);
            drop(error);
            assert_eq!(work.allocations.live_bytes(), 0);
            assert_eq!(original.live_allocation_bytes(), baseline);
            drop(spill_file);
        }
    }
    let work =
        NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(error, _)| error)?;
    let requests = [
        ObjectReadRequest {
            object_id: first,
            maximum_bytes: first_bytes.len() as u64,
        },
        ObjectReadRequest {
            object_id: second,
            maximum_bytes: second_bytes.len() as u64,
        },
    ];
    let batch = store
        .read_many(&requests, &mut crate::AuthorityWork::native(&work, &token))
        .await
        .map(|(values, allocation)| (values, allocation.expect("native batch allocation")))?;
    assert_eq!(batch.0.len(), 2);
    assert_eq!(batch.0[0].bytes, first_bytes);
    assert_eq!(batch.0[1].bytes, second_bytes);
    assert_eq!(
        work.receipt().object_bytes_read,
        (first_bytes.len() + second_bytes.len()) as u64
    );
    let alias = batch.0[0].bytes.clone();
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
