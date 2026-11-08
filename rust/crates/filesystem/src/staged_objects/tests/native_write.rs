use super::*;

fn native_write_budget() -> WorkBudget {
    WorkBudget {
        backend_read_operations: 128,
        backend_write_operations: 128,
        durability_operations: 128,
        object_probes: 256,
        object_bytes_read: 64 * 1_048_576,
        object_bytes_written: 64 * 1_048_576,
        source_bytes_read: 64 * 1_048_576,
        output_bytes: 64 * 1_048_576,
        bytes_hashed: 128 * 1_048_576,
        bytes_copied: 128 * 1_048_576,
        bytes_encoded: crate::performance::BOUNDARY_BUDGET.bytes_encoded,
        items_examined: 8_192,
        items_returned: 256,
        allocation_operations: 1_024,
        peak_allocation_bytes: 64 * 1_048_576,
        ..WorkBudget::default()
    }
}

#[tokio::test]
async fn native_flush_caller_cancellation_refuses_before_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, _provider) = open_staged(directory.path()).await?;
    let original_token = CancellationToken::new();
    let caller = CancellationToken::new();
    let original = NativeMaterializationWork::new(native_write_budget(), &original_token)
        .map_err(|(error, _)| error)?;
    let (id, bytes) = blob(OwnedBytes::from_static(
        b"cancelled publication stays staged",
    ));
    store
        .put(
            id,
            bytes.clone(),
            &mut AuthorityWork::native(&original, &original_token),
        )
        .await?;
    let before = original.receipt();
    let live = original.live_allocation_bytes();
    caller.cancel();
    assert!(matches!(
        store
            .flush_before_publish(
                PublicationScope {
                    objects: &[id],
                    proven_at: store.collection.sweeps()
                },
                &mut AuthorityWork::native(&original, &caller),
            )
            .await,
        Err(ObjectStoreError::Cancelled)
    ));
    assert_eq!(original.receipt(), before);
    assert_eq!(original.live_allocation_bytes(), live);
    assert!(store.index().objects.contains_key(&id));
    let collecting = store.collection.begin().await;
    let (sweep, _) = collecting.sweepable(Vec::new()).await;
    for (publication, exit, hold_spill) in [
        (false, 0, true),
        (false, 1, true),
        (false, 2, true),
        (true, 0, true),
        (true, 1, true),
        (true, 2, true),
        (true, 0, false),
        (true, 1, false),
        (true, 2, false),
    ] {
        let caller = CancellationToken::new();
        let original_token = CancellationToken::new();
        let original = NativeMaterializationWork::new(native_write_budget(), &original_token)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let before = original.receipt();
        let mut work = if exit == 0 {
            AuthorityWork::new(native_write_budget(), &caller)
        } else {
            AuthorityWork::native(&original, &caller)
        };
        let closure = [id];
        let spill = if hold_spill {
            Some(store.spill_io.write().await)
        } else {
            None
        };
        let mut publishing = Box::pin(async {
            if publication {
                store
                    .flush_before_publish(
                        PublicationScope {
                            objects: &closure,
                            proven_at: store.collection.sweeps(),
                        },
                        &mut work,
                    )
                    .await
                    .map(drop)
            } else {
                store.relieve_window(&mut work).await
            }
        });
        let wake = Arc::new(CancellationWake::default());
        let waker = std::task::Waker::from(Arc::clone(&wake));
        assert!(
            publishing
                .as_mut()
                .poll(&mut std::task::Context::from_waker(&waker))
                .is_pending()
        );
        if !hold_spill {
            let mut admission = std::pin::pin!(store.collection.gate_waits.notified());
            assert!(
                futures::poll!(admission.as_mut()).is_ready(),
                "publication must reach the original collection gate"
            );
        }
        let mut independent = AuthorityWork::new(native_write_budget(), &original_token);
        let mut read = std::pin::pin!(store.read(id, 1024, &mut independent));
        let std::task::Poll::Ready(result) = futures::poll!(read.as_mut()) else {
            panic!("publication leases blocked an independent resident read");
        };
        assert_eq!(result?.bytes, bytes);
        if exit == 2 {
            original_token.cancel();
        } else {
            caller.cancel();
        }
        assert!(
            wake.0.load(Ordering::Relaxed),
            "cancellation must wake the writer"
        );
        // The original blocking lease stays held through cancellation.
        let std::task::Poll::Ready(result) = futures::poll!(publishing.as_mut()) else {
            panic!("a cancelled writer still waits for its original lease");
        };
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
        drop(publishing);
        drop(spill);
        let completed = work.spent();
        assert_eq!(completed.backend_write_operations, 0);
        assert_eq!(completed.durability_operations, 0);
        assert_eq!(completed.object_bytes_written, 0);
        assert!(
            completed.items_examined <= 5,
            "only bounded waiter cleanup is required"
        );
        assert_eq!(
            completed.allocation_operations,
            if exit == 0 {
                2
            } else {
                before.allocation_operations + 2 + u64::from(publication)
            }
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert!(matches!(store.lookup(id), Lookup::Resident(_)));
    }
    drop(sweep);
    drop(collecting);
    drop(
        store
            .flush_before_publish(
                PublicationScope {
                    objects: &[id],
                    proven_at: store.collection.sweeps(),
                },
                &mut AuthorityWork::native(&original, &original_token),
            )
            .await?,
    );
    assert!(store.index().objects.is_empty());
    assert_eq!(
        store
            .inner()
            .read(
                id,
                bytes.len() as u64,
                &mut AuthorityWork::native(&original, &original_token)
            )
            .await?
            .bytes,
        bytes
    );
    Ok(())
}

#[tokio::test]
async fn native_flush_exact_closure_preserves_intrinsic_alias_without_readoption()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let token = CancellationToken::new();
    let work = NativeMaterializationWork::new(native_write_budget(), &token)
        .map_err(|(error, _)| error)?;
    let (buffer, mut body) = staged_vector::<u8>(32, Some(&work))?;
    body.extend_from_slice(b"original physical byte owner");
    let bytes = staged_owned_bytes(body, buffer.ok_or("unfunded test buffer")?, &work)?;
    let object = HashedObject::new(ObjectKind::BlobChunk, bytes.clone());
    let object_id = object.object_id();
    let store = StagedObjects::open(
        crate::MemoryObjectStore::default(),
        directory.path().to_path_buf(),
    );
    let before = work.receipt();
    store
        .put_hashed(object, &mut crate::AuthorityWork::native(&work, &token))
        .await?;
    assert_eq!(
        work.receipt().allocation_operations,
        before.allocation_operations
    );
    let alias = match store.lookup(object_id) {
        Lookup::Resident(bytes) => bytes,
        _ => return Err("native admission did not remain resident".into()),
    };
    assert_eq!(alias.as_ptr(), bytes.as_ptr());
    let (unselected, obsolete) = blob(OwnedBytes::from_static(b"unpublished superseded object"));
    store
        .put(
            unselected,
            obsolete,
            &mut crate::AuthorityWork::native(&work, &token),
        )
        .await?;
    let closure = [object_id];
    let hold = store
        .flush_before_publish(
            PublicationScope {
                objects: &closure,
                proven_at: store.collection.sweeps(),
            },
            &mut crate::AuthorityWork::native(&work, &token),
        )
        .await?;
    assert!(!store.index().objects.contains_key(&object_id));
    assert!(store.index().objects.contains_key(&unselected));
    assert!(
        store
            .inner()
            .contains(
                object_id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &token)
            )
            .await?
    );
    assert!(
        !store
            .inner()
            .contains(
                unselected,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &token)
            )
            .await?
    );
    assert_eq!(work.receipt().backend_write_operations, 1);
    drop(hold);
    drop(bytes);
    drop(store);
    assert!(work.live_allocation_bytes() > NativeMaterializationWork::accounting_state_bytes());
    drop(alias);
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn native_spilled_flush_cleans_acknowledged_objects_and_reuses_spill()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    assert!(store.spill_io.read().await.is_none());
    let mut closure = Vec::new();
    for marker in 0..3_u8 {
        let object = HashedObject::new(ObjectKind::BlobChunk, vec![marker; 2 * 1_048_576].into());
        closure.push(object.object_id());
        store
            .put_hashed(
                object,
                &mut AuthorityWork::new(native_write_budget(), &token),
            )
            .await?;
    }
    // Admission crosses the original resident window and writes its real
    // spill. Publication must authenticate those physical bytes once while
    // carrying the remaining resident seal without another hash.
    assert_eq!(store.index().spilled_objects, 2);
    assert!(store.spill_io.read().await.is_some());
    let spilled_bytes = store.index().spill_end;
    for truncated in [false, true] {
        {
            let spill = store.spill_io.write().await;
            let file = spill.as_ref().ok_or("admission did not retain its spill")?;
            if truncated {
                file.set_len_async(spilled_bytes - 1, None)
                    .expect("unmetered truncation construction cannot fail")
                    .await?;
            } else {
                file.write_all_batch_async(vec![OwnedWrite {
                    offset: 0,
                    bytes: bytes::Bytes::from_static(&[255]),
                }])
                .await?;
            }
        }
        let failed = NativeMaterializationWork::new(native_write_budget(), &token)
            .map_err(|(error, _)| error)?;
        assert!(matches!(
            store
                .flush_before_publish(
                    PublicationScope {
                        objects: &closure,
                        proven_at: store.collection.sweeps(),
                    },
                    &mut AuthorityWork::native(&failed, &token),
                )
                .await,
            Err(ObjectStoreError::Corrupt)
        ));
        let verified_prefix = 2 * 1_048_576;
        assert_eq!(
            failed.receipt().object_bytes_read,
            if truncated {
                spilled_bytes - 1
            } else {
                verified_prefix
            }
        );
        assert_eq!(failed.receipt().bytes_hashed, verified_prefix);
        assert_eq!(failed.receipt().backend_write_operations, 0);
        assert_eq!(failed.receipt().object_bytes_written, 0);
        assert_eq!(failed.receipt().durability_operations, 0);
        assert_eq!(
            failed.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes()
        );
        assert_eq!(store.index().objects.len(), closure.len());
        assert_eq!(store.index().spilled_objects, 2);
        assert_eq!(store.index().spill_end, spilled_bytes);
        for &id in &closure {
            assert!(
                !store
                    .inner()
                    .contains(id, &mut AuthorityWork::new(native_write_budget(), &token),)
                    .await?
            );
        }
        // Restore only the damaged byte in the same physical spill. The
        // original staging owns the retry; it must not acknowledge a prefix.
        let spill = store.spill_io.write().await;
        spill
            .as_ref()
            .ok_or("refused publication discarded its spill")?
            .write_all_batch_async(vec![OwnedWrite {
                offset: if truncated { spilled_bytes - 1 } else { 0 },
                bytes: bytes::Bytes::from_static(if truncated { &[1] } else { &[0] }),
            }])
            .await?;
    }
    let work = NativeMaterializationWork::new(native_write_budget(), &token)
        .map_err(|(error, _)| error)?;
    let hold = store
        .flush_before_publish(
            PublicationScope {
                objects: &closure,
                proven_at: store.collection.sweeps(),
            },
            &mut crate::AuthorityWork::native(&work, &token),
        )
        .await?;
    assert!(store.index().objects.is_empty());
    assert_eq!(store.index().spilled_objects, 0);
    assert_eq!(store.index().spill_end, 0);
    assert!(store.spill_io.read().await.is_none());
    assert_eq!(work.receipt().object_bytes_read, spilled_bytes);
    assert_eq!(work.receipt().bytes_hashed, spilled_bytes);
    assert!(work.receipt().backend_read_operations > 0);
    assert!(work.receipt().backend_write_operations > 0);
    drop(hold);
    drop(store);
    drop(provider);
    assert!(reopen_contains(directory.path(), &closure).await?);
    Ok(())
}

#[tokio::test]
async fn spill_denial_precedes_physical_write_and_restores_victims()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = StagedObjects::open(
        crate::MemoryObjectStore::default(),
        directory.path().to_path_buf(),
    );
    let token = CancellationToken::new();
    let mut ids = Vec::new();
    for marker in 0..3_u8 {
        let (id, bytes) = blob(OwnedBytes::from(vec![marker; 2 * 1_048_576]));
        store
            .index_mut()
            .admit_resident(HashedObject::verify(id, bytes).ok_or(ObjectStoreError::Corrupt)?)?;
        ids.push(id);
    }
    let mut spill_file = store.spill_io.write().await;
    for allocation_operations in [0, 1] {
        let mut unfunded = AuthorityWork::new(
            WorkBudget {
                allocation_operations,
                ..native_write_budget()
            },
            &token,
        );
        assert!(matches!(
            store.spill_locked(&mut spill_file, &mut unfunded).await,
            Err(ObjectStoreError::Work(crate::WorkError::BudgetExceeded {
                counter: "allocation_operations",
                ..
            }))
        ));
        assert_eq!(
            unfunded.spent().allocation_operations,
            allocation_operations
        );
        assert_eq!(unfunded.spent().backend_write_operations, 0);
        assert_eq!(unfunded.spent().object_bytes_written, 0);
        assert_eq!(store.index().order.len(), ids.len());
        assert_eq!(store.index().spilled_objects, 0);
        assert!(spill_file.is_none());
    }
    let mut budget = native_write_budget();
    budget.backend_write_operations = 0;
    let denied = NativeMaterializationWork::new(budget, &token).map_err(|(error, _)| error)?;
    assert!(
        store
            .spill_locked(&mut spill_file, &mut AuthorityWork::native(&denied, &token))
            .await
            .is_err()
    );
    assert_eq!(denied.receipt().backend_write_operations, 0);
    assert_eq!(denied.receipt().object_bytes_written, 0);
    assert_eq!(store.index().spill_end, 0);
    assert_eq!(store.index().spilled_objects, 0);
    assert_eq!(store.index().order.len(), ids.len());
    // A refused spill leaves no spill file, so it holds no descriptor.
    assert!(spill_file.is_none());
    for id in &ids {
        assert!(matches!(store.lookup(*id), Lookup::Resident(_)));
    }
    let work = NativeMaterializationWork::new(native_write_budget(), &token)
        .map_err(|(error, _)| error)?;
    store
        .spill_locked(&mut spill_file, &mut AuthorityWork::native(&work, &token))
        .await?;
    assert!(work.receipt().backend_write_operations > 0);
    assert_eq!(work.receipt().object_bytes_written, store.index().spill_end);
    assert!(store.index().spilled_objects > 0);
    Ok(())
}

#[tokio::test]
async fn native_flush_spilled_read_denial_keeps_private_remainder()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, _provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    let (id, _) = spill_fixture(&store, &token).await?;
    let mut budget = native_write_budget();
    budget.backend_read_operations = 0;
    let work = NativeMaterializationWork::new(budget, &token).map_err(|(error, _)| error)?;
    let closure = [id];
    assert!(
        store
            .flush_before_publish(
                PublicationScope {
                    objects: &closure,
                    proven_at: store.collection.sweeps(),
                },
                &mut crate::AuthorityWork::native(&work, &token),
            )
            .await
            .is_err()
    );
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    assert_eq!(work.receipt().object_bytes_read, 0);
    assert!(matches!(store.lookup(id), Lookup::Spilled { .. }));
    assert_eq!(store.index().spilled_objects, 1);
    Ok(())
}

struct CancelAfterNativePut {
    inner: LogicalObjectStore<acyclic_objects::local::LocalObjects>,
    cancel_once: AtomicBool,
    committed_write_operations: std::sync::atomic::AtomicU64,
}

impl AsyncObjectStore for CancelAfterNativePut {
    crate::async_storage::forward_object_store!(self => self.inner;
        requires_child_first_publication,
        maximum_reference_bytes,
        prepare_hashed,
        put,
        read,
        read_many,
        contains,
        flush_before_publish,
    );

    async fn put_hashed(
        &self,
        object: HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_hashed(object, context).await?;
        if let Some(original) = context.original
            && self.cancel_once.swap(false, Ordering::Relaxed)
        {
            self.committed_write_operations.store(
                original.receipt().backend_write_operations,
                Ordering::Relaxed,
            );
            context.cancellation.cancel();
        }
        Ok(())
    }
}

#[tokio::test]
async fn native_flush_cancellation_retains_unacknowledged_batch_for_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = StagedObjects::open(
        CancelAfterNativePut {
            inner: LogicalObjectStore::new(Arc::new(
                acyclic_objects::local::LocalObjects::open(
                    directory.path().join("objects"),
                    acyclic_objects::local::LocalObjectsLimits::default(),
                    None,
                )
                .await?,
            )),
            cancel_once: AtomicBool::new(true),
            committed_write_operations: std::sync::atomic::AtomicU64::new(0),
        },
        directory.path().to_path_buf(),
    );
    let token = CancellationToken::new();
    let (first, first_bytes) = blob(OwnedBytes::from_static(b"acknowledged first"));
    let (second, second_bytes) = blob(OwnedBytes::from_static(b"retryable second"));
    let work = NativeMaterializationWork::new(native_write_budget(), &token)
        .map_err(|(error, _)| error)?;
    store
        .put(
            first,
            first_bytes.clone(),
            &mut crate::AuthorityWork::native(&work, &token),
        )
        .await?;
    store
        .put(
            second,
            second_bytes,
            &mut crate::AuthorityWork::native(&work, &token),
        )
        .await?;
    let closure = [first, second];
    assert!(matches!(
        store
            .flush_before_publish(
                PublicationScope {
                    objects: &closure,
                    proven_at: store.collection.sweeps(),
                },
                &mut crate::AuthorityWork::native(&work, &token),
            )
            .await,
        Err(ObjectStoreError::Native(
            crate::NativeTreeMaterializationError::Cancelled
        ))
    ));
    assert!(store.index().objects.contains_key(&first));
    assert!(store.index().objects.contains_key(&second));
    assert!(
        store
            .inner()
            .inner
            .contains(
                first,
                &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            )
            .await?
    );
    assert!(
        !store
            .inner()
            .inner
            .contains(
                second,
                &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            )
            .await?
    );
    let committed_write_operations = store
        .inner()
        .committed_write_operations
        .load(Ordering::Relaxed);
    assert!(committed_write_operations > 0);
    assert_eq!(
        work.receipt().backend_write_operations,
        committed_write_operations
    );
    assert_eq!(
        work.receipt().object_bytes_written,
        first_bytes.len() as u64
    );
    let token = CancellationToken::new();
    let retry = NativeMaterializationWork::new(native_write_budget(), &token)
        .map_err(|(error, _)| error)?;
    let hold = store
        .flush_before_publish(
            PublicationScope {
                objects: &closure,
                proven_at: store.collection.sweeps(),
            },
            &mut crate::AuthorityWork::native(&retry, &token),
        )
        .await?;
    assert!(store.index().objects.is_empty());
    assert_eq!(retry.receipt().bytes_hashed, 0);
    assert_eq!(
        work.receipt().backend_write_operations,
        committed_write_operations
    );
    drop(hold);
    Ok(())
}
