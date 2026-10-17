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
async fn native_flush_exact_closure_preserves_intrinsic_alias_without_readoption()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let token = CancellationToken::new();
    let work = NativeMaterializationWork::new(native_write_budget(), &token)
        .map_err(|(error, _)| error)?;
    let (buffer, mut body) = staged_vector::<u8>(32, Some(&work))?;
    body.extend_from_slice(b"original physical byte owner");
    let bytes = staged_owned_bytes(body, buffer.ok_or("unfunded test buffer")?, &work)?;
    let object = HashedObject::new(ObjectKind::Blob, bytes.clone());
    let object_id = object.object_id();
    let store = StagedObjects::open(
        crate::MemoryObjectStore::default(),
        directory.path().to_path_buf(),
    )
    .await?;
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
            PublicationScope::Closure {
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
    let (id, bytes) = spill_fixture(&store, &token).await?;
    let work = NativeMaterializationWork::new(native_write_budget(), &token)
        .map_err(|(error, _)| error)?;
    let closure = [id];
    let hold = store
        .flush_before_publish(
            PublicationScope::Closure {
                objects: &closure,
                proven_at: store.collection.sweeps(),
            },
            &mut crate::AuthorityWork::native(&work, &token),
        )
        .await?;
    assert!(store.index().objects.is_empty());
    assert_eq!(store.index().spilled_objects, 0);
    assert_eq!(store.index().spill_end, 0);
    assert_eq!(work.receipt().object_bytes_read, bytes.len() as u64);
    assert!(work.receipt().backend_read_operations > 0);
    assert!(work.receipt().backend_write_operations > 0);
    assert!(work.receipt().durability_operations > 0);
    drop(hold);
    drop(store);
    drop(provider);
    assert!(reopen_contains(directory.path(), &closure).await?);
    Ok(())
}

#[tokio::test]
async fn native_spill_denial_precedes_physical_write_and_restores_victims()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = StagedObjects::open(
        crate::MemoryObjectStore::default(),
        directory.path().to_path_buf(),
    )
    .await?;
    let token = CancellationToken::new();
    let mut ids = Vec::new();
    for marker in 0..3_u8 {
        let (id, bytes) = blob(OwnedBytes::from(vec![marker; 2 * 1_048_576]));
        store.index_mut().admit_resident(id, bytes);
        ids.push(id);
    }
    let mut budget = native_write_budget();
    budget.backend_write_operations = 0;
    let denied = NativeMaterializationWork::new(budget, &token).map_err(|(error, _)| error)?;
    let _spill_file = store.spill_io.write().await;
    assert!(
        store
            .spill_locked(&mut AuthorityWork::native(&denied, &token))
            .await
            .is_err()
    );
    assert_eq!(denied.receipt().backend_write_operations, 0);
    assert_eq!(denied.receipt().object_bytes_written, 0);
    assert_eq!(store.index().spill_end, 0);
    assert_eq!(store.index().spilled_objects, 0);
    assert_eq!(store.index().order.len(), ids.len());
    let physical = store
        .spill
        .read_batch_async(vec![OwnedRead {
            offset: 0,
            length: 1,
        }])
        .await?;
    assert!(physical[0].is_empty());
    for id in &ids {
        assert!(matches!(store.lookup(*id), Lookup::Resident(_)));
    }
    let work = NativeMaterializationWork::new(native_write_budget(), &token)
        .map_err(|(error, _)| error)?;
    store
        .spill_locked(&mut AuthorityWork::native(&work, &token))
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
                PublicationScope::Closure {
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
    inner: crate::MemoryObjectStore,
    cancel_once: AtomicBool,
}

impl AsyncObjectStore for CancelAfterNativePut {
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        for write in writes {
            context
                .accounting()
                .check()
                .map_err(crate::async_storage::object_error)?;
            self.put(write.object_id, write.bytes.clone(), context)
                .await?;
        }
        Ok(())
    }
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
                AsyncObjectStore::put(
                    &self.inner,
                    id,
                    bytes,
                    &mut crate::AuthorityWork::native(work, cancellation),
                )
                .await?;
                if self.cancel_once.swap(false, Ordering::Relaxed) {
                    cancellation.cancel();
                }
                Ok(())
            }
            .await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            {
                let __object_args = (id, bytes, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
                let __object_result = AsyncObjectStore::put(
                    &self.inner,
                    __object_args.0,
                    __object_args.1,
                    &mut __object_work,
                )
                .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.read_budget()?;
        let cancellation = context.cancellation;
        let result = async {
            {
                let __object_args = (id, maximum, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
                let __object_result = AsyncObjectStore::read(
                    &self.inner,
                    __object_args.0,
                    __object_args.1,
                    &mut __object_work,
                )
                .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            {
                let __object_args = (requests, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result =
                    AsyncObjectStore::read_many(&self.inner, __object_args.0, &mut __object_work)
                        .await
                        .map(|(values, _allocation)| values);
                __object_work.receipt(__object_result)
            }
        }
        .await;
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
        let result = async {
            {
                let __object_args = (id, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result =
                    AsyncObjectStore::contains(&self.inner, __object_args.0, &mut __object_work)
                        .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
}

#[tokio::test]
async fn native_flush_cancellation_retains_unacknowledged_batch_for_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = StagedObjects::open(
        CancelAfterNativePut {
            inner: crate::MemoryObjectStore::default(),
            cancel_once: AtomicBool::new(true),
        },
        directory.path().to_path_buf(),
    )
    .await?;
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
                PublicationScope::Closure {
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
    assert_eq!(work.receipt().backend_write_operations, 1);
    assert_eq!(
        work.receipt().object_bytes_written,
        first_bytes.len() as u64
    );
    let token = CancellationToken::new();
    let retry = NativeMaterializationWork::new(native_write_budget(), &token)
        .map_err(|(error, _)| error)?;
    let hold = store
        .flush_before_publish(
            PublicationScope::Closure {
                objects: &closure,
                proven_at: store.collection.sweeps(),
            },
            &mut crate::AuthorityWork::native(&retry, &token),
        )
        .await?;
    assert!(store.index().objects.is_empty());
    assert_eq!(retry.receipt().backend_write_operations, 2);
    assert_eq!(work.receipt().backend_write_operations, 1);
    drop(hold);
    Ok(())
}
