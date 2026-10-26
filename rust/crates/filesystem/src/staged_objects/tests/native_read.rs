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
            .and_then(|_| store.decoded_cache_admit(__decoded_args.0, __decoded_args.1))
    }?);
    let cached = {
        let __decoded_args = (key, &work);
        __decoded_args
            .1
            .remaining()
            .map_err(crate::ObjectStoreError::from)
            .and_then(|_| store.decoded_cache_get(__decoded_args.0))
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
