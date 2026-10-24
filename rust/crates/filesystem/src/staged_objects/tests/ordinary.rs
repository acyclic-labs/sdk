use super::*;

#[tokio::test]
async fn durable_reads_do_not_wait_for_the_private_spill_publication_gate()
-> Result<(), Box<dyn std::error::Error>> {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let directory = tempfile::tempdir()?;
    let store = StagedObjects::open(
        crate::MemoryObjectStore::default(),
        directory.path().to_owned(),
    )
    .await?;
    let token = CancellationToken::new();
    let (id, bytes) = blob(OwnedBytes::from_static(b"already durable immutable bytes"));
    store
        .inner()
        .put(
            id,
            bytes.clone(),
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
        )
        .await?;
    // A publication can remain suspended on this runtime. Neither a direct
    // read nor a batch read of an already durable object depends on its spill.
    let _publication = store.spill_io.write().await;
    for native in [false, true] {
        let original = NativeMaterializationWork::new(native_read_budget(), &token)
            .map_err(|(cause, _)| cause)?;
        let mut work = if native {
            AuthorityWork::native(&original, &token)
        } else {
            AuthorityWork::new(native_read_budget(), &token)
        };
        let mut future = std::pin::pin!(store.read(id, bytes.len() as u64, &mut work));
        let Poll::Ready(result) =
            Future::poll(future.as_mut(), &mut Context::from_waker(Waker::noop()))
        else {
            panic!("durable read waited for an unrelated spill publication");
        };
        assert_eq!(result?.bytes, bytes);
        drop(future);
        let requests = [ObjectReadRequest {
            object_id: id,
            maximum_bytes: bytes.len() as u64,
        }];
        let mut work = if native {
            AuthorityWork::native(&original, &token)
        } else {
            AuthorityWork::new(native_read_budget(), &token)
        };
        let mut future = std::pin::pin!(store.read_many(&requests, &mut work));
        let Poll::Ready(result) =
            Future::poll(future.as_mut(), &mut Context::from_waker(Waker::noop()))
        else {
            panic!("durable batch read waited for an unrelated spill publication");
        };
        assert_eq!(result?.0[0].bytes, bytes);
        assert_eq!(
            original.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes()
        );
    }
    let denied = NativeMaterializationWork::new(
        WorkBudget {
            object_bytes_read: 0,
            ..native_read_budget()
        },
        &token,
    )
    .map_err(|(cause, _)| cause)?;
    assert!(
        store
            .read(
                id,
                bytes.len() as u64,
                &mut AuthorityWork::native(&denied, &token)
            )
            .await
            .is_err()
    );
    assert_eq!(denied.receipt().object_bytes_read, 0);
    token.cancel();
    assert!(
        store
            .read(
                id,
                bytes.len() as u64,
                &mut AuthorityWork::new(native_read_budget(), &token)
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn durable_reads_admit_the_original_frame_before_polling_the_backend()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = StagedObjects::open(
        crate::MemoryObjectStore::default(),
        directory.path().to_owned(),
    )
    .await?;
    let token = CancellationToken::new();
    let (id, bytes) = blob(OwnedBytes::from_static(b"durable bytes"));
    store
        .inner()
        .put(
            id,
            bytes,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
        )
        .await?;
    let original = NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 1,
            ..native_read_budget()
        },
        &token,
    )
    .map_err(|(cause, _)| cause)?;
    let before = original.receipt();
    assert!(
        store
            .read(id, 1024, &mut AuthorityWork::native(&original, &token))
            .await
            .is_err()
    );
    assert_eq!(original.receipt(), before);
    assert_eq!(
        original.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn failed_drain_keeps_objects_private_until_a_durable_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, provider) = open_staged(directory.path()).await?;
    let (object_id, bytes) = blob(OwnedBytes::from_static(b"staged immutable body"));
    let token = CancellationToken::new();
    {
        let __object_args = (object_id, bytes.clone(), WorkBudget::UNBOUNDED, &token);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    assert!(
        {
            let __object_args = (object_id, WorkBudget::UNBOUNDED, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store.contains(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }?
        .value
    );
    assert!(
        !store
            .inner()
            .contains(
                object_id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &token)
            )
            .await?
    );

    let mut insufficient = WorkBudget::UNBOUNDED;
    insufficient.object_bytes_written = 0;
    assert!(
        {
            let __object_args = (PublicationScope::Everything, insufficient, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store
                .flush_before_publish(__object_args.0, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }
        .is_err()
    );
    assert!(
        {
            let __object_args = (object_id, WorkBudget::UNBOUNDED, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store.contains(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }?
        .value
    );
    assert!(
        !store
            .inner()
            .contains(
                object_id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &token)
            )
            .await?
    );

    {
        let __object_args = (PublicationScope::Everything, WorkBudget::UNBOUNDED, &token);
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .flush_before_publish(__object_args.0, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    assert!(
        store
            .inner()
            .contains(
                object_id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &token)
            )
            .await?
    );
    drop(store);
    drop(provider);
    assert!(reopen_contains(directory.path(), &[object_id]).await?);
    Ok(())
}

#[tokio::test]
async fn overflow_spills_privately_until_the_publication_drain()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    // Three windows of distinct bodies force repeated spills, and the
    // drain must split them across several segment-bounded batches.
    let body_bytes = 256 * 1_024;
    let count = 3 * usize::try_from(MAXIMUM_RESIDENT_BYTES)? / body_bytes;
    let objects = (0..count)
        .map(|index| {
            blob(OwnedBytes::from(vec![
                u8::try_from(index % 251).unwrap_or(0);
                body_bytes
            ]))
        })
        .collect::<Vec<_>>();
    for (object_id, bytes) in &objects {
        {
            let __object_args = (*object_id, bytes.clone(), WorkBudget::UNBOUNDED, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .put(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }?;
    }
    for (object_id, bytes) in &objects {
        assert!(
            !store
                .inner()
                .contains(
                    *object_id,
                    &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &token)
                )
                .await?,
            "overflow must not make staged bytes durable"
        );
        let read = {
            let __object_args = (*object_id, u64::MAX, WorkBudget::UNBOUNDED, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }?;
        assert_eq!(&read.value.bytes, bytes);
    }
    {
        let __object_args = (PublicationScope::Everything, WorkBudget::UNBOUNDED, &token);
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .flush_before_publish(__object_args.0, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    for (object_id, bytes) in &objects {
        let read = {
            let __object_args = (*object_id, u64::MAX, WorkBudget::UNBOUNDED, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }?;
        assert_eq!(&read.value.bytes, bytes);
    }
    assert_eq!(store.index().spill_end, 0);
    let identities = objects
        .iter()
        .map(|(object_id, _)| *object_id)
        .collect::<Vec<_>>();
    drop(store);
    drop(provider);
    assert!(reopen_contains(directory.path(), &identities).await?);
    Ok(())
}

#[tokio::test]
async fn a_spill_past_its_bound_drains_without_charging_or_failing_admissions()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    let body_bytes = 256 * 1_024;
    let window = usize::try_from(MAXIMUM_RESIDENT_BYTES)? / body_bytes;
    let objects = (0..8 * window)
        .map(|index| {
            blob(OwnedBytes::from(vec![
                u8::try_from(index % 251).unwrap_or(0);
                body_bytes
            ]))
        })
        .collect::<Vec<_>>();
    let (early, late) = objects.split_at(3 * window);
    for (object_id, bytes) in early {
        {
            let __object_args = (*object_id, bytes.clone(), WorkBudget::UNBOUNDED, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .put(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }?;
    }
    assert!(store.index().spilled_objects > 0);
    // Stand in for a spill file that has grown past its bound.
    store.index_mut().spill_end += MAXIMUM_SPILL_BYTES;

    // An admission may spend exactly its own spill: one backend write.
    let mut admission = WorkBudget::UNBOUNDED;
    admission.backend_write_operations = 1;
    for (object_id, bytes) in late {
        let receipt = {
            let __object_args = (*object_id, bytes.clone(), admission, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .put(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }?;
        assert!(receipt.work.backend_write_operations <= 1);
    }
    let durable = {
        let mut durable = 0;
        for (object_id, _) in early {
            if store
                .inner()
                .contains(
                    *object_id,
                    &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
                )
                .await?
            {
                durable += 1;
            }
        }
        durable
    };
    assert!(durable > 0, "the drain steps made spilled objects durable");
    assert!(
        store.index().spill_end <= MAXIMUM_SPILL_BYTES,
        "the steps emptied and reused the spill file"
    );
    for (object_id, bytes) in &objects {
        let read = {
            let __object_args = (*object_id, u64::MAX, WorkBudget::UNBOUNDED, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }?;
        assert_eq!(&read.value.bytes, bytes);
    }
    drop(store);
    drop(provider);
    Ok(())
}

#[tokio::test]
async fn publication_drains_exactly_its_closure_and_keeps_the_rest_private()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    // Enough distinct bodies to spill several windows, so the closure
    // spans resident and spilled objects alike.
    let body_bytes = 256 * 1_024;
    let count = 3 * usize::try_from(MAXIMUM_RESIDENT_BYTES)? / body_bytes;
    let objects = (0..count)
        .map(|index| {
            HashedObject::new(
                ObjectKind::Blob,
                OwnedBytes::from(vec![u8::try_from(index % 251).unwrap_or(0); body_bytes]),
            )
        })
        .collect::<Vec<_>>();
    for object in &objects {
        {
            let __object_args = (object.clone(), WorkBudget::UNBOUNDED, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store.put_hashed(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }?;
    }
    let identities = objects
        .iter()
        .map(HashedObject::object_id)
        .collect::<Vec<_>>();
    let (closure, superseded): (Vec<_>, Vec<_>) = identities
        .iter()
        .enumerate()
        .partition(|(index, _)| index % 2 == 0);
    let closure = closure.into_iter().map(|(_, id)| *id).collect::<Vec<_>>();
    let superseded = superseded
        .into_iter()
        .map(|(_, id)| *id)
        .collect::<Vec<_>>();

    {
        let __object_args = (
            PublicationScope::Closure {
                objects: &closure,
                proven_at: 0,
            },
            WorkBudget::UNBOUNDED,
            &token,
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .flush_before_publish(__object_args.0, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    for &object_id in &superseded {
        assert!(
            !store
                .inner()
                .contains(
                    object_id,
                    &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &token)
                )
                .await?,
            "an object outside the published closure must stay private"
        );
    }
    for object in &objects {
        let read = {
            let __object_args = (object.object_id(), u64::MAX, WorkBudget::UNBOUNDED, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }?;
        assert_eq!(read.value.bytes, object.clone().into_parts().1);
    }
    drop(store);
    drop(provider);
    assert!(reopen_contains(directory.path(), &closure).await?);
    for object_id in superseded {
        assert!(!reopen_contains(directory.path(), &[object_id]).await?);
    }
    Ok(())
}

#[tokio::test]
async fn each_publication_writes_only_what_is_not_already_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    // Every new file restages the same default metadata; each
    // publication must write only its new objects, so the work per
    // publication stays constant however many came before.
    let (shared, shared_bytes) = blob(OwnedBytes::from_static(b"shared metadata"));
    let publish = |index: usize| {
        let store = &store;
        let token = &token;
        let shared_bytes = shared_bytes.clone();
        async move {
            let (fresh, fresh_bytes) = blob(OwnedBytes::from(format!("file body {index:04}")));
            {
                let __object_args = (shared, shared_bytes, WorkBudget::UNBOUNDED, token);
                let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
                let __object_result = store
                    .put(__object_args.0, __object_args.1, &mut __object_work)
                    .await;
                __object_work.receipt(__object_result)
            }?;
            {
                let __object_args = (fresh, fresh_bytes, WorkBudget::UNBOUNDED, token);
                let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
                let __object_result = store
                    .put(__object_args.0, __object_args.1, &mut __object_work)
                    .await;
                __object_work.receipt(__object_result)
            }?;
            let flushed = {
                let __object_args = (
                    PublicationScope::Closure {
                        objects: &[shared, fresh],
                        proven_at: store.collection.sweeps(),
                    },
                    WorkBudget::UNBOUNDED,
                    token,
                );
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result = store
                    .flush_before_publish(__object_args.0, &mut __object_work)
                    .await;
                __object_work.receipt(__object_result)
            }?;
            Ok::<_, Box<dyn std::error::Error>>((fresh, flushed.work))
        }
    };
    let (_, first) = publish(0).await?;
    assert_eq!(first.object_bytes_written, 15 + 14);
    let mut published = Vec::new();
    for index in 1..32 {
        let (fresh, work) = publish(index).await?;
        assert_eq!(work.backend_write_operations, 1);
        assert_eq!(work.backend_read_operations, 0);
        assert_eq!(work.object_bytes_written, 14);
        assert!(store.index().objects.is_empty());
        published.push(fresh);
    }

    // A sweep may have removed it, so the next publication writes it.
    let collecting = store.collection.begin().await;
    let (gate, _) = collecting.sweepable(vec![shared]).await;
    collecting.sweeping(shared);
    provider
        .delete(acyclic_objects::v1::wire::DeleteObjectRequest {
            bucket: Some(acyclic_objects::v1::wire::BucketRef {
                name: "staged-test".to_owned(),
            }),
            object_key: crate::distributed::object_key(shared),
            ..Default::default()
        })
        .await?;
    drop(gate);
    drop(collecting);
    let (fresh, work) = publish(32).await?;
    assert_eq!(work.object_bytes_written, 15 + 14);
    published.extend([shared, fresh]);
    drop(store);
    drop(provider);
    assert!(reopen_contains(directory.path(), &published).await?);
    Ok(())
}

#[tokio::test]
async fn hashed_admission_skips_rehashing_and_unproven_bytes_are_refused()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, _provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    let object = HashedObject::new(ObjectKind::Blob, OwnedBytes::from_static(b"proven body"));
    let admitted = {
        let __object_args = (object.clone(), WorkBudget::UNBOUNDED, &token);
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store.put_hashed(__object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }?;
    assert_eq!(admitted.work.bytes_hashed, 0);
    // A resident page keeps its own decoded representation, which leaves
    // with it once publication makes the page durable.
    let key = DecodedCacheKey::new::<u32>(object.object_id(), DecodeLimits::default());
    let decoded = DecodedCacheValue {
        value: Arc::new(7_u32),
        logical_bytes: 4,
        native_custody: None,
    };
    assert!(store.decoded_cache_get(key)?.is_none());
    assert!(matches!(
        store.decoded_cache_admit(key, decoded)?,
        DecodedCacheAdmission::Shared(_)
    ));
    let cached = store
        .decoded_cache_get(key)?
        .ok_or("resident page lost its decoding")?;
    assert_eq!(cached.value.downcast_ref::<u32>(), Some(&7));
    let other = DecodedCacheKey::new::<u64>(object.object_id(), DecodeLimits::default());
    assert!(store.decoded_cache_get(other)?.is_none());
    {
        let __object_args = (PublicationScope::Everything, WorkBudget::UNBOUNDED, &token);
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .flush_before_publish(__object_args.0, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    assert!(store.decoded_cache_get(key)?.is_none());
    let (object_id, _) = object.into_parts();
    let forged = {
        let __object_args = (
            object_id,
            OwnedBytes::from_static(b"other body"),
            WorkBudget::UNBOUNDED,
            &token,
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    };
    assert!(matches!(
        forged.map(|_| ()).map_err(|failure| failure.error),
        Err(ObjectStoreError::DigestMismatch)
    ));
    Ok(())
}

#[tokio::test]
async fn concurrent_admissions_reads_and_publication_agree()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    let body_bytes = 64 * 1_024;
    let objects = (0..4 * usize::try_from(MAXIMUM_RESIDENT_BYTES)? / body_bytes)
        .map(|index| {
            HashedObject::new(
                ObjectKind::Blob,
                OwnedBytes::from(vec![u8::try_from(index % 251).unwrap_or(0); body_bytes]),
            )
        })
        .collect::<Vec<_>>();
    // Writers race readers of everything admitted so far while the
    // window spills and one publication drains half the objects.
    let closure = objects
        .iter()
        .step_by(2)
        .map(HashedObject::object_id)
        .collect::<Vec<_>>();
    let (staged, objects, token) = (&store, &objects, &token);
    let writers = futures::future::try_join_all(objects.chunks(8).map(|chunk| async move {
        for object in chunk {
            {
                let __object_args = (object.clone(), WorkBudget::UNBOUNDED, token);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result = staged.put_hashed(__object_args.0, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }?;
            for earlier in objects.iter().take(8) {
                let read = {
                    let __object_args =
                        (earlier.object_id(), u64::MAX, WorkBudget::UNBOUNDED, token);
                    let mut __object_work =
                        crate::AuthorityWork::new(__object_args.2, __object_args.3);
                    let __object_result = staged
                        .read(__object_args.0, __object_args.1, &mut __object_work)
                        .await;
                    __object_work.receipt(__object_result)
                };
                if let Ok(read) = read {
                    assert_eq!(read.value.bytes, earlier.clone().into_parts().1);
                }
            }
        }
        Ok::<_, ObjectFailure>(())
    }));
    writers.await?;
    {
        let __object_args = (
            PublicationScope::Closure {
                objects: &closure,
                proven_at: 0,
            },
            WorkBudget::UNBOUNDED,
            token,
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .flush_before_publish(__object_args.0, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    for object in objects {
        let read = {
            let __object_args = (object.object_id(), u64::MAX, WorkBudget::UNBOUNDED, token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }?;
        assert_eq!(read.value.bytes, object.clone().into_parts().1);
    }
    drop(store);
    drop(provider);
    assert!(reopen_contains(directory.path(), &closure).await?);
    Ok(())
}
