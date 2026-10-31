use super::*;

#[tokio::test]
async fn flush_vector_and_visit_denial_precede_provider_work()
-> Result<(), Box<dyn std::error::Error>> {
    for budget in [
        WorkBudget {
            allocation_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
        WorkBudget {
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        },
        WorkBudget {
            items_examined: 0,
            ..WorkBudget::UNBOUNDED
        },
    ] {
        let directory = tempfile::tempdir()?;
        let store = StagedObjects::open(
            crate::MemoryObjectStore::default(),
            directory.path().to_owned(),
        );
        let token = CancellationToken::new();
        let (id, bytes) = blob(OwnedBytes::from_static(b"fund the real staged publication"));
        store
            .put(
                id,
                bytes.clone(),
                &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
            )
            .await?;
        let scope = PublicationScope::Closure {
            objects: &[id],
            proven_at: store.collection.sweeps(),
        };
        let mut denied = AuthorityWork::new(budget, &token);
        assert!(matches!(
            store.flush_before_publish(scope, &mut denied).await,
            Err(ObjectStoreError::Work(
                crate::WorkError::BudgetExceeded { .. }
            ))
        ));
        assert_eq!(denied.spent().object_probes, 0);
        assert_eq!(denied.spent().bytes_hashed, 0);
        assert_eq!(denied.spent().backend_write_operations, 0);
        assert!(store.index().objects.contains_key(&id));
        let mut retry = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
        assert!(!store.inner().contains(id, &mut retry).await?);
        drop(store.flush_before_publish(scope, &mut retry).await?);
        assert!(store.index().objects.is_empty());
        assert_eq!(retry.spent().items_examined, 3);
        assert_eq!(
            store
                .inner()
                .read(id, bytes.len() as u64, &mut retry)
                .await?
                .bytes,
            bytes
        );
    }
    Ok(())
}

#[tokio::test]
async fn durable_reads_do_not_wait_for_the_private_spill_publication_gate()
-> Result<(), Box<dyn std::error::Error>> {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let directory = tempfile::tempdir()?;
    let store = StagedObjects::open(
        crate::MemoryObjectStore::default(),
        directory.path().to_owned(),
    );
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
    );
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
    advance_spill_past_bound(&store).await?;

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
                ObjectKind::BlobChunk,
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
    // Every new file restages the same default metadata; the blob store
    // dedupes it by digest and a repeat put writes nothing, so the work per
    // publication is only its fresh body however many came before.
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
    let (gate, _) = collecting.sweepable(vec![shared.digest]).await;
    collecting.sweeping(shared.digest);
    provider
        .delete(acyclic_objects::Digest::from_bytes(
            *shared.digest.as_bytes(),
        ))
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
    let object = HashedObject::new(
        ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"proven body"),
    );
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
                ObjectKind::BlobChunk,
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

/// Holds its first flush until the test releases `gate`, counting the
/// objects that reach it.
struct PausedFlush {
    inner: crate::MemoryObjectStore,
    gate: Arc<tokio::sync::Mutex<()>>,
    first: std::sync::atomic::AtomicBool,
    paused: tokio::sync::Notify,
    stored: std::sync::atomic::AtomicUsize,
}

impl AsyncObjectStore for PausedFlush {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.stored.fetch_add(1, Ordering::Relaxed);
        self.inner.put(id, bytes, work).await
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.stored.fetch_add(writes.len(), Ordering::Relaxed);
        self.inner.put_many(writes, work).await
    }
    async fn flush_before_publish(
        &self,
        scope: crate::PublicationScope<'_>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        if self.first.swap(false, Ordering::Relaxed) {
            self.paused.notify_one();
            drop(self.gate.lock().await);
        }
        self.inner.flush_before_publish(scope, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        self.inner.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.inner.contains(id, work).await
    }
}

#[tokio::test]
async fn a_paused_inner_flush_does_not_hold_another_publications_drain()
-> Result<(), Box<dyn std::error::Error>> {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let directory = tempfile::tempdir()?;
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let held = Arc::clone(&gate).lock_owned().await;
    let store = StagedObjects::open(
        PausedFlush {
            inner: crate::MemoryObjectStore::default(),
            gate,
            first: std::sync::atomic::AtomicBool::new(true),
            paused: tokio::sync::Notify::new(),
            stored: std::sync::atomic::AtomicUsize::new(0),
        },
        directory.path().to_owned(),
    );
    let token = CancellationToken::new();
    let (first, first_bytes) = blob(OwnedBytes::from_static(b"first publication"));
    let (second, second_bytes) = blob(OwnedBytes::from_static(b"second publication"));
    for (id, bytes) in [(first, first_bytes), (second, second_bytes)] {
        store
            .put(
                id,
                bytes,
                &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
            )
            .await?;
    }
    let flush = |id: ObjectId| {
        let store = &store;
        let token = &token;
        async move {
            let objects = [id];
            let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, token);
            store
                .flush_before_publish(
                    PublicationScope::Closure {
                        objects: &objects,
                        proven_at: store.collection.sweeps(),
                    },
                    &mut work,
                )
                .await
                .map(drop)
        }
    };
    let paused = flush(first);
    let during = async {
        // The first publication drained and waits on its inner flush.
        store.inner().paused.notified().await;
        assert_eq!(store.inner().stored.load(Ordering::Relaxed), 1);
        let mut second = std::pin::pin!(flush(second));
        let finished = match Future::poll(second.as_mut(), &mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(flushed) => {
                flushed?;
                assert_eq!(store.inner().stored.load(Ordering::Relaxed), 2);
                true
            }
            Poll::Pending => false,
        };
        drop(held);
        if !finished {
            second.await?;
        }
        Ok::<_, ObjectStoreError>(finished)
    };
    let (paused, finished) = tokio::join!(paused, during);
    paused?;
    assert!(
        finished?,
        "the second publication's drain waited on the first one's inner flush"
    );
    assert_eq!(store.inner().stored.load(Ordering::Relaxed), 2);
    Ok(())
}
