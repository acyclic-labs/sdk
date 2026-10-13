use super::*;
use crate::AuthorityWork;
use crate::materializer::NativeTreeMaterializationError;
use futures::FutureExt as _;

fn budget() -> WorkBudget {
    WorkBudget {
        backend_read_operations: 8,
        object_probes: 32,
        object_bytes_read: 65_536,
        bytes_hashed: 65_536,
        bytes_copied: 65_536,
        source_bytes_read: 65_536,
        items_examined: 256,
        items_returned: 256,
        allocation_operations: 128,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}

#[derive(Clone, Copy)]
enum Finish {
    Return,
    Error,
    Panic,
    Pending,
}

struct OwnedNativeStore {
    inner: MemoryObjectStore,
    finish: Finish,
}

impl AsyncObjectStore for OwnedNativeStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        context.object_fallback()?;
        self.inner.put(id, bytes, context).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        context: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        context.object_fallback()?;
        self.inner.contains(id, context).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let source = self.inner.read(id, maximum, context).await?;
        let Some(original) = context.original else {
            return Ok(source);
        };
        let (mut bytes, allocation) = original.copy_bytes(&source.bytes)?;
        bytes.resize(bytes.capacity(), 0);
        let prepared =
            acyclic_stream::PreparedOwnedBytes::new(bytes, allocation, original.runtime_meter())
                .map_err(|failure| match failure.into_error() {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                        original.runtime_error(error).into()
                    }
                    acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                        ObjectStoreError::Corrupt
                    }
                })?;
        let bytes = prepared
            .freeze(source.bytes.len())
            .map_err(|_| ObjectStoreError::Corrupt)?;
        let logical_bytes = bytes.retained_capacity().ok_or(ObjectStoreError::Corrupt)? as u64;
        let read = crate::async_storage::owned_read(
            ObjectRead {
                bytes,
                retention: ObjectReadRetention::Owned { logical_bytes },
            },
            original,
        )?;
        match self.finish {
            Finish::Return => Ok(read),
            Finish::Error => Err(ObjectStoreError::Native(
                NativeTreeMaterializationError::InvalidLayout,
            )),
            Finish::Panic => panic!("native producer unwinds after copying"),
            Finish::Pending => std::future::pending().await,
        }
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        crate::async_storage::read_many_sequential(self, requests, context).await
    }
}

async fn owned_cache(
    finish: Finish,
    cancel: &CancellationToken,
) -> Result<(CachedObjectStore<OwnedNativeStore>, ObjectId), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let (id, bytes) = object(b"owned");
    inner
        .put(
            id,
            bytes,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, cancel),
        )
        .await?;
    Ok((
        CachedObjectStore::new(OwnedNativeStore { inner, finish }, options(2, 64))?,
        id,
    ))
}

#[tokio::test]
async fn scalar_and_batch_cache_eviction_cannot_release_a_live_byte_alias()
-> Result<(), Box<dyn std::error::Error>> {
    for batch in [false, true] {
        let cancel = CancellationToken::new();
        let (cached, id) = owned_cache(Finish::Return, &cancel).await?;
        let original =
            NativeMaterializationWork::new(budget(), &cancel).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut work = AuthorityWork::native(&original, &cancel);
        let requests = [ObjectReadRequest {
            object_id: id,
            maximum_bytes: 5,
        }; 2];
        let alias = if batch {
            let values = cached.read_many(&requests, &mut work).await?;
            assert_eq!(values.0.len(), 2);
            assert_eq!(values.0[0].bytes, values.0[1].bytes);
            values.0[0].bytes.clone()
        } else {
            cached.read(id, 5, &mut work).await?.bytes
        };
        let held = original.live_allocation_bytes();
        assert!(held > baseline);
        drop(cached.read(id, 5, &mut work).await?);
        cached.clear()?;
        assert_eq!(original.live_allocation_bytes(), held);
        assert_eq!(alias.as_ref(), b"owned");
        drop(alias);
        assert_eq!(original.live_allocation_bytes(), baseline);
        original.receipt().verify(budget())?;
    }
    Ok(())
}

#[tokio::test]
async fn failed_and_abandoned_scalar_and_batch_reads_release_flights_and_allocations()
-> Result<(), Box<dyn std::error::Error>> {
    for batch in [false, true] {
        for finish in [Finish::Error, Finish::Panic, Finish::Pending] {
            let cancel = CancellationToken::new();
            let (cached, id) = owned_cache(finish, &cancel).await?;
            let original =
                NativeMaterializationWork::new(budget(), &cancel).map_err(|(error, _)| error)?;
            let baseline = original.live_allocation_bytes();
            let mut future = Box::pin(async {
                let mut work = AuthorityWork::native(&original, &cancel);
                if batch {
                    cached
                        .read_many(
                            &[ObjectReadRequest {
                                object_id: id,
                                maximum_bytes: 5,
                            }; 2],
                            &mut work,
                        )
                        .await
                        .map(|_| ())
                } else {
                    cached.read(id, 5, &mut work).await.map(|_| ())
                }
            });
            match finish {
                Finish::Error => assert!(matches!(
                    future.await,
                    Err(ObjectStoreError::Native(
                        NativeTreeMaterializationError::InvalidLayout
                    ))
                )),
                Finish::Panic => assert!(
                    std::panic::AssertUnwindSafe(future)
                        .catch_unwind()
                        .await
                        .is_err()
                ),
                Finish::Pending => {
                    assert!(
                        future
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                    assert!(original.live_allocation_bytes() > baseline);
                    drop(future);
                }
                Finish::Return => unreachable!(),
            }
            assert_eq!(original.live_allocation_bytes(), baseline);
            assert_eq!(cached.stats()?.in_flight, 0);
            original.receipt().verify(budget())?;
        }
        let cancel = CancellationToken::new();
        let (cached, id) = owned_cache(Finish::Return, &cancel).await?;
        let mut denied = budget();
        if batch {
            denied.allocation_operations = 1;
        } else {
            denied.backend_read_operations = 0;
        }
        let original =
            NativeMaterializationWork::new(denied, &cancel).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut work = AuthorityWork::native(&original, &cancel);
        let failure = if batch {
            cached
                .read_many(
                    &[ObjectReadRequest {
                        object_id: id,
                        maximum_bytes: 5,
                    }],
                    &mut work,
                )
                .await
                .map(|_| ())
        } else {
            cached.read(id, 5, &mut work).await.map(|_| ())
        };
        assert!(matches!(
            failure,
            Err(ObjectStoreError::Native(
                NativeTreeMaterializationError::Work(_)
            ))
        ));
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert_eq!(cached.stats()?.in_flight, 0);
        original.receipt().verify(denied)?;
    }
    Ok(())
}
