use super::*;
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

fn copied_native_bytes(
    source: &[u8],
    work: &NativeMaterializationWork,
) -> Result<OwnedBytes, ObjectStoreError> {
    let mut allocation = {
        let claim_bytes = source.len() as u64;
        work.claim(
            claim_bytes,
            u64::from(claim_bytes != 0),
            crate::WorkCounters::UNCHARGED,
        )
    }?;
    let copy = work.reserve_work(crate::WorkCounters {
        bytes_copied: source.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    })?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(source.len()).map_err(|error| {
        ObjectStoreError::Native(NativeTreeMaterializationError::AllocationFailed(error))
    })?;
    allocation.reconcile_capacity(bytes.capacity() as u64)?;
    bytes.resize(bytes.capacity(), 0);
    let mut prepared =
        acyclic_stream::PreparedOwnedBytes::new(bytes, allocation, work.runtime_meter()).map_err(
            |failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                    work.runtime_error(error).into()
                }
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                    ObjectStoreError::Native(NativeTreeMaterializationError::InvalidLayout)
                }
            },
        )?;
    prepared.writable()[..source.len()].copy_from_slice(source);
    {
        let mut copy = copy;
        copy.record_completed(crate::WorkCounters {
            bytes_copied: source.len() as u64,
            ..crate::WorkCounters::UNCHARGED
        })
    }?;
    prepared
        .freeze(source.len())
        .map_err(|_| ObjectStoreError::Corrupt)
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
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        context.work.remaining(context.budget)?;

        self.inner.put(id, bytes, context).await
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let token = context.cancellation;
            return async {
                let source = {
                    let mut __object_work = crate::AuthorityWork::native(work, token);
                    let __object_result = self.inner.read(id, maximum, &mut __object_work).await;
                    __object_result
                }?;
                let bytes = copied_native_bytes(&source.bytes, work)?;
                let logical_bytes =
                    bytes.retained_capacity().ok_or(ObjectStoreError::Corrupt)? as u64;
                let read = crate::async_storage::owned_read(
                    ObjectRead {
                        bytes,
                        retention: ObjectReadRetention::Owned { logical_bytes },
                    },
                    work,
                )?;
                match self.finish {
                    Finish::Return => {}
                    Finish::Error => {
                        return Err(ObjectStoreError::Native(
                            NativeTreeMaterializationError::InvalidLayout,
                        ));
                    }
                    Finish::Panic => panic!("native producer unwinds after its actual copy"),
                    Finish::Pending => std::future::pending::<()>().await,
                }
                Ok(read)
            }
            .await;
        }
        context.work.remaining(context.budget)?;

        self.inner.read(id, maximum, context).await
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
        context.work.remaining(context.budget)?;

        self.inner.contains(id, context).await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let token = context.cancellation;
            return async {
                let source = {
                    let mut __object_work = crate::AuthorityWork::native(work, token);
                    let __object_result = self
                        .inner
                        .read_many(requests, &mut __object_work)
                        .await
                        .map(|(values, allocation)| {
                            (values, allocation.expect("native batch allocation"))
                        });
                    __object_result
                }?;
                let (values, allocation) = source;
                let mut buffer = BatchBuffer {
                    values,
                    allocation: Some(KernelOwnedAllocation::NativeOriginal(allocation)),
                };
                for value in &mut buffer.values {
                    let bytes = copied_native_bytes(&value.bytes, work)?;
                    let logical_bytes =
                        bytes.retained_capacity().ok_or(ObjectStoreError::Corrupt)? as u64;
                    *value = crate::async_storage::owned_read(
                        ObjectRead {
                            bytes,
                            retention: ObjectReadRetention::Owned { logical_bytes },
                        },
                        work,
                    )?;
                }
                match self.finish {
                    Finish::Return => {}
                    Finish::Error => {
                        return Err(ObjectStoreError::Native(
                            NativeTreeMaterializationError::InvalidLayout,
                        ));
                    }
                    Finish::Panic => {
                        panic!("native batch producer unwinds after its actual copy")
                    }
                    Finish::Pending => std::future::pending::<()>().await,
                }
                let BatchBuffer { values, allocation } = buffer;
                match allocation {
                    Some(KernelOwnedAllocation::NativeOriginal(allocation)) => {
                        crate::async_storage::native_read_batch(values, allocation)
                    }
                    _ => {
                        drop(values);
                        Err(ObjectStoreError::Corrupt)
                    }
                }
            }
            .await
            .map(|(values, allocation)| (values, Some(allocation)));
        }
        context.work.remaining(context.budget)?;

        self.inner
            .read_many(requests, context)
            .await
            .map(|(values, _allocation)| values)
            .map(|values| (values, None))
    }
}

async fn owned_cache(
    finish: Finish,
    token: &CancellationToken,
) -> Result<(CachedObjectStore<OwnedNativeStore>, ObjectId), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let (id, bytes) = object(b"owned");
    inner
        .put(
            id,
            bytes,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, token),
        )
        .await?;
    Ok((
        CachedObjectStore::new(OwnedNativeStore { inner, finish }, options(2, 64))?,
        id,
    ))
}

#[tokio::test]
async fn native_owned_cache_holds_original_buffer_until_final_byte_alias_drop()
-> Result<(), Box<dyn std::error::Error>> {
    let token = CancellationToken::new();
    let (cached, id) = owned_cache(Finish::Return, &token).await?;
    let work = NativeMaterializationWork::new(budget(), &token).map_err(|(error, _)| error)?;
    let read = {
        let mut __object_work = crate::AuthorityWork::native(&work, &token);
        let __object_result = cached.read(id, 5, &mut __object_work).await;
        __object_result
    }?;
    let alias = read.bytes.clone();
    let held = work.live_allocation_bytes();
    assert!(held > NativeMaterializationWork::accounting_state_bytes() + 5);
    drop(read);
    assert_eq!(work.live_allocation_bytes(), held);
    drop({
        let mut __object_work = crate::AuthorityWork::native(&work, &token);
        let __object_result = cached.read(id, 5, &mut __object_work).await;
        __object_result
    }?);
    cached.clear()?;
    assert_eq!(work.live_allocation_bytes(), held);
    assert_eq!(alias.as_ref(), b"owned");
    drop(alias);
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn native_backend_denial_error_panic_and_future_drop_keep_actual_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let token = CancellationToken::new();
    let (cached, id) = owned_cache(Finish::Return, &token).await?;
    let mut denied_budget = budget();
    denied_budget.backend_read_operations = 0;
    let denied =
        NativeMaterializationWork::new(denied_budget, &token).map_err(|(error, _)| error)?;
    assert!(matches!(
        {
            let mut __object_work = crate::AuthorityWork::native(&denied, &token);
            let __object_result = cached.read(id, 5, &mut __object_work).await;
            __object_result
        },
        Err(ObjectStoreError::Native(
            NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded {
                counter: "backend_read_operations",
                ..
            })
        ))
    ));
    assert_eq!(denied.receipt().backend_read_operations, 0);
    assert_eq!(denied.receipt().bytes_copied, 0);
    assert_eq!(
        denied.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    assert_eq!(cached.stats()?.in_flight, 0);
    for finish in [Finish::Error, Finish::Panic, Finish::Pending] {
        let (cached, id) = owned_cache(finish, &token).await?;
        let work = NativeMaterializationWork::new(budget(), &token).map_err(|(error, _)| error)?;
        match finish {
            Finish::Error => assert!(matches!(
                {
                    let mut __object_work = crate::AuthorityWork::native(&work, &token);
                    let __object_result = cached.read(id, 5, &mut __object_work).await;
                    __object_result
                },
                Err(ObjectStoreError::Native(
                    NativeTreeMaterializationError::InvalidLayout
                ))
            )),
            Finish::Panic => assert!(
                std::panic::AssertUnwindSafe(async {
                    let mut __object_work = crate::AuthorityWork::native(&work, &token);
                    let __object_result = cached.read(id, 5, &mut __object_work).await;
                    __object_result
                })
                .catch_unwind()
                .await
                .is_err()
            ),
            Finish::Pending => {
                let mut future = Box::pin(async {
                    let mut __object_work = crate::AuthorityWork::native(&work, &token);
                    let __object_result = cached.read(id, 5, &mut __object_work).await;
                    __object_result
                });
                assert!(
                    future
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                assert!(
                    work.live_allocation_bytes()
                        > NativeMaterializationWork::accounting_state_bytes()
                );
                drop(future);
            }
            Finish::Return => unreachable!(),
        }
        assert_eq!(
            work.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes()
        );
        assert_eq!(cached.stats()?.in_flight, 0);
    }
    Ok(())
}

#[tokio::test]
async fn native_batch_owned_alias_and_failed_prefix_keep_real_buffer_guards()
-> Result<(), Box<dyn std::error::Error>> {
    let token = CancellationToken::new();
    let (cached, id) = owned_cache(Finish::Return, &token).await?;
    let requests = [ObjectReadRequest {
        object_id: id,
        maximum_bytes: 5,
    }; 2];
    let work = NativeMaterializationWork::new(budget(), &token).map_err(|(error, _)| error)?;
    let batch = {
        let mut __object_work = crate::AuthorityWork::native(&work, &token);
        let __object_result = cached
            .read_many(&requests, &mut __object_work)
            .await
            .map(|(values, allocation)| (values, allocation.expect("native batch allocation")));
        __object_result
    }?;
    let alias = batch.0[0].bytes.clone();
    drop(batch);
    let held = work.live_allocation_bytes();
    assert!(held > NativeMaterializationWork::accounting_state_bytes() + 5);
    cached.clear()?;
    assert_eq!(work.live_allocation_bytes(), held);
    drop(alias);
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    for finish in [Finish::Error, Finish::Panic, Finish::Pending] {
        let (cached, id) = owned_cache(finish, &token).await?;
        let requests = [ObjectReadRequest {
            object_id: id,
            maximum_bytes: 5,
        }; 2];
        let work = NativeMaterializationWork::new(budget(), &token).map_err(|(error, _)| error)?;
        match finish {
            Finish::Error => assert!(matches!(
                {
                    let mut __object_work = crate::AuthorityWork::native(&work, &token);
                    let __object_result = cached
                        .read_many(&requests, &mut __object_work)
                        .await
                        .map(|(values, allocation)| {
                            (values, allocation.expect("native batch allocation"))
                        });
                    __object_result
                },
                Err(ObjectStoreError::Native(
                    NativeTreeMaterializationError::InvalidLayout
                ))
            )),
            Finish::Panic => assert!(
                std::panic::AssertUnwindSafe(async {
                    let mut __object_work = crate::AuthorityWork::native(&work, &token);
                    let __object_result = cached
                        .read_many(&requests, &mut __object_work)
                        .await
                        .map(|(values, allocation)| {
                            (values, allocation.expect("native batch allocation"))
                        });
                    __object_result
                })
                .catch_unwind()
                .await
                .is_err()
            ),
            Finish::Pending => {
                let mut future = Box::pin(async {
                    let mut __object_work = crate::AuthorityWork::native(&work, &token);
                    let __object_result = cached
                        .read_many(&requests, &mut __object_work)
                        .await
                        .map(|(values, allocation)| {
                            (values, allocation.expect("native batch allocation"))
                        });
                    __object_result
                });
                assert!(
                    future
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                drop(future);
            }
            Finish::Return => unreachable!(),
        }
        assert_eq!(
            work.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes()
        );
        assert_eq!(cached.stats()?.in_flight, 0);
    }
    let (cached, id) = owned_cache(Finish::Return, &token).await?;
    let mut limit = budget();
    limit.allocation_operations = 1;
    let work = NativeMaterializationWork::new(limit, &token).map_err(|(error, _)| error)?;
    assert!(matches!(
        {
            let authority_input_0 = &[ObjectReadRequest {
                object_id: id,
                maximum_bytes: 5,
            }];

            let mut __object_work = crate::AuthorityWork::native(&work, &token);
            let __object_result = cached
                .read_many(authority_input_0, &mut __object_work)
                .await
                .map(|(values, allocation)| (values, allocation.expect("native batch allocation")));
            __object_result
        },
        Err(ObjectStoreError::Native(
            NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded {
                counter: "allocation_operations",
                ..
            })
        ))
    ));
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().bytes_copied, 0);
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    assert_eq!(cached.stats()?.in_flight, 0);
    Ok(())
}
