use super::*;
use crate::async_storage::poll_ready;
use crate::kernel::DecodeLimits;
use crate::{MemoryObjectStore, ObjectKind, object_digest};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn object(value: &'static [u8]) -> (ObjectId, Bytes) {
    let bytes = Bytes::from_static(value);
    (
        ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &bytes),
        },
        bytes,
    )
}

fn options(entries: u32, bytes: u64) -> ObjectCacheOptions {
    ObjectCacheOptions {
        maximum_entries: entries,
        maximum_bytes: bytes,
        maximum_in_flight: 8,
        maximum_waiters_per_object: 8,
    }
}

#[test]
fn cache_is_bounded_lru_authenticated_and_physically_observable()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (first_id, first_bytes) = object(b"first");
    let (second_id, second_bytes) = object(b"second");
    for (object_id, bytes) in [(first_id, first_bytes), (second_id, second_bytes)] {
        poll_ready(inner.put(object_id, bytes, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("memory object put blocked")??;
    }
    let cached = CachedObjectStore::new(inner, options(1, 8))?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let first = runtime.block_on(cached.read(first_id, 5, WorkBudget::UNBOUNDED, &cancellation))?;
    let warm = runtime.block_on(cached.read(first_id, 5, WorkBudget::UNBOUNDED, &cancellation))?;
    let second =
        runtime.block_on(cached.read(second_id, 6, WorkBudget::UNBOUNDED, &cancellation))?;
    let reloaded =
        runtime.block_on(cached.read(first_id, 5, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(first.value.bytes, warm.value.bytes);
    assert_eq!(second.value.bytes.as_ref(), b"second");
    assert_eq!(reloaded.value.bytes.as_ref(), b"first");
    assert_eq!(first.work.backend_read_operations, 1);
    assert_eq!(warm.work.backend_read_operations, 0);
    assert_eq!(warm.value.retention, ObjectReadRetention::Shared);
    assert_eq!(
        cached.stats()?,
        ObjectCacheStats {
            hits: 1,
            decoded_hits: 0,
            misses: 3,
            coalesced_reads: 0,
            evictions: 2,
            resident_entries: 1,
            resident_bytes: 5,
            resident_canonical_objects: 1,
            resident_canonical_bytes: 5,
            resident_decoded_pages: 0,
            resident_decoded_bytes: 0,
            in_flight: 0,
        }
    );
    Ok(())
}

#[test]
fn cache_bounds_cancellation_and_clear_fail_before_hidden_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        CachedObjectStore::new(MemoryObjectStore::default(), options(0, 1)).err(),
        Some(ObjectCacheConfigError::ZeroBound)
    );
    let inner = MemoryObjectStore::default();
    let (object_id, bytes) = object(b"bounded");
    let active = CancellationToken::new();
    poll_ready(inner.put(object_id, bytes, WorkBudget::UNBOUNDED, &active))
        .ok_or("memory object put blocked")??;
    let cached = CachedObjectStore::new(inner, options(1, 16))?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let failure = runtime
        .block_on(cached.read(object_id, 7, WorkBudget::UNBOUNDED, &cancelled))
        .err()
        .ok_or("cancelled cache read succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(cached.stats()?, ObjectCacheStats::default());
    runtime.block_on(cached.read(object_id, 7, WorkBudget::UNBOUNDED, &active))?;
    cached.clear()?;
    assert_eq!(cached.stats()?.resident_entries, 0);
    assert_eq!(cached.stats()?.resident_bytes, 0);
    Ok(())
}

#[test]
fn put_contains_and_decoded_pages_share_one_deterministic_lru()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cached = CachedObjectStore::new(inner, options(2, 64))?;
    let cancellation = CancellationToken::new();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let (canonical_id, canonical_bytes) = object(b"canonical");
    runtime.block_on(cached.put(
        canonical_id,
        canonical_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(
        poll_ready(
            cached
                .inner()
                .contains(canonical_id, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("inner contains blocked")??
        .value
    );
    assert!(
        runtime
            .block_on(cached.contains(canonical_id, WorkBudget::UNBOUNDED, &cancellation,))?
            .value
    );
    let (missing_id, _) = object(b"missing");
    assert!(
        !runtime
            .block_on(cached.contains(missing_id, WorkBudget::UNBOUNDED, &cancellation))?
            .value
    );

    let first_key = DecodedCacheKey::new::<u64>(canonical_id, DecodeLimits::default());
    let first_value = DecodedCacheValue {
        value: Arc::new(41_u64),
        logical_bytes: 8,
    };
    assert!(matches!(
        cached.decoded_cache_admit(first_key, first_value)?,
        DecodedCacheAdmission::Shared(_)
    ));
    let cached_value = cached
        .decoded_cache_get(first_key)?
        .ok_or("decoded cache admission was not observable")?;
    assert_eq!(cached_value.value.downcast_ref::<u64>(), Some(&41));

    let second_key = DecodedCacheKey::new::<u32>(missing_id, DecodeLimits::default());
    assert!(matches!(
        cached.decoded_cache_admit(
            second_key,
            DecodedCacheValue {
                value: Arc::new(7_u32),
                logical_bytes: 4,
            },
        )?,
        DecodedCacheAdmission::Shared(_)
    ));
    let stats = cached.stats()?;
    assert_eq!(stats.evictions, 1);
    assert_eq!(stats.resident_canonical_objects, 0);
    assert_eq!(stats.resident_decoded_pages, 2);

    let replacement = DecodedCacheValue {
        value: Arc::new(99_u64),
        logical_bytes: 8,
    };
    let DecodedCacheAdmission::Shared(existing) =
        cached.decoded_cache_admit(first_key, replacement)?
    else {
        return Err("existing decoded page was not shared".into());
    };
    assert_eq!(existing.value.downcast_ref::<u64>(), Some(&41));

    let oversized_key = DecodedCacheKey::new::<u16>(missing_id, DecodeLimits::default());
    assert!(matches!(
        cached.decoded_cache_admit(
            oversized_key,
            DecodedCacheValue {
                value: Arc::new(5_u16),
                logical_bytes: 65,
            },
        )?,
        DecodedCacheAdmission::Uncached(_)
    ));
    Ok(())
}

#[test]
fn flight_waiter_bound_and_completion_are_deterministic() {
    struct WakeIdentity;

    impl std::task::Wake for WakeIdentity {
        fn wake(self: Arc<Self>) {}
    }

    let flight = Flight::new(1);
    let first_waker = Waker::from(Arc::new(WakeIdentity));
    let second_waker = Waker::from(Arc::new(WakeIdentity));
    let first_context = &mut Context::from_waker(&first_waker);
    let second_context = &mut Context::from_waker(&second_waker);
    assert!(flight.poll(first_context).is_pending());
    assert!(matches!(
        flight.poll(second_context),
        Poll::Ready(Err(ObjectStoreError::Rejected(message)))
            if message.contains("waiter bound")
    ));
    flight.finish(Some(Bytes::from_static(b"ready")));
    assert!(matches!(
        flight.poll(first_context),
        Poll::Ready(Ok(Some(bytes))) if bytes.as_ref() == b"ready"
    ));
    flight.finish(None);
}

#[test]
fn missing_scalar_and_batch_reads_release_every_flight_for_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (present_id, present_bytes) = object(b"present");
    poll_ready(inner.put(
        present_id,
        present_bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory object put blocked")??;
    let (missing_id, _) = object(b"absent");
    let cached = CachedObjectStore::new(inner, options(4, 64))?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;

    for _ in 0..2 {
        assert!(
            runtime
                .block_on(cached.read(missing_id, 6, WorkBudget::UNBOUNDED, &cancellation,))
                .is_err()
        );
        assert_eq!(cached.stats()?.in_flight, 0);
    }
    let requests = [
        ObjectReadRequest {
            object_id: present_id,
            maximum_bytes: 7,
        },
        ObjectReadRequest {
            object_id: missing_id,
            maximum_bytes: 6,
        },
    ];
    assert!(
        runtime
            .block_on(cached.read_many(&requests, WorkBudget::UNBOUNDED, &cancellation,))
            .is_err()
    );
    assert_eq!(cached.stats()?.in_flight, 0);
    let retry =
        runtime.block_on(cached.read(present_id, 7, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(retry.value.bytes, present_bytes);

    let empty = runtime
        .block_on(cached.read_many(&[], WorkBudget::UNBOUNDED, &cancellation))
        .err()
        .ok_or("empty cache batch succeeded")?;
    assert!(matches!(empty.error, ObjectStoreError::Rejected(_)));
    assert_eq!(*empty.work, WorkCounters::default());
    Ok(())
}

struct Gate {
    open: AtomicBool,
    waiters: Mutex<Vec<Waker>>,
}

impl Gate {
    fn new() -> Self {
        Self {
            open: AtomicBool::new(false),
            waiters: Mutex::new(Vec::new()),
        }
    }

    async fn wait(&self) {
        poll_fn(|context| {
            if self.open.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            if let Ok(mut waiters) = self.waiters.lock()
                && !waiters
                    .iter()
                    .any(|waiter| waiter.will_wake(context.waker()))
            {
                waiters.push(context.waker().clone());
            }
            Poll::Pending
        })
        .await;
    }

    fn release(&self) {
        self.open.store(true, Ordering::Release);
        let waiters = self
            .waiters
            .lock()
            .map(|mut waiters| std::mem::take(&mut *waiters))
            .unwrap_or_default();
        for waiter in waiters {
            waiter.wake();
        }
    }
}

#[derive(Clone)]
struct GatedStore {
    inner: Arc<MemoryObjectStore>,
    gate: Arc<Gate>,
    reads: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct CountingStore {
    inner: Arc<MemoryObjectStore>,
    scalar_reads: Arc<AtomicUsize>,
    batch_reads: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct ShortBatchStore {
    inner: Arc<MemoryObjectStore>,
}

impl AsyncObjectStore for CountingStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.inner.put(object_id, bytes, budget, cancellation).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        self.scalar_reads.fetch_add(1, Ordering::AcqRel);
        self.inner
            .read(object_id, maximum_bytes, budget, cancellation)
            .await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        self.batch_reads.fetch_add(1, Ordering::AcqRel);
        self.inner.read_many(requests, budget, cancellation).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        self.inner.contains(object_id, budget, cancellation).await
    }
}

impl AsyncObjectStore for ShortBatchStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.inner.put(object_id, bytes, budget, cancellation).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        self.inner
            .read(object_id, maximum_bytes, budget, cancellation)
            .await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        self.inner
            .read_many(&requests[..1], budget, cancellation)
            .await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        self.inner.contains(object_id, budget, cancellation).await
    }
}

fn counting_store(inner: MemoryObjectStore) -> (CountingStore, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let scalar_reads = Arc::new(AtomicUsize::new(0));
    let batch_reads = Arc::new(AtomicUsize::new(0));
    (
        CountingStore {
            inner: Arc::new(inner),
            scalar_reads: Arc::clone(&scalar_reads),
            batch_reads: Arc::clone(&batch_reads),
        },
        scalar_reads,
        batch_reads,
    )
}

impl AsyncObjectStore for GatedStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.inner.put(object_id, bytes, budget, cancellation).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        self.gate.wait().await;
        self.inner
            .read(object_id, maximum_bytes, budget, cancellation)
            .await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        crate::async_storage::read_many_sequential_async(self, requests, budget, cancellation).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        self.inner.contains(object_id, budget, cancellation).await
    }
}

#[test]
fn concurrent_reads_share_one_backend_request_and_dropped_leaders_recover()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"single-flight");
    poll_ready(inner.put(
        object_id,
        bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory object put blocked")??;
    let gate = Arc::new(Gate::new());
    let reads = Arc::new(AtomicUsize::new(0));
    let cached = CachedObjectStore::new(
        GatedStore {
            inner: Arc::new(inner),
            gate: Arc::clone(&gate),
            reads: Arc::clone(&reads),
        },
        options(4, 64),
    )?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let (first, second) = runtime.block_on(async {
        let first = cached.read(object_id, 13, WorkBudget::UNBOUNDED, &cancellation);
        let second = cached.read(object_id, 13, WorkBudget::UNBOUNDED, &cancellation);
        let release = async {
            while reads.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
            gate.release();
        };
        let (first, second, ()) = tokio::join!(first, second, release);
        (first, second)
    });
    let first = first?;
    let second = second?;
    assert_eq!(first.value.bytes, bytes);
    assert_eq!(second.value.bytes, bytes);
    assert_eq!(reads.load(Ordering::Acquire), 1);
    assert_eq!(
        first.work.backend_read_operations + second.work.backend_read_operations,
        1
    );
    assert_eq!(cached.stats()?.coalesced_reads, 1);
    Ok(())
}

#[test]
fn concurrent_scalar_and_batch_reads_share_success_and_retry_lost_results()
-> Result<(), Box<dyn std::error::Error>> {
    for (object_id, body) in [object(b"scalar-batch"), object(b"missing-batch")]
        .into_iter()
        .enumerate()
    {
        let inner = MemoryObjectStore::default();
        let cancellation = CancellationToken::new();
        let (identity, bytes) = body;
        if object_id == 0 {
            poll_ready(inner.put(
                identity,
                bytes.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("memory object put blocked")??;
        }
        let gate = Arc::new(Gate::new());
        let reads = Arc::new(AtomicUsize::new(0));
        let cached = CachedObjectStore::new(
            GatedStore {
                inner: Arc::new(inner),
                gate: Arc::clone(&gate),
                reads: Arc::clone(&reads),
            },
            options(4, 64),
        )?;
        let requests = [ObjectReadRequest {
            object_id: identity,
            maximum_bytes: u64::try_from(bytes.len())?,
        }];
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let (scalar, batch) = runtime.block_on(async {
            let scalar = cached.read(
                identity,
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                WorkBudget::UNBOUNDED,
                &cancellation,
            );
            let batch = cached.read_many(&requests, WorkBudget::UNBOUNDED, &cancellation);
            let release = async {
                while reads.load(Ordering::Acquire) == 0 {
                    tokio::task::yield_now().await;
                }
                gate.release();
            };
            let (scalar, batch, ()) = tokio::join!(scalar, batch, release);
            (scalar, batch)
        });
        if object_id == 0 {
            assert_eq!(scalar?.value.bytes, bytes);
            assert_eq!(batch?.value[0].bytes, bytes);
            assert_eq!(reads.load(Ordering::Acquire), 1);
        } else {
            assert!(scalar.is_err());
            assert!(batch.is_err());
            assert_eq!(reads.load(Ordering::Acquire), 2);
        }
        assert_eq!(cached.stats()?.in_flight, 0);
        assert_eq!(cached.stats()?.coalesced_reads, 1);
    }
    Ok(())
}

#[test]
fn malformed_backend_batch_cardinality_releases_all_flights()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = Arc::new(MemoryObjectStore::default());
    let cancellation = CancellationToken::new();
    let objects = [object(b"first-short"), object(b"second-short")];
    for (object_id, bytes) in &objects {
        poll_ready(inner.put(
            *object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("memory object put blocked")??;
    }
    let cached = CachedObjectStore::new(ShortBatchStore { inner }, options(4, 64))?;
    let requests = objects.map(|(object_id, bytes)| ObjectReadRequest {
        object_id,
        maximum_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    });
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let failure = runtime
        .block_on(cached.read_many(&requests, WorkBudget::UNBOUNDED, &cancellation))
        .err()
        .ok_or("short backend batch was accepted")?;
    assert!(matches!(failure.error, ObjectStoreError::Corrupt));
    assert_eq!(cached.stats()?.in_flight, 0);
    Ok(())
}

#[test]
fn mixed_batches_preserve_backend_multiget_order_duplicates_and_exact_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let objects = [object(b"warm"), object(b"second"), object(b"third")];
    for (object_id, bytes) in &objects {
        poll_ready(inner.put(
            *object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("memory object put blocked")??;
    }
    let (counting, scalar_reads, batch_reads) = counting_store(inner);
    let cached = CachedObjectStore::new(counting, options(8, 128))?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(cached.read(objects[0].0, 4, WorkBudget::UNBOUNDED, &cancellation))?;
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
    let receipt =
        runtime.block_on(cached.read_many(&requests, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(
        receipt
            .value
            .iter()
            .map(|value| value.bytes.as_ref())
            .collect::<Vec<_>>(),
        vec![b"warm".as_slice(), b"second", b"second", b"third"]
    );
    assert_eq!(receipt.work.backend_read_operations, 1);
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
    let failure = runtime
        .block_on(cached.read_many(&unequal, WorkBudget::UNBOUNDED, &cancellation))
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
    Ok(())
}

#[test]
fn oversized_objects_bypass_residency_while_clones_share_warm_entries()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (small_id, small_bytes) = object(b"small");
    let (large_id, large_bytes) = object(b"larger-than-cache");
    for (object_id, bytes) in [(small_id, small_bytes), (large_id, large_bytes)] {
        poll_ready(inner.put(object_id, bytes, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("memory object put blocked")??;
    }
    let (counting, scalar_reads, _) = counting_store(inner);
    let cached = CachedObjectStore::new(counting, options(4, 8))?;
    let clone = cached.clone();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(cached.read(small_id, 5, WorkBudget::UNBOUNDED, &cancellation))?;
    let warm = runtime.block_on(clone.read(small_id, 5, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(warm.work.backend_read_operations, 0);
    runtime.block_on(cached.read(large_id, 17, WorkBudget::UNBOUNDED, &cancellation))?;
    runtime.block_on(clone.read(large_id, 17, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(scalar_reads.load(Ordering::Acquire), 3);
    assert_eq!(cached.stats()?.resident_entries, 1);
    assert_eq!(cached.stats()?.resident_bytes, 5);
    Ok(())
}

#[test]
fn dropped_leaders_release_flights_and_retry_without_a_stale_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"drop-safe");
    poll_ready(inner.put(
        object_id,
        bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory object put blocked")??;
    let gate = Arc::new(Gate::new());
    let reads = Arc::new(AtomicUsize::new(0));
    let cached = CachedObjectStore::new(
        GatedStore {
            inner: Arc::new(inner),
            gate: Arc::clone(&gate),
            reads: Arc::clone(&reads),
        },
        options(4, 64),
    )?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(async {
        {
            let read = cached.read(object_id, 9, WorkBudget::UNBOUNDED, &cancellation);
            tokio::pin!(read);
            poll_fn(|context| match read.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(Ok(())),
                Poll::Ready(_) => Poll::Ready(Err(std::io::Error::other(
                    "gated leader completed before cancellation",
                ))),
            })
            .await?;
        }
        assert_eq!(cached.stats()?.in_flight, 0);
        gate.release();
        let retry = cached
            .read(object_id, 9, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert_eq!(retry.value.bytes, bytes);
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    assert_eq!(reads.load(Ordering::Acquire), 2);
    Ok(())
}

#[test]
fn failed_batch_planning_releases_every_unstarted_flight_without_backend_io()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let objects = [object(b"one"), object(b"two")];
    for (object_id, bytes) in &objects {
        poll_ready(inner.put(
            *object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("memory object put blocked")??;
    }
    let (counting, scalar_reads, batch_reads) = counting_store(inner);
    let cached = CachedObjectStore::new(
        counting,
        ObjectCacheOptions {
            maximum_entries: 4,
            maximum_bytes: 64,
            maximum_in_flight: 1,
            maximum_waiters_per_object: 1,
        },
    )?;
    let requests = objects.map(|(object_id, bytes)| ObjectReadRequest {
        object_id,
        maximum_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    });
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let failure = runtime
        .block_on(cached.read_many(&requests, WorkBudget::UNBOUNDED, &cancellation))
        .err()
        .ok_or("over-capacity batch unexpectedly succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Rejected(_)));
    assert_eq!(cached.stats()?.in_flight, 0);
    assert_eq!(scalar_reads.load(Ordering::Acquire), 0);
    assert_eq!(batch_reads.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn fs_composition_consumes_the_cache_without_a_second_runtime_or_adapter()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"embedded-fs");
    poll_ready(inner.put(
        object_id,
        bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory object put blocked")??;
    let (counting, scalar_reads, _) = counting_store(inner);
    let cached = CachedObjectStore::new(counting, options(8, 128))?;
    let fs = crate::Fs::new(
        crate::MemoryAuthorityStore::default(),
        cached,
        crate::EmbeddedCapabilities::MEMORY,
    );
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let first =
        runtime.block_on(fs.export_object(object_id, 11, WorkBudget::UNBOUNDED, &cancellation))?;
    let warm =
        runtime.block_on(fs.export_object(object_id, 11, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(first.value.bytes, bytes);
    assert_eq!(warm.value.bytes, bytes);
    assert_eq!(first.work.backend_read_operations, 1);
    assert_eq!(warm.work.backend_read_operations, 0);
    assert_eq!(scalar_reads.load(Ordering::Acquire), 1);

    let (imported_id, imported_bytes) = object(b"facade-import");
    let imported = runtime.block_on(fs.import_object(
        imported_id,
        imported_bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(imported.value, ());
    assert_eq!(imported.work.backend_write_operations, 1);
    let exported = runtime.block_on(fs.export_object(
        imported_id,
        13,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(exported.value.bytes, imported_bytes);
    assert_eq!(exported.work.backend_read_operations, 0);
    Ok(())
}

#[test]
fn decoded_eviction_poison_recovery_and_failure_accounting_are_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let cached = CachedObjectStore::new(MemoryObjectStore::default(), options(1, 64))?;
    let (first_id, first_bytes) = object(b"first");
    let (second_id, _) = object(b"second");
    let first_key = DecodedCacheKey::new::<u64>(first_id, DecodeLimits::default());
    let second_key = DecodedCacheKey::new::<u32>(second_id, DecodeLimits::default());
    assert!(matches!(
        cached.decoded_cache_admit(
            first_key,
            DecodedCacheValue {
                value: Arc::new(1_u64),
                logical_bytes: 8,
            },
        )?,
        DecodedCacheAdmission::Shared(_)
    ));
    assert!(matches!(
        cached.decoded_cache_admit(
            second_key,
            DecodedCacheValue {
                value: Arc::new(2_u32),
                logical_bytes: 4,
            },
        )?,
        DecodedCacheAdmission::Shared(_)
    ));
    assert!(cached.decoded_cache_get(first_key)?.is_none());
    assert_eq!(
        cached
            .decoded_cache_get(second_key)?
            .and_then(|value| value.value.downcast_ref::<u32>().copied()),
        Some(2)
    );
    let stats = cached.stats()?;
    assert_eq!(stats.evictions, 1);
    assert_eq!(stats.resident_decoded_pages, 1);

    cached.clear()?;
    assert!(cached.insert(first_id, first_bytes.clone())?);
    assert!(cached.insert(first_id, first_bytes.clone())?);
    let conflicting = cached
        .insert(first_id, Bytes::from_static(b"different"))
        .err()
        .ok_or("conflicting immutable cache bytes were accepted")?;
    assert!(matches!(conflicting, ObjectStoreError::Corrupt));

    let prior = WorkCounters {
        bytes_hashed: u64::MAX,
        ..WorkCounters::default()
    };
    let combined = combine_failure(
        prior,
        ObjectFailure::new(
            ObjectStoreError::Missing,
            WorkCounters {
                bytes_hashed: 1,
                ..WorkCounters::default()
            },
        ),
    );
    assert!(matches!(
        combined.error,
        ObjectStoreError::Work(crate::performance::WorkError::Overflow)
    ));
    assert_eq!(*combined.work, prior);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = poll_ready(wait_for_flight(&Flight::new(1), &cancellation))
        .ok_or("pre-cancelled flight wait blocked")?
        .err()
        .ok_or("pre-cancelled flight wait succeeded")?;
    assert!(matches!(cancelled, ObjectStoreError::Cancelled));

    let flight = Flight::new(1);
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = match flight.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        std::panic::resume_unwind(Box::new("poison flight state"));
    }));
    assert!(poisoned.is_err());
    flight.finish(Some(Bytes::from_static(b"must-not-survive-poison")));
    let flight_state = match flight.state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    assert!(matches!(&*flight_state, FlightState::Complete(None)));

    let completed = Flight::new(1);
    completed.finish(Some(Bytes::from_static(b"complete")));
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = match completed.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        std::panic::resume_unwind(Box::new("poison completed flight state"));
    }));
    assert!(poisoned.is_err());
    completed.finish(None);
    Ok(())
}

#[test]
fn abandoned_scalar_and_batch_followers_retry_through_one_fresh_leader()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let cancellation = CancellationToken::new();

    for batch in [false, true] {
        let inner = MemoryObjectStore::default();
        let (object_id, bytes) = object(if batch {
            b"batch-retry"
        } else {
            b"scalar-retry"
        });
        poll_ready(inner.put(
            object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("memory object put blocked")??;
        let cached = CachedObjectStore::new(inner, options(4, 64))?;
        let abandoned = Arc::new(Flight::new(1));
        abandoned.finish(None);
        {
            let mut state = cached.lock_state()?;
            state.flights.insert(object_id, Arc::clone(&abandoned));
        }

        if batch {
            let receipt = runtime.block_on(cached.read_many(
                &[ObjectReadRequest {
                    object_id,
                    maximum_bytes: u64::try_from(bytes.len())?,
                }],
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))?;
            assert_eq!(receipt.value[0].bytes, bytes);
        } else {
            let receipt = runtime.block_on(cached.read(
                object_id,
                u64::try_from(bytes.len())?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))?;
            assert_eq!(receipt.value.bytes, bytes);
        }
        assert_eq!(cached.stats()?.coalesced_reads, 1);
        assert_eq!(cached.stats()?.in_flight, 0);
    }
    Ok(())
}
