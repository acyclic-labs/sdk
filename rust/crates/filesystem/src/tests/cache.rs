use acyclic_stream::OwnedBytes;
use super::*;
use crate::async_storage::poll_ready;
use crate::kernel::DecodeLimits;
use crate::{MemoryObjectStore, ObjectKind, object_digest};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn object(value: &'static [u8]) -> (ObjectId, OwnedBytes) {
    let bytes = OwnedBytes::from_static(value);
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
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
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
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: None,
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
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
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
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: None,
            },
        )?,
        DecodedCacheAdmission::Uncached(_)
    ));
    Ok(())
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
        bytes: OwnedBytes,
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

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work(
        &self, object_id: ObjectId, maximum_bytes: u64,
        work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<NativeObjectRead, ObjectStoreError> {
        self.scalar_reads.fetch_add(1, Ordering::AcqRel);
        self.inner.read_with_native_work(object_id, maximum_bytes, work, cancellation).await
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

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_many_with_native_work(
        &self, requests: &[ObjectReadRequest],
        work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<NativeObjectReadBatch, ObjectStoreError> {
        self.batch_reads.fetch_add(1, Ordering::AcqRel);
        self.inner.read_many_with_native_work(requests, work, cancellation).await
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
        bytes: OwnedBytes,
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
        bytes: OwnedBytes,
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
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: None,
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
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: None,
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
        .insert(first_id, OwnedBytes::from_static(b"different"))
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
    flight.finish(Some(OwnedBytes::from_static(b"must-not-survive-poison")));
    let flight_state = match flight.state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    assert!(matches!(&*flight_state, FlightState::Complete(None)));

    let completed = Flight::new(1);
    completed.finish(Some(OwnedBytes::from_static(b"complete")));
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

#[cfg(not(target_arch = "wasm32"))]
mod native_cache {
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
        source: &[u8], work: &NativeMaterializationWork,
    ) -> Result<OwnedBytes, ObjectStoreError> {
        let mut allocation = work.claim(source.len() as u64)?;
        let copy = work.reserve_work(crate::WorkCounters { bytes_copied: source.len() as u64, ..crate::WorkCounters::UNCHARGED })?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(source.len())
            .map_err(|error| ObjectStoreError::Native(NativeTreeMaterializationError::AllocationFailed(error)))?;
        allocation.reconcile_capacity(bytes.capacity() as u64)?;
        bytes.resize(bytes.capacity(), 0);
        let mut prepared = acyclic_stream::PreparedOwnedBytes::new(
            bytes, allocation, work.runtime_meter_owner(),
        ).map_err(|failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => work.runtime_error(error).into(),
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
            =>
                ObjectStoreError::Native(NativeTreeMaterializationError::InvalidLayout),
        })?;
        prepared.writable()[..source.len()].copy_from_slice(source);
        { let mut copy = copy; copy.record_completed(crate::WorkCounters { bytes_copied: source.len() as u64, ..crate::WorkCounters::UNCHARGED }) }?;
        prepared.freeze(source.len()).map_err(|_| ObjectStoreError::Corrupt)
    }

    #[derive(Clone, Copy)]
    enum Finish { Return, Error, Panic, Pending }

    struct OwnedNativeStore {
        inner: MemoryObjectStore,
        finish: Finish,
    }

    impl AsyncObjectStore for OwnedNativeStore {
        async fn put(
            &self, id: ObjectId, bytes: OwnedBytes, budget: WorkBudget, token: &CancellationToken,
        ) -> ObjectResult<()> {
            self.inner.put(id, bytes, budget, token).await
        }

        async fn read(
            &self, id: ObjectId, maximum: u64, budget: WorkBudget, token: &CancellationToken,
        ) -> ObjectResult<ObjectRead> {
            self.inner.read(id, maximum, budget, token).await
        }

        async fn contains(
            &self, id: ObjectId, budget: WorkBudget, token: &CancellationToken,
        ) -> ObjectResult<bool> {
            self.inner.contains(id, budget, token).await
        }

        async fn read_many(
            &self, requests: &[ObjectReadRequest], budget: WorkBudget, token: &CancellationToken,
        ) -> ObjectResult<Vec<ObjectRead>> {
            self.inner.read_many(requests, budget, token).await
        }

        async fn read_with_native_work(
            &self, id: ObjectId, maximum: u64, work: &NativeMaterializationWork, token: &CancellationToken,
        ) -> Result<NativeObjectRead, ObjectStoreError> {
            let source = self.inner.read_with_native_work(id, maximum, work, token).await?;
            let bytes = copied_native_bytes(&source.read().bytes, work)?;
            let logical_bytes = bytes.retained_capacity().ok_or(ObjectStoreError::Corrupt)? as u64;
            let read = NativeObjectRead::intrinsic(ObjectRead {
                bytes,
                retention: ObjectReadRetention::Owned { logical_bytes },
            }, work)?;
            match self.finish {
                Finish::Return => {}
                Finish::Error => return Err(ObjectStoreError::Native(NativeTreeMaterializationError::InvalidLayout)),
                Finish::Panic => panic!("native producer unwinds after its actual copy"),
                Finish::Pending => std::future::pending::<()>().await,
            }
            Ok(read)
        }

        async fn read_many_with_native_work(
            &self, requests: &[ObjectReadRequest], work: &NativeMaterializationWork, token: &CancellationToken,
        ) -> Result<NativeObjectReadBatch, ObjectStoreError> {
            let source = self.inner.read_many_with_native_work(requests, work, token).await?;
            let (values, allocation) = source.into_parts();
            let mut buffer = BatchBuffer {
                values, allocation: Some(KernelOwnedAllocation::NativeOriginal(allocation)),
            };
            for value in &mut buffer.values {
                let bytes = copied_native_bytes(&value.bytes, work)?;
                let logical_bytes = bytes.retained_capacity().ok_or(ObjectStoreError::Corrupt)? as u64;
                *value = NativeObjectRead::intrinsic(ObjectRead {
                    bytes,
                    retention: ObjectReadRetention::Owned { logical_bytes },
                }, work)?.into_read();
            }
            match self.finish {
                Finish::Return => {}
                Finish::Error => return Err(ObjectStoreError::Native(NativeTreeMaterializationError::InvalidLayout)),
                Finish::Panic => panic!("native batch producer unwinds after its actual copy"),
                Finish::Pending => std::future::pending::<()>().await,
            }
            let BatchBuffer { values, allocation } = buffer;
            match allocation {
                Some(KernelOwnedAllocation::NativeOriginal(allocation)) => NativeObjectReadBatch::new(values, allocation),
                _ => {
                    drop(values);
                    Err(ObjectStoreError::Corrupt)
                }
            }
        }
    }

    async fn owned_cache(finish: Finish, token: &CancellationToken)
    -> Result<(CachedObjectStore<OwnedNativeStore>, ObjectId), Box<dyn std::error::Error>> {
        let inner = MemoryObjectStore::default();
        let (id, bytes) = object(b"owned");
        inner.put(id, bytes, WorkBudget::UNBOUNDED, token).await?;
        Ok((CachedObjectStore::new(OwnedNativeStore { inner, finish }, options(2, 64))?, id))
    }

    #[tokio::test]
    async fn native_owned_cache_holds_original_buffer_until_final_byte_alias_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();
        let (cached, id) = owned_cache(Finish::Return, &token).await?;
        let work = NativeMaterializationWork::new(budget(), &token).map_err(|(error, _)| error)?;
        let read = cached.read_with_native_work(id, 5, &work, &token).await?;
        let alias = read.read().bytes.clone();
        let held = work.live_allocation_bytes();
        assert!(held > NativeMaterializationWork::accounting_state_bytes() + 5);
        drop(read);
        assert_eq!(work.live_allocation_bytes(), held);
        drop(cached.read_with_native_work(id, 5, &work, &token).await?);
        cached.clear()?;
        assert_eq!(work.live_allocation_bytes(), held);
        assert_eq!(alias.as_ref(), b"owned");
        drop(alias);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn native_backend_denial_error_panic_and_future_drop_keep_actual_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();
        let (cached, id) = owned_cache(Finish::Return, &token).await?;
        let mut denied_budget = budget();
        denied_budget.backend_read_operations = 0;
        let denied = NativeMaterializationWork::new(denied_budget, &token).map_err(|(error, _)| error)?;
        assert!(matches!(cached.read_with_native_work(id, 5, &denied, &token).await,
            Err(ObjectStoreError::Native(NativeTreeMaterializationError::Work(
                crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. })))));
        assert_eq!(denied.receipt().backend_read_operations, 0);
        assert_eq!(denied.receipt().bytes_copied, 0);
        assert_eq!(denied.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        assert_eq!(cached.stats()?.in_flight, 0);
        for finish in [Finish::Error, Finish::Panic, Finish::Pending] {
            let (cached, id) = owned_cache(finish, &token).await?;
            let work = NativeMaterializationWork::new(budget(), &token).map_err(|(error, _)| error)?;
            match finish {
                Finish::Error => assert!(matches!(cached.read_with_native_work(id, 5, &work, &token).await,
                    Err(ObjectStoreError::Native(NativeTreeMaterializationError::InvalidLayout)))),
                Finish::Panic => assert!(std::panic::AssertUnwindSafe(
                    cached.read_with_native_work(id, 5, &work, &token)).catch_unwind().await.is_err()),
                Finish::Pending => {
                    let mut future = Box::pin(cached.read_with_native_work(id, 5, &work, &token));
                    assert!(future.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
                    assert!(work.live_allocation_bytes() > NativeMaterializationWork::accounting_state_bytes());
                    drop(future);
                }
                Finish::Return => unreachable!(),
            }
            assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
            assert_eq!(cached.stats()?.in_flight, 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn native_batch_owned_alias_and_failed_prefix_keep_real_buffer_guards()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();
        let (cached, id) = owned_cache(Finish::Return, &token).await?;
        let requests = [ObjectReadRequest { object_id: id, maximum_bytes: 5 }; 2];
        let work = NativeMaterializationWork::new(budget(), &token).map_err(|(error, _)| error)?;
        let batch = cached.read_many_with_native_work(&requests, &work, &token).await?;
        let alias = batch.reads()[0].bytes.clone();
        drop(batch);
        let held = work.live_allocation_bytes();
        assert!(held > NativeMaterializationWork::accounting_state_bytes() + 5);
        cached.clear()?;
        assert_eq!(work.live_allocation_bytes(), held);
        drop(alias);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        for finish in [Finish::Error, Finish::Panic, Finish::Pending] {
            let (cached, id) = owned_cache(finish, &token).await?;
            let requests = [ObjectReadRequest { object_id: id, maximum_bytes: 5 }; 2];
            let work = NativeMaterializationWork::new(budget(), &token).map_err(|(error, _)| error)?;
            match finish {
                Finish::Error => assert!(matches!(cached.read_many_with_native_work(&requests, &work, &token).await,
                    Err(ObjectStoreError::Native(NativeTreeMaterializationError::InvalidLayout)))),
                Finish::Panic => assert!(std::panic::AssertUnwindSafe(
                    cached.read_many_with_native_work(&requests, &work, &token)).catch_unwind().await.is_err()),
                Finish::Pending => {
                    let mut future = Box::pin(cached.read_many_with_native_work(&requests, &work, &token));
                    assert!(future.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
                    drop(future);
                }
                Finish::Return => unreachable!(),
            }
            assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
            assert_eq!(cached.stats()?.in_flight, 0);
        }
        let (cached, id) = owned_cache(Finish::Return, &token).await?;
        let mut limit = budget();
        limit.allocation_operations = 1;
        let work = NativeMaterializationWork::new(limit, &token).map_err(|(error, _)| error)?;
        assert!(matches!(cached.read_many_with_native_work(
            &[ObjectReadRequest { object_id: id, maximum_bytes: 5 }], &work, &token,
        ).await, Err(ObjectStoreError::Native(NativeTreeMaterializationError::Work(
            crate::WorkError::BudgetExceeded { counter: "allocation_operations", .. })))));
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().bytes_copied, 0);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        assert_eq!(cached.stats()?.in_flight, 0);
        Ok(())
    }

}
