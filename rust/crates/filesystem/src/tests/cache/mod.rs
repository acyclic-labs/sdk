use super::*;
use crate::kernel::DecodeLimits;
use crate::{MemoryObjectStore, ObjectKind, object_digest};
use acyclic_stream::OwnedBytes;
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
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self.inner.put(object_id, bytes, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
                self.scalar_reads.fetch_add(1, Ordering::AcqRel);
                {
                    let mut __object_work = crate::AuthorityWork::native(work, cancellation);
                    let __object_result = self
                        .inner
                        .read(object_id, maximum_bytes, &mut __object_work)
                        .await;
                    __object_result
                }
            }
            .await;
        }
        let budget = context.read_budget()?;
        let cancellation = context.cancellation;
        let result = async {
            self.scalar_reads.fetch_add(1, Ordering::AcqRel);
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self
                    .inner
                    .read(object_id, maximum_bytes, &mut __object_work)
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
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
                self.batch_reads.fetch_add(1, Ordering::AcqRel);
                {
                    let mut __object_work = crate::AuthorityWork::native(work, cancellation);
                    let __object_result = self
                        .inner
                        .read_many(requests, &mut __object_work)
                        .await
                        .map(|(values, allocation)| {
                            (values, allocation.expect("native batch allocation"))
                        });
                    __object_result
                }
            }
            .await
            .map(|(values, allocation)| (values, Some(allocation)));
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            self.batch_reads.fetch_add(1, Ordering::AcqRel);
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self
                    .inner
                    .read_many(requests, &mut __object_work)
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
        object_id: ObjectId,
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self.inner.contains(object_id, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
}

impl AsyncObjectStore for ShortBatchStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self.inner.put(object_id, bytes, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self
                    .inner
                    .read(object_id, maximum_bytes, &mut __object_work)
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
                let authority_input_0 = &requests[..1];

                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self
                    .inner
                    .read_many(authority_input_0, &mut __object_work)
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
        object_id: ObjectId,
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self.inner.contains(object_id, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
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
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self.inner.put(object_id, bytes, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.read_budget()?;
        let cancellation = context.cancellation;
        let result = async {
            self.reads.fetch_add(1, Ordering::AcqRel);
            self.gate.wait().await;
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self
                    .inner
                    .read(object_id, maximum_bytes, &mut __object_work)
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
        crate::async_storage::read_many_sequential(self, requests, context).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = self.inner.contains(object_id, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod native_cache;

mod batches;
mod flights;
mod residency;
