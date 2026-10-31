use super::*;
use crate::kernel::DecodeLimits;
use crate::{MemoryObjectStore, ObjectKind, object_digest};
use std::sync::atomic::{AtomicUsize, Ordering};

fn object(value: &'static [u8]) -> (ObjectId, OwnedBytes) {
    let bytes = OwnedBytes::from_static(value);
    (
        ObjectId {
            storage: None,
            kind: ObjectKind::BlobChunk,
            digest: object_digest(&bytes),
        },
        bytes,
    )
}

fn options(entries: u32, bytes: u64) -> ObjectCacheOptions {
    ObjectCacheOptions {
        maximum_entries: entries,
        maximum_bytes: bytes,
    }
}

#[derive(Clone)]
struct GatedStore {
    inner: Arc<MemoryObjectStore>,
    gate: futures::future::Shared<futures::channel::oneshot::Receiver<()>>,
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
use crate::AuthorityWork;
use acyclic_stream::OwnedBytes;

macro_rules! object_work {
    ($store:expr, $method:ident($($arg:expr),*), $budget:expr, $cancel:expr) => {
        async {
            let mut work = AuthorityWork::new($budget, $cancel);
            let result = object_work!(@call $store, $method($($arg),*), work);
            work.receipt(result)
        }
    };
    (@call $store:expr, read_many($($arg:expr),*), $work:ident) => {
        $store.read_many($($arg,)* &mut $work).await.map(|(values, _)| values)
    };
    (@call $store:expr, $method:ident($($arg:expr),*), $work:ident) => {
        $store.$method($($arg,)* &mut $work).await
    };
}

impl AsyncObjectStore for CountingStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        ordinary(work)?;
        self.inner.put(id, bytes, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        ordinary(work)?;
        self.inner.contains(id, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        work.read_budget()?;
        self.scalar_reads.fetch_add(1, Ordering::AcqRel);
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        work.work.remaining(work.budget)?;
        self.batch_reads.fetch_add(1, Ordering::AcqRel);
        self.inner.read_many(requests, work).await
    }
}

impl AsyncObjectStore for ShortBatchStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        ordinary(work)?;
        self.inner.put(id, bytes, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        ordinary(work)?;
        self.inner.contains(id, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        work.read_budget()?;
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        ordinary(work)?;
        self.inner.read_many(&requests[..1], work).await
    }
}

impl AsyncObjectStore for GatedStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        ordinary(work)?;
        self.inner.put(id, bytes, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        ordinary(work)?;
        self.inner.contains(id, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        work.read_budget()?;
        ordinary(work)?;
        self.reads.fetch_add(1, Ordering::AcqRel);
        self.gate.clone().await.map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "original read controller closed",
            )
        })?;
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        ordinary(work)?;
        crate::async_storage::read_many_sequential(self, requests, work).await
    }
}

mod batches;
mod epochs;
mod flights;
#[cfg(not(target_arch = "wasm32"))]
mod native_cache;
mod residency;

const B: WorkBudget = WorkBudget::UNBOUNDED;

fn ordinary(work: &AuthorityWork<'_>) -> Result<(), ObjectStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if work.original.is_some() {
        return Err(ObjectStoreError::NativeUnavailable);
    }
    work.work.remaining(work.budget)?;
    Ok(())
}
