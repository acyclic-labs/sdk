use acyclic_fs::{
    AsyncObjectStore, AuthorityAllocation, AuthorityWork, MemoryObjectStore, ObjectId, ObjectKind,
    ObjectRead, ObjectReadRequest, ObjectStoreError, ObjectWrite,
};
use acyclic_stream::OwnedBytes;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Default)]
pub(super) struct CountingObjects {
    inner: Arc<MemoryObjectStore>,
    pub(super) reads: Arc<AtomicUsize>,
}

impl AsyncObjectStore for CountingObjects {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put(id, bytes, work).await
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_many(writes, work).await
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        if id.kind == ObjectKind::BlobChunk {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.read(id, maximum, work).await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, AuthorityAllocation), ObjectStoreError> {
        self.reads.fetch_add(
            requests
                .iter()
                .filter(|request| request.object_id.kind == ObjectKind::BlobChunk)
                .count(),
            Ordering::SeqCst,
        );
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
