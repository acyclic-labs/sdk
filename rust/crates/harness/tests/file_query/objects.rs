use acyclic_fs::{
    AsyncObjectStore, AuthorityAllocation, AuthorityWork, MemoryObjectBackend, ObjectId, ObjectKind,
    ObjectRead, ObjectReadRequest, ObjectStoreError, storage::ObjectWrite,
};
use acyclic_stream::OwnedBytes;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
pub(super) struct CountingObjects {
    inner: Arc<MemoryObjectBackend>,
    pub(super) reads: Arc<AtomicUsize>,
}

impl Default for CountingObjects {
    fn default() -> Self {
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        Self {
            inner: Arc::new(MemoryObjectBackend::new(Arc::new(objects), bucket)),
            reads: Arc::default(),
        }
    }
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
