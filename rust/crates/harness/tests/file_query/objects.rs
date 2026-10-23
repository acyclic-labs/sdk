use acyclic_fs::{
    AsyncObjectStore, AuthorityAllocation, AuthorityWork, MemoryObjectBackend, ObjectId,
    ObjectKind, ObjectRead, ObjectReadRequest, ObjectStoreError, storage::ObjectWrite,
};
use acyclic_stream::OwnedBytes;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
pub(super) type PauseChannels = (
    futures::channel::oneshot::Receiver<()>,
    futures::channel::oneshot::Sender<()>,
);

type ReadPause = (
    ObjectKind,
    bool,
    futures::channel::oneshot::Sender<()>,
    futures::channel::oneshot::Receiver<()>,
);

// Inline files do not await the blob backend. Use the smallest actual chunk
// when a test must stop a content read after authorization.
pub(super) fn blob_text(complete: bool) -> String {
    let mut text = if complete { "- [x] " } else { "- [ ] " }.to_owned();
    text.extend(std::iter::repeat_n(
        'x',
        acyclic_fs::kernel::MAXIMUM_INLINE_FILE_BYTES + 1 - text.len(),
    ));
    text
}

#[derive(Clone)]
pub(super) struct CountingObjects {
    inner: Arc<MemoryObjectBackend>,
    pub(super) reads: Arc<AtomicUsize>,
    pub(super) content_reads: Arc<AtomicUsize>,
    pause: Arc<Mutex<Option<ReadPause>>>,
}

impl Default for CountingObjects {
    fn default() -> Self {
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        Self {
            inner: Arc::new(MemoryObjectBackend::new(Arc::new(objects), bucket)),
            reads: Arc::default(),
            content_reads: Arc::default(),
            pause: Arc::default(),
        }
    }
}

impl CountingObjects {
    pub(super) fn pause(&self, kind: ObjectKind) -> Result<PauseChannels, ObjectStoreError> {
        self.pause_io(kind, false)
    }

    pub(super) fn pause_io(
        &self,
        kind: ObjectKind,
        write: bool,
    ) -> Result<PauseChannels, ObjectStoreError> {
        let (entered, arrival) = futures::channel::oneshot::channel();
        let (release, released) = futures::channel::oneshot::channel();
        *self
            .pause
            .lock()
            .map_err(|_| ObjectStoreError::Simulation("fixture pause lock poisoned"))? =
            Some((kind, write, entered, released));
        Ok((arrival, release))
    }

    async fn pause_at(
        &self,
        id: ObjectId,
        bytes: &[u8],
        write: bool,
    ) -> Result<(), ObjectStoreError> {
        if !write && id.kind == ObjectKind::BlobChunk && bytes.starts_with(b"- [") {
            self.content_reads.fetch_add(1, Ordering::SeqCst);
        }
        let pause = {
            let mut gate = self
                .pause
                .lock()
                .map_err(|_| ObjectStoreError::Simulation("fixture pause lock poisoned"))?;
            if gate.as_ref().is_some_and(|(kind, writes, _, _)| {
                *writes == write
                    && *kind == id.kind
                    && (*kind != ObjectKind::BlobChunk || bytes.starts_with(b"- ["))
            }) {
                gate.take()
            } else {
                None
            }
        };
        if let Some((_, _, entered, released)) = pause {
            entered
                .send(())
                .map_err(|()| ObjectStoreError::Simulation("fixture pause observer dropped"))?;
            released
                .await
                .map_err(|_| ObjectStoreError::Simulation("fixture pause release dropped"))?;
        }
        Ok(())
    }
}

impl AsyncObjectStore for CountingObjects {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.pause_at(id, &bytes, true).await?;
        self.inner.put(id, bytes, work).await
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        for write in writes {
            self.pause_at(write.object_id, &write.bytes, true).await?;
        }
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
        let read = self.inner.read(id, maximum, work).await?;
        self.pause_at(id, &read.bytes, false).await?;
        Ok(read)
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
        let result = self.inner.read_many(requests, work).await?;
        for (request, read) in requests.iter().zip(&result.0) {
            self.pause_at(request.object_id, &read.bytes, false).await?;
        }
        Ok(result)
    }

    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.inner.contains(id, work).await
    }
}
