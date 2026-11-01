use super::*;
use crate::AuthorityWork;
use std::sync::{Arc, Mutex};

// Pause the actual store result, including its physical byte owner, at the
// provider boundary. Dropping the sole release producer terminates the wait.
pub(crate) struct PausedStore {
    pub(crate) inner: Arc<MemoryObjectStore>,
    pub(crate) release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl PausedStore {
    async fn pause(&self) -> Result<(), ObjectStoreError> {
        let release = self
            .release
            .lock()
            .map_err(|_| ObjectStoreError::Corrupt)?
            .take();
        if let Some(release) = release {
            release
                .await
                .map_err(|_| ObjectStoreError::Rejected("cache fill producer lost".to_owned()))?;
        }
        Ok(())
    }
}

impl AsyncObjectStore for PausedStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put(id, bytes, work).await?;
        self.pause().await
    }

    async fn put_hashed(
        &self,
        object: crate::HashedObject,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_hashed(object, work).await?;
        self.pause().await
    }

    async fn put_many(
        &self,
        writes: &[crate::storage::ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_many(writes, work).await?;
        self.pause().await
    }

    async fn put_many_hashed(
        &self,
        objects: &[crate::HashedObject],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_many_hashed(objects, work).await?;
        self.pause().await
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let read = self.inner.read(id, maximum, work).await?;
        self.pause().await?;
        Ok(read)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let reads = self.inner.read_many(requests, work).await?;
        self.pause().await?;
        Ok(reads)
    }

    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.inner.contains(id, work).await
    }
}
