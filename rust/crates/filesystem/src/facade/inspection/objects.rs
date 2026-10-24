//! Backend reads are forwarded; candidate objects remain in this call's memory.
use super::*;
use crate::storage::{HashedObject, ObjectWrite};
use crate::{
    AuthorityAllocation, AuthorityStoreError, AuthorityWork, ObjectRead, ObjectReadRequest,
    ObjectReadRetention, ObjectStoreError,
};
use acyclic_stream::OwnedBytes;
mod index;
use index::Entries;

pub(super) struct ReadAuthority<'a, A>(pub(super) &'a A);

impl<A: AsyncAuthorityStore> AsyncAuthorityStore for ReadAuthority<'_, A> {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.0.operation_claims()
    }
    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        self.0.head(authority, work).await
    }
    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: crate::ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        self.0.replay(authority, after, limit, work).await
    }
}

pub(super) struct TransientObjects<'a, O> {
    inner: &'a O,
    entries: Mutex<Entries>,
    _composition: crate::NativeAllocation,
}

impl<'a, O> TransientObjects<'a, O> {
    pub(super) fn new(inner: &'a O, composition: crate::NativeAllocation) -> Self {
        Self {
            inner,
            entries: Mutex::default(),
            _composition: composition,
        }
    }

    fn find(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<OwnedBytes>, ObjectStoreError> {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.find(id, work)
    }

    fn insert(
        &self,
        object: HashedObject,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.insert(object, work)
    }
}

impl<O: AsyncObjectStore> AsyncObjectStore for TransientObjects<'_, O> {
    fn supports_decoded_cache(&self) -> bool {
        false
    }
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        work.accounting()
            .charge(WorkCounters {
                bytes_hashed: bytes.len() as u64 + crate::OBJECT_DIGEST_ENVELOPE_BYTES,
                ..WorkCounters::UNCHARGED
            })
            .map_err(crate::async_storage::object_error)?;
        self.insert(
            HashedObject::verify(id, bytes).ok_or(ObjectStoreError::DigestMismatch)?,
            work,
        )
    }
    async fn put_hashed(
        &self,
        object: HashedObject,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.insert(object, work)
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        for write in writes {
            self.put(write.object_id, write.bytes.clone(), work).await?;
        }
        Ok(())
    }
    async fn put_many_hashed(
        &self,
        objects: &[HashedObject],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        for object in objects {
            self.put_hashed(object.clone(), work).await?;
        }
        Ok(())
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum_bytes: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        if let Some(bytes) = self.find(id, work)? {
            let length = bytes.len() as u64;
            if length > maximum_bytes {
                return Err(ObjectStoreError::TooLarge {
                    observed: length,
                    maximum: maximum_bytes,
                });
            }
            work.accounting()
                .charge(WorkCounters {
                    object_bytes_read: length,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(crate::async_storage::object_error)?;
            return Ok(ObjectRead {
                bytes,
                retention: ObjectReadRetention::Shared,
            });
        }
        self.inner.read(id, maximum_bytes, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, AuthorityAllocation), ObjectStoreError> {
        crate::async_storage::read_many_sequential(self, requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        if self.find(id, work)?.is_some() {
            return Ok(true);
        }
        self.inner.contains(id, work).await
    }
    async fn historical_generation_evidence(
        &self,
        authority: AuthorityId,
        record: &DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<crate::kernel::HistoricalGenerationEvidence>, ObjectStoreError> {
        self.inner
            .historical_generation_evidence(authority, record, maximum_bytes, maximum_objects, work)
            .await
    }
}
