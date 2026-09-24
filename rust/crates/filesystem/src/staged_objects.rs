//! Bounded local-only immutable staging before the authority publication barrier.
//!
//! Staged objects are private to one embedded engine and are never considered
//! crash-durable until `flush_before_publish` has admitted them through the
//! underlying durable batch provider. A crash may orphan or lose unreferenced
//! staged bytes, but cannot publish an authority head referring to them.

use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt,
    ObjectResult, ObjectStoreError, ObjectWrite, object_digest,
};
use bytes::Bytes;
use std::collections::BTreeMap;
use tokio::sync::Mutex;

// The local provider stores one batch in a bounded segment. Keep the SDK
// staging window no larger than that segment, regardless of source tree size.
const MAXIMUM_STAGED_BYTES: u64 = 4 * 1_024 * 1_024;

#[derive(Default)]
struct Pending {
    objects: BTreeMap<ObjectId, Bytes>,
    bytes: u64,
}

/// One private local object admission buffer. Remote and memory providers keep
/// their existing direct semantics; only the local filesystem opts into this.
pub struct StagedObjects<S> {
    inner: S,
    pending: Mutex<Pending>,
}

impl<S> StagedObjects<S> {
    /// Wraps a durable local provider with bounded pre-publication staging.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            pending: Mutex::new(Pending::default()),
        }
    }

    pub(crate) fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: AsyncObjectStore> StagedObjects<S> {
    async fn flush_locked(
        &self,
        pending: &mut Pending,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        if pending.objects.is_empty() {
            return Ok(ObjectReceipt {
                value: (),
                work: WorkCounters::default(),
            });
        }
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let writes = pending
            .objects
            .iter()
            .map(|(&object_id, bytes)| ObjectWrite {
                object_id,
                bytes: bytes.clone(),
            })
            .collect::<Vec<_>>();
        let receipt = self.inner.put_many(&writes, budget, cancellation).await?;
        pending.objects.clear();
        pending.bytes = 0;
        Ok(receipt)
    }
}

impl<S: AsyncObjectStore> AsyncObjectStore for StagedObjects<S> {
    fn decoded_cache_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        self.inner.decoded_cache_get(key)
    }

    fn decoded_cache_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        self.inner.decoded_cache_admit(key, value)
    }

    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let byte_count = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let verified = WorkCounters {
            bytes_hashed: byte_count,
            ..WorkCounters::default()
        };
        verified
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        if object_digest(object_id.kind, &bytes) != object_id.digest {
            return Err(ObjectFailure::new(
                ObjectStoreError::DigestMismatch,
                verified,
            ));
        }
        let mut pending = self.pending.lock().await;
        if let Some(existing) = pending.objects.get(&object_id) {
            return if existing == &bytes {
                Ok(ObjectReceipt {
                    value: (),
                    work: verified,
                })
            } else {
                Err(ObjectFailure::new(ObjectStoreError::Corrupt, verified))
            };
        }
        if byte_count > MAXIMUM_STAGED_BYTES {
            let flushed = self
                .flush_locked(
                    &mut pending,
                    verified
                        .remaining(budget)
                        .map_err(|error| ObjectFailure::new(error.into(), verified))?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(verified, std::convert::identity))?;
            let prior = verified
                .checked_add(flushed.work)
                .map_err(|error| ObjectFailure::new(error.into(), verified))?;
            let written = self
                .inner
                .put(
                    object_id,
                    bytes,
                    prior
                        .remaining(budget)
                        .map_err(|error| ObjectFailure::new(error.into(), prior))?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(prior, std::convert::identity))?;
            return Ok(ObjectReceipt {
                value: (),
                work: prior
                    .checked_add(written.work)
                    .map_err(|error| ObjectFailure::new(error.into(), prior))?,
            });
        }
        let mut work = verified;
        if pending.bytes.saturating_add(byte_count) > MAXIMUM_STAGED_BYTES {
            let flushed = self
                .flush_locked(
                    &mut pending,
                    work.remaining(budget)
                        .map_err(|error| ObjectFailure::new(error.into(), work))?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = work
                .checked_add(flushed.work)
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
        }
        pending.bytes = pending.bytes.saturating_add(byte_count);
        pending.objects.insert(object_id, bytes);
        Ok(ObjectReceipt { value: (), work })
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        if writes.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object write batch is empty".to_owned(),
            )));
        }
        let mut work = WorkCounters::default();
        for write in writes {
            let receipt = self
                .put(
                    write.object_id,
                    write.bytes.clone(),
                    work.remaining(budget)
                        .map_err(|error| ObjectFailure::new(error.into(), work))?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = work
                .checked_add(receipt.work)
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
        }
        Ok(ObjectReceipt { value: (), work })
    }

    async fn flush_before_publish(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let mut pending = self.pending.lock().await;
        self.flush_locked(&mut pending, budget, cancellation).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if let Some(bytes) = self.pending.lock().await.objects.get(&object_id).cloned() {
            let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if length > maximum_bytes {
                return Err(ObjectFailure::before_work(ObjectStoreError::TooLarge {
                    observed: length,
                    maximum: maximum_bytes,
                }));
            }
            let work = WorkCounters {
                object_bytes_read: length,
                ..WorkCounters::default()
            };
            work.verify(budget)
                .map_err(|error| ObjectFailure::before_work(error.into()))?;
            return Ok(ObjectReceipt {
                value: ObjectRead {
                    bytes,
                    retention: ObjectReadRetention::Shared,
                },
                work,
            });
        }
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
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.pending.lock().await.objects.contains_key(&object_id) {
            return Ok(ObjectReceipt {
                value: true,
                work: WorkCounters::default(),
            });
        }
        self.inner.contains(object_id, budget, cancellation).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::ProviderObjectStore;
    use crate::storage::ObjectKind;
    use acyclic_objects::ObjectsProvider as _;
    use std::sync::Arc;

    #[tokio::test]
    async fn failed_drain_keeps_objects_private_until_a_durable_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = Arc::new(
            acyclic_objects::LocalObjects::open(
                directory.path(),
                acyclic_objects::LocalObjectsLimits::default(),
            )
            .await?,
        );
        let bucket = provider
            .create_bucket("staged-test".to_owned(), None)
            .await?
            .bucket
            .ok_or("bucket creation returned no bucket")?;
        let store = StagedObjects::new(ProviderObjectStore::new(Arc::clone(&provider), bucket));
        let bytes = Bytes::from_static(b"staged immutable body");
        let object_id = ObjectId {
            kind: ObjectKind::Blob,
            digest: object_digest(ObjectKind::Blob, &bytes),
        };
        let token = CancellationToken::new();
        store
            .put(object_id, bytes.clone(), WorkBudget::UNBOUNDED, &token)
            .await?;
        assert!(
            store
                .contains(object_id, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
        );
        assert!(
            !store
                .inner()
                .contains(object_id, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
        );

        let mut insufficient = WorkBudget::UNBOUNDED;
        insufficient.object_bytes_written = 0;
        assert!(
            store
                .flush_before_publish(insufficient, &token)
                .await
                .is_err()
        );
        assert!(
            store
                .contains(object_id, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
        );
        assert!(
            !store
                .inner()
                .contains(object_id, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
        );

        store
            .flush_before_publish(WorkBudget::UNBOUNDED, &token)
            .await?;
        assert!(
            store
                .inner()
                .contains(object_id, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
        );
        drop(store);
        drop(provider);
        let reopened = acyclic_objects::LocalObjects::open(
            directory.path(),
            acyclic_objects::LocalObjectsLimits::default(),
        )
        .await?;
        let bucket = reopened
            .bucket_named("staged-test")
            .await?
            .ok_or("missing bucket")?;
        let reopened = ProviderObjectStore::new(Arc::new(reopened), bucket);
        assert!(
            reopened
                .contains(object_id, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
        );
        Ok(())
    }
}
