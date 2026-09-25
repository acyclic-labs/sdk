//! Bounded local-only immutable staging before the authority publication barrier.
//!
//! Staged objects are private to one embedded engine and are never considered
//! crash-durable until `flush_before_publish` has admitted them through the
//! underlying durable batch provider. A crash may orphan or lose unreferenced
//! staged bytes, but cannot publish an authority head referring to them.
//!
//! Staged bytes beyond the resident memory window spill to an unlinked private
//! file without any durability barrier, so authoring never waits on the
//! durable provider. Publication durably drains exactly the staged part of
//! the closure it makes reachable; objects no published closure needs, such
//! as pages later mutations superseded, stay private and vanish with the
//! engine. Only a spill file past its bound is drained wholesale, which is
//! always safe because durability is a superset of staging.

use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue, PublicationScope,
};
use crate::cancellation::CancellationToken;
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    HashedObject, ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention,
    ObjectReceipt, ObjectResult, ObjectStoreError, ObjectWrite,
};
use acyclic_native_runtime::{NativeFile, OwnedRead, OwnedWrite};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

// The local provider stores one batch in a bounded segment. Every durable
// drain batch stays within that segment regardless of source tree size, and a
// single larger object is admitted directly instead of staged.
const MAXIMUM_DRAIN_BYTES: u64 = 4 * 1_024 * 1_024;
// Staged bytes held in memory before the window spills to the private file.
const MAXIMUM_RESIDENT_BYTES: u64 = 4 * 1_024 * 1_024;
// Spill file length past which every spilled object is drained durably, so
// private staging stays bounded however long publication is deferred.
const MAXIMUM_SPILL_BYTES: u64 = 1_024 * 1_024 * 1_024;

enum Staged {
    /// Held in memory; `recency` is this object's key in `Pending::recency`.
    Resident {
        bytes: Bytes,
        recency: u64,
    },
    Spilled {
        offset: u64,
        length: u64,
    },
}

impl Staged {
    fn length(&self) -> u64 {
        match self {
            Self::Resident { bytes, .. } => byte_length(bytes),
            Self::Spilled { length, .. } => *length,
        }
    }
}

#[derive(Default)]
struct Pending {
    objects: BTreeMap<ObjectId, Staged>,
    // Resident identities from least to most recently staged or read. The
    // window spills from the front, so pages the current candidate keeps
    // reading stay in memory while superseded ones leave it.
    recency: BTreeMap<u64, ObjectId>,
    clock: u64,
    resident_bytes: u64,
    spilled_objects: usize,
    // Append offset of the spill file; space is reused only once no staged
    // object remains spilled.
    spill_end: u64,
}

impl Pending {
    /// Holds `bytes` in memory as the most recently used object, replacing a
    /// spilled copy of the same identity.
    fn insert_resident(&mut self, object_id: ObjectId, bytes: Bytes) {
        let recency = self.next_recency();
        self.resident_bytes = self.resident_bytes.saturating_add(byte_length(&bytes));
        self.recency.insert(recency, object_id);
        if let Some(Staged::Spilled { .. }) = self
            .objects
            .insert(object_id, Staged::Resident { bytes, recency })
        {
            self.spilled_objects -= 1;
        }
    }

    /// Marks one resident object as the most recently used.
    fn touch(&mut self, object_id: ObjectId) {
        let recency = self.next_recency();
        if let Some(Staged::Resident {
            recency: current, ..
        }) = self.objects.get_mut(&object_id)
        {
            self.recency.remove(current);
            *current = recency;
            self.recency.insert(recency, object_id);
        }
    }

    /// Forgets one object the durable provider has acknowledged.
    fn remove(&mut self, object_id: ObjectId) {
        match self.objects.remove(&object_id) {
            Some(Staged::Resident { bytes, recency }) => {
                self.recency.remove(&recency);
                self.resident_bytes = self.resident_bytes.saturating_sub(byte_length(&bytes));
            }
            Some(Staged::Spilled { .. }) => self.spilled_objects -= 1,
            None => {}
        }
    }

    fn next_recency(&mut self) -> u64 {
        let recency = self.clock;
        self.clock = self.clock.wrapping_add(1);
        recency
    }
}

/// One private local object admission buffer. Remote and memory providers keep
/// their existing direct semantics; only the local filesystem opts into this.
pub struct StagedObjects<S> {
    inner: S,
    spill: NativeFile,
    pending: Mutex<Pending>,
}

impl<S> StagedObjects<S> {
    /// Wraps a durable local provider with bounded pre-publication staging
    /// whose overflow spills to an unlinked file in `spill_directory`.
    ///
    /// # Errors
    ///
    /// Returns the host error if the private spill file cannot be created.
    pub async fn open(inner: S, spill_directory: PathBuf) -> std::io::Result<Self> {
        let spill = acyclic_native_runtime::run_blocking_io(move || create_spill(&spill_directory))
            .await??;
        Ok(Self {
            inner,
            spill: NativeFile::from_file(spill)?,
            pending: Mutex::new(Pending::default()),
        })
    }

    pub(crate) fn inner(&self) -> &S {
        &self.inner
    }

    /// Moves the least recently used resident objects, down to half the
    /// window, to the end of the spill file with one unsynchronized write.
    /// Objects are re-indexed only after the write completes, so a failed
    /// spill leaves staging unchanged.
    async fn spill_locked(&self, pending: &mut Pending, budget: WorkBudget) -> ObjectResult<()> {
        let mut victims = Vec::new();
        let mut spilled = 0_u64;
        for (&recency, object_id) in &pending.recency {
            if pending.resident_bytes.saturating_sub(spilled) <= MAXIMUM_RESIDENT_BYTES / 2 {
                break;
            }
            if let Some(Staged::Resident { bytes, .. }) = pending.objects.get(object_id) {
                spilled = spilled.saturating_add(byte_length(bytes));
                victims.push((recency, *object_id, bytes.clone()));
            }
        }
        let work = WorkCounters {
            backend_write_operations: 1,
            object_bytes_written: spilled,
            bytes_copied: spilled,
            allocation_operations: 1,
            peak_allocation_bytes: spilled,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(usize::try_from(spilled).unwrap_or(usize::MAX))
            .map_err(|_| {
                ObjectFailure::before_work(ObjectStoreError::Rejected(
                    "staged spill allocation failed".to_owned(),
                ))
            })?;
        for (_, _, bytes) in &victims {
            buffer.extend_from_slice(bytes);
        }
        self.spill
            .write_all_batch_async(vec![OwnedWrite {
                offset: pending.spill_end,
                bytes: Bytes::from(buffer),
            }])
            .await
            .map_err(|error| ObjectFailure::new(error.into(), work))?;
        for (recency, object_id, bytes) in victims {
            let length = byte_length(&bytes);
            pending.recency.remove(&recency);
            pending.objects.insert(
                object_id,
                Staged::Spilled {
                    offset: pending.spill_end,
                    length,
                },
            );
            pending.spilled_objects += 1;
            pending.spill_end = pending.spill_end.saturating_add(length);
            pending.resident_bytes = pending.resident_bytes.saturating_sub(length);
        }
        Ok(ObjectReceipt { value: (), work })
    }

    /// Reads spilled objects back in one submission and authenticates each
    /// against its identity.
    async fn read_spilled(
        &self,
        spilled: &[(ObjectId, u64, u64)],
    ) -> Result<Vec<HashedObject>, ObjectStoreError> {
        let reads = spilled
            .iter()
            .map(|&(_, offset, length)| {
                usize::try_from(length)
                    .map(|length| OwnedRead { offset, length })
                    .map_err(|_| ObjectStoreError::Corrupt)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let bytes = self.spill.read_batch_async(reads).await?;
        if bytes.len() != spilled.len() {
            return Err(ObjectStoreError::Corrupt);
        }
        spilled
            .iter()
            .zip(bytes)
            .map(|(&(object_id, _, _), bytes)| {
                HashedObject::verify(object_id, bytes).ok_or(ObjectStoreError::Corrupt)
            })
            .collect()
    }
}

impl<S: AsyncObjectStore> StagedObjects<S> {
    /// Admits `bytes` into the resident window, first spilling the least
    /// recently used objects when it would overflow, and durably draining
    /// every spilled object once the spill file reaches its bound.
    async fn admit_locked(
        &self,
        pending: &mut Pending,
        object: HashedObject,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let mut work = WorkCounters::default();
        if pending.resident_bytes.saturating_add(object.length()) > MAXIMUM_RESIDENT_BYTES {
            work = self.spill_locked(pending, budget).await?.work;
            if pending.spill_end > MAXIMUM_SPILL_BYTES {
                let spilled = pending
                    .objects
                    .iter()
                    .filter(|(_, staged)| matches!(staged, Staged::Spilled { .. }))
                    .map(|(&object_id, _)| object_id)
                    .collect::<Vec<_>>();
                let drained = self
                    .drain_locked(
                        pending,
                        spilled,
                        work.remaining(budget)
                            .map_err(|error| ObjectFailure::new(error.into(), work))?,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                work = work
                    .checked_add(drained.work)
                    .map_err(|error| ObjectFailure::new(error.into(), work))?;
            }
        }
        let (object_id, bytes) = object.into_parts();
        pending.insert_resident(object_id, bytes);
        Ok(ObjectReceipt { value: (), work })
    }

    /// Durably admits the staged objects among `targets` in segment-bounded
    /// batches. A batch leaves staging only once the provider acknowledges
    /// it, so a failed drain keeps the remainder private and retryable. The
    /// spill file is reused once no staged object remains spilled.
    async fn drain_locked(
        &self,
        pending: &mut Pending,
        targets: Vec<ObjectId>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let mut work = WorkCounters::default();
        let mut targets = targets.into_iter().peekable();
        loop {
            cancellation
                .check()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
            let mut batch_bytes = 0_u64;
            let mut writes = Vec::new();
            let mut spilled = Vec::new();
            while let Some(&object_id) = targets.peek() {
                let Some(staged) = pending.objects.get(&object_id) else {
                    targets.next();
                    continue;
                };
                let length = staged.length();
                if !(writes.is_empty() && spilled.is_empty())
                    && batch_bytes.saturating_add(length) > MAXIMUM_DRAIN_BYTES
                {
                    break;
                }
                targets.next();
                batch_bytes = batch_bytes.saturating_add(length);
                match *staged {
                    Staged::Resident { ref bytes, .. } => writes.push(ObjectWrite {
                        object_id,
                        bytes: bytes.clone(),
                    }),
                    Staged::Spilled { offset, length } => spilled.push((object_id, offset, length)),
                }
            }
            if writes.is_empty() && spilled.is_empty() {
                break;
            }
            if !spilled.is_empty() {
                let spilled_bytes = spilled
                    .iter()
                    .fold(0_u64, |total, &(_, _, length)| total.saturating_add(length));
                work = work
                    .checked_add(WorkCounters {
                        backend_read_operations: 1,
                        object_bytes_read: spilled_bytes,
                        bytes_hashed: spilled_bytes,
                        ..WorkCounters::default()
                    })
                    .map_err(|error| ObjectFailure::new(error.into(), work))?;
                work.verify(budget)
                    .map_err(|error| ObjectFailure::new(error.into(), work))?;
                let read = self
                    .read_spilled(&spilled)
                    .await
                    .map_err(|error| ObjectFailure::new(error, work))?;
                writes.extend(read.into_iter().map(|object| {
                    let (object_id, bytes) = object.into_parts();
                    ObjectWrite { object_id, bytes }
                }));
            }
            let receipt = self
                .inner
                .put_many(
                    &writes,
                    work.remaining(budget)
                        .map_err(|error| ObjectFailure::new(error.into(), work))?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = work
                .checked_add(receipt.work)
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            for write in &writes {
                pending.remove(write.object_id);
            }
        }
        if pending.spilled_objects == 0 && pending.spill_end != 0 {
            self.spill
                .set_len_async(0)
                .await
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            pending.spill_end = 0;
        }
        Ok(ObjectReceipt { value: (), work })
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
        let verified = WorkCounters {
            bytes_hashed: byte_length(&bytes),
            ..WorkCounters::default()
        };
        verified
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let object = HashedObject::verify(object_id, bytes)
            .ok_or_else(|| ObjectFailure::new(ObjectStoreError::DigestMismatch, verified))?;
        let admitted = self
            .put_hashed(
                object,
                verified
                    .remaining(budget)
                    .map_err(|error| ObjectFailure::new(error.into(), verified))?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(verified, std::convert::identity))?;
        Ok(ObjectReceipt {
            value: (),
            work: verified
                .checked_add(admitted.work)
                .map_err(|error| ObjectFailure::new(error.into(), verified))?,
        })
    }

    async fn put_hashed(
        &self,
        object: HashedObject,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if object.length() > MAXIMUM_DRAIN_BYTES {
            return self.inner.put_hashed(object, budget, cancellation).await;
        }
        let mut pending = self.pending.lock().await;
        // The object's hash is its identity: an equal identity already
        // staged holds these exact bytes.
        if pending.objects.contains_key(&object.object_id()) {
            return Ok(ObjectReceipt {
                value: (),
                work: WorkCounters::default(),
            });
        }
        self.admit_locked(&mut pending, object, budget, cancellation)
            .await
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
        scope: PublicationScope<'_>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let mut pending = self.pending.lock().await;
        let targets = match scope {
            PublicationScope::Closure(closure) => closure
                .iter()
                .copied()
                .filter(|object_id| pending.objects.contains_key(object_id))
                .collect(),
            PublicationScope::Everything => pending.objects.keys().copied().collect(),
        };
        self.drain_locked(&mut pending, targets, budget, cancellation)
            .await
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
        // Holding the window across a spilled read keeps a concurrent drain
        // from truncating the spill file underneath it.
        let mut pending = self.pending.lock().await;
        let Some(staged) = pending.objects.get(&object_id) else {
            drop(pending);
            return self
                .inner
                .read(object_id, maximum_bytes, budget, cancellation)
                .await;
        };
        let length = staged.length();
        if length > maximum_bytes {
            return Err(ObjectFailure::before_work(ObjectStoreError::TooLarge {
                observed: length,
                maximum: maximum_bytes,
            }));
        }
        let (bytes, work) = match *staged {
            Staged::Resident { ref bytes, .. } => {
                let work = WorkCounters {
                    object_bytes_read: length,
                    ..WorkCounters::default()
                };
                work.verify(budget)
                    .map_err(|error| ObjectFailure::before_work(error.into()))?;
                let bytes = bytes.clone();
                pending.touch(object_id);
                (bytes, work)
            }
            Staged::Spilled { offset, length } => {
                // A spilled object that is read again is live; it returns to
                // the resident window as the most recently used.
                let read = WorkCounters {
                    backend_read_operations: 1,
                    object_bytes_read: length,
                    bytes_hashed: length,
                    ..WorkCounters::default()
                };
                read.verify(budget)
                    .map_err(|error| ObjectFailure::before_work(error.into()))?;
                let object = self
                    .read_spilled(&[(object_id, offset, length)])
                    .await
                    .map_err(|error| ObjectFailure::new(error, read))?
                    .pop()
                    .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Corrupt, read))?;
                let (_, bytes) = object.clone().into_parts();
                let admitted = self
                    .admit_locked(
                        &mut pending,
                        object,
                        read.remaining(budget)
                            .map_err(|error| ObjectFailure::new(error.into(), read))?,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| failure.map_with_prior_work(read, std::convert::identity))?;
                let work = read
                    .checked_add(admitted.work)
                    .map_err(|error| ObjectFailure::new(error.into(), read))?;
                (bytes, work)
            }
        };
        Ok(ObjectReceipt {
            value: ObjectRead {
                bytes,
                retention: ObjectReadRetention::Shared,
            },
            work,
        })
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

/// Creates a spill file no other handle names, which the host removes once
/// its last handle closes, including after a crash.
#[cfg(unix)]
fn create_spill(directory: &Path) -> std::io::Result<std::fs::File> {
    tempfile::tempfile_in(directory)
}

/// Creates a spill file no other handle names, which the host removes once
/// its last handle closes, including after a crash. The native runtime
/// reopens the handle for overlapped I/O, so the file admits shared access.
#[cfg(windows)]
fn create_spill(directory: &Path) -> std::io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_TEMPORARY, FILE_FLAG_DELETE_ON_CLOSE, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE,
    };
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .attributes(FILE_ATTRIBUTE_TEMPORARY.0)
        .custom_flags(FILE_FLAG_DELETE_ON_CLOSE.0)
        .open(directory.join(format!(
            ".acyclic-staging-{}",
            uuid::Uuid::new_v4().simple()
        )))
}

fn byte_length(bytes: &Bytes) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::ProviderObjectStore;
    use crate::storage::{ObjectKind, object_digest};
    use acyclic_objects::ObjectsProvider as _;
    use std::sync::Arc;

    type LocalStaged = StagedObjects<ProviderObjectStore<acyclic_objects::LocalObjects>>;

    async fn open_staged(
        directory: &Path,
    ) -> Result<(LocalStaged, Arc<acyclic_objects::LocalObjects>), Box<dyn std::error::Error>> {
        let provider = Arc::new(
            acyclic_objects::LocalObjects::open(
                directory.join("objects"),
                acyclic_objects::LocalObjectsLimits::default(),
            )
            .await?,
        );
        let bucket = provider
            .create_bucket("staged-test".to_owned(), None)
            .await?
            .bucket
            .ok_or("bucket creation returned no bucket")?;
        let store = StagedObjects::open(
            ProviderObjectStore::new(Arc::clone(&provider), bucket),
            directory.to_path_buf(),
        )
        .await?;
        Ok((store, provider))
    }

    async fn reopen_contains(
        directory: &Path,
        objects: &[ObjectId],
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let reopened = acyclic_objects::LocalObjects::open(
            directory.join("objects"),
            acyclic_objects::LocalObjectsLimits::default(),
        )
        .await?;
        let bucket = reopened
            .bucket_named("staged-test")
            .await?
            .ok_or("missing bucket")?;
        let reopened = ProviderObjectStore::new(Arc::new(reopened), bucket);
        let token = CancellationToken::new();
        for &object_id in objects {
            if !reopened
                .contains(object_id, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn blob(bytes: Bytes) -> (ObjectId, Bytes) {
        let object_id = ObjectId {
            kind: ObjectKind::Blob,
            digest: object_digest(ObjectKind::Blob, &bytes),
        };
        (object_id, bytes)
    }

    #[tokio::test]
    async fn failed_drain_keeps_objects_private_until_a_durable_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, provider) = open_staged(directory.path()).await?;
        let (object_id, bytes) = blob(Bytes::from_static(b"staged immutable body"));
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
                .flush_before_publish(PublicationScope::Everything, insufficient, &token)
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
            .flush_before_publish(PublicationScope::Everything, WorkBudget::UNBOUNDED, &token)
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
        assert!(reopen_contains(directory.path(), &[object_id]).await?);
        Ok(())
    }

    #[tokio::test]
    async fn overflow_spills_privately_until_the_publication_drain()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        // Three windows of distinct bodies force repeated spills, and the
        // drain must split them across several segment-bounded batches.
        let body_bytes = 256 * 1_024;
        let count = 3 * usize::try_from(MAXIMUM_RESIDENT_BYTES)? / body_bytes;
        let objects = (0..count)
            .map(|index| {
                blob(Bytes::from(vec![
                    u8::try_from(index % 251).unwrap_or(0);
                    body_bytes
                ]))
            })
            .collect::<Vec<_>>();
        for (object_id, bytes) in &objects {
            store
                .put(*object_id, bytes.clone(), WorkBudget::UNBOUNDED, &token)
                .await?;
        }
        for (object_id, bytes) in &objects {
            assert!(
                !store
                    .inner()
                    .contains(*object_id, WorkBudget::UNBOUNDED, &token)
                    .await?
                    .value,
                "overflow must not make staged bytes durable"
            );
            let read = store
                .read(*object_id, u64::MAX, WorkBudget::UNBOUNDED, &token)
                .await?;
            assert_eq!(&read.value.bytes, bytes);
        }
        store
            .flush_before_publish(PublicationScope::Everything, WorkBudget::UNBOUNDED, &token)
            .await?;
        for (object_id, bytes) in &objects {
            let read = store
                .read(*object_id, u64::MAX, WorkBudget::UNBOUNDED, &token)
                .await?;
            assert_eq!(&read.value.bytes, bytes);
        }
        assert_eq!(store.pending.lock().await.spill_end, 0);
        let identities = objects
            .iter()
            .map(|(object_id, _)| *object_id)
            .collect::<Vec<_>>();
        drop(store);
        drop(provider);
        assert!(reopen_contains(directory.path(), &identities).await?);
        Ok(())
    }

    #[tokio::test]
    async fn publication_drains_exactly_its_closure_and_keeps_the_rest_private()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        // Enough distinct bodies to spill several windows, so the closure
        // spans resident and spilled objects alike.
        let body_bytes = 256 * 1_024;
        let count = 3 * usize::try_from(MAXIMUM_RESIDENT_BYTES)? / body_bytes;
        let objects = (0..count)
            .map(|index| {
                HashedObject::new(
                    ObjectKind::Blob,
                    Bytes::from(vec![u8::try_from(index % 251).unwrap_or(0); body_bytes]),
                )
            })
            .collect::<Vec<_>>();
        for object in &objects {
            store
                .put_hashed(object.clone(), WorkBudget::UNBOUNDED, &token)
                .await?;
        }
        let identities = objects
            .iter()
            .map(HashedObject::object_id)
            .collect::<Vec<_>>();
        let (closure, superseded): (Vec<_>, Vec<_>) = identities
            .iter()
            .enumerate()
            .partition(|(index, _)| index % 2 == 0);
        let closure = closure.into_iter().map(|(_, id)| *id).collect::<Vec<_>>();
        let superseded = superseded
            .into_iter()
            .map(|(_, id)| *id)
            .collect::<Vec<_>>();

        store
            .flush_before_publish(
                PublicationScope::Closure(&closure),
                WorkBudget::UNBOUNDED,
                &token,
            )
            .await?;
        for &object_id in &superseded {
            assert!(
                !store
                    .inner()
                    .contains(object_id, WorkBudget::UNBOUNDED, &token)
                    .await?
                    .value,
                "an object outside the published closure must stay private"
            );
        }
        for object in &objects {
            let read = store
                .read(object.object_id(), u64::MAX, WorkBudget::UNBOUNDED, &token)
                .await?;
            assert_eq!(read.value.bytes, object.clone().into_parts().1);
        }
        drop(store);
        drop(provider);
        assert!(reopen_contains(directory.path(), &closure).await?);
        for object_id in superseded {
            assert!(!reopen_contains(directory.path(), &[object_id]).await?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn hashed_admission_skips_rehashing_and_unproven_bytes_are_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, _provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        let object = HashedObject::new(ObjectKind::Blob, Bytes::from_static(b"proven body"));
        let admitted = store
            .put_hashed(object.clone(), WorkBudget::UNBOUNDED, &token)
            .await?;
        assert_eq!(admitted.work.bytes_hashed, 0);
        let (object_id, _) = object.into_parts();
        let forged = store
            .put(
                object_id,
                Bytes::from_static(b"other body"),
                WorkBudget::UNBOUNDED,
                &token,
            )
            .await;
        assert!(matches!(
            forged.map(|_| ()).map_err(|failure| failure.error),
            Err(ObjectStoreError::DigestMismatch)
        ));
        Ok(())
    }
}
