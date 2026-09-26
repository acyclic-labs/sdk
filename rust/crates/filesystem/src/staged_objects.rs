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
//! engine. A spill file past its bound is drained a bounded step at a time
//! by the admissions that grow it, as housekeeping outside their own work,
//! which is always safe because durability is a superset of staging.

use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue, PublicationScope,
};
use crate::cancellation::CancellationToken;
use crate::heap_future::in_heap;
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    HashedObject, ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention,
    ObjectReceipt, ObjectResult, ObjectStoreError, ObjectWrite,
};
use acyclic_native_runtime::{NativeFile, OwnedRead, OwnedWrite};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, PoisonError};
use tokio::sync::RwLock;

// The local provider stores one batch in a bounded segment. Every durable
// drain batch stays within that segment regardless of source tree size, and a
// single larger object is admitted directly instead of staged.
const MAXIMUM_DRAIN_BYTES: u64 = 4 * 1_024 * 1_024;
// Staged bytes held in memory before the window spills to the private file.
const MAXIMUM_RESIDENT_BYTES: u64 = 4 * 1_024 * 1_024;
// Spill file length past which each spill also drains one segment-bounded
// batch of spilled objects durably. A spill moves at most half the resident
// window and a drain step up to a whole segment, so the spill shrinks to
// empty, and its file is reused, however long publication is deferred.
const MAXIMUM_SPILL_BYTES: u64 = 1_024 * 1_024 * 1_024;

enum Staged {
    /// Held in memory. `referenced` records a read since the window last
    /// considered spilling it; `order` is its key in `Index::order`;
    /// `decoded` holds its first decoded representation, which lives and
    /// leaves with the resident bytes.
    Resident {
        bytes: Bytes,
        referenced: AtomicBool,
        order: u64,
        decoded: OnceLock<(DecodedCacheKey, DecodedCacheValue)>,
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

/// Where one object is staged, as one read finds it.
enum Lookup {
    Absent,
    Resident(Bytes),
    Spilled { offset: u64, length: u64 },
}

/// Serves one resident object without copying its bytes.
fn resident_read(bytes: Bytes, maximum_bytes: u64, budget: WorkBudget) -> ObjectResult<ObjectRead> {
    let length = byte_length(&bytes);
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
    Ok(ObjectReceipt {
        value: ObjectRead {
            bytes,
            retention: ObjectReadRetention::Shared,
        },
        work,
    })
}

/// Hashes an [`ObjectId`] by folding the words it writes: its digest is
/// already a cryptographic hash, so mixing it again, as the default
/// `SipHash` does, only adds work to every staged read and admission.
#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut words = bytes.chunks_exact(8);
        for word in &mut words {
            let mut buffer = [0_u8; 8];
            buffer.copy_from_slice(word);
            self.write_u64(u64::from_le_bytes(buffer));
        }
        for &byte in words.remainder() {
            self.write_u64(u64::from(byte));
        }
    }

    fn write_u64(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0x517c_c1b7_2722_0a95);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

#[derive(Default)]
struct Index {
    objects: HashMap<ObjectId, Staged, BuildHasherDefault<IdentityHasher>>,
    // Resident identities in admission order, each re-queued once when read
    // since it was last considered: the window spills from the front, so
    // pages the current candidate keeps reading stay in memory while
    // superseded ones leave it.
    order: BTreeMap<u64, ObjectId>,
    next_order: u64,
    resident_bytes: u64,
    spilled_objects: usize,
    // Append offset of the spill file; space is reused only once no staged
    // object remains spilled.
    spill_end: u64,
}

impl Index {
    /// Holds `bytes` in memory as the newest object unless the identity is
    /// already staged. Returns whether the object was admitted.
    fn admit_resident(&mut self, object_id: ObjectId, bytes: Bytes) -> bool {
        if self.objects.contains_key(&object_id) {
            return false;
        }
        self.insert_resident(object_id, bytes);
        true
    }

    fn insert_resident(&mut self, object_id: ObjectId, bytes: Bytes) {
        let order = self.next_order;
        self.next_order += 1;
        self.resident_bytes = self.resident_bytes.saturating_add(byte_length(&bytes));
        self.order.insert(order, object_id);
        if let Some(Staged::Spilled { .. }) = self.objects.insert(
            object_id,
            Staged::Resident {
                bytes,
                referenced: AtomicBool::new(false),
                order,
                decoded: OnceLock::new(),
            },
        ) {
            self.spilled_objects -= 1;
        }
    }

    /// Takes the resident objects to spill, oldest first, until the window
    /// would be half full. An object read since it was last considered is
    /// re-queued once instead.
    fn spill_victims(&mut self) -> Vec<(ObjectId, Bytes)> {
        let mut victims = Vec::new();
        let mut spilled = 0_u64;
        while self.resident_bytes.saturating_sub(spilled) > MAXIMUM_RESIDENT_BYTES / 2 {
            let Some((_, object_id)) = self.order.pop_first() else {
                break;
            };
            let Some(Staged::Resident {
                bytes,
                referenced,
                order,
                ..
            }) = self.objects.get_mut(&object_id)
            else {
                continue;
            };
            if referenced.swap(false, Ordering::Relaxed) {
                *order = self.next_order;
                self.next_order += 1;
                self.order.insert(*order, object_id);
                continue;
            }
            spilled = spilled.saturating_add(byte_length(bytes));
            victims.push((object_id, bytes.clone()));
        }
        victims
    }

    /// Records victims written at `offset` onward as spilled. Each is still
    /// resident: only spills and drains move objects, and both hold the
    /// spill file exclusively.
    fn mark_spilled(&mut self, victims: &[(ObjectId, Bytes)], mut offset: u64) {
        for (object_id, bytes) in victims {
            let length = byte_length(bytes);
            if let Some(staged) = self.objects.get_mut(object_id) {
                *staged = Staged::Spilled { offset, length };
                self.spilled_objects += 1;
                self.resident_bytes = self.resident_bytes.saturating_sub(length);
            }
            offset = offset.saturating_add(length);
        }
        self.spill_end = offset;
    }

    /// Forgets one object the durable provider has acknowledged.
    fn remove(&mut self, object_id: ObjectId) {
        match self.objects.remove(&object_id) {
            Some(Staged::Resident { bytes, order, .. }) => {
                self.order.remove(&order);
                self.resident_bytes = self.resident_bytes.saturating_sub(byte_length(&bytes));
            }
            Some(Staged::Spilled { .. }) => self.spilled_objects -= 1,
            None => {}
        }
    }
}

/// One private local object admission buffer. Remote and memory providers keep
/// their existing direct semantics; only the local filesystem opts into this.
///
/// Reads of resident objects share the index and never wait for I/O. The
/// index is never held across an await; spill-file I/O is ordered by its own
/// lock, which spilled reads share and which spilling, draining, and
/// truncating hold exclusively, so no read ever observes a reused offset.
pub struct StagedObjects<S> {
    inner: S,
    spill: NativeFile,
    index: std::sync::RwLock<Index>,
    spill_io: RwLock<()>,
    collection: Arc<crate::Collection>,
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
            index: std::sync::RwLock::new(Index::default()),
            spill_io: RwLock::new(()),
            collection: Arc::default(),
        })
    }

    pub(crate) fn inner(&self) -> &S {
        &self.inner
    }

    fn index(&self) -> std::sync::RwLockReadGuard<'_, Index> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn index_mut(&self) -> std::sync::RwLockWriteGuard<'_, Index> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Where one object is staged, marking a resident one as read.
    fn lookup(&self, object_id: ObjectId) -> Lookup {
        match self.index().objects.get(&object_id) {
            None => Lookup::Absent,
            Some(Staged::Resident {
                bytes, referenced, ..
            }) => {
                referenced.store(true, Ordering::Relaxed);
                Lookup::Resident(bytes.clone())
            }
            Some(&Staged::Spilled { offset, length }) => Lookup::Spilled { offset, length },
        }
    }

    /// Moves the least recently used resident objects, down to half the
    /// window, to the end of the spill file with one unsynchronized write.
    /// The caller holds the spill file exclusively. Objects stay resident,
    /// and readable from memory, until the write completes, so a failed
    /// spill leaves staging unchanged apart from recency.
    async fn spill_locked(&self, budget: WorkBudget) -> ObjectResult<()> {
        let (victims, offset) = {
            let mut index = self.index_mut();
            if index.resident_bytes <= MAXIMUM_RESIDENT_BYTES {
                return Ok(ObjectReceipt {
                    value: (),
                    work: WorkCounters::default(),
                });
            }
            (index.spill_victims(), index.spill_end)
        };
        let spilled = victims.iter().fold(0_u64, |total, (_, bytes)| {
            total.saturating_add(byte_length(bytes))
        });
        let work = WorkCounters {
            backend_write_operations: 1,
            object_bytes_written: spilled,
            bytes_copied: spilled,
            allocation_operations: 1,
            peak_allocation_bytes: spilled,
            ..WorkCounters::default()
        };
        let restore = |index: &mut Index| {
            for (object_id, _) in &victims {
                if let Some(Staged::Resident { order, .. }) = index.objects.get(object_id) {
                    index.order.insert(*order, *object_id);
                }
            }
        };
        if let Err(error) = work.verify(budget) {
            restore(&mut self.index_mut());
            return Err(ObjectFailure::before_work(error.into()));
        }
        let mut buffer = Vec::new();
        if buffer
            .try_reserve_exact(usize::try_from(spilled).unwrap_or(usize::MAX))
            .is_err()
        {
            restore(&mut self.index_mut());
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "staged spill allocation failed".to_owned(),
            )));
        }
        for (_, bytes) in &victims {
            buffer.extend_from_slice(bytes);
        }
        if let Err(error) = self
            .spill
            .write_all_batch_async(vec![OwnedWrite {
                offset,
                bytes: Bytes::from(buffer),
            }])
            .await
        {
            restore(&mut self.index_mut());
            return Err(ObjectFailure::new(error.into(), work));
        }
        self.index_mut().mark_spilled(&victims, offset);
        Ok(ObjectReceipt { value: (), work })
    }

    /// Reads spilled objects back in one submission and authenticates each
    /// against its identity. The caller holds the spill file.
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
    /// Spills the least recently used resident objects once the window
    /// overflows and, while the spill file is past its bound, drains one
    /// batch of spilled objects durably.
    ///
    /// Only the spill belongs to the admission: it is what keeps staging's
    /// memory bounded. The drain step is housekeeping any admission could
    /// have done, so it runs under its own bounded work, is not charged to
    /// the caller, and never fails the admission; a failed step leaves its
    /// objects staged for the next step or publication.
    async fn relieve_window(&self, budget: WorkBudget) -> ObjectResult<()> {
        let _spill_file = self.spill_io.write().await;
        let spilled = self.spill_locked(budget).await?;
        let targets = {
            let index = self.index();
            if index.spill_end <= MAXIMUM_SPILL_BYTES {
                return Ok(spilled);
            }
            let mut batch_bytes = 0_u64;
            index
                .objects
                .iter()
                .filter_map(|(&object_id, staged)| match *staged {
                    Staged::Spilled { length, .. } => Some((object_id, length)),
                    Staged::Resident { .. } => None,
                })
                .take_while(|&(_, length)| {
                    let first = batch_bytes == 0;
                    batch_bytes = batch_bytes.saturating_add(length);
                    first || batch_bytes <= MAXIMUM_DRAIN_BYTES
                })
                .map(|(object_id, _)| object_id)
                .collect::<Vec<_>>()
        };
        // One segment-bounded batch bounds the step's work; a fresh token
        // keeps the caller's cancellation from abandoning it midway.
        let _step = self
            .drain_locked(targets, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await;
        Ok(spilled)
    }

    /// Durably admits the staged objects among `targets` in segment-bounded
    /// batches. The caller holds the spill file exclusively. A batch leaves
    /// staging only once the provider acknowledges it, so a failed drain
    /// keeps the remainder private and retryable. The spill file is reused
    /// once no staged object remains spilled.
    async fn drain_locked(
        &self,
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
            let mut writes = Vec::new();
            let mut spilled = Vec::new();
            {
                let index = self.index();
                let mut batch_bytes = 0_u64;
                while let Some(&object_id) = targets.peek() {
                    let Some(staged) = index.objects.get(&object_id) else {
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
                        Staged::Spilled { offset, length } => {
                            spilled.push((object_id, offset, length));
                        }
                    }
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
            let mut index = self.index_mut();
            for write in &writes {
                index.remove(write.object_id);
            }
        }
        let truncate = {
            let index = self.index();
            index.spilled_objects == 0 && index.spill_end != 0
        };
        if truncate {
            self.spill
                .set_len_async(0)
                .await
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            self.index_mut().spill_end = 0;
        }
        Ok(ObjectReceipt { value: (), work })
    }

    /// Reads one object that was spilled when looked up: from the spill file
    /// if it still is, from memory if it has since returned to the window,
    /// and from the durable provider if a drain has since admitted it.
    async fn read_spilled_object(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        // Sharing the spill file keeps a drain from truncating or reusing it
        // until this read completes.
        let spill_file = self.spill_io.read().await;
        let (offset, length) = match self.lookup(object_id) {
            Lookup::Spilled { offset, length } => (offset, length),
            Lookup::Resident(bytes) => return resident_read(bytes, maximum_bytes, budget),
            Lookup::Absent => {
                drop(spill_file);
                return self
                    .inner
                    .read(object_id, maximum_bytes, budget, cancellation)
                    .await;
            }
        };
        if length > maximum_bytes {
            return Err(ObjectFailure::before_work(ObjectStoreError::TooLarge {
                observed: length,
                maximum: maximum_bytes,
            }));
        }
        let work = WorkCounters {
            backend_read_operations: 1,
            object_bytes_read: length,
            bytes_hashed: length,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let object = self
            .read_spilled(&[(object_id, offset, length)])
            .await
            .map_err(|error| ObjectFailure::new(error, work))?
            .pop()
            .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Corrupt, work))?;
        let (_, bytes) = object.into_parts();
        // A spilled object read again is live; it returns to the
        // window when there is room, and otherwise stays spilled.
        let mut index = self.index_mut();
        if index.resident_bytes.saturating_add(length) <= MAXIMUM_RESIDENT_BYTES
            && matches!(
                index.objects.get(&object_id),
                Some(&Staged::Spilled { offset: current, .. }) if current == offset
            )
        {
            index.insert_resident(object_id, bytes.clone());
        }
        Ok(ObjectReceipt {
            value: ObjectRead {
                bytes,
                retention: ObjectReadRetention::Shared,
            },
            work,
        })
    }
}

impl<S: AsyncObjectStore> AsyncObjectStore for StagedObjects<S> {
    /// Staged pages are the private working set of unpublished candidates,
    /// and most are superseded by the next mutation that reads them. Rather
    /// than churn the shared decoded cache and evict published pages, a
    /// resident page keeps its own decoded representation, which leaves with
    /// it; a spilled page is decoded afresh. The cache is keyed by immutable
    /// identity and decoder, so every answer is sound.
    fn decoded_cache_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        match self.index().objects.get(&key.object_id) {
            Some(Staged::Resident { decoded, .. }) => Ok(decoded
                .get()
                .filter(|(cached, _)| *cached == key)
                .map(|(_, value)| value.clone())),
            Some(Staged::Spilled { .. }) => Ok(None),
            None => self.inner.decoded_cache_get(key),
        }
    }

    fn decoded_cache_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        match self.index().objects.get(&key.object_id) {
            Some(Staged::Resident { decoded, .. }) => {
                let (cached, shared) = decoded.get_or_init(|| (key, value.clone()));
                Ok(if *cached == key {
                    DecodedCacheAdmission::Shared(shared.clone())
                } else {
                    DecodedCacheAdmission::Uncached(value)
                })
            }
            Some(Staged::Spilled { .. }) => Ok(DecodedCacheAdmission::Uncached(value)),
            None => self.inner.decoded_cache_admit(key, value),
        }
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
        // Direct admission and spilling await I/O in their own heap frames,
        // so a resident admission, which almost every page write is, never
        // builds or moves their larger futures.
        if object.length() > MAXIMUM_DRAIN_BYTES {
            return in_heap(|| self.inner.put_hashed(object, budget, cancellation)).await;
        }
        // The object's hash is its identity: an equal identity already
        // staged holds these exact bytes.
        let overflowing = {
            let (object_id, bytes) = object.into_parts();
            let mut index = self.index_mut();
            index.admit_resident(object_id, bytes) && index.resident_bytes > MAXIMUM_RESIDENT_BYTES
        };
        if !overflowing {
            return Ok(ObjectReceipt {
                value: (),
                work: WorkCounters::default(),
            });
        }
        in_heap(|| self.relieve_window(budget)).await
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

    async fn put_many_hashed(
        &self,
        objects: &[HashedObject],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        if objects.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object write batch is empty".to_owned(),
            )));
        }
        let mut work = WorkCounters::default();
        for object in objects {
            let receipt = self
                .put_hashed(
                    object.clone(),
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
    ) -> ObjectResult<crate::PublicationHold> {
        let _spill_file = self.spill_io.write().await;
        // Admitted before the drain: a collection cannot sweep what the
        // drain stores until the record naming it is written.
        let (objects, proven_at) = match scope {
            PublicationScope::Closure { objects, proven_at } => (objects, proven_at),
            PublicationScope::Everything => (&[][..], self.collection.sweeps()),
        };
        let hold = self
            .collection
            .admit(
                objects,
                |object_id| self.index().objects.contains_key(object_id),
                proven_at,
            )
            .await
            .map_err(ObjectFailure::before_work)?;
        let targets = {
            let index = self.index();
            match scope {
                PublicationScope::Closure { objects, .. } => objects
                    .iter()
                    .copied()
                    .filter(|object_id| index.objects.contains_key(object_id))
                    .collect(),
                PublicationScope::Everything => index.objects.keys().copied().collect(),
            }
        };
        let drained = self.drain_locked(targets, budget, cancellation).await?;
        Ok(ObjectReceipt {
            value: hold,
            work: drained.work,
        })
    }

    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        Some(&self.collection)
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
        match self.lookup(object_id) {
            Lookup::Resident(bytes) => resident_read(bytes, maximum_bytes, budget),
            // Every other answer awaits I/O and runs in its own heap frame,
            // so the resident answer, which every staged page read takes,
            // never builds or moves those larger futures.
            Lookup::Absent => {
                in_heap(|| {
                    self.inner
                        .read(object_id, maximum_bytes, budget, cancellation)
                })
                .await
            }
            Lookup::Spilled { .. } => {
                in_heap(|| self.read_spilled_object(object_id, maximum_bytes, budget, cancellation))
                    .await
            }
        }
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
        if self.index().objects.contains_key(&object_id) {
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
    use crate::kernel::DecodeLimits;
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
        assert_eq!(store.index().spill_end, 0);
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
    async fn a_spill_past_its_bound_drains_without_charging_or_failing_admissions()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        let body_bytes = 256 * 1_024;
        let window = usize::try_from(MAXIMUM_RESIDENT_BYTES)? / body_bytes;
        let objects = (0..8 * window)
            .map(|index| {
                blob(Bytes::from(vec![
                    u8::try_from(index % 251).unwrap_or(0);
                    body_bytes
                ]))
            })
            .collect::<Vec<_>>();
        let (early, late) = objects.split_at(3 * window);
        for (object_id, bytes) in early {
            store
                .put(*object_id, bytes.clone(), WorkBudget::UNBOUNDED, &token)
                .await?;
        }
        assert!(store.index().spilled_objects > 0);
        // Stand in for a spill file that has grown past its bound.
        store.index_mut().spill_end += MAXIMUM_SPILL_BYTES;

        // An admission may spend exactly its own spill: one backend write.
        let mut admission = WorkBudget::UNBOUNDED;
        admission.backend_write_operations = 1;
        for (object_id, bytes) in late {
            let receipt = store
                .put(*object_id, bytes.clone(), admission, &token)
                .await?;
            assert!(receipt.work.backend_write_operations <= 1);
        }
        let durable = {
            let mut durable = 0;
            for (object_id, _) in early {
                if store
                    .inner()
                    .contains(*object_id, WorkBudget::UNBOUNDED, &token)
                    .await?
                    .value
                {
                    durable += 1;
                }
            }
            durable
        };
        assert!(durable > 0, "the drain steps made spilled objects durable");
        assert!(
            store.index().spill_end <= MAXIMUM_SPILL_BYTES,
            "the steps emptied and reused the spill file"
        );
        for (object_id, bytes) in &objects {
            let read = store
                .read(*object_id, u64::MAX, WorkBudget::UNBOUNDED, &token)
                .await?;
            assert_eq!(&read.value.bytes, bytes);
        }
        drop(store);
        drop(provider);
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
                PublicationScope::Closure {
                    objects: &closure,
                    proven_at: 0,
                },
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
        // A resident page keeps its own decoded representation, which leaves
        // with it once publication makes the page durable.
        let key = DecodedCacheKey::new::<u32>(object.object_id(), DecodeLimits::default());
        let decoded = DecodedCacheValue {
            value: Arc::new(7_u32),
            logical_bytes: 4,
        };
        assert!(store.decoded_cache_get(key)?.is_none());
        assert!(matches!(
            store.decoded_cache_admit(key, decoded)?,
            DecodedCacheAdmission::Shared(_)
        ));
        let cached = store
            .decoded_cache_get(key)?
            .ok_or("resident page lost its decoding")?;
        assert_eq!(cached.value.downcast_ref::<u32>(), Some(&7));
        let other = DecodedCacheKey::new::<u64>(object.object_id(), DecodeLimits::default());
        assert!(store.decoded_cache_get(other)?.is_none());
        store
            .flush_before_publish(PublicationScope::Everything, WorkBudget::UNBOUNDED, &token)
            .await?;
        assert!(store.decoded_cache_get(key)?.is_none());
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

    #[tokio::test]
    async fn concurrent_admissions_reads_and_publication_agree()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        let body_bytes = 64 * 1_024;
        let objects = (0..4 * usize::try_from(MAXIMUM_RESIDENT_BYTES)? / body_bytes)
            .map(|index| {
                HashedObject::new(
                    ObjectKind::Blob,
                    Bytes::from(vec![u8::try_from(index % 251).unwrap_or(0); body_bytes]),
                )
            })
            .collect::<Vec<_>>();
        // Writers race readers of everything admitted so far while the
        // window spills and one publication drains half the objects.
        let closure = objects
            .iter()
            .step_by(2)
            .map(HashedObject::object_id)
            .collect::<Vec<_>>();
        let (staged, objects, token) = (&store, &objects, &token);
        let writers = futures::future::try_join_all(objects.chunks(8).map(|chunk| async move {
            for object in chunk {
                staged
                    .put_hashed(object.clone(), WorkBudget::UNBOUNDED, token)
                    .await?;
                for earlier in objects.iter().take(8) {
                    let read = staged
                        .read(earlier.object_id(), u64::MAX, WorkBudget::UNBOUNDED, token)
                        .await;
                    if let Ok(read) = read {
                        assert_eq!(read.value.bytes, earlier.clone().into_parts().1);
                    }
                }
            }
            Ok::<_, ObjectFailure>(())
        }));
        writers.await?;
        store
            .flush_before_publish(
                PublicationScope::Closure {
                    objects: &closure,
                    proven_at: 0,
                },
                WorkBudget::UNBOUNDED,
                token,
            )
            .await?;
        for object in objects {
            let read = store
                .read(object.object_id(), u64::MAX, WorkBudget::UNBOUNDED, token)
                .await?;
            assert_eq!(read.value.bytes, object.clone().into_parts().1);
        }
        drop(store);
        drop(provider);
        assert!(reopen_contains(directory.path(), &closure).await?);
        Ok(())
    }
}
