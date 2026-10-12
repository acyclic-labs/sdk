//! Bounded local-only immutable staging before the authority publication barrier.
//!
//! Staged objects are private to one embedded engine and are never considered
//! crash-durable until `flush_before_publish` has admitted them through the
//! underlying durable batch provider. A crash may orphan or lose unreferenced
//! staged bytes, but cannot publish an authority head referring to them.
//!
//! Staged bytes beyond the resident memory window spill to an unlinked private
//! file without a durability barrier. Publication durably drains exactly the
//! staged part of the closure it makes reachable; superseded objects remain
//! private. A spill file past its bound drains a segment-bounded step. Native
//! staging and overflow use the caller's original work and cancellation,
//! including their inner provider admissions and publication fence.
//!
//! Resident OwnedBytes preserve their intrinsic custody without readoption. Spill
//! copies and runtime reads retain actual buffers through the last byte alias;
//! known vectors, boxed frames and byte-owner controls use the original ledger.
//! Opaque index nodes and provider transport internals are excluded.

use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue, PublicationScope,
};
use crate::cancellation::CancellationToken;
use crate::materializer::{NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    HashedObject, ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention,
    ObjectReceipt, ObjectResult, ObjectStoreError, ObjectWrite,
};
use acyclic_native_runtime::{NativeFile, OwnedRead, OwnedWrite};
use acyclic_stream::OwnedBytes;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, PoisonError};
use tokio::sync::RwLock;

fn staged_vector<T>(
    capacity: usize, native: Option<&NativeMaterializationWork>,
) -> Result<(Option<NativeAllocation>, Vec<T>), ObjectStoreError> {
    let bytes = capacity.checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)?;
    let mut control = native.map(|work| {
        if bytes == 0 { work.retain_owned(0) } else { work.claim(bytes as u64) }
    }).transpose()?;
    let mut values = Vec::new();
    values.try_reserve_exact(capacity).map_err(|error|
        ObjectStoreError::Native(NativeTreeMaterializationError::AllocationFailed(error)))?;
    if let Some(control) = &mut control {
        let actual = values.capacity().checked_mul(std::mem::size_of::<T>())
            .ok_or(crate::WorkError::Overflow)?;
        control.reconcile_capacity(actual as u64)?;
    }
    Ok((control, values))
}

fn staged_owned_bytes(
    bytes: Vec<u8>, allocation: NativeAllocation, native: &NativeMaterializationWork,
) -> Result<OwnedBytes, ObjectStoreError> {
    OwnedBytes::from_vec_with_guard(bytes, allocation, native.runtime_meter_owner())
        .map_err(|failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => native.runtime_error(error).into(),
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
            =>
                ObjectStoreError::Native(NativeTreeMaterializationError::InvalidLayout),
        })
}

async fn staged_native_read(
    file: &NativeFile, offset: u64, length: usize, native: &NativeMaterializationWork,
) -> Result<OwnedBytes, ObjectStoreError> {
    if length == 0 {
        native.runtime_meter_owner().check().map_err(|error| native.runtime_error(error))?;
        return Ok(OwnedBytes::new());
    }
    let (allocation, mut bytes) = staged_vector::<u8>(length, Some(native))?;
    bytes.resize(bytes.capacity(), 0);
    let prepared = acyclic_stream::PreparedOwnedBytes::new(
        bytes, allocation.expect("native read buffer is funded"), native.runtime_meter_owner(),
    ).map_err(|failure| match failure.into_error() {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => native.runtime_error(error).into(),
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
        =>
            ObjectStoreError::Native(NativeTreeMaterializationError::InvalidLayout),
    })?;
    let (prepared, filled) = file.read_prepared_bounded_async(
        offset, prepared, length, false, native.runtime_meter_owner(),
    ).map_err(|error| native.runtime_error(error))?
        .await.map_err(|error| native.runtime_error(error))?;
    prepared.freeze(filled).map_err(|_| ObjectStoreError::Corrupt)
}

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
// Identities remembered as drained since the last sweep. Past this many the
// memory starts over, so it stays bounded however long the engine runs.
const MAXIMUM_DRAINED_IDENTITIES: usize = 4_096;

enum Staged {
    /// Held in memory. `referenced` records a read since the window last
    /// considered spilling it; `order` is its key in `Index::order`;
    /// `decoded` holds its first decoded representation, which lives and
    /// leaves with the resident bytes.
    Resident {
        bytes: OwnedBytes,
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
    Resident(OwnedBytes),
    Spilled { offset: u64, length: u64 },
}

/// Serves one resident object without copying its bytes.
fn resident_read(
    bytes: OwnedBytes, maximum_bytes: u64, budget: WorkBudget,
    native: Option<&NativeMaterializationWork>,
) -> Result<ObjectReceipt<ObjectRead>, (ObjectStoreError, WorkCounters)> {
    let length = byte_length(&bytes);
    if length > maximum_bytes {
        return Err((ObjectStoreError::TooLarge {
            observed: length, maximum: maximum_bytes,
        }, WorkCounters::default()));
    }
    let work = WorkCounters {
        object_bytes_read: length,
        ..WorkCounters::default()
    };
    if let Some(native) = native {
        native.charge(work).map_err(|error| (error.into(), WorkCounters::default()))?;
    } else {
        work.verify(budget).map_err(|error| (error.into(), WorkCounters::default()))?;
    }
    Ok(ObjectReceipt {
        value: ObjectRead { bytes, retention: ObjectReadRetention::Shared },
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
        let (words, rest) = bytes.as_chunks::<8>();
        for &word in words {
            self.write_u64(u64::from_le_bytes(word));
        }
        for &byte in rest {
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
    // Objects the durable provider acknowledged while the collection's sweep
    // count was `drained_at`. Only a sweep removes a durable object, so while
    // the count is unchanged each is still durable, and staging one again
    // (as every new file restages the shared default metadata) needs no
    // second write.
    drained: HashSet<ObjectId, BuildHasherDefault<IdentityHasher>>,
    drained_at: u64,
}

impl Index {
    /// Holds `bytes` in memory as the newest object unless the identity is
    /// already staged. Returns whether the object was admitted.
    fn admit_resident(&mut self, object_id: ObjectId, bytes: OwnedBytes) -> bool {
        if self.objects.contains_key(&object_id) {
            return false;
        }
        self.insert_resident(object_id, bytes);
        true
    }

    fn insert_resident(&mut self, object_id: ObjectId, bytes: OwnedBytes) {
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
    fn spill_victims(
        &mut self, victims: &mut Vec<(ObjectId, OwnedBytes)>,
    ) {
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
        // The caller owns the preallocated vector and its native custody.
    }

    /// Records victims written at `offset` onward as spilled. Each is still
    /// resident: only spills and drains move objects, and both hold the
    /// spill file exclusively.
    fn mark_spilled(&mut self, victims: &[(ObjectId, OwnedBytes)], mut offset: u64) {
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

    /// Forgets one object the durable provider has acknowledged, and
    /// remembers it as drained.
    fn acknowledge(&mut self, object_id: ObjectId) {
        self.remove(object_id);
        if self.drained.len() >= MAXIMUM_DRAINED_IDENTITIES {
            self.drained.clear();
        }
        self.drained.insert(object_id);
    }


    /// Forgets one staged object.
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
        let spill = acyclic_native_runtime::run_blocking_io(move || create_spill(&spill_directory), None).expect("unmetered task construction cannot fail")
            .await??;
        Ok(Self {
            inner,
            spill: NativeFile::from_file(spill, None).map_err(|(error, _)| error)?,
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
    async fn spill_locked(
        &self, budget: WorkBudget, cancellation: &CancellationToken,
        native: Option<&NativeMaterializationWork>,
    ) -> ObjectResult<()> {
        cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let (_victim_control, victims, offset) = {
            // Admission and victim selection share this lock: concurrent
            // resident puts cannot grow the vector beyond its funded capacity.
            let mut index = self.index_mut();
            if index.resident_bytes <= MAXIMUM_RESIDENT_BYTES {
                return Ok(ObjectReceipt { value: (), work: WorkCounters::default() });
            }
            let (control, mut victims) = staged_vector(index.order.len(), native)
                .map_err(ObjectFailure::before_work)?;
            index.spill_victims(&mut victims);
            (control, victims, index.spill_end)
        };
        let spilled = victims.iter().fold(0_u64, |total, (_, bytes)|
            total.saturating_add(byte_length(bytes)));
        let work = WorkCounters {
            backend_write_operations: 1,
            object_bytes_written: spilled,
            bytes_copied: spilled,
            allocation_operations: 1,
            peak_allocation_bytes: spilled,
            ..WorkCounters::default()
        };
        let result = async {
            if native.is_none() {
                work.verify(budget).map_err(|error| ObjectFailure::before_work(error.into()))?;
            }
            let length = usize::try_from(spilled)
                .map_err(|_| ObjectFailure::before_work(crate::WorkError::Overflow.into()))?;
            let copy = native.map(|native| native.reserve_copy(spilled)).transpose()
                .map_err(|error| ObjectFailure::before_work(error.into()))?;
            let (buffer_control, mut buffer) = staged_vector(length, native)
                .map_err(ObjectFailure::before_work)?;
            for (_, bytes) in &victims {
                buffer.extend_from_slice(bytes);
            }
            if let Some(copy) = copy {
                copy.record(spilled).map_err(|error| ObjectFailure::before_work(error.into()))?;
            }
            if let Some(native) = native {
                native.admit(WorkCounters { object_bytes_written: spilled, ..WorkCounters::default() })
                    .map_err(|error| ObjectFailure::before_work(error.into()))?;
                let bytes = staged_owned_bytes(
                    buffer, buffer_control.expect("native spill buffer is admitted"), native,
                ).map_err(ObjectFailure::before_work)?;
                self.spill.control_async(move |file, meter| { let meter = meter.expect("metered control");
                    acyclic_native_runtime::write_all_at(file, offset, &bytes, Some((meter, true)))
                }, Some(native.runtime_meter_owner()))
                    .map_err(|error| ObjectFailure::before_work(native.runtime_error(error).into()))?
                    .await.map_err(|error| ObjectFailure::before_work(native.runtime_error(error).into()))?;
                // Physical partial writes (including cancellation) are
                // recorded by the runtime as output_bytes before returning.
                native.record_admitted(WorkCounters { object_bytes_written: spilled, ..WorkCounters::default() })
                    .map_err(|error| ObjectFailure::before_work(error.into()))?;
            } else {
                self.spill.write_all_batch_async(vec![OwnedWrite {
                    offset, bytes: bytes::Bytes::from(buffer),
                }]).await.map_err(|error| ObjectFailure::new(error.into(), work))?;
            }
            Ok(())
        }.await;
        if let Err(error) = result {
            let mut index = self.index_mut();
            for (object_id, _) in &victims {
                if let Some(Staged::Resident { order, .. }) = index.objects.get(object_id) {
                    let order = *order;
                    index.order.insert(order, *object_id);
                }
            }
            return Err(error);
        }
        self.index_mut().mark_spilled(&victims, offset);
        Ok(ObjectReceipt { value: (), work: if native.is_some() { WorkCounters::default() } else { work } })
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
                HashedObject::verify(object_id, bytes.into()).ok_or(ObjectStoreError::Corrupt)
            })
            .collect()
    }
}

impl<S: AsyncObjectStore> StagedObjects<S> {
    /// Spills the least recently used resident objects once the window
    /// overflows and, while the spill file is past its bound, drains one
    /// batch of spilled objects durably.
    ///
    /// Ordinary overflow housekeeping retains its independent bounded step.
    /// Native overflow stays on the admission's original ledger and token,
    /// including the durable drain; failure leaves the remainder staged.
    async fn relieve_window(
        &self, budget: WorkBudget, cancellation: &CancellationToken,
        native: Option<&NativeMaterializationWork>,
    ) -> ObjectResult<()> {
        let _spill_file = self.spill_io.write().await;
        let spilled = self.spill_locked(budget, cancellation, native).await?;
        let (_target_control, targets) = {
            let index = self.index();
            if index.spill_end <= MAXIMUM_SPILL_BYTES {
                return Ok(spilled);
            }
            let mut batch_bytes = 0_u64;
            let mut count = 0;
            for staged in index.objects.values() {
                if let Some(native) = native {
                    native.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                        .map_err(|error| ObjectFailure::before_work(error.into()))?;
                }
                if let Staged::Spilled { length, .. } = staged {
                    if count != 0 && batch_bytes.saturating_add(*length) > MAXIMUM_DRAIN_BYTES { break; }
                    batch_bytes = batch_bytes.saturating_add(*length);
                    count += 1;
                }
            }
            let (control, mut targets) = staged_vector(count, native).map_err(ObjectFailure::before_work)?;
            for (&object_id, staged) in &index.objects {
                if targets.len() == count { break; }
                if let Some(native) = native {
                    native.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                        .map_err(|error| ObjectFailure::before_work(error.into()))?;
                }
                if matches!(staged, Staged::Spilled { .. }) { targets.push(object_id); }
            }
            (control, targets)
        };
        let unswept = self.collection.unswept().await;
        if let Some(native) = native {
            self.drain_locked(&unswept, &targets, budget, cancellation, Some(native)).await?;
        } else {
            let _step = self.drain_locked(
                &unswept, &targets, WorkBudget::UNBOUNDED, &CancellationToken::new(), None,
            ).await;
        }
        Ok(spilled)
    }

    /// Durably admits the staged objects among `targets` in segment-bounded
    /// batches. The caller holds the spill file exclusively. A batch leaves
    /// staging only once the provider acknowledges it, so a failed drain
    /// keeps the remainder private and retryable. The spill file is reused
    /// once no staged object remains spilled.
    ///
    /// A target drained since the last sweep leaves staging unwritten: the
    /// caller's `unswept` keeps any sweep from running until the record
    /// naming it is written, and a running collection is told it is being
    /// kept, exactly as if it were written again.
    async fn drain_locked(
        &self, unswept: &crate::collection::Unswept, targets: &[ObjectId],
        budget: WorkBudget, cancellation: &CancellationToken,
        native: Option<&NativeMaterializationWork>,
    ) -> ObjectResult<()> {
        let mut work = WorkCounters::default();
        {
            let mut index = self.index_mut();
            let sweeps = self.collection.sweeps();
            if index.drained_at != sweeps {
                index.drained.clear();
                index.drained_at = sweeps;
            }
        }
        let mut position = 0;
        loop {
            cancellation.check().map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
            let mut end = position;
            let mut count = 0;
            {
                let index = self.index();
                let mut bytes = 0_u64;
                for &object_id in &targets[position..] {
                    if let Some(native) = native {
                        native.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                            .map_err(|error| ObjectFailure::new(error.into(), work))?;
                    }
                    if let Some(staged) = index.objects.get(&object_id) {
                        if !index.drained.contains(&object_id) {
                            if count != 0 && bytes.saturating_add(staged.length()) > MAXIMUM_DRAIN_BYTES { break; }
                            bytes = bytes.saturating_add(staged.length());
                            count += 1;
                        }
                    }
                    end += 1;
                }
            }
            let (_write_control, mut writes) = staged_vector::<ObjectWrite>(count, native)
                .map_err(|error| ObjectFailure::new(error, work))?;
            let (_spilled_control, mut spilled) = staged_vector::<(ObjectId, u64, u64)>(count, native)
                .map_err(|error| ObjectFailure::new(error, work))?;
            {
                let mut index = self.index_mut();
                for &object_id in &targets[position..end] {
                    if let Some(native) = native {
                        native.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                            .map_err(|error| ObjectFailure::new(error.into(), work))?;
                    }
                    if index.drained.contains(&object_id) {
                        self.collection.writing(unswept, [object_id]);
                        index.remove(object_id);
                        continue;
                    }
                    match index.objects.get(&object_id) {
                        Some(Staged::Resident { bytes, .. }) => writes.push(ObjectWrite {
                            object_id, bytes: bytes.clone(),
                        }),
                        Some(&Staged::Spilled { offset, length }) => spilled.push((object_id, offset, length)),
                        None => {}
                    }
                }
            }
            position = end;
            if writes.is_empty() && spilled.is_empty() { break; }
            if let Some(native) = native {
                for (object_id, offset, length) in spilled {
                    cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
                    let hashed = length.checked_add(crate::storage::OBJECT_DIGEST_ENVELOPE_BYTES)
                        .ok_or_else(|| ObjectFailure::before_work(crate::WorkError::Overflow.into()))?;
                    native.admit(WorkCounters { object_bytes_read: length, bytes_hashed: hashed, ..WorkCounters::default() })
                        .map_err(|error| ObjectFailure::before_work(error.into()))?;
                    let length_usize = usize::try_from(length)
                        .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Corrupt))?;
                    let bytes = staged_native_read(&self.spill, offset, length_usize, native).await
                        .map_err(ObjectFailure::before_work)?;
                    let observed = bytes.len() as u64;
                    native.record_admitted(WorkCounters { object_bytes_read: observed, ..WorkCounters::default() })
                        .map_err(|error| ObjectFailure::before_work(error.into()))?;
                    if observed != length { return Err(ObjectFailure::before_work(ObjectStoreError::Corrupt)); }
                    native.charge(WorkCounters { bytes_hashed: hashed, ..WorkCounters::default() })
                        .map_err(|error| ObjectFailure::before_work(error.into()))?;
                    if crate::storage::object_digest(object_id.kind, &bytes) != object_id.digest {
                        return Err(ObjectFailure::before_work(ObjectStoreError::Corrupt));
                    }
                    writes.push(ObjectWrite { object_id, bytes });
                }
                // A native batch has no partial-success receipt. Admit one
                // object at a time and remove only the acknowledged prefix.
                for write in &writes {
                    cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
                    self.collection.writing(unswept, [write.object_id]);
                    self.inner.put_with_native_work(write.object_id, write.bytes.clone(), native, cancellation)
                        .await.map_err(ObjectFailure::before_work)?;
                    self.index_mut().acknowledge(write.object_id);
                }
            } else {
                if !spilled.is_empty() {
                    let spilled_bytes = spilled.iter().fold(0_u64, |total, &(_, _, length)| total.saturating_add(length));
                    work = work.checked_add(WorkCounters {
                        backend_read_operations: 1, object_bytes_read: spilled_bytes,
                        bytes_hashed: spilled_bytes, ..WorkCounters::default()
                    }).map_err(|error| ObjectFailure::new(error.into(), work))?;
                    work.verify(budget).map_err(|error| ObjectFailure::new(error.into(), work))?;
                    let read = self.read_spilled(&spilled).await.map_err(|error| ObjectFailure::new(error, work))?;
                    writes.extend(read.into_iter().map(|object| {
                        let (object_id, bytes) = object.into_parts();
                        ObjectWrite { object_id, bytes }
                    }));
                }
                self.collection.writing(unswept, writes.iter().map(|write| write.object_id));
                let receipt = self.inner.put_many(&writes,
                    work.remaining(budget).map_err(|error| ObjectFailure::new(error.into(), work))?, cancellation,
                ).await.map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                // Cleanup cannot be skipped by cancellation after acknowledgement.
                for write in &writes { self.index_mut().acknowledge(write.object_id); }
                work = work.checked_add(receipt.work).map_err(|error| ObjectFailure::new(error.into(), work))?;
            }
        }
        let truncate = {
            let index = self.index();
            index.spilled_objects == 0 && index.spill_end != 0
        };
        if truncate {
            if let Some(native) = native {
                self.spill.set_len_async(0, Some(native.runtime_meter_owner()))
                    .map_err(|error| ObjectFailure::before_work(native.runtime_error(error).into()))?
                    .await.map_err(|error| ObjectFailure::before_work(native.runtime_error(error).into()))?;
            } else {
                self.spill.set_len_async(0, None).expect("unmetered unit construction cannot fail").await.map_err(|error| ObjectFailure::new(error.into(), work))?;
            }
            self.index_mut().spill_end = 0;
        }
        Ok(ObjectReceipt { value: (), work })
    }

    async fn put_engine(
        &self, object_id: ObjectId, bytes: OwnedBytes, budget: WorkBudget,
        cancellation: &CancellationToken, native: Option<&NativeMaterializationWork>,
    ) -> ObjectResult<()> {
        cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let hashed = byte_length(&bytes).checked_add(
            if native.is_some() { crate::storage::OBJECT_DIGEST_ENVELOPE_BYTES } else { 0 },
        ).ok_or_else(|| ObjectFailure::before_work(crate::WorkError::Overflow.into()))?;
        let verified = WorkCounters { bytes_hashed: hashed, ..WorkCounters::default() };
        if let Some(native) = native {
            native.charge(verified).map_err(|error| ObjectFailure::before_work(error.into()))?;
        } else {
            verified.verify(budget).map_err(|error| ObjectFailure::before_work(error.into()))?;
        }
        let prior = if native.is_some() { WorkCounters::default() } else { verified };
        let object = HashedObject::verify(object_id, bytes)
            .ok_or_else(|| ObjectFailure::new(ObjectStoreError::DigestMismatch, prior))?;
        let admitted = self.put_hashed_engine(object,
            prior.remaining(budget).map_err(|error| ObjectFailure::new(error.into(), prior))?,
            cancellation, native,
        ).await.map_err(|failure| failure.map_with_prior_work(prior, std::convert::identity))?;
        Ok(ObjectReceipt { value: (), work: prior.checked_add(admitted.work)
            .map_err(|error| ObjectFailure::new(error.into(), prior))? })
    }

    async fn put_hashed_engine(
        &self, object: HashedObject, budget: WorkBudget,
        cancellation: &CancellationToken, native: Option<&NativeMaterializationWork>,
    ) -> ObjectResult<()> {
        cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if let Some(native) = native { native.remaining().map_err(|error| ObjectFailure::before_work(error.into()))?; }
        if object.length() > MAXIMUM_DRAIN_BYTES {
            let mut allocation = None;
            let (mut future, _) = crate::heap_future::in_heap_admitted(
                native,
                |native, bytes| {
                    allocation = native.map(|native| native.claim(bytes)).transpose()?;
                    Ok::<_, NativeTreeMaterializationError>(())
                },
                |native| async move {
                    let unswept = self.collection.unswept().await;
                    self.collection.writing(&unswept, [object.object_id()]);
                    if let Some(native) = native {
                        self.inner.put_hashed_with_native_work(object, native, cancellation).await
                            .map_err(ObjectFailure::before_work)?;
                        Ok(ObjectReceipt { value: (), work: WorkCounters::default() })
                    } else {
                        self.inner.put_hashed(object, budget, cancellation).await
                    }
                },
            ).map_err(|error| ObjectFailure::before_work(error.into()))?;
            let result = future.as_mut().await;
            drop(future);
            drop(allocation);
            return result;
        }
        // OwnedBytes already owns its intrinsic custody. Staging clones or moves
        // that physical owner; it never adopts the incoming allocation again.
        let overflowing = {
            let (object_id, bytes) = object.into_parts();
            let mut index = self.index_mut();
            index.admit_resident(object_id, bytes) && index.resident_bytes > MAXIMUM_RESIDENT_BYTES
        };
        if !overflowing { return Ok(ObjectReceipt { value: (), work: WorkCounters::default() }); }
        let mut allocation = None;
        let (mut future, _) = crate::heap_future::in_heap_admitted(
            native,
            |native, bytes| {
                allocation = native.map(|native| native.claim(bytes)).transpose()?;
                Ok::<_, NativeTreeMaterializationError>(())
            },
            |native| self.relieve_window(budget, cancellation, native),
        ).map_err(|error| ObjectFailure::before_work(error.into()))?;
        let result = future.as_mut().await;
        drop(future);
        drop(allocation);
        result
    }

    async fn put_many_engine(
        &self, writes: &[ObjectWrite], budget: WorkBudget,
        cancellation: &CancellationToken, native: Option<&NativeMaterializationWork>,
    ) -> ObjectResult<()> {
        if writes.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected("object write batch is empty".to_owned())));
        }
        let mut work = WorkCounters::default();
        for write in writes {
            let receipt = self.put_engine(write.object_id, write.bytes.clone(),
                work.remaining(budget).map_err(|error| ObjectFailure::new(error.into(), work))?,
                cancellation, native,
            ).await.map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = work.checked_add(receipt.work).map_err(|error| ObjectFailure::new(error.into(), work))?;
        }
        Ok(ObjectReceipt { value: (), work })
    }

    async fn flush_engine(
        &self, scope: PublicationScope<'_>, budget: WorkBudget,
        cancellation: &CancellationToken, native: Option<&NativeMaterializationWork>,
    ) -> ObjectResult<crate::PublicationHold> {
        cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let span = crate::obs::span!(
            DEBUG, "acyclic.fs.staged.flush",
            outcome = crate::obs::Empty, error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty, work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
        );
        let result = crate::obs::in_span(&span, async move {
            let _spill_file = self.spill_io.write().await;
            let (objects, proven_at) = match scope {
                PublicationScope::Closure { objects, proven_at } => (objects, proven_at),
                PublicationScope::Everything => (&[][..], self.collection.sweeps()),
            };
            let unswept = self.collection.admit(
                objects, |object_id| self.index().objects.contains_key(object_id), proven_at,
            ).await.map_err(ObjectFailure::before_work)?;
            let (_target_control, targets) = {
                let index = self.index();
                let capacity = match scope {
                    PublicationScope::Closure { objects, .. } => objects.len(),
                    PublicationScope::Everything => index.objects.len(),
                };
                let (control, mut targets) = staged_vector(capacity, native).map_err(ObjectFailure::before_work)?;
                match scope {
                    PublicationScope::Closure { objects, .. } => {
                        for &object_id in objects {
                            if let Some(native) = native {
                                native.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                                    .map_err(|error| ObjectFailure::before_work(error.into()))?;
                            }
                            if index.objects.contains_key(&object_id) { targets.push(object_id); }
                        }
                    },
                    PublicationScope::Everything => {
                        for &object_id in index.objects.keys() {
                            if let Some(native) = native {
                                native.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                                    .map_err(|error| ObjectFailure::before_work(error.into()))?;
                            }
                            targets.push(object_id);
                        }
                    },
                }
                (control, targets)
            };
            let drained = self.drain_locked(&unswept, &targets, budget, cancellation, native).await?;
            Ok(ObjectReceipt { value: unswept.into_hold(), work: drained.work })
        }).await;
        crate::obs::measured_on(&span, result, |receipt| &receipt.work)
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
        native: Option<&NativeMaterializationWork>,
    ) -> Result<ObjectReceipt<ObjectRead>, (ObjectStoreError, WorkCounters)> {
        // Sharing the spill file keeps a drain from truncating or reusing it
        // until this read completes.
        let spill_file = self.spill_io.read().await;
        let (offset, length) = match self.lookup(object_id) {
            Lookup::Spilled { offset, length } => (offset, length),
            Lookup::Resident(bytes) => return resident_read(bytes, maximum_bytes, budget, native),
            Lookup::Absent => {
                drop(spill_file);
                return self.read_durable(object_id, maximum_bytes, budget, cancellation, native).await;
            }
        };
        if length > maximum_bytes {
            return Err((ObjectStoreError::TooLarge {
                observed: length, maximum: maximum_bytes,
            }, WorkCounters::default()));
        }
        let work = WorkCounters {
            backend_read_operations: 1,
            object_bytes_read: length,
            bytes_hashed: length,
            ..WorkCounters::default()
        };
        let bytes = if let Some(native) = native {
            cancellation.check().map_err(|_| (ObjectStoreError::Cancelled, WorkCounters::default()))?;
            native.admit(WorkCounters {
                object_bytes_read: length, bytes_hashed: length, ..WorkCounters::default()
            }).map_err(|error| (error.into(), WorkCounters::default()))?;
            let requested = usize::try_from(length).map_err(|_| (ObjectStoreError::Corrupt, WorkCounters::default()))?;
            let bytes = staged_native_read(&self.spill, offset, requested, native).await
                .map_err(|error| (error, WorkCounters::default()))?;
            let observed = bytes.len() as u64;
            native.record_admitted(WorkCounters {
                object_bytes_read: observed, ..WorkCounters::default()
            }).map_err(|error| (error.into(), WorkCounters::default()))?;
            native.charge(WorkCounters {
                bytes_hashed: observed, ..WorkCounters::default()
            }).map_err(|error| (error.into(), WorkCounters::default()))?;
            if observed != length || object_id.digest != crate::storage::object_digest(object_id.kind, &bytes) {
                return Err((ObjectStoreError::Corrupt, WorkCounters::default()));
            }
            bytes
        } else {
            work.verify(budget).map_err(|error| (error.into(), WorkCounters::default()))?;
            let object = self.read_spilled(&[(object_id, offset, length)]).await
                .map_err(|error| (error, work))?.pop().ok_or((ObjectStoreError::Corrupt, work))?;
            object.into_parts().1
        };
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
            value: ObjectRead { bytes, retention: ObjectReadRetention::Shared },
            work,
        })
    }

    async fn read_durable(
        &self, object_id: ObjectId, maximum_bytes: u64, budget: WorkBudget,
        cancellation: &CancellationToken, native: Option<&NativeMaterializationWork>,
    ) -> Result<ObjectReceipt<ObjectRead>, (ObjectStoreError, WorkCounters)> {
        if let Some(native) = native {
            return self.inner.read_with_native_work(object_id, maximum_bytes, native, cancellation)
                .await.map(|read| ObjectReceipt { value: read, work: WorkCounters::default() })
                .map_err(|error| (error, WorkCounters::default()));
        }
        self.inner.read(object_id, maximum_bytes, budget, cancellation).await
            .map_err(|failure| (failure.error, *failure.work))
    }

    async fn read_engine(
        &self, object_id: ObjectId, maximum_bytes: u64, budget: WorkBudget,
        cancellation: &CancellationToken, native: Option<&NativeMaterializationWork>,
    ) -> Result<ObjectReceipt<ObjectRead>, (ObjectStoreError, WorkCounters)> {
        cancellation.check().map_err(|_| (ObjectStoreError::Cancelled, WorkCounters::default()))?;
        match self.lookup(object_id) {
            Lookup::Resident(bytes) => resident_read(bytes, maximum_bytes, budget, native),
            // Keep the canonical resident fast path out of the larger I/O frame.
            Lookup::Absent => {
                let mut allocation = None;
                let (mut future, _) = crate::heap_future::in_heap_admitted(
                    native,
                    |native, bytes| {
                        allocation = native.map(|native| native.claim(bytes)).transpose()?;
                        Ok::<_, NativeTreeMaterializationError>(())
                    },
                    |native| self.read_durable(object_id, maximum_bytes, budget, cancellation, native),
                ).map_err(|error| (error.into(), WorkCounters::default()))?;
                let result = future.as_mut().await;
                drop(future);
                drop(allocation);
                result
            }
            Lookup::Spilled { .. } => {
                let mut allocation = None;
                let (mut future, _) = crate::heap_future::in_heap_admitted(
                    native,
                    |native, bytes| {
                        allocation = native.map(|native| native.claim(bytes)).transpose()?;
                        Ok::<_, NativeTreeMaterializationError>(())
                    },
                    |native| self.read_spilled_object(object_id, maximum_bytes, budget, cancellation, native),
                ).map_err(|error| (error.into(), WorkCounters::default()))?;
                let result = future.as_mut().await;
                drop(future);
                drop(allocation);
                result
            }
        }
    }

    fn decoded_get(
        &self, key: DecodedCacheKey, native: Option<&NativeMaterializationWork>,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        if let Some(native) = native { native.remaining()?; }
        match self.index().objects.get(&key.object_id) {
            Some(Staged::Resident { decoded, .. }) => Ok(decoded.get()
                .filter(|(cached, _)| *cached == key).map(|(_, value)| value.clone())),
            Some(Staged::Spilled { .. }) => Ok(None),
            None => match native {
                Some(native) => self.inner.decoded_cache_get_with_native_work(key, native),
                None => self.inner.decoded_cache_get(key),
            },
        }
    }

    fn decoded_admit(
        &self, key: DecodedCacheKey, value: DecodedCacheValue, native: Option<&NativeMaterializationWork>,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        if let Some(native) = native { native.remaining()?; }
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
            None => match native {
                Some(native) => self.inner.decoded_cache_admit_with_native_work(key, value, native),
                None => self.inner.decoded_cache_admit(key, value),
            },
        }
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
        self.decoded_get(key, None)
    }

    fn decoded_cache_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        self.decoded_admit(key, value, None)
    }

    fn decoded_cache_get_with_native_work(
        &self, key: DecodedCacheKey, work: &NativeMaterializationWork,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        self.decoded_get(key, Some(work))
    }

    fn decoded_cache_admit_with_native_work(
        &self, key: DecodedCacheKey, value: DecodedCacheValue, work: &NativeMaterializationWork,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        self.decoded_admit(key, value, Some(work))
    }

    async fn put(
        &self, object_id: ObjectId, bytes: OwnedBytes, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.put_engine(object_id, bytes, budget, cancellation, None).await
    }

    async fn put_hashed(
        &self, object: HashedObject, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.put_hashed_engine(object, budget, cancellation, None).await
    }

    async fn put_many(
        &self, writes: &[ObjectWrite], budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.put_many_engine(writes, budget, cancellation, None).await
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
        &self, scope: PublicationScope<'_>, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<crate::PublicationHold> {
        self.flush_engine(scope, budget, cancellation, None).await
    }

    async fn put_with_native_work(
        &self, object_id: ObjectId, bytes: OwnedBytes, work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_engine(object_id, bytes, work.remaining()?, cancellation, Some(work))
            .await.map(|_| ()).map_err(|failure| failure.error)
    }

    async fn put_hashed_with_native_work(
        &self, object: HashedObject, work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_hashed_engine(object, work.remaining()?, cancellation, Some(work))
            .await.map(|_| ()).map_err(|failure| failure.error)
    }

    async fn put_many_with_native_work(
        &self, writes: &[ObjectWrite], work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_many_engine(writes, work.remaining()?, cancellation, Some(work))
            .await.map(|_| ()).map_err(|failure| failure.error)
    }

    async fn flush_before_publish_with_native_work(
        &self, scope: PublicationScope<'_>, work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        let mut allocation = None;
        let (mut future, _) = crate::heap_future::in_heap_admitted(
            Some(work),
            |native, bytes| {
                allocation = native.map(|native| native.claim(bytes)).transpose()?;
                Ok::<_, NativeTreeMaterializationError>(())
            },
            |native| self.flush_engine(scope, WorkBudget::default(), cancellation, native),
        )?;
        let result = future.as_mut().await.map(|receipt| receipt.value).map_err(|failure| failure.error);
        drop(future);
        drop(allocation);
        result
    }

    async fn prepare_generation_publication(
        &self, request: &crate::kernel::PublishGenerationRequest,
        proof: &crate::kernel::GenerationProof, append: &crate::GuardedAppend,
        scope: crate::PublicationScope<'_>, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.inner.prepare_generation_publication(
            request, proof, append, scope, budget, cancellation,
        ).await
    }

    async fn prepare_generation_publication_with_native_work(
        &self, request: &crate::kernel::PublishGenerationRequest,
        proof: &crate::kernel::GenerationProof, append: &crate::GuardedAppend,
        scope: crate::PublicationScope<'_>, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.inner.prepare_generation_publication_with_native_work(
            request, proof, append, scope, original, cancellation,
        ).await
    }

    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        Some(&self.collection)
    }

    async fn historical_generation_evidence(
        &self,
        authority_id: crate::AuthorityId,
        record: &crate::DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Option<crate::kernel::HistoricalGenerationEvidence>> {
        self.inner
            .historical_generation_evidence(
                authority_id, record, maximum_bytes, maximum_objects, budget, cancellation,
            )
            .await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        self.read_engine(object_id, maximum_bytes, budget, cancellation, None)
            .await.map_err(|(error, work)| ObjectFailure::new(error, work))
    }

    async fn read_with_native_work(
        &self, object_id: ObjectId, maximum_bytes: u64,
        work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let mut read = self.read_engine(object_id, maximum_bytes, work.remaining()?, cancellation, Some(work))
            .await.map_err(|(error, _)| error)?.value;
        read.retention = ObjectReadRetention::Shared;
        Ok(read)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        crate::async_storage::read_many_sequential_async(self, requests, budget, cancellation).await
    }

    async fn read_many_with_native_work(
        &self, requests: &[ObjectReadRequest],
        work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(Vec<ObjectRead>, crate::materializer::NativeAllocation), ObjectStoreError> {
        crate::async_storage::read_many_sequential_with_native_work(self, requests, work, cancellation).await
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

fn byte_length(bytes: &OwnedBytes) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LogicalObjectStore;
    use crate::kernel::DecodeLimits;
    use crate::storage::{ObjectKind, object_digest};
    use acyclic_objects::v1::ObjectsProvider as _;
    use std::sync::Arc;

    type LocalStaged = StagedObjects<LogicalObjectStore<acyclic_objects::v1::local::LocalObjects>>;

    async fn open_staged(
        directory: &Path,
    ) -> Result<
        (LocalStaged, Arc<acyclic_objects::v1::local::LocalObjects>),
        Box<dyn std::error::Error>,
    > {
        let provider = Arc::new(
            acyclic_objects::v1::local::LocalObjects::open(
                directory.join("objects"),
                acyclic_objects::LocalObjectsLimits::default(),
                None,
            )
            .await?,
        );
        let bucket = provider
            .create_bucket(acyclic_objects::v1::wire::CreateBucketRequest {
                name: "staged-test".to_owned(),
                mutation: None,
            })
            .await?
            .bucket
            .ok_or("bucket creation returned no bucket")?;
        let store = StagedObjects::open(
            LogicalObjectStore::new(Arc::clone(&provider), bucket),
            directory.to_path_buf(),
        )
        .await?;
        Ok((store, provider))
    }

    async fn reopen_contains(
        directory: &Path,
        objects: &[ObjectId],
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let reopened = acyclic_objects::v1::local::LocalObjects::open(
            directory.join("objects"),
            acyclic_objects::LocalObjectsLimits::default(),
            None,
        )
        .await?;
        let bucket = acyclic_objects::v1::wire::BucketRef {
            name: "staged-test".to_owned(),
        };
        reopened
            .head_bucket(acyclic_objects::v1::wire::HeadBucketRequest {
                bucket: Some(bucket.clone()),
            })
            .await?;
        let reopened = LogicalObjectStore::new(Arc::new(reopened), bucket);
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

    fn blob(bytes: OwnedBytes) -> (ObjectId, OwnedBytes) {
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
        let (object_id, bytes) = blob(OwnedBytes::from_static(b"staged immutable body"));
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
                blob(OwnedBytes::from(vec![
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
                blob(OwnedBytes::from(vec![
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
                    OwnedBytes::from(vec![u8::try_from(index % 251).unwrap_or(0); body_bytes]),
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
    async fn each_publication_writes_only_what_is_not_already_durable()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        // Every new file restages the same default metadata; each
        // publication must write only its new objects, so the work per
        // publication stays constant however many came before.
        let (shared, shared_bytes) = blob(OwnedBytes::from_static(b"shared metadata"));
        let publish = |index: usize| {
            let store = &store;
            let token = &token;
            let shared_bytes = shared_bytes.clone();
            async move {
                let (fresh, fresh_bytes) = blob(OwnedBytes::from(format!("file body {index:04}")));
                store
                    .put(shared, shared_bytes, WorkBudget::UNBOUNDED, token)
                    .await?;
                store
                    .put(fresh, fresh_bytes, WorkBudget::UNBOUNDED, token)
                    .await?;
                let flushed = store
                    .flush_before_publish(
                        PublicationScope::Closure {
                            objects: &[shared, fresh],
                            proven_at: store.collection.sweeps(),
                        },
                        WorkBudget::UNBOUNDED,
                        token,
                    )
                    .await?;
                Ok::<_, Box<dyn std::error::Error>>((fresh, flushed.work))
            }
        };
        let (_, first) = publish(0).await?;
        assert_eq!(first.object_bytes_written, 15 + 14);
        let mut published = Vec::new();
        for index in 1..32 {
            let (fresh, work) = publish(index).await?;
            assert_eq!(work.backend_write_operations, 1);
            assert_eq!(work.backend_read_operations, 0);
            assert_eq!(work.object_bytes_written, 14);
            assert!(store.index().objects.is_empty());
            published.push(fresh);
        }

        // A sweep may have removed it, so the next publication writes it.
        let collecting = store.collection.begin().await;
        let (gate, _) = collecting.sweepable(vec![shared]).await;
        collecting.sweeping(shared);
        provider
            .delete(acyclic_objects::v1::wire::DeleteObjectRequest {
                bucket: Some(acyclic_objects::v1::wire::BucketRef {
                    name: "staged-test".to_owned(),
                }),
                object_key: crate::distributed::object_key(shared),
                ..Default::default()
            })
            .await?;
        drop(gate);
        drop(collecting);
        let (fresh, work) = publish(32).await?;
        assert_eq!(work.object_bytes_written, 15 + 14);
        published.extend([shared, fresh]);
        drop(store);
        drop(provider);
        assert!(reopen_contains(directory.path(), &published).await?);
        Ok(())
    }

    #[tokio::test]
    async fn hashed_admission_skips_rehashing_and_unproven_bytes_are_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, _provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        let object = HashedObject::new(ObjectKind::Blob, OwnedBytes::from_static(b"proven body"));
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
            native_custody: None,
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
                OwnedBytes::from_static(b"other body"),
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
                    OwnedBytes::from(vec![u8::try_from(index % 251).unwrap_or(0); body_bytes]),
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

    fn native_read_budget() -> WorkBudget {
        WorkBudget {
            backend_read_operations: 8,
            object_probes: 64,
            object_bytes_read: 65_536,
            bytes_hashed: 65_536,
            bytes_copied: 65_536,
            source_bytes_read: 65_536,
            items_examined: 4_096,
            items_returned: 256,
            allocation_operations: 256,
            peak_allocation_bytes: 1_048_576,
            ..WorkBudget::default()
        }
    }

    async fn spill_fixture(store: &LocalStaged, token: &CancellationToken)
    -> Result<(ObjectId, OwnedBytes), Box<dyn std::error::Error>> {
        let (id, bytes) = blob(OwnedBytes::from_static(b"real spill bytes"));
        store.put(id, bytes.clone(), WorkBudget::UNBOUNDED, token).await?;
        let _spill_file = store.spill_io.write().await;
        let spill_bytes = bytes.clone();
        store.spill.control_async(move |file, _| {
            acyclic_native_runtime::write_all_at(file, 0, spill_bytes.as_slice(), None)
        }, None).expect("unmetered control construction cannot fail").await?;
        let mut index = store.index_mut();
        if let Some(Staged::Resident { order, .. }) = index.objects.get(&id) {
            let order = *order;
            index.order.remove(&order);
        }
        index.mark_spilled(&[(id, bytes.clone())], 0);
        Ok((id, bytes))
    }

    #[tokio::test]
    async fn native_spill_alias_retains_actual_buffer_after_cache_and_store_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, _provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        let (id, bytes) = spill_fixture(&store, &token).await?;
        let work = NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(error, _)| error)?;
        let read = store.read_with_native_work(id, bytes.len() as u64, &work, &token).await?;
        assert_eq!(read.bytes, bytes);
        let alias = read.bytes.clone();
        let held = work.live_allocation_bytes();
        assert!(held >= NativeMaterializationWork::accounting_state_bytes() + bytes.len() as u64);
        assert_eq!(work.receipt().backend_read_operations, 1);
        assert_eq!(work.receipt().object_bytes_read, bytes.len() as u64);
        let prefix = work.receipt();
        drop(store.read_with_native_work(id, bytes.len() as u64, &work, &token).await?);
        assert_eq!(work.receipt().backend_read_operations, prefix.backend_read_operations);
        assert_eq!(work.receipt().allocation_operations, prefix.allocation_operations);
        drop(read);
        drop(store);
        assert_eq!(work.live_allocation_bytes(), held);
        assert_eq!(alias, bytes);
        drop(alias);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn native_spill_denial_corruption_and_abandoned_frame_preserve_original_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, _provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        let (id, bytes) = spill_fixture(&store, &token).await?;
        let mut budget = native_read_budget();
        budget.backend_read_operations = 0;
        let denied = NativeMaterializationWork::new(budget, &token).map_err(|(error, _)| error)?;
        assert!(matches!(store.read_with_native_work(id, bytes.len() as u64, &denied, &token).await,
            Err(ObjectStoreError::Native(NativeTreeMaterializationError::Work(
                crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. })))));
        assert_eq!(denied.receipt().backend_read_operations, 0);
        assert_eq!(denied.receipt().object_bytes_read, 0);
        assert_eq!(denied.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());

        let work = NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(error, _)| error)?;
        let spill_file = store.spill_io.write().await;
        let mut future = Box::pin(store.read_with_native_work(id, bytes.len() as u64, &work, &token));
        assert!(Future::poll(future.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())).is_pending());
        assert!(work.live_allocation_bytes() > NativeMaterializationWork::accounting_state_bytes());
        drop(future);
        drop(spill_file);
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());

        store.spill.write_all_batch_async(vec![OwnedWrite {
            offset: 0, bytes: bytes::Bytes::from_static(b"bad! spill bytes"),
        }]).await?;
        let work = NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(error, _)| error)?;
        assert!(matches!(store.read_with_native_work(id, bytes.len() as u64, &work, &token).await,
            Err(ObjectStoreError::Corrupt)));
        assert_eq!(work.receipt().backend_read_operations, 1);
        assert_eq!(work.receipt().object_bytes_read, bytes.len() as u64);
        assert_eq!(work.receipt().bytes_hashed, bytes.len() as u64);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        assert!(matches!(store.lookup(id), Lookup::Spilled { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn native_staged_decoded_alias_keeps_original_guard_after_resident_ejection()
    -> Result<(), Box<dyn std::error::Error>> {
        struct Page { _custody: Arc<NativeAllocation> }
        let directory = tempfile::tempdir()?;
        let (store, _provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        let (id, bytes) = blob(OwnedBytes::from_static(b"resident native page"));
        store.put(id, bytes.clone(), WorkBudget::UNBOUNDED, &token).await?;
        let work = NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(error, _)| error)?;
        let initial = work.receipt().allocation_operations;
        let read = store.read_with_native_work(id, bytes.len() as u64, &work, &token).await?;
        assert_eq!(read.bytes.as_ptr(), bytes.as_ptr());
        assert_eq!(work.receipt().allocation_operations, initial);
        assert_eq!(work.receipt().backend_read_operations, 0);
        let mut allocation = work.claim(std::mem::size_of::<Page>() as u64)?;
        let mut control = work.claim(std::mem::size_of::<NativeAllocation>() as u64)?;
        allocation.absorb(&mut control)?;
        let custody = Arc::new(allocation);
        let page = Arc::new(Page { _custody: Arc::clone(&custody) });
        let key = DecodedCacheKey::new::<Page>(id, DecodeLimits::default());
        let prefix = work.receipt();
        drop(store.decoded_cache_admit_with_native_work(key, DecodedCacheValue {
            value: page, logical_bytes: std::mem::size_of::<Page>() as u64, native_custody: Some(custody),
        }, &work)?);
        let cached = store.decoded_cache_get_with_native_work(key, &work)?.ok_or("resident native decoded miss")?;
        assert_eq!(work.receipt().allocation_operations, prefix.allocation_operations);
        let alias = Arc::clone(&cached.value);
        let held = work.live_allocation_bytes();
        drop(cached);
        store.index_mut().remove(id);
        assert_eq!(work.live_allocation_bytes(), held);
        drop(alias);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn native_staged_batch_preserves_two_actual_scalar_spill_reads_and_header_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, _provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        let (first, first_bytes) = spill_fixture(&store, &token).await?;
        let (second, second_bytes) = blob(OwnedBytes::from_static(b"second spill bytes"));
        store.put(second, second_bytes.clone(), WorkBudget::UNBOUNDED, &token).await?;
        {
            let _spill_file = store.spill_io.write().await;
            let offset = store.index().spill_end;
            let spill_bytes = second_bytes.clone();
            store.spill.control_async(move |file, _| {
                acyclic_native_runtime::write_all_at(file, offset, spill_bytes.as_slice(), None)
            }, None).expect("unmetered control construction cannot fail").await?;
            let mut index = store.index_mut();
            if let Some(Staged::Resident { order, .. }) = index.objects.get(&second) {
                let order = *order;
                index.order.remove(&order);
            }
            index.mark_spilled(&[(second, second_bytes.clone())], offset);
        }
        let work = NativeMaterializationWork::new(native_read_budget(), &token).map_err(|(error, _)| error)?;
        let requests = [
            ObjectReadRequest { object_id: first, maximum_bytes: first_bytes.len() as u64 },
            ObjectReadRequest { object_id: second, maximum_bytes: second_bytes.len() as u64 },
        ];
        let batch = store.read_many_with_native_work(&requests, &work, &token).await?;
        assert_eq!(batch.0.len(), 2);
        assert_eq!(batch.0[0].bytes, first_bytes);
        assert_eq!(batch.0[1].bytes, second_bytes);
        // Staging's canonical ordinary convention is sequential scalar reads;
        // native attribution must retain both real submissions, not invent one.
        assert_eq!(work.receipt().backend_read_operations, 2);
        assert_eq!(work.receipt().object_bytes_read, (first_bytes.len() + second_bytes.len()) as u64);
        let alias = batch.0[0].bytes.clone();
        let (values, allocation) = batch;
        assert_eq!(allocation.bytes(), (values.capacity() * std::mem::size_of::<ObjectRead>()) as u64);
        drop(values);
        drop(allocation);
        drop(store);
        assert!(work.live_allocation_bytes() > NativeMaterializationWork::accounting_state_bytes());
        drop(alias);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }
    fn native_write_budget() -> WorkBudget {
        WorkBudget {
            backend_read_operations: 128,
            backend_write_operations: 128,
            durability_operations: 128,
            object_probes: 256,
            object_bytes_read: 64 * 1_048_576,
            object_bytes_written: 64 * 1_048_576,
            source_bytes_read: 64 * 1_048_576,
            output_bytes: 64 * 1_048_576,
            bytes_hashed: 128 * 1_048_576,
            bytes_copied: 128 * 1_048_576,
            items_examined: 8_192,
            items_returned: 256,
            allocation_operations: 1_024,
            peak_allocation_bytes: 64 * 1_048_576,
            ..WorkBudget::default()
        }
    }

    #[tokio::test]
    async fn native_flush_exact_closure_preserves_intrinsic_alias_without_readoption()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let token = CancellationToken::new();
        let work = NativeMaterializationWork::new(native_write_budget(), &token).map_err(|(error, _)| error)?;
        let (buffer, mut body) = staged_vector::<u8>(32, Some(&work))?;
        body.extend_from_slice(b"original physical byte owner");
        let bytes = staged_owned_bytes(body, buffer.ok_or("unfunded test buffer")?, &work)?;
        let object = HashedObject::new(ObjectKind::Blob, bytes.clone());
        let object_id = object.object_id();
        let store = StagedObjects::open(crate::MemoryObjectStore::default(), directory.path().to_path_buf()).await?;
        let before = work.receipt();
        store.put_hashed_with_native_work(object, &work, &token).await?;
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        let alias = match store.lookup(object_id) {
            Lookup::Resident(bytes) => bytes,
            _ => return Err("native admission did not remain resident".into()),
        };
        assert_eq!(alias.as_ptr(), bytes.as_ptr());
        let (unselected, obsolete) = blob(OwnedBytes::from_static(b"unpublished superseded object"));
        store.put_with_native_work(unselected, obsolete, &work, &token).await?;
        let closure = [object_id];
        let hold = store.flush_before_publish_with_native_work(
            PublicationScope::Closure { objects: &closure, proven_at: store.collection.sweeps() }, &work, &token,
        ).await?;
        assert!(!store.index().objects.contains_key(&object_id));
        assert!(store.index().objects.contains_key(&unselected));
        assert!(store.inner().contains(object_id, WorkBudget::UNBOUNDED, &token).await?.value);
        assert!(!store.inner().contains(unselected, WorkBudget::UNBOUNDED, &token).await?.value);
        assert_eq!(work.receipt().backend_write_operations, 1);
        drop(hold);
        drop(bytes);
        drop(store);
        assert!(work.live_allocation_bytes() > NativeMaterializationWork::accounting_state_bytes());
        drop(alias);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn native_spilled_flush_cleans_acknowledged_objects_and_reuses_spill()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        let (id, bytes) = spill_fixture(&store, &token).await?;
        let work = NativeMaterializationWork::new(native_write_budget(), &token).map_err(|(error, _)| error)?;
        let closure = [id];
        let hold = store.flush_before_publish_with_native_work(
            PublicationScope::Closure { objects: &closure, proven_at: store.collection.sweeps() }, &work, &token,
        ).await?;
        assert!(store.index().objects.is_empty());
        assert_eq!(store.index().spilled_objects, 0);
        assert_eq!(store.index().spill_end, 0);
        assert_eq!(work.receipt().object_bytes_read, bytes.len() as u64);
        assert!(work.receipt().backend_read_operations > 0);
        assert!(work.receipt().backend_write_operations > 0);
        assert!(work.receipt().durability_operations > 0);
        drop(hold);
        drop(store);
        drop(provider);
        assert!(reopen_contains(directory.path(), &closure).await?);
        Ok(())
    }

    #[tokio::test]
    async fn native_spill_denial_precedes_physical_write_and_restores_victims()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let store = StagedObjects::open(crate::MemoryObjectStore::default(), directory.path().to_path_buf()).await?;
        let token = CancellationToken::new();
        let mut ids = Vec::new();
        for marker in 0..3_u8 {
            let (id, bytes) = blob(OwnedBytes::from(vec![marker; 2 * 1_048_576]));
            store.index_mut().admit_resident(id, bytes);
            ids.push(id);
        }
        let mut budget = native_write_budget();
        budget.backend_write_operations = 0;
        let denied = NativeMaterializationWork::new(budget, &token).map_err(|(error, _)| error)?;
        let _spill_file = store.spill_io.write().await;
        assert!(store.spill_locked(WorkBudget::default(), &token, Some(&denied)).await.is_err());
        assert_eq!(denied.receipt().backend_write_operations, 0);
        assert_eq!(denied.receipt().object_bytes_written, 0);
        assert_eq!(store.index().spill_end, 0);
        assert_eq!(store.index().spilled_objects, 0);
        assert_eq!(store.index().order.len(), ids.len());
        let physical = store.spill.read_batch_async(vec![OwnedRead { offset: 0, length: 1 }]).await?;
        assert!(physical[0].is_empty());
        for id in &ids { assert!(matches!(store.lookup(*id), Lookup::Resident(_))); }
        let work = NativeMaterializationWork::new(native_write_budget(), &token).map_err(|(error, _)| error)?;
        store.spill_locked(WorkBudget::default(), &token, Some(&work)).await?;
        assert!(work.receipt().backend_write_operations > 0);
        assert_eq!(work.receipt().object_bytes_written, store.index().spill_end);
        assert!(store.index().spilled_objects > 0);
        Ok(())
    }

    #[tokio::test]
    async fn native_flush_spilled_read_denial_keeps_private_remainder()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let (store, _provider) = open_staged(directory.path()).await?;
        let token = CancellationToken::new();
        let (id, _) = spill_fixture(&store, &token).await?;
        let mut budget = native_write_budget();
        budget.backend_read_operations = 0;
        let work = NativeMaterializationWork::new(budget, &token).map_err(|(error, _)| error)?;
        let closure = [id];
        assert!(store.flush_before_publish_with_native_work(
            PublicationScope::Closure { objects: &closure, proven_at: store.collection.sweeps() }, &work, &token,
        ).await.is_err());
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert_eq!(work.receipt().object_bytes_read, 0);
        assert!(matches!(store.lookup(id), Lookup::Spilled { .. }));
        assert_eq!(store.index().spilled_objects, 1);
        Ok(())
    }

    struct CancelAfterNativePut {
        inner: crate::MemoryObjectStore,
        cancel_once: AtomicBool,
    }

    impl AsyncObjectStore for CancelAfterNativePut {
        async fn put(&self, id: ObjectId, bytes: OwnedBytes, budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<()> {
            AsyncObjectStore::put(&self.inner, id, bytes, budget, cancellation).await
        }
        async fn put_with_native_work(
            &self, id: ObjectId, bytes: OwnedBytes, work: &NativeMaterializationWork, cancellation: &CancellationToken,
        ) -> Result<(), ObjectStoreError> {
            AsyncObjectStore::put_with_native_work(&self.inner, id, bytes, work, cancellation).await?;
            if self.cancel_once.swap(false, Ordering::Relaxed) { cancellation.cancel(); }
            Ok(())
        }
        async fn read(&self, id: ObjectId, maximum: u64, budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<ObjectRead> {
            AsyncObjectStore::read(&self.inner, id, maximum, budget, cancellation).await
        }
        async fn read_many(&self, requests: &[ObjectReadRequest], budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<Vec<ObjectRead>> {
            AsyncObjectStore::read_many(&self.inner, requests, budget, cancellation).await
        }
        async fn contains(&self, id: ObjectId, budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<bool> {
            AsyncObjectStore::contains(&self.inner, id, budget, cancellation).await
        }
    }

    #[tokio::test]
    async fn native_flush_cancellation_removes_successful_prefix_only()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let store = StagedObjects::open(CancelAfterNativePut {
            inner: crate::MemoryObjectStore::default(), cancel_once: AtomicBool::new(true),
        }, directory.path().to_path_buf()).await?;
        let token = CancellationToken::new();
        let (first, first_bytes) = blob(OwnedBytes::from_static(b"acknowledged first"));
        let (second, second_bytes) = blob(OwnedBytes::from_static(b"retryable second"));
        let work = NativeMaterializationWork::new(native_write_budget(), &token).map_err(|(error, _)| error)?;
        store.put_with_native_work(first, first_bytes.clone(), &work, &token).await?;
        store.put_with_native_work(second, second_bytes, &work, &token).await?;
        let closure = [first, second];
        assert!(matches!(store.flush_before_publish_with_native_work(
            PublicationScope::Closure { objects: &closure, proven_at: store.collection.sweeps() }, &work, &token,
        ).await, Err(ObjectStoreError::Cancelled)));
        assert!(!store.index().objects.contains_key(&first));
        assert!(store.index().objects.contains_key(&second));
        assert_eq!(work.receipt().backend_write_operations, 1);
        assert_eq!(work.receipt().object_bytes_written, first_bytes.len() as u64);
        let token = CancellationToken::new();
        let retry = NativeMaterializationWork::new(native_write_budget(), &token).map_err(|(error, _)| error)?;
        let hold = store.flush_before_publish_with_native_work(
            PublicationScope::Closure { objects: &closure, proven_at: store.collection.sweeps() }, &retry, &token,
        ).await?;
        assert!(store.index().objects.is_empty());
        assert_eq!(retry.receipt().backend_write_operations, 1);
        assert_eq!(work.receipt().backend_write_operations, 1);
        drop(hold);
        Ok(())
    }
}
