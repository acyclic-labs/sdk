//! Durable local Objects provider over the canonical in-memory state machine.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use acyclic_native_runtime::OwnershipAnchor;
use async_trait::async_trait;
use fs2::FileExt;
use prost::Message;
use tokio::sync::{Mutex, mpsc};

use crate::{
    BufferedObject, Condition, DeleteResult, ExternalBody, GetRequest, LocalBodyLocation,
    LocalBodyReference, MemoryObjects, ObjectsError, ObjectsProvider, ProviderListPage, PutRequest,
    ReadTarget, wire,
};

const JOURNAL_MAGIC: &[u8; 23] = b"ACYCLIC-OBJECTS-LOCAL\0\x03";
const JOURNAL_HEADER_BYTES: u64 = 55;
const MAXIMUM_RECORD_BYTES: usize = 2 * 1_024 * 1_024;
const SEGMENT_MAGIC: &[u8; 24] = b"ACYCLIC-OBJECT-SEGMENT\0\x02";
const SEGMENT_HEADER_BYTES: usize = SEGMENT_MAGIC.len() + 4;
const SEGMENT_RECORD_BYTES: usize = 32 + 8;
const MAXIMUM_SEGMENT_BODIES: usize = 4;
const MAXIMUM_SEGMENT_BYTES: usize = 4 * 1024 * 1024;
const REPLAY_PIPELINE_RECORDS: usize = 32;
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// How journal frames and immutable bodies are made durable before they become observable.
///
/// A per-open policy, not part of the on-disk contract: a store written under one policy
/// reopens under any other.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalDurability {
    /// Flushes file contents and requests durable publication. Windows uses write-through
    /// moves; its API does not expose a documented equivalent of POSIX directory `fsync`.
    /// Power-loss durability of same-volume Windows moves is unproven; storage hardware
    /// and remote filesystems may also provide weaker guarantees.
    #[default]
    FullFlush,
    /// Uses Apple's `F_BARRIERFSYNC`. Opening on a target without that exact primitive fails
    /// with an I/O error instead of substituting different durability semantics.
    Barrier,
}

/// Exact durable-local capacity contract. Reopening requires the same capacity limits;
/// `durability` is a per-open policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalObjectsLimits {
    /// Largest body admitted by a single completed object version.
    pub maximum_object_bytes: u64,
    /// Largest aggregate logical body/part footprint admitted by the provider.
    pub maximum_bytes: u64,
    /// Maximum complete mutation frames replayed at startup.
    pub maximum_journal_operations: u64,
    /// Maximum journal bytes including its durable header and frame checksums.
    pub maximum_journal_bytes: u64,
    /// Synchronization policy for journal frames and bodies. Not recorded in the durable
    /// header.
    pub durability: LocalDurability,
}

impl Default for LocalObjectsLimits {
    fn default() -> Self {
        Self {
            maximum_object_bytes: 64 * 1_024 * 1_024,
            maximum_bytes: 4 * 1_024 * 1_024 * 1_024,
            maximum_journal_operations: 1_000_000,
            maximum_journal_bytes: 1_024 * 1_024 * 1_024,
            durability: LocalDurability::FullFlush,
        }
    }
}

/// Failure while opening or recovering a durable local provider.
#[derive(Debug, thiserror::Error)]
pub enum LocalObjectsError {
    /// Local configuration is invalid or differs from the durable store header.
    #[error("invalid local Objects configuration: {0}")]
    Invalid(&'static str),
    /// Durable bytes are corrupt or reference a missing authenticated body.
    #[error("corrupt local Objects store")]
    Corrupt,
    /// Another process owns this exact store root.
    #[error("local Objects store already has an owner")]
    AlreadyOwned,
    /// A journal write failed, so a reopen is required before further access.
    #[error("local Objects journal state is uncertain; reopen the store")]
    Unavailable,
    /// Host filesystem operation failed.
    #[error("local Objects I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

struct Persistence {
    root: PathBuf,
    journal: StdMutex<File>,
    journal_operations: AtomicU64,
    poisoned: AtomicBool,
    #[cfg(test)]
    fault_after_bytes: AtomicU64,
    #[cfg(test)]
    fault_sync_once: AtomicBool,
    #[cfg(test)]
    blocking_work: StdMutex<Option<BlockingWorkHook>>,
    limits: LocalObjectsLimits,
    _ownership: File,
    _ownership_anchor: Option<OwnershipAnchor>,
}

#[cfg(test)]
#[derive(Clone, Copy, Eq, PartialEq)]
enum BlockingWork {
    CollectGarbage,
    PersistSegment,
}

#[cfg(test)]
struct BlockingWorkHook {
    operation: BlockingWork,
    started: Option<tokio::sync::oneshot::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}

struct GarbageCollectionWork {
    _body_io: tokio::sync::OwnedRwLockWriteGuard<()>,
    _mutation: tokio::sync::OwnedMutexGuard<()>,
    live_bodies: BTreeSet<LocalBodyReference>,
    maximum_candidates: u64,
    // Keep persistence last so root ownership is published only after the operation guards drop.
    persistence: Arc<Persistence>,
}

impl GarbageCollectionWork {
    fn run(&self) -> Result<LocalObjectsGarbageCollection, LocalObjectsError> {
        self.persistence
            .collect_garbage(&self.live_bodies, self.maximum_candidates)
    }
}

impl Persistence {
    fn collect_garbage(
        &self,
        live_bodies: &BTreeSet<LocalBodyReference>,
        maximum_candidates: u64,
    ) -> Result<LocalObjectsGarbageCollection, LocalObjectsError> {
        #[cfg(test)]
        self.block_work(BlockingWork::CollectGarbage);
        collect_physical_garbage(
            &self.root,
            live_bodies,
            maximum_candidates,
            self.limits.maximum_object_bytes,
            self.limits.durability,
        )
    }

    fn persist_segment(
        &self,
        bodies: &[([u8; 32], bytes::Bytes)],
    ) -> Result<([u8; 32], Vec<u64>), LocalObjectsError> {
        #[cfg(test)]
        self.block_work(BlockingWork::PersistSegment);
        persist_segment(&self.root, bodies, self.limits.durability)
    }

    #[cfg(test)]
    fn block_work(&self, operation: BlockingWork) {
        let hook = self.blocking_work.lock().ok().and_then(|mut hook| {
            hook.as_ref()
                .is_some_and(|hook| hook.operation == operation)
                .then(|| hook.take())
                .flatten()
        });
        if let Some(mut hook) = hook {
            if let Some(started) = hook.started.take() {
                let _ = started.send(());
            }
            let _ = hook.release.recv();
        }
    }
}

/// Crash-safe, exclusive-owner local implementation of the public Objects contract.
///
/// Semantic decisions are delegated to [`MemoryObjects`], the public reference state machine.
/// This adapter owns only durable mutation replay and immutable authenticated body segments.
#[derive(Clone)]
pub struct LocalObjects {
    semantic: MemoryObjects,
    persistence: Arc<Persistence>,
    mutation: Arc<Mutex<()>>,
    body_io: Arc<tokio::sync::RwLock<()>>,
}

async fn run_owned_initialization<T, F>(initialize: F) -> Result<T, LocalObjectsError>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    tokio::spawn(initialize)
        .await
        .map_err(|_| LocalObjectsError::Unavailable)
}

/// Exact bounded physical-reclamation result for a durable local Objects root.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalObjectsGarbageCollection {
    /// Immutable body segments examined.
    pub segments_examined: u64,
    /// Segments unreachable from every retained body removed.
    pub segments_removed: u64,
    /// Crash-left temporary publication files removed.
    pub temporary_files_removed: u64,
}

#[derive(Clone, PartialEq, Message)]
struct PutRecord {
    #[prost(message, optional, tag = "1")]
    header: Option<wire::PutObjectHeader>,
    #[prost(bytes = "vec", tag = "2")]
    body_digest: Vec<u8>,
    #[prost(uint64, tag = "3")]
    body_length: u64,
    #[prost(bytes = "vec", tag = "4")]
    segment_id: Vec<u8>,
    #[prost(uint64, tag = "5")]
    segment_offset: u64,
}

#[derive(Clone, PartialEq, Message)]
struct PutBatchRecord {
    #[prost(message, repeated, tag = "1")]
    puts: Vec<PutRecord>,
}

#[derive(Clone, PartialEq, Message)]
struct UploadPartRecord {
    #[prost(message, optional, tag = "1")]
    header: Option<wire::UploadPartHeader>,
    #[prost(bytes = "vec", tag = "2")]
    body_digest: Vec<u8>,
    #[prost(uint64, tag = "3")]
    body_length: u64,
    #[prost(bytes = "vec", tag = "4")]
    segment_id: Vec<u8>,
    #[prost(uint64, tag = "5")]
    segment_offset: u64,
}

#[derive(Clone, PartialEq, Message)]
struct MutationRecord {
    #[prost(
        oneof = "mutation_record::Operation",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13"
    )]
    operation: Option<mutation_record::Operation>,
}

mod mutation_record {
    use super::{PutBatchRecord, PutRecord, UploadPartRecord, wire};

    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Operation {
        #[prost(message, tag = "1")]
        CreateBucket(wire::CreateBucketRequest),
        #[prost(message, tag = "2")]
        DeleteBucket(wire::DeleteBucketRequest),
        #[prost(message, tag = "3")]
        Put(PutRecord),
        #[prost(message, tag = "4")]
        Delete(wire::DeleteObjectRequest),
        #[prost(message, tag = "5")]
        Snapshot(wire::CreateSnapshotRequest),
        #[prost(message, tag = "6")]
        DestroySnapshot(wire::DestroySnapshotRequest),
        #[prost(message, tag = "7")]
        ForkSnapshot(wire::ForkSnapshotRequest),
        #[prost(message, tag = "8")]
        ForkBucket(wire::ForkBucketRequest),
        #[prost(message, tag = "9")]
        CreateMultipart(wire::CreateMultipartRequest),
        #[prost(message, tag = "10")]
        UploadPart(UploadPartRecord),
        #[prost(message, tag = "11")]
        CompleteMultipart(wire::CompleteMultipartRequest),
        #[prost(message, tag = "12")]
        AbortMultipart(wire::AbortMultipartRequest),
        #[prost(message, tag = "13")]
        PutBatch(PutBatchRecord),
    }
}

impl LocalObjects {
    /// Resolves a durable bucket by canonical name without appending a journal record.
    pub async fn bucket_named(&self, name: &str) -> Result<Option<wire::BucketRef>, ObjectsError> {
        self.check_available()?;
        self.semantic.bucket_named(name).await
    }

    /// Reclaims physical bodies not referenced by any live bucket, snapshot, or multipart upload.
    ///
    /// The provider's exclusive root ownership fences concurrent mutation. Every retained
    /// segment is authenticated before any unreachable file is removed.
    pub async fn collect_garbage(
        &self,
        maximum_candidates: u64,
    ) -> Result<LocalObjectsGarbageCollection, LocalObjectsError> {
        if maximum_candidates == 0 {
            return Err(LocalObjectsError::Invalid(
                "garbage-collection candidate bound must be positive",
            ));
        }
        let body_io = Arc::clone(&self.body_io).write_owned().await;
        let mutation = Arc::clone(&self.mutation).lock_owned().await;
        self.check_available()
            .map_err(|_| LocalObjectsError::Unavailable)?;
        let live_bodies = self.semantic.local_body_references().await;
        let work = GarbageCollectionWork {
            _body_io: body_io,
            _mutation: mutation,
            live_bodies,
            maximum_candidates,
            persistence: Arc::clone(&self.persistence),
        };
        tokio::task::spawn_blocking(move || work.run())
            .await
            .map_err(|_| LocalObjectsError::Corrupt)?
    }

    /// Opens or creates one exclusively owned durable provider and replays its valid prefix.
    ///
    /// A torn final journal frame is discarded. Corruption in a complete frame fails closed.
    /// Immutable bodies are segmented, authenticated, and range-read without whole-body loading.
    pub async fn open(
        root: impl AsRef<Path>,
        limits: LocalObjectsLimits,
    ) -> Result<Self, LocalObjectsError> {
        Self::open_with_optional_ownership_anchor(root, limits, None).await
    }

    /// Opens a provider while retaining an external ownership gate through every provider clone.
    #[doc(hidden)]
    pub async fn open_with_ownership_anchor(
        root: impl AsRef<Path>,
        limits: LocalObjectsLimits,
        ownership_anchor: OwnershipAnchor,
    ) -> Result<Self, LocalObjectsError> {
        Self::open_with_optional_ownership_anchor(root, limits, Some(ownership_anchor)).await
    }

    async fn open_with_optional_ownership_anchor(
        root: impl AsRef<Path>,
        limits: LocalObjectsLimits,
        ownership_anchor: Option<OwnershipAnchor>,
    ) -> Result<Self, LocalObjectsError> {
        let root = root.as_ref().to_path_buf();
        // Dropping a JoinHandle detaches its task. Keep the complete initialization sequence in
        // that independently owned task so caller cancellation cannot release owner.lock while a
        // blocking recovery or validation worker is still accessing the durable root.
        run_owned_initialization(
            async move { Self::open_owned(root, limits, ownership_anchor).await },
        )
        .await?
    }

    #[allow(clippy::too_many_lines)]
    async fn open_owned(
        root: PathBuf,
        limits: LocalObjectsLimits,
        ownership_anchor: Option<OwnershipAnchor>,
    ) -> Result<Self, LocalObjectsError> {
        if limits.maximum_object_bytes == 0
            || limits.maximum_bytes == 0
            || limits.maximum_object_bytes > limits.maximum_bytes
            || limits.maximum_journal_operations == 0
            || limits.maximum_journal_bytes < JOURNAL_HEADER_BYTES
        {
            return Err(LocalObjectsError::Invalid("invalid capacity limits"));
        }
        // Validate the in-memory replay target before acquiring durable ownership or starting a
        // blocking recovery worker. No fallible setup may abandon that worker while it owns the
        // journal.
        let semantic =
            MemoryObjects::new_with_limits(limits.maximum_object_bytes, limits.maximum_bytes)
                .map_err(|_| LocalObjectsError::Invalid("capacity limits are not representable"))?;
        fs::create_dir_all(root.join("segments"))?;
        sync_parent(&root, limits.durability)?;
        // A failed earlier write may leave a visible but unsynced prefix.
        // Settle its family name before a reopened writer can reuse it and
        // publish a new journal reference beneath it.
        sync_parent(&root.join("segments"), limits.durability)?;

        let ownership = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("owner.lock"))?;
        ownership
            .try_lock_exclusive()
            .map_err(|_| LocalObjectsError::AlreadyOwned)?;

        let journal_path = root.join("mutations.log");
        let (records, mut receiver) = mpsc::channel(REPLAY_PIPELINE_RECORDS);
        let recovery = tokio::task::spawn_blocking(move || {
            let mut journal = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(journal_path)?;
            initialize_or_validate_header(&mut journal, limits)?;
            let operations = decode_replay(&mut journal, limits, &records)?;
            Ok::<_, LocalObjectsError>((journal, operations))
        });
        let mut replay_error = None;
        while let Some(record) = receiver.recv().await {
            if replay_error.is_none()
                && let Err(error) = replay_operation(&root, &semantic, record).await
            {
                replay_error = Some(error);
            }
        }
        let (journal, journal_operations) = recovery
            .await
            .map_err(|_| LocalObjectsError::Unavailable)??;
        if let Some(error) = replay_error {
            return Err(error);
        }
        let bodies = semantic.local_body_references().await;
        let validation_root = root.clone();
        tokio::task::spawn_blocking(move || {
            validate_referenced_segments(&validation_root, &bodies, limits.maximum_object_bytes)
        })
        .await
        .map_err(|_| LocalObjectsError::Unavailable)??;
        Ok(Self {
            semantic,
            persistence: Arc::new(Persistence {
                root,
                journal: StdMutex::new(journal),
                journal_operations: AtomicU64::new(journal_operations),
                poisoned: AtomicBool::new(false),
                #[cfg(test)]
                fault_after_bytes: AtomicU64::new(u64::MAX),
                #[cfg(test)]
                fault_sync_once: AtomicBool::new(false),
                #[cfg(test)]
                blocking_work: StdMutex::new(None),
                limits,
                _ownership: ownership,
                _ownership_anchor: ownership_anchor,
            }),
            mutation: Arc::new(Mutex::new(())),
            body_io: Arc::new(tokio::sync::RwLock::new(())),
        })
    }

    fn append(&self, operation: mutation_record::Operation) -> Result<(), ObjectsError> {
        self.append_count(operation, 1)
    }

    fn append_count(
        &self,
        operation: mutation_record::Operation,
        operation_count: u64,
    ) -> Result<(), ObjectsError> {
        self.check_available()?;
        let payload = MutationRecord {
            operation: Some(operation),
        }
        .encode_to_vec();
        if payload.len() > MAXIMUM_RECORD_BYTES {
            return Err(ObjectsError::Invalid("local mutation record is too large"));
        }
        let mut journal = self.persistence.journal.lock().map_err(|_| {
            self.persistence.poisoned.store(true, Ordering::Release);
            ObjectsError::Unavailable
        })?;
        let frame_bytes = 36_u64
            .checked_add(payload.len() as u64)
            .ok_or(ObjectsError::Capacity)?;
        let operations = self.persistence.journal_operations.load(Ordering::Acquire);
        if operations
            .checked_add(operation_count)
            .is_none_or(|count| count > self.persistence.limits.maximum_journal_operations)
            || journal
                .metadata()
                .map_err(|_| ObjectsError::Unavailable)?
                .len()
                .checked_add(frame_bytes)
                .is_none_or(|bytes| bytes > self.persistence.limits.maximum_journal_bytes)
        {
            return Err(ObjectsError::Capacity);
        }
        #[cfg(test)]
        let result = match self
            .persistence
            .fault_after_bytes
            .swap(u64::MAX, Ordering::AcqRel)
        {
            u64::MAX
                if self
                    .persistence
                    .fault_sync_once
                    .swap(false, Ordering::AcqRel) =>
            {
                append_frame_with_sync(&mut journal, &payload, |_| {
                    Err(std::io::Error::other("injected journal sync failure"))
                })
            }
            u64::MAX => append_frame(&mut journal, &payload, self.persistence.limits.durability),
            offset => append_frame_with_fault(
                &mut journal,
                &payload,
                self.persistence.limits.durability,
                offset,
            ),
        };
        #[cfg(not(test))]
        let result = append_frame(&mut journal, &payload, self.persistence.limits.durability);
        if result.is_err() {
            // The frame may be partial, or complete without a confirmed sync.
            // No later mutation may be acknowledged behind that uncertain tail.
            self.persistence.poisoned.store(true, Ordering::Release);
            return Err(ObjectsError::Unavailable);
        }
        self.persistence
            .journal_operations
            .fetch_add(operation_count, Ordering::Release);
        Ok(())
    }

    async fn persist_segment(
        &self,
        bodies: Vec<([u8; 32], bytes::Bytes)>,
    ) -> Result<([u8; 32], Vec<u64>), ObjectsError> {
        let persistence = Arc::clone(&self.persistence);
        let result = tokio::task::spawn_blocking(move || persistence.persist_segment(&bodies))
            .await
            .map_err(|_| LocalObjectsError::Corrupt)
            .and_then(std::convert::identity);
        if result.is_err() {
            self.persistence.poisoned.store(true, Ordering::Release);
        }
        result.map_err(|_| ObjectsError::Unavailable)
    }

    fn check_available(&self) -> Result<(), ObjectsError> {
        if self.persistence.poisoned.load(Ordering::Acquire)
            || self.persistence.journal.is_poisoned()
        {
            self.persistence.poisoned.store(true, Ordering::Release);
            Err(ObjectsError::Unavailable)
        } else {
            Ok(())
        }
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl ObjectsProvider for LocalObjects {
    async fn create_bucket(
        &self,
        name: String,
        idempotency_key: Option<String>,
    ) -> Result<wire::Bucket, ObjectsError> {
        let _mutation = self.mutation.lock().await;
        self.append(mutation_record::Operation::CreateBucket(
            wire::CreateBucketRequest {
                name: name.clone(),
                mutation: mutation(idempotency_key.clone()),
            },
        ))?;
        self.semantic.create_bucket(name, idempotency_key).await
    }

    async fn head_bucket(&self, bucket: &wire::BucketRef) -> Result<wire::Bucket, ObjectsError> {
        self.check_available()?;
        self.semantic.head_bucket(bucket).await
    }

    async fn delete_bucket(
        &self,
        bucket: &wire::BucketRef,
        idempotency_key: Option<String>,
    ) -> Result<bool, ObjectsError> {
        let _mutation = self.mutation.lock().await;
        self.append(mutation_record::Operation::DeleteBucket(
            wire::DeleteBucketRequest {
                bucket: Some(bucket.clone()),
                mutation: mutation(idempotency_key.clone()),
            },
        ))?;
        self.semantic.delete_bucket(bucket, idempotency_key).await
    }

    async fn put(&self, request: PutRequest) -> Result<wire::ObjectVersion, ObjectsError> {
        let _body_io = self.body_io.read().await;
        self.check_available()?;
        let body_length = request.body.len();
        let digest = *blake3::hash(&request.body).as_bytes();
        // A recorded failure must not publish a new immutable body during a
        // retry. The mutation lock protects the idempotency decision.
        {
            let _mutation = self.mutation.lock().await;
            if let Some(outcome) = self.semantic.recorded_put(&request, &digest).await
                && outcome.is_err()
            {
                return outcome;
            }
        }
        // Immutable body publication can overlap other puts. The mutation lock
        // still orders every journal intent and semantic state transition.
        let bodies = vec![(digest, request.body.clone())];
        let (segment_id, offsets) = self.persist_segment(bodies).await?;
        let segment_offset = *offsets.first().ok_or(ObjectsError::Unavailable)?;
        let _mutation = self.mutation.lock().await;
        self.check_available()?;
        // A recorded retry has already been journaled. Successful retries
        // still authenticate and repair the immutable body before returning.
        if let Some(outcome) = self.semantic.recorded_put(&request, &digest).await {
            return outcome;
        }
        self.append(mutation_record::Operation::Put(PutRecord {
            header: Some(wire::PutObjectHeader {
                bucket: Some(request.bucket.clone()),
                object_key: request.object_key.clone(),
                metadata: Some(request.metadata.clone()),
                preconditions: request.condition.clone().map(Condition::wire),
                mutation: mutation(request.idempotency_key.clone()),
            }),
            body_digest: digest.to_vec(),
            body_length: request.body.len() as u64,
            segment_id: segment_id.to_vec(),
            segment_offset,
        }))?;
        self.semantic
            .put_external(
                request,
                ExternalBody {
                    root: self.persistence.root.clone(),
                    digest,
                    length: body_length,
                    location: LocalBodyLocation::Segment {
                        id: segment_id,
                        offset: segment_offset,
                    },
                },
            )
            .await
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "every stored index comes from enumerate() over the same request length used to allocate outcomes"
    )]
    async fn put_batch(
        &self,
        requests: Vec<PutRequest>,
    ) -> Vec<Result<wire::ObjectVersion, ObjectsError>> {
        if requests.is_empty() {
            return Vec::new();
        }
        if !self.segment_batch_is_bounded(&requests) {
            return self.put_batch_partitioned(requests).await;
        }
        let _body_io = self.body_io.read().await;
        if let Err(error) = self.check_available() {
            return vec![Err(error); requests.len()];
        }
        let _mutation = self.mutation.lock().await;
        let mut outcomes = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
        let mut pending = Vec::new();
        for (index, request) in requests.into_iter().enumerate() {
            let digest = *blake3::hash(&request.body).as_bytes();
            if let Some(outcome) = self.semantic.recorded_put(&request, &digest).await {
                outcomes[index] = Some(outcome);
            } else {
                pending.push((index, request, digest));
            }
        }
        if pending.is_empty() {
            return outcomes
                .into_iter()
                .map(|outcome| outcome.unwrap_or(Err(ObjectsError::Unavailable)))
                .collect();
        }
        let mut unique_bodies = Vec::new();
        let mut body_indices = BTreeMap::new();
        let mut pending_body_indices = Vec::with_capacity(pending.len());
        for (_, request, digest) in &pending {
            let body_index = if let Some(index) = body_indices.get(digest) {
                if unique_bodies
                    .get(*index)
                    .is_none_or(|(_, body): &([u8; 32], bytes::Bytes)| body != &request.body)
                {
                    for (index, _, _) in pending {
                        outcomes[index] = Some(Err(ObjectsError::Unavailable));
                    }
                    return outcomes
                        .into_iter()
                        .map(|outcome| outcome.unwrap_or(Err(ObjectsError::Unavailable)))
                        .collect();
                }
                *index
            } else {
                let index = unique_bodies.len();
                unique_bodies.push((*digest, request.body.clone()));
                body_indices.insert(*digest, index);
                index
            };
            pending_body_indices.push(body_index);
        }
        let (segment_id, unique_offsets) = match self.persist_segment(unique_bodies).await {
            Ok(segment) => segment,
            Err(error) => {
                for (index, _, _) in pending {
                    outcomes[index] = Some(Err(error.clone()));
                }
                return outcomes
                    .into_iter()
                    .map(|outcome| outcome.unwrap_or(Err(ObjectsError::Unavailable)))
                    .collect();
            }
        };
        let records = pending
            .iter()
            .zip(pending_body_indices.iter())
            .map(|((_, request, digest), body_index)| PutRecord {
                header: Some(wire::PutObjectHeader {
                    bucket: Some(request.bucket.clone()),
                    object_key: request.object_key.clone(),
                    metadata: Some(request.metadata.clone()),
                    preconditions: request.condition.clone().map(Condition::wire),
                    mutation: mutation(request.idempotency_key.clone()),
                }),
                body_digest: digest.to_vec(),
                body_length: u64::try_from(request.body.len()).unwrap_or(u64::MAX),
                segment_id: segment_id.to_vec(),
                segment_offset: unique_offsets[*body_index],
            })
            .collect::<Vec<_>>();
        let operation_count = u64::try_from(records.len()).unwrap_or(u64::MAX);
        if let Err(error) = self.append_count(
            mutation_record::Operation::PutBatch(PutBatchRecord { puts: records }),
            operation_count,
        ) {
            for (index, _, _) in pending {
                outcomes[index] = Some(Err(error.clone()));
            }
            return outcomes
                .into_iter()
                .map(|outcome| outcome.unwrap_or(Err(ObjectsError::Unavailable)))
                .collect();
        }
        for ((index, request, digest), body_index) in pending.into_iter().zip(pending_body_indices)
        {
            let length = request.body.len();
            outcomes[index] = Some(
                self.semantic
                    .put_external(
                        request,
                        ExternalBody {
                            root: self.persistence.root.clone(),
                            digest,
                            length,
                            location: LocalBodyLocation::Segment {
                                id: segment_id,
                                offset: unique_offsets[body_index],
                            },
                        },
                    )
                    .await,
            );
        }
        outcomes
            .into_iter()
            .map(|outcome| outcome.unwrap_or(Err(ObjectsError::Unavailable)))
            .collect()
    }

    async fn get(&self, request: GetRequest) -> Result<BufferedObject, ObjectsError> {
        self.check_available()?;
        let _body_io = self.body_io.read().await;
        self.semantic.get(request).await
    }

    async fn get_batch(
        &self,
        requests: Vec<GetRequest>,
    ) -> Vec<Result<BufferedObject, ObjectsError>> {
        if let Err(error) = self.check_available() {
            return vec![Err(error); requests.len()];
        }
        let _body_io = self.body_io.read().await;
        self.semantic.get_batch(requests).await
    }

    async fn delete(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        version_id: Option<String>,
        condition: Option<Condition>,
        idempotency_key: Option<String>,
    ) -> Result<DeleteResult, ObjectsError> {
        let _mutation = self.mutation.lock().await;
        self.append(mutation_record::Operation::Delete(
            wire::DeleteObjectRequest {
                bucket: Some(bucket.clone()),
                object_key: object_key.clone(),
                version_id: version_id.clone().unwrap_or_default(),
                preconditions: condition.clone().map(Condition::wire),
                mutation: mutation(idempotency_key.clone()),
            },
        ))?;
        self.semantic
            .delete(bucket, object_key, version_id, condition, idempotency_key)
            .await
    }

    async fn list(
        &self,
        target: ReadTarget,
        prefix: String,
        delimiter: Option<String>,
        versions: bool,
        page_size: u32,
        continuation: Option<String>,
    ) -> Result<ProviderListPage, ObjectsError> {
        self.check_available()?;
        self.semantic
            .list(target, prefix, delimiter, versions, page_size, continuation)
            .await
    }

    async fn snapshot(
        &self,
        bucket: wire::BucketRef,
        idempotency_key: Option<String>,
    ) -> Result<wire::Snapshot, ObjectsError> {
        let _mutation = self.mutation.lock().await;
        self.append(mutation_record::Operation::Snapshot(
            wire::CreateSnapshotRequest {
                bucket: Some(bucket.clone()),
                mutation: mutation(idempotency_key.clone()),
            },
        ))?;
        self.semantic.snapshot(bucket, idempotency_key).await
    }

    async fn destroy_snapshot(
        &self,
        snapshot: wire::SnapshotRef,
        idempotency_key: Option<String>,
    ) -> Result<bool, ObjectsError> {
        let _mutation = self.mutation.lock().await;
        self.append(mutation_record::Operation::DestroySnapshot(
            wire::DestroySnapshotRequest {
                snapshot: Some(snapshot.clone()),
                mutation: mutation(idempotency_key.clone()),
            },
        ))?;
        self.semantic
            .destroy_snapshot(snapshot, idempotency_key)
            .await
    }

    async fn fork(
        &self,
        source: ReadTarget,
        destination_name: String,
        idempotency_key: Option<String>,
    ) -> Result<wire::Bucket, ObjectsError> {
        let _mutation = self.mutation.lock().await;
        let operation = match &source {
            ReadTarget::Bucket(source) => {
                mutation_record::Operation::ForkBucket(wire::ForkBucketRequest {
                    source: Some(source.clone()),
                    destination_name: destination_name.clone(),
                    mutation: mutation(idempotency_key.clone()),
                })
            }
            ReadTarget::Snapshot(snapshot) => {
                mutation_record::Operation::ForkSnapshot(wire::ForkSnapshotRequest {
                    snapshot: Some(snapshot.clone()),
                    destination_name: destination_name.clone(),
                    mutation: mutation(idempotency_key.clone()),
                })
            }
        };
        self.append(operation)?;
        self.semantic
            .fork(source, destination_name, idempotency_key)
            .await
    }

    async fn create_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        metadata: wire::ObjectMetadata,
        condition: Option<Condition>,
        idempotency_key: Option<String>,
    ) -> Result<wire::MultipartUpload, ObjectsError> {
        let _mutation = self.mutation.lock().await;
        self.append(mutation_record::Operation::CreateMultipart(
            wire::CreateMultipartRequest {
                bucket: Some(bucket.clone()),
                object_key: object_key.clone(),
                metadata: Some(metadata.clone()),
                preconditions: condition.clone().map(Condition::wire),
                mutation: mutation(idempotency_key.clone()),
            },
        ))?;
        self.semantic
            .create_multipart(bucket, object_key, metadata, condition, idempotency_key)
            .await
    }

    async fn upload_part(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        part_number: u32,
        body: bytes::Bytes,
        idempotency_key: Option<String>,
    ) -> Result<wire::UploadedPart, ObjectsError> {
        let _body_io = self.body_io.read().await;
        let _mutation = self.mutation.lock().await;
        self.check_available()?;
        let body_length = body.len();
        let digest = *blake3::hash(&body).as_bytes();
        let bodies = vec![(digest, body.clone())];
        let (segment_id, offsets) = self.persist_segment(bodies).await?;
        let segment_offset = *offsets.first().ok_or(ObjectsError::Unavailable)?;
        self.append(mutation_record::Operation::UploadPart(UploadPartRecord {
            header: Some(wire::UploadPartHeader {
                bucket: Some(bucket.clone()),
                object_key: object_key.clone(),
                upload_id: upload_id.clone(),
                part_number,
                mutation: mutation(idempotency_key.clone()),
            }),
            body_digest: digest.to_vec(),
            body_length: body.len() as u64,
            segment_id: segment_id.to_vec(),
            segment_offset,
        }))?;
        self.semantic
            .upload_part_external(
                bucket,
                object_key,
                upload_id,
                part_number,
                ExternalBody {
                    root: self.persistence.root.clone(),
                    digest,
                    length: body_length,
                    location: LocalBodyLocation::Segment {
                        id: segment_id,
                        offset: segment_offset,
                    },
                },
                idempotency_key,
            )
            .await
    }

    async fn list_parts(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
    ) -> Result<Vec<wire::UploadedPart>, ObjectsError> {
        self.check_available()?;
        self.semantic
            .list_parts(bucket, object_key, upload_id)
            .await
    }

    async fn complete_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        parts: Vec<wire::UploadedPart>,
        idempotency_key: Option<String>,
    ) -> Result<wire::ObjectVersion, ObjectsError> {
        let _mutation = self.mutation.lock().await;
        self.append(mutation_record::Operation::CompleteMultipart(
            wire::CompleteMultipartRequest {
                bucket: Some(bucket.clone()),
                object_key: object_key.clone(),
                upload_id: upload_id.clone(),
                parts: parts.clone(),
                mutation: mutation(idempotency_key.clone()),
            },
        ))?;
        self.semantic
            .complete_multipart(bucket, object_key, upload_id, parts, idempotency_key)
            .await
    }

    async fn abort_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        idempotency_key: Option<String>,
    ) -> Result<bool, ObjectsError> {
        let _mutation = self.mutation.lock().await;
        self.append(mutation_record::Operation::AbortMultipart(
            wire::AbortMultipartRequest {
                bucket: Some(bucket.clone()),
                object_key: object_key.clone(),
                upload_id: upload_id.clone(),
                mutation: mutation(idempotency_key.clone()),
            },
        ))?;
        self.semantic
            .abort_multipart(bucket, object_key, upload_id, idempotency_key)
            .await
    }
}

impl LocalObjects {
    fn segment_batch_is_bounded(&self, requests: &[PutRequest]) -> bool {
        requests.len() <= MAXIMUM_SEGMENT_BODIES
            && requests.iter().all(|request| {
                u64::try_from(request.body.len()).unwrap_or(u64::MAX)
                    <= self.persistence.limits.maximum_object_bytes
            })
            && requests
                .iter()
                .map(|request| request.body.len())
                .try_fold(0_usize, usize::checked_add)
                .is_some_and(|bytes| bytes <= MAXIMUM_SEGMENT_BYTES)
    }

    async fn put_batch_partitioned(
        &self,
        requests: Vec<PutRequest>,
    ) -> Vec<Result<wire::ObjectVersion, ObjectsError>> {
        let mut results = Vec::with_capacity(requests.len());
        let mut batch = Vec::with_capacity(MAXIMUM_SEGMENT_BODIES);
        let mut batch_bytes = 0_usize;
        for request in requests {
            let body_bytes = request.body.len();
            if u64::try_from(body_bytes).unwrap_or(u64::MAX)
                > self.persistence.limits.maximum_object_bytes
                || body_bytes > MAXIMUM_SEGMENT_BYTES
            {
                if !batch.is_empty() {
                    results.extend(self.put_batch(std::mem::take(&mut batch)).await);
                    batch_bytes = 0;
                }
                results.push(self.put(request).await);
                continue;
            }
            let would_exceed_bytes = !batch.is_empty()
                && batch_bytes
                    .checked_add(body_bytes)
                    .is_none_or(|bytes| bytes > MAXIMUM_SEGMENT_BYTES);
            if batch.len() == MAXIMUM_SEGMENT_BODIES || would_exceed_bytes {
                results.extend(self.put_batch(std::mem::take(&mut batch)).await);
                batch_bytes = 0;
            }
            batch_bytes = batch_bytes.saturating_add(body_bytes);
            batch.push(request);
        }
        if !batch.is_empty() {
            results.extend(self.put_batch(batch).await);
        }
        results
    }
}

fn mutation(idempotency_key: Option<String>) -> Option<wire::MutationIdentity> {
    idempotency_key.map(|idempotency_key| wire::MutationIdentity { idempotency_key })
}

fn idempotency(mutation: Option<wire::MutationIdentity>) -> Option<String> {
    mutation.map(|value| value.idempotency_key)
}

fn condition(value: Option<wire::Preconditions>) -> Result<Option<Condition>, LocalObjectsError> {
    value
        .map(
            |value| match value.condition.ok_or(LocalObjectsError::Corrupt)? {
                wire::preconditions::Condition::IfAbsent(true) => Ok(Condition::IfAbsent),
                wire::preconditions::Condition::IfAbsent(false) => Err(LocalObjectsError::Corrupt),
                wire::preconditions::Condition::IfMatch(value) => Ok(Condition::IfMatch(value)),
                wire::preconditions::Condition::IfVersion(value) => Ok(Condition::IfVersion(value)),
            },
        )
        .transpose()
}

fn required<T>(value: Option<T>) -> Result<T, LocalObjectsError> {
    value.ok_or(LocalObjectsError::Corrupt)
}

fn decode_replay(
    journal: &mut File,
    limits: LocalObjectsLimits,
    records: &mpsc::Sender<mutation_record::Operation>,
) -> Result<u64, LocalObjectsError> {
    journal.seek(SeekFrom::Start(JOURNAL_HEADER_BYTES))?;
    let mut operations = 0_u64;
    loop {
        let frame_start = journal.stream_position()?;
        let Some(payload) = read_frame(journal, frame_start, limits.durability)? else {
            break;
        };
        let record =
            MutationRecord::decode(payload.as_slice()).map_err(|_| LocalObjectsError::Corrupt)?;
        let operation_count = match record.operation.as_ref() {
            Some(mutation_record::Operation::PutBatch(batch)) => {
                if !put_batch_record_is_bounded(batch) {
                    return Err(LocalObjectsError::Corrupt);
                }
                u64::try_from(batch.puts.len()).map_err(|_| LocalObjectsError::Corrupt)?
            }
            Some(_) => 1,
            None => return Err(LocalObjectsError::Corrupt),
        };
        if operation_count == 0 {
            return Err(LocalObjectsError::Corrupt);
        }
        operations = operations
            .checked_add(operation_count)
            .ok_or(LocalObjectsError::Corrupt)?;
        if operations > limits.maximum_journal_operations
            || journal.stream_position()? > limits.maximum_journal_bytes
        {
            return Err(LocalObjectsError::Corrupt);
        }
        records
            .blocking_send(required(record.operation)?)
            .map_err(|_| LocalObjectsError::Unavailable)?;
    }
    journal.seek(SeekFrom::End(0))?;
    Ok(operations)
}

#[allow(clippy::too_many_lines)]
async fn replay_operation(
    root: &Path,
    semantic: &MemoryObjects,
    operation: mutation_record::Operation,
) -> Result<(), LocalObjectsError> {
    match operation {
        mutation_record::Operation::CreateBucket(request) => {
            replay_effect(
                &semantic
                    .create_bucket(request.name, idempotency(request.mutation))
                    .await,
            )?;
        }
        mutation_record::Operation::DeleteBucket(request) => {
            replay_effect(
                &semantic
                    .delete_bucket(&required(request.bucket)?, idempotency(request.mutation))
                    .await,
            )?;
        }
        mutation_record::Operation::Put(record) => {
            replay_put(root, semantic, record).await?;
        }
        mutation_record::Operation::Delete(request) => {
            replay_effect(
                &semantic
                    .delete(
                        required(request.bucket)?,
                        request.object_key,
                        (!request.version_id.is_empty()).then_some(request.version_id),
                        condition(request.preconditions)?,
                        idempotency(request.mutation),
                    )
                    .await,
            )?;
        }
        mutation_record::Operation::Snapshot(request) => {
            replay_effect(
                &semantic
                    .snapshot(required(request.bucket)?, idempotency(request.mutation))
                    .await,
            )?;
        }
        mutation_record::Operation::DestroySnapshot(request) => {
            replay_effect(
                &semantic
                    .destroy_snapshot(required(request.snapshot)?, idempotency(request.mutation))
                    .await,
            )?;
        }
        mutation_record::Operation::ForkSnapshot(request) => {
            replay_effect(
                &semantic
                    .fork(
                        ReadTarget::Snapshot(required(request.snapshot)?),
                        request.destination_name,
                        idempotency(request.mutation),
                    )
                    .await,
            )?;
        }
        mutation_record::Operation::ForkBucket(request) => {
            replay_effect(
                &semantic
                    .fork(
                        ReadTarget::Bucket(required(request.source)?),
                        request.destination_name,
                        idempotency(request.mutation),
                    )
                    .await,
            )?;
        }
        mutation_record::Operation::CreateMultipart(request) => {
            replay_effect(
                &semantic
                    .create_multipart(
                        required(request.bucket)?,
                        request.object_key,
                        required(request.metadata)?,
                        condition(request.preconditions)?,
                        idempotency(request.mutation),
                    )
                    .await,
            )?;
        }
        mutation_record::Operation::UploadPart(record) => {
            let header = required(record.header)?;
            let digest = parse_digest(&record.body_digest)?;
            let segment_id = parse_digest(&record.segment_id)?;
            let length =
                usize::try_from(record.body_length).map_err(|_| LocalObjectsError::Corrupt)?;
            replay_effect(
                &semantic
                    .upload_part_external(
                        required(header.bucket)?,
                        header.object_key,
                        header.upload_id,
                        header.part_number,
                        ExternalBody {
                            root: root.to_path_buf(),
                            digest,
                            length,
                            location: LocalBodyLocation::Segment {
                                id: segment_id,
                                offset: record.segment_offset,
                            },
                        },
                        idempotency(header.mutation),
                    )
                    .await,
            )?;
        }
        mutation_record::Operation::CompleteMultipart(request) => {
            replay_effect(
                &semantic
                    .complete_multipart(
                        required(request.bucket)?,
                        request.object_key,
                        request.upload_id,
                        request.parts,
                        idempotency(request.mutation),
                    )
                    .await,
            )?;
        }
        mutation_record::Operation::AbortMultipart(request) => {
            replay_effect(
                &semantic
                    .abort_multipart(
                        required(request.bucket)?,
                        request.object_key,
                        request.upload_id,
                        idempotency(request.mutation),
                    )
                    .await,
            )?;
        }
        mutation_record::Operation::PutBatch(batch) => {
            if !put_batch_record_is_bounded(&batch) {
                return Err(LocalObjectsError::Corrupt);
            }
            for record in batch.puts {
                replay_put(root, semantic, record).await?;
            }
        }
    }
    Ok(())
}

fn put_batch_record_is_bounded(batch: &PutBatchRecord) -> bool {
    !batch.puts.is_empty()
        && batch.puts.len() <= MAXIMUM_SEGMENT_BODIES
        && batch
            .puts
            .iter()
            .try_fold(0_u64, |total, record| total.checked_add(record.body_length))
            .is_some_and(|bytes| bytes <= MAXIMUM_SEGMENT_BYTES as u64)
}

async fn replay_put(
    root: &Path,
    semantic: &MemoryObjects,
    record: PutRecord,
) -> Result<(), LocalObjectsError> {
    let header = required(record.header)?;
    let digest = parse_digest(&record.body_digest)?;
    let length = usize::try_from(record.body_length).map_err(|_| LocalObjectsError::Corrupt)?;
    let location = LocalBodyLocation::Segment {
        id: parse_digest(&record.segment_id)?,
        offset: record.segment_offset,
    };
    replay_effect(
        &semantic
            .put_external(
                PutRequest {
                    bucket: required(header.bucket)?,
                    object_key: header.object_key,
                    body: bytes::Bytes::new(),
                    metadata: required(header.metadata)?,
                    condition: condition(header.preconditions)?,
                    idempotency_key: idempotency(header.mutation),
                },
                ExternalBody {
                    root: root.to_path_buf(),
                    digest,
                    length,
                    location,
                },
            )
            .await,
    )
}

fn replay_effect<T>(result: &Result<T, ObjectsError>) -> Result<(), LocalObjectsError> {
    match result {
        Ok(_)
        | Err(
            ObjectsError::Invalid(_)
            | ObjectsError::NotFound
            | ObjectsError::AlreadyExists
            | ObjectsError::PreconditionFailed
            | ObjectsError::IdempotencyMismatch
            | ObjectsError::Capacity,
        ) => Ok(()),
        // Rejected intents are journalled, but the in-memory reference model
        // cannot legitimately fail from transport or storage unavailability.
        Err(ObjectsError::Unsupported | ObjectsError::Unauthorized | ObjectsError::Unavailable) => {
            Err(LocalObjectsError::Corrupt)
        }
    }
}

fn initialize_or_validate_header(
    journal: &mut File,
    limits: LocalObjectsLimits,
) -> Result<(), LocalObjectsError> {
    let length = journal.metadata()?.len();
    if length == 0 {
        let capacity = usize::try_from(JOURNAL_HEADER_BYTES)
            .map_err(|_| LocalObjectsError::Invalid("journal header is too large"))?;
        let mut header = Vec::with_capacity(capacity);
        header.extend_from_slice(JOURNAL_MAGIC);
        header.extend_from_slice(&limits.maximum_object_bytes.to_le_bytes());
        header.extend_from_slice(&limits.maximum_bytes.to_le_bytes());
        header.extend_from_slice(&limits.maximum_journal_operations.to_le_bytes());
        header.extend_from_slice(&limits.maximum_journal_bytes.to_le_bytes());
        journal.write_all(&header)?;
        sync_file(journal, limits.durability)?;
        return Ok(());
    }
    if length < JOURNAL_HEADER_BYTES {
        return Err(LocalObjectsError::Corrupt);
    }
    journal.seek(SeekFrom::Start(0))?;
    let mut magic = [0; JOURNAL_MAGIC.len()];
    journal.read_exact(&mut magic)?;
    let mut maximum_object_bytes = [0; 8];
    let mut maximum_bytes = [0; 8];
    let mut maximum_journal_operations = [0; 8];
    let mut maximum_journal_bytes = [0; 8];
    journal.read_exact(&mut maximum_object_bytes)?;
    journal.read_exact(&mut maximum_bytes)?;
    journal.read_exact(&mut maximum_journal_operations)?;
    journal.read_exact(&mut maximum_journal_bytes)?;
    if &magic != JOURNAL_MAGIC
        || u64::from_le_bytes(maximum_object_bytes) != limits.maximum_object_bytes
        || u64::from_le_bytes(maximum_bytes) != limits.maximum_bytes
        || u64::from_le_bytes(maximum_journal_operations) != limits.maximum_journal_operations
        || u64::from_le_bytes(maximum_journal_bytes) != limits.maximum_journal_bytes
    {
        return Err(LocalObjectsError::Invalid(
            "durable header or capacity limits differ",
        ));
    }
    Ok(())
}

fn append_frame(
    journal: &mut File,
    payload: &[u8],
    durability: LocalDurability,
) -> std::io::Result<()> {
    append_frame_with_sync(journal, payload, |file| sync_file_data(file, durability))
}

fn append_frame_with_sync(
    journal: &mut File,
    payload: &[u8],
    sync: impl FnOnce(&File) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let frame = encode_frame(payload)?;
    journal.write_all(&frame)?;
    sync(journal)
}

fn encode_frame(payload: &[u8]) -> std::io::Result<Vec<u8>> {
    let length = u32::try_from(payload.len())
        .map_err(|_| std::io::Error::other("local mutation record is too large"))?;
    let capacity = 36_usize
        .checked_add(payload.len())
        .ok_or_else(|| std::io::Error::other("local mutation record is too large"))?;
    let mut frame = Vec::with_capacity(capacity);
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(blake3::hash(payload).as_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

#[cfg(test)]
fn append_frame_with_fault(
    journal: &mut File,
    payload: &[u8],
    durability: LocalDurability,
    fault_after_bytes: u64,
) -> std::io::Result<()> {
    struct FaultingWriter<'a> {
        file: &'a mut File,
        remaining: u64,
    }

    impl Write for FaultingWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Err(std::io::Error::other("injected journal write failure"));
            }
            // Repeated short writes exercise write_all before the injected failure.
            let allowed = usize::try_from(self.remaining)
                .unwrap_or(usize::MAX)
                .min(bytes.len())
                .min(3);
            let written = self.file.write(bytes.get(..allowed).unwrap_or_default())?;
            self.remaining -= written as u64;
            Ok(written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.file.flush()
        }
    }

    let mut writer = FaultingWriter {
        file: journal,
        remaining: fault_after_bytes,
    };
    let frame = encode_frame(payload)?;
    writer.write_all(&frame)?;
    if writer.remaining == 0 {
        return Err(std::io::Error::other("injected post-write journal failure"));
    }
    sync_file_data(writer.file, durability)
}

fn read_frame(
    journal: &mut File,
    frame_start: u64,
    durability: LocalDurability,
) -> Result<Option<Vec<u8>>, LocalObjectsError> {
    let mut length = [0; 4];
    if journal.read(&mut length[..1])? == 0 {
        return Ok(None);
    }
    if !read_exact_or_torn(journal, &mut length[1..])? {
        journal.set_len(frame_start)?;
        sync_file_data(journal, durability)?;
        return Ok(None);
    }
    let length = u32::from_le_bytes(length) as usize;
    if length == 0 || length > MAXIMUM_RECORD_BYTES {
        return Err(LocalObjectsError::Corrupt);
    }
    let mut checksum = [0; 32];
    let mut payload = vec![0; length];
    for part in [&mut checksum[..], &mut payload[..]] {
        if !read_exact_or_torn(journal, part)? {
            journal.set_len(frame_start)?;
            sync_file_data(journal, durability)?;
            return Ok(None);
        }
    }
    if blake3::hash(&payload).as_bytes() != &checksum {
        return Err(LocalObjectsError::Corrupt);
    }
    Ok(Some(payload))
}

fn read_exact_or_torn(reader: &mut impl Read, bytes: &mut [u8]) -> std::io::Result<bool> {
    match reader.read_exact(bytes) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error),
    }
}

#[allow(clippy::too_many_lines)]
fn persist_segment(
    root: &Path,
    bodies: &[([u8; 32], bytes::Bytes)],
    durability: LocalDurability,
) -> Result<([u8; 32], Vec<u64>), LocalObjectsError> {
    if bodies.is_empty() {
        return Err(LocalObjectsError::Invalid("object segment is empty"));
    }
    if bodies.len() > MAXIMUM_SEGMENT_BODIES {
        return Err(LocalObjectsError::Invalid(
            "object segment has too many bodies",
        ));
    }
    if bodies.len() > 1
        && bodies
            .iter()
            .map(|(_, body)| body.len())
            .try_fold(0_usize, usize::checked_add)
            .is_none_or(|bytes| bytes > MAXIMUM_SEGMENT_BYTES)
    {
        return Err(LocalObjectsError::Invalid(
            "multi-body object segment is too large",
        ));
    }
    let count = u32::try_from(bodies.len())
        .map_err(|_| LocalObjectsError::Invalid("object segment has too many bodies"))?;
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(bodies.len())
        .map_err(|_| LocalObjectsError::Invalid("object segment offset allocation failed"))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(SEGMENT_MAGIC);
    hasher.update(&count.to_le_bytes());
    let table_bytes =
        bodies
            .len()
            .checked_mul(SEGMENT_RECORD_BYTES)
            .ok_or(LocalObjectsError::Invalid(
                "object segment length overflowed",
            ))?;
    let mut position = u64::try_from(SEGMENT_HEADER_BYTES.checked_add(table_bytes).ok_or(
        LocalObjectsError::Invalid("object segment length overflowed"),
    )?)
    .map_err(|_| LocalObjectsError::Invalid("object segment length overflowed"))?;
    for (digest, body) in bodies {
        let length = u64::try_from(body.len())
            .map_err(|_| LocalObjectsError::Invalid("object segment body is too large"))?;
        hasher.update(digest);
        hasher.update(&length.to_le_bytes());
        offsets.push(position);
        position = position
            .checked_add(length)
            .ok_or(LocalObjectsError::Invalid(
                "object segment length overflowed",
            ))?;
    }
    for (_, body) in bodies {
        hasher.update(body);
    }
    let id = *hasher.finalize().as_bytes();
    let parent = root.join("segments");
    let destination = segment_path(root, &id);
    if destination.exists() {
        validate_segment_file(&destination, &id, position)?;
        return Ok((id, offsets));
    }
    let identity = hex(&id);
    let (temporary, mut file) = (0..1_024)
        .find_map(|_| {
            let nonce = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(".{identity}.{}-{nonce}.tmp", std::process::id()));
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(file) => Some(Ok((path, file))),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => None,
                Err(error) => Some(Err(error)),
            }
        })
        .transpose()?
        .ok_or(LocalObjectsError::Corrupt)?;
    let write_result = (|| -> std::io::Result<()> {
        let metadata_capacity = SEGMENT_HEADER_BYTES
            .checked_add(table_bytes)
            .ok_or_else(|| std::io::Error::other("object segment metadata is too large"))?;
        let mut metadata = Vec::with_capacity(metadata_capacity);
        metadata.extend_from_slice(SEGMENT_MAGIC);
        metadata.extend_from_slice(&count.to_le_bytes());
        for (digest, body) in bodies {
            metadata.extend_from_slice(digest);
            let length = u64::try_from(body.len())
                .map_err(|_| std::io::Error::other("object segment body is too large"))?;
            metadata.extend_from_slice(&length.to_le_bytes());
        }
        file.write_all(&metadata)?;
        for (_, body) in bodies {
            file.write_all(body)?;
        }
        sync_file(&file, durability)
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    match publish_new(&temporary, &destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            validate_segment_file(&destination, &id, position)?;
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            return Err(error.into());
        }
    }
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    sync_parent(&parent, durability)?;
    Ok((id, offsets))
}

fn validate_segment_file(
    path: &Path,
    expected_id: &[u8; 32],
    expected_length: u64,
) -> Result<(), LocalObjectsError> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != expected_length {
        return Err(LocalObjectsError::Corrupt);
    }
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(buffer.get(..read).ok_or(LocalObjectsError::Corrupt)?);
    }
    if hasher.finalize().as_bytes() != expected_id {
        return Err(LocalObjectsError::Corrupt);
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct ValidatedSegmentRecord {
    digest: [u8; 32],
    length: u64,
}

fn validate_referenced_segments(
    root: &Path,
    bodies: &BTreeSet<LocalBodyReference>,
    maximum_object_bytes: u64,
) -> Result<(), LocalObjectsError> {
    let mut validated_segments = BTreeMap::new();
    for body in bodies {
        let LocalBodyLocation::Segment { id, offset } = &body.location;
        if !validated_segments.contains_key(id) {
            let records =
                validate_segment_records(&segment_path(root, id), id, maximum_object_bytes)?;
            validated_segments.insert(*id, records);
        }
        let expected_length = u64::try_from(body.length).map_err(|_| LocalObjectsError::Corrupt)?;
        if !validated_segments.get(id).is_some_and(|records| {
            records.get(offset).is_some_and(|record| {
                record.digest == body.digest && record.length == expected_length
            })
        }) {
            return Err(LocalObjectsError::Corrupt);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn validate_segment_records(
    path: &Path,
    expected_id: &[u8; 32],
    maximum_object_bytes: u64,
) -> Result<BTreeMap<u64, ValidatedSegmentRecord>, LocalObjectsError> {
    let mut file = File::open(path).map_err(|_| LocalObjectsError::Corrupt)?;
    let actual_length = file
        .metadata()
        .map_err(|_| LocalObjectsError::Corrupt)?
        .len();
    let mut hasher = blake3::Hasher::new();
    let mut magic = [0_u8; SEGMENT_MAGIC.len()];
    file.read_exact(&mut magic)
        .map_err(|_| LocalObjectsError::Corrupt)?;
    if magic != *SEGMENT_MAGIC {
        return Err(LocalObjectsError::Corrupt);
    }
    hasher.update(&magic);
    let mut count = [0_u8; 4];
    file.read_exact(&mut count)
        .map_err(|_| LocalObjectsError::Corrupt)?;
    hasher.update(&count);
    let count =
        usize::try_from(u32::from_le_bytes(count)).map_err(|_| LocalObjectsError::Corrupt)?;
    if count == 0 || count > MAXIMUM_SEGMENT_BODIES {
        return Err(LocalObjectsError::Corrupt);
    }
    let table_bytes = count
        .checked_mul(SEGMENT_RECORD_BYTES)
        .ok_or(LocalObjectsError::Corrupt)?;
    let body_start = SEGMENT_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or(LocalObjectsError::Corrupt)?;
    let mut metadata = Vec::new();
    metadata
        .try_reserve_exact(count)
        .map_err(|_| LocalObjectsError::Corrupt)?;
    for _ in 0..count {
        let mut digest = [0_u8; 32];
        file.read_exact(&mut digest)
            .map_err(|_| LocalObjectsError::Corrupt)?;
        hasher.update(&digest);
        let mut length = [0_u8; 8];
        file.read_exact(&mut length)
            .map_err(|_| LocalObjectsError::Corrupt)?;
        hasher.update(&length);
        metadata.push((digest, u64::from_le_bytes(length)));
    }
    if file
        .stream_position()
        .map_err(|_| LocalObjectsError::Corrupt)?
        != u64::try_from(body_start).map_err(|_| LocalObjectsError::Corrupt)?
    {
        return Err(LocalObjectsError::Corrupt);
    }
    let mut records = BTreeMap::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut body_bytes = 0_u64;
    for (digest, length) in metadata {
        let offset = file
            .stream_position()
            .map_err(|_| LocalObjectsError::Corrupt)?;
        if records
            .insert(offset, ValidatedSegmentRecord { digest, length })
            .is_some()
        {
            return Err(LocalObjectsError::Corrupt);
        }
        body_bytes = body_bytes
            .checked_add(length)
            .ok_or(LocalObjectsError::Corrupt)?;
        let maximum_body_bytes = if count == 1 {
            maximum_object_bytes
        } else {
            MAXIMUM_SEGMENT_BYTES as u64
        };
        if body_bytes > maximum_body_bytes {
            return Err(LocalObjectsError::Corrupt);
        }
        let mut body_hasher = blake3::Hasher::new();
        let mut remaining = length;
        while remaining != 0 {
            let selected = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| LocalObjectsError::Corrupt)?;
            let bytes = buffer
                .get_mut(..selected)
                .ok_or(LocalObjectsError::Corrupt)?;
            file.read_exact(bytes)
                .map_err(|_| LocalObjectsError::Corrupt)?;
            hasher.update(bytes);
            body_hasher.update(bytes);
            remaining -= u64::try_from(selected).map_err(|_| LocalObjectsError::Corrupt)?;
        }
        if body_hasher.finalize().as_bytes() != &digest {
            return Err(LocalObjectsError::Corrupt);
        }
    }
    if file
        .stream_position()
        .map_err(|_| LocalObjectsError::Corrupt)?
        != actual_length
        || hasher.finalize().as_bytes() != expected_id
    {
        return Err(LocalObjectsError::Corrupt);
    }
    Ok(records)
}

fn collect_physical_garbage(
    root: &Path,
    live_bodies: &BTreeSet<LocalBodyReference>,
    maximum_candidates: u64,
    maximum_object_bytes: u64,
    durability: LocalDurability,
) -> Result<LocalObjectsGarbageCollection, LocalObjectsError> {
    let mut candidates = 0_u64;
    let mut temporary = Vec::new();
    let segments = scan_segments(root, maximum_candidates, &mut candidates, &mut temporary)?;
    let live_segments = live_bodies
        .iter()
        .map(|body| match body.location {
            LocalBodyLocation::Segment { id, .. } => id,
        })
        .collect::<BTreeSet<_>>();
    let mut validated_segments = BTreeMap::new();
    for id in &live_segments {
        validated_segments.insert(
            *id,
            validate_segment_records(&segment_path(root, id), id, maximum_object_bytes)?,
        );
    }
    for body in live_bodies {
        let LocalBodyLocation::Segment { id, offset } = body.location;
        let expected_length = u64::try_from(body.length).map_err(|_| LocalObjectsError::Corrupt)?;
        if !validated_segments.get(&id).is_some_and(|records| {
            records.get(&offset).is_some_and(|record| {
                record.digest == body.digest && record.length == expected_length
            })
        }) {
            return Err(LocalObjectsError::Corrupt);
        }
    }
    let mut report = LocalObjectsGarbageCollection {
        segments_examined: u64::try_from(segments.len()).unwrap_or(u64::MAX),
        ..LocalObjectsGarbageCollection::default()
    };
    let mut changed_parents = BTreeSet::new();
    for (id, path) in segments {
        if !live_segments.contains(&id) {
            fs::remove_file(&path)?;
            report.segments_removed = report.segments_removed.saturating_add(1);
            changed_parents.insert(root.join("segments"));
        }
    }
    for path in temporary {
        fs::remove_file(&path)?;
        report.temporary_files_removed = report.temporary_files_removed.saturating_add(1);
        if let Some(parent) = path.parent() {
            changed_parents.insert(parent.to_path_buf());
        }
    }
    for parent in changed_parents {
        sync_parent(&parent, durability)?;
    }
    Ok(report)
}

fn scan_segments(
    root: &Path,
    maximum_candidates: u64,
    candidates: &mut u64,
    temporary: &mut Vec<PathBuf>,
) -> Result<Vec<([u8; 32], PathBuf)>, LocalObjectsError> {
    let mut entries = fs::read_dir(root.join("segments"))?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    let mut files = Vec::new();
    for entry in entries {
        if !entry.file_type()?.is_file() {
            return Err(LocalObjectsError::Corrupt);
        }
        *candidates = candidates.checked_add(1).ok_or(LocalObjectsError::Invalid(
            "garbage-collection count overflowed",
        ))?;
        if *candidates > maximum_candidates {
            return Err(LocalObjectsError::Invalid(
                "garbage-collection candidate bound exceeded",
            ));
        }
        let name = entry.file_name();
        let name = name.to_str().ok_or(LocalObjectsError::Corrupt)?;
        if name.starts_with('.') && name.ends_with(".tmp") {
            temporary.push(entry.path());
            continue;
        }
        let encoded = name
            .strip_suffix(".segment")
            .ok_or(LocalObjectsError::Corrupt)?;
        if encoded.len() != 64 || !encoded.bytes().all(is_lower_hex) {
            return Err(LocalObjectsError::Corrupt);
        }
        let mut id = [0_u8; 32];
        decode_lower_hex(encoded, &mut id)?;
        files.push((id, entry.path()));
    }
    Ok(files)
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn decode_lower_hex(encoded: &str, output: &mut [u8]) -> Result<(), LocalObjectsError> {
    if encoded.len() != output.len().saturating_mul(2) {
        return Err(LocalObjectsError::Corrupt);
    }
    for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
        let &[high, low] = pair else {
            unreachable!(
                "the length check above guarantees encoded.len() is even, so chunks_exact(2) never yields a partial chunk"
            )
        };
        let value = (hex_nibble(high)? << 4) | hex_nibble(low)?;
        *output.get_mut(index).ok_or(LocalObjectsError::Corrupt)? = value;
    }
    Ok(())
}

fn hex_nibble(byte: u8) -> Result<u8, LocalObjectsError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(LocalObjectsError::Corrupt),
    }
}

#[cfg(test)]
pub(crate) fn read_body_at(
    root: &Path,
    expected_digest: &[u8; 32],
    expected_length: usize,
    location: &LocalBodyLocation,
    start: usize,
    end: usize,
) -> Result<bytes::Bytes, ObjectsError> {
    let LocalBodyLocation::Segment { id, offset } = location;
    if start > end || end > expected_length {
        return Err(ObjectsError::Invalid("invalid range"));
    }
    let file = File::open(segment_path(root, id)).map_err(|_| ObjectsError::Unavailable)?;
    if !segment_record_matches(
        &file,
        *offset,
        expected_digest,
        u64::try_from(expected_length).map_err(|_| ObjectsError::Unavailable)?,
    )? {
        return Err(ObjectsError::Unavailable);
    }
    let selected_length = end.saturating_sub(start);
    let mut selected = vec![0_u8; selected_length];
    let position = offset
        .checked_add(u64::try_from(start).map_err(|_| ObjectsError::Unavailable)?)
        .ok_or(ObjectsError::Unavailable)?;
    read_exact_at(&file, position, &mut selected)?;
    Ok(selected.into())
}

pub(crate) async fn read_body_at_async(
    root: &Path,
    expected_digest: &[u8; 32],
    expected_length: usize,
    location: &LocalBodyLocation,
    start: usize,
    end: usize,
) -> Result<bytes::Bytes, ObjectsError> {
    let LocalBodyLocation::Segment { id, offset } = location;
    if start > end || end > expected_length {
        return Err(ObjectsError::Invalid("invalid range"));
    }
    let file = File::open(segment_path(root, id)).map_err(|_| ObjectsError::Unavailable)?;
    let file_length = file
        .metadata()
        .map_err(|_| ObjectsError::Unavailable)?
        .len();
    let selected_length = end.saturating_sub(start);
    let selected_offset = offset
        .checked_add(u64::try_from(start).map_err(|_| ObjectsError::Unavailable)?)
        .ok_or(ObjectsError::Unavailable)?;
    let maximum_prefix = SEGMENT_HEADER_BYTES
        .checked_add(MAXIMUM_SEGMENT_BODIES * SEGMENT_RECORD_BYTES)
        .ok_or(ObjectsError::Unavailable)?;
    let reads = acyclic_native_runtime::read_batch_async(
        file,
        vec![
            acyclic_native_runtime::OwnedRead {
                offset: 0,
                length: maximum_prefix,
            },
            acyclic_native_runtime::OwnedRead {
                offset: selected_offset,
                length: selected_length,
            },
        ],
    )
    .await
    .map_err(|_| ObjectsError::Unavailable)?;
    let mut reads = reads.into_iter();
    let prefix = reads.next().ok_or(ObjectsError::Unavailable)?;
    let selected = reads.next().ok_or(ObjectsError::Unavailable)?;
    if prefix.len() < SEGMENT_HEADER_BYTES
        || prefix.get(..SEGMENT_MAGIC.len()) != Some(SEGMENT_MAGIC)
    {
        return Err(ObjectsError::Unavailable);
    }
    let count = usize::try_from(u32::from_le_bytes(
        prefix
            .get(SEGMENT_MAGIC.len()..SEGMENT_HEADER_BYTES)
            .ok_or(ObjectsError::Unavailable)?
            .try_into()
            .map_err(|_| ObjectsError::Unavailable)?,
    ))
    .map_err(|_| ObjectsError::Unavailable)?;
    if count == 0 || count > MAXIMUM_SEGMENT_BODIES {
        return Err(ObjectsError::Unavailable);
    }
    let table_bytes = count
        .checked_mul(SEGMENT_RECORD_BYTES)
        .ok_or(ObjectsError::Unavailable)?;
    let table_end = SEGMENT_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or(ObjectsError::Unavailable)?;
    let table = prefix
        .get(SEGMENT_HEADER_BYTES..table_end)
        .ok_or(ObjectsError::Unavailable)?;
    if selected.len() != selected_length {
        return Err(ObjectsError::Unavailable);
    }
    if !segment_table_matches(
        table,
        file_length,
        *offset,
        expected_digest,
        u64::try_from(expected_length).map_err(|_| ObjectsError::Unavailable)?,
    )? {
        return Err(ObjectsError::Unavailable);
    }
    Ok(selected)
}

fn segment_table_matches(
    table: &[u8],
    file_length: u64,
    expected_offset: u64,
    expected_digest: &[u8; 32],
    expected_length: u64,
) -> Result<bool, ObjectsError> {
    let mut offset = u64::try_from(
        SEGMENT_HEADER_BYTES
            .checked_add(table.len())
            .ok_or(ObjectsError::Unavailable)?,
    )
    .map_err(|_| ObjectsError::Unavailable)?;
    for record in table.chunks_exact(SEGMENT_RECORD_BYTES) {
        let digest: [u8; 32] = record
            .get(..32)
            .ok_or(ObjectsError::Unavailable)?
            .try_into()
            .map_err(|_| ObjectsError::Unavailable)?;
        let length = u64::from_le_bytes(
            record
                .get(32..SEGMENT_RECORD_BYTES)
                .ok_or(ObjectsError::Unavailable)?
                .try_into()
                .map_err(|_| ObjectsError::Unavailable)?,
        );
        let end = offset
            .checked_add(length)
            .ok_or(ObjectsError::Unavailable)?;
        if end > file_length {
            return Err(ObjectsError::Unavailable);
        }
        if offset == expected_offset && digest == *expected_digest && length == expected_length {
            return Ok(true);
        }
        offset = end;
    }
    Ok(false)
}

fn segment_record_matches(
    file: &File,
    expected_offset: u64,
    expected_digest: &[u8; 32],
    expected_length: u64,
) -> Result<bool, ObjectsError> {
    let file_length = file
        .metadata()
        .map_err(|_| ObjectsError::Unavailable)?
        .len();
    let mut header = vec![0_u8; SEGMENT_HEADER_BYTES];
    read_exact_at(file, 0, &mut header)?;
    if header.get(..SEGMENT_MAGIC.len()) != Some(SEGMENT_MAGIC) {
        return Err(ObjectsError::Unavailable);
    }
    let count = usize::try_from(u32::from_le_bytes(
        header
            .get(SEGMENT_MAGIC.len()..SEGMENT_HEADER_BYTES)
            .ok_or(ObjectsError::Unavailable)?
            .try_into()
            .map_err(|_| ObjectsError::Unavailable)?,
    ))
    .map_err(|_| ObjectsError::Unavailable)?;
    if count == 0 || count > MAXIMUM_SEGMENT_BODIES {
        return Err(ObjectsError::Unavailable);
    }
    let table_bytes = count
        .checked_mul(SEGMENT_RECORD_BYTES)
        .ok_or(ObjectsError::Unavailable)?;
    let mut table = vec![0_u8; table_bytes];
    read_exact_at(
        file,
        u64::try_from(SEGMENT_HEADER_BYTES).map_err(|_| ObjectsError::Unavailable)?,
        &mut table,
    )?;
    segment_table_matches(
        &table,
        file_length,
        expected_offset,
        expected_digest,
        expected_length,
    )
}

fn read_exact_at(file: &File, mut offset: u64, mut bytes: &mut [u8]) -> Result<(), ObjectsError> {
    while !bytes.is_empty() {
        let count = acyclic_native_runtime::read_at(file, offset, bytes)
            .map_err(|_| ObjectsError::Unavailable)?;
        if count == 0 {
            return Err(ObjectsError::Unavailable);
        }
        offset = offset
            .checked_add(u64::try_from(count).map_err(|_| ObjectsError::Unavailable)?)
            .ok_or(ObjectsError::Unavailable)?;
        bytes = bytes.get_mut(count..).ok_or(ObjectsError::Unavailable)?;
    }
    Ok(())
}

pub(crate) fn hash_body_at(
    root: &Path,
    expected_digest: &[u8; 32],
    expected_length: usize,
    location: &LocalBodyLocation,
    hasher: &mut blake3::Hasher,
) -> Result<(), ObjectsError> {
    let LocalBodyLocation::Segment { id, offset } = location;
    let file = File::open(segment_path(root, id)).map_err(|_| ObjectsError::Unavailable)?;
    if !segment_record_matches(
        &file,
        *offset,
        expected_digest,
        u64::try_from(expected_length).map_err(|_| ObjectsError::Unavailable)?,
    )? {
        return Err(ObjectsError::Unavailable);
    }
    let mut reader = acyclic_native_runtime::RangeReader::new(
        &file,
        *offset,
        u64::try_from(expected_length).map_err(|_| ObjectsError::Unavailable)?,
    );
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|_| ObjectsError::Unavailable)?;
        if read == 0 {
            break;
        }
        hasher.update(buffer.get(..read).ok_or(ObjectsError::Unavailable)?);
    }
    Ok(())
}

fn segment_path(root: &Path, id: &[u8; 32]) -> PathBuf {
    root.join("segments").join(format!("{}.segment", hex(id)))
}

fn parse_digest(value: &[u8]) -> Result<[u8; 32], LocalObjectsError> {
    value.try_into().map_err(|_| LocalObjectsError::Corrupt)
}

#[allow(
    clippy::indexing_slicing,
    reason = "`byte >> 4` and `byte & 0x0f` are both bit operations on a u8 bounded to 0..16, always in range for the 16-entry DIGITS table"
)]
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[(byte >> 4) as usize]));
        output.push(char::from(DIGITS[(byte & 0x0f) as usize]));
    }
    output
}

/// Splits a hex-encoded digest `identity` into its two-character storage-directory
/// prefix and the remaining suffix used as the file stem.
///
/// Every caller passes an `identity` produced by [`hex`] applied to a `[u8; 32]`
/// digest, which always yields exactly 64 lowercase ASCII hex digits. Because the
/// string is provably pure ASCII, splitting at the fixed byte offset `2` can never
/// land inside a multi-byte character, so `split_at` (unlike byte-offset string
/// indexing) is both panic-free here and exempt from `clippy::string_slice`.
fn sync_file(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, native_durability(durability))
}

#[cfg(not(windows))]
fn publish_new(temporary: &Path, destination: &Path) -> std::io::Result<()> {
    fs::hard_link(temporary, destination)
}

#[cfg(windows)]
fn publish_new(temporary: &Path, destination: &Path) -> std::io::Result<()> {
    acyclic_native_runtime::durable_rename(
        temporary,
        destination,
        acyclic_native_runtime::RenameMode::NoReplace,
    )
}

fn sync_file_data(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_data(file, native_durability(durability))
}

fn native_durability(durability: LocalDurability) -> acyclic_native_runtime::Durability {
    match durability {
        LocalDurability::FullFlush => acyclic_native_runtime::Durability::Full,
        LocalDurability::Barrier => acyclic_native_runtime::Durability::Barrier,
    }
}

fn sync_parent(path: &Path, durability: LocalDurability) -> Result<(), LocalObjectsError> {
    acyclic_native_runtime::sync_parent(path, native_durability(durability))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_background_work_retains_ownership_until_physical_io_stops()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;

        runtime.block_on(async move {
            for operation in [BlockingWork::PersistSegment, BlockingWork::CollectGarbage] {
                let root = tempfile::tempdir()?;
                let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
                let ownership = Arc::clone(&lifecycle).lock_owned().await;
                let provider = LocalObjects::open_with_ownership_anchor(
                    root.path(),
                    LocalObjectsLimits::default(),
                    OwnershipAnchor::new(ownership),
                )
                .await?;
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
                *provider
                    .persistence
                    .blocking_work
                    .lock()
                    .map_err(|_| "background-work test hook was poisoned")? =
                    Some(BlockingWorkHook {
                        operation,
                        started: Some(started_tx),
                        release: release_rx,
                    });

                let work = tokio::spawn({
                    let provider = provider.clone();
                    async move {
                        match operation {
                            BlockingWork::PersistSegment => {
                                let _ = provider
                                    .persist_segment(vec![(
                                        [7; 32],
                                        bytes::Bytes::from_static(b"x"),
                                    )])
                                    .await;
                            }
                            BlockingWork::CollectGarbage => {
                                let _ = provider.collect_garbage(1).await;
                            }
                        }
                    }
                });

                started_rx.await?;
                work.abort();
                assert!(work.await.is_err_and(|error| error.is_cancelled()));
                if operation == BlockingWork::CollectGarbage {
                    assert!(
                        Arc::clone(&provider.body_io).try_read_owned().is_err(),
                        "cancelled garbage collection must keep body publication fenced"
                    );
                    assert!(
                        Arc::clone(&provider.mutation).try_lock_owned().is_err(),
                        "cancelled garbage collection must keep journal mutation fenced"
                    );
                }
                drop(provider);
                assert!(
                    Arc::clone(&lifecycle).try_lock_owned().is_err(),
                    "detached physical I/O must retain local-root ownership"
                );

                release_tx.send(())?;
                tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    Arc::clone(&lifecycle).lock_owned(),
                )
                .await?;
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }

    #[tokio::test]
    async fn cancelled_open_caller_keeps_ownership_until_initialization_stops()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let lock_path = root.path().join("owner.lock");
        let ownership = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        ownership.try_lock_exclusive()?;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let opening = tokio::spawn(run_owned_initialization(async move {
            let _ownership = ownership;
            let _ = started_tx.send(());
            let _ = release_rx.await;
            drop(_ownership);
        }));

        started_rx.await?;
        opening.abort();
        let cancelled = opening.await;
        assert!(
            cancelled.as_ref().is_err_and(|error| error.is_cancelled()),
            "opening caller must be cancelled: {cancelled:?}"
        );
        let contender = OpenOptions::new().read(true).write(true).open(lock_path)?;
        assert!(
            contender.try_lock_exclusive().is_err(),
            "caller cancellation must not release an active initialization's ownership file"
        );
        let _ = release_tx.send(());
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if contender.try_lock_exclusive().is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    #[cfg(not(target_vendor = "apple"))]
    #[tokio::test]
    async fn barrier_policy_rejects_targets_without_exact_barrier_semantics() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits {
            durability: LocalDurability::Barrier,
            ..LocalObjectsLimits::default()
        };
        assert!(matches!(
            LocalObjects::open(root.path(), limits).await,
            Err(LocalObjectsError::Io(error))
                if error.kind() == std::io::ErrorKind::Unsupported
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_puts_remain_recoverable_after_garbage_collection() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("parallel-bodies".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let mut puts = Vec::new();
        for index in 0..4_u8 {
            let store = provider.clone();
            let bucket = bucket.clone();
            puts.push(tokio::spawn(async move {
                store
                    .put(PutRequest {
                        bucket,
                        object_key: format!("body-{index}"),
                        body: bytes::Bytes::from(vec![index; 64 * 1024 + 1]),
                        metadata: wire::ObjectMetadata::default(),
                        condition: None,
                        idempotency_key: None,
                    })
                    .await
            }));
        }
        let collector = {
            let store = provider.clone();
            tokio::spawn(async move { store.collect_garbage(100).await })
        };
        for put in puts {
            assert!(put.await.unwrap_or_else(|_| unreachable!()).is_ok());
        }
        assert!(collector.await.unwrap_or_else(|_| unreachable!()).is_ok());
        drop(provider);
        let reopened = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        for index in 0..4_u8 {
            let body = reopened
                .get(GetRequest {
                    target: ReadTarget::Bucket(bucket.clone()),
                    object_key: format!("body-{index}"),
                    version_id: None,
                    range: None,
                    if_match: None,
                    if_none_match: None,
                    maximum_bytes: u64::try_from(64 * 1024 + 1).unwrap_or(u64::MAX),
                })
                .await
                .unwrap_or_else(|_| unreachable!())
                .body;
            assert_eq!(body.as_ref(), vec![index; 64 * 1024 + 1]);
        }
    }

    #[cfg(windows)]
    #[test]
    fn journal_read_distinguishes_torn_tail_from_host_failure() {
        let mut complete = std::io::Cursor::new([1, 2, 3]);
        let mut bytes = [0; 3];
        assert!(matches!(
            read_exact_or_torn(&mut complete, &mut bytes),
            Ok(true)
        ));
        assert_eq!(bytes, [1, 2, 3]);

        let mut short = std::io::Cursor::new([1, 2]);
        assert!(matches!(
            read_exact_or_torn(&mut short, &mut bytes),
            Ok(false)
        ));

        struct FailingRead;
        impl Read for FailingRead {
            fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::from(std::io::ErrorKind::Other))
            }
        }
        assert_eq!(
            read_exact_or_torn(&mut FailingRead, &mut bytes)
                .err()
                .map(|error| error.kind()),
            Some(std::io::ErrorKind::Other)
        );
    }

    #[tokio::test]
    async fn continuation_from_before_reopen_cannot_bind_to_new_listing() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("reopen-listing".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        for key in ["a", "b"] {
            provider
                .put(PutRequest {
                    bucket: bucket.clone(),
                    object_key: key.into(),
                    body: bytes::Bytes::from_static(b"x"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: None,
                })
                .await
                .unwrap_or_else(|_| unreachable!());
        }
        let stale = provider
            .list(
                ReadTarget::Bucket(bucket.clone()),
                String::new(),
                None,
                false,
                1,
                None,
            )
            .await
            .unwrap_or_else(|_| unreachable!())
            .continuation;
        drop(provider);
        let reopened = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        reopened
            .list(
                ReadTarget::Bucket(bucket.clone()),
                String::new(),
                None,
                false,
                1,
                None,
            )
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(
            reopened
                .list(
                    ReadTarget::Bucket(bucket),
                    String::new(),
                    None,
                    false,
                    1,
                    stale
                )
                .await,
            Err(ObjectsError::Invalid("invalid continuation"))
        );
    }

    #[tokio::test]
    async fn durable_provider_passes_public_conformance_and_reopens() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits {
            maximum_object_bytes: 16 * 1_024 * 1_024,
            maximum_bytes: 64 * 1_024 * 1_024,
            ..LocalObjectsLimits::default()
        };
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert!(
            crate::conformance::verify(&provider, "local-conformance")
                .await
                .is_ok()
        );
        let retained = provider
            .create_bucket("retained-bucket".into(), Some("retain".into()))
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        drop(provider);
        let reopened = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = reopened
            .head_bucket(&retained)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(
            bucket.bucket.unwrap_or_else(|| unreachable!()).name,
            "retained-bucket"
        );
    }

    #[tokio::test]
    async fn ownership_torn_tail_and_range_authentication_are_exact() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert!(matches!(
            LocalObjects::open(root.path(), limits).await,
            Err(LocalObjectsError::AlreadyOwned)
        ));
        let bucket = provider
            .create_bucket("range-bucket".into(), Some("create".into()))
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let body = bytes::Bytes::from(vec![7; 64 * 1024 + 19]);
        let results = provider
            .put_batch(vec![PutRequest {
                bucket: bucket.clone(),
                object_key: "large".into(),
                body,
                metadata: wire::ObjectMetadata::default(),
                condition: Some(Condition::IfAbsent),
                idempotency_key: Some("put".into()),
            }])
            .await;
        assert!(results.into_iter().all(|result| result.is_ok()));
        assert_eq!(
            provider
                .get(GetRequest {
                    target: ReadTarget::Bucket(bucket),
                    object_key: "large".into(),
                    version_id: None,
                    range: Some((64 * 1024 - 4, Some(64 * 1024 + 3))),
                    if_match: None,
                    if_none_match: None,
                    maximum_bytes: 8,
                })
                .await
                .unwrap_or_else(|_| unreachable!())
                .body,
            bytes::Bytes::from_static(&[7; 8])
        );
        drop(provider);
        let journal_path = root.path().join("mutations.log");
        let mut journal = OpenOptions::new()
            .append(true)
            .open(journal_path)
            .unwrap_or_else(|_| unreachable!());
        journal
            .write_all(&[3, 0])
            .unwrap_or_else(|_| unreachable!());
        journal.sync_all().unwrap_or_else(|_| unreachable!());
        drop(journal);
        drop(
            LocalObjects::open(root.path(), limits)
                .await
                .unwrap_or_else(|_| unreachable!()),
        );
    }

    #[tokio::test]
    async fn local_ranges_read_only_the_selected_body_window() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("lazy-range-bucket".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let mut body = vec![0_u8; 4 * 64 * 1024];
        body.get_mut(131_071..131_075)
            .unwrap_or_else(|| unreachable!())
            .copy_from_slice(b"lazy");
        assert!(
            provider
                .put_batch(vec![PutRequest {
                    bucket: bucket.clone(),
                    object_key: "large".into(),
                    body: body.into(),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: None,
                }])
                .await
                .into_iter()
                .all(|result| result.is_ok())
        );

        let selected = provider
            .get(GetRequest {
                target: ReadTarget::Bucket(bucket.clone()),
                object_key: "large".into(),
                version_id: None,
                range: Some((131_071, Some(131_074))),
                if_match: None,
                if_none_match: None,
                maximum_bytes: 4,
            })
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(selected.body, bytes::Bytes::from_static(b"lazy"));

        let empty = provider
            .get(GetRequest {
                target: ReadTarget::Bucket(bucket),
                object_key: "large".into(),
                version_id: None,
                range: Some((0, Some(0))),
                if_match: None,
                if_none_match: None,
                maximum_bytes: 1,
            })
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(empty.body, bytes::Bytes::from_static(&[0]));
    }

    #[tokio::test]
    async fn journal_capacity_rejects_before_semantic_mutation() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits {
            maximum_journal_operations: 1,
            maximum_journal_bytes: 4 * 1_024,
            ..LocalObjectsLimits::default()
        };
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let first = provider
            .create_bucket("first-bucket".into(), Some("first".into()))
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        assert_eq!(
            provider
                .create_bucket("second-bucket".into(), Some("second".into()))
                .await,
            Err(ObjectsError::Capacity)
        );
        drop(provider);
        let reopened = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert!(reopened.head_bucket(&first).await.is_ok());
        assert_eq!(
            reopened
                .head_bucket(&wire::BucketRef {
                    bucket_id: "bucket-0000000000000002".into(),
                    name: "second-bucket".into(),
                })
                .await,
            Err(ObjectsError::NotFound)
        );
    }

    #[tokio::test]
    async fn failed_journal_write_poisoned_until_reopen() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let first = provider
            .create_bucket("before-fault".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let journal_path = root.path().join("mutations.log");
        let writable = {
            let mut journal = provider
                .persistence
                .journal
                .lock()
                .unwrap_or_else(|_| unreachable!());
            std::mem::replace(
                &mut *journal,
                File::open(journal_path).unwrap_or_else(|_| unreachable!()),
            )
        };
        assert_eq!(
            provider.create_bucket("faulted".into(), None).await,
            Err(ObjectsError::Unavailable)
        );
        *provider
            .persistence
            .journal
            .lock()
            .unwrap_or_else(|_| unreachable!()) = writable;
        assert_eq!(
            provider.create_bucket("after-fault".into(), None).await,
            Err(ObjectsError::Unavailable)
        );
        assert_eq!(
            provider.head_bucket(&first).await,
            Err(ObjectsError::Unavailable)
        );
        assert!(matches!(
            provider.collect_garbage(10).await,
            Err(LocalObjectsError::Unavailable)
        ));
        drop(provider);
        let reopened = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert!(reopened.head_bucket(&first).await.is_ok());
        assert_eq!(reopened.bucket_named("after-fault").await, Ok(None));
    }

    #[tokio::test]
    async fn injected_partial_append_and_post_write_failure_fail_closed() {
        let payload = MutationRecord {
            operation: Some(mutation_record::Operation::CreateBucket(
                wire::CreateBucketRequest {
                    name: "faulted".into(),
                    mutation: None,
                },
            )),
        }
        .encode_to_vec();
        let frame_bytes = 36 + payload.len();
        for offset in [
            0,
            2,
            4,
            20,
            36,
            37,
            frame_bytes / 2,
            frame_bytes - 1,
            frame_bytes,
        ] {
            let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
            let limits = LocalObjectsLimits::default();
            let provider = LocalObjects::open(root.path(), limits)
                .await
                .unwrap_or_else(|_| unreachable!());
            let first = provider
                .create_bucket("before-fault".into(), None)
                .await
                .unwrap_or_else(|_| unreachable!())
                .bucket
                .unwrap_or_else(|| unreachable!());
            provider
                .persistence
                .fault_after_bytes
                .store(offset as u64, Ordering::Release);
            assert_eq!(
                provider.create_bucket("faulted".into(), None).await,
                Err(ObjectsError::Unavailable),
                "offset {offset}"
            );
            assert_eq!(
                provider.create_bucket("after-fault".into(), None).await,
                Err(ObjectsError::Unavailable),
                "offset {offset}"
            );
            assert_eq!(
                provider.head_bucket(&first).await,
                Err(ObjectsError::Unavailable),
                "offset {offset}"
            );
            drop(provider);

            let recovered = LocalObjects::open(root.path(), limits)
                .await
                .unwrap_or_else(|_| unreachable!());
            assert!(
                recovered.head_bucket(&first).await.is_ok(),
                "offset {offset}"
            );
            assert_eq!(
                recovered
                    .bucket_named("faulted")
                    .await
                    .unwrap_or_else(|_| unreachable!())
                    .is_some(),
                offset == frame_bytes,
                "offset {offset}"
            );
            assert_eq!(recovered.bucket_named("after-fault").await, Ok(None));
            recovered
                .create_bucket("after-recovery".into(), None)
                .await
                .unwrap_or_else(|_| unreachable!());
            drop(recovered);

            let reopened = LocalObjects::open(root.path(), limits)
                .await
                .unwrap_or_else(|_| unreachable!());
            assert!(
                reopened.head_bucket(&first).await.is_ok(),
                "offset {offset}"
            );
            assert_eq!(reopened.bucket_named("after-fault").await, Ok(None));
            assert!(
                reopened
                    .bucket_named("after-recovery")
                    .await
                    .is_ok_and(|bucket| bucket.is_some()),
                "offset {offset}"
            );
        }
    }

    #[tokio::test]
    async fn injected_journal_sync_error_poison_and_recover_complete_frame() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let first = provider
            .create_bucket("before-sync-fault".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        provider
            .persistence
            .fault_sync_once
            .store(true, Ordering::Release);
        assert_eq!(
            provider.create_bucket("sync-fault".into(), None).await,
            Err(ObjectsError::Unavailable)
        );
        assert_eq!(
            provider
                .create_bucket("after-sync-fault".into(), None)
                .await,
            Err(ObjectsError::Unavailable)
        );
        assert_eq!(
            provider.head_bucket(&first).await,
            Err(ObjectsError::Unavailable)
        );
        drop(provider);

        let recovered = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert!(recovered.head_bucket(&first).await.is_ok());
        assert!(
            recovered
                .bucket_named("sync-fault")
                .await
                .is_ok_and(|bucket| bucket.is_some())
        );
        assert_eq!(recovered.bucket_named("after-sync-fault").await, Ok(None));
    }

    #[tokio::test]
    async fn new_invalid_put_intents_still_consume_journal_capacity() {
        for variant in 0..3 {
            let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
            let limits = LocalObjectsLimits {
                maximum_journal_operations: 2,
                ..LocalObjectsLimits::default()
            };
            let provider = LocalObjects::open(root.path(), limits)
                .await
                .unwrap_or_else(|_| unreachable!());
            let bucket = provider
                .create_bucket("invalid-put".into(), None)
                .await
                .unwrap_or_else(|_| unreachable!())
                .bucket
                .unwrap_or_else(|| unreachable!());
            let mut request = PutRequest {
                bucket,
                object_key: "valid".into(),
                body: bytes::Bytes::new(),
                metadata: wire::ObjectMetadata::default(),
                condition: None,
                idempotency_key: None,
            };
            match variant {
                0 => request.object_key.clear(),
                1 => request.metadata.content_type = "bad\nheader".into(),
                _ => request.idempotency_key = Some(String::new()),
            }
            assert!(
                matches!(provider.put(request).await, Err(ObjectsError::Invalid(_))),
                "variant {variant}"
            );
            assert_eq!(
                provider.create_bucket("after-invalid".into(), None).await,
                Err(ObjectsError::Capacity),
                "variant {variant}"
            );
        }
    }

    #[tokio::test]
    async fn replayed_failed_puts_neither_publish_bodies_nor_append() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits {
            maximum_object_bytes: 4,
            ..LocalObjectsLimits::default()
        };
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("failed-replays".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        provider
            .put(PutRequest {
                bucket: bucket.clone(),
                object_key: "existing".into(),
                body: bytes::Bytes::from_static(b"base"),
                metadata: wire::ObjectMetadata::default(),
                condition: None,
                idempotency_key: None,
            })
            .await
            .unwrap_or_else(|_| unreachable!());
        let mut missing_bucket = bucket.clone();
        missing_bucket.bucket_id = "missing-bucket".into();
        let cases = [
            (
                PutRequest {
                    bucket: bucket.clone(),
                    object_key: "existing".into(),
                    body: bytes::Bytes::from_static(b"one"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: Some(Condition::IfAbsent),
                    idempotency_key: Some("failed-condition".into()),
                },
                ObjectsError::PreconditionFailed,
            ),
            (
                PutRequest {
                    bucket: missing_bucket,
                    object_key: "missing".into(),
                    body: bytes::Bytes::from_static(b"two"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: Some("failed-bucket".into()),
                },
                ObjectsError::NotFound,
            ),
            (
                PutRequest {
                    bucket,
                    object_key: "too-large".into(),
                    body: bytes::Bytes::from_static(b"overflow"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: Some("failed-capacity".into()),
                },
                ObjectsError::Capacity,
            ),
        ];
        for (request, expected) in cases {
            assert_eq!(provider.put(request.clone()).await, Err(expected.clone()));
            let journal_bytes = fs::metadata(root.path().join("mutations.log"))
                .unwrap_or_else(|_| unreachable!())
                .len();
            let operations = provider
                .persistence
                .journal_operations
                .load(Ordering::Acquire);
            assert_eq!(provider.put(request).await, Err(expected));
            assert_eq!(
                fs::metadata(root.path().join("mutations.log"))
                    .unwrap_or_else(|_| unreachable!())
                    .len(),
                journal_bytes
            );
            assert_eq!(
                provider
                    .persistence
                    .journal_operations
                    .load(Ordering::Acquire),
                operations
            );
        }
        drop(provider);
        LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
    }

    #[tokio::test]
    async fn recovery_discards_partial_frame_at_every_write_boundary() {
        let payload = MutationRecord {
            operation: Some(mutation_record::Operation::CreateBucket(
                wire::CreateBucketRequest {
                    name: "torn-bucket".into(),
                    mutation: None,
                },
            )),
        }
        .encode_to_vec();
        let mut frame = Vec::with_capacity(36 + payload.len());
        frame.extend_from_slice(
            &u32::try_from(payload.len())
                .unwrap_or_else(|_| unreachable!())
                .to_le_bytes(),
        );
        frame.extend_from_slice(blake3::hash(&payload).as_bytes());
        frame.extend_from_slice(&payload);

        for prefix in 1..frame.len() {
            let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
            let limits = LocalObjectsLimits::default();
            let provider = LocalObjects::open(root.path(), limits)
                .await
                .unwrap_or_else(|_| unreachable!());
            let first = provider
                .create_bucket("before-torn".into(), None)
                .await
                .unwrap_or_else(|_| unreachable!())
                .bucket
                .unwrap_or_else(|| unreachable!());
            drop(provider);

            let mut journal = OpenOptions::new()
                .append(true)
                .open(root.path().join("mutations.log"))
                .unwrap_or_else(|_| unreachable!());
            journal
                .write_all(frame.get(..prefix).unwrap_or_else(|| unreachable!()))
                .unwrap_or_else(|_| unreachable!());
            journal.sync_all().unwrap_or_else(|_| unreachable!());
            drop(journal);

            let recovered = LocalObjects::open(root.path(), limits)
                .await
                .unwrap_or_else(|_| unreachable!());
            assert!(
                recovered.head_bucket(&first).await.is_ok(),
                "prefix {prefix}"
            );
            assert_eq!(
                recovered.bucket_named("torn-bucket").await,
                Ok(None),
                "prefix {prefix}"
            );
            recovered
                .create_bucket("after-recovery".into(), None)
                .await
                .unwrap_or_else(|_| unreachable!());
            drop(recovered);

            let reopened = LocalObjects::open(root.path(), limits)
                .await
                .unwrap_or_else(|_| unreachable!());
            assert!(
                reopened.head_bucket(&first).await.is_ok(),
                "prefix {prefix}"
            );
            assert_eq!(
                reopened.bucket_named("torn-bucket").await,
                Ok(None),
                "prefix {prefix}"
            );
            assert!(
                reopened
                    .bucket_named("after-recovery")
                    .await
                    .is_ok_and(|bucket| bucket.is_some()),
                "prefix {prefix}"
            );
        }
    }

    #[tokio::test]
    async fn replay_failure_drains_the_bounded_pipeline_and_releases_ownership() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        drop(provider);

        let invalid = MutationRecord {
            operation: Some(mutation_record::Operation::DeleteBucket(
                wire::DeleteBucketRequest {
                    bucket: None,
                    mutation: None,
                },
            )),
        }
        .encode_to_vec();
        let valid = MutationRecord {
            operation: Some(mutation_record::Operation::CreateBucket(
                wire::CreateBucketRequest {
                    name: "later".into(),
                    mutation: None,
                },
            )),
        }
        .encode_to_vec();
        let journal_path = root.path().join("mutations.log");
        let mut journal = OpenOptions::new()
            .append(true)
            .open(journal_path)
            .unwrap_or_else(|_| unreachable!());
        journal
            .write_all(&encode_frame(&invalid).unwrap_or_else(|_| unreachable!()))
            .unwrap_or_else(|_| unreachable!());
        let valid = encode_frame(&valid).unwrap_or_else(|_| unreachable!());
        for _ in 0..REPLAY_PIPELINE_RECORDS * 4 {
            journal.write_all(&valid).unwrap_or_else(|_| unreachable!());
        }
        journal.sync_all().unwrap_or_else(|_| unreachable!());
        drop(journal);

        for _ in 0..2 {
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                LocalObjects::open(root.path(), limits),
            )
            .await
            .unwrap_or_else(|_| unreachable!());
            assert!(matches!(result, Err(LocalObjectsError::Corrupt)));
        }
    }

    #[tokio::test]
    #[allow(
        clippy::panic,
        reason = "this test intentionally poisons the journal mutex"
    )]
    async fn poisoned_journal_mutex_blocks_reads_and_reclamation() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let provider = LocalObjects::open(root.path(), LocalObjectsLimits::default())
            .await
            .unwrap_or_else(|_| unreachable!());
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _journal = provider
                .persistence
                .journal
                .lock()
                .unwrap_or_else(|_| unreachable!());
            panic!("inject a journal-holder panic");
        }));
        assert!(caught.is_err());
        assert_eq!(
            provider.bucket_named("unavailable").await,
            Err(ObjectsError::Unavailable)
        );
        assert_eq!(
            provider.create_bucket("unavailable".into(), None).await,
            Err(ObjectsError::Unavailable)
        );
        assert!(matches!(
            provider.collect_garbage(10).await,
            Err(LocalObjectsError::Unavailable)
        ));
    }

    #[tokio::test]
    async fn removed_journal_versions_are_rejected() {
        for version in [1_u8, 2] {
            let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
            let limits = LocalObjectsLimits::default();
            let mut journal =
                File::create(root.path().join("mutations.log")).unwrap_or_else(|_| unreachable!());
            let mut magic = *JOURNAL_MAGIC;
            *magic.last_mut().unwrap_or_else(|| unreachable!()) = version;
            journal.write_all(&magic).unwrap_or_else(|_| unreachable!());
            for value in [
                limits.maximum_object_bytes,
                limits.maximum_bytes,
                limits.maximum_journal_operations,
                limits.maximum_journal_bytes,
            ] {
                journal
                    .write_all(&value.to_le_bytes())
                    .unwrap_or_else(|_| unreachable!());
            }
            journal.sync_all().unwrap_or_else(|_| unreachable!());
            drop(journal);

            assert!(matches!(
                LocalObjects::open(root.path(), limits).await,
                Err(LocalObjectsError::Invalid(
                    "durable header or capacity limits differ"
                ))
            ));
        }
    }

    #[tokio::test]
    async fn one_body_segment_can_exceed_the_multi_body_limit() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("large-segment".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let body = bytes::Bytes::from(vec![37; MAXIMUM_SEGMENT_BYTES + 1]);
        let results = provider
            .put_batch(vec![PutRequest {
                bucket: bucket.clone(),
                object_key: "large".into(),
                body,
                metadata: wire::ObjectMetadata::default(),
                condition: Some(Condition::IfAbsent),
                idempotency_key: None,
            }])
            .await;
        assert!(results.into_iter().all(|result| result.is_ok()));
        drop(provider);

        let reopened = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let range_start = u64::try_from(MAXIMUM_SEGMENT_BYTES - 3).unwrap_or(u64::MAX);
        assert_eq!(
            reopened
                .get(GetRequest {
                    target: ReadTarget::Bucket(bucket),
                    object_key: "large".into(),
                    version_id: None,
                    range: Some((range_start, Some(range_start + 3))),
                    if_match: None,
                    if_none_match: None,
                    maximum_bytes: 4,
                })
                .await
                .unwrap_or_else(|_| unreachable!())
                .body,
            bytes::Bytes::from_static(&[37; 4])
        );
    }

    #[tokio::test]
    async fn segment_gc_is_bounded_and_preserves_live_bodies() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("segment-gc".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        provider
            .put(PutRequest {
                bucket: bucket.clone(),
                object_key: "live".into(),
                body: bytes::Bytes::from_static(b"live body"),
                metadata: wire::ObjectMetadata::default(),
                condition: None,
                idempotency_key: None,
            })
            .await
            .unwrap_or_else(|_| unreachable!());
        let orphan_body = bytes::Bytes::from_static(b"orphan body");
        let orphan_digest = *blake3::hash(&orphan_body).as_bytes();
        let (orphan_id, _) = persist_segment(
            root.path(),
            &[(orphan_digest, orphan_body)],
            limits.durability,
        )
        .unwrap_or_else(|_| unreachable!());

        assert!(matches!(
            provider.collect_garbage(1).await,
            Err(LocalObjectsError::Invalid(
                "garbage-collection candidate bound exceeded"
            ))
        ));
        let report = provider
            .collect_garbage(16)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(report.segments_examined, 2);
        assert_eq!(report.segments_removed, 1);
        assert!(!segment_path(root.path(), &orphan_id).exists());
        assert_eq!(
            provider
                .get(GetRequest {
                    target: ReadTarget::Bucket(bucket),
                    object_key: "live".into(),
                    version_id: None,
                    range: None,
                    if_match: None,
                    if_none_match: None,
                    maximum_bytes: 9,
                })
                .await
                .unwrap_or_else(|_| unreachable!())
                .body,
            bytes::Bytes::from_static(b"live body")
        );
    }

    #[tokio::test]
    async fn multipart_replay_authenticates_its_segment() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("multipart-segment".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let upload = provider
            .create_multipart(
                bucket.clone(),
                "object".into(),
                wire::ObjectMetadata::default(),
                None,
                None,
            )
            .await
            .unwrap_or_else(|_| unreachable!());
        let part = provider
            .upload_part(
                bucket.clone(),
                "object".into(),
                upload.upload_id.clone(),
                1,
                bytes::Bytes::from_static(b"multipart body"),
                None,
            )
            .await
            .unwrap_or_else(|_| unreachable!());
        provider
            .complete_multipart(bucket, "object".into(), upload.upload_id, vec![part], None)
            .await
            .unwrap_or_else(|_| unreachable!());
        drop(provider);

        let segment = fs::read_dir(root.path().join("segments"))
            .unwrap_or_else(|_| unreachable!())
            .next()
            .unwrap_or_else(|| unreachable!())
            .unwrap_or_else(|_| unreachable!())
            .path();
        let mut corrupted = fs::read(&segment).unwrap_or_else(|_| unreachable!());
        *corrupted.last_mut().unwrap_or_else(|| unreachable!()) ^= 1;
        fs::write(segment, corrupted).unwrap_or_else(|_| unreachable!());
        assert!(matches!(
            LocalObjects::open(root.path(), limits).await,
            Err(LocalObjectsError::Corrupt)
        ));
    }

    #[tokio::test]
    async fn oversized_public_batch_is_partitioned_into_bounded_segments() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("bounded-batch".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let requests = (0..=MAXIMUM_SEGMENT_BODIES)
            .map(|index| PutRequest {
                bucket: bucket.clone(),
                object_key: format!("object-{index}"),
                body: bytes::Bytes::from(vec![u8::try_from(index).unwrap_or(u8::MAX); index + 1]),
                metadata: wire::ObjectMetadata::default(),
                condition: Some(Condition::IfAbsent),
                idempotency_key: None,
            })
            .collect();
        assert!(provider.put_batch(requests).await.iter().all(Result::is_ok));

        let segments = scan_segments(root.path(), 16, &mut 0, &mut Vec::new())
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(segments.len(), 2);
        let mut record_counts = segments
            .into_iter()
            .map(|(id, path)| {
                validate_segment_records(&path, &id, limits.maximum_object_bytes)
                    .unwrap_or_else(|_| unreachable!())
                    .len()
            })
            .collect::<Vec<_>>();
        record_counts.sort_unstable();
        assert_eq!(record_counts, [1, MAXIMUM_SEGMENT_BODIES]);
    }

    #[tokio::test]
    async fn invalid_large_body_never_shares_a_live_segment() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits {
            maximum_object_bytes: 4,
            ..LocalObjectsLimits::default()
        };
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("mixed-size-batch".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let results = provider
            .put_batch(vec![
                PutRequest {
                    bucket: bucket.clone(),
                    object_key: "invalid".into(),
                    body: bytes::Bytes::from_static(b"too large"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: Some(Condition::IfAbsent),
                    idempotency_key: None,
                },
                PutRequest {
                    bucket,
                    object_key: "valid".into(),
                    body: bytes::Bytes::from_static(b"ok"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: Some(Condition::IfAbsent),
                    idempotency_key: None,
                },
            ])
            .await;
        assert!(matches!(results.first(), Some(Err(ObjectsError::Capacity))));
        assert!(matches!(results.get(1), Some(Ok(_))));

        let report = provider
            .collect_garbage(16)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(report.segments_removed, 1);
        let segments = scan_segments(root.path(), 16, &mut 0, &mut Vec::new())
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(segments.len(), 1);
        let (id, path) = segments
            .into_iter()
            .next()
            .unwrap_or_else(|| unreachable!());
        assert_eq!(
            validate_segment_records(&path, &id, limits.maximum_object_bytes)
                .unwrap_or_else(|_| unreachable!())
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn put_batch_uses_one_segment_and_replays_exact_ranges() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("segment-batch".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let bodies = [
            bytes::Bytes::from_static(b"first segmented body"),
            bytes::Bytes::from_static(b"second segmented body"),
        ];
        let requests = bodies
            .iter()
            .enumerate()
            .map(|(index, body)| PutRequest {
                bucket: bucket.clone(),
                object_key: format!("object-{index}"),
                body: body.clone(),
                metadata: wire::ObjectMetadata::default(),
                condition: Some(Condition::IfAbsent),
                idempotency_key: Some(format!("segment-put-{index}")),
            })
            .collect::<Vec<_>>();
        let before = provider
            .persistence
            .journal_operations
            .load(Ordering::Acquire);
        let results = provider.put_batch(requests).await;
        assert!(results.iter().all(Result::is_ok));
        assert_eq!(
            provider
                .persistence
                .journal_operations
                .load(Ordering::Acquire),
            before + 2
        );
        assert_eq!(
            fs::read_dir(root.path().join("segments"))
                .unwrap_or_else(|_| unreachable!())
                .count(),
            1
        );
        drop(provider);

        let reopened = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        for (index, expected) in bodies.iter().enumerate() {
            let read = reopened
                .get(GetRequest {
                    target: ReadTarget::Bucket(bucket.clone()),
                    object_key: format!("object-{index}"),
                    version_id: None,
                    range: Some((2, Some(8))),
                    if_match: None,
                    if_none_match: None,
                    maximum_bytes: expected.len() as u64,
                })
                .await
                .unwrap_or_else(|_| unreachable!());
            assert_eq!(read.body, expected.slice(2..9));
        }
        drop(reopened);

        let segment_path = fs::read_dir(root.path().join("segments"))
            .unwrap_or_else(|_| unreachable!())
            .next()
            .unwrap_or_else(|| unreachable!())
            .unwrap_or_else(|_| unreachable!())
            .path();
        let original = fs::read(&segment_path).unwrap_or_else(|_| unreachable!());
        let mut corrupted = original.clone();
        corrupted.push(0);
        fs::write(&segment_path, corrupted).unwrap_or_else(|_| unreachable!());
        assert!(matches!(
            LocalObjects::open(root.path(), limits).await,
            Err(LocalObjectsError::Corrupt)
        ));
        let mut corrupted = original;
        if let Some(last) = corrupted.last_mut() {
            *last ^= 1;
        }
        fs::write(segment_path, corrupted).unwrap_or_else(|_| unreachable!());
        assert!(matches!(
            LocalObjects::open(root.path(), limits).await,
            Err(LocalObjectsError::Corrupt)
        ));
    }

    #[tokio::test]
    async fn put_batch_interns_identical_physical_bodies() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("deduplicated-segment".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let body = bytes::Bytes::from_static(b"one shared physical body");
        let before = provider
            .persistence
            .journal_operations
            .load(Ordering::Acquire);
        let results = provider
            .put_batch(
                (0..2)
                    .map(|index| PutRequest {
                        bucket: bucket.clone(),
                        object_key: format!("object-{index}"),
                        body: body.clone(),
                        metadata: wire::ObjectMetadata::default(),
                        condition: Some(Condition::IfAbsent),
                        idempotency_key: None,
                    })
                    .collect(),
            )
            .await;
        assert!(results.iter().all(Result::is_ok));
        assert_eq!(
            provider
                .persistence
                .journal_operations
                .load(Ordering::Acquire),
            before + 2
        );
        let segment = fs::read_dir(root.path().join("segments"))
            .unwrap_or_else(|_| unreachable!())
            .next()
            .unwrap_or_else(|| unreachable!())
            .unwrap_or_else(|_| unreachable!())
            .path();
        let segment_id = segment
            .file_stem()
            .and_then(|name| name.to_str())
            .and_then(|name| hex::decode(name).ok())
            .and_then(|bytes| bytes.try_into().ok())
            .unwrap_or_else(|| unreachable!());
        let records = validate_segment_records(
            &segment,
            &segment_id,
            LocalObjectsLimits::default().maximum_object_bytes,
        )
        .unwrap_or_else(|_| unreachable!());
        assert_eq!(records.len(), 1);
        drop(provider);

        let reopened = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        for index in 0..2 {
            let read = reopened
                .get(GetRequest {
                    target: ReadTarget::Bucket(bucket.clone()),
                    object_key: format!("object-{index}"),
                    version_id: None,
                    range: None,
                    if_match: None,
                    if_none_match: None,
                    maximum_bytes: u64::try_from(body.len()).unwrap_or(u64::MAX),
                })
                .await
                .unwrap_or_else(|_| unreachable!());
            assert_eq!(read.body, body);
        }
    }

    #[tokio::test]
    async fn reads_hold_body_io_against_physical_reclamation() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let provider = LocalObjects::open(root.path(), LocalObjectsLimits::default())
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("guarded-segment-read".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let results = provider
            .put_batch(vec![
                PutRequest {
                    bucket: bucket.clone(),
                    object_key: "first".into(),
                    body: bytes::Bytes::from_static(b"first"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: None,
                },
                PutRequest {
                    bucket: bucket.clone(),
                    object_key: "second".into(),
                    body: bytes::Bytes::from_static(b"second"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: None,
                },
            ])
            .await;
        assert!(results.iter().all(Result::is_ok));

        let body_io = provider.body_io.write().await;
        let reader = provider.clone();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let started = Arc::clone(&barrier);
        let mut read = tokio::spawn(async move {
            started.wait().await;
            reader
                .get(GetRequest {
                    target: ReadTarget::Bucket(bucket),
                    object_key: "first".into(),
                    version_id: None,
                    range: None,
                    if_match: None,
                    if_none_match: None,
                    maximum_bytes: 5,
                })
                .await
        });
        barrier.wait().await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut read)
                .await
                .is_err(),
            "physical read must wait while reclamation owns body I/O"
        );
        drop(body_io);
        let object = read
            .await
            .unwrap_or_else(|_| unreachable!())
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(object.body, bytes::Bytes::from_static(b"first"));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn truncated_segment_with_live_empty_body_fails_reopen() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("empty-segment".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let results = provider
            .put_batch(vec![
                PutRequest {
                    bucket: bucket.clone(),
                    object_key: "empty".into(),
                    body: bytes::Bytes::new(),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: None,
                },
                PutRequest {
                    bucket,
                    object_key: "nonempty".into(),
                    body: bytes::Bytes::from_static(b"body"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: None,
                },
            ])
            .await;
        assert!(results.iter().all(Result::is_ok));
        let empty = provider
            .semantic
            .local_body_references()
            .await
            .into_iter()
            .find(|body| body.length == 0)
            .unwrap_or_else(|| unreachable!());
        let LocalBodyLocation::Segment { id, .. } = empty.location;
        assert!(
            read_body_at(
                root.path(),
                &empty.digest,
                0,
                &LocalBodyLocation::Segment { id, offset: 0 },
                0,
                0,
            )
            .is_err()
        );
        drop(provider);

        let segment = fs::read_dir(root.path().join("segments"))
            .unwrap_or_else(|_| unreachable!())
            .next()
            .unwrap_or_else(|| unreachable!())
            .unwrap_or_else(|_| unreachable!())
            .path();
        let empty_record_end = u64::try_from(SEGMENT_HEADER_BYTES + 2 * SEGMENT_RECORD_BYTES)
            .unwrap_or_else(|_| unreachable!());
        OpenOptions::new()
            .write(true)
            .open(segment)
            .unwrap_or_else(|_| unreachable!())
            .set_len(empty_record_end)
            .unwrap_or_else(|_| unreachable!());
        assert!(matches!(
            LocalObjects::open(root.path(), limits).await,
            Err(LocalObjectsError::Corrupt)
        ));
    }

    #[tokio::test]
    async fn replay_rejects_empty_body_reference_outside_segment_records() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("empty-offset".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let results = provider
            .put_batch(vec![
                PutRequest {
                    bucket: bucket.clone(),
                    object_key: "empty".into(),
                    body: bytes::Bytes::new(),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: None,
                },
                PutRequest {
                    bucket,
                    object_key: "body".into(),
                    body: bytes::Bytes::from_static(b"body"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: None,
                },
            ])
            .await;
        assert!(results.iter().all(Result::is_ok));
        drop(provider);

        let journal_path = root.path().join("mutations.log");
        let original = fs::read(&journal_path).unwrap_or_else(|_| unreachable!());
        let mut cursor = usize::try_from(JOURNAL_HEADER_BYTES).unwrap_or_else(|_| unreachable!());
        let mut last_frame = None;
        while cursor < original.len() {
            let length = original
                .get(cursor..cursor + 4)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_le_bytes)
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or_else(|| unreachable!());
            let payload_start = cursor + 36;
            let frame_end = payload_start + length;
            last_frame = Some((cursor, payload_start, frame_end));
            cursor = frame_end;
        }
        assert_eq!(cursor, original.len());
        let (frame_start, payload_start, frame_end) = last_frame.unwrap_or_else(|| unreachable!());
        let mut record = MutationRecord::decode(
            original
                .get(payload_start..frame_end)
                .unwrap_or_else(|| unreachable!()),
        )
        .unwrap_or_else(|_| unreachable!());
        let Some(mutation_record::Operation::PutBatch(batch)) = record.operation.as_mut() else {
            unreachable!();
        };
        let empty = batch
            .puts
            .iter_mut()
            .find(|put| put.body_length == 0)
            .unwrap_or_else(|| unreachable!());
        empty.segment_offset = 0;
        let mut forged = original
            .get(..frame_start)
            .unwrap_or_else(|| unreachable!())
            .to_vec();
        let frame = encode_frame(&record.encode_to_vec()).unwrap_or_else(|_| unreachable!());
        forged.write_all(&frame).unwrap_or_else(|_| unreachable!());
        fs::write(journal_path, forged).unwrap_or_else(|_| unreachable!());
        assert!(matches!(
            LocalObjects::open(root.path(), limits).await,
            Err(LocalObjectsError::Corrupt)
        ));
    }

    #[tokio::test]
    async fn replay_rejects_put_batches_outside_segment_bounds() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let semantic = MemoryObjects::default();
        let oversized_count = PutBatchRecord {
            puts: (0..=MAXIMUM_SEGMENT_BODIES)
                .map(|_| PutRecord::default())
                .collect(),
        };
        assert!(matches!(
            replay_operation(
                root.path(),
                &semantic,
                mutation_record::Operation::PutBatch(oversized_count),
            )
            .await,
            Err(LocalObjectsError::Corrupt)
        ));
        let oversized_bytes = PutBatchRecord {
            puts: vec![PutRecord {
                body_length: u64::try_from(MAXIMUM_SEGMENT_BYTES)
                    .unwrap_or_else(|_| unreachable!())
                    + 1,
                ..PutRecord::default()
            }],
        };
        assert!(matches!(
            replay_operation(
                root.path(),
                &semantic,
                mutation_record::Operation::PutBatch(oversized_bytes),
            )
            .await,
            Err(LocalObjectsError::Corrupt)
        ));
    }

    #[tokio::test]
    async fn torn_batch_journal_keeps_segment_unpublished_and_collectible() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("torn-segment-batch".into(), None)
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        provider
            .persistence
            .fault_after_bytes
            .store(8, Ordering::Release);
        let results = provider
            .put_batch(vec![
                PutRequest {
                    bucket: bucket.clone(),
                    object_key: "first".into(),
                    body: bytes::Bytes::from_static(b"first orphan"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: None,
                },
                PutRequest {
                    bucket: bucket.clone(),
                    object_key: "second".into(),
                    body: bytes::Bytes::from_static(b"second orphan"),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: None,
                },
            ])
            .await;
        assert!(
            results
                .iter()
                .all(|result| result == &Err(ObjectsError::Unavailable))
        );
        drop(provider);

        let reopened = LocalObjects::open(root.path(), limits)
            .await
            .unwrap_or_else(|_| unreachable!());
        for key in ["first", "second"] {
            assert_eq!(
                reopened
                    .get(GetRequest {
                        target: ReadTarget::Bucket(bucket.clone()),
                        object_key: key.into(),
                        version_id: None,
                        range: None,
                        if_match: None,
                        if_none_match: None,
                        maximum_bytes: 64,
                    })
                    .await,
                Err(ObjectsError::NotFound)
            );
        }
        reopened
            .collect_garbage(8)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(
            fs::read_dir(root.path().join("segments"))
                .unwrap_or_else(|_| unreachable!())
                .count(),
            0
        );
    }
}
