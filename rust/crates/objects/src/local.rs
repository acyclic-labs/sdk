//! Durable local Objects provider over the canonical in-memory state machine.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use fs2::FileExt;
use prost::Message;
use tokio::sync::Mutex;

use crate::{
    BufferedObject, Condition, DeleteResult, ExternalBody, GetRequest, MemoryObjects, ObjectsError,
    ObjectsProvider, ProviderListPage, PutRequest, ReadTarget, wire,
};

const JOURNAL_MAGIC: &[u8; 23] = b"ACYCLIC-OBJECTS-LOCAL\0\x01";
const JOURNAL_HEADER_BYTES: u64 = 55;
const MAXIMUM_RECORD_BYTES: usize = 2 * 1_024 * 1_024;
const CHUNK_BYTES: usize = 1_024 * 1_024;
const MANIFEST_MAGIC: &[u8; 16] = b"ACYCLIC-BODY\0\0\0\x01";
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Exact durable-local capacity contract. Reopening requires the same limits.
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
}

impl Default for LocalObjectsLimits {
    fn default() -> Self {
        Self {
            maximum_object_bytes: 64 * 1_024 * 1_024,
            maximum_bytes: 4 * 1_024 * 1_024 * 1_024,
            maximum_journal_operations: 1_000_000,
            maximum_journal_bytes: 1_024 * 1_024 * 1_024,
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
    /// Host filesystem operation failed.
    #[error("local Objects I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

struct Persistence {
    root: PathBuf,
    journal: StdMutex<File>,
    journal_operations: AtomicU64,
    limits: LocalObjectsLimits,
    _ownership: File,
}

/// Crash-safe, exclusive-owner local implementation of the public Objects contract.
///
/// Semantic decisions are delegated to [`MemoryObjects`], the public reference state machine.
/// This adapter owns only durable mutation replay and immutable authenticated body chunks.
#[derive(Clone)]
pub struct LocalObjects {
    semantic: MemoryObjects,
    persistence: Arc<Persistence>,
    mutation: Arc<Mutex<()>>,
}

/// Exact bounded physical-reclamation result for a durable local Objects root.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalObjectsGarbageCollection {
    /// Body manifests examined.
    pub manifests_examined: u64,
    /// Unreferenced body manifests removed.
    pub manifests_removed: u64,
    /// Immutable body chunks examined.
    pub chunks_examined: u64,
    /// Chunks unreachable from every retained body removed.
    pub chunks_removed: u64,
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
}

#[derive(Clone, PartialEq, Message)]
struct UploadPartRecord {
    #[prost(message, optional, tag = "1")]
    header: Option<wire::UploadPartHeader>,
    #[prost(bytes = "vec", tag = "2")]
    body_digest: Vec<u8>,
    #[prost(uint64, tag = "3")]
    body_length: u64,
}

#[derive(Clone, PartialEq, Message)]
struct MutationRecord {
    #[prost(
        oneof = "mutation_record::Operation",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12"
    )]
    operation: Option<mutation_record::Operation>,
}

mod mutation_record {
    use super::{PutRecord, UploadPartRecord, wire};

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
    }
}

impl LocalObjects {
    /// Resolves a durable bucket by canonical name without appending a journal record.
    pub async fn bucket_named(&self, name: &str) -> Result<Option<wire::BucketRef>, ObjectsError> {
        self.semantic.bucket_named(name).await
    }

    /// Reclaims physical bodies not referenced by any live bucket, snapshot, or multipart upload.
    ///
    /// The provider's exclusive root ownership fences concurrent mutation. Every retained
    /// manifest and chunk is authenticated before any unreachable file is removed.
    pub async fn collect_garbage(
        &self,
        maximum_candidates: u64,
    ) -> Result<LocalObjectsGarbageCollection, LocalObjectsError> {
        if maximum_candidates == 0 {
            return Err(LocalObjectsError::Invalid(
                "garbage-collection candidate bound must be positive",
            ));
        }
        let _mutation = self.mutation.lock().await;
        let live_manifests = self.semantic.local_body_digests().await;
        let root = self.persistence.root.clone();
        tokio::task::spawn_blocking(move || {
            collect_physical_garbage(&root, &live_manifests, maximum_candidates)
        })
        .await
        .map_err(|_| LocalObjectsError::Corrupt)?
    }

    /// Opens or creates one exclusively owned durable provider and replays its valid prefix.
    ///
    /// A torn final journal frame is discarded. Corruption in a complete frame fails closed.
    /// Immutable bodies are chunked, authenticated, and range-read without whole-body loading.
    pub async fn open(
        root: impl AsRef<Path>,
        limits: LocalObjectsLimits,
    ) -> Result<Self, LocalObjectsError> {
        if limits.maximum_object_bytes == 0
            || limits.maximum_bytes == 0
            || limits.maximum_object_bytes > limits.maximum_bytes
            || limits.maximum_journal_operations == 0
            || limits.maximum_journal_bytes < JOURNAL_HEADER_BYTES
        {
            return Err(LocalObjectsError::Invalid("invalid capacity limits"));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("chunks"))?;
        fs::create_dir_all(root.join("manifests"))?;
        fs::create_dir_all(root.join("quarantine"))?;
        sync_parent(&root)?;

        let ownership = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("owner.lock"))?;
        ownership
            .try_lock_exclusive()
            .map_err(|_| LocalObjectsError::AlreadyOwned)?;

        let mut journal = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("mutations.log"))?;
        initialize_or_validate_header(&mut journal, limits)?;
        let semantic =
            MemoryObjects::new_with_limits(limits.maximum_object_bytes, limits.maximum_bytes)
                .map_err(|_| LocalObjectsError::Invalid("capacity limits are not representable"))?;
        let journal_operations = replay(&root, &semantic, &mut journal, limits).await?;
        for digest in semantic.local_body_digests().await {
            validate_live_body_manifest(&root, &digest)?;
        }
        journal.seek(SeekFrom::End(0))?;
        Ok(Self {
            semantic,
            persistence: Arc::new(Persistence {
                root,
                journal: StdMutex::new(journal),
                journal_operations: AtomicU64::new(journal_operations),
                limits,
                _ownership: ownership,
            }),
            mutation: Arc::new(Mutex::new(())),
        })
    }

    fn append(&self, operation: mutation_record::Operation) -> Result<(), ObjectsError> {
        let payload = MutationRecord {
            operation: Some(operation),
        }
        .encode_to_vec();
        if payload.len() > MAXIMUM_RECORD_BYTES {
            return Err(ObjectsError::Invalid("local mutation record is too large"));
        }
        let mut journal = self
            .persistence
            .journal
            .lock()
            .map_err(|_| ObjectsError::Unavailable)?;
        let frame_bytes = 36_u64
            .checked_add(payload.len() as u64)
            .ok_or(ObjectsError::Capacity)?;
        let operations = self.persistence.journal_operations.load(Ordering::Acquire);
        if operations >= self.persistence.limits.maximum_journal_operations
            || journal
                .metadata()
                .map_err(|_| ObjectsError::Unavailable)?
                .len()
                .checked_add(frame_bytes)
                .is_none_or(|bytes| bytes > self.persistence.limits.maximum_journal_bytes)
        {
            return Err(ObjectsError::Capacity);
        }
        append_frame(&mut journal, &payload).map_err(|_| ObjectsError::Unavailable)?;
        self.persistence
            .journal_operations
            .fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn persist_body(&self, body: &[u8]) -> Result<([u8; 32], PathBuf), ObjectsError> {
        persist_body(&self.persistence.root, body).map_err(|_| ObjectsError::Unavailable)
    }
}

#[async_trait]
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
        let _mutation = self.mutation.lock().await;
        let body_length = request.body.len();
        let (digest, _) = self.persist_body(&request.body)?;
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
        }))?;
        self.semantic
            .put_external(
                request,
                ExternalBody {
                    root: self.persistence.root.clone(),
                    digest,
                    length: body_length,
                },
            )
            .await
    }

    async fn get(&self, request: GetRequest) -> Result<BufferedObject, ObjectsError> {
        self.semantic.get(request).await
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
        let _mutation = self.mutation.lock().await;
        let body_length = body.len();
        let (digest, _) = self.persist_body(&body)?;
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

async fn replay(
    root: &Path,
    semantic: &MemoryObjects,
    journal: &mut File,
    limits: LocalObjectsLimits,
) -> Result<u64, LocalObjectsError> {
    journal.seek(SeekFrom::Start(JOURNAL_HEADER_BYTES))?;
    let mut operations = 0_u64;
    loop {
        let frame_start = journal.stream_position()?;
        let Some(payload) = read_frame(journal, frame_start)? else {
            break;
        };
        let record =
            MutationRecord::decode(payload.as_slice()).map_err(|_| LocalObjectsError::Corrupt)?;
        operations = operations
            .checked_add(1)
            .ok_or(LocalObjectsError::Corrupt)?;
        if operations > limits.maximum_journal_operations
            || journal.stream_position()? > limits.maximum_journal_bytes
        {
            return Err(LocalObjectsError::Corrupt);
        }
        replay_operation(root, semantic, required(record.operation)?).await?;
    }
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
            let _ = semantic
                .create_bucket(request.name, idempotency(request.mutation))
                .await;
        }
        mutation_record::Operation::DeleteBucket(request) => {
            let _ = semantic
                .delete_bucket(&required(request.bucket)?, idempotency(request.mutation))
                .await;
        }
        mutation_record::Operation::Put(record) => {
            let header = required(record.header)?;
            let digest = parse_digest(&record.body_digest)?;
            let length =
                usize::try_from(record.body_length).map_err(|_| LocalObjectsError::Corrupt)?;
            let bucket = required(header.bucket)?;
            let object_key = header.object_key;
            let _ = semantic
                .put_external(
                    PutRequest {
                        bucket: bucket.clone(),
                        object_key: object_key.clone(),
                        body: bytes::Bytes::new(),
                        metadata: required(header.metadata)?,
                        condition: condition(header.preconditions)?,
                        idempotency_key: idempotency(header.mutation),
                    },
                    ExternalBody {
                        root: root.to_path_buf(),
                        digest,
                        length,
                    },
                )
                .await;
        }
        mutation_record::Operation::Delete(request) => {
            let _ = semantic
                .delete(
                    required(request.bucket)?,
                    request.object_key,
                    (!request.version_id.is_empty()).then_some(request.version_id),
                    condition(request.preconditions)?,
                    idempotency(request.mutation),
                )
                .await;
        }
        mutation_record::Operation::Snapshot(request) => {
            let _ = semantic
                .snapshot(required(request.bucket)?, idempotency(request.mutation))
                .await;
        }
        mutation_record::Operation::DestroySnapshot(request) => {
            let _ = semantic
                .destroy_snapshot(required(request.snapshot)?, idempotency(request.mutation))
                .await;
        }
        mutation_record::Operation::ForkSnapshot(request) => {
            let _ = semantic
                .fork(
                    ReadTarget::Snapshot(required(request.snapshot)?),
                    request.destination_name,
                    idempotency(request.mutation),
                )
                .await;
        }
        mutation_record::Operation::ForkBucket(request) => {
            let _ = semantic
                .fork(
                    ReadTarget::Bucket(required(request.source)?),
                    request.destination_name,
                    idempotency(request.mutation),
                )
                .await;
        }
        mutation_record::Operation::CreateMultipart(request) => {
            let _ = semantic
                .create_multipart(
                    required(request.bucket)?,
                    request.object_key,
                    required(request.metadata)?,
                    condition(request.preconditions)?,
                    idempotency(request.mutation),
                )
                .await;
        }
        mutation_record::Operation::UploadPart(record) => {
            let header = required(record.header)?;
            let digest = parse_digest(&record.body_digest)?;
            let length =
                usize::try_from(record.body_length).map_err(|_| LocalObjectsError::Corrupt)?;
            let upload_id = header.upload_id;
            let _ = semantic
                .upload_part_external(
                    required(header.bucket)?,
                    header.object_key,
                    upload_id,
                    header.part_number,
                    ExternalBody {
                        root: root.to_path_buf(),
                        digest,
                        length,
                    },
                    idempotency(header.mutation),
                )
                .await;
        }
        mutation_record::Operation::CompleteMultipart(request) => {
            let bucket = required(request.bucket)?;
            let object_key = request.object_key;
            let _ = semantic
                .complete_multipart(
                    bucket,
                    object_key,
                    request.upload_id,
                    request.parts,
                    idempotency(request.mutation),
                )
                .await;
        }
        mutation_record::Operation::AbortMultipart(request) => {
            let _ = semantic
                .abort_multipart(
                    required(request.bucket)?,
                    request.object_key,
                    request.upload_id,
                    idempotency(request.mutation),
                )
                .await;
        }
    }
    Ok(())
}

fn initialize_or_validate_header(
    journal: &mut File,
    limits: LocalObjectsLimits,
) -> Result<(), LocalObjectsError> {
    let length = journal.metadata()?.len();
    if length == 0 {
        journal.write_all(JOURNAL_MAGIC)?;
        journal.write_all(&limits.maximum_object_bytes.to_le_bytes())?;
        journal.write_all(&limits.maximum_bytes.to_le_bytes())?;
        journal.write_all(&limits.maximum_journal_operations.to_le_bytes())?;
        journal.write_all(&limits.maximum_journal_bytes.to_le_bytes())?;
        journal.sync_all()?;
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

fn append_frame(journal: &mut File, payload: &[u8]) -> std::io::Result<()> {
    let length = u32::try_from(payload.len())
        .map_err(|_| std::io::Error::other("local mutation record is too large"))?;
    journal.write_all(&length.to_le_bytes())?;
    journal.write_all(blake3::hash(payload).as_bytes())?;
    journal.write_all(payload)?;
    journal.sync_data()
}

fn read_frame(journal: &mut File, frame_start: u64) -> Result<Option<Vec<u8>>, LocalObjectsError> {
    let mut length = [0; 4];
    let read = journal.read(&mut length)?;
    if read == 0 {
        return Ok(None);
    }
    if read != length.len() {
        journal.set_len(frame_start)?;
        journal.sync_data()?;
        return Ok(None);
    }
    let length = u32::from_le_bytes(length) as usize;
    if length == 0 || length > MAXIMUM_RECORD_BYTES {
        return Err(LocalObjectsError::Corrupt);
    }
    let mut checksum = [0; 32];
    let mut payload = vec![0; length];
    if journal.read_exact(&mut checksum).is_err() || journal.read_exact(&mut payload).is_err() {
        journal.set_len(frame_start)?;
        journal.sync_data()?;
        return Ok(None);
    }
    if blake3::hash(&payload).as_bytes() != &checksum {
        return Err(LocalObjectsError::Corrupt);
    }
    Ok(Some(payload))
}

fn persist_body(root: &Path, body: &[u8]) -> Result<([u8; 32], PathBuf), LocalObjectsError> {
    let digest = *blake3::hash(body).as_bytes();
    let mut manifest = Vec::new();
    manifest.extend_from_slice(MANIFEST_MAGIC);
    manifest.extend_from_slice(&(body.len() as u64).to_le_bytes());
    manifest.extend_from_slice(&digest);
    let chunk_count = body.len().div_ceil(CHUNK_BYTES);
    manifest.extend_from_slice(&(chunk_count as u32).to_le_bytes());
    for chunk in body.chunks(CHUNK_BYTES) {
        let chunk_digest = *blake3::hash(chunk).as_bytes();
        manifest.extend_from_slice(&chunk_digest);
        manifest.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
        persist_exact(root, "chunks", &chunk_digest, "chunk", chunk)?;
    }
    let checksum = blake3::hash(&manifest);
    manifest.extend_from_slice(checksum.as_bytes());
    let path = persist_exact(root, "manifests", &digest, "manifest", &manifest)?;
    Ok((digest, path))
}

fn persist_exact(
    root: &Path,
    family: &str,
    digest: &[u8; 32],
    extension: &str,
    bytes: &[u8],
) -> Result<PathBuf, LocalObjectsError> {
    let identity = hex(digest);
    let parent = root.join(family).join(&identity[..2]);
    fs::create_dir_all(&parent)?;
    let destination = parent.join(format!("{}.{}", &identity[2..], extension));
    if destination.exists() {
        let existing = fs::read(&destination)?;
        if existing == bytes {
            return Ok(destination);
        }
        quarantine(root, &destination, &identity)?;
    }
    let nonce = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(".{identity}.{}-{nonce}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    match fs::hard_link(&temporary, &destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::read(&destination)? != bytes {
                let _ = fs::remove_file(&temporary);
                return Err(LocalObjectsError::Corrupt);
            }
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            return Err(error.into());
        }
    }
    fs::remove_file(&temporary)?;
    sync_parent(&parent)?;
    Ok(destination)
}

fn collect_physical_garbage(
    root: &Path,
    live_manifests: &BTreeSet<[u8; 32]>,
    maximum_candidates: u64,
) -> Result<LocalObjectsGarbageCollection, LocalObjectsError> {
    let mut candidates = 0_u64;
    let mut temporary = Vec::new();
    let manifests = scan_immutable_family(
        root,
        "manifests",
        "manifest",
        maximum_candidates,
        &mut candidates,
        &mut temporary,
    )?;
    let chunks = scan_immutable_family(
        root,
        "chunks",
        "chunk",
        maximum_candidates,
        &mut candidates,
        &mut temporary,
    )?;
    let manifest_paths = manifests.iter().cloned().collect::<BTreeMap<_, _>>();
    let chunk_paths = chunks.iter().cloned().collect::<BTreeMap<_, _>>();
    let mut live_chunks = BTreeMap::new();
    for digest in live_manifests {
        let path = manifest_paths
            .get(digest)
            .ok_or(LocalObjectsError::Corrupt)?;
        let bytes = fs::read(path)?;
        let (_, expected_length) = local_body_identity(&bytes, digest)?;
        for (chunk, length) in parse_manifest(&bytes, digest, expected_length)
            .map_err(|_| LocalObjectsError::Corrupt)?
        {
            if let Some(previous) = live_chunks.insert(chunk, length)
                && previous != length
            {
                return Err(LocalObjectsError::Corrupt);
            }
        }
    }
    for (digest, expected_length) in &live_chunks {
        let path = chunk_paths.get(digest).ok_or(LocalObjectsError::Corrupt)?;
        let bytes = fs::read(path)?;
        if bytes.len() != *expected_length || blake3::hash(&bytes).as_bytes() != digest {
            return Err(LocalObjectsError::Corrupt);
        }
    }
    let mut report = LocalObjectsGarbageCollection {
        manifests_examined: u64::try_from(manifests.len()).unwrap_or(u64::MAX),
        chunks_examined: u64::try_from(chunks.len()).unwrap_or(u64::MAX),
        ..LocalObjectsGarbageCollection::default()
    };
    let mut changed_parents = BTreeSet::new();
    for (digest, path) in manifests {
        if !live_manifests.contains(&digest) {
            fs::remove_file(&path)?;
            report.manifests_removed = report.manifests_removed.saturating_add(1);
            if let Some(parent) = path.parent() {
                changed_parents.insert(parent.to_path_buf());
            }
        }
    }
    for (digest, path) in chunks {
        if !live_chunks.contains_key(&digest) {
            fs::remove_file(&path)?;
            report.chunks_removed = report.chunks_removed.saturating_add(1);
            if let Some(parent) = path.parent() {
                changed_parents.insert(parent.to_path_buf());
            }
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
        sync_parent(&parent)?;
    }
    Ok(report)
}

fn scan_immutable_family(
    root: &Path,
    family: &str,
    extension: &str,
    maximum_candidates: u64,
    candidates: &mut u64,
    temporary: &mut Vec<PathBuf>,
) -> Result<Vec<([u8; 32], PathBuf)>, LocalObjectsError> {
    let base = root.join(family);
    let mut prefixes = fs::read_dir(&base)?.collect::<Result<Vec<_>, _>>()?;
    prefixes.sort_by_key(std::fs::DirEntry::file_name);
    let mut files = Vec::new();
    for prefix in prefixes {
        if !prefix.file_type()?.is_dir() {
            return Err(LocalObjectsError::Corrupt);
        }
        let prefix_name = prefix.file_name();
        let prefix_name = prefix_name.to_str().ok_or(LocalObjectsError::Corrupt)?;
        if prefix_name.len() != 2 || !prefix_name.bytes().all(is_lower_hex) {
            return Err(LocalObjectsError::Corrupt);
        }
        let mut entries = fs::read_dir(prefix.path())?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
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
            let suffix = format!(".{extension}");
            let tail = name
                .strip_suffix(&suffix)
                .ok_or(LocalObjectsError::Corrupt)?;
            if tail.len() != 62 || !tail.bytes().all(is_lower_hex) {
                return Err(LocalObjectsError::Corrupt);
            }
            let mut digest = [0_u8; 32];
            decode_lower_hex(&format!("{prefix_name}{tail}"), &mut digest)?;
            files.push((digest, entry.path()));
        }
    }
    Ok(files)
}

fn local_body_identity(
    manifest: &[u8],
    expected_digest: &[u8; 32],
) -> Result<([u8; 32], usize), LocalObjectsError> {
    let minimum = MANIFEST_MAGIC.len() + 8 + 32 + 4 + 32;
    if manifest.len() < minimum {
        return Err(LocalObjectsError::Corrupt);
    }
    let mut cursor = MANIFEST_MAGIC.len();
    let length =
        usize::try_from(read_u64(manifest, &mut cursor).map_err(|_| LocalObjectsError::Corrupt)?)
            .map_err(|_| LocalObjectsError::Corrupt)?;
    let digest = read_array::<32>(manifest, &mut cursor).map_err(|_| LocalObjectsError::Corrupt)?;
    if &digest != expected_digest {
        return Err(LocalObjectsError::Corrupt);
    }
    Ok((digest, length))
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn decode_lower_hex(encoded: &str, output: &mut [u8]) -> Result<(), LocalObjectsError> {
    if encoded.len() != output.len().saturating_mul(2) {
        return Err(LocalObjectsError::Corrupt);
    }
    for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
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

fn quarantine(root: &Path, source: &Path, identity: &str) -> Result<(), LocalObjectsError> {
    let destination = root
        .join("quarantine")
        .join(format!("{identity}-{}.corrupt", std::process::id()));
    fs::rename(source, destination)?;
    sync_parent(&root.join("quarantine"))
}

pub(crate) fn read_body(
    root: &Path,
    expected_digest: &[u8; 32],
    expected_length: usize,
    start: usize,
    end: usize,
) -> Result<bytes::Bytes, ObjectsError> {
    if start > end || end > expected_length {
        return Err(ObjectsError::Invalid("invalid range"));
    }
    let identity = hex(expected_digest);
    let manifest_path = root
        .join("manifests")
        .join(&identity[..2])
        .join(format!("{}.manifest", &identity[2..]));
    let manifest = fs::read(manifest_path).map_err(|_| ObjectsError::Unavailable)?;
    let entries = parse_manifest(&manifest, expected_digest, expected_length)?;
    let mut output = Vec::with_capacity(end.saturating_sub(start));
    let mut offset = 0usize;
    for (digest, length) in entries {
        let chunk_end = offset
            .checked_add(length)
            .ok_or(ObjectsError::Unavailable)?;
        if chunk_end > start && offset < end {
            let chunk_identity = hex(&digest);
            let path = root
                .join("chunks")
                .join(&chunk_identity[..2])
                .join(format!("{}.chunk", &chunk_identity[2..]));
            let chunk = fs::read(path).map_err(|_| ObjectsError::Unavailable)?;
            if chunk.len() != length || blake3::hash(&chunk).as_bytes() != &digest {
                return Err(ObjectsError::Unavailable);
            }
            let selected_start = start.saturating_sub(offset).min(length);
            let selected_end = end.saturating_sub(offset).min(length);
            output.extend_from_slice(&chunk[selected_start..selected_end]);
        }
        offset = chunk_end;
    }
    if offset != expected_length || output.len() != end.saturating_sub(start) {
        return Err(ObjectsError::Unavailable);
    }
    if start == 0 && end == expected_length && blake3::hash(&output).as_bytes() != expected_digest {
        return Err(ObjectsError::Unavailable);
    }
    Ok(output.into())
}

pub(crate) fn hash_body(
    root: &Path,
    expected_digest: &[u8; 32],
    expected_length: usize,
    hasher: &mut blake3::Hasher,
) -> Result<(), ObjectsError> {
    let identity = hex(expected_digest);
    let manifest_path = root
        .join("manifests")
        .join(&identity[..2])
        .join(format!("{}.manifest", &identity[2..]));
    let manifest = fs::read(manifest_path).map_err(|_| ObjectsError::Unavailable)?;
    let entries = parse_manifest(&manifest, expected_digest, expected_length)?;
    let mut observed = 0usize;
    for (digest, length) in entries {
        let chunk_identity = hex(&digest);
        let path = root
            .join("chunks")
            .join(&chunk_identity[..2])
            .join(format!("{}.chunk", &chunk_identity[2..]));
        let chunk = fs::read(path).map_err(|_| ObjectsError::Unavailable)?;
        if chunk.len() != length || blake3::hash(&chunk).as_bytes() != &digest {
            return Err(ObjectsError::Unavailable);
        }
        observed = observed
            .checked_add(length)
            .ok_or(ObjectsError::Unavailable)?;
        hasher.update(&chunk);
    }
    if observed != expected_length {
        return Err(ObjectsError::Unavailable);
    }
    Ok(())
}

fn validate_live_body_manifest(
    root: &Path,
    expected_digest: &[u8; 32],
) -> Result<(), LocalObjectsError> {
    let identity = hex(expected_digest);
    let path = root
        .join("manifests")
        .join(&identity[..2])
        .join(format!("{}.manifest", &identity[2..]));
    let bytes = fs::read(path)?;
    let (_, expected_length) = local_body_identity(&bytes, expected_digest)?;
    parse_manifest(&bytes, expected_digest, expected_length)
        .map(|_| ())
        .map_err(|_| LocalObjectsError::Corrupt)
}

fn parse_manifest(
    bytes: &[u8],
    expected_digest: &[u8; 32],
    expected_length: usize,
) -> Result<Vec<([u8; 32], usize)>, ObjectsError> {
    let fixed = MANIFEST_MAGIC.len() + 8 + 32 + 4 + 32;
    if bytes.len() < fixed {
        return Err(ObjectsError::Unavailable);
    }
    let (payload, checksum) = bytes.split_at(bytes.len() - 32);
    if blake3::hash(payload).as_bytes() != checksum {
        return Err(ObjectsError::Unavailable);
    }
    let mut cursor = MANIFEST_MAGIC.len();
    if &payload[..cursor] != MANIFEST_MAGIC {
        return Err(ObjectsError::Unavailable);
    }
    let length = read_u64(payload, &mut cursor)?;
    let digest = read_array::<32>(payload, &mut cursor)?;
    let count = read_u32(payload, &mut cursor)? as usize;
    if length != expected_length as u64 || digest != *expected_digest {
        return Err(ObjectsError::Unavailable);
    }
    let expected_payload = cursor
        .checked_add(count.checked_mul(36).ok_or(ObjectsError::Unavailable)?)
        .ok_or(ObjectsError::Unavailable)?;
    if expected_payload != payload.len() || count != expected_length.div_ceil(CHUNK_BYTES) {
        return Err(ObjectsError::Unavailable);
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let digest = read_array::<32>(payload, &mut cursor)?;
        let length = read_u32(payload, &mut cursor)? as usize;
        if length == 0 || length > CHUNK_BYTES {
            return Err(ObjectsError::Unavailable);
        }
        entries.push((digest, length));
    }
    Ok(entries)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, ObjectsError> {
    Ok(u64::from_le_bytes(read_array(bytes, cursor)?))
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, ObjectsError> {
    Ok(u32::from_le_bytes(read_array(bytes, cursor)?))
}

fn read_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N], ObjectsError> {
    let end = cursor.checked_add(N).ok_or(ObjectsError::Unavailable)?;
    let selected = bytes.get(*cursor..end).ok_or(ObjectsError::Unavailable)?;
    let value = selected.try_into().map_err(|_| ObjectsError::Unavailable)?;
    *cursor = end;
    Ok(value)
}

fn parse_digest(value: &[u8]) -> Result<[u8; 32], LocalObjectsError> {
    value.try_into().map_err(|_| LocalObjectsError::Corrupt)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[(byte >> 4) as usize]));
        output.push(char::from(DIGITS[(byte & 0x0f) as usize]));
    }
    output
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), LocalObjectsError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
fn sync_parent(_path: &Path) -> Result<(), LocalObjectsError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let body = bytes::Bytes::from(vec![7; CHUNK_BYTES + 19]);
        provider
            .put(PutRequest {
                bucket: bucket.clone(),
                object_key: "large".into(),
                body,
                metadata: wire::ObjectMetadata::default(),
                condition: Some(Condition::IfAbsent),
                idempotency_key: Some("put".into()),
            })
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(
            provider
                .get(GetRequest {
                    target: ReadTarget::Bucket(bucket),
                    object_key: "large".into(),
                    version_id: None,
                    range: Some((CHUNK_BYTES as u64 - 4, Some(CHUNK_BYTES as u64 + 3))),
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
    async fn physical_gc_is_bounded_and_preserves_every_referenced_chunk() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let provider = LocalObjects::open(root.path(), LocalObjectsLimits::default())
            .await
            .unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("gc-bucket".into(), Some("create-gc".into()))
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
                condition: Some(Condition::IfAbsent),
                idempotency_key: Some("put-live".into()),
            })
            .await
            .unwrap_or_else(|_| unreachable!());
        let (orphan, _) =
            persist_body(root.path(), b"orphan body").unwrap_or_else(|_| unreachable!());
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
        assert_eq!(report.manifests_removed, 1);
        assert_eq!(report.chunks_removed, 1);
        let orphan_identity = hex(&orphan);
        assert!(
            !root
                .path()
                .join("manifests")
                .join(&orphan_identity[..2])
                .join(format!("{}.manifest", &orphan_identity[2..]))
                .exists()
        );
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
}
