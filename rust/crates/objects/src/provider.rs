//! Transport-neutral provider contract and deterministic process-local reference implementation.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[cfg(feature = "local")]
use std::path::PathBuf;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{limits, wire};

const MEMORY_BYTES: usize = 64 * 1_024 * 1_024;
const SYSTEM_METADATA_BYTES: usize = 2 * 1_024;

/// Exactly one current-version mutation condition shared by every provider transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Condition {
    /// Require no live current version.
    IfAbsent,
    /// Require the current opaque `ETag`.
    IfMatch(String),
    /// Require the current opaque version identity.
    IfVersion(String),
}

#[cfg(any(feature = "grpc", feature = "local"))]
impl Condition {
    pub(crate) fn wire(self) -> wire::Preconditions {
        let condition = match self {
            Self::IfAbsent => wire::preconditions::Condition::IfAbsent(true),
            Self::IfMatch(etag) => wire::preconditions::Condition::IfMatch(etag),
            Self::IfVersion(version_id) => wire::preconditions::Condition::IfVersion(version_id),
        };
        wire::Preconditions {
            condition: Some(condition),
        }
    }
}

/// A bucket or immutable snapshot selected for a read.
#[derive(Clone, Debug, PartialEq)]
pub enum ReadTarget {
    /// Read the current state of this bucket identity.
    Bucket(wire::BucketRef),
    /// Read the captured state of this snapshot identity.
    Snapshot(wire::SnapshotRef),
}

/// One transport-independent put request.
#[derive(Clone, Debug)]
pub struct PutRequest {
    /// Exact bucket identity and display name.
    pub bucket: wire::BucketRef,
    /// UTF-8 object key.
    pub object_key: String,
    /// Complete body bytes.
    pub body: bytes::Bytes,
    /// Immutable metadata for the new version.
    pub metadata: wire::ObjectMetadata,
    /// Optional current-version precondition.
    pub condition: Option<Condition>,
    /// Optional original-request idempotency key.
    pub idempotency_key: Option<String>,
}

/// One transport-independent object read.
#[derive(Clone, Debug)]
pub struct GetRequest {
    /// Current bucket or immutable snapshot.
    pub target: ReadTarget,
    /// UTF-8 object key.
    pub object_key: String,
    /// Exact retained version, or the visible current version when absent.
    pub version_id: Option<String>,
    /// One inclusive byte range, or the complete body when absent.
    pub range: Option<(u64, Option<u64>)>,
    /// Optional representation validator that must match.
    pub if_match: Option<String>,
    /// Optional representation validator that must not match.
    pub if_none_match: Option<String>,
    /// Maximum selected body bytes the provider may allocate or return.
    pub maximum_bytes: u64,
}

/// One immutable descriptor and its selected body bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct BufferedObject {
    /// Immutable version descriptor.
    pub version: wire::ObjectVersion,
    /// Complete body or requested range.
    pub body: bytes::Bytes,
}

/// Result of an unqualified marker creation or exact-version deletion.
#[derive(Clone, Debug, PartialEq)]
pub struct DeleteResult {
    /// Whether an exact addressed version existed. Marker creation is always `true`.
    pub existed: bool,
    /// Newly created marker for an unqualified delete.
    pub marker: Option<wire::ObjectVersion>,
}

/// One stable page over a captured listing view.
#[derive(Clone, Debug, PartialEq)]
pub struct ProviderListPage {
    /// Object/version entries in contract order.
    pub entries: Vec<wire::ListEntry>,
    /// Delimiter-grouped prefixes in lexical order.
    pub common_prefixes: Vec<String>,
    /// Opaque continuation for the same immutable view.
    pub continuation: Option<String>,
}

/// Provider-visible semantic failure. Transport errors remain adapter-specific.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ObjectsError {
    /// Input violates the public contract.
    #[error("invalid objects request: {0}")]
    Invalid(&'static str),
    /// The addressed bucket, key, version, or snapshot does not exist.
    #[error("objects resource not found")]
    NotFound,
    /// A bucket name or fork destination already exists.
    #[error("objects resource already exists")]
    AlreadyExists,
    /// A current-version condition did not hold and no mutation occurred.
    #[error("objects precondition failed")]
    PreconditionFailed,
    /// An idempotency key was reused with different arguments.
    #[error("objects idempotency mismatch")]
    IdempotencyMismatch,
    /// A fixed or provider-specific bound was exhausted.
    #[error("objects capacity exhausted")]
    Capacity,
    /// The provider does not implement the requested capability.
    #[error("objects capability is unsupported")]
    Unsupported,
    /// Authentication or authorization was rejected.
    #[error("objects provider rejected authorization")]
    Unauthorized,
    /// The provider could not be reached or returned an invalid response.
    #[error("objects provider is unavailable")]
    Unavailable,
}

/// Public semantic provider implemented by memory, customer-hosted, and Acyclic adapters.
#[async_trait]
pub trait ObjectsProvider: Send + Sync {
    /// Create one permanently versioned bucket.
    async fn create_bucket(
        &self,
        name: String,
        idempotency_key: Option<String>,
    ) -> Result<wire::Bucket, ObjectsError>;
    /// Resolve one exact bucket identity.
    async fn head_bucket(&self, bucket: &wire::BucketRef) -> Result<wire::Bucket, ObjectsError>;
    /// Delete one empty exact bucket identity.
    async fn delete_bucket(
        &self,
        bucket: &wire::BucketRef,
        idempotency_key: Option<String>,
    ) -> Result<bool, ObjectsError>;
    /// Create one immutable version.
    async fn put(&self, request: PutRequest) -> Result<wire::ObjectVersion, ObjectsError>;
    /// Read one immutable version or its visible current selection.
    async fn get(&self, request: GetRequest) -> Result<BufferedObject, ObjectsError>;
    /// Create a delete marker or delete one exact version.
    async fn delete(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        version_id: Option<String>,
        condition: Option<Condition>,
        idempotency_key: Option<String>,
    ) -> Result<DeleteResult, ObjectsError>;
    /// Capture or continue one stable listing.
    async fn list(
        &self,
        target: ReadTarget,
        prefix: String,
        delimiter: Option<String>,
        versions: bool,
        page_size: u32,
        continuation: Option<String>,
    ) -> Result<ProviderListPage, ObjectsError>;
    /// Atomically capture an immutable whole-bucket snapshot.
    async fn snapshot(
        &self,
        bucket: wire::BucketRef,
        idempotency_key: Option<String>,
    ) -> Result<wire::Snapshot, ObjectsError>;
    /// Destroy one exact snapshot identity.
    async fn destroy_snapshot(
        &self,
        snapshot: wire::SnapshotRef,
        idempotency_key: Option<String>,
    ) -> Result<bool, ObjectsError>;
    /// Atomically fork a whole bucket or snapshot into an independent bucket.
    async fn fork(
        &self,
        source: ReadTarget,
        destination_name: String,
        idempotency_key: Option<String>,
    ) -> Result<wire::Bucket, ObjectsError>;
    /// Create one multipart upload without publishing a version.
    async fn create_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        metadata: wire::ObjectMetadata,
        condition: Option<Condition>,
        idempotency_key: Option<String>,
    ) -> Result<wire::MultipartUpload, ObjectsError>;
    /// Upload or replace one exact staged part.
    async fn upload_part(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        part_number: u32,
        body: bytes::Bytes,
        idempotency_key: Option<String>,
    ) -> Result<wire::UploadedPart, ObjectsError>;
    /// List staged parts in ascending part-number order.
    async fn list_parts(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
    ) -> Result<Vec<wire::UploadedPart>, ObjectsError>;
    /// Atomically publish a version from exact staged part receipts.
    async fn complete_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        parts: Vec<wire::UploadedPart>,
        idempotency_key: Option<String>,
    ) -> Result<wire::ObjectVersion, ObjectsError>;
    /// Abort one exact staged upload.
    async fn abort_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        idempotency_key: Option<String>,
    ) -> Result<bool, ObjectsError>;
}

#[derive(Clone)]
enum StoredBody {
    Memory(bytes::Bytes),
    Composite {
        parts: Arc<[StoredBody]>,
        length: usize,
    },
    #[cfg(feature = "local")]
    Local {
        root: Arc<PathBuf>,
        digest: [u8; 32],
        length: usize,
    },
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
enum BodyIdentity {
    Memory {
        address: usize,
        length: usize,
    },
    #[cfg(feature = "local")]
    Local([u8; 32]),
}

impl StoredBody {
    fn memory(body: bytes::Bytes) -> Self {
        Self::Memory(body)
    }

    fn len(&self) -> usize {
        match self {
            Self::Memory(body) => body.len(),
            Self::Composite { length, .. } => *length,
            #[cfg(feature = "local")]
            Self::Local { length, .. } => *length,
        }
    }

    fn read(&self, start: usize, end: usize) -> Result<bytes::Bytes, ObjectsError> {
        match self {
            Self::Memory(body) => Ok(body.slice(start..end)),
            Self::Composite { parts, .. } => {
                let mut output = Vec::with_capacity(end.saturating_sub(start));
                let mut offset = 0usize;
                for part in parts.iter() {
                    let part_end = offset
                        .checked_add(part.len())
                        .ok_or(ObjectsError::Unavailable)?;
                    if part_end > start && offset < end {
                        let selected_start = start.saturating_sub(offset).min(part.len());
                        let selected_end = end.saturating_sub(offset).min(part.len());
                        output.extend_from_slice(&part.read(selected_start, selected_end)?);
                    }
                    offset = part_end;
                }
                if output.len() != end.saturating_sub(start) {
                    return Err(ObjectsError::Unavailable);
                }
                Ok(output.into())
            }
            #[cfg(feature = "local")]
            Self::Local {
                root,
                digest,
                length,
            } => {
                if end > *length || start > end {
                    return Err(ObjectsError::Invalid("invalid range"));
                }
                crate::local::read_body(root, digest, *length, start, end)
            }
        }
    }

    fn update_hash(&self, hasher: &mut blake3::Hasher) -> Result<(), ObjectsError> {
        match self {
            Self::Memory(body) => {
                hasher.update(body);
                Ok(())
            }
            Self::Composite { parts, .. } => {
                for part in parts.iter() {
                    part.update_hash(hasher)?;
                }
                Ok(())
            }
            #[cfg(feature = "local")]
            Self::Local {
                root,
                digest,
                length,
            } => crate::local::hash_body(root, digest, *length, hasher),
        }
    }

    fn leaves(&self, output: &mut Vec<(BodyIdentity, usize)>) {
        match self {
            Self::Memory(body) => output.push((
                BodyIdentity::Memory {
                    address: body.as_ptr() as usize,
                    length: body.len(),
                },
                body.len(),
            )),
            Self::Composite { parts, .. } => {
                for part in parts.iter() {
                    part.leaves(output);
                }
            }
            #[cfg(feature = "local")]
            Self::Local { digest, length, .. } => {
                output.push((BodyIdentity::Local(*digest), *length));
            }
        }
    }

    async fn read_async(&self, start: usize, end: usize) -> Result<bytes::Bytes, ObjectsError> {
        #[cfg(feature = "local")]
        if self.has_local_storage() {
            let body = self.clone();
            return tokio::task::spawn_blocking(move || body.read(start, end))
                .await
                .map_err(|_| ObjectsError::Unavailable)?;
        }
        self.read(start, end)
    }

    #[cfg(feature = "local")]
    fn has_local_storage(&self) -> bool {
        match self {
            Self::Memory(_) => false,
            Self::Local { .. } => true,
            Self::Composite { parts, .. } => parts.iter().any(Self::has_local_storage),
        }
    }
}

#[derive(Clone)]
struct Version {
    descriptor: wire::ObjectVersion,
    body: Option<StoredBody>,
}

#[derive(Clone)]
struct BucketState {
    reference: wire::BucketRef,
    created_at: prost_types::Timestamp,
    objects: BTreeMap<String, Vec<Version>>,
}

#[derive(Clone)]
struct SnapshotState {
    reference: wire::SnapshotRef,
    bucket: BucketState,
}

struct MultipartState {
    bucket: wire::BucketRef,
    object_key: String,
    metadata: wire::ObjectMetadata,
    condition: Option<Condition>,
    parts: BTreeMap<u32, (wire::UploadedPart, StoredBody)>,
}

struct StoredPartRequest {
    bucket: wire::BucketRef,
    object_key: String,
    upload_id: String,
    part_number: u32,
    body: StoredBody,
    body_digest: [u8; 32],
    idempotency_key: Option<String>,
}

#[cfg(feature = "local")]
pub(crate) struct ExternalBody {
    pub(crate) root: PathBuf,
    pub(crate) digest: [u8; 32],
    pub(crate) length: usize,
}

#[derive(Clone)]
enum ListingItem {
    Entry(Box<wire::ListEntry>),
    Prefix(String),
}

struct ListingView {
    binding: String,
    items: Vec<ListingItem>,
}

#[derive(Clone)]
enum MutationOutcome {
    Bucket(Result<wire::Bucket, ObjectsError>),
    Boolean(Result<bool, ObjectsError>),
    Version(Result<wire::ObjectVersion, ObjectsError>),
    Delete(Result<DeleteResult, ObjectsError>),
    Snapshot(Result<wire::Snapshot, ObjectsError>),
    Multipart(Result<wire::MultipartUpload, ObjectsError>),
    Part(Result<wire::UploadedPart, ObjectsError>),
}

struct IdempotencyRecord {
    fingerprint: blake3::Hash,
    outcome: MutationOutcome,
}

struct State {
    maximum_bytes: usize,
    maximum_object_bytes: usize,
    sequence: u64,
    listing_sequence: u64,
    bytes: usize,
    body_references: BTreeMap<BodyIdentity, (usize, usize)>,
    names: BTreeMap<String, String>,
    buckets: BTreeMap<String, BucketState>,
    snapshots: BTreeMap<String, SnapshotState>,
    multiparts: BTreeMap<String, MultipartState>,
    listings: BTreeMap<String, ListingView>,
    idempotency: BTreeMap<String, IdempotencyRecord>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            maximum_bytes: MEMORY_BYTES,
            maximum_object_bytes: MEMORY_BYTES,
            sequence: 0,
            listing_sequence: 0,
            bytes: 0,
            body_references: BTreeMap::new(),
            names: BTreeMap::new(),
            buckets: BTreeMap::new(),
            snapshots: BTreeMap::new(),
            multiparts: BTreeMap::new(),
            listings: BTreeMap::new(),
            idempotency: BTreeMap::new(),
        }
    }
}

impl State {
    fn additional_body_bytes(&self, body: &StoredBody) -> Result<usize, ObjectsError> {
        let mut leaves = Vec::new();
        body.leaves(&mut leaves);
        leaves
            .into_iter()
            .try_fold(0usize, |total, (identity, length)| {
                if self.body_references.contains_key(&identity) {
                    Ok(total)
                } else {
                    total.checked_add(length).ok_or(ObjectsError::Capacity)
                }
            })
    }

    fn retain_body(&mut self, body: &StoredBody) -> Result<(), ObjectsError> {
        let mut leaves = Vec::new();
        body.leaves(&mut leaves);
        for (identity, length) in leaves {
            match self.body_references.entry(identity) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    self.bytes = self
                        .bytes
                        .checked_add(length)
                        .ok_or(ObjectsError::Capacity)?;
                    entry.insert((length, 1));
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if entry.get().0 != length {
                        return Err(ObjectsError::Unavailable);
                    }
                    entry.get_mut().1 =
                        entry.get().1.checked_add(1).ok_or(ObjectsError::Capacity)?;
                }
            }
        }
        Ok(())
    }

    fn release_body(&mut self, body: &StoredBody) -> Result<(), ObjectsError> {
        let mut leaves = Vec::new();
        body.leaves(&mut leaves);
        for (identity, length) in leaves {
            let references = self
                .body_references
                .get_mut(&identity)
                .ok_or(ObjectsError::Unavailable)?;
            if references.0 != length || references.1 == 0 {
                return Err(ObjectsError::Unavailable);
            }
            references.1 -= 1;
            if references.1 == 0 {
                self.body_references.remove(&identity);
                self.bytes = self
                    .bytes
                    .checked_sub(length)
                    .ok_or(ObjectsError::Unavailable)?;
            }
        }
        Ok(())
    }

    fn retain_bucket(&mut self, bucket: &BucketState) -> Result<(), ObjectsError> {
        for body in bucket
            .objects
            .values()
            .flatten()
            .filter_map(|version| version.body.as_ref())
        {
            self.retain_body(body)?;
        }
        Ok(())
    }

    fn release_bucket(&mut self, bucket: &BucketState) -> Result<(), ObjectsError> {
        for body in bucket
            .objects
            .values()
            .flatten()
            .filter_map(|version| version.body.as_ref())
        {
            self.release_body(body)?;
        }
        Ok(())
    }
}

/// Bounded deterministic process-local reference provider.
///
/// Acknowledged state is ephemeral and has no isolation, durability, distribution, or production
/// availability claim.
#[derive(Clone, Default)]
pub struct MemoryObjects {
    state: Arc<Mutex<State>>,
}

impl MemoryObjects {
    #[cfg(feature = "local")]
    pub(crate) async fn local_body_digests(&self) -> BTreeSet<[u8; 32]> {
        let state = self.state.lock().await;
        state
            .body_references
            .keys()
            .filter_map(|identity| match identity {
                BodyIdentity::Local(digest) => Some(*digest),
                BodyIdentity::Memory { .. } => None,
            })
            .collect()
    }

    /// Resolves a bucket by its canonical name without mutating provider state.
    pub async fn bucket_named(&self, name: &str) -> Result<Option<wire::BucketRef>, ObjectsError> {
        Self::validate_bucket_name(name)?;
        let state = self.state.lock().await;
        let Some(bucket_id) = state.names.get(name) else {
            return Ok(None);
        };
        let bucket = state
            .buckets
            .get(bucket_id)
            .ok_or(ObjectsError::Unavailable)?;
        Ok(Some(bucket.reference.clone()))
    }

    /// Creates the default bounded reference provider with one deterministic bucket.
    #[must_use]
    pub fn with_default_bucket() -> (Self, wire::BucketRef) {
        Self::from_bucket("memory-bucket".to_owned(), MEMORY_BYTES)
    }

    /// Creates an empty reference provider with an explicit aggregate byte ceiling.
    ///
    /// # Errors
    ///
    /// Rejects zero or process-unrepresentable limits before allocating provider state.
    pub fn new(maximum_bytes: u64) -> Result<Self, ObjectsError> {
        if maximum_bytes == 0 {
            return Err(ObjectsError::Invalid("memory byte limit must be positive"));
        }
        Self::new_with_limits(maximum_bytes, maximum_bytes)
    }

    pub(crate) fn new_with_limits(
        maximum_object_bytes: u64,
        maximum_bytes: u64,
    ) -> Result<Self, ObjectsError> {
        let maximum_object_bytes = usize::try_from(maximum_object_bytes)
            .map_err(|_| ObjectsError::Invalid("memory object limit is not representable"))?;
        let maximum_bytes = usize::try_from(maximum_bytes)
            .map_err(|_| ObjectsError::Invalid("memory byte limit is not representable"))?;
        if maximum_object_bytes == 0 || maximum_bytes == 0 || maximum_object_bytes > maximum_bytes {
            return Err(ObjectsError::Invalid("invalid memory object limits"));
        }
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                maximum_bytes,
                maximum_object_bytes,
                ..State::default()
            })),
        })
    }

    /// Creates a bounded provider with one deterministic bucket already admitted.
    ///
    /// This synchronous constructor lets another in-process SDK family compose the exact public
    /// reference provider without running an executor merely to establish its private bucket.
    ///
    /// # Errors
    ///
    /// Rejects an invalid bucket name or memory ceiling.
    pub fn with_bucket(
        name: impl Into<String>,
        maximum_bytes: u64,
    ) -> Result<(Self, wire::BucketRef), ObjectsError> {
        let maximum_bytes = usize::try_from(maximum_bytes)
            .map_err(|_| ObjectsError::Invalid("memory byte limit is not representable"))?;
        if maximum_bytes == 0 {
            return Err(ObjectsError::Invalid("memory byte limit must be positive"));
        }
        let name = name.into();
        Self::validate_bucket_name(&name)?;
        Ok(Self::from_bucket(name, maximum_bytes))
    }

    /// Creates one deterministic bucket with independent per-object and aggregate bounds.
    ///
    /// # Errors
    ///
    /// Rejects an invalid bucket name, zero limits, a per-object limit larger than the aggregate
    /// limit, or limits that cannot be represented by this process.
    pub fn with_bucket_limits(
        name: impl Into<String>,
        maximum_object_bytes: u64,
        maximum_bytes: u64,
    ) -> Result<(Self, wire::BucketRef), ObjectsError> {
        let maximum_object_bytes = usize::try_from(maximum_object_bytes)
            .map_err(|_| ObjectsError::Invalid("memory object limit is not representable"))?;
        let maximum_bytes = usize::try_from(maximum_bytes)
            .map_err(|_| ObjectsError::Invalid("memory byte limit is not representable"))?;
        if maximum_object_bytes == 0 || maximum_bytes == 0 || maximum_object_bytes > maximum_bytes {
            return Err(ObjectsError::Invalid("invalid memory object limits"));
        }
        let name = name.into();
        Self::validate_bucket_name(&name)?;
        Ok(Self::from_bucket_limits(
            name,
            maximum_object_bytes,
            maximum_bytes,
        ))
    }

    fn from_bucket(name: String, maximum_bytes: usize) -> (Self, wire::BucketRef) {
        Self::from_bucket_limits(name, maximum_bytes, maximum_bytes)
    }

    fn from_bucket_limits(
        name: String,
        maximum_object_bytes: usize,
        maximum_bytes: usize,
    ) -> (Self, wire::BucketRef) {
        let bucket_id = "bucket-0000000000000001".to_owned();
        let reference = wire::BucketRef {
            bucket_id: bucket_id.clone(),
            name: name.clone(),
        };
        let mut state = State {
            maximum_bytes,
            maximum_object_bytes,
            sequence: 1,
            ..State::default()
        };
        state.names.insert(name, bucket_id.clone());
        state.buckets.insert(
            bucket_id,
            BucketState {
                reference: reference.clone(),
                created_at: Self::timestamp(&state),
                objects: BTreeMap::new(),
            },
        );
        (
            Self {
                state: Arc::new(Mutex::new(state)),
            },
            reference,
        )
    }

    fn fingerprint(kind: &str, fields: &[&[u8]]) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        for field in std::iter::once(kind.as_bytes()).chain(fields.iter().copied()) {
            hasher.update(&(field.len() as u64).to_le_bytes());
            hasher.update(field);
        }
        hasher.finalize()
    }

    fn execute<T: Clone>(
        state: &mut State,
        idempotency_key: Option<&str>,
        fingerprint: blake3::Hash,
        unpack: impl FnOnce(&MutationOutcome) -> Option<Result<T, ObjectsError>>,
        pack: impl FnOnce(Result<T, ObjectsError>) -> MutationOutcome,
        operation: impl FnOnce(&mut State) -> Result<T, ObjectsError>,
    ) -> Result<T, ObjectsError> {
        let Some(idempotency_key) = idempotency_key else {
            return operation(state);
        };
        if idempotency_key.is_empty() || idempotency_key.len() > 256 {
            return Err(ObjectsError::Invalid("invalid idempotency key"));
        }
        if let Some(record) = state.idempotency.get(idempotency_key) {
            if record.fingerprint != fingerprint {
                return Err(ObjectsError::IdempotencyMismatch);
            }
            return unpack(&record.outcome).ok_or(ObjectsError::IdempotencyMismatch)?;
        }
        let outcome = operation(state);
        state.idempotency.insert(
            idempotency_key.to_owned(),
            IdempotencyRecord {
                fingerprint,
                outcome: pack(outcome.clone()),
            },
        );
        outcome
    }

    fn reference_fields(reference: &wire::BucketRef) -> [&[u8]; 2] {
        [reference.bucket_id.as_bytes(), reference.name.as_bytes()]
    }

    fn condition_bytes(condition: &Option<Condition>) -> Vec<u8> {
        match condition {
            None => b"none".to_vec(),
            Some(Condition::IfAbsent) => b"absent".to_vec(),
            Some(Condition::IfMatch(value)) => [b"etag:".as_slice(), value.as_bytes()].concat(),
            Some(Condition::IfVersion(value)) => {
                [b"version:".as_slice(), value.as_bytes()].concat()
            }
        }
    }

    fn metadata_bytes(metadata: &wire::ObjectMetadata) -> Vec<u8> {
        let mut fields = vec![
            metadata.content_type.as_str(),
            metadata.content_encoding.as_str(),
            metadata.cache_control.as_str(),
            metadata.content_disposition.as_str(),
            metadata.content_language.as_str(),
        ];
        fields.push(if metadata.expires_unix_seconds.is_some() {
            "expires"
        } else {
            "no-expires"
        });
        let mut encoded = Vec::new();
        for field in fields {
            encoded.extend_from_slice(&(field.len() as u64).to_le_bytes());
            encoded.extend_from_slice(field.as_bytes());
        }
        if let Some(expires) = metadata.expires_unix_seconds {
            encoded.extend_from_slice(&expires.to_le_bytes());
        }
        let mut user = metadata.user.iter().collect::<Vec<_>>();
        user.sort_unstable_by(|left, right| left.0.cmp(right.0));
        for (name, value) in user {
            for field in [name.as_bytes(), value.as_bytes()] {
                encoded.extend_from_slice(&(field.len() as u64).to_le_bytes());
                encoded.extend_from_slice(field);
            }
        }
        encoded
    }

    fn next(state: &mut State, kind: &str) -> Result<String, ObjectsError> {
        let sequence = if kind == "listing" {
            &mut state.listing_sequence
        } else {
            &mut state.sequence
        };
        *sequence = sequence.checked_add(1).ok_or(ObjectsError::Capacity)?;
        Ok(format!("{kind}-{sequence:016x}"))
    }

    fn timestamp(state: &State) -> prost_types::Timestamp {
        prost_types::Timestamp {
            seconds: i64::try_from(state.sequence).unwrap_or(i64::MAX),
            nanos: 0,
        }
    }

    fn bucket<'a>(
        state: &'a State,
        reference: &wire::BucketRef,
    ) -> Result<&'a BucketState, ObjectsError> {
        state
            .buckets
            .get(&reference.bucket_id)
            .filter(|bucket| bucket.reference == *reference)
            .ok_or(ObjectsError::NotFound)
    }

    fn bucket_mut<'a>(
        state: &'a mut State,
        reference: &wire::BucketRef,
    ) -> Result<&'a mut BucketState, ObjectsError> {
        state
            .buckets
            .get_mut(&reference.bucket_id)
            .filter(|bucket| bucket.reference == *reference)
            .ok_or(ObjectsError::NotFound)
    }

    fn validate_key(value: &str) -> Result<(), ObjectsError> {
        if value.is_empty() || value.len() > limits::KEY_BYTES || value.contains('\0') {
            Err(ObjectsError::Invalid("invalid object key"))
        } else {
            Ok(())
        }
    }

    fn validate_bucket_name(name: &str) -> Result<(), ObjectsError> {
        let valid_edges = name
            .as_bytes()
            .first()
            .zip(name.as_bytes().last())
            .is_some_and(|(first, last)| {
                first.is_ascii_alphanumeric() && last.is_ascii_alphanumeric()
            });
        let valid_body = name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
        if !(3..=63).contains(&name.len()) || !valid_edges || !valid_body {
            Err(ObjectsError::Invalid("invalid bucket name"))
        } else {
            Ok(())
        }
    }

    fn validate_metadata(metadata: &wire::ObjectMetadata) -> Result<(), ObjectsError> {
        let fixed = [
            &metadata.content_type,
            &metadata.content_encoding,
            &metadata.cache_control,
            &metadata.content_disposition,
            &metadata.content_language,
        ]
        .into_iter()
        .map(String::len)
        .sum::<usize>();
        let user = metadata
            .user
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum::<usize>();
        if fixed > SYSTEM_METADATA_BYTES
            || user > limits::USER_METADATA_BYTES
            || metadata.user.iter().any(|(name, value)| {
                !name.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric()
                        || matches!(
                            byte,
                            b'!' | b'#'
                                | b'$'
                                | b'%'
                                | b'&'
                                | b'\''
                                | b'*'
                                | b'+'
                                | b'-'
                                | b'.'
                                | b'^'
                                | b'_'
                                | b'`'
                                | b'|'
                                | b'~'
                        )
                }) || name.is_empty()
                    || !Self::valid_header_value(value)
            })
            || [
                &metadata.content_type,
                &metadata.content_encoding,
                &metadata.cache_control,
                &metadata.content_disposition,
                &metadata.content_language,
            ]
            .into_iter()
            .any(|value| !Self::valid_header_value(value))
            || metadata
                .expires_unix_seconds
                .is_some_and(|seconds| !(-62_167_219_200..=253_402_300_799).contains(&seconds))
        {
            Err(ObjectsError::Invalid("invalid object metadata"))
        } else {
            Ok(())
        }
    }

    fn valid_header_value(value: &str) -> bool {
        value
            .bytes()
            .all(|byte| byte == b'\t' || (0x20..=0x7e).contains(&byte))
    }

    fn target(state: &State, target: &ReadTarget) -> Result<BucketState, ObjectsError> {
        match target {
            ReadTarget::Bucket(reference) => Ok(Self::bucket(state, reference)?.clone()),
            ReadTarget::Snapshot(reference) => state
                .snapshots
                .get(&reference.snapshot_id)
                .filter(|snapshot| snapshot.reference == *reference)
                .map(|snapshot| snapshot.bucket.clone())
                .ok_or(ObjectsError::NotFound),
        }
    }

    fn target_ref<'a>(
        state: &'a State,
        target: &ReadTarget,
    ) -> Result<&'a BucketState, ObjectsError> {
        match target {
            ReadTarget::Bucket(reference) => Self::bucket(state, reference),
            ReadTarget::Snapshot(reference) => state
                .snapshots
                .get(&reference.snapshot_id)
                .filter(|snapshot| snapshot.reference == *reference)
                .map(|snapshot| &snapshot.bucket)
                .ok_or(ObjectsError::NotFound),
        }
    }

    fn visible<'a>(
        bucket: &'a BucketState,
        object_key: &str,
        version_id: Option<&str>,
    ) -> Result<&'a Version, ObjectsError> {
        let versions = bucket
            .objects
            .get(object_key)
            .ok_or(ObjectsError::NotFound)?;
        let version = match version_id {
            Some(identity) => versions
                .iter()
                .rev()
                .find(|version| version.descriptor.version_id == identity),
            None => versions.last(),
        }
        .ok_or(ObjectsError::NotFound)?;
        if version.descriptor.delete_marker {
            Err(ObjectsError::NotFound)
        } else {
            Ok(version)
        }
    }

    fn condition(current: Option<&Version>, condition: &Option<Condition>) -> bool {
        match condition {
            None => true,
            Some(Condition::IfAbsent) => current.is_none_or(|value| value.descriptor.delete_marker),
            Some(Condition::IfMatch(etag)) => current.is_some_and(|value| {
                !value.descriptor.delete_marker && value.descriptor.etag == *etag
            }),
            Some(Condition::IfVersion(identity)) => {
                current.is_some_and(|value| value.descriptor.version_id == *identity)
            }
        }
    }

    fn descriptor(
        state: &mut State,
        body_digest: &[u8; 32],
        body_length: usize,
        metadata: wire::ObjectMetadata,
        marker: bool,
    ) -> Result<wire::ObjectVersion, ObjectsError> {
        let version_id = Self::next(state, "version")?;
        let etag = if marker {
            String::new()
        } else {
            format!("\"{}\"", blake3::Hash::from_bytes(*body_digest).to_hex())
        };
        Ok(wire::ObjectVersion {
            version_id,
            etag,
            size: body_length as u64,
            delete_marker: marker,
            metadata: Some(metadata),
            created_at: Some(Self::timestamp(state)),
        })
    }

    fn binding(
        target: &ReadTarget,
        prefix: &str,
        delimiter: Option<&str>,
        versions: bool,
    ) -> String {
        let target = match target {
            ReadTarget::Bucket(value) => format!("b:{}:{}", value.bucket_id, value.name),
            ReadTarget::Snapshot(value) => {
                format!("s:{}:{}", value.snapshot_id, value.source_bucket_id)
            }
        };
        format!(
            "{target}|{prefix}|{}|{versions}",
            delimiter.unwrap_or_default()
        )
    }

    fn listing_items(
        bucket: &BucketState,
        prefix: &str,
        delimiter: Option<&str>,
        versions: bool,
    ) -> Vec<ListingItem> {
        let mut items = Vec::new();
        let mut prefixes = BTreeSet::new();
        for (object_key, history) in bucket.objects.range(prefix.to_owned()..) {
            if !object_key.starts_with(prefix) {
                break;
            }
            if let Some(delimiter) = delimiter
                && let Some(position) = object_key[prefix.len()..].find(delimiter)
            {
                let end = prefix.len() + position + delimiter.len();
                prefixes.insert(object_key[..end].to_owned());
                continue;
            }
            if versions {
                items.extend(history.iter().rev().map(|version| {
                    ListingItem::Entry(Box::new(wire::ListEntry {
                        object_key: object_key.clone(),
                        version: Some(version.descriptor.clone()),
                    }))
                }));
            } else if let Some(version) = history.last()
                && !version.descriptor.delete_marker
            {
                items.push(ListingItem::Entry(Box::new(wire::ListEntry {
                    object_key: object_key.clone(),
                    version: Some(version.descriptor.clone()),
                })));
            }
        }
        items.extend(prefixes.into_iter().map(ListingItem::Prefix));
        items.sort_by(|left, right| {
            let left = match left {
                ListingItem::Entry(value) => &value.object_key,
                ListingItem::Prefix(value) => value,
            };
            let right = match right {
                ListingItem::Entry(value) => &value.object_key,
                ListingItem::Prefix(value) => value,
            };
            left.cmp(right)
        });
        items
    }

    async fn put_stored(
        &self,
        request: PutRequest,
        body: StoredBody,
        body_digest: [u8; 32],
    ) -> Result<wire::ObjectVersion, ObjectsError> {
        let PutRequest {
            bucket,
            object_key,
            body: _,
            metadata,
            condition,
            idempotency_key,
        } = request;
        Self::validate_key(&object_key)?;
        Self::validate_metadata(&metadata)?;
        let bucket_fields = Self::reference_fields(&bucket);
        let condition_bytes = Self::condition_bytes(&condition);
        let metadata_bytes = Self::metadata_bytes(&metadata);
        let fingerprint = Self::fingerprint(
            "put",
            &[
                bucket_fields[0],
                bucket_fields[1],
                object_key.as_bytes(),
                &condition_bytes,
                &metadata_bytes,
                &body_digest,
            ],
        );
        let mut state = self.state.lock().await;
        Self::execute(
            &mut state,
            idempotency_key.as_deref(),
            fingerprint,
            |outcome| match outcome {
                MutationOutcome::Version(value) => Some(value.clone()),
                _ => None,
            },
            MutationOutcome::Version,
            |state| {
                let current = Self::bucket(state, &bucket)?
                    .objects
                    .get(&object_key)
                    .and_then(|history| history.last())
                    .cloned();
                if !Self::condition(current.as_ref(), &condition) {
                    return Err(ObjectsError::PreconditionFailed);
                }
                let size = body.len();
                let additional = state.additional_body_bytes(&body)?;
                if size > state.maximum_object_bytes
                    || state
                        .bytes
                        .checked_add(additional)
                        .is_none_or(|bytes| bytes > state.maximum_bytes)
                {
                    return Err(ObjectsError::Capacity);
                }
                let descriptor = Self::descriptor(state, &body_digest, size, metadata, false)?;
                state.retain_body(&body)?;
                Self::bucket_mut(state, &bucket)?
                    .objects
                    .entry(object_key)
                    .or_default()
                    .push(Version {
                        descriptor: descriptor.clone(),
                        body: Some(body),
                    });
                Ok(descriptor)
            },
        )
    }

    #[cfg(feature = "local")]
    pub(crate) async fn put_external(
        &self,
        request: PutRequest,
        external: ExternalBody,
    ) -> Result<wire::ObjectVersion, ObjectsError> {
        let ExternalBody {
            root,
            digest,
            length,
        } = external;
        self.put_stored(
            request,
            StoredBody::Local {
                root: Arc::new(root),
                digest,
                length,
            },
            digest,
        )
        .await
    }

    async fn upload_part_stored(
        &self,
        request: StoredPartRequest,
    ) -> Result<wire::UploadedPart, ObjectsError> {
        let StoredPartRequest {
            bucket,
            object_key,
            upload_id,
            part_number,
            body,
            body_digest,
            idempotency_key,
        } = request;
        if !(1..=limits::MULTIPART_PARTS).contains(&part_number)
            || body.len() as u64 > limits::MAX_MULTIPART_PART_BYTES
        {
            return Err(ObjectsError::Invalid("invalid multipart part"));
        }
        let bucket_fields = Self::reference_fields(&bucket);
        let part_number_bytes = part_number.to_le_bytes();
        let fingerprint = Self::fingerprint(
            "upload-part",
            &[
                bucket_fields[0],
                bucket_fields[1],
                object_key.as_bytes(),
                upload_id.as_bytes(),
                &part_number_bytes,
                &body_digest,
            ],
        );
        let mut state = self.state.lock().await;
        Self::execute(
            &mut state,
            idempotency_key.as_deref(),
            fingerprint,
            |outcome| match outcome {
                MutationOutcome::Part(value) => Some(value.clone()),
                _ => None,
            },
            MutationOutcome::Part,
            |state| {
                let existing_body = state
                    .multiparts
                    .get(&upload_id)
                    .filter(|upload| upload.bucket == bucket && upload.object_key == object_key)
                    .ok_or(ObjectsError::NotFound)?
                    .parts
                    .get(&part_number)
                    .map(|(_, body)| body.clone());
                if let Some(existing) = &existing_body {
                    state.release_body(existing)?;
                }
                let additional = state.additional_body_bytes(&body)?;
                if state
                    .bytes
                    .checked_add(additional)
                    .is_none_or(|bytes| bytes > state.maximum_bytes)
                {
                    if let Some(existing) = &existing_body {
                        state.retain_body(existing)?;
                    }
                    return Err(ObjectsError::Capacity);
                }
                let part = wire::UploadedPart {
                    part_number,
                    etag: format!("\"{}\"", blake3::Hash::from_bytes(body_digest).to_hex()),
                    size: body.len() as u64,
                };
                state
                    .multiparts
                    .get_mut(&upload_id)
                    .ok_or(ObjectsError::NotFound)?
                    .parts
                    .insert(part_number, (part.clone(), body));
                let admitted = state
                    .multiparts
                    .get(&upload_id)
                    .and_then(|upload| upload.parts.get(&part_number))
                    .map(|(_, body)| body.clone())
                    .ok_or(ObjectsError::Unavailable)?;
                state.retain_body(&admitted)?;
                Ok(part)
            },
        )
    }

    #[cfg(feature = "local")]
    pub(crate) async fn upload_part_external(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        part_number: u32,
        external: ExternalBody,
        idempotency_key: Option<String>,
    ) -> Result<wire::UploadedPart, ObjectsError> {
        let ExternalBody {
            root,
            digest,
            length,
        } = external;
        self.upload_part_stored(StoredPartRequest {
            bucket,
            object_key,
            upload_id,
            part_number,
            body: StoredBody::Local {
                root: Arc::new(root),
                digest,
                length,
            },
            body_digest: digest,
            idempotency_key,
        })
        .await
    }
}

#[async_trait]
impl ObjectsProvider for MemoryObjects {
    async fn create_bucket(
        &self,
        name: String,
        idempotency_key: Option<String>,
    ) -> Result<wire::Bucket, ObjectsError> {
        Self::validate_bucket_name(&name)?;
        let fingerprint = Self::fingerprint("create-bucket", &[name.as_bytes()]);
        let mut state = self.state.lock().await;
        Self::execute(
            &mut state,
            idempotency_key.as_deref(),
            fingerprint,
            |outcome| match outcome {
                MutationOutcome::Bucket(value) => Some(value.clone()),
                _ => None,
            },
            MutationOutcome::Bucket,
            |state| {
                if state.names.contains_key(&name) {
                    return Err(ObjectsError::AlreadyExists);
                }
                let bucket_id = Self::next(state, "bucket")?;
                let reference = wire::BucketRef {
                    bucket_id: bucket_id.clone(),
                    name: name.clone(),
                };
                let bucket = BucketState {
                    reference: reference.clone(),
                    created_at: Self::timestamp(state),
                    objects: BTreeMap::new(),
                };
                state.names.insert(name, bucket_id.clone());
                state.buckets.insert(bucket_id, bucket.clone());
                Ok(wire::Bucket {
                    bucket: Some(reference),
                    created_at: Some(bucket.created_at),
                })
            },
        )
    }

    async fn head_bucket(&self, bucket: &wire::BucketRef) -> Result<wire::Bucket, ObjectsError> {
        let state = self.state.lock().await;
        let bucket = Self::bucket(&state, bucket)?;
        Ok(wire::Bucket {
            bucket: Some(bucket.reference.clone()),
            created_at: Some(bucket.created_at),
        })
    }

    async fn delete_bucket(
        &self,
        bucket: &wire::BucketRef,
        idempotency_key: Option<String>,
    ) -> Result<bool, ObjectsError> {
        let fields = Self::reference_fields(bucket);
        let fingerprint = Self::fingerprint("delete-bucket", &fields);
        let mut state = self.state.lock().await;
        Self::execute(
            &mut state,
            idempotency_key.as_deref(),
            fingerprint,
            |outcome| match outcome {
                MutationOutcome::Boolean(value) => Some(value.clone()),
                _ => None,
            },
            MutationOutcome::Boolean,
            |state| {
                let existing = Self::bucket(state, bucket)?.clone();
                if existing.objects.values().any(|history| !history.is_empty())
                    || state
                        .multiparts
                        .values()
                        .any(|upload| upload.bucket == *bucket)
                {
                    return Err(ObjectsError::PreconditionFailed);
                }
                state.buckets.remove(&bucket.bucket_id);
                state.names.remove(&bucket.name);
                Ok(true)
            },
        )
    }

    async fn put(&self, request: PutRequest) -> Result<wire::ObjectVersion, ObjectsError> {
        let digest = *blake3::hash(&request.body).as_bytes();
        let body = StoredBody::memory(request.body.clone());
        self.put_stored(request, body, digest).await
    }

    async fn get(&self, request: GetRequest) -> Result<BufferedObject, ObjectsError> {
        Self::validate_key(&request.object_key)?;
        let state = self.state.lock().await;
        let bucket = Self::target_ref(&state, &request.target)?;
        let version = Self::visible(bucket, &request.object_key, request.version_id.as_deref())?;
        if request
            .if_match
            .as_ref()
            .is_some_and(|etag| *etag != version.descriptor.etag)
            || request
                .if_none_match
                .as_ref()
                .is_some_and(|etag| *etag == version.descriptor.etag)
        {
            return Err(ObjectsError::PreconditionFailed);
        }
        let descriptor = version.descriptor.clone();
        let body = version.body.clone().ok_or(ObjectsError::NotFound)?;
        drop(state);
        let (start, end) = match request.range {
            None => (0, body.len()),
            Some((start, end)) => {
                let start =
                    usize::try_from(start).map_err(|_| ObjectsError::Invalid("invalid range"))?;
                let end = usize::try_from(end.unwrap_or(body.len().saturating_sub(1) as u64))
                    .map_err(|_| ObjectsError::Invalid("invalid range"))?;
                if start > end || end >= body.len() {
                    return Err(ObjectsError::Invalid("invalid range"));
                }
                (start, end.saturating_add(1))
            }
        };
        if end.saturating_sub(start) as u64 > request.maximum_bytes {
            return Err(ObjectsError::Capacity);
        }
        let selected = body.read_async(start, end).await?;
        Ok(BufferedObject {
            version: descriptor,
            body: selected,
        })
    }

    async fn delete(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        version_id: Option<String>,
        condition: Option<Condition>,
        idempotency_key: Option<String>,
    ) -> Result<DeleteResult, ObjectsError> {
        Self::validate_key(&object_key)?;
        let bucket_fields = Self::reference_fields(&bucket);
        let condition_bytes = Self::condition_bytes(&condition);
        let fingerprint = Self::fingerprint(
            "delete",
            &[
                bucket_fields[0],
                bucket_fields[1],
                object_key.as_bytes(),
                version_id.as_deref().unwrap_or_default().as_bytes(),
                &condition_bytes,
            ],
        );
        let mut state = self.state.lock().await;
        Self::execute(
            &mut state,
            idempotency_key.as_deref(),
            fingerprint,
            |outcome| match outcome {
                MutationOutcome::Delete(value) => Some(value.clone()),
                _ => None,
            },
            MutationOutcome::Delete,
            |state| {
                let current = Self::bucket(state, &bucket)?
                    .objects
                    .get(&object_key)
                    .and_then(|history| history.last())
                    .cloned();
                if !Self::condition(current.as_ref(), &condition) {
                    return Err(ObjectsError::PreconditionFailed);
                }
                if let Some(identity) = version_id {
                    let (removed_body, empty) = {
                        let Some(history) = Self::bucket_mut(state, &bucket)?
                            .objects
                            .get_mut(&object_key)
                        else {
                            return Ok(DeleteResult {
                                existed: false,
                                marker: None,
                            });
                        };
                        let Some(position) = history
                            .iter()
                            .position(|version| version.descriptor.version_id == identity)
                        else {
                            return Ok(DeleteResult {
                                existed: false,
                                marker: None,
                            });
                        };
                        let removed = history.remove(position);
                        (removed.body, history.is_empty())
                    };
                    if let Some(body) = removed_body {
                        state.release_body(&body)?;
                    }
                    if empty {
                        Self::bucket_mut(state, &bucket)?
                            .objects
                            .remove(&object_key);
                    }
                    Ok(DeleteResult {
                        existed: true,
                        marker: None,
                    })
                } else {
                    let marker = Self::descriptor(
                        state,
                        blake3::hash(&[]).as_bytes(),
                        0,
                        wire::ObjectMetadata::default(),
                        true,
                    )?;
                    Self::bucket_mut(state, &bucket)?
                        .objects
                        .entry(object_key)
                        .or_default()
                        .push(Version {
                            descriptor: marker.clone(),
                            body: None,
                        });
                    Ok(DeleteResult {
                        existed: true,
                        marker: Some(marker),
                    })
                }
            },
        )
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
        if page_size == 0
            || page_size > limits::LIST_PAGE_ENTRIES
            || delimiter.as_ref().is_some_and(String::is_empty)
        {
            return Err(ObjectsError::Invalid("invalid listing"));
        }
        let mut state = self.state.lock().await;
        let binding = Self::binding(&target, &prefix, delimiter.as_deref(), versions);
        let (view_id, offset) = if let Some(token) = continuation {
            let (view, offset) = token
                .rsplit_once(':')
                .ok_or(ObjectsError::Invalid("invalid continuation"))?;
            let offset = offset
                .parse::<usize>()
                .map_err(|_| ObjectsError::Invalid("invalid continuation"))?;
            (view.to_owned(), offset)
        } else {
            let bucket = Self::target(&state, &target)?;
            let view_id = Self::next(&mut state, "listing")?;
            state.listings.insert(
                view_id.clone(),
                ListingView {
                    binding: binding.clone(),
                    items: Self::listing_items(&bucket, &prefix, delimiter.as_deref(), versions),
                },
            );
            (view_id, 0)
        };
        let view = state
            .listings
            .get(&view_id)
            .filter(|view| view.binding == binding)
            .ok_or(ObjectsError::Invalid("invalid continuation"))?;
        if offset > view.items.len() {
            return Err(ObjectsError::Invalid("invalid continuation"));
        }
        let end = offset
            .saturating_add(page_size as usize)
            .min(view.items.len());
        let mut entries = Vec::new();
        let mut common_prefixes = Vec::new();
        for item in &view.items[offset..end] {
            match item {
                ListingItem::Entry(value) => entries.push(value.as_ref().clone()),
                ListingItem::Prefix(value) => common_prefixes.push(value.clone()),
            }
        }
        Ok(ProviderListPage {
            entries,
            common_prefixes,
            continuation: (end < view.items.len()).then(|| format!("{view_id}:{end}")),
        })
    }

    async fn snapshot(
        &self,
        bucket: wire::BucketRef,
        idempotency_key: Option<String>,
    ) -> Result<wire::Snapshot, ObjectsError> {
        let fields = Self::reference_fields(&bucket);
        let fingerprint = Self::fingerprint("snapshot", &fields);
        let mut state = self.state.lock().await;
        Self::execute(
            &mut state,
            idempotency_key.as_deref(),
            fingerprint,
            |outcome| match outcome {
                MutationOutcome::Snapshot(value) => Some(value.clone()),
                _ => None,
            },
            MutationOutcome::Snapshot,
            |state| {
                let source = Self::bucket(state, &bucket)?.clone();
                state.retain_bucket(&source)?;
                let snapshot_id = Self::next(state, "snapshot")?;
                let reference = wire::SnapshotRef {
                    snapshot_id: snapshot_id.clone(),
                    source_bucket_id: bucket.bucket_id,
                };
                let created_at = Self::timestamp(state);
                state.snapshots.insert(
                    snapshot_id,
                    SnapshotState {
                        reference: reference.clone(),
                        bucket: source,
                    },
                );
                Ok(wire::Snapshot {
                    snapshot: Some(reference),
                    created_at: Some(created_at),
                })
            },
        )
    }

    async fn destroy_snapshot(
        &self,
        snapshot: wire::SnapshotRef,
        idempotency_key: Option<String>,
    ) -> Result<bool, ObjectsError> {
        let fingerprint = Self::fingerprint(
            "destroy-snapshot",
            &[
                snapshot.snapshot_id.as_bytes(),
                snapshot.source_bucket_id.as_bytes(),
            ],
        );
        let mut state = self.state.lock().await;
        Self::execute(
            &mut state,
            idempotency_key.as_deref(),
            fingerprint,
            |outcome| match outcome {
                MutationOutcome::Boolean(value) => Some(value.clone()),
                _ => None,
            },
            MutationOutcome::Boolean,
            |state| {
                let exists = state
                    .snapshots
                    .get(&snapshot.snapshot_id)
                    .is_some_and(|value| value.reference == snapshot);
                if !exists {
                    return Ok(false);
                }
                let removed = state
                    .snapshots
                    .remove(&snapshot.snapshot_id)
                    .ok_or(ObjectsError::Unavailable)?;
                state.release_bucket(&removed.bucket)?;
                Ok(true)
            },
        )
    }

    async fn fork(
        &self,
        source: ReadTarget,
        destination_name: String,
        idempotency_key: Option<String>,
    ) -> Result<wire::Bucket, ObjectsError> {
        let source_identity = match &source {
            ReadTarget::Bucket(value) => format!("bucket:{}:{}", value.bucket_id, value.name),
            ReadTarget::Snapshot(value) => {
                format!("snapshot:{}:{}", value.snapshot_id, value.source_bucket_id)
            }
        };
        let fingerprint = Self::fingerprint(
            "fork",
            &[source_identity.as_bytes(), destination_name.as_bytes()],
        );
        let mut state = self.state.lock().await;
        Self::execute(
            &mut state,
            idempotency_key.as_deref(),
            fingerprint,
            |outcome| match outcome {
                MutationOutcome::Bucket(value) => Some(value.clone()),
                _ => None,
            },
            MutationOutcome::Bucket,
            |state| {
                if state.names.contains_key(&destination_name) {
                    return Err(ObjectsError::AlreadyExists);
                }
                let source = Self::target(state, &source)?;
                state.retain_bucket(&source)?;
                let bucket_id = Self::next(state, "bucket")?;
                let reference = wire::BucketRef {
                    bucket_id: bucket_id.clone(),
                    name: destination_name.clone(),
                };
                let created_at = Self::timestamp(state);
                state.names.insert(destination_name, bucket_id.clone());
                state.buckets.insert(
                    bucket_id,
                    BucketState {
                        reference: reference.clone(),
                        created_at,
                        objects: source.objects,
                    },
                );
                Ok(wire::Bucket {
                    bucket: Some(reference),
                    created_at: Some(created_at),
                })
            },
        )
    }

    async fn create_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        metadata: wire::ObjectMetadata,
        condition: Option<Condition>,
        idempotency_key: Option<String>,
    ) -> Result<wire::MultipartUpload, ObjectsError> {
        Self::validate_key(&object_key)?;
        Self::validate_metadata(&metadata)?;
        let bucket_fields = Self::reference_fields(&bucket);
        let metadata_bytes = Self::metadata_bytes(&metadata);
        let condition_bytes = Self::condition_bytes(&condition);
        let fingerprint = Self::fingerprint(
            "create-multipart",
            &[
                bucket_fields[0],
                bucket_fields[1],
                object_key.as_bytes(),
                &metadata_bytes,
                &condition_bytes,
            ],
        );
        let mut state = self.state.lock().await;
        Self::execute(
            &mut state,
            idempotency_key.as_deref(),
            fingerprint,
            |outcome| match outcome {
                MutationOutcome::Multipart(value) => Some(value.clone()),
                _ => None,
            },
            MutationOutcome::Multipart,
            |state| {
                Self::bucket(state, &bucket)?;
                let upload_id = Self::next(state, "upload")?;
                state.multiparts.insert(
                    upload_id.clone(),
                    MultipartState {
                        bucket,
                        object_key,
                        metadata,
                        condition,
                        parts: BTreeMap::new(),
                    },
                );
                Ok(wire::MultipartUpload { upload_id })
            },
        )
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
        let digest = *blake3::hash(&body).as_bytes();
        self.upload_part_stored(StoredPartRequest {
            bucket,
            object_key,
            upload_id,
            part_number,
            body: StoredBody::memory(body),
            body_digest: digest,
            idempotency_key,
        })
        .await
    }

    async fn list_parts(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
    ) -> Result<Vec<wire::UploadedPart>, ObjectsError> {
        let state = self.state.lock().await;
        let upload = state
            .multiparts
            .get(&upload_id)
            .filter(|upload| upload.bucket == bucket && upload.object_key == object_key)
            .ok_or(ObjectsError::NotFound)?;
        Ok(upload
            .parts
            .values()
            .map(|(part, _)| part.clone())
            .collect())
    }

    async fn complete_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        parts: Vec<wire::UploadedPart>,
        idempotency_key: Option<String>,
    ) -> Result<wire::ObjectVersion, ObjectsError> {
        if parts.is_empty() || parts.len() > limits::MULTIPART_PARTS as usize {
            return Err(ObjectsError::Invalid("invalid multipart completion"));
        }
        let bucket_fields = Self::reference_fields(&bucket);
        let mut part_bytes = Vec::new();
        for part in &parts {
            part_bytes.extend_from_slice(&part.part_number.to_le_bytes());
            part_bytes.extend_from_slice(&(part.etag.len() as u64).to_le_bytes());
            part_bytes.extend_from_slice(part.etag.as_bytes());
            part_bytes.extend_from_slice(&part.size.to_le_bytes());
        }
        let fingerprint = Self::fingerprint(
            "complete-multipart",
            &[
                bucket_fields[0],
                bucket_fields[1],
                object_key.as_bytes(),
                upload_id.as_bytes(),
                &part_bytes,
            ],
        );
        let mut state = self.state.lock().await;
        Self::execute(
            &mut state,
            idempotency_key.as_deref(),
            fingerprint,
            |outcome| match outcome {
                MutationOutcome::Version(value) => Some(value.clone()),
                _ => None,
            },
            MutationOutcome::Version,
            |state| {
                let upload = state
                    .multiparts
                    .remove(&upload_id)
                    .filter(|upload| upload.bucket == bucket && upload.object_key == object_key)
                    .ok_or(ObjectsError::NotFound)?;
                let exact = upload.parts.values().map(|(part, _)| part).eq(parts.iter());
                let sizes_valid = parts.iter().enumerate().all(|(index, part)| {
                    index + 1 == parts.len() || part.size >= limits::MIN_MULTIPART_PART_BYTES
                });
                if !exact || !sizes_valid {
                    state.multiparts.insert(upload_id, upload);
                    return Err(ObjectsError::Invalid("multipart receipts do not match"));
                }
                let current = Self::bucket(state, &bucket)?
                    .objects
                    .get(&object_key)
                    .and_then(|history| history.last())
                    .cloned();
                if !Self::condition(current.as_ref(), &upload.condition) {
                    state.multiparts.insert(upload_id, upload);
                    return Err(ObjectsError::PreconditionFailed);
                }
                let body_length =
                    match upload.parts.values().try_fold(0usize, |total, (_, body)| {
                        total.checked_add(body.len()).ok_or(ObjectsError::Capacity)
                    }) {
                        Ok(body_length) => body_length,
                        Err(error) => {
                            state.multiparts.insert(upload_id, upload);
                            return Err(error);
                        }
                    };
                if body_length > state.maximum_object_bytes {
                    state.multiparts.insert(upload_id, upload);
                    return Err(ObjectsError::Capacity);
                }
                let bodies = upload
                    .parts
                    .into_values()
                    .map(|(_, body)| body)
                    .collect::<Vec<_>>();
                let mut hasher = blake3::Hasher::new();
                for body in &bodies {
                    body.update_hash(&mut hasher)?;
                }
                let body_digest = *hasher.finalize().as_bytes();
                let descriptor =
                    Self::descriptor(state, &body_digest, body_length, upload.metadata, false)?;
                Self::bucket_mut(state, &bucket)?
                    .objects
                    .entry(object_key)
                    .or_default()
                    .push(Version {
                        descriptor: descriptor.clone(),
                        body: Some(StoredBody::Composite {
                            parts: bodies.into(),
                            length: body_length,
                        }),
                    });
                Ok(descriptor)
            },
        )
    }

    async fn abort_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        idempotency_key: Option<String>,
    ) -> Result<bool, ObjectsError> {
        let bucket_fields = Self::reference_fields(&bucket);
        let fingerprint = Self::fingerprint(
            "abort-multipart",
            &[
                bucket_fields[0],
                bucket_fields[1],
                object_key.as_bytes(),
                upload_id.as_bytes(),
            ],
        );
        let mut state = self.state.lock().await;
        Self::execute(
            &mut state,
            idempotency_key.as_deref(),
            fingerprint,
            |outcome| match outcome {
                MutationOutcome::Boolean(value) => Some(value.clone()),
                _ => None,
            },
            MutationOutcome::Boolean,
            |state| {
                let matches = state.multiparts.get(&upload_id).is_some_and(|upload| {
                    upload.bucket == bucket && upload.object_key == object_key
                });
                if !matches {
                    return Ok(false);
                }
                if let Some(upload) = state.multiparts.remove(&upload_id) {
                    for (_, body) in upload.parts.values() {
                        state.release_body(body)?;
                    }
                }
                Ok(true)
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata() -> wire::ObjectMetadata {
        wire::ObjectMetadata {
            content_type: "text/plain".into(),
            ..Default::default()
        }
    }

    async fn put(
        store: &MemoryObjects,
        bucket: &wire::BucketRef,
        object_key: &str,
        body: &[u8],
        condition: Option<Condition>,
    ) -> wire::ObjectVersion {
        store
            .put(PutRequest {
                bucket: bucket.clone(),
                object_key: object_key.into(),
                body: bytes::Bytes::copy_from_slice(body),
                metadata: metadata(),
                condition,
                idempotency_key: None,
            })
            .await
            .unwrap_or_else(|_| unreachable!())
    }

    #[tokio::test]
    async fn synchronous_bucket_composition_is_exact_and_capacity_bounded() {
        assert!(matches!(
            MemoryObjects::new(0),
            Err(ObjectsError::Invalid("memory byte limit must be positive"))
        ));
        assert!(MemoryObjects::with_bucket("INVALID", 4).is_err());
        assert!(MemoryObjects::with_bucket_limits("embedded", 5, 4).is_err());

        let (independent, independent_bucket) =
            MemoryObjects::with_bucket_limits("independent", 3, 6)
                .unwrap_or_else(|_| unreachable!());
        put(&independent, &independent_bucket, "first", b"one", None).await;
        put(&independent, &independent_bucket, "second", b"two", None).await;
        assert_eq!(
            independent
                .put(PutRequest {
                    bucket: independent_bucket.clone(),
                    object_key: "aggregate-overflow".into(),
                    body: bytes::Bytes::from_static(b"x"),
                    metadata: metadata(),
                    condition: None,
                    idempotency_key: None,
                })
                .await,
            Err(ObjectsError::Capacity)
        );
        let (per_object, per_object_bucket) = MemoryObjects::with_bucket_limits("per-object", 3, 6)
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(
            per_object
                .put(PutRequest {
                    bucket: per_object_bucket,
                    object_key: "too-large".into(),
                    body: bytes::Bytes::from_static(b"four"),
                    metadata: metadata(),
                    condition: None,
                    idempotency_key: None,
                })
                .await,
            Err(ObjectsError::Capacity)
        );

        let (store, bucket) =
            MemoryObjects::with_bucket("embedded", 4).unwrap_or_else(|_| unreachable!());
        assert_eq!(
            store
                .head_bucket(&bucket)
                .await
                .unwrap_or_else(|_| unreachable!())
                .bucket,
            Some(bucket.clone())
        );
        put(&store, &bucket, "full", b"four", None).await;
        assert_eq!(
            store
                .put(PutRequest {
                    bucket,
                    object_key: "overflow".into(),
                    body: bytes::Bytes::from_static(b"x"),
                    metadata: metadata(),
                    condition: None,
                    idempotency_key: None,
                })
                .await,
            Err(ObjectsError::Capacity)
        );
    }

    #[tokio::test]
    async fn memory_provider_passes_the_public_conformance_suite() -> Result<(), ObjectsError> {
        crate::conformance::verify(&MemoryObjects::default(), "conformance").await
    }

    #[tokio::test]
    async fn shared_snapshot_and_fork_bodies_retain_exact_capacity() {
        let store = MemoryObjects::new(3).unwrap_or_else(|_| unreachable!());
        let source = store
            .create_bucket("source-bucket".into(), Some("create-source".into()))
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        let version = put(&store, &source, "shared", b"abc", None).await;
        let version_id = version.version_id.clone();
        let snapshot = store
            .snapshot(source.clone(), Some("snapshot".into()))
            .await
            .unwrap_or_else(|_| unreachable!())
            .snapshot
            .unwrap_or_else(|| unreachable!());
        assert!(
            store
                .delete(
                    source.clone(),
                    "shared".into(),
                    Some(version.version_id),
                    None,
                    Some("delete-source".into()),
                )
                .await
                .is_ok()
        );
        assert!(matches!(
            store
                .put(PutRequest {
                    bucket: source.clone(),
                    object_key: "replacement".into(),
                    body: bytes::Bytes::from_static(b"def"),
                    metadata: metadata(),
                    condition: None,
                    idempotency_key: None,
                })
                .await,
            Err(ObjectsError::Capacity)
        ));
        let fork = store
            .fork(
                ReadTarget::Snapshot(snapshot.clone()),
                "fork-bucket".into(),
                Some("fork".into()),
            )
            .await
            .unwrap_or_else(|_| unreachable!())
            .bucket
            .unwrap_or_else(|| unreachable!());
        assert!(
            store
                .destroy_snapshot(snapshot, Some("destroy-snapshot".into()))
                .await
                .unwrap_or(false)
        );
        assert!(matches!(
            store
                .put(PutRequest {
                    bucket: source.clone(),
                    object_key: "replacement".into(),
                    body: bytes::Bytes::from_static(b"def"),
                    metadata: metadata(),
                    condition: None,
                    idempotency_key: None,
                })
                .await,
            Err(ObjectsError::Capacity)
        ));
        assert!(
            store
                .delete(
                    fork,
                    "shared".into(),
                    Some(version_id),
                    None,
                    Some("delete-fork".into()),
                )
                .await
                .is_ok()
        );
        assert!(
            store
                .put(PutRequest {
                    bucket: source,
                    object_key: "replacement".into(),
                    body: bytes::Bytes::from_static(b"def"),
                    metadata: metadata(),
                    condition: None,
                    idempotency_key: None,
                })
                .await
                .is_ok()
        );
    }
}
