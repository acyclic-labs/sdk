//! AWS S3 HTTP translation over the canonical workspace S3 view.
//!
//! Request parsing, SigV4 verification, XML, and HTTP framing are delegated to
//! `s3s`. This module contains no namespace or storage implementation: every
//! accepted operation resolves one scoped workspace and invokes [`crate::S3Workspace`].

use crate::kernel::{AsyncBlobSource, ClosureLimits, DecodeLimits};
use crate::model::FilesystemProfile;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, ByteRange, CancellationToken, Digest, IdempotencyKey,
    ObjectId, ObjectKind, S3Error, S3ListCursor, S3ListOptions, StagedContent,
    StreamAuthorityStore, Workspace,
};
use acyclic_stream::StreamProvider;
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::{StreamExt, stream};
use s3s::{
    S3, S3Request, S3Response, S3Result,
    auth::{S3Auth, SecretKey},
    dto::{
        AbortMultipartUploadInput, AbortMultipartUploadOutput, CommonPrefix,
        CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CompletedPart,
        CopyObjectInput, CopyObjectOutput, CopyObjectResult, CreateMultipartUploadInput,
        CreateMultipartUploadOutput, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput,
        DeleteObjectsOutput, DeletedObject, GetBucketLocationInput, GetBucketLocationOutput,
        GetObjectInput, GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput,
        HeadObjectOutput, ListObjectsV2Input, ListObjectsV2Output, ListPartsInput, ListPartsOutput,
        Object, Part, PutObjectInput, PutObjectOutput, Range, StreamingBlob, UploadPartInput,
        UploadPartOutput,
    },
};
use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};
use subtle::ConstantTimeEq;

const IDEMPOTENCY_HEADER: &str = "x-acyclic-idempotency-key";
const SDK_INVOCATION_HEADER: &str = "amz-sdk-invocation-id";
const SESSION_TOKEN_HEADER: &str = "x-amz-security-token";
const MULTIPART_DOMAIN: &[u8] = b"acyclic-fs-s3-multipart-v1\0";
const MULTIPART_RECORD_VERSION: u8 = 1;

/// Hard request bounds for the S3 protocol edge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilesystemS3Limits {
    /// Largest single request body accepted before publication.
    pub maximum_request_bytes: u64,
    /// Largest response chunk returned from the canonical range reader.
    pub response_chunk_bytes: u64,
    /// Largest combined object/prefix page.
    pub maximum_list_keys: u32,
    /// Largest authenticated namespace frontier examined by one list request.
    pub maximum_list_entries_examined: u32,
    /// Largest encoded object key or continuation marker.
    pub maximum_key_bytes: u32,
    /// Largest number of parts retained by one durable multipart upload.
    pub maximum_multipart_parts: u32,
    /// Largest durable state-machine record count, including part replacements.
    pub maximum_multipart_records: u32,
    /// Largest logical byte count admitted for one multipart part.
    pub maximum_multipart_part_bytes: u64,
    /// Minimum size of every completed part except the final part.
    pub minimum_multipart_part_bytes: u64,
    /// Maximum optimistic Stream retries under concurrent multipart activity.
    pub maximum_multipart_cas_retries: u32,
}

/// Administrative bounds for discovering active multipart object roots before
/// immutable-object reclamation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct S3MultipartRetentionLimits {
    /// Largest number of durable upload registry entries examined.
    pub maximum_uploads: u64,
    /// Largest number of distinct staged immutable objects retained.
    pub maximum_objects: u64,
    /// Largest cumulative canonical bytes authenticated while tracing roots.
    pub maximum_object_bytes: u64,
}

impl Default for S3MultipartRetentionLimits {
    fn default() -> Self {
        Self {
            maximum_uploads: 1_000_000,
            maximum_objects: 10_000_000,
            maximum_object_bytes: 1024 * 1024 * 1024 * 1024,
        }
    }
}

impl S3MultipartRetentionLimits {
    fn validate(self) -> S3Result<Self> {
        if self.maximum_uploads == 0 || self.maximum_objects == 0 || self.maximum_object_bytes == 0
        {
            return Err(s3s::s3_error!(InvalidArgument));
        }
        Ok(self)
    }
}

impl Default for FilesystemS3Limits {
    fn default() -> Self {
        Self {
            maximum_request_bytes: 5 * 1024 * 1024 * 1024,
            response_chunk_bytes: 8 * 1024 * 1024,
            maximum_list_keys: 1_000,
            maximum_list_entries_examined: 100_000,
            maximum_key_bytes: 32 * 1024,
            maximum_multipart_parts: 10_000,
            maximum_multipart_records: 20_003,
            maximum_multipart_part_bytes: 5 * 1024 * 1024 * 1024,
            minimum_multipart_part_bytes: 5 * 1024 * 1024,
            maximum_multipart_cas_retries: 32,
        }
    }
}

impl FilesystemS3Limits {
    /// Validates every protocol bound before a listener is created.
    ///
    /// # Errors
    ///
    /// Rejects zero limits and response chunks larger than request bodies.
    pub fn validate(self) -> S3Result<Self> {
        if self.maximum_request_bytes == 0
            || self.response_chunk_bytes == 0
            || self.maximum_list_keys == 0
            || self.maximum_list_entries_examined == 0
            || self.maximum_key_bytes == 0
            || self.maximum_multipart_parts == 0
            || self.maximum_multipart_records < self.maximum_multipart_parts.saturating_add(3)
            || self.maximum_multipart_part_bytes == 0
            || self.minimum_multipart_part_bytes > self.maximum_multipart_part_bytes
            || self.maximum_multipart_cas_retries == 0
            || self.response_chunk_bytes > self.maximum_request_bytes
        {
            return Err(s3s::s3_error!(InvalidArgument));
        }
        Ok(self)
    }
}

/// Authority adapter that retains filesystem state in the canonical public
/// hierarchical Stream provider.
pub trait StreamBackedAuthority: AsyncAuthorityStore {
    /// Exact Stream implementation used by this deployment.
    type Stream: acyclic_stream::StreamProvider;

    /// Returns the shared provider; no shadow log or multipart catalog is created.
    fn stream_provider(&self) -> Arc<Self::Stream>;
}

impl<P: acyclic_stream::StreamProvider> StreamBackedAuthority for StreamAuthorityStore<P> {
    type Stream = P;

    fn stream_provider(&self) -> Arc<Self::Stream> {
        self.provider()
    }
}

/// One authenticated credential resolved to one exact workspace.
#[derive(Clone)]
pub struct FilesystemS3Principal<A, O> {
    /// Secret used only by the SigV4 verifier.
    pub secret_key: SecretKey,
    /// Opaque bucket coordinate issued for this workspace.
    pub bucket: String,
    /// Canonical workspace selected by the capability.
    pub workspace: Workspace<A, O>,
    /// Optional immutable generation bound into a read-only capability.
    pub generation: Option<crate::GenerationId>,
    /// Whether the capability admits S3 mutations.
    pub writable: bool,
}

/// Deployment-owned credential lookup. Implementations authorize, expire, and
/// revoke access keys before returning a workspace capability.
#[async_trait]
pub trait FilesystemS3Resolver<A, O>: Send + Sync + 'static {
    /// Resolves only the signing secret required to authenticate SigV4.
    async fn resolve_secret_key(&self, access_key: &str) -> S3Result<SecretKey>;

    /// Resolves and authorizes the complete request capability after SigV4.
    async fn resolve_principal(
        &self,
        access_key: &str,
        session_token: Option<&str>,
        bucket: &str,
    ) -> S3Result<FilesystemS3Principal<A, O>>;
}

/// SigV4 secret lookup sharing the exact resolver used by semantic dispatch.
pub struct FilesystemS3Authentication<R, A, O> {
    resolver: Arc<R>,
    marker: PhantomData<fn() -> (A, O)>,
}

impl<R, A, O> FilesystemS3Authentication<R, A, O> {
    /// Creates an authentication adapter without duplicating credential state.
    #[must_use]
    pub fn new(resolver: Arc<R>) -> Self {
        Self {
            resolver,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<R, A, O> S3Auth for FilesystemS3Authentication<R, A, O>
where
    R: FilesystemS3Resolver<A, O>,
    A: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        self.resolver.resolve_secret_key(access_key).await
    }
}

/// Stateless S3 operation adapter over canonical workspaces.
pub struct FilesystemS3Adapter<R, A, O> {
    resolver: Arc<R>,
    limits: FilesystemS3Limits,
    marker: PhantomData<fn() -> (A, O)>,
}

impl<R, A, O> FilesystemS3Adapter<R, A, O> {
    /// Creates one bounded adapter.
    ///
    /// # Errors
    ///
    /// Rejects invalid hard limits.
    pub fn new(resolver: Arc<R>, limits: FilesystemS3Limits) -> S3Result<Self> {
        Ok(Self {
            resolver,
            limits: limits.validate()?,
            marker: PhantomData,
        })
    }
}

impl<R, A, O> FilesystemS3Adapter<R, A, O>
where
    R: FilesystemS3Resolver<A, O>,
    A: StreamBackedAuthority,
    O: AsyncObjectStore,
{
    async fn principal<T>(
        &self,
        request: &S3Request<T>,
        bucket: &str,
    ) -> S3Result<FilesystemS3Principal<A, O>> {
        let credentials = request
            .credentials
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        let principal = self
            .resolver
            .resolve_principal(
                &credentials.access_key,
                request
                    .headers
                    .get(SESSION_TOKEN_HEADER)
                    .and_then(|value| value.to_str().ok()),
                bucket,
            )
            .await?;
        if !bool::from(principal.secret_key.ct_eq(&credentials.secret_key)) {
            return Err(s3s::s3_error!(AccessDenied));
        }
        Ok(principal)
    }

    async fn scoped_principal<T>(
        &self,
        request: &S3Request<T>,
        bucket: &str,
        mutation: bool,
    ) -> S3Result<FilesystemS3Principal<A, O>> {
        let principal = self.principal(request, bucket).await?;
        if principal.bucket != bucket
            || mutation && (!principal.writable || principal.generation.is_some())
        {
            return Err(s3s::s3_error!(AccessDenied));
        }
        Ok(principal)
    }
}

#[derive(Clone, Debug)]
struct MultipartPart {
    staged: StagedContent,
    etag: String,
}

#[derive(Clone, Debug)]
enum MultipartTerminal {
    Completing(Vec<(u32, String)>),
    Completed(crate::GenerationId),
    Aborted,
}

#[derive(Clone, Debug)]
struct MultipartSnapshot {
    key: String,
    tail: u64,
    minimum_part_bytes: u64,
    parts: BTreeMap<u32, MultipartPart>,
    operations: BTreeMap<[u8; 16], (u32, String)>,
    terminal: Option<MultipartTerminal>,
    work: crate::WorkCounters,
}

/// Authenticates and returns every immutable object retained by active
/// multipart Stream state.
///
/// Completed and aborted uploads are excluded:
/// completed bytes are reachable from the published generation and aborted
/// bytes are collectible. The operation fails closed if either discovery bound
/// is reached, a hierarchy entry is malformed, or any upload history is not a
/// valid canonical state machine.
pub async fn active_s3_multipart_objects<P: acyclic_stream::StreamProvider, O: AsyncObjectStore>(
    provider: &P,
    objects: &O,
    protocol_limits: FilesystemS3Limits,
    retention_limits: S3MultipartRetentionLimits,
    budget: crate::WorkBudget,
    cancellation: &CancellationToken,
) -> S3Result<crate::OperationReceipt<Vec<ObjectId>>> {
    let protocol_limits = protocol_limits.validate()?;
    let retention_limits = retention_limits.validate()?;
    cancellation
        .check()
        .map_err(|_| s3s::s3_error!(RequestTimeout))?;
    let (roots, mut work) =
        discover_s3_multipart_roots(provider, protocol_limits, retention_limits).await?;
    work.verify(budget).map_err(|_| s3s::s3_error!(SlowDown))?;
    let mut retained = BTreeMap::new();
    for staged in roots {
        cancellation
            .check()
            .map_err(|_| s3s::s3_error!(RequestTimeout))?;
        let remaining = work
            .remaining(budget)
            .map_err(|_| s3s::s3_error!(SlowDown))?;
        let (closure, nested) = crate::kernel::prove_blob_closure_async(
            objects,
            staged.root(),
            staged.logical_bytes(),
            ClosureLimits {
                decode: DecodeLimits::default(),
                maximum_objects: retention_limits.maximum_objects,
                maximum_files: 0,
                maximum_object_bytes: retention_limits.maximum_object_bytes,
                profile: FilesystemProfile::Portable,
                symbolic_links: false,
                hard_links: false,
                sparse_files: false,
            },
            remaining,
            cancellation,
        )
        .await
        .map_err(|_| s3s::s3_error!(InternalError))?;
        work = work
            .checked_add(nested)
            .map_err(|_| s3s::s3_error!(InternalError))?;
        if work.object_bytes_read > retention_limits.maximum_object_bytes {
            return Err(s3s::s3_error!(SlowDown));
        }
        for object in closure {
            retained.insert(object, ());
        }
        if retained.len() as u64 > retention_limits.maximum_objects {
            return Err(s3s::s3_error!(SlowDown));
        }
    }
    work.verify(budget).map_err(|_| s3s::s3_error!(SlowDown))?;
    Ok(crate::OperationReceipt {
        value: retained.into_keys().collect(),
        work,
    })
}

async fn discover_s3_multipart_roots<P: acyclic_stream::StreamProvider>(
    provider: &P,
    protocol_limits: FilesystemS3Limits,
    retention_limits: S3MultipartRetentionLimits,
) -> S3Result<(Vec<StagedContent>, crate::WorkCounters)> {
    let root = acyclic_stream::StreamPath::new("fs/s3-multipart/registry")
        .map_err(|_| s3s::s3_error!(InternalError))?;
    let (registries, mut work) =
        bounded_children(provider, root, 256, "fs/s3-multipart/registry/").await?;
    let mut upload_paths = BTreeMap::new();
    let mut examined = 0_u64;
    for registry in registries {
        let suffix = registry
            .as_str()
            .strip_prefix("fs/s3-multipart/registry/")
            .ok_or_else(|| s3s::s3_error!(InternalError))?;
        if suffix.len() != 2 || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(s3s::s3_error!(InternalError));
        }
        let tail = provider
            .tail(registry.clone())
            .await
            .map_err(stream_read_error)?;
        work.backend_read_operations = work
            .backend_read_operations
            .checked_add(1)
            .ok_or_else(|| s3s::s3_error!(InternalError))?;
        let mut from = 0_u64;
        while from < tail {
            let available = retention_limits.maximum_uploads.saturating_sub(examined);
            if available == 0 {
                return Err(s3s::s3_error!(SlowDown));
            }
            let limit = u32::try_from(
                (tail - from)
                    .min(available)
                    .min(acyclic_stream::MAX_ITEMS as u64),
            )
            .map_err(|_| s3s::s3_error!(InternalError))?;
            let mut records = provider
                .read(acyclic_stream::ReadRequest {
                    path: registry.clone(),
                    from,
                    limit,
                })
                .await
                .map_err(stream_read_error)?;
            work.backend_read_operations = work
                .backend_read_operations
                .checked_add(1)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
            let before = examined;
            while let Some(record) = records.next().await {
                let record = record.map_err(stream_read_error)?;
                if record.sequence != from + (examined - before) {
                    return Err(s3s::s3_error!(InternalError));
                }
                let path = std::str::from_utf8(&record.value)
                    .map_err(|_| s3s::s3_error!(InternalError))?;
                let path = acyclic_stream::StreamPath::new(path)
                    .map_err(|_| s3s::s3_error!(InternalError))?;
                validate_multipart_stream_path(&path)?;
                upload_paths.insert(path.as_str().to_owned(), path);
                work.authority_records_read = work
                    .authority_records_read
                    .checked_add(1)
                    .ok_or_else(|| s3s::s3_error!(InternalError))?;
                work.authority_bytes_read = work
                    .authority_bytes_read
                    .checked_add(record.value.len() as u64)
                    .ok_or_else(|| s3s::s3_error!(InternalError))?;
                work.items_examined = work
                    .items_examined
                    .checked_add(1)
                    .ok_or_else(|| s3s::s3_error!(InternalError))?;
                examined = examined
                    .checked_add(1)
                    .ok_or_else(|| s3s::s3_error!(InternalError))?;
            }
            if examined == before {
                return Err(s3s::s3_error!(InternalError));
            }
            from = from
                .checked_add(examined - before)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
        }
    }
    let mut roots = BTreeMap::new();
    for upload in upload_paths.into_values() {
        let snapshot = read_multipart_snapshot(provider, upload, protocol_limits).await?;
        work = work
            .checked_add(snapshot.work)
            .map_err(|_| s3s::s3_error!(InternalError))?;
        if matches!(
            snapshot.terminal,
            Some(MultipartTerminal::Completed(_) | MultipartTerminal::Aborted)
        ) {
            continue;
        }
        for part in snapshot.parts.into_values() {
            roots.insert(part.staged.root(), part.staged);
        }
    }
    Ok((roots.into_values().collect(), work))
}

fn validate_multipart_stream_path(path: &acyclic_stream::StreamPath) -> S3Result {
    let suffix = path
        .as_str()
        .strip_prefix("fs/s3-multipart/")
        .ok_or_else(|| s3s::s3_error!(InternalError))?;
    let mut segments = suffix.split('/');
    let workspace = segments
        .next()
        .ok_or_else(|| s3s::s3_error!(InternalError))?;
    let upload = segments
        .next()
        .ok_or_else(|| s3s::s3_error!(InternalError))?;
    if segments.next().is_some()
        || workspace.len() != 32
        || upload.len() != 32
        || !workspace.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !upload.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(s3s::s3_error!(InternalError));
    }
    Ok(())
}

async fn bounded_children<P: acyclic_stream::StreamProvider>(
    provider: &P,
    parent: acyclic_stream::StreamPath,
    maximum: u32,
    exact_prefix: &str,
) -> S3Result<(Vec<acyclic_stream::StreamPath>, crate::WorkCounters)> {
    let limit = maximum
        .checked_add(1)
        .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
    let mut stream = provider
        .children(acyclic_stream::ChildrenRequest {
            parent: Some(parent),
            limit,
        })
        .await
        .map_err(stream_read_error)?;
    let mut children = Vec::new();
    while let Some(child) = stream.next().await {
        let path = child.map_err(stream_read_error)?.path;
        if !path.as_str().starts_with(exact_prefix)
            || path.as_str()[exact_prefix.len()..].contains('/')
        {
            return Err(s3s::s3_error!(InternalError));
        }
        children.push(path);
    }
    if children.len() > maximum as usize {
        return Err(s3s::s3_error!(SlowDown));
    }
    let child_count = children.len() as u64;
    Ok((
        children,
        crate::WorkCounters {
            backend_read_operations: 1,
            items_examined: child_count,
            ..crate::WorkCounters::default()
        },
    ))
}

impl<R, A, O> FilesystemS3Adapter<R, A, O>
where
    R: FilesystemS3Resolver<A, O>,
    A: StreamBackedAuthority,
    O: AsyncObjectStore,
{
    fn multipart_path(
        workspace: &Workspace<A, O>,
        upload_id: &str,
    ) -> S3Result<acyclic_stream::StreamPath> {
        if upload_id.len() != 32 || !upload_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(s3s::s3_error!(NoSuchUpload));
        }
        acyclic_stream::StreamPath::new(format!(
            "fs/s3-multipart/{}/{}",
            hex::encode(workspace.id().into_bytes()),
            upload_id.to_ascii_lowercase()
        ))
        .map_err(|_| s3s::s3_error!(InvalidRequest))
    }

    fn multipart_upload_id(workspace: &Workspace<A, O>, operation: IdempotencyKey) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(MULTIPART_DOMAIN);
        hasher.update(b"upload-id\0");
        hasher.update(&workspace.id().into_bytes());
        hasher.update(&operation.into_bytes());
        hex::encode(&hasher.finalize().as_bytes()[..16])
    }

    fn multipart_registry_path(upload_id: &str) -> S3Result<acyclic_stream::StreamPath> {
        let shard = upload_id
            .get(..2)
            .filter(|value| value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
        acyclic_stream::StreamPath::new(format!(
            "fs/s3-multipart/registry/{}",
            shard.to_ascii_lowercase()
        ))
        .map_err(|_| s3s::s3_error!(InvalidRequest))
    }

    fn multipart_cas_operation(
        upload_id: &str,
        phase: &[u8],
        operation: IdempotencyKey,
        expected_tail: u64,
    ) -> S3Result<acyclic_stream::IdempotencyKey> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(MULTIPART_DOMAIN);
        hasher.update(phase);
        hasher.update(upload_id.as_bytes());
        hasher.update(&operation.into_bytes());
        hasher.update(&expected_tail.to_le_bytes());
        acyclic_stream::IdempotencyKey::new(Bytes::copy_from_slice(hasher.finalize().as_bytes()))
            .map_err(|_| s3s::s3_error!(InvalidRequest))
    }

    async fn multipart_snapshot(
        &self,
        workspace: &Workspace<A, O>,
        upload_id: &str,
    ) -> S3Result<MultipartSnapshot> {
        let path = Self::multipart_path(workspace, upload_id)?;
        let provider = workspace.volume.fs.authority().stream_provider();
        read_multipart_snapshot(provider.as_ref(), path, self.limits).await
    }

    async fn multipart_append(
        &self,
        workspace: &Workspace<A, O>,
        upload_id: &str,
        expected_tail: u64,
        record: Bytes,
        operation: acyclic_stream::IdempotencyKey,
    ) -> S3Result<bool> {
        let provider = workspace.volume.fs.authority().stream_provider();
        match provider
            .append(acyclic_stream::AppendRequest {
                path: Self::multipart_path(workspace, upload_id)?,
                records: vec![record],
                if_tail: Some(expected_tail),
                idempotency_key: Some(operation),
            })
            .await
            .map_err(stream_write_error)?
        {
            acyclic_stream::AppendOutcome::Committed(_) => Ok(true),
            acyclic_stream::AppendOutcome::TailConflict { .. } => Ok(false),
        }
    }
}

#[async_trait]
impl<R, A, O> S3 for FilesystemS3Adapter<R, A, O>
where
    R: FilesystemS3Resolver<A, O>,
    A: StreamBackedAuthority + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn create_multipart_upload(
        &self,
        request: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        validate_http_key(&request.input.key, self.limits.maximum_key_bytes)?;
        reject_create_multipart_extensions(&request.input)?;
        let operation = request_operation(&request)?;
        let upload_id = Self::multipart_upload_id(&workspace, operation);
        let path = Self::multipart_path(&workspace, &upload_id)?;
        let provider = workspace.volume.fs.authority().stream_provider();
        let record = encode_multipart_created(
            &request.input.key,
            self.limits.maximum_multipart_parts,
            self.limits.maximum_multipart_part_bytes,
            self.limits.minimum_multipart_part_bytes,
        )?;
        let registry = Self::multipart_registry_path(&upload_id)?;
        for _ in 0..self.limits.maximum_multipart_cas_retries {
            let (registry_condition, registry_tail) = match provider.tail(registry.clone()).await {
                Ok(tail) => (
                    acyclic_stream::CommitCondition::Tail {
                        path: registry.clone(),
                        expected: tail,
                    },
                    tail,
                ),
                Err(acyclic_stream::StreamError::NotFound) => (
                    acyclic_stream::CommitCondition::Absent {
                        path: registry.clone(),
                    },
                    0,
                ),
                Err(error) => return Err(stream_read_error(error)),
            };
            let attempt =
                Self::multipart_cas_operation(&upload_id, b"create\0", operation, registry_tail)?;
            match provider
                .commit(acyclic_stream::CommitRequest {
                    conditions: vec![
                        registry_condition,
                        acyclic_stream::CommitCondition::Absent { path: path.clone() },
                    ],
                    mutations: vec![
                        acyclic_stream::CommitMutation::Append {
                            path: registry.clone(),
                            records: vec![Bytes::copy_from_slice(path.as_str().as_bytes())],
                        },
                        acyclic_stream::CommitMutation::Append {
                            path: path.clone(),
                            records: vec![record.clone()],
                        },
                    ],
                    idempotency_key: attempt,
                })
                .await
                .map_err(stream_write_error)?
            {
                acyclic_stream::CommitOutcome::Committed(_) => break,
                acyclic_stream::CommitOutcome::Conflict(_) => {
                    if let Ok(snapshot) = self.multipart_snapshot(&workspace, &upload_id).await {
                        if snapshot.key != request.input.key {
                            return Err(s3s::s3_error!(InvalidRequest));
                        }
                        break;
                    }
                    continue;
                }
            }
        }
        if provider.tail(path.clone()).await.is_err() {
            return Err(s3s::s3_error!(SlowDown));
        }
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(request.input.bucket),
            key: Some(request.input.key),
            upload_id: Some(upload_id),
            ..CreateMultipartUploadOutput::default()
        }))
    }

    async fn upload_part(
        &self,
        mut request: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        reject_upload_part_extensions(&request.input)?;
        let part_number = u32::try_from(request.input.part_number)
            .map_err(|_| s3s::s3_error!(InvalidArgument))?;
        if part_number == 0 || part_number > self.limits.maximum_multipart_parts {
            return Err(s3s::s3_error!(InvalidArgument));
        }
        let operation = request_operation(&request)?;
        let transaction = workspace
            .begin_transaction(operation)
            .await
            .map_err(|error| s3_error(S3Error::Workspace(error)))?;
        let mut source = StreamingBodySource::new(request.input.body.take());
        let staged = transaction
            .stage_content(&mut source, self.limits.maximum_multipart_part_bytes)
            .await
            .map_err(|error| s3_error(S3Error::Workspace(error)))?;
        if request
            .input
            .content_length
            .is_some_and(|length| u64::try_from(length).ok() != Some(staged.logical_bytes()))
        {
            return Err(s3s::s3_error!(IncompleteBody));
        }
        let etag_value = format!("\"{}\"", hex::encode(staged.root().digest.as_bytes()));
        for _ in 0..self.limits.maximum_multipart_cas_retries {
            let snapshot = self
                .multipart_snapshot(&workspace, &request.input.upload_id)
                .await?;
            if snapshot.key != request.input.key || snapshot.terminal.is_some() {
                return Err(s3s::s3_error!(NoSuchUpload));
            }
            if let Some((existing_number, existing_etag)) =
                snapshot.operations.get(&operation.into_bytes())
            {
                if *existing_number != part_number || existing_etag != &etag_value {
                    return Err(s3s::s3_error!(InvalidRequest));
                }
                return Ok(S3Response::new(UploadPartOutput {
                    e_tag: Some(etag(existing_etag)?),
                    ..UploadPartOutput::default()
                }));
            }
            if snapshot.tail.saturating_add(2) >= u64::from(self.limits.maximum_multipart_records) {
                return Err(s3s::s3_error!(SlowDown));
            }
            if self
                .multipart_append(
                    &workspace,
                    &request.input.upload_id,
                    snapshot.tail,
                    encode_multipart_part(part_number, staged, operation, &etag_value),
                    Self::multipart_cas_operation(
                        &request.input.upload_id,
                        b"part\0",
                        operation,
                        snapshot.tail,
                    )?,
                )
                .await?
            {
                return Ok(S3Response::new(UploadPartOutput {
                    e_tag: Some(etag(&etag_value)?),
                    ..UploadPartOutput::default()
                }));
            }
        }
        return Err(s3s::s3_error!(SlowDown));
    }

    async fn list_parts(
        &self,
        request: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, false)
            .await?
            .workspace;
        let snapshot = self
            .multipart_snapshot(&workspace, &request.input.upload_id)
            .await?;
        if snapshot.key != request.input.key
            || matches!(snapshot.terminal, Some(MultipartTerminal::Aborted))
        {
            return Err(s3s::s3_error!(NoSuchUpload));
        }
        let marker = request.input.part_number_marker.unwrap_or(0);
        let maximum = request.input.max_parts.unwrap_or(1_000).clamp(
            0,
            i32::try_from(self.limits.maximum_multipart_parts).unwrap_or(i32::MAX),
        );
        let maximum = usize::try_from(maximum).map_err(|_| s3s::s3_error!(InvalidArgument))?;
        let mut selected = snapshot.parts.iter().filter(|(number, _)| {
            i32::try_from(**number)
                .ok()
                .is_some_and(|value| value > marker)
        });
        let values = selected
            .by_ref()
            .take(maximum)
            .map(|(number, part)| {
                Ok(Part {
                    e_tag: Some(etag(&part.etag)?),
                    part_number: Some(
                        i32::try_from(*number).map_err(|_| s3s::s3_error!(InternalError))?,
                    ),
                    size: Some(
                        i64::try_from(part.staged.logical_bytes())
                            .map_err(|_| s3s::s3_error!(InternalError))?,
                    ),
                    ..Part::default()
                })
            })
            .collect::<S3Result<Vec<_>>>()?;
        let truncated = selected.next().is_some();
        let next_marker = truncated
            .then(|| values.last().and_then(|part| part.part_number))
            .flatten();
        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(request.input.bucket),
            key: Some(request.input.key),
            upload_id: Some(request.input.upload_id),
            max_parts: Some(i32::try_from(maximum).map_err(|_| s3s::s3_error!(InternalError))?),
            part_number_marker: Some(marker),
            next_part_number_marker: next_marker,
            is_truncated: Some(truncated),
            parts: (!values.is_empty()).then_some(values),
            ..ListPartsOutput::default()
        }))
    }

    async fn complete_multipart_upload(
        &self,
        request: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        reject_complete_multipart_extensions(&request.input)?;
        let operation = request_operation(&request)?;
        let requested = completed_parts(request.input.multipart_upload.as_ref())?;
        let mut admitted = None;
        let mut terminalized = false;
        for _ in 0..self.limits.maximum_multipart_cas_retries {
            let mut snapshot = self
                .multipart_snapshot(&workspace, &request.input.upload_id)
                .await?;
            if snapshot.key != request.input.key {
                return Err(s3s::s3_error!(NoSuchUpload));
            }
            match &snapshot.terminal {
                Some(MultipartTerminal::Completed(generation)) => {
                    return Ok(S3Response::new(CompleteMultipartUploadOutput {
                        bucket: Some(request.input.bucket),
                        key: Some(request.input.key.clone()),
                        e_tag: Some(etag_for_generation(*generation, &request.input.key)?),
                        ..CompleteMultipartUploadOutput::default()
                    }));
                }
                Some(MultipartTerminal::Completing(existing)) if existing == &requested => {
                    admitted = Some(snapshot);
                    break;
                }
                Some(_) => return Err(s3s::s3_error!(NoSuchUpload)),
                None => {
                    validate_completed_parts(
                        &snapshot.parts,
                        &requested,
                        snapshot.minimum_part_bytes,
                    )?;
                    if self
                        .multipart_append(
                            &workspace,
                            &request.input.upload_id,
                            snapshot.tail,
                            encode_multipart_completing(&requested)?,
                            Self::multipart_cas_operation(
                                &request.input.upload_id,
                                b"complete-intent\0",
                                operation,
                                snapshot.tail,
                            )?,
                        )
                        .await?
                    {
                        snapshot.tail += 1;
                        snapshot.terminal = Some(MultipartTerminal::Completing(requested.clone()));
                        admitted = Some(snapshot);
                        break;
                    }
                }
            }
        }
        let snapshot = admitted.ok_or_else(|| s3s::s3_error!(SlowDown))?;
        let mut transaction = workspace
            .begin_transaction(operation)
            .await
            .map_err(|error| s3_error(S3Error::Workspace(error)))?;
        let staged = requested
            .iter()
            .map(|(number, _)| {
                snapshot
                    .parts
                    .get(number)
                    .map(|part| part.staged)
                    .ok_or_else(|| s3s::s3_error!(InvalidPart))
            })
            .collect::<S3Result<Vec<_>>>()?;
        crate::s3::create_parent_directories(&mut transaction, &request.input.key)
            .await
            .map_err(s3_error)?;
        transaction
            .write_staged(&format!("/{}", request.input.key), &staged)
            .await
            .map_err(|error| s3_error(S3Error::Workspace(error)))?;
        let generation = committed_generation(
            transaction
                .commit()
                .await
                .map_err(|error| s3_error(S3Error::Workspace(error)))?,
        )?;
        for _ in 0..self.limits.maximum_multipart_cas_retries {
            let latest = self
                .multipart_snapshot(&workspace, &request.input.upload_id)
                .await?;
            match latest.terminal {
                Some(MultipartTerminal::Completed(existing)) if existing == generation.id() => {
                    terminalized = true;
                    break;
                }
                Some(MultipartTerminal::Completing(ref existing)) if existing == &requested => {
                    if self
                        .multipart_append(
                            &workspace,
                            &request.input.upload_id,
                            latest.tail,
                            encode_multipart_completed(generation.id()),
                            Self::multipart_cas_operation(
                                &request.input.upload_id,
                                b"complete-terminal\0",
                                operation,
                                latest.tail,
                            )?,
                        )
                        .await?
                    {
                        terminalized = true;
                        break;
                    }
                }
                _ => return Err(s3s::s3_error!(InternalError)),
            }
        }
        if !terminalized {
            return Err(s3s::s3_error!(SlowDown));
        }
        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(request.input.bucket),
            key: Some(request.input.key.clone()),
            e_tag: Some(etag_for_generation(generation.id(), &request.input.key)?),
            ..CompleteMultipartUploadOutput::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        request: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        if request.input.if_match_initiated_time.is_some() {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let operation = request_operation(&request)?;
        for _ in 0..self.limits.maximum_multipart_cas_retries {
            let snapshot = self
                .multipart_snapshot(&workspace, &request.input.upload_id)
                .await?;
            if snapshot.key != request.input.key {
                return Err(s3s::s3_error!(NoSuchUpload));
            }
            match snapshot.terminal {
                Some(MultipartTerminal::Aborted) => {
                    return Ok(S3Response::new(AbortMultipartUploadOutput::default()));
                }
                Some(_) => return Err(s3s::s3_error!(NoSuchUpload)),
                None => {
                    if self
                        .multipart_append(
                            &workspace,
                            &request.input.upload_id,
                            snapshot.tail,
                            encode_multipart_aborted(),
                            Self::multipart_cas_operation(
                                &request.input.upload_id,
                                b"abort\0",
                                operation,
                                snapshot.tail,
                            )?,
                        )
                        .await?
                    {
                        return Ok(S3Response::new(AbortMultipartUploadOutput::default()));
                    }
                }
            }
        }
        Err(s3s::s3_error!(SlowDown))
    }

    async fn head_bucket(
        &self,
        request: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.scoped_principal(&request, &request.input.bucket, false)
            .await?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn get_bucket_location(
        &self,
        request: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.scoped_principal(&request, &request.input.bucket, false)
            .await?;
        Ok(S3Response::new(GetBucketLocationOutput::default()))
    }

    async fn list_objects_v2(
        &self,
        request: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let principal = self
            .scoped_principal(&request, &request.input.bucket, false)
            .await?;
        if request.input.encoding_type.is_some() || request.input.fetch_owner == Some(true) {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let maximum_keys = request.input.max_keys.unwrap_or(1_000);
        let maximum_keys = u32::try_from(maximum_keys)
            .map_err(|_| s3s::s3_error!(InvalidArgument))?
            .min(self.limits.maximum_list_keys);
        let delimiter = match request.input.delimiter.as_deref() {
            None | Some("") => None,
            Some("/") => Some('/'),
            Some(_) => return Err(s3s::s3_error!(NotImplemented)),
        };
        let continuation = request
            .input
            .continuation_token
            .as_deref()
            .map(|token| S3ListCursor::decode(token, self.limits.maximum_key_bytes))
            .transpose()
            .map_err(s3_error)?;
        let options = S3ListOptions {
            prefix: request.input.prefix.clone().unwrap_or_default(),
            delimiter,
            start_after: request.input.start_after.clone(),
            continuation,
            maximum_keys,
            maximum_entries_examined: self.limits.maximum_list_entries_examined,
        };
        let view = principal.workspace.s3();
        let page = match principal.generation {
            Some(id) => {
                let generation = principal
                    .workspace
                    .generation(id)
                    .await
                    .map_err(|error| s3_error(S3Error::Workspace(error)))?;
                view.list_objects_at(&generation, options).await
            }
            None => view.list_objects(options).await,
        }
        .map_err(s3_error)?;
        let key_count = page
            .objects
            .len()
            .saturating_add(page.common_prefixes.len());
        let contents = page
            .objects
            .into_iter()
            .map(|value| {
                Ok(Object {
                    e_tag: Some(etag(&value.etag)?),
                    key: Some(value.key),
                    size: Some(
                        i64::try_from(value.content_length)
                            .map_err(|_| s3s::s3_error!(InternalError))?,
                    ),
                    ..Object::default()
                })
            })
            .collect::<S3Result<Vec<_>>>()?;
        let common_prefixes = page
            .common_prefixes
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(prefix),
            })
            .collect::<Vec<_>>();
        let next = page.next_continuation.map(|cursor| cursor.encode());
        Ok(S3Response::new(ListObjectsV2Output {
            common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
            contents: (!contents.is_empty()).then_some(contents),
            continuation_token: request.input.continuation_token,
            delimiter: request.input.delimiter,
            is_truncated: Some(next.is_some()),
            key_count: Some(i32::try_from(key_count).map_err(|_| s3s::s3_error!(InternalError))?),
            max_keys: Some(i32::try_from(maximum_keys).map_err(|_| s3s::s3_error!(InternalError))?),
            name: Some(request.input.bucket),
            next_continuation_token: next,
            prefix: request.input.prefix,
            start_after: request.input.start_after,
            ..ListObjectsV2Output::default()
        }))
    }

    async fn head_object(
        &self,
        request: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let principal = self
            .scoped_principal(&request, &request.input.bucket, false)
            .await?;
        let view = principal.workspace.s3();
        let head = match principal.generation {
            Some(id) => {
                let generation = principal
                    .workspace
                    .generation(id)
                    .await
                    .map_err(|error| s3_error(S3Error::Workspace(error)))?;
                view.head_object_at(&generation, &request.input.key).await
            }
            None => view.head_object(&request.input.key).await,
        }
        .map_err(s3_error)?;
        check_conditions(
            &head.etag,
            request.input.if_match.as_ref(),
            request.input.if_none_match.as_ref(),
        )?;
        let selected = select_range(head.content_length, request.input.range.as_ref())?;
        Ok(S3Response::new(HeadObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            content_length: Some(
                i64::try_from(selected.length).map_err(|_| s3s::s3_error!(InternalError))?,
            ),
            content_range: selected.content_range,
            e_tag: Some(etag(&head.etag)?),
            ..HeadObjectOutput::default()
        }))
    }

    async fn get_object(
        &self,
        request: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let principal = self
            .scoped_principal(&request, &request.input.bucket, false)
            .await?;
        let view = principal.workspace.s3();
        let generation = match principal.generation {
            Some(id) => principal
                .workspace
                .generation(id)
                .await
                .map_err(|error| s3_error(S3Error::Workspace(error)))?,
            None => principal
                .workspace
                .head()
                .await
                .map_err(|error| s3_error(S3Error::Workspace(error)))?,
        };
        let head = view
            .head_object_at(&generation, &request.input.key)
            .await
            .map_err(s3_error)?;
        check_conditions(
            &head.etag,
            request.input.if_match.as_ref(),
            request.input.if_none_match.as_ref(),
        )?;
        let selected = select_range(head.content_length, request.input.range.as_ref())?;
        let body = StreamingBlob::wrap(sync_wrapper::SyncStream::new(stream::try_unfold(
            (
                view,
                generation,
                request.input.key.clone(),
                selected.offset,
                selected.length,
                self.limits.response_chunk_bytes,
            ),
            |(view, generation, key, offset, remaining, chunk_bytes)| async move {
                if remaining == 0 {
                    return Ok(None);
                }
                let length = remaining.min(chunk_bytes);
                let bytes = view
                    .get_object_range_at(&generation, &key, ByteRange { offset, length })
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                if u64::try_from(bytes.len()).ok() != Some(length) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "canonical range reader returned a short chunk",
                    ));
                }
                Ok(Some((
                    bytes,
                    (
                        view,
                        generation,
                        key,
                        offset.saturating_add(length),
                        remaining - length,
                        chunk_bytes,
                    ),
                )))
            },
        )));
        Ok(S3Response::new(GetObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            body: Some(body),
            content_length: Some(
                i64::try_from(selected.length).map_err(|_| s3s::s3_error!(InternalError))?,
            ),
            content_range: selected.content_range,
            e_tag: Some(etag(&head.etag)?),
            ..GetObjectOutput::default()
        }))
    }

    async fn put_object(
        &self,
        mut request: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        reject_put_extensions(&request.input)?;
        let operation = request_operation(&request)?;
        let mut transaction = workspace
            .begin_transaction(operation)
            .await
            .map_err(|error| s3_error(S3Error::Workspace(error)))?;
        let mut source = StreamingBodySource::new(request.input.body.take());
        let staged = transaction
            .stage_content(&mut source, self.limits.maximum_request_bytes)
            .await
            .map_err(|error| s3_error(S3Error::Workspace(error)))?;
        if request
            .input
            .content_length
            .is_some_and(|length| u64::try_from(length).ok() != Some(staged.logical_bytes()))
        {
            return Err(s3s::s3_error!(IncompleteBody));
        }
        crate::s3::create_parent_directories(&mut transaction, &request.input.key)
            .await
            .map_err(s3_error)?;
        transaction
            .write_staged(&format!("/{}", request.input.key), &[staged])
            .await
            .map_err(|error| s3_error(S3Error::Workspace(error)))?;
        let commit = transaction
            .commit()
            .await
            .map_err(|error| s3_error(S3Error::Workspace(error)))?;
        let generation = committed_generation(commit)?;
        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(etag_for_generation(generation.id(), &request.input.key)?),
            ..PutObjectOutput::default()
        }))
    }

    async fn delete_object(
        &self,
        request: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        if request.input.version_id.is_some() {
            return Err(s3s::s3_error!(NotImplemented));
        }
        workspace
            .s3()
            .delete_object(&request.input.key, request_operation(&request)?)
            .await
            .map_err(s3_error)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_objects(
        &self,
        request: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        let keys = request
            .input
            .delete
            .objects
            .iter()
            .map(|object| {
                if object.version_id.is_some() {
                    return Err(s3s::s3_error!(NotImplemented));
                }
                Ok(object.key.clone())
            })
            .collect::<S3Result<Vec<_>>>()?;
        workspace
            .s3()
            .delete_objects(&keys, request_operation(&request)?)
            .await
            .map_err(s3_error)?;
        let deleted = (!request.input.delete.quiet.unwrap_or(false)).then(|| {
            keys.into_iter()
                .map(|key| DeletedObject {
                    key: Some(key),
                    ..DeletedObject::default()
                })
                .collect()
        });
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted,
            ..DeleteObjectsOutput::default()
        }))
    }

    async fn copy_object(
        &self,
        request: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        let (source_bucket, source_key) = match &request.input.copy_source {
            s3s::dto::CopySource::Bucket {
                bucket,
                key,
                version_id: None,
            } => (bucket.as_ref(), key.as_ref()),
            s3s::dto::CopySource::Bucket {
                version_id: Some(_),
                ..
            }
            | s3s::dto::CopySource::AccessPoint { .. } => {
                return Err(s3s::s3_error!(NotImplemented));
            }
        };
        if source_bucket != request.input.bucket {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let commit = workspace
            .s3()
            .copy_object(source_key, &request.input.key, request_operation(&request)?)
            .await
            .map_err(s3_error)?;
        let generation = committed_generation(commit)?;
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(etag_for_generation(generation.id(), &request.input.key)?),
                ..CopyObjectResult::default()
            }),
            ..CopyObjectOutput::default()
        }))
    }
}

struct SelectedRange {
    offset: u64,
    length: u64,
    content_range: Option<String>,
}

fn select_range(length: u64, requested: Option<&Range>) -> S3Result<SelectedRange> {
    let Some(requested) = requested else {
        return Ok(SelectedRange {
            offset: 0,
            length,
            content_range: None,
        });
    };
    if length == 0 {
        return Err(s3s::s3_error!(InvalidRange));
    }
    let range = requested.check(length)?;
    Ok(SelectedRange {
        offset: range.start,
        length: range.end - range.start,
        content_range: Some(format!(
            "bytes {}-{}/{}",
            range.start,
            range.end - 1,
            length
        )),
    })
}

struct StreamingBodySource {
    body: Option<StreamingBlob>,
    pending: Bytes,
}

impl StreamingBodySource {
    fn new(body: Option<StreamingBlob>) -> Self {
        Self {
            body,
            pending: Bytes::new(),
        }
    }
}

impl AsyncBlobSource for StreamingBodySource {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        if cancellation.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "S3 request body cancelled",
            ));
        }
        while self.pending.is_empty() {
            let Some(body) = self.body.as_mut() else {
                return Ok(0);
            };
            match body.next().await {
                Some(Ok(chunk)) if !chunk.is_empty() => self.pending = chunk,
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(std::io::Error::other(error)),
                None => {
                    self.body = None;
                    return Ok(0);
                }
            }
        }
        let count = destination.len().min(self.pending.len());
        destination[..count].copy_from_slice(&self.pending[..count]);
        self.pending.advance(count);
        Ok(count)
    }
}

fn validate_http_key(key: &str, maximum: u32) -> S3Result {
    if key.is_empty() || key.len() > maximum as usize {
        return Err(s3s::s3_error!(InvalidArgument));
    }
    Ok(())
}

fn encode_multipart_created(
    key: &str,
    maximum_parts: u32,
    maximum_part_bytes: u64,
    minimum_part_bytes: u64,
) -> S3Result<Bytes> {
    let key_length = u32::try_from(key.len()).map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let mut output = Vec::with_capacity(2 + 4 + key.len() + 4 + 8 + 8);
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 1]);
    output.extend_from_slice(&key_length.to_le_bytes());
    output.extend_from_slice(key.as_bytes());
    output.extend_from_slice(&maximum_parts.to_le_bytes());
    output.extend_from_slice(&maximum_part_bytes.to_le_bytes());
    output.extend_from_slice(&minimum_part_bytes.to_le_bytes());
    Ok(Bytes::from(output))
}

fn encode_multipart_part(
    part_number: u32,
    staged: StagedContent,
    operation: IdempotencyKey,
    etag_value: &str,
) -> Bytes {
    let mut output = Vec::with_capacity(2 + 4 + 1 + 32 + 8 + 16 + 32);
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 2]);
    output.extend_from_slice(&part_number.to_le_bytes());
    output.push(staged.root().kind.canonical_tag());
    output.extend_from_slice(staged.root().digest.as_bytes());
    output.extend_from_slice(&staged.logical_bytes().to_le_bytes());
    output.extend_from_slice(&operation.into_bytes());
    let mut etag_digest = [0_u8; 32];
    let unquoted = etag_value.trim_matches('"');
    if hex::decode_to_slice(unquoted, &mut etag_digest).is_err() {
        etag_digest = *blake3::hash(etag_value.as_bytes()).as_bytes();
    }
    output.extend_from_slice(&etag_digest);
    Bytes::from(output)
}

fn encode_multipart_completing(parts: &[(u32, String)]) -> S3Result<Bytes> {
    let count = u32::try_from(parts.len()).map_err(|_| s3s::s3_error!(InvalidRequest))?;
    let mut output = Vec::with_capacity(6 + parts.len().saturating_mul(36));
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 3]);
    output.extend_from_slice(&count.to_le_bytes());
    for (number, etag_value) in parts {
        output.extend_from_slice(&number.to_le_bytes());
        let mut digest = [0_u8; 32];
        hex::decode_to_slice(etag_value.trim_matches('"'), &mut digest)
            .map_err(|_| s3s::s3_error!(InvalidPart))?;
        output.extend_from_slice(&digest);
    }
    Ok(Bytes::from(output))
}

fn encode_multipart_completed(generation: crate::GenerationId) -> Bytes {
    let mut output = Vec::with_capacity(34);
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 4]);
    output.extend_from_slice(generation.digest().as_bytes());
    Bytes::from(output)
}

fn encode_multipart_aborted() -> Bytes {
    Bytes::from_static(&[MULTIPART_RECORD_VERSION, 5])
}

async fn read_multipart_snapshot<P: acyclic_stream::StreamProvider>(
    provider: &P,
    path: acyclic_stream::StreamPath,
    limits: FilesystemS3Limits,
) -> S3Result<MultipartSnapshot> {
    let mut work = crate::WorkCounters {
        backend_read_operations: 1,
        ..crate::WorkCounters::default()
    };
    let tail = provider
        .tail(path.clone())
        .await
        .map_err(stream_read_error)?;
    let maximum_records = u64::from(limits.maximum_multipart_records);
    if tail == 0 || tail > maximum_records {
        return Err(s3s::s3_error!(InvalidRequest));
    }
    let mut records = Vec::new();
    let mut from = 0_u64;
    while from < tail {
        let remaining = tail - from;
        let limit = u32::try_from(remaining.min(acyclic_stream::MAX_ITEMS as u64))
            .map_err(|_| s3s::s3_error!(InternalError))?;
        let mut page = provider
            .read(acyclic_stream::ReadRequest {
                path: path.clone(),
                from,
                limit,
            })
            .await
            .map_err(stream_read_error)?;
        work.backend_read_operations = work
            .backend_read_operations
            .checked_add(1)
            .ok_or_else(|| s3s::s3_error!(InternalError))?;
        let before = records.len();
        while let Some(record) = page.next().await {
            let record = record.map_err(stream_read_error)?;
            if record.sequence != from + u64::try_from(records.len() - before).unwrap_or(u64::MAX) {
                return Err(s3s::s3_error!(InternalError));
            }
            work.authority_records_read = work
                .authority_records_read
                .checked_add(1)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
            work.authority_bytes_read = work
                .authority_bytes_read
                .checked_add(record.value.len() as u64)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
            work.items_examined = work
                .items_examined
                .checked_add(1)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
            records.push(record.value);
        }
        let received = records.len() - before;
        if received == 0 {
            return Err(s3s::s3_error!(InternalError));
        }
        from = from
            .checked_add(u64::try_from(received).map_err(|_| s3s::s3_error!(InternalError))?)
            .ok_or_else(|| s3s::s3_error!(InternalError))?;
    }
    let mut snapshot = decode_multipart(records, tail, limits)?;
    snapshot.work = work;
    Ok(snapshot)
}

fn decode_multipart(
    records: Vec<Bytes>,
    tail: u64,
    limits: FilesystemS3Limits,
) -> S3Result<MultipartSnapshot> {
    let mut records = records.into_iter();
    let created = records.next().ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
    let mut cursor = RecordCursor::new(&created);
    cursor.header(1)?;
    let key_length = cursor.u32()?;
    if key_length == 0 || key_length > limits.maximum_key_bytes {
        return Err(s3s::s3_error!(InvalidRequest));
    }
    let key = std::str::from_utf8(cursor.take(key_length as usize)?)
        .map_err(|_| s3s::s3_error!(InvalidRequest))?
        .to_owned();
    let maximum_parts = cursor.u32()?;
    let maximum_part_bytes = cursor.u64()?;
    let minimum_part_bytes = cursor.u64()?;
    cursor.finish()?;
    if maximum_parts == 0
        || maximum_parts > limits.maximum_multipart_parts
        || maximum_part_bytes == 0
        || maximum_part_bytes > limits.maximum_multipart_part_bytes
        || minimum_part_bytes > maximum_part_bytes
    {
        return Err(s3s::s3_error!(InvalidRequest));
    }
    let mut parts = BTreeMap::new();
    let mut operations = BTreeMap::new();
    let mut terminal = None;
    for record in records {
        let mut cursor = RecordCursor::new(&record);
        let tag = cursor.any_header()?;
        if terminal.is_some() && tag != 4 {
            return Err(s3s::s3_error!(InvalidRequest));
        }
        match tag {
            2 => {
                let part_number = cursor.u32()?;
                let kind = ObjectKind::from_canonical_tag(cursor.u8()?)
                    .map_err(|_| s3s::s3_error!(InvalidRequest))?;
                let digest = Digest::from_bytes(cursor.array32()?);
                let logical_bytes = cursor.u64()?;
                let operation = cursor.array16()?;
                let etag_digest = cursor.array32()?;
                cursor.finish()?;
                if part_number == 0
                    || part_number > maximum_parts
                    || logical_bytes > maximum_part_bytes
                {
                    return Err(s3s::s3_error!(InvalidRequest));
                }
                let etag = format!("\"{}\"", hex::encode(etag_digest));
                if let Some((prior_number, prior_etag)) = operations.get(&operation)
                    && (*prior_number != part_number || prior_etag != &etag)
                {
                    return Err(s3s::s3_error!(InvalidRequest));
                }
                operations.insert(operation, (part_number, etag.clone()));
                parts.insert(
                    part_number,
                    MultipartPart {
                        staged: StagedContent::from_canonical_parts(
                            ObjectId { kind, digest },
                            logical_bytes,
                        ),
                        etag,
                    },
                );
            }
            3 => {
                let count = cursor.u32()?;
                if count == 0 || count > maximum_parts {
                    return Err(s3s::s3_error!(InvalidRequest));
                }
                let mut selected = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    selected.push((
                        cursor.u32()?,
                        format!("\"{}\"", hex::encode(cursor.array32()?)),
                    ));
                }
                cursor.finish()?;
                terminal = Some(MultipartTerminal::Completing(selected));
            }
            4 => {
                let generation = crate::GenerationId::new(Digest::from_bytes(cursor.array32()?));
                cursor.finish()?;
                if !matches!(terminal, Some(MultipartTerminal::Completing(_))) {
                    return Err(s3s::s3_error!(InvalidRequest));
                }
                terminal = Some(MultipartTerminal::Completed(generation));
            }
            5 => {
                cursor.finish()?;
                terminal = Some(MultipartTerminal::Aborted);
            }
            _ => return Err(s3s::s3_error!(InvalidRequest)),
        }
    }
    Ok(MultipartSnapshot {
        key,
        tail,
        minimum_part_bytes,
        parts,
        operations,
        terminal,
        work: crate::WorkCounters::default(),
    })
}

struct RecordCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> RecordCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> S3Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> S3Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> S3Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| s3s::s3_error!(InvalidRequest))?,
        ))
    }

    fn u64(&mut self) -> S3Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| s3s::s3_error!(InvalidRequest))?,
        ))
    }

    fn array32(&mut self) -> S3Result<[u8; 32]> {
        self.take(32)?
            .try_into()
            .map_err(|_| s3s::s3_error!(InvalidRequest))
    }

    fn array16(&mut self) -> S3Result<[u8; 16]> {
        self.take(16)?
            .try_into()
            .map_err(|_| s3s::s3_error!(InvalidRequest))
    }

    fn any_header(&mut self) -> S3Result<u8> {
        if self.u8()? != MULTIPART_RECORD_VERSION {
            return Err(s3s::s3_error!(InvalidRequest));
        }
        self.u8()
    }

    fn header(&mut self, tag: u8) -> S3Result {
        if self.any_header()? != tag {
            return Err(s3s::s3_error!(InvalidRequest));
        }
        Ok(())
    }

    fn finish(&self) -> S3Result {
        if self.offset != self.bytes.len() {
            return Err(s3s::s3_error!(InvalidRequest));
        }
        Ok(())
    }
}

fn completed_parts(
    upload: Option<&s3s::dto::CompletedMultipartUpload>,
) -> S3Result<Vec<(u32, String)>> {
    let parts = upload
        .and_then(|upload| upload.parts.as_ref())
        .ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
    if parts.is_empty() {
        return Err(s3s::s3_error!(InvalidRequest));
    }
    parts
        .iter()
        .map(|part: &CompletedPart| {
            let number = part
                .part_number
                .ok_or_else(|| s3s::s3_error!(InvalidPart))?;
            let number = u32::try_from(number).map_err(|_| s3s::s3_error!(InvalidPart))?;
            let etag = part
                .e_tag
                .as_ref()
                .ok_or_else(|| s3s::s3_error!(InvalidPart))?;
            Ok((number, format!("\"{}\"", etag.value())))
        })
        .collect()
}

fn validate_completed_parts(
    available: &BTreeMap<u32, MultipartPart>,
    requested: &[(u32, String)],
    minimum_part_bytes: u64,
) -> S3Result {
    let mut previous = 0;
    for (index, (number, requested_etag)) in requested.iter().enumerate() {
        if *number <= previous {
            return Err(s3s::s3_error!(InvalidPartOrder));
        }
        let part = available
            .get(number)
            .ok_or_else(|| s3s::s3_error!(InvalidPart))?;
        let parsed = etag(requested_etag)?;
        let actual = etag(&part.etag)?;
        if !parsed.strong_cmp(&actual) {
            return Err(s3s::s3_error!(InvalidPart));
        }
        if index + 1 < requested.len() && part.staged.logical_bytes() < minimum_part_bytes {
            return Err(s3s::s3_error!(EntityTooSmall));
        }
        previous = *number;
    }
    Ok(())
}

fn stream_read_error(error: acyclic_stream::StreamError) -> s3s::S3Error {
    match error {
        acyclic_stream::StreamError::NotFound => s3s::s3_error!(NoSuchUpload),
        acyclic_stream::StreamError::AccessDenied => s3s::s3_error!(AccessDenied),
        acyclic_stream::StreamError::Unavailable => s3s::s3_error!(ServiceUnavailable),
        _ => s3s::s3_error!(InternalError),
    }
}

fn stream_write_error(error: acyclic_stream::StreamError) -> s3s::S3Error {
    match error {
        acyclic_stream::StreamError::IdempotencyMismatch => s3s::s3_error!(InvalidRequest),
        acyclic_stream::StreamError::Capacity | acyclic_stream::StreamError::LimitExceeded => {
            s3s::s3_error!(SlowDown)
        }
        other => stream_read_error(other),
    }
}

fn reject_create_multipart_extensions(input: &CreateMultipartUploadInput) -> S3Result {
    if input.acl.is_some()
        || input.bucket_key_enabled.is_some()
        || input.cache_control.is_some()
        || input.checksum_algorithm.is_some()
        || input.checksum_type.is_some()
        || input.content_disposition.is_some()
        || input.content_encoding.is_some()
        || input.content_language.is_some()
        || input.content_type.is_some()
        || input.expected_bucket_owner.is_some()
        || input.expires.is_some()
        || input.grant_full_control.is_some()
        || input.grant_read.is_some()
        || input.grant_read_acp.is_some()
        || input.grant_write_acp.is_some()
        || input.metadata.is_some()
        || input.object_lock_legal_hold_status.is_some()
        || input.object_lock_mode.is_some()
        || input.object_lock_retain_until_date.is_some()
        || input.request_payer.is_some()
        || input.sse_customer_algorithm.is_some()
        || input.sse_customer_key.is_some()
        || input.sse_customer_key_md5.is_some()
        || input.ssekms_encryption_context.is_some()
        || input.ssekms_key_id.is_some()
        || input.server_side_encryption.is_some()
        || input.storage_class.is_some()
        || input.tagging.is_some()
        || input.website_redirect_location.is_some()
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}

fn reject_upload_part_extensions(input: &UploadPartInput) -> S3Result {
    if input.checksum_algorithm.is_some()
        || input.checksum_crc32.is_some()
        || input.checksum_crc32c.is_some()
        || input.checksum_crc64nvme.is_some()
        || input.checksum_sha1.is_some()
        || input.checksum_sha256.is_some()
        || input.content_md5.is_some()
        || input.expected_bucket_owner.is_some()
        || input.request_payer.is_some()
        || input.sse_customer_algorithm.is_some()
        || input.sse_customer_key.is_some()
        || input.sse_customer_key_md5.is_some()
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}

fn reject_complete_multipart_extensions(input: &CompleteMultipartUploadInput) -> S3Result {
    if input.checksum_crc32.is_some()
        || input.checksum_crc32c.is_some()
        || input.checksum_crc64nvme.is_some()
        || input.checksum_sha1.is_some()
        || input.checksum_sha256.is_some()
        || input.checksum_type.is_some()
        || input.expected_bucket_owner.is_some()
        || input.if_match.is_some()
        || input.if_none_match.is_some()
        || input.mpu_object_size.is_some()
        || input.request_payer.is_some()
        || input.sse_customer_algorithm.is_some()
        || input.sse_customer_key.is_some()
        || input.sse_customer_key_md5.is_some()
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}

fn check_conditions(
    etag_value: &str,
    if_match: Option<&s3s::dto::ETagCondition>,
    if_none_match: Option<&s3s::dto::ETagCondition>,
) -> S3Result {
    let current = etag(etag_value)?;
    if let Some(value) = if_match
        && !value.is_any()
        && value
            .as_etag()
            .is_none_or(|value| !value.strong_cmp(&current))
    {
        return Err(s3s::s3_error!(PreconditionFailed));
    }
    if let Some(value) = if_none_match
        && (value.is_any()
            || value
                .as_etag()
                .is_some_and(|value| value.weak_cmp(&current)))
    {
        return Err(s3s::s3_error!(NotModified));
    }
    Ok(())
}

fn committed_generation<A, O>(
    commit: crate::TransactionCommit<A, O>,
) -> S3Result<crate::Generation<A, O>> {
    match commit {
        crate::TransactionCommit::Committed(generation)
        | crate::TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
        crate::TransactionCommit::Conflict { .. }
        | crate::TransactionCommit::Fenced
        | crate::TransactionCommit::IdempotencyConflict => Err(s3s::s3_error!(PreconditionFailed)),
    }
}

fn etag(value: &str) -> S3Result<s3s::dto::ETag> {
    s3s::dto::ETag::parse_http_header(value.as_bytes()).map_err(|_| s3s::s3_error!(InternalError))
}

fn etag_for_generation(generation: crate::GenerationId, key: &str) -> S3Result<s3s::dto::ETag> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-s3-etag-v1\0");
    hasher.update(generation.digest().as_bytes());
    hasher.update(key.as_bytes());
    etag(&format!("\"{}\"", hasher.finalize().to_hex()))
}

fn request_operation<T>(request: &S3Request<T>) -> S3Result<IdempotencyKey> {
    let supplied = request
        .headers
        .get(IDEMPOTENCY_HEADER)
        .or_else(|| request.headers.get(SDK_INVOCATION_HEADER))
        .ok_or_else(|| s3s::s3_error!(InvalidRequest))?
        .to_str()
        .map_err(|_| s3s::s3_error!(InvalidRequest))?;
    if supplied.is_empty() || supplied.len() > 1_024 {
        return Err(s3s::s3_error!(InvalidRequest));
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-s3-operation-v1\0");
    hasher.update(supplied.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ok(IdempotencyKey::from_bytes(bytes))
}

fn reject_put_extensions(input: &PutObjectInput) -> S3Result {
    if input.acl.is_some()
        || input.bucket_key_enabled.is_some()
        || input.cache_control.is_some()
        || input.checksum_algorithm.is_some()
        || input.checksum_crc32.is_some()
        || input.checksum_crc32c.is_some()
        || input.checksum_crc64nvme.is_some()
        || input.checksum_sha1.is_some()
        || input.checksum_sha256.is_some()
        || input.content_disposition.is_some()
        || input.content_encoding.is_some()
        || input.content_language.is_some()
        || input.content_md5.is_some()
        || input.content_type.is_some()
        || input.expected_bucket_owner.is_some()
        || input.expires.is_some()
        || input.grant_full_control.is_some()
        || input.grant_read.is_some()
        || input.grant_read_acp.is_some()
        || input.grant_write_acp.is_some()
        || input.if_match.is_some()
        || input.if_none_match.is_some()
        || input.metadata.is_some()
        || input.object_lock_legal_hold_status.is_some()
        || input.object_lock_mode.is_some()
        || input.object_lock_retain_until_date.is_some()
        || input.request_payer.is_some()
        || input.sse_customer_algorithm.is_some()
        || input.sse_customer_key.is_some()
        || input.sse_customer_key_md5.is_some()
        || input.ssekms_encryption_context.is_some()
        || input.ssekms_key_id.is_some()
        || input.server_side_encryption.is_some()
        || input.storage_class.is_some()
        || input.tagging.is_some()
        || input.website_redirect_location.is_some()
        || input.write_offset_bytes.is_some()
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}

fn s3_error(error: S3Error) -> s3s::S3Error {
    match error {
        S3Error::InvalidKey | S3Error::InvalidRequest(_) | S3Error::InvalidContinuation => {
            s3s::s3_error!(InvalidArgument)
        }
        S3Error::NotFound => s3s::s3_error!(NoSuchKey),
        S3Error::NotRegularFile => s3s::s3_error!(InvalidObjectState),
        S3Error::ListLimit | S3Error::MultipartLimit => s3s::s3_error!(SlowDown),
        S3Error::MissingPart(_) => s3s::s3_error!(InvalidPart),
        S3Error::UnsupportedNamespace => s3s::s3_error!(NotImplemented),
        S3Error::ForeignGeneration => s3s::s3_error!(AccessDenied),
        S3Error::InvalidObject | S3Error::Workspace(_) => s3s::s3_error!(InternalError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
    use futures::TryStreamExt;
    use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
    use s3s::auth::Credentials;
    use s3s::dto::{
        AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletedMultipartUpload,
        CompletedPart, CreateMultipartUploadInput, GetObjectInput, ListObjectsV2Input,
        ListPartsInput, PutObjectInput, UploadPartInput,
    };

    struct Resolver {
        principal: FilesystemS3Principal<MemoryAuthorityBackend, MemoryObjectBackend>,
    }

    #[async_trait]
    impl FilesystemS3Resolver<MemoryAuthorityBackend, MemoryObjectBackend> for Resolver {
        async fn resolve_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
            if access_key != "access" {
                return Err(s3s::s3_error!(InvalidAccessKeyId));
            }
            Ok(self.principal.secret_key.clone())
        }

        async fn resolve_principal(
            &self,
            access_key: &str,
            _session_token: Option<&str>,
            bucket: &str,
        ) -> S3Result<FilesystemS3Principal<MemoryAuthorityBackend, MemoryObjectBackend>> {
            if access_key != "access" || bucket != self.principal.bucket {
                return Err(s3s::s3_error!(InvalidAccessKeyId));
            }
            Ok(self.principal.clone())
        }
    }

    fn request<T>(input: T) -> S3Request<T> {
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_HEADER,
            HeaderValue::from_static("test-operation"),
        );
        S3Request {
            input,
            method: Method::PUT,
            uri: Uri::from_static("http://localhost/bucket/key"),
            headers,
            extensions: Extensions::new(),
            credentials: Some(Credentials {
                access_key: "access".to_owned(),
                secret_key: SecretKey::from("secret"),
            }),
            region: None,
            service: Some("s3".to_owned()),
            trailing_headers: None,
        }
    }

    async fn adapter(
        writable: bool,
    ) -> S3Result<FilesystemS3Adapter<Resolver, MemoryAuthorityBackend, MemoryObjectBackend>> {
        adapter_with_limits(
            writable,
            FilesystemS3Limits {
                minimum_multipart_part_bytes: 1,
                ..FilesystemS3Limits::default()
            },
        )
        .await
    }

    async fn adapter_with_limits(
        writable: bool,
        limits: FilesystemS3Limits,
    ) -> S3Result<FilesystemS3Adapter<Resolver, MemoryAuthorityBackend, MemoryObjectBackend>> {
        let filesystem = Fs::memory();
        let workspace = filesystem
            .create_workspace("s3-http")
            .await
            .map_err(|_| s3s::s3_error!(InternalError))?;
        FilesystemS3Adapter::new(
            Arc::new(Resolver {
                principal: FilesystemS3Principal {
                    secret_key: SecretKey::from("secret"),
                    bucket: "bucket".to_owned(),
                    workspace,
                    generation: None,
                    writable,
                },
            }),
            limits,
        )
    }

    fn body(bytes: Bytes) -> StreamingBlob {
        StreamingBlob::wrap(stream::iter([Ok::<Bytes, std::io::Error>(bytes)]))
    }

    #[tokio::test]
    async fn adapter_uses_one_workspace_and_generation_stable_pagination() -> S3Result {
        let adapter = adapter(true).await?;
        for (operation, key) in [("one", "a"), ("two", "b")] {
            let mut request = request(PutObjectInput {
                body: Some(body(Bytes::copy_from_slice(key.as_bytes()))),
                bucket: "bucket".to_owned(),
                content_length: Some(1),
                key: key.to_owned(),
                ..PutObjectInput::default()
            });
            request.headers.insert(
                IDEMPOTENCY_HEADER,
                HeaderValue::from_str(operation).map_err(|_| s3s::s3_error!(InternalError))?,
            );
            S3::put_object(&adapter, request).await?;
        }
        let first = S3::list_objects_v2(
            &adapter,
            request(ListObjectsV2Input {
                bucket: "bucket".to_owned(),
                max_keys: Some(1),
                ..ListObjectsV2Input::default()
            }),
        )
        .await?
        .output;
        let continuation = first
            .next_continuation_token
            .ok_or_else(|| s3s::s3_error!(InternalError))?;
        let mut later = request(PutObjectInput {
            body: Some(body(Bytes::from_static(b"later"))),
            bucket: "bucket".to_owned(),
            content_length: Some(5),
            key: "aa".to_owned(),
            ..PutObjectInput::default()
        });
        later.headers.insert(
            IDEMPOTENCY_HEADER,
            HeaderValue::from_static("later-operation"),
        );
        S3::put_object(&adapter, later).await?;
        let second = S3::list_objects_v2(
            &adapter,
            request(ListObjectsV2Input {
                bucket: "bucket".to_owned(),
                continuation_token: Some(continuation),
                max_keys: Some(10),
                ..ListObjectsV2Input::default()
            }),
        )
        .await?
        .output;
        let keys = second
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| entry.key)
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["b"]);
        Ok(())
    }

    #[tokio::test]
    async fn read_only_credentials_reject_mutation_before_polling_the_body() -> S3Result {
        let adapter = adapter(false).await?;
        let result = S3::put_object(
            &adapter,
            request(PutObjectInput {
                body: Some(body(Bytes::from_static(b"not-consumed"))),
                bucket: "bucket".to_owned(),
                content_length: Some(12),
                key: "denied".to_owned(),
                ..PutObjectInput::default()
            }),
        )
        .await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn get_streams_bounded_chunks_from_one_immutable_generation() -> S3Result {
        let adapter = adapter_with_limits(
            true,
            FilesystemS3Limits {
                maximum_request_bytes: 16,
                response_chunk_bytes: 2,
                ..FilesystemS3Limits::default()
            },
        )
        .await?;
        let mut put = request(PutObjectInput {
            body: Some(body(Bytes::from_static(b"abcde"))),
            bucket: "bucket".to_owned(),
            content_length: Some(5),
            key: "file".to_owned(),
            ..PutObjectInput::default()
        });
        put.headers
            .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("initial"));
        S3::put_object(&adapter, put).await?;

        let response = S3::get_object(
            &adapter,
            request(GetObjectInput {
                bucket: "bucket".to_owned(),
                key: "file".to_owned(),
                ..GetObjectInput::default()
            }),
        )
        .await?;

        let mut replace = request(PutObjectInput {
            body: Some(body(Bytes::from_static(b"vwxyz"))),
            bucket: "bucket".to_owned(),
            content_length: Some(5),
            key: "file".to_owned(),
            ..PutObjectInput::default()
        });
        replace
            .headers
            .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("replace"));
        S3::put_object(&adapter, replace).await?;

        let chunks = response
            .output
            .body
            .ok_or_else(|| s3s::s3_error!(InternalError))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|_| s3s::s3_error!(InternalError))?;
        assert_eq!(
            chunks,
            vec![Bytes::from("ab"), Bytes::from("cd"), Bytes::from("e")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn generation_scoped_credentials_are_immutable_snapshots() -> S3Result {
        let writable = adapter(true).await?;
        let workspace = writable.resolver.principal.workspace.clone();
        let mut initial = request(PutObjectInput {
            body: Some(body(Bytes::from_static(b"old"))),
            bucket: "bucket".to_owned(),
            content_length: Some(3),
            key: "file".to_owned(),
            ..PutObjectInput::default()
        });
        initial
            .headers
            .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("old"));
        S3::put_object(&writable, initial).await?;
        let generation = workspace
            .head()
            .await
            .map_err(|_| s3s::s3_error!(InternalError))?;

        let mut replacement = request(PutObjectInput {
            body: Some(body(Bytes::from_static(b"new"))),
            bucket: "bucket".to_owned(),
            content_length: Some(3),
            key: "file".to_owned(),
            ..PutObjectInput::default()
        });
        replacement
            .headers
            .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("new"));
        S3::put_object(&writable, replacement).await?;

        let pinned = FilesystemS3Adapter::new(
            Arc::new(Resolver {
                principal: FilesystemS3Principal {
                    secret_key: SecretKey::from("secret"),
                    bucket: "bucket".to_owned(),
                    workspace,
                    generation: Some(generation.id()),
                    writable: false,
                },
            }),
            FilesystemS3Limits::default(),
        )?;
        let response = S3::get_object(
            &pinned,
            request(GetObjectInput {
                bucket: "bucket".to_owned(),
                key: "file".to_owned(),
                ..GetObjectInput::default()
            }),
        )
        .await?;
        let bytes = response
            .output
            .body
            .ok_or_else(|| s3s::s3_error!(InternalError))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|_| s3s::s3_error!(InternalError))?
            .concat();
        assert_eq!(bytes, b"old");
        Ok(())
    }

    #[tokio::test]
    async fn multipart_is_stream_durable_stateless_and_exactly_retryable() -> S3Result {
        let adapter = adapter(true).await?;
        let created = S3::create_multipart_upload(
            &adapter,
            request(CreateMultipartUploadInput {
                bucket: "bucket".to_owned(),
                key: "large/object".to_owned(),
                ..CreateMultipartUploadInput::default()
            }),
        )
        .await?
        .output;
        let upload_id = created
            .upload_id
            .ok_or_else(|| s3s::s3_error!(InternalError))?;

        let mut first_request = request(UploadPartInput {
            body: Some(body(Bytes::from_static(b"first-"))),
            bucket: "bucket".to_owned(),
            content_length: Some(6),
            key: "large/object".to_owned(),
            part_number: 1,
            upload_id: upload_id.clone(),
            ..UploadPartInput::default()
        });
        first_request
            .headers
            .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("part-one"));
        let first_etag = S3::upload_part(&adapter, first_request)
            .await?
            .output
            .e_tag
            .ok_or_else(|| s3s::s3_error!(InternalError))?;

        let mut retry = request(UploadPartInput {
            body: Some(body(Bytes::from_static(b"first-"))),
            bucket: "bucket".to_owned(),
            content_length: Some(6),
            key: "large/object".to_owned(),
            part_number: 1,
            upload_id: upload_id.clone(),
            ..UploadPartInput::default()
        });
        retry
            .headers
            .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("part-one"));
        assert_eq!(
            S3::upload_part(&adapter, retry).await?.output.e_tag,
            Some(first_etag.clone())
        );

        let restarted =
            FilesystemS3Adapter::new(Arc::clone(&adapter.resolver), FilesystemS3Limits::default())?;
        let mut second_request = request(UploadPartInput {
            body: Some(body(Bytes::from_static(b"second"))),
            bucket: "bucket".to_owned(),
            content_length: Some(6),
            key: "large/object".to_owned(),
            part_number: 2,
            upload_id: upload_id.clone(),
            ..UploadPartInput::default()
        });
        second_request
            .headers
            .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("part-two"));
        let second_etag = S3::upload_part(&restarted, second_request)
            .await?
            .output
            .e_tag
            .ok_or_else(|| s3s::s3_error!(InternalError))?;

        let mut bounded_discovery = request(CreateMultipartUploadInput {
            bucket: "bucket".to_owned(),
            key: "bounded-discovery".to_owned(),
            ..CreateMultipartUploadInput::default()
        });
        bounded_discovery.headers.insert(
            IDEMPOTENCY_HEADER,
            HeaderValue::from_static("bounded-discovery"),
        );
        S3::create_multipart_upload(&restarted, bounded_discovery).await?;

        let stream = restarted
            .resolver
            .principal
            .workspace
            .volume
            .fs
            .authority()
            .stream_provider();
        let cancellation = CancellationToken::new();
        assert!(
            active_s3_multipart_objects(
                stream.as_ref(),
                restarted.resolver.principal.workspace.volume.fs.objects(),
                FilesystemS3Limits::default(),
                S3MultipartRetentionLimits {
                    maximum_uploads: 1,
                    ..S3MultipartRetentionLimits::default()
                },
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .is_err()
        );
        assert!(
            active_s3_multipart_objects(
                stream.as_ref(),
                restarted.resolver.principal.workspace.volume.fs.objects(),
                FilesystemS3Limits::default(),
                S3MultipartRetentionLimits {
                    maximum_object_bytes: 1,
                    ..S3MultipartRetentionLimits::default()
                },
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .is_err()
        );
        let active = active_s3_multipart_objects(
            stream.as_ref(),
            restarted.resolver.principal.workspace.volume.fs.objects(),
            FilesystemS3Limits::default(),
            S3MultipartRetentionLimits::default(),
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert!(active.value.len() >= 4);
        assert!(active.work.authority_records_read >= 4);
        assert!(active.work.authority_bytes_read > 0);
        assert!(active.work.backend_read_operations >= 5);
        assert!(active.work.object_bytes_read > 0);

        let listed = S3::list_parts(
            &restarted,
            request(ListPartsInput {
                bucket: "bucket".to_owned(),
                key: "large/object".to_owned(),
                upload_id: upload_id.clone(),
                ..ListPartsInput::default()
            }),
        )
        .await?
        .output;
        assert_eq!(listed.parts.as_ref().map(Vec::len), Some(2));

        let mut complete = request(CompleteMultipartUploadInput {
            bucket: "bucket".to_owned(),
            key: "large/object".to_owned(),
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![
                    CompletedPart {
                        e_tag: Some(first_etag),
                        part_number: Some(1),
                        ..CompletedPart::default()
                    },
                    CompletedPart {
                        e_tag: Some(second_etag),
                        part_number: Some(2),
                        ..CompletedPart::default()
                    },
                ]),
            }),
            upload_id: upload_id.clone(),
            ..CompleteMultipartUploadInput::default()
        });
        complete.headers.insert(
            IDEMPOTENCY_HEADER,
            HeaderValue::from_static("complete-upload"),
        );
        S3::complete_multipart_upload(&restarted, complete).await?;
        assert!(
            active_s3_multipart_objects(
                stream.as_ref(),
                restarted.resolver.principal.workspace.volume.fs.objects(),
                FilesystemS3Limits::default(),
                S3MultipartRetentionLimits::default(),
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value
            .is_empty()
        );

        let response = S3::get_object(
            &restarted,
            request(GetObjectInput {
                bucket: "bucket".to_owned(),
                key: "large/object".to_owned(),
                ..GetObjectInput::default()
            }),
        )
        .await?;
        let bytes = response
            .output
            .body
            .ok_or_else(|| s3s::s3_error!(InternalError))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|_| s3s::s3_error!(InternalError))?
            .concat();
        assert_eq!(bytes, b"first-second");
        Ok(())
    }

    #[tokio::test]
    async fn multipart_abort_is_durable_and_fences_parts() -> S3Result {
        let adapter = adapter(true).await?;
        let upload_id = S3::create_multipart_upload(
            &adapter,
            request(CreateMultipartUploadInput {
                bucket: "bucket".to_owned(),
                key: "aborted".to_owned(),
                ..CreateMultipartUploadInput::default()
            }),
        )
        .await?
        .output
        .upload_id
        .ok_or_else(|| s3s::s3_error!(InternalError))?;
        let mut staged = request(UploadPartInput {
            body: Some(body(Bytes::from_static(b"collectable-after-abort"))),
            bucket: "bucket".to_owned(),
            content_length: Some(23),
            key: "aborted".to_owned(),
            part_number: 1,
            upload_id: upload_id.clone(),
            ..UploadPartInput::default()
        });
        staged
            .headers
            .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("staged"));
        S3::upload_part(&adapter, staged).await?;
        let stream = adapter
            .resolver
            .principal
            .workspace
            .volume
            .fs
            .authority()
            .stream_provider();
        let cancellation = CancellationToken::new();
        assert!(
            !active_s3_multipart_objects(
                stream.as_ref(),
                adapter.resolver.principal.workspace.volume.fs.objects(),
                FilesystemS3Limits::default(),
                S3MultipartRetentionLimits::default(),
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value
            .is_empty()
        );
        let mut abort = request(AbortMultipartUploadInput {
            bucket: "bucket".to_owned(),
            key: "aborted".to_owned(),
            upload_id: upload_id.clone(),
            ..AbortMultipartUploadInput::default()
        });
        abort
            .headers
            .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("abort"));
        S3::abort_multipart_upload(&adapter, abort).await?;
        assert!(
            active_s3_multipart_objects(
                stream.as_ref(),
                adapter.resolver.principal.workspace.volume.fs.objects(),
                FilesystemS3Limits::default(),
                S3MultipartRetentionLimits::default(),
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value
            .is_empty()
        );
        let mut upload = request(UploadPartInput {
            body: Some(body(Bytes::from_static(b"late"))),
            bucket: "bucket".to_owned(),
            content_length: Some(4),
            key: "aborted".to_owned(),
            part_number: 1,
            upload_id,
            ..UploadPartInput::default()
        });
        upload
            .headers
            .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("late"));
        assert!(S3::upload_part(&adapter, upload).await.is_err());
        Ok(())
    }
}
