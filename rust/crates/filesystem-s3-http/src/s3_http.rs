//! AWS S3 HTTP translation over the canonical workspace S3 view.
//!
//! Request parsing, `SigV4` verification, XML, and HTTP framing are delegated to
//! `s3s`. This module contains no namespace or storage implementation: every
//! accepted operation resolves one scoped workspace and invokes [`acyclic_fs::S3Workspace`].

#[cfg(any(test, feature = "test-support"))]
use acyclic_fs::ObjectId;
use acyclic_fs::WorkBudget;
use acyclic_fs::kernel::AsyncBlobSource;
#[cfg(test)]
use acyclic_fs::s3_multipart::read_multipart_snapshot;
#[cfg(feature = "test-support")]
use acyclic_fs::s3_multipart::{
    MULTIPART_RECORD_VERSION, decode_multipart_header, inspect_multipart_header,
};
use acyclic_fs::s3_multipart::{
    MultipartPart, MultipartSnapshot, MultipartStorageError, MultipartTerminal,
    StreamBackedAuthority, encode_multipart_aborted, encode_multipart_completed,
    encode_multipart_completing, encode_multipart_created, encode_multipart_part,
    multipart_cas_operation,
};
#[cfg(any(test, feature = "test-support"))]
use acyclic_fs::s3_multipart::{MultipartStorageResult, decode_multipart};
use acyclic_fs::{
    AsyncObjectStore, ByteRange, CancellationToken, IdempotencyKey, S3Error, S3ListCursor,
    S3ListOptions, Workspace,
};
use acyclic_fs_format::s3_limits::FilesystemS3Limits;
#[cfg(test)]
use acyclic_fs_format::s3_limits::S3MultipartRetentionLimits;
#[cfg(test)]
use acyclic_stream::StreamProviderExt as _;
use async_trait::async_trait;
use base64::Engine as _;
use bytes::{Buf, Bytes};
use futures::{StreamExt, stream};
use s3s::{
    S3, S3Request, S3Response, S3Result,
    auth::{S3Auth, SecretKey},
    dto::{
        AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, CommonPrefix,
        CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CompletedPart,
        CopyObjectInput, CopyObjectOutput, CopyObjectResult, CreateBucketInput, CreateBucketOutput,
        CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketInput,
        DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput,
        DeleteObjectsOutput, DeletedObject, GetBucketLocationInput, GetBucketLocationOutput,
        GetObjectInput, GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput,
        HeadObjectOutput, ListBucketsInput, ListBucketsOutput, ListObjectVersionsInput,
        ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput, ListObjectsV2Input,
        ListObjectsV2Output, ListPartsInput, ListPartsOutput, Object, ObjectVersion, Part,
        PostObjectInput, PostObjectOutput, PutObjectInput, PutObjectOutput, Range, StreamingBlob,
        UploadPartInput, UploadPartOutput,
    },
};
use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};
use subtle::ConstantTimeEq;

const IDEMPOTENCY_HEADER: &str = "x-acyclic-idempotency-key";
const SDK_INVOCATION_HEADER: &str = "amz-sdk-invocation-id";
const SESSION_TOKEN_HEADER: &str = "x-amz-security-token";

/// One authenticated credential resolved to one exact workspace.
#[derive(Clone)]
pub struct FilesystemS3Principal<A: 'static, O: 'static> {
    /// Secret used only by the `SigV4` verifier.
    pub secret_key: SecretKey,
    /// Opaque bucket coordinate issued for this workspace.
    pub bucket: String,
    /// Canonical workspace selected by the capability.
    pub workspace: Workspace<A, O>,
    /// Optional immutable generation bound into a read-only capability.
    pub generation: Option<acyclic_fs::GenerationId>,
    /// Whether the capability admits S3 mutations.
    pub writable: bool,
}

/// Deployment-owned credential lookup.
#[async_trait]
pub trait FilesystemS3Resolver<A, O>: Send + Sync + 'static {
    /// Resolves only the signing secret required to authenticate `SigV4`.
    async fn resolve_secret_key(&self, access_key: &str) -> S3Result<SecretKey>;

    /// Resolves and authorizes the complete request capability after `SigV4`.
    async fn resolve_principal(
        &self,
        access_key: &str,
        session_token: Option<&str>,
        bucket: &str,
    ) -> S3Result<FilesystemS3Principal<A, O>>;

    /// Creates a bucket backed by a distinct workspace, if this deployment exposes bucket administration.
    async fn create_bucket(
        &self,
        _access_key: &str,
        _session_token: Option<&str>,
        _bucket: &str,
    ) -> S3Result<()> {
        Err(s3s::s3_error!(NotImplemented))
    }

    /// Lists only buckets owned by this credential.
    async fn list_buckets(
        &self,
        _access_key: &str,
        _session_token: Option<&str>,
    ) -> S3Result<Vec<String>> {
        Err(s3s::s3_error!(NotImplemented))
    }

    /// Deletes an empty bucket after the adapter has authorized its workspace.
    async fn delete_bucket(
        &self,
        _access_key: &str,
        _session_token: Option<&str>,
        _bucket: &str,
    ) -> S3Result<()> {
        Err(s3s::s3_error!(NotImplemented))
    }
}

/// `SigV4` secret lookup sharing the exact resolver used by semantic dispatch.
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
    pub fn new(resolver: Arc<R>, limits: FilesystemS3Limits) -> S3Result<Self> {
        Ok(Self {
            resolver,
            limits: limits
                .validate()
                .map_err(|_| s3s::s3_error!(InvalidArgument))?,
            marker: PhantomData,
        })
    }
}

impl<R, A, O> FilesystemS3Adapter<R, A, O>
where
    R: FilesystemS3Resolver<A, O>,
    A: StreamBackedAuthority + 'static,
    O: AsyncObjectStore + 'static,
{
    async fn authenticated_access_key<'a, T>(
        &self,
        request: &'a S3Request<T>,
    ) -> S3Result<&'a str> {
        let credentials = request
            .credentials
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        let secret = self
            .resolver
            .resolve_secret_key(&credentials.access_key)
            .await?;
        if !bool::from(secret.ct_eq(&credentials.secret_key)) {
            return Err(s3s::s3_error!(AccessDenied));
        }
        Ok(&credentials.access_key)
    }
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
                request_session_token(request),
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
        let mut principal = self.principal(request, bucket).await?;
        if principal.bucket != bucket
            || mutation && (!principal.writable || principal.generation.is_some())
        {
            return Err(s3s::s3_error!(AccessDenied));
        }
        use acyclic_fs::access::Action;
        let read = [Action::List, Action::Read, Action::ReadHistory];
        let write = [
            Action::List,
            Action::Read,
            Action::ReadHistory,
            Action::Create,
            Action::Write,
            Action::Append,
            Action::Delete,
            Action::Rename,
            Action::Lock,
        ];
        principal.workspace = principal
            .workspace
            .bind_coarse_access(
                if mutation { &write } else { &read },
                &CancellationToken::new(),
            )
            .await
            .map_err(|_| s3s::s3_error!(AccessDenied))?;
        Ok(principal)
    }
}

fn request_session_token<T>(request: &S3Request<T>) -> Option<&str> {
    request
        .headers
        .get(SESSION_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
}

include!("s3_http/multipart.rs");

#[async_trait]
impl<R, A, O> S3 for FilesystemS3Adapter<R, A, O>
where
    R: FilesystemS3Resolver<A, O>,
    A: StreamBackedAuthority + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn create_bucket(
        &self,
        request: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.s3_create_bucket(request).await
    }

    async fn list_buckets(
        &self,
        request: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        self.s3_list_buckets(request).await
    }

    async fn delete_bucket(
        &self,
        request: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.s3_delete_bucket(request).await
    }

    async fn create_multipart_upload(
        &self,
        request: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.s3_create_multipart_upload(request).await
    }

    async fn upload_part(
        &self,
        request: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        self.s3_upload_part(request).await
    }

    async fn list_parts(
        &self,
        request: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        self.s3_list_parts(request).await
    }

    async fn complete_multipart_upload(
        &self,
        request: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.s3_complete_multipart_upload(request).await
    }

    async fn abort_multipart_upload(
        &self,
        request: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.s3_abort_multipart_upload(request).await
    }

    async fn head_bucket(
        &self,
        request: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.s3_head_bucket(request).await
    }

    async fn get_bucket_location(
        &self,
        request: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.s3_get_bucket_location(request).await
    }

    async fn list_objects(
        &self,
        request: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        self.s3_list_objects(request).await
    }

    async fn list_objects_v2(
        &self,
        request: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        self.s3_list_objects_v2(request).await
    }

    async fn list_object_versions(
        &self,
        request: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        self.s3_list_object_versions(request).await
    }

    async fn head_object(
        &self,
        request: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        self.s3_head_object(request).await
    }

    async fn get_object(
        &self,
        request: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        self.s3_get_object(request).await
    }

    async fn put_object(
        &self,
        request: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        self.s3_put_object(request).await
    }

    async fn post_object(
        &self,
        _request: S3Request<PostObjectInput>,
    ) -> S3Result<S3Response<PostObjectOutput>> {
        self.s3_post_object(_request).await
    }

    async fn delete_object(
        &self,
        request: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.s3_delete_object(request).await
    }

    async fn delete_objects(
        &self,
        request: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        self.s3_delete_objects(request).await
    }

    async fn copy_object(
        &self,
        request: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        self.s3_copy_object(request).await
    }
}

include!("s3_http/operations_0.rs");
include!("s3_http/operations_1.rs");
include!("s3_http/body.rs");
include!("s3_http/helpers.rs");

#[cfg(feature = "test-support")]
#[path = "s3_http/codec_fuzz.rs"]
#[doc(hidden)]
pub mod codec_fuzz;

#[cfg(test)]
mod tests;
