//! Idiomatic gRPC client for the public Objects contract.

use crate::{Condition, wire};
use bytes::Bytes;
use futures::{Stream, StreamExt, stream::iter};
use prost::Message;
use thiserror::Error;
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::{Channel, Endpoint},
};

const BODY_FRAME_BYTES: usize = 64 * 1024;

/// Errors returned while constructing or using a client.
#[derive(Debug, Error)]
pub enum Error {
    /// The configured endpoint was not a valid URI.
    #[error("invalid Objects endpoint: {0}")]
    InvalidEndpoint(#[from] tonic::transport::Error),
    /// Customer endpoints must use authenticated TLS.
    #[error("Objects customer endpoints must use https")]
    InsecureEndpoint,
    /// The supplied bearer credential could not be encoded as request metadata.
    #[error("invalid Objects bearer credential")]
    InvalidCredential,
    /// The service rejected a request with a canonical public semantic code.
    #[error("Objects request rejected ({code:?}, request {request_id}): {message}")]
    Rejected {
        /// Stable public error category.
        code: wire::ErrorCode,
        /// Request identity supplied by the service when available.
        request_id: String,
        /// Human-readable diagnostic, not suitable for control flow.
        message: String,
    },
    /// The transport failed without a valid canonical Objects error detail.
    #[error("Objects transport failed: {0}")]
    Transport(tonic::Status),
    /// A successful response violated the versioned protocol contract.
    #[error("invalid Objects protocol response: {0}")]
    Protocol(&'static str),
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        if let Ok(detail) = wire::ErrorDetail::decode(status.details())
            && let Ok(code) = wire::ErrorCode::try_from(detail.code)
            && code != wire::ErrorCode::Unspecified
        {
            return Self::Rejected {
                code,
                request_id: detail.request_id,
                message: status.message().to_owned(),
            };
        }
        Self::Transport(status)
    }
}

/// Result type used by this SDK.
pub type Result<T> = std::result::Result<T, Error>;

/// A configured Objects account client.
#[derive(Clone)]
pub struct Client {
    channel: Channel,
    authorization: MetadataValue<Ascii>,
}

impl Client {
    /// Connect to an account endpoint using its account-bound bearer credential.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint/credential is invalid or the TLS connection fails.
    pub async fn connect(endpoint: impl AsRef<str>, bearer_token: impl AsRef<str>) -> Result<Self> {
        let endpoint = Endpoint::from_shared(endpoint.as_ref().to_owned())?;
        if endpoint.uri().scheme_str() != Some("https") {
            return Err(Error::InsecureEndpoint);
        }
        let authorization = format!("Bearer {}", bearer_token.as_ref())
            .parse::<MetadataValue<Ascii>>()
            .map_err(|_| Error::InvalidCredential)?;
        let channel = endpoint.connect().await?;
        Ok(Self {
            channel,
            authorization,
        })
    }

    /// Create a permanently versioned bucket.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn create_bucket(
        &self,
        name: impl Into<String>,
        options: MutationOptions,
    ) -> Result<Bucket> {
        let mut client =
            wire::buckets_service_client::BucketsServiceClient::new(self.channel.clone());
        let response = client
            .create_bucket(authenticated(
                &self.authorization,
                wire::CreateBucketRequest {
                    name: name.into(),
                    mutation: options.wire(),
                },
            ))
            .await?
            .into_inner();
        let reference = response
            .bucket
            .ok_or(Error::Protocol("missing bucket identity"))?;
        Ok(Bucket::new(self, reference))
    }

    /// Validate and open one exact persisted bucket identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the identity is stale, mismatched, or unavailable.
    pub async fn open_bucket(&self, reference: wire::BucketRef) -> Result<Bucket> {
        let mut client =
            wire::buckets_service_client::BucketsServiceClient::new(self.channel.clone());
        let response = client
            .head_bucket(authenticated(
                &self.authorization,
                wire::HeadBucketRequest {
                    bucket: Some(reference.clone()),
                },
            ))
            .await?
            .into_inner();
        if response.bucket.as_ref() != Some(&reference) {
            return Err(Error::Protocol("bucket identity changed"));
        }
        Ok(Bucket::new(self, reference))
    }

    /// Reconstruct a read-only snapshot handle from its exact persisted identity.
    #[must_use]
    pub fn snapshot(&self, reference: wire::SnapshotRef) -> Snapshot {
        Snapshot::new(self, reference)
    }
}

/// Options common to retryable mutations.
#[derive(Clone, Debug, Default)]
pub struct MutationOptions {
    idempotency_key: String,
}

impl MutationOptions {
    /// Attach an account-scoped idempotency key retained for seven days.
    #[must_use]
    pub fn idempotency_key(mut self, value: impl Into<String>) -> Self {
        self.idempotency_key = value.into();
        self
    }

    fn wire(&self) -> Option<wire::MutationIdentity> {
        (!self.idempotency_key.is_empty()).then(|| wire::MutationIdentity {
            idempotency_key: self.idempotency_key.clone(),
        })
    }
}

/// Options for a single object put.
#[derive(Clone, Debug, Default)]
pub struct PutOptions {
    metadata: wire::ObjectMetadata,
    mutation: MutationOptions,
    condition: Option<Condition>,
}

impl PutOptions {
    /// Attach an account-scoped idempotency key retained for seven days.
    #[must_use]
    pub fn idempotency_key(mut self, value: impl Into<String>) -> Self {
        self.mutation = self.mutation.idempotency_key(value);
        self
    }

    /// Set the immutable content type for this version.
    #[must_use]
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.metadata.content_type = value.into();
        self
    }

    /// Set the immutable content encoding for this version.
    #[must_use]
    pub fn content_encoding(mut self, value: impl Into<String>) -> Self {
        self.metadata.content_encoding = value.into();
        self
    }

    /// Set the immutable cache-control value for this version.
    #[must_use]
    pub fn cache_control(mut self, value: impl Into<String>) -> Self {
        self.metadata.cache_control = value.into();
        self
    }

    /// Set the immutable content disposition for this version.
    #[must_use]
    pub fn content_disposition(mut self, value: impl Into<String>) -> Self {
        self.metadata.content_disposition = value.into();
        self
    }

    /// Set the immutable content language for this version.
    #[must_use]
    pub fn content_language(mut self, value: impl Into<String>) -> Self {
        self.metadata.content_language = value.into();
        self
    }

    /// Set the immutable HTTP expiry time as Unix seconds.
    #[must_use]
    pub fn expires_unix_seconds(mut self, value: i64) -> Self {
        self.metadata.expires_unix_seconds = Some(value);
        self
    }

    /// Add immutable user metadata to this version.
    #[must_use]
    pub fn user_metadata(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.user.insert(name.into(), value.into());
        self
    }

    /// Attach the sole current-version condition.
    #[must_use]
    pub fn condition(mut self, value: Condition) -> Self {
        self.condition = Some(value);
        self
    }
}

/// Options for an object read or head.
#[derive(Clone, Debug)]
pub struct GetOptions {
    version_id: String,
    range: Option<(u64, Option<u64>)>,
    if_match: String,
    if_none_match: String,
    maximum_bytes: u64,
}

impl Default for GetOptions {
    fn default() -> Self {
        Self {
            version_id: String::new(),
            range: None,
            if_match: String::new(),
            if_none_match: String::new(),
            maximum_bytes: crate::limits::OBJECT_BYTES,
        }
    }
}

impl GetOptions {
    /// Read one exact immutable version.
    #[must_use]
    pub fn version(mut self, value: impl Into<String>) -> Self {
        self.version_id = value.into();
        self
    }

    /// Read one inclusive byte range.
    #[must_use]
    pub fn range(mut self, start: u64, end_inclusive: Option<u64>) -> Self {
        self.range = Some((start, end_inclusive));
        self
    }

    /// Require one opaque `ETag`.
    #[must_use]
    pub fn if_match(mut self, value: impl Into<String>) -> Self {
        self.if_match = value.into();
        self
    }

    /// Reject one opaque `ETag`.
    #[must_use]
    pub fn if_none_match(mut self, value: impl Into<String>) -> Self {
        self.if_none_match = value.into();
        self
    }

    /// Bound the selected response body before any body frame is decoded.
    #[must_use]
    pub fn maximum_bytes(mut self, value: u64) -> Self {
        self.maximum_bytes = value;
        self
    }
}

/// Options for object deletion.
#[derive(Clone, Debug, Default)]
pub struct DeleteOptions {
    version_id: String,
    mutation: MutationOptions,
    condition: Option<Condition>,
}

impl DeleteOptions {
    /// Delete one exact immutable version rather than publishing a marker.
    #[must_use]
    pub fn version(mut self, value: impl Into<String>) -> Self {
        self.version_id = value.into();
        self
    }

    /// Attach an account-scoped idempotency key.
    #[must_use]
    pub fn idempotency_key(mut self, value: impl Into<String>) -> Self {
        self.mutation = self.mutation.idempotency_key(value);
        self
    }

    /// Attach the sole current-version condition for marker publication.
    #[must_use]
    pub fn condition(mut self, value: Condition) -> Self {
        self.condition = Some(value);
        self
    }
}

/// Options for one stable listing page.
#[derive(Clone, Debug)]
pub struct ListOptions {
    /// Prefix filter.
    pub prefix: String,
    /// Optional delimiter.
    pub delimiter: String,
    /// Include every version instead of live current versions.
    pub versions: bool,
    /// Maximum combined objects and common prefixes.
    pub page_size: u32,
    /// Opaque continuation token.
    pub continuation: String,
}

impl Default for ListOptions {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            delimiter: String::new(),
            versions: false,
            page_size: 1_000,
            continuation: String::new(),
        }
    }
}

/// One typed stable listing page.
#[derive(Clone, Debug)]
pub struct ListPage {
    /// Ordered object/version entries.
    pub entries: Vec<wire::ListEntry>,
    /// Ordered delimiter-derived prefixes.
    pub common_prefixes: Vec<String>,
    /// Opaque continuation token, absent on the final page.
    pub continuation: Option<String>,
}

/// A handle bound to one exact bucket identity.
#[derive(Clone)]
pub struct Bucket {
    channel: Channel,
    authorization: MetadataValue<Ascii>,
    reference: wire::BucketRef,
}

impl Bucket {
    fn new(client: &Client, reference: wire::BucketRef) -> Self {
        Self {
            channel: client.channel.clone(),
            authorization: client.authorization.clone(),
            reference,
        }
    }

    /// The opaque bucket identity. Name reuse never revives it.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.reference.bucket_id
    }

    /// The account-scoped bucket name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.reference.name
    }

    /// Copy the complete opaque bucket identity for durable application state.
    #[must_use]
    pub fn reference(&self) -> wire::BucketRef {
        self.reference.clone()
    }

    /// Delete this exact bucket when it has no versions, markers, or multipart uploads.
    ///
    /// # Errors
    ///
    /// Returns an error when the bucket is not empty or the service cannot complete the request.
    pub async fn destroy(&self, options: MutationOptions) -> Result<bool> {
        let mut client =
            wire::buckets_service_client::BucketsServiceClient::new(self.channel.clone());
        Ok(client
            .delete_bucket(authenticated(
                &self.authorization,
                wire::DeleteBucketRequest {
                    bucket: Some(self.reference.clone()),
                    mutation: options.wire(),
                },
            ))
            .await?
            .into_inner()
            .existed)
    }

    /// Create one immutable version from buffered bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn put(
        &self,
        object_key: impl Into<String>,
        body: Bytes,
        options: PutOptions,
    ) -> Result<wire::ObjectVersion> {
        self.put_stream(object_key, iter([body]), options).await
    }

    /// Stream one immutable version without buffering it in the SDK.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn put_stream<S>(
        &self,
        object_key: impl Into<String>,
        body: S,
        options: PutOptions,
    ) -> Result<wire::ObjectVersion>
    where
        S: Stream<Item = Bytes> + Send + 'static,
    {
        let header = wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Header(
                wire::PutObjectHeader {
                    bucket: Some(self.reference.clone()),
                    object_key: object_key.into(),
                    metadata: Some(options.metadata),
                    preconditions: options.condition.map(Condition::wire),
                    mutation: options.mutation.wire(),
                },
            )),
        };
        let frames = iter([header]).chain(body.flat_map(|chunk| {
            iter(
                chunk
                    .chunks(BODY_FRAME_BYTES)
                    .map(|frame| wire::PutObjectRequest {
                        frame: Some(wire::put_object_request::Frame::Body(frame.to_vec())),
                    })
                    .collect::<Vec<_>>(),
            )
        }));
        let mut client =
            wire::objects_service_client::ObjectsServiceClient::new(self.channel.clone());
        Ok(client
            .put_object(authenticated(&self.authorization, frames))
            .await?
            .into_inner())
    }

    /// Open a streaming object read.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects the read or violates framing.
    pub async fn get(
        &self,
        object_key: impl Into<String>,
        options: GetOptions,
    ) -> Result<StoredObject> {
        get_from(
            &self.channel,
            &self.authorization,
            bucket_target(&self.reference),
            object_key.into(),
            options,
        )
        .await
    }

    /// Read immutable object metadata without transferring its body.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn head(
        &self,
        object_key: impl Into<String>,
        options: GetOptions,
    ) -> Result<wire::ObjectVersion> {
        head_from(
            &self.channel,
            &self.authorization,
            bucket_target(&self.reference),
            object_key.into(),
            options,
        )
        .await
    }

    /// Publish a delete marker or delete one exact immutable version.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn delete(
        &self,
        object_key: impl Into<String>,
        options: DeleteOptions,
    ) -> Result<wire::DeleteObjectResponse> {
        let mut client =
            wire::objects_service_client::ObjectsServiceClient::new(self.channel.clone());
        Ok(client
            .delete_object(authenticated(
                &self.authorization,
                wire::DeleteObjectRequest {
                    bucket: Some(self.reference.clone()),
                    object_key: object_key.into(),
                    version_id: options.version_id,
                    preconditions: options.condition.map(Condition::wire),
                    mutation: options.mutation.wire(),
                },
            ))
            .await?
            .into_inner())
    }

    /// Read one page from a fixed 24-hour listing view.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn list_page(&self, options: ListOptions) -> Result<ListPage> {
        list_from(
            &self.channel,
            &self.authorization,
            bucket_target(&self.reference),
            options,
        )
        .await
    }

    /// Capture an immutable whole-bucket snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn snapshot(&self, options: MutationOptions) -> Result<Snapshot> {
        let mut client =
            wire::snapshots_service_client::SnapshotsServiceClient::new(self.channel.clone());
        let response = client
            .create_snapshot(authenticated(
                &self.authorization,
                wire::CreateSnapshotRequest {
                    bucket: Some(self.reference.clone()),
                    mutation: options.wire(),
                },
            ))
            .await?
            .into_inner();
        let reference = response
            .snapshot
            .ok_or(Error::Protocol("missing snapshot identity"))?;
        Ok(Snapshot {
            channel: self.channel.clone(),
            authorization: self.authorization.clone(),
            reference,
        })
    }

    /// Atomically fork the current complete bucket state.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn fork_current(
        &self,
        destination_name: impl Into<String>,
        options: MutationOptions,
    ) -> Result<Self> {
        let mut client =
            wire::snapshots_service_client::SnapshotsServiceClient::new(self.channel.clone());
        let response = client
            .fork_bucket(authenticated(
                &self.authorization,
                wire::ForkBucketRequest {
                    source: Some(self.reference.clone()),
                    destination_name: destination_name.into(),
                    mutation: options.wire(),
                },
            ))
            .await?
            .into_inner();
        let reference = response
            .bucket
            .ok_or(Error::Protocol("missing fork bucket identity"))?;
        Ok(Self {
            channel: self.channel.clone(),
            authorization: self.authorization.clone(),
            reference,
        })
    }

    /// Begin a multipart upload.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn create_multipart(
        &self,
        object_key: impl Into<String>,
        options: PutOptions,
    ) -> Result<MultipartUpload> {
        let object_key = object_key.into();
        let mut client =
            wire::multipart_service_client::MultipartServiceClient::new(self.channel.clone());
        let response = client
            .create_multipart(authenticated(
                &self.authorization,
                wire::CreateMultipartRequest {
                    bucket: Some(self.reference.clone()),
                    object_key: object_key.clone(),
                    metadata: Some(options.metadata),
                    preconditions: options.condition.map(Condition::wire),
                    mutation: options.mutation.wire(),
                },
            ))
            .await?
            .into_inner();
        Ok(MultipartUpload {
            bucket: self.clone(),
            object_key,
            upload_id: response.upload_id,
        })
    }
}

/// Immutable snapshot read handle.
#[derive(Clone)]
pub struct Snapshot {
    channel: Channel,
    authorization: MetadataValue<Ascii>,
    reference: wire::SnapshotRef,
}

impl Snapshot {
    fn new(client: &Client, reference: wire::SnapshotRef) -> Self {
        Self {
            channel: client.channel.clone(),
            authorization: client.authorization.clone(),
            reference,
        }
    }

    /// Opaque snapshot identity.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.reference.snapshot_id
    }

    /// Copy the complete opaque snapshot identity for durable application state.
    #[must_use]
    pub fn reference(&self) -> wire::SnapshotRef {
        self.reference.clone()
    }

    /// Open a streaming immutable object read.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects the read or violates framing.
    pub async fn get(
        &self,
        object_key: impl Into<String>,
        options: GetOptions,
    ) -> Result<StoredObject> {
        get_from(
            &self.channel,
            &self.authorization,
            snapshot_target(&self.reference),
            object_key.into(),
            options,
        )
        .await
    }

    /// Read one page from this immutable snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn list_page(&self, options: ListOptions) -> Result<ListPage> {
        list_from(
            &self.channel,
            &self.authorization,
            snapshot_target(&self.reference),
            options,
        )
        .await
    }

    /// Atomically publish an independent bucket from this snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn fork(
        &self,
        destination_name: impl Into<String>,
        options: MutationOptions,
    ) -> Result<Bucket> {
        let mut client =
            wire::snapshots_service_client::SnapshotsServiceClient::new(self.channel.clone());
        let response = client
            .fork_snapshot(authenticated(
                &self.authorization,
                wire::ForkSnapshotRequest {
                    snapshot: Some(self.reference.clone()),
                    destination_name: destination_name.into(),
                    mutation: options.wire(),
                },
            ))
            .await?
            .into_inner();
        let reference = response
            .bucket
            .ok_or(Error::Protocol("missing fork bucket identity"))?;
        Ok(Bucket {
            channel: self.channel.clone(),
            authorization: self.authorization.clone(),
            reference,
        })
    }

    /// Destroy this snapshot identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn destroy(&self, options: MutationOptions) -> Result<bool> {
        let mut client =
            wire::snapshots_service_client::SnapshotsServiceClient::new(self.channel.clone());
        Ok(client
            .destroy_snapshot(authenticated(
                &self.authorization,
                wire::DestroySnapshotRequest {
                    snapshot: Some(self.reference.clone()),
                    mutation: options.wire(),
                },
            ))
            .await?
            .into_inner()
            .existed)
    }
}

/// Streaming object response after its immutable descriptor has been validated.
pub struct StoredObject {
    /// Immutable version descriptor.
    pub version: wire::ObjectVersion,
    stream: tonic::Streaming<wire::GetObjectResponse>,
    remaining: u64,
}

impl StoredObject {
    /// Read the next body chunk.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failure or invalid response framing.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        let Some(response) = self.stream.message().await? else {
            return if self.remaining == 0 {
                Ok(None)
            } else {
                Err(Error::Protocol("object body ended before declared size"))
            };
        };
        match response.frame {
            Some(wire::get_object_response::Frame::Body(body)) => {
                let size = u64::try_from(body.len())
                    .map_err(|_| Error::Protocol("object body frame is too large"))?;
                self.remaining = self
                    .remaining
                    .checked_sub(size)
                    .ok_or(Error::Protocol("object body exceeded declared size"))?;
                Ok(Some(Bytes::from(body)))
            }
            Some(wire::get_object_response::Frame::Version(_)) => {
                Err(Error::Protocol("duplicate object version frame"))
            }
            None => Err(Error::Protocol("empty object response frame")),
        }
    }
}

/// Typed multipart upload handle.
#[derive(Clone)]
pub struct MultipartUpload {
    bucket: Bucket,
    object_key: String,
    upload_id: String,
}

impl MultipartUpload {
    /// Opaque upload identity.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.upload_id
    }

    /// Upload or replace one buffered part.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn upload_part(
        &self,
        part_number: u32,
        body: Bytes,
        options: MutationOptions,
    ) -> Result<wire::UploadedPart> {
        let header = wire::UploadPartRequest {
            frame: Some(wire::upload_part_request::Frame::Header(
                wire::UploadPartHeader {
                    bucket: Some(self.bucket.reference.clone()),
                    object_key: self.object_key.clone(),
                    upload_id: self.upload_id.clone(),
                    part_number,
                    mutation: options.wire(),
                },
            )),
        };
        let content = body
            .chunks(BODY_FRAME_BYTES)
            .map(|frame| wire::UploadPartRequest {
                frame: Some(wire::upload_part_request::Frame::Body(frame.to_vec())),
            })
            .collect::<Vec<_>>();
        let mut client = wire::multipart_service_client::MultipartServiceClient::new(
            self.bucket.channel.clone(),
        );
        Ok(client
            .upload_part(authenticated(
                &self.bucket.authorization,
                iter([header]).chain(iter(content)),
            ))
            .await?
            .into_inner())
    }

    /// List staged parts in ascending part-number order.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn list_parts(&self) -> Result<Vec<wire::UploadedPart>> {
        let mut client = wire::multipart_service_client::MultipartServiceClient::new(
            self.bucket.channel.clone(),
        );
        Ok(client
            .list_parts(authenticated(
                &self.bucket.authorization,
                wire::ListPartsRequest {
                    bucket: Some(self.bucket.reference.clone()),
                    object_key: self.object_key.clone(),
                    upload_id: self.upload_id.clone(),
                },
            ))
            .await?
            .into_inner()
            .parts)
    }

    /// Atomically publish one immutable version from exact part receipts.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn complete(
        &self,
        parts: Vec<wire::UploadedPart>,
        options: MutationOptions,
    ) -> Result<wire::ObjectVersion> {
        let mut client = wire::multipart_service_client::MultipartServiceClient::new(
            self.bucket.channel.clone(),
        );
        Ok(client
            .complete_multipart(authenticated(
                &self.bucket.authorization,
                wire::CompleteMultipartRequest {
                    bucket: Some(self.bucket.reference.clone()),
                    object_key: self.object_key.clone(),
                    upload_id: self.upload_id.clone(),
                    parts,
                    mutation: options.wire(),
                },
            ))
            .await?
            .into_inner())
    }

    /// Abort this exact upload.
    ///
    /// # Errors
    ///
    /// Returns an error when the service rejects or cannot complete the request.
    pub async fn abort(&self, options: MutationOptions) -> Result<bool> {
        let mut client = wire::multipart_service_client::MultipartServiceClient::new(
            self.bucket.channel.clone(),
        );
        Ok(client
            .abort_multipart(authenticated(
                &self.bucket.authorization,
                wire::AbortMultipartRequest {
                    bucket: Some(self.bucket.reference.clone()),
                    object_key: self.object_key.clone(),
                    upload_id: self.upload_id.clone(),
                    mutation: options.wire(),
                },
            ))
            .await?
            .into_inner()
            .existed)
    }
}

async fn get_from(
    channel: &Channel,
    authorization: &MetadataValue<Ascii>,
    target: wire::ReadTarget,
    object_key: String,
    options: GetOptions,
) -> Result<StoredObject> {
    let requested_range = options.range;
    let (range_start, range_end_inclusive) = requested_range.unwrap_or((0, None));
    let frame_body_limit = usize::try_from(options.maximum_bytes.min(BODY_FRAME_BYTES as u64))
        .map_err(|_| Error::Protocol("object response bound is not representable"))?;
    let decoding_limit = frame_body_limit
        .checked_add(256)
        .ok_or(Error::Protocol("object response bound overflowed"))?;
    let mut client = wire::objects_service_client::ObjectsServiceClient::new(channel.clone())
        .max_decoding_message_size(decoding_limit);
    let mut stream = client
        .get_object(authenticated(
            authorization,
            wire::GetObjectRequest {
                target: Some(target),
                object_key,
                version_id: options.version_id,
                range_start,
                range_end_inclusive,
                if_match: options.if_match,
                if_none_match: options.if_none_match,
            },
        ))
        .await?
        .into_inner();
    let first = stream
        .message()
        .await?
        .and_then(|response| response.frame)
        .ok_or(Error::Protocol("missing object version frame"))?;
    let wire::get_object_response::Frame::Version(version) = first else {
        return Err(Error::Protocol("object version frame must be first"));
    };
    if version.delete_marker {
        return Err(Error::Protocol("get returned a delete marker"));
    }
    let remaining = match requested_range {
        None => version.size,
        Some((start, end)) => {
            let end = end.unwrap_or_else(|| version.size.saturating_sub(1));
            if start > end || end >= version.size {
                return Err(Error::Protocol("response cannot satisfy requested range"));
            }
            end - start + 1
        }
    };
    if remaining > options.maximum_bytes {
        return Err(Error::Protocol("object body exceeds requested bound"));
    }
    Ok(StoredObject {
        version,
        stream,
        remaining,
    })
}

async fn head_from(
    channel: &Channel,
    authorization: &MetadataValue<Ascii>,
    target: wire::ReadTarget,
    object_key: String,
    options: GetOptions,
) -> Result<wire::ObjectVersion> {
    let mut client = wire::objects_service_client::ObjectsServiceClient::new(channel.clone());
    client
        .head_object(authenticated(
            authorization,
            wire::HeadObjectRequest {
                target: Some(target),
                object_key,
                version_id: options.version_id,
                if_match: options.if_match,
                if_none_match: options.if_none_match,
            },
        ))
        .await?
        .into_inner()
        .version
        .ok_or(Error::Protocol("missing object version"))
}

async fn list_from(
    channel: &Channel,
    authorization: &MetadataValue<Ascii>,
    target: wire::ReadTarget,
    options: ListOptions,
) -> Result<ListPage> {
    let mut client = wire::objects_service_client::ObjectsServiceClient::new(channel.clone());
    let response = client
        .list_objects(authenticated(
            authorization,
            wire::ListObjectsRequest {
                target: Some(target),
                prefix: options.prefix,
                delimiter: options.delimiter,
                mode: if options.versions {
                    wire::ListingMode::Versions as i32
                } else {
                    wire::ListingMode::Current as i32
                },
                page_size: options.page_size,
                continuation_token: options.continuation,
            },
        ))
        .await?
        .into_inner();
    Ok(ListPage {
        entries: response.entries,
        common_prefixes: response.common_prefixes,
        continuation: (!response.continuation_token.is_empty())
            .then_some(response.continuation_token),
    })
}

fn authenticated<T>(authorization: &MetadataValue<Ascii>, message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", authorization.clone());
    request
}

fn bucket_target(reference: &wire::BucketRef) -> wire::ReadTarget {
    wire::ReadTarget {
        target: Some(wire::read_target::Target::Bucket(reference.clone())),
    }
}

fn snapshot_target(reference: &wire::SnapshotRef) -> wire::ReadTarget {
    wire::ReadTarget {
        target: Some(wire::read_target::Target::Snapshot(reference.clone())),
    }
}

fn mutation_options(
    idempotency_key: Option<String>,
) -> std::result::Result<MutationOptions, crate::ObjectsError> {
    if idempotency_key
        .as_ref()
        .is_some_and(|key| key.is_empty() || key.len() > 256)
    {
        return Err(crate::ObjectsError::Invalid("invalid idempotency key"));
    }
    Ok(MutationOptions {
        idempotency_key: idempotency_key.unwrap_or_default(),
    })
}

fn validate_bucket(
    bucket: wire::Bucket,
    expected: Option<&wire::BucketRef>,
    expected_name: Option<&str>,
) -> std::result::Result<wire::Bucket, crate::ObjectsError> {
    let reference = bucket
        .bucket
        .as_ref()
        .ok_or(crate::ObjectsError::Unavailable)?;
    if reference.bucket_id.is_empty()
        || reference.name.is_empty()
        || expected.is_some_and(|value| value != reference)
        || expected_name.is_some_and(|value| value != reference.name)
        || bucket.created_at.is_none()
    {
        return Err(crate::ObjectsError::Unavailable);
    }
    Ok(bucket)
}

fn validate_snapshot(
    snapshot: wire::Snapshot,
    source: &wire::BucketRef,
) -> std::result::Result<wire::Snapshot, crate::ObjectsError> {
    let reference = snapshot
        .snapshot
        .as_ref()
        .ok_or(crate::ObjectsError::Unavailable)?;
    if reference.snapshot_id.is_empty()
        || reference.source_bucket_id != source.bucket_id
        || snapshot.created_at.is_none()
    {
        return Err(crate::ObjectsError::Unavailable);
    }
    Ok(snapshot)
}

fn provider_target(target: crate::ReadTarget) -> wire::ReadTarget {
    match target {
        crate::ReadTarget::Bucket(reference) => bucket_target(&reference),
        crate::ReadTarget::Snapshot(reference) => snapshot_target(&reference),
    }
}

fn provider_error(error: Error) -> crate::ObjectsError {
    match error {
        Error::Rejected { code, .. } => match code {
            wire::ErrorCode::InvalidArgument => {
                crate::ObjectsError::Invalid("service rejected the request")
            }
            wire::ErrorCode::TokenExpired => crate::ObjectsError::Unauthorized,
            wire::ErrorCode::NotFound => crate::ObjectsError::NotFound,
            wire::ErrorCode::AlreadyExists => crate::ObjectsError::AlreadyExists,
            wire::ErrorCode::PreconditionFailed => crate::ObjectsError::PreconditionFailed,
            wire::ErrorCode::IdempotencyMismatch => crate::ObjectsError::IdempotencyMismatch,
            wire::ErrorCode::QuotaExceeded => crate::ObjectsError::Capacity,
            wire::ErrorCode::Unsupported => crate::ObjectsError::Unsupported,
            wire::ErrorCode::Unavailable | wire::ErrorCode::Unspecified => {
                crate::ObjectsError::Unavailable
            }
        },
        Error::InvalidEndpoint(_) | Error::InsecureEndpoint | Error::InvalidCredential => {
            crate::ObjectsError::Invalid("invalid provider configuration")
        }
        Error::Transport(status)
            if matches!(
                status.code(),
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
            ) =>
        {
            crate::ObjectsError::Unauthorized
        }
        Error::Transport(_) | Error::Protocol(_) => crate::ObjectsError::Unavailable,
    }
}

#[async_trait::async_trait]
impl crate::ObjectsProvider for Client {
    async fn create_bucket(
        &self,
        name: String,
        idempotency_key: Option<String>,
    ) -> std::result::Result<wire::Bucket, crate::ObjectsError> {
        let expected_name = name.clone();
        let mut client =
            wire::buckets_service_client::BucketsServiceClient::new(self.channel.clone());
        let bucket = client
            .create_bucket(authenticated(
                &self.authorization,
                wire::CreateBucketRequest {
                    name,
                    mutation: mutation_options(idempotency_key)?.wire(),
                },
            ))
            .await
            .map(tonic::Response::into_inner)
            .map_err(Error::from)
            .map_err(provider_error)?;
        validate_bucket(bucket, None, Some(&expected_name))
    }

    async fn head_bucket(
        &self,
        bucket: &wire::BucketRef,
    ) -> std::result::Result<wire::Bucket, crate::ObjectsError> {
        let mut client =
            wire::buckets_service_client::BucketsServiceClient::new(self.channel.clone());
        let response = client
            .head_bucket(authenticated(
                &self.authorization,
                wire::HeadBucketRequest {
                    bucket: Some(bucket.clone()),
                },
            ))
            .await
            .map(tonic::Response::into_inner)
            .map_err(Error::from)
            .map_err(provider_error)?;
        validate_bucket(response, Some(bucket), None)
    }

    async fn delete_bucket(
        &self,
        bucket: &wire::BucketRef,
        idempotency_key: Option<String>,
    ) -> std::result::Result<bool, crate::ObjectsError> {
        Bucket::new(self, bucket.clone())
            .destroy(mutation_options(idempotency_key)?)
            .await
            .map_err(provider_error)
    }

    async fn put(
        &self,
        request: crate::PutRequest,
    ) -> std::result::Result<wire::ObjectVersion, crate::ObjectsError> {
        let bucket = Bucket::new(self, request.bucket);
        bucket
            .put(
                request.object_key,
                request.body,
                PutOptions {
                    metadata: request.metadata,
                    mutation: mutation_options(request.idempotency_key)?,
                    condition: request.condition,
                },
            )
            .await
            .map_err(provider_error)
    }

    async fn get(
        &self,
        request: crate::GetRequest,
    ) -> std::result::Result<crate::BufferedObject, crate::ObjectsError> {
        let mut object = get_from(
            &self.channel,
            &self.authorization,
            provider_target(request.target),
            request.object_key,
            GetOptions {
                version_id: request.version_id.unwrap_or_default(),
                range: request.range,
                if_match: request.if_match.unwrap_or_default(),
                if_none_match: request.if_none_match.unwrap_or_default(),
                maximum_bytes: request.maximum_bytes,
            },
        )
        .await
        .map_err(provider_error)?;
        let version = object.version.clone();
        if object.remaining > request.maximum_bytes {
            return Err(crate::ObjectsError::Capacity);
        }
        let capacity =
            usize::try_from(object.remaining).map_err(|_| crate::ObjectsError::Capacity)?;
        let mut body = Vec::with_capacity(capacity);
        while let Some(chunk) = object.next_chunk().await.map_err(provider_error)? {
            body.extend_from_slice(&chunk);
        }
        Ok(crate::BufferedObject {
            version,
            body: Bytes::from(body),
        })
    }

    async fn delete(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        version_id: Option<String>,
        condition: Option<Condition>,
        idempotency_key: Option<String>,
    ) -> std::result::Result<crate::DeleteResult, crate::ObjectsError> {
        let response = Bucket::new(self, bucket)
            .delete(
                object_key,
                DeleteOptions {
                    version_id: version_id.unwrap_or_default(),
                    mutation: mutation_options(idempotency_key)?,
                    condition,
                },
            )
            .await
            .map_err(provider_error)?;
        Ok(crate::DeleteResult {
            existed: response.existed,
            marker: response.version,
        })
    }

    async fn list(
        &self,
        target: crate::ReadTarget,
        prefix: String,
        delimiter: Option<String>,
        versions: bool,
        page_size: u32,
        continuation: Option<String>,
    ) -> std::result::Result<crate::ProviderListPage, crate::ObjectsError> {
        let page = list_from(
            &self.channel,
            &self.authorization,
            provider_target(target),
            ListOptions {
                prefix,
                delimiter: delimiter.unwrap_or_default(),
                versions,
                page_size,
                continuation: continuation.unwrap_or_default(),
            },
        )
        .await
        .map_err(provider_error)?;
        Ok(crate::ProviderListPage {
            entries: page.entries,
            common_prefixes: page.common_prefixes,
            continuation: page.continuation,
        })
    }

    async fn snapshot(
        &self,
        bucket: wire::BucketRef,
        idempotency_key: Option<String>,
    ) -> std::result::Result<wire::Snapshot, crate::ObjectsError> {
        let mut client =
            wire::snapshots_service_client::SnapshotsServiceClient::new(self.channel.clone());
        let snapshot = client
            .create_snapshot(authenticated(
                &self.authorization,
                wire::CreateSnapshotRequest {
                    bucket: Some(bucket.clone()),
                    mutation: mutation_options(idempotency_key)?.wire(),
                },
            ))
            .await
            .map(tonic::Response::into_inner)
            .map_err(Error::from)
            .map_err(provider_error)?;
        validate_snapshot(snapshot, &bucket)
    }

    async fn destroy_snapshot(
        &self,
        snapshot: wire::SnapshotRef,
        idempotency_key: Option<String>,
    ) -> std::result::Result<bool, crate::ObjectsError> {
        Snapshot::new(self, snapshot)
            .destroy(mutation_options(idempotency_key)?)
            .await
            .map_err(provider_error)
    }

    async fn fork(
        &self,
        source: crate::ReadTarget,
        destination_name: String,
        idempotency_key: Option<String>,
    ) -> std::result::Result<wire::Bucket, crate::ObjectsError> {
        let mut client =
            wire::snapshots_service_client::SnapshotsServiceClient::new(self.channel.clone());
        let mutation = mutation_options(idempotency_key)?.wire();
        let expected_name = destination_name.clone();
        let response = match source {
            crate::ReadTarget::Bucket(source) => {
                client
                    .fork_bucket(authenticated(
                        &self.authorization,
                        wire::ForkBucketRequest {
                            source: Some(source),
                            destination_name: destination_name.clone(),
                            mutation,
                        },
                    ))
                    .await
            }
            crate::ReadTarget::Snapshot(snapshot) => {
                client
                    .fork_snapshot(authenticated(
                        &self.authorization,
                        wire::ForkSnapshotRequest {
                            snapshot: Some(snapshot),
                            destination_name,
                            mutation,
                        },
                    ))
                    .await
            }
        };
        let bucket = response
            .map(tonic::Response::into_inner)
            .map_err(Error::from)
            .map_err(provider_error)?;
        validate_bucket(bucket, None, Some(&expected_name))
    }

    async fn create_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        metadata: wire::ObjectMetadata,
        condition: Option<Condition>,
        idempotency_key: Option<String>,
    ) -> std::result::Result<wire::MultipartUpload, crate::ObjectsError> {
        let upload = Bucket::new(self, bucket)
            .create_multipart(
                object_key,
                PutOptions {
                    metadata,
                    mutation: mutation_options(idempotency_key)?,
                    condition,
                },
            )
            .await
            .map_err(provider_error)?;
        Ok(wire::MultipartUpload {
            upload_id: upload.upload_id,
        })
    }

    async fn upload_part(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        part_number: u32,
        body: Bytes,
        idempotency_key: Option<String>,
    ) -> std::result::Result<wire::UploadedPart, crate::ObjectsError> {
        MultipartUpload {
            bucket: Bucket::new(self, bucket),
            object_key,
            upload_id,
        }
        .upload_part(part_number, body, mutation_options(idempotency_key)?)
        .await
        .map_err(provider_error)
    }

    async fn list_parts(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
    ) -> std::result::Result<Vec<wire::UploadedPart>, crate::ObjectsError> {
        MultipartUpload {
            bucket: Bucket::new(self, bucket),
            object_key,
            upload_id,
        }
        .list_parts()
        .await
        .map_err(provider_error)
    }

    async fn complete_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        parts: Vec<wire::UploadedPart>,
        idempotency_key: Option<String>,
    ) -> std::result::Result<wire::ObjectVersion, crate::ObjectsError> {
        MultipartUpload {
            bucket: Bucket::new(self, bucket),
            object_key,
            upload_id,
        }
        .complete(parts, mutation_options(idempotency_key)?)
        .await
        .map_err(provider_error)
    }

    async fn abort_multipart(
        &self,
        bucket: wire::BucketRef,
        object_key: String,
        upload_id: String,
        idempotency_key: Option<String>,
    ) -> std::result::Result<bool, crate::ObjectsError> {
        MultipartUpload {
            bucket: Bucket::new(self, bucket),
            object_key,
            upload_id,
        }
        .abort(mutation_options(idempotency_key)?)
        .await
        .map_err(provider_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_provider<T: crate::ObjectsProvider>() {}

    #[test]
    fn remote_client_implements_the_public_provider_boundary() {
        assert_provider::<Client>();
    }

    #[test]
    fn provider_idempotency_keys_match_memory_validation() {
        assert!(matches!(
            mutation_options(Some(String::new())),
            Err(crate::ObjectsError::Invalid(_))
        ));
        assert!(matches!(
            mutation_options(Some("x".repeat(257))),
            Err(crate::ObjectsError::Invalid(_))
        ));
        assert!(mutation_options(None).is_ok());
        assert!(mutation_options(Some("key".into())).is_ok());
    }

    #[test]
    fn provider_rejects_malformed_and_substituted_identities() {
        let expected = wire::BucketRef {
            bucket_id: "bucket-1".into(),
            name: "expected".into(),
        };
        assert!(matches!(
            validate_bucket(wire::Bucket::default(), Some(&expected), None),
            Err(crate::ObjectsError::Unavailable)
        ));
        let substituted = wire::Bucket {
            bucket: Some(wire::BucketRef {
                bucket_id: "bucket-2".into(),
                name: "expected".into(),
            }),
            created_at: Some(prost_types::Timestamp::default()),
        };
        assert!(matches!(
            validate_bucket(substituted, Some(&expected), None),
            Err(crate::ObjectsError::Unavailable)
        ));
    }

    #[tokio::test]
    async fn plaintext_customer_endpoint_fails_before_connecting() {
        assert!(matches!(
            Client::connect("http://127.0.0.1:1", "token").await,
            Err(Error::InsecureEndpoint)
        ));
    }

    #[test]
    fn default_mutation_does_not_emit_an_empty_identity() {
        assert!(MutationOptions::default().wire().is_none());
    }

    #[test]
    fn canonical_error_details_remain_typed() {
        let detail = wire::ErrorDetail {
            code: wire::ErrorCode::IdempotencyMismatch as i32,
            request_id: "request-7".into(),
        }
        .encode_to_vec();
        let error = Error::from(tonic::Status::with_details(
            tonic::Code::AlreadyExists,
            "idempotency mismatch",
            detail.into(),
        ));
        assert!(matches!(
            error,
            Error::Rejected {
                code: wire::ErrorCode::IdempotencyMismatch,
                request_id,
                ..
            } if request_id == "request-7"
        ));
    }
}
