//! Validated semantic values for the Objects v2 wire contract.
//!
//! The generated protobuf messages remain the transport representation. These
//! wrappers make the values that already have canonical validators explicit at
//! Rust API boundaries without copying any of the validation rules.

use std::{fmt, num::NonZeroU64};

use super::{Error, request, response, wire};

/// A validated logical bucket name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BucketName(String);

impl BucketName {
    /// Validates and owns one logical bucket name.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        request::bucket_name(&value)?;
        Ok(Self(value))
    }

    /// Returns the validated bucket name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns its validated string.
    pub fn into_string(self) -> String {
        self.0
    }

    fn from_validated(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for BucketName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<String> for BucketName {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for BucketName {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<BucketName> for wire::BucketRef {
    fn from(value: BucketName) -> Self {
        Self {
            name: value.into_string(),
        }
    }
}

impl TryFrom<wire::BucketRef> for BucketName {
    type Error = Error;

    fn try_from(value: wire::BucketRef) -> Result<Self, Self::Error> {
        Self::new(value.name)
    }
}

impl fmt::Display for BucketName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(formatter)
    }
}

/// A validated nonempty logical object key.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Validates and owns one object key without normalizing path characters.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        request::key(&value)?;
        Ok(Self(value))
    }

    /// Returns the validated object key.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns its validated string.
    pub fn into_string(self) -> String {
        self.0
    }

    fn from_validated(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for ObjectKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<String> for ObjectKey {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ObjectKey {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ObjectKey> for String {
    fn from(value: ObjectKey) -> Self {
        value.into_string()
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(formatter)
    }
}

/// A validated caller retry identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Validates and owns one idempotency key.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        let candidate = Some(wire::MutationIdentity {
            idempotency_key: value.clone(),
        });
        request::identity(&candidate)?;
        Ok(Self(value))
    }

    /// Returns the validated idempotency key.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns its validated string.
    pub fn into_string(self) -> String {
        self.0
    }

    fn from_validated(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for IdempotencyKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<String> for IdempotencyKey {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for IdempotencyKey {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<wire::MutationIdentity> for IdempotencyKey {
    type Error = Error;

    fn try_from(value: wire::MutationIdentity) -> Result<Self, Self::Error> {
        Self::new(value.idempotency_key)
    }
}

impl From<IdempotencyKey> for wire::MutationIdentity {
    fn from(value: IdempotencyKey) -> Self {
        Self {
            idempotency_key: value.into_string(),
        }
    }
}

/// A validated opaque multipart upload identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UploadId(String);

impl UploadId {
    /// Validates and owns one multipart upload identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        request::upload_id(&value)?;
        Ok(Self(value))
    }

    /// Returns the validated upload identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns its validated string.
    pub fn into_string(self) -> String {
        self.0
    }

    fn from_validated(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for UploadId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<String> for UploadId {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for UploadId {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<UploadId> for String {
    fn from(value: UploadId) -> Self {
        value.into_string()
    }
}

impl fmt::Display for UploadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(formatter)
    }
}

/// A validated opaque ETag used by single-value preconditions.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Etag(String);

impl Etag {
    /// Validates and owns one ETag using the canonical precondition validator.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        let candidate = Some(wire::Preconditions {
            condition: Some(wire::preconditions::Condition::IfMatch(value.clone())),
        });
        request::preconditions(&candidate)?;
        Ok(Self(value))
    }

    fn from_validated(value: String) -> Self {
        Self(value)
    }

    /// Returns the validated ETag.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns its validated string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for Etag {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<String> for Etag {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for Etag {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// A canonical single-object mutation condition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Precondition {
    /// Publish only when the logical key is absent.
    IfAbsent,
    /// Publish only when the current opaque ETag matches.
    IfMatch(Etag),
}

impl Precondition {
    fn from_validated(value: wire::Preconditions) -> Self {
        match value.condition {
            Some(wire::preconditions::Condition::IfAbsent(true)) => Self::IfAbsent,
            Some(wire::preconditions::Condition::IfMatch(value)) => {
                Self::IfMatch(Etag::from_validated(value))
            }
            _ => unreachable!("request validator admitted an invalid precondition"),
        }
    }
}

impl TryFrom<wire::Preconditions> for Precondition {
    type Error = Error;

    fn try_from(value: wire::Preconditions) -> Result<Self, Self::Error> {
        request::preconditions(&Some(value.clone()))?;
        match value.condition {
            Some(wire::preconditions::Condition::IfAbsent(true)) => Ok(Self::IfAbsent),
            Some(wire::preconditions::Condition::IfMatch(value)) => {
                Ok(Self::IfMatch(Etag::new(value)?))
            }
            _ => Err(wire::ErrorCode::InvalidArgument.into()),
        }
    }
}

impl From<Precondition> for wire::Preconditions {
    fn from(value: Precondition) -> Self {
        let condition = match value {
            Precondition::IfAbsent => wire::preconditions::Condition::IfAbsent(true),
            Precondition::IfMatch(value) => {
                wire::preconditions::Condition::IfMatch(value.into_string())
            }
        };
        Self {
            condition: Some(condition),
        }
    }
}

/// A typed streaming object upload header.
///
/// The generated header remains the transport form. This façade keeps the
/// bucket, key, metadata, precondition, and retry identity values validated by
/// the canonical upload-header validator before they reach a provider.
#[derive(Clone, Debug, PartialEq)]
pub struct PutObjectHeader {
    bucket: BucketName,
    object_key: ObjectKey,
    metadata: Option<ObjectMetadata>,
    preconditions: Option<Precondition>,
    mutation: Option<IdempotencyKey>,
}

impl PutObjectHeader {
    /// Creates an upload header for one validated bucket and object key.
    pub fn new(bucket: BucketName, object_key: ObjectKey) -> Self {
        Self {
            bucket,
            object_key,
            metadata: None,
            preconditions: None,
            mutation: None,
        }
    }

    /// Adds validated representation metadata to the upload.
    pub fn with_metadata(mut self, metadata: ObjectMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Adds an atomic current-value condition to the upload.
    pub fn with_precondition(mut self, precondition: Precondition) -> Self {
        self.preconditions = Some(precondition);
        self
    }

    /// Adds a caller retry identity to the upload.
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.mutation = Some(key);
        self
    }

    /// Returns the validated bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// Returns the validated object key.
    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns optional validated representation metadata.
    pub fn metadata(&self) -> Option<&ObjectMetadata> {
        self.metadata.as_ref()
    }

    /// Returns the optional atomic condition.
    pub fn precondition(&self) -> Option<&Precondition> {
        self.preconditions.as_ref()
    }

    /// Returns the optional caller retry identity.
    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.mutation.as_ref()
    }
}

impl TryFrom<wire::PutObjectHeader> for PutObjectHeader {
    type Error = Error;

    fn try_from(value: wire::PutObjectHeader) -> Result<Self, Self::Error> {
        request::put_stream_digest(&value)?;
        let wire::PutObjectHeader {
            bucket,
            object_key,
            metadata,
            preconditions,
            mutation,
        } = value;
        let bucket = bucket
            .ok_or(wire::ErrorCode::InvalidArgument)
            .map(|value| BucketName::from_validated(value.name))?;
        Ok(Self {
            bucket,
            object_key: ObjectKey::from_validated(object_key),
            metadata: metadata.map(ObjectMetadata::from_validated),
            preconditions: preconditions.map(Precondition::from_validated),
            mutation: mutation.map(|value| IdempotencyKey::from_validated(value.idempotency_key)),
        })
    }
}

impl From<PutObjectHeader> for wire::PutObjectHeader {
    fn from(value: PutObjectHeader) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
            object_key: value.object_key.into_string(),
            metadata: value.metadata.map(Into::into),
            preconditions: value.preconditions.map(Into::into),
            mutation: value.mutation.map(Into::into),
        }
    }
}

/// A typed request that starts independently staged multipart work.
#[derive(Clone, Debug, PartialEq)]
pub struct CreateMultipartRequest {
    bucket: BucketName,
    object_key: ObjectKey,
    metadata: Option<ObjectMetadata>,
    mutation: Option<IdempotencyKey>,
}

impl CreateMultipartRequest {
    /// Creates a multipart request for one validated bucket and object key.
    pub fn new(bucket: BucketName, object_key: ObjectKey) -> Self {
        Self {
            bucket,
            object_key,
            metadata: None,
            mutation: None,
        }
    }

    /// Adds validated representation metadata to the staged upload.
    pub fn with_metadata(mut self, metadata: ObjectMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Adds a caller retry identity to the staged upload.
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.mutation = Some(key);
        self
    }

    /// Returns the validated bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// Returns the validated object key.
    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns optional validated representation metadata.
    pub fn metadata(&self) -> Option<&ObjectMetadata> {
        self.metadata.as_ref()
    }

    /// Returns the optional caller retry identity.
    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.mutation.as_ref()
    }
}

impl TryFrom<wire::CreateMultipartRequest> for CreateMultipartRequest {
    type Error = Error;

    fn try_from(value: wire::CreateMultipartRequest) -> Result<Self, Self::Error> {
        request::create_multipart_digest(&value)?;
        let wire::CreateMultipartRequest {
            bucket,
            object_key,
            metadata,
            mutation,
        } = value;
        let bucket = bucket
            .ok_or(wire::ErrorCode::InvalidArgument)
            .map(|value| BucketName::from_validated(value.name))?;
        Ok(Self {
            bucket,
            object_key: ObjectKey::from_validated(object_key),
            metadata: metadata.map(ObjectMetadata::from_validated),
            mutation: mutation.map(|value| IdempotencyKey::from_validated(value.idempotency_key)),
        })
    }
}

impl From<CreateMultipartRequest> for wire::CreateMultipartRequest {
    fn from(value: CreateMultipartRequest) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
            object_key: value.object_key.into_string(),
            metadata: value.metadata.map(Into::into),
            mutation: value.mutation.map(Into::into),
        }
    }
}

/// A typed header for publishing one staged multipart part.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UploadPartHeader {
    bucket: BucketName,
    object_key: ObjectKey,
    upload_id: UploadId,
    part_number: PartNumber,
    mutation: Option<IdempotencyKey>,
}

impl UploadPartHeader {
    /// Creates a part header for one validated staged upload.
    pub fn new(
        bucket: BucketName,
        object_key: ObjectKey,
        upload_id: UploadId,
        part_number: PartNumber,
    ) -> Self {
        Self {
            bucket,
            object_key,
            upload_id,
            part_number,
            mutation: None,
        }
    }

    /// Adds a caller retry identity to the part publication.
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.mutation = Some(key);
        self
    }

    /// Returns the validated bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// Returns the validated object key.
    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns the validated staged upload identifier.
    pub fn upload_id(&self) -> &UploadId {
        &self.upload_id
    }

    /// Returns the validated multipart part number.
    pub const fn part_number(&self) -> PartNumber {
        self.part_number
    }

    /// Returns the optional caller retry identity.
    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.mutation.as_ref()
    }
}

impl TryFrom<wire::UploadPartHeader> for UploadPartHeader {
    type Error = Error;

    fn try_from(value: wire::UploadPartHeader) -> Result<Self, Self::Error> {
        request::upload_part_stream_digest(&value)?;
        let wire::UploadPartHeader {
            bucket,
            object_key,
            upload_id,
            part_number,
            mutation,
        } = value;
        let bucket = bucket
            .ok_or(wire::ErrorCode::InvalidArgument)
            .map(|value| BucketName::from_validated(value.name))?;
        Ok(Self {
            bucket,
            object_key: ObjectKey::from_validated(object_key),
            upload_id: UploadId::from_validated(upload_id),
            part_number: PartNumber::new(part_number)?,
            mutation: mutation.map(|value| IdempotencyKey::from_validated(value.idempotency_key)),
        })
    }
}

impl From<UploadPartHeader> for wire::UploadPartHeader {
    fn from(value: UploadPartHeader) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
            object_key: value.object_key.into_string(),
            upload_id: value.upload_id.into_string(),
            part_number: value.part_number.get(),
            mutation: value.mutation.map(Into::into),
        }
    }
}

/// A typed request that aborts staged multipart work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbortMultipartRequest {
    bucket: BucketName,
    object_key: ObjectKey,
    upload_id: UploadId,
    mutation: Option<IdempotencyKey>,
}

impl AbortMultipartRequest {
    /// Creates an abort request for one validated staged upload.
    pub fn new(bucket: BucketName, object_key: ObjectKey, upload_id: UploadId) -> Self {
        Self {
            bucket,
            object_key,
            upload_id,
            mutation: None,
        }
    }

    /// Adds a caller retry identity to the abort request.
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.mutation = Some(key);
        self
    }

    /// Returns the validated bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// Returns the validated object key.
    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns the validated staged upload identifier.
    pub fn upload_id(&self) -> &UploadId {
        &self.upload_id
    }

    /// Returns the optional caller retry identity.
    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.mutation.as_ref()
    }
}

impl TryFrom<wire::AbortMultipartRequest> for AbortMultipartRequest {
    type Error = Error;

    fn try_from(value: wire::AbortMultipartRequest) -> Result<Self, Self::Error> {
        request::abort_multipart_digest(&value)?;
        let wire::AbortMultipartRequest {
            bucket,
            object_key,
            upload_id,
            mutation,
        } = value;
        let bucket = bucket
            .ok_or(wire::ErrorCode::InvalidArgument)
            .map(|value| BucketName::from_validated(value.name))?;
        Ok(Self {
            bucket,
            object_key: ObjectKey::from_validated(object_key),
            upload_id: UploadId::from_validated(upload_id),
            mutation: mutation.map(|value| IdempotencyKey::from_validated(value.idempotency_key)),
        })
    }
}

impl From<AbortMultipartRequest> for wire::AbortMultipartRequest {
    fn from(value: AbortMultipartRequest) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
            object_key: value.object_key.into_string(),
            upload_id: value.upload_id.into_string(),
            mutation: value.mutation.map(Into::into),
        }
    }
}

/// A validated identifier returned when multipart staging begins.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartUpload {
    upload_id: UploadId,
}

impl MultipartUpload {
    /// Validates and owns one staged upload identifier.
    pub fn new(upload_id: UploadId) -> Self {
        Self { upload_id }
    }

    /// Validates a multipart creation response through the response contract.
    pub fn try_from_wire(value: wire::MultipartUpload) -> Result<Self, Error> {
        use prost::Message;
        response::validate_binary("multipart/create", &[], &value.encode_to_vec(), 0)?;
        Ok(Self {
            upload_id: UploadId::from_validated(value.upload_id),
        })
    }

    /// Returns the validated staged upload identifier.
    pub fn upload_id(&self) -> &UploadId {
        &self.upload_id
    }
}

impl TryFrom<wire::MultipartUpload> for MultipartUpload {
    type Error = Error;

    fn try_from(value: wire::MultipartUpload) -> Result<Self, Self::Error> {
        Self::try_from_wire(value)
    }
}

impl From<MultipartUpload> for wire::MultipartUpload {
    fn from(value: MultipartUpload) -> Self {
        Self {
            upload_id: value.upload_id.into_string(),
        }
    }
}

/// A validated part receipt selected when completing a multipart upload.
///
/// The part number, ETag, and decoded size are kept together so a completion
/// request cannot accidentally select a receipt with an invalid field. The
/// ordering and nonempty-list rules remain request-level invariants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletedPart {
    part_number: PartNumber,
    etag: Etag,
    size: u64,
}

impl CompletedPart {
    /// Validates one receipt through the canonical uploaded-part predicates.
    pub fn new(part_number: PartNumber, etag: Etag, size: u64) -> Result<Self, Error> {
        let value = wire::UploadedPart {
            part_number: part_number.get(),
            etag: etag.as_str().to_owned(),
            size,
        };
        request::validate_uploaded_part(&value, 0, false)?;
        Ok(Self {
            part_number,
            etag,
            size,
        })
    }

    /// Returns the validated multipart part number.
    pub const fn part_number(&self) -> PartNumber {
        self.part_number
    }

    /// Returns the validated opaque receipt ETag.
    pub fn etag(&self) -> &Etag {
        &self.etag
    }

    /// Returns the decoded size recorded in the receipt.
    pub const fn size(&self) -> u64 {
        self.size
    }
}

impl TryFrom<wire::UploadedPart> for CompletedPart {
    type Error = Error;

    fn try_from(value: wire::UploadedPart) -> Result<Self, Self::Error> {
        request::validate_uploaded_part(&value, 0, false)?;
        Ok(Self {
            part_number: PartNumber::new(value.part_number)?,
            etag: Etag::from_validated(value.etag),
            size: value.size,
        })
    }
}

impl From<CompletedPart> for wire::UploadedPart {
    fn from(value: CompletedPart) -> Self {
        Self {
            part_number: value.part_number.get(),
            etag: value.etag.into_string(),
            size: value.size,
        }
    }
}

/// A typed live eventual object listing request.
///
/// Empty strings in the generated request are represented as `None` here.
/// Likewise, a wire page size of zero means the protocol default and is kept
/// distinct from a caller-supplied [`PageSize`]. Continuation tokens remain
/// opaque; this type checks only their wire length and NUL restriction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListObjectsRequest {
    bucket: BucketName,
    prefix: Option<String>,
    delimiter: Option<String>,
    page_size: Option<PageSize>,
    continuation_token: Option<String>,
}

impl ListObjectsRequest {
    /// Creates a listing request using the protocol's default filters and page size.
    pub fn new(bucket: BucketName) -> Self {
        Self {
            bucket,
            prefix: None,
            delimiter: None,
            page_size: None,
            continuation_token: None,
        }
    }

    /// Adds a prefix filter, validating its wire representation.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Result<Self, Error> {
        let prefix = prefix.into();
        self.prefix = (!prefix.is_empty()).then_some(prefix);
        self.validate()?;
        Ok(self)
    }

    /// Adds a delimiter filter, validating its wire representation.
    pub fn with_delimiter(mut self, delimiter: impl Into<String>) -> Result<Self, Error> {
        let delimiter = delimiter.into();
        self.delimiter = (!delimiter.is_empty()).then_some(delimiter);
        self.validate()?;
        Ok(self)
    }

    /// Uses a caller-supplied nonzero page size instead of the wire default.
    pub fn with_page_size(mut self, page_size: PageSize) -> Self {
        self.page_size = Some(page_size);
        self
    }

    /// Resumes from an opaque continuation token after checking wire bounds.
    pub fn with_continuation_token(mut self, token: impl Into<String>) -> Result<Self, Error> {
        let token = token.into();
        self.continuation_token = (!token.is_empty()).then_some(token);
        self.validate()?;
        Ok(self)
    }

    /// Returns the validated logical bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// Returns the optional prefix filter.
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// Returns the optional delimiter filter.
    pub fn delimiter(&self) -> Option<&str> {
        self.delimiter.as_deref()
    }

    /// Returns the caller page size, or `None` for the protocol default.
    pub fn page_size(&self) -> Option<PageSize> {
        self.page_size
    }

    /// Returns the opaque continuation token, if this request resumes a page.
    pub fn continuation_token(&self) -> Option<&str> {
        self.continuation_token.as_deref()
    }

    fn validate(&self) -> Result<(), Error> {
        use prost::Message;
        request::validate_binary(
            "objects/list",
            &wire::ListObjectsRequest::from(self.clone()).encode_to_vec(),
            0,
        )
    }
}

impl TryFrom<wire::ListObjectsRequest> for ListObjectsRequest {
    type Error = Error;

    fn try_from(value: wire::ListObjectsRequest) -> Result<Self, Self::Error> {
        use prost::Message;
        request::validate_binary("objects/list", &value.encode_to_vec(), 0)?;
        let wire::ListObjectsRequest {
            bucket,
            prefix,
            delimiter,
            page_size,
            continuation_token,
        } = value;
        Ok(Self {
            bucket: BucketName::try_from(bucket.ok_or(wire::ErrorCode::InvalidArgument)?)?,
            prefix: (!prefix.is_empty()).then_some(prefix),
            delimiter: (!delimiter.is_empty()).then_some(delimiter),
            page_size: (page_size != 0).then(|| PageSize::new(page_size)).transpose()?,
            continuation_token: (!continuation_token.is_empty()).then_some(continuation_token),
        })
    }
}

impl From<ListObjectsRequest> for wire::ListObjectsRequest {
    fn from(value: ListObjectsRequest) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
            prefix: value.prefix.unwrap_or_default(),
            delimiter: value.delimiter.unwrap_or_default(),
            page_size: value.page_size.map(PageSize::get).unwrap_or_default(),
            continuation_token: value.continuation_token.unwrap_or_default(),
        }
    }
}

/// A typed request for a page of receipts from one staged multipart upload.
///
/// A zero wire `after_part_number` and page size are represented as `None` so
/// default traversal remains distinguishable from explicit values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPartsRequest {
    bucket: BucketName,
    object_key: ObjectKey,
    upload_id: UploadId,
    after_part_number: Option<PartNumber>,
    page_size: Option<PageSize>,
}

impl ListPartsRequest {
    /// Creates a first-page request using both protocol defaults.
    pub fn new(bucket: BucketName, object_key: ObjectKey, upload_id: UploadId) -> Self {
        Self {
            bucket,
            object_key,
            upload_id,
            after_part_number: None,
            page_size: None,
        }
    }

    /// Starts after a validated inclusive part number.
    pub fn with_after_part_number(mut self, part_number: PartNumber) -> Self {
        self.after_part_number = Some(part_number);
        self
    }

    /// Uses a caller-supplied nonzero page size instead of the wire default.
    pub fn with_page_size(mut self, page_size: PageSize) -> Self {
        self.page_size = Some(page_size);
        self
    }

    /// Returns the validated bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// Returns the validated object key.
    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns the validated staged upload identifier.
    pub fn upload_id(&self) -> &UploadId {
        &self.upload_id
    }

    /// Returns the optional part-number cursor.
    pub const fn after_part_number(&self) -> Option<PartNumber> {
        self.after_part_number
    }

    /// Returns the caller page size, or `None` for the protocol default.
    pub const fn page_size(&self) -> Option<PageSize> {
        self.page_size
    }
}

impl TryFrom<wire::ListPartsRequest> for ListPartsRequest {
    type Error = Error;

    fn try_from(value: wire::ListPartsRequest) -> Result<Self, Self::Error> {
        use prost::Message;
        request::validate_binary("multipart/list-parts", &value.encode_to_vec(), 0)?;
        let wire::ListPartsRequest {
            bucket,
            object_key,
            upload_id,
            after_part_number,
            page_size,
        } = value;
        Ok(Self {
            bucket: BucketName::try_from(bucket.ok_or(wire::ErrorCode::InvalidArgument)?)?,
            object_key: ObjectKey::from_validated(object_key),
            upload_id: UploadId::from_validated(upload_id),
            after_part_number: (after_part_number != 0)
                .then(|| PartNumber::new(after_part_number))
                .transpose()?,
            page_size: (page_size != 0).then(|| PageSize::new(page_size)).transpose()?,
        })
    }
}

impl From<ListPartsRequest> for wire::ListPartsRequest {
    fn from(value: ListPartsRequest) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
            object_key: value.object_key.into_string(),
            upload_id: value.upload_id.into_string(),
            after_part_number: value
                .after_part_number
                .map(PartNumber::get)
                .unwrap_or_default(),
            page_size: value.page_size.map(PageSize::get).unwrap_or_default(),
        }
    }
}

/// A typed request that atomically publishes selected multipart receipts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteMultipartRequest {
    bucket: BucketName,
    object_key: ObjectKey,
    upload_id: UploadId,
    parts: Vec<CompletedPart>,
    preconditions: Option<Precondition>,
    mutation: Option<IdempotencyKey>,
}

impl CompleteMultipartRequest {
    /// Creates a completion request after validating nonempty ordered receipts.
    pub fn new(
        bucket: BucketName,
        object_key: ObjectKey,
        upload_id: UploadId,
        parts: Vec<CompletedPart>,
    ) -> Result<Self, Error> {
        let value = Self {
            bucket,
            object_key,
            upload_id,
            parts,
            preconditions: None,
            mutation: None,
        };
        value.validate()?;
        Ok(value)
    }

    /// Adds an atomic current-value condition.
    pub fn with_precondition(mut self, precondition: Precondition) -> Self {
        self.preconditions = Some(precondition);
        self
    }

    /// Adds a caller retry identity.
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.mutation = Some(key);
        self
    }

    /// Returns the validated bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// Returns the validated object key.
    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns the validated staged upload identifier.
    pub fn upload_id(&self) -> &UploadId {
        &self.upload_id
    }

    /// Returns the ordered validated completion receipts.
    pub fn parts(&self) -> &[CompletedPart] {
        &self.parts
    }

    /// Returns the optional atomic condition.
    pub fn precondition(&self) -> Option<&Precondition> {
        self.preconditions.as_ref()
    }

    /// Returns the optional caller retry identity.
    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.mutation.as_ref()
    }

    fn validate(&self) -> Result<(), Error> {
        request::complete_multipart_digest(&wire::CompleteMultipartRequest::from(self.clone()))
            .map(|_| ())
    }
}

impl TryFrom<wire::CompleteMultipartRequest> for CompleteMultipartRequest {
    type Error = Error;

    fn try_from(value: wire::CompleteMultipartRequest) -> Result<Self, Self::Error> {
        request::complete_multipart_digest(&value)?;
        let wire::CompleteMultipartRequest {
            bucket,
            object_key,
            upload_id,
            parts,
            preconditions,
            mutation,
        } = value;
        Ok(Self {
            bucket: BucketName::try_from(bucket.ok_or(wire::ErrorCode::InvalidArgument)?)?,
            object_key: ObjectKey::from_validated(object_key),
            upload_id: UploadId::from_validated(upload_id),
            parts: parts
                .into_iter()
                .map(CompletedPart::try_from)
                .collect::<Result<_, _>>()?,
            preconditions: preconditions.map(Precondition::try_from).transpose()?,
            mutation: mutation.map(IdempotencyKey::try_from).transpose()?,
        })
    }
}

impl From<CompleteMultipartRequest> for wire::CompleteMultipartRequest {
    fn from(value: CompleteMultipartRequest) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
            object_key: value.object_key.into_string(),
            upload_id: value.upload_id.into_string(),
            parts: value.parts.into_iter().map(Into::into).collect(),
            preconditions: value.preconditions.map(Into::into),
            mutation: value.mutation.map(Into::into),
        }
    }
}

/// A typed object read request with explicit optional range and ETag filters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetObjectRequest {
    bucket: BucketName,
    object_key: ObjectKey,
    range: Option<ByteSelection>,
    if_match: Option<Etag>,
    if_none_match: Option<Etag>,
}

impl GetObjectRequest {
    /// Creates a read request for one validated bucket and object key.
    pub fn new(bucket: BucketName, object_key: ObjectKey) -> Self {
        Self {
            bucket,
            object_key,
            range: None,
            if_match: None,
            if_none_match: None,
        }
    }

    /// Adds a validated inclusive or suffix byte selection.
    pub fn with_range(mut self, range: ByteSelection) -> Self {
        self.range = Some(range);
        self
    }

    /// Adds a validated current ETag filter.
    pub fn with_if_match(mut self, etag: Etag) -> Self {
        self.if_match = Some(etag);
        self
    }

    /// Adds a validated nonmatching ETag filter.
    pub fn with_if_none_match(mut self, etag: Etag) -> Self {
        self.if_none_match = Some(etag);
        self
    }

    /// Returns the validated bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// Returns the validated object key.
    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns the optional validated byte selection.
    pub fn range(&self) -> Option<ByteSelection> {
        self.range
    }

    /// Returns the optional current ETag filter.
    pub fn if_match(&self) -> Option<&Etag> {
        self.if_match.as_ref()
    }

    /// Returns the optional nonmatching ETag filter.
    pub fn if_none_match(&self) -> Option<&Etag> {
        self.if_none_match.as_ref()
    }
}

impl TryFrom<wire::GetObjectRequest> for GetObjectRequest {
    type Error = Error;

    fn try_from(value: wire::GetObjectRequest) -> Result<Self, Self::Error> {
        use prost::Message;
        request::validate_binary("objects/get", &value.encode_to_vec(), 0)?;
        let wire::GetObjectRequest {
            bucket,
            object_key,
            range,
            if_match,
            if_none_match,
        } = value;
        let bucket = bucket
            .ok_or(wire::ErrorCode::InvalidArgument)
            .map(|value| BucketName::from_validated(value.name))?;
        Ok(Self {
            bucket,
            object_key: ObjectKey::from_validated(object_key),
            range: range.map(ByteSelection::try_from).transpose()?,
            if_match: (!if_match.is_empty()).then(|| Etag::from_validated(if_match)),
            if_none_match: (!if_none_match.is_empty())
                .then(|| Etag::from_validated(if_none_match)),
        })
    }
}

impl From<GetObjectRequest> for wire::GetObjectRequest {
    fn from(value: GetObjectRequest) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
            object_key: value.object_key.into_string(),
            range: value.range.map(Into::into),
            if_match: value.if_match.map(Etag::into_string).unwrap_or_default(),
            if_none_match: value
                .if_none_match
                .map(Etag::into_string)
                .unwrap_or_default(),
        }
    }
}

/// A typed object metadata read request with explicit optional ETag filters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadObjectRequest {
    bucket: BucketName,
    object_key: ObjectKey,
    if_match: Option<Etag>,
    if_none_match: Option<Etag>,
}

impl HeadObjectRequest {
    /// Creates a metadata request for one validated bucket and object key.
    pub fn new(bucket: BucketName, object_key: ObjectKey) -> Self {
        Self {
            bucket,
            object_key,
            if_match: None,
            if_none_match: None,
        }
    }

    /// Adds a validated current ETag filter.
    pub fn with_if_match(mut self, etag: Etag) -> Self {
        self.if_match = Some(etag);
        self
    }

    /// Adds a validated nonmatching ETag filter.
    pub fn with_if_none_match(mut self, etag: Etag) -> Self {
        self.if_none_match = Some(etag);
        self
    }

    /// Returns the validated bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// Returns the validated object key.
    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns the optional current ETag filter.
    pub fn if_match(&self) -> Option<&Etag> {
        self.if_match.as_ref()
    }

    /// Returns the optional nonmatching ETag filter.
    pub fn if_none_match(&self) -> Option<&Etag> {
        self.if_none_match.as_ref()
    }
}

impl TryFrom<wire::HeadObjectRequest> for HeadObjectRequest {
    type Error = Error;

    fn try_from(value: wire::HeadObjectRequest) -> Result<Self, Self::Error> {
        use prost::Message;
        request::validate_binary("objects/head", &value.encode_to_vec(), 0)?;
        let wire::HeadObjectRequest {
            bucket,
            object_key,
            if_match,
            if_none_match,
        } = value;
        let bucket = bucket
            .ok_or(wire::ErrorCode::InvalidArgument)
            .map(|value| BucketName::from_validated(value.name))?;
        Ok(Self {
            bucket,
            object_key: ObjectKey::from_validated(object_key),
            if_match: (!if_match.is_empty()).then(|| Etag::from_validated(if_match)),
            if_none_match: (!if_none_match.is_empty())
                .then(|| Etag::from_validated(if_none_match)),
        })
    }
}

impl From<HeadObjectRequest> for wire::HeadObjectRequest {
    fn from(value: HeadObjectRequest) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
            object_key: value.object_key.into_string(),
            if_match: value.if_match.map(Etag::into_string).unwrap_or_default(),
            if_none_match: value
                .if_none_match
                .map(Etag::into_string)
                .unwrap_or_default(),
        }
    }
}

/// A typed object deletion request.
///
/// The generated request remains the transport form; this façade keeps its
/// bucket, key, condition, and retry identity values validated by construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteObjectRequest {
    bucket: BucketName,
    object_key: ObjectKey,
    preconditions: Option<Precondition>,
    mutation: Option<IdempotencyKey>,
}

impl DeleteObjectRequest {
    /// Creates a deletion request for one validated bucket and object key.
    pub fn new(bucket: BucketName, object_key: ObjectKey) -> Self {
        Self {
            bucket,
            object_key,
            preconditions: None,
            mutation: None,
        }
    }

    /// Adds an atomic current-value condition.
    pub fn with_precondition(mut self, precondition: Precondition) -> Self {
        self.preconditions = Some(precondition);
        self
    }

    /// Adds a caller retry identity.
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.mutation = Some(key);
        self
    }

    /// Returns the validated bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// Returns the validated object key.
    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns the optional atomic condition.
    pub fn precondition(&self) -> Option<&Precondition> {
        self.preconditions.as_ref()
    }

    /// Returns the optional caller retry identity.
    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.mutation.as_ref()
    }
}

impl TryFrom<wire::DeleteObjectRequest> for DeleteObjectRequest {
    type Error = Error;

    fn try_from(value: wire::DeleteObjectRequest) -> Result<Self, Self::Error> {
        request::delete_digest(&value)?;
        let bucket = BucketName::try_from(value.bucket.ok_or(wire::ErrorCode::InvalidArgument)?)?;
        let object_key = ObjectKey::new(value.object_key)?;
        let preconditions = value
            .preconditions
            .map(Precondition::try_from)
            .transpose()?;
        let mutation = value
            .mutation
            .map(IdempotencyKey::try_from)
            .transpose()?;
        Ok(Self {
            bucket,
            object_key,
            preconditions,
            mutation,
        })
    }
}

impl From<DeleteObjectRequest> for wire::DeleteObjectRequest {
    fn from(value: DeleteObjectRequest) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
            object_key: value.object_key.into_string(),
            preconditions: value.preconditions.map(Into::into),
            mutation: value.mutation.map(Into::into),
        }
    }
}

/// A typed logical bucket creation request.
///
/// The generated request remains the transport form; this façade keeps the
/// bucket name and optional retry identity validated by construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateBucketRequest {
    name: BucketName,
    mutation: Option<IdempotencyKey>,
}

impl CreateBucketRequest {
    /// Creates a bucket request for one validated logical bucket name.
    pub fn new(name: BucketName) -> Self {
        Self {
            name,
            mutation: None,
        }
    }

    /// Adds a caller retry identity.
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.mutation = Some(key);
        self
    }

    /// Returns the validated logical bucket name.
    pub fn name(&self) -> &BucketName {
        &self.name
    }

    /// Returns the optional caller retry identity.
    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.mutation.as_ref()
    }
}

impl TryFrom<wire::CreateBucketRequest> for CreateBucketRequest {
    type Error = Error;

    fn try_from(value: wire::CreateBucketRequest) -> Result<Self, Self::Error> {
        request::create_bucket_digest(&value)?;
        let name = BucketName::try_from(value.name)?;
        let mutation = value
            .mutation
            .map(IdempotencyKey::try_from)
            .transpose()?;
        Ok(Self { name, mutation })
    }
}

impl From<CreateBucketRequest> for wire::CreateBucketRequest {
    fn from(value: CreateBucketRequest) -> Self {
        Self {
            name: value.name.into_string(),
            mutation: value.mutation.map(Into::into),
        }
    }
}

/// A typed logical bucket inspection request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadBucketRequest {
    bucket: BucketName,
}

impl HeadBucketRequest {
    /// Creates a bucket inspection request for one validated name.
    pub fn new(bucket: BucketName) -> Self {
        Self { bucket }
    }

    /// Returns the validated logical bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }
}

impl TryFrom<wire::HeadBucketRequest> for HeadBucketRequest {
    type Error = Error;

    fn try_from(value: wire::HeadBucketRequest) -> Result<Self, Self::Error> {
        request::bucket(&value.bucket)?;
        let bucket = BucketName::try_from(value.bucket.ok_or(wire::ErrorCode::InvalidArgument)?)?;
        Ok(Self { bucket })
    }
}

impl From<HeadBucketRequest> for wire::HeadBucketRequest {
    fn from(value: HeadBucketRequest) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
        }
    }
}

/// A typed logical bucket deletion request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteBucketRequest {
    bucket: BucketName,
    mutation: Option<IdempotencyKey>,
}

impl DeleteBucketRequest {
    /// Creates a bucket deletion request for one validated logical bucket.
    pub fn new(bucket: BucketName) -> Self {
        Self {
            bucket,
            mutation: None,
        }
    }

    /// Adds a caller retry identity.
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.mutation = Some(key);
        self
    }

    /// Returns the validated logical bucket name.
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// Returns the optional caller retry identity.
    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.mutation.as_ref()
    }
}

impl TryFrom<wire::DeleteBucketRequest> for DeleteBucketRequest {
    type Error = Error;

    fn try_from(value: wire::DeleteBucketRequest) -> Result<Self, Self::Error> {
        request::delete_bucket_digest(&value)?;
        let bucket = BucketName::try_from(value.bucket.ok_or(wire::ErrorCode::InvalidArgument)?)?;
        let mutation = value
            .mutation
            .map(IdempotencyKey::try_from)
            .transpose()?;
        Ok(Self { bucket, mutation })
    }
}

impl From<DeleteBucketRequest> for wire::DeleteBucketRequest {
    fn from(value: DeleteBucketRequest) -> Self {
        Self {
            bucket: Some(value.bucket.into()),
            mutation: value.mutation.map(Into::into),
        }
    }
}

/// A timestamp accepted by the Objects response contract.
///
/// The underlying protobuf value stays private so callers cannot construct an
/// invalid timestamp through the semantic API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedTimestamp(prost_types::Timestamp);

impl ValidatedTimestamp {
    /// Returns the validated Unix timestamp seconds.
    pub const fn seconds(&self) -> i64 {
        self.0.seconds
    }

    /// Returns the validated nanosecond fraction.
    pub const fn nanos(&self) -> i32 {
        self.0.nanos
    }

    /// Borrows the validated protobuf timestamp for transport integration.
    pub fn as_ref(&self) -> &prost_types::Timestamp {
        &self.0
    }

    /// Consumes the wrapper and returns its validated protobuf timestamp.
    pub fn into_inner(self) -> prost_types::Timestamp {
        self.0
    }

    fn from_validated(value: prost_types::Timestamp) -> Self {
        Self(value)
    }
}

impl AsRef<prost_types::Timestamp> for ValidatedTimestamp {
    fn as_ref(&self) -> &prost_types::Timestamp {
        &self.0
    }
}

impl TryFrom<prost_types::Timestamp> for ValidatedTimestamp {
    type Error = Error;

    fn try_from(value: prost_types::Timestamp) -> Result<Self, Self::Error> {
        response::timestamp(&value)?;
        Ok(Self(value))
    }
}

/// Validated representation metadata for a current object.
///
/// The generated metadata message remains private inside this wrapper so
/// callers can only obtain one after the canonical header validation passes.
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectMetadata(wire::ObjectMetadata);

impl ObjectMetadata {
    /// Validates and owns representation metadata for a request or response.
    pub fn new(value: wire::ObjectMetadata) -> Result<Self, Error> {
        request::metadata(&Some(value.clone()))?;
        Ok(Self(value))
    }

    fn from_validated(value: wire::ObjectMetadata) -> Self {
        Self(value)
    }

    /// Returns the validated content type.
    pub fn content_type(&self) -> &str {
        &self.0.content_type
    }

    /// Returns the validated user metadata map.
    pub fn user(&self) -> &std::collections::BTreeMap<String, String> {
        &self.0.user
    }

    /// Returns the validated content encoding.
    pub fn content_encoding(&self) -> &str {
        &self.0.content_encoding
    }

    /// Returns the validated cache-control value.
    pub fn cache_control(&self) -> &str {
        &self.0.cache_control
    }

    /// Returns the validated content-disposition value.
    pub fn content_disposition(&self) -> &str {
        &self.0.content_disposition
    }

    /// Returns the validated content-language value.
    pub fn content_language(&self) -> &str {
        &self.0.content_language
    }

    /// Returns the optional validated expiry Unix timestamp in seconds.
    pub fn expires_unix_seconds(&self) -> Option<i64> {
        self.0.expires_unix_seconds
    }

    /// Borrows the validated protobuf metadata for transport integration.
    pub fn as_ref(&self) -> &wire::ObjectMetadata {
        &self.0
    }

    /// Consumes the wrapper and returns its validated protobuf metadata.
    pub fn into_inner(self) -> wire::ObjectMetadata {
        self.0
    }
}

impl AsRef<wire::ObjectMetadata> for ObjectMetadata {
    fn as_ref(&self) -> &wire::ObjectMetadata {
        &self.0
    }
}

impl TryFrom<wire::ObjectMetadata> for ObjectMetadata {
    type Error = Error;

    fn try_from(value: wire::ObjectMetadata) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ObjectMetadata> for wire::ObjectMetadata {
    fn from(value: ObjectMetadata) -> Self {
        value.into_inner()
    }
}

/// A validated current object representation.
///
/// Construction delegates to [`response::object_info`], which is the
/// canonical response validator for ETag, metadata, and timestamp semantics.
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectInfo {
    etag: Etag,
    size: u64,
    metadata: Option<ObjectMetadata>,
    last_modified: ValidatedTimestamp,
}

impl ObjectInfo {
    /// Validates and owns one wire representation metadata response.
    pub fn try_from_wire(value: wire::ObjectInfo) -> Result<Self, Error> {
        response::object_info(&value)?;
        let metadata = value
            .metadata
            .map(ObjectMetadata::from_validated);
        let last_modified = ValidatedTimestamp::from_validated(
            value
                .last_modified
                .ok_or(wire::ErrorCode::InvalidArgument)?,
        );
        Ok(Self {
            etag: Etag::from_validated(value.etag),
            size: value.size,
            metadata,
            last_modified,
        })
    }

    /// Returns the validated opaque ETag.
    pub fn etag(&self) -> &Etag {
        &self.etag
    }

    /// Returns the complete representation size in bytes.
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Returns optional validated representation metadata.
    pub fn metadata(&self) -> Option<&ObjectMetadata> {
        self.metadata.as_ref()
    }

    /// Returns the validated last-modified timestamp.
    pub fn last_modified(&self) -> &ValidatedTimestamp {
        &self.last_modified
    }
}

impl TryFrom<wire::ObjectInfo> for ObjectInfo {
    type Error = Error;

    fn try_from(value: wire::ObjectInfo) -> Result<Self, Self::Error> {
        Self::try_from_wire(value)
    }
}

impl From<ObjectInfo> for wire::ObjectInfo {
    fn from(value: ObjectInfo) -> Self {
        Self {
            etag: value.etag.into_string(),
            size: value.size,
            metadata: value.metadata.map(ObjectMetadata::into_inner),
            last_modified: Some(value.last_modified.into_inner()),
        }
    }
}

/// A validated, bounded object download returned by a typed provider.
///
/// The body length and optional range are checked against the request and
/// response header before this value is constructed.
#[derive(Clone, Debug, PartialEq)]
pub struct DownloadedObject {
    info: ObjectInfo,
    content_range: Option<ContentRange>,
    body: bytes::Bytes,
}

impl DownloadedObject {
    /// Validates a provider download against its typed request and allocation bound.
    pub fn try_from_wire(
        value: super::Object,
        request: &GetObjectRequest,
        maximum: u64,
    ) -> Result<Self, Error> {
        let wire_request = wire::GetObjectRequest::from(request.clone());
        let expected = response::get_header(&value.header, &wire_request.range, maximum)?;
        if value.body.len() as u64 != expected {
            return Err(response::invalid());
        }
        let content_range = match value.header.content_range {
            Some(range) => Some(ContentRange::try_from_wire(
                range,
                request
                    .range()
                    .ok_or_else(response::invalid)?,
                value
                    .header
                    .object
                    .as_ref()
                    .ok_or_else(response::invalid)?
                    .size,
            )?),
            None => None,
        };
        let info = ObjectInfo::try_from(
            value
                .header
                .object
                .ok_or_else(response::invalid)?,
        )?;
        Ok(Self {
            info,
            content_range,
            body: value.body,
        })
    }

    /// Returns validated object metadata for the selected representation.
    pub fn info(&self) -> &ObjectInfo {
        &self.info
    }

    /// Returns the canonical range when the request selected a byte range.
    pub fn content_range(&self) -> Option<ContentRange> {
        self.content_range
    }

    /// Returns the bounded selected bytes.
    pub fn body(&self) -> &bytes::Bytes {
        &self.body
    }

    /// Consumes the wrapper and returns the bounded selected bytes.
    pub fn into_body(self) -> bytes::Bytes {
        self.body
    }
}

/// A validated page from an eventual object listing.
#[derive(Clone, Debug, PartialEq)]
pub struct ListObjectsPage {
    entries: Vec<(ObjectKey, ObjectInfo)>,
    common_prefixes: Vec<String>,
    continuation_token: Option<String>,
    is_truncated: bool,
}

impl ListObjectsPage {
    /// Validates a listing page against the typed request and converts its entries.
    pub fn try_from_wire(
        value: wire::ListObjectsResponse,
        request: &ListObjectsRequest,
    ) -> Result<Self, Error> {
        let wire_request = wire::ListObjectsRequest::from(request.clone());
        response::listing(&wire_request, &value)?;
        let entries = value
            .entries
            .into_iter()
            .map(|entry| {
                let object = entry.object.ok_or_else(response::invalid)?;
                Ok((ObjectKey::try_from(entry.object_key)?, ObjectInfo::try_from(object)?))
            })
            .collect::<Result<_, Error>>()?;
        Ok(Self {
            entries,
            common_prefixes: value.common_prefixes,
            continuation_token: (!value.continuation_token.is_empty())
                .then_some(value.continuation_token),
            is_truncated: value.is_truncated,
        })
    }

    /// Returns validated object entries in provider order.
    pub fn entries(&self) -> &[(ObjectKey, ObjectInfo)] {
        &self.entries
    }

    /// Returns validated common prefixes in provider order.
    pub fn common_prefixes(&self) -> &[String] {
        &self.common_prefixes
    }

    /// Returns the opaque next-page token, if the page is truncated.
    pub fn continuation_token(&self) -> Option<&str> {
        self.continuation_token.as_deref()
    }

    /// Returns whether another page is available.
    pub const fn is_truncated(&self) -> bool {
        self.is_truncated
    }
}

/// A validated page of receipts from one staged multipart upload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPartsPage {
    parts: Vec<CompletedPart>,
    next_part_number: Option<PartNumber>,
    is_truncated: bool,
}

impl ListPartsPage {
    /// Validates a parts page against its typed request and converts its receipts.
    pub fn try_from_wire(
        value: wire::ListPartsResponse,
        request: &ListPartsRequest,
    ) -> Result<Self, Error> {
        let wire_request = wire::ListPartsRequest::from(request.clone());
        response::parts(&wire_request, &value)?;
        let next_part_number = (value.next_part_number != 0)
            .then(|| PartNumber::new(value.next_part_number))
            .transpose()?;
        let parts = value
            .parts
            .into_iter()
            .map(CompletedPart::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            parts,
            next_part_number,
            is_truncated: value.is_truncated,
        })
    }

    /// Returns validated multipart receipts in provider order.
    pub fn parts(&self) -> &[CompletedPart] {
        &self.parts
    }

    /// Returns the next part cursor when the page is truncated.
    pub const fn next_part_number(&self) -> Option<PartNumber> {
        self.next_part_number
    }

    /// Returns whether another page is available.
    pub const fn is_truncated(&self) -> bool {
        self.is_truncated
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ByteSelectionKind {
    Bytes { start: u64, end: Option<u64> },
    Suffix(NonZeroU64),
}

/// A validated byte-range selection for a complete object representation.
///
/// The end of an inclusive byte range remains optional. Suffix selections use
/// `NonZeroU64`, so the wire zero sentinel cannot be represented as a suffix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteSelection(ByteSelectionKind);

impl ByteSelection {
    /// Constructs an inclusive byte selection and validates its wire shape.
    pub fn bytes(start: u64, end: Option<u64>) -> Result<Self, Error> {
        let selection = Self(ByteSelectionKind::Bytes { start, end });
        request::range(&Some(selection.to_wire()), u64::MAX)?;
        Ok(selection)
    }

    /// Constructs a nonzero suffix selection.
    pub fn suffix(length: u64) -> Result<Self, Error> {
        let length = NonZeroU64::new(length)
            .ok_or(wire::ErrorCode::RangeNotSatisfiable)?;
        let selection = Self(ByteSelectionKind::Suffix(length));
        request::range(&Some(selection.to_wire()), u64::MAX)?;
        Ok(selection)
    }

    /// Returns the inclusive start for a byte selection, if present.
    pub const fn start(self) -> Option<u64> {
        match self.0 {
            ByteSelectionKind::Bytes { start, .. } => Some(start),
            ByteSelectionKind::Suffix(_) => None,
        }
    }

    /// Returns the optional inclusive end for a byte selection.
    pub const fn end(self) -> Option<u64> {
        match self.0 {
            ByteSelectionKind::Bytes { end, .. } => end,
            ByteSelectionKind::Suffix(_) => None,
        }
    }

    /// Returns the nonzero suffix length, if this is a suffix selection.
    pub const fn suffix_length(self) -> Option<NonZeroU64> {
        match self.0 {
            ByteSelectionKind::Bytes { .. } => None,
            ByteSelectionKind::Suffix(length) => Some(length),
        }
    }

    /// Resolves this selection through the canonical object-range validator.
    pub fn resolve(self, total: u64) -> Result<ContentRange, Error> {
        let value = request::range(&Some(self.to_wire()), total)?
            .ok_or(wire::ErrorCode::RangeNotSatisfiable)?;
        Ok(ContentRange::from_validated(value))
    }

    fn to_wire(self) -> wire::ByteRange {
        let selection = match self.0 {
            ByteSelectionKind::Bytes { start, end } => {
                wire::byte_range::Selection::Bytes(wire::InclusiveRange { start, end })
            }
            ByteSelectionKind::Suffix(length) => {
                wire::byte_range::Selection::SuffixLength(length.get())
            }
        };
        wire::ByteRange {
            selection: Some(selection),
        }
    }
}

impl TryFrom<wire::ByteRange> for ByteSelection {
    type Error = Error;

    fn try_from(value: wire::ByteRange) -> Result<Self, Self::Error> {
        match value.selection {
            Some(wire::byte_range::Selection::Bytes(value)) => Self::bytes(value.start, value.end),
            Some(wire::byte_range::Selection::SuffixLength(length)) => Self::suffix(length),
            None => Err(wire::ErrorCode::RangeNotSatisfiable.into()),
        }
    }
}

impl From<ByteSelection> for wire::ByteRange {
    fn from(value: ByteSelection) -> Self {
        value.to_wire()
    }
}

/// A canonical inclusive byte range resolved against a complete object size.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContentRange {
    start: u64,
    end: u64,
    total: u64,
}

impl ContentRange {
    fn from_validated(value: wire::ContentRange) -> Self {
        Self {
            start: value.start,
            end: value.end,
            total: value.total,
        }
    }

    /// Resolves a selection and returns its canonical response range.
    pub fn from_selection(selection: ByteSelection, total: u64) -> Result<Self, Error> {
        selection.resolve(total)
    }

    /// Validates a response range against the original selection and size.
    pub fn try_from_wire(
        value: wire::ContentRange,
        selection: ByteSelection,
        total: u64,
    ) -> Result<Self, Error> {
        let expected = selection.resolve(total)?;
        if wire::ContentRange::from(expected) != value {
            return Err(wire::ErrorCode::RangeNotSatisfiable.into());
        }
        Ok(expected)
    }

    /// Returns the inclusive start byte.
    pub const fn start(self) -> u64 {
        self.start
    }

    /// Returns the inclusive end byte.
    pub const fn end(self) -> u64 {
        self.end
    }

    /// Returns the complete representation size used for resolution.
    pub const fn total(self) -> u64 {
        self.total
    }
}

impl From<ContentRange> for wire::ContentRange {
    fn from(value: ContentRange) -> Self {
        Self {
            start: value.start,
            end: value.end,
            total: value.total,
        }
    }
}

/// A validated bucket response bound to the request's logical bucket name.
///
/// Responses are constructed with [`Self::try_from_wire`] so the returned
/// value proves both the response timestamp and the request/response identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bucket {
    name: BucketName,
    created_at: ValidatedTimestamp,
}

impl Bucket {
    /// Validates a wire response against the bucket named by its request.
    pub fn try_from_wire(
        value: wire::Bucket,
        expected: &BucketName,
    ) -> Result<Self, Error> {
        let expected_ref = wire::BucketRef {
            name: expected.as_str().to_owned(),
        };
        response::bucket(&value, &expected_ref)?;
        let name = BucketName::try_from(value.bucket.ok_or(wire::ErrorCode::InvalidArgument)?)?;
        let created_at = ValidatedTimestamp::try_from(
            value
                .created_at
                .ok_or(wire::ErrorCode::InvalidArgument)?,
        )?;
        Ok(Self { name, created_at })
    }

    /// Returns the validated logical bucket name.
    pub fn name(&self) -> &BucketName {
        &self.name
    }

    /// Returns the validated creation timestamp.
    pub fn created_at(&self) -> &ValidatedTimestamp {
        &self.created_at
    }
}

impl From<Bucket> for wire::Bucket {
    fn from(value: Bucket) -> Self {
        Self {
            bucket: Some(wire::BucketRef {
                name: value.name.into_string(),
            }),
            created_at: Some(value.created_at.into_inner()),
        }
    }
}

/// A validated nonzero caller page size.
///
/// The wire contract accepts zero as shorthand for its default. The semantic
/// API keeps that sentinel out of the type; callers that want the wire default
/// should use [`Default::default`].
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PageSize(u32);

impl PageSize {
    /// Constructs a nonzero page size.
    pub fn new(value: u32) -> Result<Self, Error> {
        request::page_size(value)?;
        if value == 0 {
            return Err(wire::ErrorCode::InvalidArgument.into());
        }
        Ok(Self(value))
    }

    /// Returns the nonzero effective page size.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Default for PageSize {
    fn default() -> Self {
        Self(wire::ObjectsLimit::MaxPageEntries as u32)
    }
}

impl TryFrom<u32> for PageSize {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<PageSize> for u32 {
    fn from(value: PageSize) -> Self {
        value.get()
    }
}

/// A validated multipart part number in the canonical inclusive range.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PartNumber(u32);

impl PartNumber {
    /// The largest part number accepted by the Objects contract.
    pub const MAX: u32 = wire::ObjectsLimit::MaxMultipartParts as u32;

    /// Constructs a multipart part number in the canonical inclusive range.
    pub const fn new(value: u32) -> Result<Self, Error> {
        match request::part_number(value) {
            Ok(()) => Ok(Self(value)),
            Err(error) => Err(error),
        }
    }

    /// Returns the validated part number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for PartNumber {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<PartNumber> for u32 {
    fn from(value: PartNumber) -> Self {
        value.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrappers_reuse_canonical_validators() {
        assert!(BucketName::try_from("customer.inputs").is_ok());
        assert!(BucketName::try_from("bad..bucket").is_err());
        assert!(ObjectKey::try_from("artifact").is_ok());
        assert!(ObjectKey::try_from("").is_err());
        assert!(IdempotencyKey::try_from("retry-1").is_ok());
        assert!(UploadId::try_from("upload-1").is_ok());
        assert!(Etag::try_from("etag-1").is_ok());
        assert!(Etag::try_from("").is_err());
    }

    #[test]
    fn conversions_make_only_canonical_presence_variants() {
        let precondition = Precondition::IfMatch(Etag::try_from("etag-1").unwrap());
        let wire = wire::Preconditions::from(precondition.clone());
        assert!(matches!(
            wire.condition.as_ref(),
            Some(wire::preconditions::Condition::IfMatch(value)) if value.as_str() == "etag-1"
        ));
        assert_eq!(Precondition::try_from(wire), Ok(precondition));
        assert!(PageSize::try_from(0).is_err());
        assert_eq!(PageSize::default().get(), 1_000);
        assert!(PartNumber::try_from(0).is_err());
        assert_eq!(PartNumber::try_from(1).unwrap().get(), 1);
    }

    #[test]
    fn typed_delete_request_round_trips_validated_values() {
        let request = DeleteObjectRequest::new(
            BucketName::try_from("customer.inputs").unwrap(),
            ObjectKey::try_from("artifact").unwrap(),
        )
        .with_precondition(Precondition::IfAbsent)
        .with_idempotency_key(IdempotencyKey::try_from("delete-1").unwrap());

        let wire = wire::DeleteObjectRequest::from(request.clone());
        assert_eq!(DeleteObjectRequest::try_from(wire), Ok(request));

        let invalid = wire::DeleteObjectRequest {
            bucket: None,
            object_key: String::new(),
            preconditions: None,
            mutation: None,
        };
        assert!(DeleteObjectRequest::try_from(invalid).is_err());
    }

    #[test]
    fn typed_object_requests_round_trip_presence_and_ranges() {
        let bucket = BucketName::try_from("customer.inputs").unwrap();
        let object_key = ObjectKey::try_from("artifact").unwrap();
        let metadata = ObjectMetadata::try_from(wire::ObjectMetadata {
            content_type: "application/octet-stream".into(),
            user: [("x-owner".into(), "customer".into())].into(),
            ..Default::default()
        })
        .unwrap();
        let put = PutObjectHeader::new(bucket.clone(), object_key.clone())
            .with_metadata(metadata)
            .with_precondition(Precondition::IfMatch(Etag::try_from("etag-1").unwrap()))
            .with_idempotency_key(IdempotencyKey::try_from("put-1").unwrap());
        let put_wire = wire::PutObjectHeader::from(put.clone());
        assert_eq!(PutObjectHeader::try_from(put_wire), Ok(put));

        let get = GetObjectRequest::new(bucket.clone(), object_key.clone())
            .with_range(ByteSelection::bytes(2, None).unwrap())
            .with_if_match(Etag::try_from("etag-1").unwrap())
            .with_if_none_match(Etag::try_from("etag-2").unwrap());
        let get_wire = wire::GetObjectRequest::from(get.clone());
        assert_eq!(GetObjectRequest::try_from(get_wire), Ok(get));

        let head = HeadObjectRequest::new(bucket, object_key)
            .with_if_none_match(Etag::try_from("etag-2").unwrap());
        let head_wire = wire::HeadObjectRequest::from(head.clone());
        assert_eq!(HeadObjectRequest::try_from(head_wire), Ok(head));
    }

    #[test]
    fn typed_object_requests_reject_invalid_wire_values() {
        assert!(PutObjectHeader::try_from(wire::PutObjectHeader {
            bucket: None,
            object_key: "artifact".into(),
            metadata: None,
            preconditions: None,
            mutation: None,
        })
        .is_err());
        assert!(GetObjectRequest::try_from(wire::GetObjectRequest {
            bucket: Some(wire::BucketRef {
                name: "customer.inputs".into(),
            }),
            object_key: "artifact".into(),
            range: Some(wire::ByteRange { selection: None }),
            if_match: String::new(),
            if_none_match: String::new(),
        })
        .is_err());
        assert!(HeadObjectRequest::try_from(wire::HeadObjectRequest {
            bucket: Some(wire::BucketRef {
                name: "customer.inputs".into(),
            }),
            object_key: "artifact".into(),
            if_match: "etag\0bad".into(),
            if_none_match: String::new(),
        })
        .is_err());
    }

    #[test]
    fn typed_multipart_requests_and_response_round_trip_presence() {
        let bucket = BucketName::try_from("customer.inputs").unwrap();
        let object_key = ObjectKey::try_from("artifact").unwrap();
        let upload_id = UploadId::try_from("upload-1").unwrap();
        let retry = IdempotencyKey::try_from("multipart-1").unwrap();
        let metadata = ObjectMetadata::try_from(wire::ObjectMetadata {
            content_type: "application/octet-stream".into(),
            ..Default::default()
        })
        .unwrap();

        let create = CreateMultipartRequest::new(bucket.clone(), object_key.clone())
            .with_metadata(metadata)
            .with_idempotency_key(retry.clone());
        let create_wire = wire::CreateMultipartRequest::from(create.clone());
        assert_eq!(CreateMultipartRequest::try_from(create_wire), Ok(create));

        let header = UploadPartHeader::new(
            bucket.clone(),
            object_key.clone(),
            upload_id.clone(),
            PartNumber::try_from(1).unwrap(),
        )
        .with_idempotency_key(retry.clone());
        let header_wire = wire::UploadPartHeader::from(header.clone());
        assert_eq!(UploadPartHeader::try_from(header_wire), Ok(header));

        let abort = AbortMultipartRequest::new(bucket, object_key, upload_id.clone())
            .with_idempotency_key(retry);
        let abort_wire = wire::AbortMultipartRequest::from(abort.clone());
        assert_eq!(AbortMultipartRequest::try_from(abort_wire), Ok(abort));

        let upload = MultipartUpload::new(upload_id);
        let upload_wire = wire::MultipartUpload::from(upload.clone());
        assert_eq!(MultipartUpload::try_from(upload_wire), Ok(upload));
    }

    #[test]
    fn typed_multipart_requests_reject_invalid_wire_values() {
        assert!(CreateMultipartRequest::try_from(wire::CreateMultipartRequest {
            bucket: None,
            object_key: "artifact".into(),
            metadata: None,
            mutation: None,
        })
        .is_err());
        assert!(UploadPartHeader::try_from(wire::UploadPartHeader {
            bucket: Some(wire::BucketRef {
                name: "customer.inputs".into(),
            }),
            object_key: "artifact".into(),
            upload_id: "upload-1".into(),
            part_number: 0,
            mutation: None,
        })
        .is_err());
        assert!(AbortMultipartRequest::try_from(wire::AbortMultipartRequest {
            bucket: Some(wire::BucketRef {
                name: "customer.inputs".into(),
            }),
            object_key: "artifact".into(),
            upload_id: String::new(),
            mutation: None,
        })
        .is_err());
        assert!(MultipartUpload::try_from(wire::MultipartUpload {
            upload_id: String::new(),
        })
        .is_err());
    }

    #[test]
    fn typed_listing_requests_preserve_wire_defaults_and_opaque_tokens() {
        let bucket = BucketName::try_from("customer.inputs").unwrap();
        let defaults = ListObjectsRequest::new(bucket.clone());
        assert_eq!(defaults.prefix(), None);
        assert_eq!(defaults.delimiter(), None);
        assert_eq!(defaults.page_size(), None);
        assert_eq!(defaults.continuation_token(), None);
        assert_eq!(
            wire::ListObjectsRequest::from(defaults),
            wire::ListObjectsRequest {
                bucket: Some(wire::BucketRef {
                    name: "customer.inputs".into(),
                }),
                ..Default::default()
            }
        );

        let request = ListObjectsRequest::new(bucket)
            .with_prefix("customer/")
            .unwrap()
            .with_delimiter("/")
            .unwrap()
            .with_page_size(PageSize::try_from(7).unwrap())
            .with_continuation_token("opaque-token")
            .unwrap();
        let wire = wire::ListObjectsRequest::from(request.clone());
        assert_eq!(wire.page_size, 7);
        assert_eq!(wire.continuation_token, "opaque-token");
        assert_eq!(ListObjectsRequest::try_from(wire), Ok(request));

        assert!(ListObjectsRequest::new(BucketName::try_from("customer.inputs").unwrap())
            .with_prefix("bad\0prefix")
            .is_err());
        assert!(ListObjectsRequest::new(BucketName::try_from("customer.inputs").unwrap())
            .with_continuation_token("x".repeat(8193))
            .is_err());
        assert!(ListObjectsRequest::try_from(wire::ListObjectsRequest {
            bucket: Some(wire::BucketRef {
                name: "customer.inputs".into(),
            }),
            page_size: 1001,
            ..Default::default()
        })
        .is_err());
    }

    #[test]
    fn typed_parts_listing_preserves_zero_cursor_and_page_defaults() {
        let bucket = BucketName::try_from("customer.inputs").unwrap();
        let defaults = ListPartsRequest::new(
            bucket.clone(),
            ObjectKey::try_from("artifact").unwrap(),
            UploadId::try_from("upload-1").unwrap(),
        );
        assert_eq!(defaults.after_part_number(), None);
        assert_eq!(defaults.page_size(), None);
        let defaults_wire = wire::ListPartsRequest::from(defaults.clone());
        assert_eq!(defaults_wire.after_part_number, 0);
        assert_eq!(defaults_wire.page_size, 0);
        assert_eq!(ListPartsRequest::try_from(defaults_wire), Ok(defaults.clone()));

        let request = defaults
            .with_after_part_number(PartNumber::try_from(3).unwrap())
            .with_page_size(PageSize::try_from(2).unwrap());
        let wire = wire::ListPartsRequest::from(request.clone());
        assert_eq!(wire.after_part_number, 3);
        assert_eq!(wire.page_size, 2);
        assert_eq!(ListPartsRequest::try_from(wire), Ok(request));

        assert!(ListPartsRequest::try_from(wire::ListPartsRequest {
            bucket: Some(wire::BucketRef {
                name: "customer.inputs".into(),
            }),
            object_key: "artifact".into(),
            upload_id: "upload-1".into(),
            after_part_number: PartNumber::MAX + 1,
            ..Default::default()
        })
        .is_err());
    }

    #[test]
    fn typed_completion_reuses_uploaded_part_order_and_presence_rules() {
        let bucket = BucketName::try_from("customer.inputs").unwrap();
        let object_key = ObjectKey::try_from("artifact").unwrap();
        let upload_id = UploadId::try_from("upload-1").unwrap();
        let part_one = CompletedPart::new(
            PartNumber::try_from(1).unwrap(),
            Etag::try_from("etag-1").unwrap(),
            5,
        )
        .unwrap();
        let part_two = CompletedPart::new(
            PartNumber::try_from(2).unwrap(),
            Etag::try_from("etag-2").unwrap(),
            8,
        )
        .unwrap();
        let request = CompleteMultipartRequest::new(
            bucket,
            object_key,
            upload_id,
            vec![part_one, part_two],
        )
        .unwrap()
        .with_precondition(Precondition::IfAbsent)
        .with_idempotency_key(IdempotencyKey::try_from("complete-1").unwrap());
        let wire = wire::CompleteMultipartRequest::from(request.clone());
        assert_eq!(wire.parts.len(), 2);
        assert!(wire.preconditions.is_some());
        assert!(wire.mutation.is_some());
        assert_eq!(CompleteMultipartRequest::try_from(wire), Ok(request));

        let duplicate = CompletedPart::new(
            PartNumber::try_from(1).unwrap(),
            Etag::try_from("etag-3").unwrap(),
            9,
        )
        .unwrap();
        assert!(CompleteMultipartRequest::new(
            BucketName::try_from("customer.inputs").unwrap(),
            ObjectKey::try_from("artifact").unwrap(),
            UploadId::try_from("upload-1").unwrap(),
            vec![duplicate.clone(), duplicate],
        )
        .is_err());
        assert!(CompletedPart::try_from(wire::UploadedPart {
            part_number: 1,
            etag: String::new(),
            size: 1,
        })
        .is_err());
    }

    #[test]
    fn typed_bucket_requests_round_trip_validated_values() {
        let name = BucketName::try_from("customer.inputs").unwrap();
        let retry = IdempotencyKey::try_from("bucket-1").unwrap();

        let create = CreateBucketRequest::new(name.clone()).with_idempotency_key(retry.clone());
        let create_wire = wire::CreateBucketRequest::from(create.clone());
        assert_eq!(CreateBucketRequest::try_from(create_wire), Ok(create));

        let head = HeadBucketRequest::new(name.clone());
        let head_wire = wire::HeadBucketRequest::from(head.clone());
        assert_eq!(HeadBucketRequest::try_from(head_wire), Ok(head));

        let delete = DeleteBucketRequest::new(name).with_idempotency_key(retry);
        let delete_wire = wire::DeleteBucketRequest::from(delete.clone());
        assert_eq!(DeleteBucketRequest::try_from(delete_wire), Ok(delete));
    }

    #[test]
    fn typed_bucket_requests_reject_invalid_wire_values() {
        assert!(CreateBucketRequest::try_from(wire::CreateBucketRequest {
            name: "UPPERCASE".into(),
            mutation: None,
        })
        .is_err());
        assert!(HeadBucketRequest::try_from(wire::HeadBucketRequest { bucket: None }).is_err());
        assert!(DeleteBucketRequest::try_from(wire::DeleteBucketRequest {
            bucket: Some(wire::BucketRef { name: "bad..name".into() }),
            mutation: None,
        })
        .is_err());
    }

    #[test]
    fn typed_bucket_response_requires_request_identity_and_valid_timestamp() {
        let expected = BucketName::try_from("customer.inputs").unwrap();
        let timestamp = prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 123,
        };
        let wire = wire::Bucket {
            bucket: Some(wire::BucketRef {
                name: expected.as_str().into(),
            }),
            created_at: Some(timestamp.clone()),
        };

        let bucket = Bucket::try_from_wire(wire.clone(), &expected).unwrap();
        assert_eq!(bucket.name(), &expected);
        assert_eq!(bucket.created_at().seconds(), timestamp.seconds);
        assert_eq!(bucket.created_at().nanos(), timestamp.nanos);
        assert_eq!(wire::Bucket::from(bucket), wire);

        let other = BucketName::try_from("customer.outputs").unwrap();
        assert!(Bucket::try_from_wire(wire.clone(), &other).is_err());

        let missing_timestamp = wire::Bucket {
            bucket: wire.bucket,
            created_at: None,
        };
        assert!(Bucket::try_from_wire(missing_timestamp, &expected).is_err());
    }

    #[test]
    fn typed_object_info_round_trips_validated_metadata() {
        let metadata = wire::ObjectMetadata {
            content_type: "application/octet-stream".into(),
            user: [("x-owner".into(), "customer".into())].into(),
            expires_unix_seconds: Some(1_700_000_000),
            ..Default::default()
        };
        let wire = wire::ObjectInfo {
            etag: "etag-1".into(),
            size: 42,
            metadata: Some(metadata),
            last_modified: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 7,
            }),
        };

        let info = ObjectInfo::try_from(wire.clone()).unwrap();
        assert_eq!(info.etag().as_str(), "etag-1");
        assert_eq!(info.size(), 42);
        assert_eq!(info.metadata().map(ObjectMetadata::content_type), Some("application/octet-stream"));
        assert_eq!(info.metadata().map(ObjectMetadata::expires_unix_seconds), Some(Some(1_700_000_000)));
        assert_eq!(info.last_modified().seconds(), 1_700_000_000);
        assert_eq!(wire::ObjectInfo::from(info), wire);

        let invalid_metadata = wire::ObjectMetadata {
            content_type: "text\nplain".into(),
            ..Default::default()
        };
        assert!(ObjectMetadata::try_from(invalid_metadata).is_err());
    }

    #[test]
    fn typed_object_info_rejects_invalid_response_fields() {
        let invalid_etag = wire::ObjectInfo {
            etag: String::new(),
            last_modified: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..Default::default()
        };
        assert!(ObjectInfo::try_from(invalid_etag).is_err());

        let duplicate_metadata = wire::ObjectMetadata {
            user: [
                ("X-Owner".into(), "one".into()),
                ("x-owner".into(), "two".into()),
            ]
            .into(),
            ..Default::default()
        };
        let invalid_metadata = wire::ObjectInfo {
            etag: "etag-1".into(),
            metadata: Some(duplicate_metadata),
            last_modified: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..Default::default()
        };
        assert!(ObjectInfo::try_from(invalid_metadata).is_err());
    }

    #[test]
    fn typed_ranges_preserve_presence_and_use_canonical_resolution() {
        let open = ByteSelection::bytes(2, None).unwrap();
        assert_eq!(open.start(), Some(2));
        assert_eq!(open.end(), None);
        assert_eq!(open.suffix_length(), None);
        assert_eq!(
            open.resolve(5).unwrap(),
            ContentRange {
                start: 2,
                end: 4,
                total: 5,
            }
        );

        let suffix = ByteSelection::suffix(u64::MAX).unwrap();
        assert_eq!(suffix.start(), None);
        assert_eq!(suffix.end(), None);
        assert_eq!(suffix.suffix_length(), NonZeroU64::new(u64::MAX));
        assert_eq!(suffix.resolve(4).unwrap().start(), 0);
        assert_eq!(suffix.resolve(4).unwrap().end(), 3);
        assert!(ByteSelection::suffix(0).is_err());
        assert!(ByteSelection::bytes(4, Some(3)).is_err());
        assert!(open.resolve(0).is_err());

        let wire = wire::ByteRange::from(open);
        assert_eq!(ByteSelection::try_from(wire).unwrap(), open);
    }

    #[test]
    fn typed_content_range_rejects_noncanonical_wire_values() {
        let selection = ByteSelection::bytes(1, Some(3)).unwrap();
        let expected = selection.resolve(8).unwrap();
        let wire = wire::ContentRange::from(expected);
        assert_eq!(
            ContentRange::try_from_wire(wire, selection, 8).unwrap(),
            expected
        );

        let mismatched = wire::ContentRange {
            end: 4,
            ..wire
        };
        assert!(ContentRange::try_from_wire(mismatched, selection, 8).is_err());
    }

    #[test]
    fn validated_timestamp_rejects_noncanonical_values() {
        assert!(ValidatedTimestamp::try_from(prost_types::Timestamp {
            seconds: -62_135_596_801,
            nanos: 0,
        })
        .is_err());
        assert!(ValidatedTimestamp::try_from(prost_types::Timestamp {
            seconds: 0,
            nanos: -1,
        })
        .is_err());
        assert!(ValidatedTimestamp::try_from(prost_types::Timestamp {
            seconds: 0,
            nanos: 1_000_000_000,
        })
        .is_err());
    }
}
