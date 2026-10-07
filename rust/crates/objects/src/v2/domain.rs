//! Validated semantic values for the Objects v2 wire contract.
//!
//! The generated protobuf messages remain the transport representation. These
//! wrappers make the values that already have canonical validators explicit at
//! Rust API boundaries without copying any of the validation rules.

use std::fmt;

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
