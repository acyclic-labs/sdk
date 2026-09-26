//! The current-value Objects contract.
//!
//! This module is the public contract for the hosted Objects service. An
//! [`ObjectId`] names one stable object in one bucket. A successful
//! [`ReplacePutRequest`] replaces that object's current bytes; it does not
//! create a customer-visible version. Providers may retain immutable write
//! generations internally for recovery and replication, but those generations
//! deliberately do not cross this API boundary.
//!
//! Reads are allowed to be eventually visible after a successful replacement.
//! Callers that need to observe a replacement should use [`get_current`] with
//! the returned ETag as `if_none_match` and retry until it is visible. A
//! transport failure during a replacement is ambiguous: retry the exact same
//! idempotency key rather than issuing a new mutation.

use async_trait::async_trait;
use bytes::Bytes;
use std::hash::{Hash, Hasher};

use crate::{Condition, GetRequest, ObjectsError, ObjectsProvider, PutRequest, ReadTarget, wire};

/// Maximum bytes in an account-scoped replacement idempotency key.
pub const MAX_REPLACEMENT_IDEMPOTENCY_KEY_BYTES: usize = 256;

/// A stable customer-visible object identity.
///
/// The bucket reference and object key together are the identity. The key is
/// opaque to the service and remains stable when the value is replaced.
#[derive(Clone, Debug)]
pub struct ObjectId {
    bucket: wire::BucketRef,
    key: String,
}

impl PartialEq for ObjectId {
    fn eq(&self, other: &Self) -> bool {
        self.bucket.bucket_id == other.bucket.bucket_id
            && self.bucket.name == other.bucket.name
            && self.key == other.key
    }
}

impl Eq for ObjectId {}

impl Hash for ObjectId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bucket.bucket_id.hash(state);
        self.bucket.name.hash(state);
        self.key.hash(state);
    }
}

impl ObjectId {
    /// Validates and creates one stable object identity.
    pub fn new(bucket: wire::BucketRef, key: impl Into<String>) -> Result<Self, ObjectsError> {
        let key = key.into();
        if bucket.bucket_id.is_empty()
            || key.is_empty()
            || key.len() > crate::limits::KEY_BYTES
            || key.contains('\0')
        {
            return Err(ObjectsError::Invalid("invalid object identity"));
        }
        Ok(Self { bucket, key })
    }

    /// Exact bucket identity selected by this object.
    #[must_use]
    pub fn bucket(&self) -> &wire::BucketRef {
        &self.bucket
    }

    /// Opaque object key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Copies this identity for durable application state.
    #[must_use]
    pub fn cloned_bucket(&self) -> wire::BucketRef {
        self.bucket.clone()
    }
}

/// The only current-value mutation conditions exposed by the replacement API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplaceCondition {
    /// Require that no current value exists.
    IfAbsent,
    /// Require the current representation to have this `ETag`.
    IfMatch(String),
}

impl ReplaceCondition {
    fn provider_condition(&self) -> Condition {
        match self {
            Self::IfAbsent => Condition::IfAbsent,
            Self::IfMatch(etag) => Condition::IfMatch(etag.clone()),
        }
    }
}

/// One replacement write request.
#[derive(Clone, Debug)]
pub struct ReplacePutRequest {
    /// Stable object identity being replaced.
    pub object: ObjectId,
    /// Complete replacement bytes.
    pub body: Bytes,
    /// Metadata associated with the new current value.
    pub metadata: wire::ObjectMetadata,
    /// Optional current-value condition.
    pub condition: Option<ReplaceCondition>,
    /// Stable retry identity. Reuse it after an ambiguous transport failure.
    pub idempotency_key: String,
}

impl ReplacePutRequest {
    /// Validates the request before handing it to a provider.
    pub fn validate(&self) -> Result<(), ObjectsError> {
        if self.idempotency_key.is_empty()
            || self.idempotency_key.len() > MAX_REPLACEMENT_IDEMPOTENCY_KEY_BYTES
        {
            return Err(ObjectsError::Invalid("invalid replacement idempotency key"));
        }
        if self
            .idempotency_key
            .bytes()
            .any(|byte| byte.is_ascii_control())
        {
            return Err(ObjectsError::Invalid("invalid replacement idempotency key"));
        }
        Ok(())
    }
}

/// Metadata returned after a replacement is durably accepted by a provider.
///
/// The hosted provider only returns this after its configured durability
/// acknowledgement. Site names, physical generations, and receipt signatures
/// remain server and operator concerns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutReceipt {
    /// Stable object identity that was replaced.
    pub object: ObjectId,
    /// `ETag` for the bytes accepted by the provider.
    pub etag: String,
    /// Number of accepted bytes.
    pub size: u64,
}

/// Current object read request.
#[derive(Clone, Debug)]
pub struct CurrentGetRequest {
    /// Stable object identity to read.
    pub object: ObjectId,
    /// Optional byte range `(start, inclusive end)`.
    pub range: Option<(u64, Option<u64>)>,
    /// Require this current `ETag`.
    pub if_match: Option<String>,
    /// Reject this current `ETag`. This is useful for waiting for visibility of
    /// a replacement without exposing a physical generation identifier.
    pub if_none_match: Option<String>,
    /// Maximum body bytes returned by the provider.
    pub maximum_bytes: u64,
}

impl CurrentGetRequest {
    /// Creates a current-value request for one stable object identity.
    #[must_use]
    pub fn new(object: ObjectId) -> Self {
        Self {
            object,
            range: None,
            if_match: None,
            if_none_match: None,
            maximum_bytes: crate::limits::OBJECT_BYTES,
        }
    }

    /// Adds an inclusive byte range to the read.
    #[must_use]
    pub fn range(mut self, start: u64, end_inclusive: Option<u64>) -> Self {
        self.range = Some((start, end_inclusive));
        self
    }

    /// Requires a specific current `ETag`.
    #[must_use]
    pub fn if_match(mut self, etag: impl Into<String>) -> Self {
        self.if_match = Some(etag.into());
        self
    }

    /// Rejects a specific current `ETag`, useful for eventual-visibility polling.
    #[must_use]
    pub fn if_none_match(mut self, etag: impl Into<String>) -> Self {
        self.if_none_match = Some(etag.into());
        self
    }

    /// Limits the returned body before provider allocation.
    #[must_use]
    pub fn maximum_bytes(mut self, maximum_bytes: u64) -> Self {
        self.maximum_bytes = maximum_bytes;
        self
    }
}

/// One current-value result. No public version identity is included.
#[derive(Clone, Debug, PartialEq)]
pub struct CurrentObject {
    /// Stable object identity that was read.
    pub object: ObjectId,
    /// Current `ETag`.
    pub etag: String,
    /// Current complete-object size, before an optional range is applied.
    pub size: u64,
    /// Current metadata.
    pub metadata: wire::ObjectMetadata,
    /// Selected body bytes.
    pub body: Bytes,
}

/// Result of verifying that returned bytes match their server `ETag`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Verification {
    /// Stable object identity verified.
    pub object: ObjectId,
    /// `ETag` returned by the provider.
    pub etag: String,
    /// Number of bytes verified.
    pub size: u64,
    /// Whether the `ETag` matches the canonical BLAKE3 representation.
    pub valid: bool,
}

/// Current-value provider operations used by the hosted data plane.
#[async_trait]
pub trait ReplaceableObjectsProvider: Send + Sync {
    /// Atomically replace the current value at one stable object identity.
    async fn replace_put(&self, request: ReplacePutRequest) -> Result<PutReceipt, ObjectsError>;

    /// Read the provider's currently visible value.
    async fn get_current(&self, request: CurrentGetRequest) -> Result<CurrentObject, ObjectsError>;

    /// Verify the current value's `ETag` against its returned bytes.
    async fn verify_current(
        &self,
        request: CurrentGetRequest,
    ) -> Result<Verification, ObjectsError>;
}

#[async_trait]
impl<P: ObjectsProvider + ?Sized> ReplaceableObjectsProvider for P {
    async fn replace_put(&self, request: ReplacePutRequest) -> Result<PutReceipt, ObjectsError> {
        request.validate()?;
        let object = request.object.clone();
        let version = self
            .put(PutRequest {
                bucket: object.bucket.clone(),
                object_key: object.key.clone(),
                body: request.body,
                metadata: request.metadata,
                condition: request
                    .condition
                    .as_ref()
                    .map(ReplaceCondition::provider_condition),
                idempotency_key: Some(request.idempotency_key),
            })
            .await?;
        Ok(PutReceipt {
            object,
            etag: version.etag,
            size: version.size,
        })
    }

    async fn get_current(&self, request: CurrentGetRequest) -> Result<CurrentObject, ObjectsError> {
        let object = request.object.clone();
        let selected = self
            .get(GetRequest {
                target: ReadTarget::Bucket(object.bucket.clone()),
                object_key: object.key.clone(),
                version_id: None,
                range: request.range,
                if_match: request.if_match,
                if_none_match: request.if_none_match,
                maximum_bytes: request.maximum_bytes,
            })
            .await?;
        let metadata = selected.version.metadata.clone().unwrap_or_default();
        Ok(CurrentObject {
            object,
            etag: selected.version.etag,
            size: selected.version.size,
            metadata,
            body: selected.body,
        })
    }

    async fn verify_current(
        &self,
        request: CurrentGetRequest,
    ) -> Result<Verification, ObjectsError> {
        if request.range.is_some() {
            return Err(ObjectsError::Invalid(
                "verification requires a complete current object",
            ));
        }
        let current = self.get_current(request).await?;
        let expected = format!("\"{}\"", blake3::hash(&current.body).to_hex());
        Ok(Verification {
            object: current.object,
            etag: current.etag.clone(),
            size: current.body.len() as u64,
            valid: current.etag == expected,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryObjects, ObjectsProvider};

    fn bucket() -> wire::BucketRef {
        wire::BucketRef {
            bucket_id: "bucket-1".into(),
            name: "bucket-one".into(),
        }
    }

    #[tokio::test]
    async fn replacement_keeps_identity_and_hides_generation() {
        let provider =
            MemoryObjects::new(crate::limits::OBJECT_BYTES).unwrap_or_else(|_| unreachable!());
        let bucket = provider
            .create_bucket("bucket-one".into(), None)
            .await
            .expect("test bucket")
            .bucket
            .expect("bucket identity");
        let object = ObjectId::new(bucket, "config.json").expect("object identity");

        let first = provider
            .replace_put(ReplacePutRequest {
                object: object.clone(),
                body: Bytes::from_static(b"one"),
                metadata: wire::ObjectMetadata::default(),
                condition: Some(ReplaceCondition::IfAbsent),
                idempotency_key: "replace-1".into(),
            })
            .await
            .expect("first replacement");
        let second = provider
            .replace_put(ReplacePutRequest {
                object: object.clone(),
                body: Bytes::from_static(b"two"),
                metadata: wire::ObjectMetadata::default(),
                condition: Some(ReplaceCondition::IfMatch(first.etag.clone())),
                idempotency_key: "replace-2".into(),
            })
            .await
            .expect("second replacement");

        let replay = provider
            .replace_put(ReplacePutRequest {
                object: object.clone(),
                body: Bytes::from_static(b"two"),
                metadata: wire::ObjectMetadata::default(),
                condition: Some(ReplaceCondition::IfMatch(first.etag.clone())),
                idempotency_key: "replace-2".into(),
            })
            .await
            .expect("idempotent replacement replay");

        assert_eq!(first.object, second.object);
        assert_ne!(first.etag, second.etag);
        assert_eq!(second, replay);
        let current = provider
            .get_current(CurrentGetRequest::new(object.clone()))
            .await
            .expect("current object");
        assert_eq!(current.object, object);
        assert_eq!(current.body, Bytes::from_static(b"two"));
        assert!(
            provider
                .verify_current(CurrentGetRequest::new(object))
                .await
                .expect("verification")
                .valid
        );
    }

    #[test]
    fn replacement_requires_a_reusable_bounded_key() {
        let request = ReplacePutRequest {
            object: ObjectId::new(bucket(), "a").expect("object identity"),
            body: Bytes::new(),
            metadata: wire::ObjectMetadata::default(),
            condition: None,
            idempotency_key: String::new(),
        };
        assert_eq!(
            request.validate(),
            Err(ObjectsError::Invalid("invalid replacement idempotency key"))
        );
    }
}
