//! Validated semantic values for the Objects v2 wire contract.
//!
//! The generated protobuf messages remain the transport representation. These
//! wrappers make the values that already have canonical validators explicit at
//! Rust API boundaries without copying any of the validation rules.

use std::fmt;

use super::{Error, request, wire};

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

    /// Constructs a nonzero multipart part number.
    pub const fn new(value: u32) -> Result<Self, Error> {
        if value == 0 || value > Self::MAX {
            return Err(Error {
                code: wire::ErrorCode::InvalidArgument,
            });
        }
        Ok(Self(value))
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
            wire.condition,
            Some(wire::preconditions::Condition::IfMatch(value)) if value == "etag-1"
        ));
        assert_eq!(Precondition::try_from(wire), Ok(precondition));
        assert!(PageSize::try_from(0).is_err());
        assert_eq!(PageSize::default().get(), 1_000);
        assert!(PartNumber::try_from(0).is_err());
        assert_eq!(PartNumber::try_from(1).unwrap().get(), 1);
    }
}
