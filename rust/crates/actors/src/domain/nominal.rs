//! Rust-owned nominal semantic values shared by every generated binding.
//!
//! These wrappers deliberately keep the published wire representation intact.
//! Their constructors and `TryFrom` implementations are the only semantic
//! admission authority; language generators consume this metadata and call the
//! exported Rust constructors instead of copying predicates.

use std::num::NonZeroU64;

use super::DomainError;

/// A non-empty Actor identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq, ts_rs::TS)]
#[ts(export_to = "actors/ActorId.ts")]
#[ts(type = "string & { readonly __brand: unique symbol }")]
pub struct ActorId(String);

#[cfg(feature = "uniffi")]
uniffi::custom_type!(ActorId, String, {
    lower: |value| value.0,
    try_lift: |value| Ok(ActorId::try_from(value)?),
});

impl ActorId {
    /// Constructs an Actor identity using the canonical contract rule.
    pub fn new(value: String) -> Result<Self, DomainError> {
        if value.is_empty() {
            return Err(DomainError::EmptyActorId);
        }
        Ok(Self(value))
    }

    /// Returns the wire spelling without changing it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ActorId {
    type Error = DomainError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<&ActorId> for String {
    fn from(value: &ActorId) -> Self {
        value.0.clone()
    }
}

impl From<ActorId> for String {
    fn from(value: ActorId) -> Self {
        value.0
    }
}

/// An exact, non-zero SHA-256 digest as used by the existing Actor validators.
#[derive(Clone, Debug, Eq, Hash, PartialEq, ts_rs::TS)]
#[ts(export_to = "actors/CodeSha256.ts")]
#[ts(type = "Uint8Array & { readonly __brand: unique symbol; readonly __length: 32 }")]
pub struct CodeSha256([u8; 32]);

#[cfg(feature = "uniffi")]
uniffi::custom_type!(CodeSha256, Vec<u8>, {
    lower: |value| value.0.to_vec(),
    try_lift: |value| Ok(CodeSha256::new(value)?),
});

impl CodeSha256 {
    /// Constructs a digest with the exact predicate used by the canonical
    /// Actor validator.
    pub fn new(value: Vec<u8>) -> Result<Self, DomainError> {
        if !valid_code_sha256(&value) {
            return Err(DomainError::InvalidCodeSha256);
        }
        let value: [u8; 32] = value
            .try_into()
            .map_err(|_| DomainError::InvalidCodeSha256)?;
        Ok(Self(value))
    }

    /// Returns the exact wire bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl TryFrom<Vec<u8>> for CodeSha256 {
    type Error = DomainError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<&CodeSha256> for Vec<u8> {
    fn from(value: &CodeSha256) -> Self {
        value.0.to_vec()
    }
}

impl TryFrom<protify::Bytes> for CodeSha256 {
    type Error = DomainError;

    fn try_from(value: protify::Bytes) -> Result<Self, Self::Error> {
        Self::new(value.to_vec())
    }
}

impl From<CodeSha256> for protify::Bytes {
    fn from(value: CodeSha256) -> Self {
        value.0.to_vec().into()
    }
}

/// A strictly positive unsigned 64-bit value.
///
/// The wire contract remains a raw `u64`; this nominal type is used only at
/// semantic boundaries where zero is not admitted.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ts_rs::TS)]
#[ts(export_to = "actors/PositiveU64.ts")]
#[ts(type = "bigint & { readonly __brand: unique symbol }")]
pub struct PositiveU64(NonZeroU64);

#[cfg(feature = "uniffi")]
uniffi::custom_type!(PositiveU64, u64, {
    lower: |value| value.get(),
    try_lift: |value| Ok(PositiveU64::new(value)?),
});

impl PositiveU64 {
    /// Constructs a positive value using the canonical semantic predicate.
    pub fn new(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(DomainError::Contract(crate::ContractError::InvalidArgument))
    }

    /// Returns the exact unsigned wire value.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for PositiveU64 {
    type Error = DomainError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<PositiveU64> for u64 {
    fn from(value: PositiveU64) -> Self {
        value.get()
    }
}

/// The only valid semantic payload for the current-head selector. The
/// protobuf wire representation remains a bool for descriptor stability,
/// while this type makes `false` unrepresentable after ingress validation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ts_rs::TS)]
#[ts(type = "true", export_to = "actors/CurrentHeadMarker.ts")]
pub struct CurrentHeadMarker;

impl CurrentHeadMarker {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl TryFrom<bool> for CurrentHeadMarker {
    type Error = DomainError;

    fn try_from(value: bool) -> Result<Self, Self::Error> {
        value
            .then_some(Self)
            .ok_or(DomainError::InvalidSubscription)
    }
}

impl From<CurrentHeadMarker> for bool {
    fn from(_: CurrentHeadMarker) -> Self {
        true
    }
}

#[cfg(feature = "uniffi")]
uniffi::custom_type!(CurrentHeadMarker, bool, {
    lower: |value| bool::from(value),
    try_lift: |value| Ok(CurrentHeadMarker::try_from(value)?),
});

/// The shared digest predicate used by the existing Actor validators.
pub(crate) fn valid_code_sha256(value: &[u8]) -> bool {
    value.len() == 32 && value.iter().any(|byte| *byte != 0)
}
