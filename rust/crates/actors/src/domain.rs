//! Rust-owned semantic projections for the Actors contract.
//!
//! This module is intentionally small while the generator integration is being
//! wired by the owning generation task. It gives the first complete vertical
//! operation (`InvokeActor`) a typed Rust boundary and derives its static
//! TypeScript declaration from the same Rust definitions. Transport behavior
//! remains in [`crate::grpc`] and [`crate::http`].

use crate::wire;
use ts_rs::TS;

/// A non-empty Actor identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq, TS)]
#[ts(type = "string & { readonly __brand: unique symbol }")]
pub struct ActorId(String);

/// An exact, non-zero SHA-256 digest as used by the existing Actor validators.
#[derive(Clone, Debug, Eq, Hash, PartialEq, TS)]
#[ts(type = "Uint8Array & { readonly __brand: unique symbol; readonly __length: 32 }")]
pub struct CodeSha256([u8; 32]);

/// Failure while constructing a semantic value from customer or wire input.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DomainError {
    /// Actor validators require an actor identity to be present.
    #[error("actor_id must not be empty")]
    EmptyActorId,
    /// Actor validators require exactly 32 bytes with at least one non-zero byte.
    #[error("code_sha256 must contain 32 bytes and must not be all zero")]
    InvalidCodeSha256,
}

impl ActorId {
    /// Constructs an Actor identity using the existing `is_empty` contract rule.
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

impl CodeSha256 {
    /// Constructs a digest with the exact predicate used by `crate::validate_create`.
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

/// The shared digest predicate used by the existing Actor validators.
pub(crate) fn valid_code_sha256(value: &[u8]) -> bool {
    value.len() == 32 && value.iter().any(|byte| *byte != 0)
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

/// The canonical wire header. The wire owner will attach `ts-rs` metadata to
/// this type; keeping an alias here avoids a second Rust header model.
pub type Header = wire::Header;

/// Typed invocation request. Method, URL, headers, and body preserve the
/// existing wire contract without adding new validation rules.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct InvokeActorRequest {
    pub actor_id: ActorId,
    pub method: String,
    pub url: String,
    #[ts(type = "Uint8Array")]
    pub body: Vec<u8>,
    #[ts(type = "Array<{ name: string; value: string }>")]
    pub headers: Vec<Header>,
}

impl TryFrom<wire::InvokeActorRequest> for InvokeActorRequest {
    type Error = DomainError;

    fn try_from(value: wire::InvokeActorRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            actor_id: value.actor_id.try_into()?,
            method: value.method,
            url: value.url,
            body: value.body.to_vec(),
            headers: value.headers,
        })
    }
}

impl From<InvokeActorRequest> for wire::InvokeActorRequest {
    fn from(value: InvokeActorRequest) -> Self {
        Self {
            actor_id: value.actor_id.0,
            method: value.method,
            url: value.url,
            body: value.body.into(),
            headers: value.headers,
        }
    }
}

/// Typed invocation response with byte-preserving body and headers.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct InvokeActorResponse {
    pub status: u32,
    #[ts(type = "Uint8Array")]
    pub body: Vec<u8>,
    #[ts(type = "Array<{ name: string; value: string }>")]
    pub headers: Vec<Header>,
}

impl From<wire::InvokeActorResponse> for InvokeActorResponse {
    fn from(value: wire::InvokeActorResponse) -> Self {
        Self {
            status: value.status,
            body: value.body.to_vec(),
            headers: value.headers,
        }
    }
}

impl From<InvokeActorResponse> for wire::InvokeActorResponse {
    fn from(value: InvokeActorResponse) -> Self {
        Self {
            status: value.status,
            body: value.body.into(),
            headers: value.headers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_id_matches_existing_presence_rule() {
        assert_eq!(ActorId::new(String::new()), Err(DomainError::EmptyActorId));
        assert_eq!(
            ActorId::new("  ".into()).map(|value| value.as_str().into()),
            Ok("  ")
        );
    }

    #[test]
    fn digest_matches_existing_create_predicate() {
        assert_eq!(
            CodeSha256::new(vec![0; 31]),
            Err(DomainError::InvalidCodeSha256)
        );
        assert_eq!(
            CodeSha256::new(vec![0; 32]),
            Err(DomainError::InvalidCodeSha256)
        );
        assert!(CodeSha256::new(vec![1; 32]).is_ok());
    }

    #[test]
    fn invoke_round_trips_without_rewriting_wire_fields() {
        let wire = wire::InvokeActorRequest {
            actor_id: "actor-1".into(),
            method: String::new(),
            url: String::new(),
            body: vec![0, 1, 2],
            headers: vec![wire::Header {
                name: "x-test".into(),
                value: "ok".into(),
            }],
        };
        let typed = InvokeActorRequest::try_from(wire.clone()).ok();
        assert!(typed.is_some());
        let Some(typed) = typed else {
            return;
        };
        let round_trip: wire::InvokeActorRequest = typed.into();
        assert_eq!(round_trip, wire);
    }

    #[test]
    fn invoke_rejects_only_the_existing_actor_id_presence_violation() {
        let wire = wire::InvokeActorRequest {
            actor_id: String::new(),
            method: String::new(),
            url: String::new(),
            body: Vec::new(),
            headers: Vec::new(),
        };
        assert_eq!(
            InvokeActorRequest::try_from(wire),
            Err(DomainError::EmptyActorId)
        );
    }
}
