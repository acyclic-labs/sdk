//! Small nominal values used at SDK boundaries.
//!
//! Protobuf messages and enums are generated from the Rust contract in
//! [`crate::wire`]. This module deliberately contains only boundary newtypes;
//! it does not repeat the message schema or operation fields.

use std::{num::NonZeroU64, path::Path};

use ts_rs::{Config, ExportError, TS};

/// A non-empty Actor identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq, TS)]
#[ts(export_to = "actors/ActorId.ts")]
#[ts(type = "string & { readonly __brand: unique symbol }")]
pub struct ActorId(String);

/// An exact, non-zero SHA-256 digest.
#[derive(Clone, Debug, Eq, Hash, PartialEq, TS)]
#[ts(export_to = "actors/CodeSha256.ts")]
#[ts(type = "Uint8Array & { readonly __brand: unique symbol; readonly __length: 32 }")]
pub struct CodeSha256([u8; 32]);

/// A strictly positive unsigned 64-bit value.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, TS)]
#[ts(export_to = "actors/PositiveU64.ts")]
#[ts(type = "bigint & { readonly __brand: unique symbol }")]
pub struct PositiveU64(NonZeroU64);

/// Failure while constructing a nominal boundary value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DomainError {
    /// Actor identities cannot be empty.
    #[error("actor_id must not be empty")]
    EmptyActorId,
    /// Digests must contain 32 bytes and at least one non-zero byte.
    #[error("code_sha256 must contain 32 bytes and must not be all zero")]
    InvalidCodeSha256,
    /// Positive values cannot be zero.
    #[error("value must be positive")]
    NotPositive,
}

impl ActorId {
    /// Construct an Actor identity.
    pub fn new(value: String) -> Result<Self, DomainError> {
        (!value.is_empty())
            .then_some(Self(value))
            .ok_or(DomainError::EmptyActorId)
    }

    /// Return the wire spelling.
    #[must_use]
    pub fn as_str(&self) -> &str { &self.0 }
}

impl CodeSha256 {
    /// Construct an exact digest.
    pub fn new(value: Vec<u8>) -> Result<Self, DomainError> {
        if !valid_code_sha256(&value) {
            return Err(DomainError::InvalidCodeSha256);
        }
        value
            .try_into()
            .map(Self)
            .map_err(|_| DomainError::InvalidCodeSha256)
    }

    /// Return the digest bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] { &self.0 }
}

impl PositiveU64 {
    /// Construct a positive value.
    pub fn new(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value).map(Self).ok_or(DomainError::NotPositive)
    }

    /// Return the exact integer.
    #[must_use]
    pub fn get(self) -> u64 { self.0.get() }
}

/// Shared digest predicate used by the canonical request validators.
pub(crate) fn valid_code_sha256(value: &[u8]) -> bool {
    value.len() == 32 && value.iter().any(|byte| *byte != 0)
}

/// Export the nominal boundary declarations for generated TypeScript types.
pub fn export_typescript(out_dir: impl AsRef<Path>) -> Result<(), ExportError> {
    let config = Config::default().with_out_dir(out_dir.as_ref().to_path_buf());
    ActorId::export_all(&config)?;
    CodeSha256::export_all(&config)?;
    PositiveU64::export_all(&config)
}
