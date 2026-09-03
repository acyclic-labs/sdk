//! Canonical independently addressable generation-retention facts.

use super::codec::{CanonicalDecodeError, Decoder, Encoder};
use super::volume::{decode_config, encode_config};
use crate::foundation::{AuthorityId, Digest, VolumeId};
use crate::model::{VolumeConfig, VolumeConfigError};
use crate::storage::{ObjectId, ObjectKind};
use thiserror::Error;

const DOMAIN: &[u8] = b"acyclic-fs-retention-created-v1\0";
const ID_DOMAIN: &[u8] = b"acyclic-fs-retention-authority-v1\0";
const DELETED_DOMAIN: &[u8] = b"acyclic-fs-workspace-deleted-v1\0";
const VERSION: u16 = 1;
const MAXIMUM_LABEL_BYTES: u32 = 255;

/// Why one immutable generation remains durably reachable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionKind {
    /// Human-readable retained workspace checkpoint.
    Checkpoint,
    /// Explicit customer generation pin.
    Pin,
    /// Internal exact ancestry root retained by an independently live fork.
    ForkBase,
}

impl RetentionKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Checkpoint => 1,
            Self::Pin => 2,
            Self::ForkBase => 3,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, CanonicalDecodeError> {
        match tag {
            1 => Ok(Self::Checkpoint),
            2 => Ok(Self::Pin),
            3 => Ok(Self::ForkBase),
            _ => Err(invariant("unknown retention kind")),
        }
    }
}

/// Immutable creation fact for one retained generation root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionCreated {
    /// Workspace that owns the retained label or pin.
    pub volume_id: VolumeId,
    /// Retention class.
    pub kind: RetentionKind,
    /// Canonical checkpoint label or opaque pin identity.
    pub label: String,
    /// Exact retained generation root.
    pub generation_root: ObjectId,
    /// Filesystem semantics needed to authenticate and reclaim the closure.
    pub config: VolumeConfig,
}

/// Canonical retention codec failure.
#[derive(Debug, Error)]
pub enum RetentionCreatedError {
    /// Retention label is absent, oversized, or contains a forbidden scalar.
    #[error("retention label is invalid")]
    InvalidLabel,
    /// Retained object is not a generation root.
    #[error("retention target is not a generation root")]
    WrongObjectKind,
    /// Volume configuration is invalid.
    #[error(transparent)]
    Config(#[from] VolumeConfigError),
    /// Canonical payload is malformed or unsupported.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
}

/// Derives the one authority owning a workspace retention name.
#[must_use]
pub fn retention_authority_id(
    volume_id: VolumeId,
    kind: RetentionKind,
    label: &str,
) -> AuthorityId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ID_DOMAIN);
    hasher.update(&volume_id.into_bytes());
    hasher.update(&[kind.tag()]);
    hasher.update(&u64::try_from(label.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(label.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    AuthorityId::from_bytes(bytes)
}

/// Encodes one exact immutable retention fact.
///
/// # Errors
///
/// Rejects invalid labels, configurations, object kinds, or allocation bounds.
pub fn encode_retention_created(
    value: &RetentionCreated,
) -> Result<Vec<u8>, RetentionCreatedError> {
    validate(value)?;
    let mut encoder = Encoder::new(DOMAIN, VERSION);
    encoder.fixed(&value.volume_id.into_bytes());
    encoder.u8(value.kind.tag());
    encoder.bounded_bytes(value.label.as_bytes())?;
    encoder.u8(value.generation_root.kind.canonical_tag());
    encoder.fixed(value.generation_root.digest.as_bytes());
    encode_config(&mut encoder, value.config);
    Ok(encoder.finish())
}

/// Decodes one exact bounded immutable retention fact.
///
/// # Errors
///
/// Rejects malformed, oversized, trailing, unsupported, or semantically invalid payloads.
pub fn decode_retention_created(
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<RetentionCreated, RetentionCreatedError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, maximum_payload_bytes)?;
    let volume_id = VolumeId::from_bytes(decoder.fixed()?);
    let kind = RetentionKind::from_tag(decoder.u8()?)?;
    let label = String::from_utf8(decoder.bounded_bytes(MAXIMUM_LABEL_BYTES)?)
        .map_err(|_| RetentionCreatedError::InvalidLabel)?;
    let object_kind = ObjectKind::from_canonical_tag(decoder.u8()?)
        .map_err(|_| RetentionCreatedError::WrongObjectKind)?;
    let generation_root = ObjectId {
        kind: object_kind,
        digest: Digest::from_bytes(decoder.fixed()?),
    };
    let config = decode_config(&mut decoder)?.validate()?;
    decoder.finish()?;
    let value = RetentionCreated {
        volume_id,
        kind,
        label,
        generation_root,
        config,
    };
    validate(&value)?;
    Ok(value)
}

/// Encodes the terminal deletion fact for one workspace authority.
#[must_use]
pub fn encode_workspace_deleted(volume_id: VolumeId) -> Vec<u8> {
    let mut encoder = Encoder::new(DELETED_DOMAIN, VERSION);
    encoder.fixed(&volume_id.into_bytes());
    encoder.finish()
}

/// Decodes one terminal workspace deletion fact.
///
/// # Errors
///
/// Rejects another domain/version, truncation, trailing bytes, or an
/// oversized payload before returning a workspace identity.
pub fn decode_workspace_deleted(
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<VolumeId, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DELETED_DOMAIN, VERSION, maximum_payload_bytes)?;
    let volume_id = VolumeId::from_bytes(decoder.fixed()?);
    decoder.finish()?;
    Ok(volume_id)
}

fn validate(value: &RetentionCreated) -> Result<(), RetentionCreatedError> {
    value.config.validate()?;
    if value.generation_root.kind != ObjectKind::GenerationRoot {
        return Err(RetentionCreatedError::WrongObjectKind);
    }
    if value.label.is_empty()
        || u32::try_from(value.label.len()).unwrap_or(u32::MAX) > MAXIMUM_LABEL_BYTES
        || value
            .label
            .chars()
            .any(|character| character.is_control() || matches!(character, '/' | '\\'))
    {
        return Err(RetentionCreatedError::InvalidLabel);
    }
    Ok(())
}

fn invariant(message: &str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(message.to_owned())
}

#[cfg(test)]
#[path = "tests/retention.rs"]
mod tests;
