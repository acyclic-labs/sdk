//! Canonical independently addressable generation-retention facts.

use super::codec::{CanonicalDecodeError, Decoder, Encoder, EncodeWorkStep};

use crate::{WorkCounters};
use super::volume::{CONFIG_ENCODED_BYTES, decode_config, encode_config};
use crate::foundation::{AuthorityId, Digest, VolumeId, usize_to_u64};
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
    /// Finite preparation permit was exhausted.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Preparation was cancelled.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
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
    hasher.update(&usize_to_u64(label.len()).to_le_bytes());
    hasher.update(label.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    AuthorityId::from_bytes(bytes)
}

/// Encodes one canonical object with before-effect admission.
pub fn encode_retention_created<E: From<CanonicalDecodeError> + From<RetentionCreatedError>>(
    value: &RetentionCreated,
    mut observe: impl FnMut(EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E> {
    observe(EncodeWorkStep::Check)?;
    validate(value)?;
    let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    observe(EncodeWorkStep::Admit(item))?;
    observe(EncodeWorkStep::Completed(item))?;
    let length = DOMAIN.len() + 2 + 16 + 1 + 4 + value.label.len() + 1 + 32 + CONFIG_ENCODED_BYTES;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN, VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)), allocation_error,
    )?;
    let fields = WorkCounters {
        bytes_encoded: (16 + 1 + 4 + value.label.len() + 1 + 32) as u64,
        bytes_copied: (48 + value.label.len()) as u64,
        ..WorkCounters::UNCHARGED
    };
    observe(EncodeWorkStep::Admit(fields))?;
    encoder.fixed(&value.volume_id.into_bytes());
    encoder.u8(value.kind.tag());
    encoder.bounded_bytes(value.label.as_bytes())?;
    encoder.u8(value.generation_root.kind.canonical_tag());
    encoder.fixed(value.generation_root.digest.as_bytes());
    observe(EncodeWorkStep::Completed(fields))?;
    let config = WorkCounters {
        bytes_encoded: CONFIG_ENCODED_BYTES as u64, ..WorkCounters::UNCHARGED
    };
    observe(EncodeWorkStep::Admit(config))?;
    encode_config(&mut encoder, value.config);
    observe(EncodeWorkStep::Completed(config))?;
    Ok(encoder.finish())
}

/// Decodes one exact bounded immutable retention fact.
pub fn decode_retention_created(
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<RetentionCreated, RetentionCreatedError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, maximum_payload_bytes, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    let volume_id = VolumeId::from_bytes(decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?);
    let kind = RetentionKind::from_tag(decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?)?;
    let label = String::from_utf8(decoder.bounded_bytes(MAXIMUM_LABEL_BYTES, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?)
        .map_err(|_| RetentionCreatedError::InvalidLabel)?;
    let object_kind = ObjectKind::from_canonical_tag(decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?)
        .map_err(|_| RetentionCreatedError::WrongObjectKind)?;
    let generation_root = ObjectId {
        kind: object_kind,
        digest: Digest::from_bytes(decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?),
    };
    let config = decode_config(&mut decoder, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?.validate()?;
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

/// Decodes one terminal workspace deletion fact.
pub fn decode_workspace_deleted(
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<VolumeId, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DELETED_DOMAIN, VERSION, maximum_payload_bytes, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    let volume_id = VolumeId::from_bytes(decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?);
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

fn invariant(message: &'static str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static(message))
}

#[cfg(test)]
#[path = "tests/retention.rs"]
mod tests;

/// Encodes one canonical terminal workspace fact with before-effect admission.
pub fn encode_workspace_deleted<E: From<CanonicalDecodeError>>(
    volume_id: VolumeId,
    mut observe: impl FnMut(EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E> {
    observe(EncodeWorkStep::Check)?;
    let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    observe(EncodeWorkStep::Admit(item))?;
    observe(EncodeWorkStep::Completed(item))?;
    let length = DELETED_DOMAIN.len() + 2 + 16;
    observe(EncodeWorkStep::Admit(WorkCounters {
        bytes_encoded: length as u64, bytes_copied: 16, ..WorkCounters::UNCHARGED
    }))?;
    let mut encoder = Encoder::with_exact_capacity(DELETED_DOMAIN, VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)), allocation_error)?;
    observe(EncodeWorkStep::Check)?;
    encoder.fixed(&volume_id.into_bytes());
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: 16, bytes_copied: 16, ..WorkCounters::UNCHARGED
    }))?;
    Ok(encoder.finish())
}
