//! Canonical volume-creation facts and deterministic authority ownership.

use super::codec::{CanonicalDecodeError, Decoder, Encoder};
use crate::foundation::{AuthorityId, Digest, VolumeId};
use crate::model::{
    CaseSensitivity, ConcurrencyMode, FilesystemProfile, Lifecycle, UnicodePolicy, VolumeConfig,
    VolumeConfigError, VolumeLimits,
};
use crate::storage::{ObjectId, ObjectKind};
use thiserror::Error;

const DOMAIN: &[u8] = b"acyclic-fs-volume-created-v1\0";
const VERSION: u16 = 1;

/// Immutable creation fact at sequence one of a volume authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VolumeCreated {
    /// Stable independently versioned volume identity.
    pub volume_id: VolumeId,
    /// Complete immutable semantics selected for the volume.
    pub config: VolumeConfig,
    /// Initial authenticated generation root.
    pub initial_generation_root: ObjectId,
}

/// Canonical volume-creation codec failures.
#[derive(Debug, Error)]
pub enum VolumeCreatedError {
    /// The volume configuration is contradictory or unbounded.
    #[error(transparent)]
    Config(#[from] VolumeConfigError),
    /// The canonical payload is malformed or unsupported.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
}

/// Returns the sole authority identity for a volume.
///
/// This bijection eliminates a mutable volume catalog: resolving a volume does
/// not need a third durable lookup besides authority and immutable objects.
#[must_use]
pub const fn volume_authority_id(volume_id: VolumeId) -> AuthorityId {
    AuthorityId::from_bytes(volume_id.into_bytes())
}

/// Encodes one canonical immutable volume-creation authority fact.
///
/// # Errors
///
/// Rejects invalid configuration or a non-generation initial root.
pub fn encode_volume_created(value: VolumeCreated) -> Result<Vec<u8>, VolumeCreatedError> {
    value.config.validate()?;
    if value.initial_generation_root.kind != ObjectKind::GenerationRoot {
        return Err(invariant("initial volume object is not a generation root").into());
    }
    let mut encoder = Encoder::new(DOMAIN, VERSION);
    encoder.fixed(&value.volume_id.into_bytes());
    encode_config(&mut encoder, value.config);
    encoder.u8(value.initial_generation_root.kind.canonical_tag());
    encoder.fixed(value.initial_generation_root.digest.as_bytes());
    Ok(encoder.finish())
}

/// Decodes one bounded canonical immutable volume-creation authority fact.
///
/// # Errors
///
/// Fails closed for malformed, trailing, unknown, contradictory, oversized, or
/// non-generation-root data.
pub fn decode_volume_created(
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<VolumeCreated, VolumeCreatedError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, maximum_payload_bytes)?;
    let volume_id = VolumeId::from_bytes(decoder.fixed()?);
    let config = decode_config(&mut decoder)?.validate()?;
    let kind = ObjectKind::from_canonical_tag(decoder.u8()?)
        .map_err(|_| invariant("unknown initial-generation object kind"))?;
    if kind != ObjectKind::GenerationRoot {
        return Err(invariant("initial volume object is not a generation root").into());
    }
    let initial_generation_root = ObjectId {
        kind,
        digest: Digest::from_bytes(decoder.fixed()?),
    };
    decoder.finish()?;
    Ok(VolumeCreated {
        volume_id,
        config,
        initial_generation_root,
    })
}

pub(crate) fn encode_config(encoder: &mut Encoder, config: VolumeConfig) {
    encoder.u8(match config.profile {
        FilesystemProfile::Portable => 1,
        FilesystemProfile::Posix => 2,
        FilesystemProfile::Windows => 3,
        FilesystemProfile::Browser => 4,
    });
    encoder.u8(match config.concurrency {
        ConcurrencyMode::ExclusiveWriter => 1,
        ConcurrencyMode::Optimistic => 2,
        ConcurrencyMode::SerializedAuthority => 3,
    });
    encoder.u8(match config.lifecycle {
        Lifecycle::Ephemeral => 1,
        Lifecycle::Durable => 2,
    });
    encoder.u8(match config.case_sensitivity {
        CaseSensitivity::Sensitive => 1,
        CaseSensitivity::ProfileFolded => 2,
    });
    encoder.u8(match config.unicode {
        UnicodePolicy::Preserve => 1,
        UnicodePolicy::RequireNfc => 2,
    });
    encoder.u8(u8::from(config.symbolic_links));
    encoder.u8(u8::from(config.hard_links));
    encoder.u8(u8::from(config.sparse_files));
    encoder.u32(config.limits.maximum_path_bytes);
    encoder.u32(config.limits.maximum_component_bytes);
    encoder.u32(u32::from(config.limits.maximum_path_depth));
    encoder.u64(config.limits.maximum_object_bytes);
    encoder.u32(config.limits.maximum_mutations_per_batch);
    encoder.u32(config.limits.maximum_paths_per_batch);
    encoder.u32(config.limits.maximum_checkout_dependencies);
    encoder.u32(config.limits.maximum_directory_page_entries);
    encoder.u32(u32::from(config.limits.maximum_page_height));
    encoder.u64(config.limits.maximum_read_bytes);
    encoder.u64(config.limits.maximum_files_per_generation);
    encoder.u64(config.limits.maximum_objects_per_generation);
    encoder.u64(config.limits.maximum_generation_bytes);
}

pub(crate) fn decode_config(
    decoder: &mut Decoder<'_>,
) -> Result<VolumeConfig, CanonicalDecodeError> {
    let profile = match decoder.u8()? {
        1 => FilesystemProfile::Portable,
        2 => FilesystemProfile::Posix,
        3 => FilesystemProfile::Windows,
        4 => FilesystemProfile::Browser,
        _ => return Err(invariant("unknown filesystem profile")),
    };
    let concurrency = match decoder.u8()? {
        1 => ConcurrencyMode::ExclusiveWriter,
        2 => ConcurrencyMode::Optimistic,
        3 => ConcurrencyMode::SerializedAuthority,
        _ => return Err(invariant("unknown concurrency mode")),
    };
    let lifecycle = match decoder.u8()? {
        1 => Lifecycle::Ephemeral,
        2 => Lifecycle::Durable,
        _ => return Err(invariant("unknown lifecycle")),
    };
    let case_sensitivity = match decoder.u8()? {
        1 => CaseSensitivity::Sensitive,
        2 => CaseSensitivity::ProfileFolded,
        _ => return Err(invariant("unknown case-sensitivity mode")),
    };
    let unicode = match decoder.u8()? {
        1 => UnicodePolicy::Preserve,
        2 => UnicodePolicy::RequireNfc,
        _ => return Err(invariant("unknown Unicode policy")),
    };
    let symbolic_links = decode_bool(decoder.u8()?)?;
    let hard_links = decode_bool(decoder.u8()?)?;
    let sparse_files = decode_bool(decoder.u8()?)?;
    let maximum_path_bytes = decoder.u32()?;
    let maximum_component_bytes = decoder.u32()?;
    let maximum_path_depth =
        u16::try_from(decoder.u32()?).map_err(|_| invariant("maximum path depth exceeds u16"))?;
    Ok(VolumeConfig {
        profile,
        concurrency,
        lifecycle,
        case_sensitivity,
        unicode,
        symbolic_links,
        hard_links,
        sparse_files,
        limits: VolumeLimits {
            maximum_path_bytes,
            maximum_component_bytes,
            maximum_path_depth,
            maximum_object_bytes: decoder.u64()?,
            maximum_mutations_per_batch: decoder.u32()?,
            maximum_paths_per_batch: decoder.u32()?,
            maximum_checkout_dependencies: decoder.u32()?,
            maximum_directory_page_entries: decoder.u32()?,
            maximum_page_height: u16::try_from(decoder.u32()?)
                .map_err(|_| invariant("maximum page height exceeds u16"))?,
            maximum_read_bytes: decoder.u64()?,
            maximum_files_per_generation: decoder.u64()?,
            maximum_objects_per_generation: decoder.u64()?,
            maximum_generation_bytes: decoder.u64()?,
        },
    })
}

fn decode_bool(value: u8) -> Result<bool, CanonicalDecodeError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(invariant("non-canonical Boolean value")),
    }
}

fn invariant(message: &str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(message.to_owned())
}

#[cfg(test)]
#[path = "tests/volume.rs"]
mod tests;
