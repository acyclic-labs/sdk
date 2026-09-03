//! Canonical bounded generation-transfer manifests.

use super::codec::{CanonicalDecodeError, Decoder, Encoder};
use super::volume::{decode_config, encode_config};
use crate::foundation::{Digest, GenerationId, VolumeId};
use crate::model::{VolumeConfig, VolumeConfigError};
use crate::storage::{ObjectId, ObjectKind};
use std::cmp::Ordering;
use thiserror::Error;

const DOMAIN: &[u8] = b"acyclic-fs-generation-export-v1\0";
const VERSION: u16 = 1;

/// Deterministic complete object manifest for one immutable generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationExportManifest {
    /// Owning volume identity.
    pub volume_id: VolumeId,
    /// Immutable per-volume semantics required for restoration.
    pub config: VolumeConfig,
    /// Typed immutable generation-root object.
    pub generation_root: ObjectId,
    /// Content-addressed generation identity.
    pub generation_id: GenerationId,
    /// Stable sorted complete closure, including `generation_root`.
    pub objects: Vec<ObjectId>,
    /// Number of reachable path-independent file records.
    pub file_count: u64,
}

/// Fail-closed transfer-manifest validation and codec failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GenerationExportManifestError {
    /// Volume semantics are invalid or unbounded.
    #[error(transparent)]
    Config(#[from] VolumeConfigError),
    /// Canonical bytes are malformed, excessive, or unsupported.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// The selected root is not a generation-root object.
    #[error("export manifest root is not a generation root")]
    WrongRootKind,
    /// Generation identity must equal the generation-root digest.
    #[error("export manifest generation identity does not match its root")]
    GenerationMismatch,
    /// Closure must contain the selected generation root.
    #[error("export manifest closure omits its generation root")]
    MissingRoot,
    /// Object identities must be in strict canonical order without duplicates.
    #[error("export manifest objects are not strictly canonically ordered")]
    NonCanonicalObjectOrder,
    /// Closure exceeds either the volume or caller item bound.
    #[error("export manifest object count exceeds its bound")]
    TooManyObjects,
    /// File count exceeds the immutable volume bound.
    #[error("export manifest file count exceeds its bound")]
    TooManyFiles,
}

/// Encodes one complete manifest using a serializer-independent binary format.
///
/// # Errors
///
/// Rejects invalid volume semantics, mismatched identities, absent roots,
/// excessive counts, duplicate objects, and non-canonical object ordering.
pub fn encode_generation_export_manifest(
    manifest: &GenerationExportManifest,
) -> Result<Vec<u8>, GenerationExportManifestError> {
    validate_generation_export_manifest(
        manifest,
        manifest.config.limits.maximum_objects_per_generation,
    )?;
    let count = u32::try_from(manifest.objects.len())
        .map_err(|_| GenerationExportManifestError::TooManyObjects)?;
    let mut encoder = Encoder::new(DOMAIN, VERSION);
    encoder.fixed(&manifest.volume_id.into_bytes());
    encode_config(&mut encoder, manifest.config);
    encode_object_id(&mut encoder, manifest.generation_root);
    encoder.fixed(manifest.generation_id.digest().as_bytes());
    encoder.u64(manifest.file_count);
    encoder.u32(count);
    for object in &manifest.objects {
        encode_object_id(&mut encoder, *object);
    }
    Ok(encoder.finish())
}

/// Decodes one bounded canonical complete manifest.
///
/// # Errors
///
/// Rejects excessive bytes/items before allocation, malformed fields, invalid
/// configuration, identity mismatch, missing root, duplicates, and disorder.
pub fn decode_generation_export_manifest(
    bytes: &[u8],
    maximum_bytes: u64,
    maximum_objects: u64,
) -> Result<GenerationExportManifest, GenerationExportManifestError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, maximum_bytes)?;
    let volume_id = VolumeId::from_bytes(decoder.fixed()?);
    let config = decode_config(&mut decoder)?.validate()?;
    let generation_root = decode_object_id(&mut decoder)?;
    let generation_id = GenerationId::new(Digest::from_bytes(decoder.fixed()?));
    let file_count = decoder.u64()?;
    let count = decoder.u32()?;
    let admitted = maximum_objects.min(config.limits.maximum_objects_per_generation);
    if u64::from(count) > admitted {
        return Err(GenerationExportManifestError::TooManyObjects);
    }
    let capacity =
        usize::try_from(count).map_err(|_| GenerationExportManifestError::TooManyObjects)?;
    let mut objects = Vec::new();
    objects
        .try_reserve_exact(capacity)
        .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
    for _ in 0..count {
        objects.push(decode_object_id(&mut decoder)?);
    }
    decoder.finish()?;
    let manifest = GenerationExportManifest {
        volume_id,
        config,
        generation_root,
        generation_id,
        objects,
        file_count,
    };
    validate_generation_export_manifest(&manifest, maximum_objects)?;
    Ok(manifest)
}

/// Validates one decoded or caller-constructed manifest without backend work.
///
/// # Errors
///
/// Rejects invalid semantics, identities, bounds, closure ordering, or a
/// missing generation root.
pub fn validate_generation_export_manifest(
    manifest: &GenerationExportManifest,
    maximum_objects: u64,
) -> Result<(), GenerationExportManifestError> {
    manifest.config.validate()?;
    if manifest.generation_root.kind != ObjectKind::GenerationRoot {
        return Err(GenerationExportManifestError::WrongRootKind);
    }
    if manifest.generation_id.digest() != manifest.generation_root.digest {
        return Err(GenerationExportManifestError::GenerationMismatch);
    }
    if manifest.file_count > manifest.config.limits.maximum_files_per_generation {
        return Err(GenerationExportManifestError::TooManyFiles);
    }
    let count = u64::try_from(manifest.objects.len()).unwrap_or(u64::MAX);
    if count > maximum_objects || count > manifest.config.limits.maximum_objects_per_generation {
        return Err(GenerationExportManifestError::TooManyObjects);
    }
    if !manifest.objects.contains(&manifest.generation_root) {
        return Err(GenerationExportManifestError::MissingRoot);
    }
    if manifest
        .objects
        .windows(2)
        .any(|pair| compare_object_id(pair[0], pair[1]) != Ordering::Less)
    {
        return Err(GenerationExportManifestError::NonCanonicalObjectOrder);
    }
    Ok(())
}

fn compare_object_id(left: ObjectId, right: ObjectId) -> Ordering {
    left.kind
        .canonical_tag()
        .cmp(&right.kind.canonical_tag())
        .then_with(|| left.digest.cmp(&right.digest))
}

fn encode_object_id(encoder: &mut Encoder, object: ObjectId) {
    encoder.u8(object.kind.canonical_tag());
    encoder.fixed(object.digest.as_bytes());
}

fn decode_object_id(decoder: &mut Decoder<'_>) -> Result<ObjectId, CanonicalDecodeError> {
    let kind = ObjectKind::from_canonical_tag(decoder.u8()?)
        .map_err(|error| CanonicalDecodeError::Invariant(error.to_string()))?;
    Ok(ObjectId {
        kind,
        digest: Digest::from_bytes(decoder.fixed()?),
    })
}

#[cfg(test)]
#[path = "tests/export.rs"]
mod tests;
