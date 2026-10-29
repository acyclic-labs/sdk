//! Canonical generation-transfer manifests with optional caller allowances.

use super::codec::{CanonicalDecodeError, Decoder, Encoder};
use super::volume::{decode_config, encode_config};
use crate::foundation::{GenerationId, VolumeId, usize_to_u64};
use crate::model::{VolumeConfig, VolumeConfigError};
use crate::storage::{ObjectId, ObjectKind};
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
    /// Complete references in the original proof order. Transfer producers
    /// order dependencies before parents and end at `generation_root`;
    /// restoration authenticates exact order and membership before publication.
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
    /// Generation identity must equal the complete generation-root reference.
    #[error("export manifest generation identity does not match its root")]
    GenerationMismatch,
    /// Closure must include the selected generation root.
    #[error("export manifest closure does not include its generation root")]
    MissingRoot,
    /// Closure exceeds an explicitly selected caller item allowance.
    #[error("export manifest object count exceeds its bound")]
    TooManyObjects,
}

/// Encodes one complete manifest using a serializer-independent binary format.
pub fn encode_generation_export_manifest(
    manifest: &GenerationExportManifest,
) -> Result<Vec<u8>, GenerationExportManifestError> {
    validate_generation_export_manifest(manifest, None)?;
    let count = u32::try_from(manifest.objects.len())
        .map_err(|_| GenerationExportManifestError::TooManyObjects)?;
    let mut encoder = Encoder::new(DOMAIN, VERSION);
    encoder.fixed(&manifest.volume_id.into_bytes());
    encode_config(&mut encoder, manifest.config);
    encode_object_id(&mut encoder, manifest.generation_root);
    encoder.u64(manifest.file_count);
    encoder.u32(count);
    for object in &manifest.objects {
        encode_object_id(&mut encoder, *object);
    }
    Ok(encoder.finish())
}

/// Decodes one canonical complete manifest under optional caller allowances.
pub fn decode_generation_export_manifest(
    bytes: &[u8],
    maximum_bytes: impl Into<Option<u64>>,
    maximum_objects: impl Into<Option<u64>>,
) -> Result<GenerationExportManifest, GenerationExportManifestError> {
    let maximum_bytes = maximum_bytes
        .into()
        .unwrap_or_else(|| usize_to_u64(bytes.len()));
    let maximum_objects = maximum_objects.into();
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, maximum_bytes, &mut |_| {
        Ok::<_, crate::kernel::CanonicalDecodeError>(())
    })?;
    let volume_id = VolumeId::from_bytes(
        decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
    );
    let config = decode_config(&mut decoder, &mut |_| {
        Ok::<_, crate::kernel::CanonicalDecodeError>(())
    })?
    .validate()?;
    let generation_root = decode_object_id(&mut decoder)?;
    let generation_id = GenerationId::from_root(generation_root);
    let file_count = decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    let count = decoder.u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    if maximum_objects.is_some_and(|maximum| u64::from(count) > maximum) {
        return Err(GenerationExportManifestError::TooManyObjects);
    }
    let capacity =
        usize::try_from(count).map_err(|_| GenerationExportManifestError::TooManyObjects)?;
    // Every identity is one kind byte and a 32-byte digest. An untrusted
    // announced count cannot cause allocation before its bytes are present.
    if capacity > decoder.remaining_bytes() / 33 {
        return Err(CanonicalDecodeError::Truncated.into());
    }
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

/// Validates bounded complete-manifest fields without backend work.
pub fn validate_generation_export_manifest(
    manifest: &GenerationExportManifest,
    maximum_objects: impl Into<Option<u64>>,
) -> Result<(), GenerationExportManifestError> {
    validate_envelope(manifest, maximum_objects)?;
    if !manifest.objects.contains(&manifest.generation_root) {
        return Err(GenerationExportManifestError::MissingRoot);
    }
    Ok(())
}

// A page touches only its own slice. Full membership is authenticated once on restore.
pub(super) fn validate_transfer_manifest(
    manifest: &GenerationExportManifest,
    maximum_objects: impl Into<Option<u64>>,
) -> Result<(), GenerationExportManifestError> {
    validate_envelope(manifest, maximum_objects)?;
    if manifest.objects.last() != Some(&manifest.generation_root) {
        return Err(GenerationExportManifestError::MissingRoot);
    }
    Ok(())
}

fn validate_envelope(
    manifest: &GenerationExportManifest,
    maximum_objects: impl Into<Option<u64>>,
) -> Result<(), GenerationExportManifestError> {
    let maximum_objects = maximum_objects.into();
    manifest.config.validate()?;
    if manifest.generation_root.kind != ObjectKind::GenerationRoot {
        return Err(GenerationExportManifestError::WrongRootKind);
    }
    if manifest.generation_id.object_id() != manifest.generation_root {
        return Err(GenerationExportManifestError::GenerationMismatch);
    }
    let count = usize_to_u64(manifest.objects.len());
    if maximum_objects.is_some_and(|maximum| count > maximum) {
        return Err(GenerationExportManifestError::TooManyObjects);
    }
    Ok(())
}

fn encode_object_id(encoder: &mut Encoder, object: ObjectId) {
    encoder.object(object);
}

fn decode_object_id(decoder: &mut Decoder<'_>) -> Result<ObjectId, CanonicalDecodeError> {
    decoder.any_object(&mut |_| Ok::<_, CanonicalDecodeError>(()))
}

#[cfg(test)]
#[path = "tests/export.rs"]
mod tests;
