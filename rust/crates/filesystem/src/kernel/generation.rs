//! Canonical immutable volume-generation roots and parent lineage.

use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder};
use super::types::digest_object;
use crate::foundation::{Digest, FileId, GenerationId, VolumeId};
use crate::storage::{ObjectId, ObjectKind, object_digest};

const DOMAIN: &[u8; 8] = b"ACYFSGEN";
const VERSION: u16 = 1;
const MAXIMUM_PARENTS: u32 = 2;
/// Largest canonical generation-root object in this format version.
pub const MAXIMUM_GENERATION_ROOT_BYTES: u64 = 150;

/// One immutable volume generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationRoot {
    /// Independently configured/versioned volume identity.
    pub volume_id: VolumeId,
    /// Root directory's path-independent file identity.
    pub root_file_id: FileId,
    /// Authenticated file-table root containing every reachable file record.
    pub file_table: ObjectId,
    /// Ordered zero-, one-, or two-parent lineage.
    pub parents: Vec<GenerationId>,
    /// Required format semantics; unknown bits are rejected by this version.
    pub required_features: u64,
}

impl GenerationRoot {
    fn validate(&self) -> Result<(), CanonicalDecodeError> {
        if self.file_table.kind != ObjectKind::FileTablePage {
            return Err(invariant("generation file table has the wrong object kind"));
        }
        if self.parents.len() > usize::try_from(MAXIMUM_PARENTS).unwrap_or(usize::MAX) {
            return Err(invariant("generation has too many parents"));
        }
        if self.parents.len() == 2 && self.parents[0] == self.parents[1] {
            return Err(invariant("generation parents are duplicated"));
        }
        if self.required_features != 0 {
            return Err(invariant("generation requires unsupported format features"));
        }
        Ok(())
    }
}

/// Encodes one validated canonical generation root.
///
/// # Errors
///
/// Rejects wrong object classes, duplicate/excess parents, and unsupported
/// required feature bits.
pub fn encode_generation_root(root: &GenerationRoot) -> Result<Vec<u8>, CanonicalDecodeError> {
    root.validate()?;
    let mut encoder = Encoder::new(DOMAIN, VERSION);
    encoder.fixed(&root.volume_id.into_bytes());
    encoder.fixed(&root.root_file_id.into_bytes());
    encoder.fixed(root.file_table.digest.as_bytes());
    encoder
        .u32(u32::try_from(root.parents.len()).map_err(|_| CanonicalDecodeError::LengthOverflow)?);
    for parent in &root.parents {
        encoder.fixed(parent.digest().as_bytes());
    }
    encoder.u64(root.required_features);
    Ok(encoder.finish())
}

/// Decodes one bounded canonical generation root.
///
/// # Errors
///
/// Fails closed on malformed bytes, unsupported versions/features, excessive or
/// duplicate parents, and trailing bytes.
pub fn decode_generation_root(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<GenerationRoot, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_object_bytes)?;
    let volume_id = VolumeId::from_bytes(decoder.fixed()?);
    let root_file_id = FileId::from_bytes(decoder.fixed()?);
    let file_table = digest_object(ObjectKind::FileTablePage, decoder.fixed()?);
    let parent_count = decoder.u32()?;
    if parent_count > MAXIMUM_PARENTS {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: parent_count,
            maximum: MAXIMUM_PARENTS,
        });
    }
    let parent_count =
        usize::try_from(parent_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    let mut parents = Vec::new();
    parents
        .try_reserve_exact(parent_count)
        .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
    for _ in 0..parent_count {
        parents.push(GenerationId::new(Digest::from_bytes(decoder.fixed()?)));
    }
    let required_features = decoder.u64()?;
    decoder.finish()?;
    let root = GenerationRoot {
        volume_id,
        root_file_id,
        file_table,
        parents,
        required_features,
    };
    root.validate()?;
    Ok(root)
}

pub(crate) fn generation_root_parent_count(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<usize, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_object_bytes)?;
    let _: [u8; 16] = decoder.fixed()?;
    let _: [u8; 16] = decoder.fixed()?;
    let _: [u8; 32] = decoder.fixed()?;
    let parent_count = decoder.u32()?;
    if parent_count > MAXIMUM_PARENTS {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: parent_count,
            maximum: MAXIMUM_PARENTS,
        });
    }
    usize::try_from(parent_count).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

/// Computes the typed object identity and generation identity of one root.
///
/// # Errors
///
/// Returns the same validation errors as [`encode_generation_root`].
pub fn generation_root_id(
    root: &GenerationRoot,
) -> Result<(ObjectId, GenerationId), CanonicalDecodeError> {
    let bytes = encode_generation_root(root)?;
    let object = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: object_digest(ObjectKind::GenerationRoot, &bytes),
    };
    Ok((object, GenerationId::new(object.digest)))
}

fn invariant(message: &str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(message.to_owned())
}

#[cfg(test)]
#[path = "tests/generation.rs"]
mod tests;
