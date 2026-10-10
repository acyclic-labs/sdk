//! Canonical immutable volume-generation roots and parent lineage.

use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder};
use super::types::digest_object;
use crate::foundation::{Digest, FileId, GenerationId, VolumeId};
use crate::storage::{ObjectId, ObjectKind, object_digest};
use crate::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use super::allocation::AllocationLedger;
use thiserror::Error;

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
    /// The initial namespace of a filtered fork is its delta baseline. Older
    /// readers reject this required feature rather than publish false deletions.
    pub const FILTERED_FORK: u64 = 1;

    /// Features of subsequent publications; filtering describes only creation.
    pub(crate) fn continuation_features(&self) -> u64 {
        self.required_features & !Self::FILTERED_FORK
    }

    fn validate(&self) -> Result<(), CanonicalDecodeError> {
        if self.file_table.kind != ObjectKind::FileTablePage {
            return Err(invariant("generation file table has the wrong object kind"));
        }
        if self.parents.len() > usize::try_from(MAXIMUM_PARENTS).unwrap_or(usize::MAX) {
            return Err(invariant("generation has too many parents"));
        }
        if let [first, second] = self.parents.as_slice()
            && first == second
        {
            return Err(invariant("generation parents are duplicated"));
        }
        if self.required_features & !Self::FILTERED_FORK != 0 {
            return Err(invariant("generation requires unsupported format features"));
        }
        if self.required_features & Self::FILTERED_FORK != 0 && self.parents.len() != 1 {
            return Err(invariant("filtered fork requires one pinned source parent"));
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
    encode_generation_root_with_permit(root, WorkBudget::UNBOUNDED, None, 0)
        .map(|receipt| receipt.value)
        .map_err(|failure| match failure.error {
            GenerationRootEncodeError::Canonical(error) => error,
            GenerationRootEncodeError::Work(_) => CanonicalDecodeError::LengthOverflow,
            GenerationRootEncodeError::Cancelled(_) => unreachable!("unobserved cancellation"),
        })
}

/// Encodes the canonical root with admission before validation and allocation.
/// `live_owned_bytes` accounts for operation-owned buffers retained by the caller.
/// Borrowed input is not allocated again. Copies count identity and parent bytes;
/// scalar serialization and the static domain contribute only encoded bytes.
/// The ordinary encoder projects this same engine with an unbounded permit.
///
/// # Errors
/// Preserves completed validation and allocation work on every failure.
pub fn encode_generation_root_measured(
    root: &GenerationRoot,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    live_owned_bytes: u64,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, GenerationRootEncodeError> {
    encode_generation_root_with_permit(root, budget, Some(cancellation), live_owned_bytes)
}

fn encode_generation_root_with_permit(
    root: &GenerationRoot,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
    live_owned_bytes: u64,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, GenerationRootEncodeError> {
    let mut work = WorkCounters::default();
    let mut allocations = AllocationLedger::default();
    let result = (|| {
        if let Some(cancellation) = cancellation { cancellation.check()?; }
        allocations.claim_bytes(live_owned_bytes, 0, &mut work, budget)
            .map_err(|error| error.into_work_or(GenerationRootEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
        work.charge_items(1, &budget)?;
        let length = generation_root_encoded_length(root)?;
        let encoded = WorkCounters {
            bytes_encoded: length as u64,
            bytes_copied: (64 + root.parents.len() * 32) as u64,
            ..WorkCounters::default()
        };
        work.admit(&encoded, &budget)?;
        if let Some(cancellation) = cancellation { cancellation.check()?; }
        let admitted = allocations.claim_elements::<u8>(length, &mut work, budget)
            .map_err(|error| error.into_work_or(GenerationRootEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
        let mut encoder = Encoder::with_exact_capacity(DOMAIN, VERSION, length)?;
        let header_bytes = (DOMAIN.len() + 2) as u64;
        work.charge(&WorkCounters { bytes_encoded: header_bytes, ..WorkCounters::default() }, &budget)?;
        let capacity = encoder.capacity() as u64;
        if capacity > admitted {
            // Allocator excess is already physically live; record it before refusal.
            allocations.claim_bytes(capacity - admitted, 0, &mut work, WorkBudget::UNBOUNDED)
                .map_err(|error| error.into_work_or(GenerationRootEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
            work.verify(budget)?;
        }
        encode_generation_root_fields(root, &mut encoder);
        work.charge(&WorkCounters {
            bytes_encoded: length as u64 - header_bytes,
            bytes_copied: encoded.bytes_copied,
            ..WorkCounters::default()
        }, &budget)?;
        Ok(encoder.finish())
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

/// Failure from finite canonical root encoding.
#[derive(Debug, Error)]
pub enum GenerationRootEncodeError {
    /// Invalid canonical root or failed owned-buffer allocation.
    #[error(transparent)]
    Canonical(#[from] CanonicalDecodeError),
    /// The actual operation exceeded its permit.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// The operation was cancelled.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
}

pub(crate) fn generation_root_encoded_length(root: &GenerationRoot) -> Result<usize, CanonicalDecodeError> {
    root.validate()?;
    DOMAIN.len().checked_add(2 + 16 + 16 + 32 + 4 + 8)
        .and_then(|length| root.parents.len().checked_mul(32).and_then(|parents| length.checked_add(parents)))
        .ok_or(CanonicalDecodeError::LengthOverflow)
}

fn encode_generation_root_fields(root: &GenerationRoot, encoder: &mut Encoder) {
    encoder.fixed(&root.volume_id.into_bytes());
    encoder.fixed(&root.root_file_id.into_bytes());
    encoder.fixed(root.file_table.digest.as_bytes());
    encoder.u32(root.parents.len() as u32);
    for parent in &root.parents {
        encoder.fixed(parent.digest().as_bytes());
    }
    encoder.u64(root.required_features);
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
