//! Canonical immutable volume-generation roots and parent lineage.

use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder, EncodeWorkStep, ReadAllocationStep};
use super::types::digest_object;
use crate::foundation::{Digest, FileId, GenerationId, VolumeId};
use crate::storage::{ObjectId, ObjectKind, object_digest};
use crate::{CancellationToken, WorkCounters};

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
    /// The initial namespace of a filtered fork is its delta baseline.
    pub const FILTERED_FORK: u64 = 1;

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

/// Encodes one canonical object with before-effect admission.
pub fn encode_generation_root<E: From<CanonicalDecodeError>>(
    root: &GenerationRoot,
    mut observe: impl FnMut(EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E> {
    observe(EncodeWorkStep::Check)?;
    let item = WorkCounters { items_examined: 1, ..WorkCounters::default() };
    observe(EncodeWorkStep::Admit(item))?;
    let length = generation_root_encoded_length(root);
    observe(EncodeWorkStep::Completed(item))?;
    let length = length?;
    let encoded = WorkCounters {
        bytes_encoded: length as u64,
        bytes_copied: (64 + root.parents.len() * 32) as u64,
        ..WorkCounters::default()
    };
    observe(EncodeWorkStep::Admit(encoded))?;
    observe(EncodeWorkStep::Check)?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN, VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)),
        allocation_error,
    )?;
    encoder.fixed(&root.volume_id.into_bytes());
    encoder.fixed(&root.root_file_id.into_bytes());
    encoder.fixed(root.file_table.digest.as_bytes());
    encoder.u32(root.parents.len() as u32);
    for parent in &root.parents { encoder.fixed(parent.digest().as_bytes()); }
    encoder.u64(root.required_features);
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: (length - DOMAIN.len() - 2) as u64,
        bytes_copied: encoded.bytes_copied,
        ..WorkCounters::default()
    }))?;
    Ok(encoder.finish())
}

pub(crate) fn generation_root_encoded_length(root: &GenerationRoot) -> Result<usize, CanonicalDecodeError> {
    root.validate()?;
    DOMAIN.len().checked_add(2 + 16 + 16 + 32 + 4 + 8)
        .and_then(|length| root.parents.len().checked_mul(32).and_then(|parents| length.checked_add(parents)))
        .ok_or(CanonicalDecodeError::LengthOverflow)
}

/// Decodes one canonical object with before-effect admission.
pub fn decode_generation_root<E>(
    bytes: &[u8],
    limits: DecodeLimits,
    mut observe: impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<GenerationRoot, E>
where
    E: From<CanonicalDecodeError>,
{
    let mut decoder = Decoder::new(
        bytes, DOMAIN, VERSION, limits.maximum_object_bytes,
        &mut observe,
    )?;
    let volume_id = VolumeId::from_bytes(decoder.fixed::<16, E>(
        &mut observe,
    )?);
    let root_file_id = FileId::from_bytes(decoder.fixed::<16, E>(
        &mut observe,
    )?);
    let file_table = digest_object(ObjectKind::FileTablePage, decoder.fixed::<32, E>(
        &mut observe,
    )?);
    let parent_count = decoder.u32(&mut observe)?;
    if parent_count > MAXIMUM_PARENTS {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: parent_count,
            maximum: MAXIMUM_PARENTS,
        }.into());
    }
    let parent_count =
        usize::try_from(parent_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    observe(ReadAllocationStep::BeforeCopy((parent_count * std::mem::size_of::<GenerationId>()) as u64))?;
    let mut parents = Vec::new();
    super::file_read::reserve_decoded_items(&mut parents, parent_count, &mut observe)?;
    for _ in 0..parent_count {
        observe(ReadAllocationStep::BeforeItem)?;
        parents.push(GenerationId::new(Digest::from_bytes(decoder.fixed(&mut observe)?)));
        observe(ReadAllocationStep::Item)?;
    }
    let required_features = decoder.u64(&mut observe)?;
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

pub(crate) async fn read_generation_root_with_accounting<S: crate::AsyncObjectStore>(
    objects: &S,
    generation_root: ObjectId,
    config: crate::VolumeConfig,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(GenerationRoot, super::allocation::KernelOwnedAllocation), crate::FsError> {
    read_generation_root_with_limits_and_accounting(
        objects, generation_root, DecodeLimits::for_volume(config), accounting, cancellation,
    ).await
}

pub(crate) async fn read_generation_root_with_limits_and_accounting<S: crate::AsyncObjectStore>(
    objects: &S,
    generation_root: ObjectId,
    limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(GenerationRoot, super::allocation::KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    cancellation.check()?;
    let maximum_bytes = MAXIMUM_GENERATION_ROOT_BYTES.min(limits.maximum_object_bytes);
    let (read, retained) = super::file_read::read_object_with_accounting(
        objects, generation_root, maximum_bytes, accounting, cancellation,
    ).await.map_err(|error| match error {
        crate::FsError::BlobRead(super::BlobReadError::Storage(error)) => error.into(),
        error => error,
    })?;
    let mut scope = super::file_read::OriginalReadAccounting::new(accounting)?;
    let root = decode_generation_root(&read, limits, |step| scope.observe(step));
    let root = match root {
        Ok(root) => Ok((root, scope.into_allocation())),
        Err(error) => { let _retired = scope.release_failed(); Err(error) }
    };
    drop(read);
    super::file_read::release_standalone_read_bytes(retained, accounting)?;
    root
}

/// Computes the typed object identity and generation identity of one root.
pub fn generation_root_id(
    root: &GenerationRoot,
) -> Result<(ObjectId, GenerationId), CanonicalDecodeError> {
    let bytes = encode_generation_root(root, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
    let object = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: object_digest(ObjectKind::GenerationRoot, &bytes),
    };
    Ok((object, GenerationId::new(object.digest)))
}

fn invariant(message: &'static str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static(message))
}

#[cfg(test)]
#[path = "tests/generation.rs"]
mod tests;
