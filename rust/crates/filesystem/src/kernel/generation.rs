//! Canonical immutable volume-generation roots and parent lineage.

use super::codec::{
    CanonicalDecodeError, DecodeLimits, Decoder, EncodeWorkStep, Encoder, ReadAllocationStep,
};
use super::types::digest_object;
use crate::foundation::{Digest, FileId, GenerationId, VolumeId};
use crate::storage::{ObjectId, ObjectKind, object_digest};
use crate::{CancellationToken, WorkCounters};

const DOMAIN: &[u8; 8] = b"ACYFSGEN";
const VERSION: u16 = 1;
const PARENTS_OFFSET: usize = DOMAIN.len() + 2 + 16 + 16 + 32 + 4;

/// One immutable volume generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationRoot {
    /// Independently configured/versioned volume identity.
    pub volume_id: VolumeId,
    /// Root directory's path-independent file identity.
    pub root_file_id: FileId,
    /// Authenticated file-table root containing every reachable file record.
    pub file_table: ObjectId,
    /// Ordered parent lineage.
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
    let item = WorkCounters {
        items_examined: 1,
        ..WorkCounters::default()
    };
    observe(EncodeWorkStep::Admit(item))?;
    let length = generation_root_encoded_length(root);
    observe(EncodeWorkStep::Completed(item))?;
    let length = length?;
    let parent_bytes = root
        .parents
        .len()
        .checked_mul(32)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    let encoded = WorkCounters {
        bytes_encoded: length as u64,
        bytes_copied: parent_bytes
            .checked_add(64)
            .ok_or(CanonicalDecodeError::LengthOverflow)? as u64,
        ..WorkCounters::default()
    };
    observe(EncodeWorkStep::Admit(encoded))?;
    observe(EncodeWorkStep::Check)?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN,
        VERSION,
        length,
        |step| observe(EncodeWorkStep::Allocation(step)),
        allocation_error,
    )?;
    encoder.fixed(&root.volume_id.into_bytes());
    encoder.fixed(&root.root_file_id.into_bytes());
    encoder.fixed(root.file_table.digest.as_bytes());
    encoder
        .u32(u32::try_from(root.parents.len()).map_err(|_| CanonicalDecodeError::LengthOverflow)?);
    for parent in &root.parents {
        observe(EncodeWorkStep::Check)?;
        encoder.fixed(parent.digest().as_bytes());
    }
    encoder.u64(root.required_features);
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: (length - DOMAIN.len() - 2) as u64,
        bytes_copied: encoded.bytes_copied,
        ..WorkCounters::default()
    }))?;
    let mut bytes = encoder.finish();
    let end = PARENTS_OFFSET
        .checked_add(parent_bytes)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    let (parents, remainder) = bytes[PARENTS_OFFSET..end].as_chunks_mut::<32>();
    debug_assert!(remainder.is_empty());
    validate_unique_parents(parents, |step| {
        observe(match step {
            ParentWork::BeforeItem => EncodeWorkStep::Admit(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            }),
            ParentWork::Item => EncodeWorkStep::Completed(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            }),
            ParentWork::BeforeCopy(bytes) => EncodeWorkStep::Admit(WorkCounters {
                bytes_copied: bytes,
                ..WorkCounters::UNCHARGED
            }),
            ParentWork::Copied(bytes) => EncodeWorkStep::Completed(WorkCounters {
                bytes_copied: bytes,
                ..WorkCounters::UNCHARGED
            }),
        })
    })?;
    if root.parents.len() > 2 {
        for (target, source) in parents.iter_mut().zip(&root.parents) {
            observe(EncodeWorkStep::Admit(WorkCounters {
                bytes_copied: 32,
                ..WorkCounters::UNCHARGED
            }))?;
            *target = source.digest().into_bytes();
            observe(EncodeWorkStep::Completed(WorkCounters {
                bytes_copied: 32,
                ..WorkCounters::UNCHARGED
            }))?;
        }
    }
    Ok(bytes)
}

pub(crate) fn generation_root_encoded_length(
    root: &GenerationRoot,
) -> Result<usize, CanonicalDecodeError> {
    root.validate()?;
    u32::try_from(root.parents.len()).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    DOMAIN
        .len()
        .checked_add(2 + 16 + 16 + 32 + 4 + 8)
        .and_then(|length| {
            root.parents
                .len()
                .checked_mul(32)
                .and_then(|parents| length.checked_add(parents))
        })
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
        bytes,
        DOMAIN,
        VERSION,
        limits.maximum_object_bytes,
        &mut observe,
    )?;
    let volume_id = VolumeId::from_bytes(decoder.fixed::<16, E>(&mut observe)?);
    let root_file_id = FileId::from_bytes(decoder.fixed::<16, E>(&mut observe)?);
    let file_table = digest_object(
        ObjectKind::FileTablePage,
        decoder.fixed::<32, E>(&mut observe)?,
    );
    let parent_count = decoder.u32(&mut observe)?;
    let parent_count =
        usize::try_from(parent_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    let parent_bytes = parent_count
        .checked_mul(32)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    let required_bytes = parent_bytes
        .checked_add(8)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    if required_bytes > decoder.remaining_bytes() {
        return Err(CanonicalDecodeError::Truncated.into());
    }
    observe(ReadAllocationStep::CheckCopy(
        (parent_count * std::mem::size_of::<GenerationId>()) as u64,
    ))?;
    let mut parents = Vec::new();
    super::file_read::reserve_decoded_items(&mut parents, parent_count, &mut observe)?;
    for _ in 0..parent_count {
        observe(ReadAllocationStep::BeforeItem)?;
        parents.push(GenerationId::new(Digest::from_bytes(
            decoder.fixed(&mut observe)?,
        )));
        observe(ReadAllocationStep::Item)?;
    }
    let required_features = decoder.u64(&mut observe)?;
    validate_unique_parents(&mut parents, |step| {
        observe(match step {
            ParentWork::BeforeItem => ReadAllocationStep::BeforeItem,
            ParentWork::Item => ReadAllocationStep::Item,
            ParentWork::BeforeCopy(bytes) => ReadAllocationStep::BeforeCopy(bytes),
            ParentWork::Copied(bytes) => ReadAllocationStep::Copied(bytes),
        })
    })?;
    if parent_count > 2 {
        for (target, source) in parents.iter_mut().zip(
            bytes[PARENTS_OFFSET..PARENTS_OFFSET + parent_bytes]
                .as_chunks::<32>()
                .0,
        ) {
            observe(ReadAllocationStep::BeforeCopy(32))?;
            *target = GenerationId::new(Digest::from_bytes(*source));
            observe(ReadAllocationStep::Copied(32))?;
        }
    }
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
        objects,
        generation_root,
        DecodeLimits::for_volume(config),
        accounting,
        cancellation,
    )
    .await
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
    let maximum_bytes = limits
        .maximum_object_bytes
        .min(accounting.remaining()?.object_bytes_read);
    let (read, retained) = super::file_read::read_object_with_accounting(
        objects,
        generation_root,
        maximum_bytes,
        accounting,
        cancellation,
    )
    .await
    .map_err(|error| match error {
        crate::FsError::BlobRead(super::BlobReadError::Storage(error)) => error.into(),
        error => error,
    })?;
    let mut scope = super::file_read::OriginalReadAccounting::new(accounting)?;
    let root = decode_generation_root(&read, limits, |step| scope.observe(step));
    let root = match root {
        Ok(root) => Ok((root, scope.into_allocation())),
        Err(error) => {
            let _retired = scope.release_failed();
            Err(error)
        }
    };
    drop(read);
    super::file_read::release_standalone_read_bytes(retained, accounting)?;
    root
}

/// Computes the typed object identity and generation identity of one root.
pub fn generation_root_id(
    root: &GenerationRoot,
) -> Result<(ObjectId, GenerationId), CanonicalDecodeError> {
    let bytes = encode_generation_root(
        root,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let object = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: object_digest(ObjectKind::GenerationRoot, &bytes),
    };
    Ok((object, GenerationId::new(object.digest)))
}

// Sorting uses the already funded output storage, then the caller restores
// source order. No scratch allocation or quadratic scan grows with parent count.
enum ParentWork {
    BeforeItem,
    Item,
    BeforeCopy(u64),
    Copied(u64),
}

fn validate_unique_parents<T: Ord, E: From<CanonicalDecodeError>>(
    parents: &mut [T],
    mut observe: impl FnMut(ParentWork) -> Result<(), E>,
) -> Result<(), E> {
    if parents.len() <= 2 {
        if parents.len() == 2 && parents[0] == parents[1] {
            return Err(invariant("generation parents are duplicated").into());
        }
        return Ok(());
    }
    super::sort::heap_sort(
        parents,
        &mut observe,
        |left, right, observe| {
            observe(ParentWork::BeforeItem)?;
            let order = left.cmp(right);
            observe(ParentWork::Item)?;
            Ok::<_, E>(order)
        },
        |values, left, right, observe| {
            let copied = (std::mem::size_of::<T>() as u64)
                .checked_mul(3)
                .ok_or(CanonicalDecodeError::LengthOverflow)?;
            observe(ParentWork::BeforeCopy(copied))?;
            values.swap(left, right);
            observe(ParentWork::Copied(copied))
        },
        |_| Ok(()),
    )?;
    for pair in parents.windows(2) {
        observe(ParentWork::BeforeItem)?;
        let duplicate = pair[0] == pair[1];
        observe(ParentWork::Item)?;
        if duplicate {
            return Err(invariant("generation parents are duplicated").into());
        }
    }
    Ok(())
}

fn invariant(message: &'static str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static(message))
}

#[cfg(test)]
#[path = "tests/generation.rs"]
mod tests;
