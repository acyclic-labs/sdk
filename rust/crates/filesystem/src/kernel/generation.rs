//! Canonical immutable volume-generation roots and parent lineage.

use super::codec::{
    CanonicalDecodeError, DecodeLimits, Decoder, EncodeWorkStep, Encoder, ReadAllocationStep,
};
#[cfg(test)]
use crate::Digest;
use crate::foundation::{FileId, GenerationId, VolumeId};
use crate::storage::{ObjectId, ObjectKind, object_digest};
use crate::{CancellationToken, WorkCounters};

const DOMAIN: &[u8; 8] = b"ACYFSGEN";
const VERSION: u16 = 1;

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
    // Duplicate validation sorts the already funded output. Fixed-width keys
    // include the complete seal; compact serialization below restores caller
    // order and removes padding without a second allocation.
    let indexed = root.parents.len() > 2;
    let parents_offset = DOMAIN.len() + 2 + 16 + 16 + root.file_table.encoded_len() + 4;
    let allocated_length = if indexed {
        root.parents
            .len()
            .checked_mul(ObjectId::ENCODED_LEN)
            .and_then(|bytes| parents_offset.checked_add(bytes))
            .and_then(|bytes| bytes.checked_add(8))
            .ok_or(CanonicalDecodeError::LengthOverflow)?
    } else {
        length
    };
    let parent_bytes = allocated_length - parents_offset - 8;
    let encoded = WorkCounters {
        bytes_encoded: allocated_length as u64,
        bytes_copied: parent_bytes
            .checked_add(32 + root.file_table.encoded_len())
            .ok_or(CanonicalDecodeError::LengthOverflow)? as u64,
        ..WorkCounters::default()
    };
    observe(EncodeWorkStep::Admit(encoded))?;
    observe(EncodeWorkStep::Check)?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN,
        VERSION,
        allocated_length,
        |step| observe(EncodeWorkStep::Allocation(step)),
        allocation_error,
    )?;
    encoder.fixed(&root.volume_id.into_bytes());
    encoder.fixed(&root.root_file_id.into_bytes());
    encoder.object(root.file_table);
    encoder
        .u32(u32::try_from(root.parents.len()).map_err(|_| CanonicalDecodeError::LengthOverflow)?);
    for parent in &root.parents {
        observe(EncodeWorkStep::Check)?;
        if indexed {
            encoder.fixed(&parent.object_id().index_key());
        } else {
            encoder.object(parent.object_id());
        }
    }
    encoder.u64(root.required_features);
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: (allocated_length - DOMAIN.len() - 2) as u64,
        bytes_copied: encoded.bytes_copied,
        ..WorkCounters::default()
    }))?;
    let mut bytes = encoder.finish();
    if indexed {
        let end = parents_offset + parent_bytes;
        let (parents, remainder) =
            bytes[parents_offset..end].as_chunks_mut::<{ ObjectId::ENCODED_LEN }>();
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
        let mut cursor = parents_offset;
        for parent in &root.parents {
            observe(EncodeWorkStep::Check)?;
            let reference = parent.object_id().to_bytes();
            let reference = reference.as_slice();
            let copying = WorkCounters {
                bytes_encoded: reference.len() as u64,
                bytes_copied: reference.len() as u64,
                ..WorkCounters::UNCHARGED
            };
            observe(EncodeWorkStep::Admit(copying))?;
            bytes[cursor..cursor + reference.len()].copy_from_slice(reference);
            cursor += reference.len();
            observe(EncodeWorkStep::Completed(copying))?;
        }
        let features = WorkCounters {
            bytes_encoded: 8,
            bytes_copied: 8,
            ..WorkCounters::UNCHARGED
        };
        observe(EncodeWorkStep::Admit(features))?;
        bytes[cursor..cursor + 8].copy_from_slice(&root.required_features.to_le_bytes());
        observe(EncodeWorkStep::Completed(features))?;
        bytes.truncate(length);
    } else if root.parents.len() == 2 && root.parents[0] == root.parents[1] {
        return Err(invariant("generation parents are duplicated").into());
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
        .checked_add(2 + 16 + 16 + root.file_table.encoded_len() + 4 + 8)
        .and_then(|length| {
            root.parents.iter().try_fold(length, |length, parent| {
                length.checked_add(parent.object_id().encoded_len())
            })
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
    let file_table = decoder.object(ObjectKind::FileTablePage, &mut observe)?;
    let parent_count = decoder.u32(&mut observe)?;
    let parent_count =
        usize::try_from(parent_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    let parent_bytes = parent_count
        .checked_mul(ObjectId::PLAIN_ENCODED_LEN)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    let required_bytes = parent_bytes
        .checked_add(8)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    if required_bytes > decoder.remaining_bytes() {
        return Err(CanonicalDecodeError::Truncated.into());
    }
    observe(ReadAllocationStep::CheckCopy(parent_bytes as u64))?;
    let mut parents = Vec::new();
    super::file_read::reserve_decoded_items(&mut parents, parent_count, &mut observe)?;
    let mut original_parents = decoder;
    for _ in 0..parent_count {
        observe(ReadAllocationStep::BeforeItem)?;
        parents.push(GenerationId::from_root(
            decoder.object(ObjectKind::GenerationRoot, &mut observe)?,
        ));
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
        for target in &mut parents {
            observe(ReadAllocationStep::BeforeItem)?;
            observe(ReadAllocationStep::BeforeCopy(
                std::mem::size_of::<GenerationId>() as u64,
            ))?;
            *target = GenerationId::from_root(
                original_parents.object(ObjectKind::GenerationRoot, &mut observe)?,
            );
            observe(ReadAllocationStep::Copied(
                std::mem::size_of::<GenerationId>() as u64,
            ))?;
            observe(ReadAllocationStep::Item)?;
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
        Ok(root)
            if generation_root
                .storage
                .is_none_or(|storage| storage.root_file_id == root.root_file_id) =>
        {
            Ok((root, scope.into_allocation()))
        }
        Ok(_) => {
            scope.release_failed()?;
            Err(invariant("generation seal has the wrong root directory identity").into())
        }
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
        storage: None,
        kind: ObjectKind::GenerationRoot,
        digest: object_digest(&bytes),
    };
    Ok((object, GenerationId::from_root(object)))
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
