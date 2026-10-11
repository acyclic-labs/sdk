//! Canonical immutable volume-generation roots and parent lineage.

use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder, EncodeAllocationStep, EncodeWorkStep, EncodeAccounting};
use super::types::digest_object;
use crate::foundation::{Digest, FileId, GenerationId, VolumeId};
use crate::storage::{ObjectId, ObjectKind, object_digest};
use crate::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use super::allocation::{AllocationLedger, KernelAccounting, KernelOwnedAllocation};
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
            GenerationRootEncodeError::Work(error) => CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Work(error)),
            GenerationRootEncodeError::Cancelled(_) => CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static("filesystem operation was cancelled")),
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
        encode_generation_root_with_allocations(
            root, &mut allocations, &mut work, budget, cancellation,
            |_| GenerationRootEncodeError::Canonical(CanonicalDecodeError::AllocationFailed),
        )
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

/// Uses the same writer on an existing operation ledger; native output keeps
/// its actual capacity guard until the caller drops the encoded bytes.
pub(crate) fn encode_generation_root_with_accounting(
    root: &GenerationRoot,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError> {
    cancellation.check()?;
    #[cfg(not(target_arch = "wasm32"))]
    let native_original = accounting.native_work().is_some();
    let mut encoding = EncodeAccounting::new(accounting);
    let result = encode_generation_root_with_steps(
        root,
        |step| encoding.observe(step),
        |error| {
            #[cfg(not(target_arch = "wasm32"))]
            if native_original {
                return crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(error));
            }
            let _ = error;
            CanonicalDecodeError::AllocationFailed.into()
        },
    );
    encoding.finish(result)
}

fn encode_generation_root_with_allocations<E>(
    root: &GenerationRoot,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where E: From<CanonicalDecodeError> + From<crate::WorkError> + From<crate::CancellationError> {
    let before = allocations.live_bytes();
    let result = encode_generation_root_with_steps(
        root,
        |step| {
            match step {
                EncodeWorkStep::Check => {
                    if let Some(cancellation) = cancellation { cancellation.check()?; }
                }
                EncodeWorkStep::Admit(delta) => work.admit(&delta, &budget)?,
                EncodeWorkStep::Completed(delta) => {
                    work.try_add_assign(&delta)?;
                    work.verify(budget)?;
                    if let Some(cancellation) = cancellation { cancellation.check()?; }
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::Reserve { capacity }) => {
                    allocations.claim_elements::<u8>(capacity, work, budget)
                        .map_err(|error| error.into_work_or(E::from(CanonicalDecodeError::AllocationFailed)))?;
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
                    if actual_bytes > admitted_bytes {
                        allocations.record_owned_bytes(actual_bytes - admitted_bytes, work)
                            .map_err(|error| error.into_work_or(E::from(CanonicalDecodeError::AllocationFailed)))?;
                        work.verify(budget)?;
                    }
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::BeforeHeader { bytes }) => {
                    work.admit(&WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() }, &budget)?;
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }) => {
                    work.try_add_assign(&WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() })?;
                    work.verify(budget)?;
                    if let Some(cancellation) = cancellation { cancellation.check()?; }
                }
            }
            Ok(())
        },
        allocation_error,
    );
    if result.is_err() {
        let owned = allocations.live_bytes().checked_sub(before).ok_or(crate::WorkError::Overflow)?;
        allocations.release(owned)
            .map_err(|error| error.into_work_or(E::from(crate::WorkError::Overflow)))?;
    }
    result
}

fn encode_generation_root_with_steps<E: From<CanonicalDecodeError>>(
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
    let mut encoder = Encoder::with_exact_capacity_with_steps(
        DOMAIN, VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)),
        allocation_error,
    )?;
    encode_generation_root_fields(root, &mut encoder);
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: (length - DOMAIN.len() - 2) as u64,
        bytes_copied: encoded.bytes_copied,
        ..WorkCounters::default()
    }))?;
    Ok(encoder.finish())
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
    decode_generation_root_with_steps(bytes, limits, |_| Ok(()))
}

/// Actual boundaries of the one canonical parent-buffer decoder.
pub(crate) enum GenerationRootDecodeStep {
    Wire(super::codec::ReadAllocationStep),
    BeforeParentsAllocation(usize),
    ReserveFailed(std::collections::TryReserveError),
    ParentsCapacity(usize),
    BeforeParent,
    ParentCopied,
}

/// Uses the canonical decoder with short synchronous admission/completion
/// callbacks. Copies are completed only after a successful parent decode/push.
pub(crate) fn decode_generation_root_with_steps<E>(
    bytes: &[u8],
    limits: DecodeLimits,
    mut observe: impl FnMut(GenerationRootDecodeStep) -> Result<(), E>,
) -> Result<GenerationRoot, E>
where
    E: From<CanonicalDecodeError>,
{
    let mut decoder = Decoder::new_with_steps(
        bytes, DOMAIN, VERSION, limits.maximum_object_bytes,
        &mut |step| observe(GenerationRootDecodeStep::Wire(step)),
    )?;
    let volume_id = VolumeId::from_bytes(decoder.fixed_with_steps::<16, E>(
        &mut |step| observe(GenerationRootDecodeStep::Wire(step)),
    )?);
    let root_file_id = FileId::from_bytes(decoder.fixed_with_steps::<16, E>(
        &mut |step| observe(GenerationRootDecodeStep::Wire(step)),
    )?);
    let file_table = digest_object(ObjectKind::FileTablePage, decoder.fixed_with_steps::<32, E>(
        &mut |step| observe(GenerationRootDecodeStep::Wire(step)),
    )?);
    let parent_count = decoder.u32_with_steps(&mut |step| observe(GenerationRootDecodeStep::Wire(step)))?;
    if parent_count > MAXIMUM_PARENTS {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: parent_count,
            maximum: MAXIMUM_PARENTS,
        }.into());
    }
    let parent_count =
        usize::try_from(parent_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    observe(GenerationRootDecodeStep::BeforeParentsAllocation(parent_count))?;
    let mut parents = Vec::new();
    if let Err(error) = parents.try_reserve_exact(parent_count) {
        observe(GenerationRootDecodeStep::ReserveFailed(error))?;
        return Err(CanonicalDecodeError::AllocationFailed.into());
    }
    observe(GenerationRootDecodeStep::ParentsCapacity(parents.capacity()))?;
    for _ in 0..parent_count {
        observe(GenerationRootDecodeStep::BeforeParent)?;
        parents.push(GenerationId::new(Digest::from_bytes(decoder.fixed()?)));
        observe(GenerationRootDecodeStep::ParentCopied)?;
    }
    let required_features = decoder.u64_with_steps(&mut |step| observe(GenerationRootDecodeStep::Wire(step)))?;
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

/// Reads and decodes a root on one selected accounting context. Native provider
/// bytes carry their original claim inside the physical buffer; only standalone
/// reads merge provider receipts and retain their existing ledger bookkeeping.
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

/// Reads the same authenticated root with the caller's exact decoder limits,
/// retaining original native parent-buffer custody through every failure.
pub(crate) async fn read_generation_root_with_limits_and_accounting<S: crate::AsyncObjectStore>(
    objects: &S,
    generation_root: ObjectId,
    limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(GenerationRoot, super::allocation::KernelOwnedAllocation), crate::FsError> {
    use super::allocation::KernelAccounting;

    accounting.check()?;
    cancellation.check()?;
    let maximum_bytes = MAXIMUM_GENERATION_ROOT_BYTES.min(limits.maximum_object_bytes);
    let (read, standalone_retained) = match accounting {
        KernelAccounting::Standalone { allocations, work, budget, .. } => {
            let (read, retained) = read_root_standalone(
                objects, generation_root, maximum_bytes, allocations, work, *budget, cancellation,
            ).await?;
            (read, Some(retained))
        }
        KernelAccounting::BorrowedStandalone { allocations, work, budget, .. } => {
            let (read, retained) = read_root_standalone(
                objects, generation_root, maximum_bytes, allocations, work, *budget, cancellation,
            ).await?;
            (read, Some(retained))
        }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work, .. } => {
            let read = objects.read_with_native_work(
                generation_root, maximum_bytes, work, cancellation,
            ).await.map_err(|error| match error {
                crate::storage::ObjectStoreError::Native(error) => crate::FsError::NativeKernel(error),
                error => error.into(),
            })?;
            (read.into_read(), None)
        }
    };
    let charge_parent_items = !matches!(
        accounting, KernelAccounting::Standalone { .. } | KernelAccounting::BorrowedStandalone { .. },
    );
    let mut scope = super::file_read::OriginalReadAccounting::new(accounting)?;
    let mut admitted_parent_bytes = 0;
    let root = decode_generation_root_with_steps(&read, limits, |step| -> Result<(), crate::FsError> {
        match step {
            GenerationRootDecodeStep::Wire(step) => {
                scope.observe(step).map_err(|error| {
                    #[cfg(not(target_arch = "wasm32"))]
                    if charge_parent_items { return error.into_original_fs_error(); }
                    match error {
                        super::file_read::ReadDecodeError::Canonical(error) => error.into(),
                        super::file_read::ReadDecodeError::AllocationFailed(_) => CanonicalDecodeError::AllocationFailed.into(),
                        super::file_read::ReadDecodeError::Accounting(error) => error,
                    }
                })?;
            }
            GenerationRootDecodeStep::BeforeParentsAllocation(parent_count) => {
                cancellation.check()?;
                admitted_parent_bytes = parent_count.checked_mul(std::mem::size_of::<GenerationId>())
                    .map(crate::foundation::usize_to_u64).ok_or(crate::WorkError::Overflow)?;
                scope.admit(WorkCounters {
                    bytes_copied: admitted_parent_bytes,
                    items_examined: if charge_parent_items { crate::foundation::usize_to_u64(parent_count) } else { 0 },
                    ..WorkCounters::UNCHARGED
                })?;
                scope.observe(super::codec::ReadAllocationStep::Allocate {
                    bytes: admitted_parent_bytes, operations: u64::from(parent_count != 0),
                }).map_err(|error| match error {
                    super::file_read::ReadDecodeError::Canonical(error) => error.into(),
                    super::file_read::ReadDecodeError::AllocationFailed(_) => CanonicalDecodeError::AllocationFailed.into(),
                    super::file_read::ReadDecodeError::Accounting(error) => error,
                })?;
            }
            GenerationRootDecodeStep::ReserveFailed(error) => {
                #[cfg(not(target_arch = "wasm32"))]
                if charge_parent_items { return Err(crate::NativeTreeMaterializationError::AllocationFailed(error).into()); }
                return Err(CanonicalDecodeError::AllocationFailed.into());
            }
            GenerationRootDecodeStep::ParentsCapacity(capacity) => {
                let actual_bytes = capacity.checked_mul(std::mem::size_of::<GenerationId>())
                    .map(crate::foundation::usize_to_u64).ok_or(crate::WorkError::Overflow)?;
                scope.observe(super::codec::ReadAllocationStep::Capacity {
                    admitted_bytes: admitted_parent_bytes, actual_bytes,
                }).map_err(|error| match error {
                    super::file_read::ReadDecodeError::Canonical(error) => error.into(),
                    super::file_read::ReadDecodeError::AllocationFailed(_) => CanonicalDecodeError::AllocationFailed.into(),
                    super::file_read::ReadDecodeError::Accounting(error) => error,
                })?;
            }
            GenerationRootDecodeStep::BeforeParent => {
                cancellation.check()?;
                scope.admit(WorkCounters {
                    bytes_copied: crate::foundation::usize_to_u64(std::mem::size_of::<GenerationId>()),
                    items_examined: u64::from(charge_parent_items), ..WorkCounters::UNCHARGED
                })?;
            }
            GenerationRootDecodeStep::ParentCopied => {
                scope.record_completed(WorkCounters {
                    bytes_copied: crate::foundation::usize_to_u64(std::mem::size_of::<GenerationId>()),
                    items_examined: u64::from(charge_parent_items), ..WorkCounters::UNCHARGED
                })?;
            }
        }
        Ok(())
    });
    let root = match root {
        Ok(root) => Ok((root, scope.into_allocation())),
        Err(error) => { let _retired = scope.release_failed(); Err(error) }
    };
    drop(read);
    if let Some(retained) = standalone_retained {
        let mut allocation = super::allocation::KernelOwnedAllocation::Standalone;
        accounting.release_owned(&mut allocation, retained)?;
    }
    root
}

async fn read_root_standalone<S: crate::AsyncObjectStore>(
    objects: &S,
    generation_root: ObjectId,
    maximum_bytes: u64,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(crate::storage::ObjectRead, u64), crate::FsError> {
    let mut remaining = work.remaining(budget)?;
    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
        .checked_sub(allocations.live_bytes()).ok_or(crate::WorkError::Overflow)?;
    let receipt = objects.read(generation_root, maximum_bytes, remaining, cancellation).await;
    let prior = *work;
    let receipt = match receipt {
        Ok(receipt) => receipt,
        Err(failure) => {
            *work = *failure.work;
            *work = prior.with_backend(*work, allocations.live_bytes())
                .map_err(|error| crate::FsError::RootReadWorkMerge {
                    prior, backend: *work, error,
                })?;
            return Err(failure.error.into());
        }
    };
    *work = receipt.work;
    *work = prior.with_backend(*work, allocations.live_bytes())
        .map_err(|error| crate::FsError::RootReadWorkMerge {
            prior, backend: *work, error,
        })?;
    let retained = match receipt.value.retention {
        crate::storage::ObjectReadRetention::Shared => 0,
        crate::storage::ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
    };
    allocations.claim_bytes(retained, 0, work, budget)
        .map_err(|error| error.into_work_or(crate::FsError::Decode(CanonicalDecodeError::AllocationFailed)))?;
    Ok((receipt.value, retained))
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

fn invariant(message: &'static str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static(message))
}

#[cfg(test)]
#[path = "tests/generation.rs"]
mod tests;
