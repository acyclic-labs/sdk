//! Canonical volume-creation facts and deterministic authority ownership.

use super::codec::{CanonicalDecodeError, Decoder, Encoder, ReadAllocationStep, EncodeAllocationStep, EncodeWorkStep, EncodeAccounting};
use super::allocation::{AllocationLedger, KernelAccounting, KernelOwnedAllocation};
use crate::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use crate::foundation::{AuthorityId, Digest, VolumeId};
use crate::model::{
    CaseSensitivity, ConcurrencyMode, FilesystemProfile, Lifecycle, UnicodePolicy, VolumeConfig,
    VolumeConfigError, VolumeLimits,
};
use crate::storage::{ObjectId, ObjectKind};
use thiserror::Error;

const DOMAIN: &[u8] = b"acyclic-fs-volume-created-v1\0";
const VERSION: u16 = 1;


/// Fixed width of the canonical immutable configuration schema.
pub(crate) const CONFIG_ENCODED_BYTES: usize = 80;
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
    /// Finite preparation permit was exhausted.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Preparation was cancelled.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
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
    encode_volume_created_with_permit(value, WorkBudget::UNBOUNDED, None, 0)
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
}

/// Encodes the identical creation fact with admission before allocation and writes.
///
/// # Errors
/// Preserves actual completed work on validation, budget, allocation or cancellation failure.
pub fn encode_volume_created_measured(
    value: VolumeCreated,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    live_owned_bytes: u64,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, VolumeCreatedError> {
    encode_volume_created_with_permit(value, budget, Some(cancellation), live_owned_bytes)
}

fn encode_volume_created_with_permit(
    value: VolumeCreated,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
    live_owned_bytes: u64,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, VolumeCreatedError> {
    let mut work = WorkCounters::UNCHARGED;
    let mut allocations = AllocationLedger::default();
    let result = (|| {
        if let Some(token) = cancellation { token.check()?; }
        allocations.claim_bytes(live_owned_bytes, 0, &mut work, budget)
            .map_err(|error| error.into_work_or(VolumeCreatedError::Decode(CanonicalDecodeError::AllocationFailed)))?;
        encode_volume_created_on_ledger(value, &mut allocations, &mut work, budget, cancellation)
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

/// Funds the same canonical writer on a standalone or original native ledger.
/// Existing caller-owned inputs must already be retained on that ledger.
pub(crate) fn encode_volume_created_with_accounting(
    value: VolumeCreated,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let native_original = accounting.native_work().is_some();
    let mut encoding = EncodeAccounting::new(accounting);
    let result = encode_volume_created_with_steps(
        value,
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

fn validate_volume_created(value: VolumeCreated) -> Result<(), VolumeCreatedError> {
    value.config.validate()?;
    if value.initial_generation_root.kind != ObjectKind::GenerationRoot {
        return Err(invariant("initial volume object is not a generation root").into());
    }
    Ok(())
}

fn encode_volume_created_on_ledger(
    value: VolumeCreated,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<u8>, VolumeCreatedError> {
    let before = allocations.live_bytes();
    let result = encode_volume_created_with_steps(
        value,
        |step| {
            match step {
                EncodeWorkStep::Check => {
                    if let Some(token) = cancellation { token.check()?; }
                }
                EncodeWorkStep::Admit(delta) => work.admit(&delta, &budget)?,
                EncodeWorkStep::Completed(delta) => {
                    work.try_add_assign(&delta)?;
                    work.verify(budget)?;
                    if let Some(token) = cancellation { token.check()?; }
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::Reserve { capacity }) => {
                    allocations.claim_elements::<u8>(capacity, work, budget)
                        .map_err(|error| error.into_work_or(VolumeCreatedError::Decode(CanonicalDecodeError::AllocationFailed)))?;
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
                    if actual_bytes > admitted_bytes {
                        allocations.record_owned_bytes(actual_bytes - admitted_bytes, work)
                            .map_err(|error| error.into_work_or(VolumeCreatedError::Decode(CanonicalDecodeError::AllocationFailed)))?;
                    }
                    work.verify(budget)?;
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::BeforeHeader { bytes }) =>
                    work.admit(&WorkCounters { bytes_encoded: bytes, ..WorkCounters::UNCHARGED }, &budget)?,
                EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }) => {
                    work.try_add_assign(&WorkCounters { bytes_encoded: bytes, ..WorkCounters::UNCHARGED })?;
                    work.verify(budget)?;
                    if let Some(token) = cancellation { token.check()?; }
                }
            }
            Ok(())
        },
        |_| VolumeCreatedError::Decode(CanonicalDecodeError::AllocationFailed),
    );
    if result.is_err() {
        let owned = allocations.live_bytes().checked_sub(before).ok_or(crate::WorkError::Overflow)?;
        allocations.release(owned)
            .map_err(|error| error.into_work_or(VolumeCreatedError::Work(crate::WorkError::Overflow)))?;
    }
    result
}

pub(crate) fn encode_volume_created_with_steps<E>(
    value: VolumeCreated,
    mut observe: impl FnMut(EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError> + From<VolumeCreatedError>,
{
    observe(EncodeWorkStep::Check)?;
    validate_volume_created(value).map_err(E::from)?;
    let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    observe(EncodeWorkStep::Admit(item))?;
    observe(EncodeWorkStep::Completed(item))?;
    let length = DOMAIN.len() + 2 + 16 + CONFIG_ENCODED_BYTES + 1 + 32;
    observe(EncodeWorkStep::Admit(WorkCounters {
        bytes_encoded: length as u64, bytes_copied: 48, ..WorkCounters::UNCHARGED
    }))?;
    observe(EncodeWorkStep::Check)?;
    let mut encoder = Encoder::with_exact_capacity_with_steps(
        DOMAIN, VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)),
        allocation_error,
    )?;
    encoder.fixed(&value.volume_id.into_bytes());
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: 16, bytes_copied: 16, ..WorkCounters::UNCHARGED
    }))?;
    encode_config(&mut encoder, value.config);
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: CONFIG_ENCODED_BYTES as u64, ..WorkCounters::UNCHARGED
    }))?;
    encoder.u8(value.initial_generation_root.kind.canonical_tag());
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: 1, ..WorkCounters::UNCHARGED
    }))?;
    encoder.fixed(value.initial_generation_root.digest.as_bytes());
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: 32, bytes_copied: 32, ..WorkCounters::UNCHARGED
    }))?;
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
    decode_volume_created_with_steps(bytes, maximum_payload_bytes, &mut |_| Ok(()))
}

pub(crate) fn decode_volume_created_with_steps<E>(
    bytes: &[u8],
    maximum_payload_bytes: u64,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<VolumeCreated, E>
where
    E: From<CanonicalDecodeError> + From<VolumeCreatedError>,
{
    let mut decoder = Decoder::new_with_steps(bytes, DOMAIN, VERSION, maximum_payload_bytes, observe)?;
    let volume_id = VolumeId::from_bytes(decoder.fixed_with_steps::<16, E>(observe)?);
    let config = decode_config_with_steps(&mut decoder, observe)?.validate()
        .map_err(VolumeCreatedError::from).map_err(E::from)?;
    let kind = ObjectKind::from_canonical_tag(decoder.u8_with_steps(observe)?)
        .map_err(|_| invariant("unknown initial-generation object kind"))?;
    if kind != ObjectKind::GenerationRoot {
        return Err(invariant("initial volume object is not a generation root").into());
    }
    let initial_generation_root = ObjectId {
        kind,
        digest: Digest::from_bytes(decoder.fixed_with_steps::<32, E>(observe)?),
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
    decode_config_with_steps(decoder, &mut |_| Ok(()))
}

pub(crate) fn decode_config_with_steps<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<VolumeConfig, E> {
    let profile = match decoder.u8_with_steps(observe)? {
        1 => FilesystemProfile::Portable,
        2 => FilesystemProfile::Posix,
        3 => FilesystemProfile::Windows,
        4 => FilesystemProfile::Browser,
        _ => return Err(invariant("unknown filesystem profile").into()),
    };
    let concurrency = match decoder.u8_with_steps(observe)? {
        1 => ConcurrencyMode::ExclusiveWriter,
        2 => ConcurrencyMode::Optimistic,
        3 => ConcurrencyMode::SerializedAuthority,
        _ => return Err(invariant("unknown concurrency mode").into()),
    };
    let lifecycle = match decoder.u8_with_steps(observe)? {
        1 => Lifecycle::Ephemeral,
        2 => Lifecycle::Durable,
        _ => return Err(invariant("unknown lifecycle").into()),
    };
    let case_sensitivity = match decoder.u8_with_steps(observe)? {
        1 => CaseSensitivity::Sensitive,
        2 => CaseSensitivity::ProfileFolded,
        _ => return Err(invariant("unknown case-sensitivity mode").into()),
    };
    let unicode = match decoder.u8_with_steps(observe)? {
        1 => UnicodePolicy::Preserve,
        2 => UnicodePolicy::RequireNfc,
        _ => return Err(invariant("unknown Unicode policy").into()),
    };
    let symbolic_links = decode_bool(decoder.u8_with_steps(observe)?)?;
    let hard_links = decode_bool(decoder.u8_with_steps(observe)?)?;
    let sparse_files = decode_bool(decoder.u8_with_steps(observe)?)?;
    let maximum_path_bytes = decoder.u32_with_steps(observe)?;
    let maximum_component_bytes = decoder.u32_with_steps(observe)?;
    let maximum_path_depth =
        u16::try_from(decoder.u32_with_steps(observe)?).map_err(|_| invariant("maximum path depth exceeds u16"))?;
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
            maximum_object_bytes: decoder.u64_with_steps(observe)?,
            maximum_mutations_per_batch: decoder.u32_with_steps(observe)?,
            maximum_paths_per_batch: decoder.u32_with_steps(observe)?,
            maximum_checkout_dependencies: decoder.u32_with_steps(observe)?,
            maximum_directory_page_entries: decoder.u32_with_steps(observe)?,
            maximum_page_height: u16::try_from(decoder.u32_with_steps(observe)?)
                .map_err(|_| invariant("maximum page height exceeds u16"))?,
            maximum_read_bytes: decoder.u64_with_steps(observe)?,
            maximum_files_per_generation: decoder.u64_with_steps(observe)?,
            maximum_objects_per_generation: decoder.u64_with_steps(observe)?,
            maximum_generation_bytes: decoder.u64_with_steps(observe)?,
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

fn invariant(message: &'static str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static(message))
}

#[cfg(test)]
#[path = "tests/volume.rs"]
mod tests;
