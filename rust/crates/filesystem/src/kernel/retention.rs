//! Canonical independently addressable generation-retention facts.

use super::codec::{CanonicalDecodeError, Decoder, Encoder, EncodeAccounting, EncodeAllocationStep, EncodeWorkStep};
use super::allocation::{AllocationLedger, KernelAccounting, KernelOwnedAllocation};
use crate::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use super::volume::{CONFIG_ENCODED_BYTES, decode_config, encode_config};
use crate::foundation::{AuthorityId, Digest, VolumeId, usize_to_u64};
use crate::model::{VolumeConfig, VolumeConfigError};
use crate::storage::{ObjectId, ObjectKind};
use thiserror::Error;

const DOMAIN: &[u8] = b"acyclic-fs-retention-created-v1\0";
const ID_DOMAIN: &[u8] = b"acyclic-fs-retention-authority-v1\0";
const DELETED_DOMAIN: &[u8] = b"acyclic-fs-workspace-deleted-v1\0";
const VERSION: u16 = 1;
const MAXIMUM_LABEL_BYTES: u32 = 255;

/// Why one immutable generation remains durably reachable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionKind {
    /// Human-readable retained workspace checkpoint.
    Checkpoint,
    /// Explicit customer generation pin.
    Pin,
    /// Internal exact ancestry root retained by an independently live fork.
    ForkBase,
}

impl RetentionKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Checkpoint => 1,
            Self::Pin => 2,
            Self::ForkBase => 3,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, CanonicalDecodeError> {
        match tag {
            1 => Ok(Self::Checkpoint),
            2 => Ok(Self::Pin),
            3 => Ok(Self::ForkBase),
            _ => Err(invariant("unknown retention kind")),
        }
    }
}

/// Immutable creation fact for one retained generation root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionCreated {
    /// Workspace that owns the retained label or pin.
    pub volume_id: VolumeId,
    /// Retention class.
    pub kind: RetentionKind,
    /// Canonical checkpoint label or opaque pin identity.
    pub label: String,
    /// Exact retained generation root.
    pub generation_root: ObjectId,
    /// Filesystem semantics needed to authenticate and reclaim the closure.
    pub config: VolumeConfig,
}

/// Canonical retention codec failure.
#[derive(Debug, Error)]
pub enum RetentionCreatedError {
    /// Retention label is absent, oversized, or contains a forbidden scalar.
    #[error("retention label is invalid")]
    InvalidLabel,
    /// Retained object is not a generation root.
    #[error("retention target is not a generation root")]
    WrongObjectKind,
    /// Volume configuration is invalid.
    #[error(transparent)]
    Config(#[from] VolumeConfigError),
    /// Canonical payload is malformed or unsupported.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Finite preparation permit was exhausted.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Preparation was cancelled.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
}

/// Derives the one authority owning a workspace retention name.
#[must_use]
pub fn retention_authority_id(
    volume_id: VolumeId,
    kind: RetentionKind,
    label: &str,
) -> AuthorityId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ID_DOMAIN);
    hasher.update(&volume_id.into_bytes());
    hasher.update(&[kind.tag()]);
    hasher.update(&usize_to_u64(label.len()).to_le_bytes());
    hasher.update(label.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    AuthorityId::from_bytes(bytes)
}

/// Encodes one exact immutable retention fact.
///
/// # Errors
///
/// Rejects invalid labels, configurations, object kinds, or allocation bounds.
pub fn encode_retention_created(
    value: &RetentionCreated,
) -> Result<Vec<u8>, RetentionCreatedError> {
    let mut work = WorkCounters::UNCHARGED;
    let mut allocations = AllocationLedger::default();
    validate(value)?;
    allocations.claim_bytes(0, 0, &mut work, WorkBudget::UNBOUNDED)
        .map_err(|error| error.into_work_or(RetentionCreatedError::Decode(CanonicalDecodeError::AllocationFailed)))?;
    encode_retention_created_on_ledger(
        value, &mut allocations, &mut work, WorkBudget::UNBOUNDED, None,
    )
}

/// Funds the identical retention writer on the original accounting ledger.
/// The transferred owned label must already be retained by the caller.
pub(crate) fn encode_retention_created_with_accounting(
    value: &RetentionCreated,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    validate(value).map_err(retention_created_fs_error)?;
    #[cfg(not(target_arch = "wasm32"))]
    let native_original = accounting.native_work().is_some();
    let mut encoding = EncodeAccounting::new(accounting);
    let result = encode_retention_created_with_steps(
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

fn retention_created_fs_error(error: RetentionCreatedError) -> crate::FsError {
    match error {
        RetentionCreatedError::Work(error) => crate::FsError::Work(error),
        RetentionCreatedError::Cancelled(error) => crate::FsError::Cancelled(error),
        error => error.into(),
    }
}

fn encode_retention_created_on_ledger(
    value: &RetentionCreated,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<u8>, RetentionCreatedError> {
    let before = allocations.live_bytes();
    let result = encode_retention_created_with_steps(
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
                        .map_err(|error| error.into_work_or(RetentionCreatedError::Decode(CanonicalDecodeError::AllocationFailed)))?;
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
                    if actual_bytes > admitted_bytes {
                        allocations.record_owned_bytes(actual_bytes - admitted_bytes, work)
                            .map_err(|error| error.into_work_or(RetentionCreatedError::Decode(CanonicalDecodeError::AllocationFailed)))?;
                    } else if actual_bytes < admitted_bytes {
                        allocations.release(admitted_bytes - actual_bytes)
                            .map_err(|error| error.into_work_or(RetentionCreatedError::Decode(CanonicalDecodeError::AllocationFailed)))?;
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
        |_| RetentionCreatedError::Decode(CanonicalDecodeError::AllocationFailed),
    );
    if result.is_err() {
        let owned = allocations.live_bytes().checked_sub(before).ok_or(crate::WorkError::Overflow)?;
        allocations.release(owned)
            .map_err(|error| error.into_work_or(RetentionCreatedError::Decode(CanonicalDecodeError::AllocationFailed)))?;
    }
    result
}

fn encode_retention_created_with_steps<E: From<CanonicalDecodeError>>(
    value: &RetentionCreated,
    mut observe: impl FnMut(EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E> {
    observe(EncodeWorkStep::Check)?;
    let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    observe(EncodeWorkStep::Admit(item))?;
    observe(EncodeWorkStep::Completed(item))?;
    let length = DOMAIN.len() + 2 + 16 + 1 + 4 + value.label.len() + 1 + 32 + CONFIG_ENCODED_BYTES;
    let mut encoder = Encoder::with_exact_capacity_with_steps(
        DOMAIN, VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)), allocation_error,
    )?;
    let fields = WorkCounters {
        bytes_encoded: (16 + 1 + 4 + value.label.len() + 1 + 32) as u64,
        bytes_copied: (48 + value.label.len()) as u64,
        ..WorkCounters::UNCHARGED
    };
    observe(EncodeWorkStep::Admit(fields))?;
    encoder.fixed(&value.volume_id.into_bytes());
    encoder.u8(value.kind.tag());
    encoder.bounded_bytes(value.label.as_bytes())?;
    encoder.u8(value.generation_root.kind.canonical_tag());
    encoder.fixed(value.generation_root.digest.as_bytes());
    observe(EncodeWorkStep::Completed(fields))?;
    let config = WorkCounters {
        bytes_encoded: CONFIG_ENCODED_BYTES as u64, ..WorkCounters::UNCHARGED
    };
    observe(EncodeWorkStep::Admit(config))?;
    encode_config(&mut encoder, value.config);
    observe(EncodeWorkStep::Completed(config))?;
    Ok(encoder.finish())
}

/// Decodes one exact bounded immutable retention fact.
///
/// # Errors
///
/// Rejects malformed, oversized, trailing, unsupported, or semantically invalid payloads.
pub fn decode_retention_created(
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<RetentionCreated, RetentionCreatedError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, maximum_payload_bytes)?;
    let volume_id = VolumeId::from_bytes(decoder.fixed()?);
    let kind = RetentionKind::from_tag(decoder.u8()?)?;
    let label = String::from_utf8(decoder.bounded_bytes(MAXIMUM_LABEL_BYTES)?)
        .map_err(|_| RetentionCreatedError::InvalidLabel)?;
    let object_kind = ObjectKind::from_canonical_tag(decoder.u8()?)
        .map_err(|_| RetentionCreatedError::WrongObjectKind)?;
    let generation_root = ObjectId {
        kind: object_kind,
        digest: Digest::from_bytes(decoder.fixed()?),
    };
    let config = decode_config(&mut decoder)?.validate()?;
    decoder.finish()?;
    let value = RetentionCreated {
        volume_id,
        kind,
        label,
        generation_root,
        config,
    };
    validate(&value)?;
    Ok(value)
}

/// Encodes the terminal deletion fact for one workspace authority.
#[must_use]
pub fn encode_workspace_deleted(volume_id: VolumeId) -> Vec<u8> {
    encode_workspace_deleted_with_permit(volume_id, WorkBudget::UNBOUNDED, None)
        .map(|receipt| receipt.value)
        .expect("unbounded terminal deletion encoding allocation failed")
}

/// Encodes the same terminal deletion record with finite producer admission.
pub fn encode_workspace_deleted_measured(
    volume_id: VolumeId,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, RetentionCreatedError> {
    encode_workspace_deleted_with_permit(volume_id, budget, Some(cancellation))
}

fn encode_workspace_deleted_with_permit(
    volume_id: VolumeId,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, RetentionCreatedError> {
    let mut work = WorkCounters::UNCHARGED;
    let mut allocations = AllocationLedger::default();
    let result = (|| {
        if let Some(token) = cancellation { token.check()?; }
        work.charge_items(1, &budget)?;
        let length = DELETED_DOMAIN.len() + 2 + 16;
        work.admit(&WorkCounters { bytes_encoded: length as u64, bytes_copied: 16, ..WorkCounters::UNCHARGED }, &budget)?;
        let admitted = allocations.claim_elements::<u8>(length, &mut work, budget)
            .map_err(|error| error.into_work_or(RetentionCreatedError::Decode(CanonicalDecodeError::AllocationFailed)))?;
        let mut encoder = Encoder::with_exact_capacity(DELETED_DOMAIN, VERSION, length)?;
        let header = (DELETED_DOMAIN.len() + 2) as u64;
        work.charge(&WorkCounters { bytes_encoded: header, ..WorkCounters::UNCHARGED }, &budget)?;
        let actual = encoder.capacity() as u64;
        if actual > admitted {
            allocations.claim_bytes(actual - admitted, 0, &mut work, WorkBudget::UNBOUNDED)
                .map_err(|error| error.into_work_or(RetentionCreatedError::Decode(CanonicalDecodeError::AllocationFailed)))?;
            work.verify(budget)?;
        }
        if let Some(token) = cancellation { token.check()?; }
        encoder.fixed(&volume_id.into_bytes());
        work.charge(&WorkCounters { bytes_encoded: 16, bytes_copied: 16, ..WorkCounters::UNCHARGED }, &budget)?;
        Ok(encoder.finish())
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

/// Decodes one terminal workspace deletion fact.
///
/// # Errors
///
/// Rejects another domain/version, truncation, trailing bytes, or an
/// oversized payload before returning a workspace identity.
pub fn decode_workspace_deleted(
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<VolumeId, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DELETED_DOMAIN, VERSION, maximum_payload_bytes)?;
    let volume_id = VolumeId::from_bytes(decoder.fixed()?);
    decoder.finish()?;
    Ok(volume_id)
}

fn validate(value: &RetentionCreated) -> Result<(), RetentionCreatedError> {
    value.config.validate()?;
    if value.generation_root.kind != ObjectKind::GenerationRoot {
        return Err(RetentionCreatedError::WrongObjectKind);
    }
    if value.label.is_empty()
        || u32::try_from(value.label.len()).unwrap_or(u32::MAX) > MAXIMUM_LABEL_BYTES
        || value
            .label
            .chars()
            .any(|character| character.is_control() || matches!(character, '/' | '\\'))
    {
        return Err(RetentionCreatedError::InvalidLabel);
    }
    Ok(())
}

fn invariant(message: &'static str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static(message))
}

#[cfg(test)]
#[path = "tests/retention.rs"]
mod tests;
