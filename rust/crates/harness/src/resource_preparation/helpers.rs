use super::*;
use crate::conversation::{ContentResidencyVerifier, FileRef};
use std::{collections::BTreeSet, mem::size_of, sync::Arc};
pub(super) fn charge(work: &mut Option<&mut dyn HistoryIo>, items: u64) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items })?;
    }
    Ok(())
}
pub(super) fn denied(work: &mut Option<&mut dyn HistoryIo>) -> Error {
    diagnostic(work, Unauthorized, "created access denied")
}
pub(super) fn overflow() -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        Error::NativeBoundary {
            code: Invalid,
            reason: "created access ownership overflows",
        }
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        Error::Invalid("created access ownership overflows".into())
    }
}
pub(super) fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or_else(overflow)
}
pub(super) fn sum(values: impl IntoIterator<Item = u64>) -> Result<u64> {
    values.into_iter().try_fold(0, add)
}
pub(super) fn slots(count: usize, size: usize) -> Result<u64> {
    count
        .checked_mul(size)
        .map(|n| n as u64)
        .ok_or_else(overflow)
}
pub(super) fn vector_bytes<T>(
    values: &Vec<T>,
    mut bytes: impl FnMut(&T) -> Result<u64>,
) -> Result<u64> {
    values
        .iter()
        .try_fold(slots(values.capacity(), size_of::<T>())?, |n, value| {
            add(n, bytes(value)?)
        })
}
pub(super) fn release(work: &mut Option<&mut dyn HistoryIo>, bytes: u64) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.release_owned(bytes)?;
    }
    Ok(())
}
pub(super) fn reserve<T>(
    values: &mut Vec<T>,
    count: usize,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if count <= values.capacity() {
        return Ok(());
    }
    let old = values.capacity();
    if let Some(io) = work.as_deref_mut() {
        io.admit_allocation(slots(count - old, size_of::<T>())?)?;
    }
    values
        .try_reserve_exact(count - values.len())
        .map_err(|error| match work.as_deref() {
            Some(io) => io.allocation_failure(error),
            None => Error::Storage(error.to_string()),
        })?;
    if let Some(io) = work.as_deref_mut() {
        io.retain_owned(slots(values.capacity() - old, size_of::<T>())?)?;
    }
    Ok(())
}
pub(super) fn insert_label(
    labels: &mut BTreeSet<String>,
    label: String,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if labels.contains(&label) {
        let bytes = label.capacity() as u64;
        drop(label);
        return release(work, bytes);
    }
    if let Some(io) = work.as_deref_mut() {
        io.retain_owned(size_of::<String>() as u64)?;
    }
    labels.insert(label);
    Ok(())
}
pub(super) fn proof_bytes(proof: &ResourceCreationProofRef) -> Result<u64> {
    sum([
        proof.provider.logical_owned_bytes()?,
        proof.resource.logical_owned_bytes()?,
        proof.evidence.logical_owned_bytes()?,
    ])
}
pub(super) fn receipt_bytes(receipt: &CreatedResourceReceipt) -> Result<u64> {
    sum([
        receipt.origin.owner.id.capacity() as u64,
        receipt.origin.source_grants.logical_owned_bytes()?,
        receipt.origin.creation_fence.reservation_id.capacity() as u64,
        receipt.origin.creation_fence.placement.capacity() as u64,
        receipt.resource.logical_owned_bytes()?,
        receipt.evidence.logical_owned_bytes()?,
        receipt.allocation_generation.logical_owned_bytes()?,
        vector_bytes(&receipt.operations, |name| Ok(name.capacity() as u64))?,
        slots(
            receipt.content.capacity(),
            size_of::<CreatedContentBinding>(),
        )?,
        slots(
            receipt.references.capacity(),
            size_of::<(FileRef, Arc<dyn ContentResidencyVerifier>)>(),
        )?,
    ])
}
/// Derives a capability for one exact resource and operation.
pub fn created_resource_capability(
    resource: &ResourceRef,
    operation: &str,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<String> {
    if operation.is_empty()
        || operation.len() > crate::COMPONENT_LABEL_MAX_BYTES
        || operation.chars().any(char::is_control)
    {
        return Err(diagnostic(
            work,
            Invalid,
            "invalid created resource operation",
        ));
    }
    let digest = crate::contract::canonical_json_digest(
        &(resource, operation),
        crate::store::default_projection_read_limits().maximum_bytes,
        work,
    )?;
    crate::conversation::content_label("resource:created:", digest, work)
}
