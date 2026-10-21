use super::*;
#[cfg(feature = "filesystem")]
use std::mem::size_of;
pub(super) fn charge(work: &mut Option<&mut dyn HistoryIo>, items: u64) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
        io.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items })?;
    }
    Ok(())
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
#[cfg(feature = "filesystem")]
pub(super) fn sum(values: impl IntoIterator<Item = u64>) -> Result<u64> {
    values.into_iter().try_fold(0, add)
}
#[cfg(feature = "filesystem")]
pub(super) fn slots(count: usize, size: usize) -> Result<u64> {
    count
        .checked_mul(size)
        .map(|n| n as u64)
        .ok_or_else(overflow)
}
#[cfg(feature = "filesystem")]
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
/// Derives a capability for one exact resource and operation.
pub fn created_resource_capability(
    resource: &ResourceRef,
    operation: &str,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<String> {
    if operation.is_empty()
        || operation.len() as u64 > crate::COMPONENT_LABEL_MAX_BYTES as u64
        || operation.chars().any(char::is_control)
    {
        return Err(diagnostic(
            work,
            Invalid,
            "invalid created resource operation",
        ));
    }
    let digest = crate::executor::json::canonical_json_digest(
        &(resource, operation),
        crate::store::default_projection_read_limits().maximum_bytes,
        work,
    )?;
    crate::conversation::content_label("resource:created:", digest, work)
}
