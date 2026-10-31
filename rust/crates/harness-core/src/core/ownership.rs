//! Logical ownership of the main reducer's records.
use super::*;
mod walk;
#[cfg(feature = "filesystem")]
pub(crate) use walk::tree_nodes;
pub(crate) use walk::{OwnedBytes, owned_enum, owned_struct, owned_tuple};
#[cfg(test)]
mod tests;

pub(crate) fn owned_sum(parts: impl IntoIterator<Item = u64>) -> Result<u64> {
    parts.into_iter().try_fold(0_u64, |sum, bytes| {
        sum.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("component ownership overflows".into()))
    })
}
pub(crate) fn owned_buffer<T>(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .map(|bytes| bytes as u64)
        .ok_or_else(|| Error::Invalid("component buffer ownership overflows".into()))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn owned_vector<T>(
    values: &Vec<T>,
    mut item: impl FnMut(&T) -> Result<u64>,
) -> Result<u64> {
    values
        .iter()
        .try_fold(owned_buffer::<T>(values.capacity())?, |sum, value| {
            owned_sum([sum, item(value)?])
        })
}

pub(crate) fn inspect_metadata<T: Serialize + ?Sized>(
    source: &T,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
        let io = crate::executor::PreparationIo::from_native_work(&native.recheck_admitted())?;
        return crate::executor::with_history_io(Some(&io), |work| {
            crate::executor::json::json_byte_length(source, u64::MAX, work).map(|_| ())
        });
    }
    if work.is_none() {
        return Ok(());
    }
    crate::executor::json::json_byte_length(source, u64::MAX, work).map(|_| ())
}

#[cfg(feature = "filesystem")]
pub(crate) use walk::admit_tree_storage;
