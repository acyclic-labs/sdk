use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::store) fn release_owned_after_drop<T>(
    result: Result<T>,
    bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<T> {
    let released = match work.as_deref_mut() {
        Some(observer) => observer.release_owned(bytes),
        None => Ok(()),
    };
    match result {
        Ok(value) => released.map(|()| value),
        Err(error) => Err(error),
    }
}

/// Freezes a producer-retained vector without replacing its original claim.
#[doc(hidden)]
pub fn owned_bytes_from_retained_vec(
    bytes: Vec<u8>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(OwnedBytes, u64)> {
    let capacity = bytes.capacity() as u64;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(observer) = work.as_deref_mut()
        && let Some(original) = observer.original_native_work().cloned()
    {
        let custody = match observer.take_native_custody(capacity) {
            Ok(custody) => custody,
            Err(error) => {
                drop(bytes);
                return release_owned_after_drop(Err(error), capacity, work);
            }
        };
        let mut bytes = bytes;
        let mut custody = Some(custody);
        return OwnedBytes::from_vec_with_guard_in_place(
            &mut bytes,
            &mut custody,
            &original.runtime_meter(),
        )
        .map(|bytes| (bytes, 0))
        .map_err(|source| Error::from(original.runtime_error(source)));
    }
    Ok((
        OwnedBytes::from(bytes),
        if work.is_some() { capacity } else { 0 },
    ))
}
