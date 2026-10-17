use super::custody_atomics::{AtomicBool, AtomicU64, Ordering, fence};

pub(crate) fn retained(owners: usize, bytes: &AtomicU64) -> u64 {
    if owners == 1 {
        fence(Ordering::Acquire);
        0
    } else {
        bytes.load(Ordering::Acquire)
    }
}
pub(crate) fn publish(dirty: &AtomicBool) {
    // Every publisher must join the release sequence, even when already dirty;
    // a plain store could hide an earlier owner's retirement from the reader.
    dirty.fetch_or(true, Ordering::Release);
}
pub(crate) fn take_dirty(dirty: &AtomicBool) -> bool {
    dirty.load(Ordering::Acquire) && dirty.swap(false, Ordering::AcqRel)
}
pub(crate) fn retire(dirty: &AtomicBool, release_owner: impl FnOnce()) {
    // Uniqueness must be observable before cleanup is published. Reversing
    // these operations can strand the slot after the dirty flag is consumed.
    release_owner();
    publish(dirty);
}

#[cfg(any(feature = "native-watch", all(test, acyclic_fs_loom)))]
pub(crate) fn retire_bytes<const RELEASE_FIRST: bool>(
    dirty: &AtomicBool,
    retained: &AtomicU64,
    bytes: u64,
    floor: u64,
) -> Result<(), crate::WorkError> {
    let publish_first = !RELEASE_FIRST && cfg!(all(test, acyclic_fs_loom));
    if publish_first {
        publish(dirty);
    }
    retained
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current
                .checked_sub(bytes)
                .filter(|current| *current >= floor)
        })
        .map_err(|_| crate::WorkError::Overflow)?;
    if !publish_first {
        publish(dirty);
    }
    Ok(())
}
