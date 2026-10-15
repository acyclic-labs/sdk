use super::custody_atomics::{AtomicBool, AtomicU64, Ordering, fence};

pub(crate) fn retained(owners: usize, bytes: &AtomicU64) -> u64 {
    if owners == 1 {
        fence(Ordering::Acquire);
        0
    } else {
        bytes.load(Ordering::Acquire)
    }
}
pub(crate) fn retire(dirty: &AtomicBool, release_owner: impl FnOnce()) {
    // Uniqueness must be observable before cleanup is published. Reversing
    // these operations can strand the slot after the dirty flag is consumed.
    release_owner();
    dirty.store(true, Ordering::Release);
}
