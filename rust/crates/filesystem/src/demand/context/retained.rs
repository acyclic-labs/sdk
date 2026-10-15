use super::{AtomicU64, Ordering};

pub(super) fn take(retained: &AtomicU64) -> u64 {
    retained.swap(0, Ordering::AcqRel)
}

pub(super) fn transfer(retained: &AtomicU64, bytes: u64) -> Result<(), crate::WorkError> {
    retained
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_sub(bytes)
        })
        .map(|_| ())
        .map_err(|_| crate::WorkError::Overflow)
}

pub(super) fn claim(retained: &AtomicU64, bytes: u64) {
    retained.fetch_add(bytes, Ordering::AcqRel);
}

pub(super) fn release(retained: &AtomicU64, bytes: u64) {
    let _ = retained.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_sub(bytes))
    });
}
