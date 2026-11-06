use super::{AtomicU64, Ordering, fence};

#[derive(Debug, Default)]
pub(crate) struct NativeAllocationAccounting {
    pub(super) retired: AtomicU64,
    pub(super) non_native: AtomicU64,
}
impl NativeAllocationAccounting {
    pub(super) const ALLOCATION_BYTES: u64 = const {
        match crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Self>()) {
            Ok(bytes) => bytes as u64,
            Err(_) => panic!("shared native accounting Arc layout overflow"),
        }
    };

    pub(crate) fn retire_native(&self, bytes: u64) {
        self.retired.fetch_add(bytes, Ordering::Release);
    }
    pub(crate) fn retain_non_native(&self, bytes: u64) {
        self.non_native.fetch_add(bytes, Ordering::Release);
    }
    pub(crate) fn retire_non_native(&self, bytes: u64) {
        self.non_native.fetch_sub(bytes, Ordering::Release);
    }
    pub(super) fn take_retired(&self) -> u64 {
        if self.retired.load(Ordering::Acquire) == 0 {
            0
        } else {
            self.retired.swap(0, Ordering::AcqRel)
        }
    }
    pub(super) fn drain_for_final_owner(
        &self,
        owners: usize,
        paid_non_native: u64,
    ) -> Result<Option<u64>, crate::WorkError> {
        if owners != 1 {
            return Ok(None);
        }
        // Arc's relaxed strong-count observation needs an acquire fence before
        // draining updates published before the last publisher's release decrement.
        fence(Ordering::Acquire);
        Self::ALLOCATION_BYTES
            .checked_add(paid_non_native)
            .and_then(|bytes| bytes.checked_add(self.take_retired()))
            .map(Some)
            .ok_or(crate::WorkError::Overflow)
    }
}
