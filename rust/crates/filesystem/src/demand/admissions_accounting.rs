use super::{AtomicU64, Ordering, fence};

#[derive(Debug, Default)]
pub(crate) struct NativeProofAccounting {
    pub(super) retired: AtomicU64,
    pub(super) non_native: AtomicU64,
}
impl NativeProofAccounting {
    pub(super) const ALLOCATION_BYTES: u64 = const {
        match crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Self>()) {
            Ok(bytes) => bytes as u64,
            Err(_) => panic!("shared proof accounting Arc layout overflow"),
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
}

pub(super) fn acquire_final_owner(owners: usize) -> bool {
    if owners != 1 {
        return false;
    }
    // Arc's relaxed strong-count observation needs an acquire fence before
    // draining updates published before the last publisher's release decrement.
    fence(Ordering::Acquire);
    true
}
