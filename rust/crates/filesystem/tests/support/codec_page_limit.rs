pub(super) fn fewer(count: usize) -> Option<u32> {
    u32::try_from(count).ok()?.checked_sub(1)
}
