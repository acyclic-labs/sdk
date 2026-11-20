/// Map an original monotonic offset through the anchor's midpoint without
/// rejecting representable timestamps because of an overflowing intermediate.
pub(super) fn project_offset(observed: u128, window: u128, offset: u128) -> Option<u128> {
    let midpoint = window / 2;
    if offset >= midpoint {
        observed.checked_add(offset - midpoint)
    } else {
        observed.checked_sub(midpoint - offset)
    }
}
