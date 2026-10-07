//! Binary searches that report how many elements they probed.
//!
//! Work receipts charge every probe, so the probe sequence is part of the
//! observable contract: each search always probes the midpoint of the range
//! still in question, and the count depends only on the slice and predicate.

use std::cmp::Ordering;

/// [`slice::binary_search_by`] that also returns how many elements it probed.
///
/// With repeated matches it may return a different matching index than the
/// standard library, which reserves the right to change its probe order.
pub(crate) fn counted_binary_search<T>(
    items: &[T],
    mut compare: impl FnMut(&T) -> Ordering,
) -> (Result<usize, usize>, u64) {
    let (mut base, mut rest, mut probes) = (0_usize, items, 0_u64);
    while let Some((lower, upper)) = rest.split_at_checked(rest.len() / 2)
        && let Some((middle, higher)) = upper.split_first()
    {
        probes += 1;
        match compare(middle) {
            Ordering::Less => {
                base += lower.len() + 1;
                rest = higher;
            }
            Ordering::Greater => rest = lower,
            Ordering::Equal => return (Ok(base + lower.len()), probes),
        }
    }
    (Err(base), probes)
}

/// [`slice::partition_point`] that also returns how many elements it probed,
/// probing exactly as [`counted_binary_search`] does.
pub(crate) fn counted_partition_point<T>(
    items: &[T],
    mut before: impl FnMut(&T) -> bool,
) -> (usize, u64) {
    let (Ok(index) | Err(index), probes) = counted_binary_search(items, |item| {
        if before(item) {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    });
    (index, probes)
}

#[cfg(test)]
#[path = "tests/search.rs"]
mod tests;
