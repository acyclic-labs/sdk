//! Binary searches with before-effect probe admission.

use std::cmp::Ordering;

pub(crate) fn binary_search_with_accounting<T>(
    items: &[T],
    accounting: &mut super::allocation::KernelAccounting<'_>,
    compare: impl FnMut(&T) -> Ordering,
) -> Result<Result<usize, usize>, crate::FsError> {
    binary_search_from_with_accounting(items, 0, accounting, compare)
}

/// Follows the ordinary whole-slice binary-search decision tree. Slots before
/// `known_before` are already known to compare Less, so those comparisons can
/// be inferred. Every actual probe is a subsequence of the baseline probes.
pub(crate) fn binary_search_from_with_accounting<T>(
    items: &[T],
    known_before: usize,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    mut compare: impl FnMut(&T) -> Ordering,
) -> Result<Result<usize, usize>, crate::FsError> {
    binary_search_indexed_with_accounting(items.len(), known_before, accounting, |index| {
        items
            .get(index)
            .map(&mut compare)
            .ok_or(crate::FsError::InvalidDiff)
    })
}

/// The same probe plan for any rank-addressable collection. Navigation to a
/// rank examines integer topology only; payload comparison follows admission.
pub(crate) fn binary_search_indexed_with_accounting(
    len: usize,
    known_before: usize,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    mut compare: impl FnMut(usize) -> Result<Ordering, crate::FsError>,
) -> Result<Result<usize, usize>, crate::FsError> {
    if known_before > len {
        return Err(crate::FsError::InvalidDiff);
    }
    if known_before == len {
        accounting.check()?;
        return Ok(Err(known_before));
    }
    let (mut base, mut end) = (0_usize, len);
    let delta = crate::WorkCounters {
        items_examined: 1,
        ..crate::WorkCounters::UNCHARGED
    };
    while base < end {
        let index = base + (end - base) / 2;
        let ordering = if index < known_before {
            Ordering::Less
        } else {
            accounting.admit(delta)?;
            let ordering = compare(index)?;
            accounting.record_completed(delta)?;
            ordering
        };
        match ordering {
            Ordering::Less => base = index + 1,
            Ordering::Greater => end = index,
            Ordering::Equal => return Ok(Ok(index)),
        }
    }
    Ok(Err(base))
}

pub(crate) fn partition_point_with_accounting<T>(
    items: &[T],
    accounting: &mut super::allocation::KernelAccounting<'_>,
    mut before: impl FnMut(&T) -> bool,
) -> Result<usize, crate::FsError> {
    let (Ok(index) | Err(index)) = binary_search_with_accounting(items, accounting, |item| {
        if before(item) {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    })?;
    Ok(index)
}

#[cfg(test)]
#[path = "tests/search.rs"]
mod tests;
