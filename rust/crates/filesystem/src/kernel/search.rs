//! Binary searches with before-effect probe admission.

use std::cmp::Ordering;

fn examine<T>(
    item: &T,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    compare: &mut impl FnMut(&T) -> Ordering,
) -> Result<Ordering, crate::FsError> {
    let delta = crate::WorkCounters {
        items_examined: 1,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(delta)?;
    let ordering = compare(item);
    accounting.record_completed(delta)?;
    Ok(ordering)
}

pub(crate) fn binary_search_with_accounting<T>(
    items: &[T],
    accounting: &mut super::allocation::KernelAccounting<'_>,
    compare: impl FnMut(&T) -> Ordering,
) -> Result<Result<usize, usize>, crate::FsError> {
    if items.is_empty() {
        return Ok(Err(0));
    }
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
    if known_before > items.len() {
        return Err(crate::FsError::InvalidDiff);
    }
    if known_before == items.len() {
        // An inferred append still belongs to an admitted invocation.
        accounting.check()?;
        return Ok(Err(known_before));
    }
    let (mut base, mut rest) = (0_usize, items);
    while let Some((lower, upper)) = rest.split_at_checked(rest.len() / 2)
        && let Some((middle, higher)) = upper.split_first()
    {
        let index = base + lower.len();
        let ordering = if index < known_before {
            Ordering::Less
        } else {
            examine(middle, accounting, &mut compare)?
        };
        match ordering {
            Ordering::Less => {
                base = index + 1;
                rest = higher;
            }
            Ordering::Greater => rest = lower,
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
