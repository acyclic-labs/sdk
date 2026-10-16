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
    mut compare: impl FnMut(&T) -> Ordering,
) -> Result<Result<usize, usize>, crate::FsError> {
    let (mut base, mut rest) = (0_usize, items);
    while let Some((lower, upper)) = rest.split_at_checked(rest.len() / 2)
        && let Some((middle, higher)) = upper.split_first()
    {
        match examine(middle, accounting, &mut compare)? {
            Ordering::Less => {
                base += lower.len() + 1;
                rest = higher;
            }
            Ordering::Greater => rest = lower,
            Ordering::Equal => return Ok(Ok(base + lower.len())),
        }
    }
    Ok(Err(base))
}

/// Searches an ordered suffix, taking one probe for its first item and
/// logarithmically many in the distance to a later matching/insertion slot.
pub(crate) fn galloping_search_with_accounting<T>(
    items: &[T],
    accounting: &mut super::allocation::KernelAccounting<'_>,
    mut compare: impl FnMut(&T) -> Ordering,
) -> Result<Result<usize, usize>, crate::FsError> {
    if items.is_empty() {
        // Knowing the insertion slot avoids comparisons, while the caller's
        // mutation still belongs to an admitted, uncancelled invocation.
        accounting.check()?;
        return Ok(Err(0));
    }
    let (mut lower, mut upper) = (0, 0);
    while let Some(item) = items.get(upper) {
        match examine(item, accounting, &mut compare)? {
            Ordering::Equal => return Ok(Ok(upper)),
            Ordering::Greater => break,
            Ordering::Less => {
                lower = upper + 1;
                upper = lower.saturating_mul(2).saturating_sub(1);
            }
        }
    }
    let rest = items
        .get(lower..upper.min(items.len()))
        .ok_or(crate::FsError::InvalidDiff)?;
    Ok(binary_search_with_accounting(rest, accounting, compare)?
        .map(|index| lower + index)
        .map_err(|index| lower + index))
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
