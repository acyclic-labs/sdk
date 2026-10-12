//! Binary searches with before-effect probe admission.

use std::cmp::Ordering;

pub(crate) fn binary_search_with_accounting<T>(
    items: &[T],
    accounting: &mut super::allocation::KernelAccounting<'_>,
    mut compare: impl FnMut(&T) -> Ordering,
) -> Result<Result<usize, usize>, crate::FsError> {
    let (mut base, mut rest) = (0_usize, items);
    let delta = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
    while let Some((lower, upper)) = rest.split_at_checked(rest.len() / 2)
        && let Some((middle, higher)) = upper.split_first()
    {
        accounting.admit(delta)?;
        let ordering = compare(middle);
        accounting.record_completed(delta)?;
        match ordering {
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

pub(crate) fn partition_point_with_accounting<T>(
    items: &[T],
    accounting: &mut super::allocation::KernelAccounting<'_>,
    mut before: impl FnMut(&T) -> bool,
) -> Result<usize, crate::FsError> {
    let (Ok(index) | Err(index)) = binary_search_with_accounting(items, accounting, |item| {
        if before(item) { Ordering::Less } else { Ordering::Greater }
    })?;
    Ok(index)
}

#[cfg(test)]
#[path = "tests/search.rs"]
mod tests;
