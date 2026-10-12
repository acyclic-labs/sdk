//! Binary searches that report how many elements they probed.

use std::cmp::Ordering;

pub(crate) fn counted_binary_search<T>(
    items: &[T],
    compare: impl FnMut(&T) -> Ordering,
) -> (Result<usize, usize>, u64) {
    match binary_search_engine(items, |_| Ok::<_, std::convert::Infallible>(()), compare) {
        Ok(result) => result,
        Err(error) => match error {},
    }
}

enum ProbePhase { Before, Completed }

fn binary_search_engine<T, E>(
    items: &[T],
    mut probe: impl FnMut(ProbePhase) -> Result<(), E>,
    mut compare: impl FnMut(&T) -> Ordering,
) -> Result<(Result<usize, usize>, u64), E> {
    let (mut base, mut rest, mut probes) = (0_usize, items, 0_u64);
    while let Some((lower, upper)) = rest.split_at_checked(rest.len() / 2)
        && let Some((middle, higher)) = upper.split_first()
    {
        probe(ProbePhase::Before)?;
        let ordering = compare(middle);
        probe(ProbePhase::Completed)?;
        probes += 1;
        match ordering {
            Ordering::Less => {
                base += lower.len() + 1;
                rest = higher;
            }
            Ordering::Greater => rest = lower,
            Ordering::Equal => return Ok((Ok(base + lower.len()), probes)),
        }
    }
    Ok((Err(base), probes))
}

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

pub(crate) fn binary_search_with_accounting<T>(
    items: &[T],
    accounting: &mut super::allocation::KernelAccounting<'_>,
    compare: impl FnMut(&T) -> Ordering,
) -> Result<Result<usize, usize>, crate::FsError> {
    binary_search_engine(items, |phase| {
        let delta = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
        match phase {
            ProbePhase::Before => accounting.admit(delta),
            ProbePhase::Completed => accounting.record_completed(delta),
        }
    }, compare).map(|(result, _)| result)
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
