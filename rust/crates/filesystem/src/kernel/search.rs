//! Binary searches with before-effect probe admission.

use std::cmp::Ordering;

struct OriginalProbes<'a, 'b, C> {
    accounting: &'a mut super::allocation::KernelAccounting<'b>,
    compare: C,
}

impl<T, C: FnMut(&T) -> Ordering> acyclic_collections::page::search::Work<T>
    for OriginalProbes<'_, '_, C>
{
    type Error = crate::FsError;

    fn check(&mut self) -> Result<(), Self::Error> {
        self.accounting.check()
    }

    fn compare(&mut self, item: &T) -> Result<Ordering, Self::Error> {
        let delta = crate::WorkCounters {
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        };
        self.accounting.admit(delta)?;
        let ordering = (self.compare)(item);
        self.accounting.record_completed(delta)?;
        Ok(ordering)
    }

    fn invalid_prefix(&self) -> Self::Error {
        crate::FsError::InvalidDiff
    }
}

pub(crate) fn binary_search_with_accounting<T>(
    items: &[T],
    accounting: &mut super::allocation::KernelAccounting<'_>,
    compare: impl FnMut(&T) -> Ordering,
) -> Result<Result<usize, usize>, crate::FsError> {
    acyclic_collections::page::search::binary_search(
        items,
        &mut OriginalProbes {
            accounting,
            compare,
        },
    )
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
    compare: impl FnMut(usize) -> Result<Ordering, crate::FsError>,
) -> Result<Result<usize, usize>, crate::FsError> {
    struct Indexed<'a, 'b, C> {
        accounting: &'a mut super::allocation::KernelAccounting<'b>,
        compare: C,
    }
    impl<C: FnMut(usize) -> Result<Ordering, crate::FsError>>
        acyclic_collections::page::search::Work<usize> for Indexed<'_, '_, C>
    {
        type Error = crate::FsError;
        fn check(&mut self) -> Result<(), Self::Error> {
            self.accounting.check()
        }
        fn invalid_prefix(&self) -> Self::Error {
            crate::FsError::InvalidDiff
        }
        fn compare(&mut self, index: &usize) -> Result<Ordering, Self::Error> {
            let delta = crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            };
            self.accounting.admit(delta)?;
            let ordering = (self.compare)(*index)?;
            self.accounting.record_completed(delta)?;
            Ok(ordering)
        }
    }
    acyclic_collections::page::search::binary_search_indexed(
        len,
        known_before,
        &mut Indexed {
            accounting,
            compare,
        },
    )
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
