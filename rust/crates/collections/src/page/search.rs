//! Borrowed ordered-page searches over the caller's original probe admission.

use std::cmp::Ordering;

/// Original admission, comparison and completion for each examined entry.
pub trait Work<T> {
    /// The caller's unchanged refusal or completion error.
    type Error;
    /// Checks an invocation that needs no actual comparison.
    fn check(&mut self) -> Result<(), Self::Error>;
    /// Admits the probe before comparison and records its completed work.
    fn compare(&mut self, item: &T) -> Result<Ordering, Self::Error>;
    /// Rejects a known prefix outside the borrowed slice, before any probe.
    fn invalid_prefix(&self) -> Self::Error;
}

/// Searches the original slice without allocating or copying entries.
pub fn binary_search<T, W: Work<T>>(
    items: &[T],
    work: &mut W,
) -> Result<Result<usize, usize>, W::Error> {
    if items.is_empty() {
        return Ok(Err(0));
    }
    binary_search_from(items, 0, work)
}

/// Follows the whole-slice decision tree, inferring only an authentic Less prefix.
/// Every actual probe is a subsequence of the ordinary search's probes. Each
/// transition returns a result or strictly reduces the remaining slice.
pub fn binary_search_from<T, W: Work<T>>(
    items: &[T],
    known_before: usize,
    work: &mut W,
) -> Result<Result<usize, usize>, W::Error> {
    struct Slice<'a, T, W> {
        items: &'a [T],
        original: &'a mut W,
    }
    impl<T, W: Work<T>> Work<usize> for Slice<'_, T, W> {
        type Error = W::Error;
        fn check(&mut self) -> Result<(), Self::Error> {
            self.original.check()
        }
        fn compare(&mut self, index: &usize) -> Result<Ordering, Self::Error> {
            self.original.compare(&self.items[*index])
        }
        fn invalid_prefix(&self) -> Self::Error {
            self.original.invalid_prefix()
        }
    }
    binary_search_indexed(
        items.len(),
        known_before,
        &mut Slice {
            items,
            original: work,
        },
    )
}

/// Uses the same probe plan over original rank-addressable storage.
/// The caller admits each comparison before observing its payload.
pub fn binary_search_indexed<W: Work<usize>>(
    len: usize,
    known_before: usize,
    work: &mut W,
) -> Result<Result<usize, usize>, W::Error> {
    if known_before > len {
        return Err(work.invalid_prefix());
    }
    if known_before == len {
        work.check()?;
        return Ok(Err(known_before));
    }
    let (mut base, mut end) = (0_usize, len);
    while base < end {
        let index = base + (end - base) / 2;
        let ordering = if index < known_before {
            Ordering::Less
        } else {
            work.compare(&index)?
        };
        match ordering {
            Ordering::Less => base = index + 1,
            Ordering::Greater => end = index,
            Ordering::Equal => return Ok(Ok(index)),
        }
    }
    Ok(Err(base))
}
