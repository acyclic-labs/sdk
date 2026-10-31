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
    if known_before > items.len() {
        return Err(work.invalid_prefix());
    }
    if known_before == items.len() {
        work.check()?;
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
            work.compare(middle)?
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
