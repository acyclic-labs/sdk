//! Borrowed ordered-page searches over the caller's original probe admission.

use std::cmp::Ordering;

/// One comparison continuation in the shared binary-search decision tree.
/// A caller may await an admitted comparison before resolving this probe;
/// no borrowed slice, key, allocation or runtime is retained here.
pub enum Search {
    /// The matching position, or the insertion position on absence.
    Complete(Result<usize, usize>),
    /// The next original position that needs an actual comparison.
    Compare(Probe),
}

/// One exact decision, with the remaining range retained in constant space.
pub struct Probe {
    base: usize,
    end: usize,
    middle: usize,
    known_before: usize,
}

impl Search {
    /// Begins the ordinary whole-slice decision tree. An authentic Less prefix
    /// avoids its redundant comparisons; an out-of-range prefix returns None.
    /// The caller checks original admission when the result needs no comparison.
    pub fn new(length: usize, known_before: usize) -> Option<Self> {
        if known_before > length {
            None
        } else if known_before == length {
            Some(Self::Complete(Err(length)))
        } else {
            Some(Self::within(0, length, known_before))
        }
    }

    fn within(mut base: usize, end: usize, known_before: usize) -> Self {
        while base < end {
            let middle = base + (end - base) / 2;
            if middle < known_before {
                base = middle + 1;
            } else {
                return Self::Compare(Probe {
                    base,
                    end,
                    middle,
                    known_before,
                });
            }
        }
        Self::Complete(Err(base))
    }
}

impl Probe {
    /// Exact original slice position to compare.
    pub const fn index(&self) -> usize {
        self.middle
    }

    /// Resolves this comparison. Every nonterminal transition strictly reduces
    /// the remaining range, including inferred comparisons inside a known prefix.
    pub fn resolve(self, ordering: Ordering) -> Search {
        match ordering {
            Ordering::Less => Search::within(self.middle + 1, self.end, self.known_before),
            Ordering::Greater => Search::within(self.base, self.middle, self.known_before),
            Ordering::Equal => Search::Complete(Ok(self.middle)),
        }
    }
}

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
    let mut search = Search::new(items.len(), known_before).ok_or_else(|| work.invalid_prefix())?;
    if known_before == items.len() {
        work.check()?;
    }
    loop {
        match search {
            Search::Complete(result) => return Ok(result),
            Search::Compare(probe) => {
                let ordering = work.compare(&items[probe.index()])?;
                search = probe.resolve(ordering);
            }
        }
    }
}
