//! Ordered mutation ranges for the original children of one decoded page.

use super::Child;
use std::ops::Range;

/// The next original child and the mutations bounded by its next sibling.
pub struct Selected<'a, K, P> {
    /// Original child position, for exact inherited endpoint ownership.
    pub index: usize,
    /// The original child, borrowed without cloning its key or reference.
    pub child: &'a Child<K, P>,
    /// The next sibling's key; `None` leaves the caller's inherited upper bound.
    pub upper: Option<&'a K>,
    /// The original sorted mutation positions assigned to this child.
    pub mutations: Range<usize>,
}

/// A monotonically advancing cursor over one page and its selected mutations.
///
/// The caller validates ordered child frontiers and supplies its original work
/// admission. The cursor retains only the original three positions; it owns no
/// page, key, allocation or IO operation.
pub struct Cursor {
    next_child: usize,
    mutation_cursor: usize,
    mutation_end: usize,
}

impl Cursor {
    /// Starts at the first original child with the request's mutation range.
    pub fn new(mutations: Range<usize>) -> Self {
        Self {
            next_child: 0,
            mutation_cursor: mutations.start,
            mutation_end: mutations.end,
        }
    }

    /// Selects one child, charging before each original frontier comparison.
    ///
    /// Success consumes one child. Across the page, each assigned mutation is
    /// consumed once; a boundary comparison can repeat at the next sibling.
    /// Admission or missing-mutation errors stop the original operation.
    pub fn select<'a, 'm, K: Ord + 'm, P, E>(
        &mut self,
        children: &'a [Child<K, P>],
        mut mutation_key: impl FnMut(usize) -> Result<&'m K, E>,
        mut charge_item: impl FnMut() -> Result<(), E>,
    ) -> Result<Option<Selected<'a, K, P>>, E> {
        let Some(child) = children.get(self.next_child) else {
            return Ok(None);
        };
        let index = self.next_child;
        self.next_child += 1;
        let start = self.mutation_cursor;
        let upper = children.get(index + 1).map(|next| &next.first);
        if let Some(upper) = upper {
            while self.mutation_cursor < self.mutation_end {
                charge_item()?;
                if mutation_key(self.mutation_cursor)? >= upper {
                    break;
                }
                self.mutation_cursor += 1;
            }
        } else {
            self.mutation_cursor = self.mutation_end;
        }
        charge_item()?;
        Ok(Some(Selected {
            index,
            child,
            upper,
            mutations: start..self.mutation_cursor,
        }))
    }
}
