//! Borrowed ordered groups and mutation ranges for decoded pages.

use super::Child;
use std::ops::Range;

/// Adjacent equal-key groups in an original ordered range.
///
/// The caller owns ordering, key backing, admission and comparison. This cursor
/// stores only positions and a terminal failure bit; it allocates and copies
/// nothing. A completed range consumes every position and compares once per
/// position after the first. Key comparison retains its input-dependent cost.
pub struct Groups {
    next: usize,
    end: usize,
    failed: bool,
}

impl Groups {
    /// Selects from an already ordered range; reversed ranges refuse on selection.
    pub fn new(range: Range<usize>) -> Self {
        Self {
            next: range.start,
            end: range.end,
            failed: range.start > range.end,
        }
    }

    /// Returns the next nonempty equal-key range, preserving original positions.
    ///
    /// `compare` admits its work before comparing the borrowed keys. A key or
    /// comparison refusal, or decreasing key, terminates this cursor; later
    /// calls refuse without examining another key. The caller checks its
    /// original cancellation even when this range is empty.
    pub fn select<'a, K: 'a, E>(
        &mut self,
        mut key: impl FnMut(usize) -> Result<&'a K, E>,
        mut compare: impl FnMut(&K, &K) -> Result<std::cmp::Ordering, E>,
        invalid: impl Fn() -> E,
    ) -> Result<Option<Range<usize>>, E> {
        if self.failed {
            return Err(invalid());
        }
        self.failed = true;
        if self.next == self.end {
            self.failed = false;
            return Ok(None);
        }
        let start = self.next;
        let first = key(start)?;
        let mut next = start + 1;
        while next < self.end {
            match compare(first, key(next)?)? {
                std::cmp::Ordering::Equal => next += 1,
                std::cmp::Ordering::Less => break,
                std::cmp::Ordering::Greater => return Err(invalid()),
            }
        }
        self.next = next;
        self.failed = false;
        Ok(Some(start..next))
    }
}

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
    /// The following child has a mutation at its exact first key.
    pub following_first_changes: bool,
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

    /// The exact unconsumed mutation positions, without scanning.
    pub fn remaining(&self) -> Range<usize> {
        self.mutation_cursor..self.mutation_end
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
        mut reaches: impl FnMut(usize, &K) -> Result<bool, E>,
        removed_from: Option<&K>,
        mut charge_item: impl FnMut() -> Result<(), E>,
    ) -> Result<Option<Selected<'a, K, P>>, E> {
        let Some(child) = children.get(self.next_child) else {
            return Ok(None);
        };
        if removed_from.is_some_and(|removed| &child.first >= removed) {
            self.next_child = children.len();
            self.mutation_cursor = self.mutation_end;
            return Ok(None);
        }
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
        let end = self.mutation_cursor;
        let following_first_changes = if let Some(upper) = upper {
            if self.mutation_cursor < self.mutation_end {
                charge_item()?;
                mutation_key(self.mutation_cursor)? == upper
            } else {
                false
            }
        } else {
            false
        };
        if let Some(upper) = upper {
            while self.mutation_cursor > start {
                charge_item()?;
                if !reaches(self.mutation_cursor - 1, upper)? {
                    break;
                }
                self.mutation_cursor -= 1;
            }
        }
        charge_item()?;
        Ok(Some(Selected {
            index,
            child,
            upper,
            mutations: start..end,
            following_first_changes,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::Groups;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(proptest::test_runner::Config {
            cases: 128,
            failure_persistence: None,
            rng_seed: proptest::test_runner::RngSeed::Fixed(0x6772_6f75_702d_7631),
            ..proptest::test_runner::Config::default()
        })]
        #[test]
        fn groups_cover_original_positions_with_linear_comparisons(
            mut keys in proptest::collection::vec(0u16..64, 0..256),
            offset in 0usize..16,
        ) {
            keys.sort_unstable();
            let mut cursor = Groups::new(offset..offset + keys.len());
            let mut consumed = 0;
            let mut comparisons = 0;
            while let Some(range) = cursor.select(
                |position| keys.get(position - offset).ok_or("missing"),
                |left, right| {
                    comparisons += 1;
                    Ok(left.cmp(right))
                },
                || "invalid",
            ).expect("ordered original keys") {
                prop_assert_eq!(range.start, offset + consumed);
                let remaining = keys.get(consumed..).ok_or_else(|| TestCaseError::fail("group starts past original keys"))?;
                let first = remaining.first().ok_or_else(|| TestCaseError::fail("selected group has no original key"))?;
                let expected = remaining.iter().take_while(|key| *key == first).count();
                prop_assert_eq!(range.len(), expected);
                consumed += expected;
            }
            prop_assert_eq!(consumed, keys.len());
            prop_assert_eq!(comparisons, keys.len().saturating_sub(1));
        }
    }

    #[test]
    fn groups_refuse_terminally_and_reject_prior_name_scan_cost() {
        let keys: Vec<_> = (0..64).collect();
        let mut cursor = Groups::new(0..keys.len());
        let mut comparisons = 0;
        while cursor
            .select(
                |position| keys.get(position).ok_or("missing"),
                |left, right| {
                    comparisons += 1;
                    Ok(left.cmp(right))
                },
                || "invalid",
            )
            .expect("ordered keys")
            .is_some()
        {}
        assert_eq!(comparisons, keys.len() - 1);
        // The former name deduplication compared every preceding mutation.
        let mut prior_comparisons = 0;
        for (position, key) in keys.iter().enumerate() {
            for prior in keys.iter().take(position) {
                prior_comparisons += 1;
                if prior == key {
                    break;
                }
            }
        }
        assert!(prior_comparisons > keys.len());

        for (start, end) in [(3, 2), (0, 1)] {
            let range = start..end;
            let failure = if range.start > range.end {
                "invalid"
            } else {
                "missing"
            };
            let mut cursor = Groups::new(range);
            assert_eq!(
                cursor.select::<i32, _>(
                    |_| Err("missing"),
                    |_, _| panic!("missing key reached comparison"),
                    || "invalid",
                ),
                Err(failure)
            );
            assert_eq!(
                cursor.select::<i32, _>(
                    |_| panic!("failed cursor examined another key"),
                    |_, _| panic!("failed cursor compared another key"),
                    || "terminal",
                ),
                Err("terminal")
            );
        }

        for keys in [&[1, 1, 2][..], &[1, 3, 2][..]] {
            let mut cursor = Groups::new(0..keys.len());
            let result = cursor.select(
                |position| keys.get(position).ok_or("missing"),
                |left, right| {
                    if left == right {
                        Err("admission")
                    } else {
                        Ok(left.cmp(right))
                    }
                },
                || "invalid",
            );
            if matches!(keys, [first, second, ..] if first == second) {
                assert_eq!(result, Err("admission"));
            } else {
                assert_eq!(result, Ok(Some(0..1)));
                assert_eq!(
                    cursor.select(
                        |position| keys.get(position).ok_or("missing"),
                        |left, right| Ok(left.cmp(right)),
                        || "invalid",
                    ),
                    Err("invalid")
                );
            }
            assert_eq!(
                cursor.select::<i32, _>(
                    |_| panic!("terminal cursor examined another key"),
                    |_, _| panic!("terminal cursor compared another key"),
                    || "terminal",
                ),
                Err("terminal")
            );
        }
    }
}
