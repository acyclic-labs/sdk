use super::*;

// Independent recursive pointer-tree model. Production uses an integer arena
// and parent walks; this model determines balance and physical write counts by
// recursive subtree replacement. Neither production topology nor accounting
// functions participate in the oracle.
struct Shape {
    key: u8,
    height: usize,
    child: [Option<Box<Shape>>; 2],
}

fn height(tree: &Option<Box<Shape>>) -> usize {
    tree.as_ref().map_or(0, |node| node.height)
}

fn rotate(mut lower: Box<Shape>, side: usize, parent: bool, bytes: &mut u64) -> Box<Shape> {
    let mut upper = lower.child[side].take().unwrap();
    let middle = upper.child[1 - side].take();
    // Lower/upper rewrites and their height refreshes, the middle's parent,
    // and either the grandparent's child or the external root pointer.
    *bytes += (4 + u64::from(middle.is_some()) + u64::from(parent)) * link_bytes()
        + if parent {
            0
        } else {
            std::mem::size_of::<Option<usize>>() as u64
        };
    lower.child[side] = middle;
    lower.height = 1 + height(&lower.child[0]).max(height(&lower.child[1]));
    upper.child[1 - side] = Some(lower);
    upper.height = 1 + height(&upper.child[0]).max(height(&upper.child[1]));
    upper
}

fn insert_shape(tree: Option<Box<Shape>>, key: u8, parent: bool, bytes: &mut u64) -> Box<Shape> {
    let Some(mut node) = tree else {
        *bytes += if parent {
            link_bytes()
        } else {
            std::mem::size_of::<Option<usize>>() as u64
        };
        return Box::new(Shape {
            key,
            height: 1,
            child: [None, None],
        });
    };
    let side = usize::from(key > node.key);
    node.child[side] = Some(insert_shape(node.child[side].take(), key, true, bytes));
    node.height = 1 + height(&node.child[0]).max(height(&node.child[1]));
    *bytes += link_bytes();
    let a = height(&node.child[0]);
    let b = height(&node.child[1]);
    let heavy = if a > b + 1 {
        Some(0)
    } else if b > a + 1 {
        Some(1)
    } else {
        None
    };
    if let Some(side) = heavy {
        let child = node.child[side].as_ref().unwrap();
        if height(&child.child[1 - side]) > height(&child.child[side]) {
            let child = node.child[side].take().unwrap();
            node.child[side] = Some(rotate(child, 1 - side, true, bytes));
        }
        rotate(node, side, parent, bytes)
    } else {
        node
    }
}

fn link_bytes() -> u64 {
    // Two child pointers, a parent pointer, height and subtree cardinality.
    (3 * std::mem::size_of::<Option<usize>>() + 2 * std::mem::size_of::<usize>()) as u64
}

// The oracle uses plain keys/side bits and integer arithmetic, never production
// search, allocation, admission, or receipt code. The linear position determines
// the result independently of the frozen old binary-search comparison plan.
fn model(
    pages: &[(bool, Vec<u8>)],
    prefix: bool,
    budget: u64,
) -> (Vec<(u8, bool, bool)>, bool, WorkCounters) {
    let mut values: Vec<(u8, bool, bool)> = Vec::new();
    let mut capacity = 0;
    let mut shape = None;
    let mut work = WorkCounters::UNCHARGED;
    let slot_bytes = std::mem::size_of::<ValueChange<FileId, FileRecord>>() as u64;
    for (before, keys) in pages {
        let source_bytes = (keys.len() * std::mem::size_of::<FileRecord>()) as u64;
        work.allocation_operations += u64::from(source_bytes != 0);
        work.peak_allocation_bytes = work
            .peak_allocation_bytes
            .max(capacity * (slot_bytes + link_bytes()) + source_bytes);
        let mut floor = 0;
        for key in keys {
            let (mut low, mut high) = (0, values.len());
            while low < high {
                let middle = low + (high - low) / 2;
                if !prefix || middle >= floor {
                    if work.items_examined == budget {
                        return (values, false, work);
                    }
                    work.items_examined += 1;
                }
                match values[middle].0.cmp(key) {
                    std::cmp::Ordering::Less => low = middle + 1,
                    std::cmp::Ordering::Greater => high = middle,
                    std::cmp::Ordering::Equal => break,
                }
            }
            let index = values
                .iter()
                .position(|entry| entry.0 >= *key)
                .unwrap_or(values.len());
            if values.get(index).is_none_or(|entry| entry.0 != *key) {
                if values.len() as u64 == capacity {
                    capacity = (capacity * 2).max(1);
                    work.allocation_operations += 2;
                    work.peak_allocation_bytes = work
                        .peak_allocation_bytes
                        .max(capacity * (slot_bytes + link_bytes()) + source_bytes);
                }
                work.bytes_copied += slot_bytes + link_bytes();
                shape = Some(insert_shape(
                    shape.take(),
                    *key,
                    false,
                    &mut work.bytes_copied,
                ));
                values.insert(index, (*key, false, false));
            }
            work.bytes_copied += std::mem::size_of::<Option<FileRecord>>() as u64;
            if *before {
                values[index].1 = true;
            } else {
                values[index].2 = true;
            }
            floor = index + 1;
        }
    }
    (values, true, work)
}

fn check<const PREFIX: bool>(
    pages: &[(bool, Vec<u8>)],
    budget: u64,
    actual: (Outcome, WorkCounters, u64, Changes),
) -> proptest::test_runner::TestCaseResult {
    let (partial, completed, expected) = model(pages, PREFIX, budget);
    let (outcome, receipt, probes, changes) = actual;
    // Check the full receipt, including allocations/peak before a failed probe,
    // and independently observed comparator calls, not merely receipt <= limit.
    prop_assert_eq!(receipt, expected, "budget={}, pages={:?}", budget, pages);
    prop_assert_eq!(probes, expected.items_examined);
    let expected_changes: Changes = partial
        .into_iter()
        .map(|(key, before, after)| {
            let record = file_record(key).unwrap();
            let mut replacement = record.clone();
            replacement.link_count = 2;
            ValueChange {
                key: record.file_id,
                before: before.then_some(record),
                after: after.then_some(replacement),
            }
        })
        .collect();
    prop_assert_eq!(
        &changes,
        &expected_changes,
        "exact completed mutation prefix"
    );
    if completed {
        prop_assert_eq!(outcome, Ok((expected_changes, false)));
    } else {
        prop_assert!(outcome.is_err());
        prop_assert!(outcome.unwrap_err().contains("items_examined"));
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn every_generated_page_side_budget_has_the_exact_independent_prefix(
        before in prop::collection::btree_set(1_u8..32, 0..9),
        after in prop::collection::btree_set(1_u8..32, 0..9),
        left_width in 1_usize..5, right_width in 1_usize..5,
        choices in prop::collection::vec(any::<bool>(), 0..17),
    ) {
        // Each side stays ordered, while page boundaries and side switches vary.
        // Bound sequence size, not the budgets checked: every budget is tested.
        let before: Vec<_> = before.into_iter().collect();
        let after: Vec<_> = after.into_iter().collect();
        let mut left = before.chunks(left_width).peekable();
        let mut right = after.chunks(right_width).peekable();
        let mut pages = Vec::new();
        for choice in choices.into_iter().chain(std::iter::repeat(true)) {
            if left.peek().is_none() && right.peek().is_none() {
                break;
            }
            let side = right.peek().is_none() || (choice && left.peek().is_some());
            let keys = if side { left.next() } else { right.next() }.unwrap();
            pages.push((side, keys.to_vec()));
        }
        let old_total = model(&pages, false, u64::MAX).2.items_examined;
        let new_total = model(&pages, true, u64::MAX).2.items_examined;
        prop_assert!(new_total <= old_total);
        for budget in 0..=old_total + 1 {
            check::<false>(&pages, budget, replay_pages::<false, true>(&pages, budget))?;
            check::<true>(&pages, budget, replay_pages::<true, true>(&pages, budget))?;
        }
    }
}

#[test]
fn prefix_oracle_rejects_early_failure_and_missing_completed_charges() {
    let pages = vec![(true, vec![1, 3, 5]), (false, vec![1, 2, 4])];
    for prefix in [false, true] {
        let run = |budget| {
            if prefix {
                replay_pages::<true, true>(&pages, budget)
            } else {
                replay_pages::<false, true>(&pages, budget)
            }
        };
        let verify = |budget, actual| {
            if prefix {
                check::<true>(&pages, budget, actual)
            } else {
                check::<false>(&pages, budget, actual)
            }
        };
        // A real production run rejecting one comparison early is invalid even
        // though its error is items_examined and its receipt is below the limit.
        assert!(verify(2, run(1)).is_err());
        let mut lost_charge = run(2);
        lost_charge.1.items_examined -= 1;
        assert!(verify(2, lost_charge).is_err());
        // Also reject a missing allocation charge and the real broken reset.
        let mut lost_allocation = run(2);
        lost_allocation.1.allocation_operations -= 1;
        assert!(verify(2, lost_allocation).is_err());
        assert!(verify(2, run(2)).is_ok());
    }
    assert!(
        check::<true>(
            &pages,
            u64::MAX,
            replay_pages::<true, false>(&pages, u64::MAX),
        )
        .is_err()
    );
}
