use crate::page::search::{self, Search};
use proptest::prelude::*;
use std::cmp::Ordering;

struct Work {
    target: i32,
    probes: Vec<i32>,
    refuse: Option<usize>,
}

impl search::Work<i32> for Work {
    type Error = ();
    fn check(&mut self) -> Result<(), ()> {
        if self.refuse == Some(self.probes.len()) {
            Err(())
        } else {
            Ok(())
        }
    }
    fn compare(&mut self, value: &i32) -> Result<Ordering, ()> {
        self.check()?;
        self.probes.push(*value);
        Ok(value.cmp(&self.target))
    }
    fn invalid_prefix(&self) {}
}

proptest! {
    #[test]
    fn search_continuations_preserve_ordered_positions_probes_and_refusals(
        mut values in prop::collection::vec(any::<i32>(), 0..128), target in any::<i32>(),
    ) {
        values.sort_unstable();
        values.dedup();
        let expected = values.binary_search(&target);
        let prefix = values.partition_point(|value| *value < target);
        let mut baseline = Work { target, probes: Vec::new(), refuse: None };
        prop_assert_eq!(search::binary_search(&values, &mut baseline), Ok(expected));
        for known in 0..=prefix {
            let mut work = Work { target, probes: Vec::new(), refuse: None };
            prop_assert_eq!(search::binary_search_from(&values, known, &mut work), Ok(expected));
            let mut original = baseline.probes.iter();
            for probe in &work.probes {
                prop_assert!(original.any(|value| value == probe));
            }
            for refused in 0..work.probes.len() {
                let mut denied = Work { target, probes: Vec::new(), refuse: Some(refused) };
                prop_assert_eq!(search::binary_search_from(&values, known, &mut denied), Err(()));
                prop_assert_eq!(denied.probes, work.probes[..refused].to_vec());
            }
        }
        prop_assert!(Search::new(values.len(), values.len() + 1).is_none());
    }
}

#[test]
fn search_coordinates_do_not_require_a_materialized_slice() {
    let mut search = Search::new(usize::MAX, 0).expect("valid coordinate range");
    let mut comparisons = 0;
    loop {
        match search {
            Search::Complete(result) => {
                assert_eq!(result, Err(usize::MAX));
                assert_eq!(comparisons, usize::BITS);
                break;
            }
            Search::Compare(probe) => {
                comparisons += 1;
                search = probe.resolve(Ordering::Less);
            }
        }
    }
}
