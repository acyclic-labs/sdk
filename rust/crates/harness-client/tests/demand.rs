//! Exhaustive admission over the public view constructor and distinct interest.
use acyclic_harness_client::{DemandError, DemandPolicy, ViewDemand};
use acyclic_stream::StreamPath;

#[test]
fn view_capacity_depends_only_on_distinct_interest() {
    let keys = ["view/a", "view/b", "view/c"].map(|key| StreamPath::new(key).unwrap());
    for length in 0..=5 {
        for mut word in 0..3_usize.pow(length) {
            let indices: Vec<_> = (0..length)
                .map(|_| {
                    let index = word % 3;
                    word /= 3;
                    index
                })
                .collect();
            let full = indices.iter().fold(0_u8, |mask, index| mask | (1 << index));
            for split in 0..=indices.len() {
                let expanded = indices[..split]
                    .iter()
                    .fold(0_u8, |mask, index| mask | (1 << index));
                for selected in [None, Some(1), Some(2), Some(3)] {
                    for pages in [None, Some(1), Some(2), Some(3)] {
                        let result = ViewDemand::new(
                            DemandPolicy::new(selected, pages).unwrap(),
                            indices[..split].iter().map(|index| keys[*index].clone()),
                            indices[split..].iter().map(|index| keys[*index].clone()),
                            indices[..split]
                                .iter()
                                .rev()
                                .map(|index| keys[*index].clone()),
                        );
                        let expected = if selected
                            .is_some_and(|cap| usize::try_from(full.count_ones()).unwrap() > cap)
                        {
                            Err(DemandError::SelectedCapacity)
                        } else if pages.is_some_and(|cap| {
                            usize::try_from(expanded.count_ones()).unwrap() > cap
                        }) {
                            Err(DemandError::ExpandedCapacity)
                        } else {
                            Ok(())
                        };
                        assert_eq!(
                            result.as_ref().map(|_| ()).map_err(|error| *error),
                            expected,
                            "distinct-interest admission: indices={indices:?} split={split} selected={selected:?} expanded={pages:?}"
                        );
                        if let Ok(demand) = result {
                            for (actual, mask) in [
                                (demand.full().collect::<Vec<_>>(), full),
                                (demand.expanded().collect(), expanded),
                            ] {
                                assert!(
                                    actual.into_iter().eq(keys
                                        .iter()
                                        .enumerate()
                                        .filter(|(index, _)| mask & (1 << index) != 0)
                                        .map(|(_, key)| key))
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
