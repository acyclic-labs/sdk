//! The shared ready-list production core against its independent lifecycle model.
#![cfg(all(test, not(loom)))]

include!("../../filesystem/src/native_mount/shared/ready.rs");

mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::VecDeque;

    proptest! {
        #![proptest_config(ProptestConfig {
            rng_seed: proptest::test_runner::RngSeed::Fixed(0x7265_6164_792d_7631),
            ..ProptestConfig::default()
        })]
        #[test]
        fn readiness_follows_original_view_lifetimes(
            operations in proptest::collection::vec((0u8..5, 0u64..32), 0..128)
        ) {
            let mut actual = ReadyViews::default();
            let mut live = HashMap::new();
            let mut ready = VecDeque::new();
            for (operation, serial) in operations {
                match operation {
                    0 => {
                        if let std::collections::hash_map::Entry::Vacant(entry) = live.entry(serial) {
                            entry.insert(serial);
                            actual.insert(serial, serial);
                        }
                    }
                    1 => {
                        let scheduled = live.contains_key(&serial) && !ready.contains(&serial);
                        prop_assert_eq!(actual.schedule(serial), scheduled);
                        if scheduled {
                            ready.push_back(serial);
                        }
                    }
                    2 => {
                        ready.retain(|queued| *queued != serial);
                        prop_assert_eq!(actual.remove(serial), live.remove(&serial));
                    }
                    3 => {
                        let expected = live.get_mut(&serial);
                        let actual = actual.get_mut(serial);
                        prop_assert_eq!(actual.is_some(), expected.is_some());
                        if let (Some(actual), Some(expected)) = (actual, expected) {
                            *actual += 1;
                            *expected += 1;
                        }
                    }
                    _ => {
                        let expected = ready.pop_front().map(|serial| (serial, live[&serial]));
                        prop_assert_eq!(actual.pop().map(|(serial, value)| (serial, *value)), expected);
                    }
                }
                prop_assert_eq!(actual.views.len(), live.len(), "retired records cannot accumulate");
                let mut next = actual.first;
                let mut previous = None;
                for &serial in &ready {
                    prop_assert_eq!(next, Some(serial), "ready work preserves admission order");
                    let links = actual.views[&serial].ready.as_ref().unwrap();
                    prop_assert_eq!(links.previous, previous);
                    previous = Some(serial);
                    next = links.next;
                }
                prop_assert_eq!(next, None, "every queued record belongs to a live view");
                prop_assert_eq!(actual.last, previous);
                for (&serial, view) in &actual.views {
                    prop_assert!(live.contains_key(&serial));
                    prop_assert_eq!(view.ready.is_some(), ready.contains(&serial));
                }
            }
        }
    }
}
