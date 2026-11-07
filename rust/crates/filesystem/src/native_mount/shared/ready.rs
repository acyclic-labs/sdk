//! A ready list whose links live in each view's birth record.
//!
//! Scheduling and cancelling a view touch only that record and its immediate
//! neighbours. Removing a view releases its record immediately, even while a
//! service pass is running; there are no retired serials to scan or retain.

use std::collections::HashMap;

pub(crate) struct ReadyViews<V> {
    views: HashMap<u64, View<V>>,
    first: Option<u64>,
    last: Option<u64>,
}

struct View<V> {
    value: V,
    ready: Option<Links>,
}

struct Links {
    previous: Option<u64>,
    next: Option<u64>,
}

impl<V> Default for ReadyViews<V> {
    fn default() -> Self {
        Self {
            views: HashMap::new(),
            first: None,
            last: None,
        }
    }
}

impl<V> ReadyViews<V> {
    /// Reserves the view's only queue record before publishing its wake hook.
    pub(crate) fn insert(&mut self, serial: u64, value: V) {
        let previous = self.views.insert(serial, View { value, ready: None });
        assert!(previous.is_none(), "a view reserves its ready record once");
    }

    /// Returns whether this call queued the live view. Repeated or late wakes
    /// cannot duplicate a record or recreate a retired view.
    pub(crate) fn schedule(&mut self, serial: u64) -> bool {
        let Some(view) = self.views.get_mut(&serial) else {
            return false;
        };
        if view.ready.is_some() {
            return false;
        }
        view.ready = Some(Links {
            previous: self.last,
            next: None,
        });
        if let Some(last) = self.last {
            self.views
                .get_mut(&last)
                .and_then(|view| view.ready.as_mut())
                .expect("the last ready view owns its links")
                .next = Some(serial);
        } else {
            self.first = Some(serial);
        }
        self.last = Some(serial);
        true
    }

    /// Takes the next pass while retaining the birth record. A wake during
    /// that pass can schedule the same view again.
    pub(crate) fn pop(&mut self) -> Option<(u64, &V)> {
        let serial = self.first?;
        self.unlink(serial);
        Some((
            serial,
            &self
                .views
                .get(&serial)
                .expect("a ready view owns its record")
                .value,
        ))
    }

    /// Cancels pending work and removes the original record in constant
    /// bookkeeping, independently of the number of other views or wakes.
    pub(crate) fn remove(&mut self, serial: u64) -> Option<V> {
        self.unlink(serial);
        self.views.remove(&serial).map(|view| view.value)
    }

    fn unlink(&mut self, serial: u64) {
        let Some(links) = self
            .views
            .get_mut(&serial)
            .and_then(|view| view.ready.take())
        else {
            return;
        };
        if let Some(previous) = links.previous {
            self.views
                .get_mut(&previous)
                .and_then(|view| view.ready.as_mut())
                .expect("the preceding ready view owns its links")
                .next = links.next;
        } else {
            self.first = links.next;
        }
        if let Some(next) = links.next {
            self.views
                .get_mut(&next)
                .and_then(|view| view.ready.as_mut())
                .expect("the following ready view owns its links")
                .previous = links.previous;
        } else {
            self.last = links.previous;
        }
    }
}

#[cfg(all(test, not(loom)))]
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
            operations in proptest::collection::vec((0u8..4, 0u64..32), 0..128)
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
