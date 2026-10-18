use super::{Operation, observe, pair, pair_of, record};
use proptest::prelude::*;
use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
};

proptest! {
    // Any subscription retains exactly its events, including repeated names
    // and arbitrary payloads, in recorded order on both independent runs.
    #[test]
    fn named_subscriptions_preserve_exact_order_and_payloads(
        events in prop::collection::vec((0usize..4, any::<u64>()), 0..64),
        selection in 0u8..16,
    ) {
        let names = ["read", "verify", "retain", "foreign"];
        let kinds: Vec<_> = names.iter().enumerate()
            .filter(|(index, _)| selection & (1 << index) != 0)
            .map(|(_, name)| *name).collect();
        let expected: Vec<_> = events.iter()
            .filter(|(index, _)| selection & (1 << index) != 0)
            .map(|(index, amount)| (names[*index], *amount)).collect();
        let captures = pair_of(&kinds, || record(("read", u64::MAX)), |(), old| {
            for (index, amount) in &events {
                record((names[*index], *amount));
                record(1u8); // A foreign type cannot enter a named capture.
            }
            old
        });
        assert_named_captures(captures, &expected);
    }

    #[test]
    fn independent_states_and_ordered_generic_events(events in prop::collection::vec(any::<u16>(), 0..64)) {
        let initializations = Cell::new(0);
        let run = |broken| pair::<_, _, u16>(|| {
            initializations.set(initializations.get() + 1);
            record(999u16); // Initialization is outside trace capture.
            events.clone()
        }, |mut state, old| {
            let result = state.clone();
            if !(broken && !old) { record(0u16); }
            for event in state.drain(..) { record(event); }
            result
        });
        let [before, after] = run(false);
        prop_assert_eq!(initializations.get(), 2);
        prop_assert_eq!(&before.0, &events);
        prop_assert_eq!(&before.1, &[vec![0], events.clone()].concat());
        prop_assert_eq!(&before, &after);
        let single = observe::<_, _, u16>(|| events.clone(), |state| {
            record(0u16);
            for event in &state { record(*event); }
            state
        });
        prop_assert_eq!(&single, &before);
        let [before, broken] = run(true);
        prop_assert_eq!(&before.0, &broken.0);
        prop_assert_ne!(before, broken); // Missing preflight violates the same equality.
    }
}

fn assert_named_captures(captures: [(bool, Vec<Operation>); 2], expected: &[Operation]) {
    for ((result, trace), old) in captures.into_iter().zip([true, false]) {
        assert_eq!(result, old);
        assert_eq!(trace, expected, "named subscription leaked or lost events");
    }
}

#[test]
fn negative_control_detects_missing_kind_subscription() {
    // The former type-only capture goes through the property's same oracle.
    let captures = pair::<_, _, Operation>(
        || (),
        |(), old| {
            record(("read", 1));
            record(("foreign", 2));
            old
        },
    );
    catch_unwind(|| assert_named_captures(captures, &[("read", 1)]))
        .expect_err("the oracle accepted an unfiltered capture");
}

#[test]
fn capture_resets_after_panic_and_rejects_nesting() {
    for kind in 0..2 {
        assert!(
            catch_unwind(AssertUnwindSafe(|| pair::<_, (), u8>(
                || (),
                |(), _| {
                    match kind {
                        0 => panic!("broken operation"),
                        _ => {
                            pair::<_, _, u8>(|| (), |(), _| ());
                        }
                    }
                }
            )))
            .is_err()
        );
        assert_eq!(
            pair::<_, _, String>(|| (), |(), _| record("read".to_owned())),
            [((), vec!["read".to_owned()]), ((), vec!["read".to_owned()])]
        );
    }
    struct Event;
    impl Drop for Event {
        fn drop(&mut self) {
            record(1u32);
        }
    }
    assert!(
        catch_unwind(|| pair::<_, (), Event>(
            || (),
            |(), _| {
                record(Event);
                panic!("event drop during reset");
            }
        ))
        .is_err()
    );
    record(1u32); // Capture is inactive again.
}

// A shared code path may carry observation points of several types; each
// capture keeps exactly its own type, in order, and ignores the others.
#[test]
fn captures_keep_only_their_event_type() {
    let run = |(), _: bool| {
        record(1u16);
        record(1u8);
        record(("foreign", 2u64));
        record(2u8);
        record(3u16);
    };
    assert_eq!(
        pair::<_, _, u8>(|| (), run),
        [((), vec![1, 2]), ((), vec![1, 2])]
    );
    assert_eq!(
        pair::<_, _, u16>(|| (), run),
        [((), vec![1, 3]), ((), vec![1, 3])]
    );
    assert_eq!(
        pair::<_, _, (&str, u64)>(|| (), run),
        [((), vec![("foreign", 2)]), ((), vec![("foreign", 2)])]
    );
}

// Observation points of one event type are kept apart by name.
#[test]
fn named_captures_keep_only_their_kinds() {
    let run = |(), _: bool| {
        record(("read", 1u64));
        record(("decode", 2u64));
        record(1u8);
        record(("verify", 3u64));
        record(("read", 4u64));
    };
    assert_eq!(
        pair_of(&["read", "verify"], || (), run),
        [
            ((), vec![("read", 1), ("verify", 3), ("read", 4)]),
            ((), vec![("read", 1), ("verify", 3), ("read", 4)]),
        ]
    );
    assert_eq!(
        pair_of(&["absent"], || (), run),
        [((), vec![]), ((), vec![])]
    );
}

#[test]
fn inactive_event_destructors_can_record_during_initialization() {
    use std::rc::Rc;
    struct Event(Rc<Cell<usize>>);
    impl Drop for Event {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
            record(1u32);
        }
    }
    let dropped = Rc::new(Cell::new(0));
    record(Event(dropped.clone()));
    assert_eq!(dropped.get(), 1);
    assert_eq!(
        pair::<_, _, u32>(|| record(Event(dropped.clone())), |(), _| record(2u32),),
        [((), vec![2]), ((), vec![2])]
    );
    assert_eq!(dropped.get(), 3);
}
