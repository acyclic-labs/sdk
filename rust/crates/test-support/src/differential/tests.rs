use super::{pair, record};
use proptest::prelude::*;
use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
};

proptest! {
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
        prop_assert_eq!(before, after);
        let [before, broken] = run(true);
        prop_assert_eq!(&before.0, &broken.0);
        prop_assert_ne!(before, broken); // Missing preflight violates the same equality.
    }
}

#[test]
fn capture_resets_after_panic_and_rejects_wrong_types_and_nesting() {
    for kind in 0..3 {
        assert!(
            catch_unwind(AssertUnwindSafe(|| pair::<_, (), u8>(
                || (),
                |(), _| {
                    match kind {
                        0 => panic!("broken operation"),
                        1 => record(1u16),
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
