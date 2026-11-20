use super::{observe, pair, record};
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

#[test]
fn async_capture_excludes_other_polls_and_releases_on_drop() {
    use std::{
        future::poll_fn,
        task::{Context, Poll, Waker},
    };
    let mut polls = 0_u8;
    let producer = poll_fn(|cx| {
        assert!(acyclic_differential::capturing());
        record(polls);
        polls += 1;
        if polls == 1 {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(42)
        }
    });
    let mut capture = Box::pin(super::observe_async::<_, u8>(producer));
    let mut context = Context::from_waker(Waker::noop());
    assert!(std::future::Future::poll(capture.as_mut(), &mut context).is_pending());
    assert!(!acyclic_differential::capturing());
    record(99_u8); // Another task's observation between the original polls.
    assert_eq!(
        std::future::Future::poll(capture.as_mut(), &mut context),
        Poll::Ready((42, vec![0, 1]))
    );
    drop(capture);
    assert!(!acyclic_differential::capturing());
    let mut dropped = Box::pin(super::observe_async::<_, u8>(poll_fn(|_| {
        record(7_u8);
        Poll::<()>::Pending
    })));
    assert!(std::future::Future::poll(dropped.as_mut(), &mut context).is_pending());
    drop(dropped);
    assert!(!acyclic_differential::capturing());
    assert_eq!(observe(|| (), |()| record(8_u8)), ((), vec![8_u8]));
}
