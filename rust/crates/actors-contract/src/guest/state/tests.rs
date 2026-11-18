use super::{GuestError, GuestState};

#[test]
fn two_handlers_interleave_without_torn_state_or_lost_progress() {
    loom::model(|| {
        let state = GuestState::new((0u8, 0u8));
        let handlers: Vec<_> = (0..2)
            .map(|_| {
                let state = state.clone();
                loom::thread::spawn(move || {
                    assert!(
                        state
                            .with(|value| {
                                value.0 += 1;
                                value.1 += 1;
                            })
                            .is_ok()
                    );
                    // An await boundary: no state lock is held while another handler runs.
                    loom::thread::yield_now();
                    assert!(
                        state
                            .with(|value| {
                                assert_eq!(value.0, value.1);
                                value.0 += 1;
                                value.1 += 1;
                            })
                            .is_ok()
                    );
                })
            })
            .collect();
        for handler in handlers {
            assert!(handler.join().is_ok());
        }
        assert!(state.with(|value| assert_eq!(*value, (4, 4))).is_ok());
    });
}

#[test]
fn lock_order_checker_rejects_same_and_different_state_nesting() {
    loom::model(|| {
        let first = GuestState::new(0);
        let second = GuestState::new(0);
        assert!(
            first
                .with(|_| {
                    assert_eq!(
                        first.with(|_| {}),
                        Err(GuestError::NestedMutation),
                        "nested mutation lost its typed refusal"
                    );
                    assert_eq!(
                        second.with(|_| {}),
                        Err(GuestError::NestedMutation),
                        "nested mutation lost its typed refusal"
                    );
                })
                .is_ok()
        );
        assert!(second.with(|value| *value += 1).is_ok());
    });
}

// Negative control: deliberately split a read/modify/write across an await.
// It exercises the same state shim, but loses atomic exclusion between steps.
#[test]
#[should_panic(expected = "lost update")]
fn model_detects_broken_mutation_exclusion() {
    loom::model(|| {
        let state = GuestState::new(0u8);
        let handlers: Vec<_> = (0..2)
            .map(|_| {
                let state = state.clone();
                loom::thread::spawn(move || {
                    let mut snapshot = 0;
                    assert!(state.with(|value| snapshot = *value).is_ok());
                    loom::thread::yield_now();
                    assert!(state.with(|value| *value = snapshot + 1).is_ok());
                })
            })
            .collect();
        for handler in handlers {
            assert!(handler.join().is_ok());
        }
        assert!(
            state
                .with(|value| assert_eq!(*value, 2, "lost update"))
                .is_ok()
        );
    });
}
