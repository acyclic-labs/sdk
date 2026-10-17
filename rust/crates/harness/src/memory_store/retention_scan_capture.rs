pub(crate) fn capture<R>(
    cancellation: &CancellationToken,
    cancel_at: Option<usize>,
    run: impl FnOnce() -> R,
) -> (R, Vec<Event>) {
    OBSERVER.with(|state| {
        let mut state = state.borrow_mut();
        assert!(state.is_none(), "nested operation trace");
        *state = Some(Observer {
            events: Vec::new(),
            cancellation: cancellation.clone(),
            cancel_at,
            observations: 0,
        });
    });
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            OBSERVER.with(|state| {
                state.borrow_mut().take();
            });
        }
    }
    let reset = Reset;
    let result = run();
    let events = OBSERVER.with(|state| state.borrow_mut().take().unwrap().events);
    drop(reset);
    (result, events)
}

pub(crate) fn assert_matches(expected: &[Event], actual: &[Event]) {
    assert_eq!(actual, expected, "preflight/charge trace changed");
}
