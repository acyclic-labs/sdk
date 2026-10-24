use std::cell::Cell;

thread_local! {
    static FIXED: Cell<Option<(i64, i32)>> = const { Cell::new(None) };
}

pub(super) fn current() -> Option<prost_types::Timestamp> {
    FIXED
        .get()
        .map(|(seconds, nanos)| prost_types::Timestamp { seconds, nanos })
}

pub(super) fn with<R>(timestamp: &prost_types::Timestamp, run: impl FnOnce() -> R) -> R {
    struct Restore(Option<(i64, i32)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            FIXED.set(self.0);
        }
    }
    let _restore = Restore(FIXED.replace(Some((timestamp.seconds, timestamp.nanos))));
    run()
}

#[test]
fn nested_scopes_restore_the_previous_clock() {
    assert_eq!(current(), None);
    let outer = prost_types::Timestamp {
        seconds: 17,
        nanos: 300_000_000,
    };
    let inner = prost_types::Timestamp {
        seconds: 18,
        nanos: 1,
    };
    with(&outer, || {
        assert_eq!(super::timestamp().unwrap(), outer);
        with(&inner, || assert_eq!(super::timestamp().unwrap(), inner));
        assert_eq!(super::timestamp().unwrap(), outer);
    });
    assert_eq!(current(), None);
}

#[test]
fn unwinding_releases_the_fixture_clock() {
    assert_eq!(current(), None);
    assert!(std::panic::catch_unwind(|| with(&Default::default(), || panic!("unwind"))).is_err());
    assert_eq!(current(), None);
}
