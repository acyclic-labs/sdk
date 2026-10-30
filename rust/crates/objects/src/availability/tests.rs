use super::*;
use futures::{FutureExt, task::ArcWake};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Wakes(AtomicUsize);
impl ArcWake for Wakes {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn parked(availability: &mut Availability, wakes: &Arc<Wakes>) {
    let waker = futures::task::waker(Arc::clone(wakes));
    let mut context = std::task::Context::from_waker(&waker);
    assert!(
        availability
            .changed()
            .boxed_local()
            .as_mut()
            .poll(&mut context)
            .is_pending()
    );
}

fn changed(availability: &mut Availability) -> Result<AvailabilityCursor, Error> {
    availability
        .changed()
        .now_or_never()
        .expect("original producer completed")
}

#[test]
fn original_producer_retains_inflight_changes_and_wakes_only_selected_keys() {
    let source = AvailabilitySource::new();
    let key = Digest::of(b"selected");
    let registration = source.register(key).unwrap();
    // Completion between registration and initial cursor capture is covered
    // by the mandatory verification, rather than a duplicate terminal frame.
    source.restored(key).unwrap();
    let mut selected = registration.open().unwrap();
    let mut unrelated = source.subscribe(Digest::of(b"other")).unwrap();
    let original = selected.cursor();
    let selected_wakes = Arc::new(Wakes::default());
    let other_wakes = Arc::new(Wakes::default());
    parked(&mut selected, &selected_wakes);
    parked(&mut unrelated, &other_wakes);

    // The original verification is in flight, with no change future being
    // polled. Physical completion must remain visible when it returns.
    for _ in 0..16 {
        source.restored(key).unwrap();
    }
    assert!(selected_wakes.0.load(Ordering::SeqCst) > 0);
    assert_eq!(other_wakes.0.load(Ordering::SeqCst), 0);
    let next = changed(&mut selected).unwrap();
    assert_eq!(next.incarnation, original.incarnation);
    assert_eq!(next.sequence, original.sequence + 16);
    assert!(unrelated.changed().now_or_never().is_none());
    assert!(selected.changed().now_or_never().is_none());
    drop(selected);
    drop(unrelated);
    assert!(source.keys().unwrap().as_ref().unwrap().is_empty());
}

#[test]
fn source_loss_ends_parked_and_unpolled_observers_without_retaining_the_owner() {
    for explicit_failure in [false, true] {
        let source = AvailabilitySource::new();
        let key = Digest::of(b"source-loss");
        let mut parked_observer = source.subscribe(key).unwrap();
        let mut unpolled = source.subscribe(key).unwrap();
        let wakes = Arc::new(Wakes::default());
        parked(&mut parked_observer, &wakes);
        if explicit_failure {
            source.fail();
            assert!(matches!(source.subscribe(key), Err(error) if error == unavailable()));
        }
        drop(source);
        assert!(wakes.0.load(Ordering::SeqCst) > 0);
        assert_eq!(changed(&mut parked_observer), Err(unavailable()));
        assert_eq!(changed(&mut unpolled), Err(unavailable()));
    }
}

#[test]
fn reconnect_registers_before_the_required_verification_and_releases_old_state() {
    let source = AvailabilitySource::new();
    let key = Digest::of(b"reconnect");
    let first = source.subscribe(key).unwrap();
    let old = first.cursor();
    drop(first);
    assert!(source.keys().unwrap().as_ref().unwrap().is_empty());
    // No history is retained. The reconnect handshake requires verification
    // even though this repair completed before the next subscription.
    source.restored(key).unwrap();
    let mut next = source.subscribe(key).unwrap();
    assert_ne!(old.incarnation, next.cursor().incarnation);
    source.restored(key).unwrap();
    assert_eq!(changed(&mut next).unwrap().sequence, 1);
    drop(next);
    assert!(source.keys().unwrap().as_ref().unwrap().is_empty());
}

#[test]
fn invalid_transport_cursor_and_source_eof_fail_closed() {
    let initial = AvailabilityCursor::new([1; 16], 4).unwrap();
    for next in [
        initial,
        AvailabilityCursor::new([1; 16], 3).unwrap(),
        AvailabilityCursor::new([2; 16], 5).unwrap(),
    ] {
        let mut observation = Availability::new(initial, stream::iter([Ok(next)]));
        assert_eq!(changed(&mut observation), Err(unavailable()));
        assert_eq!(changed(&mut observation), Err(unavailable()));
    }
    let mut ended = Availability::new(initial, stream::empty());
    assert_eq!(changed(&mut ended), Err(unavailable()));
}
