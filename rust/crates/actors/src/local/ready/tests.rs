use super::*;

fn alarm(
    generation: u64,
    deadline_unix_ms: Option<u64>,
    delivered_generation: Option<u64>,
) -> super::super::Alarm {
    super::super::Alarm {
        generation,
        deadline_unix_ms,
        delivered_generation,
    }
}

#[cfg(not(loom))]
#[test]
fn due_alarms_share_one_bounded_index_and_replacements_remove_old_nodes()
-> Result<(), super::super::Error> {
    let set = ReadySet::default();
    let first = set.register(1)?;
    let second = set.register(2)?;
    let third = set.register(3)?;
    while let Some(work) = set.take() {
        drop(work);
    }
    first.update_alarm(&alarm(1, Some(20), None))?;
    second.update_alarm(&alarm(1, Some(30), None))?;
    third.update_alarm(&alarm(1, Some(40), None))?;
    first.update_alarm(&alarm(2, Some(50), None))?;
    assert_eq!(
        set.alarm_count(),
        3,
        "replacement must remove the old timer node"
    );
    assert_eq!(set.next_deadline(), Some(30));
    assert_eq!(
        set.wake_due(25, 10),
        0,
        "replaced deadline must never become ready"
    );
    assert_eq!(
        set.wake_due(100, 1),
        1,
        "work budget must bound overdue processing"
    );
    let work = set
        .take()
        .ok_or(super::super::Error::Invalid("missing due work"))?;
    assert_eq!(*work.key(), 2);
    drop(work);
    assert_eq!(set.alarm_count(), 2);
    second.update_alarm(&alarm(1, Some(30), Some(1)))?;
    assert!(
        matches!(
            second.update_alarm(&alarm(1, Some(30), None)),
            Err(super::super::Error::Conflict)
        ),
        "consumed generation cannot be made eligible again"
    );
    drop(third);
    assert_eq!(
        set.alarm_count(),
        1,
        "cancellation must unlink the actual due node"
    );
    assert_eq!(set.next_deadline(), Some(50));
    assert_eq!(set.wake_due(50, 0), 0);
    assert_eq!(set.wake_due(50, 1), 1);
    assert_eq!(set.alarm_count(), 0);
    assert!(set.next_deadline().is_none());
    Ok(())
}

#[cfg(not(loom))]
#[test]
fn due_wake_survives_running_work_but_cleared_and_stale_generations_do_not_rearm()
-> Result<(), super::super::Error> {
    let set = ReadySet::default();
    let registration = set.register(7)?;
    let work = set
        .take()
        .ok_or(super::super::Error::Invalid("missing replay"))?;
    registration.update_alarm(&alarm(1, Some(10), None))?;
    assert_eq!(set.wake_due(10, 1), 1);
    registration.update_alarm(&alarm(2, Some(20), None))?;
    assert!(
        matches!(
            registration.update_alarm(&alarm(1, Some(10), None)),
            Err(super::super::Error::Conflict)
        ),
        "old durable observations cannot replace a newer alarm"
    );
    drop(work);
    assert_eq!(
        set.counts(),
        (1, 1),
        "due while running must survive parking"
    );
    drop(set.take());
    registration.update_alarm(&alarm(3, None, None))?;
    assert_eq!(
        set.wake_due(100, 10),
        0,
        "clear must remove the replacement's due node"
    );
    assert_eq!(set.counts(), (1, 0));
    assert_eq!(set.alarm_count(), 0);
    Ok(())
}

#[cfg(not(loom))]
#[test]
fn earlier_deadline_rearms_the_shared_dispatcher_after_unlock() -> Result<(), super::super::Error> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Signal {
        core: Arc<Core<u64>>,
        wakes: AtomicUsize,
    }
    impl Wake for Signal {
        fn wake(self: Arc<Self>) {
            assert!(
                self.core.index.try_lock().is_ok(),
                "dispatcher callback must run after publication unlock"
            );
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }
    let set = ReadySet::default();
    let registration = set.register(7_u64)?;
    drop(set.take());
    let signal = Arc::new(Signal {
        core: Arc::clone(&set.core),
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&signal));
    assert!(
        set.poll_ready(&mut Context::from_waker(&waker))
            .is_pending()
    );
    registration.update_alarm(&alarm(1, Some(100), None))?;
    assert!(
        set.poll_ready(&mut Context::from_waker(&waker))
            .is_pending()
    );
    registration.update_alarm(&alarm(2, Some(50), None))?;
    assert_eq!(
        signal.wakes.load(Ordering::SeqCst),
        2,
        "an earlier committed deadline must rearm the shared dispatcher"
    );
    assert_eq!(set.next_deadline(), Some(50));
    assert_eq!(
        set.counts(),
        (1, 0),
        "installing a future alarm does not execute a handler"
    );
    Ok(())
}

#[cfg(not(loom))]
#[test]
fn replay_coalescing_and_hibernation_have_bounded_ownership() -> Result<(), super::super::Error> {
    let set = ReadySet::default();
    let registration = set.register(7)?;
    assert_eq!(
        set.counts(),
        (1, 1),
        "registered subscriptions must start with replay"
    );
    let wake = registration.waker();
    for _ in 0..100 {
        wake.wake_by_ref();
    }
    assert_eq!(set.counts(), (1, 1));
    let work = set
        .take()
        .ok_or(super::super::Error::Invalid("missing initial replay"))?;
    assert_eq!(*work.key(), 7);
    wake.wake_by_ref();
    drop(work);
    assert_eq!(
        set.counts(),
        (1, 1),
        "append during replay must survive parking"
    );
    drop(set.take());
    assert_eq!(set.counts(), (1, 0));
    drop(registration);
    wake.wake_by_ref();
    assert_eq!(
        set.counts(),
        (0, 0),
        "retired wake cannot recreate registration"
    );
    Ok(())
}

#[cfg(not(loom))]
#[test]
fn dormant_population_retains_no_work_and_unlinks_cancelled_ready_entries()
-> Result<(), super::super::Error> {
    let set = ReadySet::default();
    let mut registrations = Vec::new();
    for id in 0..100_000 {
        registrations.push(set.register(id)?);
    }
    assert_eq!(set.counts(), (100_000, 100_000));
    while let Some(work) = set.take() {
        drop(work);
    }
    assert_eq!(set.counts(), (100_000, 0));
    for registration in &registrations {
        registration.waker().wake();
    }
    // Removal unlinks any position directly; no tombstone is retained in a queue.
    registrations.reverse();
    drop(registrations);
    assert_eq!(set.counts(), (0, 0));
    Ok(())
}

#[cfg(loom)]
mod schedules {
    use super::*;
    use loom::{
        sync::atomic::{AtomicUsize, Ordering},
        thread,
    };

    #[test]
    fn append_racing_replay_and_park_cannot_lose_ready_work() {
        loom::model(|| {
            let set = ReadySet::default();
            let registration = set.register(7).unwrap();
            let work = set.take().unwrap();
            let wake = registration.waker();
            let tail = Arc::new(AtomicUsize::new(0));
            let consumed = Arc::new(AtomicUsize::new(0));
            let reader_tail = Arc::clone(&tail);
            let reader_consumed = Arc::clone(&consumed);
            let reader = thread::spawn(move || {
                reader_consumed.store(reader_tail.load(Ordering::SeqCst), Ordering::SeqCst);
                drop(work);
            });
            let writer = thread::spawn(move || {
                tail.store(1, Ordering::SeqCst);
                wake.wake();
            });
            reader.join().unwrap();
            writer.join().unwrap();
            assert!(
                consumed.load(Ordering::SeqCst) == 1 || set.counts().1 == 1,
                "committed input must be replayed or remain ready across parking"
            );
        });
    }

    #[test]
    fn cancelled_registration_cannot_wake_a_new_placement() {
        loom::model(|| {
            let set = ReadySet::default();
            let old = set.register(1).unwrap();
            drop(set.take());
            let stale = old.waker();
            let current = set.register(2).unwrap();
            let wake = stale.clone();
            let writer = thread::spawn(move || wake.wake());
            let cancel = thread::spawn(move || drop(old));
            writer.join().unwrap();
            cancel.join().unwrap();
            stale.wake();
            assert_eq!(set.counts(), (1, 1));
            let work = set.take().unwrap();
            assert_eq!(*work.key(), 2, "stale wake cannot substitute old placement");
            drop(work);
            drop(current);
            assert_eq!(set.counts(), (0, 0));
        });
    }

    #[test]
    fn due_alarm_racing_running_work_parking_retains_one_ready_entry() {
        loom::model(|| {
            let set = Arc::new(ReadySet::default());
            let registration = set.register(7).unwrap();
            let work = set.take().unwrap();
            registration
                .update_alarm(&alarm(1, Some(10), None))
                .unwrap();
            let due_set = Arc::clone(&set);
            let due = thread::spawn(move || due_set.wake_due(10, 1));
            let park = thread::spawn(move || drop(work));
            assert_eq!(due.join().unwrap(), 1);
            park.join().unwrap();
            assert_eq!(set.alarm_count(), 0);
            assert_eq!(
                set.counts(),
                (1, 1),
                "a due alarm must survive either order of the parking handoff"
            );
            drop(set.take());
            assert_eq!(set.counts(), (1, 0));
            drop(registration);
            assert_eq!(set.counts(), (0, 0));
        });
    }

    #[test]
    fn clear_racing_due_selection_cannot_restore_the_old_generation() {
        loom::model(|| {
            let set = Arc::new(ReadySet::default());
            let registration = Arc::new(set.register(7).unwrap());
            drop(set.take());
            registration
                .update_alarm(&alarm(1, Some(10), None))
                .unwrap();
            let due_set = Arc::clone(&set);
            let due = thread::spawn(move || due_set.wake_due(10, 1));
            let clearing = Arc::clone(&registration);
            let clear = thread::spawn(move || {
                clearing.update_alarm(&alarm(2, None, None)).unwrap();
            });
            due.join().unwrap();
            clear.join().unwrap();
            assert_eq!(set.alarm_count(), 0);
            assert_eq!(set.next_deadline(), None);
            assert!(matches!(
                registration.update_alarm(&alarm(1, Some(10), None)),
                Err(super::super::super::Error::Conflict)
            ));
            // Selection may already have queued work. The retained generation,
            // rather than absence of a stale wake, fences actual publication.
            assert_eq!(set.wake_due(10, 1), 0);
            drop(set.take());
            assert_eq!(set.counts(), (1, 0));
            drop(registration);
            assert_eq!(set.counts(), (0, 0));
        });
    }

    #[test]
    fn earlier_alarm_racing_dispatcher_wait_retains_a_rearm_signal() {
        loom::model(|| {
            let set = ReadySet::default();
            let registration = set.register(7).unwrap();
            drop(set.take());
            registration
                .update_alarm(&alarm(1, Some(100), None))
                .unwrap();
            let counter = Arc::new(Counter(AtomicUsize::new(0)));
            let consumer_counter = Arc::clone(&counter);
            let consumer = thread::spawn(move || {
                let waiter = Waker::from(consumer_counter);
                assert!(
                    set.poll_ready(&mut Context::from_waker(&waiter))
                        .is_pending()
                );
                let observed = set.next_deadline();
                (set, observed)
            });
            let producer = thread::spawn(move || {
                registration
                    .update_alarm(&alarm(2, Some(50), None))
                    .unwrap();
                registration
            });
            let (_set, observed) = consumer.join().unwrap();
            let registration = producer.join().unwrap();
            assert!(
                observed == Some(50) || counter.0.load(Ordering::SeqCst) > 0,
                "the sole dispatcher must observe the earlier deadline or retain its wake"
            );
            drop(registration);
        });
    }
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn dispatcher_registers_its_waiter_with_the_empty_check() {
        loom::model(|| {
            let set = ReadySet::default();
            let registration = set.register(1).unwrap();
            drop(set.take());
            let wake = registration.waker();
            let counter = Arc::new(Counter(AtomicUsize::new(0)));
            let consumer_counter = Arc::clone(&counter);
            let consumer = thread::spawn(move || {
                let waiter = Waker::from(consumer_counter);
                let pending = set
                    .poll_ready(&mut Context::from_waker(&waiter))
                    .is_pending();
                (set, pending)
            });
            let producer = thread::spawn(move || wake.wake());
            let (_set, pending) = consumer.join().unwrap();
            producer.join().unwrap();
            assert!(
                !pending || counter.0.load(Ordering::SeqCst) > 0,
                "empty-to-wait handoff must retain append notification"
            );
            drop(registration);
        });
    }
}
