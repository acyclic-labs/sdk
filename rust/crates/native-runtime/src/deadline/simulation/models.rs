use super::*;

fn pool_capacity(broken: bool) {
    loom::model(move || {
        let clock = Clock::new();
        clock.0.state.lock().unwrap().queued_is_runnable = broken;
        let worker = clock.register();
        worker.queued(0, 1);
        let queued = clock.register();
        queued.queued(0, 1);
        // A free worker can start either queued job; time must not advance.
        assert!(!clock.advance_if_idle());
        let running = worker.clone();
        let advancing = clock.clone();
        let thread = loom::thread::spawn(move || {
            running.started();
            running.block(
                Some(Deadline::at(advancing.now() + Duration::from_secs(3))),
                None,
                None,
            );
            advancing.advance_if_idle();
            assert_eq!(
                advancing.elapsed(),
                Duration::from_secs(3),
                "full-pool queue prevented a bounded wait expiring"
            );
        });
        // The driver can inspect capacity before, during or after the worker
        // parks. Whichever acquires the clock last must expire the same bound.
        clock.advance_if_idle();
        thread.join().unwrap();
        worker.resume();
        assert!(!clock.advance_if_idle());
        drop(worker);
        assert!(!clock.advance_if_idle());
        queued.started();
        queued.block(
            Some(Deadline::at(clock.now() + Duration::from_secs(2))),
            None,
            None,
        );
        assert!(clock.advance_if_idle());
        assert_eq!(
            clock.advances(),
            [Duration::from_secs(3), Duration::from_secs(5)]
        );
    });
}
#[test]
fn full_worker_pools_block_queued_actors_until_capacity_is_available() {
    pool_capacity(false);
}
#[test]
#[should_panic(expected = "full-pool queue prevented a bounded wait expiring")]
fn counting_queued_work_as_runnable_fails_the_same_pool_model() {
    pool_capacity(true);
}

fn synchronous_child(broken: bool) {
    loom::model(move || {
        let clock = Clock::new();
        clock.0.state.lock().unwrap().forget_parent_suspend = broken;
        let parent = clock.register();
        let child = clock.register();
        parent.enter(|| {
            child.enter(|| {
                let suspended = clock.0.state.lock().unwrap().actors[&parent.id].suspended;
                assert_eq!(suspended, 1, "synchronous child left its parent runnable");
                super::super::sleep(Duration::from_secs(3));
            })
        });
        assert_eq!(clock.advances(), [Duration::from_secs(3)]);
        assert_eq!(
            clock.0.state.lock().unwrap().actors[&parent.id].suspended,
            0
        );
    });
}
#[test]
fn synchronous_children_park_their_calling_actor_until_return() {
    synchronous_child(false);
}
#[test]
#[should_panic(expected = "synchronous child left its parent runnable")]
fn leaving_the_synchronous_parent_runnable_fails_the_same_model() {
    synchronous_child(true);
}

fn callback_entry(broken: bool) {
    loom::model(move || {
        let clock = Clock::new();
        clock.0.state.lock().unwrap().duplicate_entry = broken;
        clock.enter(|| {
            let captured = Clock::capture().unwrap();
            captured.enter(|| {
                let actors = clock.0.state.lock().unwrap().actors.len();
                assert_eq!(actors, 1, "reentrant callback created a duplicate actor");
                super::super::sleep(Duration::from_secs(3));
            });
        });
        assert_eq!(clock.advances(), [Duration::from_secs(3)]);
        assert!(clock.0.state.lock().unwrap().actors.is_empty());
    });
}
#[test]
fn reentrant_callbacks_reuse_their_actor_and_expire_their_own_wait() {
    callback_entry(false);
}
#[test]
#[should_panic(expected = "reentrant callback created a duplicate actor")]
fn duplicate_callback_actors_fail_the_same_wait_model() {
    callback_entry(true);
}

fn advance(broken: bool) {
    loom::model(move || {
        let clock = Clock::new();
        let blocked = clock.register();
        let running = clock.register();
        blocked.block(
            Some(Deadline::at(clock.now() + Duration::from_secs(5))),
            None,
            None,
        );
        clock.0.state.lock().unwrap().advance_while_running = broken;
        let other = clock.clone();
        let thread = loom::thread::spawn(move || {
            assert!(
                !other.advance_if_idle(),
                "virtual clock advanced over runnable work"
            );
        });
        running.resume();
        thread.join().unwrap();
        assert!(clock.advances().is_empty());
    });
}
#[test]
fn running_actors_block_deadline_advancement_under_every_interleaving() {
    advance(false);
}
#[test]
#[should_panic(expected = "virtual clock advanced over runnable work")]
fn advancing_past_running_work_fails_the_same_model() {
    advance(true);
}

#[test]
fn a_notification_cannot_be_overwritten_by_a_pending_poll() {
    loom::model(|| {
        let clock = Clock::new();
        let actor = clock.register();
        let generation = clock.0.state.lock().unwrap().actors[&actor.id].generation;
        let notified = actor.clone();
        let thread = loom::thread::spawn(move || notified.resume());
        actor.block(None, None, Some(generation));
        thread.join().unwrap();
        assert!(
            clock.0.state.lock().unwrap().actors[&actor.id]
                .blocked
                .is_none(),
            "pending poll erased a wake"
        );
    });
}

#[test]
fn final_blocking_actor_advances_and_cannot_lose_its_timer_wake() {
    loom::model(|| {
        let clock = Clock::new();
        let actor = clock.register();
        let deadline = Deadline::at(clock.now() + Duration::from_secs(3));
        let waiter = Arc::new(Waiter::new());
        let thread = loom::thread::spawn(move || actor.enter(|| waiter.wait(Some(deadline))));
        thread.join().unwrap();
        assert_eq!(clock.advances(), [Duration::from_secs(3)]);
    });
}

#[test]
fn notification_and_deadline_publish_before_the_waiting_actor_can_resume() {
    loom::model(|| {
        let clock = Clock::new();
        let actor = clock.register();
        let waiter = Arc::new(Waiter::new());
        let notified = waiter.clone();
        let deadline = Deadline::at(clock.now() + Duration::from_secs(3));
        let thread = loom::thread::spawn(move || actor.enter(|| waiter.wait(Some(deadline))));
        notified.notify();
        thread.join().unwrap();
        assert!(
            clock
                .advances()
                .iter()
                .all(|time| *time == Duration::from_secs(3))
        );
    });
}

fn completion_retirement(broken: bool) {
    loom::model(move || {
        use crate::{CompletionState, TaskTerminal};
        let clock = Clock::new();
        let timer = clock.register();
        timer.block(
            Some(Deadline::at(clock.now() + Duration::from_secs(3))),
            None,
            None,
        );
        let mut completion = CompletionState::<()> {
            task_pending: true,
            actor: Some(clock.register()),
            result: Some(Ok(())),
            ..CompletionState::default()
        };
        if broken {
            completion.actor.take();
        }
        assert!(
            clock.advances().is_empty(),
            "physical actor retired before publication"
        );
        let completion = Arc::new(Mutex::new(completion));
        let published = completion.clone();
        let thread = loom::thread::spawn(move || {
            published.publish();
        });
        {
            let state = completion.lock().unwrap();
            if state.task_pending {
                assert!(
                    !clock.advance_if_idle(),
                    "physical actor retired before publication"
                );
            }
        }
        thread.join().unwrap();
        assert_eq!(clock.advances(), [Duration::from_secs(3)]);
    });
}
#[test]
fn real_completion_retires_its_actor_after_publication_and_outside_the_lock() {
    completion_retirement(false);
}
#[test]
#[should_panic(expected = "physical actor retired before publication")]
fn erasing_the_physical_actor_fails_the_same_completion_model() {
    completion_retirement(true);
}

fn completion_wake(broken: bool) {
    loom::model(move || {
        use crate::{CompletionState, TaskTerminal};
        struct Noop;
        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
        }
        let clock = Clock::new();
        let observer = clock.register();
        observer.block(None, None, None);
        let timer = clock.register();
        timer.block(
            Some(Deadline::at(clock.now() + Duration::from_secs(3))),
            None,
            None,
        );
        let completion = Mutex::new(CompletionState::<()> {
            task_pending: true,
            actor: Some(clock.register()),
            result: Some(Ok(())),
            waker: Some(Waker::from(Arc::new(ScopedWake {
                actor: Arc::downgrade(&observer),
                task: Waker::from(Arc::new(Noop)),
            }))),
            broken_wake: broken,
            ..CompletionState::default()
        });
        if let Some(waker) = completion.publish() {
            waker.wake();
        }
        assert!(
            clock.advances().is_empty(),
            "deadline advanced before observer wake"
        );
    });
}
#[test]
fn completion_makes_its_observer_runnable_before_retiring_the_physical_actor() {
    completion_wake(false);
}
#[test]
#[should_panic(expected = "deadline advanced before observer wake")]
fn erasing_the_completion_wake_fails_the_same_clock_model() {
    completion_wake(true);
}
