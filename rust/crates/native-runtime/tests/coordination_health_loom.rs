//! Models the production retirement primitive and the task completion edge.
#![cfg(loom)]
#[path = "../src/coordination/health.rs"]
mod health;

use health::ShardHealth;
use loom::sync::{Arc, mpsc};
use std::{
    future::Future,
    task::{Context, Waker},
};

fn retirement_before_completion(ordered: bool) {
    loom::model(move || {
        let health = Arc::new(ShardHealth::new());
        let mut retired = Box::pin(health.retired());
        let mut context = Context::from_waker(Waker::noop());
        assert!(retired.as_mut().poll(&mut context).is_pending());
        let owner = Arc::clone(&health);
        let (completed, terminal) = mpsc::channel();
        let thread = loom::thread::spawn(move || {
            if ordered {
                owner.retire();
            }
            completed.send(()).unwrap();
            if !ordered {
                owner.retire();
            }
        });
        terminal.recv().unwrap();
        assert!(!health.is_available(), "completion preceded retirement");
        assert!(
            retired.as_mut().poll(&mut context).is_ready(),
            "published retirement left its original waiter pending"
        );
        thread.join().unwrap();
    });
}

#[test]
fn cancelled_tasks_observe_their_original_producer_retirement() {
    retirement_before_completion(true);
}

#[test]
#[should_panic(expected = "completion preceded retirement")]
fn publishing_completion_before_retirement_fails_the_same_oracle() {
    retirement_before_completion(false);
}
