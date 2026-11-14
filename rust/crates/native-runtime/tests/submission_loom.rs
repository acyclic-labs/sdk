//! Exercise the production retained-job handoff at every capacity-wake boundary.
#![cfg(loom)]

mod sync {
    pub(crate) mod atomic {
        pub(crate) use loom::sync::atomic::{AtomicUsize, Ordering};
    }
}

#[path = "../src/task/submission.rs"]
mod submission;

use loom::sync::{Arc, atomic::{AtomicBool, Ordering}};
use submission::SubmissionHandoff;

fn submit(handoff: &SubmissionHandoff, available: &AtomicBool, accepted: &AtomicBool) {
    if !handoff.request() {
        return;
    }
    loop {
        handoff.begin();
        if available.load(Ordering::Acquire) {
            accepted.store(true, Ordering::Release);
        }
        if handoff.release() {
            return;
        }
    }
}

#[test]
fn capacity_wake_cannot_be_lost_at_submission_release() {
    loom::model(|| {
        let handoff = Arc::new(SubmissionHandoff::default());
        let available = Arc::new(AtomicBool::new(false));
        let accepted = Arc::new(AtomicBool::new(false));
        let worker = {
            let handoff = handoff.clone();
            let available = available.clone();
            let accepted = accepted.clone();
            loom::thread::spawn(move || {
                available.store(true, Ordering::Release);
                submit(&handoff, &available, &accepted);
            })
        };
        submit(&handoff, &available, &accepted);
        worker.join().unwrap();
        assert!(accepted.load(Ordering::Acquire), "capacity wake was lost");
    });
}
