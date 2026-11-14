//! Model the production submission handoff, including release/capacity races.
#![cfg(loom)]
mod sync {
    pub(crate) mod atomic {
        pub(crate) use loom::sync::atomic::{AtomicUsize, Ordering};
    }
}
#[path = "../src/task/submission.rs"]
mod submission;
