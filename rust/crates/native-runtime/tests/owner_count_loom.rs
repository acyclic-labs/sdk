//! Check the production final-owner election with instrumented atomics.
#![cfg(loom)]

mod sync {
    pub(crate) mod atomic {
        pub(crate) use loom::sync::atomic::{AtomicUsize, Ordering};
    }
}

#[path = "../src/health/owners.rs"]
mod owners;
