//! Native runtime integration tests and instrumented models.
mod cached_flush;
mod file_lock;
mod native_claim_record;
mod native_owner_record;
mod native_report_port;
mod node_process;
#[cfg(all(target_os = "linux", not(loom)))]
mod output;
mod private_file;
mod seeded_runtime;
mod signing_custody;
#[path = "runtime_api/mod.rs"]
mod tests;

#[cfg(loom)]
mod sync {
    pub(crate) use loom::sync::{Condvar, Mutex};
    pub(crate) mod atomic {
        pub(crate) use loom::sync::atomic::{AtomicUsize, Ordering};
    }
}
#[cfg(loom)]
#[path = "../src/wake.rs"]
mod wake;

#[cfg(feature = "model-checking")]
#[path = "device_flush_loom.rs"]
mod device_flush_loom;
#[cfg(feature = "model-checking")]
#[path = "owner_count_loom.rs"]
mod owner_count_loom;
#[cfg(feature = "model-checking")]
#[path = "process_signal_loom.rs"]
mod process_signal_loom;
#[cfg(feature = "model-checking")]
#[path = "retained_submission_loom.rs"]
mod retained_submission_loom;
#[cfg(feature = "model-checking")]
#[path = "wake_loom.rs"]
mod wake_loom;
